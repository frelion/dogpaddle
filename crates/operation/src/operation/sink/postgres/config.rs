use std::{
    fmt,
    future::Future,
    net::IpAddr,
    time::{Duration, Instant},
};

use tokio::runtime::{Builder, Runtime};
use tokio_postgres::{Client, Config, GenericClient, IsolationLevel, NoTls};

use super::{
    definition::{PostgresSinkDefinition, validate_names},
    error::{PostgresSinkError, database_error, invalid_config, timeout},
};

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct PgClient {
    pub(super) runtime: Runtime,
    pub(super) client: Client,
}

fn bounded_with_timeout<T>(
    runtime: &Runtime,
    duration: Duration,
    stage: &'static str,
    future: impl Future<Output = Result<T, PostgresSinkError>>,
) -> Result<T, PostgresSinkError> {
    runtime.block_on(async move {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| timeout(stage))?
    })
}

pub(super) fn bounded_until<T>(
    runtime: &Runtime,
    deadline: Instant,
    stage: &'static str,
    future: impl Future<Output = Result<T, PostgresSinkError>>,
) -> Result<T, PostgresSinkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(timeout(stage));
    }
    let value = bounded_with_timeout(runtime, remaining, stage, future)?;
    if Instant::now() >= deadline {
        return Err(timeout(stage));
    }
    Ok(value)
}

/// Ephemeral credentials and endpoint for one `PostgreSQL` sink.
///
/// This pilot uses an unencrypted connection. The value is supplied as a runtime
/// resource and is never encoded into an Operation or Flow Definition.
pub struct PostgresSinkConfig {
    host: IpAddr,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl PostgresSinkConfig {
    /// Creates runtime configuration for a numeric IP address without opening
    /// a connection.
    ///
    /// # Errors
    ///
    /// Rejects a non-IP host, zero port, blank connection fields, or embedded
    /// NUL bytes. Requiring numeric IPv4 or IPv6 keeps the connection deadline
    /// independent of non-cancellable system DNS. The password may be empty
    /// for independently secured local access.
    pub fn new_unencrypted(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, PostgresSinkError> {
        let host = host
            .into()
            .parse()
            .map_err(|_| invalid_config("host must be a numeric IPv4 or IPv6 address"))?;
        let config = Self {
            host,
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        };
        if port == 0 {
            return Err(invalid_config("port must be nonzero"));
        }
        if [&config.database, &config.user]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || config.password.contains('\0')
        {
            return Err(invalid_config("invalid PostgreSQL connection fields"));
        }
        Ok(config)
    }

    /// Returns a pure sink plan with the discovered stable target identity.
    /// Verifies that every sink-owned schema object is absent through read-only
    /// catalog access.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection and catalog failures, malformed
    /// identifiers, a missing schema, or an existing target object.
    pub fn discover_target(
        &self,
        sink_id: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<PostgresSinkDefinition, PostgresSinkError> {
        let mut spec = PostgresSinkDefinition {
            sink_id: sink_id.into(),
            database: self.database.clone(),
            schema: schema.into(),
            table: table.into(),
            system_identifier: String::new(),
            database_oid: 0,
        };
        validate_names(&spec)?;

        let deadline = Instant::now() + DATABASE_TIMEOUT;
        let mut connection =
            self.connect_with_timeout(deadline.saturating_duration_since(Instant::now()))?;
        let PgClient { runtime, client } = &mut connection;
        bounded_until(runtime, deadline, "target discovery", async {
            let transaction = client
                .build_transaction()
                .read_only(true)
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .await
                .map_err(|error| database_error("begin target discovery", &error))?;
            let identity = transaction
                .query_one(
                    "SELECT s.system_identifier::text, d.oid, \
                            current_setting('fsync') = 'on', \
                            current_setting('synchronous_commit') IN ('on', 'remote_write', 'remote_apply'), \
                            current_setting('server_encoding') = 'UTF8' \
                     FROM pg_catalog.pg_control_system() AS s \
                     CROSS JOIN pg_catalog.pg_database AS d \
                     WHERE d.datname = pg_catalog.current_database()",
                    &[],
                )
                .await
                .map_err(|error| database_error("read target identity", &error))?;
            spec.system_identifier = identity.get(0);
            spec.database_oid = identity.get(1);
            if !identity.get::<_, bool>(2) || !identity.get::<_, bool>(3) {
                return Err(PostgresSinkError::DurabilityDisabled);
            }
            if !identity.get::<_, bool>(4) {
                return Err(PostgresSinkError::UnsupportedServerEncoding);
            }
            spec.validate()?;

            require_absent(&transaction, &spec).await?;
            transaction
                .commit()
                .await
                .map_err(|error| database_error("finish target discovery", &error))?;
            Ok(())
        })?;
        Ok(spec)
    }

    pub(super) fn connect_with_timeout(
        &self,
        duration: Duration,
    ) -> Result<PgClient, PostgresSinkError> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread Tokio runtime can be constructed");
        let mut config = Config::new();
        config
            .hostaddr(self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(&self.password)
            .connect_timeout(duration)
            .options(
                "-c statement_timeout=5000 \
                 -c lock_timeout=5000 \
                 -c idle_in_transaction_session_timeout=5000 \
                 -c work_mem=4MB \
                 -c synchronous_commit=on \
                 -c search_path=pg_catalog \
                 -c application_name=dogpaddle_postgres_sink",
            );
        let (client, connection) = bounded_with_timeout(&runtime, duration, "connect", async {
            config
                .connect(NoTls)
                .await
                .map_err(|error| database_error("connect", &error))
        })?;
        drop(runtime.spawn(async move {
            let _ = connection.await;
        }));
        Ok(PgClient { runtime, client })
    }

    pub(super) fn database(&self) -> &str {
        &self.database
    }
}

const ABSENCE_QUERY: &str = "SELECT \
    EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1), \
    (SELECT c.relname::text FROM pg_catalog.pg_class AS c \
     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
     WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[]) \
     ORDER BY array_position($2::text[], c.relname::text) LIMIT 1), \
    (SELECT t.typname::text FROM pg_catalog.pg_type AS t \
           JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace \
           WHERE n.nspname = $1 AND t.typname = ANY($3::text[]) \
           ORDER BY array_position($3::text[], t.typname::text) LIMIT 1)";

pub(super) async fn require_absent(
    client: &impl GenericClient,
    spec: &PostgresSinkDefinition,
) -> Result<(), PostgresSinkError> {
    let names = spec.object_names().to_vec();
    let types = [spec.table().to_owned(), spec.frontier_table()];
    let row = client
        .query_one(ABSENCE_QUERY, &[&spec.schema(), &names, &types.as_slice()])
        .await
        .map_err(|error| database_error("inspect target objects", &error))?;
    validate_absence_snapshot(
        spec,
        row.get(0),
        row.get::<_, Option<String>>(1),
        row.get(2),
    )
}

pub(super) fn validate_absence_snapshot(
    spec: &PostgresSinkDefinition,
    schema_exists: bool,
    class_conflict: Option<String>,
    row_type_conflict: Option<String>,
) -> Result<(), PostgresSinkError> {
    if !schema_exists {
        return Err(PostgresSinkError::TargetMissing {
            name: spec.schema().to_owned(),
        });
    }
    if let Some(name) = class_conflict {
        return Err(PostgresSinkError::TargetExists { name });
    }
    if let Some(name) = row_type_conflict {
        return Err(PostgresSinkError::TargetExists { name });
    }
    Ok(())
}

impl fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSinkConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .finish()
    }
}
