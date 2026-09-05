use std::{fmt, future::Future, net::IpAddr, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::{Client, Config, GenericClient, IsolationLevel, NoTls};

use super::error::{PostgresSinkError, database_error, invalid_config, invalid_spec, timeout};

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IDENTIFIER_BYTES: usize = 63;
const MAX_SINK_ID_BYTES: usize = 32;

pub(super) struct PgClient {
    pub(super) runtime: Runtime,
    pub(super) client: Client,
}

pub(super) fn bounded<T>(
    runtime: &Runtime,
    stage: &'static str,
    future: impl Future<Output = Result<T, PostgresSinkError>>,
) -> Result<T, PostgresSinkError> {
    bounded_with_timeout(runtime, DATABASE_TIMEOUT, stage, future)
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

    /// Discovers stable target identity and verifies that every sink-owned
    /// schema object is absent. The call performs read-only catalog access.
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
    ) -> Result<PostgresTargetSpec, PostgresSinkError> {
        let mut spec = PostgresTargetSpec {
            sink_id: sink_id.into(),
            database: self.database.clone(),
            schema: schema.into(),
            table: table.into(),
            system_identifier: String::new(),
            database_oid: 0,
        };
        validate_names(&spec)?;

        let mut connection = self.connect()?;
        let PgClient { runtime, client } = &mut connection;
        bounded(runtime, "target discovery", async {
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
            validate_identity(&spec)?;

            require_absent(&transaction, &spec).await?;
            transaction
                .commit()
                .await
                .map_err(|error| database_error("finish target discovery", &error))?;
            Ok(())
        })?;
        Ok(spec)
    }

    pub(super) fn connect(&self) -> Result<PgClient, PostgresSinkError> {
        self.connect_with_timeout(DATABASE_TIMEOUT)
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
    EXISTS(SELECT 1 FROM pg_catalog.pg_type AS t \
           JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace \
           WHERE n.nspname = $1 AND t.typname = $3)";

pub(super) async fn require_absent(
    client: &impl GenericClient,
    spec: &PostgresTargetSpec,
) -> Result<(), PostgresSinkError> {
    let names = spec.object_names().to_vec();
    let row = client
        .query_one(ABSENCE_QUERY, &[&spec.schema(), &names, &spec.table()])
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
    spec: &PostgresTargetSpec,
    schema_exists: bool,
    class_conflict: Option<String>,
    row_type_exists: bool,
) -> Result<(), PostgresSinkError> {
    if !schema_exists {
        return Err(PostgresSinkError::TargetMissing {
            name: spec.schema().to_owned(),
        });
    }
    if let Some(name) = class_conflict {
        return Err(PostgresSinkError::TargetExists { name });
    }
    if row_type_exists {
        return Err(PostgresSinkError::TargetExists {
            name: spec.table().to_owned(),
        });
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

/// Non-sensitive, persistent identity of a sink-owned `PostgreSQL` target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresTargetSpec {
    sink_id: String,
    database: String,
    schema: String,
    table: String,
    system_identifier: String,
    database_oid: u32,
}

impl PostgresTargetSpec {
    /// Validates a manually assembled target identity.
    ///
    /// Production callers normally obtain this value through
    /// [`PostgresSinkConfig::discover_target`]. Constructing a value manually
    /// does not adopt or share objects owned by another Flow.
    ///
    /// # Errors
    ///
    /// Rejects malformed identifiers and zero cluster/database identities.
    pub fn try_new(
        sink_id: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        system_identifier: impl Into<String>,
        database_oid: u32,
    ) -> Result<Self, PostgresSinkError> {
        let spec = Self {
            sink_id: sink_id.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            system_identifier: system_identifier.into(),
            database_oid,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Validates all persistent fields after decoding.
    ///
    /// # Errors
    ///
    /// Rejects malformed identifiers and zero cluster/database identities.
    pub fn validate(&self) -> Result<(), PostgresSinkError> {
        validate_names(self)?;
        validate_identity(self)
    }

    /// Returns the stable identity of this sink instance.
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Returns the `PostgreSQL` database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the exact quoted target schema component.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the exact quoted target table component.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the `PostgreSQL` cluster system identifier as decimal text.
    #[must_use]
    pub fn system_identifier(&self) -> &str {
        &self.system_identifier
    }

    /// Returns the database OID captured during discovery.
    #[must_use]
    pub const fn database_oid(&self) -> u32 {
        self.database_oid
    }

    pub(super) fn hash_index(&self) -> String {
        format!("$dogpaddle.hash.{}", self.sink_id)
    }

    pub(super) fn object_names(&self) -> [String; 3] {
        [
            self.table.clone(),
            self.hash_index(),
            format!("$dogpaddle.pk.{}", self.sink_id),
        ]
    }
}

fn validate_names(spec: &PostgresTargetSpec) -> Result<(), PostgresSinkError> {
    if spec.sink_id.is_empty()
        || spec.sink_id.len() > MAX_SINK_ID_BYTES
        || !spec
            .sink_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid_spec(
            "sink ID must contain 1–32 lowercase ASCII letters, digits, or underscores",
        ));
    }
    for (label, value) in [
        ("database", spec.database.as_str()),
        ("schema", spec.schema.as_str()),
        ("table", spec.table.as_str()),
    ] {
        if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.contains('\0') {
            return Err(invalid_spec(format!(
                "{label} must be a nonempty PostgreSQL identifier of at most 63 bytes"
            )));
        }
    }
    for name in spec.object_names() {
        if name.len() > MAX_IDENTIFIER_BYTES {
            return Err(invalid_spec("derived sink object name exceeds 63 bytes"));
        }
    }
    if spec
        .object_names()
        .into_iter()
        .skip(1)
        .any(|name| name == spec.table)
    {
        return Err(invalid_spec(
            "target table name collides with a sink-owned object",
        ));
    }
    Ok(())
}

fn validate_identity(spec: &PostgresTargetSpec) -> Result<(), PostgresSinkError> {
    if spec
        .system_identifier
        .parse::<u64>()
        .ok()
        .is_none_or(|identifier| identifier == 0)
        || spec.database_oid == 0
    {
        return Err(invalid_spec(
            "cluster system identifier and database OID must be nonzero",
        ));
    }
    Ok(())
}
