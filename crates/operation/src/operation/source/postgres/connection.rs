use std::{fmt, path::PathBuf, time::Duration};

use dogpaddle_debezium::{Checkpoint, Connector, ConnectorConfig, DebeziumRuntime};
use postgres::{Client, Config, GenericClient, NoTls};

use super::{PostgresColumn, PostgresSourceError, PostgresSourceSpec, PostgresType, schema};

const CONNECTOR_CLASS: &str = "io.debezium.connector.postgresql.PostgresConnector";
const MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;

/// Ephemeral `PostgreSQL` credentials and the installed Debezium runtime bundle.
///
/// This pilot explicitly uses unencrypted `PostgreSQL` connections. Use it only
/// over a trusted local network or an independently secured tunnel. It is never
/// encoded into an Operation or Flow Definition; supply it again when opening.
pub struct PostgresSourceConfig {
    runtime_bundle: PathBuf,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl PostgresSourceConfig {
    /// Creates runtime configuration without connecting or opening the bundle.
    ///
    /// `PostgreSQL` TLS is disabled for both discovery and Debezium streaming.
    ///
    /// # Errors
    ///
    /// Rejects a relative bundle path, zero port, blank connection fields, or
    /// NUL bytes. The password may be empty for externally secured local access.
    pub fn new_unencrypted(
        runtime_bundle: impl Into<PathBuf>,
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, PostgresSourceError> {
        let config = Self {
            runtime_bundle: runtime_bundle.into(),
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        };
        if !config.runtime_bundle.is_absolute() || port == 0 {
            return Err(PostgresSourceError::new(
                "PostgreSQL runtime requires an absolute bundle path and nonzero port",
            ));
        }
        if [&config.host, &config.database, &config.user]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || config.password.contains('\0')
        {
            return Err(PostgresSourceError::new(
                "invalid PostgreSQL connection fields",
            ));
        }
        Ok(config)
    }

    /// Discovers one preconfigured table before constructing a Flow Definition.
    ///
    /// Reads catalog metadata only. Requires a permanent ordinary table with
    /// `REPLICA IDENTITY FULL`, a usable inactive `pgoutput` slot, and an
    /// existing unfiltered publication of every column and mutation kind. No
    /// table, publication, or slot is created or changed. The caller needs
    /// `EXECUTE` on `pg_control_system()` as well as normal CDC permissions.
    ///
    /// The first checkpoint starts at the existing slot, without a data
    /// snapshot. The caller must establish the matching initial relation; the
    /// simplest setup creates the slot while the captured table is empty.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection or catalog failures, incompatible
    /// replication settings, or unsupported table column types.
    pub fn discover(
        &self,
        engine_name: &str,
        table_schema: &str,
        table: &str,
        slot: &str,
        publication: &str,
    ) -> Result<PostgresSourceSpec, PostgresSourceError> {
        let mut client = self.connect()?;
        let mut transaction = client
            .build_transaction()
            .read_only(true)
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .start()
            .map_err(|error| catalog_error("begin discovery", &error))?;
        let spec = self.read_spec(
            &mut transaction,
            engine_name,
            table_schema,
            table,
            slot,
            publication,
        )?;
        transaction
            .commit()
            .map_err(|error| catalog_error("finish discovery", &error))?;
        Ok(spec)
    }

    pub(super) fn start(
        &self,
        expected: &PostgresSourceSpec,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Connector, PostgresSourceError> {
        let actual = self.discover(
            &expected.engine_name,
            &expected.schema,
            &expected.table,
            &expected.slot,
            &expected.publication,
        )?;
        if &actual != expected {
            return Err(PostgresSourceError::new(
                "PostgreSQL source identity or logical schema changed",
            ));
        }
        let runtime = DebeziumRuntime::open(&self.runtime_bundle).map_err(|error| {
            PostgresSourceError::new(format!("Debezium runtime open failed ({:?})", error.kind()))
        })?;
        runtime
            .start(self.connector_config(expected)?, checkpoint)
            .map_err(|error| {
                PostgresSourceError::new(format!(
                    "Debezium connector start failed ({:?})",
                    error.kind()
                ))
            })
    }

    fn connect(&self) -> Result<Client, PostgresSourceError> {
        Config::new()
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(&self.password)
            .connect_timeout(Duration::from_secs(5))
            .options("-c statement_timeout=5000 -c default_transaction_read_only=on")
            .connect(NoTls)
            .map_err(|error| catalog_error("connect", &error))
    }

    fn read_spec(
        &self,
        client: &mut impl GenericClient,
        engine_name: &str,
        table_schema: &str,
        table: &str,
        slot: &str,
        publication: &str,
    ) -> Result<PostgresSourceSpec, PostgresSourceError> {
        let identity = client.query_one(
            "SELECT s.system_identifier::text, d.oid FROM pg_catalog.pg_control_system() s CROSS JOIN pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()", &[])
            .map_err(|error| catalog_error("read cluster identity", &error))?;
        let relation = client.query_opt(
            "SELECT c.oid, c.relkind::text, c.relpersistence::text, c.relreplident::text, c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2", &[&table_schema, &table])
            .map_err(|error| catalog_error("read table identity", &error))?
            .ok_or_else(|| PostgresSourceError::new("PostgreSQL source table does not exist"))?;
        if relation.get::<_, String>(1) != "r"
            || relation.get::<_, String>(2) != "p"
            || relation.get::<_, String>(3) != "f"
            || relation.get::<_, bool>(4)
        {
            return Err(PostgresSourceError::new(
                "PostgreSQL source requires a permanent nonpartition table with REPLICA IDENTITY FULL",
            ));
        }
        let table_oid: u32 = relation.get(0);
        let rows = client.query(
            "SELECT attname, atttypid, atttypmod, NOT attnotnull, attgenerated::text FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum", &[&table_oid])
            .map_err(|error| catalog_error("read table columns", &error))?;
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            if !row.get::<_, String>(4).is_empty() {
                return Err(PostgresSourceError::new(
                    "generated PostgreSQL source columns are unsupported",
                ));
            }
            columns.push(PostgresColumn::new(
                row.get::<_, String>(0),
                column_type(row.get(1), row.get(2))?,
                row.get(3),
            ));
        }
        schema::compile(&columns)?;
        validate_publication(client, publication, table_schema, table, &columns)?;
        validate_slot(client, slot, &self.database)?;
        Ok(PostgresSourceSpec {
            engine_name: engine_name.to_owned(),
            database: self.database.clone(),
            schema: table_schema.to_owned(),
            table: table.to_owned(),
            slot: slot.to_owned(),
            publication: publication.to_owned(),
            system_identifier: identity.get(0),
            database_oid: identity.get(1),
            table_oid,
            columns,
        })
    }

    fn connector_config(
        &self,
        spec: &PostgresSourceSpec,
    ) -> Result<ConnectorConfig, PostgresSourceError> {
        let mut config = ConnectorConfig::new(&spec.engine_name, CONNECTOR_CLASS)
            .and_then(|config| config.max_delivery_bytes(MAX_DELIVERY_BYTES))
            .map_err(|_| PostgresSourceError::new("invalid PostgreSQL connector identity"))?;
        let port = self.port.to_string();
        // Definition identifiers are restricted to lowercase ASCII and '_'.
        let include = format!("{}\\.{}", spec.schema, spec.table);
        for (key, value) in [
            ("database.hostname", self.host.as_str()),
            ("database.port", &port),
            ("database.dbname", self.database.as_str()),
            ("database.user", self.user.as_str()),
            ("database.password", self.password.as_str()),
            ("database.sslmode", "disable"),
            ("plugin.name", "pgoutput"),
            ("topic.prefix", &spec.engine_name),
            ("slot.name", &spec.slot),
            ("publication.name", &spec.publication),
            ("table.include.list", &include),
            ("publication.autocreate.mode", "disabled"),
            ("slot.drop.on.stop", "false"),
            ("snapshot.mode", "no_data"),
            ("time.precision.mode", "microseconds"),
            ("decimal.handling.mode", "precise"),
            ("binary.handling.mode", "bytes"),
            ("tombstones.on.delete", "false"),
            ("provide.transaction.metadata", "false"),
            ("skipped.operations", "none"),
            ("heartbeat.interval.ms", "1000"),
            ("max.batch.size", "1024"),
            ("max.queue.size", "2048"),
            ("max.queue.size.in.bytes", "16777216"),
            ("poll.interval.ms", "100"),
            ("slot.max.retries", "0"),
            ("driver.connectTimeout", "5"),
            ("database.query.timeout.ms", "5000"),
            ("event.processing.failure.handling.mode", "fail"),
        ] {
            config = config.property(key, value).map_err(|_| {
                PostgresSourceError::new("invalid fixed PostgreSQL connector configuration")
            })?;
        }
        Ok(config)
    }
}

impl fmt::Debug for PostgresSourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSourceConfig")
            .field("runtime_bundle", &self.runtime_bundle)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .finish()
    }
}

fn validate_publication(
    client: &mut impl GenericClient,
    publication: &str,
    table_schema: &str,
    table: &str,
    columns: &[PostgresColumn],
) -> Result<(), PostgresSourceError> {
    let row = client.query_opt(
        "SELECT p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate, t.attnames::text[], t.rowfilter IS NULL FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_tables t ON t.pubname = p.pubname WHERE p.pubname = $1 AND t.schemaname = $2 AND t.tablename = $3",
        &[&publication, &table_schema, &table])
        .map_err(|error| catalog_error("read publication", &error))?
        .ok_or_else(|| PostgresSourceError::new("existing PostgreSQL publication does not include the source table"))?;
    let actual_columns: Vec<String> = row.get(1);
    if !row.get::<_, bool>(0)
        || !row.get::<_, bool>(2)
        || !actual_columns
            .iter()
            .map(String::as_str)
            .eq(columns.iter().map(PostgresColumn::name))
    {
        return Err(PostgresSourceError::new(
            "PostgreSQL publication must include all columns and insert/update/delete/truncate without a row filter",
        ));
    }
    Ok(())
}

fn validate_slot(
    client: &mut impl GenericClient,
    slot: &str,
    database: &str,
) -> Result<(), PostgresSourceError> {
    let row = client.query_opt(
        "SELECT plugin = 'pgoutput' AND slot_type = 'logical' AND database = $2 AND NOT temporary AND NOT active AND wal_status IN ('reserved', 'extended') AND NOT two_phase FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot, &database])
        .map_err(|error| catalog_error("read replication slot", &error))?
        .ok_or_else(|| PostgresSourceError::new("PostgreSQL source requires an existing pgoutput replication slot"))?;
    if row.get::<_, Option<bool>>(0) != Some(true) {
        return Err(PostgresSourceError::new(
            "PostgreSQL replication slot is active, incompatible, or no longer retains its WAL",
        ));
    }
    Ok(())
}

fn column_type(oid: u32, modifier: i32) -> Result<PostgresType, PostgresSourceError> {
    Ok(match oid {
        16 => PostgresType::Boolean,
        21 => PostgresType::Int16,
        23 => PostgresType::Int32,
        20 => PostgresType::Int64,
        700 => PostgresType::Float32,
        701 => PostgresType::Float64,
        25 | 1043 => PostgresType::Text,
        17 => PostgresType::Bytea,
        1082 => PostgresType::Date,
        1114 => PostgresType::Timestamp,
        1184 => PostgresType::TimestampTz,
        1700 if modifier >= 4 => {
            let modifier = u32::try_from(modifier - 4)
                .map_err(|_| PostgresSourceError::new("invalid PostgreSQL numeric modifier"))?;
            let precision = u8::try_from(modifier >> 16).map_err(|_| {
                PostgresSourceError::new("PostgreSQL numeric precision exceeds Decimal128")
            })?;
            // PostgreSQL numeric scale occupies a signed 11-bit field.
            let scale = i32::try_from(modifier & 0x7ff)
                .map_err(|_| PostgresSourceError::new("invalid PostgreSQL numeric scale"))?;
            let scale = if scale >= 1024 { scale - 2048 } else { scale };
            let scale = i8::try_from(scale)
                .map_err(|_| PostgresSourceError::new("PostgreSQL numeric scale is unsupported"))?;
            PostgresType::Numeric { precision, scale }
        }
        _ => {
            return Err(PostgresSourceError::new(format!(
                "unsupported PostgreSQL column type OID {oid}"
            )));
        }
    })
}

fn catalog_error(stage: &str, error: &postgres::Error) -> PostgresSourceError {
    PostgresSourceError::new(format!(
        "PostgreSQL {stage} failed (SQLSTATE {})",
        error
            .code()
            .map_or("unavailable", postgres::error::SqlState::code)
    ))
}
