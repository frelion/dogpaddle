use std::{fmt, path::PathBuf, time::Duration};

use dogpaddle_debezium::{Checkpoint, Connector, ConnectorConfig, DebeziumRuntime};
use postgres::{Client, Config, GenericClient, NoTls};

use super::{PostgresCdcScanError, PostgresCdcScanSpec, PostgresColumn, PostgresType, schema};

pub(super) const CONNECTOR_CLASS: &str = "io.debezium.connector.postgresql.PostgresConnector";
const MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
enum SlotState {
    Absent,
    Streaming,
}

#[derive(Clone, Copy)]
enum ConnectorMode {
    Snapshot,
    Streaming,
}

/// Ephemeral `PostgreSQL` credentials and the installed Debezium runtime bundle.
///
/// This pilot explicitly uses unencrypted `PostgreSQL` connections. Use it only
/// over a trusted local network or an independently secured tunnel. It is never
/// encoded into an Operation or Flow Definition; supply it again when opening.
pub struct PostgresCdcScanConfig {
    runtime_bundle: PathBuf,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl PostgresCdcScanConfig {
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
    ) -> Result<Self, PostgresCdcScanError> {
        let config = Self {
            runtime_bundle: runtime_bundle.into(),
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        };
        if !config.runtime_bundle.is_absolute() || port == 0 {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL runtime requires an absolute bundle path and nonzero port",
            ));
        }
        if [&config.host, &config.database, &config.user]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || config.password.contains('\0')
        {
            return Err(PostgresCdcScanError::new(
                "invalid PostgreSQL connection fields",
            ));
        }
        Ok(config)
    }

    /// Discovers one preconfigured table before constructing a Flow Definition.
    ///
    /// Reads catalog metadata only. Requires a permanent ordinary table with
    /// `REPLICA IDENTITY FULL`, an absent source-owned slot name, and an
    /// existing unfiltered publication of every column and mutation kind. No
    /// table, publication, or slot is created or changed during discovery. The
    /// caller needs `EXECUTE` on `pg_control_system()` as well as normal CDC
    /// and logical-slot creation permissions.
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
    ) -> Result<PostgresCdcScanSpec, PostgresCdcScanError> {
        self.inspect(
            engine_name,
            table_schema,
            table,
            slot,
            publication,
            SlotState::Absent,
        )
    }

    pub(super) fn start_snapshot(
        &self,
        expected: &PostgresCdcScanSpec,
    ) -> Result<Connector, PostgresCdcScanError> {
        self.verify(expected, SlotState::Absent)?;
        self.start(expected, ConnectorMode::Snapshot, None)
    }

    pub(super) fn start_streaming(
        &self,
        expected: &PostgresCdcScanSpec,
        checkpoint: &Checkpoint,
    ) -> Result<Connector, PostgresCdcScanError> {
        self.verify(expected, SlotState::Streaming)?;
        self.start(expected, ConnectorMode::Streaming, Some(checkpoint))
    }

    pub(super) fn drop_snapshot_slot(
        &self,
        expected: &PostgresCdcScanSpec,
    ) -> Result<(), PostgresCdcScanError> {
        let mut client = self.connect_management()?;
        // Capture may have failed precisely because the table or publication
        // drifted. Cleanup therefore binds only the immutable cluster/database
        // identity and the complete slot identity checked below.
        let identity = client
            .query_one(
                "SELECT s.system_identifier::text, d.oid FROM pg_catalog.pg_control_system() s CROSS JOIN pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()",
                &[],
            )
            .map_err(|error| catalog_error("read cluster identity for reset", &error))?;
        if identity.get::<_, String>(0).as_str() != expected.system_identifier.as_str()
            || identity.get::<_, u32>(1) != expected.database_oid
        {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL bootstrap reset reached a different cluster or database",
            ));
        }
        let Some(row) = client
            .query_opt(
                "SELECT plugin, slot_type, database, temporary, active, two_phase FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&expected.slot],
            )
            .map_err(|error| catalog_error("read replication slot for reset", &error))?
        else {
            return Ok(());
        };
        if row.get::<_, String>(0) != "pgoutput"
            || row.get::<_, String>(1) != "logical"
            || row.get::<_, Option<String>>(2).as_deref() != Some(expected.database.as_str())
            || row.get::<_, bool>(3)
            || row.get::<_, bool>(4)
            || row.get::<_, bool>(5)
        {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL bootstrap slot is active or incompatible",
            ));
        }
        let rows = client
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot($1)",
                &[&expected.slot],
            )
            .map_err(|error| catalog_error("drop bootstrap replication slot", &error))?;
        if rows.len() != 1 {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL bootstrap slot drop returned an unexpected result",
            ));
        }
        Ok(())
    }

    fn start(
        &self,
        expected: &PostgresCdcScanSpec,
        mode: ConnectorMode,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Connector, PostgresCdcScanError> {
        let runtime = DebeziumRuntime::open(&self.runtime_bundle).map_err(|error| {
            PostgresCdcScanError::new(format!("Debezium runtime open failed ({:?})", error.kind()))
        })?;
        runtime
            .start(self.connector_config(expected, mode)?, checkpoint)
            .map_err(|error| {
                PostgresCdcScanError::new(format!(
                    "Debezium connector start failed ({:?})",
                    error.kind()
                ))
            })
    }

    fn connect_read_only(&self) -> Result<Client, PostgresCdcScanError> {
        self.connect("-c statement_timeout=5000 -c default_transaction_read_only=on")
    }

    fn connect_management(&self) -> Result<Client, PostgresCdcScanError> {
        self.connect("-c statement_timeout=5000")
    }

    fn connect(&self, options: &str) -> Result<Client, PostgresCdcScanError> {
        Config::new()
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(&self.password)
            .connect_timeout(Duration::from_secs(5))
            .options(options)
            .connect(NoTls)
            .map_err(|error| catalog_error("connect", &error))
    }

    fn inspect(
        &self,
        engine_name: &str,
        table_schema: &str,
        table: &str,
        slot: &str,
        publication: &str,
        slot_state: SlotState,
    ) -> Result<PostgresCdcScanSpec, PostgresCdcScanError> {
        let mut client = self.connect_read_only()?;
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
            slot_state,
        )?;
        transaction
            .commit()
            .map_err(|error| catalog_error("finish discovery", &error))?;
        Ok(spec)
    }

    fn verify(
        &self,
        expected: &PostgresCdcScanSpec,
        slot_state: SlotState,
    ) -> Result<(), PostgresCdcScanError> {
        let actual = self.inspect(
            &expected.engine_name,
            &expected.schema,
            &expected.table,
            &expected.slot,
            &expected.publication,
            slot_state,
        )?;
        if &actual != expected {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL CDC scan identity or logical schema changed",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn read_spec(
        &self,
        client: &mut impl GenericClient,
        engine_name: &str,
        table_schema: &str,
        table: &str,
        slot: &str,
        publication: &str,
        slot_state: SlotState,
    ) -> Result<PostgresCdcScanSpec, PostgresCdcScanError> {
        let identity = client.query_one(
            "SELECT s.system_identifier::text, d.oid FROM pg_catalog.pg_control_system() s CROSS JOIN pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()", &[])
            .map_err(|error| catalog_error("read cluster identity", &error))?;
        let relation = client.query_opt(
            "SELECT c.oid, c.relkind::text, c.relpersistence::text, c.relreplident::text, c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2", &[&table_schema, &table])
            .map_err(|error| catalog_error("read table identity", &error))?
            .ok_or_else(|| {
                PostgresCdcScanError::new("PostgreSQL CDC captured table does not exist")
            })?;
        if relation.get::<_, String>(1) != "r"
            || relation.get::<_, String>(2) != "p"
            || relation.get::<_, String>(3) != "f"
            || relation.get::<_, bool>(4)
        {
            return Err(PostgresCdcScanError::new(
                "PostgreSQL CDC scan requires a permanent nonpartition table with REPLICA IDENTITY FULL",
            ));
        }
        let table_oid: u32 = relation.get(0);
        let rows = client.query(
            "SELECT attname, atttypid, atttypmod, NOT attnotnull, attgenerated::text FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum", &[&table_oid])
            .map_err(|error| catalog_error("read table columns", &error))?;
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            if !row.get::<_, String>(4).is_empty() {
                return Err(PostgresCdcScanError::new(
                    "generated PostgreSQL CDC scan columns are unsupported",
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
        match slot_state {
            SlotState::Absent => validate_absent_slot(client, slot)?,
            SlotState::Streaming => validate_streaming_slot(client, slot, &self.database)?,
        }
        Ok(PostgresCdcScanSpec {
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
        spec: &PostgresCdcScanSpec,
        mode: ConnectorMode,
    ) -> Result<ConnectorConfig, PostgresCdcScanError> {
        let mut config = ConnectorConfig::new(&spec.engine_name, CONNECTOR_CLASS)
            .and_then(|config| config.max_delivery_bytes(MAX_DELIVERY_BYTES))
            .map_err(|_| PostgresCdcScanError::new("invalid PostgreSQL connector identity"))?;
        let port = self.port.to_string();
        let snapshot_mode = match mode {
            ConnectorMode::Snapshot => "initial",
            ConnectorMode::Streaming => "no_data",
        };
        let heartbeat_interval = match mode {
            ConnectorMode::Snapshot => "1",
            ConnectorMode::Streaming => "1000",
        };
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
            ("snapshot.mode", snapshot_mode),
            ("snapshot.max.threads", "1"),
            ("lsn.flush.mode", "connector"),
            ("time.precision.mode", "microseconds"),
            ("decimal.handling.mode", "precise"),
            ("binary.handling.mode", "bytes"),
            ("tombstones.on.delete", "false"),
            ("provide.transaction.metadata", "false"),
            ("skipped.operations", "none"),
            ("heartbeat.interval.ms", heartbeat_interval),
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
                PostgresCdcScanError::new("invalid fixed PostgreSQL connector configuration")
            })?;
        }
        if matches!(mode, ConnectorMode::Snapshot) {
            let notification_topic = format!("__dogpaddle-notification.{}", spec.engine_name);
            for (key, value) in [
                ("notification.enabled.channels", "sink"),
                ("notification.sink.topic.name", notification_topic.as_str()),
            ] {
                config = config.property(key, value).map_err(|_| {
                    PostgresCdcScanError::new("invalid fixed PostgreSQL connector configuration")
                })?;
            }
        }
        Ok(config)
    }
}

impl fmt::Debug for PostgresCdcScanConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresCdcScanConfig")
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
) -> Result<(), PostgresCdcScanError> {
    let row = client.query_opt(
        "SELECT p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate, t.attnames::text[], t.rowfilter IS NULL FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_publication_tables t ON t.pubname = p.pubname WHERE p.pubname = $1 AND t.schemaname = $2 AND t.tablename = $3",
        &[&publication, &table_schema, &table])
        .map_err(|error| catalog_error("read publication", &error))?
        .ok_or_else(|| PostgresCdcScanError::new("existing PostgreSQL publication does not include the captured table"))?;
    let actual_columns: Vec<String> = row.get(1);
    if !row.get::<_, bool>(0)
        || !row.get::<_, bool>(2)
        || !actual_columns
            .iter()
            .map(String::as_str)
            .eq(columns.iter().map(PostgresColumn::name))
    {
        return Err(PostgresCdcScanError::new(
            "PostgreSQL publication must include all columns and insert/update/delete/truncate without a row filter",
        ));
    }
    Ok(())
}

fn validate_absent_slot(
    client: &mut impl GenericClient,
    slot: &str,
) -> Result<(), PostgresCdcScanError> {
    if client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .map_err(|error| catalog_error("read replication slot", &error))?
        .is_some()
    {
        return Err(PostgresCdcScanError::new(
            "PostgreSQL bootstrap requires its source-owned replication slot name to be absent",
        ));
    }
    Ok(())
}

fn validate_streaming_slot(
    client: &mut impl GenericClient,
    slot: &str,
    database: &str,
) -> Result<(), PostgresCdcScanError> {
    let row = client.query_opt(
        "SELECT plugin = 'pgoutput' AND slot_type = 'logical' AND database = $2 AND NOT temporary AND NOT active AND wal_status IN ('reserved', 'extended') AND NOT two_phase FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot, &database])
        .map_err(|error| catalog_error("read replication slot", &error))?
        .ok_or_else(|| {
            PostgresCdcScanError::new(
                "PostgreSQL CDC continuation requires its bootstrap pgoutput replication slot",
            )
        })?;
    if row.get::<_, Option<bool>>(0) != Some(true) {
        return Err(PostgresCdcScanError::new(
            "PostgreSQL replication slot is active, incompatible, or no longer retains its WAL",
        ));
    }
    Ok(())
}

fn column_type(oid: u32, modifier: i32) -> Result<PostgresType, PostgresCdcScanError> {
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
                .map_err(|_| PostgresCdcScanError::new("invalid PostgreSQL numeric modifier"))?;
            let precision = u8::try_from(modifier >> 16).map_err(|_| {
                PostgresCdcScanError::new("PostgreSQL numeric precision exceeds Decimal128")
            })?;
            // PostgreSQL numeric scale occupies a signed 11-bit field.
            let scale = i32::try_from(modifier & 0x7ff)
                .map_err(|_| PostgresCdcScanError::new("invalid PostgreSQL numeric scale"))?;
            let scale = if scale >= 1024 { scale - 2048 } else { scale };
            let scale = i8::try_from(scale).map_err(|_| {
                PostgresCdcScanError::new("PostgreSQL numeric scale is unsupported")
            })?;
            PostgresType::Numeric { precision, scale }
        }
        _ => {
            return Err(PostgresCdcScanError::new(format!(
                "unsupported PostgreSQL column type OID {oid}"
            )));
        }
    })
}

fn catalog_error(stage: &str, error: &postgres::Error) -> PostgresCdcScanError {
    PostgresCdcScanError::new(format!(
        "PostgreSQL {stage} failed (SQLSTATE {})",
        error
            .code()
            .map_or("unavailable", postgres::error::SqlState::code)
    ))
}
