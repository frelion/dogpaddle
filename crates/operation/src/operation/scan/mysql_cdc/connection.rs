use std::{
    fmt,
    path::PathBuf,
    time::{Duration, Instant},
};

use dogpaddle_debezium::{Checkpoint, Connector, ConnectorConfig, DebeziumRuntime};
use mysql::{Conn, OptsBuilder, params, prelude::Queryable};

use super::definition::{CONNECTOR_CLASS, validate_spec};
use super::{
    MySqlCdcScanDefinition, MySqlCdcScanError, MySqlCdcScanSpec, MySqlColumn, MySqlType,
    convert::{BootstrapControl, validate_bootstrap_control},
    schema,
};

const MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;
const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
type ColumnRow = (
    String,
    String,
    String,
    String,
    Option<u64>,
    Option<u64>,
    String,
    String,
);

/// Ephemeral `MySQL` credentials and the installed Debezium runtime bundle.
///
/// This pilot explicitly uses unencrypted `MySQL` connections. Use it only
/// over a trusted local network or an independently secured tunnel. It is never
/// encoded into an Operation or Flow Definition; supply it again when opening.
pub struct MySqlCdcScanConfig {
    runtime_bundle: PathBuf,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    replication_client_id: u32,
}

impl MySqlCdcScanConfig {
    /// Creates runtime configuration without connecting or opening the bundle.
    ///
    /// `MySQL` TLS is disabled for both discovery and Debezium streaming.
    ///
    /// # Errors
    ///
    /// Rejects a relative bundle path, zero port or replication client ID,
    /// blank connection fields, or NUL bytes. The password may be empty for
    /// externally secured local access.
    pub fn new_unencrypted(
        runtime_bundle: impl Into<PathBuf>,
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        replication_client_id: u32,
    ) -> Result<Self, MySqlCdcScanError> {
        let config = Self {
            runtime_bundle: runtime_bundle.into(),
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
            replication_client_id,
        };
        if !config.runtime_bundle.is_absolute() || port == 0 || replication_client_id == 0 {
            return Err(MySqlCdcScanError::new(
                "MySQL runtime requires an absolute bundle path, nonzero port, and nonzero replication client ID",
            ));
        }
        if [&config.host, &config.database, &config.user]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || config.password.contains('\0')
        {
            return Err(MySqlCdcScanError::new("invalid MySQL connection fields"));
        }
        Ok(config)
    }

    /// Discovers one preconfigured table before constructing a Flow Definition.
    ///
    /// Reads catalog metadata only. Requires a single permanent, nonpartitioned
    /// `InnoDB` table; binary logging with row events and full row images; and
    /// access to `INFORMATION_SCHEMA.INNODB_TABLES` for the table identity. No
    /// source object is created or changed. The replication client ID must be
    /// unique among active `MySQL` replication clients.
    ///
    /// This is catalog-only discovery. Normal callers should use
    /// [`Self::bootstrap_definition`], which also captures a pre-publication
    /// CDC seed. It does not establish an initial relation: callers that need a
    /// new sink to be a full mirror must keep the source empty through a
    /// successful build, or use a future snapshot source.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection or catalog failures,
    /// incompatible binlog settings, or unsupported table column types.
    pub fn discover(
        &self,
        engine_name: &str,
        table: &str,
    ) -> Result<MySqlCdcScanSpec, MySqlCdcScanError> {
        let mut connection = self.connect()?;
        connection
            .query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .map_err(|_| catalog_error("set discovery isolation"))?;
        connection
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .map_err(|_| catalog_error("begin discovery"))?;
        let spec = self
            .read_spec(&mut connection, engine_name, table)
            .and_then(|spec| {
                validate_spec(&spec)?;
                Ok(spec)
            });
        let finish = connection.query_drop(if spec.is_ok() { "COMMIT" } else { "ROLLBACK" });
        match (spec, finish) {
            (Ok(spec), Ok(())) => Ok(spec),
            (Ok(_), Err(_)) => Err(catalog_error("finish discovery")),
            (Err(error), _) => Err(error),
        }
    }

    /// Discovers one table and captures its pre-publication Debezium tail seed.
    ///
    /// This performs one short `no_data` Debezium snapshot outside Flow build.
    /// It consumes only snapshot schema-control deliveries until their final
    /// completion marker, confirms the completed checkpoint through the
    /// bridge, stops and disposes that temporary connector, and embeds the
    /// checkpoint into the returned Definition. The seed becomes durable only
    /// when that canonical Definition is committed by a Flow build. A later
    /// runtime then always starts in `recovery` mode from the Definition seed
    /// or a newer checkpoint Cell value.
    ///
    /// The schema-only snapshot intentionally does not emit table rows. Its
    /// native binlog position is the CDC origin, not an atomic cut at the start
    /// of this method: changes at or before that position are outside this
    /// Scan's contract. Changes strictly after it are replayed after
    /// publication when the binlog remains retained.
    ///
    /// # Errors
    ///
    /// Returns an error for catalog discovery, bundle startup, an unexpected
    /// snapshot delivery, an incomplete schema bootstrap, checkpoint
    /// acknowledgement, or connector shutdown failure. No Flow has been
    /// published when this method returns an error.
    pub fn bootstrap_definition(
        &self,
        engine_name: &str,
        table: &str,
    ) -> Result<MySqlCdcScanDefinition, MySqlCdcScanError> {
        let spec = self.discover(engine_name, table)?;
        let checkpoint = self.bootstrap_checkpoint(&spec)?;
        let observed = self.discover(engine_name, table)?;
        if observed != spec {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC scan identity or logical schema changed during bootstrap",
            ));
        }
        MySqlCdcScanDefinition::from_bootstrap(spec, checkpoint)
    }

    pub(super) fn start(
        &self,
        expected: &MySqlCdcScanSpec,
        checkpoint: &Checkpoint,
    ) -> Result<Connector, MySqlCdcScanError> {
        let actual = self.discover(&expected.engine_name, &expected.table)?;
        if &actual != expected {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC scan identity or logical schema changed",
            ));
        }
        let runtime = DebeziumRuntime::open(&self.runtime_bundle).map_err(|error| {
            MySqlCdcScanError::new(format!("Debezium runtime open failed ({:?})", error.kind()))
        })?;
        runtime
            .start(
                self.connector_config(expected, ConnectorMode::Recovery)?,
                Some(checkpoint),
            )
            .map_err(|error| {
                MySqlCdcScanError::new(format!(
                    "Debezium connector start failed ({:?})",
                    error.kind()
                ))
            })
    }

    fn bootstrap_checkpoint(
        &self,
        spec: &MySqlCdcScanSpec,
    ) -> Result<Checkpoint, MySqlCdcScanError> {
        let runtime = DebeziumRuntime::open(&self.runtime_bundle).map_err(|error| {
            MySqlCdcScanError::new(format!("Debezium runtime open failed ({:?})", error.kind()))
        })?;
        let mut connector = runtime
            .start(self.connector_config(spec, ConnectorMode::Bootstrap)?, None)
            .map_err(|error| {
                MySqlCdcScanError::new(format!(
                    "Debezium bootstrap start failed ({:?})",
                    error.kind()
                ))
            })?;

        let checkpoint: Result<Checkpoint, MySqlCdcScanError> = (|| {
            let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(MySqlCdcScanError::new(
                        "Debezium bootstrap did not complete its schema snapshot within 30 seconds",
                    ));
                }
                let delivery = match connector.poll(remaining) {
                    Ok(Some(delivery)) => delivery,
                    Ok(None) => {
                        return Err(MySqlCdcScanError::new(
                            "Debezium bootstrap did not complete its schema snapshot within 30 seconds",
                        ));
                    }
                    Err(error) => {
                        return Err(MySqlCdcScanError::new(format!(
                            "Debezium bootstrap poll failed ({:?})",
                            error.kind()
                        )));
                    }
                };
                let complete = matches!(
                    validate_bootstrap_control(delivery.records(), &spec.engine_name)?,
                    BootstrapControl::Complete
                );
                let checkpoint = delivery.checkpoint().clone();
                delivery.ack().map_err(|error| {
                    MySqlCdcScanError::new(format!(
                        "Debezium bootstrap ACK failed ({:?})",
                        error.kind()
                    ))
                })?;
                if complete {
                    return Ok(checkpoint);
                }
            }
        })();
        let stopped = connector.stop(BOOTSTRAP_TIMEOUT).map_err(|error| {
            MySqlCdcScanError::new(format!(
                "Debezium bootstrap stop failed ({:?})",
                error.kind()
            ))
        });
        match (checkpoint, stopped) {
            (Ok(checkpoint), Ok(())) => Ok(checkpoint),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn connect(&self) -> Result<Conn, MySqlCdcScanError> {
        let options = OptsBuilder::new()
            .ip_or_hostname(Some(self.host.clone()))
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(DATABASE_TIMEOUT))
            .read_timeout(Some(DATABASE_TIMEOUT))
            .write_timeout(Some(DATABASE_TIMEOUT));
        Conn::new(options).map_err(|_| catalog_error("connect"))
    }

    fn read_spec(
        &self,
        connection: &mut Conn,
        engine_name: &str,
        table: &str,
    ) -> Result<MySqlCdcScanSpec, MySqlCdcScanError> {
        let server: Option<(String, String, String, String, u8)> = connection
            .query_first(
                "SELECT @@server_uuid, @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.lower_case_table_names",
            )
            .map_err(|_| catalog_error("read server settings"))?;
        let Some((server_uuid, log_bin, binlog_format, row_image, lower_case_table_names)) = server
        else {
            return Err(MySqlCdcScanError::new(
                "MySQL server settings are unavailable",
            ));
        };
        if !log_bin.eq_ignore_ascii_case("ON")
            || !binlog_format.eq_ignore_ascii_case("ROW")
            || !row_image.eq_ignore_ascii_case("FULL")
            || lower_case_table_names != 0
        {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC scan requires log_bin=ON, ROW binlog format, FULL row image, and lower_case_table_names=0",
            ));
        }

        let table_properties: Option<(String, String)> = connection
            .exec_first(
                "SELECT ENGINE, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table",
                params! { "database" => self.database.as_str(), "table" => table },
            )
            .map_err(|_| catalog_error("read table identity"))?;
        let Some((engine, table_type)) = table_properties else {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC captured table does not exist",
            ));
        };
        if !engine.eq_ignore_ascii_case("InnoDB") || table_type != "BASE TABLE" {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC scan requires one permanent InnoDB base table",
            ));
        }
        let partitions: Option<u64> = connection
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table AND PARTITION_NAME IS NOT NULL",
                params! { "database" => self.database.as_str(), "table" => table },
            )
            .map_err(|_| catalog_error("read table partitions"))?;
        if partitions != Some(0) {
            return Err(MySqlCdcScanError::new(
                "MySQL CDC scan does not support partitioned tables",
            ));
        }
        let table_name = format!("{}/{}", self.database, table);
        let table_id: Option<u64> = connection
            .exec_first(
                "SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = :name",
                params! { "name" => table_name },
            )
            .map_err(|_| catalog_error("read InnoDB table identity"))?;
        let table_id = table_id.ok_or_else(|| {
            MySqlCdcScanError::new("MySQL CDC scan could not resolve the InnoDB table identity")
        })?;

        let columns = self.read_columns(connection, table)?;
        schema::compile(&columns)?;
        Ok(MySqlCdcScanSpec {
            engine_name: engine_name.to_owned(),
            database: self.database.clone(),
            table: table.to_owned(),
            server_uuid,
            table_id,
            columns,
        })
    }

    fn read_columns(
        &self,
        connection: &mut Conn,
        table: &str,
    ) -> Result<Vec<MySqlColumn>, MySqlCdcScanError> {
        let rows: Vec<ColumnRow> = connection
            .exec(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, EXTRA, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table ORDER BY ORDINAL_POSITION",
                params! { "database" => self.database.as_str(), "table" => table },
            )
            .map_err(|_| catalog_error("read table columns"))?;
        let mut columns = Vec::with_capacity(rows.len());
        for (name, data_type, column_definition, nullable, precision, scale, extra, generation) in
            rows
        {
            if extra.to_ascii_uppercase().contains("GENERATED")
                || extra.to_ascii_uppercase().contains("INVISIBLE")
                || !generation.is_empty()
            {
                return Err(MySqlCdcScanError::new(
                    "generated or invisible MySQL CDC scan columns are unsupported",
                ));
            }
            columns.push(MySqlColumn::new(
                name,
                column_type(&data_type, &column_definition, precision, scale)?,
                match nullable.as_str() {
                    "YES" => true,
                    "NO" => false,
                    _ => return Err(MySqlCdcScanError::new("invalid MySQL column nullability")),
                },
            ));
        }
        Ok(columns)
    }

    fn connector_config(
        &self,
        spec: &MySqlCdcScanSpec,
        mode: ConnectorMode,
    ) -> Result<ConnectorConfig, MySqlCdcScanError> {
        let mut config = ConnectorConfig::new(&spec.engine_name, CONNECTOR_CLASS)
            .and_then(|config| config.max_delivery_bytes(MAX_DELIVERY_BYTES))
            .map_err(|_| MySqlCdcScanError::new("invalid MySQL connector identity"))?;
        let port = self.port.to_string();
        let replication_client_id = self.replication_client_id.to_string();
        let snapshot_mode = mode.snapshot_mode();
        let (heartbeat_interval, batch_size, queue_size) = match mode {
            ConnectorMode::Bootstrap => ("0", "1", "2"),
            ConnectorMode::Recovery => ("1000", "1024", "2048"),
        };
        // Definition identifiers are restricted to lowercase ASCII and '_'.
        let include = format!("^{}\\.{}$", spec.database, spec.table);
        for (key, value) in [
            ("database.hostname", self.host.as_str()),
            ("database.port", &port),
            ("database.user", self.user.as_str()),
            ("database.password", self.password.as_str()),
            ("database.ssl.mode", "disabled"),
            ("database.server.id", &replication_client_id),
            ("topic.prefix", &spec.engine_name),
            ("database.include.list", &spec.database),
            ("table.include.list", &include),
            // The temporary bootstrap prepares the pre-publication tail seed.
            // Every materialized Scan starts in `recovery` only after a Flow
            // Definition has durably published that seed, or a newer mutable
            // checkpoint Cell value exists.
            ("snapshot.mode", snapshot_mode),
            // This CDC-only pilot never reads table data. Fixed Schema lets
            // its schema-only snapshots avoid deliberate MySQL read locks.
            // That does not turn invocation time into a source-write fence:
            // pre-origin state remains outside the CDC-only contract.
            ("snapshot.locking.mode", "none"),
            (
                "schema.history.internal",
                "io.debezium.relational.history.MemorySchemaHistory",
            ),
            // The temporary `no_data` bootstrap accepts only snapshot schema
            // controls through its final completion marker. Runtime recovery
            // rejects streaming DDL before it can be acknowledged.
            ("include.schema.changes", "true"),
            (
                "schema.history.internal.store.only.captured.databases.ddl",
                "true",
            ),
            (
                "schema.history.internal.store.only.captured.tables.ddl",
                "true",
            ),
            ("schema.history.internal.skip.unparseable.ddl", "false"),
            ("decimal.handling.mode", "precise"),
            ("bigint.unsigned.handling.mode", "precise"),
            ("binary.handling.mode", "bytes"),
            ("tombstones.on.delete", "false"),
            ("provide.transaction.metadata", "false"),
            ("skipped.operations", "none"),
            ("heartbeat.interval.ms", heartbeat_interval),
            ("max.batch.size", batch_size),
            ("max.queue.size", queue_size),
            ("max.queue.size.in.bytes", "16777216"),
            ("poll.interval.ms", "100"),
            ("database.connectionTimeZone", "UTC"),
            ("event.processing.failure.handling.mode", "fail"),
        ] {
            config = config.property(key, value).map_err(|_| {
                MySqlCdcScanError::new("invalid fixed MySQL connector configuration")
            })?;
        }
        Ok(config)
    }
}

#[derive(Clone, Copy)]
enum ConnectorMode {
    Bootstrap,
    Recovery,
}

impl ConnectorMode {
    const fn snapshot_mode(self) -> &'static str {
        match self {
            Self::Bootstrap => "no_data",
            Self::Recovery => "recovery",
        }
    }
}

impl fmt::Debug for MySqlCdcScanConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MySqlCdcScanConfig")
            .field("runtime_bundle", &self.runtime_bundle)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .field("replication_client_id", &self.replication_client_id)
            .finish()
    }
}

fn column_type(
    data_type: &str,
    definition: &str,
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<MySqlType, MySqlCdcScanError> {
    let definition = definition.to_ascii_lowercase();
    if definition.contains("unsigned") || definition.contains("zerofill") {
        return Err(MySqlCdcScanError::new(
            "unsigned and zerofill MySQL CDC scan columns are unsupported",
        ));
    }
    match data_type {
        "tinyint" | "smallint" => Ok(MySqlType::Int16),
        "mediumint" | "int" | "integer" => Ok(MySqlType::Int32),
        "bigint" => Ok(MySqlType::Int64),
        "double" => Ok(MySqlType::Float64),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => Ok(MySqlType::Text),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            Ok(MySqlType::Binary)
        }
        "decimal" => {
            let precision = precision
                .and_then(|value| u8::try_from(value).ok())
                .filter(|value| (1..=38).contains(value))
                .ok_or_else(|| MySqlCdcScanError::new("MySQL decimal precision is unsupported"))?;
            let scale = scale
                .and_then(|value| i8::try_from(value).ok())
                .filter(|value| {
                    *value >= 0 && u8::try_from(*value).is_ok_and(|value| value <= precision)
                })
                .ok_or_else(|| MySqlCdcScanError::new("MySQL decimal scale is unsupported"))?;
            Ok(MySqlType::Decimal { precision, scale })
        }
        _ => Err(MySqlCdcScanError::new(format!(
            "unsupported MySQL column type {data_type}"
        ))),
    }
}

fn catalog_error(stage: &str) -> MySqlCdcScanError {
    MySqlCdcScanError::new(format!("MySQL {stage} failed"))
}

#[cfg(test)]
mod tests {
    use super::ConnectorMode;

    #[test]
    fn only_temporary_bootstrap_uses_no_data_snapshot_mode() {
        assert_eq!(ConnectorMode::Bootstrap.snapshot_mode(), "no_data");
        assert_eq!(ConnectorMode::Recovery.snapshot_mode(), "recovery");
    }
}
