use std::{fmt, num::NonZeroU32, path::PathBuf, time::Duration};

use dogpaddle_debezium::{Checkpoint, Connector, ConnectorConfig, DebeziumRuntime, ErrorKind};
use mysql::{Conn, OptsBuilder, params, prelude::Queryable};

use super::definition::{CONNECTOR_CLASS, validate_spec};
use super::{MySqlCdcScanError, MySqlCdcScanSpec, MySqlColumn, MySqlType, schema};

const MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;
const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CONNECT_TIMEOUT_MS: i32 = 30_000;
const DEFAULT_QUERY_TIMEOUT_MS: i32 = 600_000;
const MAX_JDBC_QUERY_TIMEOUT_MS: i32 = i32::MAX / 1_000 * 1_000;
const DEFAULT_RETRY_LIMIT: i32 = -1;
const RETRY_INITIAL_DELAY_MS: i32 = 300;
const DEFAULT_RETRY_MAX_DELAY_MS: i32 = 10_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: i32 = 1_000;
const SNAPSHOT_HEARTBEAT_INTERVAL_MS: i32 = 1;
const REPLICATION_CLIENT_ID_CONTEXT: &str =
    "dogpaddle MySQL CDC replication client ID derived from engine name v1";
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

/// Runtime tuning for a [`MySqlCdcScanConfig`].
///
/// Defaults preserve the fixed discovery bounds and Debezium connector
/// behavior used by the Scan. These options are ephemeral: they are neither
/// encoded in the Operation Definition nor persisted in Flow state, so supply
/// the desired values again when reopening a Flow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MySqlCdcScanOptions {
    discovery_connect_timeout: Duration,
    discovery_query_timeout: Duration,
    connect_timeout_ms: i32,
    query_timeout_ms: i32,
    retry_limit: i32,
    retry_max_delay_ms: i32,
    heartbeat_interval_ms: i32,
    snapshot_fetch_size: Option<i32>,
}

impl MySqlCdcScanOptions {
    /// Creates options with the Scan's stable runtime defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            discovery_connect_timeout: DATABASE_TIMEOUT,
            discovery_query_timeout: DATABASE_TIMEOUT,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            query_timeout_ms: DEFAULT_QUERY_TIMEOUT_MS,
            retry_limit: DEFAULT_RETRY_LIMIT,
            retry_max_delay_ms: DEFAULT_RETRY_MAX_DELAY_MS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            snapshot_fetch_size: None,
        }
    }

    /// Sets both discovery and Debezium connection timeouts.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values whose
    /// millisecond count exceeds a Java signed 32-bit integer.
    pub fn connect_timeout(mut self, timeout: Duration) -> Result<Self, MySqlCdcScanError> {
        self.connect_timeout_ms = positive_milliseconds("connect_timeout", timeout)?;
        self.discovery_connect_timeout = timeout;
        Ok(self)
    }

    /// Sets both discovery socket and Debezium query timeouts.
    ///
    /// Debezium 3.6 ultimately gives JDBC whole seconds, so a sub-second
    /// remainder is rounded up for the connector while native discovery keeps
    /// the exact duration.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values greater than
    /// 2,147,483,000 milliseconds.
    pub fn query_timeout(mut self, timeout: Duration) -> Result<Self, MySqlCdcScanError> {
        let milliseconds = positive_milliseconds("query timeout", timeout)?;
        if milliseconds > MAX_JDBC_QUERY_TIMEOUT_MS {
            return Err(MySqlCdcScanError::invalid_options(
                "query timeout exceeds 2,147,483,000 milliseconds",
            ));
        }
        self.query_timeout_ms = milliseconds;
        self.discovery_query_timeout = timeout;
        Ok(self)
    }

    /// Sets the finite number of Debezium retryable polling-failure retries.
    ///
    /// Zero disables retries. Leaving this option unset preserves Debezium's
    /// unlimited `-1` default. This setting applies after connector startup;
    /// it does not govern initial task startup.
    ///
    /// # Errors
    ///
    /// Rejects values greater than a Java signed 32-bit integer.
    pub fn retry_limit(mut self, limit: u32) -> Result<Self, MySqlCdcScanError> {
        self.retry_limit = i32::try_from(limit).map_err(|_| {
            MySqlCdcScanError::invalid_options("retry limit exceeds Java Integer.MAX_VALUE")
        })?;
        Ok(self)
    }

    /// Sets the maximum delay between Debezium post-start polling retries.
    ///
    /// # Errors
    ///
    /// Rejects durations that are not whole milliseconds, exceed a Java
    /// signed 32-bit integer, or are at most the fixed 300 ms initial delay.
    pub fn retry_max_delay(mut self, delay: Duration) -> Result<Self, MySqlCdcScanError> {
        let milliseconds = positive_milliseconds("retry_max_delay", delay)?;
        if milliseconds <= RETRY_INITIAL_DELAY_MS {
            return Err(MySqlCdcScanError::invalid_options(
                "maximum retry delay must be greater than 300 milliseconds",
            ));
        }
        self.retry_max_delay_ms = milliseconds;
        Ok(self)
    }

    /// Sets the streaming heartbeat interval.
    ///
    /// Snapshot capture retains its fixed 1 ms heartbeat so that bootstrap
    /// checkpoint behavior is independent of runtime tuning.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values whose
    /// millisecond count exceeds a Java signed 32-bit integer.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Result<Self, MySqlCdcScanError> {
        self.heartbeat_interval_ms = positive_milliseconds("heartbeat_interval", interval)?;
        Ok(self)
    }

    /// Sets the maximum rows in one Debezium snapshot fetch.
    ///
    /// By default the property is omitted completely, retaining the `MySQL`
    /// connector's special streaming-result behavior.
    ///
    /// # Errors
    ///
    /// Rejects values greater than a Java signed 32-bit integer.
    pub fn snapshot_fetch_size(mut self, rows: NonZeroU32) -> Result<Self, MySqlCdcScanError> {
        self.snapshot_fetch_size = Some(i32::try_from(rows.get()).map_err(|_| {
            MySqlCdcScanError::invalid_options("snapshot fetch size exceeds Java Integer.MAX_VALUE")
        })?);
        Ok(self)
    }
}

impl Default for MySqlCdcScanOptions {
    fn default() -> Self {
        Self::new()
    }
}

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
    options: MySqlCdcScanOptions,
}

impl MySqlCdcScanConfig {
    /// Creates runtime configuration without connecting or opening the bundle.
    ///
    /// `MySQL` TLS is disabled for both discovery and Debezium streaming.
    ///
    /// # Errors
    ///
    /// Rejects a relative bundle path, zero port, blank connection fields, or
    /// NUL bytes. The password may be empty for externally secured local
    /// access.
    pub fn new_unencrypted(
        runtime_bundle: impl Into<PathBuf>,
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, MySqlCdcScanError> {
        let config = Self {
            runtime_bundle: runtime_bundle.into(),
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
            options: MySqlCdcScanOptions::new(),
        };
        if !config.runtime_bundle.is_absolute() || port == 0 {
            return Err(MySqlCdcScanError::new(
                "MySQL runtime requires an absolute bundle path and nonzero port",
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

    /// Uses the supplied ephemeral runtime tuning.
    #[must_use]
    pub const fn options(mut self, options: MySqlCdcScanOptions) -> Self {
        self.options = options;
        self
    }

    /// Discovers one preconfigured table before constructing a Flow Definition.
    ///
    /// Reads catalog metadata only. Requires a single permanent, nonpartitioned
    /// `InnoDB` table; binary logging with row events and full row images; and
    /// access to `INFORMATION_SCHEMA.INNODB_TABLES` for the table identity. No
    /// source object is created or changed. The replication client ID is
    /// derived from the persisted engine name.
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
        let binlog_status: Option<mysql::Row> = connection
            .query_first("SHOW BINARY LOG STATUS")
            .map_err(|_| catalog_error("read binary log status"))?;
        let binlog_status = binlog_status
            .ok_or_else(|| MySqlCdcScanError::new("MySQL binary log status is unavailable"))?;
        let file = binlog_status.get::<String, _>(0).unwrap_or_default();
        let position = binlog_status.get::<u64, _>(1).unwrap_or_default();
        if file.is_empty() || position == 0 {
            return Err(MySqlCdcScanError::new(
                "MySQL binary log position is unavailable",
            ));
        }
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

    pub(super) fn start_snapshot(
        &self,
        expected: &MySqlCdcScanSpec,
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
                self.connector_config(expected, ConnectorMode::Snapshot)?,
                None,
            )
            .map_err(|error| match error.kind() {
                ErrorKind::Timeout => MySqlCdcScanError::new(
                    "Debezium snapshot did not enter polling within DogPaddle's fixed 60-second readiness deadline",
                ),
                kind => MySqlCdcScanError::new(format!(
                    "Debezium snapshot start failed ({kind:?})"
                )),
            })
    }

    pub(super) fn start_streaming(
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
            .map_err(|error| match error.kind() {
                ErrorKind::Timeout => MySqlCdcScanError::new(
                    "Debezium connector did not enter polling within DogPaddle's fixed 60-second readiness deadline",
                ),
                kind => MySqlCdcScanError::new(format!(
                    "Debezium connector start failed ({kind:?})"
                )),
            })
    }

    fn connect(&self) -> Result<Conn, MySqlCdcScanError> {
        let options = OptsBuilder::new()
            .ip_or_hostname(Some(self.host.clone()))
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(self.options.discovery_connect_timeout))
            .read_timeout(Some(self.options.discovery_query_timeout))
            .write_timeout(Some(self.options.discovery_query_timeout));
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
        if !mysql_global_flag_enabled(&log_bin)
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
        let replication_client_id = replication_client_id(&spec.engine_name).to_string();
        let snapshot_mode = mode.snapshot_mode();
        let notification_topic = format!("__dogpaddle-notification.{}", spec.engine_name);
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
            ("snapshot.mode", snapshot_mode),
            // Minimal locking obtains the binlog origin and schema under a
            // short global read lock, then scans InnoDB from one consistent
            // snapshot while ordinary writes continue.
            ("snapshot.locking.mode", "minimal"),
            ("snapshot.max.threads", "1"),
            (
                "schema.history.internal",
                "io.debezium.relational.history.MemorySchemaHistory",
            ),
            ("include.schema.changes", "false"),
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
            ("max.batch.size", "1024"),
            ("max.queue.size", "2048"),
            ("max.queue.size.in.bytes", "16777216"),
            ("poll.interval.ms", "100"),
            ("database.connectionTimeZone", "UTC"),
            ("event.processing.failure.handling.mode", "fail"),
        ] {
            config = config.property(key, value).map_err(|_| {
                MySqlCdcScanError::new("invalid fixed MySQL connector configuration")
            })?;
        }
        for (key, value) in connector_option_properties(&self.options, mode) {
            config = config
                .property(key, value)
                .map_err(|_| MySqlCdcScanError::new("invalid MySQL connector runtime options"))?;
        }
        if matches!(mode, ConnectorMode::Snapshot) {
            for (key, value) in [
                ("notification.enabled.channels", "sink"),
                ("notification.sink.topic.name", notification_topic.as_str()),
            ] {
                config = config.property(key, value).map_err(|_| {
                    MySqlCdcScanError::new("invalid fixed MySQL connector configuration")
                })?;
            }
        }
        Ok(config)
    }
}

#[derive(Clone, Copy)]
enum ConnectorMode {
    Snapshot,
    Recovery,
}

impl ConnectorMode {
    const fn snapshot_mode(self) -> &'static str {
        match self {
            Self::Snapshot => "initial_only",
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
            .field("options", &self.options)
            .finish()
    }
}

fn positive_milliseconds(label: &str, duration: Duration) -> Result<i32, MySqlCdcScanError> {
    let milliseconds = i32::try_from(duration.as_millis()).map_err(|_| {
        MySqlCdcScanError::invalid_options(format!(
            "{label} exceeds Java Integer.MAX_VALUE milliseconds"
        ))
    })?;
    if milliseconds == 0
        || Duration::from_millis(u64::try_from(milliseconds).expect("positive i32 fits u64"))
            != duration
    {
        return Err(MySqlCdcScanError::invalid_options(format!(
            "{label} must be a positive whole-millisecond duration"
        )));
    }
    Ok(milliseconds)
}

fn mysql_global_flag_enabled(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("ON")
}

fn connector_option_properties(
    options: &MySqlCdcScanOptions,
    mode: ConnectorMode,
) -> Vec<(&'static str, String)> {
    let heartbeat_interval_ms = match mode {
        ConnectorMode::Snapshot => SNAPSHOT_HEARTBEAT_INTERVAL_MS,
        ConnectorMode::Recovery => options.heartbeat_interval_ms,
    };
    let mut properties = vec![
        ("connect.timeout.ms", options.connect_timeout_ms.to_string()),
        (
            "database.query.timeout.ms",
            jdbc_query_timeout_millis(options.query_timeout_ms).to_string(),
        ),
        ("errors.max.retries", options.retry_limit.to_string()),
        (
            "errors.retry.delay.initial.ms",
            RETRY_INITIAL_DELAY_MS.to_string(),
        ),
        (
            "errors.retry.delay.max.ms",
            options.retry_max_delay_ms.to_string(),
        ),
        ("heartbeat.interval.ms", heartbeat_interval_ms.to_string()),
    ];
    if matches!(mode, ConnectorMode::Snapshot)
        && let Some(snapshot_fetch_size) = options.snapshot_fetch_size
    {
        properties.push(("snapshot.fetch.size", snapshot_fetch_size.to_string()));
    }
    properties
}

fn jdbc_query_timeout_millis(milliseconds: i32) -> i32 {
    (milliseconds / 1_000 + i32::from(milliseconds % 1_000 != 0)) * 1_000
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

fn replication_client_id(engine_name: &str) -> u32 {
    let mut hasher = blake3::Hasher::new_derive_key(REPLICATION_CLIENT_ID_CONTEXT);
    hasher.update(engine_name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    let domain = u64::from(u32::MAX);
    u32::try_from(u64::from_be_bytes(bytes) % domain + 1)
        .expect("the derived replication client ID is in the u32 domain")
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use super::{
        ConnectorMode, DATABASE_TIMEOUT, MySqlCdcScanOptions, connector_option_properties,
        mysql_global_flag_enabled, replication_client_id,
    };

    #[test]
    fn global_boolean_accepts_mysql_text_and_numeric_forms() {
        assert!(mysql_global_flag_enabled("ON"));
        assert!(mysql_global_flag_enabled("on"));
        assert!(mysql_global_flag_enabled("1"));
        assert!(!mysql_global_flag_enabled("OFF"));
        assert!(!mysql_global_flag_enabled("0"));
    }

    #[test]
    fn snapshot_then_recovery_are_the_only_connector_modes() {
        assert_eq!(ConnectorMode::Snapshot.snapshot_mode(), "initial_only");
        assert_eq!(ConnectorMode::Recovery.snapshot_mode(), "recovery");
    }

    #[test]
    fn replication_client_id_is_stable_nonzero_and_bound_to_the_engine() {
        assert_eq!(replication_client_id("orders"), 3_766_999_730);
        assert_ne!(replication_client_id("orders"), 0);
        assert_ne!(
            replication_client_id("orders"),
            replication_client_id("users")
        );
    }

    #[test]
    fn connector_options_pin_defaults_without_setting_snapshot_fetch_size() {
        let options = MySqlCdcScanOptions::new();
        assert_eq!(options.discovery_connect_timeout, DATABASE_TIMEOUT);
        assert_eq!(options.discovery_query_timeout, DATABASE_TIMEOUT);
        assert_eq!(
            connector_option_properties(&options, ConnectorMode::Snapshot),
            vec![
                ("connect.timeout.ms", "30000".to_owned()),
                ("database.query.timeout.ms", "600000".to_owned()),
                ("errors.max.retries", "-1".to_owned()),
                ("errors.retry.delay.initial.ms", "300".to_owned()),
                ("errors.retry.delay.max.ms", "10000".to_owned()),
                ("heartbeat.interval.ms", "1".to_owned()),
            ]
        );
        assert_eq!(
            connector_option_properties(&options, ConnectorMode::Recovery),
            vec![
                ("connect.timeout.ms", "30000".to_owned()),
                ("database.query.timeout.ms", "600000".to_owned()),
                ("errors.max.retries", "-1".to_owned()),
                ("errors.retry.delay.initial.ms", "300".to_owned()),
                ("errors.retry.delay.max.ms", "10000".to_owned()),
                ("heartbeat.interval.ms", "1000".to_owned()),
            ]
        );
    }

    #[test]
    fn connector_options_map_explicit_values_and_keep_snapshot_heartbeat_fixed() {
        let options = MySqlCdcScanOptions::new()
            .connect_timeout(Duration::from_millis(7))
            .unwrap()
            .query_timeout(Duration::from_millis(8))
            .unwrap()
            .retry_limit(9)
            .unwrap()
            .retry_max_delay(Duration::from_millis(301))
            .unwrap()
            .heartbeat_interval(Duration::from_millis(11))
            .unwrap()
            .snapshot_fetch_size(NonZeroU32::new(12).unwrap())
            .unwrap();
        assert_eq!(options.discovery_connect_timeout, Duration::from_millis(7));
        assert_eq!(options.discovery_query_timeout, Duration::from_millis(8));
        assert_eq!(
            connector_option_properties(&options, ConnectorMode::Snapshot),
            vec![
                ("connect.timeout.ms", "7".to_owned()),
                ("database.query.timeout.ms", "1000".to_owned()),
                ("errors.max.retries", "9".to_owned()),
                ("errors.retry.delay.initial.ms", "300".to_owned()),
                ("errors.retry.delay.max.ms", "301".to_owned()),
                ("heartbeat.interval.ms", "1".to_owned()),
                ("snapshot.fetch.size", "12".to_owned()),
            ]
        );
        assert_eq!(
            connector_option_properties(&options, ConnectorMode::Recovery)[5],
            ("heartbeat.interval.ms", "11".to_owned())
        );
        assert_eq!(
            connector_option_properties(&options, ConnectorMode::Recovery).len(),
            6
        );
    }
}
