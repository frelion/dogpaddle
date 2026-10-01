use std::{
    env,
    fmt::Write as _,
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
};

use datafusion_sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments,
    ObjectName, TableFunctionArgs, Value,
};
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::OperationDefinition;
use dogpaddle_operation::operation::{
    scan::{
        CdcOptions, MySqlCdcScanConfig, MySqlCdcScanDefinition, PostgresCdcScanConfig,
        PostgresCdcScanDefinition, SequenceScanDefinition,
    },
    sink::{
        ClickHouseSinkConfig, ClickHouseSinkDefinition, DiscardDefinition, DorisSinkConfig,
        DorisSinkDefinition, PostgresSinkConfig, PostgresSinkDefinition, SqliteSinkDefinition,
    },
};
use percent_encoding::percent_decode_str;
use url::Url;

use crate::{
    SqlError,
    program::{SINK_OPERATION_ID, scan_operation_id, write_identity_bytes},
};

const DEBEZIUM_RUNTIME_ENV: &str = "DOGPADDLE_DEBEZIUM_RUNTIME";

pub(crate) enum Parameter {
    Literal(String),
    Environment(String),
}

impl Parameter {
    fn resolve(&self) -> Result<String, SqlError> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Environment(name) => {
                env::var(name).map_err(|_| SqlError::Environment { name: name.clone() })
            }
        }
    }

    fn resolve_u64(&self, endpoint: &str, name: &str) -> Result<u64, SqlError> {
        self.resolve()?.parse().map_err(|_| {
            SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must resolve to an unsigned 64-bit integer"
            ))
        })
    }

    fn resolve_nonzero_u64(&self, endpoint: &str, name: &str) -> Result<NonZeroU64, SqlError> {
        NonZeroU64::new(self.resolve_u64(endpoint, name)?).ok_or_else(|| {
            SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must resolve to a nonzero unsigned 64-bit integer"
            ))
        })
    }
}

#[derive(Clone, Copy)]
enum ParameterKind {
    String,
    U64,
}

#[derive(Clone, Copy)]
struct ParameterSpec {
    name: &'static str,
    kind: ParameterKind,
    presence: ParameterPresence,
}

#[derive(Clone, Copy)]
enum ParameterPresence {
    Required,
    Default(&'static str),
    Optional,
}

const fn string(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::String,
        presence: ParameterPresence::Required,
    }
}

const fn u64_parameter(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
        presence: ParameterPresence::Required,
    }
}

const fn optional_u64(name: &'static str, default: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
        presence: ParameterPresence::Default(default),
    }
}

const fn optional_u64_override(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
        presence: ParameterPresence::Optional,
    }
}

// The parser owns name/type/presence validation. Resolution reads this immutable
// argument set once into the concrete Resolved endpoint snapshot.
pub(crate) struct Arguments(Vec<(&'static str, Parameter)>);

impl Arguments {
    fn get(&self, name: &str) -> Option<&Parameter> {
        self.0
            .iter()
            .find_map(|(key, value)| (*key == name).then_some(value))
    }

    fn required(&self, name: &str) -> &Parameter {
        self.get(name)
            .expect("the endpoint specification requires this parameter")
    }

    fn resolve_tuning(&self, endpoint: &str) -> Result<CdcOptions, SqlError> {
        let mut options = CdcOptions::new();
        if let Some(value) = self.milliseconds(endpoint, "connect_timeout_ms")? {
            options = options
                .connect_timeout(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "connect_timeout_ms", error))?;
        }
        if let Some(value) = self.milliseconds(endpoint, "query_timeout_ms")? {
            options = options
                .query_timeout(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "query_timeout_ms", error))?;
        }
        if let Some(value) = self.unsigned(endpoint, "retry_limit")? {
            options = options
                .retry_limit(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "retry_limit", error))?;
        }
        if let Some(value) = self.milliseconds(endpoint, "retry_max_delay_ms")? {
            options = options
                .retry_max_delay(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "retry_max_delay_ms", error))?;
        }
        if let Some(value) = self.milliseconds(endpoint, "heartbeat_interval_ms")? {
            options = options
                .heartbeat_interval(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "heartbeat_interval_ms", error))?;
        }
        if let Some(value) = self.nonzero_unsigned(endpoint, "snapshot_fetch_size")? {
            options = options
                .snapshot_fetch_size(value)
                .map_err(|error| invalid_cdc_tuning(endpoint, "snapshot_fetch_size", error))?;
        }
        Ok(options)
    }

    fn milliseconds(
        &self,
        endpoint: &str,
        name: &str,
    ) -> Result<Option<std::time::Duration>, SqlError> {
        self.get(name)
            .map(|parameter| {
                parameter
                    .resolve_u64(endpoint, name)
                    .map(std::time::Duration::from_millis)
            })
            .transpose()
    }

    fn unsigned(&self, endpoint: &str, name: &str) -> Result<Option<u32>, SqlError> {
        self.get(name)
            .map(|parameter| {
                let value = parameter.resolve_u64(endpoint, name)?;
                u32::try_from(value).map_err(|_| {
                    SqlError::invalid(format!(
                        "{endpoint} parameter {name:?} exceeds an unsigned 32-bit integer"
                    ))
                })
            })
            .transpose()
    }

    fn nonzero_unsigned(&self, endpoint: &str, name: &str) -> Result<Option<NonZeroU32>, SqlError> {
        self.unsigned(endpoint, name)?
            .map(|value| {
                NonZeroU32::new(value).ok_or_else(|| {
                    SqlError::invalid(format!(
                        "{endpoint} parameter {name:?} must resolve to a nonzero unsigned integer"
                    ))
                })
            })
            .transpose()
    }
}

fn invalid_cdc_tuning(endpoint: &str, name: &str, error: impl std::fmt::Display) -> SqlError {
    SqlError::invalid(format!("{endpoint} parameter {name:?} is invalid: {error}"))
}

struct DatabaseConnection {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl DatabaseConnection {
    fn postgres(parameter: &Parameter, endpoint: &str) -> Result<Self, SqlError> {
        Self::parse(parameter, endpoint, &["postgres", "postgresql"], 5432)
    }

    fn mysql(parameter: &Parameter) -> Result<Self, SqlError> {
        Self::parse(parameter, "mysql_cdc", &["mysql"], 3306)
    }

    fn doris(parameter: &Parameter) -> Result<Self, SqlError> {
        Self::parse(parameter, "doris", &["doris"], 9030)
    }

    fn clickhouse(parameter: &Parameter) -> Result<Self, SqlError> {
        Self::parse(
            parameter,
            "clickhouse",
            &["clickhouse", "clickhouse+http"],
            8123,
        )
    }

    fn parse(
        parameter: &Parameter,
        endpoint: &str,
        schemes: &[&str],
        default_port: u16,
    ) -> Result<Self, SqlError> {
        let value = parameter.resolve()?;
        let url = Url::parse(&value).map_err(|_| invalid_connection(endpoint))?;
        if !schemes.contains(&url.scheme()) || url.query().is_some() || url.fragment().is_some() {
            return Err(invalid_connection(endpoint));
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| invalid_connection(endpoint))?
            .to_owned();
        let port = url.port().unwrap_or(default_port);
        if port == 0 {
            return Err(invalid_connection(endpoint));
        }
        let user = decode_url_component(url.username(), endpoint)?;
        if user.is_empty() {
            return Err(invalid_connection(endpoint));
        }
        let password = url.password().map_or(Ok(String::new()), |value| {
            decode_url_component(value, endpoint)
        })?;
        let mut segments = url
            .path_segments()
            .ok_or_else(|| invalid_connection(endpoint))?;
        let database = segments
            .next()
            .filter(|database| !database.is_empty())
            .ok_or_else(|| invalid_connection(endpoint))?;
        if segments.next().is_some() {
            return Err(invalid_connection(endpoint));
        }
        let database = decode_url_component(database, endpoint)?;
        if database.contains('\0') {
            return Err(invalid_connection(endpoint));
        }
        Ok(Self {
            host,
            port,
            database,
            user,
            password,
        })
    }

    fn postgres_cdc_config(
        &self,
        runtime_bundle: &Path,
        options: CdcOptions,
    ) -> Result<PostgresCdcScanConfig, SqlError> {
        PostgresCdcScanConfig::new_unencrypted(
            runtime_bundle,
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
        .map(|config| config.options(options))
        .map_err(SqlError::endpoint)
    }

    fn postgres_sink_config(&self) -> Result<PostgresSinkConfig, SqlError> {
        PostgresSinkConfig::new_unencrypted(
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
        .map_err(SqlError::endpoint)
    }

    fn doris_sink_config(&self) -> Result<DorisSinkConfig, SqlError> {
        DorisSinkConfig::new_unencrypted(
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
        .map_err(SqlError::endpoint)
    }

    fn clickhouse_sink_config(&self) -> Result<ClickHouseSinkConfig, SqlError> {
        ClickHouseSinkConfig::new_unencrypted(
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
        .map_err(SqlError::endpoint)
    }

    fn mysql_config(
        &self,
        runtime_bundle: &Path,
        options: CdcOptions,
    ) -> Result<MySqlCdcScanConfig, SqlError> {
        MySqlCdcScanConfig::new_unencrypted(
            runtime_bundle,
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
        .map(|config| config.options(options))
        .map_err(SqlError::endpoint)
    }
}

fn decode_url_component(value: &str, endpoint: &str) -> Result<String, SqlError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| invalid_connection(endpoint))
}

fn invalid_connection(endpoint: &str) -> SqlError {
    SqlError::invalid(format!(
        "{endpoint} parameter \"connection\" must be a database URL without options or fragments"
    ))
}

pub(crate) enum ScanEndpoint {
    Sequence(Arguments),
    PostgresCdc(Arguments),
    MySqlCdc(Arguments),
}

pub(crate) struct ResolvedEndpoints {
    pub(crate) scans: Vec<ResolvedScanEndpoint>,
    pub(crate) sink: ResolvedSinkEndpoint,
}

pub(crate) enum ResolvedScanEndpoint {
    Sequence { start: u64 },
    PostgresCdc(Box<ResolvedPostgresCdc>),
    MySqlCdc(Box<ResolvedMySqlCdc>),
}

pub(crate) fn project_scan_definition(
    definition: OperationDefinition,
    projection: &[usize],
) -> Result<OperationDefinition, SqlError> {
    let projection = projection
        .iter()
        .map(|index| {
            u32::try_from(*index)
                .map_err(|_| SqlError::invalid("scan projection index exceeds u32"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match definition {
        OperationDefinition::SequenceScan(definition) => {
            Ok(OperationDefinition::SequenceScan(definition))
        }
        OperationDefinition::PostgresCdcScan(definition) => {
            Ok(PostgresCdcScanDefinition::try_new_projected(
                definition.spec().clone(),
                projection,
                definition.bootstrap_spool_bytes(),
            )
            .map_err(SqlError::endpoint)?
            .into())
        }
        OperationDefinition::MySqlCdcScan(definition) => {
            Ok(MySqlCdcScanDefinition::try_new_projected(
                definition.spec().clone(),
                projection,
                definition.bootstrap_spool_bytes(),
            )
            .map_err(SqlError::endpoint)?
            .into())
        }
        _ => Err(SqlError::invalid("expected a scan definition")),
    }
}

pub(crate) struct ResolvedPostgresCdc {
    connection: DatabaseConnection,
    schema: String,
    table: String,
    publication: String,
    bootstrap_spool_bytes: NonZeroU64,
    options: CdcOptions,
}

pub(crate) struct ResolvedMySqlCdc {
    connection: DatabaseConnection,
    table: String,
    bootstrap_spool_bytes: NonZeroU64,
    options: CdcOptions,
}

impl ScanEndpoint {
    pub(crate) fn resolve(&self) -> Result<ResolvedScanEndpoint, SqlError> {
        match self {
            Self::Sequence(arguments) => Ok(ResolvedScanEndpoint::Sequence {
                start: arguments
                    .required("start")
                    .resolve_u64("sequence", "start")?,
            }),
            Self::PostgresCdc(endpoint) => {
                let connection =
                    DatabaseConnection::postgres(endpoint.required("connection"), "postgres_cdc")?;
                let (schema, table) = qualified_table(endpoint.required("table"), "postgres_cdc")?;
                validate_cdc_identifier(&schema, "postgres_cdc", "table schema")?;
                validate_cdc_identifier(&table, "postgres_cdc", "table name")?;
                let publication = endpoint.required("publication").resolve()?;
                validate_cdc_identifier(&publication, "postgres_cdc", "publication")?;
                Ok(ResolvedScanEndpoint::PostgresCdc(Box::new(
                    ResolvedPostgresCdc {
                        connection,
                        schema,
                        table,
                        publication,
                        bootstrap_spool_bytes: endpoint
                            .required("bootstrap_spool_bytes")
                            .resolve_nonzero_u64("postgres_cdc", "bootstrap_spool_bytes")?,
                        options: endpoint.resolve_tuning("postgres_cdc")?,
                    },
                )))
            }
            Self::MySqlCdc(endpoint) => {
                let connection = DatabaseConnection::mysql(endpoint.required("connection"))?;
                let (database, table) = qualified_table(endpoint.required("table"), "mysql_cdc")?;
                require_database(&connection, &database, "mysql_cdc")?;
                validate_cdc_identifier(&database, "mysql_cdc", "database")?;
                validate_cdc_identifier(&table, "mysql_cdc", "table name")?;
                Ok(ResolvedScanEndpoint::MySqlCdc(Box::new(ResolvedMySqlCdc {
                    connection,
                    table,
                    bootstrap_spool_bytes: endpoint
                        .required("bootstrap_spool_bytes")
                        .resolve_nonzero_u64("mysql_cdc", "bootstrap_spool_bytes")?,
                    options: endpoint.resolve_tuning("mysql_cdc")?,
                })))
            }
        }
    }

    pub(crate) fn parse(
        name: &ObjectName,
        arguments: &TableFunctionArgs,
    ) -> Result<Self, SqlError> {
        if arguments.settings.is_some() {
            return Err(SqlError::invalid("scan functions do not accept SETTINGS"));
        }
        match endpoint_name(name).as_deref() {
            Some("sequence") => Ok(Self::Sequence(parse_arguments(
                "sequence",
                &arguments.args,
                &[u64_parameter("start")],
            )?)),
            Some("postgres_cdc") => parse_postgres_cdc(arguments),
            Some("mysql_cdc") => parse_mysql_cdc(arguments),
            _ => Err(SqlError::invalid(format!("unknown scan function {name}"))),
        }
    }
}

impl ResolvedScanEndpoint {
    pub(crate) const fn needs_debezium(&self) -> bool {
        matches!(self, Self::PostgresCdc(_) | Self::MySqlCdc(_))
    }

    pub(crate) fn build(
        &self,
        identity: &[u8; 32],
        index: usize,
        state_path: &Path,
        runtime_bundle: Option<&Path>,
        factory: &mut FlowFactory,
    ) -> Result<OperationDefinition, SqlError> {
        match self {
            Self::Sequence { start } => Ok(SequenceScanDefinition::new(*start).into()),
            Self::PostgresCdc(endpoint) => {
                let config = endpoint.connection.postgres_cdc_config(
                    runtime_bundle.expect("CDC programs resolve one runtime"),
                    endpoint.options,
                )?;
                let engine_name = scan_name(identity, state_path, index);
                let spec = config
                    .discover(
                        &engine_name,
                        &endpoint.schema,
                        &endpoint.table,
                        &engine_name,
                        &endpoint.publication,
                    )
                    .map_err(SqlError::endpoint)?;
                let definition =
                    PostgresCdcScanDefinition::try_new(spec, endpoint.bootstrap_spool_bytes)
                        .map_err(SqlError::endpoint)?;
                factory.resource(scan_operation_id(index), config)?;
                Ok(definition.into())
            }
            Self::MySqlCdc(endpoint) => {
                let config = endpoint.connection.mysql_config(
                    runtime_bundle.expect("CDC programs resolve one runtime"),
                    endpoint.options,
                )?;
                let spec = config
                    .discover(&scan_name(identity, state_path, index), &endpoint.table)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    MySqlCdcScanDefinition::try_new(spec, endpoint.bootstrap_spool_bytes)
                        .map_err(SqlError::endpoint)?;
                factory.resource(scan_operation_id(index), config)?;
                Ok(definition.into())
            }
        }
    }

    pub(crate) fn write_identity(&self, encoded: &mut Vec<u8>) {
        match self {
            Self::Sequence { start } => {
                encoded.push(0);
                encoded.extend_from_slice(&start.to_be_bytes());
            }
            Self::PostgresCdc(endpoint) => {
                encoded.push(1);
                write_identity_bytes(encoded, endpoint.connection.database.as_bytes());
                write_identity_bytes(encoded, endpoint.schema.as_bytes());
                write_identity_bytes(encoded, endpoint.table.as_bytes());
                write_identity_bytes(encoded, endpoint.publication.as_bytes());
                encoded.extend_from_slice(&endpoint.bootstrap_spool_bytes.get().to_be_bytes());
            }
            Self::MySqlCdc(endpoint) => {
                encoded.push(2);
                write_identity_bytes(encoded, endpoint.connection.database.as_bytes());
                write_identity_bytes(encoded, endpoint.table.as_bytes());
                encoded.extend_from_slice(&endpoint.bootstrap_spool_bytes.get().to_be_bytes());
            }
        }
    }

    pub(crate) fn install_open_runtime_resource(
        &self,
        factory: &mut FlowFactory,
        station_id: &str,
        runtime_bundle: Option<&Path>,
    ) -> Result<(), SqlError> {
        match self {
            Self::Sequence { .. } => {}
            Self::PostgresCdc(endpoint) => {
                factory.resource(
                    station_id,
                    endpoint.connection.postgres_cdc_config(
                        runtime_bundle.expect("CDC programs resolve one runtime"),
                        endpoint.options,
                    )?,
                )?;
            }
            Self::MySqlCdc(endpoint) => {
                factory.resource(
                    station_id,
                    endpoint.connection.mysql_config(
                        runtime_bundle.expect("CDC programs resolve one runtime"),
                        endpoint.options,
                    )?,
                )?;
            }
        }
        Ok(())
    }
}

fn parse_postgres_cdc(arguments: &TableFunctionArgs) -> Result<ScanEndpoint, SqlError> {
    Ok(ScanEndpoint::PostgresCdc(parse_arguments(
        "postgres_cdc",
        &arguments.args,
        &[
            string("connection"),
            string("table"),
            string("publication"),
            optional_u64("bootstrap_spool_bytes", "1073741824"),
            optional_u64_override("connect_timeout_ms"),
            optional_u64_override("query_timeout_ms"),
            optional_u64_override("retry_limit"),
            optional_u64_override("retry_max_delay_ms"),
            optional_u64_override("heartbeat_interval_ms"),
            optional_u64_override("snapshot_fetch_size"),
        ],
    )?))
}

fn parse_mysql_cdc(arguments: &TableFunctionArgs) -> Result<ScanEndpoint, SqlError> {
    Ok(ScanEndpoint::MySqlCdc(parse_arguments(
        "mysql_cdc",
        &arguments.args,
        &[
            string("connection"),
            string("table"),
            optional_u64("bootstrap_spool_bytes", "1073741824"),
            optional_u64_override("connect_timeout_ms"),
            optional_u64_override("query_timeout_ms"),
            optional_u64_override("retry_limit"),
            optional_u64_override("retry_max_delay_ms"),
            optional_u64_override("heartbeat_interval_ms"),
            optional_u64_override("snapshot_fetch_size"),
        ],
    )?))
}

pub(crate) struct ResolvedDatabaseSink {
    connection: DatabaseConnection,
    namespace: String,
    table: String,
}

pub(crate) enum ResolvedSinkEndpoint {
    ClickHouse(ResolvedDatabaseSink),
    Doris(ResolvedDatabaseSink),
    Postgres(ResolvedDatabaseSink),
    Sqlite { path: String, table: String },
    Discard,
}

pub(crate) enum SinkEndpoint {
    ClickHouse(Arguments),
    Doris(Arguments),
    Postgres(Arguments),
    Sqlite(Arguments),
    Discard,
}

impl SinkEndpoint {
    pub(crate) fn resolve(&self) -> Result<ResolvedSinkEndpoint, SqlError> {
        match self {
            Self::ClickHouse(endpoint) => {
                let connection = DatabaseConnection::clickhouse(endpoint.required("connection"))?;
                let (namespace, table) = qualified_table(endpoint.required("table"), "clickhouse")?;
                require_connection_database(&connection, &namespace, "clickhouse")?;
                Ok(ResolvedSinkEndpoint::ClickHouse(ResolvedDatabaseSink {
                    connection,
                    namespace,
                    table,
                }))
            }
            Self::Doris(endpoint) => {
                let connection = DatabaseConnection::doris(endpoint.required("connection"))?;
                let (namespace, table) = qualified_table(endpoint.required("table"), "doris")?;
                require_connection_database(&connection, &namespace, "doris")?;
                Ok(ResolvedSinkEndpoint::Doris(ResolvedDatabaseSink {
                    connection,
                    namespace,
                    table,
                }))
            }
            Self::Postgres(endpoint) => {
                let connection =
                    DatabaseConnection::postgres(endpoint.required("connection"), "postgres")?;
                let (namespace, table) = qualified_table(endpoint.required("table"), "postgres")?;
                Ok(ResolvedSinkEndpoint::Postgres(ResolvedDatabaseSink {
                    connection,
                    namespace,
                    table,
                }))
            }
            Self::Sqlite(endpoint) => Ok(ResolvedSinkEndpoint::Sqlite {
                path: endpoint.required("path").resolve()?,
                table: endpoint.required("table").resolve()?,
            }),
            Self::Discard => Ok(ResolvedSinkEndpoint::Discard),
        }
    }

    pub(crate) fn parse(function: &Function) -> Result<Self, SqlError> {
        if function.uses_odbc_syntax
            || !matches!(function.parameters, FunctionArguments::None)
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
        {
            return Err(SqlError::invalid(
                "sink declaration must be a plain function call",
            ));
        }
        let FunctionArguments::List(arguments) = &function.args else {
            return Err(SqlError::invalid("sink declaration requires parentheses"));
        };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
            return Err(SqlError::invalid(
                "sink function arguments cannot contain clauses",
            ));
        }

        match endpoint_name(&function.name).as_deref() {
            Some("clickhouse") => Ok(Self::ClickHouse(parse_arguments(
                "clickhouse",
                &arguments.args,
                &[string("connection"), string("table")],
            )?)),
            Some("doris") => Ok(Self::Doris(parse_arguments(
                "doris",
                &arguments.args,
                &[string("connection"), string("table")],
            )?)),
            Some("postgres") => Ok(Self::Postgres(parse_arguments(
                "postgres",
                &arguments.args,
                &[string("connection"), string("table")],
            )?)),
            Some("sqlite") => Ok(Self::Sqlite(parse_arguments(
                "sqlite",
                &arguments.args,
                &[string("path"), string("table")],
            )?)),
            Some("discard") => {
                parse_arguments("discard", &arguments.args, &[])?;
                Ok(Self::Discard)
            }
            _ => Err(SqlError::invalid(format!(
                "unknown sink function {}",
                function.name
            ))),
        }
    }
}

impl ResolvedSinkEndpoint {
    pub(crate) fn build(
        &self,
        identity: &[u8; 32],
        state_path: &Path,
        factory: &mut FlowFactory,
    ) -> Result<OperationDefinition, SqlError> {
        match self {
            Self::ClickHouse(endpoint) => {
                let connection = &endpoint.connection;
                let table = &endpoint.table;
                let config = connection.clickhouse_sink_config()?;
                let target = config
                    .discover_target(sink_name(identity, state_path), table)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    ClickHouseSinkDefinition::try_new(target).map_err(SqlError::endpoint)?;
                factory.resource(SINK_OPERATION_ID, config)?;
                Ok(definition.into())
            }
            Self::Doris(endpoint) => {
                let connection = &endpoint.connection;
                let table = &endpoint.table;
                let config = connection.doris_sink_config()?;
                let target = config
                    .discover_target(sink_name(identity, state_path), table)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    DorisSinkDefinition::try_new(target).map_err(SqlError::endpoint)?;
                factory.resource(SINK_OPERATION_ID, config)?;
                Ok(definition.into())
            }
            Self::Postgres(endpoint) => {
                let connection = &endpoint.connection;
                let table = &endpoint.table;
                let config = connection.postgres_sink_config()?;
                let target = config
                    .discover_target(sink_name(identity, state_path), &endpoint.namespace, table)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    PostgresSinkDefinition::try_new(target).map_err(SqlError::endpoint)?;
                factory.resource(SINK_OPERATION_ID, config)?;
                Ok(definition.into())
            }
            Self::Sqlite { path, table } => {
                let definition = SqliteSinkDefinition::try_new(PathBuf::from(path), table)
                    .map_err(SqlError::endpoint)?;
                Ok(definition.into())
            }
            Self::Discard => Ok(DiscardDefinition::new().into()),
        }
    }

    pub(crate) fn write_identity(&self, encoded: &mut Vec<u8>) {
        match self {
            Self::ClickHouse(endpoint) => {
                encoded.push(3);
                let connection = &endpoint.connection;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let table = &endpoint.table;
                write_identity_bytes(encoded, table.as_bytes());
            }
            Self::Doris(endpoint) => {
                encoded.push(4);
                let connection = &endpoint.connection;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let table = &endpoint.table;
                write_identity_bytes(encoded, table.as_bytes());
            }
            Self::Postgres(endpoint) => {
                encoded.push(0);
                let connection = &endpoint.connection;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let table = &endpoint.table;
                write_identity_bytes(encoded, endpoint.namespace.as_bytes());
                write_identity_bytes(encoded, table.as_bytes());
            }
            Self::Sqlite { path, table } => {
                encoded.push(1);
                write_identity_bytes(encoded, path.as_bytes());
                write_identity_bytes(encoded, table.as_bytes());
            }
            Self::Discard => encoded.push(2),
        }
    }

    pub(crate) fn install_open_runtime_resource(
        &self,
        factory: &mut FlowFactory,
    ) -> Result<(), SqlError> {
        match self {
            Self::ClickHouse(endpoint) => {
                let connection = &endpoint.connection;
                factory.resource(SINK_OPERATION_ID, connection.clickhouse_sink_config()?)?;
            }
            Self::Doris(endpoint) => {
                let connection = &endpoint.connection;
                factory.resource(SINK_OPERATION_ID, connection.doris_sink_config()?)?;
            }
            Self::Postgres(endpoint) => {
                let connection = &endpoint.connection;
                factory.resource(SINK_OPERATION_ID, connection.postgres_sink_config()?)?;
            }
            Self::Sqlite { .. } | Self::Discard => {}
        }
        Ok(())
    }
}

fn require_connection_database(
    connection: &DatabaseConnection,
    table_database: &str,
    endpoint: &str,
) -> Result<(), SqlError> {
    if connection.database == table_database {
        Ok(())
    } else {
        Err(SqlError::invalid(format!(
            "{endpoint} table database must match the connection database"
        )))
    }
}

pub(crate) fn resolve_debezium_runtime() -> Result<PathBuf, SqlError> {
    let candidate = if let Some(path) = env::var_os(DEBEZIUM_RUNTIME_ENV) {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(SqlError::endpoint(format!(
                "{DEBEZIUM_RUNTIME_ENV} must contain an absolute path"
            )));
        }
        path
    } else {
        let executable = env::current_exe()
            .map_err(|_| SqlError::endpoint("cannot locate the installed Debezium runtime"))?;
        let installation = executable
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| SqlError::endpoint("cannot locate the installed Debezium runtime"))?;
        installation.join("libexec/dogpaddle/debezium")
    };
    let runtime = fs::canonicalize(candidate)
        .map_err(|_| SqlError::endpoint("the installed Debezium runtime is unavailable"))?;
    if runtime.is_dir() {
        Ok(runtime)
    } else {
        Err(SqlError::endpoint(
            "the installed Debezium runtime is unavailable",
        ))
    }
}

fn qualified_table(parameter: &Parameter, endpoint: &str) -> Result<(String, String), SqlError> {
    let value = parameter.resolve()?;
    let mut parts = value.split('.');
    let Some(namespace) = parts.next().filter(|part| !part.is_empty()) else {
        return Err(invalid_table(endpoint));
    };
    let Some(table) = parts.next().filter(|part| !part.is_empty()) else {
        return Err(invalid_table(endpoint));
    };
    if parts.next().is_some() {
        return Err(invalid_table(endpoint));
    }
    Ok((namespace.to_owned(), table.to_owned()))
}

fn invalid_table(endpoint: &str) -> SqlError {
    SqlError::invalid(format!(
        "{endpoint} parameter \"table\" must contain exactly two nonempty components"
    ))
}

fn validate_cdc_identifier(value: &str, endpoint: &str, label: &str) -> Result<(), SqlError> {
    if !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Ok(())
    } else {
        Err(SqlError::invalid(format!(
            "{endpoint} {label} must contain 1-63 lowercase ASCII letters, digits, or underscores"
        )))
    }
}

fn require_database(
    connection: &DatabaseConnection,
    qualified_database: &str,
    endpoint: &str,
) -> Result<(), SqlError> {
    if connection.database == qualified_database {
        Ok(())
    } else {
        Err(SqlError::invalid(format!(
            "{endpoint} table database must match its connection URL"
        )))
    }
}

fn scan_name(identity: &[u8; 32], state_path: &Path, index: usize) -> String {
    derived_name(b"dogpaddle-sql/scan-name/v1", identity, state_path, index)
}

fn sink_name(identity: &[u8; 32], state_path: &Path) -> String {
    derived_name(b"dogpaddle-sql/sink-name/v1", identity, state_path, 0)
}

fn derived_name(domain: &[u8], identity: &[u8; 32], state_path: &Path, index: usize) -> String {
    let digest = derived_digest(domain, identity, state_path, index);
    let mut name = String::from("dp_");
    write_hex(&mut name, &digest.as_bytes()[..14]);
    name
}

fn write_hex(output: &mut String, bytes: &[u8]) {
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
}

fn derived_digest(
    domain: &[u8],
    identity: &[u8; 32],
    state_path: &Path,
    index: usize,
) -> blake3::Hash {
    let mut encoded = Vec::new();
    write_identity_bytes(&mut encoded, domain);
    write_identity_bytes(&mut encoded, identity);
    write_identity_bytes(
        &mut encoded,
        state_path
            .to_str()
            .expect("SQL state paths are validated as UTF-8")
            .as_bytes(),
    );
    let index = u64::try_from(index).expect("an endpoint index fits in u64");
    encoded.extend_from_slice(&index.to_be_bytes());
    blake3::hash(&encoded)
}

fn parse_arguments(
    endpoint: &str,
    arguments: &[FunctionArg],
    expected: &[ParameterSpec],
) -> Result<Arguments, SqlError> {
    let mut values = Arguments(Vec::with_capacity(expected.len()));
    for argument in arguments {
        let FunctionArg::Named {
            name,
            arg,
            operator,
        } = argument
        else {
            return Err(SqlError::invalid(format!(
                "{endpoint} accepts named arguments only"
            )));
        };
        if operator != &FunctionArgOperator::RightArrow {
            return Err(SqlError::invalid(format!(
                "{endpoint} named arguments must use =>"
            )));
        }
        let name = normalized_identifier(name);
        let Some(specification) = expected
            .iter()
            .find(|specification| specification.name == name)
        else {
            if expected.is_empty() {
                return Err(SqlError::invalid(format!(
                    "{endpoint} does not accept parameters"
                )));
            }
            let supported = expected
                .iter()
                .map(|specification| format!("{:?}", specification.name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(SqlError::invalid(format!(
                "unknown {endpoint} parameter {name:?}; supported parameters are {supported}"
            )));
        };
        if values.get(specification.name).is_some() {
            return Err(SqlError::invalid(format!(
                "duplicate {endpoint} parameter {name:?}"
            )));
        }
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must be a literal or env('NAME')"
            )));
        };
        values.0.push((
            specification.name,
            parse_parameter(endpoint, specification.name, specification.kind, expression)?,
        ));
    }

    for specification in expected {
        if values.get(specification.name).is_some() {
            continue;
        }
        match specification.presence {
            ParameterPresence::Default(value) => {
                values
                    .0
                    .push((specification.name, Parameter::Literal(value.to_owned())));
            }
            ParameterPresence::Optional => {}
            ParameterPresence::Required => {
                return Err(SqlError::invalid(format!(
                    "missing {endpoint} parameter {:?}",
                    specification.name
                )));
            }
        }
    }
    Ok(values)
}

fn parse_parameter(
    endpoint: &str,
    name: &str,
    kind: ParameterKind,
    expression: &Expr,
) -> Result<Parameter, SqlError> {
    let invalid = || {
        SqlError::invalid(format!(
            "{endpoint} parameter {name:?} must be a matching literal or env('NAME')"
        ))
    };
    match expression {
        Expr::Value(value) => match (&value.value, kind) {
            (Value::SingleQuotedString(value), ParameterKind::String) => {
                Ok(Parameter::Literal(value.clone()))
            }
            (Value::Number(value, _), ParameterKind::U64) => {
                value.parse::<u64>().map_err(|_| invalid())?;
                Ok(Parameter::Literal(value.clone()))
            }
            _ => Err(invalid()),
        },
        Expr::Function(function) if endpoint_name(&function.name).as_deref() == Some("env") => {
            parse_environment(function).ok_or_else(invalid)
        }
        _ => Err(invalid()),
    }
}

fn parse_environment(function: &Function) -> Option<Parameter> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return None;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] =
        arguments.args.as_slice()
    else {
        return None;
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return None;
    }
    let Value::SingleQuotedString(name) = &value.value else {
        return None;
    };
    if name.is_empty() {
        return None;
    }
    Some(Parameter::Environment(name.clone()))
}

fn endpoint_name(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };
    part.as_ident().map(normalized_identifier)
}

fn normalized_identifier(identifier: &datafusion_sql::sqlparser::ast::Ident) -> String {
    if identifier.quote_style.is_none() {
        identifier.value.to_ascii_lowercase()
    } else {
        identifier.value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_url_parsing_decodes_components_without_accepting_options() {
        let parameter = Parameter::Literal(
            "postgresql://alice%40team:p%40ss@127.0.0.1:6432/app%2Ddb".to_owned(),
        );
        let connection = DatabaseConnection::postgres(&parameter, "postgres").unwrap();
        assert_eq!(connection.host, "127.0.0.1");
        assert_eq!(connection.port, 6432);
        assert_eq!(connection.database, "app-db");
        assert_eq!(connection.user, "alice@team");
        assert_eq!(connection.password, "p@ss");

        for value in [
            "postgresql://user:secret@127.0.0.1/app?sslmode=require",
            "postgresql://user:secret@127.0.0.1/app#fragment",
            "postgresql://user:secret@127.0.0.1/app/extra",
        ] {
            let parameter = Parameter::Literal(value.to_owned());
            assert!(DatabaseConnection::postgres(&parameter, "postgres").is_err());
        }
    }

    #[test]
    fn persistent_endpoint_names_are_stable_and_separate_deployments() {
        let identity = [7; 32];
        let first_path = Path::new("/var/lib/dogpaddle/orders");
        let second_path = Path::new("/var/lib/dogpaddle/orders-copy");

        assert_eq!(
            scan_name(&identity, first_path, 0),
            "dp_88345df1cf02c7bd7934aa1be6ad"
        );
        assert_eq!(
            sink_name(&identity, first_path),
            "dp_7a61955b8a8a4c973df493a39db3"
        );
        assert_ne!(
            scan_name(&identity, first_path, 0),
            scan_name(&identity, first_path, 1)
        );
        assert_ne!(
            scan_name(&identity, first_path, 0),
            scan_name(&identity, second_path, 0)
        );
        assert_ne!(
            scan_name(&identity, first_path, 0),
            sink_name(&identity, first_path)
        );
    }

    #[test]
    fn cdc_tuning_names_translate_to_the_exact_connector_options() {
        let tuning = Arguments(vec![
            ("connect_timeout_ms", Parameter::Literal("1001".to_owned())),
            ("query_timeout_ms", Parameter::Literal("2002".to_owned())),
            ("retry_limit", Parameter::Literal("3".to_owned())),
            ("retry_max_delay_ms", Parameter::Literal("4004".to_owned())),
            (
                "heartbeat_interval_ms",
                Parameter::Literal("5005".to_owned()),
            ),
            ("snapshot_fetch_size", Parameter::Literal("6006".to_owned())),
        ]);
        let expected = CdcOptions::new()
            .connect_timeout(std::time::Duration::from_millis(1001))
            .unwrap()
            .query_timeout(std::time::Duration::from_millis(2002))
            .unwrap()
            .retry_limit(3)
            .unwrap()
            .retry_max_delay(std::time::Duration::from_millis(4004))
            .unwrap()
            .heartbeat_interval(std::time::Duration::from_millis(5005))
            .unwrap()
            .snapshot_fetch_size(NonZeroU32::new(6006).unwrap())
            .unwrap();
        assert_eq!(tuning.resolve_tuning("postgres_cdc").unwrap(), expected);
        assert_eq!(tuning.resolve_tuning("mysql_cdc").unwrap(), expected);
    }
}
