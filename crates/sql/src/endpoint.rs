use std::{
    env,
    fmt::Write as _,
    fs,
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use datafusion_sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments,
    ObjectName, TableFunctionArgs, Value,
};
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::{
    scan::{
        MySqlCdcScanConfig, MySqlCdcScanDefinition, PostgresCdcScanConfig,
        PostgresCdcScanDefinition, SequenceScanDefinition,
    },
    sink::{DiscardDefinition, PostgresSinkConfig, PostgresSinkDefinition, SqliteSinkDefinition},
};
use percent_encoding::percent_decode_str;
use url::Url;

use crate::{SqlError, program::write_identity_bytes};

const DEBEZIUM_RUNTIME_ENV: &str = "DOGPADDLE_DEBEZIUM_RUNTIME";

pub(crate) enum Parameter {
    Literal(String),
    Environment(String),
}

impl Parameter {
    fn resolved(&self) -> Result<Self, SqlError> {
        self.resolve().map(Self::Literal)
    }

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

    fn write_resolved_identity(&self, encoded: &mut Vec<u8>) -> Result<(), SqlError> {
        write_identity_bytes(encoded, self.resolve()?.as_bytes());
        Ok(())
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
    default: Option<&'static str>,
}

const fn string(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::String,
        default: None,
    }
}

const fn u64_parameter(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
        default: None,
    }
}

const fn optional_u64(name: &'static str, default: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
        default: Some(default),
    }
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
    ) -> Result<PostgresCdcScanConfig, SqlError> {
        PostgresCdcScanConfig::new_unencrypted(
            runtime_bundle,
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
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

    fn mysql_config(&self, runtime_bundle: &Path) -> Result<MySqlCdcScanConfig, SqlError> {
        MySqlCdcScanConfig::new_unencrypted(
            runtime_bundle,
            &self.host,
            self.port,
            &self.database,
            &self.user,
            &self.password,
        )
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

pub(crate) struct PostgresCdcEndpoint {
    connection: Parameter,
    table: Parameter,
    publication: Parameter,
    bootstrap_spool_bytes: Parameter,
}

pub(crate) struct MySqlCdcEndpoint {
    connection: Parameter,
    table: Parameter,
    bootstrap_spool_bytes: Parameter,
}

pub(crate) enum ScanEndpoint {
    Sequence { start: Parameter },
    PostgresCdc(Box<PostgresCdcEndpoint>),
    MySqlCdc(Box<MySqlCdcEndpoint>),
}

pub(crate) enum BuiltScan {
    Sequence(SequenceScanDefinition),
    PostgresCdc(Box<BuiltPostgresCdcScan>),
    MySqlCdc(Box<BuiltMySqlCdcScan>),
}

pub(crate) struct BuiltPostgresCdcScan {
    pub(crate) definition: PostgresCdcScanDefinition,
    pub(crate) config: PostgresCdcScanConfig,
}

pub(crate) struct BuiltMySqlCdcScan {
    pub(crate) definition: MySqlCdcScanDefinition,
    pub(crate) config: MySqlCdcScanConfig,
}

impl ScanEndpoint {
    pub(crate) fn resolved(&self) -> Result<Self, SqlError> {
        match self {
            Self::Sequence { start } => Ok(Self::Sequence {
                start: start.resolved()?,
            }),
            Self::PostgresCdc(endpoint) => Ok(Self::PostgresCdc(Box::new(PostgresCdcEndpoint {
                connection: endpoint.connection.resolved()?,
                table: endpoint.table.resolved()?,
                publication: endpoint.publication.resolved()?,
                bootstrap_spool_bytes: endpoint.bootstrap_spool_bytes.resolved()?,
            }))),
            Self::MySqlCdc(endpoint) => Ok(Self::MySqlCdc(Box::new(MySqlCdcEndpoint {
                connection: endpoint.connection.resolved()?,
                table: endpoint.table.resolved()?,
                bootstrap_spool_bytes: endpoint.bootstrap_spool_bytes.resolved()?,
            }))),
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
            Some("sequence") => {
                let [start] = exact_parameters(parse_arguments(
                    "sequence",
                    &arguments.args,
                    &[u64_parameter("start")],
                )?);
                Ok(Self::Sequence { start })
            }
            Some("postgres_cdc") => parse_postgres_cdc(arguments),
            Some("mysql_cdc") => parse_mysql_cdc(arguments),
            _ => Err(SqlError::invalid(format!("unknown scan function {name}"))),
        }
    }

    pub(crate) const fn needs_debezium(&self) -> bool {
        matches!(self, Self::PostgresCdc(_) | Self::MySqlCdc(_))
    }

    pub(crate) fn build(
        &self,
        identity: &[u8; 32],
        index: usize,
        state_path: &Path,
        runtime_bundle: Option<&Path>,
    ) -> Result<BuiltScan, SqlError> {
        match self {
            Self::Sequence { start } => Ok(BuiltScan::Sequence(SequenceScanDefinition::new(
                start.resolve_u64("sequence", "start")?,
            ))),
            Self::PostgresCdc(endpoint) => {
                let runtime_bundle = runtime_bundle.expect("CDC programs resolve one runtime");
                let connection =
                    DatabaseConnection::postgres(&endpoint.connection, "postgres_cdc")?;
                let (schema, table) = qualified_table(&endpoint.table, "postgres_cdc")?;
                let publication = endpoint.publication.resolve()?;
                let bootstrap_spool_bytes = endpoint
                    .bootstrap_spool_bytes
                    .resolve_nonzero_u64("postgres_cdc", "bootstrap_spool_bytes")?;
                let engine_name = scan_name(identity, state_path, index);
                let config = connection.postgres_cdc_config(runtime_bundle)?;
                let spec = config
                    .discover(&engine_name, &schema, &table, &engine_name, &publication)
                    .map_err(SqlError::endpoint)?;
                let definition = PostgresCdcScanDefinition::try_new(spec, bootstrap_spool_bytes)
                    .map_err(SqlError::endpoint)?;
                Ok(BuiltScan::PostgresCdc(Box::new(BuiltPostgresCdcScan {
                    definition,
                    config,
                })))
            }
            Self::MySqlCdc(endpoint) => {
                let runtime_bundle = runtime_bundle.expect("CDC programs resolve one runtime");
                let connection = DatabaseConnection::mysql(&endpoint.connection)?;
                let (database, table) = qualified_table(&endpoint.table, "mysql_cdc")?;
                require_database(&connection, &database, "mysql_cdc")?;
                let bootstrap_spool_bytes = endpoint
                    .bootstrap_spool_bytes
                    .resolve_nonzero_u64("mysql_cdc", "bootstrap_spool_bytes")?;
                let engine_name = scan_name(identity, state_path, index);
                let config = connection.mysql_config(runtime_bundle)?;
                let spec = config
                    .discover(&engine_name, &table)
                    .map_err(SqlError::endpoint)?;
                let definition = MySqlCdcScanDefinition::try_new(spec, bootstrap_spool_bytes)
                    .map_err(SqlError::endpoint)?;
                Ok(BuiltScan::MySqlCdc(Box::new(BuiltMySqlCdcScan {
                    definition,
                    config,
                })))
            }
        }
    }

    pub(crate) fn write_identity(&self, encoded: &mut Vec<u8>) -> Result<(), SqlError> {
        match self {
            Self::Sequence { start } => {
                encoded.push(0);
                encoded.extend_from_slice(&start.resolve_u64("sequence", "start")?.to_be_bytes());
            }
            Self::PostgresCdc(endpoint) => {
                encoded.push(1);
                let connection =
                    DatabaseConnection::postgres(&endpoint.connection, "postgres_cdc")?;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let (schema, table) = qualified_table(&endpoint.table, "postgres_cdc")?;
                validate_cdc_identifier(&schema, "postgres_cdc", "table schema")?;
                validate_cdc_identifier(&table, "postgres_cdc", "table name")?;
                write_identity_bytes(encoded, schema.as_bytes());
                write_identity_bytes(encoded, table.as_bytes());
                let publication = endpoint.publication.resolve()?;
                validate_cdc_identifier(&publication, "postgres_cdc", "publication")?;
                write_identity_bytes(encoded, publication.as_bytes());
                encoded.extend_from_slice(
                    &endpoint
                        .bootstrap_spool_bytes
                        .resolve_nonzero_u64("postgres_cdc", "bootstrap_spool_bytes")?
                        .get()
                        .to_be_bytes(),
                );
            }
            Self::MySqlCdc(endpoint) => {
                encoded.push(2);
                let connection = DatabaseConnection::mysql(&endpoint.connection)?;
                validate_cdc_identifier(&connection.database, "mysql_cdc", "database")?;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let (database, table) = qualified_table(&endpoint.table, "mysql_cdc")?;
                require_database(&connection, &database, "mysql_cdc")?;
                validate_cdc_identifier(&table, "mysql_cdc", "table name")?;
                write_identity_bytes(encoded, table.as_bytes());
                encoded.extend_from_slice(
                    &endpoint
                        .bootstrap_spool_bytes
                        .resolve_nonzero_u64("mysql_cdc", "bootstrap_spool_bytes")?
                        .get()
                        .to_be_bytes(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn install_open_runtime_resource(
        &self,
        factory: &mut FlowFactory,
        station_id: &str,
        runtime_bundle: Option<&Path>,
    ) -> Result<(), SqlError> {
        match self {
            Self::Sequence { .. } => Ok(()),
            Self::PostgresCdc(endpoint) => {
                let runtime_bundle = runtime_bundle.expect("CDC programs resolve one runtime");
                let connection =
                    DatabaseConnection::postgres(&endpoint.connection, "postgres_cdc")?;
                factory.resource(station_id, connection.postgres_cdc_config(runtime_bundle)?)?;
                Ok(())
            }
            Self::MySqlCdc(endpoint) => {
                let runtime_bundle = runtime_bundle.expect("CDC programs resolve one runtime");
                let connection = DatabaseConnection::mysql(&endpoint.connection)?;
                let config = connection.mysql_config(runtime_bundle)?;
                factory.resource(station_id, config)?;
                Ok(())
            }
        }
    }
}

fn parse_postgres_cdc(arguments: &TableFunctionArgs) -> Result<ScanEndpoint, SqlError> {
    let [connection, table, publication, bootstrap_spool_bytes] =
        exact_parameters(parse_arguments(
            "postgres_cdc",
            &arguments.args,
            &[
                string("connection"),
                string("table"),
                string("publication"),
                optional_u64("bootstrap_spool_bytes", "1073741824"),
            ],
        )?);
    Ok(ScanEndpoint::PostgresCdc(Box::new(PostgresCdcEndpoint {
        connection,
        table,
        publication,
        bootstrap_spool_bytes,
    })))
}

fn parse_mysql_cdc(arguments: &TableFunctionArgs) -> Result<ScanEndpoint, SqlError> {
    let [connection, table, bootstrap_spool_bytes] = exact_parameters(parse_arguments(
        "mysql_cdc",
        &arguments.args,
        &[
            string("connection"),
            string("table"),
            optional_u64("bootstrap_spool_bytes", "1073741824"),
        ],
    )?);
    Ok(ScanEndpoint::MySqlCdc(Box::new(MySqlCdcEndpoint {
        connection,
        table,
        bootstrap_spool_bytes,
    })))
}

pub(crate) struct PostgresSinkEndpoint {
    connection: Parameter,
    table: Parameter,
}

pub(crate) enum SinkEndpoint {
    Postgres(PostgresSinkEndpoint),
    Sqlite { path: Parameter, table: Parameter },
    Discard,
}

pub(crate) enum BuiltSink {
    Postgres {
        definition: PostgresSinkDefinition,
        config: PostgresSinkConfig,
    },
    Sqlite(SqliteSinkDefinition),
    Discard(DiscardDefinition),
}

impl SinkEndpoint {
    pub(crate) fn resolved(&self) -> Result<Self, SqlError> {
        match self {
            Self::Postgres(endpoint) => Ok(Self::Postgres(PostgresSinkEndpoint {
                connection: endpoint.connection.resolved()?,
                table: endpoint.table.resolved()?,
            })),
            Self::Sqlite { path, table } => Ok(Self::Sqlite {
                path: path.resolved()?,
                table: table.resolved()?,
            }),
            Self::Discard => Ok(Self::Discard),
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
            Some("postgres") => {
                let [connection, table] = exact_parameters(parse_arguments(
                    "postgres",
                    &arguments.args,
                    &[string("connection"), string("table")],
                )?);
                Ok(Self::Postgres(PostgresSinkEndpoint { connection, table }))
            }
            Some("sqlite") => {
                let [path, table] = exact_parameters(parse_arguments(
                    "sqlite",
                    &arguments.args,
                    &[string("path"), string("table")],
                )?);
                Ok(Self::Sqlite { path, table })
            }
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

    pub(crate) fn build(
        &self,
        identity: &[u8; 32],
        state_path: &Path,
    ) -> Result<BuiltSink, SqlError> {
        match self {
            Self::Postgres(endpoint) => {
                let connection = DatabaseConnection::postgres(&endpoint.connection, "postgres")?;
                let (schema, table) = qualified_table(&endpoint.table, "postgres")?;
                let config = connection.postgres_sink_config()?;
                let target = config
                    .discover_target(sink_name(identity, state_path), schema, table)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    PostgresSinkDefinition::try_new(target).map_err(SqlError::endpoint)?;
                Ok(BuiltSink::Postgres { definition, config })
            }
            Self::Sqlite { path, table } => {
                SqliteSinkDefinition::try_new(PathBuf::from(path.resolve()?), table.resolve()?)
                    .map(BuiltSink::Sqlite)
                    .map_err(SqlError::endpoint)
            }
            Self::Discard => Ok(BuiltSink::Discard(DiscardDefinition::new())),
        }
    }

    pub(crate) fn write_identity(&self, encoded: &mut Vec<u8>) -> Result<(), SqlError> {
        match self {
            Self::Postgres(endpoint) => {
                encoded.push(0);
                let connection = DatabaseConnection::postgres(&endpoint.connection, "postgres")?;
                write_identity_bytes(encoded, connection.database.as_bytes());
                let (schema, table) = qualified_table(&endpoint.table, "postgres")?;
                write_identity_bytes(encoded, schema.as_bytes());
                write_identity_bytes(encoded, table.as_bytes());
            }
            Self::Sqlite { path, table } => {
                encoded.push(1);
                path.write_resolved_identity(encoded)?;
                table.write_resolved_identity(encoded)?;
            }
            Self::Discard => encoded.push(2),
        }
        Ok(())
    }

    pub(crate) fn open_runtime_config(&self) -> Result<Option<PostgresSinkConfig>, SqlError> {
        match self {
            Self::Postgres(endpoint) => {
                let connection = DatabaseConnection::postgres(&endpoint.connection, "postgres")?;
                Ok(Some(connection.postgres_sink_config()?))
            }
            Self::Sqlite { .. } | Self::Discard => Ok(None),
        }
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
) -> Result<Vec<Parameter>, SqlError> {
    let mut values = std::iter::repeat_with(|| None)
        .take(expected.len())
        .collect::<Vec<_>>();
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
        let Some((index, specification)) = expected
            .iter()
            .enumerate()
            .find(|(_, specification)| specification.name == name)
        else {
            return Err(SqlError::invalid(format!(
                "unknown {endpoint} parameter {name:?}"
            )));
        };
        if values[index].is_some() {
            return Err(SqlError::invalid(format!(
                "duplicate {endpoint} parameter {name:?}"
            )));
        }
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must be a literal or env('NAME')"
            )));
        };
        values[index] = Some(parse_parameter(
            endpoint,
            specification.name,
            specification.kind,
            expression,
        )?);
    }

    expected
        .iter()
        .zip(values)
        .map(|(specification, value)| {
            value
                .or_else(|| {
                    specification
                        .default
                        .map(|value| Parameter::Literal(value.to_owned()))
                })
                .ok_or_else(|| {
                    SqlError::invalid(format!(
                        "missing {endpoint} parameter {:?}",
                        specification.name
                    ))
                })
        })
        .collect()
}

fn exact_parameters<const N: usize>(values: Vec<Parameter>) -> [Parameter; N] {
    let Ok(values) = values.try_into() else {
        unreachable!("the parameter specification fixes the result length")
    };
    values
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
}
