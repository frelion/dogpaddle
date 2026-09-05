use std::{env, path::PathBuf};

use datafusion_sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments,
    ObjectName, TableFunctionArgs, Value,
};
use dogpaddle_operation::operation::{
    scan::{PostgresCdcScanConfig, PostgresCdcScanDefinition, SequenceScanDefinition},
    sink::{DiscardDefinition, PostgresSinkConfig, PostgresSinkDefinition, SqliteSinkDefinition},
};

use crate::SqlError;

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

    fn resolve_u16(&self, endpoint: &str, name: &str) -> Result<u16, SqlError> {
        self.resolve()?.parse().map_err(|_| {
            SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must resolve to an unsigned 16-bit integer"
            ))
        })
    }

    fn resolve_u64(&self, endpoint: &str, name: &str) -> Result<u64, SqlError> {
        self.resolve()?.parse().map_err(|_| {
            SqlError::invalid(format!(
                "{endpoint} parameter {name:?} must resolve to an unsigned 64-bit integer"
            ))
        })
    }
}

#[derive(Clone, Copy)]
enum ParameterKind {
    String,
    U16,
    U64,
}

#[derive(Clone, Copy)]
struct ParameterSpec {
    name: &'static str,
    kind: ParameterKind,
}

const fn string(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::String,
    }
}

const fn u16_parameter(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U16,
    }
}

const fn u64_parameter(name: &'static str) -> ParameterSpec {
    ParameterSpec {
        name,
        kind: ParameterKind::U64,
    }
}

struct PostgresConnection {
    host: Parameter,
    port: Parameter,
    database: Parameter,
    user: Parameter,
    password: Parameter,
}

impl PostgresConnection {
    fn cdc_config(&self, runtime_bundle: &Parameter) -> Result<PostgresCdcScanConfig, SqlError> {
        PostgresCdcScanConfig::new_unencrypted(
            PathBuf::from(runtime_bundle.resolve()?),
            self.host.resolve()?,
            self.port.resolve_u16("postgres_cdc", "port")?,
            self.database.resolve()?,
            self.user.resolve()?,
            self.password.resolve()?,
        )
        .map_err(SqlError::endpoint)
    }

    fn sink_config(&self) -> Result<PostgresSinkConfig, SqlError> {
        PostgresSinkConfig::new_unencrypted(
            self.host.resolve()?,
            self.port.resolve_u16("postgres", "port")?,
            self.database.resolve()?,
            self.user.resolve()?,
            self.password.resolve()?,
        )
        .map_err(SqlError::endpoint)
    }
}

pub(crate) struct PostgresCdcEndpoint {
    engine_name: Parameter,
    runtime_bundle: Parameter,
    connection: PostgresConnection,
    schema: Parameter,
    table: Parameter,
    slot: Parameter,
    publication: Parameter,
}

pub(crate) enum ScanEndpoint {
    Sequence { start: Parameter },
    PostgresCdc(Box<PostgresCdcEndpoint>),
}

pub(crate) enum BuiltScan {
    Sequence(SequenceScanDefinition),
    PostgresCdc(Box<BuiltPostgresCdcScan>),
}

pub(crate) struct BuiltPostgresCdcScan {
    pub(crate) definition: PostgresCdcScanDefinition,
    pub(crate) config: PostgresCdcScanConfig,
}

impl ScanEndpoint {
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
            Some("postgres_cdc") => {
                let [
                    engine_name,
                    runtime_bundle,
                    host,
                    port,
                    database,
                    user,
                    password,
                    schema,
                    table,
                    slot,
                    publication,
                ] = exact_parameters(parse_arguments(
                    "postgres_cdc",
                    &arguments.args,
                    &[
                        string("engine_name"),
                        string("runtime_bundle"),
                        string("host"),
                        u16_parameter("port"),
                        string("database"),
                        string("user"),
                        string("password"),
                        string("schema"),
                        string("table"),
                        string("slot"),
                        string("publication"),
                    ],
                )?);
                Ok(Self::PostgresCdc(Box::new(PostgresCdcEndpoint {
                    engine_name,
                    runtime_bundle,
                    connection: PostgresConnection {
                        host,
                        port,
                        database,
                        user,
                        password,
                    },
                    schema,
                    table,
                    slot,
                    publication,
                })))
            }
            _ => Err(SqlError::invalid(format!("unknown scan function {name}"))),
        }
    }

    pub(crate) fn build(&self) -> Result<BuiltScan, SqlError> {
        match self {
            Self::Sequence { start } => Ok(BuiltScan::Sequence(SequenceScanDefinition::new(
                start.resolve_u64("sequence", "start")?,
            ))),
            Self::PostgresCdc(endpoint) => {
                let config = endpoint.connection.cdc_config(&endpoint.runtime_bundle)?;
                let engine_name = endpoint.engine_name.resolve()?;
                let schema = endpoint.schema.resolve()?;
                let table = endpoint.table.resolve()?;
                let slot = endpoint.slot.resolve()?;
                let publication = endpoint.publication.resolve()?;
                let spec = config
                    .discover(&engine_name, &schema, &table, &slot, &publication)
                    .map_err(SqlError::endpoint)?;
                let definition =
                    PostgresCdcScanDefinition::try_new(spec).map_err(SqlError::endpoint)?;
                Ok(BuiltScan::PostgresCdc(Box::new(BuiltPostgresCdcScan {
                    definition,
                    config,
                })))
            }
        }
    }

    pub(crate) fn open_runtime_config(&self) -> Result<Option<PostgresCdcScanConfig>, SqlError> {
        match self {
            Self::Sequence { start } => {
                let _ = start.resolve_u64("sequence", "start")?;
                Ok(None)
            }
            Self::PostgresCdc(endpoint) => {
                let config = endpoint.connection.cdc_config(&endpoint.runtime_bundle)?;
                let _ = endpoint.engine_name.resolve()?;
                let _ = endpoint.schema.resolve()?;
                let _ = endpoint.table.resolve()?;
                let _ = endpoint.slot.resolve()?;
                let _ = endpoint.publication.resolve()?;
                Ok(Some(config))
            }
        }
    }
}

pub(crate) struct PostgresSinkEndpoint {
    sink_id: Parameter,
    connection: PostgresConnection,
    schema: Parameter,
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
                let [sink_id, host, port, database, user, password, schema, table] =
                    exact_parameters(parse_arguments(
                        "postgres",
                        &arguments.args,
                        &[
                            string("sink_id"),
                            string("host"),
                            u16_parameter("port"),
                            string("database"),
                            string("user"),
                            string("password"),
                            string("schema"),
                            string("table"),
                        ],
                    )?);
                Ok(Self::Postgres(PostgresSinkEndpoint {
                    sink_id,
                    connection: PostgresConnection {
                        host,
                        port,
                        database,
                        user,
                        password,
                    },
                    schema,
                    table,
                }))
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

    pub(crate) fn build(&self) -> Result<BuiltSink, SqlError> {
        match self {
            Self::Postgres(endpoint) => {
                let config = endpoint.connection.sink_config()?;
                let sink_id = endpoint.sink_id.resolve()?;
                let schema = endpoint.schema.resolve()?;
                let table = endpoint.table.resolve()?;
                let target = config
                    .discover_target(sink_id, schema, table)
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

    pub(crate) fn open_runtime_config(&self) -> Result<Option<PostgresSinkConfig>, SqlError> {
        match self {
            Self::Postgres(endpoint) => {
                let config = endpoint.connection.sink_config()?;
                let _ = endpoint.sink_id.resolve()?;
                let _ = endpoint.schema.resolve()?;
                let _ = endpoint.table.resolve()?;
                Ok(Some(config))
            }
            Self::Sqlite { path, table } => {
                let _ = path.resolve()?;
                let _ = table.resolve()?;
                Ok(None)
            }
            Self::Discard => Ok(None),
        }
    }
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
            value.ok_or_else(|| {
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
            (Value::Number(value, _), ParameterKind::U16) => {
                value.parse::<u16>().map_err(|_| invalid())?;
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
