use std::{fs, ops::ControlFlow, path::Path};

use datafusion_sql::sqlparser::{
    ast::{
        Cte, Expr, GroupByExpr, Ident, ObjectName, Query, Select, SelectFlavor, SetExpr,
        SetOperator, SetQuantifier, TableAlias, TableAliasColumnDef, TableFactor, TableWithJoins,
        VisitMut, VisitorMut, With,
    },
    dialect::GenericDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan},
};
use dogpaddle_flow::{Flow, FlowFactory};

use crate::{
    SqlError,
    endpoint::{ScanEndpoint, SinkEndpoint},
    lower::{add_sink, internal_scan_name, lower_query, plan, scan_station_id},
};

/// One `INSERT INTO sink(...)` statement and its streaming query.
pub struct SqlProgram {
    sink: SinkEndpoint,
    query: Query,
    scans: Vec<ScanEndpoint>,
}

impl SqlProgram {
    /// Parses exactly one `DogPaddle` SQL program.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid SQL, syntax outside the supported query
    /// subset, any outer statement other than direct `INSERT INTO sink(...) Query`,
    /// or malformed endpoint parameters.
    pub fn parse(sql: &str) -> Result<Self, SqlError> {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(sql)?;
        parser.expect_keyword(Keyword::INSERT)?;
        parser.expect_keyword(Keyword::INTO)?;
        let sink = match parser.parse_expr()? {
            Expr::Function(function) => SinkEndpoint::parse(&function)?,
            _ => {
                return Err(SqlError::invalid(
                    "INSERT INTO requires a sink function call",
                ));
            }
        };
        let mut query = *parser.parse_query()?;
        let _ = parser.consume_token(&Token::SemiColon);
        if parser.peek_token().token != Token::EOF {
            return Err(SqlError::invalid(
                "a SQL file must contain exactly one INSERT statement",
            ));
        }
        if contains_limit_all(&parser.into_tokens()) {
            return Err(SqlError::Unsupported("LIMIT".to_owned()));
        }
        validate_query(&query)?;

        let mut collector = ScanCollector::default();
        let _ = query.visit(&mut collector);
        if let Some(error) = collector.error {
            return Err(error);
        }
        Ok(Self {
            sink,
            query,
            scans: collector.scans,
        })
    }

    /// Reads and parses one UTF-8 SQL file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or its contents do not
    /// form one valid `DogPaddle` SQL program.
    pub fn read(path: impl AsRef<Path>) -> Result<Self, SqlError> {
        let path = path.as_ref();
        let sql = fs::read_to_string(path).map_err(|source| SqlError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&sql)
    }

    /// Discovers external Schemas and targets, lowers the query, and builds a Flow.
    ///
    /// # Errors
    ///
    /// Returns an error for an unresolved environment parameter, failed
    /// discovery, unsupported relational plan, invalid Operation binding, or
    /// Flow construction failure.
    pub fn build(&self, path: impl AsRef<Path>) -> Result<Flow, SqlError> {
        let scans = self
            .scans
            .iter()
            .map(ScanEndpoint::build)
            .collect::<Result<Vec<_>, _>>()?;
        let logical_plan = plan(self.query.clone(), &scans)?;
        let (mut factory, output) = lower_query(FlowFactory::new(path), &logical_plan, scans)?;
        let sink = self.sink.build()?;
        add_sink(&mut factory, output, sink)?;
        factory.build().map_err(Into::into)
    }

    /// Reopens a built Flow using only endpoint runtime resources from this program.
    ///
    /// Persisted topology, expressions, Schemas, and non-sensitive endpoint
    /// identities come from the Flow definition stored at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error for any unresolved environment parameter, invalid
    /// runtime configuration, or Flow open failure.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Flow, SqlError> {
        let mut factory = FlowFactory::new(path);
        for (index, scan) in self.scans.iter().enumerate() {
            if let Some(config) = scan.open_runtime_config()? {
                factory.resource(scan_station_id(index), config)?;
            }
        }
        if let Some(config) = self.sink.open_runtime_config()? {
            factory.resource("sql/sink", config)?;
        }
        factory.open().map_err(Into::into)
    }
}

#[derive(Default)]
struct ScanCollector {
    scans: Vec<ScanEndpoint>,
    error: Option<SqlError>,
}

impl VisitorMut for ScanCollector {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        let TableFactor::Table { name, args, .. } = table_factor else {
            return ControlFlow::Continue(());
        };
        let Some(arguments) = args else {
            if is_internal_scan_reference(name) {
                self.error = Some(SqlError::invalid("reserved SQL relation name"));
                return ControlFlow::Break(());
            }
            return ControlFlow::Continue(());
        };
        match ScanEndpoint::parse(name, arguments) {
            Ok(scan) => {
                let index = self.scans.len();
                self.scans.push(scan);
                *name = ObjectName::from(Ident::new(internal_scan_name(index)));
                if let TableFactor::Table { args, .. } = table_factor {
                    *args = None;
                }
                ControlFlow::Continue(())
            }
            Err(error) => {
                self.error = Some(error);
                ControlFlow::Break(())
            }
        }
    }
}

fn is_internal_scan_reference(name: &ObjectName) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|identifier| {
            let value = if identifier.quote_style.is_none() {
                identifier.value.to_ascii_lowercase()
            } else {
                identifier.value.clone()
            };
            value.starts_with("__dogpaddle_sql_scan_")
        })
}

fn contains_limit_all(tokens: &[TokenWithSpan]) -> bool {
    let mut previous_was_limit = false;
    for token in tokens {
        if matches!(token.token, Token::Whitespace(_) | Token::EOF) {
            continue;
        }
        if previous_was_limit
            && matches!(&token.token, Token::Word(word) if word.keyword == Keyword::ALL)
        {
            return true;
        }
        previous_was_limit =
            matches!(&token.token, Token::Word(word) if word.keyword == Keyword::LIMIT);
    }
    false
}

fn validate_query(query: &Query) -> Result<(), SqlError> {
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;
    if order_by.is_some()
        || limit_clause.is_some()
        || fetch.is_some()
        || !locks.is_empty()
        || for_clause.is_some()
        || settings.is_some()
        || format_clause.is_some()
        || !pipe_operators.is_empty()
    {
        return Err(SqlError::Unsupported("query modifier".to_owned()));
    }
    if let Some(with) = with {
        validate_with(with)?;
    }
    validate_set_expr(body)
}

fn validate_with(with: &With) -> Result<(), SqlError> {
    let With {
        with_token: _,
        recursive,
        cte_tables,
    } = with;
    if *recursive {
        return Err(SqlError::Unsupported("recursive CTE".to_owned()));
    }
    for cte in cte_tables {
        let Cte {
            alias,
            query,
            from,
            materialized,
            closing_paren_token: _,
        } = cte;
        validate_alias(alias)?;
        if from.is_some() || materialized.is_some() {
            return Err(SqlError::Unsupported("CTE modifier".to_owned()));
        }
        validate_query(query)?;
    }
    Ok(())
}

fn validate_set_expr(expression: &SetExpr) -> Result<(), SqlError> {
    match expression {
        SetExpr::Select(select) => validate_select(select),
        SetExpr::Query(query) => validate_query(query),
        SetExpr::SetOperation {
            left,
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            right,
        } => {
            validate_set_expr(left)?;
            validate_set_expr(right)
        }
        SetExpr::SetOperation { .. } => Err(SqlError::Unsupported(
            "set operation other than UNION ALL".to_owned(),
        )),
        SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => Err(SqlError::Unsupported("query body".to_owned())),
    }
}

fn validate_select(select: &Select) -> Result<(), SqlError> {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        top_before_distinct,
        projection: _,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection: _,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify,
        value_table_mode,
        flavor,
    } = select;
    let empty_group_by = matches!(
        group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    );
    if !optimizer_hints.is_empty()
        || distinct.is_some()
        || select_modifiers.is_some()
        || top.is_some()
        || *top_before_distinct
        || exclude.is_some()
        || into.is_some()
        || !lateral_views.is_empty()
        || prewhere.is_some()
        || !connect_by.is_empty()
        || !empty_group_by
        || !cluster_by.is_empty()
        || !distribute_by.is_empty()
        || !sort_by.is_empty()
        || having.is_some()
        || !named_window.is_empty()
        || qualify.is_some()
        || *window_before_qualify
        || value_table_mode.is_some()
        || *flavor != SelectFlavor::Standard
    {
        return Err(SqlError::Unsupported("SELECT modifier".to_owned()));
    }
    for table in from {
        validate_table(table)?;
    }
    Ok(())
}

fn validate_table(table: &TableWithJoins) -> Result<(), SqlError> {
    let TableWithJoins { relation, joins } = table;
    if !joins.is_empty() {
        return Err(SqlError::Unsupported("join".to_owned()));
    }
    match relation {
        TableFactor::Table {
            name: _,
            alias,
            args: _,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } if with_hints.is_empty()
            && version.is_none()
            && !with_ordinality
            && partitions.is_empty()
            && json_path.is_none()
            && sample.is_none()
            && index_hints.is_empty() =>
        {
            validate_optional_alias(alias.as_ref())
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } if !lateral && sample.is_none() => {
            validate_optional_alias(alias.as_ref())?;
            validate_query(subquery)
        }
        _ => Err(SqlError::Unsupported("table modifier".to_owned())),
    }
}

fn validate_optional_alias(alias: Option<&TableAlias>) -> Result<(), SqlError> {
    alias.map_or(Ok(()), validate_alias)
}

fn validate_alias(alias: &TableAlias) -> Result<(), SqlError> {
    let TableAlias {
        explicit: _,
        name: _,
        columns,
        at,
    } = alias;
    let typed_column = columns.iter().any(|column| {
        let TableAliasColumnDef { name: _, data_type } = column;
        data_type.is_some()
    });
    if at.is_some() || typed_column {
        return Err(SqlError::Unsupported("typed or indexed alias".to_owned()));
    }
    Ok(())
}
