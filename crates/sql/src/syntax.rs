use datafusion_common::config::SqlParserOptions;
use std::ops::ControlFlow;

use crate::{
    SqlError,
    endpoint::{ScanEndpoint, SinkEndpoint},
};
use datafusion_sql::sqlparser::{
    ast::{
        Distinct as SelectDistinct, Expr, GroupByExpr, Ident, ObjectName, PipeOperator, Query,
        Select, SelectFlavor, TableAlias, TableFactor, TableWithJoins, VisitMut, VisitorMut,
    },
    dialect::GenericDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan},
};

pub(crate) fn parse(sql: &str) -> Result<(SinkEndpoint, Query, Vec<ScanEndpoint>), SqlError> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(SqlParserOptions::default().recursion_limit.get())
        .try_with_sql(sql)?;
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

    let mut collector = ScanCollector::default();
    let _ = query.visit(&mut collector);
    if let Some(error) = collector.error {
        return Err(error);
    }
    Ok((sink, query, collector.scans))
}

#[derive(Default)]
struct ScanCollector {
    scans: Vec<ScanEndpoint>,
    error: Option<SqlError>,
}

impl VisitorMut for ScanCollector {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Err(error) = reject_ignored_query_parts(query) {
            self.error = Some(error);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        if let Err(error) = reject_ignored_select_parts(select) {
            self.error = Some(error);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let Err(error) = reject_ignored_table_parts(table_factor) {
            self.error = Some(error);
            return ControlFlow::Break(());
        }
        let TableFactor::Table {
            name, alias, args, ..
        } = table_factor
        else {
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
                let Some(relation_name) = name.0.last().and_then(|part| part.as_ident()).cloned()
                else {
                    self.error = Some(SqlError::invalid(
                        "a scan function name must be one identifier",
                    ));
                    return ControlFlow::Break(());
                };
                let index = self.scans.len();
                self.scans.push(scan);
                *name = ObjectName::from(Ident::new(internal_scan_name(index)));
                *args = None;
                if alias.is_none() {
                    *alias = Some(TableAlias {
                        explicit: false,
                        name: relation_name,
                        columns: Vec::new(),
                        at: None,
                    });
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

pub(crate) fn internal_scan_name(index: usize) -> String {
    format!("__dogpaddle_sql_scan_{index:08x}")
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

// SqlToRel and lowering own relational capabilities. Reject only syntax whose
// meaning the pinned planner would discard before producing that plan.
fn reject_ignored_query_parts(query: &Query) -> Result<(), SqlError> {
    if !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || query.pipe_operators.iter().any(|pipe| match pipe {
            PipeOperator::Join(join) => join.global,
            PipeOperator::Aggregate {
                full_table_exprs,
                group_by_expr,
            } => full_table_exprs
                .iter()
                .chain(group_by_expr)
                .any(|expr| expr.order_by.sort.is_some() || expr.order_by.nulls_first.is_some()),
            _ => false,
        })
    {
        return Err(SqlError::Unsupported("query modifier".to_owned()));
    }
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            reject_ignored_alias_parts(Some(&cte.alias))?;
            if cte.from.is_some() || cte.materialized.is_some() {
                return Err(SqlError::Unsupported("CTE modifier".to_owned()));
            }
        }
    }
    Ok(())
}

fn reject_ignored_select_parts(select: &Select) -> Result<(), SqlError> {
    let group_modifiers = match &select.group_by {
        GroupByExpr::All(modifiers) | GroupByExpr::Expressions(_, modifiers) => modifiers,
    };
    if !select.optimizer_hints.is_empty()
        || matches!(select.distinct, Some(SelectDistinct::All))
        || select.select_modifiers.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !group_modifiers.is_empty()
        || select.value_table_mode.is_some()
        || select.flavor != SelectFlavor::Standard
    {
        return Err(SqlError::Unsupported("SELECT modifier".to_owned()));
    }
    for table in &select.from {
        reject_global_joins(table)?;
    }
    Ok(())
}

fn reject_global_joins(table: &TableWithJoins) -> Result<(), SqlError> {
    if table.joins.iter().any(|join| join.global) {
        return Err(SqlError::Unsupported("join modifier".to_owned()));
    }
    Ok(())
}

fn reject_ignored_table_parts(relation: &TableFactor) -> Result<(), SqlError> {
    let alias = match relation {
        TableFactor::Table {
            alias,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(SqlError::Unsupported("table modifier".to_owned()));
            }
            alias.as_ref()
        }
        TableFactor::Derived {
            lateral,
            alias,
            sample,
            ..
        } => {
            if *lateral || sample.is_some() {
                return Err(SqlError::Unsupported("table modifier".to_owned()));
            }
            alias.as_ref()
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            reject_global_joins(table_with_joins)?;
            alias.as_ref()
        }
        TableFactor::UNNEST { alias, .. } => alias.as_ref(),
        _ => None,
    };
    reject_ignored_alias_parts(alias)
}

fn reject_ignored_alias_parts(alias: Option<&TableAlias>) -> Result<(), SqlError> {
    if alias.is_some_and(|alias| {
        alias.at.is_some()
            || alias
                .columns
                .iter()
                .any(|column| column.data_type.is_some())
    }) {
        return Err(SqlError::Unsupported("typed or indexed alias".to_owned()));
    }
    Ok(())
}
