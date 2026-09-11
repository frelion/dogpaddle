use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion_common::{Column, DFSchema, DataFusionError, TableReference, config::ConfigOptions};
use datafusion_expr::{
    AggregateUDF, Distinct as LogicalDistinct, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF,
    TableSource, WindowUDF, expr::AggregateFunction, expr_rewriter::unnormalize_col,
    logical_plan::Aggregate, planner::ExprPlanner,
};
use datafusion_functions_aggregate::planner::AggregateFunctionPlanner;
use datafusion_optimizer::{Analyzer, analyzer::type_coercion::TypeCoercion};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use dogpaddle_operation::{
    OperationDefinition,
    operation::transform::{
        AggregateCall, AggregateDefinition, DistinctDefinition, FilterDefinition,
        SchemaAlignDefinition, SchemaAlignField, UnionAllDefinition,
    },
};

use crate::{
    SqlError,
    aggregate::{lower as lower_builtin_aggregate, planning_builtins},
    compiler::{LogicalArena, LogicalNodeId, LogicalOperator, LogicalQuery, TransformDefinition},
    endpoint::BuiltScan,
};

#[derive(Debug)]
struct ScanSource {
    index: usize,
    schema: SchemaRef,
}

impl TableSource for ScanSource {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct PlanningContext {
    options: ConfigOptions,
    sources: HashMap<String, Arc<ScanSource>>,
    aggregates: HashMap<&'static str, Arc<AggregateUDF>>,
    expression_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl PlanningContext {
    fn new(scans: &[BuiltScan]) -> Result<Self, SqlError> {
        let mut options = ConfigOptions::default();
        options.sql_parser.map_string_types_to_utf8view = false;
        let sources = scans
            .iter()
            .enumerate()
            .map(|(index, scan)| {
                Ok((
                    internal_scan_name(index),
                    Arc::new(ScanSource {
                        index,
                        schema: scan_schema(scan)?,
                    }),
                ))
            })
            .collect::<Result<_, SqlError>>()?;
        let aggregates = planning_builtins();
        Ok(Self {
            options,
            sources,
            aggregates,
            expression_planners: vec![Arc::new(AggregateFunctionPlanner)],
        })
    }
}

impl ContextProvider for PlanningContext {
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expression_planners
    }

    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        self.sources
            .get(name.table())
            .map(|source| Arc::clone(source) as Arc<dyn TableSource>)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("table {name} is not a DogPaddle scan function"))
            })
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregates.get(name).map(Arc::clone)
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow_schema::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.aggregates.keys().map(ToString::to_string).collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

pub(crate) fn plan(
    query: datafusion_sql::sqlparser::ast::Query,
    scans: &[BuiltScan],
) -> Result<LogicalPlan, SqlError> {
    let context = PlanningContext::new(scans)?;
    let plan = SqlToRel::new(&context).sql_statement_to_plan(Statement::Query(Box::new(query)))?;
    Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())])
        .execute_and_check(plan, &context.options, |_, _| {})
        .map_err(Into::into)
}

pub(crate) fn lower_query(
    plan: &LogicalPlan,
    scans: Vec<BuiltScan>,
) -> Result<LogicalQuery, SqlError> {
    let mut lowerer = Lowerer {
        arena: LogicalArena::default(),
        scans: scans.into_iter().map(Some).collect(),
        scan_nodes: HashMap::new(),
    };
    let output = lowerer.lower(plan)?;
    if lowerer.scans.iter().any(Option::is_some) {
        return Err(SqlError::invalid(
            "every declared scan must be reachable from the query result",
        ));
    }
    Ok(LogicalQuery::new(lowerer.arena, output))
}

struct Lowerer {
    arena: LogicalArena,
    scans: Vec<Option<BuiltScan>>,
    scan_nodes: HashMap<usize, LogicalNodeId>,
}

impl Lowerer {
    fn lower(&mut self, plan: &LogicalPlan) -> Result<LogicalNodeId, SqlError> {
        match plan {
            LogicalPlan::TableScan(scan) => self.lower_scan(scan),
            LogicalPlan::Filter(filter) => {
                let input = self.lower(&filter.input)?;
                let definition =
                    FilterDefinition::try_new(unnormalize_col(filter.predicate.clone()))
                        .map_err(SqlError::endpoint)?;
                Ok(self.add_transform([input], definition))
            }
            LogicalPlan::Projection(projection) => {
                let input = self.lower(&projection.input)?;
                let fields = projection
                    .expr
                    .iter()
                    .cloned()
                    .zip(projection.schema.fields())
                    .map(|(expression, field)| {
                        SchemaAlignField::try_new_with_metadata(
                            field.name().to_owned(),
                            unnormalize_col(expression.unalias()),
                            field.is_nullable(),
                            field.metadata().clone(),
                        )
                        .map_err(SqlError::endpoint)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let definition = SchemaAlignDefinition::try_new_with_metadata(
                    fields,
                    projection.schema.metadata().clone(),
                )
                .map_err(SqlError::endpoint)?;
                Ok(self.add_transform([input], definition))
            }
            LogicalPlan::Distinct(LogicalDistinct::All(input)) => {
                let input = self.lower(input)?;
                Ok(self.add_transform([input], DistinctDefinition::new()))
            }
            LogicalPlan::Aggregate(aggregate) => self.lower_aggregate(aggregate),
            LogicalPlan::Union(union) => {
                let inputs = union
                    .inputs
                    .iter()
                    .map(|plan| {
                        let input = self.lower(plan)?;
                        self.align_union_input(input, plan.schema(), union.schema.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let input_count = u32::try_from(inputs.len())
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| SqlError::invalid("UNION ALL has too many inputs"))?;
                let definition = UnionAllDefinition::new(input_count);
                Ok(self.add_transform(inputs, definition))
            }
            LogicalPlan::SubqueryAlias(alias) => self.lower(&alias.input),
            _ => Err(SqlError::Unsupported(format!(
                "relational plan node {}",
                plan.display()
            ))),
        }
    }

    fn lower_aggregate(&mut self, aggregate: &Aggregate) -> Result<LogicalNodeId, SqlError> {
        if aggregate.group_expr.is_empty() {
            return Err(SqlError::Unsupported("global aggregate".to_owned()));
        }
        if aggregate
            .group_expr
            .iter()
            .any(|expression| matches!(expression, Expr::GroupingSet(_)))
        {
            return Err(SqlError::Unsupported("grouping set".to_owned()));
        }

        let input = self.lower(&aggregate.input)?;
        let group_count = aggregate.group_expr.len();
        let groups = aggregate
            .group_expr
            .iter()
            .zip(&aggregate.schema.fields()[..group_count])
            .map(|(expression, field)| {
                (field.name().to_owned(), unnormalize_col(expression.clone()))
            });
        let calls = aggregate
            .aggr_expr
            .iter()
            .zip(&aggregate.schema.fields()[group_count..])
            .map(|(expression, field)| {
                Ok((field.name().to_owned(), lower_aggregate_call(expression)?))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        let definition = AggregateDefinition::try_new(groups, calls).map_err(SqlError::endpoint)?;
        Ok(self.add_transform([input], definition))
    }

    fn lower_scan(
        &mut self,
        scan: &datafusion_expr::logical_plan::TableScan,
    ) -> Result<LogicalNodeId, SqlError> {
        if scan.projection.is_some() || !scan.filters.is_empty() || scan.fetch.is_some() {
            return Err(SqlError::invalid(
                "DataFusion embedded projection, filter, or fetch in a scan",
            ));
        }
        let source = scan
            .source
            .downcast_ref::<ScanSource>()
            .ok_or_else(|| SqlError::invalid("logical plan contains a foreign table source"))?;
        if let Some(node) = self.scan_nodes.get(&source.index) {
            return Ok(*node);
        }
        let built = self
            .scans
            .get_mut(source.index)
            .and_then(Option::take)
            .ok_or_else(|| SqlError::invalid("logical plan references an unknown scan"))?;
        let node = self.arena.push(
            [],
            LogicalOperator::Scan {
                source_index: source.index,
                definition: built,
            },
        );
        self.scan_nodes.insert(source.index, node);
        Ok(node)
    }

    fn align_union_input(
        &mut self,
        input: LogicalNodeId,
        input_schema: &DFSchema,
        union_schema: &DFSchema,
    ) -> Result<LogicalNodeId, SqlError> {
        if input_schema.as_arrow() == union_schema.as_arrow() {
            return Ok(input);
        }
        if input_schema.fields().len() != union_schema.fields().len() {
            return Err(SqlError::invalid(
                "DataFusion produced incompatible UNION ALL field counts",
            ));
        }
        let fields = input_schema
            .fields()
            .iter()
            .zip(union_schema.fields())
            .map(|(source, target)| {
                SchemaAlignField::try_new_with_metadata(
                    target.name().to_owned(),
                    Expr::Column(Column::new_unqualified(source.name())),
                    target.is_nullable(),
                    target.metadata().clone(),
                )
                .map_err(SqlError::endpoint)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let definition =
            SchemaAlignDefinition::try_new_with_metadata(fields, union_schema.metadata().clone())
                .map_err(SqlError::endpoint)?;
        Ok(self.add_transform([input], definition))
    }

    fn add_transform<I, D>(&mut self, inputs: I, definition: D) -> LogicalNodeId
    where
        I: IntoIterator<Item = LogicalNodeId>,
        D: Into<TransformDefinition>,
    {
        self.arena
            .push(inputs, LogicalOperator::Transform(definition.into()))
    }
}

fn lower_aggregate_call(expression: &Expr) -> Result<AggregateCall, SqlError> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expression.clone().unalias()
    else {
        return Err(SqlError::invalid(
            "DataFusion aggregate node contains a non-aggregate expression",
        ));
    };
    if params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
    {
        return Err(SqlError::Unsupported("aggregate modifier".to_owned()));
    }
    let arguments = params.args.into_iter().map(unnormalize_col).collect();
    lower_builtin_aggregate(func.name(), arguments)
        .ok_or_else(|| SqlError::Unsupported(format!("aggregate function {}", func.name())))
}

fn scan_schema(scan: &BuiltScan) -> Result<SchemaRef, SqlError> {
    let definition: &dyn OperationDefinition = match scan {
        BuiltScan::Sequence(definition) => definition,
        BuiltScan::PostgresCdc(scan) => &scan.definition,
        BuiltScan::MySqlCdc(scan) => &scan.definition,
    };
    let binding = definition.bind(&[]).map_err(SqlError::endpoint)?;
    binding
        .output_schema()
        .cloned()
        .ok_or_else(|| SqlError::invalid("scan definition has no output Schema"))
}

pub(crate) fn internal_scan_name(index: usize) -> String {
    format!("__dogpaddle_sql_scan_{index:08x}")
}
