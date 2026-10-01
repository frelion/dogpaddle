use std::{
    collections::{BTreeSet, HashMap},
    num::NonZeroU32,
    sync::Arc,
};

use arrow_schema::SchemaRef;
use datafusion_common::{
    Column, DFSchema, DataFusionError, JoinConstraint, JoinType, NullEquality, TableReference,
    config::ConfigOptions,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_expr::{
    AggregateUDF, Distinct as LogicalDistinct, Expr, ExprSchemable, HigherOrderUDF, LogicalPlan,
    Operator, ScalarUDF, TableSource, WindowUDF,
    expr::{AggregateFunction, BinaryExpr},
    logical_plan::{Aggregate, AsOfJoin, Join},
    planner::ExprPlanner,
    utils::{find_valid_equijoin_key_pair, split_conjunction_owned},
};
use datafusion_functions_aggregate::planner::AggregateFunctionPlanner;
use datafusion_optimizer::{
    Analyzer, Optimizer, OptimizerContext, analyzer::type_coercion::TypeCoercion,
    optimize_projections::OptimizeProjections,
};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use dogpaddle_flow::{FlowFactory, OperationRef};
use dogpaddle_operation::{
    OperationDefinition,
    operation::transform::{
        AggregateCall, AggregateDefinition, AsOfDirection, AsOfEqualityKey, AsOfJoinDefinition,
        AsOfOrderKey, DistinctDefinition, EquiJoinDefinition, EquiJoinKind, FilterDefinition,
        SelectDefinition, SelectField, UnionAllDefinition,
    },
};

use crate::{
    SqlError,
    aggregate::{lower as lower_builtin_aggregate, planning_builtins},
    program::scan_operation_id,
    syntax::internal_scan_name,
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
    fn new(scans: &[OperationDefinition]) -> Result<Self, SqlError> {
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
    scans: &[OperationDefinition],
) -> Result<LogicalPlan, SqlError> {
    let context = PlanningContext::new(scans)?;
    let plan = SqlToRel::new(&context).sql_statement_to_plan(Statement::Query(Box::new(query)))?;
    let plan = Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())])
        .execute_and_check(plan, &context.options, |_, _| {})
        .map_err(SqlError::from)?;
    let optimizer = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())]);
    let optimizer_context =
        OptimizerContext::new_with_config_options(Arc::new(context.options.clone()));
    optimizer
        .optimize(plan, &optimizer_context, |_, _| {})
        .map_err(Into::into)
}

pub(crate) fn lower_query(
    plan: &LogicalPlan,
    scans: Vec<(OperationDefinition, Vec<usize>)>,
    factory: &mut FlowFactory,
) -> Result<OperationRef, SqlError> {
    let mut lowerer = Lowerer {
        factory,
        next_transform: 0,
        scans: scans
            .into_iter()
            .map(|(definition, projection)| ScanBinding {
                definition: Some(definition),
                projection,
                relation: None,
            })
            .collect(),
    };
    let output = lowerer.lower(plan)?;
    // Join lowering keeps unique physical names so parent expressions remain
    // unambiguous. The optimizer can remove the root projection that would
    // otherwise restore the SQL names, so bind the final positional Schema.
    let output_schema = Arc::new(plan.schema().as_arrow().clone());
    let output_positions = (0..output_schema.fields().len()).collect::<Vec<_>>();
    let output = lowerer.project_columns(output, &output_positions, &output_schema)?;
    Ok(output.node)
}

#[derive(Clone)]
struct LoweredRelation {
    node: OperationRef,
    physical_schema: SchemaRef,
}

struct ScanBinding {
    // The first occurrence moves definition into FlowFactory and records relation.
    // Later occurrences reuse that relation with their own projected columns.
    definition: Option<OperationDefinition>,
    projection: Vec<usize>,
    relation: Option<LoweredRelation>,
}

type EquiJoinKey = (Expr, Expr);

struct OrientedJoin {
    kind: EquiJoinKind,
    inputs: [LoweredRelation; 2],
    keys: Vec<EquiJoinKey>,
    source_order: Vec<usize>,
    swapped: bool,
}

struct Lowerer<'a> {
    factory: &'a mut FlowFactory,
    next_transform: usize,
    scans: Vec<ScanBinding>,
}

impl Lowerer<'_> {
    fn lower(&mut self, plan: &LogicalPlan) -> Result<LoweredRelation, SqlError> {
        match plan {
            LogicalPlan::TableScan(scan) => self.lower_scan(scan),
            LogicalPlan::Filter(filter) => {
                let input = self.lower(&filter.input)?;
                let predicate = rewrite_columns(
                    filter.predicate.clone(),
                    filter.input.schema(),
                    &input.physical_schema,
                )?;
                let definition =
                    FilterDefinition::try_new(predicate).map_err(SqlError::endpoint)?;
                self.add_transform([input], definition)
            }
            LogicalPlan::Projection(projection) => {
                let input = self.lower(&projection.input)?;
                let fields = projection
                    .expr
                    .iter()
                    .cloned()
                    .zip(projection.schema.fields())
                    .map(|(expression, field)| {
                        Ok(SelectField {
                            name: field.name().to_owned(),
                            expression: rewrite_columns(
                                expression.unalias(),
                                projection.input.schema(),
                                &input.physical_schema,
                            )?,
                            nullable: Some(field.is_nullable()),
                            metadata: Some(field.metadata().clone()),
                        })
                    })
                    .collect::<Result<Vec<_>, SqlError>>()?;
                let definition = SelectDefinition::try_new(fields)
                    .map_err(SqlError::endpoint)?
                    .with_metadata(projection.schema.metadata().clone());
                self.add_transform([input], definition)
            }
            LogicalPlan::Distinct(LogicalDistinct::All(input)) => {
                let input = self.lower(input)?;
                self.add_transform([input], DistinctDefinition::new())
            }
            LogicalPlan::AsOfJoin(join) => self.lower_asof_join(join),
            LogicalPlan::Join(join) => self.lower_join(join),
            LogicalPlan::Aggregate(aggregate) => self.lower_aggregate(aggregate),
            LogicalPlan::Union(union) => {
                let inputs = union
                    .inputs
                    .iter()
                    .map(|plan| {
                        let input = self.lower(plan)?;
                        self.align_union_input(input, union.schema.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let input_count = u32::try_from(inputs.len())
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| SqlError::invalid("UNION ALL has too many inputs"))?;
                let definition = UnionAllDefinition::new(input_count);
                self.add_transform(inputs, definition)
            }
            LogicalPlan::SubqueryAlias(alias) => self.lower(&alias.input),
            _ => Err(SqlError::Unsupported(format!(
                "relational plan node {}",
                plan.display()
            ))),
        }
    }

    fn lower_asof_join(&mut self, join: &AsOfJoin) -> Result<LoweredRelation, SqlError> {
        if !matches!(
            join.join_constraint,
            JoinConstraint::On | JoinConstraint::Using
        ) {
            return Err(SqlError::Unsupported(
                "ASOF join constraint semantics".to_owned(),
            ));
        }

        let left = self.lower(&join.left)?;
        let right = self.lower(&join.right)?;
        let equalities = join
            .on
            .iter()
            .map(|(left_key, right_key)| {
                Ok(AsOfEqualityKey::new(
                    rewrite_columns(left_key.clone(), join.left.schema(), &left.physical_schema)?,
                    rewrite_columns(
                        right_key.clone(),
                        join.right.schema(),
                        &right.physical_schema,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        let direction = asof_direction(join.match_condition.op)?;
        let order = AsOfOrderKey::new(
            rewrite_columns(
                join.match_condition.left.clone(),
                join.left.schema(),
                &left.physical_schema,
            )?,
            rewrite_columns(
                join.match_condition.right.clone(),
                join.right.schema(),
                &right.physical_schema,
            )?,
        );
        let output_count =
            left.physical_schema.fields().len() + right.physical_schema.fields().len();
        let source_order = (0..output_count).collect::<Vec<_>>();
        let definition = AsOfJoinDefinition::try_new(
            direction,
            equalities,
            order,
            (0..output_count).map(internal_join_field_name),
        )
        .map_err(SqlError::endpoint)?;
        let joined = self.add_transform([left, right], definition)?;
        self.align_join_output(joined, join.schema.as_ref(), &source_order)
    }

    fn lower_join(&mut self, join: &Join) -> Result<LoweredRelation, SqlError> {
        if join.join_constraint != JoinConstraint::On
            || join.null_equality != NullEquality::NullEqualsNothing
            || join.null_aware
        {
            return Err(SqlError::Unsupported("join type or semantics".to_owned()));
        }

        let (logical_keys, residuals) = collect_join_condition(join)?;
        if logical_keys.is_empty() {
            return Err(unsupported_join_condition());
        }

        let left = self.lower(&join.left)?;
        let right = self.lower(&join.right)?;
        let keys = logical_keys
            .into_iter()
            .map(|(left_key, right_key)| {
                Ok((
                    rewrite_columns(left_key, join.left.schema(), &left.physical_schema)?,
                    rewrite_columns(right_key, join.right.schema(), &right.physical_schema)?,
                ))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        let OrientedJoin {
            kind,
            inputs,
            keys,
            source_order,
            swapped,
        } = orient_join(join.join_type, left, right, keys)?;
        let residual = residuals
            .into_iter()
            .reduce(Expr::and)
            .map(|residual| {
                rewrite_join_residual(
                    residual,
                    join.left.schema(),
                    join.right.schema(),
                    &inputs,
                    swapped,
                )
            })
            .transpose()?;
        let output_count = source_order.len();
        let definition = EquiJoinDefinition::try_new(
            kind,
            keys,
            (0..output_count).map(internal_join_field_name),
            residual,
        )
        .map_err(SqlError::endpoint)?;
        let joined = self.add_transform(inputs, definition)?;
        self.align_join_output(joined, join.schema.as_ref(), &source_order)
    }

    fn lower_aggregate(&mut self, aggregate: &Aggregate) -> Result<LoweredRelation, SqlError> {
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
                Ok((
                    field.name().to_owned(),
                    rewrite_columns(
                        expression.clone(),
                        aggregate.input.schema(),
                        &input.physical_schema,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        let calls = aggregate
            .aggr_expr
            .iter()
            .zip(&aggregate.schema.fields()[group_count..])
            .map(|(expression, field)| {
                Ok((
                    field.name().to_owned(),
                    lower_aggregate_call(
                        expression,
                        aggregate.input.schema(),
                        &input.physical_schema,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, SqlError>>()?;
        let definition = AggregateDefinition::try_new(groups, calls).map_err(SqlError::endpoint)?;
        self.add_transform([input], definition)
    }

    fn lower_scan(
        &mut self,
        scan: &datafusion_expr::logical_plan::TableScan,
    ) -> Result<LoweredRelation, SqlError> {
        if !scan.filters.is_empty() || scan.fetch.is_some() {
            return Err(SqlError::invalid(
                "DataFusion embedded filter or fetch in a scan",
            ));
        }
        let source = scan
            .source
            .downcast_ref::<ScanSource>()
            .ok_or_else(|| SqlError::invalid("logical plan contains a foreign table source"))?;
        let occurrence = scan_projection(scan, source)?;
        let shared = if let Some(relation) = self
            .scans
            .get(source.index)
            .and_then(|scan| scan.relation.as_ref())
        {
            relation.clone()
        } else {
            let binding = self
                .scans
                .get_mut(source.index)
                .ok_or_else(|| SqlError::invalid("logical plan references an unknown scan"))?;
            let definition = binding
                .definition
                .take()
                .ok_or_else(|| SqlError::invalid("logical plan references an unknown scan"))?;
            let projection = binding.projection.clone();
            let physical_schema = definition
                .output_schema(&[])
                .map_err(SqlError::endpoint)?
                .ok_or_else(|| SqlError::invalid("scan definition has no output Schema"))?;
            let node = self
                .factory
                .operation(scan_operation_id(source.index), definition, []);
            let relation = LoweredRelation {
                node,
                physical_schema,
            };
            let schema =
                Arc::new(source.schema.project(&projection).map_err(|_| {
                    SqlError::invalid("scan projection is outside its source Schema")
                })?);
            let relation = if relation.physical_schema.as_ref() == source.schema.as_ref() {
                self.project_columns(relation, &projection, &schema)?
            } else if relation.physical_schema.as_ref() == schema.as_ref() {
                relation
            } else {
                return Err(SqlError::invalid(
                    "scan Definition output differs from its full and shared projected Schemas",
                ));
            };
            self.scans[source.index].relation = Some(relation.clone());
            relation
        };
        let shared_projection = &self.scans[source.index].projection;
        let positions = occurrence
            .iter()
            .map(|index| {
                shared_projection.binary_search(index).map_err(|_| {
                    SqlError::invalid("scan occurrence projection is outside its shared projection")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(scan.projected_schema.as_arrow().clone());
        self.project_columns(shared, &positions, &schema)
    }

    fn project_columns(
        &mut self,
        input: LoweredRelation,
        positions: &[usize],
        target: &SchemaRef,
    ) -> Result<LoweredRelation, SqlError> {
        if positions.len() != target.fields().len()
            || positions
                .iter()
                .any(|position| *position >= input.physical_schema.fields().len())
        {
            return Err(SqlError::invalid(
                "DataFusion projection and physical output shape diverged",
            ));
        }
        if input.physical_schema.as_ref() == target.as_ref()
            && positions.iter().copied().eq(0..positions.len())
        {
            return Ok(input);
        }
        let fields = positions
            .iter()
            .zip(target.fields())
            .map(|(position, target)| SelectField {
                name: target.name().to_owned(),
                expression: Expr::Column(Column::new_unqualified(
                    input.physical_schema.field(*position).name(),
                )),
                nullable: Some(target.is_nullable()),
                metadata: Some(target.metadata().clone()),
            })
            .collect::<Vec<_>>();
        let definition = SelectDefinition::try_new(fields)
            .map_err(SqlError::endpoint)?
            .with_metadata(target.metadata().clone());
        self.add_transform([input], definition)
    }

    fn align_union_input(
        &mut self,
        input: LoweredRelation,
        union_schema: &DFSchema,
    ) -> Result<LoweredRelation, SqlError> {
        let target = Arc::new(union_schema.as_arrow().clone());
        let positions = (0..target.fields().len()).collect::<Vec<_>>();
        self.project_columns(input, &positions, &target)
    }

    fn align_join_output(
        &mut self,
        input: LoweredRelation,
        join_schema: &DFSchema,
        source_order: &[usize],
    ) -> Result<LoweredRelation, SqlError> {
        if source_order.len() != join_schema.fields().len()
            || source_order
                .iter()
                .any(|source| *source >= input.physical_schema.fields().len())
        {
            return Err(SqlError::invalid(
                "DataFusion JOIN Schema and physical output shape diverged",
            ));
        }
        let fields = source_order
            .iter()
            .zip(join_schema.fields())
            .enumerate()
            .map(|(index, (source, target))| SelectField {
                name: internal_join_field_name(index),
                expression: Expr::Column(Column::new_unqualified(
                    input.physical_schema.field(*source).name(),
                )),
                nullable: Some(target.is_nullable()),
                metadata: Some(target.metadata().clone()),
            })
            .collect::<Vec<_>>();
        let definition = SelectDefinition::try_new(fields)
            .map_err(SqlError::endpoint)?
            .with_metadata(join_schema.metadata().clone());
        self.add_transform([input], definition)
    }

    fn add_transform<I, D>(&mut self, inputs: I, definition: D) -> Result<LoweredRelation, SqlError>
    where
        I: IntoIterator<Item = LoweredRelation>,
        D: Into<OperationDefinition>,
    {
        let inputs = inputs.into_iter().collect::<Vec<_>>();
        let input_schemas = inputs
            .iter()
            .map(|input| Arc::clone(&input.physical_schema))
            .collect::<Vec<_>>();
        let definition: OperationDefinition = definition.into();
        let physical_schema = definition
            .output_schema(&input_schemas)
            .map_err(SqlError::endpoint)?
            .ok_or_else(|| SqlError::invalid("transform definition has no output Schema"))?;
        let id = format!("sql/transform/{:08x}", self.next_transform);
        self.next_transform += 1;
        let node =
            self.factory
                .operation(id, definition, inputs.into_iter().map(|input| input.node));
        Ok(LoweredRelation {
            node,
            physical_schema,
        })
    }
}

pub(crate) fn scan_projections(
    plan: &LogicalPlan,
    scan_count: usize,
) -> Result<Vec<Vec<usize>>, SqlError> {
    fn collect(plan: &LogicalPlan, unions: &mut [Option<BTreeSet<usize>>]) -> Result<(), SqlError> {
        if let LogicalPlan::TableScan(scan) = plan {
            let source = scan
                .source
                .downcast_ref::<ScanSource>()
                .ok_or_else(|| SqlError::invalid("logical plan contains a foreign table source"))?;
            let projection = scan_projection(scan, source)?;
            unions
                .get_mut(source.index)
                .ok_or_else(|| SqlError::invalid("logical plan references an unknown scan"))?
                .get_or_insert_with(BTreeSet::new)
                .extend(projection);
        }
        for input in plan.inputs() {
            collect(input, unions)?;
        }
        Ok(())
    }

    let mut unions = vec![None; scan_count];
    collect(plan, &mut unions)?;
    unions
        .into_iter()
        .map(|projection| {
            projection
                .map(|indices| indices.into_iter().collect())
                .ok_or_else(|| {
                    SqlError::invalid("every declared scan must be reachable from the query result")
                })
        })
        .collect()
}

fn scan_projection(
    scan: &datafusion_expr::logical_plan::TableScan,
    source: &ScanSource,
) -> Result<Vec<usize>, SqlError> {
    let projection = scan
        .projection
        .clone()
        .unwrap_or_else(|| (0..source.schema.fields().len()).collect());
    if projection.len() != scan.projected_schema.fields().len() {
        return Err(SqlError::invalid(
            "DataFusion scan projection and projected Schema diverged",
        ));
    }
    for (projected, index) in scan
        .projected_schema
        .as_arrow()
        .fields()
        .iter()
        .zip(&projection)
    {
        if source.schema.fields().get(*index) != Some(projected) {
            return Err(SqlError::invalid(
                "DataFusion scan projection field differs from its source Schema",
            ));
        }
    }
    Ok(projection)
}

fn lower_aggregate_call(
    expression: &Expr,
    logical_schema: &DFSchema,
    physical_schema: &SchemaRef,
) -> Result<AggregateCall, SqlError> {
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
    let arguments = params
        .args
        .into_iter()
        .map(|argument| rewrite_columns(argument, logical_schema, physical_schema))
        .collect::<Result<Vec<_>, _>>()?;
    lower_builtin_aggregate(func.name(), arguments)
        .ok_or_else(|| SqlError::Unsupported(format!("aggregate function {}", func.name())))
}

fn collect_join_condition(join: &Join) -> Result<(Vec<EquiJoinKey>, Vec<Expr>), SqlError> {
    let mut keys = Vec::with_capacity(join.on.len() + usize::from(join.filter.is_some()));
    let mut residuals = Vec::new();
    for (left_key, right_key) in &join.on {
        if let Some(key) =
            supported_join_key(left_key, right_key, join.left.schema(), join.right.schema())?
        {
            keys.push(key);
        } else {
            residuals.push(left_key.clone().eq(right_key.clone()));
        }
    }

    if let Some(filter) = &join.filter {
        for predicate in split_conjunction_owned(filter.clone()) {
            let key = match &predicate {
                Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) => supported_join_key(left, right, join.left.schema(), join.right.schema())?,
                _ => None,
            };
            if let Some(key) = key {
                keys.push(key);
            } else {
                residuals.push(predicate);
            }
        }
    }
    Ok((keys, residuals))
}

fn supported_join_key(
    left_key: &Expr,
    right_key: &Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<Option<EquiJoinKey>, SqlError> {
    let Some((left_key, right_key)) =
        find_valid_equijoin_key_pair(left_key, right_key, left_schema, right_schema)?
    else {
        return Ok(None);
    };
    let left_type = left_key.get_type(left_schema)?;
    let right_type = right_key.get_type(right_schema)?;
    if left_type == right_type && EquiJoinDefinition::supports_key_type(&left_type) {
        Ok(Some((left_key, right_key)))
    } else {
        Ok(None)
    }
}

fn orient_join(
    join_type: JoinType,
    left: LoweredRelation,
    right: LoweredRelation,
    keys: Vec<EquiJoinKey>,
) -> Result<OrientedJoin, SqlError> {
    let left_count = left.physical_schema.fields().len();
    let right_count = right.physical_schema.fields().len();
    let oriented = match join_type {
        JoinType::Inner
        | JoinType::Left
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::LeftAnti => {
            let (kind, output_count) = match join_type {
                JoinType::Inner => (EquiJoinKind::Inner, left_count + right_count),
                JoinType::Left => (EquiJoinKind::LeftOuter, left_count + right_count),
                JoinType::Full => (EquiJoinKind::FullOuter, left_count + right_count),
                JoinType::LeftSemi => (EquiJoinKind::LeftSemi, left_count),
                JoinType::LeftAnti => (EquiJoinKind::LeftAnti, left_count),
                _ => unreachable!("the outer match selected a non-right JOIN"),
            };
            OrientedJoin {
                kind,
                inputs: [left, right],
                keys,
                source_order: (0..output_count).collect(),
                swapped: false,
            }
        }
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
            let (kind, output_count) = match join_type {
                JoinType::Right => (EquiJoinKind::LeftOuter, left_count + right_count),
                JoinType::RightSemi => (EquiJoinKind::LeftSemi, right_count),
                JoinType::RightAnti => (EquiJoinKind::LeftAnti, right_count),
                _ => unreachable!("the outer match selected a right-preserving JOIN"),
            };
            let source_order = if join_type == JoinType::Right {
                (right_count..right_count + left_count)
                    .chain(0..right_count)
                    .collect()
            } else {
                (0..output_count).collect()
            };
            OrientedJoin {
                kind,
                inputs: [right, left],
                keys: keys
                    .into_iter()
                    .map(|(left, right)| (right, left))
                    .collect(),
                source_order,
                swapped: true,
            }
        }
        _ => return Err(SqlError::Unsupported("join type or semantics".to_owned())),
    };
    Ok(oriented)
}

fn unsupported_join_condition() -> SqlError {
    SqlError::Unsupported("JOIN without a cross-input equality key".to_owned())
}

fn asof_direction(operator: Operator) -> Result<AsOfDirection, SqlError> {
    match operator {
        Operator::Lt => Ok(AsOfDirection::Forward { allow_exact: false }),
        Operator::LtEq => Ok(AsOfDirection::Forward { allow_exact: true }),
        Operator::Gt => Ok(AsOfDirection::Backward { allow_exact: false }),
        Operator::GtEq => Ok(AsOfDirection::Backward { allow_exact: true }),
        operator => Err(SqlError::invalid(format!(
            "DataFusion produced unsupported ASOF match operator {operator}"
        ))),
    }
}

fn rewrite_join_residual(
    expression: Expr,
    logical_left: &DFSchema,
    logical_right: &DFSchema,
    oriented_inputs: &[LoweredRelation; 2],
    swapped: bool,
) -> Result<Expr, SqlError> {
    let (physical_left, physical_right, left_port, right_port) = if swapped {
        (
            &oriented_inputs[1].physical_schema,
            &oriented_inputs[0].physical_schema,
            "right",
            "left",
        )
    } else {
        (
            &oriented_inputs[0].physical_schema,
            &oriented_inputs[1].physical_schema,
            "left",
            "right",
        )
    };
    if logical_left.fields().len() != physical_left.fields().len()
        || logical_right.fields().len() != physical_right.fields().len()
    {
        return Err(SqlError::invalid(
            "logical and physical JOIN input field counts diverged while lowering SQL",
        ));
    }

    expression
        .transform_up(|nested| match nested {
            Expr::Column(column) => {
                let left = logical_left.maybe_index_of_column(&column);
                let right = logical_right.maybe_index_of_column(&column);
                let (physical, port, index) = match (left, right) {
                    (Some(index), None) => (physical_left, left_port, index),
                    (None, Some(index)) => (physical_right, right_port, index),
                    (Some(_), Some(_)) => {
                        return Err(DataFusionError::Plan(
                            "JOIN residual column is ambiguous between its inputs".to_owned(),
                        ));
                    }
                    (None, None) => {
                        return Err(DataFusionError::Plan(
                            "JOIN residual column is outside both inputs".to_owned(),
                        ));
                    }
                };
                Ok(Transformed::yes(Expr::Column(Column::new(
                    Some(TableReference::bare(port)),
                    physical.field(index).name(),
                ))))
            }
            _ => Ok(Transformed::no(nested)),
        })
        .data()
        .map_err(Into::into)
}

fn rewrite_columns(
    expression: Expr,
    logical_schema: &DFSchema,
    physical_schema: &SchemaRef,
) -> Result<Expr, SqlError> {
    if logical_schema.fields().len() != physical_schema.fields().len() {
        return Err(SqlError::invalid(
            "logical and physical field counts diverged while lowering SQL",
        ));
    }
    expression
        .transform_up(|nested| match nested {
            Expr::Column(column) => {
                let index = logical_schema.index_of_column(&column)?;
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    physical_schema.field(index).name(),
                ))))
            }
            _ => Ok(Transformed::no(nested)),
        })
        .data()
        .map_err(Into::into)
}

fn scan_schema(definition: &OperationDefinition) -> Result<SchemaRef, SqlError> {
    definition
        .output_schema(&[])
        .map_err(SqlError::endpoint)?
        .ok_or_else(|| SqlError::invalid("scan definition has no output Schema"))
}

fn internal_join_field_name(index: usize) -> String {
    format!("__dogpaddle_sql_join_{index:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use dogpaddle_operation::operation::scan::{
        PostgresCdcScanDefinition, PostgresCdcScanSpec, PostgresColumn, PostgresType,
    };

    fn wide_definition() -> PostgresCdcScanDefinition {
        PostgresCdcScanDefinition::try_new(
            PostgresCdcScanSpec {
                engine_name: "rows".into(),
                database: "app".into(),
                schema: "public".into(),
                table: "rows".into(),
                slot: "rows_slot".into(),
                publication: "rows_pub".into(),
                system_identifier: "1".into(),
                database_oid: 2,
                table_oid: 3,
                columns: vec![
                    PostgresColumn::new("id", PostgresType::Int64, false),
                    PostgresColumn::new("payload", PostgresType::Text, true),
                    PostgresColumn::new("bucket", PostgresType::Int32, false),
                    PostgresColumn::new("unused", PostgresType::Bytea, true),
                ],
            },
            NonZeroU64::new(1024).unwrap(),
        )
        .unwrap()
    }

    fn wide_scan() -> OperationDefinition {
        wide_definition().into()
    }

    fn plan_sql(sql: &str, scans: &[OperationDefinition]) -> LogicalPlan {
        let (_, query, endpoints) = crate::syntax::parse(sql).unwrap();
        assert_eq!(endpoints.len(), scans.len());
        plan(query, scans).unwrap()
    }

    fn table_scans<'a>(
        plan: &'a LogicalPlan,
        output: &mut Vec<&'a datafusion_expr::logical_plan::TableScan>,
    ) {
        if let LogicalPlan::TableScan(scan) = plan {
            output.push(scan);
        }
        for input in plan.inputs() {
            table_scans(input, output);
        }
    }

    #[test]
    fn projection_optimization_prunes_join_and_aggregate_inputs() {
        let scans = vec![wide_scan(), wide_scan()];
        let plan = plan_sql(
            "INSERT INTO discard() \
             SELECT left_rows.bucket, COUNT(left_rows.payload) \
             FROM postgres_cdc( \
                 connection => 'postgresql://user:secret@127.0.0.1/app', \
                 table => 'public.left_rows', publication => 'left_rows_pub' \
             ) AS left_rows \
             JOIN postgres_cdc( \
                 connection => 'postgresql://user:secret@127.0.0.1/app', \
                 table => 'public.right_rows', publication => 'right_rows_pub' \
             ) AS right_rows \
             ON left_rows.id = right_rows.id \
             GROUP BY left_rows.bucket",
            &scans,
        );

        assert_eq!(
            scan_projections(&plan, scans.len()).unwrap(),
            [vec![0, 1, 2], vec![0]]
        );
    }

    #[test]
    fn repeated_scan_uses_one_union_projection_and_branch_subsets() {
        let scans = vec![wide_scan()];
        let plan = plan_sql(
            "INSERT INTO discard() \
             WITH rows AS ( \
                 SELECT id, payload, bucket, unused \
                 FROM postgres_cdc( \
                     connection => 'postgresql://user:secret@127.0.0.1/app', \
                     table => 'public.rows', publication => 'rows_pub' \
                 ) \
             ) \
             SELECT left_rows.payload \
             FROM rows AS left_rows \
             JOIN rows AS right_rows ON left_rows.id = right_rows.id",
            &scans,
        );
        let projections = scan_projections(&plan, scans.len()).unwrap();
        assert_eq!(projections, [vec![0, 1]]);

        let full = wide_definition();
        let projected: OperationDefinition = PostgresCdcScanDefinition::try_new_projected(
            full.spec().clone(),
            vec![0, 1],
            full.bootstrap_spool_bytes(),
        )
        .unwrap()
        .into();

        let mut occurrences = Vec::new();
        table_scans(&plan, &mut occurrences);
        assert_eq!(occurrences.len(), 2);
        let mut factory = FlowFactory::new("unused-column-pruning-test-state");
        let mut lowerer = Lowerer {
            factory: &mut factory,
            next_transform: 0,
            scans: vec![ScanBinding {
                definition: Some(projected),
                projection: projections[0].clone(),
                relation: None,
            }],
        };
        let mut schemas = occurrences
            .into_iter()
            .map(|scan| {
                lowerer
                    .lower_scan(scan)
                    .unwrap()
                    .physical_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        schemas.sort();

        assert_eq!(
            schemas,
            [
                vec!["id".to_owned()],
                vec!["id".to_owned(), "payload".to_owned()]
            ]
        );
        assert_eq!(
            lowerer.scans[0]
                .relation
                .as_ref()
                .unwrap()
                .physical_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "payload"]
        );
        assert_eq!(lowerer.next_transform, 1);
    }

    #[test]
    fn native_asof_comparisons_map_to_direction_and_exactness() {
        assert_eq!(
            asof_direction(Operator::Lt).unwrap(),
            AsOfDirection::Forward { allow_exact: false }
        );
        assert_eq!(
            asof_direction(Operator::LtEq).unwrap(),
            AsOfDirection::Forward { allow_exact: true }
        );
        assert_eq!(
            asof_direction(Operator::Gt).unwrap(),
            AsOfDirection::Backward { allow_exact: false }
        );
        assert_eq!(
            asof_direction(Operator::GtEq).unwrap(),
            AsOfDirection::Backward { allow_exact: true }
        );
        assert!(asof_direction(Operator::Eq).is_err());
    }
}
