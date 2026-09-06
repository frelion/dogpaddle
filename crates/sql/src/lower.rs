use std::{collections::HashMap, num::NonZeroU32, num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion_common::{Column, DFSchema, DataFusionError, TableReference, config::ConfigOptions};
use datafusion_expr::{
    AggregateUDF, Distinct as LogicalDistinct, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF,
    TableSource, WindowUDF, expr_rewriter::unnormalize_col,
};
use datafusion_optimizer::{Analyzer, analyzer::type_coercion::TypeCoercion};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use dogpaddle_flow::{FlowFactory, StationRef};
use dogpaddle_operation::{
    OperationDefinition,
    operation::transform::{
        DistinctDefinition, FilterDefinition, SchemaAlignDefinition, SchemaAlignField,
        UnionAllDefinition,
    },
};

use crate::{
    SqlError,
    endpoint::{BuiltScan, BuiltSink},
};

const OUTPUT_CAPACITY: NonZeroU64 = NonZeroU64::new(64 * 1024 * 1024).expect("64 MiB is nonzero");

pub(crate) fn scan_station_id(index: usize) -> String {
    format!("sql/scan/{index:08x}")
}

fn transform_station_id(index: usize) -> String {
    format!("sql/transform/{index:08x}")
}

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
        Ok(Self { options, sources })
    }
}

impl ContextProvider for PlanningContext {
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

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
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
        Vec::new()
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
    mut factory: FlowFactory,
    plan: &LogicalPlan,
    scans: Vec<BuiltScan>,
) -> Result<(FlowFactory, StationRef), SqlError> {
    let mut lowerer = Lowerer {
        factory: &mut factory,
        scans: scans.into_iter().map(Some).collect(),
        scan_stations: HashMap::new(),
        next_transform: 0,
    };
    let input = lowerer.lower(plan)?;
    if lowerer.scans.iter().any(Option::is_some) {
        return Err(SqlError::invalid(
            "every declared scan must be reachable from the query result",
        ));
    }
    Ok((factory, input))
}

struct Lowerer<'a> {
    factory: &'a mut FlowFactory,
    scans: Vec<Option<BuiltScan>>,
    scan_stations: HashMap<usize, StationRef>,
    next_transform: usize,
}

impl Lowerer<'_> {
    fn lower(&mut self, plan: &LogicalPlan) -> Result<StationRef, SqlError> {
        match plan {
            LogicalPlan::TableScan(scan) => self.lower_scan(scan),
            LogicalPlan::Filter(filter) => {
                let input = self.lower(&filter.input)?;
                let definition =
                    FilterDefinition::try_new(unnormalize_col(filter.predicate.clone()))
                        .map_err(SqlError::endpoint)?;
                Ok(self.add_transform(input, definition))
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
                Ok(self.add_transform(input, definition))
            }
            LogicalPlan::Distinct(LogicalDistinct::All(input)) => {
                let input = self.lower(input)?;
                Ok(self.add_transform(input, DistinctDefinition::new()))
            }
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
                let station = self.new_transform(definition);
                self.factory.connect(inputs, station);
                Ok(station)
            }
            LogicalPlan::SubqueryAlias(alias) => self.lower(&alias.input),
            _ => Err(SqlError::Unsupported(format!(
                "relational plan node {}",
                plan.display()
            ))),
        }
    }

    fn lower_scan(
        &mut self,
        scan: &datafusion_expr::logical_plan::TableScan,
    ) -> Result<StationRef, SqlError> {
        if scan.projection.is_some() || !scan.filters.is_empty() || scan.fetch.is_some() {
            return Err(SqlError::invalid(
                "DataFusion embedded projection, filter, or fetch in a scan",
            ));
        }
        let source = scan
            .source
            .downcast_ref::<ScanSource>()
            .ok_or_else(|| SqlError::invalid("logical plan contains a foreign table source"))?;
        if let Some(station) = self.scan_stations.get(&source.index) {
            return Ok(*station);
        }
        let built = self
            .scans
            .get_mut(source.index)
            .and_then(Option::take)
            .ok_or_else(|| SqlError::invalid("logical plan references an unknown scan"))?;
        let id = scan_station_id(source.index);
        let station = match built {
            BuiltScan::Sequence(definition) => self.factory.station(&id, definition),
            BuiltScan::PostgresCdc(scan) => {
                self.factory.resource(&id, scan.config)?;
                self.factory.station(&id, scan.definition)
            }
        };
        self.factory.output_capacity_bytes(station, OUTPUT_CAPACITY);
        self.scan_stations.insert(source.index, station);
        Ok(station)
    }

    fn align_union_input(
        &mut self,
        input: StationRef,
        input_schema: &DFSchema,
        union_schema: &DFSchema,
    ) -> Result<StationRef, SqlError> {
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
        Ok(self.add_transform(input, definition))
    }

    fn add_transform<D>(&mut self, input: StationRef, definition: D) -> StationRef
    where
        D: OperationDefinition,
    {
        let station = self.new_transform(definition);
        self.factory.connect([input], station);
        station
    }

    fn new_transform<D>(&mut self, definition: D) -> StationRef
    where
        D: OperationDefinition,
    {
        let id = transform_station_id(self.next_transform);
        self.next_transform += 1;
        let station = self.factory.station(id, definition);
        self.factory.output_capacity_bytes(station, OUTPUT_CAPACITY);
        station
    }
}

pub(crate) fn add_sink(
    factory: &mut FlowFactory,
    input: StationRef,
    sink: BuiltSink,
) -> Result<(), SqlError> {
    let station = match sink {
        BuiltSink::Postgres { definition, config } => {
            factory.resource("sql/sink", config)?;
            factory.station("sql/sink", definition)
        }
        BuiltSink::Sqlite(definition) => factory.station("sql/sink", definition),
        BuiltSink::Discard(definition) => factory.station("sql/sink", definition),
    };
    factory.connect([input], station);
    Ok(())
}

fn scan_schema(scan: &BuiltScan) -> Result<SchemaRef, SqlError> {
    let definition: &dyn OperationDefinition = match scan {
        BuiltScan::Sequence(definition) => definition,
        BuiltScan::PostgresCdc(scan) => &scan.definition,
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
