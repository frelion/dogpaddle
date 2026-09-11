use std::num::NonZeroU64;

use dogpaddle_flow::{FlowFactory, StationRef};
use dogpaddle_operation::{
    InlineOperationDefinition, OperationDefinition,
    operation::transform::{
        AggregateDefinition, DistinctDefinition, FilterDefinition, SchemaAlignDefinition,
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

pub(crate) struct LogicalQuery {
    arena: LogicalArena,
    output: LogicalNodeId,
}

impl LogicalQuery {
    pub(crate) const fn new(arena: LogicalArena, output: LogicalNodeId) -> Self {
        Self { arena, output }
    }

    pub(crate) fn emit(
        mut self,
        factory: FlowFactory,
        sink: BuiltSink,
    ) -> Result<FlowFactory, SqlError> {
        self.arena.push([self.output], LogicalOperator::Sink(sink));
        let core_plans = plan_partition(&self.arena.nodes);
        emit_partition(self.arena, core_plans, factory)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LogicalNodeId(usize);

#[derive(Default)]
pub(crate) struct LogicalArena {
    // Lowering appends nodes in deterministic postorder, so every input names
    // an earlier node. Reused Scan identities represent fan-out directly.
    nodes: Vec<LogicalNode>,
}

impl LogicalArena {
    pub(crate) fn push(
        &mut self,
        inputs: impl IntoIterator<Item = LogicalNodeId>,
        operator: LogicalOperator,
    ) -> LogicalNodeId {
        let id = LogicalNodeId(self.nodes.len());
        let inputs = inputs.into_iter().collect::<Vec<_>>();
        assert_eq!(
            inputs.len(),
            operator.input_count(),
            "logical node input count must match its Operation"
        );
        assert!(
            inputs.iter().all(|input| input.0 < id.0),
            "logical node inputs must precede their consumer"
        );
        self.nodes.push(LogicalNode { inputs, operator });
        id
    }
}

struct LogicalNode {
    inputs: Vec<LogicalNodeId>,
    operator: LogicalOperator,
}

pub(crate) enum LogicalOperator {
    Scan {
        source_index: usize,
        definition: BuiltScan,
    },
    Transform(TransformDefinition),
    Sink(BuiltSink),
}

pub(crate) enum TransformDefinition {
    Aggregate(AggregateDefinition),
    Distinct(DistinctDefinition),
    Filter(FilterDefinition),
    SchemaAlign(SchemaAlignDefinition),
    UnionAll(UnionAllDefinition),
}

impl From<AggregateDefinition> for TransformDefinition {
    fn from(definition: AggregateDefinition) -> Self {
        Self::Aggregate(definition)
    }
}

impl From<DistinctDefinition> for TransformDefinition {
    fn from(definition: DistinctDefinition) -> Self {
        Self::Distinct(definition)
    }
}

impl From<FilterDefinition> for TransformDefinition {
    fn from(definition: FilterDefinition) -> Self {
        Self::Filter(definition)
    }
}

impl From<SchemaAlignDefinition> for TransformDefinition {
    fn from(definition: SchemaAlignDefinition) -> Self {
        Self::SchemaAlign(definition)
    }
}

impl From<UnionAllDefinition> for TransformDefinition {
    fn from(definition: UnionAllDefinition) -> Self {
        Self::UnionAll(definition)
    }
}

impl LogicalOperator {
    fn input_count(&self) -> usize {
        match self {
            Self::Scan { .. } => 0,
            Self::Transform(definition) => definition.input_count(),
            Self::Sink(_) => 1,
        }
    }

    fn is_inline_candidate(&self) -> bool {
        matches!(self, Self::Transform(definition) if definition.is_inline_candidate())
    }
}

impl TransformDefinition {
    fn input_count(&self) -> usize {
        let definition: &dyn OperationDefinition = match self {
            Self::Aggregate(definition) => definition,
            Self::Distinct(definition) => definition,
            Self::Filter(definition) => definition,
            Self::SchemaAlign(definition) => definition,
            Self::UnionAll(definition) => definition,
        };
        usize::try_from(definition.kind().input_count())
            .expect("an Operation input count fits usize")
    }

    fn is_inline_candidate(&self) -> bool {
        match self {
            Self::Filter(definition) => definition.clone().try_into_inline().is_ok(),
            Self::SchemaAlign(definition) => definition.clone().try_into_inline().is_ok(),
            Self::Aggregate(_) | Self::Distinct(_) | Self::UnionAll(_) => false,
        }
    }

    fn emit_inline_input(
        self,
        factory: &mut FlowFactory,
        station: StationRef,
        port: usize,
    ) -> Result<(), SqlError> {
        match self {
            Self::Filter(definition) => factory.inline_input(station, port, definition),
            Self::SchemaAlign(definition) => factory.inline_input(station, port, definition),
            Self::Aggregate(_) | Self::Distinct(_) | Self::UnionAll(_) => {
                unreachable!("only an inline-capable transform can be assigned to a pipeline")
            }
        }
        .map(|_| ())
        .map_err(SqlError::endpoint)
    }

    fn emit_inline_output(
        self,
        factory: &mut FlowFactory,
        station: StationRef,
    ) -> Result<(), SqlError> {
        match self {
            Self::Filter(definition) => factory.inline_output(station, definition),
            Self::SchemaAlign(definition) => factory.inline_output(station, definition),
            Self::Aggregate(_) | Self::Distinct(_) | Self::UnionAll(_) => {
                unreachable!("only an inline-capable transform can be assigned to a pipeline")
            }
        }
        .map(|_| ())
        .map_err(SqlError::endpoint)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct InputRoute {
    producer: LogicalNodeId,
    stages: Vec<LogicalNodeId>,
}

#[derive(Debug, Eq, PartialEq)]
struct CorePlan {
    inputs: Vec<InputRoute>,
    output: Vec<LogicalNodeId>,
}

fn plan_partition(nodes: &[LogicalNode]) -> Vec<Option<CorePlan>> {
    let consumers = consumers(nodes);
    let inline = nodes
        .iter()
        .map(|node| node.operator.is_inline_candidate())
        .collect::<Vec<_>>();
    // Scans, sinks, stateful or multi-input transforms, and ineligible unary
    // transforms establish durable cores. Each core first absorbs its maximal
    // one-consumer pure chain into its output. A remaining pure fan-out node
    // becomes a durable adapter core; every other pure chain belongs to one
    // downstream input port.
    let mut is_core = inline
        .iter()
        .map(|candidate| !candidate)
        .collect::<Vec<_>>();
    let mut output_owner = vec![None; nodes.len()];
    let mut output_stages = std::iter::repeat_with(Vec::new)
        .take(nodes.len())
        .collect::<Vec<_>>();
    plan_output_pipelines(
        nodes,
        &consumers,
        &inline,
        &mut is_core,
        &mut output_owner,
        &mut output_stages,
    );
    plan_core_pipelines(nodes, &is_core, &output_owner, output_stages)
}

fn plan_core_pipelines(
    nodes: &[LogicalNode],
    is_core: &[bool],
    output_owner: &[Option<LogicalNodeId>],
    mut output_stages: Vec<Vec<LogicalNodeId>>,
) -> Vec<Option<CorePlan>> {
    let mut core_plans = std::iter::repeat_with(|| None)
        .take(nodes.len())
        .collect::<Vec<_>>();
    for (index, node) in nodes.iter().enumerate() {
        if !is_core[index] {
            continue;
        }
        let inputs = node
            .inputs
            .iter()
            .map(|input| trace_input(nodes, is_core, output_owner, *input))
            .collect();
        core_plans[index] = Some(CorePlan {
            inputs,
            output: std::mem::take(&mut output_stages[index]),
        });
    }
    core_plans
}

fn consumers(nodes: &[LogicalNode]) -> Vec<Vec<LogicalNodeId>> {
    let mut consumers = std::iter::repeat_with(Vec::new)
        .take(nodes.len())
        .collect::<Vec<_>>();
    for (consumer, node) in nodes.iter().enumerate() {
        for input in &node.inputs {
            consumers[input.0].push(LogicalNodeId(consumer));
        }
    }
    consumers
}

fn plan_output_pipelines(
    nodes: &[LogicalNode],
    consumers: &[Vec<LogicalNodeId>],
    inline: &[bool],
    is_core: &mut [bool],
    output_owner: &mut [Option<LogicalNodeId>],
    output_stages: &mut [Vec<LogicalNodeId>],
) {
    for node in 0..nodes.len() {
        if !inline[node] {
            continue;
        }
        let input = nodes[node].inputs[0];
        let owner = if consumers[input.0].len() == 1 {
            if is_core[input.0] {
                Some(input)
            } else {
                output_owner[input.0]
            }
        } else {
            None
        };
        if let Some(owner) = owner {
            output_owner[node] = Some(owner);
            output_stages[owner.0].push(LogicalNodeId(node));
        } else if consumers[node].len() > 1 {
            is_core[node] = true;
        }
    }
}

fn trace_input(
    nodes: &[LogicalNode],
    is_core: &[bool],
    output_owner: &[Option<LogicalNodeId>],
    start: LogicalNodeId,
) -> InputRoute {
    let mut current = start;
    let mut stages = Vec::new();
    let producer = loop {
        if is_core[current.0] {
            break current;
        }
        if let Some(owner) = output_owner[current.0] {
            break owner;
        }
        stages.push(current);
        let [input] = nodes[current.0].inputs.as_slice() else {
            unreachable!("an inline logical node has exactly one input")
        };
        current = *input;
    };
    stages.reverse();
    InputRoute { producer, stages }
}

fn emit_partition(
    arena: LogicalArena,
    mut core_plans: Vec<Option<CorePlan>>,
    mut factory: FlowFactory,
) -> Result<FlowFactory, SqlError> {
    // Every definition remains in its logical node until this final pass takes
    // it exactly once as either a core or an inline stage.
    let mut nodes = arena.nodes.into_iter().map(Some).collect::<Vec<_>>();
    let mut references = vec![None; nodes.len()];
    let mut next_transform = 0;
    for index in 0..nodes.len() {
        let Some(core_plan) = core_plans[index].take() else {
            continue;
        };
        let node = nodes[index]
            .take()
            .expect("a logical node can belong to only one Station");
        let inputs = core_plan
            .inputs
            .iter()
            .map(|input| {
                references[input.producer.0].expect("a physical producer precedes its consumer")
            })
            .collect::<Vec<_>>();
        let (id, has_output) = match &node.operator {
            LogicalOperator::Scan { source_index, .. } => (scan_station_id(*source_index), true),
            LogicalOperator::Transform(_) => {
                let id = transform_station_id(next_transform);
                next_transform += 1;
                (id, true)
            }
            LogicalOperator::Sink(_) => ("sql/sink".to_owned(), false),
        };
        let reference = node.operator.emit(&mut factory, &id)?;
        for (port, input) in core_plan.inputs.into_iter().enumerate() {
            for stage in input.stages {
                take_inline_transform(&mut nodes, stage).emit_inline_input(
                    &mut factory,
                    reference,
                    port,
                )?;
            }
        }
        for stage in core_plan.output {
            take_inline_transform(&mut nodes, stage).emit_inline_output(&mut factory, reference)?;
        }
        if has_output {
            factory.output_capacity_bytes(reference, OUTPUT_CAPACITY);
        }
        if !inputs.is_empty() {
            factory.connect(inputs, reference);
        }
        references[index] = Some(reference);
    }
    assert!(
        nodes.into_iter().all(|node| node.is_none()),
        "every logical node must belong to exactly one Station"
    );
    Ok(factory)
}

fn take_inline_transform(
    nodes: &mut [Option<LogicalNode>],
    stage: LogicalNodeId,
) -> TransformDefinition {
    let node = nodes[stage.0]
        .take()
        .expect("an inline stage can belong to only one pipeline");
    let LogicalOperator::Transform(definition) = node.operator else {
        unreachable!("only a transform can be assigned to an inline pipeline")
    };
    definition
}

impl LogicalOperator {
    fn emit(self, factory: &mut FlowFactory, id: &str) -> Result<StationRef, SqlError> {
        match self {
            Self::Scan { definition, .. } => match definition {
                BuiltScan::Sequence(definition) => Ok(factory.station(id, definition)),
                BuiltScan::PostgresCdc(scan) => {
                    factory.resource(id, scan.config)?;
                    Ok(factory.station(id, scan.definition))
                }
                BuiltScan::MySqlCdc(scan) => {
                    factory.resource(id, scan.config)?;
                    Ok(factory.station(id, scan.definition))
                }
            },
            Self::Transform(definition) => Ok(definition.emit(factory, id)),
            Self::Sink(sink) => match sink {
                BuiltSink::Postgres { definition, config } => {
                    factory.resource(id, config)?;
                    Ok(factory.station(id, definition))
                }
                BuiltSink::Sqlite(definition) => Ok(factory.station(id, definition)),
                BuiltSink::Discard(definition) => Ok(factory.station(id, definition)),
            },
        }
    }
}

impl TransformDefinition {
    fn emit(self, factory: &mut FlowFactory, id: &str) -> StationRef {
        match self {
            Self::Aggregate(definition) => factory.station(id, definition),
            Self::Distinct(definition) => factory.station(id, definition),
            Self::Filter(definition) => factory.station(id, definition),
            Self::SchemaAlign(definition) => factory.station(id, definition),
            Self::UnionAll(definition) => factory.station(id, definition),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use datafusion_expr::{lit, placeholder};
    use datafusion_sql::sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
    use dogpaddle_operation::operation::{
        scan::SequenceScanDefinition,
        sink::DiscardDefinition,
        transform::{DistinctDefinition, UnionAllDefinition},
    };

    use super::*;

    #[test]
    fn lowering_reuses_one_scan_identity_across_cte_references() {
        let mut statements = Parser::parse_sql(
            &GenericDialect {},
            "WITH numbers AS (SELECT value FROM __dogpaddle_sql_scan_00000000) \
             SELECT value FROM numbers WHERE value % 2 = 0 \
             UNION ALL \
             SELECT value FROM numbers WHERE value % 3 = 0",
        )
        .unwrap();
        let Statement::Query(query) = statements.remove(0) else {
            panic!("test SQL must parse as a query");
        };
        let scans = vec![BuiltScan::Sequence(SequenceScanDefinition::new(7))];
        let plan = crate::lower::plan(*query, &scans).unwrap();
        let query = crate::lower::lower_query(&plan, scans).unwrap();
        let scan_nodes = query
            .arena
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| {
                matches!(&node.operator, LogicalOperator::Scan { .. })
                    .then_some(LogicalNodeId(index))
            })
            .collect::<Vec<_>>();

        assert_eq!(scan_nodes.len(), 1);
        assert_eq!(
            query
                .arena
                .nodes
                .iter()
                .flat_map(|node| &node.inputs)
                .filter(|input| **input == scan_nodes[0])
                .count(),
            2
        );
    }

    #[test]
    fn partition_preserves_postorder_and_shared_core_inputs() {
        let mut arena = LogicalArena::default();
        let scan = arena.push(
            [],
            LogicalOperator::Scan {
                source_index: 3,
                definition: BuiltScan::Sequence(SequenceScanDefinition::new(7)),
            },
        );
        let left = arena.push(
            [scan],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let right = arena.push(
            [scan],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let union = arena.push(
            [left, right],
            LogicalOperator::Transform(TransformDefinition::UnionAll(UnionAllDefinition::new(
                NonZeroU32::new(2).unwrap(),
            ))),
        );
        let plans = plan_with_discard(&mut arena, union);

        assert_eq!(core_indices(&plans), [0, 1, 2, 3, 4]);
        assert_eq!(core(&plans, left).inputs, [route(scan, [])]);
        assert_eq!(core(&plans, right).inputs, [route(scan, [])]);
        assert_eq!(
            core(&plans, union).inputs,
            [route(left, []), route(right, [])]
        );
        assert_eq!(core(&plans, LogicalNodeId(4)).inputs, [route(union, [])]);
    }

    #[test]
    fn partition_fuses_linear_output_and_fanout_input_pipelines() {
        let mut linear = LogicalArena::default();
        let scan = sequence(&mut linear);
        let first = filter(&mut linear, scan);
        let second = filter(&mut linear, first);
        let plans = plan_with_discard(&mut linear, second);

        assert_eq!(core_indices(&plans), [0, 3]);
        assert_eq!(core(&plans, scan).output, [first, second]);
        assert_eq!(core(&plans, LogicalNodeId(3)).inputs, [route(scan, [])]);

        let mut fanout = LogicalArena::default();
        let scan = sequence(&mut fanout);
        let left = filter(&mut fanout, scan);
        let right = filter(&mut fanout, scan);
        let union = fanout.push(
            [left, right],
            LogicalOperator::Transform(TransformDefinition::UnionAll(UnionAllDefinition::new(
                NonZeroU32::new(2).unwrap(),
            ))),
        );
        let plans = plan_with_discard(&mut fanout, union);

        assert_eq!(core_indices(&plans), [0, 3, 4]);
        assert!(core(&plans, scan).output.is_empty());
        assert_eq!(
            core(&plans, union).inputs,
            [route(scan, [left]), route(scan, [right])]
        );
        assert_eq!(core(&plans, LogicalNodeId(4)).inputs, [route(union, [])]);
    }

    #[test]
    fn partition_promotes_an_unowned_shared_transform_to_an_adapter_core() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
        let shared = filter(&mut arena, scan);
        let left = arena.push(
            [shared],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let right = arena.push(
            [shared],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let union = arena.push(
            [left, right, scan],
            LogicalOperator::Transform(TransformDefinition::UnionAll(UnionAllDefinition::new(
                NonZeroU32::new(3).unwrap(),
            ))),
        );
        let plans = plan_with_discard(&mut arena, union);

        assert_eq!(core_indices(&plans), [0, 1, 2, 3, 4, 5]);
        assert_eq!(core(&plans, shared).inputs, [route(scan, [])]);
        assert_eq!(core(&plans, left).inputs, [route(shared, [])]);
        assert_eq!(core(&plans, right).inputs, [route(shared, [])]);
        assert_eq!(
            core(&plans, union).inputs,
            [route(left, []), route(right, []), route(scan, [])]
        );
    }

    #[test]
    fn partition_keeps_a_shared_pure_tail_on_its_producer_output() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
        let shared = filter(&mut arena, scan);
        let left = arena.push(
            [shared],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let right = arena.push(
            [shared],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let union = arena.push(
            [left, right],
            LogicalOperator::Transform(TransformDefinition::UnionAll(UnionAllDefinition::new(
                NonZeroU32::new(2).unwrap(),
            ))),
        );
        let plans = plan_with_discard(&mut arena, union);

        assert_eq!(core_indices(&plans), [0, 2, 3, 4, 5]);
        assert_eq!(core(&plans, scan).output, [shared]);
        assert_eq!(core(&plans, left).inputs, [route(scan, [])]);
        assert_eq!(core(&plans, right).inputs, [route(scan, [])]);
    }

    #[test]
    fn partition_keeps_an_ineligible_pure_definition_as_a_core() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
        let parameter = arena.push(
            [scan],
            LogicalOperator::Transform(TransformDefinition::Filter(
                FilterDefinition::try_new(placeholder("$1")).unwrap(),
            )),
        );
        let plans = plan_with_discard(&mut arena, parameter);

        assert_eq!(core_indices(&plans), [0, 1, 2]);
        assert!(core(&plans, scan).output.is_empty());
        assert_eq!(core(&plans, parameter).inputs, [route(scan, [])]);
    }

    fn plan_with_discard(arena: &mut LogicalArena, output: LogicalNodeId) -> Vec<Option<CorePlan>> {
        arena.push(
            [output],
            LogicalOperator::Sink(BuiltSink::Discard(DiscardDefinition::new())),
        );
        plan_partition(&arena.nodes)
    }

    fn core(plans: &[Option<CorePlan>], node: LogicalNodeId) -> &CorePlan {
        plans[node.0].as_ref().unwrap()
    }

    fn core_indices(plans: &[Option<CorePlan>]) -> Vec<usize> {
        plans
            .iter()
            .enumerate()
            .filter_map(|(index, plan)| plan.as_ref().map(|_| index))
            .collect()
    }

    fn route(
        producer: LogicalNodeId,
        stages: impl IntoIterator<Item = LogicalNodeId>,
    ) -> InputRoute {
        InputRoute {
            producer,
            stages: stages.into_iter().collect(),
        }
    }

    fn sequence(arena: &mut LogicalArena) -> LogicalNodeId {
        arena.push(
            [],
            LogicalOperator::Scan {
                source_index: 0,
                definition: BuiltScan::Sequence(SequenceScanDefinition::new(7)),
            },
        )
    }

    fn filter(arena: &mut LogicalArena, input: LogicalNodeId) -> LogicalNodeId {
        arena.push(
            [input],
            LogicalOperator::Transform(TransformDefinition::Filter(
                FilterDefinition::try_new(lit(true)).unwrap(),
            )),
        )
    }
}
