use std::num::NonZeroU64;

use dogpaddle_flow::{FlowError, FlowFactory, StationRef};
use dogpaddle_operation::{
    OperationDefinition, OperationKind,
    operation::transform::{
        AggregateDefinition, DistinctDefinition, FilterDefinition, InnerEquiJoinDefinition,
        SchemaAlignDefinition, UnionAllDefinition,
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
        emit_arena(self.arena, factory)
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
    InnerEquiJoin(InnerEquiJoinDefinition),
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

impl From<InnerEquiJoinDefinition> for TransformDefinition {
    fn from(definition: InnerEquiJoinDefinition) -> Self {
        Self::InnerEquiJoin(definition)
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

    fn allows_append(&self) -> bool {
        match self {
            Self::Scan { .. } => true,
            Self::Transform(definition) => definition.kind().allows_atomic_tail(),
            Self::Sink(_) => false,
        }
    }

    fn has_output(&self) -> bool {
        !matches!(self, Self::Sink(_))
    }
}

impl TransformDefinition {
    fn definition(&self) -> &dyn OperationDefinition {
        match self {
            Self::Aggregate(definition) => definition,
            Self::Distinct(definition) => definition,
            Self::Filter(definition) => definition,
            Self::InnerEquiJoin(definition) => definition,
            Self::SchemaAlign(definition) => definition,
            Self::UnionAll(definition) => definition,
        }
    }

    fn kind(&self) -> OperationKind {
        self.definition().kind()
    }

    fn input_count(&self) -> usize {
        usize::try_from(self.kind().input_count()).expect("an Operation input count fits usize")
    }

    fn is_append_candidate(&self) -> bool {
        self.kind().is_atomic() && self.input_count() == 1
    }

    fn append(self, factory: &mut FlowFactory, station: StationRef) -> Result<(), SqlError> {
        match self {
            Self::Aggregate(definition) => factory.append(station, definition),
            Self::Distinct(definition) => factory.append(station, definition),
            Self::Filter(definition) => factory.append(station, definition),
            Self::InnerEquiJoin(definition) => factory.append(station, definition),
            Self::SchemaAlign(definition) => factory.append(station, definition),
            Self::UnionAll(definition) => factory.append(station, definition),
        }
        .map(|_| ())
        .map_err(|error| SqlError::Flow(FlowError::from(error)))
    }
}

fn consumer_counts(nodes: &[LogicalNode]) -> Vec<usize> {
    let mut counts = vec![0; nodes.len()];
    for node in nodes {
        for input in &node.inputs {
            counts[input.0] += 1;
        }
    }
    counts
}

fn emit_arena(arena: LogicalArena, mut factory: FlowFactory) -> Result<FlowFactory, SqlError> {
    let consumer_counts = consumer_counts(&arena.nodes);
    let node_count = arena.nodes.len();
    let mut references = vec![None; node_count];
    let mut station_tail = vec![false; node_count];
    let mut station_allows_append = vec![false; node_count];
    let mut next_transform = 0;
    for (index, node) in arena.nodes.into_iter().enumerate() {
        let append_input = match &node.operator {
            LogicalOperator::Transform(definition) if definition.is_append_candidate() => {
                let [input] = node.inputs.as_slice() else {
                    unreachable!("an append candidate has exactly one input")
                };
                (consumer_counts[input.0] == 1
                    && station_tail[input.0]
                    && station_allows_append[input.0])
                    .then_some(*input)
            }
            _ => None,
        };
        if let Some(input) = append_input {
            let reference =
                references[input.0].expect("an appended Operation's Station precedes it");
            let LogicalOperator::Transform(definition) = node.operator else {
                unreachable!("only a Transform can be appended")
            };
            definition.append(&mut factory, reference)?;
            station_tail[input.0] = false;
            station_tail[index] = true;
            station_allows_append[index] = true;
            references[index] = Some(reference);
            continue;
        }

        let inputs = node
            .inputs
            .iter()
            .map(|input| references[input.0].expect("a producer Station precedes its consumer"))
            .collect::<Vec<_>>();
        let id = match &node.operator {
            LogicalOperator::Scan { source_index, .. } => scan_station_id(*source_index),
            LogicalOperator::Transform(_) => {
                let id = transform_station_id(next_transform);
                next_transform += 1;
                id
            }
            LogicalOperator::Sink(_) => "sql/sink".to_owned(),
        };
        let allows_append = node.operator.allows_append();
        let has_output = node.operator.has_output();
        let reference = node.operator.emit(&mut factory, &id)?;
        if has_output {
            factory.output_capacity_bytes(reference, OUTPUT_CAPACITY);
        }
        if !inputs.is_empty() {
            factory.connect(inputs, reference);
        }
        station_tail[index] = true;
        station_allows_append[index] = allows_append;
        references[index] = Some(reference);
    }
    Ok(factory)
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
            Self::InnerEquiJoin(definition) => factory.station(id, definition),
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
    fn assembly_keeps_fanout_branches_and_multi_input_heads_durable() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
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
        let tail = filter(&mut arena, union);

        assert_eq!(
            station_ids(arena, tail),
            [
                "sql/scan/00000000",
                "sql/transform/00000000",
                "sql/transform/00000001",
                "sql/transform/00000002",
                "sql/sink",
            ]
        );
    }

    #[test]
    fn assembly_fuses_the_maximal_linear_atomic_chain() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
        let first = filter(&mut arena, scan);
        let distinct = arena.push(
            [first],
            LogicalOperator::Transform(TransformDefinition::Distinct(DistinctDefinition::new())),
        );
        let second = filter(&mut arena, distinct);

        assert_eq!(
            station_ids(arena, second),
            ["sql/scan/00000000", "sql/sink"]
        );
    }

    #[test]
    fn assembly_keeps_a_shared_atomic_tail_on_its_producer_station() {
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

        assert_eq!(
            station_ids(arena, union),
            [
                "sql/scan/00000000",
                "sql/transform/00000000",
                "sql/transform/00000001",
                "sql/transform/00000002",
                "sql/sink",
            ]
        );
    }

    #[test]
    fn consumer_count_counts_repeated_input_edges() {
        let mut arena = LogicalArena::default();
        let scan = sequence(&mut arena);
        arena.push(
            [scan, scan],
            LogicalOperator::Transform(TransformDefinition::UnionAll(UnionAllDefinition::new(
                NonZeroU32::new(2).unwrap(),
            ))),
        );

        assert_eq!(consumer_counts(&arena.nodes), [2, 0]);
    }

    #[test]
    fn ineligible_expression_is_an_exclusive_transform() {
        let definition =
            TransformDefinition::Filter(FilterDefinition::try_new(placeholder("$1")).unwrap());

        assert_eq!(
            definition.kind(),
            OperationKind::ExclusiveTransform(NonZeroU32::new(1).unwrap())
        );
        assert!(!definition.is_append_candidate());
    }

    fn station_ids(mut arena: LogicalArena, output: LogicalNodeId) -> Vec<String> {
        arena.push(
            [output],
            LogicalOperator::Sink(BuiltSink::Discard(DiscardDefinition::new())),
        );
        let root = tempfile::tempdir().unwrap();
        let factory = emit_arena(arena, FlowFactory::new(root.path().join("flow"))).unwrap();
        factory
            .build()
            .unwrap()
            .status()
            .unwrap()
            .iter()
            .map(|station| station.id.clone())
            .collect()
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
