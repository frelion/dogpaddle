use std::{
    collections::{HashSet, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_operation::OperationDefinition;
use thiserror::Error;

static NEXT_BUILDER_TOKEN: AtomicU64 = AtomicU64::new(1);

pub(crate) trait InputCount {
    fn input_count(&self) -> usize;
}

impl InputCount for OperationDefinition {
    fn input_count(&self) -> usize {
        OperationDefinition::input_count(self)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StageId(String);

/// Temporary reference to a stage declared in one [`FlowBuilder`](crate::FlowBuilder).
///
/// A reference is valid only while assembling the builder that created it. The
/// durable Flow definition stores stable stage IDs instead.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StageRef {
    builder_token: u64,
    index: usize,
}

#[derive(Debug)]
pub(crate) struct StageDefinition<D> {
    id: StageId,
    operation: D,
    sources: Vec<StageId>,
}

#[derive(Debug)]
pub(crate) struct Topology<D> {
    stages: Vec<StageDefinition<D>>,
}

#[derive(Debug)]
struct PendingStage<D> {
    id: String,
    operation: D,
}

#[derive(Debug)]
struct PendingConnection {
    sources: Vec<StageRef>,
    target: StageRef,
}

#[derive(Debug)]
pub(crate) struct TopologyBuilder<D> {
    token: u64,
    stages: Vec<PendingStage<D>>,
    connections: Vec<PendingConnection>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvalidStageIdReason {
    /// The stage ID is empty.
    Empty,
    /// The stage ID contains a NUL character.
    ContainsNul,
}

/// Failure while validating a Flow's static topology.
#[derive(Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TopologyError {
    /// A Flow must contain at least one stage.
    #[error("a flow must contain at least one stage")]
    EmptyTopology,
    /// A stage ID violates the stable identity rules.
    #[error("invalid stage ID {id:?}: {reason:?}")]
    InvalidStageId {
        /// The rejected ID.
        id: String,
        /// Why the ID was rejected.
        reason: InvalidStageIdReason,
    },
    /// Two stages declared the same stable ID.
    #[error("duplicate stage ID {0:?}")]
    DuplicateStageId(String),
    /// A connection used a reference created by another builder.
    #[error("stage reference belongs to another flow builder")]
    ForeignStageRef(StageRef),
    /// `connect` was called without any sources.
    #[error("stage {0:?} was connected with an empty source list")]
    EmptySources(String),
    /// A target's complete source list was declared more than once.
    #[error("sources for stage {0:?} were already set")]
    SourcesAlreadySet(String),
    /// A stage directly references itself.
    #[error("stage {0:?} directly references itself")]
    SelfLoop(String),
    /// The number of connected sources does not match the operation definition.
    #[error("stage {stage:?} requires {expected} sources but received {actual}")]
    InputCount {
        /// The stage whose arity did not match.
        stage: String,
        /// Required source count.
        expected: usize,
        /// Connected source count.
        actual: usize,
    },
    /// The topology contains an indirect cycle.
    #[error("flow topology contains a cycle")]
    Cycle,
}

impl StageId {
    fn new_validated(id: String) -> Self {
        Self(id)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<D> StageDefinition<D> {
    pub(crate) fn id(&self) -> &str {
        self.id.as_str()
    }

    pub(crate) const fn operation(&self) -> &D {
        &self.operation
    }

    pub(crate) fn sources(&self) -> impl ExactSizeIterator<Item = &str> {
        self.sources.iter().map(StageId::as_str)
    }
}

impl<D> Topology<D> {
    pub(crate) fn stages(&self) -> &[StageDefinition<D>] {
        &self.stages
    }
}

impl<D> TopologyBuilder<D> {
    pub(crate) fn new() -> Self {
        let token = NEXT_BUILDER_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "topology builder token space exhausted");
        Self {
            token,
            stages: Vec::new(),
            connections: Vec::new(),
        }
    }

    pub(crate) fn stage(&mut self, id: impl Into<String>, operation: D) -> StageRef {
        let reference = StageRef {
            builder_token: self.token,
            index: self.stages.len(),
        };
        self.stages.push(PendingStage {
            id: id.into(),
            operation,
        });
        reference
    }

    pub(crate) fn connect<I>(&mut self, sources: I, target: StageRef) -> &mut Self
    where
        I: IntoIterator<Item = StageRef>,
    {
        self.connections.push(PendingConnection {
            sources: sources.into_iter().collect(),
            target,
        });
        self
    }

    pub(crate) fn validate_stage_ids(&self) -> Result<(), TopologyError> {
        validate_stage_ids(&self.stages)
    }
}

impl<D: InputCount> TopologyBuilder<D> {
    pub(crate) fn finish(self) -> Result<Topology<D>, TopologyError> {
        let Self {
            token,
            stages,
            connections,
        } = self;
        validate_stage_ids(&stages)?;
        let mut sources_by_target = validate_connections(token, &stages, &connections)?;
        validate_input_counts(&stages, &sources_by_target)?;
        validate_acyclic(stages.len(), &sources_by_target)?;

        let stage_ids = stages
            .iter()
            .map(|stage| StageId::new_validated(stage.id.clone()))
            .collect::<Vec<_>>();
        let stages = stages
            .into_iter()
            .enumerate()
            .map(|(index, stage)| StageDefinition {
                id: stage_ids[index].clone(),
                operation: stage.operation,
                sources: sources_by_target[index]
                    .take()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|source| stage_ids[source].clone())
                    .collect(),
            })
            .collect();

        Ok(Topology { stages })
    }
}

impl<D> Default for TopologyBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_input_counts<D: InputCount>(
    stages: &[PendingStage<D>],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (stage, sources) in stages.iter().zip(sources_by_target) {
        let expected = stage.operation.input_count();
        let actual = sources.as_ref().map_or(0, Vec::len);
        if actual != expected {
            return Err(TopologyError::InputCount {
                stage: stage.id.clone(),
                expected,
                actual,
            });
        }
    }
    Ok(())
}

fn validate_stage_ids<D>(stages: &[PendingStage<D>]) -> Result<(), TopologyError> {
    if stages.is_empty() {
        return Err(TopologyError::EmptyTopology);
    }

    let mut seen = HashSet::with_capacity(stages.len());
    for stage in stages {
        let reason = if stage.id.is_empty() {
            Some(InvalidStageIdReason::Empty)
        } else if stage.id.as_bytes().contains(&0) {
            Some(InvalidStageIdReason::ContainsNul)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(TopologyError::InvalidStageId {
                id: stage.id.clone(),
                reason,
            });
        }
        if !seen.insert(stage.id.as_str()) {
            return Err(TopologyError::DuplicateStageId(stage.id.clone()));
        }
    }
    Ok(())
}

fn validate_connections<D>(
    token: u64,
    stages: &[PendingStage<D>],
    connections: &[PendingConnection],
) -> Result<Vec<Option<Vec<usize>>>, TopologyError> {
    let mut sources_by_target = vec![None; stages.len()];
    for connection in connections {
        let target = resolve_ref(token, stages.len(), connection.target)?;
        if connection.sources.is_empty() {
            return Err(TopologyError::EmptySources(stages[target].id.clone()));
        }
        if sources_by_target[target].is_some() {
            return Err(TopologyError::SourcesAlreadySet(stages[target].id.clone()));
        }

        let sources = connection
            .sources
            .iter()
            .map(|source| resolve_ref(token, stages.len(), *source))
            .collect::<Result<Vec<_>, _>>()?;
        if sources.contains(&target) {
            return Err(TopologyError::SelfLoop(stages[target].id.clone()));
        }
        sources_by_target[target] = Some(sources);
    }
    Ok(sources_by_target)
}

fn resolve_ref(
    token: u64,
    stage_count: usize,
    reference: StageRef,
) -> Result<usize, TopologyError> {
    if reference.builder_token != token || reference.index >= stage_count {
        Err(TopologyError::ForeignStageRef(reference))
    } else {
        Ok(reference.index)
    }
}

fn validate_acyclic(
    stage_count: usize,
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    let mut indegrees = vec![0_usize; stage_count];
    let mut targets_by_source = vec![Vec::new(); stage_count];
    for (target, sources) in sources_by_target.iter().enumerate() {
        for source in sources.iter().flatten() {
            indegrees[target] += 1;
            targets_by_source[*source].push(target);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(stage, indegree)| (*indegree == 0).then_some(stage))
        .collect::<VecDeque<_>>();
    let mut visited = 0_usize;
    while let Some(source) = ready.pop_front() {
        visited += 1;
        for target in &targets_by_source[source] {
            indegrees[*target] -= 1;
            if indegrees[*target] == 0 {
                ready.push_back(*target);
            }
        }
    }

    if visited == stage_count {
        Ok(())
    } else {
        Err(TopologyError::Cycle)
    }
}

#[cfg(test)]
mod tests {
    use dogpaddle_operation::{CountDefinition, OperationDefinition};

    use super::{
        InputCount, InvalidStageIdReason, StageDefinition, StageRef, Topology, TopologyBuilder,
        TopologyError,
    };

    #[derive(Debug, Eq, PartialEq)]
    struct TestDefinition {
        name: &'static str,
        input_count: usize,
    }

    impl InputCount for TestDefinition {
        fn input_count(&self) -> usize {
            self.input_count
        }
    }

    const fn definition(name: &'static str, input_count: usize) -> TestDefinition {
        TestDefinition { name, input_count }
    }

    fn builder() -> TopologyBuilder<TestDefinition> {
        TopologyBuilder::new()
    }

    fn find_stage<'a>(
        topology: &'a Topology<TestDefinition>,
        id: &str,
    ) -> &'a StageDefinition<TestDefinition> {
        topology
            .stages
            .iter()
            .find(|stage| stage.id.as_str() == id)
            .unwrap()
    }

    fn finish_with_input_count(
        expected: usize,
        actual: usize,
    ) -> Result<Topology<TestDefinition>, TopologyError> {
        let mut builder = builder();
        let sources = (0..actual)
            .map(|index| builder.stage(format!("source-{index}"), definition("source", 0)))
            .collect::<Vec<_>>();
        let target = builder.stage("target", definition("target", expected));
        if !sources.is_empty() {
            builder.connect(sources, target);
        }
        builder.finish()
    }

    #[test]
    fn finish_preserves_stage_and_source_order_and_resolves_references() {
        let mut builder = builder();
        let first = builder.stage("first", definition("source", 0));
        let second = builder.stage("second", definition("source", 0));
        let target = builder.stage("target", definition("join", 2));
        builder.connect([second, first], target);

        let topology = builder.finish().unwrap();

        assert_eq!(
            topology
                .stages
                .iter()
                .map(|stage| stage.id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second", "target"]
        );
        let target = find_stage(&topology, "target");
        assert_eq!(target.operation.name, "join");
        assert_eq!(target.operation, definition("join", 2));
        assert_eq!(
            target
                .sources
                .iter()
                .map(super::StageId::as_str)
                .collect::<Vec<_>>(),
            ["second", "first"]
        );
    }

    #[test]
    fn finish_rejects_an_empty_topology() {
        assert_eq!(
            builder().finish().unwrap_err(),
            TopologyError::EmptyTopology
        );
    }

    #[test]
    fn finish_rejects_invalid_stage_ids_in_declaration_order() {
        let mut builder = builder();
        builder.stage("", definition("first", 0));
        builder.stage("contains\0nul", definition("second", 0));

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::InvalidStageId {
                id: String::new(),
                reason: InvalidStageIdReason::Empty,
            }
        );

        let mut nul_builder = TopologyBuilder::new();
        nul_builder.stage("contains\0nul", definition("only", 0));
        assert_eq!(
            nul_builder.finish().unwrap_err(),
            TopologyError::InvalidStageId {
                id: "contains\0nul".to_owned(),
                reason: InvalidStageIdReason::ContainsNul,
            }
        );
    }

    #[test]
    fn finish_rejects_duplicate_stage_ids_before_connections() {
        let mut builder = builder();
        let first = builder.stage("same", definition("first", 0));
        builder.stage("same", definition("second", 0));
        builder.connect(
            [first],
            StageRef {
                builder_token: 0,
                index: 0,
            },
        );

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::DuplicateStageId("same".to_owned())
        );
    }

    #[test]
    fn finish_rejects_foreign_source_and_target_references() {
        let mut left = builder();
        let foreign = left.stage("foreign", definition("source", 0));

        let mut right = builder();
        let target = right.stage("target", definition("sink", 1));
        right.connect([foreign], target);
        assert_eq!(
            right.finish().unwrap_err(),
            TopologyError::ForeignStageRef(foreign)
        );

        let mut right = builder();
        let source = right.stage("source", definition("source", 0));
        right.connect([source], foreign);
        assert_eq!(
            right.finish().unwrap_err(),
            TopologyError::ForeignStageRef(foreign)
        );
    }

    #[test]
    fn finish_rejects_an_explicit_empty_source_list() {
        let mut builder = builder();
        let target = builder.stage("target", definition("source", 0));
        builder.connect([], target);

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::EmptySources("target".to_owned())
        );
    }

    #[test]
    fn finish_accepts_exact_zero_unary_binary_and_n_ary_input_counts() {
        for input_count in 0..=4 {
            let topology = finish_with_input_count(input_count, input_count).unwrap();
            assert_eq!(find_stage(&topology, "target").sources.len(), input_count);
        }
    }

    #[test]
    fn finish_rejects_every_input_count_mismatch() {
        for (expected, actual) in [(0, 1), (1, 0), (1, 2), (2, 1), (3, 2), (3, 4)] {
            assert_eq!(
                finish_with_input_count(expected, actual).unwrap_err(),
                TopologyError::InputCount {
                    stage: "target".to_owned(),
                    expected,
                    actual,
                }
            );
        }
    }

    #[test]
    fn finish_rejects_setting_sources_twice() {
        let mut builder = builder();
        let first = builder.stage("first", definition("source", 0));
        let second = builder.stage("second", definition("source", 0));
        let target = builder.stage("target", definition("sink", 1));
        builder.connect([first], target);
        builder.connect([second], target);

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::SourcesAlreadySet("target".to_owned())
        );
    }

    #[test]
    fn finish_rejects_a_direct_self_loop() {
        let mut builder = builder();
        let stage = builder.stage("stage", definition("operation", 1));
        builder.connect([stage], stage);

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::SelfLoop("stage".to_owned())
        );
    }

    #[test]
    fn finish_rejects_a_multi_stage_cycle() {
        let mut builder = builder();
        let first = builder.stage("first", definition("operation", 1));
        let second = builder.stage("second", definition("operation", 1));
        let third = builder.stage("third", definition("operation", 1));
        builder.connect([first], second);
        builder.connect([second], third);
        builder.connect([third], first);

        assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
    }

    #[test]
    fn finish_allows_fan_out_and_repeated_sources() {
        let mut builder = builder();
        let source = builder.stage("source", definition("source", 0));
        let left = builder.stage("left", definition("sink", 1));
        let right = builder.stage("right", definition("join", 2));
        builder.connect([source], left);
        builder.connect([source, source], right);

        let topology = builder.finish().unwrap();

        assert_eq!(find_stage(&topology, "left").sources.len(), 1);
        assert_eq!(
            find_stage(&topology, "right")
                .sources
                .iter()
                .map(super::StageId::as_str)
                .collect::<Vec<_>>(),
            ["source", "source"]
        );
    }

    #[test]
    fn finish_allows_zero_input_stages_and_disconnected_components() {
        let mut builder = builder();
        builder.stage("isolated", definition("source", 0));
        let source = builder.stage("source", definition("source", 0));
        let sink = builder.stage("sink", definition("sink", 1));
        builder.connect([source], sink);

        let topology = builder.finish().unwrap();

        assert!(find_stage(&topology, "isolated").sources.is_empty());
        assert!(find_stage(&topology, "source").sources.is_empty());
        assert_eq!(find_stage(&topology, "sink").sources.len(), 1);
    }

    #[test]
    fn finish_rejects_a_cycle_in_one_of_multiple_components() {
        let mut builder = builder();
        builder.stage("isolated", definition("source", 0));
        let left = builder.stage("left", definition("operation", 1));
        let right = builder.stage("right", definition("operation", 1));
        builder.connect([left], right);
        builder.connect([right], left);

        assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
    }

    #[test]
    fn finish_uses_the_closed_operation_definition_input_count() {
        let mut builder = TopologyBuilder::<OperationDefinition>::new();
        builder.stage("count", CountDefinition::new().into());

        assert_eq!(
            builder.finish().unwrap_err(),
            TopologyError::InputCount {
                stage: "count".to_owned(),
                expected: 1,
                actual: 0,
            }
        );
    }
}
