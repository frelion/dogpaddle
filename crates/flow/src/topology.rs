use std::{
    collections::{HashSet, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_operation::OperationDefinition;
use thiserror::Error;

static NEXT_BUILDER_TOKEN: AtomicU64 = AtomicU64::new(1);

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
pub(crate) struct StageDefinition {
    id: String,
    operation: OperationDefinition,
    sources: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Topology {
    stages: Vec<StageDefinition>,
}

#[derive(Debug)]
struct PendingStage {
    id: String,
    operation: OperationDefinition,
}

#[derive(Debug)]
struct PendingConnection {
    sources: Vec<StageRef>,
    target: StageRef,
}

#[derive(Debug)]
pub(crate) struct TopologyBuilder {
    token: u64,
    stages: Vec<PendingStage>,
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

impl StageDefinition {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) const fn operation(&self) -> &OperationDefinition {
        &self.operation
    }

    pub(crate) fn sources(&self) -> impl ExactSizeIterator<Item = &str> {
        self.sources.iter().map(String::as_str)
    }
}

impl Topology {
    pub(crate) fn stages(&self) -> &[StageDefinition] {
        &self.stages
    }
}

impl TopologyBuilder {
    pub(crate) fn new() -> Self {
        let token = NEXT_BUILDER_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "topology builder token space exhausted");
        Self {
            token,
            stages: Vec::new(),
            connections: Vec::new(),
        }
    }

    pub(crate) fn stage(
        &mut self,
        id: impl Into<String>,
        operation: OperationDefinition,
    ) -> StageRef {
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

    pub(crate) fn finish(self) -> Result<Topology, TopologyError> {
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
            .map(|stage| stage.id.clone())
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

fn validate_input_counts(
    stages: &[PendingStage],
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

fn validate_stage_ids(stages: &[PendingStage]) -> Result<(), TopologyError> {
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

fn validate_connections(
    token: u64,
    stages: &[PendingStage],
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
#[path = "../tests/unit/topology.rs"]
mod tests;
