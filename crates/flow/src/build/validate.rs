use std::collections::{HashSet, VecDeque};

use thiserror::Error;

use super::{
    StageRef,
    definition::{FlowDefinition, StageDefinition},
};

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
    /// A connection names a stage that has no output stream as its source.
    #[error("stage {target_stage:?} cannot read from outputless stage {source_stage:?}")]
    SourceHasNoOutput {
        /// Stage without an output stream.
        source_stage: String,
        /// Stage that attempted to consume it.
        target_stage: String,
    },
    /// The topology contains an indirect cycle.
    #[error("flow topology contains a cycle")]
    Cycle,
}

pub(super) fn finish_definition(
    token: u64,
    mut stages: Vec<StageDefinition>,
    connections: &[(Vec<StageRef>, StageRef)],
) -> Result<FlowDefinition, TopologyError> {
    validate_stage_ids(&stages)?;
    let mut sources_by_target = validate_connections(token, &stages, connections)?;
    validate_input_counts(&stages, &sources_by_target)?;
    validate_sources_produce_output(&stages, &sources_by_target)?;
    validate_acyclic(stages.len(), &sources_by_target)?;

    let stage_ids = stages
        .iter()
        .map(|stage| stage.id.clone())
        .collect::<Vec<_>>();
    for (index, stage) in stages.iter_mut().enumerate() {
        stage.sources = sources_by_target[index]
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(|source| stage_ids[source].clone())
            .collect();
    }

    Ok(FlowDefinition::new(stages))
}

pub(super) fn validate_decoded_definition(
    stages: &[StageDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    validate_stage_ids(stages)?;
    for (target, sources) in sources_by_target.iter().enumerate() {
        if sources
            .as_ref()
            .is_some_and(|sources| sources.contains(&target))
        {
            return Err(TopologyError::SelfLoop(stages[target].id.clone()));
        }
    }
    validate_input_counts(stages, sources_by_target)?;
    validate_sources_produce_output(stages, sources_by_target)?;
    validate_acyclic(stages.len(), sources_by_target)
}

fn validate_sources_produce_output(
    stages: &[StageDefinition],
    sources_by_target: &[Option<Vec<usize>>],
) -> Result<(), TopologyError> {
    for (target, sources) in sources_by_target.iter().enumerate() {
        for source in sources.iter().flatten() {
            if !stages[*source].operation.produces_output() {
                return Err(TopologyError::SourceHasNoOutput {
                    source_stage: stages[*source].id.clone(),
                    target_stage: stages[target].id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_input_counts(
    stages: &[StageDefinition],
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

pub(super) fn validate_stage_ids(stages: &[StageDefinition]) -> Result<(), TopologyError> {
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

pub(super) fn validate_connections(
    token: u64,
    stages: &[StageDefinition],
    connections: &[(Vec<StageRef>, StageRef)],
) -> Result<Vec<Option<Vec<usize>>>, TopologyError> {
    let mut sources_by_target = vec![None; stages.len()];
    for (sources, target) in connections {
        let target = resolve_ref(token, stages.len(), *target)?;
        if sources.is_empty() {
            return Err(TopologyError::EmptySources(stages[target].id.clone()));
        }
        if sources_by_target[target].is_some() {
            return Err(TopologyError::SourcesAlreadySet(stages[target].id.clone()));
        }

        let sources = sources
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

pub(super) fn validate_acyclic(
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
