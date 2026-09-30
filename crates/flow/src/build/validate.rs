use super::{
    DeclaredOperation, OperationRef,
    definition::{FlowDefinition, OperationNode},
};
use std::collections::HashSet;
use thiserror::Error;

pub(crate) const MAX_OPERATIONS: usize = 1024;
pub(crate) const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Consumer {
    pub(crate) operation: usize,
    pub(crate) port: usize,
}

/// Derived indices only: no persistent identity, queue or lifecycle.
#[derive(Debug)]
pub(crate) struct ResolvedTopology {
    pub(crate) schedule: Vec<usize>,
    pub(crate) tails: Vec<Vec<usize>>,
    pub(crate) consumers: Vec<Vec<Consumer>>,
    pub(crate) heads: Vec<bool>,
}

/// Why a stable Operation ID is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvalidOperationIdReason {
    /// The ID is empty.
    Empty,
    /// The ID contains NUL.
    ContainsNul,
    /// The ID exceeds 1024 UTF-8 bytes.
    TooLong,
}

/// Failure while validating the logical DAG.
#[derive(Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TopologyError {
    /// A Flow needs at least one Operation.
    #[error("a flow must contain at least one operation")]
    EmptyTopology,
    /// An ID violates the identity rules.
    #[error("invalid operation ID {id:?}: {reason:?}")]
    InvalidOperationId {
        /// Rejected ID.
        id: String,
        /// Rejection reason.
        reason: InvalidOperationIdReason,
    },
    /// Two Operations share an ID.
    #[error("duplicate operation ID {0:?}")]
    DuplicateOperationId(String),
    /// A reference comes from another factory or is not an earlier declaration.
    #[error("operation reference is not valid in this flow factory")]
    ForeignOperationRef(OperationRef),
    /// A persisted edge does not identify an Operation.
    #[error("operation {operation:?} references unknown input {input}")]
    UnknownInput {
        /// Consumer ID.
        operation: String,
        /// Invalid producer ordinal.
        input: usize,
    },
    /// The graph contains a cycle.
    #[error("flow topology contains a cycle")]
    Cycle,
    /// A root is not a source.
    #[error("root operation {0:?} is not a source")]
    RootIsNotScan(String),
    /// A leaf is not a sink.
    #[error("terminal operation {0:?} is not a sink")]
    TerminalIsNotSink(String),
    /// The connected arity differs from the declaration.
    #[error("operation {operation:?} requires {expected} inputs but received {actual}")]
    InputCount {
        /// Consumer ID.
        operation: String,
        /// Declared arity.
        expected: usize,
        /// Connected arity.
        actual: usize,
    },
    /// A sink cannot produce an input.
    #[error("operation {operation:?} reads outputless input {input:?}")]
    InputHasNoOutput {
        /// Consumer ID.
        operation: String,
        /// Producer ID.
        input: String,
    },
    /// The graph exceeds a fixed construction bound.
    #[error("flow exceeds {0}")]
    Limit(&'static str),
}

pub(super) fn finish_definition(
    owner_identity: Option<[u8; 32]>,
    token: u64,
    declarations: Vec<DeclaredOperation>,
) -> Result<FlowDefinition, TopologyError> {
    let mut operations = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        let inputs = declaration
            .inputs
            .into_iter()
            .map(|reference| {
                if reference.factory_token != token || reference.index >= operations.len() {
                    Err(TopologyError::ForeignOperationRef(reference))
                } else {
                    Ok(reference.index)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        operations.push(OperationNode {
            id: declaration.id,
            definition: declaration.definition,
            inputs,
        });
    }
    let definition = FlowDefinition {
        owner_identity,
        operations,
    };
    resolve(&definition)?;
    Ok(definition)
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep bounded DAG validation and derived call indices together."
)]
pub(crate) fn resolve(definition: &FlowDefinition) -> Result<ResolvedTopology, TopologyError> {
    let nodes = &definition.operations;
    if nodes.is_empty() {
        return Err(TopologyError::EmptyTopology);
    }
    if nodes.len() > MAX_OPERATIONS {
        return Err(TopologyError::Limit("1024 operations"));
    }
    let mut ids = HashSet::new();
    for node in nodes {
        let reason = if node.id.is_empty() {
            Some(InvalidOperationIdReason::Empty)
        } else if node.id.contains('\0') {
            Some(InvalidOperationIdReason::ContainsNul)
        } else if node.id.len() > 1024 {
            Some(InvalidOperationIdReason::TooLong)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(TopologyError::InvalidOperationId {
                id: node.id.clone(),
                reason,
            });
        }
        if !ids.insert(&node.id) {
            return Err(TopologyError::DuplicateOperationId(node.id.clone()));
        }
    }
    let mut consumers = vec![Vec::new(); nodes.len()];
    let mut indegrees = vec![0; nodes.len()];
    for (index, node) in nodes.iter().enumerate() {
        let expected = node.definition.kind().input_count() as usize;
        if expected != node.inputs.len() {
            return Err(TopologyError::InputCount {
                operation: node.id.clone(),
                expected,
                actual: node.inputs.len(),
            });
        }
        for (port, &input) in node.inputs.iter().enumerate() {
            let producer = nodes
                .get(input)
                .ok_or_else(|| TopologyError::UnknownInput {
                    operation: node.id.clone(),
                    input,
                })?;
            if !producer.definition.kind().has_output() {
                return Err(TopologyError::InputHasNoOutput {
                    operation: node.id.clone(),
                    input: producer.id.clone(),
                });
            }
            consumers[input].push(Consumer {
                operation: index,
                port,
            });
            indegrees[index] += 1;
        }
    }
    for (index, node) in nodes.iter().enumerate() {
        if indegrees[index] == 0 && !node.definition.kind().is_scan() {
            return Err(TopologyError::RootIsNotScan(node.id.clone()));
        }
        if consumers[index].is_empty() && !node.definition.kind().is_sink() {
            return Err(TopologyError::TerminalIsNotSink(node.id.clone()));
        }
    }
    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(i, &n)| (n == 0).then_some(i))
        .collect::<Vec<_>>();
    let mut schedule = Vec::with_capacity(nodes.len());
    while !ready.is_empty() {
        let mut next = Vec::new();
        for index in ready {
            schedule.push(index);
            for consumer in &consumers[index] {
                indegrees[consumer.operation] -= 1;
                if indegrees[consumer.operation] == 0 {
                    next.push(consumer.operation);
                }
            }
        }
        next.sort_unstable();
        ready = next;
    }
    if schedule.len() != nodes.len() {
        return Err(TopologyError::Cycle);
    }
    let mut tails = vec![Vec::new(); nodes.len()];
    let mut heads = vec![true; nodes.len()];
    let mut head_of = (0..nodes.len()).collect::<Vec<_>>();
    for &index in &schedule {
        let node = &nodes[index];
        if node.definition.kind().is_atomic() && node.inputs.len() == 1 {
            let producer = node.inputs[0];
            if consumers[producer].len() == 1 {
                let head = head_of[producer];
                tails[head].push(index);
                heads[index] = false;
                head_of[index] = head;
            }
        }
    }
    let mut depth = vec![0; nodes.len()];
    for &index in &schedule {
        if !heads[index] || nodes[index].definition.kind().is_sink() {
            continue;
        }
        depth[index] = nodes[index]
            .inputs
            .iter()
            .map(|&input| depth[head_of[input]])
            .max()
            .unwrap_or(0)
            + 1;
        if depth[index] > MAX_DEPTH {
            return Err(TopologyError::Limit("64 call frames"));
        }
    }
    let consumers = (0..nodes.len())
        .map(|index| consumers[tails[index].last().copied().unwrap_or(index)].clone())
        .collect();
    Ok(ResolvedTopology {
        schedule,
        tails,
        consumers,
        heads,
    })
}
