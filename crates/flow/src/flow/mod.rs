use std::path::{Path, PathBuf};

use dogpaddle_store::{OrderedMap, Small, Transactions};

use crate::{
    build::{FlowDefinition, StageDefinition},
    stage::Stage,
};

/// The runtime handle for a built or reopened persistent Flow.
///
/// A Flow uniquely owns its Store transaction capability. During future work,
/// it will temporarily lend `&mut Transactions` to one runtime stage. The stage
/// starts and commits that work transaction during the call, but cannot retain
/// the transaction-start capability across calls. The definition and data
/// object set were frozen by a successful build.
pub struct Flow {
    path: PathBuf,
    definition: FlowDefinition,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "flow state is consumed by the next run phase")
    )]
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "stage instances are consumed by the next run phase"
        )
    )]
    stages: Vec<Stage>,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "transactions are consumed by the next run phase")
    )]
    transactions: Transactions,
}

impl Flow {
    pub(crate) fn from_parts(
        path: PathBuf,
        definition: FlowDefinition,
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        stages: Vec<Stage>,
        transactions: Transactions,
    ) -> Self {
        Self {
            path,
            definition,
            state,
            stages,
            transactions,
        }
    }

    /// Returns the Store path owned by this Flow.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of stages in declaration order.
    #[must_use]
    pub fn stage_count(&self) -> usize {
        self.definition.stages().len()
    }

    /// Iterates over stable stage IDs in declaration order.
    #[must_use]
    pub fn stage_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.definition.stages().iter().map(StageDefinition::id)
    }

    #[cfg(test)]
    pub(crate) fn into_runtime_parts(self) -> (Transactions, Vec<Stage>) {
        (self.transactions, self.stages)
    }
}

#[cfg(test)]
mod tests;
