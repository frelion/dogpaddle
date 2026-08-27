use std::path::{Path, PathBuf};

use dogpaddle_store::{OrderedMap, Small, Transactions};

use crate::{
    build::{FlowDefinition, StationDefinition},
    station::Station,
};

/// The runtime handle for a built or reopened persistent Flow.
///
/// A Flow uniquely owns its Store transaction capability. During future work,
/// it will temporarily lend `&mut Transactions` to one runtime station. The
/// station starts and commits that work transaction during the call, but cannot
/// retain the transaction-start capability across calls. The definition and
/// data object set were frozen by a successful build.
pub struct Flow {
    path: PathBuf,
    definition: FlowDefinition,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "flow state is consumed by the future scheduling phase"
        )
    )]
    state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "station instances are consumed by the future scheduling phase"
        )
    )]
    stations: Vec<Station>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "transactions are consumed by the future scheduling phase"
        )
    )]
    transactions: Transactions,
}

impl Flow {
    pub(crate) fn from_parts(
        path: PathBuf,
        definition: FlowDefinition,
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        stations: Vec<Station>,
        transactions: Transactions,
    ) -> Self {
        Self {
            path,
            definition,
            state,
            stations,
            transactions,
        }
    }

    /// Returns the Store path owned by this Flow.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of stations in declaration order.
    #[must_use]
    pub fn station_count(&self) -> usize {
        self.definition.stations().len()
    }

    /// Iterates over stable station IDs in declaration order.
    #[must_use]
    pub fn station_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.definition.stations().iter().map(StationDefinition::id)
    }

    #[cfg(test)]
    pub(crate) fn into_runtime_parts(self) -> (Transactions, Vec<Station>) {
        (self.transactions, self.stations)
    }
}

#[cfg(test)]
mod tests;
