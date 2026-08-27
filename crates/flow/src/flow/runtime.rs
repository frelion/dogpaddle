use std::path::{Path, PathBuf};

use dogpaddle_store::{OrderedMap, ReadTransactions, Small, Transactions};

use crate::{
    build::{FlowDefinition, StationDefinition},
    station::Station,
};

/// The runtime handle for a built or reopened persistent Flow.
///
/// A Flow owns separate Store capabilities for beginning read-only and write
/// transactions. During future scheduling it will lend the read capability to
/// station intake and the write capability to one station's process phase. A
/// station cannot retain either transaction-start capability across calls. The
/// definition and data object set were frozen by a successful build.
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
    pub(super) state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "station instances are consumed by the future scheduling phase"
        )
    )]
    pub(super) stations: Vec<Station>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "transactions are consumed by the future scheduling phase"
        )
    )]
    pub(super) transactions: Transactions,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "read transactions are consumed by the future scheduling phase"
        )
    )]
    pub(super) reads: ReadTransactions,
}

impl Flow {
    pub(crate) fn from_parts(
        path: PathBuf,
        definition: FlowDefinition,
        state: OrderedMap<Vec<u8>, Vec<u8>, Small>,
        stations: Vec<Station>,
        transactions: Transactions,
        reads: ReadTransactions,
    ) -> Self {
        Self {
            path,
            definition,
            state,
            stations,
            transactions,
            reads,
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
    pub(crate) fn into_runtime_parts(self) -> (Transactions, ReadTransactions, Vec<Station>) {
        (self.transactions, self.reads, self.stations)
    }
}
