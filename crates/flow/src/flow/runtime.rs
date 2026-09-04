use std::path::{Path, PathBuf};

use dogpaddle_store::{ReadTransactions, Transactions};

use crate::station::Station;

pub(crate) struct RuntimeTopology {
    pub(crate) schedule: Vec<usize>,
}

/// The runtime handle for a built or reopened persistent Flow.
///
/// A Flow owns separate Store capabilities for beginning read-only and write
/// transactions. During scheduling it lends the read capability to Station
/// intake and the write capability to one Station's processing phase. A
/// Station cannot retain either transaction-start capability across calls.
/// The definition and data object set were frozen by a successful build. Only
/// lightweight Station IDs remain after runtime assembly.
pub struct Flow {
    path: PathBuf,
    pub(super) station_ids: Box<[String]>,
    pub(super) stations: Vec<Station>,
    pub(super) topology: RuntimeTopology,
    pub(super) transactions: Transactions,
    pub(super) reads: ReadTransactions,
}

impl Flow {
    pub(crate) fn from_parts(
        path: PathBuf,
        station_ids: Vec<String>,
        stations: Vec<Station>,
        topology: RuntimeTopology,
        transactions: Transactions,
        reads: ReadTransactions,
    ) -> Self {
        assert_eq!(
            station_ids.len(),
            stations.len(),
            "runtime Station IDs must align with assembled Stations"
        );
        Self {
            path,
            station_ids: station_ids.into_boxed_slice(),
            stations,
            topology,
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
        self.station_ids.len()
    }

    /// Iterates over stable station IDs in declaration order.
    #[must_use]
    pub fn station_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.station_ids.iter().map(String::as_str)
    }

    #[cfg(test)]
    pub(crate) fn into_runtime_parts(self) -> (Transactions, ReadTransactions, Vec<Station>) {
        (self.transactions, self.reads, self.stations)
    }
}
