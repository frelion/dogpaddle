use crate::{FlowError, error::runtime_state_error};

use super::{AdvanceOutcome, Flow};

/// Read-only state of one Station, in Flow declaration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StationStatus {
    /// Stable Station ID.
    pub id: String,
    /// Whether this runtime must be reopened before another scheduling round.
    pub needs_reopen: bool,
    /// Processing outcome in the most recent `advance` call, without the
    /// aggregate's progress precedence. None means unvisited or failed.
    /// This is an observation of that call, not a prediction of the next one.
    pub last_outcome: Option<AdvanceOutcome>,
    /// Durably selected input port; None for a Source.
    pub active_input: Option<usize>,
    /// Consumer positions in declared input-port order.
    pub inputs: Vec<InputStatus>,
    /// Retained output state; None for a Sink.
    pub output: Option<OutputStatus>,
}

/// One input edge's position in complete Changes, not rows or source events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputStatus {
    /// Next upstream log offset to complete.
    pub cursor: u64,
    /// Upstream log's exclusive tail; `tail - cursor` is this edge's backlog.
    pub tail: u64,
}

/// Physical output retention, using Store's existing accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputStatus {
    /// First retained Change offset.
    pub head: u64,
    /// Exclusive Change tail; `tail - head` is the retained entry count.
    pub tail: u64,
    /// Encoded entries and their offset keys, excluding MDBX overhead.
    pub retained_bytes: u64,
    /// Soft high watermark. An empty log may admit one oversized entry.
    pub capacity_bytes: u64,
}

impl Flow {
    /// Reads all Station counters in one short read-only Store snapshot.
    ///
    /// Does not call Operations, connect to external systems, decode Changes,
    /// begin a write transaction, or advance any position. Also available on a
    /// fail-stopped Flow. Last outcomes and fail-stop flags are runtime-only;
    /// counters and active inputs survive reopen. No snapshot handle escapes.
    ///
    /// # Errors
    ///
    /// Returns [`FlowError`] if Store access fails or a durable position is invalid.
    pub fn status(&self) -> Result<Vec<StationStatus>, FlowError> {
        let snapshot = self.reads.begin()?;
        self.station_ids
            .iter()
            .zip(&self.stations)
            .map(|(id, station)| {
                station
                    .status(id, snapshot.access())
                    .map_err(|error| runtime_state_error(id, error))
            })
            .collect()
    }
}
