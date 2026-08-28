use crate::{error::FlowRunError, station::ProcessOutcome};

use super::runtime::Flow;

/// Aggregate result of one bounded Flow scheduling round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    /// No Station committed progress during the round.
    Idle,
    /// At least one Station committed progress during the round.
    Progressed,
}

impl Flow {
    /// Runs one bounded scheduling round in deterministic topological order.
    ///
    /// Every Station receives at most one turn. An upstream Station's committed
    /// output is therefore visible to downstream intake later in the same
    /// round, while an unbounded source cannot monopolize the call. After every
    /// successful turn, each distinct direct upstream receives one bounded GC
    /// attempt, independently of whether the turn was idle or progressed.
    ///
    /// # Errors
    ///
    /// Returns [`FlowRunError`] with the stable Station ID when intake,
    /// processing, or an upstream GC attempt returns an error.
    ///
    /// # Panics
    ///
    /// The current implementation reaches the explicit `todo!()` in
    /// `Station::process`; the Station-Operation processing protocol is not yet
    /// defined.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        let mut outcome = AdvanceOutcome::Idle;
        for &index in &self.topology.schedule {
            let station_id = self.definition.stations()[index].id();
            let station_outcome = self.stations[index]
                .advance(&self.reads, &mut self.transactions)
                .map_err(|source| FlowRunError::new(station_id, source))?;
            for &upstream in &self.topology.gc_upstreams[index] {
                let upstream_id = self.definition.stations()[upstream].id();
                self.stations[upstream]
                    .gc(&mut self.transactions)
                    .map_err(|source| FlowRunError::new(upstream_id, source))?;
            }
            if station_outcome == ProcessOutcome::Progressed {
                outcome = AdvanceOutcome::Progressed;
            }
        }
        Ok(outcome)
    }
}
