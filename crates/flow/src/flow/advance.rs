use crate::{error::FlowRunError, station::ProcessOutcome};

use super::runtime::Flow;

/// Aggregate result of one bounded Flow scheduling round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    /// No Station committed progress or encountered output pressure during the round.
    Idle,
    /// No Station committed progress, but at least one output was rejected by its capacity.
    Backpressured,
    /// At least one Operation, durable input pin, or physical GC committed progress.
    Progressed,
}

impl Flow {
    /// Runs one bounded scheduling round in deterministic topological order.
    ///
    /// Every Station receives at most one turn. An upstream Station's committed
    /// output is therefore visible to downstream intake later in the same
    /// round, while an unbounded source cannot monopolize the call. After every
    /// successful turn, each distinct direct upstream receives one bounded GC
    /// attempt, independently of whether the turn was idle, backpressured, or
    /// progressed. Backpressure never short-circuits the remaining schedule.
    /// Outcomes aggregate as `Progressed > Backpressured > Idle`; physical GC
    /// that advances an output head therefore counts as progress even when its
    /// downstream Operation was idle.
    /// Selecting a non-active input durably pins that port before its Operation
    /// turn. That pin counts as progress even if the Operation is idle; a later
    /// turn with the already-pinned input can then report idle normally.
    ///
    /// # Errors
    ///
    /// Returns [`FlowRunError`] with the stable Station ID when intake,
    /// processing, or an upstream GC attempt returns an error.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        let mut outcome = ProcessOutcome::Idle;
        for &index in &self.topology.schedule {
            let station_id = self.definition.stations()[index].id();
            let station_outcome = self.stations[index]
                .advance(&self.reads, &mut self.transactions)
                .map_err(|source| FlowRunError::new(station_id, source))?;
            for &upstream in &self.topology.gc_upstreams[index] {
                let upstream_id = self.definition.stations()[upstream].id();
                let collected = self.stations[upstream]
                    .gc(&mut self.transactions)
                    .map_err(|source| FlowRunError::new(upstream_id, source))?;
                if collected {
                    outcome = outcome.join(ProcessOutcome::Progressed);
                }
            }
            outcome = outcome.join(station_outcome);
        }
        Ok(match outcome {
            ProcessOutcome::Idle => AdvanceOutcome::Idle,
            ProcessOutcome::Backpressured => AdvanceOutcome::Backpressured,
            ProcessOutcome::Progressed => AdvanceOutcome::Progressed,
        })
    }
}
