use crate::error::FlowRunError;

use super::runtime::Flow;

/// Aggregate result of one bounded Flow scheduling round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    /// No Station committed progress or encountered output pressure during the round.
    Idle,
    /// No Station committed progress, but at least one output was rejected by its capacity.
    Backpressured,
    /// At least one Operation, durable input pin, or inline reclaim committed progress.
    Progressed,
}

impl AdvanceOutcome {
    pub(crate) const fn join(self, other: Self) -> Self {
        match (self, other) {
            (Self::Progressed, _) | (_, Self::Progressed) => Self::Progressed,
            (Self::Backpressured, _) | (_, Self::Backpressured) => Self::Backpressured,
            (Self::Idle, Self::Idle) => Self::Idle,
        }
    }
}

impl Flow {
    /// Runs one bounded scheduling round in deterministic topological order.
    ///
    /// Every Station receives at most one turn. An upstream Station's committed
    /// output is therefore visible to downstream intake later in the same
    /// round, while an unbounded source cannot monopolize the call. Completing
    /// an input advances its consumer frontier and, when all consumers have
    /// completed the physical head, reclaims that one entry in the same
    /// transaction. Backpressure never short-circuits the remaining schedule.
    /// Outcomes aggregate as `Progressed > Backpressured > Idle`.
    /// Selecting a non-active input durably pins that port before its Operation
    /// turn. That pin counts as progress even if the Operation is idle; a later
    /// turn with the already-pinned input can then report idle normally.
    ///
    /// # Errors
    ///
    /// Returns [`FlowRunError`] with the stable Station ID when intake,
    /// or processing returns an error.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        let mut outcome = AdvanceOutcome::Idle;
        for &index in &self.topology.schedule {
            let station_id = self.definition.stations()[index].id();
            let station_outcome = self.stations[index]
                .advance(&self.reads, &mut self.transactions)
                .map_err(|source| FlowRunError::new(station_id, source))?;
            outcome = outcome.join(station_outcome);
        }
        Ok(outcome)
    }
}
