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
    /// round, while an unbounded source cannot monopolize the call.
    ///
    /// # Errors
    ///
    /// Returns [`FlowRunError`] with the stable Station ID when intake or the
    /// Station processing phase returns an error.
    ///
    /// # Panics
    ///
    /// The current implementation reaches the explicit `todo!()` in
    /// `Station::process`; the Station-Operation processing protocol is not yet
    /// defined.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        let mut outcome = AdvanceOutcome::Idle;
        for &index in &self.schedule {
            let station_id = self.definition.stations()[index].id();
            let station_outcome = self.stations[index]
                .advance(&self.reads, &mut self.transactions)
                .map_err(|source| FlowRunError::new(station_id, source))?;
            if station_outcome == ProcessOutcome::Progressed {
                outcome = AdvanceOutcome::Progressed;
            }
        }
        Ok(outcome)
    }
}
