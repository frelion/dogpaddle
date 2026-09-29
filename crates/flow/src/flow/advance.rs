use crate::error::FlowRunError;
use crate::station::StationError;

use super::runtime::Flow;

/// Aggregate result of one bounded Flow scheduling round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    /// No Station committed progress or encountered output pressure during the round.
    Idle,
    /// No Station committed progress, but at least one output was rejected by its capacity.
    Backpressured,
    /// At least one Operation, durable input pin, or input completion committed progress.
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
    /// Every Station receives at most one turn. An input Station's committed
    /// output is therefore visible to its consumers later in the same
    /// round, while an unbounded Scan cannot monopolize the call. Completing
    /// an input acknowledges its exact Subscription offset; Store atomically
    /// advances that position and updates log retention accounting.
    /// Backpressure never short-circuits the remaining schedule.
    /// Outcomes aggregate as `Progressed > Backpressured > Idle`.
    /// Selecting a non-active input durably pins that port before its Operation
    /// turn. That pin counts as progress even if the Operation is idle; a later
    /// turn with the already-pinned input can then report idle normally.
    /// An Operation prepares its turn without an active write transaction. A
    /// ready turn is applied once inside a transaction. Independent Station
    /// commits share a final durability barrier; a post-commit completion that
    /// can affect an external system first forces that barrier and then runs.
    ///
    /// # Errors
    ///
    /// Returns [`FlowRunError`] with the stable Station ID when intake or
    /// processing fails. [`FlowRunError::requires_reopen`] identifies failures
    /// after which this runtime cannot safely continue scheduling.
    pub fn advance(&mut self) -> Result<AdvanceOutcome, FlowRunError> {
        for station in &mut self.stations {
            station.clear_outcome();
        }
        for &index in &self.schedule {
            let station_id = &self.station_ids[index];
            self.stations[index]
                .ensure_runnable()
                .map_err(|source| FlowRunError::new(station_id, source))?;
        }

        let mut outcome = AdvanceOutcome::Idle;
        let mut pending_start = None;
        let mut batch = self.transactions.durability_batch();
        for position in 0..self.schedule.len() {
            let index = self.schedule[position];
            let station_id = &self.station_ids[index];
            let station_result = self.stations[index].advance(&self.reads, &mut batch);
            if batch.has_pending() {
                pending_start.get_or_insert(position);
            } else {
                pending_start = None;
            }
            match station_result {
                Ok(station_outcome) => outcome = outcome.join(station_outcome),
                Err(source @ StationError::DurabilityBarrier { .. }) => {
                    drop(batch);
                    if let Some(start) = pending_start {
                        for &pending in &self.schedule[start..=position] {
                            self.stations[pending].mark_needs_reopen();
                        }
                    }
                    return Err(FlowRunError::new(station_id, source));
                }
                Err(source) => {
                    if let Err(barrier) = batch.finish() {
                        return Err(self.durability_failure(pending_start, position + 1, barrier));
                    }
                    return Err(FlowRunError::new(station_id, source));
                }
            }
        }
        batch.finish().map_err(|source| {
            self.durability_failure(pending_start, self.schedule.len(), source)
        })?;
        Ok(outcome)
    }

    pub(super) fn durability_failure(
        &mut self,
        pending_start: Option<usize>,
        end: usize,
        source: dogpaddle_store::StoreError,
    ) -> FlowRunError {
        let start =
            pending_start.expect("a failed durability barrier has at least one pending Station");
        let failed = self.schedule[start];
        for &index in &self.schedule[start..end] {
            self.stations[index].mark_needs_reopen();
        }
        FlowRunError::new(
            &self.station_ids[failed],
            StationError::DurabilityBarrier { source },
        )
    }
}
