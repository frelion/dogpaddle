//! One fixed-ID relation protocol shared by database sinks.

mod plan;
mod runtime;
mod state;

use dogpaddle_change::Change;
use dogpaddle_store::Cell;
use thiserror::Error;

use crate::{DataDeclaration, definition::DataName, operation::OperationError};

pub(crate) use crate::operation::relation::{RowError, canonical_row, encode_canonical, row_hash};
pub(crate) use runtime::RelationalSink;

pub(crate) const MAX_MUTATIONS_PER_BATCH: usize = 1024;
pub(crate) const FIRST_TECHNICAL_ID: u64 = 1;
pub(crate) const MAX_TECHNICAL_ID: u64 = i64::MAX.unsigned_abs();
const EXHAUSTED_ID: u64 = MAX_TECHNICAL_ID + 1;
pub(crate) const STATE: DataName<Cell<Vec<u8>>> = DataName::new("relation_sink.state");
pub(crate) const DATA: &[DataDeclaration] = &[STATE.declaration()];

/// Position inside the complete Change retained by Station.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Position {
    pub row_index: u64,
    pub remaining: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Continuation {
    Done,
    Position(Position),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Insert {
    pub row_index: u64,
    pub technical_id: u64,
}

/// Immutable work: insert these IDs, then delete these IDs, atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Batch {
    pub inserts: Vec<Insert>,
    pub deletes: Vec<u64>,
    pub continuation: Continuation,
}

/// One request per distinct logical row. Counts may exceed the returned ID limit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Lookup {
    pub row_index: usize,
    pub needed: u64,
    pub take: usize,
}

#[derive(Debug)]
pub(crate) struct Matches {
    pub count: u64,
    pub ids: Vec<u64>,
}

/// Database I/O only; no Store access or ownership of input progress.
pub(crate) trait RelationTarget: Send + 'static {
    /// Fresh construction must reject existing targets before publishing intent.
    fn require_absent(&mut self) -> Result<(), OperationError>;
    /// Creates or verifies the owned empty layout after initialization is durable.
    fn initialize(&mut self) -> Result<(), OperationError>;
    /// Returns exact matches in request order, with ascending IDs and bounded counts.
    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError>;
    /// One target transaction; duplicate inserts and missing deletes are replay.
    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError>;
}

#[derive(Debug, Error)]
#[error("relation sink: {0}")]
struct RelationError(String);

fn invalid(message: impl Into<String>) -> OperationError {
    Box::new(RelationError(message.into()))
}

fn validate_next_id(next_id: u64) -> Result<(), OperationError> {
    if (FIRST_TECHNICAL_ID..=EXHAUSTED_ID).contains(&next_id) {
        Ok(())
    } else {
        Err(invalid("next ID is outside 1..=i64::MAX+1"))
    }
}

fn position_index(change: &Change, position: Position) -> Result<usize, OperationError> {
    let index = usize::try_from(position.row_index)
        .map_err(|_| invalid("input row position exceeds usize"))?;
    if index >= change.num_rows()
        || position.remaining == 0
        || position.remaining > change.diffs().value(index).unsigned_abs()
    {
        return Err(invalid("input position does not match the retained Change"));
    }
    Ok(index)
}

fn first_position(change: &Change) -> Position {
    Position {
        row_index: 0,
        remaining: change.diffs().value(0).unsigned_abs(),
    }
}

fn advance_position(change: &Change, position: Position, take: u64) -> Continuation {
    if take < position.remaining {
        Continuation::Position(Position {
            remaining: position.remaining - take,
            ..position
        })
    } else {
        let next = usize::try_from(position.row_index).expect("the position was validated") + 1;
        if next == change.num_rows() {
            Continuation::Done
        } else {
            Continuation::Position(Position {
                row_index: u64::try_from(next).expect("an addressable row fits u64"),
                remaining: change.diffs().value(next).unsigned_abs(),
            })
        }
    }
}

#[cfg(test)]
mod tests;
