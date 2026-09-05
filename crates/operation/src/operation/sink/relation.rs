//! Pure mechanics shared by exact multiset-relation sinks.

use dogpaddle_change::Change;

pub(crate) const MAX_MUTATIONS_PER_BATCH: usize = 1024;
pub(crate) const FIRST_TECHNICAL_ID: u64 = 1;
pub(crate) const MAX_TECHNICAL_ID: u64 = i64::MAX.unsigned_abs();
pub(crate) const EXHAUSTED_TECHNICAL_ID: u64 = MAX_TECHNICAL_ID + 1;

/// Durable position within the complete Change retained by the owning Station.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Position {
    pub(crate) row_index: u64,
    pub(crate) remaining: u64,
}

/// Work that follows one concrete relation-mutation batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Continuation {
    Done,
    Position(Position),
}

/// One concrete physical-row mutation selected during preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Mutation {
    pub(crate) kind: MutationKind,
    pub(crate) row_index: u64,
    pub(crate) technical_id: u64,
}

/// Direction of one concrete physical-row mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationKind {
    Insert,
    Delete,
}

/// A target-neutral violation of relation continuation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelationError {
    InvalidNextId { next_id: u64 },
    TechnicalIdExhausted { next_id: u64, needed: u64 },
    InputMismatch { message: String },
}

pub(crate) fn validate_next_id(next_id: u64) -> Result<(), RelationError> {
    if (FIRST_TECHNICAL_ID..=EXHAUSTED_TECHNICAL_ID).contains(&next_id) {
        Ok(())
    } else {
        Err(RelationError::InvalidNextId { next_id })
    }
}

pub(crate) fn ensure_id_capacity(next_id: u64, needed: u64) -> Result<(), RelationError> {
    let available = EXHAUSTED_TECHNICAL_ID.saturating_sub(next_id);
    if needed <= available {
        Ok(())
    } else {
        Err(RelationError::TechnicalIdExhausted { next_id, needed })
    }
}

pub(crate) fn first_position(change: &Change) -> Position {
    let diff = change.diffs().value(0);
    Position {
        row_index: 0,
        remaining: diff.unsigned_abs(),
    }
}

pub(crate) fn position_index(change: &Change, position: Position) -> Result<usize, RelationError> {
    let row_index = usize::try_from(position.row_index)
        .map_err(|_| mismatch("row index cannot be represented by usize"))?;
    if row_index >= change.num_rows() {
        return Err(mismatch(format!(
            "row index {} is outside a {}-row Change",
            position.row_index,
            change.num_rows()
        )));
    }
    Ok(row_index)
}

pub(crate) fn validate_position_for_change(
    change: &Change,
    position: Position,
) -> Result<(), RelationError> {
    if position.remaining == 0 {
        return Err(mismatch("position has zero remaining multiplicity"));
    }
    let row_index = position_index(change, position)?;
    let magnitude = change.diffs().value(row_index).unsigned_abs();
    if position.remaining > magnitude {
        return Err(mismatch(format!(
            "position remainder {} exceeds row {} magnitude {magnitude}",
            position.remaining, position.row_index
        )));
    }
    Ok(())
}

pub(crate) fn validate_batch_boundary(
    change: &Change,
    position: Position,
) -> Result<(), RelationError> {
    let row_index = position_index(change, position)?;
    let batch_size = u64::try_from(MAX_MUTATIONS_PER_BATCH)
        .expect("the fixed relation mutation batch size fits u64");
    let mut consumed_modulo = 0_u64;
    for index in 0..row_index {
        consumed_modulo = (consumed_modulo
            + change.diffs().value(index).unsigned_abs() % batch_size)
            % batch_size;
    }
    let row_magnitude = change.diffs().value(row_index).unsigned_abs();
    consumed_modulo =
        (consumed_modulo + (row_magnitude - position.remaining) % batch_size) % batch_size;
    if consumed_modulo == 0 {
        Ok(())
    } else {
        Err(mismatch(
            "position is not aligned to a stable 1024-mutation batch boundary",
        ))
    }
}

pub(crate) fn advance_position(
    change: &Change,
    position: Position,
    consumed: u64,
) -> Result<Continuation, RelationError> {
    if consumed == 0 || consumed > position.remaining {
        return Err(mismatch("batch consumed an invalid multiplicity"));
    }
    if consumed < position.remaining {
        return Ok(Continuation::Position(Position {
            row_index: position.row_index,
            remaining: position.remaining - consumed,
        }));
    }
    let next_row = position
        .row_index
        .checked_add(1)
        .ok_or_else(|| mismatch("row index overflow"))?;
    let next_index = usize::try_from(next_row)
        .map_err(|_| mismatch("row index cannot be represented by usize"))?;
    if next_index == change.num_rows() {
        return Ok(Continuation::Done);
    }
    if next_index > change.num_rows() {
        return Err(mismatch("row position advanced beyond the Change"));
    }
    Ok(Continuation::Position(Position {
        row_index: next_row,
        remaining: change.diffs().value(next_index).unsigned_abs(),
    }))
}

pub(crate) fn validate_apply_for_change(
    change: &Change,
    next_id: u64,
    start_position: Position,
    continuation: Continuation,
    mutations: &[Mutation],
) -> Result<(), RelationError> {
    validate_position_for_change(change, start_position)?;
    validate_batch_boundary(change, start_position)?;
    let insert_count = u64::try_from(
        mutations
            .iter()
            .filter(|mutation| mutation.kind == MutationKind::Insert)
            .count(),
    )
    .expect("the bounded mutation count fits u64");
    let reserved_start = if insert_count == 0 {
        None
    } else {
        Some(
            next_id
                .checked_sub(insert_count)
                .filter(|first| *first >= FIRST_TECHNICAL_ID)
                .ok_or_else(|| mismatch("prepared insert count exceeds the durable ID frontier"))?,
        )
    };
    let mut actual = Continuation::Position(start_position);
    let mut last_insert_id = None;
    for (mutation_index, mutation) in mutations.iter().enumerate() {
        let Continuation::Position(position) = actual else {
            return Err(mismatch("mutations continue after the Change ends"));
        };
        let row_index = position_index(change, position)?;
        if mutation.row_index != position.row_index {
            return Err(mismatch(
                "mutation row does not match the flattened position",
            ));
        }
        let expected_kind = if change.diffs().value(row_index) > 0 {
            MutationKind::Insert
        } else {
            MutationKind::Delete
        };
        if mutation.kind != expected_kind {
            return Err(mismatch("mutation kind disagrees with the row difference"));
        }
        if mutation.technical_id >= next_id {
            return Err(mismatch(
                "prepared mutation ID is not below the durable next-ID frontier",
            ));
        }
        match mutation.kind {
            MutationKind::Insert => last_insert_id = Some(mutation.technical_id),
            MutationKind::Delete => {
                if reserved_start.is_some_and(|first| mutation.technical_id >= first)
                    && last_insert_id.is_none_or(|last| mutation.technical_id > last)
                {
                    return Err(mismatch(
                        "a mutation deletes a newly allocated ID before inserting it",
                    ));
                }
            }
        }
        actual = advance_position(change, position, 1)?;
        if actual == Continuation::Done && mutation_index + 1 != mutations.len() {
            return Err(mismatch("mutations continue after the Change ends"));
        }
    }
    if actual != continuation {
        return Err(mismatch(
            "stored continuation differs from the flattened mutation sequence",
        ));
    }
    if let Some(last_insert_id) = last_insert_id
        && last_insert_id.checked_add(1) != Some(next_id)
    {
        return Err(mismatch(
            "prepared insert range does not end at the durable next-ID frontier",
        ));
    }
    Ok(())
}

fn mismatch(message: impl Into<String>) -> RelationError {
    RelationError::InputMismatch {
        message: message.into(),
    }
}
