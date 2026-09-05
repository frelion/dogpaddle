use std::collections::HashSet;

use dogpaddle_change::Change;

use super::{
    Batch, Continuation, Insert, MAX_MUTATIONS_PER_BATCH, Position, canonical_row, invalid,
    position_index, validate_next_id,
};
use crate::operation::OperationError;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum State {
    Initialize,
    Ready {
        next_id: u64,
        position: Option<Position>,
    },
    Prepared {
        next_id: u64,
        batch: Batch,
    },
}

impl State {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        match self {
            Self::Initialize => bytes.push(0),
            Self::Ready { next_id, position } => {
                bytes.push(1);
                bytes.extend(next_id.to_be_bytes());
                encode_position(&mut bytes, *position);
            }
            Self::Prepared { next_id, batch } => {
                bytes.push(2);
                bytes.extend(next_id.to_be_bytes());
                encode_position(
                    &mut bytes,
                    match batch.continuation {
                        Continuation::Done => None,
                        Continuation::Position(position) => Some(position),
                    },
                );
                bytes.extend(
                    u16::try_from(batch.inserts.len())
                        .expect("bounded batch")
                        .to_be_bytes(),
                );
                bytes.extend(
                    u16::try_from(batch.deletes.len())
                        .expect("bounded batch")
                        .to_be_bytes(),
                );
                for insert in &batch.inserts {
                    bytes.extend(insert.row_index.to_be_bytes());
                    bytes.extend(insert.technical_id.to_be_bytes());
                }
                for id in &batch.deletes {
                    bytes.extend(id.to_be_bytes());
                }
            }
        }
        bytes
    }

    pub(super) fn decode(mut bytes: &[u8], input: &Change) -> Result<Self, OperationError> {
        if read::<1>(&mut bytes)? != [1] {
            return Err(invalid("unknown state version"));
        }
        let tag = read::<1>(&mut bytes)?[0];
        let state = if tag == 0 {
            Self::Initialize
        } else {
            let next_id = u64::from_be_bytes(read(&mut bytes)?);
            validate_next_id(next_id)?;
            let position = decode_position(&mut bytes)?;
            if let Some(position) = position {
                position_index(input, position)?;
                if processed_modulo(input, Some(position)) != 0 {
                    return Err(invalid("continuation is not a batch boundary"));
                }
            }
            match tag {
                1 => Self::Ready { next_id, position },
                2 => {
                    let inserts = usize::from(u16::from_be_bytes(read(&mut bytes)?));
                    let deletes = usize::from(u16::from_be_bytes(read(&mut bytes)?));
                    let count = inserts + deletes;
                    if count == 0
                        || count > MAX_MUTATIONS_PER_BATCH
                        || (position.is_some() && count != MAX_MUTATIONS_PER_BATCH)
                        || (position.is_none()
                            && count % MAX_MUTATIONS_PER_BATCH != processed_modulo(input, None))
                    {
                        return Err(invalid("invalid prepared batch size"));
                    }
                    let mut batch = Batch {
                        inserts: Vec::with_capacity(inserts),
                        deletes: Vec::with_capacity(deletes),
                        continuation: position.map_or(Continuation::Done, Continuation::Position),
                    };
                    for _ in 0..inserts {
                        batch.inserts.push(Insert {
                            row_index: u64::from_be_bytes(read(&mut bytes)?),
                            technical_id: u64::from_be_bytes(read(&mut bytes)?),
                        });
                    }
                    for _ in 0..deletes {
                        batch.deletes.push(u64::from_be_bytes(read(&mut bytes)?));
                    }
                    validate_batch(&batch, next_id, input)?;
                    Self::Prepared { next_id, batch }
                }
                _ => return Err(invalid("unknown state phase")),
            }
        };
        if !bytes.is_empty() {
            return Err(invalid("trailing state bytes"));
        }
        Ok(state)
    }
}

fn encode_position(bytes: &mut Vec<u8>, position: Option<Position>) {
    bytes.push(u8::from(position.is_some()));
    if let Some(position) = position {
        bytes.extend(position.row_index.to_be_bytes());
        bytes.extend(position.remaining.to_be_bytes());
    }
}

fn decode_position(bytes: &mut &[u8]) -> Result<Option<Position>, OperationError> {
    match read::<1>(bytes)?[0] {
        0 => Ok(None),
        1 => Ok(Some(Position {
            row_index: u64::from_be_bytes(read(bytes)?),
            remaining: u64::from_be_bytes(read(bytes)?),
        })),
        _ => Err(invalid("invalid position tag")),
    }
}

fn read<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], OperationError> {
    let (value, rest) = bytes
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated state"))?;
    *bytes = rest;
    Ok(value
        .try_into()
        .expect("the split has the requested length"))
}

// Only recovery reads the preceding diffs; ordinary turns use the cached position.
fn processed_modulo(input: &Change, position: Option<Position>) -> usize {
    let (end, remaining) = position.map_or((input.num_rows(), 0), |position| {
        (
            usize::try_from(position.row_index).expect("validated position") + 1,
            position.remaining,
        )
    });
    let limit = u64::try_from(MAX_MUTATIONS_PER_BATCH).expect("batch limit fits u64");
    let total = input.diffs().values()[..end].iter().fold(0, |total, diff| {
        (total + diff.unsigned_abs() % limit) % limit
    });
    usize::try_from((total + limit - remaining % limit) % limit).expect("bounded remainder")
}

// Validate the expanded event order without persisting another copy of it.
fn validate_batch(batch: &Batch, next_id: u64, input: &Change) -> Result<(), OperationError> {
    let first_id = next_id
        .checked_sub(u64::try_from(batch.inserts.len()).expect("bounded batch"))
        .filter(|id| *id > 0)
        .ok_or_else(|| invalid("invalid insert ID frontier"))?;
    if batch.inserts.iter().enumerate().any(|(index, insert)| {
        insert.technical_id != first_id + u64::try_from(index).expect("bounded batch")
    }) || batch.deletes.iter().any(|id| *id == 0 || *id >= next_id)
        || batch.deletes.iter().copied().collect::<HashSet<_>>().len() != batch.deletes.len()
    {
        return Err(invalid("invalid prepared technical IDs"));
    }
    let (mut row, mut remaining) = match batch.continuation {
        Continuation::Done => {
            let row = input.num_rows() - 1;
            (row, input.diffs().value(row).unsigned_abs())
        }
        Continuation::Position(position) => {
            let row = position_index(input, position)?;
            (
                row,
                input.diffs().value(row).unsigned_abs() - position.remaining,
            )
        }
    };
    let mut inserts = batch.inserts.iter().rev();
    let mut deletions = batch.deletes.iter().rev();
    for _ in 0..batch.inserts.len() + batch.deletes.len() {
        if remaining == 0 {
            row = row
                .checked_sub(1)
                .ok_or_else(|| invalid("batch precedes the input"))?;
            remaining = input.diffs().value(row).unsigned_abs();
        }
        if input.diffs().value(row) > 0 {
            if inserts.next().map(|insert| insert.row_index)
                != Some(u64::try_from(row).expect("addressable row"))
            {
                return Err(invalid("prepared inserts do not match the input"));
            }
        } else {
            let id = *deletions
                .next()
                .ok_or_else(|| invalid("missing prepared deletion"))?;
            if id >= first_id {
                let insert = &batch.inserts[usize::try_from(id - first_id).expect("bounded batch")];
                let source =
                    usize::try_from(insert.row_index).map_err(|_| invalid("invalid insert row"))?;
                if source >= row
                    || canonical_row(input.records(), source)?
                        != canonical_row(input.records(), row)?
                {
                    return Err(invalid(
                        "deletion cannot consume a later or different insert",
                    ));
                }
            }
        }
        remaining -= 1;
    }
    if inserts.next().is_some() || deletions.next().is_some() {
        return Err(invalid("prepared mutations do not match the input"));
    }
    Ok(())
}
