use std::collections::HashSet;

use thiserror::Error;

pub(super) use super::super::relation::{
    Continuation, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID, Mutation, MutationKind, Position,
};

const FORMAT_VERSION: u16 = 1;
const INITIALIZE_TAG: u8 = 0;
const PREPARE_TAG: u8 = 1;
const APPLY_TAG: u8 = 2;
const DONE_TAG: u8 = 0;
const POSITION_TAG: u8 = 1;
const INSERT_TAG: u8 = 0;
const DELETE_TAG: u8 = 1;

/// Durable `SQLite` sink continuation stored independently of the Station claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PendingState {
    Initialize,
    Prepare {
        position: Position,
    },
    Apply {
        start_position: Position,
        continuation: Continuation,
        mutations: Vec<Mutation>,
    },
}

/// Failure to encode or decode the `SQLite` sink's durable continuation.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(super) enum PendingStateCodecError {
    #[error("SQLite sink pending state is truncated")]
    Truncated,
    #[error("unsupported SQLite sink pending state format version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown SQLite sink pending state tag {0}")]
    UnknownStateTag(u8),
    #[error("unknown SQLite sink continuation tag {0}")]
    UnknownContinuationTag(u8),
    #[error("unknown SQLite sink mutation kind tag {0}")]
    UnknownMutationKindTag(u8),
    #[error("SQLite sink pending state contains trailing bytes")]
    TrailingBytes,
    #[error("SQLite sink position must have a nonzero remaining multiplicity")]
    ZeroRemaining,
    #[error("SQLite sink Apply state must contain at least one mutation")]
    EmptyMutationBatch,
    #[error("SQLite sink mutation batch contains {0} mutations, exceeding the limit")]
    TooManyMutations(usize),
    #[error("SQLite sink mutation technical ID {0} is outside 1..=i64::MAX")]
    InvalidTechnicalId(u64),
    #[error("SQLite sink Apply state's first mutation is not at its start position")]
    FirstMutationRowMismatch,
    #[error("SQLite sink mutation row indices are not contiguous and nondecreasing")]
    NonContiguousMutationRows,
    #[error("SQLite sink mutations for one input row have different kinds")]
    MixedMutationKinds,
    #[error("SQLite sink insert technical IDs are not consecutive")]
    NonConsecutiveInsertIds,
    #[error("SQLite sink delete technical IDs for one input row are not increasing")]
    NonIncreasingDeleteIds,
    #[error("SQLite sink mutation batch deletes one technical ID more than once")]
    DuplicateDeleteId,
    #[error("SQLite sink Apply state consumes more than the start position's remainder")]
    StartRemainderExceeded,
    #[error("SQLite sink Apply continuation does not follow its mutations canonically")]
    InvalidContinuation,
}

impl PendingState {
    pub(super) fn encode(&self) -> Result<Vec<u8>, PendingStateCodecError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());

        match self {
            Self::Initialize => encoded.push(INITIALIZE_TAG),
            Self::Prepare { position } => {
                encoded.push(PREPARE_TAG);
                encode_position(*position, &mut encoded);
            }
            Self::Apply {
                start_position,
                continuation,
                mutations,
            } => {
                encoded.push(APPLY_TAG);
                encode_position(*start_position, &mut encoded);
                match continuation {
                    Continuation::Done => encoded.push(DONE_TAG),
                    Continuation::Position(position) => {
                        encoded.push(POSITION_TAG);
                        encode_position(*position, &mut encoded);
                    }
                }
                let count = u16::try_from(mutations.len())
                    .expect("the validated mutation limit fits a u16");
                encoded.extend_from_slice(&count.to_be_bytes());
                for mutation in mutations {
                    encoded.push(match mutation.kind {
                        MutationKind::Insert => INSERT_TAG,
                        MutationKind::Delete => DELETE_TAG,
                    });
                    encoded.extend_from_slice(&mutation.row_index.to_be_bytes());
                    encoded.extend_from_slice(&mutation.technical_id.to_be_bytes());
                }
            }
        }
        Ok(encoded)
    }

    pub(super) fn decode(encoded: &[u8]) -> Result<Self, PendingStateCodecError> {
        let mut cursor = Cursor::new(encoded);
        let version = cursor.read_u16()?;
        if version != FORMAT_VERSION {
            return Err(PendingStateCodecError::UnsupportedVersion(version));
        }

        let state = match cursor.read_u8()? {
            INITIALIZE_TAG => Self::Initialize,
            PREPARE_TAG => Self::Prepare {
                position: decode_position(&mut cursor)?,
            },
            APPLY_TAG => {
                let start_position = decode_position(&mut cursor)?;
                let continuation = match cursor.read_u8()? {
                    DONE_TAG => Continuation::Done,
                    POSITION_TAG => Continuation::Position(decode_position(&mut cursor)?),
                    tag => return Err(PendingStateCodecError::UnknownContinuationTag(tag)),
                };
                let count = usize::from(cursor.read_u16()?);
                if count == 0 {
                    return Err(PendingStateCodecError::EmptyMutationBatch);
                }
                if count > MAX_MUTATIONS_PER_BATCH {
                    return Err(PendingStateCodecError::TooManyMutations(count));
                }

                let mut mutations = Vec::with_capacity(count);
                for _ in 0..count {
                    let kind = match cursor.read_u8()? {
                        INSERT_TAG => MutationKind::Insert,
                        DELETE_TAG => MutationKind::Delete,
                        tag => return Err(PendingStateCodecError::UnknownMutationKindTag(tag)),
                    };
                    mutations.push(Mutation {
                        kind,
                        row_index: cursor.read_u64()?,
                        technical_id: cursor.read_u64()?,
                    });
                }
                Self::Apply {
                    start_position,
                    continuation,
                    mutations,
                }
            }
            tag => return Err(PendingStateCodecError::UnknownStateTag(tag)),
        };

        cursor.finish()?;
        state.validate()?;
        Ok(state)
    }

    pub(super) fn validate(&self) -> Result<(), PendingStateCodecError> {
        match self {
            Self::Initialize => Ok(()),
            Self::Prepare { position } => validate_position(*position),
            Self::Apply {
                start_position,
                continuation,
                mutations,
            } => validate_apply(*start_position, *continuation, mutations),
        }
    }
}

fn validate_apply(
    start_position: Position,
    continuation: Continuation,
    mutations: &[Mutation],
) -> Result<(), PendingStateCodecError> {
    validate_position(start_position)?;
    if let Continuation::Position(position) = continuation {
        validate_position(position)?;
    }
    if mutations.is_empty() {
        return Err(PendingStateCodecError::EmptyMutationBatch);
    }
    if mutations.len() > MAX_MUTATIONS_PER_BATCH {
        return Err(PendingStateCodecError::TooManyMutations(mutations.len()));
    }
    if mutations[0].row_index != start_position.row_index {
        return Err(PendingStateCodecError::FirstMutationRowMismatch);
    }

    let mut first_row_count = 0_u64;
    let mut previous: Option<Mutation> = None;
    let mut previous_insert_id: Option<u64> = None;
    let mut delete_ids = HashSet::new();
    for mutation in mutations {
        validate_mutation(*mutation)?;
        if mutation.row_index == start_position.row_index {
            first_row_count += 1;
        }

        if let Some(previous_mutation) = previous {
            if mutation.row_index == previous_mutation.row_index {
                if mutation.kind != previous_mutation.kind {
                    return Err(PendingStateCodecError::MixedMutationKinds);
                }
                if mutation.kind == MutationKind::Delete
                    && mutation.technical_id <= previous_mutation.technical_id
                {
                    return Err(PendingStateCodecError::NonIncreasingDeleteIds);
                }
            } else if previous_mutation
                .row_index
                .checked_add(1)
                .is_none_or(|next| mutation.row_index != next)
            {
                return Err(PendingStateCodecError::NonContiguousMutationRows);
            }
        }

        if mutation.kind == MutationKind::Insert {
            if let Some(previous_id) = previous_insert_id
                && previous_id
                    .checked_add(1)
                    .is_none_or(|next| mutation.technical_id != next)
            {
                return Err(PendingStateCodecError::NonConsecutiveInsertIds);
            }
            previous_insert_id = Some(mutation.technical_id);
        } else if !delete_ids.insert(mutation.technical_id) {
            return Err(PendingStateCodecError::DuplicateDeleteId);
        }
        previous = Some(*mutation);
    }

    if first_row_count > start_position.remaining {
        return Err(PendingStateCodecError::StartRemainderExceeded);
    }

    let last_row = mutations
        .last()
        .expect("the mutation batch was checked as nonempty")
        .row_index;
    match continuation {
        Continuation::Done => {
            if first_row_count < start_position.remaining {
                return Err(PendingStateCodecError::InvalidContinuation);
            }
        }
        Continuation::Position(position) => {
            if mutations.len() != MAX_MUTATIONS_PER_BATCH {
                return Err(PendingStateCodecError::InvalidContinuation);
            }
            if first_row_count < start_position.remaining {
                let expected_remaining = start_position.remaining - first_row_count;
                if last_row != start_position.row_index
                    || position.row_index != start_position.row_index
                    || position.remaining != expected_remaining
                {
                    return Err(PendingStateCodecError::InvalidContinuation);
                }
            } else if (position.row_index != last_row
                && last_row
                    .checked_add(1)
                    .is_none_or(|next| position.row_index != next))
                || position.row_index == start_position.row_index
            {
                return Err(PendingStateCodecError::InvalidContinuation);
            }
        }
    }

    Ok(())
}

const fn validate_position(position: Position) -> Result<(), PendingStateCodecError> {
    if position.remaining == 0 {
        Err(PendingStateCodecError::ZeroRemaining)
    } else {
        Ok(())
    }
}

const fn validate_mutation(mutation: Mutation) -> Result<(), PendingStateCodecError> {
    if mutation.technical_id == 0 || mutation.technical_id > MAX_TECHNICAL_ID {
        Err(PendingStateCodecError::InvalidTechnicalId(
            mutation.technical_id,
        ))
    } else {
        Ok(())
    }
}

fn encode_position(position: Position, output: &mut Vec<u8>) {
    output.extend_from_slice(&position.row_index.to_be_bytes());
    output.extend_from_slice(&position.remaining.to_be_bytes());
}

fn decode_position(cursor: &mut Cursor<'_>) -> Result<Position, PendingStateCodecError> {
    Ok(Position {
        row_index: cursor.read_u64()?,
        remaining: cursor.read_u64()?,
    })
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn read_u8(&mut self) -> Result<u8, PendingStateCodecError> {
        Ok(self.take::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, PendingStateCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn read_u64(&mut self) -> Result<u64, PendingStateCodecError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    const fn finish(self) -> Result<(), PendingStateCodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(PendingStateCodecError::TrailingBytes)
        }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], PendingStateCodecError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(PendingStateCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(*value)
    }
}
