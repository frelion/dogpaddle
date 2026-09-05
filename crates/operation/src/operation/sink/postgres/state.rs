use std::collections::HashSet;

use thiserror::Error;

use super::super::relation::{
    Continuation, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID, Mutation, MutationKind, Position,
};

const FORMAT_VERSION: u16 = 1;
const INITIALIZE_TAG: u8 = 0;
const READY_TAG: u8 = 1;
const PREPARED_TAG: u8 = 2;
const NONE_TAG: u8 = 0;
const SOME_TAG: u8 = 1;
const DONE_TAG: u8 = 0;
const POSITION_TAG: u8 = 1;
const INSERT_TAG: u8 = 0;
const DELETE_TAG: u8 = 1;
const DIGEST_LENGTH: usize = 32;
const EXHAUSTED_SEQUENCE: u64 = MAX_TECHNICAL_ID + 1;

/// Durable state of one materialized `PostgreSQL` relation sink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PostgresSinkState {
    /// Target creation is durably intended but not yet locally settled.
    Initialize,
    /// The target is ready to prepare the next bounded delivery.
    Ready {
        /// Next unallocated target receipt sequence.
        next_delivery: u64,
        /// Next unallocated physical-row identity.
        next_id: u64,
        /// Remaining position in the Station's retained Change, if any.
        position: Option<Position>,
    },
    /// One immutable delivery that may be submitted or replayed externally.
    Prepared {
        /// Positive target receipt sequence for this delivery.
        delivery: u64,
        /// Digest of the exact ordered target mutation payload.
        digest: [u8; DIGEST_LENGTH],
        /// Physical-row identity frontier before this delivery's inserts.
        next_id_before: u64,
        /// Position at which this delivery begins.
        start_position: Position,
        /// Position to persist after this delivery is settled.
        continuation: Continuation,
        /// Exact ordered physical mutations in this delivery.
        mutations: Vec<Mutation>,
    },
}

/// Failure to encode or decode durable `PostgreSQL` sink state.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PostgresSinkStateCodecError {
    #[error("PostgreSQL sink state is truncated")]
    Truncated,
    #[error("unsupported PostgreSQL sink state format version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown PostgreSQL sink state tag {0}")]
    UnknownStateTag(u8),
    #[error("unknown PostgreSQL sink position tag {0}")]
    UnknownPositionTag(u8),
    #[error("unknown PostgreSQL sink continuation tag {0}")]
    UnknownContinuationTag(u8),
    #[error("unknown PostgreSQL sink mutation kind tag {0}")]
    UnknownMutationKindTag(u8),
    #[error("PostgreSQL sink state contains trailing bytes")]
    TrailingBytes,
    #[error("PostgreSQL sink next delivery {0} is outside 1..=i64::MAX+1")]
    InvalidNextDelivery(u64),
    #[error("PostgreSQL sink delivery {0} is outside 1..=i64::MAX")]
    InvalidDelivery(u64),
    #[error("PostgreSQL sink next technical ID {0} is outside 1..=i64::MAX+1")]
    InvalidNextId(u64),
    #[error("PostgreSQL sink position must have a nonzero remaining multiplicity")]
    ZeroRemaining,
    #[error("PostgreSQL sink Prepared state must contain at least one mutation")]
    EmptyMutationBatch,
    #[error("PostgreSQL sink mutation batch contains {0} mutations, exceeding the limit")]
    TooManyMutations(usize),
    #[error("PostgreSQL sink mutation technical ID {0} is outside 1..=i64::MAX")]
    InvalidTechnicalId(u64),
    #[error("PostgreSQL sink Prepared state's first mutation is not at its start position")]
    FirstMutationRowMismatch,
    #[error("PostgreSQL sink mutation row indices are not contiguous and nondecreasing")]
    NonContiguousMutationRows,
    #[error("PostgreSQL sink mutations for one input row have different kinds")]
    MixedMutationKinds,
    #[error("PostgreSQL sink insert technical IDs do not begin at next_id_before")]
    InsertRangeStartMismatch,
    #[error("PostgreSQL sink insert technical IDs are not consecutive")]
    NonConsecutiveInsertIds,
    #[error("PostgreSQL sink insert technical-ID range exceeds i64::MAX")]
    InsertRangeOverflow,
    #[error("PostgreSQL sink delete technical IDs for one input row are not increasing")]
    NonIncreasingDeleteIds,
    #[error("PostgreSQL sink mutation batch deletes one technical ID more than once")]
    DuplicateDeleteId,
    #[error("PostgreSQL sink mutation references a technical ID not yet allocated")]
    UnallocatedTechnicalId,
    #[error("PostgreSQL sink mutation deletes a newly allocated ID before inserting it")]
    DeleteBeforeInsert,
    #[error("PostgreSQL sink Prepared state consumes more than the start position's remainder")]
    StartRemainderExceeded,
    #[error("PostgreSQL sink Prepared continuation does not follow its mutations canonically")]
    InvalidContinuation,
}

impl PostgresSinkState {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, PostgresSinkStateCodecError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());

        match self {
            Self::Initialize => encoded.push(INITIALIZE_TAG),
            Self::Ready {
                next_delivery,
                next_id,
                position,
            } => {
                encoded.push(READY_TAG);
                encoded.extend_from_slice(&next_delivery.to_be_bytes());
                encoded.extend_from_slice(&next_id.to_be_bytes());
                encode_optional_position(*position, &mut encoded);
            }
            Self::Prepared {
                delivery,
                digest,
                next_id_before,
                start_position,
                continuation,
                mutations,
            } => {
                encoded.push(PREPARED_TAG);
                encoded.extend_from_slice(&delivery.to_be_bytes());
                encoded.extend_from_slice(digest);
                encoded.extend_from_slice(&next_id_before.to_be_bytes());
                encode_position(*start_position, &mut encoded);
                encode_continuation(*continuation, &mut encoded);
                let count =
                    u16::try_from(mutations.len()).expect("the validated mutation limit fits u16");
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

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, PostgresSinkStateCodecError> {
        let mut cursor = Cursor::new(encoded);
        let version = cursor.read_u16()?;
        if version != FORMAT_VERSION {
            return Err(PostgresSinkStateCodecError::UnsupportedVersion(version));
        }

        let state = match cursor.read_u8()? {
            INITIALIZE_TAG => Self::Initialize,
            READY_TAG => Self::Ready {
                next_delivery: cursor.read_u64()?,
                next_id: cursor.read_u64()?,
                position: decode_optional_position(&mut cursor)?,
            },
            PREPARED_TAG => {
                let delivery = cursor.read_u64()?;
                let digest = cursor.take::<DIGEST_LENGTH>()?;
                let next_id_before = cursor.read_u64()?;
                let start_position = decode_position(&mut cursor)?;
                let continuation = decode_continuation(&mut cursor)?;
                let count = usize::from(cursor.read_u16()?);
                if count == 0 {
                    return Err(PostgresSinkStateCodecError::EmptyMutationBatch);
                }
                if count > MAX_MUTATIONS_PER_BATCH {
                    return Err(PostgresSinkStateCodecError::TooManyMutations(count));
                }
                let mut mutations = Vec::with_capacity(count);
                for _ in 0..count {
                    let kind = match cursor.read_u8()? {
                        INSERT_TAG => MutationKind::Insert,
                        DELETE_TAG => MutationKind::Delete,
                        tag => {
                            return Err(PostgresSinkStateCodecError::UnknownMutationKindTag(tag));
                        }
                    };
                    mutations.push(Mutation {
                        kind,
                        row_index: cursor.read_u64()?,
                        technical_id: cursor.read_u64()?,
                    });
                }
                Self::Prepared {
                    delivery,
                    digest,
                    next_id_before,
                    start_position,
                    continuation,
                    mutations,
                }
            }
            tag => return Err(PostgresSinkStateCodecError::UnknownStateTag(tag)),
        };

        cursor.finish()?;
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn validate(&self) -> Result<(), PostgresSinkStateCodecError> {
        match self {
            Self::Initialize => Ok(()),
            Self::Ready {
                next_delivery,
                next_id,
                position,
            } => {
                validate_next_delivery(*next_delivery)?;
                validate_next_id(*next_id)?;
                if let Some(position) = position {
                    validate_position(*position)?;
                }
                Ok(())
            }
            Self::Prepared {
                delivery,
                next_id_before,
                start_position,
                continuation,
                mutations,
                ..
            } => validate_prepared(
                *delivery,
                *next_id_before,
                *start_position,
                *continuation,
                mutations,
            ),
        }
    }

    /// Returns the allocation frontiers after this prepared delivery settles.
    pub(crate) fn settled_frontiers(&self) -> Option<(u64, u64)> {
        let Self::Prepared {
            delivery,
            next_id_before,
            mutations,
            ..
        } = self
        else {
            return None;
        };
        let insert_count = mutations
            .iter()
            .filter(|mutation| mutation.kind == MutationKind::Insert)
            .count();
        let insert_count =
            u64::try_from(insert_count).expect("the validated mutation count fits u64");
        Some((
            delivery
                .checked_add(1)
                .expect("a valid delivery has a successor sentinel"),
            next_id_before
                .checked_add(insert_count)
                .expect("a validated insert range has a successor frontier"),
        ))
    }
}

fn validate_prepared(
    delivery: u64,
    next_id_before: u64,
    start_position: Position,
    continuation: Continuation,
    mutations: &[Mutation],
) -> Result<(), PostgresSinkStateCodecError> {
    validate_delivery(delivery)?;
    validate_next_id(next_id_before)?;
    validate_position(start_position)?;
    if let Continuation::Position(position) = continuation {
        validate_position(position)?;
    }
    if mutations.is_empty() {
        return Err(PostgresSinkStateCodecError::EmptyMutationBatch);
    }
    if mutations.len() > MAX_MUTATIONS_PER_BATCH {
        return Err(PostgresSinkStateCodecError::TooManyMutations(
            mutations.len(),
        ));
    }

    validate_mutation_sequence(next_id_before, start_position, mutations)?;
    validate_continuation(start_position, continuation, mutations)
}

fn validate_mutation_sequence(
    next_id_before: u64,
    start_position: Position,
    mutations: &[Mutation],
) -> Result<(), PostgresSinkStateCodecError> {
    if mutations[0].row_index != start_position.row_index {
        return Err(PostgresSinkStateCodecError::FirstMutationRowMismatch);
    }
    let insert_count = u64::try_from(
        mutations
            .iter()
            .filter(|mutation| mutation.kind == MutationKind::Insert)
            .count(),
    )
    .expect("the bounded mutation count fits u64");
    let next_id_after = next_id_before
        .checked_add(insert_count)
        .filter(|next| *next <= EXHAUSTED_SEQUENCE)
        .ok_or(PostgresSinkStateCodecError::InsertRangeOverflow)?;

    let mut expected_insert_id = next_id_before;
    let mut previous: Option<Mutation> = None;
    let mut delete_ids = HashSet::new();
    for mutation in mutations {
        validate_technical_id(mutation.technical_id)?;
        if mutation.technical_id >= next_id_after {
            return Err(PostgresSinkStateCodecError::UnallocatedTechnicalId);
        }
        if let Some(previous_mutation) = previous {
            validate_row_order(previous_mutation, *mutation)?;
        }

        match mutation.kind {
            MutationKind::Insert => {
                if mutation.technical_id != expected_insert_id {
                    return Err(if expected_insert_id == next_id_before {
                        PostgresSinkStateCodecError::InsertRangeStartMismatch
                    } else {
                        PostgresSinkStateCodecError::NonConsecutiveInsertIds
                    });
                }
                expected_insert_id = expected_insert_id
                    .checked_add(1)
                    .expect("the validated insert range has a successor frontier");
            }
            MutationKind::Delete => {
                if !delete_ids.insert(mutation.technical_id) {
                    return Err(PostgresSinkStateCodecError::DuplicateDeleteId);
                }
                if mutation.technical_id >= next_id_before
                    && mutation.technical_id >= expected_insert_id
                {
                    return Err(PostgresSinkStateCodecError::DeleteBeforeInsert);
                }
            }
        }
        previous = Some(*mutation);
    }
    debug_assert_eq!(expected_insert_id, next_id_after);
    Ok(())
}

fn validate_row_order(
    previous: Mutation,
    mutation: Mutation,
) -> Result<(), PostgresSinkStateCodecError> {
    if mutation.row_index == previous.row_index {
        if mutation.kind != previous.kind {
            return Err(PostgresSinkStateCodecError::MixedMutationKinds);
        }
        if mutation.kind == MutationKind::Delete && mutation.technical_id <= previous.technical_id {
            return Err(PostgresSinkStateCodecError::NonIncreasingDeleteIds);
        }
    } else if previous
        .row_index
        .checked_add(1)
        .is_none_or(|next| mutation.row_index != next)
    {
        return Err(PostgresSinkStateCodecError::NonContiguousMutationRows);
    }
    Ok(())
}

fn validate_continuation(
    start_position: Position,
    continuation: Continuation,
    mutations: &[Mutation],
) -> Result<(), PostgresSinkStateCodecError> {
    let first_row_count = u64::try_from(
        mutations
            .iter()
            .take_while(|mutation| mutation.row_index == start_position.row_index)
            .count(),
    )
    .expect("the bounded mutation count fits u64");
    if first_row_count > start_position.remaining {
        return Err(PostgresSinkStateCodecError::StartRemainderExceeded);
    }

    let last_row = mutations
        .last()
        .expect("the mutation batch was checked as nonempty")
        .row_index;
    match continuation {
        Continuation::Done => {
            if first_row_count < start_position.remaining {
                return Err(PostgresSinkStateCodecError::InvalidContinuation);
            }
        }
        Continuation::Position(position) => {
            if mutations.len() != MAX_MUTATIONS_PER_BATCH {
                return Err(PostgresSinkStateCodecError::InvalidContinuation);
            }
            if first_row_count < start_position.remaining {
                let expected_remaining = start_position.remaining - first_row_count;
                if last_row != start_position.row_index
                    || position.row_index != start_position.row_index
                    || position.remaining != expected_remaining
                {
                    return Err(PostgresSinkStateCodecError::InvalidContinuation);
                }
            } else if (position.row_index != last_row
                && last_row
                    .checked_add(1)
                    .is_none_or(|next| position.row_index != next))
                || position.row_index == start_position.row_index
            {
                return Err(PostgresSinkStateCodecError::InvalidContinuation);
            }
        }
    }
    Ok(())
}

const fn validate_next_delivery(next_delivery: u64) -> Result<(), PostgresSinkStateCodecError> {
    if next_delivery == 0 || next_delivery > EXHAUSTED_SEQUENCE {
        Err(PostgresSinkStateCodecError::InvalidNextDelivery(
            next_delivery,
        ))
    } else {
        Ok(())
    }
}

const fn validate_delivery(delivery: u64) -> Result<(), PostgresSinkStateCodecError> {
    if delivery == 0 || delivery > MAX_TECHNICAL_ID {
        Err(PostgresSinkStateCodecError::InvalidDelivery(delivery))
    } else {
        Ok(())
    }
}

const fn validate_next_id(next_id: u64) -> Result<(), PostgresSinkStateCodecError> {
    if next_id == 0 || next_id > EXHAUSTED_SEQUENCE {
        Err(PostgresSinkStateCodecError::InvalidNextId(next_id))
    } else {
        Ok(())
    }
}

const fn validate_position(position: Position) -> Result<(), PostgresSinkStateCodecError> {
    if position.remaining == 0 {
        Err(PostgresSinkStateCodecError::ZeroRemaining)
    } else {
        Ok(())
    }
}

const fn validate_technical_id(technical_id: u64) -> Result<(), PostgresSinkStateCodecError> {
    if technical_id == 0 || technical_id > MAX_TECHNICAL_ID {
        Err(PostgresSinkStateCodecError::InvalidTechnicalId(
            technical_id,
        ))
    } else {
        Ok(())
    }
}

fn encode_optional_position(position: Option<Position>, output: &mut Vec<u8>) {
    match position {
        None => output.push(NONE_TAG),
        Some(position) => {
            output.push(SOME_TAG);
            encode_position(position, output);
        }
    }
}

fn decode_optional_position(
    cursor: &mut Cursor<'_>,
) -> Result<Option<Position>, PostgresSinkStateCodecError> {
    match cursor.read_u8()? {
        NONE_TAG => Ok(None),
        SOME_TAG => Ok(Some(decode_position(cursor)?)),
        tag => Err(PostgresSinkStateCodecError::UnknownPositionTag(tag)),
    }
}

fn encode_continuation(continuation: Continuation, output: &mut Vec<u8>) {
    match continuation {
        Continuation::Done => output.push(DONE_TAG),
        Continuation::Position(position) => {
            output.push(POSITION_TAG);
            encode_position(position, output);
        }
    }
}

fn decode_continuation(
    cursor: &mut Cursor<'_>,
) -> Result<Continuation, PostgresSinkStateCodecError> {
    match cursor.read_u8()? {
        DONE_TAG => Ok(Continuation::Done),
        POSITION_TAG => Ok(Continuation::Position(decode_position(cursor)?)),
        tag => Err(PostgresSinkStateCodecError::UnknownContinuationTag(tag)),
    }
}

fn encode_position(position: Position, output: &mut Vec<u8>) {
    output.extend_from_slice(&position.row_index.to_be_bytes());
    output.extend_from_slice(&position.remaining.to_be_bytes());
}

fn decode_position(cursor: &mut Cursor<'_>) -> Result<Position, PostgresSinkStateCodecError> {
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

    fn read_u8(&mut self) -> Result<u8, PostgresSinkStateCodecError> {
        Ok(self.take::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, PostgresSinkStateCodecError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn read_u64(&mut self) -> Result<u64, PostgresSinkStateCodecError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    const fn finish(self) -> Result<(), PostgresSinkStateCodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(PostgresSinkStateCodecError::TrailingBytes)
        }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], PostgresSinkStateCodecError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or(PostgresSinkStateCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(*value)
    }
}
