use super::{SinkTarget, invalid};
use crate::operation::OperationError;

#[cfg(test)]
use super::DeliveryBatch;

const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Position {
    pub(super) sequence: u64,
    pub(super) row_index: u64,
    /// Zero denotes an entry boundary whose first diff has not been decoded.
    pub(super) remaining: u64,
}

impl Position {
    pub(super) const fn entry_start(sequence: u64) -> Self {
        Self {
            sequence,
            row_index: 0,
            remaining: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BufferState {
    pub(super) head: Option<Position>,
    pub(super) tail: u64,
    pub(super) pending_events: u64,
    pub(super) retained_bytes: u64,
}

impl BufferState {
    pub(super) const EMPTY: Self = Self {
        head: None,
        tail: 0,
        pending_events: 0,
        retained_bytes: 0,
    };

    pub(super) const fn is_empty(self) -> bool {
        self.head.is_none()
    }

    pub(super) fn validate(self) -> Result<(), OperationError> {
        match self.head {
            None if self == Self::EMPTY => Ok(()),
            Some(head)
                if head.sequence < self.tail
                    && (head.remaining != 0 || head.row_index == 0)
                    && self.pending_events != 0
                    && self.pending_events >= head.remaining
                    && self.tail - head.sequence <= self.pending_events
                    && self.retained_bytes >= size_of::<u64>() as u64 =>
            {
                Ok(())
            }
            None | Some(_) => Err(invalid("invalid buffer control state")),
        }
    }
}

pub(super) const fn has_batch_id_capacity(buffer: BufferState, next_batch_id: u64) -> bool {
    next_batch_id != 0 && buffer.pending_events <= u64::MAX - next_batch_id
}

fn validate_batch_id_capacity(
    buffer: BufferState,
    next_batch_id: u64,
) -> Result<(), OperationError> {
    if has_batch_id_capacity(buffer, next_batch_id) {
        Ok(())
    } else {
        Err(invalid(
            "buffered input cannot drain before the batch ID range is exhausted",
        ))
    }
}

#[derive(Clone)]
pub(super) struct Ready<C> {
    pub(super) buffer: BufferState,
    pub(super) next_batch_id: u64,
    pub(super) checkpoint: C,
}

#[derive(Clone)]
pub(super) struct Prepared<C, P> {
    pub(super) before: BufferState,
    pub(super) after: BufferState,
    pub(super) batch_id: u64,
    pub(super) checkpoint: C,
    pub(super) plan: P,
}

#[derive(Clone)]
pub(super) enum State<C, P> {
    Initialize,
    Ready(Ready<C>),
    Prepared(Prepared<C, P>),
}

pub(super) enum Header<'input, C> {
    Initialize,
    Ready(Ready<C>),
    Prepared {
        before: BufferState,
        after: BufferState,
        batch_id: u64,
        checkpoint: C,
        encoded_plan: &'input [u8],
    },
}

impl<C, P> State<C, P> {
    pub(super) fn validate(&self) -> Result<(), OperationError> {
        match self {
            Self::Initialize => Ok(()),
            Self::Ready(ready) => {
                ready.buffer.validate()?;
                if ready.next_batch_id == 0 {
                    Err(invalid("next batch ID is zero"))
                } else {
                    validate_batch_id_capacity(ready.buffer, ready.next_batch_id)
                }
            }
            Self::Prepared(prepared) => {
                validate_settlement(prepared.before, prepared.after)?;
                if prepared.batch_id == 0 || prepared.batch_id == u64::MAX {
                    Err(invalid("prepared batch ID is outside 1..u64::MAX"))
                } else {
                    validate_batch_id_capacity(prepared.after, prepared.batch_id + 1)
                }
            }
        }
    }

    pub(super) fn encode<T>(&self) -> Vec<u8>
    where
        T: SinkTarget<Checkpoint = C, Plan = P>,
    {
        let mut output = vec![VERSION];
        match self {
            Self::Initialize => output.push(0),
            Self::Ready(ready) => {
                output.push(1);
                encode_ready::<T>(ready, &mut output);
            }
            Self::Prepared(prepared) => {
                output.push(2);
                encode_buffer(prepared.before, &mut output);
                encode_buffer(prepared.after, &mut output);
                output.extend(prepared.batch_id.to_be_bytes());
                T::encode_checkpoint(&prepared.checkpoint, &mut output);
                T::encode_plan(&prepared.plan, &mut output);
            }
        }
        output
    }

    #[cfg(test)]
    pub(super) fn decode<T>(
        input: &[u8],
        prepared_input: Option<&DeliveryBatch>,
    ) -> Result<Self, OperationError>
    where
        T: SinkTarget<Checkpoint = C, Plan = P>,
    {
        match decode_header::<T>(input)? {
            Header::Initialize => Ok(Self::Initialize),
            Header::Ready(ready) => Ok(Self::Ready(ready)),
            Header::Prepared {
                before,
                after,
                batch_id,
                checkpoint,
                mut encoded_plan,
            } => {
                let change = prepared_input
                    .ok_or_else(|| invalid("prepared input is required to decode its plan"))?;
                let plan = T::decode_plan(&mut encoded_plan, change, &checkpoint)?;
                if !encoded_plan.is_empty() {
                    return Err(invalid("trailing control-state bytes"));
                }
                Ok(Self::Prepared(Prepared {
                    before,
                    after,
                    batch_id,
                    checkpoint,
                    plan,
                }))
            }
        }
    }
}

pub(super) fn decode_header<'input, T>(
    mut input: &'input [u8],
) -> Result<Header<'input, T::Checkpoint>, OperationError>
where
    T: SinkTarget,
{
    if read::<1>(&mut input)? != [VERSION] {
        return Err(invalid("unknown control-state version"));
    }
    match read::<1>(&mut input)?[0] {
        0 => {
            require_end(input)?;
            Ok(Header::Initialize)
        }
        1 => {
            let ready = decode_ready::<T>(&mut input)?;
            require_end(input)?;
            Ok(Header::Ready(ready))
        }
        2 => {
            let before = decode_buffer(&mut input)?;
            let after = decode_buffer(&mut input)?;
            validate_settlement(before, after)?;
            let batch_id = u64::from_be_bytes(read(&mut input)?);
            if batch_id == 0 || batch_id == u64::MAX {
                return Err(invalid("prepared batch ID is outside 1..u64::MAX"));
            }
            validate_batch_id_capacity(after, batch_id + 1)?;
            let checkpoint = T::decode_checkpoint(&mut input)?;
            Ok(Header::Prepared {
                before,
                after,
                batch_id,
                checkpoint,
                encoded_plan: input,
            })
        }
        _ => Err(invalid("unknown control-state phase")),
    }
}

fn encode_ready<T: SinkTarget>(ready: &Ready<T::Checkpoint>, output: &mut Vec<u8>) {
    encode_buffer(ready.buffer, output);
    output.extend(ready.next_batch_id.to_be_bytes());
    T::encode_checkpoint(&ready.checkpoint, output);
}

fn decode_ready<T: SinkTarget>(input: &mut &[u8]) -> Result<Ready<T::Checkpoint>, OperationError> {
    let buffer = decode_buffer(input)?;
    let next_batch_id = u64::from_be_bytes(read(input)?);
    if next_batch_id == 0 {
        return Err(invalid("next batch ID is zero"));
    }
    validate_batch_id_capacity(buffer, next_batch_id)?;
    let checkpoint = T::decode_checkpoint(input)?;
    Ok(Ready {
        buffer,
        next_batch_id,
        checkpoint,
    })
}

fn encode_buffer(buffer: BufferState, output: &mut Vec<u8>) {
    match buffer.head {
        None => output.push(0),
        Some(head) => {
            output.push(1);
            output.extend(head.sequence.to_be_bytes());
            output.extend(head.row_index.to_be_bytes());
            output.extend(head.remaining.to_be_bytes());
        }
    }
    output.extend(buffer.tail.to_be_bytes());
    output.extend(buffer.pending_events.to_be_bytes());
    output.extend(buffer.retained_bytes.to_be_bytes());
}

fn decode_buffer(input: &mut &[u8]) -> Result<BufferState, OperationError> {
    let head = match read::<1>(input)?[0] {
        0 => None,
        1 => Some(Position {
            sequence: u64::from_be_bytes(read(input)?),
            row_index: u64::from_be_bytes(read(input)?),
            remaining: u64::from_be_bytes(read(input)?),
        }),
        _ => return Err(invalid("invalid buffer-head tag")),
    };
    let state = BufferState {
        head,
        tail: u64::from_be_bytes(read(input)?),
        pending_events: u64::from_be_bytes(read(input)?),
        retained_bytes: u64::from_be_bytes(read(input)?),
    };
    state.validate()?;
    Ok(state)
}

fn validate_settlement(before: BufferState, after: BufferState) -> Result<(), OperationError> {
    before.validate()?;
    after.validate()?;
    let heads_progress = match (before.head, after.head) {
        (Some(_), None) => after == BufferState::EMPTY,
        (Some(start), Some(end)) => {
            before.tail == after.tail
                && (end.sequence > start.sequence
                    || (end.sequence == start.sequence
                        && (end.row_index > start.row_index
                            || (end.row_index == start.row_index
                                && if start.remaining == 0 {
                                    end.remaining != 0
                                } else {
                                    end.remaining < start.remaining
                                }))))
        }
        (None, _) => false,
    };
    if before.is_empty()
        || !heads_progress
        || after.pending_events >= before.pending_events
        || after.retained_bytes > before.retained_bytes
    {
        return Err(invalid("invalid prepared settlement"));
    }
    Ok(())
}

fn require_end(input: &[u8]) -> Result<(), OperationError> {
    if input.is_empty() {
        Ok(())
    } else {
        Err(invalid("trailing control-state bytes"))
    }
}

pub(super) fn read<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], OperationError> {
    let (value, rest) = input
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated control state"))?;
    *input = rest;
    Ok(value.try_into().expect("split has the requested length"))
}
