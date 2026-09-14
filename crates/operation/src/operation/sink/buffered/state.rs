use dogpaddle_change::Change;

use super::{SinkTarget, invalid};
use crate::operation::OperationError;

const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Position {
    pub(super) sequence: u64,
    pub(super) row_index: u64,
    pub(super) remaining: u64,
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
            None if self.pending_events == 0 && self.retained_bytes == 0 => Ok(()),
            Some(head)
                if head.sequence < self.tail
                    && head.remaining != 0
                    && self.pending_events >= head.remaining
                    && self.retained_bytes >= size_of::<u64>() as u64 =>
            {
                Ok(())
            }
            None | Some(_) => Err(invalid("invalid buffer control state")),
        }
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

impl<C, P> State<C, P> {
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

    pub(super) fn decode<T>(
        mut input: &[u8],
        prepared_input: Option<&Change>,
    ) -> Result<Self, OperationError>
    where
        T: SinkTarget<Checkpoint = C, Plan = P>,
    {
        if read::<1>(&mut input)? != [VERSION] {
            return Err(invalid("unknown control-state version"));
        }
        let state = match read::<1>(&mut input)?[0] {
            0 => Self::Initialize,
            1 => Self::Ready(decode_ready::<T>(&mut input)?),
            2 => {
                let before = decode_buffer(&mut input)?;
                let after = decode_buffer(&mut input)?;
                validate_settlement(before, after)?;
                let batch_id = u64::from_be_bytes(read(&mut input)?);
                if batch_id == u64::MAX {
                    return Err(invalid("prepared batch ID is exhausted"));
                }
                let checkpoint = T::decode_checkpoint(&mut input)?;
                let change = prepared_input
                    .ok_or_else(|| invalid("prepared input is required to decode its plan"))?;
                let plan = T::decode_plan(&mut input, change, &checkpoint)?;
                Self::Prepared(Prepared {
                    before,
                    after,
                    batch_id,
                    checkpoint,
                    plan,
                })
            }
            _ => return Err(invalid("unknown control-state phase")),
        };
        if !input.is_empty() {
            return Err(invalid("trailing control-state bytes"));
        }
        Ok(state)
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
    if next_batch_id == u64::MAX {
        return Err(invalid("next batch ID is exhausted"));
    }
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
    if before.is_empty()
        || before.tail != after.tail
        || after.pending_events >= before.pending_events
        || after.retained_bytes > before.retained_bytes
        || match (before.head, after.head) {
            (Some(_), None) => after.pending_events != 0 || after.retained_bytes != 0,
            (Some(start), Some(end)) => end.sequence < start.sequence,
            (None, _) => true,
        }
    {
        return Err(invalid("invalid prepared settlement"));
    }
    Ok(())
}

pub(super) fn read<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], OperationError> {
    let (value, rest) = input
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated control state"))?;
    *input = rest;
    Ok(value.try_into().expect("split has the requested length"))
}
