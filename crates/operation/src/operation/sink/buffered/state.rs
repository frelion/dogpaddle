use super::invalid;
use crate::operation::OperationError;
use crate::operation::sink::relation::MAX_MUTATIONS_PER_BATCH;

const VERSION: u8 = 1;
const BUFFER_BYTES: usize = 4 * size_of::<u64>();
pub(super) const MAX_READY_BYTES: usize = 2 + BUFFER_BYTES;
pub(super) const MAX_CONTROL_BYTES: usize =
    2 + 2 * BUFFER_BYTES + size_of::<u16>() + MAX_MUTATIONS_PER_BATCH * size_of::<u64>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Position {
    pub(super) entry_start: u64,
    pub(super) event_offset: u64,
}

impl Position {
    pub(super) const fn entry_start(event_offset: u64) -> Self {
        Self {
            entry_start: event_offset,
            event_offset,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BufferState {
    pub(super) head: Position,
    pub(super) tail: u64,
    pub(super) retained_bytes: u64,
}

impl BufferState {
    pub(super) const EMPTY: Self = Self {
        head: Position::entry_start(1),
        tail: 1,
        retained_bytes: 0,
    };

    pub(super) const fn is_empty(self) -> bool {
        self.head.event_offset == self.tail
    }

    pub(super) const fn pending_events(self) -> u64 {
        self.tail - self.head.event_offset
    }

    pub(super) fn validate(self) -> Result<(), OperationError> {
        if self.head.entry_start == 0
            || self.head.entry_start > self.head.event_offset
            || self.head.event_offset > self.tail
            || if self.is_empty() {
                self.head.entry_start != self.tail || self.retained_bytes != 0
            } else {
                self.retained_bytes < size_of::<u64>() as u64
            }
        {
            return Err(invalid("invalid buffer control state"));
        }
        Ok(())
    }
}

pub(super) struct Prepared {
    pub(super) before: BufferState,
    pub(super) after: BufferState,
    pub(super) negative_ids: Vec<u64>,
}

pub(super) enum State {
    Initialize,
    Ready(BufferState),
    Prepared(Prepared),
}

pub(super) enum Header<'input> {
    Initialize,
    Ready(BufferState),
    Prepared {
        before: BufferState,
        after: BufferState,
        encoded_negative_ids: &'input [u8],
    },
}

impl State {
    pub(super) fn validate(&self) -> Result<(), OperationError> {
        match self {
            Self::Initialize => Ok(()),
            Self::Ready(buffer) => buffer.validate(),
            Self::Prepared(prepared) => {
                validate_settlement(prepared.before, prepared.after)?;
                if prepared.negative_ids.len() > MAX_MUTATIONS_PER_BATCH
                    || prepared.negative_ids.len() as u64
                        > prepared.after.head.event_offset - prepared.before.head.event_offset
                    || prepared
                        .negative_ids
                        .iter()
                        .any(|id| *id == 0 || *id == u64::MAX)
                {
                    return Err(invalid("invalid prepared negative IDs"));
                }
                Ok(())
            }
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut output = vec![VERSION];
        match self {
            Self::Initialize => output.push(0),
            Self::Ready(buffer) => {
                output.push(1);
                encode_buffer(*buffer, &mut output);
            }
            Self::Prepared(prepared) => {
                output.push(2);
                encode_buffer(prepared.before, &mut output);
                encode_buffer(prepared.after, &mut output);
                output.extend(
                    u16::try_from(prepared.negative_ids.len())
                        .expect("negative IDs are bounded")
                        .to_be_bytes(),
                );
                for id in &prepared.negative_ids {
                    output.extend(id.to_be_bytes());
                }
            }
        }
        output
    }
}

pub(super) fn decode_header(mut input: &[u8]) -> Result<Header<'_>, OperationError> {
    if read::<1>(&mut input)? != [VERSION] {
        return Err(invalid("unknown control-state version"));
    }
    match read::<1>(&mut input)?[0] {
        0 => {
            require_end(input)?;
            Ok(Header::Initialize)
        }
        1 => {
            let buffer = decode_buffer(&mut input)?;
            require_end(input)?;
            Ok(Header::Ready(buffer))
        }
        2 => {
            let before = decode_buffer(&mut input)?;
            let after = decode_buffer(&mut input)?;
            validate_settlement(before, after)?;
            Ok(Header::Prepared {
                before,
                after,
                encoded_negative_ids: input,
            })
        }
        _ => Err(invalid("unknown control-state phase")),
    }
}

pub(super) fn decode_negative_ids(mut input: &[u8]) -> Result<Vec<u64>, OperationError> {
    let count = usize::from(u16::from_be_bytes(read(&mut input)?));
    if count > MAX_MUTATIONS_PER_BATCH || input.len() != count * size_of::<u64>() {
        return Err(invalid("invalid prepared negative-ID count"));
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let id = u64::from_be_bytes(read(&mut input)?);
        if id == 0 || id == u64::MAX {
            return Err(invalid("prepared negative ID is outside the event domain"));
        }
        ids.push(id);
    }
    Ok(ids)
}

fn encode_buffer(buffer: BufferState, output: &mut Vec<u8>) {
    output.extend(buffer.head.entry_start.to_be_bytes());
    output.extend(buffer.head.event_offset.to_be_bytes());
    output.extend(buffer.tail.to_be_bytes());
    output.extend(buffer.retained_bytes.to_be_bytes());
}

fn decode_buffer(input: &mut &[u8]) -> Result<BufferState, OperationError> {
    let buffer = BufferState {
        head: Position {
            entry_start: u64::from_be_bytes(read(input)?),
            event_offset: u64::from_be_bytes(read(input)?),
        },
        tail: u64::from_be_bytes(read(input)?),
        retained_bytes: u64::from_be_bytes(read(input)?),
    };
    buffer.validate()?;
    Ok(buffer)
}

fn validate_settlement(before: BufferState, after: BufferState) -> Result<(), OperationError> {
    before.validate()?;
    after.validate()?;
    if before.is_empty()
        || before.tail != after.tail
        || after.head.event_offset <= before.head.event_offset
        || after.head.entry_start < before.head.entry_start
        || after.retained_bytes > before.retained_bytes
        || (after.head.entry_start == before.head.entry_start
            && after.retained_bytes != before.retained_bytes)
        || (after.head.entry_start > before.head.entry_start
            && after.retained_bytes >= before.retained_bytes)
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

fn read<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], OperationError> {
    let (value, rest) = input
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated control state"))?;
    *input = rest;
    Ok(value.try_into().expect("split has the requested length"))
}
