use super::invalid;
use crate::operation::OperationError;

const VERSION: u8 = 1;
const BUFFER_BYTES: usize = 4 * size_of::<u64>();
pub(super) const MAX_CONTROL_BYTES: usize = 2 + BUFFER_BYTES;

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

pub(super) enum State {
    Initialize,
    Ready(BufferState),
}

impl State {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut output = vec![VERSION];
        match self {
            Self::Initialize => output.push(0),
            Self::Ready(buffer) => {
                output.push(1);
                encode_buffer(*buffer, &mut output);
            }
        }
        output
    }
}

pub(super) fn decode(mut input: &[u8]) -> Result<State, OperationError> {
    if read::<1>(&mut input)? != [VERSION] {
        return Err(invalid("unknown control-state version"));
    }
    match read::<1>(&mut input)?[0] {
        0 => {
            require_end(input)?;
            Ok(State::Initialize)
        }
        1 => {
            let buffer = decode_buffer(&mut input)?;
            require_end(input)?;
            Ok(State::Ready(buffer))
        }
        _ => Err(invalid("unknown control-state phase")),
    }
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
