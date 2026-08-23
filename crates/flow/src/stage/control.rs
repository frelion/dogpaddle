use std::io::{Cursor, Read};

use dogpaddle_store::{CellAccess, CodecError, StoreError, StoreValue};

use super::{MAX_CHECKPOINT_BYTES, MAX_FAILURE_BYTES};
use crate::FlowError;

const MARKER: &[u8] = b"dogpaddle.stage";

#[derive(Clone, Debug)]
pub(super) struct Active {
    pub(super) kind: ActiveKind,
    pub(super) port: u32,
    pub(super) position: u64,
    pub(super) checkpoint: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ActiveKind {
    Start,
    Data,
    End,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct InputCursor {
    pub(super) position: u64,
    pub(super) ended: bool,
}

#[derive(Clone, Debug)]
pub(super) struct Control {
    pub(super) lifecycle: Lifecycle,
    pub(super) active: Option<Active>,
    pub(super) cursors: Vec<InputCursor>,
    pub(super) next_input: u32,
    pub(super) output_tail: u64,
    pub(super) failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Lifecycle {
    Running,
    Finished,
    Failed,
}

impl Control {
    pub(super) fn new(inputs: usize) -> Self {
        Self {
            lifecycle: Lifecycle::Running,
            active: None,
            cursors: vec![
                InputCursor {
                    position: 0,
                    ended: false,
                };
                inputs
            ],
            next_input: 0,
            output_tail: 0,
            failure: None,
        }
    }

    pub(super) const fn lifecycle(&self) -> Lifecycle {
        self.lifecycle
    }

    pub(super) fn input_count(&self) -> usize {
        self.cursors.len()
    }

    pub(super) fn cursor(&self, stage: &str, port: u32) -> Result<InputCursor, FlowError> {
        self.cursors
            .get(port as usize)
            .copied()
            .ok_or_else(|| corrupt(stage))
    }

    pub(super) fn cursor_mut(
        &mut self,
        stage: &str,
        port: u32,
    ) -> Result<&mut InputCursor, FlowError> {
        self.cursors
            .get_mut(port as usize)
            .ok_or_else(|| corrupt(stage))
    }
}

impl StoreValue for Control {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MARKER);
        bytes.push(match self.lifecycle {
            Lifecycle::Running => 0,
            Lifecycle::Finished => 1,
            Lifecycle::Failed => 2,
        });
        bytes.extend_from_slice(&self.next_input.to_be_bytes());
        bytes.extend_from_slice(&self.output_tail.to_be_bytes());
        push_len(&mut bytes, self.cursors.len())?;
        for cursor in &self.cursors {
            bytes.extend_from_slice(&cursor.position.to_be_bytes());
            bytes.push(u8::from(cursor.ended));
        }
        match &self.active {
            None => bytes.push(0),
            Some(active) => {
                bytes.push(1);
                bytes.push(match active.kind {
                    ActiveKind::Start => 0,
                    ActiveKind::Data => 1,
                    ActiveKind::End => 2,
                });
                bytes.extend_from_slice(&active.port.to_be_bytes());
                bytes.extend_from_slice(&active.position.to_be_bytes());
                push_optional_bytes(&mut bytes, active.checkpoint.as_deref())?;
            }
        }
        push_optional_bytes(&mut bytes, self.failure.as_deref().map(str::as_bytes))?;
        Ok(bytes)
    }

    fn decode_value(bytes: Vec<u8>) -> Result<Self, CodecError> {
        let mut input = Cursor::new(bytes.as_slice());
        let mut marker = [0; MARKER.len()];
        input
            .read_exact(&mut marker)
            .map_err(|error| codec(&error))?;
        if marker != MARKER {
            return Err(CodecError::new("invalid stage control marker"));
        }
        let lifecycle = match read_u8(&mut input)? {
            0 => Lifecycle::Running,
            1 => Lifecycle::Finished,
            2 => Lifecycle::Failed,
            _ => return Err(CodecError::new("invalid stage lifecycle")),
        };
        let next_input = read_u32(&mut input)?;
        let output_tail = read_u64(&mut input)?;
        let cursor_count = read_u32(&mut input)? as usize;
        if cursor_count > remaining(&input) / 9 {
            return Err(CodecError::new("invalid input cursor count"));
        }
        let mut cursors = Vec::with_capacity(cursor_count);
        for _ in 0..cursor_count {
            cursors.push(InputCursor {
                position: read_u64(&mut input)?,
                ended: match read_u8(&mut input)? {
                    0 => false,
                    1 => true,
                    _ => return Err(CodecError::new("invalid input cursor state")),
                },
            });
        }
        let active = match read_u8(&mut input)? {
            0 => None,
            1 => Some(Active {
                kind: match read_u8(&mut input)? {
                    0 => ActiveKind::Start,
                    1 => ActiveKind::Data,
                    2 => ActiveKind::End,
                    _ => return Err(CodecError::new("invalid active work kind")),
                },
                port: read_u32(&mut input)?,
                position: read_u64(&mut input)?,
                checkpoint: read_optional_bytes(&mut input, MAX_CHECKPOINT_BYTES)?,
            }),
            _ => return Err(CodecError::new("invalid active work marker")),
        };
        let failure = read_optional_bytes(&mut input, MAX_FAILURE_BYTES)?
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| CodecError::new(format!("invalid failure text: {error}")))?;
        if input.position() != bytes.len() as u64 {
            return Err(CodecError::new("trailing stage control bytes"));
        }
        validate(
            lifecycle,
            next_input,
            &cursors,
            active.as_ref(),
            failure.as_ref(),
        )?;
        Ok(Self {
            lifecycle,
            active,
            cursors,
            next_input,
            output_tail,
            failure,
        })
    }
}

pub(super) fn load(stage: &str, access: &CellAccess<'_, Control>) -> Result<Control, FlowError> {
    match access.get() {
        Ok(Some(control)) => Ok(control),
        Ok(None) | Err(StoreError::Codec(_)) => Err(corrupt(stage)),
        Err(error) => Err(error.into()),
    }
}

fn validate(
    lifecycle: Lifecycle,
    next_input: u32,
    cursors: &[InputCursor],
    active: Option<&Active>,
    failure: Option<&String>,
) -> Result<(), CodecError> {
    if (lifecycle == Lifecycle::Failed) != failure.is_some() {
        return Err(CodecError::new("stage failure state is inconsistent"));
    }
    if cursors.is_empty() {
        if next_input != 0 {
            return Err(CodecError::new("invalid source input cursor"));
        }
    } else if next_input as usize >= cursors.len() {
        return Err(CodecError::new("invalid next input cursor"));
    }
    if lifecycle == Lifecycle::Finished
        && (active.is_some() || cursors.iter().any(|cursor| !cursor.ended))
    {
        return Err(CodecError::new("unfinished state in finished stage"));
    }
    if lifecycle == Lifecycle::Running
        && !cursors.is_empty()
        && active.is_none()
        && cursors.iter().all(|cursor| cursor.ended)
    {
        return Err(CodecError::new("running stage has ended all inputs"));
    }
    if let Some(active) = active {
        if active.checkpoint.is_none() {
            return Err(CodecError::new("active work has no checkpoint"));
        }
        match active.kind {
            ActiveKind::Start
                if !cursors.is_empty() || active.port != u32::MAX || active.position != 0 =>
            {
                return Err(CodecError::new("invalid source work"));
            }
            ActiveKind::Data | ActiveKind::End if active.port as usize >= cursors.len() => {
                return Err(CodecError::new("invalid input work"));
            }
            ActiveKind::Start | ActiveKind::Data | ActiveKind::End => {}
        }
    }
    Ok(())
}

fn corrupt(stage: &str) -> FlowError {
    FlowError::CorruptStage {
        stage: stage.to_owned(),
    }
}

fn push_len(output: &mut Vec<u8>, len: usize) -> Result<(), CodecError> {
    let len = u32::try_from(len).map_err(|_| CodecError::new("stage field is too large"))?;
    output.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn push_optional_bytes(output: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), CodecError> {
    match value {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            push_len(output, value.len())?;
            output.extend_from_slice(value);
        }
    }
    Ok(())
}

fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8, CodecError> {
    let mut bytes = [0; 1];
    input
        .read_exact(&mut bytes)
        .map_err(|error| codec(&error))?;
    Ok(bytes[0])
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32, CodecError> {
    let mut bytes = [0; 4];
    input
        .read_exact(&mut bytes)
        .map_err(|error| codec(&error))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64, CodecError> {
    let mut bytes = [0; 8];
    input
        .read_exact(&mut bytes)
        .map_err(|error| codec(&error))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_optional_bytes(
    input: &mut Cursor<&[u8]>,
    maximum: usize,
) -> Result<Option<Vec<u8>>, CodecError> {
    match read_u8(input)? {
        0 => Ok(None),
        1 => {
            let len = read_u32(input)? as usize;
            if len > maximum || len > remaining(input) {
                return Err(CodecError::new("invalid stage field length"));
            }
            let mut bytes = vec![0; len];
            input
                .read_exact(&mut bytes)
                .map_err(|error| codec(&error))?;
            Ok(Some(bytes))
        }
        _ => Err(CodecError::new("invalid optional field marker")),
    }
}

fn remaining(input: &Cursor<&[u8]>) -> usize {
    input
        .get_ref()
        .len()
        .saturating_sub(usize::try_from(input.position()).unwrap_or(usize::MAX))
}

fn codec(error: &std::io::Error) -> CodecError {
    CodecError::new(format!("invalid stage control: {error}"))
}

#[cfg(test)]
mod tests {
    use dogpaddle_store::StoreValue;

    use super::{Control, InputCursor, Lifecycle};

    #[test]
    fn control_round_trips_exactly() {
        let control = Control::new(2);
        let encoded = control.encode_value().unwrap().as_ref().to_vec();
        let decoded = Control::decode_value(encoded).unwrap();

        assert_eq!(decoded.lifecycle, Lifecycle::Running);
        assert_eq!(decoded.cursors.len(), 2);
        assert_eq!(decoded.output_tail, 0);
    }

    #[test]
    fn control_rejects_trailing_bytes_and_impossible_lifecycle() {
        let control = Control::new(1);
        let mut trailing = control.encode_value().unwrap().as_ref().to_vec();
        trailing.push(0);
        assert!(Control::decode_value(trailing).is_err());

        let impossible = Control {
            lifecycle: Lifecycle::Running,
            active: None,
            cursors: vec![InputCursor {
                position: 0,
                ended: true,
            }],
            next_input: 0,
            output_tail: 0,
            failure: None,
        };
        let encoded = impossible.encode_value().unwrap().as_ref().to_vec();
        assert!(Control::decode_value(encoded).is_err());
    }
}
