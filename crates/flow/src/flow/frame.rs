use crate::error::{FlowError, store_error};
use dogpaddle_operation::operation::{OperationError, Progress, Resume};
use dogpaddle_store::{
    CodecError, DataScope, OrderedMap, ReadTransactionAccess, ScanDirection, ScanLimit, StoreError,
    StoreValue, TransactionAccess,
};
use std::borrow::Cow;

pub(super) const PAGE_BYTES: usize = 1024 * 1024;
pub(super) const ROOT_BYTES: usize = 8 * PAGE_BYTES;
pub(super) const CONTROL_BYTES: usize = 64 * 1024 + 32;
pub(super) const STEP_BYTES: usize = 4 * PAGE_BYTES;

/// The only durable execution position. A child borrows its parent's output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Frame {
    pub(super) head: usize,
    pub(super) input_port: Option<usize>,
    pub(super) phase: FramePhase,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FramePhase {
    Run(Resume),
    Send {
        next_consumer: usize,
        after: Progress,
    },
}
pub(crate) struct Frames {
    pub(super) controls: OrderedMap<u32, Frame>,
    pub(super) outputs: OrderedMap<u32, Vec<u8>>,
}
impl Frames {
    pub(crate) fn bind(data: &mut DataScope<'_>) -> Result<Self, FlowError> {
        Ok(Self {
            controls: data.data("flow/frames").map_err(store_error)?,
            outputs: data.data("flow/outputs").map_err(store_error)?,
        })
    }
    pub(super) fn top(
        &self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<(u32, Frame)>, StoreError> {
        Ok(self
            .controls
            .read(access)?
            .scan(
                ..,
                ScanDirection::Descending,
                None,
                ScanLimit::new(1, CONTROL_BYTES + 4)?,
            )?
            .entries
            .pop())
    }
    pub(super) fn output(
        &self,
        depth: u32,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Vec<u8>, OperationError> {
        self.outputs
            .read(access)?
            .get_bounded(&depth, PAGE_BYTES)?
            .ok_or_else(|| "frame output is missing".into())
    }
    pub(super) fn put(
        &self,
        depth: u32,
        frame: &Frame,
        access: TransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        self.controls.access(access)?.put(&depth, frame)?;
        Ok(())
    }
    pub(super) fn pop(
        &self,
        depth: u32,
        access: TransactionAccess<'_>,
    ) -> Result<(), OperationError> {
        self.controls.access(access)?.remove(&depth)?;
        self.outputs.access(access)?.remove(&depth)?;
        Ok(())
    }
}
impl StoreValue for Frame {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = vec![1];
        write_index(&mut encoded, self.head)?;
        match self.input_port {
            None => encoded.push(0),
            Some(port) => {
                encoded.push(1);
                write_index(&mut encoded, port)?;
            }
        }
        match &self.phase {
            FramePhase::Run(resume) => {
                encoded.push(0);
                encoded.extend_from_slice(resume.encode_value()?.as_ref());
            }
            FramePhase::Send {
                next_consumer,
                after,
            } => {
                encoded.push(1);
                write_index(&mut encoded, *next_consumer)?;
                match after {
                    Progress::Done => encoded.push(0),
                    Progress::More(resume) => {
                        encoded.push(1);
                        encoded.extend_from_slice(resume.encode_value()?.as_ref());
                    }
                }
            }
        }
        if encoded.len() > CONTROL_BYTES {
            return Err(CodecError::new("frame control is too large"));
        }
        Ok(encoded)
    }
    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.len() > CONTROL_BYTES {
            return Err(CodecError::new("frame control is too large"));
        }
        let mut remaining = bytes.as_ref();
        if take::<1>(&mut remaining)? != [1] {
            return Err(CodecError::new("invalid frame version"));
        }
        let head = read_index(&mut remaining)?;
        let input_port = match take::<1>(&mut remaining)?[0] {
            0 => None,
            1 => Some(read_index(&mut remaining)?),
            _ => return Err(CodecError::new("invalid frame port tag")),
        };
        let phase = match take::<1>(&mut remaining)?[0] {
            0 => FramePhase::Run(Resume::decode_value(Cow::Borrowed(remaining))?),
            1 => {
                let next_consumer = read_index(&mut remaining)?;
                let after = match take::<1>(&mut remaining)?[0] {
                    0 if remaining.is_empty() => Progress::Done,
                    1 => Progress::More(Resume::decode_value(Cow::Borrowed(remaining))?),
                    _ => return Err(CodecError::new("invalid frame progress")),
                };
                FramePhase::Send {
                    next_consumer,
                    after,
                }
            }
            _ => return Err(CodecError::new("invalid frame phase")),
        };
        let frame = Self {
            head,
            input_port,
            phase,
        };
        if frame.encode_value()?.as_ref() != bytes.as_ref() {
            return Err(CodecError::new("noncanonical frame"));
        }
        Ok(frame)
    }
}
fn write_index(bytes: &mut Vec<u8>, index: usize) -> Result<(), CodecError> {
    bytes.extend_from_slice(
        &u32::try_from(index)
            .map_err(|_| CodecError::new("frame index exceeds u32"))?
            .to_be_bytes(),
    );
    Ok(())
}
fn read_index(bytes: &mut &[u8]) -> Result<usize, CodecError> {
    Ok(u32::from_be_bytes(take::<4>(bytes)?) as usize)
}
fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], CodecError> {
    let (value, remaining) = bytes
        .split_first_chunk::<N>()
        .ok_or_else(|| CodecError::new("truncated frame"))?;
    *bytes = remaining;
    Ok(*value)
}
