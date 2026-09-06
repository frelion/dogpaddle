use std::borrow::Cow;

use dogpaddle_store::{CodecError, StoreValue};

const FORMAT_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BucketEntry {
    pub(super) row: Vec<u8>,
    pub(super) weight: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CollisionBucket {
    pub(super) entries: Vec<BucketEntry>,
}

impl StoreValue for CollisionBucket {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let count = u32::try_from(self.entries.len())
            .map_err(|_| CodecError::new("relation row bucket has too many entries"))?;
        if count == 0 {
            return Err(CodecError::new("relation row bucket is empty"));
        }
        let mut encoded = Vec::new();
        encoded.push(FORMAT_VERSION);
        encoded.extend_from_slice(&count.to_be_bytes());
        for entry in &self.entries {
            if entry.weight == 0 {
                return Err(CodecError::new(
                    "relation row bucket contains a zero weight",
                ));
            }
            let length = u64::try_from(entry.row.len())
                .map_err(|_| CodecError::new("relation row length exceeds u64"))?;
            encoded.extend_from_slice(&length.to_be_bytes());
            encoded.extend_from_slice(&entry.row);
            encoded.extend_from_slice(&entry.weight.to_be_bytes());
        }
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut remaining = bytes.as_ref();
        let version = take::<1>(&mut remaining)?[0];
        if version != FORMAT_VERSION {
            return Err(CodecError::new("unsupported relation row bucket version"));
        }
        let count = usize::try_from(u32::from_be_bytes(take::<4>(&mut remaining)?))
            .map_err(|_| CodecError::new("relation row bucket entry count exceeds usize"))?;
        if count == 0 {
            return Err(CodecError::new("relation row bucket is empty"));
        }

        let mut entries = Vec::new();
        for _ in 0..count {
            let length = usize::try_from(u64::from_be_bytes(take::<8>(&mut remaining)?))
                .map_err(|_| CodecError::new("relation row length exceeds usize"))?;
            let row = take_slice(&mut remaining, length)?.to_vec();
            let weight = u64::from_be_bytes(take::<8>(&mut remaining)?);
            if weight == 0 {
                return Err(CodecError::new(
                    "relation row bucket contains a zero weight",
                ));
            }
            entries.push(BucketEntry { row, weight });
        }
        if !remaining.is_empty() {
            return Err(CodecError::new(
                "relation row bucket contains trailing bytes",
            ));
        }
        Ok(Self { entries })
    }
}

fn take<const N: usize>(remaining: &mut &[u8]) -> Result<[u8; N], CodecError> {
    let (value, trailing) = remaining
        .split_first_chunk::<N>()
        .ok_or_else(|| CodecError::new("relation row bucket is truncated"))?;
    *remaining = trailing;
    Ok(*value)
}

fn take_slice<'a>(remaining: &mut &'a [u8], length: usize) -> Result<&'a [u8], CodecError> {
    let (value, trailing) = remaining
        .split_at_checked(length)
        .ok_or_else(|| CodecError::new("relation row bucket is truncated"))?;
    *remaining = trailing;
    Ok(value)
}
