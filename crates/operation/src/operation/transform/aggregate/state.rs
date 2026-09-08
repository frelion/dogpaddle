use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, PartitionedMultiset, StoreKey, StoreValue};

pub(super) type Groups = OrderedMap<Vec<u8>, GroupState>;
pub(super) type Entries = PartitionedMultiset<EntryPartition, Vec<u8>>;
pub(super) type Control = Cell<u64>;

const GROUP_STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupState {
    pub(super) id: u64,
    pub(super) weight: u64,
    pub(super) folds: Vec<Vec<u8>>,
}

impl StoreValue for GroupState {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        let count = u32::try_from(self.folds.len())
            .map_err(|_| CodecError::new("aggregate group has too many fold states"))?;
        let mut encoded = Vec::new();
        encoded.push(GROUP_STATE_VERSION);
        encoded.extend_from_slice(&self.id.to_be_bytes());
        encoded.extend_from_slice(&self.weight.to_be_bytes());
        encoded.extend_from_slice(&count.to_be_bytes());
        for state in &self.folds {
            put_bytes(&mut encoded, state)?;
        }
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut cursor = ValueCursor::new(bytes.as_ref());
        if cursor.u8()? != GROUP_STATE_VERSION {
            return Err(CodecError::new("unsupported aggregate group state version"));
        }
        let id = cursor.u64()?;
        let weight = cursor.u64()?;
        if weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        let count = usize::try_from(cursor.u32()?)
            .map_err(|_| CodecError::new("aggregate call count exceeds usize"))?;
        let mut folds = Vec::with_capacity(count);
        for _ in 0..count {
            folds.push(cursor.bytes()?.to_vec());
        }
        cursor.finish()?;
        Ok(Self { id, weight, folds })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct EntryPartition {
    layout: u32,
    group: u64,
}

impl EntryPartition {
    pub(super) const fn new(layout: u32, group: u64) -> Self {
        Self { layout, group }
    }
}

impl StoreKey for EntryPartition {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = [0_u8; 12];
        encoded[..4].copy_from_slice(&self.layout.to_be_bytes());
        encoded[4..].copy_from_slice(&self.group.to_be_bytes());
        Ok(encoded)
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let encoded: [u8; 12] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid aggregate entry partition length"))?;
        Ok(Self {
            layout: u32::from_be_bytes(
                encoded[..4]
                    .try_into()
                    .expect("the slice has exactly four bytes"),
            ),
            group: u64::from_be_bytes(
                encoded[4..]
                    .try_into()
                    .expect("the slice has exactly eight bytes"),
            ),
        })
    }
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), CodecError> {
    let length = u64::try_from(value.len())
        .map_err(|_| CodecError::new("aggregate state value exceeds u64"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

struct ValueCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> ValueCursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.take()?))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.take()?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let length = usize::try_from(self.u64()?)
            .map_err(|_| CodecError::new("aggregate state length exceeds usize"))?;
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or_else(|| CodecError::new("aggregate state is truncated"))?;
        self.remaining = remaining;
        Ok(value)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let (value, remaining) = self
            .remaining
            .split_first_chunk::<N>()
            .ok_or_else(|| CodecError::new("aggregate state is truncated"))?;
        self.remaining = remaining;
        Ok(*value)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new("aggregate state has trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use dogpaddle_store::{StoreKey, StoreValue};

    use super::{EntryPartition, GroupState};

    #[test]
    fn group_state_and_partition_round_trip() {
        let state = GroupState {
            id: 7,
            weight: 3,
            folds: vec![vec![1, 2], Vec::new()],
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [
                1, // version
                0, 0, 0, 0, 0, 0, 0, 7, // group ID
                0, 0, 0, 0, 0, 0, 0, 3, // group weight
                0, 0, 0, 2, // fold count
                0, 0, 0, 0, 0, 0, 0, 2, 1, 2, // first fold
                0, 0, 0, 0, 0, 0, 0, 0, // second fold
            ]
        );
        assert_eq!(
            GroupState::decode_value(Cow::Borrowed(&encoded)).unwrap(),
            state
        );

        let partition = EntryPartition::new(5, 9);
        let encoded = partition.encode_key().unwrap().as_ref().to_vec();
        assert_eq!(encoded, [0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 9]);
        assert_eq!(
            EntryPartition::decode_key(Cow::Borrowed(&encoded)).unwrap(),
            partition
        );
    }
}
