use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, Large, OrderedMap, StoreKey, StoreValue};

use crate::operation::relation::{CollisionBucket, RowDigest};

pub(super) type Groups = OrderedMap<RowDigest, GroupBucket, Large>;
pub(super) type Entries = OrderedMap<EntryKey, CollisionBucket, Large>;
pub(super) type Control = Cell<u64>;

const GROUP_BUCKET_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupEntry {
    pub(super) group: Vec<u8>,
    pub(super) id: u64,
    pub(super) weight: u64,
    pub(super) calls: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupBucket {
    entries: Vec<GroupEntry>,
}

impl GroupBucket {
    pub(super) fn one(entry: GroupEntry) -> Self {
        Self {
            entries: vec![entry],
        }
    }

    pub(super) fn get_mut(&mut self, group: &[u8]) -> Option<&mut GroupEntry> {
        self.entries
            .binary_search_by(|entry| entry.group.as_slice().cmp(group))
            .ok()
            .map(|index| &mut self.entries[index])
    }

    pub(super) fn insert(&mut self, entry: GroupEntry) {
        let index = self
            .entries
            .binary_search_by(|candidate| candidate.group.cmp(&entry.group))
            .expect_err("the caller inserts a group absent from its digest bucket");
        self.entries.insert(index, entry);
    }

    pub(super) fn remove(&mut self, group: &[u8]) {
        let index = self
            .entries
            .binary_search_by(|entry| entry.group.as_slice().cmp(group))
            .expect("the caller removes a group present in its digest bucket");
        self.entries.remove(index);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl StoreValue for GroupBucket {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let count = u32::try_from(self.entries.len())
            .map_err(|_| CodecError::new("aggregate group bucket has too many entries"))?;
        if count == 0 {
            return Err(CodecError::new("aggregate group bucket is empty"));
        }

        let mut encoded = Vec::new();
        encoded.push(GROUP_BUCKET_VERSION);
        encoded.extend_from_slice(&count.to_be_bytes());
        let mut previous: Option<&[u8]> = None;
        for entry in &self.entries {
            if entry.weight == 0 {
                return Err(CodecError::new("aggregate group has zero weight"));
            }
            if previous.is_some_and(|previous| previous >= entry.group.as_slice()) {
                return Err(CodecError::new(
                    "aggregate group bucket is not canonically ordered",
                ));
            }
            previous = Some(&entry.group);
            put_bytes(&mut encoded, &entry.group)?;
            encoded.extend_from_slice(&entry.id.to_be_bytes());
            encoded.extend_from_slice(&entry.weight.to_be_bytes());
            let calls = u32::try_from(entry.calls.len())
                .map_err(|_| CodecError::new("aggregate group has too many call states"))?;
            encoded.extend_from_slice(&calls.to_be_bytes());
            for state in &entry.calls {
                put_bytes(&mut encoded, state)?;
            }
        }
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut cursor = ValueCursor::new(bytes.as_ref());
        if cursor.u8()? != GROUP_BUCKET_VERSION {
            return Err(CodecError::new(
                "unsupported aggregate group bucket version",
            ));
        }
        let count = usize::try_from(cursor.u32()?)
            .map_err(|_| CodecError::new("aggregate group count exceeds usize"))?;
        if count == 0 {
            return Err(CodecError::new("aggregate group bucket is empty"));
        }

        let mut entries = Vec::new();
        for _ in 0..count {
            let group = cursor.bytes()?.to_vec();
            let id = cursor.u64()?;
            let weight = cursor.u64()?;
            if weight == 0 {
                return Err(CodecError::new("aggregate group has zero weight"));
            }
            let call_count = usize::try_from(cursor.u32()?)
                .map_err(|_| CodecError::new("aggregate call count exceeds usize"))?;
            let mut calls = Vec::new();
            for _ in 0..call_count {
                calls.push(cursor.bytes()?.to_vec());
            }
            entries.push(GroupEntry {
                group,
                id,
                weight,
                calls,
            });
        }
        cursor.finish()?;
        if !entries.windows(2).all(|pair| pair[0].group < pair[1].group) {
            return Err(CodecError::new(
                "aggregate group bucket is not canonically ordered",
            ));
        }
        Ok(Self { entries })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct EntryKey {
    layout: u32,
    group: u64,
    digest: [u8; 32],
}

impl EntryKey {
    pub(super) fn new(layout: u32, group: u64, digest: RowDigest) -> Self {
        Self {
            layout,
            group,
            digest: *digest.as_bytes(),
        }
    }

    pub(super) const fn first(layout: u32, group: u64) -> Self {
        Self {
            layout,
            group,
            digest: [0; 32],
        }
    }

    pub(super) const fn last(layout: u32, group: u64) -> Self {
        Self {
            layout,
            group,
            digest: [u8::MAX; 32],
        }
    }
}

impl StoreKey for EntryKey {
    fn encode_key(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        let mut encoded = [0_u8; 44];
        encoded[..4].copy_from_slice(&self.layout.to_be_bytes());
        encoded[4..12].copy_from_slice(&self.group.to_be_bytes());
        encoded[12..].copy_from_slice(&self.digest);
        Ok(encoded)
    }

    fn decode_key(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let encoded: [u8; 44] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| CodecError::new("invalid aggregate entry key length"))?;
        Ok(Self {
            layout: u32::from_be_bytes(encoded[..4].try_into().expect("four-byte slice")),
            group: u64::from_be_bytes(encoded[4..12].try_into().expect("eight-byte slice")),
            digest: encoded[12..].try_into().expect("32-byte slice"),
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
        Ok(u32::from_be_bytes(self.take::<4>()?))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
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

    use crate::operation::relation::RowDigest;

    use super::{EntryKey, GroupBucket, GroupEntry};

    const GROUP_BUCKET_LITERAL: &[u8] = concat!(
        "\x01\x00\x00\x00\x02",
        "\x00\x00\x00\x00\x00\x00\x00\x05first",
        "\x00\x00\x00\x00\x00\x00\x00\x01",
        "\x00\x00\x00\x00\x00\x00\x00\x01",
        "\x00\x00\x00\x01",
        "\x00\x00\x00\x00\x00\x00\x00\x01\x01",
        "\x00\x00\x00\x00\x00\x00\x00\x06second",
        "\x00\x00\x00\x00\x00\x00\x00\x02",
        "\x00\x00\x00\x00\x00\x00\x00\x01",
        "\x00\x00\x00\x01",
        "\x00\x00\x00\x00\x00\x00\x00\x01\x02",
    )
    .as_bytes();

    fn group(bytes: &[u8], id: u64) -> GroupEntry {
        GroupEntry {
            group: bytes.to_vec(),
            id,
            weight: 1,
            calls: vec![vec![u8::try_from(id).unwrap()]],
        }
    }

    #[test]
    fn entry_key_is_fixed_width_and_group_bucket_resolves_full_byte_collisions() {
        let digest = RowDigest::decode_key(Cow::Owned(vec![0x7f; 32])).unwrap();
        let key = EntryKey::new(0x0102_0304, 0x0506_0708_090a_0b0c, digest);
        let encoded_key = key.encode_key().unwrap();
        assert_eq!(encoded_key.as_ref().len(), 44);
        assert_eq!(&encoded_key.as_ref()[..4], &0x0102_0304_u32.to_be_bytes());
        assert_eq!(
            &encoded_key.as_ref()[4..12],
            &0x0506_0708_090a_0b0c_u64.to_be_bytes()
        );
        assert_eq!(&encoded_key.as_ref()[12..], &[0x7f; 32]);
        assert_eq!(
            EntryKey::decode_key(Cow::Borrowed(encoded_key.as_ref())).unwrap(),
            key
        );

        let mut bucket = GroupBucket::one(group(b"second", 2));
        bucket.insert(group(b"first", 1));
        assert_eq!(bucket.get_mut(b"first").unwrap().id, 1);
        assert_eq!(bucket.get_mut(b"second").unwrap().id, 2);
        let encoded_bucket = bucket.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded_bucket, GROUP_BUCKET_LITERAL);
        let decoded = GroupBucket::decode_value(Cow::Borrowed(&encoded_bucket)).unwrap();
        assert_eq!(decoded, bucket);
        bucket.remove(b"first");
        assert!(bucket.get_mut(b"first").is_none());
        assert_eq!(bucket.get_mut(b"second").unwrap().id, 2);
    }
}
