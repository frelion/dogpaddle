use std::{borrow::Cow, fmt, marker::PhantomData};

use dogpaddle_store::{Cell, CodecError, OrderedMap, PartitionedMultiset, StoreKey, StoreValue};
use serde::{Deserialize, Deserializer, Serialize, de::SeqAccess};

pub(super) type Groups = OrderedMap<Vec<u8>, GroupState>;
pub(super) type Entries = PartitionedMultiset<EntryPartition, Vec<u8>>;
pub(super) type Control = Cell<u64>;

const GROUP_STATE_VERSION: u8 = 2;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct GroupState {
    pub(super) id: u64,
    pub(super) weight: u64,
    #[serde(deserialize_with = "decode_folds")]
    pub(super) folds: Vec<Vec<u8>>,
    /// Current extreme key per bound extrema slot, absent while no key is stored.
    ///
    /// The entries partition remains the source of truth; this is a cache that
    /// is written in the same transaction and only re-read when the extreme
    /// itself is retracted. A present but empty key is a valid key, so presence
    /// is encoded explicitly rather than inferred from the key length.
    #[serde(deserialize_with = "decode_extremes")]
    pub(super) extremes: Box<[Option<Vec<u8>>]>,
}

impl StoreValue for GroupState {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        u32::try_from(self.folds.len())
            .map_err(|_| CodecError::new("aggregate group has too many fold states"))?;
        u32::try_from(self.extremes.len())
            .map_err(|_| CodecError::new("aggregate group has too many cached extrema"))?;
        bincode::serde::encode_to_vec(
            (GROUP_STATE_VERSION, self),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("aggregate group state cannot be encoded"))
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        if bytes.first() != Some(&GROUP_STATE_VERSION) {
            return Err(CodecError::new("unsupported aggregate group state version"));
        }
        let ((_, state), consumed): ((u8, Self), usize) = bincode::serde::borrow_decode_from_slice(
            bytes.as_ref(),
            bincode::config::standard()
                .with_big_endian()
                .with_variable_int_encoding()
                .with_limit::<{ isize::MAX as usize }>(),
        )
        .map_err(|_| CodecError::new("aggregate group state is invalid"))?;
        if consumed != bytes.len() {
            return Err(CodecError::new("aggregate state has trailing bytes"));
        }
        if state.weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        if u32::try_from(state.folds.len()).is_err() || u32::try_from(state.extremes.len()).is_err()
        {
            return Err(CodecError::new("aggregate group state count is too large"));
        }
        if state.encode_value()?.as_ref() != bytes.as_ref() {
            return Err(CodecError::new("aggregate group state is non-canonical"));
        }
        Ok(state)
    }
}

// Serde's normal Vec visitor can reserve up to 1 MiB from a forged count in a
// short value. Push only elements actually present, copying each borrowed
// field directly into the final state instead of staging a second Vec.
fn decode_sequence<'de, D, B, O, F>(decoder: D, map: F) -> Result<Vec<O>, D::Error>
where
    D: Deserializer<'de>,
    B: Deserialize<'de>,
    F: Fn(B) -> O,
{
    struct NoReserve<B, O, F>(F, PhantomData<fn(B) -> O>);

    impl<'de, B, O, F> serde::de::Visitor<'de> for NoReserve<B, O, F>
    where
        B: Deserialize<'de>,
        F: Fn(B) -> O,
    {
        type Value = Vec<O>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a sequence of stored bytes")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element::<B>()? {
                values.push((self.0)(value));
            }
            Ok(values)
        }
    }

    decoder.deserialize_seq(NoReserve(map, PhantomData))
}

fn decode_folds<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<Vec<u8>>, D::Error> {
    decode_sequence(decoder, |bytes: &'de [u8]| bytes.to_vec())
}

#[expect(
    clippy::type_complexity,
    reason = "Serde requires the exact extrema field type"
)]
fn decode_extremes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Box<[Option<Vec<u8>>]>, D::Error> {
    let keys = decode_sequence(decoder, |key: Option<&'de [u8]>| key.map(ToOwned::to_owned))?;
    Ok(keys.into_boxed_slice())
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
            extremes: Box::new([Some(Vec::new()), None, Some(vec![0x80, 0x2a])]),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [
                2, // version
                7, // group ID
                3, // group weight
                2, // fold count
                2, 1, 2, // first fold
                0, // second fold
                3, // extrema cache count
                1, 0, // present but empty key
                0, // absent key
                1, 2, 0x80, 0x2a, // present two-byte key
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

    #[test]
    fn group_state_rejects_an_unknown_extrema_cache_marker() {
        let state = GroupState {
            id: 1,
            weight: 1,
            folds: Vec::new(),
            extremes: Box::new([None]),
        };
        let mut encoded = state.encode_value().unwrap().as_ref().to_vec();
        let marker = encoded.len() - 1;
        encoded[marker] = 2;
        assert!(GroupState::decode_value(Cow::Owned(encoded)).is_err());
    }

    #[test]
    fn group_state_rejects_noncanonical_lengths_and_unbounded_counts() {
        let state = GroupState {
            id: 7,
            weight: 1,
            folds: Vec::new(),
            extremes: Box::new([]),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded, [2, 7, 1, 0, 0]);
        for length in 0..encoded.len() {
            assert!(GroupState::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
        }

        let mut overlong_id = vec![2, 251, 0, 7];
        overlong_id.extend_from_slice(&encoded[2..]);
        assert!(GroupState::decode_value(Cow::Borrowed(&overlong_id)).is_err());
        assert!(GroupState::decode_value(Cow::Borrowed(&[2, 7, 1, 251, 0, 0, 0])).is_err());
        assert!(GroupState::decode_value(Cow::Borrowed(&[2, 7, 1, 0, 251, 0, 0])).is_err());
        assert!(GroupState::decode_value(Cow::Borrowed(&[2, 7, 0, 0, 0])).is_err());

        let huge = [253, 255, 255, 255, 255, 255, 255, 255, 255];
        let mut huge_folds = vec![2, 7, 1];
        huge_folds.extend_from_slice(&huge);
        assert!(GroupState::decode_value(Cow::Borrowed(&huge_folds)).is_err());
        let mut huge_fold_value = vec![2, 7, 1, 1];
        huge_fold_value.extend_from_slice(&huge);
        assert!(GroupState::decode_value(Cow::Borrowed(&huge_fold_value)).is_err());
        let mut huge_extremes = vec![2, 7, 1, 0];
        huge_extremes.extend_from_slice(&huge);
        assert!(GroupState::decode_value(Cow::Borrowed(&huge_extremes)).is_err());

        let mut invalid_version = encoded.clone();
        invalid_version[0] = u8::MAX;
        assert!(GroupState::decode_value(Cow::Borrowed(&invalid_version)).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(GroupState::decode_value(Cow::Borrowed(&trailing)).is_err());
    }
}
