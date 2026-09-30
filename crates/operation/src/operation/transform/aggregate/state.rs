use std::borrow::Cow;

use dogpaddle_store::{Cell, CodecError, OrderedMap, PartitionKey, StoreKey, StoreValue};

pub(super) type Groups = OrderedMap<Vec<u8>, GroupState>;
pub(super) type Entries = OrderedMap<PartitionKey<EntryPartition, Vec<u8>>, std::num::NonZeroU64>;
pub(super) type Control = Cell<u64>;

const GROUP_STATE_VERSION: u8 = 1;
const STATISTIC_BYTES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Statistic {
    Count(u64),
    Signed { count: u64, sum: i128 },
    Unsigned { count: u64, sum: u128 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupState {
    pub(super) id: u64,
    pub(super) weight: u64,
    pub(super) statistics: Vec<Statistic>,
    /// Entries are the source of truth; this cache changes in the same transaction.
    /// Present empty keys are valid, so absence has its own wire marker.
    pub(super) extremes: Box<[Option<Vec<u8>>]>,
}

impl GroupState {
    pub(super) fn logical_bytes(&self) -> usize {
        24_usize
            .saturating_add(self.statistics.len().saturating_mul(STATISTIC_BYTES))
            .saturating_add(
                self.extremes
                    .iter()
                    .map(|key| 8 + key.as_ref().map_or(0, Vec::len))
                    .sum::<usize>(),
            )
    }
}

// Wire bytes are logical_bytes() + one version byte. Fixed-width statistics
// prevent compact Count/None sequences from expanding after a bounded Map read.
impl StoreValue for GroupState {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        if self.weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        let statistics = u32::try_from(self.statistics.len())
            .map_err(|_| CodecError::new("aggregate group has too many statistics"))?;
        let extremes = u32::try_from(self.extremes.len())
            .map_err(|_| CodecError::new("aggregate group has too many cached extrema"))?;
        let mut encoded = Vec::with_capacity(self.logical_bytes().saturating_add(1));
        encoded.push(GROUP_STATE_VERSION);
        encoded.extend_from_slice(&self.id.to_be_bytes());
        encoded.extend_from_slice(&self.weight.to_be_bytes());
        encoded.extend_from_slice(&statistics.to_be_bytes());
        encoded.extend_from_slice(&extremes.to_be_bytes());
        for statistic in &self.statistics {
            let mut bytes = [0; STATISTIC_BYTES];
            let (tag, count, sum) = match statistic {
                Statistic::Count(count) => (0, *count, [0; 16]),
                Statistic::Signed { count, sum } => (1, *count, sum.to_be_bytes()),
                Statistic::Unsigned { count, sum } => (2, *count, sum.to_be_bytes()),
            };
            bytes[0] = tag;
            bytes[1..9].copy_from_slice(&count.to_be_bytes());
            bytes[9..25].copy_from_slice(&sum);
            encoded.extend_from_slice(&bytes);
        }
        for key in &self.extremes {
            match key {
                None => encoded.extend_from_slice(&u64::MAX.to_be_bytes()),
                Some(key) => {
                    let length = u64::try_from(key.len())
                        .map_err(|_| CodecError::new("aggregate extreme key is too large"))?;
                    encoded.extend_from_slice(&length.to_be_bytes());
                    encoded.extend_from_slice(key);
                }
            }
        }
        Ok(encoded)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut remaining = bytes.as_ref();
        if take::<1>(&mut remaining)? != [GROUP_STATE_VERSION] {
            return Err(CodecError::new("unsupported aggregate group state version"));
        }
        let id = u64::from_be_bytes(take(&mut remaining)?);
        let weight = u64::from_be_bytes(take(&mut remaining)?);
        if weight == 0 {
            return Err(CodecError::new("aggregate group has zero weight"));
        }
        let statistic_count = u32::from_be_bytes(take(&mut remaining)?) as usize;
        let extreme_count = u32::from_be_bytes(take(&mut remaining)?) as usize;
        let minimum = statistic_count
            .checked_mul(STATISTIC_BYTES)
            .and_then(|bytes| {
                extreme_count
                    .checked_mul(8)
                    .and_then(|keys| bytes.checked_add(keys))
            })
            .ok_or_else(|| CodecError::new("aggregate state count overflows"))?;
        if minimum > remaining.len() {
            return Err(CodecError::new("aggregate state count exceeds its payload"));
        }
        let mut statistics = Vec::with_capacity(statistic_count);
        for _ in 0..statistic_count {
            let bytes = take::<STATISTIC_BYTES>(&mut remaining)?;
            if bytes[25..].iter().any(|byte| *byte != 0) {
                return Err(CodecError::new("noncanonical aggregate statistic padding"));
            }
            let count = u64::from_be_bytes(bytes[1..9].try_into().expect("fixed count width"));
            let sum = bytes[9..25].try_into().expect("fixed sum width");
            statistics.push(match bytes[0] {
                0 if sum == [0; 16] => Statistic::Count(count),
                1 => Statistic::Signed {
                    count,
                    sum: i128::from_be_bytes(sum),
                },
                2 => Statistic::Unsigned {
                    count,
                    sum: u128::from_be_bytes(sum),
                },
                _ => return Err(CodecError::new("invalid aggregate statistic")),
            });
        }
        let mut extremes = Vec::with_capacity(extreme_count);
        for index in 0..extreme_count {
            let length = u64::from_be_bytes(take(&mut remaining)?);
            if length == u64::MAX {
                extremes.push(None);
            } else {
                let length = usize::try_from(length)
                    .map_err(|_| CodecError::new("aggregate extreme key length exceeds usize"))?;
                let reserved = (extreme_count - index - 1) * 8;
                if length > remaining.len().saturating_sub(reserved) {
                    return Err(CodecError::new("aggregate extreme key exceeds its payload"));
                }
                let (key, rest) = remaining.split_at(length);
                extremes.push(Some(key.to_vec()));
                remaining = rest;
            }
        }
        if !remaining.is_empty() {
            return Err(CodecError::new("aggregate state has trailing bytes"));
        }
        Ok(Self {
            id,
            weight,
            statistics,
            extremes: extremes.into_boxed_slice(),
        })
    }
}

fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], CodecError> {
    let (value, remaining) = bytes
        .split_first_chunk::<N>()
        .ok_or_else(|| CodecError::new("aggregate state is truncated"))?;
    *bytes = remaining;
    Ok(*value)
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

    use super::{EntryPartition, GroupState, Statistic};

    #[test]
    fn group_state_and_partition_round_trip() {
        let state = GroupState {
            id: 7,
            weight: 3,
            statistics: vec![Statistic::Count(2), Statistic::Signed { count: 1, sum: -2 }],
            extremes: Box::new([Some(Vec::new()), None, Some(vec![0x80, 0x2a])]),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(
            encoded,
            [
                1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
                0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255,
                255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 2, 128, 42,
            ]
        );
        assert_eq!(encoded.len(), state.logical_bytes() + 1);
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
            statistics: Vec::new(),
            extremes: Box::new([None]),
        };
        let mut encoded = state.encode_value().unwrap().as_ref().to_vec();
        let marker = encoded.len() - 1;
        encoded[marker] = 2;
        assert!(GroupState::decode_value(Cow::Owned(encoded)).is_err());
    }

    #[test]
    fn group_state_rejects_forged_counts_padding_and_trailing_bytes() {
        let state = GroupState {
            id: 7,
            weight: 1,
            statistics: Vec::new(),
            extremes: Box::new([]),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded.len(), 25);
        for length in 0..encoded.len() {
            assert!(GroupState::decode_value(Cow::Borrowed(&encoded[..length])).is_err());
        }
        for range in [17..21, 21..25] {
            let mut forged = encoded.clone();
            forged[range].copy_from_slice(&u32::MAX.to_be_bytes());
            assert!(GroupState::decode_value(Cow::Owned(forged)).is_err());
        }
        let mut zero_weight = encoded.clone();
        zero_weight[9..17].fill(0);
        assert!(GroupState::decode_value(Cow::Owned(zero_weight)).is_err());
        let mut invalid_version = encoded.clone();
        invalid_version[0] = u8::MAX;
        assert!(GroupState::decode_value(Cow::Owned(invalid_version)).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(GroupState::decode_value(Cow::Owned(trailing)).is_err());

        let state = GroupState {
            id: 1,
            weight: 1,
            statistics: vec![Statistic::Count(0)],
            extremes: Box::new([]),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        for offset in [25, 34, 50] {
            let mut malformed = encoded.clone();
            malformed[offset] = u8::MAX;
            assert!(GroupState::decode_value(Cow::Owned(malformed)).is_err());
        }
    }

    #[test]
    fn bounded_state_read_admits_logical_statistics_before_decoding() {
        use dogpaddle_store::{OrderedMap, Store, StoreError};
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let groups = store
            .create_data::<OrderedMap<Vec<u8>, GroupState>>("groups")
            .unwrap();
        let state = GroupState {
            id: 1,
            weight: 1,
            statistics: vec![Statistic::Count(0); 4096],
            extremes: vec![None; 4096].into_boxed_slice(),
        };
        let encoded = state.encode_value().unwrap().as_ref().to_vec();
        assert_eq!(encoded.len(), state.logical_bytes() + 1);
        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin();
            groups
                .access(transaction.access())
                .unwrap()
                .put(&vec![1], &state)
                .unwrap();
            transaction.commit().unwrap();
        }
        let (_transactions, reads) = transactions.split();
        let snapshot = reads.begin();
        let map = groups.read(snapshot.access()).unwrap();
        assert!(matches!(
            map.get_bounded(&vec![1], 4096),
            Err(StoreError::ItemTooLarge { .. })
        ));
        assert_eq!(
            map.get_bounded(&vec![1], encoded.len()).unwrap(),
            Some(state)
        );
    }
}
