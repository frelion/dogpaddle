use dogpaddle_store::{OrderedMap, PartitionKey};
use serde::{Deserialize, Deserializer, Serialize};

pub(super) type Rows = OrderedMap<PartitionKey<Vec<u8>, Vec<u8>>, std::num::NonZeroU64>;
pub(super) type MatchCounts = OrderedMap<Vec<u8>, u64>;

/// Returns the collision-free key for one row's committed qualifying-match count.
pub(super) fn actual_match_key(port: usize, row: &[u8]) -> Vec<u8> {
    let port = u8::try_from(port)
        .ok()
        .filter(|port| *port <= 1)
        .expect("a validated equi-join port is zero or one");
    let mut key = Vec::with_capacity(row.len().saturating_add(1));
    key.push(port);
    key.extend_from_slice(row);
    key
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct JoinCursor {
    pub(super) found_match: bool,
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) resume_after: Option<Vec<u8>>,
}
fn decode_optional_bytes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<&[u8]>::deserialize(decoder).map(|value| value.map(ToOwned::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_count_keys_separate_ports_and_preserve_the_canonical_row() {
        assert_eq!(actual_match_key(0, &[]), [0]);
        assert_eq!(actual_match_key(1, &[0, 1, 2]), [1, 0, 1, 2]);
        assert_ne!(actual_match_key(0, &[1, 2]), actual_match_key(1, &[1, 2]));
    }
}
