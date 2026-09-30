//! Persistent ASOF row multiplicities and replay continuation.

use std::num::NonZeroU64;

use dogpaddle_store::OrderedMap;
use serde::{Deserialize, Deserializer, Serialize};

pub(super) type Rows = OrderedMap<Vec<u8>, NonZeroU64>;

/// Last fully corrected left key for the current right event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AsOfCursor {
    #[serde(deserialize_with = "decode_optional_bytes")]
    pub(super) left_resume_after: Option<Vec<u8>>,
}
fn decode_optional_bytes<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<&[u8]>::deserialize(decoder).map(|value| value.map(ToOwned::to_owned))
}
