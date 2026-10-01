use std::num::NonZeroU64;

use crate::{
    DataAccess, MapPartition, MapReadPartition, OrderedMapAccess, OrderedMapReadAccess,
    ReadDataAccess, StoreError, StoreKey,
};

/// Multiplicity immediately before and after one checked map adjustment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultiplicityChange {
    before: u64,
    after: u64,
}
impl MultiplicityChange {
    /// Returns the prior multiplicity, including zero for an absent key.
    #[must_use]
    pub const fn before(self) -> u64 {
        self.before
    }
    /// Returns the resulting multiplicity; zero means the key was erased.
    #[must_use]
    pub const fn after(self) -> u64 {
        self.after
    }
}

/// Checks one signed weight change without accessing durable state.
/// # Errors
/// Returns underflow or overflow when the result is outside `0..=u64::MAX`.
pub fn checked_weight(before: u64, difference: i64) -> Result<u64, StoreError> {
    if difference >= 0 {
        before
            .checked_add(difference.unsigned_abs())
            .ok_or(StoreError::MultiplicityOverflow)
    } else {
        before
            .checked_sub(difference.unsigned_abs())
            .ok_or(StoreError::MultiplicityUnderflow)
    }
}

impl<K: StoreKey> OrderedMapAccess<'_, K, NonZeroU64> {
    /// Returns a positive weight, or zero for an absent key.
    /// # Errors
    /// Encoding, decoding and storage failures poison the transaction.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = self.data.poison_on_error(key.encode_key())?;
        read_weight(self.data.as_read(), key.as_ref())
    }
    /// Applies one checked signed difference, deleting zero weight.
    /// # Errors
    /// Invalid weights, encoding and storage errors poison the transaction.
    pub fn adjust(&mut self, key: &K, difference: i64) -> Result<MultiplicityChange, StoreError> {
        let key = self.data.poison_on_error(key.encode_key())?;
        adjust_encoded(&mut self.data, key.as_ref(), difference)
    }
    /// Writes a previously checked weight; zero erases its key.
    /// # Errors
    /// Encoding and storage errors poison the transaction.
    pub fn set_multiplicity(&mut self, key: &K, weight: u64) -> Result<(), StoreError> {
        let key = self.data.poison_on_error(key.encode_key())?;
        set_encoded(&mut self.data, key.as_ref(), weight)
    }
}
impl<K: StoreKey> OrderedMapReadAccess<'_, K, NonZeroU64> {
    /// Returns a weight visible to this snapshot, or zero when absent.
    /// # Errors
    /// Encoding, decoding and storage errors poison the snapshot.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = self.data.poison_on_error(key.encode_key())?;
        read_weight(&self.data, key.as_ref())
    }
}
impl<K: StoreKey> MapPartition<'_, '_, K, NonZeroU64> {
    /// Returns a positive weight, or zero for an absent local key.
    /// # Errors
    /// Encoding, decoding and storage errors poison the transaction.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = super::partition::encode_key(self.data.as_read(), &self.prefix, key)?;
        read_weight(self.data.as_read(), &key)
    }
    /// Applies one checked signed local-key difference, deleting zero weight.
    /// # Errors
    /// Invalid weights, encoding and storage errors poison the transaction.
    pub fn adjust(&mut self, key: &K, difference: i64) -> Result<MultiplicityChange, StoreError> {
        let key = super::partition::encode_key(self.data.as_read(), &self.prefix, key)?;
        adjust_encoded(self.data, &key, difference)
    }
    /// Writes a previously checked local-key weight; zero erases its key.
    /// # Errors
    /// Encoding and storage errors poison the transaction.
    pub fn set_multiplicity(&mut self, key: &K, weight: u64) -> Result<(), StoreError> {
        let key = super::partition::encode_key(self.data.as_read(), &self.prefix, key)?;
        set_encoded(self.data, &key, weight)
    }
}
impl<K: StoreKey> MapReadPartition<'_, '_, K, NonZeroU64> {
    /// Returns a local-key weight visible to this snapshot, or zero when absent.
    /// # Errors
    /// Encoding, decoding and storage errors poison the snapshot.
    pub fn multiplicity(&self, key: &K) -> Result<u64, StoreError> {
        let key = super::partition::encode_key(self.data, &self.prefix, key)?;
        read_weight(self.data, &key)
    }
}
fn read_weight(data: &ReadDataAccess<'_>, key: &[u8]) -> Result<u64, StoreError> {
    let value = match data.get_bounded::<NonZeroU64>(key, 8) {
        Err(StoreError::ItemTooLarge { .. }) => {
            return data.poison_on_error(Err(StoreError::Codec(crate::CodecError::new(
                "positive weight is not eight bytes",
            ))));
        }
        result => result?,
    };
    Ok(value.map_or(0, NonZeroU64::get))
}
fn adjust_encoded(
    data: &mut DataAccess<'_>,
    key: &[u8],
    difference: i64,
) -> Result<MultiplicityChange, StoreError> {
    let before = read_weight(data.as_read(), key)?;
    let after = data.poison_on_error(checked_weight(before, difference))?;
    if before != after {
        set_encoded(data, key, after)?;
    }
    Ok(MultiplicityChange { before, after })
}
fn set_encoded(data: &mut DataAccess<'_>, key: &[u8], weight: u64) -> Result<(), StoreError> {
    if weight == 0 {
        data.erase(key)
    } else {
        data.put(key, &weight.to_be_bytes())
    }
}
