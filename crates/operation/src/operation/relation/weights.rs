use dogpaddle_store::{Large, OrderedMap, OrderedMapAccess, StoreError};
use thiserror::Error;

use super::{CollisionBucket, RowDigest, bucket::BucketEntry, row_digest};

/// Durable positive multiplicities grouped by exact canonical row digest.
pub(crate) type RowWeights = OrderedMap<RowDigest, CollisionBucket, Large>;

#[derive(Debug, Error)]
pub(crate) enum RowWeightError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("relation row weight cannot become negative")]
    Negative,
    #[error("relation row weight overflow")]
    Overflow,
}

pub(crate) fn apply_weight(
    weights: &mut OrderedMapAccess<'_, RowDigest, CollisionBucket>,
    row: Vec<u8>,
    difference: i64,
) -> Result<Option<i64>, RowWeightError> {
    let digest = row_digest(&row);
    let mut bucket = weights.get(&digest)?;
    let output_difference = update_bucket(&mut bucket, row, difference)?;
    if let Some(bucket) = bucket {
        weights.put(&digest, &bucket)?;
    } else {
        weights.remove(&digest)?;
    }
    Ok(output_difference)
}

pub(crate) fn update_bucket(
    bucket: &mut Option<CollisionBucket>,
    row: Vec<u8>,
    difference: i64,
) -> Result<Option<i64>, RowWeightError> {
    let Some(existing) = bucket.as_mut() else {
        let weight = u64::try_from(difference).map_err(|_| RowWeightError::Negative)?;
        *bucket = Some(CollisionBucket {
            entries: vec![BucketEntry { row, weight }],
        });
        return Ok(Some(1));
    };

    if let Some(index) = existing.entries.iter().position(|entry| entry.row == row) {
        let new = apply_difference(existing.entries[index].weight, difference)?;
        if new == 0 {
            existing.entries.remove(index);
            if existing.entries.is_empty() {
                *bucket = None;
            }
            Ok(Some(-1))
        } else {
            existing.entries[index].weight = new;
            Ok(None)
        }
    } else {
        let weight = u64::try_from(difference).map_err(|_| RowWeightError::Negative)?;
        existing.entries.push(BucketEntry { row, weight });
        Ok(Some(1))
    }
}

fn apply_difference(weight: u64, difference: i64) -> Result<u64, RowWeightError> {
    if difference > 0 {
        weight
            .checked_add(difference.unsigned_abs())
            .ok_or(RowWeightError::Overflow)
    } else {
        weight
            .checked_sub(difference.unsigned_abs())
            .ok_or(RowWeightError::Negative)
    }
}
