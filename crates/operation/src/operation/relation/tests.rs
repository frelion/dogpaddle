use std::borrow::Cow;

use dogpaddle_store::{StoreKey, StoreValue};

use super::{
    CollisionBucket,
    bucket::BucketEntry,
    row::row_digest,
    weights::{RowWeightError, update_bucket},
};

#[test]
fn row_digest_and_bucket_encoding_have_stable_v1_literals() {
    let digest = row_digest(b"abc");
    assert_eq!(
        digest.as_bytes(),
        &[
            4, 218, 114, 182, 185, 175, 202, 56, 247, 249, 33, 13, 62, 154, 119, 167, 18, 200, 60,
            112, 173, 109, 193, 43, 238, 116, 42, 51, 80, 209, 71, 1,
        ]
    );
    assert_eq!(
        digest.encode_key().unwrap().as_ref(),
        digest.as_bytes().as_slice()
    );
    assert_eq!(
        super::RowDigest::decode_key(Cow::Borrowed(digest.as_bytes())).unwrap(),
        digest
    );
    for length in [0, 31, 33] {
        assert!(super::RowDigest::decode_key(Cow::Owned(vec![0; length])).is_err());
    }

    let bucket = CollisionBucket {
        entries: vec![
            BucketEntry {
                row: b"a".to_vec(),
                weight: 2,
            },
            BucketEntry {
                row: b"bc".to_vec(),
                weight: 5,
            },
        ],
    };
    let encoded = bucket.encode_value().unwrap();
    assert_eq!(
        encoded.as_ref(),
        &[
            1, 0, 0, 0, 2, // version and count
            0, 0, 0, 0, 0, 0, 0, 1, b'a', // first row
            0, 0, 0, 0, 0, 0, 0, 2, // first weight
            0, 0, 0, 0, 0, 0, 0, 2, b'b', b'c', // second row
            0, 0, 0, 0, 0, 0, 0, 5, // second weight
        ]
    );
    assert_eq!(
        CollisionBucket::decode_value(Cow::Borrowed(encoded.as_ref())).unwrap(),
        bucket
    );
    for length in 0..encoded.as_ref().len() {
        assert!(CollisionBucket::decode_value(Cow::Borrowed(&encoded.as_ref()[..length])).is_err());
    }
}

#[test]
fn exact_rows_coexist_and_update_independently_inside_one_collision_bucket() {
    let mut bucket = None;
    assert_eq!(
        update_bucket(&mut bucket, b"right".to_vec(), 3).unwrap(),
        Some(1)
    );
    assert_eq!(
        update_bucket(&mut bucket, b"left".to_vec(), 2).unwrap(),
        Some(1)
    );
    assert_eq!(
        update_bucket(&mut bucket, b"right".to_vec(), -1).unwrap(),
        None
    );
    let entries = &bucket.as_ref().unwrap().entries;
    assert_eq!(entries.len(), 2);
    for row in [b"left".as_slice(), b"right".as_slice()] {
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.row == row)
                .unwrap()
                .weight,
            2
        );
    }
    assert_eq!(
        update_bucket(&mut bucket, b"left".to_vec(), -2).unwrap(),
        Some(-1)
    );
    assert_eq!(bucket.as_ref().unwrap().entries.len(), 1);
    assert_eq!(
        update_bucket(&mut bucket, b"right".to_vec(), -2).unwrap(),
        Some(-1)
    );
    assert!(bucket.is_none());
}

#[test]
fn weight_updates_reject_negative_prefix_and_overflow_without_mutation() {
    let mut missing = None;
    assert!(matches!(
        update_bucket(&mut missing, b"row".to_vec(), -1),
        Err(RowWeightError::Negative)
    ));
    assert!(missing.is_none());

    let mut bucket = Some(CollisionBucket {
        entries: vec![BucketEntry {
            row: b"row".to_vec(),
            weight: u64::MAX,
        }],
    });
    let before = bucket.clone();
    assert!(matches!(
        update_bucket(&mut bucket, b"row".to_vec(), 1),
        Err(RowWeightError::Overflow)
    ));
    assert_eq!(bucket, before);
}

#[test]
fn bucket_codec_rejects_empty_zero_weight_and_damaged_values() {
    assert!(CollisionBucket { entries: vec![] }.encode_value().is_err());
    assert!(
        CollisionBucket {
            entries: vec![BucketEntry {
                row: vec![],
                weight: 0,
            }],
        }
        .encode_value()
        .is_err()
    );

    let mut trailing = raw_bucket(&[(b"a", 1)]);
    trailing.push(0);
    for malformed in [
        vec![],
        vec![2, 0, 0, 0, 0],
        vec![1, 0, 0, 0, 0],
        vec![1, 0, 0, 0, 1],
        vec![
            1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        raw_bucket(&[(b"a", 0)]),
        trailing,
    ] {
        assert!(CollisionBucket::decode_value(Cow::Owned(malformed)).is_err());
    }
}

fn raw_bucket(entries: &[(&[u8], u64)]) -> Vec<u8> {
    let mut encoded = vec![1];
    encoded.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for (row, weight) in entries {
        encoded.extend_from_slice(&u64::try_from(row.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(row);
        encoded.extend_from_slice(&weight.to_be_bytes());
    }
    encoded
}
