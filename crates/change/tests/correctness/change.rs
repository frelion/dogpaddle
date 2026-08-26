use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use dogpaddle_change::{Change, ChangeError, decode_change, encode_change};

use super::support::{
    assert_array_buffers_shared, assert_change_eq, event_change, events, representative_change,
    simple_schema,
};

#[test]
fn change_rejects_invalid_shape_and_accepts_the_complete_diff_range() {
    let empty = RecordBatch::new_empty(simple_schema());
    assert!(matches!(
        Change::try_new(empty, Int64Array::from(Vec::<i64>::new())),
        Err(ChangeError::Empty)
    ));

    let two_rows = RecordBatch::try_new(
        simple_schema(),
        vec![Arc::new(UInt64Array::from(vec![1, 2]))],
    )
    .unwrap();
    assert!(matches!(
        Change::try_new(two_rows.clone(), Int64Array::from(vec![1])),
        Err(ChangeError::LengthMismatch {
            records: 2,
            diffs: 1
        })
    ));
    assert!(matches!(
        Change::try_new(two_rows.clone(), Int64Array::from(vec![Some(1), None])),
        Err(ChangeError::NullDiff { index: 1 })
    ));
    assert!(matches!(
        Change::try_new(two_rows, Int64Array::from(vec![i64::MIN, 0])),
        Err(ChangeError::ZeroDiff { index: 1 })
    ));

    let accepted = event_change(&[(1, i64::MIN), (2, -1), (3, 1), (4, i64::MAX)]);
    assert_eq!(accepted.diffs().values(), &[i64::MIN, -1, 1, i64::MAX]);
}

#[test]
fn change_preserves_duplicates_event_order_and_negative_prefixes() {
    let expected = [(7, -1), (7, 1), (8, 2), (7, -1)];
    assert_eq!(events(&event_change(&expected)), expected);
}

#[test]
fn into_parts_recovers_the_original_record_batch_and_differences() {
    let change = representative_change();
    let expected_records = change.records().clone();
    let expected_diffs = change.diffs().clone();
    let record_buffer = change.records().column(0).to_data().buffers()[0].as_ptr();
    let diff_buffer = change.diffs().values().as_ptr();

    let (records, diffs) = change.into_parts();

    assert_eq!(records, expected_records);
    assert_eq!(diffs, expected_diffs);
    assert_eq!(
        records.column(0).to_data().buffers()[0].as_ptr(),
        record_buffer
    );
    assert_eq!(diffs.values().as_ptr(), diff_buffer);
}

#[test]
fn slice_is_zero_copy_contiguous_owned_and_checks_its_bounds() {
    let slice = {
        let source = representative_change();
        let slice = source.try_slice(1, 2).unwrap();

        for (source, sliced) in source
            .records()
            .columns()
            .iter()
            .zip(slice.records().columns())
        {
            assert_array_buffers_shared(source.as_ref(), sliced.as_ref());
        }
        assert_eq!(
            source.diffs().values()[1..].as_ptr(),
            slice.diffs().values().as_ptr()
        );
        slice
    };

    assert_eq!(slice.num_rows(), 2);
    assert_eq!(slice.diffs().values(), &[-1, 2]);
    assert_eq!(
        slice
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[7, 8]
    );
    assert_change_eq(
        &decode_change(&encode_change(&slice).unwrap()).unwrap(),
        &slice,
    );

    assert!(matches!(slice.try_slice(0, 0), Err(ChangeError::Empty)));
    assert!(matches!(
        slice.try_slice(1, 2),
        Err(ChangeError::SliceOutOfBounds { .. })
    ));
    assert!(matches!(
        slice.try_slice(usize::MAX, 1),
        Err(ChangeError::SliceOutOfBounds { .. })
    ));
}

#[test]
fn stable_rebatching_preserves_the_flattened_event_sequence() {
    let expected = [(7, 1), (8, 1), (7, -1), (9, 2), (9, -1), (10, 3), (8, -1)];
    let batchings: &[&[usize]] = &[&[7], &[1, 6], &[3, 2, 2], &[1, 1, 1, 1, 1, 1, 1]];

    for sizes in batchings {
        let mut start = 0;
        let mut actual = Vec::new();
        for &size in *sizes {
            let end = start + size;
            let encoded = encode_change(&event_change(&expected[start..end])).unwrap();
            actual.extend(events(&decode_change(&encoded).unwrap()));
            start = end;
        }
        assert_eq!(start, expected.len());
        assert_eq!(actual, expected);
    }
}
