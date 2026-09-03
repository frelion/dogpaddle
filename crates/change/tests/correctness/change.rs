use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Int64Array, ListArray, RecordBatch, StructArray, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
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
fn change_validates_decimal128_values_against_precision_and_preserves_negative_scale() {
    let valid = Decimal128Array::from(vec![Some(-99), Some(99), None])
        .with_precision_and_scale(2, -2)
        .unwrap();
    assert!(decimal_change(Arc::new(valid)).is_ok());

    for value in [-100, 100] {
        let invalid = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(2, -2)
            .unwrap();
        assert!(matches!(
            decimal_change(Arc::new(invalid)),
            Err(ChangeError::InvalidDecimal128Value {
                ref field,
                index: 0,
                value: actual,
                precision: 2,
                scale: -2,
            }) if field == "amount" && actual == value
        ));
    }
}

#[test]
fn decimal128_validation_uses_the_local_slice_and_physical_child_validity() {
    let source = Arc::new(
        Decimal128Array::from(vec![Some(100), Some(-99), Some(99), Some(-100)])
            .with_precision_and_scale(2, 0)
            .unwrap(),
    ) as ArrayRef;
    assert!(decimal_change(source.slice(1, 2)).is_ok());
    assert!(matches!(
        decimal_change(source.slice(1, 3)),
        Err(ChangeError::InvalidDecimal128Value {
            ref field,
            index: 2,
            value: -100,
            precision: 2,
            scale: 0,
        }) if field == "amount"
    ));

    let item = Arc::new(Field::new("item", DataType::Decimal128(2, 0), false));
    let list_source = Arc::new(ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2, 3, 4])),
        Arc::new(
            Decimal128Array::from(vec![Some(100), Some(1), Some(2), Some(-100)])
                .with_precision_and_scale(2, 0)
                .unwrap(),
        ),
        None,
    )) as ArrayRef;
    assert!(single_column_change("amounts", list_source.slice(1, 2)).is_ok());
    assert!(matches!(
        single_column_change("amounts", list_source.slice(1, 3)),
        Err(ChangeError::InvalidDecimal128Value {
            ref field,
            index: 2,
            value: -100,
            precision: 2,
            scale: 0,
        }) if field == "amounts.item"
    ));

    let list = ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
        Arc::new(
            Decimal128Array::from(vec![Some(100), Some(1)])
                .with_precision_and_scale(2, 0)
                .unwrap(),
        ),
        Some(NullBuffer::from(vec![false, true])),
    );
    let list_schema = Arc::new(Schema::new(vec![Field::new(
        "amounts",
        list.data_type().clone(),
        true,
    )]));
    let list_records = RecordBatch::try_new(list_schema, vec![Arc::new(list)]).unwrap();
    assert!(matches!(
        Change::try_new(list_records, Int64Array::from(vec![1, 1])),
        Err(ChangeError::InvalidDecimal128Value {
            ref field,
            index: 0,
            value: 100,
            precision: 2,
            scale: 0,
        }) if field == "amounts.item"
    ));

    let amount = Arc::new(Field::new("amount", DataType::Decimal128(2, 0), false));
    let object = StructArray::new(
        vec![Arc::clone(&amount)].into(),
        vec![Arc::new(
            Decimal128Array::from(vec![Some(100), Some(1)])
                .with_precision_and_scale(2, 0)
                .unwrap(),
        )],
        Some(NullBuffer::from(vec![false, true])),
    );
    let object_schema = Arc::new(Schema::new(vec![Field::new(
        "object",
        object.data_type().clone(),
        true,
    )]));
    let object_records = RecordBatch::try_new(object_schema, vec![Arc::new(object)]).unwrap();
    assert!(matches!(
        Change::try_new(object_records, Int64Array::from(vec![1, 1])),
        Err(ChangeError::InvalidDecimal128Value {
            ref field,
            index: 0,
            value: 100,
            precision: 2,
            scale: 0,
        }) if field == "object.amount"
    ));
}

#[test]
fn slice_and_into_parts_are_zero_copy_contiguous_owned_and_check_bounds() {
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

    let columns = slice.records().columns().to_vec();
    let diffs = slice.diffs().values().as_ptr();
    let (records, extracted_diffs) = slice.into_parts();
    for (before, after) in columns.iter().zip(records.columns()) {
        assert_array_buffers_shared(before.as_ref(), after.as_ref());
    }
    assert_eq!(diffs, extracted_diffs.values().as_ptr());
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

fn decimal_change(array: ArrayRef) -> Result<Change, ChangeError> {
    single_column_change("amount", array)
}

fn single_column_change(name: &str, array: ArrayRef) -> Result<Change, ChangeError> {
    let rows = array.len();
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    let records = RecordBatch::try_new(schema, vec![array]).unwrap();
    Change::try_new(records, Int64Array::from(vec![1; rows]))
}
