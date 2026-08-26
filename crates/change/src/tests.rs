use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt64Array, new_null_array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::{
    Change, ChangeError, ChangeProjection, MAX_NESTING_DEPTH, ProjectionError, SchemaError,
    validate_schema,
};

fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn event_change(events: &[(u64, i64)]) -> Change {
    let values = events.iter().map(|(value, _)| *value).collect::<Vec<_>>();
    let diffs = events.iter().map(|(_, diff)| *diff).collect::<Vec<_>>();
    let records =
        RecordBatch::try_new(simple_schema(), vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

fn events(change: &Change) -> Vec<(u64, i64)> {
    let values = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    values
        .values()
        .iter()
        .copied()
        .zip(change.diffs().values().iter().copied())
        .collect()
}

pub(crate) fn representative_change() -> Change {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(Vec::<Option<i64>>::new()),
    ]);
    let object_name = Arc::new(Field::new("name", DataType::Utf8, true));
    let object_score = Arc::new(Field::new("score", DataType::Int64, false));
    let object = StructArray::from(vec![
        (
            Arc::clone(&object_name),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        ),
        (
            Arc::clone(&object_score),
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![7, 7, 8])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(StringArray::from(vec![Some("add"), None, Some("next")])),
        Arc::new(BinaryArray::from(vec![
            Some(b"one".as_slice()),
            Some(b"two".as_slice()),
            Some(b"three".as_slice()),
        ])),
        Arc::new(items),
        Arc::new(object),
        new_null_array(&DataType::Null, 3),
    ];
    let fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("enabled", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true)
            .with_metadata(HashMap::from([("semantic".to_owned(), "label".to_owned())])),
        Field::new("payload", DataType::Binary, false),
        Field::new("items", columns[4].data_type().clone(), true),
        Field::new(
            "object",
            DataType::Struct(vec![object_name, object_score].into()),
            true,
        ),
        Field::new("nothing", DataType::Null, true),
    ];
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("source".to_owned(), "representative".to_owned())]),
    ));
    let records = RecordBatch::try_new(schema, columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap()
}

pub(crate) fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
}

fn nested_schema(depth: usize) -> Schema {
    let mut data_type = DataType::Int64;
    for index in 0..depth {
        data_type = DataType::List(Arc::new(Field::new(
            format!("item_{index}"),
            data_type,
            true,
        )));
    }
    Schema::new(vec![Field::new("root", data_type, true)])
}

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
fn change_preserves_event_order_and_allows_a_negative_prefix() {
    let expected = [(7, -1), (7, 1), (8, 2), (7, -1)];
    let change = event_change(&expected);

    assert_eq!(events(&change), expected);
}

#[test]
fn slice_preserves_a_contiguous_subsequence_and_rejects_invalid_ranges() {
    let change = event_change(&[(7, 1), (8, 1), (7, -1), (9, 1)]);
    let slice = change.try_slice(1, 2).unwrap();

    assert_eq!(events(&slice), [(8, 1), (7, -1)]);
    assert!(matches!(change.try_slice(0, 0), Err(ChangeError::Empty)));
    assert!(matches!(
        change.try_slice(3, 2),
        Err(ChangeError::SliceOutOfBounds { .. })
    ));
    assert!(matches!(
        change.try_slice(usize::MAX, 1),
        Err(ChangeError::SliceOutOfBounds { .. })
    ));
}

#[test]
fn projection_deletes_only_fields_and_shares_all_existing_arrow_buffers() {
    let change = representative_change();
    let partial_plan = ChangeProjection::try_new(change.schema(), [0, 2, 4, 5, 6]).unwrap();
    let partial = change.try_project(&partial_plan).unwrap();

    assert_eq!(
        partial
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "label", "items", "object", "nothing"]
    );
    assert_eq!(partial.schema(), partial_plan.output_schema());
    assert_eq!(partial.schema().metadata(), change.schema().metadata());
    assert_eq!(
        partial
            .schema()
            .field(1)
            .metadata()
            .get("semantic")
            .map(String::as_str),
        Some("label")
    );
    for (source, target) in [(0, 0), (2, 1), (4, 2), (5, 3), (6, 4)] {
        assert!(Arc::ptr_eq(
            change.records().column(source),
            partial.records().column(target)
        ));
    }
    assert_eq!(
        change.diffs().values().as_ptr(),
        partial.diffs().values().as_ptr()
    );

    let empty_plan = ChangeProjection::try_new(change.schema(), []).unwrap();
    let empty = change.try_project(&empty_plan).unwrap();
    assert_eq!(empty.records().num_columns(), 0);
    assert_eq!(empty.num_rows(), change.num_rows());
    assert_eq!(empty.schema().metadata(), change.schema().metadata());
    assert_eq!(empty.diffs().values(), change.diffs().values());
    assert_eq!(
        empty.diffs().values().as_ptr(),
        change.diffs().values().as_ptr()
    );

    let field_count = change.schema().fields().len();
    let full_plan = ChangeProjection::try_new(change.schema(), 0..field_count).unwrap();
    assert_eq!(full_plan.output_schema(), change.schema());
    let full = change.try_project(&full_plan).unwrap();
    assert_change_eq(&full, &change);
    for index in 0..field_count {
        assert!(Arc::ptr_eq(
            change.records().column(index),
            full.records().column(index)
        ));
    }
}

#[test]
fn projection_rejects_reordering_duplicates_bounds_and_schema_drift() {
    let change = representative_change();
    let schema = change.schema();
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [2, 0]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 2,
            current: 0
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [1, 1]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 1,
            current: 1
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [schema.fields().len()]),
        Err(ProjectionError::FieldOutOfBounds { .. })
    ));

    let mut metadata = schema.metadata().clone();
    metadata.insert("drift".to_owned(), "true".to_owned());
    let drifted = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    let drifted_plan = ChangeProjection::try_new(drifted, [0]).unwrap();
    assert!(matches!(
        change.try_project(&drifted_plan),
        Err(ProjectionError::SchemaMismatch)
    ));
}

#[test]
fn schema_rejects_duplicate_unsupported_and_reserved_shapes_deterministically() {
    let duplicate = Schema::new(vec![
        Field::new("same", DataType::Int64, false),
        Field::new("same", DataType::Int64, true),
    ]);
    assert!(matches!(
        validate_schema(&duplicate),
        Err(SchemaError::DuplicateField { scope, name })
            if scope.is_empty() && name == "same"
    ));

    let duplicate_struct = Schema::new(vec![Field::new(
        "object",
        DataType::Struct(
            vec![
                Arc::new(Field::new("same", DataType::Int64, false)),
                Arc::new(Field::new("same", DataType::Int64, true)),
            ]
            .into(),
        ),
        true,
    )]);
    assert!(matches!(
        validate_schema(&duplicate_struct),
        Err(SchemaError::DuplicateField { scope, name })
            if scope == "object" && name == "same"
    ));

    let unsupported = Schema::new(vec![Field::new("value", DataType::LargeUtf8, true)]);
    assert!(matches!(
        validate_schema(&unsupported),
        Err(SchemaError::UnsupportedType { field, .. }) if field == "value"
    ));

    let reserved_field = Schema::new(vec![Field::new("$dogpaddle.diff", DataType::Int64, false)]);
    assert!(matches!(
        validate_schema(&reserved_field),
        Err(SchemaError::ReservedFieldName { field, name })
            if field == "$dogpaddle.diff" && name == "$dogpaddle.diff"
    ));

    let reserved_field_metadata = Schema::new(vec![
        Field::new("value", DataType::Int64, false).with_metadata(HashMap::from([(
            "dogpaddle.private".to_owned(),
            "value".to_owned(),
        )])),
    ]);
    assert!(matches!(
        validate_schema(&reserved_field_metadata),
        Err(SchemaError::ReservedMetadataKey { owner, key })
            if owner == "value" && key == "dogpaddle.private"
    ));

    let reserved_schema_metadata = Schema::new_with_metadata(
        Vec::<Field>::new(),
        HashMap::from([
            ("dogpaddle.z".to_owned(), "last".to_owned()),
            ("dogpaddle.a".to_owned(), "first".to_owned()),
        ]),
    );
    assert!(matches!(
        validate_schema(&reserved_schema_metadata),
        Err(SchemaError::ReservedMetadataKey { owner, key })
            if owner == "schema" && key == "dogpaddle.a"
    ));
}

#[test]
fn schema_nesting_accepts_the_limit_and_rejects_the_next_boundary() {
    assert!(validate_schema(&nested_schema(MAX_NESTING_DEPTH)).is_ok());
    assert!(matches!(
        validate_schema(&nested_schema(MAX_NESTING_DEPTH + 1)),
        Err(SchemaError::NestingTooDeep { max_depth })
            if max_depth == MAX_NESTING_DEPTH
    ));
}
