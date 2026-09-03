use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use dogpaddle_change::{MAX_NESTING_DEPTH, SchemaError, validate_schema};

use super::support::nested_schema;

#[test]
fn schema_rejects_duplicate_fields_at_their_exact_scope() {
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
}

#[test]
fn schema_rejects_representative_types_outside_the_v1_subset() {
    let unsupported = [
        DataType::LargeUtf8,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
        DataType::Date64,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];

    for data_type in unsupported {
        let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
        assert!(matches!(
            validate_schema(&schema),
            Err(SchemaError::UnsupportedType {
                field,
                data_type: rejected
            }) if field == "value" && rejected == data_type
        ));
    }
}

#[test]
fn schema_accepts_date32_all_timestamp_units_and_valid_decimal128_parameters() {
    let supported = [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("+08:00".into())),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Los_Angeles".into())),
        DataType::Decimal128(1, 0),
        DataType::Decimal128(38, 38),
        DataType::Decimal128(38, -128),
    ];

    for data_type in supported {
        let schema = Schema::new(vec![Field::new("value", data_type, true)]);
        assert_eq!(validate_schema(&schema), Ok(()));
    }
}

#[test]
fn schema_rejects_invalid_decimal128_parameters_at_the_exact_nested_path() {
    for (precision, scale) in [(0, 0), (39, 0), (2, 3)] {
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(precision, scale),
                true,
            ))),
            true,
        )]);
        assert!(matches!(
            validate_schema(&schema),
            Err(SchemaError::InvalidDecimal128 {
                field,
                precision: rejected_precision,
                scale: rejected_scale,
            }) if field == "items.item"
                && rejected_precision == precision
                && rejected_scale == scale
        ));
    }
}

#[test]
fn schema_rejects_an_empty_timestamp_timezone_without_normalizing_identity() {
    let schema = Schema::new(vec![Field::new(
        "occurred_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
        true,
    )]);
    assert!(matches!(
        validate_schema(&schema),
        Err(SchemaError::EmptyTimestampTimezone { field }) if field == "occurred_at"
    ));
}

#[test]
fn reserved_names_and_metadata_report_deterministic_nested_paths() {
    let reserved_field = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new(
            "$dogpaddle.item",
            DataType::Int64,
            true,
        ))),
        true,
    )]);
    assert!(matches!(
        validate_schema(&reserved_field),
        Err(SchemaError::ReservedFieldName { field, name })
            if field == "items.$dogpaddle.item" && name == "$dogpaddle.item"
    ));

    let nested_metadata = Schema::new(vec![Field::new(
        "object",
        DataType::Struct(
            vec![Arc::new(
                Field::new("value", DataType::Int64, false).with_metadata(HashMap::from([(
                    "dogpaddle.private".to_owned(),
                    "value".to_owned(),
                )])),
            )]
            .into(),
        ),
        true,
    )]);
    assert!(matches!(
        validate_schema(&nested_metadata),
        Err(SchemaError::ReservedMetadataKey { owner, key })
            if owner == "object.value" && key == "dogpaddle.private"
    ));

    let schema_metadata = Schema::new_with_metadata(
        Vec::<Field>::new(),
        HashMap::from([
            ("dogpaddle.z".to_owned(), "last".to_owned()),
            ("dogpaddle.a".to_owned(), "first".to_owned()),
        ]),
    );
    assert!(matches!(
        validate_schema(&schema_metadata),
        Err(SchemaError::ReservedMetadataKey { owner, key })
            if owner == "schema" && key == "dogpaddle.a"
    ));
}

#[test]
fn ordinary_schema_field_and_nested_metadata_are_valid() {
    let child = Arc::new(
        Field::new("item", DataType::Utf8, true)
            .with_metadata(HashMap::from([("unit".to_owned(), "text".to_owned())])),
    );
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("items", DataType::List(child), true)
                .with_metadata(HashMap::from([("container".to_owned(), "list".to_owned())])),
        ],
        HashMap::from([("source".to_owned(), "test".to_owned())]),
    );
    assert_eq!(validate_schema(&schema), Ok(()));
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
