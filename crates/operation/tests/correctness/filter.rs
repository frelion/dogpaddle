use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array,
    ListArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray, UInt64Array,
    types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    Expr, ExpressionError, OperationKind, Operator, ScalarValue, col, decode_definition, lit,
    operation::{
        Action, OperationInput,
        transform::{FilterDefinition, FilterError},
    },
};
use dogpaddle_store::Store;

use super::support::{
    TestStore, assert_literal_definition, bind, change, change_with_field_name, commit_ready,
    data_names, decode_hex, rollback_ready, roundtripped_output, stateless_operation,
    temporal_and_decimal_change, turn_input, value_schema,
};

const FILTER_V1: &str = include_str!("../fixtures/v1/filter_complex_expression.hex");

fn binary(operator: Operator, left: Expr, right: Expr) -> Expr {
    match operator {
        Operator::Eq => left.eq(right),
        Operator::NotEq => left.not_eq(right),
        Operator::And => left.and(right),
        Operator::Or => left.or(right),
        _ => panic!("test helper does not support {operator}"),
    }
}

fn complex_predicate() -> Expr {
    let uint_match = binary(Operator::Eq, col("value"), lit(7_u64));
    let signed_null = binary(Operator::Eq, lit(-2_i64), lit(ScalarValue::Int64(None))).is_null();
    let utf8_null = binary(Operator::NotEq, lit("x"), lit(ScalarValue::Utf8(None))).is_null();
    let boolean_null = binary(Operator::Eq, lit(true), lit(ScalarValue::Boolean(None))).is_null();
    let nullable_or = binary(Operator::Or, lit(ScalarValue::Boolean(None)), lit(false)).is_null();
    let known_true = !lit(false);
    let uint_null = lit(ScalarValue::UInt64(None)).is_null();
    [
        signed_null,
        utf8_null,
        boolean_null,
        nullable_or,
        known_true,
        uint_null,
    ]
    .into_iter()
    .fold(uint_match, |left, right| binary(Operator::And, left, right))
}

#[test]
fn literal_definition_reconstructs_predicate_binding_and_runtime() {
    let input = value_schema();
    let predicate = complex_predicate();
    let definition = FilterDefinition::try_new(predicate.clone()).unwrap();
    let decoded = assert_literal_definition(
        &definition,
        FILTER_V1,
        5,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert_eq!(definition.predicate(), &predicate);
    assert!(data_names(&definition).is_empty());
    assert_eq!(
        bind(decoded.as_ref(), std::slice::from_ref(&input))
            .unwrap()
            .output_schema(),
        Some(&input)
    );

    let records = RecordBatch::try_new(
        Arc::clone(&input),
        vec![Arc::new(UInt64Array::from(vec![7, 8, 7]))],
    )
    .unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let mut operation = stateless_operation(decoded.as_ref(), Arc::clone(&input));
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(filtered)) =
        commit_ready(&mut operation, Some(turn_input(&change)), &mut transactions).unwrap()
    else {
        panic!("decoded complex Filter did not emit its expected rows");
    };
    let values = filtered
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.values(), &[7, 7]);
    assert_eq!(filtered.diffs().values(), &[1, 2]);

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&decode_hex(FILTER_V1)).unwrap();
    let mut operation = stateless_operation(decoded.as_ref(), input);
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(reopened_filtered)) =
        commit_ready(&mut operation, Some(turn_input(&change)), &mut transactions).unwrap()
    else {
        panic!("reopened Filter did not emit its expected rows");
    };
    let values = reopened_filtered
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.values(), &[7, 7]);
    assert_eq!(reopened_filtered.diffs().values(), &[1, 2]);
}

#[test]
fn runtime_rejects_invalid_port_and_schema_drift() {
    let input = change(&[1]);
    let mut operation = stateless_operation(
        &FilterDefinition::try_new(lit(true)).unwrap(),
        input.schema(),
    );
    let root = TestStore::new();
    let store = Store::create(root.path()).unwrap();
    let mut transactions = store.into_transactions();
    let error = rollback_ready(
        &mut operation,
        Some(OperationInput {
            port: 1,
            change: &input,
        }),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::InvalidInputPort { port: 1 })
    ));

    let drifted = change_with_field_name("renamed", &[1]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&drifted)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterError>(),
        Some(FilterError::Expression(ExpressionError::SchemaMismatch))
    ));
}

#[test]
fn filter_keeps_only_true_rows_with_the_same_order_records_and_diffs() {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        Some(vec![Some(2)]),
        None,
        Some(vec![Some(4), Some(5)]),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30, 40])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(StringArray::from(vec![
                Some("ten"),
                Some("twenty"),
                None,
                Some("forty"),
            ])),
            Arc::new(items),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2])).unwrap();
    let mut operation = stateless_operation(
        &FilterDefinition::try_new(col("keep")).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("Filter did not complete with a partial output Change");
    };
    assert_eq!(output.schema(), schema);
    assert_eq!(output.diffs().values(), &[1, -2]);
    let ids = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[10, 40]);
    let labels = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        labels.iter().collect::<Vec<_>>(),
        [Some("ten"), Some("forty")]
    );
    let items = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(1), None]
    );
    assert_eq!(
        items
            .value(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[4, 5]
    );
}

#[test]
fn filter_partially_selects_null_binary_and_struct_columns() {
    let score_field = Arc::new(Field::new("score", DataType::Int64, true));
    let object = StructArray::from(vec![(
        Arc::clone(&score_field),
        Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(40)])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Boolean, true),
        Field::new("nothing", DataType::Null, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("object", DataType::Struct(vec![score_field].into()), false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            arrow_array::new_null_array(&DataType::Null, 4),
            Arc::new(BinaryArray::from(vec![
                Some(b"ten".as_slice()),
                Some(b"twenty".as_slice()),
                None,
                Some(b"forty".as_slice()),
            ])),
            Arc::new(object),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 2, 3, 4])).unwrap();
    let mut operation = stateless_operation(
        &FilterDefinition::try_new(col("keep")).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("Filter did not produce its partial heterogeneous output");
    };
    assert_eq!(output.schema(), schema);
    assert_eq!(output.diffs().values(), &[1, 4]);
    assert_eq!(output.records().column(1).logical_null_count(), 2);
    let payload = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), b"ten");
    assert_eq!(payload.value(1), b"forty");
    let object = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let scores = object
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[10, 40]);
}

#[test]
fn filter_all_true_is_zero_copy_and_all_false_or_null_completes_without_output() {
    let input = change(&[1, -1, 2]);
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut all_true = stateless_operation(
        &FilterDefinition::try_new(lit(true)).unwrap(),
        input.schema(),
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut all_true, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
        panic!("all-true Filter did not retain its complete input");
    };
    assert!(Arc::ptr_eq(
        output.records().column(0),
        input.records().column(0)
    ));
    assert_eq!(
        output.diffs().values().as_ptr(),
        input.diffs().values().as_ptr()
    );

    for predicate in [lit(false), lit(ScalarValue::Boolean(None))] {
        let mut operation = stateless_operation(
            &FilterDefinition::try_new(predicate).unwrap(),
            input.schema(),
        );
        assert!(matches!(
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions,).unwrap(),
            Action::Complete(None)
        ));
    }
}

#[test]
fn temporal_and_decimal_filter_comparisons_preserve_selected_order_after_codec_roundtrip() {
    let input = temporal_and_decimal_change();
    let predicate = col("date")
        .gt_eq(lit(ScalarValue::Date32(Some(0))))
        .and(col("occurred_at").lt(lit(ScalarValue::TimestampMillisecond(Some(3_000), None))))
        .and(col("amount").not_eq(lit(ScalarValue::Decimal128(Some(300), 10, 2))));
    let filter_definition = FilterDefinition::try_new(predicate).unwrap();
    let filtered = roundtripped_output(&filter_definition, &input);
    assert_eq!(filtered.diffs().values(), &[1, -2]);
    let dates = filtered
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.values(), &[0, 3]);
    let times = filtered
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(times.values(), &[1_000, 2_500]);
    let amounts = filtered
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[100, 400]);
}
