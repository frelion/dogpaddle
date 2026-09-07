use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, MaterializeError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition, encode_definition,
    operation::{
        Action, Operation,
        transform::{
            AggregateCall, AggregateDefinition, AggregateDefinitionError, AggregateError,
            AggregateSchemaError,
        },
    },
};
use dogpaddle_store::{Store, Transactions};

use super::support::{
    TestStore, assert_literal_definition, bind, commit_ready, data_names, materialize,
    rollback_ready, turn_input,
};
use dogpaddle_operation::col;

const AGGREGATE_V1: &str = include_str!("../fixtures/v1/aggregate_department.hex");

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("department", DataType::Utf8, false).with_metadata(HashMap::from([(
                "source".to_owned(),
                "departments.name".to_owned(),
            )])),
            Field::new("value", DataType::Int64, true),
        ],
        HashMap::from([("owner".to_owned(), "aggregate-test".to_owned())]),
    ))
}

fn definition() -> AggregateDefinition {
    AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("rows", AggregateCall::count_all()),
            ("values", AggregateCall::count(col("value"))),
            ("sum", AggregateCall::sum(col("value"))),
            ("avg", AggregateCall::avg(col("value"))),
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap()
}

fn change(departments: &[&str], values: &[Option<i64>], diffs: &[i64]) -> Change {
    assert_eq!(departments.len(), values.len());
    assert_eq!(values.len(), diffs.len());
    let records = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(departments.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn create_operation(
    root: &TestStore,
    definition: &dyn OperationDefinition,
) -> (Box<dyn Operation>, Transactions) {
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition
        .data()
        .iter()
        .zip(["groups", "entries", "control"])
    {
        declaration.create(&mut store, physical).unwrap();
    }
    let operation = materialize(
        definition,
        &[input_schema()],
        &store,
        &["groups", "entries", "control"],
    );
    (operation, store.into_transactions())
}

#[derive(Debug, PartialEq)]
struct OutputRow<'a> {
    department: &'a str,
    rows: i64,
    values: i64,
    sum: Option<i64>,
    avg: Option<f64>,
    min: Option<i64>,
    max: Option<i64>,
    diff: i64,
}

type OutputValues = (
    i64,
    i64,
    Option<i64>,
    Option<f64>,
    Option<i64>,
    Option<i64>,
    i64,
);

fn a((rows, values, sum, avg, min, max, diff): OutputValues) -> OutputRow<'static> {
    OutputRow {
        department: "A",
        rows,
        values,
        sum,
        avg,
        min,
        max,
        diff,
    }
}

fn output_rows(output: &Change) -> Vec<OutputRow<'_>> {
    let departments = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rows = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg = output
        .records()
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let min = output
        .records()
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max = output
        .records()
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..output.num_rows())
        .map(|row| OutputRow {
            department: StringArray::value(departments, row),
            rows: Int64Array::value(rows, row),
            values: Int64Array::value(values, row),
            sum: (!Int64Array::is_null(sum, row)).then(|| Int64Array::value(sum, row)),
            avg: (!Float64Array::is_null(avg, row)).then(|| Float64Array::value(avg, row)),
            min: (!Int64Array::is_null(min, row)).then(|| Int64Array::value(min, row)),
            max: (!Int64Array::is_null(max, row)).then(|| Int64Array::value(max, row)),
            diff: output.diffs().value(row),
        })
        .collect()
}

type TraceRow = (
    String,
    i64,
    i64,
    Option<i64>,
    Option<f64>,
    Option<i64>,
    Option<i64>,
    i64,
);

fn append_output(action: Action, output: &mut Vec<TraceRow>) {
    match action {
        Action::Complete(Some(change)) => {
            output.extend(output_rows(&change).into_iter().map(|row| {
                (
                    row.department.to_owned(),
                    row.rows,
                    row.values,
                    row.sum,
                    row.avg,
                    row.min,
                    row.max,
                    row.diff,
                )
            }));
        }
        Action::Complete(None) => {}
        Action::Idle | Action::Commit(_) => panic!("Aggregate returned the wrong action"),
    }
}

fn aggregate_trace(events: &[(&str, Option<i64>, i64)], batches: &[usize]) -> Vec<TraceRow> {
    assert_eq!(batches.iter().sum::<usize>(), events.len());
    let root = TestStore::new();
    let definition = definition();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let batch = &events[start..start + rows];
        let input = change(
            &batch.iter().map(|event| event.0).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.1).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.2).collect::<Vec<_>>(),
        );
        append_output(
            commit_ready(
                operation.as_mut(),
                Some(turn_input(&input)),
                &mut transactions,
            )
            .unwrap(),
            &mut output,
        );
        start += rows;
    }
    output
}

#[test]
fn definition_binds_one_operation_and_three_private_data_objects() {
    let definition = definition();
    let decoded = assert_literal_definition(
        &definition,
        AGGREGATE_V1,
        14,
        OperationKind::Transform(NonZeroU32::MIN),
    );
    assert_eq!(definition.kind(), OperationKind::Transform(NonZeroU32::MIN));
    assert_eq!(definition.persistence_tag(), 14);
    assert_eq!(
        data_names(&definition),
        ["aggregate.groups", "aggregate.entries", "aggregate.control"]
    );

    let input = input_schema();
    let binding = bind(&definition, std::slice::from_ref(&input)).unwrap();
    let output = binding.output_schema().unwrap();
    assert_eq!(output.metadata(), input.metadata());
    assert_eq!(output.fields().len(), 7);
    assert_eq!(output.field(0), input.field(0));
    assert_eq!(output.field(1), &Field::new("rows", DataType::Int64, false));
    assert_eq!(
        output.field(2),
        &Field::new("values", DataType::Int64, false)
    );
    for field in &output.fields()[3..] {
        assert!(field.is_nullable());
    }

    let result = bind(decoded.as_ref(), &[input])
        .unwrap()
        .materialize(DataInstances::new(), RuntimeResource::none());
    assert!(matches!(
        result,
        Err(MaterializeError::MissingData {
            name: "aggregate.groups"
        })
    ));
}

#[test]
fn count_sum_average_and_indexed_extrema_follow_ordered_group_transitions() {
    let root = TestStore::new();
    let definition = definition();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let input = change(
        &["A", "A", "A", "A", "A", "A"],
        &[Some(10), Some(20), None, Some(10), Some(20), None],
        &[1, 1, 1, -1, -1, -1],
    );
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Aggregate did not emit its group transitions");
    };

    assert_eq!(
        output_rows(&output),
        [
            a((1, 1, Some(10), Some(10.0), Some(10), Some(10), 1)),
            a((1, 1, Some(10), Some(10.0), Some(10), Some(10), -1)),
            a((2, 2, Some(30), Some(15.0), Some(10), Some(20), 1)),
            a((2, 2, Some(30), Some(15.0), Some(10), Some(20), -1)),
            a((3, 2, Some(30), Some(15.0), Some(10), Some(20), 1)),
            a((3, 2, Some(30), Some(15.0), Some(10), Some(20), -1)),
            a((2, 1, Some(20), Some(20.0), Some(20), Some(20), 1)),
            a((2, 1, Some(20), Some(20.0), Some(20), Some(20), -1)),
            a((1, 0, None, None, None, None, 1)),
            a((1, 0, None, None, None, None, -1)),
        ]
    );
}

#[test]
fn unchanged_extrema_do_not_emit_redundant_rows() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("min", AggregateCall::min(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let input = change(
        &["A", "A", "A"],
        &[Some(10), Some(20), Some(20)],
        &[1, 1, -1],
    );
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Aggregate did not emit the new group");
    };
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.diffs().values(), &[1]);
}

#[test]
fn non_unit_differences_and_rebatching_preserve_the_flattened_trace() {
    let events = [
        ("A", Some(10), 2),
        ("A", Some(10), -1),
        ("A", Some(20), 1),
        ("A", Some(10), -1),
        ("A", Some(20), -1),
    ];
    let expected = aggregate_trace(&events, &[events.len()]);
    assert_eq!(
        expected.first(),
        Some(&(
            "A".to_owned(),
            2,
            2,
            Some(20),
            Some(10.0),
            Some(10),
            Some(10),
            1,
        ))
    );
    assert_eq!(expected.last().unwrap().7, -1);
    for batches in [&[1, 4][..], &[2, 1, 2], &[1, 1, 1, 1, 1]] {
        assert_eq!(aggregate_trace(&events, batches), expected);
    }
}

#[test]
fn empty_call_list_groups_rows_without_redundant_updates() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        std::iter::empty::<(&str, AggregateCall)>(),
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let input = change(
        &["A", "A", "A", "A"],
        &[Some(10), Some(20), Some(10), Some(20)],
        &[2, 1, -2, -1],
    );
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("group-only Aggregate did not emit presence transitions");
    };
    let groups = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(groups.iter().collect::<Vec<_>>(), [Some("A"), Some("A")]);
    assert_eq!(output.diffs().values(), &[1, -1]);
}

#[test]
fn unsigned_sum_and_average_use_input_multiplicity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, true),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("sum", AggregateCall::sum(col("value"))),
            ("avg", AggregateCall::avg(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let mut store = Store::create(root.path()).unwrap();
    for (declaration, physical) in definition
        .data()
        .iter()
        .zip(["groups", "entries", "control"])
    {
        declaration.create(&mut store, physical).unwrap();
    }
    let mut operation = materialize(
        &definition,
        std::slice::from_ref(&schema),
        &store,
        &["groups", "entries", "control"],
    );
    let mut transactions = store.into_transactions();
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(UInt64Array::from(vec![Some(2), Some(4)])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![2, 1])).unwrap();
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("unsigned Aggregate did not emit output");
    };
    let sums = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let averages = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(sums.value(output.num_rows() - 1), 8);
    assert_eq!(
        averages.value(output.num_rows() - 1).to_bits(),
        (8.0_f64 / 3.0).to_bits()
    );
}

#[test]
fn exact_row_admission_rolls_back_the_whole_change() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::count_all())],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[1]);
    commit_ready(
        operation.as_mut(),
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let invalid = change(&["B", "A"], &[Some(20), Some(11)], &[1, -1]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(turn_input(&invalid)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::NegativeWeight)
    ));

    let retry = change(&["B"], &[Some(20)], &[1]);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&retry)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("rolled-back group leaked into durable state");
    };
    assert_eq!(output.diffs().values(), &[1]);
}

#[test]
fn count_overflow_rolls_back_the_whole_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::count_all())],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[i64::MAX]);
    commit_ready(
        operation.as_mut(),
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let overflow = change(&["A"], &[Some(10)], &[1]);
    let error = rollback_ready(
        operation.as_mut(),
        Some(turn_input(&overflow)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ArithmeticOverflow)
    ));

    let retract = change(&["A"], &[Some(10)], &[-i64::MAX]);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&retract)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("rolled-back overflow changed durable group weight");
    };
    let rows = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(rows.values(), &[i64::MAX]);
    assert_eq!(output.diffs().values(), &[-1]);
}

#[test]
fn decoded_definition_reopens_group_and_index_state() {
    let root = TestStore::new();
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("min", AggregateCall::min(col("value")))],
    )
    .unwrap();
    let encoded = encode_definition(&definition);
    let (mut operation, mut transactions) = create_operation(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 1]);
    commit_ready(
        operation.as_mut(),
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&encoded).unwrap();
    let mut operation = materialize(
        decoded.as_ref(),
        &[input_schema()],
        &store,
        &["groups", "entries", "control"],
    );
    let mut transactions = store.into_transactions();
    let retract_min = change(&["A"], &[Some(10)], &[-1]);
    let Action::Complete(Some(output)) = commit_ready(
        operation.as_mut(),
        Some(turn_input(&retract_min)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("reopened Aggregate did not replace its minimum");
    };
    let values = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[10, 20]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn schema_binding_rejects_float_keys_float_extrema_and_uncoerced_sum() {
    assert!(matches!(
        AggregateDefinition::try_new(
            std::iter::empty::<(&str, dogpaddle_operation::Expr)>(),
            [("rows", AggregateCall::count_all())],
        ),
        Err(AggregateDefinitionError::EmptyGroupBy)
    ));

    let float_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float32,
        false,
    )]));
    let float_group = AggregateDefinition::try_new(
        [("value", col("value"))],
        [("rows", AggregateCall::count_all())],
    )
    .unwrap();
    let Err(error) = bind(&float_group, std::slice::from_ref(&float_schema)) else {
        panic!("floating-point GROUP BY unexpectedly bound");
    };
    assert!(matches!(
        error,
        dogpaddle_operation::OperationBindError::Rejected { source }
            if matches!(source.downcast_ref::<AggregateSchemaError>(), Some(AggregateSchemaError::FloatGroupKey { .. }))
    ));

    let keyed_float_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Float32, false),
    ]));
    for call in [
        AggregateCall::min(col("value")),
        AggregateCall::sum(col("value")),
    ] {
        let definition =
            AggregateDefinition::try_new([("key", col("key"))], [("result", call)]).unwrap();
        let Err(error) = bind(&definition, std::slice::from_ref(&keyed_float_schema)) else {
            panic!("unsupported floating-point aggregate unexpectedly bound");
        };
        assert!(matches!(
            error,
            dogpaddle_operation::OperationBindError::Rejected { source }
                if matches!(source.downcast_ref::<AggregateSchemaError>(), Some(AggregateSchemaError::UnsupportedArgument { .. }))
        ));
    }
}
