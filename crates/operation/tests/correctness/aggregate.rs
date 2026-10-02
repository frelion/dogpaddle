use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, OperationSetupError, RuntimeResource,
    operation::{
        Operation, StepBudget,
        transform::{
            AggregateCall, AggregateDefinition, AggregateDefinitionError, AggregateError,
            AggregateSchemaError,
        },
    },
};
use dogpaddle_store::{Store, StoreError, StoreSetup, Transactions};

use super::support::{
    TestStore, assert_literal_definition, construct_checked, rollback_input, run_input, step_input,
};
use dogpaddle_operation::col;

const AGGREGATE_V1: &str = include_str!("../fixtures/v1/aggregate_department.hex");
const AGGREGATE_PREFIX: &str = "operation/aggregate";

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
            ("rows", AggregateCall::CountAll),
            ("values", AggregateCall::Count(col("value"))),
            ("sum", AggregateCall::Sum(col("value"))),
            ("avg", AggregateCall::Avg(col("value"))),
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap()
}

#[test]
fn aggregate_plan_structure_and_binding_validate_separate_invariants() {
    let mut unknown_function = serde_json::to_value(definition()).unwrap();
    unknown_function["calls"][0]["call"] = serde_json::json!({"unknown":null});
    assert!(serde_json::from_str::<AggregateDefinition>(&unknown_function.to_string()).is_err());

    let mut empty_groups = serde_json::to_value(definition()).unwrap();
    empty_groups["groups"] = serde_json::json!([]);
    let plan = serde_json::from_str::<AggregateDefinition>(&empty_groups.to_string()).unwrap();
    assert!(
        OperationDefinition::from(plan)
            .output_schema(&[input_schema()])
            .is_err()
    );
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

fn construct_aggregate(
    root: &TestStore,
    definition: &(impl Clone + Into<OperationDefinition>),
) -> (Operation, Transactions) {
    construct_aggregate_for_schema(root, definition, input_schema())
}

fn construct_aggregate_for_schema(
    root: &TestStore,
    definition: &(impl Clone + Into<OperationDefinition>),
    schema: SchemaRef,
) -> (Operation, Transactions) {
    let definition: OperationDefinition = definition.clone().into();
    let mut setup = StoreSetup::new();
    let constructed = definition
        .construct(
            &[schema],
            &mut setup.data_scope().scoped(AGGREGATE_PREFIX),
            RuntimeResource::none(),
        )
        .unwrap();
    let (operation, _) = constructed.into_parts();
    let transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
    (operation, transactions)
}

fn reopen_aggregate(
    store: &Store,
    definition: &(impl Clone + Into<OperationDefinition>),
) -> Operation {
    reopen_aggregate_for_schema(store, definition, input_schema())
}

fn reopen_aggregate_for_schema(
    store: &Store,
    definition: &(impl Clone + Into<OperationDefinition>),
    schema: SchemaRef,
) -> Operation {
    let definition: OperationDefinition = definition.clone().into();
    let constructed = definition
        .construct(
            &[schema],
            &mut store.data_scope().scoped(AGGREGATE_PREFIX),
            RuntimeResource::none(),
        )
        .unwrap();
    constructed.into_parts().0
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

fn append_output(action: Option<Change>, output: &mut Vec<TraceRow>) {
    if let Some(change) = action {
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
}

fn aggregate_trace(events: &[(&str, Option<i64>, i64)], batches: &[usize]) -> Vec<TraceRow> {
    assert_eq!(batches.iter().sum::<usize>(), events.len());
    let root = TestStore::new();
    let definition = definition();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
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
            run_input(&operation, step_input(&input), &mut transactions).unwrap(),
            &mut output,
        );
        start += rows;
    }
    output
}

#[test]
fn definition_binds_schema_and_typed_setup_requires_the_stable_three_resource_layout() {
    let definition = definition();
    let decoded = assert_literal_definition(&definition, AGGREGATE_V1, 1);
    assert_eq!(
        OperationDefinition::from(definition.clone()).input_count(),
        1
    );
    let input = input_schema();
    let binding = construct_checked(&definition, std::slice::from_ref(&input)).unwrap();
    let output = binding.as_ref().unwrap();
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

    let fixture = TestStore::new();
    let mut setup = StoreSetup::new();
    let constructed = OperationDefinition::from(definition.clone())
        .construct(
            &[input_schema()],
            &mut setup.data_scope().scoped(AGGREGATE_PREFIX),
            RuntimeResource::none(),
        )
        .unwrap();
    let (operation, _) = constructed.into_parts();
    assert!(matches!(operation, Operation::Atomic(_)));
    let transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    drop((operation, transactions));

    let store = Store::open(fixture.path()).unwrap();
    let Err(error) = decoded.construct(
        &[input],
        &mut store.data_scope().scoped("operation/missing-aggregate"),
        RuntimeResource::none(),
    ) else {
        panic!("missing Aggregate state unexpectedly opened");
    };
    assert!(matches!(
        error,
        OperationSetupError::Store(StoreError::DataNotFound(name))
            if name == "operation/missing-aggregate/aggregate.groups"
    ));
}

#[test]
fn count_sum_average_and_extrema_follow_ordered_group_transitions() {
    let root = TestStore::new();
    let definition = definition();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A", "A", "A", "A"],
        &[Some(10), Some(20), None, Some(10), Some(20), None],
        &[1, 1, 1, -1, -1, -1],
    );
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
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
        [("min", AggregateCall::Min(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A"],
        &[Some(10), Some(20), Some(20)],
        &[1, 1, -1],
    );
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("Aggregate did not emit the new group");
    };
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.diffs().values(), &[1]);
}

#[test]
fn extrema_order_signed_values_by_value() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(&["A", "A", "A"], &[Some(0), Some(-10), Some(5)], &[1, 1, 1]);
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("Aggregate did not emit extrema transitions");
    };
    let minimum = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let last = output.num_rows() - 1;
    assert_eq!((minimum.value(last), maximum.value(last)), (-10, 5));

    let retract = change(&["A"], &[Some(-10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("Aggregate did not replace its minimum");
    };
    let minimum = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minimum.values(), &[-10, 0]);
    assert_eq!(maximum.values(), &[5, 5]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn extrema_preserve_byte_order_for_empty_and_prefix_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, true),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min_text", AggregateCall::Min(col("text"))),
            ("max_text", AggregateCall::Max(col("text"))),
            ("min_bytes", AggregateCall::Min(col("bytes"))),
            ("max_bytes", AggregateCall::Max(col("bytes"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A", "A"])),
            Arc::new(StringArray::from(vec![
                None,
                Some("aa"),
                Some("a"),
                Some(""),
            ])),
            Arc::new(BinaryArray::from(vec![
                None,
                Some(b"aa".as_slice()),
                Some(b"a".as_slice()),
                Some(b"".as_slice()),
            ])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, 1, 1, 1])).unwrap();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
        panic!("Aggregate did not emit byte extrema transitions");
    };
    let last = output.num_rows() - 1;
    let min_text = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let max_text = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let min_bytes = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let max_bytes = output
        .records()
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(min_text.value(last), "");
    assert_eq!(max_text.value(last), "aa");
    assert_eq!(min_bytes.value(last), b"");
    assert_eq!(max_bytes.value(last), b"aa");
}

type MixedAggregateValues<'a> = (i64, u64, i64, &'a str, u64, i64, &'a str, i64, f64, f64);

fn assert_mixed_aggregate_trace(
    output: &Change,
    expected: &[MixedAggregateValues<'_>],
    diffs: &[i64],
) {
    let records = RecordBatch::try_new(
        output.records().schema(),
        vec![
            Arc::new(StringArray::from(vec!["A"; expected.len()])),
            Arc::new(Int64Array::from(
                expected.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                expected.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                expected.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                expected.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                expected.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                expected.iter().map(|row| row.5).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                expected.iter().map(|row| row.6).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                expected.iter().map(|row| row.7).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                expected.iter().map(|row| row.8).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                expected.iter().map(|row| row.9).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    assert_eq!(output.records(), &records);
    assert_eq!(output.diffs().values(), diffs);
}

#[test]
fn mixed_argument_roles_preserve_dense_state_addresses_and_trace_across_reopen() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::UInt64, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min_a", AggregateCall::Min(col("a"))),
            ("sum_b", AggregateCall::Sum(col("b"))),
            ("count_a", AggregateCall::Count(col("a"))),
            ("max_c", AggregateCall::Max(col("c"))),
            ("min_b", AggregateCall::Min(col("b"))),
            ("sum_a", AggregateCall::Sum(col("a"))),
            ("min_c", AggregateCall::Min(col("c"))),
            ("max_a", AggregateCall::Max(col("a"))),
            ("avg_b", AggregateCall::Avg(col("b"))),
            ("avg_a", AggregateCall::Avg(col("a"))),
        ],
    )
    .unwrap();
    let make_change = |a: Vec<i64>, b: Vec<u64>, c: Vec<&str>, diffs: Vec<i64>| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["A"; a.len()])),
                    Arc::new(Int64Array::from(a)),
                    Arc::new(UInt64Array::from(b)),
                    Arc::new(StringArray::from(c)),
                ],
            )
            .unwrap(),
            Int64Array::from(diffs),
        )
        .unwrap()
    };
    let first = (10, 9, 1, "m", 9, 10, "m", 10, 9.0, 10.0);
    let second = (4, 11, 2, "z", 2, 14, "m", 10, 5.5, 7.0);
    let third = (4, 18, 3, "z", 2, 34, "a", 20, 6.0, 34.0 / 3.0);
    let remaining = (10, 16, 2, "m", 7, 30, "a", 20, 8.0, 15.0);
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let initial = make_change(
        vec![10, 4, 20],
        vec![9, 2, 7],
        vec!["m", "z", "a"],
        vec![1; 3],
    );
    let output = run_input(&operation, step_input(&initial), &mut transactions)
        .unwrap()
        .unwrap();
    assert_mixed_aggregate_trace(
        &output,
        &[first, first, second, second, third],
        &[1, -1, 1, -1, 1],
    );
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let operation = reopen_aggregate_for_schema(&store, &definition, Arc::clone(&schema));
    let mut transactions = store.into_transactions();
    let retract = make_change(vec![4, 20], vec![2, 7], vec!["z", "a"], vec![-1; 2]);
    let output = run_input(&operation, step_input(&retract), &mut transactions)
        .unwrap()
        .unwrap();
    assert_mixed_aggregate_trace(
        &output,
        &[third, remaining, remaining, first],
        &[-1, 1, -1, 1],
    );
    let remove = make_change(vec![10], vec![9], vec!["m"], vec![-1]);
    let output = run_input(&operation, step_input(&remove), &mut transactions)
        .unwrap()
        .unwrap();
    assert_mixed_aggregate_trace(&output, &[first], &[-1]);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end trace keeps interleaving, reopen, extrema refresh, and group death together"
)]
fn distinct_extrema_layouts_refresh_interleaved_groups_across_reopen() {
    const WIDE_TEXT: &str =
        "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
    const WIDE_BYTES: &[u8] =
        b"\x05mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, true),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min_text", AggregateCall::Min(col("text"))),
            ("min_bytes", AggregateCall::Min(col("bytes"))),
            ("max_text", AggregateCall::Max(col("text"))),
            ("max_bytes", AggregateCall::Max(col("bytes"))),
            ("min_text_again", AggregateCall::Min(col("text"))),
        ],
    )
    .unwrap();
    let make_change = |departments: Vec<&str>,
                       text: Vec<Option<&str>>,
                       bytes: Vec<Option<&[u8]>>,
                       diffs: Vec<i64>| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(departments)),
                    Arc::new(StringArray::from(text)),
                    Arc::new(BinaryArray::from(bytes)),
                ],
            )
            .unwrap(),
            Int64Array::from(diffs),
        )
        .unwrap()
    };

    let root = TestStore::new();
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let initial = make_change(
        vec!["A", "B", "A", "B", "A", "A"],
        vec![
            Some(WIDE_TEXT),
            Some("b"),
            Some("a"),
            Some("y"),
            Some("z"),
            None,
        ],
        vec![
            Some(WIDE_BYTES),
            Some(b"\x02"),
            Some(b"\x09"),
            Some(b"\x08"),
            Some(b"\x01"),
            None,
        ],
        vec![1; 6],
    );
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    let operation = reopen_aggregate_for_schema(&store, &decoded, Arc::clone(&schema));
    let mut transactions = store.into_transactions();
    let retract = make_change(
        vec!["A", "B", "A"],
        vec![Some("a"), Some("y"), Some("z")],
        vec![Some(b"\x09"), Some(b"\x08"), Some(b"\x01")],
        vec![-1; 3],
    );
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("Aggregate did not refresh distinct extrema layouts");
    };
    assert_eq!(output.diffs().values(), &[-1, 1, -1, 1, -1, 1]);
    let text = |column| {
        output
            .records()
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    let bytes = |column| {
        output
            .records()
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
    };
    assert_eq!(text(1).value(1), WIDE_TEXT);
    assert_eq!(text(3).value(1), "z");
    assert_eq!(bytes(2).value(1), b"\x01");
    assert_eq!(bytes(4).value(1), WIDE_BYTES);
    assert_eq!(text(5).value(1), text(1).value(1));
    assert_eq!(text(1).value(3), "b");
    assert_eq!(text(3).value(3), "b");
    assert_eq!(bytes(2).value(3), b"\x02");
    assert_eq!(bytes(4).value(3), b"\x02");
    assert_eq!(text(1).value(5), WIDE_TEXT);
    assert_eq!(text(3).value(5), WIDE_TEXT);
    assert_eq!(bytes(2).value(5), WIDE_BYTES);
    assert_eq!(bytes(4).value(5), WIDE_BYTES);
    assert_eq!(text(5).value(5), text(1).value(5));

    let remove_group = make_change(
        vec!["A", "A"],
        vec![Some(WIDE_TEXT), None],
        vec![Some(WIDE_BYTES), None],
        vec![-1, -1],
    );
    assert!(
        run_input(&operation, step_input(&remove_group), &mut transactions,)
            .unwrap()
            .is_some()
    );
    let recreate = make_change(vec!["A"], vec![Some("c")], vec![Some(b"\x03")], vec![1]);
    let Some(output) = run_input(&operation, step_input(&recreate), &mut transactions).unwrap()
    else {
        panic!("Aggregate did not recreate a removed multi-layout group");
    };
    for column in [1, 3, 5] {
        assert_eq!(
            output
                .records()
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "c"
        );
    }
    for column in [2, 4] {
        assert_eq!(
            output
                .records()
                .column(column)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"\x03"
        );
    }
}

#[test]
fn null_arguments_never_enter_the_extrema_partition() {
    let root = TestStore::new();
    let definition = definition();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[None, None], &[1, 1]);
    let Some(output) = run_input(&operation, step_input(&initial), &mut transactions).unwrap()
    else {
        panic!("Aggregate did not emit NULL-argument group transitions");
    };
    let rows = output_rows(&output);
    let last = rows.last().unwrap();
    assert_eq!(last.rows, 2);
    assert_eq!(last.values, 0);
    assert_eq!(last.min, None);
    assert_eq!(last.max, None);
}

#[test]
fn extrema_multiplicity_and_group_partitions_are_independent() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("identity", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let make_change =
        |departments: Vec<&str>, identities: Vec<i64>, values: Vec<i64>, diffs: Vec<i64>| {
            Change::try_new(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(departments)),
                        Arc::new(Int64Array::from(identities)),
                        Arc::new(Int64Array::from(values)),
                    ],
                )
                .unwrap(),
                Int64Array::from(diffs),
            )
            .unwrap()
        };

    let initial = make_change(
        vec!["A", "A", "B"],
        vec![1, 2, 3],
        vec![5, 5, 5],
        vec![1, 1, 1],
    );
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let retract_one = make_change(vec!["A"], vec![1], vec![5], vec![-1]);
    assert!(
        run_input(&operation, step_input(&retract_one), &mut transactions,)
            .unwrap()
            .is_none()
    );

    let extend_other_group = make_change(vec!["B"], vec![4], vec![7], vec![1]);
    let Some(output) = run_input(
        &operation,
        step_input(&extend_other_group),
        &mut transactions,
    )
    .unwrap() else {
        panic!("Aggregate did not update the independent group");
    };
    let departments = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let maximum = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        departments.iter().collect::<Vec<_>>(),
        [Some("B"), Some("B")]
    );
    assert_eq!(maximum.values(), &[5, 7]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn non_unit_differences_and_rebatching_preserve_the_flattened_trace() {
    let events = [
        ("A", Some(10), 2),
        ("A", Some(10), -1),
        ("A", Some(20), 1),
        ("A", Some(10), -1),
        ("A", Some(20), -1),
        ("A", Some(30), 1),
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
    assert_eq!(expected[expected.len() - 2].7, -1);
    assert_eq!(expected.last().unwrap().7, 1);
    for batches in [&[1, 5][..], &[2, 1, 3], &[1, 1, 1, 1, 1, 1]] {
        assert_eq!(aggregate_trace(&events, batches), expected);
    }
}

#[test]
fn repeated_extrema_key_refreshes_each_ordered_prefix_across_batching() {
    let events = [
        ("A", Some(10), 1),
        ("A", Some(20), 1),
        ("A", Some(10), 1),
        ("A", Some(10), -2),
        ("A", Some(10), 1),
    ];
    let expected = aggregate_trace(&events, &[events.len()]);
    let row = |rows, sum, avg, min, max, diff| {
        (
            "A".to_owned(),
            rows,
            rows,
            Some(sum),
            Some(avg),
            Some(min),
            Some(max),
            diff,
        )
    };
    assert_eq!(
        expected,
        vec![
            row(1, 10, 10.0, 10, 10, 1),
            row(1, 10, 10.0, 10, 10, -1),
            row(2, 30, 15.0, 10, 20, 1),
            row(2, 30, 15.0, 10, 20, -1),
            row(3, 40, 40.0 / 3.0, 10, 20, 1),
            row(3, 40, 40.0 / 3.0, 10, 20, -1),
            row(1, 20, 20.0, 20, 20, 1),
            row(1, 20, 20.0, 20, 20, -1),
            row(2, 30, 15.0, 10, 20, 1),
        ]
    );
    assert_eq!(aggregate_trace(&events, &[2, 1, 2]), expected);
    assert_eq!(aggregate_trace(&events, &[1, 1, 1, 1, 1]), expected);
}

#[test]
fn existing_group_net_zero_cycle_emits_ordered_updates_and_survives_reopen() {
    let root = TestStore::new();
    let definition = definition();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let cycle = change(&["A", "A"], &[Some(10), Some(10)], &[1, -1]);
    let Some(output) = run_input(&operation, step_input(&cycle), &mut transactions).unwrap() else {
        panic!("the intermediate COUNT/SUM change must emit updates");
    };
    assert_eq!(
        output_rows(&output),
        [
            a((2, 2, Some(30), Some(15.0), Some(10), Some(20), -1)),
            a((3, 3, Some(40), Some(40.0 / 3.0), Some(10), Some(20), 1)),
            a((3, 3, Some(40), Some(40.0 / 3.0), Some(10), Some(20), -1)),
            a((2, 2, Some(30), Some(15.0), Some(10), Some(20), 1)),
        ]
    );
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let operation = reopen_aggregate(&store, &definition);
    let mut transactions = store.into_transactions();
    let retract = change(&["A"], &[Some(10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("the reopened group must retain its original state");
    };
    assert_eq!(
        output_rows(&output),
        [
            a((2, 2, Some(30), Some(15.0), Some(10), Some(20), -1)),
            a((1, 1, Some(20), Some(20.0), Some(20), Some(20), 1)),
        ]
    );
}

#[test]
fn group_death_flushes_pending_extrema_before_same_change_recreation() {
    let root = TestStore::new();
    let definition = definition();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[None], &[1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    // The first value exists only in the pending extrema cache until its
    // own retraction kills the group. The final row recreates the same group
    // key with a new durable group ID in this Change.
    let replace = change(
        &["A", "A", "A", "A"],
        &[Some(10), None, Some(10), Some(20)],
        &[1, -1, -1, 1],
    );
    let Some(output) = run_input(&operation, step_input(&replace), &mut transactions).unwrap()
    else {
        panic!("group death and recreation must emit ordered transitions");
    };
    assert_eq!(
        output_rows(&output).last(),
        Some(&a((1, 1, Some(20), Some(20.0), Some(20), Some(20), 1)))
    );

    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let operation = reopen_aggregate(&store, &definition);
    let mut transactions = store.into_transactions();
    let retract = change(&["A"], &[Some(20)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("the recreated group must survive reopen");
    };
    assert_eq!(
        output_rows(&output),
        [a((1, 1, Some(20), Some(20.0), Some(20), Some(20), -1))]
    );
}

#[test]
fn empty_call_list_groups_rows_without_redundant_updates() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        std::iter::empty::<(&str, AggregateCall)>(),
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A", "A"],
        &[Some(10), Some(20), Some(10), Some(20)],
        &[2, 1, -2, -1],
    );
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
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
            ("sum", AggregateCall::Sum(col("value"))),
            ("avg", AggregateCall::Avg(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(UInt64Array::from(vec![Some(2), Some(4)])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![2, 1])).unwrap();
    let Some(output) = run_input(&operation, step_input(&input), &mut transactions).unwrap() else {
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
fn unknown_extrema_argument_rolls_back_the_whole_change() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("rows", AggregateCall::CountAll),
            ("min", AggregateCall::Min(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let invalid = change(&["B", "A"], &[Some(20), Some(11)], &[1, -1]);
    let error = rollback_input(&operation, step_input(&invalid), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ExtremaWeightUnderflow)
    ));

    let retry = change(&["B"], &[Some(30)], &[1]);
    let Some(output) = run_input(&operation, step_input(&retry), &mut transactions).unwrap() else {
        panic!("rolled-back group leaked into durable state");
    };
    assert_eq!(output.diffs().values(), &[1]);
    let minimum = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minimum.values(), &[30]);
}

#[test]
fn cached_extrema_underflow_poisons_commit_and_preserves_durable_state() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 3]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    // The first event reads Store, the second updates the pending count, and
    // the last underflows that cached prefix while the group weight stays valid.
    let invalid = change(
        &["A", "A", "A"],
        &[Some(10), Some(10), Some(10)],
        &[1, 1, -4],
    );
    let transaction = transactions.begin();
    let error = operation
        .step(
            step_input(&invalid),
            &operation.initial_resume(),
            transaction.access(),
            &mut StepBudget::new(256, 4 * 1024 * 1024),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ExtremaWeightUnderflow)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let retract = change(&["A"], &[Some(10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("the failed turn changed durable extrema counts");
    };
    let minimum = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minimum.values(), &[10, 20]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn first_extrema_event_underflow_poisons_commit_after_read_only_lookup() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("min", AggregateCall::Min(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let invalid = change(&["A"], &[Some(11)], &[-1]);
    let transaction = transactions.begin();
    let error = operation
        .step(
            step_input(&invalid),
            &operation.initial_resume(),
            transaction.access(),
            &mut StepBudget::new(256, 4 * 1024 * 1024),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ExtremaWeightUnderflow)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));

    let retract = change(&["A"], &[Some(10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("the failed turn changed durable extrema counts");
    };
    let minimum = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minimum.values(), &[10, 20]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn cached_extrema_overflow_poisons_commit() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("min", AggregateCall::Min(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(
        &["A", "A", "A"],
        &[Some(10), Some(10), Some(10)],
        &[i64::MAX, i64::MAX, 1],
    );
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    // A NULL retraction leaves room in the group count while the ordered
    // argument returns to u64::MAX inside the pending extrema cache.
    let invalid = change(
        &["A", "A", "A", "A"],
        &[Some(10), Some(10), None, Some(10)],
        &[-1, 1, -1, 1],
    );
    let transaction = transactions.begin();
    let error = operation
        .step(
            step_input(&invalid),
            &operation.initial_resume(),
            transaction.access(),
            &mut StepBudget::new(256, 4 * 1024 * 1024),
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ArithmeticOverflow)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}

#[test]
fn group_weight_underflow_rolls_back_the_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::CountAll)],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let invalid = change(&["A"], &[Some(10)], &[-2]);
    let error = rollback_input(&operation, step_input(&invalid), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::GroupWeightUnderflow)
    ));

    let retract = change(&["A"], &[Some(10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
        panic!("the failed turn leaked into durable state");
    };
    let rows = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(rows.values(), &[1]);
    assert_eq!(output.diffs().values(), &[-1]);
}

#[test]
fn retraction_of_an_unused_column_is_accepted() {
    // The weakened contract: the Aggregate no longer keeps an exact-row ledger,
    // so a retraction whose (group, extrema argument) pair is covered by other
    // rows is accepted even though that exact row was never stored. The retained
    // checks are the group row count and each extrema argument's multiplicity,
    // and the `note` column is covered by neither.
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
        Field::new("note", DataType::Utf8, false),
    ]));
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let rows = |values: Vec<i64>, notes: Vec<&str>, diffs: Vec<i64>| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["A"; values.len()])),
                    Arc::new(Int64Array::from(
                        values.into_iter().map(Some).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(notes)),
                ],
            )
            .unwrap(),
            Int64Array::from(diffs),
        )
        .unwrap()
    };

    let initial = rows(vec![10, 20], vec!["x", "y"], vec![1, 1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let covered = rows(vec![10], vec!["z"], vec![-1]);
    let Some(output) = run_input(&operation, step_input(&covered), &mut transactions).unwrap()
    else {
        panic!("Aggregate did not accept the covered retraction");
    };
    let min = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // The retracted value was the minimum, so it advances and the maximum stays.
    assert_eq!(min.values(), &[10, 20]);
    assert_eq!(max.values(), &[20, 20]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn non_null_count_underflow_rolls_back_the_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("values", AggregateCall::Count(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(
        &["A", "A", "A", "A"],
        &[None, None, None, Some(5)],
        &[1, 1, 1, 1],
    );
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();
    let retract = change(&["A"], &[Some(5)], &[-1]);
    run_input(&operation, step_input(&retract), &mut transactions).unwrap();

    // The group still holds rows, but one call's non-null count cannot go below
    // zero: that is a distinct condition from a negative group row count.
    let again = change(&["A"], &[Some(5)], &[-1]);
    let error = rollback_input(&operation, step_input(&again), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::CallWeightUnderflow)
    ));
}

#[test]
fn count_overflow_rolls_back_the_whole_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::CountAll)],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[i64::MAX]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();

    let overflow = change(&["A"], &[Some(10)], &[1]);
    let error = rollback_input(&operation, step_input(&overflow), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ArithmeticOverflow)
    ));

    let retract = change(&["A"], &[Some(10)], &[-i64::MAX]);
    let Some(output) = run_input(&operation, step_input(&retract), &mut transactions).unwrap()
    else {
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
        [("min", AggregateCall::Min(col("value")))],
    )
    .unwrap();
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 1]);
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    let operation = reopen_aggregate(&store, &decoded);
    let mut transactions = store.into_transactions();
    let retract_min = change(&["A"], &[Some(10)], &[-1]);
    let Some(output) = run_input(&operation, step_input(&retract_min), &mut transactions).unwrap()
    else {
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
fn cached_extrema_follow_duplicate_retraction_across_reopen() {
    let root = TestStore::new();
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(
        &["A", "A", "A"],
        &[Some(10), Some(10), Some(20)],
        &[1, 1, 1],
    );
    run_input(&operation, step_input(&initial), &mut transactions).unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    let operation = reopen_aggregate(&store, &decoded);
    let mut transactions = store.into_transactions();

    // A duplicate leaves: neither extreme moves, so the turn emits nothing and
    // the cached extremes must stay untouched.
    let duplicate = change(&["A"], &[Some(10)], &[-1]);
    assert!(
        run_input(&operation, step_input(&duplicate), &mut transactions)
            .unwrap()
            .is_none()
    );

    // The last copy leaves: the cached minimum must be re-read from the partition.
    let last_copy = change(&["A"], &[Some(10)], &[-1]);
    let action = run_input(&operation, step_input(&last_copy), &mut transactions).unwrap();
    let Some(output) = action else {
        panic!("reopened Aggregate did not refresh its cached minimum: {action:?}");
    };
    let minimum = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = output
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minimum.values(), &[10, 20]);
    assert_eq!(maximum.values(), &[20, 20]);
    assert_eq!(output.diffs().values(), &[-1, 1]);
}

#[test]
fn schema_binding_rejects_float_keys_float_extrema_and_uncoerced_sum() {
    assert!(matches!(
        AggregateDefinition::try_new(
            std::iter::empty::<(&str, dogpaddle_operation::Expr)>(),
            [("rows", AggregateCall::CountAll)],
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
        [("rows", AggregateCall::CountAll)],
    )
    .unwrap();
    let Err(error) = construct_checked(&float_group, std::slice::from_ref(&float_schema)) else {
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
        AggregateCall::Min(col("value")),
        AggregateCall::Sum(col("value")),
    ] {
        let definition =
            AggregateDefinition::try_new([("key", col("key"))], [("result", call)]).unwrap();
        let Err(error) = construct_checked(&definition, std::slice::from_ref(&keyed_float_schema))
        else {
            panic!("unsupported floating-point aggregate unexpectedly bound");
        };
        assert!(matches!(
            error,
            dogpaddle_operation::OperationBindError::Rejected { source }
                if matches!(source.downcast_ref::<AggregateSchemaError>(), Some(AggregateSchemaError::UnsupportedArgument { .. }))
        ));
    }
}

/// One logical aggregate output row, used as a relation key.
type RelationKey = (String, i64, Option<i64>, Option<i64>, Option<i64>);

/// Folds one emitted Change into the accumulated relation.
fn fold_relation(change: &Change, relation: &mut BTreeMap<RelationKey, i64>) {
    let departments = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rows = change
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = change
        .records()
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min = change
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max = change
        .records()
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row in 0..change.num_rows() {
        let key = (
            StringArray::value(departments, row).to_owned(),
            Int64Array::value(rows, row),
            (!Int64Array::is_null(sum, row)).then(|| Int64Array::value(sum, row)),
            (!Int64Array::is_null(min, row)).then(|| Int64Array::value(min, row)),
            (!Int64Array::is_null(max, row)).then(|| Int64Array::value(max, row)),
        );
        *relation.entry(key).or_insert(0) += change.diffs().value(row);
    }
}

/// Deterministic stream of retractions, duplicates and NULL arguments together
/// with the multiset it describes. An event is only admitted while every
/// (group, argument) multiplicity stays non-negative, which is exactly the
/// contract the Aggregate itself checks.
#[expect(
    clippy::type_complexity,
    reason = "the model is one flat multiset keyed by group and argument"
)]
fn multiset_stream() -> (
    Vec<(&'static str, Option<i64>, i64)>,
    BTreeMap<(&'static str, Option<i64>), i64>,
) {
    let departments = ["A", "B"];
    let mut model: BTreeMap<(&'static str, Option<i64>), i64> = BTreeMap::new();
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;
    let mut random = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut events = Vec::new();
    while events.len() < 400 {
        let department = departments[(random() % 2) as usize];
        let value = match random() % 4 {
            0 => None,
            other => Some(i64::try_from(other).unwrap()),
        };
        let difference = [-2, -1, 1, 2][(random() % 4) as usize];
        let next = model.get(&(department, value)).copied().unwrap_or(0) + difference;
        if next < 0 {
            continue;
        }
        model.insert((department, value), next);
        events.push((department, value, difference));
    }
    (events, model)
}

/// The relation the multiset model requires, one row per non-empty group.
fn modelled_relation(model: &BTreeMap<(&str, Option<i64>), i64>) -> BTreeMap<RelationKey, i64> {
    let mut grouped: BTreeMap<&str, Vec<(Option<i64>, i64)>> = BTreeMap::new();
    for ((department, value), weight) in model {
        if *weight > 0 {
            grouped
                .entry(*department)
                .or_default()
                .push((*value, *weight));
        }
    }
    let mut expected = BTreeMap::new();
    for (department, values) in grouped {
        let rows: i64 = values.iter().map(|(_, weight)| *weight).sum();
        let non_null: Vec<(i64, i64)> = values
            .iter()
            .filter_map(|(value, weight)| value.map(|value| (value, *weight)))
            .collect();
        let sum = (!non_null.is_empty())
            .then(|| non_null.iter().map(|(value, weight)| value * weight).sum());
        let min = non_null.iter().map(|(value, _)| *value).min();
        let max = non_null.iter().map(|(value, _)| *value).max();
        expected.insert((department.to_owned(), rows, sum, min, max), 1);
    }
    expected
}

#[test]
fn emitted_relation_matches_a_multiset_model_under_retraction() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("rows", AggregateCall::CountAll),
            ("sum", AggregateCall::Sum(col("value"))),
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);

    let (events, model) = multiset_stream();
    let mut emitted = BTreeMap::new();
    for batch in events.chunks(13) {
        let input = change(
            &batch.iter().map(|event| event.0).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.1).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.2).collect::<Vec<_>>(),
        );
        if let Some(change) = run_input(&operation, step_input(&input), &mut transactions).unwrap()
        {
            fold_relation(&change, &mut emitted);
        }
    }
    emitted.retain(|_, weight| *weight != 0);

    assert_eq!(emitted, modelled_relation(&model));
}

#[test]
fn zero_group_rejects_unretracted_statistics_and_extrema_and_preserves_reopen() {
    for calls in [
        vec![("avg", AggregateCall::Avg(col("value")))],
        vec![("min", AggregateCall::Min(col("value")))],
        vec![("count", AggregateCall::Count(col("value")))],
    ] {
        let definition =
            AggregateDefinition::try_new([("department", col("department"))], calls).unwrap();
        let root = TestStore::new();
        let (operation, mut transactions) = construct_aggregate(&root, &definition);
        let seed = change(&["A"], &[Some(10)], &[1]);
        run_input(&operation, step_input(&seed), &mut transactions).unwrap();
        let invalid = change(&["A"], &[None], &[-1]);
        let error =
            rollback_input(&operation, step_input(&invalid), &mut transactions).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<AggregateError>(),
            Some(AggregateError::InvalidState)
        ));
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        let operation = reopen_aggregate(&store, &definition);
        let mut transactions = store.into_transactions();
        let valid = change(&["A"], &[Some(10)], &[-1]);
        assert!(
            run_input(&operation, step_input(&valid), &mut transactions)
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn zero_group_rejects_a_zero_count_with_nonzero_wide_sum() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("avg", AggregateCall::Avg(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition);
    let seed = change(&["A"], &[Some(10)], &[1]);
    run_input(&operation, step_input(&seed), &mut transactions).unwrap();
    let invalid = change(&["A"], &[Some(9)], &[-1]);
    let error = rollback_input(&operation, step_input(&invalid), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::InvalidState)
    ));
}

#[test]
fn average_keeps_a_wide_sum_and_shared_sum_still_checks_every_event() {
    let average = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("avg", AggregateCall::Avg(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &average);
    let wide = change(&["A"], &[Some(i64::MAX)], &[2]);
    let output = run_input(&operation, step_input(&wide), &mut transactions)
        .unwrap()
        .unwrap();
    let averages = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    #[expect(clippy::cast_precision_loss, reason = "AVG's public output is Float64")]
    let expected = i64::MAX as f64;
    assert_eq!(averages.value(0).to_bits(), expected.to_bits());
    drop((operation, transactions));
    let store = Store::open(root.path()).unwrap();
    let operation = reopen_aggregate(&store, &average);
    let mut transactions = store.into_transactions();
    let retract = change(&["A"], &[Some(i64::MAX)], &[-2]);
    assert!(
        run_input(&operation, step_input(&retract), &mut transactions)
            .unwrap()
            .is_some()
    );

    let shared = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("avg", AggregateCall::Avg(col("value"))),
            ("sum", AggregateCall::Sum(col("value"))),
            ("count", AggregateCall::Count(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &shared);
    let transient = change(
        &["A", "A", "A"],
        &[Some(i64::MAX), Some(1), Some(1)],
        &[1, 1, -1],
    );
    let error = rollback_input(&operation, step_input(&transient), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ArithmeticOverflow)
    ));
    let valid = change(&["A"], &[Some(2)], &[1]);
    let output = run_input(&operation, step_input(&valid), &mut transactions)
        .unwrap()
        .unwrap();
    let counts = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values(), &[1]);
}

#[test]
fn aggregate_budget_failure_rolls_back_pending_state_and_does_not_consume_tail_items() {
    let root = TestStore::new();
    let (operation, mut transactions) = construct_aggregate(&root, &definition());
    let input = change(&["A"], &[Some(10)], &[1]);
    let Operation::Atomic(kernel) = &operation else {
        panic!("aggregate must be atomic")
    };
    {
        let transaction = transactions.begin();
        let mut budget = StepBudget::new(7, 32);
        let error = kernel
            .apply(step_input(&input), transaction.access(), &mut budget)
            .unwrap_err();
        assert!(error.is::<dogpaddle_operation::operation::BudgetExceeded>());
        assert_eq!(budget.head_remaining(), 7);
        drop(transaction);
    }
    let output = run_input(&operation, step_input(&input), &mut transactions)
        .unwrap()
        .unwrap();
    assert_eq!(
        output_rows(&output),
        [a((1, 1, Some(10), Some(10.0), Some(10), Some(10), 1))]
    );
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["groups"] = serde_json::json!([]);
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"aggregate": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[input_schema()],
        RuntimeResource::none(),
    );
}

#[test]
fn aggregate_call_shape_rejects_unknown_functions_and_wrong_arity() {
    for call in [
        serde_json::json!({"unknown": null}),
        serde_json::json!({"sum": []}),
        serde_json::json!({"count_all": ["argument"]}),
    ] {
        let mut payload = serde_json::to_value(definition()).unwrap();
        payload["calls"][0]["call"] = call;
        assert!(serde_json::from_str::<AggregateDefinition>(&payload.to_string()).is_err());
    }
}

#[test]
fn raw_duplicate_output_names_are_rejected_before_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["calls"][0]["name"] = payload["groups"][0]["name"].clone();
    let plan: OperationDefinition =
        serde_json::from_str(&serde_json::json!({"aggregate": payload}).to_string()).unwrap();
    crate::support::assert_rejected_plan_before_data(
        &plan,
        &[input_schema()],
        RuntimeResource::none(),
    );
}

fn list_group_metadata_records(nested: u8, child_nullable: bool, state: u8) -> RecordBatch {
    use arrow_array::{ArrayRef, ListArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};

    let tag = |name: &str, data_type, nullable| {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            "arbitrary-key".to_owned(),
            format!("metadata for {name}"),
        )]))
    };
    // Every selected parent is a nonzero-offset slice of a three-row array.
    let primitive = Arc::new(Int64Array::from(vec![99, 7, 8, 66])) as ArrayRef;
    let values: ArrayRef = match nested {
        0 => primitive,
        1 => Arc::new(ListArray::new(
            Arc::new(tag("deep-item", DataType::Int64, false)),
            OffsetBuffer::from_lengths([1, 1, 1, 1]),
            primitive,
            None,
        )),
        2 => Arc::new(StructArray::new(
            vec![tag("deep-field", DataType::Int64, false)].into(),
            vec![primitive],
            None,
        )),
        _ => unreachable!(),
    };
    let field = Arc::new(tag(
        "custom-item",
        values.data_type().clone(),
        child_nullable,
    ));
    let lengths = if state == 1 { [1, 0, 3] } else { [1, 2, 1] };
    let full = ListArray::new(
        Arc::clone(&field),
        OffsetBuffer::from_lengths(lengths),
        values,
        (state == 2).then(|| NullBuffer::from(vec![true, false, true])),
    );
    let selected = full.slice(1, 1);
    assert_eq!(selected.value_offsets()[0], 1);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![tag("g", DataType::List(field), true)],
        HashMap::from([("schema-key".to_owned(), "schema-value".to_owned())]),
    ));
    RecordBatch::try_new(schema, vec![Arc::new(selected)]).unwrap()
}

fn assert_list_group_metadata_output(
    output: Option<Change>,
    expected: &[(i64, i64)],
    expected_schema: &SchemaRef,
    nested: u8,
    state: u8,
) {
    use arrow_array::{ListArray, StructArray};

    let output = output.expect("count transition emits rows");
    assert_eq!(output.records().schema_ref(), expected_schema);
    assert_eq!(output.num_rows(), expected.len());
    let group = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let count = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for (row, &(value, difference)) in expected.iter().enumerate() {
        assert_eq!(count.value(row), value);
        assert_eq!(output.diffs().value(row), difference);
        assert_eq!(group.is_null(row), state == 2);
        if state == 2 {
            assert_eq!(group.value(row).len(), 0);
            continue;
        }
        let child = group.value(row);
        assert_eq!(child.len(), if state == 1 { 0 } else { 2 });
        if state == 1 {
            continue;
        }
        let values = match nested {
            0 => child,
            1 => {
                let list = child.as_any().downcast_ref::<ListArray>().unwrap();
                for (index, expected) in [7, 8].into_iter().enumerate() {
                    let item = list.value(index);
                    let item = item.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(item.values().as_ref(), &[expected]);
                }
                continue;
            }
            2 => Arc::clone(
                child
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0),
            ),
            _ => unreachable!(),
        };
        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[7, 8]);
    }
}

#[test]
fn list_group_fields_survive_weighted_output_rollback_and_reopen() {
    for nested in 0..3 {
        for child_nullable in [false, true] {
            for state in 0..3 {
                // nonempty, empty, NULL with hidden child values
                let records = list_group_metadata_records(nested, child_nullable, state);
                let schema = records.schema();
                let input = |difference| {
                    Change::try_new(records.clone(), Int64Array::from(vec![difference])).unwrap()
                };
                let expected_schema = Arc::new(Schema::new_with_metadata(
                    vec![
                        schema.field(0).clone(),
                        Field::new("count", DataType::Int64, false),
                    ],
                    schema.metadata().clone(),
                ));
                let check = |output: Option<Change>, expected: &[(i64, i64)]| {
                    assert_list_group_metadata_output(
                        output,
                        expected,
                        &expected_schema,
                        nested,
                        state,
                    );
                };
                let definition = AggregateDefinition::try_new(
                    [("g", col("g"))],
                    [("count", AggregateCall::CountAll)],
                )
                .unwrap();
                let root = TestStore::new();
                let (operation, mut transactions) =
                    construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
                check(
                    run_input(&operation, step_input(&input(2)), &mut transactions).unwrap(),
                    &[(2, 1)],
                );
                check(
                    rollback_input(&operation, step_input(&input(1)), &mut transactions).unwrap(),
                    &[(2, -1), (3, 1)],
                );
                drop((operation, transactions));
                let store = Store::open(root.path()).unwrap();
                let operation =
                    reopen_aggregate_for_schema(&store, &definition, Arc::clone(&schema));
                let mut transactions = store.into_transactions();
                check(
                    run_input(&operation, step_input(&input(-1)), &mut transactions).unwrap(),
                    &[(2, -1), (1, 1)],
                );
                check(
                    run_input(&operation, step_input(&input(-1)), &mut transactions).unwrap(),
                    &[(1, -1)],
                );
                // Rebirth after complete retraction proves no phantom surviving group.
                check(
                    run_input(&operation, step_input(&input(1)), &mut transactions).unwrap(),
                    &[(1, 1)],
                );
                check(
                    run_input(&operation, step_input(&input(-1)), &mut transactions).unwrap(),
                    &[(1, -1)],
                );
            }
        }
    }
}

#[test]
fn many_empty_list_group_updates_keep_single_row_offset_budget() {
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;
    const ROWS: usize = 4096;
    let child = Arc::new(Field::new("item", DataType::Int64, false));
    // Empty metadata intentionally lets the pre-fix baseline prove this valid case.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "g",
        DataType::List(Arc::clone(&child)),
        false,
    )]));
    let groups = ListArray::new(
        child,
        OffsetBuffer::from_lengths(std::iter::repeat_n(0, ROWS)),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        None,
    );
    let differences = [vec![1; ROWS / 2], vec![-1; ROWS / 2]].concat();
    let input = Change::try_new(
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(groups)]).unwrap(),
        Int64Array::from(differences.clone()),
    )
    .unwrap();
    let definition =
        AggregateDefinition::try_new([("g", col("g"))], [("count", AggregateCall::CountAll)])
            .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let Operation::Atomic(atomic) = &operation else {
        panic!("Aggregate is Atomic")
    };
    let output = {
        let transaction = transactions.begin();
        let output = atomic
            .apply(
                step_input(&input),
                transaction.access(),
                &mut StepBudget::new(0, 64 * 1024 * 1024),
            )
            .unwrap()
            .unwrap();
        transaction.commit().unwrap();
        output
    };
    assert_eq!(output.num_rows(), 2 * ROWS - 2);
    assert_eq!(output.records().schema().field(0), schema.field(0));
    let groups = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(groups.values().is_empty());
    assert!(groups.value_offsets().iter().all(|offset| *offset == 0));
    let counts = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut count = 0_i64;
    let mut row = 0;
    for difference in differences {
        if count != 0 {
            assert_eq!((counts.value(row), output.diffs().value(row)), (count, -1));
            row += 1;
        }
        count += difference;
        if count != 0 {
            assert_eq!((counts.value(row), output.diffs().value(row)), (count, 1));
            row += 1;
        }
    }
    assert_eq!((count, row), (0, output.num_rows()));
}

#[test]
fn sliced_empty_list_group_does_not_charge_unselected_parent_offsets() {
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;
    let child = Arc::new(Field::new("item", DataType::Int64, false));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "g",
        DataType::List(Arc::clone(&child)),
        false,
    )]));
    let groups = ListArray::new(
        child,
        OffsetBuffer::from_lengths(std::iter::repeat_n(0, 100_000)),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        None,
    );
    let large = Change::try_new(
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(groups)]).unwrap(),
        Int64Array::from(vec![1; 100_000]),
    )
    .unwrap();
    let input = large.try_slice(0, 64).unwrap();
    let definition =
        AggregateDefinition::try_new([("g", col("g"))], [("count", AggregateCall::CountAll)])
            .unwrap();
    let root = TestStore::new();
    let (operation, mut transactions) =
        construct_aggregate_for_schema(&root, &definition, Arc::clone(&schema));
    let Operation::Atomic(atomic) = &operation else {
        panic!("Aggregate is Atomic")
    };
    let output = {
        let transaction = transactions.begin();
        let output = atomic
            .apply(
                step_input(&input),
                transaction.access(),
                &mut StepBudget::new(0, 64 * 1024 * 1024),
            )
            .unwrap()
            .unwrap();
        transaction.commit().unwrap();
        output
    };
    assert_eq!(output.num_rows(), 127);
    assert_eq!(output.records().schema().field(0), schema.field(0));
    let groups = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(groups.values().is_empty());
    assert!(groups.value_offsets().iter().all(|offset| *offset == 0));
    let counts = output
        .records()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((counts.value(0), output.diffs().value(0)), (1, 1));
    for count in 1..64_i64 {
        let row = usize::try_from(2 * count - 1).unwrap();
        assert_eq!((counts.value(row), output.diffs().value(row)), (count, -1));
        assert_eq!(
            (counts.value(row + 1), output.diffs().value(row + 1)),
            (count + 1, 1)
        );
    }
    let retract = Change::try_new(input.records().clone(), Int64Array::from(vec![-1; 64])).unwrap();
    let output = run_input(&operation, step_input(&retract), &mut transactions)
        .unwrap()
        .unwrap();
    assert_eq!(output.num_rows(), 127);
    assert_eq!(output.diffs().value(126), -1);
}
