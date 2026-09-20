use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU32,
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, OperationKind, OperationSetupError, RuntimeResource, decode_definition,
    encode_definition,
    operation::{
        Action, Operation,
        transform::{
            AggregateCall, AggregateDefinition, AggregateDefinitionError, AggregateError,
            AggregateSchemaError,
        },
    },
};
use dogpaddle_store::{Store, StoreError, StoreSetup, Transactions};

use super::support::{
    TestStore, assert_literal_definition, commit_ready, construct_checked, rollback_ready,
    turn_input,
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

fn construct_aggregate(
    root: &TestStore,
    definition: &dyn OperationDefinition,
) -> (Operation, Transactions) {
    construct_aggregate_for_schema(root, definition, input_schema())
}

fn construct_aggregate_for_schema(
    root: &TestStore,
    definition: &dyn OperationDefinition,
    schema: SchemaRef,
) -> (Operation, Transactions) {
    let mut setup = StoreSetup::new();
    let constructed = (definition as &dyn OperationDefinition)
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

fn reopen_aggregate(store: &Store, definition: &dyn OperationDefinition) -> Operation {
    reopen_aggregate_for_schema(store, definition, input_schema())
}

fn reopen_aggregate_for_schema(
    store: &Store,
    definition: &dyn OperationDefinition,
    schema: SchemaRef,
) -> Operation {
    let constructed = (definition as &dyn OperationDefinition)
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
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
            commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap(),
            &mut output,
        );
        start += rows;
    }
    output
}

#[test]
fn definition_binds_schema_and_typed_setup_requires_the_stable_three_resource_layout() {
    let definition = definition();
    let decoded = assert_literal_definition(
        &definition,
        AGGREGATE_V1,
        14,
        OperationKind::AtomicTransform(NonZeroU32::MIN),
    );
    assert_eq!(
        definition.kind(),
        OperationKind::AtomicTransform(NonZeroU32::MIN)
    );
    assert_eq!(definition.persistence_tag(), 14);
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
    let constructed = (&definition as &dyn OperationDefinition)
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A", "A", "A", "A"],
        &[Some(10), Some(20), None, Some(10), Some(20), None],
        &[1, 1, 1, -1, -1, -1],
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A"],
        &[Some(10), Some(20), Some(20)],
        &[1, 1, -1],
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(&["A", "A", "A"], &[Some(0), Some(-10), Some(5)], &[1, 1, 1]);
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&retract)),
        &mut transactions,
    )
    .unwrap() else {
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
            ("min_text", AggregateCall::min(col("text"))),
            ("max_text", AggregateCall::max(col("text"))),
            ("min_bytes", AggregateCall::min(col("bytes"))),
            ("max_bytes", AggregateCall::max(col("bytes"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) =
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
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
            ("min_text", AggregateCall::min(col("text"))),
            ("min_bytes", AggregateCall::min(col("bytes"))),
            ("max_text", AggregateCall::max(col("text"))),
            ("max_bytes", AggregateCall::max(col("bytes"))),
            ("min_text_again", AggregateCall::min(col("text"))),
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
    let encoded = encode_definition(&definition);
    let (mut operation, mut transactions) =
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
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&encoded).unwrap();
    let mut operation = reopen_aggregate_for_schema(&store, decoded.as_ref(), Arc::clone(&schema));
    let mut transactions = store.into_transactions();
    let retract = make_change(
        vec!["A", "B", "A"],
        vec![Some("a"), Some("y"), Some("z")],
        vec![Some(b"\x09"), Some(b"\x08"), Some(b"\x01")],
        vec![-1; 3],
    );
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&retract)),
        &mut transactions,
    )
    .unwrap() else {
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
    assert!(matches!(
        commit_ready(
            &mut operation,
            Some(turn_input(&remove_group)),
            &mut transactions,
        )
        .unwrap(),
        Action::Complete(Some(_))
    ));
    let recreate = make_change(vec!["A"], vec![Some("c")], vec![Some(b"\x03")], vec![1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&recreate)),
        &mut transactions,
    )
    .unwrap() else {
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[None, None], &[1, 1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap() else {
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
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) =
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
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let retract_one = make_change(vec!["A"], vec![1], vec![5], vec![-1]);
    assert!(matches!(
        commit_ready(
            &mut operation,
            Some(turn_input(&retract_one)),
            &mut transactions,
        )
        .unwrap(),
        Action::Complete(None)
    ));

    let extend_other_group = make_change(vec!["B"], vec![4], vec![7], vec![1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&extend_other_group)),
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let input = change(
        &["A", "A", "A", "A"],
        &[Some(10), Some(20), Some(10), Some(20)],
        &[2, 1, -2, -1],
    );
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
    let (mut operation, mut transactions) =
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
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap()
    else {
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
            ("rows", AggregateCall::count_all()),
            ("min", AggregateCall::min(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[1]);
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let invalid = change(&["B", "A"], &[Some(20), Some(11)], &[1, -1]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&invalid)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::ExtremaWeightUnderflow)
    ));

    let retry = change(&["B"], &[Some(30)], &[1]);
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(&retry)), &mut transactions).unwrap()
    else {
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
fn group_weight_underflow_rolls_back_the_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::count_all())],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[1]);
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let invalid = change(&["A"], &[Some(10)], &[-2]);
    let error = rollback_ready(
        &mut operation,
        Some(turn_input(&invalid)),
        &mut transactions,
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::GroupWeightUnderflow)
    ));

    let retract = change(&["A"], &[Some(10)], &[-1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&retract)),
        &mut transactions,
    )
    .unwrap() else {
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
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) =
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
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let covered = rows(vec![10], vec!["z"], vec![-1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
        Some(turn_input(&covered)),
        &mut transactions,
    )
    .unwrap() else {
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
        [("values", AggregateCall::count(col("value")))],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(
        &["A", "A", "A", "A"],
        &[None, None, None, Some(5)],
        &[1, 1, 1, 1],
    );
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();
    let retract = change(&["A"], &[Some(5)], &[-1]);
    commit_ready(
        &mut operation,
        Some(turn_input(&retract)),
        &mut transactions,
    )
    .unwrap();

    // The group still holds rows, but one call's non-null count cannot go below
    // zero: that is a distinct condition from a negative group row count.
    let again = change(&["A"], &[Some(5)], &[-1]);
    let error =
        rollback_ready(&mut operation, Some(turn_input(&again)), &mut transactions).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<AggregateError>(),
        Some(AggregateError::CallWeightUnderflow)
    ));
}

#[test]
fn count_overflow_rolls_back_the_whole_turn() {
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [("rows", AggregateCall::count_all())],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A"], &[Some(10)], &[i64::MAX]);
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();

    let overflow = change(&["A"], &[Some(10)], &[1]);
    let error = rollback_ready(
        &mut operation,
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
        &mut operation,
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
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(&["A", "A"], &[Some(10), Some(20)], &[1, 1]);
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&encoded).unwrap();
    let mut operation = reopen_aggregate(&store, decoded.as_ref());
    let mut transactions = store.into_transactions();
    let retract_min = change(&["A"], &[Some(10)], &[-1]);
    let Action::Complete(Some(output)) = commit_ready(
        &mut operation,
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
fn cached_extrema_follow_duplicate_retraction_across_reopen() {
    let root = TestStore::new();
    let definition = AggregateDefinition::try_new(
        [("department", col("department"))],
        [
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap();
    let encoded = encode_definition(&definition);
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);
    let initial = change(
        &["A", "A", "A"],
        &[Some(10), Some(10), Some(20)],
        &[1, 1, 1],
    );
    commit_ready(
        &mut operation,
        Some(turn_input(&initial)),
        &mut transactions,
    )
    .unwrap();
    drop((operation, transactions));

    let store = Store::open(root.path()).unwrap();
    let decoded = decode_definition(&encoded).unwrap();
    let mut operation = reopen_aggregate(&store, decoded.as_ref());
    let mut transactions = store.into_transactions();

    // A duplicate leaves: neither extreme moves, so the turn emits nothing and
    // the cached extremes must stay untouched.
    let duplicate = change(&["A"], &[Some(10)], &[-1]);
    assert!(matches!(
        commit_ready(
            &mut operation,
            Some(turn_input(&duplicate)),
            &mut transactions
        )
        .unwrap(),
        Action::Complete(None)
    ));

    // The last copy leaves: the cached minimum must be re-read from the partition.
    let last_copy = change(&["A"], &[Some(10)], &[-1]);
    let action = commit_ready(
        &mut operation,
        Some(turn_input(&last_copy)),
        &mut transactions,
    )
    .unwrap();
    let Action::Complete(Some(output)) = action else {
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
        AggregateCall::min(col("value")),
        AggregateCall::sum(col("value")),
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
            ("rows", AggregateCall::count_all()),
            ("sum", AggregateCall::sum(col("value"))),
            ("min", AggregateCall::min(col("value"))),
            ("max", AggregateCall::max(col("value"))),
        ],
    )
    .unwrap();
    let root = TestStore::new();
    let (mut operation, mut transactions) = construct_aggregate(&root, &definition);

    let (events, model) = multiset_stream();
    let mut emitted = BTreeMap::new();
    for batch in events.chunks(13) {
        let input = change(
            &batch.iter().map(|event| event.0).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.1).collect::<Vec<_>>(),
            &batch.iter().map(|event| event.2).collect::<Vec<_>>(),
        );
        match commit_ready(&mut operation, Some(turn_input(&input)), &mut transactions).unwrap() {
            Action::Complete(Some(change)) => fold_relation(&change, &mut emitted),
            Action::Complete(None) => {}
            Action::Idle | Action::Commit(_) => panic!("Aggregate returned the wrong action"),
        }
    }
    emitted.retain(|_, weight| *weight != 0);

    assert_eq!(emitted, modelled_relation(&model));
}
