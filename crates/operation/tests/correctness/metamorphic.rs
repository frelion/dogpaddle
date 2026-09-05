use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DataInstances, OperationDefinition, col, lit,
    operation::{
        Action, OperationInput,
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, SchemaAlignDefinition,
            SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::Store;

use super::support::{TestStore, commit_ready, stateless_operation, turn_input};

fn structural_trace(
    definition: &dyn OperationDefinition,
    port: usize,
    rows: &[(u64, u64, i64)],
    batches: &[usize],
) -> Vec<(Vec<u64>, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), rows.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::UInt64, false),
        Field::new("right", DataType::UInt64, false),
    ]));
    let input_schemas = (0..definition.kind().input_count())
        .map(|_| Arc::clone(&schema))
        .collect::<Vec<_>>();
    let data = DataInstances::new();
    let mut operation = definition
        .bind(&input_schemas)
        .unwrap()
        .materialize(data, dogpaddle_operation::RuntimeResource::none())
        .unwrap();
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut trace = Vec::new();
    let mut start = 0;

    for &batch_rows in batches {
        let batch = &rows[start..start + batch_rows];
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from_iter_values(batch.iter().map(|row| row.0))),
                Arc::new(UInt64Array::from_iter_values(batch.iter().map(|row| row.1))),
            ],
        )
        .unwrap();
        let input = Change::try_new(
            records,
            Int64Array::from_iter_values(batch.iter().map(|row| row.2)),
        )
        .unwrap();
        let Action::Complete(Some(output)) = commit_ready(
            operation.as_mut(),
            Some(OperationInput {
                port,
                change: &input,
            }),
            &mut transactions,
        )
        .unwrap() else {
            panic!("structural Operation returned the wrong action");
        };
        for row in 0..output.num_rows() {
            let values = output
                .records()
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(row)
                })
                .collect();
            trace.push((values, output.diffs().value(row)));
        }
        start += batch_rows;
    }
    trace
}

#[test]
fn project_select_and_schema_align_preserve_flattened_records_and_diffs_across_rebatching() {
    let rows = [(1, 10, 1), (2, 20, -1), (3, 30, 2), (4, 40, -2)];
    let cases: [(&str, Box<dyn OperationDefinition>); 3] = [
        ("Project", Box::new(ProjectDefinition::new([1]))),
        (
            "Select",
            Box::new(
                SelectDefinition::try_new([
                    ("right", col("right")),
                    ("next", col("left") + lit(1_u64)),
                ])
                .unwrap(),
            ),
        ),
        (
            "SchemaAlign",
            Box::new(
                SchemaAlignDefinition::try_new([
                    SchemaAlignField::try_new("right", col("right"), false).unwrap(),
                    SchemaAlignField::try_new("left", col("left"), true).unwrap(),
                ])
                .unwrap(),
            ),
        ),
    ];

    for (name, definition) in cases {
        let expected = structural_trace(definition.as_ref(), 0, &rows, &[rows.len()]);
        for batches in [&[1, 3][..], &[2, 1, 1], &[1, 1, 1, 1]] {
            assert_eq!(
                structural_trace(definition.as_ref(), 0, &rows, batches),
                expected,
                "{name} changed its flattened trace after rebatching"
            );
        }
    }
}

#[test]
fn union_all_preserves_each_port_subsequence_across_rebatching() {
    let definition = UnionAllDefinition::new(std::num::NonZeroU32::new(2).unwrap());
    let ports = [
        (0, &[(1, 10, 1), (2, 20, -1), (3, 30, 2)][..]),
        (1, &[(101, 110, -2), (102, 120, 3), (103, 130, 1)][..]),
    ];

    for (port, rows) in ports {
        let expected = structural_trace(&definition, port, rows, &[rows.len()]);
        for batches in [&[1, 2][..], &[2, 1], &[1, 1, 1]] {
            assert_eq!(
                structural_trace(&definition, port, rows, batches),
                expected,
                "UnionAll changed port {port}'s flattened subsequence after rebatching"
            );
        }
    }
}

fn predicate_change(values: &[u64], keep: &[Option<bool>], diffs: &[i64]) -> Change {
    assert_eq!(values.len(), keep.len());
    assert_eq!(values.len(), diffs.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(values.to_vec())),
            Arc::new(BooleanArray::from(keep.to_vec())),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn filter_trace(
    values: &[u64],
    keep: &[Option<bool>],
    diffs: &[i64],
    batches: &[usize],
) -> Vec<(u64, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), values.len());
    let schema = predicate_change(&values[..1], &keep[..1], &diffs[..1]).schema();
    let mut operation =
        stateless_operation(&FilterDefinition::try_new(col("keep")).unwrap(), schema);
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let input = predicate_change(
            &values[start..start + rows],
            &keep[start..start + rows],
            &diffs[start..start + rows],
        );
        match commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap()
        {
            Action::Complete(Some(change)) => {
                let values = change
                    .records()
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                output.extend(
                    values
                        .values()
                        .iter()
                        .copied()
                        .zip(change.diffs().values().iter().copied()),
                );
            }
            Action::Complete(None) => {}
            Action::Idle | Action::Commit(_) => panic!("Filter returned the wrong action"),
        }
        start += rows;
    }
    output
}

fn extend_trace(values: &[u64], diffs: &[i64], batches: &[usize]) -> Vec<(u64, Option<bool>, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), values.len());
    assert_eq!(values.len(), diffs.len());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let mut operation = stateless_operation(
        &ExtendDefinition::try_new("seven", col("value").eq(lit(7_u64))).unwrap(),
        Arc::clone(&schema),
    );
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let mut output = Vec::new();
    let mut start = 0;
    for &rows in batches {
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(
                values[start..start + rows].to_vec(),
            ))],
        )
        .unwrap();
        let input = Change::try_new(
            records,
            Int64Array::from(diffs[start..start + rows].to_vec()),
        )
        .unwrap();
        let Action::Complete(Some(change)) = commit_ready(
            operation.as_mut(),
            Some(turn_input(&input)),
            &mut transactions,
        )
        .unwrap() else {
            panic!("Extend returned the wrong action");
        };
        let derived = change
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let values = change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        output.extend(
            values
                .values()
                .iter()
                .copied()
                .zip(derived.iter())
                .zip(change.diffs().values().iter().copied())
                .map(|((value, derived), diff)| (value, derived, diff)),
        );
        start += rows;
    }
    output
}

#[test]
fn filter_and_extend_are_rebatch_invariant() {
    let values = [5, 7, 7, 9, 9, 11];
    let keep = [Some(false), Some(true), Some(true), None, None, Some(false)];
    let diffs = [1, 2, -1, 3, -2, 1];
    for batches in [&[6][..], &[2, 4], &[1, 1, 1, 1, 1, 1]] {
        assert_eq!(
            filter_trace(&values, &keep, &diffs, batches),
            [(7, 2), (7, -1)]
        );
        assert_eq!(
            extend_trace(&values, &diffs, batches),
            [
                (5, Some(false), 1),
                (7, Some(true), 2),
                (7, Some(true), -1),
                (9, Some(false), 3),
                (9, Some(false), -2),
                (11, Some(false), 1),
            ]
        );
    }
}
