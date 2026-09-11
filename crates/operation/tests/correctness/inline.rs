use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_expr::placeholder;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    DefinitionCodecError, InlineBindError, InlineEligibilityError, InlineOperationDefinition,
    OperationDefinition, col, decode_inline_definition, encode_definition,
    encode_inline_definition,
    operation::{
        Action,
        sink::DiscardDefinition,
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, SchemaAlignDefinition,
            SchemaAlignField, SelectDefinition, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::Store;

use super::support::{TestStore, commit_ready, stateless_operation, turn_input};

fn input_change(keep: [Option<bool>; 4]) -> Change {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30, 40])),
            Arc::new(BooleanArray::from(keep.to_vec())),
            Arc::new(StringArray::from(vec![
                Some("ten"),
                Some("twenty"),
                None,
                Some("forty"),
            ])),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2])).unwrap()
}

fn assert_same_change(expected: Option<Change>, actual: Option<Change>) {
    match (expected, actual) {
        (Some(expected), Some(actual)) => {
            assert_eq!(actual.schema(), expected.schema());
            assert_eq!(actual.records(), expected.records());
            assert_eq!(actual.diffs(), expected.diffs());
        }
        (None, None) => {}
        (expected, actual) => panic!(
            "standalone and inline output presence differs: standalone={}, inline={}",
            expected.is_some(),
            actual.is_some()
        ),
    }
}

fn assert_inline_matches_standalone<D>(definition: &D, input: &Change)
where
    D: InlineOperationDefinition + Clone,
{
    let expected_schema = (definition as &dyn OperationDefinition)
        .bind(&[input.schema()])
        .unwrap()
        .output_schema()
        .unwrap()
        .clone();
    let inline = D::clone(definition).try_into_inline().unwrap();
    let mut binding = inline.bind(input.schema()).unwrap();
    assert_eq!(binding.output_schema(), &expected_schema);
    let actual = binding.apply(input).unwrap();

    let mut operation = stateless_operation(definition, input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(expected) = commit_ready(
        operation.as_mut(),
        Some(turn_input(input)),
        &mut transactions,
    )
    .unwrap() else {
        panic!("standalone transform did not complete its input");
    };

    assert_same_change(expected, actual);
}

fn assert_inline_codec<D>(definition: D)
where
    D: InlineOperationDefinition,
{
    let normal = encode_definition(&definition);
    let inline = definition.try_into_inline().unwrap();
    assert_eq!(encode_inline_definition(&inline), normal);
    let decoded = decode_inline_definition(&normal).unwrap();
    assert_eq!(encode_inline_definition(&decoded), normal);
}

#[test]
fn every_inline_definition_uses_the_normal_tag_and_payload() {
    assert_inline_codec(ProjectDefinition::new([0, 2]));
    assert_inline_codec(FilterDefinition::try_new(col("keep")).unwrap());
    assert_inline_codec(ExtendDefinition::try_new("copy", col("id")).unwrap());
    assert_inline_codec(
        SelectDefinition::try_new([("id", col("id")), ("label", col("label"))]).unwrap(),
    );
    assert_inline_codec(
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("label", col("label"), true).unwrap(),
            SchemaAlignField::try_new("id", col("id"), false).unwrap(),
        ])
        .unwrap(),
    );
}

#[test]
fn every_inline_runtime_matches_its_standalone_adapter() {
    let input = input_change([Some(true), Some(false), None, Some(true)]);
    assert_inline_matches_standalone(&ProjectDefinition::new([0, 2]), &input);
    assert_inline_matches_standalone(&FilterDefinition::try_new(col("keep")).unwrap(), &input);
    assert_inline_matches_standalone(
        &ExtendDefinition::try_new("copy", col("id")).unwrap(),
        &input,
    );
    assert_inline_matches_standalone(
        &SelectDefinition::try_new([("label", col("label")), ("id", col("id"))]).unwrap(),
        &input,
    );
    assert_inline_matches_standalone(
        &SchemaAlignDefinition::try_new_with_metadata(
            [
                SchemaAlignField::try_new_with_metadata(
                    "label",
                    col("label"),
                    true,
                    [("field".to_owned(), "metadata".to_owned())],
                )
                .unwrap(),
                SchemaAlignField::try_new("id", col("id"), true).unwrap(),
            ],
            [("schema".to_owned(), "metadata".to_owned())],
        )
        .unwrap(),
        &input,
    );
}

#[test]
fn every_inline_runtime_rejects_a_schema_other_than_its_binding() {
    let input = input_change([Some(true), Some(false), None, Some(true)]);
    let mut metadata = input.schema().metadata().clone();
    metadata.insert("different".to_owned(), "schema".to_owned());
    let records = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            input.schema().fields().clone(),
            metadata,
        )),
        input.records().columns().to_vec(),
    )
    .unwrap();
    let mismatched = Change::try_new(records, input.diffs().clone()).unwrap();
    let definitions = vec![
        ProjectDefinition::new([0, 2]).try_into_inline().unwrap(),
        FilterDefinition::try_new(col("keep"))
            .unwrap()
            .try_into_inline()
            .unwrap(),
        ExtendDefinition::try_new("copy", col("id"))
            .unwrap()
            .try_into_inline()
            .unwrap(),
        SelectDefinition::try_new([("id", col("id")), ("label", col("label"))])
            .unwrap()
            .try_into_inline()
            .unwrap(),
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("label", col("label"), true).unwrap(),
            SchemaAlignField::try_new("id", col("id"), false).unwrap(),
        ])
        .unwrap()
        .try_into_inline()
        .unwrap(),
    ];

    for definition in definitions {
        let mut binding = definition.bind(input.schema()).unwrap();
        assert!(binding.apply(&mismatched).is_err());
    }
}

#[test]
fn inline_filter_represents_an_empty_event_stream_as_none() {
    let input = input_change([Some(false), None, Some(false), None]);
    assert_inline_matches_standalone(&FilterDefinition::try_new(col("keep")).unwrap(), &input);
}

#[test]
fn inline_conversion_rejects_non_row_local_expression_instances() {
    let definition =
        SelectDefinition::try_new([("id", col("id")), ("parameter", placeholder("$1"))]).unwrap();
    assert_eq!(
        definition.try_into_inline().unwrap_err(),
        InlineEligibilityError::UnsupportedExpression {
            expression: 1,
            kind: "placeholder",
        }
    );
}

#[test]
fn inline_binding_has_its_own_schema_guards() {
    let invalid_input = Arc::new(Schema::new(vec![Field::new(
        "$dogpaddle.invalid",
        DataType::UInt64,
        false,
    )]));
    let project = ProjectDefinition::new([0]).try_into_inline().unwrap();
    assert!(matches!(
        project.bind(invalid_input),
        Err(InlineBindError::InvalidInputSchema { .. })
    ));

    let input: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let out_of_bounds = ProjectDefinition::new([1]).try_into_inline().unwrap();
    assert!(matches!(
        out_of_bounds.bind(Arc::clone(&input)),
        Err(InlineBindError::Rejected { .. })
    ));

    let invalid_output = ExtendDefinition::try_new("$dogpaddle.invalid", col("id"))
        .unwrap()
        .try_into_inline()
        .unwrap();
    assert!(matches!(
        invalid_output.bind(input),
        Err(InlineBindError::InvalidOutputSchema { .. })
    ));
}

#[test]
fn inline_decoder_rejects_normal_only_operations_and_malformed_payloads() {
    let normal_only = dogpaddle_operation::operation::transform::RunningEventCountDefinition::new();
    assert_eq!(
        decode_inline_definition(&encode_definition(&normal_only)).unwrap_err(),
        DefinitionCodecError::NotInlineCapable(2)
    );
    assert_eq!(
        decode_inline_definition(&encode_definition(&DiscardDefinition::new())).unwrap_err(),
        DefinitionCodecError::NotInlineCapable(3)
    );
    assert_eq!(
        decode_inline_definition(&encode_definition(&UnionAllDefinition::new(
            NonZeroU32::MIN
        )))
        .unwrap_err(),
        DefinitionCodecError::NotInlineCapable(8)
    );

    let bytes = encode_definition(&ProjectDefinition::new([0]));
    for length in 0..bytes.len() {
        assert_eq!(
            decode_inline_definition(&bytes[..length]).unwrap_err(),
            DefinitionCodecError::Truncated,
            "wrong error for inline definition prefix {length}/{}",
            bytes.len()
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        decode_inline_definition(&trailing).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );
}

fn fused_trace(rows: &[(u64, u64, i64)], batches: &[usize]) -> Vec<(u64, u64, i64)> {
    assert_eq!(batches.iter().sum::<usize>(), rows.len());
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::UInt64, false),
        Field::new("right", DataType::UInt64, false),
    ]));
    let definitions = vec![
        ExtendDefinition::try_new("keep", col("left").not_eq(dogpaddle_operation::lit(2_u64)))
            .unwrap()
            .try_into_inline()
            .unwrap(),
        FilterDefinition::try_new(col("keep"))
            .unwrap()
            .try_into_inline()
            .unwrap(),
        SelectDefinition::try_new([
            ("right", col("right")),
            ("left", col("left")),
            ("keep", col("keep")),
        ])
        .unwrap()
        .try_into_inline()
        .unwrap(),
        ProjectDefinition::new([0, 1]).try_into_inline().unwrap(),
        SchemaAlignDefinition::try_new([
            SchemaAlignField::try_new("left_out", col("left"), false).unwrap(),
            SchemaAlignField::try_new("right_out", col("right"), true).unwrap(),
        ])
        .unwrap()
        .try_into_inline()
        .unwrap(),
    ];

    let mut schema = Arc::clone(&input_schema);
    let mut transforms = Vec::new();
    for definition in definitions {
        let binding = definition.bind(schema).unwrap();
        schema = Arc::clone(binding.output_schema());
        transforms.push(binding);
    }

    let mut trace = Vec::new();
    let mut start = 0;
    for &batch_rows in batches {
        let batch = &rows[start..start + batch_rows];
        let records = RecordBatch::try_new(
            Arc::clone(&input_schema),
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
        let mut current = Some(input);
        for transform in &mut transforms {
            current = current
                .as_ref()
                .and_then(|change| transform.apply(change).unwrap());
        }
        if let Some(output) = current {
            let left = output
                .records()
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let right = output
                .records()
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            trace.extend(
                left.values()
                    .iter()
                    .copied()
                    .zip(right.values().iter().copied())
                    .zip(output.diffs().values().iter().copied())
                    .map(|((left, right), diff)| (left, right, diff)),
            );
        }
        start += batch_rows;
    }
    trace
}

#[test]
fn fused_inline_pipeline_is_homomorphic_over_rebatching() {
    let rows = [(1, 10, 1), (2, 20, -1), (3, 30, 2), (4, 40, -2)];
    let expected = [(1, 10, 1), (3, 30, 2), (4, 40, -2)];
    for batches in [&[4][..], &[1, 3], &[2, 1, 1], &[1, 1, 1, 1]] {
        assert_eq!(fused_trace(&rows, batches), expected);
    }
}
