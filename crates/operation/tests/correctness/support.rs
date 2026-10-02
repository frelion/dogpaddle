use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Date32Array, Decimal128Array, Int64Array, RecordBatch, TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationBindError, OperationDefinition, RuntimeResource,
    operation::{BudgetExceeded, Operation, OperationError, OperationInput, Progress, StepBudget},
};
use dogpaddle_store::{Store, StoreSetup, Transactions};
use tempfile::TempDir;

pub struct TestStore {
    _root: TempDir,
    path: PathBuf,
}

impl TestStore {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        Self { _root: root, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn assert_literal_definition<D: Clone + Into<OperationDefinition>>(
    definition: &D,
    fixture: &str,
    expected_inputs: u32,
) -> OperationDefinition {
    let definition = definition.clone().into();
    let literal = decode_hex(fixture);
    assert_eq!(definition.input_count(), expected_inputs);
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition).unwrap(),
        literal
    );

    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&literal).unwrap();
    assert_eq!(decoded.input_count(), expected_inputs);
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
        literal
    );
    for length in 0..literal.len() {
        assert!(
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&literal[..length])
                .is_err()
        );
    }
    let mut trailing = literal;
    trailing.push(0);
    assert!(serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&trailing).is_err());
    decoded
}

pub fn construct_checked<D: Clone + Into<OperationDefinition>>(
    definition: &D,
    input_schemas: &[SchemaRef],
) -> Result<Option<SchemaRef>, OperationBindError> {
    let definition = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
        &serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap(),
    )
    .unwrap();
    definition.output_schema(input_schemas)
}

pub fn construct_checked_with_resource<D: Clone + Into<OperationDefinition>>(
    definition: &D,
    input_schemas: &[SchemaRef],
    resource: &RuntimeResource,
) -> Result<Option<SchemaRef>, OperationBindError> {
    let definition = serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
        &serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap(),
    )
    .unwrap();
    definition
        .validate_resource(resource)
        .expect("correctness helper received an invalid runtime resource");
    definition.output_schema(input_schemas)
}

pub fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

pub fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

pub fn project_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("message", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ]))
}

pub fn change(diffs: &[i64]) -> Change {
    change_with_field_name("input", diffs)
}

pub fn change_with_field_name(field_name: &str, diffs: &[i64]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![7; diffs.len()]))],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

pub fn temporal_and_decimal_change() -> Change {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new(
            "occurred_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                None,
                Some(2_500),
                Some(4_000),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(100), None, Some(300), Some(400), Some(500)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2, -2, 3])).unwrap()
}

pub const fn step_input(change: &Change) -> OperationInput<'_> {
    OperationInput { port: 0, change }
}

pub fn stateless_operation<D: Clone + Into<OperationDefinition>>(
    definition: &D,
    input_schema: SchemaRef,
) -> Operation {
    let definition = definition.clone().into();
    let fixture = TestStore::new();
    let mut setup = StoreSetup::new();
    let constructed = definition
        .construct(
            &[input_schema],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        )
        .unwrap();
    let (operation, _) = constructed.into_parts();
    let _transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    operation
}

pub fn roundtripped_output<D: Clone + Into<OperationDefinition>>(
    definition: &D,
    input: &Change,
) -> Change {
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.clone().into())
            .unwrap();
    let decoded =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    assert_eq!(
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&decoded).unwrap(),
        encoded
    );
    let operation = stateless_operation(&decoded, input.schema());
    let fixture = TestStore::new();
    let store = StoreSetup::new();
    let mut transactions = store.commit(fixture.path(), |_| Ok(())).unwrap();
    let Some(output) = run_input(&operation, step_input(input), &mut transactions).unwrap() else {
        panic!("round-tripped stateless Operation did not complete with output");
    };
    output
}

pub fn output_values(output: Option<Change>, field_name: &str) -> Vec<u64> {
    let output = output.expect("Operation did not produce an output Change");
    let field = output.schema().field(0).clone();
    assert_eq!(field.name(), field_name);
    assert_eq!(field.data_type(), &DataType::UInt64);
    assert!(!field.is_nullable());
    assert_eq!(
        output.diffs().values(),
        vec![1; output.num_rows()].as_slice()
    );
    let values = output
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..output.num_rows())
        .map(|index| values.value(index))
        .collect()
}

/// Applies the whole immutable input through committed bounded pages.
pub fn run_input(
    operation: &Operation,
    input: OperationInput<'_>,
    transactions: &mut Transactions,
) -> Result<Option<Change>, OperationError> {
    let mut resume = operation.initial_resume();
    let mut outputs = Vec::new();
    loop {
        let mut allowance = 256;
        let step = loop {
            let transaction = transactions.begin();
            let mut budget = StepBudget::new(allowance, 4 * 1024 * 1024);
            match operation.step(input, &resume, transaction.access(), &mut budget) {
                Ok(step) => {
                    transaction.commit()?;
                    break step;
                }
                Err(error) if error.is::<BudgetExceeded>() && allowance > 1 => {
                    drop(transaction);
                    allowance /= 2;
                }
                Err(error) => return Err(error),
            }
        };
        if let Some(output) = step.output {
            outputs.push(output);
        }
        match step.progress {
            Progress::More(next) => {
                assert_ne!(next, resume);
                resume = next;
            }
            Progress::Done => break,
        }
    }
    combine_output(outputs)
}

/// Runs one bounded step and discards its transaction and position.
pub fn rollback_input(
    operation: &Operation,
    input: OperationInput<'_>,
    transactions: &mut Transactions,
) -> Result<Option<Change>, OperationError> {
    let transaction = transactions.begin();
    let mut budget = StepBudget::new(256, 4 * 1024 * 1024);
    let step = operation.step(
        input,
        &operation.initial_resume(),
        transaction.access(),
        &mut budget,
    )?;
    drop(transaction);
    Ok(step.output)
}

fn combine_output(mut outputs: Vec<Change>) -> Result<Option<Change>, OperationError> {
    match outputs.len() {
        0 => Ok(None),
        1 => Ok(outputs.pop()),
        _ => {
            let records = arrow_select::concat::concat_batches(
                &outputs[0].schema(),
                outputs.iter().map(Change::records),
            )?;
            let diffs = outputs
                .iter()
                .flat_map(|output| output.diffs().values().iter().copied())
                .collect::<Vec<_>>();
            Ok(Some(Change::try_new(records, Int64Array::from(diffs))?))
        }
    }
}

pub fn decode_hex(encoded: &str) -> Vec<u8> {
    let digits = encoded
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(digits.len() % 2, 0, "hex fixture has an odd digit count");
    digits
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        b'A'..=b'F' => digit - b'A' + 10,
        _ => panic!("invalid hex digit {digit:?}"),
    }
}

/// Binding rejects raw semantic mistakes before attempting to retrieve owner data.
pub fn assert_rejected_plan_before_data(
    definition: &OperationDefinition,
    inputs: &[SchemaRef],
    resource: RuntimeResource,
) {
    let encoded =
        serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(definition).unwrap();
    let plan =
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded).unwrap();
    assert!(plan.output_schema(inputs).is_err());
    let root = TestStore::new();
    let transactions = StoreSetup::new().commit(root.path(), |_| Ok(())).unwrap();
    drop(transactions);
    let store = Store::open(root.path()).unwrap();
    let result = plan.construct(inputs, &mut store.data_scope().scoped("absent"), resource);
    assert!(matches!(
        result,
        Err(dogpaddle_operation::OperationSetupError::Schema { .. })
    ));
}
