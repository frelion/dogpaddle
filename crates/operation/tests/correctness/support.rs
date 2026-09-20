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
    DefinitionCodecError, OperationBindError, OperationDefinition, OperationKind, RuntimeResource,
    decode_definition, encode_definition,
    operation::{Action, AfterCommit, Operation, OperationError, OperationInput, Turn},
};
use dogpaddle_store::{Store, StoreSetup, TransactionAccess, Transactions};
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

pub fn assert_literal_definition(
    definition: &dyn OperationDefinition,
    fixture: &str,
    expected_tag: u16,
    expected_kind: OperationKind,
) -> Box<dyn OperationDefinition> {
    let literal = decode_hex(fixture);
    assert_eq!(definition.persistence_tag(), expected_tag);
    assert_eq!(definition.kind(), expected_kind);
    assert_eq!(encode_definition(definition), literal);

    let decoded = decode_definition(&literal).unwrap();
    assert_eq!(decoded.persistence_tag(), expected_tag);
    assert_eq!(decoded.kind(), expected_kind);
    assert_eq!(encode_definition(decoded.as_ref()), literal);
    for length in 0..literal.len() {
        assert_eq!(
            decode_definition(&literal[..length]).unwrap_err(),
            DefinitionCodecError::Truncated,
            "wrong error for definition prefix {length}/{} for tag {expected_tag}",
            literal.len()
        );
    }
    let mut trailing = literal;
    trailing.push(0);
    assert_eq!(
        decode_definition(&trailing).unwrap_err(),
        DefinitionCodecError::TrailingBytes
    );
    decoded
}

pub fn construct_checked(
    definition: &dyn OperationDefinition,
    input_schemas: &[SchemaRef],
) -> Result<Option<SchemaRef>, OperationBindError> {
    let definition = decode_definition(&encode_definition(definition)).unwrap();
    definition.output_schema(input_schemas)
}

pub fn construct_checked_with_resource(
    definition: &dyn OperationDefinition,
    input_schemas: &[SchemaRef],
    resource: &RuntimeResource,
) -> Result<Option<SchemaRef>, OperationBindError> {
    let definition = decode_definition(&encode_definition(definition)).unwrap();
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

pub const fn turn_input(change: &Change) -> OperationInput<'_> {
    OperationInput { port: 0, change }
}

pub fn stateless_operation(
    definition: &dyn OperationDefinition,
    input_schema: SchemaRef,
) -> Operation {
    let fixture = TestStore::new();
    let mut setup = StoreSetup::new();
    let constructed = <dyn OperationDefinition>::construct(
        definition,
        &[input_schema],
        &mut setup.data_scope(),
        "operation",
        RuntimeResource::none(),
    )
    .unwrap();
    let (operation, _) = constructed.into_parts();
    let _transactions = setup.commit(fixture.path(), |_| Ok(())).unwrap();
    operation
}

pub fn roundtripped_output(definition: &dyn OperationDefinition, input: &Change) -> Change {
    let encoded = encode_definition(definition);
    let decoded = decode_definition(&encoded).unwrap();
    assert_eq!(encode_definition(decoded.as_ref()), encoded);
    let mut operation = stateless_operation(decoded.as_ref(), input.schema());
    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let Action::Complete(Some(output)) =
        commit_ready(&mut operation, Some(turn_input(input)), &mut transactions).unwrap()
    else {
        panic!("round-tripped stateless Operation did not complete with output");
    };
    output
}

#[derive(Clone, Copy)]
pub enum ExpectedAction {
    Commit,
    Complete,
}

pub fn output_values(
    action: Action,
    expected_action: ExpectedAction,
    field_name: &str,
) -> Vec<u64> {
    let output = match expected_action {
        ExpectedAction::Commit => {
            let Action::Commit(Some(output)) = action else {
                panic!("Operation did not commit one output Change")
            };
            output
        }
        ExpectedAction::Complete => {
            let Action::Complete(Some(output)) = action else {
                panic!("Operation did not complete with one output Change")
            };
            output
        }
    };
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

fn apply_ready<'turn>(
    turn: Turn<'turn>,
    access: TransactionAccess<'_>,
) -> Result<(Action, AfterCommit<'turn>), OperationError> {
    match turn {
        Turn::Ready(prepared) => prepared.apply(access),
        Turn::Idle => panic!("a transactional built-in Operation returned an outer idle turn"),
    }
}

pub fn commit_ready(
    operation: &mut Operation,
    input: Option<OperationInput<'_>>,
    transactions: &mut Transactions,
) -> Result<Action, OperationError> {
    let turn = operation.turn(input)?;
    let transaction = transactions.begin();
    let (action, after_commit) = apply_ready(turn, transaction.access())?;
    if matches!(&action, Action::Idle) {
        drop(transaction);
        drop(after_commit);
        return Ok(action);
    }
    transaction.commit()?;
    after_commit
        .run()
        .map_err(|error| Box::new(error) as OperationError)?;
    Ok(action)
}

pub fn rollback_ready(
    operation: &mut Operation,
    input: Option<OperationInput<'_>>,
    transactions: &mut Transactions,
) -> Result<Action, OperationError> {
    let turn = operation.turn(input)?;
    let transaction = transactions.begin();
    let (action, after_commit) = apply_ready(turn, transaction.access())?;
    drop(transaction);
    drop(after_commit);
    Ok(action)
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
