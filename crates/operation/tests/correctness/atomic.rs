use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::placeholder;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, OperationKind, col,
    operation::{
        AtomicOperation, Operation, OperationInput, StepBudget,
        transform::{
            AggregateCall, AggregateDefinition, DistinctDefinition, FilterDefinition,
            RunningEventCountDefinition, SchemaAlignDefinition, SchemaAlignField, SelectDefinition,
            UnionAllDefinition,
        },
    },
};
use dogpaddle_store::Store;

use super::support::{TestStore, stateless_operation};

fn unary() -> NonZeroU32 {
    NonZeroU32::MIN
}

#[test]
fn transform_kind_declares_atomic_execution_explicitly() {
    let definitions: Vec<OperationDefinition> = vec![
        FilterDefinition::try_new(col("keep")).unwrap().into(),
        SelectDefinition::try_new([("id", col("id"))])
            .unwrap()
            .into(),
        SchemaAlignDefinition::try_new(
            [SchemaAlignField::try_new("id", col("id"), false).unwrap()],
        )
        .unwrap()
        .into(),
        RunningEventCountDefinition::new().into(),
        DistinctDefinition::new().into(),
        AggregateDefinition::try_new([("id", col("id"))], [("count", AggregateCall::CountAll)])
            .unwrap()
            .into(),
    ];
    for definition in definitions {
        assert_eq!(definition.kind(), OperationKind::AtomicTransform(unary()));
    }
    let union = UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
    assert_eq!(
        OperationDefinition::from(union).kind(),
        OperationKind::AtomicTransform(NonZeroU32::new(2).unwrap())
    );
}

#[test]
fn every_expression_owner_rejects_unbound_parameters_at_definition_time() {
    let parameter = placeholder("$1");
    assert!(FilterDefinition::try_new(parameter.clone().eq(parameter.clone())).is_err());
    assert!(SelectDefinition::try_new([("parameter", parameter.clone())]).is_err());
    assert!(SchemaAlignField::try_new("parameter", parameter.clone(), true).is_err());
    assert!(
        AggregateDefinition::try_new(
            [("parameter", parameter.clone())],
            [("count", AggregateCall::CountAll)],
        )
        .is_err()
    );
    assert!(
        AggregateDefinition::try_new(
            [("id", col("id"))],
            [("count", AggregateCall::Count(parameter))]
        )
        .is_err()
    );
}

#[test]
fn atomic_runtime_applies_a_complete_change_directly() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();
    let input = Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap();
    let operation = stateless_operation(&FilterDefinition::try_new(col("keep")).unwrap(), schema);
    let Operation::Atomic(operation) = &operation else {
        panic!("eligible Filter was not created as an atomic operation");
    };

    let fixture = TestStore::new();
    let store = Store::create(fixture.path()).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    let output = AtomicOperation::apply(
        operation.as_ref(),
        OperationInput {
            port: 0,
            change: &input,
        },
        transaction.access(),
        &mut StepBudget::new(0, 4 * 1024 * 1024),
    )
    .unwrap()
    .unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(output.diffs().values(), &[1, 2]);
}
