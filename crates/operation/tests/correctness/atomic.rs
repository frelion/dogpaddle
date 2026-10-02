use std::{num::NonZeroU32, sync::Arc};

use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::placeholder;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col,
    operation::{
        AtomicOperation, Operation, OperationInput, StepBudget,
        transform::{
            AggregateCall, AggregateDefinition, DistinctDefinition, FilterDefinition,
            RunningEventCountDefinition, SelectDefinition, UnionAllDefinition,
        },
    },
};
use dogpaddle_store::StoreSetup;

use super::support::{TestStore, stateless_operation};

#[test]
fn transform_construction_exposes_atomic_execution() {
    let definitions: Vec<OperationDefinition> = vec![
        FilterDefinition::try_new(col("keep")).unwrap().into(),
        SelectDefinition::try_new([("id", col("id"))])
            .unwrap()
            .into(),
        RunningEventCountDefinition::new().into(),
        DistinctDefinition::new().into(),
        AggregateDefinition::try_new([("id", col("id"))], [("count", AggregateCall::CountAll)])
            .unwrap()
            .into(),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("keep", DataType::Boolean, false),
    ]));
    for definition in definitions {
        assert_eq!(definition.input_count(), 1);
        let mut setup = StoreSetup::new();
        let (operation, _) = definition
            .construct(
                &[Arc::clone(&schema)],
                &mut setup.data_scope(),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts();
        assert!(matches!(operation, Operation::Atomic(_)));
    }
    let union = UnionAllDefinition::new(NonZeroU32::new(2).unwrap());
    let union = OperationDefinition::from(union);
    assert_eq!(union.input_count(), 2);
    let mut setup = StoreSetup::new();
    let (operation, _) = union
        .construct(
            &[Arc::clone(&schema), schema],
            &mut setup.data_scope(),
            RuntimeResource::none(),
        )
        .unwrap()
        .into_parts();
    assert!(matches!(operation, Operation::Atomic(_)));
}

#[test]
fn every_expression_owner_rejects_unbound_parameters_at_definition_time() {
    let parameter = placeholder("$1");
    assert!(FilterDefinition::try_new(parameter.clone().eq(parameter.clone())).is_err());
    assert!(SelectDefinition::try_new([("parameter", parameter.clone())]).is_err());
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
    let store = StoreSetup::new();
    let mut transactions = store.commit(fixture.path(), |_| Ok(())).unwrap();
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
