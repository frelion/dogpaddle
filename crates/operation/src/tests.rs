use std::{fmt, num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_store::{Store, TransactionAccess};

use crate::{
    OperationBindError, OperationBinding, OperationDefinition, OperationKind, OperationSchemaError,
    RuntimeResource,
    codec::DECODERS,
    create_operation,
    definition::Sealed,
    open_operation,
    operation::{AtomicOperation, Operation, OperationError, OperationInput, Turn, TurnOperation},
};

#[test]
fn decoder_registry_contains_each_builtin_tag_once() {
    let mut tags = DECODERS.iter().map(|(tag, _)| *tag).collect::<Vec<_>>();
    tags.sort_unstable();
    assert_eq!(tags, (1..=19).collect::<Vec<_>>());
}

#[derive(Debug)]
struct InvalidDefinition {
    kind: OperationKind,
    output: Option<SchemaRef>,
    body: Body,
}

#[derive(Clone, Copy, Debug)]
enum Body {
    Atomic,
    Turn,
}

impl Sealed for InvalidDefinition {
    fn bind_schemas(
        &self,
        _input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        Ok(match self.body {
            Body::Atomic => OperationBinding::atomic_ready(
                self.output.clone().expect("atomic test body needs output"),
                TestAtomic,
            ),
            Body::Turn => OperationBinding::turn_ready(self.output.clone(), TestTurn),
        })
    }
}

impl OperationDefinition for InvalidDefinition {
    fn kind(&self) -> OperationKind {
        self.kind
    }

    fn persistence_tag(&self) -> u16 {
        0
    }

    fn encode_payload(&self, _output: &mut Vec<u8>) {}
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

#[test]
fn bind_rejects_missing_and_unexpected_output() {
    let missing = InvalidDefinition {
        kind: OperationKind::Scan,
        output: None,
        body: Body::Turn,
    };
    assert!(matches!(
        (&missing as &dyn OperationDefinition).bind(&[]),
        Err(OperationBindError::MissingOutput)
    ));

    let unexpected = InvalidDefinition {
        kind: OperationKind::Sink(NonZeroU32::MIN),
        output: Some(schema()),
        body: Body::Turn,
    };
    assert!(matches!(
        (&unexpected as &dyn OperationDefinition).bind(&[schema()]),
        Err(OperationBindError::UnexpectedOutput)
    ));
}

#[test]
fn exclusive_transform_create_and_open_accept_atomic_and_turn_bodies() {
    for body in [Body::Atomic, Body::Turn] {
        let definition = InvalidDefinition {
            kind: OperationKind::ExclusiveTransform(NonZeroU32::MIN),
            output: Some(schema()),
            body,
        };
        let input = schema();
        let root = tempfile::tempdir().unwrap();
        let mut setup = Store::setup(root.path().join("store")).unwrap();
        let operation = create_operation(
            (&definition as &dyn OperationDefinition)
                .bind(std::slice::from_ref(&input))
                .expect("bind exclusive body for create"),
            &mut setup,
            "operation",
            RuntimeResource::none(),
        )
        .expect("create exclusive body");
        assert!(matches!(operation, Operation::Turn(_)));
        let transactions = setup.commit(|_| Ok(())).unwrap();
        drop(transactions);

        let store = Store::open(root.path().join("store")).unwrap();
        let operation = open_operation(
            (&definition as &dyn OperationDefinition)
                .bind(&[input])
                .expect("bind exclusive body for open"),
            &store,
            "operation",
            RuntimeResource::none(),
        )
        .expect("open exclusive body");
        assert!(matches!(operation, Operation::Turn(_)));
    }
}

#[test]
fn atomic_transform_rejects_a_turn_body() {
    let definition = InvalidDefinition {
        kind: OperationKind::AtomicTransform(NonZeroU32::MIN),
        output: Some(schema()),
        body: Body::Turn,
    };
    assert!(matches!(
        (&definition as &dyn OperationDefinition).bind(&[schema()]),
        Err(OperationBindError::ExecutionKind)
    ));
}

struct TestAtomic;

impl AtomicOperation for TestAtomic {
    fn apply(
        &mut self,
        _input: OperationInput<'_>,
        _transaction: TransactionAccess<'_>,
    ) -> Result<Option<Change>, OperationError> {
        Ok(None)
    }
}

struct TestTurn;

impl TurnOperation for TestTurn {
    fn turn(&mut self, _input: Option<OperationInput<'_>>) -> Result<Turn<'_>, OperationError> {
        Ok(Turn::Idle)
    }
}

impl fmt::Debug for TestTurn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TestTurn")
    }
}
