use std::{fmt, num::NonZeroU32, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_store::{DataScope, StoreSetup, TransactionAccess};

use crate::{
    OperationBindError, OperationDefinition, OperationKind, OperationSetupError, RuntimeResource,
    codec::DECODERS,
    definition::{ConstructedOperation, Sealed},
    operation::{AtomicOperation, OperationError, OperationInput, Turn, TurnOperation},
};

#[test]
fn decoder_registry_contains_each_builtin_tag_once() {
    let mut tags = DECODERS.iter().map(|(tag, _)| *tag).collect::<Vec<_>>();
    tags.sort_unstable();
    assert_eq!(
        tags,
        (1..=19)
            .filter(|tag| ![4, 6].contains(tag))
            .collect::<Vec<_>>()
    );
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
    fn output_schema_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        _inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        Ok(self.output.clone())
    }

    fn construct_unchecked(
        &self,
        _: crate::definition::ConstructionToken,
        _inputs: &[SchemaRef],
        _data: &mut DataScope<'_>,
        _resource: RuntimeResource,
    ) -> Result<ConstructedOperation, OperationSetupError> {
        Ok(match self.body {
            Body::Atomic => ConstructedOperation::atomic(
                self.output.clone().expect("atomic test body needs output"),
                TestAtomic,
            ),
            Body::Turn => ConstructedOperation::turn(self.output.clone(), TestTurn),
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
fn construct_rejects_missing_and_unexpected_output() {
    let missing = InvalidDefinition {
        kind: OperationKind::Scan,
        output: None,
        body: Body::Turn,
    };
    let mut setup = StoreSetup::new();
    assert!(matches!(
        (&missing as &dyn OperationDefinition).construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        ),
        Err(OperationSetupError::Bind(OperationBindError::MissingOutput))
    ));

    let unexpected = InvalidDefinition {
        kind: OperationKind::Sink(NonZeroU32::MIN),
        output: Some(schema()),
        body: Body::Turn,
    };
    let mut setup = StoreSetup::new();
    assert!(matches!(
        (&unexpected as &dyn OperationDefinition).construct(
            &[schema()],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::none(),
        ),
        Err(OperationSetupError::Bind(
            OperationBindError::UnexpectedOutput
        ))
    ));
}

#[test]
fn execution_kind_rejects_crossing_atomic_and_turn_bodies() {
    for (kind, body) in [
        (OperationKind::AtomicTransform(NonZeroU32::MIN), Body::Turn),
        (OperationKind::TurnTransform(NonZeroU32::MIN), Body::Atomic),
    ] {
        let definition = InvalidDefinition {
            kind,
            output: Some(schema()),
            body,
        };
        let mut setup = StoreSetup::new();
        assert!(matches!(
            (&definition as &dyn OperationDefinition).construct(
                &[schema()],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            ),
            Err(OperationSetupError::ExecutionKind)
        ));
    }
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
