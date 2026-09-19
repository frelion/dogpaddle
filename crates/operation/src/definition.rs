use std::{any::TypeId, error::Error, fmt::Debug, num::NonZeroU32};

use arrow_schema::SchemaRef;
use dogpaddle_change::{SchemaError, validate_schema};
use thiserror::Error;

use crate::{
    RuntimeResource,
    operation::{AtomicOperation, Operation, TurnOperation, exclusive_turn},
};

mod private {
    use arrow_schema::SchemaRef;

    use super::{OperationBinding, OperationSchemaError};

    pub trait Sealed {
        fn bind_schemas(
            &self,
            input_schemas: &[SchemaRef],
        ) -> Result<OperationBinding, OperationSchemaError>;
    }
}

pub(crate) use private::Sealed;

/// Type-erased Schema rejection from one concrete Operation definition.
pub type OperationSchemaError = Box<dyn Error + Send + Sync + 'static>;

/// Complete structural kind explicitly declared by an Operation definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationKind {
    /// Produces records without consuming input.
    Scan,
    /// Completely consumes one input Change inside the Station transaction.
    AtomicTransform(NonZeroU32),
    /// Owns a full, replayable turn and may lead a Station's atomic tail.
    TurnTransform(NonZeroU32),
    /// Owns a full turn and requires a durable output boundary before downstream work.
    ExclusiveTransform(NonZeroU32),
    /// Consumes input records without producing output.
    Sink(NonZeroU32),
}

impl OperationKind {
    /// Returns the exact number of ordered inputs.
    #[must_use]
    pub const fn input_count(self) -> u32 {
        match self {
            Self::Scan => 0,
            Self::AtomicTransform(count)
            | Self::TurnTransform(count)
            | Self::ExclusiveTransform(count)
            | Self::Sink(count) => count.get(),
        }
    }

    /// Returns whether this kind is a Scan.
    #[must_use]
    pub const fn is_scan(self) -> bool {
        matches!(self, Self::Scan)
    }

    /// Returns whether this kind is a sink.
    #[must_use]
    pub const fn is_sink(self) -> bool {
        matches!(self, Self::Sink(_))
    }

    /// Returns whether this kind completely consumes one input Change in the current transaction.
    #[must_use]
    pub const fn is_atomic(self) -> bool {
        matches!(self, Self::AtomicTransform(_))
    }

    /// Returns whether this kind may lead a Station's atomic tail.
    #[must_use]
    pub const fn allows_atomic_tail(self) -> bool {
        matches!(
            self,
            Self::Scan | Self::AtomicTransform(_) | Self::TurnTransform(_)
        )
    }

    /// Returns whether this kind owns an output stream.
    #[must_use]
    pub const fn has_output(self) -> bool {
        matches!(
            self,
            Self::Scan
                | Self::AtomicTransform(_)
                | Self::TurnTransform(_)
                | Self::ExclusiveTransform(_)
        )
    }
}

/// Pure definition shared by every built-in operation.
pub trait OperationDefinition: private::Sealed + Debug + Send + Sync + 'static {
    /// Returns the Operation's explicitly declared structural kind and input arity.
    fn kind(&self) -> OperationKind;

    /// Returns this definition's stable persistent tag.
    #[doc(hidden)]
    fn persistence_tag(&self) -> u16;

    /// Appends this definition's variant-specific persistent payload.
    #[doc(hidden)]
    fn encode_payload(&self, output: &mut Vec<u8>);
}

impl dyn OperationDefinition + '_ {
    /// Purely binds this definition to ordered, exact logical input Schemas.
    ///
    /// Inputs follow their zero-based port order; Scans receive an empty slice.
    /// Binding may depend only on the persistent Definition and these Schemas:
    /// it must not access Store, external registries, time, or randomness.
    /// A `TurnTransform` must remain replayable from unchanged durable state if
    /// a later atomic tail or final output admission rolls its transaction back.
    ///
    /// # Errors
    ///
    /// Returns [`OperationBindError`] when arity, Schema, or execution capability is invalid.
    pub fn bind(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationBindError> {
        let kind = self.kind();
        let expected = kind.input_count() as usize;
        let actual = input_schemas.len();
        if actual != expected {
            return Err(OperationBindError::InputCount { expected, actual });
        }
        for (input, schema) in input_schemas.iter().enumerate() {
            validate_schema(schema)
                .map_err(|source| OperationBindError::InvalidInputSchema { input, source })?;
        }
        let mut binding = private::Sealed::bind_schemas(self, input_schemas)
            .map_err(|source| OperationBindError::Rejected { source })?;
        match (kind.has_output(), binding.output_schema.as_ref()) {
            (true, None) => return Err(OperationBindError::MissingOutput),
            (false, Some(_)) => return Err(OperationBindError::UnexpectedOutput),
            (true, Some(schema)) => validate_schema(schema)
                .map_err(|source| OperationBindError::InvalidOutputSchema { source })?,
            (false, None) => {}
        }
        if !binding.body.supports(kind) {
            return Err(OperationBindError::ExecutionKind);
        }
        binding.kind = kind;
        Ok(binding)
    }
}

/// One pure, ephemeral binding of an Operation definition to exact input Schemas.
#[doc(hidden)]
pub struct OperationBinding {
    pub(crate) output_schema: Option<SchemaRef>,
    pub(crate) kind: OperationKind,
    pub(crate) body: BoundBody,
}

pub(crate) enum BoundBody {
    AtomicReady(Box<dyn AtomicOperation>),
    TurnReady(Box<dyn TurnOperation>),
    Sequence(crate::operation::scan::sequence::BoundSequence),
    PostgresCdc(Box<crate::operation::scan::postgres_cdc::BoundPostgresCdc>),
    MySqlCdc(Box<crate::operation::scan::mysql_cdc::BoundMySqlCdc>),
    RunningEventCount(crate::operation::transform::running_event_count::BoundRunningEventCount),
    Distinct(crate::operation::transform::distinct::BoundDistinct),
    Aggregate(Box<crate::operation::transform::aggregate::BoundAggregateOperation>),
    EquiJoin(Box<crate::operation::transform::equi_join::BoundEquiJoin>),
    AsOfJoin(Box<crate::operation::transform::asof_join::BoundAsOfJoin>),
    SqliteSink(Box<crate::operation::sink::sqlite::BoundSqliteSink>),
    PostgresSink(Box<crate::operation::sink::postgres::BoundPostgresSink>),
    DorisSink(Box<crate::operation::sink::doris::BoundDorisSink>),
    ClickHouseSink(Box<crate::operation::sink::clickhouse::BoundClickHouseSink>),
}

impl BoundBody {
    fn supports(&self, kind: OperationKind) -> bool {
        let atomic = matches!(
            self,
            Self::AtomicReady(_)
                | Self::RunningEventCount(_)
                | Self::Distinct(_)
                | Self::Aggregate(_)
        );
        match kind {
            OperationKind::AtomicTransform(_) => atomic,
            OperationKind::ExclusiveTransform(_) => true,
            OperationKind::Scan | OperationKind::TurnTransform(_) | OperationKind::Sink(_) => {
                !atomic
            }
        }
    }

    pub(crate) fn resource_type(&self) -> Option<TypeId> {
        match self {
            Self::PostgresCdc(_) => Some(TypeId::of::<
                crate::operation::scan::postgres_cdc::PostgresCdcScanConfig,
            >()),
            Self::MySqlCdc(_) => Some(TypeId::of::<
                crate::operation::scan::mysql_cdc::MySqlCdcScanConfig,
            >()),
            Self::PostgresSink(_) => Some(TypeId::of::<
                crate::operation::sink::postgres::PostgresSinkConfig,
            >()),
            Self::DorisSink(_) => {
                Some(TypeId::of::<crate::operation::sink::doris::DorisSinkConfig>())
            }
            Self::ClickHouseSink(_) => Some(TypeId::of::<
                crate::operation::sink::clickhouse::ClickHouseSinkConfig,
            >()),
            _ => None,
        }
    }
}

impl OperationBinding {
    pub(crate) fn atomic_ready(output_schema: SchemaRef, operation: impl AtomicOperation) -> Self {
        Self {
            output_schema: Some(output_schema),
            kind: OperationKind::AtomicTransform(NonZeroU32::MIN),
            body: BoundBody::AtomicReady(Box::new(operation)),
        }
    }

    pub(crate) fn turn_ready(
        output_schema: Option<SchemaRef>,
        operation: impl TurnOperation,
    ) -> Self {
        Self {
            output_schema,
            kind: OperationKind::Scan,
            body: BoundBody::TurnReady(Box::new(operation)),
        }
    }

    pub(crate) fn bound(output_schema: Option<SchemaRef>, body: BoundBody) -> Self {
        Self {
            output_schema,
            kind: OperationKind::Scan,
            body,
        }
    }

    /// Checks the resource's presence and exact type without accessing it.
    ///
    /// # Errors
    /// Returns an error for a missing, unexpected, or wrong-type resource.
    pub fn validate_resource(
        &self,
        resource: &RuntimeResource,
    ) -> Result<(), crate::OperationSetupError> {
        resource.validate(self.body.resource_type())
    }

    /// Returns the exact logical output Schema, or `None` for a Sink binding.
    #[doc(hidden)]
    #[must_use]
    pub const fn output_schema(&self) -> Option<&SchemaRef> {
        self.output_schema.as_ref()
    }

    pub(crate) fn normalize(
        kind: OperationKind,
        operation: Operation,
    ) -> Result<Operation, crate::OperationSetupError> {
        match (kind, operation) {
            (OperationKind::ExclusiveTransform(_), Operation::Atomic(operation)) => {
                Ok(Operation::Turn(exclusive_turn(operation)))
            }
            (OperationKind::AtomicTransform(_), operation @ Operation::Atomic(_))
            | (
                OperationKind::Scan
                | OperationKind::TurnTransform(_)
                | OperationKind::ExclusiveTransform(_)
                | OperationKind::Sink(_),
                operation @ Operation::Turn(_),
            ) => Ok(operation),
            _ => Err(crate::OperationSetupError::ExecutionKind),
        }
    }
}

/// Failure while binding one Operation definition to exact logical Schemas.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OperationBindError {
    #[error("operation requires {expected} input schemas but received {actual}")]
    InputCount { expected: usize, actual: usize },
    #[error("operation input schema {input} is invalid: {source}")]
    InvalidInputSchema {
        input: usize,
        #[source]
        source: SchemaError,
    },
    #[error("operation rejected its input schemas: {source}")]
    Rejected {
        #[source]
        source: OperationSchemaError,
    },
    #[error("operation kind requires an output schema but its binding has none")]
    MissingOutput,
    #[error("outputless operation kind bound an output schema")]
    UnexpectedOutput,
    #[error("operation output schema is invalid: {source}")]
    InvalidOutputSchema {
        #[source]
        source: SchemaError,
    },
    #[error("operation binding execution capability does not match its declared kind")]
    ExecutionKind,
}
