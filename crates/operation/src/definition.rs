use std::{error::Error, fmt::Debug, num::NonZeroU32};

use arrow_schema::SchemaRef;
use dogpaddle_change::{SchemaError, validate_schema};
use dogpaddle_store::DataScope;
use thiserror::Error;

use crate::{
    RuntimeResource,
    operation::{AtomicOperation, Operation, TurnOperation, exclusive_turn},
};

mod private {
    use super::ConstructedOperation;
    use crate::{OperationSetupError, RuntimeResource};
    use arrow_schema::SchemaRef;
    use dogpaddle_store::DataScope;
    use std::any::TypeId;

    /// Unnameable in-crate capability guarding the unchecked construction
    /// entry points. Nominally public so it can appear in the sealed trait,
    /// but every ancestor module is private: downstream crates can neither
    /// name nor construct it. They must use the checked
    /// `OperationDefinition::output_schema` and
    /// `OperationDefinition::construct` entry points instead.
    pub struct ConstructionToken(pub(super) ());

    pub trait Sealed {
        fn output_schema_unchecked(
            &self,
            _: ConstructionToken,
            inputs: &[SchemaRef],
        ) -> Result<Option<SchemaRef>, crate::OperationSchemaError>;
        fn construct_unchecked(
            &self,
            _: ConstructionToken,
            inputs: &[SchemaRef],
            data: &mut DataScope<'_>,
            resource: RuntimeResource,
        ) -> Result<ConstructedOperation, OperationSetupError>;
        fn resource_type(&self) -> Option<TypeId> {
            None
        }
    }
}
pub(crate) use private::{ConstructionToken, Sealed};

/// Type-erased error from a concrete operation's pure Schema compiler.
pub type OperationSchemaError = Box<dyn Error + Send + Sync + 'static>;

/// Declared execution role, exact input arity, and station-fusion capability of an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OperationKind {
    /// Zero-input source driven by turns.
    Scan,
    /// Transaction-local transform with the given nonzero input arity.
    AtomicTransform(NonZeroU32),
    /// Turn-based transform that may head an atomic tail.
    TurnTransform(NonZeroU32),
    /// Transform that must occupy its own station.
    ExclusiveTransform(NonZeroU32),
    /// Outputless terminal operation.
    Sink(NonZeroU32),
}
impl OperationKind {
    /// Returns the exact number of ordered inputs.
    #[must_use]
    pub const fn input_count(self) -> u32 {
        match self {
            Self::Scan => 0,
            Self::AtomicTransform(n)
            | Self::TurnTransform(n)
            | Self::ExclusiveTransform(n)
            | Self::Sink(n) => n.get(),
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
        !matches!(self, Self::Sink(_))
    }
}

/// Sealed persistent operation plan with authoritative pure Schema derivation and final construction.
///
/// Use the checked entry points through a trait object:
///
/// ```
/// use dogpaddle_operation::{OperationDefinition, RuntimeResource};
/// use dogpaddle_operation::operation::transform::RunningEventCountDefinition;
/// use dogpaddle_store::StoreSetup;
///
/// let definition = RunningEventCountDefinition::new();
/// let definition: &dyn OperationDefinition = &definition;
/// let mut setup = StoreSetup::new();
/// // Missing input is rejected, rather than reaching the concrete constructor.
/// assert!(definition.output_schema(&[]).is_err());
/// assert!(definition.construct(
///     &[], &mut setup.data_scope().scoped("count"), RuntimeResource::none(),
/// ).is_err());
/// ```
///
/// Sealing alone does not hide inherited methods. The unchecked entry points
/// also require an internal capability that downstream code cannot obtain.
/// Calling either entry point without that capability fails to compile:
///
/// ```compile_fail,E0061
/// use dogpaddle_operation::OperationDefinition;
/// fn bypass(definition: &dyn OperationDefinition) {
///     let _ = definition.output_schema_unchecked(&[]);
/// }
/// ```
///
/// ```compile_fail,E0061
/// use dogpaddle_operation::{OperationDefinition, RuntimeResource};
/// use dogpaddle_store::StoreSetup;
/// fn bypass(definition: &dyn OperationDefinition) {
///     let mut setup = StoreSetup::new();
///     let _ = definition.construct_unchecked(
///         &[], &mut setup.data_scope().scoped("count"), RuntimeResource::none(),
///     );
/// }
/// ```
///
/// The capability cannot be obtained using type inference and `Default`:
///
/// ```compile_fail,E0277
/// use dogpaddle_operation::OperationDefinition;
/// fn bypass(definition: &dyn OperationDefinition) {
///     let _ = definition.output_schema_unchecked(Default::default(), &[]);
/// }
/// ```
///
/// Its constructor is not publicly accessible either:
///
/// ```compile_fail,E0603
/// use dogpaddle_operation::definition::ConstructionToken;
/// let _ = ConstructionToken(());
/// ```
pub trait OperationDefinition: private::Sealed + Debug + Send + Sync + 'static {
    /// Returns the operation's declared execution role and exact input arity.
    fn kind(&self) -> OperationKind;
    #[doc(hidden)]
    fn persistence_tag(&self) -> u16;
    #[doc(hidden)]
    fn encode_payload(&self, output: &mut Vec<u8>);
}

/// Final runtime operation paired with its checked logical output Schema metadata.
pub struct ConstructedOperation {
    operation: Operation,
    output_schema: Option<SchemaRef>,
}
impl ConstructedOperation {
    pub(crate) fn atomic(schema: SchemaRef, op: impl AtomicOperation) -> Self {
        Self {
            operation: Operation::Atomic(Box::new(op)),
            output_schema: Some(schema),
        }
    }

    pub(crate) fn turn(schema: Option<SchemaRef>, op: impl TurnOperation) -> Self {
        Self {
            operation: Operation::Turn(Box::new(op)),
            output_schema: schema,
        }
    }

    pub(crate) fn new(operation: Operation, output_schema: Option<SchemaRef>) -> Self {
        Self {
            operation,
            output_schema,
        }
    }

    /// Borrows the final checked logical output Schema, if this operation has output.
    #[must_use]
    pub const fn output_schema(&self) -> Option<&SchemaRef> {
        self.output_schema.as_ref()
    }

    /// Consumes the result into its runnable operation and output Schema metadata.
    #[must_use]
    pub fn into_parts(self) -> (Operation, Option<SchemaRef>) {
        (self.operation, self.output_schema)
    }
}

impl dyn OperationDefinition + '_ {
    /// Purely derives the exact logical output Schema from this definition and its inputs.
    ///
    /// This path performs the same checked Schema compilation used by final construction, but
    /// declares no Store data and creates no runtime operation.
    ///
    /// # Errors
    /// Returns an error for invalid input arity or Schemas, a concrete Schema rejection, or an
    /// output whose presence or logical Schema violates the declared operation kind.
    pub fn output_schema(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, OperationBindError> {
        validate_inputs(self.kind(), inputs)?;
        let output = private::Sealed::output_schema_unchecked(self, ConstructionToken(()), inputs)
            .map_err(|source| OperationBindError::Rejected { source })?;
        validate_output(self.kind(), output.as_ref())?;
        Ok(output)
    }

    /// Constructs the final runtime operation and exact output Schema through the only checked path.
    /// Resource metadata is checked before concrete code may access Store data.
    /// The caller scopes `data` to this operation’s resource prefix before construction.
    /// Concrete definitions declare only their own fixed logical names within that scope.
    /// Construction performs no transactions, state reads, or external I/O.
    ///
    /// # Errors
    /// Returns an error for invalid arity/Schemas, resources, typed data, or execution capability.
    pub fn construct(
        &self,
        inputs: &[SchemaRef],
        data: &mut DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, OperationSetupError> {
        let kind = self.kind();
        validate_inputs(kind, inputs)?;
        resource.validate(private::Sealed::resource_type(self))?;
        let mut built = private::Sealed::construct_unchecked(
            self,
            ConstructionToken(()),
            inputs,
            data,
            resource,
        )?;
        validate_output(kind, built.output_schema.as_ref())?;
        built.operation = match (kind, built.operation) {
            (OperationKind::ExclusiveTransform(_), Operation::Atomic(op)) => {
                Operation::Turn(exclusive_turn(op))
            }
            (OperationKind::AtomicTransform(_), op @ Operation::Atomic(_))
            | (
                OperationKind::Scan
                | OperationKind::TurnTransform(_)
                | OperationKind::ExclusiveTransform(_)
                | OperationKind::Sink(_),
                op @ Operation::Turn(_),
            ) => op,
            _ => return Err(OperationSetupError::ExecutionKind),
        };
        Ok(built)
    }

    /// Preflights runtime-resource presence and exact type.
    ///
    /// # Errors
    /// Returns an error for a missing, unexpected, or wrong-type resource.
    pub fn validate_resource(&self, resource: &RuntimeResource) -> Result<(), OperationSetupError> {
        resource.validate(private::Sealed::resource_type(self))
    }
}

fn validate_inputs(kind: OperationKind, inputs: &[SchemaRef]) -> Result<(), OperationBindError> {
    let expected = kind.input_count() as usize;
    if inputs.len() != expected {
        return Err(OperationBindError::InputCount {
            expected,
            actual: inputs.len(),
        });
    }
    for (input, schema) in inputs.iter().enumerate() {
        validate_schema(schema)
            .map_err(|source| OperationBindError::InvalidInputSchema { input, source })?;
    }
    Ok(())
}

fn validate_output(
    kind: OperationKind,
    output: Option<&SchemaRef>,
) -> Result<(), OperationBindError> {
    match (kind.has_output(), output) {
        (true, None) => Err(OperationBindError::MissingOutput),
        (false, Some(_)) => Err(OperationBindError::UnexpectedOutput),
        (true, Some(schema)) => validate_schema(schema)
            .map_err(|source| OperationBindError::InvalidOutputSchema { source }),
        (false, None) => Ok(()),
    }
}

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
    #[error("operation kind requires an output schema but construction has none")]
    MissingOutput,
    #[error("outputless operation kind constructed an output schema")]
    UnexpectedOutput,
    #[error("operation output schema is invalid: {source}")]
    InvalidOutputSchema {
        #[source]
        source: SchemaError,
    },
    #[error("operation execution capability does not match its declared kind")]
    ExecutionKind,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OperationSetupError {
    #[error(transparent)]
    Bind(#[from] OperationBindError),
    #[error("operation rejected its input schemas: {source}")]
    Schema {
        #[source]
        source: OperationSchemaError,
    },
    #[error("operation runtime resource was not provided")]
    MissingRuntimeResource,
    #[error("operation runtime resource has the wrong type")]
    WrongRuntimeResource,
    #[error("operation does not accept a runtime resource")]
    UnexpectedRuntimeResource,
    #[error(transparent)]
    Store(#[from] dogpaddle_store::StoreError),
    #[error("operation construction execution capability does not match its declared kind")]
    ExecutionKind,
}

pub(crate) fn schema_error<E>(source: E) -> OperationSetupError
where
    E: Into<OperationSchemaError>,
{
    OperationSetupError::Schema {
        source: source.into(),
    }
}
