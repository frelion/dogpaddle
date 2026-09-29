use std::{any::TypeId, error::Error, num::NonZeroU32};

use arrow_schema::SchemaRef;
use dogpaddle_change::{SchemaError, validate_schema};
use dogpaddle_store::DataScope;
use serde::Serialize;
use thiserror::Error;

use crate::{
    RuntimeResource,
    operation::{AtomicOperation, Operation, TurnOperation, scan, sink, transform},
};

/// Type-erased error from a concrete operation's pure Schema compiler.
pub type OperationSchemaError = Box<dyn Error + Send + Sync + 'static>;

const TWO_INPUTS: NonZeroU32 = NonZeroU32::new(2).expect("two is nonzero");

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
    /// Outputless terminal operation.
    Sink(NonZeroU32),
}
impl OperationKind {
    /// Returns the exact number of ordered inputs.
    #[must_use]
    pub const fn input_count(self) -> u32 {
        match self {
            Self::Scan => 0,
            Self::AtomicTransform(n) | Self::TurnTransform(n) | Self::Sink(n) => n.get(),
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

/// Persistent, typed plan for one built-in operation.
///
/// This enum is the complete set of operations accepted by Flow. Each variant
/// contains only persistent plan data; runtime clients and state handles are
/// acquired through [`Self::construct`].
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OperationDefinition {
    /// `MySqlCdcScan` operation plan.
    MySqlCdcScan(Box<scan::MySqlCdcScanDefinition>),
    /// `PostgresCdcScan` operation plan.
    PostgresCdcScan(Box<scan::PostgresCdcScanDefinition>),
    /// `SequenceScan` operation plan.
    SequenceScan(Box<scan::SequenceScanDefinition>),
    /// `Aggregate` operation plan.
    Aggregate(Box<transform::AggregateDefinition>),
    /// `AsOfJoin` operation plan.
    AsOfJoin(Box<transform::AsOfJoinDefinition>),
    /// `Distinct` operation plan.
    Distinct(Box<transform::DistinctDefinition>),
    /// `RunningEventCount` operation plan.
    RunningEventCount(Box<transform::RunningEventCountDefinition>),
    /// `Filter` operation plan.
    Filter(Box<transform::FilterDefinition>),
    /// `EquiJoin` operation plan.
    EquiJoin(Box<transform::EquiJoinDefinition>),
    /// `Select` operation plan.
    Select(Box<transform::SelectDefinition>),
    /// `UnionAll` operation plan.
    UnionAll(Box<transform::UnionAllDefinition>),
    /// `SchemaAlign` operation plan.
    SchemaAlign(Box<transform::SchemaAlignDefinition>),
    /// `ClickHouseSink` operation plan.
    ClickHouseSink(Box<sink::ClickHouseSinkDefinition>),
    /// `Discard` operation plan.
    Discard(Box<sink::DiscardDefinition>),
    /// `DorisSink` operation plan.
    DorisSink(Box<sink::DorisSinkDefinition>),
    /// `PostgresSink` operation plan.
    PostgresSink(Box<sink::PostgresSinkDefinition>),
    /// `SqliteSink` operation plan.
    SqliteSink(Box<sink::SqliteSinkDefinition>),
}

impl From<scan::MySqlCdcScanDefinition> for OperationDefinition {
    fn from(definition: scan::MySqlCdcScanDefinition) -> Self {
        Self::MySqlCdcScan(Box::new(definition))
    }
}

impl From<scan::PostgresCdcScanDefinition> for OperationDefinition {
    fn from(definition: scan::PostgresCdcScanDefinition) -> Self {
        Self::PostgresCdcScan(Box::new(definition))
    }
}

impl From<scan::SequenceScanDefinition> for OperationDefinition {
    fn from(definition: scan::SequenceScanDefinition) -> Self {
        Self::SequenceScan(Box::new(definition))
    }
}

impl From<transform::AggregateDefinition> for OperationDefinition {
    fn from(definition: transform::AggregateDefinition) -> Self {
        Self::Aggregate(Box::new(definition))
    }
}

impl From<transform::AsOfJoinDefinition> for OperationDefinition {
    fn from(definition: transform::AsOfJoinDefinition) -> Self {
        Self::AsOfJoin(Box::new(definition))
    }
}

impl From<transform::DistinctDefinition> for OperationDefinition {
    fn from(definition: transform::DistinctDefinition) -> Self {
        Self::Distinct(Box::new(definition))
    }
}

impl From<transform::RunningEventCountDefinition> for OperationDefinition {
    fn from(definition: transform::RunningEventCountDefinition) -> Self {
        Self::RunningEventCount(Box::new(definition))
    }
}

impl From<transform::FilterDefinition> for OperationDefinition {
    fn from(definition: transform::FilterDefinition) -> Self {
        Self::Filter(Box::new(definition))
    }
}

impl From<transform::EquiJoinDefinition> for OperationDefinition {
    fn from(definition: transform::EquiJoinDefinition) -> Self {
        Self::EquiJoin(Box::new(definition))
    }
}

impl From<transform::SelectDefinition> for OperationDefinition {
    fn from(definition: transform::SelectDefinition) -> Self {
        Self::Select(Box::new(definition))
    }
}

impl From<transform::UnionAllDefinition> for OperationDefinition {
    fn from(definition: transform::UnionAllDefinition) -> Self {
        Self::UnionAll(Box::new(definition))
    }
}

impl From<transform::SchemaAlignDefinition> for OperationDefinition {
    fn from(definition: transform::SchemaAlignDefinition) -> Self {
        Self::SchemaAlign(Box::new(definition))
    }
}

impl From<sink::ClickHouseSinkDefinition> for OperationDefinition {
    fn from(definition: sink::ClickHouseSinkDefinition) -> Self {
        Self::ClickHouseSink(Box::new(definition))
    }
}

impl From<sink::DiscardDefinition> for OperationDefinition {
    fn from(definition: sink::DiscardDefinition) -> Self {
        Self::Discard(Box::new(definition))
    }
}

impl From<sink::DorisSinkDefinition> for OperationDefinition {
    fn from(definition: sink::DorisSinkDefinition) -> Self {
        Self::DorisSink(Box::new(definition))
    }
}

impl From<sink::PostgresSinkDefinition> for OperationDefinition {
    fn from(definition: sink::PostgresSinkDefinition) -> Self {
        Self::PostgresSink(Box::new(definition))
    }
}

impl From<sink::SqliteSinkDefinition> for OperationDefinition {
    fn from(definition: sink::SqliteSinkDefinition) -> Self {
        Self::SqliteSink(Box::new(definition))
    }
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

impl OperationDefinition {
    /// Returns the execution role and exact input arity.
    #[must_use]
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::MySqlCdcScan(_) | Self::PostgresCdcScan(_) | Self::SequenceScan(_) => {
                OperationKind::Scan
            }
            Self::AsOfJoin(_) | Self::EquiJoin(_) => OperationKind::TurnTransform(TWO_INPUTS),
            Self::UnionAll(definition) => OperationKind::AtomicTransform(definition.input_count()),
            Self::Aggregate(_)
            | Self::Distinct(_)
            | Self::RunningEventCount(_)
            | Self::Filter(_)
            | Self::Select(_)
            | Self::SchemaAlign(_) => OperationKind::AtomicTransform(NonZeroU32::MIN),
            Self::ClickHouseSink(_)
            | Self::Discard(_)
            | Self::DorisSink(_)
            | Self::PostgresSink(_)
            | Self::SqliteSink(_) => OperationKind::Sink(NonZeroU32::MIN),
        }
    }

    /// Returns the stable v1 payload tag for this operation.
    #[must_use]
    pub fn persistence_tag(&self) -> u16 {
        match self {
            Self::MySqlCdcScan(_) => scan::mysql_cdc::TAG,
            Self::PostgresCdcScan(_) => scan::postgres_cdc::TAG,
            Self::SequenceScan(_) => scan::sequence::TAG,
            Self::Aggregate(_) => transform::aggregate::TAG,
            Self::AsOfJoin(_) => transform::asof_join::TAG,
            Self::Distinct(_) => transform::distinct::TAG,
            Self::RunningEventCount(_) => transform::running_event_count::TAG,
            Self::Filter(_) => transform::filter::TAG,
            Self::EquiJoin(_) => transform::equi_join::TAG,
            Self::Select(_) => transform::select::TAG,
            Self::UnionAll(_) => transform::union_all::TAG,
            Self::SchemaAlign(_) => transform::schema_align::TAG,
            Self::ClickHouseSink(_) => sink::clickhouse::TAG,
            Self::Discard(_) => sink::discard::TAG,
            Self::DorisSink(_) => sink::doris::TAG,
            Self::PostgresSink(_) => sink::postgres::TAG,
            Self::SqliteSink(_) => sink::sqlite::TAG,
        }
    }

    fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, OperationSchemaError> {
        match self {
            Self::MySqlCdcScan(definition) => definition.output_schema_unchecked(),
            Self::PostgresCdcScan(definition) => definition.output_schema_unchecked(),
            Self::SequenceScan(_) => {
                Ok(Some(scan::SequenceScanDefinition::output_schema_unchecked()))
            }
            Self::Aggregate(definition) => definition.output_schema_unchecked(inputs),
            Self::AsOfJoin(definition) => definition.output_schema_unchecked(inputs),
            Self::Distinct(_) => Ok(Some(
                transform::DistinctDefinition::output_schema_unchecked(inputs),
            )),
            Self::RunningEventCount(_) => Ok(Some(
                transform::RunningEventCountDefinition::output_schema_unchecked(),
            )),
            Self::Filter(definition) => definition.output_schema_unchecked(inputs),
            Self::EquiJoin(definition) => definition.output_schema_unchecked(inputs),
            Self::Select(definition) => definition.output_schema_unchecked(inputs),
            Self::UnionAll(_) => transform::UnionAllDefinition::compile_schema(inputs).map(Some),
            Self::SchemaAlign(definition) => definition.output_schema_unchecked(inputs),
            Self::ClickHouseSink(_) => {
                sink::ClickHouseSinkDefinition::output_schema_unchecked(inputs)?;
                Ok(None)
            }
            Self::Discard(_) => Ok(None),
            Self::DorisSink(_) => {
                sink::DorisSinkDefinition::output_schema_unchecked(inputs)?;
                Ok(None)
            }
            Self::PostgresSink(_) => {
                sink::PostgresSinkDefinition::output_schema_unchecked(inputs)?;
                Ok(None)
            }
            Self::SqliteSink(_) => {
                sink::SqliteSinkDefinition::output_schema_unchecked(inputs)?;
                Ok(None)
            }
        }
    }

    fn construct_unchecked(
        &self,
        inputs: &[SchemaRef],
        data: &mut DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, OperationSetupError> {
        match self {
            Self::MySqlCdcScan(definition) => definition.construct_unchecked(data, resource),
            Self::PostgresCdcScan(definition) => definition.construct_unchecked(data, resource),
            Self::SequenceScan(definition) => (**definition).construct_unchecked(data),
            Self::Aggregate(definition) => definition.construct_unchecked(inputs, data),
            Self::AsOfJoin(definition) => definition.construct_unchecked(inputs, data),
            Self::Distinct(_) => transform::DistinctDefinition::construct_unchecked(inputs, data),
            Self::RunningEventCount(_) => {
                transform::RunningEventCountDefinition::construct_unchecked(inputs, data)
            }
            Self::Filter(definition) => definition.construct_unchecked(inputs),
            Self::EquiJoin(definition) => definition.construct_unchecked(inputs, data),
            Self::Select(definition) => definition.construct_unchecked(inputs),
            Self::UnionAll(definition) => (**definition).construct_unchecked(inputs),
            Self::SchemaAlign(definition) => definition.construct_unchecked(inputs),
            Self::ClickHouseSink(definition) => {
                definition.construct_unchecked(inputs, data, resource)
            }
            Self::Discard(_) => Ok(sink::DiscardDefinition::construct_unchecked()),
            Self::DorisSink(definition) => definition.construct_unchecked(inputs, data, resource),
            Self::PostgresSink(definition) => {
                definition.construct_unchecked(inputs, data, resource)
            }
            Self::SqliteSink(definition) => definition.construct_unchecked(inputs, data),
        }
    }

    fn resource_type(&self) -> Option<TypeId> {
        match self {
            Self::MySqlCdcScan(_) => Some(scan::MySqlCdcScanDefinition::resource_type()),
            Self::PostgresCdcScan(_) => Some(scan::PostgresCdcScanDefinition::resource_type()),
            Self::ClickHouseSink(_) => Some(sink::ClickHouseSinkDefinition::resource_type()),
            Self::DorisSink(_) => Some(sink::DorisSinkDefinition::resource_type()),
            Self::PostgresSink(_) => Some(sink::PostgresSinkDefinition::resource_type()),
            _ => None,
        }
    }

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
        let output = self
            .output_schema_unchecked(inputs)
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
        resource.validate(self.resource_type())?;
        let built = self.construct_unchecked(inputs, data, resource)?;
        validate_output(kind, built.output_schema.as_ref())?;
        if !matches!(
            (kind, &built.operation),
            (OperationKind::AtomicTransform(_), Operation::Atomic(_))
                | (
                    OperationKind::Scan | OperationKind::TurnTransform(_) | OperationKind::Sink(_),
                    Operation::Turn(_),
                )
        ) {
            return Err(OperationSetupError::ExecutionKind);
        }
        Ok(built)
    }

    /// Preflights runtime-resource presence and exact type.
    ///
    /// # Errors
    /// Returns an error for a missing, unexpected, or wrong-type resource.
    pub fn validate_resource(&self, resource: &RuntimeResource) -> Result<(), OperationSetupError> {
        resource.validate(self.resource_type())
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
