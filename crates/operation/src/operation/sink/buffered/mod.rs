//! Durable buffering and delivery protocol shared by external sinks.

mod batch;
mod runtime;
mod state;

use std::fmt;

use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_store::{Cell, DataScope, OrderedMap};

use crate::{
    ConstructedOperation, OperationSetupError, definition::schema_error, operation::OperationError,
};

use super::relation::RelationTarget;

pub(crate) use batch::DeliveryBatch;
pub(crate) use runtime::BufferedSink;
pub use runtime::SinkPending;

pub(crate) const CONTROL: &str = "sink.control";
pub(crate) const BUFFER: &str = "sink.buffer";

pub(crate) fn construct<T: RelationTarget>(
    schema: arrow_schema::SchemaRef,
    target: T,
    scope: &mut DataScope<'_>,
) -> Result<ConstructedOperation, OperationSetupError> {
    let control = scope.data::<Cell<Vec<u8>>>(CONTROL)?;
    let buffer = scope.data::<OrderedMap<u64, Vec<u8>>>(BUFFER)?;
    let codec = SchemaBoundChangeCodec::try_new(schema).map_err(schema_error)?;
    Ok(ConstructedOperation::sink(BufferedSink::new(
        codec, target, control, buffer,
    )))
}
pub(crate) const MAX_TARGET_BATCH_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
struct BufferedSinkError(String);

impl fmt::Display for BufferedSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "buffered sink: {}", self.0)
    }
}

impl std::error::Error for BufferedSinkError {}

fn invalid(message: impl Into<String>) -> OperationError {
    Box::new(BufferedSinkError(message.into()))
}

#[cfg(test)]
mod tests;
