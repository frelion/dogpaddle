use super::{
    PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanError, PostgresCdcScanSpec,
};
use crate::operation::OperationError;
use crate::operation::scan::{
    cdc_convert::{ConvertError, Row, SnapshotMarker},
    cdc_runtime::{CdcRuntime, Phase, Source},
};
use arrow_schema::{Fields, SchemaRef};
use dogpaddle_debezium::{Checkpoint, Connector};
use dogpaddle_store::{Cell, Queue};
use serde_json::Value;

pub(super) type PostgresCdcScanOperation = CdcRuntime<PostgresSource>;
pub(super) struct PostgresSource {
    spec: PostgresCdcScanSpec,
    output_projection: Vec<u32>,
    config: PostgresCdcScanConfig,
}

impl PostgresCdcScanOperation {
    pub(super) fn new_bound(
        definition: &PostgresCdcScanDefinition,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        input: Queue<Vec<u8>>,
        config: PostgresCdcScanConfig,
    ) -> Result<Self, dogpaddle_change::CodecError> {
        Self::new(
            PostgresSource {
                spec: definition.spec().clone(),
                output_projection: definition.output_projection().to_vec(),
                config,
            },
            output_schema,
            phase_cell,
            checkpoint,
            input,
            definition.bootstrap_spool_bytes(),
        )
    }
}

impl Source for PostgresSource {
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;
    const CAPTURE_ACCEPTS_STREAMING: bool = true;
    const STREAMING_TOMBSTONES: bool = false;
    fn columns(&self) -> &Fields {
        &self.spec.columns
    }
    fn output_projection(&self) -> &[u32] {
        &self.output_projection
    }
    fn engine_name(&self) -> &str {
        &self.spec.engine_name
    }
    fn table_topic(&self) -> String {
        format!(
            "{}.{}.{}",
            self.spec.engine_name, self.spec.schema, self.spec.table
        )
    }
    fn snapshot_marker(
        &self,
        payload: &Row,
        capturing: bool,
    ) -> Result<SnapshotMarker, ConvertError> {
        let _ = capturing;
        validate_metadata(payload, &self.spec.schema, &self.spec.table)
    }
    fn conversion_error(error: ConvertError) -> OperationError {
        PostgresCdcScanError::from(error).into()
    }
    fn start_snapshot(&self) -> Result<Connector, OperationError> {
        Ok(self.config.start_snapshot(&self.spec)?)
    }
    fn start_streaming(&self, checkpoint: &Checkpoint) -> Result<Connector, OperationError> {
        Ok(self.config.start_streaming(&self.spec, checkpoint)?)
    }
    fn cleanup_snapshot(&self) -> Result<(), OperationError> {
        self.config.drop_snapshot_slot(&self.spec)?;
        Ok(())
    }
    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        _input_empty: bool,
    ) -> Result<Option<Checkpoint>, OperationError> {
        match phase {
            Phase::Sealed | Phase::Streaming => Ok(Some(parse_checkpoint(
                bytes.ok_or(PostgresCdcScanError::InvalidState(
                    "sealed CDC scan has no checkpoint",
                ))?,
                &self.spec,
            )?)),
            Phase::Fresh | Phase::Capturing | Phase::Resetting => Ok(None),
        }
    }
    fn invalid_state(message: &'static str) -> OperationError {
        Box::new(PostgresCdcScanError::InvalidState(message))
    }
    fn runtime_error(message: String) -> OperationError {
        Box::new(PostgresCdcScanError::new(message))
    }
    fn spool_full() -> OperationError {
        Box::new(PostgresCdcScanError::BootstrapSpoolFull)
    }
    fn codec_error(error: dogpaddle_change::CodecError) -> OperationError {
        Box::new(PostgresCdcScanError::from(error))
    }
}
fn parse_checkpoint(
    bytes: Vec<u8>,
    spec: &PostgresCdcScanSpec,
) -> Result<Checkpoint, PostgresCdcScanError> {
    let checkpoint = Checkpoint::from_bytes(bytes)
        .map_err(|_| PostgresCdcScanError::InvalidState("CDC scan checkpoint is invalid"))?;
    if !checkpoint.matches(&spec.engine_name, super::connection::CONNECTOR_CLASS) {
        return Err(PostgresCdcScanError::InvalidState(
            "CDC scan checkpoint belongs to another PostgreSQL connector",
        ));
    }
    Ok(checkpoint)
}

fn validate_metadata(
    payload: &Row,
    table_schema: &str,
    table: &str,
) -> Result<SnapshotMarker, ConvertError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium CDC metadata"))?;
    for (field, expected) in [
        ("schema", table_schema),
        ("table", table),
        ("connector", "postgresql"),
    ] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium CDC metadata does not match configured {field}"
            )));
        }
    }
    match metadata.get("snapshot") {
        Some(Value::String(value)) if matches!(value.as_str(), "true" | "first") => {
            Ok(SnapshotMarker::Snapshot)
        }
        Some(Value::String(value)) if value == "last" => Ok(SnapshotMarker::Last),
        Some(Value::Null) => Ok(SnapshotMarker::Streaming),
        _ => Err(invalid(
            "Debezium CDC metadata has an invalid snapshot marker",
        )),
    }
}

fn invalid(message: impl Into<String>) -> ConvertError {
    ConvertError::Invalid(message.into())
}

#[cfg(test)]
impl PostgresSource {
    pub(super) fn for_test(spec: &PostgresCdcScanSpec, projection: &[u32]) -> Self {
        Self {
            spec: spec.clone(),
            output_projection: projection.to_vec(),
            config: PostgresCdcScanConfig::new_unencrypted(
                "/unused/runtime",
                "localhost",
                1,
                "unused",
                "unused",
                "",
            )
            .unwrap(),
        }
    }
}
