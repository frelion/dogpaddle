use super::{MySqlCdcScanConfig, MySqlCdcScanDefinition, MySqlCdcScanError, MySqlCdcScanSpec};
use crate::operation::OperationError;
use crate::operation::scan::{
    cdc_convert::{ConvertError, Row, SnapshotMarker},
    cdc_runtime::{CdcRuntime, Phase, Source},
};
use arrow_schema::{Fields, SchemaRef};
use dogpaddle_debezium::{Checkpoint, Connector};
use dogpaddle_store::{Cell, Queue};
use serde_json::Value;

pub(super) type MySqlCdcScanOperation = CdcRuntime<MySqlSource>;
pub(super) struct MySqlSource {
    spec: MySqlCdcScanSpec,
    output_projection: Vec<u32>,
    config: MySqlCdcScanConfig,
}

impl MySqlCdcScanOperation {
    pub(super) fn new_bound(
        definition: &MySqlCdcScanDefinition,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        input: Queue<Vec<u8>>,
        config: MySqlCdcScanConfig,
    ) -> Result<Self, dogpaddle_change::CodecError> {
        Self::new(
            MySqlSource {
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

impl Source for MySqlSource {
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = false;
    const CAPTURE_ACCEPTS_STREAMING: bool = false;
    const STREAMING_TOMBSTONES: bool = true;
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
            self.spec.engine_name, self.spec.database, self.spec.table
        )
    }
    fn snapshot_marker(
        &self,
        payload: &Row,
        capturing: bool,
    ) -> Result<SnapshotMarker, ConvertError> {
        if capturing {
            validate_snapshot_metadata(payload, &self.spec.database, &self.spec.table).map(|last| {
                if last {
                    SnapshotMarker::Last
                } else {
                    SnapshotMarker::Snapshot
                }
            })
        } else {
            validate_metadata(payload, &self.spec.database, &self.spec.table)?;
            Ok(SnapshotMarker::Streaming)
        }
    }
    fn conversion_error(error: ConvertError) -> OperationError {
        MySqlCdcScanError::from(error).into()
    }
    fn start_snapshot(&self) -> Result<Connector, OperationError> {
        Ok(self.config.start_snapshot(&self.spec)?)
    }
    fn start_streaming(&self, checkpoint: &Checkpoint) -> Result<Connector, OperationError> {
        Ok(self.config.start_streaming(&self.spec, checkpoint)?)
    }
    fn cleanup_snapshot(&self) -> Result<(), OperationError> {
        Ok(())
    }
    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        input_empty: bool,
    ) -> Result<Option<Checkpoint>, OperationError> {
        let checkpoint = bytes
            .map(Checkpoint::from_bytes)
            .transpose()
            .map_err(|_| MySqlCdcScanError::InvalidState("CDC scan checkpoint is invalid"))?;
        if checkpoint.as_ref().is_some_and(|checkpoint| {
            !checkpoint.matches(&self.spec.engine_name, super::definition::CONNECTOR_CLASS)
        }) {
            return Err(MySqlCdcScanError::InvalidState(
                "CDC scan checkpoint belongs to another MySQL connector",
            )
            .into());
        }
        if matches!(phase, Phase::Sealed | Phase::Streaming) && checkpoint.is_none() {
            return Err(
                MySqlCdcScanError::InvalidState("sealed CDC scan has no checkpoint").into(),
            );
        }
        if phase == Phase::Resetting && checkpoint.is_none() && !input_empty {
            return Err(MySqlCdcScanError::InvalidState(
                "partial bootstrap spool has no checkpoint",
            )
            .into());
        }
        Ok(checkpoint)
    }
    fn invalid_state(message: &'static str) -> OperationError {
        Box::new(MySqlCdcScanError::InvalidState(message))
    }
    fn runtime_error(message: String) -> OperationError {
        Box::new(MySqlCdcScanError::new(message))
    }
    fn spool_full() -> OperationError {
        Box::new(MySqlCdcScanError::BootstrapSpoolFull)
    }
    fn codec_error(error: dogpaddle_change::CodecError) -> OperationError {
        Box::new(error)
    }
}

fn validate_snapshot_metadata(
    payload: &Row,
    database: &str,
    table: &str,
) -> Result<bool, ConvertError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium snapshot metadata"))?;
    for (field, expected) in [("db", database), ("table", table), ("connector", "mysql")] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium snapshot metadata does not match configured {field}"
            )));
        }
    }
    match metadata.get("snapshot").and_then(Value::as_str) {
        Some("true" | "first" | "first_in_data_collection") => Ok(false),
        Some("last" | "last_in_data_collection") => Ok(true),
        _ => Err(invalid(
            "Debezium record is not part of the initial snapshot",
        )),
    }
}

fn validate_metadata(payload: &Row, database: &str, table: &str) -> Result<(), ConvertError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium CDC metadata"))?;
    for (field, expected) in [("db", database), ("table", table), ("connector", "mysql")] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium CDC metadata does not match configured {field}"
            )));
        }
    }
    // SnapshotRecord.FALSE deliberately leaves this Struct field unset in
    // some Debezium paths. Snapshot operations are independently rejected by
    // the operation guard below.
    if !matches!(
        metadata.get("snapshot"),
        Some(Value::Null | Value::Bool(false))
    ) && metadata.get("snapshot").and_then(Value::as_str) != Some("false")
    {
        return Err(invalid(
            "Debezium CDC metadata does not identify a non-snapshot record",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConvertError {
    ConvertError::Invalid(message.into())
}

#[cfg(test)]
impl MySqlSource {
    pub(super) fn for_test(spec: &MySqlCdcScanSpec, projection: &[u32]) -> Self {
        Self {
            spec: spec.clone(),
            output_projection: projection.to_vec(),
            config: MySqlCdcScanConfig::new_unencrypted(
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
