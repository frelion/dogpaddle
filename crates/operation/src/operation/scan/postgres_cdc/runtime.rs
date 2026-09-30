use super::{
    PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanError, PostgresCdcScanSpec,
    convert::{CaptureProgress, convert_capture_values, convert_values},
};
use crate::operation::OperationError;
use crate::operation::scan::cdc_runtime::{Captured, CdcRuntime, Phase, Source};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_debezium::{Checkpoint, Connector, Record};
use dogpaddle_store::{Cell, Queue};

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
        bootstrap_spool: Queue<Vec<u8>>,
        published: Queue<Vec<u8>>,
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
            bootstrap_spool,
            published,
            definition.bootstrap_spool_bytes(),
        )
    }
}

impl Source for PostgresSource {
    type Progress = CaptureProgress;
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;
    fn source_fields(&self) -> usize {
        self.spec.columns.len()
    }
    fn data_envelopes(&self, records: &[Record]) -> usize {
        let topic = format!(
            "{}.{}.{}",
            self.spec.engine_name, self.spec.schema, self.spec.table
        );
        records
            .iter()
            .filter(|record| record.topic() == Some(topic.as_str()))
            .count()
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
    fn capture(
        &self,
        schema: SchemaRef,
        records: &[Record],
        progress: Self::Progress,
    ) -> Result<Captured<Self::Progress>, OperationError> {
        Ok(convert_capture_values(
            &self.spec,
            &self.output_projection,
            schema,
            records
                .iter()
                .map(|record| (record.topic(), record.value())),
            progress,
        )?)
    }
    fn stream(
        &self,
        schema: SchemaRef,
        records: &[Record],
    ) -> Result<Option<Change>, OperationError> {
        Ok(convert_values(
            &self.spec,
            &self.output_projection,
            schema,
            records
                .iter()
                .map(|record| (record.topic(), record.value())),
        )?)
    }
    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        _spool_empty: bool,
    ) -> Result<Option<Checkpoint>, OperationError> {
        match phase {
            Phase::Publishing | Phase::Streaming => Ok(Some(parse_checkpoint(
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
