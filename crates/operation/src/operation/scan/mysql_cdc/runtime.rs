use super::{
    MySqlCdcScanConfig, MySqlCdcScanDefinition, MySqlCdcScanError, MySqlCdcScanSpec,
    convert::{SnapshotProgress, convert_snapshot_values, convert_values},
};
use crate::operation::OperationError;
use crate::operation::scan::cdc_runtime::{Captured, CdcRuntime, Phase, Source};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_debezium::{Checkpoint, Connector, Record};
use dogpaddle_store::{Cell, Queue};

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
        bootstrap_spool: Queue<Vec<u8>>,
        published: Queue<Vec<u8>>,
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
            bootstrap_spool,
            published,
            definition.bootstrap_spool_bytes(),
        )
    }
}

impl Source for MySqlSource {
    type Progress = SnapshotProgress;
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = false;
    fn source_fields(&self) -> usize {
        self.spec.columns.len()
    }
    fn data_envelopes(&self, records: &[Record]) -> usize {
        let topic = format!(
            "{}.{}.{}",
            self.spec.engine_name, self.spec.database, self.spec.table
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
        Ok(())
    }
    fn capture(
        &self,
        schema: SchemaRef,
        records: &[Record],
        progress: Self::Progress,
    ) -> Result<Captured<Self::Progress>, OperationError> {
        Ok(convert_snapshot_values(
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
        spool_empty: bool,
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
        if matches!(phase, Phase::Publishing | Phase::Streaming) && checkpoint.is_none() {
            return Err(
                MySqlCdcScanError::InvalidState("sealed CDC scan has no checkpoint").into(),
            );
        }
        if phase == Phase::Resetting && checkpoint.is_none() && !spool_empty {
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
