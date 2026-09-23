use super::{
    MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanSpec,
    convert::{SnapshotProgress, convert_records, convert_snapshot_records},
};
use crate::operation::OperationError;
use crate::operation::scan::cdc_runtime::{Captured, CdcRuntime, Phase, Source};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_debezium::{Checkpoint, Connector, Record};
use dogpaddle_store::{Cell, Queue};
use std::num::NonZeroU64;

pub(super) type MySqlCdcScanOperation = CdcRuntime<MySqlSource>;
pub(super) struct MySqlSource {
    spec: MySqlCdcScanSpec,
    config: MySqlCdcScanConfig,
}

impl MySqlCdcScanOperation {
    pub(super) fn new_bound(
        spec: MySqlCdcScanSpec,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        bootstrap_spool: Queue<Vec<u8>>,
        bootstrap_spool_bytes: NonZeroU64,
        config: MySqlCdcScanConfig,
    ) -> Self {
        Self::new(
            MySqlSource { spec, config },
            output_schema,
            phase_cell,
            checkpoint,
            bootstrap_spool,
            bootstrap_spool_bytes,
        )
    }
}

impl Source for MySqlSource {
    type Progress = SnapshotProgress;
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = false;
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
        Ok(convert_snapshot_records(
            &self.spec.columns,
            schema,
            &self.spec.engine_name,
            &self.spec.database,
            &self.spec.table,
            records,
            progress,
        )?)
    }
    fn stream(
        &self,
        schema: SchemaRef,
        records: &[Record],
    ) -> Result<Option<Change>, OperationError> {
        Ok(convert_records(
            &self.spec.columns,
            schema,
            &self.spec.engine_name,
            &self.spec.database,
            &self.spec.table,
            records,
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use arrow_array::{Int64Array, RecordBatch};
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use dogpaddle_change::{Change, encode_change};
    use dogpaddle_store::{Cell, Queue, Store};

    const CAPTURING: u32 = 1;
    const RESETTING: u32 = 4;
    const PUBLISHING: u32 = 2;
    const STREAMING: u32 = 3;
    use super::*;
    use crate::operation::scan::cdc_runtime::NextStep;
    use crate::operation::scan::{MySqlColumn, MySqlType};
    use crate::operation::{Action, Turn, TurnOperation};
    use std::sync::Arc;

    fn checkpoint() -> Checkpoint {
        Checkpoint::from_bytes(
            BASE64_STANDARD
                .decode(
                    "RFBEQkNQMDEAAQAAAAZvcmRlcnMAAAAqaW8uZGViZXppdW0uY29ubmVjdG9yLm15c3FsLk15U3FsQ29ubmVjdG9yAAAAAQAAAAVteXNxbAAAAAMAAQK8UTFt",
                )
                .unwrap(),
        )
        .unwrap()
    }

    fn change(schema: SchemaRef, value: i64) -> Change {
        Change::try_new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap(),
            Int64Array::from(vec![1]),
        )
        .unwrap()
    }

    fn config() -> MySqlCdcScanConfig {
        MySqlCdcScanConfig::new_unencrypted(
            "/nonexistent/dogpaddle-runtime",
            "127.0.0.1",
            1,
            "shop",
            "cdc",
            "password",
        )
        .unwrap()
    }

    fn queue_capacity() -> NonZeroU64 {
        NonZeroU64::new(u64::MAX).unwrap()
    }

    fn queued_bytes(value: &[u8]) -> u64 {
        u64::try_from(value.len()).unwrap() + 8
    }

    struct Fixture {
        operation: MySqlCdcScanOperation,
        phase: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: Queue<Vec<u8>>,
        transactions: dogpaddle_store::Transactions,
        _root: tempfile::TempDir,
    }

    impl Fixture {
        fn create() -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut store = Store::create(root.path().join("store")).unwrap();
            let phase = store.create_data::<Cell<u32>>("phase").unwrap();
            let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
            let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
            let columns = vec![MySqlColumn::new("id", MySqlType::Int64, false)];
            let schema = super::super::schema::compile(&columns).unwrap();
            let operation = MySqlCdcScanOperation::new_bound(
                MySqlCdcScanSpec {
                    engine_name: "orders".to_owned(),
                    database: "shop".to_owned(),
                    table: "orders".to_owned(),
                    server_uuid: "01234567-89ab-cdef-0123-456789abcdef".to_owned(),
                    table_id: 43,
                    columns,
                },
                schema,
                phase.clone(),
                checkpoint.clone(),
                spool.clone(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                config(),
            );
            Self {
                operation,
                phase,
                checkpoint,
                spool,
                transactions: store.into_transactions(),
                _root: root,
            }
        }

        fn commit(&mut self) -> Action {
            let Turn::Ready(prepared) = self.operation.turn(None).unwrap() else {
                panic!("expected prepared turn");
            };
            let transaction = self.transactions.begin();
            let (action, completion) = prepared.apply(transaction.access()).unwrap();
            transaction.commit().unwrap();
            completion.run().unwrap();
            action
        }

        fn rollback(&mut self) -> Action {
            let Turn::Ready(prepared) = self.operation.turn(None).unwrap() else {
                panic!("expected prepared turn");
            };
            let transaction = self.transactions.begin();
            let (action, completion) = prepared.apply(transaction.access()).unwrap();
            drop(transaction);
            drop(completion);
            action
        }

        fn durable(&mut self) -> (Option<u32>, Option<Vec<u8>>, u64) {
            let transaction = self.transactions.begin();
            let result = (
                self.phase
                    .access(transaction.access())
                    .unwrap()
                    .get()
                    .unwrap(),
                self.checkpoint
                    .access(transaction.access())
                    .unwrap()
                    .get()
                    .unwrap(),
                self.spool
                    .access(transaction.access())
                    .unwrap()
                    .queued_bytes()
                    .unwrap(),
            );
            transaction.commit().unwrap();
            result
        }
    }

    #[test]
    fn fresh_is_durably_pinned_before_snapshot_start() {
        let mut fixture = Fixture::create();
        assert!(matches!(fixture.rollback(), Action::Commit(None)));
        assert_eq!(fixture.operation.next_step, NextStep::Restore);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
        assert!(matches!(fixture.rollback(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (None, None, 0));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (Some(CAPTURING), None, 0));
    }

    #[test]
    fn reopening_a_partial_capture_enters_incremental_reset() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&CAPTURING)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint().as_bytes().to_vec())
            .unwrap();
        let mut spool = fixture.spool.access(transaction.access()).unwrap();
        assert!(spool.try_push(&encoded, queue_capacity()).unwrap());
        assert!(spool.try_push(&encoded, queue_capacity()).unwrap());
        transaction.commit().unwrap();

        fixture.rollback();
        assert_eq!(fixture.durable().0, Some(CAPTURING));
        assert_eq!(fixture.operation.next_step, NextStep::Restore);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable().0, Some(RESETTING));
        fixture.commit();
        assert_eq!(fixture.durable().2, queued_bytes(&encoded));
        fixture.rollback();
        assert_eq!(
            fixture.durable(),
            (
                Some(RESETTING),
                Some(checkpoint().as_bytes().to_vec()),
                queued_bytes(&encoded)
            )
        );
        assert_eq!(fixture.operation.next_step, NextStep::Reset);
        fixture.commit();
        assert_eq!(fixture.durable(), (None, None, 0));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
    }

    #[test]
    fn partial_capture_without_its_atomic_checkpoint_is_rejected() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&CAPTURING)
            .unwrap();
        assert!(
            fixture
                .spool
                .access(transaction.access())
                .unwrap()
                .try_push(&encoded, queue_capacity())
                .unwrap()
        );
        transaction.commit().unwrap();

        let Turn::Ready(prepared) = fixture.operation.turn(None).unwrap() else {
            panic!("expected restore");
        };
        let transaction = fixture.transactions.begin();
        assert!(prepared.apply(transaction.access()).is_err());
        drop(transaction);
        assert_eq!(
            fixture.durable(),
            (Some(CAPTURING), None, queued_bytes(&encoded))
        );
    }

    #[test]
    fn publishing_pop_and_streaming_transition_rollback_together() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 9)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&PUBLISHING)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint().as_bytes().to_vec())
            .unwrap();
        assert!(
            fixture
                .spool
                .access(transaction.access())
                .unwrap()
                .try_push(&encoded, queue_capacity())
                .unwrap()
        );
        transaction.commit().unwrap();
        fixture.commit();

        let Turn::Ready(prepared) = fixture.operation.turn(None).unwrap() else {
            panic!("expected publish");
        };
        let transaction = fixture.transactions.begin();
        let (action, completion) = prepared.apply(transaction.access()).unwrap();
        assert!(matches!(action, Action::Commit(Some(_))));
        drop(transaction);
        drop(completion);
        assert_eq!(fixture.durable().2, queued_bytes(&encoded));
        assert_eq!(fixture.durable().0, Some(PUBLISHING));

        assert!(matches!(fixture.commit(), Action::Commit(Some(_))));
        assert_eq!(fixture.durable().2, 0);
        assert_eq!(fixture.durable().0, Some(STREAMING));
    }

    #[test]
    fn bootstrap_spool_capacity_is_a_hard_limit_even_when_empty() {
        let mut fixture = Fixture::create();
        let encoded = [0; 9];
        let transaction = fixture.transactions.begin();
        let mut spool = fixture.spool.access(transaction.access()).unwrap();
        assert!(
            !spool
                .try_push(
                    &encoded.to_vec(),
                    NonZeroU64::new(u64::try_from(encoded.len()).unwrap() + 7).unwrap(),
                )
                .unwrap()
        );
        assert!(spool.is_empty().unwrap());
        assert!(
            spool
                .try_push(
                    &encoded.to_vec(),
                    NonZeroU64::new(u64::try_from(encoded.len()).unwrap() + 8).unwrap(),
                )
                .unwrap()
        );
        transaction.commit().unwrap();
        assert_eq!(fixture.durable().2, queued_bytes(&encoded));
    }
}
