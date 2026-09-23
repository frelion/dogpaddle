use super::{
    PostgresCdcScanConfig, PostgresCdcScanError, PostgresCdcScanSpec,
    convert::{CaptureProgress, convert_capture_records, convert_records},
};
use crate::operation::OperationError;
use crate::operation::scan::cdc_runtime::{Captured, CdcRuntime, Phase, Source};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_debezium::{Checkpoint, Connector, Record};
use dogpaddle_store::{Cell, Queue};
use std::num::NonZeroU64;

pub(super) type PostgresCdcScanOperation = CdcRuntime<PostgresSource>;
pub(super) struct PostgresSource {
    spec: PostgresCdcScanSpec,
    config: PostgresCdcScanConfig,
}

impl PostgresCdcScanOperation {
    pub(super) fn new_bound(
        spec: PostgresCdcScanSpec,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        bootstrap_spool: Queue<Vec<u8>>,
        config: PostgresCdcScanConfig,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Self {
        Self::new(
            PostgresSource { spec, config },
            output_schema,
            phase_cell,
            checkpoint,
            bootstrap_spool,
            bootstrap_spool_bytes,
        )
    }
}

impl Source for PostgresSource {
    type Progress = CaptureProgress;
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;
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
        Ok(convert_capture_records(
            &self.spec.columns,
            schema,
            &self.spec.engine_name,
            &self.spec.schema,
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
            &self.spec.schema,
            &self.spec.table,
            records,
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

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch};
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use dogpaddle_change::{Change, encode_change};
    use dogpaddle_store::{Store, Transactions};

    use super::*;
    use crate::operation::scan::cdc_runtime::NextStep;
    use crate::operation::scan::{PostgresColumn, PostgresType};
    use crate::operation::{Action, Turn, TurnOperation};
    use std::sync::Arc;

    fn config() -> PostgresCdcScanConfig {
        PostgresCdcScanConfig::new_unencrypted(
            "/nonexistent/dogpaddle-runtime",
            "127.0.0.1",
            1,
            "shop",
            "cdc",
            "password",
        )
        .unwrap()
    }

    fn checkpoint() -> Vec<u8> {
        BASE64_STANDARD
            .decode(
                "RFBEQkNQMDEAAQAAAAZvcmRlcnMAAAAyaW8uZGViZXppdW0uY29ubmVjdG9yLnBvc3RncmVzcWwuUG9zdGdyZXNDb25uZWN0b3IAAAAAUVBN2Q==",
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

    fn queue_capacity() -> NonZeroU64 {
        NonZeroU64::new(u64::MAX).unwrap()
    }

    fn queued_bytes(value: &[u8]) -> u64 {
        u64::try_from(value.len()).unwrap() + 8
    }

    struct Fixture {
        _root: tempfile::TempDir,
        operation: PostgresCdcScanOperation,
        phase: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: Queue<Vec<u8>>,
        transactions: Transactions,
    }

    impl Fixture {
        fn create() -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut store = Store::create(root.path().join("store")).unwrap();
            let phase = store.create_data::<Cell<u32>>("phase").unwrap();
            let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
            let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
            let columns = vec![PostgresColumn::new("id", PostgresType::Int64, false)];
            let schema = super::super::schema::compile(&columns).unwrap();
            let operation = PostgresCdcScanOperation::new_bound(
                PostgresCdcScanSpec {
                    engine_name: "orders".to_owned(),
                    database: "shop".to_owned(),
                    schema: "public".to_owned(),
                    table: "orders".to_owned(),
                    slot: "orders_slot".to_owned(),
                    publication: "orders_pub".to_owned(),
                    system_identifier: "123".to_owned(),
                    database_oid: 42,
                    table_oid: 43,
                    columns,
                },
                schema,
                phase.clone(),
                checkpoint.clone(),
                spool.clone(),
                config(),
                NonZeroU64::new(1024 * 1024).unwrap(),
            );
            Self {
                _root: root,
                operation,
                phase,
                checkpoint,
                spool,
                transactions: store.into_transactions(),
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
            let durable = (
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
            durable
        }
    }

    #[test]
    fn bootstrap_spool_capacity_is_enforced_by_the_queue_when_empty() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
        let mut transactions = store.into_transactions();
        let encoded = vec![0; 100];

        let transaction = transactions.begin();
        let mut access = spool.access(transaction.access()).unwrap();
        assert!(
            !access
                .try_push(&encoded, NonZeroU64::new(107).unwrap())
                .unwrap()
        );
        assert!(
            access
                .try_push(&encoded, NonZeroU64::new(108).unwrap())
                .unwrap()
        );
        assert_eq!(access.queued_bytes().unwrap(), 108);
        transaction.commit().unwrap();
    }

    #[test]
    fn fresh_is_durably_capturing_before_external_start() {
        let mut fixture = Fixture::create();
        assert!(matches!(fixture.rollback(), Action::Commit(None)));
        assert_eq!(fixture.operation.next_step, NextStep::Restore);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
        assert!(matches!(fixture.rollback(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (None, None, 0));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (Some(1), None, 0));
    }

    #[test]
    fn reopening_capture_requires_slot_cleanup_before_durable_reset() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&1)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint())
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
        assert_eq!(fixture.operation.next_step, NextStep::PrepareReset);
        assert_eq!(
            fixture.durable(),
            (Some(1), Some(checkpoint()), queued_bytes(&encoded))
        );
    }

    #[test]
    fn captured_output_without_its_checkpoint_is_rejected() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&1)
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
            panic!("expected restore turn");
        };
        let transaction = fixture.transactions.begin();
        assert!(prepared.apply(transaction.access()).is_err());
        drop(transaction);
        assert_eq!(fixture.durable(), (Some(1), None, queued_bytes(&encoded)));
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
            .set(&2)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint())
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
            panic!("expected publish turn");
        };
        let transaction = fixture.transactions.begin();
        let (action, completion) = prepared.apply(transaction.access()).unwrap();
        assert!(matches!(action, Action::Commit(Some(_))));
        drop(transaction);
        drop(completion);
        assert_eq!(
            fixture.durable(),
            (Some(2), Some(checkpoint()), queued_bytes(&encoded))
        );

        assert!(matches!(fixture.commit(), Action::Commit(Some(_))));
        assert_eq!(fixture.durable(), (Some(3), Some(checkpoint()), 0));
    }

    #[test]
    fn reset_pops_at_most_one_entry_per_turn_before_returning_fresh() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 1)).unwrap();
        let transaction = fixture.transactions.begin();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&4)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint())
            .unwrap();
        let mut spool = fixture.spool.access(transaction.access()).unwrap();
        assert!(spool.try_push(&encoded, queue_capacity()).unwrap());
        assert!(spool.try_push(&encoded, queue_capacity()).unwrap());
        transaction.commit().unwrap();

        fixture.commit();
        fixture.commit();
        assert_eq!(fixture.durable().2, queued_bytes(&encoded));
        fixture.rollback();
        assert_eq!(
            fixture.durable(),
            (Some(4), Some(checkpoint()), queued_bytes(&encoded))
        );
        assert_eq!(fixture.operation.next_step, NextStep::Reset);
        fixture.commit();
        assert_eq!(fixture.durable(), (None, None, 0));
        assert_eq!(fixture.operation.next_step, NextStep::BeginCapture);
    }
}
