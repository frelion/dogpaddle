use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use arrow_schema::SchemaRef;
use dogpaddle_change::{decode_change, encode_change};
use dogpaddle_debezium::{Checkpoint, Connector};
use dogpaddle_store::{AppendLog, Cell, CodecError as StoreCodecError, ScanLimit};

use crate::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
};

use super::{
    MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanSpec,
    convert::{convert_records, convert_snapshot_records},
};

const CAPTURING: u32 = 1;
const PUBLISHING: u32 = 2;
const STREAMING: u32 = 3;
const RESETTING: u32 = 4;
const CONNECTOR_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_OFFSET_BYTES: u64 = size_of::<u64>() as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Fresh,
    Capturing,
    Publishing,
    Streaming,
    Resetting,
}

impl Phase {
    fn decode(encoded: Option<u32>) -> Result<Self, MySqlCdcScanError> {
        match encoded {
            None => Ok(Self::Fresh),
            Some(CAPTURING) => Ok(Self::Capturing),
            Some(PUBLISHING) => Ok(Self::Publishing),
            Some(STREAMING) => Ok(Self::Streaming),
            Some(RESETTING) => Ok(Self::Resetting),
            Some(_) => Err(MySqlCdcScanError::InvalidState("CDC scan phase is invalid")),
        }
    }
}

/// One materialized `MySQL` snapshot and CDC Scan.
///
/// The initial snapshot is first sealed in a private durable spool. Only then
/// is it published to the Station output, after which the connector resumes
/// continuous binlog CDC from the sealed checkpoint.
pub struct MySqlCdcScanOperation {
    spec: MySqlCdcScanSpec,
    output_schema: SchemaRef,
    phase_cell: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    bootstrap_spool: AppendLog<Vec<u8>>,
    bootstrap_spool_bytes: NonZeroU64,
    config: MySqlCdcScanConfig,
    phase: Option<Phase>,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    snapshot_failed: bool,
    restart_connector: bool,
}

impl MySqlCdcScanOperation {
    pub(super) fn new_bound(
        spec: MySqlCdcScanSpec,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        bootstrap_spool: AppendLog<Vec<u8>>,
        bootstrap_spool_bytes: NonZeroU64,
        config: MySqlCdcScanConfig,
    ) -> Self {
        Self {
            spec,
            output_schema,
            phase_cell,
            checkpoint,
            bootstrap_spool,
            bootstrap_spool_bytes,
            config,
            phase: None,
            resume: None,
            connector: None,
            snapshot_failed: false,
            restart_connector: false,
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let encoded_phase = self.phase_cell.access(access)?.get()?;
            let mut phase = Phase::decode(encoded_phase)?;
            let checkpoint = self
                .checkpoint
                .access(access)?
                .get()?
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
            let bounds = self.bootstrap_spool.access(access)?.bounds()?;
            let spool_empty = bounds.is_empty();
            match phase {
                Phase::Fresh if checkpoint.is_some() || !spool_empty => {
                    return Err(MySqlCdcScanError::InvalidState(
                        "fresh CDC scan has bootstrap state",
                    )
                    .into());
                }
                Phase::Publishing if checkpoint.is_none() => {
                    return Err(MySqlCdcScanError::InvalidState(
                        "publishing CDC scan has no sealed checkpoint",
                    )
                    .into());
                }
                Phase::Streaming if checkpoint.is_none() || !spool_empty => {
                    return Err(MySqlCdcScanError::InvalidState(
                        "streaming CDC scan has invalid bootstrap state",
                    )
                    .into());
                }
                Phase::Capturing | Phase::Resetting if checkpoint.is_none() && !spool_empty => {
                    return Err(MySqlCdcScanError::InvalidState(
                        "partial bootstrap spool has no checkpoint",
                    )
                    .into());
                }
                Phase::Capturing => {
                    // A partial snapshot is never resumed. A newly materialized
                    // runtime owns no live connector, so it can durably enter
                    // incremental cleanup immediately.
                    self.phase_cell.access(access)?.set(&RESETTING)?;
                    phase = Phase::Resetting;
                }
                Phase::Fresh | Phase::Publishing | Phase::Streaming | Phase::Resetting => {}
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Some(phase);
                    self.resume = checkpoint;
                    Ok(())
                }),
            ))
        })
    }

    fn begin_capture(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            self.phase_cell.access(access)?.set(&CAPTURING)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Some(Phase::Capturing);
                    Ok(())
                }),
            ))
        })
    }

    fn begin_reset(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            self.phase_cell.access(access)?.set(&RESETTING)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Some(Phase::Resetting);
                    self.snapshot_failed = false;
                    Ok(())
                }),
            ))
        })
    }

    fn reset(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let mut spool = self.bootstrap_spool.access(access)?;
            let bounds = spool.bounds()?;
            let finished = if bounds.is_empty() {
                true
            } else {
                let target = bounds.start + 1;
                spool.truncate_before(target, NonZeroUsize::MIN)?;
                target == bounds.end
            };
            if finished {
                self.checkpoint.access(access)?.clear()?;
                self.phase_cell.access(access)?.clear()?;
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    if finished {
                        self.phase = Some(Phase::Fresh);
                        self.resume = None;
                    }
                    Ok(())
                }),
            ))
        })
    }

    fn capture(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.connector.is_none() {
            match self.config.start_snapshot(&self.spec) {
                Ok(connector) => self.connector = Some(connector),
                Err(error) => {
                    self.snapshot_failed = true;
                    return Err(error.into());
                }
            }
        }
        let connector = self
            .connector
            .as_mut()
            .expect("snapshot connector was started above");
        let delivery = match connector.poll(Duration::ZERO) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(Turn::Idle),
            Err(error) => {
                self.snapshot_failed = true;
                return Err(MySqlCdcScanError::new(format!(
                    "Debezium snapshot poll failed ({:?})",
                    error.kind()
                ))
                .into());
            }
        };
        let snapshot = match convert_snapshot_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.database,
            &self.spec.table,
            delivery.records(),
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.snapshot_failed = true;
                return Err(error.into());
            }
        };
        let encoded_change = match snapshot.change.as_ref().map(encode_change).transpose() {
            Ok(encoded) => encoded,
            Err(error) => {
                self.snapshot_failed = true;
                return Err(error.into());
            }
        };
        let complete = snapshot.complete;
        let durable_checkpoint = delivery.checkpoint().as_bytes().to_vec();
        let resumed_checkpoint = delivery.checkpoint().clone();
        let spool = &self.bootstrap_spool;
        let capacity = self.bootstrap_spool_bytes;
        let checkpoint = &self.checkpoint;
        let phase_cell = &self.phase_cell;
        let phase = &mut self.phase;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            if let Some(encoded) = encoded_change {
                let mut spool = spool.access(access)?;
                require_bootstrap_capacity(spool.retained_bytes()?, encoded.len(), capacity)?;
                spool.append(&encoded)?;
            }
            checkpoint.access(access)?.set(&durable_checkpoint)?;
            if complete {
                phase_cell.access(access)?.set(&PUBLISHING)?;
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        PostCommitError::new(MySqlCdcScanError::new(format!(
                            "Debezium snapshot ACK failed ({:?})",
                            error.kind()
                        )))
                    })?;
                    *resume = Some(resumed_checkpoint);
                    if complete {
                        *phase = Some(Phase::Publishing);
                    }
                    Ok(())
                }),
            ))
        }))
    }

    fn publish(&mut self) -> Result<Turn<'_>, OperationError> {
        self.stop_connector("snapshot")?;
        Ok(Turn::ready(move |access| {
            let mut spool = self.bootstrap_spool.access(access)?;
            let bounds = spool.bounds()?;
            if bounds.is_empty() {
                self.phase_cell.access(access)?.set(&STREAMING)?;
                return Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        self.phase = Some(Phase::Streaming);
                        Ok(())
                    }),
                ));
            }

            let mut change = None;
            spool.scan(
                bounds.start,
                ScanLimit::new(1, usize::MAX)?,
                |entry| -> Result<(), MySqlCdcScanError> {
                    change = Some(entry.project(|encoded| {
                        decode_change(encoded)
                            .map_err(|error| StoreCodecError::new(error.to_string()))
                    })?);
                    Ok(())
                },
            )?;
            let change = change.expect("a non-empty spool scan returns its head entry");
            if change.records().schema().as_ref() != self.output_schema.as_ref() {
                return Err(MySqlCdcScanError::InvalidState(
                    "bootstrap spool Change has the wrong schema",
                )
                .into());
            }
            let next = bounds.start + 1;
            spool.truncate_before(next, NonZeroUsize::MIN)?;
            let finished = next == bounds.end;
            if finished {
                self.phase_cell.access(access)?.set(&STREAMING)?;
            }
            Ok((
                Action::Commit(Some(change)),
                AfterCommit::new(move || {
                    if finished {
                        self.phase = Some(Phase::Streaming);
                    }
                    Ok(())
                }),
            ))
        }))
    }

    fn stream(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.restart_connector {
            self.stop_connector("failed stream")?;
            self.restart_connector = false;
        }
        if self.connector.is_none() {
            let checkpoint = self.resume.as_ref().ok_or(MySqlCdcScanError::InvalidState(
                "streaming CDC scan has no checkpoint",
            ))?;
            self.connector = Some(self.config.start_streaming(&self.spec, checkpoint)?);
        }
        let connector = self
            .connector
            .as_mut()
            .expect("streaming connector was started above");
        self.restart_connector = true;
        let polled = connector.poll(Duration::ZERO).map_err(|error| {
            MySqlCdcScanError::new(format!("Debezium poll failed ({:?})", error.kind()))
        })?;
        let Some(delivery) = polled else {
            self.restart_connector = false;
            return Ok(Turn::Idle);
        };
        let change = convert_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.database,
            &self.spec.table,
            delivery.records(),
        )?;
        self.restart_connector = false;
        let encoded = delivery.checkpoint().as_bytes().to_vec();
        let resumed = delivery.checkpoint().clone();
        let checkpoint = &self.checkpoint;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            checkpoint.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(change),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        PostCommitError::new(MySqlCdcScanError::new(format!(
                            "Debezium ACK failed ({:?})",
                            error.kind()
                        )))
                    })?;
                    *resume = Some(resumed);
                    Ok(())
                }),
            ))
        }))
    }

    fn stop_connector(&mut self, stage: &str) -> Result<(), MySqlCdcScanError> {
        let Some(connector) = self.connector.as_mut() else {
            return Ok(());
        };
        connector.stop(CONNECTOR_STOP_TIMEOUT).map_err(|error| {
            MySqlCdcScanError::new(format!("Debezium {stage} stop failed ({:?})", error.kind()))
        })?;
        self.connector = None;
        Ok(())
    }
}

impl Operation for MySqlCdcScanOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err(MySqlCdcScanError::new("MySQL CDC scan does not accept input").into());
        }
        match self.phase {
            None => Ok(self.restore()),
            Some(Phase::Fresh) => Ok(self.begin_capture()),
            Some(Phase::Capturing) if self.snapshot_failed => {
                self.stop_connector("failed snapshot")?;
                Ok(self.begin_reset())
            }
            Some(Phase::Capturing) => self.capture(),
            Some(Phase::Publishing) => self.publish(),
            Some(Phase::Streaming) => self.stream(),
            Some(Phase::Resetting) => Ok(self.reset()),
        }
    }
}

fn require_bootstrap_capacity(
    retained: u64,
    encoded_bytes: usize,
    capacity: NonZeroU64,
) -> Result<(), MySqlCdcScanError> {
    let value_bytes = u64::try_from(encoded_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(LOG_OFFSET_BYTES))
        .ok_or_else(|| MySqlCdcScanError::new("bootstrap spool byte count overflowed"))?;
    if retained
        .checked_add(value_bytes)
        .is_none_or(|bytes| bytes > capacity.get())
    {
        return Err(MySqlCdcScanError::new(format!(
            "bootstrap spool capacity of {} bytes is exhausted; rebuild with a larger bootstrap_spool_bytes",
            capacity.get()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use arrow_array::{Int64Array, RecordBatch};
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use dogpaddle_change::{Change, encode_change};
    use dogpaddle_store::{AppendLog, Cell, Store};

    use super::*;
    use crate::operation::scan::{MySqlColumn, MySqlType};

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
            54_001,
        )
        .unwrap()
    }

    struct Fixture {
        operation: MySqlCdcScanOperation,
        phase: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: AppendLog<Vec<u8>>,
        transactions: dogpaddle_store::Transactions,
        _root: tempfile::TempDir,
    }

    impl Fixture {
        fn create() -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut store = Store::create(root.path().join("store")).unwrap();
            let phase = store.create_data::<Cell<u32>>("phase").unwrap();
            let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
            let spool = store.create_data::<AppendLog<Vec<u8>>>("spool").unwrap();
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
            let transaction = self.transactions.begin().unwrap();
            let (action, completion) = prepared.apply(transaction.access()).unwrap();
            transaction.commit().unwrap();
            completion.run().unwrap();
            action
        }

        fn durable(&mut self) -> (Option<u32>, Option<Vec<u8>>, std::ops::Range<u64>) {
            let transaction = self.transactions.begin().unwrap();
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
                    .bounds()
                    .unwrap(),
            );
            transaction.commit().unwrap();
            result
        }
    }

    #[test]
    fn fresh_is_durably_pinned_before_snapshot_start() {
        let mut fixture = Fixture::create();
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.operation.phase, Some(Phase::Fresh));
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (Some(CAPTURING), None, 0..0));
    }

    #[test]
    fn reopening_a_partial_capture_enters_incremental_reset() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
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
        spool.append(&encoded).unwrap();
        spool.append(&encoded).unwrap();
        transaction.commit().unwrap();

        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable().0, Some(RESETTING));
        fixture.commit();
        assert_eq!(fixture.durable().2, 1..2);
        fixture.commit();
        assert_eq!(fixture.durable(), (None, None, 2..2));
        assert_eq!(fixture.operation.phase, Some(Phase::Fresh));
    }

    #[test]
    fn partial_capture_without_its_atomic_checkpoint_is_rejected() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&CAPTURING)
            .unwrap();
        fixture
            .spool
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();

        let Turn::Ready(prepared) = fixture.operation.turn(None).unwrap() else {
            panic!("expected restore");
        };
        let transaction = fixture.transactions.begin().unwrap();
        assert!(prepared.apply(transaction.access()).is_err());
        drop(transaction);
        assert_eq!(fixture.durable(), (Some(CAPTURING), None, 0..1));
    }

    #[test]
    fn publishing_pop_and_streaming_transition_rollback_together() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 9)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
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
        fixture
            .spool
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();
        fixture.commit();

        let Turn::Ready(prepared) = fixture.operation.turn(None).unwrap() else {
            panic!("expected publish");
        };
        let transaction = fixture.transactions.begin().unwrap();
        let (action, completion) = prepared.apply(transaction.access()).unwrap();
        assert!(matches!(action, Action::Commit(Some(_))));
        drop(transaction);
        drop(completion);
        assert_eq!(fixture.durable().2, 0..1);
        assert_eq!(fixture.durable().0, Some(PUBLISHING));

        assert!(matches!(fixture.commit(), Action::Commit(Some(_))));
        assert_eq!(fixture.durable().2, 1..1);
        assert_eq!(fixture.durable().0, Some(STREAMING));
    }

    #[test]
    fn bootstrap_spool_capacity_is_a_hard_limit_even_when_empty() {
        let mut fixture = Fixture::create();
        let encoded = [0; 9];
        let transaction = fixture.transactions.begin().unwrap();
        let error = require_bootstrap_capacity(
            fixture
                .spool
                .access(transaction.access())
                .unwrap()
                .retained_bytes()
                .unwrap(),
            encoded.len(),
            NonZeroU64::new(u64::try_from(encoded.len()).unwrap() + 7).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("capacity"));
        drop(transaction);
        assert_eq!(fixture.durable().2, 0..0);

        let transaction = fixture.transactions.begin().unwrap();
        let mut spool = fixture.spool.access(transaction.access()).unwrap();
        require_bootstrap_capacity(
            spool.retained_bytes().unwrap(),
            encoded.len(),
            NonZeroU64::new(u64::try_from(encoded.len()).unwrap() + 8).unwrap(),
        )
        .unwrap();
        spool.append(&encoded.to_vec()).unwrap();
        transaction.commit().unwrap();
        assert_eq!(fixture.durable().2, 0..1);
    }
}
