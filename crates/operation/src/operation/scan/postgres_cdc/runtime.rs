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
    PostgresCdcScanConfig, PostgresCdcScanError, PostgresCdcScanSpec,
    connection::CONNECTOR_CLASS,
    convert::{CaptureProgress, convert_capture_records, convert_records},
};

const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const ONE_ENTRY: NonZeroUsize = NonZeroUsize::MIN;
const OFFSET_BYTES: u64 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Fresh,
    Capturing,
    Publishing,
    Streaming,
    Resetting,
}

impl Phase {
    fn decode(value: Option<u32>) -> Result<Self, PostgresCdcScanError> {
        match value {
            None => Ok(Self::Fresh),
            Some(1) => Ok(Self::Capturing),
            Some(2) => Ok(Self::Publishing),
            Some(3) => Ok(Self::Streaming),
            Some(4) => Ok(Self::Resetting),
            Some(_) => Err(PostgresCdcScanError::InvalidState(
                "CDC scan phase is invalid",
            )),
        }
    }

    const fn durable(self) -> Option<u32> {
        match self {
            Self::Fresh => None,
            Self::Capturing => Some(1),
            Self::Publishing => Some(2),
            Self::Streaming => Some(3),
            Self::Resetting => Some(4),
        }
    }
}

/// One materialized `PostgreSQL` CDC Scan with reconstructible connector resources.
///
/// The initial snapshot is durably sealed in a private append log before any
/// row becomes public. A capture interrupted before sealing is discarded and
/// restarted with a newly created logical slot.
pub struct PostgresCdcScanOperation {
    spec: PostgresCdcScanSpec,
    output_schema: SchemaRef,
    phase_cell: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    bootstrap_spool: AppendLog<Vec<u8>>,
    config: PostgresCdcScanConfig,
    bootstrap_spool_bytes: NonZeroU64,
    restored: bool,
    phase: Phase,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    capture_progress: CaptureProgress,
    reset_capture: bool,
    restart_streaming: bool,
}

impl PostgresCdcScanOperation {
    pub(super) fn new_bound(
        spec: PostgresCdcScanSpec,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        bootstrap_spool: AppendLog<Vec<u8>>,
        config: PostgresCdcScanConfig,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Self {
        Self {
            spec,
            output_schema,
            phase_cell,
            checkpoint,
            bootstrap_spool,
            config,
            bootstrap_spool_bytes,
            restored: false,
            phase: Phase::Fresh,
            resume: None,
            connector: None,
            capture_progress: CaptureProgress::default(),
            reset_capture: false,
            restart_streaming: false,
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let phase = Phase::decode(self.phase_cell.access(access)?.get()?)?;
            let checkpoint = self.checkpoint.access(access)?.get()?;
            let bounds = self.bootstrap_spool.access(access)?.bounds()?;
            match phase {
                Phase::Fresh if checkpoint.is_some() || !bounds.is_empty() => {
                    return Err(PostgresCdcScanError::InvalidState(
                        "fresh CDC scan retains bootstrap data",
                    )
                    .into());
                }
                Phase::Streaming if !bounds.is_empty() => {
                    return Err(PostgresCdcScanError::InvalidState(
                        "streaming CDC scan retains bootstrap output",
                    )
                    .into());
                }
                Phase::Capturing if !bounds.is_empty() && checkpoint.is_none() => {
                    return Err(PostgresCdcScanError::InvalidState(
                        "captured bootstrap output has no checkpoint",
                    )
                    .into());
                }
                _ => {}
            }
            let resume = match phase {
                Phase::Publishing | Phase::Streaming => Some(parse_checkpoint(
                    checkpoint.ok_or(PostgresCdcScanError::InvalidState(
                        "sealed CDC scan has no checkpoint",
                    ))?,
                    &self.spec,
                )?),
                Phase::Fresh | Phase::Capturing | Phase::Resetting => None,
            };
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = phase;
                    self.resume = resume;
                    self.reset_capture = phase == Phase::Capturing;
                    self.restored = true;
                    Ok(())
                }),
            ))
        })
    }

    fn begin_capture(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            self.phase_cell
                .access(access)?
                .set(&Phase::Capturing.durable().expect("capturing is durable"))?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Capturing;
                    self.capture_progress = CaptureProgress::default();
                    Ok(())
                }),
            ))
        })
    }

    fn prepare_capture_reset(&mut self) -> Result<Turn<'_>, OperationError> {
        if let Some(connector) = self.connector.as_mut() {
            connector.stop(STOP_TIMEOUT).map_err(|error| {
                PostgresCdcScanError::new(format!(
                    "Debezium bootstrap stop failed ({:?})",
                    error.kind()
                ))
            })?;
        }
        self.connector = None;
        self.config.drop_snapshot_slot(&self.spec)?;
        Ok(Turn::ready(move |access| {
            self.phase_cell
                .access(access)?
                .set(&Phase::Resetting.durable().expect("resetting is durable"))?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Resetting;
                    self.resume = None;
                    self.capture_progress = CaptureProgress::default();
                    self.reset_capture = false;
                    Ok(())
                }),
            ))
        }))
    }

    fn reset(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let mut spool = self.bootstrap_spool.access(access)?;
            let bounds = spool.bounds()?;
            if !bounds.is_empty() {
                let next = bounds.start + 1;
                spool.truncate_before(next, ONE_ENTRY)?;
                if next != bounds.end {
                    return Ok((Action::Commit(None), AfterCommit::none()));
                }
            }
            self.checkpoint.access(access)?.clear()?;
            self.phase_cell.access(access)?.clear()?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.phase = Phase::Fresh;
                    Ok(())
                }),
            ))
        })
    }

    fn capture(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.connector.is_none() {
            self.reset_capture = true;
            self.connector = Some(self.config.start_snapshot(&self.spec)?);
            self.reset_capture = false;
        }

        let current_progress = self.capture_progress;
        let connector = self
            .connector
            .as_mut()
            .expect("snapshot connector was started above");
        let polled = connector.poll(Duration::ZERO);
        let Some(delivery) = (match polled {
            Ok(delivery) => delivery,
            Err(error) => {
                self.reset_capture = true;
                return Err(PostgresCdcScanError::new(format!(
                    "Debezium bootstrap poll failed ({:?})",
                    error.kind()
                ))
                .into());
            }
        }) else {
            return Ok(Turn::Idle);
        };
        let converted = match convert_capture_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.schema,
            &self.spec.table,
            delivery.records(),
            current_progress,
        ) {
            Ok(converted) => converted,
            Err(error) => {
                drop(delivery);
                self.reset_capture = true;
                return Err(error.into());
            }
        };
        let encoded = converted
            .change
            .as_ref()
            .map(encode_change)
            .transpose()
            .map_err(PostgresCdcScanError::from)?;
        let sealed = converted.sealed;
        let next_progress = converted.next_progress;
        let checkpoint_bytes = delivery.checkpoint().as_bytes().to_vec();
        let resume_checkpoint = delivery.checkpoint().clone();
        let checkpoint = &self.checkpoint;
        let phase_cell = &self.phase_cell;
        let spool = &self.bootstrap_spool;
        let capacity = self.bootstrap_spool_bytes;
        let phase = &mut self.phase;
        let progress = &mut self.capture_progress;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            if let Some(encoded) = encoded {
                let mut spool = spool.access(access)?;
                require_spool_capacity(spool.retained_bytes()?, encoded.len(), capacity)?;
                spool.append(&encoded)?;
            }
            checkpoint.access(access)?.set(&checkpoint_bytes)?;
            if sealed {
                phase_cell
                    .access(access)?
                    .set(&Phase::Publishing.durable().expect("publishing is durable"))?;
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        PostCommitError::new(PostgresCdcScanError::new(format!(
                            "Debezium bootstrap ACK failed ({:?})",
                            error.kind()
                        )))
                    })?;
                    if sealed {
                        *phase = Phase::Publishing;
                        *resume = Some(resume_checkpoint);
                    } else {
                        *progress = next_progress;
                    }
                    Ok(())
                }),
            ))
        }))
    }

    fn publish(&mut self) -> Result<Turn<'_>, OperationError> {
        if let Some(connector) = self.connector.as_mut() {
            connector.stop(STOP_TIMEOUT).map_err(|error| {
                PostgresCdcScanError::new(format!(
                    "Debezium bootstrap stop failed ({:?})",
                    error.kind()
                ))
            })?;
        }
        self.connector = None;
        Ok(Turn::ready(move |access| {
            let mut spool = self.bootstrap_spool.access(access)?;
            let bounds = spool.bounds()?;
            if bounds.is_empty() {
                self.phase_cell
                    .access(access)?
                    .set(&Phase::Streaming.durable().expect("streaming is durable"))?;
                return Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        self.connector = None;
                        self.phase = Phase::Streaming;
                        Ok(())
                    }),
                ));
            }

            let mut change = None;
            spool.scan(
                bounds.start,
                ScanLimit::new(1, usize::MAX)?,
                |entry| -> Result<(), PostgresCdcScanError> {
                    change = Some(entry.project(|encoded| {
                        decode_change(encoded).map_err(|_| {
                            StoreCodecError::new("invalid PostgreSQL bootstrap spool Change")
                        })
                    })?);
                    Ok(())
                },
            )?;
            let change = change.expect("one retained append-log entry was scanned");
            if change.records().schema().as_ref() != self.output_schema.as_ref() {
                return Err(PostgresCdcScanError::InvalidState(
                    "bootstrap spool Change has the wrong schema",
                )
                .into());
            }
            let next = bounds
                .start
                .checked_add(1)
                .ok_or(PostgresCdcScanError::InvalidState(
                    "bootstrap spool offset is exhausted",
                ))?;
            spool.truncate_before(next, ONE_ENTRY)?;
            let finished = next == bounds.end;
            if finished {
                self.phase_cell
                    .access(access)?
                    .set(&Phase::Streaming.durable().expect("streaming is durable"))?;
            }
            Ok((
                Action::Commit(Some(change)),
                if finished {
                    AfterCommit::new(move || {
                        self.connector = None;
                        self.phase = Phase::Streaming;
                        Ok(())
                    })
                } else {
                    AfterCommit::none()
                },
            ))
        }))
    }

    fn stream(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.restart_streaming {
            if let Some(connector) = self.connector.as_mut() {
                connector.stop(STOP_TIMEOUT).map_err(|error| {
                    PostgresCdcScanError::new(format!(
                        "Debezium streaming stop failed ({:?})",
                        error.kind()
                    ))
                })?;
            }
            self.connector = None;
            self.restart_streaming = false;
        }
        if self.connector.is_none() {
            let checkpoint = self
                .resume
                .as_ref()
                .expect("publishing and restore retain the sealed checkpoint");
            self.connector = Some(self.config.start_streaming(&self.spec, checkpoint)?);
        }

        let connector = self
            .connector
            .as_mut()
            .expect("streaming connector was started above");
        let polled = connector.poll(Duration::ZERO);
        let Some(delivery) = (match polled {
            Ok(delivery) => delivery,
            Err(error) => {
                self.restart_streaming = true;
                return Err(PostgresCdcScanError::new(format!(
                    "Debezium streaming poll failed ({:?})",
                    error.kind()
                ))
                .into());
            }
        }) else {
            return Ok(Turn::Idle);
        };
        let change = match convert_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.schema,
            &self.spec.table,
            delivery.records(),
        ) {
            Ok(change) => change,
            Err(error) => {
                drop(delivery);
                self.restart_streaming = true;
                return Err(error.into());
            }
        };
        let checkpoint_bytes = delivery.checkpoint().as_bytes().to_vec();
        let resume_checkpoint = delivery.checkpoint().clone();
        let checkpoint = &self.checkpoint;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            checkpoint.access(access)?.set(&checkpoint_bytes)?;
            Ok((
                Action::Commit(change),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        PostCommitError::new(PostgresCdcScanError::new(format!(
                            "Debezium streaming ACK failed ({:?})",
                            error.kind()
                        )))
                    })?;
                    *resume = Some(resume_checkpoint);
                    Ok(())
                }),
            ))
        }))
    }
}

impl Operation for PostgresCdcScanOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err(
                PostgresCdcScanError::new("PostgreSQL CDC scan does not accept input").into(),
            );
        }
        if !self.restored {
            return Ok(self.restore());
        }
        match self.phase {
            Phase::Fresh => Ok(self.begin_capture()),
            Phase::Capturing if self.reset_capture => self.prepare_capture_reset(),
            Phase::Capturing => self.capture(),
            Phase::Publishing => self.publish(),
            Phase::Streaming => self.stream(),
            Phase::Resetting => Ok(self.reset()),
        }
    }
}

fn parse_checkpoint(
    bytes: Vec<u8>,
    spec: &PostgresCdcScanSpec,
) -> Result<Checkpoint, PostgresCdcScanError> {
    let checkpoint = Checkpoint::from_bytes(bytes)
        .map_err(|_| PostgresCdcScanError::InvalidState("CDC scan checkpoint is invalid"))?;
    if !checkpoint.matches(&spec.engine_name, CONNECTOR_CLASS) {
        return Err(PostgresCdcScanError::InvalidState(
            "CDC scan checkpoint belongs to another PostgreSQL connector",
        ));
    }
    Ok(checkpoint)
}

fn require_spool_capacity(
    retained_bytes: u64,
    encoded_len: usize,
    capacity: NonZeroU64,
) -> Result<(), PostgresCdcScanError> {
    let fits = u64::try_from(encoded_len)
        .ok()
        .and_then(|bytes| retained_bytes.checked_add(OFFSET_BYTES)?.checked_add(bytes))
        .is_some_and(|bytes| bytes <= capacity.get());
    if !fits {
        return Err(PostgresCdcScanError::BootstrapSpoolFull);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch};
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use dogpaddle_change::Change;
    use dogpaddle_store::{Store, Transactions};

    use super::*;
    use crate::operation::scan::{PostgresColumn, PostgresType};

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

    struct Fixture {
        _root: tempfile::TempDir,
        operation: PostgresCdcScanOperation,
        phase: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: AppendLog<Vec<u8>>,
        transactions: Transactions,
    }

    impl Fixture {
        fn create() -> Self {
            let root = tempfile::tempdir().unwrap();
            let mut store = Store::create(root.path().join("store")).unwrap();
            let phase = store.create_data::<Cell<u32>>("phase").unwrap();
            let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
            let spool = store.create_data::<AppendLog<Vec<u8>>>("spool").unwrap();
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
            let transaction = self.transactions.begin().unwrap();
            let (action, completion) = prepared.apply(transaction.access()).unwrap();
            transaction.commit().unwrap();
            completion.run().unwrap();
            action
        }

        fn durable(&mut self) -> (Option<u32>, Option<Vec<u8>>, std::ops::Range<u64>) {
            let transaction = self.transactions.begin().unwrap();
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
                    .bounds()
                    .unwrap(),
            );
            transaction.commit().unwrap();
            durable
        }
    }

    #[test]
    fn bootstrap_spool_capacity_includes_the_offset_key() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let spool = store.create_data::<AppendLog<Vec<u8>>>("spool").unwrap();
        let mut transactions = store.into_transactions();
        let encoded = vec![0; 100];

        let transaction = transactions.begin().unwrap();
        assert!(require_spool_capacity(0, encoded.len(), NonZeroU64::new(107).unwrap()).is_err());
        drop(transaction);

        let transaction = transactions.begin().unwrap();
        let mut access = spool.access(transaction.access()).unwrap();
        require_spool_capacity(
            access.retained_bytes().unwrap(),
            encoded.len(),
            NonZeroU64::new(108).unwrap(),
        )
        .unwrap();
        access.append(&encoded).unwrap();
        transaction.commit().unwrap();

        let transaction = transactions.begin().unwrap();
        assert_eq!(
            spool
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
            0..1
        );
    }

    #[test]
    fn fresh_is_durably_capturing_before_external_start() {
        let mut fixture = Fixture::create();
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.operation.phase, Phase::Fresh);
        assert!(matches!(fixture.commit(), Action::Commit(None)));
        assert_eq!(fixture.durable(), (Some(1), None, 0..0));
    }

    #[test]
    fn reopening_capture_requires_slot_cleanup_before_durable_reset() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
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
        fixture
            .spool
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();

        fixture.commit();
        assert_eq!(fixture.operation.phase, Phase::Capturing);
        assert!(fixture.operation.reset_capture);
        assert_eq!(fixture.durable(), (Some(1), Some(checkpoint()), 0..1));
    }

    #[test]
    fn captured_output_without_its_checkpoint_is_rejected() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 7)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
        fixture
            .phase
            .access(transaction.access())
            .unwrap()
            .set(&1)
            .unwrap();
        fixture
            .spool
            .access(transaction.access())
            .unwrap()
            .append(&encoded)
            .unwrap();
        transaction.commit().unwrap();

        let Turn::Ready(prepared) = fixture.operation.turn(None).unwrap() else {
            panic!("expected restore turn");
        };
        let transaction = fixture.transactions.begin().unwrap();
        assert!(prepared.apply(transaction.access()).is_err());
        drop(transaction);
        assert_eq!(fixture.durable(), (Some(1), None, 0..1));
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
            .set(&2)
            .unwrap();
        fixture
            .checkpoint
            .access(transaction.access())
            .unwrap()
            .set(&checkpoint())
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
            panic!("expected publish turn");
        };
        let transaction = fixture.transactions.begin().unwrap();
        let (action, completion) = prepared.apply(transaction.access()).unwrap();
        assert!(matches!(action, Action::Commit(Some(_))));
        drop(transaction);
        drop(completion);
        assert_eq!(fixture.durable(), (Some(2), Some(checkpoint()), 0..1));

        assert!(matches!(fixture.commit(), Action::Commit(Some(_))));
        assert_eq!(fixture.durable(), (Some(3), Some(checkpoint()), 1..1));
    }

    #[test]
    fn reset_truncates_at_most_one_entry_per_turn_before_returning_fresh() {
        let mut fixture = Fixture::create();
        let encoded =
            encode_change(&change(Arc::clone(&fixture.operation.output_schema), 1)).unwrap();
        let transaction = fixture.transactions.begin().unwrap();
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
        spool.append(&encoded).unwrap();
        spool.append(&encoded).unwrap();
        transaction.commit().unwrap();

        fixture.commit();
        fixture.commit();
        assert_eq!(fixture.durable().2, 1..2);
        fixture.commit();
        assert_eq!(fixture.durable(), (None, None, 2..2));
        assert_eq!(fixture.operation.phase, Phase::Fresh);
    }
}
