use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dogpaddle_store::{Store, StoreSetup};

use super::*;

// Connector-bound v1 checkpoints with zero and one offset entry, respectively.
const SNAPSHOT_CHECKPOINT: &[u8] = b"DPDBCP01\x00\x01\x00\x00\x00\x06source\x00\x00\x00\x0etest.connector\x00\x00\x00\x00\xd6\x10\x9f\xec";
const STREAM_CHECKPOINT: &[u8] = b"DPDBCP01\x00\x01\x00\x00\x00\x06source\x00\x00\x00\x0etest.connector\x00\x00\x00\x01\x00\x00\x00\x01x\x00\x00\x00\x01y\xdf\x9a\xac\x33";

#[derive(Clone, Default)]
struct SnapshotOwner {
    fail_cleanup: Arc<AtomicBool>,
    cleanup_calls: Arc<AtomicUsize>,
}

impl Source for SnapshotOwner {
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;

    const CAPTURE_ACCEPTS_STREAMING: bool = true;
    const STREAMING_TOMBSTONES: bool = false;
    fn columns(&self) -> &Fields {
        panic!("reset must not inspect source fields")
    }
    fn output_projection(&self) -> &[u32] {
        panic!("reset must not inspect projection")
    }
    fn engine_name(&self) -> &'static str {
        "source"
    }
    fn table_topic(&self) -> String {
        "source.test".into()
    }
    fn snapshot_marker(&self, _: &Row, _: bool) -> Result<SnapshotMarker, ConvertError> {
        panic!("reset must not convert records")
    }
    fn conversion_error(error: ConvertError) -> OperationError {
        error.into()
    }
    fn start_snapshot(&self) -> Result<Connector, OperationError> {
        panic!("reset must not start a connector")
    }

    fn start_streaming(&self, _: &Checkpoint) -> Result<Connector, OperationError> {
        Err(std::io::Error::other("stream start failed").into())
    }

    fn cleanup_snapshot(&self) -> Result<(), OperationError> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_cleanup.load(Ordering::SeqCst) {
            Err(std::io::Error::other("snapshot cleanup failed").into())
        } else {
            Ok(())
        }
    }

    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        _: bool,
    ) -> Result<Option<Checkpoint>, OperationError> {
        if matches!(phase, Phase::Sealed | Phase::Streaming) {
            return Ok(Some(Checkpoint::from_bytes(bytes.ok_or_else(|| {
                Self::invalid_state("sealed source has no checkpoint")
            })?)?));
        }
        Ok(None)
    }

    fn invalid_state(message: &'static str) -> OperationError {
        std::io::Error::other(message).into()
    }

    fn runtime_error(message: String) -> OperationError {
        std::io::Error::other(message).into()
    }

    fn spool_full() -> OperationError {
        std::io::Error::other("spool full").into()
    }

    fn codec_error(error: dogpaddle_change::CodecError) -> OperationError {
        error.into()
    }
}

fn bind(store: &mut StoreSetup, capacity: u64) -> CdcRuntime<SnapshotOwner> {
    CdcRuntime::new(
        SnapshotOwner::default(),
        Arc::new(arrow_schema::Schema::empty()),
        store.create_data::<Cell<u32>>("phase").unwrap(),
        store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap(),
        store.create_data::<Queue<Vec<u8>>>("input").unwrap(),
        NonZeroU64::new(capacity).unwrap(),
    )
    .unwrap()
}

fn assert_state(
    runtime: &CdcRuntime<SnapshotOwner>,
    access: ReadTransactionAccess<'_>,
    phase: Option<u32>,
    checkpoint: Option<&[u8]>,
    queued_bytes: u64,
) {
    assert_eq!(
        runtime.phase_cell.read(access).unwrap().get().unwrap(),
        phase
    );
    assert_eq!(
        runtime
            .checkpoint
            .read(access)
            .unwrap()
            .get()
            .unwrap()
            .as_deref(),
        checkpoint
    );
    assert_eq!(
        runtime.input.read(access).unwrap().queued_bytes().unwrap(),
        queued_bytes
    );
}

#[test]
fn failed_cleanup_and_rolled_back_reset_preserve_capture_and_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let mut runtime = bind(&mut store, 4096);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        runtime
            .phase_cell
            .access(txn.access())
            .unwrap()
            .set(&CAPTURING)
            .unwrap();
        runtime
            .record_capture(
                txn.access(),
                Some(&vec![8]),
                SNAPSHOT_CHECKPOINT,
                false,
                false,
            )
            .unwrap();
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    runtime.source.fail_cleanup.store(true, Ordering::SeqCst);
    assert!(runtime.poll().is_err());
    assert_eq!(
        runtime
            .phase_cell
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        Some(CAPTURING)
    );
    runtime.source.fail_cleanup.store(false, Ordering::SeqCst);
    let mut action = runtime.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
    }
    assert_eq!(
        runtime
            .input
            .read(reads.begin().access())
            .unwrap()
            .queued_bytes()
            .unwrap(),
        9
    );
    assert_eq!(
        runtime
            .checkpoint
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap(),
        SNAPSHOT_CHECKPOINT
    );
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
        txn.commit().unwrap();
    }
    runtime.ack(action).unwrap();
    assert_eq!(
        runtime
            .phase_cell
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        None
    );
    assert_eq!(
        runtime
            .checkpoint
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        None
    );
    assert!(
        runtime
            .input
            .read(reads.begin().access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
}

#[test]
fn unsealed_input_is_hidden_until_the_seal_transaction_commits() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let runtime = bind(&mut store, 4096);
    let encoded = vec![8];
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(
                    txn.access(),
                    Some(&encoded),
                    SNAPSHOT_CHECKPOINT,
                    false,
                    false
                )
                .unwrap()
        );
        txn.commit().unwrap();
    }
    for phase in [None, Some(RESETTING), Some(CAPTURING)] {
        {
            let txn = writes.begin();
            match phase {
                Some(phase) => runtime
                    .phase_cell
                    .access(txn.access())
                    .unwrap()
                    .set(&phase)
                    .unwrap(),
                None => {
                    runtime
                        .phase_cell
                        .access(txn.access())
                        .unwrap()
                        .clear()
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        assert!(runtime.published(reads.begin().access()).unwrap().is_none());
        let txn = writes.begin();
        assert!(runtime.consume_published(txn.access()).is_err());
    }
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(txn.access(), None, SNAPSHOT_CHECKPOINT, true, false)
                .unwrap()
        );
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(CAPTURING),
        Some(SNAPSHOT_CHECKPOINT),
        9,
    );
    assert!(runtime.published(reads.begin().access()).unwrap().is_none());
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(txn.access(), None, SNAPSHOT_CHECKPOINT, true, false)
                .unwrap()
        );
        txn.commit().unwrap();
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(SEALED),
        Some(SNAPSHOT_CHECKPOINT),
        9,
    );
    assert_eq!(
        runtime.published(reads.begin().access()).unwrap(),
        Some(encoded)
    );
}

#[test]
fn sealed_input_front_survives_rolled_back_consumption_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut store = StoreSetup::new();
    let runtime = bind(&mut store, 4096);
    let batch = arrow_array::RecordBatch::try_new_with_options(
        runtime.codec.schema(),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let change = Change::try_new(batch, arrow_array::Int64Array::from(vec![1])).unwrap();
    let encoded = runtime.codec.encode(&change).unwrap();
    let (mut writes, reads) = store.commit(&path, |_| Ok(())).unwrap().split();
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(
                    txn.access(),
                    Some(&encoded),
                    SNAPSHOT_CHECKPOINT,
                    true,
                    false
                )
                .unwrap()
        );
        txn.commit().unwrap();
    }
    {
        let txn = writes.begin();
        runtime.consume_published(txn.access()).unwrap();
    }
    drop((runtime, writes, reads));
    let store = Store::open(&path).unwrap();
    let mut runtime = CdcRuntime::new(
        SnapshotOwner::default(),
        Arc::new(arrow_schema::Schema::empty()),
        store.open_data::<Cell<u32>>("phase").unwrap(),
        store.open_data::<Cell<Vec<u8>>>("checkpoint").unwrap(),
        store.open_data::<Queue<Vec<u8>>>("input").unwrap(),
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    // Flow validates an active root before calling Source.restore.
    assert_eq!(
        runtime.published(reads.begin().access()).unwrap(),
        Some(encoded)
    );
    runtime.restore(reads.begin().access()).unwrap();
    assert_eq!(runtime.next_step, NextStep::Stream);
    assert_eq!(
        runtime
            .codec
            .decode_owned(runtime.published(reads.begin().access()).unwrap().unwrap())
            .unwrap()
            .diffs()
            .values(),
        &[1]
    );
    {
        let txn = writes.begin();
        runtime.consume_published(txn.access()).unwrap();
        txn.commit().unwrap();
    }
    assert!(runtime.published(reads.begin().access()).unwrap().is_none());
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(SEALED),
        Some(SNAPSHOT_CHECKPOINT),
        0,
    );
}

fn fill_to_streaming_quota(runtime: &CdcRuntime<SnapshotOwner>, access: TransactionAccess<'_>) {
    let item = vec![0; 2 * 1024 * 1024 - 8];
    let mut input = runtime.input.access(access).unwrap();
    for _ in 0..32 {
        assert!(input.try_push(&item, runtime.capacity).unwrap());
    }
    runtime
        .checkpoint
        .access(access)
        .unwrap()
        .set(&SNAPSHOT_CHECKPOINT.to_vec())
        .unwrap();
    runtime
        .phase_cell
        .access(access)
        .unwrap()
        .set(&SEALED)
        .unwrap();
}

#[test]
fn oversized_sealed_backlog_blocks_data_and_heartbeats_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let mut runtime = bind(&mut store, STREAMING_BYTES + 4096);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        assert!(
            runtime
                .input
                .access(txn.access())
                .unwrap()
                .try_push(&vec![7], runtime.capacity)
                .unwrap()
        );
        fill_to_streaming_quota(&runtime, txn.access());
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    assert_eq!(runtime.next_step, NextStep::Stream);
    let appended = vec![8; 100];
    for encoded in [None, Some(&appended)] {
        let txn = writes.begin();
        assert!(
            !runtime
                .record_capture(txn.access(), encoded, STREAM_CHECKPOINT, false, true)
                .unwrap()
        );
        txn.commit().unwrap();
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(SEALED),
        Some(SNAPSHOT_CHECKPOINT),
        STREAMING_BYTES + 9,
    );
    assert_eq!(
        runtime.published(reads.begin().access()).unwrap(),
        Some(vec![7])
    );
    {
        let txn = writes.begin();
        runtime.consume_published(txn.access()).unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(txn.access(), None, STREAM_CHECKPOINT, false, true)
                .unwrap()
        );
        txn.commit().unwrap();
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(STREAMING),
        Some(STREAM_CHECKPOINT),
        STREAMING_BYTES,
    );
}

#[test]
fn streaming_quota_boundary_and_first_phase_transition_roll_back_together() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let runtime = bind(&mut store, STREAMING_BYTES);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        fill_to_streaming_quota(&runtime, txn.access());
        txn.commit().unwrap();
    }
    let appended = vec![8; 100];
    {
        let txn = writes.begin();
        assert!(
            !runtime
                .record_capture(
                    txn.access(),
                    Some(&appended),
                    STREAM_CHECKPOINT,
                    false,
                    true
                )
                .unwrap()
        );
        assert!(
            runtime
                .record_capture(txn.access(), None, STREAM_CHECKPOINT, false, true)
                .unwrap()
        );
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(SEALED),
        Some(SNAPSHOT_CHECKPOINT),
        STREAMING_BYTES,
    );
    {
        let txn = writes.begin();
        runtime.consume_published(txn.access()).unwrap();
        assert!(
            runtime
                .record_capture(
                    txn.access(),
                    Some(&appended),
                    STREAM_CHECKPOINT,
                    false,
                    true
                )
                .unwrap()
        );
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(SEALED),
        Some(SNAPSHOT_CHECKPOINT),
        STREAMING_BYTES,
    );
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(txn.access(), None, STREAM_CHECKPOINT, false, true)
                .unwrap()
        );
        txn.commit().unwrap();
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(STREAMING),
        Some(STREAM_CHECKPOINT),
        STREAMING_BYTES,
    );
    {
        let txn = writes.begin();
        runtime.consume_published(txn.access()).unwrap();
        assert!(
            runtime
                .record_capture(
                    txn.access(),
                    Some(&appended),
                    STREAM_CHECKPOINT,
                    false,
                    true
                )
                .unwrap()
        );
        txn.commit().unwrap();
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(STREAMING),
        Some(STREAM_CHECKPOINT),
        STREAMING_BYTES - 2 * 1024 * 1024 + 108,
    );
}

#[test]
fn reset_discards_only_a_bounded_batch_and_clears_checkpoint_with_the_last_entry() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let mut runtime = bind(&mut store, 4096);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        let mut input = runtime.input.access(txn.access()).unwrap();
        for _ in 0..=RESET_BATCH_ENTRIES {
            assert!(input.try_push(&vec![8], runtime.capacity).unwrap());
        }
        runtime
            .checkpoint
            .access(txn.access())
            .unwrap()
            .set(&SNAPSHOT_CHECKPOINT.to_vec())
            .unwrap();
        runtime
            .phase_cell
            .access(txn.access())
            .unwrap()
            .set(&CAPTURING)
            .unwrap();
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    let mut action = runtime.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
        txn.commit().unwrap();
    }
    runtime.ack(action).unwrap();
    assert_eq!(runtime.next_step, NextStep::Reset);
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(RESETTING),
        Some(SNAPSHOT_CHECKPOINT),
        9,
    );
    assert!(runtime.published(reads.begin().access()).unwrap().is_none());
    let mut action = runtime.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
    }
    assert_state(
        &runtime,
        reads.begin().access(),
        Some(RESETTING),
        Some(SNAPSHOT_CHECKPOINT),
        9,
    );
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
        txn.commit().unwrap();
    }
    runtime.ack(action).unwrap();
    assert_eq!(runtime.next_step, NextStep::BeginCapture);
    assert_eq!(runtime.source.cleanup_calls.load(Ordering::SeqCst), 1);
    assert_state(&runtime, reads.begin().access(), None, None, 0);
}

#[test]
fn restore_uses_the_streaming_quota_instead_of_the_smaller_bootstrap_quota() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let mut runtime = bind(&mut store, 4096);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        assert!(
            runtime
                .input
                .access(txn.access())
                .unwrap()
                .try_push(&vec![8; 5000], NonZeroU64::new(STREAMING_BYTES).unwrap())
                .unwrap()
        );
        runtime
            .checkpoint
            .access(txn.access())
            .unwrap()
            .set(&STREAM_CHECKPOINT.to_vec())
            .unwrap();
        runtime
            .phase_cell
            .access(txn.access())
            .unwrap()
            .set(&SEALED)
            .unwrap();
        txn.commit().unwrap();
    }
    assert!(runtime.restore(reads.begin().access()).is_err());
    assert_eq!(runtime.next_step, NextStep::Restore);
    {
        let txn = writes.begin();
        runtime
            .phase_cell
            .access(txn.access())
            .unwrap()
            .set(&STREAMING)
            .unwrap();
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    assert_eq!(runtime.next_step, NextStep::Stream);
    assert_eq!(
        runtime
            .published(reads.begin().access())
            .unwrap()
            .unwrap()
            .len(),
        5000
    );
}

#[test]
fn early_stream_start_failure_preserves_sealed_input_and_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let mut store = StoreSetup::new();
    let mut runtime = bind(&mut store, 4096);
    let (mut writes, reads) = store
        .commit(root.path().join("state"), |_| Ok(()))
        .unwrap()
        .split();
    {
        let txn = writes.begin();
        assert!(
            runtime
                .record_capture(
                    txn.access(),
                    Some(&vec![8]),
                    SNAPSHOT_CHECKPOINT,
                    true,
                    false
                )
                .unwrap()
        );
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    assert_eq!(runtime.next_step, NextStep::Stream);
    assert_eq!(
        runtime.poll().err().unwrap().to_string(),
        "stream start failed"
    );
    assert_eq!(
        runtime
            .phase_cell
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        Some(SEALED)
    );
    assert_eq!(
        runtime
            .checkpoint
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap()
            .unwrap(),
        SNAPSHOT_CHECKPOINT
    );
    assert_eq!(
        runtime.published(reads.begin().access()).unwrap(),
        Some(vec![8])
    );
}
