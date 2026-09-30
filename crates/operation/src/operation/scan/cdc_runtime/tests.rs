use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dogpaddle_store::Store;

use super::*;

#[derive(Clone, Default)]
struct SnapshotOwner {
    fail_cleanup: Arc<AtomicBool>,
    cleanup_calls: Arc<AtomicUsize>,
}

impl Source for SnapshotOwner {
    type Progress = ();
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;

    fn source_fields(&self) -> usize {
        0
    }
    fn start_snapshot(&self) -> Result<Connector, OperationError> {
        panic!("reset must not start a connector")
    }

    fn start_streaming(&self, _: &Checkpoint) -> Result<Connector, OperationError> {
        panic!("reset must not start streaming")
    }

    fn cleanup_snapshot(&self) -> Result<(), OperationError> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_cleanup.load(Ordering::SeqCst) {
            Err(std::io::Error::other("snapshot cleanup failed").into())
        } else {
            Ok(())
        }
    }

    fn capture(&self, _: SchemaRef, _: &[Record], (): ()) -> Result<Captured<()>, OperationError> {
        panic!("reset must not convert records")
    }

    fn stream(&self, _: SchemaRef, _: &[Record]) -> Result<Option<Change>, OperationError> {
        panic!("reset must not convert records")
    }

    fn restore_checkpoint(
        &self,
        phase: Phase,
        _: Option<Vec<u8>>,
        _: bool,
    ) -> Result<Option<Checkpoint>, OperationError> {
        let _ = phase;
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

#[test]
fn failed_cleanup_and_rolled_back_reset_preserve_capture_and_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("state")).unwrap();
    let phase = store.create_data::<Cell<u32>>("phase").unwrap();
    let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
    let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
    let published = store.create_data::<Queue<Vec<u8>>>("published").unwrap();
    let source = SnapshotOwner::default();
    let mut runtime = CdcRuntime::new(
        source.clone(),
        Arc::new(arrow_schema::Schema::empty()),
        phase.clone(),
        checkpoint.clone(),
        spool.clone(),
        published,
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    {
        let txn = writes.begin();
        phase.access(txn.access()).unwrap().set(&CAPTURING).unwrap();
        checkpoint
            .access(txn.access())
            .unwrap()
            .set(&vec![9])
            .unwrap();
        spool
            .access(txn.access())
            .unwrap()
            .try_push(&vec![8], NonZeroU64::new(4096).unwrap())
            .unwrap();
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    source.fail_cleanup.store(true, Ordering::SeqCst);
    assert!(runtime.poll().is_err());
    assert_eq!(
        phase.read(reads.begin().access()).unwrap().get().unwrap(),
        Some(CAPTURING)
    );
    source.fail_cleanup.store(false, Ordering::SeqCst);
    let mut action = runtime.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
    }
    assert_eq!(
        spool
            .read(reads.begin().access())
            .unwrap()
            .queued_bytes()
            .unwrap(),
        9
    );
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
        txn.commit().unwrap();
    }
    runtime.ack(action).unwrap();
    assert_eq!(
        phase.read(reads.begin().access()).unwrap().get().unwrap(),
        None
    );
    assert_eq!(
        checkpoint
            .read(reads.begin().access())
            .unwrap()
            .get()
            .unwrap(),
        None
    );
    assert!(
        spool
            .read(reads.begin().access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
}
#[test]
fn publication_and_sealed_phase_move_atomically_to_source_owned_queue() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("state")).unwrap();
    let phase = store.create_data::<Cell<u32>>("phase").unwrap();
    let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
    let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
    let published = store.create_data::<Queue<Vec<u8>>>("published").unwrap();
    let schema = Arc::new(arrow_schema::Schema::empty());
    let codec = SchemaBoundChangeCodec::try_new(Arc::clone(&schema)).unwrap();
    let batch = arrow_array::RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let change = Change::try_new(batch, arrow_array::Int64Array::from(vec![1])).unwrap();
    let encoded = codec.encode(&change).unwrap();
    let mut runtime = CdcRuntime::new(
        SnapshotOwner::default(),
        schema,
        phase.clone(),
        checkpoint.clone(),
        spool.clone(),
        published.clone(),
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    {
        let txn = writes.begin();
        phase
            .access(txn.access())
            .unwrap()
            .set(&PUBLISHING)
            .unwrap();
        checkpoint
            .access(txn.access())
            .unwrap()
            .set(&vec![9])
            .unwrap();
        spool
            .access(txn.access())
            .unwrap()
            .try_push(&encoded, NonZeroU64::new(4096).unwrap())
            .unwrap();
        txn.commit().unwrap();
    }
    runtime.restore(reads.begin().access()).unwrap();
    let mut action = runtime.poll().unwrap().unwrap();
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
    }
    assert_eq!(
        phase.read(reads.begin().access()).unwrap().get().unwrap(),
        Some(PUBLISHING)
    );
    assert!(
        published
            .read(reads.begin().access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
    {
        let txn = writes.begin();
        assert!(runtime.record(txn.access(), &mut action).unwrap());
        txn.commit().unwrap();
    }
    runtime.ack(action).unwrap();
    assert_eq!(
        phase.read(reads.begin().access()).unwrap().get().unwrap(),
        Some(STREAMING)
    );
    {
        let txn = writes.begin();
        assert_eq!(
            runtime
                .codec
                .decode_owned(runtime.published(reads.begin().access()).unwrap().unwrap())
                .unwrap()
                .diffs()
                .values(),
            &[1]
        );
        runtime.consume_published(txn.access()).unwrap();
    }
    {
        let txn = writes.begin();
        assert!(runtime.published(reads.begin().access()).unwrap().is_some());
        runtime.consume_published(txn.access()).unwrap();
        txn.commit().unwrap();
    }
}
