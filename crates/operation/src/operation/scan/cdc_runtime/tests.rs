use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dogpaddle_store::{Store, Transactions};

use super::*;

#[derive(Clone, Default)]
struct SnapshotOwner {
    fail_cleanup: Arc<AtomicBool>,
    cleanup_calls: Arc<AtomicUsize>,
}

impl Source for SnapshotOwner {
    type Progress = ();
    const RESET_REQUIRES_SOURCE_CLEANUP: bool = true;

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
        assert_eq!(phase, Phase::Capturing);
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

fn commit(runtime: &mut CdcRuntime<SnapshotOwner>, transactions: &mut Transactions) {
    let Turn::Ready(prepared) = runtime.turn(None).unwrap() else {
        panic!("expected recovery work");
    };
    let transaction = transactions.begin();
    let (action, completion) = prepared.apply(transaction.access()).unwrap();
    assert!(matches!(action, Action::Commit(None)));
    transaction.commit().unwrap();
    completion.run().unwrap();
}

#[test]
fn failed_source_cleanup_and_rolled_back_reset_preserve_the_captured_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let phase = store.create_data::<Cell<u32>>("phase").unwrap();
    let checkpoint = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
    let spool = store.create_data::<Queue<Vec<u8>>>("spool").unwrap();
    let mut transactions = store.into_transactions();
    let source = SnapshotOwner::default();
    source.fail_cleanup.store(true, Ordering::SeqCst);
    let capacity = NonZeroU64::new(1024).unwrap();
    let mut runtime = CdcRuntime::new(
        source.clone(),
        Arc::new(arrow_schema::Schema::empty()),
        phase.clone(),
        checkpoint.clone(),
        spool.clone(),
        capacity,
    );
    let transaction = transactions.begin();
    let access = transaction.access();
    phase.access(access).unwrap().set(&1).unwrap();
    checkpoint.access(access).unwrap().set(&vec![9]).unwrap();
    assert!(
        spool
            .access(access)
            .unwrap()
            .try_push(&vec![1], capacity)
            .unwrap()
    );
    assert!(
        spool
            .access(access)
            .unwrap()
            .try_push(&vec![2], capacity)
            .unwrap()
    );
    transaction.commit().unwrap();

    commit(&mut runtime, &mut transactions);
    assert!(runtime.turn(None).is_err());
    assert_eq!(source.cleanup_calls.load(Ordering::SeqCst), 1);
    source.fail_cleanup.store(false, Ordering::SeqCst);
    let Turn::Ready(prepared) = runtime.turn(None).unwrap() else {
        panic!("cleanup success must prepare reset");
    };
    let transaction = transactions.begin();
    let (_, completion) = prepared.apply(transaction.access()).unwrap();
    drop(transaction);
    drop(completion);

    let transaction = transactions.begin();
    let access = transaction.access();
    assert_eq!(phase.access(access).unwrap().get().unwrap(), Some(1));
    assert_eq!(
        checkpoint.access(access).unwrap().get().unwrap(),
        Some(vec![9])
    );
    assert_eq!(spool.access(access).unwrap().queued_bytes().unwrap(), 18);
    transaction.commit().unwrap();

    // The external cleanup is safe to repeat when its following local
    // transaction did not commit. Only then may bounded spool cleanup begin.
    commit(&mut runtime, &mut transactions);
    assert_eq!(source.cleanup_calls.load(Ordering::SeqCst), 3);
    commit(&mut runtime, &mut transactions);
    let transaction = transactions.begin();
    let access = transaction.access();
    assert_eq!(phase.access(access).unwrap().get().unwrap(), Some(4));
    assert_eq!(spool.access(access).unwrap().queued_bytes().unwrap(), 9);
    transaction.commit().unwrap();
    commit(&mut runtime, &mut transactions);
    let transaction = transactions.begin();
    let access = transaction.access();
    assert_eq!(phase.access(access).unwrap().get().unwrap(), None);
    assert_eq!(checkpoint.access(access).unwrap().get().unwrap(), None);
    assert!(spool.access(access).unwrap().is_empty().unwrap());
    transaction.commit().unwrap();
}
