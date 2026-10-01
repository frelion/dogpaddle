use dogpaddle_store::StoreSetup;

use super::*;

#[test]
fn a_failed_first_page_keeps_its_source_identity_and_never_bypasses_to_another_root() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let first = factory.operation("first", RunningEventCountDefinition::new(), [source]);
    let last = factory.operation("last", RunningEventCountDefinition::new(), [first]);
    factory.operation("sink", DiscardDefinition::new(), [last]);
    let other = factory.operation("other-source", SequenceScanDefinition::new(u64::MAX), []);
    let other_count = factory.operation("other-count", RunningEventCountDefinition::new(), [other]);
    factory.operation("other-sink", DiscardDefinition::new(), [other_count]);
    drop(factory.build().unwrap());
    let store = Store::open(&path).unwrap();
    let last: Cell<u64> = store
        .open_data("operation/00000002/running_event_count.count")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    last.access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    capture(&mut flow, 0);
    capture(&mut flow, 4);
    let frame = initial_source_frame(&flow, 0);
    let other_frame = initial_source_frame(&flow, 4);
    let input = flow
        .runtime
        .input(0, &frame, flow.reads.begin().access())
        .unwrap();
    let other_input = flow
        .runtime
        .input(0, &other_frame, flow.reads.begin().access())
        .unwrap();
    assert!(
        flow.runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap()
            .is_none()
    );
    for reopen in [false, false, true, false, true] {
        if reopen {
            drop(flow);
            assert_eq!(operation_count(&path, 1), None);
            assert_eq!(operation_count(&path, 5), None);
            flow = FlowFactory::new(&path).open().unwrap();
        }
        let error = flow.advance().unwrap_err();
        assert!(!error.requires_reopen());
        assert_eq!(error.operation_id(), "source");
        assert_eq!(
            flow.runtime
                .frames
                .top(flow.reads.begin().access())
                .unwrap(),
            Some((0, frame.clone()))
        );
        assert_eq!(
            flow.runtime
                .input(0, &frame, flow.reads.begin().access())
                .unwrap(),
            input
        );
        assert_eq!(
            flow.runtime
                .input(0, &other_frame, flow.reads.begin().access())
                .unwrap(),
            other_input
        );
    }
    drop(flow);
    assert_eq!(operation_count(&path, 1), None);
    assert_eq!(operation_count(&path, 5), None);
}

fn drain_once(flow: &mut Flow, index: usize) {
    let mut batch = flow.transactions.durability_batch();
    flow.runtime.drain(index, &flow.reads, &mut batch).unwrap();
    batch.finish().unwrap();
}

#[expect(
    clippy::type_complexity,
    reason = "Compare exact persisted control and ordered queue bytes."
)]
fn sink_state(path: &std::path::Path, index: usize) -> (Option<Vec<u8>>, Vec<(u64, Vec<u8>)>) {
    use dogpaddle_store::{OrderedMap, ScanDirection, ScanLimit};
    let store = Store::open(path).unwrap();
    let control: Cell<Vec<u8>> = store
        .open_data(&format!("operation/{index:08x}/sink.control"))
        .unwrap();
    let buffer: OrderedMap<u64, Vec<u8>> = store
        .open_data(&format!("operation/{index:08x}/sink.buffer"))
        .unwrap();
    let read = store.read_transaction();
    (
        control
            .read(read.access())
            .unwrap()
            .get_bounded(ROOT_BYTES)
            .unwrap(),
        buffer
            .read(read.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(4, ROOT_BYTES).unwrap(),
            )
            .unwrap()
            .entries,
    )
}

#[test]
fn a_sink_error_after_enqueue_rolls_back_the_head_outbox_and_route_position_together() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("sink.sqlite");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let counter = factory.operation("count", RunningEventCountDefinition::new(), [source]);
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, "result").unwrap(),
        [counter],
    );
    let mut flow = factory.build().unwrap();
    let sink = flow.runtime.sinks[0];
    drain_once(&mut flow, sink);
    capture(&mut flow, 0);
    let frame = initial_source_frame(&flow, 0);
    let before = Some((0, frame.clone()));
    assert!(
        flow.runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap()
            .is_none()
    );
    let input = flow
        .runtime
        .input(0, &frame, flow.reads.begin().access())
        .unwrap();
    drop(flow);
    let expected_sink = sink_state(&path, sink);
    assert!(expected_sink.1.is_empty());
    for _ in 0..2 {
        flow = FlowFactory::new(&path).open().unwrap();
        install_routing_probe(
            &mut flow,
            sink,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            true,
        );
        let error = try_one_action(&mut flow).unwrap_err();
        assert!(error.to_string().contains("injected sink failure"));
        assert_eq!(
            flow.runtime
                .frames
                .top(flow.reads.begin().access())
                .unwrap(),
            before
        );
        assert_eq!(
            flow.runtime
                .input(0, &frame, flow.reads.begin().access())
                .unwrap(),
            input
        );
        assert!(
            flow.runtime
                .frames
                .outputs
                .read(flow.reads.begin().access())
                .unwrap()
                .get_bounded(&0, 0)
                .unwrap()
                .is_none()
        );
        drop(flow);
        assert_eq!(count(&path), None);
        assert_eq!(sink_state(&path, sink), expected_sink);
    }
    flow = FlowFactory::new(&path).open().unwrap();
    one_action(&mut flow);
    assert_eq!(flow.status().unwrap().depth, 0);
    drain_once(&mut flow, sink);
    drop(flow);
    assert_eq!(count(&path), Some(1));
    let connection = rusqlite::Connection::open(target).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM result", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn backpressure_retains_the_pending_call_while_capture_and_drain_continue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut flow = counted_flow(&path);
    let (blocked, loads) = install_probe(&mut flow, true, false);
    flow.advance().unwrap();
    let initial = flow
        .runtime
        .frames
        .top(flow.reads.begin().access())
        .unwrap()
        .unwrap();
    assert!(matches!(
        initial.1.phase,
        FramePhase::Send {
            next_consumer: 0,
            ..
        }
    ));
    let input = flow
        .runtime
        .input(0, &initial.1, flow.reads.begin().access())
        .unwrap();
    flow.advance().unwrap();
    flow.advance().unwrap();
    assert_eq!(
        flow.runtime
            .input(0, &initial.1, flow.reads.begin().access())
            .unwrap(),
        input
    );
    assert_eq!(
        flow.runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap(),
        Some(initial)
    );
    assert_eq!(loads.load(Ordering::Relaxed), 3);
    blocked.store(false, Ordering::Relaxed);
    drop(flow);
    let store = Store::open(&path).unwrap();
    let source: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    assert_eq!(
        source.access(transaction.access()).unwrap().get().unwrap(),
        Some(2)
    );
    source
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    transaction.commit().unwrap();
    drop(transactions);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    for _ in 0..20 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            break;
        }
    }
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    assert_eq!(count(&path), Some(3));
}
#[test]
fn a_panicking_boundary_fail_stops_the_runtime_before_any_further_capture() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut flow = counted_flow(&path);
    install_probe(&mut flow, false, true);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| flow.advance())).is_err());
    assert!(flow.status().unwrap().needs_reopen);
    assert!(flow.advance().unwrap_err().requires_reopen());
    drop(flow);
    assert_eq!(count(&path), Some(1));
    let mut flow = FlowFactory::new(&path).open().unwrap();
    flow.advance().unwrap();
    drop(flow);
    assert_eq!(count(&path), Some(2));
}

#[test]
fn a_rejected_commit_fail_stops_all_future_source_and_stack_work() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut flow = counted_flow(&path);
    let mut foreign = StoreSetup::new();
    let foreign_cell: Cell<u64> = foreign.create_data("control").unwrap();
    let _foreign_transactions = foreign
        .commit(root.path().join("foreign"), |_| Ok(()))
        .unwrap();
    {
        let mut batch = flow.transactions.durability_batch();
        let transaction = batch.begin();
        assert!(foreign_cell.access(transaction.access()).is_err());
        assert!(commit(transaction, &mut flow.runtime.needs_reopen).is_err());
        batch.finish().unwrap();
    }
    assert!(flow.advance().unwrap_err().requires_reopen());
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    assert_eq!(count(&path), None);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    flow.advance().unwrap();
    drop(flow);
    assert_eq!(count(&path), Some(1));
}
