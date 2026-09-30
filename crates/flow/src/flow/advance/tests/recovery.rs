use super::*;

#[test]
fn every_durable_call_transition_can_reopen_without_repeating_a_branch() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    for index in 0..2 {
        let count = factory.operation(
            format!("count-{index}"),
            RunningEventCountDefinition::new(),
            [source],
        );
        factory.operation(format!("sink-{index}"), DiscardDefinition::new(), [count]);
    }
    let mut flow = factory.build().unwrap();
    capture(&mut flow, 0);
    let mut steps = 0;
    let mut child_calls = Vec::new();
    loop {
        let outcome = one_action(&mut flow);
        steps += 1;
        if let Some((depth, frame)) = flow
            .runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap()
            && depth != 0
        {
            let read = flow.reads.begin();
            let parent = flow
                .runtime
                .frames
                .controls
                .read(read.access())
                .unwrap()
                .get_bounded(&(depth - 1), CONTROL_BYTES)
                .unwrap()
                .unwrap();
            let FramePhase::Send { next_consumer, .. } = parent.phase else {
                panic!("an active child always has a suspended sending parent");
            };
            let consumer = flow.runtime.topology.consumers[parent.head][next_consumer - 1];
            assert_eq!(consumer.operation, frame.head);
            assert_eq!(Some(consumer.port), frame.input_port);
            assert_eq!(
                flow.runtime.input(depth, &frame, read.access()).unwrap(),
                flow.runtime
                    .frames
                    .output(depth - 1, read.access())
                    .unwrap()
            );
            child_calls.push(frame.head);
        }
        drop(flow);
        for index in [1, 3] {
            assert!(matches!(operation_count(&path, index), None | Some(1)));
        }
        flow = FlowFactory::new(&path).open().unwrap();
        if outcome == AdvanceOutcome::Idle {
            break;
        }
        assert!(steps < 40);
    }
    assert_eq!(child_calls, [1, 3]);
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    let store = Store::open(&path).unwrap();
    let read = store.read_transaction();
    for index in [1, 3] {
        let count: Cell<u64> = store
            .open_data(&format!("operation/{index:08x}/running_event_count.count"))
            .unwrap();
        assert_eq!(count.read(read.access()).unwrap().get().unwrap(), Some(1));
    }
}

#[test]
fn same_source_self_join_accounts_the_cross_term_once() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("sink.sqlite");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let join = factory.operation(
        "join",
        EquiJoinDefinition::try_new(
            EquiJoinKind::Inner,
            [(col("value"), col("value"))],
            ["left", "right"],
            None,
        )
        .unwrap(),
        [source, source],
    );
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, "result").unwrap(),
        [join],
    );
    let mut flow = factory.build().unwrap();
    for _ in 0..10 {
        flow.advance().unwrap();
    }
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    let connection = rusqlite::Connection::open(target).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM result", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

fn install_two_branch_probes(
    flow: &mut Flow,
    blocked: &[Arc<AtomicBool>; 2],
    enqueues: &[Arc<AtomicUsize>; 2],
    accepted: &[Arc<AtomicUsize>; 2],
) {
    for ordinal in 0..2 {
        install_routing_probe(
            flow,
            flow.runtime.sinks[ordinal],
            Arc::clone(&blocked[ordinal]),
            Arc::clone(&enqueues[ordinal]),
            Arc::clone(&accepted[ordinal]),
            false,
        );
    }
}

#[test]
fn reopening_a_partially_routed_page_never_repeats_or_skips_a_sink_branch() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    factory.operation("first", DiscardDefinition::new(), [source]);
    factory.operation("blocked", DiscardDefinition::new(), [source]);
    let mut flow = factory.build().unwrap();
    let blocked = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
    ];
    let enqueues = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    let accepted = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    install_two_branch_probes(&mut flow, &blocked, &enqueues, &accepted);
    capture(&mut flow, 0);
    one_action(&mut flow);
    let suspended = flow
        .runtime
        .frames
        .top(flow.reads.begin().access())
        .unwrap()
        .unwrap();
    assert!(matches!(
        suspended.1.phase,
        FramePhase::Send {
            next_consumer: 1,
            after: Progress::Done,
        }
    ));
    let output = flow
        .runtime
        .frames
        .output(0, flow.reads.begin().access())
        .unwrap();
    assert_eq!(accepted[0].load(Ordering::Relaxed), 1);
    assert_eq!(accepted[1].load(Ordering::Relaxed), 0);
    for _ in 0..3 {
        drop(flow);
        flow = FlowFactory::new(&path).open().unwrap();
        install_two_branch_probes(&mut flow, &blocked, &enqueues, &accepted);
        one_action(&mut flow);
        assert_eq!(
            flow.runtime
                .frames
                .top(flow.reads.begin().access())
                .unwrap(),
            Some(suspended.clone())
        );
        assert_eq!(
            flow.runtime
                .frames
                .output(0, flow.reads.begin().access())
                .unwrap(),
            output
        );
        assert_eq!(enqueues[0].load(Ordering::Relaxed), 1);
        assert_eq!(accepted[1].load(Ordering::Relaxed), 0);
    }
    blocked[1].store(false, Ordering::Relaxed);
    drop(flow);
    flow = FlowFactory::new(&path).open().unwrap();
    install_two_branch_probes(&mut flow, &blocked, &enqueues, &accepted);
    one_action(&mut flow);
    assert_eq!(flow.status().unwrap().depth, 0);
    assert_eq!(accepted[0].load(Ordering::Relaxed), 1);
    assert_eq!(accepted[1].load(Ordering::Relaxed), 1);
    assert!(enqueues[1].load(Ordering::Relaxed) >= 5);
    assert_eq!(one_action(&mut flow), AdvanceOutcome::Idle);
}
