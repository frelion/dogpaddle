use super::*;

fn seed_source(path: &std::path::Path, values: Vec<u64>) {
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use dogpaddle_store::Queue;
    use std::{num::NonZeroU64, sync::Arc};
    let rows = values.len();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let change = Change::try_new(
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))]).unwrap(),
        Int64Array::from(vec![1; rows]),
    )
    .unwrap();
    let encoded = dogpaddle_change::SchemaBoundChangeCodec::try_new(change.schema())
        .unwrap()
        .encode(&change)
        .unwrap();
    let store = Store::open(path).unwrap();
    let queue: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert!(
        queue
            .access(transaction.access())
            .unwrap()
            .try_push(&encoded, NonZeroU64::MAX)
            .unwrap()
    );
    transaction.commit().unwrap();
}
fn wide_tail(path: &std::path::Path, width: usize) {
    use dogpaddle_operation::{lit, operation::transform::SelectDefinition};
    let mut factory = FlowFactory::new(path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let count = factory.operation("count", RunningEventCountDefinition::new(), [source]);
    let wide = factory.operation(
        "wide",
        SelectDefinition::try_new([("value", lit("x".repeat(width)))]).unwrap(),
        [count],
    );
    factory.operation("sink", DiscardDefinition::new(), [wide]);
    drop(factory.build().unwrap());
}

#[test]
fn oversized_pages_retry_the_same_input_and_roll_back_the_entire_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    wide_tail(&path, 10_000);
    seed_source(&path, (0..512).collect());
    let mut flow = FlowFactory::new(&path).open().unwrap();
    install_probe(&mut flow, true, false);
    one_action(&mut flow);
    let bytes = flow
        .runtime
        .frames
        .output(0, flow.reads.begin().access())
        .unwrap();
    let output = flow.runtime.output_codec(0).decode(&bytes).unwrap();
    assert_eq!(output.num_rows(), 64);
    assert!(bytes.len() <= PAGE_BYTES);
    let top = flow
        .runtime
        .frames
        .top(flow.reads.begin().access())
        .unwrap()
        .unwrap();
    assert!(matches!(
        top.1.phase,
        FramePhase::Send {
            next_consumer: 0,
            after: Progress::More(_),
        }
    ));
    let input = flow
        .runtime
        .input(0, &top.1, flow.reads.begin().access())
        .unwrap();
    assert_eq!(
        flow.runtime
            .input_codec(&top.1)
            .decode(&input)
            .unwrap()
            .num_rows(),
        512
    );
    drop(flow);
    assert_eq!(count(&path), Some(64));
    let mut flow = FlowFactory::new(&path).open().unwrap();
    for _ in 0..100 {
        if one_action(&mut flow) == AdvanceOutcome::Idle {
            break;
        }
        drop(flow);
        flow = FlowFactory::new(&path).open().unwrap();
    }
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    assert_eq!(count(&path), Some(512));
}
#[test]
fn an_oversized_single_event_fails_with_its_state_and_resume_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    wide_tail(&path, 2 * PAGE_BYTES);
    seed_source(&path, vec![1]);
    for _ in 0..2 {
        let mut flow = FlowFactory::new(&path).open().unwrap();
        let error = flow.advance().unwrap_err();
        assert!(!error.requires_reopen());
        assert_eq!(flow.status().unwrap().depth, 1);
        drop(flow);
        assert_eq!(count(&path), None);
    }
}

#[test]
fn route_budget_yields_at_the_first_unsent_sink_and_reopen_preserves_the_prefix() {
    use dogpaddle_operation::{lit, operation::transform::SelectDefinition};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(0), []);
    let wide = factory.operation(
        "wide",
        SelectDefinition::try_new([("value", lit("x".repeat(100 * 1024)))]).unwrap(),
        [source],
    );
    for index in 0..32 {
        factory.operation(format!("sink-{index}"), DiscardDefinition::new(), [wide]);
    }
    drop(factory.build().unwrap());
    seed_source(&path, vec![42]);
    let enqueues = (0..32)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect::<Vec<_>>();
    let accepted = (0..32)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect::<Vec<_>>();
    let mut flow = FlowFactory::new(&path).open().unwrap();
    for index in 0..32 {
        let sink = flow.runtime.sinks[index];
        install_routing_probe(
            &mut flow,
            sink,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&enqueues[index]),
            Arc::clone(&accepted[index]),
            false,
        );
    }
    one_action(&mut flow);
    let (_, frame) = flow
        .runtime
        .frames
        .top(flow.reads.begin().access())
        .unwrap()
        .unwrap();
    let FramePhase::Send {
        next_consumer,
        after: Progress::Done,
    } = frame.phase
    else {
        panic!("routing a bounded prefix must retain its exact unsent position");
    };
    assert!((1..32).contains(&next_consumer));
    for (index, calls) in enqueues.iter().enumerate() {
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(index < next_consumer)
        );
    }
    let pending = flow
        .runtime
        .frames
        .output(0, flow.reads.begin().access())
        .unwrap();
    drop(flow);
    flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(
        flow.runtime
            .frames
            .output(0, flow.reads.begin().access())
            .unwrap(),
        pending
    );
    for index in 0..32 {
        let sink = flow.runtime.sinks[index];
        install_routing_probe(
            &mut flow,
            sink,
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&enqueues[index]),
            Arc::clone(&accepted[index]),
            false,
        );
    }
    one_action(&mut flow);
    assert_eq!(flow.status().unwrap().depth, 0);
    for index in 0..32 {
        assert_eq!(enqueues[index].load(Ordering::Relaxed), 1);
        assert_eq!(accepted[index].load(Ordering::Relaxed), 1);
    }
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
}

#[test]
fn a_routing_budget_failure_rolls_back_the_computed_tail_and_keeps_its_input() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut flow = counted_flow(&path);
    capture(&mut flow, 0);
    let depth = 0;
    let frame = initial_source_frame(&flow, 0);
    assert!(
        flow.runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap()
            .is_none()
    );
    let bytes = flow
        .runtime
        .input(depth, &frame, flow.reads.begin().access())
        .unwrap();
    let input = flow
        .runtime
        .input_codec(&frame)
        .decode_owned(bytes.clone())
        .unwrap();
    {
        let mut batch = flow.transactions.durability_batch();
        let transaction = batch.begin();
        // The source slice and count tail fit; encoding/routing cannot be admitted.
        let mut budget = StepBudget::new(256, CONTROL_BYTES + 48);
        let error = flow
            .runtime
            .run_page(depth, &frame, &input, transaction.access(), &mut budget)
            .unwrap_err();
        assert!(is_budget_error(error.as_ref()));
        drop(transaction);
        batch.finish().unwrap();
    }
    assert_eq!(
        flow.runtime
            .frames
            .top(flow.reads.begin().access())
            .unwrap(),
        None
    );
    assert_eq!(
        flow.runtime
            .input(depth, &frame, flow.reads.begin().access())
            .unwrap(),
        bytes
    );
    assert!(
        flow.runtime
            .frames
            .outputs
            .read(flow.reads.begin().access())
            .unwrap()
            .get_bounded(&depth, 0)
            .unwrap()
            .is_none()
    );
    drop(flow);
    assert_eq!(count(&path), None);
    flow = FlowFactory::new(&path).open().unwrap();
    one_action(&mut flow);
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    assert_eq!(count(&path), Some(1));
}
