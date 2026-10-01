use super::support::encode_output_entry;
use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_flow::{FlowError, FlowFactory};
use dogpaddle_operation::operation::{
    Resume, scan::SequenceScanDefinition, sink::DiscardDefinition,
    transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, OrderedMap, Queue, Store, StoreValue};
use std::{borrow::Cow, num::NonZeroU64, sync::Arc};

#[test]
fn a_fused_intermediate_frame_head_is_rejected_without_rewriting_state() {
    use dogpaddle_operation::{col, operation::transform::SelectDefinition};

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let first = factory.operation(
        "first",
        SelectDefinition::try_new([("first", col("value"))]).unwrap(),
        [source],
    );
    let last = factory.operation(
        "last",
        SelectDefinition::try_new([("last", col("first"))]).unwrap(),
        [first],
    );
    factory.operation("sink", DiscardDefinition::new(), [last]);
    drop(factory.build().unwrap());
    let input = payload("value", u64::MAX);
    let control = run_frame(1, None);
    write(&path, &[(0, control.clone())], Some(&input), &[]);
    let definition = {
        let store = Store::open(&path).unwrap();
        let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
        definition
            .read(store.read_transaction().access())
            .unwrap()
            .get()
            .unwrap()
    };
    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::InvalidRuntimeState { reason })
            if reason == "frame does not refer to a computation head"
    ));
    assert_eq!(read_control(&path, 0), Some(control));
    let store = Store::open(&path).unwrap();
    let read = store.read_transaction();
    let persisted_definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    assert_eq!(
        persisted_definition
            .read(read.access())
            .unwrap()
            .get()
            .unwrap(),
        definition
    );
    let queue: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    assert_eq!(
        queue
            .read(read.access())
            .unwrap()
            .front_bounded(input.len())
            .unwrap(),
        Some(input)
    );
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    assert_eq!(
        position.read(read.access()).unwrap().get().unwrap(),
        Some(u64::MAX)
    );
}

fn fixture(path: &std::path::Path) {
    let mut factory = FlowFactory::new(path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let count = factory.operation("count", RunningEventCountDefinition::new(), [source]);
    factory.operation("sink", DiscardDefinition::new(), [count]);
    factory.operation("second", DiscardDefinition::new(), [source]);
    drop(factory.build().unwrap());
}
fn payload(name: &str, value: u64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)]));
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![value]))]).unwrap();
    encode_output_entry(&Change::try_new(records, Int64Array::from(vec![1])).unwrap())
}
fn run_frame(head: u32, port: Option<u32>) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&head.to_be_bytes());
    if let Some(port) = port {
        bytes.push(1);
        bytes.extend_from_slice(&port.to_be_bytes());
    } else {
        bytes.push(0);
    }
    bytes.push(0);
    bytes.extend_from_slice(Resume::batch().encode_value().unwrap().as_ref());
    bytes
}
fn send_frame(next: u32) -> Vec<u8> {
    let mut bytes = vec![1, 0, 0, 0, 0, 0, 1];
    bytes.extend_from_slice(&next.to_be_bytes());
    bytes.push(0);
    bytes
}
fn write(
    path: &std::path::Path,
    controls: &[(u32, Vec<u8>)],
    input: Option<&Vec<u8>>,
    outputs: &[(u32, Vec<u8>)],
) {
    let store = Store::open(path).unwrap();
    let root_input: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    let position: Cell<u64> = store
        .open_data("operation/00000000/sequence_scan.position")
        .unwrap();
    let maps = ["flow/frames", "flow/outputs"]
        .map(|name| store.open_data::<OrderedMap<u32, Vec<u8>>>(name).unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    if let Some(input) = input {
        assert!(
            root_input
                .access(transaction.access())
                .unwrap()
                .try_push(input, NonZeroU64::MAX)
                .unwrap()
        );
        position
            .access(transaction.access())
            .unwrap()
            .set(&u64::MAX)
            .unwrap();
    }
    for (map, values) in maps.iter().zip([controls, outputs]) {
        for (key, value) in values {
            map.access(transaction.access())
                .unwrap()
                .put(key, value)
                .unwrap();
        }
    }
    transaction.commit().unwrap();
}
fn read_control(path: &std::path::Path, depth: u32) -> Option<Vec<u8>> {
    let store = Store::open(path).unwrap();
    let map: OrderedMap<u32, Vec<u8>> = store.open_data("flow/frames").unwrap();
    map.read(store.read_transaction().access())
        .unwrap()
        .get(&depth)
        .unwrap()
}
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the independent malformed frame matrix together."
)]
fn malformed_or_inconsistent_frames_are_rejected_without_rewriting_state() {
    let root = tempfile::tempdir().unwrap();
    let mut bad_resume = run_frame(0, None);
    let resume_start = 7;
    let mut resume = Resume::batch().encode_value().unwrap().as_ref().to_vec();
    resume[1] = 5;
    assert!(Resume::decode_value(Cow::Borrowed(&resume)).is_ok());
    bad_resume.truncate(resume_start);
    bad_resume.extend_from_slice(&resume);
    for (name, depth, control, input, output) in [
        ("truncated", 0, vec![1], payload("value", 1), None),
        (
            "unknown-head",
            0,
            run_frame(999, None),
            payload("value", 1),
            None,
        ),
        (
            "source-port",
            0,
            run_frame(0, Some(0)),
            payload("value", 1),
            None,
        ),
        (
            "non-source-root",
            0,
            run_frame(1, Some(0)),
            payload("value", 1),
            None,
        ),
        (
            "depth-gap",
            1,
            run_frame(0, None),
            payload("value", 1),
            None,
        ),
        ("past-input", 0, bad_resume, payload("value", 1), None),
        (
            "wrong-schema",
            0,
            run_frame(0, None),
            payload("count", 1),
            None,
        ),
        (
            "invalid-payload",
            0,
            run_frame(0, None),
            vec![0, 1, 2],
            None,
        ),
        (
            "oversized-source-front",
            0,
            run_frame(0, None),
            vec![0; 9 * 1024 * 1024],
            None,
        ),
        (
            "run-output",
            0,
            run_frame(0, None),
            payload("value", 1),
            Some(payload("value", 1)),
        ),
        (
            "oversized-run-output",
            0,
            run_frame(0, None),
            payload("value", 1),
            Some(vec![0; 9 * 1024 * 1024]),
        ),
        (
            "send-without-output",
            0,
            send_frame(0),
            payload("value", 1),
            None,
        ),
        (
            "past-consumer",
            0,
            send_frame(3),
            payload("value", 1),
            Some(payload("value", 1)),
        ),
    ] {
        let path = root.path().join(name);
        fixture(&path);
        let outputs = output
            .into_iter()
            .map(|bytes| (depth, bytes))
            .collect::<Vec<_>>();
        write(&path, &[(depth, control.clone())], Some(&input), &outputs);
        assert!(
            matches!(
                FlowFactory::new(&path).open(),
                Err(FlowError::InvalidRuntimeState { .. })
            ),
            "{name}"
        );
        assert_eq!(read_control(&path, depth), Some(control), "{name}");
        let store = Store::open(&path).unwrap();
        let queue: Queue<Vec<u8>> = store
            .open_data("operation/00000000/sequence_scan.published")
            .unwrap();
        assert_eq!(
            queue
                .read(store.read_transaction().access())
                .unwrap()
                .front_bounded(input.len())
                .unwrap(),
            Some(input),
            "{name}"
        );
    }
}
#[test]
fn child_borrows_the_parent_page_and_validates_the_call_edge() {
    let root = tempfile::tempdir().unwrap();
    for (name, parent, child, output) in [
        (
            "valid",
            send_frame(1),
            run_frame(1, Some(0)),
            Some(payload("value", 1)),
        ),
        (
            "wrong-head",
            send_frame(1),
            run_frame(2, Some(0)),
            Some(payload("value", 1)),
        ),
        (
            "wrong-port",
            send_frame(1),
            run_frame(1, Some(1)),
            Some(payload("value", 1)),
        ),
        (
            "wrong-parent-call",
            send_frame(2),
            run_frame(1, Some(0)),
            Some(payload("value", 1)),
        ),
        (
            "running-parent",
            run_frame(0, None),
            run_frame(1, Some(0)),
            Some(payload("value", 1)),
        ),
        ("missing-page", send_frame(1), run_frame(1, Some(0)), None),
        (
            "wrong-schema",
            send_frame(1),
            run_frame(1, Some(0)),
            Some(payload("count", 1)),
        ),
    ] {
        let path = root.path().join(name);
        fixture(&path);
        let outputs = output
            .into_iter()
            .map(|bytes| (0, bytes))
            .collect::<Vec<_>>();
        write(
            &path,
            &[(0, parent.clone()), (1, child.clone())],
            Some(&payload("value", 1)),
            &outputs,
        );
        let result = FlowFactory::new(&path).open();
        if name == "valid" {
            assert_eq!(result.unwrap().status().unwrap().depth, 2);
        } else {
            assert!(
                matches!(result, Err(FlowError::InvalidRuntimeState { .. })),
                "{name}"
            );
        }
        assert_eq!(read_control(&path, 0), Some(parent), "{name}");
        assert_eq!(read_control(&path, 1), Some(child), "{name}");
    }
}
#[test]
fn orphan_payloads_are_rejected_even_with_an_empty_stack() {
    let root = tempfile::tempdir().unwrap();
    for (name, outputs) in [
        ("output", vec![(0, payload("value", 1))]),
        ("oversized-output", vec![(0, vec![0; 2 * 1024 * 1024])]),
    ] {
        let path = root.path().join(name);
        fixture(&path);
        write(&path, &[], None, &outputs);
        assert!(
            matches!(
                FlowFactory::new(path).open(),
                Err(FlowError::InvalidRuntimeState { .. })
            ),
            "{name}"
        );
    }
}

#[test]
fn a_root_frame_requires_its_source_queue_front() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    fixture(&path);
    let control = run_frame(0, None);
    write(&path, &[(0, control.clone())], None, &[]);
    assert!(matches!(
        FlowFactory::new(&path).open(),
        Err(FlowError::InvalidRuntimeState { .. })
    ));
    assert_eq!(read_control(&path, 0), Some(control));
}

#[test]
fn an_empty_stack_borrows_pending_source_input_without_creating_a_payload_resource() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    fixture(&path);
    write(&path, &[], Some(&payload("value", 1)), &[]);
    let mut flow = FlowFactory::new(&path).open().unwrap();
    assert_eq!(flow.status().unwrap().depth, 0);
    flow.advance().unwrap();
    drop(flow);
    let store = Store::open(&path).unwrap();
    let queue: Queue<Vec<u8>> = store
        .open_data("operation/00000000/sequence_scan.published")
        .unwrap();
    assert!(
        queue
            .read(store.read_transaction().access())
            .unwrap()
            .is_empty()
            .unwrap()
    );
    let frames: OrderedMap<u32, Vec<u8>> = store.open_data("flow/frames").unwrap();
    assert_eq!(
        frames
            .read(store.read_transaction().access())
            .unwrap()
            .get(&0)
            .unwrap(),
        None
    );
}
