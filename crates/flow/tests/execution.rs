use std::io;

use dogpaddle_flow::{
    Decision, Event, Flow, FlowError, Operation, OperationError, StepOutcome, Work,
};
use dogpaddle_store::{Cell, DataPlacement, OrderedMap, Store, Transaction};

struct Source {
    tag: u8,
}

impl Operation for Source {
    fn fingerprint(&self) -> &[u8] {
        match self.tag {
            1 => b"source-left",
            2 => b"source-right",
            _ => b"source",
        }
    }

    fn step(
        &mut self,
        work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        assert_eq!(work.event(), Event::Start);
        match work.checkpoint() {
            None => Ok(Decision::Publish {
                output: vec![self.tag, 1],
                checkpoint: vec![1],
            }),
            Some([1]) => Ok(Decision::Publish {
                output: vec![self.tag, 2],
                checkpoint: vec![2],
            }),
            Some([2]) => Ok(Decision::Complete {
                output: Some(vec![self.tag, 3]),
            }),
            checkpoint => panic!("unexpected source checkpoint {checkpoint:?}"),
        }
    }
}

struct Merge;

impl Operation for Merge {
    fn fingerprint(&self) -> &[u8] {
        b"merge"
    }

    fn step(
        &mut self,
        work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        match work.event() {
            Event::Data { port, bytes, .. } => {
                let mut output = vec![u8::from(port == "right")];
                output.extend_from_slice(bytes);
                Ok(Decision::Complete {
                    output: Some(output),
                })
            }
            Event::End { .. } => Ok(Decision::Complete { output: None }),
            Event::Start => panic!("merge is not a source"),
        }
    }
}

struct Sink {
    rows: OrderedMap<u64, Vec<u8>>,
}

impl Operation for Sink {
    fn fingerprint(&self) -> &[u8] {
        b"sink"
    }

    fn step(
        &mut self,
        work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        match work.event() {
            Event::Data {
                position, bytes, ..
            } => {
                self.rows
                    .access(transaction)?
                    .put(&position, &bytes.to_vec())?;
                Ok(Decision::Complete { output: None })
            }
            Event::End { .. } => Ok(Decision::Complete { output: None }),
            Event::Start => panic!("sink is not a source"),
        }
    }
}

fn declare(path: &std::path::Path, create: bool) -> Flow {
    let mut flow = if create {
        Flow::create(path).unwrap()
    } else {
        Flow::open(path).unwrap()
    };
    let rows = OrderedMap::new(flow.data("sink/rows", DataPlacement::Dedicated).unwrap());
    let left = flow.stage("left", &[], Source { tag: 1 }).unwrap();
    let right = flow.stage("right", &[], Source { tag: 2 }).unwrap();
    let merge = flow.stage("merge", &["left", "right"], Merge).unwrap();
    let sink = flow.stage("sink", &["input"], Sink { rows }).unwrap();
    flow.connect(left, merge, "left").unwrap();
    flow.connect(right, merge, "right").unwrap();
    flow.connect(merge, sink, "input").unwrap();
    flow
}

fn finish_with_reopen(path: &std::path::Path) {
    for _ in 0..100 {
        let mut flow = declare(path, false);
        match flow.step().unwrap() {
            StepOutcome::Progress => {}
            StepOutcome::Finished => return,
            StepOutcome::Idle => panic!("finite flow became idle"),
        }
    }
    panic!("finite flow did not finish");
}

#[test]
fn durable_fan_in_runs_to_completion_and_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = declare(&path, true);
        assert_eq!(flow.step().unwrap(), StepOutcome::Progress);
    }
    finish_with_reopen(&path);

    let store = Store::open(&path).unwrap();
    let rows = OrderedMap::<u64, Vec<u8>>::new(store.open_data("sink/rows").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let rows = rows.access(&transaction).unwrap();
    let mut actual = Vec::new();
    for position in 0..6 {
        actual.push(rows.get(&position).unwrap().unwrap());
    }
    assert_eq!(
        actual,
        vec![
            vec![0, 1, 1],
            vec![1, 2, 1],
            vec![0, 1, 2],
            vec![1, 2, 2],
            vec![0, 1, 3],
            vec![1, 2, 3],
        ]
    );
}

struct PendingWrite {
    value: Cell<u64>,
}

impl Operation for PendingWrite {
    fn fingerprint(&self) -> &[u8] {
        b"pending-write"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        self.value.access(transaction)?.set(&99)?;
        Ok(Decision::Pending)
    }
}

#[test]
fn pending_rolls_back_operation_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        let value = Cell::new(flow.data("value", DataPlacement::Shared).unwrap());
        flow.stage("pending", &[], PendingWrite { value }).unwrap();
        assert_eq!(flow.step().unwrap(), StepOutcome::Idle);
    }

    let store = Store::open(&path).unwrap();
    let value = Cell::<u64>::new(store.open_data("value").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(value.access(&transaction).unwrap().get().unwrap(), None);
}

struct EmptyCheckpoint;

impl Operation for EmptyCheckpoint {
    fn fingerprint(&self) -> &[u8] {
        b"empty-checkpoint"
    }

    fn step(
        &mut self,
        work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        match work.checkpoint() {
            None => Ok(Decision::Checkpoint {
                checkpoint: Vec::new(),
            }),
            Some([]) => Ok(Decision::Complete { output: None }),
            checkpoint => panic!("unexpected checkpoint {checkpoint:?}"),
        }
    }
}

#[test]
fn an_empty_checkpoint_is_distinct_from_no_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        flow.stage("source", &[], EmptyCheckpoint).unwrap();
        assert_eq!(flow.step().unwrap(), StepOutcome::Progress);
    }
    let mut flow = Flow::open(&path).unwrap();
    flow.stage("source", &[], EmptyCheckpoint).unwrap();
    assert_eq!(flow.step().unwrap(), StepOutcome::Finished);
}

struct Failing {
    value: Cell<u64>,
}

struct Witness {
    value: Cell<u64>,
}

impl Operation for Failing {
    fn fingerprint(&self) -> &[u8] {
        b"failing"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        self.value.access(transaction)?.set(&7)?;
        Err(io::Error::other("broken operation").into())
    }
}

impl Operation for Witness {
    fn fingerprint(&self) -> &[u8] {
        b"witness"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        self.value.access(transaction)?.set(&11)?;
        Ok(Decision::Complete { output: None })
    }
}

#[test]
fn operation_failure_rolls_back_and_is_durable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        let value = Cell::new(flow.data("value", DataPlacement::Shared).unwrap());
        let witness = Cell::new(flow.data("witness", DataPlacement::Shared).unwrap());
        flow.stage("failing", &[], Failing { value }).unwrap();
        flow.stage("witness", &[], Witness { value: witness })
            .unwrap();
        assert!(matches!(
            flow.step(),
            Err(FlowError::StageFailed { stage, message })
                if stage == "failing" && message == "broken operation"
        ));
        assert!(matches!(
            flow.step(),
            Err(FlowError::StageFailed { stage, message })
                if stage == "failing" && message == "broken operation"
        ));
    }
    {
        let mut flow = Flow::open(&path).unwrap();
        let value = Cell::new(flow.data("value", DataPlacement::Shared).unwrap());
        let witness = Cell::new(flow.data("witness", DataPlacement::Shared).unwrap());
        flow.stage("failing", &[], Failing { value }).unwrap();
        flow.stage("witness", &[], Witness { value: witness })
            .unwrap();
        assert!(matches!(
            flow.step(),
            Err(FlowError::StageFailed { stage, message })
                if stage == "failing" && message == "broken operation"
        ));
    }

    let store = Store::open(&path).unwrap();
    let value = Cell::<u64>::new(store.open_data("value").unwrap());
    let witness = Cell::<u64>::new(store.open_data("witness").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(value.access(&transaction).unwrap().get().unwrap(), None);
    assert_eq!(witness.access(&transaction).unwrap().get().unwrap(), None);
}

struct UnconnectedOutput;

impl Operation for UnconnectedOutput {
    fn fingerprint(&self) -> &[u8] {
        b"unconnected-output"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        Ok(Decision::Complete {
            output: Some(vec![1]),
        })
    }
}

#[test]
fn a_leaf_stage_cannot_create_an_unread_output_log() {
    let directory = tempfile::tempdir().unwrap();
    let mut flow = Flow::create(directory.path().join("flow")).unwrap();
    flow.stage("leaf", &[], UnconnectedOutput).unwrap();

    assert!(matches!(
        flow.step(),
        Err(FlowError::StageFailed { stage, message })
            if stage == "leaf" && message == "an unconnected stage cannot publish output"
    ));
}

struct CheckpointCounter {
    value: Cell<u64>,
    identity: &'static [u8],
}

impl Operation for CheckpointCounter {
    fn fingerprint(&self) -> &[u8] {
        self.identity
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        let mut value = self.value.access(transaction)?;
        let next = value.get()?.unwrap_or(0) + 1;
        value.set(&next)?;
        Ok(Decision::Checkpoint {
            checkpoint: next.to_be_bytes().to_vec(),
        })
    }
}

fn fairness_flow(mut flow: Flow) -> Flow {
    let left = Cell::new(flow.data("left/count", DataPlacement::Shared).unwrap());
    let right = Cell::new(flow.data("right/count", DataPlacement::Shared).unwrap());
    flow.stage(
        "left",
        &[],
        CheckpointCounter {
            value: left,
            identity: b"left:left/count",
        },
    )
    .unwrap();
    flow.stage(
        "right",
        &[],
        CheckpointCounter {
            value: right,
            identity: b"right:right/count",
        },
    )
    .unwrap();
    flow
}

#[test]
fn scheduler_fairness_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    for turn in 0..4 {
        let flow = if turn == 0 {
            Flow::create(&path).unwrap()
        } else {
            Flow::open(&path).unwrap()
        };
        let mut flow = fairness_flow(flow);
        assert_eq!(flow.step().unwrap(), StepOutcome::Progress);
    }

    let store = Store::open(&path).unwrap();
    let left = Cell::<u64>::new(store.open_data("left/count").unwrap());
    let right = Cell::<u64>::new(store.open_data("right/count").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(left.access(&transaction).unwrap().get().unwrap(), Some(2));
    assert_eq!(right.access(&transaction).unwrap().get().unwrap(), Some(2));
}

struct CountingSource {
    calls: Cell<u64>,
}

impl Operation for CountingSource {
    fn fingerprint(&self) -> &[u8] {
        b"counting-source:calls"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        let mut calls = self.calls.access(transaction)?;
        let next = calls.get()?.unwrap_or(0) + 1;
        calls.set(&next)?;
        Ok(Decision::Publish {
            output: vec![u8::try_from(next).unwrap()],
            checkpoint: next.to_be_bytes().to_vec(),
        })
    }
}

struct PendingSink;

impl Operation for PendingSink {
    fn fingerprint(&self) -> &[u8] {
        b"pending-sink"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        Ok(Decision::Pending)
    }
}

#[test]
fn a_slow_consumer_bounds_each_upstream_to_one_block() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        let calls = Cell::new(flow.data("calls", DataPlacement::Shared).unwrap());
        let source = flow.stage("source", &[], CountingSource { calls }).unwrap();
        let sink = flow.stage("sink", &["input"], PendingSink).unwrap();
        flow.connect(source, sink, "input").unwrap();

        assert_eq!(flow.step().unwrap(), StepOutcome::Progress);
        assert_eq!(flow.step().unwrap(), StepOutcome::Idle);
        assert_eq!(flow.step().unwrap(), StepOutcome::Idle);
    }

    let store = Store::open(&path).unwrap();
    let calls = Cell::<u64>::new(store.open_data("calls").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(calls.access(&transaction).unwrap().get().unwrap(), Some(1));
}

struct OneBlock;

impl Operation for OneBlock {
    fn fingerprint(&self) -> &[u8] {
        b"one-block"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        Ok(Decision::Complete {
            output: Some(vec![7]),
        })
    }
}

struct RecordingSink {
    value: Cell<u64>,
    identity: &'static [u8],
}

impl Operation for RecordingSink {
    fn fingerprint(&self) -> &[u8] {
        self.identity
    }

    fn step(
        &mut self,
        work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        match work.event() {
            Event::Data { bytes: [value], .. } => {
                self.value.access(transaction)?.set(&u64::from(*value))?;
                Ok(Decision::Complete { output: None })
            }
            Event::End { .. } => Ok(Decision::Complete { output: None }),
            event => panic!("unexpected fan-out event {event:?}"),
        }
    }
}

fn fanout_flow(mut flow: Flow) -> Flow {
    let left_value = Cell::new(flow.data("left/value", DataPlacement::Shared).unwrap());
    let right_value = Cell::new(flow.data("right/value", DataPlacement::Shared).unwrap());
    let source = flow.stage("source", &[], OneBlock).unwrap();
    let left = flow
        .stage(
            "left",
            &["input"],
            RecordingSink {
                value: left_value,
                identity: b"left:left/value",
            },
        )
        .unwrap();
    let right = flow
        .stage(
            "right",
            &["input"],
            RecordingSink {
                value: right_value,
                identity: b"right:right/value",
            },
        )
        .unwrap();
    flow.connect(source, left, "input").unwrap();
    flow.connect(source, right, "input").unwrap();
    flow
}

#[test]
fn fanout_delivers_each_block_to_every_consumer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    let mut finished = false;
    for turn in 0..10 {
        let flow = if turn == 0 {
            Flow::create(&path).unwrap()
        } else {
            Flow::open(&path).unwrap()
        };
        let mut flow = fanout_flow(flow);
        if flow.step().unwrap() == StepOutcome::Finished {
            finished = true;
            break;
        }
    }
    assert!(finished, "fan-out flow did not finish");

    let store = Store::open(&path).unwrap();
    let left = Cell::<u64>::new(store.open_data("left/value").unwrap());
    let right = Cell::<u64>::new(store.open_data("right/value").unwrap());
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(left.access(&transaction).unwrap().get().unwrap(), Some(7));
    assert_eq!(right.access(&transaction).unwrap().get().unwrap(), Some(7));
}
