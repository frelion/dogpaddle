mod boundaries;
mod budget;
mod recovery;

use super::*;
use crate::FlowFactory;
use dogpaddle_operation::{
    col,
    operation::{
        scan::SequenceScanDefinition,
        sink::{DiscardDefinition, SqliteSinkDefinition},
        transform::{EquiJoinDefinition, EquiJoinKind, RunningEventCountDefinition},
    },
};
use dogpaddle_store::{Cell, Store};

fn one_action(flow: &mut Flow) -> AdvanceOutcome {
    try_one_action(flow).unwrap()
}
fn try_one_action(flow: &mut Flow) -> Result<AdvanceOutcome, OperationError> {
    let mut batch = flow.transactions.durability_batch();
    let mut allowance = STACK_ROUND_BYTES;
    let result = if let Some((depth, frame)) = flow
        .runtime
        .frames
        .top(flow.reads.begin().access())
        .unwrap()
    {
        flow.runtime
            .advance_frame(depth, frame, &flow.reads, &mut batch, &mut allowance)
    } else {
        flow.runtime
            .advance_next_root(&flow.reads, &mut batch, &mut allowance)
            .map(|(_, outcome)| outcome)
            .map_err(|(_, error)| error)
    };
    batch.finish().unwrap();
    result
}
fn capture(flow: &mut Flow, index: usize) {
    let mut batch = flow.transactions.durability_batch();
    assert_eq!(
        flow.runtime.capture(index, &mut batch).unwrap(),
        AdvanceOutcome::Progressed
    );
    batch.finish().unwrap();
}

fn initial_source_frame(flow: &Flow, index: usize) -> Frame {
    Frame {
        head: index,
        input_port: None,
        phase: FramePhase::Run(flow.runtime.nodes[index].operation.initial_resume()),
    }
}

fn count(path: &std::path::Path) -> Option<u64> {
    operation_count(path, 1)
}
fn operation_count(path: &std::path::Path, index: usize) -> Option<u64> {
    let store = Store::open(path).unwrap();
    let counter: Cell<u64> = store
        .open_data(&format!("operation/{index:08x}/running_event_count.count"))
        .unwrap();
    counter
        .read(store.read_transaction().access())
        .unwrap()
        .get()
        .unwrap()
}

use dogpaddle_operation::operation::{SinkOperation, SinkPending};
use dogpaddle_store::ReadTransactionAccess;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct SinkProbe {
    inner: Box<dyn SinkOperation>,
    blocked: Arc<AtomicBool>,
    loads: Arc<AtomicUsize>,
    panic_on_load: bool,
    enqueues: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
    fail_after_enqueue: bool,
}
impl SinkOperation for SinkProbe {
    fn try_enqueue(
        &mut self,
        access: TransactionAccess<'_>,
        page: &Change,
    ) -> Result<bool, OperationError> {
        self.enqueues.fetch_add(1, Ordering::Relaxed);
        if self.blocked.load(Ordering::Relaxed) {
            Ok(false)
        } else {
            let accepted = self.inner.try_enqueue(access, page)?;
            if accepted {
                self.accepted.fetch_add(1, Ordering::Relaxed);
                if self.fail_after_enqueue {
                    return Err("injected sink failure after transactional enqueue".into());
                }
            }
            Ok(accepted)
        }
    }
    fn load(
        &mut self,
        access: ReadTransactionAccess<'_>,
    ) -> Result<Option<SinkPending>, OperationError> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        assert!(!self.panic_on_load, "injected boundary panic");
        self.inner.load(access)
    }
    fn prepare_initialize(&mut self, pending: &SinkPending) -> Result<bool, OperationError> {
        self.inner.prepare_initialize(pending)
    }
    fn persist_initialize(
        &self,
        access: TransactionAccess<'_>,
        pending: &SinkPending,
    ) -> Result<(), OperationError> {
        self.inner.persist_initialize(access, pending)
    }
    fn deliver(&mut self, pending: &SinkPending) -> Result<(), OperationError> {
        self.inner.deliver(pending)
    }
    fn settle(
        &mut self,
        access: TransactionAccess<'_>,
        pending: &SinkPending,
    ) -> Result<(), OperationError> {
        self.inner.settle(access, pending)
    }
}
fn install_probe(
    flow: &mut Flow,
    blocked: bool,
    panic_on_load: bool,
) -> (Arc<AtomicBool>, Arc<AtomicUsize>) {
    let index = flow.runtime.sinks[0];
    let mut node = flow.runtime.nodes.remove(index);
    let Operation::Sink(inner) = node.operation else {
        unreachable!()
    };
    let blocked = Arc::new(AtomicBool::new(blocked));
    let loads = Arc::new(AtomicUsize::new(0));
    node.operation = Operation::Sink(Box::new(SinkProbe {
        inner,
        blocked: blocked.clone(),
        loads: loads.clone(),
        panic_on_load,
        enqueues: Arc::new(AtomicUsize::new(0)),
        accepted: Arc::new(AtomicUsize::new(0)),
        fail_after_enqueue: false,
    }));
    flow.runtime.nodes.insert(index, node);
    (blocked, loads)
}

fn install_routing_probe(
    flow: &mut Flow,
    index: usize,
    blocked: Arc<AtomicBool>,
    enqueues: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
    fail_after_enqueue: bool,
) {
    let mut node = flow.runtime.nodes.remove(index);
    let Operation::Sink(inner) = node.operation else {
        unreachable!()
    };
    node.operation = Operation::Sink(Box::new(SinkProbe {
        inner,
        blocked,
        loads: Arc::new(AtomicUsize::new(0)),
        panic_on_load: false,
        enqueues,
        accepted,
        fail_after_enqueue,
    }));
    flow.runtime.nodes.insert(index, node);
}

fn counted_flow(path: &std::path::Path) -> Flow {
    let mut factory = FlowFactory::new(path);
    let source = factory.operation("source", SequenceScanDefinition::new(0), []);
    let counter = factory.operation("count", RunningEventCountDefinition::new(), [source]);
    factory.operation("sink", DiscardDefinition::new(), [counter]);
    factory.build().unwrap()
}
