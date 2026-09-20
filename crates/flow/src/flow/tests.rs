use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dogpaddle_operation::operation::{
    Action, AfterCommit, OperationError, OperationInput, PostCommitError, Turn, TurnOperation,
    scan::SequenceScanDefinition, sink::DiscardDefinition, transform::RunningEventCountDefinition,
};
use dogpaddle_store::{Cell, Store, SubscribedLog};

use crate::{build::FlowFactory, error::FlowRunError, station::StationError};

struct FailingAfterCommit {
    runs: Arc<AtomicUsize>,
}

impl TurnOperation for FailingAfterCommit {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        assert!(input.is_none());
        let runs = Arc::clone(&self.runs);
        Ok(Turn::ready(move |_access| {
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    runs.fetch_add(1, Ordering::Relaxed);
                    Err(PostCommitError::new(std::io::Error::other(
                        "planned after-commit failure",
                    )))
                }),
            ))
        }))
    }
}

#[test]
fn precommit_flow_errors_do_not_require_reopen() {
    let error = FlowRunError::new("scan", StationError::UnexpectedOutput);
    assert!(!error.requires_reopen());
}

#[test]
fn build_and_open_derive_a_stable_layered_topological_schedule() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let second_scan =
        builder.operation("second-scan", Box::new(SequenceScanDefinition::new(0)), []);
    let first_scan = builder.operation("first-scan", Box::new(SequenceScanDefinition::new(0)), []);
    let first_target = builder.operation(
        "first-target",
        Box::new(RunningEventCountDefinition::new()),
        [first_scan],
    );
    builder.operation(
        "first-sink",
        Box::new(DiscardDefinition::new()),
        [first_target],
    );
    let second_target = builder.operation(
        "second-target",
        Box::new(RunningEventCountDefinition::new()),
        [second_scan],
    );

    builder.operation(
        "second-sink",
        Box::new(DiscardDefinition::new()),
        [second_target],
    );
    for station in [first_target, second_target, second_scan, first_scan] {
        builder.materialize(station, NonZeroU64::MAX);
    }

    let flow = builder.build().unwrap();
    assert_eq!(flow.schedule, [0, 1, 2, 4, 3, 5]);
    drop(flow);

    let reopened = FlowFactory::new(path).open().unwrap();
    assert_eq!(reopened.schedule, [0, 1, 2, 4, 3, 5]);
}

#[test]
fn reopen_reinstates_each_output_capacity_and_does_not_short_circuit_backpressure() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let blocked_scan =
        builder.operation("blocked-scan", Box::new(SequenceScanDefinition::new(0)), []);
    let progressing_scan = builder.operation(
        "progressing-scan",
        Box::new(SequenceScanDefinition::new(0)),
        [],
    );
    builder.operation(
        "blocked-sink",
        Box::new(DiscardDefinition::new()),
        [blocked_scan],
    );
    builder.operation(
        "progressing-sink",
        Box::new(DiscardDefinition::new()),
        [progressing_scan],
    );
    builder.materialize(blocked_scan, NonZeroU64::new(1).unwrap());
    builder.materialize(progressing_scan, NonZeroU64::MAX);

    let mut flow = builder.build().unwrap();
    flow.schedule = vec![0, 1];
    assert_eq!(flow.advance().unwrap(), super::AdvanceOutcome::Progressed);
    drop(flow);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    reopened.schedule = vec![0, 1];
    assert_eq!(
        reopened.advance().unwrap(),
        super::AdvanceOutcome::Progressed
    );
    reopened.schedule = vec![0];
    assert_eq!(
        reopened.advance().unwrap(),
        super::AdvanceOutcome::Backpressured
    );
    drop(reopened);

    let store = Store::open(path).unwrap();
    let blocked_position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let progressing_position: Cell<u64> = store
        .open_data("station/00000001/operation/00000000/sequence_scan.position")
        .unwrap();
    let blocked_output: SubscribedLog<Vec<u8>> =
        store.open_data("station/00000000/output").unwrap();
    let progressing_output: SubscribedLog<Vec<u8>> =
        store.open_data("station/00000001/output").unwrap();
    let transaction = store.read_transaction();
    let blocked_output = blocked_output
        .writer()
        .status(transaction.access())
        .unwrap();
    let progressing_output = progressing_output
        .writer()
        .status(transaction.access())
        .unwrap();
    assert_eq!(
        (
            blocked_position
                .read(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            (blocked_output.head, blocked_output.tail),
            progressing_position
                .read(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            (progressing_output.head, progressing_output.tail),
        ),
        (Some(0), (0, 1), Some(1), (0, 2))
    );
    assert!(blocked_output.retained_bytes > 0);
    assert!(progressing_output.retained_bytes > blocked_output.retained_bytes);
}

#[test]
fn fanout_retains_output_until_the_slowest_subscription_completes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let scan = builder.operation("scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation("first-sink", Box::new(DiscardDefinition::new()), [scan]);
    builder.operation("slow-sink", Box::new(DiscardDefinition::new()), [scan]);
    builder.materialize(scan, NonZeroU64::MAX);

    let mut flow = builder.build().unwrap();

    flow.schedule = vec![0, 1];
    assert_eq!(flow.advance().unwrap(), super::AdvanceOutcome::Progressed);
    let pending = flow.status().unwrap();
    let output = pending[0].output.as_ref().unwrap();
    assert_eq!((output.head, output.tail), (0, 1));
    assert!(output.retained_bytes > 0);
    assert_eq!(
        (pending[1].inputs[0].position, pending[2].inputs[0].position),
        (1, 0)
    );
    drop(flow);

    let mut reopened = FlowFactory::new(&path).open().unwrap();
    let pending = reopened.status().unwrap();
    assert_eq!(
        (
            pending[0].output.as_ref().unwrap().head,
            pending[2].inputs[0].position
        ),
        (0, 0)
    );
    reopened.schedule = vec![2];
    assert_eq!(
        reopened.advance().unwrap(),
        super::AdvanceOutcome::Progressed
    );
    let caught_up = reopened.status().unwrap();
    let output = caught_up[0].output.as_ref().unwrap();
    assert_eq!((output.head, output.tail, output.retained_bytes), (1, 1, 0));
    assert_eq!(caught_up[2].inputs[0].position, 1);
}

#[test]
fn advance_preflights_every_station_before_earlier_stations_can_commit() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_scan = builder.operation("first-scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation(
        "first-sink",
        Box::new(DiscardDefinition::new()),
        [first_scan],
    );
    let failed_scan =
        builder.operation("failed-scan", Box::new(SequenceScanDefinition::new(0)), []);
    builder.operation(
        "failed-sink",
        Box::new(DiscardDefinition::new()),
        [failed_scan],
    );
    builder.materialize(first_scan, NonZeroU64::MAX);
    builder.materialize(failed_scan, NonZeroU64::MAX);

    let mut flow = builder.build().unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    flow.stations[2].replace_operation(Box::new(FailingAfterCommit {
        runs: Arc::clone(&runs),
    }));

    let first_error = flow.advance().unwrap_err();
    assert_eq!(first_error.station_id(), "failed-scan");
    assert!(first_error.requires_reopen());
    assert_eq!(runs.load(Ordering::Relaxed), 1);

    let statuses = flow.status().unwrap();
    assert!(statuses[2].needs_reopen);
    assert!(statuses[2].last_outcome.is_none());
    assert!(
        statuses[1].last_outcome.is_none(),
        "later Stations were not visited"
    );
    assert_eq!(statuses[0].output.as_ref().unwrap().tail, 1);

    let preflight_error = flow.advance().unwrap_err();
    assert_eq!(preflight_error.station_id(), "failed-scan");
    assert!(preflight_error.requires_reopen());
    assert!(
        preflight_error
            .to_string()
            .contains("station must be reopened after an uncertain commit or post-commit failure")
    );
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert!(
        flow.status()
            .unwrap()
            .iter()
            .all(|station| station.last_outcome.is_none())
    );
    drop(flow);

    let store = Store::open(path).unwrap();
    let first_position: Cell<u64> = store
        .open_data("station/00000000/operation/00000000/sequence_scan.position")
        .unwrap();
    let first_output: SubscribedLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let transaction = store.read_transaction();
    assert_eq!(
        first_position
            .read(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(0)
    );
    let output = first_output.writer().status(transaction.access()).unwrap();
    assert_eq!((output.head, output.tail), (0, 1));
}
