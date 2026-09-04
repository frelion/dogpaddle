use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dogpaddle_operation::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
    sink::DiscardDefinition, source::SequenceSourceDefinition,
    transform::RunningEventCountDefinition,
};
use dogpaddle_store::{AppendLog, Cell, Store};

use crate::{build::FlowFactory, error::FlowRunError, station::StationError};

struct FailingAfterCommit {
    runs: Arc<AtomicUsize>,
}

impl Operation for FailingAfterCommit {
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
    let error = FlowRunError::new("source", StationError::UnexpectedOutput);
    assert!(!error.requires_reopen());
}

#[test]
fn build_and_open_derive_a_stable_layered_topological_schedule() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_target = builder.station("first-target", RunningEventCountDefinition::new());
    let second_target = builder.station("second-target", RunningEventCountDefinition::new());
    let second_source = builder.station("second-source", SequenceSourceDefinition::new(0));
    let first_source = builder.station("first-source", SequenceSourceDefinition::new(0));
    let first_sink = builder.station("first-sink", DiscardDefinition::new());
    let second_sink = builder.station("second-sink", DiscardDefinition::new());
    for station in [first_target, second_target, second_source, first_source] {
        builder.output_capacity_bytes(station, NonZeroU64::MAX);
    }
    builder.connect([first_source], first_target);
    builder.connect([second_source], second_target);
    builder.connect([first_target], first_sink);
    builder.connect([second_target], second_sink);

    let flow = builder.build().unwrap();
    assert_eq!(flow.topology.schedule, [2, 3, 0, 1, 4, 5]);
    drop(flow);

    let reopened = FlowFactory::open(path).unwrap();
    assert_eq!(reopened.topology.schedule, [2, 3, 0, 1, 4, 5]);
}

#[test]
fn reopen_reinstates_each_output_capacity_and_does_not_short_circuit_backpressure() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let blocked_source = builder.station("blocked-source", SequenceSourceDefinition::new(0));
    let progressing_source =
        builder.station("progressing-source", SequenceSourceDefinition::new(0));
    let blocked_sink = builder.station("blocked-sink", DiscardDefinition::new());
    let progressing_sink = builder.station("progressing-sink", DiscardDefinition::new());
    builder.output_capacity_bytes(blocked_source, NonZeroU64::new(1).unwrap());
    builder.output_capacity_bytes(progressing_source, NonZeroU64::MAX);
    builder.connect([blocked_source], blocked_sink);
    builder.connect([progressing_source], progressing_sink);
    let mut flow = builder.build().unwrap();
    flow.topology.schedule = vec![0, 1];
    assert_eq!(flow.advance().unwrap(), super::AdvanceOutcome::Progressed);
    drop(flow);

    let mut reopened = FlowFactory::open(&path).unwrap();
    reopened.topology.schedule = vec![0, 1];
    assert_eq!(
        reopened.advance().unwrap(),
        super::AdvanceOutcome::Progressed
    );
    reopened.topology.schedule = vec![0];
    assert_eq!(
        reopened.advance().unwrap(),
        super::AdvanceOutcome::Backpressured
    );
    drop(reopened);

    let store = Store::open(path).unwrap();
    let blocked_position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let progressing_position: Cell<u64> = store
        .open_data("station/00000001/operation/sequence_source.position")
        .unwrap();
    let blocked_output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let progressing_output: AppendLog<Vec<u8>> =
        store.open_data("station/00000001/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        (
            blocked_position
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            blocked_output
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
            progressing_position
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            progressing_output
                .access(transaction.access())
                .unwrap()
                .bounds()
                .unwrap(),
        ),
        (Some(0), 0..1, Some(1), 0..2)
    );
}

#[test]
fn advance_preflights_every_station_before_earlier_stations_can_commit() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut builder = FlowFactory::new(&path);
    let first_source = builder.station("first-source", SequenceSourceDefinition::new(0));
    let first_sink = builder.station("first-sink", DiscardDefinition::new());
    let failed_source = builder.station("failed-source", SequenceSourceDefinition::new(0));
    let failed_sink = builder.station("failed-sink", DiscardDefinition::new());
    builder.output_capacity_bytes(first_source, NonZeroU64::MAX);
    builder.output_capacity_bytes(failed_source, NonZeroU64::MAX);
    builder.connect([first_source], first_sink);
    builder.connect([failed_source], failed_sink);
    let mut flow = builder.build().unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    flow.stations[2].replace_operation(Box::new(FailingAfterCommit {
        runs: Arc::clone(&runs),
    }));

    let first_error = flow.advance().unwrap_err();
    assert_eq!(first_error.station_id(), "failed-source");
    assert!(first_error.requires_reopen());
    assert_eq!(runs.load(Ordering::Relaxed), 1);

    let preflight_error = flow.advance().unwrap_err();
    assert_eq!(preflight_error.station_id(), "failed-source");
    assert!(preflight_error.requires_reopen());
    assert!(
        preflight_error
            .to_string()
            .contains("station must be reopened after a post-commit failure")
    );
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    drop(flow);

    let store = Store::open(path).unwrap();
    let first_position: Cell<u64> = store
        .open_data("station/00000000/operation/sequence_source.position")
        .unwrap();
    let first_output: AppendLog<Vec<u8>> = store.open_data("station/00000000/output").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        first_position
            .access(transaction.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        first_output
            .access(transaction.access())
            .unwrap()
            .bounds()
            .unwrap(),
        0..1
    );
}
