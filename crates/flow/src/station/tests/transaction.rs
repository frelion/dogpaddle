use std::{
    num::NonZeroU64,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dogpaddle_operation::operation::Action;
use dogpaddle_store::StoreError;

use crate::flow::AdvanceOutcome;

use super::{
    super::protocol::StationError,
    support::{
        FailingAtomic, ScriptResult, ScriptedOperation, change, claim_id, claim_ptr, count_change,
        duplicate_input_station, multi_input_station, poisoned_script, read_attempt,
        reopen_multi_input, scan_count_sink, scan_sink, set_result, set_script,
    },
};

#[test]
fn actions_commit_only_their_declared_state_output_and_input_effects() {
    let mut fixture = scan_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.states[1].clone();

    set_result(
        &mut fixture.stations[1],
        &state,
        b"idle",
        ScriptResult::Action(Action::Idle),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Idle);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);

    set_result(
        &mut fixture.stations[1],
        &state,
        b"error",
        ScriptResult::Error,
    );
    assert!(matches!(
        fixture.try_step(1),
        Err(StationError::Operation { operation: 0, .. })
    ));
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);

    set_script(
        &mut fixture.stations[1],
        &state,
        b"commit",
        Action::Commit(Some(count_change(&[9]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    let claim_identity = claim_ptr(&fixture.stations[1]);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"commit".as_slice())
    );
    assert_eq!(fixture.position(1, 0), 0);
    assert_eq!(fixture.bounds(1), 0..1);

    set_script(
        &mut fixture.stations[1],
        &state,
        b"rejected-commit",
        Action::Commit(Some(count_change(&[10]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Backpressured);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"commit".as_slice())
    );
    assert_eq!(claim_ptr(&fixture.stations[1]), claim_identity);

    assert_eq!(fixture.step(2), AdvanceOutcome::Progressed);
    set_script(
        &mut fixture.stations[1],
        &state,
        b"complete",
        Action::Complete(Some(count_change(&[11]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"complete".as_slice())
    );
    assert_eq!((fixture.position(1, 0), fixture.bounds(0)), (1, 1..1));
    assert_eq!(claim_id(&fixture.stations[1]), None);

    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    set_script(
        &mut fixture.stations[1],
        &state,
        b"rejected-complete",
        Action::Complete(Some(count_change(&[12]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Backpressured);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"complete".as_slice())
    );
    assert_eq!(
        (fixture.position(1, 0), fixture.bounds(0), fixture.bounds(1)),
        (1, 1..2, 1..2)
    );
    assert_eq!(claim_id(&fixture.stations[1]), Some((0, 1)));
}

#[test]
fn turn_idle_never_enters_the_transactional_body() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let state = fixture.states[0].clone();
    fixture.stations[0].replace_operation(Box::new(ScriptedOperation::idle_before_transaction(
        state.clone(),
        b"must-not-run",
    )));

    assert_eq!(fixture.step(0), AdvanceOutcome::Idle);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert_eq!(fixture.bounds(0), 0..0);
}

#[test]
fn duplicate_edges_acknowledge_independently_and_release_the_shared_entry() {
    let mut fixture = duplicate_input_station();

    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(
        (fixture.position(0), fixture.position(1), fixture.active()),
        (1, 0, 1)
    );
    assert_eq!(fixture.bounds(0), 0..1);

    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(
        (fixture.position(0), fixture.position(1), fixture.active()),
        (1, 1, 0)
    );
    assert_eq!(fixture.bounds(0), 1..1);
}

#[test]
fn after_commit_runs_once_after_each_successful_store_commit() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let state = fixture.states[0].clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.stations[0].replace_operation(Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed",
            ScriptResult::Action(Action::Commit(None)),
        )
        .with_after_commit(Arc::clone(&runs), false),
    ));

    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"committed".as_slice())
    );

    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(runs.load(Ordering::Relaxed), 2);
}

#[test]
fn backpressure_rolls_back_the_turn_and_discards_after_commit() {
    let mut fixture = scan_sink(1, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.states[0].clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.stations[0].replace_operation(Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"backpressured",
            ScriptResult::Action(Action::Commit(Some(change(&[9])))),
        )
        .with_after_commit(Arc::clone(&runs), false),
    ));

    assert_eq!(fixture.step(0), AdvanceOutcome::Backpressured);
    assert_eq!(runs.load(Ordering::Relaxed), 0);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert_eq!(fixture.bounds(0), 0..1);
}

#[test]
fn atomic_tail_failure_rolls_back_every_write_and_discards_after_commit() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let head_state = fixture.states[0].clone();
    let tail_state = fixture.states[1].clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.stations[0].replace_operation(Box::new(
        ScriptedOperation::writing(
            head_state.clone(),
            b"head",
            ScriptResult::Action(Action::Commit(Some(change(&[9])))),
        )
        .with_after_commit(Arc::clone(&runs), false),
    ));
    fixture.stations[0].replace_tail(vec![Box::new(FailingAtomic::new(
        tail_state.clone(),
        b"tail",
    ))]);

    let error = fixture.try_step(0).unwrap_err();
    assert!(matches!(
        error,
        StationError::Operation { operation: 1, .. }
    ));
    assert_eq!(runs.load(Ordering::Relaxed), 0);
    assert_eq!(read_attempt(&head_state, &mut fixture.transactions), None);
    assert_eq!(read_attempt(&tail_state, &mut fixture.transactions), None);
    assert_eq!(fixture.bounds(0), 0..0);
}

#[test]
fn commit_failure_rolls_back_and_makes_the_station_fail_stop() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let state = fixture.states[0].clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.stations[0].replace_operation(Box::new(
        poisoned_script(&state, b"must-roll-back", Action::Commit(None))
            .with_after_commit(Arc::clone(&runs), false),
    ));

    let error = fixture.try_step(0).unwrap_err();
    assert!(matches!(
        &error,
        StationError::Commit {
            source: StoreError::TransactionPoisoned
        }
    ));
    assert!(error.requires_reopen());
    assert_eq!(runs.load(Ordering::Relaxed), 0);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert!(matches!(
        fixture.try_step(0),
        Err(StationError::NeedsReopen)
    ));
}

#[test]
fn after_commit_failure_preserves_completion_and_makes_the_station_fail_stop() {
    let mut fixture = multi_input_station(Action::Idle);
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));
    let state = fixture.state.clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.station.replace_operation(Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed-before-failure",
            ScriptResult::Action(Action::Complete(None)),
        )
        .with_after_commit(Arc::clone(&runs), true),
    ));

    let error = fixture
        .station
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(matches!(error, StationError::AfterCommit { .. }));
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"committed-before-failure".as_slice())
    );
    assert_eq!((fixture.active(), fixture.position(1)), (0, 1));
    assert_eq!(fixture.bounds(1), 1..1);
    assert_eq!(claim_id(&fixture.station), None);
    assert!(matches!(
        fixture.station.process(&mut fixture.transactions),
        Err(StationError::NeedsReopen)
    ));

    fixture.append(0, &change(&[8]));
    assert_eq!(fixture.bounds(0), 0..1);
    assert!(matches!(fixture.try_step(), Err(StationError::NeedsReopen)));
    assert_eq!(fixture.position(0), 0);

    let mut reopened = reopen_multi_input(fixture, Action::Complete(None));
    assert_eq!(reopened.step(), AdvanceOutcome::Progressed);
    assert_eq!((reopened.active(), reopened.position(0)), (1, 1));
    assert_eq!(reopened.bounds(0), 1..1);
}

#[test]
fn after_commit_panic_keeps_the_station_fail_stop_until_reopen() {
    let mut fixture = multi_input_station(Action::Idle);
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    let state = fixture.state.clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.station.replace_operation(Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed-before-panic",
            ScriptResult::Action(Action::Complete(None)),
        )
        .with_panicking_after_commit(Arc::clone(&runs)),
    ));

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = fixture.station.process(&mut fixture.transactions);
    }));
    assert!(panic.is_err());
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"committed-before-panic".as_slice())
    );
    assert_eq!((fixture.active(), fixture.position(1)), (0, 1));
    assert_eq!(fixture.bounds(1), 1..1);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));
    assert!(matches!(
        fixture.station.process(&mut fixture.transactions),
        Err(StationError::NeedsReopen)
    ));

    let reopened = reopen_multi_input(fixture, Action::Complete(None));
    reopened.station.ensure_runnable().unwrap();
    assert_eq!(claim_id(&reopened.station), None);
}

#[test]
fn output_schema_mismatch_precedes_capacity_and_rolls_back() {
    let mut fixture = scan_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.states[1].clone();

    set_script(
        &mut fixture.stations[1],
        &state,
        b"valid",
        Action::Commit(Some(count_change(&[9]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    let claim = claim_ptr(&fixture.stations[1]);

    set_script(
        &mut fixture.stations[1],
        &state,
        b"wrong-schema",
        Action::Complete(Some(change(&[10]))),
    );
    assert!(matches!(
        fixture.try_step(1),
        Err(StationError::OutputSchemaMismatch { .. })
    ));
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"valid".as_slice())
    );
    assert_eq!(claim_ptr(&fixture.stations[1]), claim);
    assert_eq!(
        (fixture.position(1, 0), fixture.bounds(0), fixture.bounds(1)),
        (0, 0..1, 0..1)
    );
}

#[test]
fn protocol_errors_roll_back_operation_and_input_effects() {
    let mut sink = scan_sink(1, NonZeroU64::MAX);
    assert_eq!(sink.step(0), AdvanceOutcome::Progressed);
    let state = sink.states[1].clone();
    set_script(
        &mut sink.stations[1],
        &state,
        b"unexpected-output",
        Action::Complete(Some(change(&[9]))),
    );
    assert!(matches!(
        sink.try_step(1),
        Err(StationError::UnexpectedOutput)
    ));
    assert_eq!(read_attempt(&state, &mut sink.transactions), None);
    assert_eq!((sink.position(1, 0), sink.bounds(0)), (0, 0..1));
    assert_eq!(claim_id(&sink.stations[1]), Some((0, 0)));

    let mut scan = scan_sink(1, NonZeroU64::MAX);
    let state = scan.states[0].clone();
    set_script(
        &mut scan.stations[0],
        &state,
        b"scan-complete",
        Action::Complete(None),
    );
    assert!(matches!(
        scan.try_step(0),
        Err(StationError::OperationCompletedWithoutInput)
    ));
    assert_eq!(read_attempt(&state, &mut scan.transactions), None);
    assert_eq!(scan.bounds(0), 0..0);
}
