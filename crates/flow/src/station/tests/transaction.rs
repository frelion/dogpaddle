use std::{
    num::NonZeroU64,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dogpaddle_change::encode_change;
use dogpaddle_operation::operation::Action;
use dogpaddle_store::StoreError;

use crate::flow::AdvanceOutcome;

use super::{
    super::protocol::StationError,
    support::{
        ScriptResult, ScriptedOperation, change, claim_id, claim_ptr, count_change,
        multi_input_station, poisoned_script, read_attempt, reopen_multi_input, scan_count_sink,
        scan_sink, set_result, set_script,
    },
};

#[test]
fn action_matrix_commits_exactly_the_allowed_effects() {
    let mut fixture = scan_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.stations[1].inbox.state().clone();

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
        Err(StationError::Operation(_))
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
    assert_eq!(fixture.cursor(1, 0), 0);
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
    assert_eq!((fixture.cursor(1, 0), fixture.bounds(0)), (1, 1..1));
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
        (fixture.cursor(1, 0), fixture.bounds(0), fixture.bounds(1)),
        (1, 1..2, 1..2)
    );
    assert_eq!(claim_id(&fixture.stations[1]), Some((0, 1)));
}

#[test]
fn turn_idle_skips_the_transactional_body() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let state = fixture.stations[0].inbox.state().clone();
    fixture.stations[0].operation = Box::new(ScriptedOperation::idle_before_transaction(
        state.clone(),
        b"must-not-run",
    ));

    assert_eq!(fixture.step(0), AdvanceOutcome::Idle);
    assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    assert_eq!(fixture.bounds(0), 0..0);
}

#[test]
fn after_commit_runs_once_per_successful_store_commit() {
    let mut fixture = scan_sink(1, NonZeroU64::MAX);
    let state = fixture.stations[0].inbox.state().clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.stations[0].operation = Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed",
            ScriptResult::Action(Action::Commit(None)),
        )
        .with_after_commit(Arc::clone(&runs), false),
    );

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
fn after_commit_panic_leaves_the_station_needing_reopen() {
    let mut fixture = multi_input_station(Action::Idle);
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));
    let state = fixture.station.inbox.state().clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.station.operation = Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed-before-panic",
            ScriptResult::Action(Action::Complete(None)),
        )
        .with_panicking_after_commit(Arc::clone(&runs)),
    );

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = fixture.station.process(&mut fixture.transactions);
    }));
    assert!(panic.is_err());
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"committed-before-panic".as_slice())
    );
    assert_eq!((fixture.active(), fixture.cursor(1)), (0, 1));
    assert_eq!(fixture.bounds(1), 1..1);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));

    let error = fixture
        .station
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(error.requires_reopen());
    assert!(matches!(error, StationError::NeedsReopen));
    assert_eq!(runs.load(Ordering::Relaxed), 1);

    let reopened = reopen_multi_input(fixture, Action::Complete(None));
    reopened.station.ensure_runnable().unwrap();
    assert_eq!(claim_id(&reopened.station), None);
}

#[test]
fn after_commit_is_abandoned_for_idle_apply_error_commit_error_and_backpressure() {
    {
        let mut fixture = scan_sink(1, NonZeroU64::MAX);
        let state = fixture.stations[0].inbox.state().clone();
        let runs = Arc::new(AtomicUsize::new(0));
        fixture.stations[0].operation = Box::new(
            ScriptedOperation::writing(
                state.clone(),
                b"action-idle",
                ScriptResult::Action(Action::Idle),
            )
            .with_after_commit(Arc::clone(&runs), false),
        );

        assert_eq!(fixture.step(0), AdvanceOutcome::Idle);
        assert_eq!(runs.load(Ordering::Relaxed), 0);
        assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    }

    {
        let mut fixture = scan_sink(1, NonZeroU64::MAX);
        let state = fixture.stations[0].inbox.state().clone();
        let runs = Arc::new(AtomicUsize::new(0));
        fixture.stations[0].operation = Box::new(
            ScriptedOperation::writing(state.clone(), b"operation-error", ScriptResult::Error)
                .with_after_commit(Arc::clone(&runs), false),
        );

        assert!(matches!(
            fixture.try_step(0),
            Err(StationError::Operation(_))
        ));
        assert_eq!(runs.load(Ordering::Relaxed), 0);
        assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    }

    {
        let mut fixture = scan_sink(1, NonZeroU64::MAX);
        let state = fixture.stations[0].inbox.state().clone();
        let runs = Arc::new(AtomicUsize::new(0));
        fixture.stations[0].operation = Box::new(
            poisoned_script(&state, b"commit-error", Action::Commit(None))
                .with_after_commit(Arc::clone(&runs), false),
        );

        assert!(matches!(
            fixture.try_step(0),
            Err(StationError::Store(StoreError::TransactionPoisoned))
        ));
        assert_eq!(runs.load(Ordering::Relaxed), 0);
        assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
    }

    {
        let mut fixture = scan_sink(1, NonZeroU64::MIN);
        assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
        let state = fixture.stations[0].inbox.state().clone();
        let runs = Arc::new(AtomicUsize::new(0));
        fixture.stations[0].operation = Box::new(
            ScriptedOperation::writing(
                state.clone(),
                b"backpressured",
                ScriptResult::Action(Action::Commit(Some(change(&[9])))),
            )
            .with_after_commit(Arc::clone(&runs), false),
        );

        assert_eq!(fixture.step(0), AdvanceOutcome::Backpressured);
        assert_eq!(runs.load(Ordering::Relaxed), 0);
        assert_eq!(read_attempt(&state, &mut fixture.transactions), None);
        assert_eq!(fixture.bounds(0), 0..1);
    }
}

#[test]
fn after_commit_failure_preserves_the_commit_and_clears_the_completed_claim() {
    let mut fixture = multi_input_station(Action::Idle);
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));
    let state = fixture.station.inbox.state().clone();
    let runs = Arc::new(AtomicUsize::new(0));
    fixture.station.operation = Box::new(
        ScriptedOperation::writing(
            state.clone(),
            b"committed-before-failure",
            ScriptResult::Action(Action::Complete(None)),
        )
        .with_after_commit(Arc::clone(&runs), true),
    );

    let first_error = fixture
        .station
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(first_error.requires_reopen());
    let StationError::AfterCommit { source } = first_error else {
        panic!("first failure did not retain its after-commit source");
    };
    assert_eq!(source.to_string(), "planned after-commit failure");
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(
        read_attempt(&state, &mut fixture.transactions).as_deref(),
        Some(b"committed-before-failure".as_slice())
    );
    assert_eq!((fixture.active(), fixture.cursor(1)), (0, 1));
    assert_eq!(fixture.bounds(1), 1..1);
    assert_eq!(claim_id(&fixture.station), None);

    let process_error = fixture
        .station
        .process(&mut fixture.transactions)
        .unwrap_err();
    assert!(process_error.requires_reopen());
    assert!(matches!(process_error, StationError::NeedsReopen));

    let transaction = fixture.transactions.begin().unwrap();
    fixture.station.inbox.ports()[0]
        .output()
        .log()
        .access(transaction.access())
        .unwrap()
        .append(&encode_change(&change(&[8])).unwrap())
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(fixture.bounds(0), 0..1);

    let advance_error = fixture.try_step().unwrap_err();
    assert!(advance_error.requires_reopen());
    assert!(matches!(advance_error, StationError::NeedsReopen));
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.cursor(0), 0);
    assert_eq!(fixture.bounds(0), 0..1);
    assert_eq!(claim_id(&fixture.station), None);

    let mut reopened = reopen_multi_input(fixture, Action::Complete(None));
    assert_eq!(reopened.step(), AdvanceOutcome::Progressed);
    assert_eq!((reopened.active(), reopened.cursor(0)), (1, 1));
    assert_eq!(reopened.bounds(0), 1..1);
    assert_eq!(claim_id(&reopened.station), None);
}

#[test]
fn output_schema_mismatch_precedes_backpressure_and_rolls_back_the_turn() {
    let mut fixture = scan_count_sink(NonZeroU64::MAX, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    let state = fixture.stations[1].inbox.state().clone();

    set_script(
        &mut fixture.stations[1],
        &state,
        b"valid",
        Action::Commit(Some(count_change(&[9]))),
    );
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    let claim = claim_ptr(&fixture.stations[1]);
    assert_eq!(fixture.bounds(1), 0..1);

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
        (fixture.cursor(1, 0), fixture.bounds(0), fixture.bounds(1)),
        (0, 0..1, 0..1)
    );
}

#[test]
fn illegal_actions_roll_back_operation_state_and_claim_effects() {
    let mut sink = scan_sink(1, NonZeroU64::MAX);
    assert_eq!(sink.step(0), AdvanceOutcome::Progressed);
    let state = sink.stations[1].inbox.state().clone();
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
    assert_eq!((sink.cursor(1, 0), sink.bounds(0)), (0, 0..1));
    assert_eq!(claim_id(&sink.stations[1]), Some((0, 0)));

    let mut scan = scan_sink(1, NonZeroU64::MAX);
    let state = scan.stations[0].inbox.state().clone();
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

#[test]
fn failed_outer_commits_preserve_claim_and_roll_back_every_durable_effect() {
    let mut complete = multi_input_station(Action::Idle);
    assert!(
        complete
            .station
            .inbox
            .intake(&complete.reads, &mut complete.transactions)
            .unwrap()
    );
    let identity = claim_ptr(&complete.station);
    let state = complete.station.inbox.state().clone();
    complete.station.operation = Box::new(poisoned_script(
        &state,
        b"must-roll-back",
        Action::Complete(None),
    ));
    assert!(matches!(
        complete.station.process(&mut complete.transactions),
        Err(StationError::Store(StoreError::TransactionPoisoned))
    ));
    assert_eq!(read_attempt(&state, &mut complete.transactions), None);
    assert_eq!(
        (complete.active(), complete.cursor(1), complete.bounds(1)),
        (1, 0, 0..1)
    );
    assert_eq!(claim_ptr(&complete.station), identity);

    complete.station.operation = Box::new(ScriptedOperation::returning(Action::Complete(None)));
    assert_eq!(
        complete
            .station
            .process(&mut complete.transactions)
            .unwrap(),
        AdvanceOutcome::Progressed
    );
    assert_eq!(claim_id(&complete.station), None);
    assert_eq!((complete.active(), complete.bounds(1)), (0, 1..1));
}
