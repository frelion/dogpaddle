use dogpaddle_operation::operation::Action;

use crate::flow::AdvanceOutcome;

use super::{
    super::{ACTIVE_INPUT_KEY, cursor_key, encode_active_input, protocol::StationError},
    support::{
        ScriptedOperation, change, claim_bytes, claim_id, claim_ptr, count_change, count_schema,
        multi_input_station, raw_station_with_change_and_schemas, read_state, reopen_multi_input,
        value_schema,
    },
};

#[test]
fn claim_trace_preserves_durable_identity_across_commit_pin_cache_loss_and_reopen() {
    let mut pinned = multi_input_station(Action::Idle);
    assert_eq!(pinned.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&pinned.station), Some((1, 0)));
    let memory_identity = claim_ptr(&pinned.station);
    let encoded = claim_bytes(&pinned.station);
    assert!(
        !pinned
            .station
            .inbox
            .intake(&pinned.reads, &mut pinned.transactions)
            .unwrap()
    );
    assert_eq!(claim_ptr(&pinned.station), memory_identity);

    let mut reopened = reopen_multi_input(pinned, Action::Idle);
    assert_eq!((reopened.active(), reopened.cursor(1)), (1, 0));
    assert_eq!(reopened.step(), AdvanceOutcome::Idle);
    assert_eq!(claim_id(&reopened.station), Some((1, 0)));
    assert_eq!(claim_bytes(&reopened.station), encoded);
    let replay_identity = claim_ptr(&reopened.station);
    reopened.station.operation = Box::new(ScriptedOperation::returning(Action::Commit(None)));
    assert_eq!(reopened.step(), AdvanceOutcome::Progressed);
    assert_eq!((reopened.active(), reopened.cursor(1)), (1, 0));
    assert_eq!(claim_ptr(&reopened.station), replay_identity);

    reopened.station.inbox.clear_cached_claim();
    assert!(
        !reopened
            .station
            .inbox
            .intake(&reopened.reads, &mut reopened.transactions)
            .unwrap()
    );
    assert_eq!(claim_id(&reopened.station), Some((1, 0)));
    assert_eq!(claim_bytes(&reopened.station), encoded);
}

#[test]
fn input_schema_uses_the_selected_ports_distinct_output_schema() {
    let schemas = [value_schema(), count_schema()];
    let mut fixture = raw_station_with_change_and_schemas(
        &[0, 1],
        &[1],
        Action::Idle,
        &count_change(&[7]),
        &schemas,
    );
    assert_eq!(fixture.active(), 0);

    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!(fixture.active(), 1);
    assert_eq!(fixture.cursor(1), 0);
    assert_eq!(fixture.bounds(1), 0..1);
    assert_eq!(claim_id(&fixture.station), Some((1, 0)));
}

#[test]
fn input_schema_mismatch_does_not_pin_or_install_a_claim_or_advance_the_cursor() {
    let schemas = [value_schema(), count_schema()];
    let mut fixture = raw_station_with_change_and_schemas(
        &[0, 1],
        &[1],
        Action::Complete(None),
        &change(&[7]),
        &schemas,
    );
    assert_eq!(fixture.active(), 0);

    assert!(matches!(
        fixture.try_step(),
        Err(StationError::InputSchemaMismatch { input: 1, .. })
    ));
    assert_eq!(fixture.active(), 0);
    assert_eq!(fixture.cursor(1), 0);
    assert_eq!(fixture.bounds(1), 0..1);
    assert_eq!(claim_id(&fixture.station), None);
}

#[test]
fn complete_rejects_a_post_claim_active_mismatch_without_effects() {
    let mut fixture = multi_input_station(Action::Complete(None));
    assert!(
        fixture
            .station
            .inbox
            .intake(&fixture.reads, &mut fixture.transactions)
            .unwrap()
    );
    let claim = claim_ptr(&fixture.station);
    let transaction = fixture.transactions.begin().unwrap();
    let mut state = fixture
        .station
        .inbox
        .state()
        .access(transaction.access())
        .unwrap();
    state
        .put(&ACTIVE_INPUT_KEY.to_vec(), &encode_active_input(0).to_vec())
        .unwrap();
    assert!(state.remove(&cursor_key(1)).unwrap());
    transaction.commit().unwrap();

    assert!(matches!(
        fixture.station.process(&mut fixture.transactions),
        Err(StationError::ClaimActiveInputMismatch {
            claimed: 1,
            durable: 0
        })
    ));
    assert_eq!((fixture.active(), fixture.bounds(1)), (0, 0..1));
    assert_eq!(
        read_state(
            fixture.station.inbox.state(),
            &mut fixture.transactions,
            &cursor_key(1)
        ),
        None
    );
    assert_eq!(claim_ptr(&fixture.station), claim);
}
