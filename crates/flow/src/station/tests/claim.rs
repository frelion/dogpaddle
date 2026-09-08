use dogpaddle_operation::operation::Action;

use crate::flow::AdvanceOutcome;

use super::{
    super::protocol::StationError,
    support::{
        ScriptedOperation, change, claim_bytes, claim_id, claim_ptr, count_change, count_schema,
        multi_input_station, raw_station_with_change_and_schemas, reopen_multi_input, value_schema,
    },
};

#[test]
fn claim_is_owned_idempotent_and_survives_commit_cache_loss_and_reopen() {
    let mut pinned = multi_input_station(Action::Idle);
    assert_eq!(pinned.step(), AdvanceOutcome::Progressed);
    assert_eq!(claim_id(&pinned.station), Some((1, 0)));
    let encoded = claim_bytes(&pinned.station);
    let memory_identity = claim_ptr(&pinned.station);
    assert!(
        !pinned
            .station
            .inbox
            .intake(&pinned.reads, &mut pinned.transactions)
            .unwrap()
    );
    assert_eq!(claim_ptr(&pinned.station), memory_identity);

    pinned
        .station
        .replace_operation(Box::new(ScriptedOperation::returning(Action::Commit(None))));
    assert_eq!(pinned.step(), AdvanceOutcome::Progressed);
    assert_eq!((pinned.active(), pinned.position(1)), (1, 0));
    assert_eq!(claim_ptr(&pinned.station), memory_identity);

    pinned.station.inbox.clear_cached_claim();
    assert!(
        !pinned
            .station
            .inbox
            .intake(&pinned.reads, &mut pinned.transactions)
            .unwrap()
    );
    assert_eq!(claim_id(&pinned.station), Some((1, 0)));
    assert_eq!(claim_bytes(&pinned.station), encoded);

    let mut reopened = reopen_multi_input(pinned, Action::Idle);
    assert_eq!((reopened.active(), reopened.position(1)), (1, 0));
    assert_eq!(claim_id(&reopened.station), None);
    assert_eq!(reopened.step(), AdvanceOutcome::Idle);
    assert_eq!(claim_id(&reopened.station), Some((1, 0)));
    assert_eq!(claim_bytes(&reopened.station), encoded);
}

#[test]
fn intake_checks_the_selected_outputs_schema_before_durable_pin() {
    let schemas = [value_schema(), count_schema()];
    let mut valid = raw_station_with_change_and_schemas(
        &[0, 1],
        &[1],
        Action::Idle,
        &count_change(&[7]),
        &schemas,
    );
    assert_eq!(valid.step(), AdvanceOutcome::Progressed);
    assert_eq!((valid.active(), valid.position(1)), (1, 0));
    assert_eq!(claim_id(&valid.station), Some((1, 0)));

    let mut invalid = raw_station_with_change_and_schemas(
        &[0, 1],
        &[1],
        Action::Complete(None),
        &change(&[7]),
        &schemas,
    );
    assert!(matches!(
        invalid.try_step(),
        Err(StationError::InputSchemaMismatch { input: 1, .. })
    ));
    assert_eq!((invalid.active(), invalid.position(1)), (0, 0));
    assert_eq!(invalid.bounds(1), 0..1);
    assert_eq!(claim_id(&invalid.station), None);
}
