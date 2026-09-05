use std::num::{NonZeroU64, NonZeroUsize};

use crate::flow::AdvanceOutcome;

use super::{
    super::{CompletionPlan, plan_complete},
    support::{cursor_vectors, duplicate_input_station, scan_sink},
};

#[test]
fn completion_planner_matches_every_small_reachable_transition() {
    for input_count in 1..=3 {
        for claim_port in 0..input_count {
            for consumer_count in 1..=3 {
                for tail in 1..=3 {
                    for cursors in cursor_vectors(consumer_count, tail) {
                        let head = *cursors.iter().min().unwrap();
                        for consumer_slot in 0..consumer_count {
                            let claim_offset = cursors[consumer_slot];
                            if claim_offset == u64::try_from(tail).unwrap() {
                                continue;
                            }
                            let actual = plan_complete(
                                claim_port,
                                claim_offset,
                                claim_port,
                                NonZeroUsize::new(input_count).unwrap(),
                                consumer_slot,
                                head..u64::try_from(tail).unwrap(),
                                &cursors,
                            )
                            .unwrap();
                            let mut updated = cursors.clone();
                            updated[consumer_slot] += 1;
                            let next_head = *updated.iter().min().unwrap();
                            assert_eq!(
                                actual,
                                CompletionPlan {
                                    next_cursor: claim_offset + 1,
                                    next_active: (claim_port + 1) % input_count,
                                    reclaim_to: (next_head != head).then_some(next_head),
                                }
                            );
                            assert!(next_head == head || next_head == head + 1);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn completion_planner_rejects_each_invalid_durable_fact() {
    #[rustfmt::skip]
    let cases = [
        ("active", 0, 0, 1, 2, 0, 0..1, vec![0], "claimed input port 0 does not match durable active input port 1"),
        ("claim", 0, 1, 0, 1, 0, 0..2, vec![0], "claimed input offset 1 does not match durable consumer cursor 0"),
        ("tail", 0, 1, 0, 1, 0, 1..1, vec![1], "claimed input offset 1 is at output tail 1"),
        ("range", 0, 0, 0, 1, 0, 1..2, vec![0], "output consumer 0 cursor 0 is outside retained range [1, 2]"),
        ("head", 0, 1, 0, 1, 0, 0..2, vec![1], "output retention head 0 does not equal minimum consumer cursor 1"),
    ];
    for (name, port, offset, active, inputs, slot, bounds, cursors, expected) in cases {
        assert_eq!(
            plan_complete(
                port,
                offset,
                active,
                NonZeroUsize::new(inputs).unwrap(),
                slot,
                bounds,
                &cursors,
            )
            .unwrap_err()
            .to_string(),
            expected,
            "case {name}"
        );
    }
}

#[test]
fn duplicate_edges_share_one_output_but_acknowledge_independently() {
    let mut fixture = duplicate_input_station();
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!((fixture.cursor(0), fixture.cursor(1)), (1, 0));
    assert_eq!((fixture.active(), fixture.bounds(0)), (1, 0..1));
    assert_eq!(fixture.step(), AdvanceOutcome::Progressed);
    assert_eq!((fixture.cursor(1), fixture.active()), (1, 0));
    assert_eq!(fixture.bounds(0), 1..1);
}

#[test]
fn completing_an_oversize_entry_restores_empty_log_admission() {
    let mut fixture = scan_sink(1, NonZeroU64::MIN);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(fixture.step(0), AdvanceOutcome::Backpressured);
    assert_eq!(fixture.bounds(0), 0..1);
    assert_eq!(fixture.step(1), AdvanceOutcome::Progressed);
    assert_eq!(fixture.bounds(0), 1..1);
    assert_eq!(fixture.step(0), AdvanceOutcome::Progressed);
    assert_eq!(fixture.bounds(0), 1..2);
}
