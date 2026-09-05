use std::num::NonZeroU64;

use dogpaddle_flow::{AdvanceOutcome, FlowFactory};
use dogpaddle_operation::operation::{
    sink::SqliteSinkDefinition, source::SequenceSourceDefinition,
    transform::RunningEventCountDefinition,
};

#[test]
fn status_observes_backpressure_without_advancing_and_preserves_durable_counters_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let sqlite = root.path().join("sink.sqlite");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX - 3));
    let count = factory.station("count", RunningEventCountDefinition::new());
    let sink = factory.station(
        "sink",
        SqliteSinkDefinition::try_new(&sqlite, "events").unwrap(),
    );
    factory.connect([source], count);
    factory.connect([count], sink);
    for station in [source, count] {
        factory.output_capacity_bytes(station, NonZeroU64::MIN);
    }
    let mut flow = factory.build().unwrap();
    let initial = flow.status().unwrap();
    assert_eq!(
        initial
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        ["source", "count", "sink"]
    );
    assert!(
        initial
            .iter()
            .all(|station| station.last_outcome.is_none() && !station.needs_reopen)
    );
    assert!(initial[0].inputs.is_empty());
    assert_eq!(initial[0].active_input, None);
    assert_eq!(initial[1].active_input, Some(0));
    assert!(initial[2].output.is_none());
    assert_eq!(flow.status().unwrap(), initial);
    assert!(
        !sqlite.exists(),
        "status must not initialize external resources"
    );

    let mut saw_pressure_with_progress = false;
    for _ in 0..64 {
        let outcome = flow.advance().unwrap();
        let statuses = flow.status().unwrap();
        assert_eq!(flow.status().unwrap(), statuses);
        for (producer, consumer) in [(0, 1), (1, 2)] {
            let output = statuses[producer].output.as_ref().unwrap();
            let input = &statuses[consumer].inputs[0];
            assert_eq!(output.head, input.cursor);
            assert_eq!(output.tail, input.tail);
            assert_eq!(output.capacity_bytes, 1);
            assert!(output.head <= output.tail);
            if output.head == output.tail {
                assert_eq!(output.retained_bytes, 0);
            } else {
                assert!(output.retained_bytes > output.capacity_bytes);
            }
        }
        if outcome == AdvanceOutcome::Progressed
            && statuses
                .iter()
                .any(|station| station.last_outcome == Some(AdvanceOutcome::Backpressured))
        {
            saw_pressure_with_progress = true;
            let mut durable = statuses;
            for station in &mut durable {
                station.last_outcome = None;
            }
            drop(flow);
            flow = FlowFactory::new(&path).open().unwrap();
            assert_eq!(flow.status().unwrap(), durable);
        }
        if outcome == AdvanceOutcome::Idle {
            break;
        }
    }
    assert!(
        saw_pressure_with_progress,
        "progress must not hide the pressured Station"
    );
    assert!(flow.status().unwrap().iter().all(|station| {
        station
            .inputs
            .iter()
            .all(|input| input.cursor == input.tail)
    }));
}
