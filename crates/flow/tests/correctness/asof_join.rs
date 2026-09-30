use super::support::{seed_source, values_change};
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    col,
    operation::{
        scan::SequenceScanDefinition,
        sink::SqliteSinkDefinition,
        transform::{AsOfDirection, AsOfJoinDefinition, AsOfOrderKey},
    },
};

#[test]
fn historical_asof_corrections_survive_reopen_between_every_scheduling_round() {
    for direction in [
        AsOfDirection::Backward { allow_exact: true },
        AsOfDirection::Forward { allow_exact: true },
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("flow");
        let target = root.path().join("target.sqlite");
        let mut factory = FlowFactory::new(&path);
        let left = factory.operation("left", SequenceScanDefinition::new(u64::MAX), []);
        let right = factory.operation("right", SequenceScanDefinition::new(u64::MAX), []);
        let join = factory.operation(
            "asof",
            AsOfJoinDefinition::try_new(
                direction,
                [],
                AsOfOrderKey::new(col("value"), col("value")),
                ["left", "right"],
            )
            .unwrap(),
            [left, right],
        );
        factory.operation(
            "sink",
            SqliteSinkDefinition::try_new(&target, "result").unwrap(),
            [join],
        );
        drop(factory.build().unwrap());
        seed_source(&path, 0, &values_change(1..=1025, 1));
        let winner = if matches!(direction, AsOfDirection::Backward { .. }) {
            0
        } else {
            1026
        };
        seed_source(&path, 1, &values_change([winner], 1));
        let mut saw_active = false;
        for _ in 0..300 {
            let mut flow = FlowFactory::new(&path).open().unwrap();
            flow.advance().unwrap();
            saw_active |= flow.status().unwrap().depth > 0;
            drop(flow);
            if target.exists() {
                let connection = rusqlite::Connection::open(&target).unwrap();
                let count = connection
                    .query_row(
                        "SELECT count(*) FROM result WHERE \"right\" IS NOT NULL",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap();
                if count == 1025 {
                    break;
                }
            }
        }
        assert!(saw_active, "fixture must span multiple durable rounds");
        let connection = rusqlite::Connection::open(&target).unwrap();
        let rows = connection
            .prepare("SELECT \"left\",\"right\" FROM result ORDER BY \"left\"")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap()
            .map(|row| {
                let (left, right) = row.unwrap();
                (
                    u64::from_be_bytes(left.try_into().unwrap()),
                    u64::from_be_bytes(right.try_into().unwrap()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            (1..=1025).map(|left| (left, winner)).collect::<Vec<_>>()
        );
    }
}
