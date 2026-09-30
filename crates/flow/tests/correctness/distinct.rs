use super::support::{run_until_idle, seed_source, values_change};
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::{
    scan::SequenceScanDefinition, sink::SqliteSinkDefinition, transform::DistinctDefinition,
};

#[test]
fn distinct_preserves_bag_presence_across_pages_retractions_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let target = root.path().join("target.sqlite");
    let mut factory = FlowFactory::new(&path);
    let source = factory.operation("source", SequenceScanDefinition::new(u64::MAX), []);
    let distinct = factory.operation("distinct", DistinctDefinition::new(), [source]);
    factory.operation(
        "sink",
        SqliteSinkDefinition::try_new(&target, "result").unwrap(),
        [distinct],
    );
    drop(factory.build().unwrap());
    for (rows, diff, expected) in [(513, 1, 1), (512, -1, 1), (1, -1, 0), (1, 1, 1)] {
        seed_source(&path, 0, &values_change(std::iter::repeat_n(7, rows), diff));
        let mut flow = FlowFactory::new(&path).open().unwrap();
        flow.advance().unwrap();
        drop(flow);
        let mut flow = FlowFactory::new(&path).open().unwrap();
        run_until_idle(&mut flow);
        drop(flow);
        let connection = rusqlite::Connection::open(&target).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM result", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            expected
        );
    }
}
