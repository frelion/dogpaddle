use dogpaddle_sql::SqlProgram;
use dogpaddle_store::{Cell, OrderedMap, Store, StoreError};

#[test]
fn physical_assembly_keeps_the_canonical_flow_definition() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         WITH numbers AS (SELECT value FROM sequence(start => 7)) \
         SELECT value AS number FROM numbers WHERE value % 2 = 0 \
         UNION ALL \
         SELECT value AS number FROM numbers WHERE value % 3 = 0",
    )
    .unwrap();

    drop(program.start(&path).unwrap());
    let definition = read_definition(&path);

    assert_eq!(definition.len(), 940);
    assert_eq!(
        blake3::hash(&definition).to_hex().as_str(),
        "94f6b81ea91d1e200dcdb9506bc2bb4ff96a846a443f3b735f5476a7374a4aa3"
    );
}

#[test]
fn outer_join_residual_is_native_and_projection_stays_in_one_transform_station() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         LEFT OUTER JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value + 1 = right_scan.value \
         AND left_scan.value = right_scan.value - 1 \
         AND left_scan.value + right_scan.value > 0",
    )
    .unwrap();

    let flow = program.start(&path).unwrap();
    let expected = [
        "sql/scan/00000000",
        "sql/scan/00000001",
        "sql/transform/00000000",
        "sql/sink",
    ];
    assert_eq!(
        flow.status()
            .unwrap()
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        expected
    );

    drop(flow);
    let store = Store::open(&path).unwrap();
    let _: OrderedMap<Vec<u8>, u64> = store
        .open_data("station/00000002/operation/00000000/equi_join.match_counts")
        .unwrap();
    assert!(matches!(
        store.open_data::<OrderedMap<Vec<u8>, u64>>(
            "station/00000002/operation/00000000/equi_join.key_counts"
        ),
        Err(StoreError::DataNotFound(_))
    ));
    drop(store);

    let reopened = program.start(&path).unwrap();
    assert_eq!(
        reopened
            .status()
            .unwrap()
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn lowering_rebinds_qualified_columns_across_self_and_nested_joins() {
    let queries = [
        "SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         JOIN sequence(start => 1) AS right_scan \
         ON right_scan.value = left_scan.value + 1",
        "WITH numbers AS (SELECT value FROM sequence(start => 0)) \
         SELECT left_numbers.value AS left_value, right_numbers.value AS right_value \
         FROM numbers AS left_numbers \
         JOIN numbers AS right_numbers \
         ON left_numbers.value + 1 = right_numbers.value \
         WHERE right_numbers.value > 0",
        "WITH numbers AS (SELECT value FROM sequence(start => 0)) \
         SELECT first.value AS first_value, second.value AS second_value, \
                third.value AS third_value \
         FROM numbers AS first \
         JOIN (numbers AS second \
               JOIN numbers AS third ON second.value = third.value) \
         ON first.value = second.value",
        "SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         RIGHT OUTER JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value + 1 = right_scan.value",
        "SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         FULL OUTER JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value = right_scan.value",
        "SELECT right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         RIGHT SEMI JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value + 1 = right_scan.value",
        "SELECT left_scan.value AS left_value \
         FROM sequence(start => 0) AS left_scan \
         LEFT ANTI JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value = right_scan.value",
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, query) in queries.into_iter().enumerate() {
        let program = SqlProgram::parse(&format!("INSERT INTO discard() {query}")).unwrap();
        program
            .start(root.path().join(format!("join-{index}")))
            .unwrap();
    }
}

#[test]
fn native_asof_join_lowers_all_directions_and_constraints() {
    let cases = [
        (">=", ""),
        (">", "ON left_scan.value = right_scan.value"),
        ("<=", "USING (value)"),
        ("<", ""),
    ];
    let root = tempfile::tempdir().unwrap();

    for (index, (comparison, constraint)) in cases.into_iter().enumerate() {
        let program = SqlProgram::parse(&format!(
            "INSERT INTO discard() \
             SELECT left_scan.value AS left_value, right_scan.value AS right_value \
             FROM sequence(start => 0) AS left_scan \
             ASOF JOIN sequence(start => 1) AS right_scan \
             MATCH_CONDITION (left_scan.value {comparison} right_scan.value) \
             {constraint}"
        ))
        .unwrap();
        let path = root.path().join(format!("asof-{index}"));
        let flow = program.start(&path).unwrap();
        assert_eq!(
            flow.status()
                .unwrap()
                .iter()
                .map(|station| station.id.as_str())
                .collect::<Vec<_>>(),
            [
                "sql/scan/00000000",
                "sql/scan/00000001",
                "sql/transform/00000000",
                "sql/sink",
            ]
        );
        drop(flow);
        program.start(&path).unwrap();
    }

    let coerced = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT left_scan.ts AS left_ts, right_scan.ts AS right_ts \
         FROM (SELECT CAST(value AS BIGINT) AS ts \
               FROM sequence(start => 0)) AS left_scan \
         ASOF JOIN (SELECT CAST(value AS INTEGER) AS ts \
                    FROM sequence(start => 1)) AS right_scan \
         MATCH_CONDITION (left_scan.ts >= right_scan.ts)",
    )
    .unwrap();
    let path = root.path().join("asof-coerced");
    drop(coerced.start(&path).unwrap());
    coerced.start(&path).unwrap();

    let partitioned = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT left_scan.ts AS left_ts, right_scan.ts AS right_ts \
         FROM (SELECT value AS ts, value % 2 AS bucket, value % 3 AS shard \
               FROM sequence(start => 0)) AS left_scan \
         ASOF JOIN (SELECT value AS ts, value % 2 AS bucket, value % 3 AS shard \
                    FROM sequence(start => 1)) AS right_scan \
         MATCH_CONDITION (left_scan.ts >= right_scan.ts) \
         ON left_scan.bucket = right_scan.bucket \
         AND left_scan.shard = right_scan.shard",
    )
    .unwrap();
    let path = root.path().join("asof-two-equalities");
    drop(partitioned.start(&path).unwrap());
    partitioned.start(&path).unwrap();
}

fn read_definition(path: &std::path::Path) -> Vec<u8> {
    let store = Store::open(path).unwrap();
    let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    definition
        .access(transaction.access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap()
}
