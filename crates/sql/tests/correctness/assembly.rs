use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory};
use dogpaddle_operation::operation::{scan::SequenceScanDefinition, sink::DiscardDefinition};
use dogpaddle_sql::{SqlError, SqlProgram};
use dogpaddle_store::{Cell, OrderedMap, Queue, ScanDirection, ScanLimit, Store, StoreError};

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

    // The sole Flow JSON fixes the current assembly owner, nodes and input ordinals.
    assert_eq!(
        (
            definition.len(),
            blake3::hash(&definition).to_hex().to_string(),
        ),
        (
            1143,
            "a7e67982804701b94f57b759a5f4dea335f1171b910827f0206f1d5dcd8884ef".to_owned(),
        )
    );
}

#[test]
fn prior_assembly_identity_is_rejected_without_replacing_durable_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    // Prior SQL identity for the exact program below, before join projection reuse.
    let old_identity =
        *blake3::Hash::from_hex("ed9c0edd4c9c98b9a0463ca28820988038fd0f97b4c44cc3410fb1bb211199d8")
            .unwrap()
            .as_bytes();
    let mut factory = FlowFactory::new(&path);
    factory.owner_identity(old_identity);
    let source = factory.operation("sequence", SequenceScanDefinition::new(7), []);
    factory.operation("discard", DiscardDefinition::new(), [source]);
    let mut flow = factory.build().unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);
    let definition = read_definition(&path);

    let program = SqlProgram::parse(
        "INSERT INTO discard() SELECT value FROM sequence(start => 7) WHERE value > 10",
    )
    .unwrap();
    assert!(matches!(
        program.start(&path),
        Err(SqlError::Flow(FlowError::OwnerIdentityMismatch))
    ));
    assert_eq!(read_definition(&path), definition);

    {
        let store = Store::open(&path).unwrap();
        // Rebind every resource in the original five-entry typed catalog.
        let _: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
        let frames: OrderedMap<u32, Vec<u8>> = store.open_data("flow/frames").unwrap();
        let outputs: OrderedMap<u32, Vec<u8>> = store.open_data("flow/outputs").unwrap();
        let position: Cell<u64> = store
            .open_data("operation/00000000/sequence_scan.position")
            .unwrap();
        let published: Queue<Vec<u8>> = store
            .open_data("operation/00000000/sequence_scan.published")
            .unwrap();
        let transaction = store.read_transaction();
        assert_eq!(
            position.read(transaction.access()).unwrap().get().unwrap(),
            Some(7)
        );
        assert_eq!(
            published
                .read(transaction.access())
                .unwrap()
                .queued_bytes()
                .unwrap(),
            0
        );
        for map in [frames, outputs] {
            let page = map
                .read(transaction.access())
                .unwrap()
                .scan(
                    ..,
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(1, 64 * 1024).unwrap(),
                )
                .unwrap();
            assert!(page.entries.is_empty());
            assert!(page.continuation.is_none());
        }
    }
    let mut factory = FlowFactory::new(&path);
    factory.owner_identity(old_identity);
    let reopened = factory.open().unwrap();
    assert_eq!(
        reopened.operation_ids().collect::<Vec<_>>(),
        ["sequence", "discard"]
    );
}

#[test]
fn outer_join_residual_is_native_and_projection_retains_logical_identity() {
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
        "sql/transform/00000001",
        "sql/sink",
    ];
    assert_eq!(flow.operation_ids().collect::<Vec<_>>(), expected);

    drop(flow);
    let store = Store::open(&path).unwrap();
    let _: OrderedMap<Vec<u8>, u64> = store
        .open_data("operation/00000002/equi_join.match_counts")
        .unwrap();
    assert!(matches!(
        store.open_data::<OrderedMap<Vec<u8>, u64>>("operation/00000002/equi_join.key_counts"),
        Err(StoreError::DataNotFound(_))
    ));
    drop(store);

    let reopened = program.start(&path).unwrap();
    assert_eq!(reopened.operation_ids().collect::<Vec<_>>(), expected);
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
            flow.operation_ids().collect::<Vec<_>>(),
            [
                "sql/scan/00000000",
                "sql/scan/00000001",
                "sql/transform/00000000",
                "sql/transform/00000001",
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
