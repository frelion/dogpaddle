use dogpaddle_sql::SqlProgram;
use dogpaddle_store::{Cell, Store};

#[test]
fn physical_compiler_keeps_the_canonical_flow_definition() {
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

    drop(program.build(&path).unwrap());
    let definition = read_definition(&path);

    assert_eq!(definition.len(), 907);
    assert_eq!(
        blake3::hash(&definition).to_hex().as_str(),
        "7252265dfc5dedc3ef43bd53652d751d2d473e9782159671835531ef69597559"
    );
}

#[test]
fn inner_join_is_a_transform_head_with_an_atomic_projection_tail() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         INNER JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value + 1 = right_scan.value \
         AND left_scan.value = right_scan.value - 1",
    )
    .unwrap();

    let flow = program.build(&path).unwrap();
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
    let reopened = program.open(&path).unwrap();
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
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, query) in queries.into_iter().enumerate() {
        let program = SqlProgram::parse(&format!("INSERT INTO discard() {query}")).unwrap();
        program
            .build(root.path().join(format!("join-{index}")))
            .unwrap();
    }
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
