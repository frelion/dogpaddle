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

    assert_eq!(definition.len(), 777);
    assert_eq!(
        blake3::hash(&definition).to_hex().as_str(),
        "235a2894ab2686559d4774ff2f5d7ae37b0cc6ff0d433d9d9b0052e414baa232"
    );
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
