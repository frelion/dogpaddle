use std::path::Path;

use dogpaddle_flow::AdvanceOutcome;
use dogpaddle_sql::SqlProgram;
use rusqlite::{Connection, OpenFlags};

const TABLE: &str = "selected_numbers";
const OUTPUT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

#[test]
fn derived_query_executes_qualified_case_and_try_cast_expressions() {
    let root = tempfile::tempdir().unwrap();
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT CASE \
                    WHEN derived.value >= 7 \
                    THEN TRY_CAST(derived.value AS VARCHAR) \
                    ELSE CAST('small' AS VARCHAR) \
                END AS label \
         FROM (\
             SELECT scan.value \
             FROM sequence(start => 7) AS scan\
         ) AS derived \
         WHERE derived.value = 7",
    )
    .unwrap();

    let mut flow = program.build(root.path().join("flow")).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
}

#[test]
fn union_all_keeps_distinct_scan_stations() {
    let root = tempfile::tempdir().unwrap();
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT value FROM sequence(start => 0) \
         UNION ALL \
         SELECT value FROM sequence(start => 1)",
    )
    .unwrap();

    let flow = program.build(root.path().join("flow")).unwrap();
    let scan_ids = flow
        .status()
        .unwrap()
        .into_iter()
        .filter_map(|station| station.id.starts_with("sql/scan/").then_some(station.id))
        .collect::<Vec<_>>();
    assert_eq!(scan_ids, ["sql/scan/00000000", "sql/scan/00000001"]);
}

#[test]
fn sql_file_builds_and_reopens_a_filtered_union_into_sqlite() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("results.sqlite");
    let sql_path = root.path().join("flow.sql");
    let sql = sqlite_program(&sqlite_path);
    std::fs::write(&sql_path, &sql).unwrap();

    let parsed = SqlProgram::parse(&sql).unwrap();
    let read = SqlProgram::read(&sql_path).unwrap();
    let mut flow = parsed.build(&flow_path).unwrap();
    assert!(!sqlite_path.exists());

    let status = flow.status().unwrap();
    let expected_ids = std::iter::once("sql/scan/00000000".to_owned())
        .chain((0..status.len() - 2).map(|index| format!("sql/transform/{index:08x}")))
        .chain(std::iter::once("sql/sink".to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        status
            .iter()
            .map(|station| station.id.clone())
            .collect::<Vec<_>>(),
        expected_ids
    );
    assert_eq!(
        status
            .iter()
            .filter(|station| station.id.starts_with("sql/scan/"))
            .count(),
        1
    );
    for station in &status[..status.len() - 1] {
        assert_eq!(
            station.output.as_ref().unwrap().capacity_bytes,
            OUTPUT_CAPACITY_BYTES
        );
    }
    assert!(status.last().unwrap().output.is_none());

    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut reopened = read.open(&flow_path).unwrap();
    let mut outcomes = Vec::new();
    for _ in 0..64 {
        let outcome = reopened.advance().unwrap();
        outcomes.push(outcome);
        if outcome == AdvanceOutcome::Idle {
            break;
        }
    }
    assert_eq!(outcomes.last(), Some(&AdvanceOutcome::Idle));
    drop(reopened);

    assert_eq!(sqlite_values(&sqlite_path), vec![u64::MAX - 1, u64::MAX]);

    let mut reopened = parsed.open(&flow_path).unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
    drop(reopened);
    assert_eq!(sqlite_values(&sqlite_path), vec![u64::MAX - 1, u64::MAX]);

    let replacement_path = root.path().join("replacement.sqlite");
    let replacement = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'replacement') \
         SELECT value FROM sequence(start => 0) WHERE value < 10",
        sql_string(&replacement_path)
    ))
    .unwrap();
    let mut reopened = replacement.open(&flow_path).unwrap();
    assert_eq!(reopened.advance().unwrap(), AdvanceOutcome::Idle);
    assert!(!replacement_path.exists());
}

fn sqlite_program(sqlite_path: &Path) -> String {
    let sqlite_path = sql_string(sqlite_path);
    format!(
        r"
        -- A CTE names one Scan whose output feeds both UNION ALL branches.
        INSERT INTO sqlite(table => '{TABLE}', path => '{sqlite_path}')
        WITH numbers AS (
            SELECT value
            FROM sequence(start => 18446744073709551613)
        ),
        even_numbers AS (
            SELECT value AS number
            FROM numbers
            WHERE value % 2 = 0
        )
        SELECT number FROM even_numbers
        UNION ALL
        SELECT value AS number
        FROM numbers
        WHERE value = 18446744073709551615;
        "
    )
}

fn sql_string(path: &Path) -> String {
    path.to_str()
        .expect("temporary paths used by this test are UTF-8")
        .replace('\'', "''")
}

fn sqlite_values(path: &Path) -> Vec<u64> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut statement = connection
        .prepare(&format!("SELECT number FROM {TABLE}"))
        .unwrap();
    let mut values = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|value| {
            u64::from_be_bytes(
                value
                    .unwrap()
                    .try_into()
                    .expect("DogPaddle stores UInt64 as an eight-byte SQLite blob"),
            )
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}
