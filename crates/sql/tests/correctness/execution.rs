use std::path::Path;

use dogpaddle_flow::{AdvanceOutcome, Flow, FlowError};
use dogpaddle_sql::{SqlError, SqlProgram};
use rusqlite::{Connection, OpenFlags};

const TABLE: &str = "selected_numbers";
const OUTPUT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

#[test]
fn bundled_quickstart_builds_and_reopens_without_duplicate_rows() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("quickstart.sqlite");
    let sql = include_str!("../../examples/quickstart.sql").replace(
        "env('DOGPADDLE_QUICKSTART_SQLITE')",
        &format!("'{}'", sql_string(&sqlite_path)),
    );
    let program = SqlProgram::parse(&sql).unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(
        flow.status()
            .unwrap()
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        ["sql/scan/00000000", "sql/sink"]
    );
    for _ in 0..12 {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(flow);
    assert_eq!(
        quickstart_rows(&sqlite_path),
        [
            (0, 0, "small".to_owned()),
            (2, 4, "small".to_owned()),
            (4, 16, "small".to_owned()),
            (6, 36, "small".to_owned()),
            (8, 64, "small".to_owned()),
        ]
    );

    let mut flow = program.start(&flow_path).unwrap();
    for _ in 0..6 {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(flow);
    assert_eq!(
        quickstart_rows(&sqlite_path),
        [
            (0, 0, "small".to_owned()),
            (2, 4, "small".to_owned()),
            (4, 16, "small".to_owned()),
            (6, 36, "small".to_owned()),
            (8, 64, "small".to_owned()),
            (10, 100, "large".to_owned()),
            (12, 144, "large".to_owned()),
        ]
    );
}

#[test]
fn projection_executes_qualified_coerced_expressions_into_sqlite() {
    let root = tempfile::tempdir().unwrap();
    let sqlite_path = root.path().join("expressions.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'expressions') \
         SELECT \
             CASE \
                 WHEN derived.value = 18446744073709551614 THEN CAST('first' AS VARCHAR) \
                 ELSE TRY_CAST(derived.value AS VARCHAR) \
             END AS label, \
             TRY_CAST(\
                 CASE \
                     WHEN derived.value = 18446744073709551614 THEN 'invalid' \
                     ELSE '7' \
                 END \
                 AS BIGINT\
             ) AS parsed, \
             CAST(derived.value AS DECIMAL(20, 0)) \
                 - CAST(18446744073709551614 AS DECIMAL(20, 0)) AS ordinal \
         FROM (\
             SELECT sequence.value \
             FROM sequence(start => 18446744073709551614)\
         ) AS derived \
         WHERE derived.value >= CAST(18446744073709551614 AS DECIMAL(20, 0))",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(root.path().join("flow")).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let mut statement = connection
        .prepare("SELECT label, parsed, ordinal FROM expressions")
        .unwrap();
    let mut rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                decode_i128(row.get::<_, Vec<u8>>(2)?),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        rows,
        [
            ("18446744073709551615".to_owned(), Some(7), 1),
            ("first".to_owned(), None, 0),
        ]
    );
    assert_eq!(sqlite_column(&connection, "expressions", "label").0, "TEXT");
    assert_eq!(
        sqlite_column(&connection, "expressions", "parsed"),
        ("INTEGER".to_owned(), false)
    );
}

#[test]
fn inner_join_executes_native_residual_and_projection_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("join.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'joined_values') \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 18446744073709551613) AS left_scan \
         JOIN sequence(start => 18446744073709551613) AS right_scan \
         ON left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
         AND left_scan.value < right_scan.value",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let mut rows = connection
        .prepare("SELECT left_value, right_value FROM joined_values")
        .unwrap()
        .query_map([], |row| {
            Ok((
                decode_u64(row.get::<_, Vec<u8>>(0)?),
                decode_u64(row.get::<_, Vec<u8>>(1)?),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort_unstable();
    assert_eq!(rows, [(u64::MAX - 2, u64::MAX)]);

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn supported_key_keeps_float_equality_as_a_residual() {
    let root = tempfile::tempdir().unwrap();
    let sqlite_path = root.path().join("float-residual.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'joined_values') \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 18446744073709551613) AS left_scan \
         JOIN sequence(start => 18446744073709551613) AS right_scan \
         ON left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
         AND CAST(left_scan.value % CAST(3 AS BIGINT UNSIGNED) AS DOUBLE) \
                = CAST(right_scan.value % CAST(3 AS BIGINT UNSIGNED) AS DOUBLE)",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(root.path().join("flow")).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let mut rows = connection
        .prepare("SELECT left_value, right_value FROM joined_values")
        .unwrap()
        .query_map([], |row| {
            Ok((
                decode_u64(row.get::<_, Vec<u8>>(0)?),
                decode_u64(row.get::<_, Vec<u8>>(1)?),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            (u64::MAX - 2, u64::MAX - 2),
            (u64::MAX - 1, u64::MAX - 1),
            (u64::MAX, u64::MAX),
        ]
    );
}

struct OuterJoinCase {
    name: &'static str,
    join: &'static str,
    left_start: u64,
    right_start: u64,
    condition: &'static str,
    expected: Vec<(Option<u64>, Option<u64>)>,
    left_not_null: bool,
    right_not_null: bool,
}

#[test]
fn outer_join_family_preserves_rows_and_schema_across_reopen() {
    let cases = [
        OuterJoinCase {
            name: "left",
            join: "LEFT OUTER JOIN",
            left_start: u64::MAX - 2,
            right_start: u64::MAX - 2,
            condition: "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        AND left_scan.value < right_scan.value",
            expected: vec![
                (Some(u64::MAX - 2), Some(u64::MAX)),
                (Some(u64::MAX - 1), None),
                (Some(u64::MAX), None),
            ],
            left_not_null: true,
            right_not_null: false,
        },
        OuterJoinCase {
            name: "right",
            join: "RIGHT OUTER JOIN",
            left_start: u64::MAX - 2,
            right_start: u64::MAX - 2,
            condition: "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        AND left_scan.value < right_scan.value",
            expected: vec![
                (None, Some(u64::MAX - 2)),
                (None, Some(u64::MAX - 1)),
                (Some(u64::MAX - 2), Some(u64::MAX)),
            ],
            left_not_null: false,
            right_not_null: true,
        },
        OuterJoinCase {
            name: "full",
            join: "FULL OUTER JOIN",
            left_start: u64::MAX - 2,
            right_start: u64::MAX - 2,
            condition: "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        AND left_scan.value < right_scan.value",
            expected: vec![
                (None, Some(u64::MAX - 2)),
                (None, Some(u64::MAX - 1)),
                (Some(u64::MAX - 2), Some(u64::MAX)),
                (Some(u64::MAX - 1), None),
                (Some(u64::MAX), None),
            ],
            left_not_null: false,
            right_not_null: false,
        },
        OuterJoinCase {
            name: "left-null-residual",
            join: "LEFT OUTER JOIN",
            left_start: u64::MAX - 2,
            right_start: u64::MAX - 2,
            condition: "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
                        AND CAST(NULL AS BOOLEAN)",
            expected: vec![
                (Some(u64::MAX - 2), None),
                (Some(u64::MAX - 1), None),
                (Some(u64::MAX), None),
            ],
            left_not_null: true,
            right_not_null: false,
        },
    ];

    let root = tempfile::tempdir().unwrap();
    for case in cases {
        assert_outer_join_case(root.path(), case);
    }
}

fn assert_outer_join_case(root: &Path, case: OuterJoinCase) {
    let flow_path = root.join(format!("{}-flow", case.name));
    let sqlite_path = root.join(format!("{}.sqlite", case.name));
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'joined') \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => {}) AS left_scan \
         {} sequence(start => {}) AS right_scan \
         ON {}",
        sql_string(&sqlite_path),
        case.left_start,
        case.join,
        case.right_start,
        case.condition,
    ))
    .unwrap();

    drop(program.start(&flow_path).unwrap());
    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let mut rows = connection
        .prepare("SELECT left_value, right_value FROM joined")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?.map(decode_u64),
                row.get::<_, Option<Vec<u8>>>(1)?.map(decode_u64),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort_unstable();
    let mut expected = case.expected;
    expected.sort_unstable();
    assert_eq!(rows, expected, "{} outer join", case.name);
    assert_eq!(
        sqlite_column(&connection, "joined", "left_value").1,
        case.left_not_null
    );
    assert_eq!(
        sqlite_column(&connection, "joined", "right_value").1,
        case.right_not_null
    );
}

#[test]
fn right_outer_join_restores_asymmetric_sql_schema_and_state_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("right-asymmetric.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'joined') \
         SELECT \
             left_scan.left_id, left_scan.left_code, \
             right_scan.right_id, right_scan.right_code, right_scan.right_text \
         FROM (\
             SELECT value AS left_id, CAST(value % 10 AS BIGINT) AS left_code \
             FROM sequence(start => {})\
         ) AS left_scan \
         RIGHT OUTER JOIN (\
             SELECT value AS right_id, CAST(value % 10 AS BIGINT) AS right_code, \
                    CAST(value AS VARCHAR) AS right_text \
             FROM sequence(start => {})\
         ) AS right_scan \
         ON left_scan.left_code % CAST(1 AS BIGINT) \
                = right_scan.right_code % CAST(1 AS BIGINT) \
         AND left_scan.left_id <= right_scan.right_id",
        sql_string(&sqlite_path),
        u64::MAX,
        u64::MAX - 1,
    ))
    .unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);
    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let rows = connection
        .prepare(
            "SELECT left_id, left_code, right_id, right_code, right_text \
             FROM joined ORDER BY right_text",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?.map(decode_u64),
                row.get::<_, Option<i64>>(1)?,
                decode_u64(row.get::<_, Vec<u8>>(2)?),
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (None, None, u64::MAX - 1, 4, (u64::MAX - 1).to_string(),),
            (Some(u64::MAX), Some(5), u64::MAX, 5, u64::MAX.to_string()),
        ]
    );
    assert_eq!(
        [
            sqlite_column(&connection, "joined", "left_id"),
            sqlite_column(&connection, "joined", "left_code"),
            sqlite_column(&connection, "joined", "right_id"),
            sqlite_column(&connection, "joined", "right_code"),
            sqlite_column(&connection, "joined", "right_text"),
        ],
        [
            ("BLOB".to_owned(), false),
            ("INTEGER".to_owned(), false),
            ("BLOB".to_owned(), true),
            ("INTEGER".to_owned(), true),
            ("TEXT".to_owned(), true),
        ]
    );
}

#[test]
fn semi_and_anti_join_family_applies_residuals_across_reopen() {
    let start = u64::MAX - 2;
    let cases = [
        (
            "left-semi",
            "left_scan.value",
            "LEFT SEMI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND left_scan.value < right_scan.value",
            vec![u64::MAX - 2],
        ),
        (
            "left-anti",
            "left_scan.value",
            "LEFT ANTI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND left_scan.value < right_scan.value",
            vec![u64::MAX - 1, u64::MAX],
        ),
        (
            "right-semi",
            "right_scan.value",
            "RIGHT SEMI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND left_scan.value < right_scan.value",
            vec![u64::MAX],
        ),
        (
            "right-anti",
            "right_scan.value",
            "RIGHT ANTI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND right_scan.value = 18446744073709551615",
            vec![u64::MAX - 2, u64::MAX - 1],
        ),
        (
            "left-semi-null-residual",
            "left_scan.value",
            "LEFT SEMI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND CAST(NULL AS BOOLEAN)",
            vec![],
        ),
        (
            "left-anti-null-residual",
            "left_scan.value",
            "LEFT ANTI JOIN",
            "left_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             = right_scan.value % CAST(2 AS BIGINT UNSIGNED) \
             AND CAST(NULL AS BOOLEAN)",
            vec![u64::MAX - 2, u64::MAX - 1, u64::MAX],
        ),
    ];

    let root = tempfile::tempdir().unwrap();
    for (name, selected, join, condition, expected) in cases {
        let flow_path = root.path().join(format!("{name}-flow"));
        let sqlite_path = root.path().join(format!("{name}.sqlite"));
        let program = SqlProgram::parse(&format!(
            "INSERT INTO sqlite(path => '{}', table => 'selected') \
             SELECT {selected} AS value \
             FROM sequence(start => {start}) AS left_scan \
             {join} sequence(start => {start}) AS right_scan \
             ON {condition}",
            sql_string(&sqlite_path),
        ))
        .unwrap();

        drop(program.start(&flow_path).unwrap());
        let mut flow = program.start(&flow_path).unwrap();
        advance_to_idle(&mut flow);
        drop(flow);

        if expected.is_empty() {
            assert!(
                !sqlite_path.exists(),
                "{name} emitted an unexpected relation"
            );
            continue;
        }
        let connection = sqlite(&sqlite_path);
        let mut values = connection
            .prepare("SELECT value FROM selected")
            .unwrap()
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|value| decode_u64(value.unwrap()))
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, expected, "{name}");
        assert!(sqlite_column(&connection, "selected", "value").1);
    }
}

#[test]
fn select_distinct_deduplicates_projected_rows_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("distinct.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'distinct_values') \
         SELECT DISTINCT CAST(value % 2 AS BIGINT) AS value \
         FROM sequence(start => 18446744073709551612)",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(station_ids(&flow), ["sql/scan/00000000", "sql/sink"]);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let mut values = connection
        .prepare("SELECT value FROM distinct_values")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    values.sort_unstable();
    assert_eq!(values, [0, 1]);
}

#[test]
fn grouped_aggregates_update_one_relation_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("aggregates.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'aggregates') \
         SELECT \
             CAST(value % 2 AS BIGINT) AS parity, \
             COUNT(*) AS row_count, \
             COUNT(1) AS literal_count, \
             COUNT(CASE WHEN value % 4 = 0 THEN value END) AS selected_count, \
             SUM(CAST(value % 4 AS BIGINT)) AS total, \
             SUM((value + CAST(0 AS BIGINT UNSIGNED)) % CAST(4 AS BIGINT UNSIGNED)) \
                 AS unsigned_total, \
             AVG(CAST(value % 4 AS BIGINT)) AS mean, \
             AVG((value + CAST(0 AS BIGINT UNSIGNED)) % CAST(4 AS BIGINT UNSIGNED)) \
                 AS unsigned_mean, \
             MIN(CAST(value % 4 AS BIGINT)) AS minimum, \
             MAX(CAST(value % 4 AS BIGINT)) AS maximum \
         FROM sequence(start => 18446744073709551612) \
         GROUP BY CAST(value % 2 AS BIGINT)",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(station_ids(&flow), ["sql/scan/00000000", "sql/sink"]);
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let rows = connection
        .prepare(
            "SELECT parity, row_count, literal_count, selected_count, total, unsigned_total, mean, unsigned_mean, minimum, maximum \
             FROM aggregates ORDER BY parity",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                decode_u64(row.get::<_, Vec<u8>>(5)?),
                decode_f64(row.get::<_, Vec<u8>>(6)?),
                decode_f64(row.get::<_, Vec<u8>>(7)?),
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (0, 2, 2, 1, 2, 2, 1.0, 1.0, 0, 2),
            (1, 2, 2, 0, 4, 4, 2.0, 2.0, 1, 3)
        ]
    );

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
}

#[test]
fn group_by_without_calls_emits_one_row_per_group() {
    let root = tempfile::tempdir().unwrap();
    let sqlite_path = root.path().join("groups.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'groups') \
         SELECT CAST(value % 2 AS BIGINT) AS parity \
         FROM sequence(start => 18446744073709551614) \
         GROUP BY CAST(value % 2 AS BIGINT)",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(root.path().join("flow")).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    let rows = connection
        .prepare("SELECT parity FROM groups ORDER BY parity")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, [0, 1]);
}

#[test]
fn union_all_keeps_separate_scan_stations() {
    let root = tempfile::tempdir().unwrap();
    let program = SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT value FROM sequence(start => 0) \
         UNION ALL \
         SELECT value FROM sequence(start => 1)",
    )
    .unwrap();

    let flow = program.start(root.path().join("flow")).unwrap();
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
    let scan_ids = flow
        .status()
        .unwrap()
        .into_iter()
        .filter_map(|station| station.id.starts_with("sql/scan/").then_some(station.id))
        .collect::<Vec<_>>();
    assert_eq!(scan_ids, ["sql/scan/00000000", "sql/scan/00000001"]);
}

#[test]
fn union_all_preserves_common_name_type_nullability_and_multiplicity() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let sqlite_path = root.path().join("union.sqlite");
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'union_values') \
         SELECT CAST(value AS DECIMAL(20, 0)) AS \"amount.value\" \
         FROM sequence(start => 18446744073709551614) \
         UNION ALL \
         SELECT \
             CASE \
                 WHEN value = 18446744073709551614 THEN NULL \
                 ELSE value \
             END AS ignored_name \
         FROM sequence(start => 18446744073709551614)",
        sql_string(&sqlite_path)
    ))
    .unwrap();

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = program.start(&flow_path).unwrap();
    advance_to_idle(&mut flow);
    drop(flow);

    let connection = sqlite(&sqlite_path);
    assert_eq!(
        sqlite_column(&connection, "union_values", "amount.value"),
        ("BLOB".to_owned(), false)
    );
    let mut statement = connection
        .prepare("SELECT \"amount.value\" FROM union_values")
        .unwrap();
    let mut values = statement
        .query_map([], |row| row.get::<_, Option<Vec<u8>>>(0))
        .unwrap()
        .map(|value| value.unwrap().map(decode_i128))
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(
        values,
        [
            None,
            Some(i128::from(u64::MAX - 1)),
            Some(i128::from(u64::MAX)),
            Some(i128::from(u64::MAX)),
        ]
    );

    let mut flow = program.start(&flow_path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    drop(flow);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM union_values", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        4
    );
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
    let mut flow = parsed.start(&flow_path).unwrap();
    assert!(!sqlite_path.exists());

    let status = flow.status().unwrap();
    assert_eq!(
        status
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        [
            "sql/scan/00000000",
            "sql/transform/00000000",
            "sql/transform/00000001",
            "sql/transform/00000002",
            "sql/sink",
        ]
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

    let mut reopened = read.start(&flow_path).unwrap();
    assert_eq!(
        reopened
            .status()
            .unwrap()
            .iter()
            .map(|station| station.id.as_str())
            .collect::<Vec<_>>(),
        [
            "sql/scan/00000000",
            "sql/transform/00000000",
            "sql/transform/00000001",
            "sql/transform/00000002",
            "sql/sink",
        ]
    );
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

    let mut reopened = parsed.start(&flow_path).unwrap();
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
    let Err(error) = replacement.start(&flow_path) else {
        panic!("different SQL opened existing state");
    };
    assert!(matches!(
        error,
        SqlError::Flow(FlowError::OwnerIdentityMismatch)
    ));
    assert!(!replacement_path.exists());
}

fn station_ids(flow: &Flow) -> Vec<String> {
    flow.status()
        .unwrap()
        .into_iter()
        .map(|station| station.id)
        .collect()
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
    let connection = sqlite(path);
    let mut statement = connection
        .prepare(&format!("SELECT number FROM {TABLE}"))
        .unwrap();
    let mut values = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .map(|value| decode_u64(value.unwrap()))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn quickstart_rows(path: &Path) -> Vec<(i64, i64, String)> {
    let connection = sqlite(path);
    connection
        .prepare("SELECT number, square, size FROM even_squares ORDER BY number")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn advance_to_idle(flow: &mut Flow) {
    for _ in 0..128 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            return;
        }
    }
    panic!("SQL Flow did not become idle within 128 advances");
}

fn sqlite(path: &Path) -> Connection {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

fn sqlite_column(connection: &Connection, table: &str, column: &str) -> (String, bool) {
    connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .unwrap()
        .find_map(|row| {
            let (name, data_type, not_null) = row.unwrap();
            (name == column).then_some((data_type, not_null))
        })
        .expect("SQLite target column exists")
}

fn decode_i128(value: Vec<u8>) -> i128 {
    i128::from_be_bytes(
        value
            .try_into()
            .expect("DogPaddle stores Decimal128 as a sixteen-byte SQLite blob"),
    )
}

fn decode_u64(value: Vec<u8>) -> u64 {
    u64::from_be_bytes(
        value
            .try_into()
            .expect("DogPaddle stores UInt64 as an eight-byte SQLite blob"),
    )
}

fn decode_f64(value: Vec<u8>) -> f64 {
    f64::from_bits(u64::from_be_bytes(
        value
            .try_into()
            .expect("DogPaddle stores Float64 as an eight-byte SQLite blob"),
    ))
}
