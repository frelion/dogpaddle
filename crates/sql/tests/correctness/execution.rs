use std::{path::Path, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_flow::{AdvanceOutcome, Flow, FlowError};
use dogpaddle_operation::OperationDefinition;
use dogpaddle_sql::{SqlError, SqlProgram};
use dogpaddle_store::{Cell, Store};
use rusqlite::{Connection, OpenFlags};

const TABLE: &str = "selected_numbers";

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
        flow.operation_ids().collect::<Vec<_>>(),
        [
            "sql/scan/00000000",
            "sql/transform/00000000",
            "sql/transform/00000001",
            "sql/transform/00000002",
            "sql/sink"
        ]
    );
    let before_reopen = advance_until_quickstart_rows(&mut flow, &sqlite_path, 5);
    drop(flow);
    assert_eq!(before_reopen, expected_quickstart_rows(before_reopen.len()));

    let mut flow = program.start(&flow_path).unwrap();
    let after_reopen =
        advance_until_quickstart_rows(&mut flow, &sqlite_path, before_reopen.len() + 2);
    drop(flow);
    assert_eq!(after_reopen, expected_quickstart_rows(after_reopen.len()));
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
    assert_restores_to_idle(&mut flow);
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
            let connection = sqlite(&sqlite_path);
            let rows = connection
                .query_row("SELECT COUNT(*) FROM selected", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            assert_eq!(rows, 0, "{name} emitted an unexpected relation");
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

struct AsOfExecutionCase {
    name: &'static str,
    comparison: &'static str,
    constraint: &'static str,
    expected: Vec<(u64, Option<u64>)>,
}

const ASOF_MAX: u64 = u64::MAX;
const ASOF_LEFT_START: u64 = ASOF_MAX - 6;
const ASOF_RIGHT_START: u64 = ASOF_MAX - 12;

fn asof_execution_cases() -> [AsOfExecutionCase; 4] {
    const PARTITIONED_ON: &str = "ON left_scan.bucket = right_scan.bucket \
                                  AND left_scan.shard = right_scan.shard";

    [
        AsOfExecutionCase {
            name: "backward-exact-partitioned",
            comparison: ">=",
            constraint: PARTITIONED_ON,
            expected: vec![
                (ASOF_MAX - 6, Some(ASOF_MAX - 6)),
                (ASOF_MAX - 5, None),
                (ASOF_MAX - 4, None),
                (ASOF_MAX - 3, None),
                (ASOF_MAX - 2, None),
                (ASOF_MAX - 1, None),
                (ASOF_MAX, Some(ASOF_MAX)),
            ],
        },
        AsOfExecutionCase {
            name: "backward-strict-partitioned",
            comparison: ">",
            constraint: "USING (bucket, shard)",
            expected: vec![
                (ASOF_MAX - 6, Some(ASOF_MAX - 12)),
                (ASOF_MAX - 5, None),
                (ASOF_MAX - 4, None),
                (ASOF_MAX - 3, None),
                (ASOF_MAX - 2, None),
                (ASOF_MAX - 1, None),
                (ASOF_MAX, Some(ASOF_MAX - 6)),
            ],
        },
        AsOfExecutionCase {
            name: "forward-exact-global",
            comparison: "<=",
            constraint: "",
            expected: vec![
                (ASOF_MAX - 6, Some(ASOF_MAX - 6)),
                (ASOF_MAX - 5, Some(ASOF_MAX)),
                (ASOF_MAX - 4, Some(ASOF_MAX)),
                (ASOF_MAX - 3, Some(ASOF_MAX)),
                (ASOF_MAX - 2, Some(ASOF_MAX)),
                (ASOF_MAX - 1, Some(ASOF_MAX)),
                (ASOF_MAX, Some(ASOF_MAX)),
            ],
        },
        AsOfExecutionCase {
            name: "forward-strict-global",
            comparison: "<",
            constraint: "",
            expected: vec![
                (ASOF_MAX - 6, Some(ASOF_MAX)),
                (ASOF_MAX - 5, Some(ASOF_MAX)),
                (ASOF_MAX - 4, Some(ASOF_MAX)),
                (ASOF_MAX - 3, Some(ASOF_MAX)),
                (ASOF_MAX - 2, Some(ASOF_MAX)),
                (ASOF_MAX - 1, Some(ASOF_MAX)),
                (ASOF_MAX, None),
            ],
        },
    ]
}

#[test]
fn native_asof_join_executes_all_directions_and_historical_right_corrections_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    for case in asof_execution_cases() {
        let flow_path = root.path().join(format!("{}-flow", case.name));
        let sqlite_path = root.path().join(format!("{}.sqlite", case.name));
        let program = SqlProgram::parse(&format!(
            "INSERT INTO sqlite(path => '{}', table => 'asof_rows') \
             SELECT left_scan.ts AS left_ts, right_scan.ts AS right_ts \
             FROM (\
                 SELECT value AS ts, \
                        value % CAST(2 AS BIGINT UNSIGNED) AS bucket, \
                        value % CAST(3 AS BIGINT UNSIGNED) AS shard \
                 FROM sequence(start => {ASOF_LEFT_START})\
             ) AS left_scan \
             ASOF JOIN (\
                 SELECT value AS ts, \
                        value % CAST(2 AS BIGINT UNSIGNED) AS bucket, \
                        value % CAST(3 AS BIGINT UNSIGNED) AS shard \
                 FROM sequence(start => {ASOF_RIGHT_START}) \
                 WHERE value % CAST(6 AS BIGINT UNSIGNED) \
                       = CAST(3 AS BIGINT UNSIGNED)\
             ) AS right_scan \
             MATCH_CONDITION (left_scan.ts {} right_scan.ts) \
             {}",
            sql_string(&sqlite_path),
            case.comparison,
            case.constraint,
        ))
        .unwrap();

        let mut flow = program.start(&flow_path).unwrap();
        // Reopen with the first ASOF result saved for sending, before any right input.
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
        let status = flow.status().unwrap();
        assert!(status.sending, "{}: {status:?}", case.name);
        let active = flow
            .operation_ids()
            .position(|id| Some(id) == status.active_operation.as_deref())
            .unwrap();
        drop(flow);
        assert_initial_asof_state(&flow_path, active);

        let mut flow = program.start(&flow_path).unwrap();
        advance_to_idle(&mut flow);
        drop(flow);

        assert_eq!(asof_rows(&sqlite_path), case.expected, "{}", case.name);
        let connection = sqlite(&sqlite_path);
        assert!(sqlite_column(&connection, "asof_rows", "left_ts").1);
        assert!(!sqlite_column(&connection, "asof_rows", "right_ts").1);

        let mut flow = program.start(&flow_path).unwrap();
        assert_restores_to_idle(&mut flow);
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
    assert_eq!(
        operation_ids(&flow),
        [
            "sql/scan/00000000",
            "sql/transform/00000000",
            "sql/transform/00000001",
            "sql/sink"
        ]
    );
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
    assert_eq!(
        operation_ids(&flow),
        [
            "sql/scan/00000000",
            "sql/transform/00000000",
            "sql/transform/00000001",
            "sql/sink"
        ]
    );
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
    assert_restores_to_idle(&mut flow);
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
        flow.operation_ids().collect::<Vec<_>>(),
        [
            "sql/scan/00000000",
            "sql/scan/00000001",
            "sql/transform/00000000",
            "sql/sink",
        ]
    );
    let scan_ids = flow
        .operation_ids()
        .filter(|id| id.starts_with("sql/scan/"))
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
    assert_restores_to_idle(&mut flow);
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

    let identities = flow.operation_ids().map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(
        identities
            .iter()
            .filter(|id| id.starts_with("sql/scan/"))
            .count(),
        1
    );
    assert_eq!(identities.last().unwrap(), "sql/sink");
    assert_eq!(flow.status().unwrap().depth, 0);

    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut reopened = read.start(&flow_path).unwrap();
    assert_eq!(
        reopened.operation_ids().collect::<Vec<_>>(),
        identities.iter().map(String::as_str).collect::<Vec<_>>()
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
    assert_restores_to_idle(&mut reopened);
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

#[test]
fn having_filters_complete_aggregate_changes_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let schema = grouped_schema();
    for (name, predicate, prefix, expected) in [
        ("at_least_two", ">= 2", vec![], vec![(0, 2), (1, 2)]),
        ("only_one", "= 1", vec![(0, 1), (1, 1)], vec![]),
    ] {
        let state = root.path().join(format!("{name}-state"));
        let target = root.path().join(format!("{name}.sqlite"));
        let query = format!(
            "SELECT CAST(value % 2 AS BIGINT) AS parity, COUNT(*) AS row_count \
             FROM sequence(start => 18446744073709551612) \
             GROUP BY CAST(value % 2 AS BIGINT) HAVING COUNT(*) {predicate}"
        );
        let program = capability_program(&target, &query);
        let definition = start_midway(&program, &state, &schema, 2);
        assert_eq!(grouped_rows(&target), prefix);
        finish(&program, &state);
        assert_eq!(grouped_rows(&target), expected);
        assert_completed_reopen(&program, &state, &definition);
        assert_eq!(grouped_rows(&target), expected);
    }
}

#[test]
fn union_distinct_coerces_nullable_rows_and_deduplicates_the_full_bag_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let target = root.path().join("union.sqlite");
    let program = capability_program(
        &target,
        "SELECT CAST(value % 2 AS BIGINT) AS \"value.code\" \
         FROM sequence(start => 18446744073709551612) \
         UNION \
         SELECT CASE WHEN value % 2 = 0 THEN NULL \
                     ELSE CAST(value % 2 AS SMALLINT) END AS ignored_name \
         FROM sequence(start => 18446744073709551612)",
    );
    let schema = Schema::new(vec![Field::new("value.code", DataType::Int64, true)]);
    let definition = start_midway(&program, &state, &schema, 2);
    assert_eq!(nullable_codes(&target), [None, Some(0)]);
    finish(&program, &state);
    assert_eq!(nullable_codes(&target), [None, Some(0), Some(1)]);
    assert_completed_reopen(&program, &state, &definition);
    assert_eq!(nullable_codes(&target), [None, Some(0), Some(1)]);
}

#[test]
fn union_by_name_preserves_reordering_null_fill_and_multiplicity_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![
        Field::new("value.code", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]);
    for (name, quantifier, prefix, expected) in [
        (
            "all",
            "ALL BY NAME",
            vec![(0, None), (0, None)],
            vec![
                (0, None),
                (0, None),
                (0, None),
                (1, None),
                (1, Some("ok".to_owned())),
                (1, Some("ok".to_owned())),
            ],
        ),
        (
            "distinct",
            "BY NAME",
            vec![(0, None)],
            vec![(0, None), (1, None), (1, Some("ok".to_owned()))],
        ),
        (
            "explicit_distinct",
            "DISTINCT BY NAME",
            vec![(0, None)],
            vec![(0, None), (1, None), (1, Some("ok".to_owned()))],
        ),
    ] {
        let state = root.path().join(format!("{name}-state"));
        let target = root.path().join(format!("{name}.sqlite"));
        let final_fields = if quantifier == "DISTINCT BY NAME" {
            "CAST(value % 2 AS BIGINT) AS \"value.code\", CAST(NULL AS VARCHAR) AS label"
        } else {
            "CAST(value % 2 AS BIGINT) AS \"value.code\""
        };
        let query = format!(
            "SELECT CAST(value % 2 AS BIGINT) AS \"value.code\", \
                    CASE WHEN value % 2 = 0 THEN CAST(NULL AS VARCHAR) \
                         ELSE 'ok' END AS label \
             FROM sequence(start => 18446744073709551614) \
             UNION {quantifier} \
             SELECT CASE WHEN value % 2 = 0 THEN CAST(NULL AS VARCHAR) \
                         ELSE 'ok' END AS label, \
                    CAST(value % 2 AS SMALLINT) AS \"value.code\" \
             FROM sequence(start => 18446744073709551614) \
             UNION {quantifier} \
             SELECT {final_fields} \
             FROM sequence(start => 18446744073709551614)"
        );
        let program = capability_program(&target, &query);
        let definition = start_midway(&program, &state, &schema, 2);
        assert_eq!(named_rows(&target), prefix);
        finish(&program, &state);
        assert_eq!(named_rows(&target), expected);
        assert_completed_reopen(&program, &state, &definition);
        assert_eq!(named_rows(&target), expected);
    }
}

#[test]
fn explicit_union_distinct_by_name_rejects_different_branch_widths_without_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let target = root.path().join("union.sqlite");
    let program = capability_program(
        &target,
        "SELECT CAST(value % 2 AS BIGINT) AS code, 'ok' AS label \
         FROM sequence(start => 18446744073709551614) \
         UNION DISTINCT BY NAME \
         SELECT CAST(value % 2 AS BIGINT) AS code \
         FROM sequence(start => 18446744073709551614)",
    );
    assert!(matches!(program.start(&state), Err(SqlError::Planning(_))));
    assert!(!state.exists());
    assert!(!target.exists());
}

#[test]
fn group_by_all_and_unsorted_pipe_aggregate_use_existing_grouped_semantics() {
    let root = tempfile::tempdir().unwrap();
    let schema = grouped_schema();
    for (name, query) in [
        (
            "all",
            "SELECT CAST(value % 2 AS BIGINT) AS parity, COUNT(*) AS row_count \
             FROM sequence(start => 18446744073709551612) GROUP BY ALL",
        ),
        (
            "pipe",
            "SELECT value FROM sequence(start => 18446744073709551612) \
             |> AGGREGATE COUNT(*) AS row_count \
             GROUP BY CAST(value % 2 AS BIGINT) AS parity",
        ),
    ] {
        let state = root.path().join(format!("{name}-state"));
        let target = root.path().join(format!("{name}.sqlite"));
        let program = capability_program(&target, query);
        let definition = start_midway(&program, &state, &schema, 2);
        assert_eq!(grouped_rows(&target), [(0, 1), (1, 1)]);
        finish(&program, &state);
        assert_eq!(grouped_rows(&target), [(0, 2), (1, 2)]);
        assert_completed_reopen(&program, &state, &definition);
        assert_eq!(grouped_rows(&target), [(0, 2), (1, 2)]);
    }
}

#[test]
fn unused_cte_sorts_do_not_change_the_executed_relation() {
    let root = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![Field::new("code", DataType::Int64, false)]);
    for (name, query) in [
        (
            "constant",
            "WITH unused AS (SELECT 1 AS x ORDER BY x) \
             SELECT CAST(value % 2 AS BIGINT) AS code \
             FROM sequence(start => 18446744073709551614)",
        ),
        (
            "shared_scan",
            "WITH s AS (\
                 SELECT CAST(value % 2 AS BIGINT) AS code \
                 FROM sequence(start => 18446744073709551614)\
             ), unused AS (SELECT code FROM s ORDER BY code) SELECT code FROM s",
        ),
    ] {
        let state = root.path().join(format!("{name}-state"));
        let target = root.path().join(format!("{name}.sqlite"));
        let program = capability_program(&target, query);
        let definition = start_midway(&program, &state, &schema, 1);
        finish(&program, &state);
        assert_eq!(codes(&target), [0, 1]);
        assert_completed_reopen(&program, &state, &definition);
        assert_eq!(codes(&target), [0, 1]);
    }
}

#[test]
fn reachable_sorts_are_rejected_during_start_without_creating_state_or_target() {
    let root = tempfile::tempdir().unwrap();
    let queries = [
        "SELECT value FROM sequence(start => 0) ORDER BY value",
        "SELECT value FROM (SELECT value FROM sequence(start => 0) ORDER BY value) AS q",
        "WITH q AS (SELECT value FROM sequence(start => 0) ORDER BY value) SELECT value FROM q",
        "SELECT value FROM (SELECT value FROM \
         (SELECT value FROM sequence(start => 0) ORDER BY value) AS a) AS b",
        "SELECT value FROM (SELECT value FROM sequence(start => 0) \
         ORDER BY value LIMIT 1) AS q",
        "SELECT value FROM sequence(start => 0) |> ORDER BY value DESC",
        "SELECT value FROM (SELECT value FROM sequence(start => 0) \
         |> ORDER BY value DESC) AS q",
    ];
    for (index, query) in queries.into_iter().enumerate() {
        let parent = root.path().join(format!("parent-{index}"));
        let state = parent.join("state");
        let target = root.path().join(format!("{index}.sqlite"));
        let program = capability_program(&target, query);
        assert!(matches!(
            program.start(&state),
            Err(SqlError::Unsupported(_))
        ));
        assert!(parent.is_dir());
        assert!(!state.exists());
        assert!(!target.exists());
    }
}

#[test]
fn ignored_pipe_order_locks_and_alias_types_are_rejected_during_parse() {
    for query in [
        "SELECT value FROM sequence(start => 0) \
         |> AGGREGATE COUNT(*) AS n GROUP BY value DESC",
        "SELECT value FROM sequence(start => 0) \
         |> AGGREGATE COUNT(*) AS n ASC GROUP BY value",
        "WITH s AS (SELECT value FROM sequence(start => 0)), \
         unused AS (SELECT (SELECT value FROM s FOR UPDATE) AS x FROM s) \
         SELECT value FROM s",
        "WITH unused AS (SELECT * FROM UNNEST(CAST(NULL AS BIGINT[])) AS t(x TEXT)) \
         SELECT value FROM sequence(start => 18446744073709551615)",
    ] {
        let result = SqlProgram::parse(&format!("INSERT INTO discard() {query}"));
        assert!(
            matches!(&result, Err(SqlError::Unsupported(_))),
            "{query}: {:?}",
            result.map(|_| ())
        );
    }
}

#[test]
fn long_unsupported_values_and_sort_diagnostics_remain_short() {
    let root = tempfile::tempdir().unwrap();
    let literal = "x".repeat(64 * 1024);
    for (index, query) in [
        format!("VALUES ('{literal}')"),
        format!("SELECT value FROM sequence(start => 0) ORDER BY '{literal}'"),
    ]
    .into_iter()
    .enumerate()
    {
        let state = root.path().join(format!("parent-{index}/state"));
        let target = root.path().join(format!("{index}.sqlite"));
        let program = capability_program(&target, &query);
        let Err(SqlError::Unsupported(message)) = program.start(&state) else {
            panic!("unsupported large plan must be rejected by lowering");
        };
        assert_eq!(message, "relational plan node");
        assert!(message.len() < 64);
        assert!(!message.contains(&literal));
        assert!(!state.exists());
        assert!(!target.exists());
    }
}

fn capability_program(target: &Path, query: &str) -> SqlProgram {
    let path = sql_string(target);
    SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{path}', table => 'result') {query}"
    ))
    .unwrap()
}

fn grouped_schema() -> Schema {
    Schema::new(vec![
        Field::new("parity", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, false),
    ])
}

fn start_midway(program: &SqlProgram, state: &Path, schema: &Schema, rounds: usize) -> Vec<u8> {
    let mut flow = program.start(state).unwrap();
    for _ in 0..rounds {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    }
    drop(flow);
    let definition = read_definition(state);
    assert_sink_schema(&definition, schema);
    definition
}

fn finish(program: &SqlProgram, state: &Path) {
    let mut flow = program.start(state).unwrap();
    assert_restores_to_idle(&mut flow);
}

fn assert_completed_reopen(program: &SqlProgram, state: &Path, definition: &[u8]) {
    let mut flow = program.start(state).unwrap();
    for _ in 0..=flow.operation_count() {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Idle);
    }
    assert_eq!(flow.status().unwrap().depth, 0);
    drop(flow);
    assert_eq!(read_definition(state), definition);
}

fn read_definition(state: &Path) -> Vec<u8> {
    let store = Store::open(state).unwrap();
    let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let read = store.read_transaction();
    definition
        .read(read.access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap()
}

fn assert_sink_schema(encoded: &[u8], expected: &Schema) {
    let magic = b"dogpaddle.flow\0";
    let header = magic.len() + 2;
    assert!(encoded.len() >= header + 4);
    assert_eq!(&encoded[..magic.len()], magic);
    assert_eq!(&encoded[magic.len()..header], &[0, 1]);
    let plan: serde_json::Value =
        serde_json::from_slice(&encoded[header..encoded.len() - 4]).unwrap();
    let mut schemas: Vec<Option<SchemaRef>> = Vec::new();
    for node in plan["operations"].as_array().unwrap() {
        let inputs = node["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|input| {
                let index = usize::try_from(input.as_u64().unwrap()).unwrap();
                Arc::clone(schemas[index].as_ref().unwrap())
            })
            .collect::<Vec<_>>();
        let definition: OperationDefinition =
            serde_json::from_str(&node["definition"].to_string()).unwrap();
        if node["id"] == "sql/sink" {
            assert!(matches!(&definition, OperationDefinition::SqliteSink(_)));
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].as_ref(), expected);
            assert!(definition.output_schema(&inputs).unwrap().is_none());
            return;
        }
        schemas.push(definition.output_schema(&inputs).unwrap());
    }
    panic!("persisted SQL definition is missing its sink");
}

fn grouped_rows(target: &Path) -> Vec<(i64, i64)> {
    sqlite(target)
        .prepare("SELECT parity, row_count FROM result ORDER BY parity, row_count")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn nullable_codes(target: &Path) -> Vec<Option<i64>> {
    sqlite(target)
        .prepare("SELECT \"value.code\" FROM result ORDER BY \"value.code\"")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn named_rows(target: &Path) -> Vec<(i64, Option<String>)> {
    sqlite(target)
        .prepare("SELECT \"value.code\", label FROM result ORDER BY \"value.code\", label")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn codes(target: &Path) -> Vec<i64> {
    sqlite(target)
        .prepare("SELECT code FROM result ORDER BY code")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn operation_ids(flow: &Flow) -> Vec<String> {
    flow.operation_ids().map(str::to_owned).collect()
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

fn asof_rows(path: &Path) -> Vec<(u64, Option<u64>)> {
    let connection = sqlite(path);
    connection
        .prepare("SELECT left_ts, right_ts FROM asof_rows ORDER BY left_ts")
        .unwrap()
        .query_map([], |row| {
            Ok((
                decode_u64(row.get::<_, Vec<u8>>(0)?),
                row.get::<_, Option<Vec<u8>>>(1)?.map(decode_u64),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn assert_initial_asof_state(path: &Path, operation: usize) {
    use dogpaddle_store::{OrderedMap, ScanDirection, ScanLimit, Store};
    use std::num::NonZeroU64;

    let store = Store::open(path).unwrap();
    let read = store.read_transaction();
    for (side, expected) in [("left", 1), ("right", 0)] {
        let rows: OrderedMap<Vec<u8>, NonZeroU64> = store
            .open_data(&format!("operation/{operation:08x}/asof_join.{side}_index"))
            .unwrap();
        let page = rows
            .read(read.access())
            .unwrap()
            .scan(
                ..,
                ScanDirection::Ascending,
                None,
                ScanLimit::new(2, 1024).unwrap(),
            )
            .unwrap();
        assert_eq!(page.entries.len(), expected, "{side} ASOF state");
        assert!(page.continuation.is_none());
    }
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

fn advance_until_quickstart_rows(
    flow: &mut Flow,
    path: &Path,
    minimum_rows: usize,
) -> Vec<(i64, i64, String)> {
    for _ in 0..128 {
        assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
        if path.exists() {
            let rows = quickstart_rows(path);
            if rows.len() >= minimum_rows {
                return rows;
            }
        }
    }
    panic!("quickstart target did not reach {minimum_rows} rows within 128 advances");
}

fn expected_quickstart_rows(rows: usize) -> Vec<(i64, i64, String)> {
    (0..rows)
        .map(|index| {
            let number = i64::try_from(index * 2).unwrap();
            let size = if number >= 10 { "large" } else { "small" };
            (number, number * number, size.to_owned())
        })
        .collect()
}

fn assert_restores_to_idle(flow: &mut Flow) {
    advance_to_idle(flow);
    assert_eq!(flow.status().unwrap().depth, 0);
}

fn advance_to_idle(flow: &mut Flow) {
    let mut idle = 0;
    for _ in 0..512 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            idle += 1;
        } else {
            idle = 0;
        }
        if idle > flow.operation_count() {
            return;
        }
    }
    panic!("finite SQL flow did not drain");
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
