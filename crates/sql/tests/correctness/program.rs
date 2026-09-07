use dogpaddle_sql::{SqlError, SqlProgram};

#[test]
fn bundled_sql_examples_parse_through_the_public_file_api() {
    for file in ["quickstart.sql", "fulfillment.sql"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join(file);
        SqlProgram::read(path).unwrap();
    }
}

#[test]
fn parse_accepts_every_v1_scan_and_sink_endpoint() {
    let postgres = r"
        INSERT INTO postgres(
            sink_id => 'orders_copy',
            host => '127.0.0.1',
            port => 5432,
            database => 'app',
            user => 'dogpaddle',
            password => env('DOGPADDLE_SQL_TEST_PASSWORD'),
            schema => 'target',
            table => 'orders'
        )
        SELECT id, amount
        FROM postgres_cdc(
            engine_name => 'orders_scan',
            runtime_bundle => '/opt/dogpaddle/debezium',
            host => '127.0.0.1',
            port => 5432,
            database => 'app',
            user => 'dogpaddle',
            password => env('DOGPADDLE_SQL_TEST_PASSWORD'),
            schema => 'public',
            table => 'orders',
            slot => 'orders_slot',
            publication => 'orders_publication'
        ) AS orders
    ";
    SqlProgram::parse(postgres).unwrap();

    SqlProgram::parse("INSERT INTO discard() SELECT value FROM sequence(start => 0)").unwrap();
}

#[test]
fn parse_rejects_non_programs_and_invalid_endpoint_arguments() {
    let cases = [
        ("bare query", "SELECT 1"),
        (
            "multiple statements",
            "INSERT INTO discard() SELECT value FROM sequence(start => 0); \
             INSERT INTO discard() SELECT value FROM sequence(start => 1)",
        ),
        (
            "unknown sink",
            "INSERT INTO nowhere() SELECT value FROM sequence(start => 0)",
        ),
        (
            "unknown scan",
            "INSERT INTO discard() SELECT * FROM nowhere(start => 0)",
        ),
        (
            "missing argument",
            "INSERT INTO discard() SELECT value FROM sequence()",
        ),
        (
            "duplicate argument",
            "INSERT INTO discard() SELECT value FROM sequence(start => 0, start => 1)",
        ),
        (
            "unknown argument",
            "INSERT INTO discard() SELECT value FROM sequence(start => 0, step => 1)",
        ),
        (
            "positional argument",
            "INSERT INTO discard() SELECT value FROM sequence(0)",
        ),
        (
            "wrong named-argument operator",
            "INSERT INTO discard() SELECT value FROM sequence(start := 0)",
        ),
        (
            "non-constant argument",
            "INSERT INTO discard() SELECT value FROM sequence(start => 1 + 1)",
        ),
        (
            "negative integer",
            "INSERT INTO discard() SELECT value FROM sequence(start => -1)",
        ),
        (
            "sink argument",
            "INSERT INTO discard(reason => 'test') SELECT value \
             FROM sequence(start => 0)",
        ),
        (
            "reserved relation",
            "INSERT INTO discard() \
             WITH hidden AS (SELECT value FROM sequence(start => 0)) \
             SELECT * FROM __dogpaddle_sql_scan_00000000",
        ),
    ];

    for (case, sql) in cases {
        assert!(SqlProgram::parse(sql).is_err(), "accepted {case}");
    }
}

#[test]
fn build_rejects_missing_environment_values_before_creating_the_flow() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let variable = "DOGPADDLE_SQL_CORRECTNESS_MISSING_7F930A9E";
    assert!(std::env::var_os(variable).is_none());
    let sql = format!(
        "INSERT INTO sqlite(path => env('{variable}'), table => 'events') \
         SELECT value FROM sequence(start => 0)"
    );
    let program = SqlProgram::parse(&sql).unwrap();

    assert!(program.build(&flow_path).is_err());
    assert!(!flow_path.exists());
}

#[test]
fn resolved_environment_values_are_not_in_errors() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let resolved = std::env::var("PATH").expect("Cargo tests have PATH");
    let program = SqlProgram::parse(
        "INSERT INTO postgres(\
            sink_id => 'sink', host => 'not-an-ip', port => 5432, database => 'app', \
            user => 'dogpaddle', password => env('PATH'), schema => 'public', table => 'events'\
         ) SELECT value FROM sequence(start => 0)",
    )
    .unwrap();

    let Err(error) = program.build(&flow_path) else {
        panic!("invalid PostgreSQL host built a Flow");
    };
    assert!(!error.to_string().contains(&resolved));
    assert!(!flow_path.exists());
}

#[test]
fn read_reports_a_missing_sql_file() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing.sql");

    assert!(SqlProgram::read(&missing).is_err());
}

#[test]
fn unsupported_relational_plans_fail_without_creating_a_flow() {
    let queries = [
        ("ordinary table", "SELECT * FROM ordinary_table"),
        (
            "join",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             JOIN sequence(start => 0) AS right_scan \
             ON left_scan.value = right_scan.value",
        ),
        (
            "sort",
            "SELECT value FROM sequence(start => 0) ORDER BY value",
        ),
        ("limit", "SELECT value FROM sequence(start => 0) LIMIT 1"),
        (
            "union distinct",
            "SELECT value FROM sequence(start => 0) \
             UNION SELECT value FROM sequence(start => 0)",
        ),
        ("values", "SELECT value FROM (VALUES (1)) AS rows(value)"),
        (
            "window",
            "SELECT row_number() OVER () FROM sequence(start => 0)",
        ),
        (
            "scalar subquery",
            "SELECT value, (SELECT 1) FROM sequence(start => 0)",
        ),
        (
            "correlated subquery",
            "SELECT outer_scan.value FROM sequence(start => 0) AS outer_scan \
             WHERE EXISTS (\
                 SELECT 1 FROM sequence(start => 0) AS inner_scan \
                 WHERE inner_scan.value = outer_scan.value\
             )",
        ),
        (
            "recursive CTE",
            "WITH RECURSIVE numbers(value) AS (\
                 SELECT value FROM sequence(start => 0) \
                 UNION ALL \
                 SELECT value FROM numbers\
             ) \
             SELECT value FROM numbers",
        ),
        (
            "function call",
            "SELECT abs(value) FROM sequence(start => 0)",
        ),
        (
            "current time",
            "SELECT CURRENT_TIMESTAMP FROM sequence(start => 0)",
        ),
        (
            "random function",
            "SELECT random() FROM sequence(start => 0)",
        ),
        (
            "session variable",
            "SELECT @@version FROM sequence(start => 0)",
        ),
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, (case, query)) in queries.into_iter().enumerate() {
        let flow_path = root.path().join(format!("flow-{index}"));
        let sql = format!("INSERT INTO discard() {query}");
        if let Ok(program) = SqlProgram::parse(&sql) {
            assert!(program.build(&flow_path).is_err(), "built {case}");
        }
        assert!(!flow_path.exists(), "{case} created a Flow path");
    }
}

#[test]
fn unsupported_aggregate_forms_fail_without_creating_a_flow() {
    let queries = [
        (
            "global aggregate",
            "SELECT COUNT(*) FROM sequence(start => 0)",
        ),
        (
            "grouping sets",
            "SELECT value, COUNT(*) FROM sequence(start => 0) \
             GROUP BY GROUPING SETS ((value))",
        ),
        (
            "aggregate distinct",
            "SELECT value % 2, COUNT(DISTINCT value) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "aggregate filter",
            "SELECT value % 2, COUNT(*) FILTER (WHERE value > 0) \
             FROM sequence(start => 0) GROUP BY value % 2",
        ),
        (
            "aggregate order",
            "SELECT value % 2, COUNT(value ORDER BY value) \
             FROM sequence(start => 0) GROUP BY value % 2",
        ),
        (
            "unregistered aggregate",
            "SELECT value % 2, MEDIAN(value) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "aggregate arity",
            "SELECT value % 2, SUM(value, value) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "floating average",
            "SELECT value % 2, AVG(CAST(value AS DOUBLE)) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "floating sum",
            "SELECT value % 2, SUM(CAST(value AS DOUBLE)) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "floating group key",
            "SELECT CAST(value AS DOUBLE), COUNT(*) FROM sequence(start => 0) \
             GROUP BY CAST(value AS DOUBLE)",
        ),
        (
            "floating minimum",
            "SELECT value % 2, MIN(CAST(value AS DOUBLE)) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
        (
            "floating maximum",
            "SELECT value % 2, MAX(CAST(value AS DOUBLE)) FROM sequence(start => 0) \
             GROUP BY value % 2",
        ),
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, (case, query)) in queries.into_iter().enumerate() {
        let flow_path = root.path().join(format!("aggregate-{index}"));
        let program = SqlProgram::parse(&format!("INSERT INTO discard() {query}")).unwrap();
        assert!(program.build(&flow_path).is_err(), "built {case}");
        assert!(!flow_path.exists(), "{case} created a Flow path");
    }
}

#[test]
fn invalid_plan_shapes_fail_without_creating_a_flow() {
    let queries = [
        ("query without a scan", "SELECT 1"),
        (
            "cross join",
            "SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan, \
                  sequence(start => 0) AS right_scan",
        ),
        (
            "union field count",
            "SELECT value FROM sequence(start => 0) \
             UNION ALL \
             SELECT value, value FROM sequence(start => 0)",
        ),
        (
            "union incompatible types",
            "SELECT value FROM sequence(start => 0) \
             UNION ALL \
             SELECT value = 0 FROM sequence(start => 0)",
        ),
        (
            "duplicate output field",
            "SELECT value AS duplicate, value AS duplicate FROM sequence(start => 0)",
        ),
        (
            "reserved output field",
            "SELECT value AS \"$dogpaddle.diff\" FROM sequence(start => 0)",
        ),
        (
            "unreachable scan",
            "WITH \
                 unused AS (SELECT value FROM sequence(start => 0)), \
                 used AS (SELECT value FROM sequence(start => 1)) \
             SELECT value FROM used",
        ),
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, (case, query)) in queries.into_iter().enumerate() {
        let flow_path = root.path().join(format!("invalid-plan-{index}"));
        let sql = format!("INSERT INTO discard() {query}");
        if let Ok(program) = SqlProgram::parse(&sql) {
            assert!(program.build(&flow_path).is_err(), "built {case}");
        }
        assert!(!flow_path.exists(), "{case} created a Flow path");
    }
}

#[test]
fn ignored_sql_modifiers_are_rejected_before_creating_a_flow() {
    let queries = [
        ("select all", "SELECT ALL value FROM sequence(start => 0)"),
        (
            "distinct on",
            "SELECT DISTINCT ON (value) value FROM sequence(start => 0)",
        ),
        (
            "table sample",
            "SELECT value FROM sequence(start => 0) TABLESAMPLE SYSTEM (0)",
        ),
        (
            "ordinality",
            "SELECT value FROM sequence(start => 0) WITH ORDINALITY",
        ),
        (
            "table hint",
            "SELECT value FROM sequence(start => 0) WITH (NOLOCK)",
        ),
        (
            "row lock",
            "SELECT value FROM sequence(start => 0) FOR UPDATE",
        ),
        (
            "limit all",
            "SELECT value FROM sequence(start => 0) LIMIT ALL",
        ),
        (
            "offset",
            "SELECT value FROM sequence(start => 0) OFFSET 1 ROW",
        ),
        (
            "fetch",
            "SELECT value FROM sequence(start => 0) FETCH FIRST 1 ROW ONLY",
        ),
        (
            "materialized CTE",
            "WITH numbers AS MATERIALIZED (SELECT value FROM sequence(start => 0)) \
             SELECT value FROM numbers",
        ),
        (
            "typed scan alias",
            "SELECT number FROM sequence(start => 0) AS numbers(number BIGINT)",
        ),
        (
            "typed derived alias",
            "SELECT number FROM (SELECT value FROM sequence(start => 0)) \
             AS numbers(number BIGINT)",
        ),
        (
            "typed CTE alias",
            "WITH numbers(number BIGINT) AS (SELECT value FROM sequence(start => 0)) \
             SELECT number FROM numbers",
        ),
    ];

    let root = tempfile::tempdir().unwrap();
    for (index, (case, query)) in queries.into_iter().enumerate() {
        let flow_path = root.path().join(format!("modifier-{index}"));
        let sql = format!("INSERT INTO discard() {query}");
        if let Ok(program) = SqlProgram::parse(&sql) {
            assert!(program.build(&flow_path).is_err(), "built {case}");
        }
        assert!(!flow_path.exists(), "{case} created a Flow path");
    }
}

#[test]
fn open_resolves_every_endpoint_parameter_before_reading_the_flow() {
    let root = tempfile::tempdir().unwrap();
    let variable = "DOGPADDLE_SQL_CORRECTNESS_OPEN_MISSING_91F6E33D";
    assert!(std::env::var_os(variable).is_none());
    let programs = [
        format!("INSERT INTO discard() SELECT value FROM sequence(start => env('{variable}'))"),
        format!(
            "INSERT INTO sqlite(path => env('{variable}'), table => 'events') \
             SELECT value FROM sequence(start => 0)"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                engine_name => env('{variable}'), \
                runtime_bundle => '/tmp/dogpaddle-runtime', \
                host => '127.0.0.1', port => 5432, database => 'app', \
                user => 'dogpaddle', password => 'secret', schema => 'public', \
                table => 'events', slot => 'events_slot', publication => 'events_pub'\
            )"
        ),
        format!(
            "INSERT INTO postgres(\
                sink_id => env('{variable}'), host => '127.0.0.1', port => 5432, \
                database => 'app', user => 'dogpaddle', password => 'secret', \
                schema => 'public', table => 'events'\
             ) SELECT value FROM sequence(start => 0)"
        ),
    ];

    for (index, sql) in programs.iter().enumerate() {
        let flow_path = root.path().join(format!("flow-{index}"));
        let program = SqlProgram::parse(sql).unwrap();
        assert!(
            matches!(program.open(&flow_path), Err(SqlError::Environment { name }) if name == variable)
        );
        assert!(!flow_path.exists());
    }
}
