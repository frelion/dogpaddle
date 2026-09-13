use dogpaddle_flow::FlowError;
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
            connection => env('DOGPADDLE_TARGET_DATABASE_URL'),
            table => 'target.orders'
        )
        SELECT id, amount
        FROM postgres_cdc(
            connection => env('DOGPADDLE_SOURCE_DATABASE_URL'),
            table => 'public.orders',
            publication => 'orders_publication',
            connect_timeout_ms => 5000,
            query_timeout_ms => 6000,
            retry_limit => 12,
            retry_max_delay_ms => 12000,
            heartbeat_interval_ms => 2000,
            snapshot_fetch_size => 4096
        ) AS orders
    ";
    SqlProgram::parse(postgres).unwrap();

    let mysql = r"
        INSERT INTO discard()
        SELECT id, amount
        FROM mysql_cdc(
            connection => env('DOGPADDLE_SOURCE_DATABASE_URL'),
            table => 'app.orders',
            connect_timeout_ms => 5000,
            query_timeout_ms => 6000,
            retry_limit => 12,
            retry_max_delay_ms => 12000,
            heartbeat_interval_ms => 2000,
            snapshot_fetch_size => 4096
        ) AS orders
    ";
    SqlProgram::parse(mysql).unwrap();

    SqlProgram::parse("INSERT INTO discard() SELECT value FROM sequence(start => 0)").unwrap();
}

#[test]
fn parse_accepts_the_equality_join_family() {
    for (keyword, selected) in [
        ("JOIN", "left_scan.value"),
        ("INNER JOIN", "left_scan.value"),
        ("LEFT JOIN", "left_scan.value"),
        ("LEFT OUTER JOIN", "left_scan.value"),
        ("RIGHT JOIN", "right_scan.value"),
        ("RIGHT OUTER JOIN", "right_scan.value"),
        ("FULL JOIN", "left_scan.value"),
        ("FULL OUTER JOIN", "right_scan.value"),
        ("LEFT SEMI JOIN", "left_scan.value"),
        ("LEFT ANTI JOIN", "left_scan.value"),
        ("RIGHT SEMI JOIN", "right_scan.value"),
        ("RIGHT ANTI JOIN", "right_scan.value"),
    ] {
        SqlProgram::parse(&format!(
            "INSERT INTO discard() \
             SELECT {selected} AS selected_value \
             FROM sequence(start => 0) AS left_scan \
             {keyword} sequence(start => 1) AS right_scan \
             ON left_scan.value + 1 = right_scan.value \
             AND left_scan.value = right_scan.value - 1"
        ))
        .unwrap();
    }
}

#[test]
fn parse_accepts_an_inner_residual_after_an_equality_key() {
    SqlProgram::parse(
        "INSERT INTO discard() \
         SELECT left_scan.value AS left_value, right_scan.value AS right_value \
         FROM sequence(start => 0) AS left_scan \
         JOIN sequence(start => 1) AS right_scan \
         ON left_scan.value = right_scan.value \
         AND left_scan.value + right_scan.value > 0",
    )
    .unwrap();
}

#[test]
fn parse_rejects_join_constraints_and_conditions_outside_the_family() {
    let queries = [
        (
            "using",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             JOIN sequence(start => 1) AS right_scan USING (value)",
        ),
        (
            "natural join",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             NATURAL JOIN sequence(start => 1) AS right_scan",
        ),
        (
            "cross join",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             CROSS JOIN sequence(start => 1) AS right_scan",
        ),
        (
            "non-equality",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             JOIN sequence(start => 1) AS right_scan \
             ON left_scan.value < right_scan.value",
        ),
        (
            "left outer residual",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             LEFT JOIN sequence(start => 1) AS right_scan \
             ON left_scan.value = right_scan.value AND left_scan.value > 0",
        ),
        (
            "left semi residual",
            "SELECT left_scan.value FROM sequence(start => 0) AS left_scan \
             LEFT SEMI JOIN sequence(start => 1) AS right_scan \
             ON left_scan.value = right_scan.value AND left_scan.value > 0",
        ),
        (
            "right anti residual",
            "SELECT right_scan.value FROM sequence(start => 0) AS left_scan \
             RIGHT ANTI JOIN sequence(start => 1) AS right_scan \
             ON left_scan.value = right_scan.value AND right_scan.value > 0",
        ),
    ];

    for (case, query) in queries {
        assert!(
            SqlProgram::parse(&format!("INSERT INTO discard() {query}")).is_err(),
            "accepted {case}"
        );
    }
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
            "missing PostgreSQL publication",
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders'\
            )",
        ),
        (
            "missing MySQL table",
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app'\
            )",
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
fn parse_rejects_every_removed_endpoint_argument() {
    let postgres_cdc_arguments = [
        ("engine_name", "'orders_scan'"),
        ("runtime_bundle", "'/opt/dogpaddle/debezium'"),
        ("host", "'127.0.0.1'"),
        ("port", "5432"),
        ("database", "'app'"),
        ("user", "'dogpaddle'"),
        ("password", "'secret'"),
        ("schema", "'public'"),
        ("slot", "'orders_slot'"),
    ];
    for (name, value) in postgres_cdc_arguments {
        let sql = format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders', publication => 'orders_publication', \
                {name} => {value}\
            )"
        );
        assert!(SqlProgram::parse(&sql).is_err(), "accepted {name}");
    }

    let mysql_cdc_arguments = [
        ("engine_name", "'orders_scan'"),
        ("runtime_bundle", "'/opt/dogpaddle/debezium'"),
        ("host", "'127.0.0.1'"),
        ("port", "3306"),
        ("database", "'app'"),
        ("user", "'dogpaddle'"),
        ("password", "'secret'"),
        ("replication_client_id", "5401"),
    ];
    for (name, value) in mysql_cdc_arguments {
        let sql = format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.orders', {name} => {value}\
            )"
        );
        assert!(SqlProgram::parse(&sql).is_err(), "accepted {name}");
    }

    let postgres_sink_arguments = [
        ("sink_id", "'orders_sink'"),
        ("host", "'127.0.0.1'"),
        ("port", "5432"),
        ("database", "'app'"),
        ("user", "'dogpaddle'"),
        ("password", "'secret'"),
        ("schema", "'public'"),
    ];
    for (name, value) in postgres_sink_arguments {
        let sql = format!(
            "INSERT INTO postgres(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders', {name} => {value}\
             ) SELECT value FROM sequence(start => 0)"
        );
        assert!(SqlProgram::parse(&sql).is_err(), "accepted {name}");
    }
}

#[test]
fn start_rejects_zero_bootstrap_spool_capacity_before_source_io() {
    let root = tempfile::tempdir().unwrap();
    let programs = [
        "INSERT INTO discard() SELECT * FROM postgres_cdc(\
            connection => 'postgresql://user:secret@127.0.0.1/app', \
            table => 'public.orders', publication => 'orders_publication', \
            bootstrap_spool_bytes => 0\
        )",
        "INSERT INTO discard() SELECT * FROM mysql_cdc(\
            connection => 'mysql://user:secret@127.0.0.1/app', table => 'app.orders', \
            bootstrap_spool_bytes => 0\
        )",
    ];

    for (index, sql) in programs.iter().enumerate() {
        let flow_path = root.path().join(format!("flow-{index}"));
        let program = SqlProgram::parse(sql).unwrap();
        assert!(program.start(&flow_path).is_err());
        assert!(!flow_path.exists());
    }
}

#[test]
fn start_rejects_invalid_cdc_tuning_before_source_or_state_io() {
    let root = tempfile::tempdir().unwrap();
    let common = [
        ("connect_timeout_ms", "0"),
        ("query_timeout_ms", "0"),
        ("query_timeout_ms", "2147483001"),
        ("retry_limit", "2147483648"),
        ("retry_max_delay_ms", "300"),
        ("heartbeat_interval_ms", "0"),
        ("snapshot_fetch_size", "0"),
    ];
    let mut programs = Vec::new();
    for (name, value) in common {
        programs.push(format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.orders', publication => 'orders_publication', \
                {name} => {value}\
            )"
        ));
        programs.push(format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.orders', {name} => {value}\
            )"
        ));
    }

    for (index, sql) in programs.iter().enumerate() {
        let state_path = root.path().join(format!("invalid-tuning-{index}"));
        let program = SqlProgram::parse(sql).unwrap();
        let Err(error) = program.start(&state_path) else {
            panic!("invalid CDC tuning unexpectedly started")
        };
        assert!(matches!(error, SqlError::Invalid(_)));
        assert!(error.to_string().contains(common[index / 2].0));
        assert!(!state_path.exists());
    }
}

#[test]
fn unknown_cdc_tuning_reports_the_supported_sql_vocabulary() {
    let Err(error) = SqlProgram::parse(
        "INSERT INTO discard() SELECT * FROM postgres_cdc(\
            connection => 'postgresql://user:secret@127.0.0.1/app', \
            table => 'public.orders', publication => 'orders_publication', \
            heartbeat_intervl_ms => 1000\
        )",
    ) else {
        panic!("misspelled CDC tuning unexpectedly parsed")
    };
    let message = error.to_string();
    assert!(message.contains("unknown postgres_cdc parameter \"heartbeat_intervl_ms\""));
    assert!(message.contains("\"heartbeat_interval_ms\""));
    assert!(!message.contains("secret"));
}

#[test]
fn start_rejects_malformed_database_urls_and_qualified_tables_before_source_io() {
    let root = tempfile::tempdir().unwrap();
    let programs = [
        (
            "PostgreSQL CDC URL",
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'not-a-url', table => 'public.orders', \
                publication => 'orders_publication'\
            )",
        ),
        (
            "MySQL CDC URL",
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'app.orders'\
            )",
        ),
        (
            "PostgreSQL sink URL",
            "INSERT INTO postgres(connection => 'postgresql:///app', table => 'public.orders') \
             SELECT value FROM sequence(start => 0)",
        ),
        (
            "unqualified PostgreSQL CDC table",
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'orders', publication => 'orders_publication'\
            )",
        ),
        (
            "over-qualified PostgreSQL sink table",
            "INSERT INTO postgres(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'catalog.public.orders'\
             ) SELECT value FROM sequence(start => 0)",
        ),
        (
            "MySQL table from another database",
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'other.orders'\
            )",
        ),
    ];

    for (index, (case, sql)) in programs.into_iter().enumerate() {
        let state_path = root.path().join(format!("invalid-endpoint-{index}"));
        let program = SqlProgram::parse(sql).unwrap();
        assert!(program.start(&state_path).is_err(), "started {case}");
        assert!(!state_path.exists(), "{case} created state");
    }
}

#[test]
fn start_rejects_missing_environment_values_before_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let variable = "DOGPADDLE_SQL_CORRECTNESS_MISSING_7F930A9E";
    assert!(std::env::var_os(variable).is_none());
    let sql = format!(
        "INSERT INTO sqlite(path => env('{variable}'), table => 'events') \
         SELECT value FROM sequence(start => 0)"
    );
    let program = SqlProgram::parse(&sql).unwrap();

    assert!(program.start(&flow_path).is_err());
    assert!(!flow_path.exists());
}

#[test]
fn start_creates_state_then_resumes_it_and_rejects_a_mismatched_program() {
    let root = tempfile::tempdir().unwrap();
    let state_path = root.path().join("state");
    let original = SqlProgram::parse(
        "INSERT INTO discard() SELECT value FROM sequence(start => 0) WHERE value < 2",
    )
    .unwrap();

    drop(original.start(&state_path).unwrap());
    assert!(state_path.exists());
    drop(original.start(&state_path).unwrap());

    let replacement = SqlProgram::parse(
        "INSERT INTO discard() SELECT value FROM sequence(start => 1) WHERE value < 2",
    )
    .unwrap();
    assert!(matches!(
        replacement.start(&state_path),
        Err(SqlError::Flow(FlowError::OwnerIdentityMismatch))
    ));

    drop(original.start(&state_path).unwrap());
}

#[test]
fn start_never_rebuilds_an_existing_incomplete_state_directory() {
    let root = tempfile::tempdir().unwrap();
    let state_path = root.path().join("incomplete-state");
    let sqlite_path = root.path().join("must-not-exist.sqlite");
    std::fs::create_dir(&state_path).unwrap();
    let program = SqlProgram::parse(&format!(
        "INSERT INTO sqlite(path => '{}', table => 'events') \
         SELECT value FROM sequence(start => 0)",
        sqlite_path.display()
    ))
    .unwrap();

    assert!(program.start(&state_path).is_err());
    assert!(state_path.is_dir());
    assert!(!sqlite_path.exists());
}

#[test]
fn resolved_environment_values_are_not_in_errors() {
    let root = tempfile::tempdir().unwrap();
    let flow_path = root.path().join("flow");
    let resolved = std::env::var("PATH").expect("Cargo tests have PATH");
    let program = SqlProgram::parse(
        "INSERT INTO postgres(\
            connection => env('PATH'), table => 'public.events'\
         ) SELECT value FROM sequence(start => 0)",
    )
    .unwrap();

    let Err(error) = program.start(&flow_path) else {
        panic!("invalid PostgreSQL connection started a Flow");
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
            assert!(program.start(&flow_path).is_err(), "started {case}");
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
        assert!(program.start(&flow_path).is_err(), "started {case}");
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
            "same-side join equality",
            "SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan \
             JOIN sequence(start => 1) AS right_scan \
             ON left_scan.value = left_scan.value",
        ),
        (
            "correlated exists",
            "SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan \
             WHERE EXISTS (\
                 SELECT right_scan.value \
                 FROM sequence(start => 1) AS right_scan \
                 WHERE right_scan.value = left_scan.value\
             )",
        ),
        (
            "in subquery",
            "SELECT left_scan.value \
             FROM sequence(start => 0) AS left_scan \
             WHERE left_scan.value IN (\
                 SELECT right_scan.value \
                 FROM sequence(start => 1) AS right_scan\
             )",
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
            assert!(program.start(&flow_path).is_err(), "started {case}");
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
            assert!(program.start(&flow_path).is_err(), "started {case}");
        }
        assert!(!flow_path.exists(), "{case} created a Flow path");
    }
}

#[test]
fn start_resolves_every_endpoint_parameter_before_selecting_state_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let variable = "DOGPADDLE_SQL_CORRECTNESS_START_MISSING_91F6E33D";
    assert!(std::env::var_os(variable).is_none());
    let mut programs = vec![
        format!("INSERT INTO discard() SELECT value FROM sequence(start => env('{variable}'))"),
        format!(
            "INSERT INTO sqlite(path => env('{variable}'), table => 'events') \
             SELECT value FROM sequence(start => 0)"
        ),
        format!(
            "INSERT INTO sqlite(path => '/tmp/events.sqlite', table => env('{variable}')) \
             SELECT value FROM sequence(start => 0)"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => env('{variable}'), table => 'public.events', \
                publication => 'events_publication'\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => env('{variable}'), publication => 'events_publication'\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.events', publication => env('{variable}')\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.events', publication => 'events_publication', \
                bootstrap_spool_bytes => env('{variable}')\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => env('{variable}'), table => 'app.events'\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => env('{variable}')\
            )"
        ),
        format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.events', bootstrap_spool_bytes => env('{variable}')\
            )"
        ),
        format!(
            "INSERT INTO postgres(connection => env('{variable}'), table => 'public.events') \
             SELECT value FROM sequence(start => 0)"
        ),
        format!(
            "INSERT INTO postgres(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => env('{variable}')\
             ) SELECT value FROM sequence(start => 0)"
        ),
    ];

    for parameter in [
        "connect_timeout_ms",
        "query_timeout_ms",
        "retry_limit",
        "retry_max_delay_ms",
        "heartbeat_interval_ms",
        "snapshot_fetch_size",
    ] {
        programs.push(format!(
            "INSERT INTO discard() SELECT * FROM postgres_cdc(\
                connection => 'postgresql://user:secret@127.0.0.1/app', \
                table => 'public.events', publication => 'events_publication', \
                {parameter} => env('{variable}')\
            )"
        ));
        programs.push(format!(
            "INSERT INTO discard() SELECT * FROM mysql_cdc(\
                connection => 'mysql://user:secret@127.0.0.1/app', \
                table => 'app.events', {parameter} => env('{variable}')\
            )"
        ));
    }

    for (index, sql) in programs.iter().enumerate() {
        let flow_path = root.path().join(format!("flow-{index}"));
        let program = SqlProgram::parse(sql).unwrap();
        assert!(
            matches!(program.start(&flow_path), Err(SqlError::Environment { name }) if name == variable)
        );
        assert!(!flow_path.exists());
    }
}
