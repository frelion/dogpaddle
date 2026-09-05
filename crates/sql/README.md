# dogpaddle-sql

`dogpaddle-sql` turns one SQL file into one persistent `DogPaddle` Flow. A program
is a direct `INSERT INTO sink(...)` followed by a `SELECT` query or CTE query:

```sql
INSERT INTO sqlite(path => '/tmp/numbers.sqlite', table => 'numbers')
SELECT value
FROM sequence(start => 0)
WHERE value < 10
```

V1 exposes `sequence` and `postgres_cdc` scans, and `sqlite`, `postgres`, and
`discard` sinks. Endpoint arguments use `name => value`; values are
single-quoted strings, non-negative integers, or `env('NAME')`. Credentials and
runtime bundle paths remain runtime resources.

```no_run
use dogpaddle_sql::SqlProgram;

let program = SqlProgram::read("flow.sql")?;
let mut flow = program.build("/var/lib/dogpaddle/flow")?;
flow.advance()?;

drop(flow);
let mut flow = program.open("/var/lib/dogpaddle/flow")?;
flow.advance()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

[`SqlProgram::build`](SqlProgram::build) discovers external schemas and targets,
uses `DataFusion` to bind SQL expressions, lowers the supported streaming subset
to `DogPaddle` Operations, and builds the Flow. [`SqlProgram::open`](SqlProgram::open)
loads topology and schemas from the persisted Flow and injects only the runtime
resources declared by the SQL program.

The relational subset is intentionally small: table scans, filters, projections,
derived queries, non-recursive CTEs, `UNION ALL`, casts, `TRY_CAST`, and `CASE`.
Aggregation, joins, sorting, limits, distinct, windows, ordinary tables,
subqueries in expressions, table sampling, hints, row locks, function
registries, and `DataFusion` physical execution are outside V1.

Run the offline contract with `cargo test -p dogpaddle-sql --test correctness`.
`system-tests/postgres/check_sql.py` is the explicit real-PostgreSQL recovery gate.
