# dogpaddle

`dogpaddle` is the command-line entry point for running one durable streaming SQL program.

Release archives contain this binary at `bin/dogpaddle` and its self-contained
Debezium/JRE runtime at `libexec/dogpaddle/debezium`. The archive can be run
from any extracted location without Rust or system Java.

```console
dogpaddle run orders.sql
dogpaddle run orders.sql --state /var/lib/dogpaddle/orders
```

Without `--state`, `orders.sql` uses `.dogpaddle/orders` beside the SQL file. The command prints the canonical state path after it has built or reopened the program. A missing path is built once; an existing path is always reopened and must belong to the same SQL program.

The process advances the Flow until interrupted. `Ctrl-C` lets the current bounded scheduling round finish and then exits. A runtime failure that makes the in-memory Flow unsafe to continue is reported with a hint to rerun the same command; the command never retries or rebuilds state automatically.

The binary has no library, runner, or second lifecycle. It passes explicit state paths directly to `SqlProgram::start`.
`Progressed` immediately starts the next round; `Idle` and `Backpressured` use a fixed short wait. It provides no Table, View, Catalog, background scheduler, cost optimizer, or second execution engine.
