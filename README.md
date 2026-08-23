# DogPaddle

DogPaddle is built around three execution concepts:

- `Flow` owns a durable static DAG and fair scheduling;
- `Stage` owns one operation and one atomic execution boundary;
- `Operation` owns domain semantics; its durable state lives only in injected
  Store collections or its checkpoint.

Each successful Stage transition commits operation state, checkpoint, output,
input progress, and scheduler progress in one Store transaction. `Pending`
rolls the attempt back. Outputs are bounded to one retained block per edge, so
a slow fan-out consumer applies backpressure without an unbounded spool.

The `dogpaddle-flow` crate contains only these runtime responsibilities. SQL,
connectors, and higher-level APIs belong in separate crates implemented as
operations. External effects are outside the Store transaction and require a
stable idempotency key when crash retries must not duplicate them.

## Store

`dogpaddle-store` separates provisioning from runtime access:

- `Store` creates and opens named data, then is consumed before runtime;
- `Transactions` is the sole runtime capability for beginning transactions;
- `DataHandle` is one named encoded key/value namespace;
- `Transaction` owns one atomic commit boundary and rolls back on drop;
- transaction-bound access values perform the actual data operations.

Typed data structures live in `collections/` and compose the generic handle.
They own their codecs and semantics; the transaction does not know about
`Cell` and `OrderedMap`.

The durable catalog binds a name to an encoded key/value namespace and its
physical placement. It does not record or validate a collection type or codec.
Code reopening a data handle must use the same collection and codecs that
wrote its contents.

`DataPlacement::Shared` suits numerous small or commit-heavy namespaces that
share the main B+Tree. `DataPlacement::Dedicated` gives a hot or bulk-oriented
namespace its own MDBX named table. Placement is chosen once during
provisioning, persisted in the catalog, and invisible to the collection API;
the benchmark below should guide the choice for a concrete workload. One Store
supports up to `Store::DEDICATED_CAPACITY` dedicated namespaces.

```rust
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, Store};
use dogpaddle_store::DataPlacement::{Dedicated, Shared};

# fn example(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
let mut store = Store::create(path)?;
let counter_data = store.create_data("counter", Shared)?;
let users_data = store.create_data("users", Dedicated)?;
let counter = Cell::<u64>::new(counter_data);
let users = OrderedMap::<u64, String>::new(users_data);

let mut transactions = store.into_transactions();

let transaction = transactions.begin()?;
{
    let mut counter = counter.access(&transaction)?;
    let mut users = users.access(&transaction)?;

    counter.set(&1)?;
    users.put(&42, &"Shiba".to_owned())?;
}
transaction.commit()?;

let transaction = transactions.begin()?;
let counter = counter.access(&transaction)?;
let users = users.access(&transaction)?;
assert_eq!(counter.get()?, Some(1));
assert_eq!(users.get(&42)?.as_deref(), Some("Shiba"));

let batch = users.scan(
    ..,
    ScanDirection::Descending,
    None,
    ScanLimit::new(100, 1024 * 1024)?,
)?;
assert_eq!(batch.items, vec![(42, "Shiba".to_owned())]);
# Ok(())
# }
```

Dropping a transaction rolls it back. `Transaction` has no explicit abort
method and no collection-specific operations. Reads and writes through all access
values share the same transaction snapshot, so any number of data objects
commit atomically. The MDBX adapter and physical layout are private.

`DataHandle::access` yields the complete collection-building surface: point
reads, writes, deletes, and bounded ordered scans in either direction. A scan
returns owned items and an optional exclusive continuation key. Reuse the same
range and direction with that key for the next batch. Built-in collections
perform their codecs through the same transaction poison boundary. A custom
collection uses `DataAccess::poison_on_error` for hard codec or invariant
failures; raw storage failures are classified automatically.

Access values are attempt-local capabilities: bind them again for every new
transaction and never cache them across Stage steps.

## Tests

Architecture and collection behavior are separate integration targets:

```bash
cargo test -p dogpaddle-store --test architecture
cargo test -p dogpaddle-store --test collections
```

Individual areas remain directly filterable, for example:

```bash
cargo test -p dogpaddle-store --test architecture transaction::
cargo test -p dogpaddle-store --test collections scan::
```

## Performance

Run the release benchmark with:

```bash
cargo bench -p dogpaddle-store --bench store
```

It reports paired `Shared`/`Dedicated` samples for raw and typed bulk writes,
point reads, ascending and descending scans, durable overwrites, Stage-shaped
multi-collection transactions, and warmed reads with multiple shared
background namespaces. Results include min/median/max; they describe this
machine and temporary filesystem, not cold-cache or power-loss behavior.
Workload sizes can be changed with `DOGPADDLE_BENCH_ENTRIES`,
`DOGPADDLE_BENCH_COMMITS`, `DOGPADDLE_BENCH_SAMPLES`, and
`DOGPADDLE_BENCH_BACKGROUND_NAMESPACES`. `DOGPADDLE_BENCH_SCAN_ITEMS`
controls the number of entries admitted per scan batch.
