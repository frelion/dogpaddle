# DogPaddle Store

`dogpaddle-store` separates provisioning from runtime access:

- `Store` creates and opens named data, then is consumed before runtime;
- `Transactions` is the sole runtime capability for beginning transactions;
- `DataHandle` is one named encoded key/value namespace;
- `Transaction` owns one atomic commit boundary and rolls back on drop;
- transaction-bound access values perform the actual data operations.

Typed data structures live in `collections/` and compose the generic handle.
They own their codecs and semantics; the transaction does not know about
`Cell`, `OrderedMap`, or future collection types.

The durable catalog binds a name to an encoded key/value namespace and its
physical placement. It does not record or validate a collection type or codec.
Code reopening a data handle must wrap it with the same collection and
compatible codecs that wrote its contents.

Small data uses `DataPlacement::Shared` and shares the main B+Tree. Large data
uses `DataPlacement::Dedicated` and owns an MDBX named table. Placement is
chosen once during provisioning, persisted in the catalog, and invisible to
the collection API. One Store supports up to `Store::DEDICATED_CAPACITY`
dedicated namespaces.

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
method and no object-specific operations. Reads and writes through all access
values share the same transaction snapshot, so any number of data objects
commit atomically. The MDBX adapter and physical key format are private.

`DataHandle::access` yields the complete collection-building surface: point
reads, writes, deletes, and bounded ordered scans in either direction. A scan
returns owned items and an optional exclusive continuation key. Reuse the same
range and direction with that key for the next batch. Built-in collections
perform their codecs through the same transaction poison boundary. A custom
collection uses `DataAccess::poison_on_error` for hard codec or invariant
failures; raw storage failures are classified automatically.

Access values are attempt-local capabilities: bind them again for every new
transaction and never cache them across Stage steps.
