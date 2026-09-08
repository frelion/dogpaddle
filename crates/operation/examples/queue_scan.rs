//! Run with `cargo run -p dogpaddle-operation --example queue_scan`.
//!
//! This standalone caller demonstrates Operation + Store. Production Flow uses
//! Station for transactions, Schema guards, capacity, and input completion.

use arrow_array::UInt64Array;
use dogpaddle_change::encode_change;
use dogpaddle_operation::operation::{Action, Operation, OperationError, Turn};
use dogpaddle_store::{Cell, Store, SubscribedLog};

#[path = "support/queue_scan.rs"]
mod queue_scan;

use queue_scan::QueueScan;

fn main() -> Result<(), OperationError> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("queue-example");
    let mut store = Store::create(&path)?;
    store.create_data::<Cell<u64>>("checkpoint")?;
    let output = store.create_data::<SubscribedLog<Vec<u8>>>("output")?;
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    output.initialize(std::num::NonZeroU64::MIN, transaction.access())?;
    transaction.commit()?;
    drop(transactions);

    // First session: initialize, emit 10, then close. Second session: recover,
    // emit 20 and 30, then observe Idle. No runtime client survives the reopen.
    for turns in [2, 4] {
        let store = Store::open(&path)?;
        let mut scan = QueueScan::new(store.open_data("checkpoint")?);
        let output: SubscribedLog<Vec<u8>> = store.open_data("output")?;
        let snapshot = store.read_transaction();
        output.validate(std::num::NonZeroU64::MIN, snapshot.access())?;
        drop(snapshot);
        let output = output.writer();
        let mut transactions = store.into_transactions();
        println!("opened Store with a fresh Operation");

        for _ in 0..turns {
            let Turn::Ready(prepared) = scan.turn(None)? else {
                println!("idle: no records left");
                break;
            };
            let transaction = transactions.begin();
            let (action, after_commit) = prepared.apply(transaction.access())?;
            let value = match action {
                Action::Idle => continue, // Drops both the transaction and completion.
                Action::Commit(None) => None,
                Action::Commit(Some(change)) => {
                    assert!(output.try_append(
                        &encode_change(&change)?,
                        std::num::NonZeroU64::MAX,
                        transaction.access(),
                    )?);
                    let values = change
                        .records()
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or("unexpected example Schema")?;
                    Some(values.value(0))
                }
                Action::Complete(_) => return Err("a Scan cannot complete an input".into()),
            };
            transaction.commit()?;
            after_commit.run()?;

            match value {
                Some(value) => println!("committed output {value} and checkpoint, then ACKed"),
                None => println!("restored checkpoint; ready to poll"),
            }
        }
    }
    Ok(())
}
