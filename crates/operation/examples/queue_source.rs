//! Run with `cargo run -p dogpaddle-operation --example queue_source`.
//!
//! This standalone caller demonstrates Operation + Store. Production Flow uses
//! Station for transactions, Schema guards, capacity, and input completion.

use arrow_array::UInt64Array;
use dogpaddle_change::encode_change;
use dogpaddle_operation::operation::{Action, Operation, OperationError, Turn};
use dogpaddle_store::{AppendLog, Cell, Store};

#[path = "support/queue_source.rs"]
mod queue_source;

use queue_source::QueueSource;

fn main() -> Result<(), OperationError> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("queue-example");
    let mut store = Store::create(&path)?;
    store.create_data::<Cell<u64>>("checkpoint")?;
    store.create_data::<AppendLog<Vec<u8>>>("output")?;
    drop(store);

    // First session: initialize, emit 10, then close. Second session: recover,
    // emit 20 and 30, then observe Idle. No runtime client survives the reopen.
    for turns in [2, 4] {
        let store = Store::open(&path)?;
        let mut source = QueueSource::new(store.open_data("checkpoint")?);
        let output: AppendLog<Vec<u8>> = store.open_data("output")?;
        let mut transactions = store.into_transactions();
        println!("opened Store with a fresh Operation");

        for _ in 0..turns {
            let Turn::Ready(prepared) = source.turn(None)? else {
                println!("idle: no records left");
                break;
            };
            let transaction = transactions.begin()?;
            let (action, after_commit) = prepared.apply(transaction.access())?;
            let value = match action {
                Action::Idle => continue, // Drops both the transaction and completion.
                Action::Commit(None) => None,
                Action::Commit(Some(change)) => {
                    output
                        .access(transaction.access())?
                        .append(&encode_change(&change)?)?;
                    let values = change
                        .records()
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or("unexpected example Schema")?;
                    Some(values.value(0))
                }
                Action::Complete(_) => return Err("a source cannot complete an input".into()),
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
