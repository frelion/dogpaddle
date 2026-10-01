//! Native `PostgreSQL` protocol gate host, driven by
//! `system-tests/postgres/check_sink.py`.
//!
//! The caller retains the complete input until `Complete`, then drives durable
//! buffered delivery with explicit no-input steps. Fault boundaries use the
//! public load/initialize-intent/deliver/settle API.

use std::{
    env,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, NullArray, RecordBatch, RecordBatchOptions,
    StringArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    RuntimeResource,
    operation::{
        Operation, OperationError,
        sink::{PostgresSinkConfig, PostgresSinkDefinition},
    },
};
use dogpaddle_store::{Cell, ReadTransactions, Store, Transactions};
use serde_json::{Value, json};

const OPERATION_PREFIX: &str = "operation";
const SINK_CONTROL: &str = "operation/sink.control";

struct Host {
    operation: Operation,
    state: Cell<Vec<u8>>,
    transactions: Transactions,
    reads: ReadTransactions,
}

impl Host {
    fn open(mode: &str, path: &Path, port: u16, scenario: &str) -> Result<Self, OperationError> {
        let config = PostgresSinkConfig::new_unencrypted(
            "127.0.0.1",
            port,
            "postgres",
            "dogpaddle_gate",
            env::var("DOGPADDLE_GATE_PASSWORD")?,
        )?;
        let schema = fixture(scenario, "seed")?.schema();
        if mode == "build" {
            let target = config.discover_target(format!("gate_{scenario}"), "public", scenario)?;
            let definition = PostgresSinkDefinition::try_new(target)?;
            let encoded =
                serde_json::to_vec::<dogpaddle_operation::OperationDefinition>(&definition.into())
                    .unwrap();
            let canonical =
                serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&encoded)?;
            let mut setup = dogpaddle_store::StoreSetup::new();
            let saved: Cell<Vec<u8>> = setup.create_data("definition")?;
            let _operation = canonical.construct(
                &[Arc::clone(&schema)],
                &mut setup.data_scope().scoped(OPERATION_PREFIX),
                RuntimeResource::new(config),
            )?;
            let _transactions = setup.commit(path, |access| {
                saved.access(access)?.set(&encoded)?;
                Ok(())
            })?;
        } else if mode != "open" {
            return Err("mode must be build or open".into());
        }
        let config = PostgresSinkConfig::new_unencrypted(
            "127.0.0.1",
            port,
            "postgres",
            "dogpaddle_gate",
            env::var("DOGPADDLE_GATE_PASSWORD")?,
        )?;
        let store = Store::open(path)?;
        let saved: Cell<Vec<u8>> = store.open_data("definition")?;
        let definition = {
            let snapshot = store.read_transaction();
            serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(
                &saved
                    .read(snapshot.access())?
                    .get()?
                    .ok_or("missing definition")?,
            )?
        };
        let operation = definition
            .construct(
                &[schema],
                &mut store.data_scope().scoped(OPERATION_PREFIX),
                RuntimeResource::new(config),
            )?
            .into_parts()
            .0;
        let state = store.open_data(SINK_CONTROL)?;
        let (transactions, reads) = store.into_transactions().split();
        Ok(Self {
            operation,
            state,
            transactions,
            reads,
        })
    }

    fn advance(&mut self, command: &str, change: Option<&Change>) -> Result<Value, OperationError> {
        let Operation::Sink(sink) = &mut self.operation else {
            return Err("expected sink".into());
        };
        if let Some(change) = change {
            let txn = self.transactions.begin();
            let before = self.state.access(txn.access())?.get()?;
            if sink.try_enqueue(txn.access(), change)? {
                if command == "rollback" {
                    drop(txn);
                    return Ok(
                        json!({"kind": "rollback", "unchanged": self.state.read(self.reads.begin().access())?.get()? == before}),
                    );
                }
                txn.commit()?;
                return Ok(json!({"kind": "advance", "outcome": "Complete"}));
            }
        }
        let pending = sink.load(self.reads.begin().access())?;
        let Some(pending) = pending else {
            return Ok(json!({"kind": "advance", "outcome": "Idle"}));
        };
        let needs_initialize = match sink.prepare_initialize(&pending) {
            Ok(value) => value,
            Err(error) => return Ok(json!({"kind":"error","message":error.to_string()})),
        };
        if needs_initialize {
            let txn = self.transactions.begin();
            let before = self.state.access(txn.access())?.get()?;
            sink.persist_initialize(txn.access(), &pending)?;
            if command == "rollback" {
                drop(txn);
                return Ok(
                    json!({"kind":"rollback","unchanged":self.state.read(self.reads.begin().access())?.get()?==before}),
                );
            }
            txn.commit()?;
        } else if command == "rollback" {
            return Err("rollback command requires enqueue or fresh initialization intent".into());
        }
        if command == "load-only" {
            return Ok(json!({"kind":"loaded"}));
        }
        sink.deliver(&pending)?;
        if command == "deliver-only" {
            return Ok(json!({"kind": "delivered"}));
        }
        {
            let txn = self.transactions.begin();
            sink.settle(txn.access(), &pending)?;
            txn.commit()?;
        }
        Ok(json!({"kind": "advance", "outcome": "Commit"}))
    }
}

fn fixture(scenario: &str, stage: &str) -> Result<Change, OperationError> {
    let (records, multiplicity) = match scenario {
        "bulk" | "bulk_invalid" => {
            let (values, diffs) = match stage {
                "seed" => (vec![u64::MAX], vec![16_385]),
                "withdraw" => (vec![u64::MAX], vec![-16_385]),
                "missing" => (vec![u64::MAX], vec![-16_386]),
                "mixed" => (vec![u64::MAX; 6], vec![3, -2, 1, 2, -2, -2]),
                "invalid-prefix" => (vec![u64::MAX; 2], vec![-1, 1]),
                _ => return Err("unknown bulk fixture".into()),
            };
            let records = RecordBatch::try_from_iter([(
                "value",
                Arc::new(UInt64Array::from(values)) as ArrayRef,
            )])?;
            return Ok(Change::try_new(records, Int64Array::from(diffs))?);
        }
        "updates" => {
            let (values, diffs) = match stage {
                "seed" => ((0..1_000).collect::<Vec<i64>>(), vec![1; 1_000]),
                "update" => (
                    (0..1_000)
                        .flat_map(|value| [value, value + 1_000])
                        .collect(),
                    [-1, 1].repeat(1_000),
                ),
                "withdraw" => ((1_000..2_000).collect(), vec![-1; 1_000]),
                _ => return Err("unknown updates fixture".into()),
            };
            let records = RecordBatch::try_from_iter([(
                "value",
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )])?;
            return Ok(Change::try_new(records, Int64Array::from(diffs))?);
        }
        scenario if scenario.starts_with("frontier_") => {
            let (values, diffs) = match stage {
                "seed" | "birth" => (vec![7], vec![1]),
                "collision" => (vec![8, 7], vec![1, 1]),
                "pair" => (vec![7, 7], vec![1, -1]),
                _ => return Err("unknown frontier fixture".into()),
            };
            let records = RecordBatch::try_from_iter([(
                "value",
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )])?;
            return Ok(Change::try_new(records, Int64Array::from(diffs))?);
        }
        "types" => (typed_records()?, 1),
        "wide" => {
            // 1,600 physical columns: 40 rows per statement at the u16
            // parameter limit. NULLs keep the physical tuple within a PG page.
            let fields = (0..1_598)
                .map(|index| Field::new(format!("f{index}"), DataType::Int64, true))
                .collect::<Vec<_>>();
            let columns = (0..1_598)
                .map(|_| Arc::new(Int64Array::from(vec![None])) as ArrayRef)
                .collect();
            (
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
                80,
            )
        }
        "empty" => (
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?,
            2,
        ),
        _ => return Err("unknown scenario".into()),
    };
    let diff = match stage {
        "seed" => multiplicity,
        "withdraw" => -multiplicity,
        _ => return Err("unknown fixture stage".into()),
    };
    let rows = records.num_rows();
    Ok(Change::try_new(
        records,
        Int64Array::from(vec![diff; rows]),
    )?)
}

fn typed_records() -> Result<RecordBatch, OperationError> {
    // Every storage parameter family, null matching, and values native SQL
    // cannot preserve (NUL UTF-8, signed zero, NaN payloads and full UInt64).
    let arrays: Vec<(&str, ArrayRef)> = vec![
        ("nothing", Arc::new(NullArray::new(2))),
        (
            "boolean",
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ),
        ("i8", Arc::new(Int8Array::from(vec![Some(i8::MIN), None]))),
        (
            "i16",
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
        ),
        (
            "i32",
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        ),
        ("u8", Arc::new(UInt8Array::from(vec![Some(u8::MAX), None]))),
        (
            "u16",
            Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
        ),
        (
            "u32",
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        ),
        (
            "f32",
            Arc::new(Float32Array::from(vec![
                Some(f32::from_bits(0x7f80_0123)),
                None,
            ])),
        ),
        ("f64", Arc::new(Float64Array::from(vec![Some(-0.0), None]))),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("before\0after"), None])),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
        ),
        (
            "decimal",
            Arc::new(Decimal128Array::from(vec![Some(-999), None]).with_precision_and_scale(3, 2)?),
        ),
        ("date", Arc::new(Date32Array::from(vec![Some(-1), None]))),
        (
            "timestamp",
            Arc::new(TimestampNanosecondArray::from(vec![Some(i64::MAX), None])),
        ),
    ];
    Ok(RecordBatch::try_from_iter(arrays)?)
}

fn respond(response: &Value) -> Result<(), OperationError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, response)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

fn main() -> Result<(), OperationError> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let [mode, path, port, scenario] = args.as_slice() else {
        return Err("usage: postgres_sink_recovery <build|open> PATH PORT \
             <bulk|bulk_invalid|updates|types|wide|empty>"
            .into());
    };
    let mut host = Host::open(mode, &PathBuf::from(path), port.parse()?, scenario)?;
    respond(&json!({"kind": "ready", "mode": mode}))?;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let mut parts = line.split_ascii_whitespace();
        let Some(command @ ("advance" | "rollback" | "load-only" | "deliver-only")) = parts.next()
        else {
            return Err("unsupported command".into());
        };
        let stage = parts.next();
        if parts.next().is_some() {
            return Err("too many command arguments".into());
        }
        let change = stage
            .filter(|stage| *stage != "-")
            .map(|stage| fixture(scenario, stage))
            .transpose()?;
        match host.advance(command, change.as_ref()) {
            Ok(response) => respond(&response)?,
            Err(error) => {
                respond(&json!({"kind": "error", "message": error.to_string()}))?;
                return Err(error);
            }
        }
    }
    Ok(())
}
