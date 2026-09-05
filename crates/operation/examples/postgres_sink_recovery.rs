//! Native `PostgreSQL` protocol gate host, driven by `tools/check_postgres_sink.py`.
//!
//! The caller retains the complete input across Commit turns and process kills.
//! Fault boundaries use the public staged-turn API, not product test hooks.

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
    DataInstances, RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn,
        sink::{PostgresSinkConfig, PostgresSinkDefinition},
    },
};
use dogpaddle_store::{Cell, Store, Transactions};
use serde_json::{Value, json};

struct Host {
    operation: Box<dyn Operation>,
    state: Cell<Vec<u8>>,
    transactions: Transactions,
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
            let encoded = encode_definition(&definition);
            let canonical = decode_definition(&encoded)?;
            canonical.bind(&[Arc::clone(&schema)])?;
            let mut store = Store::create(path)?;
            let saved: Cell<Vec<u8>> = store.create_data("definition")?;
            for declaration in canonical.data() {
                declaration.create(&mut store, declaration.name())?;
            }
            let mut transactions = store.into_transactions();
            let transaction = transactions.begin()?;
            saved.access(transaction.access())?.set(&encoded)?;
            transaction.commit()?;
        } else if mode != "open" {
            return Err("mode must be build or open".into());
        }
        let store = Store::open(path)?;
        let saved: Cell<Vec<u8>> = store.open_data("definition")?;
        let definition = {
            let snapshot = store.read_transaction()?;
            decode_definition(
                &saved
                    .read(snapshot.access())?
                    .get()?
                    .ok_or("missing definition")?,
            )?
        };
        let binding = definition.bind(&[schema])?;
        let mut data = DataInstances::new();
        for declaration in definition.data() {
            data.insert(declaration.open(&store, declaration.name())?)?;
        }
        Ok(Self {
            operation: binding.materialize(data, RuntimeResource::new(config))?,
            state: store.open_data("postgres_sink.state")?,
            transactions: store.into_transactions(),
        })
    }

    fn advance(&mut self, command: &str, change: &Change) -> Result<Value, OperationError> {
        let prepared = match self
            .operation
            .turn(Some(OperationInput { port: 0, change }))
        {
            Ok(Turn::Ready(prepared)) => prepared,
            Ok(Turn::Idle) => return Err("a sink with input unexpectedly idled".into()),
            // Ordinary turn errors leave the same runtime retryable. Errors
            // from apply/completion remain fatal in this small protocol host.
            Err(error) => return Ok(json!({"kind": "error", "message": error.to_string()})),
        };
        let transaction = self.transactions.begin()?;
        let before = self.state.access(transaction.access())?.get()?;
        let (action, completion) = prepared.apply(transaction.access())?;
        let action = match action {
            Action::Commit(None) => "Commit",
            Action::Complete(None) => "Complete",
            _ => return Err("sink returned an unexpected action".into()),
        };
        if command == "rollback" {
            drop(completion);
            drop(transaction);
            let transaction = self.transactions.begin()?;
            let unchanged = self.state.access(transaction.access())?.get()? == before;
            return Ok(json!({"kind": "rollback", "unchanged": unchanged}));
        }
        transaction.commit()?;
        if command == "prepare-only" {
            // The driver kills this process immediately, then reopens it.
            drop(completion);
            return Ok(json!({"kind": "prepared"}));
        }
        completion.run()?;
        Ok(json!({"kind": "advance", "outcome": action}))
    }
}

fn fixture(scenario: &str, stage: &str) -> Result<Change, OperationError> {
    let (records, multiplicity) = match scenario {
        "bulk" => {
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
        return Err(
            "usage: postgres_sink_recovery <build|open> PATH PORT <bulk|types|wide|empty>".into(),
        );
    };
    let mut host = Host::open(mode, &PathBuf::from(path), port.parse()?, scenario)?;
    respond(&json!({"kind": "ready", "mode": mode}))?;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let Some((command @ ("advance" | "rollback" | "prepare-only"), stage)) =
            line.split_once(' ')
        else {
            return Err("unsupported command".into());
        };
        let change = fixture(scenario, stage)?;
        match host.advance(command, &change) {
            Ok(response) => respond(&response)?,
            Err(error) => {
                respond(&json!({"kind": "error", "message": error.to_string()}))?;
                return Err(error);
            }
        }
    }
    Ok(())
}
