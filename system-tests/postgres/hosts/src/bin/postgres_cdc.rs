//! Explicit real-`PostgreSQL` gate host; never started by ordinary Cargo tests.
//!
//! `system-tests/postgres/check_cdc.py` owns the disposable database
//! and drives this JSONL host. Flow mode uses only public Flow APIs. Direct mode demonstrates
//! the public Operation protocol and can terminate the process between durable
//! checkpoint/output commit and ACK without a product fault-injection hook.

use std::{
    env,
    io::{self, BufRead, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    process,
};

use arrow_array::{Int32Array, Int64Array, StringArray};
use dogpaddle_change::{decode_change, encode_change};
use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::{
    DataInstances, OperationDefinition, RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, Turn,
        scan::{PostgresCdcScanConfig, PostgresCdcScanDefinition},
        sink::{PostgresSinkConfig, PostgresSinkDefinition, SqliteSinkDefinition},
    },
};
use dogpaddle_store::{Cell, OrderedMap, ScanDirection, ScanLimit, Store, Transactions};
use serde_json::{Value, json};

const SCAN_CHECKPOINT: &str = "postgres_cdc_scan.checkpoint";
const SCAN_PHASE: &str = "postgres_cdc_scan.phase";

struct Options {
    mode: String,
    root: PathBuf,
    config: PostgresCdcScanConfig,
    table: String,
    slot: String,
    publication: String,
    port: u16,
}

impl Options {
    fn read() -> Result<Self, OperationError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [mode, root, bundle, port, table, slot, publication] = args.as_slice() else {
            return Err(
                "usage: postgres_cdc <flow|flow-pg|direct> ROOT BUNDLE PORT TABLE SLOT PUBLICATION"
                    .into(),
            );
        };
        let config = PostgresCdcScanConfig::new_unencrypted(
            bundle,
            "127.0.0.1",
            port.parse()?,
            "postgres",
            "dogpaddle_gate",
            env::var("DOGPADDLE_GATE_PASSWORD")?,
        )?;
        Ok(Self {
            mode: mode.clone(),
            root: root.into(),
            config,
            table: table.clone(),
            slot: slot.clone(),
            publication: publication.clone(),
            port: port.parse()?,
        })
    }

    fn definition(&self) -> Result<PostgresCdcScanDefinition, OperationError> {
        Ok(PostgresCdcScanDefinition::try_new(
            self.config.discover(
                &format!("dogpaddle_gate_{}", self.table),
                "public",
                &self.table,
                &self.slot,
                &self.publication,
            )?,
            NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
        )?)
    }
}

fn main() -> Result<(), OperationError> {
    let options = Options::read()?;
    let mut runner = match options.mode.as_str() {
        "flow" | "flow-pg" => Runner::Flow(open_flow(options)?),
        "direct" => Runner::Direct(DirectScan::open(options)?),
        _ => return Err("mode must be flow, flow-pg or direct".into()),
    };
    respond(&json!({"kind": "ready"}))?;
    for command in io::stdin().lock().lines() {
        let command = command?;
        if command == "quit" {
            break;
        }
        match runner.command(&command) {
            Ok(response) => respond(&response)?,
            Err(error) => {
                respond(&json!({"kind": "error", "message": error.to_string()}))?;
                return Err(error);
            }
        }
    }
    Ok(())
}

fn respond(response: &Value) -> Result<(), OperationError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, response)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

enum Runner {
    Flow(Flow),
    Direct(DirectScan),
}

impl Runner {
    fn command(&mut self, command: &str) -> Result<Value, OperationError> {
        match (self, command) {
            (Self::Flow(flow), "advance") => {
                Ok(json!({"kind": "advance", "outcome": format!("{:?}", flow.advance()?)}))
            }
            (Self::Direct(scan), "read") => scan.read(),
            (
                Self::Direct(scan),
                "advance"
                | "rollback"
                | "crash-before-ack"
                | "crash-partial-capture"
                | "crash-terminal-capture"
                | "backpressure",
            ) => scan.advance(command),
            _ => Err("unsupported gate command".into()),
        }
    }
}

fn open_flow(options: Options) -> Result<Flow, OperationError> {
    let flow_path = options.root.join("flow");
    let mut factory = FlowFactory::new(&flow_path);
    let sink_config = if options.mode == "flow-pg" {
        Some(PostgresSinkConfig::new_unencrypted(
            "127.0.0.1",
            options.port,
            "postgres",
            "dogpaddle_gate",
            env::var("DOGPADDLE_GATE_PASSWORD")?,
        )?)
    } else {
        None
    };
    if flow_path.exists() {
        factory.resource("pg", options.config)?;
        if let Some(config) = sink_config {
            factory.resource("sink", config)?;
        }
        return Ok(factory.open()?);
    }
    let scan = factory.station("pg", options.definition()?);
    let sink = if let Some(config) = &sink_config {
        let target = config.discover_target("roundtrip_sink", "public", "roundtrip_target")?;
        factory.station("sink", PostgresSinkDefinition::try_new(target)?)
    } else {
        factory.station(
            "sqlite",
            SqliteSinkDefinition::try_new(options.root.join("sink.sqlite"), "events")?,
        )
    };
    // One retained entry at a time, with the normal empty-log oversize rule.
    factory.output_capacity_bytes(scan, NonZeroU64::MIN);
    factory.connect([scan], sink);
    factory.resource("pg", options.config)?;
    if let Some(config) = sink_config {
        factory.resource("sink", config)?;
    }
    Ok(factory.build()?)
}

struct DirectScan {
    scan: Box<dyn Operation>,
    phase: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    output: OrderedMap<u64, Vec<u8>>,
    output_tail: Cell<u64>,
    transactions: Transactions,
}

impl DirectScan {
    fn open(options: Options) -> Result<Self, OperationError> {
        let path = options.root.join("scan");
        if !path.exists() {
            Self::create(&path, &options.definition()?)?;
        }
        let store = Store::open(&path)?;
        let definition_cell: Cell<Vec<u8>> = store.open_data("definition")?;
        let definition = {
            let snapshot = store.read_transaction();
            decode_definition(
                &definition_cell
                    .read(snapshot.access())?
                    .get()?
                    .ok_or("missing definition")?,
            )?
        };
        let binding = definition.bind(&[])?;
        let mut data = DataInstances::new();
        for declaration in definition.data() {
            data.insert(declaration.open(&store, declaration.name())?)?;
        }
        Ok(Self {
            scan: binding.materialize(data, RuntimeResource::new(options.config))?,
            phase: store.open_data(SCAN_PHASE)?,
            checkpoint: store.open_data(SCAN_CHECKPOINT)?,
            output: store.open_data("output")?,
            output_tail: store.open_data("output-tail")?,
            transactions: store.into_transactions(),
        })
    }

    fn create(path: &Path, definition: &dyn OperationDefinition) -> Result<(), OperationError> {
        let encoded = encode_definition(definition);
        let canonical = decode_definition(&encoded)?;
        let _ = canonical.bind(&[])?;
        let mut store = Store::create(path)?;
        let saved: Cell<Vec<u8>> = store.create_data("definition")?;
        for declaration in canonical.data() {
            let _ = declaration.create(&mut store, declaration.name())?;
        }
        store.create_data::<OrderedMap<u64, Vec<u8>>>("output")?;
        store.create_data::<Cell<u64>>("output-tail")?;
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin();
        saved.access(transaction.access())?.set(&encoded)?;
        transaction.commit()?;
        Ok(())
    }

    fn advance(&mut self, command: &str) -> Result<Value, OperationError> {
        let Turn::Ready(prepared) = self.scan.turn(None)? else {
            return Ok(json!({"kind": "idle"}));
        };
        let transaction = self.transactions.begin();
        let before = self.checkpoint.access(transaction.access())?.get()?;
        let before_tail = self
            .output_tail
            .access(transaction.access())?
            .get()?
            .unwrap_or(0);
        let (action, completion) = prepared.apply(transaction.access())?;
        let after = self.checkpoint.access(transaction.access())?.get()?;
        let checkpoint_present = after.is_some();
        let checkpoint_changed = before != after;
        let phase = self.phase.access(transaction.access())?.get()?;
        let mut backpressured = false;
        let has_output = match &action {
            Action::Idle => return Ok(json!({"kind": "idle"})),
            Action::Commit(Some(change)) => {
                if command == "backpressure" && before_tail != 0 {
                    backpressured = true;
                } else {
                    self.output
                        .access(transaction.access())?
                        .put(&before_tail, &encode_change(change)?)?;
                    let next = before_tail
                        .checked_add(1)
                        .ok_or("direct gate output tail is exhausted")?;
                    self.output_tail.access(transaction.access())?.set(&next)?;
                }
                true
            }
            Action::Commit(None) => false,
            Action::Complete(_) => return Err("a Scan cannot complete an input".into()),
        };
        if backpressured || (command == "rollback" && has_output) {
            drop(completion);
            drop(transaction);
            let transaction = self.transactions.begin();
            let checkpoint_unchanged =
                before == self.checkpoint.access(transaction.access())?.get()?;
            let output_unchanged = self
                .output_tail
                .access(transaction.access())?
                .get()?
                .unwrap_or(0)
                == before_tail;
            return Ok(json!({
                "kind": if backpressured { "backpressure" } else { "rollback" },
                "output": has_output,
                "checkpoint_unchanged": checkpoint_unchanged,
                "output_unchanged": output_unchanged,
                "commits": 0,
            }));
        }
        transaction.commit()?;
        let capture_crash = match command {
            "crash-partial-capture" => !has_output && checkpoint_changed && phase == Some(1),
            "crash-terminal-capture" => !has_output && checkpoint_changed && phase == Some(2),
            _ => false,
        };
        if capture_crash {
            respond(&json!({
                "kind": if phase == Some(1) {
                    "durable-partial-capture"
                } else {
                    "durable-terminal-capture"
                },
                "checkpoint_present": checkpoint_present,
                "commits": 1,
            }))?;
            process::exit(if phase == Some(1) { 75 } else { 76 });
        }
        if command == "crash-before-ack" && has_output {
            // Terminate before post-commit completion. During streaming this
            // also leaves the real Delivery unacknowledged.
            respond(&json!({
                "kind": "durable-before-ack", "output": true,
                "checkpoint_present": checkpoint_present, "commits": 1,
            }))?;
            process::exit(74);
        }
        completion.run()?;
        Ok(json!({
            "kind": "advance", "output": has_output,
            "checkpoint_present": checkpoint_present, "commits": 1,
        }))
    }

    fn read(&mut self) -> Result<Value, OperationError> {
        let transaction = self.transactions.begin();
        let mut rows = Vec::new();
        let continuation = self.output.access(transaction.access())?.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(4096, usize::MAX)?,
            |entry| -> Result<(), OperationError> {
                let (_, encoded) = entry.decode_owned()?;
                let change = decode_change(&encoded)?;
                let columns = change.records().columns();
                let ids = columns[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("id is not int64")?;
                let sequences = columns[1]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("tx_seq is not int32")?;
                let payloads = columns[2]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("payload is not text")?;
                for row in 0..change.num_rows() {
                    rows.push(json!([
                        change.diffs().value(row),
                        ids.value(row),
                        sequences.value(row),
                        payloads.value(row)
                    ]));
                }
                Ok(())
            },
        )?;
        if continuation.is_some() {
            return Err("gate output exceeded the bounded diagnostic scan".into());
        }
        let checkpoint_present = self
            .checkpoint
            .access(transaction.access())?
            .get()?
            .is_some();
        Ok(json!({"kind": "rows", "rows": rows, "checkpoint_present": checkpoint_present}))
    }
}
