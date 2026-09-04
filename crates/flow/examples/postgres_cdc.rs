//! Explicit real-`PostgreSQL` gate host; never started by ordinary Cargo tests.
//!
//! `tools/check_postgres_cdc.py` owns the disposable database and drives this
//! JSONL example. Flow mode uses only public Flow APIs. Direct mode demonstrates
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
        sink::SqliteSinkDefinition,
        source::{PostgresSourceConfig, PostgresSourceDefinition},
    },
};
use dogpaddle_store::{AppendLog, Cell, ScanLimit, Store, Transactions};
use serde_json::{Value, json};

const SOURCE_CHECKPOINT: &str = "postgres_source.checkpoint";

struct Options {
    mode: String,
    root: PathBuf,
    config: PostgresSourceConfig,
    table: String,
    slot: String,
    publication: String,
}

impl Options {
    fn read() -> Result<Self, OperationError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [mode, root, bundle, port, table, slot, publication] = args.as_slice() else {
            return Err(
                "usage: postgres_cdc <flow|direct> ROOT BUNDLE PORT TABLE SLOT PUBLICATION".into(),
            );
        };
        let config = PostgresSourceConfig::new_unencrypted(
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
        })
    }

    fn definition(&self) -> Result<PostgresSourceDefinition, OperationError> {
        Ok(PostgresSourceDefinition::try_new(self.config.discover(
            &format!("dogpaddle_gate_{}", self.table),
            "public",
            &self.table,
            &self.slot,
            &self.publication,
        )?)?)
    }
}

fn main() -> Result<(), OperationError> {
    let options = Options::read()?;
    let mut runner = match options.mode.as_str() {
        "flow" => Runner::Flow(open_flow(options)?),
        "direct" => Runner::Direct(DirectSource::open(options)?),
        _ => return Err("mode must be flow or direct".into()),
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
    Direct(DirectSource),
}

impl Runner {
    fn command(&mut self, command: &str) -> Result<Value, OperationError> {
        match (self, command) {
            (Self::Flow(flow), "advance") => {
                Ok(json!({"kind": "advance", "outcome": format!("{:?}", flow.advance()?)}))
            }
            (Self::Direct(source), "read") => source.read(),
            (
                Self::Direct(source),
                "advance" | "rollback" | "crash-before-ack" | "backpressure",
            ) => source.advance(command),
            _ => Err("unsupported gate command".into()),
        }
    }
}

fn open_flow(options: Options) -> Result<Flow, OperationError> {
    let flow_path = options.root.join("flow");
    let mut factory = FlowFactory::new(&flow_path);
    if flow_path.exists() {
        factory.resource("pg", options.config)?;
        return Ok(factory.open()?);
    }
    let source = factory.station("pg", options.definition()?);
    let sink = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(options.root.join("sink.sqlite"), "events")?,
    );
    // One retained entry at a time, with the normal empty-log oversize rule.
    factory.output_capacity_bytes(source, NonZeroU64::MIN);
    factory.connect([source], sink);
    factory.resource("pg", options.config)?;
    Ok(factory.build()?)
}

struct DirectSource {
    source: Box<dyn Operation>,
    checkpoint: Cell<Vec<u8>>,
    output: AppendLog<Vec<u8>>,
    transactions: Transactions,
}

impl DirectSource {
    fn open(options: Options) -> Result<Self, OperationError> {
        let path = options.root.join("source");
        if !path.exists() {
            Self::create(&path, &options.definition()?)?;
        }
        let store = Store::open(&path)?;
        let definition_cell: Cell<Vec<u8>> = store.open_data("definition")?;
        let definition = {
            let snapshot = store.read_transaction()?;
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
            source: binding.materialize(data, RuntimeResource::new(options.config))?,
            checkpoint: store.open_data(SOURCE_CHECKPOINT)?,
            output: store.open_data("output")?,
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
        store.create_data::<AppendLog<Vec<u8>>>("output")?;
        let mut transactions = store.into_transactions();
        let transaction = transactions.begin()?;
        saved.access(transaction.access())?.set(&encoded)?;
        transaction.commit()?;
        Ok(())
    }

    fn advance(&mut self, command: &str) -> Result<Value, OperationError> {
        let Turn::Ready(prepared) = self.source.turn(None)? else {
            return Ok(json!({"kind": "idle"}));
        };
        let transaction = self.transactions.begin()?;
        let before = self.checkpoint.access(transaction.access())?.get()?;
        let before_bounds = self.output.access(transaction.access())?.bounds()?;
        let before_bytes = self.output.access(transaction.access())?.retained_bytes()?;
        let (action, completion) = prepared.apply(transaction.access())?;
        let checkpoint_present = self
            .checkpoint
            .access(transaction.access())?
            .get()?
            .is_some();
        let mut backpressured = false;
        let has_output = match &action {
            Action::Idle => return Ok(json!({"kind": "idle"})),
            Action::Commit(Some(change)) => {
                let capacity = if command == "backpressure" {
                    NonZeroU64::MIN
                } else {
                    NonZeroU64::MAX
                };
                if self
                    .output
                    .access(transaction.access())?
                    .try_append(&encode_change(change)?, capacity)?
                    .is_none()
                {
                    backpressured = true;
                }
                true
            }
            Action::Commit(None) => false,
            Action::Complete(_) => return Err("a Source cannot complete an input".into()),
        };
        if backpressured || (command == "rollback" && has_output) {
            drop(completion);
            drop(transaction);
            let transaction = self.transactions.begin()?;
            let checkpoint_unchanged =
                before == self.checkpoint.access(transaction.access())?.get()?;
            let output_unchanged = before_bounds
                == self.output.access(transaction.access())?.bounds()?
                && before_bytes == self.output.access(transaction.access())?.retained_bytes()?;
            return Ok(json!({
                "kind": if backpressured { "backpressure" } else { "rollback" },
                "output": has_output,
                "checkpoint_unchanged": checkpoint_unchanged,
                "output_unchanged": output_unchanged,
                "commits": 0,
            }));
        }
        transaction.commit()?;
        if command == "crash-before-ack" && has_output {
            // Terminate a real process: neither Delivery nor connector is dropped.
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
        let transaction = self.transactions.begin()?;
        let mut rows = Vec::new();
        let scanned = self.output.access(transaction.access())?.scan(
            0,
            ScanLimit::new(4096, usize::MAX)?,
            |entry| -> Result<(), OperationError> {
                let change = decode_change(&entry.decode_owned()?)?;
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
        if !scanned.caught_up {
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
