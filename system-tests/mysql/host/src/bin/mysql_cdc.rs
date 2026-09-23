//! Real `MySQL` CDC crash-window host; only the explicit system gate starts it.
//! Uses the public Operation/Store protocol to exit after the durable transaction
//! and before the real Debezium Delivery ACK, without production fault hooks.

use std::{
    env,
    io::{self, BufRead, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    process,
};

use arrow_array::{Int32Array, Int64Array, StringArray};
use dogpaddle_change::{decode_change, encode_change};
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, decode_definition, encode_definition,
    operation::{
        Action, Operation, OperationError, Turn,
        scan::{MySqlCdcScanConfig, MySqlCdcScanDefinition},
    },
};
use dogpaddle_store::{Cell, OrderedMap, Queue, ScanDirection, ScanLimit, Store, Transactions};
use serde_json::{Value, json};

const OPERATION_PREFIX: &str = "operation";
const SCAN_CHECKPOINT: &str = "operation/mysql_cdc_scan.checkpoint";
const SCAN_PHASE: &str = "operation/mysql_cdc_scan.phase";
const SCAN_SPOOL: &str = "operation/mysql_cdc_scan.bootstrap_spool";
const DIAGNOSTIC_OUTPUT_ITEMS: usize = 16;
const DIAGNOSTIC_OUTPUT_BYTES: usize = 1024 * 1024;
const DIAGNOSTIC_OUTPUT_ROWS: usize = 64;

struct Options {
    root: PathBuf,
    bundle: PathBuf,
    port: u16,
    table: String,
    password: String,
}

impl Options {
    fn read() -> Result<Self, OperationError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [root, bundle, port, table] = args.as_slice() else {
            return Err("usage: mysql_cdc ROOT BUNDLE PORT TABLE".into());
        };
        Ok(Self {
            root: root.into(),
            bundle: bundle.into(),
            port: port.parse()?,
            table: table.clone(),
            password: env::var("DOGPADDLE_GATE_PASSWORD")?,
        })
    }

    fn config(&self) -> Result<MySqlCdcScanConfig, OperationError> {
        Ok(MySqlCdcScanConfig::new_unencrypted(
            &self.bundle,
            "127.0.0.1",
            self.port,
            "dogpaddle_gate",
            "dogpaddle_gate",
            &self.password,
        )?)
    }

    fn definition(&self) -> Result<MySqlCdcScanDefinition, OperationError> {
        Ok(MySqlCdcScanDefinition::try_new(
            self.config()?
                .discover(&format!("dogpaddle_gate_{}", self.table), &self.table)?,
            NonZeroU64::new(1024 * 1024 * 1024).expect("nonzero spool capacity"),
        )?)
    }
}

fn main() -> Result<(), OperationError> {
    let options = Options::read()?;
    let mut scan = DirectScan::open(&options)?;
    respond(&json!({"kind": "ready"}))?;
    for command in io::stdin().lock().lines() {
        let command = command?;
        if command == "quit" {
            break;
        }
        let response = match command.as_str() {
            "read" => scan.read(),
            "advance" | "crash-terminal-capture" | "crash-before-ack" => scan.advance(&command),
            _ => Err("unsupported gate command".into()),
        };
        match response {
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

struct DirectScan {
    scan: Operation,
    phase: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    spool: Queue<Vec<u8>>,
    output: OrderedMap<u64, Vec<u8>>,
    output_tail: Cell<u64>,
    transactions: Transactions,
}

impl DirectScan {
    fn open(options: &Options) -> Result<Self, OperationError> {
        let path = options.root.join("scan");
        if !path.exists() {
            Self::create(&path, &options.definition()?, options.config()?)?;
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
        let scan = definition
            .construct(
                &[],
                &mut store.data_scope().scoped(OPERATION_PREFIX),
                RuntimeResource::new(options.config()?),
            )?
            .into_parts()
            .0;
        Ok(Self {
            scan,
            phase: store.open_data(SCAN_PHASE)?,
            checkpoint: store.open_data(SCAN_CHECKPOINT)?,
            spool: store.open_data(SCAN_SPOOL)?,
            output: store.open_data("output")?,
            output_tail: store.open_data("output-tail")?,
            transactions: store.into_transactions(),
        })
    }

    fn create(
        path: &Path,
        definition: &dyn OperationDefinition,
        config: MySqlCdcScanConfig,
    ) -> Result<(), OperationError> {
        let encoded = encode_definition(definition);
        let canonical = decode_definition(&encoded)?;
        let mut setup = dogpaddle_store::StoreSetup::new();
        let saved: Cell<Vec<u8>> = setup.create_data("definition")?;
        let _operation = canonical.construct(
            &[],
            &mut setup.data_scope().scoped(OPERATION_PREFIX),
            RuntimeResource::new(config),
        )?;
        setup.create_data::<OrderedMap<u64, Vec<u8>>>("output")?;
        setup.create_data::<Cell<u64>>("output-tail")?;
        let _transactions = setup.commit(path, |access| {
            saved.access(access)?.set(&encoded)?;
            Ok(())
        })?;
        Ok(())
    }

    fn advance(&mut self, command: &str) -> Result<Value, OperationError> {
        let Turn::Ready(prepared) = self.scan.turn(None)? else {
            return Ok(json!({"kind": "idle"}));
        };
        let transaction = self.transactions.begin();
        let before = self.checkpoint.access(transaction.access())?.get()?;
        let tail = self
            .output_tail
            .access(transaction.access())?
            .get()?
            .unwrap_or(0);
        let (action, completion) = prepared.apply(transaction.access())?;
        let after = self.checkpoint.access(transaction.access())?.get()?;
        let checkpoint_present = after.is_some();
        let checkpoint_changed = before != after;
        let phase = self.phase.access(transaction.access())?.get()?;
        let spool_nonempty = !self.spool.access(transaction.access())?.is_empty()?;
        let has_output = match action {
            Action::Idle => return Ok(json!({"kind": "idle"})),
            Action::Commit(Some(change)) => {
                self.output
                    .access(transaction.access())?
                    .put(&tail, &encode_change(&change)?)?;
                let next = tail.checked_add(1).ok_or("gate output tail exhausted")?;
                self.output_tail.access(transaction.access())?.set(&next)?;
                true
            }
            Action::Commit(None) => false,
            Action::Complete(_) => return Err("a Scan cannot complete an input".into()),
        };
        transaction.commit()?;
        if command == "crash-terminal-capture"
            && !has_output
            && checkpoint_present
            && spool_nonempty
            && phase == Some(2)
        {
            respond(&json!({
                "kind": "durable-terminal-capture", "checkpoint_present": checkpoint_present,
                "commits": 1,
            }))?;
            process::exit(76);
        }
        if command == "crash-before-ack" && has_output && checkpoint_changed && phase == Some(3) {
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
        let page = self.output.access(transaction.access())?.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(DIAGNOSTIC_OUTPUT_ITEMS, DIAGNOSTIC_OUTPUT_BYTES)?,
        )?;
        if page.continuation.is_some() {
            return Err("gate output exceeded the bounded diagnostic scan".into());
        }
        let mut rows = Vec::new();
        for (_, encoded) in page.entries {
            let change = decode_change(&encoded)?;
            if change.num_rows() > DIAGNOSTIC_OUTPUT_ROWS - rows.len() {
                return Err("gate output exceeded the diagnostic row limit".into());
            }
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
                    payloads.value(row),
                ]));
            }
        }
        let checkpoint_present = self
            .checkpoint
            .access(transaction.access())?
            .get()?
            .is_some();
        let phase = self.phase.access(transaction.access())?.get()?;
        let spool_nonempty = !self.spool.access(transaction.access())?.is_empty()?;
        Ok(json!({
            "kind": "rows", "rows": rows,
            "checkpoint_present": checkpoint_present,
            "phase": phase, "spool_nonempty": spool_nonempty,
        }))
    }
}
