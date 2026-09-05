//! Explicit real-PostgreSQL sink recovery gate host.
//!
//! `tools/check_postgres_sink.py` alone starts the disposable `PostgreSQL`
//! cluster and drives this process one bounded Flow round at a time.

use std::{
    env,
    error::Error,
    io::{self, BufRead, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::{PostgresSinkConfig, PostgresSinkDefinition},
    source::SequenceSourceDefinition,
};
use serde_json::json;

const SOURCE_ID: &str = "sequence";
const SINK_ID: &str = "postgres";
const TARGET_SINK_ID: &str = "gate_sink";
const TARGET_TABLE: &str = "events";
const FIRST_VALUE: u64 = u64::MAX - 2;

type GateError = Box<dyn Error>;

struct Options {
    mode: String,
    path: PathBuf,
    port: u16,
}

impl Options {
    fn read() -> Result<Self, GateError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [mode, path, port] = args.as_slice() else {
            return Err("usage: postgres_sink <build|open> FLOW_PATH PORT".into());
        };
        Ok(Self {
            mode: mode.clone(),
            path: path.into(),
            port: port.parse()?,
        })
    }

    fn config(&self) -> Result<PostgresSinkConfig, GateError> {
        Ok(PostgresSinkConfig::new_unencrypted(
            "127.0.0.1",
            self.port,
            "postgres",
            "dogpaddle_gate",
            env::var("DOGPADDLE_GATE_PASSWORD")?,
        )?)
    }
}

fn main() -> Result<(), GateError> {
    let options = Options::read()?;
    let mode = options.mode.clone();
    let mut flow = match mode.as_str() {
        "build" => build_flow(&options.path, options.config()?)?,
        "open" => open_flow(&options.path, options.config()?)?,
        _ => return Err("mode must be build or open".into()),
    };

    respond(&json!({"kind": "ready", "mode": mode}))?;
    for command in io::stdin().lock().lines() {
        match command?.as_str() {
            "advance" => {
                let response = match flow.advance() {
                    Ok(outcome) => json!({"kind": "advance", "outcome": format!("{outcome:?}")}),
                    Err(error) => {
                        json!({"kind": "error", "message": error.to_string(), "requires_reopen": error.requires_reopen()})
                    }
                };
                respond(&response)?;
            }
            "quit" => break,
            _ => return Err("unsupported gate command".into()),
        }
    }
    Ok(())
}

fn build_flow(path: &Path, config: PostgresSinkConfig) -> Result<Flow, GateError> {
    let target = config.discover_target(TARGET_SINK_ID, "public", TARGET_TABLE)?;
    let mut factory = FlowFactory::new(path);
    let source = factory.station(SOURCE_ID, SequenceSourceDefinition::new(FIRST_VALUE));
    let sink = factory.station(SINK_ID, PostgresSinkDefinition::try_new(target)?);
    factory.output_capacity_bytes(source, NonZeroU64::MAX);
    factory.connect([source], sink);
    factory.resource(SINK_ID, config)?;
    Ok(factory.build()?)
}

fn open_flow(path: &Path, config: PostgresSinkConfig) -> Result<Flow, GateError> {
    let mut factory = FlowFactory::new(path);
    factory.resource(SINK_ID, config)?;
    Ok(factory.open()?)
}

fn respond(value: &serde_json::Value) -> Result<(), GateError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
