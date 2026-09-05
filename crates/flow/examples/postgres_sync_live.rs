//! Live `PostgreSQL` CDC-to-`PostgreSQL` demo host used by the README recorder.
//!
//! The recorder owns one disposable database with separate `source` and
//! `target` schemas. This host only uses public Flow and Operation APIs and
//! accepts one bounded `advance` command at a time over standard input.

use std::{
    env,
    error::Error,
    io::{self, BufRead, Write},
    num::NonZeroU64,
    path::PathBuf,
};

use dogpaddle_flow::{Flow, FlowFactory};
use dogpaddle_operation::operation::{
    sink::{PostgresSinkConfig, PostgresSinkDefinition},
    source::{PostgresSourceConfig, PostgresSourceDefinition},
};
use serde_json::json;

const SOURCE_ID: &str = "source";
const SINK_ID: &str = "target";
const DATABASE: &str = "postgres";
const SOURCE_SCHEMA: &str = "source";
const TARGET_SCHEMA: &str = "target";
const TABLE: &str = "orders";
const SLOT: &str = "orders_slot";
const PUBLICATION: &str = "orders_publication";
const SOURCE_ENGINE: &str = "readme_source";
const TARGET_SINK_ID: &str = "readme_sink";

type DemoError = Box<dyn Error>;

struct Options {
    mode: String,
    flow_path: PathBuf,
    runtime_bundle: PathBuf,
    port: u16,
}

impl Options {
    fn read() -> Result<Self, DemoError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [mode, flow_path, runtime_bundle, port] = args.as_slice() else {
            return Err(
                "usage: postgres_sync_live <build|open> FLOW_PATH RUNTIME_BUNDLE PORT".into(),
            );
        };
        Ok(Self {
            mode: mode.clone(),
            flow_path: flow_path.into(),
            runtime_bundle: runtime_bundle.into(),
            port: port.parse()?,
        })
    }

    fn source_config(&self) -> Result<PostgresSourceConfig, DemoError> {
        Ok(PostgresSourceConfig::new_unencrypted(
            &self.runtime_bundle,
            "127.0.0.1",
            self.port,
            DATABASE,
            "dogpaddle_demo",
            env::var("DOGPADDLE_SOURCE_PASSWORD")?,
        )?)
    }

    fn sink_config(&self) -> Result<PostgresSinkConfig, DemoError> {
        Ok(PostgresSinkConfig::new_unencrypted(
            "127.0.0.1",
            self.port,
            DATABASE,
            "dogpaddle_demo",
            env::var("DOGPADDLE_TARGET_PASSWORD")?,
        )?)
    }
}

fn main() -> Result<(), DemoError> {
    let options = Options::read()?;
    let mode = options.mode.clone();
    let mut flow = match mode.as_str() {
        "build" => build_flow(&options)?,
        "open" => open_flow(&options)?,
        _ => return Err("mode must be build or open".into()),
    };

    respond(&json!({"kind": "ready", "mode": mode}))?;
    for command in io::stdin().lock().lines() {
        match command?.as_str() {
            "advance" => respond(&json!({
                "kind": "advance",
                "outcome": format!("{:?}", flow.advance()?),
            }))?,
            "quit" => break,
            _ => return Err("unsupported demo command".into()),
        }
    }
    Ok(())
}

fn build_flow(options: &Options) -> Result<Flow, DemoError> {
    let source_config = options.source_config()?;
    let source = PostgresSourceDefinition::try_new(source_config.discover(
        SOURCE_ENGINE,
        SOURCE_SCHEMA,
        TABLE,
        SLOT,
        PUBLICATION,
    )?)?;
    let sink_config = options.sink_config()?;
    let sink = PostgresSinkDefinition::try_new(sink_config.discover_target(
        TARGET_SINK_ID,
        TARGET_SCHEMA,
        TABLE,
    )?)?;

    let mut factory = FlowFactory::new(&options.flow_path);
    let source_station = factory.station(SOURCE_ID, source);
    let sink_station = factory.station(SINK_ID, sink);
    factory.output_capacity_bytes(
        source_station,
        NonZeroU64::new(1024 * 1024).expect("one MiB is nonzero"),
    );
    factory.connect([source_station], sink_station);
    factory.resource(SOURCE_ID, source_config)?;
    factory.resource(SINK_ID, sink_config)?;
    Ok(factory.build()?)
}

fn open_flow(options: &Options) -> Result<Flow, DemoError> {
    let mut factory = FlowFactory::new(&options.flow_path);
    factory.resource(SOURCE_ID, options.source_config()?)?;
    factory.resource(SINK_ID, options.sink_config()?)?;
    Ok(factory.open()?)
}

fn respond(value: &serde_json::Value) -> Result<(), DemoError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
