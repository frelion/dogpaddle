//! Interactive host for the README's live `PostgreSQL` SQL ETL recording.
//!
//! The recorder sends one bounded `advance` command at a time, then kills and
//! reopens this process to show durable recovery. All Flow construction goes
//! through the public [`SqlProgram`] API and the checked-in SQL file.

use std::{
    env,
    error::Error,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use dogpaddle_sql::SqlProgram;
use serde_json::json;

type ExampleError = Box<dyn Error>;

struct Options {
    mode: String,
    sql_path: PathBuf,
    state_path: PathBuf,
}

impl Options {
    fn read() -> Result<Self, ExampleError> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        let [mode, sql_path, state_path] = arguments.as_slice() else {
            return Err("usage: postgres_etl_live <build|open> SQL_FILE STATE_PATH".into());
        };
        Ok(Self {
            mode: mode.clone(),
            sql_path: sql_path.into(),
            state_path: state_path.into(),
        })
    }
}

fn main() -> Result<(), ExampleError> {
    let options = Options::read()?;
    let program = SqlProgram::read(&options.sql_path)?;
    let mut flow = match options.mode.as_str() {
        "build" => program.build(&options.state_path)?,
        "open" => program.open(&options.state_path)?,
        _ => return Err("mode must be build or open".into()),
    };

    respond(&json!({"kind": "ready", "mode": options.mode}))?;
    for command in io::stdin().lock().lines() {
        match command?.as_str() {
            "advance" => {
                let outcome = flow.advance()?;
                respond(&advance_response(&flow, outcome)?)?;
            }
            "quit" => break,
            _ => return Err("unsupported demo command".into()),
        }
    }
    Ok(())
}

fn advance_response(
    flow: &dogpaddle_flow::Flow,
    outcome: dogpaddle_flow::AdvanceOutcome,
) -> Result<serde_json::Value, ExampleError> {
    let sink = flow
        .status()?
        .into_iter()
        .find(|station| station.id == "sql/sink")
        .ok_or("SQL Flow has no sql/sink Station")?;
    let [input] = sink.inputs.as_slice() else {
        return Err("SQL sink must have exactly one input".into());
    };
    Ok(json!({
        "kind": "advance",
        "outcome": format!("{outcome:?}"),
        "sink": {"cursor": input.cursor, "tail": input.tail},
    }))
}

fn respond(value: &serde_json::Value) -> Result<(), ExampleError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
