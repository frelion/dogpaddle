//! Explicit real-`PostgreSQL` SQL gate host.
//!
//! `system-tests/postgres/check_sql.py` owns the disposable database and drives this
//! process one bounded Flow scheduling round at a time.

use std::{
    env,
    error::Error,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use dogpaddle_sql::SqlProgram;
use serde_json::json;

type HostError = Box<dyn Error>;

struct Options {
    mode: String,
    sql_path: PathBuf,
    flow_path: PathBuf,
}

impl Options {
    fn read() -> Result<Self, HostError> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let [mode, sql_path, flow_path] = args.as_slice() else {
            return Err("usage: postgres_sql <first|restart> SQL_FILE FLOW_PATH".into());
        };
        Ok(Self {
            mode: mode.clone(),
            sql_path: sql_path.into(),
            flow_path: flow_path.into(),
        })
    }
}

fn main() -> Result<(), HostError> {
    let options = Options::read()?;
    let program = SqlProgram::read(&options.sql_path)?;
    if !matches!(options.mode.as_str(), "first" | "restart") {
        return Err("mode must be first or restart".into());
    }
    let mut flow = program.start(&options.flow_path)?;

    respond(&json!({"kind": "ready", "mode": options.mode}))?;
    for command in io::stdin().lock().lines() {
        match command?.as_str() {
            "advance" => {
                let response = match flow.advance() {
                    Ok(outcome) => advance_response(&flow, outcome)?,
                    Err(error) => json!({
                        "kind": "error",
                        "message": error.to_string(),
                        "requires_reopen": error.requires_reopen(),
                    }),
                };
                respond(&response)?;
            }
            "quit" => break,
            _ => return Err("unsupported host command".into()),
        }
    }
    Ok(())
}

fn advance_response(
    flow: &dogpaddle_flow::Flow,
    outcome: dogpaddle_flow::AdvanceOutcome,
) -> Result<serde_json::Value, HostError> {
    let status = flow.status()?;
    Ok(
        json!({"kind": "advance", "outcome": format!("{outcome:?}"), "flow": {"depth": status.depth, "active_operation": status.active_operation, "sending": status.sending, "needs_reopen": status.needs_reopen}}),
    )
}

fn respond(value: &serde_json::Value) -> Result<(), HostError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
