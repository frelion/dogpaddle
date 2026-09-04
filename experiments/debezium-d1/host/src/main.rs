mod bridge;
mod cli;
mod error;

use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::bridge::Bridge;
use crate::cli::{Arguments, Command, DEFAULT_STOP_TIMEOUT_MS};
use crate::error::HostError;

struct Session {
    bridge: Bridge,
    configuration: Vec<u8>,
    handle: i64,
}

impl Session {
    fn create(bridge: Bridge, configuration: Vec<u8>) -> Result<Self, HostError> {
        let handle = bridge.create(&configuration)?;
        Ok(Self {
            bridge,
            configuration,
            handle,
        })
    }

    fn replace_handle(&mut self, configuration: Option<Vec<u8>>) -> Result<(), HostError> {
        let state = self.state()?;
        if !matches!(state.as_str(), "created" | "stopped" | "failed") {
            return Err(HostError::Usage(format!(
                "create requires a created, stopped, or failed engine; current state is {state}"
            )));
        }
        let candidate = configuration.as_deref().unwrap_or(&self.configuration);
        let handle = self.bridge.create(candidate)?;
        if let Some(configuration) = configuration {
            self.configuration = configuration;
        }
        self.handle = handle;
        Ok(())
    }

    fn start(&mut self, timeout_ms: u64) -> Result<Value, HostError> {
        let current = self.state()?;
        if matches!(current.as_str(), "stopped" | "failed") {
            self.handle = self.bridge.create(&self.configuration)?;
        }
        self.bridge.start(self.handle)?;
        self.wait_until_started(timeout_ms)
    }

    fn wait_until_started(&self, timeout_ms: u64) -> Result<Value, HostError> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(timeout_ms))
            .ok_or_else(|| HostError::Usage("start timeout is too large".to_owned()))?;
        loop {
            let status = self.status()?;
            match state_from(&status)? {
                "running" => return Ok(as_kind(status, "state")),
                "failed" | "stopped" => {
                    return Err(HostError::Usage(format!(
                        "engine did not start: {}",
                        serde_json::to_string(&status)?
                    )));
                }
                _ if Instant::now() >= deadline => {
                    return Err(HostError::Usage(format!(
                        "engine did not reach running state within {timeout_ms}ms"
                    )));
                }
                _ => thread::sleep(Duration::from_millis(25)),
            }
        }
    }

    fn state(&self) -> Result<String, HostError> {
        Ok(state_from(&self.status()?)?.to_owned())
    }

    fn status(&self) -> Result<Value, HostError> {
        let mut status = self.bridge.status(self.handle)?;
        set_field(&mut status, "handle", Value::from(self.handle))?;
        set_field(
            &mut status,
            "rust_process_id",
            Value::from(u64::from(std::process::id())),
        )?;
        Ok(status)
    }

    fn response(&self, kind: &str) -> Result<Value, HostError> {
        Ok(as_kind(self.status()?, kind))
    }

    fn error_response(&self, error: &HostError) -> Value {
        self.status()
            .and_then(|status| error_from_status(status, error))
            .unwrap_or_else(|_| error_json(error))
    }
}

fn main() {
    if let Err(error) = run() {
        let _ = emit_to(io::stdout().lock(), &error_json(&error));
        std::process::exit(1);
    }
}

fn run() -> Result<(), HostError> {
    let arguments = Arguments::parse(env::args_os())?;
    validate_file(&arguments.bridge_jar, "bridge JAR")?;
    validate_file(&arguments.config, "connector configuration")?;
    let configuration = fs::read(&arguments.config)?;
    let bridge = Bridge::launch(&arguments)?;
    let mut session = Session::create(bridge, configuration)?;

    let stdin = io::stdin();
    let mut output = BufWriter::new(io::stdout().lock());
    emit_to(&mut output, &session.response("state")?)?;

    for line in BufReader::new(stdin.lock()).lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let command = match Command::parse(line) {
            Ok(command) => command,
            Err(error) => {
                emit_to(&mut output, &session.error_response(&error))?;
                continue;
            }
        };
        let quit = matches!(&command, Command::Quit { .. });
        let response =
            execute(&mut session, command).unwrap_or_else(|error| session.error_response(&error));
        emit_to(&mut output, &response)?;
        if quit {
            break;
        }
    }

    let state = session.state().unwrap_or_else(|_| "unknown".to_owned());
    if !matches!(state.as_str(), "created" | "stopped" | "failed") {
        let _ = session.bridge.stop(session.handle, DEFAULT_STOP_TIMEOUT_MS);
    }
    Ok(())
}

fn execute(session: &mut Session, command: Command) -> Result<Value, HostError> {
    match command {
        Command::Create { config } => {
            let configuration = config.map(fs::read).transpose()?;
            session.replace_handle(configuration)?;
            session.response("state")
        }
        Command::Start { timeout_ms } => session.start(timeout_ms),
        Command::Poll {
            timeout_ms,
            max_bytes,
        } => {
            if let Some(bytes) = session.bridge.poll(session.handle, timeout_ms, max_bytes)? {
                Ok(serde_json::from_slice(&bytes)?)
            } else {
                let mut status = session.status()?;
                if status
                    .get("outstanding")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    let bytes = session
                        .bridge
                        .poll(session.handle, 0, max_bytes)?
                        .ok_or_else(|| {
                            HostError::Usage(
                                "bridge status reported an outstanding delivery that poll could not read"
                                    .to_owned(),
                            )
                        })?;
                    return Ok(serde_json::from_slice(&bytes)?);
                }
                set_field(&mut status, "kind", Value::String("idle".to_owned()))?;
                Ok(status)
            }
        }
        Command::Ack { token } => {
            session.bridge.ack(session.handle, token)?;
            let mut status = session.response("ack")?;
            if status
                .get("outstanding")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let outstanding_token = status.get("token").cloned().unwrap_or(Value::Null);
                set_field(&mut status, "outstanding_token", outstanding_token)?;
            }
            set_field(&mut status, "token", Value::from(token))?;
            Ok(status)
        }
        Command::Status => session.response("status"),
        Command::Stop { timeout_ms } => {
            session.bridge.stop(session.handle, timeout_ms)?;
            session.response("state")
        }
        Command::Quit { timeout_ms } => {
            session.bridge.stop(session.handle, timeout_ms)?;
            session.response("bye")
        }
    }
}

fn state_from(status: &Value) -> Result<&str, HostError> {
    status
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| HostError::Usage("bridge status has no string state".to_owned()))
}

fn as_kind(mut value: Value, kind: &str) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("kind".to_owned(), Value::String(kind.to_owned()));
    }
    value
}

fn set_field(value: &mut Value, name: &str, field: Value) -> Result<(), HostError> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| HostError::Usage("bridge returned a non-object JSON value".to_owned()))?;
    object.insert(name.to_owned(), field);
    Ok(())
}

fn error_json(error: &HostError) -> Value {
    json!({
        "protocol": 1,
        "kind": "error",
        "state": "error",
        "rust_process_id": std::process::id(),
        "message": error.to_string()
    })
}

fn error_from_status(mut status: Value, error: &HostError) -> Result<Value, HostError> {
    set_field(&mut status, "kind", Value::String("error".to_owned()))?;
    set_field(&mut status, "message", Value::String(error.to_string()))?;
    Ok(status)
}

fn emit_to(mut writer: impl Write, value: &Value) -> Result<(), HostError> {
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn validate_file(path: &Path, description: &str) -> Result<(), HostError> {
    if !path.is_file() {
        return Err(HostError::Usage(format!(
            "{description} does not exist: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_output_does_not_include_connector_configuration() {
        let value = error_json(&HostError::Usage("bad command".to_owned()));

        assert_eq!(value["kind"], "error");
        assert_eq!(value["message"], "bad command");
        assert!(value.get("outstanding").is_none());
        assert_eq!(value["rust_process_id"], std::process::id());
    }

    #[test]
    fn runtime_error_preserves_the_actual_status() {
        let status = json!({
            "kind": "status",
            "state": "running",
            "outstanding": true,
            "token": 9,
            "handle": 2
        });

        let value = error_from_status(status, &HostError::Usage("bad ACK".to_owned())).unwrap();

        assert_eq!(value["kind"], "error");
        assert_eq!(value["state"], "running");
        assert_eq!(value["outstanding"], true);
        assert_eq!(value["token"], 9);
        assert_eq!(value["handle"], 2);
        assert_eq!(value["message"], "bad ACK");
    }

    #[test]
    fn changing_kind_preserves_status_fields() {
        let status = json!({"kind":"status", "state":"running", "token":7});

        let state = as_kind(status, "state");

        assert_eq!(state["kind"], "state");
        assert_eq!(state["state"], "running");
        assert_eq!(state["token"], 7);
    }

    #[test]
    fn emits_one_compact_json_line() {
        let mut bytes = Vec::new();

        emit_to(&mut bytes, &json!({"kind":"status", "state":"created"})).unwrap();

        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "{\"kind\":\"status\",\"state\":\"created\"}\n"
        );
    }
}
