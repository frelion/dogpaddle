mod cli;
mod error;

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use dogpaddle_debezium::{
    Checkpoint, Connector, ConnectorConfig, DebeziumRuntime, Delivery, Header, Record,
};
use serde_json::{Value, json};

use crate::cli::{Arguments, Command, DEFAULT_STOP_TIMEOUT_MS};
use crate::error::HostError;

const POSTGRES_CONNECTOR: &str = "io.debezium.connector.postgresql.PostgresConnector";

#[derive(Clone)]
struct Configuration {
    properties: BTreeMap<String, String>,
}

impl Configuration {
    fn load(path: &Path) -> Result<Self, HostError> {
        let properties: BTreeMap<String, String> = serde_json::from_slice(&fs::read(path)?)?;
        Self::validate(properties)
    }

    fn validate(properties: BTreeMap<String, String>) -> Result<Self, HostError> {
        if properties.get("connector.class").map(String::as_str) != Some(POSTGRES_CONNECTOR) {
            return Err(HostError::Usage(
                "D1 requires the stock Debezium PostgreSQL connector".to_owned(),
            ));
        }
        if properties.get("lsn.flush.mode").map(String::as_str) != Some("connector") {
            return Err(HostError::Usage(
                "D1 requires lsn.flush.mode=connector".to_owned(),
            ));
        }
        if let Some(key) = properties.keys().find(|key| key.starts_with("offset.")) {
            return Err(HostError::Usage(format!(
                "D1 checkpoint ownership forbids connector property '{key}'"
            )));
        }
        Ok(Self { properties })
    }

    fn connector_config(
        &self,
        maximum_delivery_bytes: usize,
    ) -> Result<ConnectorConfig, HostError> {
        let name = self
            .properties
            .get("name")
            .ok_or_else(|| HostError::Usage("connector configuration has no name".to_owned()))?;
        let class_name = self.properties.get("connector.class").ok_or_else(|| {
            HostError::Usage("connector configuration has no connector.class".to_owned())
        })?;
        let mut config = ConnectorConfig::new(name, class_name)?;
        for (key, value) in &self.properties {
            if key != "name" && key != "connector.class" {
                config = config.property(key, value)?;
            }
        }
        Ok(config.max_delivery_bytes(maximum_delivery_bytes)?)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct HeaderSnapshot {
    key: String,
    value: Option<Vec<u8>>,
}

impl HeaderSnapshot {
    fn capture(header: &Header) -> Self {
        Self {
            key: header.key().to_owned(),
            value: header.value().map(<[u8]>::to_vec),
        }
    }

    fn json(&self) -> Result<Value, HostError> {
        Ok(json!({
            "key": self.key,
            "value": decode_json(self.value.as_deref())?,
        }))
    }
}

#[derive(Clone, PartialEq, Eq)]
struct RecordSnapshot {
    topic: Option<String>,
    kafka_partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<HeaderSnapshot>,
}

impl RecordSnapshot {
    fn capture(record: &Record) -> Self {
        Self {
            topic: record.topic().map(str::to_owned),
            kafka_partition: record.kafka_partition(),
            timestamp: record.timestamp(),
            key: record.key().map(<[u8]>::to_vec),
            value: record.value().map(<[u8]>::to_vec),
            headers: record
                .headers()
                .iter()
                .map(HeaderSnapshot::capture)
                .collect(),
        }
    }

    fn json(&self) -> Result<Value, HostError> {
        let headers = self
            .headers
            .iter()
            .map(HeaderSnapshot::json)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "topic": self.topic,
            "kafka_partition": self.kafka_partition,
            "timestamp": self.timestamp,
            "key": decode_json(self.key.as_deref())?,
            "value": decode_json(self.value.as_deref())?,
            "headers": headers,
        }))
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DeliverySnapshot {
    checkpoint: Vec<u8>,
    records: Vec<RecordSnapshot>,
}

impl DeliverySnapshot {
    fn capture(delivery: &Delivery<'_>) -> Self {
        Self {
            checkpoint: delivery.checkpoint().as_bytes().to_vec(),
            records: delivery
                .records()
                .iter()
                .map(RecordSnapshot::capture)
                .collect(),
        }
    }

    fn json(&self, token: u64) -> Result<Value, HostError> {
        let events = self
            .records
            .iter()
            .map(RecordSnapshot::json)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "protocol": 2,
            "kind": "delivery",
            "token": token,
            "record_count": events.len(),
            "checkpoint": hex(&self.checkpoint),
            "checkpoint_bytes": self.checkpoint.len(),
            "events": events,
        }))
    }
}

#[derive(Clone)]
struct Outstanding {
    token: u64,
    delivery: DeliverySnapshot,
}

struct Session {
    runtime: DebeziumRuntime,
    configuration: Configuration,
    checkpoint_path: PathBuf,
    maximum_delivery_bytes: usize,
    connector: Option<Connector>,
    outstanding: Option<Outstanding>,
    next_token: u64,
    resumed_checkpoint_bytes: usize,
}

impl Session {
    fn new(
        runtime: DebeziumRuntime,
        configuration: Configuration,
        checkpoint_path: PathBuf,
        maximum_delivery_bytes: usize,
    ) -> Self {
        Self {
            runtime,
            configuration,
            checkpoint_path,
            maximum_delivery_bytes,
            connector: None,
            outstanding: None,
            next_token: 1,
            resumed_checkpoint_bytes: 0,
        }
    }

    fn start(&mut self) -> Result<Value, HostError> {
        if self.connector.is_some() {
            return Err(HostError::Usage("connector is already running".to_owned()));
        }
        let checkpoint = read_checkpoint(&self.checkpoint_path)?;
        let resumed_checkpoint_bytes = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.as_bytes().len());
        let config = self
            .configuration
            .connector_config(self.maximum_delivery_bytes)?;
        let connector = self.runtime.start(config, checkpoint.as_ref())?;
        self.connector = Some(connector);
        self.outstanding = None;
        self.resumed_checkpoint_bytes = resumed_checkpoint_bytes;
        self.response("state")
    }

    fn poll(&mut self, timeout_ms: u64) -> Result<Value, HostError> {
        let observed = {
            let connector = self
                .connector
                .as_mut()
                .ok_or_else(|| HostError::Usage("connector is not running".to_owned()))?;
            connector
                .poll(Duration::from_millis(timeout_ms))?
                .as_ref()
                .map(DeliverySnapshot::capture)
        };
        let Some(observed) = observed else {
            return self.response("idle");
        };

        let token = if let Some(outstanding) = &self.outstanding {
            if outstanding.delivery != observed {
                return Err(HostError::Usage(
                    "product runtime changed an unacknowledged delivery".to_owned(),
                ));
            }
            outstanding.token
        } else {
            let token = self.next_token;
            self.next_token = self.next_token.checked_add(1).ok_or_else(|| {
                HostError::Usage("run-local token sequence is exhausted".to_owned())
            })?;
            self.outstanding = Some(Outstanding {
                token,
                delivery: observed,
            });
            token
        };
        self.outstanding
            .as_ref()
            .expect("outstanding delivery was just installed")
            .delivery
            .json(token)
    }

    fn save(&self) -> Result<Value, HostError> {
        let outstanding = self
            .outstanding
            .as_ref()
            .ok_or_else(|| HostError::Usage("there is no outstanding delivery".to_owned()))?;
        persist_checkpoint(&self.checkpoint_path, &outstanding.delivery.checkpoint)?;
        Ok(json!({
            "protocol": 2,
            "kind": "saved",
            "state": "running",
            "token": outstanding.token,
            "checkpoint_bytes": outstanding.delivery.checkpoint.len(),
        }))
    }

    fn ack(&mut self, token: u64) -> Result<Value, HostError> {
        let expected = self
            .outstanding
            .clone()
            .ok_or_else(|| HostError::Usage("there is no outstanding delivery".to_owned()))?;
        if expected.token != token {
            return Err(HostError::Usage(format!(
                "ACK token {token} does not match run-local token {}",
                expected.token
            )));
        }
        let persisted = fs::read(&self.checkpoint_path).map_err(|error| {
            HostError::Usage(format!("checkpoint must be saved before ACK: {error}"))
        })?;
        if persisted != expected.delivery.checkpoint {
            return Err(HostError::Usage(
                "persisted checkpoint does not match the outstanding delivery".to_owned(),
            ));
        }

        {
            let connector = self
                .connector
                .as_mut()
                .ok_or_else(|| HostError::Usage("connector is not running".to_owned()))?;
            let delivery = connector
                .poll(Duration::ZERO)?
                .ok_or_else(|| HostError::Usage("outstanding delivery disappeared".to_owned()))?;
            let observed = DeliverySnapshot::capture(&delivery);
            if observed != expected.delivery {
                return Err(HostError::Usage("delivery changed before ACK".to_owned()));
            }
            delivery.ack()?;
        }
        self.outstanding = None;
        let mut response = self.status();
        set_field(&mut response, "kind", Value::String("ack".to_owned()))?;
        set_field(&mut response, "token", Value::from(token))?;
        Ok(response)
    }

    fn stop(&mut self, timeout_ms: u64) -> Result<Value, HostError> {
        if let Some(connector) = self.connector.as_mut() {
            connector.stop(Duration::from_millis(timeout_ms))?;
        }
        self.connector = None;
        self.outstanding = None;
        self.response("state")
    }

    fn status(&self) -> Value {
        let state = if self.connector.is_some() {
            "running"
        } else {
            "stopped"
        };
        let (outstanding, token, checkpoint_persisted) =
            self.outstanding
                .as_ref()
                .map_or((false, Value::Null, false), |outstanding| {
                    (
                        true,
                        Value::from(outstanding.token),
                        fs::read(&self.checkpoint_path)
                            .is_ok_and(|bytes| bytes == outstanding.delivery.checkpoint),
                    )
                });
        json!({
            "protocol": 2,
            "kind": "status",
            "state": state,
            "outstanding": outstanding,
            "token": token,
            "checkpoint_persisted": checkpoint_persisted,
            "resumed_checkpoint_bytes": self.resumed_checkpoint_bytes,
            "rust_process_id": std::process::id(),
        })
    }

    fn response(&self, kind: &str) -> Result<Value, HostError> {
        let mut response = self.status();
        set_field(&mut response, "kind", Value::String(kind.to_owned()))?;
        Ok(response)
    }

    fn error_response(&self, error: &HostError) -> Value {
        let mut response = self.status();
        if let Some(object) = response.as_object_mut() {
            object.insert("kind".to_owned(), Value::String("error".to_owned()));
            object.insert("message".to_owned(), Value::String(error.to_string()));
        }
        response
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
    validate_directory(&arguments.bundle, "Debezium runtime bundle")?;
    validate_file(&arguments.config, "connector configuration")?;
    if let Some(parent) = arguments.checkpoint.parent() {
        fs::create_dir_all(parent)?;
    }

    let runtime = DebeziumRuntime::open(&arguments.bundle)?;
    let configuration = Configuration::load(&arguments.config)?;
    let mut session = Session::new(
        runtime,
        configuration,
        arguments.checkpoint,
        arguments.max_delivery_bytes,
    );

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
            execute(&mut session, &command).unwrap_or_else(|error| session.error_response(&error));
        emit_to(&mut output, &response)?;
        if quit {
            break;
        }
    }

    if session.connector.is_some() {
        let _ = session.stop(DEFAULT_STOP_TIMEOUT_MS);
    }
    Ok(())
}

fn execute(session: &mut Session, command: &Command) -> Result<Value, HostError> {
    match command {
        Command::Start => session.start(),
        Command::Poll { timeout_ms } => session.poll(*timeout_ms),
        Command::Save => session.save(),
        Command::Ack { token } => session.ack(*token),
        Command::Status => session.response("status"),
        Command::Stop { timeout_ms } => session.stop(*timeout_ms),
        Command::Quit { timeout_ms } => {
            let mut response = session.stop(*timeout_ms)?;
            set_field(&mut response, "kind", Value::String("bye".to_owned()))?;
            Ok(response)
        }
    }
}

fn read_checkpoint(path: &Path) -> Result<Option<Checkpoint>, HostError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(Checkpoint::from_bytes(bytes)?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn persist_checkpoint(path: &Path, bytes: &[u8]) -> Result<(), HostError> {
    let temporary = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn decode_json(bytes: Option<&[u8]>) -> Result<Value, HostError> {
    bytes.map_or(Ok(Value::Null), |bytes| Ok(serde_json::from_slice(bytes)?))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn set_field(value: &mut Value, name: &str, field: Value) -> Result<(), HostError> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| HostError::Usage("host response is not a JSON object".to_owned()))?;
    object.insert(name.to_owned(), field);
    Ok(())
}

fn error_json(error: &HostError) -> Value {
    json!({
        "protocol": 2,
        "kind": "error",
        "state": "error",
        "rust_process_id": std::process::id(),
        "message": error.to_string()
    })
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

fn validate_directory(path: &Path, description: &str) -> Result<(), HostError> {
    if !path.is_dir() {
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

    fn configuration() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("name".to_owned(), "d1".to_owned()),
            ("connector.class".to_owned(), POSTGRES_CONNECTOR.to_owned()),
            ("lsn.flush.mode".to_owned(), "connector".to_owned()),
        ])
    }

    #[test]
    fn d1_configuration_rejects_a_second_offset_truth() {
        let mut properties = configuration();
        properties.insert(
            "offset.storage".to_owned(),
            "org.apache.kafka.connect.storage.FileOffsetBackingStore".to_owned(),
        );

        let result = Configuration::validate(properties);

        assert!(
            matches!(result, Err(HostError::Usage(message)) if message.contains("offset.storage"))
        );
    }

    #[test]
    fn d1_configuration_requires_connector_owned_lsn_flush() {
        let mut properties = configuration();
        properties.insert(
            "lsn.flush.mode".to_owned(),
            "connector_and_driver".to_owned(),
        );

        let result = Configuration::validate(properties);

        assert!(
            matches!(result, Err(HostError::Usage(message)) if message.contains("lsn.flush.mode=connector"))
        );
    }

    #[test]
    fn error_output_is_compact_json_without_configuration_values() {
        let value = error_json(&HostError::Usage("bad command".to_owned()));
        let mut bytes = Vec::new();

        emit_to(&mut bytes, &value).unwrap();

        assert_eq!(value["kind"], "error");
        assert_eq!(value["message"], "bad command");
        assert_eq!(
            String::from_utf8(bytes).unwrap().lines().count(),
            1,
            "one command response must remain one JSONL record"
        );
    }
}
