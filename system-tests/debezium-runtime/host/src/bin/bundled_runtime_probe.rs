use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use dogpaddle_debezium::{Checkpoint, Connector, ConnectorConfig, Delivery, Header, Record};
use serde_json::Value;

const CONNECTOR_CLASS: &str = "dev.dogpaddle.debezium.probe.LifecycleProbeConnector";
const ENGINE_NAME: &str = "dogpaddle-native-bundle-lifecycle-probe";
const TOPIC: &str = "dogpaddle-lifecycle-probe";
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

type HeaderSnapshot = (Box<str>, Option<Box<[u8]>>);

#[derive(Debug, Eq, PartialEq)]
struct RecordSnapshot {
    topic: Option<Box<str>>,
    kafka_partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Box<[u8]>>,
    value: Option<Box<[u8]>>,
    headers: Box<[HeaderSnapshot]>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let bundle = match (arguments.next(), arguments.next()) {
        (Some(bundle), None) => PathBuf::from(bundle),
        _ => return Err(probe_error("usage: bundled_runtime_probe BUNDLE_ROOT")),
    };

    let runtime = dogpaddle_debezium::DebeziumRuntime::open(&bundle)?;
    let config = ConnectorConfig::new(ENGINE_NAME, CONNECTOR_CLASS)?;
    let mut connector = runtime.start(config, None)?;

    let (checkpoint, records) = {
        let delivery = required_delivery(&mut connector)?;
        verify_fixture_record(&delivery, 1)?;
        let checkpoint = delivery.checkpoint().as_bytes().to_vec();
        require(!checkpoint.is_empty(), "delivery checkpoint is empty")?;
        let records = snapshot(delivery.records());
        (checkpoint, records)
    };

    let repeated = required_delivery(&mut connector)?;
    verify_fixture_record(&repeated, 1)?;
    require(
        repeated.checkpoint().as_bytes() == checkpoint,
        "dropping a delivery changed its repeated checkpoint",
    )?;
    require(
        snapshot(repeated.records()) == records,
        "dropping a delivery changed its repeated records",
    )?;
    repeated.ack()?;

    connector.stop(STOP_TIMEOUT)?;

    let checkpoint = Checkpoint::from_bytes(checkpoint)?;
    let config = ConnectorConfig::new(ENGINE_NAME, CONNECTOR_CLASS)?;
    let mut restored = runtime.start(config, Some(&checkpoint))?;
    let witness = required_delivery(&mut restored)?;
    verify_fixture_record(&witness, 2)?;
    require(
        witness.checkpoint().as_bytes() != checkpoint.as_bytes(),
        "checkpoint restore witness did not advance the checkpoint",
    )?;
    witness.ack()?;
    restored.stop(STOP_TIMEOUT)?;

    println!(
        "PASS bundled Debezium public lifecycle: {}",
        bundle.display()
    );
    Ok(())
}

fn required_delivery(connector: &mut Connector) -> Result<Delivery<'_>, Box<dyn Error>> {
    connector
        .poll(POLL_TIMEOUT)?
        .ok_or_else(|| probe_error("lifecycle probe timed out before delivering a record"))
}

fn verify_fixture_record(
    delivery: &Delivery<'_>,
    expected_position: i64,
) -> Result<(), Box<dyn Error>> {
    let [record] = delivery.records() else {
        return Err(probe_error(
            "lifecycle probe delivery must contain one record",
        ));
    };
    require(record.topic() == Some(TOPIC), "unexpected record topic")?;
    require(
        record.kafka_partition() == Some(7),
        "unexpected record Kafka partition",
    )?;
    require(
        record.timestamp() == Some(1_700_000_000_000 + expected_position),
        "unexpected record timestamp",
    )?;
    require_json_payload(
        record.key(),
        &format!("probe-key-{expected_position}"),
        "record key",
    )?;
    require_json_payload(
        record.value(),
        &format!("probe-value-{expected_position}"),
        "record value",
    )?;

    let [first, second] = record.headers() else {
        return Err(probe_error(
            "lifecycle probe record must contain two headers",
        ));
    };
    verify_header(
        first,
        "probe-header-a",
        &format!("header-a-{expected_position}"),
    )?;
    verify_header(
        second,
        "probe-header-b",
        &format!("header-b-{expected_position}"),
    )?;
    Ok(())
}

fn verify_header(
    header: &Header,
    expected_key: &str,
    expected_payload: &str,
) -> Result<(), Box<dyn Error>> {
    require(header.key() == expected_key, "unexpected record header key")?;
    require_json_payload(header.value(), expected_payload, "record header")
}

fn require_json_payload(
    bytes: Option<&[u8]>,
    expected: &str,
    label: &str,
) -> Result<(), Box<dyn Error>> {
    let bytes = bytes.ok_or_else(|| probe_error(format!("{label} is null")))?;
    let document: Value = serde_json::from_slice(bytes)?;
    require(
        document.get("payload").and_then(Value::as_str) == Some(expected),
        format!("{label} has an unexpected schemas-enabled JSON payload"),
    )
}

fn snapshot(records: &[Record]) -> Box<[RecordSnapshot]> {
    records
        .iter()
        .map(|record| RecordSnapshot {
            topic: record.topic().map(Into::into),
            kafka_partition: record.kafka_partition(),
            timestamp: record.timestamp(),
            key: record.key().map(Into::into),
            value: record.value().map(Into::into),
            headers: record
                .headers()
                .iter()
                .map(|header| {
                    (
                        Box::<str>::from(header.key()),
                        header.value().map(Into::into),
                    )
                })
                .collect(),
        })
        .collect()
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), Box<dyn Error>> {
    if condition {
        Ok(())
    } else {
        Err(probe_error(message))
    }
}

fn probe_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::other(message.into()))
}
