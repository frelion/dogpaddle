use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Barrier, mpsc};
use std::time::{Duration, Instant};

use dogpaddle_debezium::{
    Checkpoint, Connector, ConnectorConfig, DebeziumRuntime, Delivery, ErrorKind, Record,
};
use serde_json::Value;

const CONNECTOR_CLASS: &str = "dev.dogpaddle.debezium.probe.LifecycleProbeConnector";
const ENGINE_NAME: &str = "dogpaddle-native-bundle-lifecycle-probe";
const TOPIC: &str = "dogpaddle-lifecycle-probe";
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Eq, PartialEq)]
struct RecordSnapshot {
    topic: Option<Box<str>>,
    value: Option<Box<[u8]>>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let (bundle, measure_open) = match (arguments.next(), arguments.next(), arguments.next()) {
        (Some(bundle), None, None) => (PathBuf::from(bundle), false),
        (Some(bundle), Some(flag), None) if flag == "--measure-open" => {
            (PathBuf::from(bundle), true)
        }
        _ => {
            return Err(probe_error(
                "usage: bundled_runtime_probe BUNDLE_ROOT [--measure-open]",
            ));
        }
    };

    // Match the product host: install its handler before initializing the JVM.
    let (interrupt, interrupted) = mpsc::sync_channel(1);
    ctrlc::set_handler(move || {
        let _ = interrupt.try_send(());
    })?;
    if measure_open {
        return measure_repeated_open(&bundle);
    }

    let runtime = verify_runtime_open(&bundle)?;
    require(
        Command::new("/bin/kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()?
            .success(),
        "failed to send SIGINT to the runtime host",
    )?;
    interrupted
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| probe_error("JVM initialization replaced the host Ctrl-C handler"))?;

    let config = ConnectorConfig::new(ENGINE_NAME, CONNECTOR_CLASS)?;
    let mut connector = runtime.start(config, None)?;

    let (checkpoint, records) = {
        let delivery = required_delivery(&mut connector)?;
        verify_fixture_record(&delivery, 1)?;
        require(
            connector.poll(Duration::ZERO).is_err(),
            "live Delivery allowed a second poll",
        )?;
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
    connector.stop(STOP_TIMEOUT)?;
    require(
        connector.ack(repeated).is_err(),
        "stop did not invalidate the outstanding capability",
    )?;

    let checkpoint = Checkpoint::from_bytes(checkpoint)?;
    let config = ConnectorConfig::new(ENGINE_NAME, CONNECTOR_CLASS)?;
    let mut restored = runtime.start(config, Some(&checkpoint))?;
    let witness = required_delivery(&mut restored)?;
    verify_fixture_record(&witness, 2)?;
    require(
        witness.checkpoint().as_bytes() != checkpoint.as_bytes(),
        "checkpoint restore witness did not advance the checkpoint",
    )?;
    require(
        connector.ack(witness).is_err(),
        "another Connector accepted a foreign capability",
    )?;
    let witness = required_delivery(&mut restored)?;
    verify_fixture_record(&witness, 2)?;
    restored.ack(witness)?;
    restored.stop(STOP_TIMEOUT)?;

    println!(
        "PASS bundled Debezium public lifecycle and host Ctrl-C handler: {}",
        bundle.display()
    );
    Ok(())
}

fn verify_runtime_open(bundle: &Path) -> Result<DebeziumRuntime, Box<dyn Error>> {
    let paths = tempfile::tempdir()?;
    let invalid_bundle = paths.path().join("invalid-bundle");
    fs::create_dir(&invalid_bundle)?;
    fs::write(invalid_bundle.join("MANIFEST"), b"invalid")?;
    require_open_error(&invalid_bundle, ErrorKind::InvalidBundle)?;

    let barrier = Barrier::new(4);
    let runtimes = std::thread::scope(|scope| {
        let workers = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    DebeziumRuntime::open(bundle)
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .map_err(|_| probe_error("concurrent runtime open panicked"))?
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()
    })?;
    let runtime = runtimes
        .into_iter()
        .next()
        .ok_or_else(|| probe_error("concurrent runtime open returned no host"))?;
    drop(DebeziumRuntime::open(bundle)?);
    let alias = paths.path().join("bundle-alias");
    std::os::unix::fs::symlink(bundle.canonicalize()?, &alias)?;
    drop(DebeziumRuntime::open(&alias)?);
    require_open_error(&invalid_bundle, ErrorKind::JvmConfigurationConflict)?;
    require_open_error(&paths.path().join("missing"), ErrorKind::InvalidBundle)?;
    let file = paths.path().join("file");
    fs::write(&file, b"not a directory")?;
    require_open_error(&file, ErrorKind::InvalidBundle)?;
    Ok(runtime)
}

fn require_open_error(path: &Path, kind: ErrorKind) -> Result<(), Box<dyn Error>> {
    require(
        DebeziumRuntime::open(path)
            .err()
            .is_some_and(|error| error.kind() == kind),
        format!("runtime open did not reject {} as {kind:?}", path.display()),
    )
}

fn measure_repeated_open(bundle: &Path) -> Result<(), Box<dyn Error>> {
    let runtime = DebeziumRuntime::open(bundle)?;
    for sample in 0..32 {
        let started = Instant::now();
        let reopened = DebeziumRuntime::open(bundle)?;
        let elapsed = started.elapsed();
        std::hint::black_box(&reopened);
        drop(reopened);
        println!(
            "repeated_open sample={sample} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
    drop(runtime);
    Ok(())
}

fn required_delivery(connector: &mut Connector) -> Result<Delivery, Box<dyn Error>> {
    connector
        .poll(POLL_TIMEOUT)?
        .ok_or_else(|| probe_error("lifecycle probe timed out before delivering a record"))
}

fn verify_fixture_record(
    delivery: &Delivery,
    expected_position: i64,
) -> Result<(), Box<dyn Error>> {
    let [record] = delivery.records() else {
        return Err(probe_error(
            "lifecycle probe delivery must contain one record",
        ));
    };
    require(record.topic() == Some(TOPIC), "unexpected record topic")?;
    require_json_payload(
        record.value(),
        &format!("probe-value-{expected_position}"),
        "record value",
    )?;

    Ok(())
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
            value: record.value().map(Into::into),
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
