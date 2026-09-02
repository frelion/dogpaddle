use std::{
    io::{self, Write},
    num::NonZeroUsize,
    time::Duration,
};

use serde_json::Value;

use super::*;
use crate::environment::{CommandOutput, macos_filesystem_description};

#[test]
fn benchmark_profiles_have_stable_protocol_names() {
    assert_eq!(BenchmarkProfile::Smoke.as_str(), "smoke");
    assert_eq!(BenchmarkProfile::Reference.as_str(), "reference");
}

#[test]
fn command_output_distinguishes_success_from_unavailability() {
    let rustc = CommandOutput::capture("rustc", &["--version"]);
    assert!(rustc.is_available());
    assert!(matches!(
        rustc,
        CommandOutput::Available(value) if value.starts_with("rustc ")
    ));

    let missing = CommandOutput::capture("dogpaddle-command-that-does-not-exist", &["--version"]);
    assert!(!missing.is_available());
    assert!(matches!(missing, CommandOutput::Unavailable(_)));
}

#[test]
fn macos_filesystem_probe_combines_mount_type_and_device() {
    let usage = "Filesystem 512-blocks Used Available Capacity iused ifree %iused Mounted on\n/dev/disk3s5 100 20 80 20% 1 2 33% /System/Volumes/Data\n";
    let mounts = "/dev/disk3s1 on / (apfs, sealed, local, read-only)\n/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled)\n";
    assert_eq!(
        macos_filesystem_description(usage, mounts).as_deref(),
        Some("apfs (/dev/disk3s5)")
    );
    assert_eq!(macos_filesystem_description(usage, ""), None);
}

#[test]
fn host_environment_serializes_every_required_reproducibility_field() {
    let host = HostEnvironment::collect(None);
    let record = EnvironmentRecord::new(
        "protocol_test",
        BenchmarkProfile::Smoke,
        host,
        Fields::new(),
    );
    let value = emit_one(&record);
    for field in [
        "record",
        "benchmark",
        "cargo_profile",
        "cargo_profile_source",
        "os",
        "arch",
        "kernel",
        "cpu",
        "parallelism",
        "rustc",
        "git_revision",
        "git_state",
        "debug_assertions",
        "unix_seconds",
    ] {
        assert!(value.get(field).is_some(), "missing field {field}");
    }
    assert_eq!(value["record"], "environment");
    assert!(value.get("filesystem_path").is_none());
    assert!(value.get("filesystem").is_none());
}

#[test]
fn fields_reject_invalid_duplicate_and_protocol_owned_names() {
    let mut fields = Fields::new();
    fields.insert("operations", 7);
    let duplicate = panic_message(|| fields.insert("operations", 8));
    assert!(duplicate.contains("stage=insert"));
    assert!(duplicate.contains("field=\"operations\""));

    let reserved = panic_message(|| Fields::new().insert("record", "sample"));
    assert!(reserved.contains("stage=validate_field"));
    assert!(reserved.contains("value=\"record\""));

    let invalid = panic_message(|| Fields::new().insert(" elapsed_ns", 1));
    assert!(invalid.contains("stage=validate_field"));
    assert!(invalid.contains("value=\" elapsed_ns\""));

    let collision = Fields::new().with("elapsed_ns", 9);
    let sample = panic_message(|| {
        SampleRecord::new("bench", "case", 0, Duration::from_nanos(1), collision);
    });
    assert!(sample.contains("stage=construct_sample"));
    assert!(sample.contains("field=\"elapsed_ns\""));

    let collision = Fields::new().with("expected_samples", 2);
    let configuration = panic_message(|| {
        ConfigurationRecord::new("bench", NonZeroUsize::new(1).unwrap(), collision);
    });
    assert!(configuration.contains("stage=construct_configuration"));
    assert!(configuration.contains("field=\"expected_samples\""));
}

#[test]
fn typed_records_emit_compact_single_line_json() {
    let sample = SampleRecord::new(
        "change_codec",
        "decode_full",
        2,
        Duration::from_nanos(123),
        Fields::new().with("operations", 64),
    );
    let mut output = Vec::new();
    JsonlWriter::new(&mut output).write(&sample);
    assert_eq!(output.last(), Some(&b'\n'));
    assert!(!output[..output.len() - 1].contains(&b'\n'));
    let line = std::str::from_utf8(&output).expect("JSONL is Unicode");
    assert!(!line[..line.len() - 1].contains('\n'));
    let value: Value = serde_json::from_slice(&output).expect("valid JSON object");
    assert_eq!(value["record"], "sample");
    assert_eq!(value["benchmark"], "change_codec");
    assert_eq!(value["series"], "decode_full");
    assert_eq!(value["sample"], 2);
    assert_eq!(value["elapsed_ns"], 123);
    assert_eq!(value["operations"], 64);
}

#[test]
fn configuration_and_completion_records_keep_stable_core_fields() {
    let mut configuration = ConfigurationRecord::new(
        "store_append_log",
        NonZeroUsize::new(19).unwrap(),
        Fields::new().with("samples", 9),
    );
    configuration.require_observation("record_bytes=128/checkpoint", NonZeroUsize::new(2).unwrap());
    let value = emit_one(&configuration);
    assert_eq!(value["record"], "configuration");
    assert_eq!(value["expected_samples"], 19);
    assert_eq!(
        value["required_observations"]["record_bytes=128/checkpoint"],
        2
    );

    let completion = CompletionRecord::new("store_append_log");
    let value = emit_one(&completion);
    assert_eq!(value["record"], "completion");
    assert_eq!(value["benchmark"], "store_append_log");
}

#[test]
fn observation_records_keep_stable_identity_and_reject_invalid_envelopes() {
    let record = ObservationRecord::new(
        "store_append_log_endurance",
        "record_bytes=128/terminal",
        0,
        Fields::new().with("validation_checksum", 42_u64),
    );
    let value = emit_one(&record);
    assert_eq!(value["record"], "observation");
    assert_eq!(value["benchmark"], "store_append_log_endurance");
    assert_eq!(value["series"], "record_bytes=128/terminal");
    assert_eq!(value["sample"], 0);
    assert_eq!(value["validation_checksum"], 42);

    let invalid_series = panic_message(|| {
        ObservationRecord::new("benchmark", " checkpoint", 0, Fields::new());
    });
    assert!(invalid_series.contains("stage=construct_observation"));
    assert!(invalid_series.contains("label=series"));

    let reserved = panic_message(|| {
        ObservationRecord::new(
            "benchmark",
            "checkpoint",
            0,
            Fields::new().with("sample", 1),
        );
    });
    assert!(reserved.contains("stage=construct_observation"));
    assert!(reserved.contains("field=\"sample\""));

    let mut configuration =
        ConfigurationRecord::new("benchmark", NonZeroUsize::new(1).unwrap(), Fields::new());
    configuration.require_observation("checkpoint", NonZeroUsize::new(1).unwrap());
    let duplicate_requirement = panic_message(|| {
        configuration.require_observation("checkpoint", NonZeroUsize::new(2).unwrap());
    });
    assert!(duplicate_requirement.contains("stage=construct_configuration"));
    assert!(duplicate_requirement.contains("series is already required"));

    let invalid_label = panic_message(|| {
        CompletionRecord::new(" benchmark");
    });
    assert!(invalid_label.contains("label=benchmark"));
    assert!(invalid_label.contains("value=\" benchmark\""));
}

#[test]
fn duration_statistics_preserve_standard_and_endurance_median_conventions() {
    let samples = [
        Duration::from_nanos(40),
        Duration::from_nanos(10),
        Duration::from_nanos(30),
        Duration::from_nanos(20),
    ];
    let original = samples;
    let summary = DurationSummary::from_samples(&samples);
    assert_eq!(summary.min(), Duration::from_nanos(10));
    assert_eq!(summary.median(), Duration::from_nanos(30));
    assert_eq!(summary.max(), Duration::from_nanos(40));
    assert_eq!(samples, original);

    let latency = LatencySummary::from_samples(&samples);
    assert_eq!(latency.p50(), Duration::from_nanos(20));
    assert_eq!(latency.p95(), Duration::from_nanos(40));
    assert_eq!(latency.p99(), Duration::from_nanos(40));
    assert_eq!(latency.max(), Duration::from_nanos(40));
    let empty = panic_message(|| {
        let _ = DurationSummary::from_samples(&[]);
    });
    assert!(empty.contains("stage=duration_summary"));
    assert!(empty.contains("value=0"));
}

#[test]
fn paired_statistics_reject_invalid_pairs_and_report_semantic_wins() {
    let first = [
        Duration::from_nanos(20),
        Duration::from_nanos(10),
        Duration::from_nanos(30),
    ];
    let second = [
        Duration::from_nanos(10),
        Duration::from_nanos(20),
        Duration::from_nanos(10),
    ];
    let summary = PairedDurationSummary::from_pairs(&first, &second);
    assert!((summary.median_first_over_second() - 2.0).abs() < f64::EPSILON);
    assert_eq!(summary.second_wins(), 2);
    let empty = panic_message(|| {
        let _ = PairedDurationSummary::from_pairs(&[], &[]);
    });
    assert!(empty.contains("value=0"));

    let mismatch = panic_message(|| {
        let _ = PairedDurationSummary::from_pairs(&first, &second[..2]);
    });
    assert!(mismatch.contains("value=first:3,second:2"));

    let zero = panic_message(|| {
        let _ = PairedDurationSummary::from_pairs(&[Duration::ZERO], &[Duration::from_nanos(1)]);
    });
    assert!(zero.contains("value=0ns"));
}

#[test]
fn pair_schedules_are_explicit_and_measurements_remain_semantically_ordered() {
    assert_eq!(
        (0..6)
            .map(|sample| PairSchedule::Alternating.order(sample))
            .collect::<Vec<_>>(),
        [
            PairOrder::Ab,
            PairOrder::Ba,
            PairOrder::Ab,
            PairOrder::Ba,
            PairOrder::Ab,
            PairOrder::Ba,
        ]
    );
    assert_eq!(
        (0..8)
            .map(|sample| PairSchedule::Counterbalanced.order(sample))
            .collect::<Vec<_>>(),
        [
            PairOrder::Ab,
            PairOrder::Ba,
            PairOrder::Ba,
            PairOrder::Ab,
            PairOrder::Ab,
            PairOrder::Ba,
            PairOrder::Ba,
            PairOrder::Ab,
        ]
    );

    let mut calls = Vec::new();
    let measured = measure_pair_with(PairOrder::Ba, |variant| {
        calls.push(variant);
        match variant {
            PairVariant::First => 10,
            PairVariant::Second => 20,
        }
    });
    assert_eq!(calls, [PairVariant::Second, PairVariant::First]);
    assert_eq!(
        measured,
        PairMeasurements {
            first: 10,
            second: 20
        }
    );
}

#[test]
fn jsonl_writer_surfaces_destination_failures() {
    let record = ConfigurationRecord::new("failure", NonZeroUsize::new(1).unwrap(), Fields::new());
    let mut writer = JsonlWriter::new(FailingWriter);
    let write = panic_message(|| writer.write(&record));
    assert!(write.contains("stage=serialize"));
    assert!(write.contains("source=expected failure"));

    let mut writer = JsonlWriter::new(FailingWriter);
    let flush = panic_message(|| writer.flush());
    assert!(flush.contains("stage=flush"));
    assert!(flush.contains("source=expected failure"));
}

fn emit_one(record: &impl BenchmarkRecord) -> Value {
    let mut output = Vec::new();
    JsonlWriter::new(&mut output).write(record);
    serde_json::from_slice(&output).expect("parse emitted JSONL")
}

fn panic_message(action: impl FnOnce()) -> String {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
        .expect_err("operation must panic");
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        panic!("panic payload must be a string")
    }
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("expected failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("expected failure"))
    }
}
