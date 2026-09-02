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
    let host = HostEnvironment::collect(None).expect("collect host environment");
    let record = EnvironmentRecord::new(
        "protocol_test",
        BenchmarkProfile::Smoke,
        host,
        Fields::new(),
    )
    .expect("construct environment record");
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
    fields.insert("operations", 7).expect("first field");
    assert!(matches!(
        fields.insert("operations", 8),
        Err(FieldError::Duplicate(_))
    ));
    assert!(matches!(
        Fields::new().insert("record", "sample"),
        Err(FieldError::ReservedRecord)
    ));
    assert!(matches!(
        Fields::new().insert(" elapsed_ns", 1),
        Err(FieldError::InvalidName(_))
    ));

    let collision = Fields::new()
        .with("elapsed_ns", 9)
        .expect("valid extension field before envelope validation");
    assert!(matches!(
        SampleRecord::new("bench", "case", 0, Duration::from_nanos(1), collision),
        Err(RecordError::ReservedField(field)) if field == "elapsed_ns"
    ));

    let collision = Fields::new()
        .with("expected_data_records", 2)
        .expect("valid extension field before envelope validation");
    assert!(matches!(
        ConfigurationRecord::new("bench", NonZeroUsize::new(1).unwrap(), collision),
        Err(RecordError::ReservedField(field)) if field == "expected_data_records"
    ));
}

#[test]
fn typed_records_emit_compact_single_line_json() {
    let sample = SampleRecord::new(
        "change_codec",
        "decode_full",
        2,
        Duration::from_nanos(123),
        Fields::new()
            .with("operations", 64)
            .expect("sample work field"),
    )
    .expect("sample record");
    let mut output = Vec::new();
    JsonlWriter::new(&mut output)
        .write(&sample)
        .expect("write JSONL sample");
    assert_eq!(output.last(), Some(&b'\n'));
    assert!(!output[..output.len() - 1].contains(&b'\n'));
    let line = std::str::from_utf8(&output).expect("JSONL is Unicode");
    assert!(!line[..line.len() - 1].contains('\n'));
    let value: Value = serde_json::from_slice(&output).expect("valid JSON object");
    assert_eq!(value["record"], "sample");
    assert_eq!(value["benchmark"], "change_codec");
    assert_eq!(value["scenario"], "decode_full");
    assert_eq!(value["sample"], 2);
    assert_eq!(value["elapsed_ns"], 123);
    assert_eq!(value["operations"], 64);
}

#[test]
fn configuration_summary_and_pair_records_keep_stable_core_fields() {
    let configuration = ConfigurationRecord::new(
        "store_append_log",
        NonZeroUsize::new(19).unwrap(),
        Fields::new()
            .with("samples", 9)
            .expect("configuration samples"),
    )
    .expect("configuration record");
    let value = emit_one(&configuration);
    assert_eq!(value["record"], "configuration");
    assert_eq!(value["expected_data_records"], 19);

    let completion = CompletionRecord::new("store_append_log").expect("completion record");
    let value = emit_one(&completion);
    assert_eq!(value["record"], "completion");
    assert_eq!(value["benchmark"], "store_append_log");

    let samples = [
        Duration::from_nanos(30),
        Duration::from_nanos(10),
        Duration::from_nanos(20),
    ];
    let summary = DurationSummary::from_samples(&samples).expect("duration summary");
    let summary = SummaryRecord::new("store_append_log", "append", summary, Fields::new())
        .expect("summary record");
    let value = emit_one(&summary);
    assert_eq!(value["samples"], 3);
    assert_eq!(value["min_ns"], 10);
    assert_eq!(value["median_ns"], 20);
    assert_eq!(value["max_ns"], 30);

    let first = [Duration::from_nanos(20), Duration::from_nanos(10)];
    let second = [Duration::from_nanos(10), Duration::from_nanos(20)];
    let paired = PairedDurationSummary::from_pairs(&first, &second).expect("paired summary");
    let paired = PairSummaryRecord::new(
        "store_append_log",
        "decode",
        "full",
        "projected",
        paired,
        Fields::new(),
    )
    .expect("pair record");
    let value = emit_one(&paired);
    assert_eq!(value["record"], "pair_summary");
    assert_eq!(value["first_variant"], "full");
    assert_eq!(value["second_variant"], "projected");
    assert_eq!(value["samples"], 2);
    assert_eq!(value["second_wins"], 1);
    assert_eq!(value["median_first_over_second"], 2.0);
}

#[test]
fn extension_records_preserve_stable_uncommon_discriminators() {
    let record = ExtensionRecord::new(
        "endurance_summary",
        "store_append_log_endurance",
        Fields::new()
            .with("validation_checksum", 42_u64)
            .expect("endurance field"),
    )
    .expect("extension record");
    let value = emit_one(&record);
    assert_eq!(value["record"], "endurance_summary");
    assert_eq!(value["benchmark"], "store_append_log_endurance");
    assert_eq!(value["validation_checksum"], 42);

    for invalid in ["", "Checkpoint", "pair-summary", "_checkpoint"] {
        assert!(matches!(
            ExtensionRecord::new(invalid, "benchmark", Fields::new()),
            Err(RecordError::InvalidDiscriminator(_))
        ));
    }
    for reserved in [
        "environment",
        "configuration",
        "sample",
        "summary",
        "pair_summary",
        "completion",
    ] {
        assert!(matches!(
            ExtensionRecord::new(reserved, "benchmark", Fields::new()),
            Err(RecordError::ReservedDiscriminator(_))
        ));
    }
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
    let summary = DurationSummary::from_samples(&samples).expect("duration summary");
    assert_eq!(summary.min(), Duration::from_nanos(10));
    assert_eq!(summary.median(), Duration::from_nanos(30));
    assert_eq!(summary.max(), Duration::from_nanos(40));
    assert_eq!(samples, original);

    let latency = LatencySummary::from_samples(&samples).expect("latency summary");
    assert_eq!(latency.p50(), Duration::from_nanos(20));
    assert_eq!(latency.p95(), Duration::from_nanos(40));
    assert_eq!(latency.p99(), Duration::from_nanos(40));
    assert_eq!(latency.max(), Duration::from_nanos(40));
    assert!(DurationSummary::from_samples(&[]).is_err());
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
    let summary = PairedDurationSummary::from_pairs(&first, &second).expect("paired summary");
    assert!((summary.median_first_over_second() - 2.0).abs() < f64::EPSILON);
    assert_eq!(summary.second_wins(), 2);
    assert!(PairedDurationSummary::from_pairs(&first, &second[..2]).is_err());
    assert!(
        PairedDurationSummary::from_pairs(&[Duration::ZERO], &[Duration::from_nanos(1)]).is_err()
    );
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
    let record = ConfigurationRecord::new("failure", NonZeroUsize::new(1).unwrap(), Fields::new())
        .expect("configuration record");
    let mut writer = JsonlWriter::new(FailingWriter);
    assert!(matches!(
        writer.write(&record),
        Err(JsonlError::Serialize(_))
    ));
}

fn emit_one(record: &impl BenchmarkRecord) -> Value {
    let mut output = Vec::new();
    JsonlWriter::new(&mut output)
        .write(record)
        .expect("emit benchmark record");
    serde_json::from_slice(&output).expect("parse emitted JSONL")
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
