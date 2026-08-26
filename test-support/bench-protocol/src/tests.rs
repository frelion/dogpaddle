use std::{
    cell::RefCell,
    ffi::OsString,
    io::{self, Write},
    time::Duration,
};

use serde_json::Value;

use super::*;
use crate::environment::macos_filesystem_description;
use crate::settings::{
    parse_positive_usize, parse_positive_usize_list, parse_string, parse_string_list,
};

#[test]
fn cargo_profile_defaults_to_bench_and_tracks_an_explicit_name() {
    let default = CargoProfile::parse(None).expect("default Cargo profile");
    assert_eq!(default.name(), "bench");
    assert_eq!(default.source(), CargoProfileSource::Default);

    let explicit =
        CargoProfile::parse(Some(OsString::from("release-lto"))).expect("explicit Cargo profile");
    assert_eq!(explicit.name(), "release-lto");
    assert_eq!(explicit.source(), CargoProfileSource::Environment);
}

#[test]
fn cargo_profile_rejects_ambiguous_names() {
    for value in ["", " bench", "bench ", "ben\nch"] {
        assert!(
            CargoProfile::parse(Some(OsString::from(value))).is_err(),
            "profile {value:?} must be rejected"
        );
    }
}

#[cfg(unix)]
#[test]
fn cargo_profile_rejects_non_unicode() {
    use std::os::unix::ffi::OsStringExt;

    let value = OsString::from_vec(vec![0xff]);
    assert!(matches!(
        CargoProfile::parse(Some(value)),
        Err(EnvError::NotUnicode { .. })
    ));
}

#[test]
fn benchmark_profile_is_a_strict_two_value_protocol() {
    assert_eq!(
        BenchmarkProfile::parse("PROFILE", None).expect("default profile"),
        BenchmarkProfile::Smoke
    );
    assert_eq!(
        BenchmarkProfile::parse("PROFILE", Some(OsString::from("reference")))
            .expect("reference profile"),
        BenchmarkProfile::Reference
    );
    assert!(matches!(
        BenchmarkProfile::parse("PROFILE", Some(OsString::from("full"))),
        Err(EnvError::InvalidProfile { .. })
    ));
}

#[test]
fn positive_settings_reject_zero_signs_and_noncanonical_decimals() {
    assert_eq!(
        parse_positive_usize("ROWS", None, 64).expect("default rows"),
        64
    );
    assert_eq!(
        parse_positive_usize("ROWS", Some(OsString::from("1024")), 64).expect("configured rows"),
        1_024
    );
    for value in ["0", "+1", "-1", "01", " 1", "1 ", "1.0"] {
        assert!(
            parse_positive_usize("ROWS", Some(OsString::from(value)), 64).is_err(),
            "setting {value:?} must be rejected"
        );
    }
    assert!(parse_positive_usize("ROWS", None, 0).is_err());
}

#[test]
fn scalar_strings_are_strict_and_lists_reject_duplicate_dimensions() {
    assert_eq!(
        parse_string("PROFILE", None, "smoke").expect("default string"),
        "smoke"
    );
    assert_eq!(
        parse_string("PROFILE", Some(OsString::from("full")), "smoke").expect("configured string"),
        "full"
    );
    for value in ["", " full", "full ", "fu\nll"] {
        assert!(parse_string("PROFILE", Some(OsString::from(value)), "smoke").is_err());
    }
    assert!(parse_string("PROFILE", None, " bad").is_err());
    assert!(matches!(
        parse_positive_usize_list("ROWS", Some(OsString::from("1,2,1")), &[7]),
        Err(EnvError::DuplicateListItem { index: 2, .. })
    ));
    assert!(matches!(
        parse_string_list(
            "WORKLOADS",
            Some(OsString::from("narrow,nested,narrow")),
            &["default"],
        ),
        Err(EnvError::DuplicateListItem { index: 2, .. })
    ));
}

#[test]
fn list_settings_trim_items_but_never_discard_empty_items() {
    assert_eq!(
        parse_positive_usize_list("ROWS", Some(OsString::from("1, 64,1024")), &[7])
            .expect("integer list"),
        [1, 64, 1_024]
    );
    assert!(parse_positive_usize_list("ROWS", Some(OsString::from("1,,2")), &[7]).is_err());
    assert!(parse_positive_usize_list("ROWS", Some(OsString::from("1,0")), &[7]).is_err());
    assert!(parse_positive_usize_list("ROWS", None, &[]).is_err());

    assert_eq!(
        parse_string_list(
            "WORKLOADS",
            Some(OsString::from("narrow, nested")),
            &["default"],
        )
        .expect("string list"),
        ["narrow", "nested"]
    );
    assert!(parse_string_list("WORKLOADS", Some(OsString::from("narrow,")), &["default"]).is_err());
}

#[test]
fn command_output_distinguishes_success_from_unavailability() {
    let rustc = CommandOutput::capture("rustc", &["--version"]);
    assert!(rustc.is_available());
    assert!(
        rustc
            .value()
            .is_some_and(|value| value.starts_with("rustc "))
    );

    let missing = CommandOutput::capture("dogpaddle-command-that-does-not-exist", &["--version"]);
    assert!(!missing.is_available());
    assert_eq!(missing.value(), None);
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
    let record = EnvironmentRecord::new("protocol_test", host, Fields::new())
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
        Fields::new()
            .with("samples", 9)
            .expect("configuration samples"),
    )
    .expect("configuration record");
    assert_eq!(emit_one(&configuration)["record"], "configuration");

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
    assert_eq!(summary.samples(), 4);
    assert_eq!(summary.min(), Duration::from_nanos(10));
    assert_eq!(summary.median(), Duration::from_nanos(30));
    assert_eq!(summary.max(), Duration::from_nanos(40));
    assert_eq!(samples, original);

    let latency = LatencySummary::from_samples(&samples).expect("latency summary");
    assert_eq!(latency.p50(), Duration::from_nanos(20));
    assert_eq!(latency.p95(), Duration::from_nanos(40));
    assert_eq!(latency.p99(), Duration::from_nanos(40));
    assert_eq!(latency.max(), Duration::from_nanos(40));
    assert_eq!(
        duration_percentile(&samples, 25).expect("p25"),
        Duration::from_nanos(10)
    );
    assert_eq!(
        duration_percentile(&samples, 100).expect("p100"),
        Duration::from_nanos(40)
    );
    assert!(duration_percentile(&samples, 0).is_err());
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
    assert_eq!(summary.samples(), 3);
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

    let calls = RefCell::new(Vec::new());
    let measured = measure_pair(
        PairOrder::Ba,
        || {
            calls.borrow_mut().push('A');
            1
        },
        || {
            calls.borrow_mut().push('B');
            2
        },
    );
    assert_eq!(*calls.borrow(), ['B', 'A']);
    assert_eq!(
        measured,
        PairMeasurements {
            first: 1,
            second: 2
        }
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
    let record = ConfigurationRecord::new("failure", Fields::new()).expect("configuration record");
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
