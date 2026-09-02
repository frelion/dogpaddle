use std::{
    io::{self, Write},
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::*;
use crate::{environment::CommandOutput, jsonl::write_record};

#[test]
fn profile_and_pair_schedules_are_stable() {
    assert_eq!(BenchmarkProfile::Smoke.as_str(), "smoke");
    assert_eq!(BenchmarkProfile::Reference.as_str(), "reference");
    assert_eq!(PairSchedule::Alternating.order(0), PairOrder::Ab);
    assert_eq!(PairSchedule::Alternating.order(1), PairOrder::Ba);
    assert_eq!(PairSchedule::Counterbalanced.order(2), PairOrder::Ba);
    assert_eq!(PairSchedule::Counterbalanced.order(3), PairOrder::Ab);
}

#[test]
fn unavailable_host_probes_remain_data() {
    let rustc = CommandOutput::capture("rustc", &["--version"]);
    assert!(rustc.is_available());
    let missing = CommandOutput::capture("dogpaddle-command-that-does-not-exist", &[]);
    assert!(!missing.is_available());
    let encoded = serde_json::to_string(&missing).unwrap();
    let decoded: CommandOutput = serde_json::from_str(&encoded).unwrap();
    assert!(!decoded.is_available());
}

#[test]
fn fields_reject_invalid_and_duplicate_names() {
    let mut fields = Fields::new();
    fields.insert("operations", 7);
    assert!(panic_message(|| fields.insert("operations", 8)).contains("already present"));
    assert!(panic_message(|| Fields::new().insert(" bad", 1)).contains("benchmark field"));
}

#[test]
fn record_enum_is_the_round_trip_wire_schema() {
    let records = complete_records();
    for record in records {
        let encoded = serde_json::to_string(&record).unwrap();
        let decoded: Record = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(record).unwrap()
        );
    }

    let sample = Record::Sample {
        case: 0,
        sample: 0,
        elapsed_ns: 12,
        fields: Fields::new().with("checksum", 3),
    };
    let value = serde_json::to_value(sample).unwrap();
    assert_eq!(value["record"], "sample");
    assert!(value.get("benchmark").is_none());
    assert!(value.get("series").is_none());
    assert_eq!(value["fields"]["checksum"], 3);
}

#[test]
fn wire_decode_rejects_unknown_fields_and_invalid_labels() {
    let mut completion = serde_json::json!({"record": "completion"});
    completion["unknown"] = serde_json::json!(true);
    assert!(serde_json::from_value::<Record>(completion).is_err());

    let mut run = serde_json::to_value(&complete_records()[0]).unwrap();
    run["benchmark"] = serde_json::json!(" bad");
    assert!(serde_json::from_value::<Record>(run).is_err());

    assert!(
        serde_json::from_value::<CaseSpec>(serde_json::json!({
            "series": "bad\nseries",
            "samples": 1,
            "unknown": 1,
        }))
        .is_err()
    );
    assert!(serde_json::from_value::<Fields>(serde_json::json!({" bad": 1})).is_err());
}

#[test]
fn fnv1a_128_vectors_lock_the_plan_catalog_algorithm() {
    assert_eq!(
        crate::validate::fnv1a_128(b""),
        0x6c62_272e_07bb_0142_62b8_2175_6295_c58d
    );
    assert_eq!(
        crate::validate::fnv1a_128(b"a"),
        0xd228_cb69_6f1a_8caf_7891_2b70_4e4a_8964
    );
    assert_eq!(
        crate::validate::fnv1a_128(b"foobar"),
        0x343e_1662_793c_64bf_6f0d_3597_ba44_6f18
    );
}

#[test]
fn validator_accepts_exact_cases_pairs_and_observations() {
    let output = emit(&complete_records());
    let artifact = RunValidator::validate("protocol_test", "smoke", &output).unwrap();
    assert_eq!(artifact.benchmark(), "protocol_test");
    assert_eq!(artifact.cases().len(), 2);
    assert_eq!(artifact.observations().len(), 1);
}

#[test]
fn catalog_locks_the_exact_canonical_plan() {
    let output = emit(&complete_records());
    let artifact = RunValidator::validate("protocol_test", "smoke", &output).unwrap();
    let fingerprint = serde_json::to_value(artifact.plan_fingerprint()).unwrap();
    let catalog = serde_json::json!({
        "benchmark": "protocol_test",
        "algorithm": "fnv1a-128-canonical-json-v1",
        "smoke": fingerprint,
        "reference": fingerprint,
    });
    RunValidator::validate_catalog(
        "protocol_test",
        "smoke",
        &output,
        &serde_json::to_string(&catalog).unwrap(),
    )
    .unwrap();

    let mut wrong = catalog;
    wrong["smoke"]["digest"] = serde_json::json!("00000000000000000000000000000000");
    let error = RunValidator::validate_catalog(
        "protocol_test",
        "smoke",
        &output,
        &serde_json::to_string(&wrong).unwrap(),
    )
    .unwrap_err();
    assert!(error.contains("target=\"protocol_test\" profile=\"smoke\""));
    assert!(error.contains("expected(cases=2, observations=1"));
    assert!(error.contains("actual(cases=2, observations=1"));
}

#[test]
fn validator_rejects_missing_extra_and_out_of_order_data() {
    let mut missing = complete_records();
    missing.remove(2);
    let error = RunValidator::validate("protocol_test", "smoke", &emit(&missing)).unwrap_err();
    assert!(error.contains("emitted 0 of 1"));

    let mut duplicate = complete_records();
    duplicate.insert(
        2,
        Record::Sample {
            case: 0,
            sample: 0,
            elapsed_ns: 1,
            fields: Fields::new(),
        },
    );
    let error = RunValidator::validate("protocol_test", "smoke", &emit(&duplicate)).unwrap_err();
    assert!(error.contains("contiguous sample index 1"));

    let mut after = complete_records();
    after.push(Record::Completion {});
    let error = RunValidator::validate("protocol_test", "smoke", &emit(&after)).unwrap_err();
    assert!(error.contains("after completion"));
}

#[test]
fn validator_rejects_bad_identity_plan_and_discriminator() {
    let output = emit(&complete_records());
    assert!(
        RunValidator::validate("other", "smoke", &output)
            .unwrap_err()
            .contains("expected \"other\"")
    );
    assert!(
        RunValidator::validate("protocol_test", "reference", &output)
            .unwrap_err()
            .contains("expected reference")
    );

    let bad_pair = vec![
        Record::Run {
            protocol: PROTOCOL_VERSION,
            benchmark: "protocol_test".to_owned(),
            profile: BenchmarkProfile::Smoke,
            host: Box::new(HostEnvironment::collect(None)),
            configuration: Fields::new(),
            cases: vec![
                CaseSpec::new("only-first", NonZeroUsize::MIN, Fields::new())
                    .paired("pair", PairSide::First),
            ],
            observations: vec![],
        },
        Record::Sample {
            case: 0,
            sample: 0,
            elapsed_ns: 1,
            fields: Fields::new(),
        },
        Record::Completion {},
    ];
    assert!(
        RunValidator::validate("protocol_test", "smoke", &emit(&bad_pair))
            .unwrap_err()
            .contains("has no second side")
    );
    let unknown = output.replacen("\"sample\"", "\"future\"", 1);
    assert!(
        RunValidator::validate("protocol_test", "smoke", &unknown)
            .unwrap_err()
            .contains("unknown variant")
    );

    let mut unsorted = complete_records();
    let Record::Run { cases, .. } = &mut unsorted[0] else {
        unreachable!()
    };
    cases.swap(0, 1);
    assert!(
        RunValidator::validate("protocol_test", "smoke", &emit(&unsorted))
            .unwrap_err()
            .contains("canonical lexicographic")
    );
}

#[test]
fn writer_failure_keeps_the_record_stage_visible() {
    let panic = panic_message(|| write_record(&mut FailingWriter, &Record::Completion {}));
    assert!(panic.contains("write benchmark record") || panic.contains("serialize benchmark"));
}

fn complete_records() -> Vec<Record> {
    vec![
        Record::Run {
            protocol: PROTOCOL_VERSION,
            benchmark: "protocol_test".to_owned(),
            profile: BenchmarkProfile::Smoke,
            host: Box::new(HostEnvironment::collect(None)),
            configuration: Fields::new().with("warmups", 1),
            cases: vec![
                CaseSpec::new("first", NonZeroUsize::MIN, Fields::new())
                    .paired("pair", PairSide::First),
                CaseSpec::new("second", NonZeroUsize::MIN, Fields::new())
                    .paired("pair", PairSide::Second),
            ],
            observations: vec![ObservationSpec::new("checkpoint", NonZeroUsize::MIN)],
        },
        Record::Sample {
            case: 0,
            sample: 0,
            elapsed_ns: 10,
            fields: Fields::new(),
        },
        Record::Sample {
            case: 1,
            sample: 0,
            elapsed_ns: 9,
            fields: Fields::new(),
        },
        Record::Observation {
            observation: 0,
            sample: 0,
            fields: Fields::new().with("checksum", 42),
        },
        Record::Completion {},
    ]
}

fn emit(records: &[Record]) -> String {
    let mut output = Vec::new();
    for record in records {
        write_record(&mut output, record);
    }
    String::from_utf8(output).unwrap()
}

fn panic_message(operation: impl FnOnce()) -> String {
    let panic = catch_unwind(AssertUnwindSafe(operation)).expect_err("operation must panic");
    panic.downcast_ref::<String>().map_or_else(
        || panic.downcast_ref::<&str>().unwrap().to_string(),
        Clone::clone,
    )
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected writer failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
