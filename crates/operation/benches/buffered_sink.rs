//! Buffered `SQLite` Sink workloads, including every synchronous Store commit.

use std::{
    ops::AddAssign,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, Throughput, measurement::WallTime};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{Action, Operation, OperationInput, Turn, sink::SqliteSinkDefinition},
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Store, StoreSetup, Transactions};
use rusqlite::{Connection, OpenFlags};
use serde_json::json;
use tempfile::TempDir;

const BENCHMARK: &str = "buffered_sink";
const TABLE: &str = "events";
const MAX_TURNS: usize = 4_096;
const CROSS_PAYLOAD_BYTES: usize = 4 * 1_024 * 1_024 + 4 * 1_024;
const CROSS_MULTIPLICITY: i64 = 2;

#[derive(Clone, Copy)]
struct Config {
    steady_rows: usize,
    staged_entries: usize,
    large_payload_bytes: usize,
    multiplicity: i64,
    warmup: Duration,
    measurement: Duration,
}

impl Config {
    const fn for_run(profile: PerformanceProfile, is_benchmark: bool) -> Self {
        match (profile, is_benchmark) {
            (PerformanceProfile::Smoke, false) => Self {
                steady_rows: 2,
                staged_entries: 4,
                large_payload_bytes: 4 * 1_024,
                multiplicity: 16,
                warmup: Duration::from_millis(5),
                measurement: Duration::from_millis(10),
            },
            (PerformanceProfile::Smoke, true) => Self {
                steady_rows: 16,
                staged_entries: 16,
                large_payload_bytes: 256 * 1_024,
                multiplicity: 2_048,
                warmup: Duration::from_millis(20),
                measurement: Duration::from_millis(100),
            },
            (PerformanceProfile::Reference, _) => Self {
                steady_rows: 128,
                staged_entries: 64,
                large_payload_bytes: 4 * 1_024 * 1_024,
                multiplicity: 8_192,
                warmup: Duration::from_secs(2),
                measurement: Duration::from_secs(5),
            },
        }
    }
}

#[derive(Clone, Copy, Default)]
struct RunStats {
    turns: u64,
    commits: u64,
    completions: u64,
}

impl AddAssign for RunStats {
    fn add_assign(&mut self, other: Self) {
        self.turns += other.turns;
        self.commits += other.commits;
        self.completions += other.completions;
    }
}

enum Step {
    Idle,
    Committed(Action),
}

struct Fixture {
    definition: SqliteSinkDefinition,
    schema: SchemaRef,
    operation: Option<Operation>,
    transactions: Option<Transactions>,
    sqlite_path: std::path::PathBuf,
    root: TempDir,
}

impl Fixture {
    fn new(root: &RunRoot, scenario: &str, schema: SchemaRef) -> Self {
        let sample = root.sample(scenario);
        let sqlite_path = sample.path().join("target.sqlite");
        let definition =
            SqliteSinkDefinition::try_new(&sqlite_path, TABLE).expect("define SQLite sink");
        let mut setup = StoreSetup::new();
        let (operation, _) = (&definition as &dyn OperationDefinition)
            .construct(
                &[Arc::clone(&schema)],
                &mut setup.data_scope(),
                "operation",
                RuntimeResource::none(),
            )
            .expect("construct SQLite Sink")
            .into_parts();
        let transactions = setup
            .commit(sample.path().join("store"), |_| Ok(()))
            .expect("commit Sink setup");
        let mut fixture = Self {
            definition,
            schema,
            operation: Some(operation),
            transactions: Some(transactions),
            sqlite_path,
            root: sample,
        };
        let initialized = fixture.drain();
        assert_eq!((initialized.turns, initialized.commits), (3, 3));
        fixture.verify_empty();
        fixture
    }

    fn process_claim(&mut self, change: &Change) -> RunStats {
        let mut stats = RunStats::default();
        for _ in 0..MAX_TURNS {
            match self.step(Some(change), &mut stats) {
                Step::Committed(Action::Commit(None)) => {}
                Step::Committed(Action::Complete(None)) => return stats,
                Step::Committed(action) => panic!("unexpected buffered Sink action {action:?}"),
                Step::Idle => panic!("buffered Sink idled with an offered Claim"),
            }
        }
        panic!("buffered Sink failed to complete a bounded Claim")
    }

    fn drain(&mut self) -> RunStats {
        let mut stats = RunStats::default();
        for _ in 0..MAX_TURNS {
            match self.step(None, &mut stats) {
                Step::Idle => return stats,
                Step::Committed(Action::Commit(None)) => {}
                Step::Committed(action) => {
                    panic!("unexpected no-Claim buffered Sink action {action:?}")
                }
            }
        }
        panic!("buffered Sink failed to drain bounded durable work")
    }

    fn round_trip(&mut self, positive: &Change, negative: &Change) -> RunStats {
        let mut stats = self.process_claim(positive);
        stats += self.drain();
        stats += self.process_claim(negative);
        stats += self.drain();
        stats
    }

    fn step(&mut self, change: Option<&Change>, stats: &mut RunStats) -> Step {
        let input = change.map(|change| OperationInput { port: 0, change });
        let operation = self.operation.as_mut().expect("live Sink operation");
        let Turn::Ready(prepared) = operation.turn(input).expect("prepare Sink turn") else {
            return Step::Idle;
        };
        stats.turns += 1;
        let transaction = self
            .transactions
            .as_mut()
            .expect("live Sink transactions")
            .begin();
        let (action, completion) = prepared
            .apply(transaction.access())
            .expect("apply Sink turn");
        if matches!(action, Action::Idle) {
            drop(transaction);
            drop(completion);
            panic!("buffered Sink returned transactional Idle")
        }
        transaction.commit().expect("commit Sink turn");
        stats.commits += 1;
        completion.run().expect("complete Sink turn");
        stats.completions += 1;
        Step::Committed(action)
    }

    fn admit_changes(&mut self, changes: &[Change]) -> RunStats {
        let mut total = RunStats::default();
        for change in changes {
            let stats = self.process_claim(change);
            assert_eq!((stats.turns, stats.commits, stats.completions), (1, 1, 1));
            total += stats;
        }
        total
    }

    fn reopen(&mut self) {
        drop(self.operation.take());
        drop(self.transactions.take());
        let store = Store::open(self.root.path().join("store")).expect("reopen Sink store");
        self.operation = Some(construct_reopened(&self.definition, &self.schema, &store));
        self.transactions = Some(store.into_transactions());
    }

    fn verify_empty(&mut self) {
        let mut stats = RunStats::default();
        assert!(matches!(self.step(None, &mut stats), Step::Idle));
        assert_eq!(stats.turns, 0);
        let connection =
            Connection::open_with_flags(&self.sqlite_path, OpenFlags::SQLITE_OPEN_READ_WRITE)
                .expect("open benchmark SQLite target");
        let rows: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {TABLE}"), [], |row| {
                row.get(0)
            })
            .expect("count benchmark target rows");
        assert_eq!(rows, 0);
    }
}

fn construct_reopened(
    definition: &SqliteSinkDefinition,
    schema: &SchemaRef,
    store: &Store,
) -> Operation {
    (definition as &dyn OperationDefinition)
        .construct(
            &[Arc::clone(schema)],
            &mut store.data_scope(),
            "operation",
            RuntimeResource::none(),
        )
        .expect("open SQLite Sink")
        .into_parts()
        .0
}

fn integer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn integer_change(schema: &SchemaRef, values: Vec<i64>, difference: i64) -> Change {
    let rows = values.len();
    Change::try_new(
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])
            .expect("build integer records"),
        Int64Array::from(vec![difference; rows]),
    )
    .expect("build integer Change")
}

fn string_change(schema: &SchemaRef, value: &str, difference: i64) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .expect("build string records"),
        Int64Array::from(vec![difference]),
    )
    .expect("build string Change")
}

fn benchmark_round_trip(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    scenario: &str,
    positive: &Change,
    negative: &Change,
    minimum_batches_per_direction: u64,
) {
    let events = positive
        .diffs()
        .values()
        .iter()
        .chain(negative.diffs().values())
        .map(|diff| diff.unsigned_abs())
        .sum();
    group.throughput(Throughput::Elements(events));
    let mut fixture = Fixture::new(root, scenario, positive.records().schema());
    let warmup = fixture.round_trip(positive, negative);
    let minimum_turns = 2 + 6 * minimum_batches_per_direction;
    assert!(warmup.turns >= minimum_turns);
    assert_eq!(warmup.turns, warmup.commits);
    assert_eq!(warmup.commits, warmup.completions);
    fixture.verify_empty();
    group.bench_function(scenario, |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let stats = fixture.round_trip(positive, negative);
                elapsed += started.elapsed();
                assert!(stats.turns >= minimum_turns);
                assert_eq!(stats.turns, stats.commits);
                assert_eq!(stats.commits, stats.completions);
                fixture.verify_empty();
            }
            elapsed
        });
    });
}

fn benchmark_multi_entry(group: &mut BenchmarkGroup<'_, WallTime>, root: &RunRoot, entries: usize) {
    let schema = integer_schema();
    let changes = (0..entries)
        .map(|entry| {
            let value = i64::try_from(entry / 2).expect("entry value fits i64");
            integer_change(&schema, vec![value], if entry % 2 == 0 { 1 } else { -1 })
        })
        .collect::<Vec<_>>();
    let events = changes
        .iter()
        .flat_map(|change| change.diffs().values())
        .map(|diff| diff.unsigned_abs())
        .sum();
    let entries_u64 = u64::try_from(entries).expect("entry count fits u64");
    group.throughput(Throughput::Elements(events));
    let mut fixture = Fixture::new(root, "multi_entry_batch", schema);
    let mut warmup = fixture.admit_changes(&changes);
    warmup += fixture.drain();
    assert_eq!(warmup.turns, entries_u64 + 3);
    fixture.verify_empty();
    group.bench_function(BenchmarkId::new("multi_entry_batch", entries), |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let mut stats = fixture.admit_changes(&changes);
                stats += fixture.drain();
                elapsed += started.elapsed();
                assert_eq!(stats.turns, entries_u64 + 3);
                assert_eq!(stats.turns, stats.commits);
                assert_eq!(stats.commits, stats.completions);
                fixture.verify_empty();
            }
            elapsed
        });
    });
}

fn benchmark_restore_validation(
    group: &mut BenchmarkGroup<'_, WallTime>,
    root: &RunRoot,
    entries: usize,
) {
    let schema = integer_schema();
    let changes = (0..entries)
        .map(|entry| {
            let value = i64::try_from(entry / 2).expect("entry value fits i64");
            integer_change(&schema, vec![value], if entry % 2 == 0 { 1 } else { -1 })
        })
        .collect::<Vec<_>>();
    let entries_u64 = u64::try_from(entries).expect("entry count fits u64");
    group.throughput(Throughput::Elements(entries_u64));
    let mut fixture = Fixture::new(root, "restore_validation", schema);

    let staged = fixture.admit_changes(&changes);
    assert_eq!(staged.turns, entries_u64);
    fixture.reopen();
    let mut restored = RunStats::default();
    assert!(matches!(
        fixture.step(None, &mut restored),
        Step::Committed(Action::Commit(None))
    ));
    assert_eq!(
        (restored.turns, restored.commits, restored.completions),
        (1, 1, 1)
    );
    let drained = fixture.drain();
    assert_eq!(
        (drained.turns, drained.commits, drained.completions),
        (3, 3, 3)
    );
    fixture.verify_empty();

    group.bench_function(BenchmarkId::new("restore_validation", entries), |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let staged = fixture.admit_changes(&changes);
                assert_eq!(staged.turns, entries_u64);

                let started = Instant::now();
                fixture.reopen();
                let mut restored = RunStats::default();
                assert!(matches!(
                    fixture.step(None, &mut restored),
                    Step::Committed(Action::Commit(None))
                ));
                elapsed += started.elapsed();
                assert_eq!(
                    (restored.turns, restored.commits, restored.completions),
                    (1, 1, 1)
                );

                let drained = fixture.drain();
                assert_eq!(
                    (drained.turns, drained.commits, drained.completions),
                    (3, 3, 3)
                );
                fixture.verify_empty();
            }
            elapsed
        });
    });
}

fn write_context(root: &RunRoot, profile: PerformanceProfile, config: Config, mode: &str) {
    let context = json!({
        "benchmark": BENCHMARK,
        "profile": profile,
        "result_directory": root.path().display().to_string(),
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "configuration": {
            "mode": mode,
            "steady_rows": config.steady_rows,
            "staged_entries": config.staged_entries,
            "large_payload_bytes": config.large_payload_bytes,
            "multiplicity": config.multiplicity,
            "cross_payload_bytes": CROSS_PAYLOAD_BYTES,
            "cross_multiplicity": CROSS_MULTIPLICITY,
            "criterion_samples": 10,
            "warmup_ms": config.warmup.as_millis(),
            "measurement_ms": config.measurement.as_millis(),
            "timed_boundaries": {
                "steady_small_admission_drain": "positive and negative Claim admission plus every load/plan/deliver/settle turn, synchronous Store commit, and AfterCommit target write",
                "multi_entry_batch": "all per-Claim durable admissions followed by every load/plan/deliver/settle turn; no reopen is part of this case",
                "restore_validation": "Store reopen, Operation bind/materialize, and the first no-Claim restore turn that validates the entire durable buffer and synchronously commits",
                "large_payload_small_event": "positive and negative Claim admission plus full target delivery and settlement",
                "large_payload_multiplicity_target_slicing": "positive and negative Claim admission plus full target delivery and settlement across at least two target-byte-bounded batches per direction",
                "high_multiplicity_finite_capacity_churn": "positive and negative Claim admission plus all 1024-event target batches and settlements"
            },
            "untimed_boundaries": {
                "all_cases": "fixture and target initialization, Criterion warmup validation, target relation oracle, and teardown",
                "restore_validation": "durable input staging before reopen and delivery/settlement after the first validated restore turn"
            },
            "cases": {
                "steady_small_admission_drain": "each small Claim admitted and fully drained before the next",
                "multi_entry_batch": "multiple complete Claims admitted before one delivery, with admission included in the timed sample",
                "restore_validation": "reopen a staged multi-entry buffer and validate all retained entries in the first restore turn",
                "large_payload_small_event": "one large UTF-8 value with unit multiplicity",
                "large_payload_multiplicity_target_slicing": "a 4 MiB-plus UTF-8 value at multiplicity two, forcing at least two target-byte-bounded batches without unbounded amplification",
                "high_multiplicity_finite_capacity_churn": "one logical row split across bounded 1024-event SQLite deliveries"
            }
        }
    });
    std::fs::write(
        root.path().join("context.json"),
        serde_json::to_vec_pretty(&context).expect("encode benchmark context"),
    )
    .expect("write benchmark context");
}

fn main() {
    let is_benchmark = std::env::args_os().any(|argument| argument == "--bench");
    let profile = PerformanceProfile::for_benchmark();
    if is_benchmark {
        require_release_build(BENCHMARK);
    }
    let config = Config::for_run(profile, is_benchmark);
    let root = RunRoot::for_profile(BENCHMARK, profile);
    write_context(
        &root,
        profile,
        config,
        if is_benchmark { "benchmark" } else { "test" },
    );
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(config.warmup)
        .measurement_time(config.measurement)
        .without_plots()
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let mut group = criterion.benchmark_group(BENCHMARK);

    let integer = integer_schema();
    let values = (0..config.steady_rows)
        .map(|value| i64::try_from(value).expect("steady value fits i64"))
        .collect::<Vec<_>>();
    benchmark_round_trip(
        &mut group,
        &root,
        "steady_small_admission_drain",
        &integer_change(&integer, values.clone(), 1),
        &integer_change(&integer, values, -1),
        1,
    );

    benchmark_multi_entry(&mut group, &root, config.staged_entries);
    benchmark_restore_validation(&mut group, &root, config.staged_entries);

    let strings = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let payload = "x".repeat(config.large_payload_bytes);
    benchmark_round_trip(
        &mut group,
        &root,
        "large_payload_small_event",
        &string_change(&strings, &payload, 1),
        &string_change(&strings, &payload, -1),
        1,
    );

    let cross_payload = "x".repeat(CROSS_PAYLOAD_BYTES);
    benchmark_round_trip(
        &mut group,
        &root,
        "large_payload_multiplicity_target_slicing",
        &string_change(&strings, &cross_payload, CROSS_MULTIPLICITY),
        &string_change(&strings, &cross_payload, -CROSS_MULTIPLICITY),
        2,
    );

    let multiplicity = integer_schema();
    benchmark_round_trip(
        &mut group,
        &root,
        "high_multiplicity_finite_capacity_churn",
        &integer_change(&multiplicity, vec![7], config.multiplicity),
        &integer_change(&multiplicity, vec![7], -config.multiplicity),
        1,
    );

    group.finish();
    criterion.final_summary();
}
