//! CDC sealed input consumption and reset, including synchronous commits but no external I/O.
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch};
use criterion::{BenchmarkId, Criterion};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{
        Operation,
        scan::{MySqlCdcScanConfig, PostgresCdcScanConfig},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{Cell, Queue, ReadTransactions, Store, StoreSetup, Transactions};
use serde_json::json;
use tempfile::TempDir;

const RESET_BATCH_ENTRIES: usize = 256;
const BATCHED_RESET_ENTRIES: usize = RESET_BATCH_ENTRIES + 1;
const RESET_ROWS_PER_ENTRY: usize = 1;

#[derive(Clone, Copy)]
enum Source {
    Postgres,
    MySql,
}

impl Source {
    const fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
        }
    }

    fn definition(self) -> OperationDefinition {
        let payload: &[u8] = match self {
            Self::Postgres => br#"{"postgres_cdc_scan":{"spec":{"engine_name":"orders","database":"shop","schema":"public","table":"orders","slot":"orders_slot","publication":"orders_pub","system_identifier":"123","database_oid":42,"table_oid":43,"columns":[{"name":"id","data_type":"Int64","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}]},"output_projection":[0],"bootstrap_spool_bytes":1048576}}"#,
            Self::MySql => br#"{"mysql_cdc_scan":{"spec":{"engine_name":"orders","database":"shop","table":"orders","server_uuid":"01234567-89ab-cdef-0123-456789abcdef","table_id":43,"columns":[{"name":"id","data_type":"Int64","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}]},"output_projection":[0],"bootstrap_spool_bytes":1048576}}"#,
        };
        let mut bytes = Vec::new();
        bytes.extend_from_slice(payload);
        serde_json::from_slice::<dogpaddle_operation::OperationDefinition>(&bytes).unwrap()
    }

    fn resource(self) -> RuntimeResource {
        match self {
            Self::Postgres => RuntimeResource::new(
                PostgresCdcScanConfig::new_unencrypted(
                    "/nonexistent/cdc-benchmark-runtime",
                    "127.0.0.1",
                    1,
                    "shop",
                    "cdc",
                    "unused",
                )
                .unwrap(),
            ),
            Self::MySql => RuntimeResource::new(
                MySqlCdcScanConfig::new_unencrypted(
                    "/nonexistent/cdc-benchmark-runtime",
                    "127.0.0.1",
                    1,
                    "shop",
                    "cdc",
                    "unused",
                )
                .unwrap(),
            ),
        }
    }

    fn checkpoint(self) -> Vec<u8> {
        let hex = match self {
            Self::Postgres => concat!(
                "44504442435030310001000000066f7264657273",
                "00000032696f2e646562657a69756d2e636f6e6e6563746f722e706f737467726573716c2e",
                "506f737467726573436f6e6e6563746f720000000051504dd9"
            ),
            Self::MySql => concat!(
                "44504442435030310001000000066f72646572730000002a",
                "696f2e646562657a69756d2e636f6e6e6563746f722e6d7973716c2e4d7953716c",
                "436f6e6e6563746f7200000001000000056d7973716c00000003000102bc51316d"
            ),
        };
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
}

struct Fixture {
    operation: Operation,
    codec: SchemaBoundChangeCodec,
    transactions: Transactions,
    reads: ReadTransactions,
    phase: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    input: Queue<Vec<u8>>,
    expected: Vec<Change>,
    expected_checkpoint: Vec<u8>,
    _root: TempDir,
}

impl Fixture {
    fn new(root: &RunRoot, source: Source, entries: usize, rows: usize, reset: bool) -> Self {
        let sample = root.sample(source.name());
        let path = sample.path().join("store");
        let definition = source.definition();
        let mut setup = StoreSetup::new();
        let built = definition
            .construct(
                &[],
                &mut setup.data_scope().scoped("operation"),
                source.resource(),
            )
            .unwrap();
        let schema = Arc::clone(built.output_schema().unwrap());
        let transactions = setup.commit(&path, |_| Ok(())).unwrap();
        drop((built, transactions));
        let store = Store::open(&path).unwrap();
        let prefix = format!("operation/{}_cdc_scan", source.name());
        let phase: Cell<u32> = store.open_data(&format!("{prefix}.phase")).unwrap();
        let checkpoint: Cell<Vec<u8>> = store.open_data(&format!("{prefix}.checkpoint")).unwrap();
        let input: Queue<Vec<u8>> = store.open_data(&format!("{prefix}.input")).unwrap();
        let (operation, _) = definition
            .construct(
                &[],
                &mut store.data_scope().scoped("operation"),
                source.resource(),
            )
            .unwrap()
            .into_parts();
        let mut transactions = store.into_transactions();
        let codec = SchemaBoundChangeCodec::try_new(Arc::clone(&schema)).unwrap();
        let expected = (0..entries)
            .map(|entry| {
                let values = (0..rows)
                    .map(|row| i64::try_from(entry * rows + row).unwrap())
                    .collect::<Vec<_>>();
                Change::try_new(
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(values))],
                    )
                    .unwrap(),
                    Int64Array::from(vec![1; rows]),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        {
            let transaction = transactions.begin();
            let access = transaction.access();
            phase
                .access(access)
                .unwrap()
                .set(&if reset { 4 } else { 2 })
                .unwrap();
            checkpoint
                .access(access)
                .unwrap()
                .set(&source.checkpoint())
                .unwrap();
            for change in &expected {
                assert!(
                    input
                        .access(access)
                        .unwrap()
                        .try_push(
                            &codec.encode(change).unwrap(),
                            NonZeroU64::new(1_048_576).unwrap()
                        )
                        .unwrap()
                );
            }
            transaction.commit().unwrap();
        }
        let (transactions, reads) = transactions.split();
        Self {
            operation,
            codec,
            transactions,
            reads,
            phase,
            checkpoint,
            input,
            expected,
            expected_checkpoint: source.checkpoint(),
            _root: sample,
        }
    }

    fn run(&mut self, reset: bool) -> Duration {
        let work_turns = if reset {
            self.expected.len().div_ceil(RESET_BATCH_ENTRIES).max(1)
        } else {
            self.expected.len()
        };
        let mut outputs = Vec::with_capacity(work_turns);
        let started = Instant::now();
        let Operation::Source(source) = &mut self.operation else {
            panic!("expected source");
        };
        source.restore(self.reads.begin().access()).unwrap();
        for _ in 0..work_turns {
            if reset {
                let mut delivery = source.poll().unwrap().expect("reset work");
                let txn = self.transactions.begin();
                assert!(source.record(txn.access(), &mut delivery).unwrap());
                txn.commit().unwrap();
                source.ack(delivery).unwrap();
            }
            let published = source.published(self.reads.begin().access()).unwrap();
            outputs.push(published.map(|encoded| self.codec.decode_owned(encoded).unwrap()));
            if outputs.last().unwrap().is_some() {
                let txn = self.transactions.begin();
                source.consume_published(txn.access()).unwrap();
                txn.commit().unwrap();
            }
        }
        let elapsed = started.elapsed();
        if reset {
            assert!(outputs.iter().all(Option::is_none));
        } else {
            for (output, expected) in outputs.into_iter().zip(&self.expected) {
                let actual = output.expect("published entry");
                assert_eq!(actual.records(), expected.records());
                assert_eq!(actual.diffs(), expected.diffs());
            }
        }
        let transaction = self.transactions.begin();
        let access = transaction.access();
        assert!(self.input.access(access).unwrap().is_empty().unwrap());
        assert_eq!(
            self.phase.access(access).unwrap().get().unwrap(),
            if reset { None } else { Some(2) }
        );
        let checkpoint = self.checkpoint.access(access).unwrap().get().unwrap();
        assert_eq!(
            checkpoint.as_deref(),
            if reset {
                None
            } else {
                Some(self.expected_checkpoint.as_slice())
            }
        );
        elapsed
    }
}

fn main() {
    let profile = PerformanceProfile::for_benchmark();
    let is_benchmark = std::env::args_os().any(|arg| arg == "--bench");
    if is_benchmark {
        require_release_build("cdc_bootstrap");
    }
    let (publish_entries, publish_rows, wide_rows_per_entry) = match (profile, is_benchmark) {
        (PerformanceProfile::Smoke, false) => (2, 2, 512),
        (PerformanceProfile::Smoke, true) => (8, 64, 16_384),
        (PerformanceProfile::Reference, _) => (32, 256, 32_768),
    };
    let reset_batches = BATCHED_RESET_ENTRIES.div_ceil(RESET_BATCH_ENTRIES);
    let root = RunRoot::for_profile("cdc_bootstrap", profile);
    std::fs::write(root.path().join("context.json"), serde_json::to_vec_pretty(&json!({
        "benchmark": "cdc_bootstrap", "profile": profile,
        "host": HostEnvironment::collect(Some(root.filesystem_root())),
        "input_entry_format": "schema-bound Change entry without a repeated Arrow Schema",
        "reset_batch_entries": RESET_BATCH_ENTRIES,
        "publish_entries": publish_entries, "publish_rows_per_entry": publish_rows,
        "reset_entries": BATCHED_RESET_ENTRIES, "reset_rows_per_entry": RESET_ROWS_PER_ENTRY,
        "wide_entries": 1, "wide_rows_per_entry": wide_rows_per_entry,
        "restore_calls_per_iteration": 1, "publish_sync_commits_per_iteration": publish_entries,
        "reset_batches_per_iteration": reset_batches, "reset_sync_commits_per_iteration": reset_batches,
        "wide_publish_sync_commits": 1,
        "wide_reset_sync_commits": 1,
        "cases": ["postgres/publish", "postgres/reset", "postgres/publish_wide", "postgres/reset_wide", "mysql/publish", "mysql/reset", "mysql/publish_wide", "mysql/reset_wide"],
        "timed_boundary": "one read-only restore; each sealed entry reads/decodes input front and consumes with one synchronous commit; no payload transfer or publication transaction; each reset batch records, synchronously commits and ACKs maintenance; output Changes retained for untimed validation",
        "untimed": "Definition decoding, construction, Store creation/open, fixture encoding/seed, output and durable-state oracle, teardown",
        "external_io": "none; stops with an empty Sealed input or Fresh before connector start",
        "limitations": "does not measure capture, connector polling, real Delivery ACK, source cleanup, or Flow computation/routing"
    })).unwrap()).unwrap();
    println!("cdc_bootstrap artifacts: {}", root.path().display());
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(20))
        .measurement_time(match profile {
            PerformanceProfile::Smoke => Duration::from_millis(100),
            PerformanceProfile::Reference => Duration::from_secs(5),
        })
        .output_directory(&root.path().join("criterion"))
        .configure_from_args();
    let mut group = criterion.benchmark_group("cdc_bootstrap");
    for source in [Source::Postgres, Source::MySql] {
        for (name, case_entries, case_rows, reset) in [
            ("publish", publish_entries, publish_rows, false),
            ("reset", BATCHED_RESET_ENTRIES, RESET_ROWS_PER_ENTRY, true),
            ("publish_wide", 1, wide_rows_per_entry, false),
            ("reset_wide", 1, wide_rows_per_entry, true),
        ] {
            group.bench_function(BenchmarkId::new(source.name(), name), |bencher| {
                bencher.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut fixture =
                            Fixture::new(&root, source, case_entries, case_rows, reset);
                        elapsed += fixture.run(reset);
                    }
                    elapsed
                });
            });
        }
    }
    group.finish();
    criterion.final_summary();
}
