//! Process-isolated Rust allocation and logical-state evidence for SQL ASOF.
use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col,
    operation::{
        BudgetExceeded, Operation, OperationInput, Progress, StepBudget,
        transform::{AsOfDirection, AsOfEqualityKey, AsOfJoinDefinition, AsOfOrderKey},
    },
};
use dogpaddle_perf_context::{HostEnvironment, PerformanceProfile, RunRoot, require_release_build};
use dogpaddle_store::{OrderedMap, ScanDirection, ScanLimit, Store, StoreSetup, Transactions};
use serde_json::json;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    process::Command,
    sync::Arc,
};

const BENCHMARK: &str = "asof_join_resources";
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;
struct Fixture {
    operation: Operation,
    transactions: Transactions,
}
#[derive(Default)]
struct Measurement {
    pages: usize,
    rows: usize,
    positive: usize,
    negative: usize,
    retries: usize,
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::UInt64, false),
        Field::new("at", DataType::Int64, true),
        Field::new("value", DataType::Int64, false),
    ]))
}
fn definition() -> AsOfJoinDefinition {
    AsOfJoinDefinition::try_new(
        AsOfDirection::Backward { allow_exact: true },
        [AsOfEqualityKey::new(col("group"), col("group"))],
        AsOfOrderKey::new(col("at"), col("at")),
        [
            "left_group",
            "left_at",
            "left_value",
            "right_group",
            "right_at",
            "right_value",
        ],
    )
    .unwrap()
}
fn change(times: Vec<Option<i64>>, values: Vec<i64>) -> Change {
    let rows = times.len();
    Change::try_new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from(vec![7; rows])),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1; rows]),
    )
    .unwrap()
}
impl Fixture {
    fn new(path: &Path) -> Self {
        let mut setup = StoreSetup::new();
        let operation = OperationDefinition::from(definition())
            .construct(
                &[schema(), schema()],
                &mut setup.data_scope().scoped("operation"),
                RuntimeResource::none(),
            )
            .unwrap()
            .into_parts()
            .0;
        let transactions = setup.commit(path, |_| Ok(())).unwrap();
        Self {
            operation,
            transactions,
        }
    }
    fn apply(&mut self, port: usize, input: &Change) -> Measurement {
        let mut result = Measurement::default();
        let mut resume = self.operation.initial_resume();
        loop {
            let mut limit = 256;
            let step = loop {
                let transaction = self.transactions.begin();
                match self.operation.step(
                    OperationInput {
                        port,
                        change: input,
                    },
                    &resume,
                    transaction.access(),
                    &mut StepBudget::new(limit, 4 * 1024 * 1024),
                ) {
                    Ok(step) => {
                        transaction.commit().unwrap();
                        break step;
                    }
                    Err(error) if error.downcast_ref::<BudgetExceeded>().is_some() && limit > 1 => {
                        result.retries += 1;
                        limit /= 2;
                    }
                    Err(error) => panic!("ASOF resource workload failed: {error}"),
                }
            };
            result.pages += 1;
            if let Some(output) = step.output {
                result.rows += output.num_rows();
                for difference in output.diffs().values() {
                    if *difference > 0 {
                        result.positive += 1;
                    } else {
                        result.negative += 1;
                    }
                }
            }
            match step.progress {
                Progress::More(next) => {
                    assert_ne!(next, resume);
                    resume = next;
                }
                Progress::Done => return result,
            }
        }
    }
}
fn state(path: &Path) -> serde_json::Value {
    let store = Store::open(path).unwrap();
    let mut records = Vec::new();
    for name in ["left_rows", "right_rows"] {
        let rows: OrderedMap<Vec<u8>, Vec<u8>> = store
            .open_data(&format!("operation/asof_join.{name}"))
            .unwrap();
        let snapshot = store.read_transaction();
        let access = rows.read(snapshot.access()).unwrap();
        let mut count = 0;
        let mut bytes = 0;
        let mut resume = None;
        loop {
            let page = access
                .scan(
                    ..,
                    ScanDirection::Ascending,
                    resume.as_ref(),
                    ScanLimit::new(256, 4 * 1024 * 1024).unwrap(),
                )
                .unwrap();
            count += page.entries.len();
            bytes += page
                .entries
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
            if let Some(next) = page.continuation {
                resume = Some(next);
            } else {
                break;
            }
        }
        records.push(json!({"collection":name,"entries":count,"encoded_key_value_bytes":bytes}));
    }
    json!(records)
}
fn child(case: &str, rows: usize, path: &Path) -> serde_json::Value {
    let database = path.join("store");
    let mut fixture = Fixture::new(&database);
    let (port, input, expected) = match case {
        "left_lookup_history" => {
            let ordinals = (0..rows)
                .map(|value| i64::try_from(value).unwrap())
                .collect::<Vec<_>>();
            fixture.apply(
                1,
                &change(ordinals.iter().copied().map(Some).collect(), ordinals),
            );
            (
                0,
                change(vec![Some(i64::try_from(rows).unwrap())], vec![1]),
                (1, 0),
            )
        }
        "right_historical_interval" => {
            fixture.apply(1, &change(vec![Some(0)], vec![0]));
            let ordinals = (0..rows)
                .map(|value| i64::try_from(value).unwrap())
                .collect::<Vec<_>>();
            fixture.apply(
                0,
                &change(
                    ordinals.iter().map(|value| Some(value + 100)).collect(),
                    ordinals,
                ),
            );
            (1, change(vec![Some(50)], vec![50]), (rows, rows))
        }
        "right_empty_interval" => {
            fixture.apply(1, &change(vec![Some(0)], vec![0]));
            let ordinals = (0..rows)
                .map(|value| i64::try_from(value).unwrap())
                .collect::<Vec<_>>();
            fixture.apply(
                0,
                &change(
                    ordinals.iter().map(|value| Some(value + 100)).collect(),
                    ordinals,
                ),
            );
            (1, change(vec![Some(-10)], vec![-10]), (0, 0))
        }
        "right_null_left_history" => {
            let ordinals = (0..rows)
                .map(|value| i64::try_from(value).unwrap())
                .collect::<Vec<_>>();
            fixture.apply(0, &change(vec![None; rows], ordinals));
            (1, change(vec![Some(10)], vec![10]), (0, 0))
        }
        _ => panic!("unknown ASOF resource case"),
    };
    let profiler = dhat::Profiler::builder().testing().build();
    let measurement = fixture.apply(port, &input);
    let heap = dhat::HeapStats::get();
    drop(profiler);
    assert_eq!((measurement.positive, measurement.negative), expected);
    if matches!(
        case,
        "right_empty_interval" | "right_null_left_history" | "left_lookup_history"
    ) {
        assert_eq!(measurement.pages, 1);
    }
    drop(fixture);
    json!({"case":case,"history_rows":rows,"pages":measurement.pages,"output_rows":measurement.rows,"failed_attempts":measurement.retries,"rust_heap":{"total_blocks":heap.total_blocks,"total_bytes":heap.total_bytes,"peak_bytes":heap.max_bytes},"persistent_logical_state":state(&database),"rss_bytes":null})
}
fn main() {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "--resource-child")
    {
        let record = child(
            &arguments[2],
            arguments[3].parse().unwrap(),
            Path::new(&arguments[4]),
        );
        println!("{}", serde_json::to_string(&record).unwrap());
        return;
    }
    let profile = PerformanceProfile::for_benchmark();
    let benchmark = arguments.iter().any(|argument| argument == "--bench");
    if benchmark {
        require_release_build(BENCHMARK);
    }
    let root = RunRoot::for_profile(BENCHMARK, profile);
    let rows = if benchmark {
        match profile {
            PerformanceProfile::Smoke => 512,
            PerformanceProfile::Reference => 4096,
        }
    } else {
        257
    };
    let context = json!({"benchmark":BENCHMARK,"profile":profile,"host":HostEnvironment::collect(Some(root.filesystem_root())),"contracts":{"rust_heap":"Fresh child starts dhat after fixture, seed and driving input; covers Rust allocation during complete paged input; excludes RocksDB native heap.","persistent_logical_state":"Encoded key and value lengths after processing, excluding control, WAL and filesystem allocation.","rss":"Not sampled; allocator counters and logical state bytes are not RSS.","comparison":"Only compare identical host, rustc, profile, workload, and baseline epoch; test-mode values only validate workload execution."}});
    fs::write(
        root.path().join("resource-context.json"),
        serde_json::to_vec_pretty(&context).unwrap(),
    )
    .unwrap();
    let mut results = File::create(root.path().join("resources.jsonl")).unwrap();
    let executable = std::env::current_exe().unwrap();
    for case in [
        "left_lookup_history",
        "right_historical_interval",
        "right_empty_interval",
        "right_null_left_history",
    ] {
        let sample = root.sample(case);
        let output = Command::new(&executable)
            .arg("--resource-child")
            .arg(case)
            .arg(rows.to_string())
            .arg(sample.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let record: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let line = serde_json::to_string(&record).unwrap();
        writeln!(results, "{line}").unwrap();
        println!("{line}");
    }
}
