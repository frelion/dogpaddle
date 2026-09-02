#[path = "support/mod.rs"]
mod support;

use std::{hint::black_box, num::NonZeroUsize, path::Path, time::Duration};

use arrow_array::UInt64Array;
use dogpaddle_bench_protocol::{
    BenchmarkProfile, ConfigurationRecord, DurationSummary, Fields, SampleRecord, SummaryRecord,
    require_benchmark_build,
};
use dogpaddle_change::{Change, ChangeProjection, decode_change_projected};
use dogpaddle_change_store_integration::{
    EncodedChanges, heterogeneous_pages_fixture, order_checksum, projectable_fixture,
};
use dogpaddle_store::{
    AppendLog, Cell, CodecError as StoreCodecError, ScanLimit, Store, StoreError,
};

use support::{BenchStoreRoot, complete, decode_entry, emit_record};

const BENCHMARK: &str = "change_append_log";

struct Config {
    rows_per_change: usize,
    changes_per_transaction: usize,
    transactions_per_sample: usize,
    payload_bytes: usize,
    samples: usize,
    warmups: usize,
    max_working_set_bytes: usize,
}

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    pages: usize,
    checksum: u64,
}

fn main() {
    require_benchmark_build(BENCHMARK);
    let stores = BenchStoreRoot::from_environment(BENCHMARK);
    let config = Config::load(stores.profile());
    stores.emit_environment(BENCHMARK);
    emit_configuration(&config);

    println!(
        "Change + AppendLog seam benchmark: profile={} rows/change={} changes/transaction={} transactions/sample={}",
        stores.profile(),
        config.rows_per_change,
        config.changes_per_transaction,
        config.transactions_per_sample
    );

    run_scenario(&config, "append_durable", "heterogeneous_pages", || {
        measure_append(&stores, &config)
    });
    run_scenario(&config, "full_replay", "heterogeneous_pages", || {
        measure_full_replay(&stores, &config)
    });
    run_scenario(&config, "projected_replay", "projectable", || {
        measure_projected_replay(&stores, &config)
    });
    run_scenario(&config, "consumer_durable", "heterogeneous_pages", || {
        measure_consumer(&stores, &config)
    });
    complete(BENCHMARK);
}

impl Config {
    fn load(profile: BenchmarkProfile) -> Self {
        let config = match profile {
            BenchmarkProfile::Smoke => Self {
                rows_per_change: 8,
                changes_per_transaction: 2,
                transactions_per_sample: 2,
                payload_bytes: 16,
                samples: 1,
                warmups: 1,
                max_working_set_bytes: 64 * 1_024 * 1_024,
            },
            BenchmarkProfile::Reference => Self {
                rows_per_change: 1_024,
                changes_per_transaction: 32,
                transactions_per_sample: 8,
                payload_bytes: 256,
                samples: 15,
                warmups: 3,
                max_working_set_bytes: 512 * 1_024 * 1_024,
            },
        };
        assert!(config.total_changes() >= 2, "benchmark needs two Changes");
        config
    }

    fn total_changes(&self) -> usize {
        self.changes_per_transaction
            .checked_mul(self.transactions_per_sample)
            .expect("benchmark Change count fits usize")
    }
}

fn emit_configuration(config: &Config) {
    let fields = Fields::new()
        .with("fixtures", ["heterogeneous_pages", "projectable"])
        .unwrap()
        .with("rows_per_change", config.rows_per_change)
        .unwrap()
        .with("changes_per_transaction", config.changes_per_transaction)
        .unwrap()
        .with("transactions_per_sample", config.transactions_per_sample)
        .unwrap()
        .with("payload_bytes", config.payload_bytes)
        .unwrap()
        .with("samples", config.samples)
        .unwrap()
        .with("warmups", config.warmups)
        .unwrap()
        .with("max_working_set_bytes", config.max_working_set_bytes)
        .unwrap()
        .with("fixture_and_validation", "outside_timing")
        .unwrap();
    let expected_data_records = NonZeroUsize::new(4 * (config.samples + 1)).unwrap();
    emit_record(&ConfigurationRecord::new(BENCHMARK, expected_data_records, fields).unwrap());
}

fn run_scenario(
    config: &Config,
    scenario: &'static str,
    fixture: &'static str,
    mut measure: impl FnMut() -> Measurement,
) {
    for _ in 0..config.warmups {
        black_box(measure());
    }
    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let measurement = measure();
        let fields = scenario_fields(config, fixture)
            .with("observed_pages", measurement.pages)
            .unwrap()
            .with("result_checksum", measurement.checksum)
            .unwrap();
        emit_record(
            &SampleRecord::new(BENCHMARK, scenario, sample, measurement.elapsed, fields).unwrap(),
        );
        durations.push(measurement.elapsed);
    }
    let summary = DurationSummary::from_samples(&durations).unwrap();
    emit_record(
        &SummaryRecord::new(
            BENCHMARK,
            scenario,
            summary,
            scenario_fields(config, fixture),
        )
        .unwrap(),
    );
    println!("{scenario}: median={:?}", summary.median());
}

fn scenario_fields(config: &Config, fixture: &'static str) -> Fields {
    Fields::new()
        .with("fixture", fixture)
        .unwrap()
        .with("rows_per_change", config.rows_per_change)
        .unwrap()
        .with("changes_per_transaction", config.changes_per_transaction)
        .unwrap()
        .with("transactions_per_sample", config.transactions_per_sample)
        .unwrap()
        .with("changes_per_sample", config.total_changes())
        .unwrap()
        .with("payload_bytes", config.payload_bytes)
        .unwrap()
        .with("validation", "outside_timing")
        .unwrap()
}

fn representative_workload(config: &Config) -> EncodedChanges {
    let workload = heterogeneous_pages_fixture(
        config.total_changes(),
        config.rows_per_change,
        config.payload_bytes,
    );
    assert!(
        workload.scan_bytes() <= config.max_working_set_bytes,
        "encoded workload exceeds working-set budget"
    );
    workload
}

fn measure_append(stores: &BenchStoreRoot, config: &Config) -> Measurement {
    let workload = representative_workload(config);
    let sample = stores.sample("append");
    let mut store = Store::create(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();

    let started = std::time::Instant::now();
    for batch in workload.encoded.chunks(config.changes_per_transaction) {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(batch)
            .unwrap();
        transaction.commit().unwrap();
    }
    let elapsed = started.elapsed();
    drop(transactions);
    validate_log(sample.path(), "changes", &workload.encoded);
    Measurement {
        elapsed,
        pages: config.transactions_per_sample,
        checksum: workload.order_checksum(),
    }
}

fn measure_full_replay(stores: &BenchStoreRoot, config: &Config) -> Measurement {
    let workload = representative_workload(config);
    let sample = stores.sample("full-replay");
    seed_log(sample.path(), "changes", &workload.encoded);
    let expected_checksum = changes_checksum(&workload.changes);

    let store = Store::open(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let started = std::time::Instant::now();
    let (pages, checksum) = scan_decoded(
        &access,
        workload.encoded.len(),
        config.changes_per_transaction,
        config.max_working_set_bytes,
        None,
    );
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_checksum);
    drop(transaction);
    drop(transactions);
    validate_log(sample.path(), "changes", &workload.encoded);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

fn measure_projected_replay(stores: &BenchStoreRoot, config: &Config) -> Measurement {
    let fixtures = (0..config.total_changes())
        .map(|index| {
            projectable_fixture(
                10_000 + u64::try_from(index * config.rows_per_change).unwrap(),
                config.rows_per_change,
                config.payload_bytes,
            )
        })
        .collect::<Vec<_>>();
    let encoded = fixtures
        .iter()
        .map(|fixture| fixture.encoded.clone())
        .collect::<Vec<_>>();
    let projected = fixtures
        .iter()
        .map(|fixture| fixture.projected.clone())
        .collect::<Vec<_>>();
    let projection = &fixtures[0].projection;
    assert!(
        encoded
            .iter()
            .map(|entry| entry.len() + size_of::<u64>())
            .sum::<usize>()
            <= config.max_working_set_bytes,
        "encoded workload exceeds working-set budget"
    );
    let sample = stores.sample("projected-replay");
    seed_log(sample.path(), "changes", &encoded);
    let expected_checksum = changes_checksum(&projected);

    let store = Store::open(sample.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let started = std::time::Instant::now();
    let (pages, checksum) = scan_decoded(
        &access,
        encoded.len(),
        config.changes_per_transaction,
        config.max_working_set_bytes,
        Some(projection),
    );
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_checksum);
    drop(transaction);
    drop(transactions);
    validate_log(sample.path(), "changes", &encoded);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

fn measure_consumer(stores: &BenchStoreRoot, config: &Config) -> Measurement {
    let workload = representative_workload(config);
    let sample = stores.sample("consumer");
    let mut store = Store::create(sample.path()).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let cursor: Cell<u64> = store.create_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append_batch(&workload.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }

    let started = std::time::Instant::now();
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0_u64;
    while usize::try_from(offset).unwrap() < workload.encoded.len() {
        let transaction = transactions.begin().unwrap();
        let input_access = input.access(transaction.access()).unwrap();
        let mut output_access = output.access(transaction.access()).unwrap();
        let progress = input_access
            .scan(
                offset,
                ScanLimit::new(config.changes_per_transaction, config.max_working_set_bytes)
                    .unwrap(),
                |entry| {
                    let change = entry.project(decode_entry)?;
                    checksum = mix(checksum, change_checksum(&change));
                    output_access.append_entry(&entry)?;
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        offset = progress.next_offset;
        cursor
            .access(transaction.access())
            .unwrap()
            .set(&offset)
            .unwrap();
        transaction.commit().unwrap();
        pages += 1;
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, changes_checksum(&workload.changes));
    drop(transactions);
    validate_log(sample.path(), "output", &workload.encoded);
    validate_cursor(sample.path(), offset);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

fn seed_log(path: &Path, name: &str, encoded: &[Vec<u8>]) {
    let mut store = Store::create(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data(name).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    log.access(transaction.access())
        .unwrap()
        .append_batch(encoded)
        .unwrap();
    transaction.commit().unwrap();
}

fn validate_log(path: &Path, name: &str, expected: &[Vec<u8>]) {
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data(name).unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(
        access.bounds().unwrap(),
        0..u64::try_from(expected.len()).unwrap()
    );
    let mut raw = Vec::new();
    let progress = access
        .scan(
            0,
            ScanLimit::new(
                expected.len(),
                expected
                    .iter()
                    .map(|entry| entry.len() + size_of::<u64>())
                    .sum(),
            )
            .unwrap(),
            |entry| {
                raw.push(entry.project(|bytes| Ok(bytes.to_vec()))?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert!(progress.caught_up);
    assert_eq!(raw, expected);
    assert_eq!(order_checksum(&raw), order_checksum(expected));
}

fn validate_cursor(path: &Path, expected: u64) {
    let store = Store::open(path).unwrap();
    let cursor: Cell<u64> = store.open_data("cursor").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        cursor.access(transaction.access()).unwrap().get().unwrap(),
        Some(expected)
    );
}

fn scan_decoded(
    access: &dogpaddle_store::AppendLogAccess<'_, Vec<u8>>,
    entries: usize,
    page_items: usize,
    page_bytes: usize,
    projection: Option<&ChangeProjection>,
) -> (usize, u64) {
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0_u64;
    while usize::try_from(offset).unwrap() < entries {
        let progress = access
            .scan(
                offset,
                ScanLimit::new(page_items, page_bytes).unwrap(),
                |entry| {
                    let change = match projection {
                        Some(projection) => entry.project(|bytes| {
                            decode_change_projected(bytes, projection)
                                .map_err(|error| StoreCodecError::new(error.to_string()))
                        })?,
                        None => entry.project(decode_entry)?,
                    };
                    checksum = mix(checksum, change_checksum(&change));
                    black_box(&change);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        offset = progress.next_offset;
        pages += 1;
    }
    (pages, checksum)
}

fn changes_checksum(changes: &[Change]) -> u64 {
    changes.iter().fold(0_u64, |checksum, change| {
        mix(checksum, change_checksum(change))
    })
}

fn change_checksum(change: &Change) -> u64 {
    let ids = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("seam fixtures begin with UInt64 IDs");
    ids.values()
        .iter()
        .copied()
        .zip(change.diffs().values().iter().copied())
        .fold(
            mix(
                change.records().num_columns() as u64,
                change.num_rows() as u64,
            ),
            |checksum, (id, diff)| mix(mix(checksum, id), u64::from_ne_bytes(diff.to_ne_bytes())),
        )
}

const fn mix(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(0x0000_0100_0000_01b3)
}
