use std::{hint::black_box, time::Instant};

use dogpaddle_change::{Change, ChangeProjection, encode_change};
use dogpaddle_change_store_integration::{
    EncodedWorkload, assert_change_eq, encoded_wide_workload,
};
use dogpaddle_store::{AppendLog, ScanLimit, Store, StoreError, Transactions};

#[path = "support/mod.rs"]
mod support;

use support::{
    BenchStoreRoot, SampleWork, checked_product, checked_sum, decode_entry, decode_projected_entry,
    emit_environment, emit_sample, report, setting,
};

const DEFAULT_ROWS_PER_CHANGE: usize = 1_024;
const DEFAULT_CHANGES_PER_TX: usize = 32;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_SAMPLES: usize = 7;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_MAX_WORKING_SET_BYTES: usize = 512 * 1_024 * 1_024;

#[derive(Clone, Copy)]
enum DecodeMode<'projection> {
    Full,
    Projected(&'projection ChangeProjection),
}

#[derive(Clone, Copy)]
struct ScanCase<'case> {
    scenario: &'static str,
    mode: DecodeMode<'case>,
    expected: &'case [Change],
}

struct Config {
    rows_per_change: usize,
    changes_per_tx: usize,
    payload_bytes: usize,
    samples: usize,
    warmups: usize,
    max_working_set_bytes: usize,
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!("change_append_log must run through `cargo bench`");
        return;
    }

    let config = Config {
        rows_per_change: setting(
            "DOGPADDLE_CHANGE_STORE_BENCH_ROWS_PER_CHANGE",
            DEFAULT_ROWS_PER_CHANGE,
        ),
        changes_per_tx: setting(
            "DOGPADDLE_CHANGE_STORE_BENCH_CHANGES_PER_TX",
            DEFAULT_CHANGES_PER_TX,
        ),
        payload_bytes: setting(
            "DOGPADDLE_CHANGE_STORE_BENCH_PAYLOAD_BYTES",
            DEFAULT_PAYLOAD_BYTES,
        ),
        samples: setting("DOGPADDLE_CHANGE_STORE_BENCH_SAMPLES", DEFAULT_SAMPLES),
        warmups: setting("DOGPADDLE_CHANGE_STORE_BENCH_WARMUPS", DEFAULT_WARMUPS),
        max_working_set_bytes: setting(
            "DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES",
            DEFAULT_MAX_WORKING_SET_BYTES,
        ),
    };
    preflight_dimensions(&config);

    let stores = BenchStoreRoot::from_environment();
    emit_environment(
        &stores,
        config.rows_per_change,
        config.changes_per_tx,
        config.payload_bytes,
        config.samples,
        config.warmups,
        config.max_working_set_bytes,
    );
    println!(
        "Change + AppendLog benchmark: profile={} store_base={} rows/change={} changes/tx={} payload_bytes={} samples={} warmups={} max_working_set_bytes={}",
        stores.profile(),
        stores.base().display(),
        config.rows_per_change,
        config.changes_per_tx,
        config.payload_bytes,
        config.samples,
        config.warmups,
        config.max_working_set_bytes,
    );
    println!(
        "controls: DOGPADDLE_CHANGE_STORE_BENCH_PROFILE, _STORE_DIR, _ROWS_PER_CHANGE, _CHANGES_PER_TX, _PAYLOAD_BYTES, _SAMPLES, _WARMUPS, _MAX_WORKING_SET_BYTES"
    );

    let workload = encoded_wide_workload(
        config.rows_per_change,
        config.changes_per_tx,
        config.payload_bytes,
    );
    validate_workload_budget(&config, &workload);
    assert_eq!(workload.changes.len(), config.changes_per_tx);
    for (change, encoded) in workload.changes.iter().zip(&workload.encoded) {
        assert_change_eq(
            &decode_entry(encoded).expect("decode preflight Change"),
            change,
        );
    }

    benchmark_preencoded_append_rollback(&config, &workload, &stores);
    benchmark_preencoded_append_durable(&config, &workload, &stores);
    benchmark_encode_append_durable(&config, &workload, &stores);
    benchmark_append_entry_forward(&config, &workload, &stores);
    benchmark_warm_scans(&config, &workload, &stores);
}

fn preflight_dimensions(config: &Config) {
    let arrow_offset_max = usize::try_from(i32::MAX).expect("i32::MAX fits usize");
    let payload_per_change = checked_product(
        "payload bytes per Change",
        config.rows_per_change,
        config.payload_bytes,
    );
    assert!(
        payload_per_change <= arrow_offset_max,
        "one Binary column requires {payload_per_change} bytes, exceeding Arrow's i32 offset limit {arrow_offset_max}"
    );
    assert!(
        config.rows_per_change < arrow_offset_max,
        "rows per Change exceed Arrow's i32 Binary offset count"
    );

    let total_rows = checked_product(
        "total benchmark rows",
        config.rows_per_change,
        config.changes_per_tx,
    );
    u64::try_from(total_rows).expect("total event ids must fit u64");

    // The fixture holds logical Arrow buffers and complete encoded streams at
    // once, while fixture construction temporarily holds one additional payload
    // copy. This intentionally conservative bound prevents an accidental env
    // setting from exhausting the benchmark host before Arrow can reject it.
    let total_payload = checked_product(
        "total benchmark payload bytes",
        payload_per_change,
        config.changes_per_tx,
    );
    let payload_budget = checked_product("payload working-set estimate", total_payload, 4);
    let row_budget = checked_product("row working-set estimate", total_rows, 128);
    let entry_budget = checked_product(
        "entry working-set estimate",
        config.changes_per_tx,
        8 * 1_024,
    );
    let estimate = checked_sum(
        "benchmark working-set estimate",
        checked_sum("benchmark working-set estimate", payload_budget, row_budget),
        entry_budget,
    );
    assert!(
        estimate <= config.max_working_set_bytes,
        "estimated working set {estimate} exceeds configured maximum {}; raise DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES deliberately",
        config.max_working_set_bytes
    );
}

fn validate_workload_budget(config: &Config, workload: &EncodedWorkload) {
    assert_eq!(workload.rows_per_change, config.rows_per_change);
    assert_eq!(workload.changes.len(), config.changes_per_tx);
    assert!(
        workload.encoded_bytes <= config.max_working_set_bytes,
        "encoded workload {} exceeds configured working-set maximum {}",
        workload.encoded_bytes,
        config.max_working_set_bytes
    );
    assert!(
        workload.scan_bytes() <= config.max_working_set_bytes,
        "one exact scan batch exceeds configured working-set maximum"
    );
}

fn benchmark_preencoded_append_rollback(
    config: &Config,
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
) {
    let scenario = "preencoded_append_body_rollback";
    let work = append_work(workload);
    for _ in 0..config.warmups {
        black_box(run_preencoded_append_rollback(workload, stores, scenario));
    }
    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = run_preencoded_append_rollback(workload, stores, scenario);
        emit_sample(scenario, sample, elapsed, work);
        durations.push(elapsed);
    }
    report(scenario, &mut durations, work);
}

fn run_preencoded_append_rollback(
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
    scenario: &str,
) -> std::time::Duration {
    let sample_store = stores.sample(scenario);
    let mut store = Store::create(sample_store.path()).expect("create pre-encoded sample Store");
    let log: AppendLog<Vec<u8>> = store
        .create_data("changes")
        .expect("create pre-encoded sample log");
    let mut transactions = store.into_transactions();
    let elapsed = {
        let transaction = transactions.begin().expect("begin append transaction");
        let mut access = log
            .access(transaction.access())
            .expect("access append benchmark log");
        let started = Instant::now();
        let offsets = access
            .append_batch(&workload.encoded)
            .expect("append pre-encoded Changes");
        let elapsed = started.elapsed();
        black_box(offsets);
        elapsed
    };
    let transaction = transactions
        .begin()
        .expect("begin rollback verification transaction");
    assert_eq!(
        log.access(transaction.access())
            .expect("access rollback verification log")
            .bounds()
            .expect("read rollback verification bounds"),
        0..0
    );
    elapsed
}

fn benchmark_preencoded_append_durable(
    config: &Config,
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
) {
    let scenario = "preencoded_append_durable_commit";
    let work = append_work(workload);
    for _ in 0..config.warmups {
        black_box(run_preencoded_append_durable(workload, stores, scenario));
    }
    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = run_preencoded_append_durable(workload, stores, scenario);
        emit_sample(scenario, sample, elapsed, work);
        durations.push(elapsed);
    }
    report(scenario, &mut durations, work);
}

fn run_preencoded_append_durable(
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
    scenario: &str,
) -> std::time::Duration {
    let sample_store = stores.sample(scenario);
    let mut store = Store::create(sample_store.path()).expect("create durable sample Store");
    let log: AppendLog<Vec<u8>> = store
        .create_data("changes")
        .expect("create durable sample log");
    let mut transactions = store.into_transactions();
    let started = Instant::now();
    let transaction = transactions.begin().expect("begin durable transaction");
    log.access(transaction.access())
        .expect("access durable benchmark log")
        .append_batch(&workload.encoded)
        .expect("append pre-encoded durable Changes");
    transaction
        .commit()
        .expect("durably commit pre-encoded benchmark batch");
    let elapsed = started.elapsed();
    drop(transactions);
    validate_reopened_raw(sample_store.path(), &workload.encoded);
    elapsed
}

fn benchmark_encode_append_durable(
    config: &Config,
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
) {
    let scenario = "encode_append_durable_commit";
    let work = append_work(workload);
    for _ in 0..config.warmups {
        black_box(run_encode_append_durable(workload, stores, scenario));
    }
    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = run_encode_append_durable(workload, stores, scenario);
        emit_sample(scenario, sample, elapsed, work);
        durations.push(elapsed);
    }
    report(scenario, &mut durations, work);
}

fn run_encode_append_durable(
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
    scenario: &str,
) -> std::time::Duration {
    let sample_store = stores.sample(scenario);
    let mut store = Store::create(sample_store.path()).expect("create encode sample Store");
    let log: AppendLog<Vec<u8>> = store
        .create_data("changes")
        .expect("create encode sample log");
    let mut transactions = store.into_transactions();
    let started = Instant::now();
    let transaction = transactions.begin().expect("begin encode transaction");
    let encoded = workload
        .changes
        .iter()
        .map(|change| encode_change(change).expect("encode benchmark Change"))
        .collect::<Vec<_>>();
    log.access(transaction.access())
        .expect("access encode benchmark log")
        .append_batch(&encoded)
        .expect("append freshly encoded benchmark Changes");
    transaction
        .commit()
        .expect("durably commit encoded benchmark batch");
    let elapsed = started.elapsed();
    drop(encoded);
    drop(transactions);
    validate_reopened_raw(sample_store.path(), &workload.encoded);
    elapsed
}

fn benchmark_append_entry_forward(
    config: &Config,
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
) {
    let scenario = "raw_scan_append_entry_durable_commit";
    let work = SampleWork {
        transactions: 1,
        rows: workload.total_rows(),
        changes: workload.encoded.len(),
        encoded_bytes: workload
            .encoded_bytes
            .checked_mul(2)
            .expect("forward encoded byte count fits usize"),
        logical_bytes: workload
            .scan_bytes()
            .checked_mul(2)
            .expect("forward read-plus-write byte count fits usize"),
    };
    for _ in 0..config.warmups {
        black_box(run_append_entry_forward(workload, stores, scenario));
    }
    let mut durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = run_append_entry_forward(workload, stores, scenario);
        emit_sample(scenario, sample, elapsed, work);
        durations.push(elapsed);
    }
    report(scenario, &mut durations, work);
}

fn run_append_entry_forward(
    workload: &EncodedWorkload,
    stores: &BenchStoreRoot,
    scenario: &str,
) -> std::time::Duration {
    let sample_store = stores.sample(scenario);
    let mut store = Store::create(sample_store.path()).expect("create forward sample Store");
    let input: AppendLog<Vec<u8>> = store
        .create_data("input")
        .expect("create forward input log");
    let output: AppendLog<Vec<u8>> = store
        .create_data("output")
        .expect("create forward output log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions
            .begin()
            .expect("begin forward seed transaction");
        input
            .access(transaction.access())
            .expect("access forward seed log")
            .append_batch(&workload.encoded)
            .expect("seed forward input Changes");
        transaction.commit().expect("commit forward input seed");
    }
    validate_raw_log(&input, &mut transactions, workload);

    let started = Instant::now();
    let transaction = transactions.begin().expect("begin forward transaction");
    let input_access = input
        .access(transaction.access())
        .expect("access forward input");
    let mut output_access = output
        .access(transaction.access())
        .expect("access forward output");
    let progress = input_access
        .scan(0, exact_scan_limit(workload), |entry| {
            black_box(output_access.append_entry(&entry)?);
            Ok::<(), StoreError>(())
        })
        .expect("forward complete Change entries");
    transaction.commit().expect("durably commit forwarding");
    let elapsed = started.elapsed();
    assert!(progress.caught_up);
    drop(transactions);

    let store = Store::open(sample_store.path()).expect("reopen forwarded sample Store");
    let output: AppendLog<Vec<u8>> = store
        .open_data("output")
        .expect("open forwarded output log");
    let mut transactions = store.into_transactions();
    validate_raw_log(&output, &mut transactions, workload);
    elapsed
}

fn benchmark_warm_scans(config: &Config, workload: &EncodedWorkload, stores: &BenchStoreRoot) {
    let sample_store = stores.sample("warm_scans");
    let mut store = Store::create(sample_store.path()).expect("create scan benchmark Store");
    let log: AppendLog<Vec<u8>> = store
        .create_data("changes")
        .expect("create scan benchmark log");
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().expect("begin scan seed transaction");
        log.access(transaction.access())
            .expect("access scan seed log")
            .append_batch(&workload.encoded)
            .expect("append scan seed Changes");
        transaction.commit().expect("commit scan seed Changes");
    }

    let diff_only = ChangeProjection::try_new(workload.changes[0].schema(), [])
        .expect("create diff-only benchmark projection");
    let narrow = ChangeProjection::try_new(workload.changes[0].schema(), [0])
        .expect("create narrow benchmark projection");
    let full_expected = workload.changes.clone();
    let diff_expected = project_expected(&workload.changes, &diff_only);
    let narrow_expected = project_expected(&workload.changes, &narrow);
    let cases = [
        ScanCase {
            scenario: "warm_scan_full_decode",
            mode: DecodeMode::Full,
            expected: &full_expected,
        },
        ScanCase {
            scenario: "warm_scan_diff_only_decode",
            mode: DecodeMode::Projected(&diff_only),
            expected: &diff_expected,
        },
        ScanCase {
            scenario: "warm_scan_narrow_decode",
            mode: DecodeMode::Projected(&narrow),
            expected: &narrow_expected,
        },
    ];

    // Complete correctness oracles run outside every measured interval. The
    // expected projected Changes are derived from the source Changes rather
    // than from the first decoder sample.
    validate_raw_log(&log, &mut transactions, workload);
    for case in cases {
        validate_decoded_log(&log, &mut transactions, workload, case);
    }

    for _ in 0..config.warmups {
        black_box(run_timed_raw_scan(&log, &mut transactions, workload));
        for case in cases {
            black_box(run_timed_decode_scan(
                &log,
                &mut transactions,
                workload,
                case.mode,
            ));
        }
    }

    let work = scan_work(workload);
    let raw_scenario = "warm_scan_raw";
    let mut raw_durations = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let elapsed = run_timed_raw_scan(&log, &mut transactions, workload);
        emit_sample(raw_scenario, sample, elapsed, work);
        raw_durations.push(elapsed);
    }
    report(raw_scenario, &mut raw_durations, work);

    let mut durations: [Vec<std::time::Duration>; 3] =
        std::array::from_fn(|_| Vec::with_capacity(config.samples));
    for sample in 0..config.samples {
        // Rotate the first case each round so cache/thermal drift is paired
        // across full, diff-only, and narrow decoding rather than by block.
        for step in 0..cases.len() {
            let case_index = (sample + step) % cases.len();
            let case = cases[case_index];
            let elapsed = run_timed_decode_scan(&log, &mut transactions, workload, case.mode);
            emit_sample(case.scenario, sample, elapsed, work);
            durations[case_index].push(elapsed);
        }
    }
    for (case, samples) in cases.into_iter().zip(&mut durations) {
        report(case.scenario, samples, work);
    }

    // Re-run strict untimed comparisons after all samples so timing callbacks
    // cannot silently mutate state or establish their own expected baseline.
    validate_raw_log(&log, &mut transactions, workload);
    for case in cases {
        validate_decoded_log(&log, &mut transactions, workload, case);
    }
}

fn run_timed_raw_scan(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    workload: &EncodedWorkload,
) -> std::time::Duration {
    let transaction = transactions.begin().expect("begin raw scan transaction");
    let access = log
        .access(transaction.access())
        .expect("access raw scan log");
    let started = Instant::now();
    let progress = access
        .scan(0, exact_scan_limit(workload), |entry| {
            let encoded_len = entry.project(|encoded| Ok(encoded.len()))?;
            black_box((entry.offset(), encoded_len));
            Ok::<(), StoreError>(())
        })
        .expect("scan raw benchmark entries");
    let elapsed = started.elapsed();
    assert!(progress.caught_up);
    elapsed
}

fn run_timed_decode_scan(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    workload: &EncodedWorkload,
    mode: DecodeMode<'_>,
) -> std::time::Duration {
    let transaction = transactions
        .begin()
        .expect("begin decoded scan transaction");
    let access = log
        .access(transaction.access())
        .expect("access decoded scan log");
    let started = Instant::now();
    let progress = access
        .scan(0, exact_scan_limit(workload), |entry| {
            let change = match mode {
                DecodeMode::Full => entry.project(decode_entry)?,
                DecodeMode::Projected(projection) => {
                    entry.project(|encoded| decode_projected_entry(encoded, projection))?
                }
            };
            // All decode modes use the same constant-time sink. Complete value
            // comparisons deliberately live in validate_decoded_log.
            black_box((entry.offset(), change));
            Ok::<(), StoreError>(())
        })
        .expect("scan benchmark Changes");
    let elapsed = started.elapsed();
    assert!(progress.caught_up);
    elapsed
}

fn project_expected(changes: &[Change], projection: &ChangeProjection) -> Vec<Change> {
    changes
        .iter()
        .map(|change| {
            change
                .try_project(projection)
                .expect("project source benchmark Change")
        })
        .collect()
}

fn validate_decoded_log(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    workload: &EncodedWorkload,
    case: ScanCase<'_>,
) {
    assert_eq!(case.expected.len(), workload.encoded.len());
    let transaction = transactions
        .begin()
        .expect("begin decoded oracle transaction");
    let access = log
        .access(transaction.access())
        .expect("access decoded oracle log");
    let mut index = 0_usize;
    let progress = access
        .scan(0, exact_scan_limit(workload), |entry| {
            let expected_offset = u64::try_from(index).expect("oracle offset fits u64");
            assert_eq!(entry.offset(), expected_offset);
            let actual = match case.mode {
                DecodeMode::Full => entry.project(decode_entry)?,
                DecodeMode::Projected(projection) => {
                    entry.project(|encoded| decode_projected_entry(encoded, projection))?
                }
            };
            assert_change_eq(&actual, &case.expected[index]);
            index += 1;
            Ok::<(), StoreError>(())
        })
        .expect("run strict decoded scan oracle");
    assert!(progress.caught_up);
    assert_eq!(index, case.expected.len());
}

fn validate_raw_log(
    log: &AppendLog<Vec<u8>>,
    transactions: &mut Transactions,
    workload: &EncodedWorkload,
) {
    let transaction = transactions.begin().expect("begin raw oracle transaction");
    let access = log
        .access(transaction.access())
        .expect("access raw oracle log");
    let mut index = 0_usize;
    let progress = access
        .scan(0, exact_scan_limit(workload), |entry| {
            let expected_offset = u64::try_from(index).expect("oracle offset fits u64");
            assert_eq!(entry.offset(), expected_offset);
            entry.project(|encoded| {
                assert_eq!(encoded, workload.encoded[index]);
                Ok(())
            })?;
            index += 1;
            Ok::<(), StoreError>(())
        })
        .expect("run strict raw scan oracle");
    assert!(progress.caught_up);
    assert_eq!(index, workload.encoded.len());
}

fn validate_reopened_raw(path: &std::path::Path, expected: &[Vec<u8>]) {
    let store = Store::open(path).expect("reopen durable benchmark Store");
    let log: AppendLog<Vec<u8>> = store
        .open_data("changes")
        .expect("open durable benchmark log");
    let mut transactions = store.into_transactions();
    let encoded_bytes = expected
        .iter()
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes.len()))
        .expect("durable oracle encoded bytes fit usize");
    let scan_bytes = encoded_bytes
        .checked_add(
            expected
                .len()
                .checked_mul(size_of::<u64>())
                .expect("durable oracle offset bytes fit usize"),
        )
        .expect("durable oracle scan bytes fit usize");
    let transaction = transactions
        .begin()
        .expect("begin durable oracle transaction");
    let access = log
        .access(transaction.access())
        .expect("access durable oracle log");
    let mut index = 0_usize;
    let progress = access
        .scan(
            0,
            ScanLimit::new(expected.len(), scan_bytes).expect("valid durable oracle limit"),
            |entry| {
                assert_eq!(
                    entry.offset(),
                    u64::try_from(index).expect("oracle offset fits u64")
                );
                entry.project(|encoded| {
                    assert_eq!(encoded, expected[index]);
                    Ok(())
                })?;
                index += 1;
                Ok::<(), StoreError>(())
            },
        )
        .expect("scan reopened durable entries");
    assert!(progress.caught_up);
    assert_eq!(index, expected.len());
}

fn exact_scan_limit(workload: &EncodedWorkload) -> ScanLimit {
    ScanLimit::new(workload.encoded.len(), workload.scan_bytes())
        .expect("valid exact benchmark scan limit")
}

fn append_work(workload: &EncodedWorkload) -> SampleWork {
    SampleWork {
        transactions: 1,
        rows: workload.total_rows(),
        changes: workload.encoded.len(),
        encoded_bytes: workload.encoded_bytes,
        logical_bytes: workload.scan_bytes(),
    }
}

fn scan_work(workload: &EncodedWorkload) -> SampleWork {
    append_work(workload)
}
