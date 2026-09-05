//! Long-running fixed-window append, prefix-GC, space-reuse, and reopen validation.

use std::{
    borrow::Cow,
    fs,
    hint::black_box,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use dogpaddle_perf_context::PerformanceProfile;
use dogpaddle_store::{
    AppendLog, CodecError, ScanLimit, Store, StoreError, StoreValue, Transactions,
};
use serde_json::json;

mod support;

use support::StoreRun;

const BENCHMARK: &str = "append_log_endurance";
const DEFAULT_RECORD_BYTES: &[usize] = &[128, 1_024, 8_192];
const DEFAULT_FULL_LOGICAL_MIB: usize = 1_024;
const DEFAULT_FULL_WINDOW_MIB: usize = 64;
const DEFAULT_FULL_BATCH_MIB: usize = 1;
const DEFAULT_FULL_CHECKPOINT_EPOCHS: usize = 64;
const DEFAULT_MAX_WORKING_SET_BYTES: usize = 1_073_741_824;
const DEFAULT_MAX_TOTAL_WRITTEN_BYTES: usize = 4_294_967_295;
const RECORD_HEADER_BYTES: usize = 16;
const MEBIBYTE_BYTES: usize = 1_048_576;
const MDBX_DATA_FILE: &str = "mdbx.dat";

#[derive(Clone)]
struct EnduranceRecord {
    encoded: Vec<u8>,
}

#[derive(Clone, Copy)]
struct FileSize {
    logical: u64,
    allocated: u64,
}

struct ProtocolRun {
    head: u64,
    tail: u64,
    wall_elapsed_ns: u128,
    append_ns: Vec<u64>,
    truncate_ns: Vec<u64>,
    checkpoints: Vec<Checkpoint>,
}

#[derive(Clone, Copy)]
struct Checkpoint {
    head: u64,
    tail: u64,
    size: FileSize,
}

struct EnduranceResult {
    record_bytes: usize,
    batch_items: usize,
    validation_checksum: u64,
    protocol: ProtocolRun,
}

#[derive(Clone, Copy)]
struct EndurancePlan {
    record_bytes: usize,
    batch_items: usize,
    window_batches: usize,
    window_items: usize,
    steady_epochs: usize,
    checkpoint_epochs: usize,
}

#[derive(Clone, Copy)]
struct ProtocolConfig<'a> {
    store_path: &'a Path,
    window_items: usize,
    steady_epochs: usize,
    checkpoint_epochs: usize,
    record_bytes: usize,
    batch_bytes: usize,
}

struct WorkloadConfig {
    record_sizes: Vec<usize>,
    logical_mib: usize,
    window_mib: usize,
    batch_mib: usize,
    checkpoint_epochs: usize,
    max_working_set_bytes: usize,
    max_total_written_bytes: usize,
}

impl WorkloadConfig {
    fn for_profile(profile: PerformanceProfile) -> Self {
        match profile {
            PerformanceProfile::Smoke => Self {
                record_sizes: vec![128],
                logical_mib: 2,
                window_mib: 1,
                batch_mib: 1,
                checkpoint_epochs: 1,
                max_working_set_bytes: 64 * MEBIBYTE_BYTES,
                max_total_written_bytes: 64 * MEBIBYTE_BYTES,
            },
            PerformanceProfile::Reference => Self {
                record_sizes: DEFAULT_RECORD_BYTES.to_vec(),
                logical_mib: DEFAULT_FULL_LOGICAL_MIB,
                window_mib: DEFAULT_FULL_WINDOW_MIB,
                batch_mib: DEFAULT_FULL_BATCH_MIB,
                checkpoint_epochs: DEFAULT_FULL_CHECKPOINT_EPOCHS,
                max_working_set_bytes: DEFAULT_MAX_WORKING_SET_BYTES,
                max_total_written_bytes: DEFAULT_MAX_TOTAL_WRITTEN_BYTES,
            },
        }
    }

    fn fields(&self, budget: BudgetEstimate) -> serde_json::Value {
        json!({
            "record_bytes": self.record_sizes,
            "logical_mib_per_width": self.logical_mib,
            "window_mib": self.window_mib,
            "batch_mib": self.batch_mib,
            "checkpoint_epochs": self.checkpoint_epochs,
            "max_working_set_bytes": self.max_working_set_bytes,
            "estimated_working_set_bytes": budget.max_working_set_bytes,
            "max_total_written_bytes": self.max_total_written_bytes,
            "estimated_total_written_bytes": budget.total_written_bytes,
            "execution": "single_thread",
            "mdbx_sync_mode": "durable",
        })
    }
}

#[derive(Clone, Copy)]
struct BudgetEstimate {
    max_working_set_bytes: usize,
    total_written_bytes: usize,
}

impl EnduranceRecord {
    fn new(index: usize, encoded_bytes: usize) -> Self {
        let key = u64::try_from(index).expect("batch record index fits in u64");
        let diff = if index.is_multiple_of(2) {
            1_i64
        } else {
            -1_i64
        };
        let fill = u8::try_from(key & 0xff).expect("masked payload byte fits in u8");
        let mut encoded = vec![fill; encoded_bytes];
        encoded[..8].copy_from_slice(&diff.to_be_bytes());
        encoded[8..RECORD_HEADER_BYTES].copy_from_slice(&key.to_be_bytes());
        Self { encoded }
    }
}

impl StoreValue for EnduranceRecord {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.encoded.as_slice())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        Ok(Self {
            encoded: bytes.into_owned(),
        })
    }
}

fn main() {
    if !std::env::args_os().any(|argument| argument == "--bench") {
        return;
    }
    let profile = PerformanceProfile::from_environment();
    let config = WorkloadConfig::for_profile(profile);
    assert!(config.logical_mib > config.window_mib);
    assert!(
        config
            .record_sizes
            .iter()
            .all(|record_bytes| *record_bytes >= RECORD_HEADER_BYTES)
    );
    let budget = estimate_budget(&config);
    assert!(
        budget.max_working_set_bytes <= config.max_working_set_bytes,
        "estimated endurance working set {} exceeds configured {} byte budget",
        budget.max_working_set_bytes,
        config.max_working_set_bytes
    );
    assert!(
        budget.total_written_bytes <= config.max_total_written_bytes,
        "estimated endurance writes {} exceed configured {} byte budget",
        budget.total_written_bytes,
        config.max_total_written_bytes
    );
    let run = StoreRun::new(BENCHMARK, profile, &config.fields(budget));
    let workloads = config
        .record_sizes
        .iter()
        .map(|&record_bytes| plan_endurance(&config, record_bytes))
        .collect::<Vec<_>>();
    let results = workloads
        .into_iter()
        .map(|workload| run_endurance(&run, workload))
        .collect::<Vec<_>>();
    run.finish();
    print_summary(&results);
}

fn print_summary(results: &[EnduranceResult]) {
    eprintln!();
    eprintln!("=== Endurance derived summary ===");
    for result in results {
        let record_bytes = result.record_bytes;
        let batch_items = u64::try_from(result.batch_items).expect("batch item count fits u64");
        let append_ns = sorted_elapsed(&result.protocol.append_ns);
        let truncate_ns = sorted_elapsed(&result.protocol.truncate_ns);
        let protocol_ns = append_ns
            .iter()
            .chain(&truncate_ns)
            .map(|&value| u128::from(value))
            .sum::<u128>();
        let steady_records = u128::from(batch_items)
            * u128::try_from(append_ns.len()).expect("sample count fits u128");
        let throughput = steady_records * 1_000_000_000 / protocol_ns.max(1);

        let allocated = result
            .protocol
            .checkpoints
            .iter()
            .map(|checkpoint| checkpoint.size.allocated)
            .collect::<Vec<_>>();
        let final_checkpoint = result
            .protocol
            .checkpoints
            .last()
            .expect("endurance has checkpoints");
        let head = final_checkpoint.head;
        let tail = final_checkpoint.tail;
        let retained_payload =
            u128::from(tail - head) * u128::try_from(record_bytes).expect("record width fits u128");
        let final_allocated = u128::from(*allocated.last().expect("endurance has checkpoints"));
        let amplification_hundredths = final_allocated * 100 / retained_payload.max(1);
        let spread_basis_points = tail_spread_basis_points(&allocated);

        let wall_ns = result.protocol.wall_elapsed_ns;
        let checksum = format!("{:#018x}", result.validation_checksum);

        eprintln!(
            "record={record_bytes} B batch={batch_items} items epochs={} steady_records={steady_records}",
            append_ns.len()
        );
        print_latency("append tx", &append_ns);
        print_latency("truncate tx", &truncate_ns);
        eprintln!(
            "  protocol={} wall={} throughput={throughput} records/s",
            duration_ns(protocol_ns),
            duration_ns(wall_ns),
        );
        eprintln!(
            "  file seed={} final={} peak={} allocated_amplification={}.{:02}x tail_spread={}.{:02}%",
            bytes(allocated[0]),
            bytes(u64::try_from(final_allocated).expect("allocated bytes fit u64")),
            bytes(*allocated.iter().max().expect("endurance has checkpoints")),
            amplification_hundredths / 100,
            amplification_hundredths % 100,
            spread_basis_points / 100,
            spread_basis_points % 100,
        );
        eprintln!("  validation=reopen+full-retained-scan checksum={checksum}");
    }
}

fn sorted_elapsed(samples: &[u64]) -> Vec<u64> {
    let mut elapsed = samples.to_vec();
    elapsed.sort_unstable();
    elapsed
}

fn print_latency(label: &str, sorted: &[u64]) {
    eprintln!(
        "  {label:<11} p50={} p95={} p99={} max={}",
        duration_ns(u128::from(percentile(sorted, 50))),
        duration_ns(u128::from(percentile(sorted, 95))),
        duration_ns(u128::from(percentile(sorted, 99))),
        duration_ns(u128::from(
            *sorted.last().expect("latency samples are non-empty")
        )),
    );
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn tail_spread_basis_points(samples: &[u64]) -> u64 {
    let tail = &samples[samples.len() / 2..];
    let minimum = *tail.iter().min().expect("endurance has tail samples");
    let maximum = *tail.iter().max().expect("endurance has tail samples");
    if minimum == 0 {
        0
    } else {
        u64::try_from(u128::from(maximum - minimum) * 10_000 / u128::from(minimum))
            .expect("tail spread fits u64")
    }
}

fn duration_ns(nanos: u128) -> String {
    let duration = Duration::from_nanos(u64::try_from(nanos).expect("duration fits u64 nanos"));
    if nanos >= 1_000_000_000 {
        format!("{:.3}s", duration.as_secs_f64())
    } else if nanos >= 1_000_000 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("endurance duration fits u64 nanoseconds")
}

fn bytes(value: u64) -> String {
    let unit = if value >= 1_073_741_824 {
        (1_073_741_824_u64, "GiB")
    } else {
        (1_048_576_u64, "MiB")
    };
    let hundredths = u128::from(value) * 100 / u128::from(unit.0);
    format!("{}.{:02} {}", hundredths / 100, hundredths % 100, unit.1)
}

fn estimate_budget(config: &WorkloadConfig) -> BudgetEstimate {
    let mut max_working_set_bytes = 0_usize;
    let mut total_written_bytes = 0_usize;
    for &record_bytes in &config.record_sizes {
        let batch_items = (mib_bytes(config.batch_mib) / record_bytes).max(1);
        let batch_bytes = batch_items
            .checked_mul(record_bytes)
            .expect("endurance batch bytes fit usize");
        let window_batches = mib_bytes(config.window_mib).div_ceil(batch_bytes).max(1);
        let total_batches = mib_bytes(config.logical_mib)
            .div_ceil(batch_bytes)
            .max(window_batches + 1);
        let steady_epochs = total_batches - window_batches;
        let latency_bytes = steady_epochs
            .checked_mul(2)
            .and_then(|value| value.checked_mul(size_of::<Duration>()))
            .expect("endurance latency sample bytes fit usize");
        let checkpoint_count = steady_epochs.div_ceil(config.checkpoint_epochs) + 1;
        let checkpoint_bytes = checkpoint_count
            .checked_mul(size_of::<FileSize>())
            .expect("endurance checkpoint bytes fit usize");
        let working_set = batch_bytes
            .checked_mul(2)
            .and_then(|value| value.checked_add(latency_bytes))
            .and_then(|value| value.checked_add(checkpoint_bytes))
            .expect("endurance working-set estimate fits usize");
        max_working_set_bytes = max_working_set_bytes.max(working_set);
        total_written_bytes = total_written_bytes
            .checked_add(
                total_batches
                    .checked_mul(batch_bytes)
                    .expect("per-width endurance writes fit usize"),
            )
            .expect("total endurance writes fit usize");
    }
    BudgetEstimate {
        max_working_set_bytes,
        total_written_bytes,
    }
}

fn plan_endurance(config: &WorkloadConfig, record_bytes: usize) -> EndurancePlan {
    let batch_target_bytes = mib_bytes(config.batch_mib);
    let batch_items = (batch_target_bytes / record_bytes).max(1);
    let batch_bytes = batch_items
        .checked_mul(record_bytes)
        .expect("batch byte size fits in usize");
    let window_batches = mib_bytes(config.window_mib).div_ceil(batch_bytes).max(1);
    let window_items = window_batches
        .checked_mul(batch_items)
        .expect("window item count fits in usize");
    let total_batches = mib_bytes(config.logical_mib)
        .div_ceil(batch_bytes)
        .max(window_batches + 1);
    let steady_epochs = total_batches - window_batches;
    EndurancePlan {
        record_bytes,
        batch_items,
        window_batches,
        window_items,
        steady_epochs,
        checkpoint_epochs: config.checkpoint_epochs,
    }
}

fn run_endurance(run: &StoreRun, plan: EndurancePlan) -> EnduranceResult {
    let max_gc_items = NonZeroUsize::new(plan.batch_items).expect("batch item count is non-zero");
    let records = (0..plan.batch_items)
        .map(|index| EnduranceRecord::new(index, plan.record_bytes))
        .collect::<Vec<_>>();

    let root = run
        .root()
        .sample(&format!("append-log-endurance-{}", plan.record_bytes));
    let store_path = root.path().join("store");
    let mut store = Store::create(&store_path).expect("create endurance benchmark store");
    let log = store
        .create_data::<AppendLog<EnduranceRecord>>("log")
        .expect("create endurance benchmark log");
    let mut transactions = store.into_transactions();
    seed_window(&mut transactions, &log, &records, plan.window_batches);
    let protocol = run_protocol(
        run,
        &mut transactions,
        &log,
        &records,
        max_gc_items,
        ProtocolConfig {
            store_path: &store_path,
            window_items: plan.window_items,
            steady_epochs: plan.steady_epochs,
            checkpoint_epochs: plan.checkpoint_epochs,
            record_bytes: plan.record_bytes,
            batch_bytes: plan
                .batch_items
                .checked_mul(plan.record_bytes)
                .expect("batch bytes fit usize"),
        },
    );

    drop(transactions);
    let validation_checksum = validate_reopened(
        &store_path,
        protocol.head,
        protocol.tail,
        plan.record_bytes,
        plan.batch_items,
        plan.window_items,
    );
    black_box(validation_checksum);
    run.observation(
        &terminal_series(plan.record_bytes),
        0,
        &json!({
            "wall_elapsed_ns": protocol.wall_elapsed_ns,
            "validation_checksum": format!("{validation_checksum:#018x}"),
        }),
    );
    EnduranceResult {
        record_bytes: plan.record_bytes,
        batch_items: plan.batch_items,
        validation_checksum,
        protocol,
    }
}

fn seed_window(
    transactions: &mut Transactions,
    log: &AppendLog<EnduranceRecord>,
    records: &[EnduranceRecord],
    batches: usize,
) {
    for _ in 0..batches {
        let transaction = transactions
            .begin()
            .expect("begin endurance seed transaction");
        log.access(transaction.access())
            .expect("access endurance seed log")
            .append_batch(records)
            .expect("append endurance seed batch");
        transaction
            .commit()
            .expect("commit endurance seed transaction");
    }
}

fn run_protocol(
    run: &StoreRun,
    transactions: &mut Transactions,
    log: &AppendLog<EnduranceRecord>,
    records: &[EnduranceRecord],
    max_gc_items: NonZeroUsize,
    config: ProtocolConfig<'_>,
) -> ProtocolRun {
    let batch_items = records.len();
    let batch_items_u64 = to_u64(batch_items);
    let mut head = 0_u64;
    let mut tail = to_u64(config.window_items);
    let mut append_ns = Vec::with_capacity(config.steady_epochs);
    let mut truncate_ns = Vec::with_capacity(config.steady_epochs);
    let mut checkpoints = vec![record_checkpoint(
        run,
        config.record_bytes,
        0,
        head,
        tail,
        data_file_size(config.store_path),
    )];

    let wall_started = Instant::now();
    for epoch in 1..=config.steady_epochs {
        let append_started = Instant::now();
        let transaction = transactions
            .begin()
            .expect("begin endurance append transaction");
        let assigned = log
            .access(transaction.access())
            .expect("access endurance append log")
            .append_batch(records)
            .expect("append endurance batch");
        transaction
            .commit()
            .expect("commit endurance append transaction");
        let append_duration = append_started.elapsed();
        assert_eq!(assigned, tail..tail + batch_items_u64);
        append_ns.push(duration_to_nanos(append_duration));
        run.sample(
            &format!("record_bytes={}/append", config.record_bytes),
            epoch - 1,
            append_duration,
            &json!({
                "operations": batch_items,
                "transactions": 1,
                "logical_bytes": config.batch_bytes,
            }),
        );
        tail += batch_items_u64;

        let target = tail - to_u64(config.window_items);
        let gc_started = Instant::now();
        let transaction = transactions
            .begin()
            .expect("begin endurance GC transaction");
        let next_head = log
            .access(transaction.access())
            .expect("access endurance GC log")
            .truncate_before(target, max_gc_items)
            .expect("truncate endurance log");
        transaction
            .commit()
            .expect("commit endurance GC transaction");
        let gc_duration = gc_started.elapsed();
        assert_eq!(next_head, target);
        truncate_ns.push(duration_to_nanos(gc_duration));
        run.sample(
            &format!("record_bytes={}/truncate", config.record_bytes),
            epoch - 1,
            gc_duration,
            &json!({
                "operations": batch_items,
                "transactions": 1,
                "logical_bytes": config.batch_bytes,
            }),
        );
        head = next_head;

        if epoch.is_multiple_of(config.checkpoint_epochs) || epoch == config.steady_epochs {
            let size = data_file_size(config.store_path);
            checkpoints.push(record_checkpoint(
                run,
                config.record_bytes,
                epoch,
                head,
                tail,
                size,
            ));
        }
    }

    ProtocolRun {
        head,
        tail,
        wall_elapsed_ns: wall_started.elapsed().as_nanos(),
        append_ns,
        truncate_ns,
        checkpoints,
    }
}

fn validate_reopened(
    store_path: &Path,
    expected_head: u64,
    expected_tail: u64,
    record_bytes: usize,
    batch_items: usize,
    window_items: usize,
) -> u64 {
    let store = Store::open(store_path).expect("reopen endurance benchmark store");
    let log = store
        .open_data::<AppendLog<EnduranceRecord>>("log")
        .expect("reopen endurance benchmark log");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin endurance validation transaction");
    let log = log
        .access(transaction.access())
        .expect("access reopened endurance log");
    assert_eq!(
        log.bounds().expect("read reopened endurance bounds"),
        expected_head..expected_tail
    );

    let item_bytes = record_bytes
        .checked_add(size_of::<u64>())
        .expect("validation item byte size fits in usize");
    let scan_bytes = item_bytes
        .checked_mul(batch_items)
        .expect("validation batch byte size fits in usize");
    let scan_limit = ScanLimit::new(batch_items, scan_bytes).expect("valid validation scan limit");
    let batch_items_u64 = to_u64(batch_items);
    let mut cursor = expected_head;
    let mut observed = 0_usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;

    loop {
        let scan = log
            .scan(cursor, scan_limit, |entry| {
                let offset = entry.offset();
                let fingerprint = entry.project(|encoded| {
                    verify_record(encoded, record_bytes, offset % batch_items_u64)
                })?;
                checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3) ^ fingerprint;
                checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3) ^ offset;
                observed += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan reopened endurance log");
        cursor = scan.next_offset;
        if scan.caught_up {
            break;
        }
    }

    assert_eq!(cursor, expected_tail);
    assert_eq!(observed, window_items);
    transaction
        .commit()
        .expect("finish endurance validation transaction");
    checksum
}

fn verify_record(
    encoded: &[u8],
    expected_bytes: usize,
    expected_key: u64,
) -> Result<u64, CodecError> {
    if encoded.len() != expected_bytes {
        return Err(CodecError::new("unexpected endurance record length"));
    }
    let diff = i64::from_be_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| CodecError::new("invalid endurance diff"))?,
    );
    let key = u64::from_be_bytes(
        encoded[8..RECORD_HEADER_BYTES]
            .try_into()
            .map_err(|_| CodecError::new("invalid endurance key"))?,
    );
    let expected_diff = if expected_key.is_multiple_of(2) {
        1_i64
    } else {
        -1_i64
    };
    let fill = u8::try_from(expected_key & 0xff).expect("masked payload byte fits in u8");
    if diff != expected_diff || key != expected_key {
        return Err(CodecError::new("unexpected endurance record header"));
    }
    if !encoded[RECORD_HEADER_BYTES..]
        .iter()
        .all(|byte| *byte == fill)
    {
        return Err(CodecError::new("unexpected endurance record payload"));
    }
    Ok(key ^ u64::from(fill) ^ diff.cast_unsigned())
}

fn record_checkpoint(
    run: &StoreRun,
    record_bytes: usize,
    epoch: usize,
    head: u64,
    tail: u64,
    size: FileSize,
) -> Checkpoint {
    run.observation(
        &checkpoint_series(record_bytes),
        epoch,
        &json!({
            "epoch": epoch,
            "head": head,
            "tail": tail,
            "file_logical_bytes": size.logical,
            "file_allocated_bytes": size.allocated,
        }),
    );
    Checkpoint { head, tail, size }
}

fn terminal_series(record_bytes: usize) -> String {
    format!("record_bytes={record_bytes}/terminal")
}

fn checkpoint_series(record_bytes: usize) -> String {
    format!("record_bytes={record_bytes}/checkpoint")
}

fn data_file_size(store_path: &Path) -> FileSize {
    let metadata = fs::metadata(store_path.join(MDBX_DATA_FILE))
        .expect("read endurance MDBX data-file metadata");
    FileSize {
        logical: metadata.len(),
        allocated: allocated_bytes(&metadata),
    }
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

fn mib_bytes(mib: usize) -> usize {
    mib.checked_mul(MEBIBYTE_BYTES)
        .expect("configured MiB value fits in usize")
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark value fits in u64")
}
