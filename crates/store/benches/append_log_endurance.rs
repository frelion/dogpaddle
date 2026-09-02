//! Long-running fixed-window append, prefix-GC, space-reuse, and reopen validation.

use std::{
    borrow::Cow,
    fs,
    hint::black_box,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use dogpaddle_bench_protocol::{
    Artifact, BenchmarkProfile, CaseId, CaseSpec, Fields, Measurement, ObservationId,
    ObservationSpec, Plan, Run,
};
use dogpaddle_store::{
    AppendLog, CodecError, ScanLimit, Store, StoreError, StoreValue, Transactions,
};

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
}

#[derive(Clone, Copy)]
struct EndurancePlan {
    record_bytes: usize,
    batch_items: usize,
    window_batches: usize,
    window_items: usize,
    steady_epochs: usize,
    checkpoint_epochs: usize,
    checkpoint: ObservationId,
    terminal: ObservationId,
    append: CaseId,
    truncate: CaseId,
}

#[derive(Clone, Copy)]
struct ProtocolConfig<'a> {
    store_path: &'a Path,
    window_items: usize,
    steady_epochs: usize,
    checkpoint_epochs: usize,
    checkpoint: ObservationId,
    append: CaseId,
    truncate: CaseId,
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
    fn for_profile(profile: BenchmarkProfile) -> Self {
        match profile {
            BenchmarkProfile::Smoke => Self {
                record_sizes: vec![128],
                logical_mib: 2,
                window_mib: 1,
                batch_mib: 1,
                checkpoint_epochs: 1,
                max_working_set_bytes: 64 * MEBIBYTE_BYTES,
                max_total_written_bytes: 64 * MEBIBYTE_BYTES,
            },
            BenchmarkProfile::Reference => Self {
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

    fn fields(&self, budget: BudgetEstimate) -> Fields {
        Fields::new()
            .with("record_bytes", &self.record_sizes)
            .with("logical_mib_per_width", self.logical_mib)
            .with("window_mib", self.window_mib)
            .with("batch_mib", self.batch_mib)
            .with("checkpoint_epochs", self.checkpoint_epochs)
            .with("max_working_set_bytes", self.max_working_set_bytes)
            .with("estimated_working_set_bytes", budget.max_working_set_bytes)
            .with("max_total_written_bytes", self.max_total_written_bytes)
            .with("estimated_total_written_bytes", budget.total_written_bytes)
            .with("execution", "single_thread")
            .with("mdbx_sync_mode", "durable")
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
    let profile = BenchmarkProfile::from_environment();
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
    let mut plan = Plan::new(profile, config.fields(budget));
    let workloads = config
        .record_sizes
        .iter()
        .map(|&record_bytes| plan_endurance(&mut plan, &config, record_bytes))
        .collect::<Vec<_>>();
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    for workload in workloads {
        run_endurance(&mut run, workload);
    }
    let artifact = run.finish(|| {});
    print_summary(&artifact, &config);
}

fn print_summary(artifact: &Artifact, config: &WorkloadConfig) {
    println!();
    println!("=== Endurance derived summary ===");
    for &record_bytes in &config.record_sizes {
        let append_series = format!("record_bytes={record_bytes}/append");
        let truncate_series = format!("record_bytes={record_bytes}/truncate");
        let (append, append_samples) = artifact
            .cases()
            .find(|(case, _)| case.series() == append_series)
            .expect("endurance artifact contains append samples");
        let (_, truncate_samples) = artifact
            .cases()
            .find(|(case, _)| case.series() == truncate_series)
            .expect("endurance artifact contains truncate samples");
        let batch_items = append
            .fields()
            .get_u64("operations")
            .expect("endurance append case declares operations");
        let append_ns = sorted_elapsed(append_samples);
        let truncate_ns = sorted_elapsed(truncate_samples);
        let protocol_ns = append_ns
            .iter()
            .chain(&truncate_ns)
            .map(|&value| u128::from(value))
            .sum::<u128>();
        let steady_records = u128::from(batch_items)
            * u128::try_from(append_samples.len()).expect("sample count fits u128");
        let throughput = steady_records * 1_000_000_000 / protocol_ns.max(1);

        let checkpoint_series = checkpoint_series(record_bytes);
        let (_, checkpoints) = artifact
            .observations()
            .find(|(spec, _)| spec.series() == checkpoint_series)
            .expect("endurance artifact contains checkpoints");
        let allocated = checkpoints
            .iter()
            .map(|checkpoint| {
                checkpoint
                    .fields()
                    .get_u64("file_allocated_bytes")
                    .expect("endurance checkpoint contains allocated bytes")
            })
            .collect::<Vec<_>>();
        let final_checkpoint = checkpoints.last().expect("endurance has checkpoints");
        let head = observation_u64(final_checkpoint, "head");
        let tail = observation_u64(final_checkpoint, "tail");
        let retained_payload =
            u128::from(tail - head) * u128::try_from(record_bytes).expect("record width fits u128");
        let final_allocated = u128::from(*allocated.last().expect("endurance has checkpoints"));
        let amplification_hundredths = final_allocated * 100 / retained_payload.max(1);
        let spread_basis_points = tail_spread_basis_points(&allocated);

        let terminal_series = terminal_series(record_bytes);
        let (_, terminal) = artifact
            .observations()
            .find(|(spec, _)| spec.series() == terminal_series)
            .expect("endurance artifact contains terminal observation");
        let terminal = terminal
            .first()
            .expect("endurance has one terminal observation");
        let wall_ns = observation_u64(terminal, "wall_elapsed_ns");
        let checksum = terminal
            .fields()
            .get_str("validation_checksum")
            .expect("endurance terminal contains validation checksum");

        println!(
            "record={record_bytes} B batch={batch_items} items epochs={} steady_records={steady_records}",
            append_samples.len()
        );
        print_latency("append tx", &append_ns);
        print_latency("truncate tx", &truncate_ns);
        println!(
            "  protocol={} wall={} throughput={throughput} records/s",
            duration_ns(protocol_ns),
            duration_ns(u128::from(wall_ns)),
        );
        println!(
            "  file seed={} final={} peak={} allocated_amplification={}.{:02}x tail_spread={}.{:02}%",
            bytes(allocated[0]),
            bytes(u64::try_from(final_allocated).expect("allocated bytes fit u64")),
            bytes(*allocated.iter().max().expect("endurance has checkpoints")),
            amplification_hundredths / 100,
            amplification_hundredths % 100,
            spread_basis_points / 100,
            spread_basis_points % 100,
        );
        println!("  validation=reopen+full-retained-scan checksum={checksum}");
    }
}

fn sorted_elapsed(samples: &[dogpaddle_bench_protocol::Sample]) -> Vec<u64> {
    let mut elapsed = samples
        .iter()
        .map(dogpaddle_bench_protocol::Sample::elapsed_ns)
        .collect::<Vec<_>>();
    elapsed.sort_unstable();
    elapsed
}

fn print_latency(label: &str, sorted: &[u64]) {
    println!(
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

fn observation_u64(observation: &dogpaddle_bench_protocol::Observation, field: &str) -> u64 {
    observation
        .fields()
        .get_u64(field)
        .unwrap_or_else(|| panic!("endurance observation requires u64 field {field:?}"))
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

fn plan_endurance(plan: &mut Plan, config: &WorkloadConfig, record_bytes: usize) -> EndurancePlan {
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
    let samples = NonZeroUsize::new(steady_epochs).expect("endurance has steady epochs");
    let sample_fields = Fields::new()
        .with("operations", batch_items)
        .with("transactions", 1)
        .with("logical_bytes", batch_bytes);
    let append = plan.case(CaseSpec::new(
        format!("record_bytes={record_bytes}/append"),
        samples,
        sample_fields.clone(),
    ));
    let truncate = plan.case(CaseSpec::new(
        format!("record_bytes={record_bytes}/truncate"),
        samples,
        sample_fields,
    ));
    let checkpoint = plan.observation(ObservationSpec::new(
        checkpoint_series(record_bytes),
        NonZeroUsize::new(steady_epochs.div_ceil(config.checkpoint_epochs) + 1)
            .expect("endurance emits checkpoints"),
    ));
    let terminal = plan.observation(ObservationSpec::new(
        terminal_series(record_bytes),
        NonZeroUsize::MIN,
    ));
    EndurancePlan {
        record_bytes,
        batch_items,
        window_batches,
        window_items,
        steady_epochs,
        checkpoint_epochs: config.checkpoint_epochs,
        checkpoint,
        terminal,
        append,
        truncate,
    }
}

fn run_endurance(run: &mut Run, plan: EndurancePlan) {
    let max_gc_items = NonZeroUsize::new(plan.batch_items).expect("batch item count is non-zero");
    let records = (0..plan.batch_items)
        .map(|index| EnduranceRecord::new(index, plan.record_bytes))
        .collect::<Vec<_>>();

    let root = run.sample(&format!("append-log-endurance-{}", plan.record_bytes));
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
            checkpoint: plan.checkpoint,
            append: plan.append,
            truncate: plan.truncate,
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
    run.observe(
        plan.terminal,
        Fields::new()
            .with("wall_elapsed_ns", protocol.wall_elapsed_ns)
            .with(
                "validation_checksum",
                format!("{validation_checksum:#018x}"),
            ),
    );
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
    run: &mut Run,
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
    record_checkpoint(
        run,
        config.checkpoint,
        0,
        head,
        tail,
        data_file_size(config.store_path),
    );

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
        run.push(config.append, Measurement::new(append_duration));
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
        run.push(config.truncate, Measurement::new(gc_duration));
        head = next_head;

        if epoch.is_multiple_of(config.checkpoint_epochs) || epoch == config.steady_epochs {
            let size = data_file_size(config.store_path);
            record_checkpoint(run, config.checkpoint, epoch, head, tail, size);
        }
    }

    ProtocolRun {
        head,
        tail,
        wall_elapsed_ns: wall_started.elapsed().as_nanos(),
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
    run: &mut Run,
    checkpoint: ObservationId,
    epoch: usize,
    head: u64,
    tail: u64,
    size: FileSize,
) {
    let mut fields = Fields::new();
    fields.insert("epoch", epoch);
    for (name, value) in [
        ("head", head),
        ("tail", tail),
        ("file_logical_bytes", size.logical),
        ("file_allocated_bytes", size.allocated),
    ] {
        fields.insert(name, value);
    }
    run.observe(checkpoint, fields);
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
