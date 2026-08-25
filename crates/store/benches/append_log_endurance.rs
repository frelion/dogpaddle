//! Long-running fixed-window append, prefix-GC, space-reuse, and reopen validation.

use std::{
    borrow::Cow,
    fs,
    hint::black_box,
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

use dogpaddle_store::{
    AppendLog, CodecError, ScanLimit, Store, StoreError, StoreValue, Transactions,
};

const DEFAULT_RECORD_BYTES: &[usize] = &[128, 1_024, 8_192];
const DEFAULT_LOGICAL_MIB: usize = 1_024;
const DEFAULT_WINDOW_MIB: usize = 64;
const DEFAULT_BATCH_MIB: usize = 1;
const DEFAULT_CHECKPOINT_EPOCHS: usize = 64;
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

struct Latencies {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

struct EnduranceResult {
    record_bytes: usize,
    batch_items: usize,
    window_items: usize,
    steady_epochs: usize,
    steady_records: usize,
    append: Latencies,
    gc: Latencies,
    protocol_elapsed: Duration,
    wall_elapsed: Duration,
    seed_file: FileSize,
    final_file: FileSize,
    peak_file: FileSize,
    tail_allocated_spread_basis_points: u64,
    validation_checksum: u64,
}

struct ProtocolRun {
    head: u64,
    tail: u64,
    append_durations: Vec<Duration>,
    gc_durations: Vec<Duration>,
    file_samples: Vec<FileSize>,
    wall_elapsed: Duration,
}

#[derive(Clone, Copy)]
struct ProtocolConfig<'a> {
    store_path: &'a Path,
    window_items: usize,
    steady_epochs: usize,
    checkpoint_epochs: usize,
    seed_file: FileSize,
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
    if cfg!(debug_assertions) {
        return;
    }

    let record_sizes = setting_list(
        "DOGPADDLE_BENCH_ENDURANCE_RECORD_BYTES",
        DEFAULT_RECORD_BYTES,
    );
    let logical_mib = setting("DOGPADDLE_BENCH_ENDURANCE_LOGICAL_MIB", DEFAULT_LOGICAL_MIB);
    let window_mib = setting("DOGPADDLE_BENCH_ENDURANCE_WINDOW_MIB", DEFAULT_WINDOW_MIB);
    let batch_mib = setting("DOGPADDLE_BENCH_ENDURANCE_BATCH_MIB", DEFAULT_BATCH_MIB);
    let checkpoint_epochs = setting(
        "DOGPADDLE_BENCH_ENDURANCE_CHECKPOINT_EPOCHS",
        DEFAULT_CHECKPOINT_EPOCHS,
    );

    assert!(logical_mib > window_mib);
    assert!(window_mib > 0 && batch_mib > 0 && checkpoint_epochs > 0);
    assert!(
        record_sizes
            .iter()
            .all(|record_bytes| *record_bytes >= RECORD_HEADER_BYTES)
    );

    println!("DogPaddle AppendLog endurance benchmark");
    println!(
        "record_bytes={record_sizes:?} logical_mib_per_width={logical_mib} window_mib={window_mib} batch_mib={batch_mib} checkpoint_epochs={checkpoint_epochs}"
    );
    println!(
        "protocol=append_batch+durable_commit then truncate_before+durable_commit sync=durable execution=single-thread"
    );
    println!(
        "platform={}-{} temp_root={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::temp_dir().display()
    );

    let mut results = Vec::with_capacity(record_sizes.len());
    for record_bytes in record_sizes {
        results.push(run_endurance(
            record_bytes,
            logical_mib,
            window_mib,
            batch_mib,
            checkpoint_epochs,
        ));
    }

    print_summary(&results);
}

fn run_endurance(
    record_bytes: usize,
    logical_mib: usize,
    window_mib: usize,
    batch_mib: usize,
    checkpoint_epochs: usize,
) -> EnduranceResult {
    let batch_target_bytes = mib_bytes(batch_mib);
    let batch_items = (batch_target_bytes / record_bytes).max(1);
    let batch_bytes = batch_items
        .checked_mul(record_bytes)
        .expect("batch byte size fits in usize");
    let window_batches = mib_bytes(window_mib).div_ceil(batch_bytes).max(1);
    let window_items = window_batches
        .checked_mul(batch_items)
        .expect("window item count fits in usize");
    let total_batches = mib_bytes(logical_mib)
        .div_ceil(batch_bytes)
        .max(window_batches + 1);
    let steady_epochs = total_batches - window_batches;
    let steady_records = steady_epochs
        .checked_mul(batch_items)
        .expect("steady record count fits in usize");
    let max_gc_items = NonZeroUsize::new(batch_items).expect("batch item count is non-zero");
    let records = (0..batch_items)
        .map(|index| EnduranceRecord::new(index, record_bytes))
        .collect::<Vec<_>>();

    let root = tempfile::tempdir().expect("temporary endurance benchmark directory");
    let store_path = root.path().join("store");
    let mut store = Store::create(&store_path).expect("create endurance benchmark store");
    let log = store
        .create_data::<AppendLog<EnduranceRecord>>("log")
        .expect("create endurance benchmark log");
    let mut transactions = store.into_transactions();
    seed_window(&mut transactions, &log, &records, window_batches);
    let seed_file = data_file_size(&store_path);
    print_protocol_header(record_bytes, batch_items, window_items, steady_epochs);
    let run = run_protocol(
        &mut transactions,
        &log,
        &records,
        max_gc_items,
        ProtocolConfig {
            store_path: &store_path,
            window_items,
            steady_epochs,
            checkpoint_epochs,
            seed_file,
        },
    );
    let protocol_elapsed = run
        .append_durations
        .iter()
        .chain(&run.gc_durations)
        .copied()
        .sum();

    let final_file = data_file_size(&store_path);
    let peak_file = FileSize {
        logical: run
            .file_samples
            .iter()
            .map(|sample| sample.logical)
            .max()
            .expect("at least the seed file sample exists"),
        allocated: run
            .file_samples
            .iter()
            .map(|sample| sample.allocated)
            .max()
            .expect("at least the seed file sample exists"),
    };
    let tail_allocated_spread_basis_points = tail_spread_basis_points(&run.file_samples);

    drop(transactions);
    let validation_checksum = validate_reopened(
        &store_path,
        run.head,
        run.tail,
        record_bytes,
        batch_items,
        window_items,
    );
    black_box(validation_checksum);

    EnduranceResult {
        record_bytes,
        batch_items,
        window_items,
        steady_epochs,
        steady_records,
        append: summarize(run.append_durations),
        gc: summarize(run.gc_durations),
        protocol_elapsed,
        wall_elapsed: run.wall_elapsed,
        seed_file,
        final_file,
        peak_file,
        tail_allocated_spread_basis_points,
        validation_checksum,
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

fn print_protocol_header(
    record_bytes: usize,
    batch_items: usize,
    window_items: usize,
    steady_epochs: usize,
) {
    println!();
    println!(
        "=== record_bytes={record_bytes} batch_items={batch_items} window_items={window_items} steady_epochs={steady_epochs} ==="
    );
    println!(
        "{:<12} {:>14} {:>14} {:>14} {:>14}",
        "epoch", "head", "tail", "file logical", "file allocated"
    );
}

fn run_protocol(
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
    let mut file_samples = vec![config.seed_file];
    let mut append_durations = Vec::with_capacity(config.steady_epochs);
    let mut gc_durations = Vec::with_capacity(config.steady_epochs);
    print_checkpoint(0, head, tail, config.seed_file);

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
        assert_eq!(assigned, tail..tail + batch_items_u64);
        transaction
            .commit()
            .expect("commit endurance append transaction");
        append_durations.push(append_started.elapsed());
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
        assert_eq!(next_head, target);
        transaction
            .commit()
            .expect("commit endurance GC transaction");
        gc_durations.push(gc_started.elapsed());
        head = next_head;

        if epoch.is_multiple_of(config.checkpoint_epochs) || epoch == config.steady_epochs {
            let size = data_file_size(config.store_path);
            file_samples.push(size);
            print_checkpoint(epoch, head, tail, size);
        }
    }

    ProtocolRun {
        head,
        tail,
        append_durations,
        gc_durations,
        file_samples,
        wall_elapsed: wall_started.elapsed(),
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

fn summarize(mut samples: Vec<Duration>) -> Latencies {
    samples.sort_unstable();
    Latencies {
        p50: percentile(&samples, 50),
        p95: percentile(&samples, 95),
        p99: percentile(&samples, 99),
        max: *samples.last().expect("at least one latency sample exists"),
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let rank = samples
        .len()
        .checked_mul(percentile)
        .expect("percentile rank fits in usize")
        .div_ceil(100)
        .saturating_sub(1);
    samples[rank.min(samples.len() - 1)]
}

fn tail_spread_basis_points(samples: &[FileSize]) -> u64 {
    let tail = &samples[samples.len() / 2..];
    let minimum = tail
        .iter()
        .map(|sample| sample.allocated)
        .min()
        .expect("at least one tail file sample exists");
    let maximum = tail
        .iter()
        .map(|sample| sample.allocated)
        .max()
        .expect("at least one tail file sample exists");
    if minimum == 0 {
        0
    } else {
        let spread = u128::from(maximum - minimum)
            .checked_mul(10_000)
            .expect("tail spread calculation fits in u128")
            / u128::from(minimum);
        u64::try_from(spread).expect("tail spread basis points fit in u64")
    }
}

fn print_checkpoint(epoch: usize, head: u64, tail: u64, size: FileSize) {
    println!(
        "{epoch:<12} {head:>14} {tail:>14} {:>14} {:>14}",
        bytes(size.logical),
        bytes(size.allocated)
    );
}

fn print_summary(results: &[EnduranceResult]) {
    println!();
    println!("=== Endurance summary ===");
    for result in results {
        let retained_payload = result
            .window_items
            .checked_mul(result.record_bytes)
            .expect("retained payload byte size fits in usize");
        let amplification_hundredths = if retained_payload == 0 {
            0
        } else {
            u128::from(result.final_file.allocated)
                .checked_mul(100)
                .expect("allocated amplification calculation fits in u128")
                / u128::try_from(retained_payload).expect("retained payload fits in u128")
        };
        let records_per_second = u128::try_from(result.steady_records)
            .expect("steady record count fits in u128")
            .checked_mul(1_000_000_000)
            .expect("throughput calculation fits in u128")
            / result.protocol_elapsed.as_nanos();
        let amplification_whole = amplification_hundredths / 100;
        let amplification_fraction = amplification_hundredths % 100;
        let spread_whole = result.tail_allocated_spread_basis_points / 100;
        let spread_fraction = result.tail_allocated_spread_basis_points % 100;
        println!();
        println!(
            "record={} B batch={} items window={} items epochs={} steady_records={}",
            result.record_bytes,
            result.batch_items,
            result.window_items,
            result.steady_epochs,
            result.steady_records
        );
        println!(
            "  append tx p50={} p95={} p99={} max={}",
            duration(result.append.p50),
            duration(result.append.p95),
            duration(result.append.p99),
            duration(result.append.max)
        );
        println!(
            "  GC tx     p50={} p95={} p99={} max={}",
            duration(result.gc.p50),
            duration(result.gc.p95),
            duration(result.gc.p99),
            duration(result.gc.max)
        );
        println!(
            "  protocol={} wall={} throughput={records_per_second:.0} records/s",
            duration(result.protocol_elapsed),
            duration(result.wall_elapsed)
        );
        println!(
            "  file seed={} final={} peak={} allocated_amplification={amplification_whole}.{amplification_fraction:02}x tail_spread={spread_whole}.{spread_fraction:02}%",
            bytes(result.seed_file.allocated),
            bytes(result.final_file.allocated),
            bytes(result.peak_file.allocated)
        );
        println!(
            "  validation=reopen+full-retained-scan checksum={:#018x}",
            result.validation_checksum
        );
    }
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

fn bytes(value: u64) -> String {
    const GIBIBYTE_BYTES: u64 = 1_073_741_824;
    const MEBIBYTE_BYTES_U64: u64 = 1_048_576;

    if value >= GIBIBYTE_BYTES {
        scaled_bytes(value, GIBIBYTE_BYTES, "GiB")
    } else {
        scaled_bytes(value, MEBIBYTE_BYTES_U64, "MiB")
    }
}

fn scaled_bytes(value: u64, unit_bytes: u64, unit: &str) -> String {
    let hundredths = u128::from(value)
        .checked_mul(100)
        .expect("byte formatting calculation fits in u128")
        / u128::from(unit_bytes);
    format!("{}.{:02} {unit}", hundredths / 100, hundredths % 100)
}

fn duration(value: Duration) -> String {
    if value.as_secs_f64() >= 1.0 {
        format!("{:.3} s", value.as_secs_f64())
    } else if value.as_millis() > 0 {
        format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} us", value.as_secs_f64() * 1_000_000.0)
    }
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("benchmark setting must be an integer")
    })
}

fn setting_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            let parsed = value
                .split(',')
                .map(str::trim)
                .map(|item| {
                    item.parse::<usize>()
                        .expect("benchmark list setting must contain integers")
                })
                .collect::<Vec<_>>();
            assert!(!parsed.is_empty(), "benchmark list setting cannot be empty");
            parsed
        },
    )
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark value fits in u64")
}
