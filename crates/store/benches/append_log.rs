//! CDC-oriented append, replay, projection, forwarding, fan-out, and GC scenarios.

use std::{borrow::Cow, hint::black_box, num::NonZeroUsize, time::Duration};

use dogpaddle_store::{
    AppendLog, Cell, CodecError, OrderedMap, ScanLimit, Small, Store, StoreError, StoreValue,
    Transactions,
};
use tempfile::TempDir;

mod support;

use support::{
    SampleWork, average_duration, emit_configuration, emit_pair_summary, emit_samples,
    emit_summary, format_duration, initialize, sample_dir, setting, setting_list,
};

const DEFAULT_ENTRIES: usize = 10_000;
const DEFAULT_COMMITS: usize = 1_000;
const DEFAULT_SAMPLES: usize = 9;
const DEFAULT_RECORD_BYTES: &[usize] = &[128, 1_024, 8_192];
const DEFAULT_SOURCE_BATCH_ITEMS: &[usize] = &[1, 64, 1_024];
const DEFAULT_STAGE_RECORD_BYTES: usize = 1_024;
const DEFAULT_STAGE_BATCH_ITEMS: usize = 1_024;
const DEFAULT_GC_ITEMS: usize = 1_024;
const DEFAULT_READERS: &[usize] = &[1, 4];
const RECORD_HEADER_BYTES: usize = 16;
const SEED_BATCH_ITEMS: usize = 4_096;
const MEBIBYTE_BYTES: u128 = 1_048_576;
const CURSOR_KEY: &[u8] = b"input/00000000/cursor";

type StageState = OrderedMap<Vec<u8>, Vec<u8>, Small>;

#[derive(Clone)]
struct CdcRecord {
    diff: i64,
    key: u64,
    payload: Vec<u8>,
}

#[derive(Clone, Copy)]
struct RecordHeader {
    diff: i64,
    key: u64,
}

struct LogFixture {
    transactions: Transactions,
    input: AppendLog<CdcRecord>,
    output: AppendLog<CdcRecord>,
    stage_state: StageState,
    count: Cell<i64>,
    reader_states: Vec<StageState>,
    _root: TempDir,
}

#[derive(Clone, Copy)]
enum FilterMode {
    PassThrough,
    ProjectedHalf,
    DecodedHalf,
}

impl CdcRecord {
    fn new(index: usize, encoded_bytes: usize) -> Self {
        let key = u64::try_from(index).expect("benchmark record index fits in u64");
        let fill = u8::try_from(key & 0xff).expect("masked payload byte fits in u8");
        Self {
            diff: if index.is_multiple_of(2) { 1 } else { -1 },
            key,
            payload: vec![fill; encoded_bytes - RECORD_HEADER_BYTES],
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(RECORD_HEADER_BYTES + self.payload.len());
        encoded.extend_from_slice(&self.diff.to_be_bytes());
        encoded.extend_from_slice(&self.key.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        encoded
    }
}

impl StoreValue for CdcRecord {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.encode())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut bytes = bytes.into_owned();
        let header = decode_header(&bytes)?;
        let payload = bytes.split_off(RECORD_HEADER_BYTES);
        Ok(Self {
            diff: header.diff,
            key: header.key,
            payload,
        })
    }
}

impl LogFixture {
    fn populated(entries: usize, record_bytes: usize, readers: usize) -> Self {
        let root = sample_dir("append-log-fixture");
        let mut store =
            Store::create(root.path().join("store")).expect("create append-log benchmark store");
        let input = store
            .create_data::<AppendLog<CdcRecord>>("input")
            .expect("create benchmark input log");
        let output = store
            .create_data::<AppendLog<CdcRecord>>("output")
            .expect("create benchmark output log");
        let stage_state = store
            .create_data::<StageState>("stage/00000000/state")
            .expect("create benchmark stage state");
        let count = store
            .create_data::<Cell<i64>>("count")
            .expect("create benchmark count");
        let reader_states = (0..readers)
            .map(|reader| {
                store
                    .create_data::<StageState>(&format!("stage/{:08x}/state", reader + 1))
                    .expect("create benchmark reader stage state")
            })
            .collect::<Vec<_>>();
        let mut fixture = Self {
            transactions: store.into_transactions(),
            input,
            output,
            stage_state,
            count,
            reader_states,
            _root: root,
        };

        {
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin benchmark state seed");
            fixture
                .stage_state
                .access(transaction.access())
                .expect("access benchmark stage state")
                .put(&CURSOR_KEY.to_vec(), &0_u64.to_be_bytes().to_vec())
                .expect("seed benchmark stage cursor");
            fixture
                .count
                .access(transaction.access())
                .expect("access benchmark count")
                .set(&0)
                .expect("seed benchmark count");
            for state in &fixture.reader_states {
                state
                    .access(transaction.access())
                    .expect("access benchmark reader stage state")
                    .put(&CURSOR_KEY.to_vec(), &0_u64.to_be_bytes().to_vec())
                    .expect("seed benchmark reader cursor");
            }
            transaction.commit().expect("commit benchmark state seed");
        }

        for start in (0..entries).step_by(SEED_BATCH_ITEMS) {
            let end = entries.min(start + SEED_BATCH_ITEMS);
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin benchmark log seed");
            let mut input = fixture
                .input
                .access(transaction.access())
                .expect("access benchmark input log");
            for index in start..end {
                input
                    .append(&CdcRecord::new(index, record_bytes))
                    .expect("seed benchmark input record");
            }
            transaction.commit().expect("commit benchmark log seed");
        }
        fixture
    }
}

fn main() {
    initialize("store_append_log");

    let entries = setting("DOGPADDLE_BENCH_LOG_ENTRIES", DEFAULT_ENTRIES);
    let commits = setting("DOGPADDLE_BENCH_COMMITS", DEFAULT_COMMITS);
    let samples = setting("DOGPADDLE_BENCH_SAMPLES", DEFAULT_SAMPLES);
    let record_sizes = setting_list("DOGPADDLE_BENCH_LOG_RECORD_BYTES", DEFAULT_RECORD_BYTES);
    let source_batches = setting_list(
        "DOGPADDLE_BENCH_LOG_SOURCE_BATCH_ITEMS",
        DEFAULT_SOURCE_BATCH_ITEMS,
    );
    let stage_record_bytes = setting(
        "DOGPADDLE_BENCH_LOG_STAGE_RECORD_BYTES",
        DEFAULT_STAGE_RECORD_BYTES,
    );
    let stage_batch_items = setting(
        "DOGPADDLE_BENCH_LOG_STAGE_BATCH_ITEMS",
        DEFAULT_STAGE_BATCH_ITEMS,
    );
    let gc_items = setting("DOGPADDLE_BENCH_LOG_GC_ITEMS", DEFAULT_GC_ITEMS);
    let readers = setting_list("DOGPADDLE_BENCH_LOG_READERS", DEFAULT_READERS);

    assert!(entries > 0 && commits > 0 && samples > 0);
    assert!(stage_batch_items > 0 && gc_items > 0);
    assert!(stage_record_bytes >= RECORD_HEADER_BYTES);
    assert!(record_sizes.iter().all(|size| *size >= RECORD_HEADER_BYTES));
    assert!(source_batches.iter().all(|size| *size > 0));
    assert!(readers.iter().all(|count| *count > 0));
    emit_configuration(
        "store_append_log",
        &format!(
            "\"entries\":{entries},\"commits_cap\":{commits},\"samples\":{samples},\"record_bytes\":{record_sizes:?},\"source_batch_items\":{source_batches:?},\"stage_record_bytes\":{stage_record_bytes},\"stage_batch_items\":{stage_batch_items},\"gc_items\":{gc_items},\"readers\":{readers:?}"
        ),
    );

    println!("DogPaddle AppendLog benchmark");
    println!(
        "entries={entries} commits_cap={commits} samples={samples} record_bytes={record_sizes:?}"
    );
    println!(
        "source_batch_items={source_batches:?} stage_record_bytes={stage_record_bytes} stage_batch_items={stage_batch_items} gc_items={gc_items} readers={readers:?}"
    );
    println!(
        "sync=durable execution=single-thread cache=warm seed=outside-timing validation=outside-timing"
    );
    print_log_section(
        "AppendLog<T>: encoded width and read strategy",
        "one durable bulk append transaction; warm scans stay in one transaction",
    );
    benchmark_record_widths(entries, samples, stage_batch_items, &record_sizes);
    print_log_section(
        "AppendLog<T>: Source commit amortization",
        "each batch is one begin -> append -> durable commit transaction",
    );
    benchmark_durable_source(
        entries,
        commits,
        samples,
        stage_record_bytes,
        &source_batches,
    );
    print_log_section(
        "AppendLog<T>: Stage, fan-out, and GC",
        "Stage state cursor, log work, output/state writes, and durable commit are timed together",
    );
    benchmark_stage_transactions(
        entries,
        samples,
        stage_record_bytes,
        stage_batch_items,
        gc_items,
        &readers,
    );
}

fn benchmark_record_widths(
    entries: usize,
    samples: usize,
    scan_items: usize,
    record_sizes: &[usize],
) {
    for &record_bytes in record_sizes {
        let records = make_records(entries, record_bytes);
        let encoded = records.iter().map(CdcRecord::encode).collect::<Vec<_>>();

        report_log(
            "bulk append pre-encoded, one tx",
            entries,
            record_bytes,
            1,
            samples,
            || measure_append(&encoded, entries),
        );
        report_log_pair(
            "append scalar body, rollback",
            "append batch body, rollback",
            entries,
            record_bytes,
            samples,
            || measure_append_body(&records, false),
            || measure_append_body(&records, true),
        );
        report_log_pair(
            "append scalar, one durable tx",
            "append batch, one durable tx",
            entries,
            record_bytes,
            samples,
            || measure_append(&records, entries),
            || measure_batch_append(&records, entries),
        );

        let mut fixture = LogFixture::populated(entries, record_bytes, 0);
        report_log_mode_pair(
            &format!("scan decode record_bytes={record_bytes}"),
            "scan project diff",
            "scan full decode",
            entries,
            record_bytes,
            1,
            samples,
            |full| {
                if full {
                    measure_decode_scan(&mut fixture, entries, record_bytes, scan_items)
                } else {
                    measure_project_scan(&mut fixture, entries, record_bytes, scan_items)
                }
            },
        );
    }
}

fn benchmark_durable_source(
    entries: usize,
    commits: usize,
    samples: usize,
    record_bytes: usize,
    batches: &[usize],
) {
    let records = make_records(entries, record_bytes);
    for &batch_items in batches {
        let measured_entries = entries.min(commits.saturating_mul(batch_items));
        let transactions = measured_entries.div_ceil(batch_items);
        report_log(
            &format!("source append b{batch_items} ({transactions} tx)"),
            measured_entries,
            record_bytes,
            transactions,
            samples,
            || measure_durable_append(&records[..measured_entries], measured_entries, batch_items),
        );
    }
}

fn benchmark_stage_transactions(
    entries: usize,
    samples: usize,
    record_bytes: usize,
    batch_items: usize,
    gc_items: usize,
    readers: &[usize],
) {
    let transactions = entries.div_ceil(batch_items);
    report_log(
        &format!("stage count project ({transactions} tx)"),
        entries,
        record_bytes,
        transactions,
        samples,
        || measure_count_stage(entries, record_bytes, batch_items),
    );
    report_log(
        &format!("stage raw pass-through ({transactions} tx)"),
        entries,
        record_bytes,
        transactions,
        samples,
        || measure_filter_stage(entries, record_bytes, batch_items, FilterMode::PassThrough),
    );
    let projected = format!("stage filter 50% project ({transactions} tx)");
    let decoded = format!("stage filter 50% decode ({transactions} tx)");
    report_log_mode_pair(
        &format!("stage filter 50% record_bytes={record_bytes}"),
        &projected,
        &decoded,
        entries,
        record_bytes,
        transactions,
        samples,
        |decode| {
            measure_filter_stage(
                entries,
                record_bytes,
                batch_items,
                if decode {
                    FilterMode::DecodedHalf
                } else {
                    FilterMode::ProjectedHalf
                },
            )
        },
    );

    let steady_transactions =
        entries.div_ceil(batch_items) + chunked_gc_transactions(entries, batch_items, gc_items);
    report_log(
        &format!("steady append + GC ({steady_transactions} tx)"),
        entries,
        record_bytes,
        steady_transactions,
        samples,
        || measure_steady_window(entries, record_bytes, batch_items, gc_items),
    );

    for &reader_count in readers {
        let deliveries = entries
            .checked_mul(reader_count)
            .expect("benchmark delivery count fits in usize");
        let reader_transactions = transactions
            .checked_mul(reader_count)
            .expect("benchmark transaction count fits in usize");
        report_log(
            &format!("downstream replay x{reader_count} ({reader_transactions} tx)"),
            deliveries,
            record_bytes,
            reader_transactions,
            samples,
            || measure_readers(entries, record_bytes, batch_items, reader_count),
        );
    }

    let gc_transactions = entries.div_ceil(gc_items);
    report_log(
        &format!("prefix GC b{gc_items} ({gc_transactions} tx)"),
        entries,
        record_bytes,
        gc_transactions,
        samples,
        || measure_gc(entries, record_bytes, gc_items),
    );
}

fn measure_append<T: StoreValue>(records: &[T], expected: usize) -> Duration {
    measure_durable_append(records, expected, records.len())
}

fn measure_batch_append<T: StoreValue>(records: &[T], expected: usize) -> Duration {
    let root = sample_dir("append-log-batch");
    let mut store =
        Store::create(root.path().join("store")).expect("create batch append benchmark store");
    let log = store
        .create_data::<AppendLog<T>>("log")
        .expect("create batch append benchmark log");
    let mut transactions = store.into_transactions();

    let started = std::time::Instant::now();
    let transaction = transactions
        .begin()
        .expect("begin batch append benchmark transaction");
    log.access(transaction.access())
        .expect("access batch append benchmark log")
        .append_batch(records)
        .expect("append benchmark batch");
    transaction
        .commit()
        .expect("commit batch append benchmark transaction");
    let elapsed = started.elapsed();
    assert_bounds(&mut transactions, &log, 0, expected);
    elapsed
}

fn measure_append_body<T: StoreValue>(records: &[T], batch: bool) -> Duration {
    let root = sample_dir("append-log-body");
    let mut store =
        Store::create(root.path().join("store")).expect("create append-body benchmark store");
    let log_handle = store
        .create_data::<AppendLog<T>>("log")
        .expect("create append-body benchmark log");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin append-body benchmark transaction");
    let elapsed = {
        let mut log = log_handle
            .access(transaction.access())
            .expect("access append-body benchmark log");

        let started = std::time::Instant::now();
        if batch {
            log.append_batch(records)
                .expect("append benchmark batch body");
        } else {
            for record in records {
                log.append(record).expect("append benchmark scalar body");
            }
        }
        let elapsed = started.elapsed();
        assert_eq!(
            log.bounds().expect("read append-body bounds"),
            0..u64::try_from(records.len()).expect("record count fits u64")
        );
        elapsed
    };
    drop(transaction);
    assert_bounds(&mut transactions, &log_handle, 0, 0);
    elapsed
}

fn measure_durable_append<T: StoreValue>(
    records: &[T],
    expected: usize,
    batch_items: usize,
) -> Duration {
    let root = sample_dir("append-log-durable");
    let mut store =
        Store::create(root.path().join("store")).expect("create append benchmark store");
    let log = store
        .create_data::<AppendLog<T>>("log")
        .expect("create append benchmark log");
    let mut transactions = store.into_transactions();

    let started = std::time::Instant::now();
    for batch in records.chunks(batch_items) {
        let transaction = transactions
            .begin()
            .expect("begin append benchmark transaction");
        let mut log = log
            .access(transaction.access())
            .expect("access append benchmark log");
        for record in batch {
            log.append(record).expect("append benchmark record");
        }
        transaction
            .commit()
            .expect("commit append benchmark transaction");
    }
    let elapsed = started.elapsed();
    assert_bounds(&mut transactions, &log, 0, expected);
    elapsed
}

fn measure_project_scan(
    fixture: &mut LogFixture,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
) -> Duration {
    let expected_checksum = expected_diff_checksum(entries);
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin projection scan transaction");
    let input = fixture
        .input
        .access(transaction.access())
        .expect("access projection scan log");
    let mut cursor = 0;
    let mut count = 0_usize;
    let mut checksum = 0_i64;
    loop {
        let scan = input
            .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                checksum = checksum.wrapping_add(entry.project(decode_diff)?);
                count += 1;
                Ok::<(), StoreError>(())
            })
            .expect("project benchmark batch");
        cursor = scan.next_offset;
        if scan.caught_up {
            break;
        }
    }
    transaction
        .commit()
        .expect("finish projection scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(cursor, to_u64(entries));
    assert_eq!(checksum, expected_checksum);
    black_box(checksum);
    elapsed
}

fn measure_decode_scan(
    fixture: &mut LogFixture,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
) -> Duration {
    let expected_checksum = expected_full_scan_checksum(entries, record_bytes);
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin full decode scan transaction");
    let input = fixture
        .input
        .access(transaction.access())
        .expect("access full decode scan log");
    let mut cursor = 0;
    let mut count = 0_usize;
    let mut checksum = 0_u64;
    loop {
        let scan = input
            .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                let record = entry.decode_owned()?;
                checksum = checksum.wrapping_add(record.key).wrapping_add(u64::from(
                    record.payload.first().copied().unwrap_or_default(),
                ));
                count += 1;
                Ok::<(), StoreError>(())
            })
            .expect("full decode benchmark batch");
        cursor = scan.next_offset;
        if scan.caught_up {
            break;
        }
    }
    transaction
        .commit()
        .expect("finish full decode scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(cursor, to_u64(entries));
    assert_eq!(checksum, expected_checksum);
    black_box(checksum);
    elapsed
}

fn measure_count_stage(entries: usize, record_bytes: usize, batch_items: usize) -> Duration {
    let mut fixture = LogFixture::populated(entries, record_bytes, 0);
    let expected_count = expected_diff_checksum(entries);
    let mut processed = 0_usize;
    let started = std::time::Instant::now();
    loop {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin count stage transaction");
        let mut stage_state = fixture
            .stage_state
            .access(transaction.access())
            .expect("access count stage state");
        let cursor = stage_state
            .get(&CURSOR_KEY.to_vec())
            .expect("read count stage cursor")
            .map(decode_cursor)
            .expect("seeded count stage cursor");
        let input = fixture
            .input
            .access(transaction.access())
            .expect("access count stage input");
        let mut batch_diff = 0_i64;
        let mut batch_count = 0_usize;
        let scan = input
            .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                batch_diff = batch_diff.wrapping_add(entry.project(decode_diff)?);
                batch_count += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan count stage input");
        let mut count = fixture
            .count
            .access(transaction.access())
            .expect("access count stage state");
        let current = count
            .get()
            .expect("read count stage state")
            .expect("seeded count stage state");
        count
            .set(&current.wrapping_add(batch_diff))
            .expect("write count stage state");
        stage_state
            .put(
                &CURSOR_KEY.to_vec(),
                &scan.next_offset.to_be_bytes().to_vec(),
            )
            .expect("advance count stage cursor");
        transaction
            .commit()
            .expect("commit count stage transaction");
        processed += batch_count;
        if scan.caught_up {
            break;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(processed, entries);
    assert_count_stage(&mut fixture, entries, expected_count);
    elapsed
}

fn measure_filter_stage(
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    mode: FilterMode,
) -> Duration {
    let mut fixture = LogFixture::populated(entries, record_bytes, 0);
    let mut processed = 0_usize;
    let mut forwarded = 0_usize;
    let started = std::time::Instant::now();
    loop {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin filter stage transaction");
        let mut stage_state = fixture
            .stage_state
            .access(transaction.access())
            .expect("access filter stage state");
        let cursor = stage_state
            .get(&CURSOR_KEY.to_vec())
            .expect("read filter stage cursor")
            .map(decode_cursor)
            .expect("seeded filter stage cursor");
        let input = fixture
            .input
            .access(transaction.access())
            .expect("access filter stage input");
        let mut output = fixture
            .output
            .access(transaction.access())
            .expect("access filter stage output");
        let scan = input
            .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                let pass = match mode {
                    FilterMode::PassThrough => true,
                    FilterMode::ProjectedHalf => entry.project(decode_key)? % 2 == 0,
                    FilterMode::DecodedHalf => entry.decode_owned()?.key % 2 == 0,
                };
                if pass {
                    output.append_entry(&entry)?;
                    forwarded += 1;
                }
                processed += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan filter stage input");
        stage_state
            .put(
                &CURSOR_KEY.to_vec(),
                &scan.next_offset.to_be_bytes().to_vec(),
            )
            .expect("advance filter stage cursor");
        transaction
            .commit()
            .expect("commit filter stage transaction");
        if scan.caught_up {
            break;
        }
    }
    let elapsed = started.elapsed();
    let expected_forwarded = match mode {
        FilterMode::PassThrough => entries,
        FilterMode::ProjectedHalf | FilterMode::DecodedHalf => entries.div_ceil(2),
    };
    assert_eq!(processed, entries);
    assert_eq!(forwarded, expected_forwarded);
    assert_stage_cursor(&mut fixture, entries);
    assert_bounds(
        &mut fixture.transactions,
        &fixture.output,
        0,
        expected_forwarded,
    );
    elapsed
}

fn measure_readers(
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    readers: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(entries, record_bytes, readers);
    let mut caught_up = vec![false; readers];
    let mut active = readers;
    let mut deliveries = 0_usize;
    let mut checksum = 0_i64;
    let started = std::time::Instant::now();
    while active > 0 {
        for (reader, done) in caught_up.iter_mut().enumerate() {
            if *done {
                continue;
            }
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin downstream transaction");
            let mut stage_state = fixture.reader_states[reader]
                .access(transaction.access())
                .expect("access downstream stage state");
            let cursor = stage_state
                .get(&CURSOR_KEY.to_vec())
                .expect("read downstream cursor")
                .map(decode_cursor)
                .expect("seeded downstream cursor");
            let input = fixture
                .input
                .access(transaction.access())
                .expect("access downstream input log");
            let scan = input
                .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                    checksum = checksum.wrapping_add(entry.project(decode_diff)?);
                    deliveries += 1;
                    Ok::<(), StoreError>(())
                })
                .expect("scan downstream input");
            stage_state
                .put(
                    &CURSOR_KEY.to_vec(),
                    &scan.next_offset.to_be_bytes().to_vec(),
                )
                .expect("advance downstream cursor");
            transaction.commit().expect("commit downstream transaction");
            if scan.caught_up {
                *done = true;
                active -= 1;
            }
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(
        deliveries,
        entries
            .checked_mul(readers)
            .expect("benchmark delivery count fits in usize")
    );
    black_box(checksum);
    elapsed
}

fn measure_gc(entries: usize, record_bytes: usize, gc_items: usize) -> Duration {
    let mut fixture = LogFixture::populated(entries, record_bytes, 0);
    let max_items = NonZeroUsize::new(gc_items).expect("non-zero GC batch");
    let target = to_u64(entries);
    let mut head = 0;
    let started = std::time::Instant::now();
    while head < target {
        let transaction = fixture.transactions.begin().expect("begin GC transaction");
        head = fixture
            .input
            .access(transaction.access())
            .expect("access GC log")
            .truncate_before(target, max_items)
            .expect("truncate benchmark log");
        transaction.commit().expect("commit GC transaction");
    }
    let elapsed = started.elapsed();
    assert_bounds(&mut fixture.transactions, &fixture.input, entries, entries);
    elapsed
}

fn measure_steady_window(
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    gc_items: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(entries, record_bytes, 0);
    let records = make_records_from(entries, entries, record_bytes);
    let max_gc_items = NonZeroUsize::new(gc_items).expect("non-zero GC batch");
    let mut appended = 0_usize;
    let mut head = 0_u64;

    let started = std::time::Instant::now();
    for batch in records.chunks(batch_items) {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin steady append transaction");
        let mut input = fixture
            .input
            .access(transaction.access())
            .expect("access steady append log");
        for record in batch {
            input.append(record).expect("append steady record");
        }
        transaction.commit().expect("commit steady append");

        appended += batch.len();
        let target = to_u64(appended);
        while head < target {
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin steady GC transaction");
            head = fixture
                .input
                .access(transaction.access())
                .expect("access steady GC log")
                .truncate_before(target, max_gc_items)
                .expect("truncate steady log");
            transaction.commit().expect("commit steady GC");
        }
    }
    let elapsed = started.elapsed();

    assert_eq!(appended, entries);
    assert_bounds(
        &mut fixture.transactions,
        &fixture.input,
        entries,
        entries
            .checked_mul(2)
            .expect("benchmark tail fits in usize"),
    );
    elapsed
}

fn assert_stage_cursor(fixture: &mut LogFixture, expected: usize) {
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage validation transaction");
    let cursor = fixture
        .stage_state
        .access(transaction.access())
        .expect("access stage validation state")
        .get(&CURSOR_KEY.to_vec())
        .expect("read stage validation cursor")
        .map(decode_cursor)
        .expect("seeded stage validation cursor");
    assert_eq!(cursor, to_u64(expected));
    transaction
        .commit()
        .expect("finish stage validation transaction");
}

fn assert_count_stage(fixture: &mut LogFixture, expected_cursor: usize, expected_count: i64) {
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin count stage validation transaction");
    let cursor = fixture
        .stage_state
        .access(transaction.access())
        .expect("access count stage validation state")
        .get(&CURSOR_KEY.to_vec())
        .expect("read count stage validation cursor")
        .map(decode_cursor)
        .expect("seeded count stage validation cursor");
    let count = fixture
        .count
        .access(transaction.access())
        .expect("access count stage validation count")
        .get()
        .expect("read count stage validation count")
        .expect("seeded count stage validation count");
    assert_eq!(cursor, to_u64(expected_cursor));
    assert_eq!(count, expected_count);
    transaction
        .commit()
        .expect("finish count stage validation transaction");
}

fn assert_bounds<T: StoreValue>(
    transactions: &mut Transactions,
    log: &AppendLog<T>,
    head: usize,
    tail: usize,
) {
    let transaction = transactions
        .begin()
        .expect("begin log validation transaction");
    let bounds = log
        .access(transaction.access())
        .expect("access validation log")
        .bounds()
        .expect("read validation bounds");
    assert_eq!(bounds, to_u64(head)..to_u64(tail));
    transaction
        .commit()
        .expect("finish log validation transaction");
}

fn decode_header(encoded: &[u8]) -> Result<RecordHeader, CodecError> {
    if encoded.len() < RECORD_HEADER_BYTES {
        return Err(CodecError::new("truncated benchmark CDC record"));
    }
    let diff = i64::from_be_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| CodecError::new("invalid benchmark diff"))?,
    );
    let key = u64::from_be_bytes(
        encoded[8..RECORD_HEADER_BYTES]
            .try_into()
            .map_err(|_| CodecError::new("invalid benchmark key"))?,
    );
    Ok(RecordHeader { diff, key })
}

fn decode_diff(encoded: &[u8]) -> Result<i64, CodecError> {
    decode_header(encoded).map(|header| header.diff)
}

fn decode_key(encoded: &[u8]) -> Result<u64, CodecError> {
    decode_header(encoded).map(|header| header.key)
}

fn decode_cursor(encoded: Vec<u8>) -> u64 {
    u64::decode_value(encoded.into()).expect("valid benchmark cursor encoding")
}

fn expected_diff_checksum(entries: usize) -> i64 {
    i64::from(!entries.is_multiple_of(2))
}

fn expected_full_scan_checksum(entries: usize, record_bytes: usize) -> u64 {
    (0..entries).fold(0_u64, |checksum, index| {
        let key = to_u64(index);
        let fill = if record_bytes == RECORD_HEADER_BYTES {
            0
        } else {
            u8::try_from(key & 0xff).expect("masked payload byte fits in u8")
        };
        checksum.wrapping_add(key).wrapping_add(u64::from(fill))
    })
}

fn make_records(entries: usize, record_bytes: usize) -> Vec<CdcRecord> {
    make_records_from(0, entries, record_bytes)
}

fn make_records_from(start: usize, entries: usize, record_bytes: usize) -> Vec<CdcRecord> {
    let end = start
        .checked_add(entries)
        .expect("benchmark record range fits in usize");
    (start..end)
        .map(|index| CdcRecord::new(index, record_bytes))
        .collect()
}

fn chunked_gc_transactions(entries: usize, batch_items: usize, gc_items: usize) -> usize {
    (0..entries)
        .step_by(batch_items)
        .map(|start| entries.min(start.saturating_add(batch_items)) - start)
        .map(|batch| batch.div_ceil(gc_items))
        .sum()
}

fn scan_limit(record_bytes: usize, batch_items: usize) -> ScanLimit {
    let item_bytes = record_bytes
        .checked_add(size_of::<u64>())
        .expect("benchmark item byte count fits in usize");
    let max_bytes = item_bytes
        .checked_mul(batch_items)
        .expect("benchmark batch byte count fits in usize");
    ScanLimit::new(batch_items, max_bytes).expect("non-zero benchmark scan limit")
}

fn report_log(
    workload: &str,
    records: usize,
    record_bytes: usize,
    transactions: usize,
    samples: usize,
    mut measure: impl FnMut() -> Duration,
) {
    measure();
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        durations.push(measure());
    }
    report_log_measurements(workload, records, record_bytes, transactions, &durations);
}

#[allow(clippy::too_many_arguments)]
fn report_log_mode_pair(
    pair_scenario: &str,
    first_name: &str,
    second_name: &str,
    records: usize,
    record_bytes: usize,
    transactions: usize,
    samples: usize,
    mut measure: impl FnMut(bool) -> Duration,
) {
    measure(false);
    measure(true);
    let mut first_durations = Vec::with_capacity(samples);
    let mut second_durations = Vec::with_capacity(samples);
    for sample in 0..samples {
        if matches!(sample % 4, 0 | 3) {
            first_durations.push(measure(false));
            second_durations.push(measure(true));
        } else {
            second_durations.push(measure(true));
            first_durations.push(measure(false));
        }
    }

    report_log_measurements(
        first_name,
        records,
        record_bytes,
        transactions,
        &first_durations,
    );
    report_log_measurements(
        second_name,
        records,
        record_bytes,
        transactions,
        &second_durations,
    );
    emit_pair_summary(
        "store_append_log",
        pair_scenario,
        first_name,
        second_name,
        &first_durations,
        &second_durations,
    );
    let wins = first_durations
        .iter()
        .zip(&second_durations)
        .filter(|(first, second)| second < first)
        .count();
    let mut ratios = first_durations
        .iter()
        .zip(&second_durations)
        .map(|(first, second)| first.as_secs_f64() / second.as_secs_f64())
        .collect::<Vec<_>>();
    ratios.sort_by(f64::total_cmp);
    println!(
        "  paired first/second median={:.3}x; second wins {wins}/{samples}",
        ratios[ratios.len() / 2]
    );
}

fn report_log_measurements(
    workload: &str,
    records: usize,
    record_bytes: usize,
    transactions: usize,
    durations: &[Duration],
) {
    let work = SampleWork {
        operations: records,
        transactions,
        logical_bytes: records.checked_mul(record_bytes).unwrap(),
    };
    emit_samples("store_append_log", workload, "default", durations, work);
    emit_summary("store_append_log", workload, "default", durations, work);
    let mut durations = durations.to_vec();
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let records_per_second = records as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_record = average_duration(median, records);
    let encoded_mib_tenths_per_second = records as u128 * record_bytes as u128 * 10 * 1_000_000_000
        / median.as_nanos()
        / MEBIBYTE_BYTES;
    let encoded_mib_per_second = format!(
        "{}.{:01}",
        encoded_mib_tenths_per_second / 10,
        encoded_mib_tenths_per_second % 10
    );
    println!(
        "{workload:<45} {record_bytes:>9} {records:>11} {:>12} {:>12} {:>12} {median_per_record:>12} {records_per_second:>13} {encoded_mib_per_second:>13}",
        format_duration(min),
        format_duration(median),
        format_duration(max),
    );
}

fn report_log_pair(
    first_name: &str,
    second_name: &str,
    records: usize,
    record_bytes: usize,
    samples: usize,
    mut first: impl FnMut() -> Duration,
    mut second: impl FnMut() -> Duration,
) {
    first();
    second();
    let mut first_durations = Vec::with_capacity(samples);
    let mut second_durations = Vec::with_capacity(samples);
    for sample in 0..samples {
        if matches!(sample % 4, 0 | 3) {
            first_durations.push(first());
            second_durations.push(second());
        } else {
            second_durations.push(second());
            first_durations.push(first());
        }
    }

    let work = SampleWork {
        operations: records,
        transactions: 1,
        logical_bytes: records.checked_mul(record_bytes).unwrap(),
    };
    emit_samples(
        "store_append_log",
        first_name,
        "first",
        &first_durations,
        work,
    );
    emit_summary(
        "store_append_log",
        first_name,
        "first",
        &first_durations,
        work,
    );
    emit_samples(
        "store_append_log",
        second_name,
        "second",
        &second_durations,
        work,
    );
    emit_summary(
        "store_append_log",
        second_name,
        "second",
        &second_durations,
        work,
    );
    emit_pair_summary(
        "store_append_log",
        &format!("record_bytes={record_bytes}"),
        first_name,
        second_name,
        &first_durations,
        &second_durations,
    );

    print_log_measurements(first_name, records, record_bytes, first_durations.clone());
    print_log_measurements(second_name, records, record_bytes, second_durations.clone());

    let wins = first_durations
        .iter()
        .zip(&second_durations)
        .filter(|(first, second)| second < first)
        .count();
    let mut ratios = first_durations
        .iter()
        .zip(&second_durations)
        .map(|(first, second)| first.as_secs_f64() / second.as_secs_f64())
        .collect::<Vec<_>>();
    ratios.sort_by(f64::total_cmp);
    println!(
        "  paired first/second median={:.3}x; second wins {wins}/{samples}",
        ratios[ratios.len() / 2]
    );
}

fn print_log_measurements(
    workload: &str,
    records: usize,
    record_bytes: usize,
    mut durations: Vec<Duration>,
) {
    durations.sort_unstable();
    let min = durations[0];
    let median = durations[durations.len() / 2];
    let max = durations[durations.len() - 1];
    let records_per_second = records as u128 * 1_000_000_000 / median.as_nanos();
    let median_per_record = average_duration(median, records);
    let encoded_mib_tenths_per_second = records as u128 * record_bytes as u128 * 10 * 1_000_000_000
        / median.as_nanos()
        / MEBIBYTE_BYTES;
    let encoded_mib_per_second = format!(
        "{}.{:01}",
        encoded_mib_tenths_per_second / 10,
        encoded_mib_tenths_per_second % 10
    );
    println!(
        "{workload:<45} {record_bytes:>9} {records:>11} {:>12} {:>12} {:>12} {median_per_record:>12} {records_per_second:>13} {encoded_mib_per_second:>13}",
        format_duration(min),
        format_duration(median),
        format_duration(max),
    );
}

fn print_log_section(name: &str, description: &str) {
    println!();
    println!("=== {name} ===");
    println!("{description}");
    println!(
        "{:<45} {:>9} {:>11} {:>12} {:>12} {:>12} {:>12} {:>13} {:>13}",
        "workload",
        "record B",
        "records",
        "min",
        "median",
        "max",
        "median/item",
        "records/s",
        "encoded MiB/s"
    );
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark value fits in u64")
}
