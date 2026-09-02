use std::{hint::black_box, num::NonZeroUsize, time::Duration};

use dogpaddle_store::{AppendLog, Store, StoreError, StoreValue};

use crate::{
    CURSOR_KEY,
    fixture::{FilterMode, LogFixture},
    oracle::{
        assert_bounds, assert_count_station, assert_station_cursor, decode_cursor, decode_diff,
        decode_key, expected_diff_checksum, expected_full_scan_checksum, make_records_from,
        scan_limit, to_u64,
    },
    support::BenchRoot,
};

pub(super) fn measure_append<T: StoreValue>(
    bench_root: &BenchRoot,
    records: &[T],
    expected: usize,
) -> Duration {
    measure_durable_append(bench_root, records, expected, records.len())
}

pub(super) fn measure_batch_append<T: StoreValue>(
    bench_root: &BenchRoot,
    records: &[T],
    expected: usize,
) -> Duration {
    let root = bench_root.sample("append-log-batch");
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

pub(super) fn measure_append_body<T: StoreValue>(
    bench_root: &BenchRoot,
    records: &[T],
    batch: bool,
) -> Duration {
    let root = bench_root.sample("append-log-body");
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

pub(super) fn measure_durable_append<T: StoreValue>(
    bench_root: &BenchRoot,
    records: &[T],
    expected: usize,
    batch_items: usize,
) -> Duration {
    let root = bench_root.sample("append-log-durable");
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

pub(super) fn measure_project_scan(
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

pub(super) fn measure_decode_scan(
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

pub(super) fn measure_count_station(
    bench_root: &BenchRoot,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, 0);
    let expected_count = expected_diff_checksum(entries);
    let mut processed = 0_usize;
    let started = std::time::Instant::now();
    loop {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin count station transaction");
        let mut station_state = fixture
            .station_state
            .access(transaction.access())
            .expect("access count station state");
        let cursor = station_state
            .get(&CURSOR_KEY.to_vec())
            .expect("read count station cursor")
            .map(decode_cursor)
            .expect("seeded count station cursor");
        let input = fixture
            .input
            .access(transaction.access())
            .expect("access count station input");
        let mut batch_diff = 0_i64;
        let mut batch_count = 0_usize;
        let scan = input
            .scan(cursor, scan_limit(record_bytes, batch_items), |entry| {
                batch_diff = batch_diff.wrapping_add(entry.project(decode_diff)?);
                batch_count += 1;
                Ok::<(), StoreError>(())
            })
            .expect("scan count station input");
        let mut count = fixture
            .count
            .access(transaction.access())
            .expect("access count station state");
        let current = count
            .get()
            .expect("read count station state")
            .expect("seeded count station state");
        count
            .set(&current.wrapping_add(batch_diff))
            .expect("write count station state");
        station_state
            .put(
                &CURSOR_KEY.to_vec(),
                &scan.next_offset.to_be_bytes().to_vec(),
            )
            .expect("advance count station cursor");
        transaction
            .commit()
            .expect("commit count station transaction");
        processed += batch_count;
        if scan.caught_up {
            break;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(processed, entries);
    assert_count_station(&mut fixture, entries, expected_count);
    elapsed
}

pub(super) fn measure_filter_station(
    bench_root: &BenchRoot,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    mode: FilterMode,
) -> Duration {
    let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, 0);
    let mut processed = 0_usize;
    let mut forwarded = 0_usize;
    let started = std::time::Instant::now();
    loop {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin filter station transaction");
        let mut station_state = fixture
            .station_state
            .access(transaction.access())
            .expect("access filter station state");
        let cursor = station_state
            .get(&CURSOR_KEY.to_vec())
            .expect("read filter station cursor")
            .map(decode_cursor)
            .expect("seeded filter station cursor");
        let input = fixture
            .input
            .access(transaction.access())
            .expect("access filter station input");
        let mut output = fixture
            .output
            .access(transaction.access())
            .expect("access filter station output");
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
            .expect("scan filter station input");
        station_state
            .put(
                &CURSOR_KEY.to_vec(),
                &scan.next_offset.to_be_bytes().to_vec(),
            )
            .expect("advance filter station cursor");
        transaction
            .commit()
            .expect("commit filter station transaction");
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
    assert_station_cursor(&mut fixture, entries);
    assert_bounds(
        &mut fixture.transactions,
        &fixture.output,
        0,
        expected_forwarded,
    );
    elapsed
}

pub(super) fn measure_readers(
    bench_root: &BenchRoot,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    readers: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, readers);
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
            let mut station_state = fixture.reader_states[reader]
                .access(transaction.access())
                .expect("access downstream station state");
            let cursor = station_state
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
            station_state
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

pub(super) fn measure_gc(
    bench_root: &BenchRoot,
    entries: usize,
    record_bytes: usize,
    gc_items: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, 0);
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

pub(super) fn measure_steady_window(
    bench_root: &BenchRoot,
    entries: usize,
    record_bytes: usize,
    batch_items: usize,
    gc_items: usize,
) -> Duration {
    let mut fixture = LogFixture::populated(bench_root, entries, record_bytes, 0);
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
