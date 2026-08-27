use std::{hint::black_box, time::Instant};

use dogpaddle_change::encode_change;
use dogpaddle_store::{AppendLog, CodecError, ScanLimit, Store, StoreError};

use crate::{
    case::BenchmarkCase,
    fixture::{empty_append, seeded_replay},
    model::Measurement,
    oracle::{
        DecodeOracle, validate_decoded, validate_raw, validate_reopened_decoded,
        validate_reopened_pipeline, validate_reopened_raw,
    },
    regular_support::decode_projected_entry,
    support::{BenchStoreRoot, decode_entry},
};

#[derive(Clone, Copy)]
pub(crate) enum AppendMode {
    Preencoded,
    Integrated,
}

#[derive(Clone, Copy)]
pub(crate) enum ReplayMode {
    Full,
    Selected,
}

pub(crate) fn append_durable(
    root: &BenchStoreRoot,
    label: &str,
    case: &BenchmarkCase,
    mode: AppendMode,
) -> Measurement {
    let mut fixture = empty_append(root, label);
    let limit = scan_limit(case);
    let mut retained_encoded = Vec::with_capacity(case.metadata.transactions_per_sample);
    let started = Instant::now();
    for transaction_index in 0..case.metadata.transactions_per_sample {
        let range = transaction_range(case, transaction_index);
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin durable append transaction");
        let mut access = fixture
            .log
            .access(transaction.access())
            .expect("access durable append log");
        let offsets = match mode {
            AppendMode::Preencoded => access
                .append_batch(&case.workload.encoded[range])
                .expect("append pre-encoded Changes"),
            AppendMode::Integrated => {
                let encoded = case.workload.changes[range]
                    .iter()
                    .map(|generated| {
                        encode_change(&generated.change).expect("encode benchmark Change")
                    })
                    .collect::<Vec<_>>();
                retained_encoded.push(encoded);
                access
                    .append_batch(
                        retained_encoded
                            .last()
                            .expect("integrated encoded transaction exists"),
                    )
                    .expect("append freshly encoded Changes")
            }
        };
        black_box(offsets);
        transaction
            .commit()
            .expect("durably commit append transaction");
    }
    let elapsed = started.elapsed();
    black_box(&retained_encoded);
    drop(retained_encoded);
    drop(fixture.transactions);
    let pages = validate_reopened_raw(
        fixture.sample.path(),
        "changes",
        &case.workload.encoded,
        limit,
    );
    Measurement {
        elapsed,
        pages,
        checksum: case.workload.order_checksum(),
    }
}

pub(crate) fn replay(
    root: &BenchStoreRoot,
    label: &str,
    case: &BenchmarkCase,
    mode: ReplayMode,
) -> Measurement {
    let mut fixture = seeded_replay(root, label, case, false);
    let limit = scan_limit(case);
    validate_raw(
        &fixture.input,
        &mut fixture.transactions,
        &case.workload.encoded,
        limit,
    );

    let (elapsed, pages, checksum) = {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin replay transaction");
        let access = fixture
            .input
            .access(transaction.access())
            .expect("access replay input log");
        let started = Instant::now();
        let (pages, checksum) = scan_decoded(&access, case, mode, limit);
        (started.elapsed(), pages, checksum)
    };
    assert!(pages >= 2, "headline replay must span multiple pages");
    let oracle_pages = validate_decoded(
        &fixture.input,
        &mut fixture.transactions,
        case,
        match mode {
            ReplayMode::Full => DecodeOracle::Full,
            ReplayMode::Selected => DecodeOracle::Selected,
        },
        limit,
    );
    assert_eq!(pages, oracle_pages);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

pub(crate) fn reopened_first_replay(
    root: &BenchStoreRoot,
    label: &str,
    case: &BenchmarkCase,
) -> Measurement {
    let mut fixture = seeded_replay(root, label, case, false);
    let limit = scan_limit(case);
    validate_raw(
        &fixture.input,
        &mut fixture.transactions,
        &case.workload.encoded,
        limit,
    );
    drop(fixture.transactions);

    let started = Instant::now();
    let store = Store::open(fixture.sample.path()).expect("reopen first-replay Store");
    let input = store
        .open_data::<AppendLog<Vec<u8>>>("input")
        .expect("open first-replay input log");
    let mut transactions = store.into_transactions();
    let transaction = transactions
        .begin()
        .expect("begin first-replay transaction");
    let access = input
        .access(transaction.access())
        .expect("access first-replay input log");
    let (pages, checksum) = scan_decoded(&access, case, ReplayMode::Full, limit);
    let elapsed = started.elapsed();
    drop(transaction);
    drop(transactions);
    assert!(pages >= 2, "reopened replay must span multiple pages");

    let oracle_pages = validate_reopened_decoded(
        fixture.sample.path(),
        "input",
        case,
        DecodeOracle::Full,
        limit,
    );
    assert_eq!(pages, oracle_pages);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

pub(crate) fn projected_pipeline(
    root: &BenchStoreRoot,
    label: &str,
    case: &BenchmarkCase,
) -> Measurement {
    let mut fixture = seeded_replay(root, label, case, true);
    let limit = scan_limit(case);
    validate_raw(
        &fixture.input,
        &mut fixture.transactions,
        &case.workload.encoded,
        limit,
    );
    let output = fixture.output.as_ref().expect("pipeline output exists");
    let cursor = fixture.cursor.as_ref().expect("pipeline cursor exists");

    let started = Instant::now();
    let mut next = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    loop {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin pipeline page transaction");
        let input_access = fixture
            .input
            .access(transaction.access())
            .expect("access pipeline input");
        let mut output_access = output
            .access(transaction.access())
            .expect("access pipeline output");
        let mut cursor_access = cursor
            .access(transaction.access())
            .expect("access pipeline cursor");
        let progress = input_access
            .scan(next, limit, |entry| {
                let index = usize::try_from(entry.offset()).expect("entry offset fits usize");
                let projected = entry.project(|encoded| {
                    decode_projected_entry(encoded, &case.selected_projections[index])
                })?;
                let encoded = encode_change(&projected)
                    .map_err(|error| CodecError::new(error.to_string()))?;
                let output_offset = output_access.append(&encoded)?;
                checksum = mix(checksum, entry.offset());
                checksum = mix(checksum, output_offset);
                checksum = mix(
                    checksum,
                    u64::try_from(encoded.len()).expect("encoded length fits u64"),
                );
                black_box(projected);
                Ok::<(), StoreError>(())
            })
            .expect("run projected replay page");
        cursor_access
            .set(&progress.next_offset)
            .expect("advance durable replay cursor");
        transaction
            .commit()
            .expect("durably commit projected replay page");
        pages += 1;
        assert!(
            progress.next_offset > next || progress.caught_up,
            "pipeline scan must make progress"
        );
        next = progress.next_offset;
        if progress.caught_up {
            break;
        }
    }
    let elapsed = started.elapsed();
    assert!(pages >= 2, "pipeline replay must span multiple pages");
    drop(fixture.transactions);
    let oracle_pages = validate_reopened_pipeline(fixture.sample.path(), case, limit);
    assert_eq!(pages, oracle_pages);
    Measurement {
        elapsed,
        pages,
        checksum,
    }
}

fn scan_decoded(
    access: &dogpaddle_store::AppendLogAccess<'_, Vec<u8>>,
    case: &BenchmarkCase,
    mode: ReplayMode,
    limit: ScanLimit,
) -> (usize, u64) {
    let mut next = 0_u64;
    let mut pages = 0_usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    loop {
        let progress = access
            .scan(next, limit, |entry| {
                let index = usize::try_from(entry.offset()).expect("entry offset fits usize");
                let change = match mode {
                    ReplayMode::Full => entry.project(decode_entry)?,
                    ReplayMode::Selected => entry.project(|encoded| {
                        decode_projected_entry(encoded, &case.selected_projections[index])
                    })?,
                };
                checksum = mix(checksum, entry.offset());
                checksum = mix(
                    checksum,
                    u64::try_from(change.num_rows()).expect("row count fits u64"),
                );
                checksum = mix(
                    checksum,
                    u64::try_from(change.records().num_columns()).expect("column count fits u64"),
                );
                black_box(change);
                Ok::<(), StoreError>(())
            })
            .expect("scan decoded benchmark page");
        pages += 1;
        assert!(
            progress.next_offset > next || progress.caught_up,
            "decoded scan must make progress"
        );
        next = progress.next_offset;
        if progress.caught_up {
            break;
        }
    }
    (pages, checksum)
}

fn transaction_range(case: &BenchmarkCase, transaction: usize) -> std::ops::Range<usize> {
    let start = transaction
        .checked_mul(case.metadata.changes_per_transaction)
        .expect("transaction start fits usize");
    let end = start
        .checked_add(case.metadata.changes_per_transaction)
        .expect("transaction end fits usize");
    start..end
}

fn scan_limit(case: &BenchmarkCase) -> ScanLimit {
    ScanLimit::new(case.metadata.page_max_items, case.metadata.page_max_bytes)
        .expect("valid benchmark replay page limit")
}

fn mix(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(0x0000_0100_0000_01b3)
}
