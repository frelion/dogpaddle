use std::{hint::black_box, time::Duration};

use dogpaddle_store::{
    CodecError, OrderedMap, ScanDirection, ScanLimit, StoreData, StoreError, Transactions,
};

use crate::{
    RANDOM_SEED, STATION_KEYS, VALUE_BYTES,
    fixture::{ByteMap, Fixture, ScanFixture, StationFixture, TypedMap},
    oracle::{
        assert_station_map, expected_scan_checksum, expected_station_first_bytes,
        read_station_cursor,
    },
};

pub(super) fn measure_byte_map_bulk_put<SIZE>(entries: usize) -> Duration
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    let mut fixture = Fixture::<SIZE>::empty();
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map write transaction");
    {
        let mut bytes = fixture
            .bytes
            .access(transaction.access())
            .expect("access byte map");
        for key in 0..entries {
            bytes
                .put(&(key as u64).to_be_bytes().to_vec(), &value)
                .expect("write byte map benchmark item");
        }
    }
    transaction
        .commit()
        .expect("commit byte map benchmark writes");
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map write validation transaction");
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map for write validation");
    let first = 0_u64.to_be_bytes().to_vec();
    let last = u64::try_from(entries - 1)
        .expect("entry count fits u64")
        .to_be_bytes()
        .to_vec();
    assert_eq!(
        bytes.get(&first).unwrap().as_deref(),
        Some(value.as_slice())
    );
    assert_eq!(bytes.get(&last).unwrap().as_deref(), Some(value.as_slice()));
    transaction
        .commit()
        .expect("finish byte map write validation transaction");
    elapsed
}

pub(super) fn measure_bulk_put<SIZE>(entries: usize) -> Duration
where
    ByteMap<SIZE>: StoreData,
    TypedMap<SIZE>: StoreData,
{
    let mut fixture = Fixture::<SIZE>::empty();
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin write transaction");
    {
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access write map");
        for key in 0..entries {
            map.put(&(key as u64), &value)
                .expect("write benchmark item");
        }
    }
    transaction.commit().expect("commit benchmark writes");
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin map write validation transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access map for write validation");
    assert_eq!(map.get(&0).unwrap().as_deref(), Some(value.as_slice()));
    assert_eq!(
        map.get(&u64::try_from(entries - 1).expect("entry count fits u64"))
            .unwrap()
            .as_deref(),
        Some(value.as_slice())
    );
    transaction
        .commit()
        .expect("finish map write validation transaction");
    elapsed
}

pub(super) fn measure_point_get<SIZE>(fixture: &mut Fixture<SIZE>, entries: usize) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin read transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access read map");
    let mut state = RANDOM_SEED;
    let mut checksum = 0_usize;
    for _ in 0..entries {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = state % entries as u64;
        let value = map.get(&key).expect("read benchmark item").unwrap();
        checksum = checksum.wrapping_add(usize::from(value[0]));
    }
    black_box(checksum);
    transaction.commit().expect("finish read transaction");
    let elapsed = started.elapsed();
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

pub(super) fn measure_byte_map_point_get<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map read transaction");
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map");
    let mut state = RANDOM_SEED;
    let mut checksum = 0_usize;
    for _ in 0..entries {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = (state % entries as u64).to_be_bytes().to_vec();
        let value = bytes.get(&key).expect("read byte map item").unwrap();
        checksum = checksum.wrapping_add(usize::from(value[0]));
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish byte map read transaction");
    let elapsed = started.elapsed();
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

pub(super) fn measure_byte_map_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin byte map scan transaction");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let bytes = fixture
        .bytes
        .access(transaction.access())
        .expect("access byte map scan");
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    loop {
        let next = bytes
            .scan(.., direction, continuation.as_ref(), limit, |entry| {
                let (_, value) = entry.decode_owned()?;
                count += 1;
                checksum = checksum.wrapping_add(usize::from(value[0]));
                Ok::<(), StoreError>(())
            })
            .expect("scan byte map benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish byte map scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

pub(super) fn measure_scan<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    direction: ScanDirection,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin scan transaction");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut count = 0_usize;
    let mut checksum = 0_usize;
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access scan map");
    let mut continuation = None;
    loop {
        let next = map
            .scan(.., direction, continuation.as_ref(), limit, |entry| {
                let (_, value) = entry.decode_owned()?;
                count += 1;
                checksum = checksum.wrapping_add(usize::from(value[0]));
                Ok::<(), StoreError>(())
            })
            .expect("scan benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction.commit().expect("finish scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, entries.checked_mul(0x5a).unwrap());
    elapsed
}

pub(super) fn measure_primitive_scan<SIZE>(
    fixture: &mut ScanFixture<u64, SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin primitive scan transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access primitive scan map");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_u64;
    loop {
        let next = map
            .scan(
                ..,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
                |entry| {
                    let (key, value) = entry.decode_owned()?;
                    count += 1;
                    checksum = checksum.wrapping_add(key ^ value);
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan primitive benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish primitive scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, expected_scan_checksum(entries, 0x5a));
    elapsed
}

pub(super) fn measure_vec_scan<SIZE>(
    transactions: &mut Transactions,
    map: &OrderedMap<u64, Vec<u8>, SIZE>,
    entries: usize,
    scan_items: usize,
    scan_bytes: usize,
    project: bool,
) -> Duration {
    let started = std::time::Instant::now();
    let transaction = transactions.begin().expect("begin vector scan transaction");
    let map = map
        .access(transaction.access())
        .expect("access vector scan map");
    let limit = ScanLimit::new(scan_items, scan_bytes).unwrap();
    let mut continuation = None;
    let mut count = 0_usize;
    let mut checksum = 0_u64;
    loop {
        let next = map
            .scan(
                ..,
                ScanDirection::Ascending,
                continuation.as_ref(),
                limit,
                |entry| {
                    let value = if project {
                        entry.project(project_vec_checksum)?
                    } else {
                        let (key, value) = entry.decode_owned()?;
                        key ^ u64::from(value[0])
                    };
                    count += 1;
                    checksum = checksum.wrapping_add(value);
                    Ok::<(), StoreError>(())
                },
            )
            .expect("scan wide benchmark page");
        if let Some(next) = next {
            continuation = Some(next);
        } else {
            break;
        }
    }
    black_box(checksum);
    transaction
        .commit()
        .expect("finish vector scan transaction");
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, expected_scan_checksum(entries, 0x5a));
    elapsed
}

fn project_vec_checksum(key: &[u8], value: &[u8]) -> Result<u64, CodecError> {
    let key = u64::from_be_bytes(
        key.try_into()
            .map_err(|_| CodecError::new("invalid benchmark key"))?,
    );
    Ok(key ^ u64::from(value[0]))
}

pub(super) fn measure_station_steps<SIZE>(
    fixture: &mut StationFixture<SIZE>,
    steps: usize,
    operations_per_step: usize,
) -> Duration
where
    TypedMap<SIZE>: StoreData,
{
    let operations_per_step_u64 =
        u64::try_from(operations_per_step).expect("station batch size fits in u64");
    let station_keys = u64::try_from(STATION_KEYS).expect("station key count fits in u64");
    let initial_cursor = read_station_cursor(fixture);
    let expected_first_bytes =
        expected_station_first_bytes(initial_cursor, steps, operations_per_step_u64, station_keys);
    let started = std::time::Instant::now();
    for _ in 0..steps {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin station transaction");
        let cursor = fixture
            .cursor
            .access(transaction.access())
            .expect("access station cursor")
            .get()
            .expect("read station cursor")
            .expect("seeded station cursor");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access station map");
            for offset in 0..operations_per_step_u64 {
                let key = cursor
                    .wrapping_mul(operations_per_step_u64)
                    .wrapping_add(offset)
                    % station_keys;
                let mut value = map
                    .get(&key)
                    .expect("read station item")
                    .expect("seeded station item");
                value[0] = value[0].wrapping_add(1);
                map.put(&key, &value).expect("write station item");
            }
        }
        fixture
            .cursor
            .access(transaction.access())
            .expect("access station cursor")
            .set(&cursor.wrapping_add(1))
            .expect("advance station cursor");
        transaction.commit().expect("commit station transaction");
    }
    let elapsed = started.elapsed();
    assert_eq!(
        read_station_cursor(fixture),
        initial_cursor.wrapping_add(u64::try_from(steps).expect("step count fits u64"))
    );
    assert_station_map(fixture, &expected_first_bytes);
    elapsed
}

pub(super) fn measure_hot_overwrite_rollback<SIZE>(
    fixture: &mut Fixture<SIZE>,
    entries: usize,
) -> Duration {
    let value = vec![0xa5; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin overwrite transaction");
    {
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access overwrite map");
        for key in 0..entries {
            map.put(&(key as u64), &value)
                .expect("overwrite benchmark item");
        }
    }
    drop(transaction);
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin rollback validation transaction");
    let value = fixture
        .map
        .access(transaction.access())
        .expect("access map for rollback validation")
        .get(&0)
        .expect("read rollback validation value")
        .expect("seeded rollback validation value");
    assert_eq!(value[0], 0x5a);
    transaction
        .commit()
        .expect("finish rollback validation transaction");
    elapsed
}

pub(super) fn measure_single_put_commits<SIZE>(
    fixture: &mut Fixture<SIZE>,
    commits: usize,
) -> Duration {
    let started = std::time::Instant::now();
    let mut encoded = vec![0x5a; VALUE_BYTES];
    for value in 0..commits {
        encoded[..std::mem::size_of::<u64>()].copy_from_slice(&(value as u64).to_be_bytes());
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin single-put transaction");
        fixture
            .map
            .access(transaction.access())
            .expect("access single-put map")
            .put(&0, &encoded)
            .expect("write single-put value");
        transaction.commit().expect("commit single-put transaction");
    }
    let elapsed = started.elapsed();
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin durable overwrite validation transaction");
    let actual = fixture
        .map
        .access(transaction.access())
        .expect("access durable overwrite validation map")
        .get(&0)
        .expect("read durable overwrite validation value")
        .expect("durable overwrite value exists");
    assert_eq!(
        &actual[..size_of::<u64>()],
        &u64::try_from(commits - 1)
            .expect("commit count fits u64")
            .to_be_bytes()
    );
    transaction
        .commit()
        .expect("finish durable overwrite validation transaction");
    elapsed
}
