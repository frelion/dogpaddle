use std::{hint::black_box, time::Duration};

use dogpaddle_store::{CodecError, ScanDirection, ScanLimit, StoreError};

use crate::{
    RANDOM_SEED, STATION_KEYS, VALUE_BYTES,
    fixture::{MapFixture, StationFixture},
};

#[derive(Clone, Copy)]
pub(super) enum EntryRead {
    Owned,
    Projected,
}

pub(super) fn measure_bulk_put(fixture: &mut MapFixture, entries: usize) -> Duration {
    let value = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    let transaction = fixture.writes.begin();
    {
        let mut map = fixture
            .map
            .access(transaction.access())
            .expect("access bulk-put map");
        for key in 0..u64::try_from(entries).expect("entry count fits u64") {
            map.put(&key, &value).expect("write benchmark entry");
        }
    }
    transaction.commit().expect("commit bulk put");
    let elapsed = started.elapsed();

    let snapshot = fixture.reads.begin();
    let map = fixture
        .map
        .read(snapshot.access())
        .expect("read bulk-put map");
    assert_eq!(map.get(&0).unwrap().as_deref(), Some(value.as_slice()));
    assert_eq!(
        map.get(&u64::try_from(entries - 1).expect("entry count fits u64"))
            .unwrap()
            .as_deref(),
        Some(value.as_slice())
    );
    elapsed
}

pub(super) fn measure_point_get(fixture: &MapFixture, operations: usize) -> Duration {
    let started = std::time::Instant::now();
    let checksum = {
        let snapshot = fixture.reads.begin();
        let map = fixture
            .map
            .read(snapshot.access())
            .expect("read point-get map");
        let key_count = u64::try_from(operations).expect("operation count fits u64");
        let mut state = RANDOM_SEED;
        let mut checksum = 0_usize;
        for _ in 0..operations {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let value = map
                .get(&(state % key_count))
                .expect("read benchmark entry")
                .expect("seeded benchmark entry");
            checksum = checksum.wrapping_add(usize::from(value[0]));
        }
        checksum
    };
    black_box(checksum);
    let elapsed = started.elapsed();
    assert_eq!(checksum, operations.checked_mul(0x5a).unwrap());
    elapsed
}

pub(super) fn measure_scan(
    fixture: &MapFixture,
    entries: usize,
    direction: ScanDirection,
    limit: ScanLimit,
    read: EntryRead,
) -> Duration {
    let started = std::time::Instant::now();
    let (count, checksum) = {
        let snapshot = fixture.reads.begin();
        let map = fixture.map.read(snapshot.access()).expect("read scan map");
        let mut continuation = None;
        let mut count = 0_usize;
        let mut checksum = 0_u64;
        loop {
            let next = map
                .scan(.., direction, continuation.as_ref(), limit, |entry| {
                    let value = match read {
                        EntryRead::Owned => {
                            let (key, value) = entry.decode_owned()?;
                            key ^ u64::from(value[0])
                        }
                        EntryRead::Projected => entry.project(project_checksum)?,
                    };
                    count += 1;
                    checksum = checksum.wrapping_add(value);
                    Ok::<(), StoreError>(())
                })
                .expect("scan benchmark page");
            if let Some(next) = next {
                continuation = Some(next);
            } else {
                break;
            }
        }
        (count, checksum)
    };
    black_box(checksum);
    let elapsed = started.elapsed();
    assert_eq!(count, entries);
    assert_eq!(checksum, expected_scan_checksum(entries));
    elapsed
}

fn project_checksum(key: &[u8], value: &[u8]) -> Result<u64, CodecError> {
    let key = u64::from_be_bytes(
        key.try_into()
            .map_err(|_| CodecError::new("invalid benchmark key"))?,
    );
    Ok(key ^ u64::from(value[0]))
}

fn expected_scan_checksum(entries: usize) -> u64 {
    (0..entries).fold(0_u64, |checksum, key| {
        checksum
            .wrapping_add(u64::try_from(key).expect("benchmark key fits u64") ^ u64::from(0x5a_u8))
    })
}

pub(super) fn measure_station_steps(
    fixture: &mut StationFixture,
    steps: usize,
    operations_per_step: usize,
) -> Duration {
    let operations_per_step =
        u64::try_from(operations_per_step).expect("station batch size fits u64");
    let station_keys = u64::try_from(STATION_KEYS).expect("station key count fits u64");
    let initial_step = read_station_step(fixture);
    let expected = expected_station_values(initial_step, steps, operations_per_step, station_keys);

    let started = std::time::Instant::now();
    for _ in 0..steps {
        let transaction = fixture.writes.begin();
        let step = fixture
            .step
            .access(transaction.access())
            .expect("access station step")
            .get()
            .expect("read station step")
            .expect("seeded station step");
        {
            let mut map = fixture
                .map
                .access(transaction.access())
                .expect("access station map");
            for offset in 0..operations_per_step {
                let key =
                    step.wrapping_mul(operations_per_step).wrapping_add(offset) % station_keys;
                let mut value = map
                    .get(&key)
                    .expect("read station entry")
                    .expect("seeded station entry");
                value[0] = value[0].wrapping_add(1);
                map.put(&key, &value).expect("write station entry");
            }
        }
        fixture
            .step
            .access(transaction.access())
            .expect("access station step")
            .set(&step.wrapping_add(1))
            .expect("advance station step");
        transaction.commit().expect("commit station step");
    }
    let elapsed = started.elapsed();

    assert_eq!(
        read_station_step(fixture),
        initial_step.wrapping_add(u64::try_from(steps).expect("step count fits u64"))
    );
    assert_station_map(fixture, &expected);
    elapsed
}

fn read_station_step(fixture: &StationFixture) -> u64 {
    let snapshot = fixture.reads.begin();
    fixture
        .step
        .read(snapshot.access())
        .expect("read station step")
        .get()
        .expect("decode station step")
        .expect("seeded station step")
}

fn expected_station_values(
    initial_step: u64,
    steps: usize,
    operations_per_step: u64,
    station_keys: u64,
) -> Vec<u8> {
    let mut expected = vec![0x5a_u8; STATION_KEYS];
    for step in 0..steps {
        let step = initial_step.wrapping_add(u64::try_from(step).expect("station step fits u64"));
        for offset in 0..operations_per_step {
            let key = step.wrapping_mul(operations_per_step).wrapping_add(offset) % station_keys;
            let key = usize::try_from(key).expect("station key fits usize");
            expected[key] = expected[key].wrapping_add(1);
        }
    }
    expected
}

fn assert_station_map(fixture: &StationFixture, expected: &[u8]) {
    let snapshot = fixture.reads.begin();
    let map = fixture
        .map
        .read(snapshot.access())
        .expect("read station map");
    for (key, expected_first) in expected
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, expected_first)| *expected_first != 0x5a)
    {
        let value = map
            .get(&u64::try_from(key).expect("station key fits u64"))
            .expect("read station entry")
            .expect("seeded station entry");
        assert_eq!(value.len(), VALUE_BYTES);
        assert_eq!(value[0], expected_first);
        assert!(value[1..].iter().all(|byte| *byte == 0x5a));
    }
}

pub(super) fn measure_single_put_commits(fixture: &mut MapFixture, commits: usize) -> Duration {
    let mut encoded = vec![0x5a; VALUE_BYTES];
    let started = std::time::Instant::now();
    for value in 0..commits {
        encoded[..size_of::<u64>()].copy_from_slice(
            &u64::try_from(value)
                .expect("commit ordinal fits u64")
                .to_be_bytes(),
        );
        let transaction = fixture.writes.begin();
        fixture
            .map
            .access(transaction.access())
            .expect("access hot map")
            .put(&0, &encoded)
            .expect("overwrite hot entry");
        transaction.commit().expect("commit hot overwrite");
    }
    let elapsed = started.elapsed();

    let snapshot = fixture.reads.begin();
    let actual = fixture
        .map
        .read(snapshot.access())
        .expect("read hot map")
        .get(&0)
        .expect("read hot entry")
        .expect("hot entry exists");
    assert_eq!(
        &actual[..size_of::<u64>()],
        &u64::try_from(commits - 1)
            .expect("commit count fits u64")
            .to_be_bytes()
    );
    elapsed
}
