use dogpaddle_store::StoreData;

use crate::{
    STAGE_KEYS, VALUE_BYTES,
    fixture::{StageFixture, TypedMap},
};

pub(super) fn read_stage_cursor<SIZE>(fixture: &mut StageFixture<SIZE>) -> u64
where
    TypedMap<SIZE>: StoreData,
{
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage cursor validation transaction");
    let cursor = fixture
        .cursor
        .access(transaction.access())
        .expect("access stage cursor for validation")
        .get()
        .expect("read stage cursor for validation")
        .expect("seeded stage cursor exists");
    transaction
        .commit()
        .expect("finish stage cursor validation transaction");
    cursor
}

pub(super) fn expected_stage_first_bytes(
    initial_cursor: u64,
    steps: usize,
    operations_per_step: u64,
    stage_keys: u64,
) -> Vec<u8> {
    let mut expected = vec![0x5a_u8; STAGE_KEYS];
    for step in 0..steps {
        let cursor =
            initial_cursor.wrapping_add(u64::try_from(step).expect("stage step fits in u64"));
        for offset in 0..operations_per_step {
            let key = cursor
                .wrapping_mul(operations_per_step)
                .wrapping_add(offset)
                % stage_keys;
            let key = usize::try_from(key).expect("stage key fits in usize");
            expected[key] = expected[key].wrapping_add(1);
        }
    }
    expected
}

pub(super) fn assert_stage_map<SIZE>(fixture: &mut StageFixture<SIZE>, expected_first_bytes: &[u8])
where
    TypedMap<SIZE>: StoreData,
{
    assert_eq!(expected_first_bytes.len(), STAGE_KEYS);
    let transaction = fixture
        .transactions
        .begin()
        .expect("begin stage map validation transaction");
    let map = fixture
        .map
        .access(transaction.access())
        .expect("access stage map for validation");
    for (key, expected_first) in expected_first_bytes.iter().copied().enumerate() {
        let value = map
            .get(&u64::try_from(key).expect("stage validation key fits in u64"))
            .expect("read stage map validation value")
            .expect("seeded stage map value exists");
        assert_eq!(value.len(), VALUE_BYTES);
        assert_eq!(value[0], expected_first);
        assert!(value[1..].iter().all(|byte| *byte == 0x5a));
    }
    transaction
        .commit()
        .expect("finish stage map validation transaction");
}

pub(super) fn expected_scan_checksum(entries: usize, value: u64) -> u64 {
    (0..entries).fold(0_u64, |checksum, key| {
        checksum.wrapping_add(u64::try_from(key).expect("benchmark key fits u64") ^ value)
    })
}
