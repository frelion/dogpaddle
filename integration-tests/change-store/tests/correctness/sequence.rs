use dogpaddle_change::{Change, encode_change};
use dogpaddle_change_store_integration::{Event, StoreFixture, flatten_narrow, narrow_change};
use dogpaddle_store::{AppendLog, Store};

use super::support::scan_changes;

#[test]
fn stable_rebatching_changes_only_physical_coordinates_not_event_order() {
    let fixture = StoreFixture::new();
    let expected = [
        Event { value: 7, diff: 1 },
        Event { value: 8, diff: 1 },
        Event { value: 7, diff: -1 },
        Event { value: 9, diff: 1 },
        Event { value: 9, diff: -1 },
    ];
    let three_two = [
        narrow_change(&[7, 8, 7], &[1, 1, -1]),
        narrow_change(&[9, 9], &[1, -1]),
    ];
    let one_four = [
        narrow_change(&[7], &[1]),
        narrow_change(&[8, 7, 9, 9], &[1, -1, 1, -1]),
    ];

    let mut store = Store::create(fixture.path()).unwrap();
    let left: AppendLog<Vec<u8>> = store.create_data("three-two").unwrap();
    let right: AppendLog<Vec<u8>> = store.create_data("one-four").unwrap();
    let left_encoded = three_two
        .iter()
        .map(|change| encode_change(change).unwrap())
        .collect::<Vec<_>>();
    let right_encoded = one_four
        .iter()
        .map(|change| encode_change(change).unwrap())
        .collect::<Vec<_>>();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        left.access(transaction.access())
            .unwrap()
            .append_batch(&left_encoded)
            .unwrap();
        right
            .access(transaction.access())
            .unwrap()
            .append_batch(&right_encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let left: AppendLog<Vec<u8>> = store.open_data("three-two").unwrap();
    let right: AppendLog<Vec<u8>> = store.open_data("one-four").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let left_access = left.access(transaction.access()).unwrap();
    let right_access = right.access(transaction.access()).unwrap();
    let left_bytes = left_encoded.iter().map(Vec::len).sum::<usize>() + 16;
    let right_bytes = right_encoded.iter().map(Vec::len).sum::<usize>() + 16;
    let left_scanned = scan_changes(&left_access, 0, 2, left_bytes);
    let right_scanned = scan_changes(&right_access, 0, 2, right_bytes);
    let left_changes = left_scanned
        .iter()
        .map(|(_, change)| change.clone())
        .collect::<Vec<_>>();
    let right_changes = right_scanned
        .iter()
        .map(|(_, change)| change.clone())
        .collect::<Vec<_>>();

    assert_eq!(flatten_narrow(&left_changes), expected);
    assert_eq!(flatten_narrow(&right_changes), expected);
    let left_coordinates = coordinates(&left_scanned);
    let right_coordinates = coordinates(&right_scanned);
    // Event 1 moves from row 1 of entry 0 to row 0 of entry 1. The physical
    // coordinate is traversal state, while the flattened event is invariant.
    assert_eq!(left_coordinates[1], (0, 1, expected[1]));
    assert_eq!(right_coordinates[1], (1, 0, expected[1]));
}

fn coordinates(changes: &[(u64, Change)]) -> Vec<(u64, usize, Event)> {
    changes
        .iter()
        .flat_map(|(offset, change)| {
            flatten_narrow(std::slice::from_ref(change))
                .into_iter()
                .enumerate()
                .map(|(row, event)| (*offset, row, event))
                .collect::<Vec<_>>()
        })
        .collect()
}
