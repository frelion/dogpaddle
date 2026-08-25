use std::{collections::BTreeMap, ops::Bound};

use dogpaddle_store::{
    Large, OrderedMap, OrderedMapAccess, ScanDirection, ScanLimit, Small, Store, StoreData,
    StoreError,
};

use crate::support::{create_map, store_path};

fn lower_allows(key: i64, bound: &Bound<i64>) -> bool {
    match bound {
        Bound::Included(lower) => key >= *lower,
        Bound::Excluded(lower) => key > *lower,
        Bound::Unbounded => true,
    }
}

fn upper_allows(key: i64, bound: &Bound<i64>) -> bool {
    match bound {
        Bound::Included(upper) => key <= *upper,
        Bound::Excluded(upper) => key < *upper,
        Bound::Unbounded => true,
    }
}

fn expected(
    model: &BTreeMap<i64, String>,
    lower: &Bound<i64>,
    upper: &Bound<i64>,
    direction: ScanDirection,
) -> Vec<(i64, String)> {
    let mut items = model
        .iter()
        .filter(|(key, _)| lower_allows(**key, lower) && upper_allows(**key, upper))
        .map(|(key, value)| (*key, value.clone()))
        .collect::<Vec<_>>();
    if direction == ScanDirection::Descending {
        items.reverse();
    }
    items
}

fn collect_pages(
    access: &OrderedMapAccess<'_, i64, String>,
    lower: Bound<i64>,
    upper: Bound<i64>,
    direction: ScanDirection,
    max_items: usize,
    expected: &[(i64, String)],
) -> Vec<(i64, String)> {
    let mut items = Vec::new();
    let mut continuation = None;
    for _ in 0..100 {
        let batch = access
            .scan(
                (lower, upper),
                direction,
                continuation.as_ref(),
                ScanLimit::new(max_items, 4_096).unwrap(),
            )
            .unwrap();
        assert!(batch.items.len() <= max_items);
        assert_eq!(
            batch.items,
            expected[items.len()..items.len() + batch.items.len()]
        );
        let has_more = items.len() + batch.items.len() < expected.len();
        assert_eq!(batch.continuation.is_some(), has_more);

        if let Some(next) = batch.continuation {
            assert_eq!(batch.items.last().map(|(key, _)| key), Some(&next));
            assert!(!batch.items.is_empty());
            continuation = Some(next);
            items.extend(batch.items);
        } else {
            items.extend(batch.items);
            assert_eq!(items.len(), expected.len());
            return items;
        }
    }
    panic!("scan continuation did not reach EOF");
}

#[test]
fn every_range_direction_and_page_size_matches_a_btree_model() {
    assert_every_range_direction_and_page_size::<Small>();
    assert_every_range_direction_and_page_size::<Large>();
}

fn assert_every_range_direction_and_page_size<SIZE>()
where
    OrderedMap<i64, String, SIZE>: StoreData,
{
    let bounds = [
        Bound::Unbounded,
        Bound::Included(-10),
        Bound::Excluded(-4),
        Bound::Included(-1),
        Bound::Excluded(0),
        Bound::Included(4),
        Bound::Excluded(4),
        Bound::Included(10),
    ];
    let model = (-4..=4)
        .map(|key| (key, format!("value-{key}")))
        .collect::<BTreeMap<_, _>>();

    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<i64, String, SIZE>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(&transaction).unwrap();
        for (key, value) in &model {
            access.put(key, value).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(&transaction).unwrap();
    for lower in &bounds {
        for upper in &bounds {
            for direction in [ScanDirection::Ascending, ScanDirection::Descending] {
                let expected = expected(&model, lower, upper, direction);
                for max_items in 1..=4 {
                    assert_eq!(
                        collect_pages(&access, *lower, *upper, direction, max_items, &expected,),
                        expected,
                        "size={} lower={lower:?} upper={upper:?} direction={direction:?} max_items={max_items}",
                        std::any::type_name::<SIZE>(),
                    );
                }
            }
        }
    }
}

#[test]
fn byte_limits_and_continuations_are_exact() {
    assert_byte_limits_and_continuations::<Small>();
    assert_byte_limits_and_continuations::<Large>();
}

fn assert_byte_limits_and_continuations<SIZE>()
where
    OrderedMap<i64, String, SIZE>: StoreData,
{
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<i64, String, SIZE>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(&transaction).unwrap();
        for key in [-2, -1, 0] {
            access.put(&key, &format!("v{key}")).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(&transaction).unwrap();
    let first = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 22).unwrap(),
        )
        .unwrap();
    assert_eq!(first.items, vec![(-2, "v-2".into()), (-1, "v-1".into())]);
    assert_eq!(first.continuation, Some(-1));
    let second = access
        .scan(
            ..,
            ScanDirection::Ascending,
            first.continuation.as_ref(),
            ScanLimit::new(10, 22).unwrap(),
        )
        .unwrap();
    assert_eq!(second.items, vec![(0, "v0".into())]);
    assert_eq!(second.continuation, None);

    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 10).unwrap(),
        ),
        Err(StoreError::ItemTooLarge {
            size: 11,
            limit: 10
        })
    ));
}

#[test]
fn continuation_outside_the_range_returns_no_items() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let map = create_map::<i64, i64, Small>(&mut store, "map").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut access = map.access(&transaction).unwrap();
        for key in -2..=2 {
            access.put(&key, &key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(&transaction).unwrap();
    let limit = ScanLimit::new(10, 1_024).unwrap();
    let ascending = access
        .scan(
            (Bound::Included(-1), Bound::Included(1)),
            ScanDirection::Ascending,
            Some(&9),
            limit,
        )
        .unwrap();
    let descending = access
        .scan(
            (Bound::Included(-1), Bound::Included(1)),
            ScanDirection::Descending,
            Some(&-9),
            limit,
        )
        .unwrap();
    assert!(ascending.items.is_empty());
    assert_eq!(ascending.continuation, None);
    assert!(descending.items.is_empty());
    assert_eq!(descending.continuation, None);
}
