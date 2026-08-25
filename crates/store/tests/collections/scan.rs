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
        let mut page = Vec::new();
        let next = access
            .scan(
                (lower, upper),
                direction,
                continuation.as_ref(),
                ScanLimit::new(max_items, 4_096).unwrap(),
                |entry| {
                    page.push(entry.decode_owned()?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(page.len() <= max_items);
        assert_eq!(page, expected[items.len()..items.len() + page.len()]);
        let has_more = items.len() + page.len() < expected.len();
        assert_eq!(next.is_some(), has_more);

        if let Some(next) = next {
            assert_eq!(page.last().map(|(key, _)| key), Some(&next));
            assert!(!page.is_empty());
            continuation = Some(next);
            items.extend(page);
        } else {
            items.extend(page);
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
        let mut access = map.access(transaction.access()).unwrap();
        for (key, value) in &model {
            access.put(key, value).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
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
        let mut access = map.access(transaction.access()).unwrap();
        for key in [-2, -1, 0] {
            access.put(&key, &format!("v{key}")).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    let mut first = Vec::new();
    let first_continuation = access
        .scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 22).unwrap(),
            |entry| {
                first.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(first, vec![(-2, "v-2".into()), (-1, "v-1".into())]);
    assert_eq!(first_continuation, Some(-1));
    let mut second = Vec::new();
    let second_continuation = access
        .scan(
            ..,
            ScanDirection::Ascending,
            first_continuation.as_ref(),
            ScanLimit::new(10, 22).unwrap(),
            |entry| {
                second.push(entry.decode_owned()?);
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(second, vec![(0, "v0".into())]);
    assert_eq!(second_continuation, None);

    assert!(matches!(
        access.scan(
            ..,
            ScanDirection::Ascending,
            None,
            ScanLimit::new(10, 10).unwrap(),
            |_| Ok::<(), StoreError>(()),
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
        let mut access = map.access(transaction.access()).unwrap();
        for key in -2..=2 {
            access.put(&key, &key).unwrap();
        }
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let access = map.access(transaction.access()).unwrap();
    let limit = ScanLimit::new(10, 1_024).unwrap();
    let mut visits = 0;
    let ascending = access
        .scan(
            (Bound::Included(-1), Bound::Included(1)),
            ScanDirection::Ascending,
            Some(&9),
            limit,
            |_| {
                visits += 1;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    let descending = access
        .scan(
            (Bound::Included(-1), Bound::Included(1)),
            ScanDirection::Descending,
            Some(&-9),
            limit,
            |_| {
                visits += 1;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert_eq!(visits, 0);
    assert_eq!(ascending, None);
    assert_eq!(descending, None);
}

#[test]
fn an_exact_page_stops_at_the_neighboring_small_namespace() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(store_path(&root)).unwrap();
    let first = create_map::<i64, i64, Small>(&mut store, "first").unwrap();
    let second = create_map::<i64, i64, Small>(&mut store, "second").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        first
            .access(transaction.access())
            .unwrap()
            .put(&1, &1)
            .unwrap();
        second
            .access(transaction.access())
            .unwrap()
            .put(&2, &2)
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let limit = ScanLimit::new(1, 1_024).unwrap();
    let mut first_items = Vec::new();
    let first_continuation = first
        .access(transaction.access())
        .unwrap()
        .scan(.., ScanDirection::Ascending, None, limit, |entry| {
            first_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    let mut second_items = Vec::new();
    let second_continuation = second
        .access(transaction.access())
        .unwrap()
        .scan(.., ScanDirection::Descending, None, limit, |entry| {
            second_items.push(entry.decode_owned()?);
            Ok::<(), StoreError>(())
        })
        .unwrap();

    assert_eq!(first_items, vec![(1, 1)]);
    assert_eq!(first_continuation, None);
    assert_eq!(second_items, vec![(2, 2)]);
    assert_eq!(second_continuation, None);
}
