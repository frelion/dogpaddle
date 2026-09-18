//! White-box coverage for the aggregate's durable layout.
//!
//! A dead group's leftover keys are unreachable through the public API because
//! group IDs are never reused, so reclaiming them can only be observed here.

use dogpaddle_store::Store;

use crate::definition::{DataInstances, DataName};

use super::{
    runtime::drain_group_entries,
    state::{Entries, EntryPartition},
};

const ENTRIES: DataName<Entries> = DataName::new("aggregate.entries");

#[test]
fn draining_a_dead_group_removes_only_its_own_partitions() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let mut data = DataInstances::new();
    data.insert(ENTRIES.declaration().create(&mut store, "entries").unwrap())
        .unwrap();
    let entries: Entries = data.take(&ENTRIES).unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        let mut access = entries.access(transaction.access()).unwrap();
        access
            .partition(&EntryPartition::new(0, 4))
            .unwrap()
            .adjust(&b"dead".to_vec(), 3)
            .unwrap();
        access
            .partition(&EntryPartition::new(1, 4))
            .unwrap()
            .adjust(&b"dead".to_vec(), 1)
            .unwrap();
        access
            .partition(&EntryPartition::new(0, 9))
            .unwrap()
            .adjust(&b"other".to_vec(), 2)
            .unwrap();
        transaction.commit().unwrap();
    }

    {
        let transaction = transactions.begin();
        let mut access = entries.access(transaction.access()).unwrap();
        drain_group_entries(2, &mut access, 4).unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin();
    let mut access = entries.access(transaction.access()).unwrap();
    for layout in 0..2 {
        let partition = access.partition(&EntryPartition::new(layout, 4)).unwrap();
        assert!(partition.first().unwrap().is_none());
        assert!(partition.last().unwrap().is_none());
    }
    assert_eq!(
        access
            .partition(&EntryPartition::new(0, 9))
            .unwrap()
            .multiplicity(&b"other".to_vec())
            .unwrap(),
        2
    );
}
