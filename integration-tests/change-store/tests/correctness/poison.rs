use std::num::NonZeroU64;

use dogpaddle_change_store_integration::nested_change_fixture;
use dogpaddle_store::{Store, SubscribedLog};

#[test]
fn invalid_change_does_not_acknowledge_its_subscription_offset() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let fixture = nested_change_fixture(10, 2, 7);
    let valid = fixture.encoded;
    let corrupt = valid[..valid.len() - 1].to_vec();

    let mut store = Store::create(&path).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let producer = log.writer();
    let subscription = log.subscription(0);
    let (mut transactions, snapshots) = store.into_transactions().split();
    {
        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        assert!(
            producer
                .try_append(&valid, NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        assert!(
            producer
                .try_append(&corrupt, NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();
    }
    {
        let snapshot = snapshots.begin();
        let (offset, encoded) = subscription.peek(snapshot.access()).unwrap().unwrap();
        assert_eq!(offset, 0);
        fixture.codec.decode_owned(encoded).unwrap();
    }
    {
        let transaction = transactions.begin();
        subscription.acknowledge(0, transaction.access()).unwrap();
        transaction.commit().unwrap();
    }
    {
        let snapshot = snapshots.begin();
        let (offset, encoded) = subscription.peek(snapshot.access()).unwrap().unwrap();
        assert_eq!(offset, 1);
        assert!(fixture.codec.decode_owned(encoded).is_err());
    }
    drop((snapshots, transactions));

    let store = Store::open(&path).unwrap();
    let log = store
        .open_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let snapshot = store.read_transaction();
    log.validate(NonZeroU64::MIN, snapshot.access()).unwrap();
    let (offset, encoded) = log
        .subscription(0)
        .peek(snapshot.access())
        .unwrap()
        .unwrap();
    assert_eq!(offset, 1);
    assert!(fixture.codec.decode_owned(encoded).is_err());
}
