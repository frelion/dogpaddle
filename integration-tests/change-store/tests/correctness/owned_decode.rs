use std::num::NonZeroU64;

use dogpaddle_change_store_integration::{assert_change_eq, projectable_fixture};
use dogpaddle_store::{Store, SubscribedLog};

use super::support::{decode_entry, decode_projected_entry};

#[test]
fn subscription_payload_is_owned_beyond_its_read_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let expected = projectable_fixture(100, 4, 17);

    let mut store = Store::create(&path).unwrap();
    let log = store
        .create_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let writer = log.writer();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin();
        log.initialize(NonZeroU64::MIN, transaction.access())
            .unwrap();
        assert!(
            writer
                .try_append(&expected.encoded, NonZeroU64::MAX, transaction.access())
                .unwrap()
        );
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let log = store
        .open_data::<SubscribedLog<Vec<u8>>>("changes")
        .unwrap();
    let subscription = log.subscription(0);
    let encoded = {
        let snapshot = store.read_transaction();
        log.validate(NonZeroU64::MIN, snapshot.access()).unwrap();
        let (offset, encoded) = subscription.peek(snapshot.access()).unwrap().unwrap();
        assert_eq!(offset, 0);
        encoded
    };

    let full = decode_entry(&encoded).unwrap();
    let projected = decode_projected_entry(&encoded, &expected.projection).unwrap();
    assert_change_eq(&full, &expected.change);
    assert_change_eq(&projected, &expected.projected);
}
