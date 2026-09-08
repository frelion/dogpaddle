use crate::{
    Cell, DataHandle, DataKind, OrderedMap, OrderedMultiset, PartitionedMultiset, Queue, StoreKey,
    StoreValue, SubscribedLog,
};

/// A typed persistent data object that can be created and opened by [`crate::Store`].
///
/// This trait is sealed. Each built-in collection records its stable kind in
/// the Store catalog.
pub trait StoreData: private::SealedStoreData {}

pub(crate) mod private {
    use crate::{DataHandle, DataKind};

    pub trait SealedStoreData: Sized {
        const KIND: DataKind;

        fn from_handle(data: DataHandle) -> Self;
    }
}

macro_rules! impl_store_data {
    ($data:ty, $kind:expr; $($bounds:tt)*) => {
        impl<$($bounds)*> private::SealedStoreData for $data {
            const KIND: DataKind = $kind;

            fn from_handle(data: DataHandle) -> Self {
                Self::from_handle(data)
            }
        }

        impl<$($bounds)*> StoreData for $data {}
    };
}

impl_store_data!(Cell<T>, DataKind::Cell; T: StoreValue);
impl_store_data!(Queue<T>, DataKind::Queue; T: StoreValue);
impl_store_data!(SubscribedLog<T>, DataKind::SubscribedLog; T: StoreValue);
impl_store_data!(OrderedMultiset<K>, DataKind::OrderedMultiset; K: StoreKey);
impl_store_data!(
    PartitionedMultiset<P, K>,
    DataKind::PartitionedMultiset;
    P: StoreKey, K: StoreKey
);
impl_store_data!(
    OrderedMap<K, V>,
    DataKind::OrderedMap;
    K: StoreKey, V: StoreValue
);

pub(crate) fn kind<D: StoreData>() -> DataKind {
    <D as private::SealedStoreData>::KIND
}

pub(crate) fn from_handle<D: StoreData>(data: DataHandle) -> D {
    <D as private::SealedStoreData>::from_handle(data)
}
