use crate::{Cell, DataHandle, DataPlacement, Large, OrderedMap, Small, StoreKey, StoreValue};

/// A typed persistent data object that can be created and opened by [`crate::Store`].
///
/// This trait is sealed. Each built-in collection fixes or explicitly selects
/// the physical placement appropriate for its semantics.
pub trait StoreData: private::SealedStoreData {}

pub(crate) mod private {
    use crate::{DataHandle, DataPlacement};

    pub trait SealedStoreData: Sized {
        const PLACEMENT: DataPlacement;

        fn from_handle(data: DataHandle) -> Self;
    }
}

macro_rules! impl_store_data {
    ($data:ty, $placement:expr; $($bounds:tt)*) => {
        impl<$($bounds)*> private::SealedStoreData for $data {
            const PLACEMENT: DataPlacement = $placement;

            fn from_handle(data: DataHandle) -> Self {
                Self::from_handle(data)
            }
        }

        impl<$($bounds)*> StoreData for $data {}
    };
}

impl_store_data!(Cell<T>, DataPlacement::Shared; T: StoreValue);
impl_store_data!(
    OrderedMap<K, V, Small>,
    DataPlacement::Shared;
    K: StoreKey, V: StoreValue
);
impl_store_data!(
    OrderedMap<K, V, Large>,
    DataPlacement::Dedicated;
    K: StoreKey, V: StoreValue
);

pub(crate) fn placement<D: StoreData>() -> DataPlacement {
    <D as private::SealedStoreData>::PLACEMENT
}

pub(crate) fn from_handle<D: StoreData>(data: DataHandle) -> D {
    <D as private::SealedStoreData>::from_handle(data)
}
