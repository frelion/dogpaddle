use std::{collections::BTreeMap, fmt::Debug};

use dogpaddle_store::DataHandle;
use thiserror::Error;

use crate::operation::Operation;

mod private {
    pub trait Sealed {}
}

pub(crate) use private::Sealed;

/// Named data handles provisioned by Flow for one operation.
///
/// Bindings are resolved by stable logical name rather than declaration order.
/// Concrete definitions provide the typed collection constructor when consuming
/// each name, so raw [`DataHandle`] values do not cross the materialization API.
#[doc(hidden)]
pub struct DataBindings {
    remaining: BTreeMap<&'static str, DataHandle>,
}

/// Pure definition shared by every built-in operation.
///
/// The trait is sealed: the persistent operation set is closed inside this
/// crate. Flow uses the hidden methods to provision the definition's declared
/// data handles before asking it to materialize a runtime [`Operation`].
pub trait OperationDefinition: private::Sealed + Debug + Send + Sync + 'static {
    /// Returns the exact number of ordered upstream stages this definition accepts.
    fn input_count(&self) -> usize;

    /// Returns stable logical data names in deterministic provisioning order.
    ///
    /// Flow prefixes these names with the owning stage's stable resource path.
    /// Names, collection types, and codecs form part of the operation's persistent
    /// schema. Materialization resolves bindings by name, not by this slice's order.
    #[doc(hidden)]
    fn data_names(&self) -> &'static [&'static str];

    /// Materializes a runtime operation from named, typed data bindings.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializeError`] when the supplied handles do not match the
    /// definition's declared data shape.
    #[doc(hidden)]
    fn materialize(&self, data: &mut DataBindings) -> Result<Box<dyn Operation>, MaterializeError>;

    /// Returns this definition's stable persistent tag.
    #[doc(hidden)]
    fn persistence_tag(&self) -> u16;

    /// Appends this definition's variant-specific persistent payload.
    #[doc(hidden)]
    fn encode_payload(&self, output: &mut Vec<u8>);
}

impl DataBindings {
    /// Creates an empty binding set.
    #[doc(hidden)]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            remaining: BTreeMap::new(),
        }
    }

    /// Adds one provisioned handle under its stable logical name.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializeError::DuplicateData`] when the name is already bound.
    #[doc(hidden)]
    pub fn insert(
        &mut self,
        name: &'static str,
        handle: DataHandle,
    ) -> Result<(), MaterializeError> {
        if self.remaining.contains_key(name) {
            return Err(MaterializeError::DuplicateData { name });
        }
        self.remaining.insert(name, handle);
        Ok(())
    }

    /// Verifies that the operation consumed every provisioned binding.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializeError::UnexpectedData`] for the first unconsumed name.
    #[doc(hidden)]
    pub fn finish(self) -> Result<(), MaterializeError> {
        self.remaining.into_keys().next().map_or(Ok(()), |name| {
            Err(MaterializeError::UnexpectedData { name })
        })
    }

    pub(crate) fn take<T>(
        &mut self,
        name: &'static str,
        bind: fn(DataHandle) -> T,
    ) -> Result<T, MaterializeError> {
        let handle = self
            .remaining
            .remove(name)
            .ok_or(MaterializeError::MissingData { name })?;
        Ok(bind(handle))
    }
}

/// Failure while binding provisioned data handles to an operation definition.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaterializeError {
    /// Flow attempted to bind the same logical resource more than once.
    #[error("operation data {name:?} was bound more than once")]
    DuplicateData {
        /// Stable logical resource name.
        name: &'static str,
    },
    /// The definition requested a logical resource that Flow did not bind.
    #[error("operation data {name:?} was not bound")]
    MissingData {
        /// Stable logical resource name.
        name: &'static str,
    },
    /// The definition did not consume one of its declared logical resources.
    #[error("operation data {name:?} was not consumed")]
    UnexpectedData {
        /// Stable logical resource name.
        name: &'static str,
    },
}
