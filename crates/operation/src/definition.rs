use std::fmt::Debug;

use dogpaddle_store::DataHandle;
use thiserror::Error;

use crate::operation::Operation;

mod private {
    pub trait Sealed {}
}

pub(crate) use private::Sealed;

/// Pure definition shared by every built-in operation.
///
/// The trait is sealed: the persistent operation set is closed inside this
/// crate. Flow uses the hidden methods to provision the definition's declared
/// data handles before asking it to materialize a runtime [`Operation`].
pub trait OperationDefinition: private::Sealed + Debug + Send + Sync + 'static {
    /// Returns the exact number of ordered upstream stages this definition accepts.
    fn input_count(&self) -> usize;

    /// Returns stable logical data names in materialization order.
    ///
    /// Flow prefixes these names with the owning stage's stable resource path.
    /// Names, order, collection types, and codecs form part of the operation's
    /// persistent schema.
    #[doc(hidden)]
    fn data_names(&self) -> &'static [&'static str];

    /// Materializes a runtime operation from handles in [`Self::data_names`] order.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializeError`] when the supplied handles do not match the
    /// definition's declared data shape.
    #[doc(hidden)]
    fn materialize(&self, data: Vec<DataHandle>) -> Result<Box<dyn Operation>, MaterializeError>;

    /// Returns this definition's stable persistent tag.
    #[doc(hidden)]
    fn persistence_tag(&self) -> u16;

    /// Appends this definition's variant-specific persistent payload.
    #[doc(hidden)]
    fn encode_payload(&self, output: &mut Vec<u8>);
}

/// Failure while binding provisioned data handles to an operation definition.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaterializeError {
    /// The number of supplied handles differs from the declared data shape.
    #[error("operation requires {expected} data handles but received {actual}")]
    DataCount {
        /// Number of handles required by the definition.
        expected: usize,
        /// Number of handles supplied by Flow.
        actual: usize,
    },
}
