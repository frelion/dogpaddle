//! `DogPaddle`'s database-independent differential record model.
//!
//! A [`Change`] assigns a non-zero signed difference to one canonical
//! [`Record`]. Records contain a deliberately small set of [`Value`] variants;
//! database-specific types belong in adapters rather than this core model.

mod change;
mod codec;
mod record;
mod value;

pub use change::{Change, ChangeError};
pub use record::{MAX_NESTING_DEPTH, Record, RecordError};
pub use value::{CanonicalF64, Value};

#[cfg(test)]
mod tests;
