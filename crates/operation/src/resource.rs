use std::any::{Any, TypeId};

use crate::MaterializeError;

/// One optional, owned runtime resource for an Operation.
///
/// This value is never encoded into a Definition or stored in Store. A binding
/// checks its exact Rust type before assembly, then moves it into the Operation.
/// Resources may contain credentials; neither their values nor their `Debug`
/// representation are exposed by this wrapper.
#[derive(Default)]
pub struct RuntimeResource(Option<Box<dyn Any + Send>>);

impl RuntimeResource {
    /// Wraps one resource for transfer into its Operation.
    pub fn new<T: Send + 'static>(resource: T) -> Self {
        Self(Some(Box::new(resource)))
    }

    /// Supplies no runtime resource to a wholly self-contained Operation.
    #[must_use]
    pub const fn none() -> Self {
        Self(None)
    }

    pub(crate) fn validate(&self, expected: Option<TypeId>) -> Result<(), MaterializeError> {
        match (expected, self.0.as_deref()) {
            (None, None) => Ok(()),
            (Some(expected), Some(value)) if expected == value.type_id() => Ok(()),
            (Some(_), None) => Err(MaterializeError::MissingRuntimeResource),
            (Some(_), Some(_)) => Err(MaterializeError::WrongRuntimeResource),
            (None, Some(_)) => Err(MaterializeError::UnexpectedRuntimeResource),
        }
    }

    pub(crate) fn take<T: Send + 'static>(self) -> Result<T, MaterializeError> {
        self.0
            .ok_or(MaterializeError::MissingRuntimeResource)?
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| MaterializeError::WrongRuntimeResource)
    }
}
