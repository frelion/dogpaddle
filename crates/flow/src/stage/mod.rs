use dogpaddle_operation::Operation;
use dogpaddle_store::OrderedMap;

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stage instances are consumed by the next run phase"
    )
)]
pub(crate) struct Stage {
    state: OrderedMap<Vec<u8>, Vec<u8>>,
    operation: Box<dyn Operation>,
}

impl Stage {
    pub(crate) fn new<O>(state: OrderedMap<Vec<u8>, Vec<u8>>, operation: O) -> Self
    where
        O: Operation,
    {
        Self {
            state,
            operation: Box::new(operation),
        }
    }
}

#[cfg(test)]
mod tests;
