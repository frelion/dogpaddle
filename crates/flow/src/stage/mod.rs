use dogpaddle_operation::operation::Operation;
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
    pub(crate) fn new(state: OrderedMap<Vec<u8>, Vec<u8>>, operation: Box<dyn Operation>) -> Self {
        Self { state, operation }
    }
}

#[cfg(test)]
mod tests;
