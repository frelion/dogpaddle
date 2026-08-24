use dogpaddle_operation::{
    CountData, CountDefinition, CountOperation, SequenceSourceData, SequenceSourceDefinition,
    SequenceSourceOperation,
};
use dogpaddle_store::{Cell, OrderedMap};

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stage instances are consumed by the next run phase"
    )
)]
pub(crate) struct Stage {
    state: OrderedMap<Vec<u8>, Vec<u8>>,
    operation: OperationInstance,
}

#[expect(
    dead_code,
    reason = "operation instances are consumed by the next run phase"
)]
enum OperationInstance {
    SequenceSource(SequenceSourceOperation),
    Count(CountOperation),
}

impl Stage {
    pub(crate) fn sequence_source(
        state: OrderedMap<Vec<u8>, Vec<u8>>,
        definition: SequenceSourceDefinition,
        position: Cell<u64>,
    ) -> Self {
        Self {
            state,
            operation: OperationInstance::SequenceSource(SequenceSourceOperation::new(
                definition,
                SequenceSourceData::new(position),
            )),
        }
    }

    pub(crate) fn count(
        state: OrderedMap<Vec<u8>, Vec<u8>>,
        definition: CountDefinition,
        count: Cell<u64>,
    ) -> Self {
        Self {
            state,
            operation: OperationInstance::Count(CountOperation::new(
                definition,
                CountData::new(count),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
