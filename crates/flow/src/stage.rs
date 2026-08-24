use dogpaddle_operation::{
    CountData, CountOperation, OperationDefinition, SequenceSourceData, SequenceSourceOperation,
};
use dogpaddle_store::{Cell, DataHandle, DataPlacement, OrderedMap, Store, StoreError};

use crate::{FlowError, format};

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stage instances are consumed by the next run phase"
    )
)]
pub(crate) struct Stage {
    data: StageData,
    operation: OperationInstance,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "stage state is consumed by the next run phase")
)]
struct StageData {
    state: OrderedMap<Vec<u8>, Vec<u8>>,
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
    pub(crate) fn create(
        store: &mut Store,
        index: usize,
        definition: &OperationDefinition,
    ) -> Result<Self, FlowError> {
        let state = store.create_data(&format::stage_state_name(index), DataPlacement::Shared)?;
        Ok(Self {
            data: StageData {
                state: OrderedMap::new(state),
            },
            operation: OperationInstance::create(store, index, definition)?,
        })
    }

    pub(crate) fn open(
        store: &Store,
        index: usize,
        definition: &OperationDefinition,
    ) -> Result<Self, FlowError> {
        let state = open_required_data(store, &format::stage_state_name(index))?;
        Ok(Self {
            data: StageData {
                state: OrderedMap::new(state),
            },
            operation: OperationInstance::open(store, index, definition)?,
        })
    }
}

impl OperationInstance {
    fn create(
        store: &mut Store,
        index: usize,
        definition: &OperationDefinition,
    ) -> Result<Self, StoreError> {
        match definition {
            OperationDefinition::SequenceSource(definition) => {
                let position = Cell::new(store.create_data(
                    &format::sequence_position_name(index),
                    DataPlacement::Shared,
                )?);
                Ok(Self::SequenceSource(SequenceSourceOperation::new(
                    *definition,
                    SequenceSourceData::new(position),
                )))
            }
            OperationDefinition::Count(definition) => {
                let count = Cell::new(
                    store.create_data(&format::count_state_name(index), DataPlacement::Shared)?,
                );
                Ok(Self::Count(CountOperation::new(
                    *definition,
                    CountData::new(count),
                )))
            }
        }
    }

    fn open(
        store: &Store,
        index: usize,
        definition: &OperationDefinition,
    ) -> Result<Self, FlowError> {
        match definition {
            OperationDefinition::SequenceSource(definition) => {
                let position = Cell::new(open_required_data(
                    store,
                    &format::sequence_position_name(index),
                )?);
                Ok(Self::SequenceSource(SequenceSourceOperation::new(
                    *definition,
                    SequenceSourceData::new(position),
                )))
            }
            OperationDefinition::Count(definition) => {
                let count = Cell::new(open_required_data(store, &format::count_state_name(index))?);
                Ok(Self::Count(CountOperation::new(
                    *definition,
                    CountData::new(count),
                )))
            }
        }
    }
}

fn open_required_data(store: &Store, name: &str) -> Result<DataHandle, FlowError> {
    match store.open_data(name) {
        Ok(data) => Ok(data),
        Err(StoreError::DataNotFound(name)) => Err(FlowError::MissingResource { name }),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
#[path = "../tests/unit/stage.rs"]
mod tests;
