use super::{FlowFactory, codec, preflight_resources};
use crate::{assembly::construct, error::FlowError, flow::Flow};
use dogpaddle_store::{Cell, Store, StoreError};
impl FlowFactory {
    /// Restores the durable graph and validates its call stack without writing.
    /// # Errors
    /// Returns an error for declarations on the opener, mismatched owner identity,
    /// missing resources, invalid schemas, corrupt state or storage failures.
    pub fn open(self) -> Result<Flow, FlowError> {
        if !self.operations.is_empty() {
            return Err(FlowError::OpenWithDefinition);
        }
        let store = Store::open(&self.path)?;
        let published = match store.open_data::<Cell<Vec<u8>>>(codec::DEFINITION_DATA_NAME) {
            Ok(data) => data,
            Err(StoreError::DataNotFound(_)) => return Err(FlowError::IncompleteBuild),
            Err(error) => return Err(error.into()),
        };
        let encoded = published
            .read(store.read_transaction().access())?
            .get_bounded(codec::MAX_DEFINITION_BYTES)?
            .ok_or(FlowError::IncompleteBuild)?;
        let definition = codec::decode(&encoded)?;
        if definition.owner_identity != self.owner_identity {
            return Err(FlowError::OwnerIdentityMismatch);
        }
        let resources = preflight_resources(&definition, self.resources)?;
        let mut runtime = construct(definition, &mut store.data_scope(), resources)?;
        runtime.restore(store.read_transaction().access())?;
        let (transactions, reads) = store.into_transactions().split();
        Ok(Flow::from_parts(self.path, runtime, transactions, reads))
    }
}
