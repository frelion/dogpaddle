use dogpaddle_operation::OperationDefinition;

/// The sole durable graph. Fusion never changes these identities or edges.
#[derive(Debug)]
pub(crate) struct FlowDefinition {
    pub(crate) owner_identity: Option<[u8; 32]>,
    pub(crate) operations: Vec<OperationNode>,
}

#[derive(Debug)]
pub(crate) struct OperationNode {
    pub(crate) id: String,
    pub(crate) definition: OperationDefinition,
    pub(crate) inputs: Vec<usize>,
}
