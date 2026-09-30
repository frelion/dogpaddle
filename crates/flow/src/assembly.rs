use crate::{
    build::{FlowDefinition, ResolvedTopology, codec},
    error::{FlowError, setup_error},
    flow::{Frames, Runtime},
};
use arrow_schema::SchemaRef;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::{RuntimeResource, operation::Operation};
use dogpaddle_store::DataScope;

pub(crate) fn construct(
    definition: FlowDefinition,
    topology: ResolvedTopology,
    data: &mut DataScope<'_>,
    resources: Vec<RuntimeResource>,
) -> Result<Runtime, FlowError> {
    let count = definition.operations.len();
    let mut resources = resources.into_iter().map(Some).collect::<Vec<_>>();
    let mut schemas: Vec<Option<SchemaRef>> = vec![None; count];
    let mut operations: Vec<Option<Operation>> = (0..count).map(|_| None).collect();
    let mut codecs: Vec<Option<SchemaBoundChangeCodec>> = (0..count).map(|_| None).collect();
    for &index in &topology.schedule {
        let node = &definition.operations[index];
        let inputs = node
            .inputs
            .iter()
            .map(|&input| {
                schemas[input]
                    .as_ref()
                    .expect("validated producer has output")
                    .clone()
            })
            .collect::<Vec<_>>();
        let (operation, schema) = node
            .definition
            .construct(
                &inputs,
                &mut data.scoped(&codec::operation_prefix(index)),
                resources[index].take().expect("resource consumed once"),
            )
            .map_err(|source| setup_error(&node.id, source))?
            .into_parts();
        codecs[index] = schema
            .as_ref()
            .map(|schema| {
                SchemaBoundChangeCodec::try_new(schema.clone()).map_err(|source| {
                    FlowError::OutputCodec {
                        operation_id: node.id.clone(),
                        source,
                    }
                })
            })
            .transpose()?;
        schemas[index] = schema;
        operations[index] = Some(operation);
    }
    let frames = Frames::bind(data)?;
    let sources = topology
        .schedule
        .iter()
        .copied()
        .filter(|&index| definition.operations[index].definition.kind().is_scan())
        .collect();
    let sinks = topology
        .schedule
        .iter()
        .copied()
        .filter(|&index| definition.operations[index].definition.kind().is_sink())
        .collect();
    Ok(Runtime {
        definition,
        topology,
        operations: operations
            .into_iter()
            .map(|operation| operation.expect("all operations constructed"))
            .collect(),
        codecs,
        frames,
        sources,
        sinks,
        source_cursor: 0,
        sink_cursor: 0,
        root_cursor: 0,
        pending: (0..count).map(|_| None).collect(),
        needs_reopen: false,
    })
}
