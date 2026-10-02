use crate::{
    build::{FlowDefinition, TopologyError, codec, validate},
    error::{FlowError, setup_error},
    flow::{Frames, Runtime, RuntimeNode},
};
use arrow_schema::SchemaRef;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::{RuntimeResource, operation::Operation};
use dogpaddle_store::DataScope;

pub(crate) fn construct(
    definition: FlowDefinition,
    data: &mut DataScope<'_>,
    resources: Vec<RuntimeResource>,
) -> Result<Runtime, FlowError> {
    let count = definition.operations.len();
    let mut schemas: Vec<Option<SchemaRef>> = Vec::with_capacity(count);
    let mut nodes: Vec<RuntimeNode> = Vec::with_capacity(count);
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for (index, (node, resource)) in definition.operations.into_iter().zip(resources).enumerate() {
        let inputs = node
            .inputs
            .iter()
            .map(|&input| {
                schemas[input]
                    .clone()
                    .ok_or_else(|| TopologyError::InputHasNoOutput {
                        operation: node.id.clone(),
                        input: nodes[input].id.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (operation, schema) = node
            .definition
            .construct(
                &inputs,
                &mut data.scoped(&codec::operation_prefix(index)),
                resource,
            )
            .map_err(|source| setup_error(&node.id, source))?
            .into_parts();
        schemas.push(schema);
        match &operation {
            Operation::Source(_) => sources.push(index),
            Operation::Sink(_) => sinks.push(index),
            _ => {}
        }
        nodes.push(RuntimeNode {
            id: node.id,
            inputs: node.inputs,
            operation,
            codec: None,
            pending: None,
        });
    }
    let topology = validate::resolve(&nodes)?;
    let mut message_outputs = vec![false; count];
    for (head, &is_head) in topology.heads.iter().enumerate() {
        if is_head {
            message_outputs[topology.tails[head].last().copied().unwrap_or(head)] = true;
        }
    }
    for (index, (node, schema)) in nodes.iter_mut().zip(schemas).enumerate() {
        node.codec = schema
            .as_ref()
            .filter(|_| matches!(&node.operation, Operation::Source(_)) || message_outputs[index])
            .map(|schema| {
                SchemaBoundChangeCodec::try_new(schema.clone()).map_err(|source| {
                    FlowError::OutputCodec {
                        operation_id: node.id.clone(),
                        source,
                    }
                })
            })
            .transpose()?;
    }
    let frames = Frames::bind(data)?;
    Ok(Runtime {
        topology,
        nodes,
        frames,
        sources,
        sinks,
        source_cursor: 0,
        sink_cursor: 0,
        root_cursor: 0,
        needs_reopen: false,
    })
}
