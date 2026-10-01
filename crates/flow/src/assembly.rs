use crate::{
    build::{FlowDefinition, ResolvedTopology, codec},
    error::{FlowError, setup_error},
    flow::{Frames, Runtime, RuntimeNode},
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
    let mut message_outputs = vec![false; count];
    for (head, &is_head) in topology.heads.iter().enumerate() {
        if is_head {
            message_outputs[topology.tails[head].last().copied().unwrap_or(head)] = true;
        }
    }
    let mut schemas: Vec<Option<SchemaRef>> = Vec::with_capacity(count);
    let mut nodes = Vec::with_capacity(count);
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for (index, (node, resource)) in definition.operations.into_iter().zip(resources).enumerate() {
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
                resource,
            )
            .map_err(|source| setup_error(&node.id, source))?
            .into_parts();
        let codec = schema
            .as_ref()
            .filter(|_| matches!(&operation, Operation::Source(_)) || message_outputs[index])
            .map(|schema| {
                SchemaBoundChangeCodec::try_new(schema.clone()).map_err(|source| {
                    FlowError::OutputCodec {
                        operation_id: node.id.clone(),
                        source,
                    }
                })
            })
            .transpose()?;
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
            codec,
            pending: None,
        });
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
