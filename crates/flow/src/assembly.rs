use crate::{
    build::{FlowDefinition, ResolvedTopology, codec},
    error::{FlowError, setup_error},
    flow::{Frames, Runtime},
};
use arrow_schema::SchemaRef;
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::RuntimeResource;
use dogpaddle_store::DataScope;

pub(crate) fn construct(
    definition: FlowDefinition,
    topology: ResolvedTopology,
    data: &mut DataScope<'_>,
    resources: Vec<RuntimeResource>,
) -> Result<Runtime, FlowError> {
    let count = definition.operations.len();
    let mut schemas: Vec<Option<SchemaRef>> = Vec::with_capacity(count);
    let mut operations = Vec::with_capacity(count);
    let mut codecs = Vec::with_capacity(count);
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for (index, (node, resource)) in definition.operations.iter().zip(resources).enumerate() {
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
        codecs.push(
            schema
                .as_ref()
                .map(|schema| {
                    SchemaBoundChangeCodec::try_new(schema.clone()).map_err(|source| {
                        FlowError::OutputCodec {
                            operation_id: node.id.clone(),
                            source,
                        }
                    })
                })
                .transpose()?,
        );
        schemas.push(schema);
        operations.push(operation);
        let kind = node.definition.kind();
        if kind.is_scan() {
            sources.push(index);
        } else if kind.is_sink() {
            sinks.push(index);
        }
    }
    let frames = Frames::bind(data)?;
    Ok(Runtime {
        definition,
        topology,
        operations,
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
