use std::collections::HashMap;

use dogpaddle_store::ReadOnly;

use crate::{
    build::FlowDefinition,
    stage::{Stage, StageParts},
};

pub(crate) fn assemble_stages(definition: &FlowDefinition, parts: Vec<StageParts>) -> Vec<Stage> {
    let indices = definition
        .stages()
        .iter()
        .enumerate()
        .map(|(index, stage)| (stage.id(), index))
        .collect::<HashMap<_, _>>();
    let inputs = definition
        .stages()
        .iter()
        .map(|stage| {
            stage
                .sources()
                .map(|source| {
                    let source_index = indices[source];
                    let output = parts[source_index]
                        .output()
                        .expect("validated source stage must produce output");
                    ReadOnly::new(output.clone())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    parts
        .into_iter()
        .zip(inputs)
        .map(|(part, inputs)| part.finish(inputs))
        .collect()
}
