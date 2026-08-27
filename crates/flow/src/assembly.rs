use std::collections::HashMap;

use dogpaddle_store::ReadOnly;

use crate::{
    build::FlowDefinition,
    station::{Station, StationParts},
};

pub(crate) fn assemble_stations(
    definition: &FlowDefinition,
    parts: Vec<StationParts>,
) -> Vec<Station> {
    let indices = definition
        .stations()
        .iter()
        .enumerate()
        .map(|(index, station)| (station.id(), index))
        .collect::<HashMap<_, _>>();
    let inputs = definition
        .stations()
        .iter()
        .map(|station| {
            station
                .sources()
                .map(|source| {
                    let source_index = indices[source];
                    let output = parts[source_index]
                        .output()
                        .expect("validated source station must produce output");
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
