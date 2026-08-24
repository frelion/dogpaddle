use dogpaddle_operation::{CountDefinition, OperationDefinition, SequenceSourceDefinition};

use super::{
    InvalidStageIdReason, StageDefinition, StageRef, Topology, TopologyBuilder, TopologyError,
    validate_acyclic, validate_connections,
};

fn source(start: u64) -> OperationDefinition {
    SequenceSourceDefinition::new(start).into()
}

fn count() -> OperationDefinition {
    CountDefinition::new().into()
}

fn builder() -> TopologyBuilder {
    TopologyBuilder::new()
}

fn find_stage<'a>(topology: &'a Topology, id: &str) -> &'a StageDefinition {
    topology.stages.iter().find(|stage| stage.id == id).unwrap()
}

fn finish_target(operation: OperationDefinition, actual: usize) -> Result<Topology, TopologyError> {
    let mut builder = builder();
    let sources = (0..actual)
        .map(|index| builder.stage(format!("source-{index}"), source(index as u64)))
        .collect::<Vec<_>>();
    let target = builder.stage("target", operation);
    if !sources.is_empty() {
        builder.connect(sources, target);
    }
    builder.finish()
}

#[test]
fn finish_preserves_stage_order_and_resolves_references() {
    let mut builder = builder();
    builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([second], target);

    let topology = builder.finish().unwrap();

    assert_eq!(
        topology
            .stages
            .iter()
            .map(|stage| stage.id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second", "target"]
    );
    let target = find_stage(&topology, "target");
    assert_eq!(target.operation, count());
    assert_eq!(
        target
            .sources
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["second"]
    );
}

#[test]
fn connection_validation_preserves_n_ary_order_and_repeated_sources() {
    let mut builder = builder();
    let first = builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([second, first, second], target);

    let sources =
        validate_connections(builder.token, &builder.stages, &builder.connections).unwrap();

    assert_eq!(sources[target.index].as_deref(), Some([1, 0, 1].as_slice()));
    assert_eq!(validate_acyclic(builder.stages.len(), &sources), Ok(()));
}

#[test]
fn finish_rejects_an_empty_topology() {
    assert_eq!(
        builder().finish().unwrap_err(),
        TopologyError::EmptyTopology
    );
}

#[test]
fn finish_rejects_invalid_stage_ids_in_declaration_order() {
    let mut builder = builder();
    builder.stage("", source(0));
    builder.stage("contains\0nul", source(1));

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::InvalidStageId {
            id: String::new(),
            reason: InvalidStageIdReason::Empty,
        }
    );

    let mut nul_builder = TopologyBuilder::new();
    nul_builder.stage("contains\0nul", source(0));
    assert_eq!(
        nul_builder.finish().unwrap_err(),
        TopologyError::InvalidStageId {
            id: "contains\0nul".to_owned(),
            reason: InvalidStageIdReason::ContainsNul,
        }
    );
}

#[test]
fn finish_rejects_duplicate_stage_ids_before_connections() {
    let mut builder = builder();
    let first = builder.stage("same", source(0));
    builder.stage("same", source(1));
    builder.connect(
        [first],
        StageRef {
            builder_token: 0,
            index: 0,
        },
    );

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::DuplicateStageId("same".to_owned())
    );
}

#[test]
fn finish_rejects_foreign_source_and_target_references() {
    let mut left = builder();
    let foreign = left.stage("foreign", source(0));

    let mut right = builder();
    let target = right.stage("target", count());
    right.connect([foreign], target);
    assert_eq!(
        right.finish().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );

    let mut right = builder();
    let source = right.stage("source", source(0));
    right.connect([source], foreign);
    assert_eq!(
        right.finish().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );
}

#[test]
fn finish_rejects_an_explicit_empty_source_list() {
    let mut builder = builder();
    let target = builder.stage("target", source(0));
    builder.connect([], target);

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::EmptySources("target".to_owned())
    );
}

#[test]
fn finish_accepts_the_known_zero_and_unary_input_counts() {
    let source_topology = finish_target(source(0), 0).unwrap();
    assert!(find_stage(&source_topology, "target").sources.is_empty());

    let count_topology = finish_target(count(), 1).unwrap();
    assert_eq!(find_stage(&count_topology, "target").sources.len(), 1);
}

#[test]
fn finish_rejects_every_known_input_count_mismatch() {
    for (operation, expected, actual) in [(source(0), 0, 1), (count(), 1, 0), (count(), 1, 2)] {
        assert_eq!(
            finish_target(operation, actual).unwrap_err(),
            TopologyError::InputCount {
                stage: "target".to_owned(),
                expected,
                actual,
            }
        );
    }
}

#[test]
fn finish_rejects_setting_sources_twice() {
    let mut builder = builder();
    let first = builder.stage("first", source(1));
    let second = builder.stage("second", source(2));
    let target = builder.stage("target", count());
    builder.connect([first], target);
    builder.connect([second], target);

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::SourcesAlreadySet("target".to_owned())
    );
}

#[test]
fn finish_rejects_a_direct_self_loop() {
    let mut builder = builder();
    let stage = builder.stage("stage", count());
    builder.connect([stage], stage);

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::SelfLoop("stage".to_owned())
    );
}

#[test]
fn finish_rejects_a_multi_stage_cycle() {
    let mut builder = builder();
    let first = builder.stage("first", count());
    let second = builder.stage("second", count());
    let third = builder.stage("third", count());
    builder.connect([first], second);
    builder.connect([second], third);
    builder.connect([third], first);

    assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
}

#[test]
fn finish_allows_fan_out() {
    let mut builder = builder();
    let source = builder.stage("source", source(0));
    let left = builder.stage("left", count());
    let right = builder.stage("right", count());
    builder.connect([source], left);
    builder.connect([source], right);

    let topology = builder.finish().unwrap();

    assert_eq!(find_stage(&topology, "left").sources, ["source"]);
    assert_eq!(find_stage(&topology, "right").sources, ["source"]);
}

#[test]
fn finish_allows_zero_input_stages_and_disconnected_components() {
    let mut builder = builder();
    builder.stage("isolated", source(0));
    let source = builder.stage("source", source(1));
    let count = builder.stage("count", count());
    builder.connect([source], count);

    let topology = builder.finish().unwrap();

    assert!(find_stage(&topology, "isolated").sources.is_empty());
    assert!(find_stage(&topology, "source").sources.is_empty());
    assert_eq!(find_stage(&topology, "count").sources, ["source"]);
}

#[test]
fn finish_rejects_a_cycle_in_one_of_multiple_components() {
    let mut builder = builder();
    builder.stage("isolated", source(0));
    let left = builder.stage("left", count());
    let right = builder.stage("right", count());
    builder.connect([left], right);
    builder.connect([right], left);

    assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
}
