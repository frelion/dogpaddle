use dogpaddle_operation::{CountDefinition, OperationDefinition};

use super::{
    InputCount, InvalidStageIdReason, StageDefinition, StageRef, Topology, TopologyBuilder,
    TopologyError,
};

#[derive(Debug, Eq, PartialEq)]
struct TestDefinition {
    name: &'static str,
    input_count: usize,
}

impl InputCount for TestDefinition {
    fn input_count(&self) -> usize {
        self.input_count
    }
}

const fn definition(name: &'static str, input_count: usize) -> TestDefinition {
    TestDefinition { name, input_count }
}

fn builder() -> TopologyBuilder<TestDefinition> {
    TopologyBuilder::new()
}

fn find_stage<'a>(
    topology: &'a Topology<TestDefinition>,
    id: &str,
) -> &'a StageDefinition<TestDefinition> {
    topology
        .stages
        .iter()
        .find(|stage| stage.id.as_str() == id)
        .unwrap()
}

fn finish_with_input_count(
    expected: usize,
    actual: usize,
) -> Result<Topology<TestDefinition>, TopologyError> {
    let mut builder = builder();
    let sources = (0..actual)
        .map(|index| builder.stage(format!("source-{index}"), definition("source", 0)))
        .collect::<Vec<_>>();
    let target = builder.stage("target", definition("target", expected));
    if !sources.is_empty() {
        builder.connect(sources, target);
    }
    builder.finish()
}

#[test]
fn finish_preserves_stage_and_source_order_and_resolves_references() {
    let mut builder = builder();
    let first = builder.stage("first", definition("source", 0));
    let second = builder.stage("second", definition("source", 0));
    let target = builder.stage("target", definition("join", 2));
    builder.connect([second, first], target);

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
    assert_eq!(target.operation.name, "join");
    assert_eq!(target.operation, definition("join", 2));
    assert_eq!(
        target
            .sources
            .iter()
            .map(super::StageId::as_str)
            .collect::<Vec<_>>(),
        ["second", "first"]
    );
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
    builder.stage("", definition("first", 0));
    builder.stage("contains\0nul", definition("second", 0));

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::InvalidStageId {
            id: String::new(),
            reason: InvalidStageIdReason::Empty,
        }
    );

    let mut nul_builder = TopologyBuilder::new();
    nul_builder.stage("contains\0nul", definition("only", 0));
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
    let first = builder.stage("same", definition("first", 0));
    builder.stage("same", definition("second", 0));
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
    let foreign = left.stage("foreign", definition("source", 0));

    let mut right = builder();
    let target = right.stage("target", definition("sink", 1));
    right.connect([foreign], target);
    assert_eq!(
        right.finish().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );

    let mut right = builder();
    let source = right.stage("source", definition("source", 0));
    right.connect([source], foreign);
    assert_eq!(
        right.finish().unwrap_err(),
        TopologyError::ForeignStageRef(foreign)
    );
}

#[test]
fn finish_rejects_an_explicit_empty_source_list() {
    let mut builder = builder();
    let target = builder.stage("target", definition("source", 0));
    builder.connect([], target);

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::EmptySources("target".to_owned())
    );
}

#[test]
fn finish_accepts_exact_zero_unary_binary_and_n_ary_input_counts() {
    for input_count in 0..=4 {
        let topology = finish_with_input_count(input_count, input_count).unwrap();
        assert_eq!(find_stage(&topology, "target").sources.len(), input_count);
    }
}

#[test]
fn finish_rejects_every_input_count_mismatch() {
    for (expected, actual) in [(0, 1), (1, 0), (1, 2), (2, 1), (3, 2), (3, 4)] {
        assert_eq!(
            finish_with_input_count(expected, actual).unwrap_err(),
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
    let first = builder.stage("first", definition("source", 0));
    let second = builder.stage("second", definition("source", 0));
    let target = builder.stage("target", definition("sink", 1));
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
    let stage = builder.stage("stage", definition("operation", 1));
    builder.connect([stage], stage);

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::SelfLoop("stage".to_owned())
    );
}

#[test]
fn finish_rejects_a_multi_stage_cycle() {
    let mut builder = builder();
    let first = builder.stage("first", definition("operation", 1));
    let second = builder.stage("second", definition("operation", 1));
    let third = builder.stage("third", definition("operation", 1));
    builder.connect([first], second);
    builder.connect([second], third);
    builder.connect([third], first);

    assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
}

#[test]
fn finish_allows_fan_out_and_repeated_sources() {
    let mut builder = builder();
    let source = builder.stage("source", definition("source", 0));
    let left = builder.stage("left", definition("sink", 1));
    let right = builder.stage("right", definition("join", 2));
    builder.connect([source], left);
    builder.connect([source, source], right);

    let topology = builder.finish().unwrap();

    assert_eq!(find_stage(&topology, "left").sources.len(), 1);
    assert_eq!(
        find_stage(&topology, "right")
            .sources
            .iter()
            .map(super::StageId::as_str)
            .collect::<Vec<_>>(),
        ["source", "source"]
    );
}

#[test]
fn finish_allows_zero_input_stages_and_disconnected_components() {
    let mut builder = builder();
    builder.stage("isolated", definition("source", 0));
    let source = builder.stage("source", definition("source", 0));
    let sink = builder.stage("sink", definition("sink", 1));
    builder.connect([source], sink);

    let topology = builder.finish().unwrap();

    assert!(find_stage(&topology, "isolated").sources.is_empty());
    assert!(find_stage(&topology, "source").sources.is_empty());
    assert_eq!(find_stage(&topology, "sink").sources.len(), 1);
}

#[test]
fn finish_rejects_a_cycle_in_one_of_multiple_components() {
    let mut builder = builder();
    builder.stage("isolated", definition("source", 0));
    let left = builder.stage("left", definition("operation", 1));
    let right = builder.stage("right", definition("operation", 1));
    builder.connect([left], right);
    builder.connect([right], left);

    assert_eq!(builder.finish().unwrap_err(), TopologyError::Cycle);
}

#[test]
fn finish_uses_the_closed_operation_definition_input_count() {
    let mut builder = TopologyBuilder::<OperationDefinition>::new();
    builder.stage("count", CountDefinition::new().into());

    assert_eq!(
        builder.finish().unwrap_err(),
        TopologyError::InputCount {
            stage: "count".to_owned(),
            expected: 1,
            actual: 0,
        }
    );
}
