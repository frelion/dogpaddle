use dogpaddle_flow::{Decision, Flow, FlowError, Operation, OperationError, Work};
use dogpaddle_store::{DataPlacement, Store, StoreError, Transaction};

struct Ports;

impl Operation for Ports {
    fn fingerprint(&self) -> &[u8] {
        b"ports"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        Ok(Decision::Pending)
    }
}

#[test]
fn rejects_unconnected_inputs_before_execution() {
    let directory = tempfile::tempdir().unwrap();
    let mut flow = Flow::create(directory.path().join("flow")).unwrap();
    flow.stage("sink", &["input"], Ports).unwrap();

    assert!(matches!(
        flow.step(),
        Err(FlowError::UnconnectedInput { stage, input })
            if stage == "sink" && input == "input"
    ));
}

#[test]
fn rejects_cycles_before_execution() {
    let directory = tempfile::tempdir().unwrap();
    let mut flow = Flow::create(directory.path().join("flow")).unwrap();
    let left = flow.stage("left", &["input"], Ports).unwrap();
    let right = flow.stage("right", &["input"], Ports).unwrap();
    flow.connect(left, right, "input").unwrap();
    flow.connect(right, left, "input").unwrap();

    assert!(matches!(flow.step(), Err(FlowError::Cycle)));
}

#[test]
fn stage_handles_are_confined_to_their_flow() {
    let directory = tempfile::tempdir().unwrap();
    let mut left = Flow::create(directory.path().join("left")).unwrap();
    let mut right = Flow::create(directory.path().join("right")).unwrap();
    let foreign = left.stage("source", &[], Ports).unwrap();
    let sink = right.stage("sink", &["input"], Ports).unwrap();

    assert!(matches!(
        right.connect(foreign, sink, "input"),
        Err(FlowError::WrongFlow)
    ));
}

#[test]
fn persisted_declaration_rejects_a_different_graph() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        flow.stage("source", &[], Ports).unwrap();
        assert_eq!(flow.step().unwrap(), dogpaddle_flow::StepOutcome::Idle);
    }

    let mut flow = Flow::open(&path).unwrap();
    flow.stage("renamed", &[], Ports).unwrap();
    assert!(matches!(flow.step(), Err(FlowError::DeclarationMismatch)));
}

#[test]
fn provisioning_resumes_after_drop_before_first_step() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        flow.data("state", DataPlacement::Dedicated).unwrap();
        flow.stage("source", &[], Ports).unwrap();
    }

    let mut flow = Flow::open(&path).unwrap();
    flow.data("state", DataPlacement::Dedicated).unwrap();
    flow.stage("source", &[], Ports).unwrap();
    assert_eq!(flow.step().unwrap(), dogpaddle_flow::StepOutcome::Idle);
}

#[test]
fn persisted_data_placement_is_part_of_the_declaration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        flow.data("state", DataPlacement::Dedicated).unwrap();
        flow.stage("source", &[], Ports).unwrap();
        assert_eq!(flow.step().unwrap(), dogpaddle_flow::StepOutcome::Idle);
    }

    let mut flow = Flow::open(&path).unwrap();
    assert!(matches!(
        flow.data("state", DataPlacement::Shared),
        Err(FlowError::DeclarationMismatch)
    ));
}

#[test]
fn a_sealed_flow_never_creates_data_from_a_new_declaration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    {
        let mut flow = Flow::create(&path).unwrap();
        flow.stage("source", &[], Ports).unwrap();
        assert_eq!(flow.step().unwrap(), dogpaddle_flow::StepOutcome::Idle);
    }

    let mut flow = Flow::open(&path).unwrap();
    assert!(matches!(
        flow.data("unexpected", DataPlacement::Shared),
        Err(FlowError::Store(StoreError::DataNotFound(name))) if name == "unexpected"
    ));
    drop(flow);

    let store = Store::open(&path).unwrap();
    assert!(matches!(
        store.open_data("unexpected"),
        Err(StoreError::DataNotFound(name)) if name == "unexpected"
    ));
}

#[test]
fn one_store_path_has_one_live_flow_executor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("flow");
    let flow = Flow::create(&path).unwrap();

    assert!(matches!(Flow::open(&path), Err(FlowError::Store(_))));
    drop(flow);
    Flow::open(&path).unwrap();
}
