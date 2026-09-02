use std::num::NonZeroU64;

use dogpaddle_change::ProjectionError;
use dogpaddle_flow::{AdvanceOutcome, FlowError, FlowFactory, FlowSchemaError};
use dogpaddle_operation::{
    OperationBindError, encode_definition,
    operation::{
        sink::DiscardDefinition,
        source::SequenceSourceDefinition,
        transform::{CountDefinition, ProjectDefinition, ProjectSchemaError},
    },
};
use dogpaddle_store::{AppendLog, Cell, OrderedMap, Small, Store};

use super::support::{read_published_definition, rewrite_checksum};

const CAPACITY: NonZeroU64 = NonZeroU64::new(1_024 * 1_024).unwrap();

#[test]
fn build_reports_the_exact_project_schema_rejection_without_creating_a_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([1]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([source], project);
    factory.connect([project], sink);

    let Err(FlowError::Schema(error)) = factory.build() else {
        panic!("schema-incompatible Flow did not return FlowError::Schema");
    };
    assert_project_field_rejection(&error);
    assert!(!path.exists(), "Schema rejection created the Store path");
}

#[test]
fn open_rebinds_the_decoded_project_definition_before_opening_runtime_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let project = factory.station("project", ProjectDefinition::new([0]));
    let sink = factory.station("sink", DiscardDefinition::new());
    factory.output_capacity_bytes(source, CAPACITY);
    factory.output_capacity_bytes(project, CAPACITY);
    factory.connect([source], project);
    factory.connect([project], sink);
    drop(factory.build().unwrap());

    let mut definition = read_published_definition(&path);
    let valid_project = encode_definition(&ProjectDefinition::new([0]));
    let offset = definition
        .windows(valid_project.len())
        .position(|candidate| candidate == valid_project)
        .expect("published Flow contains the Project definition");
    let encoded_index = offset + valid_project.len() - size_of::<u32>();
    definition[encoded_index..encoded_index + size_of::<u32>()]
        .copy_from_slice(&1_u32.to_be_bytes());
    rewrite_checksum(&mut definition);
    replace_published_definition(&path, &definition);

    let Err(FlowError::Schema(error)) = FlowFactory::open(&path) else {
        panic!("open did not rebind the decoded schema-incompatible Project");
    };
    assert_project_field_rejection(&error);
    assert_eq!(read_published_definition(&path), definition);
}

#[test]
fn empty_project_schema_runs_through_count_and_discard_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("flow");
    let mut factory = FlowFactory::new(&path);
    let source = factory.station("source", SequenceSourceDefinition::new(u64::MAX));
    let project = factory.station("project", ProjectDefinition::new([]));
    let count = factory.station("count", CountDefinition::new());
    let sink = factory.station("sink", DiscardDefinition::new());
    for station in [source, project, count] {
        factory.output_capacity_bytes(station, CAPACITY);
    }
    factory.connect([source], project);
    factory.connect([project], count);
    factory.connect([count], sink);
    drop(factory.build().unwrap());

    let mut flow = FlowFactory::open(&path).unwrap();
    assert_eq!(flow.advance().unwrap(), AdvanceOutcome::Progressed);
    drop(flow);

    let mut flow = FlowFactory::open(&path).unwrap();
    let mut reached_idle = false;
    for _ in 0..4 {
        if flow.advance().unwrap() == AdvanceOutcome::Idle {
            reached_idle = true;
            break;
        }
    }
    assert!(reached_idle, "finite Project Flow did not become idle");
    drop(flow);

    let store = Store::open(&path).unwrap();
    let _project_state: OrderedMap<Vec<u8>, Vec<u8>, Small> =
        store.open_data("station/00000001/state").unwrap();
    let _project_output: AppendLog<Vec<u8>> = store.open_data("station/00000001/output").unwrap();
    let count: Cell<u64> = store.open_data("station/00000002/operation/count").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        count.access(transaction.access()).unwrap().get().unwrap(),
        Some(1)
    );
}

fn assert_project_field_rejection(error: &FlowSchemaError) {
    assert_eq!(error.station_id(), "project");
    let OperationBindError::Rejected { source } = error.operation_error() else {
        panic!("Project returned a non-concrete Schema binding error");
    };
    assert!(matches!(
        source.downcast_ref::<ProjectSchemaError>(),
        Some(ProjectSchemaError::Projection(
            ProjectionError::FieldOutOfBounds {
                index: 1,
                fields: 1
            }
        ))
    ));
}

fn replace_published_definition(path: &std::path::Path, definition: &[u8]) {
    let store = Store::open(path).unwrap();
    let published: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    published
        .access(transaction.access())
        .unwrap()
        .set(&definition.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}
