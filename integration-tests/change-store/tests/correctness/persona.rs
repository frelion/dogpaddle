use std::{collections::BTreeSet, num::NonZeroUsize, path::Path, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{
    Change, ChangeProjection, decode_change, decode_change_projected, encode_change,
};
use dogpaddle_change_store_integration::{
    ChangeWorkloadSpec, ChurnEvent, DiffModel, PersonaWorkload, ProjectionProfile,
    SchemaDescriptor, StoreFixture, WorkloadPersona, assert_change_eq, churn_changes,
    flatten_churn_changes, generate_persona_change, generate_persona_workload, valid_churn_events,
    validate_churn,
};
use dogpaddle_store::{AppendLog, CodecError as StoreCodecError, ScanLimit, Store, StoreError};

const ROW_BOUNDARIES: [usize; 7] = [1, 7, 8, 9, 63, 64, 65];
const CONCRETE_PERSONAS: [WorkloadPersona; 8] = [
    WorkloadPersona::DiffOnlyControl,
    WorkloadPersona::LayoutV1_16,
    WorkloadPersona::FixedEvent8,
    WorkloadPersona::MixedEvent16,
    WorkloadPersona::WideNumeric64,
    WorkloadPersona::BlobEvent4,
    WorkloadPersona::NestedEvent8,
    WorkloadPersona::SlicedMixed16,
];

#[derive(Default)]
struct FieldMetrics {
    leaves: usize,
    nullable: usize,
    variable_width_leaves: usize,
    nested: usize,
}

#[test]
fn descriptors_match_generated_schema_trees_and_legal_projections() {
    assert_eq!(WorkloadPersona::ALL.len(), 9);
    let names = WorkloadPersona::ALL
        .iter()
        .map(|persona| persona.name())
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), WorkloadPersona::ALL.len());

    for persona in CONCRETE_PERSONAS {
        let descriptor = persona.descriptor();
        assert_eq!(descriptor.diff_model, DiffModel::InsertOnly);
        assert_eq!(descriptor.schemas.len(), 1);
        let generated = generate_persona_change(persona, 0, 10, ChangeWorkloadSpec::new(9, 17));
        let schema = generated.change.schema();
        let expected = descriptor.schemas[0];
        assert_descriptor_matches(expected, schema.as_ref());
        assert_eq!(generated.schema_descriptor(), &expected);

        let mut profiles = BTreeSet::new();
        for projection in expected.projection_profiles {
            assert!(profiles.insert(projection.profile.as_str()));
            assert_strictly_increasing(projection.selected_columns);
            assert!(
                projection
                    .selected_columns
                    .iter()
                    .all(|index| *index < expected.business_columns)
            );
            assert_eq!(
                persona.projection_indices(projection.profile),
                Some(projection.selected_columns)
            );
            let bound = ChangeProjection::try_new(
                Arc::clone(&schema),
                projection.selected_columns.iter().copied(),
            )
            .unwrap();
            assert_eq!(
                bound.output_schema().fields().len(),
                projection.selected_columns.len()
            );
        }
        assert_eq!(
            profiles.contains(ProjectionProfile::PayloadOnly.as_str()),
            expected.top_level_variable_width_columns > 0
        );
    }

    let heterogeneous = WorkloadPersona::Heterogeneous.descriptor();
    assert_eq!(heterogeneous.diff_model, DiffModel::InsertOnly);
    assert_eq!(heterogeneous.schemas.len(), CONCRETE_PERSONAS.len());
    assert_eq!(
        heterogeneous
            .schemas
            .iter()
            .map(|schema| schema.name)
            .collect::<Vec<_>>(),
        CONCRETE_PERSONAS
            .iter()
            .map(|persona| persona.descriptor().schemas[0].name)
            .collect::<Vec<_>>()
    );
    assert!(
        WorkloadPersona::Heterogeneous
            .projection_indices(ProjectionProfile::DiffOnly)
            .is_none()
    );
}

#[test]
fn persona_matrix_survives_append_commit_reopen_raw_full_and_every_projection() {
    let fixture = StoreFixture::new();
    let mut expected = Vec::new();
    let mut event_start = 1_000_u64;
    for persona in CONCRETE_PERSONAS {
        for (boundary_index, rows) in ROW_BOUNDARIES.into_iter().enumerate() {
            let payload_bytes = [1, 7, 8, 9, 63, 64, 65][boundary_index];
            let generated = generate_persona_change(
                persona,
                boundary_index,
                event_start,
                ChangeWorkloadSpec::new(rows, payload_bytes),
            );
            assert!(
                generated
                    .change
                    .diffs()
                    .values()
                    .iter()
                    .all(|diff| *diff == 1)
            );
            let encoded = encode_change(&generated.change).unwrap();
            assert_change_eq(&decode_change(&encoded).unwrap(), &generated.change);
            expected.push((generated, encoded));
            event_start = event_start
                .checked_add(u64::try_from(rows).unwrap())
                .unwrap();
        }
    }

    let mut store = Store::create(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("persona-matrix").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(
                &expected
                    .iter()
                    .map(|(_, encoded)| encoded.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("persona-matrix").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let scan_bytes = expected
        .iter()
        .map(|(_, encoded)| encoded.len() + size_of::<u64>())
        .sum();
    let mut index = 0_usize;
    let progress = access
        .scan(
            0,
            ScanLimit::new(expected.len(), scan_bytes).unwrap(),
            |entry| {
                let (generated, encoded) = &expected[index];
                assert_eq!(entry.offset(), u64::try_from(index).unwrap());
                let raw = entry.project(|bytes| Ok(bytes.to_vec()))?;
                assert_eq!(&raw, encoded);
                let full = entry.project(decode_entry)?;
                assert_change_eq(&full, &generated.change);
                for projection in generated.schema_descriptor().projection_profiles {
                    let bound = ChangeProjection::try_new(
                        generated.change.schema(),
                        projection.selected_columns.iter().copied(),
                    )
                    .unwrap();
                    let actual = entry.project(|bytes| decode_projected_entry(bytes, &bound))?;
                    let expected =
                        independent_projection(&generated.change, projection.selected_columns);
                    assert_change_eq(&actual, &expected);
                }
                index += 1;
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert!(progress.caught_up);
    assert_eq!(index, expected.len());
}

#[test]
fn heterogeneous_variable_entries_page_after_rollback_and_reopen_without_losing_order() {
    let specs = heterogeneous_specs();
    let workload = generate_persona_workload(WorkloadPersona::Heterogeneous, 55, &specs);
    assert_heterogeneous_workload(&workload, specs.len());

    let fixture = StoreFixture::new();
    append_after_rollback(fixture.path(), &workload);
    let (pages, raw_entries) = scan_heterogeneous_pages(fixture.path(), &workload);
    assert!(pages > 1);
    assert_eq!(raw_entries, workload.encoded);
    assert_eq!(ordered_checksum(&raw_entries), workload.order_checksum());

    let retained_start = truncate_to_byte_window(fixture.path(), &workload);
    verify_retained_after_reopen(fixture.path(), &workload, retained_start);
}

fn heterogeneous_specs() -> [ChangeWorkloadSpec; 16] {
    [
        ChangeWorkloadSpec::new(1, 1),
        ChangeWorkloadSpec::new(7, 17),
        ChangeWorkloadSpec::new(8, 65),
        ChangeWorkloadSpec::new(9, 257),
        ChangeWorkloadSpec::new(63, 9),
        ChangeWorkloadSpec::new(64, 129),
        ChangeWorkloadSpec::new(65, 33),
        ChangeWorkloadSpec::new(3, 1_025),
        ChangeWorkloadSpec::new(5, 3),
        ChangeWorkloadSpec::new(11, 511),
        ChangeWorkloadSpec::new(17, 21),
        ChangeWorkloadSpec::new(31, 73),
        ChangeWorkloadSpec::new(2, 2_049),
        ChangeWorkloadSpec::new(13, 37),
        ChangeWorkloadSpec::new(29, 145),
        ChangeWorkloadSpec::new(41, 5),
    ]
}

fn assert_heterogeneous_workload(workload: &PersonaWorkload, expected_changes: usize) {
    assert_eq!(workload.changes.len(), expected_changes);
    assert_eq!(
        workload
            .changes
            .iter()
            .map(|change| change.persona)
            .collect::<BTreeSet<_>>()
            .len(),
        CONCRETE_PERSONAS.len()
    );
    assert!(
        workload
            .encoded
            .iter()
            .map(Vec::len)
            .collect::<BTreeSet<_>>()
            .len()
            >= 8
    );
}

fn append_after_rollback(path: &Path, workload: &PersonaWorkload) {
    let mut store = Store::create(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.create_data("heterogeneous").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(&workload.encoded)
            .unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            log.access(transaction.access()).unwrap().bounds().unwrap(),
            0..0
        );
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        log.access(transaction.access())
            .unwrap()
            .append_batch(&workload.encoded)
            .unwrap();
        transaction.commit().unwrap();
    }
}

fn scan_heterogeneous_pages(path: &Path, workload: &PersonaWorkload) -> (usize, Vec<Vec<u8>>) {
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("heterogeneous").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    let largest_charge = workload
        .encoded
        .iter()
        .map(|encoded| encoded.len() + size_of::<u64>())
        .max()
        .unwrap();
    let limit = ScanLimit::new(workload.encoded.len(), largest_charge).unwrap();
    let mut offset = 0_u64;
    let mut pages = 0_usize;
    let mut raw_entries = Vec::new();
    loop {
        pages += 1;
        let page_start = offset;
        let progress = access
            .scan(offset, limit, |entry| {
                let index = usize::try_from(entry.offset()).unwrap();
                let generated = &workload.changes[index];
                let raw = entry.project(|bytes| Ok(bytes.to_vec()))?;
                assert_eq!(raw, workload.encoded[index]);
                raw_entries.push(raw);
                assert_change_eq(&entry.project(decode_entry)?, &generated.change);
                for projection in generated.schema_descriptor().projection_profiles {
                    let bound = ChangeProjection::try_new(
                        generated.change.schema(),
                        projection.selected_columns.iter().copied(),
                    )
                    .unwrap();
                    let actual = entry.project(|bytes| decode_projected_entry(bytes, &bound))?;
                    assert_change_eq(
                        &actual,
                        &independent_projection(&generated.change, projection.selected_columns),
                    );
                }
                Ok::<(), StoreError>(())
            })
            .unwrap();
        offset = progress.next_offset;
        assert!(progress.caught_up || offset > page_start);
        if progress.caught_up {
            break;
        }
    }
    assert_eq!(offset, u64::try_from(workload.changes.len()).unwrap());
    transaction.commit().unwrap();
    (pages, raw_entries)
}

fn truncate_to_byte_window(path: &Path, workload: &PersonaWorkload) -> usize {
    let retained_target_bytes = workload.scan_bytes() / 2;
    let mut retained_bytes = 0_usize;
    let mut retained_start = workload.encoded.len();
    for index in (0..workload.encoded.len()).rev() {
        let entry_charge = workload.encoded[index]
            .len()
            .checked_add(size_of::<u64>())
            .unwrap();
        let next = retained_bytes.checked_add(entry_charge).unwrap();
        if retained_bytes > 0 && next > retained_target_bytes {
            break;
        }
        retained_bytes = next;
        retained_start = index;
    }
    assert!(retained_start > 0);
    assert!(retained_start < workload.encoded.len());
    assert!(
        retained_bytes <= retained_target_bytes || retained_start + 1 == workload.encoded.len()
    );
    let target = u64::try_from(retained_start).unwrap();
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("heterogeneous").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let mut access = log.access(transaction.access()).unwrap();
    let mut head = 0_u64;
    let mut steps = 0_usize;
    while head < target {
        head = access
            .truncate_before(target, NonZeroUsize::new(2).unwrap())
            .unwrap();
        steps += 1;
    }
    assert_eq!(head, target);
    assert!(steps > 1);
    transaction.commit().unwrap();
    retained_start
}

fn verify_retained_after_reopen(path: &Path, workload: &PersonaWorkload, retained_start: usize) {
    let target = u64::try_from(retained_start).unwrap();
    let store = Store::open(path).unwrap();
    let log: AppendLog<Vec<u8>> = store.open_data("heterogeneous").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let access = log.access(transaction.access()).unwrap();
    assert_eq!(
        access.bounds().unwrap(),
        target..u64::try_from(workload.encoded.len()).unwrap()
    );
    let retained_scan_bytes = workload.encoded[retained_start..]
        .iter()
        .map(|encoded| encoded.len() + size_of::<u64>())
        .sum();
    let mut retained_raw = Vec::new();
    let progress = access
        .scan(
            target,
            ScanLimit::new(workload.encoded.len() - retained_start, retained_scan_bytes).unwrap(),
            |entry| {
                let index = usize::try_from(entry.offset()).unwrap();
                let raw = entry.project(|bytes| Ok(bytes.to_vec()))?;
                assert_eq!(raw, workload.encoded[index]);
                retained_raw.push(raw);
                assert_change_eq(
                    &entry.project(decode_entry)?,
                    &workload.changes[index].change,
                );
                Ok::<(), StoreError>(())
            },
        )
        .unwrap();
    assert!(progress.caught_up);
    assert_eq!(retained_raw, workload.encoded[retained_start..]);
    assert_eq!(
        ordered_checksum(&retained_raw),
        ordered_checksum(&workload.encoded[retained_start..])
    );
}

#[test]
fn valid_churn_remains_valid_and_ordered_across_stable_rebatching_and_reopen() {
    let expected = valid_churn_events(700, 65);
    let expected_model = validate_churn(&expected).unwrap();
    let partitions = [
        vec![65],
        vec![1, 64],
        vec![7, 8, 9, 41],
        vec![1, 7, 8, 9, 40],
    ];
    let changes = partitions
        .iter()
        .map(|partition| churn_changes(700, partition))
        .collect::<Vec<_>>();
    for actual in &changes {
        let flattened = flatten_churn_changes(actual);
        assert_eq!(flattened, expected);
        assert_eq!(validate_churn(&flattened).unwrap(), expected_model);
    }

    let fixture = StoreFixture::new();
    let mut store = Store::create(fixture.path()).unwrap();
    let logs = partitions
        .iter()
        .enumerate()
        .map(|(index, _)| {
            store
                .create_data::<AppendLog<Vec<u8>>>(&format!("rebatch-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let encoded = changes
        .iter()
        .map(|changes| {
            changes
                .iter()
                .map(|change| encode_change(change).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        for (log, encoded) in logs.iter().zip(&encoded) {
            log.access(transaction.access())
                .unwrap()
                .append_batch(encoded)
                .unwrap();
        }
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(fixture.path()).unwrap();
    let logs = partitions
        .iter()
        .enumerate()
        .map(|(index, _)| {
            store
                .open_data::<AppendLog<Vec<u8>>>(&format!("rebatch-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut transactions = store.into_transactions();
    for ((log, encoded), partition) in logs.iter().zip(&encoded).zip(&partitions) {
        let transaction = transactions.begin().unwrap();
        let access = log.access(transaction.access()).unwrap();
        let bytes = encoded
            .iter()
            .map(|entry| entry.len() + size_of::<u64>())
            .sum();
        let mut decoded = Vec::new();
        let progress = access
            .scan(
                0,
                ScanLimit::new(partition.len(), bytes).unwrap(),
                |entry| {
                    decoded.push(entry.project(decode_entry)?);
                    Ok::<(), StoreError>(())
                },
            )
            .unwrap();
        assert!(progress.caught_up);
        let flattened = flatten_churn_changes(&decoded);
        assert_eq!(flattened, expected);
        assert_eq!(validate_churn(&flattened).unwrap(), expected_model);
    }
}

#[test]
fn valid_churn_oracle_rejects_the_first_negative_prefix() {
    let events = [
        ChurnEvent { value: 1, diff: 1 },
        ChurnEvent { value: 1, diff: -1 },
        ChurnEvent { value: 2, diff: -1 },
    ];
    let error = validate_churn(&events).unwrap_err();
    assert_eq!(error.index, 2);
    assert_eq!(error.value, 2);
    assert_eq!(error.weight, -1);
}

#[test]
fn persona_generation_accepts_zero_seed_slices_and_rejects_identifier_overflow_up_front() {
    let sliced = generate_persona_change(
        WorkloadPersona::SlicedMixed16,
        0,
        0,
        ChangeWorkloadSpec::new(1, 1),
    );
    assert_eq!(sliced.event_start, 1);
    let ids = sliced
        .change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1]);

    let overflow_change = std::panic::catch_unwind(|| {
        let _ = generate_persona_change(
            WorkloadPersona::FixedEvent8,
            0,
            u64::MAX,
            ChangeWorkloadSpec::new(2, 1),
        );
    });
    assert!(overflow_change.is_err());

    let overflow_workload = std::panic::catch_unwind(|| {
        let _ = generate_persona_workload(
            WorkloadPersona::FixedEvent8,
            u64::MAX,
            &[ChangeWorkloadSpec::new(1, 1), ChangeWorkloadSpec::new(1, 1)],
        );
    });
    assert!(overflow_workload.is_err());

    let overflow_churn = std::panic::catch_unwind(|| valid_churn_events(u64::MAX, 5));
    assert!(overflow_churn.is_err());
}

fn assert_descriptor_matches(expected: SchemaDescriptor, schema: &Schema) {
    assert_eq!(
        schema.fields().len(),
        expected.business_columns,
        "{}",
        expected.name
    );
    assert_eq!(expected.physical_columns, expected.business_columns + 1);
    let mut total = FieldMetrics::default();
    for field in schema.fields() {
        add_field_metrics(field, &mut total);
    }
    assert_eq!(total.leaves, expected.leaf_columns, "{}", expected.name);
    assert_eq!(
        total.nullable, expected.total_nullable_fields,
        "{}",
        expected.name
    );
    assert_eq!(
        total.variable_width_leaves, expected.variable_width_leaf_columns,
        "{}",
        expected.name
    );
    assert_eq!(
        total.nested, expected.total_nested_fields,
        "{}",
        expected.name
    );
    assert_eq!(
        schema
            .fields()
            .iter()
            .filter(|field| field.is_nullable())
            .count(),
        expected.top_level_nullable_columns,
        "{}",
        expected.name
    );
    assert_eq!(
        schema
            .fields()
            .iter()
            .filter(|field| matches!(field.data_type(), DataType::Utf8 | DataType::Binary))
            .count(),
        expected.top_level_variable_width_columns,
        "{}",
        expected.name
    );
    assert_eq!(
        schema
            .fields()
            .iter()
            .filter(|field| matches!(field.data_type(), DataType::List(_) | DataType::Struct(_)))
            .count(),
        expected.top_level_nested_columns,
        "{}",
        expected.name
    );
}

fn add_field_metrics(field: &Field, metrics: &mut FieldMetrics) {
    metrics.nullable += usize::from(field.is_nullable());
    match field.data_type() {
        DataType::List(child) => {
            metrics.nested += 1;
            add_field_metrics(child, metrics);
        }
        DataType::Struct(children) => {
            metrics.nested += 1;
            for child in children {
                add_field_metrics(child, metrics);
            }
        }
        DataType::Utf8 | DataType::Binary => {
            metrics.leaves += 1;
            metrics.variable_width_leaves += 1;
        }
        _ => metrics.leaves += 1,
    }
}

fn assert_strictly_increasing(indices: &[usize]) {
    assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
}

fn independent_projection(change: &Change, indices: &[usize]) -> Change {
    let schema = Arc::new(Schema::new_with_metadata(
        indices
            .iter()
            .map(|index| change.schema().field(*index).clone())
            .collect::<Vec<_>>(),
        change.schema().metadata().clone(),
    ));
    let columns = indices
        .iter()
        .map(|index| Arc::clone(change.records().column(*index)))
        .collect::<Vec<ArrayRef>>();
    let records = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(change.num_rows())),
        )
        .unwrap()
    } else {
        RecordBatch::try_new(schema, columns).unwrap()
    };
    Change::try_new(records, change.diffs().clone()).unwrap()
}

fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}

fn ordered_checksum(entries: &[Vec<u8>]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    entries.iter().fold(OFFSET, |mut state, entry| {
        for byte in u64::try_from(entry.len())
            .unwrap()
            .to_le_bytes()
            .iter()
            .chain(entry)
        {
            state = (state ^ u64::from(*byte)).wrapping_mul(PRIME);
        }
        state
    })
}
