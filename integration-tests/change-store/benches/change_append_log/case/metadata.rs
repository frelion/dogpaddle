use std::collections::BTreeSet;

use arrow_array::Array;
use dogpaddle_change_store_integration::{
    PersonaWorkload, ProjectionProfile, SchemaDescriptor, WorkloadPersona,
};

use super::matrix::{BenchmarkCaseSpec, PageProfile};
use crate::{
    config::Config,
    model::{CaseMetadata, NumberSummary, ProjectionMetadata},
    regular_support::{checked_product, checked_sum},
};

const ONE_MEBIBYTE: usize = 1_024 * 1_024;
const SIXTEEN_MEBIBYTES: usize = 16 * ONE_MEBIBYTE;

pub(super) fn selected_profile(persona: WorkloadPersona) -> ProjectionProfile {
    if persona
        .projection_indices(ProjectionProfile::Sparse)
        .is_some()
    {
        ProjectionProfile::Sparse
    } else {
        ProjectionProfile::DiffOnly
    }
}

pub(super) fn build_metadata(
    spec: &BenchmarkCaseSpec,
    workload: &PersonaWorkload,
    selected_profiles: &[ProjectionProfile],
    selected_encoded: &[Vec<u8>],
) -> CaseMetadata {
    let schemas = workload
        .changes
        .iter()
        .map(dogpaddle_change_store_integration::GeneratedChange::schema_descriptor)
        .collect::<Vec<_>>();
    let schema_names = schemas
        .iter()
        .map(|schema| schema.name)
        .collect::<BTreeSet<_>>();
    let type_summaries = schemas
        .iter()
        .map(|schema| schema.type_summary)
        .collect::<BTreeSet<_>>();
    let encoded_entry_bytes = NumberSummary::from_values(workload.encoded.iter().map(Vec::len));
    let encoded_bytes_per_transaction =
        NumberSummary::from_values(workload.encoded.chunks(spec.changes_per_transaction).map(
            |transaction| {
                transaction
                    .iter()
                    .try_fold(0_usize, |total, entry| total.checked_add(entry.len()))
                    .expect("encoded transaction bytes fit usize")
            },
        ));
    let projected_encoded_entry_bytes =
        NumberSummary::from_values(selected_encoded.iter().map(Vec::len));
    let projected_encoded_bytes_per_transaction = NumberSummary::from_values(
        selected_encoded
            .chunks(spec.changes_per_transaction)
            .map(sum_entry_bytes),
    );
    let projected_encoded_bytes_per_sample = sum_entry_bytes(selected_encoded);
    let largest_entry = checked_sum(
        "largest AppendLog entry bytes",
        encoded_entry_bytes.max,
        size_of::<u64>(),
    );
    let (page_max_items, page_max_bytes) = page_limits(spec, largest_entry);
    let full_projection = projection_metadata(
        workload,
        &vec![ProjectionProfile::Identity; workload.changes.len()],
    );
    let selected_projection = projection_metadata(workload, selected_profiles);
    let descriptor = workload.requested_persona.descriptor();
    assert_eq!(descriptor.diff_model.as_str(), "insert_only");

    CaseMetadata {
        case_id: spec.case_id.clone(),
        family: spec.family.as_str(),
        workload: descriptor.name,
        matrix_axis: spec.matrix_axis,
        diff_model: descriptor.diff_model.as_str(),
        schema_count: schema_names.len(),
        schema_names: schema_names.into_iter().collect::<Vec<_>>().join(","),
        schema_sequence: descriptor
            .schemas
            .iter()
            .map(|schema| schema.name)
            .collect::<Vec<_>>()
            .join(">"),
        business_columns: summarize(&schemas, |schema| schema.business_columns),
        physical_columns: summarize(&schemas, |schema| schema.physical_columns),
        leaf_columns: summarize(&schemas, |schema| schema.leaf_columns),
        top_level_nullable_columns: summarize(&schemas, |schema| schema.top_level_nullable_columns),
        top_level_variable_width_columns: summarize(&schemas, |schema| {
            schema.top_level_variable_width_columns
        }),
        top_level_nested_columns: summarize(&schemas, |schema| schema.top_level_nested_columns),
        total_nullable_fields: summarize(&schemas, |schema| schema.total_nullable_fields),
        variable_width_leaf_columns: summarize(&schemas, |schema| {
            schema.variable_width_leaf_columns
        }),
        total_nested_fields: summarize(&schemas, |schema| schema.total_nested_fields),
        type_summary: type_summaries.into_iter().collect::<Vec<_>>().join("|"),
        rows_per_change: spec.rows_per_change,
        payload_bytes: spec.payload_bytes,
        changes_per_transaction: spec.changes_per_transaction,
        transactions_per_sample: spec.transactions_per_sample,
        changes_per_sample: spec.total_changes(),
        rows_per_sample: workload.total_rows(),
        encoded_entry_bytes,
        encoded_bytes_per_transaction,
        encoded_bytes_per_sample: workload.encoded_bytes,
        projected_encoded_entry_bytes,
        projected_encoded_bytes_per_transaction,
        projected_encoded_bytes_per_sample,
        scan_bytes_per_sample: workload.scan_bytes(),
        page_max_items,
        page_max_bytes,
        page_profile: spec.page.as_str(),
        full_projection,
        selected_projection,
    }
}

fn page_limits(spec: &BenchmarkCaseSpec, largest_entry: usize) -> (usize, usize) {
    match spec.page {
        PageProfile::Transaction => (
            spec.changes_per_transaction,
            checked_product(
                "transaction replay page byte limit",
                largest_entry,
                spec.changes_per_transaction,
            ),
        ),
        PageProfile::OneEntry => (1, largest_entry),
        PageProfile::ApproxOneMebibyte => (spec.total_changes(), ONE_MEBIBYTE.max(largest_entry)),
        PageProfile::ApproxSixteenMebibytes => {
            (spec.total_changes(), SIXTEEN_MEBIBYTES.max(largest_entry))
        }
    }
}

fn sum_entry_bytes(entries: &[Vec<u8>]) -> usize {
    entries
        .iter()
        .try_fold(0_usize, |total, entry| total.checked_add(entry.len()))
        .expect("encoded entry byte sum fits usize")
}

fn summarize(
    schemas: &[&SchemaDescriptor],
    field: impl Fn(&SchemaDescriptor) -> usize,
) -> NumberSummary {
    NumberSummary::from_values(schemas.iter().map(|schema| field(schema)))
}

fn projection_metadata(
    workload: &PersonaWorkload,
    profiles: &[ProjectionProfile],
) -> ProjectionMetadata {
    assert_eq!(workload.changes.len(), profiles.len());
    let mut profile_names = BTreeSet::new();
    let mut selected_columns = Vec::with_capacity(profiles.len());
    let mut total_columns = Vec::with_capacity(profiles.len());
    let mut column_selectivity = Vec::with_capacity(profiles.len());
    let mut selected_array_bytes = Vec::with_capacity(profiles.len());
    let mut total_array_bytes = Vec::with_capacity(profiles.len());
    let mut array_bytes_selectivity = Vec::with_capacity(profiles.len());

    for (generated, profile) in workload.changes.iter().zip(profiles) {
        profile_names.insert(profile.as_str());
        let indices = generated
            .persona
            .projection_indices(*profile)
            .expect("reported projection is legal");
        let total = generated.change.records().num_columns();
        let diff_bytes = generated.change.diffs().get_buffer_memory_size();
        let business_bytes = generated
            .change
            .records()
            .columns()
            .iter()
            .map(Array::get_buffer_memory_size)
            .sum::<usize>();
        let selected_business_bytes = indices
            .iter()
            .map(|index| {
                generated
                    .change
                    .records()
                    .column(*index)
                    .get_buffer_memory_size()
            })
            .sum::<usize>();
        let all_array_bytes = checked_sum("full Arrow array bytes", diff_bytes, business_bytes);
        let projected_array_bytes = checked_sum(
            "projected Arrow array bytes",
            diff_bytes,
            selected_business_bytes,
        );

        selected_columns.push(indices.len());
        total_columns.push(total);
        column_selectivity.push(ratio_basis_points(indices.len(), total));
        selected_array_bytes.push(projected_array_bytes);
        total_array_bytes.push(all_array_bytes);
        array_bytes_selectivity.push(ratio_basis_points(projected_array_bytes, all_array_bytes));
    }

    ProjectionMetadata {
        profile: profile_names.into_iter().collect::<Vec<_>>().join("+"),
        selected_columns: NumberSummary::from_values(selected_columns),
        total_columns: NumberSummary::from_values(total_columns),
        column_selectivity_basis_points: NumberSummary::from_values(column_selectivity),
        selected_array_bytes: NumberSummary::from_values(selected_array_bytes),
        total_array_bytes: NumberSummary::from_values(total_array_bytes),
        array_bytes_selectivity_basis_points: NumberSummary::from_values(array_bytes_selectivity),
    }
}

fn ratio_basis_points(selected: usize, total: usize) -> usize {
    if total == 0 {
        return 10_000;
    }
    selected
        .checked_mul(10_000)
        .expect("selectivity numerator fits usize")
        / total
}

pub(super) fn validate_working_set(
    config: &Config,
    workload: &PersonaWorkload,
    selected_encoded: &[Vec<u8>],
) {
    let projected_bytes = selected_encoded
        .iter()
        .map(Vec::len)
        .try_fold(0_usize, usize::checked_add)
        .expect("projected encoded bytes fit usize");
    let arrow_bytes = workload
        .changes
        .iter()
        .try_fold(0_usize, |total, generated| {
            let change_bytes = generated
                .change
                .records()
                .columns()
                .iter()
                .map(Array::get_buffer_memory_size)
                .try_fold(
                    generated.change.diffs().get_buffer_memory_size(),
                    usize::checked_add,
                )?;
            total.checked_add(change_bytes)
        })
        .expect("Arrow workload bytes fit usize");
    // Integrated production retains a freshly encoded copy until the sample
    // clock stops so allocator destruction cannot pollute durability.
    let estimate = [
        workload.encoded_bytes,
        workload.encoded_bytes,
        projected_bytes,
        arrow_bytes,
    ]
    .into_iter()
    .try_fold(0_usize, usize::checked_add)
    .expect("benchmark working-set estimate fits usize");
    assert!(
        estimate <= config.max_working_set_bytes,
        "estimated fixture working set {estimate} exceeds configured maximum {}; raise DOGPADDLE_CHANGE_STORE_BENCH_MAX_WORKING_SET_BYTES deliberately",
        config.max_working_set_bytes
    );
}

pub(super) fn preflight_dimensions(spec: &BenchmarkCaseSpec) {
    assert!(spec.transactions_per_sample >= 2);
    let arrow_offset_max = usize::try_from(i32::MAX).expect("i32::MAX fits usize");
    let payload_per_change = checked_product(
        "payload bytes per Change",
        spec.rows_per_change,
        spec.payload_bytes,
    );
    assert!(
        payload_per_change <= arrow_offset_max,
        "one variable-width column exceeds Arrow's i32 offset limit"
    );
    assert!(
        spec.rows_per_change < arrow_offset_max,
        "rows per Change exceed Arrow's i32 offset count"
    );
    u64::try_from(checked_product(
        "total benchmark rows",
        spec.total_changes(),
        spec.rows_per_change,
    ))
    .expect("total benchmark rows fit u64");
}
