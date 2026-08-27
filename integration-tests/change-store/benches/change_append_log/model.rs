use std::time::Duration;

#[derive(Clone, Copy)]
pub(crate) struct NumberSummary {
    pub(crate) min: usize,
    pub(crate) p50: usize,
    pub(crate) max: usize,
}

impl NumberSummary {
    pub(crate) fn from_values(values: impl IntoIterator<Item = usize>) -> Self {
        let mut values = values.into_iter().collect::<Vec<_>>();
        assert!(!values.is_empty(), "a benchmark summary needs one value");
        values.sort_unstable();
        Self {
            min: values[0],
            p50: values[values.len() / 2],
            max: *values.last().expect("non-empty numeric summary"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ProjectionMetadata {
    pub(crate) profile: String,
    pub(crate) selected_columns: NumberSummary,
    pub(crate) total_columns: NumberSummary,
    pub(crate) column_selectivity_basis_points: NumberSummary,
    pub(crate) selected_array_bytes: NumberSummary,
    pub(crate) total_array_bytes: NumberSummary,
    pub(crate) array_bytes_selectivity_basis_points: NumberSummary,
}

#[derive(Clone)]
pub(crate) struct CaseMetadata {
    pub(crate) case_id: String,
    pub(crate) family: &'static str,
    pub(crate) workload: &'static str,
    pub(crate) matrix_axis: &'static str,
    pub(crate) diff_model: &'static str,
    pub(crate) schema_count: usize,
    pub(crate) schema_names: String,
    pub(crate) schema_sequence: String,
    pub(crate) business_columns: NumberSummary,
    pub(crate) physical_columns: NumberSummary,
    pub(crate) leaf_columns: NumberSummary,
    pub(crate) top_level_nullable_columns: NumberSummary,
    pub(crate) top_level_variable_width_columns: NumberSummary,
    pub(crate) top_level_nested_columns: NumberSummary,
    pub(crate) total_nullable_fields: NumberSummary,
    pub(crate) variable_width_leaf_columns: NumberSummary,
    pub(crate) total_nested_fields: NumberSummary,
    pub(crate) type_summary: String,
    pub(crate) rows_per_change: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) changes_per_transaction: usize,
    pub(crate) transactions_per_sample: usize,
    pub(crate) changes_per_sample: usize,
    pub(crate) rows_per_sample: usize,
    pub(crate) encoded_entry_bytes: NumberSummary,
    pub(crate) encoded_bytes_per_transaction: NumberSummary,
    pub(crate) encoded_bytes_per_sample: usize,
    pub(crate) projected_encoded_entry_bytes: NumberSummary,
    pub(crate) projected_encoded_bytes_per_transaction: NumberSummary,
    pub(crate) projected_encoded_bytes_per_sample: usize,
    pub(crate) scan_bytes_per_sample: usize,
    pub(crate) page_max_items: usize,
    pub(crate) page_max_bytes: usize,
    pub(crate) page_profile: &'static str,
    pub(crate) full_projection: ProjectionMetadata,
    pub(crate) selected_projection: ProjectionMetadata,
}

#[derive(Clone)]
pub(crate) struct ScenarioMetadata {
    pub(crate) scenario: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) variant: &'static str,
    pub(crate) lifecycle: &'static str,
    pub(crate) timing_boundary: &'static str,
    pub(crate) headline: bool,
    pub(crate) projection: Option<ProjectionMetadata>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Measurement {
    pub(crate) elapsed: Duration,
    pub(crate) pages: usize,
    pub(crate) checksum: u64,
}
