mod matrix;
mod metadata;

use dogpaddle_change::{Change, ChangeProjection, decode_change, encode_change};
use dogpaddle_change_store_integration::{
    ChangeWorkloadSpec, PersonaWorkload, ProjectionProfile, assert_change_eq,
    generate_persona_workload,
};

use matrix::ProjectionChoice;
pub(crate) use matrix::{BenchmarkCaseSpec, CaseFamily, benchmark_specs};
use metadata::{build_metadata, preflight_dimensions, selected_profile, validate_working_set};

use crate::{config::Config, model::CaseMetadata};

pub(crate) struct BenchmarkCase {
    pub(crate) family: CaseFamily,
    pub(crate) metadata: CaseMetadata,
    pub(crate) workload: PersonaWorkload,
    pub(crate) selected_projections: Vec<ChangeProjection>,
    pub(crate) selected_expected: Vec<Change>,
    pub(crate) selected_encoded: Vec<Vec<u8>>,
}

pub(crate) fn build_case(
    config: &Config,
    spec: &BenchmarkCaseSpec,
    case_index: usize,
) -> BenchmarkCase {
    preflight_dimensions(spec);
    let dimensions = vec![
        ChangeWorkloadSpec::new(spec.rows_per_change, spec.payload_bytes);
        spec.total_changes()
    ];
    let seed = 0x5eed_0000_0000_0000_u64
        .checked_add(u64::try_from(case_index).expect("case index fits u64") << 40)
        .expect("benchmark seed fits u64");
    let workload = generate_persona_workload(spec.persona, seed, &dimensions);
    assert_eq!(workload.requested_persona, spec.persona);
    assert_eq!(workload.changes.len(), spec.total_changes());
    assert_eq!(
        workload.total_rows(),
        spec.total_changes() * spec.rows_per_change
    );

    for (generated, encoded) in workload.changes.iter().zip(&workload.encoded) {
        assert!(
            generated
                .change
                .diffs()
                .values()
                .iter()
                .all(|diff| *diff == 1),
            "performance workloads must be valid insert-only streams"
        );
        let decoded = decode_change(encoded).expect("decode generated benchmark Change");
        assert_change_eq(&decoded, &generated.change);
    }

    let selected_profiles = workload
        .changes
        .iter()
        .map(|generated| match spec.projection {
            ProjectionChoice::Fixed(profile) => profile,
            ProjectionChoice::SparsePerSchema => selected_profile(generated.persona),
        })
        .collect::<Vec<ProjectionProfile>>();
    let selected_projections = workload
        .changes
        .iter()
        .zip(&selected_profiles)
        .map(|(generated, profile)| {
            let indices = generated
                .persona
                .projection_indices(*profile)
                .expect("selected benchmark projection is legal");
            ChangeProjection::try_new(generated.change.schema(), indices.iter().copied())
                .expect("construct schema-bound benchmark projection")
        })
        .collect::<Vec<_>>();
    let selected_expected = workload
        .changes
        .iter()
        .zip(&selected_projections)
        .map(|(generated, projection)| {
            generated
                .change
                .try_project(projection)
                .expect("project generated benchmark Change")
        })
        .collect::<Vec<_>>();
    let selected_encoded = selected_expected
        .iter()
        .map(|change| encode_change(change).expect("encode projected benchmark Change"))
        .collect::<Vec<_>>();

    let metadata = build_metadata(spec, &workload, &selected_profiles, &selected_encoded);
    validate_working_set(config, &workload, &selected_encoded);
    if matches!(
        spec.family,
        CaseFamily::Anchor | CaseFamily::ProjectionReplay | CaseFamily::PageReplay
    ) {
        assert!(
            metadata.scan_bytes_per_sample > metadata.page_max_bytes
                || metadata.changes_per_sample > metadata.page_max_items,
            "replay case {} must span multiple pages",
            metadata.case_id
        );
    }
    BenchmarkCase {
        family: spec.family,
        metadata,
        workload,
        selected_projections,
        selected_expected,
        selected_encoded,
    }
}
