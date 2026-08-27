use dogpaddle_bench_protocol::BenchmarkProfile;
use dogpaddle_change_store_integration::{ProjectionProfile, WorkloadPersona};

use crate::{config::Config, regular_support::checked_product};

const ROW_AXIS_TOTAL_ROWS: usize = 32_768;
const ROW_AXIS_VALUES: [usize; 4] = [1, 64, 1_024, 16_384];
const CHANGE_AXIS_TOTAL_CHANGES: usize = 256;
const CHANGE_AXIS_VALUES: [usize; 4] = [1, 8, 32, 128];
const PAYLOAD_AXIS_VALUES: [usize; 3] = [128, 1_024, 8_192];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CaseFamily {
    Anchor,
    Producer,
    ProjectionReplay,
    PageReplay,
}

impl CaseFamily {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Anchor => "anchor",
            Self::Producer => "producer",
            Self::ProjectionReplay => "projection_replay",
            Self::PageReplay => "page_replay",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ProjectionChoice {
    Fixed(ProjectionProfile),
    SparsePerSchema,
}

#[derive(Clone, Copy)]
pub(super) enum PageProfile {
    Transaction,
    OneEntry,
    ApproxOneMebibyte,
    ApproxSixteenMebibytes,
}

impl PageProfile {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Transaction => "changes_per_transaction",
            Self::OneEntry => "one_entry",
            Self::ApproxOneMebibyte => "approximately_1_mib",
            Self::ApproxSixteenMebibytes => "approximately_16_mib",
        }
    }
}

#[derive(Clone, Copy)]
struct Dimensions {
    rows_per_change: usize,
    changes_per_transaction: usize,
    transactions_per_sample: usize,
    payload_bytes: usize,
}

impl Dimensions {
    const fn anchor(config: &Config) -> Self {
        Self {
            rows_per_change: config.rows_per_change,
            changes_per_transaction: config.changes_per_transaction,
            transactions_per_sample: config.transactions_per_sample,
            payload_bytes: config.payload_bytes,
        }
    }
}

pub(crate) struct BenchmarkCaseSpec {
    pub(super) case_id: String,
    pub(super) family: CaseFamily,
    pub(super) matrix_axis: &'static str,
    pub(super) persona: WorkloadPersona,
    pub(super) rows_per_change: usize,
    pub(super) changes_per_transaction: usize,
    pub(super) transactions_per_sample: usize,
    pub(super) payload_bytes: usize,
    pub(super) projection: ProjectionChoice,
    pub(super) page: PageProfile,
}

impl BenchmarkCaseSpec {
    pub(super) fn total_changes(&self) -> usize {
        checked_product(
            "case Changes per sample",
            self.changes_per_transaction,
            self.transactions_per_sample,
        )
    }
}

pub(crate) fn benchmark_specs(config: &Config) -> Vec<BenchmarkCaseSpec> {
    match config.profile {
        BenchmarkProfile::Smoke => smoke_specs(config),
        BenchmarkProfile::Reference => reference_specs(config),
    }
}

fn smoke_specs(config: &Config) -> Vec<BenchmarkCaseSpec> {
    let anchor = Dimensions::anchor(config);
    vec![
        anchor_spec(config),
        producer_spec(
            "smoke_schema_nested_event_8".into(),
            "schema_shape",
            WorkloadPersona::NestedEvent8,
            anchor,
        ),
        producer_spec(
            "smoke_rows_nonanchor".into(),
            "rows_per_change",
            WorkloadPersona::MixedEvent16,
            Dimensions {
                rows_per_change: config
                    .rows_per_change
                    .checked_add(1)
                    .expect("smoke row-axis value fits usize"),
                ..anchor
            },
        ),
        producer_spec(
            "smoke_changes_per_tx_nonanchor".into(),
            "changes_per_transaction",
            WorkloadPersona::MixedEvent16,
            Dimensions {
                changes_per_transaction: config
                    .changes_per_transaction
                    .checked_add(1)
                    .expect("smoke Change-axis value fits usize"),
                transactions_per_sample: 2,
                ..anchor
            },
        ),
        producer_spec(
            "smoke_payload_nonanchor".into(),
            "payload_bytes",
            WorkloadPersona::BlobEvent4,
            Dimensions {
                payload_bytes: config
                    .payload_bytes
                    .checked_add(1)
                    .expect("smoke payload-axis value fits usize"),
                ..anchor
            },
        ),
        replay_spec(
            "smoke_projection_blob_payload_only".into(),
            CaseFamily::ProjectionReplay,
            "projection_profile",
            WorkloadPersona::BlobEvent4,
            anchor,
            ProjectionChoice::Fixed(ProjectionProfile::PayloadOnly),
            PageProfile::Transaction,
        ),
        replay_spec(
            "smoke_page_one_entry".into(),
            CaseFamily::PageReplay,
            "replay_page_limit",
            WorkloadPersona::MixedEvent16,
            anchor,
            ProjectionChoice::Fixed(ProjectionProfile::Sparse),
            PageProfile::OneEntry,
        ),
    ]
}

fn reference_specs(config: &Config) -> Vec<BenchmarkCaseSpec> {
    let anchor = Dimensions::anchor(config);
    let mut specs = vec![anchor_spec(config)];

    for persona in WorkloadPersona::ALL {
        if persona == WorkloadPersona::MixedEvent16 {
            continue;
        }
        specs.push(producer_spec(
            format!("schema_{}", persona.name()),
            if persona == WorkloadPersona::Heterogeneous {
                "schema_sequence"
            } else {
                "schema_shape"
            },
            persona,
            anchor,
        ));
    }

    for rows_per_change in ROW_AXIS_VALUES {
        assert_eq!(ROW_AXIS_TOTAL_ROWS % rows_per_change, 0);
        let total_changes = ROW_AXIS_TOTAL_ROWS / rows_per_change;
        assert_eq!(total_changes % 2, 0);
        let changes_per_transaction = total_changes / 2;
        specs.push(producer_spec(
            format!("rows_{rows_per_change}"),
            "rows_per_change",
            WorkloadPersona::MixedEvent16,
            Dimensions {
                rows_per_change,
                changes_per_transaction,
                transactions_per_sample: 2,
                ..anchor
            },
        ));
    }

    for changes_per_transaction in CHANGE_AXIS_VALUES {
        assert_eq!(CHANGE_AXIS_TOTAL_CHANGES % changes_per_transaction, 0);
        let transactions_per_sample = CHANGE_AXIS_TOTAL_CHANGES / changes_per_transaction;
        assert!(transactions_per_sample >= 2);
        specs.push(producer_spec(
            format!("changes_per_tx_{changes_per_transaction}"),
            "changes_per_transaction",
            WorkloadPersona::MixedEvent16,
            Dimensions {
                changes_per_transaction,
                transactions_per_sample,
                ..anchor
            },
        ));
    }

    for payload_bytes in PAYLOAD_AXIS_VALUES {
        specs.push(producer_spec(
            format!("blob_payload_{payload_bytes}"),
            "payload_bytes",
            WorkloadPersona::BlobEvent4,
            Dimensions {
                rows_per_change: 64,
                changes_per_transaction: 8,
                transactions_per_sample: 4,
                payload_bytes,
            },
        ));
    }

    for persona in [
        WorkloadPersona::MixedEvent16,
        WorkloadPersona::BlobEvent4,
        WorkloadPersona::NestedEvent8,
    ] {
        for profile in ProjectionProfile::ALL {
            specs.push(replay_spec(
                format!("projection_{}_{}", persona.name(), profile.as_str()),
                CaseFamily::ProjectionReplay,
                "projection_profile",
                persona,
                anchor,
                ProjectionChoice::Fixed(profile),
                PageProfile::Transaction,
            ));
        }
    }

    for page in [
        PageProfile::OneEntry,
        PageProfile::ApproxOneMebibyte,
        PageProfile::ApproxSixteenMebibytes,
    ] {
        specs.push(replay_spec(
            format!("page_{}", page.as_str()),
            CaseFamily::PageReplay,
            "replay_page_limit",
            WorkloadPersona::MixedEvent16,
            anchor,
            ProjectionChoice::Fixed(ProjectionProfile::Sparse),
            page,
        ));
    }
    specs
}

fn anchor_spec(config: &Config) -> BenchmarkCaseSpec {
    let dimensions = Dimensions::anchor(config);
    BenchmarkCaseSpec {
        case_id: "anchor_mixed_event_16".into(),
        family: CaseFamily::Anchor,
        matrix_axis: "anchor",
        persona: WorkloadPersona::MixedEvent16,
        rows_per_change: dimensions.rows_per_change,
        changes_per_transaction: dimensions.changes_per_transaction,
        transactions_per_sample: dimensions.transactions_per_sample,
        payload_bytes: dimensions.payload_bytes,
        projection: ProjectionChoice::Fixed(ProjectionProfile::Sparse),
        page: PageProfile::Transaction,
    }
}

fn producer_spec(
    case_id: String,
    matrix_axis: &'static str,
    persona: WorkloadPersona,
    dimensions: Dimensions,
) -> BenchmarkCaseSpec {
    BenchmarkCaseSpec {
        case_id,
        family: CaseFamily::Producer,
        matrix_axis,
        persona,
        rows_per_change: dimensions.rows_per_change,
        changes_per_transaction: dimensions.changes_per_transaction,
        transactions_per_sample: dimensions.transactions_per_sample,
        payload_bytes: dimensions.payload_bytes,
        projection: ProjectionChoice::SparsePerSchema,
        page: PageProfile::Transaction,
    }
}

fn replay_spec(
    case_id: String,
    family: CaseFamily,
    matrix_axis: &'static str,
    persona: WorkloadPersona,
    dimensions: Dimensions,
    projection: ProjectionChoice,
    page: PageProfile,
) -> BenchmarkCaseSpec {
    BenchmarkCaseSpec {
        case_id,
        family,
        matrix_axis,
        persona,
        rows_per_change: dimensions.rows_per_change,
        changes_per_transaction: dimensions.changes_per_transaction,
        transactions_per_sample: dimensions.transactions_per_sample,
        payload_bytes: dimensions.payload_bytes,
        projection,
        page,
    }
}
