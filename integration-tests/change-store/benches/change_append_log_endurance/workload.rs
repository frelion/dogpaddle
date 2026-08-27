use arrow_array::Array;
use dogpaddle_change::{Change, ChangeProjection, encode_change};
use dogpaddle_change_store_integration::{
    ChangeWorkloadSpec, GeneratedChange, ProjectionProfile, WorkloadPersona,
    generate_persona_change,
};

use super::config::{Config, WorkloadMode};

const EVENT_SEED: u64 = 0x4d59_5df4_d0f3_3173;

pub(super) struct WorkloadStream<'config> {
    config: &'config Config,
    mode: WorkloadMode,
    next_ordinal: usize,
    next_event: u64,
}

pub(super) struct PreparedEntry {
    pub(super) generated: GeneratedChange,
    generation_seed: u64,
    pub(super) spec: ChangeWorkloadSpec,
    pub(super) expected_encoded: Vec<u8>,
}

pub(super) struct PreparedBatch {
    pub(super) entries: Vec<PreparedEntry>,
    pub(super) encoded_bytes: usize,
    pub(super) rows: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EntryOracle {
    pub(super) offset: u64,
    pub(super) ordinal: usize,
    pub(super) generation_seed: u64,
    pub(super) event_start: u64,
    pub(super) spec: ChangeWorkloadSpec,
    pub(super) concrete_persona: WorkloadPersona,
    pub(super) encoded_len: usize,
}

pub(super) struct ExpectedPageEntry {
    pub(super) offset: u64,
    pub(super) full: Change,
    pub(super) projection: ChangeProjection,
    pub(super) projected: Change,
    pub(super) concrete_persona: WorkloadPersona,
    pub(super) encoded_len: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ProjectionMeasurement {
    pub(super) selected_columns: usize,
    pub(super) total_columns: usize,
    pub(super) column_selectivity_basis_points: usize,
    pub(super) selected_array_bytes: usize,
    pub(super) total_array_bytes: usize,
    pub(super) array_bytes_selectivity_basis_points: usize,
}

impl<'config> WorkloadStream<'config> {
    pub(super) const fn new(config: &'config Config, mode: WorkloadMode) -> Self {
        Self {
            config,
            mode,
            next_ordinal: 0,
            next_event: EVENT_SEED,
        }
    }

    pub(super) fn prepare(&mut self, entries: usize) -> PreparedBatch {
        assert!(entries > 0, "a prepared endurance batch is non-empty");
        let mut prepared = Vec::with_capacity(entries);
        let mut encoded_bytes = 0_usize;
        let mut rows = 0_usize;
        for _ in 0..entries {
            let ordinal = self.next_ordinal;
            let event_start = self.next_event;
            let spec = self.config.spec(self.mode, ordinal);
            let generated =
                generate_persona_change(self.mode.persona(), ordinal, event_start, spec);
            let expected_encoded =
                encode_change(&generated.change).expect("encode generated endurance Change oracle");
            encoded_bytes = encoded_bytes
                .checked_add(expected_encoded.len())
                .expect("prepared encoded bytes fit usize");
            rows = rows
                .checked_add(spec.rows)
                .expect("prepared row count fits usize");
            self.next_ordinal = self
                .next_ordinal
                .checked_add(1)
                .expect("endurance ordinal fits usize");
            self.next_event = generated
                .event_start
                .checked_add(u64::try_from(spec.rows).expect("Change rows fit u64"))
                .expect("endurance event id fits u64");
            prepared.push(PreparedEntry {
                generated,
                generation_seed: event_start,
                spec,
                expected_encoded,
            });
        }
        PreparedBatch {
            entries: prepared,
            encoded_bytes,
            rows,
        }
    }
}

impl PreparedBatch {
    pub(super) fn extend(&mut self, other: Self) {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(other.encoded_bytes)
            .expect("combined encoded bytes fit usize");
        self.rows = self
            .rows
            .checked_add(other.rows)
            .expect("combined row count fits usize");
        self.entries.extend(other.entries);
    }

    pub(super) fn expected_encoded(&self) -> Vec<Vec<u8>> {
        self.entries
            .iter()
            .map(|entry| entry.expected_encoded.clone())
            .collect()
    }

    pub(super) fn encode_for_producer(&self) -> Vec<Vec<u8>> {
        self.entries
            .iter()
            .map(|entry| {
                encode_change(&entry.generated.change).expect("encode producer endurance Change")
            })
            .collect()
    }

    pub(super) fn assert_encoded(&self, actual: &[Vec<u8>]) {
        assert_eq!(actual.len(), self.entries.len());
        for (actual, expected) in actual.iter().zip(&self.entries) {
            assert_eq!(actual, &expected.expected_encoded);
        }
    }

    pub(super) fn into_oracles(self, start_offset: u64) -> Vec<EntryOracle> {
        self.entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| EntryOracle {
                offset: start_offset
                    .checked_add(u64::try_from(index).expect("batch index fits u64"))
                    .expect("endurance offset fits u64"),
                ordinal: entry.generated.ordinal,
                generation_seed: entry.generation_seed,
                event_start: entry.generated.event_start,
                spec: entry.spec,
                concrete_persona: entry.generated.persona,
                encoded_len: entry.expected_encoded.len(),
            })
            .collect()
    }
}

impl PreparedEntry {
    pub(super) fn projection_measurement(&self) -> ProjectionMeasurement {
        let profile = projection_profile(self.generated.persona);
        let selected = self
            .generated
            .schema_descriptor()
            .projection(profile)
            .expect("selected projection profile is legal");
        let total_columns = self.generated.change.records().num_columns();
        let diff_bytes = self.generated.change.diffs().get_array_memory_size();
        let logical_bytes = self
            .generated
            .change
            .records()
            .columns()
            .iter()
            .map(Array::get_array_memory_size)
            .sum::<usize>();
        let selected_logical_bytes = selected
            .iter()
            .map(|index| {
                self.generated
                    .change
                    .records()
                    .column(*index)
                    .get_array_memory_size()
            })
            .sum::<usize>();
        let total_array_bytes = diff_bytes
            .checked_add(logical_bytes)
            .expect("total array memory fits usize");
        let selected_array_bytes = diff_bytes
            .checked_add(selected_logical_bytes)
            .expect("selected array memory fits usize");
        ProjectionMeasurement {
            selected_columns: selected.len(),
            total_columns,
            column_selectivity_basis_points: ratio_basis_points(selected.len(), total_columns),
            selected_array_bytes,
            total_array_bytes,
            array_bytes_selectivity_basis_points: ratio_basis_points(
                selected_array_bytes,
                total_array_bytes,
            ),
        }
    }
}

impl EntryOracle {
    pub(super) fn regenerate(self, requested: WorkloadPersona) -> GeneratedChange {
        let generated =
            generate_persona_change(requested, self.ordinal, self.generation_seed, self.spec);
        assert_eq!(generated.persona, self.concrete_persona);
        assert_eq!(generated.event_start, self.event_start);
        generated
    }

    pub(super) fn regenerate_with_encoded(
        self,
        requested: WorkloadPersona,
    ) -> (GeneratedChange, Vec<u8>) {
        let generated = self.regenerate(requested);
        let encoded = encode_change(&generated.change).expect("re-encode endurance raw oracle");
        assert_eq!(encoded.len(), self.encoded_len);
        (generated, encoded)
    }

    pub(super) fn expected_page_entry(self, requested: WorkloadPersona) -> ExpectedPageEntry {
        let generated = self.regenerate(requested);
        let profile = projection_profile(generated.persona);
        let indices = generated
            .schema_descriptor()
            .projection(profile)
            .expect("selected projection profile is legal for concrete persona");
        let projection =
            ChangeProjection::try_new(generated.change.schema(), indices.iter().copied())
                .expect("persona projection binds to its generated Schema");
        let projected = generated
            .change
            .try_project(&projection)
            .expect("project generated endurance oracle");
        ExpectedPageEntry {
            offset: self.offset,
            full: generated.change,
            projection,
            projected,
            concrete_persona: self.concrete_persona,
            encoded_len: self.encoded_len,
        }
    }
}

pub(super) fn projection_profile(persona: WorkloadPersona) -> ProjectionProfile {
    let schema = persona.descriptor().schemas[0];
    [
        ProjectionProfile::Sparse,
        ProjectionProfile::KeyOnly,
        ProjectionProfile::DiffOnly,
    ]
    .into_iter()
    .find(|profile| schema.projection(*profile).is_some())
    .expect("every persona has at least a diff-only projection")
}

pub(super) fn projection_metadata(persona: WorkloadPersona) -> Vec<String> {
    persona
        .descriptor()
        .schemas
        .iter()
        .map(|schema| {
            let profile = [
                ProjectionProfile::Sparse,
                ProjectionProfile::KeyOnly,
                ProjectionProfile::DiffOnly,
            ]
            .into_iter()
            .find(|profile| schema.projection(*profile).is_some())
            .expect("every workload Schema supports diff-only projection");
            let indices = schema
                .projection(profile)
                .expect("selected projection is legal");
            format!("{}:{}:{indices:?}", schema.name, profile.as_str())
        })
        .collect()
}

fn ratio_basis_points(numerator: usize, denominator: usize) -> usize {
    if denominator == 0 {
        return 0;
    }
    numerator
        .checked_mul(10_000)
        .expect("selectivity numerator fits usize")
        / denominator
}
