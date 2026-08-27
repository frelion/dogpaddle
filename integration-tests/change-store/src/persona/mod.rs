mod churn;
mod descriptor;
mod generator;

pub use churn::{
    ChurnEvent, ChurnModel, ChurnValidationError, churn_changes, flatten_churn_changes,
    valid_churn_events, validate_churn,
};
pub use descriptor::{
    DiffModel, ProjectionDescriptor, ProjectionProfile, SchemaDescriptor, WorkloadDescriptor,
    WorkloadPersona,
};

use dogpaddle_change::{Change, encode_change};

use generator::{event_span, make_change, validate_change_event_ids, validate_workload_event_ids};

/// Dimensions for one generated Change entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangeWorkloadSpec {
    /// Logical rows in the Change.
    pub rows: usize,
    /// Target bytes per value in the persona's primary Binary payload.
    pub payload_bytes: usize,
}

impl ChangeWorkloadSpec {
    /// Creates a non-empty Change specification.
    ///
    /// # Panics
    ///
    /// Panics when `rows` is zero.
    #[must_use]
    pub const fn new(rows: usize, payload_bytes: usize) -> Self {
        assert!(rows > 0, "a persisted Change must contain a row");
        Self {
            rows,
            payload_bytes,
        }
    }
}

/// One generated Change and the concrete persona selected for it.
pub struct GeneratedChange {
    /// Concrete persona. This is never [`WorkloadPersona::Heterogeneous`].
    pub persona: WorkloadPersona,
    /// Entry position within the generated workload.
    pub ordinal: usize,
    /// First logical event identifier present in the generated Change.
    pub event_start: u64,
    /// Generated valid Change.
    pub change: Change,
}

impl GeneratedChange {
    /// Returns the concrete Schema descriptor.
    #[must_use]
    pub fn schema_descriptor(&self) -> &'static SchemaDescriptor {
        &self.persona.descriptor().schemas[0]
    }
}

/// A deterministic set of logical Changes and their complete IPC entries.
pub struct PersonaWorkload {
    /// Persona requested by the caller.
    pub requested_persona: WorkloadPersona,
    /// Logical Changes and their concrete Schema identities.
    pub changes: Vec<GeneratedChange>,
    /// One complete Arrow IPC Stream per generated Change.
    pub encoded: Vec<Vec<u8>>,
    /// Sum of encoded entry bytes, excluding `AppendLog` offsets.
    pub encoded_bytes: usize,
}

impl PersonaWorkload {
    /// Returns the total number of logical rows.
    ///
    /// # Panics
    ///
    /// Panics if externally mutated workload dimensions overflow `usize`.
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.changes
            .iter()
            .try_fold(0_usize, |total, generated| {
                total.checked_add(generated.change.num_rows())
            })
            .expect("workload rows fit usize")
    }

    /// Returns the exact bytes charged by one complete `AppendLog` scan.
    ///
    /// # Panics
    ///
    /// Panics if externally mutated workload dimensions overflow `usize`.
    #[must_use]
    pub fn scan_bytes(&self) -> usize {
        self.encoded_bytes
            .checked_add(
                self.encoded
                    .len()
                    .checked_mul(size_of::<u64>())
                    .expect("offset bytes fit usize"),
            )
            .expect("scan bytes fit usize")
    }

    /// Returns an order-sensitive checksum over complete encoded entries.
    ///
    /// # Panics
    ///
    /// Panics if an encoded entry length cannot be represented by `u64`.
    #[must_use]
    pub fn order_checksum(&self) -> u64 {
        self.encoded.iter().fold(FNV_OFFSET, |state, entry| {
            hash_bytes(
                hash_u64(
                    state,
                    u64::try_from(entry.len()).expect("entry length fits u64"),
                ),
                entry,
            )
        })
    }
}

/// Generates one deterministic, insert-only Change.
///
/// `Heterogeneous` deterministically resolves to a concrete persona by
/// `ordinal`.
///
/// # Panics
///
/// Panics when dimensions exceed Arrow limits or constructing the fixture
/// violates the Change contract.
#[must_use]
pub fn generate_persona_change(
    persona: WorkloadPersona,
    ordinal: usize,
    event_start: u64,
    spec: ChangeWorkloadSpec,
) -> GeneratedChange {
    let concrete = persona.concrete_at(ordinal);
    validate_change_event_ids(concrete, event_start, spec);
    let change = make_change(concrete, event_start, spec);
    assert!(change.diffs().values().iter().all(|diff| *diff == 1));
    let actual_event_start = if matches!(concrete, WorkloadPersona::SlicedMixed16) {
        event_start
            .checked_add(1)
            .expect("sliced event start fits u64")
    } else {
        event_start
    };
    GeneratedChange {
        persona: concrete,
        ordinal,
        event_start: actual_event_start,
        change,
    }
}

/// Generates and encodes a deterministic workload.
///
/// # Panics
///
/// Panics when `specs` is empty, dimensions overflow, or an unexpected Change
/// encoding failure occurs.
#[must_use]
pub fn generate_persona_workload(
    persona: WorkloadPersona,
    seed: u64,
    specs: &[ChangeWorkloadSpec],
) -> PersonaWorkload {
    assert!(!specs.is_empty(), "a workload must contain a Change");
    validate_workload_event_ids(persona, seed, specs);
    let mut event_start = seed;
    let mut changes = Vec::with_capacity(specs.len());
    let mut encoded = Vec::with_capacity(specs.len());
    let mut encoded_bytes = 0_usize;
    for (ordinal, spec) in specs.iter().copied().enumerate() {
        let generated = generate_persona_change(persona, ordinal, event_start, spec);
        let bytes = encode_change(&generated.change).expect("encode generated persona Change");
        encoded_bytes = encoded_bytes
            .checked_add(bytes.len())
            .expect("encoded workload bytes fit usize");
        let event_span = event_span(generated.persona, spec.rows);
        changes.push(generated);
        encoded.push(bytes);
        if ordinal + 1 < specs.len() {
            event_start = event_start
                .checked_add(u64::try_from(event_span).expect("event span fits u64"))
                .expect("workload event identifiers fit u64");
        }
    }
    PersonaWorkload {
        requested_persona: persona,
        changes,
        encoded,
        encoded_bytes,
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn hash_u64(state: u64, value: u64) -> u64 {
    hash_bytes(state, &value.to_le_bytes())
}

fn hash_bytes(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state = (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
    }
    state
}
