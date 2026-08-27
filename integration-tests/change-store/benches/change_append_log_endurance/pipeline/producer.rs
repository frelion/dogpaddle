use std::{collections::VecDeque, ops::Range, time::Duration};

use super::{
    super::{
        config::Config,
        oracle::StreamOracle,
        workload::{EntryOracle, PreparedBatch, ProjectionMeasurement},
    },
    lifecycle::Session,
};

pub(super) struct ProjectionSamples {
    pub(super) selected_columns: Vec<usize>,
    pub(super) total_columns: Vec<usize>,
    pub(super) column_selectivity_basis_points: Vec<usize>,
    pub(super) selected_array_bytes: Vec<usize>,
    pub(super) total_array_bytes: Vec<usize>,
    pub(super) array_bytes_selectivity_basis_points: Vec<usize>,
}

pub(super) struct ProductionLedger {
    pub(super) actual_written_bytes: usize,
    pub(super) entry_lengths: Vec<usize>,
    pub(super) row_counts: Vec<usize>,
    pub(super) payload_widths: Vec<usize>,
    pub(super) projection: ProjectionSamples,
    pub(super) oracle: StreamOracle,
}

pub(super) struct AppendOutcome {
    pub(super) batch: PreparedBatch,
    pub(super) range: Range<u64>,
    pub(super) duration: Duration,
}

impl ProductionLedger {
    pub(super) fn new() -> Self {
        Self {
            actual_written_bytes: 0,
            entry_lengths: Vec::new(),
            row_counts: Vec::new(),
            payload_widths: Vec::new(),
            projection: ProjectionSamples {
                selected_columns: Vec::new(),
                total_columns: Vec::new(),
                column_selectivity_basis_points: Vec::new(),
                selected_array_bytes: Vec::new(),
                total_array_bytes: Vec::new(),
                array_bytes_selectivity_basis_points: Vec::new(),
            },
            oracle: StreamOracle::new(0),
        }
    }

    fn observe(&mut self, batch: &PreparedBatch, encoded: &[Vec<u8>], start_offset: u64) {
        for (index, (entry, encoded)) in batch.entries.iter().zip(encoded).enumerate() {
            let offset = start_offset
                .checked_add(u64::try_from(index).expect("batch index fits u64"))
                .expect("producer offset fits u64");
            self.oracle.observe(
                offset,
                &entry.generated.change,
                entry.generated.persona,
                encoded,
            );
            self.entry_lengths.push(encoded.len());
            self.row_counts.push(entry.generated.change.num_rows());
            self.payload_widths.push(entry.spec.payload_bytes);
            self.projection.observe(entry.projection_measurement());
        }
    }
}

impl ProjectionSamples {
    fn observe(&mut self, measurement: ProjectionMeasurement) {
        self.selected_columns.push(measurement.selected_columns);
        self.total_columns.push(measurement.total_columns);
        self.column_selectivity_basis_points
            .push(measurement.column_selectivity_basis_points);
        self.selected_array_bytes
            .push(measurement.selected_array_bytes);
        self.total_array_bytes.push(measurement.total_array_bytes);
        self.array_bytes_selectivity_basis_points
            .push(measurement.array_bytes_selectivity_basis_points);
    }
}

pub(super) fn append_seed(
    session: &mut Session,
    batch: PreparedBatch,
    next_offset: u64,
    ledger: &mut ProductionLedger,
    config: &Config,
) -> AppendOutcome {
    append(session, batch, next_offset, ledger, config, false)
}

pub(super) fn append_measured(
    session: &mut Session,
    batch: PreparedBatch,
    next_offset: u64,
    ledger: &mut ProductionLedger,
    config: &Config,
) -> AppendOutcome {
    assert!(
        batch
            .encoded_bytes
            .checked_mul(3)
            .expect("producer working-set estimate fits usize")
            <= config.max_working_set_bytes,
        "prepared oracle, timed encoding, and Store batch exceed the working-set budget"
    );
    append(session, batch, next_offset, ledger, config, true)
}

fn append(
    session: &mut Session,
    batch: PreparedBatch,
    next_offset: u64,
    ledger: &mut ProductionLedger,
    config: &Config,
    measured: bool,
) -> AppendOutcome {
    let started = measured.then(std::time::Instant::now);
    let encoded = if measured {
        batch.encode_for_producer()
    } else {
        batch.expected_encoded()
    };
    let transaction = session
        .transactions
        .begin()
        .expect("begin endurance producer transaction");
    let assigned = session
        .log
        .access(transaction.access())
        .expect("access endurance producer log")
        .append_batch(&encoded)
        .expect("append endurance producer batch");
    transaction
        .commit()
        .expect("durably commit endurance producer batch");
    let duration = started.map_or(Duration::ZERO, |started| started.elapsed());

    assert_eq!(assigned.start, next_offset);
    assert_eq!(
        assigned.end,
        next_offset
            .checked_add(u64::try_from(batch.entries.len()).expect("batch entries fit u64"))
            .expect("producer tail fits u64")
    );
    batch.assert_encoded(&encoded);
    ledger.observe(&batch, &encoded, assigned.start);
    ledger.actual_written_bytes = ledger
        .actual_written_bytes
        .checked_add(batch.encoded_bytes)
        .expect("actual written bytes fit usize");
    assert!(
        ledger.actual_written_bytes <= config.max_total_written_bytes,
        "actual encoded writes exceeded the configured total budget"
    );
    AppendOutcome {
        batch,
        range: assigned,
        duration,
    }
}

pub(super) fn retain(
    outcome: AppendOutcome,
    retained: &mut VecDeque<EntryOracle>,
    retained_bytes: &mut usize,
    next_offset: &mut u64,
) {
    let encoded_bytes = outcome.batch.encoded_bytes;
    let oracles = outcome.batch.into_oracles(outcome.range.start);
    *retained_bytes = retained_bytes
        .checked_add(encoded_bytes)
        .expect("retained encoded bytes fit usize");
    *next_offset = outcome.range.end;
    retained.extend(oracles);
}
