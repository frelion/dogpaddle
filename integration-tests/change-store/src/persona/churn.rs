use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;

use super::{FNV_OFFSET, hash_u64};

/// One event in the deterministic valid-churn model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChurnEvent {
    /// Complete record identity for the narrow churn Schema.
    pub value: u64,
    /// Signed relation difference.
    pub diff: i64,
}

/// The independently evaluated result of a churn sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChurnModel {
    /// Final non-zero relation weights by complete record identity.
    pub final_weights: BTreeMap<u64, i64>,
    /// Order-sensitive checksum over all events.
    pub order_checksum: u64,
}

/// Failure returned by the independent valid-flow oracle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChurnValidationError {
    /// Zero-based event position that violated the model.
    pub index: usize,
    /// Record whose prefix weight became invalid.
    pub value: u64,
    /// Invalid prefix weight.
    pub weight: i64,
}

impl fmt::Display for ChurnValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event {} made record {} weight negative: {}",
            self.index, self.value, self.weight
        )
    }
}

impl Error for ChurnValidationError {}

/// Generates a deterministic churn sequence whose every prefix is valid from
/// an empty relation.
///
/// # Panics
///
/// Panics when `seed + events / 4` exceeds `u64`.
#[must_use]
pub fn valid_churn_events(seed: u64, events: usize) -> Vec<ChurnEvent> {
    (0..events)
        .map(|index| {
            let group = index / 4;
            let value = seed
                .checked_add(u64::try_from(group).expect("group index fits u64"))
                .expect("churn identifiers fit u64");
            let diff = [2, -1, 1, -2][index % 4];
            ChurnEvent { value, diff }
        })
        .collect()
}

/// Evaluates relation weights and event order independently of Change.
///
/// # Errors
///
/// Returns the first event that would make a record's prefix weight negative.
pub fn validate_churn(events: &[ChurnEvent]) -> Result<ChurnModel, ChurnValidationError> {
    let mut weights = BTreeMap::<u64, i64>::new();
    let mut checksum = FNV_OFFSET;
    for (index, event) in events.iter().copied().enumerate() {
        let weight = weights.entry(event.value).or_default();
        *weight = weight.checked_add(event.diff).unwrap_or(i64::MIN);
        if *weight < 0 {
            return Err(ChurnValidationError {
                index,
                value: event.value,
                weight: *weight,
            });
        }
        checksum = hash_u64(checksum, event.value);
        checksum = hash_u64(checksum, u64::from_ne_bytes(event.diff.to_ne_bytes()));
    }
    weights.retain(|_, weight| *weight != 0);
    Ok(ChurnModel {
        final_weights: weights,
        order_checksum: checksum,
    })
}

/// Partitions one continuous valid churn stream into Changes.
///
/// # Panics
///
/// Panics when there are no partitions, a partition is empty, dimensions
/// overflow, or fixture construction fails.
#[must_use]
pub fn churn_changes(seed: u64, rows_per_change: &[usize]) -> Vec<Change> {
    assert!(
        !rows_per_change.is_empty(),
        "churn must contain at least one Change"
    );
    assert!(
        rows_per_change.iter().all(|rows| *rows > 0),
        "churn Changes must be non-empty"
    );
    let total_rows = rows_per_change
        .iter()
        .try_fold(0_usize, |total, rows| total.checked_add(*rows))
        .expect("churn row count fits usize");
    let events = valid_churn_events(seed, total_rows);
    validate_churn(&events).expect("generated churn is valid");
    let mut start = 0;
    rows_per_change
        .iter()
        .map(|rows| {
            let end = start + rows;
            let change = churn_change(&events[start..end]);
            start = end;
            change
        })
        .collect()
}

/// Flattens Changes created by [`churn_changes`] into independent events.
///
/// # Panics
///
/// Panics when a Change does not use the narrow churn Schema.
#[must_use]
pub fn flatten_churn_changes(changes: &[Change]) -> Vec<ChurnEvent> {
    changes
        .iter()
        .flat_map(|change| {
            assert_eq!(change.records().num_columns(), 1);
            let schema = change.schema();
            let field = schema.field(0);
            assert_eq!(field.name(), "value");
            assert!(!field.is_nullable());
            let values = change.records().column(0);
            assert_eq!(values.data_type(), &DataType::UInt64);
            let values = values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("churn value array");
            assert_eq!(values.null_count(), 0);
            values
                .values()
                .iter()
                .copied()
                .zip(change.diffs().values().iter().copied())
                .map(|(value, diff)| ChurnEvent { value, diff })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn churn_change(events: &[ChurnEvent]) -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from_iter_values(
            events.iter().map(|event| event.value),
        ))],
    )
    .expect("construct churn record batch");
    Change::try_new(
        records,
        Int64Array::from_iter_values(events.iter().map(|event| event.diff)),
    )
    .expect("construct churn Change")
}
