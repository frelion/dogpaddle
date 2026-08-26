use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;

/// Returns the one-column schema used for event-order tests.
#[must_use]
pub fn narrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

/// Builds a narrow Change from explicit values and differences.
///
/// # Panics
///
/// Panics when the fixture inputs violate the Change contract.
#[must_use]
pub fn narrow_change(values: &[u64], diffs: &[i64]) -> Change {
    let records = RecordBatch::try_new(
        narrow_schema(),
        vec![Arc::new(UInt64Array::from(values.to_vec()))],
    )
    .expect("valid narrow fixture record batch");
    Change::try_new(records, Int64Array::from(diffs.to_vec())).expect("valid narrow fixture Change")
}

/// Returns the wide schema used to expose projection costs.
#[must_use]
pub fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]))
}

/// Builds a deterministic wide Change with fixed-size payloads.
///
/// Differences alternate between insertion and retraction. IDs and tail
/// values encode the caller-provided start so checksums remain sensitive to
/// order and loss.
///
/// # Panics
///
/// Panics when `rows` or `payload_bytes` is zero, or allocation exceeds Arrow
/// array limits.
#[must_use]
pub fn wide_change(start: u64, rows: usize, payload_bytes: usize) -> Change {
    assert!(rows > 0, "a Change fixture must contain a row");
    assert!(payload_bytes > 0, "payload width must be non-zero");

    let mut ids = Vec::with_capacity(rows);
    let mut payload_storage = Vec::with_capacity(rows);
    let mut tails = Vec::with_capacity(rows);
    let mut diffs = Vec::with_capacity(rows);
    for index in 0..rows {
        let index = u64::try_from(index).expect("fixture row index fits u64");
        let id = start.checked_add(index).expect("fixture id fits u64");
        ids.push(id);
        tails.push(id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15);
        diffs.push(if index.is_multiple_of(3) { -1 } else { 1 });

        let mut payload = vec![0_u8; payload_bytes];
        for (byte_index, byte) in payload.iter_mut().enumerate() {
            let shift = u32::try_from((byte_index % 8) * 8).expect("shift fits u32");
            *byte = id.rotate_left(shift).to_le_bytes()[0]
                ^ u8::try_from(byte_index % 251).expect("payload byte pattern fits u8");
        }
        payload_storage.push(payload);
    }

    let payloads = payload_storage
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    wide_with_payloads(&ids, &payloads, &tails, &diffs)
}

/// Builds a wide Change from explicit columns.
///
/// # Panics
///
/// Panics when column lengths differ or inputs violate the Change contract.
#[must_use]
pub fn wide_with_payloads(ids: &[u64], payloads: &[&[u8]], tails: &[u64], diffs: &[i64]) -> Change {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.to_vec())),
        Arc::new(BinaryArray::from_iter_values(payloads.iter().copied())),
        Arc::new(UInt64Array::from(tails.to_vec())),
    ];
    let records = RecordBatch::try_new(wide_schema(), columns)
        .expect("equal-length valid wide fixture columns");
    Change::try_new(records, Int64Array::from(diffs.to_vec())).expect("valid wide fixture Change")
}
