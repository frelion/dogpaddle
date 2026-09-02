use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, Int64Array, ListArray, RecordBatch, StringArray, UInt64Array,
    types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::{Change, ChangeProjection, encode_change};

/// Logical Changes paired with the exact bytes stored in an `AppendLog`.
pub struct EncodedChanges {
    /// Changes in durable log order.
    pub changes: Vec<Change>,
    /// One complete Arrow IPC Stream per Change.
    pub encoded: Vec<Vec<u8>>,
}

impl EncodedChanges {
    fn new(changes: Vec<Change>) -> Self {
        assert!(!changes.is_empty(), "a seam workload must not be empty");
        let encoded = changes
            .iter()
            .map(|change| encode_change(change).expect("encode fixture Change"))
            .collect();
        Self { changes, encoded }
    }

    /// Returns the exact item-key plus value-byte charge for a full scan.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate byte count exceeds `usize`.
    #[must_use]
    pub fn scan_bytes(&self) -> usize {
        self.encoded.iter().fold(0_usize, |total, entry| {
            total
                .checked_add(size_of::<u64>())
                .and_then(|value| value.checked_add(entry.len()))
                .expect("fixture scan charge fits usize")
        })
    }

    /// Returns an order-sensitive checksum of the exact encoded entries.
    #[must_use]
    pub fn order_checksum(&self) -> u64 {
        order_checksum(&self.encoded)
    }
}

/// A sliced nested Change and its full and projected expectations.
pub struct ProjectableFixture {
    /// Full Change with non-zero Arrow array offsets.
    pub change: Change,
    /// Complete self-describing Stream for `change`.
    pub encoded: Vec<u8>,
    /// Schema-bound top-level projection.
    pub projection: ChangeProjection,
    /// Expected projected Change.
    pub projected: Change,
}

/// Builds a nested, variable-width Change whose arrays start at a non-zero offset.
///
/// # Panics
///
/// Panics when `rows` or `payload_bytes` is zero or fixture dimensions overflow.
#[must_use]
pub fn projectable_fixture(seed: u64, rows: usize, payload_bytes: usize) -> ProjectableFixture {
    assert!(rows > 0, "a projectable fixture must contain a row");
    assert!(payload_bytes > 0, "payload width must be non-zero");
    let source_rows = rows.checked_add(2).expect("fixture row count fits usize");
    let ids = fixture_ids(seed, source_rows);
    let labels = (0..source_rows)
        .map(|index| (!index.is_multiple_of(3)).then(|| format!("label-{}", ids[index])))
        .collect::<Vec<_>>();
    let payload_storage = ids
        .iter()
        .enumerate()
        .map(|(index, id)| fixture_payload(*id, payload_bytes + index % 3))
        .collect::<Vec<_>>();
    let values = ListArray::from_iter_primitive::<Int64Type, _, _>((0..source_rows).map(|index| {
        if index.is_multiple_of(4) {
            None
        } else if index.is_multiple_of(3) {
            Some(Vec::<Option<i64>>::new())
        } else {
            let value = i64::try_from(ids[index]).expect("fixture id fits i64");
            Some(vec![
                Some(value),
                (!index.is_multiple_of(2)).then_some(-value),
            ])
        }
    }));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(labels.iter().map(Option::as_deref).collect::<StringArray>()),
        Arc::new(
            payload_storage
                .iter()
                .map(|payload| Some(payload.as_slice()))
                .collect::<BinaryArray>(),
        ),
        Arc::new(values),
        Arc::new(UInt64Array::from(
            ids.iter()
                .map(|id| id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15)
                .collect::<Vec<_>>(),
        )),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, false),
        Field::new("values", columns[3].data_type().clone(), true),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(schema, columns).expect("construct projectable fixture");
    let source = Change::try_new(records, Int64Array::from(vec![1; source_rows]))
        .expect("construct valid projectable Change");
    let change = source
        .try_slice(1, rows)
        .expect("slice projectable Change at a non-zero offset");
    let projection = ChangeProjection::try_new(change.schema(), [0, 2, 3, 4])
        .expect("bind projectable fixture projection");
    let projected = change
        .try_project(&projection)
        .expect("project fixture Change");
    let encoded = encode_change(&change).expect("encode projectable fixture");
    ProjectableFixture {
        change,
        encoded,
        projection,
        projected,
    }
}

/// Builds alternating schemas and entry widths for the regular seam benchmark.
///
/// # Panics
///
/// Panics when fewer than two entries are requested, dimensions are zero, or
/// fixture dimensions overflow.
#[must_use]
pub fn heterogeneous_pages_fixture(
    entries: usize,
    rows: usize,
    payload_bytes: usize,
) -> EncodedChanges {
    assert!(
        entries >= 2,
        "heterogeneous paging needs at least two entries"
    );
    assert!(rows > 0, "a Change fixture must contain a row");
    assert!(payload_bytes > 0, "payload width must be non-zero");
    let mut start = 1_000_u64;
    let mut changes = Vec::with_capacity(entries);
    for ordinal in 0..entries {
        let change = if ordinal.is_multiple_of(2) {
            scalar_change(start, rows)
        } else {
            let width = payload_bytes
                .checked_mul(ordinal % 3 + 1)
                .expect("fixture payload width fits usize");
            wide_change(start, rows, width)
        };
        start = start
            .checked_add(u64::try_from(rows).expect("row count fits u64"))
            .expect("fixture id fits u64");
        changes.push(change);
    }
    EncodedChanges::new(changes)
}

/// Builds a fixed-schema, variable-width Change.
///
/// # Panics
///
/// Panics when `rows` or `payload_bytes` is zero or fixture dimensions overflow.
#[must_use]
fn wide_change(start: u64, rows: usize, payload_bytes: usize) -> Change {
    assert!(rows > 0, "a Change fixture must contain a row");
    assert!(payload_bytes > 0, "payload width must be non-zero");
    let ids = fixture_ids(start, rows);
    let payload_storage = ids
        .iter()
        .map(|id| fixture_payload(*id, payload_bytes))
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(
            payload_storage
                .iter()
                .map(|payload| Some(payload.as_slice()))
                .collect::<BinaryArray>(),
        ),
        Arc::new(UInt64Array::from(
            ids.iter()
                .map(|id| id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15)
                .collect::<Vec<_>>(),
        )),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(schema, columns).expect("construct wide fixture");
    Change::try_new(records, Int64Array::from(vec![1; rows])).expect("construct valid wide Change")
}

/// Asserts complete logical equality between two Changes.
///
/// # Panics
///
/// Panics when schemas, arrays, row order, or differences differ.
pub fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
}

/// Computes an order-sensitive checksum over exact persisted entries.
#[must_use]
pub fn order_checksum<I, B>(entries: I) -> u64
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    entries
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |state, entry| {
            let entry = entry.as_ref();
            entry
                .len()
                .to_le_bytes()
                .iter()
                .chain(entry)
                .fold(state, |state, byte| {
                    (state ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                })
        })
}

fn scalar_change(start: u64, rows: usize) -> Change {
    let values = fixture_ids(start, rows);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))])
        .expect("construct scalar fixture");
    Change::try_new(
        records,
        Int64Array::from(
            (0..rows)
                .map(|row| if row.is_multiple_of(3) { 2 } else { 1 })
                .collect::<Vec<_>>(),
        ),
    )
    .expect("construct valid scalar Change")
}

fn fixture_ids(start: u64, rows: usize) -> Vec<u64> {
    (0..rows)
        .map(|index| {
            start
                .checked_add(u64::try_from(index).expect("fixture row index fits u64"))
                .expect("fixture id fits u64")
        })
        .collect()
}

fn fixture_payload(id: u64, width: usize) -> Vec<u8> {
    (0..width)
        .map(|index| {
            let shift = u32::try_from((index % 8) * 8).expect("shift fits u32");
            id.rotate_left(shift).to_le_bytes()[0]
                ^ u8::try_from(index % 251).expect("payload pattern fits u8")
        })
        .collect()
}
