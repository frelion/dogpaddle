use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, Int64Array, ListArray, RecordBatch, StringArray, UInt64Array,
    types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::{Change, SchemaBoundChangeCodec};

/// Logical Changes paired with the exact bytes stored in a `SubscribedLog`.
pub struct EncodedChanges {
    /// Codec shared by every entry in this exact-Schema resource.
    pub codec: SchemaBoundChangeCodec,
    /// Changes in durable log order.
    pub changes: Vec<Change>,
    /// One schema-bound entry per Change.
    pub encoded: Vec<Vec<u8>>,
}

impl EncodedChanges {
    fn new(schema: SchemaRef, changes: Vec<Change>) -> Self {
        assert!(!changes.is_empty(), "a seam workload must not be empty");
        let codec = SchemaBoundChangeCodec::try_new(schema).expect("bind fixture Change Schema");
        let encoded = changes
            .iter()
            .map(|change| codec.encode(change).expect("encode fixture Change"))
            .collect();
        Self {
            codec,
            changes,
            encoded,
        }
    }

    /// Returns an order-sensitive checksum of the exact encoded entries.
    #[must_use]
    pub fn order_checksum(&self) -> u64 {
        order_checksum(&self.encoded)
    }
}

/// A sliced nested Change and its exact schema-bound encoding.
pub struct EncodedChange {
    /// Codec bound to the resource's exact Schema.
    pub codec: SchemaBoundChangeCodec,
    /// Change with non-zero Arrow array offsets.
    pub change: Change,
    /// Schema-bound entry for `change`.
    pub encoded: Vec<u8>,
}

/// Builds a nested, variable-width Change whose arrays start at a non-zero offset.
///
/// # Panics
///
/// Panics when `rows` or `payload_bytes` is zero or fixture dimensions overflow.
#[must_use]
pub fn nested_change_fixture(seed: u64, rows: usize, payload_bytes: usize) -> EncodedChange {
    assert!(rows > 0, "a nested fixture must contain a row");
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
    let records = RecordBatch::try_new(schema, columns).expect("construct nested fixture");
    let source = Change::try_new(records, Int64Array::from(vec![1; source_rows]))
        .expect("construct valid nested Change");
    let change = source
        .try_slice(1, rows)
        .expect("slice nested Change at a non-zero offset");
    let codec =
        SchemaBoundChangeCodec::try_new(change.schema()).expect("bind nested Change Schema");
    let encoded = codec.encode(&change).expect("encode nested fixture");
    EncodedChange {
        codec,
        change,
        encoded,
    }
}

/// Builds fixed-schema Changes with alternating entry widths for the seam benchmark.
///
/// # Panics
///
/// Panics when fewer than two entries are requested, dimensions are zero, or
/// fixture dimensions overflow.
#[must_use]
pub fn fixed_schema_changes_fixture(
    entries: usize,
    rows: usize,
    payload_bytes: usize,
) -> EncodedChanges {
    assert!(
        entries >= 2,
        "fixed-schema workload needs at least two entries"
    );
    assert!(rows > 0, "a Change fixture must contain a row");
    assert!(payload_bytes > 0, "payload width must be non-zero");
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let mut start = 1_000_u64;
    let mut changes = Vec::with_capacity(entries);
    for ordinal in 0..entries {
        let width = payload_bytes
            .checked_mul(ordinal % 3 + 1)
            .expect("fixture payload width fits usize");
        let change = wide_change(Arc::clone(&schema), start, rows, width);
        start = start
            .checked_add(u64::try_from(rows).expect("row count fits u64"))
            .expect("fixture id fits u64");
        changes.push(change);
    }
    EncodedChanges::new(schema, changes)
}

/// Builds a fixed-schema, variable-width Change.
///
/// # Panics
///
/// Panics when `rows` or `payload_bytes` is zero or fixture dimensions overflow.
#[must_use]
fn wide_change(schema: SchemaRef, start: u64, rows: usize, payload_bytes: usize) -> Change {
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
    let records = RecordBatch::try_new(schema, columns).expect("construct wide fixture");
    Change::try_new(records, Int64Array::from(vec![1; rows])).expect("construct valid wide Change")
}

fn order_checksum<I, B>(entries: I) -> u64
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
