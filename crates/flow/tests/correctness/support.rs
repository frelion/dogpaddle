use std::path::Path;

use dogpaddle_change::{Change, SchemaBoundChangeCodec};
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::{scan::SequenceScanDefinition, sink::DiscardDefinition};
use dogpaddle_store::{Cell, Store, StoreSetup};

pub(super) fn encode_output_entry(change: &Change) -> Vec<u8> {
    SchemaBoundChangeCodec::try_new(change.schema())
        .unwrap()
        .encode(change)
        .unwrap()
}

pub(super) fn build_scan_sink_and_read_definition(path: &Path) -> Vec<u8> {
    let mut builder = FlowFactory::new(path);
    let scan = builder.operation("scan", SequenceScanDefinition::new(0), []);
    builder.operation("sink", DiscardDefinition::new(), [scan]);

    drop(builder.build().unwrap());

    read_published_definition(path)
}

pub(super) fn read_published_definition(path: &Path) -> Vec<u8> {
    let store = Store::open(path).unwrap();
    let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    definition
        .access(transaction.access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap()
}

pub(super) fn publish_definition(path: &Path, encoded: &[u8]) {
    let mut store = StoreSetup::new();
    let definition: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let mut transactions = store.commit(path, |_| Ok(())).unwrap();
    let transaction = transactions.begin();
    definition
        .access(transaction.access())
        .unwrap()
        .set(&encoded.to_vec())
        .unwrap();
    transaction.commit().unwrap();
}

pub(super) fn fixture_bytes(contents: &str) -> Vec<u8> {
    let compact = contents
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(compact.len() % 2, 0, "hex fixture must contain full bytes");
    compact
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

pub(super) fn rewrite_checksum(encoded: &mut [u8]) {
    const CHECKSUM_LENGTH: usize = size_of::<u32>();

    let checksum_offset = encoded
        .len()
        .checked_sub(CHECKSUM_LENGTH)
        .expect("fixture includes a checksum");
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}

fn crc32(bytes: &[u8]) -> u32 {
    const POLYNOMIAL: u32 = 0xedb8_8320;

    let mut checksum = u32::MAX;
    for byte in bytes {
        checksum ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (checksum & 1).wrapping_neg();
            checksum = (checksum >> 1) ^ (POLYNOMIAL & mask);
        }
    }
    !checksum
}

pub(super) fn run_until_idle(flow: &mut dogpaddle_flow::Flow) {
    // Boundaries rotate independently; require a full sweep without progress.
    let mut idle = 0;
    for _ in 0..1000 {
        if flow.advance().unwrap() == dogpaddle_flow::AdvanceOutcome::Idle {
            idle += 1;
        } else {
            idle = 0;
        }
        if idle > flow.operation_count() {
            return;
        }
    }
    panic!("finite flow did not drain");
}

pub(super) fn seed_source(path: &Path, index: usize, change: &Change) {
    use dogpaddle_store::Queue;
    let store = Store::open(path).unwrap();
    let position: Cell<u64> = store
        .open_data(&format!("operation/{index:08x}/sequence_scan.position"))
        .unwrap();
    let queue: Queue<Vec<u8>> = store
        .open_data(&format!("operation/{index:08x}/sequence_scan.published"))
        .unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    position
        .access(transaction.access())
        .unwrap()
        .set(&u64::MAX)
        .unwrap();
    assert!(
        queue
            .access(transaction.access())
            .unwrap()
            .try_push(&encode_output_entry(change), std::num::NonZeroU64::MAX)
            .unwrap()
    );
    transaction.commit().unwrap();
}

pub(super) fn values_change(values: impl IntoIterator<Item = u64>, diff: i64) -> Change {
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let values = values.into_iter().collect::<Vec<_>>();
    let rows = values.len();
    Change::try_new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(values))],
        )
        .unwrap(),
        Int64Array::from(vec![diff; rows]),
    )
    .unwrap()
}
