use std::path::Path;

use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::operation::{sink::DiscardDefinition, source::SequenceSourceDefinition};
use dogpaddle_store::{Cell, Store};

pub(super) fn build_source_sink_and_read_definition(path: &Path) -> Vec<u8> {
    let mut builder = FlowFactory::new(path);
    let source = builder.station("source", SequenceSourceDefinition::new(0));
    let sink = builder.station("sink", DiscardDefinition::new());
    builder.connect([source], sink);
    drop(builder.build().unwrap());

    read_published_definition(path)
}

pub(super) fn read_published_definition(path: &Path) -> Vec<u8> {
    let store = Store::open(path).unwrap();
    let definition: Cell<Vec<u8>> = store.open_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    definition
        .access(transaction.access())
        .unwrap()
        .get()
        .unwrap()
        .unwrap()
}

pub(super) fn publish_definition(path: &Path, encoded: &[u8]) {
    let mut store = Store::create(path).unwrap();
    let definition: Cell<Vec<u8>> = store.create_data("flow/definition").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        definition
            .access(transaction.access())
            .unwrap()
            .set(&encoded.to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }
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
