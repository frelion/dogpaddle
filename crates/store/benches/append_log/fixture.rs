use std::borrow::Cow;

use dogpaddle_store::{
    AppendLog, Cell, CodecError, OrderedMap, Small, Store, StoreValue, Transactions,
};
use tempfile::TempDir;

use crate::{CURSOR_KEY, RECORD_HEADER_BYTES, SEED_BATCH_ITEMS, support::BenchRoot};

pub(super) type StationState = OrderedMap<Vec<u8>, Vec<u8>, Small>;

#[derive(Clone)]
pub(super) struct CdcRecord {
    pub(super) diff: i64,
    pub(super) key: u64,
    pub(super) payload: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(super) struct RecordHeader {
    pub(super) diff: i64,
    pub(super) key: u64,
}

pub(super) struct LogFixture {
    pub(super) transactions: Transactions,
    pub(super) input: AppendLog<CdcRecord>,
    pub(super) output: AppendLog<CdcRecord>,
    pub(super) station_state: StationState,
    pub(super) count: Cell<i64>,
    pub(super) reader_states: Vec<StationState>,
    _root: TempDir,
}

#[derive(Clone, Copy)]
pub(super) enum FilterMode {
    PassThrough,
    ProjectedHalf,
    DecodedHalf,
}

impl CdcRecord {
    pub(super) fn new(index: usize, encoded_bytes: usize) -> Self {
        let key = u64::try_from(index).expect("benchmark record index fits in u64");
        let fill = u8::try_from(key & 0xff).expect("masked payload byte fits in u8");
        Self {
            diff: if index.is_multiple_of(2) { 1 } else { -1 },
            key,
            payload: vec![fill; encoded_bytes - RECORD_HEADER_BYTES],
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(RECORD_HEADER_BYTES + self.payload.len());
        encoded.extend_from_slice(&self.diff.to_be_bytes());
        encoded.extend_from_slice(&self.key.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        encoded
    }
}

impl StoreValue for CdcRecord {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        Ok(self.encode())
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        let mut bytes = bytes.into_owned();
        let header = decode_header(&bytes)?;
        let payload = bytes.split_off(RECORD_HEADER_BYTES);
        Ok(Self {
            diff: header.diff,
            key: header.key,
            payload,
        })
    }
}

impl LogFixture {
    pub(super) fn populated(
        bench_root: &BenchRoot,
        entries: usize,
        record_bytes: usize,
        readers: usize,
    ) -> Self {
        let root = bench_root.sample("append-log-fixture");
        let mut store =
            Store::create(root.path().join("store")).expect("create append-log benchmark store");
        let input = store
            .create_data::<AppendLog<CdcRecord>>("input")
            .expect("create benchmark input log");
        let output = store
            .create_data::<AppendLog<CdcRecord>>("output")
            .expect("create benchmark output log");
        let station_state = store
            .create_data::<StationState>("station/00000000/state")
            .expect("create benchmark station state");
        let count = store
            .create_data::<Cell<i64>>("count")
            .expect("create benchmark count");
        let reader_states = (0..readers)
            .map(|reader| {
                store
                    .create_data::<StationState>(&format!("station/{:08x}/state", reader + 1))
                    .expect("create benchmark reader station state")
            })
            .collect::<Vec<_>>();
        let mut fixture = Self {
            transactions: store.into_transactions(),
            input,
            output,
            station_state,
            count,
            reader_states,
            _root: root,
        };

        {
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin benchmark state seed");
            fixture
                .station_state
                .access(transaction.access())
                .expect("access benchmark station state")
                .put(&CURSOR_KEY.to_vec(), &0_u64.to_be_bytes().to_vec())
                .expect("seed benchmark station cursor");
            fixture
                .count
                .access(transaction.access())
                .expect("access benchmark count")
                .set(&0)
                .expect("seed benchmark count");
            for state in &fixture.reader_states {
                state
                    .access(transaction.access())
                    .expect("access benchmark reader station state")
                    .put(&CURSOR_KEY.to_vec(), &0_u64.to_be_bytes().to_vec())
                    .expect("seed benchmark reader cursor");
            }
            transaction.commit().expect("commit benchmark state seed");
        }

        for start in (0..entries).step_by(SEED_BATCH_ITEMS) {
            let end = entries.min(start + SEED_BATCH_ITEMS);
            let transaction = fixture
                .transactions
                .begin()
                .expect("begin benchmark log seed");
            let mut input = fixture
                .input
                .access(transaction.access())
                .expect("access benchmark input log");
            for index in start..end {
                input
                    .append(&CdcRecord::new(index, record_bytes))
                    .expect("seed benchmark input record");
            }
            transaction.commit().expect("commit benchmark log seed");
        }
        fixture
    }
}
pub(super) fn decode_header(encoded: &[u8]) -> Result<RecordHeader, CodecError> {
    if encoded.len() < RECORD_HEADER_BYTES {
        return Err(CodecError::new("truncated benchmark CDC record"));
    }
    let diff = i64::from_be_bytes(
        encoded[..8]
            .try_into()
            .map_err(|_| CodecError::new("invalid benchmark diff"))?,
    );
    let key = u64::from_be_bytes(
        encoded[8..RECORD_HEADER_BYTES]
            .try_into()
            .map_err(|_| CodecError::new("invalid benchmark key"))?,
    );
    Ok(RecordHeader { diff, key })
}
