use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::{AppendLog, ReadOnly, ReadTransactions, ScanLimit, Transactions};

use super::{protocol::StationError, runtime::Station};

pub(super) const ACTIVE_INPUT_KEY: &[u8] = b"input/active";
pub(super) const CURSOR_ORIGIN: u64 = 0;

pub(super) struct InputChange {
    pub(super) input: usize,
    pub(super) offset: u64,
    pub(super) change: Change,
}

struct EncodedInputChange {
    input: usize,
    offset: u64,
    encoded: Vec<u8>,
}

pub(super) struct Inputs {
    pub(super) logs: Vec<ReadOnly<AppendLog<Vec<u8>>>>,
    pub(super) cache: Option<InputChange>,
}

impl Inputs {
    pub(super) const fn new(logs: Vec<ReadOnly<AppendLog<Vec<u8>>>>) -> Self {
        Self { logs, cache: None }
    }
}

impl Station {
    /// Idempotently loads at most one Change into the Station-wide cache.
    ///
    /// A populated cache is already a complete owned entry, so repeated calls
    /// do not access the Store. On a miss, inputs are searched cyclically from
    /// the durable active input until one complete entry is selected. When the
    /// selected port differs from the active port, a separate write transaction
    /// durably pins that port before the entry is decoded and cached. The cache
    /// retains only that entry's input and offset identity alongside the owned
    /// Change. This phase does not advance a cursor or interpret progress within
    /// the Change.
    pub(crate) fn intake(
        &mut self,
        reads: &ReadTransactions,
        transactions: &mut Transactions,
    ) -> Result<bool, StationError> {
        if self.inputs.cache.is_some() || self.inputs.logs.is_empty() {
            return Ok(false);
        }

        let (active, selected) = {
            let transaction = reads.begin()?;
            let state = self.state.read(transaction.access())?;
            let encoded = state
                .get(&ACTIVE_INPUT_KEY.to_vec())?
                .ok_or(StationError::MissingActiveInput)?;
            let active = decode_active_input(&encoded).ok_or(StationError::MalformedActiveInput)?;
            if active >= self.inputs.logs.len() {
                return Err(StationError::ActiveInputOutOfRange {
                    input: active,
                    input_count: self.inputs.logs.len(),
                });
            }

            let mut selected = None;
            for index in (active..self.inputs.logs.len()).chain(0..active) {
                let key = cursor_key(index);
                let encoded = state
                    .get(&key)?
                    .ok_or(StationError::MissingCursor { input: index })?;
                let offset = decode_cursor(&encoded)
                    .ok_or(StationError::MalformedCursor { input: index })?;

                let input_log = self.inputs.logs[index].read(transaction.access())?;
                input_log.scan(
                    offset,
                    ScanLimit::new(1, usize::MAX)?,
                    |entry| -> Result<(), StationError> {
                        selected = Some(EncodedInputChange {
                            input: index,
                            offset: entry.offset(),
                            encoded: entry.decode_owned()?,
                        });
                        Ok(())
                    },
                )?;
                if selected.is_some() {
                    break;
                }
            }
            (active, selected)
        };

        let Some(selected) = selected else {
            return Ok(false);
        };

        let pinned = selected.input != active;
        if pinned {
            let transaction = transactions.begin()?;
            self.state.access(transaction.access())?.put(
                &ACTIVE_INPUT_KEY.to_vec(),
                &encode_active_input(selected.input).to_vec(),
            )?;
            transaction.commit()?;
        }

        let change = decode_change(&selected.encoded).map_err(|source| {
            StationError::InvalidInputChange {
                input: selected.input,
                source,
            }
        })?;
        self.inputs.cache = Some(InputChange {
            input: selected.input,
            offset: selected.offset,
            change,
        });
        Ok(pinned)
    }
}

pub(super) fn encode_active_input(input: usize) -> [u8; size_of::<u32>()] {
    u32::try_from(input)
        .expect("validated input count fits the Flow format")
        .to_be_bytes()
}

pub(super) fn decode_active_input(encoded: &[u8]) -> Option<usize> {
    let input = u32::from_be_bytes(encoded.try_into().ok()?);
    usize::try_from(input).ok()
}

pub(super) fn cursor_key(index: usize) -> Vec<u8> {
    let index = u32::try_from(index).expect("validated input count fits the Flow format");
    format!("input/{index:08x}/cursor").into_bytes()
}

pub(super) const fn encode_cursor(offset: u64) -> [u8; size_of::<u64>()] {
    offset.to_be_bytes()
}

pub(super) fn decode_cursor(encoded: &[u8]) -> Option<u64> {
    encoded.try_into().ok().map(u64::from_be_bytes)
}
