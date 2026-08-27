use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::{AppendLog, ReadOnly, ReadTransactions, ScanLimit};

use super::{protocol::StationError, runtime::Station};

pub(super) const CURSOR_ORIGIN: u64 = 0;

pub(super) struct Input {
    pub(super) log: ReadOnly<AppendLog<Vec<u8>>>,
    pub(super) cache: Option<Change>,
}

impl Input {
    pub(super) const fn new(log: ReadOnly<AppendLog<Vec<u8>>>) -> Self {
        Self { log, cache: None }
    }
}

impl Station {
    /// Idempotently loads one Change at each input's durable offset.
    ///
    /// A populated cache is already a complete owned entry, so repeated calls
    /// do not read that input again. This phase neither advances the offset nor
    /// interprets progress within the cached Change.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Flow scheduler is not implemented yet")
    )]
    pub(crate) fn intake(&mut self, reads: &ReadTransactions) -> Result<(), StationError> {
        if self.inputs.iter().all(|input| input.cache.is_some()) {
            return Ok(());
        }

        let transaction = reads.begin()?;
        let state = self.state.read(transaction.access())?;
        for (index, input) in self.inputs.iter_mut().enumerate() {
            if input.cache.is_some() {
                continue;
            }

            let key = cursor_key(index);
            let encoded = state
                .get(&key)?
                .ok_or(StationError::MissingCursor { input: index })?;
            let offset =
                decode_cursor(&encoded).ok_or(StationError::MalformedCursor { input: index })?;

            let input_log = input.log.read(transaction.access())?;
            input_log.scan(
                offset,
                ScanLimit::new(1, usize::MAX)?,
                |entry| -> Result<(), StationError> {
                    let encoded = entry.decode_owned()?;
                    input.cache = Some(decode_change(&encoded).map_err(|source| {
                        StationError::InvalidInputChange {
                            input: index,
                            source,
                        }
                    })?);
                    Ok(())
                },
            )?;
        }
        Ok(())
    }
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
