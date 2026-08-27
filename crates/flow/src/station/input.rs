use dogpaddle_change::{Change, decode_change};
use dogpaddle_store::{AppendLog, ReadOnly, ReadTransactions, ScanLimit};

use super::{protocol::StationError, runtime::Station};

const CURSOR_BYTES: usize = 2 * size_of::<u64>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Cursor {
    pub(super) offset: u64,
    pub(super) row_index: u64,
}

impl Cursor {
    pub(super) const ORIGIN: Self = Self {
        offset: 0,
        row_index: 0,
    };

    pub(super) const fn encode(self) -> [u8; CURSOR_BYTES] {
        let mut encoded = [0; CURSOR_BYTES];
        let offset = self.offset.to_be_bytes();
        let row_index = self.row_index.to_be_bytes();
        let mut index = 0;
        while index < size_of::<u64>() {
            encoded[index] = offset[index];
            encoded[size_of::<u64>() + index] = row_index[index];
            index += 1;
        }
        encoded
    }

    pub(super) fn decode(encoded: &[u8]) -> Option<Self> {
        let encoded: [u8; CURSOR_BYTES] = encoded.try_into().ok()?;
        let (offset, row_index) = encoded.split_at(size_of::<u64>());
        Some(Self {
            offset: u64::from_be_bytes(offset.try_into().ok()?),
            row_index: u64::from_be_bytes(row_index.try_into().ok()?),
        })
    }
}

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
    /// Idempotently loads the Change containing each input's next unread row.
    ///
    /// A populated cache is already a complete owned entry and makes this
    /// phase a no-op for that input. The later write phase owns durable cursor
    /// validation and releases the cache after consuming the entry.
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
        let mut loaded = Vec::with_capacity(self.inputs.len());
        for (index, input) in self.inputs.iter().enumerate() {
            if input.cache.is_some() {
                loaded.push(None);
                continue;
            }

            let key = cursor_key(index);
            let encoded = state
                .get(&key)?
                .ok_or(StationError::MissingCursor { input: index })?;
            let cursor =
                Cursor::decode(&encoded).ok_or(StationError::MalformedCursor { input: index })?;

            let input_log = input.log.read(transaction.access())?;
            let mut cache = None;
            input_log.scan(
                cursor.offset,
                ScanLimit::new(1, usize::MAX)?,
                |entry| -> Result<(), StationError> {
                    let encoded = entry.decode_owned()?;
                    cache = Some(decode_change(&encoded).map_err(|source| {
                        StationError::InvalidInputChange {
                            input: index,
                            source,
                        }
                    })?);
                    Ok(())
                },
            )?;
            let Some(cache) = cache else {
                if cursor.row_index != 0 {
                    return Err(StationError::NonzeroRowAtTail {
                        input: index,
                        offset: cursor.offset,
                        row_index: cursor.row_index,
                    });
                }
                loaded.push(None);
                continue;
            };
            validate_cached_cursor(index, cursor, &cache)?;
            loaded.push(Some(cache));
        }
        for (input, cache) in self.inputs.iter_mut().zip(loaded) {
            if cache.is_some() {
                input.cache = cache;
            }
        }
        Ok(())
    }
}

pub(super) fn cursor_key(index: usize) -> Vec<u8> {
    let index = u32::try_from(index).expect("validated input count fits the Flow format");
    format!("input/{index:08x}/cursor").into_bytes()
}

fn validate_cached_cursor(
    input: usize,
    cursor: Cursor,
    change: &Change,
) -> Result<(), StationError> {
    let Ok(row_index) = usize::try_from(cursor.row_index) else {
        return Err(StationError::CursorRowOutOfRange {
            input,
            row_index: cursor.row_index,
            rows: change.num_rows(),
        });
    };
    if row_index >= change.num_rows() {
        return Err(StationError::CursorRowOutOfRange {
            input,
            row_index: cursor.row_index,
            rows: change.num_rows(),
        });
    }
    Ok(())
}
