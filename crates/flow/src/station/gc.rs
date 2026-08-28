use std::num::NonZeroUsize;

use dogpaddle_store::{OrderedMap, ReadOnly, Small, TransactionAccess, Transactions};

use super::{
    input::{cursor_key, decode_cursor},
    protocol::StationError,
    runtime::Station,
};

pub(super) const GC_MAX_ITEMS: NonZeroUsize =
    NonZeroUsize::new(1_024).expect("the Station GC batch size is nonzero");

pub(crate) struct ConsumerCursor {
    state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
    input: usize,
}

impl ConsumerCursor {
    pub(crate) const fn new(
        state: ReadOnly<OrderedMap<Vec<u8>, Vec<u8>, Small>>,
        input: usize,
    ) -> Self {
        Self { state, input }
    }

    fn read(&self, consumer: usize, access: TransactionAccess<'_>) -> Result<u64, StationError> {
        let state = self.state.access(access)?;
        let encoded = state
            .get(&cursor_key(self.input))?
            .ok_or(StationError::MissingConsumerCursor { consumer })?;
        decode_cursor(&encoded).ok_or(StationError::MalformedConsumerCursor { consumer })
    }
}

impl Station {
    pub(crate) fn gc(&self, transactions: &mut Transactions) -> Result<(), StationError> {
        if self.consumers.is_empty() {
            return Ok(());
        }

        let transaction = transactions.begin()?;
        let mut output = self
            .output
            .as_ref()
            .expect("a Station with consumers must produce output")
            .access(transaction.access())?;
        let bounds = output.bounds()?;
        let mut target = bounds.end;
        for (consumer, cursor) in self.consumers.iter().enumerate() {
            let offset = cursor.read(consumer, transaction.access())?;
            if offset < bounds.start || offset > bounds.end {
                return Err(StationError::ConsumerCursorOutOfRange {
                    consumer,
                    offset,
                    head: bounds.start,
                    tail: bounds.end,
                });
            }
            target = target.min(offset);
        }
        output.truncate_before(target, GC_MAX_ITEMS)?;
        transaction.commit()?;
        Ok(())
    }
}
