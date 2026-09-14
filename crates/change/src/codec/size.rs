use arrow_array::Array;
use arrow_data::ArrayData;

use super::CodecError;
use crate::{change::Change, schema::DataTypeLayout};

pub(super) fn body_len_bounded(change: &Change, max_bytes: usize) -> Result<usize, CodecError> {
    let mut budget = BodyBudget {
        bytes: 0,
        max_bytes,
    };
    visit(&change.diffs().to_data(), &mut budget)?;
    for column in change.records().columns() {
        visit(&column.to_data(), &mut budget)?;
    }
    Ok(budget.bytes)
}

struct BodyBudget {
    bytes: usize,
    max_bytes: usize,
}

impl BodyBudget {
    fn buffer(&mut self, bytes: usize) -> Result<(), CodecError> {
        let padded = bytes
            .checked_add(7)
            .map(|bytes| bytes & !7)
            .ok_or_else(|| CodecError::size_limit(self.max_bytes))?;
        self.bytes = self
            .bytes
            .checked_add(padded)
            .filter(|total| *total <= self.max_bytes)
            .ok_or_else(|| CodecError::size_limit(self.max_bytes))?;
        Ok(())
    }
}

fn visit(data: &ArrayData, budget: &mut BodyBudget) -> Result<(), CodecError> {
    let layout = DataTypeLayout::classify(data.data_type())
        .ok_or_else(|| CodecError::invalid("Change contains an unsupported Arrow type"))?;
    // Arrow IPC V5 materializes an all-valid bitmap when the ArrayData has no
    // null buffer, so every non-Null field still contributes this buffer.
    if !matches!(layout, DataTypeLayout::Null) {
        budget.buffer(bitmap_bytes(data.len(), budget.max_bytes)?)?;
    }

    match layout {
        DataTypeLayout::Null => Ok(()),
        DataTypeLayout::Bitmap => budget.buffer(bitmap_bytes(data.len(), budget.max_bytes)?),
        DataTypeLayout::FixedWidth(width) => {
            budget.buffer(product(data.len(), width, budget.max_bytes)?)
        }
        DataTypeLayout::VariableWidth => {
            budget.buffer(offset_bytes(data.len(), budget.max_bytes)?)?;
            budget.buffer(offset_span(data, budget.max_bytes)?.1)
        }
        DataTypeLayout::List(_) => {
            budget.buffer(offset_bytes(data.len(), budget.max_bytes)?)?;
            let (start, length) = offset_span(data, budget.max_bytes)?;
            let child = data
                .child_data()
                .first()
                .ok_or_else(|| CodecError::invalid("Arrow List has no child data"))?
                .slice(start, length);
            visit(&child, budget)
        }
        DataTypeLayout::Struct(_) => {
            for child in data.child_data() {
                visit(child, budget)?;
            }
            Ok(())
        }
    }
}

fn offset_span(data: &ArrayData, max_bytes: usize) -> Result<(usize, usize), CodecError> {
    if data.is_empty() {
        return Ok((0, 0));
    }
    let offsets = data
        .buffers()
        .first()
        .ok_or_else(|| CodecError::invalid("variable-width Arrow array has no offsets"))?
        .typed_data::<i32>();
    let end_index = data
        .offset()
        .checked_add(data.len())
        .ok_or_else(|| CodecError::size_limit(max_bytes))?;
    let start = usize::try_from(
        *offsets
            .get(data.offset())
            .ok_or_else(|| CodecError::invalid("Arrow offsets are truncated"))?,
    )
    .map_err(|_| CodecError::invalid("Arrow offset is negative"))?;
    let end = usize::try_from(
        *offsets
            .get(end_index)
            .ok_or_else(|| CodecError::invalid("Arrow offsets are truncated"))?,
    )
    .map_err(|_| CodecError::invalid("Arrow offset is negative"))?;
    let length = end
        .checked_sub(start)
        .ok_or_else(|| CodecError::invalid("Arrow offsets are not monotonic"))?;
    Ok((start, length))
}

fn bitmap_bytes(elements: usize, max_bytes: usize) -> Result<usize, CodecError> {
    elements
        .checked_add(7)
        .map(|elements| elements / 8)
        .ok_or_else(|| CodecError::size_limit(max_bytes))
}

fn offset_bytes(elements: usize, max_bytes: usize) -> Result<usize, CodecError> {
    product(
        elements
            .checked_add(1)
            .ok_or_else(|| CodecError::size_limit(max_bytes))?,
        size_of::<i32>(),
        max_bytes,
    )
}

fn product(left: usize, right: usize, max_bytes: usize) -> Result<usize, CodecError> {
    left.checked_mul(right)
        .filter(|bytes| *bytes <= max_bytes)
        .ok_or_else(|| CodecError::size_limit(max_bytes))
}
