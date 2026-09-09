use datafusion_common::ScalarValue;

use arrow_schema::DataType;

use super::{BoundReduction, Descriptor, Fold, Reduction, apply_weight, read_u64, write_u64};
use crate::operation::transform::aggregate::AggregateError;

pub(super) const COUNT_ALL_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::COUNT_ALL,
    arguments: 0,
    bind: |_| {
        Ok(BoundReduction {
            reduction: Reduction::Fold(Box::new(CountAll)),
            output_type: DataType::Int64,
            nullable: false,
        })
    },
};

pub(super) const COUNT_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::COUNT,
    arguments: 1,
    bind: |_| {
        Ok(BoundReduction {
            reduction: Reduction::Fold(Box::new(Count)),
            output_type: DataType::Int64,
            nullable: false,
        })
    },
};

struct CountAll;
struct Count;

impl Fold for CountAll {
    fn empty(&self) -> Vec<u8> {
        Vec::new()
    }

    fn apply(
        &self,
        _state: &mut Vec<u8>,
        values: &[ScalarValue],
        _difference: i64,
        group_weight: u64,
    ) -> Result<(), AggregateError> {
        if !values.is_empty() {
            return Err(AggregateError::InvalidState);
        }
        i64::try_from(group_weight)
            .map(|_| ())
            .map_err(|_| AggregateError::ArithmeticOverflow)
    }

    fn output(&self, state: &[u8], group_weight: u64) -> Result<ScalarValue, AggregateError> {
        if !state.is_empty() {
            return Err(AggregateError::InvalidState);
        }
        let count = i64::try_from(group_weight).map_err(|_| AggregateError::ArithmeticOverflow)?;
        Ok(ScalarValue::Int64(Some(count)))
    }
}

impl Fold for Count {
    fn empty(&self) -> Vec<u8> {
        0_u64.to_be_bytes().to_vec()
    }

    fn apply(
        &self,
        state: &mut Vec<u8>,
        values: &[ScalarValue],
        difference: i64,
        _group_weight: u64,
    ) -> Result<(), AggregateError> {
        let [value] = values else {
            return Err(AggregateError::InvalidState);
        };
        if value.is_null() {
            return Ok(());
        }
        let count = apply_weight(read_u64(state)?, difference)?;
        if count > i64::MAX.cast_unsigned() {
            return Err(AggregateError::ArithmeticOverflow);
        }
        write_u64(state, count);
        Ok(())
    }

    fn output(&self, state: &[u8], _group_weight: u64) -> Result<ScalarValue, AggregateError> {
        let count =
            i64::try_from(read_u64(state)?).map_err(|_| AggregateError::ArithmeticOverflow)?;
        Ok(ScalarValue::Int64(Some(count)))
    }
}
