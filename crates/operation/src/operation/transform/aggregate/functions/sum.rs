use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use super::{BoundReduction, Descriptor, Fold, Reduction, apply_weight, unsupported};
use crate::{
    expression::BoundExpression,
    operation::transform::aggregate::{AggregateError, AggregateSchemaError},
};

pub(super) const DESCRIPTOR: Descriptor = Descriptor {
    tag: super::SUM,
    arguments: 1,
    bind,
};

enum Sum {
    Signed,
    Unsigned,
}

fn bind(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    let input = arguments
        .first()
        .expect("the descriptor validates SUM argument count")
        .output_type();
    let (sum, output_type) = match input {
        DataType::Int64 => (Sum::Signed, DataType::Int64),
        DataType::UInt64 => (Sum::Unsigned, DataType::UInt64),
        other => return Err(unsupported("SUM", other)),
    };
    Ok(BoundReduction {
        reduction: Reduction::Fold(Box::new(sum)),
        output_type,
        nullable: true,
    })
}

impl Fold for Sum {
    fn empty(&self) -> Vec<u8> {
        match self {
            Self::Signed | Self::Unsigned => vec![0; 16],
        }
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

        match (self, value) {
            (Self::Signed, ScalarValue::Int64(Some(value))) => {
                let (count, sum) = read_i64_state(state)?;
                let count = apply_weight(count, difference)?;
                let delta = i128::from(*value) * i128::from(difference);
                let next = i128::from(sum)
                    .checked_add(delta)
                    .and_then(|sum| i64::try_from(sum).ok())
                    .ok_or(AggregateError::ArithmeticOverflow)?;
                write_i64_state(state, count, next);
            }
            (Self::Unsigned, ScalarValue::UInt64(Some(value))) => {
                let (count, sum) = read_u64_state(state)?;
                let count = apply_weight(count, difference)?;
                let magnitude = u128::from(*value) * u128::from(difference.unsigned_abs());
                let next = if difference > 0 {
                    u128::from(sum).checked_add(magnitude)
                } else {
                    u128::from(sum).checked_sub(magnitude)
                }
                .and_then(|sum| u64::try_from(sum).ok())
                .ok_or(AggregateError::ArithmeticOverflow)?;
                write_u64_state(state, count, next);
            }
            _ => return Err(AggregateError::InvalidState),
        }
        Ok(())
    }

    fn output(&self, state: &[u8], _group_weight: u64) -> Result<ScalarValue, AggregateError> {
        match self {
            Self::Signed => {
                let (count, sum) = read_i64_state(state)?;
                Ok(ScalarValue::Int64((count > 0).then_some(sum)))
            }
            Self::Unsigned => {
                let (count, sum) = read_u64_state(state)?;
                Ok(ScalarValue::UInt64((count > 0).then_some(sum)))
            }
        }
    }
}

fn read_i64_state(state: &[u8]) -> Result<(u64, i64), AggregateError> {
    let state: [u8; 16] = state.try_into().map_err(|_| AggregateError::InvalidState)?;
    Ok((
        u64::from_be_bytes(state[..8].try_into().expect("eight-byte slice")),
        i64::from_be_bytes(state[8..].try_into().expect("eight-byte slice")),
    ))
}

fn write_i64_state(state: &mut Vec<u8>, count: u64, sum: i64) {
    state.clear();
    state.extend_from_slice(&count.to_be_bytes());
    state.extend_from_slice(&sum.to_be_bytes());
}

fn read_u64_state(state: &[u8]) -> Result<(u64, u64), AggregateError> {
    let state: [u8; 16] = state.try_into().map_err(|_| AggregateError::InvalidState)?;
    Ok((
        u64::from_be_bytes(state[..8].try_into().expect("eight-byte slice")),
        u64::from_be_bytes(state[8..].try_into().expect("eight-byte slice")),
    ))
}

fn write_u64_state(state: &mut Vec<u8>, count: u64, sum: u64) {
    state.clear();
    state.extend_from_slice(&count.to_be_bytes());
    state.extend_from_slice(&sum.to_be_bytes());
}
