use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use super::{Binder, BoundReduction, Descriptor, Fold, Reduction, apply_weight, unsupported};
use crate::{
    expression::BoundExpression,
    operation::transform::aggregate::{AggregateError, AggregateSchemaError},
};

pub(super) const DESCRIPTOR: Descriptor = Descriptor {
    tag: super::AVG,
    arguments: 1,
    bind: Binder::Fallible(bind),
};

enum Average {
    Signed,
    Unsigned,
}

fn bind(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    let input = arguments
        .first()
        .expect("the descriptor validates AVG argument count")
        .output_type();
    let average = match input {
        DataType::Int64 => Average::Signed,
        DataType::UInt64 => Average::Unsigned,
        other => return Err(unsupported("AVG", other)),
    };
    Ok(BoundReduction {
        reduction: Reduction::Fold(Box::new(average)),
        output_type: DataType::Float64,
        nullable: true,
    })
}

impl Fold for Average {
    fn empty(&self) -> Vec<u8> {
        vec![0; 24]
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
                let (count, sum) = read_i128_state(state)?;
                let count = apply_weight(count, difference)?;
                let delta = i128::from(*value) * i128::from(difference);
                let sum = sum
                    .checked_add(delta)
                    .ok_or(AggregateError::ArithmeticOverflow)?;
                write_i128_state(state, count, sum);
            }
            (Self::Unsigned, ScalarValue::UInt64(Some(value))) => {
                let (count, sum) = read_u128_state(state)?;
                let count = apply_weight(count, difference)?;
                let delta = u128::from(*value) * u128::from(difference.unsigned_abs());
                let sum = if difference > 0 {
                    sum.checked_add(delta)
                } else {
                    sum.checked_sub(delta)
                }
                .ok_or(AggregateError::ArithmeticOverflow)?;
                write_u128_state(state, count, sum);
            }
            _ => return Err(AggregateError::InvalidState),
        }
        Ok(())
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "SQL AVG returns Float64 for integer input"
    )]
    fn output(&self, state: &[u8], _group_weight: u64) -> Result<ScalarValue, AggregateError> {
        let value = match self {
            Self::Signed => {
                let (count, sum) = read_i128_state(state)?;
                (count > 0).then(|| sum as f64 / count as f64)
            }
            Self::Unsigned => {
                let (count, sum) = read_u128_state(state)?;
                (count > 0).then(|| sum as f64 / count as f64)
            }
        };
        Ok(ScalarValue::Float64(value))
    }
}

fn read_i128_state(state: &[u8]) -> Result<(u64, i128), AggregateError> {
    let state: [u8; 24] = state.try_into().map_err(|_| AggregateError::InvalidState)?;
    Ok((
        u64::from_be_bytes(state[..8].try_into().expect("eight-byte slice")),
        i128::from_be_bytes(state[8..].try_into().expect("16-byte slice")),
    ))
}

fn write_i128_state(state: &mut Vec<u8>, count: u64, sum: i128) {
    state.clear();
    state.extend_from_slice(&count.to_be_bytes());
    state.extend_from_slice(&sum.to_be_bytes());
}

fn read_u128_state(state: &[u8]) -> Result<(u64, u128), AggregateError> {
    let state: [u8; 24] = state.try_into().map_err(|_| AggregateError::InvalidState)?;
    Ok((
        u64::from_be_bytes(state[..8].try_into().expect("eight-byte slice")),
        u128::from_be_bytes(state[8..].try_into().expect("16-byte slice")),
    ))
}

fn write_u128_state(state: &mut Vec<u8>, count: u64, sum: u128) {
    state.clear();
    state.extend_from_slice(&count.to_be_bytes());
    state.extend_from_slice(&sum.to_be_bytes());
}
