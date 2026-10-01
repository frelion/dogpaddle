use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use super::{AggregateError, AggregateSchemaError, state::Statistic};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StatisticKind {
    Count,
    Signed,
    Unsigned,
}

impl StatisticKind {
    pub(super) const fn empty(self) -> Statistic {
        match self {
            Self::Count => Statistic::Count(0),
            Self::Signed => Statistic::Signed { count: 0, sum: 0 },
            Self::Unsigned => Statistic::Unsigned { count: 0, sum: 0 },
        }
    }
}

pub(super) fn numeric_kind(
    function: &'static str,
    data_type: &DataType,
) -> Result<StatisticKind, AggregateSchemaError> {
    match data_type {
        DataType::Int64 => Ok(StatisticKind::Signed),
        DataType::UInt64 => Ok(StatisticKind::Unsigned),
        other => Err(unsupported(function, other)),
    }
}

pub(super) fn unsupported(function: &'static str, data_type: &DataType) -> AggregateSchemaError {
    AggregateSchemaError::UnsupportedArgument {
        function,
        data_type: data_type.clone(),
    }
}

#[derive(Clone, Copy)]
pub(super) enum TrackedWeight {
    Group,
    Call,
}

pub(super) fn apply_weight(
    weight: u64,
    difference: i64,
    tracked: TrackedWeight,
) -> Result<u64, AggregateError> {
    if difference > 0 {
        weight
            .checked_add(difference.unsigned_abs())
            .ok_or(AggregateError::ArithmeticOverflow)
    } else {
        weight
            .checked_sub(difference.unsigned_abs())
            .ok_or(match tracked {
                TrackedWeight::Group => AggregateError::GroupWeightUnderflow,
                TrackedWeight::Call => AggregateError::CallWeightUnderflow,
            })
    }
}

impl Statistic {
    pub(super) fn apply(
        &mut self,
        value: &ScalarValue,
        difference: i64,
    ) -> Result<(), AggregateError> {
        if value.is_null() {
            return Ok(());
        }
        match (self, value) {
            (Self::Count(count), _) => {
                *count = apply_weight(*count, difference, TrackedWeight::Call)?;
            }
            (Self::Signed { count, sum }, ScalarValue::Int64(Some(value))) => {
                *count = apply_weight(*count, difference, TrackedWeight::Call)?;
                *sum = sum
                    .checked_add(i128::from(*value) * i128::from(difference))
                    .ok_or(AggregateError::ArithmeticOverflow)?;
            }
            (Self::Unsigned { count, sum }, ScalarValue::UInt64(Some(value))) => {
                *count = apply_weight(*count, difference, TrackedWeight::Call)?;
                let delta = u128::from(*value) * u128::from(difference.unsigned_abs());
                *sum = if difference > 0 {
                    sum.checked_add(delta)
                } else {
                    sum.checked_sub(delta)
                }
                .ok_or(AggregateError::ArithmeticOverflow)?;
            }
            _ => return Err(AggregateError::InvalidState),
        }
        Ok(())
    }

    pub(super) const fn count(&self) -> u64 {
        match self {
            Self::Count(count) | Self::Signed { count, .. } | Self::Unsigned { count, .. } => {
                *count
            }
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        match self {
            Self::Count(count) => *count == 0,
            Self::Signed { count, sum } => *count == 0 && *sum == 0,
            Self::Unsigned { count, sum } => *count == 0 && *sum == 0,
        }
    }

    pub(super) fn sum(&self) -> Result<ScalarValue, AggregateError> {
        match self {
            Self::Signed { count, sum } => Ok(ScalarValue::Int64(if *count == 0 {
                None
            } else {
                Some(i64::try_from(*sum).map_err(|_| AggregateError::ArithmeticOverflow)?)
            })),
            Self::Unsigned { count, sum } => Ok(ScalarValue::UInt64(if *count == 0 {
                None
            } else {
                Some(u64::try_from(*sum).map_err(|_| AggregateError::ArithmeticOverflow)?)
            })),
            Self::Count(_) => Err(AggregateError::InvalidState),
        }
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "SQL AVG returns Float64 for integer input"
    )]
    pub(super) fn average(&self) -> Result<ScalarValue, AggregateError> {
        let value = match self {
            Self::Signed { count, sum } => (*count > 0).then(|| *sum as f64 / *count as f64),
            Self::Unsigned { count, sum } => (*count > 0).then(|| *sum as f64 / *count as f64),
            Self::Count(_) => return Err(AggregateError::InvalidState),
        };
        Ok(ScalarValue::Float64(value))
    }
}
