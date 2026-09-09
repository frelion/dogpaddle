use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;

use crate::expression::BoundExpression;

use super::{AggregateError, AggregateSchemaError};

mod avg;
mod count;
mod extrema;
mod sum;

pub(super) use extrema::ExtremaDirection;

pub(super) const COUNT_ALL: u16 = 1;
pub(super) const COUNT: u16 = 2;
pub(super) const SUM: u16 = 3;
pub(super) const AVG: u16 = 4;
pub(super) const MIN: u16 = 5;
pub(super) const MAX: u16 = 6;

pub(super) struct Descriptor {
    pub(super) tag: u16,
    pub(super) arguments: usize,
    pub(super) bind: fn(&[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError>,
}

pub(super) struct BoundReduction {
    pub(super) reduction: Reduction,
    pub(super) output_type: DataType,
    pub(super) nullable: bool,
}

pub(super) enum Reduction {
    Fold(Box<dyn Fold>),
    Extrema(extrema::ExtremaDirection),
}

pub(super) trait Fold: Send {
    fn empty(&self) -> Vec<u8>;

    fn apply(
        &self,
        state: &mut Vec<u8>,
        values: &[ScalarValue],
        difference: i64,
        group_weight: u64,
    ) -> Result<(), AggregateError>;

    fn output(&self, state: &[u8], group_weight: u64) -> Result<ScalarValue, AggregateError>;
}

const DESCRIPTORS: &[Descriptor] = &[
    count::COUNT_ALL_DESCRIPTOR,
    count::COUNT_DESCRIPTOR,
    sum::DESCRIPTOR,
    avg::DESCRIPTOR,
    extrema::MIN_DESCRIPTOR,
    extrema::MAX_DESCRIPTOR,
];

pub(super) fn descriptor(tag: u16) -> Option<&'static Descriptor> {
    DESCRIPTORS.iter().find(|descriptor| descriptor.tag == tag)
}

pub(super) fn argument_field(expression: &BoundExpression) -> Field {
    Field::new(
        "argument",
        expression.output_type().clone(),
        expression.output_nullable(),
    )
}

pub(super) fn unsupported(function: &'static str, data_type: &DataType) -> AggregateSchemaError {
    AggregateSchemaError::UnsupportedArgument {
        function,
        data_type: data_type.clone(),
    }
}

pub(super) fn apply_weight(weight: u64, difference: i64) -> Result<u64, AggregateError> {
    let next = if difference > 0 {
        weight.checked_add(difference.unsigned_abs())
    } else {
        weight.checked_sub(difference.unsigned_abs())
    };
    next.ok_or(AggregateError::ArithmeticOverflow)
}

pub(super) fn read_u64(state: &[u8]) -> Result<u64, AggregateError> {
    state
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| AggregateError::InvalidState)
}

pub(super) fn write_u64(state: &mut Vec<u8>, value: u64) {
    state.clear();
    state.extend_from_slice(&value.to_be_bytes());
}
