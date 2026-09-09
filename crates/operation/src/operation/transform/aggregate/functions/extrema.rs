use super::{BoundReduction, Descriptor, Reduction, unsupported};
use crate::{
    expression::BoundExpression,
    operation::transform::aggregate::{AggregateSchemaError, value::indexable},
};

pub(super) const MIN_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::MIN,
    arguments: 1,
    bind: bind_min,
};

pub(super) const MAX_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::MAX,
    arguments: 1,
    bind: bind_max,
};

#[derive(Clone, Copy)]
pub(in crate::operation::transform::aggregate) enum ExtremaDirection {
    Min,
    Max,
}

fn bind_min(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    bind(arguments, ExtremaDirection::Min, "MIN")
}

fn bind_max(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    bind(arguments, ExtremaDirection::Max, "MAX")
}

fn bind(
    arguments: &[BoundExpression],
    direction: ExtremaDirection,
    name: &'static str,
) -> Result<BoundReduction, AggregateSchemaError> {
    let argument = arguments
        .first()
        .expect("the descriptor validates extrema argument count");
    if !indexable(argument.output_type()) {
        return Err(unsupported(name, argument.output_type()));
    }
    Ok(BoundReduction {
        output_type: argument.output_type().clone(),
        nullable: true,
        reduction: Reduction::Extrema(direction),
    })
}
