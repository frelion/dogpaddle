use std::cmp::Ordering;

use arrow_schema::Field;
use datafusion_common::ScalarValue;

use super::{Binder, BoundReduction, Descriptor, Indexed, Reduction, argument_field, unsupported};
use crate::{
    expression::BoundExpression,
    operation::transform::aggregate::{
        AggregateError, AggregateSchemaError,
        value::{indexable, null, scalar},
    },
};

pub(super) const MIN_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::MIN,
    arguments: 1,
    bind: Binder::Fallible(bind_min),
};

pub(super) const MAX_DESCRIPTOR: Descriptor = Descriptor {
    tag: super::MAX,
    arguments: 1,
    bind: Binder::Fallible(bind_max),
};

enum Direction {
    Min,
    Max,
}

struct Extrema {
    direction: Direction,
    field: Field,
}

fn bind_min(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    bind(arguments, Direction::Min, "MIN")
}

fn bind_max(arguments: &[BoundExpression]) -> Result<BoundReduction, AggregateSchemaError> {
    bind(arguments, Direction::Max, "MAX")
}

fn bind(
    arguments: &[BoundExpression],
    direction: Direction,
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
        reduction: Reduction::Indexed(Box::new(Extrema {
            direction,
            field: argument_field(argument),
        })),
    })
}

impl Indexed for Extrema {
    fn empty(&self) -> Vec<u8> {
        Vec::new()
    }

    fn change(
        &self,
        state: &mut Vec<u8>,
        values: &[ScalarValue],
        encoded: &[u8],
        presence: Option<i64>,
    ) -> Result<bool, AggregateError> {
        let [value] = values else {
            return Err(AggregateError::InvalidState);
        };
        if value.is_null() {
            return Ok(false);
        }
        match presence {
            Some(1) => {
                if state.is_empty() || self.better(value, &scalar(&self.field, state)?)? {
                    state.clear();
                    state.extend_from_slice(encoded);
                }
                Ok(false)
            }
            Some(-1) => Ok(state == encoded),
            None => Ok(false),
            Some(_) => Err(AggregateError::InvalidState),
        }
    }

    fn begin_scan(&self) -> Vec<u8> {
        Vec::new()
    }

    fn push(
        &self,
        scan: &mut Vec<u8>,
        values: &[ScalarValue],
        encoded: &[u8],
        _weight: u64,
    ) -> Result<(), AggregateError> {
        let [value] = values else {
            return Err(AggregateError::InvalidState);
        };
        if value.is_null() {
            return Ok(());
        }
        if scan.is_empty() || self.better(value, &scalar(&self.field, scan)?)? {
            scan.clear();
            scan.extend_from_slice(encoded);
        }
        Ok(())
    }

    fn finish_scan(&self, state: &mut Vec<u8>, scan: Vec<u8>) {
        *state = scan;
    }

    fn output(&self, state: &[u8]) -> Result<ScalarValue, AggregateError> {
        if state.is_empty() {
            null(self.field.data_type())
        } else {
            scalar(&self.field, state)
        }
    }
}

impl Extrema {
    fn better(
        &self,
        candidate: &ScalarValue,
        current: &ScalarValue,
    ) -> Result<bool, AggregateError> {
        let ordering = candidate
            .partial_cmp(current)
            .ok_or(AggregateError::InvalidState)?;
        Ok(match self.direction {
            Direction::Min => ordering == Ordering::Less,
            Direction::Max => ordering == Ordering::Greater,
        })
    }
}
