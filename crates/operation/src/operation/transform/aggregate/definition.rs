use std::sync::Arc;

use serde::{Deserialize, Serialize};

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    ConstructedOperation, Expr, OperationSchemaError, definition::schema_error,
    expression::StoredExpression, operation::relation::indexable,
};

use super::{
    AggregateDefinitionError, AggregateSchemaError,
    functions::{StatisticKind, numeric_kind, unsupported},
    runtime::{BoundArgument, BoundExtrema, BoundStatistic},
    value::contains_float,
};

pub(super) const GROUPS: &str = "aggregate.groups";
pub(super) const ENTRIES: &str = "aggregate.entries";
pub(super) const CONTROL: &str = "aggregate.control";

pub(crate) struct AggregateLayout {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[crate::expression::BoundExpression]>,
    pub(super) calls: Box<[AggregateCall<usize>]>,
    pub(super) arguments: Box<[BoundArgument]>,
    pub(super) statistic_count: usize,
    pub(super) layout_count: usize,
    pub(super) extrema_count: usize,
}

/// One built-in aggregate invocation without its output field name.
///
/// Each variant fixes its argument count. The enclosing [`AggregateDefinition`] owns
/// canonical expression persistence and reports any encoding failure once.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateCall<E = Expr> {
    /// Counts every row, including rows containing NULL.
    CountAll,
    /// Counts non-NULL values.
    Count(E),
    /// Sums non-NULL integer values.
    Sum(E),
    /// Averages non-NULL integer values.
    Avg(E),
    /// Finds the smallest non-NULL value.
    Min(E),
    /// Finds the largest non-NULL value.
    Max(E),
}

/// Pure definition of one grouped relational aggregate.
///
/// All grouping expressions and calls are evaluated by one Operation so group
/// ownership, tracked-weight validation, state, and output transitions share one
/// transaction.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateDefinition {
    groups: Box<[NamedExpression]>,
    calls: Box<[NamedCall]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NamedExpression {
    name: String,
    expression: StoredExpression,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NamedCall {
    name: String,
    call: AggregateCall<StoredExpression>,
}

impl<E> AggregateCall<E> {
    fn try_map<T, F>(self, mut map: impl FnMut(E) -> Result<T, F>) -> Result<AggregateCall<T>, F> {
        Ok(match self {
            Self::CountAll => AggregateCall::CountAll,
            Self::Count(value) => AggregateCall::Count(map(value)?),
            Self::Sum(value) => AggregateCall::Sum(map(value)?),
            Self::Avg(value) => AggregateCall::Avg(map(value)?),
            Self::Min(value) => AggregateCall::Min(map(value)?),
            Self::Max(value) => AggregateCall::Max(map(value)?),
        })
    }
}

impl AggregateDefinition {
    /// Creates one non-global aggregate with ordered group fields and calls.
    ///
    /// Calls may be empty, in which case the Operation emits one row for every
    /// present grouping key. Output order is the supplied group fields followed
    /// by the supplied calls.
    ///
    /// # Errors
    ///
    /// Non-replayable expressions (not immutable or not row-local) are rejected.
    ///
    /// Returns [`AggregateDefinitionError`] for an empty grouping list, a value
    /// too large for the stable format, or an expression that cannot be
    /// persisted canonically.
    pub fn try_new<G, A, GN, AN>(groups: G, aggregates: A) -> Result<Self, AggregateDefinitionError>
    where
        G: IntoIterator<Item = (GN, Expr)>,
        A: IntoIterator<Item = (AN, AggregateCall)>,
        GN: Into<String>,
        AN: Into<String>,
    {
        let mut stored_groups = Vec::new();
        for (group, (name, expression)) in groups.into_iter().enumerate() {
            let name = name.into();
            let expression = StoredExpression::try_new(expression)
                .map_err(|source| AggregateDefinitionError::GroupExpression { group, source })?;
            stored_groups.push(NamedExpression { name, expression });
        }
        let mut stored_calls = Vec::new();
        for (aggregate, (name, call)) in aggregates.into_iter().enumerate() {
            let name = name.into();
            let call = call.try_map(|expression| {
                StoredExpression::try_new(expression).map_err(|source| {
                    AggregateDefinitionError::AggregateExpression { aggregate, source }
                })
            })?;
            stored_calls.push(NamedCall { name, call });
        }
        let definition = Self {
            groups: stored_groups.into_boxed_slice(),
            calls: stored_calls.into_boxed_slice(),
        };
        definition.validate()?;
        Ok(definition)
    }
}

impl AggregateDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<Option<SchemaRef>, OperationSchemaError> {
        self.compile_layout(&inputs[0])
            .map(|layout| Some(layout.output_schema))
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Aggregate input arity");
        let layout = self.compile_layout(input_schema).map_err(schema_error)?;
        let output_schema = Arc::clone(&layout.output_schema);
        let operation = super::construct(layout, data)?;
        Ok(ConstructedOperation::new(operation, Some(output_schema)))
    }
}

impl AggregateDefinition {
    fn validate(&self) -> Result<(), AggregateDefinitionError> {
        if self.groups.is_empty() {
            return Err(AggregateDefinitionError::EmptyGroupBy);
        }
        for (index, field) in self.groups.iter().enumerate() {
            ensure_count(index)?;
            ensure_name(&field.name)?;
        }
        for (index, field) in self.calls.iter().enumerate() {
            ensure_count(index)?;
            ensure_name(&field.name)?;
        }
        Ok(())
    }

    pub(super) fn compile_layout(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<AggregateLayout, OperationSchemaError> {
        self.validate()?;
        let mut output_fields = Vec::with_capacity(self.groups.len() + self.calls.len());
        let group_expressions = self.bind_groups(input_schema, &mut output_fields)?;
        let mut calls = Vec::with_capacity(self.calls.len());
        let mut arguments = Vec::new();
        let mut statistic_count = 0;
        let mut layout_count = 0;
        let mut extrema_count = 0;
        for (aggregate, named) in self.calls.iter().enumerate() {
            let call = named.call.clone().try_map(|stored| {
                indexed_argument(&mut arguments, stored, input_schema, aggregate)
            })?;
            let output_type = configure_call(
                &call,
                &mut arguments,
                &mut statistic_count,
                &mut layout_count,
                &mut extrema_count,
            )?;
            output_fields.push(Arc::new(Field::new(
                &named.name,
                output_type,
                !matches!(call, AggregateCall::CountAll | AggregateCall::Count(_)),
            )));
            calls.push(call);
        }
        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            input_schema.metadata().clone(),
        ));
        dogpaddle_change::validate_schema(&output_schema)?;
        Ok(AggregateLayout {
            input_schema: Arc::clone(input_schema),
            output_schema,
            group_expressions,
            calls: calls.into_boxed_slice(),
            arguments: arguments
                .into_iter()
                .map(|(_, argument)| argument)
                .collect(),
            statistic_count,
            layout_count,
            extrema_count,
        })
    }

    fn bind_groups(
        &self,
        input_schema: &SchemaRef,
        output_fields: &mut Vec<Arc<Field>>,
    ) -> Result<Box<[crate::expression::BoundExpression]>, OperationSchemaError> {
        let mut expressions = Vec::with_capacity(self.groups.len());
        for (group, field) in self.groups.iter().enumerate() {
            let expression = field.expression.bind(Arc::clone(input_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(AggregateSchemaError::GroupExpression { group, source })
                },
            )?;
            if contains_float(expression.output_type()) {
                return Err(Box::new(AggregateSchemaError::FloatGroupKey { group }));
            }
            let output = Field::new(
                &field.name,
                expression.output_type().clone(),
                expression.output_nullable(),
            )
            .with_metadata(expression.output_metadata().clone());
            output_fields.push(Arc::new(output));
            expressions.push(expression);
        }
        Ok(expressions.into_boxed_slice())
    }
}

fn indexed_argument(
    arguments: &mut Vec<(StoredExpression, BoundArgument)>,
    stored: StoredExpression,
    input_schema: &SchemaRef,
    owner: usize,
) -> Result<usize, OperationSchemaError> {
    if let Some(index) = arguments
        .iter()
        .position(|(existing, _)| *existing == stored)
    {
        return Ok(index);
    }
    let expression =
        stored
            .bind(Arc::clone(input_schema))
            .map_err(|source| -> OperationSchemaError {
                Box::new(AggregateSchemaError::AggregateExpression {
                    aggregate: owner,
                    source,
                })
            })?;
    let index = arguments.len();
    arguments.push((
        stored,
        BoundArgument {
            owner,
            field: Arc::new(Field::new(
                "argument",
                expression.output_type().clone(),
                expression.output_nullable(),
            )),
            expression,
            statistic: None,
            extrema: None,
        },
    ));
    Ok(index)
}

fn configure_call(
    call: &AggregateCall<usize>,
    arguments: &mut [(StoredExpression, BoundArgument)],
    statistic_count: &mut usize,
    layout_count: &mut usize,
    extrema_count: &mut usize,
) -> Result<DataType, AggregateSchemaError> {
    match *call {
        AggregateCall::CountAll => Ok(DataType::Int64),
        AggregateCall::Count(index) => {
            statistic(
                &mut arguments[index].1,
                statistic_count,
                StatisticKind::Count,
            )
            .count_output = true;
            Ok(DataType::Int64)
        }
        AggregateCall::Sum(index) | AggregateCall::Avg(index) => {
            let argument = &mut arguments[index].1;
            let is_sum = matches!(call, AggregateCall::Sum(_));
            let function = if is_sum { "SUM" } else { "AVG" };
            let kind = numeric_kind(function, argument.field.data_type())?;
            let bound = statistic(argument, statistic_count, kind);
            bound.sum_output |= is_sum;
            Ok(if is_sum {
                argument.field.data_type().clone()
            } else {
                DataType::Float64
            })
        }
        AggregateCall::Min(index) | AggregateCall::Max(index) => {
            let argument = &mut arguments[index].1;
            let is_min = matches!(call, AggregateCall::Min(_));
            let function = if is_min { "MIN" } else { "MAX" };
            if !indexable(argument.field.data_type()) {
                return Err(unsupported(function, argument.field.data_type()));
            }
            let extrema = argument.extrema.get_or_insert_with(|| {
                let partition = *layout_count;
                *layout_count += 1;
                BoundExtrema {
                    partition,
                    min_slot: None,
                    max_slot: None,
                }
            });
            let slot = if is_min {
                &mut extrema.min_slot
            } else {
                &mut extrema.max_slot
            };
            slot.get_or_insert_with(|| {
                let slot = *extrema_count;
                *extrema_count += 1;
                slot
            });
            Ok(argument.field.data_type().clone())
        }
    }
}

fn statistic<'a>(
    argument: &'a mut BoundArgument,
    count: &mut usize,
    kind: StatisticKind,
) -> &'a mut BoundStatistic {
    let statistic = argument.statistic.get_or_insert_with(|| {
        let index = *count;
        *count += 1;
        BoundStatistic {
            index,
            kind,
            count_output: false,
            sum_output: false,
        }
    });
    if kind != StatisticKind::Count {
        statistic.kind = kind;
    }
    statistic
}

fn ensure_count(index: usize) -> Result<(), AggregateDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(AggregateDefinitionError::TooManyFields)
}

fn ensure_name(name: &str) -> Result<(), AggregateDefinitionError> {
    u32::try_from(name.len())
        .map(|_| ())
        .map_err(|_| AggregateDefinitionError::FieldNameTooLong)
}
