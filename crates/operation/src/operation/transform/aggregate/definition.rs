use std::sync::Arc;

use serde::{Deserialize, Serialize};

use arrow_schema::{Field, Schema, SchemaRef};

use crate::{
    ConstructedOperation, Expr, OperationSchemaError, definition::schema_error,
    expression::StoredExpression,
};

use super::{
    AggregateDefinitionError, AggregateSchemaError,
    functions::{ExtremaDirection, Reduction, bind},
    runtime::{BoundAggregate, BoundArgument, BoundCall, BoundLayout, BoundStatistic, ExtremaSlot},
    value::contains_float,
};

pub(super) const GROUPS: &str = "aggregate.groups";
pub(super) const ENTRIES: &str = "aggregate.entries";
pub(super) const CONTROL: &str = "aggregate.control";

pub(crate) struct AggregateLayout {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[crate::expression::BoundExpression]>,
    pub(super) calls: Box<[BoundCall]>,
    pub(super) arguments: Box<[BoundArgument]>,
    pub(super) statistics: Box<[BoundStatistic]>,
    pub(super) layouts: Box<[BoundLayout]>,
    pub(super) slots: Box<[ExtremaSlot]>,
}

/// One built-in aggregate invocation without its output field name.
///
/// Constructors are infallible. The enclosing [`AggregateDefinition`] owns
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
    /// Creates `COUNT(*)`.
    #[must_use]
    pub const fn count_all() -> Self {
        Self::CountAll
    }
    /// Creates `COUNT(expression)`.
    #[must_use]
    pub fn count(expression: E) -> Self {
        Self::Count(expression)
    }
    /// Creates `SUM(expression)`.
    #[must_use]
    pub fn sum(expression: E) -> Self {
        Self::Sum(expression)
    }
    /// Creates `AVG(expression)`.
    #[must_use]
    pub fn avg(expression: E) -> Self {
        Self::Avg(expression)
    }
    /// Creates `MIN(expression)`.
    #[must_use]
    pub fn min(expression: E) -> Self {
        Self::Min(expression)
    }
    /// Creates `MAX(expression)`.
    #[must_use]
    pub fn max(expression: E) -> Self {
        Self::Max(expression)
    }

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

    fn argument(&self) -> Option<&E> {
        match self {
            Self::CountAll => None,
            Self::Count(value)
            | Self::Sum(value)
            | Self::Avg(value)
            | Self::Min(value)
            | Self::Max(value) => Some(value),
        }
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

    fn compile_layout(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<AggregateLayout, OperationSchemaError> {
        self.validate()?;
        let mut output_fields = Vec::with_capacity(self.groups.len() + self.calls.len());
        let group_expressions = self.bind_groups(input_schema, &mut output_fields)?;
        let BoundAggregate {
            calls,
            arguments,
            statistics,
            layouts,
            slots,
        } = self.bind_calls(input_schema, &mut output_fields)?;
        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            input_schema.metadata().clone(),
        ));
        dogpaddle_change::validate_schema(&output_schema)?;
        Ok(AggregateLayout {
            input_schema: Arc::clone(input_schema),
            output_schema,
            group_expressions,
            calls,
            arguments,
            statistics,
            layouts,
            slots,
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

    pub(super) fn bind_calls(
        &self,
        input_schema: &SchemaRef,
        output_fields: &mut Vec<Arc<Field>>,
    ) -> Result<BoundAggregate, OperationSchemaError> {
        let mut calls = Vec::with_capacity(self.calls.len());
        let mut arguments: Vec<(StoredExpression, BoundArgument)> = Vec::new();
        let mut statistics: Vec<BoundStatistic> = Vec::new();
        let mut layouts: Vec<BoundLayout> = Vec::new();
        let mut slots = Vec::new();
        for (aggregate, call) in self.calls.iter().enumerate() {
            let bound_call = call.call.clone().try_map(|argument| {
                argument
                    .bind(Arc::clone(input_schema))
                    .map_err(|source| -> OperationSchemaError {
                        Box::new(AggregateSchemaError::AggregateExpression { aggregate, source })
                    })
            })?;
            let (bound_argument, bound) = bind(bound_call)?;
            output_fields.push(Arc::new(Field::new(
                &call.name,
                bound.output_type,
                bound.nullable,
            )));
            let argument = bound_argument
                .zip(call.call.argument())
                .map(|(expression, stored)| {
                    indexed_argument(&mut arguments, stored, expression, aggregate)
                });
            calls.push(match bound.reduction {
                Reduction::RowsCount => BoundCall::RowsCount,
                Reduction::Extrema(direction) => {
                    let argument = argument.expect("extrema has one argument");
                    let layout = if let Some(index) = layouts
                        .iter()
                        .position(|layout| layout.argument == argument)
                    {
                        index
                    } else {
                        let index = layouts.len();
                        layouts.push(BoundLayout {
                            argument,
                            field: Arc::clone(&arguments[argument].1.field),
                            min_slot: None,
                            max_slot: None,
                        });
                        index
                    };
                    BoundCall::Extrema {
                        slot: indexed_slot(&mut slots, &mut layouts[layout], layout, direction),
                    }
                }
                reduction => {
                    let argument = argument.expect("statistics have one argument");
                    let kind = match reduction {
                        Reduction::Count => super::functions::StatisticKind::Count,
                        Reduction::Sum(kind) | Reduction::Average(kind) => kind,
                        _ => unreachable!(),
                    };
                    let statistic = if let Some(index) = statistics
                        .iter()
                        .position(|statistic| statistic.argument == argument)
                    {
                        if kind != super::functions::StatisticKind::Count {
                            statistics[index].kind = kind;
                        }
                        index
                    } else {
                        let index = statistics.len();
                        statistics.push(BoundStatistic {
                            argument,
                            kind,
                            count_output: false,
                            sum_output: false,
                        });
                        index
                    };
                    match reduction {
                        Reduction::Count => {
                            statistics[statistic].count_output = true;
                            BoundCall::Count { statistic }
                        }
                        Reduction::Sum(_) => {
                            statistics[statistic].sum_output = true;
                            BoundCall::Sum { statistic }
                        }
                        Reduction::Average(_) => BoundCall::Average { statistic },
                        _ => unreachable!(),
                    }
                }
            });
        }
        Ok(BoundAggregate {
            calls: calls.into_boxed_slice(),
            arguments: arguments
                .into_iter()
                .map(|(_, argument)| argument)
                .collect(),
            statistics: statistics.into_boxed_slice(),
            layouts: layouts.into_boxed_slice(),
            slots: slots.into_boxed_slice(),
        })
    }
}

fn indexed_argument(
    arguments: &mut Vec<(StoredExpression, BoundArgument)>,
    stored: &StoredExpression,
    expression: crate::expression::BoundExpression,
    owner: usize,
) -> usize {
    if let Some(index) = arguments
        .iter()
        .position(|(existing, _)| existing == stored)
    {
        return index;
    }
    let index = arguments.len();
    arguments.push((
        stored.clone(),
        BoundArgument {
            owner,
            field: Arc::new(Field::new(
                "argument",
                expression.output_type().clone(),
                expression.output_nullable(),
            )),
            expression,
        },
    ));
    index
}

/// Returns the dense slot of one (layout, direction) pair, adding it if absent.
///
/// `MIN(x), MAX(x)` share the layout but need one slot each; repeating the same
/// call reuses one slot, so the cached group state stays proportional to the
/// distinct extremes the definition actually reads.
fn indexed_slot(
    slots: &mut Vec<ExtremaSlot>,
    bound_layout: &mut BoundLayout,
    layout: usize,
    direction: ExtremaDirection,
) -> usize {
    let cached_slot = match direction {
        ExtremaDirection::Min => &mut bound_layout.min_slot,
        ExtremaDirection::Max => &mut bound_layout.max_slot,
    };
    if let Some(slot) = *cached_slot {
        return slot;
    }
    let slot = slots.len();
    slots.push(ExtremaSlot { layout });
    *cached_slot = Some(slot);
    slot
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
