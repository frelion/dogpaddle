use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use arrow_schema::{Field, Schema, SchemaRef};

use crate::{
    ConstructedOperation, DefinitionCodecError, Expr, OperationSchemaError,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::schema_error,
    expression::StoredExpression,
};

use super::{
    AggregateDefinitionError, AggregateSchemaError,
    functions::{AVG, COUNT, COUNT_ALL, ExtremaDirection, MAX, MIN, Reduction, SUM, descriptor},
    runtime::{BoundAggregate, BoundCall, BoundLayout, ExtremaSlot},
    value::contains_float,
};

pub(crate) const TAG: u16 = 14;

pub(super) const GROUPS: &str = "aggregate.groups";
pub(super) const ENTRIES: &str = "aggregate.entries";
pub(super) const CONTROL: &str = "aggregate.control";

pub(crate) struct AggregateLayout {
    pub(super) input_schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) group_expressions: Box<[crate::expression::BoundExpression]>,
    pub(super) calls: Box<[BoundCall]>,
    pub(super) layouts: Box<[BoundLayout]>,
    pub(super) slots: Box<[ExtremaSlot]>,
}

/// One built-in aggregate invocation without its output field name.
///
/// Constructors are infallible. The enclosing [`AggregateDefinition`] owns
/// canonical expression persistence and reports any encoding failure once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateCall {
    function: u16,
    arguments: Box<[Expr]>,
}

/// Pure definition of one grouped relational aggregate.
///
/// All grouping expressions and calls are evaluated by one Operation so group
/// ownership, tracked-weight validation, state, and output transitions share one
/// transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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
    function: u16,
    arguments: Box<[StoredExpression]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    groups: Box<[NamedExpression]>,
    calls: Box<[NamedCall]>,
}

impl Payload {
    fn into_definition(self) -> Result<AggregateDefinition, &'static str> {
        if self.groups.is_empty() {
            return Err("Aggregate GROUP BY is empty");
        }
        for call in &self.calls {
            let Some(descriptor) = descriptor(call.function) else {
                return Err("Aggregate function tag is unknown");
            };
            if call.arguments.len() != descriptor.arguments {
                return Err("Aggregate function argument count is invalid");
            }
        }
        Ok(AggregateDefinition {
            groups: self.groups,
            calls: self.calls,
        })
    }
}

impl<'de> Deserialize<'de> for AggregateDefinition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Payload::deserialize(deserializer)?
            .into_definition()
            .map_err(D::Error::custom)
    }
}

impl AggregateCall {
    /// Creates `COUNT(*)`.
    #[must_use]
    pub fn count_all() -> Self {
        Self::new(COUNT_ALL, [])
    }

    /// Creates `COUNT(expression)`.
    #[must_use]
    pub fn count(expression: Expr) -> Self {
        Self::new(COUNT, [expression])
    }

    /// Creates `SUM(expression)`.
    #[must_use]
    pub fn sum(expression: Expr) -> Self {
        Self::new(SUM, [expression])
    }

    /// Creates `AVG(expression)`.
    #[must_use]
    pub fn avg(expression: Expr) -> Self {
        Self::new(AVG, [expression])
    }

    /// Creates `MIN(expression)`.
    #[must_use]
    pub fn min(expression: Expr) -> Self {
        Self::new(MIN, [expression])
    }

    /// Creates `MAX(expression)`.
    #[must_use]
    pub fn max(expression: Expr) -> Self {
        Self::new(MAX, [expression])
    }

    fn new(function: u16, arguments: impl IntoIterator<Item = Expr>) -> Self {
        Self {
            function,
            arguments: arguments.into_iter().collect(),
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
            ensure_count(group)?;
            let name = name.into();
            ensure_name(&name)?;
            let expression = StoredExpression::try_new(expression)
                .map_err(|source| AggregateDefinitionError::GroupExpression { group, source })?;
            stored_groups.push(NamedExpression { name, expression });
        }
        if stored_groups.is_empty() {
            return Err(AggregateDefinitionError::EmptyGroupBy);
        }

        let mut stored_calls = Vec::new();
        for (aggregate, (name, call)) in aggregates.into_iter().enumerate() {
            ensure_count(aggregate)?;
            let name = name.into();
            ensure_name(&name)?;
            let mut arguments = Vec::with_capacity(call.arguments.len());
            for expression in call.arguments {
                arguments.push(StoredExpression::try_new(expression).map_err(|source| {
                    AggregateDefinitionError::AggregateExpression { aggregate, source }
                })?);
            }
            stored_calls.push(NamedCall {
                name,
                function: call.function,
                arguments: arguments.into_boxed_slice(),
            });
        }
        Ok(Self {
            groups: stored_groups.into_boxed_slice(),
            calls: stored_calls.into_boxed_slice(),
        })
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
    fn compile_layout(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<AggregateLayout, OperationSchemaError> {
        let mut output_fields = Vec::with_capacity(self.groups.len() + self.calls.len());
        let group_expressions = self.bind_groups(input_schema, &mut output_fields)?;
        let BoundAggregate {
            calls,
            layouts,
            slots,
        } = self.bind_calls(input_schema, &mut output_fields)?;
        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            input_schema.metadata().clone(),
        ));
        Ok(AggregateLayout {
            input_schema: Arc::clone(input_schema),
            output_schema,
            group_expressions,
            calls,
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

    fn bind_calls(
        &self,
        input_schema: &SchemaRef,
        output_fields: &mut Vec<Arc<Field>>,
    ) -> Result<BoundAggregate, OperationSchemaError> {
        let mut calls = Vec::with_capacity(self.calls.len());
        let mut layouts: Vec<(StoredExpression, BoundLayout)> = Vec::new();
        let mut slots: Vec<ExtremaSlot> = Vec::new();
        let mut fold_states = 0;
        for (aggregate, call) in self.calls.iter().enumerate() {
            let function = descriptor(call.function)
                .expect("AggregateDefinition construction and decoding admit known functions");
            let arguments = call
                .arguments
                .iter()
                .map(|argument| argument.bind(Arc::clone(input_schema)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| -> OperationSchemaError {
                    Box::new(AggregateSchemaError::AggregateExpression { aggregate, source })
                })?;
            let bound = (function.bind)(&arguments)
                .map_err(|error| Box::new(error) as OperationSchemaError)?;
            output_fields.push(Arc::new(Field::new(
                &call.name,
                bound.output_type,
                bound.nullable,
            )));

            match bound.reduction {
                Reduction::Fold(reduction) => {
                    calls.push(BoundCall::Fold {
                        state: fold_states,
                        arguments: arguments.into_boxed_slice(),
                        reduction,
                    });
                    fold_states += 1;
                }
                Reduction::Extrema(direction) => {
                    let stored = call
                        .arguments
                        .first()
                        .expect("extrema has exactly one stored argument");
                    let argument = arguments
                        .into_iter()
                        .next()
                        .expect("extrema has exactly one bound argument");
                    let layout = indexed_layout(&mut layouts, stored, argument, aggregate);
                    let slot = indexed_slot(&mut slots, &mut layouts[layout].1, layout, direction);
                    calls.push(BoundCall::Extrema { slot });
                }
            }
        }
        let layouts = layouts
            .into_iter()
            .map(|(_, layout)| layout)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(BoundAggregate {
            calls: calls.into_boxed_slice(),
            layouts,
            slots: slots.into_boxed_slice(),
        })
    }
}

fn indexed_layout(
    layouts: &mut Vec<(StoredExpression, BoundLayout)>,
    stored: &StoredExpression,
    argument: crate::expression::BoundExpression,
    owner: usize,
) -> usize {
    if let Some(position) = layouts
        .iter()
        .position(|(expression, _)| expression == stored)
    {
        return position;
    }
    let position = layouts.len();
    layouts.push((
        stored.clone(),
        BoundLayout {
            owner,
            field: Arc::new(Field::new(
                "argument",
                argument.output_type().clone(),
                argument.output_nullable(),
            )),
            expression: argument,
            min_slot: None,
            max_slot: None,
        },
    ));
    position
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

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<AggregateDefinition>, DefinitionCodecError> {
    let definition = parse_json_payload::<Payload>(payload)?
        .into_definition()
        .map_err(DefinitionCodecError::InvalidPayload)?;
    require_canonical_json_payload(&definition, payload, "invalid Aggregate payload")?;
    Ok(Box::new(definition))
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
