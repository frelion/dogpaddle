use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{Field, Schema, SchemaRef};

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, Expr, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::{DataName, Sealed as SealedDefinition},
    expression::StoredExpression,
    operation::Operation,
};

use super::{
    AggregateDefinitionError, AggregateSchemaError,
    functions::{AVG, COUNT, COUNT_ALL, MAX, MIN, Reduction, SUM, argument_field, descriptor},
    runtime::{AggregateOperation, BoundCall, BoundLayout},
    state::{Control, Entries, Groups},
    value::contains_float,
};

pub(crate) const TAG: u16 = 14;

const GROUPS: DataName<Groups> = DataName::new("aggregate.groups");
const ENTRIES: DataName<Entries> = DataName::new("aggregate.entries");
const CONTROL: DataName<Control> = DataName::new("aggregate.control");
const DATA: &[DataDeclaration] = &[
    GROUPS.declaration(),
    ENTRIES.declaration(),
    CONTROL.declaration(),
];

type BoundCalls = (Box<[BoundCall]>, Box<[BoundLayout]>);

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
/// ownership, exact-row admission, state, and output transitions share one
/// transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateDefinition {
    groups: Box<[NamedExpression]>,
    calls: Box<[NamedCall]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NamedExpression {
    name: String,
    expression: StoredExpression,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NamedCall {
    name: String,
    function: u16,
    arguments: Box<[StoredExpression]>,
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

impl SealedDefinition for AggregateDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces Aggregate input arity");
        let mut output_fields = Vec::with_capacity(self.groups.len() + self.calls.len());
        let group_expressions = self.bind_groups(input_schema, &mut output_fields)?;
        let (calls, layouts) = self.bind_calls(input_schema, &mut output_fields)?;

        let output_schema = Arc::new(Schema::new_with_metadata(
            output_fields,
            input_schema.metadata().clone(),
        ));
        let runtime_input = Arc::clone(input_schema);
        let runtime_output = Arc::clone(&output_schema);
        Ok(OperationBinding::new(
            Some(output_schema),
            move |data: &mut DataInstances| -> Result<Box<dyn Operation>, MaterializeError> {
                Ok(Box::new(AggregateOperation {
                    input_schema: runtime_input,
                    output_schema: runtime_output,
                    group_expressions,
                    calls,
                    layouts,
                    groups: data.take(&GROUPS)?,
                    entries: data.take(&ENTRIES)?,
                    control: data.take(&CONTROL)?,
                }))
            },
        ))
    }
}

impl AggregateDefinition {
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
    ) -> Result<BoundCalls, OperationSchemaError> {
        let mut calls = Vec::with_capacity(self.calls.len());
        let mut layouts: Vec<(StoredExpression, BoundLayout)> = Vec::new();
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
                    calls.push(BoundCall::fold(
                        fold_states,
                        arguments.into_boxed_slice(),
                        reduction,
                    ));
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
                    calls.push(BoundCall::extrema(layout, direction));
                }
            }
        }
        let layouts = layouts
            .into_iter()
            .map(|(_, layout)| layout)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok((calls.into_boxed_slice(), layouts))
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
    let id = u32::try_from(position + 1).expect("stable Aggregate call count bounds layout IDs");
    layouts.push((
        stored.clone(),
        BoundLayout {
            id,
            owner,
            field: Arc::new(argument_field(&argument)),
            expression: argument,
        },
    ));
    position
}

impl OperationDefinition for AggregateDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Transform(NonZeroU32::MIN)
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        put_count(output, self.groups.len());
        for group in &self.groups {
            put_name(output, &group.name);
            group.expression.encode(output);
        }
        put_count(output, self.calls.len());
        for call in &self.calls {
            put_name(output, &call.name);
            output.extend_from_slice(&call.function.to_be_bytes());
            put_count(output, call.arguments.len());
            for argument in &call.arguments {
                argument.encode(output);
            }
        }
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let group_count = cursor.read_u32()?;
    if group_count == 0 {
        return Err(DefinitionCodecError::InvalidPayload(
            "Aggregate GROUP BY is empty",
        ));
    }
    let mut groups = Vec::new();
    for _ in 0..group_count {
        groups.push(NamedExpression {
            name: read_name(&mut cursor)?,
            expression: StoredExpression::decode(&mut cursor)?,
        });
    }

    let call_count = cursor.read_u32()?;
    let mut calls = Vec::new();
    for _ in 0..call_count {
        let name = read_name(&mut cursor)?;
        let function = cursor.read_u16()?;
        let descriptor = descriptor(function).ok_or(DefinitionCodecError::InvalidPayload(
            "Aggregate function tag is unknown",
        ))?;
        let argument_count = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("Aggregate argument count is invalid")
        })?;
        if argument_count != descriptor.arguments {
            return Err(DefinitionCodecError::InvalidPayload(
                "Aggregate function argument count is invalid",
            ));
        }
        let mut arguments = Vec::new();
        for _ in 0..argument_count {
            arguments.push(StoredExpression::decode(&mut cursor)?);
        }
        calls.push(NamedCall {
            name,
            function,
            arguments: arguments.into_boxed_slice(),
        });
    }
    cursor.finish()?;
    Ok(Box::new(AggregateDefinition {
        groups: groups.into_boxed_slice(),
        calls: calls.into_boxed_slice(),
    }))
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

fn put_count(output: &mut Vec<u8>, count: usize) {
    output.extend_from_slice(
        &u32::try_from(count)
            .expect("AggregateDefinition construction bounds field counts")
            .to_be_bytes(),
    );
}

fn put_name(output: &mut Vec<u8>, name: &str) {
    put_count(output, name.len());
    output.extend_from_slice(name.as_bytes());
}

fn read_name(cursor: &mut PayloadCursor<'_>) -> Result<String, DefinitionCodecError> {
    let length = usize::try_from(cursor.read_u32()?)
        .map_err(|_| DefinitionCodecError::InvalidPayload("Aggregate name length is invalid"))?;
    let name = cursor.read_bytes(length)?;
    std::str::from_utf8(name)
        .map(str::to_owned)
        .map_err(|_| DefinitionCodecError::InvalidPayload("Aggregate name is invalid UTF-8"))
}
