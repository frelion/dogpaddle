use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{Schema, SchemaRef};

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, Expr, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::{DataName, Sealed as SealedDefinition},
    expression::StoredExpression,
};

use super::{
    InnerEquiJoinDefinitionError, InnerEquiJoinSchemaError, key_type_supported,
    runtime::{BoundKey, BoundKeyPair, InnerEquiJoinOperation},
    state::{Continuation, Rows},
};

pub(crate) const TAG: u16 = 16;

const LEFT_ROWS: DataName<Rows> = DataName::new("inner_join.left_rows");
const RIGHT_ROWS: DataName<Rows> = DataName::new("inner_join.right_rows");
const CONTINUATION: DataName<Continuation> = DataName::new("inner_join.continuation");
const DATA: &[DataDeclaration] = &[
    LEFT_ROWS.declaration(),
    RIGHT_ROWS.declaration(),
    CONTINUATION.declaration(),
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredKeyPair {
    left: StoredExpression,
    right: StoredExpression,
}

/// Pure definition of a two-input inner equality join.
///
/// Port `0` is permanently the left relation and port `1` is the right
/// relation. Keys are evaluated in declaration order. The output always
/// contains every left field followed by every right field; `output_names`
/// supplies the unique physical names required by a `DogPaddle` Schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InnerEquiJoinDefinition {
    keys: Box<[StoredKeyPair]>,
    output_names: Box<[String]>,
}

impl InnerEquiJoinDefinition {
    /// Creates an inner equality join with immutable ordered key pairs.
    ///
    /// Output-name cardinality and uniqueness depend on the eventual exact
    /// input Schemas and are validated by the [`OperationDefinition`] binding entrypoint.
    ///
    /// # Errors
    ///
    /// Returns [`InnerEquiJoinDefinitionError`] when there are no keys, a
    /// stable count or name length overflows, or an expression is not an
    /// immutable canonical `DataFusion` expression.
    pub fn try_new<K, N, S>(keys: K, output_names: N) -> Result<Self, InnerEquiJoinDefinitionError>
    where
        K: IntoIterator<Item = (Expr, Expr)>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut stored_keys = Vec::new();
        for (key, (left, right)) in keys.into_iter().enumerate() {
            ensure_count(key, "key pairs")?;
            let left = store_key(left, key, "left")?;
            let right = store_key(right, key, "right")?;
            stored_keys.push(StoredKeyPair { left, right });
        }
        if stored_keys.is_empty() {
            return Err(InnerEquiJoinDefinitionError::EmptyKeys);
        }

        let mut names = Vec::new();
        for (output, name) in output_names.into_iter().enumerate() {
            ensure_count(output, "output names")?;
            let name = name.into();
            if u32::try_from(name.len()).is_err() {
                return Err(InnerEquiJoinDefinitionError::OutputNameTooLong { output });
            }
            names.push(name);
        }
        Ok(Self {
            keys: stored_keys.into_boxed_slice(),
            output_names: names.into_boxed_slice(),
        })
    }

    /// Returns ordered left/right key expressions.
    #[must_use]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = (&Expr, &Expr)> {
        self.keys
            .iter()
            .map(|key| (key.left.expression(), key.right.expression()))
    }

    /// Returns physical output names in fixed left-then-right field order.
    #[must_use]
    pub fn output_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.output_names.iter().map(String::as_str)
    }
}

impl SealedDefinition for InnerEquiJoinDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let [left_schema, right_schema] = input_schemas else {
            unreachable!("the final binding entrypoint enforces Join input arity")
        };
        let expected_names = left_schema
            .fields()
            .len()
            .checked_add(right_schema.fields().len())
            .expect("two valid Arrow Schema field counts fit usize");
        if self.output_names.len() != expected_names {
            return Err(Box::new(InnerEquiJoinSchemaError::OutputNameCount {
                expected: expected_names,
                actual: self.output_names.len(),
            }));
        }

        let mut bound_keys = Vec::with_capacity(self.keys.len());
        for (key, stored) in self.keys.iter().enumerate() {
            let left = stored.left.bind(Arc::clone(left_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(InnerEquiJoinSchemaError::KeyExpression {
                        key,
                        side: "left",
                        source,
                    })
                },
            )?;
            let right = stored.right.bind(Arc::clone(right_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(InnerEquiJoinSchemaError::KeyExpression {
                        key,
                        side: "right",
                        source,
                    })
                },
            )?;
            if left.output_type() != right.output_type() {
                return Err(Box::new(InnerEquiJoinSchemaError::KeyTypeMismatch {
                    key,
                    left: left.output_type().clone(),
                    right: right.output_type().clone(),
                }));
            }
            if !key_type_supported(left.output_type()) {
                return Err(Box::new(InnerEquiJoinSchemaError::UnsupportedKeyType {
                    key,
                    data_type: left.output_type().clone(),
                }));
            }
            bound_keys.push(BoundKeyPair {
                left: BoundKey::new(left),
                right: BoundKey::new(right),
            });
        }

        let output_fields = left_schema
            .fields()
            .iter()
            .chain(right_schema.fields())
            .zip(&self.output_names)
            .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new(output_fields));
        let runtime_left_schema = Arc::clone(left_schema);
        let runtime_right_schema = Arc::clone(right_schema);
        let runtime_output_schema = Arc::clone(&output_schema);
        Ok(OperationBinding::turn(
            Some(output_schema),
            move |data: &mut DataInstances| -> Result<InnerEquiJoinOperation, MaterializeError> {
                Ok(InnerEquiJoinOperation::new_bound(
                    [runtime_left_schema, runtime_right_schema],
                    runtime_output_schema,
                    bound_keys.into_boxed_slice(),
                    data.take(&LEFT_ROWS)?,
                    data.take(&RIGHT_ROWS)?,
                    data.take(&CONTINUATION)?,
                ))
            },
        ))
    }
}

impl OperationDefinition for InnerEquiJoinDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::TurnTransform(NonZeroU32::new(2).expect("inner equi-join has two inputs"))
    }

    fn data(&self) -> &'static [DataDeclaration] {
        DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        put_count(output, self.keys.len());
        for key in &self.keys {
            key.left.encode(output);
            key.right.encode(output);
        }
        put_count(output, self.output_names.len());
        for name in &self.output_names {
            put_count(output, name.len());
            output.extend_from_slice(name.as_bytes());
        }
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let mut cursor = PayloadCursor::new(payload);
    let key_count = cursor.read_u32()?;
    if key_count == 0 {
        return Err(DefinitionCodecError::InvalidPayload(
            "inner equi-join key list is empty",
        ));
    }
    let mut keys = Vec::new();
    for _ in 0..key_count {
        let left = decode_key(&mut cursor)?;
        let right = decode_key(&mut cursor)?;
        keys.push(StoredKeyPair { left, right });
    }
    let output_count = cursor.read_u32()?;
    let mut output_names = Vec::new();
    for _ in 0..output_count {
        let length = usize::try_from(cursor.read_u32()?).map_err(|_| {
            DefinitionCodecError::InvalidPayload("inner equi-join output name length is invalid")
        })?;
        let name = cursor.read_bytes(length)?;
        let name = std::str::from_utf8(name).map_err(|_| {
            DefinitionCodecError::InvalidPayload("inner equi-join output name is invalid UTF-8")
        })?;
        output_names.push(name.to_owned());
    }
    cursor.finish()?;
    Ok(Box::new(InnerEquiJoinDefinition {
        keys: keys.into_boxed_slice(),
        output_names: output_names.into_boxed_slice(),
    }))
}

fn store_key(
    expression: Expr,
    key: usize,
    side: &'static str,
) -> Result<StoredExpression, InnerEquiJoinDefinitionError> {
    let expression = StoredExpression::try_new(expression)
        .map_err(|source| InnerEquiJoinDefinitionError::KeyExpression { key, side, source })?;
    if !expression.is_atomic() {
        return Err(InnerEquiJoinDefinitionError::NonImmutableKey { key, side });
    }
    Ok(expression)
}

fn decode_key(cursor: &mut PayloadCursor<'_>) -> Result<StoredExpression, DefinitionCodecError> {
    let expression = StoredExpression::decode(cursor)?;
    if !expression.is_atomic() {
        return Err(DefinitionCodecError::InvalidPayload(
            "inner equi-join key expression is not immutable",
        ));
    }
    Ok(expression)
}

fn ensure_count(index: usize, kind: &'static str) -> Result<(), InnerEquiJoinDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(InnerEquiJoinDefinitionError::TooMany { kind })
}

fn put_count(output: &mut Vec<u8>, count: usize) {
    output.extend_from_slice(
        &u32::try_from(count)
            .expect("InnerEquiJoinDefinition construction bounds persistent counts")
            .to_be_bytes(),
    );
}
