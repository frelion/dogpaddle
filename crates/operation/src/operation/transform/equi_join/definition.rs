use std::{num::NonZeroU32, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use datafusion_common::ScalarValue;

use crate::{
    DataDeclaration, DataInstances, DefinitionCodecError, Expr, MaterializeError, OperationBinding,
    OperationDefinition, OperationKind, OperationSchemaError,
    codec::PayloadCursor,
    definition::{DataName, Sealed as SealedDefinition},
    expression::StoredExpression,
};

use super::{
    EquiJoinDefinitionError, EquiJoinKind, EquiJoinSchemaError, key_type_supported,
    runtime::{BoundKey, BoundKeyPair, EquiJoinOperation},
    state::{Continuation, Counts, Rows},
};

pub(crate) const TAG: u16 = 16;

const LEFT_ROWS: DataName<Rows> = DataName::new("equi_join.left_rows");
const RIGHT_ROWS: DataName<Rows> = DataName::new("equi_join.right_rows");
const CONTINUATION: DataName<Continuation> = DataName::new("equi_join.continuation");
const KEY_COUNTS: DataName<Counts> = DataName::new("equi_join.key_counts");
const INNER_DATA: &[DataDeclaration] = &[
    LEFT_ROWS.declaration(),
    RIGHT_ROWS.declaration(),
    CONTINUATION.declaration(),
];
const COUNTED_DATA: &[DataDeclaration] = &[
    LEFT_ROWS.declaration(),
    RIGHT_ROWS.declaration(),
    CONTINUATION.declaration(),
    KEY_COUNTS.declaration(),
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredKeyPair {
    left: StoredExpression,
    right: StoredExpression,
}

/// Pure definition of a two-input equality join.
///
/// Port `0` is permanently the left relation and port `1` is the right
/// relation. Keys are evaluated in declaration order. The output always
/// contains left fields only for Semi/Anti, and every left field followed by
/// every right field for Inner/Outer; `output_names`
/// supplies the unique physical names required by a `DogPaddle` Schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EquiJoinDefinition {
    kind: EquiJoinKind,
    keys: Box<[StoredKeyPair]>,
    output_names: Box<[String]>,
}

impl EquiJoinDefinition {
    /// Creates an equality join with immutable ordered key pairs.
    ///
    /// Output-name cardinality and uniqueness depend on the eventual exact
    /// input Schemas and are validated by the [`OperationDefinition`] binding entrypoint.
    ///
    /// # Errors
    ///
    /// Returns [`EquiJoinDefinitionError`] when there are no keys, a
    /// stable count or name length overflows, or an expression is not an
    /// immutable canonical `DataFusion` expression.
    pub fn try_new<K, N, S>(
        kind: EquiJoinKind,
        keys: K,
        output_names: N,
    ) -> Result<Self, EquiJoinDefinitionError>
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
            return Err(EquiJoinDefinitionError::EmptyKeys);
        }

        let mut names = Vec::new();
        for (output, name) in output_names.into_iter().enumerate() {
            ensure_count(output, "output names")?;
            let name = name.into();
            if u32::try_from(name.len()).is_err() {
                return Err(EquiJoinDefinitionError::OutputNameTooLong { output });
            }
            names.push(name);
        }
        Ok(Self {
            kind,
            keys: stored_keys.into_boxed_slice(),
            output_names: names.into_boxed_slice(),
        })
    }

    /// Returns the relational output semantics.
    #[must_use]
    pub const fn join_kind(&self) -> EquiJoinKind {
        self.kind
    }

    /// Returns ordered left/right key expressions.
    #[must_use]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = (&Expr, &Expr)> {
        self.keys
            .iter()
            .map(|key| (key.left.expression(), key.right.expression()))
    }

    /// Returns physical output names, left-only for Semi/Anti and left-then-right otherwise.
    #[must_use]
    pub fn output_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.output_names.iter().map(String::as_str)
    }
}

impl SealedDefinition for EquiJoinDefinition {
    fn bind_schemas(
        &self,
        input_schemas: &[SchemaRef],
    ) -> Result<OperationBinding, OperationSchemaError> {
        let [left_schema, right_schema] = input_schemas else {
            unreachable!("the final binding entrypoint enforces Join input arity")
        };
        let expected_names = left_schema.fields().len()
            + if self.kind.left_only() {
                0
            } else {
                right_schema.fields().len()
            };
        if self.output_names.len() != expected_names {
            return Err(Box::new(EquiJoinSchemaError::OutputNameCount {
                expected: expected_names,
                actual: self.output_names.len(),
            }));
        }

        let mut bound_keys = Vec::with_capacity(self.keys.len());
        for (key, stored) in self.keys.iter().enumerate() {
            let left = stored.left.bind(Arc::clone(left_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(EquiJoinSchemaError::KeyExpression {
                        key,
                        side: "left",
                        source,
                    })
                },
            )?;
            let right = stored.right.bind(Arc::clone(right_schema)).map_err(
                |source| -> OperationSchemaError {
                    Box::new(EquiJoinSchemaError::KeyExpression {
                        key,
                        side: "right",
                        source,
                    })
                },
            )?;
            if left.output_type() != right.output_type() {
                return Err(Box::new(EquiJoinSchemaError::KeyTypeMismatch {
                    key,
                    left: left.output_type().clone(),
                    right: right.output_type().clone(),
                }));
            }
            if !key_type_supported(left.output_type()) {
                return Err(Box::new(EquiJoinSchemaError::UnsupportedKeyType {
                    key,
                    data_type: left.output_type().clone(),
                }));
            }
            bound_keys.push(BoundKeyPair {
                left: BoundKey::new(left),
                right: BoundKey::new(right),
            });
        }

        let mut output_fields = Vec::with_capacity(expected_names);
        let mut nulls = [Vec::new(), Vec::new()];
        for (port, schema) in input_schemas.iter().enumerate() {
            if port == 1 && self.kind.left_only() {
                break;
            }
            let pad = self.kind.preserves(1 - port);
            for field in schema.fields() {
                let name = &self.output_names[output_fields.len()];
                let mut output = field.as_ref().clone().with_name(name);
                if pad {
                    output = output.with_nullable(true);
                    nulls[port].push(ScalarValue::try_from(field.data_type()).map_err(
                        |source| -> OperationSchemaError {
                            Box::new(EquiJoinSchemaError::NullPadding(source))
                        },
                    )?);
                }
                output_fields.push(Arc::new(output));
            }
        }
        let output_schema = Arc::new(Schema::new(output_fields));
        let runtime_left_schema = Arc::clone(left_schema);
        let runtime_right_schema = Arc::clone(right_schema);
        let runtime_output_schema = Arc::clone(&output_schema);
        let kind = self.kind;
        Ok(OperationBinding::turn(
            Some(output_schema),
            move |data: &mut DataInstances| -> Result<EquiJoinOperation, MaterializeError> {
                Ok(EquiJoinOperation {
                    kind,
                    input_schemas: [runtime_left_schema, runtime_right_schema],
                    output_schema: runtime_output_schema,
                    keys: bound_keys.into_boxed_slice(),
                    nulls,
                    left_rows: data.take(&LEFT_ROWS)?,
                    right_rows: data.take(&RIGHT_ROWS)?,
                    continuation: data.take(&CONTINUATION)?,
                    key_counts: if kind == EquiJoinKind::Inner {
                        None
                    } else {
                        Some(data.take(&KEY_COUNTS)?)
                    },
                    prepared: None,
                })
            },
        ))
    }
}

impl OperationDefinition for EquiJoinDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::TurnTransform(NonZeroU32::new(2).expect("equi-join has two inputs"))
    }

    fn data(&self) -> &'static [DataDeclaration] {
        if self.kind == EquiJoinKind::Inner {
            INNER_DATA
        } else {
            COUNTED_DATA
        }
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.push(self.kind.code());
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
    let kind = EquiJoinKind::from_code(cursor.read_bytes(1)?[0]).ok_or(
        DefinitionCodecError::InvalidPayload("equi-join kind is invalid"),
    )?;
    let key_count = cursor.read_u32()?;
    if key_count == 0 {
        return Err(DefinitionCodecError::InvalidPayload(
            "equi-join key list is empty",
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
            DefinitionCodecError::InvalidPayload("equi-join output name length is invalid")
        })?;
        let name = cursor.read_bytes(length)?;
        let name = std::str::from_utf8(name).map_err(|_| {
            DefinitionCodecError::InvalidPayload("equi-join output name is invalid UTF-8")
        })?;
        output_names.push(name.to_owned());
    }
    cursor.finish()?;
    Ok(Box::new(EquiJoinDefinition {
        kind,
        keys: keys.into_boxed_slice(),
        output_names: output_names.into_boxed_slice(),
    }))
}

fn store_key(
    expression: Expr,
    key: usize,
    side: &'static str,
) -> Result<StoredExpression, EquiJoinDefinitionError> {
    let expression = StoredExpression::try_new(expression)
        .map_err(|source| EquiJoinDefinitionError::KeyExpression { key, side, source })?;
    if !expression.is_atomic() {
        return Err(EquiJoinDefinitionError::NonImmutableKey { key, side });
    }
    Ok(expression)
}

fn decode_key(cursor: &mut PayloadCursor<'_>) -> Result<StoredExpression, DefinitionCodecError> {
    let expression = StoredExpression::decode(cursor)?;
    if !expression.is_atomic() {
        return Err(DefinitionCodecError::InvalidPayload(
            "equi-join key expression is not immutable",
        ));
    }
    Ok(expression)
}

fn ensure_count(index: usize, kind: &'static str) -> Result<(), EquiJoinDefinitionError> {
    index
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .map(|_| ())
        .ok_or(EquiJoinDefinitionError::TooMany { kind })
}

fn put_count(output: &mut Vec<u8>, count: usize) {
    output.extend_from_slice(
        &u32::try_from(count)
            .expect("EquiJoinDefinition construction bounds persistent counts")
            .to_be_bytes(),
    );
}
