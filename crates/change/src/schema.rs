use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field, Fields, Schema};
use thiserror::Error;

/// Maximum number of nested Arrow List or Struct boundaries.
pub const MAX_NESTING_DEPTH: usize = 60;

pub(crate) const RESERVED_FIELD_PREFIX: &str = "$dogpaddle.";
pub(crate) const RESERVED_METADATA_PREFIX: &str = "dogpaddle.";

#[derive(Clone, Copy)]
pub(crate) enum DataTypeLayout<'a> {
    Null,
    Bitmap,
    FixedWidth(usize),
    VariableWidth,
    List(&'a Field),
    Struct(&'a Fields),
}

impl<'a> DataTypeLayout<'a> {
    pub(crate) fn classify(data_type: &'a DataType) -> Option<Self> {
        match data_type {
            DataType::Null => Some(Self::Null),
            DataType::Boolean => Some(Self::Bitmap),
            DataType::Int8 | DataType::UInt8 => Some(Self::FixedWidth(1)),
            DataType::Int16 | DataType::UInt16 => Some(Self::FixedWidth(2)),
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => Some(Self::FixedWidth(4)),
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => Some(Self::FixedWidth(8)),
            DataType::Utf8 | DataType::Binary => Some(Self::VariableWidth),
            DataType::List(child) => Some(Self::List(child)),
            DataType::Struct(fields) => Some(Self::Struct(fields)),
            _ => None,
        }
    }

    pub(crate) const fn own_buffer_count(self) -> usize {
        match self {
            Self::Null => 0,
            Self::Struct(_) => 1,
            Self::Bitmap | Self::FixedWidth(_) | Self::List(_) => 2,
            Self::VariableWidth => 3,
        }
    }
}

/// Validates a logical `DogPaddle` record schema.
///
/// Field order, names, nullability, data types, and metadata remain part of
/// Arrow schema identity. Field names must be unique within each Schema or
/// Struct scope, and v1 deliberately accepts only the documented type subset.
/// Field names beginning with `$dogpaddle.` and Schema or Field metadata keys
/// beginning with `dogpaddle.` are reserved for the physical Change protocol.
///
/// # Errors
///
/// Returns `SchemaError` for duplicate or reserved field names, reserved
/// metadata, unsupported Arrow types, or nesting deeper than
/// `MAX_NESTING_DEPTH`.
pub fn validate_schema(schema: &Schema) -> Result<(), SchemaError> {
    validate_metadata(schema.metadata(), "schema")?;
    validate_fields(schema.fields(), "", 0)
}

/// A logical record schema validation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SchemaError {
    /// A field name occurs more than once in one Schema or Struct scope.
    #[error("duplicate field {name:?} in schema scope {scope:?}")]
    DuplicateField {
        /// Dot-separated diagnostic path to the containing scope.
        scope: String,
        /// The duplicated field name.
        name: String,
    },
    /// A logical field name uses `DogPaddle`'s physical protocol namespace.
    #[error("reserved field name {name:?} at field {field:?}")]
    ReservedFieldName {
        /// Dot-separated diagnostic path to the field.
        field: String,
        /// Reserved field name.
        name: String,
    },
    /// Logical Schema or Field metadata uses `DogPaddle`'s protocol namespace.
    #[error("reserved metadata key {key:?} on {owner:?}")]
    ReservedMetadataKey {
        /// Schema or dot-separated field path owning the metadata.
        owner: String,
        /// Reserved metadata key.
        key: String,
    },
    /// A field uses an Arrow type outside `DogPaddle`'s v1 subset.
    #[error("unsupported Arrow type {data_type} at field {field:?}")]
    UnsupportedType {
        /// Dot-separated diagnostic path to the field.
        field: String,
        /// The unsupported Arrow type.
        data_type: DataType,
    },
    /// Nested Lists or Structs exceed the stable depth limit.
    #[error("schema nesting exceeds the maximum depth of {max_depth}")]
    NestingTooDeep {
        /// The supported maximum depth.
        max_depth: usize,
    },
}

fn validate_fields(fields: &Fields, scope: &str, depth: usize) -> Result<(), SchemaError> {
    let mut names = HashSet::with_capacity(fields.len());
    for field in fields {
        if !names.insert(field.name().as_str()) {
            return Err(SchemaError::DuplicateField {
                scope: scope.to_owned(),
                name: field.name().clone(),
            });
        }
        let path = join_path(scope, field.name());
        validate_field(field, &path, depth)?;
    }
    Ok(())
}

fn validate_field(field: &Field, path: &str, depth: usize) -> Result<(), SchemaError> {
    if field.name().starts_with(RESERVED_FIELD_PREFIX) {
        return Err(SchemaError::ReservedFieldName {
            field: path.to_owned(),
            name: field.name().clone(),
        });
    }
    validate_metadata(field.metadata(), path)?;
    match DataTypeLayout::classify(field.data_type()) {
        Some(DataTypeLayout::List(child)) => {
            let nested = enter_container(depth)?;
            validate_field(child, &join_path(path, child.name()), nested)
        }
        Some(DataTypeLayout::Struct(fields)) => {
            let nested = enter_container(depth)?;
            validate_fields(fields, path, nested)
        }
        Some(_) => Ok(()),
        None => Err(SchemaError::UnsupportedType {
            field: path.to_owned(),
            data_type: field.data_type().clone(),
        }),
    }
}

fn validate_metadata(metadata: &HashMap<String, String>, owner: &str) -> Result<(), SchemaError> {
    if let Some(key) = metadata
        .keys()
        .filter(|key| key.starts_with(RESERVED_METADATA_PREFIX))
        .min()
    {
        Err(SchemaError::ReservedMetadataKey {
            owner: owner.to_owned(),
            key: key.clone(),
        })
    } else {
        Ok(())
    }
}

fn enter_container(depth: usize) -> Result<usize, SchemaError> {
    let nested = depth.checked_add(1).ok_or(SchemaError::NestingTooDeep {
        max_depth: MAX_NESTING_DEPTH,
    })?;
    if nested > MAX_NESTING_DEPTH {
        Err(SchemaError::NestingTooDeep {
            max_depth: MAX_NESTING_DEPTH,
        })
    } else {
        Ok(nested)
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}.{name}")
    }
}
