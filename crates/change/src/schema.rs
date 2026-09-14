use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field, Fields, Schema};
use thiserror::Error;

/// Maximum number of nested Arrow List or Struct boundaries.
pub const MAX_NESTING_DEPTH: usize = 60;
/// Maximum total number of logical fields, including nested List and Struct fields.
pub const MAX_SCHEMA_FIELDS: usize = 16_384;
/// Maximum total number of Schema and Field metadata entries.
pub const MAX_SCHEMA_METADATA_ENTRIES: usize = 49_152;
/// Maximum total UTF-8 bytes in field names, Timestamp timezones, and metadata.
pub const MAX_SCHEMA_TEXT_BYTES: usize = 8 * 1024 * 1024;

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
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => {
                Some(Self::FixedWidth(4))
            }
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => {
                Some(Self::FixedWidth(8))
            }
            DataType::Decimal128(_, _) => Some(Self::FixedWidth(16)),
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
/// Returns `SchemaError` for a Schema outside the bounded field, metadata, or
/// identity-text representation; duplicate or reserved names; unsupported
/// Arrow types; or nesting deeper than [`MAX_NESTING_DEPTH`].
pub fn validate_schema(schema: &Schema) -> Result<(), SchemaError> {
    let mut budget = SchemaBudget::default();
    let mut path = Vec::new();
    validate_metadata(schema.metadata(), &path, &mut budget)?;
    validate_fields(schema.fields(), &mut path, 0, &mut budget)
}

#[derive(Default)]
struct SchemaBudget {
    fields: usize,
    metadata_entries: usize,
    text_bytes: usize,
}

/// A logical record schema validation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SchemaError {
    /// A Schema contains more fields than the bounded v1 representation accepts.
    #[error("schema contains more than {max_fields} total fields")]
    TooManyFields {
        /// Maximum number of top-level plus nested fields.
        max_fields: usize,
    },
    /// Aggregate Schema and Field metadata exceeds the bounded v1 representation.
    #[error("schema metadata exceeds {max_entries} total entries while reading {owner:?}")]
    TooManyMetadataEntries {
        /// Schema or dot-separated field path owning the metadata.
        owner: String,
        /// Maximum aggregate entries accepted across the Schema.
        max_entries: usize,
    },
    /// Schema identity text exceeds the bounded v1 representation.
    #[error("schema identity text exceeds {max_bytes} total UTF-8 bytes while reading {owner:?}")]
    TooManyTextBytes {
        /// Schema or dot-separated field path whose text crossed the limit.
        owner: String,
        /// Maximum aggregate UTF-8 byte count.
        max_bytes: usize,
    },
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
        /// Bounded name of the unsupported top-level Arrow type.
        data_type: &'static str,
    },
    /// A Decimal128 field has precision or scale outside the stable v1 range.
    #[error(
        "invalid Decimal128 precision {precision} and scale {scale} at field {field:?}; precision must be 1..=38 and a positive scale cannot exceed precision"
    )]
    InvalidDecimal128 {
        /// Dot-separated diagnostic path to the field.
        field: String,
        /// Declared decimal precision.
        precision: u8,
        /// Declared decimal scale.
        scale: i8,
    },
    /// A Timestamp uses an empty timezone string, which Arrow IPC cannot
    /// distinguish from an absent timezone.
    #[error("empty Timestamp timezone at field {field:?}; use no timezone for a naive timestamp")]
    EmptyTimestampTimezone {
        /// Dot-separated diagnostic path to the field.
        field: String,
    },
    /// Nested Lists or Structs exceed the stable depth limit.
    #[error("schema nesting exceeds the maximum depth of {max_depth}")]
    NestingTooDeep {
        /// The supported maximum depth.
        max_depth: usize,
    },
}

fn validate_fields<'a>(
    fields: &'a Fields,
    path: &mut Vec<&'a str>,
    depth: usize,
    budget: &mut SchemaBudget,
) -> Result<(), SchemaError> {
    if budget
        .fields
        .checked_add(fields.len())
        .is_none_or(|count| count > MAX_SCHEMA_FIELDS)
    {
        return Err(SchemaError::TooManyFields {
            max_fields: MAX_SCHEMA_FIELDS,
        });
    }
    let mut names = HashSet::with_capacity(fields.len());
    for field in fields {
        if !names.insert(field.name().as_str()) {
            return Err(SchemaError::DuplicateField {
                scope: path.join("."),
                name: field.name().clone(),
            });
        }
        path.push(field.name());
        let result = validate_field(field, path, depth, budget);
        path.pop();
        result?;
    }
    Ok(())
}

fn validate_field<'a>(
    field: &'a Field,
    path: &mut Vec<&'a str>,
    depth: usize,
    budget: &mut SchemaBudget,
) -> Result<(), SchemaError> {
    budget.fields = budget
        .fields
        .checked_add(1)
        .filter(|count| *count <= MAX_SCHEMA_FIELDS)
        .ok_or(SchemaError::TooManyFields {
            max_fields: MAX_SCHEMA_FIELDS,
        })?;
    charge_text(budget, field.name().len(), path)?;
    if field.name().starts_with(RESERVED_FIELD_PREFIX) {
        return Err(SchemaError::ReservedFieldName {
            field: path.join("."),
            name: field.name().clone(),
        });
    }
    validate_metadata(field.metadata(), path, budget)?;
    if let DataType::Decimal128(precision, scale) = field.data_type()
        && !valid_decimal128_parameters(*precision, *scale)
    {
        return Err(SchemaError::InvalidDecimal128 {
            field: path.join("."),
            precision: *precision,
            scale: *scale,
        });
    }
    if let DataType::Timestamp(_, Some(timezone)) = field.data_type() {
        charge_text(budget, timezone.len(), path)?;
    }
    if let DataType::Timestamp(_, Some(timezone)) = field.data_type()
        && timezone.is_empty()
    {
        return Err(SchemaError::EmptyTimestampTimezone {
            field: path.join("."),
        });
    }
    match DataTypeLayout::classify(field.data_type()) {
        Some(DataTypeLayout::List(child)) => {
            let nested = enter_container(depth)?;
            path.push(child.name());
            let result = validate_field(child, path, nested, budget);
            path.pop();
            result
        }
        Some(DataTypeLayout::Struct(fields)) => {
            let nested = enter_container(depth)?;
            validate_fields(fields, path, nested, budget)
        }
        Some(_) => Ok(()),
        None => Err(SchemaError::UnsupportedType {
            field: path.join("."),
            data_type: unsupported_type_name(field.data_type()),
        }),
    }
}

fn unsupported_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Float16 => "Float16",
        DataType::Date64 => "Date64",
        DataType::Time32(_) => "Time32",
        DataType::Time64(_) => "Time64",
        DataType::Duration(_) => "Duration",
        DataType::Interval(_) => "Interval",
        DataType::FixedSizeBinary(_) => "FixedSizeBinary",
        DataType::LargeBinary => "LargeBinary",
        DataType::BinaryView => "BinaryView",
        DataType::LargeUtf8 => "LargeUtf8",
        DataType::Utf8View => "Utf8View",
        DataType::ListView(_) => "ListView",
        DataType::FixedSizeList(_, _) => "FixedSizeList",
        DataType::LargeList(_) => "LargeList",
        DataType::LargeListView(_) => "LargeListView",
        DataType::Union(_, _) => "Union",
        DataType::Dictionary(_, _) => "Dictionary",
        DataType::Decimal32(_, _) => "Decimal32",
        DataType::Decimal64(_, _) => "Decimal64",
        DataType::Decimal256(_, _) => "Decimal256",
        DataType::Map(_, _) => "Map",
        DataType::RunEndEncoded(_, _) => "RunEndEncoded",
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Binary
        | DataType::Utf8
        | DataType::List(_)
        | DataType::Struct(_)
        | DataType::Decimal128(_, _) => {
            unreachable!("supported Arrow types have a DataTypeLayout")
        }
    }
}

pub(crate) const fn valid_decimal128_parameters(precision: u8, scale: i8) -> bool {
    precision > 0 && precision <= 38 && (scale <= 0 || scale <= precision.cast_signed())
}

fn validate_metadata(
    metadata: &HashMap<String, String>,
    path: &[&str],
    budget: &mut SchemaBudget,
) -> Result<(), SchemaError> {
    budget.metadata_entries = budget
        .metadata_entries
        .checked_add(metadata.len())
        .filter(|entries| *entries <= MAX_SCHEMA_METADATA_ENTRIES)
        .ok_or_else(|| SchemaError::TooManyMetadataEntries {
            owner: owner(path),
            max_entries: MAX_SCHEMA_METADATA_ENTRIES,
        })?;
    for (key, value) in metadata {
        charge_text(budget, key.len(), path)?;
        charge_text(budget, value.len(), path)?;
    }
    if let Some(key) = metadata
        .keys()
        .filter(|key| key.starts_with(RESERVED_METADATA_PREFIX))
        .min()
    {
        Err(SchemaError::ReservedMetadataKey {
            owner: owner(path),
            key: key.clone(),
        })
    } else {
        Ok(())
    }
}

fn charge_text(budget: &mut SchemaBudget, bytes: usize, path: &[&str]) -> Result<(), SchemaError> {
    budget.text_bytes = budget
        .text_bytes
        .checked_add(bytes)
        .filter(|total| *total <= MAX_SCHEMA_TEXT_BYTES)
        .ok_or_else(|| SchemaError::TooManyTextBytes {
            owner: owner(path),
            max_bytes: MAX_SCHEMA_TEXT_BYTES,
        })?;
    Ok(())
}

fn owner(path: &[&str]) -> String {
    if path.is_empty() {
        "schema".to_owned()
    } else {
        path.join(".")
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
