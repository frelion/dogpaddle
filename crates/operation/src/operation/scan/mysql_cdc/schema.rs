use std::{collections::HashSet, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use super::MySqlCdcScanError;

/// One supported, lossless `MySQL`-to-Arrow column mapping.
///
/// This initial fixed-schema Scan deliberately accepts a narrow type set.
/// Temporal, unsigned, JSON, enum, set, bit, generated, and invisible columns
/// are rejected during discovery instead of receiving an implicit conversion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlType {
    /// `tinyint` or signed `smallint`, emitted by Debezium as Connect `int16`.
    Int16,
    /// Signed `mediumint`, `int`, or `integer`.
    Int32,
    /// Signed `bigint`.
    Int64,
    /// `double`.
    Float64,
    /// Character and text types, represented as Arrow UTF-8.
    Text,
    /// Binary and blob types, represented as Arrow Binary.
    Binary,
    /// Fixed-precision `decimal`, represented as Arrow `Decimal128`.
    Decimal {
        /// Total number of decimal digits, from 1 through 38.
        precision: u8,
        /// Number of fractional decimal digits, from 0 through `precision`.
        scale: i8,
    },
}

/// A column in the captured table's fixed, ordered logical schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlColumn {
    name: String,
    data_type: MySqlType,
    nullable: bool,
}

impl MySqlColumn {
    /// Describes a column; Scan binding validates the complete schema.
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: MySqlType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    /// Returns the exact `MySQL` column name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the supported column type.
    #[must_use]
    pub const fn data_type(&self) -> MySqlType {
        self.data_type
    }

    /// Returns whether `MySQL` permits null values in this column.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }
}

impl MySqlType {
    pub(super) fn arrow_type(self) -> DataType {
        match self {
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Text => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
        }
    }

    pub(super) const fn connect_type(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::Int16 => ("int16", None),
            Self::Int32 => ("int32", None),
            Self::Int64 => ("int64", None),
            Self::Float64 => ("double", None),
            Self::Text => ("string", None),
            Self::Binary => ("bytes", None),
            Self::Decimal { .. } => ("bytes", Some("org.apache.kafka.connect.data.Decimal")),
        }
    }
}

pub(super) fn compile(columns: &[MySqlColumn]) -> Result<SchemaRef, MySqlCdcScanError> {
    if columns.is_empty() || columns.len() > 1_600 {
        return Err(MySqlCdcScanError::InvalidDefinition(
            "table must have between 1 and 1600 columns".into(),
        ));
    }
    let mut names = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.name.is_empty()
            || column.name.contains('\0')
            || !names.insert(column.name.as_str())
        {
            return Err(MySqlCdcScanError::InvalidDefinition(
                "column names must be nonempty, NUL-free, and unique".into(),
            ));
        }
        if let MySqlType::Decimal { precision, scale } = column.data_type
            && (!(1..=38).contains(&precision)
                || scale < 0
                || !u8::try_from(scale).is_ok_and(|scale| scale <= precision))
        {
            return Err(MySqlCdcScanError::InvalidDefinition(
                "decimal requires 1 <= precision <= 38 and 0 <= scale <= precision".into(),
            ));
        }
    }
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.arrow_type(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    dogpaddle_change::validate_schema(&schema)
        .map_err(|error| MySqlCdcScanError::InvalidDefinition(error.to_string()))?;
    Ok(schema)
}
