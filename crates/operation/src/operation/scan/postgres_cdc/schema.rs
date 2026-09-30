use std::{collections::HashSet, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use super::PostgresCdcScanError;
use crate::operation::scan::cdc_convert::{WireColumn, WireKind};

/// One supported, lossless `PostgreSQL`-to-Arrow column mapping.
///
/// Arrays, domains, unconstrained numeric, and online type changes are not
/// supported. Numeric requires `1 <= precision <= 38` and `0 <= scale <= precision`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresType {
    /// `PostgreSQL` `boolean`.
    Boolean,
    /// `PostgreSQL` `smallint`.
    Int16,
    /// `PostgreSQL` `integer`.
    Int32,
    /// `PostgreSQL` `bigint`.
    Int64,
    /// `PostgreSQL` `real`.
    Float32,
    /// `PostgreSQL` `double precision`.
    Float64,
    /// `PostgreSQL` `text` or `varchar`, represented as Arrow UTF-8.
    Text,
    /// `PostgreSQL` `bytea`.
    Bytea,
    /// Finite `PostgreSQL` `date` values that fit Arrow epoch days.
    Date,
    /// Finite `PostgreSQL` `timestamp`, represented as naive epoch microseconds.
    Timestamp,
    /// Finite RFC 3339 `PostgreSQL` `timestamptz`, normalized to UTC microseconds.
    /// Extended-year and infinity values are rejected rather than truncated.
    TimestampTz,
    /// Fixed-precision `PostgreSQL` `numeric`, represented as Arrow `Decimal128`.
    Numeric {
        /// Total number of decimal digits, from 1 through 38.
        precision: u8,
        /// Number of fractional decimal digits, from 0 through `precision`.
        scale: i8,
    },
}

/// A column in the captured table's fixed, ordered logical schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresColumn {
    name: String,
    data_type: PostgresType,
    nullable: bool,
}

impl PostgresColumn {
    /// Describes a column; Scan binding validates the complete schema.
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: PostgresType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    /// Returns the exact `PostgreSQL` column name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the supported column type.
    #[must_use]
    pub const fn data_type(&self) -> PostgresType {
        self.data_type
    }

    /// Returns whether `PostgreSQL` permits null values in this column.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }
}

impl PostgresType {
    pub(super) fn arrow_type(self) -> DataType {
        self.wire_kind().arrow_type()
    }

    #[cfg(test)]
    pub(super) const fn connect_type(self) -> (&'static str, Option<&'static str>) {
        self.wire_kind().connect_type()
    }

    const fn wire_kind(self) -> WireKind {
        match self {
            Self::Boolean => WireKind::Boolean,
            Self::Int16 => WireKind::Int16,
            Self::Int32 => WireKind::Int32,
            Self::Int64 => WireKind::Int64,
            Self::Float32 => WireKind::Float32,
            Self::Float64 => WireKind::Float64,
            Self::Text => WireKind::Text,
            Self::Bytea => WireKind::Binary,
            Self::Date => WireKind::Date,
            Self::Timestamp => WireKind::Timestamp,
            Self::TimestampTz => WireKind::TimestampTz,
            Self::Numeric { precision, scale } => WireKind::Decimal { precision, scale },
        }
    }
}

impl WireColumn for PostgresColumn {
    type Error = PostgresCdcScanError;

    const INCOMPLETE_IMAGE: &'static str =
        "missing complete row image; the captured table requires REPLICA IDENTITY FULL";
    const DECIMAL_LABEL: &'static str = "numeric";

    fn name(&self) -> &str {
        &self.name
    }

    fn nullable(&self) -> bool {
        self.nullable
    }

    fn wire_kind(&self) -> WireKind {
        self.data_type.wire_kind()
    }

    fn invalid_record(message: String) -> Self::Error {
        PostgresCdcScanError::InvalidRecord(message)
    }
}

pub(super) fn compile(columns: &[PostgresColumn]) -> Result<SchemaRef, PostgresCdcScanError> {
    if columns.is_empty() || columns.len() > 1_600 {
        return Err(PostgresCdcScanError::InvalidDefinition(
            "table must have between 1 and 1600 columns".into(),
        ));
    }
    let mut names = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.name.is_empty()
            || column.name.contains('\0')
            || !names.insert(column.name.as_str())
        {
            return Err(PostgresCdcScanError::InvalidDefinition(
                "column names must be nonempty, NUL-free, and unique".into(),
            ));
        }
        if let PostgresType::Numeric { precision, scale } = column.data_type
            && (!(1..=38).contains(&precision)
                || scale < 0
                || !u8::try_from(scale).is_ok_and(|scale| scale <= precision))
        {
            return Err(PostgresCdcScanError::InvalidDefinition(
                "numeric requires 1 <= precision <= 38 and 0 <= scale <= precision".into(),
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
        .map_err(|error| PostgresCdcScanError::InvalidDefinition(error.to_string()))?;
    Ok(schema)
}
