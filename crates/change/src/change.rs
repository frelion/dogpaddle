use arrow_array::{
    Array, Decimal128Array, Int64Array, ListArray, RecordBatch, StructArray, make_array,
};
use arrow_schema::{DataType, Field, SchemaRef};
use thiserror::Error;

use crate::{
    projection::{ChangeProjection, ProjectionError},
    schema::{SchemaError, validate_schema},
};

/// A non-empty contiguous columnar segment of an ordered change sequence.
///
/// Row position is semantic: record row `i` and diff `i` form the `i`th event,
/// and consumers must observe events from row zero onward without reordering.
/// `Change` does not sort or consolidate equal records. It also carries no
/// previously materialized relation state, so whether a negative diff can be
/// applied is outside this type and belongs to the relation's validation
/// boundary.
#[derive(Clone, Debug)]
pub struct Change {
    records: RecordBatch,
    diffs: Int64Array,
}

impl Change {
    /// Constructs a validated change while preserving the supplied row order.
    ///
    /// This method does not sort, consolidate, or otherwise normalize events.
    ///
    /// # Errors
    ///
    /// Returns `ChangeError` when the change is empty, row counts differ,
    /// a diff is null or zero, the record schema is unsupported, or a non-null
    /// Decimal128 physical slot exceeds its field's declared precision.
    pub fn try_new(records: RecordBatch, diffs: Int64Array) -> Result<Self, ChangeError> {
        let change = Self::try_new_shape(records, diffs)?;
        validate_schema(change.records.schema_ref())?;
        validate_decimal128_values(&change.records)?;
        Ok(change)
    }

    pub(crate) fn try_new_with_validated_schema(
        records: RecordBatch,
        diffs: Int64Array,
    ) -> Result<Self, ChangeError> {
        let change = Self::try_new_shape(records, diffs)?;
        validate_decimal128_values(&change.records)?;
        Ok(change)
    }

    fn try_new_shape(records: RecordBatch, diffs: Int64Array) -> Result<Self, ChangeError> {
        if records.num_rows() == 0 {
            return Err(ChangeError::Empty);
        }
        if records.num_rows() != diffs.len() {
            return Err(ChangeError::LengthMismatch {
                records: records.num_rows(),
                diffs: diffs.len(),
            });
        }
        for (index, diff) in diffs.iter().enumerate() {
            match diff {
                None => return Err(ChangeError::NullDiff { index }),
                Some(0) => return Err(ChangeError::ZeroDiff { index }),
                Some(_) => {}
            }
        }
        Ok(Self { records, diffs })
    }

    /// Returns the ordered Arrow record columns.
    #[must_use]
    pub const fn records(&self) -> &RecordBatch {
        &self.records
    }

    /// Returns the ordered non-null, non-zero signed differences.
    #[must_use]
    pub const fn diffs(&self) -> &Int64Array {
        &self.diffs
    }

    /// Returns the number of change rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.records.num_rows()
    }

    /// Returns the logical record schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.records.schema()
    }

    /// Returns an order-preserving, zero-copy slice sharing the Arrow buffers.
    ///
    /// # Errors
    ///
    /// Returns `Empty` for a zero-length slice, or `SliceOutOfBounds` when the
    /// requested range is invalid.
    pub fn try_slice(&self, offset: usize, length: usize) -> Result<Self, ChangeError> {
        if length == 0 {
            return Err(ChangeError::Empty);
        }
        offset
            .checked_add(length)
            .filter(|end| *end <= self.num_rows())
            .ok_or(ChangeError::SliceOutOfBounds {
                offset,
                length,
                rows: self.num_rows(),
            })?;
        Ok(Self {
            records: self.records.slice(offset, length),
            diffs: self.diffs.slice(offset, length),
        })
    }

    /// Returns an order-preserving, zero-copy logical-field projection.
    ///
    /// Selected Arrow arrays and the difference array share their existing
    /// buffers with this Change. The result is an ordinary owned `Change` and
    /// can outlive the source value. Projection never deletes or reorders rows
    /// and never changes differences.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::SchemaMismatch`] when `projection` was
    /// created for a different logical Schema. Arrow projection failures are
    /// also returned rather than panicking.
    pub fn try_project(&self, projection: &ChangeProjection) -> Result<Self, ProjectionError> {
        projection.require_schema(self.records.schema_ref())?;
        if projection.is_identity() {
            return Ok(self.clone());
        }
        let records = self.records.project(projection.field_indices())?;
        debug_assert_eq!(records.schema_ref(), projection.output_schema_ref());
        Ok(Self {
            records,
            diffs: self.diffs.clone(),
        })
    }

    /// Consumes the change into its record columns and differences.
    #[must_use]
    pub fn into_parts(self) -> (RecordBatch, Int64Array) {
        (self.records, self.diffs)
    }
}

/// A change construction failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChangeError {
    /// An empty columnar collection carries no changes and is not representable.
    #[error("a change cannot be empty")]
    Empty,
    /// Record and diff row counts differ.
    #[error("record row count {records} differs from diff count {diffs}")]
    LengthMismatch {
        /// Number of record rows.
        records: usize,
        /// Number of differences.
        diffs: usize,
    },
    /// A difference is null.
    #[error("change difference at row {index} is null")]
    NullDiff {
        /// Zero-based row index.
        index: usize,
    },
    /// A difference is zero.
    #[error("change difference at row {index} is zero")]
    ZeroDiff {
        /// Zero-based row index.
        index: usize,
    },
    /// A non-null Decimal128 physical slot exceeds its field's precision.
    #[error(
        "Decimal128 value {value} at field {field:?} physical slot {index} does not fit precision {precision} and scale {scale}"
    )]
    InvalidDecimal128Value {
        /// Dot-separated diagnostic path to the Decimal128 field.
        field: String,
        /// Zero-based slot in the local physical Decimal128 child array.
        index: usize,
        /// Rejected unscaled signed integer value.
        value: i128,
        /// Declared Decimal128 precision.
        precision: u8,
        /// Declared Decimal128 scale.
        scale: i8,
    },
    /// A requested slice is outside the change.
    #[error("slice starting at {offset} with length {length} is outside a {rows}-row change")]
    SliceOutOfBounds {
        /// Requested starting row.
        offset: usize,
        /// Requested row count.
        length: usize,
        /// Available row count.
        rows: usize,
    },
    /// The logical record schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

fn validate_decimal128_values(records: &RecordBatch) -> Result<(), ChangeError> {
    for (field, array) in records.schema_ref().fields().iter().zip(records.columns()) {
        validate_decimal128_array(field, array.as_ref(), field.name())?;
    }
    Ok(())
}

fn validate_decimal128_array(
    field: &Field,
    array: &dyn Array,
    path: &str,
) -> Result<(), ChangeError> {
    match field.data_type() {
        DataType::Decimal128(precision, scale) => {
            let canonical;
            let decimal = if let Some(decimal) = array.as_any().downcast_ref::<Decimal128Array>() {
                decimal
            } else {
                canonical = make_array(array.to_data());
                canonical
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("make_array canonicalizes an Arrow Decimal128 array")
            };
            let limit = 10_i128.pow(u32::from(*precision));
            for (index, value) in decimal.iter().enumerate() {
                if let Some(value) = value
                    && !(-limit < value && value < limit)
                {
                    return Err(ChangeError::InvalidDecimal128Value {
                        field: path.to_owned(),
                        index,
                        value,
                        precision: *precision,
                        scale: *scale,
                    });
                }
            }
        }
        DataType::List(child) => {
            let canonical;
            let list = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
                list
            } else {
                canonical = make_array(array.to_data());
                canonical
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("make_array canonicalizes an Arrow List array")
            };
            let offsets = list.value_offsets();
            let start = usize::try_from(offsets[0])
                .expect("an Arrow ListArray has non-negative validated offsets");
            let end = usize::try_from(offsets[offsets.len() - 1])
                .expect("an Arrow ListArray has non-negative validated offsets");
            let values = list.values().slice(start, end - start);
            validate_decimal128_array(child, values.as_ref(), &join_path(path, child.name()))?;
        }
        DataType::Struct(fields) => {
            let canonical;
            let structure = if let Some(structure) = array.as_any().downcast_ref::<StructArray>() {
                structure
            } else {
                canonical = make_array(array.to_data());
                canonical
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("make_array canonicalizes an Arrow Struct array")
            };
            for (child, array) in fields.iter().zip(structure.columns()) {
                validate_decimal128_array(child, array.as_ref(), &join_path(path, child.name()))?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn join_path(parent: &str, child: &str) -> String {
    format!("{parent}.{child}")
}
