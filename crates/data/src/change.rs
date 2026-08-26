use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use thiserror::Error;

use crate::{SchemaError, validate_schema};

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
    /// a diff is null or zero, or the record schema is unsupported.
    pub fn try_new(records: RecordBatch, diffs: Int64Array) -> Result<Self, ChangeError> {
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
        validate_schema(records.schema_ref())?;
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
