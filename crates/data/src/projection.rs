use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use thiserror::Error;

use crate::{SchemaError, validate_schema};

/// A top-level logical-field projection bound to one exact input Schema.
///
/// A projection can only delete fields: its zero-based logical field indices
/// must be strictly increasing, so projected fields retain their original
/// relative order. The physical `$dogpaddle.diff` field is not addressable and
/// is always retained implicitly. An empty field list is valid and describes a
/// zero-column record batch with the original row count and differences.
///
/// Selecting a `List` or `Struct` field selects its complete nested subtree.
/// The input Schema binding prevents an index from silently naming a different
/// field after Schema drift.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeProjection {
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    field_indices: Box<[usize]>,
}

impl ChangeProjection {
    /// Creates a projection for one exact logical input Schema.
    ///
    /// Schema metadata and all metadata of selected fields are preserved in
    /// the output Schema.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError`] when the Schema is unsupported, an index is
    /// outside the Schema, or the indices contain a duplicate or reordering.
    pub fn try_new(
        input_schema: SchemaRef,
        field_indices: impl IntoIterator<Item = usize>,
    ) -> Result<Self, ProjectionError> {
        validate_schema(input_schema.as_ref())?;
        let field_indices = field_indices.into_iter().collect::<Box<[_]>>();
        let field_count = input_schema.fields().len();
        let mut previous = None;
        for &current in &field_indices {
            if current >= field_count {
                return Err(ProjectionError::FieldOutOfBounds {
                    index: current,
                    fields: field_count,
                });
            }
            if let Some(previous) = previous
                && current <= previous
            {
                return Err(ProjectionError::FieldsNotStrictlyIncreasing { previous, current });
            }
            previous = Some(current);
        }

        let fields = field_indices
            .iter()
            .map(|&index| Arc::clone(&input_schema.fields()[index]))
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        Ok(Self {
            input_schema,
            output_schema,
            field_indices,
        })
    }

    /// Returns the exact logical Schema this projection accepts.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }

    /// Returns the projected logical Schema.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Returns the selected top-level logical field indices.
    #[must_use]
    pub const fn field_indices(&self) -> &[usize] {
        &self.field_indices
    }

    pub(crate) fn require_schema(&self, actual: &Schema) -> Result<(), ProjectionError> {
        if self.input_schema.as_ref() == actual {
            Ok(())
        } else {
            Err(ProjectionError::SchemaMismatch)
        }
    }

    pub(crate) fn is_identity(&self) -> bool {
        self.field_indices.len() == self.input_schema.fields().len()
    }
}

/// A logical Change projection failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ProjectionError {
    /// The bound logical Schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// A requested logical field does not exist.
    #[error("logical field index {index} is outside a schema with {fields} fields")]
    FieldOutOfBounds {
        /// Requested zero-based logical field index.
        index: usize,
        /// Number of fields in the bound input Schema.
        fields: usize,
    },
    /// Field indices contain a duplicate or attempt to reorder fields.
    #[error("logical field indices must be strictly increasing; {current} follows {previous}")]
    FieldsNotStrictlyIncreasing {
        /// Previous logical field index.
        previous: usize,
        /// Duplicate or reordered logical field index.
        current: usize,
    },
    /// The projection was applied to a different logical Schema.
    #[error("the projection was created for a different logical schema")]
    SchemaMismatch,
    /// Arrow rejected an otherwise validated in-memory projection.
    #[error("Arrow rejected the logical projection: {message}")]
    Arrow {
        /// Arrow diagnostic message.
        message: String,
    },
}
