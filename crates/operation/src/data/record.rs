use thiserror::Error;

use super::Value;

/// Maximum number of nested array or object boundaries below a root record.
///
/// A root [`Record`] has depth zero. Entering either [`Value::Array`] or
/// [`Value::Object`] increases the depth by one, so 64 nested containers are
/// accepted and 65 are rejected.
pub const MAX_NESTING_DEPTH: usize = 64;

/// A canonical immutable mapping from field names to values.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Record {
    fields: Box<[(String, Value)]>,
}

impl Record {
    /// Constructs a canonical record.
    ///
    /// Fields are sorted by their UTF-8 names. Empty names are allowed, but
    /// duplicate names and values beyond [`MAX_NESTING_DEPTH`] are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::DuplicateField`] when a name occurs more than
    /// once, or [`RecordError::NestingTooDeep`] when a value exceeds the
    /// nesting limit.
    pub fn try_new<I, K>(fields: I) -> Result<Self, RecordError>
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut fields: Vec<_> = fields
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect();
        fields.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

        for adjacent in fields.windows(2) {
            if adjacent[0].0 == adjacent[1].0 {
                return Err(RecordError::DuplicateField {
                    name: adjacent[0].0.clone(),
                });
            }
        }
        validate_fields(&fields, 0)?;
        Ok(Self {
            fields: fields.into_boxed_slice(),
        })
    }

    /// Returns the number of fields.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns whether this record has no fields.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns a field by its exact UTF-8 name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .binary_search_by(|(candidate, _)| candidate.as_bytes().cmp(name.as_bytes()))
            .ok()
            .map(|index| &self.fields[index].1)
    }

    /// Returns whether this record contains `name`.
    #[must_use]
    pub fn contains_field(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Iterates over fields in canonical name order.
    #[must_use]
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (&str, &Value)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Consumes this record and returns its fields in canonical name order.
    #[must_use]
    pub fn into_fields(self) -> Box<[(String, Value)]> {
        self.fields
    }

    pub(crate) fn from_canonical_fields(fields: Vec<(String, Value)>) -> Self {
        Self {
            fields: fields.into_boxed_slice(),
        }
    }

    pub(crate) fn as_fields(&self) -> &[(String, Value)] {
        &self.fields
    }
}

/// A record construction failure.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RecordError {
    /// A field name occurred more than once.
    #[error("duplicate record field {name:?}")]
    DuplicateField {
        /// The duplicated field name.
        name: String,
    },
    /// A value exceeded the stable container nesting limit.
    #[error("record nesting exceeds the maximum depth of {max_depth}")]
    NestingTooDeep {
        /// The supported maximum depth.
        max_depth: usize,
    },
}

fn validate_fields(fields: &[(String, Value)], depth: usize) -> Result<(), RecordError> {
    for (_, value) in fields {
        validate_value(value, depth)?;
    }
    Ok(())
}

fn validate_value(value: &Value, depth: usize) -> Result<(), RecordError> {
    match value {
        Value::Array(values) => {
            let nested = enter_container(depth)?;
            for value in values {
                validate_value(value, nested)?;
            }
        }
        Value::Object(record) => {
            let nested = enter_container(depth)?;
            validate_fields(record.as_fields(), nested)?;
        }
        Value::Null
        | Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::String(_)
        | Value::Bytes(_) => {}
    }
    Ok(())
}

fn enter_container(depth: usize) -> Result<usize, RecordError> {
    let nested = depth + 1;
    if nested > MAX_NESTING_DEPTH {
        return Err(RecordError::NestingTooDeep {
            max_depth: MAX_NESTING_DEPTH,
        });
    }
    Ok(nested)
}
