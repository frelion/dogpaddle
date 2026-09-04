use std::collections::BTreeMap;
use std::fmt;

use crate::checkpoint::MAX_BINDING_BYTES;
use crate::{Checkpoint, Error, ErrorKind};

const DEFAULT_MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;
const DELIVERY_BYTES_OUTSIDE_CHECKPOINT: usize = 48;
const EMPTY_CHECKPOINT_FIXED_BYTES: usize = 26;

/// Connector-neutral properties for one embedded Debezium Engine.
///
/// This is intentionally a thin, secret-safe wrapper around Debezium
/// properties rather than a connector-specific configuration DSL.
pub struct ConnectorConfig {
    properties: BTreeMap<String, String>,
    max_delivery_bytes: usize,
}

impl ConnectorConfig {
    /// Creates a configuration with the required stable engine name and
    /// connector implementation class.
    ///
    /// # Errors
    ///
    /// Returns an error when either value is blank or exceeds the checkpoint
    /// binding limit.
    pub fn new(name: impl Into<String>, connector_class: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        let connector_class = connector_class.into();
        require_binding("name", &name)?;
        require_binding("connector.class", &connector_class)?;

        let mut properties = BTreeMap::new();
        properties.insert("connector.class".to_owned(), connector_class);
        properties.insert("name".to_owned(), name);
        Ok(Self {
            properties,
            max_delivery_bytes: DEFAULT_MAX_DELIVERY_BYTES,
        })
    }

    /// Adds one connector property.
    ///
    /// Runtime-owned offset, commit, task, and SMT settings cannot be set by
    /// callers because changing them would invalidate the checkpoint and ACK
    /// protocol.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank key or a runtime-reserved property.
    pub fn property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, Error> {
        let key = key.into();
        require_non_blank("property key", &key)?;
        if is_reserved(&key) {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                format!("connector property '{key}' is owned by dogpaddle-debezium"),
            ));
        }
        self.properties.insert(key, value.into());
        Ok(self)
    }

    /// Sets the maximum encoded size of one delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when `bytes` cannot hold the smallest delivery for
    /// this connector or cannot be represented by the Java bridge protocol.
    pub fn max_delivery_bytes(mut self, bytes: usize) -> Result<Self, Error> {
        let minimum = self.minimum_delivery_bytes(None)?;
        if bytes < minimum || i32::try_from(bytes).is_err() {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                format!("max delivery bytes must be between {minimum} and Java Integer.MAX_VALUE"),
            ));
        }
        self.max_delivery_bytes = bytes;
        Ok(self)
    }

    pub(crate) fn engine_name(&self) -> &str {
        self.properties
            .get("name")
            .expect("ConnectorConfig always contains name")
    }

    pub(crate) fn connector_class(&self) -> &str {
        self.properties
            .get("connector.class")
            .expect("ConnectorConfig always contains connector.class")
    }

    pub(crate) fn validate_delivery_bound(
        &self,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<(), Error> {
        let minimum = self.minimum_delivery_bytes(checkpoint)?;
        if self.max_delivery_bytes < minimum {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                format!("max delivery bytes must be at least {minimum} for the initial checkpoint"),
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(self) -> Result<(Vec<u8>, usize), Error> {
        let bytes = serde_json::to_vec(&self.properties).map_err(|_| {
            Error::new(
                ErrorKind::InvalidConfiguration,
                "connector properties cannot be encoded for the Java bridge",
            )
        })?;
        Ok((bytes, self.max_delivery_bytes))
    }

    fn minimum_delivery_bytes(&self, checkpoint: Option<&Checkpoint>) -> Result<usize, Error> {
        let checkpoint_bytes = if let Some(checkpoint) = checkpoint {
            checkpoint.as_bytes().len()
        } else {
            EMPTY_CHECKPOINT_FIXED_BYTES
                .checked_add(self.engine_name().len())
                .and_then(|bytes| bytes.checked_add(self.connector_class().len()))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidConfiguration,
                        "connector checkpoint binding length overflows",
                    )
                })?
        };
        checkpoint_bytes
            .checked_add(DELIVERY_BYTES_OUTSIDE_CHECKPOINT)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidConfiguration,
                    "minimum connector delivery length overflows",
                )
            })
    }
}

impl fmt::Debug for ConnectorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorConfig")
            .field("property_keys", &self.properties.keys().collect::<Vec<_>>())
            .field("max_delivery_bytes", &self.max_delivery_bytes)
            .finish()
    }
}

fn require_non_blank(label: &str, value: &str) -> Result<(), Error> {
    if value.trim().is_empty() {
        Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!("connector {label} must not be blank"),
        ))
    } else {
        Ok(())
    }
}

fn require_binding(label: &str, value: &str) -> Result<(), Error> {
    require_non_blank(label, value)?;
    if value.len() > MAX_BINDING_BYTES {
        return Err(Error::new(
            ErrorKind::InvalidConfiguration,
            format!(
                "connector {label} exceeds the {MAX_BINDING_BYTES}-byte checkpoint binding limit"
            ),
        ));
    }
    Ok(())
}

fn is_reserved(key: &str) -> bool {
    key == "name"
        || key == "connector.class"
        || key == "tasks.max"
        || is_namespace(key, "offset")
        || is_namespace(key, "record.processing")
        || is_namespace(key, "transforms")
        || is_namespace(key, "predicates")
        || is_namespace(key, "key.converter")
        || is_namespace(key, "value.converter")
        || is_namespace(key, "header.converter")
        || is_namespace(key, "dogpaddle")
}

fn is_namespace(key: &str, namespace: &str) -> bool {
    key == namespace
        || key
            .strip_prefix(namespace)
            .is_some_and(|suffix| suffix.starts_with('.'))
}
