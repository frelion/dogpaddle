use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::jvm::{JvmHost, RuntimeObject};
use crate::protocol::decode_delivery;
use crate::{Checkpoint, Error, ErrorKind};

const ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// One encoded Kafka Connect header.
pub struct Header {
    key: Box<str>,
    value: Option<Box<[u8]>>,
}

impl Header {
    pub(crate) const fn new(key: Box<str>, value: Option<Box<[u8]>>) -> Self {
        Self { key, value }
    }

    /// Returns the header name.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the schemas-enabled Kafka Connect JSON value, or `None` for a
    /// Java null.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Header")
            .field("key", &self.key)
            .field("value_bytes", &self.value.as_ref().map(|value| value.len()))
            .finish()
    }
}

/// An owned Kafka Connect `SourceRecord` representation.
///
/// Key, value, and header values use Kafka Connect's schemas-enabled JSON
/// encoding. They remain owned Rust bytes after the JNI call returns.
pub struct Record {
    topic: Option<Box<str>>,
    kafka_partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Box<[u8]>>,
    value: Option<Box<[u8]>>,
    headers: Box<[Header]>,
}

impl Record {
    pub(crate) const fn new(
        topic: Option<Box<str>>,
        kafka_partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<Box<[u8]>>,
        value: Option<Box<[u8]>>,
        headers: Box<[Header]>,
    ) -> Self {
        Self {
            topic,
            kafka_partition,
            timestamp,
            key,
            value,
            headers,
        }
    }

    /// Returns the Kafka topic attached to this source record, if any.
    #[must_use]
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    /// Returns the optional Kafka partition metadata.
    #[must_use]
    pub const fn kafka_partition(&self) -> Option<i32> {
        self.kafka_partition
    }

    /// Returns the optional source-record timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns the schemas-enabled Kafka Connect JSON key, or `None` for a
    /// Java null.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the schemas-enabled Kafka Connect JSON value, or `None` for a
    /// Java null.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns headers in their original order.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Record")
            .field("topic", &self.topic)
            .field("kafka_partition", &self.kafka_partition)
            .field("timestamp", &self.timestamp)
            .field("key_bytes", &self.key.as_ref().map(|key| key.len()))
            .field("value_bytes", &self.value.as_ref().map(|value| value.len()))
            .field("headers", &self.headers)
            .finish()
    }
}

/// A running, single-threaded Debezium connector.
///
/// Every connector operation needs exclusive access. An owned [`Delivery`] prevents
/// another poll until consumed or dropped. ACK validates the original capability;
/// stopping invalidates it.
pub struct Connector {
    host: Arc<JvmHost>,
    runtime: Option<RuntimeObject>,
    engine_name: Box<str>,
    class_name: Box<str>,
    max_delivery_bytes: usize,
    poisoned: bool,
    outstanding: Weak<()>,
}

impl Connector {
    pub(crate) fn new(
        host: Arc<JvmHost>,
        runtime: RuntimeObject,
        engine_name: Box<str>,
        class_name: Box<str>,
        max_delivery_bytes: usize,
    ) -> Self {
        Self {
            host,
            runtime: Some(runtime),
            engine_name,
            class_name,
            max_delivery_bytes,
            poisoned: false,
            outstanding: Weak::new(),
        }
    }

    /// Waits for one delivery up to `timeout`.
    ///
    /// `Ok(None)` means only that the timeout elapsed while the running
    /// connector had no delivery. Dropping a returned delivery does not ACK
    /// it; the next poll returns the same outstanding bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid duration, connector failure, malformed
    /// bridge response, or use after stop or an uncertain ACK.
    pub fn poll(&mut self, timeout: Duration) -> Result<Option<Delivery>, Error> {
        if self.outstanding.upgrade().is_some() {
            return Err(Error::new(
                ErrorKind::Protocol,
                "a delivery capability is still outstanding",
            ));
        }
        let runtime = self.usable_runtime()?;
        let polled = self.host.poll(runtime, timeout, self.max_delivery_bytes);
        let Some(bytes) = (match polled {
            Ok(bytes) => bytes,
            Err(error) => {
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectorFailed | ErrorKind::DeliveryTooLarge | ErrorKind::Protocol
                ) {
                    self.poisoned = true;
                }
                return Err(error);
            }
        }) else {
            return Ok(None);
        };
        let decoded = match decode_delivery(&bytes, self.max_delivery_bytes) {
            Ok(decoded) => decoded,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        if !decoded
            .checkpoint
            .matches(&self.engine_name, &self.class_name)
        {
            self.poisoned = true;
            return Err(Error::new(
                ErrorKind::Protocol,
                "delivery checkpoint belongs to a different connector",
            ));
        }
        let capability = Arc::new(());
        self.outstanding = Arc::downgrade(&capability);
        Ok(Some(Delivery {
            capability,
            checkpoint: decoded.checkpoint,
            records: decoded.records,
        }))
    }

    /// Stops this connector within `timeout` and releases its Java runtime.
    ///
    /// An outstanding delivery is aborted and is never acknowledged. If the
    /// deadline expires, the Java cleanup worker continues and this method can
    /// be called again.
    ///
    /// # Errors
    ///
    /// Returns an error when the duration is invalid, shutdown fails, or the
    /// deadline expires.
    pub fn stop(&mut self, timeout: Duration) -> Result<(), Error> {
        self.outstanding = Weak::new();
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(());
        };
        self.host.stop(runtime, timeout)?;
        self.host.dispose(runtime)?;
        self.runtime = None;
        self.outstanding = Weak::new();
        Ok(())
    }

    /// Consumes the original delivery after its complete data and checkpoint are durable.
    ///
    /// An owned delivery is valid only for this connector's current outstanding batch.
    /// ACK errors poison the connector; stop and restart from the durable checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign or invalidated capability, stopped connector,
    /// bridge failure, or uncertain acknowledgement.
    pub fn ack(&mut self, delivery: Delivery) -> Result<(), Error> {
        if !self
            .outstanding
            .ptr_eq(&Arc::downgrade(&delivery.capability))
        {
            return Err(Error::new(
                ErrorKind::Protocol,
                "delivery capability belongs to another connector or was invalidated",
            ));
        }
        let runtime = self.usable_runtime()?;
        if let Err(error) = self.host.ack(runtime, ACK_TIMEOUT) {
            self.poisoned = true;
            return Err(error);
        }
        self.outstanding = Weak::new();
        drop(delivery);
        Ok(())
    }

    fn usable_runtime(&self) -> Result<&RuntimeObject, Error> {
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::ConnectorFailed,
                "connector is unusable after an uncertain ACK or bridge protocol failure; stop it and restart from the persisted checkpoint",
            ));
        }
        self.runtime
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::ConnectorFailed, "connector has already stopped"))
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.as_ref() {
            self.host.abandon(runtime);
        }
    }
}

/// One owned, linear capability for an unacknowledged batch.
///
/// Dropping it leaves the batch unacknowledged and permits polling the same bytes again.
/// [`Connector::ack`] verifies ownership and consumes the original capability.
///
/// ```compile_fail
/// use dogpaddle_debezium::{Connector, Delivery};
/// fn duplicate(connector: &mut Connector, delivery: Delivery) {
///     connector.ack(delivery).unwrap();
///     connector.ack(delivery).unwrap();
/// }
/// ```
#[must_use = "dropping a delivery leaves it unacknowledged"]
pub struct Delivery {
    capability: Arc<()>,
    checkpoint: Checkpoint,
    records: Box<[Record]>,
}
impl Delivery {
    /// Returns the complete offset-store image that resumes after this batch.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns source records in Debezium's delivery order.
    #[must_use]
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("checkpoint", &self.checkpoint)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}
