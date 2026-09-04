use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::jvm::JvmHost;
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

/// A running, single-threaded Debezium connector handle.
///
/// Every operation needs exclusive access. A live [`Delivery`] borrows that
/// access, which makes polling, stopping, or acknowledging through another
/// connector impossible at compile time.
pub struct Connector {
    host: Arc<JvmHost>,
    handle: Option<i64>,
    engine_name: Box<str>,
    class_name: Box<str>,
    max_delivery_bytes: usize,
    poisoned: bool,
}

impl Connector {
    pub(crate) fn new(
        host: Arc<JvmHost>,
        handle: i64,
        engine_name: Box<str>,
        class_name: Box<str>,
        max_delivery_bytes: usize,
    ) -> Self {
        Self {
            host,
            handle: Some(handle),
            engine_name,
            class_name,
            max_delivery_bytes,
            poisoned: false,
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
    pub fn poll(&mut self, timeout: Duration) -> Result<Option<Delivery<'_>>, Error> {
        self.ensure_usable()?;
        let handle = self.handle.ok_or_else(|| {
            Error::new(ErrorKind::ConnectorFailed, "connector has already stopped")
        })?;
        let polled = self.host.poll(handle, timeout, self.max_delivery_bytes);
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
        Ok(Some(Delivery {
            connector: self,
            token: decoded.token,
            checkpoint: decoded.checkpoint,
            records: decoded.records,
        }))
    }

    /// Stops this connector within `timeout` and releases its Java handle.
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
        let Some(handle) = self.handle else {
            return Ok(());
        };
        self.host.stop(handle, timeout)?;
        self.host.dispose(handle)?;
        self.handle = None;
        Ok(())
    }

    fn acknowledge(&mut self, token: i64) -> Result<(), Error> {
        self.ensure_usable()?;
        let handle = self.handle.ok_or_else(|| {
            Error::new(ErrorKind::ConnectorFailed, "connector has already stopped")
        })?;
        if let Err(error) = self.host.ack(handle, token, ACK_TIMEOUT) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    fn ensure_usable(&self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::ConnectorFailed,
                "connector is unusable after an uncertain ACK or bridge protocol failure; stop it and restart from the persisted checkpoint",
            ));
        }
        if self.handle.is_none() {
            return Err(Error::new(
                ErrorKind::ConnectorFailed,
                "connector has already stopped",
            ));
        }
        Ok(())
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        if let Some(handle) = self.handle {
            self.host.abandon(handle);
        }
    }
}

/// One unacknowledged batch and its pre-ACK recovery checkpoint.
///
/// The record and checkpoint data are fully owned by Rust. The lifetime only
/// reserves exclusive control of the connector until this delivery is `ACKed`
/// or dropped.
#[must_use = "dropping a delivery leaves it unacknowledged"]
pub struct Delivery<'connector> {
    connector: &'connector mut Connector,
    token: i64,
    checkpoint: Checkpoint,
    records: Box<[Record]>,
}

impl Delivery<'_> {
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

    /// Acknowledges this delivery after its records and checkpoint are durable.
    ///
    /// This consumes the delivery capability. If acknowledgement fails, the
    /// connector is poisoned because the exact outcome may be uncertain; stop
    /// it and restart from the checkpoint already made durable by the caller.
    /// Acknowledgement waits at most 30 seconds for the Engine handler and
    /// offset-store commit to settle.
    ///
    /// # Errors
    ///
    /// Returns an error when the Java handler cannot finish the exact
    /// outstanding batch or its actual offset state differs from the pre-ACK
    /// checkpoint.
    pub fn ack(self) -> Result<(), Error> {
        self.connector.acknowledge(self.token)
    }
}

impl fmt::Debug for Delivery<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("checkpoint", &self.checkpoint)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}
