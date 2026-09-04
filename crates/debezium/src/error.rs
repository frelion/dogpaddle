use std::fmt;

/// A stable category for a Debezium runtime failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The self-contained runtime bundle is missing or malformed.
    InvalidBundle,
    /// A process JVM already exists with incompatible settings.
    JvmConfigurationConflict,
    /// Connector properties are incomplete or reserved by the runtime.
    InvalidConfiguration,
    /// Resume checkpoint bytes are malformed or belong to another connector.
    InvalidCheckpoint,
    /// The embedded JVM could not be initialized.
    JvmStartup,
    /// The connector could not reach its running state.
    ConnectorStartup,
    /// A running connector failed.
    ConnectorFailed,
    /// A delivery exceeded its configured bound.
    DeliveryTooLarge,
    /// The private Rust/Java protocol was violated.
    Protocol,
    /// A bounded lifecycle operation reached its deadline.
    Timeout,
}

/// An error returned by the embedded Debezium runtime.
///
/// Error text is deliberately limited to runtime-controlled context. Connector
/// property values are never included because they commonly contain secrets.
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the stable category of this error.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}
