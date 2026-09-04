use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use jni::objects::{JByteArray, Reference};
use jni::{InitArgsBuilder, JValue, JavaVM, jni_sig, jni_str};
use serde::Deserialize;

use crate::connector::Connector;
use crate::distribution::Distribution;
use crate::{Checkpoint, ConnectorConfig, Error, ErrorKind};

const START_TIMEOUT: Duration = Duration::from_secs(30);
const START_POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_STATUS_BYTES: usize = 64 * 1024;
const BRIDGE_PROTOCOL_VERSION: i32 = 1;

static JVM_HOST: OnceLock<Mutex<Option<Arc<JvmHost>>>> = OnceLock::new();

/// A cloneable reference to `DogPaddle`'s process-wide embedded JVM.
///
/// The first successful call to [`DebeziumRuntime::open`] fixes the JVM
/// distribution for the process. Opening the same canonical distribution
/// again reuses that JVM only when its validated contents are unchanged;
/// opening a different distribution fails explicitly.
#[derive(Clone)]
pub struct DebeziumRuntime {
    host: Arc<JvmHost>,
}

impl DebeziumRuntime {
    /// Opens a validated Debezium distribution and starts or reuses the
    /// process-wide JVM.
    ///
    /// `distribution` must contain `lib/dogpaddle-debezium-bridge.jar` and all
    /// pinned runtime dependency JARs. The host JVM is located by `jni-rs`,
    /// normally through `JAVA_HOME`.
    ///
    /// # Errors
    ///
    /// Returns an error when the distribution is invalid, the JVM cannot be
    /// started, or another distribution already initialized the process JVM.
    pub fn open(distribution: impl AsRef<Path>) -> Result<Self, Error> {
        let distribution = Distribution::open(distribution.as_ref())?;
        let slot = JVM_HOST.get_or_init(|| Mutex::new(None));
        let mut guard = slot.lock().map_err(|_| {
            Error::new(
                ErrorKind::JvmStartup,
                "process JVM initialization lock is poisoned",
            )
        })?;

        if let Some(host) = guard.as_ref() {
            if host.distribution_root != distribution.root()
                || host.distribution_fingerprint != *distribution.fingerprint()
            {
                return Err(Error::new(
                    ErrorKind::JvmConfigurationConflict,
                    format!(
                        "the process JVM is already bound to Debezium distribution {}",
                        host.distribution_root.display()
                    ),
                ));
            }
            return Ok(Self {
                host: Arc::clone(host),
            });
        }

        let host = Arc::new(JvmHost::launch(&distribution)?);
        *guard = Some(Arc::clone(&host));
        Ok(Self { host })
    }

    /// Starts one connector and waits until Debezium begins polling.
    ///
    /// `checkpoint`, when supplied, must have been produced for the exact same
    /// engine name and connector class. No external or Java offset file is
    /// consulted.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration/checkpoint pairing, bridge
    /// failure, connector startup failure, or startup timeout.
    pub fn start(
        &self,
        config: ConnectorConfig,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Connector, Error> {
        if checkpoint.is_some_and(|checkpoint| {
            !checkpoint.matches(config.engine_name(), config.connector_class())
        }) {
            return Err(Error::new(
                ErrorKind::InvalidCheckpoint,
                "checkpoint belongs to a different engine name or connector class",
            ));
        }
        config.validate_delivery_bound(checkpoint)?;

        let engine_name = Box::<str>::from(config.engine_name());
        let connector_class = Box::<str>::from(config.connector_class());
        let (configuration, delivery_bound) = config.encode()?;
        let handle = self.host.create(
            &configuration,
            checkpoint.map(Checkpoint::as_bytes),
            delivery_bound,
        )?;

        let result = self.host.start(handle).and_then(|()| {
            self.host
                .wait_until_running(handle, START_TIMEOUT)
                .map(|()| {
                    Connector::new(
                        Arc::clone(&self.host),
                        handle,
                        engine_name,
                        connector_class,
                        delivery_bound,
                    )
                })
        });
        if result.is_err() {
            self.host.abandon(handle);
        }
        result
    }
}

impl std::fmt::Debug for DebeziumRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DebeziumRuntime")
            .field("distribution", &self.host.distribution_root)
            .finish_non_exhaustive()
    }
}

pub(crate) struct JvmHost {
    vm: JavaVM,
    distribution_root: PathBuf,
    distribution_fingerprint: [u8; 32],
}

impl JvmHost {
    fn launch(distribution: &Distribution) -> Result<Self, Error> {
        let classpath_option = format!("-Djava.class.path={}", distribution.classpath());
        let arguments = InitArgsBuilder::new()
            .option(classpath_option)
            .option("-Dfile.encoding=UTF-8")
            .option("-Dorg.slf4j.simpleLogger.logFile=System.err")
            .option("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn")
            .build()
            .map_err(|_| {
                Error::new(
                    ErrorKind::JvmStartup,
                    "cannot construct embedded JVM options",
                )
            })?;
        let vm = JavaVM::new(arguments).map_err(|_| {
            Error::new(
                ErrorKind::JvmStartup,
                "cannot start embedded JVM; verify JAVA_HOME and the supported JDK",
            )
        })?;
        let host = Self {
            vm,
            distribution_root: distribution.root().to_path_buf(),
            distribution_fingerprint: *distribution.fingerprint(),
        };
        host.validate_bridge_protocol()?;
        Ok(host)
    }

    fn validate_bridge_protocol(&self) -> Result<(), Error> {
        let version = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<i32> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("protocolVersion"),
                    jni_sig!("()I"),
                    &[],
                )?;
                result.into_int()
            })
            .map_err(|_| {
                Error::new(
                    ErrorKind::JvmStartup,
                    "embedded JVM cannot load the pinned Debezium bridge; restart the process after fixing the distribution",
                )
            })?;
        if version != BRIDGE_PROTOCOL_VERSION {
            return Err(Error::new(
                ErrorKind::Protocol,
                "embedded Debezium bridge protocol is not supported; restart the process with the pinned distribution",
            ));
        }
        Ok(())
    }

    pub(crate) fn create(
        &self,
        configuration: &[u8],
        checkpoint: Option<&[u8]>,
        max_delivery_bytes: usize,
    ) -> Result<i64, Error> {
        let max_delivery_bytes = i32::try_from(max_delivery_bytes).map_err(|_| {
            Error::new(
                ErrorKind::InvalidConfiguration,
                "max delivery bytes exceed Java Integer.MAX_VALUE",
            )
        })?;
        self.vm
            .attach_current_thread(|environment| -> jni::errors::Result<i64> {
                let configuration = environment.byte_array_from_slice(configuration)?;
                let checkpoint: JByteArray<'_> = if let Some(bytes) = checkpoint {
                    environment.byte_array_from_slice(bytes)?
                } else {
                    JByteArray::null()
                };
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("create"),
                    jni_sig!("([B[BI)J"),
                    &[
                        JValue::Object(configuration.as_ref()),
                        JValue::Object(checkpoint.as_ref()),
                        JValue::Int(max_delivery_bytes),
                    ],
                )?;
                result.into_long()
            })
            .map_err(|_| bridge_error(ErrorKind::InvalidConfiguration, "create"))
    }

    pub(crate) fn start(&self, handle: i64) -> Result<(), Error> {
        self.vm
            .attach_current_thread(|environment| -> jni::errors::Result<()> {
                environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("start"),
                    jni_sig!("(J)V"),
                    &[JValue::Long(handle)],
                )?;
                Ok(())
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorStartup, "start"))
    }

    pub(crate) fn poll(
        &self,
        handle: i64,
        timeout: Duration,
        max_delivery_bytes: usize,
    ) -> Result<Option<Vec<u8>>, Error> {
        let timeout = duration_millis(timeout, "poll")?;
        self.vm
            .attach_current_thread(|environment| -> Result<Option<Vec<u8>>, BoundedCallError> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("poll"),
                    jni_sig!("(JJ)[B"),
                    &[JValue::Long(handle), JValue::Long(timeout)],
                )?;
                let object = result.into_object()?;
                if object.is_null() {
                    return Ok(None);
                }
                let bytes = JByteArray::cast_local(environment, object)?;
                if bytes.len(environment)? > max_delivery_bytes {
                    return Err(BoundedCallError::ResponseTooLarge);
                }
                Ok(Some(environment.convert_byte_array(&bytes)?))
            })
            .map_err(|error| match error {
                BoundedCallError::Jni => match self.status(handle) {
                    Ok(status) => bridge_error(
                        status.reported_error_kind(ErrorKind::ConnectorFailed),
                        "poll",
                    ),
                    Err(error) if error.kind() == ErrorKind::Protocol => error,
                    Err(_) => bridge_error(ErrorKind::ConnectorFailed, "poll"),
                },
                BoundedCallError::ResponseTooLarge => Error::new(
                    ErrorKind::DeliveryTooLarge,
                    "Java bridge returned a delivery larger than its configured bound",
                ),
            })
    }

    pub(crate) fn ack(&self, handle: i64, token: i64, timeout: Duration) -> Result<(), Error> {
        let timeout = duration_millis(timeout, "ack")?;
        let settled = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<bool> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("ack"),
                    jni_sig!("(JJJ)Z"),
                    &[
                        JValue::Long(handle),
                        JValue::Long(token),
                        JValue::Long(timeout),
                    ],
                )?;
                result.into_bool()
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorFailed, "ack"))?;
        if settled {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Timeout,
                "embedded Debezium acknowledgement did not settle within 30 seconds",
            ))
        }
    }

    pub(crate) fn stop(&self, handle: i64, timeout: Duration) -> Result<(), Error> {
        let timeout = duration_millis(timeout, "stop")?;
        let stopped = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<bool> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("stop"),
                    jni_sig!("(JJ)Z"),
                    &[JValue::Long(handle), JValue::Long(timeout)],
                )?;
                result.into_bool()
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorFailed, "stop"))?;
        if stopped {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Timeout,
                "embedded Debezium connector did not stop within the requested deadline",
            ))
        }
    }

    pub(crate) fn abandon(&self, handle: i64) {
        let _ = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<()> {
                environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("abandon"),
                    jni_sig!("(J)V"),
                    &[JValue::Long(handle)],
                )?;
                Ok(())
            });
    }

    pub(crate) fn dispose(&self, handle: i64) -> Result<(), Error> {
        self.vm
            .attach_current_thread(|environment| -> jni::errors::Result<()> {
                environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("dispose"),
                    jni_sig!("(J)V"),
                    &[JValue::Long(handle)],
                )?;
                Ok(())
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorFailed, "dispose"))
    }

    fn wait_until_running(&self, handle: i64, timeout: Duration) -> Result<(), Error> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::new(
                ErrorKind::ConnectorStartup,
                "connector startup timeout is too large",
            )
        })?;
        loop {
            let status = self.status(handle)?;
            match status.state {
                BridgeState::Running => return Ok(()),
                BridgeState::Failed | BridgeState::Stopped | BridgeState::Disposed => {
                    return Err(Error::new(
                        status.reported_error_kind(ErrorKind::ConnectorStartup),
                        "embedded Debezium connector failed during startup",
                    ));
                }
                _ if Instant::now() >= deadline => {
                    return Err(Error::new(
                        ErrorKind::Timeout,
                        "embedded Debezium connector did not start within 30 seconds",
                    ));
                }
                BridgeState::Created | BridgeState::Starting | BridgeState::Stopping => {
                    thread::sleep(START_POLL_INTERVAL);
                }
            }
        }
    }

    fn status(&self, handle: i64) -> Result<BridgeStatus, Error> {
        let bytes = self
            .vm
            .attach_current_thread(|environment| -> Result<Vec<u8>, BoundedCallError> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("status"),
                    jni_sig!("(J)[B"),
                    &[JValue::Long(handle)],
                )?;
                let object = result.into_object()?;
                let bytes = JByteArray::cast_local(environment, object)?;
                if bytes.len(environment)? > MAX_STATUS_BYTES {
                    return Err(BoundedCallError::ResponseTooLarge);
                }
                Ok(environment.convert_byte_array(&bytes)?)
            })
            .map_err(|error| match error {
                BoundedCallError::Jni => bridge_error(ErrorKind::ConnectorStartup, "status"),
                BoundedCallError::ResponseTooLarge => {
                    bridge_error(ErrorKind::Protocol, "bounded status")
                }
            })?;
        decode_status(&bytes)
    }
}

#[derive(Deserialize)]
pub(crate) struct BridgeStatus {
    protocol: u16,
    kind: String,
    state: BridgeState,
    failure_kind: Option<BridgeFailureKind>,
}

impl BridgeStatus {
    pub(crate) const fn reported_error_kind(&self, fallback: ErrorKind) -> ErrorKind {
        match self.failure_kind {
            Some(BridgeFailureKind::DeliveryTooLarge) => ErrorKind::DeliveryTooLarge,
            None => fallback,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum BridgeState {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Disposed,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BridgeFailureKind {
    DeliveryTooLarge,
}

pub(crate) fn decode_status(bytes: &[u8]) -> Result<BridgeStatus, Error> {
    let status: BridgeStatus = serde_json::from_slice(bytes)
        .map_err(|_| bridge_error(ErrorKind::Protocol, "decode status"))?;
    if i32::from(status.protocol) != BRIDGE_PROTOCOL_VERSION || status.kind != "status" {
        return Err(bridge_error(ErrorKind::Protocol, "validate status"));
    }
    Ok(status)
}

enum BoundedCallError {
    Jni,
    ResponseTooLarge,
}

impl From<jni::errors::Error> for BoundedCallError {
    fn from(_: jni::errors::Error) -> Self {
        Self::Jni
    }
}

fn duration_millis(duration: Duration, operation: &str) -> Result<i64, Error> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        Error::new(
            ErrorKind::Timeout,
            format!("{operation} timeout exceeds Java Long.MAX_VALUE milliseconds"),
        )
    })
}

fn bridge_error(kind: ErrorKind, operation: &str) -> Error {
    Error::new(
        kind,
        format!("Debezium Java bridge failed during {operation}"),
    )
}
