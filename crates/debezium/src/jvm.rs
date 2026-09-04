use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use jni::objects::{JByteArray, Reference};
use jni::{InitArgsBuilder, JValue, JavaVM, jni_sig, jni_str};

use crate::bundle::Bundle;
use crate::connector::Connector;
use crate::{Checkpoint, ConnectorConfig, Error, ErrorKind};

const START_TIMEOUT: Duration = Duration::from_secs(30);
const BRIDGE_PROTOCOL_VERSION: i32 = 1;
const FAILURE_NONE: i32 = 0;
const FAILURE_DELIVERY_TOO_LARGE: i32 = 1;

static JVM_HOST: OnceLock<Mutex<Option<Arc<JvmHost>>>> = OnceLock::new();

/// A cloneable reference to `DogPaddle`'s process-wide embedded JVM.
///
/// The first successful call to [`DebeziumRuntime::open`] fixes the
/// self-contained runtime bundle for the process. Opening the same canonical
/// bundle root again reuses that JVM; opening a different root fails explicitly.
/// The installed bundle must remain immutable for the life of the process.
/// `DogPaddle` must be the first and only JVM initializer during process startup;
/// another JNI component must not race [`DebeziumRuntime::open`] or initialize a
/// JVM before it.
#[derive(Clone)]
pub struct DebeziumRuntime {
    host: Arc<JvmHost>,
}

impl DebeziumRuntime {
    /// Opens a validated, self-contained Debezium bundle and starts or reuses
    /// its process-wide JVM.
    ///
    /// `bundle` must contain the pinned platform-specific Temurin runtime under
    /// `runtime/` and the pinned Java bridge and dependencies under
    /// `debezium/`. No system Java installation or `JAVA_HOME` fallback is
    /// consulted.
    ///
    /// # Errors
    ///
    /// Returns an error when the bundle is invalid, the bundled JVM cannot be
    /// started, or another bundle already initialized the process JVM.
    pub fn open(bundle: impl AsRef<Path>) -> Result<Self, Error> {
        let bundle = Bundle::open(bundle.as_ref())?;
        let slot = JVM_HOST.get_or_init(|| Mutex::new(None));
        let mut guard = slot.lock().map_err(|_| {
            Error::new(
                ErrorKind::JvmStartup,
                "process JVM initialization lock is poisoned",
            )
        })?;

        if let Some(host) = guard.as_ref() {
            if host.bundle_root != bundle.root() {
                return Err(Error::new(
                    ErrorKind::JvmConfigurationConflict,
                    format!(
                        "the process JVM is already bound to Debezium runtime bundle {}",
                        host.bundle_root.display()
                    ),
                ));
            }
            return Ok(Self {
                host: Arc::clone(host),
            });
        }

        let host = Arc::new(JvmHost::launch(&bundle)?);
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

        let result = self.host.start(handle, START_TIMEOUT).map(|()| {
            Connector::new(
                Arc::clone(&self.host),
                handle,
                engine_name,
                connector_class,
                delivery_bound,
            )
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
            .field("bundle", &self.host.bundle_root)
            .finish_non_exhaustive()
    }
}

pub(crate) struct JvmHost {
    vm: JavaVM,
    bundle_root: PathBuf,
}

impl JvmHost {
    fn launch(bundle: &Bundle) -> Result<Self, Error> {
        let classpath_option = format!("-Djava.class.path={}", bundle.classpath());
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
        if JavaVM::singleton().is_ok() {
            return Err(Error::new(
                ErrorKind::JvmConfigurationConflict,
                "a JVM was initialized before the Debezium runtime bundle",
            ));
        }
        let jvm_library = bundle.jvm_library().to_path_buf();
        let vm = JavaVM::with_libjvm(arguments, || Ok(&jvm_library)).map_err(|error| {
            Error::new(
                ErrorKind::JvmStartup,
                format!(
                    "cannot start the JVM from bundled library {}: {error}",
                    jvm_library.display()
                ),
            )
        })?;
        let host = Self {
            vm,
            bundle_root: bundle.root().to_path_buf(),
        };
        host.validate_bridge_and_runtime()?;
        Ok(host)
    }

    fn validate_bridge_and_runtime(&self) -> Result<(), Error> {
        let version = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<i32> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("protocolVersion"),
                    jni_sig!("()I"),
                    &[],
                )?;
                let version = result.into_int()?;
                environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("verifyRuntime"),
                    jni_sig!("()V"),
                    &[],
                )?;
                Ok(version)
            })
            .map_err(|_| {
                Error::new(
                    ErrorKind::JvmStartup,
                    "embedded JVM cannot load the pinned Debezium bridge or required charset, timezone, TLS and DNS resources; restart the process after fixing the runtime bundle",
                )
            })?;
        if version != BRIDGE_PROTOCOL_VERSION {
            return Err(Error::new(
                ErrorKind::Protocol,
                "embedded Debezium bridge protocol is not supported; restart the process with the pinned runtime bundle",
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

    pub(crate) fn start(&self, handle: i64, timeout: Duration) -> Result<(), Error> {
        let timeout = duration_millis(timeout, "start")?;
        let started = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<bool> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("start"),
                    jni_sig!("(JJ)Z"),
                    &[JValue::Long(handle), JValue::Long(timeout)],
                )?;
                result.into_bool()
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorStartup, "start"))?;
        if started {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Timeout,
                "embedded Debezium connector did not start within 30 seconds",
            ))
        }
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
                BoundedCallError::Jni => match self.failure_kind(handle) {
                    Ok(FAILURE_NONE) | Err(_) => bridge_error(ErrorKind::ConnectorFailed, "poll"),
                    Ok(FAILURE_DELIVERY_TOO_LARGE) => {
                        bridge_error(ErrorKind::DeliveryTooLarge, "poll")
                    }
                    Ok(_) => bridge_error(ErrorKind::Protocol, "failure kind"),
                },
                BoundedCallError::ResponseTooLarge => Error::new(
                    ErrorKind::DeliveryTooLarge,
                    "Java bridge returned a delivery larger than its configured bound",
                ),
            })
    }

    pub(crate) fn ack(&self, handle: i64, timeout: Duration) -> Result<(), Error> {
        let timeout = duration_millis(timeout, "ack")?;
        let settled = self
            .vm
            .attach_current_thread(|environment| -> jni::errors::Result<bool> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("ack"),
                    jni_sig!("(JJ)Z"),
                    &[JValue::Long(handle), JValue::Long(timeout)],
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

    fn failure_kind(&self, handle: i64) -> Result<i32, Error> {
        self.vm
            .attach_current_thread(|environment| -> jni::errors::Result<i32> {
                let result = environment.call_static_method(
                    jni_str!("dev/dogpaddle/debezium/DebeziumBridge"),
                    jni_str!("failureKind"),
                    jni_sig!("(J)I"),
                    &[JValue::Long(handle)],
                )?;
                result.into_int()
            })
            .map_err(|_| bridge_error(ErrorKind::ConnectorFailed, "failure kind"))
    }
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
