use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum HostError {
    #[error("{0}")]
    Usage(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("JNI error: {0}")]
    Jni(#[from] jni::errors::Error),
    #[error("JVM option error: {0}")]
    JvmOption(#[from] jni::vm::JvmError),
    #[error("cannot start JVM: {0}")]
    StartJvm(#[from] jni::errors::StartJvmError),
}
