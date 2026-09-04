use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use jni::objects::{JByteArray, Reference};
use jni::{InitArgsBuilder, JValue, JavaVM, jni_sig, jni_str};
use serde_json::Value;

use crate::cli::Arguments;
use crate::error::HostError;

pub(crate) struct Bridge {
    vm: JavaVM,
}

impl Bridge {
    pub(crate) fn launch(arguments: &Arguments) -> Result<Self, HostError> {
        let classpath = classpath(arguments)?;
        let classpath = classpath.to_str().ok_or_else(|| {
            HostError::Usage("JVM classpath contains a non-UTF-8 path".to_owned())
        })?;
        let init_args = InitArgsBuilder::new()
            .option(format!("-Djava.class.path={classpath}"))
            .option("-Dfile.encoding=UTF-8")
            .option("-Dorg.slf4j.simpleLogger.logFile=System.err")
            .build()?;

        let explicit_libjvm = arguments
            .libjvm
            .clone()
            .or_else(|| arguments.java_home.as_deref().map(libjvm_under));
        let vm = if let Some(libjvm) = explicit_libjvm {
            if !libjvm.is_file() {
                return Err(HostError::Usage(format!(
                    "JVM shared library does not exist: {}",
                    libjvm.display()
                )));
            }
            JavaVM::with_libjvm(init_args, || Ok(libjvm))?
        } else {
            JavaVM::new(init_args)?
        };
        Ok(Self { vm })
    }

    pub(crate) fn create(&self, configuration: &[u8]) -> Result<i64, HostError> {
        self.vm.attach_current_thread(|environment| {
            let json = environment.byte_array_from_slice(configuration)?;
            let result = environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("create"),
                jni_sig!("([B)J"),
                &[JValue::Object(json.as_ref())],
            )?;
            Ok(result.into_long()?)
        })
    }

    pub(crate) fn start(&self, handle: i64) -> Result<(), HostError> {
        self.vm.attach_current_thread(|environment| {
            environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("start"),
                jni_sig!("(J)V"),
                &[JValue::Long(handle)],
            )?;
            Ok(())
        })
    }

    pub(crate) fn poll(
        &self,
        handle: i64,
        timeout_ms: u64,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, HostError> {
        let timeout_ms = i64::try_from(timeout_ms)
            .map_err(|_| HostError::Usage("poll timeout exceeds Java long".to_owned()))?;
        let max_bytes = i32::try_from(max_bytes)
            .map_err(|_| HostError::Usage("poll max_bytes exceeds Java int".to_owned()))?;
        self.vm.attach_current_thread(|environment| {
            let result = environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("poll"),
                jni_sig!("(JJI)[B"),
                &[
                    JValue::Long(handle),
                    JValue::Long(timeout_ms),
                    JValue::Int(max_bytes),
                ],
            )?;
            let object = result.into_object()?;
            if object.is_null() {
                return Ok(None);
            }
            let bytes = JByteArray::cast_local(environment, object)?;
            Ok(Some(environment.convert_byte_array(&bytes)?))
        })
    }

    pub(crate) fn ack(&self, handle: i64, token: i64) -> Result<(), HostError> {
        self.vm.attach_current_thread(|environment| {
            environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("ack"),
                jni_sig!("(JJ)V"),
                &[JValue::Long(handle), JValue::Long(token)],
            )?;
            Ok(())
        })
    }

    pub(crate) fn stop(&self, handle: i64, timeout_ms: u64) -> Result<(), HostError> {
        let timeout_ms = i64::try_from(timeout_ms)
            .map_err(|_| HostError::Usage("stop timeout exceeds Java long".to_owned()))?;
        self.vm.attach_current_thread(|environment| {
            environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("stop"),
                jni_sig!("(JJ)V"),
                &[JValue::Long(handle), JValue::Long(timeout_ms)],
            )?;
            Ok(())
        })
    }

    pub(crate) fn status(&self, handle: i64) -> Result<Value, HostError> {
        let bytes = self.vm.attach_current_thread(|environment| {
            let result = environment.call_static_method(
                jni_str!("dev/dogpaddle/experiments/debeziumd1/D1Bridge"),
                jni_str!("status"),
                jni_sig!("(J)[B"),
                &[JValue::Long(handle)],
            )?;
            let object = result.into_object()?;
            if object.is_null() {
                return Err(HostError::Usage(
                    "bridge returned null status bytes".to_owned(),
                ));
            }
            let bytes = JByteArray::cast_local(environment, object)?;
            Ok(environment.convert_byte_array(&bytes)?)
        })?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

fn classpath(arguments: &Arguments) -> Result<PathBuf, HostError> {
    let dependency_dir = arguments.dependency_dir.clone().unwrap_or_else(|| {
        arguments
            .bridge_jar
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("dependency")
    });
    if !dependency_dir.is_dir() {
        return Err(HostError::Usage(format!(
            "runtime dependency directory does not exist: {} (run Maven package first)",
            dependency_dir.display()
        )));
    }

    let mut entries = vec![arguments.bridge_jar.clone()];
    for entry in fs::read_dir(&dependency_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "jar") {
            entries.push(path);
        }
    }
    entries[1..].sort();
    env::join_paths(entries)
        .map(PathBuf::from)
        .map_err(|error| HostError::Usage(format!("cannot construct JVM classpath: {error}")))
}

#[cfg(target_os = "windows")]
fn libjvm_under(java_home: &Path) -> PathBuf {
    java_home.join("bin").join("server").join("jvm.dll")
}

#[cfg(target_os = "macos")]
fn libjvm_under(java_home: &Path) -> PathBuf {
    java_home.join("lib").join("server").join("libjvm.dylib")
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn libjvm_under(java_home: &Path) -> PathBuf {
    java_home.join("lib").join("server").join("libjvm.so")
}
