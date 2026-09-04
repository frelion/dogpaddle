use std::ffi::OsString;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::HostError;

pub(crate) const DEFAULT_POLL_TIMEOUT_MS: u64 = 1_000;
pub(crate) const DEFAULT_START_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_STOP_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Arguments {
    pub(crate) bridge_jar: PathBuf,
    pub(crate) dependency_dir: Option<PathBuf>,
    pub(crate) config: PathBuf,
    pub(crate) java_home: Option<PathBuf>,
    pub(crate) libjvm: Option<PathBuf>,
}

impl Arguments {
    pub(crate) fn parse(values: impl IntoIterator<Item = OsString>) -> Result<Self, HostError> {
        let mut values = values.into_iter();
        let _program = values.next();
        let mut bridge_jar = None;
        let mut dependency_dir = None;
        let mut config = None;
        let mut java_home = None;
        let mut libjvm = None;

        while let Some(option) = values.next() {
            let target = match option.to_str() {
                Some("--bridge-jar") => &mut bridge_jar,
                Some("--dependency-dir") => &mut dependency_dir,
                Some("--config") => &mut config,
                Some("--java-home") => &mut java_home,
                Some("--libjvm") => &mut libjvm,
                Some("--help" | "-h") => return Err(HostError::Usage(usage())),
                Some(other) => {
                    return Err(HostError::Usage(format!(
                        "unknown option '{other}'\n{}",
                        usage()
                    )));
                }
                None => {
                    return Err(HostError::Usage(format!(
                        "option is not valid UTF-8\n{}",
                        usage()
                    )));
                }
            };
            let value = values.next().ok_or_else(|| {
                HostError::Usage(format!(
                    "{} requires a value\n{}",
                    option.to_string_lossy(),
                    usage()
                ))
            })?;
            *target = Some(PathBuf::from(value));
        }

        if java_home.is_some() && libjvm.is_some() {
            return Err(HostError::Usage(
                "--java-home and --libjvm are mutually exclusive".to_owned(),
            ));
        }

        Ok(Self {
            bridge_jar: bridge_jar.ok_or_else(|| {
                HostError::Usage(format!("--bridge-jar is required\n{}", usage()))
            })?,
            dependency_dir,
            config: config
                .ok_or_else(|| HostError::Usage(format!("--config is required\n{}", usage())))?,
            java_home,
            libjvm,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Command {
    Create { config: Option<PathBuf> },
    Start { timeout_ms: u64 },
    Poll { timeout_ms: u64, max_bytes: usize },
    Ack { token: i64 },
    Status,
    Stop { timeout_ms: u64 },
    Quit { timeout_ms: u64 },
}

#[derive(Debug, Deserialize)]
struct JsonCommand {
    #[serde(alias = "op")]
    command: String,
    timeout_ms: Option<u64>,
    max_bytes: Option<usize>,
    token: Option<i64>,
    #[serde(alias = "config")]
    config_path: Option<PathBuf>,
}

impl Command {
    pub(crate) fn parse(line: &str) -> Result<Self, HostError> {
        if line.starts_with('{') {
            let command: JsonCommand = serde_json::from_str(line)?;
            return Self::from_parts(
                &command.command,
                command.timeout_ms,
                command.max_bytes,
                command.token,
                command.config_path,
            );
        }

        let mut words = line.split_whitespace();
        let name = words
            .next()
            .ok_or_else(|| HostError::Usage("empty command".to_owned()))?;
        let arguments: Vec<&str> = words.collect();
        match name {
            "create" if arguments.len() <= 1 => Ok(Self::Create {
                config: arguments.first().map(PathBuf::from),
            }),
            "start" if arguments.len() <= 1 => Ok(Self::Start {
                timeout_ms: parse_or_default(arguments.first(), DEFAULT_START_TIMEOUT_MS)?,
            }),
            "poll" if arguments.len() <= 2 => Ok(Self::Poll {
                timeout_ms: parse_or_default(arguments.first(), DEFAULT_POLL_TIMEOUT_MS)?,
                max_bytes: parse_or_default(arguments.get(1), DEFAULT_MAX_BYTES)?,
            }),
            "ack" if arguments.len() == 1 => Ok(Self::Ack {
                token: parse_number(arguments[0], "ACK token")?,
            }),
            "status" if arguments.is_empty() => Ok(Self::Status),
            "stop" if arguments.len() <= 1 => Ok(Self::Stop {
                timeout_ms: parse_or_default(arguments.first(), DEFAULT_STOP_TIMEOUT_MS)?,
            }),
            "quit" if arguments.len() <= 1 => Ok(Self::Quit {
                timeout_ms: parse_or_default(arguments.first(), DEFAULT_STOP_TIMEOUT_MS)?,
            }),
            _ => Err(HostError::Usage(format!("invalid command '{line}'"))),
        }
    }

    fn from_parts(
        name: &str,
        timeout_ms: Option<u64>,
        max_bytes: Option<usize>,
        token: Option<i64>,
        config_path: Option<PathBuf>,
    ) -> Result<Self, HostError> {
        match name {
            "create" => Ok(Self::Create {
                config: config_path,
            }),
            "start" => Ok(Self::Start {
                timeout_ms: timeout_ms.unwrap_or(DEFAULT_START_TIMEOUT_MS),
            }),
            "poll" => Ok(Self::Poll {
                timeout_ms: timeout_ms.unwrap_or(DEFAULT_POLL_TIMEOUT_MS),
                max_bytes: max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
            }),
            "ack" => Ok(Self::Ack {
                token: token.ok_or_else(|| HostError::Usage("ACK requires token".to_owned()))?,
            }),
            "status" => Ok(Self::Status),
            "stop" => Ok(Self::Stop {
                timeout_ms: timeout_ms.unwrap_or(DEFAULT_STOP_TIMEOUT_MS),
            }),
            "quit" => Ok(Self::Quit {
                timeout_ms: timeout_ms.unwrap_or(DEFAULT_STOP_TIMEOUT_MS),
            }),
            _ => Err(HostError::Usage(format!("unknown command '{name}'"))),
        }
    }
}

fn parse_or_default<T>(value: Option<&&str>, default: T) -> Result<T, HostError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.map_or(Ok(default), |value| parse_number(value, "number"))
}

fn parse_number<T>(value: &str, description: &str) -> Result<T, HostError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| HostError::Usage(format!("invalid {description} '{value}': {error}")))
}

fn usage() -> String {
    "usage: dogpaddle-debezium-d1-host --bridge-jar PATH --config PATH \
     [--dependency-dir PATH] [--java-home PATH | --libjvm PATH]"
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_word_and_json_poll_commands() {
        assert_eq!(
            Command::parse("poll 250 4096").unwrap(),
            Command::Poll {
                timeout_ms: 250,
                max_bytes: 4096
            }
        );
        assert_eq!(
            Command::parse(r#"{"command":"poll","timeout_ms":500,"max_bytes":8192}"#).unwrap(),
            Command::Poll {
                timeout_ms: 500,
                max_bytes: 8192
            }
        );
    }

    #[test]
    fn parses_restart_capable_commands() {
        assert_eq!(
            Command::parse("create").unwrap(),
            Command::Create { config: None }
        );
        assert_eq!(
            Command::parse("create unsafe.json").unwrap(),
            Command::Create {
                config: Some(PathBuf::from("unsafe.json"))
            }
        );
        assert_eq!(
            Command::parse(r#"{"command":"create","config_path":"alternate.json"}"#).unwrap(),
            Command::Create {
                config: Some(PathBuf::from("alternate.json"))
            }
        );
        assert_eq!(
            Command::parse("start").unwrap(),
            Command::Start {
                timeout_ms: DEFAULT_START_TIMEOUT_MS
            }
        );
        assert_eq!(
            Command::parse("stop 42").unwrap(),
            Command::Stop { timeout_ms: 42 }
        );
    }

    #[test]
    fn rejects_conflicting_jvm_locations() {
        let result = Arguments::parse(
            [
                "host",
                "--bridge-jar",
                "bridge.jar",
                "--config",
                "connector.json",
                "--java-home",
                "jdk",
                "--libjvm",
                "libjvm.so",
            ]
            .map(OsString::from),
        );

        assert!(
            matches!(result, Err(HostError::Usage(message)) if message.contains("mutually exclusive"))
        );
    }
}
