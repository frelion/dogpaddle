use std::ffi::OsString;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::HostError;

pub(crate) const DEFAULT_POLL_TIMEOUT_MS: u64 = 1_000;
pub(crate) const DEFAULT_STOP_TIMEOUT_MS: u64 = 30_000;
pub(crate) const DEFAULT_MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Arguments {
    pub(crate) distribution: PathBuf,
    pub(crate) config: PathBuf,
    pub(crate) checkpoint: PathBuf,
    pub(crate) max_delivery_bytes: usize,
}

impl Arguments {
    pub(crate) fn parse(values: impl IntoIterator<Item = OsString>) -> Result<Self, HostError> {
        let mut values = values.into_iter();
        let _program = values.next();
        let mut distribution = None;
        let mut config = None;
        let mut checkpoint = None;
        let mut max_delivery_bytes = None;

        while let Some(option) = values.next() {
            if matches!(option.to_str(), Some("--help" | "-h")) {
                return Err(HostError::Usage(usage()));
            }
            let value = values.next().ok_or_else(|| {
                HostError::Usage(format!(
                    "{} requires a value\n{}",
                    option.to_string_lossy(),
                    usage()
                ))
            })?;
            match option.to_str() {
                Some("--distribution") => distribution = Some(PathBuf::from(value)),
                Some("--config") => config = Some(PathBuf::from(value)),
                Some("--checkpoint") => checkpoint = Some(PathBuf::from(value)),
                Some("--max-delivery-bytes") => {
                    let value = value.to_str().ok_or_else(|| {
                        HostError::Usage("maximum delivery bytes are not valid UTF-8".to_owned())
                    })?;
                    max_delivery_bytes = Some(parse_number(value, "maximum delivery bytes")?);
                }
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
            }
        }

        Ok(Self {
            distribution: distribution.ok_or_else(|| {
                HostError::Usage(format!("--distribution is required\n{}", usage()))
            })?,
            config: config
                .ok_or_else(|| HostError::Usage(format!("--config is required\n{}", usage())))?,
            checkpoint: checkpoint.ok_or_else(|| {
                HostError::Usage(format!("--checkpoint is required\n{}", usage()))
            })?,
            max_delivery_bytes: max_delivery_bytes.unwrap_or(DEFAULT_MAX_DELIVERY_BYTES),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Command {
    Start,
    Poll { timeout_ms: u64 },
    Save,
    Ack { token: u64 },
    Status,
    Stop { timeout_ms: u64 },
    Quit { timeout_ms: u64 },
}

#[derive(Debug, Deserialize)]
struct JsonCommand {
    #[serde(alias = "op")]
    command: String,
    timeout_ms: Option<u64>,
    token: Option<u64>,
}

impl Command {
    pub(crate) fn parse(line: &str) -> Result<Self, HostError> {
        if line.starts_with('{') {
            let command: JsonCommand = serde_json::from_str(line)?;
            return Self::from_parts(&command.command, command.timeout_ms, command.token);
        }

        let mut words = line.split_whitespace();
        let name = words
            .next()
            .ok_or_else(|| HostError::Usage("empty command".to_owned()))?;
        let arguments: Vec<&str> = words.collect();
        match name {
            "start" if arguments.is_empty() => Ok(Self::Start),
            "poll" if arguments.len() <= 1 => Ok(Self::Poll {
                timeout_ms: parse_or_default(arguments.first(), DEFAULT_POLL_TIMEOUT_MS)?,
            }),
            "save" if arguments.is_empty() => Ok(Self::Save),
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
        token: Option<u64>,
    ) -> Result<Self, HostError> {
        match name {
            "start" => Ok(Self::Start),
            "poll" => Ok(Self::Poll {
                timeout_ms: timeout_ms.unwrap_or(DEFAULT_POLL_TIMEOUT_MS),
            }),
            "save" => Ok(Self::Save),
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
    "usage: dogpaddle-debezium-d1-host --distribution PATH --config PATH \\
     --checkpoint PATH [--max-delivery-bytes BYTES]"
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_word_and_json_commands() {
        assert_eq!(
            Command::parse("poll 250").unwrap(),
            Command::Poll { timeout_ms: 250 }
        );
        assert_eq!(
            Command::parse(r#"{"command":"poll","timeout_ms":500}"#).unwrap(),
            Command::Poll { timeout_ms: 500 }
        );
        assert_eq!(Command::parse("save").unwrap(), Command::Save);
        assert_eq!(
            Command::parse(r#"{"command":"ack","token":7}"#).unwrap(),
            Command::Ack { token: 7 }
        );
    }

    #[test]
    fn parses_product_runtime_arguments() {
        let arguments = Arguments::parse(
            [
                "host",
                "--distribution",
                "distribution",
                "--config",
                "connector.json",
                "--checkpoint",
                "checkpoint.bin",
                "--max-delivery-bytes",
                "4096",
            ]
            .map(OsString::from),
        )
        .unwrap();

        assert_eq!(arguments.distribution, PathBuf::from("distribution"));
        assert_eq!(arguments.config, PathBuf::from("connector.json"));
        assert_eq!(arguments.checkpoint, PathBuf::from("checkpoint.bin"));
        assert_eq!(arguments.max_delivery_bytes, 4096);
    }
}
