use std::{fmt, fmt::Write as _, net::IpAddr, time::Duration};

use serde::{Deserialize, Serialize};
use ureq::Agent;
use url::Url;

use super::error::{ClickHouseSinkError, database, invalid_config, invalid_response, invalid_spec};

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IDENTIFIER_BYTES: usize = 255;
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SINK_ID_BYTES: usize = 32;

/// Ephemeral HTTP credentials and endpoint for one `ClickHouse` sink.
pub struct ClickHouseSinkConfig {
    host: IpAddr,
    port: u16,
    database: String,
    user: String,
    password: String,
    agent: Agent,
}

impl ClickHouseSinkConfig {
    /// Creates an unencrypted numeric-IP HTTP configuration.
    ///
    /// # Errors
    ///
    /// Rejects a nonnumeric host, zero port, blank database/user, or control
    /// characters that cannot be placed in HTTP headers.
    pub fn new_unencrypted(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, ClickHouseSinkError> {
        let host = host
            .into()
            .parse()
            .map_err(|_| invalid_config("host must be a numeric IPv4 or IPv6 address"))?;
        let database = database.into();
        let user = user.into();
        let password = password.into();
        if port == 0 {
            return Err(invalid_config("port must be nonzero"));
        }
        if [&database, &user]
            .iter()
            .any(|value| value.trim().is_empty() || contains_header_control(value))
            || contains_header_control(&password)
        {
            return Err(invalid_config("invalid ClickHouse connection fields"));
        }
        let agent = Agent::config_builder()
            .timeout_global(Some(DATABASE_TIMEOUT))
            .max_redirects(0)
            .build()
            .into();
        Ok(Self {
            host,
            port,
            database,
            user,
            password,
            agent,
        })
    }

    /// Discovers the Atomic database UUID and rejects existing target objects.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection/catalog failures, invalid
    /// identifiers, a database without a persistent UUID, or existing objects.
    pub fn discover_target(
        &self,
        sink_id: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<ClickHouseTargetSpec, ClickHouseSinkError> {
        let mut spec = ClickHouseTargetSpec {
            sink_id: sink_id.into(),
            database: self.database.clone(),
            table: table.into(),
            database_uuid: String::new(),
        };
        spec.validate_names()?;
        self.command(
                &format!(
                    "SELECT toString(uuid) FROM system.databases WHERE name = {} FORMAT TabSeparatedRaw",
                    string_literal(&spec.database)
                ),
                "read database identity",
            )?
            .trim()
            .clone_into(&mut spec.database_uuid);
        spec.validate()?;
        require_absent(self, &spec)?;
        Ok(spec)
    }

    pub(super) fn command(
        &self,
        sql: &str,
        stage: &'static str,
    ) -> Result<String, ClickHouseSinkError> {
        let mut response = self
            .agent
            .post(self.endpoint()?.as_str())
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .send(sql.as_bytes())
            .map_err(|_| database(stage))?;
        response
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_string()
            .map_err(|_| invalid_response(stage))
    }

    fn endpoint(&self) -> Result<Url, ClickHouseSinkError> {
        let host = match self.host {
            IpAddr::V4(address) => address.to_string(),
            IpAddr::V6(address) => format!("[{address}]"),
        };
        let mut url = Url::parse(&format!("http://{host}:{}/", self.port))
            .map_err(|_| invalid_config("invalid HTTP endpoint"))?;
        url.query_pairs_mut()
            .append_pair("database", &self.database)
            .append_pair("wait_end_of_query", "1")
            .append_pair("max_query_size", "16777216")
            .append_pair("send_progress_in_http_headers", "0");
        Ok(url)
    }

    pub(super) fn database(&self) -> &str {
        &self.database
    }
}

impl fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseSinkConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Non-sensitive persistent identity of a sink-owned `ClickHouse` target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseTargetSpec {
    sink_id: String,
    database: String,
    table: String,
    database_uuid: String,
}

impl ClickHouseTargetSpec {
    /// Builds and validates a target specification.
    ///
    /// # Errors
    ///
    /// Rejects invalid names or a zero/malformed database UUID.
    pub fn try_new(
        sink_id: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
        database_uuid: impl Into<String>,
    ) -> Result<Self, ClickHouseSinkError> {
        let spec = Self {
            sink_id: sink_id.into(),
            database: database.into(),
            table: table.into(),
            database_uuid: database_uuid.into(),
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Validates decoded persistent fields.
    ///
    /// # Errors
    ///
    /// Rejects invalid names or a zero/malformed database UUID.
    pub fn validate(&self) -> Result<(), ClickHouseSinkError> {
        self.validate_names()?;
        if self.database_uuid.len() != 36
            || !self.database_uuid.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }
            })
            || self.database_uuid == "00000000-0000-0000-0000-000000000000"
        {
            return Err(invalid_spec(
                "database UUID must be a nonzero canonical UUID",
            ));
        }
        Ok(())
    }

    fn validate_names(&self) -> Result<(), ClickHouseSinkError> {
        if self.sink_id.is_empty()
            || self.sink_id.len() > MAX_SINK_ID_BYTES
            || !self
                .sink_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(invalid_spec(
                "sink ID must contain 1–32 lowercase ASCII letters, digits, or underscores",
            ));
        }
        for (label, value) in [("database", &self.database), ("table", &self.table)] {
            if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.contains('\0') {
                return Err(invalid_spec(format!(
                    "{label} must be a nonempty ClickHouse identifier of at most 255 bytes"
                )));
            }
        }
        if self.state_table().len() > MAX_IDENTIFIER_BYTES {
            return Err(invalid_spec("derived state-table name exceeds 255 bytes"));
        }
        if self.table == self.state_table() {
            return Err(invalid_spec("target view collides with the state table"));
        }
        Ok(())
    }

    /// Sink identity.
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Exposed target view.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Atomic database UUID captured during discovery.
    #[must_use]
    pub fn database_uuid(&self) -> &str {
        &self.database_uuid
    }

    pub(super) fn state_table(&self) -> String {
        format!("$dogpaddle.state.{}", self.sink_id)
    }

    pub(super) fn marker(&self) -> String {
        format!("dogpaddle.clickhouse-sink.v1:{}", self.sink_id)
    }
}

pub(super) fn require_absent(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseTargetSpec,
) -> Result<(), ClickHouseSinkError> {
    let sql = format!(
        "SELECT name FROM system.tables WHERE database = {} AND name IN ({}, {}) ORDER BY name FORMAT TabSeparatedRaw",
        string_literal(spec.database()),
        string_literal(spec.table()),
        string_literal(&spec.state_table())
    );
    let body = config.command(&sql, "inspect target absence")?;
    if let Some(name) = body.lines().next().filter(|name| !name.is_empty()) {
        Err(ClickHouseSinkError::TargetExists {
            name: name.to_owned(),
        })
    } else {
        Ok(())
    }
}

pub(super) fn string_literal(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('\'');
    for byte in value.bytes() {
        match byte {
            b'\\' => encoded.push_str("\\\\"),
            b'\'' => encoded.push_str("\\'"),
            b'\n' => encoded.push_str("\\n"),
            b'\r' => encoded.push_str("\\r"),
            b'\t' => encoded.push_str("\\t"),
            0x20..=0x7e => encoded.push(char::from(byte)),
            _ => write!(encoded, "\\x{byte:02x}").expect("writing to String cannot fail"),
        }
    }
    encoded.push('\'');
    encoded
}

fn contains_header_control(value: &str) -> bool {
    value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
}
