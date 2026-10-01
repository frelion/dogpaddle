use std::{
    fmt,
    fmt::Write as _,
    net::IpAddr,
    time::{Duration, Instant},
};

use ureq::Agent;
use url::Url;

use super::{
    definition::ClickHouseSinkDefinition,
    error::{ClickHouseSinkError, database, invalid_config, invalid_response},
};

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

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

    /// Returns a pure sink plan with the discovered Atomic database UUID.
    /// Rejects existing target objects before returning the plan.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection/catalog failures, invalid
    /// identifiers, a database without a persistent UUID, or existing objects.
    pub fn discover_target(
        &self,
        sink_id: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<ClickHouseSinkDefinition, ClickHouseSinkError> {
        let mut spec = ClickHouseSinkDefinition {
            sink_id: sink_id.into(),
            database: self.database.clone(),
            table: table.into(),
            database_uuid: String::new(),
        };
        spec.validate_names()?;
        let deadline = Instant::now() + DATABASE_TIMEOUT;
        self.command_before(
                deadline,
                &format!(
                    "SELECT toString(uuid) FROM system.databases WHERE name = {} FORMAT TabSeparatedRaw",
                    string_literal(&spec.database)
                ),
                "read database identity",
            )?
            .trim()
            .clone_into(&mut spec.database_uuid);
        spec.validate()?;
        require_absent(self, &spec, deadline)?;
        Ok(spec)
    }

    #[cfg(test)]
    pub(super) fn command(
        &self,
        sql: &str,
        stage: &'static str,
    ) -> Result<String, ClickHouseSinkError> {
        self.command_before(Instant::now() + DATABASE_TIMEOUT, sql, stage)
    }

    pub(super) fn command_before(
        &self,
        deadline: Instant,
        sql: &str,
        stage: &'static str,
    ) -> Result<String, ClickHouseSinkError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| database(stage))?;
        let mut response = self
            .agent
            .post(self.endpoint()?.as_str())
            .config()
            .timeout_global(Some(remaining))
            .build()
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .send(sql.as_bytes())
            .map_err(|_| database(stage))?;
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_string()
            .map_err(|_| invalid_response(stage))?;
        if Instant::now() >= deadline {
            return Err(database(stage));
        }
        Ok(body)
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
            .append_pair("max_execution_time", "5")
            .append_pair("timeout_overflow_mode", "throw")
            .append_pair("max_memory_usage", "67108864")
            .append_pair("cancel_http_readonly_queries_on_client_close", "1")
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

pub(super) fn require_absent(
    config: &ClickHouseSinkConfig,
    spec: &ClickHouseSinkDefinition,
    deadline: Instant,
) -> Result<(), ClickHouseSinkError> {
    let sql = format!(
        "SELECT name FROM system.tables WHERE database = {} AND name IN ({}, {}) ORDER BY name FORMAT TabSeparatedRaw",
        string_literal(spec.database()),
        string_literal(spec.table()),
        string_literal(&spec.state_table())
    );
    let body = config.command_before(deadline, &sql, "inspect target absence")?;
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
