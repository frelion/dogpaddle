use std::{fmt, net::IpAddr, time::Duration};

use mysql::{Conn, OptsBuilder, params, prelude::Queryable};
use serde::{Deserialize, Serialize};

use super::error::{DorisSinkError, database, invalid_config, invalid_spec};

const DATABASE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IDENTIFIER_BYTES: usize = 64;
const MAX_SINK_ID_BYTES: usize = 32;

/// Ephemeral credentials and endpoint for one Apache Doris sink.
pub struct DorisSinkConfig {
    host: IpAddr,
    port: u16,
    database: String,
    user: String,
    password: String,
}

impl DorisSinkConfig {
    /// Creates an unencrypted MySQL-protocol runtime configuration.
    ///
    /// # Errors
    ///
    /// Rejects a nonnumeric host, zero port, blank database/user, or NUL bytes.
    pub fn new_unencrypted(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, DorisSinkError> {
        let host = host
            .into()
            .parse()
            .map_err(|_| invalid_config("host must be a numeric IPv4 or IPv6 address"))?;
        let config = Self {
            host,
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        };
        if port == 0 {
            return Err(invalid_config("port must be nonzero"));
        }
        if [&config.database, &config.user]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || config.password.contains('\0')
        {
            return Err(invalid_config("invalid Doris connection fields"));
        }
        Ok(config)
    }

    /// Discovers the stable cluster identity and rejects existing target objects.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for connection/catalog failures, invalid names,
    /// a missing database, or an existing target object.
    pub fn discover_target(
        &self,
        sink_id: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<DorisTargetSpec, DorisSinkError> {
        let mut spec = DorisTargetSpec {
            sink_id: sink_id.into(),
            database: self.database.clone(),
            table: table.into(),
            cluster_id: 0,
        };
        spec.validate_names()?;
        let mut connection = self.connect()?;
        let exists: Option<u8> = connection
            .exec_first(
                "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = :database",
                params! { "database" => spec.database() },
            )
            .map_err(|_| database("read database identity"))?;
        if exists != Some(1) {
            return Err(invalid_spec("target database does not exist"));
        }
        let ids: Vec<u64> = connection
            .query("SELECT DISTINCT ClusterId FROM frontends()")
            .map_err(|_| database("read cluster identity"))?;
        let [cluster_id] = ids.as_slice() else {
            return Err(invalid_spec(
                "Doris frontend metadata does not expose one cluster identity",
            ));
        };
        spec.cluster_id = *cluster_id;
        spec.validate()?;
        require_absent(&mut connection, &spec)?;
        Ok(spec)
    }

    pub(super) fn connect(&self) -> Result<Conn, DorisSinkError> {
        let options = OptsBuilder::new()
            .ip_or_hostname(Some(self.host.to_string()))
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(DATABASE_TIMEOUT))
            .read_timeout(Some(DATABASE_TIMEOUT))
            .write_timeout(Some(DATABASE_TIMEOUT));
        Conn::new(options).map_err(|_| database("connect"))
    }

    pub(super) fn database(&self) -> &str {
        &self.database
    }
}

impl fmt::Debug for DorisSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DorisSinkConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .finish()
    }
}

/// Non-sensitive persistent identity of a sink-owned Doris target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DorisTargetSpec {
    sink_id: String,
    database: String,
    table: String,
    cluster_id: u64,
}

impl DorisTargetSpec {
    /// Builds and validates a target specification.
    ///
    /// # Errors
    ///
    /// Rejects invalid identifiers or a zero cluster identity.
    pub fn try_new(
        sink_id: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
        cluster_id: u64,
    ) -> Result<Self, DorisSinkError> {
        let spec = Self {
            sink_id: sink_id.into(),
            database: database.into(),
            table: table.into(),
            cluster_id,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Validates decoded persistent fields.
    ///
    /// # Errors
    ///
    /// Rejects invalid identifiers or a zero cluster identity.
    pub fn validate(&self) -> Result<(), DorisSinkError> {
        self.validate_names()?;
        if self.cluster_id == 0 {
            return Err(invalid_spec("cluster identity must be nonzero"));
        }
        Ok(())
    }

    fn validate_names(&self) -> Result<(), DorisSinkError> {
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
                    "{label} must be a nonempty Doris identifier of at most 64 bytes"
                )));
            }
        }
        if self.state_table().len() > MAX_IDENTIFIER_BYTES {
            return Err(invalid_spec("derived state-table name exceeds 64 bytes"));
        }
        if self.table.eq_ignore_ascii_case(&self.state_table()) {
            return Err(invalid_spec("target view collides with the state table"));
        }
        Ok(())
    }

    /// Stable sink identity.
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Target database.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Exposed target view.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Doris cluster identity captured during discovery.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub(super) fn state_table(&self) -> String {
        format!("$dogpaddle.state.{}", self.sink_id)
    }

    pub(super) fn object_names(&self) -> [String; 2] {
        [self.table.clone(), self.state_table()]
    }

    pub(super) fn marker(&self) -> String {
        format!("dogpaddle.doris-sink.v1:{}", self.sink_id)
    }
}

pub(super) fn require_absent(
    connection: &mut Conn,
    spec: &DorisTargetSpec,
) -> Result<(), DorisSinkError> {
    let names = spec.object_names();
    let existing: Vec<String> = connection
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = :database AND TABLE_NAME IN (:target, :state)",
            params! {
                "database" => spec.database(),
                "target" => names[0].as_str(),
                "state" => names[1].as_str(),
            },
        )
        .map_err(|_| database("inspect target absence"))?;
    if let Some(name) = existing.into_iter().next() {
        Err(DorisSinkError::TargetExists { name })
    } else {
        Ok(())
    }
}
