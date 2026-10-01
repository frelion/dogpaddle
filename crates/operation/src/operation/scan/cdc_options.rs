use std::{num::NonZeroU32, time::Duration};

use thiserror::Error;

const MAX_JDBC_QUERY_TIMEOUT_MS: i32 = i32::MAX / 1_000 * 1_000;
const RETRY_INITIAL_DELAY_MS: i32 = 300;

/// Runtime overrides shared by `PostgreSQL` and `MySQL` CDC Scans.
///
/// Unset fields retain each Scan's discovery and connector defaults. These
/// options are ephemeral: they are not part of an Operation Definition or Flow
/// state, so supply the desired overrides again when reopening a Flow.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CdcOptions {
    pub(super) connect_timeout_ms: Option<i32>,
    pub(super) query_timeout_ms: Option<i32>,
    retry_limit: Option<i32>,
    retry_max_delay_ms: Option<i32>,
    heartbeat_interval_ms: Option<i32>,
    snapshot_fetch_size: Option<i32>,
}

/// A CDC runtime override exceeds the connector's supported bounds.
///
/// The diagnostic names the setter and explains the invalid value.
#[derive(Debug, Eq, Error, PartialEq)]
#[error("invalid CDC option {0}: {1}")]
pub struct CdcOptionsError(&'static str, &'static str);

impl CdcOptions {
    /// Creates options without overriding either Scan's runtime defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            connect_timeout_ms: None,
            query_timeout_ms: None,
            retry_limit: None,
            retry_max_delay_ms: None,
            heartbeat_interval_ms: None,
            snapshot_fetch_size: None,
        }
    }

    /// Sets both discovery and Debezium connection timeouts.
    ///
    /// `PostgreSQL` JDBC rounds this up to whole seconds; native discovery and
    /// the `MySQL` connector retain exact milliseconds.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values whose
    /// millisecond count exceeds a Java signed 32-bit integer.
    pub fn connect_timeout(mut self, timeout: Duration) -> Result<Self, CdcOptionsError> {
        self.connect_timeout_ms = Some(positive_milliseconds("connect_timeout", timeout)?);
        Ok(self)
    }

    /// Sets both discovery and Debezium query timeouts.
    ///
    /// Discovery keeps exact milliseconds. Debezium 3.6 gives JDBC whole
    /// seconds, so the connector rounds a sub-second remainder up. `MySQL`
    /// discovery applies this value to socket reads and writes.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values greater than
    /// 2,147,483,000 milliseconds.
    pub fn query_timeout(mut self, timeout: Duration) -> Result<Self, CdcOptionsError> {
        let milliseconds = positive_milliseconds("query_timeout", timeout)?;
        if milliseconds > MAX_JDBC_QUERY_TIMEOUT_MS {
            return Err(CdcOptionsError(
                "query_timeout",
                "exceeds 2,147,483,000 milliseconds",
            ));
        }
        self.query_timeout_ms = Some(milliseconds);
        Ok(self)
    }

    /// Sets Debezium's finite number of retryable polling-failure retries.
    ///
    /// Zero disables retries. Leaving this unset preserves unlimited retries.
    /// This setting applies after connector startup, not to initial startup.
    ///
    /// # Errors
    ///
    /// Rejects values greater than a Java signed 32-bit integer.
    pub fn retry_limit(mut self, limit: u32) -> Result<Self, CdcOptionsError> {
        self.retry_limit = Some(
            i32::try_from(limit)
                .map_err(|_| CdcOptionsError("retry_limit", "exceeds Java Integer.MAX_VALUE"))?,
        );
        Ok(self)
    }

    /// Sets Debezium's maximum retryable polling-failure backoff.
    ///
    /// # Errors
    ///
    /// Rejects fractional-millisecond durations, values at or below the fixed
    /// 300-millisecond initial delay, or values exceeding a Java signed integer.
    pub fn retry_max_delay(mut self, delay: Duration) -> Result<Self, CdcOptionsError> {
        let milliseconds = positive_milliseconds("retry_max_delay", delay)?;
        if milliseconds <= RETRY_INITIAL_DELAY_MS {
            return Err(CdcOptionsError(
                "retry_max_delay",
                "must exceed 300 milliseconds",
            ));
        }
        self.retry_max_delay_ms = Some(milliseconds);
        Ok(self)
    }

    /// Sets the streaming heartbeat interval.
    ///
    /// Bootstrap capture always uses its private one-millisecond heartbeat.
    ///
    /// # Errors
    ///
    /// Rejects zero, fractional-millisecond durations, or values whose
    /// millisecond count exceeds a Java signed 32-bit integer.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Result<Self, CdcOptionsError> {
        self.heartbeat_interval_ms = Some(positive_milliseconds("heartbeat_interval", interval)?);
        Ok(self)
    }

    /// Sets the initial snapshot's JDBC fetch size.
    ///
    /// # Errors
    ///
    /// Rejects values greater than a Java signed 32-bit integer.
    pub fn snapshot_fetch_size(mut self, rows: NonZeroU32) -> Result<Self, CdcOptionsError> {
        self.snapshot_fetch_size = Some(i32::try_from(rows.get()).map_err(|_| {
            CdcOptionsError("snapshot_fetch_size", "exceeds Java Integer.MAX_VALUE")
        })?);
        Ok(self)
    }

    pub(super) fn connect_timeout_or(self, default: Duration) -> Duration {
        self.connect_timeout_ms
            .map_or(default, milliseconds_duration)
    }

    pub(super) fn query_timeout_or(self, default: Duration) -> Duration {
        self.query_timeout_ms.map_or(default, milliseconds_duration)
    }

    pub(super) fn connector_properties(
        self,
        connect_timeout: (&'static str, String),
        snapshot: bool,
        default_query_timeout_ms: i32,
        default_snapshot_fetch_size: Option<i32>,
    ) -> Vec<(&'static str, String)> {
        let query_timeout_ms = self.query_timeout_ms.unwrap_or(default_query_timeout_ms);
        let jdbc_query_timeout_ms =
            (query_timeout_ms / 1_000 + i32::from(query_timeout_ms % 1_000 != 0)) * 1_000;
        let heartbeat_interval_ms = if snapshot {
            1
        } else {
            self.heartbeat_interval_ms.unwrap_or(1_000)
        };
        let mut properties = vec![
            connect_timeout,
            (
                "database.query.timeout.ms",
                jdbc_query_timeout_ms.to_string(),
            ),
            (
                "errors.max.retries",
                self.retry_limit.unwrap_or(-1).to_string(),
            ),
            (
                "errors.retry.delay.initial.ms",
                RETRY_INITIAL_DELAY_MS.to_string(),
            ),
            (
                "errors.retry.delay.max.ms",
                self.retry_max_delay_ms.unwrap_or(10_000).to_string(),
            ),
            ("heartbeat.interval.ms", heartbeat_interval_ms.to_string()),
        ];
        if snapshot && let Some(rows) = self.snapshot_fetch_size.or(default_snapshot_fetch_size) {
            properties.push(("snapshot.fetch.size", rows.to_string()));
        }
        properties
    }
}

fn positive_milliseconds(label: &'static str, duration: Duration) -> Result<i32, CdcOptionsError> {
    let milliseconds = i32::try_from(duration.as_millis()).map_err(|_| {
        CdcOptionsError(
            label,
            "duration exceeds Java Integer.MAX_VALUE milliseconds",
        )
    })?;
    if milliseconds == 0 || !duration.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(CdcOptionsError(
            label,
            "duration must be a positive whole number of milliseconds",
        ));
    }
    Ok(milliseconds)
}

fn milliseconds_duration(milliseconds: i32) -> Duration {
    Duration::from_millis(u64::try_from(milliseconds).expect("validated timeout is positive"))
}
