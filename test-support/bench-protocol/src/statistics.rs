use std::{fmt, time::Duration};

use serde::Serialize;

/// Min/median/max statistics for ordinary benchmark samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DurationSummary {
    samples: usize,
    #[serde(rename = "min_ns")]
    min: DurationNanos,
    #[serde(rename = "median_ns")]
    median: DurationNanos,
    #[serde(rename = "max_ns")]
    max: DurationNanos,
}

impl DurationSummary {
    /// Computes a summary from a non-empty set of duration samples.
    ///
    /// For an even sample count, the median is the upper middle element. This
    /// preserves the established ordinary benchmark summary convention;
    /// endurance p50 remains nearest-rank via [`LatencySummary`].
    ///
    /// # Errors
    ///
    /// Returns [`StatisticsError::EmptySamples`] when `samples` is empty.
    pub fn from_samples(samples: &[Duration]) -> Result<Self, StatisticsError> {
        let sorted = sorted(samples)?;
        let max = *sorted.last().ok_or(StatisticsError::EmptySamples)?;
        Ok(Self {
            samples: sorted.len(),
            min: DurationNanos(sorted[0]),
            median: DurationNanos(sorted[sorted.len() / 2]),
            max: DurationNanos(max),
        })
    }

    /// Returns the number of samples summarized.
    #[must_use]
    pub const fn samples(self) -> usize {
        self.samples
    }

    /// Returns the minimum duration.
    #[must_use]
    pub const fn min(self) -> Duration {
        self.min.0
    }

    /// Returns the median, using the upper middle element for an even count.
    #[must_use]
    pub const fn median(self) -> Duration {
        self.median.0
    }

    /// Returns the maximum duration.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max.0
    }
}

/// P50/p95/p99/max latency statistics for endurance protocols.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LatencySummary {
    samples: usize,
    #[serde(rename = "p50_ns")]
    p50: DurationNanos,
    #[serde(rename = "p95_ns")]
    p95: DurationNanos,
    #[serde(rename = "p99_ns")]
    p99: DurationNanos,
    #[serde(rename = "max_ns")]
    max: DurationNanos,
}

impl LatencySummary {
    /// Computes nearest-rank endurance percentiles from non-empty samples.
    ///
    /// # Errors
    ///
    /// Returns [`StatisticsError::EmptySamples`] when `samples` is empty.
    pub fn from_samples(samples: &[Duration]) -> Result<Self, StatisticsError> {
        let sorted = sorted(samples)?;
        let max = *sorted.last().ok_or(StatisticsError::EmptySamples)?;
        Ok(Self {
            samples: sorted.len(),
            p50: DurationNanos(percentile_of_sorted(&sorted, 50)),
            p95: DurationNanos(percentile_of_sorted(&sorted, 95)),
            p99: DurationNanos(percentile_of_sorted(&sorted, 99)),
            max: DurationNanos(max),
        })
    }

    /// Returns the number of samples summarized.
    #[must_use]
    pub const fn samples(self) -> usize {
        self.samples
    }

    /// Returns the nearest-rank p50 latency.
    #[must_use]
    pub const fn p50(self) -> Duration {
        self.p50.0
    }

    /// Returns the nearest-rank p95 latency.
    #[must_use]
    pub const fn p95(self) -> Duration {
        self.p95.0
    }

    /// Returns the nearest-rank p99 latency.
    #[must_use]
    pub const fn p99(self) -> Duration {
        self.p99.0
    }

    /// Returns the maximum observed latency.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max.0
    }
}

/// Statistics derived from paired first/second benchmark samples.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct PairedDurationSummary {
    samples: usize,
    median_first_over_second: f64,
    second_wins: usize,
}

impl PairedDurationSummary {
    /// Computes a median paired ratio and win count.
    ///
    /// Ratios are computed pairwise as `first / second`; the win count is the
    /// number of pairs where the second duration is strictly smaller.
    ///
    /// # Errors
    ///
    /// Returns [`StatisticsError`] when either slice is empty, lengths differ, or
    /// any duration is zero.
    pub fn from_pairs(first: &[Duration], second: &[Duration]) -> Result<Self, StatisticsError> {
        if first.len() != second.len() {
            return Err(StatisticsError::LengthMismatch {
                first: first.len(),
                second: second.len(),
            });
        }
        if first.is_empty() {
            return Err(StatisticsError::EmptySamples);
        }
        if first.iter().chain(second).any(Duration::is_zero) {
            return Err(StatisticsError::ZeroDuration);
        }
        let mut ratios = first
            .iter()
            .zip(second)
            .map(|(first, second)| first.as_secs_f64() / second.as_secs_f64())
            .collect::<Vec<_>>();
        ratios.sort_by(f64::total_cmp);
        Ok(Self {
            samples: first.len(),
            median_first_over_second: ratios[ratios.len() / 2],
            second_wins: first
                .iter()
                .zip(second)
                .filter(|(first, second)| second < first)
                .count(),
        })
    }

    /// Returns the number of paired samples.
    #[must_use]
    pub const fn samples(self) -> usize {
        self.samples
    }

    /// Returns the median of the pairwise `first / second` ratios.
    #[must_use]
    pub const fn median_first_over_second(self) -> f64 {
        self.median_first_over_second
    }

    /// Returns the number of pairs won by the second variant.
    #[must_use]
    pub const fn second_wins(self) -> usize {
        self.second_wins
    }
}

/// Computes a nearest-rank percentile from duration samples.
///
/// `percentile` must be in `1..=100`; the input need not be sorted and is not
/// mutated.
///
/// # Errors
///
/// Returns [`StatisticsError::EmptySamples`] for no samples and
/// [`StatisticsError::InvalidPercentile`] outside `1..=100`.
pub fn duration_percentile(
    samples: &[Duration],
    percentile: u8,
) -> Result<Duration, StatisticsError> {
    if !(1..=100).contains(&percentile) {
        return Err(StatisticsError::InvalidPercentile(percentile));
    }
    let sorted = sorted(samples)?;
    Ok(percentile_of_sorted(&sorted, percentile))
}

/// Describes invalid benchmark sample statistics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsError {
    /// At least one duration sample is required.
    EmptySamples,
    /// Percentiles must be in `1..=100`.
    InvalidPercentile(u8),
    /// Paired sample slices must have equal lengths.
    LengthMismatch { first: usize, second: usize },
    /// Paired ratios are undefined for zero durations.
    ZeroDuration,
}

impl fmt::Display for StatisticsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySamples => formatter.write_str("benchmark samples must not be empty"),
            Self::InvalidPercentile(percentile) => {
                write!(formatter, "percentile must be in 1..=100, got {percentile}")
            }
            Self::LengthMismatch { first, second } => write!(
                formatter,
                "paired sample lengths differ: first={first}, second={second}"
            ),
            Self::ZeroDuration => {
                formatter.write_str("paired benchmark durations must be non-zero")
            }
        }
    }
}

impl std::error::Error for StatisticsError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DurationNanos(Duration);

impl Serialize for DurationNanos {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u128(self.0.as_nanos())
    }
}

fn sorted(samples: &[Duration]) -> Result<Vec<Duration>, StatisticsError> {
    if samples.is_empty() {
        return Err(StatisticsError::EmptySamples);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Ok(sorted)
}

fn percentile_of_sorted(sorted: &[Duration], percentile: u8) -> Duration {
    sorted[nearest_rank(sorted.len(), percentile)]
}

fn nearest_rank(length: usize, percentile: u8) -> usize {
    let percentile = usize::from(percentile);
    let rank = (length / 100) * percentile + ((length % 100) * percentile).div_ceil(100);
    rank.saturating_sub(1)
}
