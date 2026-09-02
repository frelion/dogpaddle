use std::time::Duration;

/// Min/median/max statistics for ordinary benchmark samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationSummary {
    min: Duration,
    median: Duration,
    max: Duration,
}

impl DurationSummary {
    /// Computes a summary from a non-empty set of duration samples.
    ///
    /// For an even sample count, the median is the upper middle element. This
    /// preserves the established ordinary benchmark summary convention;
    /// endurance p50 remains nearest-rank via [`LatencySummary`].
    ///
    /// # Panics
    ///
    /// Panics when `samples` is empty.
    #[must_use]
    #[track_caller]
    pub fn from_samples(samples: &[Duration]) -> Self {
        let sorted = sorted("duration_summary", samples);
        Self {
            min: sorted[0],
            median: sorted[sorted.len() / 2],
            max: sorted[sorted.len() - 1],
        }
    }

    /// Returns the minimum duration.
    #[must_use]
    pub const fn min(self) -> Duration {
        self.min
    }

    /// Returns the median, using the upper middle element for an even count.
    #[must_use]
    pub const fn median(self) -> Duration {
        self.median
    }

    /// Returns the maximum duration.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }
}

/// P50/p95/p99/max latency statistics for endurance protocols.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatencySummary {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

impl LatencySummary {
    /// Computes nearest-rank endurance percentiles from non-empty samples.
    ///
    /// # Panics
    ///
    /// Panics when `samples` is empty.
    #[must_use]
    #[track_caller]
    pub fn from_samples(samples: &[Duration]) -> Self {
        let sorted = sorted("latency_summary", samples);
        Self {
            p50: percentile_of_sorted(&sorted, 50),
            p95: percentile_of_sorted(&sorted, 95),
            p99: percentile_of_sorted(&sorted, 99),
            max: sorted[sorted.len() - 1],
        }
    }

    /// Returns the nearest-rank p50 latency.
    #[must_use]
    pub const fn p50(self) -> Duration {
        self.p50
    }

    /// Returns the nearest-rank p95 latency.
    #[must_use]
    pub const fn p95(self) -> Duration {
        self.p95
    }

    /// Returns the nearest-rank p99 latency.
    #[must_use]
    pub const fn p99(self) -> Duration {
        self.p99
    }

    /// Returns the maximum observed latency.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }
}

/// Statistics derived from paired first/second benchmark samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PairedDurationSummary {
    median_first_over_second: f64,
    second_wins: usize,
}

impl PairedDurationSummary {
    /// Computes a median paired ratio and win count.
    ///
    /// Ratios are computed pairwise as `first / second`; the win count is the
    /// number of pairs where the second duration is strictly smaller.
    ///
    /// # Panics
    ///
    /// Panics when either slice is empty, lengths differ, or any duration is
    /// zero.
    #[must_use]
    #[track_caller]
    pub fn from_pairs(first: &[Duration], second: &[Duration]) -> Self {
        assert!(
            first.len() == second.len(),
            "benchmark statistics failure: stage=validate_pairs label=length value=first:{},second:{} source=paired sample lengths differ",
            first.len(),
            second.len()
        );
        assert!(
            !first.is_empty(),
            "benchmark statistics failure: stage=validate_pairs label=samples value=0 source=paired samples must not be empty"
        );
        assert!(
            !first.iter().chain(second).any(Duration::is_zero),
            "benchmark statistics failure: stage=validate_pairs label=duration value=0ns source=paired durations must be non-zero"
        );
        let mut ratios = first
            .iter()
            .zip(second)
            .map(|(first, second)| first.as_secs_f64() / second.as_secs_f64())
            .collect::<Vec<_>>();
        ratios.sort_by(f64::total_cmp);
        Self {
            median_first_over_second: ratios[ratios.len() / 2],
            second_wins: first
                .iter()
                .zip(second)
                .filter(|(first, second)| second < first)
                .count(),
        }
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

#[track_caller]
fn sorted(stage: &'static str, samples: &[Duration]) -> Vec<Duration> {
    assert!(
        !samples.is_empty(),
        "benchmark statistics failure: stage={stage} label=samples value=0 source=samples must not be empty"
    );
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted
}

fn percentile_of_sorted(sorted: &[Duration], percentile: u8) -> Duration {
    sorted[nearest_rank(sorted.len(), percentile)]
}

fn nearest_rank(length: usize, percentile: u8) -> usize {
    let percentile = usize::from(percentile);
    let rank = (length / 100) * percentile + ((length % 100) * percentile).div_ceil(100);
    rank.saturating_sub(1)
}
