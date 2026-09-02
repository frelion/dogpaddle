/// The execution order of one semantically paired A/B sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairOrder {
    /// Measure A before B.
    Ab,
    /// Measure B before A.
    Ba,
}

/// The semantic variant requested by a single paired-measurement callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairVariant {
    /// Measure the first, or A, variant.
    First,
    /// Measure the second, or B, variant.
    Second,
}

/// A deterministic schedule for counterbalancing paired measurements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairSchedule {
    /// Alternates `AB, BA, AB, BA, ...`.
    Alternating,
    /// Repeats `AB, BA, BA, AB`, balancing a complete four-round block.
    Counterbalanced,
}

impl PairSchedule {
    /// Returns the execution order for `sample`.
    #[must_use]
    pub const fn order(self, sample: usize) -> PairOrder {
        match self {
            Self::Alternating => {
                if sample.is_multiple_of(2) {
                    PairOrder::Ab
                } else {
                    PairOrder::Ba
                }
            }
            Self::Counterbalanced => {
                if matches!(sample % 4, 0 | 3) {
                    PairOrder::Ab
                } else {
                    PairOrder::Ba
                }
            }
        }
    }
}

/// Results from one paired measurement, always stored in semantic A/B order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PairMeasurements<A, B> {
    /// Result produced by the A callback.
    pub first: A,
    /// Result produced by the B callback.
    pub second: B,
}

/// Executes one mutable callback for both variants in `order`.
///
/// The callback can naturally share one mutable fixture between variants
/// without interior mutability. Results are returned in semantic first/second
/// order.
pub fn measure_pair_with<T>(
    order: PairOrder,
    mut measure: impl FnMut(PairVariant) -> T,
) -> PairMeasurements<T, T> {
    match order {
        PairOrder::Ab => PairMeasurements {
            first: measure(PairVariant::First),
            second: measure(PairVariant::Second),
        },
        PairOrder::Ba => {
            let second = measure(PairVariant::Second);
            let first = measure(PairVariant::First);
            PairMeasurements { first, second }
        }
    }
}
