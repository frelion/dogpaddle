use std::time::Duration;

use dogpaddle_bench_protocol::{
    DurationSummary, Fields, PairOrder, PairSchedule, PairSummaryRecord, PairVariant,
    PairedDurationSummary, SampleRecord, SummaryRecord, measure_pair_with,
};

use crate::{
    BENCHMARK,
    support::{average_duration, format_duration, write_record},
};

pub(super) struct BenchmarkCase {
    workload: String,
    operations: usize,
    transactions: usize,
    logical_bytes: usize,
}

impl BenchmarkCase {
    pub(super) fn per_operation(
        workload: impl Into<String>,
        operations: usize,
        transactions: usize,
        bytes_per_operation: usize,
    ) -> Self {
        Self {
            workload: workload.into(),
            operations,
            transactions,
            logical_bytes: operations
                .checked_mul(bytes_per_operation)
                .expect("benchmark logical byte count fits in usize"),
        }
    }
}

struct PairedSamples {
    first: Vec<Duration>,
    second: Vec<Duration>,
}

impl PairedSamples {
    fn collect(samples: usize, mut measure: impl FnMut(bool) -> Duration) -> Self {
        let mut measure_variant = |variant| measure(matches!(variant, PairVariant::Second));
        let _ = measure_pair_with(PairOrder::Ab, &mut measure_variant);
        let mut first = Vec::with_capacity(samples);
        let mut second = Vec::with_capacity(samples);
        for sample in 0..samples {
            let pair = measure_pair_with(
                PairSchedule::Alternating.order(sample),
                &mut measure_variant,
            );
            first.push(pair.first);
            second.push(pair.second);
        }
        Self { first, second }
    }

    fn summary(&self) -> PairedDurationSummary {
        PairedDurationSummary::from_pairs(&self.first, &self.second)
            .expect("summarize paired OrderedMap measurements")
    }

    fn first_wins(&self) -> usize {
        self.first
            .iter()
            .zip(&self.second)
            .filter(|(first, second)| first < second)
            .count()
    }
}

pub(super) fn report_size_pair(
    case: &BenchmarkCase,
    samples: usize,
    mut small: impl FnMut() -> Duration,
    mut large: impl FnMut() -> Duration,
) {
    let measurements =
        PairedSamples::collect(samples, |second| if second { large() } else { small() });
    emit_pair(case, "Small", "Large", &measurements);
    println!(
        "  paired Small/Large median={:.3}x; Small wins {}/{samples}",
        measurements.summary().median_first_over_second(),
        measurements.first_wins()
    );
}

pub(super) fn report_mode_pair<T>(
    case: &BenchmarkCase,
    samples: usize,
    fixture: &mut T,
    mut full: impl FnMut(&mut T) -> Duration,
    mut projected: impl FnMut(&mut T) -> Duration,
) {
    let measurements = PairedSamples::collect(samples, |second| {
        if second {
            projected(fixture)
        } else {
            full(fixture)
        }
    });
    emit_pair(case, "Full", "Projected", &measurements);
    println!(
        "  paired Full/Projected median={:.3}x; projection wins {}/{samples}",
        measurements.summary().median_first_over_second(),
        measurements.summary().second_wins()
    );
}

fn emit_pair(
    case: &BenchmarkCase,
    first_label: &str,
    second_label: &str,
    measurements: &PairedSamples,
) {
    let record = PairSummaryRecord::new(
        BENCHMARK,
        &case.workload,
        first_label,
        second_label,
        measurements.summary(),
        Fields::new(),
    )
    .expect("construct OrderedMap pair summary record");
    write_record(&record);
    report(case, first_label, &measurements.first);
    report(case, second_label, &measurements.second);
}

fn report(case: &BenchmarkCase, variant: &str, durations: &[Duration]) {
    for (sample, elapsed) in durations.iter().copied().enumerate() {
        let record = SampleRecord::new(
            BENCHMARK,
            &case.workload,
            sample,
            elapsed,
            measurement_fields(case, variant),
        )
        .expect("construct OrderedMap sample record");
        write_record(&record);
    }
    let summary =
        DurationSummary::from_samples(durations).expect("summarize OrderedMap measurements");
    let record = SummaryRecord::new(
        BENCHMARK,
        &case.workload,
        summary,
        measurement_fields(case, variant),
    )
    .expect("construct OrderedMap summary record");
    write_record(&record);

    let rate = case.operations as u128 * 1_000_000_000 / summary.median().as_nanos();
    let median_per_operation = average_duration(summary.median(), case.operations);
    println!(
        "{:<28} {variant:<10} {:>12} {:>12} {:>12} {:>12} {median_per_operation:>12} {rate:>14}",
        case.workload,
        case.operations,
        format_duration(summary.min()),
        format_duration(summary.median()),
        format_duration(summary.max()),
    );
}

fn measurement_fields(case: &BenchmarkCase, variant: &str) -> Fields {
    let mut fields = Fields::new();
    fields
        .insert("variant", variant)
        .expect("construct OrderedMap variant field");
    for (name, value) in [
        ("operations", case.operations),
        ("transactions", case.transactions),
        ("logical_bytes", case.logical_bytes),
    ] {
        fields
            .insert(name, value)
            .expect("construct OrderedMap work fields");
    }
    fields
}

pub(super) fn print_section(name: &str, description: &str) {
    println!();
    println!("=== {name} ===");
    println!("{description}");
    println!(
        "{:<28} {:<10} {:>12} {:>12} {:>12} {:>12} {:>12} {:>14}",
        "workload", "data", "operations", "min", "median", "max", "median/op", "median ops/s"
    );
}

pub(super) fn print_group(description: &str) {
    println!();
    println!("-- {description} --");
}
