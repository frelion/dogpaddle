use std::time::Duration;

use dogpaddle_bench_protocol::{
    DurationSummary, Fields, PairOrder, PairSchedule, PairSummaryRecord, PairVariant,
    PairedDurationSummary, SampleRecord, SummaryRecord, measure_pair_with,
};

use crate::{
    BENCHMARK, MEBIBYTE_BYTES,
    support::{average_duration, format_duration, write_record},
};

pub(super) struct LogCase {
    workload: String,
    records: usize,
    record_bytes: usize,
    transactions: usize,
}

impl LogCase {
    pub(super) fn new(
        workload: impl Into<String>,
        records: usize,
        record_bytes: usize,
        transactions: usize,
    ) -> Self {
        Self {
            workload: workload.into(),
            records,
            record_bytes,
            transactions,
        }
    }

    fn fields(&self, variant: &str) -> Fields {
        let mut fields = Fields::new();
        fields
            .insert("variant", variant)
            .expect("construct AppendLog variant field");
        for (name, value) in [
            ("operations", self.records),
            ("transactions", self.transactions),
            (
                "logical_bytes",
                self.records
                    .checked_mul(self.record_bytes)
                    .expect("benchmark logical byte count fits in usize"),
            ),
        ] {
            fields
                .insert(name, value)
                .expect("construct AppendLog work fields");
        }
        fields
    }
}

pub(super) struct LogPair {
    scenario: String,
    first: LogCase,
    second_workload: String,
    first_data: &'static str,
    second_data: &'static str,
}

impl LogPair {
    pub(super) fn variants(
        scenario: impl Into<String>,
        first: LogCase,
        second_workload: impl Into<String>,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            first,
            second_workload: second_workload.into(),
            first_data: "first",
            second_data: "second",
        }
    }

    pub(super) fn modes(
        scenario: impl Into<String>,
        first: LogCase,
        second_workload: impl Into<String>,
    ) -> Self {
        Self {
            scenario: scenario.into(),
            first,
            second_workload: second_workload.into(),
            first_data: "default",
            second_data: "default",
        }
    }

    fn second(&self) -> LogCase {
        LogCase {
            workload: self.second_workload.clone(),
            records: self.first.records,
            record_bytes: self.first.record_bytes,
            transactions: self.first.transactions,
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
                PairSchedule::Counterbalanced.order(sample),
                &mut measure_variant,
            );
            first.push(pair.first);
            second.push(pair.second);
        }
        Self { first, second }
    }

    fn summary(&self) -> PairedDurationSummary {
        PairedDurationSummary::from_pairs(&self.first, &self.second)
            .expect("summarize paired AppendLog measurements")
    }
}

pub(super) fn report_log(case: &LogCase, samples: usize, mut measure: impl FnMut() -> Duration) {
    measure();
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        durations.push(measure());
    }
    report_measurements(case, "default", &durations);
}

pub(super) fn report_log_pair(
    pair: &LogPair,
    samples: usize,
    mut first: impl FnMut() -> Duration,
    mut second: impl FnMut() -> Duration,
) {
    let measurements = PairedSamples::collect(
        samples,
        |is_second| {
            if is_second { second() } else { first() }
        },
    );
    report_pair_measurements(pair, &measurements, samples);
}

pub(super) fn report_log_mode_pair(
    pair: &LogPair,
    samples: usize,
    measure: impl FnMut(bool) -> Duration,
) {
    let measurements = PairedSamples::collect(samples, measure);
    report_pair_measurements(pair, &measurements, samples);
}

fn report_pair_measurements(pair: &LogPair, measurements: &PairedSamples, samples: usize) {
    let second = pair.second();
    report_measurements(&pair.first, pair.first_data, &measurements.first);
    report_measurements(&second, pair.second_data, &measurements.second);
    let summary = measurements.summary();
    let record = PairSummaryRecord::new(
        BENCHMARK,
        &pair.scenario,
        &pair.first.workload,
        &second.workload,
        summary,
        Fields::new(),
    )
    .expect("construct AppendLog pair summary record");
    write_record(&record);
    println!(
        "  paired first/second median={:.3}x; second wins {}/{samples}",
        summary.median_first_over_second(),
        summary.second_wins()
    );
}

fn report_measurements(case: &LogCase, variant: &str, durations: &[Duration]) {
    for (sample, elapsed) in durations.iter().copied().enumerate() {
        let record = SampleRecord::new(
            BENCHMARK,
            &case.workload,
            sample,
            elapsed,
            case.fields(variant),
        )
        .expect("construct AppendLog sample record");
        write_record(&record);
    }
    let summary =
        DurationSummary::from_samples(durations).expect("summarize AppendLog measurements");
    let record = SummaryRecord::new(BENCHMARK, &case.workload, summary, case.fields(variant))
        .expect("construct AppendLog summary record");
    write_record(&record);

    let records_per_second = case.records as u128 * 1_000_000_000 / summary.median().as_nanos();
    let median_per_record = average_duration(summary.median(), case.records);
    let encoded_mib_tenths_per_second =
        case.records as u128 * case.record_bytes as u128 * 10 * 1_000_000_000
            / summary.median().as_nanos()
            / MEBIBYTE_BYTES;
    let encoded_mib_per_second = format!(
        "{}.{:01}",
        encoded_mib_tenths_per_second / 10,
        encoded_mib_tenths_per_second % 10
    );
    println!(
        "{:<45} {:>9} {:>11} {:>12} {:>12} {:>12} {median_per_record:>12} {records_per_second:>13} {encoded_mib_per_second:>13}",
        case.workload,
        case.record_bytes,
        case.records,
        format_duration(summary.min()),
        format_duration(summary.median()),
        format_duration(summary.max()),
    );
}

pub(super) fn print_log_section(name: &str, description: &str) {
    println!();
    println!("=== {name} ===");
    println!("{description}");
    println!(
        "{:<45} {:>9} {:>11} {:>12} {:>12} {:>12} {:>12} {:>13} {:>13}",
        "workload",
        "record B",
        "records",
        "min",
        "median",
        "max",
        "median/item",
        "records/s",
        "encoded MiB/s"
    );
}
