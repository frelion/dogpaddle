use std::collections::BTreeMap;

use crate::{Artifact, PairSide};

pub(crate) fn print(artifact: &Artifact) {
    println!();
    println!("=== {} raw-sample summary ===", artifact.benchmark());
    println!(
        "{:<72} {:>7} {:>12} {:>12} {:>12} {:>14}",
        "series", "samples", "min", "median", "p95", "operations/s"
    );
    for (case, samples) in artifact.cases() {
        let mut elapsed = samples
            .iter()
            .map(super::Sample::elapsed_ns)
            .collect::<Vec<_>>();
        elapsed.sort_unstable();
        let median = elapsed[elapsed.len() / 2];
        let p95 = percentile(&elapsed, 95);
        let operations = case
            .fields()
            .get("operations")
            .and_then(serde_json::Value::as_u64)
            .map_or_else(
                || "-".to_owned(),
                |operations| {
                    (u128::from(operations) * 1_000_000_000 / u128::from(median.max(1))).to_string()
                },
            );
        println!(
            "{:<72} {:>7} {:>12} {:>12} {:>12} {:>14}",
            case.series(),
            elapsed.len(),
            duration(elapsed[0]),
            duration(median),
            duration(p95),
            operations,
        );
        let mut latencies = samples
            .iter()
            .filter_map(|sample| sample.fields().get("round_latencies_ns"))
            .filter_map(serde_json::Value::as_array)
            .flatten()
            .filter_map(serde_json::Value::as_u64)
            .collect::<Vec<_>>();
        if !latencies.is_empty() {
            latencies.sort_unstable();
            println!(
                "  round latency: p50={} p95={} p99={} max={}",
                duration(percentile(&latencies, 50)),
                duration(percentile(&latencies, 95)),
                duration(percentile(&latencies, 99)),
                duration(*latencies.last().expect("non-empty latencies")),
            );
        }
        let work_rates = [
            ("advances", "advances/s"),
            ("committed_station_turns", "committed_station_turns/s"),
            ("input_completions", "input_completions/s"),
        ]
        .into_iter()
        .filter_map(|(field, label)| {
            case.fields()
                .get(field)
                .and_then(serde_json::Value::as_u64)
                .map(|work| format!("{label}={}", aggregate_rate(work, elapsed.len(), &elapsed)))
        })
        .collect::<Vec<_>>();
        if !work_rates.is_empty() {
            println!("  work throughput: {}", work_rates.join(" "));
        }
    }
    print_pairs(artifact);
    for (spec, values) in artifact.observations() {
        let last = values.last().map_or_else(
            || "{}".to_owned(),
            |value| serde_json::to_string(value.fields()).expect("serialize observation"),
        );
        println!(
            "observation {}: records={} last={last}",
            spec.series(),
            values.len()
        );
    }
}

fn print_pairs(artifact: &Artifact) {
    let mut pairs = BTreeMap::<&str, [Option<Vec<u64>>; 2]>::new();
    for (case, samples) in artifact.cases() {
        let Some(pairing) = case.pairing() else {
            continue;
        };
        let side = match pairing.side() {
            PairSide::First => 0,
            PairSide::Second => 1,
        };
        pairs.entry(pairing.pair()).or_default()[side] =
            Some(samples.iter().map(super::Sample::elapsed_ns).collect());
    }
    for (pair, sides) in pairs {
        let [Some(first), Some(second)] = sides else {
            unreachable!("validated pair has two sides")
        };
        let mut ratios = first
            .iter()
            .zip(&second)
            .map(|(first, second)| {
                std::time::Duration::from_nanos(*first).as_secs_f64()
                    / std::time::Duration::from_nanos((*second).max(1)).as_secs_f64()
            })
            .collect::<Vec<_>>();
        ratios.sort_by(f64::total_cmp);
        let second_wins = first
            .iter()
            .zip(&second)
            .filter(|(first, second)| second < first)
            .count();
        println!(
            "pair {pair}: median first/second={:.3}x second_wins={second_wins}/{}",
            ratios[ratios.len() / 2],
            ratios.len()
        );
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn duration(nanos: u64) -> String {
    let value = std::time::Duration::from_nanos(nanos);
    if nanos >= 1_000_000_000 {
        format!("{:.3}s", value.as_secs_f64())
    } else if nanos >= 1_000_000 {
        format!("{:.3}ms", value.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", value.as_secs_f64() * 1_000_000.0)
    }
}

fn aggregate_rate(work_per_sample: u64, samples: usize, elapsed_ns: &[u64]) -> u128 {
    let work = u128::from(work_per_sample)
        * u128::try_from(samples).expect("benchmark sample count fits u128");
    let elapsed = elapsed_ns
        .iter()
        .map(|value| u128::from(*value))
        .sum::<u128>();
    work * 1_000_000_000 / elapsed.max(1)
}
