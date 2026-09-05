use super::*;
use crate::environment::CommandOutput;

#[test]
fn profile_spellings_are_stable() {
    assert_eq!(PerformanceProfile::Smoke.as_str(), "smoke");
    assert_eq!(PerformanceProfile::Reference.as_str(), "reference");
}

#[test]
fn unavailable_host_probes_remain_serializable_data() {
    let missing = CommandOutput::capture("dogpaddle-command-that-does-not-exist", &[]);
    assert!(!missing.is_available());
    let encoded = serde_json::to_string(&missing).unwrap();
    let decoded: CommandOutput = serde_json::from_str(&encoded).unwrap();
    assert!(!decoded.is_available());
}

#[test]
fn complete_host_environment_round_trips_without_a_shared_result_schema() {
    let environment = HostEnvironment::collect(None);
    let encoded = serde_json::to_value(&environment).unwrap();
    let decoded: HostEnvironment = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
}
