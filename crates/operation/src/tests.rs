use crate::OperationDefinition;
use crate::operation::transform::RunningEventCountDefinition;

#[test]
fn json_plan_round_trips_the_stable_variant() {
    let definition: OperationDefinition = RunningEventCountDefinition::new().into();
    let json = serde_json::to_string(&definition).unwrap();
    assert_eq!(json, r#"{"running_event_count":{}}"#);
    let plan: OperationDefinition = serde_json::from_str(&json).unwrap();
    let bytes = serde_json::to_vec::<crate::OperationDefinition>(&plan).unwrap();
    assert_eq!(
        serde_json::to_string(
            &serde_json::from_slice::<crate::OperationDefinition>(&bytes).unwrap()
        )
        .unwrap(),
        json
    );
}
