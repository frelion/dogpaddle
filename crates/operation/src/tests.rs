use crate::operation::transform::RunningEventCountDefinition;
use crate::{OperationDefinition, decode_definition, encode_definition};

#[test]
fn json_plan_and_persistent_definition_share_the_same_variant() {
    let definition: OperationDefinition = RunningEventCountDefinition::new().into();
    let json = serde_json::to_string(&definition).unwrap();
    assert_eq!(json, r#"{"running_event_count":{}}"#);
    let plan: OperationDefinition = serde_json::from_str(&json).unwrap();
    let bytes = encode_definition(&plan);
    assert_eq!(
        serde_json::to_string(&decode_definition(&bytes).unwrap()).unwrap(),
        json
    );
}
