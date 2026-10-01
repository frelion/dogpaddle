use std::{fmt, marker::PhantomData};

use dogpaddle_operation::OperationDefinition;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, DeserializeSeed, SeqAccess, Visitor},
};

use super::validate::MAX_OPERATIONS;

/// The sole durable graph. Fusion never changes these identities or edges.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FlowDefinition {
    pub(crate) owner_identity: Option<[u8; 32]>,
    #[serde(deserialize_with = "bounded_vec")]
    pub(crate) operations: Vec<OperationNode>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperationNode {
    pub(crate) id: String,
    pub(crate) definition: OperationDefinition,
    #[serde(deserialize_with = "bounded_vec")]
    pub(crate) inputs: Vec<usize>,
}

fn bounded_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Bounded<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array with at most 1024 elements")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            for _ in 0..MAX_OPERATIONS {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            let _ = sequence.next_element_seed(RejectElement)?;
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Bounded(PhantomData))
}

// At the limit, SeqAccess may consume the closing bracket, but no next value.
struct RejectElement;
impl<'de> DeserializeSeed<'de> for RejectElement {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
        Err(de::Error::custom("flow array exceeds 1024 elements"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_limit_rejects_the_next_value_before_decoding_its_payload() {
        let node = r#"{"id":"s","definition":{"sequence_scan":{"start":0}},"inputs":[]}"#;
        let prefix = std::iter::repeat_n(node, MAX_OPERATIONS)
            .collect::<Vec<_>>()
            .join(",");
        let valid = format!(r#"{{"owner_identity":null,"operations":[{prefix}]}}"#);
        let plan: FlowDefinition = serde_json::from_str(&valid).unwrap();
        assert_eq!(plan.operations.len(), MAX_OPERATIONS);
        // The extra node is not even syntactically complete: its T is never decoded.
        let invalid = format!(r#"{{"owner_identity":null,"operations":[{prefix},{{"definition":"#);
        let error = serde_json::from_str::<FlowDefinition>(&invalid).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("flow array exceeds 1024 elements")
        );
    }

    #[test]
    fn port_limit_rejects_the_next_value_before_decoding_it() {
        let prefix = std::iter::repeat_n("0", MAX_OPERATIONS)
            .collect::<Vec<_>>()
            .join(",");
        let valid = format!(
            r#"{{"id":"s","definition":{{"sequence_scan":{{"start":0}}}},"inputs":[{prefix}]}}"#
        );
        assert_eq!(
            serde_json::from_str::<OperationNode>(&valid)
                .unwrap()
                .inputs
                .len(),
            MAX_OPERATIONS
        );
        let invalid = format!(
            r#"{{"id":"s","definition":{{"sequence_scan":{{"start":0}}}},"inputs":[{prefix},{{"#
        );
        let error = serde_json::from_str::<OperationNode>(&invalid).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("flow array exceeds 1024 elements")
        );
    }
}
