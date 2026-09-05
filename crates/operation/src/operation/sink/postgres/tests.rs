use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use super::{
    super::relation::{Continuation, MAX_TECHNICAL_ID, Mutation, MutationKind, Position},
    PostgresSinkConfig, PostgresTargetSpec,
    row::{PostgresRowCodec, PostgresValue},
    schema::PostgresLayout,
    state::{PostgresSinkState, PostgresSinkStateCodecError},
    target::{SqlPlan, quote_identifier},
};

fn spec(table: &str) -> PostgresTargetSpec {
    PostgresTargetSpec::try_new("sink_1", "database", "Target Schema", table, "1", 2).unwrap()
}

#[test]
fn runtime_config_debug_redacts_the_password() {
    let config = PostgresSinkConfig::new_unencrypted(
        "localhost",
        5432,
        "database",
        "writer",
        "visible-secret",
    )
    .unwrap();
    let debug = format!("{config:?}");

    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("visible-secret"));
}

#[test]
fn identifiers_are_quoted_as_independent_postgresql_components() {
    assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "odd\"column",
        DataType::Int64,
        false,
    )]));
    let layout = PostgresLayout::try_new(schema).unwrap();
    let plan = SqlPlan::new(&spec("odd.table"), &layout);

    assert!(plan.initialize.contains("\"Target Schema\".\"odd.table\""));
    assert!(
        plan.initialize
            .contains("CREATE INDEX \"$dogpaddle.hash.sink_1\" ON")
    );
    assert!(
        !plan
            .initialize
            .contains("CREATE INDEX \"Target Schema\".\"$dogpaddle.hash.sink_1\"")
    );
    assert!(plan.insert_row.contains("\"odd\"\"column\""));
    assert!(!plan.initialize.contains("\"Target Schema.odd.table\""));
}

#[test]
fn maximum_sink_identity_keeps_derived_names_below_postgresql_limit() {
    let sink_id = "a".repeat(32);
    let spec = PostgresTargetSpec::try_new(sink_id, "database", "schema", "table", "1", 2).unwrap();

    assert!(spec.object_names().iter().all(|name| name.len() <= 63));
}

#[test]
fn row_codec_preserves_unsigned_and_float_bit_patterns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("float32", DataType::Float32, false),
        Field::new("float64", DataType::Float64, false),
    ]));
    let float32 = f32::from_bits(0x7f80_0123);
    let float64 = -0.0_f64;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
            Arc::new(Float32Array::from(vec![float32])) as ArrayRef,
            Arc::new(Float64Array::from(vec![float64])) as ArrayRef,
        ],
    )
    .unwrap();
    let encoded = PostgresRowCodec::new(PostgresLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();

    assert_eq!(
        encoded.values,
        [
            PostgresValue::Bytes(Some(u64::MAX.to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(float32.to_bits().to_be_bytes().to_vec())),
            PostgresValue::Bytes(Some(float64.to_bits().to_be_bytes().to_vec())),
        ]
    );
}

#[test]
fn matching_and_delete_use_distinct_correct_parameter_offsets() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let layout = PostgresLayout::try_new(schema).unwrap();
    let plan = SqlPlan::new(&spec("target"), &layout);

    assert!(
        plan.select_matching
            .contains("\"a\" IS NOT DISTINCT FROM $2")
    );
    assert!(
        plan.select_matching
            .contains("\"b\" IS NOT DISTINCT FROM $3")
    );
    assert!(plan.select_matching.ends_with("LIMIT $4"));
    assert!(plan.delete_exact.contains("\"a\" IS NOT DISTINCT FROM $3"));
    assert!(plan.delete_exact.contains("\"b\" IS NOT DISTINCT FROM $4"));
}

#[test]
fn row_codec_preserves_the_complete_utf8_domain_as_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["before\0after"])) as ArrayRef],
    )
    .unwrap();
    let encoded = PostgresRowCodec::new(PostgresLayout::try_new(schema).unwrap())
        .encode_row(&batch, 0)
        .unwrap();

    assert_eq!(
        encoded.values,
        [PostgresValue::Bytes(Some(b"before\0after".to_vec()))]
    );
}

#[test]
fn sink_state_v1_golden_bytes_round_trip() {
    let cases = [
        (PostgresSinkState::Initialize, vec![0, 1, 0]),
        (
            PostgresSinkState::Ready {
                next_delivery: 2,
                next_id: 3,
                position: None,
            },
            vec![
                0, 1, 1, // version and Ready
                0, 0, 0, 0, 0, 0, 0, 2, // next delivery
                0, 0, 0, 0, 0, 0, 0, 3, // next technical ID
                0, // no retained-Change position
            ],
        ),
        (
            PostgresSinkState::Ready {
                next_delivery: 4,
                next_id: 5,
                position: Some(Position {
                    row_index: 6,
                    remaining: 7,
                }),
            },
            vec![
                0, 1, 1, // version and Ready
                0, 0, 0, 0, 0, 0, 0, 4, // next delivery
                0, 0, 0, 0, 0, 0, 0, 5, // next technical ID
                1, // position follows
                0, 0, 0, 0, 0, 0, 0, 6, // row
                0, 0, 0, 0, 0, 0, 0, 7, // remaining
            ],
        ),
        (
            prepared_state(),
            vec![
                0, 1, 2, // version and Prepared
                0, 0, 0, 0, 0, 0, 0, 7, // delivery
                0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, // digest
                0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
                0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0, 0, 0, 0, 0, 0, 0,
                9, // next technical ID before
                0, 0, 0, 0, 0, 0, 0, 2, // start row
                0, 0, 0, 0, 0, 0, 0, 2, // start remaining
                0, // Done
                0, 2, // mutation count
                0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 9, // insert
                0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 10, // insert
            ],
        ),
    ];

    for (state, golden) in cases {
        assert_eq!(state.encode().unwrap(), golden);
        assert_eq!(PostgresSinkState::decode(&golden).unwrap(), state);
    }
}

#[test]
fn every_truncated_prepared_state_prefix_is_rejected() {
    let encoded = prepared_state().encode().unwrap();

    for length in 0..encoded.len() {
        assert_eq!(
            PostgresSinkState::decode(&encoded[..length]),
            Err(PostgresSinkStateCodecError::Truncated),
            "accepted prefix of length {length}"
        );
    }
}

#[test]
fn sink_state_decoder_rejects_unknown_tags_versions_and_trailing_bytes() {
    assert_eq!(
        PostgresSinkState::decode(&[0, 2, 0]),
        Err(PostgresSinkStateCodecError::UnsupportedVersion(2))
    );
    assert_eq!(
        PostgresSinkState::decode(&[0, 1, 3]),
        Err(PostgresSinkStateCodecError::UnknownStateTag(3))
    );
    assert_eq!(
        PostgresSinkState::decode(&[0, 1, 0, 0]),
        Err(PostgresSinkStateCodecError::TrailingBytes)
    );

    let mut ready = PostgresSinkState::Ready {
        next_delivery: 1,
        next_id: 1,
        position: None,
    }
    .encode()
    .unwrap();
    *ready.last_mut().unwrap() = 2;
    assert_eq!(
        PostgresSinkState::decode(&ready),
        Err(PostgresSinkStateCodecError::UnknownPositionTag(2))
    );

    let mut prepared = prepared_state().encode().unwrap();
    prepared[67] = 2;
    assert_eq!(
        PostgresSinkState::decode(&prepared),
        Err(PostgresSinkStateCodecError::UnknownContinuationTag(2))
    );
    let mut prepared = prepared_state().encode().unwrap();
    prepared[70] = 2;
    assert_eq!(
        PostgresSinkState::decode(&prepared),
        Err(PostgresSinkStateCodecError::UnknownMutationKindTag(2))
    );
}

#[test]
fn sink_state_rejects_invalid_frontiers_mutations_and_continuations() {
    let invalid = [
        (
            PostgresSinkState::Ready {
                next_delivery: 0,
                next_id: 1,
                position: None,
            },
            PostgresSinkStateCodecError::InvalidNextDelivery(0),
        ),
        (
            PostgresSinkState::Ready {
                next_delivery: 1,
                next_id: MAX_TECHNICAL_ID + 2,
                position: None,
            },
            PostgresSinkStateCodecError::InvalidNextId(MAX_TECHNICAL_ID + 2),
        ),
        (
            prepared_with_delivery(
                0,
                9,
                Position {
                    row_index: 2,
                    remaining: 2,
                },
                Continuation::Done,
                vec![insert(2, 9), insert(2, 10)],
            ),
            PostgresSinkStateCodecError::InvalidDelivery(0),
        ),
        (
            prepared_with(
                0,
                Position {
                    row_index: 2,
                    remaining: 1,
                },
                Continuation::Done,
                vec![insert(2, 1)],
            ),
            PostgresSinkStateCodecError::InvalidNextId(0),
        ),
        (
            prepared_with(
                9,
                Position {
                    row_index: 2,
                    remaining: 2,
                },
                Continuation::Done,
                vec![insert(2, 10), insert(2, 11)],
            ),
            PostgresSinkStateCodecError::InsertRangeStartMismatch,
        ),
        (
            prepared_with(
                9,
                Position {
                    row_index: 2,
                    remaining: 1,
                },
                Continuation::Done,
                vec![delete(2, 9), insert(3, 9)],
            ),
            PostgresSinkStateCodecError::DeleteBeforeInsert,
        ),
        (
            prepared_with(
                9,
                Position {
                    row_index: 2,
                    remaining: 2,
                },
                Continuation::Done,
                vec![insert(2, 9)],
            ),
            PostgresSinkStateCodecError::InvalidContinuation,
        ),
        (
            prepared_with(
                9,
                Position {
                    row_index: 2,
                    remaining: 1,
                },
                Continuation::Done,
                vec![delete(2, 1), delete(3, 1)],
            ),
            PostgresSinkStateCodecError::DuplicateDeleteId,
        ),
    ];

    for (state, expected) in invalid {
        assert_eq!(state.validate(), Err(expected));
    }
}

fn prepared_state() -> PostgresSinkState {
    prepared_with(
        9,
        Position {
            row_index: 2,
            remaining: 2,
        },
        Continuation::Done,
        vec![insert(2, 9), insert(2, 10)],
    )
}

fn prepared_with(
    next_id_before: u64,
    start_position: Position,
    continuation: Continuation,
    mutations: Vec<Mutation>,
) -> PostgresSinkState {
    prepared_with_delivery(7, next_id_before, start_position, continuation, mutations)
}

fn prepared_with_delivery(
    delivery: u64,
    next_id_before: u64,
    start_position: Position,
    continuation: Continuation,
    mutations: Vec<Mutation>,
) -> PostgresSinkState {
    PostgresSinkState::Prepared {
        delivery,
        digest: [0xab; 32],
        next_id_before,
        start_position,
        continuation,
        mutations,
    }
}

const fn insert(row_index: u64, technical_id: u64) -> Mutation {
    Mutation {
        kind: MutationKind::Insert,
        row_index,
        technical_id,
    }
}

const fn delete(row_index: u64, technical_id: u64) -> Mutation {
    Mutation {
        kind: MutationKind::Delete,
        row_index,
        technical_id,
    }
}
