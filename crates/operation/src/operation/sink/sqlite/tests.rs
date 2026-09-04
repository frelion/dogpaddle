mod row {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, NullArray,
        RecordBatch, RecordBatchOptions, StringArray, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array, types::Int64Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use rusqlite::{Connection, params_from_iter, types::Value};

    use super::super::{
        definition::SqliteSinkSchemaError,
        row::RowCodec,
        target::{column_definition, quote_identifier},
    };

    #[test]
    fn validates_names_and_builds_strict_column_definitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("", DataType::Null, false),
            Field::new("quote\"", DataType::Utf8, true),
            Field::new("unsigned", DataType::UInt64, false),
        ]));
        let codec = RowCodec::new_validated(Arc::clone(&schema));
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| quote_identifier(field.name()))
                .collect::<Vec<_>>(),
            ["\"\"", "\"quote\"\"\"", "\"unsigned\""]
        );
        let definitions = schema
            .fields()
            .iter()
            .map(|field| column_definition(field).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(definitions[0], "\"\" BLOB CHECK(\"\" IS NULL)");
        assert!(definitions[1].contains("TEXT COLLATE BINARY"));
        assert!(definitions[2].contains("length(\"unsigned\") = 8"));
        assert_eq!(codec.schema().as_ref(), schema.as_ref());
    }

    #[test]
    fn an_unmapped_future_type_returns_a_schema_error_instead_of_panicking() {
        let field = Field::new("future", DataType::LargeUtf8, false);

        assert_eq!(
            column_definition(&field),
            Err(SqliteSinkSchemaError::UnsupportedType {
                field: "future".to_owned(),
                data_type: DataType::LargeUtf8,
            })
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one auditable fixture keeps every supported v1 type in a single row golden"
    )]
    fn every_v1_type_round_trips_through_sqlite_with_exact_bits_and_nested_nulls() {
        let rows = 4;
        let decimal_max = 10_i128.pow(38) - 1;
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), None, Some(-2)]),
            None,
            Some(Vec::<Option<i64>>::new()),
            Some(vec![Some(i64::MAX)]),
        ]);
        let flag = Arc::new(Field::new("flag", DataType::Boolean, false));
        let score = Arc::new(Field::new("score", DataType::Int64, true));
        let structure = StructArray::from(vec![
            (
                Arc::clone(&flag),
                Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
            ),
            (
                Arc::clone(&score),
                Arc::new(Int64Array::from(vec![Some(7), None, Some(-9), Some(12)])) as ArrayRef,
            ),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(rows)),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(0),
                Some(i8::MAX),
            ])),
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                None,
                Some(0),
                Some(i16::MAX),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(0),
                Some(i32::MAX),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(0),
                Some(i64::MAX),
            ])),
            Arc::new(UInt8Array::from(vec![
                Some(u8::MAX),
                None,
                Some(0),
                Some(1),
            ])),
            Arc::new(UInt16Array::from(vec![
                Some(u16::MAX),
                None,
                Some(0),
                Some(1),
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(u32::MAX),
                None,
                Some(0),
                Some(1),
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(u64::MAX),
                None,
                Some(0),
                Some(1),
            ])),
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                None,
                Some(0.0),
                Some(f32::NEG_INFINITY),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                None,
                Some(f64::INFINITY),
                Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            ])),
            Arc::new(StringArray::from(vec![
                Some("utf8"),
                None,
                Some(""),
                Some("z"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0_u8, 255][..]),
                None,
                Some(&[][..]),
                Some(&[7][..]),
            ])),
            Arc::new(list),
            Arc::new(structure),
            Arc::new(Date32Array::from(vec![
                Some(i32::MIN),
                None,
                Some(0),
                Some(i32::MAX),
            ])),
            Arc::new(TimestampSecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(0),
                Some(i64::MAX),
            ])),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    Some(i64::MAX),
                    None,
                    Some(0),
                    Some(i64::MIN),
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(-1), None, Some(0), Some(1)])
                    .with_timezone("+08:00"),
            ),
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    Some(i64::MIN + 1),
                    None,
                    Some(0),
                    Some(i64::MAX - 1),
                ])
                .with_timezone("America/New_York"),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(decimal_max), None, Some(0), Some(-decimal_max)])
                    .with_precision_and_scale(38, 18)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(-9_999), None, Some(0), Some(9_999)])
                    .with_precision_and_scale(4, -2)
                    .unwrap(),
            ),
        ];
        let fields = vec![
            Field::new("null", DataType::Null, false),
            Field::new("bool", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("binary", DataType::Binary, true),
            Field::new("list", columns[14].data_type().clone(), true),
            Field::new("struct", DataType::Struct(vec![flag, score].into()), true),
            Field::new("date32", DataType::Date32, true),
            Field::new("timestamp_s", columns[17].data_type().clone(), true),
            Field::new("timestamp_ms", columns[18].data_type().clone(), true),
            Field::new("timestamp_us", columns[19].data_type().clone(), true),
            Field::new("timestamp_ns", columns[20].data_type().clone(), true),
            Field::new("decimal", columns[21].data_type().clone(), true),
            Field::new(
                "decimal_negative_scale",
                columns[22].data_type().clone(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let codec = RowCodec::new_validated(Arc::clone(&schema));
        let encoded = (0..rows)
            .map(|row| codec.encode_row(&batch, row).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            encoded[0].values[9],
            Value::Blob(u64::MAX.to_be_bytes().to_vec())
        );
        assert_eq!(
            encoded[0].values[10],
            Value::Blob((-0.0_f32).to_bits().to_be_bytes().to_vec())
        );
        assert_eq!(
            encoded[2].values[10],
            Value::Blob(0.0_f32.to_bits().to_be_bytes().to_vec())
        );
        assert_eq!(
            encoded[0].values[11],
            Value::Blob(0x7ff8_0000_0000_0001_u64.to_be_bytes().to_vec())
        );
        assert_eq!(
            encoded[3].values[11],
            Value::Blob(0x7ff8_0000_0000_0002_u64.to_be_bytes().to_vec())
        );
        assert!(
            encoded[1].values[..15]
                .iter()
                .all(|value| *value == Value::Null)
        );
        assert!(
            encoded[1].values[16..]
                .iter()
                .all(|value| *value == Value::Null)
        );
        assert_eq!(encoded[0].values[16], Value::Integer(i64::from(i32::MIN)));
        assert_eq!(encoded[0].values[17], Value::Integer(i64::MIN));
        assert_eq!(encoded[0].values[18], Value::Integer(i64::MAX));
        assert_eq!(encoded[0].values[19], Value::Integer(-1));
        assert_eq!(encoded[0].values[20], Value::Integer(i64::MIN + 1));
        assert_eq!(
            encoded[0].values[21],
            Value::Blob(decimal_max.to_be_bytes().to_vec())
        );
        assert_eq!(
            encoded[0].values[22],
            Value::Blob((-9_999_i128).to_be_bytes().to_vec())
        );

        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute(
                &format!(
                    "CREATE TABLE candidate ({}) STRICT",
                    schema
                        .fields()
                        .iter()
                        .map(|field| column_definition(field).unwrap())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                [],
            )
            .unwrap();
        let placeholders = (1..=schema.fields().len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = format!("INSERT INTO candidate VALUES ({placeholders})");
        for row in &encoded {
            connection
                .execute(&insert, params_from_iter(row.values.iter()))
                .unwrap();
        }
        let mut statement = connection
            .prepare("SELECT * FROM candidate ORDER BY rowid")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        for expected in &encoded {
            let row = rows.next().unwrap().unwrap();
            assert!(expected.matches(row, 0).unwrap());
        }
        assert!(rows.next().unwrap().is_none());

        assert_eq!(encoded[0].canonical, canonical_all_types_row_golden());
        assert_eq!(
            encoded[0].hash,
            [
                0x72, 0x8c, 0xd5, 0xb5, 0x65, 0xcb, 0x3d, 0x5c, 0x2c, 0x1e, 0xe7, 0xce, 0xfb, 0xe8,
                0x95, 0x80,
            ]
        );
    }

    fn canonical_all_types_row_golden() -> Vec<u8> {
        let hex = "
            00 01 01 01 80 01 80 00 01 80 00 00 00
            01 80 00 00 00 00 00 00 00 01 ff 01 ff ff
            01 ff ff ff ff 01 ff ff ff ff ff ff ff ff
            01 80 00 00 00 01 7f f8 00 00 00 00 00 01
            01 00 00 00 00 00 00 00 04 75 74 66 38
            01 00 00 00 00 00 00 00 02 00 ff
            01 00 00 00 00 00 00 00 03
              01 00 00 00 00 00 00 00 01
              00
              01 ff ff ff ff ff ff ff fe
            01 01 01 01 00 00 00 00 00 00 00 07
            01 80 00 00 00
            01 80 00 00 00 00 00 00 00
            01 7f ff ff ff ff ff ff ff
            01 ff ff ff ff ff ff ff ff
            01 80 00 00 00 00 00 00 01
            01 4b 3b 4c a8 5a 86 c4 7a 09 8a 22 3f ff ff ff ff
            01 ff ff ff ff ff ff ff ff ff ff ff ff ff ff d8 f1
        ";
        let digits = hex
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        digits
            .chunks_exact(2)
            .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
            .collect()
    }

    fn hex_nibble(digit: u8) -> u8 {
        match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            _ => panic!("invalid hexadecimal digit"),
        }
    }

    #[test]
    fn compares_sqlite_candidate_without_lossy_conversion() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                Arc::new(StringArray::from(vec![Some("same")])),
            ],
        )
        .unwrap();
        let codec = RowCodec::new_validated(schema);
        let encoded = codec.encode_row(&batch, 0).unwrap();
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute(
                "CREATE TABLE candidate (prefix INTEGER, number BLOB, text TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO candidate VALUES (7, ?1, ?2)",
                params_from_iter(encoded.values.iter()),
            )
            .unwrap();
        let mut statement = connection
            .prepare("SELECT prefix, number, text FROM candidate")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let matches = encoded.matches(row, 1).unwrap();
        assert!(matches);
    }

    #[test]
    fn a_null_struct_parent_ignores_its_hidden_non_nullable_child() {
        let child = Arc::new(Field::new("child", DataType::Int64, false));
        let structure = StructArray::new_null(vec![Arc::clone(&child)].into(), 1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "object",
            DataType::Struct(vec![child].into()),
            true,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(structure) as ArrayRef])
                .unwrap();
        let encoded = RowCodec::new_validated(schema)
            .encode_row(&batch, 0)
            .unwrap();

        assert_eq!(encoded.canonical, [0]);
        assert_eq!(encoded.values, [Value::Null]);
    }

    #[test]
    fn zero_column_row_has_stable_v1_hash() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let encoded = RowCodec::new_validated(schema)
            .encode_row(&batch, 0)
            .unwrap();

        assert!(encoded.canonical.is_empty());
        assert!(encoded.values.is_empty());
        assert_eq!(
            encoded.hash,
            [
                0x05, 0xb7, 0xb2, 0x91, 0xf8, 0xf9, 0xc4, 0x69, 0xe8, 0x3b, 0x36, 0x60, 0xb3, 0xc7,
                0x99, 0x26,
            ]
        );
    }
}

mod state {
    use super::super::state::{
        Continuation, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID, Mutation, MutationKind,
        PendingState, PendingStateCodecError, Position,
    };

    const DONE_TAG: u8 = 0;

    #[test]
    fn stable_v1_golden_bytes_round_trip() {
        let cases = [
            (PendingState::Initialize, vec![0, 1, 0]),
            (
                PendingState::Prepare {
                    position: Position {
                        row_index: 2,
                        remaining: 3,
                    },
                },
                vec![0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3],
            ),
            (
                PendingState::Apply {
                    start_position: Position {
                        row_index: 4,
                        remaining: 2,
                    },
                    continuation: Continuation::Done,
                    mutations: vec![
                        Mutation {
                            kind: MutationKind::Insert,
                            row_index: 4,
                            technical_id: 9,
                        },
                        Mutation {
                            kind: MutationKind::Insert,
                            row_index: 4,
                            technical_id: 10,
                        },
                        Mutation {
                            kind: MutationKind::Delete,
                            row_index: 5,
                            technical_id: 7,
                        },
                    ],
                },
                vec![
                    0, 1, 2, // format and Apply
                    0, 0, 0, 0, 0, 0, 0, 4, // start row
                    0, 0, 0, 0, 0, 0, 0, 2, // start remaining
                    0, // Done
                    0, 3, // mutation count
                    0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 9, // insert
                    0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 10, // insert
                    1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, // delete
                ],
            ),
        ];

        for (state, golden) in cases {
            assert_eq!(state.encode().unwrap(), golden);
            assert_eq!(PendingState::decode(&golden).unwrap(), state);
        }
    }

    #[test]
    fn every_truncated_golden_prefix_is_rejected() {
        let state = PendingState::Apply {
            start_position: Position {
                row_index: 0,
                remaining: 1,
            },
            continuation: Continuation::Done,
            mutations: vec![Mutation {
                kind: MutationKind::Insert,
                row_index: 0,
                technical_id: 1,
            }],
        };
        let encoded = state.encode().unwrap();
        for length in 0..encoded.len() {
            assert_eq!(
                PendingState::decode(&encoded[..length]),
                Err(PendingStateCodecError::Truncated),
                "accepted prefix of length {length}"
            );
        }
    }

    #[test]
    fn decoder_rejects_tags_versions_and_trailing_bytes() {
        assert_eq!(
            PendingState::decode(&[0, 2, 0]),
            Err(PendingStateCodecError::UnsupportedVersion(2))
        );
        assert_eq!(
            PendingState::decode(&[0, 1, 3]),
            Err(PendingStateCodecError::UnknownStateTag(3))
        );
        assert_eq!(
            PendingState::decode(&[0, 1, 0, 0]),
            Err(PendingStateCodecError::TrailingBytes)
        );

        let mut invalid_continuation = apply_prefix();
        invalid_continuation.push(2);
        assert_eq!(
            PendingState::decode(&invalid_continuation),
            Err(PendingStateCodecError::UnknownContinuationTag(2))
        );

        let mut invalid_kind = apply_prefix();
        invalid_kind.extend_from_slice(&[DONE_TAG, 0, 1, 2]);
        assert_eq!(
            PendingState::decode(&invalid_kind),
            Err(PendingStateCodecError::UnknownMutationKindTag(2))
        );
    }

    #[test]
    fn codec_rejects_invalid_positions_counts_and_ids() {
        let zero_remaining = PendingState::Prepare {
            position: Position {
                row_index: 0,
                remaining: 0,
            },
        };
        assert_eq!(
            zero_remaining.encode(),
            Err(PendingStateCodecError::ZeroRemaining)
        );

        let empty = PendingState::Apply {
            start_position: Position {
                row_index: 0,
                remaining: 1,
            },
            continuation: Continuation::Done,
            mutations: Vec::new(),
        };
        assert_eq!(
            empty.encode(),
            Err(PendingStateCodecError::EmptyMutationBatch)
        );

        for technical_id in [0, MAX_TECHNICAL_ID + 1] {
            let invalid_id = PendingState::Apply {
                start_position: Position {
                    row_index: 0,
                    remaining: 1,
                },
                continuation: Continuation::Done,
                mutations: vec![Mutation {
                    kind: MutationKind::Insert,
                    row_index: 0,
                    technical_id,
                }],
            };
            assert_eq!(
                invalid_id.encode(),
                Err(PendingStateCodecError::InvalidTechnicalId(technical_id))
            );
        }

        let too_many = PendingState::Apply {
            start_position: Position {
                row_index: 0,
                remaining: u64::try_from(MAX_MUTATIONS_PER_BATCH + 1).unwrap(),
            },
            continuation: Continuation::Done,
            mutations: vec![
                Mutation {
                    kind: MutationKind::Insert,
                    row_index: 0,
                    technical_id: 1,
                };
                MAX_MUTATIONS_PER_BATCH + 1
            ],
        };
        assert_eq!(
            too_many.encode(),
            Err(PendingStateCodecError::TooManyMutations(
                MAX_MUTATIONS_PER_BATCH + 1
            ))
        );
    }

    #[test]
    fn decoder_revalidates_counts_positions_and_ids() {
        let mut zero_remaining = PendingState::Prepare {
            position: Position {
                row_index: 0,
                remaining: 1,
            },
        }
        .encode()
        .unwrap();
        zero_remaining[11..19].fill(0);
        assert_eq!(
            PendingState::decode(&zero_remaining),
            Err(PendingStateCodecError::ZeroRemaining)
        );

        let mut empty_batch = apply_prefix();
        empty_batch.extend_from_slice(&[DONE_TAG, 0, 0]);
        assert_eq!(
            PendingState::decode(&empty_batch),
            Err(PendingStateCodecError::EmptyMutationBatch)
        );

        let mut oversized_batch = apply_prefix();
        oversized_batch.push(DONE_TAG);
        let oversized_count = u16::try_from(MAX_MUTATIONS_PER_BATCH + 1)
            .unwrap()
            .to_be_bytes();
        oversized_batch.extend_from_slice(&oversized_count);
        assert_eq!(
            PendingState::decode(&oversized_batch),
            Err(PendingStateCodecError::TooManyMutations(
                MAX_MUTATIONS_PER_BATCH + 1
            ))
        );

        let valid = PendingState::Apply {
            start_position: Position {
                row_index: 0,
                remaining: 1,
            },
            continuation: Continuation::Done,
            mutations: vec![insert(0, 1)],
        };
        let mut zero_id = valid.encode().unwrap();
        let id_offset = zero_id.len() - size_of::<u64>();
        zero_id[id_offset..].fill(0);
        assert_eq!(
            PendingState::decode(&zero_id),
            Err(PendingStateCodecError::InvalidTechnicalId(0))
        );
    }

    #[test]
    fn codec_rejects_noncanonical_apply_sequences() {
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 1,
            },
            Continuation::Done,
            vec![insert(2, 1)],
            PendingStateCodecError::FirstMutationRowMismatch,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 1,
            },
            Continuation::Done,
            vec![insert(1, 1), insert(3, 2)],
            PendingStateCodecError::NonContiguousMutationRows,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 2,
            },
            Continuation::Done,
            vec![insert(1, 1), delete(1, 2)],
            PendingStateCodecError::MixedMutationKinds,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 2,
            },
            Continuation::Done,
            vec![insert(1, 1), insert(1, 3)],
            PendingStateCodecError::NonConsecutiveInsertIds,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 2,
            },
            Continuation::Done,
            vec![delete(1, 2), delete(1, 1)],
            PendingStateCodecError::NonIncreasingDeleteIds,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 1,
            },
            Continuation::Done,
            vec![delete(1, 1), delete(2, 1)],
            PendingStateCodecError::DuplicateDeleteId,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 1,
            },
            Continuation::Done,
            vec![insert(1, 1), insert(1, 2)],
            PendingStateCodecError::StartRemainderExceeded,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 2,
            },
            Continuation::Done,
            vec![insert(1, 1)],
            PendingStateCodecError::InvalidContinuation,
        );
        assert_apply_error(
            Position {
                row_index: 1,
                remaining: 1,
            },
            Continuation::Position(Position {
                row_index: 2,
                remaining: 1,
            }),
            vec![insert(1, 1)],
            PendingStateCodecError::InvalidContinuation,
        );
    }

    #[test]
    fn full_batch_can_continue_within_a_row() {
        let batch_size = u64::try_from(MAX_MUTATIONS_PER_BATCH).unwrap();
        let mutations = (1..=batch_size)
            .map(|technical_id| insert(7, technical_id))
            .collect::<Vec<_>>();
        let state = PendingState::Apply {
            start_position: Position {
                row_index: 7,
                remaining: batch_size + 3,
            },
            continuation: Continuation::Position(Position {
                row_index: 7,
                remaining: 3,
            }),
            mutations,
        };

        let encoded = state.encode().unwrap();
        assert_eq!(PendingState::decode(&encoded).unwrap(), state);
    }

    fn apply_prefix() -> Vec<u8> {
        let mut encoded = vec![0, 1, 2];
        encoded.extend_from_slice(&0_u64.to_be_bytes());
        encoded.extend_from_slice(&1_u64.to_be_bytes());
        encoded
    }

    fn insert(row_index: u64, technical_id: u64) -> Mutation {
        Mutation {
            kind: MutationKind::Insert,
            row_index,
            technical_id,
        }
    }

    fn delete(row_index: u64, technical_id: u64) -> Mutation {
        Mutation {
            kind: MutationKind::Delete,
            row_index,
            technical_id,
        }
    }

    fn assert_apply_error(
        start_position: Position,
        continuation: Continuation,
        mutations: Vec<Mutation>,
        expected: PendingStateCodecError,
    ) {
        let state = PendingState::Apply {
            start_position,
            continuation,
            mutations,
        };
        assert_eq!(state.encode(), Err(expected));
    }
}

mod runtime {
    use std::{path::PathBuf, sync::Arc};

    use arrow_array::{
        ArrayRef, Date32Array, Decimal128Array, Int64Array, NullArray, RecordBatch,
        TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use dogpaddle_change::Change;
    use dogpaddle_store::{Cell, Store, Transactions};
    use rusqlite::{Connection, TransactionBehavior, params};
    use tempfile::TempDir;

    use super::super::{
        error::SqliteSinkError,
        runtime::{SqliteSinkCompiled, SqliteSinkOperation},
        state::{Continuation, MAX_TECHNICAL_ID, MutationKind, PendingState, Position},
    };
    use crate::operation::{Action, Operation, OperationError, OperationInput};

    const TABLE: &str = "materialized";

    struct Fixture {
        root: TempDir,
        store_path: PathBuf,
        sqlite_path: PathBuf,
        next_id: Cell<u64>,
        pending: Cell<Vec<u8>>,
        operation: SqliteSinkOperation,
        transactions: Transactions,
    }

    impl Fixture {
        fn new(schema: SchemaRef) -> Self {
            let root = tempfile::tempdir().unwrap();
            let store_path = root.path().join("store");
            let sqlite_path = root.path().join("sink.sqlite");
            let mut store = Store::create(&store_path).unwrap();
            let next_id = store
                .create_data::<Cell<u64>>("sqlite_sink.next_id")
                .unwrap();
            let pending = store
                .create_data::<Cell<Vec<u8>>>("sqlite_sink.pending")
                .unwrap();
            let compiled =
                SqliteSinkCompiled::try_new(sqlite_path.clone(), TABLE.to_owned(), schema).unwrap();
            let operation =
                SqliteSinkOperation::new_bound(compiled, next_id.clone(), pending.clone());
            let transactions = store.into_transactions();
            Self {
                root,
                store_path,
                sqlite_path,
                next_id,
                pending,
                operation,
                transactions,
            }
        }

        fn reopen(self, schema: SchemaRef) -> Self {
            let Self {
                root,
                store_path,
                sqlite_path,
                operation,
                transactions,
                ..
            } = self;
            drop(operation);
            drop(transactions);

            let store = Store::open(&store_path).unwrap();
            let next_id = store.open_data::<Cell<u64>>("sqlite_sink.next_id").unwrap();
            let pending = store
                .open_data::<Cell<Vec<u8>>>("sqlite_sink.pending")
                .unwrap();
            let compiled =
                SqliteSinkCompiled::try_new(sqlite_path.clone(), TABLE.to_owned(), schema).unwrap();
            let operation =
                SqliteSinkOperation::new_bound(compiled, next_id.clone(), pending.clone());
            let transactions = store.into_transactions();
            Self {
                root,
                store_path,
                sqlite_path,
                next_id,
                pending,
                operation,
                transactions,
            }
        }

        fn committed_turn(&mut self, change: &Change) -> Action {
            let transaction = self.transactions.begin().unwrap();
            let action = self
                .operation
                .turn(
                    Some(OperationInput { port: 0, change }),
                    transaction.access(),
                )
                .unwrap();
            transaction.commit().unwrap();
            action
        }

        fn rolled_back_turn(&mut self, change: &Change) -> Action {
            let transaction = self.transactions.begin().unwrap();
            let action = self
                .operation
                .turn(
                    Some(OperationInput { port: 0, change }),
                    transaction.access(),
                )
                .unwrap();
            drop(transaction);
            action
        }

        fn failed_turn(&mut self, change: &Change) -> OperationError {
            let transaction = self.transactions.begin().unwrap();
            let error = self
                .operation
                .turn(
                    Some(OperationInput { port: 0, change }),
                    transaction.access(),
                )
                .unwrap_err();
            drop(transaction);
            error
        }

        fn pending_state(&mut self) -> Option<PendingState> {
            let transaction = self.transactions.begin().unwrap();
            let encoded = self
                .pending
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap();
            drop(transaction);
            encoded.map(|encoded| PendingState::decode(&encoded).unwrap())
        }

        fn durable_next_id(&mut self) -> Option<u64> {
            let transaction = self.transactions.begin().unwrap();
            let next_id = self
                .next_id
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap();
            drop(transaction);
            next_id
        }

        fn set_pending_state(&mut self, state: &PendingState) {
            let transaction = self.transactions.begin().unwrap();
            self.pending
                .access(transaction.access())
                .unwrap()
                .set(&state.encode().unwrap())
                .unwrap();
            transaction.commit().unwrap();
        }

        fn set_next_id(&mut self, next_id: u64) {
            let transaction = self.transactions.begin().unwrap();
            self.next_id
                .access(transaction.access())
                .unwrap()
                .set(&next_id)
                .unwrap();
            transaction.commit().unwrap();
        }

        fn initialize(&mut self, change: &Change) {
            assert_commit(self.committed_turn(change));
            assert_eq!(self.pending_state(), Some(PendingState::Initialize));
            assert_eq!(self.durable_next_id(), None);
            assert_commit(self.committed_turn(change));
            assert_eq!(self.pending_state(), None);
            assert_eq!(self.durable_next_id(), Some(1));
        }

        fn row_count(&self) -> i64 {
            let connection = Connection::open(&self.sqlite_path).unwrap();
            connection
                .query_row(&format!("SELECT COUNT(*) FROM \"{TABLE}\""), [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn ids(&self) -> Vec<i64> {
            let connection = Connection::open(&self.sqlite_path).unwrap();
            let mut statement = connection
                .prepare(&format!(
                    "SELECT \"$dogpaddle.id\" FROM \"{TABLE}\" ORDER BY \"$dogpaddle.id\""
                ))
                .unwrap();
            statement
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }

        fn id_values(&self) -> Vec<(i64, i64)> {
            let connection = Connection::open(&self.sqlite_path).unwrap();
            let mut statement = connection
                .prepare(
                    "SELECT \"$dogpaddle.id\", \"value\" FROM \"materialized\" \
                     ORDER BY \"$dogpaddle.id\"",
                )
                .unwrap();
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }

        fn finish_change(&mut self, change: &Change) {
            loop {
                match self.committed_turn(change) {
                    Action::Commit(None) => {}
                    Action::Complete(None) => break,
                    action => panic!("unexpected SQLite sink action: {action:?}"),
                }
            }
        }
    }

    #[test]
    fn first_turn_opens_database_but_two_stage_initialization_delays_table_creation() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        assert!(!fixture.sqlite_path.exists());

        assert_commit(fixture.committed_turn(&change));
        assert!(fixture.sqlite_path.exists());
        let connection = Connection::open(&fixture.sqlite_path).unwrap();
        let objects: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name IN (?1, ?2)",
                [TABLE, "$dogpaddle.hash_index.materialized"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(objects, 0);
        assert_eq!(fixture.pending_state(), Some(PendingState::Initialize));
        assert_eq!(fixture.durable_next_id(), None);

        assert_commit(fixture.committed_turn(&change));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1));
        assert_eq!(fixture.row_count(), 0);

        assert_commit(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 0);
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 1);
        assert_eq!(fixture.ids(), [1]);
    }

    #[test]
    fn temporal_and_decimal_row_inserts_queries_and_retracts_without_loss() {
        let date = Arc::new(Date32Array::from(vec![i32::MIN])) as ArrayRef;
        let timestamp = Arc::new(
            TimestampNanosecondArray::from(vec![i64::MAX]).with_timezone("America/New_York"),
        ) as ArrayRef;
        let amount = Arc::new(
            Decimal128Array::from(vec![-12_345_i128])
                .with_precision_and_scale(38, -4)
                .unwrap(),
        ) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("timestamp", timestamp.data_type().clone(), false),
            Field::new("amount", amount.data_type().clone(), false),
        ]));
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![date, Arc::clone(&timestamp), Arc::clone(&amount)],
        )
        .unwrap();
        let insert = Change::try_new(records.clone(), Int64Array::from(vec![1])).unwrap();
        let retract = Change::try_new(records, Int64Array::from(vec![-1])).unwrap();
        let mut fixture = Fixture::new(schema);

        fixture.initialize(&insert);
        fixture.finish_change(&insert);
        let connection = Connection::open(&fixture.sqlite_path).unwrap();
        let stored = connection
            .query_row(
                "SELECT date, timestamp, amount FROM materialized",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            stored,
            (
                i64::from(i32::MIN),
                i64::MAX,
                (-12_345_i128).to_be_bytes().to_vec(),
            )
        );
        drop(connection);

        fixture.finish_change(&retract);
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn initialization_replay_requires_an_exact_empty_layout_across_reopen() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        assert_commit(fixture.committed_turn(&change));

        assert_commit(fixture.rolled_back_turn(&change));
        assert_eq!(fixture.pending_state(), Some(PendingState::Initialize));
        assert_eq!(fixture.durable_next_id(), None);
        assert_eq!(fixture.row_count(), 0);

        let connection = Connection::open(&fixture.sqlite_path).unwrap();
        connection
            .execute(
                "INSERT INTO \"materialized\" \
                 (\"$dogpaddle.id\", \"$dogpaddle.hash\", \"value\") \
                 VALUES (1, zeroblob(16), 7)",
                [],
            )
            .unwrap();
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TargetNotEmpty { table }) if table == TABLE
        ));
        connection
            .execute("DELETE FROM \"materialized\"", [])
            .unwrap();
        drop(connection);

        let mut fixture = fixture.reopen(schema);
        assert_commit(fixture.committed_turn(&change));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1));
    }

    #[test]
    fn first_turn_detects_existing_object_names_with_sqlite_identifier_casing() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        Connection::open(&fixture.sqlite_path)
            .unwrap()
            .execute("CREATE TABLE \"MATERIALIZED\" (value INTEGER)", [])
            .unwrap();

        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TargetExists { name }) if name == TABLE
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), None);
    }

    #[test]
    fn initialization_replay_rejects_an_attached_trigger() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        assert_commit(fixture.committed_turn(&change));
        assert_commit(fixture.rolled_back_turn(&change));
        Connection::open(&fixture.sqlite_path)
            .unwrap()
            .execute(
                "CREATE TRIGGER \"erase_insert\" AFTER INSERT ON \"materialized\" \
                 BEGIN DELETE FROM \"materialized\" \
                 WHERE \"$dogpaddle.id\" = NEW.\"$dogpaddle.id\"; END",
                [],
            )
            .unwrap();

        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TargetLayoutMismatch { name }) if name == "erase_insert"
        ));
        assert_eq!(fixture.pending_state(), Some(PendingState::Initialize));
        assert_eq!(fixture.durable_next_id(), None);
    }

    #[test]
    fn ready_state_rejects_missing_and_extra_schema_objects() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        let connection = Connection::open(&fixture.sqlite_path).unwrap();

        connection
            .execute("DROP INDEX \"$dogpaddle.hash_index.materialized\"", [])
            .unwrap();
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TargetMissing { name })
                if name == "$dogpaddle.hash_index.materialized"
        ));

        connection
            .execute(
                "CREATE INDEX \"$dogpaddle.hash_index.materialized\" \
                 ON \"materialized\"(\"$dogpaddle.hash\")",
                [],
            )
            .unwrap();
        connection
            .execute("CREATE INDEX \"extra\" ON \"materialized\"(\"value\")", [])
            .unwrap();
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TargetLayoutMismatch { name }) if name == "extra"
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1));
    }

    #[test]
    fn one_row_multiplicity_respects_the_1024_and_1025_batch_boundary() {
        for (multiplicity, expected_batches) in [(1_024, 1), (1_025, 2)] {
            let schema = value_schema();
            let change = value_change(&schema, &[7], &[multiplicity]);
            let mut fixture = Fixture::new(schema);
            fixture.initialize(&change);

            let mut batches = 0;
            loop {
                assert_commit(fixture.committed_turn(&change));
                let Some(PendingState::Apply { mutations, .. }) = fixture.pending_state() else {
                    panic!("prepare turn did not persist an Apply batch")
                };
                assert!(mutations.len() <= 1_024);
                batches += 1;
                match fixture.committed_turn(&change) {
                    Action::Commit(None) => {}
                    Action::Complete(None) => break,
                    action => panic!("unexpected SQLite sink action: {action:?}"),
                }
            }

            assert_eq!(batches, expected_batches);
            assert_eq!(fixture.row_count(), multiplicity);
            assert_eq!(
                fixture.durable_next_id(),
                Some(u64::try_from(multiplicity).unwrap() + 1)
            );
        }
    }

    #[test]
    fn maximum_1998_column_schema_creates_and_inserts_into_a_strict_table() {
        let schema = Arc::new(Schema::new(
            (0..1_998)
                .map(|index| Field::new(format!("field_{index}"), DataType::Null, false))
                .collect::<Vec<_>>(),
        ));
        let columns = (0..1_998)
            .map(|_| Arc::new(NullArray::new(1)) as ArrayRef)
            .collect::<Vec<_>>();
        let records = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
        let mut fixture = Fixture::new(schema);

        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 1);
        assert_eq!(fixture.ids(), [1]);
    }

    #[test]
    fn a_full_batch_crosses_change_rows_without_reordering_them() {
        let schema = value_schema();
        let change = value_change(&schema, &[10, 20], &[600, 500]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);

        assert_commit(fixture.committed_turn(&change));
        let Some(PendingState::Apply {
            continuation,
            mutations,
            ..
        }) = fixture.pending_state()
        else {
            panic!("prepare turn did not persist an Apply batch")
        };
        assert_eq!(mutations.len(), 1_024);
        assert_eq!(
            mutations
                .iter()
                .filter(|mutation| mutation.row_index == 0)
                .count(),
            600
        );
        assert_eq!(
            mutations
                .iter()
                .filter(|mutation| mutation.row_index == 1)
                .count(),
            424
        );
        assert_eq!(
            continuation,
            Continuation::Position(Position {
                row_index: 1,
                remaining: 76,
            })
        );

        assert_commit(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 1_024);
        assert_eq!(
            fixture.pending_state(),
            Some(PendingState::Prepare {
                position: Position {
                    row_index: 1,
                    remaining: 76,
                },
            })
        );
        assert_commit(fixture.committed_turn(&change));
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 1_100);
    }

    #[test]
    fn stable_change_rebatching_preserves_ids_and_final_relation() {
        let schema = value_schema();
        let combined = value_change(&schema, &[7, 7, 8, 8], &[2, -1, 2, -1]);
        let first = value_change(&schema, &[7], &[2]);
        let second = value_change(&schema, &[7, 8, 8], &[-1, 2, -1]);

        let mut one_change = Fixture::new(Arc::clone(&schema));
        one_change.initialize(&combined);
        one_change.finish_change(&combined);

        let mut two_changes = Fixture::new(schema);
        two_changes.initialize(&first);
        two_changes.finish_change(&first);
        two_changes.finish_change(&second);

        assert_eq!(one_change.id_values(), [(2, 7), (4, 8)]);
        assert_eq!(two_changes.id_values(), one_change.id_values());
        assert_eq!(two_changes.durable_next_id(), Some(5));
    }

    #[test]
    fn a_persisted_prepare_position_must_be_on_a_stable_batch_boundary() {
        let schema = value_schema();
        let change = value_change(&schema, &[10, 20], &[600, 500]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        fixture.set_pending_state(&PendingState::Prepare {
            position: Position {
                row_index: 1,
                remaining: 500,
            },
        });

        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::PendingInputMismatch { message })
                if message.contains("stable 1024-mutation batch boundary")
        ));
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn a_persisted_apply_insert_range_must_end_at_the_next_id_frontier() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        fixture.set_next_id(100);
        fixture.set_pending_state(&PendingState::Apply {
            start_position: Position {
                row_index: 0,
                remaining: 1,
            },
            continuation: Continuation::Done,
            mutations: vec![super::super::state::Mutation {
                kind: MutationKind::Insert,
                row_index: 0,
                technical_id: 1,
            }],
        });

        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::PendingInputMismatch { message })
                if message.contains("next-ID frontier")
        ));
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn negative_multiplicity_is_fully_preflighted_without_partial_work() {
        let schema = value_schema();
        let invalid_after_insert = value_change(&schema, &[7, 8], &[1, -2]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        fixture.initialize(&invalid_after_insert);

        let error = fixture.failed_turn(&invalid_after_insert);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::MissingRetraction {
                row_index: 1,
                needed: 2,
                available: 0,
            })
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1));
        assert_eq!(fixture.row_count(), 0);

        let minimum = value_change(&schema, &[9], &[i64::MIN]);
        let error = fixture.failed_turn(&minimum);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::MissingRetraction {
                row_index: 0,
                needed,
                available: 0,
            }) if *needed == 1_u64 << 63
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.row_count(), 0);

        let existing = value_change(&schema, &[11], &[1_024]);
        fixture.finish_change(&existing);
        let over_batch_boundary = value_change(&schema, &[11], &[-1_025]);
        let error = fixture.failed_turn(&over_batch_boundary);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::MissingRetraction {
                row_index: 0,
                needed: 1_025,
                available: 1_024,
            })
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1_025));
        assert_eq!(fixture.row_count(), 1_024);
    }

    #[test]
    fn id_exhaustion_is_checked_for_the_complete_remaining_multiplicity() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[2]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        fixture.set_next_id(MAX_TECHNICAL_ID);

        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TechnicalIdExhausted {
                next_id,
                needed: 2,
            }) if *next_id == MAX_TECHNICAL_ID
        ));
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(MAX_TECHNICAL_ID));
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn an_insert_then_delete_of_the_same_row_cancels_inside_one_batch() {
        let schema = value_schema();
        let change = value_change(&schema, &[7, 7], &[1, -1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);

        assert_commit(fixture.committed_turn(&change));
        let Some(PendingState::Apply { mutations, .. }) = fixture.pending_state() else {
            panic!("prepare turn did not persist an Apply batch")
        };
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].kind, MutationKind::Insert);
        assert_eq!(mutations[1].kind, MutationKind::Delete);
        assert_eq!(mutations[0].technical_id, mutations[1].technical_id);

        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 0);
        assert_eq!(fixture.durable_next_id(), Some(2));
    }

    #[test]
    fn deleting_duplicate_rows_selects_the_smallest_technical_id() {
        let schema = value_schema();
        let inserts = value_change(&schema, &[7], &[3]);
        let delete = value_change(&schema, &[7], &[-1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&inserts);
        assert_commit(fixture.committed_turn(&inserts));
        assert_complete(fixture.committed_turn(&inserts));
        assert_eq!(fixture.ids(), [1, 2, 3]);

        assert_commit(fixture.committed_turn(&delete));
        let Some(PendingState::Apply { mutations, .. }) = fixture.pending_state() else {
            panic!("prepare turn did not persist an Apply batch")
        };
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].kind, MutationKind::Delete);
        assert_eq!(mutations[0].technical_id, 1);

        assert_complete(fixture.committed_turn(&delete));
        assert_eq!(fixture.ids(), [2, 3]);
    }

    #[test]
    fn hash_collisions_are_filtered_by_exact_logical_values() {
        let schema = value_schema();
        let inserts = value_change(&schema, &[10, 20], &[1, 1]);
        let delete_twenty = value_change(&schema, &[20], &[-1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&inserts);
        assert_commit(fixture.committed_turn(&inserts));
        assert_complete(fixture.committed_turn(&inserts));

        let connection = Connection::open(&fixture.sqlite_path).unwrap();
        let hash: Vec<u8> = connection
            .query_row(
                "SELECT \"$dogpaddle.hash\" FROM \"materialized\" \
                 WHERE \"$dogpaddle.id\" = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "UPDATE \"materialized\" SET \"$dogpaddle.hash\" = ?1 \
                 WHERE \"$dogpaddle.id\" = 1",
                params![hash],
            )
            .unwrap();
        drop(connection);

        assert_commit(fixture.committed_turn(&delete_twenty));
        let Some(PendingState::Apply { mutations, .. }) = fixture.pending_state() else {
            panic!("prepare turn did not persist an Apply batch")
        };
        assert_eq!(mutations[0].technical_id, 2);
        assert_complete(fixture.committed_turn(&delete_twenty));
        assert_eq!(fixture.ids(), [1]);
    }

    #[test]
    fn replay_after_sqlite_commit_and_store_rollback_applies_exactly_once() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        let apply = fixture.pending_state().unwrap();

        assert_complete(fixture.rolled_back_turn(&change));
        assert_eq!(fixture.row_count(), 1);
        assert_eq!(fixture.pending_state(), Some(apply.clone()));

        let mut fixture = fixture.reopen(schema);
        assert_eq!(fixture.pending_state(), Some(apply));
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.row_count(), 1);
        assert_eq!(fixture.ids(), [1]);
        assert_eq!(fixture.pending_state(), None);
    }

    #[test]
    fn sqlite_write_lock_error_preserves_apply_state_and_can_retry() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        let apply = fixture.pending_state().unwrap();

        let mut blocker = Connection::open(&fixture.sqlite_path).unwrap();
        let blocker_transaction = blocker
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::Sqlite(_))
        ));
        drop(blocker_transaction);

        assert_eq!(fixture.pending_state(), Some(apply));
        assert_eq!(fixture.row_count(), 0);
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.ids(), [1]);
    }

    #[test]
    fn sqlite_commit_lock_error_rolls_back_rows_and_pending_then_can_retry() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(schema);
        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        let apply = fixture.pending_state().unwrap();

        let blocker = Connection::open(&fixture.sqlite_path).unwrap();
        blocker.execute_batch("BEGIN").unwrap();
        let mut statement = blocker
            .prepare("SELECT \"$dogpaddle.id\" FROM \"materialized\"")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        assert!(rows.next().unwrap().is_none());
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::Sqlite(_))
        ));
        drop(rows);
        drop(statement);
        blocker.execute_batch("COMMIT").unwrap();

        assert_eq!(fixture.pending_state(), Some(apply));
        assert_eq!(fixture.row_count(), 0);
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.ids(), [1]);
    }

    #[test]
    fn conflicting_insert_and_delete_rows_preserve_apply_state_and_can_retry() {
        let schema = value_schema();
        let change = value_change(&schema, &[7], &[1]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        let apply = fixture.pending_state().unwrap();

        let connection = Connection::open(&fixture.sqlite_path).unwrap();
        connection
            .execute(
                "INSERT INTO \"materialized\" \
                 (\"$dogpaddle.id\", \"$dogpaddle.hash\", \"value\") \
                 VALUES (1, zeroblob(16), 99)",
                [],
            )
            .unwrap();
        drop(connection);
        let error = fixture.failed_turn(&change);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::TechnicalIdConflict { id: 1 })
        ));
        assert_eq!(fixture.pending_state(), Some(apply));

        Connection::open(&fixture.sqlite_path)
            .unwrap()
            .execute(
                "DELETE FROM \"materialized\" WHERE \"$dogpaddle.id\" = 1",
                [],
            )
            .unwrap();
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.ids(), [1]);

        let delete = value_change(&schema, &[7], &[-1]);
        assert_commit(fixture.committed_turn(&delete));
        let apply = fixture.pending_state().unwrap();
        Connection::open(&fixture.sqlite_path)
            .unwrap()
            .execute(
                "UPDATE \"materialized\" SET \"value\" = 99 \
                 WHERE \"$dogpaddle.id\" = 1",
                [],
            )
            .unwrap();
        let error = fixture.failed_turn(&delete);
        assert!(matches!(
            error.downcast_ref::<SqliteSinkError>(),
            Some(SqliteSinkError::DeleteRowMismatch { id: 1 })
        ));
        assert_eq!(fixture.pending_state(), Some(apply));

        Connection::open(&fixture.sqlite_path)
            .unwrap()
            .execute(
                "UPDATE \"materialized\" SET \"value\" = 7 \
                 WHERE \"$dogpaddle.id\" = 1",
                [],
            )
            .unwrap();
        assert_complete(fixture.committed_turn(&delete));
        assert_eq!(fixture.row_count(), 0);
    }

    #[test]
    fn replay_of_a_committed_delete_after_store_rollback_is_exactly_once() {
        let schema = value_schema();
        let insert = value_change(&schema, &[7], &[1]);
        let delete = value_change(&schema, &[7], &[-1]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        fixture.initialize(&insert);
        assert_commit(fixture.committed_turn(&insert));
        assert_complete(fixture.committed_turn(&insert));
        assert_eq!(fixture.ids(), [1]);

        assert_commit(fixture.committed_turn(&delete));
        let apply = fixture.pending_state().unwrap();
        assert_complete(fixture.rolled_back_turn(&delete));
        assert_eq!(fixture.row_count(), 0);
        assert_eq!(fixture.pending_state(), Some(apply.clone()));

        let mut fixture = fixture.reopen(schema);
        assert_eq!(fixture.pending_state(), Some(apply));
        assert_complete(fixture.committed_turn(&delete));
        assert_eq!(fixture.row_count(), 0);
        assert_eq!(fixture.pending_state(), None);
    }

    #[test]
    fn replay_of_a_full_1024_mutation_batch_is_exactly_once() {
        let schema = value_schema();
        let change = value_change(&schema, &[7, 8, 7], &[1_022, 1, -1]);
        let mut fixture = Fixture::new(Arc::clone(&schema));
        fixture.initialize(&change);
        assert_commit(fixture.committed_turn(&change));
        let apply = fixture.pending_state().unwrap();

        assert_complete(fixture.rolled_back_turn(&change));
        assert_eq!(fixture.ids(), (2_i64..=1_023).collect::<Vec<_>>());
        assert_eq!(fixture.pending_state(), Some(apply.clone()));

        let mut fixture = fixture.reopen(schema);
        assert_eq!(fixture.pending_state(), Some(apply));
        assert_complete(fixture.committed_turn(&change));
        assert_eq!(fixture.ids(), (2_i64..=1_023).collect::<Vec<_>>());
        assert_eq!(fixture.pending_state(), None);
        assert_eq!(fixture.durable_next_id(), Some(1_024));
    }

    fn value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn value_change(schema: &SchemaRef, values: &[i64], diffs: &[i64]) -> Change {
        assert_eq!(values.len(), diffs.len());
        let records = RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
    }

    fn assert_commit(action: Action) {
        match action {
            Action::Commit(None) => {}
            action => panic!("expected Commit(None), got {action:?}"),
        }
    }

    fn assert_complete(action: Action) {
        match action {
            Action::Complete(None) => {}
            action => panic!("expected Complete(None), got {action:?}"),
        }
    }
}
