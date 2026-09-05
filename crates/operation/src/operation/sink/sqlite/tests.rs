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
                0x7c, 0xdb, 0x54, 0xfc, 0x12, 0x8d, 0x90, 0xc1, 0x31, 0x4d, 0x2f, 0xf3, 0x85, 0xa5,
                0xfc, 0x70,
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
    fn zero_column_row_has_stable_relation_hash() {
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
                0x4b, 0x9b, 0x97, 0x07, 0xa9, 0xae, 0xba, 0x64, 0x25, 0x41, 0x84, 0x6f, 0x72, 0x96,
                0x45, 0xf8,
            ]
        );
    }
}

mod target {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use dogpaddle_change::Change;
    use rusqlite::{Connection, params};
    use tempfile::TempDir;

    use super::super::{error::SqliteSinkError, target::SqliteTarget};
    use crate::operation::sink::relation::{Batch, Continuation, Insert, Lookup, RelationTarget};

    struct Fixture {
        root: TempDir,
        schema: SchemaRef,
        target: SqliteTarget,
    }

    impl Fixture {
        fn new(schema: SchemaRef) -> Self {
            let root = tempfile::tempdir().unwrap();
            let target = SqliteTarget::try_new(
                root.path().join("sink.sqlite"),
                "materialized".to_owned(),
                Arc::clone(&schema),
            )
            .unwrap();
            Self {
                root,
                schema,
                target,
            }
        }

        fn fresh_target(&self) -> SqliteTarget {
            SqliteTarget::try_new(
                self.root.path().join("sink.sqlite"),
                "materialized".to_owned(),
                Arc::clone(&self.schema),
            )
            .unwrap()
        }

        fn connection(&self) -> Connection {
            Connection::open(self.root.path().join("sink.sqlite")).unwrap()
        }

        fn initialize(&mut self) {
            self.target.require_absent().unwrap();
            self.target.initialize().unwrap();
        }

        fn rows(&self) -> Vec<(i64, i64)> {
            self.connection()
                .prepare(
                    "SELECT \"$dogpaddle.id\", value FROM materialized ORDER BY \"$dogpaddle.id\"",
                )
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn change(schema: &SchemaRef, values: Vec<ArrayRef>) -> Change {
        let records = RecordBatch::try_new_with_options(
            Arc::clone(schema),
            values,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let rows = records.num_rows();
        Change::try_new(records, Int64Array::from(vec![1; rows])).unwrap()
    }

    fn values(schema: &SchemaRef, values: &[i64]) -> Change {
        let records = RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        Change::try_new(records, Int64Array::from(vec![1; values.len()])).unwrap()
    }

    fn batch(inserts: &[(u64, u64)], deletes: &[u64]) -> Batch {
        Batch {
            inserts: inserts
                .iter()
                .map(|&(row_index, technical_id)| Insert {
                    row_index,
                    technical_id,
                })
                .collect(),
            deletes: deletes.to_vec(),
            continuation: Continuation::Done,
        }
    }

    #[test]
    fn initialization_requires_absence_and_replay_requires_the_exact_empty_layout() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture.fresh_target().initialize().unwrap();
        assert!(matches!(
            fixture.fresh_target().require_absent(),
            Err(SqliteSinkError::TargetExists { .. })
        ));
        let input = values(&fixture.schema, &[7]);
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1)], &[]))
            .unwrap();
        assert!(matches!(
            fixture.fresh_target().initialize(),
            Err(SqliteSinkError::TargetNotEmpty { .. })
        ));
    }

    #[test]
    fn initialization_and_reconnect_reject_extra_schema_objects() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture
            .connection()
            .execute_batch(
                "CREATE TRIGGER unexpected AFTER INSERT ON materialized BEGIN SELECT 1; END;",
            )
            .unwrap();
        assert!(matches!(
            fixture.fresh_target().initialize(),
            Err(SqliteSinkError::TargetLayoutMismatch { .. })
        ));
        let input = values(&fixture.schema, &[7]);
        assert!(
            fixture
                .fresh_target()
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    }]
                )
                .is_err()
        );
    }

    #[test]
    fn fixed_inserts_and_deletes_replay_as_one_idempotent_batch() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        let input = values(&fixture.schema, &[7, 8]);
        let writes = batch(&[(0, 1), (1, 2), (0, 3)], &[1, 3, 999]);
        fixture.target.write_batch(&input, &writes).unwrap();
        assert_eq!(fixture.rows(), [(2, 8)]);
        fixture.target = fixture.fresh_target();
        fixture.target.write_batch(&input, &writes).unwrap();
        assert_eq!(fixture.rows(), [(2, 8)]);
    }

    #[test]
    fn only_primary_key_duplicates_are_ignored_and_other_errors_roll_back_the_batch() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        let input = values(&fixture.schema, &[7, 8]);
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1)], &[]))
            .unwrap();
        fixture
            .connection()
            .execute_batch(
                "CREATE TRIGGER reject_eight BEFORE INSERT ON materialized \
             WHEN NEW.value = 8 BEGIN SELECT RAISE(ABORT, 'rejected'); END;",
            )
            .unwrap();
        assert!(
            fixture
                .target
                .write_batch(&input, &batch(&[(0, 2), (1, 3)], &[1]))
                .is_err()
        );
        assert_eq!(fixture.rows(), [(1, 7)]);
        fixture
            .connection()
            .execute_batch("DROP TRIGGER reject_eight")
            .unwrap();
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 2), (1, 3)], &[1]))
            .unwrap();
        assert_eq!(fixture.rows(), [(2, 7), (3, 8)]);
    }

    #[test]
    fn lookup_returns_bounded_smallest_ids_and_a_capped_exact_count() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        let input = values(&fixture.schema, &[7]);
        for offset in [0_u64, 1_024] {
            let writes = batch(
                &(1..=1_024).map(|id| (0, offset + id)).collect::<Vec<_>>(),
                &[],
            );
            fixture.target.write_batch(&input, &writes).unwrap();
        }
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    needed: 2_000,
                    take: 4,
                }],
            )
            .unwrap();
        assert_eq!(matches[0].count, 2_000);
        assert_eq!(matches[0].ids, [1, 2, 3, 4]);
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    needed: 3_000,
                    take: 1_024,
                }],
            )
            .unwrap();
        assert_eq!(matches[0].count, 2_048);
        assert_eq!(matches[0].ids.len(), 1_024);
        fixture
            .target
            .write_batch(&input, &batch(&[], &(1..=1_024).collect::<Vec<_>>()))
            .unwrap();
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    needed: 4,
                    take: 4,
                }],
            )
            .unwrap();
        assert_eq!(matches[0].ids, [1_025, 1_026, 1_027, 1_028]);
    }

    #[test]
    fn hash_collisions_do_not_match_other_logical_rows() {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        let input = values(&fixture.schema, &[7, 8]);
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1), (1, 2), (0, 3)], &[]))
            .unwrap();
        let hash = fixture.target.encode_row(&input, 0).unwrap().hash;
        fixture
            .connection()
            .execute(
                "UPDATE materialized SET \"$dogpaddle.hash\" = ?1 WHERE \"$dogpaddle.id\" = 2",
                params![hash],
            )
            .unwrap();
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    needed: 3,
                    take: 3,
                }],
            )
            .unwrap();
        assert_eq!(matches[0].count, 2);
        assert_eq!(matches[0].ids, [1, 3]);
    }

    #[test]
    fn null_and_nul_containing_text_are_matched_exactly_in_request_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let input = Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![
                    Some("a\0b"),
                    Some("a\0c"),
                    None,
                ]))],
            )
            .unwrap(),
            Int64Array::from(vec![1, 1, 1]),
        )
        .unwrap();
        let mut fixture = Fixture::new(schema);
        fixture.initialize();
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1), (1, 2), (2, 3)], &[]))
            .unwrap();
        let first_hash = fixture.target.encode_row(&input, 0).unwrap().hash;
        fixture
            .connection()
            .execute(
                "UPDATE materialized SET \"$dogpaddle.hash\" = ?1 WHERE \"$dogpaddle.id\" = 2",
                params![first_hash],
            )
            .unwrap();
        let matches = fixture
            .target
            .lookup(
                &input,
                &[
                    Lookup {
                        row_index: 2,
                        needed: 2,
                        take: 2,
                    },
                    Lookup {
                        row_index: 0,
                        needed: 2,
                        take: 2,
                    },
                ],
            )
            .unwrap();
        assert_eq!(matches[0].ids, [3]);
        assert_eq!(matches[1].ids, [1]);
    }

    #[test]
    fn empty_and_maximum_width_schemas_support_insert_lookup_and_delete() {
        for fields in [0, 1_998] {
            let schema = Arc::new(Schema::new(
                (0..fields)
                    .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
                    .collect::<Vec<_>>(),
            ));
            let input = change(
                &schema,
                (0..fields)
                    .map(|_| Arc::new(Int64Array::from(vec![None])) as ArrayRef)
                    .collect(),
            );
            let mut fixture = Fixture::new(schema);
            fixture.initialize();
            fixture
                .target
                .write_batch(&input, &batch(&[(0, 1)], &[]))
                .unwrap();
            let matches = fixture
                .target
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    }],
                )
                .unwrap();
            assert_eq!(matches[0].ids, [1]);
            fixture
                .target
                .write_batch(&input, &batch(&[], &[1]))
                .unwrap();
            let matches = fixture
                .target
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        needed: 1,
                        take: 1,
                    }],
                )
                .unwrap();
            assert!(matches[0].ids.is_empty());
        }
    }
}
