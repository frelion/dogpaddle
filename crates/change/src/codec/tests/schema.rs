use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_ipc::{
    Date as IpcDate, DateArgs, DateUnit as IpcDateUnit, Decimal as IpcDecimal, DecimalArgs,
    DictionaryEncoding, DictionaryEncodingArgs, Endianness, Field as IpcField, FieldArgs,
    FloatingPoint as IpcFloatingPoint, FloatingPointArgs, Int as IpcInt, IntArgs,
    LargeUtf8 as IpcLargeUtf8, LargeUtf8Args, List as IpcList, ListArgs, Message as IpcMessage,
    MessageArgs, MessageHeader, MetadataVersion, Null as IpcNull, NullArgs, Precision,
    Schema as IpcSchema, SchemaArgs, TimeUnit as IpcTimeUnit, Timestamp as IpcTimestamp,
    TimestampArgs, Type as IpcType, reader::StreamReader, writer::IpcWriteOptions,
};
use arrow_schema::{DataType, Field, Schema};
use flatbuffers::FlatBufferBuilder;

use super::super::{CodecError, decode_change, decode_change_projected, encode_change};
use super::support::*;
use crate::{ChangeError, ChangeProjection};

#[derive(Clone, Copy, Debug)]
pub(super) enum MalformedSchemaCase {
    Date64,
    DateMissingTable,
    DateUnknownUnit,
    DecimalBitWidth,
    DecimalMissingTable,
    DecimalPrecisionDoesNotFit,
    DecimalPrecisionTooWide,
    DecimalPrecisionZero,
    DecimalScaleDoesNotFit,
    DecimalScaleGreaterThanPrecision,
    Dictionary,
    FloatMissingTable,
    HalfFloat,
    IntMissingTable,
    IntWidth,
    ListMissingChildren,
    ListTwoChildren,
    NestingTooDeep,
    TimestampEmptyTimezone,
    TimestampMissingTable,
    TimestampUnknownUnit,
    UnsupportedType,
}

pub(super) const MALFORMED_SCHEMA_CASES: &[(MalformedSchemaCase, &str)] = &[
    (MalformedSchemaCase::Date64, "only DAY is supported"),
    (
        MalformedSchemaCase::DateMissingTable,
        "Exactly one of union discriminant",
    ),
    (MalformedSchemaCase::DateUnknownUnit, "unsupported unit"),
    (
        MalformedSchemaCase::DecimalBitWidth,
        "only 128 is supported",
    ),
    (
        MalformedSchemaCase::DecimalMissingTable,
        "Exactly one of union discriminant",
    ),
    (
        MalformedSchemaCase::DecimalPrecisionDoesNotFit,
        "does not fit u8",
    ),
    (
        MalformedSchemaCase::DecimalPrecisionTooWide,
        "invalid precision 39",
    ),
    (
        MalformedSchemaCase::DecimalPrecisionZero,
        "invalid precision 0",
    ),
    (
        MalformedSchemaCase::DecimalScaleDoesNotFit,
        "does not fit i8",
    ),
    (
        MalformedSchemaCase::DecimalScaleGreaterThanPrecision,
        "invalid precision 2 and scale 3",
    ),
    (MalformedSchemaCase::Dictionary, "dictionary encoding"),
    (
        MalformedSchemaCase::FloatMissingTable,
        "Exactly one of union discriminant",
    ),
    (MalformedSchemaCase::HalfFloat, "unsupported precision"),
    (
        MalformedSchemaCase::IntMissingTable,
        "Exactly one of union discriminant",
    ),
    (MalformedSchemaCase::IntWidth, "unsupported bit width"),
    (
        MalformedSchemaCase::ListMissingChildren,
        "has no children vector",
    ),
    (
        MalformedSchemaCase::ListTwoChildren,
        "must have exactly one child",
    ),
    (
        MalformedSchemaCase::NestingTooDeep,
        "Nested table depth limit reached",
    ),
    (
        MalformedSchemaCase::TimestampEmptyTimezone,
        "empty timezone",
    ),
    (
        MalformedSchemaCase::TimestampMissingTable,
        "Exactly one of union discriminant",
    ),
    (
        MalformedSchemaCase::TimestampUnknownUnit,
        "unsupported unit",
    ),
    (
        MalformedSchemaCase::UnsupportedType,
        "unsupported Arrow IPC type",
    ),
];

#[expect(
    clippy::too_many_lines,
    reason = "one table-driven FlatBuffer fixture keeps every malformed schema shape comparable"
)]
pub(super) fn malformed_schema_stream(case: MalformedSchemaCase) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let name = builder.create_string("malformed");
    let field = if matches!(case, MalformedSchemaCase::NestingTooDeep) {
        let leaf_name = builder.create_string("leaf");
        let null_type = IpcNull::create(&mut builder, &NullArgs::default());
        let mut current = IpcField::create(
            &mut builder,
            &FieldArgs {
                name: Some(leaf_name),
                type_type: IpcType::Null,
                type_: Some(null_type.as_union_value()),
                ..FieldArgs::default()
            },
        );
        for _ in 0..=crate::MAX_NESTING_DEPTH {
            let children = builder.create_vector(&[current]);
            let data_type = IpcList::create(&mut builder, &ListArgs::default());
            current = IpcField::create(
                &mut builder,
                &FieldArgs {
                    name: Some(name),
                    type_type: IpcType::List,
                    type_: Some(data_type.as_union_value()),
                    children: Some(children),
                    ..FieldArgs::default()
                },
            );
        }
        current
    } else {
        let (type_type, type_, children, dictionary) = match case {
            MalformedSchemaCase::Date64 => {
                let data_type = IpcDate::create(
                    &mut builder,
                    &DateArgs {
                        unit: IpcDateUnit::MILLISECOND,
                    },
                );
                (IpcType::Date, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::DateMissingTable => (IpcType::Date, None, None, None),
            MalformedSchemaCase::DateUnknownUnit => {
                let data_type = IpcDate::create(
                    &mut builder,
                    &DateArgs {
                        unit: IpcDateUnit(7),
                    },
                );
                (IpcType::Date, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::DecimalBitWidth => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: 38,
                        scale: 0,
                        bitWidth: 256,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::DecimalMissingTable => (IpcType::Decimal, None, None, None),
            MalformedSchemaCase::DecimalPrecisionDoesNotFit => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: i32::MAX,
                        scale: 0,
                        bitWidth: 128,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::DecimalPrecisionTooWide => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: 39,
                        scale: 0,
                        bitWidth: 128,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::DecimalPrecisionZero => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: 0,
                        scale: 0,
                        bitWidth: 128,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::DecimalScaleDoesNotFit => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: 38,
                        scale: i32::MAX,
                        bitWidth: 128,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::DecimalScaleGreaterThanPrecision => {
                let data_type = IpcDecimal::create(
                    &mut builder,
                    &DecimalArgs {
                        precision: 2,
                        scale: 3,
                        bitWidth: 128,
                    },
                );
                (
                    IpcType::Decimal,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::Dictionary => {
                let data_type = IpcNull::create(&mut builder, &NullArgs::default());
                let dictionary =
                    DictionaryEncoding::create(&mut builder, &DictionaryEncodingArgs::default());
                (
                    IpcType::Null,
                    Some(data_type.as_union_value()),
                    None,
                    Some(dictionary),
                )
            }
            MalformedSchemaCase::FloatMissingTable => (IpcType::FloatingPoint, None, None, None),
            MalformedSchemaCase::HalfFloat => {
                let data_type = IpcFloatingPoint::create(
                    &mut builder,
                    &FloatingPointArgs {
                        precision: Precision::HALF,
                    },
                );
                (
                    IpcType::FloatingPoint,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::IntMissingTable => (IpcType::Int, None, None, None),
            MalformedSchemaCase::IntWidth => {
                let data_type = IpcInt::create(
                    &mut builder,
                    &IntArgs {
                        bitWidth: 24,
                        is_signed: true,
                    },
                );
                (IpcType::Int, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::ListMissingChildren => {
                let data_type = IpcList::create(&mut builder, &ListArgs::default());
                (IpcType::List, Some(data_type.as_union_value()), None, None)
            }
            MalformedSchemaCase::ListTwoChildren => {
                let child_name = builder.create_string("child");
                let children = [0, 1].map(|_| {
                    let data_type = IpcNull::create(&mut builder, &NullArgs::default());
                    IpcField::create(
                        &mut builder,
                        &FieldArgs {
                            name: Some(child_name),
                            type_type: IpcType::Null,
                            type_: Some(data_type.as_union_value()),
                            ..FieldArgs::default()
                        },
                    )
                });
                let children = builder.create_vector(&children);
                let data_type = IpcList::create(&mut builder, &ListArgs::default());
                (
                    IpcType::List,
                    Some(data_type.as_union_value()),
                    Some(children),
                    None,
                )
            }
            MalformedSchemaCase::TimestampEmptyTimezone => {
                let timezone = builder.create_string("");
                let data_type = IpcTimestamp::create(
                    &mut builder,
                    &TimestampArgs {
                        unit: IpcTimeUnit::MILLISECOND,
                        timezone: Some(timezone),
                    },
                );
                (
                    IpcType::Timestamp,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::TimestampMissingTable => (IpcType::Timestamp, None, None, None),
            MalformedSchemaCase::TimestampUnknownUnit => {
                let data_type = IpcTimestamp::create(
                    &mut builder,
                    &TimestampArgs {
                        unit: IpcTimeUnit(7),
                        timezone: None,
                    },
                );
                (
                    IpcType::Timestamp,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::UnsupportedType => {
                let data_type = IpcLargeUtf8::create(&mut builder, &LargeUtf8Args::default());
                (
                    IpcType::LargeUtf8,
                    Some(data_type.as_union_value()),
                    None,
                    None,
                )
            }
            MalformedSchemaCase::NestingTooDeep => unreachable!("handled above"),
        };
        IpcField::create(
            &mut builder,
            &FieldArgs {
                name: Some(name),
                type_type,
                type_,
                children,
                dictionary,
                ..FieldArgs::default()
            },
        )
    };
    let fields = builder.create_vector(&[field]);
    let schema = IpcSchema::create(
        &mut builder,
        &SchemaArgs {
            endianness: Endianness::Little,
            fields: Some(fields),
            ..SchemaArgs::default()
        },
    );
    let message = IpcMessage::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            ..MessageArgs::default()
        },
    );
    builder.finish(message, None);
    let mut encoded = frame_ipc_message(builder.finished_data(), &[]);
    encoded.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
    encoded
}

#[test]
fn decoder_rejects_invalid_physical_schema_and_version_markers() {
    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let schemas = [
        unit_physical_schema(HashMap::new()),
        unit_physical_schema(HashMap::from([
            (KIND_KEY.to_owned(), "record".to_owned()),
            (VERSION_KEY.to_owned(), "1".to_owned()),
        ])),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("diff", DataType::Int64, false)],
            marked_metadata(),
        )),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("$dogpaddle.diff", DataType::Int64, true)],
            marked_metadata(),
        )),
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("$dogpaddle.diff", DataType::UInt64, false)],
            marked_metadata(),
        )),
    ];
    for schema in schemas {
        let column: ArrayRef = match schema.field(0).data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(vec![1])),
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![1])),
            data_type => unreachable!("unexpected physical diff type {data_type}"),
        };
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        assert_both_invalid_encoding(&encode_stream(&schema, &[batch]), &projection);
    }

    let mut version = marked_metadata();
    version.insert(VERSION_KEY.to_owned(), "2".to_owned());
    let schema = unit_physical_schema(version);
    let encoded = encode_stream(
        &schema,
        &[unit_physical_batch(
            Arc::clone(&schema),
            Int64Array::from(vec![1]),
        )],
    );
    assert!(matches!(
        decode_change(&encoded),
        Err(CodecError::UnsupportedVersion { version }) if version == "2"
    ));
    assert!(matches!(
        decode_change_projected(&encoded, &projection),
        Err(CodecError::UnsupportedVersion { version }) if version == "2"
    ));

    let mut unknown = marked_metadata();
    unknown.insert("dogpaddle.unknown".to_owned(), "value".to_owned());
    let schema = unit_physical_schema(unknown);
    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    assert_both_invalid_encoding(&encode_stream(&schema, &[batch]), &projection);
}

#[test]
fn decoder_rejects_incomplete_noncanonical_or_unsupported_streams() {
    let change = simple_change(&[-1, 1]);
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let encoded = encode_change(&change).unwrap();
    for end in 0..encoded.len() {
        assert_both_invalid_encoding(&encoded[..end], &projection);
    }

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_both_invalid_encoding(&trailing, &projection);
    assert_both_invalid_encoding(&encoded[4..], &projection);

    let reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
    let batch_offset = usize::try_from(reader.get_ref().position()).unwrap();
    let mut legacy_batch = encoded.clone();
    legacy_batch.drain(batch_offset..batch_offset + 4);
    assert_both_invalid_encoding(&legacy_batch, &projection);

    let schema = unit_physical_schema(marked_metadata());
    let unit_projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    assert_both_invalid_encoding(&encode_stream(&schema, &[]), &unit_projection);
    let batches = [
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1])),
        unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![-1])),
    ];
    assert_both_invalid_encoding(&encode_stream(&schema, &batches), &unit_projection);

    let mut oversized_metadata = encoded.clone();
    oversized_metadata[4..8].copy_from_slice(&(i32::MAX - 7).to_le_bytes());
    assert_both_invalid_encoding(&oversized_metadata, &projection);
    let metadata = ipc_batch_metadata(1, None, None, i64::MAX - 7, false);
    let oversized_body = replace_batch_message(&encoded, &metadata, &[]);
    assert_both_invalid_encoding(&oversized_body, &projection);

    let batch = unit_physical_batch(Arc::clone(&schema), Int64Array::from(vec![1]));
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap();
    assert_both_invalid_encoding(
        &encode_stream_with_options(&schema, &[batch], options),
        &unit_projection,
    );
    assert_both_invalid_encoding(&big_endian_schema_stream(), &unit_projection);
    let compressed =
        replace_batch_message(&encoded, &ipc_batch_metadata(1, None, None, 0, true), &[]);
    assert_both_invalid_encoding(&compressed, &projection);
}

#[test]
fn decoder_rechecks_zero_diff_after_arrow_decoding() {
    let schema = unit_physical_schema(marked_metadata());
    let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
    let encoded = encode_stream(
        &schema,
        &[unit_physical_batch(
            Arc::clone(&schema),
            Int64Array::from(vec![0]),
        )],
    );
    assert!(matches!(
        decode_change(&encoded),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));
    assert!(matches!(
        decode_change_projected(&encoded, &projection),
        Err(CodecError::Change(ChangeError::ZeroDiff { index: 0 }))
    ));

    let empty = unit_physical_batch(Arc::clone(&schema), Int64Array::from(Vec::<i64>::new()));
    assert_both_invalid_encoding(&encode_stream(&schema, &[empty]), &projection);
}
