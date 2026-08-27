use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, RecordBatch, RecordBatchOptions, StringArray, StructArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;

use super::{ChangeWorkloadSpec, WorkloadPersona};

pub(super) fn make_change(
    persona: WorkloadPersona,
    event_start: u64,
    spec: ChangeWorkloadSpec,
) -> Change {
    match persona {
        WorkloadPersona::DiffOnlyControl => make_diff_only(spec.rows),
        WorkloadPersona::LayoutV1_16 => make_layout_v1(event_start, spec),
        WorkloadPersona::FixedEvent8 => make_fixed_event(event_start, spec.rows),
        WorkloadPersona::MixedEvent16 => make_mixed_event(event_start, spec),
        WorkloadPersona::WideNumeric64 => make_wide_numeric(event_start, spec.rows),
        WorkloadPersona::BlobEvent4 => make_blob_event(event_start, spec),
        WorkloadPersona::NestedEvent8 => make_nested_event(event_start, spec),
        WorkloadPersona::SlicedMixed16 => make_sliced_mixed(event_start, spec),
        WorkloadPersona::Heterogeneous => {
            unreachable!("heterogeneous resolves to a concrete persona")
        }
    }
}

fn make_diff_only(rows: usize) -> Change {
    let records = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .expect("construct diff-only records");
    make_insert_only_change(records, rows)
}

fn make_fixed_event(event_start: u64, rows: usize) -> Change {
    let ids = ids(event_start, rows);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(UInt64Array::from_iter_values(
            ids.iter().map(|value| value.rotate_left(7)),
        )),
        Arc::new(Int64Array::from_iter_values(ids.iter().map(|value| {
            i64::from_ne_bytes(value.rotate_left(13).to_ne_bytes())
        }))),
        Arc::new(UInt32Array::from_iter_values(
            ids.iter().map(|value| low_u32(*value)),
        )),
        Arc::new(UInt16Array::from_iter_values(
            ids.iter().map(|value| low_u16(*value)),
        )),
        Arc::new(Int32Array::from_iter_values(
            ids.iter()
                .map(|value| i32::from_ne_bytes(low_u32(*value).to_ne_bytes())),
        )),
        Arc::new(Float64Array::from_iter_values(
            ids.iter().map(|value| f64::from(low_u32(*value)) * 0.25),
        )),
        Arc::new(BooleanArray::from(
            ids.iter()
                .map(|value| value.is_multiple_of(2))
                .collect::<Vec<_>>(),
        )),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("version", DataType::Int64, false),
        Field::new("shard", DataType::UInt32, false),
        Field::new("kind", DataType::UInt16, false),
        Field::new("code", DataType::Int32, false),
        Field::new("score", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    make_insert_only_change(
        RecordBatch::try_new(schema, columns).expect("construct fixed-event records"),
        rows,
    )
}

fn make_mixed_event(event_start: u64, spec: ChangeWorkloadSpec) -> Change {
    let ids = ids(event_start, spec.rows);
    let payloads = ids
        .iter()
        .enumerate()
        .map(|(index, value)| {
            (!index.is_multiple_of(5)).then(|| payload(*value, spec.payload_bytes))
        })
        .collect::<BinaryArray>();
    let tokens = ids
        .iter()
        .map(|value| Some(payload(value.rotate_left(19), 8)))
        .collect::<BinaryArray>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(UInt64Array::from_iter_values(
            ids.iter().map(|value| value.rotate_left(5)),
        )),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| (!index.is_multiple_of(7)).then_some(value.is_multiple_of(2)))
                .collect::<BooleanArray>(),
        ),
        Arc::new(Int32Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(11))
                        .then_some(i32::from_ne_bytes(low_u32(*value).to_ne_bytes()))
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(13)).then_some(f64::from(low_u32(*value)) * 0.5)
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(3)).then(|| format!("event-{value:016x}"))
                })
                .collect::<StringArray>(),
        ),
        Arc::new(
            ids.iter()
                .map(|value| Some(format!("category-{}", value % 17)))
                .collect::<StringArray>(),
        ),
        Arc::new(payloads),
        Arc::new(tokens),
        Arc::new(Int64Array::from_iter_values(ids.iter().map(|value| {
            i64::from_ne_bytes(value.rotate_left(23).to_ne_bytes())
        }))),
        Arc::new(UInt32Array::from_iter_values(
            ids.iter().map(|value| low_u32(*value)),
        )),
        Arc::new(Int16Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(9))
                        .then_some(i16::from_ne_bytes(low_u16(*value).to_ne_bytes()))
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float32Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(4)).then_some(f32::from(low_u16(*value)) / 7.0)
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            ids.iter()
                .map(|value| value.is_multiple_of(3))
                .collect::<Vec<_>>(),
        )),
        new_null_array(&DataType::Null, spec.rows),
        Arc::new(UInt64Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| (!index.is_multiple_of(6)).then_some(*value ^ 0xa5a5))
                .collect::<Vec<_>>(),
        )),
    ];
    let schema = mixed_event_schema();
    make_insert_only_change(
        RecordBatch::try_new(schema, columns).expect("construct mixed-event records"),
        spec.rows,
    )
}

fn make_wide_numeric(event_start: u64, rows: usize) -> Change {
    let ids = ids(event_start, rows);
    let mut fields = Vec::with_capacity(64);
    let mut columns = Vec::<ArrayRef>::with_capacity(64);
    for column in 0..64 {
        fields.push(Field::new(
            format!("numeric_{column:02}"),
            DataType::UInt64,
            false,
        ));
        let rotation = u32::try_from(column % 64).expect("rotation fits u32");
        columns.push(Arc::new(UInt64Array::from_iter_values(ids.iter().map(
            |value| value.rotate_left(rotation) ^ u64::try_from(column).expect("column fits u64"),
        ))));
    }
    make_insert_only_change(
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("construct wide-numeric records"),
        rows,
    )
}

fn make_blob_event(event_start: u64, spec: ChangeWorkloadSpec) -> Change {
    let ids = ids(event_start, spec.rows);
    let payload_storage = ids
        .iter()
        .map(|value| payload(*value, spec.payload_bytes))
        .collect::<Vec<_>>();
    let payloads = BinaryArray::from_iter_values(payload_storage.iter().map(Vec::as_slice));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(UInt32Array::from_iter_values(
            ids.iter().map(|value| low_u32(*value) % 31),
        )),
        Arc::new(payloads),
        Arc::new(UInt64Array::from_iter_values(
            ids.iter().map(|value| value.rotate_left(29) ^ 0x9e37_79b9),
        )),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("kind", DataType::UInt32, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("checksum", DataType::UInt64, false),
    ]));
    make_insert_only_change(
        RecordBatch::try_new(schema, columns).expect("construct blob-event records"),
        spec.rows,
    )
}

fn make_nested_event(event_start: u64, spec: ChangeWorkloadSpec) -> Change {
    let ids = ids(event_start, spec.rows);
    let values = ListArray::from_iter_primitive::<Int64Type, _, _>(ids.iter().enumerate().map(
        |(index, value)| {
            if index.is_multiple_of(7) {
                None
            } else if index.is_multiple_of(5) {
                Some(Vec::<Option<i64>>::new())
            } else {
                let value = i64::from_ne_bytes(value.to_ne_bytes());
                Some(vec![
                    Some(value),
                    (!index.is_multiple_of(3)).then_some(-value),
                ])
            }
        },
    ));
    let object_code = Arc::new(Field::new("code", DataType::UInt64, false));
    let object_name = Arc::new(Field::new("name", DataType::Utf8, true));
    let object = StructArray::from(vec![
        (
            Arc::clone(&object_code),
            Arc::new(UInt64Array::from_iter_values(
                ids.iter().map(|value| value.rotate_left(31)),
            )) as ArrayRef,
        ),
        (
            Arc::clone(&object_name),
            Arc::new(
                ids.iter()
                    .enumerate()
                    .map(|(index, value)| {
                        (!index.is_multiple_of(9)).then(|| format!("object-{value}"))
                    })
                    .collect::<StringArray>(),
            ) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(UInt64Array::from_iter_values(
            ids.iter().map(|value| value.rotate_left(5)),
        )),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(4)).then(|| format!("event-{value:016x}"))
                })
                .collect::<StringArray>(),
        ),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(6)).then(|| payload(*value, spec.payload_bytes))
                })
                .collect::<BinaryArray>(),
        ),
        Arc::new(values),
        Arc::new(object),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| (!index.is_multiple_of(8)).then_some(value.is_multiple_of(2)))
                .collect::<BooleanArray>(),
        ),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(10)).then(|| format!("note-{}", value % 101))
                })
                .collect::<StringArray>(),
        ),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("values", columns[4].data_type().clone(), true),
        Field::new(
            "object",
            DataType::Struct(vec![object_code, object_name].into()),
            true,
        ),
        Field::new("active", DataType::Boolean, true),
        Field::new("note", DataType::Utf8, true),
    ]));
    make_insert_only_change(
        RecordBatch::try_new(schema, columns).expect("construct nested-event records"),
        spec.rows,
    )
}

fn make_layout_v1(event_start: u64, spec: ChangeWorkloadSpec) -> Change {
    let ids = ids(event_start, spec.rows);
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>(ids.iter().map(|value| {
        let value = i64::from_ne_bytes(value.to_ne_bytes());
        Some(vec![Some(value), None])
    }));
    let object_code = Arc::new(Field::new("code", DataType::UInt64, false));
    let object_name = Arc::new(Field::new("name", DataType::Utf8, true));
    let object = StructArray::from(vec![
        (
            Arc::clone(&object_code),
            Arc::new(UInt64Array::from_iter_values(ids.iter().copied())) as ArrayRef,
        ),
        (
            Arc::clone(&object_name),
            Arc::new(
                ids.iter()
                    .enumerate()
                    .map(|(index, value)| {
                        (!index.is_multiple_of(5)).then(|| format!("object-{value}"))
                    })
                    .collect::<StringArray>(),
            ) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids.clone())),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| (!index.is_multiple_of(7)).then_some(value.is_multiple_of(2)))
                .collect::<BooleanArray>(),
        ),
        Arc::new(Int8Array::from_iter_values(
            ids.iter().map(|value| low_u8(*value).cast_signed()),
        )),
        Arc::new(UInt8Array::from_iter_values(
            ids.iter().map(|value| low_u8(*value)),
        )),
        Arc::new(Int16Array::from_iter_values(
            ids.iter()
                .map(|value| i16::from_ne_bytes(low_u16(*value).to_ne_bytes())),
        )),
        Arc::new(UInt16Array::from_iter_values(
            ids.iter().map(|value| low_u16(*value)),
        )),
        Arc::new(Int32Array::from_iter_values(
            ids.iter()
                .map(|value| i32::from_ne_bytes(low_u32(*value).to_ne_bytes())),
        )),
        Arc::new(UInt32Array::from_iter_values(
            ids.iter().map(|value| low_u32(*value)),
        )),
        Arc::new(Float32Array::from_iter_values(
            ids.iter().map(|value| f32::from(low_u16(*value)) * 0.25),
        )),
        Arc::new(Int64Array::from_iter_values(
            ids.iter()
                .map(|value| i64::from_ne_bytes(value.to_ne_bytes())),
        )),
        Arc::new(Float64Array::from(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(11)).then_some(f64::from(low_u32(*value)) * 0.5)
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            ids.iter()
                .enumerate()
                .map(|(index, value)| {
                    (!index.is_multiple_of(3)).then(|| format!("event-{value:016x}"))
                })
                .collect::<StringArray>(),
        ),
        Arc::new({
            let storage = ids
                .iter()
                .map(|value| payload(*value, spec.payload_bytes))
                .collect::<Vec<_>>();
            BinaryArray::from_iter_values(storage.iter().map(Vec::as_slice))
        }),
        new_null_array(&DataType::Null, spec.rows),
        Arc::new(items),
        Arc::new(object),
    ];
    let schema = layout_v1_schema(columns[14].data_type().clone(), object_code, object_name);
    make_insert_only_change(
        RecordBatch::try_new(schema, columns).expect("construct layout-v1 records"),
        spec.rows,
    )
}

fn make_sliced_mixed(event_start: u64, spec: ChangeWorkloadSpec) -> Change {
    let source_spec = ChangeWorkloadSpec::new(
        spec.rows
            .checked_add(2)
            .expect("sliced source rows fit usize"),
        spec.payload_bytes,
    );
    make_mixed_event(event_start, source_spec)
        .try_slice(1, spec.rows)
        .expect("slice mixed-event Change")
}

fn make_insert_only_change(records: RecordBatch, rows: usize) -> Change {
    Change::try_new(records, Int64Array::from_value(1, rows))
        .expect("construct insert-only persona Change")
}

fn ids(event_start: u64, rows: usize) -> Vec<u64> {
    (0..rows)
        .map(|index| {
            event_start
                .checked_add(u64::try_from(index).expect("row index fits u64"))
                .expect("event identifiers fit u64")
        })
        .collect()
}

pub(super) fn validate_workload_event_ids(
    persona: WorkloadPersona,
    seed: u64,
    specs: &[ChangeWorkloadSpec],
) {
    let mut event_start = seed;
    for (ordinal, spec) in specs.iter().copied().enumerate() {
        let concrete = persona.concrete_at(ordinal);
        validate_change_event_ids(concrete, event_start, spec);
        if ordinal + 1 < specs.len() {
            event_start = event_start
                .checked_add(u64::try_from(event_span(concrete, spec.rows)).expect("span fits u64"))
                .expect("workload event identifiers fit u64");
        }
    }
}

pub(super) fn validate_change_event_ids(
    persona: WorkloadPersona,
    event_start: u64,
    spec: ChangeWorkloadSpec,
) {
    assert!(spec.rows > 0, "a persisted Change must contain a row");
    let generated_rows = if matches!(persona, WorkloadPersona::SlicedMixed16) {
        spec.rows
            .checked_add(2)
            .expect("sliced source rows fit usize")
    } else if matches!(persona, WorkloadPersona::DiffOnlyControl) {
        return;
    } else {
        spec.rows
    };
    let last_index = generated_rows - 1;
    event_start
        .checked_add(u64::try_from(last_index).expect("row index fits u64"))
        .expect("event identifiers fit u64");
}

pub(super) fn event_span(persona: WorkloadPersona, rows: usize) -> usize {
    if matches!(persona, WorkloadPersona::SlicedMixed16) {
        rows.checked_add(1).expect("sliced event span fits usize")
    } else {
        rows
    }
}

fn mixed_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("active", DataType::Boolean, true),
        Field::new("priority", DataType::Int32, true),
        Field::new("score", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("category", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, true),
        Field::new("token", DataType::Binary, false),
        Field::new("version", DataType::Int64, false),
        Field::new("shard", DataType::UInt32, false),
        Field::new("status", DataType::Int16, true),
        Field::new("ratio", DataType::Float32, true),
        Field::new("flag", DataType::Boolean, false),
        Field::new("nothing", DataType::Null, true),
        Field::new("counter", DataType::UInt64, true),
    ]))
}

fn layout_v1_schema(
    list: DataType,
    object_code: Arc<Field>,
    object_name: Arc<Field>,
) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, false),
        Field::new("u8", DataType::UInt8, false),
        Field::new("i16", DataType::Int16, false),
        Field::new("u16", DataType::UInt16, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("u32", DataType::UInt32, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("i64", DataType::Int64, false),
        Field::new("f64", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, false),
        Field::new("nothing", DataType::Null, true),
        Field::new("items", list, true),
        Field::new(
            "object",
            DataType::Struct(vec![object_code, object_name].into()),
            true,
        ),
    ]))
}

fn payload(seed: u64, bytes: usize) -> Vec<u8> {
    (0..bytes)
        .map(|index| {
            let rotation = u32::try_from(index % 64).expect("rotation fits u32");
            seed.rotate_left(rotation).to_le_bytes()[index % 8]
                ^ u8::try_from(index % 251).expect("payload index fits u8")
        })
        .collect()
}

fn low_u8(value: u64) -> u8 {
    value.to_le_bytes()[0]
}

fn low_u16(value: u64) -> u16 {
    let bytes = value.to_le_bytes();
    u16::from_le_bytes([bytes[0], bytes[1]])
}

fn low_u32(value: u64) -> u32 {
    let bytes = value.to_le_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
