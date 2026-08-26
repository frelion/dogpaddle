use std::{collections::HashMap, fmt::Write as _, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt64Array, new_null_array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;

pub fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

pub fn event_change(events: &[(u64, i64)]) -> Change {
    let values = events.iter().map(|(value, _)| *value).collect::<Vec<_>>();
    let diffs = events.iter().map(|(_, diff)| *diff).collect::<Vec<_>>();
    let records =
        RecordBatch::try_new(simple_schema(), vec![Arc::new(UInt64Array::from(values))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs)).unwrap()
}

pub fn events(change: &Change) -> Vec<(u64, i64)> {
    let values = change
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    values
        .values()
        .iter()
        .copied()
        .zip(change.diffs().values().iter().copied())
        .collect()
}

pub fn representative_change() -> Change {
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(Vec::<Option<i64>>::new()),
    ]);
    let object_name = Arc::new(Field::new("name", DataType::Utf8, true));
    let object_score = Arc::new(Field::new("score", DataType::Int64, false));
    let object = StructArray::from(vec![
        (
            Arc::clone(&object_name),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        ),
        (
            Arc::clone(&object_score),
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![7, 7, 8])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(StringArray::from(vec![Some("add"), None, Some("next")])),
        Arc::new(BinaryArray::from(vec![
            Some(b"one".as_slice()),
            Some(b"two".as_slice()),
            Some(b"three".as_slice()),
        ])),
        Arc::new(items),
        Arc::new(object),
        new_null_array(&DataType::Null, 3),
    ];
    let fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("enabled", DataType::Boolean, true),
        Field::new("label", DataType::Utf8, true)
            .with_metadata(HashMap::from([("semantic".to_owned(), "label".to_owned())])),
        Field::new("payload", DataType::Binary, false),
        Field::new("items", columns[4].data_type().clone(), true),
        Field::new(
            "object",
            DataType::Struct(vec![object_name, object_score].into()),
            true,
        ),
        Field::new("nothing", DataType::Null, true),
    ];
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("source".to_owned(), "representative".to_owned())]),
    ));
    let records = RecordBatch::try_new(schema, columns).unwrap();
    Change::try_new(records, Int64Array::from(vec![1, -1, 2])).unwrap()
}

pub fn assert_change_eq(actual: &Change, expected: &Change) {
    assert_eq!(actual.records(), expected.records());
    assert_eq!(actual.diffs(), expected.diffs());
}

pub fn assert_array_buffers_shared(source: &dyn Array, target: &dyn Array) {
    let source = source.to_data();
    let target = target.to_data();
    assert_eq!(source.data_type(), target.data_type());
    assert_eq!(source.buffers().len(), target.buffers().len());
    for (source, target) in source.buffers().iter().zip(target.buffers()) {
        assert_eq!(source.data_ptr(), target.data_ptr());
    }
    match (source.nulls(), target.nulls()) {
        (Some(source), Some(target)) => {
            assert_eq!(source.buffer().data_ptr(), target.buffer().data_ptr());
        }
        (None, None) => {}
        _ => panic!("slice changed the presence of a validity buffer"),
    }
    assert_eq!(source.child_data().len(), target.child_data().len());
    for (source, target) in source.child_data().iter().zip(target.child_data()) {
        let source = arrow_array::make_array(source.clone());
        let target = arrow_array::make_array(target.clone());
        assert_array_buffers_shared(source.as_ref(), target.as_ref());
    }
}

pub fn nested_schema(depth: usize) -> Schema {
    let mut data_type = DataType::Int64;
    for index in 0..depth {
        data_type = DataType::List(Arc::new(Field::new(
            format!("item_{index}"),
            data_type,
            true,
        )));
    }
    Schema::new(vec![Field::new("root", data_type, true)])
}

pub fn hex(encoded: &[u8]) -> String {
    let mut output = String::with_capacity(encoded.len() * 2);
    for byte in encoded {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

pub fn fixture_hex(contents: &str) -> String {
    contents.split_ascii_whitespace().collect()
}
