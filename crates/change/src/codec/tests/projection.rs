use std::sync::Arc;

use arrow_array::{BinaryArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use super::super::{CodecError, decode_change, decode_change_projected, encode_change};
use super::support::*;
use crate::{Change, ChangeProjection, ProjectionError};

const OFFSETS_BUFFER: usize = 1;
const VARIABLE_VALUES_BUFFER: usize = 2;

#[test]
fn projected_body_omits_a_large_unselected_binary_field() {
    let huge = vec![7_u8; 64 * 1_024];
    let schema = Arc::new(Schema::new(vec![
        Field::new("head", DataType::UInt64, false),
        Field::new("huge", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![10])),
            Arc::new(BinaryArray::from(vec![Some(huge.as_slice())])),
            Arc::new(UInt64Array::from(vec![20])),
        ],
    )
    .unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let projection = ChangeProjection::try_new(schema, [0, 2]).unwrap();
    let encoded = encode_change(&change).unwrap();
    let (parsed, layout) = parsed_layout(&encoded);
    let compact = layout.compact(parsed.body, &projection).unwrap();

    assert!(compact.body.len() * 100 < parsed.body.len());
    assert_change_eq(
        &decode_change_projected(&encoded, &projection).unwrap(),
        &change.try_project(&projection).unwrap(),
    );
}

#[test]
fn projected_decode_skips_only_unselected_utf8_value_validation() {
    let change = layout_change();
    let mut encoded = encode_change(&change).unwrap();
    let values = field_buffer_range(&encoded, "label", VARIABLE_VALUES_BUFFER);
    encoded[values.start] = 0xff;

    let schema = change.schema();
    let mut metadata = schema.metadata().clone();
    metadata.insert("schema-drift".to_owned(), "true".to_owned());
    let drifted = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    let label = drifted.index_of("label").unwrap();
    let drifted = ChangeProjection::try_new(drifted, [label]).unwrap();
    assert!(matches!(
        decode_change_projected(&encoded, &drifted),
        Err(CodecError::Projection(ProjectionError::SchemaMismatch))
    ));

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &keep_id).unwrap(),
        &change.try_project(&keep_id).unwrap(),
    );
    let label = change.schema().index_of("label").unwrap();
    let select_label = ChangeProjection::try_new(change.schema(), [label]).unwrap();
    assert_arrow_error(&decode_change_projected(&encoded, &select_label));
    assert_arrow_error(&decode_change(&encoded));
}

#[test]
fn projected_decode_skips_only_unselected_list_offset_validation() {
    let change = layout_change();
    let mut encoded = encode_change(&change).unwrap();
    let offsets = field_buffer_range(&encoded, "items", OFFSETS_BUFFER);
    let last = offsets.start + change.num_rows() * size_of::<i32>();
    encoded[last..last + size_of::<i32>()].copy_from_slice(&i32::MAX.to_le_bytes());

    let keep_id = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    assert_change_eq(
        &decode_change_projected(&encoded, &keep_id).unwrap(),
        &change.try_project(&keep_id).unwrap(),
    );
    let items = change.schema().index_of("items").unwrap();
    let select_items = ChangeProjection::try_new(change.schema(), [items]).unwrap();
    assert_arrow_error(&decode_change_projected(&encoded, &select_items));
    assert_arrow_error(&decode_change(&encoded));
}
