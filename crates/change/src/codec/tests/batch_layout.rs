use std::sync::Arc;

use arrow_array::{Int64Array, NullArray, RecordBatch};
use arrow_ipc::{Buffer as IpcBuffer, FieldNode};
use arrow_schema::{DataType, Field, Schema};

use super::super::{CodecError, SchemaBoundChangeCodec};
use super::support::*;
use crate::Change;

#[test]
pub(super) fn borrowed_and_owned_decoders_validate_all_batch_metadata() {
    let change = layout_change();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    let short_fixed_width = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() - 8);
        8
    });
    let invalid_struct = corrupt_layout(&encoded, &codec, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "object").nodes.start;
        nodes[index] = FieldNode::new(parsed.batch.length() - 1, nodes[index].null_count());
        0
    });
    let invalid_non_nullable = corrupt_layout(&encoded, &codec, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "payload").nodes.start;
        nodes[index] = FieldNode::new(nodes[index].length(), 1);
        0
    });
    let out_of_body = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() + 8);
        0
    });

    let invalid_payload_offset = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "label").buffers.start + 2;
        buffers[index] = IpcBuffer::new(-1, buffers[index].length());
        0
    });
    let invalid_payload_length = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "label").buffers.start + 2;
        buffers[index] = IpcBuffer::new(buffers[index].offset(), -1);
        0
    });
    let payload_out_of_body = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "label").buffers.start + 2;
        buffers[index] = IpcBuffer::new(
            buffers[index].offset(),
            i64::try_from(parsed.body.len()).unwrap() + 8,
        );
        0
    });

    for malformed in [
        short_fixed_width,
        invalid_struct,
        invalid_non_nullable,
        out_of_body,
        invalid_payload_offset,
        invalid_payload_length,
        payload_out_of_body,
    ] {
        assert_both_invalid_encoding(&malformed, &codec);
    }
}

#[test]
pub(super) fn conflicting_metadata_errors_keep_long_field_names_out_of_diagnostics() {
    let name = "x".repeat(64 * 1024);
    let schema = Arc::new(Schema::new(vec![Field::new(&name, DataType::Null, true)]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(1))]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1])).unwrap();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    let (parsed, layout) = parsed_layout(&encoded, &codec);
    let field_node = field_layout(&parsed, &layout, &name).nodes.start;
    let mut nodes = layout.nodes;
    nodes[field_node] = FieldNode::new(-1, 0);
    nodes.push(FieldNode::new(0, 0));
    let buffers = parsed
        .batch
        .buffers()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let malformed = replace_batch_layout(
        &encoded,
        parsed.batch.length(),
        &nodes,
        &buffers,
        parsed.body,
    );

    for result in [
        codec.decode(&malformed),
        codec.decode_owned(malformed.clone()),
    ] {
        let Err(CodecError::InvalidEncoding { message }) = result else {
            panic!("conflicting malformed metadata must be rejected");
        };
        assert!(message.len() <= 256, "layout diagnostics must stay short");
        assert!(!message.contains(&name));
        assert_ne!(message, "Arrow IPC decoding panicked");
    }
}

#[test]
pub(super) fn non_nullable_null_still_requires_an_all_null_field_node() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nothing",
        DataType::Null,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(2))]).unwrap();
    let change = Change::try_new(records, Int64Array::from(vec![1, -1])).unwrap();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    let malformed = corrupt_layout(&encoded, &codec, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "nothing").nodes.start;
        nodes[index] = FieldNode::new(parsed.batch.length(), parsed.batch.length() - 1);
        0
    });

    assert_both_invalid_encoding(&malformed, &codec);
}

#[test]
pub(super) fn temporal_and_decimal_buffer_widths_are_validated() {
    let change = extended_fixed_width_change();
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();

    for name in ["date", "timestamp", "decimal"] {
        let malformed = corrupt_layout(&encoded, &codec, |parsed, layout, _, buffers| {
            let index = field_layout(parsed, layout, name).buffers.start + 1;
            let descriptor = buffers[index];
            buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() - 1);
            0
        });
        assert_both_invalid_encoding(&malformed, &codec);
    }
}

#[test]
pub(super) fn batch_layout_rejects_missing_extra_negative_and_noncanonical_descriptors() {
    let change = simple_change(&[1, -1]);
    let codec = SchemaBoundChangeCodec::try_new(change.schema()).unwrap();
    let encoded = codec.encode(&change).unwrap();
    let (parsed, layout) = parsed_layout(&encoded, &codec);
    let row_count = parsed.batch.length();
    let body = parsed.body.to_vec();
    let nodes = layout.nodes;
    let buffers = parsed
        .batch
        .buffers()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();

    let replace = |nodes: Option<&[FieldNode]>, buffers: Option<&[IpcBuffer]>| {
        let metadata = ipc_batch_metadata(
            row_count,
            nodes,
            buffers,
            i64::try_from(body.len()).unwrap(),
            false,
        );
        replace_batch_message(&encoded, &metadata, &body)
    };

    let mut extra_nodes = nodes.clone();
    extra_nodes.push(FieldNode::new(0, 0));
    let mut extra_buffers = buffers.clone();
    extra_buffers.push(IpcBuffer::new(i64::try_from(body.len()).unwrap(), 0));

    let mut negative_length_node = nodes.clone();
    negative_length_node[0] = FieldNode::new(-1, 0);
    let mut negative_null_count = nodes.clone();
    negative_null_count[0] = FieldNode::new(row_count, -1);
    let mut excessive_null_count = nodes.clone();
    excessive_null_count[0] = FieldNode::new(row_count, row_count + 1);

    let mut negative_offset = buffers.clone();
    negative_offset[0] = IpcBuffer::new(-1, negative_offset[0].length());
    let mut negative_buffer_length = buffers.clone();
    negative_buffer_length[0] = IpcBuffer::new(negative_buffer_length[0].offset(), -1);
    let mut gap = buffers.clone();
    gap[1] = IpcBuffer::new(gap[1].offset() + 8, gap[1].length());
    let mut overlap = buffers.clone();
    overlap[1] = IpcBuffer::new(0, overlap[1].length());

    let mut trailing_body = body.clone();
    trailing_body.extend_from_slice(&[0; 8]);
    let metadata = ipc_batch_metadata(
        row_count,
        Some(&nodes),
        Some(&buffers),
        i64::try_from(trailing_body.len()).unwrap(),
        false,
    );
    let uncovered_body = replace_batch_message(&encoded, &metadata, &trailing_body);

    let malformed = [
        replace(None, Some(&buffers)),
        replace(Some(&nodes), None),
        replace(Some(&nodes[..nodes.len() - 1]), Some(&buffers)),
        replace(Some(&nodes), Some(&buffers[..buffers.len() - 1])),
        replace(Some(&extra_nodes), Some(&buffers)),
        replace(Some(&nodes), Some(&extra_buffers)),
        replace(Some(&negative_length_node), Some(&buffers)),
        replace(Some(&negative_null_count), Some(&buffers)),
        replace(Some(&excessive_null_count), Some(&buffers)),
        replace(Some(&nodes), Some(&negative_offset)),
        replace(Some(&nodes), Some(&negative_buffer_length)),
        replace(Some(&nodes), Some(&gap)),
        replace(Some(&nodes), Some(&overlap)),
        uncovered_body,
    ];
    for encoded in malformed {
        assert_both_invalid_encoding(&encoded, &codec);
    }
}
