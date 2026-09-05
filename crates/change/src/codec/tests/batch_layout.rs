use arrow_ipc::{Buffer as IpcBuffer, FieldNode};

use super::super::encode_change;
use super::support::*;
use crate::ChangeProjection;

#[test]
fn both_decoders_validate_all_unselected_batch_metadata() {
    let change = layout_change();
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), [0]).unwrap();
    let short_fixed_width = corrupt_layout(&encoded, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() - 8);
        8
    });
    let invalid_struct = corrupt_layout(&encoded, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "object").nodes.start;
        nodes[index] = FieldNode::new(parsed.batch.length() - 1, nodes[index].null_count());
        0
    });
    let invalid_non_nullable = corrupt_layout(&encoded, |parsed, layout, nodes, _| {
        let index = field_layout(parsed, layout, "payload").nodes.start;
        nodes[index] = FieldNode::new(nodes[index].length(), 1);
        0
    });
    let out_of_body = corrupt_layout(&encoded, |parsed, layout, _, buffers| {
        let index = field_layout(parsed, layout, "object").buffers.end - 1;
        let descriptor = buffers[index];
        buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() + 8);
        0
    });

    for malformed in [
        short_fixed_width,
        invalid_struct,
        invalid_non_nullable,
        out_of_body,
    ] {
        assert_both_invalid_encoding(&malformed, &projection);
    }
}

#[test]
fn temporal_and_decimal_buffer_widths_are_validated_even_when_unselected() {
    let change = extended_fixed_width_change();
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), []).unwrap();

    for name in ["date", "timestamp", "decimal"] {
        let malformed = corrupt_layout(&encoded, |parsed, layout, _, buffers| {
            let index = field_layout(parsed, layout, name).buffers.start + 1;
            let descriptor = buffers[index];
            buffers[index] = IpcBuffer::new(descriptor.offset(), descriptor.length() - 1);
            0
        });
        assert_both_invalid_encoding(&malformed, &projection);
    }
}

#[test]
fn batch_layout_rejects_missing_extra_negative_and_noncanonical_descriptors() {
    let change = simple_change(&[1, -1]);
    let encoded = encode_change(&change).unwrap();
    let projection = ChangeProjection::try_new(change.schema(), []).unwrap();
    let (parsed, layout) = parsed_layout(&encoded);
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

    let malformed = [
        replace(None, Some(&buffers)),
        replace(Some(&nodes), None),
        replace(Some(&extra_nodes), Some(&buffers)),
        replace(Some(&nodes), Some(&extra_buffers)),
        replace(Some(&negative_length_node), Some(&buffers)),
        replace(Some(&negative_null_count), Some(&buffers)),
        replace(Some(&excessive_null_count), Some(&buffers)),
        replace(Some(&nodes), Some(&negative_offset)),
        replace(Some(&nodes), Some(&negative_buffer_length)),
        replace(Some(&nodes), Some(&gap)),
        replace(Some(&nodes), Some(&overlap)),
    ];
    for encoded in malformed {
        assert_both_invalid_encoding(&encoded, &projection);
    }
}
