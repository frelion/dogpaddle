use std::{num::NonZeroU64, sync::Arc};

use super::{
    super::{
        ACTIVE_INPUT_KEY, CURSOR_ORIGIN, cursor_key, decode_active_input, decode_cursor,
        encode_active_input, encode_cursor,
    },
    support::{count_schema, scan_count_sink, value_schema},
};

#[test]
fn input_state_keys_and_values_keep_the_v1_encoding() {
    assert_eq!(ACTIVE_INPUT_KEY, b"input/active");
    assert_eq!(CURSOR_ORIGIN, 0);
    assert_eq!(encode_active_input(0x0102_0304), [1, 2, 3, 4]);
    assert_eq!(decode_active_input(&[1, 2, 3, 4]), Some(0x0102_0304));
    assert_eq!(decode_active_input(&[1, 2, 3]), None);
    assert_eq!(cursor_key(0x0102_0304), b"input/01020304/cursor");
    assert_eq!(
        encode_cursor(0x0102_0304_0506_0708),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        decode_cursor(&[1, 2, 3, 4, 5, 6, 7, 8]),
        Some(0x0102_0304_0506_0708)
    );
    assert_eq!(decode_cursor(&[1, 2, 3, 4]), None);
}

#[test]
fn assembly_shares_each_unified_output_with_its_input_ports() {
    let fixture = scan_count_sink(NonZeroU64::MAX, NonZeroU64::MAX);
    assert!(fixture.stations[0].inbox.ports().is_empty());
    assert_eq!(fixture.stations[1].inbox.ports().len(), 1);
    assert_eq!(fixture.stations[2].inbox.ports().len(), 1);
    assert!(fixture.stations[2].output.is_none());
    assert!(Arc::ptr_eq(
        fixture.stations[0].output.as_ref().unwrap(),
        fixture.stations[1].inbox.ports()[0].output(),
    ));
    assert!(Arc::ptr_eq(
        fixture.stations[1].output.as_ref().unwrap(),
        fixture.stations[2].inbox.ports()[0].output(),
    ));
    assert_eq!(
        fixture.stations[0].output.as_ref().unwrap().schema(),
        &value_schema()
    );
    assert_eq!(
        fixture.stations[1].output.as_ref().unwrap().schema(),
        &count_schema()
    );
}
