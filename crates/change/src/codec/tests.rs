use std::{
    process::{Command, exit},
    sync::Arc,
};

use arrow_schema::Schema;

use super::{CodecError, decode_change, decode_change_projected, stream};
use crate::ChangeProjection;

mod batch_layout;
mod projection;
mod schema;
mod support;

use schema::{MALFORMED_SCHEMA_CASES, malformed_schema_stream};
use support::assert_invalid_encoding_without_decoder_panic;

const MALFORMED_SCHEMA_PANIC_PROBE: &str = "DOGPADDLE_CHANGE_MALFORMED_SCHEMA_PANIC_PROBE";
const PANIC_HOOK_EXIT_CODE: i32 = 86;
const DECODE_PANIC_MESSAGE: &str = "Arrow IPC decoding panicked";
const PANIC_PROBE_COMPLETED: &str = "dogpaddle-change malformed decoder probe completed";

#[test]
fn decoder_rejects_malformed_schema_without_invoking_the_panic_hook() {
    // Isolate the process-wide hook from other tests that may run concurrently.
    if std::env::var_os(MALFORMED_SCHEMA_PANIC_PROBE).is_some() {
        let projection = ChangeProjection::try_new(Arc::new(Schema::empty()), []).unwrap();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| exit(PANIC_HOOK_EXIT_CODE)));

        for &(case, expected) in MALFORMED_SCHEMA_CASES {
            let encoded = malformed_schema_stream(case);
            let Err(CodecError::InvalidEncoding { message }) = stream::parse(&encoded) else {
                panic!("{case:?} did not return InvalidEncoding");
            };
            assert!(
                message.contains(expected),
                "{case:?} returned {message:?}, expected a diagnostic containing {expected:?}"
            );
            assert_ne!(message, DECODE_PANIC_MESSAGE, "decoder panic was caught");
            assert_invalid_encoding_without_decoder_panic(decode_change(&encoded));
            assert_invalid_encoding_without_decoder_panic(decode_change_projected(
                &encoded,
                &projection,
            ));
        }

        std::panic::set_hook(previous_hook);
        println!("{PANIC_PROBE_COMPLETED}");
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "codec::tests::decoder_rejects_malformed_schema_without_invoking_the_panic_hook",
            "--nocapture",
        ])
        .env(MALFORMED_SCHEMA_PANIC_PROBE, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "malformed Schema decoder probe exited with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(PANIC_PROBE_COMPLETED),
        "malformed Schema decoder probe did not execute the child test\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
