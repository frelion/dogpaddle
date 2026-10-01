use std::process::{Command, exit};

mod batch_layout;
mod framing;
mod support;
mod values;

const PANIC_PROBE: &str = "DOGPADDLE_CHANGE_MALFORMED_BATCH_PANIC_PROBE";
const PROBE_COMPLETED: &str = "dogpaddle-change malformed batch probe completed";

#[test]
fn malformed_batches_do_not_invoke_the_panic_hook() {
    // Isolate the process-wide hook from tests running concurrently.
    if std::env::var_os(PANIC_PROBE).is_some() {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| exit(86)));
        batch_layout::borrowed_and_owned_decoders_validate_all_batch_metadata();
        batch_layout::non_nullable_null_still_requires_an_all_null_field_node();
        batch_layout::temporal_and_decimal_buffer_widths_are_validated();
        batch_layout::batch_layout_rejects_missing_extra_negative_and_noncanonical_descriptors();
        std::panic::set_hook(previous_hook);
        println!("{PROBE_COMPLETED}");
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "codec::tests::malformed_batches_do_not_invoke_the_panic_hook",
            "--nocapture",
        ])
        .env(PANIC_PROBE, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "malformed batch probe exited with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(PROBE_COMPLETED));
}
