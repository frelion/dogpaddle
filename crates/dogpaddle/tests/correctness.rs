#![cfg(unix)]

use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
fn run_builds_and_reopens_the_default_state_until_ctrl_c() {
    let root = tempfile::tempdir().unwrap();
    let sql_directory = root.path().join("jobs");
    fs::create_dir(&sql_directory).unwrap();
    let sql_path = sql_directory.join("orders.sql");
    fs::write(
        &sql_path,
        "INSERT INTO discard() SELECT value FROM sequence(start => 18446744073709551615)",
    )
    .unwrap();
    let state_path = sql_directory.join(".dogpaddle/orders");

    let expected_output = format!(
        "state: {}\n",
        fs::canonicalize(&sql_directory)
            .unwrap()
            .join(".dogpaddle/orders")
            .display()
    );
    for attempt in 0..2 {
        let stdout_path = root.path().join(format!("run-{attempt}.stdout"));
        let stdout = fs::File::create(&stdout_path).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_dogpaddle"))
            .arg("run")
            .arg(&sql_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        wait_for_output(&mut child, &stdout_path, &expected_output);
        send_interrupt(child.id());
        let output = wait_for_exit(child);
        assert!(
            output.status.success(),
            "dogpaddle failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(stdout_path).unwrap(), expected_output);
    }
    assert!(state_path.is_dir());
}

#[test]
fn invalid_invocation_reports_the_single_command_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_dogpaddle"))
        .arg("status")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("usage: dogpaddle run SQL_FILE [--state DIR]"));
}

fn wait_for_output(child: &mut Child, path: &Path, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "dogpaddle exited early"
        );
        if fs::read_to_string(path).unwrap() == expected {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    panic!("dogpaddle did not report its state path");
}

fn send_interrupt(pid: u32) {
    assert!(
        Command::new("kill")
            .arg("-INT")
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success()
    );
}

fn wait_for_exit(mut child: Child) -> Output {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    child.wait_with_output().unwrap()
}
