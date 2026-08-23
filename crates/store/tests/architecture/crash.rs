#![cfg(unix)]

use std::{
    os::unix::process::ExitStatusExt,
    path::Path,
    process::{Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use dogpaddle_store::{DataPlacement, Store};

use crate::support::store_path;

const WORKER_SCENARIO: &str = "DOGPADDLE_CRASH_SCENARIO";
const WORKER_STORE: &str = "DOGPADDLE_CRASH_STORE";

fn prepare(path: &Path) {
    let mut store = Store::create(path).unwrap();
    store.create_data("shared", DataPlacement::Shared).unwrap();
    store
        .create_data("dedicated", DataPlacement::Dedicated)
        .unwrap();
}

fn run_worker(path: &Path, scenario: &str) -> ExitStatus {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash::crash_worker")
        .arg("--ignored")
        .arg("--test-threads=1")
        .arg("--quiet")
        .env(WORKER_SCENARIO, scenario)
        .env(WORKER_STORE, path)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("crash worker timed out in scenario {scenario}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_values(path: &Path, expected: Option<&[u8]>) {
    let store = Store::open(path).unwrap();
    let shared = store.open_data("shared").unwrap();
    let dedicated = store.open_data("dedicated").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    assert_eq!(
        shared.access(&transaction).unwrap().get(b"key").unwrap(),
        expected.map(<[u8]>::to_vec)
    );
    assert_eq!(
        dedicated.access(&transaction).unwrap().get(b"key").unwrap(),
        expected.map(<[u8]>::to_vec)
    );
}

#[test]
fn process_sigkill_before_and_after_commit_preserves_the_atomic_boundary() {
    let root = tempfile::tempdir().unwrap();
    let path = store_path(&root);
    prepare(&path);

    let before = run_worker(&path, "before-commit");
    assert_eq!(before.signal(), Some(9));
    assert_values(&path, None);

    let after = run_worker(&path, "after-commit");
    assert_eq!(after.signal(), Some(9));
    assert_values(&path, Some(b"committed"));
}

#[test]
#[ignore = "invoked as a subprocess by the crash test"]
fn crash_worker() {
    let Ok(scenario) = std::env::var(WORKER_SCENARIO) else {
        return;
    };
    let path = std::env::var_os(WORKER_STORE).expect("worker store path");
    let store = Store::open(path).unwrap();
    let shared = store.open_data("shared").unwrap();
    let dedicated = store.open_data("dedicated").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    shared
        .access(&transaction)
        .unwrap()
        .put(b"key", b"committed")
        .unwrap();
    dedicated
        .access(&transaction)
        .unwrap()
        .put(b"key", b"committed")
        .unwrap();

    match scenario.as_str() {
        "before-commit" => kill_self(),
        "after-commit" => {
            transaction.commit().unwrap();
            kill_self();
        }
        _ => panic!("unknown crash scenario: {scenario}"),
    }
}

fn kill_self() -> ! {
    let status = Command::new("/bin/kill")
        .arg("-9")
        .arg(std::process::id().to_string())
        .status()
        .expect("invoke /bin/kill");
    panic!("SIGKILL unexpectedly returned with {status}");
}
