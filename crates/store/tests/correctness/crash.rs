#![cfg(unix)]

use std::{
    num::NonZeroU64,
    os::unix::process::ExitStatusExt,
    path::Path,
    process::{Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use dogpaddle_store::{Store, SubscribedLog};

use crate::support::{create_byte_map, open_byte_map, store_path};

const WORKER_SCENARIO: &str = "DOGPADDLE_CRASH_SCENARIO";
const WORKER_STORE: &str = "DOGPADDLE_CRASH_STORE";

fn prepare(path: &Path) {
    let mut store = Store::create(path).unwrap();
    create_byte_map(&mut store, "first").unwrap();
    create_byte_map(&mut store, "second").unwrap();
    let log = store.create_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    log.initialize(NonZeroU64::MIN, transaction.access())
        .unwrap();
    transaction.commit().unwrap();
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
    let first = open_byte_map(&store, "first").unwrap();
    let second = open_byte_map(&store, "second").unwrap();
    let log = store.open_data::<SubscribedLog<Vec<u8>>>("log").unwrap();
    let subscription = log.subscription(0);
    let transaction = store.read_transaction();
    let access = transaction.access();
    assert_eq!(
        first.read(access).unwrap().get(&b"key".to_vec()).unwrap(),
        expected.map(<[u8]>::to_vec)
    );
    assert_eq!(
        subscription.peek(access).unwrap(),
        expected.map(|value| (0, value.to_vec()))
    );
    assert_eq!(
        second.read(access).unwrap().get(&b"key".to_vec()).unwrap(),
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
    let first = open_byte_map(&store, "first").unwrap();
    let second = open_byte_map(&store, "second").unwrap();
    let log = store
        .open_data::<SubscribedLog<Vec<u8>>>("log")
        .unwrap()
        .writer();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin();
    first
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"committed".to_vec())
        .unwrap();
    second
        .access(transaction.access())
        .unwrap()
        .put(&b"key".to_vec(), &b"committed".to_vec())
        .unwrap();
    assert!(
        log.try_append(
            &b"committed".to_vec(),
            NonZeroU64::new(1_024).unwrap(),
            transaction.access(),
        )
        .unwrap()
    );

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
