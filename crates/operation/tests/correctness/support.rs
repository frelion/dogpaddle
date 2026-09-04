use std::path::{Path, PathBuf};

use dogpaddle_operation::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, Turn,
};
use dogpaddle_store::{TransactionAccess, Transactions};
use tempfile::TempDir;

pub struct TestStore {
    _root: TempDir,
    path: PathBuf,
}

impl TestStore {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("store");
        Self { _root: root, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn apply_ready<'turn>(
    turn: Turn<'turn>,
    access: TransactionAccess<'_>,
) -> Result<(Action, AfterCommit<'turn>), OperationError> {
    match turn {
        Turn::Ready(prepared) => prepared.apply(access),
        Turn::Idle => panic!("a transactional built-in Operation returned an outer idle turn"),
    }
}

pub fn commit_ready<O>(
    operation: &mut O,
    input: Option<OperationInput<'_>>,
    transactions: &mut Transactions,
) -> Result<Action, OperationError>
where
    O: Operation + ?Sized,
{
    let turn = operation.turn(input)?;
    let transaction = transactions.begin()?;
    let (action, after_commit) = apply_ready(turn, transaction.access())?;
    if matches!(&action, Action::Idle) {
        drop(transaction);
        drop(after_commit);
        return Ok(action);
    }
    transaction.commit()?;
    after_commit
        .run()
        .map_err(|error| Box::new(error) as OperationError)?;
    Ok(action)
}

pub fn rollback_ready<O>(
    operation: &mut O,
    input: Option<OperationInput<'_>>,
    transactions: &mut Transactions,
) -> Result<Action, OperationError>
where
    O: Operation + ?Sized,
{
    let turn = operation.turn(input)?;
    let transaction = transactions.begin()?;
    let (action, after_commit) = apply_ready(turn, transaction.access())?;
    drop(transaction);
    drop(after_commit);
    Ok(action)
}

pub fn decode_hex(encoded: &str) -> Vec<u8> {
    let digits = encoded
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(digits.len() % 2, 0, "hex fixture has an odd digit count");
    digits
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        b'A'..=b'F' => digit - b'A' + 10,
        _ => panic!("invalid hex digit {digit:?}"),
    }
}
