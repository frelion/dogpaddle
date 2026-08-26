use std::path::{Path, PathBuf};

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
