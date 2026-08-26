use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// Owns an isolated filesystem location for a Store integration scenario.
pub struct StoreFixture {
    _root: TempDir,
    path: PathBuf,
}

impl StoreFixture {
    /// Creates a fresh path whose final Store directory does not yet exist.
    ///
    /// # Panics
    ///
    /// Panics when the temporary parent directory cannot be created.
    #[must_use]
    pub fn new() -> Self {
        let root = tempfile::tempdir().expect("create temporary Store parent");
        let path = root.path().join("store");
        Self { _root: root, path }
    }

    /// Returns the Store directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Default for StoreFixture {
    fn default() -> Self {
        Self::new()
    }
}
