use std::path::{Path, PathBuf};

use dogpaddle_store::{DataPlacement, Store};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};
use tempfile::TempDir;

pub const PLACEMENTS: [DataPlacement; 2] = [DataPlacement::Shared, DataPlacement::Dedicated];

pub fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("store")
}

pub fn raw_database(path: &Path) -> Database<NoWriteMap> {
    Database::<NoWriteMap>::open_with_options(
        path,
        DatabaseOptions {
            permissions: Some(0o600),
            max_tables: Some(u64::from(Store::DEDICATED_CAPACITY)),
            exclusive: true,
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap()
}
