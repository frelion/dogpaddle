use dogpaddle_change::{Change, encode_change};

use crate::wide_change;

/// A deterministic group of complete Change streams plus their logical input.
pub struct EncodedWorkload {
    /// Changes before IPC encoding.
    pub changes: Vec<Change>,
    /// One complete Arrow IPC Stream per Change.
    pub encoded: Vec<Vec<u8>>,
    /// Sum of encoded entry bytes, excluding `AppendLog`'s eight-byte offsets.
    pub encoded_bytes: usize,
    /// Rows carried by every Change.
    pub rows_per_change: usize,
}

impl EncodedWorkload {
    /// Returns the total number of logical rows.
    ///
    /// # Panics
    ///
    /// Panics if this fixture's dimensions were externally changed to
    /// overflow `usize`.
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.rows_per_change
            .checked_mul(self.changes.len())
            .expect("fixture row count fits usize")
    }

    /// Returns the exact logical bytes charged by one `AppendLog` scan.
    ///
    /// # Panics
    ///
    /// Panics if this fixture's dimensions were externally changed to
    /// overflow `usize`.
    #[must_use]
    pub fn scan_bytes(&self) -> usize {
        self.encoded_bytes
            .checked_add(
                self.encoded
                    .len()
                    .checked_mul(size_of::<u64>())
                    .expect("offset byte count fits usize"),
            )
            .expect("scan byte count fits usize")
    }
}

/// Builds a deterministic wide workload.
///
/// # Panics
///
/// Panics for zero dimensions, arithmetic overflow, or an unexpected Change
/// encoding failure.
#[must_use]
pub fn encoded_wide_workload(
    rows_per_change: usize,
    changes: usize,
    payload_bytes: usize,
) -> EncodedWorkload {
    assert!(rows_per_change > 0, "rows per Change must be non-zero");
    assert!(changes > 0, "changes per workload must be non-zero");
    assert!(payload_bytes > 0, "payload width must be non-zero");

    let logical = (0..changes)
        .map(|index| {
            let start = index
                .checked_mul(rows_per_change)
                .and_then(|value| u64::try_from(value).ok())
                .expect("fixture event id fits u64");
            wide_change(start, rows_per_change, payload_bytes)
        })
        .collect::<Vec<_>>();
    let encoded = logical
        .iter()
        .map(|change| encode_change(change).expect("encode valid fixture Change"))
        .collect::<Vec<_>>();
    let encoded_bytes = encoded
        .iter()
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes.len()))
        .expect("encoded workload byte count fits usize");
    EncodedWorkload {
        changes: logical,
        encoded,
        encoded_bytes,
        rows_per_change,
    }
}
