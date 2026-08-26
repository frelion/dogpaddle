use dogpaddle_change::{Change, ChangeProjection, decode_change, decode_change_projected};
use dogpaddle_store::{AppendLogAccess, CodecError as StoreCodecError, ScanLimit, StoreError};

pub(crate) fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}

pub(crate) fn scan_raw(
    access: &AppendLogAccess<'_, Vec<u8>>,
    start: u64,
    items: usize,
    bytes: usize,
) -> Vec<(u64, Vec<u8>)> {
    let mut result = Vec::new();
    let progress = access
        .scan(
            start,
            ScanLimit::new(items, bytes).expect("valid test scan limit"),
            |entry| {
                let encoded = entry.project(|bytes| Ok(bytes.to_vec()))?;
                result.push((entry.offset(), encoded));
                Ok::<(), StoreError>(())
            },
        )
        .expect("scan raw Change entries");
    assert!(progress.caught_up);
    result
}

pub(crate) fn scan_changes(
    access: &AppendLogAccess<'_, Vec<u8>>,
    start: u64,
    items: usize,
    bytes: usize,
) -> Vec<(u64, Change)> {
    let mut result = Vec::new();
    let progress = access
        .scan(
            start,
            ScanLimit::new(items, bytes).expect("valid test scan limit"),
            |entry| {
                let change = entry.project(decode_entry)?;
                result.push((entry.offset(), change));
                Ok::<(), StoreError>(())
            },
        )
        .expect("scan decoded Changes");
    assert!(progress.caught_up);
    result
}
