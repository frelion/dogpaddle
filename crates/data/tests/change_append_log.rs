use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_data::{
    Change, ChangeProjection, decode_change, decode_change_projected, encode_change,
};
use dogpaddle_store::{
    AppendLog, AppendLogAccess, CodecError as StoreCodecError, ScanLimit, Store, StoreError,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]))
}

fn change(schema: SchemaRef, values: &[u64], diffs: &[i64]) -> Change {
    let records =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values.to_vec()))]).unwrap();
    Change::try_new(records, Int64Array::from(diffs.to_vec())).unwrap()
}

fn decode_entry(encoded: &[u8]) -> Result<Change, StoreCodecError> {
    decode_change(encoded).map_err(|error| StoreCodecError::new(error.to_string()))
}

fn decode_projected_entry(
    encoded: &[u8],
    projection: &ChangeProjection,
) -> Result<Change, StoreCodecError> {
    decode_change_projected(encoded, projection)
        .map_err(|error| StoreCodecError::new(error.to_string()))
}

fn scan_changes(access: &AppendLogAccess<'_, Vec<u8>>) -> Vec<(u64, Change)> {
    let mut changes = Vec::new();
    let scan = access
        .scan(0, ScanLimit::new(16, 64 * 1_024).unwrap(), |entry| {
            changes.push((entry.offset(), entry.project(decode_entry)?));
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert!(scan.caught_up);
    changes
}

fn scan_projected_change(
    access: &AppendLogAccess<'_, Vec<u8>>,
    projection: &ChangeProjection,
) -> Change {
    let mut projected = None;
    let scan = access
        .scan(0, ScanLimit::new(1, 64 * 1_024).unwrap(), |entry| {
            projected = Some(entry.project(|encoded| decode_projected_entry(encoded, projection))?);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    assert!(scan.caught_up);
    projected.unwrap()
}

fn assert_change(actual: &Change, expected_values: &[u64], expected_diffs: &[i64]) {
    let values = actual
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.values(), expected_values);
    assert_eq!(actual.diffs().values(), expected_diffs);
}

#[test]
fn append_log_reopen_preserves_offset_then_row_order() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let logical_schema = schema();

    let mut store = Store::create(&path).unwrap();
    let changes: AppendLog<Vec<u8>> = store.create_data("edge/changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut changes = changes.access(transaction.access()).unwrap();
        let first =
            encode_change(&change(logical_schema.clone(), &[7, 8, 7], &[1, 1, -1])).unwrap();
        let second = encode_change(&change(logical_schema, &[8, 9, 9], &[-1, 1, -1])).unwrap();
        assert_eq!(changes.append(&first).unwrap(), 0);
        assert_eq!(changes.append(&second).unwrap(), 1);
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let changes: AppendLog<Vec<u8>> = store.open_data("edge/changes").unwrap();
    let mut transactions = store.into_transactions();
    let transaction = transactions.begin().unwrap();
    let changes = changes.access(transaction.access()).unwrap();
    let scanned_changes = scan_changes(&changes);
    let mut actual = Vec::new();
    for (offset, change) in &scanned_changes {
        let values = change
            .records()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for (row_index, (&value, &diff)) in values
            .values()
            .iter()
            .zip(change.diffs().values())
            .enumerate()
        {
            actual.push((*offset, row_index, value, diff));
        }
    }
    assert_eq!(
        actual,
        [
            (0, 0, 7, 1),
            (0, 1, 8, 1),
            (0, 2, 7, -1),
            (1, 0, 8, -1),
            (1, 1, 9, 1),
            (1, 2, 9, -1),
        ]
    );
}

#[test]
fn one_append_log_entry_supports_independent_owned_projections() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let logical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![7, 8])),
        Arc::new(BinaryArray::from(vec![
            Some(vec![3_u8; 4_096].as_slice()),
            Some(vec![4_u8; 4_096].as_slice()),
        ])),
        Arc::new(UInt64Array::from(vec![70, 80])),
    ];
    let complete = Change::try_new(
        RecordBatch::try_new(Arc::clone(&logical_schema), columns).unwrap(),
        Int64Array::from(vec![1, -1]),
    )
    .unwrap();
    let selected_projection =
        ChangeProjection::try_new(Arc::clone(&logical_schema), [0, 2]).unwrap();
    let diffs_only_projection = ChangeProjection::try_new(logical_schema, []).unwrap();

    let mut store = Store::create(&path).unwrap();
    let changes: AppendLog<Vec<u8>> = store.create_data("output-port/changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        changes
            .access(transaction.access())
            .unwrap()
            .append(&encode_change(&complete).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    drop(transactions);

    let store = Store::open(&path).unwrap();
    let changes: AppendLog<Vec<u8>> = store.open_data("output-port/changes").unwrap();
    let mut transactions = store.into_transactions();
    let selected = {
        let transaction = transactions.begin().unwrap();
        let changes = changes.access(transaction.access()).unwrap();
        assert_eq!(changes.bounds().unwrap(), 0..1);
        scan_projected_change(&changes, &selected_projection)
    };
    let diffs_only = {
        let transaction = transactions.begin().unwrap();
        let changes = changes.access(transaction.access()).unwrap();
        assert_eq!(changes.bounds().unwrap(), 0..1);
        scan_projected_change(&changes, &diffs_only_projection)
    };

    assert_eq!(
        selected.schema(),
        selected_projection.output_schema().clone()
    );
    assert_eq!(selected.diffs().values(), &[1, -1]);
    for (index, expected) in [(0, &[7, 8][..]), (1, &[70, 80][..])] {
        let values = selected
            .records()
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(values.values(), expected);
    }
    assert_eq!(diffs_only.records().num_columns(), 0);
    assert_eq!(diffs_only.num_rows(), 2);
    assert_eq!(diffs_only.diffs().values(), &[1, -1]);
}

#[test]
fn encoded_changes_can_be_forwarded_in_the_same_transaction() {
    let root = tempfile::tempdir().unwrap();
    let logical_schema = schema();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        let mut input = input.access(transaction.access()).unwrap();
        input
            .append(&encode_change(&change(logical_schema.clone(), &[10], &[-1])).unwrap())
            .unwrap();
        input
            .append(&encode_change(&change(logical_schema, &[20], &[3])).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let input = input.access(transaction.access()).unwrap();
        let mut output = output.access(transaction.access()).unwrap();
        input
            .scan(0, ScanLimit::new(16, 64 * 1_024).unwrap(), |entry| {
                let decoded = entry.project(decode_entry)?;
                if decoded
                    .diffs()
                    .iter()
                    .all(|diff| diff.is_some_and(|diff| diff > 0))
                {
                    output.append_entry(&entry)?;
                }
                Ok::<(), StoreError>(())
            })
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    let changes = scan_changes(&output);
    assert_eq!(changes.len(), 1);
    assert_change(&changes[0].1, &[20], &[3]);
}

#[test]
fn dropping_a_forwarding_transaction_rolls_back_the_output() {
    let root = tempfile::tempdir().unwrap();
    let logical_schema = schema();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let input: AppendLog<Vec<u8>> = store.create_data("input").unwrap();
    let output: AppendLog<Vec<u8>> = store.create_data("output").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        input
            .access(transaction.access())
            .unwrap()
            .append(&encode_change(&change(logical_schema, &[42], &[1])).unwrap())
            .unwrap();
        transaction.commit().unwrap();
    }
    {
        let transaction = transactions.begin().unwrap();
        let input = input.access(transaction.access()).unwrap();
        let mut output = output.access(transaction.access()).unwrap();
        input
            .scan(0, ScanLimit::new(1, 64 * 1_024).unwrap(), |entry| {
                output.append_entry(&entry)?;
                Ok::<(), StoreError>(())
            })
            .unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let output = output.access(transaction.access()).unwrap();
    assert_eq!(output.bounds().unwrap(), 0..0);
}

#[test]
fn malformed_change_bytes_poison_the_projection_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = Store::create(root.path().join("store")).unwrap();
    let changes: AppendLog<Vec<u8>> = store.create_data("changes").unwrap();
    let mut transactions = store.into_transactions();
    {
        let transaction = transactions.begin().unwrap();
        changes
            .access(transaction.access())
            .unwrap()
            .append(&b"not a change".to_vec())
            .unwrap();
        transaction.commit().unwrap();
    }

    let transaction = transactions.begin().unwrap();
    let changes = changes.access(transaction.access()).unwrap();
    let error = changes
        .scan(
            0,
            ScanLimit::new(1, 1_024).unwrap(),
            |entry| -> Result<(), StoreError> {
                entry.project(decode_entry)?;
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::Codec(_)));
    assert!(matches!(
        transaction.commit(),
        Err(StoreError::TransactionPoisoned)
    ));
}
