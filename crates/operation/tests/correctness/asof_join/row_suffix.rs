use super::*;
use arrow_array::RecordBatchOptions;
use dogpaddle_operation::lit;
use dogpaddle_store::StoreValue;
use std::borrow::Cow;

#[test]
fn empty_canonical_rows_obey_all_four_neighbor_bounds_after_reopen() {
    for direction in [
        AsOfDirection::Backward { allow_exact: false },
        AsOfDirection::Backward { allow_exact: true },
        AsOfDirection::Forward { allow_exact: false },
        AsOfDirection::Forward { allow_exact: true },
    ] {
        let root = TestStore::new();
        let schema = Arc::new(Schema::empty());
        let definition = AsOfJoinDefinition::try_new(
            direction,
            [],
            AsOfOrderKey::new(lit(10_i64), lit(10_i64)),
            std::iter::empty::<&str>(),
        )
        .unwrap();
        let construct = |scope: &mut dogpaddle_store::DataScope<'_>| {
            OperationDefinition::from(definition.clone())
                .construct(
                    &[Arc::clone(&schema), Arc::clone(&schema)],
                    &mut scope.scoped("operation"),
                    RuntimeResource::none(),
                )
                .unwrap()
                .into_parts()
                .0
        };
        let mut setup = StoreSetup::new();
        let mut operation = construct(&mut setup.data_scope());
        let mut transactions = setup.commit(root.path(), |_| Ok(())).unwrap();
        let input = |diff| {
            Change::try_new(
                RecordBatch::try_new_with_options(
                    Arc::clone(&schema),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                )
                .unwrap(),
                Int64Array::from(vec![diff]),
            )
            .unwrap()
        };
        let run = |operation: &mut Operation, transactions: &mut Transactions, port, diff| {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port,
                        change: &input(diff),
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 4 * 1024 * 1024),
                )
                .unwrap();
            assert_eq!(step.progress, Progress::Done);
            let differences = step.output.map_or_else(Vec::new, |change| {
                assert_eq!(change.records().num_columns(), 0);
                change.diffs().values().to_vec()
            });
            transaction.commit().unwrap();
            differences
        };
        assert_eq!(run(&mut operation, &mut transactions, 0, 1), [1]);
        let corrections = match direction {
            AsOfDirection::Backward { allow_exact: true }
            | AsOfDirection::Forward { allow_exact: true } => vec![-1, 1],
            _ => vec![],
        };
        assert_eq!(run(&mut operation, &mut transactions, 1, 1), corrections);
        drop((operation, transactions));
        let store = Store::open(root.path()).unwrap();
        operation = construct(&mut store.data_scope());
        transactions = store.into_transactions();
        assert!(run(&mut operation, &mut transactions, 1, 1).is_empty());
        assert!(run(&mut operation, &mut transactions, 1, -1).is_empty());
        assert_eq!(run(&mut operation, &mut transactions, 1, -1), corrections);
    }
}

#[test]
fn malformed_chosen_row_suffix_is_rejected_without_repair_or_progress() {
    for truncate in [false, true] {
        let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
        fixture.run(1, &change(&[((Some(1), Some(10), 4), 1)]), 1);
        let (key, value) = fixture.right_rows().pop().unwrap();
        let mut malformed = key.clone();
        if truncate {
            malformed.pop().unwrap();
        } else {
            malformed.push(0);
        }
        {
            let transaction = fixture.transactions.begin();
            let mut rows = fixture.raw_right.access(transaction.access()).unwrap();
            assert!(rows.remove(&key).unwrap());
            rows.put(&malformed, &value).unwrap();
            transaction.commit().unwrap();
        }
        let input = change(&[((Some(1), Some(20), 8), 1)]);
        for _ in 0..2 {
            assert!(fixture.page(0, &input, 1, true).is_err());
            assert_eq!(fixture.right_rows(), [(malformed.clone(), value.clone())]);
            {
                let transaction = fixture.transactions.begin();
                assert!(
                    fixture
                        .frame
                        .access(transaction.access())
                        .unwrap()
                        .get()
                        .unwrap()
                        .is_none()
                );
            }
            fixture = fixture.reopen();
        }
    }
}

#[test]
fn malformed_resume_row_suffix_preserves_the_committed_page_after_reopen() {
    for truncate in [false, true] {
        let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
        fixture.run(
            0,
            &change(&[((Some(1), Some(20), 8), 1), ((Some(1), Some(30), 9), 1)]),
            1,
        );
        let input = change(&[((Some(1), Some(10), 4), 1)]);
        let first = fixture.page(1, &input, 1, true).unwrap();
        let Progress::More(resume) = first.progress else {
            panic!("expected correction continuation")
        };
        let json = serde_json::to_value(&resume).unwrap();
        let mut key: Vec<u8> =
            serde_json::from_value(json["cursor"]["AsOf"]["left_resume_after"].clone()).unwrap();
        let config = bincode::config::standard()
            .with_big_endian()
            .with_variable_int_encoding();
        let old_tail = bincode::encode_to_vec(&key, config).unwrap();
        let encoded = resume.encode_value().unwrap();
        assert!(encoded.as_ref().ends_with(&old_tail));
        let mut encoded = encoded.as_ref()[..encoded.as_ref().len() - old_tail.len()].to_vec();
        if truncate {
            key.pop().unwrap();
        } else {
            key.push(0);
        }
        encoded.extend(bincode::encode_to_vec(&key, config).unwrap());
        let malformed = Resume::decode_value(Cow::Owned(encoded)).unwrap();
        {
            let transaction = fixture.transactions.begin();
            fixture
                .frame
                .access(transaction.access())
                .unwrap()
                .set(&malformed)
                .unwrap();
            transaction.commit().unwrap();
        }
        fixture = fixture.reopen();
        assert!(fixture.page(1, &input, 1, true).is_err());
        assert!(fixture.right_rows().is_empty());
        let transaction = fixture.transactions.begin();
        assert_eq!(
            fixture
                .frame
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            Some(malformed)
        );
    }
}

#[test]
fn retired_three_frame_layout_is_rejected_read_only_even_when_empty() {
    let mut current = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    current.run(1, &change(&[((Some(1), Some(10), 4), 1)]), 1);
    let (key, value) = current.right_rows().pop().unwrap();
    let mut header_end = 0;
    for _ in 0..2 {
        header_end += key[header_end..]
            .windows(2)
            .position(|pair| pair == [0, 0])
            .unwrap()
            + 2;
    }
    let mut retired_key = key[..header_end].to_vec();
    for byte in &key[header_end..] {
        retired_key.push(*byte);
        if *byte == 0 {
            retired_key.push(255);
        }
    }
    retired_key.extend_from_slice(&[0, 0]);
    drop(current);
    for populated in [false, true] {
        let root = TestStore::new();
        let mut setup = StoreSetup::new();
        let left = setup
            .create_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/asof_join.left_rows")
            .unwrap();
        let right = setup
            .create_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/asof_join.right_rows")
            .unwrap();
        let transactions = setup
            .commit(root.path(), |access| {
                if populated {
                    right.access(access)?.put(&retired_key, &value)?;
                }
                Ok(())
            })
            .unwrap();
        drop((left, right, transactions));
        for _ in 0..2 {
            let store = Store::open(root.path()).unwrap();
            assert!(
                OperationDefinition::from(definition(AsOfDirection::Backward {
                    allow_exact: true
                }))
                .construct(
                    &[schema(), schema()],
                    &mut store.data_scope().scoped("operation"),
                    RuntimeResource::none()
                )
                .is_err()
            );
            assert!(
                store
                    .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/asof_join.left_index")
                    .is_err()
            );
            assert!(
                store
                    .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/asof_join.right_index")
                    .is_err()
            );
            let right = store
                .open_data::<OrderedMap<Vec<u8>, Vec<u8>>>("operation/asof_join.right_rows")
                .unwrap();
            let mut transactions = store.into_transactions();
            let transaction = transactions.begin();
            let page = right
                .access(transaction.access())
                .unwrap()
                .scan(
                    ..,
                    ScanDirection::Ascending,
                    None,
                    ScanLimit::new(2, 4096).unwrap(),
                )
                .unwrap();
            let expected = if populated {
                vec![(retired_key.clone(), value.clone())]
            } else {
                vec![]
            };
            assert_eq!(page.entries, expected);
        }
    }
}

#[test]
fn output_nullability_cannot_mask_a_null_in_a_nonnullable_source_field() {
    let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
    fixture.run(1, &change(&[((Some(1), Some(10), 4), 1)]), 1);
    let (key, value) = fixture.right_rows().pop().unwrap();
    // The final source field is nonnullable Int64: marker 1 and eight bytes.
    // Its outer output field is nullable, but that cannot validate stored input.
    assert_eq!(&key[key.len() - 9..], &[1, 0, 0, 0, 0, 0, 0, 0, 4]);
    let mut malformed = key[..key.len() - 9].to_vec();
    malformed.push(0);
    {
        let transaction = fixture.transactions.begin();
        let mut rows = fixture.raw_right.access(transaction.access()).unwrap();
        assert!(rows.remove(&key).unwrap());
        rows.put(&malformed, &value).unwrap();
        transaction.commit().unwrap();
    }
    let input = change(&[((Some(1), Some(20), 8), 1)]);
    for _ in 0..2 {
        let error = fixture.page(0, &input, 256, true).unwrap_err();
        let mut cause: &(dyn std::error::Error + 'static) = error.as_ref();
        while let Some(source) = cause.source() {
            cause = source;
        }
        assert!(cause.to_string().contains("non-nullable field \"id\""));
        assert_eq!(fixture.right_rows(), [(malformed.clone(), value.clone())]);
        {
            let transaction = fixture.transactions.begin();
            assert!(
                fixture
                    .frame
                    .access(transaction.access())
                    .unwrap()
                    .get()
                    .unwrap()
                    .is_none()
            );
            assert!(
                fixture
                    .raw_left
                    .access(transaction.access())
                    .unwrap()
                    .scan(
                        ..,
                        ScanDirection::Ascending,
                        None,
                        ScanLimit::new(1, 4096).unwrap(),
                    )
                    .unwrap()
                    .entries
                    .is_empty()
            );
        }
        fixture = fixture.reopen();
    }
}

#[test]
fn a_multi_event_right_input_keeps_each_left_old_new_correction_in_order() {
    let old = (Some(1), Some(10), 10);
    let first = (Some(1), Some(15), 15);
    let last = (Some(1), Some(18), 18);
    let left_a = (Some(1), Some(20), 101);
    let left_b = (Some(1), Some(30), 102);
    let expected = vec![
        ((left_a, Some(old)), -2),
        ((left_a, Some(first)), 2),
        ((left_b, Some(old)), -3),
        ((left_b, Some(first)), 3),
        ((left_a, Some(first)), -2),
        ((left_a, Some(old)), 2),
        ((left_b, Some(first)), -3),
        ((left_b, Some(old)), 3),
        ((left_a, Some(old)), -2),
        ((left_a, Some(last)), 2),
        ((left_b, Some(old)), -3),
        ((left_b, Some(last)), 3),
    ];
    for items in [1, 256] {
        let mut fixture = Fixture::new(AsOfDirection::Backward { allow_exact: true });
        fixture.run(1, &change(&[(old, 1)]), items);
        fixture.run(0, &change(&[(left_a, 2), (left_b, 3)]), items);
        fixture = fixture.reopen();
        let input = change(&[(first, 1), (first, -1), (last, 1)]);
        let mut trace = Vec::new();
        loop {
            let step = fixture.page(1, &input, items, true).unwrap();
            if let Some(change) = step.output {
                trace.extend(output(&change));
            }
            let done = step.progress == Progress::Done;
            fixture = fixture.reopen();
            if done {
                break;
            }
        }
        assert_eq!(trace, expected, "items={items}");
        assert_eq!(fixture.right_rows().len(), 2);
    }
}

fn nested_list_event(
    schema: &Arc<Schema>,
    item: &Arc<Field>,
    data_type: &DataType,
    at: i64,
    length: i32,
) -> Change {
    use arrow_array::{ArrayRef, ListArray, NullArray};
    use arrow_buffer::OffsetBuffer;
    let length_usize = usize::try_from(length).unwrap();
    let values: ArrayRef = if *data_type == DataType::Null {
        Arc::new(NullArray::new(length_usize))
    } else {
        Arc::new(UInt64Array::from(vec![7; length_usize]))
    };
    Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![at])),
                Arc::new(ListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::new(vec![0, length].into()),
                    values,
                    None,
                )),
            ],
        )
        .unwrap(),
        Int64Array::from(vec![1]),
    )
    .unwrap()
}

fn assert_nested_winner(output: &Change, data_type: &DataType) {
    use arrow_array::ListArray;
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.diffs().value(0), 1);
    let list = output
        .records()
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 128 * 1024);
    assert_eq!(list.values().data_type(), data_type);
    if *data_type == DataType::UInt64 {
        assert!(
            list.values()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .all(|value| *value == 7)
        );
    }
}

#[test]
fn nested_winners_admit_null_children_and_charge_actual_nonnull_payload() {
    for data_type in [DataType::Null, DataType::UInt64] {
        let item = Arc::new(Field::new(
            "item",
            data_type.clone(),
            data_type == DataType::Null,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("at", DataType::Int64, false),
            Field::new("items", DataType::List(Arc::clone(&item)), false),
        ]));
        let (_root, operation, left_rows, mut transactions) =
            asof_runtime_fixture(&schema, AsOfDirection::Backward { allow_exact: true });
        let event = |at, length| nested_list_event(&schema, &item, &data_type, at, length);
        let right = event(10, 128 * 1024);
        {
            let transaction = transactions.begin();
            let step = operation
                .step(
                    OperationInput {
                        port: 1,
                        change: &right,
                    },
                    &operation.initial_resume(),
                    transaction.access(),
                    &mut StepBudget::new(1, 64 * 1024 * 1024),
                )
                .unwrap();
            assert_eq!(step.progress, Progress::Done);
            assert!(step.output.is_none());
            transaction.commit().unwrap();
        }
        let left = event(20, 0);
        {
            let transaction = transactions.begin();
            let result = operation.step(
                OperationInput {
                    port: 0,
                    change: &left,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 2 * 1024 * 1024),
            );
            if data_type == DataType::Null {
                let step = result.unwrap();
                assert_eq!(step.progress, Progress::Done);
                assert_eq!(step.output.unwrap().num_rows(), 1);
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .is::<dogpaddle_operation::operation::BudgetExceeded>()
                );
            }
            // Both the successful probe and the budget refusal roll back.
        }
        {
            let transaction = transactions.begin();
            assert!(
                left_rows
                    .access(transaction.access())
                    .unwrap()
                    .scan(
                        ..,
                        ScanDirection::Ascending,
                        None,
                        ScanLimit::new(1, 1024).unwrap(),
                    )
                    .unwrap()
                    .entries
                    .is_empty()
            );
        }
        let transaction = transactions.begin();
        let step = operation
            .step(
                OperationInput {
                    port: 0,
                    change: &left,
                },
                &operation.initial_resume(),
                transaction.access(),
                &mut StepBudget::new(1, 64 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(step.progress, Progress::Done);
        let output = step.output.unwrap();
        assert_nested_winner(&output, &data_type);
        transaction.commit().unwrap();
    }
}
