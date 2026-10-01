use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    ListArray, NullArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource, col, lit,
    operation::{
        OperationInput, Progress, StepBudget,
        transform::{EquiJoinDefinition, EquiJoinKind},
    },
};
use dogpaddle_store::StoreSetup;

const FLOAT32_BITS: [u32; 4] = [0x8000_0000, 0x7fc1_2345, 0xffc2_3456, 0];
const FLOAT64_BITS: [u64; 4] = [
    0x8000_0000_0000_0000,
    0x7ff8_1234_5678_9abc,
    0xfff8_fedc_ba98_7654,
    0,
];

fn payload() -> Change {
    let children = Fields::from(vec![
        Field::new("number", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false)
            .with_metadata(HashMap::from([("owner".into(), "nested".into())])),
    ]);
    let structure = StructArray::new(
        children.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7, 999, -9, 11])),
            Arc::new(StringArray::from(vec!["first", "hidden", "last", "tail"])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let empty =
        StructArray::new_empty_fields(4, Some(NullBuffer::from(vec![true, false, true, true])));
    let item = Arc::new(Field::new("item", DataType::Null, true));
    let lists = ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(vec![0, 0, 0, 1, 3].into()),
        Arc::new(NullArray::new(3)),
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("structure", DataType::Struct(children), true),
        Field::new("empty", empty.data_type().clone(), true),
        Field::new("items", DataType::List(item), true),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new("decimal", DataType::Decimal128(12, 2), false),
        Field::new(
            "time",
            DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("Asia/Shanghai".into()),
            ),
            false,
        ),
        Field::new("bytes", DataType::Binary, false)
            .with_metadata(HashMap::from([("owner".into(), "payload".into())])),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
        Arc::new(structure),
        Arc::new(empty),
        Arc::new(lists),
        Arc::new(Float32Array::from(
            FLOAT32_BITS.map(f32::from_bits).to_vec(),
        )),
        Arc::new(Float64Array::from(
            FLOAT64_BITS.map(f64::from_bits).to_vec(),
        )),
        Arc::new(
            Decimal128Array::from(vec![-123, 0, 999, 1])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(
            TimestampNanosecondArray::from(vec![-7, 0, 12, 99]).with_timezone("Asia/Shanghai"),
        ),
        Arc::new(BinaryArray::from(vec![
            b"a\0b".as_slice(),
            b"\0",
            b"",
            b"z",
        ])),
    ];
    Change::try_new(
        RecordBatch::try_new(schema, columns).unwrap(),
        Int64Array::from(vec![1; 4]),
    )
    .unwrap()
}

fn assert_nested_payload(records: &RecordBatch, row: usize, start: usize, expected: usize) {
    let structure = records
        .column(start + 1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(structure.is_null(row), expected == 1);
    assert_eq!(structure.column(0).len(), records.num_rows());
    assert_eq!(structure.column(1).len(), records.num_rows());
    if expected != 1 {
        assert_eq!(
            structure
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            [7, 999, -9, 11][expected]
        );
        assert_eq!(
            structure
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
            ["first", "hidden", "last", "tail"][expected]
        );
    }
    assert_eq!(
        structure.fields()[1].metadata().get("owner").unwrap(),
        "nested"
    );
    let empty = records
        .column(start + 2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(empty.len(), records.num_rows());
    assert_eq!(empty.is_null(row), expected == 1);
    let lists = records
        .column(start + 3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.is_null(row), expected == 1);
    assert_eq!(lists.value_length(row), [0, 0, 1, 2][expected]);
    assert_eq!(lists.value(row).data_type(), &DataType::Null);
}

fn assert_side(records: &RecordBatch, row: usize, start: usize, expected: usize) {
    assert_eq!(
        records
            .column(start)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row),
        u64::try_from(expected + 1).unwrap()
    );
    assert_nested_payload(records, row, start, expected);
    assert_eq!(
        records
            .column(start + 4)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_bits(),
        FLOAT32_BITS[expected]
    );
    assert_eq!(
        records
            .column(start + 5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_bits(),
        FLOAT64_BITS[expected]
    );
    let decimal = records
        .column(start + 6)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(decimal.data_type(), &DataType::Decimal128(12, 2));
    assert_eq!(decimal.value(row), [-123, 0, 999, 1][expected]);
    let time = records
        .column(start + 7)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(time.timezone(), Some("Asia/Shanghai"));
    assert_eq!(time.value(row), [-7, 0, 12, 99][expected]);
    assert_eq!(
        records
            .column(start + 8)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row),
        [b"a\0b".as_slice(), b"\0", b"", b"z"][expected]
    );
    assert_eq!(
        records
            .schema()
            .field(start + 8)
            .metadata()
            .get("owner")
            .unwrap(),
        "payload"
    );
}

#[test]
fn pure_and_residual_outer_pages_preserve_nested_shapes_metadata_and_raw_bits() {
    let input = payload();
    let right = input.try_slice(0, 3).unwrap();
    for residual in [None, Some(lit(true))] {
        for items in [1, 256] {
            let root = tempfile::tempdir().unwrap();
            let definition = OperationDefinition::from(
                EquiJoinDefinition::try_new(
                    EquiJoinKind::LeftOuter,
                    [(col("key"), col("key"))],
                    (0..18).map(|index| format!("out{index}")),
                    residual.clone(),
                )
                .unwrap(),
            );
            let mut setup = StoreSetup::new();
            let operation = definition
                .construct(
                    &[input.schema(), input.schema()],
                    &mut setup.data_scope(),
                    RuntimeResource::none(),
                )
                .unwrap()
                .into_parts()
                .0;
            let mut transactions = setup.commit(root.path().join("store"), |_| Ok(())).unwrap();
            for (port, change) in [(1, &right), (0, &input)] {
                let mut resume = operation.initial_resume();
                let mut expected = 0;
                loop {
                    let transaction = transactions.begin();
                    let step = operation
                        .step(
                            OperationInput { port, change },
                            &resume,
                            transaction.access(),
                            &mut StepBudget::new(items, 4 * 1024 * 1024),
                        )
                        .unwrap();
                    if port == 1 {
                        assert!(step.output.is_none());
                    } else {
                        let output = step.output.unwrap();
                        for row in 0..output.num_rows() {
                            assert_eq!(output.diffs().value(row), 1);
                            assert_side(output.records(), row, 0, expected);
                            if expected == 3 {
                                for column in &output.records().columns()[9..] {
                                    assert!(column.is_null(row));
                                    assert_eq!(column.len(), output.num_rows());
                                }
                            } else {
                                assert_side(output.records(), row, 9, expected);
                            }
                            expected += 1;
                        }
                    }
                    transaction.commit().unwrap();
                    match step.progress {
                        Progress::Done => break,
                        Progress::More(next) => resume = next,
                    }
                }
                if port == 0 {
                    assert_eq!(expected, 4);
                }
            }
        }
    }
}
