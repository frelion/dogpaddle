use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, UInt64Array, new_null_array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_change::Change;

pub(crate) const DEFAULT_WORKLOADS: &[&str] = &[
    "diff_only",
    "narrow_fixed",
    "wide_projectable",
    "mixed_nullable",
    "nested",
    "sliced",
];

const MAX_I32_OFFSET: usize = 2_147_483_647;

pub(crate) struct Fixture {
    pub(crate) name: &'static str,
    pub(crate) change: Change,
    pub(crate) narrow_fields: &'static [usize],
}

pub(crate) fn fixtures(rows: usize, payload_bytes: usize, selected: &[String]) -> Vec<Fixture> {
    validate_dimensions(rows, payload_bytes, selected);
    selected
        .iter()
        .map(|name| match name.as_str() {
            "diff_only" => diff_only(rows),
            "narrow_fixed" => narrow_fixed(rows),
            "wide_projectable" => wide_projectable(rows, payload_bytes),
            "mixed_nullable" => mixed_nullable(rows),
            "nested" => nested(rows),
            "sliced" => sliced(rows, payload_bytes),
            _ => panic!("unknown Change benchmark workload {name:?}"),
        })
        .collect()
}

pub(crate) fn validate_dimensions(rows: usize, payload_bytes: usize, selected: &[String]) {
    assert!(rows > 0, "benchmark rows per Change must be non-zero");
    assert!(
        payload_bytes > 0,
        "benchmark payload width must be non-zero"
    );
    assert!(
        rows <= MAX_I32_OFFSET,
        "benchmark rows per Change exceed Arrow i32 offset capacity"
    );
    for workload in selected {
        match workload.as_str() {
            "diff_only" => {
                checked_mul(rows, size_of::<i64>(), "diff-only fixture bytes");
            }
            "narrow_fixed" => {
                checked_mul(
                    rows,
                    size_of::<u64>()
                        .checked_mul(2)
                        .and_then(|value| value.checked_add(size_of::<i64>()))
                        .expect("fixed-width row size fits usize"),
                    "narrow fixed-width fixture bytes",
                );
            }
            "wide_projectable" => validate_wide_dimensions(rows, payload_bytes),
            "mixed_nullable" => validate_mixed_dimensions(rows),
            "nested" => validate_nested_dimensions(rows),
            "sliced" => {
                let source_rows = rows
                    .checked_add(2)
                    .expect("sliced fixture source row count fits usize");
                assert!(
                    source_rows <= MAX_I32_OFFSET,
                    "sliced fixture source rows exceed Arrow i32 offset capacity"
                );
                validate_wide_dimensions(source_rows, payload_bytes);
            }
            _ => panic!("unknown Change benchmark workload {workload:?}"),
        }
    }
}

fn validate_wide_dimensions(rows: usize, payload_bytes: usize) {
    let payload = checked_mul(rows, payload_bytes, "wide Binary values bytes");
    assert!(
        payload <= MAX_I32_OFFSET,
        "wide Binary values exceed Arrow i32 offset capacity"
    );
    let fixed = checked_mul(
        rows,
        size_of::<u64>()
            .checked_mul(2)
            .and_then(|value| value.checked_add(size_of::<i64>()))
            .expect("wide fixed-width row size fits usize"),
        "wide fixed-width fixture bytes",
    );
    payload
        .checked_add(fixed)
        .expect("wide fixture working bytes fit usize");
}

fn validate_mixed_dimensions(rows: usize) {
    let largest_index = rows - 1;
    let label_bytes_per_row = "event-"
        .len()
        .checked_add(format!("{largest_index:08x}").len())
        .expect("mixed label width fits usize");
    let labels = checked_mul(rows, label_bytes_per_row, "mixed Utf8 values bytes");
    assert!(
        labels <= MAX_I32_OFFSET,
        "mixed Utf8 values exceed Arrow i32 offset capacity"
    );
    let binary = checked_mul(rows, size_of::<u64>(), "mixed Binary values bytes");
    assert!(
        binary <= MAX_I32_OFFSET,
        "mixed Binary values exceed Arrow i32 offset capacity"
    );
    labels
        .checked_add(binary)
        .and_then(|value| value.checked_add(rows))
        .expect("mixed fixture working bytes fit usize");
}

fn validate_nested_dimensions(rows: usize) {
    let list_values = checked_mul(rows, 2, "nested List child values");
    assert!(
        list_values <= MAX_I32_OFFSET,
        "nested List child values exceed Arrow i32 offset capacity"
    );
    let largest_index = rows - 1;
    let label_bytes_per_row = "object-"
        .len()
        .checked_add(largest_index.to_string().len())
        .expect("nested label width fits usize");
    let labels = checked_mul(rows, label_bytes_per_row, "nested Utf8 values bytes");
    assert!(
        labels <= MAX_I32_OFFSET,
        "nested Utf8 values exceed Arrow i32 offset capacity"
    );
    labels
        .checked_add(
            list_values
                .checked_mul(size_of::<i64>())
                .expect("nested List child bytes fit usize"),
        )
        .expect("nested fixture working bytes fit usize");
}

fn checked_mul(left: usize, right: usize, description: &str) -> usize {
    left.checked_mul(right)
        .unwrap_or_else(|| panic!("{description} overflow usize"))
}

fn diff_only(rows: usize) -> Fixture {
    let records = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .expect("construct diff-only benchmark batch");
    Fixture {
        name: "diff_only",
        change: make_change(records, rows),
        narrow_fields: &[],
    }
}

fn narrow_fixed(rows: usize) -> Fixture {
    let keys = UInt64Array::from_iter_values((0..rows).map(to_u64));
    let versions = Int64Array::from_iter_values(
        (0..rows).map(|index| i64::try_from(index).expect("benchmark row index fits in i64")),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("version", DataType::Int64, false),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![Arc::new(keys) as ArrayRef, Arc::new(versions) as ArrayRef],
    )
    .expect("construct narrow fixed-width benchmark batch");
    Fixture {
        name: "narrow_fixed",
        change: make_change(records, rows),
        narrow_fields: &[0],
    }
}

fn wide_projectable(rows: usize, payload_bytes: usize) -> Fixture {
    let ids = UInt64Array::from_iter_values((0..rows).map(to_u64));
    let payloads = (0..rows)
        .map(|index| {
            let fill = u8::try_from(index & 0xff).expect("masked row index fits in u8");
            let mut payload = vec![fill; payload_bytes];
            if let Some(first) = payload.first_mut() {
                *first = fill.wrapping_mul(31);
            }
            payload
        })
        .collect::<Vec<_>>();
    let payloads = BinaryArray::from_iter_values(payloads.iter().map(Vec::as_slice));
    let tails = UInt64Array::from_iter_values(
        (0..rows).map(|index| to_u64(index).wrapping_mul(17).wrapping_add(3)),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("tail", DataType::UInt64, false),
    ]));
    let records = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(payloads) as ArrayRef,
            Arc::new(tails) as ArrayRef,
        ],
    )
    .expect("construct wide projectable benchmark batch");
    Fixture {
        name: "wide_projectable",
        change: make_change(records, rows),
        narrow_fields: &[0],
    }
}

fn mixed_nullable(rows: usize) -> Fixture {
    let flags = (0..rows)
        .map(|index| (!index.is_multiple_of(7)).then_some(index.is_multiple_of(2)))
        .collect::<BooleanArray>();
    let scores = (0..rows)
        .map(|index| {
            (!index.is_multiple_of(11)).then(|| {
                let value = u32::try_from(index).expect("benchmark row index fits in u32");
                f64::from(value) * 0.25
            })
        })
        .collect::<Float64Array>();
    let labels = (0..rows)
        .map(|index| (!index.is_multiple_of(5)).then(|| format!("event-{index:08x}")))
        .collect::<StringArray>();
    let payloads = (0..rows)
        .map(|index| {
            (!index.is_multiple_of(13)).then(|| {
                let value = to_u64(index).to_le_bytes();
                value.to_vec()
            })
        })
        .collect::<BinaryArray>();
    let columns = vec![
        Arc::new(flags) as ArrayRef,
        Arc::new(scores) as ArrayRef,
        Arc::new(labels) as ArrayRef,
        Arc::new(payloads) as ArrayRef,
        new_null_array(&DataType::Null, rows),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("score", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("nothing", DataType::Null, true),
    ]));
    let records =
        RecordBatch::try_new(schema, columns).expect("construct mixed nullable benchmark batch");
    Fixture {
        name: "mixed_nullable",
        change: make_change(records, rows),
        narrow_fields: &[0, 2],
    }
}

fn nested(rows: usize) -> Fixture {
    let ids = UInt64Array::from_iter_values((0..rows).map(to_u64));
    let items = ListArray::from_iter_primitive::<Int64Type, _, _>((0..rows).map(|index| {
        if index.is_multiple_of(7) {
            None
        } else if index.is_multiple_of(5) {
            Some(Vec::<Option<i64>>::new())
        } else {
            let value = i64::try_from(index).expect("benchmark row index fits in i64");
            Some(vec![
                Some(value),
                (!index.is_multiple_of(3)).then_some(-value),
            ])
        }
    }));
    let object_id = Arc::new(Field::new("id", DataType::UInt64, false));
    let object_label = Arc::new(Field::new("label", DataType::Utf8, true));
    let object = StructArray::from(vec![
        (
            Arc::clone(&object_id),
            Arc::new(UInt64Array::from_iter_values(
                (0..rows).map(|index| to_u64(index).wrapping_add(100)),
            )) as ArrayRef,
        ),
        (
            Arc::clone(&object_label),
            Arc::new(
                (0..rows)
                    .map(|index| (!index.is_multiple_of(9)).then(|| format!("object-{index}")))
                    .collect::<StringArray>(),
            ) as ArrayRef,
        ),
    ]);
    let columns = vec![
        Arc::new(ids) as ArrayRef,
        Arc::new(items) as ArrayRef,
        Arc::new(object) as ArrayRef,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("items", columns[1].data_type().clone(), true),
        Field::new(
            "object",
            DataType::Struct(vec![object_id, object_label].into()),
            true,
        ),
    ]));
    let records = RecordBatch::try_new(schema, columns).expect("construct nested benchmark batch");
    Fixture {
        name: "nested",
        change: make_change(records, rows),
        narrow_fields: &[0],
    }
}

fn sliced(rows: usize, payload_bytes: usize) -> Fixture {
    let source_rows = rows
        .checked_add(2)
        .expect("sliced fixture source row count fits usize");
    let source = wide_projectable(source_rows, payload_bytes);
    Fixture {
        name: "sliced",
        change: source
            .change
            .try_slice(1, rows)
            .expect("construct non-zero-offset benchmark Change"),
        narrow_fields: &[0],
    }
}

fn make_change(records: RecordBatch, rows: usize) -> Change {
    let diffs = Int64Array::from_iter_values((0..rows).map(|index| match index % 4 {
        0 | 3 => 1,
        1 => -1,
        _ => 2,
    }));
    Change::try_new(records, diffs).expect("construct valid Change benchmark fixture")
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark value fits in u64")
}
