//! Definition codec and persistent Operation-turn benchmark protocol.

use std::{hint::black_box, path::Path, sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_bench_protocol::require_benchmark_build;
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, decode_definition, encode_definition,
    operation::{
        Operation, OperationInput,
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{Cell, Store, Transactions};

use support::{BenchRoot, Config, MachineRecords};

mod support;

const SEQUENCE_START: u64 = 1_000_000;

struct CountFixture {
    transactions: Transactions,
    state: Cell<u64>,
    operation: CountOperation,
    input: Change,
    expected: Option<u64>,
}

struct SequenceFixture {
    transactions: Transactions,
    state: Cell<u64>,
    operation: SequenceSourceOperation,
    expected: Option<u64>,
}

impl CountFixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).expect("create Count benchmark store");
        let state = store
            .create_data::<Cell<u64>>("count")
            .expect("create Count benchmark state");
        let operation = CountOperation::new(CountDefinition::new(), state.clone());
        Self {
            transactions: store.into_transactions(),
            state,
            operation,
            input: one_row_change(),
            expected: None,
        }
    }

    fn validate(&mut self) {
        let transaction = self
            .transactions
            .begin()
            .expect("begin Count validation transaction");
        let access = transaction.access();
        let actual = self
            .state
            .access(access)
            .expect("access Count state for validation")
            .get()
            .expect("read Count state for validation");
        assert_eq!(actual, self.expected);
        transaction
            .commit()
            .expect("commit Count validation transaction");
    }
}

impl SequenceFixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).expect("create Sequence benchmark store");
        let state = store
            .create_data::<Cell<u64>>("position")
            .expect("create Sequence benchmark state");
        let operation = SequenceSourceOperation::new(
            SequenceSourceDefinition::new(SEQUENCE_START),
            state.clone(),
        );
        Self {
            transactions: store.into_transactions(),
            state,
            operation,
            expected: None,
        }
    }

    fn validate(&mut self) {
        let transaction = self
            .transactions
            .begin()
            .expect("begin Sequence validation transaction");
        let actual = self
            .state
            .access(transaction.access())
            .expect("access Sequence state for validation")
            .get()
            .expect("read Sequence state for validation");
        assert_eq!(actual, self.expected);
        transaction
            .commit()
            .expect("commit Sequence validation transaction");
    }
}

fn main() {
    require_benchmark_build("operation_core");

    let config = Config::load();
    let root = BenchRoot::from_environment();
    println!("DogPaddle Operation core benchmark");
    println!(
        "timing=explicit-boundaries setup=outside validation=outside warmup=unreported rows-per-turn=1"
    );
    root.emit_environment();
    config.emit(root.profile());

    let mut records = MachineRecords::new();
    benchmark_definition_codec(&config, &mut records);
    for &steps in &config.steps {
        benchmark_count_body(&config, &root, steps, &mut records);
        benchmark_sequence_body(&config, &root, steps, &mut records);
        benchmark_count_durable(&config, &root, steps, &mut records);
        benchmark_sequence_durable(&config, &root, steps, &mut records);
    }
    root.assert_samples_released();
    records.emit();
}

fn benchmark_definition_codec(config: &Config, records: &mut MachineRecords) {
    println!();
    println!("=== Definition public codec ===");
    let definitions: [(&str, Box<dyn OperationDefinition>); 2] = [
        ("count", Box::new(CountDefinition::new())),
        (
            "sequence",
            Box::new(SequenceSourceDefinition::new(SEQUENCE_START)),
        ),
    ];

    for (operation, definition) in definitions {
        let encoded = encode_definition(definition.as_ref());
        assert_eq!(
            encode_definition(decode_definition(&encoded).unwrap().as_ref()),
            encoded
        );

        measure_encode(definition.as_ref(), config.codec_warmup_operations());
        let durations = (0..config.samples)
            .map(|_| measure_encode(definition.as_ref(), config.codec_operations))
            .collect();
        records.record(
            operation,
            "definition_encode",
            config.codec_operations,
            0,
            0,
            durations,
        );

        measure_decode(&encoded, config.codec_warmup_operations());
        let durations = (0..config.samples)
            .map(|_| measure_decode(&encoded, config.codec_operations))
            .collect();
        records.record(
            operation,
            "definition_decode",
            config.codec_operations,
            0,
            0,
            durations,
        );
    }
}

fn benchmark_count_body(
    config: &Config,
    root: &BenchRoot,
    steps: usize,
    records: &mut MachineRecords,
) {
    let sample_store = root.sample(&format!("count-body-{steps}"));
    let mut fixture = CountFixture::create(sample_store.path());
    measure_count_body(&mut fixture, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_count_body(&mut fixture, steps, config.body_transactions))
        .collect();
    records.record(
        "count",
        "turn_rollback_body",
        operation_count(steps, config.body_transactions),
        config.body_transactions,
        steps,
        durations,
    );
}

fn benchmark_sequence_body(
    config: &Config,
    root: &BenchRoot,
    steps: usize,
    records: &mut MachineRecords,
) {
    let sample_store = root.sample(&format!("sequence-body-{steps}"));
    let mut fixture = SequenceFixture::create(sample_store.path());
    measure_sequence_body(&mut fixture, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_sequence_body(&mut fixture, steps, config.body_transactions))
        .collect();
    records.record(
        "sequence",
        "turn_rollback_body",
        operation_count(steps, config.body_transactions),
        config.body_transactions,
        steps,
        durations,
    );
}

fn benchmark_count_durable(
    config: &Config,
    root: &BenchRoot,
    steps: usize,
    records: &mut MachineRecords,
) {
    {
        let warmup_store = root.sample(&format!("count-durable-{steps}-warmup"));
        let mut warmup = CountFixture::create(warmup_store.path());
        measure_count_durable(&mut warmup, steps, config.warmup_transactions);
    }
    let durations = (0..config.samples)
        .map(|sample| {
            let sample_store = root.sample(&format!("count-durable-{steps}-sample-{sample}"));
            let mut fixture = CountFixture::create(sample_store.path());
            measure_count_durable(&mut fixture, steps, config.durable_transactions)
        })
        .collect();
    records.record(
        "count",
        "turn_durable_transaction",
        operation_count(steps, config.durable_transactions),
        config.durable_transactions,
        steps,
        durations,
    );
}

fn benchmark_sequence_durable(
    config: &Config,
    root: &BenchRoot,
    steps: usize,
    records: &mut MachineRecords,
) {
    {
        let warmup_store = root.sample(&format!("sequence-durable-{steps}-warmup"));
        let mut warmup = SequenceFixture::create(warmup_store.path());
        measure_sequence_durable(&mut warmup, steps, config.warmup_transactions);
    }
    let durations = (0..config.samples)
        .map(|sample| {
            let sample_store = root.sample(&format!("sequence-durable-{steps}-sample-{sample}"));
            let mut fixture = SequenceFixture::create(sample_store.path());
            measure_sequence_durable(&mut fixture, steps, config.durable_transactions)
        })
        .collect();
    records.record(
        "sequence",
        "turn_durable_transaction",
        operation_count(steps, config.durable_transactions),
        config.durable_transactions,
        steps,
        durations,
    );
}

fn measure_encode(definition: &dyn OperationDefinition, operations: usize) -> Duration {
    let expected_bytes = encode_definition(definition).len();
    let mut checksum = 0_usize;
    let started = std::time::Instant::now();
    for _ in 0..operations {
        let encoded = encode_definition(definition);
        checksum = checksum.wrapping_add(encoded.len());
        black_box(encoded);
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_bytes.wrapping_mul(operations));
    elapsed
}

fn measure_decode(encoded: &[u8], operations: usize) -> Duration {
    let expected_signature = {
        let definition = decode_definition(encoded).expect("decode benchmark fixture");
        definition.input_count() + definition.data().len()
    };
    let mut checksum = 0_usize;
    let started = std::time::Instant::now();
    for _ in 0..operations {
        let definition = decode_definition(encoded).expect("decode valid benchmark definition");
        checksum = checksum.wrapping_add(definition.input_count() + definition.data().len());
        black_box(definition);
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_signature.wrapping_mul(operations));
    elapsed
}

fn measure_count_body(fixture: &mut CountFixture, steps: usize, transactions: usize) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Count rollback transaction");
        let access = transaction.access();
        let input = OperationInput {
            port: 0,
            change: &fixture.input,
        };
        let started = std::time::Instant::now();
        for _ in 0..steps {
            black_box(
                fixture
                    .operation
                    .turn(Some(input), access)
                    .expect("run Count rollback workload"),
            );
        }
        let elapsed = started.elapsed();
        total = total.checked_add(elapsed).expect("body duration fits");
    }
    fixture.expected = None;
    fixture.validate();
    total
}

fn measure_sequence_body(
    fixture: &mut SequenceFixture,
    steps: usize,
    transactions: usize,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Sequence rollback transaction");
        let access = transaction.access();
        let started = std::time::Instant::now();
        for _ in 0..steps {
            black_box(
                fixture
                    .operation
                    .turn(None, access)
                    .expect("run Sequence rollback workload"),
            );
        }
        let elapsed = started.elapsed();
        total = total.checked_add(elapsed).expect("body duration fits");
    }
    fixture.expected = None;
    fixture.validate();
    total
}

fn measure_count_durable(
    fixture: &mut CountFixture,
    steps: usize,
    transactions: usize,
) -> Duration {
    let operations = operation_count(steps, transactions);
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Count durable transaction");
        let access = transaction.access();
        let input = OperationInput {
            port: 0,
            change: &fixture.input,
        };
        for _ in 0..steps {
            black_box(
                fixture
                    .operation
                    .turn(Some(input), access)
                    .expect("run Count durable workload"),
            );
        }
        transaction
            .commit()
            .expect("commit Count durable transaction");
    }
    let elapsed = started.elapsed();
    let expected = fixture
        .expected
        .unwrap_or_default()
        .checked_add(u64::try_from(operations).expect("operation count fits u64"))
        .expect("Count durable fixture does not overflow");
    fixture.expected = Some(expected);
    fixture.validate();
    elapsed
}

fn measure_sequence_durable(
    fixture: &mut SequenceFixture,
    steps: usize,
    transactions: usize,
) -> Duration {
    let operations = operation_count(steps, transactions);
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Sequence durable transaction");
        let access = transaction.access();
        for _ in 0..steps {
            black_box(
                fixture
                    .operation
                    .turn(None, access)
                    .expect("run Sequence durable workload"),
            );
        }
        transaction
            .commit()
            .expect("commit Sequence durable transaction");
    }
    let elapsed = started.elapsed();
    let operations = u64::try_from(operations).expect("operation count fits u64");
    let expected = fixture.expected.map_or_else(
        || {
            SEQUENCE_START
                .checked_add(operations - 1)
                .expect("Sequence durable fixture does not overflow")
        },
        |previous| {
            previous
                .checked_add(operations)
                .expect("Sequence durable fixture does not overflow")
        },
    );
    fixture.expected = Some(expected);
    fixture.validate();
    elapsed
}

fn one_row_change() -> Change {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        DataType::UInt64,
        false,
    )]));
    let records = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![0]))])
        .expect("build Count benchmark input batch");
    Change::try_new(records, Int64Array::from(vec![1])).expect("build Count benchmark input Change")
}

fn operation_count(steps: usize, transactions: usize) -> usize {
    steps
        .checked_mul(transactions)
        .expect("configured operation count fits usize")
}
