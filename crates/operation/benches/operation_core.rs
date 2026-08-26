//! Definition codec and persistent operation-step benchmark protocol.

use std::{hint::black_box, path::Path, time::Duration};

use dogpaddle_operation::{
    OperationDefinition, decode_definition, encode_definition,
    operation::{
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{CountDefinition, CountOperation},
    },
};
use dogpaddle_store::{Cell, Store, Transactions};

use support::{BenchRoot, Config, SampleRecord, emit_samples, record_samples};

mod support;

const SEQUENCE_START: u64 = 1_000_000;

struct CountFixture {
    transactions: Transactions,
    state: Cell<u64>,
    operation: CountOperation,
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
            expected: None,
        }
    }

    fn validate(&mut self) {
        let transaction = self
            .transactions
            .begin()
            .expect("begin Count validation transaction");
        let actual = self
            .state
            .access(transaction.access())
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
    if cfg!(debug_assertions) {
        eprintln!("operation_core must run through `cargo bench`");
        return;
    }

    let config = Config::load();
    let root = BenchRoot::from_environment();
    println!("DogPaddle Operation core benchmark");
    println!(
        "timing=explicit-boundaries setup=outside validation=outside warmup=unreported rows-metric=not-applicable"
    );
    root.emit_environment();
    config.emit(root.profile());

    let mut records = Vec::<SampleRecord>::new();
    benchmark_definition_codec(&config, &mut records);
    for &steps in &config.steps {
        benchmark_count_body(&config, &root, steps, &mut records);
        benchmark_sequence_body(&config, &root, steps, &mut records);
        benchmark_count_durable(&config, &root, steps, &mut records);
        benchmark_sequence_durable(&config, &root, steps, &mut records);
    }
    emit_samples(&records);
}

fn benchmark_definition_codec(config: &Config, records: &mut Vec<SampleRecord>) {
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
        record_samples(
            records,
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
        record_samples(
            records,
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
    records: &mut Vec<SampleRecord>,
) {
    let mut fixture = CountFixture::create(&root.store_path(&format!("count-body-{steps}")));
    measure_count_body(&mut fixture, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_count_body(&mut fixture, steps, config.body_transactions))
        .collect();
    record_samples(
        records,
        "count",
        "step_rollback_body",
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
    records: &mut Vec<SampleRecord>,
) {
    let mut fixture = SequenceFixture::create(&root.store_path(&format!("sequence-body-{steps}")));
    measure_sequence_body(&mut fixture, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_sequence_body(&mut fixture, steps, config.body_transactions))
        .collect();
    record_samples(
        records,
        "sequence",
        "step_rollback_body",
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
    records: &mut Vec<SampleRecord>,
) {
    let mut warmup =
        CountFixture::create(&root.store_path(&format!("count-durable-{steps}-warmup")));
    measure_count_durable(&mut warmup, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|sample| {
            let mut fixture = CountFixture::create(
                &root.store_path(&format!("count-durable-{steps}-sample-{sample}")),
            );
            measure_count_durable(&mut fixture, steps, config.durable_transactions)
        })
        .collect();
    record_samples(
        records,
        "count",
        "step_durable_transaction",
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
    records: &mut Vec<SampleRecord>,
) {
    let mut warmup =
        SequenceFixture::create(&root.store_path(&format!("sequence-durable-{steps}-warmup")));
    measure_sequence_durable(&mut warmup, steps, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|sample| {
            let mut fixture = SequenceFixture::create(
                &root.store_path(&format!("sequence-durable-{steps}-sample-{sample}")),
            );
            measure_sequence_durable(&mut fixture, steps, config.durable_transactions)
        })
        .collect();
    record_samples(
        records,
        "sequence",
        "step_durable_transaction",
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
    let expected_last = u64::try_from(steps).expect("step count fits u64");
    let mut total = Duration::ZERO;
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Count rollback transaction");
        let access = transaction.access();
        let mut last = None;
        let started = std::time::Instant::now();
        for _ in 0..steps {
            last = Some(
                fixture
                    .operation
                    .step(access)
                    .expect("step Count rollback workload"),
            );
            black_box(last);
        }
        let elapsed = started.elapsed();
        assert_eq!(last, Some(expected_last));
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
    let expected_last = SEQUENCE_START
        .checked_add(u64::try_from(steps - 1).expect("step count fits u64"))
        .expect("Sequence body fixture does not overflow");
    let mut total = Duration::ZERO;
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Sequence rollback transaction");
        let access = transaction.access();
        let mut last = None;
        let started = std::time::Instant::now();
        for _ in 0..steps {
            last = Some(
                fixture
                    .operation
                    .step(access)
                    .expect("step Sequence rollback workload"),
            );
            black_box(last);
        }
        let elapsed = started.elapsed();
        assert_eq!(last, Some(expected_last));
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
    let mut last = None;
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Count durable transaction");
        let access = transaction.access();
        for _ in 0..steps {
            last = Some(
                fixture
                    .operation
                    .step(access)
                    .expect("step Count durable workload"),
            );
            black_box(last);
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
    assert_eq!(last, Some(expected));
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
    let mut last = None;
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Sequence durable transaction");
        let access = transaction.access();
        for _ in 0..steps {
            last = Some(
                fixture
                    .operation
                    .step(access)
                    .expect("step Sequence durable workload"),
            );
            black_box(last);
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
    assert_eq!(last, Some(expected));
    fixture.expected = Some(expected);
    fixture.validate();
    elapsed
}

fn operation_count(steps: usize, transactions: usize) -> usize {
    steps
        .checked_mul(transactions)
        .expect("configured operation count fits usize")
}
