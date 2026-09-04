//! Definition codec and persistent Operation-turn benchmark protocol.

use std::{hint::black_box, num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use dogpaddle_bench_protocol::{BenchmarkProfile, CaseId, Plan, Run};
use dogpaddle_change::Change;
use dogpaddle_operation::{
    OperationDefinition, col, decode_definition, encode_definition, lit,
    operation::{
        Action, Operation, OperationError, OperationInput, Turn,
        sink::DiscardDefinition,
        source::{SequenceSourceDefinition, SequenceSourceOperation},
        transform::{
            ExtendDefinition, FilterDefinition, ProjectDefinition, RunningEventCountDefinition,
            RunningEventCountOperation, SchemaAlignDefinition, SchemaAlignField, SelectDefinition,
            UnionAllDefinition,
        },
    },
};
use dogpaddle_store::{Cell, Store, TransactionAccess, Transactions};

use support::{Config, SampleStore, case, record};

mod support;

const BENCHMARK: &str = "operation_core";
const SEQUENCE_START: u64 = 1_000_000;

struct RunningEventCountFixture {
    transactions: Transactions,
    state: Cell<u64>,
    operation: RunningEventCountOperation,
    input: Change,
    expected: Option<u64>,
}

struct SequenceFixture {
    transactions: Transactions,
    state: Cell<u64>,
    operation: SequenceSourceOperation,
    expected: Option<u64>,
}

struct CodecPlan {
    definition: Box<dyn OperationDefinition>,
    encode: CaseId,
    decode: CaseId,
}

struct TurnPlan {
    turns: usize,
    running_event_count_body: CaseId,
    sequence_body: CaseId,
    running_event_count_durable: CaseId,
    sequence_durable: CaseId,
}

impl RunningEventCountFixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).expect("create RunningEventCount benchmark store");
        let state = store
            .create_data::<Cell<u64>>("running_event_count.count")
            .expect("create RunningEventCount benchmark state");
        let operation = RunningEventCountOperation::new(state.clone());
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
            .expect("begin RunningEventCount validation transaction");
        let access = transaction.access();
        let actual = self
            .state
            .access(access)
            .expect("access RunningEventCount state for validation")
            .get()
            .expect("read RunningEventCount state for validation");
        assert_eq!(actual, self.expected);
        transaction
            .commit()
            .expect("commit RunningEventCount validation transaction");
    }
}

impl SequenceFixture {
    fn create(path: &Path) -> Self {
        let mut store = Store::create(path).expect("create Sequence benchmark store");
        let state = store
            .create_data::<Cell<u64>>("position")
            .expect("create Sequence benchmark state");
        let operation = SequenceSourceOperation::new(SEQUENCE_START, state.clone());
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
    let profile = BenchmarkProfile::from_environment();
    let config = Config::for_profile(profile);
    let mut plan = Plan::new(profile, config.fields());
    let codec = codec_definitions()
        .into_iter()
        .map(|(operation, definition)| CodecPlan {
            definition,
            encode: plan.case(case(
                operation,
                "definition_encode",
                config.codec_operations,
                0,
                0,
                config.samples,
            )),
            decode: plan.case(case(
                operation,
                "definition_decode",
                config.codec_operations,
                0,
                0,
                config.samples,
            )),
        })
        .collect::<Vec<_>>();
    let turns = config
        .turns
        .iter()
        .map(|&turns| TurnPlan {
            turns,
            running_event_count_body: plan.case(case(
                "running_event_count",
                "turn_rollback_body",
                operation_count(turns, config.body_transactions),
                config.body_transactions,
                turns,
                config.samples,
            )),
            sequence_body: plan.case(case(
                "sequence",
                "turn_rollback_body",
                operation_count(turns, config.body_transactions),
                config.body_transactions,
                turns,
                config.samples,
            )),
            running_event_count_durable: plan.case(case(
                "running_event_count",
                "turn_durable_transaction",
                operation_count(turns, config.durable_transactions),
                config.durable_transactions,
                turns,
                config.samples,
            )),
            sequence_durable: plan.case(case(
                "sequence",
                "turn_durable_transaction",
                operation_count(turns, config.durable_transactions),
                config.durable_transactions,
                turns,
                config.samples,
            )),
        })
        .collect::<Vec<_>>();
    let mut run = Run::persistent(BENCHMARK, plan);
    if run.is_plan_only() {
        run.emit_plan();
        return;
    }
    benchmark_definition_codec(&config, &codec, &mut run);
    for planned in turns {
        benchmark_running_event_count_body(
            &config,
            &mut run,
            planned.turns,
            planned.running_event_count_body,
        );
        benchmark_sequence_body(&config, &mut run, planned.turns, planned.sequence_body);
        benchmark_running_event_count_durable(
            &config,
            &mut run,
            planned.turns,
            planned.running_event_count_durable,
        );
        benchmark_sequence_durable(&config, &mut run, planned.turns, planned.sequence_durable);
    }
    assert!(
        std::fs::read_dir(run.path())
            .expect("read Operation benchmark run root")
            .next()
            .is_none(),
        "validated Operation sample Stores must be released immediately"
    );
    run.finish(|| {});
}

fn benchmark_definition_codec(config: &Config, plan: &[CodecPlan], run: &mut Run) {
    for planned in plan {
        let encoded = encode_definition(planned.definition.as_ref());
        assert_eq!(
            encode_definition(decode_definition(&encoded).unwrap().as_ref()),
            encoded
        );

        measure_encode(
            planned.definition.as_ref(),
            config.codec_warmup_operations(),
        );
        let durations = (0..config.samples)
            .map(|_| measure_encode(planned.definition.as_ref(), config.codec_operations))
            .collect();
        record(run, planned.encode, durations);

        measure_decode(&encoded, config.codec_warmup_operations());
        let durations = (0..config.samples)
            .map(|_| measure_decode(&encoded, config.codec_operations))
            .collect();
        record(run, planned.decode, durations);
    }
}

fn codec_definitions() -> [(&'static str, Box<dyn OperationDefinition>); 9] {
    [
        ("discard", Box::new(DiscardDefinition::new())),
        (
            "extend",
            Box::new(ExtendDefinition::try_new("copy", col("input")).unwrap()),
        ),
        (
            "filter",
            Box::new(FilterDefinition::try_new(col("input").eq(lit(0_u64))).unwrap()),
        ),
        ("project", Box::new(ProjectDefinition::new([0]))),
        (
            "running_event_count",
            Box::new(RunningEventCountDefinition::new()),
        ),
        (
            "schema_align",
            Box::new(
                SchemaAlignDefinition::try_new([SchemaAlignField::try_new(
                    "copy",
                    col("input"),
                    false,
                )
                .unwrap()])
                .unwrap(),
            ),
        ),
        (
            "select",
            Box::new(SelectDefinition::try_new([("copy", col("input"))]).unwrap()),
        ),
        (
            "sequence",
            Box::new(SequenceSourceDefinition::new(SEQUENCE_START)),
        ),
        (
            "union_all",
            Box::new(UnionAllDefinition::new(NonZeroU32::new(2).unwrap())),
        ),
    ]
}

fn benchmark_running_event_count_body(config: &Config, run: &mut Run, turns: usize, case: CaseId) {
    let sample_store = SampleStore::new(run, &format!("running-event-count-body-{turns}"));
    let mut fixture = RunningEventCountFixture::create(sample_store.path());
    measure_running_event_count_body(&mut fixture, turns, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_running_event_count_body(&mut fixture, turns, config.body_transactions))
        .collect();
    record(run, case, durations);
}

fn benchmark_sequence_body(config: &Config, run: &mut Run, turns: usize, case: CaseId) {
    let sample_store = SampleStore::new(run, &format!("sequence-body-{turns}"));
    let mut fixture = SequenceFixture::create(sample_store.path());
    measure_sequence_body(&mut fixture, turns, config.warmup_transactions);
    let durations = (0..config.samples)
        .map(|_| measure_sequence_body(&mut fixture, turns, config.body_transactions))
        .collect();
    record(run, case, durations);
}

fn benchmark_running_event_count_durable(
    config: &Config,
    run: &mut Run,
    turns: usize,
    case: CaseId,
) {
    {
        let warmup_store =
            SampleStore::new(run, &format!("running-event-count-durable-{turns}-warmup"));
        let mut warmup = RunningEventCountFixture::create(warmup_store.path());
        measure_running_event_count_durable(&mut warmup, turns, config.warmup_transactions);
    }
    let durations = (0..config.samples)
        .map(|sample| {
            let sample_store = SampleStore::new(
                run,
                &format!("running-event-count-durable-{turns}-sample-{sample}"),
            );
            let mut fixture = RunningEventCountFixture::create(sample_store.path());
            measure_running_event_count_durable(&mut fixture, turns, config.durable_transactions)
        })
        .collect();
    record(run, case, durations);
}

fn benchmark_sequence_durable(config: &Config, run: &mut Run, turns: usize, case: CaseId) {
    {
        let warmup_store = SampleStore::new(run, &format!("sequence-durable-{turns}-warmup"));
        let mut warmup = SequenceFixture::create(warmup_store.path());
        measure_sequence_durable(&mut warmup, turns, config.warmup_transactions);
    }
    let durations = (0..config.samples)
        .map(|sample| {
            let sample_store =
                SampleStore::new(run, &format!("sequence-durable-{turns}-sample-{sample}"));
            let mut fixture = SequenceFixture::create(sample_store.path());
            measure_sequence_durable(&mut fixture, turns, config.durable_transactions)
        })
        .collect();
    record(run, case, durations);
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
        definition_signature(definition.as_ref())
    };
    let mut checksum = 0_usize;
    let started = std::time::Instant::now();
    for _ in 0..operations {
        let definition = decode_definition(encoded).expect("decode valid benchmark definition");
        checksum = checksum.wrapping_add(definition_signature(definition.as_ref()));
        black_box(definition);
    }
    let elapsed = started.elapsed();
    assert_eq!(checksum, expected_signature.wrapping_mul(operations));
    elapsed
}

fn definition_signature(definition: &dyn OperationDefinition) -> usize {
    usize::try_from(definition.kind().input_count())
        .expect("Operation input count fits usize")
        .wrapping_add(definition.data().len())
}

fn apply_transactional_turn(
    turn: Result<Turn<'_>, OperationError>,
    access: TransactionAccess<'_>,
) -> Action {
    let Turn::Ready(turn) = turn.expect("prepare transactional Operation benchmark turn") else {
        panic!("a transactional benchmark Operation returned an outer idle turn");
    };
    let (action, after_commit) = turn
        .apply(access)
        .expect("apply transactional Operation benchmark turn");
    // Both benchmarked runtime types use the crate's TransactionalOperation
    // adapter: its outer turn is structurally inert and its completion empty.
    // This benchmark alone may therefore call turn after begin and release the
    // completion here to preserve its turns-per-transaction workload. An
    // effectful Operation must prepare before begin and run its completion only
    // after Store commit.
    drop(after_commit);
    action
}

fn measure_running_event_count_body(
    fixture: &mut RunningEventCountFixture,
    turns: usize,
    transactions: usize,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin RunningEventCount rollback transaction");
        let access = transaction.access();
        let input = OperationInput {
            port: 0,
            change: &fixture.input,
        };
        let started = std::time::Instant::now();
        for _ in 0..turns {
            black_box(apply_transactional_turn(
                fixture.operation.turn(Some(input)),
                access,
            ));
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
    turns: usize,
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
        for _ in 0..turns {
            black_box(apply_transactional_turn(
                fixture.operation.turn(None),
                access,
            ));
        }
        let elapsed = started.elapsed();
        total = total.checked_add(elapsed).expect("body duration fits");
    }
    fixture.expected = None;
    fixture.validate();
    total
}

fn measure_running_event_count_durable(
    fixture: &mut RunningEventCountFixture,
    turns: usize,
    transactions: usize,
) -> Duration {
    let operations = operation_count(turns, transactions);
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin RunningEventCount durable transaction");
        let access = transaction.access();
        let input = OperationInput {
            port: 0,
            change: &fixture.input,
        };
        for _ in 0..turns {
            black_box(apply_transactional_turn(
                fixture.operation.turn(Some(input)),
                access,
            ));
        }
        transaction
            .commit()
            .expect("commit RunningEventCount durable transaction");
    }
    let elapsed = started.elapsed();
    let expected = fixture
        .expected
        .unwrap_or_default()
        .checked_add(u64::try_from(operations).expect("operation count fits u64"))
        .expect("RunningEventCount durable fixture does not overflow");
    fixture.expected = Some(expected);
    fixture.validate();
    elapsed
}

fn measure_sequence_durable(
    fixture: &mut SequenceFixture,
    turns: usize,
    transactions: usize,
) -> Duration {
    let operations = operation_count(turns, transactions);
    let started = std::time::Instant::now();
    for _ in 0..transactions {
        let transaction = fixture
            .transactions
            .begin()
            .expect("begin Sequence durable transaction");
        let access = transaction.access();
        for _ in 0..turns {
            black_box(apply_transactional_turn(
                fixture.operation.turn(None),
                access,
            ));
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
        .expect("build RunningEventCount benchmark input batch");
    Change::try_new(records, Int64Array::from(vec![1]))
        .expect("build RunningEventCount benchmark input Change")
}

fn operation_count(turns: usize, transactions: usize) -> usize {
    turns
        .checked_mul(transactions)
        .expect("configured operation count fits usize")
}
