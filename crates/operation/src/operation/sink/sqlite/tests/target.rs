use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use rusqlite::{Connection, params};
use tempfile::TempDir;

use super::super::{error::SqliteSinkError, target::SqliteTarget};
use crate::operation::{
    OperationError,
    sink::{
        buffered::DeliveryBatch,
        relation::{RelationTarget, decode_signed_id, encode_signed_id},
    },
};

struct Fixture {
    root: TempDir,
    schema: SchemaRef,
    target: SqliteTarget,
}
impl Fixture {
    fn new(schema: SchemaRef) -> Self {
        let root = tempfile::tempdir().unwrap();
        let target = SqliteTarget::try_new(
            root.path().join("sink.sqlite"),
            "materialized".into(),
            Arc::clone(&schema),
        )
        .unwrap();
        Self {
            root,
            schema,
            target,
        }
    }
    fn fresh_target(&self) -> SqliteTarget {
        SqliteTarget::try_new(
            self.root.path().join("sink.sqlite"),
            "materialized".into(),
            Arc::clone(&self.schema),
        )
        .unwrap()
    }
    fn connection(&self) -> Connection {
        Connection::open(self.root.path().join("sink.sqlite")).unwrap()
    }
    fn initialize(&mut self) {
        self.target.require_absent().unwrap();
        self.target.initialize().unwrap();
    }
    fn deliver(&mut self, input: &Change, start: u64, tail: u64) -> Result<(), OperationError> {
        let delivery = DeliveryBatch::for_test(input.clone(), start)?;
        self.target.deliver_prefix(&delivery, tail, (start, input))
    }
    fn frontier(&self) -> u64 {
        let next: i64 = self
            .connection()
            .query_row(
                "SELECT next_event FROM \"$dogpaddle.frontier.materialized\"",
                [],
                |row| row.get(0),
            )
            .unwrap();
        u64::from_ne_bytes(next.to_ne_bytes()) ^ (1_u64 << 63)
    }
    fn rows(&self) -> Vec<(u64, i64)> {
        self.connection()
            .prepare("SELECT \"$dogpaddle.id\",value FROM materialized ORDER BY \"$dogpaddle.id\"")
            .unwrap()
            .query_map([], |row| {
                Ok((decode_signed_id(row.get(0)?).unwrap(), row.get(1)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}
fn values(schema: &SchemaRef, values: &[i64], diffs: &[i64]) -> Change {
    Change::try_new(
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap(),
        Int64Array::from(diffs.to_vec()),
    )
    .unwrap()
}

#[test]
fn initialization_replay_requires_all_exact_empty_objects_and_frontier_one() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    fixture.fresh_target().initialize().unwrap();
    assert!(matches!(
        fixture.fresh_target().require_absent(),
        Err(SqliteSinkError::TargetExists { .. })
    ));
    fixture
        .connection()
        .execute(
            "UPDATE \"$dogpaddle.frontier.materialized\" SET next_event=?1",
            [encode_signed_id(2)],
        )
        .unwrap();
    assert!(fixture.fresh_target().initialize().is_err());
    assert_eq!(fixture.frontier(), 2);
}

#[test]
fn ready_never_recreates_missing_frontier_or_accepts_wrong_catalog_objects() {
    for damage in [
        "DROP TABLE \"$dogpaddle.frontier.materialized\"",
        "CREATE TRIGGER unexpected AFTER UPDATE ON \"$dogpaddle.frontier.materialized\" BEGIN SELECT 1; END",
        "ALTER TABLE \"$dogpaddle.frontier.materialized\" ADD COLUMN extra INTEGER",
        "CREATE INDEX unexpected ON \"$dogpaddle.frontier.materialized\"(next_event)",
        "ALTER TABLE \"$dogpaddle.frontier.materialized\" RENAME TO temporary_frontier; ALTER TABLE temporary_frontier RENAME TO \"$DOGPADDLE.frontier.materialized\"",
        "CREATE INDEX unexpected ON materialized(value)",
    ] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture.connection().execute_batch(damage).unwrap();
        fixture.target = fixture.fresh_target();
        let input = values(&fixture.schema, &[7], &[1]);
        assert!(fixture.deliver(&input, 1, 2).is_err());
        assert!(fixture.rows().is_empty());
        assert!(fixture.fresh_target().initialize().is_err());
    }
}

#[test]
fn old_identity_layout_is_rejected_without_rewriting_it() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let connection = fixture.connection();
    let ddl: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE name='materialized'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let old = ddl.replace("$dogpaddle.event-prefix.v1", "$dogpaddle.event-address.v1");
    connection.execute_batch("DROP TABLE materialized").unwrap();
    connection.execute_batch(&old).unwrap();
    connection.execute_batch("CREATE INDEX \"$dogpaddle.hash_index.materialized\" ON \"materialized\"(\"$dogpaddle.hash\")").unwrap();
    assert!(fixture.fresh_target().initialize().is_err());
    assert_eq!(
        connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='materialized'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        old
    );
}

#[test]
fn unknown_longer_commit_short_recut_and_late_replay_preserve_fifo() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let full = values(&fixture.schema, &[7, 8, 7, 8, 7, 8], &[3, 2, -2, 1, 4, -1]);
    let old = values(&fixture.schema, &[7, 8, 7, 8], &[3, 2, -2, 1]);
    fixture.deliver(&old, 1, 14).unwrap();
    assert_eq!(fixture.frontier(), 9);
    fixture.target = fixture.fresh_target();
    fixture
        .deliver(&values(&fixture.schema, &[7], &[2]), 1, 14)
        .unwrap();
    assert_eq!(fixture.frontier(), 9);
    fixture.deliver(&full, 1, 14).unwrap();
    assert_eq!(
        fixture.rows(),
        [(3, 7), (5, 8), (8, 8), (9, 7), (10, 7), (11, 7), (12, 7)]
    );
    assert_eq!(fixture.frontier(), 14);
    fixture.deliver(&old, 1, 14).unwrap();
    assert_eq!(
        fixture.rows(),
        [(3, 7), (5, 8), (8, 8), (9, 7), (10, 7), (11, 7), (12, 7)]
    );
}

#[test]
fn existing_occurrences_remain_fifo_when_positive_and_negative_merge_or_split() {
    for split in [false, true] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture
            .deliver(&values(&fixture.schema, &[7], &[1]), 1, 2)
            .unwrap();
        if split {
            fixture
                .deliver(&values(&fixture.schema, &[7], &[1]), 2, 4)
                .unwrap();
            fixture
                .deliver(&values(&fixture.schema, &[7], &[-1]), 3, 4)
                .unwrap();
        } else {
            fixture
                .deliver(&values(&fixture.schema, &[7, 7], &[1, -1]), 2, 4)
                .unwrap();
        }
        assert_eq!(fixture.rows(), [(2, 7)]);
        assert_eq!(fixture.frontier(), 4);
    }
}

#[test]
fn deleted_zero_has_no_tombstone_and_old_birth_cannot_reappear_after_reopen() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let birth = values(&fixture.schema, &[7], &[1]);
    fixture.deliver(&birth, 1, 3).unwrap();
    fixture
        .deliver(&values(&fixture.schema, &[7], &[-1]), 2, 3)
        .unwrap();
    assert!(fixture.rows().is_empty());
    fixture.target = fixture.fresh_target();
    fixture.deliver(&birth, 1, 3).unwrap();
    assert!(fixture.rows().is_empty());
    assert_eq!(fixture.frontier(), 3);
}

#[test]
fn gap_future_frontier_and_negative_prefix_leave_rows_and_frontier_unchanged() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    assert!(
        fixture
            .deliver(&values(&fixture.schema, &[7], &[1]), 2, 3)
            .is_err()
    );
    assert!(
        fixture
            .deliver(&values(&fixture.schema, &[7, 7], &[-1, 1]), 1, 3)
            .is_err()
    );
    assert!(fixture.rows().is_empty());
    assert_eq!(fixture.frontier(), 1);
    fixture
        .connection()
        .execute(
            "UPDATE \"$dogpaddle.frontier.materialized\" SET next_event=?1",
            [encode_signed_id(5)],
        )
        .unwrap();
    assert!(
        fixture
            .deliver(&values(&fixture.schema, &[7], &[1]), 1, 2)
            .is_err()
    );
    assert!(fixture.rows().is_empty());
    assert_eq!(fixture.frontier(), 5);
}

#[test]
fn actual_second_unique_error_rolls_back_first_insert_and_frontier() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    fixture
        .deliver(&values(&fixture.schema, &[7], &[1]), 1, 2)
        .unwrap();
    fixture
        .connection()
        .execute_batch("CREATE UNIQUE INDEX reject_duplicate_value ON materialized(value)")
        .unwrap();
    let error = fixture
        .deliver(&values(&fixture.schema, &[8, 7], &[1, 1]), 2, 4)
        .unwrap_err();
    let sqlite = error
        .downcast_ref::<rusqlite::Error>()
        .or_else(|| match error.downcast_ref::<SqliteSinkError>() {
            Some(SqliteSinkError::Sqlite(error)) => Some(error),
            _ => None,
        })
        .unwrap();
    assert!(
        matches!(sqlite,rusqlite::Error::SqliteFailure(code,_) if code.extended_code==rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE)
    );
    assert_eq!(fixture.rows(), [(1, 7)]);
    assert_eq!(fixture.frontier(), 2);
}

#[test]
fn frontier_accepts_max_exclusive_tail_and_crosses_signed_boundary() {
    for start in [(1_u64 << 63) - 1, u64::MAX - 1] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture
            .connection()
            .execute(
                "UPDATE \"$dogpaddle.frontier.materialized\" SET next_event=?1",
                [encode_signed_id(start)],
            )
            .unwrap();
        let input = values(&fixture.schema, &[7], &[1]);
        fixture.deliver(&input, start, start + 1).unwrap();
        assert_eq!(fixture.rows(), [(start, 7)]);
        assert_eq!(fixture.frontier(), start + 1);
        fixture.target = fixture.fresh_target();
        fixture.deliver(&input, start, start + 1).unwrap();
        assert_eq!(fixture.rows(), [(start, 7)]);
        if start != u64::MAX - 1 {
            fixture.deliver(&input, start + 1, start + 2).unwrap();
            assert_eq!(fixture.rows(), [(start, 7), (start + 1, 7)]);
            assert_eq!(fixture.frontier(), start + 2);
        }
    }
}

#[test]
fn delivery_observes_another_connection_commit_or_rollback() {
    for commit in [false, true] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture
            .deliver(&values(&fixture.schema, &[7], &[1]), 1, 3)
            .unwrap();
        let mut connection = fixture.connection();
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        transaction.execute("INSERT INTO materialized SELECT ?1,\"$dogpaddle.hash\",value FROM materialized WHERE \"$dogpaddle.id\"=?2",params![encode_signed_id(2),encode_signed_id(1)]).unwrap();
        transaction
            .execute(
                "UPDATE \"$dogpaddle.frontier.materialized\" SET next_event=?1",
                [encode_signed_id(3)],
            )
            .unwrap();
        let input = values(&fixture.schema, &[7], &[1]);
        let fresh = fixture.fresh_target();
        let mut target = std::mem::replace(&mut fixture.target, fresh);
        std::thread::scope(|scope| {
            let (started_send, started_receive) = std::sync::mpsc::channel();
            let (result_send, result_receive) = std::sync::mpsc::channel();
            scope.spawn(move || {
                started_send.send(()).unwrap();
                let delivery = DeliveryBatch::for_test(input.clone(), 2).unwrap();
                result_send
                    .send(target.deliver_prefix(&delivery, 3, (2, &input)))
                    .unwrap();
            });
            started_receive.recv().unwrap();
            if commit {
                transaction.commit().unwrap();
            } else {
                drop(transaction);
            }
            result_receive
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .unwrap();
        });
        assert_eq!(fixture.rows(), [(1, 7), (2, 7)]);
        assert_eq!(fixture.frontier(), 3);
    }
}

#[test]
fn malformed_singleton_is_rejected_and_not_repaired() {
    for damage in [
        "DELETE FROM \"$dogpaddle.frontier.materialized\"",
        "PRAGMA ignore_check_constraints=ON; UPDATE \"$dogpaddle.frontier.materialized\" SET next_event=-9223372036854775808",
        "PRAGMA ignore_check_constraints=ON; INSERT INTO \"$dogpaddle.frontier.materialized\" VALUES (2,-9223372036854775807)",
    ] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture.connection().execute_batch(damage).unwrap();
        assert!(
            fixture
                .deliver(&values(&fixture.schema, &[7], &[1]), 1, 2)
                .is_err()
        );
        assert!(fixture.rows().is_empty());
    }
}

#[test]
fn reserved_stored_occurrence_ids_fail_lookup_without_changing_the_frontier() {
    for id in [0, u64::MAX] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        fixture
            .deliver(&values(&fixture.schema, &[7], &[1]), 1, 3)
            .unwrap();
        let connection = fixture.connection();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection
            .execute(
                "UPDATE materialized SET \"$dogpaddle.id\"=?1",
                [encode_signed_id(id)],
            )
            .unwrap();
        let error = fixture
            .deliver(&values(&fixture.schema, &[7], &[-1]), 2, 3)
            .unwrap_err();
        assert!(
            matches!(error.downcast_ref::<SqliteSinkError>(),Some(SqliteSinkError::InvalidStoredTechnicalId{id:stored}) if *stored==encode_signed_id(id))
        );
        assert_eq!(
            connection
                .query_row("SELECT \"$dogpaddle.id\" FROM materialized", [], |row| row
                    .get::<_, i64>(
                    0
                ))
                .unwrap(),
            encode_signed_id(id)
        );
        assert_eq!(fixture.frontier(), 2);
    }
}

#[test]
fn hash_collision_and_null_or_nul_text_retract_only_the_exact_row() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let make = |texts: Vec<Option<&str>>, diffs: Vec<i64>| {
        Change::try_new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(texts))],
            )
            .unwrap(),
            Int64Array::from(diffs),
        )
        .unwrap()
    };
    let mut fixture = Fixture::new(Arc::clone(&schema));
    fixture.initialize();
    let input = make(vec![Some("a\0b"), Some("a\0c"), None], vec![1, 1, 1]);
    fixture.deliver(&input, 1, 6).unwrap();
    let hash = fixture.target.encode_row(&input, 0).unwrap().hash;
    fixture
        .connection()
        .execute(
            "UPDATE materialized SET \"$dogpaddle.hash\"=?1 WHERE \"$dogpaddle.id\"=?2",
            params![hash, encode_signed_id(2)],
        )
        .unwrap();
    fixture
        .deliver(&make(vec![None, Some("a\0b")], vec![-1, -1]), 4, 6)
        .unwrap();
    let rows = fixture
        .connection()
        .prepare("SELECT \"$dogpaddle.id\",text FROM materialized")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, [(encode_signed_id(2), "a\0c".to_owned())]);
    assert_eq!(fixture.frontier(), 6);
}

#[test]
fn empty_and_maximum_width_schemas_deliver_and_retract() {
    for width in [0, 1998] {
        let schema = Arc::new(Schema::new(
            (0..width)
                .map(|index| Field::new(format!("f{index}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let records = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            (0..width)
                .map(|_| Arc::new(Int64Array::from(vec![None])) as ArrayRef)
                .collect(),
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let mut fixture = Fixture::new(schema);
        fixture.initialize();
        fixture
            .deliver(
                &Change::try_new(records.clone(), Int64Array::from(vec![1])).unwrap(),
                1,
                3,
            )
            .unwrap();
        fixture
            .deliver(
                &Change::try_new(records, Int64Array::from(vec![-1])).unwrap(),
                2,
                3,
            )
            .unwrap();
        assert_eq!(
            fixture
                .connection()
                .query_row("SELECT count(*) FROM materialized", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(fixture.frontier(), 3);
    }
}
