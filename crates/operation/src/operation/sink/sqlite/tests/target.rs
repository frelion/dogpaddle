use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dogpaddle_change::Change;
use rusqlite::{Connection, params};
use tempfile::TempDir;

use super::super::{error::SqliteSinkError, target::SqliteTarget};
use crate::operation::sink::relation::{
    Batch, Delete, Insert, Lookup, RelationTarget, decode_signed_id, encode_signed_id,
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
            "materialized".to_owned(),
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
            "materialized".to_owned(),
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

    fn rows(&self) -> Vec<(u64, i64)> {
        let stored = self
            .connection()
            .prepare("SELECT \"$dogpaddle.id\", value FROM materialized ORDER BY \"$dogpaddle.id\"")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        stored
            .into_iter()
            .map(|(id, row)| (decode_signed_id(id).unwrap(), row))
            .collect()
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn change(schema: &SchemaRef, values: Vec<ArrayRef>) -> Change {
    let records = RecordBatch::try_new_with_options(
        Arc::clone(schema),
        values,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let rows = records.num_rows();
    Change::try_new(records, Int64Array::from(vec![1; rows])).unwrap()
}

fn values(schema: &SchemaRef, values: &[i64]) -> Change {
    let records = RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    Change::try_new(records, Int64Array::from(vec![1; values.len()])).unwrap()
}

fn batch(inserts: &[(u64, u64)], deletes: &[(u64, u64)]) -> Batch {
    Batch {
        inserts: inserts
            .iter()
            .map(|&(row_index, technical_id)| Insert {
                row_index,
                technical_id,
            })
            .collect(),
        deletes: deletes
            .iter()
            .map(|&(row_index, technical_id)| Delete {
                row_index,
                technical_id,
            })
            .collect(),
    }
}

#[test]
fn initialization_requires_absence_and_replay_requires_the_exact_empty_layout() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    fixture.fresh_target().initialize().unwrap();
    assert!(matches!(
        fixture.fresh_target().require_absent(),
        Err(SqliteSinkError::TargetExists { .. })
    ));
    let input = values(&fixture.schema, &[7]);
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 1)], &[]))
        .unwrap();
    assert!(matches!(
        fixture.fresh_target().initialize(),
        Err(SqliteSinkError::TargetNotEmpty { .. })
    ));
}

#[test]
fn initialization_and_reconnect_reject_extra_schema_objects() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    fixture
        .connection()
        .execute_batch(
            "CREATE TRIGGER unexpected AFTER INSERT ON materialized BEGIN SELECT 1; END;",
        )
        .unwrap();
    assert!(matches!(
        fixture.fresh_target().initialize(),
        Err(SqliteSinkError::TargetLayoutMismatch { .. })
    ));
    let input = values(&fixture.schema, &[7]);
    assert!(
        fixture
            .fresh_target()
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    take: 1,
                }]
            )
            .is_err()
    );
}

#[test]
fn fixed_inserts_and_deletes_replay_as_one_idempotent_batch() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7, 8]);
    let writes = batch(&[(0, 1), (1, 2), (0, 3)], &[(0, 1), (0, 3), (0, 999)]);
    fixture.target.write_batch(&input, &writes).unwrap();
    assert_eq!(fixture.rows(), [(2, 8)]);
    fixture.target = fixture.fresh_target();
    fixture.target.write_batch(&input, &writes).unwrap();
    assert_eq!(fixture.rows(), [(2, 8)]);
}

#[test]
fn event_addresses_preserve_order_across_the_signed_boundary_and_replay() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7]);
    let ids = [1, i64::MAX.unsigned_abs(), 1_u64 << 63, u64::MAX - 1];
    let writes = batch(&ids.map(|id| (0, id)), &[]);
    fixture.target.write_batch(&input, &writes).unwrap();
    fixture.target = fixture.fresh_target();
    fixture.target.write_batch(&input, &writes).unwrap();
    assert_eq!(fixture.rows(), ids.map(|id| (id, 7)));
    let found = fixture
        .target
        .lookup(
            &input,
            &[Lookup {
                row_index: 0,
                take: 4,
            }],
        )
        .unwrap();
    assert_eq!(found[0].ids, ids);
    let sql_ids = fixture
        .connection()
        .prepare("SELECT \"$dogpaddle.id\" FROM materialized ORDER BY \"$dogpaddle.id\"")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(sql_ids, [i64::MIN + 1, -1, 0, i64::MAX - 1]);
    fixture
        .target
        .write_batch(&input, &batch(&[], &[(0, 1_u64 << 63), (0, u64::MAX - 1)]))
        .unwrap();
    assert_eq!(fixture.rows(), [(1, 7), (i64::MAX.unsigned_abs(), 7)]);
}

#[test]
fn reserved_event_addresses_are_rejected_on_write_and_target_read() {
    for id in [0, u64::MAX] {
        let mut fixture = Fixture::new(schema());
        fixture.initialize();
        let input = values(&fixture.schema, &[7]);
        assert!(
            fixture
                .target
                .write_batch(&input, &batch(&[(0, id)], &[]))
                .is_err()
        );
        assert!(fixture.rows().is_empty());
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1)], &[]))
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
        assert!(
            fixture
                .target
                .lookup(
                    &input,
                    &[Lookup {
                        row_index: 0,
                        take: 1
                    }]
                )
                .is_err()
        );
    }
}

#[test]
fn initialization_rejects_the_previous_identity_layout_without_modifying_it() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let connection = fixture.connection();
    let current: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='materialized'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let guard = format!(
        " CONSTRAINT \"$dogpaddle.event-address.v1\" \
         CHECK(\"$dogpaddle.id\" > {} AND \"$dogpaddle.id\" < {})",
        i64::MIN,
        i64::MAX
    );
    let old = current.replace(&guard, "");
    assert_ne!(old, current);
    connection.execute_batch("DROP TABLE materialized").unwrap();
    connection.execute_batch(&old).unwrap();
    connection
        .execute_batch(
            "CREATE INDEX \"$dogpaddle.hash_index.materialized\" \
         ON \"materialized\"(\"$dogpaddle.hash\")",
        )
        .unwrap();
    assert!(matches!(
        fixture.fresh_target().initialize(),
        Err(SqliteSinkError::TargetLayoutMismatch { .. })
    ));
    let retained: String = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='materialized'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, old);
}

#[test]
fn mutation_row_index_is_rejected_while_the_adapter_encodes_the_row() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7]);

    assert!(
        fixture
            .target
            .write_batch(&input, &batch(&[(1, 1)], &[]))
            .is_err()
    );
    assert!(fixture.rows().is_empty());
}

#[test]
fn replay_rejects_an_existing_id_bound_to_a_different_row() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7, 8]);
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 1)], &[]))
        .unwrap();

    assert!(
        fixture
            .target
            .write_batch(&input, &batch(&[(1, 1)], &[]))
            .is_err()
    );
    assert!(
        fixture
            .target
            .write_batch(&input, &batch(&[], &[(1, 1)]))
            .is_err()
    );
    assert_eq!(fixture.rows(), [(1, 7)]);

    fixture
        .target
        .write_batch(&input, &batch(&[], &[(1, 999)]))
        .unwrap();
    assert_eq!(fixture.rows(), [(1, 7)]);
}

#[test]
fn only_primary_key_duplicates_are_ignored_and_other_errors_roll_back_the_batch() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7, 8]);
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 1)], &[]))
        .unwrap();
    fixture
        .connection()
        .execute_batch(
            "CREATE TRIGGER reject_eight BEFORE INSERT ON materialized \
             WHEN NEW.value = 8 BEGIN SELECT RAISE(ABORT, 'rejected'); END;",
        )
        .unwrap();
    assert!(
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 2), (1, 3)], &[(0, 1)]))
            .is_err()
    );
    assert_eq!(fixture.rows(), [(1, 7)]);
    fixture
        .connection()
        .execute_batch("DROP TRIGGER reject_eight")
        .unwrap();
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 2), (1, 3)], &[(0, 1)]))
        .unwrap();
    assert_eq!(fixture.rows(), [(2, 7), (3, 8)]);
}

#[test]
fn lookup_returns_only_the_requested_smallest_ids_without_full_count() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7]);
    for offset in [0_u64, 1_024] {
        let writes = batch(
            &(1..=1_024).map(|id| (0, offset + id)).collect::<Vec<_>>(),
            &[],
        );
        fixture.target.write_batch(&input, &writes).unwrap();
    }
    let matches = fixture
        .target
        .lookup(
            &input,
            &[Lookup {
                row_index: 0,
                take: 4,
            }],
        )
        .unwrap();
    assert_eq!(matches[0].ids.len(), 4);
    assert_eq!(matches[0].ids, [1, 2, 3, 4]);
    let matches = fixture
        .target
        .lookup(
            &input,
            &[Lookup {
                row_index: 0,
                take: 1_024,
            }],
        )
        .unwrap();
    assert_eq!(matches[0].ids.len(), 1024);
    assert_eq!(matches[0].ids.len(), 1_024);
    fixture
        .target
        .write_batch(
            &input,
            &batch(&[], &(1..=1_024).map(|id| (0, id)).collect::<Vec<_>>()),
        )
        .unwrap();
    let matches = fixture
        .target
        .lookup(
            &input,
            &[Lookup {
                row_index: 0,
                take: 4,
            }],
        )
        .unwrap();
    assert_eq!(matches[0].ids, [1_025, 1_026, 1_027, 1_028]);
}

#[test]
fn hash_collisions_do_not_match_other_logical_rows() {
    let mut fixture = Fixture::new(schema());
    fixture.initialize();
    let input = values(&fixture.schema, &[7, 8]);
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 1), (1, 2), (0, 3)], &[]))
        .unwrap();
    let hash = fixture.target.encode_row(&input, 0).unwrap().hash;
    fixture
        .connection()
        .execute(
            "UPDATE materialized SET \"$dogpaddle.hash\" = ?1 WHERE \"$dogpaddle.id\" = ?2",
            params![hash, encode_signed_id(2)],
        )
        .unwrap();
    let matches = fixture
        .target
        .lookup(
            &input,
            &[Lookup {
                row_index: 0,
                take: 3,
            }],
        )
        .unwrap();
    assert_eq!(matches[0].ids.len(), 2);
    assert_eq!(matches[0].ids, [1, 3]);
}

#[test]
fn null_and_nul_containing_text_are_matched_exactly_in_request_order() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let input = Change::try_new(
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("a\0b"),
                Some("a\0c"),
                None,
            ]))],
        )
        .unwrap(),
        Int64Array::from(vec![1, 1, 1]),
    )
    .unwrap();
    let mut fixture = Fixture::new(schema);
    fixture.initialize();
    fixture
        .target
        .write_batch(&input, &batch(&[(0, 1), (1, 2), (2, 3)], &[]))
        .unwrap();
    let first_hash = fixture.target.encode_row(&input, 0).unwrap().hash;
    fixture
        .connection()
        .execute(
            "UPDATE materialized SET \"$dogpaddle.hash\" = ?1 WHERE \"$dogpaddle.id\" = ?2",
            params![first_hash, encode_signed_id(2)],
        )
        .unwrap();
    let matches = fixture
        .target
        .lookup(
            &input,
            &[
                Lookup {
                    row_index: 2,
                    take: 2,
                },
                Lookup {
                    row_index: 0,
                    take: 2,
                },
            ],
        )
        .unwrap();
    assert_eq!(matches[0].ids, [3]);
    assert_eq!(matches[1].ids, [1]);
}

#[test]
fn empty_and_maximum_width_schemas_support_insert_lookup_and_delete() {
    for fields in [0, 1_998] {
        let schema = Arc::new(Schema::new(
            (0..fields)
                .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let input = change(
            &schema,
            (0..fields)
                .map(|_| Arc::new(Int64Array::from(vec![None])) as ArrayRef)
                .collect(),
        );
        let mut fixture = Fixture::new(schema);
        fixture.initialize();
        fixture
            .target
            .write_batch(&input, &batch(&[(0, 1)], &[]))
            .unwrap();
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    take: 1,
                }],
            )
            .unwrap();
        assert_eq!(matches[0].ids, [1]);
        fixture
            .target
            .write_batch(&input, &batch(&[], &[(0, 1)]))
            .unwrap();
        let matches = fixture
            .target
            .lookup(
                &input,
                &[Lookup {
                    row_index: 0,
                    take: 1,
                }],
            )
            .unwrap();
        assert!(matches[0].ids.is_empty());
    }
}
