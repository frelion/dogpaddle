use arrow_schema::Schema;
use std::{
    fmt::Write as _,
    sync::Arc,
    time::{Duration, Instant},
};

use dogpaddle_change::Change;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, GenericClient, IsolationLevel, types::ToSql};

use crate::operation::{
    OperationError,
    sink::{
        buffered::DeliveryBatch,
        relation::{
            Lookup, MAX_MUTATIONS_PER_BATCH, Matches, RelationTarget, decode_signed_id,
            encode_signed_id, plan, validate_technical_id,
        },
    },
};

use super::{
    config::{PgClient, PostgresSinkConfig, PostgresTargetSpec, bounded_until, require_absent},
    error::{PostgresSinkError, database_error, invalid_batch},
    row::{EncodedRow, HASH_LENGTH, PostgresRowCodec, PostgresValue},
    schema::{self, TECHNICAL_HASH, TECHNICAL_ID},
};

/// Database-specific SQL and connection. Durable work belongs to the shared sink.
pub(super) struct PostgresTarget {
    config: PostgresSinkConfig,
    spec: PostgresTargetSpec,
    row_codec: PostgresRowCodec,
    sql: SqlPlan,
    client: Option<PgClient>,
    layout_verified: bool,
}

impl PostgresTarget {
    pub(super) fn new_bound(
        config: PostgresSinkConfig,
        spec: PostgresTargetSpec,
        codec: PostgresRowCodec,
    ) -> Self {
        let sql = SqlPlan::new(&spec, codec.schema());
        Self {
            config,
            spec,
            row_codec: codec,
            sql,
            client: None,
            layout_verified: false,
        }
    }

    fn with_client<R>(
        &mut self,
        action: impl FnOnce(
            &Runtime,
            &mut Client,
            &PostgresTargetSpec,
            &SqlPlan,
            &PostgresRowCodec,
            &mut bool,
            Instant,
        ) -> Result<R, OperationError>,
    ) -> Result<R, OperationError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let Self {
            config,
            spec,
            row_codec,
            sql,
            client: session,
            layout_verified,
        } = self;
        if config.database() != spec.database() {
            return Err(PostgresSinkError::DatabaseMismatch.into());
        }
        if session.is_none() {
            let mut opened =
                config.connect_with_timeout(deadline.saturating_duration_since(Instant::now()))?;
            let PgClient {
                runtime,
                client: opened_client,
            } = &mut opened;
            bounded_until(runtime, deadline, "verify target identity", async {
                verify_identity(opened_client, spec).await
            })?;
            *session = Some(opened);
            *layout_verified = false;
        }
        let PgClient { runtime, client } =
            session.as_mut().expect("the client was initialized above");
        let result = action(
            runtime,
            client,
            spec,
            sql,
            row_codec,
            layout_verified,
            deadline,
        );
        if result.is_err() {
            // No failed transaction or poisoned connection survives a retry.
            *session = None;
            *layout_verified = false;
        }
        result
    }
}

impl RelationTarget for PostgresTarget {
    fn require_absent(&mut self) -> Result<(), OperationError> {
        self.with_client(|runtime, client, spec, _, _, _, deadline| {
            bounded_until(runtime, deadline, "check target absence", async {
                require_absent(client, spec).await
            })
            .map_err(Into::into)
        })
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        self.with_client(|runtime, client, spec, sql, _, layout_verified, deadline| {
            bounded_until(runtime, deadline, "initialize target", async {
                let transaction = client
                    .build_transaction()
                    .isolation_level(IsolationLevel::Serializable)
                    .start()
                    .await
                    .map_err(|error| database_error("begin initialization", &error))?;
                match object_count(&transaction, spec).await? {
                    0 => transaction
                        .batch_execute(&sql.initialize)
                        .await
                        .map_err(|error| database_error("create target layout", &error))?,
                    count if count == spec.object_names().len() => (),
                    _ => {
                        return Err(PostgresSinkError::TargetLayoutMismatch {
                            name: spec.table().to_owned(),
                        });
                    }
                }
                require_owned_layout(&transaction, spec, sql).await?;
                let empty: bool = transaction
                    .query_one(&sql.target_empty, &[])
                    .await
                    .map_err(|error| database_error("verify empty target", &error))?
                    .get(0);
                if !empty {
                    return Err(PostgresSinkError::TargetNotEmpty);
                }
                if read_frontier(&transaction, sql).await? != 1 {
                    return Err(invalid_batch("initialization frontier is not one"));
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| database_error("commit initialization", &error))?;
                *layout_verified = true;
                Ok(())
            })
            .map_err(Into::into)
        })
    }

    fn deliver_prefix(
        &mut self,
        input: &DeliveryBatch,
        tail: u64,
        original_head: (u64, &Change),
    ) -> Result<(), OperationError> {
        self.with_client(
            |runtime, client, spec, sql, codec, layout_verified, deadline| {
                let transaction = bounded_until(runtime, deadline, "begin delivery", async {
                    client
                        .build_transaction()
                        .isolation_level(IsolationLevel::ReadCommitted)
                        .start()
                        .await
                        .map_err(|error| database_error("begin delivery", &error))
                })?;
                let frontier = bounded_until(runtime, deadline, "lock target frontier", async {
                    verify_once(&transaction, spec, sql, layout_verified).await?;
                    read_frontier(&transaction, sql).await
                })?;
                let end = input.end_event_offset()?;
                if frontier < input.first_event_offset() || frontier > tail || end > tail {
                    return Err(invalid_batch(
                        "target frontier is outside the loaded durable prefix",
                    )
                    .into());
                }
                if end <= frontier {
                    bounded_until(runtime, deadline, "finish covered prefix", async {
                        transaction
                            .commit()
                            .await
                            .map_err(|error| database_error("finish covered prefix", &error))
                    })?;
                    return Ok(());
                }
                let batch = plan(input, frontier, tail, original_head, |requests| {
                    bounded_until(runtime, deadline, "match locked target rows", async {
                        lookup(
                            &transaction,
                            sql,
                            codec,
                            input.change(),
                            requests,
                            frontier - 1,
                        )
                        .await
                    })
                    .map_err(Into::into)
                })?;
                let mut inserts = Vec::with_capacity(batch.inserts.len());
                for group in batch
                    .inserts
                    .chunk_by(|left, right| left.row_index == right.row_index)
                {
                    let row_index = usize::try_from(group[0].row_index)?;
                    let row = Arc::new(codec.encode_row(input.change().records(), row_index)?);
                    for insert in group {
                        inserts.push(EncodedInsert {
                            technical_id: technical_id_as_i64(insert.technical_id)?,
                            row: Arc::clone(&row),
                        });
                    }
                }
                let deletes = batch
                    .deletes
                    .iter()
                    .map(|delete| technical_id_as_i64(delete.technical_id))
                    .collect::<Result<Vec<_>, _>>()?;
                bounded_until(runtime, deadline, "write locked target suffix", async {
                    for inserts in inserts.chunks(sql.insert_batch_size()) {
                        insert_batch(&transaction, sql, inserts).await?;
                    }
                    if !deletes.is_empty() {
                        transaction
                            .execute(&sql.delete, &[&deletes])
                            .await
                            .map_err(|error| database_error("delete target rows", &error))?;
                    }
                    let affected = transaction
                        .execute(
                            &sql.advance_frontier,
                            &[&encode_signed_id(end), &encode_signed_id(frontier)],
                        )
                        .await
                        .map_err(|error| database_error("advance target frontier", &error))?;
                    if affected != 1 {
                        return Err(invalid_batch("locked frontier changed or disappeared"));
                    }
                    transaction
                        .commit()
                        .await
                        .map_err(|error| database_error("commit delivery", &error))?;
                    Ok(())
                })?;
                Ok(())
            },
        )
    }
}

struct EncodedInsert {
    technical_id: i64,
    row: Arc<EncodedRow>,
}

async fn lookup(
    transaction: &tokio_postgres::Transaction<'_>,
    sql: &SqlPlan,
    codec: &PostgresRowCodec,
    input: &Change,
    requests: &[Lookup],
    through: u64,
) -> Result<Vec<Matches>, PostgresSinkError> {
    let mut matches = Vec::with_capacity(requests.len());
    for batch in requests.chunks(sql.lookup_batch_size()) {
        let encoded = batch
            .iter()
            .map(|request| codec.encode_row(input.records(), request.row_index))
            .collect::<Result<Vec<_>, _>>()?;
        let limits = batch
            .iter()
            .map(|request| {
                i64::try_from(request.take).map_err(|_| invalid_batch("lookup take exceeds bigint"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let hashes = encoded
            .iter()
            .map(|row| row.hash.as_slice())
            .collect::<Vec<_>>();
        let mut parameters: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for ((row, hash), take) in encoded.iter().zip(&hashes).zip(&limits) {
            parameters.extend([take as &(dyn ToSql + Sync), hash]);
            parameters.extend(row.values.iter().map(PostgresValue::as_parameter));
        }
        let rows = transaction
            .query(&sql.lookup_statement(batch.len()), &parameters)
            .await
            .map_err(|error| database_error("match target rows", &error))?;
        if rows.len() != batch.len() {
            return Err(invalid_batch("lookup request count differs"));
        }
        for (index, row) in rows.into_iter().enumerate() {
            if row.get::<_, i32>(0) != i32::try_from(index).expect("bounded request index fits int")
            {
                return Err(invalid_batch("lookup request order differs"));
            }
            let ids = row
                .get::<_, Vec<i64>>(1)
                .into_iter()
                .map(|id| decode_signed_id(id).map_err(|error| invalid_batch(error.to_string())))
                .collect::<Result<Vec<_>, _>>()?;
            matches.push(Matches { through, ids });
        }
    }
    Ok(matches)
}

async fn read_frontier(
    transaction: &tokio_postgres::Transaction<'_>,
    sql: &SqlPlan,
) -> Result<u64, PostgresSinkError> {
    let rows = transaction
        .query(&sql.read_frontier, &[])
        .await
        .map_err(|error| database_error("lock target frontier", &error))?;
    if rows.len() != 1 || rows[0].get::<_, i16>(0) != 1 {
        return Err(invalid_batch("owned frontier is not one singleton"));
    }
    let next = rows[0].get::<_, i64>(1);
    if next == i64::MIN {
        return Err(invalid_batch("owned frontier is zero"));
    }
    Ok(u64::from_ne_bytes(next.to_ne_bytes()) ^ (1_u64 << 63))
}

async fn insert_batch(
    transaction: &tokio_postgres::Transaction<'_>,
    sql: &SqlPlan,
    inserts: &[EncodedInsert],
) -> Result<(), PostgresSinkError> {
    let hashes = inserts
        .iter()
        .map(|insert| insert.row.hash.as_slice())
        .collect::<Vec<_>>();
    let mut parameters: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for (insert, hash) in inserts.iter().zip(&hashes) {
        parameters.extend([&insert.technical_id as &(dyn ToSql + Sync), hash]);
        parameters.extend(insert.row.values.iter().map(PostgresValue::as_parameter));
    }
    transaction
        .execute(&sql.insert_statement(inserts.len()), &parameters)
        .await
        .map_err(|error| database_error("insert target rows", &error))?;
    Ok(())
}

fn technical_id_as_i64(id: u64) -> Result<i64, PostgresSinkError> {
    validate_technical_id(id).map_err(|error| invalid_batch(error.to_string()))?;
    Ok(encode_signed_id(id))
}

pub(super) struct SqlPlan {
    table_name: String,
    frontier_name: String,
    marker: String,
    pub(super) initialize: String,
    insert_prefix: String,
    parameter_types: Vec<&'static str>,
    lookup_prefix: String,
    lookup_suffix: String,
    read_frontier: String,
    advance_frontier: String,
    pub(super) delete: String,
    target_empty: String,
}

impl SqlPlan {
    #[allow(clippy::too_many_lines)]
    pub(super) fn new(spec: &PostgresTargetSpec, schema: &Schema) -> Self {
        let target = qualified(spec.schema(), spec.table());
        let hash_index_name = spec.hash_index();
        let hash_index = qualified(spec.schema(), &hash_index_name);
        let frontier_name = spec.frontier_table();
        let frontier = qualified(spec.schema(), &frontier_name);
        let frontier_pk = spec.frontier_pk();
        let frontier_slot = format!("$dogpaddle.frontier_slot.{}", spec.sink_id());
        let frontier_next = format!("$dogpaddle.frontier_next.{}", spec.sink_id());
        let target_pk = format!("$dogpaddle.pk.{}", spec.sink_id());
        let id_check = format!("$dogpaddle.id_check.{}", spec.sink_id());
        let hash_check = format!("$dogpaddle.hash_check.{}", spec.sink_id());
        let id = quote_identifier(TECHNICAL_ID);
        let hash = quote_identifier(TECHNICAL_HASH);
        let mut definitions = vec![
            format!("{id} bigint NOT NULL"),
            format!("{hash} bytea NOT NULL"),
        ];
        for (index, column) in schema.fields().iter().enumerate() {
            let name = quote_identifier(column.name());
            let mut definition = format!("{name} {}", schema::sql_type(column.data_type()));
            if !schema::nullable(column) {
                definition.push_str(" NOT NULL");
            }
            let condition = schema::column_check(column.data_type(), &name);
            if let Some(condition) = condition {
                let constraint = format!("$dogpaddle.c.{index:04x}.{}", spec.sink_id());
                let checked = if schema::nullable(column)
                    && !matches!(column.data_type(), arrow_schema::DataType::Null)
                {
                    format!("{name} IS NULL OR ({condition})")
                } else {
                    condition
                };
                write!(
                    definition,
                    " CONSTRAINT {} CHECK ({checked})",
                    quote_identifier(&constraint)
                )
                .expect("writing SQL cannot fail");
            }
            definitions.push(definition);
        }
        definitions.extend([
            format!(
                "CONSTRAINT {} PRIMARY KEY ({id})",
                quote_identifier(&target_pk)
            ),
            format!(
                "CONSTRAINT {} CHECK ({id} > {} AND {id} < {})",
                quote_identifier(&id_check),
                i64::MIN,
                i64::MAX
            ),
            format!(
                "CONSTRAINT {} CHECK (octet_length({hash}) = {HASH_LENGTH})",
                quote_identifier(&hash_check)
            ),
        ]);
        let create_target = format!("CREATE TABLE {target} ({})", definitions.join(", "));
        let create_hash_index = format!(
            "CREATE INDEX {} ON {target} USING btree ({hash}, {id})",
            quote_identifier(&hash_index_name)
        );
        let create_frontier = format!(
            "CREATE TABLE {frontier} (singleton smallint NOT NULL, next_event bigint NOT NULL, CONSTRAINT {} PRIMARY KEY (singleton), CONSTRAINT {} CHECK (singleton = 1), CONSTRAINT {} CHECK (next_event > '{}'::bigint))",
            quote_identifier(&frontier_pk),
            quote_identifier(&frontier_slot),
            quote_identifier(&frontier_next),
            i64::MIN
        );
        let marker_hash = blake3::hash(
            format!("{create_target}\0{create_hash_index}\0{create_frontier}").as_bytes(),
        );
        let marker = format!(
            "dogpaddle.postgres-relation.event-prefix.v1:{}",
            marker_hash.to_hex()
        );
        let marker_literal = quote_literal(&marker);
        let initialize = format!(
            "{create_target}; {create_hash_index}; {create_frontier}; \
             INSERT INTO {frontier} VALUES (1, {}); \
             COMMENT ON TABLE {target} IS {marker_literal}; \
             COMMENT ON TABLE {frontier} IS {marker_literal}; \
             COMMENT ON INDEX {} IS {marker_literal}; \
             COMMENT ON INDEX {} IS {marker_literal}; \
             COMMENT ON INDEX {hash_index} IS {marker_literal}",
            encode_signed_id(1),
            qualified(spec.schema(), &frontier_pk),
            qualified(spec.schema(), &target_pk)
        );
        let logical_names = schema
            .fields()
            .iter()
            .map(|column| quote_identifier(column.name()))
            .collect::<Vec<_>>();
        let mut all_names = vec![id.clone(), hash.clone()];
        all_names.extend(logical_names.iter().cloned());
        let insert_prefix = format!("INSERT INTO {target} ({}) VALUES ", all_names.join(", "));
        let mut parameter_types = vec!["bigint", "bytea"];
        parameter_types.extend(
            schema
                .fields()
                .iter()
                .map(|column| schema::sql_type(column.data_type())),
        );

        let mut request_names = vec!["n".to_owned(), "take".to_owned(), "hash".to_owned()];
        request_names.extend((0..logical_names.len()).map(|index| format!("c{index}")));
        let mut exact = String::new();
        for (index, name) in logical_names.iter().enumerate() {
            // The explicit NULL branch lets PostgreSQL estimate nullable
            // predicates; IS NOT DISTINCT FROM can hide their selectivity
            // and turn a bounded ID lookup into a full scan and sort.
            write!(exact, " AND (target.{name} = request.c{index} OR (target.{name} IS NULL AND request.c{index} IS NULL))")
                .expect("writing SQL cannot fail");
        }
        let matching =
            format!("FROM ONLY {target} AS target WHERE target.{hash} = request.hash{exact}");
        // OFFSET 0 keeps the bounded ID array in its LATERAL subquery instead
        // of duplicating its scan when CASE and the result both reference it.
        let lookup_suffix = format!(
            ") AS request ({}) CROSS JOIN LATERAL \
             (SELECT ARRAY(SELECT target.{id} {matching} ORDER BY target.{id} LIMIT request.take) AS ids OFFSET 0) AS selected \
             ORDER BY request.n",
            request_names.join(", ")
        );
        let lookup_prefix = "SELECT request.n, selected.ids FROM (VALUES ".to_owned();
        let read_frontier =
            format!("SELECT singleton, next_event FROM ONLY {frontier} LIMIT 2 FOR UPDATE");
        let advance_frontier = format!(
            "UPDATE ONLY {frontier} SET next_event = $1 WHERE singleton = 1 AND next_event = $2"
        );
        let delete = format!("DELETE FROM ONLY {target} WHERE {id} = ANY($1::bigint[])");
        let target_empty = format!("SELECT NOT EXISTS(SELECT 1 FROM ONLY {target})");
        Self {
            table_name: spec.table().to_owned(),
            frontier_name,
            marker,
            initialize,
            insert_prefix,
            parameter_types,
            lookup_prefix,
            lookup_suffix,
            read_frontier,
            advance_frontier,
            delete,
            target_empty,
        }
    }

    pub(super) fn insert_batch_size(&self) -> usize {
        (usize::from(u16::MAX) / self.parameter_types.len()).min(MAX_MUTATIONS_PER_BATCH)
    }

    pub(super) fn lookup_batch_size(&self) -> usize {
        (usize::from(u16::MAX) / (self.parameter_types.len() + 1)).min(MAX_MUTATIONS_PER_BATCH)
    }

    pub(super) fn insert_statement(&self, rows: usize) -> String {
        assert!((1..=self.insert_batch_size()).contains(&rows));
        let mut sql = self.insert_prefix.clone();
        write_values(&mut sql, rows, &self.parameter_types, false);
        sql
    }

    pub(super) fn lookup_statement(&self, rows: usize) -> String {
        assert!((1..=self.lookup_batch_size()).contains(&rows));
        let mut sql = self.lookup_prefix.clone();
        let types = self.parameter_types.clone();
        write_values(&mut sql, rows, &types, true);
        sql.push_str(&self.lookup_suffix);
        sql
    }
}

fn write_values(sql: &mut String, rows: usize, types: &[&str], ordinal: bool) {
    for row in 0..rows {
        if row != 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        if ordinal {
            write!(sql, "{row}, ").expect("writing SQL cannot fail");
        }
        for (column, sql_type) in types.iter().enumerate() {
            if column != 0 {
                sql.push_str(", ");
            }
            let index = row * types.len() + column + 1;
            sql.push('$');
            write!(sql, "{index}::{sql_type}").expect("writing SQL cannot fail");
        }
        sql.push(')');
    }
}

async fn verify_identity(
    client: &Client,
    spec: &PostgresTargetSpec,
) -> Result<(), PostgresSinkError> {
    let row = client
        .query_one(
            "SELECT s.system_identifier::text, d.oid, \
         current_setting('fsync') = 'on', \
         current_setting('synchronous_commit') IN ('on', 'remote_write', 'remote_apply'), \
         current_setting('server_encoding') = 'UTF8' \
         FROM pg_catalog.pg_control_system() AS s \
         CROSS JOIN pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database()",
            &[],
        )
        .await
        .map_err(|error| database_error("verify target identity", &error))?;
    if row.get::<_, String>(0) != spec.system_identifier()
        || row.get::<_, u32>(1) != spec.database_oid()
    {
        return Err(PostgresSinkError::TargetIdentityChanged);
    }
    if !row.get::<_, bool>(2) || !row.get::<_, bool>(3) {
        return Err(PostgresSinkError::DurabilityDisabled);
    }
    if !row.get::<_, bool>(4) {
        return Err(PostgresSinkError::UnsupportedServerEncoding);
    }
    Ok(())
}

async fn verify_once(
    client: &impl GenericClient,
    spec: &PostgresTargetSpec,
    sql: &SqlPlan,
    verified: &mut bool,
) -> Result<(), PostgresSinkError> {
    if !*verified {
        require_owned_layout(client, spec, sql).await?;
        *verified = true;
    }
    Ok(())
}

async fn require_owned_layout(
    client: &impl GenericClient,
    spec: &PostgresTargetSpec,
    sql: &SqlPlan,
) -> Result<(), PostgresSinkError> {
    for name in [&sql.table_name, &sql.frontier_name] {
        let row = client.query_opt(
        "SELECT c.relkind::text, c.relpersistence::text, \
         c.relispartition, c.relrowsecurity, c.relforcerowsecurity, c.relhasrules, \
         pg_catalog.obj_description(c.oid, 'pg_class'), \
         EXISTS(SELECT 1 FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) \
         OR EXISTS(SELECT 1 FROM pg_catalog.pg_inherits AS i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) \
         FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2", &[&spec.schema(), &name])
        .await
        .map_err(|error| database_error("inspect target relation", &error))?
        .ok_or_else(|| PostgresSinkError::TargetMissing { name: name.clone() })?;
        let valid = row.get::<_, String>(0) == "r"
            && row.get::<_, String>(1) == "p"
            && !row.get::<_, bool>(2)
            && !row.get::<_, bool>(3)
            && !row.get::<_, bool>(4)
            && !row.get::<_, bool>(5)
            && row.get::<_, Option<String>>(6).as_deref() == Some(sql.marker.as_str())
            && !row.get::<_, bool>(7);
        if !valid {
            return Err(PostgresSinkError::TargetLayoutMismatch { name: name.clone() });
        }
    }
    let names = vec![
        spec.hash_index(),
        format!("$dogpaddle.pk.{}", spec.sink_id()),
        spec.frontier_pk(),
    ];
    let row = client.query_one(
        "SELECT COUNT(*)::bigint, COALESCE(bool_and(c.relkind = 'i' AND c.relpersistence = 'p' \
         AND i.indisvalid AND pg_catalog.obj_description(c.oid, 'pg_class') IS NOT DISTINCT FROM $3), false) \
         FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         LEFT JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.oid \
         WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[])", &[&spec.schema(), &names, &sql.marker])
        .await
        .map_err(|error| database_error("inspect target indexes", &error))?;
    if usize::try_from(row.get::<_, i64>(0)).ok() != Some(names.len()) || !row.get::<_, bool>(1) {
        return Err(PostgresSinkError::TargetLayoutMismatch {
            name: sql.table_name.clone(),
        });
    }
    require_frontier_layout(client, spec, sql).await?;
    Ok(())
}

async fn require_frontier_layout(
    client: &impl GenericClient,
    spec: &PostgresTargetSpec,
    sql: &SqlPlan,
) -> Result<(), PostgresSinkError> {
    let relation = qualified(spec.schema(), &sql.frontier_name);
    let columns = client.query("SELECT attname::text, atttypid, attnotnull, attisdropped, atthasdef, attidentity::text, attgenerated::text FROM pg_catalog.pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0 ORDER BY attnum", &[&relation]).await
        .map_err(|error| database_error("inspect frontier columns", &error))?;
    let valid_columns = columns.len() == 2
        && columns
            .iter()
            .zip([("singleton", 21_u32), ("next_event", 20_u32)])
            .all(|(column, (name, oid))| {
                column.get::<_, String>(0) == name
                    && column.get::<_, u32>(1) == oid
                    && column.get::<_, bool>(2)
                    && !column.get::<_, bool>(3)
                    && !column.get::<_, bool>(4)
                    && column.get::<_, String>(5).is_empty()
                    && column.get::<_, String>(6).is_empty()
            });
    let constraints = client.query("SELECT conname::text, contype::text, conkey, convalidated, condeferrable, condeferred, pg_catalog.pg_get_constraintdef(oid, true) FROM pg_catalog.pg_constraint WHERE conrelid = $1::text::regclass ORDER BY conname COLLATE \"C\"", &[&relation]).await
        .map_err(|error| database_error("inspect frontier constraints", &error))?;
    let mut expected = vec![
        (
            spec.frontier_pk(),
            "p",
            vec![1_i16],
            "PRIMARY KEY (singleton)".to_owned(),
        ),
        (
            format!("$dogpaddle.frontier_slot.{}", spec.sink_id()),
            "c",
            vec![1_i16],
            "CHECK (singleton = 1)".to_owned(),
        ),
        (
            format!("$dogpaddle.frontier_next.{}", spec.sink_id()),
            "c",
            vec![2_i16],
            format!("CHECK (next_event > '{}'::bigint)", i64::MIN),
        ),
    ];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    let valid_constraints = constraints.len() == expected.len()
        && constraints
            .iter()
            .zip(&expected)
            .all(|(actual, (name, kind, key, definition))| {
                actual.get::<_, String>(0) == *name
                    && actual.get::<_, String>(1) == *kind
                    && actual.get::<_, Vec<i16>>(2) == *key
                    && actual.get::<_, bool>(3)
                    && !actual.get::<_, bool>(4)
                    && !actual.get::<_, bool>(5)
                    && actual.get::<_, String>(6) == *definition
            });
    let index = client
        .query_one(
            "SELECT COUNT(*)::bigint, COALESCE(bool_and(c.relname = $2 AND a.amname = 'btree' \
         AND i.indisprimary AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive \
         AND i.indnatts = 1 AND i.indnkeyatts = 1 AND i.indkey[0] = 1 \
         AND i.indpred IS NULL AND i.indexprs IS NULL), false) \
         FROM pg_catalog.pg_index AS i JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid \
         JOIN pg_catalog.pg_am AS a ON a.oid = c.relam WHERE i.indrelid = $1::text::regclass",
            &[&relation, &spec.frontier_pk()],
        )
        .await
        .map_err(|error| database_error("inspect frontier primary key", &error))?;
    if !valid_columns
        || !valid_constraints
        || index.get::<_, i64>(0) != 1
        || !index.get::<_, bool>(1)
    {
        return Err(PostgresSinkError::TargetLayoutMismatch {
            name: sql.frontier_name.clone(),
        });
    }
    Ok(())
}

async fn object_count(
    client: &impl GenericClient,
    spec: &PostgresTargetSpec,
) -> Result<usize, PostgresSinkError> {
    let names = spec.object_names().to_vec();
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[])",
            &[&spec.schema(), &names],
        )
        .await
        .map_err(|error| database_error("inspect target objects", &error))?
        .get(0);
    usize::try_from(count).map_err(|_| invalid_batch("invalid target object count"))
}

pub(super) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified(schema: &str, object: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(object))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
