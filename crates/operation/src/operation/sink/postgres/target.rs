use std::{fmt::Write as _, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, GenericClient, IsolationLevel, types::ToSql};

use crate::operation::{
    OperationError,
    sink::relation::{Batch, Insert, Lookup, MAX_MUTATIONS_PER_BATCH, Matches, RelationTarget},
};

use super::{
    config::{PgClient, PostgresSinkConfig, PostgresTargetSpec, bounded, require_absent},
    error::{PostgresSinkError, database_error, invalid_batch},
    row::{EncodedRow, HASH_LENGTH, PostgresRowCodec, PostgresValue},
    schema::{PostgresLayout, StorageType, TECHNICAL_HASH, TECHNICAL_ID},
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
        input_schema: SchemaRef,
    ) -> Self {
        let layout = PostgresLayout::try_new(input_schema)
            .expect("the sealed Definition binding validated this exact Schema");
        let sql = SqlPlan::new(&spec, &layout);
        Self {
            config,
            spec,
            row_codec: PostgresRowCodec::new(layout),
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
        ) -> Result<R, PostgresSinkError>,
    ) -> Result<R, PostgresSinkError> {
        let Self {
            config,
            spec,
            row_codec,
            sql,
            client: session,
            layout_verified,
        } = self;
        if config.database() != spec.database() {
            return Err(PostgresSinkError::DatabaseMismatch);
        }
        if session.is_none() {
            let mut opened = config.connect()?;
            let PgClient {
                runtime,
                client: opened_client,
            } = &mut opened;
            bounded(runtime, "verify target identity", async {
                verify_identity(opened_client, spec).await
            })?;
            *session = Some(opened);
            *layout_verified = false;
        }
        let PgClient { runtime, client } =
            session.as_mut().expect("the client was initialized above");
        let result = action(runtime, client, spec, sql, row_codec, layout_verified);
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
        self.with_client(|runtime, client, spec, _, _, _| {
            bounded(runtime, "check target absence", async {
                require_absent(client, spec).await
            })
        })
        .map_err(Into::into)
    }

    fn initialize(&mut self) -> Result<(), OperationError> {
        self.with_client(|runtime, client, spec, sql, _, layout_verified| {
            bounded(runtime, "initialize target", async {
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
                transaction
                    .commit()
                    .await
                    .map_err(|error| database_error("commit initialization", &error))?;
                *layout_verified = true;
                Ok(())
            })
        })
        .map_err(Into::into)
    }

    fn lookup(
        &mut self,
        input: &Change,
        requests: &[Lookup],
    ) -> Result<Vec<Matches>, OperationError> {
        self.with_client(|runtime, client, spec, sql, codec, layout_verified| {
            bounded(runtime, "match target rows", async {
                verify_once(client, spec, sql, layout_verified).await?;
                let mut matches = Vec::with_capacity(requests.len());
                for batch in requests.chunks(sql.lookup_batch_size()) {
                    let encoded = batch
                        .iter()
                        .map(|request| codec.encode_row(input.records(), request.row_index))
                        .collect::<Result<Vec<_>, _>>()?;
                    let limits = batch
                        .iter()
                        .map(|request| {
                            let needed = request.needed.min(i64::MAX.unsigned_abs());
                            Ok((
                                i64::try_from(needed).expect("needed was capped at bigint"),
                                i64::try_from(request.take)
                                    .map_err(|_| invalid_batch("lookup take exceeds bigint"))?,
                            ))
                        })
                        .collect::<Result<Vec<_>, PostgresSinkError>>()?;
                    let hashes = encoded
                        .iter()
                        .map(|row| row.hash.as_slice())
                        .collect::<Vec<_>>();
                    let mut parameters: Vec<&(dyn ToSql + Sync)> = Vec::new();
                    for ((row, hash), (needed, take)) in encoded.iter().zip(&hashes).zip(&limits) {
                        parameters.extend([needed as &(dyn ToSql + Sync), take, hash]);
                        parameters.extend(row.values.iter().map(PostgresValue::as_parameter));
                    }
                    let rows = client
                        .query(&sql.lookup_statement(batch.len()), &parameters)
                        .await
                        .map_err(|error| database_error("match target rows", &error))?;
                    for row in rows {
                        let count = u64::try_from(row.get::<_, i64>(1))
                            .map_err(|_| invalid_batch("negative matching-row count"))?;
                        let ids =
                            row.get::<_, Vec<i64>>(2)
                                .into_iter()
                                .map(|id| {
                                    u64::try_from(id).ok().filter(|id| *id != 0).ok_or_else(|| {
                                        invalid_batch("nonpositive target technical ID")
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                        matches.push(Matches { count, ids });
                    }
                }
                Ok(matches)
            })
        })
        .map_err(Into::into)
    }

    fn write_batch(&mut self, input: &Change, batch: &Batch) -> Result<(), OperationError> {
        self.with_client(|runtime, client, spec, sql, codec, layout_verified| {
            let inserts = encode_inserts(codec, input, &batch.inserts)?;
            let deletes = batch
                .deletes
                .iter()
                .map(|id| positive_i64(*id))
                .collect::<Result<Vec<_>, _>>()?;
            bounded(runtime, "write target batch", async {
                let transaction = client
                    .transaction()
                    .await
                    .map_err(|error| database_error("begin batch", &error))?;
                verify_once(&transaction, spec, sql, layout_verified).await?;
                // Planning validated every input prefix. The owned target has no
                // user triggers: grouping is atomic, and replay repeats insert then
                // delete even when an ID occurs in both lists.
                for inserts in inserts.chunks(sql.insert_batch_size()) {
                    insert_batch(&transaction, sql, inserts).await?;
                }
                if !deletes.is_empty() {
                    transaction
                        .execute(&sql.delete, &[&deletes])
                        .await
                        .map_err(|error| database_error("delete target rows", &error))?;
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| database_error("commit batch", &error))?;
                Ok(())
            })
        })
        .map_err(Into::into)
    }
}

struct EncodedInsert {
    technical_id: i64,
    row: Arc<EncodedRow>,
}

fn encode_inserts(
    codec: &PostgresRowCodec,
    input: &Change,
    inserts: &[Insert],
) -> Result<Vec<EncodedInsert>, PostgresSinkError> {
    let mut encoded = Vec::with_capacity(inserts.len());
    let mut cached = None::<(u64, Arc<EncodedRow>)>;
    for insert in inserts {
        let row = match &cached {
            Some((index, row)) if *index == insert.row_index => Arc::clone(row),
            _ => {
                let index = usize::try_from(insert.row_index)
                    .map_err(|_| invalid_batch("insert row index exceeds usize"))?;
                let row = Arc::new(codec.encode_row(input.records(), index)?);
                cached = Some((insert.row_index, Arc::clone(&row)));
                row
            }
        };
        encoded.push(EncodedInsert {
            technical_id: positive_i64(insert.technical_id)?,
            row,
        });
    }
    Ok(encoded)
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

fn positive_i64(id: u64) -> Result<i64, PostgresSinkError> {
    i64::try_from(id)
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| invalid_batch("technical ID must fit positive PostgreSQL bigint"))
}

pub(super) struct SqlPlan {
    table_name: String,
    marker: String,
    pub(super) initialize: String,
    insert_prefix: String,
    insert_suffix: String,
    parameter_types: Vec<&'static str>,
    lookup_prefix: String,
    lookup_suffix: String,
    pub(super) delete: String,
    target_empty: String,
}

impl SqlPlan {
    #[allow(clippy::too_many_lines)]
    pub(super) fn new(spec: &PostgresTargetSpec, layout: &PostgresLayout) -> Self {
        let target = qualified(spec.schema(), spec.table());
        let hash_index_name = spec.hash_index();
        let hash_index = qualified(spec.schema(), &hash_index_name);
        let target_pk = format!("$dogpaddle.pk.{}", spec.sink_id());
        let id_check = format!("$dogpaddle.id_check.{}", spec.sink_id());
        let hash_check = format!("$dogpaddle.hash_check.{}", spec.sink_id());
        let id = quote_identifier(TECHNICAL_ID);
        let hash = quote_identifier(TECHNICAL_HASH);
        let mut definitions = vec![
            format!("{id} bigint NOT NULL"),
            format!("{hash} bytea NOT NULL"),
        ];
        for (index, column) in layout.columns().iter().enumerate() {
            let name = quote_identifier(column.name());
            let mut definition = format!("{name} {}", column.storage().sql());
            if !column.nullable() {
                definition.push_str(" NOT NULL");
            }
            let condition = column
                .check()
                .map(|check| format!("{name} {check}"))
                .or_else(|| {
                    if let StorageType::Bytes(Some(length)) = column.storage() {
                        Some(format!("octet_length({name}) = {length}"))
                    } else {
                        None
                    }
                });
            if let Some(condition) = condition {
                let constraint = format!("$dogpaddle.c.{index:04x}.{}", spec.sink_id());
                let checked = if column.nullable() && column.check() != Some("IS NULL") {
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
                "CONSTRAINT {} CHECK ({id} > 0)",
                quote_identifier(&id_check)
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
        let marker_hash = blake3::hash(format!("{create_target}\0{create_hash_index}").as_bytes());
        let marker = format!("dogpaddle.postgres-relation.v2:{}", marker_hash.to_hex());
        let marker_literal = quote_literal(&marker);
        let initialize = format!(
            "{create_target}; {create_hash_index}; \
             COMMENT ON TABLE {target} IS {marker_literal}; \
             COMMENT ON INDEX {} IS {marker_literal}; \
             COMMENT ON INDEX {hash_index} IS {marker_literal}",
            qualified(spec.schema(), &target_pk)
        );
        let logical_names = layout
            .columns()
            .iter()
            .map(|column| quote_identifier(column.name()))
            .collect::<Vec<_>>();
        let mut all_names = vec![id.clone(), hash.clone()];
        all_names.extend(logical_names.iter().cloned());
        let insert_prefix = format!("INSERT INTO {target} ({}) VALUES ", all_names.join(", "));
        let insert_suffix = format!(" ON CONFLICT ({id}) DO NOTHING");
        let mut parameter_types = vec!["bigint", "bytea"];
        parameter_types.extend(layout.columns().iter().map(|column| column.storage().sql()));

        let mut request_names = vec![
            "n".to_owned(),
            "needed".to_owned(),
            "take".to_owned(),
            "hash".to_owned(),
        ];
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
        let lookup_prefix = format!(
            "SELECT request.n, CASE WHEN cardinality(selected.ids) = request.take \
             AND request.needed > request.take THEN \
             (SELECT count(*) FROM (SELECT 1 {matching} LIMIT request.needed) AS counted) \
             ELSE cardinality(selected.ids)::bigint END, selected.ids FROM (VALUES "
        );
        let delete = format!("DELETE FROM ONLY {target} WHERE {id} = ANY($1::bigint[])");
        let target_empty = format!("SELECT NOT EXISTS(SELECT 1 FROM ONLY {target})");
        Self {
            table_name: spec.table().to_owned(),
            marker,
            initialize,
            insert_prefix,
            insert_suffix,
            parameter_types,
            lookup_prefix,
            lookup_suffix,
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
        sql.push_str(&self.insert_suffix);
        sql
    }

    pub(super) fn lookup_statement(&self, rows: usize) -> String {
        assert!((1..=self.lookup_batch_size()).contains(&rows));
        let mut sql = self.lookup_prefix.clone();
        let mut types = vec!["bigint"];
        types.extend(&self.parameter_types);
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
    let row = client.query_opt(
        "SELECT c.relkind::text, c.relpersistence::text, \
         c.relispartition, c.relrowsecurity, c.relforcerowsecurity, c.relhasrules, \
         pg_catalog.obj_description(c.oid, 'pg_class'), \
         EXISTS(SELECT 1 FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) \
         OR EXISTS(SELECT 1 FROM pg_catalog.pg_inherits AS i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) \
         FROM pg_catalog.pg_class AS c \
         JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2", &[&spec.schema(), &sql.table_name])
        .await
        .map_err(|error| database_error("inspect target relation", &error))?
        .ok_or_else(|| PostgresSinkError::TargetMissing { name: sql.table_name.clone() })?;
    let valid = row.get::<_, String>(0) == "r"
        && row.get::<_, String>(1) == "p"
        && !row.get::<_, bool>(2)
        && !row.get::<_, bool>(3)
        && !row.get::<_, bool>(4)
        && !row.get::<_, bool>(5)
        && row.get::<_, Option<String>>(6).as_deref() == Some(sql.marker.as_str())
        && !row.get::<_, bool>(7);
    if !valid {
        return Err(PostgresSinkError::TargetLayoutMismatch {
            name: sql.table_name.clone(),
        });
    }
    let names = spec.object_names().into_iter().skip(1).collect::<Vec<_>>();
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
