use std::{collections::HashSet, fmt::Write as _, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use postgres::{Client, GenericClient, IsolationLevel, types::ToSql};

use crate::operation::sink::relation::{MAX_MUTATIONS_PER_BATCH, Mutation, MutationKind};

use super::{
    config::{PostgresSinkConfig, PostgresTargetSpec},
    error::{PostgresSinkError, database_error, invalid_batch},
    row::{EncodedRow, HASH_LENGTH, PostgresRowCodec, RowError},
    schema::{PostgresLayout, StorageType, TECHNICAL_HASH, TECHNICAL_ID},
};

const DIGEST_LENGTH: usize = 32;

const DELIVERY_COLUMN: &str = "$dogpaddle.delivery";
const DIGEST_COLUMN: &str = "$dogpaddle.digest";
const MUTATION_COUNT_COLUMN: &str = "$dogpaddle.mutations";

/// Identity of one durably prepared external delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Delivery {
    /// Positive, monotonically allocated sequence within one sink instance.
    pub(crate) sequence: u64,
    /// Technical-ID frontier before this delivery reserved its inserts.
    pub(crate) next_id_before: u64,
    /// Digest of the exact ordered mutation payload.
    pub(crate) digest: [u8; DIGEST_LENGTH],
}

/// Result of an idempotent `PostgreSQL` delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitOutcome {
    /// The receipt and mutations were committed by this call.
    Applied,
    /// The same immutable receipt was already committed.
    AlreadyApplied,
}

/// Result of creating or replaying sink target initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InitializeOutcome {
    /// This call created the empty target layout.
    Created,
    /// The sink-owned empty target layout already existed.
    AlreadyCreated,
}

/// Exact matching target IDs selected in ascending order.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MatchingIds {
    /// Number of available matches, capped at the requested scan limit.
    pub(crate) count: u64,
    /// Earliest matches, capped at the requested selection limit.
    pub(crate) selected: Vec<u64>,
}

/// Lazily connected, exact-Schema-bound `PostgreSQL` relation target.
pub(crate) struct PostgresTarget {
    config: PostgresSinkConfig,
    spec: PostgresTargetSpec,
    row_codec: PostgresRowCodec,
    sql: SqlPlan,
    client: Option<Client>,
    layout_verified: bool,
}

impl PostgresTarget {
    /// Purely binds runtime configuration and a persistent target spec to an
    /// exact Arrow Schema. No network I/O occurs here.
    pub(crate) fn new_bound(
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

    pub(crate) const fn schema(&self) -> &SchemaRef {
        self.row_codec.schema()
    }

    /// Encodes one retained Change row for deterministic relation planning.
    pub(crate) fn encode_row(
        &self,
        change: &Change,
        row_index: usize,
    ) -> Result<EncodedRow, PostgresSinkError> {
        self.row_codec
            .encode_row(change.records(), row_index)
            .map_err(PostgresSinkError::from)
    }

    /// Creates all sink-owned objects in one target transaction. Repeating the
    /// call after an uncertain successful commit accepts only the owned empty
    /// layout created by this sink.
    pub(crate) fn initialize(&mut self) -> Result<InitializeOutcome, PostgresSinkError> {
        self.with_client(|client, spec, sql, _, layout_verified| {
            let mut transaction = client
                .build_transaction()
                .isolation_level(IsolationLevel::Serializable)
                .start()
                .map_err(|error| database_error("begin initialization", &error))?;
            let existing = object_count(&mut transaction, spec)?;
            let outcome = if existing == 0 {
                transaction
                    .batch_execute(&sql.initialize)
                    .map_err(|error| database_error("create target layout", &error))?;
                InitializeOutcome::Created
            } else if existing == spec.object_names().len() {
                InitializeOutcome::AlreadyCreated
            } else {
                return Err(PostgresSinkError::TargetLayoutMismatch {
                    name: spec.table().to_owned(),
                });
            };
            require_owned_layout(&mut transaction, spec, sql)?;
            if !target_is_empty(&mut transaction, sql)? {
                return Err(PostgresSinkError::TargetNotEmpty);
            }
            transaction
                .commit()
                .map_err(|error| database_error("commit initialization", &error))?;
            *layout_verified = true;
            Ok(outcome)
        })
    }

    /// Verifies the owned layout and the two durable allocation frontiers.
    /// This is intended for materialization/reopen checks.
    pub(crate) fn verify_ready(
        &mut self,
        next_id: u64,
        next_delivery: u64,
    ) -> Result<(), PostgresSinkError> {
        if next_id == 0 || next_delivery == 0 {
            return Err(invalid_batch(
                "durable allocation frontiers must be positive",
            ));
        }
        self.with_client(|client, spec, sql, _, layout_verified| {
            require_owned_layout(client, spec, sql)?;
            let bounds = client
                .query_one(&sql.frontiers, &[])
                .map_err(|error| database_error("read target frontiers", &error))?;
            let maximum_id: Option<i64> = bounds.get(0);
            let maximum_delivery: Option<i64> = bounds.get(1);
            if let Some(id) = maximum_id {
                let id =
                    u64::try_from(id).map_err(|_| PostgresSinkError::TargetLayoutMismatch {
                        name: spec.table().to_owned(),
                    })?;
                if id >= next_id {
                    return Err(PostgresSinkError::TechnicalIdFrontierMismatch { id, next_id });
                }
            }
            let delivery = maximum_delivery
                .map(|delivery| {
                    u64::try_from(delivery).map_err(|_| PostgresSinkError::TargetLayoutMismatch {
                        name: spec.receipt_table(),
                    })
                })
                .transpose()?
                .unwrap_or(0);
            if delivery != next_delivery - 1 {
                return Err(PostgresSinkError::DeliveryFrontierMismatch {
                    delivery,
                    next_delivery,
                });
            }
            *layout_verified = true;
            Ok(())
        })
    }

    /// Returns exact, non-excluded physical row identities in ascending order.
    pub(crate) fn matching_ids(
        &mut self,
        encoded: &EncodedRow,
        excluded: &HashSet<u64>,
        scan_limit: u64,
        select_limit: usize,
    ) -> Result<MatchingIds, PostgresSinkError> {
        if select_limit > MAX_MUTATIONS_PER_BATCH
            || select_limit > usize::try_from(scan_limit).unwrap_or(usize::MAX)
        {
            return Err(invalid_batch("selection limit exceeds scan limit"));
        }
        let scan_limit = i64::try_from(scan_limit)
            .map_err(|_| invalid_batch("matching-row scan limit exceeds PostgreSQL bigint"))?;
        let selection_limit = i64::try_from(select_limit).expect("the batch limit fits i64");
        let excluded = excluded
            .iter()
            .map(|id| positive_i64(*id, "excluded technical ID"))
            .collect::<Result<Vec<_>, _>>()?;
        self.with_client(|client, spec, sql, _, layout_verified| {
            if !*layout_verified {
                require_owned_layout(client, spec, sql)?;
                *layout_verified = true;
            }
            let hash = encoded.hash.to_vec();
            let mut parameters: Vec<&(dyn ToSql + Sync)> =
                Vec::with_capacity(encoded.values.len() + 3);
            parameters.push(&hash);
            parameters.extend(
                encoded
                    .values
                    .iter()
                    .map(super::row::PostgresValue::as_parameter),
            );
            parameters.push(&excluded);
            parameters.push(&selection_limit);
            let rows = client
                .query(&sql.select_matching, &parameters)
                .map_err(|error| database_error("select exact target rows", &error))?;
            let mut selected = Vec::with_capacity(select_limit);
            for row in rows {
                let raw_id: i64 = row.get(0);
                let id =
                    u64::try_from(raw_id).map_err(|_| PostgresSinkError::TargetLayoutMismatch {
                        name: sql.table_name.clone(),
                    })?;
                if id == 0 {
                    return Err(invalid_batch("target technical ID must be positive"));
                }
                selected.push(id);
            }
            // A large negative event must be admitted in full before deleting
            // any of it. Count on the server; never transfer its entire ID set.
            let count = if selected.len() == select_limit && scan_limit > selection_limit {
                *parameters
                    .last_mut()
                    .expect("the limit parameter is present") = &scan_limit;
                let count: i64 = client
                    .query_one(&sql.count_matching, &parameters)
                    .map_err(|error| database_error("count exact target rows", &error))?
                    .get(0);
                u64::try_from(count).map_err(|_| invalid_batch("negative matching-row count"))?
            } else {
                u64::try_from(selected.len()).expect("the batch limit fits u64")
            };
            Ok(MatchingIds { count, selected })
        })
    }

    /// Encodes and digests an exact ordered mutation batch before it is stored
    /// as a durable committable.
    pub(crate) fn digest_batch(
        &self,
        sequence: u64,
        next_id_before: u64,
        change: &Change,
        mutations: &[Mutation],
    ) -> Result<[u8; DIGEST_LENGTH], PostgresSinkError> {
        let _ = positive_i64(sequence, "delivery sequence")?;
        validate_next_id(next_id_before)?;
        let encoded = encode_mutations(&self.row_codec, change, mutations)?;
        Ok(batch_digest(&self.spec, sequence, next_id_before, &encoded))
    }

    /// Commits a receipt and every mutation in one `PostgreSQL` transaction.
    /// A replay of the same delivery verifies its digest and mutation count and
    /// performs no target mutations.
    pub(crate) fn commit_batch(
        &mut self,
        delivery: Delivery,
        change: &Change,
        mutations: &[Mutation],
    ) -> Result<CommitOutcome, PostgresSinkError> {
        let sequence = positive_i64(delivery.sequence, "delivery sequence")?;
        validate_next_id(delivery.next_id_before)?;
        let encoded = encode_mutations(&self.row_codec, change, mutations)?;
        if batch_digest(
            &self.spec,
            delivery.sequence,
            delivery.next_id_before,
            &encoded,
        ) != delivery.digest
        {
            return Err(invalid_batch(
                "delivery digest does not match its mutations",
            ));
        }
        let mutation_count = i32::try_from(encoded.len())
            .map_err(|_| invalid_batch("mutation count exceeds PostgreSQL integer"))?;

        self.with_client(|client, spec, sql, _, layout_verified| {
            let mut transaction = client
                .transaction()
                .map_err(|error| database_error("begin delivery", &error))?;
            if !*layout_verified {
                require_owned_layout(&mut transaction, spec, sql)?;
            }
            let predecessor: Option<i64> = transaction
                .query_one(&sql.receipt_frontier, &[])
                .map_err(|error| database_error("read delivery frontier", &error))?
                .get(0);
            let predecessor = predecessor
                .map(|value| {
                    u64::try_from(value).map_err(|_| PostgresSinkError::TargetLayoutMismatch {
                        name: sql.receipt_name.clone(),
                    })
                })
                .transpose()?
                .unwrap_or(0);
            if predecessor != delivery.sequence - 1 && predecessor != delivery.sequence {
                return Err(PostgresSinkError::DeliveryFrontierMismatch {
                    delivery: predecessor,
                    next_delivery: delivery.sequence,
                });
            }
            let digest = delivery.digest.to_vec();
            let inserted = transaction
                .execute(&sql.insert_receipt, &[&sequence, &digest, &mutation_count])
                .map_err(|error| database_error("insert delivery receipt", &error))?;
            if inserted == 0 {
                let existing = transaction
                    .query_one(&sql.select_receipt, &[&sequence])
                    .map_err(|error| database_error("read delivery receipt", &error))?;
                let existing_digest: Vec<u8> = existing.get(0);
                let existing_count: i32 = existing.get(1);
                if existing_digest != digest || existing_count != mutation_count {
                    return Err(PostgresSinkError::DeliveryConflict {
                        delivery: delivery.sequence,
                    });
                }
                transaction
                    .commit()
                    .map_err(|error| database_error("commit replay verification", &error))?;
                *layout_verified = true;
                return Ok(CommitOutcome::AlreadyApplied);
            }
            if inserted != 1 {
                return Err(invalid_batch(
                    "receipt insert changed an unexpected row count",
                ));
            }

            // Group only adjacent mutations of the same kind. In particular,
            // an insert followed by its withdrawal must remain in that order.
            for run in encoded.chunk_by(|left, right| left.kind == right.kind) {
                for batch in run.chunks(sql.mutation_batch_size()) {
                    apply_mutations(&mut transaction, sql, batch)?;
                }
            }
            // Preparing this delivery required settling its predecessor in
            // MDBX. Only this receipt can ever be replayed; retire predecessors
            // atomically with the new receipt and mutations, not in a GC phase.
            transaction
                .execute(&sql.retire_receipts, &[&sequence])
                .map_err(|error| database_error("retire settled receipts", &error))?;
            transaction
                .commit()
                .map_err(|error| database_error("commit delivery", &error))?;
            *layout_verified = true;
            Ok(CommitOutcome::Applied)
        })
    }

    fn with_client<R>(
        &mut self,
        action: impl FnOnce(
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
            client,
            layout_verified,
        } = self;
        if config.database() != spec.database() {
            return Err(PostgresSinkError::DatabaseMismatch);
        }
        if client.is_none() {
            let mut opened = config.connect()?;
            verify_identity(&mut opened, spec)?;
            *client = Some(opened);
            *layout_verified = false;
        }
        let result = action(
            client
                .as_mut()
                .expect("the PostgreSQL client was initialized above"),
            spec,
            sql,
            row_codec,
            layout_verified,
        );
        if result.is_err() {
            *client = None;
            *layout_verified = false;
        }
        result
    }
}

struct EncodedMutation {
    kind: MutationKind,
    technical_id: i64,
    row_index: u64,
    row: Arc<EncodedRow>,
}

fn encode_mutations(
    codec: &PostgresRowCodec,
    change: &Change,
    mutations: &[Mutation],
) -> Result<Vec<EncodedMutation>, PostgresSinkError> {
    if change.schema().as_ref() != codec.schema().as_ref() {
        return Err(PostgresSinkError::InputSchemaMismatch {
            expected: Arc::clone(codec.schema()),
            actual: change.schema(),
        });
    }
    if mutations.is_empty() || mutations.len() > MAX_MUTATIONS_PER_BATCH {
        return Err(invalid_batch(format!(
            "mutation count must be within 1..={MAX_MUTATIONS_PER_BATCH}"
        )));
    }

    let mut encoded = Vec::with_capacity(mutations.len());
    let mut previous_row = None;
    let mut cached_row = None::<Arc<EncodedRow>>;
    for mutation in mutations {
        if previous_row.is_some_and(|row| mutation.row_index < row) {
            return Err(invalid_batch("mutation row indices must be nondecreasing"));
        }
        let row_index = usize::try_from(mutation.row_index)
            .map_err(|_| invalid_batch("mutation row index cannot be represented by usize"))?;
        let row = if previous_row == Some(mutation.row_index) {
            Arc::clone(
                cached_row
                    .as_ref()
                    .expect("a repeated mutation row has a cached encoding"),
            )
        } else {
            let row = Arc::new(codec.encode_row(change.records(), row_index)?);
            cached_row = Some(Arc::clone(&row));
            row
        };
        encoded.push(EncodedMutation {
            kind: mutation.kind,
            technical_id: positive_i64(mutation.technical_id, "technical ID")?,
            row_index: mutation.row_index,
            row,
        });
        previous_row = Some(mutation.row_index);
    }
    Ok(encoded)
}

fn batch_digest(
    spec: &PostgresTargetSpec,
    sequence: u64,
    next_id_before: u64,
    mutations: &[EncodedMutation],
) -> [u8; DIGEST_LENGTH] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dogpaddle.postgres-delivery.v1\0");
    hash_bytes(&mut hasher, spec.system_identifier().as_bytes());
    hasher.update(&spec.database_oid().to_be_bytes());
    hash_bytes(&mut hasher, spec.database().as_bytes());
    hash_bytes(&mut hasher, spec.schema().as_bytes());
    hash_bytes(&mut hasher, spec.table().as_bytes());
    hash_bytes(&mut hasher, spec.sink_id().as_bytes());
    hasher.update(&sequence.to_be_bytes());
    hasher.update(&next_id_before.to_be_bytes());
    hasher.update(
        &u64::try_from(mutations.len())
            .expect("the bounded mutation count fits u64")
            .to_be_bytes(),
    );
    for mutation in mutations {
        hasher.update(&[match mutation.kind {
            MutationKind::Insert => 0,
            MutationKind::Delete => 1,
        }]);
        hasher.update(&mutation.row_index.to_be_bytes());
        hasher.update(&mutation.technical_id.to_be_bytes());
        hasher.update(
            &u64::try_from(mutation.row.canonical.len())
                .expect("an addressable row length fits u64")
                .to_be_bytes(),
        );
        hasher.update(&mutation.row.canonical);
    }
    *hasher.finalize().as_bytes()
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(
        &u64::try_from(bytes.len())
            .expect("an addressable identity length fits u64")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}

fn apply_mutations(
    transaction: &mut postgres::Transaction<'_>,
    sql: &SqlPlan,
    mutations: &[EncodedMutation],
) -> Result<(), PostgresSinkError> {
    let kind = mutations[0].kind;
    let statement = sql.mutation_statement(kind, mutations.len());
    let hashes = mutations
        .iter()
        .map(|mutation| mutation.row.hash.as_slice())
        .collect::<Vec<_>>();
    let mut parameters: Vec<&(dyn ToSql + Sync)> =
        Vec::with_capacity(mutations.len() * sql.parameter_types.len());
    for (mutation, hash) in mutations.iter().zip(&hashes) {
        parameters.push(&mutation.technical_id);
        parameters.push(hash);
        parameters.extend(
            mutation
                .row
                .values
                .iter()
                .map(super::row::PostgresValue::as_parameter),
        );
    }
    let changed = transaction
        .query(&statement, &parameters)
        .map_err(|error| database_error("apply target mutations", &error))?
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<HashSet<_>>();
    for mutation in mutations {
        if !changed.contains(&mutation.technical_id) {
            let id = u64::try_from(mutation.technical_id).expect("encoded IDs are positive");
            return Err(match kind {
                MutationKind::Insert => PostgresSinkError::TechnicalIdConflict { id },
                MutationKind::Delete => PostgresSinkError::DeleteRowMismatch { id },
            });
        }
    }
    Ok(())
}

fn positive_i64(value: u64, label: &'static str) -> Result<i64, PostgresSinkError> {
    i64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_batch(format!("{label} must fit positive PostgreSQL bigint")))
}

fn validate_next_id(next_id: u64) -> Result<(), PostgresSinkError> {
    let exhausted = i64::MAX.unsigned_abs() + 1;
    if (1..=exhausted).contains(&next_id) {
        Ok(())
    } else {
        Err(invalid_batch(
            "next technical ID must be within 1..=i64::MAX+1",
        ))
    }
}

fn verify_identity(
    client: &mut Client,
    spec: &PostgresTargetSpec,
) -> Result<(), PostgresSinkError> {
    let row = client
        .query_one(
            "SELECT s.system_identifier::text, d.oid, \
                    current_setting('fsync') = 'on', \
                    current_setting('synchronous_commit') IN ('on', 'remote_write', 'remote_apply'), \
                    current_setting('server_encoding') = 'UTF8' \
             FROM pg_catalog.pg_control_system() AS s \
             CROSS JOIN pg_catalog.pg_database AS d \
             WHERE d.datname = pg_catalog.current_database()",
            &[],
        )
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

pub(super) struct SqlPlan {
    table_name: String,
    receipt_name: String,
    marker: String,
    pub(super) initialize: String,
    pub(super) select_matching: String,
    pub(super) count_matching: String,
    insert_prefix: String,
    insert_suffix: String,
    delete_prefix: String,
    delete_suffix: String,
    parameter_types: Vec<&'static str>,
    insert_receipt: String,
    retire_receipts: String,
    select_receipt: String,
    receipt_frontier: String,
    frontiers: String,
    target_empty: String,
}

impl SqlPlan {
    #[allow(clippy::too_many_lines)]
    pub(super) fn new(spec: &PostgresTargetSpec, layout: &PostgresLayout) -> Self {
        let target = qualified(spec.schema(), spec.table());
        let receipt_name = spec.receipt_table();
        let receipt = qualified(spec.schema(), &receipt_name);
        let hash_index_name = spec.hash_index();
        let hash_index = qualified(spec.schema(), &hash_index_name);
        let target_pk = format!("$dogpaddle.pk.{}", spec.sink_id());
        let receipt_pk = format!("$dogpaddle.receipt_pk.{}", spec.sink_id());
        let id_check = format!("$dogpaddle.id_check.{}", spec.sink_id());
        let hash_check = format!("$dogpaddle.hash_check.{}", spec.sink_id());
        let delivery_check = format!("$dogpaddle.delivery_check.{}", spec.sink_id());
        let digest_check = format!("$dogpaddle.digest_check.{}", spec.sink_id());
        let count_check = format!("$dogpaddle.count_check.{}", spec.sink_id());
        let quoted_id = quote_identifier(TECHNICAL_ID);
        let quoted_hash = quote_identifier(TECHNICAL_HASH);
        let quoted_delivery = quote_identifier(DELIVERY_COLUMN);
        let quoted_digest = quote_identifier(DIGEST_COLUMN);
        let quoted_count = quote_identifier(MUTATION_COUNT_COLUMN);

        let mut target_definitions = vec![
            format!("{quoted_id} bigint NOT NULL"),
            format!("{quoted_hash} bytea NOT NULL"),
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
                .expect("writing SQL into a String cannot fail");
            }
            target_definitions.push(definition);
        }
        target_definitions.extend([
            format!(
                "CONSTRAINT {} PRIMARY KEY ({quoted_id})",
                quote_identifier(&target_pk)
            ),
            format!(
                "CONSTRAINT {} CHECK ({quoted_id} > 0)",
                quote_identifier(&id_check)
            ),
            format!(
                "CONSTRAINT {} CHECK (octet_length({quoted_hash}) = {HASH_LENGTH})",
                quote_identifier(&hash_check)
            ),
        ]);
        let create_target = format!("CREATE TABLE {target} ({})", target_definitions.join(", "));
        let create_hash_index = format!(
            "CREATE INDEX {} ON {target} USING btree ({quoted_hash}, {quoted_id})",
            quote_identifier(&hash_index_name)
        );
        let create_receipt = format!(
            "CREATE TABLE {receipt} (\
             {quoted_delivery} bigint NOT NULL, \
             {quoted_digest} bytea NOT NULL, \
             {quoted_count} integer NOT NULL, \
             CONSTRAINT {} PRIMARY KEY ({quoted_delivery}), \
             CONSTRAINT {} CHECK ({quoted_delivery} > 0), \
             CONSTRAINT {} CHECK (octet_length({quoted_digest}) = {DIGEST_LENGTH}), \
             CONSTRAINT {} CHECK ({quoted_count} BETWEEN 1 AND {MAX_MUTATIONS_PER_BATCH})\
             )",
            quote_identifier(&receipt_pk),
            quote_identifier(&delivery_check),
            quote_identifier(&digest_check),
            quote_identifier(&count_check),
        );

        let marker_hash = blake3::hash(
            format!("{create_target}\0{create_hash_index}\0{create_receipt}").as_bytes(),
        );
        let marker = format!("dogpaddle.postgres-relation.v1:{}", marker_hash.to_hex());
        let marker_literal = quote_literal(&marker);
        let initialize = format!(
            "{create_target}; \
             {create_hash_index}; \
             {create_receipt}; \
             COMMENT ON TABLE {target} IS {marker_literal}; \
             COMMENT ON INDEX {} IS {marker_literal}; \
             COMMENT ON INDEX {hash_index} IS {marker_literal}; \
             COMMENT ON TABLE {receipt} IS {marker_literal}; \
             COMMENT ON INDEX {} IS {marker_literal}",
            qualified(spec.schema(), &target_pk),
            qualified(spec.schema(), &receipt_pk)
        );

        let logical_names = layout
            .columns()
            .iter()
            .map(|column| quote_identifier(column.name()))
            .collect::<Vec<_>>();
        let mut all_names = vec![quoted_id.clone(), quoted_hash.clone()];
        all_names.extend(logical_names.iter().cloned());
        let insert_prefix = format!("INSERT INTO {target} ({}) VALUES ", all_names.join(", "));
        let insert_suffix = format!(" ON CONFLICT ({quoted_id}) DO NOTHING RETURNING {quoted_id}");
        let matching_exact = logical_names
            .iter()
            .enumerate()
            .map(|(index, name)| format!("{name} IS NOT DISTINCT FROM ${}", index + 2))
            .collect::<Vec<_>>();
        let matching_exact_suffix = if matching_exact.is_empty() {
            String::new()
        } else {
            format!(" AND {}", matching_exact.join(" AND "))
        };
        let deleting_exact = all_names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let comparison = if index < 2 {
                    "="
                } else {
                    "IS NOT DISTINCT FROM"
                };
                format!("target.{name} {comparison} expected.{name}")
            })
            .collect::<Vec<_>>();
        let excluded_parameter = layout.columns().len() + 2;
        let limit_parameter = excluded_parameter + 1;
        let matching = format!(
            "FROM ONLY {target} WHERE {quoted_hash} = $1{matching_exact_suffix} \
             AND {quoted_id} <> ALL(${excluded_parameter}::bigint[])"
        );
        let select_matching = format!(
            "SELECT {quoted_id} {matching} \
             ORDER BY {quoted_id} LIMIT ${limit_parameter}"
        );
        let count_matching = format!(
            "SELECT count(*) FROM (SELECT 1 {matching} LIMIT ${limit_parameter}) AS matches"
        );
        let delete_prefix = format!("DELETE FROM ONLY {target} AS target USING (VALUES ");
        let delete_suffix = format!(
            ") AS expected ({}) WHERE {} RETURNING target.{quoted_id}",
            all_names.join(", "),
            deleting_exact.join(" AND ")
        );
        let mut parameter_types = vec!["bigint", "bytea"];
        parameter_types.extend(layout.columns().iter().map(|column| column.storage().sql()));
        let insert_receipt = format!(
            "INSERT INTO {receipt} ({quoted_delivery}, {quoted_digest}, {quoted_count}) \
             VALUES ($1, $2, $3) ON CONFLICT ({quoted_delivery}) DO NOTHING"
        );
        let select_receipt = format!(
            "SELECT {quoted_digest}, {quoted_count} FROM ONLY {receipt} \
             WHERE {quoted_delivery} = $1"
        );
        let retire_receipts = format!("DELETE FROM ONLY {receipt} WHERE {quoted_delivery} < $1");
        let receipt_frontier = format!("SELECT MAX({quoted_delivery}) FROM ONLY {receipt}");
        let frontiers = format!(
            "SELECT (SELECT MAX({quoted_id}) FROM ONLY {target}), \
                    (SELECT MAX({quoted_delivery}) FROM ONLY {receipt})"
        );
        let target_empty = format!(
            "SELECT NOT EXISTS(SELECT 1 FROM ONLY {target}) \
                    AND NOT EXISTS(SELECT 1 FROM ONLY {receipt})"
        );

        Self {
            table_name: spec.table().to_owned(),
            receipt_name,
            marker,
            initialize,
            select_matching,
            count_matching,
            insert_prefix,
            insert_suffix,
            delete_prefix,
            delete_suffix,
            parameter_types,
            insert_receipt,
            retire_receipts,
            select_receipt,
            receipt_frontier,
            frontiers,
            target_empty,
        }
    }

    pub(super) fn mutation_batch_size(&self) -> usize {
        // PostgreSQL's Bind message carries a u16 parameter count. Wide
        // Schemas split into smaller statements within the same transaction.
        (usize::from(u16::MAX) / self.parameter_types.len()).min(MAX_MUTATIONS_PER_BATCH)
    }

    pub(super) fn mutation_statement(&self, kind: MutationKind, rows: usize) -> String {
        assert!((1..=self.mutation_batch_size()).contains(&rows));
        let (prefix, suffix) = match kind {
            MutationKind::Insert => (&self.insert_prefix, &self.insert_suffix),
            MutationKind::Delete => (&self.delete_prefix, &self.delete_suffix),
        };
        let mut statement = prefix.clone();
        for row in 0..rows {
            if row != 0 {
                statement.push_str(", ");
            }
            statement.push('(');
            for (column, sql_type) in self.parameter_types.iter().enumerate() {
                if column != 0 {
                    statement.push_str(", ");
                }
                let index = row * self.parameter_types.len() + column + 1;
                write!(statement, "${index}::{sql_type}").expect("writing SQL cannot fail");
            }
            statement.push(')');
        }
        statement.push_str(suffix);
        statement
    }
}

fn require_owned_layout(
    client: &mut impl GenericClient,
    spec: &PostgresTargetSpec,
    sql: &SqlPlan,
) -> Result<(), PostgresSinkError> {
    require_relation(client, spec, &sql.table_name, &sql.marker)?;
    require_relation(client, spec, &sql.receipt_name, &sql.marker)?;
    require_indexes(client, spec, sql)?;
    Ok(())
}

fn require_relation(
    client: &mut impl GenericClient,
    spec: &PostgresTargetSpec,
    name: &str,
    marker: &str,
) -> Result<(), PostgresSinkError> {
    let row = client
        .query_opt(
            "SELECT c.oid, c.relkind::text, c.relpersistence::text, \
                    c.relispartition, c.relrowsecurity, c.relforcerowsecurity, \
                    c.relhasrules, pg_catalog.obj_description(c.oid, 'pg_class'), \
                    EXISTS(SELECT 1 FROM pg_catalog.pg_trigger AS t \
                           WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) \
                    OR EXISTS(SELECT 1 FROM pg_catalog.pg_inherits AS i \
                              WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) \
                    FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&spec.schema(), &name],
        )
        .map_err(|error| database_error("inspect target relation", &error))?
        .ok_or_else(|| PostgresSinkError::TargetMissing {
            name: name.to_owned(),
        })?;
    let valid = row.get::<_, String>(1) == "r"
        && row.get::<_, String>(2) == "p"
        && !row.get::<_, bool>(3)
        && !row.get::<_, bool>(4)
        && !row.get::<_, bool>(5)
        && !row.get::<_, bool>(6)
        && row.get::<_, Option<String>>(7).as_deref() == Some(marker)
        && !row.get::<_, bool>(8);
    if !valid {
        return Err(PostgresSinkError::TargetLayoutMismatch {
            name: name.to_owned(),
        });
    }
    Ok(())
}

fn require_indexes(
    client: &mut impl GenericClient,
    spec: &PostgresTargetSpec,
    sql: &SqlPlan,
) -> Result<(), PostgresSinkError> {
    let names = spec.object_names().into_iter().skip(2).collect::<Vec<_>>();
    let row = client
        .query_one(
            "SELECT COUNT(*)::bigint, \
                    COALESCE(bool_and(c.relkind = 'i' \
                                      AND c.relpersistence = 'p' \
                                      AND i.indisvalid \
                                      AND pg_catalog.obj_description(c.oid, 'pg_class') \
                                          IS NOT DISTINCT FROM $3), \
                             false) \
             FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             LEFT JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.oid \
             WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[])",
            &[&spec.schema(), &names, &sql.marker],
        )
        .map_err(|error| database_error("inspect target indexes", &error))?;
    let count = usize::try_from(row.get::<_, i64>(0))
        .map_err(|_| invalid_batch("target index count cannot be represented by usize"))?;
    if count != names.len() || !row.get::<_, bool>(1) {
        return Err(PostgresSinkError::TargetLayoutMismatch {
            name: spec.table().to_owned(),
        });
    }
    Ok(())
}

fn object_count(
    client: &mut impl GenericClient,
    spec: &PostgresTargetSpec,
) -> Result<usize, PostgresSinkError> {
    let names = spec.object_names().to_vec();
    let row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname::text = ANY($2::text[])",
            &[&spec.schema(), &names],
        )
        .map_err(|error| database_error("inspect initialization objects", &error))?;
    usize::try_from(row.get::<_, i64>(0))
        .map_err(|_| invalid_batch("target object count cannot be represented by usize"))
}

fn target_is_empty(
    client: &mut impl GenericClient,
    sql: &SqlPlan,
) -> Result<bool, PostgresSinkError> {
    Ok(client
        .query_one(&sql.target_empty, &[])
        .map_err(|error| database_error("verify empty target", &error))?
        .get(0))
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

impl From<RowError> for PostgresSinkError {
    fn from(error: RowError) -> Self {
        match error {
            RowError::SchemaMismatch => invalid_batch(error.to_string()),
            _ => Self::Row {
                message: error.to_string(),
            },
        }
    }
}
