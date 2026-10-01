use std::{any::TypeId, num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    ConstructedOperation, DefinitionCodecError, RuntimeResource,
    codec::{parse_json_payload, require_canonical_json_payload},
    definition::schema_error,
};

use super::{
    PostgresCdcScanConfig, PostgresCdcScanError, PostgresCdcScanOperation, PostgresColumn, schema,
};
use dogpaddle_store::{Cell, Queue};

pub(crate) const TAG: u16 = 11;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
pub(super) const PHASE: &str = "postgres_cdc_scan.phase";
pub(super) const CHECKPOINT: &str = "postgres_cdc_scan.checkpoint";
pub(super) const INPUT: &str = "postgres_cdc_scan.input";

/// Non-sensitive identity and ordered logical columns discovered before building a Flow.
///
/// The runtime verifies this identity against `PostgreSQL` before starting its
/// connector. Reusing an engine name, publication, or slot for another live
/// Scan is unsupported. The Scan owns the slot name and creates or drops that
/// slot only while bootstrapping; the publication remains preconfigured.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcScanSpec {
    /// Stable Debezium engine name, also used as its topic prefix.
    pub engine_name: String,
    /// `PostgreSQL` database name.
    pub database: String,
    /// Captured table's schema name.
    pub schema: String,
    /// Captured table name.
    pub table: String,
    /// Exclusively owned logical replication slot, absent before first start.
    pub slot: String,
    /// Pre-created publication containing the complete captured table.
    pub publication: String,
    /// `PostgreSQL` cluster system identifier, preserved as decimal text.
    pub system_identifier: String,
    /// Database object identity inside this cluster.
    pub database_oid: u32,
    /// Table object identity inside this database.
    pub table_oid: u32,
    /// Complete ordered logical columns; unsupported `PostgreSQL` types are rejected.
    pub columns: Vec<PostgresColumn>,
}

/// Fixed-Schema, single-table `PostgreSQL` Scan with an initial snapshot and
/// continuous WAL CDC.
///
/// Credentials and runtime bundle paths are supplied separately through
/// [`super::PostgresCdcScanConfig`]. Construction, binding, build, and open perform no
/// `PostgreSQL` or JVM I/O. Online Schema evolution is not supported. The
/// initial snapshot remains private until sealed, then becomes visible in the
/// input queue. WAL streaming resumes from the sealed checkpoint while Flow
/// consumes that input, with streaming capture bounded by its queue capacity.
#[derive(Clone, Debug, Serialize)]
pub struct PostgresCdcScanDefinition {
    spec: PostgresCdcScanSpec,
    output_projection: Vec<u32>,
    bootstrap_spool_bytes: NonZeroU64,
}

impl PostgresCdcScanDefinition {
    /// Freezes a non-sensitive Scan specification as a persistent definition.
    ///
    /// Obtain the specification with [`super::PostgresCdcScanConfig::discover`] before
    /// constructing a Flow. Runtime checks also protect manually supplied specs.
    ///
    /// `bootstrap_spool_bytes` is the maximum logical bytes retained by the
    /// input queue during bootstrap. Each Change contributes its actual
    /// schema-bound encoded-entry length plus the queue's private eight-byte
    /// sequence key. It must hold the complete snapshot plus WAL changes
    /// observed before the snapshot is sealed and becomes visible.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identifiers or an oversized persistent definition.
    pub fn try_new(
        spec: PostgresCdcScanSpec,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, PostgresCdcScanError> {
        let output_projection = (0..spec.columns.len())
            .map(u32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                PostgresCdcScanError::InvalidDefinition(
                    "source column count exceeds the projection index range".to_owned(),
                )
            })?;
        Self::try_new_projected(spec, output_projection, bootstrap_spool_bytes)
    }

    /// Freezes a source while exposing only the selected source columns.
    ///
    /// `output_projection` contains strictly increasing zero-based indexes into
    /// the complete ordered [`PostgresCdcScanSpec::columns`]. An empty
    /// projection preserves row counts and differences without retaining any
    /// source column in the input queue or public output.
    ///
    /// The complete source Schema remains in the specification and is still
    /// validated against every Debezium envelope and row image.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid source specification, an unordered,
    /// duplicate, or out-of-range projection, or an oversized persistent
    /// definition.
    pub fn try_new_projected(
        spec: PostgresCdcScanSpec,
        output_projection: Vec<u32>,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, PostgresCdcScanError> {
        validate(&spec)?;
        super::super::ordered_projection(&output_projection, spec.columns.len())
            .ok_or_else(invalid_projection)?;
        let definition = Self {
            spec,
            output_projection,
            bootstrap_spool_bytes,
        };
        if encode(&definition)?.len() > MAX_DEFINITION_BYTES {
            return Err(PostgresCdcScanError::InvalidDefinition(
                "scan definition exceeds 1 MiB".to_owned(),
            ));
        }
        Ok(definition)
    }

    /// Returns the frozen, non-sensitive Scan specification.
    #[must_use]
    pub const fn spec(&self) -> &PostgresCdcScanSpec {
        &self.spec
    }

    /// Returns the strictly increasing source-column indexes exposed by this Scan.
    #[must_use]
    pub fn output_projection(&self) -> &[u32] {
        &self.output_projection
    }

    /// Returns the input queue's bootstrap capacity in logical bytes.
    #[must_use]
    pub const fn bootstrap_spool_bytes(&self) -> NonZeroU64 {
        self.bootstrap_spool_bytes
    }
}

impl PostgresCdcScanDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        output_schema(&self.spec, &self.output_projection)
            .map(Some)
            .map_err(Into::into)
    }

    pub(crate) fn construct_unchecked(
        &self,
        scope: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        let output = output_schema(&self.spec, &self.output_projection).map_err(schema_error)?;
        let phase = scope.data::<Cell<u32>>(PHASE)?;
        let checkpoint = scope.data::<Cell<Vec<u8>>>(CHECKPOINT)?;
        let input = scope.data::<Queue<Vec<u8>>>(INPUT)?;
        let config = resource.take::<PostgresCdcScanConfig>()?;
        let operation = PostgresCdcScanOperation::new_bound(
            self,
            Arc::clone(&output),
            phase,
            checkpoint,
            input,
            config,
        )
        .map_err(schema_error)?;
        Ok(ConstructedOperation::source(Some(output), operation))
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<PostgresCdcScanConfig>()
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<PostgresCdcScanDefinition>, DefinitionCodecError> {
    let invalid =
        || DefinitionCodecError::InvalidPayload("invalid PostgreSQL CDC scan specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let persistent: PersistentDefinition = parse_json_payload(payload)?;
    let capacity = NonZeroU64::new(persistent.bootstrap_spool_bytes).ok_or_else(invalid)?;
    let definition = PostgresCdcScanDefinition::try_new_projected(
        persistent.spec,
        persistent.output_projection,
        capacity,
    )
    .map_err(|_| invalid())?;
    require_canonical_json_payload(
        &definition,
        payload,
        "invalid PostgreSQL CDC scan specification",
    )?;
    Ok(Box::new(definition))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistentDefinition {
    spec: PostgresCdcScanSpec,
    output_projection: Vec<u32>,
    bootstrap_spool_bytes: u64,
}

fn encode(definition: &PostgresCdcScanDefinition) -> Result<Vec<u8>, PostgresCdcScanError> {
    serde_json::to_vec(definition).map_err(|_| {
        PostgresCdcScanError::InvalidDefinition("cannot encode scan definition".to_owned())
    })
}

fn validate(spec: &PostgresCdcScanSpec) -> Result<(), PostgresCdcScanError> {
    let invalid = |message: &str| PostgresCdcScanError::InvalidDefinition(message.to_owned());
    for value in [
        &spec.engine_name,
        &spec.schema,
        &spec.table,
        &spec.slot,
        &spec.publication,
    ] {
        if value.is_empty()
            || value.len() > 63
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(invalid(
                "pilot identifiers must contain 1–63 lowercase ASCII letters, digits, or underscores",
            ));
        }
    }
    if spec.database.is_empty() || spec.database.len() > 63 || spec.database.contains('\0') {
        return Err(invalid("invalid database name"));
    }
    if spec
        .system_identifier
        .parse::<u64>()
        .ok()
        .is_none_or(|id| id == 0)
        || spec.database_oid == 0
        || spec.table_oid == 0
    {
        return Err(invalid(
            "cluster, database, and table identities must be nonzero",
        ));
    }
    if spec.columns.is_empty() || spec.columns.len() > 1600 {
        return Err(invalid("pilot tables require between 1 and 1600 columns"));
    }
    Ok(())
}

fn output_schema(
    spec: &PostgresCdcScanSpec,
    projection: &[u32],
) -> Result<SchemaRef, PostgresCdcScanError> {
    let full = schema::compile(&spec.columns)?;
    let indices = super::super::ordered_projection(projection, full.fields().len())
        .ok_or_else(invalid_projection)?;
    Ok(Arc::new(
        full.project(&indices).map_err(|_| invalid_projection())?,
    ))
}

fn invalid_projection() -> PostgresCdcScanError {
    PostgresCdcScanError::InvalidDefinition(
        "output projection must contain unique source columns in source order".to_owned(),
    )
}
