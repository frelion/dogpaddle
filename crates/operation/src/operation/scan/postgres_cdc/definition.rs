use std::{num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_store::{Cell, Queue};
use serde::{Deserialize, Serialize};

use crate::{
    DataDeclaration, DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::{DataName, Sealed},
};

use super::{
    PostgresCdcScanConfig, PostgresCdcScanError, PostgresCdcScanOperation, PostgresColumn, schema,
};

pub(crate) const TAG: u16 = 11;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const PHASE: DataName<Cell<u32>> = DataName::new("postgres_cdc_scan.phase");
const CHECKPOINT: DataName<Cell<Vec<u8>>> = DataName::new("postgres_cdc_scan.checkpoint");
const BOOTSTRAP_SPOOL: DataName<Queue<Vec<u8>>> =
    DataName::new("postgres_cdc_scan.bootstrap_spool");
static DATA: [DataDeclaration; 3] = [
    PHASE.declaration(),
    CHECKPOINT.declaration(),
    BOOTSTRAP_SPOOL.declaration(),
];

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
/// [`PostgresCdcScanConfig`]. Construction, binding, build, and open perform no
/// `PostgreSQL` or JVM I/O. Online Schema evolution is not supported. The
/// initial snapshot remains private until it is complete, then drains through
/// the ordinary Flow output before WAL streaming resumes from the snapshot's
/// sealed checkpoint.
#[derive(Clone, Debug)]
pub struct PostgresCdcScanDefinition {
    spec: PostgresCdcScanSpec,
    bootstrap_spool_bytes: NonZeroU64,
}

impl PostgresCdcScanDefinition {
    /// Freezes a non-sensitive Scan specification as a persistent definition.
    ///
    /// Obtain the specification with [`PostgresCdcScanConfig::discover`] before
    /// constructing a Flow. Runtime checks also protect manually supplied specs.
    ///
    /// `bootstrap_spool_bytes` is the maximum logical bytes retained by the
    /// private initial-snapshot spool. It must hold the complete snapshot plus
    /// WAL changes observed before snapshot publication finishes.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identifiers or an oversized specification.
    pub fn try_new(
        spec: PostgresCdcScanSpec,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, PostgresCdcScanError> {
        validate(&spec)?;
        let definition = Self {
            spec,
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

    /// Returns the private initial-snapshot spool capacity in logical bytes.
    #[must_use]
    pub const fn bootstrap_spool_bytes(&self) -> NonZeroU64 {
        self.bootstrap_spool_bytes
    }
}

impl Sealed for PostgresCdcScanDefinition {
    fn bind_schemas(&self, _: &[SchemaRef]) -> Result<OperationBinding, OperationSchemaError> {
        let output = schema::compile(&self.spec.columns)?;
        let spec = self.spec.clone();
        let bootstrap_spool_bytes = self.bootstrap_spool_bytes;
        Ok(OperationBinding::with_resource::<PostgresCdcScanConfig, _>(
            Some(Arc::clone(&output)),
            move |data, config| {
                Ok(Box::new(PostgresCdcScanOperation::new_bound(
                    spec,
                    output,
                    data.take(&PHASE)?,
                    data.take(&CHECKPOINT)?,
                    data.take(&BOOTSTRAP_SPOOL)?,
                    config,
                    bootstrap_spool_bytes,
                )))
            },
        ))
    }
}

impl OperationDefinition for PostgresCdcScanDefinition {
    fn kind(&self) -> OperationKind {
        OperationKind::Scan
    }

    fn data(&self) -> &'static [DataDeclaration] {
        &DATA
    }

    fn persistence_tag(&self) -> u16 {
        TAG
    }

    fn encode_payload(&self, output: &mut Vec<u8>) {
        output.extend(encode(self).expect("validated PostgreSQL scan definition is encodable"));
    }
}

pub(crate) fn decode_definition(
    payload: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let invalid =
        || DefinitionCodecError::InvalidPayload("invalid PostgreSQL CDC scan specification");
    if payload.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let persistent: PersistentDefinition =
        serde_json::from_slice(payload).map_err(|_| invalid())?;
    let capacity = NonZeroU64::new(persistent.bootstrap_spool_bytes).ok_or_else(invalid)?;
    let definition =
        PostgresCdcScanDefinition::try_new(persistent.spec, capacity).map_err(|_| invalid())?;
    let mut canonical = Vec::new();
    definition.encode_payload(&mut canonical);
    if canonical != payload {
        return Err(invalid());
    }
    Ok(Box::new(definition))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistentDefinition {
    spec: PostgresCdcScanSpec,
    bootstrap_spool_bytes: u64,
}

fn encode(definition: &PostgresCdcScanDefinition) -> Result<Vec<u8>, PostgresCdcScanError> {
    serde_json::to_vec(&PersistentDefinition {
        spec: definition.spec.clone(),
        bootstrap_spool_bytes: definition.bootstrap_spool_bytes.get(),
    })
    .map_err(|_| {
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
    if serde_json::to_vec(spec)
        .map_err(|_| invalid("cannot encode scan specification"))?
        .len()
        > MAX_DEFINITION_BYTES
    {
        return Err(invalid("scan specification exceeds 1 MiB"));
    }
    Ok(())
}
