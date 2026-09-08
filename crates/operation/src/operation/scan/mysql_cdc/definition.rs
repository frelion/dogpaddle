use std::{num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use dogpaddle_store::{AppendLog, Cell};
use serde::{Deserialize, Serialize};

use crate::{
    DataDeclaration, DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::{DataName, Sealed},
};

use super::{MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanOperation, MySqlColumn, schema};

pub(crate) const TAG: u16 = 15;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const PHASE: DataName<Cell<u32>> = DataName::new("mysql_cdc_scan.phase");
const CHECKPOINT: DataName<Cell<Vec<u8>>> = DataName::new("mysql_cdc_scan.checkpoint");
const BOOTSTRAP_SPOOL: DataName<AppendLog<Vec<u8>>> =
    DataName::new("mysql_cdc_scan.bootstrap_spool");
static DATA: [DataDeclaration; 3] = [
    PHASE.declaration(),
    CHECKPOINT.declaration(),
    BOOTSTRAP_SPOOL.declaration(),
];

pub(super) const CONNECTOR_CLASS: &str = "io.debezium.connector.mysql.MySqlConnector";

/// Non-sensitive identity and ordered logical columns discovered before building a Flow.
///
/// The runtime verifies this identity against `MySQL` before starting its
/// connector. Reusing an engine name or replication client ID for another live
/// Scan is unsupported. The Scan does not create or alter the source table.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlCdcScanSpec {
    /// Stable Debezium engine name, also used as its topic prefix.
    pub engine_name: String,
    /// `MySQL` database name.
    pub database: String,
    /// Captured table name.
    pub table: String,
    /// Immutable UUID of the source `MySQL` server.
    pub server_uuid: String,
    /// `InnoDB`'s nonzero durable table identity.
    pub table_id: u64,
    /// Complete ordered logical columns; unsupported `MySQL` types are rejected.
    pub columns: Vec<MySqlColumn>,
}

/// Fixed-Schema, single-table `MySQL` snapshot and binlog CDC Scan.
///
/// Credentials and runtime bundle paths are supplied separately through
/// [`MySqlCdcScanConfig`]. Build and open perform no `MySQL` or JVM I/O. On its
/// first advance the Scan captures a consistent full table snapshot into its
/// private durable spool, publishes that spool, and then continues from the
/// snapshot's sealed checkpoint. No source-write gate is required. Online
/// Schema evolution is not supported.
#[derive(Clone, Debug)]
pub struct MySqlCdcScanDefinition {
    spec: MySqlCdcScanSpec,
    bootstrap_spool_bytes: NonZeroU64,
}

impl MySqlCdcScanDefinition {
    /// Freezes a discovered source and its private bootstrap spool capacity.
    ///
    /// The capacity is an exact logical retained-byte ceiling: each encoded
    /// Change contributes its full IPC byte length plus its eight-byte log
    /// offset. It must hold the complete initial snapshot until publication.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid source identity, invalid logical
    /// columns, or an oversized persistent definition.
    pub fn try_new(
        spec: MySqlCdcScanSpec,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, MySqlCdcScanError> {
        validate_spec(&spec)?;
        let definition = Self {
            spec,
            bootstrap_spool_bytes,
        };
        if encode(&definition)?.len() > MAX_DEFINITION_BYTES {
            return Err(MySqlCdcScanError::InvalidDefinition(
                "scan definition exceeds 1 MiB".to_owned(),
            ));
        }
        Ok(definition)
    }

    /// Returns the frozen, non-sensitive Scan specification.
    #[must_use]
    pub const fn spec(&self) -> &MySqlCdcScanSpec {
        &self.spec
    }

    /// Returns the exact logical retained-byte limit of the bootstrap spool.
    #[must_use]
    pub const fn bootstrap_spool_bytes(&self) -> NonZeroU64 {
        self.bootstrap_spool_bytes
    }
}

impl Sealed for MySqlCdcScanDefinition {
    fn bind_schemas(&self, _: &[SchemaRef]) -> Result<OperationBinding, OperationSchemaError> {
        let output = schema::compile(&self.spec.columns)?;
        let spec = self.spec.clone();
        let bootstrap_spool_bytes = self.bootstrap_spool_bytes;
        Ok(OperationBinding::with_resource::<MySqlCdcScanConfig, _>(
            Some(Arc::clone(&output)),
            move |data, config| {
                Ok(Box::new(MySqlCdcScanOperation::new_bound(
                    spec,
                    output,
                    data.take(&PHASE)?,
                    data.take(&CHECKPOINT)?,
                    data.take(&BOOTSTRAP_SPOOL)?,
                    bootstrap_spool_bytes,
                    config,
                )))
            },
        ))
    }
}

impl OperationDefinition for MySqlCdcScanDefinition {
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
        output.extend(encode(self).expect("validated MySQL scan definition is encodable"));
    }
}

pub(crate) fn decode_definition(
    payload_bytes: &[u8],
) -> Result<Box<dyn OperationDefinition>, DefinitionCodecError> {
    let invalid = || DefinitionCodecError::InvalidPayload("invalid MySQL CDC scan specification");
    if payload_bytes.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let payload: PersistentDefinition =
        serde_json::from_slice(payload_bytes).map_err(|_| invalid())?;
    let definition = MySqlCdcScanDefinition::try_new(payload.spec, payload.bootstrap_spool_bytes)
        .map_err(|_| invalid())?;
    let mut canonical = Vec::new();
    definition.encode_payload(&mut canonical);
    if canonical != payload_bytes {
        return Err(invalid());
    }
    Ok(Box::new(definition))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistentDefinition {
    spec: MySqlCdcScanSpec,
    bootstrap_spool_bytes: NonZeroU64,
}

fn encode(definition: &MySqlCdcScanDefinition) -> Result<Vec<u8>, MySqlCdcScanError> {
    serde_json::to_vec(&PersistentDefinition {
        spec: definition.spec.clone(),
        bootstrap_spool_bytes: definition.bootstrap_spool_bytes,
    })
    .map_err(|_| MySqlCdcScanError::InvalidDefinition("cannot encode scan definition".to_owned()))
}

pub(super) fn validate_spec(spec: &MySqlCdcScanSpec) -> Result<(), MySqlCdcScanError> {
    let invalid = |message: &str| MySqlCdcScanError::InvalidDefinition(message.to_owned());
    for value in [&spec.engine_name, &spec.database, &spec.table] {
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
    if !is_uuid(&spec.server_uuid) || spec.table_id == 0 {
        return Err(invalid(
            "server UUID must be canonical lowercase hexadecimal and table identity must be nonzero",
        ));
    }
    if spec.columns.is_empty() || spec.columns.len() > 1600 {
        return Err(invalid("pilot tables require between 1 and 1600 columns"));
    }
    Ok(())
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(byte == b'-')
                .unwrap_or_else(|| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::super::MySqlType;
    use super::*;

    fn spec(engine_name: &str) -> MySqlCdcScanSpec {
        MySqlCdcScanSpec {
            engine_name: engine_name.to_owned(),
            database: "shop".to_owned(),
            table: "orders".to_owned(),
            server_uuid: "01234567-89ab-cdef-0123-456789abcdef".to_owned(),
            table_id: 1,
            columns: vec![MySqlColumn::new("id", MySqlType::Int64, false)],
        }
    }

    fn capacity() -> NonZeroU64 {
        NonZeroU64::new(1024 * 1024).unwrap()
    }

    #[test]
    fn spool_capacity_is_canonical_definition_payload_and_survives_binding() {
        let definition = MySqlCdcScanDefinition::try_new(spec("orders"), capacity()).unwrap();
        let payload = encode(&definition).unwrap();
        let decoded = decode_definition(&payload).unwrap();
        assert_eq!(
            crate::encode_definition(decoded.as_ref()),
            crate::encode_definition(&definition)
        );
        assert_eq!(definition.bootstrap_spool_bytes(), capacity());
    }

    #[test]
    fn definition_rejects_invalid_source_identity_and_schema() {
        let column = |data_type| MySqlColumn::new("id", data_type, false);
        for columns in [
            vec![],
            vec![MySqlColumn::new("", MySqlType::Int64, false)],
            vec![MySqlColumn::new(
                "$dogpaddle.value",
                MySqlType::Int64,
                false,
            )],
            vec![column(MySqlType::Int64), column(MySqlType::Text)],
            vec![column(MySqlType::Decimal {
                precision: 0,
                scale: 0,
            })],
            vec![column(MySqlType::Decimal {
                precision: 39,
                scale: 0,
            })],
            vec![column(MySqlType::Decimal {
                precision: 2,
                scale: 3,
            })],
            vec![column(MySqlType::Decimal {
                precision: 2,
                scale: -1,
            })],
        ] {
            let mut candidate = spec("orders");
            candidate.columns = columns;
            if let Ok(definition) = MySqlCdcScanDefinition::try_new(candidate, capacity()) {
                assert!((&definition as &dyn OperationDefinition).bind(&[]).is_err());
            }
        }
        for (server_uuid, table_id) in [
            ("not-a-uuid".into(), 1),
            ("01234567-89ab-cdef-0123-456789abcdef".into(), 0),
        ] {
            let mut candidate = spec("orders");
            candidate.server_uuid = server_uuid;
            candidate.table_id = table_id;
            assert!(MySqlCdcScanDefinition::try_new(candidate, capacity()).is_err());
        }
    }
}
