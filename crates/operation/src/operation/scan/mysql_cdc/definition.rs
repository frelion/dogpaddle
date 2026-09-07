use std::sync::Arc;

use arrow_schema::SchemaRef;
use base64::{Engine as _, prelude::BASE64_STANDARD};
use dogpaddle_debezium::Checkpoint;
use dogpaddle_store::Cell;
use serde::{Deserialize, Serialize};

use crate::{
    DataDeclaration, DefinitionCodecError, OperationBinding, OperationDefinition, OperationKind,
    OperationSchemaError,
    definition::{DataName, Sealed},
};

use super::{MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanOperation, MySqlColumn, schema};

pub(crate) const TAG: u16 = 14;
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const CHECKPOINT: DataName<Cell<Vec<u8>>> = DataName::new("mysql_cdc_scan.checkpoint");
static DATA: [DataDeclaration; 1] = [CHECKPOINT.declaration()];

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

/// Fixed-Schema, single-table `MySQL` Scan using only continuous binlog CDC.
///
/// Credentials and runtime bundle paths are supplied separately through
/// [`MySqlCdcScanConfig`]. Build and open perform no `MySQL` or JVM I/O. The
/// configuration's [`MySqlCdcScanConfig::bootstrap_definition`] method creates
/// a Definition only after it has obtained one exact pre-publication Debezium
/// tail seed. The seed becomes durable when its canonical Flow Definition
/// commits.
/// Initial table-data snapshots and online Schema evolution are not supported;
/// the runtime performs only its internal schema recovery.
///
/// That immutable cursor is atomically published with the Flow Definition and
/// becomes the fallback when the mutable checkpoint Cell is empty. Every
/// reopen uses the current catalog to reconstruct in-memory schema history, so
/// DDL is unsupported throughout the retained-binlog recovery window.
#[derive(Clone, Debug)]
pub struct MySqlCdcScanDefinition {
    spec: MySqlCdcScanSpec,
    bootstrap_checkpoint: Checkpoint,
}

impl MySqlCdcScanDefinition {
    /// Freezes the verified pre-publication bootstrap result.
    ///
    /// This is intentionally not public: matching a checkpoint's engine and
    /// connector class cannot prove that its opaque offsets came from this
    /// exact source table. Public callers must use
    /// [`MySqlCdcScanConfig::bootstrap_definition`].
    pub(crate) fn from_bootstrap(
        spec: MySqlCdcScanSpec,
        bootstrap_checkpoint: Checkpoint,
    ) -> Result<Self, MySqlCdcScanError> {
        validate_spec(&spec)?;
        if !bootstrap_checkpoint.matches(&spec.engine_name, CONNECTOR_CLASS) {
            return Err(MySqlCdcScanError::InvalidDefinition(
                "bootstrap checkpoint belongs to a different MySQL connector".to_owned(),
            ));
        }
        let definition = Self {
            spec,
            bootstrap_checkpoint,
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
}

impl Sealed for MySqlCdcScanDefinition {
    fn bind_schemas(&self, _: &[SchemaRef]) -> Result<OperationBinding, OperationSchemaError> {
        let output = schema::compile(&self.spec.columns)?;
        let spec = self.spec.clone();
        let bootstrap_checkpoint = self.bootstrap_checkpoint.clone();
        Ok(OperationBinding::with_resource::<MySqlCdcScanConfig, _>(
            Some(Arc::clone(&output)),
            move |data, config| {
                Ok(Box::new(MySqlCdcScanOperation::new_bound(
                    spec,
                    output,
                    data.take(&CHECKPOINT)?,
                    config,
                    bootstrap_checkpoint,
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
    let checkpoint = BASE64_STANDARD
        .decode(payload.bootstrap_checkpoint)
        .map_err(|_| invalid())?;
    let checkpoint = Checkpoint::from_bytes(checkpoint).map_err(|_| invalid())?;
    let definition =
        MySqlCdcScanDefinition::from_bootstrap(payload.spec, checkpoint).map_err(|_| invalid())?;
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
    bootstrap_checkpoint: String,
}

fn encode(definition: &MySqlCdcScanDefinition) -> Result<Vec<u8>, MySqlCdcScanError> {
    serde_json::to_vec(&PersistentDefinition {
        spec: definition.spec.clone(),
        bootstrap_checkpoint: BASE64_STANDARD.encode(definition.bootstrap_checkpoint.as_bytes()),
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

    fn checkpoint(engine_name: &str) -> Checkpoint {
        let encoded = match engine_name {
            "orders" => {
                "RFBEQkNQMDEAAQAAAAZvcmRlcnMAAAAqaW8uZGViZXppdW0uY29ubmVjdG9yLm15c3FsLk15U3FsQ29ubmVjdG9yAAAAAQAAAAEAAAAAAQDCpe+v"
            }
            "other" => {
                "RFBEQkNQMDEAAQAAAAVvdGhlcgAAACppby5kZWJleml1bS5jb25uZWN0b3IubXlzcWwuTXlTcWxDb25uZWN0b3IAAAABAAAAAQAAAAABADrK/00="
            }
            _ => unreachable!(),
        };
        Checkpoint::from_bytes(BASE64_STANDARD.decode(encoded).unwrap()).unwrap()
    }

    #[test]
    fn bootstrap_checkpoint_must_match_the_definition_engine_and_mysql_connector() {
        assert!(
            MySqlCdcScanDefinition::from_bootstrap(spec("orders"), checkpoint("orders")).is_ok()
        );
        assert!(
            MySqlCdcScanDefinition::from_bootstrap(spec("orders"), checkpoint("other")).is_err()
        );
    }

    #[test]
    fn bootstrap_checkpoint_is_canonical_definition_payload_and_survives_binding() {
        let definition =
            MySqlCdcScanDefinition::from_bootstrap(spec("orders"), checkpoint("orders")).unwrap();
        let payload = encode(&definition).unwrap();
        let decoded = decode_definition(&payload).unwrap();
        assert_eq!(
            crate::encode_definition(decoded.as_ref()),
            crate::encode_definition(&definition)
        );
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
            if let Ok(definition) =
                MySqlCdcScanDefinition::from_bootstrap(candidate, checkpoint("orders"))
            {
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
            assert!(
                MySqlCdcScanDefinition::from_bootstrap(candidate, checkpoint("orders")).is_err()
            );
        }
    }
}
