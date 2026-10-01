use std::{any::TypeId, num::NonZeroU64, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{ConstructedOperation, RuntimeResource, definition::schema_error};

use super::{MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanOperation, MySqlColumn, schema};
use dogpaddle_store::{Cell, Queue};

pub(super) const CONNECTOR_CLASS: &str = "io.debezium.connector.mysql.MySqlConnector";
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
pub(super) const PHASE: &str = "mysql_cdc_scan.phase";
pub(super) const CHECKPOINT: &str = "mysql_cdc_scan.checkpoint";
pub(super) const INPUT: &str = "mysql_cdc_scan.input";

/// Non-sensitive identity and ordered logical columns discovered before building a Flow.
///
/// The runtime verifies this identity against `MySQL` before starting its
/// connector. Reusing an engine name for another live Scan is unsupported; it
/// also determines the connector's replication client ID. The Scan does not
/// create or alter the source table.
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
/// [`super::MySqlCdcScanConfig`]. Build and open perform no `MySQL` or JVM I/O. On its
/// first advance the Scan captures a consistent full table snapshot into its
/// durable input queue and makes that input visible when sealed. Streaming
/// resumes from the sealed checkpoint while Flow consumes that input, with
/// streaming capture bounded by its queue capacity. No source-write gate is required. Online
/// Schema evolution is not supported.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlCdcScanDefinition {
    spec: MySqlCdcScanSpec,
    output_projection: Vec<u32>,
    bootstrap_spool_bytes: NonZeroU64,
}

impl MySqlCdcScanDefinition {
    /// Freezes a discovered source and the input queue's bootstrap capacity.
    ///
    /// The capacity is an exact logical retained-byte ceiling: each Change
    /// contributes its actual schema-bound encoded-entry length plus the
    /// queue's private eight-byte sequence key. It must hold the complete
    /// initial snapshot until it is sealed and becomes visible.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid source identity, invalid logical
    /// columns, or an oversized persistent definition.
    pub fn try_new(
        spec: MySqlCdcScanSpec,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, MySqlCdcScanError> {
        let output_projection = (0..spec.columns.len())
            .map(u32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                MySqlCdcScanError::InvalidDefinition(
                    "source column count exceeds the projection index range".to_owned(),
                )
            })?;
        Self::try_new_projected(spec, output_projection, bootstrap_spool_bytes)
    }

    /// Freezes a source while exposing only the selected source columns.
    ///
    /// `output_projection` contains strictly increasing zero-based indexes into
    /// the complete ordered [`MySqlCdcScanSpec::columns`]. An empty projection
    /// preserves row counts and differences without retaining source columns in
    /// the input queue or public output.
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
        spec: MySqlCdcScanSpec,
        output_projection: Vec<u32>,
        bootstrap_spool_bytes: NonZeroU64,
    ) -> Result<Self, MySqlCdcScanError> {
        let definition = Self {
            spec,
            output_projection,
            bootstrap_spool_bytes,
        };
        definition.validate()?;
        Ok(definition)
    }

    fn validate(&self) -> Result<(), MySqlCdcScanError> {
        validate_spec(&self.spec)?;
        super::super::ordered_projection(&self.output_projection, self.spec.columns.len())
            .ok_or_else(invalid_projection)?;
        if encode(self)?.len() > MAX_DEFINITION_BYTES {
            return Err(MySqlCdcScanError::InvalidDefinition(
                "scan definition exceeds 1 MiB".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns the frozen, non-sensitive Scan specification.
    #[must_use]
    pub const fn spec(&self) -> &MySqlCdcScanSpec {
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

impl MySqlCdcScanDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
    ) -> Result<Option<SchemaRef>, crate::OperationSchemaError> {
        self.validate()?;
        output_schema(&self.spec, &self.output_projection)
            .map(Some)
            .map_err(Into::into)
    }

    pub(crate) fn construct_unchecked(
        &self,
        scope: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        self.validate().map_err(schema_error)?;
        let output = output_schema(&self.spec, &self.output_projection).map_err(schema_error)?;
        let phase = scope.data::<Cell<u32>>(PHASE)?;
        let checkpoint = scope.data::<Cell<Vec<u8>>>(CHECKPOINT)?;
        let input = scope.data::<Queue<Vec<u8>>>(INPUT)?;
        let config = resource.take::<MySqlCdcScanConfig>()?;
        let operation = MySqlCdcScanOperation::new_bound(
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
        TypeId::of::<MySqlCdcScanConfig>()
    }
}

fn encode(definition: &MySqlCdcScanDefinition) -> Result<Vec<u8>, MySqlCdcScanError> {
    serde_json::to_vec(definition).map_err(|_| {
        MySqlCdcScanError::InvalidDefinition("cannot encode scan definition".to_owned())
    })
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

fn output_schema(
    spec: &MySqlCdcScanSpec,
    projection: &[u32],
) -> Result<SchemaRef, MySqlCdcScanError> {
    let full = schema::compile(&spec.columns)?;
    let indices = super::super::ordered_projection(projection, full.fields().len())
        .ok_or_else(invalid_projection)?;
    Ok(Arc::new(
        full.project(&indices).map_err(|_| invalid_projection())?,
    ))
}

fn invalid_projection() -> MySqlCdcScanError {
    MySqlCdcScanError::InvalidDefinition(
        "output projection must contain unique source columns in source order".to_owned(),
    )
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
        let decoded: MySqlCdcScanDefinition = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            crate::encode_definition(&crate::OperationDefinition::from(decoded)),
            crate::encode_definition(&crate::OperationDefinition::from(definition.clone()))
        );
        assert_eq!(definition.bootstrap_spool_bytes(), capacity());
    }

    #[test]
    fn output_projection_is_ordered_and_can_be_empty() {
        let mut spec = spec("orders");
        spec.columns
            .push(MySqlColumn::new("payload", MySqlType::Text, true));

        let projected =
            MySqlCdcScanDefinition::try_new_projected(spec.clone(), vec![1], capacity()).unwrap();
        assert_eq!(projected.output_projection(), &[1]);
        let output = crate::OperationDefinition::from(projected)
            .output_schema(&[])
            .unwrap()
            .unwrap();
        assert_eq!(output.fields().len(), 1);
        assert_eq!(output.field(0).name(), "payload");
        assert!(output.field(0).is_nullable());

        let empty =
            MySqlCdcScanDefinition::try_new_projected(spec.clone(), vec![], capacity()).unwrap();
        assert!(
            crate::OperationDefinition::from(empty)
                .output_schema(&[])
                .unwrap()
                .unwrap()
                .fields()
                .is_empty()
        );

        for invalid in [vec![0, 0], vec![1, 0], vec![2]] {
            assert!(
                MySqlCdcScanDefinition::try_new_projected(spec.clone(), invalid, capacity())
                    .is_err()
            );
        }
    }

    #[test]
    fn source_identity_is_checked_at_definition_and_schema_at_binding() {
        let mut no_columns = spec("orders");
        no_columns.columns.clear();
        assert!(MySqlCdcScanDefinition::try_new(no_columns, capacity()).is_err());

        let column = |data_type| MySqlColumn::new("id", data_type, false);
        for columns in [
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
            let definition = MySqlCdcScanDefinition::try_new(candidate, capacity()).unwrap();
            assert!(
                crate::OperationDefinition::from(definition)
                    .output_schema(&[])
                    .is_err()
            );
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
