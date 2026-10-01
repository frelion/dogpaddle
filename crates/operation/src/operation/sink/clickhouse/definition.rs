use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use super::{
    buffered,
    config::ClickHouseSinkConfig,
    error::{ClickHouseSinkError, invalid_spec},
    row::ClickHouseRowCodec,
    schema,
    target::ClickHouseTarget,
};
use crate::operation::sink::is_valid_sink_id;
use crate::{ConstructedOperation, RuntimeResource, definition::schema_error};

const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 255;

/// Non-sensitive persistent identity of a sink-owned `ClickHouse` target.
///
/// Credentials are supplied separately through [`ClickHouseSinkConfig`].
/// Construction and Schema binding perform no network I/O.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSinkDefinition {
    pub(super) sink_id: String,
    pub(super) database: String,
    pub(super) table: String,
    pub(super) database_uuid: String,
}

impl ClickHouseSinkDefinition {
    /// Builds and validates a pure persistent sink plan without network I/O.
    ///
    /// # Errors
    ///
    /// Rejects invalid names or a zero/malformed database UUID.
    /// Also rejects plans exceeding the 1 MiB persistent definition limit.
    pub fn try_new(
        sink_id: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
        database_uuid: impl Into<String>,
    ) -> Result<Self, ClickHouseSinkError> {
        let spec = Self {
            sink_id: sink_id.into(),
            database: database.into(),
            table: table.into(),
            database_uuid: database_uuid.into(),
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Validates all persistent fields and the 1 MiB limit after decoding.
    ///
    /// # Errors
    ///
    /// Rejects invalid names or a zero/malformed database UUID.
    /// Also rejects plans exceeding the 1 MiB persistent definition limit.
    pub fn validate(&self) -> Result<(), ClickHouseSinkError> {
        self.validate_names()?;
        if self.database_uuid.len() != 36
            || !self.database_uuid.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }
            })
            || self.database_uuid == "00000000-0000-0000-0000-000000000000"
        {
            return Err(invalid_spec(
                "database UUID must be a nonzero canonical UUID",
            ));
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid_spec("target specification is not JSON-serializable"))?
            .len()
            > MAX_DEFINITION_BYTES
        {
            return Err(invalid_spec(
                "target specification exceeds the 1 MiB definition limit",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_names(&self) -> Result<(), ClickHouseSinkError> {
        if !is_valid_sink_id(&self.sink_id) {
            return Err(invalid_spec(
                "sink ID must contain 1–32 lowercase ASCII letters, digits, or underscores",
            ));
        }
        for (label, value) in [("database", &self.database), ("table", &self.table)] {
            if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.contains('\0') {
                return Err(invalid_spec(format!(
                    "{label} must be a nonempty ClickHouse identifier of at most 255 bytes"
                )));
            }
        }
        if self.state_table().len() > MAX_IDENTIFIER_BYTES {
            return Err(invalid_spec("derived state-table name exceeds 255 bytes"));
        }
        if self.table == self.state_table() {
            return Err(invalid_spec("target view collides with the state table"));
        }
        Ok(())
    }

    /// Sink identity.
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Exposed target view.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Atomic database UUID captured during discovery.
    #[must_use]
    pub fn database_uuid(&self) -> &str {
        &self.database_uuid
    }

    pub(super) fn state_table(&self) -> String {
        format!("$dogpaddle.state.{}", self.sink_id)
    }

    pub(super) fn marker(&self) -> String {
        format!(
            "dogpaddle.clickhouse-sink.occurrence-version.v1:{}",
            self.sink_id
        )
    }
}

impl ClickHouseSinkDefinition {
    pub(crate) fn output_schema_unchecked(
        &self,
        inputs: &[SchemaRef],
    ) -> Result<(), crate::OperationSchemaError> {
        self.validate()?;
        schema::validate(&inputs[0])?;
        Ok(())
    }

    pub(crate) fn construct_unchecked(
        &self,
        input_schemas: &[SchemaRef],
        data: &mut dogpaddle_store::DataScope<'_>,
        resource: RuntimeResource,
    ) -> Result<ConstructedOperation, crate::OperationSetupError> {
        self.validate().map_err(schema_error)?;
        let input_schema = input_schemas
            .first()
            .expect("the final binding entrypoint enforces ClickHouse sink input arity");
        let codec = ClickHouseRowCodec::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<ClickHouseSinkConfig>()?;
        let target = ClickHouseTarget::new_bound(config, self.clone(), codec);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<ClickHouseSinkConfig>()
    }
}
