use std::{any::TypeId, sync::Arc};

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use super::{
    buffered,
    config::PostgresSinkConfig,
    error::{PostgresSinkError, invalid_spec},
    row::PostgresRowCodec,
    schema,
    target::PostgresTarget,
};
use crate::operation::sink::is_valid_sink_id;
use crate::{ConstructedOperation, RuntimeResource, definition::schema_error};

const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 63;

/// Pure persistent plan for a sink-owned `PostgreSQL` target.
///
/// Credentials are supplied separately through [`PostgresSinkConfig`].
/// Construction and Schema binding perform no network I/O.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkDefinition {
    pub(super) sink_id: String,
    pub(super) database: String,
    pub(super) schema: String,
    pub(super) table: String,
    pub(super) system_identifier: String,
    pub(super) database_oid: u32,
}

impl PostgresSinkDefinition {
    /// Builds and validates a pure persistent sink plan without network I/O.
    ///
    /// Production callers normally obtain this value through
    /// [`PostgresSinkConfig::discover_target`]. Constructing a value manually
    /// does not adopt or share objects owned by another Flow.
    ///
    /// # Errors
    ///
    /// Rejects malformed identifiers and zero cluster/database identities.
    /// Also rejects plans exceeding the 1 MiB persistent definition limit.
    pub fn try_new(
        sink_id: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        system_identifier: impl Into<String>,
        database_oid: u32,
    ) -> Result<Self, PostgresSinkError> {
        let spec = Self {
            sink_id: sink_id.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            system_identifier: system_identifier.into(),
            database_oid,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Validates all persistent fields and the 1 MiB limit after decoding.
    ///
    /// # Errors
    ///
    /// Rejects malformed identifiers and zero cluster/database identities.
    /// Also rejects plans exceeding the 1 MiB persistent definition limit.
    pub fn validate(&self) -> Result<(), PostgresSinkError> {
        validate_names(self)?;
        validate_identity(self)?;
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

    /// Returns the stable identity of this sink instance.
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Returns the `PostgreSQL` database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the exact quoted target schema component.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the exact quoted target table component.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the `PostgreSQL` cluster system identifier as decimal text.
    #[must_use]
    pub fn system_identifier(&self) -> &str {
        &self.system_identifier
    }

    /// Returns the database OID captured during discovery.
    #[must_use]
    pub const fn database_oid(&self) -> u32 {
        self.database_oid
    }

    pub(super) fn hash_index(&self) -> String {
        format!("$dogpaddle.hash.{}", self.sink_id)
    }

    pub(super) fn frontier_table(&self) -> String {
        format!("$dogpaddle.frontier.{}", self.sink_id)
    }

    pub(super) fn frontier_pk(&self) -> String {
        format!("$dogpaddle.frontier_pk.{}", self.sink_id)
    }

    pub(super) fn object_names(&self) -> [String; 5] {
        [
            self.table.clone(),
            self.hash_index(),
            format!("$dogpaddle.pk.{}", self.sink_id),
            self.frontier_table(),
            self.frontier_pk(),
        ]
    }
}

pub(super) fn validate_names(spec: &PostgresSinkDefinition) -> Result<(), PostgresSinkError> {
    if !is_valid_sink_id(&spec.sink_id) {
        return Err(invalid_spec(
            "sink ID must contain 1–32 lowercase ASCII letters, digits, or underscores",
        ));
    }
    for (label, value) in [
        ("database", spec.database.as_str()),
        ("schema", spec.schema.as_str()),
        ("table", spec.table.as_str()),
    ] {
        if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.contains('\0') {
            return Err(invalid_spec(format!(
                "{label} must be a nonempty PostgreSQL identifier of at most 63 bytes"
            )));
        }
    }
    for name in spec.object_names() {
        if name.len() > MAX_IDENTIFIER_BYTES {
            return Err(invalid_spec("derived sink object name exceeds 63 bytes"));
        }
    }
    if spec
        .object_names()
        .into_iter()
        .skip(1)
        .any(|name| name == spec.table)
    {
        return Err(invalid_spec(
            "target table name collides with a sink-owned object",
        ));
    }
    Ok(())
}

fn validate_identity(spec: &PostgresSinkDefinition) -> Result<(), PostgresSinkError> {
    if spec
        .system_identifier
        .parse::<u64>()
        .ok()
        .is_none_or(|identifier| identifier == 0)
        || spec.database_oid == 0
    {
        return Err(invalid_spec(
            "cluster system identifier and database OID must be nonzero",
        ));
    }
    Ok(())
}

impl PostgresSinkDefinition {
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
            .expect("the final binding entrypoint enforces PostgreSQL sink input arity");
        let codec = PostgresRowCodec::try_new(Arc::clone(input_schema)).map_err(schema_error)?;
        let input_schema = Arc::clone(input_schema);
        let config = resource.take::<PostgresSinkConfig>()?;
        let target = PostgresTarget::new_bound(config, self.clone(), codec);
        buffered::construct(input_schema, target, data)
    }

    pub(crate) fn resource_type() -> TypeId {
        TypeId::of::<PostgresSinkConfig>()
    }
}
