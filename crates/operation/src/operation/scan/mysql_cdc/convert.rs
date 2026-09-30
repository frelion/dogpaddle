use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use serde_json::Value;

use super::{MySqlCdcScanError, MySqlCdcScanSpec, MySqlColumn};
use crate::operation::scan::{
    cdc_convert::{
        Row, build_change, complete_row, validate_envelope, validate_heartbeat,
        validate_snapshot_notification,
    },
    cdc_runtime::Captured,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SnapshotProgress {
    saw_snapshot_row: bool,
    saw_last: bool,
}

// The byte-level boundary also lets tests use actual Connect JSON without
// exposing constructors for the runtime's owned Record capability.
pub(super) fn convert_snapshot_values<'a>(
    spec: &MySqlCdcScanSpec,
    output_projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
    progress: SnapshotProgress,
) -> Result<Captured<SnapshotProgress>, MySqlCdcScanError> {
    let columns = &spec.columns;
    let table_topic = format!("{}.{}.{}", spec.engine_name, spec.database, spec.table);
    let heartbeat_topic = format!("__debezium-heartbeat.{}", spec.engine_name);
    let notification_topic = format!("__dogpaddle-notification.{}", spec.engine_name);
    let mut rows = Vec::new();
    let mut progress = progress;
    let mut complete = false;
    for (topic, bytes) in values {
        let bytes = bytes.ok_or_else(|| invalid("snapshot record cannot be a tombstone"))?;
        let mut value: Value = serde_json::from_slice(bytes)
            .map_err(|_| invalid("record is not valid schemas-enabled Connect JSON"))?;
        let schema = object_field(&value, "schema")?;
        if topic == Some(notification_topic.as_str()) {
            if validate_snapshot_notification::<MySqlColumn>(object_field(&value, "payload")?)? {
                if complete {
                    return Err(invalid("snapshot completion notification is duplicated"));
                }
                if progress.saw_snapshot_row && !progress.saw_last {
                    return Err(invalid(
                        "snapshot completed before Debezium marked the last snapshot row",
                    ));
                }
                complete = true;
            }
            continue;
        }
        if topic == Some(heartbeat_topic.as_str()) {
            validate_heartbeat::<MySqlColumn>(schema, object_field(&value, "payload")?)?;
            continue;
        }
        if topic != Some(table_topic.as_str()) {
            return Err(invalid("snapshot record has an unexpected topic"));
        }
        if complete || progress.saw_last {
            return Err(invalid("snapshot record arrived after snapshot completion"));
        }
        validate_envelope(columns, schema)?;
        let payload = value
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("missing object field payload"))?;
        let last = validate_snapshot_metadata(payload, &spec.database, &spec.table)?;
        let before = payload
            .remove("before")
            .ok_or_else(|| invalid("missing before"))?;
        let after = payload
            .remove("after")
            .ok_or_else(|| invalid("missing after"))?;
        if payload.get("op").and_then(Value::as_str) != Some("r") || !before.is_null() {
            return Err(invalid("expected one initial snapshot read event"));
        }
        rows.push(complete_row(columns, output_projection, after)?);
        progress.saw_snapshot_row = true;
        if last {
            progress.saw_last = true;
        }
    }
    let diffs = vec![1; rows.len()];
    Ok(Captured {
        change: build_change(columns, output_projection, output_schema, &rows, diffs)?,
        sealed: complete,
        progress,
    })
}

// The byte-level boundary also lets tests use actual Connect JSON without
// exposing constructors for the runtime's owned Record capability.
pub(super) fn convert_values<'a>(
    spec: &MySqlCdcScanSpec,
    output_projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, MySqlCdcScanError> {
    let columns = &spec.columns;
    let table_topic = format!("{}.{}.{}", spec.engine_name, spec.database, spec.table);
    let heartbeat_topic = format!("__debezium-heartbeat.{}", spec.engine_name);
    let mut rows = Vec::new();
    let mut diffs = Vec::new();
    for (topic, bytes) in values {
        if topic != Some(table_topic.as_str()) && topic != Some(heartbeat_topic.as_str()) {
            return Err(invalid("record has an unexpected topic"));
        }
        let Some(bytes) = bytes else {
            if topic == Some(table_topic.as_str()) {
                continue;
            }
            return Err(invalid("heartbeat cannot be a tombstone"));
        };
        let mut value: Value = serde_json::from_slice(bytes)
            .map_err(|_| invalid("record is not valid schemas-enabled Connect JSON"))?;
        let schema = object_field(&value, "schema")?;
        if topic == Some(heartbeat_topic.as_str()) {
            validate_heartbeat::<MySqlColumn>(schema, object_field(&value, "payload")?)?;
            continue;
        }
        validate_envelope(columns, schema)?;
        let payload = value
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("missing object field payload"))?;
        validate_metadata(payload, &spec.database, &spec.table)?;
        let before = payload
            .remove("before")
            .ok_or_else(|| invalid("missing before"))?;
        let after = payload
            .remove("after")
            .ok_or_else(|| invalid("missing after"))?;
        match payload.get("op").and_then(Value::as_str) {
            Some("c") if before.is_null() => {
                rows.push(complete_row(columns, output_projection, after)?);
                diffs.push(1);
            }
            Some("u") => {
                rows.push(complete_row(columns, output_projection, before)?);
                rows.push(complete_row(columns, output_projection, after)?);
                diffs.extend([-1, 1]);
            }
            Some("d") if after.is_null() => {
                rows.push(complete_row(columns, output_projection, before)?);
                diffs.push(-1);
            }
            _ => return Err(invalid("expected a streaming insert, update, or delete")),
        }
    }
    build_change(columns, output_projection, output_schema, &rows, diffs)
}

fn object_field<'a>(value: &'a Value, field: &str) -> Result<&'a Row, MySqlCdcScanError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("missing object field {field}")))
}

fn validate_snapshot_metadata(
    payload: &Row,
    database: &str,
    table: &str,
) -> Result<bool, MySqlCdcScanError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium snapshot metadata"))?;
    for (field, expected) in [("db", database), ("table", table), ("connector", "mysql")] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium snapshot metadata does not match configured {field}"
            )));
        }
    }
    match metadata.get("snapshot").and_then(Value::as_str) {
        Some("true" | "first" | "first_in_data_collection") => Ok(false),
        Some("last" | "last_in_data_collection") => Ok(true),
        _ => Err(invalid(
            "Debezium record is not part of the initial snapshot",
        )),
    }
}

fn validate_metadata(payload: &Row, database: &str, table: &str) -> Result<(), MySqlCdcScanError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium CDC metadata"))?;
    for (field, expected) in [("db", database), ("table", table), ("connector", "mysql")] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium CDC metadata does not match configured {field}"
            )));
        }
    }
    // SnapshotRecord.FALSE deliberately leaves this Struct field unset in
    // some Debezium paths. Snapshot operations are independently rejected by
    // the operation guard below.
    if !matches!(
        metadata.get("snapshot"),
        Some(Value::Null | Value::Bool(false))
    ) && metadata.get("snapshot").and_then(Value::as_str) != Some("false")
    {
        return Err(invalid(
            "Debezium CDC metadata does not identify a non-snapshot record",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> MySqlCdcScanError {
    MySqlCdcScanError::InvalidRecord(message.into())
}
