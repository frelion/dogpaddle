use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use serde_json::Value;

use super::{PostgresCdcScanError, PostgresCdcScanSpec};
use crate::operation::scan::{
    cdc_convert::{
        Row, build_change, complete_row, validate_envelope, validate_heartbeat,
        validate_snapshot_notification,
    },
    cdc_runtime::Captured,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CaptureProgress {
    saw_snapshot_row: bool,
    snapshot_complete: bool,
}

enum ConversionMode {
    Capture {
        progress: CaptureProgress,
        sealed: bool,
    },
    Streaming,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SnapshotMarker {
    Snapshot,
    Last,
    Streaming,
}

// The byte-level boundary also lets tests use actual Connect JSON without
// exposing constructors for the runtime's owned Record capability.
pub(super) fn convert_values<'a>(
    spec: &PostgresCdcScanSpec,
    output_projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, PostgresCdcScanError> {
    Ok(convert_values_in_mode(
        spec,
        output_projection,
        output_schema,
        values,
        ConversionMode::Streaming,
    )?
    .change)
}

pub(super) fn convert_capture_values<'a>(
    spec: &PostgresCdcScanSpec,
    output_projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
    progress: CaptureProgress,
) -> Result<Captured<CaptureProgress>, PostgresCdcScanError> {
    convert_values_in_mode(
        spec,
        output_projection,
        output_schema,
        values,
        ConversionMode::Capture {
            progress,
            sealed: false,
        },
    )
}

fn convert_values_in_mode<'a>(
    spec: &PostgresCdcScanSpec,
    output_projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
    mut mode: ConversionMode,
) -> Result<Captured<CaptureProgress>, PostgresCdcScanError> {
    let columns = &spec.columns;
    let table_topic = format!("{}.{}.{}", spec.engine_name, spec.schema, spec.table);
    let heartbeat_topic = format!("__debezium-heartbeat.{}", spec.engine_name);
    let notification_topic = format!("__dogpaddle-notification.{}", spec.engine_name);
    let mut rows = Vec::new();
    let mut diffs = Vec::new();
    for (topic, bytes) in values {
        let is_notification = topic == Some(notification_topic.as_str());
        if topic != Some(table_topic.as_str())
            && topic != Some(heartbeat_topic.as_str())
            && !(is_notification && matches!(mode, ConversionMode::Capture { .. }))
        {
            return Err(invalid("record has an unexpected topic"));
        }
        let Some(bytes) = bytes else {
            return Err(invalid("CDC records cannot be tombstones"));
        };
        let mut value: Value = serde_json::from_slice(bytes)
            .map_err(|_| invalid("record is not valid schemas-enabled Connect JSON"))?;
        let schema = object_field(&value, "schema")?;
        if is_notification {
            apply_snapshot_notification(object_field(&value, "payload")?, &mut mode)?;
            continue;
        }
        if topic == Some(heartbeat_topic.as_str()) {
            validate_heartbeat(schema, object_field(&value, "payload")?)?;
            continue;
        }
        validate_envelope(columns, schema)?;
        let payload = value
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("missing object field payload"))?;
        let marker = validate_metadata(payload, &spec.schema, &spec.table)?;
        let before = payload
            .remove("before")
            .ok_or_else(|| invalid("missing before"))?;
        let after = payload
            .remove("after")
            .ok_or_else(|| invalid("missing after"))?;
        let operation = payload.get("op").and_then(Value::as_str);
        let accepts_streaming = match &mode {
            ConversionMode::Streaming => true,
            ConversionMode::Capture { progress, .. } => progress.snapshot_complete,
        };
        match (operation, marker) {
            (Some("r"), SnapshotMarker::Snapshot | SnapshotMarker::Last) if before.is_null() => {
                let ConversionMode::Capture { progress, sealed } = &mut mode else {
                    return Err(invalid("snapshot record arrived while streaming"));
                };
                if *sealed || progress.snapshot_complete {
                    return Err(invalid("snapshot record arrived after snapshot completion"));
                }
                rows.push(complete_row(columns, output_projection, after)?);
                diffs.push(1);
                progress.saw_snapshot_row = true;
                if marker == SnapshotMarker::Last {
                    progress.snapshot_complete = true;
                }
            }
            (Some("c"), SnapshotMarker::Streaming) if accepts_streaming && before.is_null() => {
                rows.push(complete_row(columns, output_projection, after)?);
                diffs.push(1);
            }
            (Some("u"), SnapshotMarker::Streaming) if accepts_streaming => {
                rows.push(complete_row(columns, output_projection, before)?);
                rows.push(complete_row(columns, output_projection, after)?);
                diffs.extend([-1, 1]);
            }
            (Some("d"), SnapshotMarker::Streaming) if accepts_streaming && after.is_null() => {
                rows.push(complete_row(columns, output_projection, before)?);
                diffs.push(-1);
            }
            _ => {
                return Err(invalid(
                    "record operation or snapshot marker is invalid for the current CDC phase",
                ));
            }
        }
    }
    let change = build_change(columns, output_projection, output_schema, &rows, diffs)?;
    let (progress, sealed) = match mode {
        ConversionMode::Capture { progress, sealed } => (progress, sealed),
        ConversionMode::Streaming => (CaptureProgress::default(), false),
    };
    Ok(Captured {
        change,
        sealed,
        progress,
    })
}

fn apply_snapshot_notification(
    payload: &Row,
    mode: &mut ConversionMode,
) -> Result<(), PostgresCdcScanError> {
    if !validate_snapshot_notification(payload)? {
        return Ok(());
    }
    let ConversionMode::Capture { progress, sealed } = mode else {
        unreachable!("streaming notifications were rejected before parsing");
    };
    if *sealed {
        return Err(invalid("snapshot completion notification is duplicated"));
    }
    if progress.saw_snapshot_row && !progress.snapshot_complete {
        return Err(invalid(
            "snapshot completed before Debezium marked the last snapshot row",
        ));
    }
    progress.snapshot_complete = true;
    *sealed = true;
    Ok(())
}

fn object_field<'a>(value: &'a Value, field: &str) -> Result<&'a Row, PostgresCdcScanError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("missing object field {field}")))
}

fn validate_metadata(
    payload: &Row,
    table_schema: &str,
    table: &str,
) -> Result<SnapshotMarker, PostgresCdcScanError> {
    let metadata = payload
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("missing Debezium CDC metadata"))?;
    for (field, expected) in [
        ("schema", table_schema),
        ("table", table),
        ("connector", "postgresql"),
    ] {
        if metadata.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(invalid(format!(
                "Debezium CDC metadata does not match configured {field}"
            )));
        }
    }
    match metadata.get("snapshot") {
        Some(Value::String(value)) if matches!(value.as_str(), "true" | "first") => {
            Ok(SnapshotMarker::Snapshot)
        }
        Some(Value::String(value)) if value == "last" => Ok(SnapshotMarker::Last),
        Some(Value::Null) => Ok(SnapshotMarker::Streaming),
        _ => Err(invalid(
            "Debezium CDC metadata has an invalid snapshot marker",
        )),
    }
}

fn invalid(message: impl Into<String>) -> PostgresCdcScanError {
    PostgresCdcScanError::InvalidRecord(message.into())
}
