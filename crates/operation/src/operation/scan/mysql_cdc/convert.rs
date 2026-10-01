use super::{MySqlCdcScanError, MySqlCdcScanSpec, runtime::MySqlSource};
use crate::operation::scan::{
    cdc_convert::{self, SnapshotProgress},
    cdc_runtime::Captured,
};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;

pub(super) fn convert_values<'a>(
    spec: &MySqlCdcScanSpec,
    projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, MySqlCdcScanError> {
    Ok(cdc_convert::convert_values(
        &MySqlSource::for_test(spec, projection),
        output_schema,
        values,
        None,
    )?
    .change)
}

pub(super) fn convert_snapshot_values<'a>(
    spec: &MySqlCdcScanSpec,
    projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
    progress: SnapshotProgress,
) -> Result<Captured, MySqlCdcScanError> {
    Ok(cdc_convert::convert_values(
        &MySqlSource::for_test(spec, projection),
        output_schema,
        values,
        Some(progress),
    )?)
}
