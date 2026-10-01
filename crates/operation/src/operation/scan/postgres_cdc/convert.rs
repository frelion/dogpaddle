use super::{PostgresCdcScanError, PostgresCdcScanSpec, runtime::PostgresSource};
use crate::operation::scan::{
    cdc_convert::{self, SnapshotProgress},
    cdc_runtime::Captured,
};
use arrow_schema::SchemaRef;
use dogpaddle_change::Change;

pub(super) fn convert_values<'a>(
    spec: &PostgresCdcScanSpec,
    projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
) -> Result<Option<Change>, PostgresCdcScanError> {
    Ok(cdc_convert::convert_values(
        &PostgresSource::for_test(spec, projection),
        output_schema,
        values,
        None,
    )?
    .change)
}

pub(super) fn convert_capture_values<'a>(
    spec: &PostgresCdcScanSpec,
    projection: &[u32],
    output_schema: SchemaRef,
    values: impl IntoIterator<Item = (Option<&'a str>, Option<&'a [u8]>)>,
    progress: SnapshotProgress,
) -> Result<Captured, PostgresCdcScanError> {
    Ok(cdc_convert::convert_values(
        &PostgresSource::for_test(spec, projection),
        output_schema,
        values,
        Some(progress),
    )?)
}
