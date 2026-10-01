//! Scan operations that produce records without consuming input.

mod cdc_convert;
mod cdc_options;
pub(crate) mod cdc_runtime;
pub(crate) mod mysql_cdc;
pub(crate) mod postgres_cdc;
pub(crate) mod sequence;

pub use cdc_options::{CdcOptions, CdcOptionsError};
pub use mysql_cdc::{
    MySqlCdcScanConfig, MySqlCdcScanDefinition, MySqlCdcScanError, MySqlCdcScanSpec,
};
pub use postgres_cdc::{
    PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanError, PostgresCdcScanSpec,
};
pub use sequence::{SequenceScanDefinition, SequenceScanError};

fn ordered_projection(projection: &[u32], source_fields: usize) -> Option<Vec<usize>> {
    let mut previous = None;
    projection
        .iter()
        .map(|&index| {
            let index = usize::try_from(index).ok()?;
            if index >= source_fields || previous.is_some_and(|previous| previous >= index) {
                return None;
            }
            previous = Some(index);
            Some(index)
        })
        .collect()
}
