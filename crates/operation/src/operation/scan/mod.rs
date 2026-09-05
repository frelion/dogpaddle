//! Scan operations that produce records without consuming input.

pub(crate) mod postgres_cdc;
pub(crate) mod sequence;

pub use postgres_cdc::{
    PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanError,
    PostgresCdcScanOperation, PostgresCdcScanSpec, PostgresColumn, PostgresType,
};
pub use sequence::{SequenceScanDefinition, SequenceScanError, SequenceScanOperation};
