//! Scan operations that produce records without consuming input.

pub(crate) mod mysql_cdc;
pub(crate) mod postgres_cdc;
pub(crate) mod sequence;

pub use mysql_cdc::{
    MySqlCdcScanConfig, MySqlCdcScanDefinition, MySqlCdcScanError, MySqlCdcScanOperation,
    MySqlCdcScanOptions, MySqlCdcScanSpec, MySqlColumn, MySqlType,
};
pub use postgres_cdc::{
    PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanError,
    PostgresCdcScanOperation, PostgresCdcScanOptions, PostgresCdcScanSpec, PostgresColumn,
    PostgresType,
};
pub use sequence::{SequenceScanDefinition, SequenceScanError, SequenceScanOperation};
