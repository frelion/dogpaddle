use std::num::NonZeroU64;

use dogpaddle_flow::{FlowFactory, OperationRef};

use crate::{
    SqlError,
    endpoint::{BuiltScan, BuiltSink},
};

pub(crate) const OUTPUT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const OUTPUT_CAPACITY: NonZeroU64 =
    NonZeroU64::new(OUTPUT_CAPACITY_BYTES).expect("64 MiB is nonzero");

pub(crate) fn scan_operation_id(index: usize) -> String {
    format!("sql/scan/{index:08x}")
}

pub(crate) fn add_scan(
    factory: &mut FlowFactory,
    index: usize,
    scan: BuiltScan,
) -> Result<OperationRef, SqlError> {
    let id = scan_operation_id(index);
    Ok(match scan {
        BuiltScan::Sequence(definition) => factory.operation(id, Box::new(definition), []),
        BuiltScan::PostgresCdc(scan) => {
            factory.resource(&id, scan.config)?;
            factory.operation(id, Box::new(scan.definition), [])
        }
        BuiltScan::MySqlCdc(scan) => {
            factory.resource(&id, scan.config)?;
            factory.operation(id, Box::new(scan.definition), [])
        }
    })
}

pub(crate) fn add_sink(
    factory: &mut FlowFactory,
    input: OperationRef,
    sink: BuiltSink,
) -> Result<(), SqlError> {
    let id = "sql/sink";
    match sink {
        BuiltSink::ClickHouse { definition, config } => {
            factory.resource(id, config)?;
            factory.operation(id, Box::new(definition), [input]);
        }
        BuiltSink::Doris { definition, config } => {
            factory.resource(id, config)?;
            factory.operation(id, Box::new(definition), [input]);
        }
        BuiltSink::Postgres { definition, config } => {
            factory.resource(id, config)?;
            factory.operation(id, Box::new(definition), [input]);
        }
        BuiltSink::Sqlite(definition) => {
            factory.operation(id, Box::new(definition), [input]);
        }
        BuiltSink::Discard(definition) => {
            factory.operation(id, Box::new(definition), [input]);
        }
    }
    Ok(())
}
