#[path = "correctness/binding.rs"]
mod binding;
#[path = "correctness/definition.rs"]
mod definition;
#[path = "correctness/distinct.rs"]
mod distinct;
#[path = "correctness/lifecycle.rs"]
mod lifecycle;
#[path = "correctness/malformed.rs"]
mod malformed;
#[path = "correctness/pipeline.rs"]
mod pipeline;
#[path = "correctness/postgres_cdc_scan.rs"]
mod postgres_cdc_scan;
#[path = "correctness/postgres_sink.rs"]
mod postgres_sink;
#[path = "correctness/runtime_corruption.rs"]
mod runtime_corruption;
#[path = "correctness/sqlite_sink.rs"]
mod sqlite_sink;
#[path = "correctness/status.rs"]
mod status;
#[path = "correctness/support.rs"]
mod support;
#[path = "correctness/topology.rs"]
mod topology;
