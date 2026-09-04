use std::env;
use std::error::Error;
use std::path::PathBuf;

use dogpaddle_debezium::DebeziumRuntime;

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let bundle = match (arguments.next(), arguments.next()) {
        (Some(bundle), None) => PathBuf::from(bundle),
        _ => return Err("usage: bundled_runtime_probe BUNDLE_ROOT".into()),
    };
    DebeziumRuntime::open(&bundle)?;
    println!("PASS bundled Debezium runtime: {}", bundle.display());
    Ok(())
}
