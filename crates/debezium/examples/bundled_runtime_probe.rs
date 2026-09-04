use std::env;
use std::error::Error;
use std::path::PathBuf;

use dogpaddle_debezium::DebeziumRuntime;

fn main() -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let bundle = executable
        .parent()
        .and_then(|bin| bin.parent())
        .map(PathBuf::from)
        .ok_or("the probe executable is not inside a bundle bin directory")?;
    DebeziumRuntime::open(&bundle)?;
    println!("PASS bundled Debezium runtime: {}", bundle.display());
    Ok(())
}
