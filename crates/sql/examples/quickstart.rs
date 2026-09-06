//! Run one SQL file as a bounded, reopenable `DogPaddle` Flow.
//!
//! ```text
//! cargo run -p dogpaddle-sql --example quickstart -- \
//!   <build|open> SQL_FILE STATE_PATH [rounds] [delay-ms]
//! ```

use std::{env, error::Error, path::PathBuf, thread, time::Duration};

use dogpaddle_sql::SqlProgram;

type ExampleError = Box<dyn Error>;

struct Options {
    mode: String,
    sql_path: PathBuf,
    state_path: PathBuf,
    rounds: u32,
    delay: Duration,
}

impl Options {
    fn read() -> Result<Self, ExampleError> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        if !(3..=5).contains(&arguments.len()) {
            return Err(
                "usage: quickstart <build|open> SQL_FILE STATE_PATH [rounds] [delay-ms]".into(),
            );
        }
        Ok(Self {
            mode: arguments[0].clone(),
            sql_path: arguments[1].clone().into(),
            state_path: arguments[2].clone().into(),
            rounds: arguments.get(3).map_or(Ok(12), |value| value.parse())?,
            delay: Duration::from_millis(arguments.get(4).map_or(Ok(0), |value| value.parse())?),
        })
    }
}

fn main() -> Result<(), ExampleError> {
    let options = Options::read()?;
    let program = SqlProgram::read(&options.sql_path)?;
    let mut flow = match options.mode.as_str() {
        "build" => program.build(&options.state_path)?,
        "open" => program.open(&options.state_path)?,
        _ => return Err("mode must be build or open".into()),
    };

    let station_count = flow.status()?.len();
    println!("DogPaddle SQL quickstart");
    println!("mode      {}", options.mode);
    println!("SQL       {}", options.sql_path.display());
    println!("state     {}", options.state_path.display());
    println!("stations  {station_count} persistent");
    println!();

    for round in 1..=options.rounds {
        let outcome = flow.advance()?;
        println!("advance {round:02}  {outcome:?}");
        thread::sleep(options.delay);
    }

    println!();
    println!("paused at a committed position; use open to continue");
    Ok(())
}
