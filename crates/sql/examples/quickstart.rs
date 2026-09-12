//! Embed one durable SQL program and advance it for a bounded number of rounds.
//!
//! ```text
//! cargo run -p dogpaddle-sql --example quickstart -- \
//!   SQL_FILE STATE_PATH [rounds]
//! ```

use std::{env, error::Error, path::PathBuf};

use dogpaddle_sql::SqlProgram;

type ExampleError = Box<dyn Error>;

struct Options {
    sql_path: PathBuf,
    state_path: PathBuf,
    rounds: u32,
}

impl Options {
    fn read() -> Result<Self, ExampleError> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        if !(2..=3).contains(&arguments.len()) {
            return Err("usage: quickstart SQL_FILE STATE_PATH [rounds]".into());
        }
        Ok(Self {
            sql_path: arguments[0].clone().into(),
            state_path: arguments[1].clone().into(),
            rounds: arguments.get(2).map_or(Ok(12), |value| value.parse())?,
        })
    }
}

fn main() -> Result<(), ExampleError> {
    let options = Options::read()?;
    let program = SqlProgram::read(&options.sql_path)?;
    let mut flow = program.start(&options.state_path)?;

    let station_count = flow.status()?.len();
    println!("DogPaddle SQL quickstart");
    println!("SQL       {}", options.sql_path.display());
    println!("state     {}", options.state_path.display());
    println!("stations  {station_count} persistent");
    println!();

    for round in 1..=options.rounds {
        let outcome = flow.advance()?;
        println!("advance {round:02}  {outcome:?}");
    }

    println!("paused at a committed position; start the same program to continue");
    Ok(())
}
