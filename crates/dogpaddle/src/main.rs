use std::{
    env,
    error::Error,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use dogpaddle_flow::AdvanceOutcome;
use dogpaddle_sql::SqlProgram;

const USAGE: &str = "usage: dogpaddle run SQL_FILE [--state DIR]";
const IDLE_SLEEP: Duration = Duration::from_millis(25);
static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() -> ExitCode {
    let (sql_path, state_path) = match parse_args(env::args_os().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("error: {message}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let state_path = match state_path {
        Some(path) => path,
        None => match default_state_path(&sql_path) {
            Ok(path) => path,
            Err(message) => {
                eprintln!("error: {message}");
                return ExitCode::FAILURE;
            }
        },
    };
    if let Err(error) = ctrlc::set_handler(|| {
        STOP_REQUESTED.store(true, Ordering::SeqCst);
    }) {
        report_error(&error);
        return ExitCode::FAILURE;
    }

    let program = match SqlProgram::read(&sql_path) {
        Ok(program) => program,
        Err(error) => {
            report_error(&error);
            return ExitCode::FAILURE;
        }
    };
    let mut flow = match program.start(&state_path) {
        Ok(flow) => flow,
        Err(error) => {
            report_error(&error);
            return ExitCode::FAILURE;
        }
    };
    println!("state: {}", flow.path().display());

    while !STOP_REQUESTED.load(Ordering::SeqCst) {
        match flow.advance() {
            Ok(AdvanceOutcome::Progressed) => {}
            Ok(AdvanceOutcome::Idle | AdvanceOutcome::Backpressured) => {
                thread::sleep(IDLE_SLEEP);
            }
            Err(error) => {
                report_error(&error);
                if error.requires_reopen() {
                    eprintln!("help: rerun the same command to reopen durable state");
                }
                return ExitCode::FAILURE;
            }
        }
    }

    ExitCode::SUCCESS
}

fn parse_args(
    mut args: impl Iterator<Item = OsString>,
) -> Result<(PathBuf, Option<PathBuf>), &'static str> {
    if args.next().as_deref() != Some(OsStr::new("run")) {
        return Err("expected the run command");
    }
    let sql_path = args.next().ok_or("missing SQL_FILE")?;
    if sql_path.is_empty() {
        return Err("SQL_FILE cannot be empty");
    }

    let state_path = match args.next() {
        None => None,
        Some(option) if option == OsStr::new("--state") => {
            let path = args.next().ok_or("missing DIR after --state")?;
            if path.is_empty() {
                return Err("DIR after --state cannot be empty");
            }
            Some(PathBuf::from(path))
        }
        Some(_) => return Err("unexpected argument"),
    };
    if args.next().is_some() {
        return Err("unexpected argument");
    }

    Ok((PathBuf::from(sql_path), state_path))
}

fn default_state_path(sql_path: &Path) -> Result<PathBuf, &'static str> {
    let stem = sql_path
        .file_stem()
        .filter(|stem| !stem.is_empty())
        .ok_or("SQL_FILE must have a file name")?;
    let parent = sql_path.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.join(".dogpaddle").join(stem))
}

fn report_error(error: &dyn Error) {
    eprintln!("error: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}
