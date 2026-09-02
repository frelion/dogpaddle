use std::{
    env,
    ffi::OsStr,
    fs,
    path::Path,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Serialize, Serializer};

/// Captured output from an optional host command or platform probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandOutput {
    /// The command completed successfully and produced valid Unicode.
    Available(String),
    /// The command could not produce usable output.
    Unavailable(String),
}

impl CommandOutput {
    /// Executes a command and captures its trimmed standard output.
    ///
    /// Spawn failures, unsuccessful exit statuses, and non-Unicode output are
    /// represented as [`Self::Unavailable`]; benchmark setup does not fail merely
    /// because an informational host probe is unavailable.
    #[must_use]
    pub fn capture(program: impl AsRef<OsStr>, arguments: &[&str]) -> Self {
        let program = program.as_ref();
        match Command::new(program).args(arguments).output() {
            Err(error) => Self::Unavailable(format!("cannot execute command: {error}")),
            Ok(output) if !output.status.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
                let reason = if stderr.is_empty() {
                    format!("command exited with {}", output.status)
                } else {
                    stderr
                };
                Self::Unavailable(reason)
            }
            Ok(output) => match String::from_utf8(output.stdout) {
                Ok(stdout) => Self::Available(stdout.trim().to_owned()),
                Err(error) => Self::Unavailable(format!("command output is not Unicode: {error}")),
            },
        }
    }

    /// Returns whether this probe produced a usable value.
    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::Available(_))
    }

    fn non_empty(self, label: &'static str) -> Self {
        match self {
            Self::Available(value) if value.is_empty() => {
                Self::Unavailable(format!("{label} produced no output"))
            }
            output => output,
        }
    }

    fn rendered(&self) -> String {
        match self {
            Self::Available(value) => value.clone(),
            Self::Unavailable(reason) => format!("unavailable: {reason}"),
        }
    }
}

impl Serialize for CommandOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.rendered())
    }
}

/// State of the current git working tree at environment collection time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GitState {
    /// `git status --porcelain` succeeded and emitted no changes.
    Clean,
    /// `git status --porcelain` succeeded and emitted at least one change.
    Dirty,
    /// The git status probe was unavailable.
    Unavailable,
}

/// Reproducibility metadata shared by all benchmark environment records.
#[derive(Clone, Debug, Serialize)]
pub struct HostEnvironment {
    cargo_profile: &'static str,
    cargo_profile_source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    filesystem_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filesystem: Option<CommandOutput>,
    os: &'static str,
    arch: &'static str,
    kernel: CommandOutput,
    cpu: CommandOutput,
    parallelism: usize,
    rustc: CommandOutput,
    git_revision: CommandOutput,
    git_state: GitState,
    debug_assertions: bool,
    unix_seconds: u64,
}

impl HostEnvironment {
    /// Collects host metadata, optionally describing the filesystem containing
    /// `filesystem_path`.
    ///
    /// Informational command failures are retained as `unavailable: ...` values.
    ///
    /// # Panics
    ///
    /// Panics when the system clock precedes the Unix epoch.
    #[must_use]
    #[track_caller]
    pub fn collect(filesystem_path: Option<&Path>) -> Self {
        Self::collect_with(filesystem_path, SystemTime::now())
    }

    #[track_caller]
    fn collect_with(filesystem_path: Option<&Path>, now: SystemTime) -> Self {
        let rustc_program = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let rustc = CommandOutput::capture(&rustc_program, &["--version"]).non_empty("rustc");
        let git_revision =
            CommandOutput::capture("git", &["rev-parse", "HEAD"]).non_empty("git revision");
        let git_status = CommandOutput::capture("git", &["status", "--porcelain"]);
        let git_state = match git_status {
            CommandOutput::Available(ref value) if value.is_empty() => GitState::Clean,
            CommandOutput::Available(_) => GitState::Dirty,
            CommandOutput::Unavailable(_) => GitState::Unavailable,
        };
        let filesystem = filesystem_path.map(filesystem_description);
        let filesystem_path = filesystem_path.map(|path| path.display().to_string());
        let unix_seconds = now.duration_since(UNIX_EPOCH).unwrap_or_else(|source| {
            panic!(
                "benchmark environment failure: stage=unix_seconds label=system_time value={now:?} source={source}"
            )
        });
        Self {
            cargo_profile: "bench",
            cargo_profile_source: "default",
            filesystem_path,
            filesystem,
            os: env::consts::OS,
            arch: env::consts::ARCH,
            kernel: CommandOutput::capture("uname", &["-a"]).non_empty("kernel probe"),
            cpu: cpu_description(),
            parallelism: std::thread::available_parallelism().map_or(0, usize::from),
            rustc,
            git_revision,
            git_state,
            debug_assertions: cfg!(debug_assertions),
            unix_seconds: unix_seconds.as_secs(),
        }
    }
}

fn cpu_description() -> CommandOutput {
    if env::consts::OS == "macos" {
        let output = CommandOutput::capture("sysctl", &["-n", "machdep.cpu.brand_string"])
            .non_empty("CPU probe");
        if output.is_available() {
            return output;
        }
    }
    if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo")
        && let Some(description) = cpuinfo.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            matches!(key.trim(), "model name" | "Hardware")
                .then(|| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
    {
        return CommandOutput::Available(description);
    }
    CommandOutput::Unavailable("CPU description is not available".to_owned())
}

fn filesystem_description(path: &Path) -> CommandOutput {
    let path = path.as_os_str();
    if env::consts::OS == "macos" {
        let usage = capture_path_command("df", &[OsStr::new("-P"), path]);
        let mounts = CommandOutput::capture("mount", &[]);
        return match (usage, mounts) {
            (CommandOutput::Available(usage), CommandOutput::Available(mounts)) => {
                macos_filesystem_description(&usage, &mounts).map_or_else(
                    || {
                        CommandOutput::Unavailable(
                            "cannot match df device to a mounted filesystem".to_owned(),
                        )
                    },
                    CommandOutput::Available,
                )
            }
            (unavailable @ CommandOutput::Unavailable(_), _)
            | (_, unavailable @ CommandOutput::Unavailable(_)) => unavailable,
        };
    }
    let kind = match env::consts::OS {
        "linux" => capture_path_command(
            "stat",
            &[OsStr::new("-f"), OsStr::new("-c"), OsStr::new("%T"), path],
        ),
        _ => CommandOutput::Unavailable("filesystem type probe is unsupported".to_owned()),
    }
    .non_empty("filesystem type probe");
    if kind.is_available() {
        return kind;
    }

    let fallback = capture_path_command("df", &[OsStr::new("-P"), path]);
    match fallback {
        CommandOutput::Available(output) => output
            .lines()
            .last()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map_or_else(
                || CommandOutput::Unavailable("df produced no filesystem row".to_owned()),
                |line| CommandOutput::Available(line.to_owned()),
            ),
        unavailable @ CommandOutput::Unavailable(_) => unavailable,
    }
}

pub(crate) fn macos_filesystem_description(usage: &str, mounts: &str) -> Option<String> {
    let device = usage.lines().last()?.split_whitespace().next()?;
    let mount = mounts
        .lines()
        .find(|line| line.starts_with(&format!("{device} on ")))?;
    let options = mount.split_once(" (")?.1;
    let kind = options.split([',', ')']).next()?.trim();
    (!kind.is_empty()).then(|| format!("{kind} ({device})"))
}

fn capture_path_command(program: &str, arguments: &[&OsStr]) -> CommandOutput {
    match Command::new(program).args(arguments).output() {
        Err(error) => CommandOutput::Unavailable(format!("cannot execute command: {error}")),
        Ok(output) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            CommandOutput::Unavailable(if stderr.is_empty() {
                format!("command exited with {}", output.status)
            } else {
                stderr
            })
        }
        Ok(output) => String::from_utf8(output.stdout).map_or_else(
            |error| CommandOutput::Unavailable(format!("command output is not Unicode: {error}")),
            |stdout| CommandOutput::Available(stdout.trim().to_owned()),
        ),
    }
}
