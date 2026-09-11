mod input;
mod program;
mod protocol;
mod runtime;

pub(crate) use protocol::StationError;
pub(crate) use runtime::{Station, StationParts};

#[cfg(test)]
use input::Output;

#[cfg(test)]
mod tests;
