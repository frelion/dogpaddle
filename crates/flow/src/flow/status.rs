use super::{Flow, frame::FramePhase};
use crate::error::FlowError;
/// A read-only view of the one active computation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowStatus {
    /// Whether this runtime must be reopened before advancing.
    pub needs_reopen: bool,
    /// Number of durable active frames; zero means no active root.
    pub depth: usize,
    /// Logical ID at the top of the stack.
    pub active_operation: Option<String>,
    /// Whether the top is sending its page to consumers.
    pub sending: bool,
}
impl Flow {
    /// Reads durable stack status without advancing work.
    /// # Errors
    /// Returns Store or control-codec errors.
    pub fn status(&self) -> Result<FlowStatus, FlowError> {
        let top = self.runtime.frames.top(self.reads.begin().access())?;
        let (depth, active_operation, sending) = top.map_or((0, None, false), |(depth, frame)| {
            (
                depth as usize + 1,
                self.runtime
                    .definition
                    .operations
                    .get(frame.head)
                    .map(|node| node.id.clone()),
                matches!(frame.phase, FramePhase::Send { .. }),
            )
        });
        Ok(FlowStatus {
            needs_reopen: self.runtime.needs_reopen,
            depth,
            active_operation,
            sending,
        })
    }
}
