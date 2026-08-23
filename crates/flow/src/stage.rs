mod control;

use dogpaddle_store::{Cell, DataHandle, OrderedMap, Transaction, Transactions};

use crate::{Decision, Event, FlowError, Operation, Work};
pub(crate) use control::Lifecycle;
use control::{Active, ActiveKind, Control, load as load_control};

const MAX_CHECKPOINT_BYTES: usize = Decision::MAX_CHECKPOINT_BYTES;
const MAX_FAILURE_BYTES: usize = 64 * 1024;

/// An opaque stage handle issued by one [`Flow`](crate::Flow).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Stage {
    pub(crate) flow: u64,
    pub(crate) index: usize,
}

pub(crate) struct StageNode {
    pub(crate) name: String,
    pub(crate) operation: Box<dyn Operation>,
    pub(crate) inputs: Vec<InputBinding>,
    pub(crate) consumers: Vec<(usize, u32)>,
    control: Option<Cell<Control>>,
    output: Option<OrderedMap<u64, Vec<u8>>>,
    pub(crate) lifecycle: Lifecycle,
}

#[derive(Clone)]
pub(crate) struct InputBinding {
    pub(crate) name: String,
    pub(crate) upstream: Option<usize>,
}

#[derive(Clone)]
pub(crate) struct StageView {
    name: String,
    control: Cell<Control>,
    output: OrderedMap<u64, Vec<u8>>,
    consumers: Vec<(usize, u32)>,
}

pub(crate) enum Poll {
    Idle,
    Progress,
    Finished,
}

enum Attempt {
    Idle,
    Progress,
    Finished,
    Failed(String),
}

struct SelectedWork {
    active: Active,
    bytes: Option<Vec<u8>>,
}

impl StageNode {
    pub(crate) fn new(
        name: String,
        operation: Box<dyn Operation>,
        inputs: Vec<InputBinding>,
    ) -> Self {
        Self {
            name,
            operation,
            inputs,
            consumers: Vec::new(),
            control: None,
            output: None,
            lifecycle: Lifecycle::Running,
        }
    }

    pub(crate) fn bind(&mut self, control: DataHandle, output: DataHandle) {
        self.control = Some(Cell::new(control));
        self.output = Some(OrderedMap::new(output));
    }

    pub(crate) fn initialize(&self, transaction: &Transaction<'_>) -> Result<(), FlowError> {
        let mut control = self.control().access(transaction)?;
        if control.get()?.is_some() {
            return Err(FlowError::DeclarationMismatch);
        }
        control.set(&Control::new(self.inputs.len()))?;
        Ok(())
    }

    pub(crate) fn restore(&mut self, transaction: &Transaction<'_>) -> Result<(), FlowError> {
        let control = load_control(&self.name, &self.control().access(transaction)?)?;
        if control.input_count() != self.inputs.len() {
            return Err(FlowError::DeclarationMismatch);
        }
        self.lifecycle = control.lifecycle();
        Ok(())
    }

    pub(crate) fn poll(
        &mut self,
        index: usize,
        views: &[StageView],
        schedule: &Cell<u64>,
        next_stage: u64,
        transactions: &mut Transactions,
    ) -> Result<Poll, FlowError> {
        match self.lifecycle {
            Lifecycle::Finished => return Ok(Poll::Finished),
            Lifecycle::Failed => return Err(self.load_failure(transactions)?),
            Lifecycle::Running => {}
        }
        match self.attempt(index, views, schedule, next_stage, transactions)? {
            Attempt::Idle => Ok(Poll::Idle),
            Attempt::Progress => Ok(Poll::Progress),
            Attempt::Finished => Ok(Poll::Finished),
            Attempt::Failed(message) => Err(self.record_failure(transactions, message)?),
        }
    }

    fn attempt(
        &mut self,
        index: usize,
        views: &[StageView],
        schedule: &Cell<u64>,
        next_stage: u64,
        transactions: &mut Transactions,
    ) -> Result<Attempt, FlowError> {
        let transaction = transactions.begin()?;
        let mut control_access = self.control().access(&transaction)?;
        let mut control = load_control(&self.name, &control_access)?;
        if self.backpressured(index, views, &control, &transaction)? {
            return Ok(Attempt::Idle);
        }
        let Some(selected) = self.select_work(&control, views, &transaction)? else {
            return Ok(Attempt::Idle);
        };
        let active = selected.active;
        let port = match active.kind {
            ActiveKind::Start => None,
            ActiveKind::Data | ActiveKind::End => self
                .inputs
                .get(active.port as usize)
                .map(|input| input.name.as_str()),
        };
        let event = match active.kind {
            ActiveKind::Start => Event::Start,
            ActiveKind::Data => Event::Data {
                port: port.ok_or_else(|| self.corrupt())?,
                position: active.position,
                bytes: selected.bytes.as_deref().ok_or_else(|| self.corrupt())?,
            },
            ActiveKind::End => Event::End {
                port: port.ok_or_else(|| self.corrupt())?,
            },
        };
        let decision = match self
            .operation
            .step(Work::new(event, active.checkpoint.as_deref()), &transaction)
        {
            Ok(decision) => decision,
            Err(error) => return Ok(Attempt::Failed(error.to_string())),
        };
        if let Some(message) = validate_decision(&decision, views[index].consumers.is_empty()) {
            return Ok(Attempt::Failed(message));
        }

        match decision {
            Decision::Pending => Ok(Attempt::Idle),
            Decision::Checkpoint { checkpoint } => {
                control.active = Some(Active {
                    checkpoint: Some(checkpoint),
                    ..active
                });
                schedule.access(&transaction)?.set(&next_stage)?;
                control_access.set(&control)?;
                transaction.commit()?;
                Ok(Attempt::Progress)
            }
            Decision::Publish { output, checkpoint } => {
                let next = control
                    .output_tail
                    .checked_add(1)
                    .ok_or_else(|| self.corrupt())?;
                let mut output_log = self.output().access(&transaction)?;
                if output_log.get(&control.output_tail)?.is_some() {
                    return Err(self.corrupt());
                }
                output_log.put(&control.output_tail, &output)?;
                control.output_tail = next;
                control.active = Some(Active {
                    checkpoint: Some(checkpoint),
                    ..active
                });
                schedule.access(&transaction)?.set(&next_stage)?;
                control_access.set(&control)?;
                transaction.commit()?;
                Ok(Attempt::Progress)
            }
            Decision::Complete { output } => {
                if let Some(output) = output {
                    let next = control
                        .output_tail
                        .checked_add(1)
                        .ok_or_else(|| self.corrupt())?;
                    let mut output_log = self.output().access(&transaction)?;
                    if output_log.get(&control.output_tail)?.is_some() {
                        return Err(self.corrupt());
                    }
                    output_log.put(&control.output_tail, &output)?;
                    control.output_tail = next;
                }
                control.active = None;
                let finished =
                    self.complete_work(index, &mut control, views, &transaction, &active)?;
                if finished {
                    control.lifecycle = Lifecycle::Finished;
                }
                schedule.access(&transaction)?.set(&next_stage)?;
                control_access.set(&control)?;
                transaction.commit()?;
                if finished {
                    self.lifecycle = Lifecycle::Finished;
                    Ok(Attempt::Finished)
                } else {
                    Ok(Attempt::Progress)
                }
            }
        }
    }

    fn backpressured(
        &self,
        index: usize,
        views: &[StageView],
        control: &Control,
        transaction: &Transaction<'_>,
    ) -> Result<bool, FlowError> {
        for (consumer_index, port) in &views[index].consumers {
            let consumer = views.get(*consumer_index).ok_or_else(|| self.corrupt())?;
            let cursor = load_control(&consumer.name, &consumer.control.access(transaction)?)?
                .cursor(&consumer.name, *port)?;
            if cursor.position > control.output_tail {
                return Err(self.corrupt());
            }
            if control.output_tail - cursor.position > 1 {
                return Err(self.corrupt());
            }
            if cursor.ended && cursor.position < control.output_tail {
                return Err(self.corrupt());
            }
            if cursor.position < control.output_tail {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn select_work(
        &self,
        control: &Control,
        views: &[StageView],
        transaction: &Transaction<'_>,
    ) -> Result<Option<SelectedWork>, FlowError> {
        if let Some(active) = &control.active {
            return Ok(Some(SelectedWork {
                bytes: self.load_active_input(control, active, views, transaction)?,
                active: active.clone(),
            }));
        }
        if self.inputs.is_empty() {
            return Ok(Some(SelectedWork {
                active: Active {
                    kind: ActiveKind::Start,
                    port: u32::MAX,
                    position: 0,
                    checkpoint: None,
                },
                bytes: None,
            }));
        }

        let start = usize::try_from(control.next_input).map_err(|_| self.corrupt())?;
        if start >= self.inputs.len() {
            return Err(self.corrupt());
        }
        for offset in 0..self.inputs.len() {
            let index = (start + offset) % self.inputs.len();
            let port = u32::try_from(index).map_err(|_| self.corrupt())?;
            let cursor = control.cursor(&self.name, port)?;
            if cursor.ended {
                continue;
            }
            let upstream = self.upstream(index, views)?;
            let upstream_control =
                load_control(&upstream.name, &upstream.control.access(transaction)?)?;
            match cursor.position.cmp(&upstream_control.output_tail) {
                std::cmp::Ordering::Less => {
                    let bytes = upstream
                        .output
                        .access(transaction)?
                        .get(&cursor.position)?
                        .ok_or_else(|| self.corrupt())?;
                    return Ok(Some(SelectedWork {
                        active: Active {
                            kind: ActiveKind::Data,
                            port,
                            position: cursor.position,
                            checkpoint: None,
                        },
                        bytes: Some(bytes),
                    }));
                }
                std::cmp::Ordering::Equal if upstream_control.lifecycle == Lifecycle::Finished => {
                    return Ok(Some(SelectedWork {
                        active: Active {
                            kind: ActiveKind::End,
                            port,
                            position: cursor.position,
                            checkpoint: None,
                        },
                        bytes: None,
                    }));
                }
                std::cmp::Ordering::Equal if upstream_control.lifecycle == Lifecycle::Running => {}
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    return Err(self.corrupt());
                }
            }
        }
        Ok(None)
    }

    fn load_active_input(
        &self,
        control: &Control,
        active: &Active,
        views: &[StageView],
        transaction: &Transaction<'_>,
    ) -> Result<Option<Vec<u8>>, FlowError> {
        match active.kind {
            ActiveKind::Start if self.inputs.is_empty() => Ok(None),
            ActiveKind::Data | ActiveKind::End if (active.port as usize) < self.inputs.len() => {
                let cursor = control.cursor(&self.name, active.port)?;
                if cursor.ended || cursor.position != active.position {
                    return Err(self.corrupt());
                }
                let upstream = self.upstream(active.port as usize, views)?;
                let upstream_control =
                    load_control(&upstream.name, &upstream.control.access(transaction)?)?;
                match active.kind {
                    ActiveKind::Data if active.position < upstream_control.output_tail => upstream
                        .output
                        .access(transaction)?
                        .get(&active.position)?
                        .map(Some)
                        .ok_or_else(|| self.corrupt()),
                    ActiveKind::End
                        if active.position == upstream_control.output_tail
                            && upstream_control.lifecycle == Lifecycle::Finished =>
                    {
                        Ok(None)
                    }
                    ActiveKind::Start | ActiveKind::Data | ActiveKind::End => Err(self.corrupt()),
                }
            }
            ActiveKind::Start | ActiveKind::Data | ActiveKind::End => Err(self.corrupt()),
        }
    }

    fn complete_work(
        &self,
        index: usize,
        control: &mut Control,
        views: &[StageView],
        transaction: &Transaction<'_>,
        active: &Active,
    ) -> Result<bool, FlowError> {
        match active.kind {
            ActiveKind::Start => Ok(true),
            ActiveKind::Data => {
                let port = active.port as usize;
                let cursor = control.cursor_mut(&self.name, active.port)?;
                if cursor.position != active.position || cursor.ended {
                    return Err(self.corrupt());
                }
                cursor.position = cursor
                    .position
                    .checked_add(1)
                    .ok_or_else(|| self.corrupt())?;
                self.advance_fairness(control, port)?;
                self.collect_output(index, control, views, port, active.position, transaction)?;
                Ok(false)
            }
            ActiveKind::End => {
                let port = active.port as usize;
                let cursor = control.cursor_mut(&self.name, active.port)?;
                if cursor.position != active.position || cursor.ended {
                    return Err(self.corrupt());
                }
                cursor.ended = true;
                self.advance_fairness(control, port)?;
                Ok(control.cursors.iter().all(|cursor| cursor.ended))
            }
        }
    }

    fn collect_output(
        &self,
        index: usize,
        control: &Control,
        views: &[StageView],
        input_port: usize,
        position: u64,
        transaction: &Transaction<'_>,
    ) -> Result<(), FlowError> {
        let upstream = self.upstream(input_port, views)?;
        for (consumer_index, consumer_port) in &upstream.consumers {
            let consumer = views.get(*consumer_index).ok_or_else(|| self.corrupt())?;
            let cursor = if *consumer_index == index {
                control.cursor(&consumer.name, *consumer_port)?
            } else {
                load_control(&consumer.name, &consumer.control.access(transaction)?)?
                    .cursor(&consumer.name, *consumer_port)?
            };
            if cursor.position <= position {
                return Ok(());
            }
        }
        if upstream.output.access(transaction)?.remove(&position)? {
            Ok(())
        } else {
            Err(self.corrupt())
        }
    }

    fn advance_fairness(&self, control: &mut Control, port: usize) -> Result<(), FlowError> {
        let next = (port + 1) % self.inputs.len();
        control.next_input = u32::try_from(next).map_err(|_| self.corrupt())?;
        Ok(())
    }

    fn upstream<'a>(
        &self,
        port: usize,
        views: &'a [StageView],
    ) -> Result<&'a StageView, FlowError> {
        self.inputs
            .get(port)
            .and_then(|input| input.upstream)
            .and_then(|upstream| views.get(upstream))
            .ok_or_else(|| self.corrupt())
    }

    fn record_failure(
        &mut self,
        transactions: &mut Transactions,
        message: String,
    ) -> Result<FlowError, FlowError> {
        let transaction = transactions.begin()?;
        let mut access = self.control().access(&transaction)?;
        let mut control = load_control(&self.name, &access)?;
        let message = truncate_message(message);
        control.lifecycle = Lifecycle::Failed;
        control.failure = Some(message.clone());
        access.set(&control)?;
        transaction.commit()?;
        self.lifecycle = Lifecycle::Failed;
        Ok(FlowError::StageFailed {
            stage: self.name.clone(),
            message,
        })
    }

    fn load_failure(&self, transactions: &mut Transactions) -> Result<FlowError, FlowError> {
        let transaction = transactions.begin()?;
        let control = load_control(&self.name, &self.control().access(&transaction)?)?;
        let message = control.failure.ok_or_else(|| self.corrupt())?;
        Ok(FlowError::StageFailed {
            stage: self.name.clone(),
            message,
        })
    }

    fn control(&self) -> &Cell<Control> {
        self.control
            .as_ref()
            .expect("flow preparation binds stage control")
    }

    fn output(&self) -> &OrderedMap<u64, Vec<u8>> {
        self.output
            .as_ref()
            .expect("flow preparation binds stage output")
    }

    fn corrupt(&self) -> FlowError {
        FlowError::CorruptStage {
            stage: self.name.clone(),
        }
    }
}

impl StageView {
    pub(crate) fn from_nodes(nodes: &[StageNode]) -> Vec<Self> {
        nodes
            .iter()
            .map(|node| Self {
                name: node.name.clone(),
                control: node.control().clone(),
                output: node.output().clone(),
                consumers: node.consumers.clone(),
            })
            .collect()
    }
}

fn validate_decision(decision: &Decision, leaf: bool) -> Option<String> {
    let (output, checkpoint) = match decision {
        Decision::Pending => (None, None),
        Decision::Checkpoint { checkpoint } => (None, Some(checkpoint.as_slice())),
        Decision::Publish { output, checkpoint } => {
            (Some(output.as_slice()), Some(checkpoint.as_slice()))
        }
        Decision::Complete { output } => (output.as_deref(), None),
    };
    if leaf && output.is_some() {
        return Some("an unconnected stage cannot publish output".to_owned());
    }
    if output.is_some_and(|bytes| bytes.len() > Decision::MAX_OUTPUT_BYTES) {
        return Some(format!(
            "output exceeds {} bytes",
            Decision::MAX_OUTPUT_BYTES
        ));
    }
    if checkpoint.is_some_and(|bytes| bytes.len() > MAX_CHECKPOINT_BYTES) {
        return Some(format!("checkpoint exceeds {MAX_CHECKPOINT_BYTES} bytes"));
    }
    None
}

fn truncate_message(mut message: String) -> String {
    if message.len() <= MAX_FAILURE_BYTES {
        return message;
    }
    let mut end = MAX_FAILURE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message
}

#[cfg(test)]
mod tests {
    use crate::Decision;

    use super::validate_decision;

    #[test]
    fn decision_size_limits_are_inclusive() {
        let exact = Decision::Publish {
            output: vec![0; Decision::MAX_OUTPUT_BYTES],
            checkpoint: vec![0; Decision::MAX_CHECKPOINT_BYTES],
        };
        assert_eq!(validate_decision(&exact, false), None);

        let output_too_large = Decision::Complete {
            output: Some(vec![0; Decision::MAX_OUTPUT_BYTES + 1]),
        };
        assert!(validate_decision(&output_too_large, false).is_some());

        let checkpoint_too_large = Decision::Checkpoint {
            checkpoint: vec![0; Decision::MAX_CHECKPOINT_BYTES + 1],
        };
        assert!(validate_decision(&checkpoint_too_large, false).is_some());
    }
}
