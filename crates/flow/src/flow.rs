use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use dogpaddle_store::{Cell, DataHandle, DataPlacement, Store, StoreError, Transactions};

use crate::{
    FlowError, Operation, Stage,
    stage::{InputBinding, Lifecycle, Poll, StageNode, StageView},
};

const SYSTEM_PREFIX: &str = "__dogpaddle/";
const MANIFEST_NAME: &str = "__dogpaddle/flow";
const SCHEDULE_NAME: &str = "__dogpaddle/schedule";
const MANIFEST_MAGIC: &[u8] = b"dogpaddle.flow";

static NEXT_FLOW: AtomicU64 = AtomicU64::new(1);

/// Observable progress after one fair scheduling turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum StepOutcome {
    /// One stage committed a checkpoint, output, or completed work.
    Progress,
    /// No stage could currently make progress.
    Idle,
    /// Every stage has durably finished.
    Finished,
}

/// A durable, static directed acyclic graph of stages.
pub struct Flow {
    token: u64,
    store: Option<Store>,
    transactions: Option<Transactions>,
    schedule: Option<Cell<u64>>,
    sealed: bool,
    data: BTreeMap<String, DataPlacement>,
    stages: Vec<StageNode>,
    next_stage: usize,
}

impl Flow {
    /// Creates a new durable flow.
    ///
    /// # Errors
    ///
    /// Returns an error when its store cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, FlowError> {
        Self::new(Store::create(path)?)
    }

    /// Opens an existing durable flow declaration.
    ///
    /// # Errors
    ///
    /// Returns an error when its store cannot be opened. The declaration is
    /// validated when execution first begins, after stages and edges are supplied.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FlowError> {
        Self::new(Store::open(path)?)
    }

    fn new(store: Store) -> Result<Self, FlowError> {
        let sealed = match store.open_data(MANIFEST_NAME) {
            Ok(_) => true,
            Err(StoreError::DataNotFound(_)) => false,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            token: NEXT_FLOW.fetch_add(1, Ordering::Relaxed),
            store: Some(store),
            transactions: None,
            schedule: None,
            sealed,
            data: BTreeMap::new(),
            stages: Vec::new(),
            next_stage: 0,
        })
    }

    /// Creates or opens one operation-owned data namespace.
    ///
    /// Flow reserves names beginning with `__dogpaddle/` for stage control.
    /// The returned handle is injected into an operation and Flow never
    /// interprets its contents.
    ///
    /// # Errors
    ///
    /// Returns an error for a reserved name, after execution has begun, or when
    /// the underlying Store cannot create or open the namespace.
    pub fn data(&mut self, name: &str, placement: DataPlacement) -> Result<DataHandle, FlowError> {
        if name.starts_with(SYSTEM_PREFIX) {
            return Err(FlowError::ReservedDataName(name.to_owned()));
        }
        if self
            .data
            .get(name)
            .is_some_and(|previous| *previous != placement)
        {
            return Err(FlowError::DeclarationMismatch);
        }
        let sealed = self.sealed;
        let store = self.store.as_mut().ok_or(FlowError::AlreadyRunning)?;
        let handle = bind_data(store, name, placement, sealed)?;
        self.data.insert(name.to_owned(), placement);
        Ok(handle)
    }

    /// Adds one operation as a named stage and returns its opaque handle.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate stage name, invalid input
    /// port declarations, or after execution has begun.
    pub fn stage(
        &mut self,
        name: impl Into<String>,
        input_ports: &[&str],
        operation: impl Operation,
    ) -> Result<Stage, FlowError> {
        if self.store.is_none() {
            return Err(FlowError::AlreadyRunning);
        }
        let name = name.into();
        if name.is_empty() || self.stages.iter().any(|stage| stage.name == name) {
            return Err(FlowError::StageName(name));
        }
        let mut seen = HashSet::new();
        let mut inputs = Vec::new();
        for input in input_ports {
            if input.is_empty() || !seen.insert(*input) {
                return Err(FlowError::InputPort {
                    stage: name,
                    port: (*input).to_owned(),
                });
            }
            inputs.push(InputBinding {
                name: (*input).to_owned(),
                upstream: None,
            });
        }
        let index = self.stages.len();
        self.stages
            .push(StageNode::new(name, Box::new(operation), inputs));
        Ok(Stage {
            flow: self.token,
            index,
        })
    }

    /// Connects one stage's single output to a named input on another stage.
    ///
    /// Fan-out is allowed. Every input has exactly one upstream stage.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign stage handle, missing input, duplicate
    /// connection, or after execution has begun. Cycles are rejected before
    /// the first execution step.
    pub fn connect(
        &mut self,
        upstream: Stage,
        downstream: Stage,
        input: &str,
    ) -> Result<(), FlowError> {
        self.ensure_stage(upstream)?;
        self.ensure_stage(downstream)?;
        if self.store.is_none() {
            return Err(FlowError::AlreadyRunning);
        }
        let port = {
            let downstream_node = &mut self.stages[downstream.index];
            let (port, binding) = downstream_node
                .inputs
                .iter_mut()
                .enumerate()
                .find(|(_, binding)| binding.name == input && binding.upstream.is_none())
                .ok_or_else(|| FlowError::InputConnection {
                    stage: downstream_node.name.clone(),
                    input: input.to_owned(),
                })?;
            binding.upstream = Some(upstream.index);
            u32::try_from(port).map_err(|_| FlowError::InputPort {
                stage: downstream_node.name.clone(),
                port: input.to_owned(),
            })?
        };
        self.stages[upstream.index]
            .consumers
            .push((downstream.index, port));
        Ok(())
    }

    /// Runs at most one successful Stage transaction using fair round-robin scheduling.
    ///
    /// Call this again after [`StepOutcome::Progress`] or after an external wakeup
    /// following [`StepOutcome::Idle`].
    ///
    /// # Errors
    ///
    /// Returns an error when declaration validation, storage, or an operation fails.
    pub fn step(&mut self) -> Result<StepOutcome, FlowError> {
        self.prepare()?;
        if self.stages.is_empty()
            || self
                .stages
                .iter()
                .all(|stage| stage.lifecycle == Lifecycle::Finished)
        {
            return Ok(StepOutcome::Finished);
        }
        let schedule = self.schedule().clone();

        if let Some(index) = self
            .stages
            .iter()
            .position(|stage| stage.lifecycle == Lifecycle::Failed)
        {
            let views = StageView::from_nodes(&self.stages);
            let transactions = self
                .transactions
                .as_mut()
                .ok_or(FlowError::AlreadyRunning)?;
            return self.stages[index]
                .poll(index, &views, &schedule, 0, transactions)
                .map(|_| StepOutcome::Idle);
        }

        let views = StageView::from_nodes(&self.stages);
        let count = self.stages.len();
        let start = self.next_stage;
        for offset in 0..count {
            let index = (start + offset) % count;
            if self.stages[index].lifecycle == Lifecycle::Finished {
                continue;
            }
            let transactions = self
                .transactions
                .as_mut()
                .ok_or(FlowError::AlreadyRunning)?;
            let next =
                u64::try_from((index + 1) % count).map_err(|_| FlowError::DeclarationMismatch)?;
            let poll = self.stages[index].poll(index, &views, &schedule, next, transactions)?;
            match poll {
                Poll::Idle => {}
                Poll::Progress => {
                    self.next_stage = (index + 1) % count;
                    return Ok(StepOutcome::Progress);
                }
                Poll::Finished => {
                    self.next_stage = (index + 1) % count;
                    return if self
                        .stages
                        .iter()
                        .all(|stage| stage.lifecycle == Lifecycle::Finished)
                    {
                        Ok(StepOutcome::Finished)
                    } else {
                        Ok(StepOutcome::Progress)
                    };
                }
            }
        }
        self.next_stage = (start + 1) % count;
        Ok(StepOutcome::Idle)
    }

    fn ensure_stage(&self, stage: Stage) -> Result<(), FlowError> {
        if stage.flow != self.token || stage.index >= self.stages.len() {
            Err(FlowError::WrongFlow)
        } else {
            Ok(())
        }
    }

    fn prepare(&mut self) -> Result<(), FlowError> {
        if self.transactions.is_some() {
            return Ok(());
        }
        self.validate_graph()?;
        let manifest = self.manifest();
        let mut store = self.store.take().ok_or(FlowError::AlreadyRunning)?;
        let sealed = self.sealed;
        for (index, stage) in self.stages.iter_mut().enumerate() {
            let control = bind_data(
                &mut store,
                &format!("{SYSTEM_PREFIX}stage/{index}/control"),
                DataPlacement::Shared,
                sealed,
            )?;
            let output = bind_data(
                &mut store,
                &format!("{SYSTEM_PREFIX}stage/{index}/output"),
                DataPlacement::Shared,
                sealed,
            )?;
            stage.bind(control, output);
        }
        let schedule_data = bind_data(&mut store, SCHEDULE_NAME, DataPlacement::Shared, sealed)?;
        let manifest_data = bind_data(&mut store, MANIFEST_NAME, DataPlacement::Shared, sealed)?;
        let schedule = Cell::new(schedule_data);
        let manifest_cell = Cell::new(manifest_data);

        let mut transactions = store.into_transactions();
        {
            let transaction = transactions.begin()?;
            let mut manifest_access = manifest_cell.access(&transaction)?;
            let mut schedule_access = schedule.access(&transaction)?;
            let persisted = manifest_access.get()?;
            if persisted.is_none() {
                if schedule_access.get()?.is_some() {
                    return Err(FlowError::DeclarationMismatch);
                }
                schedule_access.set(&0)?;
                manifest_access.set(&manifest)?;
                for stage in &self.stages {
                    stage.initialize(&transaction)?;
                }
            } else {
                if persisted.as_deref() != Some(manifest.as_slice()) {
                    return Err(FlowError::DeclarationMismatch);
                }
                for stage in &mut self.stages {
                    stage.restore(&transaction)?;
                }
                let next = schedule_access
                    .get()?
                    .ok_or(FlowError::DeclarationMismatch)?;
                let next = usize::try_from(next).map_err(|_| FlowError::DeclarationMismatch)?;
                if (self.stages.is_empty() && next != 0)
                    || (!self.stages.is_empty() && next >= self.stages.len())
                {
                    return Err(FlowError::DeclarationMismatch);
                }
                self.next_stage = next;
            }
            transaction.commit()?;
        }
        self.schedule = Some(schedule);
        self.sealed = true;
        self.transactions = Some(transactions);
        Ok(())
    }

    fn validate_graph(&self) -> Result<(), FlowError> {
        let mut indegree = vec![0_usize; self.stages.len()];
        let mut outgoing = vec![Vec::new(); self.stages.len()];
        for (downstream, stage) in self.stages.iter().enumerate() {
            for input in &stage.inputs {
                let upstream = input.upstream.ok_or_else(|| FlowError::UnconnectedInput {
                    stage: stage.name.clone(),
                    input: input.name.clone(),
                })?;
                indegree[downstream] += 1;
                outgoing[upstream].push(downstream);
            }
        }
        let mut ready = indegree
            .iter()
            .enumerate()
            .filter_map(|(index, degree)| (*degree == 0).then_some(index))
            .collect::<VecDeque<_>>();
        let mut visited = 0;
        while let Some(stage) = ready.pop_front() {
            visited += 1;
            for downstream in &outgoing[stage] {
                indegree[*downstream] -= 1;
                if indegree[*downstream] == 0 {
                    ready.push_back(*downstream);
                }
            }
        }
        if visited == self.stages.len() {
            Ok(())
        } else {
            Err(FlowError::Cycle)
        }
    }

    fn manifest(&self) -> Vec<u8> {
        let mut manifest = Vec::new();
        push_bytes(&mut manifest, MANIFEST_MAGIC);
        push_u64(&mut manifest, self.data.len() as u64);
        for (name, placement) in &self.data {
            push_bytes(&mut manifest, name.as_bytes());
            manifest.push(match placement {
                DataPlacement::Shared => 0,
                DataPlacement::Dedicated => 1,
            });
        }
        push_u64(&mut manifest, self.stages.len() as u64);
        for stage in &self.stages {
            push_bytes(&mut manifest, stage.name.as_bytes());
            push_bytes(&mut manifest, stage.operation.fingerprint());
            push_u64(&mut manifest, stage.inputs.len() as u64);
            for input in &stage.inputs {
                push_bytes(&mut manifest, input.name.as_bytes());
                push_u64(
                    &mut manifest,
                    input.upstream.expect("validated graph has complete inputs") as u64,
                );
            }
        }
        manifest
    }

    fn schedule(&self) -> &Cell<u64> {
        self.schedule
            .as_ref()
            .expect("flow preparation binds the schedule")
    }
}

fn bind_data(
    store: &mut Store,
    name: &str,
    placement: DataPlacement,
    sealed: bool,
) -> Result<DataHandle, FlowError> {
    match store.open_data(name) {
        Ok(data) if data.placement() == placement => Ok(data),
        Ok(_) => Err(FlowError::DeclarationMismatch),
        Err(StoreError::DataNotFound(_)) if !sealed => Ok(store.create_data(name, placement)?),
        Err(error) => Err(error.into()),
    }
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    push_u64(output, bytes.len() as u64);
    output.extend_from_slice(bytes);
}
