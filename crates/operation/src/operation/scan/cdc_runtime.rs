//! The shared sealed-snapshot and committed-delivery protocol for the two CDC sources.
use std::{num::NonZeroU64, time::Duration};

use arrow_schema::SchemaRef;
use dogpaddle_change::{Change, decode_change_owned, encode_change};
use dogpaddle_debezium::{Checkpoint, Connector, Record};
use dogpaddle_store::{Cell, Queue};

use crate::operation::{Action, AfterCommit, OperationError, OperationInput, Turn, TurnOperation};

const CAPTURING: u32 = 1;
const PUBLISHING: u32 = 2;
const STREAMING: u32 = 3;
const RESETTING: u32 = 4;

const STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Fresh,
    Capturing,
    Publishing,
    Streaming,
    Resetting,
}

impl Phase {
    fn decode<B: Source>(value: Option<u32>) -> Result<Self, OperationError> {
        match value {
            None => Ok(Self::Fresh),
            Some(CAPTURING) => Ok(Self::Capturing),
            Some(PUBLISHING) => Ok(Self::Publishing),
            Some(STREAMING) => Ok(Self::Streaming),
            Some(RESETTING) => Ok(Self::Resetting),
            Some(_) => Err(B::invalid_state("CDC scan phase is invalid")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NextStep {
    Restore,
    BeginCapture,
    Capture,
    PrepareReset,
    Reset,
    Publish,
    Stream,
    RestartStream,
}

pub(super) struct Captured<P> {
    pub(super) change: Option<Change>,
    pub(super) sealed: bool,
    pub(super) progress: P,
}

/// Only database-specific work lives here; the runtime owns Connector and every transaction.
pub(super) trait Source: Send + 'static {
    type Progress: Copy + Default + Send + 'static;
    /// `PostgreSQL` must remove its abandoned slot before durably entering reset.
    const RESET_REQUIRES_SOURCE_CLEANUP: bool;
    fn start_snapshot(&self) -> Result<Connector, OperationError>;
    fn start_streaming(&self, checkpoint: &Checkpoint) -> Result<Connector, OperationError>;
    fn cleanup_snapshot(&self) -> Result<(), OperationError>;
    fn capture(
        &self,
        schema: SchemaRef,
        records: &[Record],
        progress: Self::Progress,
    ) -> Result<Captured<Self::Progress>, OperationError>;
    fn stream(
        &self,
        schema: SchemaRef,
        records: &[Record],
    ) -> Result<Option<Change>, OperationError>;
    fn restore_checkpoint(
        &self,
        phase: Phase,
        bytes: Option<Vec<u8>>,
        spool_empty: bool,
    ) -> Result<Option<Checkpoint>, OperationError>;
    fn invalid_state(message: &'static str) -> OperationError;
    fn runtime_error(message: String) -> OperationError;
    fn spool_full() -> OperationError;
    fn codec_error(error: dogpaddle_change::CodecError) -> OperationError;
}

pub(super) struct CdcRuntime<B: Source> {
    source: B,
    pub(super) output_schema: SchemaRef,
    phase_cell: Cell<u32>,
    checkpoint: Cell<Vec<u8>>,
    spool: Queue<Vec<u8>>,
    capacity: NonZeroU64,
    pub(super) next_step: NextStep,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    progress: B::Progress,
}

impl<B: Source> CdcRuntime<B> {
    pub(super) fn new(
        source: B,
        output_schema: SchemaRef,
        phase_cell: Cell<u32>,
        checkpoint: Cell<Vec<u8>>,
        spool: Queue<Vec<u8>>,
        capacity: NonZeroU64,
    ) -> Self {
        Self {
            source,
            output_schema,
            phase_cell,
            checkpoint,
            spool,
            capacity,
            next_step: NextStep::Restore,
            resume: None,
            connector: None,
            progress: B::Progress::default(),
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let phase = Phase::decode::<B>(self.phase_cell.access(access)?.get()?)?;
            let checkpoint = self.checkpoint.access(access)?.get()?;
            let empty = self.spool.access(access)?.is_empty()?;
            match phase {
                Phase::Fresh if checkpoint.is_some() || !empty => {
                    return Err(B::invalid_state("fresh CDC scan retains bootstrap data"));
                }
                Phase::Streaming if !empty => {
                    return Err(B::invalid_state(
                        "streaming CDC scan retains bootstrap output",
                    ));
                }
                Phase::Capturing if !empty && checkpoint.is_none() => {
                    return Err(B::invalid_state(
                        "captured bootstrap output has no checkpoint",
                    ));
                }
                _ => {}
            }
            let resume = self.source.restore_checkpoint(phase, checkpoint, empty)?;
            if phase == Phase::Capturing && !B::RESET_REQUIRES_SOURCE_CLEANUP {
                self.phase_cell.access(access)?.set(&RESETTING)?;
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.next_step = match phase {
                        Phase::Fresh => NextStep::BeginCapture,
                        Phase::Capturing if B::RESET_REQUIRES_SOURCE_CLEANUP => {
                            NextStep::PrepareReset
                        }
                        Phase::Capturing | Phase::Resetting => NextStep::Reset,
                        Phase::Publishing => NextStep::Publish,
                        Phase::Streaming => NextStep::Stream,
                    };
                    self.resume = resume;
                    Ok(())
                }),
            ))
        })
    }

    fn begin_capture(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            self.phase_cell.access(access)?.set(&CAPTURING)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.next_step = NextStep::Capture;
                    self.progress = B::Progress::default();
                    Ok(())
                }),
            ))
        })
    }

    fn stop_connector(&mut self, stage: &str) -> Result<(), OperationError> {
        if let Some(connector) = self.connector.as_mut() {
            connector.stop(STOP_TIMEOUT).map_err(|error| {
                B::runtime_error(format!("Debezium {stage} stop failed ({:?})", error.kind()))
            })?;
        }
        self.connector = None;
        Ok(())
    }

    fn prepare_reset(&mut self) -> Result<Turn<'_>, OperationError> {
        self.stop_connector("snapshot")?;
        self.source.cleanup_snapshot()?;
        Ok(Turn::ready(move |access| {
            self.phase_cell.access(access)?.set(&RESETTING)?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.next_step = NextStep::Reset;
                    self.resume = None;
                    self.progress = B::Progress::default();
                    Ok(())
                }),
            ))
        }))
    }

    fn reset(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let mut spool = self.spool.access(access)?;
            let finished = spool.pop_front()?.is_none() || spool.is_empty()?;
            if finished {
                self.checkpoint.access(access)?.clear()?;
                self.phase_cell.access(access)?.clear()?;
            }
            Ok((
                Action::Commit(None),
                if finished {
                    AfterCommit::new(move || {
                        self.next_step = NextStep::BeginCapture;
                        self.resume = None;
                        Ok(())
                    })
                } else {
                    AfterCommit::none()
                },
            ))
        })
    }

    fn capture(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.connector.is_none() {
            self.next_step = NextStep::PrepareReset;
            self.connector = Some(self.source.start_snapshot()?);
            self.next_step = NextStep::Capture;
        }
        let connector = self
            .connector
            .as_mut()
            .expect("snapshot connector was started");
        let delivery = match connector.poll(Duration::ZERO) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(Turn::Idle),
            Err(error) => {
                self.next_step = NextStep::PrepareReset;
                return Err(B::runtime_error(format!(
                    "Debezium snapshot poll failed ({:?})",
                    error.kind()
                )));
            }
        };
        let captured = match self.source.capture(
            self.output_schema.clone(),
            delivery.records(),
            self.progress,
        ) {
            Ok(captured) => captured,
            Err(error) => {
                self.next_step = NextStep::PrepareReset;
                return Err(error);
            }
        };
        let encoded = match captured.change.as_ref().map(encode_change).transpose() {
            Ok(encoded) => encoded,
            Err(error) => {
                self.next_step = NextStep::PrepareReset;
                return Err(B::codec_error(error));
            }
        };
        let checkpoint_bytes = delivery.checkpoint().as_bytes().to_vec();
        let resumed = delivery.checkpoint().clone();
        let checkpoint = &self.checkpoint;
        let phase_cell = &self.phase_cell;
        let spool = &self.spool;
        let capacity = self.capacity;
        let next_step = &mut self.next_step;
        let progress = &mut self.progress;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            if let Some(encoded) = encoded
                && !spool.access(access)?.try_push(&encoded, capacity)?
            {
                return Err(B::spool_full());
            }
            checkpoint.access(access)?.set(&checkpoint_bytes)?;
            if captured.sealed {
                phase_cell.access(access)?.set(&PUBLISHING)?;
            }
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        B::runtime_error(format!(
                            "Debezium snapshot ACK failed ({:?})",
                            error.kind()
                        ))
                    })?;
                    if captured.sealed {
                        *next_step = NextStep::Publish;
                        *resume = Some(resumed);
                    } else {
                        *progress = captured.progress;
                    }
                    Ok(())
                }),
            ))
        }))
    }

    fn publish(&mut self) -> Result<Turn<'_>, OperationError> {
        self.stop_connector("snapshot")?;
        Ok(Turn::ready(move |access| {
            let mut spool = self.spool.access(access)?;
            let change = spool
                .pop_front()?
                .map(|encoded| {
                    let change = decode_change_owned(encoded)
                        .map_err(|_| B::invalid_state("bootstrap spool Change is invalid"))?;
                    if change.records().schema() != self.output_schema {
                        return Err(B::invalid_state(
                            "bootstrap spool Change has the wrong schema",
                        ));
                    }
                    Ok::<_, OperationError>(change)
                })
                .transpose()?;
            let finished = spool.is_empty()?;
            if finished {
                self.phase_cell.access(access)?.set(&STREAMING)?;
            }
            Ok((
                Action::Commit(change),
                if finished {
                    AfterCommit::new(move || {
                        self.next_step = NextStep::Stream;
                        Ok(())
                    })
                } else {
                    AfterCommit::none()
                },
            ))
        }))
    }

    fn stream(&mut self) -> Result<Turn<'_>, OperationError> {
        if self.connector.is_none() {
            let checkpoint = self
                .resume
                .as_ref()
                .ok_or_else(|| B::invalid_state("streaming CDC scan has no checkpoint"))?;
            self.connector = Some(self.source.start_streaming(checkpoint)?);
        }
        self.next_step = NextStep::RestartStream;
        let connector = self
            .connector
            .as_mut()
            .expect("streaming connector was started");
        let Some(delivery) = connector.poll(Duration::ZERO).map_err(|error| {
            B::runtime_error(format!(
                "Debezium streaming poll failed ({:?})",
                error.kind()
            ))
        })?
        else {
            self.next_step = NextStep::Stream;
            return Ok(Turn::Idle);
        };
        let change = self
            .source
            .stream(self.output_schema.clone(), delivery.records())?;
        self.next_step = NextStep::Stream;
        let encoded = delivery.checkpoint().as_bytes().to_vec();
        let resumed = delivery.checkpoint().clone();
        let checkpoint = &self.checkpoint;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            checkpoint.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(change),
                AfterCommit::new(move || {
                    delivery.ack().map_err(|error| {
                        B::runtime_error(format!(
                            "Debezium streaming ACK failed ({:?})",
                            error.kind()
                        ))
                    })?;
                    *resume = Some(resumed);
                    Ok(())
                }),
            ))
        }))
    }
}

impl<B: Source> TurnOperation for CdcRuntime<B> {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err(B::runtime_error(
                "CDC scan does not accept input".to_owned(),
            ));
        }
        match self.next_step {
            NextStep::Restore => Ok(self.restore()),
            NextStep::BeginCapture => Ok(self.begin_capture()),
            NextStep::Capture => self.capture(),
            NextStep::PrepareReset => self.prepare_reset(),
            NextStep::Reset => Ok(self.reset()),
            NextStep::Publish => self.publish(),
            NextStep::Stream => self.stream(),
            NextStep::RestartStream => {
                self.stop_connector("streaming")?;
                self.next_step = NextStep::Stream;
                self.stream()
            }
        }
    }
}
