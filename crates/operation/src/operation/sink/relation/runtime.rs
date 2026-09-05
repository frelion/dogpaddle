use arrow_schema::SchemaRef;
use dogpaddle_change::Change;
use dogpaddle_store::Cell;

use super::{
    Continuation, FIRST_TECHNICAL_ID, Position, RelationTarget, first_position, invalid, plan,
    state::State,
};
use crate::operation::{Action, AfterCommit, Operation, OperationError, OperationInput, Turn};

pub(crate) struct RelationalSink<T> {
    schema: SchemaRef,
    target: T,
    state: Cell<Vec<u8>>,
    phase: Phase,
}

// A disposable cache. Every transition happens only after the local commit.
#[derive(Clone, Copy)]
enum Phase {
    Restore,
    New,
    Initialized,
    Ready {
        next_id: u64,
        position: Option<Position>,
    },
    Delivered {
        next_id: u64,
        continuation: Continuation,
    },
}

impl<T: RelationTarget> RelationalSink<T> {
    pub(crate) const fn new(schema: SchemaRef, target: T, state: Cell<Vec<u8>>) -> Self {
        Self {
            schema,
            target,
            state,
            phase: Phase::Restore,
        }
    }

    fn deliver(&mut self, state: State, input: &Change) -> Result<(), OperationError> {
        self.phase = match state {
            State::Initialize => {
                self.target.initialize()?;
                Phase::Initialized
            }
            State::Ready { next_id, position } => Phase::Ready { next_id, position },
            State::Prepared { next_id, batch } => {
                self.target.write_batch(input, &batch)?;
                Phase::Delivered {
                    next_id,
                    continuation: batch.continuation,
                }
            }
        };
        Ok(())
    }

    fn persist<'turn>(
        &'turn mut self,
        state: State,
        input: &'turn Change,
        complete: bool,
    ) -> Turn<'turn> {
        let bytes = state.encode();
        Turn::ready(move |access| {
            self.state.access(access)?.set(&bytes)?;
            let action = if complete {
                Action::Complete(None)
            } else {
                Action::Commit(None)
            };
            Ok((
                action,
                AfterCommit::new(move || self.deliver(state, input).map_err(Into::into)),
            ))
        })
    }
}

impl<T: RelationTarget> Operation for RelationalSink<T> {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        let input = input.ok_or_else(|| invalid("one input Change is required"))?;
        if input.port != 0 {
            return Err(invalid("only input port zero is supported"));
        }
        let input = input.change;
        if input.records().schema() != self.schema {
            return Err(invalid("input Schema differs from the bound Schema"));
        }
        Ok(match self.phase {
            Phase::Restore => Turn::ready(move |access| {
                let state = self
                    .state
                    .access(access)?
                    .get()?
                    .map(|bytes| State::decode(&bytes, input))
                    .transpose()?;
                Ok((
                    Action::Commit(None),
                    AfterCommit::new(move || {
                        if let Some(state) = state {
                            self.deliver(state, input)?;
                        } else {
                            self.phase = Phase::New;
                        }
                        Ok(())
                    }),
                ))
            }),
            Phase::New => {
                self.target.require_absent()?;
                self.persist(State::Initialize, input, false)
            }
            Phase::Initialized => self.persist(
                State::Ready {
                    next_id: FIRST_TECHNICAL_ID,
                    position: None,
                },
                input,
                false,
            ),
            Phase::Ready { next_id, position } => {
                let (next_id, batch) = plan::prepare(
                    &mut self.target,
                    input,
                    next_id,
                    position.unwrap_or_else(|| first_position(input)),
                )?;
                self.persist(State::Prepared { next_id, batch }, input, false)
            }
            Phase::Delivered {
                next_id,
                continuation,
            } => {
                let position = match continuation {
                    Continuation::Done => None,
                    Continuation::Position(position) => Some(position),
                };
                self.persist(
                    State::Ready { next_id, position },
                    input,
                    position.is_none(),
                )
            }
        })
    }
}
