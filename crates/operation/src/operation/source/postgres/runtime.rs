use std::{sync::Arc, time::Duration};

use arrow_schema::SchemaRef;
use dogpaddle_debezium::{Checkpoint, Connector};
use dogpaddle_store::Cell;

use crate::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
};

use super::{
    PostgresSourceConfig, PostgresSourceError, PostgresSourceSpec, convert::convert_records,
};

/// One materialized `PostgreSQL` source with reconstructible connector resources.
///
/// Durable state belongs to its declared Store cell; constructing this runtime
/// opens neither `PostgreSQL` nor the Debezium bundle.
pub struct PostgresSourceOperation {
    spec: PostgresSourceSpec,
    output_schema: SchemaRef,
    checkpoint: Cell<Vec<u8>>,
    config: PostgresSourceConfig,
    restored: bool,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    restart_connector: bool,
}

impl PostgresSourceOperation {
    pub(super) fn new_bound(
        spec: PostgresSourceSpec,
        output_schema: SchemaRef,
        checkpoint: Cell<Vec<u8>>,
        config: PostgresSourceConfig,
    ) -> Self {
        Self {
            spec,
            output_schema,
            checkpoint,
            config,
            restored: false,
            resume: None,
            connector: None,
            restart_connector: false,
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let resume = self
                .checkpoint
                .access(access)?
                .get()?
                .map(Checkpoint::from_bytes)
                .transpose()
                .map_err(|_| PostgresSourceError::InvalidState("source checkpoint is invalid"))?;
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.resume = resume;
                    self.restored = true;
                    Ok(())
                }),
            ))
        })
    }
}

impl Operation for PostgresSourceOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err(PostgresSourceError::new("PostgreSQL source does not accept input").into());
        }

        // The first turn only restores durable state. Opening a JVM or PostgreSQL
        // connection belongs to the next turn, outside the Store transaction.
        if !self.restored {
            return Ok(self.restore());
        }

        if self.restart_connector || self.connector.is_none() {
            // Connector Drop only abandons work; it never acknowledges it.
            self.connector = None;
            self.connector = Some(self.config.start(&self.spec, self.resume.as_ref())?);
            self.restart_connector = false;
        }
        let connector = self
            .connector
            .as_mut()
            .expect("connector was started above");
        // A failed poll may poison Debezium. Remember to reconstruct it before
        // the next attempt without fighting the lifetime of a borrowed Delivery.
        self.restart_connector = true;
        // Waiting for data must not delay unrelated Stations in Flow's schedule.
        let polled = connector.poll(Duration::ZERO).map_err(|error| {
            PostgresSourceError::new(format!("Debezium poll failed ({:?})", error.kind()))
        })?;
        self.restart_connector = false;
        let Some(delivery) = polled else {
            return Ok(Turn::Idle);
        };
        let change = convert_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.schema,
            &self.spec.table,
            delivery.records(),
        )?;
        let encoded = delivery.checkpoint().as_bytes().to_vec();
        let checkpoint = &self.checkpoint;
        let resume = &mut self.resume;
        Ok(Turn::ready(move |access| {
            // Station commits this checkpoint and its output append atomically.
            // Backpressure rolls both back and drops the unacknowledged Delivery.
            checkpoint.access(access)?.set(&encoded)?;
            Ok((
                Action::Commit(change),
                AfterCommit::new(move || {
                    *resume = Some(delivery.checkpoint().clone());
                    delivery.ack().map_err(|error| {
                        PostCommitError::new(PostgresSourceError::new(format!(
                            "Debezium ACK failed ({:?})",
                            error.kind()
                        )))
                    })
                }),
            ))
        }))
    }
}
