use std::{sync::Arc, time::Duration};

use arrow_schema::SchemaRef;
use dogpaddle_debezium::{Checkpoint, Connector};
use dogpaddle_store::Cell;

use crate::operation::{
    Action, AfterCommit, Operation, OperationError, OperationInput, PostCommitError, Turn,
};

use super::{MySqlCdcScanConfig, MySqlCdcScanError, MySqlCdcScanSpec, convert::convert_records};

/// One materialized `MySQL` CDC Scan with reconstructible connector resources.
///
/// Durable state belongs to its declared Store cell; constructing this runtime
/// opens neither `MySQL` nor the Debezium bundle.
pub struct MySqlCdcScanOperation {
    spec: MySqlCdcScanSpec,
    output_schema: SchemaRef,
    checkpoint: Cell<Vec<u8>>,
    config: MySqlCdcScanConfig,
    bootstrap_checkpoint: Checkpoint,
    restored: bool,
    resume: Option<Checkpoint>,
    connector: Option<Connector>,
    restart_connector: bool,
}

impl MySqlCdcScanOperation {
    pub(super) fn new_bound(
        spec: MySqlCdcScanSpec,
        output_schema: SchemaRef,
        checkpoint: Cell<Vec<u8>>,
        config: MySqlCdcScanConfig,
        bootstrap_checkpoint: Checkpoint,
    ) -> Self {
        Self {
            spec,
            output_schema,
            checkpoint,
            config,
            bootstrap_checkpoint,
            restored: false,
            resume: None,
            connector: None,
            restart_connector: false,
        }
    }

    fn restore(&mut self) -> Turn<'_> {
        Turn::ready(move |access| {
            let durable = self
                .checkpoint
                .access(access)?
                .get()?
                .map(Checkpoint::from_bytes)
                .transpose()
                .map_err(|_| MySqlCdcScanError::InvalidState("CDC scan checkpoint is invalid"))?;
            if let Some(checkpoint) = durable.as_ref()
                && !checkpoint.matches(&self.spec.engine_name, super::definition::CONNECTOR_CLASS)
            {
                return Err(MySqlCdcScanError::InvalidState(
                    "CDC scan checkpoint belongs to another MySQL connector",
                )
                .into());
            }
            let resume = durable.unwrap_or_else(|| self.bootstrap_checkpoint.clone());
            Ok((
                Action::Commit(None),
                AfterCommit::new(move || {
                    self.resume = Some(resume);
                    self.restored = true;
                    Ok(())
                }),
            ))
        })
    }
}

impl Operation for MySqlCdcScanOperation {
    fn turn<'turn>(
        &'turn mut self,
        input: Option<OperationInput<'turn>>,
    ) -> Result<Turn<'turn>, OperationError> {
        if input.is_some() {
            return Err(MySqlCdcScanError::new("MySQL CDC scan does not accept input").into());
        }

        // The first turn only restores durable state. Opening a JVM or MySQL
        // connection belongs to the next turn, outside the Store transaction.
        if !self.restored {
            return Ok(self.restore());
        }

        if self.restart_connector || self.connector.is_none() {
            // Connector Drop only abandons work; it never acknowledges it.
            self.connector = None;
            let checkpoint = self
                .resume
                .as_ref()
                .expect("restore always selects a bootstrap or durable checkpoint");
            self.connector = Some(self.config.start(&self.spec, checkpoint)?);
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
            MySqlCdcScanError::new(format!("Debezium poll failed ({:?})", error.kind()))
        })?;
        self.restart_connector = false;
        let Some(delivery) = polled else {
            return Ok(Turn::Idle);
        };
        let change = convert_records(
            &self.spec.columns,
            Arc::clone(&self.output_schema),
            &self.spec.engine_name,
            &self.spec.database,
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
                        PostCommitError::new(MySqlCdcScanError::new(format!(
                            "Debezium ACK failed ({:?})",
                            error.kind()
                        )))
                    })
                }),
            ))
        }))
    }
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, prelude::BASE64_STANDARD};
    use dogpaddle_store::{Cell, Store};

    use super::*;
    use crate::operation::scan::{MySqlColumn, MySqlType};

    fn checkpoint() -> Checkpoint {
        Checkpoint::from_bytes(
            BASE64_STANDARD
                .decode(
                    "RFBEQkNQMDEAAQAAAAZvcmRlcnMAAAAqaW8uZGViZXppdW0uY29ubmVjdG9yLm15c3FsLk15U3FsQ29ubmVjdG9yAAAAAQAAAAVteXNxbAAAAAMAAQK8UTFt",
                )
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn empty_mutable_checkpoint_resolves_the_immutable_bootstrap_seed() {
        let root = tempfile::tempdir().unwrap();
        let mut store = Store::create(root.path().join("store")).unwrap();
        let checkpoint_cell = store.create_data::<Cell<Vec<u8>>>("checkpoint").unwrap();
        let observed_cell = checkpoint_cell.clone();
        let mut transactions = store.into_transactions();
        let columns = vec![MySqlColumn::new("id", MySqlType::Int64, false)];
        let bootstrap_checkpoint = checkpoint();
        let mut operation = MySqlCdcScanOperation::new_bound(
            MySqlCdcScanSpec {
                engine_name: "orders".to_owned(),
                database: "shop".to_owned(),
                table: "orders".to_owned(),
                server_uuid: "01234567-89ab-cdef-0123-456789abcdef".to_owned(),
                table_id: 43,
                columns: columns.clone(),
            },
            super::super::schema::compile(&columns).unwrap(),
            checkpoint_cell,
            MySqlCdcScanConfig::new_unencrypted(
                "/nonexistent/dogpaddle-runtime",
                "127.0.0.1",
                1,
                "shop",
                "cdc",
                "password",
                54_001,
            )
            .unwrap(),
            bootstrap_checkpoint.clone(),
        );

        let Turn::Ready(prepared) = operation.turn(None).unwrap() else {
            panic!("expected restore work");
        };
        let transaction = transactions.begin().unwrap();
        let (action, completion) = prepared.apply(transaction.access()).unwrap();
        assert!(matches!(action, Action::Commit(None)));
        transaction.commit().unwrap();
        completion.run().unwrap();

        assert_eq!(operation.resume, Some(bootstrap_checkpoint));
        assert!(operation.restored);
        let transaction = transactions.begin().unwrap();
        assert_eq!(
            observed_cell
                .access(transaction.access())
                .unwrap()
                .get()
                .unwrap(),
            None
        );
        transaction.commit().unwrap();
    }
}
