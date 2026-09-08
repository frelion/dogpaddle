use std::sync::Arc;

use rocksdb::{
    OptimisticTransactionDB as Database, OptimisticTransactionOptions,
    Transaction as RocksTransaction, WriteOptions,
};

use super::{
    DataHandle, ReadTransaction, ReadTransactionAccess, ReadTransactions, Store, Transaction,
    TransactionAccess, Transactions,
};
use crate::StoreError;

impl Store {
    /// Begins a short-lived read-only snapshot during data object setup.
    ///
    /// The snapshot borrows this Store rather than exposing an owned
    /// transaction-start capability. After it is dropped, the same Store may
    /// continue opening data objects before being consumed with
    /// [`Store::into_transactions`]. The returned [`ReadTransaction`] has no
    /// write or commit authority.
    ///
    /// ```compile_fail
    /// use dogpaddle_store::{ReadTransaction, Store};
    ///
    /// fn export_snapshot(store: &Store) -> ReadTransaction<'static> {
    ///     store.read_transaction()
    /// }
    /// ```
    pub fn read_transaction(&self) -> ReadTransaction<'_> {
        begin_read_transaction(&self.database, self.token)
    }
}

impl Transactions {
    /// Splits this owned capability into write and read-only capabilities.
    ///
    /// This consumes `self` and returns the same unique write capability
    /// alongside a read-only capability over the same database. A caller that
    /// only borrows [`Transactions`] therefore cannot export a
    /// transaction-start capability.
    ///
    /// ```compile_fail,E0507
    /// use dogpaddle_store::Transactions;
    ///
    /// fn cannot_export_reader(transactions: &mut Transactions) {
    ///     let (_writes, _reads) = transactions.split();
    /// }
    /// ```
    #[must_use]
    pub fn split(self) -> (Self, ReadTransactions) {
        let reads = ReadTransactions {
            database: Arc::clone(&self.database),
            store_token: self.store_token,
        };
        (self, reads)
    }

    /// Begins one atomic write transaction.
    ///
    /// The returned [`Transaction`] exclusively borrows this unique capability.
    /// While that guard remains live, another transaction cannot be started
    /// through it. Because [`Transactions`] is not cloneable, its owner is the
    /// sole coordinator of transaction boundaries for this Store.
    ///
    /// ```compile_fail
    /// use dogpaddle_store::Transactions;
    ///
    /// fn begin_twice(transactions: &mut Transactions) {
    ///     let first = transactions.begin();
    ///     let second = transactions.begin();
    ///     drop((first, second));
    /// }
    /// ```
    pub fn begin(&mut self) -> Transaction<'_> {
        Transaction {
            inner: begin_write_transaction(&self.database),
            store_token: self.store_token,
            poisoned: std::cell::Cell::new(false),
            _thread_bound: std::marker::PhantomData,
        }
    }
}

impl ReadTransactions {
    /// Begins one independent read-only snapshot.
    ///
    /// Beginning a snapshot only borrows this capability, so other shared
    /// borrowers and the unique [`Transactions`] capability may remain active
    /// at the same time. The returned transaction contains no commit or write
    /// authority.
    pub fn begin(&self) -> ReadTransaction<'_> {
        begin_read_transaction(&self.database, self.store_token)
    }
}

fn begin_read_transaction(database: &Database, store_token: u64) -> ReadTransaction<'_> {
    ReadTransaction {
        snapshot: database.snapshot(),
        store_token,
        poisoned: std::cell::Cell::new(false),
        _thread_bound: std::marker::PhantomData,
    }
}

impl Transaction<'_> {
    /// Borrows this transaction as a typed data-access capability.
    #[must_use]
    pub fn access(&self) -> TransactionAccess<'_> {
        TransactionAccess { transaction: self }
    }

    /// Atomically commits all changes and consumes the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or `RocksDB` cannot
    /// commit it.
    pub fn commit(self) -> Result<(), StoreError> {
        let Self {
            inner, poisoned, ..
        } = self;
        if poisoned.get() {
            return Err(StoreError::TransactionPoisoned);
        }
        commit_transaction(inner)
    }

    pub(super) fn record_result<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(error) = &result
            && error.poisons_transaction()
        {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn ensure_access(&self, handle: &DataHandle) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        if handle.store_token != self.store_token {
            self.poisoned.set(true);
            return Err(StoreError::WrongStore);
        }
        Ok(())
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), StoreError> {
        if self.poisoned.get() {
            Err(StoreError::TransactionPoisoned)
        } else {
            Ok(())
        }
    }
}

impl ReadTransaction<'_> {
    /// Borrows this snapshot as a typed read-only data-access capability.
    #[must_use]
    pub fn access(&self) -> ReadTransactionAccess<'_> {
        ReadTransactionAccess { transaction: self }
    }

    pub(super) fn record_result<T>(&self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(error) = &result
            && error.poisons_transaction()
        {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn poison_on_error<T, E>(&self, result: Result<T, E>) -> Result<T, E> {
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }

    pub(super) fn ensure_access(&self, handle: &DataHandle) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        if handle.store_token != self.store_token {
            self.poisoned.set(true);
            return Err(StoreError::WrongStore);
        }
        Ok(())
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), StoreError> {
        if self.poisoned.get() {
            Err(StoreError::TransactionPoisoned)
        } else {
            Ok(())
        }
    }
}

impl<'transaction> TransactionAccess<'transaction> {
    pub(super) const fn transaction(self) -> &'transaction Transaction<'transaction> {
        self.transaction
    }
}

impl<'transaction> ReadTransactionAccess<'transaction> {
    pub(super) const fn transaction(self) -> &'transaction ReadTransaction<'transaction> {
        self.transaction
    }
}

pub(super) fn begin_write_transaction(database: &Database) -> RocksTransaction<'_, Database> {
    let write_options = durable_write_options();
    let mut transaction_options = OptimisticTransactionOptions::default();
    transaction_options.set_snapshot(true);
    database.transaction_opt(&write_options, &transaction_options)
}

pub(super) fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    options
}

pub(super) fn commit_transaction(
    transaction: RocksTransaction<'_, Database>,
) -> Result<(), StoreError> {
    transaction
        .commit()
        .map_err(|error| StoreError::storage("commit transaction", error))
}
