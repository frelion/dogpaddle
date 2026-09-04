use std::sync::Arc;

use libmdbx::{Database, NoWriteMap, RW, Transaction as MdbxTransaction};

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
    /// use dogpaddle_store::{ReadTransaction, Store, StoreError};
    ///
    /// fn export_snapshot(store: &Store) -> Result<ReadTransaction<'static>, StoreError> {
    ///     store.read_transaction()
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when MDBX cannot begin the read-only transaction.
    pub fn read_transaction(&self) -> Result<ReadTransaction<'_>, StoreError> {
        begin_read_transaction(&self.database, self.token)
    }
}

impl Transactions {
    /// Splits this owned capability into write and read-only capabilities.
    ///
    /// This consumes `self` and returns the same unique write capability
    /// alongside a read-only capability over the same MDBX environment. A
    /// caller that only borrows [`Transactions`] therefore cannot export a
    /// transaction-start capability. The full owner may deliberately split
    /// again after recovering the write capability from the returned pair.
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
    /// Intentionally leaking the guard also leaks the underlying write
    /// transaction and is outside the normal RAII lifecycle.
    ///
    /// ```compile_fail
    /// use dogpaddle_store::{StoreError, Transactions};
    ///
    /// fn begin_twice(transactions: &mut Transactions) -> Result<(), StoreError> {
    ///     let first = transactions.begin()?;
    ///     let second = transactions.begin()?;
    ///     drop((first, second));
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when MDBX cannot begin the write transaction.
    pub fn begin(&mut self) -> Result<Transaction<'_>, StoreError> {
        Ok(Transaction {
            mdbx: self
                .database
                .begin_rw_txn()
                .map_err(|error| StoreError::storage("begin transaction", error))?,
            store_token: self.store_token,
            poisoned: std::cell::Cell::new(false),
            _thread_bound: std::marker::PhantomData,
        })
    }
}

impl ReadTransactions {
    /// Begins one independent read-only snapshot.
    ///
    /// Beginning a snapshot only borrows this capability, so other shared
    /// borrowers and the unique [`Transactions`] capability may remain active
    /// at the same time. The returned transaction contains no commit or write
    /// authority.
    ///
    /// # Errors
    ///
    /// Returns an error when MDBX cannot begin the read-only transaction.
    pub fn begin(&self) -> Result<ReadTransaction<'_>, StoreError> {
        begin_read_transaction(&self.database, self.store_token)
    }
}

fn begin_read_transaction(
    database: &Database<NoWriteMap>,
    store_token: u64,
) -> Result<ReadTransaction<'_>, StoreError> {
    Ok(ReadTransaction {
        mdbx: database
            .begin_ro_txn()
            .map_err(|error| StoreError::storage("begin read transaction", error))?,
        store_token,
        poisoned: std::cell::Cell::new(false),
        _thread_bound: std::marker::PhantomData,
    })
}

impl Transaction<'_> {
    /// Borrows this transaction as a typed data-access capability.
    ///
    /// The returned value may be passed to existing [`crate::Cell`],
    /// [`crate::OrderedMap`], and [`crate::AppendLog`] objects. It cannot
    /// commit the transaction or create, open, or enumerate data objects.
    #[must_use]
    pub fn access(&self) -> TransactionAccess<'_> {
        TransactionAccess { transaction: self }
    }

    /// Atomically commits all changes and consumes the transaction.
    ///
    /// A successful return makes every change visible together. An error
    /// means the transaction was aborted and none of its changes became
    /// visible; callers never need to account for a partially committed write
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is poisoned or MDBX cannot commit it.
    pub fn commit(self) -> Result<(), StoreError> {
        let Self { mdbx, poisoned, .. } = self;
        if poisoned.get() {
            return Err(StoreError::TransactionPoisoned);
        }
        commit_mdbx(mdbx)
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
    ///
    /// The returned value may be passed only to collection `read` methods. It
    /// cannot write, commit, or escape this snapshot's lifetime.
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

pub(super) fn commit_mdbx(
    transaction: MdbxTransaction<'_, RW, NoWriteMap>,
) -> Result<(), StoreError> {
    match transaction.commit() {
        Ok(false) => Ok(()),
        Ok(true) => Err(StoreError::storage(
            "commit transaction",
            "MDBX aborted a transaction marked with a prior error",
        )),
        Err(error) => Err(StoreError::storage("commit transaction", error)),
    }
}
