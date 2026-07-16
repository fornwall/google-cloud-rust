// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::batch::BatchDml;
use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::CommitResponse;
use crate::model::request_options::Priority;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::mutation::Mutation;
use crate::read_only_transaction::BeginTransactionOption;
use crate::read_write_transaction::{ReadWriteTransaction, ReadWriteTransactionBuilder};
use crate::result_set::ResultSet;
use crate::statement::Statement;
use std::time::Duration as StdDuration;
use tokio::time::Instant;
use wkt::Duration;

/// A builder for [StatementBasedReadWriteTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let mut transaction = db_client.statement_based_read_write_transaction().build().await?;
/// let statement = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
/// transaction.execute_update(statement).await?;
/// transaction.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// This builder accepts the same transaction options as
/// [TransactionRunnerBuilder][crate::builder::TransactionRunnerBuilder], except for the
/// retry policy options: a statement-based transaction is not retried automatically,
/// so there is no retry policy to configure.
#[derive(Debug)]
pub struct StatementBasedReadWriteTransactionBuilder {
    builder: ReadWriteTransactionBuilder,
    timeout: Option<StdDuration>,
    begin_gax_options: Option<crate::RequestOptions>,
    commit_gax_options: Option<crate::RequestOptions>,
}

impl StatementBasedReadWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            builder: ReadWriteTransactionBuilder::new(client),
            timeout: None,
            begin_gax_options: None,
            commit_gax_options: None,
        }
    }

    /// Sets the timeout for the entire transaction.
    ///
    /// This timeout applies to the total time spent executing the transaction,
    /// including all statements and the commit. Each individual RPC within the
    /// transaction is automatically assigned a deadline derived from the
    /// remaining time of this overall timeout. The timeout also spans retry
    /// attempts started with
    /// [reset_for_retry][StatementBasedReadWriteTransaction::reset_for_retry].
    pub fn with_transaction_timeout(mut self, timeout: StdDuration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the per-attempt timeout for the BeginTransaction RPC.
    ///
    /// Note: This timeout is only used if the transaction uses the `ExplicitBegin`
    /// transaction option.
    pub fn with_begin_attempt_timeout(mut self, timeout: StdDuration) -> Self {
        self.begin_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_attempt_timeout(timeout);
        self
    }

    /// Sets the per-attempt timeout for the Commit RPC.
    pub fn with_commit_attempt_timeout(mut self, timeout: StdDuration) -> Self {
        self.commit_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_attempt_timeout(timeout);
        self
    }

    /// Sets the isolation level for the transaction.
    pub fn set_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.builder = self.builder.set_isolation_level(isolation_level);
        self
    }

    /// Sets the read lock mode for the transaction.
    ///
    /// This option is only used in combination with isolation level `Serializable`.
    pub fn set_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        self.builder = self.builder.set_read_lock_mode(read_lock_mode);
        self
    }

    /// Sets the transaction tag for the transaction.
    pub fn set_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.builder = self.builder.set_transaction_tag(tag);
        self
    }

    /// Sets whether the transaction should be started with an explicit
    /// BeginTransaction RPC, or inlined with the first statement in the
    /// transaction. Inlining the BeginTransaction option with the first
    /// statement saves one round-trip to Spanner, and is the default.
    pub fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.builder = self.builder.with_begin_transaction_option(option);
        self
    }

    /// Sets the priority for the Commit RPC of the transaction.
    pub fn set_commit_priority(mut self, priority: Priority) -> Self {
        self.builder = self.builder.set_commit_priority(priority);
        self
    }

    /// Sets the max commit delay for the transaction.
    ///
    /// The amount of latency this request is configured to incur in order to
    /// improve throughput. See <https://docs.cloud.google.com/spanner/docs/throughput-optimized-writes>
    /// for more information.
    pub fn set_max_commit_delay(mut self, delay: Duration) -> Self {
        self.builder = self.builder.set_max_commit_delay(delay);
        self
    }

    /// Sets whether the transaction should be excluded from change streams
    /// with the DDL option `allow_txn_exclusion=true`.
    pub fn set_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.builder = self.builder.set_exclude_txn_from_change_streams(exclude);
        self
    }

    /// Sets whether the transaction should request commit statistics from Spanner.
    pub fn set_return_commit_stats(mut self, return_stats: bool) -> Self {
        self.builder = self.builder.set_return_commit_stats(return_stats);
        self
    }

    /// Builds the [StatementBasedReadWriteTransaction].
    ///
    /// If the transaction uses the `ExplicitBegin` transaction option, this
    /// executes the BeginTransaction RPC. Otherwise, the transaction is started
    /// lazily by the first statement that is executed on it.
    pub async fn build(self) -> crate::Result<StatementBasedReadWriteTransaction> {
        let deadline = self.timeout.map(|t| Instant::now() + t);
        let builder = self
            .builder
            .with_begin_transaction_request_options(self.begin_gax_options)
            .with_commit_request_options(self.commit_gax_options);
        let transaction = builder.build(deadline).await?;
        Ok(StatementBasedReadWriteTransaction {
            builder,
            transaction,
            deadline,
            finished: false,
        })
    }
}

/// A read/write transaction whose lifecycle is controlled by the caller.
///
/// Unlike a [TransactionRunner][crate::transaction::TransactionRunner], which
/// executes the transaction inside a closure and automatically retries the
/// whole transaction when Spanner aborts it, a statement-based transaction
/// gives the caller explicit control: statements are executed directly on the
/// transaction, and the caller decides when to [commit][Self::commit] or
/// [rollback][Self::rollback].
///
/// This is intended for use cases where the transaction lifecycle cannot be
/// expressed as a closure, for example when implementing a connection-oriented
/// database API (with explicit `BEGIN`/`COMMIT` semantics) on top of this
/// client. Applications that can express their transaction as a closure should
/// prefer [read_write_transaction][DatabaseClient::read_write_transaction],
/// which handles aborted-transaction retries automatically.
///
/// # Retrying aborted transactions
///
/// Spanner can abort any read/write transaction at any time. The caller of a
/// statement-based transaction is responsible for handling this: when a
/// statement or the commit fails with an `Aborted` error, call
/// [reset_for_retry][Self::reset_for_retry] and re-execute all statements of
/// the transaction.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_gax::error::rpc::Code;
/// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let mut transaction = db_client.statement_based_read_write_transaction().build().await?;
/// let commit_response = loop {
///     let result = async {
///         let statement = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
///         transaction.execute_update(statement).await?;
///         transaction.commit().await
///     }
///     .await;
///     match result {
///         Ok(response) => break response,
///         Err(e) if e.status().is_some_and(|s| s.code == Code::Aborted) => {
///             transaction.reset_for_retry().await?;
///         }
///         Err(e) => {
///             transaction.rollback().await?;
///             return Err(e);
///         }
///     }
/// };
/// # Ok(())
/// # }
/// ```
///
/// A transaction that is dropped without calling [commit][Self::commit] or
/// [rollback][Self::rollback] does not leak any client-side resources, but it
/// can hold locks on Spanner until the server times the transaction out.
/// Prefer calling [rollback][Self::rollback] on a transaction that will not be
/// committed.
#[derive(Debug)]
pub struct StatementBasedReadWriteTransaction {
    builder: ReadWriteTransactionBuilder,
    transaction: ReadWriteTransaction,
    deadline: Option<Instant>,
    finished: bool,
}

impl StatementBasedReadWriteTransaction {
    fn check_active(&self) -> crate::Result<()> {
        if self.finished {
            return Err(internal_error(
                "the transaction has already been committed or rolled back",
            ));
        }
        Ok(())
    }

    /// Buffers one or more mutations to be applied when the transaction commits.
    ///
    /// See [ReadWriteTransaction::buffer] for more details.
    pub fn buffer<I>(&self, mutations: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Mutation>,
    {
        self.check_active()?;
        self.transaction.buffer(mutations)
    }

    /// Executes a query using this transaction.
    ///
    /// See [ReadWriteTransaction::execute_query] for more details.
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        self.check_active()?;
        self.transaction.execute_query(statement).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple
    /// key/value style alternative to execute_query.
    ///
    /// See [ReadWriteTransaction::execute_read] for more details.
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.check_active()?;
        self.transaction.execute_read(read).await
    }

    /// Executes an update using this transaction and returns the number of
    /// modified rows.
    ///
    /// See [ReadWriteTransaction::execute_update] for more details.
    pub async fn execute_update<T: Into<Statement>>(&self, statement: T) -> crate::Result<i64> {
        self.check_active()?;
        self.transaction.execute_update(statement).await
    }

    /// Executes a batch of DML statements using this transaction.
    ///
    /// See [ReadWriteTransaction::execute_batch_update] for more details.
    pub async fn execute_batch_update<T: Into<BatchDml>>(
        &self,
        batch: T,
    ) -> crate::Result<Vec<i64>> {
        self.check_active()?;
        self.transaction.execute_batch_update(batch).await
    }

    /// Commits the transaction.
    ///
    /// If the commit fails with an `Aborted` error, the transaction can be
    /// retried with [reset_for_retry][Self::reset_for_retry]. Any other error
    /// leaves the transaction in an undetermined state on Spanner; call
    /// [rollback][Self::rollback] to release its locks if it will not be
    /// retried.
    pub async fn commit(&mut self) -> crate::Result<CommitResponse> {
        self.check_active()?;
        let response = self.transaction.clone().commit().await?;
        self.finished = true;
        Ok(response)
    }

    /// Rolls back the transaction.
    ///
    /// Calling rollback on a transaction that has not started on Spanner
    /// (because no statement was executed on it) is a no-op and returns `Ok`.
    pub async fn rollback(&mut self) -> crate::Result<()> {
        self.check_active()?;
        self.transaction.clone().rollback().await?;
        self.finished = true;
        Ok(())
    }

    /// Resets the transaction so it can be retried after an `Aborted` error.
    ///
    /// When a statement or the commit fails with an `Aborted` error, call this
    /// method and then re-execute all statements of the transaction. The retry
    /// references the aborted transaction, which increases the priority of the
    /// retry attempt on Spanner and makes it less likely to be aborted again.
    ///
    /// This method does not wait before returning. Callers that retry in a
    /// tight loop should apply their own backoff between attempts.
    pub async fn reset_for_retry(&mut self) -> crate::Result<()> {
        self.check_active()?;
        let selector = self.transaction.context.transaction_selector.clone();
        let previous_transaction_id = selector.get_id_no_wait().ok().flatten();
        self.builder = self
            .builder
            .clone()
            .set_previous_transaction_id(previous_transaction_id);
        let mut builder = self.builder.clone();
        if selector.is_first_statement_failed() {
            // The first statement of the previous attempt failed, so no
            // statement can start the new transaction inline: begin it
            // explicitly instead.
            builder = builder.with_begin_transaction_option(BeginTransactionOption::ExplicitBegin);
        }
        self.transaction = builder.build(self.deadline).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::spanner::v1;
    use std::fmt::Debug;
    use std::time::Duration as StdDuration;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(
            StatementBasedReadWriteTransactionBuilder: Send,
            Sync,
            Debug
        );
        static_assertions::assert_impl_all!(
            StatementBasedReadWriteTransaction: Send,
            Sync,
            Debug
        );
    }

    fn update_result_set(transaction_id: Option<Vec<u8>>) -> v1::ResultSet {
        let mut metadata = v1::ResultSetMetadata {
            row_type: Some(v1::StructType { fields: vec![] }),
            ..Default::default()
        };
        if let Some(id) = transaction_id {
            metadata.transaction = Some(v1::Transaction {
                id,
                ..Default::default()
            });
        }
        v1::ResultSet {
            metadata: Some(metadata),
            stats: Some(v1::ResultSetStats {
                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn build_transaction(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> (
        StatementBasedReadWriteTransaction,
        tokio::task::JoinHandle<()>,
    ) {
        let (db_client, server) = setup_db_client(mock).await;
        let tx = StatementBasedReadWriteTransactionBuilder::new(db_client)
            .build()
            .await
            .expect("Failed to build transaction");
        (tx, server)
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_commit() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            let selector = req
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .expect("selector required");
            assert!(matches!(
                selector,
                v1::transaction_selector::Selector::Begin(_)
            ));
            Ok(tonic::Response::new(update_result_set(Some(vec![1, 2, 3]))))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (mut tx, _server) = build_transaction(mock).await;

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1);

        let response = tx.commit().await?;
        assert_eq!(
            response
                .commit_timestamp
                .expect("timestamp should be present")
                .seconds(),
            123456789
        );

        // The transaction can no longer be used after a successful commit.
        let err = tx
            .execute_update("UPDATE Users SET Name = 'Bob' WHERE Id = 1")
            .await
            .expect_err("Expected an error after commit");
        assert!(
            format!("{:?}", err).contains("already been committed or rolled back"),
            "Error did not contain expected message: {:?}",
            err
        );
        let err = tx
            .commit()
            .await
            .expect_err("Expected an error on recommit");
        assert!(
            format!("{:?}", err).contains("already been committed or rolled back"),
            "Error did not contain expected message: {:?}",
            err
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_rollback() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(move |_req| {
            Ok(tonic::Response::new(update_result_set(Some(vec![9, 9, 9]))))
        });

        mock.expect_rollback().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.transaction_id, vec![9, 9, 9]);
            Ok(tonic::Response::new(()))
        });

        let (mut tx, _server) = build_transaction(mock).await;

        tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        tx.rollback().await?;

        let err = tx.rollback().await.expect_err("Expected an error");
        assert!(
            format!("{:?}", err).contains("already been committed or rolled back"),
            "Error did not contain expected message: {:?}",
            err
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_rollback_not_started() -> anyhow::Result<()> {
        // Rolling back a transaction that never executed a statement is a
        // client-side no-op: no Rollback RPC is expected on the mock.
        let mock = create_session_mock();
        let (mut tx, _server) = build_transaction(mock).await;
        tx.rollback().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_reset_for_retry_after_aborted_commit() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();

        // First attempt: the statement starts the transaction inline.
        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .expect("selector required");
            assert!(matches!(
                selector,
                v1::transaction_selector::Selector::Begin(_)
            ));
            Ok(tonic::Response::new(update_result_set(Some(vec![0, 0, 7]))))
        });

        // The first commit attempt is aborted by Spanner.
        mock.expect_commit()
            .once()
            .returning(move |_req| Err(create_aborted_status(StdDuration::ZERO)));

        // Second attempt: the statement must reference the aborted transaction
        // as the previous transaction of the retry.
        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .expect("selector required");
            let v1::transaction_selector::Selector::Begin(options) = selector else {
                panic!("Expected a Begin selector in the retry, got {selector:?}");
            };
            let Some(v1::transaction_options::Mode::ReadWrite(rw)) = &options.mode else {
                panic!("Expected read/write options in the retry");
            };
            assert_eq!(
                rw.multiplexed_session_previous_transaction_id,
                vec![0, 0, 7],
                "Expected the retry to reference the aborted transaction"
            );
            Ok(tonic::Response::new(update_result_set(Some(vec![0, 0, 8]))))
        });

        mock.expect_commit().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    0, 0, 8
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (mut tx, _server) = build_transaction(mock).await;

        tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        let err = tx.commit().await.expect_err("Expected an aborted commit");
        assert!(
            crate::transaction_retry_policy::is_aborted(&err),
            "Expected an Aborted error: {:?}",
            err
        );

        tx.reset_for_retry().await?;

        tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        let response = tx.commit().await?;
        assert_eq!(
            response
                .commit_timestamp
                .expect("timestamp should be present")
                .seconds(),
            1000
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_reset_for_retry_after_failed_first_statement()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // The first statement fails with a non-aborted error, so it cannot
        // start the transaction inline.
        mock.expect_execute_sql()
            .once()
            .returning(move |_req| Err(tonic::Status::invalid_argument("Table not found: Users")));

        // The retry cannot rely on a statement to start the transaction
        // inline either, so it must begin the transaction explicitly.
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .expect("selector required");
            assert_eq!(
                selector,
                &v1::transaction_selector::Selector::Id(vec![4, 5, 6])
            );
            Ok(tonic::Response::new(update_result_set(None)))
        });

        mock.expect_commit().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    4, 5, 6
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (mut tx, _server) = build_transaction(mock).await;

        let err = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect_err("Expected the first statement to fail");
        assert!(
            format!("{:?}", err).contains("Table not found"),
            "Error did not contain expected message: {:?}",
            err
        );

        tx.reset_for_retry().await?;

        tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        let response = tx.commit().await?;
        assert_eq!(
            response
                .commit_timestamp
                .expect("timestamp should be present")
                .seconds(),
            1000
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn statement_based_transaction_buffered_mutations_only() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // A transaction with only buffered mutations has no statement to start
        // it inline: the commit path begins it explicitly with a mutation key.
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 7, 7],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.mutations.len(), 1);
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (mut tx, _server) = build_transaction(mock).await;

        let mutation = Mutation::new_insert_builder("Users")
            .set("Id")
            .to(&1)
            .build();
        tx.buffer([mutation])?;
        tx.commit().await?;

        Ok(())
    }
}
