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

use super::append_future::AppendFuture;
use super::runner::WriteRequest;
use crate::Error;
use crate::model::AppendRowsRequest;
use gaxi::prost::ToProto;
use tokio::sync::{mpsc, oneshot};

/// A request builder for appending rows with a specific stream offset,
/// ensuring exactly-once semantics.
#[derive(Clone, Debug)]
pub struct AppendWithOffset {
    req_tx: mpsc::UnboundedSender<WriteRequest>,
    pub(crate) req: AppendRowsRequest,
}

impl AppendWithOffset {
    #[allow(dead_code)]
    pub(crate) fn new(req_tx: mpsc::UnboundedSender<WriteRequest>, req: AppendRowsRequest) -> Self {
        Self { req_tx, req }
    }

    /// Sets the target stream offset to guarantee [exactly-once] writes.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::write::arrow::PendingWriter;
    /// # async fn sample(writer: PendingWriter) -> anyhow::Result<()> {
    /// let resp = writer.append(rows()).set_offset(0).send().await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowRecordBatch;
    /// fn rows() -> ArrowRecordBatch {
    ///   todo!("Define your rows...")
    /// }
    /// ```
    ///
    /// [exactly-once]: https://docs.cloud.google.com/bigquery/docs/write-api-best-practices#manage_stream_offsets_to_achieve_exactly-once_semantics
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.req.offset = Some(offset);
        self
    }

    /// Append rows to the stream.
    ///
    /// Applications are encouraged to queue up requests and await their
    /// responses independently.
    ///
    /// Note that the service will reject requests with a mismatched offset.
    /// Each call queues its request before returning, so calls made from a
    /// single task queue in the order they are made. Calls made concurrently
    /// from several tasks have no defined order, and the application must
    /// synchronize them to control the offsets.
    ///
    /// Dropping the returned [AppendFuture] does not cancel the request: the
    /// rows may still be appended to the stream.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::write::arrow::PendingWriter;
    /// # async fn sample(writer: PendingWriter) -> anyhow::Result<()> {
    /// let f1 = writer.append(rows()).set_offset(0).send();
    /// let f2 = writer.append(rows()).set_offset(1).send();
    ///
    /// let resp1 = f1.await?;
    /// let resp2 = f2.await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowRecordBatch;
    /// fn rows() -> ArrowRecordBatch {
    ///   todo!("Define your rows...")
    /// }
    /// ```
    pub fn send(self) -> AppendFuture {
        queue_append_request(&self.req_tx, self.req)
    }
}

/// A request builder for appending rows on the default stream.
#[derive(Clone, Debug)]
pub struct Append {
    req_tx: mpsc::UnboundedSender<WriteRequest>,
    pub(crate) req: AppendRowsRequest,
}

impl Append {
    pub(crate) fn new(req_tx: mpsc::UnboundedSender<WriteRequest>, req: AppendRowsRequest) -> Self {
        Self { req_tx, req }
    }

    /// Append rows to the stream.
    ///
    /// Applications are encouraged to queue up requests and await their
    /// responses independently.
    ///
    /// Each call queues its request before returning, so calls made from a
    /// single task append their rows in the order they are made. Calls made
    /// concurrently from several tasks have no defined order.
    ///
    /// Dropping the returned [AppendFuture] does not cancel the request: the
    /// rows may still be appended to the stream.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::write::arrow::DefaultWriter;
    /// # async fn sample(writer: DefaultWriter) -> anyhow::Result<()> {
    /// let f1 = writer.append(rows()).send();
    /// let f2 = writer.append(rows()).send();
    ///
    /// let resp1 = f1.await?;
    /// let resp2 = f2.await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowRecordBatch;
    /// fn rows() -> ArrowRecordBatch {
    ///   todo!("Define your rows...")
    /// }
    /// ```
    pub fn send(self) -> AppendFuture {
        queue_append_request(&self.req_tx, self.req)
    }
}

/// Performs the proto translation for an `AppendRowsRequest`, and queues it for
/// transmission on the `AppendRows` stream.
///
/// The request is placed on the stream's queue before this function returns.
/// Consecutive calls therefore reach the service in the order the application
/// made them, as required to write with explicit offsets.
fn queue_append_request(
    req_tx: &mpsc::UnboundedSender<WriteRequest>,
    req: AppendRowsRequest,
) -> AppendFuture {
    let req = match req.to_proto() {
        Ok(req) => req,
        Err(e) => return AppendFuture::from_error(Error::deser(e).into()),
    };
    let (resp_tx, resp_rx) = oneshot::channel();
    // If the stream has shut down, the response channel is dropped along with
    // the request, and the future resolves to `UnexpectedEndOfStream`.
    let _ = req_tx.send(WriteRequest { req, resp_tx });
    AppendFuture::new(resp_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1;
    use crate::google::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult, Response,
    };
    use crate::model::TableSchema;
    use crate::write::error::AppendError;

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = Append::new(req_tx, req);
        let future = builder.send();

        // Receive and verify the request
        let write = req_rx.recv().await.expect("should receive request");
        assert_eq!(write.req.write_stream, write_stream());

        // Provide a successful response
        let resp = v1::AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult::default())),
            write_stream: write_stream(),
            updated_schema: Some(v1::TableSchema::default()),
            ..Default::default()
        };
        write
            .resp_tx
            .send(Ok(resp))
            .expect("sending on channel always succeeds");

        let resp = future.await?;
        assert_eq!(resp.offset, None);
        assert_eq!(resp.updated_schema, Some(TableSchema::default()));
        Ok(())
    }

    #[tokio::test]
    async fn stream_closed() -> anyhow::Result<()> {
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = Append::new(req_tx, req);
        let future = builder.send();

        // Simulate a stream closure
        drop(req_rx);

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
        Ok(())
    }

    #[tokio::test]
    async fn rpc_error() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = Append::new(req_tx, req);
        let future = builder.send();

        // Simulate a stream ending in a known error
        let write = req_rx.recv().await.expect("should receive request");
        let append_err: AppendError = Error::io("fail").into();
        write
            .resp_tx
            .send(Err(append_err))
            .expect("sending on channel always succeeds");

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
        Ok(())
    }

    #[tokio::test]
    async fn row_errors() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = Append::new(req_tx, req);
        let future = builder.send();

        let write = req_rx.recv().await.expect("should receive request");

        let row_error = v1::RowError {
            index: 42,
            code: v1::row_error::RowErrorCode::FieldsError as i32,
            message: "fail".to_string(),
        };
        let resp = v1::AppendRowsResponse {
            row_errors: vec![row_error],
            write_stream: write_stream(),
            ..Default::default()
        };
        write
            .resp_tx
            .send(Ok(resp))
            .expect("sending on channel always succeeds");

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::RowErrors(_)));
        Ok(())
    }

    #[test]
    fn queued_before_send_returns() {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        // Note that the future is never polled, and that this is not a
        // `#[tokio::test]`: `send()` must not need an ambient runtime.
        let _future = Append::new(req_tx, req).send();

        let write = req_rx.try_recv().expect("request should already be queued");
        assert_eq!(write.req.write_stream, write_stream());
    }

    #[test]
    fn dropped_future_keeps_request_queued() {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        // Dropping the future cannot withdraw the request. The service
        // correlates its responses by position on the stream.
        drop(Append::new(req_tx, req).send());

        let write = req_rx.try_recv().expect("request should still be queued");
        assert_eq!(write.req.write_stream, write_stream());

        // The runner ignores the error from the abandoned response channel.
        let resp = v1::AppendRowsResponse::default();
        assert!(write.resp_tx.send(Ok(resp)).is_err(), "receiver is gone");
    }

    #[tokio::test]
    async fn conversion_error() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();

        // Keep a sender alive, so that an empty queue reads as empty rather
        // than as disconnected.
        let future = Append::new(req_tx.clone(), unconvertible_request()).send();

        // A request we cannot convert must not take a place on the stream.
        let err = req_rx.try_recv().expect_err("nothing should be queued");
        assert!(matches!(err, mpsc::error::TryRecvError::Empty));

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
        Ok(())
    }

    #[tokio::test]
    async fn offset_success() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = AppendWithOffset::new(req_tx, req).set_offset(100);
        let future = builder.send();

        let write = req_rx.recv().await.expect("should receive request");
        assert_eq!(write.req.offset, Some(100));

        let resp = v1::AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult::default())),
            write_stream: write_stream(),
            // Ensure schema matches none
            ..Default::default()
        };
        write
            .resp_tx
            .send(Ok(resp))
            .expect("sending on channel always succeeds");

        let resp = future.await?;
        assert_eq!(resp.offset, None);
        assert_eq!(resp.updated_schema, None);
        Ok(())
    }

    #[tokio::test]
    async fn offset_stream_closed() -> anyhow::Result<()> {
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = AppendWithOffset::new(req_tx, req).set_offset(100);
        let future = builder.send();

        drop(req_rx);

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
        Ok(())
    }

    #[tokio::test]
    async fn offset_rpc_error() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = AppendWithOffset::new(req_tx, req).set_offset(100);
        let future = builder.send();

        // Simulate a stream ending in a known error
        let write = req_rx.recv().await.expect("should receive request");
        let append_err: AppendError = Error::io("fail").into();
        write
            .resp_tx
            .send(Err(append_err))
            .expect("sending on channel always succeeds");

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
        Ok(())
    }

    #[tokio::test]
    async fn offset_row_errors() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        let builder = AppendWithOffset::new(req_tx, req).set_offset(100);
        let future = builder.send();

        let write = req_rx.recv().await.expect("should receive request");

        let row_error = v1::RowError {
            index: 42,
            code: v1::row_error::RowErrorCode::FieldsError as i32,
            message: "fail".to_string(),
        };
        let resp = v1::AppendRowsResponse {
            row_errors: vec![row_error],
            write_stream: write_stream(),
            ..Default::default()
        };
        write
            .resp_tx
            .send(Ok(resp))
            .expect("sending on channel always succeeds");

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::RowErrors(_)));
        Ok(())
    }

    #[test]
    fn offset_queued_before_send_returns() {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        // Note that the future is never polled, and that this is not a
        // `#[tokio::test]`: `send()` must not need an ambient runtime.
        let _future = AppendWithOffset::new(req_tx, req).set_offset(100).send();

        let write = req_rx.try_recv().expect("request should already be queued");
        assert_eq!(write.req.offset, Some(100));
    }

    #[test]
    fn offset_dropped_future_keeps_request_queued() {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let req = AppendRowsRequest::new().set_write_stream(write_stream());

        // Dropping the future cannot withdraw the request. The service
        // correlates its responses by position on the stream.
        drop(AppendWithOffset::new(req_tx, req).set_offset(100).send());

        let write = req_rx.try_recv().expect("request should still be queued");
        assert_eq!(write.req.offset, Some(100));

        // The runner ignores the error from the abandoned response channel.
        let resp = v1::AppendRowsResponse::default();
        assert!(write.resp_tx.send(Ok(resp)).is_err(), "receiver is gone");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn synchronous_queueing() -> anyhow::Result<()> {
        const NUM_WRITES: i64 = 1000;
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();
        let write_handle = tokio::spawn(async move {
            let mut writes = tokio::task::JoinSet::new();
            for i in 0..NUM_WRITES {
                writes.spawn(
                    AppendWithOffset::new(req_tx.clone(), AppendRowsRequest::new())
                        .set_offset(i)
                        .send(),
                );
            }
            let _ = writes.join_all().await;
        });

        for i in 0..NUM_WRITES {
            let write = req_rx.recv().await.expect("should receive request");
            assert_eq!(write.req.offset, Some(i), "received out of order write");
        }
        write_handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn offset_conversion_error() -> anyhow::Result<()> {
        let (req_tx, mut req_rx) = mpsc::unbounded_channel();

        // Keep a sender alive, so that an empty queue reads as empty rather
        // than as disconnected.
        let future = AppendWithOffset::new(req_tx.clone(), unconvertible_request())
            .set_offset(100)
            .send();

        // A request we cannot convert must not take a place on the stream.
        let err = req_rx.try_recv().expect_err("nothing should be queued");
        assert!(matches!(err, mpsc::error::TryRecvError::Empty));

        let err = future.await.expect_err("should return an error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
        Ok(())
    }

    /// A request the client library cannot convert to its proto representation.
    /// An enum value it does not know has no integer to put on the wire.
    fn unconvertible_request() -> AppendRowsRequest {
        AppendRowsRequest::new()
            .set_write_stream(write_stream())
            .set_default_missing_value_interpretation("NOT_A_MISSING_VALUE_INTERPRETATION")
    }

    fn write_stream() -> String {
        "projects/p/datasets/d/tables/t/streams/_default".to_string()
    }
}
