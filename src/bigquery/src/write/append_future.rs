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

use super::append_response::{AppendResponse, proto_to_result};
use super::error::{AppendError, AppendResult};
use crate::google::cloud::bigquery::storage::v1;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A future that resolves to the result of an async append operation.
///
/// This future represents a write request that the client library has already
/// queued to send over the network. Awaiting this future yields the server's
/// acknowledgment or an error if the write fails.
///
/// Because the request is queued before the future is created, dropping the
/// future does not cancel it: the rows may still be appended to the stream, as
/// the service correlates its responses by their order on the stream.
///
/// The exception is a request the client library could not convert. Such a
/// request never reaches the queue, and the future only carries the conversion
/// error.
#[derive(Debug)]
pub struct AppendFuture {
    rx: oneshot::Receiver<AppendResult<v1::AppendRowsResponse>>,
}

impl AppendFuture {
    pub(crate) fn new(rx: oneshot::Receiver<AppendResult<v1::AppendRowsResponse>>) -> Self {
        Self { rx }
    }

    /// A future for a request that could not be queued, resolving to `error`.
    pub(crate) fn from_error(error: AppendError) -> Self {
        let (tx, rx) = oneshot::channel();
        tx.send(Err(error))
            .expect("sending on channel always succeeds");
        Self::new(rx)
    }
}

impl Future for AppendFuture {
    type Output = AppendResult<AppendResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match std::task::ready!(Pin::new(&mut self.rx).poll(cx)) {
            Ok(resp) => Poll::Ready(resp.and_then(proto_to_result)),
            // The runner dropped our response channel without a response.
            Err(_) => Poll::Ready(Err(AppendError::UnexpectedEndOfStream)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::google::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult as ProtoAppendResult, Response,
    };
    use crate::model::TableSchema;

    #[tokio::test]
    async fn happy_path() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(v1::AppendRowsResponse {
            response: Some(Response::AppendResult(ProtoAppendResult {
                offset: Some(42),
            })),
            updated_schema: Some(v1::TableSchema::default()),
            ..Default::default()
        }));
        let future = AppendFuture::new(rx);
        let resp = future.await.expect("should succeed");
        assert_eq!(resp.offset, Some(42));
        assert_eq!(resp.updated_schema, Some(TableSchema::default()));
    }

    #[tokio::test]
    async fn dropped_sender() {
        let (tx, rx) = oneshot::channel::<AppendResult<v1::AppendRowsResponse>>();
        // Drop the sender immediately
        drop(tx);

        let future = AppendFuture::new(rx);
        let err = future
            .await
            .expect_err("should return unexpected end of stream");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
    }

    #[tokio::test]
    async fn channel_returns_error() {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AppendError::UnexpectedEndOfStream));
        let future = AppendFuture::new(rx);
        let err = future
            .await
            .expect_err("should return the error from the channel");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));
    }

    #[tokio::test]
    async fn never_queued() {
        let future = AppendFuture::from_error(Error::deser("fail").into());
        let err = future.await.expect_err("should return error");
        assert!(matches!(err, AppendError::Rpc { source: _ }));
    }
}
