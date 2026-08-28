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

use crate::Error;
use crate::model::{RowError, StorageError};
use gaxi::as_inner::as_inner;
use gaxi::grpc::tonic::Status as TonicStatus;
use gaxi::prost::FromProto;
use google_cloud_gax::error::rpc::{Status, StatusDetails};
use prost::Message;

/// Represents an error that can occur when appending rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AppendError {
    /// The underlying RPC failed.
    ///
    /// Use [storage_error][AppendError::storage_error] to get the
    /// BigQuery-specific details about the failure, if any.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[from]
        #[source]
        source: Error,
    },

    /// Certain rows have errors.
    #[error(
        "the service reports an error for the following rows. No rows in the batch were appended. You can remove the bad rows and retry the request. Rows: {0:?}"
    )]
    RowErrors(Vec<RowError>),

    /// The `AppendRows` stream closed unexpectedly.
    #[error(
        "the `AppendRows` stream closed unexpectedly and the client library could not recover."
    )]
    UnexpectedEndOfStream,
}

impl AppendError {
    /// The BigQuery-specific details about the failure, if the service sent
    /// any.
    ///
    /// See [StorageWriteErrorExt::storage_error][crate::error::StorageWriteErrorExt::storage_error].
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::error::AppendError;
    /// use google_cloud_bigquery::model::storage_error::StorageErrorCode;
    /// fn is_schema_mismatch(err: &AppendError) -> bool {
    ///     matches!(
    ///         err.storage_error().map(|e| e.code),
    ///         Some(StorageErrorCode::SchemaMismatchExtraFields)
    ///     )
    /// }
    /// ```
    pub fn storage_error(&self) -> Option<StorageError> {
        let Self::Rpc { source } = self else {
            return None;
        };
        source.storage_error()
    }
}

mod sealed {
    /// A sealed trait to prevent external implementation of `StorageWriteErrorExt`.
    pub trait StorageWriteErrorExt {}
    impl StorageWriteErrorExt for crate::Error {}
}

/// An extension trait for [Error] to examine the BigQuery-specific details of a
/// Storage Write failure.
///
/// This trait is sealed and cannot be implemented for types outside of this
/// crate.
pub trait StorageWriteErrorExt: sealed::StorageWriteErrorExt {
    /// The BigQuery-specific details about a Storage Write failure, if the
    /// service sent any.
    ///
    /// The service reports failures with a generic RPC code, such as
    /// `INVALID_ARGUMENT`. When it can be more specific it attaches a
    /// [StorageError] to the status. Applications use its
    /// [code][StorageError::code] to distinguish, say, a table schema mismatch
    /// from any other malformed request.
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::error::StorageWriteErrorExt;
    /// use google_cloud_bigquery::model::storage_error::StorageErrorCode;
    /// fn is_already_committed(err: &google_cloud_bigquery::Error) -> bool {
    ///     matches!(
    ///         err.storage_error().map(|e| e.code),
    ///         Some(StorageErrorCode::StreamAlreadyCommitted)
    ///     )
    /// }
    /// ```
    fn storage_error(&self) -> Option<StorageError>;
}

impl StorageWriteErrorExt for Error {
    fn storage_error(&self) -> Option<StorageError> {
        // A failure reported on the `AppendRows` response arrives as a
        // converted status; `write::status` knows this detail type.
        if let Some(status) = self.status()
            && let Some(e) = from_status(status)
        {
            return Some(e);
        }
        // Everything else arrives as a `tonic::Status`. `gaxi` drops the
        // details it does not know, so decode the wire status again and pick
        // out the detail.
        let status = as_inner::<TonicStatus, _>(self)?;
        let pb = crate::google::rpc::Status::decode(status.details()).ok()?;
        pb.details
            .iter()
            .find_map(|d| {
                d.to_msg::<crate::google::cloud::bigquery::storage::v1::StorageError>()
                    .ok()
            })
            .and_then(|v| v.cnv().ok())
    }
}

/// Extracts the [StorageError] from the converted details of a status.
fn from_status(status: &Status) -> Option<StorageError> {
    status.details.iter().find_map(|d| match d {
        StatusDetails::Other(any) => any.to_msg::<StorageError>().ok(),
        _ => None,
    })
}

pub(crate) type AppendResult<T> = std::result::Result<T, AppendError>;

/// Represents an error that can occur when attaching to an existing stream.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AttachError {
    /// The stream type provided by the service did not match the expected type.
    #[error("stream type mismatch: requested {expected:?}, but matched resource yields {actual:?}")]
    TypeMismatch {
        /// The expected stream type.
        expected: crate::model::write_stream::Type,
        /// The actual stream type returned by the service.
        actual: crate::model::write_stream::Type,
    },

    /// The underlying RPC failed.
    ///
    /// Use [storage_error][AttachError::storage_error] to get the
    /// BigQuery-specific details about the failure, if any.
    #[non_exhaustive]
    #[error("the operation failed. RPC error: {source}")]
    Rpc {
        /// The error returned by the service for the request.
        #[from]
        #[source]
        source: Error,
    },
}

impl AttachError {
    /// The BigQuery-specific details about the failure, if the service sent
    /// any.
    ///
    /// See [StorageWriteErrorExt::storage_error][crate::error::StorageWriteErrorExt::storage_error].
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::error::AttachError;
    /// use google_cloud_bigquery::model::storage_error::StorageErrorCode;
    /// fn is_stream_finalized(err: &AttachError) -> bool {
    ///     matches!(
    ///         err.storage_error().map(|e| e.code),
    ///         Some(StorageErrorCode::StreamFinalized)
    ///     )
    /// }
    /// ```
    pub fn storage_error(&self) -> Option<StorageError> {
        let Self::Rpc { source } = self else {
            return None;
        };
        source.storage_error()
    }
}

pub(crate) type AttachResult<T> = std::result::Result<T, AttachError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_error::StorageErrorCode;
    use gaxi::prost::ToProto;
    use google_cloud_gax::error::rpc::Code;

    fn storage_error() -> StorageError {
        StorageError::new()
            .set_code(StorageErrorCode::SchemaMismatchExtraFields)
            .set_entity("projects/p/datasets/d/tables/t")
            .set_error_message("the schema does not match")
    }

    #[test]
    fn append_error_rpc_debug() {
        let e = AppendError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("inner fail"),
            ),
        };
        let fmt = format!("{e}");
        assert!(fmt.contains("operation failed."), "{fmt}");
        assert!(fmt.contains("inner fail"), "{fmt}");
    }

    /// The service reports most append failures on the response for the
    /// append. This crate converts the details of those statuses.
    #[test]
    fn storage_error_for_append() -> anyhow::Result<()> {
        let e = AppendError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message("fail")
                    .set_details([wkt::Any::from_msg(&storage_error())?]),
            ),
        };
        assert_eq!(e.storage_error(), Some(storage_error()));
        Ok(())
    }

    /// The service may also terminate the stream with a `StorageError`.
    #[test]
    fn storage_error_for_stream() -> anyhow::Result<()> {
        let pb = crate::google::rpc::Status {
            code: Code::InvalidArgument as i32,
            message: "fail".into(),
            details: vec![prost_types::Any::from_msg(&storage_error().to_proto()?)?],
        };
        let status = gaxi::grpc::tonic::Status::with_details(
            gaxi::grpc::tonic::Code::InvalidArgument,
            "fail",
            pb.encode_to_vec().into(),
        );
        let e = AppendError::from(gaxi::grpc::from_status::to_gax_error(status));
        assert_eq!(e.storage_error(), Some(storage_error()));
        Ok(())
    }

    #[test]
    fn storage_error_missing() -> anyhow::Result<()> {
        assert_eq!(AppendError::UnexpectedEndOfStream.storage_error(), None);
        assert_eq!(AppendError::RowErrors(Vec::new()).storage_error(), None);

        let e: AppendError = Error::io("fail").into();
        assert_eq!(e.storage_error(), None);

        // A status without details, and one with details of a different type.
        let e = AppendError::Rpc {
            source: Error::service(Status::default().set_code(Code::InvalidArgument)),
        };
        assert_eq!(e.storage_error(), None);
        let e = AppendError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_details([wkt::Any::from_msg(&RowError::new().set_index(1))?]),
            ),
        };
        assert_eq!(e.storage_error(), None);

        // A `tonic::Status` with details that do not decode.
        let status = gaxi::grpc::tonic::Status::with_details(
            gaxi::grpc::tonic::Code::InvalidArgument,
            "with bad details",
            bytes::Bytes::from_static(b"\x01"),
        );
        let e = AppendError::from(gaxi::grpc::from_status::to_gax_error(status));
        assert_eq!(e.storage_error(), None);
        Ok(())
    }

    #[test]
    fn storage_error_for_attach() -> anyhow::Result<()> {
        use crate::model::write_stream::Type;

        let e = AttachError::Rpc {
            source: Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("fail")
                    .set_details([wkt::Any::from_msg(&storage_error())?]),
            ),
        };
        assert_eq!(e.storage_error(), Some(storage_error()));

        let e = AttachError::TypeMismatch {
            expected: Type::Pending,
            actual: Type::Committed,
        };
        assert_eq!(e.storage_error(), None);
        Ok(())
    }
}
