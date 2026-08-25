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

use crate::google;
use crate::model::StorageError;
use gaxi::prost::FromProto;
use gaxi::prost::ToProto;
use google_cloud_rpc::model::Status;
use wkt::message::Message;

/// Converts one detail of a `google.rpc.Status` to its idiomatic form.
///
/// The BigQuery Storage Write API attaches a [StorageError] to the status
/// describing a failure. It is the only way to distinguish, say, a schema
/// mismatch from any other malformed request. `gaxi` only knows the standard
/// `google.rpc.*` detail types, so handle this one here and delegate the rest
/// back to it.
///
/// This converts the status on an `AppendRows` response. Statuses that arrive
/// as a `tonic::Status` never reach this function: `gaxi` has already dropped
/// the detail by then, so
/// [StorageWriteErrorExt::storage_error][crate::write::error::StorageWriteErrorExt::storage_error]
/// decodes the wire status and picks out the detail itself.
fn any_from_prost(value: prost_types::Any) -> Option<wkt::Any> {
    if value.type_url == StorageError::typename() {
        return value
            .to_msg::<google::cloud::bigquery::storage::v1::StorageError>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .and_then(|v| wkt::Any::from_msg(&v).ok());
    }
    gaxi::grpc::status::any_from_prost(value)
}

/// Converts the details of a [Status] to their wire form.
///
/// The inverse of [any_from_prost].
fn any_to_prost(value: wkt::Any) -> Option<prost_types::Any> {
    if value.type_url() == Some(StorageError::typename()) {
        return value
            .to_msg::<StorageError>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .and_then(|v| prost_types::Any::from_msg(&v).ok());
    }
    gaxi::grpc::status::any_to_prost(value)
}

impl ToProto<google::rpc::Status> for Status {
    type Output = google::rpc::Status;
    fn to_proto(self) -> Result<google::rpc::Status, gaxi::prost::ConvertError> {
        Ok(google::rpc::Status {
            code: self.code,
            message: self.message.to_string(),
            details: self.details.into_iter().filter_map(any_to_prost).collect(),
        })
    }
}

impl FromProto<Status> for google::rpc::Status {
    fn cnv(self) -> Result<Status, gaxi::prost::ConvertError> {
        let mut status = Status::new();
        status = status.set_code(self.code);
        status = status.set_message(self.message);
        status = status.set_details(
            self.details
                .into_iter()
                .filter_map(any_from_prost)
                .collect::<Vec<wkt::Any>>(),
        );
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        let got = input.cnv()?;
        let want = Status::new().set_code(12).set_message("test-message");
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn to_proto() -> anyhow::Result<()> {
        let input = Status::new().set_code(12).set_message("test-message");
        let got: google::rpc::Status = input.to_proto()?;
        let want = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        assert_eq!(got, want);
        Ok(())
    }

    fn prost_storage_error() -> google::cloud::bigquery::storage::v1::StorageError {
        use google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
        google::cloud::bigquery::storage::v1::StorageError {
            code: StorageErrorCode::SchemaMismatchExtraFields as i32,
            entity: "projects/p/datasets/d/tables/t".into(),
            error_message: "the schema does not match".into(),
        }
    }

    fn storage_error() -> StorageError {
        use crate::model::storage_error::StorageErrorCode;
        StorageError::new()
            .set_code(StorageErrorCode::SchemaMismatchExtraFields)
            .set_entity("projects/p/datasets/d/tables/t")
            .set_error_message("the schema does not match")
    }

    #[test]
    fn from_proto_storage_error() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 3,
            message: "test-message".into(),
            details: vec![prost_types::Any::from_msg(&prost_storage_error())?],
        };
        let got = input.cnv()?;
        let want = Status::new()
            .set_code(3)
            .set_message("test-message")
            .set_details([wkt::Any::from_msg(&storage_error())?]);
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn to_proto_storage_error() -> anyhow::Result<()> {
        let input = Status::new()
            .set_code(3)
            .set_message("test-message")
            .set_details([wkt::Any::from_msg(&storage_error())?]);
        let got: google::rpc::Status = input.to_proto()?;
        let want = google::rpc::Status {
            code: 3,
            message: "test-message".into(),
            details: vec![prost_types::Any::from_msg(&prost_storage_error())?],
        };
        assert_eq!(got, want);
        Ok(())
    }

    /// The standard `google.rpc.*` details are still converted by `gaxi`.
    #[test]
    fn standard_details_round_trip() -> anyhow::Result<()> {
        use google_cloud_rpc::model::DebugInfo;
        let want = Status::new()
            .set_code(3)
            .set_message("test-message")
            .set_details([
                wkt::Any::from_msg(&DebugInfo::new().set_detail("test-detail"))?,
                wkt::Any::from_msg(&storage_error())?,
            ]);
        let pb: google::rpc::Status = want.clone().to_proto()?;
        assert_eq!(pb.details.len(), 2, "{pb:?}");
        let got = pb.cnv()?;
        assert_eq!(got, want);
        Ok(())
    }

    /// Details for types the service does not send on a status are dropped.
    #[test]
    fn unknown_details_are_dropped() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 3,
            message: "test-message".into(),
            details: vec![prost_types::Any::from_msg(
                &google::cloud::bigquery::storage::v1::RowError {
                    index: 1,
                    ..Default::default()
                },
            )?],
        };
        assert_eq!(input.cnv()?.details, Vec::new());

        let input = Status::new()
            .set_code(3)
            .set_message("test-message")
            .set_details([wkt::Any::from_msg(
                &crate::model::RowError::new().set_index(1),
            )?]);
        let got: google::rpc::Status = input.to_proto()?;
        assert_eq!(got.details, Vec::new());
        Ok(())
    }
}
