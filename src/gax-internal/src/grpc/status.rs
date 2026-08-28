// Copyright 2025 Google LLC
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
use crate::prost::{FromProto, ToProto};
use google_cloud_rpc::model::Status;
use google_cloud_rpc::model::{
    BadRequest, DebugInfo, ErrorInfo, Help, LocalizedMessage, PreconditionFailure, QuotaFailure,
    RequestInfo, ResourceInfo, RetryInfo,
};

/// Converts one service-specific status detail between its wire and idiomatic
/// forms.
///
/// This library only knows the standard `google.rpc.*` detail types. Services
/// that attach their own messages to a status, such as
/// `google.cloud.bigquery.storage.v1.StorageError`, describe them with this
/// type and supply them through [DetailConverters].
///
/// Each function returns `None` when it does not recognize the detail, which
/// lets the caller try the next converter.
#[derive(Clone, Copy, Debug)]
pub struct DetailConverter {
    /// Converts the detail from its wire form.
    pub from_prost: fn(&prost_types::Any) -> Option<wkt::Any>,
    /// Converts the detail to its wire form.
    pub to_prost: fn(&wkt::Any) -> Option<prost_types::Any>,
}

impl DetailConverter {
    /// Describes a detail type by its Protobuf-generated type `P` and its
    /// idiomatic type `T`.
    ///
    /// # Example
    /// ```text
    /// use google_cloud_gax_internal::grpc::status::{DetailConverter, DetailConverters};
    ///
    /// const CONVERTERS: DetailConverters = DetailConverters(&[
    ///     DetailConverter::new::<proto::StorageError, model::StorageError>(),
    /// ]);
    /// ```
    pub const fn new<P, T>() -> Self
    where
        P: prost::Message + prost::Name + Default + FromProto<T>,
        T: wkt::message::Message + ToProto<P, Output = P>,
    {
        Self {
            from_prost: from_prost::<P, T>,
            to_prost: to_prost::<P, T>,
        }
    }
}

fn from_prost<P, T>(value: &prost_types::Any) -> Option<wkt::Any>
where
    P: prost::Message + prost::Name + Default + FromProto<T>,
    T: wkt::message::Message,
{
    // `to_msg` verifies the type URL, so a converter for a different type
    // simply yields `None`.
    let msg = value.to_msg::<P>().ok()?;
    wkt::Any::from_msg(&msg.cnv().ok()?).ok()
}

fn to_prost<P, T>(value: &wkt::Any) -> Option<prost_types::Any>
where
    P: prost::Message + prost::Name,
    T: wkt::message::Message + ToProto<P, Output = P>,
{
    // As above, `to_msg` verifies the type URL.
    let msg = value.to_msg::<T>().ok()?;
    prost_types::Any::from_msg(&msg.to_proto().ok()?).ok()
}

/// The service-specific status details a client knows how to convert.
///
/// Clients that need this supply it as a [ClientConfig] extension. It then
/// applies to every status the client converts, whether the status arrives as
/// a `tonic::Status` or as a field of a response message.
///
/// [ClientConfig]: crate::options::ClientConfig
#[derive(Clone, Copy, Debug, Default)]
pub struct DetailConverters(pub &'static [DetailConverter]);

impl DetailConverters {
    /// No service-specific details; only the standard `google.rpc.*` types are
    /// converted.
    pub const NONE: Self = Self(&[]);
}

pub(crate) fn status_from_proto(s: google::rpc::Status, extra: DetailConverters) -> Status {
    Status::new()
        .set_code(s.code)
        .set_message(s.message)
        .set_details(
            s.details
                .into_iter()
                .filter_map(|d| any_from_prost_with(&d, extra)),
        )
}

/// Converts a status detail from its wire form, including the service-specific
/// types in `extra`.
pub fn any_from_prost_with(value: &prost_types::Any, extra: DetailConverters) -> Option<wkt::Any> {
    if let Some(any) = extra.0.iter().find_map(|c| (c.from_prost)(value)) {
        return Some(any);
    }
    any_from_prost(value.clone())
}

/// Converts a status detail to its wire form, including the service-specific
/// types in `extra`.
pub fn any_to_prost_with(value: &wkt::Any, extra: DetailConverters) -> Option<prost_types::Any> {
    if let Some(any) = extra.0.iter().find_map(|c| (c.to_prost)(value)) {
        return Some(any);
    }
    any_to_prost(value.clone())
}

pub fn any_to_prost(value: wkt::Any) -> Option<prost_types::Any> {
    let mapped = value.type_url().map(|url| match url {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .to_msg::<BadRequest>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .to_msg::<DebugInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .to_msg::<ErrorInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.Help" => value
            .to_msg::<Help>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .to_msg::<LocalizedMessage>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .to_msg::<PreconditionFailure>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .to_msg::<QuotaFailure>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .to_msg::<RequestInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .to_msg::<ResourceInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .to_msg::<RetryInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        _ => None,
    });
    mapped.flatten().transpose().ok().flatten()
}

pub fn any_from_prost(value: prost_types::Any) -> Option<wkt::Any> {
    let mapped = match value.type_url.as_str() {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .to_msg::<google::rpc::BadRequest>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .to_msg::<google::rpc::DebugInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .to_msg::<google::rpc::ErrorInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.Help" => value
            .to_msg::<google::rpc::Help>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .to_msg::<google::rpc::LocalizedMessage>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .to_msg::<google::rpc::PreconditionFailure>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .to_msg::<google::rpc::QuotaFailure>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .to_msg::<google::rpc::RequestInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .to_msg::<google::rpc::ResourceInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .to_msg::<google::rpc::RetryInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        _ => None,
    };
    mapped.transpose().ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    // `google.rpc.BadRequest.FieldViolation` is a real message that is not one
    // of the ten status details this library knows, which makes it a good
    // stand-in for a service-specific detail type.
    type ProtoDetail = google::rpc::bad_request::FieldViolation;
    type ModelDetail = google_cloud_rpc::model::bad_request::FieldViolation;

    const CONVERTERS: DetailConverters =
        DetailConverters(&[DetailConverter::new::<ProtoDetail, ModelDetail>()]);

    fn model_detail() -> ModelDetail {
        ModelDetail::new()
            .set_field("field")
            .set_description("desc")
    }

    fn proto_detail() -> ProtoDetail {
        #[allow(clippy::needless_update)]
        ProtoDetail {
            field: "field".into(),
            description: "desc".into(),
            ..Default::default()
        }
    }

    /// Without a converter the detail is dropped, with one it survives.
    #[test]
    fn extra_detail_from_prost() -> anyhow::Result<()> {
        let input = prost_types::Any::from_msg(&proto_detail())?;
        assert_eq!(any_from_prost_with(&input, DetailConverters::NONE), None);
        assert_eq!(
            any_from_prost_with(&input, CONVERTERS),
            Some(wkt::Any::from_msg(&model_detail())?)
        );
        Ok(())
    }

    #[test]
    fn extra_detail_to_prost() -> anyhow::Result<()> {
        let input = wkt::Any::from_msg(&model_detail())?;
        assert_eq!(any_to_prost_with(&input, DetailConverters::NONE), None);
        assert_eq!(
            any_to_prost_with(&input, CONVERTERS),
            Some(prost_types::Any::from_msg(&proto_detail())?)
        );
        Ok(())
    }

    /// The standard details are still converted when extra converters exist,
    /// and details that match no converter are still dropped.
    #[test]
    fn extra_converters_do_not_shadow_the_standard_ones() -> anyhow::Result<()> {
        use google::rpc::Help;
        let standard = prost_types::Any::from_msg(&ErrorInfo::default().to_proto()?)?;
        assert!(any_from_prost_with(&standard, CONVERTERS).is_some());

        // `Help` is a standard detail, `help::Link` is not, and neither has a
        // converter in `CONVERTERS`.
        let unknown = prost_types::Any::from_msg(&google::rpc::help::Link::default())?;
        assert_eq!(any_from_prost_with(&unknown, CONVERTERS), None);
        let known = prost_types::Any::from_msg(&Help::default())?;
        assert!(any_from_prost_with(&known, CONVERTERS).is_some());
        Ok(())
    }

    #[test]
    fn status_from_proto_keeps_extra_details() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 3,
            message: "test-only".into(),
            details: vec![
                prost_types::Any::from_msg(&ErrorInfo::default().to_proto()?)?,
                prost_types::Any::from_msg(&proto_detail())?,
            ],
        };
        let got = status_from_proto(input.clone(), CONVERTERS);
        assert_eq!(
            got.details,
            vec![
                wkt::Any::from_msg(&ErrorInfo::default())?,
                wkt::Any::from_msg(&model_detail())?
            ]
        );

        // The same status keeps only the standard detail without converters.
        let got = status_from_proto(input, DetailConverters::NONE);
        assert_eq!(
            got.details,
            vec![wkt::Any::from_msg(&ErrorInfo::default())?]
        );
        Ok(())
    }

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let got: Vec<wkt::Any> = prost_details()
            .into_iter()
            .filter_map(any_from_prost)
            .collect();
        assert_eq!(got, wkt_details());
        Ok(())
    }

    #[test]
    fn to_proto() -> anyhow::Result<()> {
        let got: Vec<prost_types::Any> =
            wkt_details().into_iter().filter_map(any_to_prost).collect();
        assert_eq!(got, prost_details());
        Ok(())
    }

    fn prost_details() -> Vec<prost_types::Any> {
        use google::rpc::*;
        use prost_types::Any;
        // We do not want our CI to break if/when the protos grow.
        #[allow(clippy::needless_update)]
        let from_msg = vec![
            Any::from_msg(&BadRequest {
                field_violations: vec![bad_request::FieldViolation {
                    field: "field".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&DebugInfo {
                stack_entries: ["stack"].map(str::to_string).to_vec(),
                detail: "detail".into(),
                ..Default::default()
            }),
            Any::from_msg(&ErrorInfo {
                reason: "reason".into(),
                domain: "domain".into(),
                ..Default::default()
            }),
            Any::from_msg(&Help {
                links: vec![help::Link {
                    description: "desc".into(),
                    url: "url".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&LocalizedMessage {
                locale: "locale".into(),
                message: "message".into(),
                ..Default::default()
            }),
            Any::from_msg(&PreconditionFailure {
                violations: vec![precondition_failure::Violation {
                    r#type: "type".into(),
                    subject: "subject".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&QuotaFailure {
                violations: vec![quota_failure::Violation {
                    subject: "subject".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&RequestInfo {
                request_id: "id".into(),
                serving_data: "data".into(),
                ..Default::default()
            }),
            Any::from_msg(&ResourceInfo {
                resource_type: "type".into(),
                resource_name: "name".into(),
                owner: "owner".into(),
                description: "desc".into(),
                ..Default::default()
            }),
            Any::from_msg(&RetryInfo {
                retry_delay: prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                    ..Default::default()
                }
                .into(),
            }),
        ];
        from_msg.into_iter().map(|r| r.unwrap()).collect()
    }

    fn wkt_details() -> Vec<wkt::Any> {
        use google_cloud_rpc::model::*;
        use wkt::Any;
        let try_from = vec![
            Any::from_msg(&BadRequest::default().set_field_violations(vec![
                bad_request::FieldViolation::default()
                    .set_field("field")
                    .set_description("desc"),
            ])),
            Any::from_msg(
                &DebugInfo::default()
                    .set_stack_entries(vec!["stack".to_string()])
                    .set_detail("detail"),
            ),
            Any::from_msg(
                &ErrorInfo::default()
                    .set_reason("reason")
                    .set_domain("domain"),
            ),
            Any::from_msg(&Help::default().set_links(vec![
                help::Link::default().set_description("desc").set_url("url"),
            ])),
            Any::from_msg(
                &LocalizedMessage::default()
                    .set_locale("locale")
                    .set_message("message"),
            ),
            Any::from_msg(&PreconditionFailure::default().set_violations(vec![
                precondition_failure::Violation::default()
                    .set_type("type")
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::from_msg(&QuotaFailure::default().set_violations(vec![
                quota_failure::Violation::default()
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::from_msg(
                &RequestInfo::default()
                    .set_request_id("id")
                    .set_serving_data("data"),
            ),
            Any::from_msg(
                &ResourceInfo::default()
                    .set_resource_type("type")
                    .set_resource_name("name")
                    .set_owner("owner")
                    .set_description("desc"),
            ),
            Any::from_msg(&RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0))),
        ];
        try_from.into_iter().map(|x| x.unwrap()).collect()
    }
}
