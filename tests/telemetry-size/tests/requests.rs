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

use google_cloud_auth::credentials::api_key_credentials::Builder as ApiKey;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_secretmanager_v1::client::SecretManagerService;
use httptest::matchers::{request::*, *};
use httptest::responders::*;
use httptest::{Expectation, Server};
use opentelemetry::trace::{
    FutureExt, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const SECRET: &str = "projects/example/secrets/example";

fn context() -> opentelemetry::Context {
    opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from_hex("00000000000000000000000000000001").unwrap(),
        SpanId::from_hex("0000000000000002").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

async fn client(server: &Server) -> Result<SecretManagerService, Box<dyn std::error::Error>> {
    Ok(SecretManagerService::builder()
        .with_endpoint(server.url_str(""))
        .with_credentials(ApiKey::new("test-key").build())
        .with_tracing()
        .with_retry_policy(Aip194Strict.with_attempt_limit(2))
        .with_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(1))
                .with_maximum_delay(Duration::from_millis(1))
                .clamp(),
        )
        .build()
        .await?)
}

#[tokio::test]
async fn retry_and_decode_with_credentials_and_trace_context() -> TestResult {
    let server = Server::run();
    #[cfg(google_cloud_unstable_no_http_telemetry)]
    let trace_headers = headers(not(contains(("traceparent", any()))));
    #[cfg(not(google_cloud_unstable_no_http_telemetry))]
    let trace_headers = headers(contains((
        "traceparent",
        "00-00000000000000000000000000000001-0000000000000002-01",
    )));
    server.expect(
        Expectation::matching(all_of![
            method_path("GET", format!("/v1/{SECRET}")),
            headers(contains(("x-goog-api-key", "test-key"))),
            trace_headers,
        ])
        .times(2)
        .respond_with(cycle![
            status_code(503)
                .body(r#"{"error":{"code":503,"status":"UNAVAILABLE","message":"retry"}}"#),
            status_code(200).body(format!(r#"{{"name":"{SECRET}"}}"#)),
        ]),
    );
    let client = client(&server).await?;
    let secret = client
        .get_secret()
        .set_name(SECRET)
        .send()
        .with_context(context())
        .await?;
    assert_eq!(secret.name, SECRET);
    Ok(())
}

#[tokio::test]
async fn permanent_error_is_preserved() -> TestResult {
    let server = Server::run();
    server.expect(
        Expectation::matching(method_path("GET", format!("/v1/{SECRET}"))).respond_with(
            status_code(403)
                .body(r#"{"error":{"code":403,"status":"PERMISSION_DENIED","message":"denied"}}"#),
        ),
    );
    let client = client(&server).await?;
    let error = client
        .get_secret()
        .set_name(SECRET)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.status().unwrap().code,
        google_cloud_gax::error::rpc::Code::PermissionDenied
    );
    Ok(())
}

#[tokio::test]
async fn raw_http_request_obeys_the_same_trace_switch() -> TestResult {
    let server = Server::run();
    #[cfg(google_cloud_unstable_no_http_telemetry)]
    let trace_headers = headers(not(contains(("traceparent", any()))));
    #[cfg(not(google_cloud_unstable_no_http_telemetry))]
    let trace_headers = headers(contains(("traceparent", any())));
    server.expect(
        Expectation::matching(all_of![
            method_path("GET", "/raw"),
            trace_headers,
            headers(contains(("x-goog-api-key", "test-key"))),
        ])
        .respond_with(status_code(200)),
    );
    let mut config = gaxi::options::ClientConfig::default();
    config.tracing = true;
    config.cred = Some(ApiKey::new("test-key").build());
    assert_eq!(
        gaxi::options::tracing_enabled(&config),
        !cfg!(google_cloud_unstable_no_http_telemetry)
    );
    let client = gaxi::http::ReqwestClient::new(config, &server.url_str("")).await?;
    let response = client
        .http_builder(gaxi::http::reqwest::Method::GET, "/raw")
        .send(
            google_cloud_gax::options::RequestOptions::default(),
            gaxi::attempt_info::AttemptInfo::new(0),
        )
        .with_context(context())
        .await?;
    assert_eq!(response.status().as_u16(), 200);
    Ok(())
}
