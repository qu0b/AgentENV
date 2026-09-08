use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use super::ApiImpl;
use crate::{
    api::server,
    api_key::ApiKey,
    cfg::AppConfig,
    image::ImageResolver,
    orchestrator::{FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator},
    sandbox::FirecrackerSandboxFactory,
    snapshot::mock::mock_snapshot_manager,
    template::TemplateBuilder,
    volume::VolumeManager,
};

const KEY: &str = "allocation-test-key-00000000000000000000";

#[tokio::test]
async fn allocation_http_owner_mismatch_never_fences_or_allocates_on_another_node() {
    let (_root_a, app_a, owner_a) = app().await;
    let (_root_b, app_b, owner_b) = app().await;
    assert_ne!(owner_a, owner_b);
    assert_eq!(
        request(&app_a, "GET", "/sandbox-allocation-owner", None, &[])
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(
            &app_a,
            "GET",
            "/sandbox-allocation-owner",
            None,
            &[("x-api-key", KEY)]
        )
        .await
        .1,
        json!({"version":1,"ownerId":owner_a})
    );
    let id = Uuid::now_v7().to_string();
    let path = format!("/sandbox-allocations/{id}");
    for method in ["POST", "GET", "DELETE"] {
        let body = (method == "POST").then_some(json!({"templateID":"never-resolve"}));
        assert_eq!(
            request(&app_b, method, &path, body.clone(), &[("x-api-key", KEY)])
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            request(
                &app_b,
                method,
                &path,
                body.clone(),
                &[
                    ("x-api-key", KEY),
                    ("X-Agentenv-Allocation-Owner", &owner_a)
                ]
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            request(
                &app_b,
                method,
                &path,
                body,
                &[
                    ("x-api-key", KEY),
                    ("X-Agentenv-Allocation-Owner", &owner_b),
                    ("X-Agentenv-Allocation-Owner", &owner_a)
                ]
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        request(
            &app_b,
            "GET",
            &path,
            None,
            &[
                ("x-api-key", KEY),
                ("X-Agentenv-Allocation-Owner", &owner_b)
            ]
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

async fn app() -> (tempfile::TempDir, Router, String) {
    app_with_cleanup(None).await
}

async fn app_with_cleanup(
    cleanup: Option<crate::orchestrator::SandboxMetadata>,
) -> (tempfile::TempDir, Router, String) {
    use crate::orchestrator::SandboxPersister;
    let root = tempfile::tempdir().unwrap();
    let persister = FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes"));
    if let Some(metadata) = cleanup {
        persister.persist_deleting(&metadata).await.unwrap();
    }
    let orchestrator = Orchestrator::new(
        InMemoryMetadataStore::new(),
        FirecrackerSandboxFactory::new(),
        persister,
    )
    .await
    .unwrap();
    let snapshots = Arc::new(mock_snapshot_manager());
    let volumes = Arc::new(
        VolumeManager::open_with_repository(
            root.path().join("volumes/catalog.json"),
            snapshots.repository(),
        )
        .await
        .unwrap(),
    );
    let api = Arc::new(ApiImpl::new(
        orchestrator,
        snapshots,
        Arc::new(TemplateBuilder::new()),
        Arc::new(ImageResolver::new(&AppConfig::default())),
        volumes,
        None,
        Vec::new(),
        ApiKey::new(KEY).unwrap(),
    ));
    let owner = api
        .orchestrator
        .allocation_journal()
        .await
        .unwrap()
        .owner_id()
        .to_string();
    (root, server::new(api), owner)
}

#[tokio::test]
async fn cleanup_http_lists_debt_without_connecting_and_acknowledges_only_proven_deletion() {
    use crate::orchestrator::{SandboxMetadata, SandboxState};
    for stopped in [false, true] {
        let metadata = SandboxMetadata {
            state: SandboxState::CleanupPending,
            runtime_stopped: stopped,
            ..Default::default()
        };
        let id = metadata.id.to_string();
        let (_root, app, _) = app_with_cleanup(Some(metadata)).await;
        let headers = [("x-api-key", KEY)];
        let list = request(&app, "GET", "/v2/sandboxes", None, &headers).await;
        assert_eq!(list.0, StatusCode::OK);
        assert_eq!(list.1[0]["sandboxID"], id);
        assert_eq!(list.1[0]["state"], "cleanup_pending");
        assert_eq!(
            request(
                &app,
                "GET",
                "/v2/sandboxes?state=running&state=paused",
                None,
                &headers
            )
            .await
            .1,
            json!([])
        );
        let path = format!("/sandboxes/{id}");
        assert_eq!(
            request(
                &app,
                "POST",
                &format!("{path}/connect"),
                Some(json!({"timeout":60})),
                &headers
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            request(&app, "DELETE", &path, None, &[]).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request(&app, "DELETE", &path, None, &headers).await.0,
            if stopped {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        );
        let remaining = request(
            &app,
            "GET",
            "/v2/sandboxes?state=cleanup_pending",
            None,
            &headers,
        )
        .await;
        assert_eq!(remaining.0, StatusCode::OK);
        assert_eq!(remaining.1.as_array().unwrap().len(), usize::from(!stopped));
    }
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(
            builder
                .body(Body::from(
                    body.map(|body| body.to_string()).unwrap_or_default(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn allocation_http_cancel_fences_late_post_and_receipts_require_auth() {
    let (_root, app, owner) = app().await;
    let id = Uuid::now_v7().to_string();
    let path = format!("/sandbox-allocations/{id}");
    for method in ["GET", "DELETE", "POST"] {
        assert_eq!(
            request(&app, method, &path, None, &[]).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request(&app, method, &path, None, &[("x-api-key", "wrong")])
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }
    let auth = [
        ("x-api-key", KEY),
        ("X-Agentenv-Allocation-Owner", owner.as_str()),
    ];
    assert_eq!(
        request(&app, "GET", &path, None, &auth).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, receipt) = request(&app, "DELETE", &path, None, &auth).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        receipt,
        json!({"version":1,"ownerId":owner,"allocationId":id,"sandboxId":null,"requestDigest":null,"state":"cancelled","cancelRequested":true})
    );
    assert_eq!(request(&app, "GET", &path, None, &auth).await.1, receipt);
    // This template cannot resolve. A 409 proves the fence was checked before
    // snapshot resolution, restoration, reservations, or VM work.
    let body = json!({"templateID":"never-resolve-after-cancel","metadata":{"allocationId":id},"secure":true});
    let headers = [
        ("x-api-key", KEY),
        ("X-Agentenv-Allocation-Owner", owner.as_str()),
    ];
    assert_eq!(
        request(&app, "POST", &path, Some(body), &headers).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(request(&app, "GET", &path, None, &auth).await.1, receipt);
}

#[tokio::test]
async fn allocation_http_failure_replay_and_changed_request_keep_original_id() {
    let (_root, app, owner) = app().await;
    let id = Uuid::now_v7().to_string();
    let path = format!("/sandbox-allocations/{id}");
    let headers = [
        ("x-api-key", KEY),
        ("X-Agentenv-Allocation-Owner", owner.as_str()),
    ];
    let body = json!({"templateID":"unsupported-by-mock","secure":true,"envVars":{"TOKEN":"private-fixture-value"}});
    let first = request(&app, "POST", &path, Some(body.clone()), &headers).await;
    assert_eq!(first.0, StatusCode::INTERNAL_SERVER_ERROR);
    let (status, receipt) = request(
        &app,
        "GET",
        &path,
        None,
        &[
            ("x-api-key", KEY),
            ("X-Agentenv-Allocation-Owner", owner.as_str()),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["state"], "settled");
    assert!(receipt["sandboxId"].is_string());
    assert!(!receipt.to_string().contains("private-fixture-value"));
    assert_eq!(
        request(&app, "POST", &path, Some(body.clone()), &headers)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let mut changed = body;
    changed["envVars"]["TOKEN"] = "changed".into();
    assert_eq!(
        request(&app, "POST", &path, Some(changed), &headers)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            &app,
            "GET",
            &path,
            None,
            &[
                ("x-api-key", KEY),
                ("X-Agentenv-Allocation-Owner", owner.as_str())
            ]
        )
        .await
        .1,
        receipt
    );
}

#[tokio::test]
async fn allocation_http_rejects_invalid_keys_and_does_not_reinterpret_legacy_metadata() {
    let (_root, app, owner) = app().await;
    let body = json!({"templateID":"must-not-be-loaded"});
    for id in ["invalid", "00000000-0000-0000-0000-000000000000"] {
        for method in ["GET", "DELETE", "POST"] {
            assert_eq!(
                request(
                    &app,
                    method,
                    &format!("/sandbox-allocations/{id}"),
                    (method == "POST").then_some(body.clone()),
                    &[
                        ("x-api-key", KEY),
                        ("X-Agentenv-Allocation-Owner", owner.as_str())
                    ]
                )
                .await
                .0,
                StatusCode::BAD_REQUEST
            );
        }
    }
    let id = Uuid::now_v7().to_string();
    let legacy = json!({"templateID":"unsupported-by-mock","metadata":{"allocationId":id}});
    assert_eq!(
        request(
            &app,
            "POST",
            "/sandboxes",
            Some(legacy),
            &[
                ("x-api-key", KEY),
                ("X-Agentenv-Allocation-Owner", owner.as_str())
            ]
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        request(
            &app,
            "GET",
            &format!("/sandbox-allocations/{id}"),
            None,
            &[
                ("x-api-key", KEY),
                ("X-Agentenv-Allocation-Owner", owner.as_str())
            ]
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}
