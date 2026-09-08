use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use tracing::warn;
use uuid::Uuid;

use crate::cfg::ConfigManager;
use crate::image::ResolvedBlockImage;
use crate::observability::prometheus::SandboxStageTimer;
use crate::orchestrator::{
    CreateSandboxRequest, NewTimeout, OrchestratorError, SandboxForkChildSpec, SandboxLaunchSource,
    SandboxListFilter, SandboxMetadata, SandboxState, SandboxTimeoutAction,
};
use crate::sandbox::{normalize_mount_path, CustomExtensionParams, ExtraDrive};
use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy, SandboxNetworkPolicy};
use crate::snapshot::{
    CommandContext, SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotVolume,
};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};
use crate::volume::{VolumeManager, VolumeMode};
use agentenv_http_server::apis::sandboxes::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::attached_drives::resolve_attached_drives;
use super::pagination::PaginationCursor;
use super::volumes::{error_response as volume_error_response, resolve_volume_mounts};
use super::ApiImpl;

fn allocation_id(value: &str) -> Result<Uuid, models::Error> {
    Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(|| ApiImpl::error(400, "allocation key must be a non-nil UUID"))
}

fn sandbox_not_found(id: impl Into<String>) -> models::Error {
    ApiImpl::error(404, format!("sandbox {} not found", id.into()))
}

fn default_sandbox_timeout() -> Duration {
    static DEFAULT_SANDBOX_TIMEOUT: OnceLock<Duration> = OnceLock::new();

    *DEFAULT_SANDBOX_TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            ConfigManager::global_config()
                .orchestrator
                .default_sandbox_timeout_secs,
        )
    })
}

impl From<OrchestratorError> for models::Error {
    fn from(err: OrchestratorError) -> Self {
        match err {
            OrchestratorError::ShuttingDown => {
                Self::new(503, "orchestrator is shutting down".to_string())
            }
            OrchestratorError::SandboxNotFound(id) => sandbox_not_found(id),
            OrchestratorError::InvalidSandboxState { .. } => Self::new(400, err.to_string()),
            OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation,
                source,
            } => Self::new(
                500,
                format!(
                    "sandbox {} operation {:?} failed: {}",
                    sandbox_id,
                    operation,
                    ApiImpl::internal_error(source.as_ref()).message
                ),
            ),
            OrchestratorError::SandboxOperationConflict { .. } => Self::new(409, err.to_string()),
            other => ApiImpl::internal_error(&other),
        }
    }
}

impl From<SandboxState> for models::SandboxState {
    fn from(state: SandboxState) -> Self {
        match state {
            SandboxState::CleanupPending => Self::CleanupPending,
            SandboxState::Pausing
            | SandboxState::Paused
            | SandboxState::Snapshotting
            | SandboxState::Forking => Self::Paused,
            _ => Self::Running,
        }
    }
}

fn started_at(created_at: SystemTime) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(created_at)
}

fn end_at(expires_at: Option<SystemTime>) -> chrono::DateTime<chrono::Utc> {
    expires_at
        .map(chrono::DateTime::<chrono::Utc>::from)
        // distant future for non-expiring sandbox
        .unwrap_or(chrono::DateTime::<chrono::Utc>::from(
            std::time::UNIX_EPOCH + Duration::from_secs(60 * 60 * 24 * 365 * 100),
        ))
}

fn volume_mounts_model(mounts: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    (!mounts.is_empty()).then(|| mounts.clone())
}

#[derive(Default)]
struct PreparedVolumeMounts {
    owner: Option<String>,
    drives: Vec<ExtraDrive>,
    mounts: HashMap<String, String>,
    volume_ids: Vec<String>,
}

async fn prepare_volume_mounts(
    manager: &VolumeManager,
    mounts: Option<&HashMap<String, String>>,
    sandbox_id: SandboxId,
) -> Result<PreparedVolumeMounts, models::Error> {
    let Some(mounts) = mounts.filter(|mounts| !mounts.is_empty()) else {
        return Ok(PreparedVolumeMounts::default());
    };
    let owner = sandbox_id.to_string();
    let (drives, mounts) = resolve_volume_mounts(manager, mounts, &owner).await?;
    Ok(PreparedVolumeMounts {
        owner: Some(owner),
        drives,
        volume_ids: mounts.values().cloned().collect(),
        mounts,
    })
}

async fn release_failed_create_volumes(
    manager: &VolumeManager,
    owner: Option<&str>,
    runtime_stopped: bool,
    volume_ids: &[String],
    restored_volume_ids: &[String],
) -> Result<(), crate::volume::VolumeError> {
    if !runtime_stopped {
        // The original sandbox ID remains the durable volume owner. Its
        // cleanup_pending record can retry deletion without an owner handoff.
        return Ok(());
    }
    if let Some(owner) = owner {
        manager
            .recover_and_publish_backings(owner, volume_ids)
            .await?;
        manager.replace_owner_for(owner, None, volume_ids).await?;
    }
    for volume_id in restored_volume_ids {
        manager.delete(volume_id).await?;
    }
    Ok(())
}

async fn cleanup_fork_volume_children(
    manager: &VolumeManager,
    children: &HashMap<SandboxId, Vec<String>>,
) {
    for (owner, volume_ids) in children {
        let _ = manager
            .replace_owner_for(&owner.to_string(), None, volume_ids)
            .await;
        for volume_id in volume_ids {
            let _ = manager.delete(volume_id).await;
        }
    }
}

/// This path runs only before VM launch. Release only this attempt's owner;
/// repository deletion still refuses any other live owner.
async fn cleanup_unlaunched_volumes(
    manager: &VolumeManager,
    sandbox_id: SandboxId,
    volume_ids: &[String],
) -> Result<(), crate::volume::VolumeError> {
    manager
        .replace_owner_for(&sandbox_id.to_string(), None, volume_ids)
        .await?;
    for volume_id in volume_ids {
        manager.delete(volume_id).await?;
    }
    Ok(())
}

async fn prepare_volume_fork_specs(
    api: &ApiImpl,
    source: &SandboxMetadata,
    count: u32,
) -> Result<(Vec<SandboxForkChildSpec>, HashMap<SandboxId, Vec<String>>), models::Error> {
    let source_owner = source.id.to_string();
    let mut source_volumes = HashMap::with_capacity(source.volume_mounts.len());
    for reference in source.volume_mounts.values() {
        let volume = api
            .volume_manager
            .get(reference)
            .await
            .map_err(|error| volume_error_response(error).1)?;
        source_volumes.insert(reference, volume);
    }

    let mut children = HashMap::new();
    let mut specs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let child_id = SandboxId::new();
        let owner = child_id.to_string();
        children.entry(child_id).or_default();
        let mut volume_mounts = HashMap::with_capacity(source.volume_mounts.len());
        let mut child_mounts = HashMap::new();
        let mut replace_drive_ids = Vec::new();

        for (mount_path, reference) in &source.volume_mounts {
            let volume = source_volumes
                .get(reference)
                .expect("source volume was resolved above");
            if volume.mode == VolumeMode::Exclusive {
                let child_name = format!("volume-fork-{}", Uuid::now_v7().simple());
                let child = match api
                    .volume_manager
                    .create_child_for_owner(
                        &volume.id,
                        child_name,
                        VolumeMode::Exclusive,
                        volume.size_mb,
                        &source_owner,
                        &owner,
                    )
                    .await
                {
                    Ok(child) => child,
                    Err(error) => {
                        cleanup_fork_volume_children(&api.volume_manager, &children).await;
                        return Err(volume_error_response(error).1);
                    }
                };
                children.entry(child_id).or_default().push(child.id.clone());
                child_mounts.insert(mount_path.clone(), child.id.clone());
                replace_drive_ids.push((volume.id.clone(), child.id.clone()));
                volume_mounts.insert(mount_path.clone(), child.id);
            } else {
                volume_mounts.insert(mount_path.clone(), volume.id.clone());
                child_mounts.insert(mount_path.clone(), volume.id.clone());
            }
        }

        let (extra_drives, _) =
            match resolve_volume_mounts(&api.volume_manager, &child_mounts, &owner).await {
                Ok(result) => result,
                Err(error) => {
                    cleanup_fork_volume_children(&api.volume_manager, &children).await;
                    return Err(error);
                }
            };

        specs.push(SandboxForkChildSpec {
            sandbox_id: child_id,
            volume_mounts,
            extra_drives,
            replace_drive_ids,
        });
    }

    Ok((specs, children))
}

async fn snapshot_sandbox_volumes(
    api: &ApiImpl,
    metadata: &SandboxMetadata,
) -> Result<Vec<SnapshotVolume>, models::Error> {
    let mut snapshots = Vec::with_capacity(metadata.volume_mounts.len());
    for (mount_path, reference) in &metadata.volume_mounts {
        let snapshot = api
            .volume_manager
            .snapshot_volume_state(reference)
            .await
            .map_err(|error| volume_error_response(error).1)?;
        snapshots.push(SnapshotVolume {
            mount_path: mount_path.clone(),
            mode: snapshot.mode,
            size_mb: snapshot.size_mb,
            layers: snapshot.backing_layers,
        });
    }
    Ok(snapshots)
}

async fn restore_snapshot_volume_mounts(
    manager: &VolumeManager,
    snapshots: &[SnapshotVolume],
    sandbox_id: SandboxId,
) -> Result<(HashMap<String, String>, Vec<String>), models::Error> {
    if snapshots.len() > manager.limits().max_mounts {
        return Err(ApiImpl::error(
            400,
            "snapshot contains too many volume mounts",
        ));
    }
    // Validate the full mount layout before creating any durable children.
    // Inserting duplicate normalized paths into a map would otherwise lose
    // the first child's mount while leaving its volume reserved forever.
    let mut mount_paths: Vec<PathBuf> = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let path = normalize_mount_path(snapshot.mount_path.clone().into())
            .map_err(|error| ApiImpl::error(400, error.to_string()))?;
        if mount_paths
            .iter()
            .any(|existing| existing.starts_with(&path) || path.starts_with(existing))
        {
            return Err(ApiImpl::error(400, "snapshot volume mount paths overlap"));
        }
        mount_paths.push(path);
    }
    let mut mounts = HashMap::new();
    let mut volume_ids = Vec::with_capacity(snapshots.len());
    for (snapshot, mount_path) in snapshots.iter().zip(mount_paths) {
        let name = format!("volume-restore-{}", Uuid::now_v7().simple());
        let child = match manager
            .create_from_snapshot(
                name,
                snapshot.mode,
                snapshot.size_mb,
                snapshot.layers.clone(),
                Some(sandbox_id),
            )
            .await
        {
            Ok(child) => child,
            Err(error) => {
                cleanup_unlaunched_volumes(manager, sandbox_id, &volume_ids)
                    .await
                    .map_err(|error| volume_error_response(error).1)?;
                return Err(volume_error_response(error).1);
            }
        };
        volume_ids.push(child.id.clone());
        mounts.insert(mount_path.to_string_lossy().into_owned(), child.id);
    }
    Ok((mounts, volume_ids))
}

impl From<SandboxTimeoutAction> for models::SandboxOnTimeout {
    fn from(action: SandboxTimeoutAction) -> Self {
        match action {
            SandboxTimeoutAction::Pause => Self::Pause,
            SandboxTimeoutAction::Delete => Self::Kill,
        }
    }
}

impl From<SandboxMetadata> for models::ListedSandbox {
    fn from(m: SandboxMetadata) -> Self {
        let volume_mounts = volume_mounts_model(&m.volume_mounts);
        Self {
            template_id: m.snapshot_id,
            alias: m.snapshot_alias,
            sandbox_id: m.id.into(),
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            started_at: started_at(m.created_at),
            end_at: end_at(m.expires_at),
            cpu_count: m.resources.cpu_count,
            memory_mb: m.resources.memory_mib,
            disk_size_mb: m.resources.disk_size_mib,
            metadata: m.user_metadata,
            state: m.state.into(),
            envd_version: m.runtime_versions.envd_version.clone(),
            volume_mounts,
        }
    }
}

impl From<SandboxMetadata> for models::Sandbox {
    fn from(m: SandboxMetadata) -> Self {
        Self {
            template_id: m.snapshot_id,
            sandbox_id: m.id.into(),
            alias: m.snapshot_alias,
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            envd_version: m.runtime_versions.envd_version.clone(),
            envd_access_token: None,
            traffic_access_token: None,
            domain: None,
        }
    }
}

impl From<&SandboxNetworkPolicy> for models::SandboxNetworkConfig {
    fn from(policy: &SandboxNetworkPolicy) -> Self {
        let egress = &policy.egress;
        Self {
            allow_public_traffic: Some(policy.allow_public_traffic),
            allow_out: (!egress.allowed_cidrs.is_empty() || !egress.allowed_domains.is_empty())
                .then(|| {
                    egress
                        .allowed_cidrs
                        .iter()
                        .map(ToString::to_string)
                        .chain(egress.allowed_domains.iter().cloned())
                        .collect()
                }),
            deny_out: (!egress.denied_cidrs.is_empty()).then(|| {
                egress
                    .denied_cidrs
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            }),
            mask_request_host: None,
        }
    }
}

fn base_policy_from_allow_internet_access(value: Option<bool>) -> BaseSandboxNetworkPolicy {
    match value {
        Some(true) => BaseSandboxNetworkPolicy::Allow,
        Some(false) => BaseSandboxNetworkPolicy::Deny,
        None => BaseSandboxNetworkPolicy::Default,
    }
}

fn allow_internet_access_from_base_policy(policy: BaseSandboxNetworkPolicy) -> Nullable<bool> {
    match policy {
        BaseSandboxNetworkPolicy::Default => Nullable::Null,
        BaseSandboxNetworkPolicy::Allow => Nullable::Present(true),
        BaseSandboxNetworkPolicy::Deny => Nullable::Present(false),
    }
}

impl From<SandboxMetadata> for models::SandboxDetail {
    fn from(m: SandboxMetadata) -> Self {
        let volume_mounts = volume_mounts_model(&m.volume_mounts);
        let network = (!m.network_policy.allow_public_traffic
            || m.network_policy.has_explicit_egress_rules())
        .then(|| models::SandboxNetworkConfig::from(&m.network_policy));
        let allow_internet_access = Some(allow_internet_access_from_base_policy(
            m.network_policy.base_policy,
        ));

        Self {
            template_id: m.snapshot_id,
            alias: m.snapshot_alias,
            sandbox_id: m.id.into(),
            client_id: "".to_string(), // Deprecated field, only reserved for E2B Python SDK.
            started_at: started_at(m.created_at),
            end_at: end_at(m.expires_at),
            envd_version: m.runtime_versions.envd_version.clone(),
            envd_access_token: None,
            allow_internet_access,
            domain: None,
            cpu_count: m.resources.cpu_count,
            memory_mb: m.resources.memory_mib,
            disk_size_mb: m.resources.disk_size_mib,
            metadata: m.user_metadata,
            state: m.state.into(),
            network,
            lifecycle: Some(models::SandboxLifecycle {
                auto_resume: m.auto_resume,
                on_timeout: m.timeout_action.into(),
            }),
            volume_mounts,
        }
    }
}

impl ApiImpl {
    async fn owned_allocation_journal(
        &self,
        owner: &str,
    ) -> Result<std::sync::Arc<crate::allocation::AllocationJournal>, models::Error> {
        let owner = Uuid::parse_str(owner)
            .map_err(|_| Self::error(400, "allocation owner must be a UUID"))?;
        let journal = self
            .orchestrator
            .allocation_journal()
            .await
            .map_err(|error| {
                warn!(%error, "allocation journal unavailable");
                Self::error(500, "allocation journal unavailable")
            })?;
        if journal.owner_id() != owner {
            return Err(Self::error(
                409,
                "allocation belongs to a different durable state owner",
            ));
        }
        Ok(journal)
    }

    async fn create_keyed_sandbox(
        &self,
        id: Uuid,
        body: &models::NewSandbox,
        journal: std::sync::Arc<crate::allocation::AllocationJournal>,
    ) -> Result<SandboxesPostResponse, ()> {
        use crate::allocation::{request_digest, AllocationConflict, AllocationResult};
        let this = self.clone();
        let body = body.clone();
        let result = async {
            journal
                .execute(id, request_digest(&body)?, move |sandbox_id| async move {
                    this.create_warm_sandbox(&body, sandbox_id).await
                })
                .await
        }
        .await;
        match result {
            Ok(AllocationResult::Completed(response)) => response,
            Ok(AllocationResult::Existing(receipt)) => {
                Ok(SandboxesPostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("allocation {} already claimed ({:?}); query its sandbox-allocation receipt", receipt.allocation_id, receipt.state),
                )))
            }
            Err(error) if error.is::<AllocationConflict>() => Ok(
                SandboxesPostResponse::Status409_Conflict(Self::error(409, error.to_string())),
            ),
            Err(error) => {
                warn!(%error, %id, "keyed sandbox allocation failed");
                Ok(SandboxesPostResponse::Status500_ServerError(Self::error(
                    500,
                    "allocation outcome unavailable; query the original allocation key",
                )))
            }
        }
    }

    async fn create_warm_sandbox(
        &self,
        body: &models::NewSandbox,
        sandbox_id: SandboxId,
    ) -> Result<SandboxesPostResponse, ()> {
        let timer = SandboxStageTimer::new("create_warm");
        let snapshot = match timer
            .time(
                "load_snapshot",
                self.snapshot_manager.load_runnable(&body.template_id),
            )
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                    400,
                    format!("template {} not found", body.template_id),
                )));
            }
            Err(err) => {
                warn!(error = ?err, template_id = %body.template_id, "failed to load runnable snapshot");
                return Ok(SandboxesPostResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let network_policy =
            match network_policy_from_create(body.allow_internet_access, body.network.as_ref()) {
                Ok(network) => network,
                Err(err) => {
                    return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                        400,
                        err.to_string(),
                    )));
                }
            };

        let custom_params = body
            .custom_extension_params
            .as_ref()
            .map(params_model_to_map);
        if let Err(err) = validate_custom_extension_params(custom_params.as_ref()) {
            return Ok(SandboxesPostResponse::Status400_BadRequest(Self::error(
                400,
                err.to_string(),
            )));
        }

        let extra_drives_in_snapshot =
            body.volume_mounts.is_none() && !snapshot.committed().volume_snapshots.is_empty();
        let (requested_volume_mounts, restored_volume_ids) = if body.volume_mounts.is_some() {
            (body.volume_mounts.clone(), Vec::new())
        } else {
            match restore_snapshot_volume_mounts(
                &self.volume_manager,
                &snapshot.committed().volume_snapshots,
                sandbox_id,
            )
            .await
            {
                Ok((mounts, volume_ids)) => ((!mounts.is_empty()).then_some(mounts), volume_ids),
                Err(error) => {
                    return Ok(match error.code {
                        500 => SandboxesPostResponse::Status500_ServerError(error),
                        _ => SandboxesPostResponse::Status400_BadRequest(error),
                    });
                }
            }
        };
        let PreparedVolumeMounts {
            owner: volume_owner,
            drives: volume_drives,
            mounts: volume_mounts,
            volume_ids: reserved_volume_ids,
        } = match prepare_volume_mounts(
            &self.volume_manager,
            requested_volume_mounts.as_ref(),
            sandbox_id,
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Err(cleanup_error) = cleanup_unlaunched_volumes(
                    &self.volume_manager,
                    sandbox_id,
                    &restored_volume_ids,
                )
                .await
                {
                    return Ok(SandboxesPostResponse::Status500_ServerError(
                        volume_error_response(cleanup_error).1,
                    ));
                }
                return Ok(match error.code {
                    500.. => SandboxesPostResponse::Status500_ServerError(error),
                    409 => SandboxesPostResponse::Status409_Conflict(error),
                    _ => SandboxesPostResponse::Status400_BadRequest(error),
                });
            }
        };

        let request = CreateSandboxRequest {
            source: SandboxLaunchSource::Snapshot(Box::new(snapshot)),
            extra_drives: volume_drives,
            extra_drives_in_snapshot,
            timeout: duration_from_secs(body.timeout),
            timeout_action: match body.auto_pause {
                Some(false) => SandboxTimeoutAction::Delete,
                _ => SandboxTimeoutAction::Pause,
            },
            auto_resume: body.auto_resume.as_ref().is_some_and(|cfg| cfg.enabled),
            user_metadata: body.metadata.clone(),
            env_vars: body
                .env_vars
                .clone()
                .filter(|env_vars| !env_vars.is_empty()),
            network_policy,
            secure: body.secure == Some(true),
            custom_extension_params: custom_params,
            volume_mounts,
        };

        let outcome = timer
            .time(
                "create_sandbox",
                self.orchestrator.create_sandbox_with_cleanup_outcome(
                    sandbox_id,
                    request,
                    restored_volume_ids.clone(),
                ),
            )
            .await;
        match outcome {
            Ok(metadata) => {
                let sandbox_id = metadata.id.to_string();
                Ok(
                    SandboxesPostResponse::Status201_TheSandboxWasCreatedSuccessfully {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(sandbox_id),
                    },
                )
            }
            Err(failure) => {
                let err = failure.error;
                if let Err(cleanup_error) = release_failed_create_volumes(
                    &self.volume_manager,
                    volume_owner.as_deref(),
                    failure.runtime_stopped,
                    &reserved_volume_ids,
                    &restored_volume_ids,
                )
                .await
                {
                    warn!(%sandbox_id, %cleanup_error, "failed create retains volume cleanup debt");
                }
                Ok(SandboxesPostResponse::Status500_ServerError(
                    Self::internal_error(&err),
                ))
            }
        }
    }

    fn sandbox_model(&self, metadata: SandboxMetadata) -> models::Sandbox {
        let traffic_access_token = (!metadata.network_policy.allow_public_traffic)
            .then(|| self.traffic_access_token(metadata.id));
        let envd_access_token = self
            .orchestrator
            .get_envd_access_token(&metadata)
            .map(|token| token.expose().to_owned());
        let mut sandbox = models::Sandbox::from(metadata);
        sandbox.envd_access_token = envd_access_token;
        sandbox.traffic_access_token = Some(match traffic_access_token {
            Some(token) => Nullable::Present(token),
            None => Nullable::Null,
        });
        sandbox.domain = self
            .sandbox_proxy_domains()
            .first()
            .map(|domain| Nullable::Present(domain.clone()));
        sandbox
    }

    fn sandbox_detail_model(&self, metadata: SandboxMetadata) -> models::SandboxDetail {
        let envd_access_token = self
            .orchestrator
            .get_envd_access_token(&metadata)
            .map(|token| token.expose().to_owned());
        let mut sandbox = models::SandboxDetail::from(metadata);
        sandbox.envd_access_token = envd_access_token;
        sandbox.domain = self
            .sandbox_proxy_domains()
            .first()
            .map(|domain| Nullable::Present(domain.clone()));
        sandbox
    }
}

fn parse_metadata_filter(raw: &Option<String>) -> Option<HashMap<String, String>> {
    let raw = raw.as_ref()?;
    let map: HashMap<String, String> = url::form_urlencoded::parse(raw.as_bytes())
        .filter(|(key, value)| !key.is_empty() && !value.is_empty())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

fn duration_from_secs(secs: Option<u32>) -> Option<Duration> {
    secs.map(|s| Duration::from_secs(s as u64))
}

fn cold_start_resources(body: &models::NewColdSandbox) -> Result<SandboxResources, models::Error> {
    let config = ConfigManager::global_config();
    let default_cpu = config.machine.vcpu_count;
    let default_mem = config.machine.mem_size_mib;
    let cpu_count = body.cpu_count.unwrap_or(default_cpu);
    if cpu_count == 0 {
        return Err(ApiImpl::error(
            400,
            "cpuCount must be greater than 0".to_string(),
        ));
    }
    let memory_mib = body.memory_mb.unwrap_or(default_mem);
    if memory_mib < 128 {
        return Err(ApiImpl::error(
            400,
            "memoryMB must be at least 128".to_string(),
        ));
    }
    let disk_size_mib = body.disk_size_mb.unwrap_or(0);
    if body.disk_size_mb.is_some() && (disk_size_mib < 1024 || !disk_size_mib.is_multiple_of(1024))
    {
        return Err(ApiImpl::error(
            400,
            "diskSizeMB must be at least 1024 and divisible by 1024".to_string(),
        ));
    }
    Ok(SandboxResources {
        cpu_count,
        memory_mib,
        // Zero means omitted; the orchestrator fills it from the resolved rootfs.
        disk_size_mib,
    })
}

/// Build source image configs from the resolved rootfs and attached drives.
fn build_image_configs(
    rootfs: &ResolvedBlockImage,
    attached: &[super::attached_drives::ResolvedAttachedDrive],
) -> ImageConfigs {
    let mut image_configs = ImageConfigs::new();
    if let Some(config) = &rootfs.raw_config {
        image_configs.add(None::<String>, "/", config.clone());
    }
    for r in attached {
        if let Some(config) = &r.raw_config {
            image_configs.add(
                Some(r.drive.drive_id()),
                r.drive.mount_path().display().to_string(),
                config.clone(),
            );
        }
    }
    image_configs
}

/// Convert a generated params model into the internal params map.
fn params_model_to_map(
    model: &std::collections::HashMap<String, agentenv_http_server::types::Object>,
) -> serde_json::Map<String, serde_json::Value> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.0.clone()))
        .collect()
}

/// Convert stored params into the generated response model. Absent params
/// yield an empty object (empty params).
fn params_map_to_model(
    params: Option<&serde_json::Map<String, serde_json::Value>>,
) -> std::collections::HashMap<String, agentenv_http_server::types::Object> {
    match params {
        Some(map) => map
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    agentenv_http_server::types::Object(value.clone()),
                )
            })
            .collect(),
        None => std::collections::HashMap::new(),
    }
}

/// Reject non-empty custom extension params when no custom extension is
/// configured. Empty params (absent or `{}`) are always allowed.
fn validate_custom_extension_params(params: Option<&CustomExtensionParams>) -> anyhow::Result<()> {
    if !crate::sandbox::custom_extension_params_is_empty(params)
        && crate::sandbox::CustomExtensionClient::global().is_none()
    {
        anyhow::bail!(
            "customExtensionParams requires the custom extension to be configured ([custom_extension].url)"
        );
    }
    Ok(())
}

fn network_policy_from_create(
    allow_internet_access: Option<bool>,
    network: Option<&models::SandboxNetworkConfig>,
) -> anyhow::Result<SandboxNetworkPolicy> {
    let base_policy = base_policy_from_allow_internet_access(allow_internet_access);
    let allow_out = network.and_then(|network| network.allow_out.clone());
    let deny_out = network.and_then(|network| network.deny_out.clone());
    let egress = SandboxNetworkEgressPolicy::new(allow_out, deny_out)?;
    let allow_public_traffic = network
        .and_then(|network| network.allow_public_traffic)
        .unwrap_or(true);
    let policy = SandboxNetworkPolicy::new(allow_public_traffic, base_policy, egress);
    validate_ipv4_cidrs(&policy)?;
    validate_domain_allowlist(&policy)?;
    Ok(policy)
}

fn network_policy_from_update(
    body: &models::SandboxNetworkUpdateConfig,
) -> anyhow::Result<SandboxNetworkPolicy> {
    let policy = SandboxNetworkEgressPolicy::new(body.allow_out.clone(), body.deny_out.clone())?;
    let policy = SandboxNetworkPolicy::new(
        true,
        base_policy_from_allow_internet_access(body.allow_internet_access),
        policy,
    );
    validate_ipv4_cidrs(&policy)?;
    validate_domain_allowlist(&policy)?;
    Ok(policy)
}

fn validate_ipv4_cidrs(policy: &SandboxNetworkPolicy) -> anyhow::Result<()> {
    if policy
        .egress
        .allowed_cidrs
        .iter()
        .chain(policy.egress.denied_cidrs.iter())
        .any(|cidr| matches!(cidr, ipnetwork::IpNetwork::V6(_)))
    {
        anyhow::bail!("IPv6 CIDRs are not supported by the sandbox network API");
    }
    Ok(())
}

fn validate_domain_allowlist(policy: &SandboxNetworkPolicy) -> anyhow::Result<()> {
    use crate::sandbox::ALL_INTERNET_TRAFFIC_CIDR;

    // E2B's domain inspection applies to HTTP/HTTPS (TCP 80/443); other TCP
    // ports remain CIDR-only. Do not reject a mixed domain/CIDR policy here:
    // the runtime still honors its explicit CIDR grants on every port.
    if policy.has_domain_allow_rules()
        && !policy
            .egress
            .denied_cidrs
            .iter()
            .any(|cidr| cidr == &ALL_INTERNET_TRAFFIC_CIDR)
    {
        anyhow::bail!("allowOut contains domains but denyOut is missing {ALL_INTERNET_TRAFFIC_CIDR} (ALL_TRAFFIC)");
    }
    Ok(())
}

#[async_trait]
impl Sandboxes<()> for ApiImpl {
    async fn sandbox_allocation_owner_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
    ) -> Result<SandboxAllocationOwnerGetResponse, ()> {
        use SandboxAllocationOwnerGetResponse as Response;
        Ok(match self.orchestrator.allocation_journal().await {
            Ok(journal) => Response::Status200_AllocationOwner(
                models::SandboxAllocationOwner::new(1, journal.owner_id().to_string()),
            ),
            Err(error) => {
                warn!(%error, "allocation owner unavailable");
                Response::Status500_ServerError(Self::error(500, "allocation owner unavailable"))
            }
        })
    }

    async fn sandbox_allocations_allocation_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        header_params: &models::SandboxAllocationsAllocationIdPostHeaderParams,
        path_params: &models::SandboxAllocationsAllocationIdPostPathParams,
        body: &models::NewSandbox,
    ) -> Result<SandboxAllocationsAllocationIdPostResponse, ()> {
        use SandboxAllocationsAllocationIdPostResponse as Response;
        let journal = match self
            .owned_allocation_journal(&header_params.x_agentenv_allocation_owner)
            .await
        {
            Ok(journal) => journal,
            Err(error) => {
                return Ok(match error.code {
                    400 => Response::Status400_BadRequest(error),
                    409 => Response::Status409_Conflict(error),
                    _ => Response::Status500_ServerError(error),
                })
            }
        };
        let id = match allocation_id(&path_params.allocation_id) {
            Ok(id) => id,
            Err(error) => return Ok(Response::Status400_BadRequest(error)),
        };
        Ok(match self.create_keyed_sandbox(id, body, journal).await? {
            SandboxesPostResponse::Status201_TheSandboxWasCreatedSuccessfully {
                body,
                x_agentenv_sandbox_id,
            } => Response::Status201_TheSandboxWasCreatedSuccessfully {
                body,
                x_agentenv_sandbox_id,
            },
            SandboxesPostResponse::Status400_BadRequest(error) => {
                Response::Status400_BadRequest(error)
            }
            SandboxesPostResponse::Status401_AuthenticationError(error) => {
                Response::Status401_AuthenticationError(error)
            }
            SandboxesPostResponse::Status409_Conflict(error) => Response::Status409_Conflict(error),
            SandboxesPostResponse::Status500_ServerError(error) => {
                Response::Status500_ServerError(error)
            }
        })
    }

    async fn sandbox_allocations_allocation_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        header_params: &models::SandboxAllocationsAllocationIdGetHeaderParams,
        path_params: &models::SandboxAllocationsAllocationIdGetPathParams,
    ) -> Result<SandboxAllocationsAllocationIdGetResponse, ()> {
        use SandboxAllocationsAllocationIdGetResponse as Response;
        let journal = match self
            .owned_allocation_journal(&header_params.x_agentenv_allocation_owner)
            .await
        {
            Ok(journal) => journal,
            Err(error) => {
                return Ok(match error.code {
                    400 => Response::Status400_BadRequest(error),
                    409 => Response::Status409_Conflict(error),
                    _ => Response::Status500_ServerError(error),
                })
            }
        };
        let id = match allocation_id(&path_params.allocation_id) {
            Ok(id) => id,
            Err(error) => return Ok(Response::Status400_BadRequest(error)),
        };
        let result: anyhow::Result<Option<models::SandboxAllocation>> = async {
            journal
                .get(id)
                .await?
                .map(|receipt| Ok(serde_json::from_value(serde_json::to_value(receipt)?)?))
                .transpose()
        }
        .await;
        Ok(match result {
            Ok(Some(receipt)) => Response::Status200_AllocationReceipt(receipt),
            Ok(None) => {
                Response::Status404_NotFound(Self::error(404, "allocation receipt not found"))
            }
            Err(error) => {
                warn!(%error, %id, "failed to read allocation receipt");
                Response::Status500_ServerError(Self::error(500, "allocation receipt unavailable"))
            }
        })
    }

    async fn sandbox_allocations_allocation_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        header_params: &models::SandboxAllocationsAllocationIdDeleteHeaderParams,
        path_params: &models::SandboxAllocationsAllocationIdDeletePathParams,
    ) -> Result<SandboxAllocationsAllocationIdDeleteResponse, ()> {
        use SandboxAllocationsAllocationIdDeleteResponse as Response;
        let journal = match self
            .owned_allocation_journal(&header_params.x_agentenv_allocation_owner)
            .await
        {
            Ok(journal) => journal,
            Err(error) => {
                return Ok(match error.code {
                    400 => Response::Status400_BadRequest(error),
                    409 => Response::Status409_Conflict(error),
                    _ => Response::Status500_ServerError(error),
                })
            }
        };
        let id = match allocation_id(&path_params.allocation_id) {
            Ok(id) => id,
            Err(error) => return Ok(Response::Status400_BadRequest(error)),
        };
        let result: anyhow::Result<models::SandboxAllocation> = async {
            Ok(serde_json::from_value(serde_json::to_value(
                journal.cancel(id).await?,
            )?)?)
        }
        .await;
        Ok(match result {
            Ok(receipt) => Response::Status200_AllocationReceipt(receipt),
            Err(error) => {
                warn!(%error, %id, "failed to fence allocation");
                Response::Status500_ServerError(Self::error(
                    500,
                    "allocation cancellation unavailable",
                ))
            }
        })
    }

    type Claims = super::Claims;

    async fn sandboxes_cold_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::NewColdSandbox,
    ) -> Result<SandboxesColdPostResponse, ()> {
        let image_resolver = self.image_resolver();
        let timer = SandboxStageTimer::new("create_cold");
        // TODO: Move cold-start image resolution into an async create operation
        // once the API supports 202 Accepted + status polling.
        let resolved_rootfs = match timer
            .time("resolve_rootfs", image_resolver.resolve(&body.image))
            .await
        {
            Ok(resolved) => resolved,
            Err(err) if err.is_user_error() => {
                return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ));
            }
            Err(err) => {
                warn!(error = %format_args!("{err:#}"), image = %body.image, "failed to resolve sandbox rootfs image");
                return Ok(SandboxesColdPostResponse::Status500_ServerError(
                    Self::error(
                        500,
                        format!("resolve sandbox rootfs image '{}': {err:#}", body.image),
                    ),
                ));
            }
        };
        let resources = match cold_start_resources(body) {
            Ok(resources) => resources,
            Err(err) => return Ok(SandboxesColdPostResponse::Status400_BadRequest(err)),
        };
        let resolved_attached = match timer
            .time(
                "resolve_attached_drives",
                resolve_attached_drives(
                    body.attached_drives.as_deref().unwrap_or_default(),
                    image_resolver.as_ref(),
                ),
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                warn!(error = %err.message, "failed to resolve attached drives");
                return Ok(Self::client_or_server_response(
                    err,
                    SandboxesColdPostResponse::Status400_BadRequest,
                    SandboxesColdPostResponse::Status500_ServerError,
                ));
            }
        };

        let network_policy =
            match network_policy_from_create(body.allow_internet_access, body.network.as_ref()) {
                Ok(network) => network,
                Err(err) => {
                    return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                        Self::error(400, err.to_string()),
                    ));
                }
            };

        let custom_params = body
            .custom_extension_params
            .as_ref()
            .map(params_model_to_map);
        if let Err(err) = validate_custom_extension_params(custom_params.as_ref()) {
            return Ok(SandboxesColdPostResponse::Status400_BadRequest(
                Self::error(400, err.to_string()),
            ));
        }

        let sandbox_id = SandboxId::new();
        let PreparedVolumeMounts {
            owner: volume_owner,
            drives: volume_drives,
            mounts: volume_mounts,
            volume_ids: reserved_volume_ids,
        } = match prepare_volume_mounts(
            &self.volume_manager,
            body.volume_mounts.as_ref(),
            sandbox_id,
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) if error.code >= 500 => {
                return Ok(SandboxesColdPostResponse::Status500_ServerError(error));
            }
            Err(error) if error.code == 409 => {
                return Ok(SandboxesColdPostResponse::Status409_Conflict(error));
            }
            Err(error) => return Ok(SandboxesColdPostResponse::Status400_BadRequest(error)),
        };

        let image_configs = build_image_configs(&resolved_rootfs, &resolved_attached);
        let mut extra_drives: Vec<_> = resolved_attached.into_iter().map(|r| r.drive).collect();
        extra_drives.extend(volume_drives);

        let base = resolved_rootfs.base_context;
        let request = CreateSandboxRequest {
            source: SandboxLaunchSource::Image {
                image_ref: resolved_rootfs.image_ref,
                overlaybd_config_path: resolved_rootfs.overlaybd_config_path,
                context: Box::new(
                    CommandContext::from_env_and_workdir(base.env_vars, base.workdir)
                        .with_user(base.user)
                        .with_exposed_ports(base.exposed_ports)
                        .with_entrypoint(base.entrypoint)
                        .with_cmd(base.cmd)
                        .with_volumes(base.volumes)
                        .with_labels(base.labels),
                ),
                resources: Some(resources),
                extra_drives,
                extra_boot_args: body.extra_boot_args.clone(),
                image_configs: Box::new(image_configs),
            },
            extra_drives: Vec::new(),
            extra_drives_in_snapshot: false,
            timeout: duration_from_secs(body.timeout),
            timeout_action: match body.auto_pause {
                Some(false) => SandboxTimeoutAction::Delete,
                _ => SandboxTimeoutAction::Pause,
            },
            auto_resume: body.auto_resume.as_ref().is_some_and(|cfg| cfg.enabled),
            user_metadata: body.metadata.clone(),
            env_vars: body
                .env_vars
                .clone()
                .filter(|env_vars| !env_vars.is_empty()),
            network_policy,
            secure: body.secure == Some(true),
            custom_extension_params: custom_params,
            volume_mounts,
        };

        let outcome = timer
            .time(
                "create_sandbox",
                self.orchestrator.create_sandbox_with_cleanup_outcome(
                    sandbox_id,
                    request,
                    Vec::new(),
                ),
            )
            .await;
        match outcome {
            Ok(metadata) => {
                let sandbox_id = metadata.id.to_string();
                Ok(
                    SandboxesColdPostResponse::Status201_TheSandboxWasCreatedSuccessfully {
                        body: self.sandbox_model(metadata),
                        x_agentenv_sandbox_id: Some(sandbox_id),
                    },
                )
            }
            Err(failure) => {
                let err = failure.error;
                if let Err(cleanup_error) = release_failed_create_volumes(
                    &self.volume_manager,
                    volume_owner.as_deref(),
                    failure.runtime_stopped,
                    &reserved_volume_ids,
                    &[],
                )
                .await
                {
                    warn!(%sandbox_id, %cleanup_error, "failed create retains volume cleanup debt");
                }
                let invalid_request = match &err {
                    OrchestratorError::SandboxOperationFailed { source, .. } => {
                        source.chain().find_map(|cause| {
                            cause
                                .downcast_ref::<uvm_ublk_daemon::InvalidRequestError>()
                                .map(ToString::to_string)
                        })
                    }
                    _ => None,
                };
                match invalid_request {
                    Some(message) => Ok(SandboxesColdPostResponse::Status400_BadRequest(
                        Self::error(400, message),
                    )),
                    None => Ok(SandboxesColdPostResponse::Status500_ServerError(
                        Self::internal_error(&err),
                    )),
                }
            }
        }
    }

    async fn sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SandboxesGetQueryParams,
    ) -> Result<SandboxesGetResponse, ()> {
        let filter = SandboxListFilter {
            states: Some(vec![SandboxState::Running]),
            user_metadata: parse_metadata_filter(&query_params.metadata),
            ..SandboxListFilter::matches_all()
        };

        let list = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };

        let out = list.into_iter().map(models::ListedSandbox::from).collect();

        Ok(SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes(out))
    }

    async fn sandboxes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::NewSandbox,
    ) -> Result<SandboxesPostResponse, ()> {
        self.create_warm_sandbox(body, SandboxId::new()).await
    }

    async fn sandboxes_sandbox_id_connect_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdConnectPostPathParams,
        body: &models::ConnectSandbox,
    ) -> Result<SandboxesSandboxIdConnectPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let metadata = match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()),
                );
            }
        };

        match metadata.state {
            SandboxState::Creating
            | SandboxState::Resuming
            | SandboxState::Running
            | SandboxState::Snapshotting
            | SandboxState::Forking => {
                match self
                    .orchestrator
                    .keep_alive_for(sandbox_id, duration_from_secs(Some(body.timeout)), false)
                    .await
                {
                    Ok(_) => {}
                    Err(OrchestratorError::SandboxNotFound(id)) => {
                        return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                            sandbox_not_found(id),
                        ));
                    }
                    Err(OrchestratorError::InvalidTimeout { timeout, .. }) => {
                        return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                            Self::error(400, format!("invalid timeout: {timeout:?}")),
                        ));
                    }
                    Err(err) => {
                        return Ok(
                            SandboxesSandboxIdConnectPostResponse::Status500_ServerError(
                                err.into(),
                            ),
                        );
                    }
                }
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status200_TheSandboxWasAlreadyRunning(
                        self.sandbox_model(metadata),
                    ),
                );
            }
            SandboxState::Killing | SandboxState::CleanupPending => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            SandboxState::Pausing | SandboxState::Paused => {}
        }

        // try to resume the sandbox
        match self
            .orchestrator
            .resume_sandbox(
                sandbox_id,
                NewTimeout::Set(Duration::from_secs(body.timeout as u64)),
            )
            .await
        {
            Ok(resumed_metadata) => return Ok(
                SandboxesSandboxIdConnectPostResponse::Status201_TheSandboxWasResumedSuccessfully(
                    self.sandbox_model(resumed_metadata),
                ),
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status400_BadRequest(
                    Self::error(
                        400,
                        format!("sandbox cannot be resumed from {} state", state),
                    ),
                ));
            }
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => {
                return Ok(SandboxesSandboxIdConnectPostResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ));
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdConnectPostResponse::Status500_ServerError(err.into()),
                );
            }
        }
    }

    async fn sandboxes_sandbox_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdDeletePathParams,
    ) -> Result<SandboxesSandboxIdDeleteResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdDeleteResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        match self.orchestrator.delete_sandbox(sandbox_id).await {
            Ok(_) => {
                Ok(SandboxesSandboxIdDeleteResponse::Status204_TheSandboxWasKilledSuccessfully)
            }
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdDeleteResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(err) => Ok(SandboxesSandboxIdDeleteResponse::Status500_ServerError(
                err.into(),
            )),
        }
    }

    async fn sandboxes_sandbox_id_fork_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdForkPostPathParams,
        body: &Option<models::SandboxForkRequest>,
    ) -> Result<SandboxesSandboxIdForkPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdForkPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };

        let source_metadata = match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Ok(SandboxesSandboxIdForkPostResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(error) => {
                return Ok(SandboxesSandboxIdForkPostResponse::Status500_ServerError(
                    error.into(),
                ));
            }
        };

        let count = body.as_ref().and_then(|b| b.count).unwrap_or(1);
        if !(1..=100).contains(&count) {
            return Ok(SandboxesSandboxIdForkPostResponse::Status400_BadRequest(
                Self::error(400, "count must be between 1 and 100"),
            ));
        }
        if !source_metadata.volume_mounts.is_empty() {
            if let Err(error) = self.orchestrator.snapshot_volume_mounts(sandbox_id).await {
                return Ok(SandboxesSandboxIdForkPostResponse::Status500_ServerError(
                    error.into(),
                ));
            }
            if let Err(error) = self
                .volume_manager
                .recover_backings(
                    &sandbox_id.to_string(),
                    &source_metadata
                        .volume_mounts
                        .values()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
                .await
            {
                return Ok(SandboxesSandboxIdForkPostResponse::Status500_ServerError(
                    Self::error(
                        500,
                        format!("failed to recover local volume backing before fork: {error}"),
                    ),
                ));
            }
        }
        let (child_specs, child_volume_ids) = if source_metadata.volume_mounts.is_empty() {
            (
                (0..count)
                    .map(|_| SandboxForkChildSpec {
                        sandbox_id: SandboxId::new(),
                        ..Default::default()
                    })
                    .collect(),
                HashMap::new(),
            )
        } else {
            match prepare_volume_fork_specs(self, &source_metadata, count).await {
                Ok(specs) => specs,
                Err(error) => {
                    return Ok(match error.code {
                        404 => SandboxesSandboxIdForkPostResponse::Status404_NotFound(error),
                        409 => SandboxesSandboxIdForkPostResponse::Status409_Conflict(error),
                        500 => SandboxesSandboxIdForkPostResponse::Status500_ServerError(error),
                        _ => SandboxesSandboxIdForkPostResponse::Status400_BadRequest(error),
                    });
                }
            }
        };
        let new_timeout = body
            .as_ref()
            .and_then(|body| body.timeout)
            .map_or(NewTimeout::UseExisting, |timeout| {
                NewTimeout::Set(Duration::from_secs(timeout as u64))
            });
        let timer = SandboxStageTimer::new("fork");
        match timer
            .time(
                "fork",
                self.orchestrator
                    .fork_sandbox_with_specs(sandbox_id, child_specs, new_timeout),
            )
            .await
        {
            Ok(outcomes) => {
                if !child_volume_ids.is_empty() {
                    let successful_ids: HashSet<_> = outcomes
                        .iter()
                        .filter_map(|outcome| outcome.as_ref().ok().map(|metadata| metadata.id))
                        .collect();
                    let failed_children = child_volume_ids
                        .into_iter()
                        .filter(|(child_id, _)| !successful_ids.contains(child_id))
                        .collect();
                    cleanup_fork_volume_children(&self.volume_manager, &failed_children).await;
                }
                let results = outcomes
                    .into_iter()
                    .map(|outcome| match outcome {
                        Ok(metadata) => models::SandboxForkResult {
                            sandbox: Some(self.sandbox_model(metadata)),
                            error: None,
                        },
                        Err(err) => models::SandboxForkResult {
                            sandbox: None,
                            error: Some(models::Error::from(err)),
                        },
                    })
                    .collect();
                Ok(
                    SandboxesSandboxIdForkPostResponse::Status201_TheSandboxWasSnapshottedAndTheForksWereAttempted(results),
                )
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                cleanup_fork_volume_children(&self.volume_manager, &child_volume_ids).await;
                Ok(SandboxesSandboxIdForkPostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ))
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok({
                cleanup_fork_volume_children(&self.volume_manager, &child_volume_ids).await;
                SandboxesSandboxIdForkPostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("sandbox cannot be forked from {} state", state),
                ))
            }),
            Err(err) => {
                cleanup_fork_volume_children(&self.volume_manager, &child_volume_ids).await;
                Ok(SandboxesSandboxIdForkPostResponse::Status500_ServerError(
                    err.into(),
                ))
            }
        }
    }

    async fn sandboxes_sandbox_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdGetPathParams,
    ) -> Result<SandboxesSandboxIdGetResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdGetResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let metadata = match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Ok(SandboxesSandboxIdGetResponse::Status404_NotFound(
                    sandbox_not_found(sandbox_id),
                ));
            }
            Err(err) => {
                return Ok(SandboxesSandboxIdGetResponse::Status500_ServerError(
                    err.into(),
                ));
            }
        };
        Ok(
            SandboxesSandboxIdGetResponse::Status200_SuccessfullyReturnedTheSandbox(
                self.sandbox_detail_model(metadata),
            ),
        )
    }

    async fn sandboxes_sandbox_id_network_put(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdNetworkPutPathParams,
        body: &models::SandboxNetworkUpdateConfig,
    ) -> Result<SandboxesSandboxIdNetworkPutResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdNetworkPutResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let network = match network_policy_from_update(body) {
            Ok(network) => network,
            Err(err) => {
                return Ok(SandboxesSandboxIdNetworkPutResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ));
            }
        };

        match self
            .orchestrator
            .replace_sandbox_network_policy(sandbox_id, network)
            .await
        {
            Ok(()) => Ok(SandboxesSandboxIdNetworkPutResponse::Status204_SuccessfullyUpdatedTheSandboxNetworkConfiguration),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdNetworkPutResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                Ok(SandboxesSandboxIdNetworkPutResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox network cannot be updated from {} state", state),
                    ),
                ))
            }
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => {
                Ok(SandboxesSandboxIdNetworkPutResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ))
            }
            Err(err) => Ok(SandboxesSandboxIdNetworkPutResponse::Status500_ServerError(err.into())),
        }
    }

    async fn sandboxes_sandbox_id_custom_extension_params_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsGetPathParams,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsGetResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            );
        };

        match self.orchestrator.get_sandbox(&sandbox_id).await {
            Ok(Some(metadata)) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status200_TheCurrentCustomExtensionParams(
                    params_map_to_model(metadata.custom_extension_params.as_ref()),
                ),
            ),
            Ok(None) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            ),
            Err(err) => Ok(
                SandboxesSandboxIdCustomExtensionParamsGetResponse::Status500_ServerError(err.into()),
            ),
        }
    }

    async fn sandboxes_sandbox_id_custom_extension_params_patch(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdCustomExtensionParamsPatchPathParams,
        body: &std::collections::HashMap<String, agentenv_http_server::types::Object>,
    ) -> Result<SandboxesSandboxIdCustomExtensionParamsPatchResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status404_NotFound(
                    sandbox_not_found(path_id),
                ),
            );
        };
        let patch = params_model_to_map(body);

        match self
            .orchestrator
            .patch_sandbox_custom_extension_params(sandbox_id, patch)
            .await
        {
            Ok(new_params) => Ok(SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status200_TheUpdatedFullCustomExtensionParams(
                params_map_to_model(new_params.as_ref()),
            )),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox custom extension params cannot be patched from {} state", state),
                    ),
                ),
            ),
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ),
            ),
            // The extension rejected the patch, or no extension is configured.
            Err(err @ OrchestratorError::SandboxOperationFailed { .. }) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status400_BadRequest(
                    Self::error(400, err.to_string()),
                ),
            ),
            Err(err) => Ok(
                SandboxesSandboxIdCustomExtensionParamsPatchResponse::Status500_ServerError(err.into()),
            ),
        }
    }

    async fn sandboxes_sandbox_id_pause_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdPausePostPathParams,
    ) -> Result<SandboxesSandboxIdPausePostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdPausePostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timer = SandboxStageTimer::new("pause");
        match timer
            .time("pause", self.orchestrator.pause_sandbox(sandbox_id))
            .await
        {
            Ok(_) => Ok(
                SandboxesSandboxIdPausePostResponse::Status204_TheSandboxWasPausedSuccessfullyAndCanBeResumed,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdPausePostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => Ok(
                SandboxesSandboxIdPausePostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("sandbox cannot be paused from {} state", state),
                )),
            ),
            Err(err) => Ok(SandboxesSandboxIdPausePostResponse::Status500_ServerError(
                err.into(),
            )),
        }
    }

    async fn sandboxes_sandbox_id_snapshots_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdSnapshotsPostPathParams,
        body: &models::SandboxSnapshotRequest,
    ) -> Result<SandboxesSandboxIdSnapshotsPostResponse, ()> {
        let timer = SandboxStageTimer::new("snapshot");
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdSnapshotsPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };

        let alias = match &body.name {
            Some(name) => match SnapshotAlias::parse(name) {
                Ok(alias) => Some(alias),
                Err(err) => {
                    return Ok(
                        SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest(Self::error(
                            400,
                            format!("invalid snapshot alias: {}", err),
                        )),
                    );
                }
            },
            None => None,
        };

        let capture = match timer
            .time("capture", self.orchestrator.capture_snapshot(sandbox_id))
            .await
        {
            Ok(capture) => capture,
            Err(OrchestratorError::SandboxNotFound(id)) => {
                return Ok(SandboxesSandboxIdSnapshotsPostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(
                    SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("sandbox cannot be snapshotted from {} state", state),
                    )),
                );
            }
            Err(err) => {
                return Ok(
                    SandboxesSandboxIdSnapshotsPostResponse::Status500_ServerError(
                        Self::internal_error(&err),
                    ),
                );
            }
        };

        let volume_snapshots = match snapshot_sandbox_volumes(self, &capture.metadata).await {
            Ok(snapshots) => snapshots,
            Err(error) => {
                return Ok(match error.code {
                    500 => SandboxesSandboxIdSnapshotsPostResponse::Status500_ServerError(error),
                    _ => SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest(error),
                });
            }
        };

        let published = match timer
            .time(
                "publish",
                self.snapshot_manager.publish_captured(
                    SnapshotPublishMetadata {
                        id: SnapshotId::generate(),
                        alias: alias.clone(),
                        source: SnapshotPublishSource::Sandbox {
                            source_sandbox_id: capture.metadata.id.to_string(),
                        },
                        context: capture.metadata.context.clone(),
                        startup: capture.metadata.startup.clone(),
                        resources: capture.metadata.resources,
                        runtime_versions: capture.metadata.runtime_versions.clone(),
                        virtualization_mode: capture.metadata.virtualization_mode,
                        image_configs: capture.metadata.image_configs.clone(),
                        volume_snapshots,
                        custom_extension_params: capture.metadata.custom_extension_params.clone(),
                    },
                    capture.captured_snapshot,
                ),
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let error =
                    Self::bad_request_for_repository_build_error(&err).unwrap_or_else(|| {
                        warn!(error = ?err, %sandbox_id, "failed to publish captured snapshot");
                        Self::error(500, err.to_string())
                    });
                return Ok(Self::client_or_server_response(
                    error,
                    SandboxesSandboxIdSnapshotsPostResponse::Status400_BadRequest,
                    SandboxesSandboxIdSnapshotsPostResponse::Status500_ServerError,
                ));
            }
        };

        let info = models::SnapshotInfo::from(published);

        Ok(SandboxesSandboxIdSnapshotsPostResponse::Status201_SnapshotCreatedSuccessfully(info))
    }

    async fn sandboxes_sandbox_id_refreshes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdRefreshesPostPathParams,
        body: &Option<models::SandboxRefreshRequest>,
    ) -> Result<SandboxesSandboxIdRefreshesPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdRefreshesPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = body
            .as_ref()
            .and_then(|b| b.duration)
            .map(|d| Duration::from_secs(d as u64));

        match self
            .orchestrator
            .keep_alive_for(sandbox_id, timeout, false)
            .await
        {
            Ok(_) => Ok(
                SandboxesSandboxIdRefreshesPostResponse::Status204_SuccessfullyRefreshedTheSandbox,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdRefreshesPostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(err) => {
                Ok(SandboxesSandboxIdRefreshesPostResponse::Status500_ServerError(err.into()))
            }
        }
    }

    async fn sandboxes_sandbox_id_resume_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdResumePostPathParams,
        body: &models::ResumedSandbox,
    ) -> Result<SandboxesSandboxIdResumePostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = duration_from_secs(body.timeout).unwrap_or(default_sandbox_timeout());

        let timer = SandboxStageTimer::new("resume");
        match timer
            .time(
                "resume",
                self.orchestrator
                    .resume_sandbox(sandbox_id, NewTimeout::Set(timeout)),
            )
            .await
        {
            Ok(metadata) => {
                return Ok(
                    SandboxesSandboxIdResumePostResponse::Status201_TheSandboxWasResumedSuccessfully(
                        self.sandbox_model(metadata),
                    ),
                );
            }
            Err(OrchestratorError::SandboxNotFound(id)) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status404_NotFound(
                    sandbox_not_found(id),
                ));
            }
            Err(OrchestratorError::InvalidTimeout { timeout, .. }) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status400_BadRequest(
                    Self::error(400, format!("invalid timeout: {timeout}")),
                ));
            }
            Err(OrchestratorError::InvalidSandboxState { state, .. }) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(
                        409,
                        format!("sandbox cannot be resumed from {} state", state),
                    ),
                ));
            }
            Err(err @ OrchestratorError::SandboxOperationConflict { .. }) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status409_Conflict(
                    Self::error(409, err.to_string()),
                ));
            }
            Err(err) => {
                return Ok(SandboxesSandboxIdResumePostResponse::Status500_ServerError(
                    err.into(),
                ));
            }
        }
    }

    async fn sandboxes_sandbox_id_timeout_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SandboxesSandboxIdTimeoutPostPathParams,
        body: &Option<models::SandboxTimeoutRequest>,
    ) -> Result<SandboxesSandboxIdTimeoutPostResponse, ()> {
        let path_id = &path_params.sandbox_id;
        let Ok(sandbox_id) = SandboxId::parse_str(path_id) else {
            return Ok(SandboxesSandboxIdTimeoutPostResponse::Status404_NotFound(
                sandbox_not_found(path_id),
            ));
        };
        let timeout = body.as_ref().map(|b| Duration::from_secs(b.timeout as u64));

        match self
            .orchestrator
            .keep_alive_for(sandbox_id, timeout, true)
            .await
        {
            Ok(_) => Ok(
                SandboxesSandboxIdTimeoutPostResponse::Status204_SuccessfullySetTheSandboxTimeout,
            ),
            Err(OrchestratorError::SandboxNotFound(id)) => Ok(
                SandboxesSandboxIdTimeoutPostResponse::Status404_NotFound(sandbox_not_found(id)),
            ),
            Err(err) => {
                Ok(SandboxesSandboxIdTimeoutPostResponse::Status500_ServerError(err.into()))
            }
        }
    }

    async fn v2_sandboxes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::V2SandboxesGetQueryParams,
    ) -> Result<V2SandboxesGetResponse, ()> {
        let states: Option<Vec<SandboxState>> = if query_params.state.is_empty() {
            None
        } else {
            Some(
                query_params
                    .state
                    .iter()
                    .map(|state| match state {
                        models::SandboxState::Running => SandboxState::Running,
                        models::SandboxState::Paused => SandboxState::Paused,
                        models::SandboxState::CleanupPending => SandboxState::CleanupPending,
                    })
                    .collect(),
            )
        };
        let include_running = states
            .as_ref()
            .is_none_or(|states| states.contains(&SandboxState::Running));
        let descending = !matches!(query_params.order, Some(models::OrderDirection::Asc));

        let filter = SandboxListFilter {
            states,
            excluded_states: None,
            user_metadata: parse_metadata_filter(&query_params.metadata),
            started_after: query_params.started_after.map(SystemTime::from),
            template: query_params.template.clone(),
        };

        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match PaginationCursor::parse(token) {
                Ok(cursor) if cursor.is_descending() == descending => cursor,
                Ok(_) => {
                    return Ok(V2SandboxesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        "next token was issued for a different sort order".to_string(),
                    )));
                }
                Err(err) => {
                    return Ok(V2SandboxesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {}", err),
                    )));
                }
            },
            None if descending => {
                PaginationCursor::new_descending(SystemTime::now(), SandboxId::max())
            }
            None => PaginationCursor::new_ascending(SystemTime::UNIX_EPOCH, SandboxId::max()),
        };

        let list = match self.orchestrator.list_sandboxes_filtered(filter).await {
            Ok(list) => list,
            Err(err) => {
                return Ok(V2SandboxesGetResponse::Status500_ServerError(err.into()));
            }
        };

        let x_total_running = include_running.then(|| {
            list.iter()
                .filter(|sandbox| sandbox.state == SandboxState::Running)
                .count()
                .try_into()
                .unwrap_or(i32::MAX)
        });

        let page = cursor.paginate(
            list,
            query_params.limit,
            |a, b| PaginationCursor::compare(descending, a.created_at, &a.id, b.created_at, &b.id),
            |sandbox, cursor| {
                PaginationCursor::compare(
                    descending,
                    sandbox.created_at,
                    &sandbox.id,
                    cursor.time(),
                    cursor.value(),
                )
            },
            |sandbox| PaginationCursor::new(sandbox.created_at, sandbox.id, descending),
        );

        let out = page
            .items
            .into_iter()
            .map(models::ListedSandbox::from)
            .collect::<Vec<_>>();

        Ok(
            V2SandboxesGetResponse::Status200_SuccessfullyReturnedAllRunningSandboxes {
                body: out,
                x_next_token: page.next_token,
                x_total_running,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn preparation_volume_manager(root: &std::path::Path) -> anyhow::Result<VolumeManager> {
        use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("cache")),
            runtime_cache_root: Some(root.join("runtime")),
        })?;
        VolumeManager::open_with_repository(root.join("volumes/catalog"), backend.repository())
            .await
    }

    fn preparation_snapshot(path: &str, mode: crate::volume::VolumeMode) -> SnapshotVolume {
        SnapshotVolume {
            mount_path: path.into(),
            mode,
            size_mb: 64,
            layers: vec![crate::snapshot::OverlaybdLayerRef::External(
                crate::snapshot::ExternalLayer {
                    digest: format!("sha256:{}", "a".repeat(64)),
                    size: 4096,
                    repo_blob_url: "https://preparation-fixture.invalid/blobs/".into(),
                },
            )],
        }
    }

    #[tokio::test]
    async fn snapshot_volume_layout_is_validated_before_any_catalog_write() -> anyhow::Result<()> {
        use crate::volume::VolumeMode;
        for paths in [
            ["/data", "/data"],
            ["/data", "/data/"],
            ["/data", "/data/nested"],
        ] {
            let root = tempfile::tempdir()?;
            let manager = preparation_volume_manager(root.path()).await?;
            let snapshots = paths.map(|path| preparation_snapshot(path, VolumeMode::Exclusive));
            let error = restore_snapshot_volume_mounts(&manager, &snapshots, SandboxId::new())
                .await
                .unwrap_err();
            assert_eq!(error.code, 400);
            assert!(manager.list_page(None, 10).await?.records.is_empty());
            assert!(
                !root.path().join("repository/volumes").exists(),
                "invalid layout must not create even catalog lock or alias files"
            );
        }
        let root = tempfile::tempdir()?;
        let manager = preparation_volume_manager(root.path()).await?;
        let oversized = (0..=manager.limits().max_mounts)
            .map(|i| preparation_snapshot(&format!("/mount-{i}"), VolumeMode::Exclusive))
            .collect::<Vec<_>>();
        assert!(
            restore_snapshot_volume_mounts(&manager, &oversized, SandboxId::new())
                .await
                .is_err()
        );
        assert!(!root.path().join("repository/volumes").exists());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_volume_preparation_cleans_only_its_original_owned_children(
    ) -> anyhow::Result<()> {
        use crate::volume::{VolumeError, VolumeMode};
        let root = tempfile::tempdir()?;
        let manager = preparation_volume_manager(root.path()).await?;
        let owner = SandboxId::new();
        let mut snapshots = vec![
            preparation_snapshot("/data", VolumeMode::Exclusive),
            preparation_snapshot("/database", VolumeMode::ReadOnly),
        ];
        snapshots[1].size_mb = 0;
        assert!(restore_snapshot_volume_mounts(&manager, &snapshots, owner)
            .await
            .is_err());
        assert!(
            manager.list_page(None, 10).await?.records.is_empty(),
            "a later preparation failure must clean the earlier owned child"
        );
        snapshots[1].size_mb = 64;
        let (mounts, ids) = restore_snapshot_volume_mounts(&manager, &snapshots, owner)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        assert_eq!(
            mounts.len(),
            2,
            "distinct path components must not be mistaken for overlap"
        );
        for id in &ids {
            assert!(manager.get(id).await?.mounted_by(&owner.to_string()));
        }
        let prepared = prepare_volume_mounts(&manager, Some(&mounts), owner)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        assert_eq!(prepared.volume_ids.len(), 2);
        let shared = mounts.get("/database").unwrap();
        manager.reserve(shared, "other-reader").await?;
        let error = cleanup_unlaunched_volumes(&manager, owner, &ids)
            .await
            .unwrap_err();
        assert!(matches!(error, VolumeError::Reserved(_)));
        let remaining = manager.get(shared).await?;
        assert!(!remaining.mounted_by(&owner.to_string()));
        assert!(remaining.mounted_by("other-reader"));
        assert!(matches!(
            manager.get(mounts.get("/data").unwrap()).await,
            Err(VolumeError::NotFound(_))
        ));
        manager
            .replace_owner_for("other-reader", None, std::slice::from_ref(shared))
            .await?;
        manager.delete(shared).await?;
        Ok(())
    }

    #[tokio::test]
    async fn mount_resolution_failure_releases_all_preowned_restored_volumes() -> anyhow::Result<()>
    {
        use crate::volume::VolumeMode;
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir()?;
        let manager = preparation_volume_manager(root.path()).await?;
        let owner = SandboxId::new();
        let snapshots = vec![
            preparation_snapshot("/data", VolumeMode::Exclusive),
            preparation_snapshot("/other", VolumeMode::Exclusive),
        ];
        let (mounts, ids) = restore_snapshot_volume_mounts(&manager, &snapshots, owner)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let blocked = manager.data_dir(mounts.get("/data").unwrap());
        std::fs::create_dir_all(&blocked)?;
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o500))?;
        let result = prepare_volume_mounts(&manager, Some(&mounts), owner).await;
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700))?;
        assert!(
            result.is_err(),
            "filesystem failure must stop mount materialization"
        );
        assert!(
            manager
                .get(mounts.get("/other").unwrap())
                .await?
                .mounted_by(&owner.to_string()),
            "restored but unvisited mounts still carry their initial owner"
        );
        cleanup_unlaunched_volumes(&manager, owner, &ids).await?;
        assert!(manager.list_page(None, 10).await?.records.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn failed_create_keeps_final_volume_owner_until_runtime_cleanup_is_confirmed(
    ) -> anyhow::Result<()> {
        use crate::orchestrator::{
            FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator,
        };
        use crate::sandbox::mock::{MockAction, MockBackendFactory, MockBehavior, MockOperation};
        use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
        use crate::snapshot::{ExternalLayer, OverlaybdLayerRef, RunnableSnapshot};
        use crate::volume::{VolumeError, VolumeMode};
        use std::sync::Arc;

        for mode in [VolumeMode::Exclusive, VolumeMode::ReadOnly] {
            for operation in [
                None,
                Some(MockOperation::Build),
                Some(MockOperation::StartNowait),
                Some(MockOperation::WaitForReady),
            ] {
                for (stop_fails, restored) in
                    [(false, false), (true, false), (false, true), (true, true)]
                {
                    if (operation.is_none() || operation == Some(MockOperation::Build))
                        && stop_fails
                    {
                        continue;
                    }
                    let root = tempfile::tempdir()?;
                    let backend = PosixFsBackend::new(PosixFsBackendConfig {
                        root: root.path().join("repository"),
                        cache_root: Some(root.path().join("cache")),
                        runtime_cache_root: Some(root.path().join("runtime")),
                    })?;
                    let catalog = root.path().join("volumes/catalog");
                    let manager = Arc::new(
                        VolumeManager::open_with_repository(&catalog, backend.repository()).await?,
                    );
                    let volume = manager
                        .create_from_snapshot(
                            "original-exclusive-volume".into(),
                            mode,
                            64,
                            vec![OverlaybdLayerRef::External(ExternalLayer {
                                digest: format!("sha256:{}", "a".repeat(64)),
                                size: 4096,
                                repo_blob_url: "https://volume-fixture.invalid/blobs/".into(),
                            })],
                            None,
                        )
                        .await?;
                    let restored_ids = if restored {
                        vec![volume.id.clone()]
                    } else {
                        Vec::new()
                    };
                    if mode == VolumeMode::ReadOnly && !restored {
                        manager.reserve(&volume.id, "other-reader").await?;
                    }
                    let sandbox_id = SandboxId::new();
                    let prepared = prepare_volume_mounts(
                        &manager,
                        Some(&HashMap::from([("/data".into(), volume.id.clone())])),
                        sandbox_id,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?;
                    assert_eq!(
                        prepared.owner.as_deref(),
                        Some(sandbox_id.to_string().as_str())
                    );
                    assert!(manager
                        .get(&volume.id)
                        .await?
                        .mounted_by(&sandbox_id.to_string()));
                    let behavior = Arc::new(MockBehavior::new());
                    if let Some(operation) = operation {
                        behavior.push_action(
                            operation,
                            MockAction::Fail {
                                message: "create failed".into(),
                            },
                        );
                    }
                    if stop_fails {
                        behavior.push_action(
                            MockOperation::Stop,
                            MockAction::Fail {
                                message: "VM still alive".into(),
                            },
                        );
                    }
                    let persister =
                        FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes"));
                    let mut orchestrator = Orchestrator::new_with_volumes(
                        InMemoryMetadataStore::new(),
                        MockBackendFactory::with_behavior(behavior.clone()),
                        persister,
                        manager.clone(),
                    )
                    .await?;
                    let outcome = orchestrator
                        .create_sandbox_with_cleanup_outcome(
                            sandbox_id,
                            CreateSandboxRequest {
                                source: SandboxLaunchSource::Snapshot(Box::new(
                                    RunnableSnapshot::mock(),
                                )),
                                extra_drives: prepared.drives,
                                extra_drives_in_snapshot: false,
                                timeout: Some(Duration::from_secs(60)),
                                timeout_action: SandboxTimeoutAction::Delete,
                                auto_resume: false,
                                user_metadata: None,
                                env_vars: None,
                                network_policy: Default::default(),
                                secure: false,
                                custom_extension_params: None,
                                volume_mounts: prepared.mounts,
                            },
                            restored_ids.clone(),
                        )
                        .await;
                    let failure = match outcome {
                        Ok(metadata) => {
                            assert!(operation.is_none());
                            assert!(metadata.volumes_created_for_launch.is_empty());
                            orchestrator.delete_sandbox(sandbox_id).await?;
                            let record = manager.get(&volume.id).await?;
                            assert!(!record.mounted_by(&sandbox_id.to_string()));
                            if mode == VolumeMode::ReadOnly && !restored {
                                assert!(record.mounted_by("other-reader"));
                            }
                            orchestrator.shutdown().await?;
                            continue;
                        }
                        Err(failure) => {
                            assert!(operation.is_some());
                            failure
                        }
                    };
                    assert_eq!(failure.runtime_stopped, !stop_fails);
                    release_failed_create_volumes(
                        &manager,
                        prepared.owner.as_deref(),
                        failure.runtime_stopped,
                        &prepared.volume_ids,
                        &restored_ids,
                    )
                    .await?;
                    let fresh =
                        VolumeManager::open_with_repository(&catalog, backend.repository()).await?;
                    if stop_fails {
                        assert!(fresh
                            .get(&volume.id)
                            .await?
                            .mounted_by(&sandbox_id.to_string()));
                        if mode == VolumeMode::Exclusive {
                            assert!(matches!(
                                fresh.reserve(&volume.id, "replacement-vm").await,
                                Err(VolumeError::Reserved(_))
                            ));
                        }
                        assert!(matches!(
                            fresh.delete(&volume.id).await,
                            Err(VolumeError::Reserved(_))
                        ));
                        assert_eq!(
                            orchestrator.get_sandbox(&sandbox_id).await?.unwrap().state,
                            SandboxState::CleanupPending
                        );
                        if restored {
                            use std::os::unix::fs::PermissionsExt;
                            let blocked = root
                                .path()
                                .join("sandboxes/artifacts")
                                .join(sandbox_id.to_string())
                                .join("locked");
                            std::fs::create_dir_all(&blocked)?;
                            std::fs::write(blocked.join("evidence"), b"cleanup retry evidence")?;
                            std::fs::set_permissions(
                                &blocked,
                                std::fs::Permissions::from_mode(0o500),
                            )?;
                            let attempt = orchestrator.delete_sandbox(sandbox_id).await;
                            std::fs::set_permissions(
                                &blocked,
                                std::fs::Permissions::from_mode(0o700),
                            )?;
                            assert!(
                                attempt.is_err(),
                                "artifact failure must retain deletion debt"
                            );
                            assert!(matches!(
                                fresh.get(&volume.id).await,
                                Err(VolumeError::NotFound(_))
                            ));
                            let previous = Arc::downgrade(&orchestrator);
                            drop(orchestrator);
                            for _ in 0..100 {
                                if previous.upgrade().is_none() {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            assert!(
                                previous.upgrade().is_none(),
                                "old orchestrator must stop before reopening state"
                            );
                            orchestrator = Orchestrator::new_with_volumes(
                                InMemoryMetadataStore::new(),
                                MockBackendFactory::new(),
                                FileBackedSandboxPersister::new_for_test(
                                    root.path().join("sandboxes"),
                                ),
                                manager.clone(),
                            )
                            .await?;
                            let loaded = orchestrator.get_sandbox(&sandbox_id).await?.unwrap();
                            assert!(loaded.runtime_stopped);
                            assert_eq!(loaded.volumes_created_for_launch, restored_ids);
                        }
                        orchestrator.delete_sandbox(sandbox_id).await?;
                    }
                    let released =
                        VolumeManager::open_with_repository(&catalog, backend.repository()).await?;
                    if restored {
                        assert!(matches!(
                            released.get(&volume.id).await,
                            Err(VolumeError::NotFound(_))
                        ));
                    } else {
                        let record = released.get(&volume.id).await?;
                        assert!(!record.mounted_by(&sandbox_id.to_string()));
                        if mode == VolumeMode::ReadOnly {
                            assert!(record.mounted_by("other-reader"));
                        }
                        released.reserve(&volume.id, "replacement-vm").await?;
                        released
                            .replace_owner_for(
                                "replacement-vm",
                                None,
                                std::slice::from_ref(&volume.id),
                            )
                            .await?;
                    }
                    orchestrator.shutdown().await?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn parse_metadata_filter_with_none_returns_none() {
        assert_eq!(parse_metadata_filter(&None), None);
    }

    #[test]
    fn parse_metadata_filter_with_empty_string_returns_none() {
        assert_eq!(parse_metadata_filter(&Some(String::new())), None);
    }

    #[test]
    fn parse_metadata_filter_with_single_pair() {
        let result = parse_metadata_filter(&Some("key=value".to_string()));
        assert_eq!(
            result,
            Some(HashMap::from([("key".to_string(), "value".to_string())]))
        );
    }

    #[test]
    fn parse_metadata_filter_with_multiple_pairs() {
        let result = parse_metadata_filter(&Some("a=1&b=2".to_string()));
        assert_eq!(result.map(|m| m.len()), Some(2));
    }

    #[test]
    fn parse_metadata_filter_filters_empty_keys() {
        let result = parse_metadata_filter(&Some("=value&key=val".to_string()));
        let map = result.unwrap();
        assert!(!map.contains_key(""));
        assert!(map.contains_key("key"));
    }

    #[test]
    fn parse_metadata_filter_with_encoded_characters() {
        let result = parse_metadata_filter(&Some(
            "key%20with%20spaces=value%20with%20spaces".to_string(),
        ));
        assert_eq!(
            result,
            Some(HashMap::from([(
                "key with spaces".to_string(),
                "value with spaces".to_string()
            )]))
        );
    }

    #[test]
    fn cold_start_resources_maps_and_validates_optional_disk_mb() {
        let mut body = models::NewColdSandbox::new("ubuntu:24.04".to_string());
        body.cpu_count = Some(2);
        body.memory_mb = Some(512);

        let resources = cold_start_resources(&body).unwrap();
        assert_eq!(resources.cpu_count, 2);
        assert_eq!(resources.memory_mib, 512);
        assert_eq!(resources.disk_size_mib, 0);

        body.disk_size_mb = Some(8192);
        assert_eq!(cold_start_resources(&body).unwrap().disk_size_mib, 8192);

        body.disk_size_mb = Some(0);
        assert!(cold_start_resources(&body).is_err());

        body.disk_size_mb = Some(512);
        assert!(cold_start_resources(&body).is_err());

        body.disk_size_mb = Some(1536);
        assert!(cold_start_resources(&body).is_err());
    }

    #[test]
    fn build_image_configs_preserves_rootfs_and_attached_drives() {
        let rootfs_config = serde_json::json!({
            "Cmd": ["/bin/bash"],
            "WorkingDir": "/workspace"
        });
        let rootfs = ResolvedBlockImage {
            image_ref: "ubuntu:24.04".to_string(),
            overlaybd_config_path: "/tmp/rootfs-image.json".into(),
            base_context: crate::image::ImageBaseContext::default(),
            raw_config: Some(rootfs_config.clone()),
        };
        let drive_config = serde_json::json!({
            "Env": ["DATA=1"]
        });
        let attached = super::super::attached_drives::ResolvedAttachedDrive {
            drive: crate::sandbox::ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/data-image.json",
                true,
                "/data",
                None::<std::path::PathBuf>,
            )
            .expect("valid test drive"),
            raw_config: Some(drive_config.clone()),
        };

        let image_configs = build_image_configs(&rootfs, &[attached]);

        assert_eq!(image_configs.len(), 2);
        let entries = image_configs.entries();
        assert_eq!(entries[0].drive_id, None);
        assert_eq!(entries[0].mount_path, "/");
        assert_eq!(entries[0].config, rootfs_config);
        assert_eq!(entries[1].drive_id.as_deref(), Some("data"));
        assert_eq!(entries[1].mount_path, "/data");
        assert_eq!(entries[1].config, drive_config);
    }

    #[test]
    fn network_update_replaces_base_policy_and_egress() {
        let body = models::SandboxNetworkUpdateConfig {
            allow_out: Some(vec!["8.8.8.8".to_string()]),
            deny_out: Some(vec!["203.0.113.0/24".to_string()]),
            allow_internet_access: Some(false),
        };

        let policy = network_policy_from_update(&body).unwrap();

        assert_eq!(policy.base_policy, BaseSandboxNetworkPolicy::Deny);
        assert_eq!(
            policy.egress.allowed_cidrs,
            vec!["8.8.8.8/32".parse().unwrap()]
        );
        assert_eq!(
            policy.egress.denied_cidrs,
            vec!["203.0.113.0/24".parse().unwrap()]
        );
    }

    #[test]
    fn network_create_preserves_private_ingress() {
        let mut network = models::SandboxNetworkConfig::new();
        network.allow_public_traffic = Some(false);

        let policy = network_policy_from_create(None, Some(&network)).unwrap();

        assert!(!policy.allow_public_traffic);
        assert_eq!(
            models::SandboxNetworkConfig::from(&policy).allow_public_traffic,
            Some(false)
        );
    }

    #[test]
    fn empty_network_update_clears_base_policy_and_egress() {
        let policy = network_policy_from_update(&models::SandboxNetworkUpdateConfig::new())
            .expect("empty update should be valid");

        assert_eq!(policy, SandboxNetworkPolicy::default());
    }

    #[test]
    fn network_create_accepts_domain_allowlist_with_explicit_deny_all() {
        let network = models::SandboxNetworkConfig {
            allow_out: Some(vec!["example.com".to_string()]),
            deny_out: Some(vec!["0.0.0.0/0".to_string()]),
            ..models::SandboxNetworkConfig::new()
        };

        let policy = network_policy_from_create(None, Some(&network)).unwrap();
        assert_eq!(policy.egress.allowed_domains, ["example.com"]);
        assert_eq!(
            policy.egress.denied_cidrs,
            vec!["0.0.0.0/0".parse().unwrap()]
        );
    }

    #[test]
    fn network_create_rejects_ipv6_cidrs() {
        let network = models::SandboxNetworkConfig {
            allow_out: Some(vec!["2001:db8::/32".to_string()]),
            ..models::SandboxNetworkConfig::new()
        };

        let error = network_policy_from_create(None, Some(&network)).unwrap_err();
        assert!(error.to_string().contains("IPv6 CIDRs"));
    }

    #[test]
    fn network_create_rejects_domain_allowlist_without_explicit_deny_all() {
        let network = models::SandboxNetworkConfig {
            allow_out: Some(vec!["example.com".to_string()]),
            ..models::SandboxNetworkConfig::new()
        };

        let error = network_policy_from_create(None, Some(&network)).unwrap_err();
        assert!(error.to_string().contains("0.0.0.0/0"));
    }

    #[test]
    fn network_update_accepts_domain_allowlist_with_explicit_deny_all() {
        let body = models::SandboxNetworkUpdateConfig {
            allow_out: Some(vec!["example.com".to_string()]),
            deny_out: Some(vec!["0.0.0.0/0".to_string()]),
            allow_internet_access: None,
        };
        let policy = network_policy_from_update(&body).unwrap();
        assert_eq!(policy.egress.allowed_domains, ["example.com"]);
        assert_eq!(
            policy.egress.denied_cidrs,
            vec!["0.0.0.0/0".parse().unwrap()]
        );
    }

    #[test]
    fn network_update_rejects_ipv6_cidrs() {
        let body = models::SandboxNetworkUpdateConfig {
            allow_out: Some(vec!["2001:db8::/32".to_string()]),
            deny_out: None,
            allow_internet_access: None,
        };

        let error = network_policy_from_update(&body).unwrap_err();
        assert!(error.to_string().contains("IPv6 CIDRs"));
    }

    #[test]
    fn network_update_rejects_domain_allowlist_without_explicit_deny_all() {
        let body = models::SandboxNetworkUpdateConfig {
            allow_out: Some(vec!["example.com".to_string()]),
            deny_out: None,
            allow_internet_access: None,
        };
        let error = network_policy_from_update(&body).unwrap_err();
        assert!(error.to_string().contains("0.0.0.0/0"));
    }
}
