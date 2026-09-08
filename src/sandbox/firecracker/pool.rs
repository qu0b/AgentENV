//! Pre-spawned Firecracker process pool.
//!
//! Each warm entry owns a network slot, a running Firecracker process, and the
//! working directory that process uses as CWD. Snapshot resume can consume an
//! entry to skip process spawn and API socket polling on the critical path.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use nix::libc;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};
use tracing::{debug, info, warn};
use warm_pool::{MaintenanceOutcome, PoolMaintenanceAction, WarmPool};

use super::config::create_firecracker_work_dir;
use super::FirecrackerInstance;
use crate::cfg::{ConfigManager, ResolvedFirecrackerPoolConfig};
use crate::sandbox::network::{NetworkManager, Slot};

const POOL_FIRECRACKER_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const POOL_PRIME_POLL_INTERVAL: Duration = Duration::from_millis(20);

static POOL: OnceLock<Option<FirecrackerPool>> = OnceLock::new();

extern "C" fn firecracker_pool_exit_hook() {
    let _ = std::panic::catch_unwind(|| {
        if let Some(Some(pool)) = POOL.get() {
            if let Err(err) = pool.shutdown_for_process_exit() {
                warn!(error = %err, "firecracker pool shutdown on process exit failed");
            }
        }
    });
}

fn register_process_exit_hook(handler: extern "C" fn()) -> i32 {
    // SAFETY: `handler` uses C ABI and `atexit` accepts callbacks with signature `extern "C" fn()`.
    unsafe { libc::atexit(handler) }
}

/// One warm entry handed to a sandbox as an indivisible ownership unit.
pub(crate) struct WarmFirecracker {
    resources: Option<WarmResources>,
    cleanup_pending: Arc<Mutex<WarmCleanupState>>,
    owns_capacity: bool,
}

#[derive(Default)]
struct WarmCleanupState {
    pending: Vec<WarmResources>,
    outstanding: usize,
}

struct WarmResources {
    slot: Option<Slot>,
    fc_instance: FirecrackerInstance,
    work_dir: TempDir,
}

impl WarmFirecracker {
    #[cfg(test)]
    fn new(resources: WarmResources, cleanup_pending: &Arc<Mutex<WarmCleanupState>>) -> Self {
        let mut owner = Self::reserve(cleanup_pending, usize::MAX).unwrap();
        owner.resources = Some(resources);
        owner
    }

    fn reserve(cleanup_pending: &Arc<Mutex<WarmCleanupState>>, limit: usize) -> Result<Self> {
        let mut state = cleanup_pending
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        anyhow::ensure!(state.outstanding < limit,
            "warm Firecracker capacity is occupied by {} ready, creating or unresolved owners (limit {limit})", state.outstanding);
        state.outstanding += 1;
        Ok(Self {
            resources: None,
            cleanup_pending: Arc::clone(cleanup_pending),
            owns_capacity: true,
        })
    }

    fn from_pending(
        resources: WarmResources,
        cleanup_pending: &Arc<Mutex<WarmCleanupState>>,
    ) -> Self {
        Self {
            resources: Some(resources),
            cleanup_pending: Arc::clone(cleanup_pending),
            owns_capacity: true,
        }
    }

    fn release_ownership(&mut self) -> WarmResources {
        let resources = self.resources.take().expect("owned warm resources");
        self.owns_capacity = false;
        self.cleanup_pending
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .outstanding -= 1;
        resources
    }

    /// Transfer all resources synchronously to the native sandbox. Only intact,
    /// ready entries are ever published to the warm pool.
    pub(crate) fn into_parts(mut self) -> (Slot, FirecrackerInstance, TempDir) {
        let resources = self.release_ownership();
        (
            resources.slot.expect("ready warm slot"),
            resources.fc_instance,
            resources.work_dir,
        )
    }
}

impl Drop for WarmFirecracker {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            // No I/O or async work in Drop. This also protects every unvisited
            // entry in a cancelled batch, and partially completed creation.
            self.cleanup_pending
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .pending
                .push(resources);
        } else if self.owns_capacity {
            // Cancellation/failure before allocating resources frees only the
            // reserved warm capacity. A populated owner stays counted in pending.
            self.cleanup_pending
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .outstanding -= 1;
        }
    }
}

impl WarmResources {
    fn finish_cleanup(&mut self, network: &NetworkManager, sync_cleanup: bool) -> Result<()> {
        network
            .cleanup_retained(&mut self.slot, sync_cleanup)
            .context("firecracker pool: cleanup warm network slot")?;
        // TempDir::drop hides deletion errors. Explicit removal retains the
        // original directory owner on error and permits a later retry.
        match std::fs::remove_dir_all(self.work_dir.path()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).context("firecracker pool: remove warm work dir"),
        }
    }
}

pub struct FirecrackerPool {
    pool: WarmPool<WarmFirecracker>,
    cleanup_pending: Arc<Mutex<WarmCleanupState>>,
    binary: PathBuf,
    socket_timeout: Duration,
    socket_poll_interval: Duration,
    fill_concurrency: usize,
    firecracker_work_base_dir: Option<PathBuf>,
    runtime: Runtime,
}

impl FirecrackerPool {
    /// Returns the global pool when `[pool.firecracker].enabled = true`.
    pub fn global() -> Option<&'static Self> {
        let entry = POOL.get_or_init(|| match Self::try_init() {
            Ok(pool) => pool,
            Err(err) => {
                warn!(
                    error = %err,
                    "firecracker pool init failed; falling back to cold spawn"
                );
                None
            }
        });

        let pool = entry.as_ref()?;
        pool.ensure_worker_started();
        Some(pool)
    }

    fn try_init() -> Result<Option<Self>> {
        let cfg = ConfigManager::global_config();
        let Some(pool_config) = cfg.firecracker_pool_config() else {
            return Ok(None);
        };

        // The pool is a process-wide singleton and can outlive the Tokio
        // runtime that first touched it in tests and benchmarks. Keep a small
        // owned runtime for the synchronous maintenance thread instead of
        // storing `Handle::current()`. A single worker is enough: maintenance
        // only runs occasional I/O-bound `block_on` calls (spawn Firecracker,
        // poll its API socket), while the default multi-thread runtime would
        // park one worker thread per CPU core for its entire lifetime.
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("firecracker-pool-runtime")
            .build()
            .context("firecracker pool: create runtime")?;

        let binary = cfg.resolved_firecracker_binary_path();

        let rc = register_process_exit_hook(firecracker_pool_exit_hook);
        if rc != 0 {
            warn!(
                code = rc,
                "failed to register firecracker pool process-exit hook"
            );
        }

        Ok(Some(Self::new(binary, pool_config, runtime)))
    }

    fn new(binary: PathBuf, pool_config: ResolvedFirecrackerPoolConfig, runtime: Runtime) -> Self {
        let app_config = ConfigManager::global_config();
        let socket_timeout = Duration::from_secs(app_config.firecracker.socket_timeout_secs);
        let socket_poll_interval = Duration::from_millis(app_config.firecracker.socket_poll_ms);
        let firecracker_work_base_dir = app_config.firecracker.work_dir.clone();

        Self {
            pool: WarmPool::new(pool_config.pool),
            cleanup_pending: Arc::new(Mutex::new(WarmCleanupState::default())),
            binary,
            socket_timeout,
            socket_poll_interval,
            fill_concurrency: pool_config.fill_concurrency,
            firecracker_work_base_dir,
            runtime,
        }
    }

    fn ensure_worker_started(&'static self) {
        self.pool.start_maintenance_worker(move || {
            if let Err(err) = self.run_maintenance_cycle() {
                warn!(error = %err, "firecracker pool maintenance cycle failed");
                MaintenanceOutcome::Retry
            } else {
                MaintenanceOutcome::Complete
            }
        });
    }

    pub(crate) fn try_acquire(&self) -> Option<WarmFirecracker> {
        let warm = self.pool.try_acquire()?;
        if self.pool.len() < self.pool.config().low_watermark {
            self.pool.request_maintenance();
        }
        Some(warm)
    }

    pub fn warm_len(&self) -> usize {
        self.pool.len()
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_with_network(NetworkManager::global()).await
    }

    async fn shutdown_with_network(&self, network: &NetworkManager) -> Result<()> {
        let mut drained = self.pool.drain_all();
        drained.extend(self.take_pending_cleanup());
        let mut failures = Vec::new();
        for warm in drained {
            if let Err(err) = self.cleanup_warm_async(warm, network).await {
                failures.push(err.to_string());
            }
        }

        self.shutdown_result(failures)
    }

    fn shutdown_for_process_exit(&self) -> Result<()> {
        self.shutdown_blocking(true)
    }

    fn shutdown_blocking(&self, sync_network_cleanup: bool) -> Result<()> {
        let mut drained = self.pool.drain_all();
        drained.extend(self.take_pending_cleanup());
        let mut failures = Vec::new();
        for warm in drained {
            if let Err(err) =
                self.cleanup_warm_blocking(warm, sync_network_cleanup, NetworkManager::global())
            {
                failures.push(err.to_string());
            }
        }

        self.shutdown_result(failures)
    }

    fn shutdown_result(&self, mut failures: Vec<String>) -> Result<()> {
        let outstanding = self
            .cleanup_pending
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .outstanding;
        if outstanding != 0 {
            failures.push(format!(
                "warm Firecracker cleanup still owns {outstanding} entries"
            ));
        }
        firecracker_pool_cleanup_result(failures)
    }

    /// Eagerly initialize the pool and wait until `low_watermark` warm entries
    /// exist, or until `timeout` elapses. This is best-effort; timeout is not an
    /// error because the cold path remains available.
    pub async fn prime(timeout: Duration) -> Result<()> {
        let Some(pool) = Self::global() else {
            debug!("firecracker pool disabled; skipping prime");
            return Ok(());
        };

        if !pool.pool.config().maintenance_enabled {
            debug!("firecracker pool maintenance disabled; skipping prime");
            return Ok(());
        }

        if !pool.pool.config().startup_prewarm {
            debug!("firecracker pool startup prewarm disabled; skipping prime");
            return Ok(());
        }

        let target = pool.pool.config().low_watermark;
        if target == 0 || pool.warm_len() >= target {
            return Ok(());
        }

        info!(
            low_watermark = target,
            current = pool.warm_len(),
            timeout_ms = timeout.as_millis(),
            "priming firecracker pool"
        );

        let started = Instant::now();
        loop {
            if pool.warm_len() >= target {
                info!(
                    warm = pool.warm_len(),
                    elapsed_ms = started.elapsed().as_millis(),
                    "firecracker pool primed"
                );
                return Ok(());
            }
            if started.elapsed() >= timeout {
                warn!(
                    warm = pool.warm_len(),
                    target, "firecracker pool prime timed out; continuing with partial warm-up"
                );
                return Ok(());
            }
            tokio::time::sleep(POOL_PRIME_POLL_INTERVAL).await;
        }
    }

    fn run_maintenance_cycle(&self) -> Result<()> {
        let mut failures = Vec::new();
        for warm in self.take_pending_cleanup() {
            if let Err(err) = self.cleanup_warm_blocking(warm, false, NetworkManager::global()) {
                failures.push(err.to_string());
            }
        }
        match self.pool.compute_maintenance_action(self.pool.len()) {
            PoolMaintenanceAction::Fill(to_fill) => {
                if let Err(err) = self.runtime.block_on(self.fill_warm_entries(to_fill)) {
                    failures.push(err.to_string());
                }
            }
            PoolMaintenanceAction::Drain(to_drain) => {
                for _ in 0..to_drain {
                    let Some(warm) = self.pool.try_drain_one() else {
                        break;
                    };
                    if let Err(err) =
                        self.cleanup_warm_blocking(warm, false, NetworkManager::global())
                    {
                        failures.push(err.to_string());
                    }
                }
            }
            PoolMaintenanceAction::Idle => {}
        }

        firecracker_pool_cleanup_result(failures)
    }

    async fn fill_warm_entries(&self, to_fill: usize) -> Result<()> {
        let mut remaining = to_fill;
        let mut cleanup_failures = Vec::new();
        while remaining > 0 && !self.pool.is_shutting_down() {
            // `remaining` tracks refill attempts left in this maintenance action.
            // On any create failure we finish processing the current batch, then
            // stop launching additional batches to preserve the old serial
            // refill behavior.
            let batch_size = remaining.min(self.fill_concurrency);
            let results = join_all((0..batch_size).map(|_| self.create_warm_async())).await;
            let (saw_failure, failures) = self
                .publish_warm_batch(results, NetworkManager::global())
                .await;
            cleanup_failures.extend(failures);

            if saw_failure {
                return Err(anyhow!(
                    "firecracker pool refill failed: {}",
                    cleanup_failures.join(" | ")
                ));
            }
            remaining -= batch_size;
        }

        firecracker_pool_cleanup_result(cleanup_failures)?;
        Ok(())
    }

    async fn publish_warm_batch(
        &self,
        results: Vec<Result<WarmFirecracker>>,
        network: &NetworkManager,
    ) -> (bool, Vec<String>) {
        let mut cleanup_failures = Vec::new();
        let mut saw_failure = false;

        for result in results {
            match result {
                Ok(warm) => {
                    if let Err(warm) = self.pool.try_push_bounded(warm) {
                        if let Err(err) = self.cleanup_warm_async(warm, network).await {
                            warn!(
                                error = %err,
                                "firecracker pool: cleanup of unqueued warm entry failed"
                            );
                            cleanup_failures.push(err.to_string());
                        }
                    }
                }
                Err(err) => {
                    saw_failure = true;
                    debug!(error = %err, "skipping firecracker pool refill attempt");
                    cleanup_failures.push(err.to_string());
                }
            }
        }

        (saw_failure, cleanup_failures)
    }

    #[tracing::instrument(skip(self))]
    async fn create_warm_async(&self) -> Result<WarmFirecracker> {
        // Reserve under the ownership lock before any filesystem, network or
        // process allocation. Pending cleanup consumes the same existing cap.
        let mut warm = self.reserve_warm_capacity()?;
        let work_dir = create_firecracker_work_dir(self.firecracker_work_base_dir.as_deref())
            .context("firecracker pool: create work dir")?;
        let slot = NetworkManager::global()
            .allocate_any()
            .context("firecracker pool: allocate network slot")?;

        let namespace = slot.namespace_path();
        warm.resources = Some(WarmResources {
            fc_instance: FirecrackerInstance::new(work_dir.path().to_path_buf()),
            slot: Some(slot),
            work_dir,
        });
        self.start_warm(warm, Some(namespace)).await
    }

    fn reserve_warm_capacity(&self) -> Result<WarmFirecracker> {
        anyhow::ensure!(
            !self.pool.is_shutting_down(),
            "warm Firecracker pool is shutting down"
        );
        WarmFirecracker::reserve(&self.cleanup_pending, self.pool.config().high_watermark)
    }

    async fn start_warm(
        &self,
        mut warm: WarmFirecracker,
        namespace: Option<PathBuf>,
    ) -> Result<WarmFirecracker> {
        // Publish cleanup ownership before the first await, including the
        // launcher's outcome receiver while its thread is still starting.
        let resources = warm.resources.as_mut().expect("owned warm resources");
        let stdout_path = warm_stdout_path(resources.work_dir.path());
        let stderr_path = warm_stderr_path(resources.work_dir.path());
        resources
            .fc_instance
            .spawn_with_netns(
                &self.binary,
                Some(&stdout_path),
                Some(&stderr_path),
                namespace.as_deref(),
            )
            .await
            .context("spawn warm firecracker process")?;
        resources
            .fc_instance
            .wait_for_ready(self.socket_timeout, self.socket_poll_interval)
            .await
            .context("wait for warm firecracker api socket")?;

        debug!(
            slot = resources.slot.as_ref().expect("warm slot").idx,
            work_dir = %resources.work_dir.path().display(),
            "firecracker pool warm entry ready"
        );
        Ok(warm)
    }

    fn take_pending_cleanup(&self) -> Vec<WarmFirecracker> {
        // Wrap the entire batch before any await so cancellation requeues even
        // entries the cleanup loop has not visited. Concurrent passes are disjoint.
        std::mem::take(
            &mut self
                .cleanup_pending
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .pending,
        )
        .into_iter()
        .map(|resources| WarmFirecracker::from_pending(resources, &self.cleanup_pending))
        .collect()
    }

    fn cleanup_warm_blocking(
        &self,
        mut warm: WarmFirecracker,
        sync_network_cleanup: bool,
        network: &NetworkManager,
    ) -> Result<()> {
        let resources = warm.resources.as_mut().expect("owned warm resources");
        if sync_network_cleanup {
            resources
                .fc_instance
                .stop_blocking(POOL_FIRECRACKER_STOP_TIMEOUT)?;
        } else {
            self.runtime
                .block_on(resources.fc_instance.stop(POOL_FIRECRACKER_STOP_TIMEOUT))?;
        }
        resources.finish_cleanup(network, sync_network_cleanup)?;
        drop(warm.release_ownership());
        Ok(())
    }

    async fn cleanup_warm_async(
        &self,
        mut warm: WarmFirecracker,
        network: &NetworkManager,
    ) -> Result<()> {
        let resources = warm.resources.as_mut().expect("owned warm resources");
        resources
            .fc_instance
            .stop(POOL_FIRECRACKER_STOP_TIMEOUT)
            .await?;
        resources.finish_cleanup(network, false)?;
        drop(warm.release_ownership());
        Ok(())
    }
}

fn firecracker_pool_cleanup_result(failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "failed to clean up firecracker pool entries: {}",
            failures.join(" | ")
        ))
    }
}

pub(crate) fn warm_stdout_path(work_dir: &Path) -> PathBuf {
    work_dir.join("firecracker-stdout.log")
}

pub(crate) fn warm_stderr_path(work_dir: &Path) -> PathBuf {
    work_dir.join("firecracker-stderr.log")
}

#[cfg(test)]
#[path = "pool_cleanup_tests.rs"]
mod cleanup_tests;
