//! Real processes and filesystem failures; logical network reservations only.
//! These tests never create/delete host interfaces or mounts.
use super::*;
use std::os::unix::fs::PermissionsExt;
use warm_pool::PoolConfig;

fn pool(capacity: usize) -> FirecrackerPool {
    FirecrackerPool::new(
        PathBuf::from("/bin/true"),
        ResolvedFirecrackerPoolConfig {
            pool: PoolConfig {
                low_watermark: 0,
                high_watermark: capacity,
                maintenance_enabled: false,
                startup_prewarm: false,
            },
            fill_concurrency: 2,
        },
        Builder::new_current_thread().enable_all().build().unwrap(),
    )
}

fn warm(pool: &FirecrackerPool, network: &NetworkManager) -> Result<WarmFirecracker> {
    Ok(WarmFirecracker::new(
        resources(network)?,
        &pool.cleanup_pending,
    ))
}

fn resources(network: &NetworkManager) -> Result<WarmResources> {
    let work_dir = tempfile::tempdir()?;
    Ok(WarmResources {
        slot: Some(network.allocate_test_slot()?),
        fc_instance: FirecrackerInstance::new(work_dir.path().to_path_buf()),
        work_dir,
    })
}

fn script(dir: &Path, ready: bool) -> Result<PathBuf> {
    let path = dir.join("firecracker-fixture");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\ntrap '' TERM\nprintf '%s' $$ > started\n{}exec sleep 60\n",
            if ready {
                "touch firecracker.socket\n"
            } else {
                ""
            }
        ),
    )?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    Ok(path)
}

async fn wait_started(dir: &Path) -> Result<nix::unistd::Pid> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(dir.join("started")) {
                if let Ok(pid) = pid.parse() {
                    return nix::unistd::Pid::from_raw(pid);
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("fixture child did not start")
}

async fn start(warm: &mut WarmFirecracker) -> Result<nix::unistd::Pid> {
    let resources = warm.resources.as_mut().unwrap();
    let binary = script(resources.work_dir.path(), true)?;
    resources
        .fc_instance
        .spawn_with_netns(&binary, None, None, None)
        .await?;
    wait_started(resources.work_dir.path()).await
}

fn kill(pid: nix::unistd::Pid) -> Result<()> {
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL)?;
    Ok(())
}

#[test]
fn cancelled_shutdown_and_retry_retain_all_warm_entries() -> Result<()> {
    let pool = pool(2);
    let network = NetworkManager::new(false, 0, 0);
    pool.runtime.block_on(async {
        let mut owners = Vec::new();
        for _ in 0..2 {
            let mut warm = warm(&pool, &network)?;
            let pid = start(&mut warm).await?;
            let resources = warm.resources.as_ref().unwrap();
            owners.push((
                pid,
                resources.work_dir.path().to_path_buf(),
                resources.slot.as_ref().unwrap().idx,
            ));
            assert!(pool.pool.try_push_bounded(warm).is_ok());
        }
        for _ in 0..2 {
            assert!(tokio::time::timeout(
                Duration::from_millis(25),
                pool.shutdown_with_network(&network)
            )
            .await
            .is_err());
            assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 2);
            for (pid, dir, idx) in &owners {
                assert!(PathBuf::from(format!("/proc/{pid}")).exists());
                assert!(dir.exists());
                assert!(network.allocate_slot(*idx).is_err());
            }
        }
        for (pid, _, _) in &owners {
            kill(*pid)?;
        }
        pool.shutdown_with_network(&network).await?;
        assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
        for (pid, dir, idx) in owners {
            assert!(
                !PathBuf::from(format!("/proc/{pid}")).exists(),
                "stop must reap the child"
            );
            assert!(!dir.exists());
            network.cleanup_allocated_slot(network.allocate_slot(idx)?, false)?;
        }
        Ok(())
    })
}

#[test]
fn cancelled_overflow_cleanup_retains_unvisited_batch_entries() -> Result<()> {
    let pool = pool(0);
    let network = NetworkManager::new(false, 0, 0);
    pool.runtime.block_on(async {
        let mut batch = Vec::new();
        let mut pids = Vec::new();
        for _ in 0..2 {
            let mut warm = warm(&pool, &network)?;
            pids.push(start(&mut warm).await?);
            batch.push(Ok(warm));
        }
        assert!(tokio::time::timeout(
            Duration::from_millis(25),
            pool.publish_warm_batch(batch, &network)
        )
        .await
        .is_err());
        assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 2);
        for pid in pids {
            kill(pid)?;
        }
        pool.shutdown_with_network(&network).await?;
        assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
        Ok(())
    })
}

#[test]
fn creation_cancellation_retains_process_directory_and_slot() -> Result<()> {
    let mut pool = pool(0);
    let network = NetworkManager::new(false, 0, 0);
    let work_dir = tempfile::tempdir()?;
    let path = work_dir.path().to_path_buf();
    pool.binary = script(&path, false)?;
    pool.socket_timeout = Duration::from_secs(30);
    let slot = network.allocate_test_slot()?;
    let idx = slot.idx;
    pool.runtime.block_on(async {
        {
            let warm = WarmFirecracker::new(
                WarmResources {
                    slot: Some(slot),
                    fc_instance: FirecrackerInstance::new(work_dir.path().into()),
                    work_dir,
                },
                &pool.cleanup_pending,
            );
            let creation = pool.start_warm(warm, None);
            tokio::pin!(creation);
            tokio::select! {
                _ = wait_started(&path) => {},
                _ = &mut creation => panic!("creation cannot finish without API socket"),
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut creation)
                    .await
                    .is_err()
            );
        }
        assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 1);
        assert!(path.exists());
        assert!(network.allocate_slot(idx).is_err());
        kill(wait_started(&path).await?)?;
        pool.shutdown_with_network(&network).await?;
        assert!(!path.exists());
        network.cleanup_allocated_slot(network.allocate_slot(idx)?, false)?;
        Ok(())
    })
}

#[test]
fn cleanup_failure_retains_owner_and_does_not_skip_other_entries() -> Result<()> {
    let pool = pool(2);
    let network = NetworkManager::new(false, 0, 0);
    let failed = warm(&pool, &network)?;
    let resources = failed.resources.as_ref().unwrap();
    let path = resources.work_dir.path().to_path_buf();
    let idx = resources.slot.as_ref().unwrap().idx;
    std::fs::create_dir(path.join("firecracker.socket"))?;
    let good = warm(&pool, &network)?;
    let good_path = good
        .resources
        .as_ref()
        .unwrap()
        .work_dir
        .path()
        .to_path_buf();
    assert!(pool.pool.try_push_bounded(failed).is_ok());
    assert!(pool.pool.try_push_bounded(good).is_ok());
    pool.runtime.block_on(async {
        for _ in 0..2 {
            assert!(pool.shutdown_with_network(&network).await.is_err());
            assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 1);
            assert!(network.allocate_slot(idx).is_err());
            assert!(path.exists());
        }
        assert!(
            !good_path.exists(),
            "an unrelated owner must still be cleaned"
        );
        std::fs::remove_dir(path.join("firecracker.socket"))?;
        pool.shutdown_with_network(&network).await?;
        assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
        assert!(!path.exists());
        network.cleanup_allocated_slot(network.allocate_slot(idx)?, false)?;
        Ok(())
    })
}

#[test]
fn network_cleanup_failure_keeps_directory_and_exact_slot_for_retry() -> Result<()> {
    let pool = pool(0);
    let owner = NetworkManager::new(false, 0, 0);
    let foreign = NetworkManager::new(false, 0, 0);
    let warm = warm(&pool, &owner)?;
    let resources = warm.resources.as_ref().unwrap();
    let identity = resources.slot.as_ref().unwrap().namespace_id.clone();
    let path = resources.work_dir.path().to_path_buf();
    pool.runtime.block_on(async {
        assert!(pool.cleanup_warm_async(warm, &foreign).await.is_err());
        assert!(path.exists());
        assert_eq!(
            pool.cleanup_pending.lock().unwrap().pending[0]
                .slot
                .as_ref()
                .unwrap()
                .namespace_id,
            identity
        );
        pool.shutdown_with_network(&owner).await?;
        assert!(!path.exists());
        assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
        Ok(())
    })
}

#[test]
fn successful_acquisition_transfers_without_scheduling_cleanup() -> Result<()> {
    let pool = pool(1);
    let network = NetworkManager::new(false, 0, 0);
    let warm = warm(&pool, &network)?;
    assert!(pool.pool.try_push_bounded(warm).is_ok());
    let (slot, mut instance, work_dir) = pool.try_acquire().unwrap().into_parts();
    assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
    assert!(work_dir.path().exists());
    instance.stop_blocking(Duration::from_millis(10))?;
    network.cleanup_allocated_slot(slot, false)?;
    work_dir.close()?;
    Ok(())
}

#[test]
fn concurrent_shutdown_cannot_confirm_another_passes_active_cleanup() -> Result<()> {
    let pool = pool(1);
    let network = NetworkManager::new(false, 0, 0);
    pool.runtime.block_on(async {
        let mut warm = warm(&pool, &network)?;
        let pid = start(&mut warm).await?;
        assert!(pool.pool.try_push_bounded(warm).is_ok());
        let first = pool.shutdown_with_network(&network);
        tokio::pin!(first);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut first)
            .await
            .is_err());
        let second = pool.shutdown_with_network(&network).await;
        kill(pid)?;
        first.await?;
        pool.shutdown_with_network(&network).await?;
        assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 0);
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("still owns 1 entries"));
        Ok(())
    })
}

#[test]
fn blocking_cleanup_failure_retains_group_for_a_later_pass() -> Result<()> {
    let pool = pool(0);
    let network = NetworkManager::new(false, 0, 0);
    let warm = warm(&pool, &network)?;
    let resources = warm.resources.as_ref().unwrap();
    let path = resources.work_dir.path().to_path_buf();
    let idx = resources.slot.as_ref().unwrap().idx;
    std::fs::create_dir(path.join("firecracker.socket"))?;
    assert!(pool.cleanup_warm_blocking(warm, true, &network).is_err());
    assert!(path.exists());
    assert!(network.allocate_slot(idx).is_err());
    assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 1);
    std::fs::remove_dir(path.join("firecracker.socket"))?;
    for warm in pool.take_pending_cleanup() {
        pool.cleanup_warm_blocking(warm, true, &network)?;
    }
    assert!(!path.exists());
    assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 0);
    network.cleanup_allocated_slot(network.allocate_slot(idx)?, false)?;
    Ok(())
}

#[test]
fn concurrent_creators_reserve_capacity_before_allocating_resources() -> Result<()> {
    let pool = pool(2);
    let gate = std::sync::Barrier::new(16);
    let mut owners = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|_| {
                scope.spawn(|| {
                    gate.wait();
                    pool.reserve_warm_capacity()
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap().ok())
            .collect::<Vec<_>>()
    });
    assert_eq!(owners.len(), 2);
    assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 2);
    assert!(pool.reserve_warm_capacity().is_err());
    drop(owners.pop()); // Cancel before any allocation.
    let replacement = pool.reserve_warm_capacity()?;
    assert!(replacement.resources.is_none());
    drop(replacement);
    drop(owners);
    assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 0);
    assert!(pool.cleanup_pending.lock().unwrap().pending.is_empty());
    Ok(())
}

#[test]
fn unresolved_warm_cleanup_blocks_replacement_until_confirmed_release() -> Result<()> {
    let mut pool = pool(1);
    let network = NetworkManager::new(false, 0, 0);
    let mut owner = pool.reserve_warm_capacity()?;
    owner.resources = Some(resources(&network)?);
    let path = owner
        .resources
        .as_ref()
        .unwrap()
        .work_dir
        .path()
        .to_path_buf();
    let obstruction = path.join("not-a-directory");
    std::fs::write(
        &obstruction,
        "no host network allocation even if the capacity guard regresses",
    )?;
    pool.firecracker_work_base_dir = Some(obstruction);
    std::fs::create_dir(path.join("firecracker.socket"))?;
    pool.runtime.block_on(async {
        assert!(pool.cleanup_warm_async(owner, &network).await.is_err());
        for _ in 0..3 {
            let error = pool
                .create_warm_async()
                .await
                .err()
                .expect("capacity must remain occupied");
            assert!(error.to_string().contains("capacity is occupied"));
            assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 1);
            assert_eq!(pool.cleanup_pending.lock().unwrap().pending.len(), 1);
        }
        std::fs::remove_dir(path.join("firecracker.socket"))?;
        for owner in pool.take_pending_cleanup() {
            pool.cleanup_warm_async(owner, &network).await?;
        }
        let next = pool.reserve_warm_capacity()?;
        assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 1);
        drop(next);
        Ok(())
    })
}

#[test]
fn preparation_failure_returns_reserved_capacity_and_shutdown_rejects_new_creation() -> Result<()> {
    let mut pool = pool(1);
    let temp = tempfile::tempdir()?;
    let obstruction = temp.path().join("not-a-directory");
    std::fs::write(&obstruction, "obstruction")?;
    pool.firecracker_work_base_dir = Some(obstruction);
    pool.runtime.block_on(async {
        for _ in 0..3 {
            let error = pool
                .create_warm_async()
                .await
                .err()
                .expect("work dir must fail before networking");
            assert!(error.to_string().contains("create work dir"));
            assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 0);
        }
        pool.pool.drain_all();
        let error = pool
            .create_warm_async()
            .await
            .err()
            .expect("shutdown rejects new work");
        assert!(error.to_string().contains("shutting down"));
        assert_eq!(pool.cleanup_pending.lock().unwrap().outstanding, 0);
        Ok(())
    })
}
