use super::*;
use crate::sandbox::ublk::test_support::{
    lease, memory_spec, raw_acquire, raw_runtime, release, step, Step, TestDaemon,
};
use std::time::Duration;
use tokio::sync::Notify;
use uvm_ublk_daemon::{
    protocol::{AccessMode, DaemonRequest, DaemonResponse},
    DeviceLease,
};

fn sandbox() -> Result<FirecrackerSandbox> {
    FirecrackerSandbox::new(FirecrackerSandboxConfig::new(
        "firecracker".into(),
        "vmlinux.bin".into(),
        "0.1.0".into(),
        "user-image.json".into(),
    ))
}

#[test]
fn network_release_error_retains_the_original_sandbox_slot() -> Result<()> {
    let owner = NetworkManager::new(false, 0, 0);
    let foreign = NetworkManager::new(false, 0, 0);
    let mut sandbox = sandbox()?;
    let slot = owner.allocate_test_slot()?;
    let original = slot.namespace_id.clone();
    sandbox.network_slot = Some(slot);
    assert!(sandbox.release_network_slot(&foreign).is_err());
    assert_eq!(
        sandbox.network_slot.as_ref().unwrap().namespace_id,
        original
    );
    sandbox.release_network_slot(&owner)?;
    assert!(sandbox.network_slot.is_none());
    Ok(())
}

#[tokio::test]
async fn cancelled_release_retains_each_original_device_receipt() -> Result<()> {
    for kind in 0..3 {
        let receipt = lease(7);
        let entered = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let expected = receipt.clone();
        let started = Arc::clone(&entered);
        let completed = Arc::clone(&finish);
        let daemon = TestDaemon::start(
            vec![
                raw_acquire(receipt.clone()),
                step(move |request| async move {
                    let DaemonRequest::ReleaseOwned { lease } = request else {
                        panic!("owned release")
                    };
                    assert_eq!(lease, expected);
                    started.notify_one();
                    completed.notified().await;
                    None // The cancelled client will never receive this outcome.
                }),
                release(receipt, true),
            ],
            false,
        )?;
        let mut sandbox = sandbox()?;
        let runtime = raw_runtime(&daemon.manager).await?;
        match kind {
            0 => sandbox.rootfs_runtime = Some(runtime),
            1 => sandbox.mem_ublk_device = Some(runtime.device),
            _ => sandbox.extra_drive_runtimes.push(runtime),
        }
        {
            let release = sandbox.release_ublk_devices(&daemon.manager);
            tokio::pin!(release);
            tokio::select! {
                result = &mut release => panic!("release completed prematurely: {result:?}"),
                _ = entered.notified() => {},
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut release)
                    .await
                    .is_err()
            );
        }
        assert_eq!(sandbox.rootfs_runtime.is_some(), kind == 0);
        assert_eq!(sandbox.mem_ublk_device.is_some(), kind == 1);
        assert_eq!(sandbox.extra_drive_runtimes.len(), usize::from(kind == 2));
        finish.notify_one();
        sandbox.release_ublk_devices(&daemon.manager).await?;
        assert!(sandbox.rootfs_runtime.is_none());
        assert!(sandbox.mem_ublk_device.is_none());
        assert!(sandbox.extra_drive_runtimes.is_empty());
        daemon.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn lost_release_replies_retain_only_remaining_cleanup_debt() -> Result<()> {
    let root = lease(1);
    let memory = lease(2);
    let first = lease(3);
    let last = lease(4);
    let daemon = TestDaemon::start(
        vec![
            raw_acquire(root.clone()),
            raw_acquire(memory.clone()),
            raw_acquire(first.clone()),
            raw_acquire(last.clone()),
            release(root.clone(), false),
            release(root, true),
            release(memory.clone(), false),
            release(memory, true),
            release(last, true),
            release(first.clone(), false),
            release(first, true),
        ],
        false,
    )?;
    let mut sandbox = sandbox()?;
    sandbox.rootfs_runtime = Some(raw_runtime(&daemon.manager).await?);
    sandbox.mem_ublk_device = Some(raw_runtime(&daemon.manager).await?.device);
    sandbox
        .extra_drive_runtimes
        .push(raw_runtime(&daemon.manager).await?);
    sandbox
        .extra_drive_runtimes
        .push(raw_runtime(&daemon.manager).await?);
    assert!(sandbox.release_ublk_devices(&daemon.manager).await.is_err());
    assert!(sandbox.rootfs_runtime.is_some());
    assert!(sandbox.mem_ublk_device.is_some());
    assert_eq!(sandbox.extra_drive_runtimes.len(), 2);
    assert!(sandbox.release_ublk_devices(&daemon.manager).await.is_err());
    assert!(sandbox.rootfs_runtime.is_none());
    assert!(sandbox.mem_ublk_device.is_some());
    assert_eq!(sandbox.extra_drive_runtimes.len(), 2);
    assert!(sandbox.release_ublk_devices(&daemon.manager).await.is_err());
    assert!(sandbox.mem_ublk_device.is_none());
    assert_eq!(sandbox.extra_drive_runtimes.len(), 1);
    sandbox.release_ublk_devices(&daemon.manager).await?;
    assert!(sandbox.extra_drive_runtimes.is_empty());
    daemon.finish().await?;
    Ok(())
}

#[tokio::test]
async fn unresolved_daemon_release_never_discards_the_native_receipt() -> Result<()> {
    for kind in 0..3 {
        let receipt = lease(7);
        let mut steps = vec![raw_acquire(receipt.clone())];
        for _ in 0..2 {
            let receipt = receipt.clone();
            steps.push(step(move |request| async move {
                let DaemonRequest::ReleaseOwned { lease } = request else {
                    panic!("owned release")
                };
                assert_eq!(lease, receipt);
                Some(DaemonResponse::Error {
                    message: "device release outcome is unresolved; reconciliation required".into(),
                })
            }));
        }
        let daemon = TestDaemon::start(steps, false)?;
        let mut sandbox = sandbox()?;
        let runtime = raw_runtime(&daemon.manager).await?;
        match kind {
            0 => sandbox.rootfs_runtime = Some(runtime),
            1 => sandbox.mem_ublk_device = Some(runtime.device),
            _ => sandbox.extra_drive_runtimes.push(runtime),
        }
        for _ in 0..2 {
            let error = sandbox
                .release_ublk_devices(&daemon.manager)
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("reconciliation required"));
            assert_eq!(sandbox.rootfs_runtime.is_some(), kind == 0);
            assert_eq!(sandbox.mem_ublk_device.is_some(), kind == 1);
            assert_eq!(sandbox.extra_drive_runtimes.len(), usize::from(kind == 2));
        }
        // Drop is not proof of release and must not asynchronously recycle a
        // memory device whose Firecracker user may still be alive.
        drop(sandbox);
        daemon.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn rootfs_link_failure_retains_receipt_and_rejects_another_start() -> Result<()> {
    let receipt = lease(1);
    let daemon = TestDaemon::start(
        vec![raw_acquire(receipt.clone()), release(receipt, true)],
        false,
    )?;
    let mut sandbox = sandbox()?;
    let attachment = sandbox.work_dir.path().join("user-rootfs");
    fs::write(&attachment, "obstruction")?;
    let runtime = raw_runtime(&daemon.manager).await?;
    assert!(sandbox.link_rootfs_runtime(runtime, &attachment).is_err());
    assert!(sandbox.rootfs_runtime.is_some());
    assert_eq!(fs::read_to_string(&attachment)?, "obstruction");
    let error = sandbox.start_nowait().await.unwrap_err();
    assert!(error.to_string().contains("existing device acquisitions"));
    sandbox.release_ublk_devices(&daemon.manager).await?;
    daemon.finish().await?;
    Ok(())
}

fn runtime_acquire(receipt: DeviceLease) -> Step {
    step(move |request| async move {
        let DaemonRequest::AcquireOwned { request } = request else {
            panic!("owned acquire")
        };
        let DaemonRequest::CreateOverlaybdRuntimeDevice { runtime_dir, .. } = *request else {
            panic!("runtime acquire")
        };
        Some(DaemonResponse::Owned {
            response: Box::new(DaemonResponse::OverlaybdRuntimeDeviceCreated {
                dev_id: receipt.dev_id,
                device_path: format!("/dev/ublkb{}", receipt.dev_id).into(),
                actual_virtual_size: 4096,
                runtime_image_config_path: runtime_dir.join("image.json"),
            }),
            lease: receipt,
        })
    })
}

fn drives() -> Result<Vec<ExtraDrive>> {
    Ok(vec![
        ExtraDrive::try_new_overlaybd("first", "/first.json", true)?,
        ExtraDrive::try_new_overlaybd("second", "/second.json", true)?,
    ])
}

#[tokio::test]
async fn extra_drive_link_failure_retains_all_acquired_devices() -> Result<()> {
    let first = lease(1);
    let second = lease(2);
    let daemon = TestDaemon::start(
        vec![
            runtime_acquire(first.clone()),
            runtime_acquire(second.clone()),
            release(second, true),
            release(first, true),
        ],
        false,
    )?;
    let mut sandbox = sandbox()?;
    let drives = drives()?;
    let obstruction = sandbox
        .work_dir
        .path()
        .join(drives[1].attachment_symlink_name());
    fs::write(&obstruction, "keep this file")?;
    let result = prepare_extra_drives(
        &drives,
        Path::new("/global.json"),
        sandbox.work_dir.path(),
        overlaybd::config::UpperMode::LogStructured,
        ExtraDrivePrepareMode::Resume,
        &mut sandbox.extra_drive_runtimes,
        &daemon.manager,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(sandbox.extra_drive_runtimes.len(), 2);
    assert_eq!(fs::read_to_string(&obstruction)?, "keep this file");
    sandbox.release_ublk_devices(&daemon.manager).await?;
    daemon.finish().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_extra_drive_preparation_retains_prior_acquisitions() -> Result<()> {
    let first = lease(1);
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let started = Arc::clone(&entered);
    let completed = Arc::clone(&finish);
    let daemon = TestDaemon::start(
        vec![
            runtime_acquire(first.clone()),
            step(move |request| async move {
                assert!(matches!(request, DaemonRequest::AcquireOwned { .. }));
                started.notify_one();
                completed.notified().await;
                None // No second acquisition receipt has reached this sandbox.
            }),
            release(first, true),
        ],
        false,
    )?;
    let mut sandbox = sandbox()?;
    let drives = drives()?;
    {
        let prepare = prepare_extra_drives(
            &drives,
            Path::new("/global.json"),
            sandbox.work_dir.path(),
            overlaybd::config::UpperMode::LogStructured,
            ExtraDrivePrepareMode::Resume,
            &mut sandbox.extra_drive_runtimes,
            &daemon.manager,
        );
        tokio::pin!(prepare);
        tokio::select! {
            result = &mut prepare => panic!("preparation completed prematurely: {result:?}"),
            _ = entered.notified() => {},
        }
    }
    assert_eq!(sandbox.extra_drive_runtimes.len(), 1);
    finish.notify_one();
    sandbox.release_ublk_devices(&daemon.manager).await?;
    daemon.finish().await?;
    Ok(())
}

#[tokio::test]
async fn memory_consumers_keep_independent_receipts_for_the_shared_device() -> Result<()> {
    let first = lease(5);
    let second = lease(5);
    let acquire = |receipt: DeviceLease| {
        step(move |request| async move {
            let DaemonRequest::AcquireOwned { request } = request else {
                panic!("owned acquire")
            };
            assert!(matches!(
                *request,
                DaemonRequest::AcquireOverlaybd {
                    access_mode: AccessMode::Shared,
                    ..
                }
            ));
            Some(DaemonResponse::Owned {
                response: Box::new(DaemonResponse::DeviceAcquired {
                    dev_id: 5,
                    device_path: "/dev/ublkb5".into(),
                }),
                lease: receipt,
            })
        })
    };
    let daemon = TestDaemon::start(
        vec![
            acquire(first.clone()),
            acquire(second.clone()),
            release(first.clone(), false),
            release(second, true),
            release(first, true),
        ],
        true,
    )?;
    let mut a = sandbox()?;
    let mut b = sandbox()?;
    a.mem_ublk_device = Some(
        daemon
            .manager
            .acquire_memory_device(&memory_spec(), 4096)
            .await?,
    );
    b.mem_ublk_device = Some(
        daemon
            .manager
            .acquire_memory_device(&memory_spec(), 4096)
            .await?,
    );
    assert_eq!(
        a.mem_ublk_device.as_ref().unwrap().device_path(),
        b.mem_ublk_device.as_ref().unwrap().device_path()
    );
    assert!(a.release_ublk_devices(&daemon.manager).await.is_err());
    b.release_ublk_devices(&daemon.manager).await?;
    assert!(a.mem_ublk_device.is_some());
    assert!(b.mem_ublk_device.is_none());
    a.release_ublk_devices(&daemon.manager).await?;
    daemon.finish().await?;
    Ok(())
}
