//! Exercise the production ownership gate independently of the kernel executor.
//! Unix-stream tests use the real client and framing, including a lost reply.
use super::*;
use crate::{DeviceLease, UblkDaemonClient};
use std::sync::atomic::{AtomicUsize, Ordering};

fn acquire_request() -> DaemonRequest {
    DaemonRequest::AcquireOwned {
        request: Box::new(DaemonRequest::CreateOverlaybd {
            image_config: "/fixture/image.json".into(),
            global_config: "/fixture/global.json".into(),
        }),
    }
}

async fn acquire(leases: &DeviceLeases, dev_id: u32) -> Result<DeviceLease> {
    let response = dispatch_device_request(leases, acquire_request(), false, |_| async {
        Ok(DaemonResponse::DeviceCreated {
            dev_id,
            device_path: format!("/dev/ublkb{dev_id}").into(),
        })
    })
    .await?;
    let DaemonResponse::Owned { lease, .. } = response else {
        bail!("missing ownership")
    };
    Ok(lease)
}

#[tokio::test]
async fn stale_release_over_unix_socket_cannot_release_reused_device() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("owned.sock");
    let listener = UnixListener::bind(&path)?;
    let effects = Arc::new(AtomicUsize::new(0));
    let server_effects = Arc::clone(&effects);
    let task = tokio::spawn(async move {
        let leases = DeviceLeases::default();
        // Acquire A, release A (reply lost), acquire B at the same number,
        // replay A's release, mutate B, release B.
        for index in 0..6 {
            let (mut stream, _) = listener.accept().await?;
            let request = recv_message::<DaemonRequest>(&mut stream).await?.unwrap();
            let response = dispatch_device_request(&leases, request, false, |request| async {
                match request {
                    DaemonRequest::CreateOverlaybd { .. } => Ok(DaemonResponse::DeviceCreated {
                        dev_id: 7,
                        device_path: "/dev/ublkb7".into(),
                    }),
                    DaemonRequest::Delete { dev_id: 7 } => {
                        server_effects.fetch_add(1, Ordering::SeqCst);
                        Ok(DaemonResponse::Deleted)
                    }
                    DaemonRequest::UpdateSize {
                        dev_id: 7,
                        new_sectors: 2048,
                    } => Ok(DaemonResponse::SizeUpdated),
                    request => bail!("unexpected executed request: {request:?}"),
                }
            })
            .await?;
            if index != 1 {
                send_message(&mut stream, &response).await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let client = UblkDaemonClient::new_for_test(path, false);
    let (_, _, original) = client
        .create_overlaybd(Path::new("/a"), Path::new("/g"))
        .await?;
    assert!(
        client.delete(&original).await.is_err(),
        "server drops the completed release reply"
    );
    let (_, _, replacement) = client
        .create_overlaybd(Path::new("/b"), Path::new("/g"))
        .await?;
    assert_ne!(original.lease_id, replacement.lease_id);
    client.delete(&original).await?;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    client.update_size(&replacement, 2048).await?;
    client.delete(&replacement).await?;
    task.await??;
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn rejects_unowned_nested_foreign_and_cross_device_operations_before_execution() -> Result<()>
{
    let leases = DeviceLeases::default();
    let lease = acquire(&leases, 5).await?;
    let foreign = acquire(&DeviceLeases::default(), 5).await?;
    let requests = [
        DaemonRequest::Delete { dev_id: 5 },
        DaemonRequest::ReleaseOverlaybd { dev_id: 5 },
        DaemonRequest::RestackSnapshot {
            dev_id: 5,
            output_layer_path: "/out".into(),
        },
        DaemonRequest::UpdateSize {
            dev_id: 5,
            new_sectors: 1,
        },
        DaemonRequest::CreateOverlaybd {
            image_config: "/img".into(),
            global_config: "/global".into(),
        },
        DaemonRequest::AcquireOwned {
            request: Box::new(DaemonRequest::Delete { dev_id: 5 }),
        },
        DaemonRequest::AcquireOwned {
            request: Box::new(acquire_request()),
        },
        DaemonRequest::UseOwned {
            lease: lease.clone(),
            request: Box::new(DaemonRequest::UpdateSize {
                dev_id: 6,
                new_sectors: 1,
            }),
        },
        DaemonRequest::UseOwned {
            lease: lease.clone(),
            request: Box::new(DaemonRequest::Delete { dev_id: 5 }),
        },
        DaemonRequest::ReleaseOwned { lease: foreign },
        DaemonRequest::ReleaseOwned {
            lease: DeviceLease { dev_id: 6, ..lease },
        },
    ];
    for request in requests {
        assert!(dispatch_device_request(&leases, request, true, |_| async {
            panic!("unauthorized numeric effect")
        })
        .await
        .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn shared_acquisitions_each_decrement_the_pool_once() -> Result<()> {
    let leases = DeviceLeases::default();
    let mut acquisitions = Vec::new();
    for _ in 0..2 {
        let response = dispatch_device_request(
            &leases,
            DaemonRequest::AcquireOwned {
                request: Box::new(DaemonRequest::AcquireOverlaybd {
                    image_config: "/img".into(),
                    global_config: "/global".into(),
                    virtual_size: 4096,
                    access_mode: AccessMode::Shared,
                }),
            },
            true,
            |_| async {
                Ok(DaemonResponse::DeviceAcquired {
                    dev_id: 5,
                    device_path: "/dev/ublkb5".into(),
                })
            },
        )
        .await?;
        let DaemonResponse::Owned { lease, .. } = response else {
            bail!("missing lease")
        };
        acquisitions.push(lease);
    }
    assert_ne!(acquisitions[0], acquisitions[1]);
    let remaining = AtomicUsize::new(2);
    let release = |lease| {
        let remaining = &remaining;
        dispatch_device_request(
            &leases,
            DaemonRequest::ReleaseOwned { lease },
            true,
            move |request| async move {
                assert!(matches!(
                    request,
                    DaemonRequest::ReleaseOverlaybd { dev_id: 5 }
                ));
                assert!(remaining.fetch_sub(1, Ordering::SeqCst) > 0);
                tokio::task::yield_now().await;
                Ok(DaemonResponse::Released)
            },
        )
    };
    let (a, b) = tokio::join!(
        release(acquisitions[0].clone()),
        release(acquisitions[0].clone())
    );
    a?;
    b?;
    assert_eq!(remaining.load(Ordering::SeqCst), 1);
    release(acquisitions[1].clone()).await?;
    assert_eq!(remaining.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn failed_release_is_not_acknowledged_or_reexecuted() -> Result<()> {
    let leases = DeviceLeases::default();
    let lease = acquire(&leases, 3).await?;
    let request = DaemonRequest::ReleaseOwned {
        lease: lease.clone(),
    };
    let error = dispatch_device_request(&leases, request.clone(), false, |_| async {
        bail!("kernel deletion failed")
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("kernel deletion failed"));
    let error = dispatch_device_request(&leases, request, false, |_| async {
        panic!("unresolved deletion repeated")
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unresolved"));
    assert!(dispatch_device_request(
        &leases,
        DaemonRequest::UseOwned {
            lease,
            request: Box::new(DaemonRequest::UpdateSize {
                dev_id: 3,
                new_sectors: 10
            }),
        },
        false,
        |_| async { panic!("mutated unresolved device") }
    )
    .await
    .is_err());
    Ok(())
}

#[tokio::test]
async fn release_waits_for_running_snapshot_and_blocks_future_mutations() -> Result<()> {
    let leases = DeviceLeases::default();
    let lease = acquire(&leases, 3).await?;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let use_request = DaemonRequest::UseOwned {
        lease: lease.clone(),
        request: Box::new(DaemonRequest::RestackSnapshot {
            dev_id: 3,
            output_layer_path: "/out".into(),
        }),
    };
    let mutation = dispatch_device_request(&leases, use_request.clone(), false, |_| async {
        entered_tx.send(()).unwrap();
        finish_rx.await?;
        Ok(DaemonResponse::RestackSnapshotCreated {
            descriptor: None,
            data_stat: None,
            ext4_used_bytes: None,
        })
    });
    let release = async {
        entered_rx.await?;
        let future = dispatch_device_request(
            &leases,
            DaemonRequest::ReleaseOwned { lease },
            false,
            |_| async { Ok(DaemonResponse::Deleted) },
        );
        tokio::pin!(future);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut future)
            .await
            .is_err());
        finish_tx.send(()).unwrap();
        future.await
    };
    let (mutation_result, release_result) = tokio::join!(mutation, release);
    mutation_result?;
    release_result?;
    assert!(
        dispatch_device_request(&leases, use_request, false, |_| async {
            panic!("released device mutation")
        })
        .await
        .is_err()
    );
    Ok(())
}

#[test]
fn failed_cache_invalidation_is_reported() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("not-a-block-device");
    assert!(clear_page_cache(&path).is_err(), "missing device must fail");
    std::fs::write(&path, "fixture")?;
    assert!(clear_page_cache(&path).is_err(), "failed ioctl must fail");
    Ok(())
}
