//! Scripted transport peer for native receipt ownership tests. No kernel device
//! is created; requests still traverse the production manager/client/framing.
use std::{future::Future, path::Path, pin::Pin};

use anyhow::Result;
use tokio::{net::UnixListener, task::JoinHandle};
use uvm_ublk_daemon::{
    protocol::{recv_message, send_message, DaemonRequest, DaemonResponse},
    DeviceLease, UblkDaemonClient,
};

use super::{OverlaybdRuntimeHandle, UblkCreateSpec, UblkDeviceManager};

pub type Step = Box<
    dyn FnOnce(DaemonRequest) -> Pin<Box<dyn Future<Output = Option<DaemonResponse>> + Send>>
        + Send,
>;

pub fn step<F, Fut>(run: F) -> Step
where
    F: FnOnce(DaemonRequest) -> Fut + Send + 'static,
    Fut: Future<Output = Option<DaemonResponse>> + Send + 'static,
{
    Box::new(|request| Box::pin(run(request)))
}

pub struct TestDaemon {
    pub manager: UblkDeviceManager,
    task: Option<JoinHandle<Result<()>>>,
    _dir: tempfile::TempDir,
}

impl TestDaemon {
    pub fn start(steps: Vec<Step>, pool_enabled: bool) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("daemon.sock");
        let listener = UnixListener::bind(&path)?;
        let task = tokio::spawn(async move {
            for run in steps {
                let (mut stream, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await??;
                let request = recv_message(&mut stream).await?.expect("request");
                if let Some(response) = run(request).await {
                    send_message(&mut stream, &response).await?;
                }
            }
            Ok(())
        });
        Ok(Self {
            manager: UblkDeviceManager::with_test_client(
                UblkDaemonClient::new_for_test(path, false),
                pool_enabled,
            ),
            task: Some(task),
            _dir: dir,
        })
    }

    pub async fn finish(mut self) -> Result<()> {
        self.task.take().unwrap().await??;
        Ok(())
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub fn lease(dev_id: u32) -> DeviceLease {
    DeviceLease {
        dev_id,
        lease_id: uuid::Uuid::now_v7().to_string(),
    }
}

pub fn raw_acquire(receipt: DeviceLease) -> Step {
    step(move |request| async move {
        let DaemonRequest::AcquireOwned { request } = request else {
            panic!("unowned acquire")
        };
        assert!(matches!(*request, DaemonRequest::CreateOverlaybd { .. }));
        Some(DaemonResponse::Owned {
            response: Box::new(DaemonResponse::DeviceCreated {
                dev_id: receipt.dev_id,
                device_path: format!("/dev/ublkb{}", receipt.dev_id).into(),
            }),
            lease: receipt,
        })
    })
}

pub fn release(receipt: DeviceLease, success: bool) -> Step {
    step(move |request| async move {
        let DaemonRequest::ReleaseOwned { lease } = request else {
            panic!("unowned release")
        };
        assert_eq!(lease, receipt);
        // A lost successful reply may later replay successfully. A daemon
        // error with unresolved effects is tested separately and stays an error.
        success.then_some(DaemonResponse::Released)
    })
}

pub fn memory_spec() -> UblkCreateSpec {
    UblkCreateSpec::Overlaybd {
        image_config: "/fixture/memory.json".into(),
        global_config: "/fixture/global.json".into(),
    }
}

pub async fn raw_runtime(manager: &UblkDeviceManager) -> Result<OverlaybdRuntimeHandle> {
    let device = manager.acquire_memory_device(&memory_spec(), 4096).await?;
    Ok(OverlaybdRuntimeHandle {
        device,
        image_config_path: Path::new("/fixture/runtime.json").into(),
        actual_virtual_size: 4096,
    })
}
