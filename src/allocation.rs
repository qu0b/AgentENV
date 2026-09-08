//! Durable ownership of keyed create requests, separate from paused VM records.
//!
//! A settled request cannot create another guest. It does not prove that the
//! assigned guest has been deleted. A different server incarnation must reconcile
//! host resources before treating absence from its in-memory inventory as deletion.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::types::SandboxId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AllocationState {
    Pending,
    Settled,
    Cancelled,
    Interrupted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AllocationReceipt {
    pub version: u32,
    pub owner_id: Uuid,
    pub allocation_id: Uuid,
    pub sandbox_id: Option<SandboxId>,
    pub request_digest: Option<String>,
    pub state: AllocationState,
    pub cancel_requested: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Record {
    receipt: AllocationReceipt,
    epoch: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JournalIdentity {
    version: u32,
    owner_id: Uuid,
}

#[derive(Debug)]
pub(crate) enum AllocationResult<T> {
    Completed(T),
    Existing(AllocationReceipt),
}

#[derive(Debug, thiserror::Error)]
#[error("allocation key was already used with a different request")]
pub(crate) struct AllocationConflict;

pub(crate) struct AllocationJournal {
    db: LocalKvStore,
    owner_id: Uuid,
    epoch: Uuid,
    gate: Mutex<()>,
}

impl AllocationJournal {
    pub async fn open(path: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let db = LocalKvStore::open(path, LocalStoreDurability::Sync).await?;
        let identity: JournalIdentity = match db.get(b"format".to_vec()).await? {
            Some(bytes) => {
                serde_json::from_slice(&bytes).context("decode allocation journal identity")?
            }
            None => {
                let identity = JournalIdentity {
                    version: 1,
                    owner_id: Uuid::now_v7(),
                };
                db.put(b"format".to_vec(), serde_json::to_vec(&identity)?)
                    .await?;
                identity
            }
        };
        ensure!(
            identity.version == 1 && !identity.owner_id.is_nil(),
            "unsupported allocation journal identity/version"
        );
        Ok(Arc::new(Self {
            db,
            owner_id: identity.owner_id,
            epoch: Uuid::now_v7(),
            gate: Mutex::new(()),
        }))
    }

    pub fn owner_id(&self) -> Uuid {
        self.owner_id
    }

    fn key(id: Uuid) -> Vec<u8> {
        format!("allocation/{id}").into_bytes()
    }

    async fn read(&self, id: Uuid) -> anyhow::Result<Option<Record>> {
        let Some(bytes) = self.db.get(Self::key(id)).await? else {
            return Ok(None);
        };
        let record: Record = serde_json::from_slice(&bytes).context("decode allocation record")?;
        let r = &record.receipt;
        ensure!(
            r.version == 1 && r.allocation_id == id && r.owner_id == self.owner_id,
            "invalid allocation identity/version"
        );
        match (r.sandbox_id, r.request_digest.as_deref(), r.state) {
            (None, None, AllocationState::Cancelled) if r.cancel_requested => {}
            (Some(_), Some(digest), state) if state != AllocationState::Cancelled => {
                ensure!(
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                    "invalid allocation digest"
                );
            }
            _ => bail!("invalid allocation state"),
        }
        Ok(Some(record))
    }

    async fn write(&self, record: &Record) -> anyhow::Result<()> {
        self.db
            .put(
                Self::key(record.receipt.allocation_id),
                serde_json::to_vec(record)?,
            )
            .await
    }

    fn receipt(&self, mut record: Record) -> AllocationReceipt {
        if record.epoch != self.epoch && record.receipt.sandbox_id.is_some() {
            record.receipt.state = AllocationState::Interrupted;
        }
        record.receipt
    }

    pub async fn get(&self, id: Uuid) -> anyhow::Result<Option<AllocationReceipt>> {
        Ok(self.read(id).await?.map(|record| self.receipt(record)))
    }

    /// Fence even a request which has not arrived yet. Once claimed, the owned
    /// operation is allowed to settle; the caller must wait before guest cleanup.
    pub async fn cancel(self: &Arc<Self>, id: Uuid) -> anyhow::Result<AllocationReceipt> {
        let this = Arc::clone(self);
        // Own the lock through the blocking database write, even if HTTP drops.
        tokio::spawn(async move {
            let _gate = this.gate.lock().await;
            let mut record = this.read(id).await?.unwrap_or(Record {
                receipt: AllocationReceipt {
                    version: 1,
                    owner_id: this.owner_id,
                    allocation_id: id,
                    sandbox_id: None,
                    request_digest: None,
                    state: AllocationState::Cancelled,
                    cancel_requested: true,
                },
                epoch: this.epoch,
            });
            record.receipt.cancel_requested = true;
            this.write(&record).await?;
            Ok(this.receipt(record))
        })
        .await
        .context("join allocation cancellation")?
    }

    /// Commit a single server-assigned ID before any create-handler side effect.
    /// Replays never execute the callback, including after failure or restart.
    pub async fn execute<T, F, Fut>(
        self: &Arc<Self>,
        id: Uuid,
        digest: String,
        operation: F,
    ) -> anyhow::Result<AllocationResult<T>>
    where
        T: Send + 'static,
        F: FnOnce(SandboxId) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid allocation request digest"
        );
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let sandbox_id = {
                let _gate = this.gate.lock().await;
                if let Some(record) = this.read(id).await? {
                    if record
                        .receipt
                        .request_digest
                        .as_ref()
                        .is_some_and(|old| old != &digest)
                    {
                        return Err(AllocationConflict.into());
                    }
                    return Ok(AllocationResult::Existing(this.receipt(record)));
                }
                let sandbox_id = SandboxId::new();
                this.write(&Record {
                    receipt: AllocationReceipt {
                        version: 1,
                        owner_id: this.owner_id,
                        allocation_id: id,
                        sandbox_id: Some(sandbox_id),
                        request_digest: Some(digest),
                        state: AllocationState::Pending,
                        cancel_requested: false,
                    },
                    epoch: this.epoch,
                })
                .await?;
                sandbox_id
            };
            // A callback panic is an interrupted operation, never a settled one.
            let result = tokio::spawn(async move { operation(sandbox_id).await }).await;
            let _gate = this.gate.lock().await;
            let mut record = this
                .read(id)
                .await?
                .context("allocation record disappeared")?;
            record.receipt.state = if result.is_ok() {
                AllocationState::Settled
            } else {
                AllocationState::Interrupted
            };
            this.write(&record).await?;
            Ok(AllocationResult::Completed(
                result.context("allocation operation interrupted")?,
            ))
        })
        .await
        .context("join allocation operation")?
    }
}

/// Hash the complete validated request without retaining its environment values
/// or extension secrets. Object order is irrelevant; array order remains relevant.
pub(crate) fn request_digest(value: &impl Serialize) -> anyhow::Result<String> {
    fn canonicalize(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(object) => {
                let sorted: std::collections::BTreeMap<_, _> = object.into_iter().collect();
                serde_json::Value::Object(
                    sorted
                        .into_iter()
                        .map(|(key, value)| (key, canonicalize(value)))
                        .collect(),
                )
            }
            serde_json::Value::Array(array) => {
                serde_json::Value::Array(array.into_iter().map(canonicalize).collect())
            }
            scalar => scalar,
        }
    }
    Ok(crate::digest::sha256_hex(&serde_json::to_vec(
        &canonicalize(serde_json::to_value(value)?),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    fn digest() -> String {
        "a".repeat(64)
    }

    #[tokio::test]
    async fn cancellation_before_create_fences_delayed_requests_across_restart() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::now_v7();
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        assert!(journal.get(id).await.unwrap().is_none());
        let receipt = journal.cancel(id).await.unwrap();
        let owner = journal.owner_id();
        assert_eq!(receipt.state, AllocationState::Cancelled);
        assert!(receipt.sandbox_id.is_none());
        drop(journal);
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        assert_eq!(journal.owner_id(), owner);
        let result = journal
            .execute(id, digest(), |_| async { panic!("late request allocated") })
            .await
            .unwrap();
        assert!(matches!(
            result,
            AllocationResult::Existing(AllocationReceipt {
                state: AllocationState::Cancelled,
                sandbox_id: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn lost_caller_and_cancel_wait_for_original_operation_to_settle() {
        let temp = tempfile::tempdir().unwrap();
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        let id = Uuid::now_v7();
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let worker = Arc::clone(&journal);
        let caller = tokio::spawn(async move {
            let proof = Arc::clone(&worker);
            worker
                .execute(id, digest(), move |sandbox_id| async move {
                    let persisted = proof.get(id).await.unwrap().unwrap();
                    assert_eq!(persisted.sandbox_id, Some(sandbox_id));
                    assert_eq!(persisted.state, AllocationState::Pending);
                    started_tx.send(sandbox_id).unwrap();
                    finish_rx.await.unwrap();
                })
                .await
        });
        let sandbox_id = started_rx.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let stopped = journal.cancel(id).await.unwrap();
        assert_eq!(stopped.sandbox_id, Some(sandbox_id));
        assert_eq!(stopped.state, AllocationState::Pending);
        assert!(stopped.cancel_requested);
        let repeat = journal
            .execute(id, digest(), |_| async { panic!("duplicate create") })
            .await
            .unwrap();
        assert!(matches!(repeat, AllocationResult::Existing(_)));
        let changed = journal
            .execute(id, "b".repeat(64), |_| async { panic!("changed create") })
            .await;
        assert!(changed.err().unwrap().is::<AllocationConflict>());
        // A slow operation does not hold the journal mutation gate.
        let other = journal
            .execute(Uuid::now_v7(), digest(), |_| async { 7 })
            .await
            .unwrap();
        assert!(matches!(other, AllocationResult::Completed(7)));
        finish_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let receipt = journal.get(id).await.unwrap().unwrap();
                assert_eq!(receipt.sandbox_id, Some(sandbox_id));
                assert!(receipt.cancel_requested);
                if receipt.state == AllocationState::Settled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_duplicates_execute_once_and_failure_does_not_reset_identity() {
        let temp = tempfile::tempdir().unwrap();
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        let id = Uuid::now_v7();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let journal = Arc::clone(&journal);
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                journal
                    .execute(id, digest(), |_| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>("controlled create failure")
                    })
                    .await
                    .unwrap()
            }));
        }
        let mut completed = 0;
        for task in tasks {
            if let AllocationResult::Completed(result) = task.await.unwrap() {
                assert_eq!(result, Err("controlled create failure"));
                completed += 1;
            }
        }
        assert_eq!(completed, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let original = journal.get(id).await.unwrap().unwrap();
        assert_eq!(original.state, AllocationState::Settled);
        drop(journal);
        let reopened = AllocationJournal::open(temp.path()).await.unwrap();
        let receipt = reopened.get(id).await.unwrap().unwrap();
        assert_eq!(receipt.sandbox_id, original.sandbox_id);
        assert_eq!(receipt.state, AllocationState::Interrupted);
        assert!(matches!(
            reopened
                .execute(id, digest(), |_| async { panic!("restart retried create") })
                .await
                .unwrap(),
            AllocationResult::Existing(_)
        ));
    }

    #[tokio::test]
    async fn callback_panic_is_interrupted_and_cannot_be_retried() {
        let temp = tempfile::tempdir().unwrap();
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        let id = Uuid::now_v7();
        assert!(journal
            .execute(id, digest(), |_| async { panic!("controlled panic") })
            .await
            .is_err());
        let receipt = journal.get(id).await.unwrap().unwrap();
        assert_eq!(receipt.state, AllocationState::Interrupted);
        assert!(receipt.sandbox_id.is_some());
        assert!(matches!(
            journal
                .execute(id, digest(), |_| async { panic!("panic retried") })
                .await
                .unwrap(),
            AllocationResult::Existing(_)
        ));
    }

    #[tokio::test]
    async fn malformed_record_or_future_version_refuses_create() {
        let temp = tempfile::tempdir().unwrap();
        let journal = AllocationJournal::open(temp.path()).await.unwrap();
        let id = Uuid::now_v7();
        journal
            .db
            .put(AllocationJournal::key(id), b"{}".to_vec())
            .await
            .unwrap();
        assert!(journal
            .execute(id, digest(), |_| async { panic!("corruption allocated") })
            .await
            .is_err());
        assert!(journal.cancel(id).await.is_err());
        journal
            .db
            .put(b"format".to_vec(), b"2".to_vec())
            .await
            .unwrap();
        drop(journal);
        assert!(AllocationJournal::open(temp.path()).await.is_err());
    }

    #[test]
    fn request_hash_includes_nested_values_and_ignores_object_order() {
        let first: serde_json::Value = serde_json::from_str(
            r#"{"envVars":{"A":"secret","B":"second"},"templateID":"base","secure":true}"#,
        )
        .unwrap();
        let reordered: serde_json::Value = serde_json::from_str(
            r#"{"secure":true,"templateID":"base","envVars":{"B":"second","A":"secret"}}"#,
        )
        .unwrap();
        assert_eq!(
            request_digest(&first).unwrap(),
            request_digest(&reordered).unwrap()
        );
        let mut changed = first.clone();
        changed["envVars"]["A"] = "changed".into();
        assert_ne!(
            request_digest(&first).unwrap(),
            request_digest(&changed).unwrap()
        );
    }

    #[test]
    fn allocation_process_worker() {
        let Ok(path) = std::env::var("EU919_ALLOCATION_CHILD_PATH") else {
            return;
        };
        let id = Uuid::parse_str(&std::env::var("EU919_ALLOCATION_CHILD_ID").unwrap()).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let journal = AllocationJournal::open(PathBuf::from(&path).join("db"))
                .await
                .unwrap();
            journal
                .execute(id, digest(), |sandbox_id| async move {
                    std::fs::write(PathBuf::from(path).join("started"), sandbox_id.to_string())
                        .unwrap();
                    std::future::pending::<()>().await;
                })
                .await
                .unwrap();
        });
    }

    #[tokio::test]
    async fn sigkill_preserves_original_id_and_never_reallocates() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::now_v7();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "allocation::tests::allocation_process_worker",
                "--nocapture",
            ])
            .env("EU919_ALLOCATION_CHILD_PATH", temp.path())
            .env("EU919_ALLOCATION_CHILD_ID", id.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let boundary = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "worker exited before allocation"
                );
                if let Ok(value) = std::fs::read_to_string(temp.path().join("started")) {
                    if let Ok(id) = SandboxId::parse_str(&value) {
                        break id;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        child.kill().unwrap();
        let status = child.wait().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        let sandbox_id = boundary.expect("worker reached durable allocation boundary");
        let journal = AllocationJournal::open(temp.path().join("db"))
            .await
            .unwrap();
        let receipt = journal.get(id).await.unwrap().unwrap();
        assert_eq!(receipt.sandbox_id, Some(sandbox_id));
        assert_eq!(receipt.state, AllocationState::Interrupted);
        assert_eq!(receipt.request_digest.as_deref(), Some(digest().as_str()));
        assert!(matches!(
            journal
                .execute(id, digest(), |_| async { panic!("SIGKILL retried create") })
                .await
                .unwrap(),
            AllocationResult::Existing(_)
        ));
        let cancelled = journal.cancel(id).await.unwrap();
        assert!(cancelled.cancel_requested);
        assert_eq!(cancelled.state, AllocationState::Interrupted);
        assert_eq!(cancelled.sandbox_id, Some(sandbox_id));
    }
}
