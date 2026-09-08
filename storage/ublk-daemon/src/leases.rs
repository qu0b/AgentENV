use std::{future::Future, sync::Arc};

use anyhow::{bail, Result};
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::protocol::DeviceLease;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ReleaseKind {
    Raw,
    Pooled,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Active,
    Releasing,
    Released,
}

struct Record {
    dev_id: u32,
    kind: ReleaseKind,
    phase: Phase,
}

/// Receipts belong to this daemon lifetime. Unknown IDs, including receipts
/// from another daemon, never authorize numeric-device operations. Terminal
/// receipts are retained; absence cannot be used as proof of completion.
#[derive(Default)]
pub(crate) struct DeviceLeases {
    records: DashMap<String, Arc<Mutex<Record>>>,
}

impl DeviceLeases {
    pub(crate) fn register(&self, dev_id: u32, kind: ReleaseKind) -> DeviceLease {
        loop {
            let lease_id = uuid::Uuid::now_v7().to_string();
            if let dashmap::mapref::entry::Entry::Vacant(entry) =
                self.records.entry(lease_id.clone())
            {
                entry.insert(Arc::new(Mutex::new(Record {
                    dev_id,
                    kind,
                    phase: Phase::Active,
                })));
                return DeviceLease { dev_id, lease_id };
            }
        }
    }

    async fn lock(&self, lease: &DeviceLease) -> Result<tokio::sync::OwnedMutexGuard<Record>> {
        let record = self
            .records
            .get(&lease.lease_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                anyhow::anyhow!("unknown device acquisition; reconciliation required")
            })?;
        let guard = record.lock_owned().await;
        if guard.dev_id != lease.dev_id {
            bail!("device acquisition ID does not match device number");
        }
        Ok(guard)
    }

    pub(crate) async fn use_active<T, F, Fut>(&self, lease: &DeviceLease, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let guard = self.lock(lease).await?;
        if guard.phase != Phase::Active {
            bail!("device acquisition is releasing or released");
        }
        // Retain this guard through the operation so release cannot invalidate
        // the device while a resize or snapshot mutation is using it.
        let result = operation().await;
        drop(guard);
        result
    }

    pub(crate) async fn release<F, Fut>(&self, lease: &DeviceLease, operation: F) -> Result<()>
    where
        F: FnOnce(ReleaseKind) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut guard = self.lock(lease).await?;
        match guard.phase {
            Phase::Released => return Ok(()),
            Phase::Releasing => {
                bail!("device release outcome is unresolved; reconciliation required")
            }
            Phase::Active => {}
        }
        // Cancellation or an error after this point retains uncertain debt.
        // Never invoke a numeric-ID release again merely because its reply
        // was lost: the device may already have returned to the pool.
        guard.phase = Phase::Releasing;
        operation(guard.kind).await?;
        guard.phase = Phase::Released;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn completed_release_replay_cannot_touch_a_reused_device_number() -> Result<()> {
        let leases = DeviceLeases::default();
        let original = leases.register(7, ReleaseKind::Pooled);
        let effects = AtomicUsize::new(0);
        leases
            .release(&original, |_| async {
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await?;
        let replacement = leases.register(7, ReleaseKind::Pooled);
        leases
            .release(&original, |_| async {
                panic!("old release touched replacement")
            })
            .await?;
        leases.use_active(&replacement, || async { Ok(()) }).await?;
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(leases
            .use_active(&original, || async { Ok(()) })
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn shared_acquisitions_release_once_each_even_with_duplicate_requests() -> Result<()> {
        let leases = DeviceLeases::default();
        let first = leases.register(9, ReleaseKind::Pooled);
        let second = leases.register(9, ReleaseKind::Pooled);
        let effects = AtomicUsize::new(0);
        let release = |lease| {
            leases.release(lease, |_| async {
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        let (a, b) = tokio::join!(release(&first), release(&first));
        a?;
        b?;
        leases.use_active(&second, || async { Ok(()) }).await?;
        release(&second).await?;
        assert_eq!(effects.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_release_keeps_unknown_outcome_and_rejects_foreign_identity() -> Result<()> {
        let leases = DeviceLeases::default();
        let lease = leases.register(3, ReleaseKind::Raw);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            leases.release(&lease, |_| async {
                std::future::pending::<Result<()>>().await
            })
        )
        .await
        .is_err());
        assert!(leases
            .release(&lease, |_| async { panic!("repeated uncertain effect") })
            .await
            .is_err());
        let mut wrong = lease.clone();
        wrong.dev_id = 4;
        assert!(leases
            .release(&wrong, |_| async { panic!("wrong device") })
            .await
            .is_err());
        assert!(DeviceLeases::default()
            .release(&lease, |_| async { panic!("foreign daemon") })
            .await
            .is_err());
        Ok(())
    }
}
