//! Kernel operations are substituted; namespace paths and filesystem failures
//! are real. These tests never create or delete host interfaces/mounts.
use super::*;
use nix::errno::Errno;
use std::sync::atomic::AtomicUsize;

fn manager(capacity: usize, netns_dir: PathBuf) -> NetworkManager {
    let manager = NetworkManager {
        allocated: AtomicBitSet::new(),
        pool: WarmPool::new(PoolConfig {
            low_watermark: 0,
            high_watermark: capacity,
            maintenance_enabled: false,
            startup_prewarm: false,
        }),
        cleanup_pending: Mutex::new(Vec::new()),
        reservations: Mutex::new(HashMap::new()),
        address_plan: NetworkAddressPlan::default(),
        netns_dir,
        egress_proxy: EgressProxy::new(),
        shutting_down: AtomicBool::new(false),
    };
    manager.allocated.insert(0);
    manager
}

fn cleanup(slot: &mut Slot, _: bool) -> Result<(), NetworkError> {
    slot.cleanup_with(|_| Ok(()), |_| Err(Errno::EINVAL))
}

#[test]
fn failed_cleanup_retains_reservation_and_cannot_reenter_pool() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let manager = manager(1, dir.path().into());
    let mut owned = manager.allocate_slot(1)?;
    owned.arm_test_cleanup();
    let identity = owned.namespace_id.clone();
    let path = owned.namespace_path();
    fs::create_dir(&path)?; // remove_file must fail after the simulated unmount.
    let pooled = manager.allocate_slot(2)?;
    manager.pool.try_push_bounded(pooled).unwrap();
    let mut retained = Some(owned);
    assert!(manager
        .release_retained_with(&mut retained, true, false, cleanup)
        .is_err());
    assert_eq!(retained.as_ref().unwrap().namespace_id, identity);
    assert!(manager.allocated.has(1));
    assert!(!retained.as_ref().unwrap().can_reuse());
    assert!(manager.allocate_slot(1).is_err());

    let pooled = manager.pool.try_acquire().unwrap();
    fs::remove_dir(&path)?;
    fs::write(&path, "namespace fixture")?;
    // There is now pool capacity, but a partially torn-down slot must finish
    // cleanup rather than become a new tenant's warm namespace.
    manager.release_retained_with(&mut retained, true, false, cleanup)?;
    assert!(retained.is_none());
    assert!(manager.pool.is_empty());
    assert!(!path.exists());
    assert!(!manager.allocated.has(1));
    manager.cleanup_allocated_slot(pooled, false)?;
    Ok(())
}

#[test]
fn background_cleanup_retains_original_slots_until_a_confirmed_retry() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let manager = Arc::new(manager(0, dir.path().into()));
    let mut paths = Vec::new();
    for idx in 1..=2 {
        let mut slot = manager.allocate_slot(idx)?;
        slot.arm_test_cleanup();
        let path = slot.namespace_path();
        fs::create_dir(&path)?;
        paths.push(path);
        assert!(manager.cleanup_owned_with(slot, false, cleanup).is_err());
    }
    assert_eq!(manager.cleanup_pending.lock().unwrap().len(), 2);
    assert!(manager.retry_pending_cleanup_with(false, cleanup).is_err());
    assert_eq!(manager.cleanup_pending.lock().unwrap().len(), 2);
    for path in paths {
        fs::remove_dir(&path)?;
        fs::write(path, "namespace")?;
    }
    let effects = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let effects = Arc::clone(&effects);
            std::thread::spawn(move || {
                manager.retry_pending_cleanup_with(false, |slot, force| {
                    effects.fetch_add(1, Ordering::SeqCst);
                    cleanup(slot, force)
                })
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap()?;
    }
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    assert!(manager.cleanup_pending.lock().unwrap().is_empty());
    assert!(manager.reservations.lock().unwrap().is_empty());
    assert!(!manager.allocated.has(1));
    assert!(!manager.allocated.has(2));
    Ok(())
}

#[test]
fn unresolved_cleanup_does_not_stall_unrelated_pool_drain() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut manager = manager(1, dir.path().into());
    manager.pool = WarmPool::new(PoolConfig {
        low_watermark: 0,
        high_watermark: 1,
        maintenance_enabled: true,
        startup_prewarm: false,
    });
    // Unknown ownership stays unresolved and must not authorize any cleanup.
    let unknown = Slot::new(
        1,
        manager.address_plan,
        dir.path().into(),
        Arc::clone(&manager.egress_proxy),
    )?;
    assert!(manager.cleanup_allocated_slot(unknown, false).is_err());
    for idx in 2..=3 {
        manager.pool.release(manager.allocate_slot(idx)?).unwrap();
    }
    assert!(manager.run_pool_maintenance_cycle().is_err());
    assert_eq!(manager.cleanup_pending.lock().unwrap().len(), 1);
    assert_eq!(
        manager.pool.len(),
        1,
        "independent drain must still progress"
    );
    manager.cleanup_pending.lock().unwrap().clear(); // logical-only fixture
    manager.shutdown()?;
    Ok(())
}

#[test]
fn stale_namespace_identity_cannot_release_a_reused_index() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let manager = manager(0, dir.path().into());
    let original = manager.allocate_slot(1)?;
    let identity = original.namespace_id.clone();
    manager.cleanup_allocated_slot(original, false)?;
    let replacement = manager.allocate_slot(1)?;
    assert_ne!(replacement.namespace_id, identity);
    let mut stale = Slot::new(
        1,
        manager.address_plan,
        dir.path().into(),
        Arc::clone(&manager.egress_proxy),
    )?;
    stale.namespace_id = identity;
    let mut stale = Some(stale);
    let error = manager
        .release_retained_with(&mut stale, true, false, |_, _| {
            panic!("stale owner touched kernel")
        })
        .unwrap_err();
    assert!(error.to_string().contains("allocation owner"));
    assert!(stale.is_some());
    assert!(manager.allocated.has(1));
    manager.validate_slot_owner(&replacement)?;
    manager.cleanup_allocated_slot(replacement, false)?;
    Ok(())
}

#[test]
fn failed_metadata_lookup_cannot_be_treated_as_namespace_absence() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let parent = dir.path().join("not-a-directory");
    fs::write(&parent, "obstruction")?;
    let manager = manager(0, parent.clone());
    let mut slot = manager.allocate_slot(1)?;
    slot.arm_test_cleanup();
    let mut retained = Some(slot);
    assert!(manager
        .release_retained_with(&mut retained, false, false, cleanup)
        .is_err());
    assert!(manager.allocated.has(1));
    assert!(retained.is_some());
    fs::remove_file(&parent)?;
    fs::create_dir(&parent)?;
    manager.release_retained_with(&mut retained, false, false, cleanup)?;
    assert!(retained.is_none());
    Ok(())
}

#[test]
fn kernel_failure_or_panic_keeps_cleanup_armed_for_the_original_owner() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let manager = manager(0, dir.path().into());
    let mut slot = manager.allocate_slot(1)?;
    slot.arm_test_cleanup();
    let mut retained = Some(slot);
    let error = manager
        .release_retained_with(&mut retained, false, false, |slot, _| {
            slot.cleanup_with(
                |_| Err(anyhow!("veth deletion failed")),
                |_| panic!("unmount after failed delete"),
            )
        })
        .unwrap_err();
    assert!(error.to_string().contains("veth deletion failed"));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        manager.release_retained_with(&mut retained, false, false, |slot, _| {
            slot.cleanup_with(|_| panic!("kernel operation panicked"), |_| Ok(()))
        })
    }));
    assert!(panic.is_err());
    assert!(retained.is_some());
    let effects = AtomicUsize::new(0);
    manager.release_retained_with(&mut retained, false, false, |slot, _| {
        slot.cleanup_with(
            |_| {
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            |_| Err(Errno::EINVAL),
        )
    })?;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "retry must execute unconfirmed cleanup"
    );
    assert!(!manager.allocated.has(1));
    Ok(())
}
