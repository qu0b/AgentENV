use super::*;
use std::sync::{mpsc, Arc};

struct Worker {
    pool: Arc<WarmPool<u32>>,
}

impl Worker {
    fn start<F>(low: usize, timing: MaintenanceTiming, cycle: F) -> Self
    where
        F: Fn() -> MaintenanceOutcome + Send + 'static,
    {
        let pool = Arc::new(WarmPool::new(PoolConfig {
            low_watermark: low,
            high_watermark: 1,
            maintenance_enabled: true,
            startup_prewarm: false,
        }));
        let worker_pool = Arc::clone(&pool);
        let handle = std::thread::spawn(move || worker_pool.maintenance_worker_loop(cycle, timing));
        *pool.maintenance_worker.lock().unwrap() = Some(handle);
        pool.maintenance_started.store(true, Ordering::Release);
        pool.request_maintenance();
        Self { pool }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.pool.drain_all();
    }
}

fn timing() -> MaintenanceTiming {
    MaintenanceTiming {
        retry_initial: Duration::from_millis(20),
        retry_max: Duration::from_millis(80),
        idle_poll: Duration::from_millis(40),
    }
}

#[test]
fn failed_cycles_back_off_despite_new_demand() {
    let (send, recv) = mpsc::channel();
    let worker = Worker::start(1, timing(), move || {
        send.send(Instant::now()).unwrap();
        MaintenanceOutcome::Retry
    });
    let mut previous = recv.recv_timeout(Duration::from_secs(2)).unwrap();
    for minimum in [20, 40, 80, 80] {
        // Demand and condvar wakeups cannot bypass the retry deadline.
        for _ in 0..100 {
            worker.pool.request_maintenance();
        }
        let next = recv.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(next.duration_since(previous) >= Duration::from_millis(minimum));
        previous = next;
    }
}

#[test]
fn incomplete_refill_without_reported_error_cannot_busy_loop() {
    let (send, recv) = mpsc::channel();
    let _worker = Worker::start(1, timing(), move || {
        send.send(Instant::now()).unwrap();
        MaintenanceOutcome::Complete // Deliberately fails to fill the pool.
    });
    let first = recv.recv_timeout(Duration::from_secs(2)).unwrap();
    let second = recv.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(second.duration_since(first) >= timing().retry_initial);
}

#[test]
fn idle_pool_discovers_and_retries_cleanup_without_requests() {
    let state = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let pending = Arc::clone(&state);
    let (send, recv) = mpsc::channel();
    let _worker = Worker::start(0, timing(), move || {
        let observed = pending.load(Ordering::Acquire);
        if observed == 2 {
            pending.store(3, Ordering::Release);
        }
        send.send(observed).unwrap();
        if observed == 1 {
            MaintenanceOutcome::Retry
        } else {
            MaintenanceOutcome::Complete
        }
    });
    assert_eq!(recv.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
    state.store(1, Ordering::Release); // Cleanup arrives after the idle callback.
    loop {
        if recv.recv_timeout(Duration::from_secs(2)).unwrap() == 1 {
            break;
        }
    }
    state.store(2, Ordering::Release); // Repair; still no acquisition/wake request.
    loop {
        if recv.recv_timeout(Duration::from_secs(2)).unwrap() == 2 {
            break;
        }
    }
    assert_eq!(state.load(Ordering::Acquire), 3);
}

#[test]
fn shutdown_wakes_and_joins_a_backing_off_worker() {
    let (send, recv) = mpsc::channel();
    let worker = Worker::start(
        0,
        MaintenanceTiming {
            retry_initial: Duration::from_secs(30),
            retry_max: Duration::from_secs(30),
            idle_poll: Duration::from_secs(30),
        },
        move || {
            send.send(()).unwrap();
            MaintenanceOutcome::Retry
        },
    );
    recv.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(recv.recv_timeout(Duration::from_millis(30)).is_err());
    let started = Instant::now();
    worker.pool.drain_all();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(worker.pool.maintenance_worker.lock().unwrap().is_none());
    assert!(recv.recv_timeout(Duration::from_millis(30)).is_err());
}

#[test]
fn callback_panic_preserves_automatic_retry() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = Arc::clone(&calls);
    let (send, recv) = mpsc::channel();
    let _worker = Worker::start(0, timing(), move || {
        if attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            panic!("injected maintenance failure");
        }
        send.send(()).unwrap();
        MaintenanceOutcome::Complete
    });
    recv.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(calls.load(Ordering::Acquire) >= 2);
}

#[test]
fn disabled_or_shutdown_pool_cannot_start_maintenance() {
    for enabled in [false, true] {
        // Public startup owns a process-lifetime reference. These two inert
        // fixtures intentionally live until the test process exits.
        let pool: &'static WarmPool<u32> = Box::leak(Box::new(WarmPool::<u32>::new(PoolConfig {
            low_watermark: 1,
            high_watermark: 1,
            maintenance_enabled: enabled,
            startup_prewarm: false,
        })));
        if enabled {
            pool.drain_all();
        }
        let lifecycle_lock = pool.maintenance_worker.lock().unwrap();
        let (done_send, done_recv) = mpsc::channel();
        let starter = std::thread::spawn(move || {
            pool.start_maintenance_worker(|| panic!("inert pool started a worker"));
            done_send.send(()).unwrap();
        });
        let returned = done_recv.recv_timeout(Duration::from_secs(2));
        drop(lifecycle_lock);
        starter.join().unwrap();
        assert!(
            returned.is_ok(),
            "inert manager lookup waited for an unrelated lifecycle lock"
        );
        assert!(!pool.maintenance_started.load(Ordering::Acquire));
        assert!(pool.maintenance_worker.lock().unwrap().is_none());
    }
}

#[test]
fn shutdown_joins_an_active_callback_before_returning() {
    let pool: &'static WarmPool<u32> = Box::leak(Box::new(WarmPool::new(PoolConfig {
        low_watermark: 0,
        high_watermark: 1,
        maintenance_enabled: true,
        startup_prewarm: false,
    })));
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    pool.start_maintenance_worker(move || {
        entered_send.send(()).unwrap();
        release_recv.recv().unwrap();
        MaintenanceOutcome::Complete
    });
    entered_recv.recv_timeout(Duration::from_secs(2)).unwrap();
    let (done_send, done_recv) = mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        pool.drain_all();
        done_send.send(()).unwrap();
    });
    let prematurely_done = done_recv.recv_timeout(Duration::from_millis(30)).is_ok();
    release_send.send(()).unwrap();
    if !prematurely_done {
        done_recv.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    shutdown.join().unwrap();
    assert!(
        !prematurely_done,
        "shutdown returned while callback still owned work"
    );
    assert!(pool.maintenance_worker.lock().unwrap().is_none());
}
