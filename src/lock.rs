//! Futex free lock for exclusive mutations to ring buffer.

use std::sync::atomic::{AtomicBool, Ordering::*};

/// An exclusive lock to protect against concurrent updates.
pub struct MutLock(AtomicBool);

impl MutLock {
    /// Create a new lock.
    ///
    /// This lock will be unlocked when newly created.
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    /// Try to obtain exclusive write lock.
    ///
    /// If another writer is already in process, this returns early without
    /// obtaining a write lock. This does not involve waiting or syscalls.
    pub fn try_lock(&self) -> Option<MutGuard<'_>> {
        if self.0.swap(true, Acquire) {
            return None;
        }

        Some(MutGuard(&self.0))
    }
}

impl Default for MutLock {
    fn default() -> Self {
        MutLock::new()
    }
}

/// RAII guard to unlock the lock when the guard goes out of scope.
pub struct MutGuard<'a>(&'a AtomicBool);

impl Drop for MutGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Release);
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::{sync::atomic::AtomicU64, thread};

    #[test]
    fn lock_acquire_only_one_wins() {
        let lock = MutLock::default();
        let guards = thread::scope(|scope| {
            // Spawn bunch of workers to obtain lock.
            let mut workers = Vec::new();
            for _ in 0..10 {
                workers.push(scope.spawn(|| lock.try_lock()));
            }

            // Make sure only one thread obtained lock.
            let mut guards = Vec::new();
            for worker in workers {
                let maybe_guard = worker.join().expect("Should join successfully");
                guards.push(maybe_guard);
            }

            guards
        });

        // Regardless of number of threads contending for lock,
        // only one of the threads should win and obtain lock.
        let obtained = guards.into_iter().filter_map(|guard| guard).count();
        assert_eq!(1, obtained);
    }

    #[test]
    fn lock_acquired_cannot_be_acquired_again() {
        let lock = MutLock::default();
        let _guard = lock.try_lock().expect("Should obtain lock");

        // Check how many threads can acquire lock.
        let acquired = AtomicU64::new(0);
        thread::scope(|scope| {
            for _ in 0..10 {
                scope.spawn(|| {
                    if lock.try_lock().is_some() {
                        acquired.fetch_add(1, Relaxed);
                    }
                });
            }
        });

        // Main thread is holding the lock.
        // No other thread should be able to obtain the lock.
        assert_eq!(0, acquired.load(Relaxed));
    }

    #[test]
    fn guard_goes_out_of_scope_can_be_acquired_again() {
        let lock = MutLock::default();
        thread::scope(|scope| {
            for _ in 0..10 {
                scope.spawn(|| while lock.try_lock().is_none() {});
            }
        });
    }
}
