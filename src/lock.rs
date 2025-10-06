//! Locks to guard against concurrent mutations from multiple threads.

use crate::sync::{
    atomic::{AtomicBool, Ordering::*},
    spin_loop,
};

/// An exclusive write lock without `Mutex` or `RwLock`.
///
/// * [`Self::spin_acquire`] waits to acquire a lock, i.e, not wait-free.
/// * [`Self::try_acquire`] returns immediately, i.e, if wait-free.
///
/// When lock is acquired, a [`RawLockGuard`] is returned. You can fearlessly
/// make mutations while holding on the the guard. When the guard goes out
/// of scope, lock is automatically released.
#[derive(Debug)]
pub(crate) struct RawLock(AtomicBool);

impl RawLock {
    /// Create a new exclusive write lock.
    ///
    /// Note that this in itself does not acquire the newly created lock.
    /// Lock must be explicitly acquired using [`Self::spin_acquire`] or
    /// [`Self::try_acquire`].
    #[inline]
    pub(crate) const fn new() -> Self {
        RawLock(AtomicBool::new(false))
    }

    /// Acquire an exclusive lock is acquired.
    ///
    /// Note that this is a spin lock, i.e, the thread consumed CPU while
    /// waiting for a previous lock to be released. There is no fairness
    /// either, i.e, if thread-1 started waited for lock before thread-2,
    /// there is no guarantee that thread-1 acquires lock first.
    ///
    /// For a wait-free alternative, use [`Self::try_acquire`].
    #[inline]
    pub(crate) fn spin_acquire(&self) -> RawLockGuard<'_> {
        while self.0.swap(true, Acquire) {
            // Hint to CPU that this is a spin loop.
            // This allows the CPU to perform certain optimizations,
            // such as saving power on the core.
            spin_loop();
        }

        RawLockGuard(&self.0)
    }

    /// Try to acquire an exclusive lock.
    ///
    /// This always returns immediately. Lock guard is returned if successful,
    /// i.e, no other thread was holding the lock. None is returned otherwise.
    #[inline]
    pub(crate) fn try_acquire(&self) -> Option<RawLockGuard<'_>> {
        if self.0.swap(true, Acquire) {
            return None;
        }

        Some(RawLockGuard(&self.0))
    }
}

/// An RAII guard used to hold an *acquired* exclusive write lock.
///
/// When this guard goes out of scope, lock is automatically released.
#[derive(Debug)]
pub(crate) struct RawLockGuard<'a>(&'a AtomicBool);

impl Drop for RawLockGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(false, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const MAX_THREADS: usize = 32;

    #[test]
    #[allow(static_mut_refs)]
    fn state_machine_test() {
        static LOCK: RawLock = RawLock::new();
        static mut DATA: Vec<usize> = Vec::new();

        thread::scope(|scope| {
            // Update the same data from multiple threads.
            let mut workers = Vec::with_capacity(MAX_THREADS);
            for i in 0..MAX_THREADS {
                let handle = scope.spawn(move || {
                    let _guard = LOCK.spin_acquire();
                    unsafe { DATA.push(i) };
                });
                workers.push(handle);
            }

            // Wait for all worker threads to complete.
            for worker in workers {
                worker.join().expect("Thread should join");
            }

            // Acquire lock to sort data.
            let _guard = LOCK.try_acquire().expect("Should obtain lock");
            unsafe { DATA.sort() };

            // Make sure expected results.
            let expected: Vec<_> = (0..MAX_THREADS).collect();
            assert_eq!(expected, unsafe { DATA.as_slice() });
        });
    }
}
