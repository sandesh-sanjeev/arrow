use std::sync::atomic::{AtomicBool, Ordering::*};

pub struct MutLock(AtomicBool);

impl MutLock {
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

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

pub struct MutGuard<'a>(&'a AtomicBool);

impl Drop for MutGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Release);
    }
}
