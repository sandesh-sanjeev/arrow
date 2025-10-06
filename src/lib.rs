//! Arrow

// TODO: Remove after initial implementation.
#![allow(dead_code)]

pub(crate) mod avec;
pub(crate) mod lock;

pub use avec::AtomicVec;

/// Synchronization primitives swapped out during concurrency tests.
mod sync {
    pub use std::hint::spin_loop;
    pub use std::sync::*;
}
