//! Arrow

// TODO: Remove after initial implementation.
#![allow(dead_code)]

pub(crate) mod lock;

/// Synchronization primitives that are swapped out during concurrency tests.
mod sync {
    pub use std::hint::spin_loop;
    pub use std::sync::*;
}
