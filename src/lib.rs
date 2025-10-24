//! # Arrow
//!
//! Arrow is an on disk ring buffer of sequenced records. It allows a single writer to append
//! new records. Any number of readers can concurrently read from the ring buffer without any
//! conflicts/synchronization.
//!
//! This sort of thing is typically useful to buffer large amounts of logs locally for fan-out
//! purposes. However it is important to note that space is reclaimed when ring buffer is full,
//! it does not wait for anyone to catchup. In other words, there is no back-pressure between
//! readers and writer.

// To customize parts of code that is included in coverage analysis.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod buf;
pub mod lock;
pub mod log;
pub mod storage;
