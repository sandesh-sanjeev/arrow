//! A thread safe, lock-free variant of a Vector.

use crate::lock::RawLock;
use crate::sync::atomic::{AtomicUsize, Ordering::*};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::{drop_in_place, slice_from_raw_parts_mut};
use std::slice;
use thiserror::Error;

/// Different types of errors that can be returned.
#[derive(Debug, Error)]
pub enum Error<T> {
    Overflow,
    Conflict(T),
}

/// An AtomicVec is a concurrent variant of [`Vec`].
///
/// * Can be mutably shared across multiple threads at once.
/// * Once allocated, as of now, this vector cannot grow or shrink in size.
#[derive(Debug)]
pub struct AtomicVec<T> {
    cap: usize,
    ptr: *mut T,
    lock: RawLock,
    len: AtomicUsize,
}

unsafe impl<T: Send> Send for AtomicVec<T> {}
unsafe impl<T: Sync> Sync for AtomicVec<T> {}

impl<T> AtomicVec<T> {
    /// Create a vector with some predefined capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of elements this vector can hold.
    pub fn with_capacity(capacity: usize) -> Self {
        let vec = Vec::with_capacity(capacity);
        let mut memory = ManuallyDrop::new(vec);

        Self {
            cap: capacity,
            ptr: memory.as_mut_ptr(),
            lock: RawLock::new(),
            len: AtomicUsize::new(0),
        }
    }

    /// Append an element at the end of a Vector.
    ///
    /// Note that as of now vector has a fixed size. Push will fail if
    /// vector exceeds maximum size of the vector. Additionally the push
    /// might fail if there was a conflicting writer.
    ///
    /// # Arguments
    ///
    /// * `elem` - Element to push into the vector.
    pub fn push(&self, elem: T) -> Result<(), Error<T>> {
        // Obtain an exclusive write lock for the vector.
        let Some(_guard) = self.lock.try_acquire() else {
            return Err(Error::Conflict(elem));
        };

        // Make sure there is no overflow.
        let len = self.len.load(Relaxed);
        if len == self.cap {
            return Err(Error::Overflow);
        }

        // Safety
        // 1. Pointer to allocated memory is guaranteed to be non-null.
        // 2. We have made sure length does not exceed capacity.
        unsafe {
            self.ptr.add(len).write(elem);
            self.len.fetch_add(1, Release);
        }

        Ok(())
    }
}

impl<T> Deref for AtomicVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        let len = self.len.load(Acquire);

        // Safety
        // 1. Pointer to allocated memory is guaranteed to be non-null.
        // 2. Length can only increase, cannot decrease.
        unsafe { slice::from_raw_parts(self.ptr, len) }
    }
}

impl<T> Drop for AtomicVec<T> {
    fn drop(&mut self) {
        let len = self.len.load(Acquire);
        let to_drop = slice_from_raw_parts_mut(self.ptr, len);

        // Safety
        // 1. Pointer to allocated memory is guaranteed to be non-null.
        // 2. Length can only increase, cannot decrease.
        unsafe {
            drop_in_place(to_drop);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const MAX_THREADS: usize = 32;

    #[test]
    fn state_machine_test() {
        let capacity = MAX_THREADS * 100;
        let vec = &AtomicVec::with_capacity(capacity);
        thread::scope(|scope| {
            // Read all the future elements in the vector.
            let mut workers = Vec::with_capacity(MAX_THREADS);
            for _ in 0..MAX_THREADS {
                workers.push(scope.spawn(move || {
                    for i in 0..capacity {
                        loop {
                            // If index does not exist, just try again in a bit.
                            let Some(elem) = vec.get(i) else {
                                continue;
                            };

                            // Make sure element has expected value.
                            assert_eq!(*elem, i);
                            break;
                        }
                    }
                }));
            }

            // Insert all elements.
            for i in 0..capacity {
                vec.push(i).expect("Should push");
            }

            // Wait for all worker threads to complete.
            for worker in workers {
                worker.join().expect("Thread should join");
            }

            // Make sure expected results.
            let expected: Vec<_> = (0..capacity).collect();
            assert_eq!(expected, vec.as_ref());
        });
    }
}
