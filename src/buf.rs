//! A re-usable buffer of log records.

use crate::log::Log;

/// A growable, reusable buffer of sequenced log records.
pub struct LogBuf {
    count: usize,
    memory: Vec<u8>,
    last: Option<u64>,
}

impl LogBuf {
    /// Create a new buffer with some pre-defined capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of bytes to reserve in the buffer.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            count: 0,
            last: None,
            memory: Vec::with_capacity(capacity),
        }
    }

    /// Number of log records in the buffer.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Number of bytes currently held in the buffer.
    pub fn len(&self) -> usize {
        self.memory.len()
    }

    /// Returns true buf is empty, false otherwise.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Maximum number of bytes that can be held in the buffer without reallocation.
    pub fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    /// Sequence number of the first log record in the buffer.
    pub fn first(&self) -> Option<u64> {
        self.iter().next().map(|log| log.seq_no())
    }

    /// Sequence number of the last log record in the buffer.
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// An iterator to iterate through log records in the buffer.
    pub fn iter(&self) -> LogVecIter<'_> {
        LogVecIter(&self.memory)
    }

    /// Append a log record into the buffer.
    ///
    /// Note that this might result in heap allocation/reallocation depending
    /// on the capacity of the buffer and logs already appended into buffer.
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to append into buffer.
    ///
    /// # Returns
    ///
    /// Returns true if the append was successful. false if sequence validation
    /// failed, when this happens log is not appended into the buffer.
    #[must_use = "returns true only if appended successfully"]
    pub fn append(&mut self, log: &Log<'_>) -> bool {
        // Perform sequence validation.
        if let Some(prev_seq_no) = self.last
            && prev_seq_no >= log.seq_no()
        {
            return false;
        }

        // Write log bytes into underlying buffer.
        log.write(&mut self.memory);

        // Keep track of the new state.
        self.count += 1;
        self.last = Some(log.seq_no());
        true
    }

    /// Clear all logs from the buffer.
    pub fn clear(&mut self) {
        self.count = 0;
        self.memory.clear();
        self.last = None;
    }

    /// Allocates additional capacity in the buffer.
    ///
    /// Note that this might speculatively allocate more bytes than
    /// requested to prevent future frequent allocations. Also might
    /// not allocate at all if buffer already has enough capacity.
    ///
    /// # Arguments
    ///
    /// * `additional` - Additional bytes to allocate.
    pub fn reserve(&mut self, additional: usize) {
        self.memory.reserve(additional);
    }

    /// Reclaim memory by shrinking the buffer.
    ///
    /// Note that this will not shrink smaller than the current length.
    ///
    /// When the buffer size gets too large, for example after a big batch
    /// of logs, this is the way to release some of the excess capacity.
    pub fn shrink_to(&mut self, capacity: usize) {
        self.memory.shrink_to(capacity);
    }

    /// Reference to bytes backing this buffer.
    #[allow(dead_code)]
    pub(crate) fn bytes(&self) -> &Vec<u8> {
        &self.memory
    }

    /// Mutable reference to bytes backing this buffer.
    #[allow(dead_code)]
    pub(crate) fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.memory
    }

    /// Reinitialize state of the buffer with contents of memory.
    #[allow(dead_code)]
    pub(crate) fn reinitialize(&mut self) {
        // Go over all the logs and
        let mut count = 0;
        let mut last = None;
        let mut logs = self.iter();
        while let Some(log) = logs.next() {
            count += 1;
            last = Some(log.seq_no());
        }

        // Check how many bytes are remaining, if any.
        // And chop them off, because they are excess bytes.
        let excess = logs.0.len();
        self.memory.truncate(self.memory.len() - excess);

        // Update current state with what we just found.
        self.count = count;
        self.last = last;
    }
}

/// An iterator to iterate through logs in the buffer.
pub struct LogVecIter<'a>(&'a [u8]);

impl LogVecIter<'_> {
    /// Get the next log record, None if no more logs exist.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Log<'_>> {
        // Parse the next log in the underlying buffer.
        // Track the bytes to read next log from.
        let (log, remaining) = Log::read(self.0)?;
        self.0 = remaining;

        // Return parsed log record.
        Some(log)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    const LOG_1: Log<'static> = Log::new_borrowed(1, b"Rust");
    const LOG_2: Log<'static> = Log::new_borrowed(2, b"Java");
    const LOG_3: Log<'static> = Log::new_borrowed(3, b"Python");

    #[test]
    fn append_records_logs_accepted() {
        let mut buf = LogBuf::with_capacity(32);

        // Make sure initial state of the buffer is correct.
        assert!(buf.is_empty());
        assert_eq!(32, buf.capacity());
        assert_eq!(0, buf.len());
        assert_eq!(0, buf.count());
        assert_eq!(None, buf.first());
        assert_eq!(None, buf.last());

        // They should all be accepted.
        assert!(buf.append(&LOG_1));
        assert!(buf.append(&LOG_2));
        assert!(buf.append(&LOG_3));

        // Make sure state of the buffer is as expected.
        assert!(!buf.is_empty());
        assert_eq!(3, buf.count());
        assert_eq!(Some(1), buf.first());
        assert_eq!(Some(3), buf.last());

        // Read those logs back and make sure correct.
        let mut logs = buf.iter();
        assert_eq!(Some(LOG_1), logs.next());
        assert_eq!(Some(LOG_2), logs.next());
        assert_eq!(Some(LOG_3), logs.next());
        assert_eq!(None, logs.next());
    }

    #[test]
    fn out_of_seq_append_is_rejected() {
        let mut buf = LogBuf::with_capacity(32);

        // First append log with sequence number 3.
        assert!(buf.append(&LOG_3));

        // Any log <= 3 should be rejected.
        assert!(!buf.append(&LOG_1));
        assert!(!buf.append(&LOG_2));
        assert!(!buf.append(&LOG_3));

        // Make sure state of the buffer is correct.
        assert_eq!(1, buf.count());
        assert_eq!(Some(3), buf.first());
        assert_eq!(Some(3), buf.last());

        // Only 3 should be visible.
        let mut logs = buf.iter();
        assert_eq!(Some(LOG_3), logs.next());
        assert_eq!(None, logs.next());
    }

    #[test]
    fn clear_resets_buf() {
        let mut buf = LogBuf::with_capacity(32);

        // They should all be accepted.
        assert!(buf.append(&LOG_1));
        assert!(buf.append(&LOG_2));
        assert!(buf.append(&LOG_3));

        // Make sure state of the buffer is as expected.
        assert_eq!(3, buf.count());
        assert_eq!(Some(1), buf.first());
        assert_eq!(Some(3), buf.last());

        // Clear the buffer.
        buf.clear();

        // Should be more or less equal to initial state.
        // Capacity can change, that the "more or less".
        assert!(buf.capacity() >= 32);
        assert_eq!(0, buf.len());
        assert_eq!(0, buf.count());
        assert_eq!(None, buf.first());
        assert_eq!(None, buf.last());
    }

    #[test]
    fn reserve_reserves_additional_capacity() {
        let mut buf = LogBuf::with_capacity(32);

        // Allocate more capacity.
        // At least those many bytes should be allocated.
        buf.reserve(1024);
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn reserve_noop() {
        let mut buf = LogBuf::with_capacity(32);

        // Allocate more capacity.
        // Buffer already has enough capacity.
        buf.reserve(16);
        assert_eq!(32, buf.capacity());
    }

    #[test]
    fn shrink_to_shinks_to_capacity() {
        let mut buf = LogBuf::with_capacity(1024);

        // Release memory.
        buf.shrink_to(32);
        assert!(buf.capacity() == 32);
    }

    #[test]
    fn shrink_to_noop() {
        let mut buf = LogBuf::with_capacity(1024);

        // Append into buffer.
        assert!(buf.append(&LOG_1));

        let len = buf.len();
        assert!(len > 5 && len < 1024);

        // To some random small value.
        buf.shrink_to(5);

        // Should not shrink below current length.
        assert_eq!(buf.len(), len);
    }

    #[test]
    fn copy_bytes_and_clone_buf() {
        let mut buf_1 = LogBuf::with_capacity(32);
        let mut buf_2 = LogBuf::with_capacity(32);

        // Append a log into the buffer.
        assert!(buf_1.append(&LOG_1));

        // Source and destination internal buffers.
        let src = buf_1.bytes();
        let dst = buf_2.bytes_mut();

        // Copy bytes and initialize state.
        dst.extend_from_slice(&src);
        buf_2.reinitialize();

        // Make sure new state is correct.
        assert_eq!(buf_1.count(), buf_2.count());
        assert_eq!(buf_1.first(), buf_2.first());
        assert_eq!(buf_1.last(), buf_2.last());
        assert_eq!(buf_1.len(), buf_2.len());
    }
}
