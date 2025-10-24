//! Sequenced log records appended into ring buffer.

use std::{borrow::Cow, cmp::Ordering};

/// A user generated sequenced log record.
///
/// This is the only type of record that can be appended into
/// the ring buffer, at least for the foreseeable future.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    seq_no: u64,
    data: Cow<'a, [u8]>,
}

impl Log<'_> {
    /// Create new log from borrowed data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `data` - Payload of the log.
    pub const fn new_borrowed(seq_no: u64, data: &[u8]) -> Log<'_> {
        Log {
            seq_no,
            data: Cow::Borrowed(data),
        }
    }

    /// Create new log from owned data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `data` - Payload of the log.
    pub const fn new_owned(seq_no: u64, data: Vec<u8>) -> Log<'static> {
        Log {
            seq_no,
            data: Cow::Owned(data),
        }
    }

    /// Sequence number of the log record.
    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    /// Reference to data held in log.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Obtain owned copy of log from this one.
    pub fn into_owned(self) -> (u64, Vec<u8>) {
        (self.seq_no, self.data.into_owned())
    }

    /// Append log bytes into a buffer.
    ///
    /// Returns the number of bytes written into buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to write log bytes into.
    pub(crate) fn write(&self, buf: &mut Vec<u8>) -> usize {
        let seq_no_bytes = self.seq_no.to_be_bytes();
        let size_bytes = self.data.len().to_be_bytes();

        // TODO: Add checksums for integrity checks.
        // Append all the bytes into the buffer.
        buf.extend_from_slice(&seq_no_bytes);
        buf.extend_from_slice(&size_bytes);
        buf.extend_from_slice(&self.data);

        // Return total number of bytes appended into buffer.
        seq_no_bytes.len() + size_bytes.len() + self.data.len()
    }

    /// Parse log bytes from a buffer.
    ///
    /// Returns parsed log and bytes remaining after parsing one log. If
    /// enough logs are not available to parse an entire log, returns None.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to read log bytes from.
    pub(crate) fn read(buf: &[u8]) -> Option<(Log<'_>, &[u8])> {
        // Fetch the sequence number of the log.
        let (seq_no_bytes, buf) = Self::const_copy_n(buf)?;
        let seq_no = u64::from_be_bytes(seq_no_bytes);

        // Fetch the size of log payload.
        let (size_bytes, buf) = Self::const_copy_n(buf)?;
        let size = usize::from_be_bytes(size_bytes);

        // Fetch the log payload.
        let (data, buf) = Self::next_n(buf, size)?;

        // Cool, have everything to construct a log record.
        Some((Log::new_borrowed(seq_no, data), buf))
    }

    /// Helper to copy next N (compile time known) bytes from a source buffer.
    ///
    /// If there are enough bytes returns a copy of those bytes, along with remaining
    /// bytes. If source buffer does not have enough bytes, returns None.
    fn const_copy_n<const N: usize>(src: &[u8]) -> Option<([u8; N], &[u8])> {
        let (bytes, next_bytes) = Self::next_n(src, N)?;
        let array = bytes.try_into().expect("Should never fail");
        Some((array, next_bytes))
    }

    /// Helper to get reference to next N bytes from a source buffer.
    ///
    /// If there are enough bytes returns a reference of those bytes,
    /// along with remaining bytes. If source buffer does not have enough
    /// bytes, returns None.
    fn next_n(src: &[u8], len: usize) -> Option<(&[u8], &[u8])> {
        src.split_at_checked(len)
    }
}

impl Ord for Log<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq_no.cmp(&other.seq_no)
    }
}

impl PartialOrd for Log<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn new_owned() {
        let seq_no = 22;
        let data = vec![9; 250];

        let log = Log::new_owned(seq_no, data.clone());
        assert_eq!(seq_no, log.seq_no());
        assert_eq!(&data, log.data());
    }

    #[test]
    fn new_borrowed() {
        let seq_no = 22;
        let data = vec![9; 250];

        let log = Log::new_borrowed(seq_no, &data);
        assert_eq!(seq_no, log.seq_no());
        assert_eq!(&data, log.data());
    }

    #[test]
    fn into_owned_returns_owned_copy() {
        let seq_no = 22;
        let data = vec![9; 250];

        let log = Log::new_borrowed(seq_no, &data);
        let (r_seq_no, r_data) = log.into_owned();

        assert_eq!(seq_no, r_seq_no);
        assert_eq!(&data, &r_data);
    }

    #[test]
    fn cmp_compares_logs() {
        let log_1 = Log::new_borrowed(1, b"data");
        let log_2 = Log::new_borrowed(2, b"data");

        assert!(log_1 < log_2);
        assert!(log_2 > log_1);
        assert!(log_1 == log_1);
        assert!(log_2 == log_2);
    }

    #[test]
    fn serialization_round_trip() {
        let mut buf = Vec::new();

        // Write some log records into buffer.
        let log_1 = Log::new_borrowed(69, b"batman");
        let log_2 = Log::new_borrowed(71, b"superman");

        log_1.write(&mut buf);
        log_2.write(&mut buf);

        // Parse log records back.
        let (r_log_1, buf) = Log::read(&buf).expect("Should parse log");
        let (r_log_2, buf) = Log::read(&buf).expect("Should parse log");

        // Make sure expected results.
        assert_eq!(log_1, r_log_1);
        assert_eq!(log_2, r_log_2);
        assert!(buf.is_empty()); // No more logs.
    }

    #[test]
    fn read_not_enough_bytes_returns_empty() {
        let mut buf = Vec::new();

        // Empty buffer should not parse log.
        assert!(Log::read(&buf).is_none());

        // Write a log record into buffer.
        let log = Log::new_borrowed(69, b"batman");
        log.write(&mut buf);

        // Remove the last bytes from the buffer.
        for _ in 0..buf.len() {
            buf.truncate(buf.len() - 1);

            // Buffer should not have enough bytes read next log.
            assert!(Log::read(&buf).is_none());
        }
    }
}
