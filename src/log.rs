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
    pub fn new_borrowed(seq_no: u64, data: &[u8]) -> Log<'_> {
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
    pub fn new_owned(seq_no: u64, data: Vec<u8>) -> Log<'static> {
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
}
