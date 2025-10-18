use std::{borrow::Cow, cmp::Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    seq_no: u64,
    data: Cow<'a, [u8]>,
}

impl Log<'_> {
    pub fn new_borrowed(seq_no: u64, data: &[u8]) -> Log<'_> {
        Log {
            seq_no,
            data: Cow::Borrowed(data),
        }
    }

    pub fn new_owned(seq_no: u64, data: Vec<u8>) -> Log<'static> {
        Log {
            seq_no,
            data: Cow::Owned(data),
        }
    }

    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

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

pub struct LogVec {
    count: usize,
    memory: Vec<u8>,
    last_seq_no: Option<u64>,
}

impl LogVec {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            count: 0,
            last_seq_no: None,
            memory: Vec::with_capacity(capacity),
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn bytes(&self) -> &[u8] {
        &self.memory
    }

    pub fn capacity(&self) -> usize {
        self.memory.capacity()
    }

    pub fn first_seq_no(&self) -> Option<u64> {
        self.iter().next().map(|log| log.seq_no())
    }

    pub fn last_seq_no(&self) -> Option<u64> {
        self.last_seq_no
    }

    pub fn iter(&self) -> LogVecIter<'_> {
        LogVecIter(&self.memory)
    }

    pub fn append(&mut self, log: Log<'_>) {
        // Bytes that represent sequence number.
        let seq_no_bytes = log.seq_no.to_be_bytes();
        self.memory.extend_from_slice(&seq_no_bytes);

        // Bytes that represent payload of the log.
        let data_bytes = log.data();

        // Bytes that represent size of payload.
        // Without this we wouldn't know how many bytes to read.
        // So this has to come before data_bytes.
        let size_bytes = data_bytes.len().to_be_bytes();
        self.memory.extend_from_slice(&size_bytes);

        // Write payload bytes into this memory.
        self.memory.extend_from_slice(data_bytes);

        // Finally keep track of the size.
        self.count += 1;
    }

    pub fn clear(&mut self) {
        self.count = 0;
        self.memory.clear();
    }

    pub fn shrink_to(&mut self, capacity: usize) {
        self.memory.shrink_to(capacity);
    }
}

pub struct LogVecIter<'a>(&'a [u8]);

impl LogVecIter<'_> {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Log<'_>> {
        // Fetch the sequence number of the log.
        let seq_no_bytes = self.copy_n()?;
        let seq_no = u64::from_be_bytes(seq_no_bytes);

        // Fetch the size of log payload.
        let size_bytes = self.copy_n()?;
        let size = usize::from_be_bytes(size_bytes);

        // Fetch the log payload.
        let data = self.next_n(size)?;

        // Cool, have everything to construct a log record.
        Some(Log::new_borrowed(seq_no, data))
    }

    fn copy_n<const N: usize>(&mut self) -> Option<[u8; N]> {
        let bytes = self.next_n(N)?;
        Some(bytes.try_into().expect("Should never fail"))
    }

    fn next_n(&mut self, len: usize) -> Option<&[u8]> {
        if self.0.len() < len {
            return None;
        }

        let bytes = &self.0[..len];
        self.0 = &self.0[len..];
        Some(bytes)
    }
}
