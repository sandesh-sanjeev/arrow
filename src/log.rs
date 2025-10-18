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
