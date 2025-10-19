use crate::{lock::MutGuard, log::LogVec, storage::Storage};
use crossbeam_utils::atomic::AtomicCell;
use std::{io, path::Path};

pub struct Page {
    storage: Storage,
    page_prev_seq_no: u64,
    state: AtomicCell<State>,
}

impl Page {
    pub fn create<P: AsRef<Path>>(path: P, prev_seq_no: u64, guard: &MutGuard) -> io::Result<Self> {
        let storage = Storage::create(path)?;
        let state = AtomicCell::new(State {
            count: 0,
            prev_seq_no,
        });

        // Header for the page.
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&1u64.to_be_bytes());
        buf.extend_from_slice(&prev_seq_no.to_be_bytes());
        storage.append(&buf, guard)?;

        Ok(Self {
            storage,
            state,
            page_prev_seq_no: prev_seq_no,
        })
    }

    pub fn page_prev_seq_no(&self) -> u64 {
        self.page_prev_seq_no
    }

    pub fn state(&self) -> State {
        self.state.load()
    }

    pub fn append(&self, logs: &LogVec, guard: &MutGuard) -> io::Result<bool> {
        // Return early if there is nothing to append.
        let (Some(first), Some(last)) = (logs.first_seq_no(), logs.prev_seq_no()) else {
            return Ok(false);
        };

        // Read the latest state.
        // Return early if sequence validation fails.
        let mut state = self.state.load();
        if first <= state.prev_seq_no {
            return Ok(false);
        }

        // Nice, we can write out log bytes.
        self.storage.append(logs.bytes(), guard)?;

        // Update state with new logs.
        state.count += logs.count();
        state.prev_seq_no = last;
        self.state.swap(state);
        Ok(true)
    }

    pub fn query(&self, logs: &mut LogVec, cursor: Cursor) -> io::Result<Cursor> {
        logs.overwrite(|buf| self.storage.read_at(cursor.0, buf))?;

        // Return pointer to next set of logs.
        Ok(Cursor(cursor.0 + logs.bytes().len() as u64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State {
    pub count: usize,
    pub prev_seq_no: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor(u64);

impl Cursor {
    /// Index to first log in the page.
    /// If page is empty, the log itself might not exist.
    pub const ZERO: Cursor = Cursor(16);
}
