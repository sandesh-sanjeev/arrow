//! Append only storage backed by file on disk.

use std::{
    cmp::min,
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering::*},
};

/// A lock-free single writer, multiple reader append only storage.
///
/// The only reason this is not wait free is because disk I/O is inherently
/// a blocking operation. It is indeed wait free in the sense that no wait
/// occur for any internal locking.
///
/// This data structure is thread safe, but not safe for concurrent access
/// or mutation across processes. We will probably never support synchronization
/// across processes, except for attempting to prevent such a case using advisory
/// file locks.
pub struct Storage {
    file: File,
    path: PathBuf,
    len: AtomicU64,
    lock: AtomicBool,
}

impl Storage {
    /// Create storage file in read-append mode.
    ///
    /// Returns an error if file already exists in path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;

        Ok(Self {
            file,
            path,
            len: AtomicU64::new(0),
            lock: AtomicBool::new(false),
        })
    }

    /// Open storage file in append mode.
    ///
    /// Returns an error if file doesn't already exist in path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .open(&path)?;

        // Fetch current size of the file.
        // This is not a file we just created, so don't know the size.
        let len = file.metadata()?.len();

        Ok(Self {
            file,
            path,
            len: AtomicU64::new(len),
            lock: AtomicBool::new(false),
        })
    }

    /// Flushes any intermediate buffers in between the disk,
    /// guaranteeing that writes have made it to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Start a write transaction against storage.
    ///
    /// If there is another write transaction in progress, this method
    /// returns early without creating a new write transaction. However
    /// this only applies across threads. There is no synchronization
    /// across processes.
    pub fn append_txn(&self) -> Option<AppendTxn<'_>> {
        // Obtain an exclusive write lock to the storage.
        if self.lock.swap(true, Acquire) {
            return None;
        }

        // Read the current state of storage.
        let len = self.len.load(Acquire);

        // Return the newly created transaction.
        Some(AppendTxn {
            next: len,
            start: len,
            complete: false,
            file: &self.file,
            len: &self.len,
            lock: &self.lock,
        })
    }

    /// Start a read transaction against storage.
    ///
    /// If starting offset is beyond the end of storage, this method
    /// returns early without creating a new read transaction.
    ///
    /// Provides snapshot read isolation level. You will read state as
    /// it existed when the transaction begun.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset in storage to begin reads.
    pub fn read_txn(&self, offset: u64) -> Option<ReadTxn<'_>> {
        // Read the current state of storage.
        let len = self.len.load(Acquire);
        if offset >= len {
            return None;
        }

        // Return the newly created transaction.
        Some(ReadTxn {
            len,
            next: offset,
            file: &self.file,
        })
    }

    /// Truncate storage to new length.
    ///
    /// Bytes will be removed from the end of storage.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of storage.
    pub fn truncate(self, len: u64) -> Result<(), (Self, io::Error)> {
        // Only truncate if there are some bytes to truncate.
        let storage_len = self.len.load(Acquire);
        if len <= storage_len {
            return Ok(());
        }

        // Resize storage.
        // Because of the check above, guaranteed to only truncate.
        let Err(e) = self.file.set_len(len) else {
            return Ok(());
        };

        Err((self, e))
    }

    /// Destroy storage.
    ///
    /// This deletes the underlying file that backs this storage.
    pub fn destroy(self) -> Result<(), (Self, io::Error)> {
        fs::remove_file(&self.path).map_err(|e| (self, e))
    }

    /// Gracefully shutdown storage.
    ///
    /// If this method completes successfully, all writes made to storage is
    /// guaranteed to be durably stored on disk.
    pub fn close(self) -> io::Result<()> {
        // Now we can attempt to clean up any unclean transaction shutdown.
        let file_len = self.file.metadata()?.len();
        let storage_len = self.len.load(Acquire);
        if file_len > storage_len {
            self.file.set_len(storage_len)?;
        }

        // Flush to disk.
        // This makes sure all writes have been durably stored to disk.
        self.flush()
    }
}

/// A read transaction against storage.
///
/// A read transaction provides snapshot isolation only, i.e, a read transaction
/// will view storage as it was when the transaction started.
pub struct ReadTxn<'a> {
    len: u64,
    next: u64,
    file: &'a File,
}

impl ReadTxn<'_> {
    /// Read next set of bytes from storage.
    ///
    /// It is okay for this operation to read lesser number of bytes
    /// than size of buffer. And this might happen even if there are
    /// more bytes to read from storage. To read exact size of buffer,
    /// use [`ReadTxn::read_exact`].
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to write bytes read from disk.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.remaining();
        let (dst, _) = buf.split_at_mut(min(remaining, buf.len()));

        // Read from disk only if there is something to read.
        let mut read = 0;
        if !dst.is_empty() {
            read = self.file.read_at(buf, self.next)?;
            self.next += read as u64;
        }

        Ok(read)
    }

    /// Read next set of bytes from storage.
    ///
    /// Reads exact number of bytes to fill the provided buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to write bytes read from disk.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        // Make sure there is enough remaining bytes to fill buffer.
        let remaining = self.remaining();
        if remaining < buf.len() {
            let kind = io::ErrorKind::UnexpectedEof;
            return Err(io::Error::new(kind, "Reached EOF without filling buffer"));
        }

        // Read bytes to fill the buffer completely.
        self.file.read_exact_at(buf, self.next)?;
        self.next += buf.len() as u64;
        Ok(())
    }

    /// Number of bytes remaining till end of storage.
    pub fn remaining(&self) -> usize {
        self.len.saturating_sub(self.next) as _
    }

    /// Commit the transaction.
    ///
    /// This does nothing but return the offset to next read on file.
    pub fn commit(self) -> u64 {
        self.next
    }
}

/// An append transaction against storage.
///
/// Changes made in the transaction are not visible to other transactions (across threads),
/// unless it is successfully committed via [`AppendTxn::commit`]. If transaction goes out
/// of scope without explicit commit then it is implicitly aborted.  When this happens, any
/// bytes appended will be rolled back. However, explicit abort via [`AppendTxn::abort`] is
/// still recommended so that you can handle errors.
pub struct AppendTxn<'a> {
    next: u64,
    start: u64,
    complete: bool,
    file: &'a File,
    len: &'a AtomicU64,
    lock: &'a AtomicBool,
}

impl AppendTxn<'_> {
    /// Append some bytes into storage.
    ///
    /// # Arguments
    ///
    /// * `buf` - Bytes to write into storage.
    pub fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all_at(buf, self.next)?;
        self.next += buf.len() as u64;
        Ok(())
    }

    /// Commit the transaction.
    ///
    /// Finalizes the transaction and commits any writes into storage.
    /// Returns the offset in file where next will will occur. That is
    /// also the size of storage at the end of this transaction.
    ///
    /// # Arguments
    ///
    /// * `flush` - Sync data to disk, flushing any intermediate buffers.
    pub fn commit(mut self, flush: bool) -> io::Result<u64> {
        // Flush writes to disk to guarantee durability.
        // This quite severely affects performance, so is optional.
        // We only perform this operation if there was some change made in transaction.
        if flush && self.start != self.next {
            self.file.sync_data()?;
        }

        // Release the changes made in this transaction to other threads.
        if self.start != self.next {
            self.len.store(self.next, Release);
        }

        // Mark transaction as explicitly completed.
        // This prevents implicit abort during drop.
        self.complete = true;

        // Return offset for next write.
        Ok(self.next)
    }

    /// Abort the transaction.
    ///
    /// This rolls back changes accumulated in the transaction. If this operation
    /// is successful, appends from this transaction will never be visible in other
    /// transaction. If it does error out, storage is in undefined state.
    ///
    /// Returns the offset in file where next will will occur. That is also the size
    /// of storage at the end of this transaction.
    pub fn abort(mut self) -> io::Result<u64> {
        // Undo changes made in this transaction by truncating the underlying file.
        // We only perform this operation if there was some change made in transaction.
        if self.next != self.start {
            self.file.set_len(self.start)?;
        }

        // Mark transaction as explicitly completed.
        // This prevents implicit abort during drop.
        self.complete = true;

        // Return offset for next write.
        Ok(self.start)
    }
}

impl Drop for AppendTxn<'_> {
    fn drop(&mut self) {
        // If the transaction was completed without being committed
        // it's implicitly aborted. If there were some (successful)
        // writes into storage, then we undo that now.
        if !self.complete && self.start != self.next {
            // There is no way to return an error back from destructor.
            // The best we can do is to attempt (unless panic). It it
            // okay, because new offset will not be visible to anyone.
            // It will be get clean up in next maintenance run.
            let _ = self.file.set_len(self.start);
        }

        // Finally release the lock.
        // Now the lock can be acquired by other threads.
        self.lock.store(false, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Error, Result};
    use std::{sync::atomic::AtomicUsize, thread};
    use tempfile::tempdir;

    const WRITERS: usize = 5;
    const READERS: usize = 5;
    const RECORD_SIZE: usize = 8;

    #[test]
    fn concurrency_tests() -> Result<()> {
        let dir = tempdir()?;

        // Create storage at a specified path.
        let path = dir.path().join("test.storage");
        let storage = Storage::create(path)?;

        // Writes to make against storage.
        let index = AtomicUsize::new(0);
        let appends: Vec<_> = (0..10000u64).map(|num| num.to_be_bytes()).collect();

        // Have multiple threads attempt to read and write from storage.
        thread::scope(|scope| {
            // Writers attempting to insert data into storage.
            for _ in 0..WRITERS {
                scope.spawn(|| {
                    loop {
                        // Create transaction to append data into storage.
                        if let Some(mut txn) = storage.append_txn() {
                            // Figure out next index to append into storage.
                            let next_index = index.fetch_add(1, Relaxed);
                            if next_index >= appends.len() {
                                break; // All writes are complete.
                            }

                            // Fetch next index to write into storage.
                            let data = &appends[next_index];
                            assert_eq!(RECORD_SIZE, data.len());

                            // Write data in index into storage.
                            txn.append(data)?;
                            txn.commit(false)?;
                        }
                    }

                    Ok::<_, Error>(())
                });
            }

            // Readers attempting to read data from storage.
            for _ in 0..READERS {
                scope.spawn(|| {
                    let mut index = 0;
                    loop {
                        // Figure out the next piece of data to read.
                        if index >= appends.len() {
                            break;
                        }

                        // Attempt to read data from storage.
                        let offset = (index * RECORD_SIZE) as u64;
                        if let Some(mut txn) = storage.read_txn(offset) {
                            // Make sure enough bytes are available for reads.
                            assert!(txn.remaining() >= RECORD_SIZE);

                            // Read data in offset.
                            let mut data = [0; RECORD_SIZE];
                            txn.read_exact(&mut data)?;

                            // Make sure contents of data is as expected.
                            assert_eq!(&data, &appends[index]);
                            index += 1;
                        }
                    }

                    Ok::<_, Error>(())
                });
            }
        });

        // Flush all writes to disk.
        storage.flush()?;

        // Final verification to make sure bytes in storage is correct.
        let mut txn = storage
            .read_txn(0)
            .expect("A read transaction should start successfully");

        // Read all the bytes from storage.
        let mut data = vec![0; appends.len() * RECORD_SIZE];
        txn.read_exact(&mut data)?;

        // Make sure bytes in disk is correct.
        let expected: Vec<_> = appends.iter().flat_map(|data| *data).collect();
        assert_eq!(expected, data);

        // Finally get rid of storage.
        storage.destroy().map_err(|(_, e)| e)?;
        Ok(())
    }
}
