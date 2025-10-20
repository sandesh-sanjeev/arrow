//! Append only storage backed by file on disk.

use crate::lock::MutGuard;
use std::{
    cmp::min,
    fs::{self, File, OpenOptions},
    io::{Error, ErrorKind, Result},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering::*},
};

/// An append only storage of bytes.
///
/// # Concurrency
///
/// A single writer can append bytes to storage at a time. To enforce this, append
/// operation requires caller to have a reference to a [`MutGuard`]. This is not fool
/// proof, because this data-structure does not own the lock for performance reasons.
/// The idea is to obtain a write lock for the entire ring buffer, but individual
/// components of the ring buffer.
///
/// Any number of readers can concurrently read from storage without any form of
/// synchronization or locking. Readers view storage as it was when a read operation
/// started, meaning readers can never conflict with writers.
///
/// However, note that there is currently no way to protect against mutable access across
/// processes. If/when that support arrives, it's going to advisory at best.
///
/// # Durability
///
/// Appends don't implicitly sync data to set with every append for performance reasons.
/// To make sure writes have actually made it to disk, explicitly call [`Storage::sync`].
/// Alternatively make all writes sync to disk via `O_SYNC`/`O_DSYNC` (not yet supported).
///
/// # Corruption
///
/// Regardless of what we do, it's possible for partial writes to exist on disk. This is
/// especially the case during I/O errors or system/process crashes. So it's highly recommended
/// to have a mechanism to detect corruption, for example using checksums.
///
/// And when you do detect corruption, you can either just throw errors or attempt to fix it.
/// The only mechanism available to fix corruption is by truncating storage to a length that
/// does not have corruption. Use [`Storage::truncate`] to trim off corrupted bytes.
///
/// It is not safe to truncate storage while there are concurrent readers. Thus, the operation
/// requires mutable reference to storage. The assumption is that truncation happens once during
/// process activation, so this is okay.
pub struct Storage {
    file: File,
    path: PathBuf,
    len: AtomicU64,
}

impl Storage {
    /// Create storage file in read-append mode.
    ///
    /// Returns an error if file already exists in path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;

        Ok(Self {
            file,
            len: AtomicU64::new(0),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Open storage file in append mode.
    ///
    /// Returns an error if file doesn't already exist in path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
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
            len: AtomicU64::new(len),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Returns the current size (in bytes) of storage.
    pub fn len(&self) -> u64 {
        self.len.load(Relaxed)
    }

    /// Returns true if storage has no bytes, false otherwise.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append some bytes into storage.
    ///
    /// # Arguments
    ///
    /// * `buf` - Bytes to write into storage.
    /// * `_guard` - Lock guard for exclusive mutable appends.
    pub fn append(&self, buf: &[u8], _guard: &MutGuard) -> Result<()> {
        // If there is nothing to append, return early.
        if buf.is_empty() {
            return Ok(());
        }

        // Write buffer into file.
        let len = self.len.load(Acquire);
        self.file.write_all_at(buf, len)?;

        // Update length of the file.
        let new_len = len + buf.len() as u64;
        self.len.store(new_len, Release);
        Ok(())
    }

    /// Read next set of bytes from storage.
    ///
    /// May return lesser than requested, if any bytes are written, they are
    /// guaranteed to be at the beginning of the buffer. Number of bytes written
    /// is return, if the read was successful. Use [`Self::read_exact_at`] to read
    /// exact number of bytes.
    ///
    /// If an error occurs, contents of the buffer is undefined.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to write bytes read from disk.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let dst = self.size_read_buf(offset, buf);

        // Read from the file only if we have to.
        if dst.is_empty() {
            return Ok(0);
        }

        // Read as many bytes as the kernel returns.
        self.file.read_at(dst, offset)
    }

    /// Read next set of bytes from storage.
    ///
    /// Returns an error if end of file is reached before buffer is filled. Use
    /// [`Self::read_at`] to read as many bytes as available.
    ///
    /// If an error occurs, contents of the buffer is undefined.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to write bytes read from disk.
    pub fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = buf.len();
        let dst = self.size_read_buf(offset, buf);

        // Make sure there is enough remaining bytes to fill buffer.
        if len != dst.len() {
            let kind = ErrorKind::UnexpectedEof;
            return Err(Error::new(kind, "EOF without filling buffer"));
        }

        // Read from the file only if we have to.
        if dst.is_empty() {
            return Ok(());
        }

        // Read bytes to fill the buffer completely.
        self.file.read_exact_at(dst, offset)
    }

    /// Flushes any intermediate buffers in between the disk,
    /// guaranteeing that writes have made it to disk.
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }

    /// Truncate storage to new length.
    ///
    /// Bytes will be removed from the end of storage.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of storage.
    pub fn truncate(&mut self, len: u64) -> Result<()> {
        // Only truncate if there are some bytes to truncate.
        let storage_len = self.len.load(Acquire);
        if len >= storage_len {
            return Ok(());
        }

        // Resize storage.
        // Because of the check above, guaranteed to only truncate.
        self.file.set_len(len)?;
        self.len.store(len, Release);
        Ok(())
    }

    /// Destroy storage.
    ///
    /// This deletes the underlying file that backs this storage.
    pub fn destroy(self) -> Result<()> {
        fs::remove_file(&self.path)
    }

    /// Gracefully shutdown storage.
    ///
    /// If this method completes successfully, all writes made to storage is
    /// guaranteed to be durably stored on disk.
    pub fn close(self) -> Result<()> {
        self.sync()
    }

    /// Size read buffer to make sure it does not exceed EOF.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset to start reads from.
    /// * `buf` - Buffer to write data read from storage.
    fn size_read_buf<'b>(&self, offset: u64, buf: &'b mut [u8]) -> &'b mut [u8] {
        // Buffer is empty, nothing to read.
        if buf.is_empty() {
            return buf;
        }

        // Read the current state of storage.
        let len = self.len.load(Acquire);

        // There is nothing to left to read, nothing to do.
        let remaining = len.saturating_sub(offset);
        if remaining == 0 {
            return &mut [];
        }

        // Only read until the end of snapshot.
        let remaining = remaining.try_into().unwrap_or(usize::MAX);
        let read_size = min(buf.len(), remaining);

        // Slice that can be safely read from file.
        &mut buf[..read_size]
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::lock::MutLock;
    use anyhow::{Result, anyhow};
    use tempfile::tempdir;

    // Exclusive lock for storage mutations.
    const LOCK: MutLock = MutLock::new();

    // Some random test data.
    const TEST_BUF: &[u8] = b"Batman is better than superman!";

    #[test]
    fn create_does_not_exist_returns_storage() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");

        // Should succeed because storage doesn't exist already.
        // Newly created storage should occupy no space.
        let storage = Storage::create(&path)?;
        assert!(storage.is_empty());

        Ok(storage.close()?)
    }

    #[test]
    fn create_already_exists_returns_error() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");

        // Create for the first time.
        let storage = Storage::create(&path)?;
        storage.close()?;

        // Try to create against should return an error.
        let Err(_) = Storage::create(&path) else {
            return Err(anyhow!("Storage should not be created again"));
        };

        Ok(())
    }

    #[test]
    fn append_empty_buf_noop() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append empty slice of bytes
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(b"", &guard)?,
        };

        // No bytes should exist in storage.
        assert!(storage.is_empty());

        Ok(storage.close()?)
    }

    #[test]
    fn append_buf_not_empty_updates_storage() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Size of storage should reflect the append.
        assert_eq!(TEST_BUF.len() as u64, storage.len());

        // Appended bytes should be readable.
        let mut read_buf = vec![0; TEST_BUF.len()];
        storage.read_exact_at(0, &mut read_buf)?;
        assert_eq!(TEST_BUF, read_buf.as_slice());

        Ok(storage.close()?)
    }

    #[test]
    fn size_read_buf_empty_buf_returns_empty_buf() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Empty buffer should not be resized.
        let mut read_buf = [];
        let sized_buf = storage.size_read_buf(0, &mut read_buf);
        assert!(sized_buf.is_empty());

        Ok(storage.close()?)
    }

    #[test]
    fn size_read_buf_nothing_remaining_empty_buf() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Read after exact end of file.
        let mut read_buf = vec![0; TEST_BUF.len()];
        let sized_buf = storage.size_read_buf(storage.len(), &mut read_buf);
        assert!(sized_buf.is_empty());

        // Read beyond the end of file.
        let sized_buf = storage.size_read_buf(storage.len() + 65, &mut read_buf);
        assert!(sized_buf.is_empty());

        Ok(storage.close()?)
    }

    #[test]
    fn size_read_buf_not_enough_remaining_shrinks_buf() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Should shrink buffer to fit remaining bytes.
        let mut read_buf = vec![0; TEST_BUF.len() + 10];
        let sized_buf = storage.size_read_buf(0, &mut read_buf);
        assert_eq!(TEST_BUF.len(), sized_buf.len());

        Ok(storage.close()?)
    }

    #[test]
    fn size_read_buf_enough_remaining_does_not_resize() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Request equal to remaining.
        let mut read_buf = vec![0; TEST_BUF.len()];
        let sized_buf = storage.size_read_buf(0, &mut read_buf);
        assert_eq!(TEST_BUF.len(), sized_buf.len());

        // Request less than remaining.
        let mut read_buf = vec![0; 3];
        let sized_buf = storage.size_read_buf(0, &mut read_buf);
        assert_eq!(3, sized_buf.len());

        Ok(storage.close()?)
    }

    #[test]
    fn read_at_empty_buf_noop() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Read buffer is empty.
        // This is okay, nothing should be returned.
        let mut read_buf = Vec::new();
        storage.read_at(0, &mut read_buf)?;

        Ok(storage.close()?)
    }

    #[test]
    fn read_at_enough_bytes_available_copies_bytes() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Buffer size is exactly equal to the number of bytes available.
        // However read_at is allowed to spuriously returned early, even if more bytes
        // are available. So, we'll need to read in a loop (till everything is read).
        let mut offset = 0;
        let mut read_buf = vec![0; TEST_BUF.len()];
        let mut buf = read_buf.as_mut_slice();
        while !buf.is_empty() {
            // Read as many bytes as storage returns.
            let read = storage.read_at(offset, &mut buf)?;

            // Consume all the bytes read from storage.
            offset += read as u64;
            buf = &mut buf[read..];
        }

        // Make sure bytes were copied correctly.
        assert_eq!(TEST_BUF, read_buf.as_slice());

        Ok(storage.close()?)
    }

    #[test]
    fn read_exact_at_empty_buf_noop() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Read buffer is empty.
        // This is okay regardless of number of bytes in storage.
        let mut read_buf = Vec::new();
        storage.read_exact_at(0, &mut read_buf)?;

        Ok(storage.close()?)
    }

    #[test]
    fn read_exact_at_not_enough_bytes_returns_error() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Buffer size is greater than total number of bytes available.
        let mut read_buf = vec![0; TEST_BUF.len() + 10];
        let Err(_) = storage.read_exact_at(0, &mut read_buf) else {
            return Err(anyhow!("Should fail, requested bytes are not available"));
        };

        Ok(storage.close()?)
    }

    #[test]
    fn read_exact_at_enough_bytes_available_copies_bytes() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Buffer size is exactly equal to the number of bytes available.
        let mut read_buf = vec![0; TEST_BUF.len()];
        storage.read_exact_at(0, &mut read_buf)?;
        assert_eq!(TEST_BUF, read_buf.as_slice());

        Ok(storage.close()?)
    }

    #[test]
    fn truncate_length_gte_storage_len_noop() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let mut storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Truncation length >= storage length should be no-op.
        storage.truncate(TEST_BUF.len() as u64)?;
        storage.truncate(TEST_BUF.len() as u64 + 100)?;

        // Size of storage should remain unchanged.
        assert_eq!(TEST_BUF.len() as u64, storage.len());

        Ok(storage.close()?)
    }

    #[test]
    fn truncate_length_smaller_than_storage_len_truncates() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let mut storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Truncation length < storage length should truncate storage.
        storage.truncate(TEST_BUF.len() as u64 - 10)?;

        // Size of storage should remain unchanged.
        assert_eq!(TEST_BUF.len() as u64 - 10, storage.len());

        // Make sure contents of storage is as expected.
        let mut read_buf = vec![0; TEST_BUF.len() - 10];
        storage.read_exact_at(0, &mut read_buf)?;
        assert_eq!(&TEST_BUF[..TEST_BUF.len() - 10], read_buf.as_slice());

        Ok(storage.close()?)
    }

    #[test]
    fn open_preserves_bytes_in_storage() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Append some bytes to storage.
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(TEST_BUF, &guard)?,
        };

        // Close storage and reopen.
        storage.close()?;
        let storage = Storage::open(&path)?;

        // Make sure all bytes are visible.
        let mut read_buf = vec![0; TEST_BUF.len()];
        storage.read_exact_at(0, &mut read_buf)?;
        assert_eq!(TEST_BUF, read_buf.as_slice());

        // Add more bytes and make sure they are visible too.
        let more_buf = b"blah";
        match LOCK.try_lock() {
            None => Err(anyhow!("Should obtain write lock"))?,
            Some(guard) => storage.append(more_buf, &guard)?,
        };

        // Close storage and reopen.
        storage.close()?;
        let storage = Storage::open(&path)?;

        // Make sure all bytes are visible.
        let mut read_buf = vec![0; more_buf.len()];
        storage.read_exact_at(TEST_BUF.len() as u64, &mut read_buf)?;
        assert_eq!(more_buf, read_buf.as_slice());

        Ok(storage.close()?)
    }

    #[test]
    fn destroy_nukes_storage() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Destroy storage.
        storage.destroy()?;

        // Once destroyed, storage should be able to be created
        // in the same path. Because nothing exists there.
        let storage = Storage::create(&path)?;

        Ok(storage.close()?)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod concurrency {
    use super::*;
    use crate::lock::MutLock;
    use anyhow::{Error, Result};
    use std::{sync::atomic::AtomicUsize, thread};
    use tempfile::tempdir;

    const WRITERS: usize = 3;
    const READERS: usize = 3;
    const RECORD_SIZE: usize = 16;

    #[test]
    fn parallel_reads_and_writes() -> Result<()> {
        let dir = tempdir()?;

        // Create storage at a specified path.
        let lock = MutLock::new();
        let path = dir.path().join("test.storage");
        let storage = Storage::create(&path)?;

        // Writes to make against storage.
        let index = AtomicUsize::new(0);
        let data: Vec<_> = (1..=100000).map(u128::to_be_bytes).collect();

        // Have multiple threads attempt to read and write from storage.
        thread::scope(|scope| {
            // Writers attempting to insert data into storage.
            for _ in 0..WRITERS {
                scope.spawn(|| {
                    loop {
                        // Obtain an exclusive write lock first.
                        let Some(guard) = lock.try_lock() else {
                            continue;
                        };

                        // Figure out next index to append into storage.
                        let next_index = index.fetch_add(1, Relaxed);
                        if next_index >= data.len() {
                            break;
                        }

                        // Append data into storage.
                        let buf = &data[next_index];
                        storage.append(buf, &guard)?;
                    }

                    Ok::<_, Error>(())
                });
            }

            // Readers attempting to read data from storage.
            for _ in 0..READERS {
                scope.spawn(|| {
                    let mut index = 0;
                    let mut buf = [0; RECORD_SIZE];
                    loop {
                        // Figure out the next piece of data to read.
                        if index >= data.len() {
                            break;
                        }

                        // Attempt to read data from storage.
                        // Requested bytes might exist in storage yet.
                        let offset = (index * RECORD_SIZE) as u64;
                        if storage.read_at(offset, &mut buf)? != buf.len() {
                            continue;
                        }

                        // Make sure contents of data is as expected.
                        assert_eq!(&buf, &data[index]);
                        index += 1;
                    }

                    Ok::<_, Error>(())
                });
            }
        });

        // Read all bytes from storage.
        let total_size: usize = data.iter().map(|slice| slice.len()).sum();
        let mut buf = vec![0; total_size];
        storage.read_exact_at(0, &mut buf)?;

        // Make sure those bytes are correct.
        for (i, record) in buf.chunks(RECORD_SIZE).enumerate() {
            assert_eq!(record, data[i]);
        }

        Ok(storage.close()?)
    }
}
