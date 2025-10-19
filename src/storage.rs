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
mod tests {
    use super::*;
    use crate::lock::MutLock;
    use anyhow::{Error, Result};
    use std::{sync::atomic::AtomicUsize, thread};
    use tempfile::tempdir;

    const WRITERS: usize = 3;
    const READERS: usize = 3;
    const RECORD_SIZE: usize = 16;

    #[test]
    fn concurrent_reads_writes() -> Result<()> {
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
                        // Obtain an exclusive write lock.
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

        Ok(storage.close()?)
    }
}
