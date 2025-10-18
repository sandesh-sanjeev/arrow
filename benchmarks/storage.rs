use anyhow::{Error, Result, anyhow};
use arrow::storage::Storage;
use clap::Parser;
use crossbeam_channel::tick;
use std::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

/// CLI arguments to execute benchmarks for append only storage.
#[derive(Parser, Debug)]
struct Args {
    /// Number of readers concurrently reading.
    #[arg(long, default_value = "8")]
    readers: usize,

    /// Size of bytes appended in one transaction.
    #[arg(long, default_value = "1048576")]
    append_size: usize,

    /// Amount of time in ms to sleep between appends.
    #[arg(long, default_value = "100")]
    append_tick_ms: u64,

    /// Amount of time in ms between flush to disk.
    #[arg(long, default_value = "10000")]
    append_flush_ms: u64,

    /// Amount of time in ms to sleep between reads.
    #[arg(long, default_value = "100")]
    read_tick_ms: u64,

    /// Total number of appends performed against storage.
    #[arg(long, default_value = "300")]
    total_appends: usize,
}

fn main() -> Result<()> {
    // Parse arguments.
    let args = Args::parse();

    // Parse durations.
    let append_tick_interval = Duration::from_millis(args.append_tick_ms);
    let append_flush_interval = Duration::from_millis(args.append_flush_ms);
    let read_tick_interval = Duration::from_millis(args.read_tick_ms);

    // Create temp directory for the tests.
    let dir = tempdir()?;

    // Create storage at a specified path.
    let path = dir.path().join("bench.storage");
    let storage = Storage::create(&path)?;
    println!("Storage path: {path:?}");

    // Run benchmark.
    let write_time = AtomicU64::new(0);
    let read_time = AtomicU64::new(0);
    thread::scope(|scope| {
        // Spawn writer.
        scope.spawn(|| {
            let start = Instant::now();
            let data = vec![6; args.append_size];
            let ticker = tick(append_tick_interval);

            let mut flushed = Instant::now();
            for _ in 0..args.total_appends {
                ticker.recv()?;

                let mut txn = storage
                    .append_txn()
                    .ok_or_else(|| anyhow!("Should get append transaction"))?;

                txn.append(&data)?;
                txn.commit(false)?;

                if flushed.elapsed() > append_flush_interval {
                    storage.flush()?;
                    flushed = Instant::now();
                }
            }

            // Register results.
            let time = start.elapsed();
            write_time.fetch_add(time.as_secs(), Relaxed);
            Ok::<_, Error>(())
        });

        // Spawn readers.
        for _ in 0..args.readers {
            scope.spawn(|| {
                let start = Instant::now();
                let ticker = tick(read_tick_interval);

                let mut batches = 0;
                let mut data = vec![0; args.append_size];
                while batches < args.total_appends {
                    let offset = batches * args.append_size;
                    if let Some(mut txn) = storage.read_txn(offset as _) {
                        txn.read_exact(&mut data)?;
                        batches += 1;
                    } else {
                        ticker.recv()?;
                    }
                }

                // Register results.
                let time = start.elapsed();
                read_time.fetch_add(time.as_secs(), Relaxed);
                Ok::<_, Error>(())
            });
        }
    });

    let write_mbps = rate(write_time.load(Relaxed), 1, &args);
    println!("Writers: {write_mbps} MB/s");

    let readers = args.readers;
    let read_mbps = rate(read_time.load(Relaxed), readers, &args);
    println!("Readers: {readers} | Avg/reader: {read_mbps} MB/s");
    Ok(())
}

fn rate(time: u64, workers: usize, args: &Args) -> u64 {
    let total_bytes = (args.total_appends * args.append_size) * workers;
    let throughput = match time {
        seconds if seconds == 0 => total_bytes as u64,
        seconds => total_bytes as u64 / seconds,
    };

    throughput / (1024 * 1024)
}
