# Arrow

[![Build Status][build-img]][build-url]
[![Documentation][doc-img]][doc-url]

[build-img]: https://github.com/sandesh-sanjeev/arrow/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/arrow/actions/workflows/ci.yml
[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/arrow/arrow/index.html

## Testing

```bash
# No features
$ cargo test
```

## Benchmarks

Generate the benchmark binaries.

```bash
$ cargo build --release --features benchmark
```

### Storage

Check binary for help.

```bash
$ ./target/release/bench_storage --help
CLI arguments to execute benchmarks for append only storage

Usage: bench_storage [OPTIONS]

Options:
      --readers <READERS>
          Number of readers concurrently reading [default: 8]
      --append-size <APPEND_SIZE>
          Size of bytes appended in one transaction [default: 1048576]
      --append-tick-ms <APPEND_TICK_MS>
          Amount of time in ms to sleep between appends [default: 100]
      --append-flush-ms <APPEND_FLUSH_MS>
          Amount of time in ms between flush to disk [default: 10000]
      --read-tick-ms <READ_TICK_MS>
          Amount of time in ms to sleep between reads [default: 100]
      --total-appends <TOTAL_APPENDS>
          Total number of appends performed against storage [default: 300]
  -h, --help
          Print help
```

Run benchmarks with options of your choice.

```bash
$ ./target/release/bench_storage --readers 256

Storage path: "/var/folders/r_/mqnj4tk93xq0dcy_7xmt7n_m0000gn/T/.tmpo339VT/bench.storage"
Writers: 10 MB/s
Readers: 256 | Avg/reader: 10 MB/s
```