# Arrow

[![Build Status][build-img]][build-url]
[![Coverage][cov-img]][cov-url]
[![Documentation][doc-img]][doc-url]
[![License][license-img]][license-url]

[build-img]: https://github.com/sandesh-sanjeev/arrow/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/arrow/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/arrow/arrow/index.html

[cov-img]: https://coveralls.io/repos/github/sandesh-sanjeev/arrow/badge.svg?branch=master
[cov-url]: https://coveralls.io/github/sandesh-sanjeev/arrow?branch=master

[license-img]: https://img.shields.io/badge/License-MIT-yellow.svg
[license-url]: https://opensource.org/licenses/MIT

## Coverage

For best results use the `nightly` toolchain to gather coverage.

```bash
# Install framework to collect coverage data.
$ cargo install cargo-llvm-cov

# Run tests with coverage.
$ cargo llvm-cov test --lcov --output-path lcov.info
```
