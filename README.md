# Arrow

## Tests

Run unit and doc tests.

```bash
$ cargo test
```

### Miri

Run Miri interpreter with tests to check for undefined behavior.

```bash
# Install Miri on nightly rust
$ rustup +nightly component add miri

# Override workspace to nightly
$ rustup override set nightly

# Run miri on tests
$ cargo miri test

# Remove workspace override
$ rustup override remove
```
