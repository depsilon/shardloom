# ShardLoom ORC dependency pin

This preserves the `orc-rust` 0.9.0 registry source with the following reviewed changes:

- Its Zstandard dependency is pinned to `=0.14.3`, and `publish = false` prevents
  publishing this local provider.
- The two `Decompressor::read` advances propagate codec errors as `std::io::Error`
  instead of panicking on malformed compressed input. Valid decoding is unchanged.
  Native adapter tests cover first/later block errors, complete compressed ORC
  readback, corrupted frames and a successful subsequent reopen.

The source comes from the SHA-256 verified crates.io archive. The Apache-2.0 license is preserved
from the repository's recorded upstream revision.

The archive's `Cargo.lock` is omitted because the repository root lockfile owns dependency
resolution. This patch adds no format capability or execution provider.
