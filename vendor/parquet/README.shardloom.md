# ShardLoom Parquet dependency pin

This is the unmodified `parquet` 59.3.1 registry source except for its manifest: normal and
development Zstandard dependencies are pinned to `=0.14.3`, and `publish = false` prevents
publishing this local provider. Source and preserved license files come from the SHA-256 verified
crates.io archive.

The archive's `Cargo.lock` is omitted because the repository root lockfile owns dependency
resolution. This vendor import adds no capabilities; it changes only dependency resolution.
