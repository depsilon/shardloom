# ShardLoom Parquet dependency pin

This is the `parquet` 59.3.1 registry source with the following local corrections. Normal and
development Zstandard dependencies are pinned to `=0.14.3`, and `publish = false` prevents
publishing this local provider. Source and preserved license files come from the SHA-256 verified
crates.io archive.

The archive's `Cargo.lock` is omitted because the repository root lockfile owns dependency
resolution. The original import changes only dependency resolution.

The October 27 temporal work also changes the Arrow-schema hint merge for
timestamps. When a hint's unit differs from the stored Parquet logical unit,
the reader keeps the stored unit and preserves the hint's presentation timezone.
It never reinterprets the integer payload in the hinted unit. INT96 retains its
existing conversion behavior. Tests cover all stored/hinted unit pairs and the
INT96 distinction. This is an original, narrow metadata-fidelity correction.
