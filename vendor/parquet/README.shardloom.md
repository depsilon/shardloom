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

The October 31 metadata-admission integration adds an optional caller-owned
reservation callback. Footer preflight borrows encoded bytes without allocating
row metadata. Parsing reserves workspace for the current row group plus the
provider-measured retained heap of preceding groups, instead of forcing callers
to reserve a multiplier of the entire footer for its full lifetime. The global
schema/file-metadata parsing allowance remains held through Arrow schema
construction. Input bytes have separate ownership. This is reservation accounting,
not an allocator or process-RSS limit; all parsing/statistics semantics are
unchanged when no callback is configured. The implementation is original and
does not add a dependency or query-engine integration.
