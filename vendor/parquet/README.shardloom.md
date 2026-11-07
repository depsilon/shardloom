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

The November 1 adaptive-execution integration adds an explicit positive
next-batch row limit capped by the reader's original configured maximum. It
updates the retained read plan and propagates preallocation hints through
composite and leaf readers. Fragmented reads within one batch keep that hint;
page, dictionary and cache ownership and row-selection position are unchanged.
Changing the limit does not release already retained state or bound individual
variable-length values. The ordinary iterator keeps its existing behavior when
this optional control is unused. Engine-level controls exercise changing limits,
real primitive buffer sizes, selection masks/selectors, nested nulls and empty
inputs; complete integration acceptance is separate from this API.

The November 7 variable-tensor integration preserves inherited null masks on
required fixed-size-list fields beneath absent structs. The reader already
inserts null child components for those absent rows; retaining their list mask
keeps the components unobservable during recursive Arrow validation. Declared
field nullability and present values are unchanged. The original regression
writes and reads nullable structs containing required zero-width and positive-
width lists, checks complete values and schema, and validates every returned
Arrow array. Native tensor persistence exercises the same path.
