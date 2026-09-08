# Weighted complete-key COUNT native runs

This PERF-06 adapter extends RFC 0044's explicit caller-owned workspace contract.
It began as executable private native-run conversion, exact merging and hostile
input tests. The separately documented public weighted COUNT integration now
registers a narrow explicit-policy route for serial validation. Execution without
a policy stays unchanged; measured performance remains a separate gate.

The admitted values are nonnullable UTF8 keys, optionally paired with one
nonnullable identity integer key, and positive U64 contribution weights. The
integer retains its exact bits and signedness, including all original widths
after existing native typed accessors sign/zero extend in registers. Declared
group order may be integer/text or text/integer. Hashes and dictionary codes are
never persisted as key identity. Native dictionary domains are resolved before
the adapter's borrowed complete-key visit. Nullable keys, transformed keys,
mixed measures and joins remain outside this checkpoint.

Existing exact transfer points are `StringCountPartial::for_each_count`,
`StringCountPartitions::replay_and_release`, `CompoundPartial::visit`, and
`CompoundPartitions::replay_and_release`. A later coordinator must join all
workers, transfer the complete committed prefix and untouched deferred suffix,
check their weighted sum against admitted source rows, and only then release
the old epoch. A failed transfer is terminal: it cannot publish a prefix or
restart into the same run registry. No selected/top-K partition output is a
valid transfer source.

Pinned Vortex 0.85 provides Struct/Primitive/VarBin arrays, the native Flat push
writer, exact row-range readers and configured native execution contexts through
`QueryRunStore`. The adapter creates bounded offsets and value buffers itself:
`VarBinArray::try_new` validates them, and `Buffer::from(Vec)` retains the vector
owner without another payload copy. It avoids the unconstrained growth of a
generic string builder. Run readers retain their `QueryRunBlock` through typed
views and copy only bounded complete merge-head keys; no head can retain an
unbounded chain of earlier source blocks. The operator reuses a borrowed runtime
and one provider session/registry. No Arrow or external query engine is involved.

Admission separately reserves record capacity, UTF8 arena bytes, native block
conversion/merge scratch, run metadata and offset-plus-limit output. Each key
has a declared positive maximum of at most 64 KiB. That admitted maximum still
reserves worst-case keys, merge heads and conversion work before input arrives.
Each initial run derives its block rows from the actual maximum key in its bounded
buffer and a 64 KiB raw block target, capped at 1,024 rows. An admitted 64 KiB
maximum therefore does not force one-row blocks for ordinary short keys. A
single full-size key uses one row and may exceed the raw target by its fixed
typed fields; the up-front work reservation includes that overlap. Every owned
run descriptor retains its actual maximum key and exact block geometry. Merged
runs take the maximum of their inputs and derive a fresh conservative geometry.
Readers validate every key against the descriptor and verify the observed
maximum at EOF. Evidence reports minimum/maximum written block rows and maximum
observed run-key size; no-run results report zero written geometry. A key beyond
the admitted maximum fails before copying it. The
buffer flushes on either record or arena pressure. It coalesces identical
complete keys within each sorted buffer and checked-adds their weights before
writing, so repeated source contributions do not force duplicate native records.
Four-way level-balanced compaction retains cross-run duplicate records, making output row count known before
native writing. Only the final EOF merge coalesces all equal complete keys and
checks positive weights/overflow, then selects count descending and declared
complete key ascending. No local top-K, sketch or hash-only equality is used.

A dedicated weighted-COUNT workspace namespace is registered in the shared
store, preserving numeric-sort and exact-DISTINCT namespace and
diagnostic compatibility. Small inputs finish in memory without creating a
workspace. Once required, the store owns overlapping input/output disk quota,
checksums, held-generation reads and cooperative cleanup. Its work/run metadata
credits survive blocks and readers. Caller source-generation validation and a
parent query envelope through the returned result are mandatory future public
integration hooks; the private adapter alone is not a public certificate.

Evidence records source contributions, coalesced initial run rows, all native
written rows and actual written file bytes, and separates source arena copies, native UTF8 serialization copies,
merge-head copies and final selection copies. Primitive run construction,
provider work, filesystem caches and JSON/oracle allocations have their own
scope; these counters are not a claim of whole-process allocation coverage or
zero-copy execution. Cancellation is checked between bounded rows/blocks and
around sorting, not advertised as synchronous interruption of blocking I/O.

Acceptance checks complete values against independent Rust maps for both key
orders, signed/unsigned extrema, cross-domain dictionary visits, repeated/skewed
weights, a late global winner, ties and high offsets. It forces byte pressure,
many runs and balanced compaction, malformed order/signature/weight, corruption,
quota overlap, cancellation, oversize keys, overflow, terminal errors and zero
owned credits after result/error drop. No performance claim is made before the
parent-run paired measurements.
