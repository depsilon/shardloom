# Native result composition

Status: implementation in progress after the ownership/preparation unit in PR #1455.
This prerequisite belongs to the existing native runtime completion plan and
PERF-02/07/10/11. It does not complete general joins or public operator parity.

Owned results retain an authoritative native dtype even when no data arrays are
returned. Every batch must match it and the checked total row count. Completed
native and compatibility sinks consume that schema, including empty output.

Vortex-first decision: `use_vortex_native_provider`. Pinned Vortex 0.85 exposes
native arrays, Chunked/Struct layouts, Flat segment serialization and VortexFile
scans. Its production VortexFile reader is constructed from footer/layout/segment
sources; it does not accept injection of an arbitrary owned array reader. Reuse
ShardLoom's existing MemoryFileGeneration and segment builder so downstream
operators keep the same Vortex scan and optimized aggregate lowering boundary.

Add a bounded owned-batch intake alongside the existing typed memory intake.
Retain typed native columns and chunk references, without row or Arrow conversion.
Normalize nullable root-Struct validity into logical field validity when building
the tabular nonnullable Struct root. Native column values and their nulls remain
unchanged. Explicit bounds cover rows, fields, batch references, segments,
serialized bytes and metadata. Existing typed-intake and JSON defaults remain.

Native serialization is an explicit copy boundary. Sliced dictionaries and
VarBinView arrays can retain whole backing domains; row count alone does not
bound the serialized volume. Deny serialized amplification before publishing an
immutable generation. Retain input reservations while serialization and assembled
segments overlap, reserve adapter reference vectors before allocation, and keep
untracked upstream serializer/layout internals explicit in the evidence.

Source-based aggregate preparation must reuse current lowering, physical-key
proofs, exact partitions, weighted reducers and owned finalizers. Bind a stable
opaque memory URI to the immutable generation; do not label it as a filesystem
source. Certificates identify memory segments, construction serialization, native
scan work and zero source-file opens. Repeated calls own fresh aggregate state.

Composed operators borrow one native execution context. Reject foreign sessions
before work; only the outer operation admits, drains and increments completed-call
counters. Producer/provider jobs must join before the next stage starts its own
workers. Generation construction does not count as query execution. Public APIs
admit ordinary calls; borrowing entry points remain internal.

Acceptance covers typed empty result/source/aggregate/sink, multiple batches over
65,536 rows, nullable and mixed-width fields, dictionary and sliced-text
amplification, explicit bounds and partial-build cancellation, source lifetime,
foreign contexts, memory provenance, and P1 composition without nested admission.
File aggregates retain their existing physical choices and Full43 values. General
joins and further prepared/owned/public families follow this source contract.
