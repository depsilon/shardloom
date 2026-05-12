# RFC 0005: Vortex-Native File IO and Output Contract

## Status

Draft

## Summary

This RFC defines ShardLoom's Vortex-native file IO and output contract.

ShardLoom must treat Vortex as both a first-class input format and a first-class output format.
Reading Vortex should preserve metadata, statistics, encodings, layouts, and validity information
where possible. Writing Vortex should be the highest-fidelity persistence path.

## Context

ShardLoom's architecture depends on computing over encoded Vortex-native layouts. If ShardLoom reads
Vortex into decoded Arrow arrays as the default path, it loses its main advantage.

Likewise, if ShardLoom writes only Parquet or Arrow IPC, it loses the ability to preserve native
physical metadata for future incremental and encoded execution.

ShardLoom therefore needs a clear Vortex-native file IO contract.

## Goals

- Define Vortex as native input.
- Define Vortex as native output.
- Define metadata-first file inspection.
- Define segment-level and byte-range-aware reads.
- Define Vortex output as the highest-fidelity target.
- Define explicit unsupported behavior.
- Distinguish Vortex-native IO from Arrow compatibility.
- Distinguish Vortex-native output from compatibility exports.

## Non-goals

- Do not implement Rust code in this RFC.
- Do not define all Vortex upstream APIs.
- Do not vendor or copy Vortex implementation code.
- Do not add Spark.
- Do not add DataFusion.
- Do not make Arrow decoded arrays the default execution representation.
- Do not define Parquet, Iceberg, or Delta output in detail here.

## Decision

ShardLoom should define native Vortex IO around the following concepts:

- VortexFileRef.
- VortexOpenOptions.
- VortexFileMetadata.
- VortexSegmentDescriptor.
- VortexSegmentReader.
- VortexReadPlan.
- VortexWritePlan.
- VortexOutputWriter.
- VortexOutputCommit.
- VortexUnsupportedDiagnostic.

These concepts should keep Vortex-specific behavior isolated primarily in the Vortex crate while
exposing clean domain types to the planner and runtime.

## Detailed design

### VortexFileRef

A reference to a Vortex file or logical Vortex object.

It should eventually support:

- Local paths.
- Object-store URIs.
- File identity.
- Optional snapshot id.
- Optional manifest linkage.
- Optional expected version or checksum.

### VortexOpenOptions

Options for opening a Vortex file.

Possible options:

- Projection.
- Predicate hints.
- Metadata-only mode.
- Statistics requirement.
- Unsupported-feature policy.
- Object-store read policy.
- Cache policy.

Opening a file should not imply reading all data.

### VortexFileMetadata

Metadata discovered before reading data.

It should include:

- Schema.
- DTypes.
- Segment list.
- Encodings.
- Layouts.
- Statistics availability.
- Nullability.
- Row counts.
- Byte-range information where available.
- File version.
- Unsupported feature list.

### VortexSegmentDescriptor

A description of an encoded segment inside a Vortex file.

It should include:

- Segment id.
- Column membership.
- Row count.
- DType.
- Encoding.
- Layout.
- Statistics.
- Validity/null metadata.
- Byte ranges.
- Sorting or clustering hints where available.

### VortexReadPlan

A plan to read Vortex data.

A VortexReadPlan should specify:

- Files.
- Segments.
- Columns.
- Predicates.
- Required metadata.
- Required materialization.
- Expected execution state.

Execution state may include:

- Metadata-only.
- Pruned.
- Encoded.
- Partially decoded.
- Fully materialized.

### VortexSegmentReader

A reader that can read the minimum necessary data for selected segments.

It should support:

- Metadata reads.
- Segment reads.
- Column reads.
- Byte-range reads where available.
- Partial decode where necessary.
- Explicit unsupported errors.

### VortexWritePlan

A plan to write Vortex-native output.

It should specify:

- Target path.
- Temporary path.
- Schema.
- DTypes.
- Encoding/layout intent.
- Statistics to emit.
- Snapshot/manifest linkage.
- Commit expectations.

### VortexOutputWriter

A writer for native Vortex output.

It should preserve:

- DTypes.
- Nullability.
- Validity information.
- Statistics.
- Layout intent.
- Encoding intent.
- Segment boundaries where appropriate.

### VortexOutputCommit

A record that native Vortex output was written and committed.

It should include:

- Output files.
- Segment descriptors.
- Statistics emitted.
- Snapshot linkage if available.
- Commit id.
- Commit status.

## Required behavior

### Read behavior

ShardLoom should:

1. Inspect metadata before reading data.
2. Use projection to avoid unused columns.
3. Use statistics to prune segments.
4. Read byte ranges rather than full files where possible.
5. Preserve encoded representation until materialization is required.
6. Fail explicitly for unsupported Vortex features.

### Write behavior

ShardLoom should:

1. Treat Vortex output as native and highest-fidelity.
2. Preserve physical metadata where possible.
3. Emit statistics where possible.
4. Record metadata loss only if unavoidable.
5. Support future manifest/snapshot integration.
6. Fail explicitly for unsupported output schemas or encodings.

## Failure behavior

Unsupported behavior must produce deterministic diagnostics.

Examples:

- Unsupported Vortex file version.
- Unsupported encoding.
- Unsupported layout.
- Unsupported DType.
- Missing required statistics.
- Invalid metadata.
- Unsupported nested structure.
- Unsupported output schema.
- Failed write validation.
- Ambiguous commit state.

Failures must not trigger Spark, DataFusion, DuckDB, Polars, Velox, or another execution engine.

## Arrow interoperability boundary

Arrow interoperability is allowed for:

- Compatibility output.
- Python or FFI boundaries.
- Reference testing.
- Debugging.
- Some translation paths.

Arrow decoded arrays must not become the default Vortex execution representation.

If Vortex-to-Arrow conversion loses metadata, that loss should be explicit.

## Alternatives considered

### Decode Vortex into Arrow immediately

Rejected as default behavior.

This would erase the primary ShardLoom advantage.

### Use DataFusion's Vortex integration

Rejected for core execution.

ShardLoom may compare against DataFusion or learn from integrations, but it must not use DataFusion
as execution fallback.

### Write Parquet first

Rejected.

Parquet is a compatibility target. Vortex is the highest-fidelity native target.

## Risks

- Upstream Vortex APIs may evolve.
- Some Vortex encodings may not have ShardLoom-native kernels initially.
- Metadata preservation may require careful adapter design.
- Object-store byte-range behavior may be more complex than local files.
- Vortex output quality may depend on encoding decisions that require later RFCs.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Vortex is native input.
- Vortex is native output.
- Vortex output is highest-fidelity.
- File IO must be metadata-first.
- Segment-level planning is required.
- Unsupported Vortex behavior must fail explicitly.
- Arrow is compatibility/interoperability, not default execution.
- Spark and DataFusion fallback remain prohibited.

## Verification plan

Future implementation PRs should verify:

- Opening invalid Vortex references fails clearly.
- Metadata can be inspected before data reads.
- Projection avoids unused columns.
- Segment descriptors are available to planning.
- Unsupported Vortex features fail deterministically.
- Vortex output is represented in the API.
- Vortex output preserves metadata where possible.
- No Spark or DataFusion dependency is introduced.

## Open questions

- Which Vortex crate APIs should be used first?
- How should upstream API volatility be isolated?
- Should ShardLoom define its own Vortex adapter traits before depending deeply on Vortex internals?
- What is the minimum native Vortex output feature set for the first implementation?
- How should ShardLoom choose output encodings?
