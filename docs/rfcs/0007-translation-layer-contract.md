# RFC 0007: Translation Layer Contract

## Status

Draft

## Summary

This RFC defines ShardLoom's columnar translation layer contract.

ShardLoom must be Vortex-native internally and for highest-fidelity output, while still supporting practical compatibility exports to formats such as Parquet, Arrow IPC, Iceberg-compatible files, and Delta-compatible files.

The translation layer is not fallback execution. It is output compatibility.

## Context

Adoption requires ShardLoom to work with existing data ecosystems.

Users may need to write results to:

- Vortex.
- Parquet.
- Arrow IPC.
- Iceberg-compatible files.
- Delta-compatible files.
- JSONL or CSV utility outputs in the future.

However, ShardLoom's native execution advantages depend on preserving Vortex-specific metadata, encodings, layouts, statistics, and validity information.

Therefore, the translation layer must explicitly distinguish high-fidelity native output from compatibility exports that may lose information.

## Goals

- Define Vortex as highest-fidelity output.
- Define compatibility export targets.
- Define translation reports.
- Define metadata preservation and degradation.
- Define output target contracts.
- Define schema compatibility behavior.
- Define unsupported output behavior.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement Rust code in this RFC.
- Do not define full Parquet writer behavior.
- Do not define full Iceberg or Delta transaction semantics.
- Do not add Spark.
- Do not add DataFusion.
- Do not use external engines to write output.
- Do not make compatibility exports equivalent to native Vortex output.

## Decision

ShardLoom should define a translation layer with these concepts:

- NativeResultStream.
- OutputTarget.
- ColumnarSink.
- TranslationPlan.
- TranslationReport.
- FidelityLevel.
- MetadataPreservation.
- UnsupportedOutputDiagnostic.

The translation layer should allow ShardLoom to produce ecosystem-compatible outputs while making metadata loss explicit.

## Core concepts

### NativeResultStream

A ShardLoom-native stream of results.

It may preserve:

- Vortex-native segment boundaries.
- Selection vectors.
- Encoded buffers.
- DTypes.
- Statistics.
- Layout hints.
- Materialization state.

Not every output target can preserve all of this information.

### OutputTarget

A requested output format or destination.

Initial conceptual targets:

- Vortex.
- Arrow IPC.
- Parquet.
- Iceberg-compatible files.
- Delta-compatible files.

Future targets may include:

- JSONL.
- CSV.
- Arrow Flight.
- Database sinks.
- Foundry-compatible dataset exports.

### ColumnarSink

A sink that accepts native or materialized columnar output.

A ColumnarSink should declare:

- Supported schemas.
- Supported DTypes.
- Supported nullability.
- Supported metadata.
- Supported streaming behavior.
- Required materialization.
- Commit behavior.
- Unsupported features.

### TranslationPlan

A plan to convert NativeResultStream into an OutputTarget.

It should specify:

- Required materialization.
- Metadata preserved.
- Metadata degraded.
- Metadata lost.
- Statistics preserved.
- Statistics recomputed.
- Encoding/layout preservation.
- Commit steps.
- Failure points.

### TranslationReport

A report describing what happened during translation.

It should include:

- Source representation.
- Target format.
- Preserved metadata.
- Degraded metadata.
- Dropped metadata.
- Required materialization.
- Unsupported features.
- Output files.
- Commit status.

### FidelityLevel

A classification of how much native information an output preserves.

Suggested levels:

- NativeFullFidelity: Vortex output preserving native metadata.
- NativePartialFidelity: Vortex output with documented limitations.
- CompatibilityHighFidelity: non-Vortex output preserving most logical information.
- CompatibilityLossyPhysical: non-Vortex output preserving values but losing physical metadata.
- Unsupported.

Vortex should normally be the only NativeFullFidelity target.

## Output target expectations

### Vortex

Vortex is native and highest-fidelity.

Expected preservation:

- Logical DTypes.
- Nullability.
- Validity.
- Statistics where possible.
- Layout hints where possible.
- Encoding intent where possible.
- Segment boundaries where appropriate.
- Snapshot/manifest linkage when available.

### Arrow IPC

Arrow IPC is a compatibility/export target.

It may preserve:

- Logical schema.
- Values.
- Nullability.

It may lose:

- Vortex encoding details.
- Vortex layout metadata.
- Segment statistics.
- Snapshot/manifest linkage.
- Some physical optimization hints.

### Parquet

Parquet is a compatibility/export target.

It may preserve:

- Logical schema.
- Values.
- Nullability.
- Some row-group/page statistics.
- Some dictionary behavior depending on writer.

It may lose:

- Vortex-specific encodings.
- Vortex layout details.
- Segment-level execution metadata.
- Some statistics and physical hints.

### Iceberg-compatible files

Iceberg-compatible output is a compatibility target.

ShardLoom may write files and metadata compatible with an Iceberg-style workflow in the future, but this RFC does not make ShardLoom an Iceberg table engine.

### Delta-compatible files

Delta-compatible output is a compatibility target.

ShardLoom may write files and metadata compatible with a Delta-style workflow in the future, but this RFC does not make ShardLoom a Delta transaction engine.

## Rules

- Translation is not execution fallback.
- Translation must not require Spark or DataFusion.
- Vortex output must remain available as native output.
- Metadata loss must be explicit where possible.
- Unsupported output features must fail deterministically.
- Compatibility exports must not weaken ShardLoom's native model.
- Output code must not silently materialize everything without reporting that materialization occurred.

## Failure behavior

Unsupported output behavior must fail explicitly.

Examples:

- Unsupported schema.
- Unsupported DType.
- Unsupported nested structure.
- Unsupported nullability.
- Unsupported metadata preservation requirement.
- Unsupported commit mode.
- Target path conflict.
- Ambiguous partial write.
- Unsupported fidelity requirement.

Failures must not trigger external engine fallback.

## Alternatives considered

### Only output Vortex

Rejected.

This would limit adoption.

### Prioritize Parquet output over Vortex

Rejected.

Vortex is the highest-fidelity target.

### Use Spark or DataFusion for output writing

Rejected.

This violates standalone execution.

### Treat all outputs as equal

Rejected.

Compatibility outputs may lose physical metadata and must not be represented as equivalent to native Vortex output.

## Risks

- Translation reports may be cumbersome.
- Some metadata loss may be difficult to describe.
- Iceberg/Delta compatibility may require careful future design.
- Writers may require materialization that harms performance.
- Users may misinterpret compatibility outputs as equivalent to native Vortex output.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Vortex output is native and highest-fidelity.
- Other output formats are compatibility exports.
- Translation is not fallback execution.
- Metadata preservation/degradation should be reported.
- Unsupported outputs fail explicitly.
- Spark/DataFusion are not used for writing outputs.

## Verification plan

Future implementation PRs should test:

- Vortex output target exists.
- Arrow IPC output target reports metadata loss.
- Parquet output target reports metadata loss.
- Unsupported schema diagnostics.
- Materialization requirements.
- Empty output.
- Nullable columns.
- Deterministic failures.
- No external engine fallback.

## Open questions

- What should the first implemented compatibility output be after Vortex?
- What fields should TranslationReport expose publicly?
- Should users be able to require minimum fidelity?
- How should Iceberg/Delta compatibility be scoped initially?
- How should output commits integrate with RFC 0004 snapshots and manifests?
