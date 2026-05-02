# Vortex Skill Pack

This directory contains Vortex-specific operating procedures for ShardLoom.

ShardLoom's core advantage depends on treating Vortex as more than a file format. Vortex must be a native execution substrate.

## Core principle

ShardLoom should not simply read Vortex into decoded Arrow arrays and then execute generic columnar operations.

ShardLoom should preserve and use:

- Logical DTypes.
- Physical encodings.
- Layout metadata.
- Array statistics.
- Validity/null information.
- Segment and byte-range information.
- Native Vortex output metadata.
- Translation loss reports when exporting to lower-fidelity formats.

## Required reading by task type

- Vortex concepts: `vortex-concepts.md`
- File reading/writing: `vortex-file-io.md`
- Encodings and layouts: `vortex-encodings-layouts.md`
- Statistics and pruning: `vortex-stats-pruning.md`
- Vortex-native output: `vortex-native-output.md`
- Scan API and source/sink thinking: `vortex-scan-api.md`
- Arrow interoperability: `vortex-arrow-interop.md`
- Upstream compatibility and version tracking: `vortex-versioning-upstream.md`

## Non-negotiable ShardLoom constraints

- Vortex is a first-class native input target.
- Vortex is a first-class native output target.
- Vortex output is the highest-fidelity persistence target.
- Parquet, Arrow IPC, Iceberg-compatible, and Delta-compatible outputs are compatibility exports.
- Format translation is allowed.
- Benchmark comparison is allowed.
- Fallback execution is not allowed.
- Spark and DataFusion must not be used as execution fallbacks.

## Review questions

Every Vortex-related PR should answer:

- Does this preserve Vortex-native execution?
- Does this preserve Vortex-native output?
- Does this avoid unnecessary decode?
- Does this use metadata and statistics before reading or materializing data?
- Does this fail explicitly for unsupported Vortex behavior?
- Does this avoid Spark/DataFusion fallback?
- Does this document any metadata loss during translation?
