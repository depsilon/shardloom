# Vortex Public API Inventory

## Purpose
This document records upstream `vortex` public APIs inspected for ShardLoom adapter work and defines narrow adapter boundaries that remain planning-only. It is explicitly not a Vortex IO implementation.

## Current dependency
- Crate: `vortex`
- Version: `0.70`
- License: Apache-2.0 (per dependency review)
- ShardLoom crate using it: `shardloom-vortex`
- Actual Vortex IO implemented: no
- Fallback execution introduced: no

## Public API discovery method
- Inspected dependency linkage and version via `shardloom-vortex/Cargo.toml` and `cargo tree -p shardloom-vortex`.
- Used compiler-safe reference checks in `shardloom-vortex` without invoking runtime IO APIs.
- Reviewed local readiness and dependency review docs.
- Documented only names/status; no copied upstream implementation code.

## Candidate API areas
### DType / logical type APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability for first adapter work: partially acceptable (name-based placeholder).
- Adapter support: planned.
- Risks: upstream typed API names may shift; avoid guessing.

### Array APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: coupling to internal array APIs.

### Encoding APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability: partially acceptable for placeholders.
- Adapter support: planned.
- Risks: encoding taxonomy drift.

### Layout APIs
- Public API names discovered: not confirmed yet.
- Use now: yes, via temporary name-based mapping only.
- Stability: partially acceptable for placeholders.
- Adapter support: planned.
- Risks: layout labels may change.

### Statistics APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred/planned.
- Risks: stats exactness/typing details unclear.

### File/open APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: would imply IO implementation scope.

### Scan/source APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: accidental execution coupling.

### Write/sink APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: deferred.
- Risks: write semantics and metadata fidelity unknown.

### Arrow interoperability APIs
- Public API names discovered: not confirmed yet.
- Use now: no.
- Stability: not confirmed yet.
- Adapter support: unsupported for default path.
- Risks: decode-to-Arrow drift as implicit fallback.

## Adapter mapping plan
- Vortex DType -> `shardloom_core::LogicalDType`
- Vortex encoding/layout -> `shardloom_core::EncodingKind` / `shardloom_core::LayoutKind`
- Vortex statistics -> `shardloom_core::SegmentStats`
- Vortex file metadata -> `shardloom_vortex::VortexFileMetadata`
- Vortex segment metadata -> `shardloom_vortex::VortexSegmentDescriptor`
- Vortex output capability -> ShardLoom native Vortex output planning (`VortexWritePlan`)

## Do not do
- Do not implement real IO yet.
- Do not default to decode-to-Arrow.
- Do not add DataFusion/Spark/DuckDB/Polars/Velox helpers.
- Do not copy upstream implementation code.
- Do not over-couple to private/internal APIs.

## Next milestone
Implement typed DType adapter mapping only if upstream public DType APIs are clearly confirmed and compile-safe; otherwise keep ShardLoom-local placeholder mapping until API stability is better understood.
