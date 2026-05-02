# RFC 0020: Schema Evolution, Catalog Integration, and Table Compatibility

## Status

Draft

## Summary

This RFC defines ShardLoom's schema evolution, catalog integration, and table compatibility design.

ShardLoom is not a new lakehouse table format, but real adoption requires compatibility with evolving schemas, cataloged datasets, and table-format ecosystems such as Iceberg-compatible and Delta-compatible workflows.

## Context

ShardLoom must read and write datasets that evolve over time while preserving Vortex-native fidelity and explicit diagnostics for compatibility boundaries.

## Goals

- Define schema evolution compatibility levels.
- Define field identity and rename behavior expectations.
- Define catalog-facing schema contracts.
- Define read-time and write-time validation policy.
- Define metadata-loss and coercion diagnostics.
- Preserve Vortex-native output as highest fidelity target.

## Non-goals

- Do not implement a full catalog service.
- Do not redefine Iceberg or Delta table specs.
- Do not add fallback execution engines.

## Core principle

Schema evolution should be explicit, deterministic, and compatibility-scored. Unsupported schema transitions must fail with stable diagnostics rather than silently coercing to lossy behavior.

## Detailed design

### Compatibility levels

- NativeExact: full Vortex-native type/metadata fidelity.
- NativeCompatible: semantically equivalent with explicit metadata mapping.
- CompatibilityLossless: translatable to compatibility outputs without semantic loss.
- CompatibilityLossy: allowed only with explicit opt-in and metadata-loss report.
- Unsupported: deterministic failure.

### Evolution operations

Potential operations:

- Add nullable field.
- Add field with default.
- Drop field.
- Rename field (requires stable field id mapping).
- Widen numeric type.
- Tighten type/nullability (generally unsupported without explicit policy).
- Struct/list nested evolution.

### Catalog integration contract

Catalog adapters should expose:

- Stable table identity.
- Schema version and field identity mapping.
- Partition/spec metadata when relevant.
- Snapshot/version reference.
- Capability flags for supported evolution operations.

### Validation and diagnostics

Validation should check:

- Read schema vs plan schema compatibility.
- Write schema vs sink/output contract.
- Coercion policy compliance.
- Metadata preservation/loss reporting.
- No-fallback policy adherence.

Diagnostics should include stable codes, affected field paths, attempted operation, and suggested next step.

## Failure behavior

Unsupported evolution, unsafe coercion, or missing catalog capability must fail explicitly with deterministic diagnostics and no fallback execution.

## Alternatives considered

- Silent coercion for convenience: rejected.
- Compatibility-format-first schema model: rejected.
- Ad hoc per-connector evolution logic without central contract: rejected.
