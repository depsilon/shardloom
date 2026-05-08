# Lakehouse Value-Prop Compatibility Matrix

Status: Draft

## Summary

This document defines how ShardLoom maps common lakehouse table-system value props (Iceberg, Delta, Hudi, and similar ecosystems) into ShardLoom-native architecture.

ShardLoom remains a standalone execution engine with native Vortex input/output. Lakehouse table systems are treated as compatibility and table-management surfaces, not execution dependencies and not execution fallback engines.

Active phase status and CG closeout decisions live in `docs/architecture/phased-execution-plan.md`. This matrix is a reference contract for future CG-9, CG-10, CG-18, CG-19, and CG-20 work.

## Context

Teams expect lakehouse semantics such as snapshots, schema evolution, and incremental processing. These features are valuable, but ShardLoom must not couple execution to external engines or silently adopt semantics it cannot enforce.

This matrix provides a planning and diagnostics contract so contributors can reason about:

- Which value props are first-class in ShardLoom.
- Which are represented through Vortex-native metadata/manifests.
- Which are compatibility-target behaviors via adapters.
- Which are planned, deferred, or unsupported.

## Goals

- Keep Vortex-native input/output as the primary fidelity path.
- Define deterministic support status per value prop.
- Prevent silent behavior drift or implicit fallback execution.
- Ensure unsupported semantics fail explicitly.
- Ensure metadata loss is visible and machine-readable.
- Keep default build lightweight.

## Non-goals

- Implementing Iceberg/Delta/Hudi readers or writers.
- Implementing object-store IO.
- Implementing execution or planner/runtime behavior.
- Implementing commit protocols or transaction coordinators.
- Implementing fallback delegation to any external engine.

## Classification model

Each value prop can be tagged with one or more of the following states:

- `shardloom_native`: Implemented as a native ShardLoom concept.
- `vortex_native`: Implemented in native Vortex layouts/metadata as highest-fidelity representation.
- `compatibility_target`: Exposed through adapter/translation contracts with external table ecosystems.
- `planned`: Intended near-/mid-term design target, not yet implemented.
- `deferred`: Explicitly postponed pending other architecture work.
- `unsupported`: Not supported; requests must fail deterministically.

### Modeling guidance

When a capability is not fully native, ShardLoom should model it as:

1. **Canonical intent in ShardLoom domain types** (planner/diagnostic-level labels).
2. **Native fidelity path in Vortex metadata/layouts** where applicable.
3. **Compatibility mapping contract** for Iceberg/Delta/Hudi-like semantics.
4. **Deterministic diagnostics** when capability or fidelity cannot be preserved.

## Compatibility matrix

| Value prop | Classification | ShardLoom stance |
|---|---|---|
| Snapshots / time travel | `compatibility_target`; state: `planned` | Treat snapshot references as table-management compatibility inputs; planning can target a specific snapshot identity only when metadata contract is available. No implicit external log replay in execution core. |
| Schema evolution | `compatibility_target`; state: `planned` | Evolving schemas are represented natively with explicit field identity/version mapping, then translated to/from compatibility systems with loss reporting when semantics diverge. |
| Schema enforcement | `shardloom_native`; state: `planned` | Enforcement is a core contract: incompatible reads/writes or unsafe coercions must fail with stable diagnostics. |
| Hidden partitioning | `compatibility_target`, `planned` | Support partition transforms as compatibility metadata semantics; internal planning consumes normalized partition descriptors, not engine-specific hidden behavior. |
| Partition evolution | `compatibility_target`, `planned` | Versioned partition specs are compatibility concepts mapped into ShardLoom manifest descriptors when available. |
| Manifests / transaction logs | `shardloom_native`, `vortex_native`, `compatibility_target`, `planned` | ShardLoom manifests are native planning artifacts; compatibility logs are adapter-facing inputs/outputs and must not become execution dependencies. |
| Change data feed / incremental queries | `compatibility_target`, `planned` | CDC/incremental boundaries are modeled as explicit change streams or snapshot deltas when metadata is sufficient; otherwise fail with actionable unsupported diagnostics. |
| Upserts / deletes / tombstones | `shardloom_native`, `compatibility_target`, `planned` | Delete vectors/tombstones must be explicit semantics in planning and diagnostics; delete/tombstone semantics are never silently ignored. |
| Clustering / compaction | `compatibility_target`, `deferred` | Recognized as table-maintenance semantics. ShardLoom may consume resulting layout metadata, but compaction orchestration is deferred. |
| Indexing / data skipping | `shardloom_native`, `vortex_native`, `compatibility_target`, `planned` | Segment statistics and encoded metadata are native pruning surfaces; external index hints are compatibility metadata that require explicit trust and applicability checks. |
| Catalog compatibility | `compatibility_target`, `planned` | Catalogs are interoperability boundaries for discovery and table references, not execution engines. |
| Rollback / recovery | `compatibility_target`, `planned` | Transaction rollback/recovery semantics are adapter-level/table-management contracts. Core engine requires deterministic snapshot/version selection inputs. |
| Multi-engine interoperability | `compatibility_target`, `planned` | ShardLoom interoperates at metadata/data contract boundaries. Execution remains standalone with no Spark/DataFusion fallback. |
| Metadata-only planning | `shardloom_native`, `vortex_native`, `planned` | First-class design goal: answer from metadata and prune aggressively before decode/materialization. |
| Agent-readable diagnostics | `shardloom_native`, `planned` | Unsupported or lossy semantics must emit stable, machine-readable diagnostics including fallback status and metadata-loss details. |

## Required invariants

1. **Vortex stays native**
   - Vortex remains first-class native input and highest-fidelity output.

2. **No fallback execution**
   - Iceberg/Delta/Hudi semantics do not authorize delegation to Spark, DataFusion, DuckDB, Polars, Velox, or other engines.

3. **Explicit unsupported behavior**
   - Unsupported semantics fail with deterministic diagnostic codes and actionable remediation.

4. **Delete/tombstone safety**
   - Deletes, tombstones, and row-level mutation markers must never be dropped silently.

5. **Metadata loss visibility**
   - Translation/compatibility pathways must report dropped or downgraded metadata explicitly.

6. **Lightweight default build**
   - Compatibility modeling must not force heavyweight connectors or runtime dependencies in the default build.

## Diagnostic contract (design-level)

For lakehouse compatibility semantics, diagnostics should include at least:

- `code`: stable identifier (e.g., unsupported feature, lossy translation, missing metadata).
- `category`: capability, compatibility, correctness, or configuration.
- `fallback_execution_allowed`: always `false` by default.
- `fallback_attempted`: `false` unless a forbidden path was requested and rejected.
- `feature`: value prop name (e.g., `tombstone_semantics`, `snapshot_time_travel`).
- `status`: implementation-state classification at request time (`planned`, `deferred`, or `unsupported`; use `shardloom_native`/`vortex_native`/`compatibility_target` in `classification`).
- `classification`: one or more capability-kind tags from the classification model.
- `metadata_loss`: explicit description when fidelity cannot be preserved.
- `next_step`: actionable guidance (choose native Vortex path, enable explicit adapter, or simplify request).

## Alternatives considered

- **Direct dependence on one table format as internal substrate**: rejected; would compromise standalone identity and increase coupling.
- **Generic “best effort” compatibility without explicit failures**: rejected; risks silent correctness loss.
- **Treating compatibility systems as execution fallback**: rejected by architecture policy.

## Risks

- Over-modeling before implementation could create stale contracts.
- Under-modeling can lead to inconsistent adapter behavior later.
- Compatibility expectations may exceed the existing skeleton-phase scope.

Mitigation: keep statuses explicit (`planned`, `deferred`, `unsupported`) and require deterministic diagnostics.

## Acceptance criteria

- A canonical matrix exists for key lakehouse value props.
- Each value prop has explicit classification and stance.
- No section implies external-engine fallback execution.
- Delete/tombstone and metadata-loss requirements are explicit.
- Document is compatible with standalone, Vortex-native architecture.
- Future adapter implementations should emit machine-readable status for each applicable value prop after the phase plan authorizes that adapter work.
- Future phase-plan items should define mandatory versus optional compatibility semantics per adapter profile.

## Verification plan

- Documentation review against architecture constraints and no-fallback policy.
- Future implementation tasks must map new behavior to this matrix and update statuses explicitly.

## Open questions

- Which compatibility semantics should be mandatory versus optional per adapter profile?
- What is the minimal stable diagnostic code set for compatibility failures?
- How should ShardLoom-native manifest schema evolve to represent snapshot lineage and row-level mutations uniformly across compatibility inputs?
