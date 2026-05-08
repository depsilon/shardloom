# Universal Input Contract

## Purpose

`ShardLoom` supports universal inputs through adapter contracts and normalized planning metadata, not by compiling every reader by default. Active implementation status for input work lives in `docs/architecture/phased-execution-plan.md`; this document is the supporting contract reference.

## Core Principles

- `Vortex` is native input.
- Compatibility inputs are explicit and feature-gated later.
- Effectful inputs require explicit enablement.
- Input adapters normalize metadata into `ShardLoom` domain types.
- Input adapters do not imply fallback execution.
- Default build stays lightweight.
- No reader should silently decode or materialize by default.

## Input Family Map

- Native `Vortex`
  - Native bridge is represented through `shardloom-vortex` planning/reporting surfaces.
  - Approved IO remains narrow and feature-gated.
- Compatibility structured files
  - Parquet, Arrow IPC, CSV, JSON/NDJSON, Avro, and ORC require explicit adapter phases.
- Catalog/table refs
  - Catalog and table references require explicit metadata and security/governance contracts.
- Object-store manifests
  - Object-store reads require request budgets, range policy, retries, and no-fallback diagnostics.
- Unstructured data
  - Requires typed references, extracted-field contracts, and effect/security policy.
- API/LLM/embedding/vector effectful inputs
  - Requires explicit effect budgets, credentials, redaction, cost, and retry policy.
- In-memory/boundary inputs
  - Boundary inputs must declare representation state and materialization requirements.

## Contract Notes

- Input planning bridge
  - Universal input reports feed scan, explain, and estimate planning surfaces.
  - Bridge remains plan-only and side-effect-free.
  - It does not read files, inspect object stores, or execute external effects.
  - Compatibility and effectful inputs remain explicit contracts.
  - No fallback execution is introduced.
- Native Vortex input bridge
  - Native `Vortex` universal inputs can route through `shardloom-vortex` metadata planning.
  - Bridge remains plan-only and side-effect-free unless an explicitly feature-gated metadata-only path is enabled.
  - It does not scan, decode, materialize, write, or inspect object stores.
- Compatibility adapter bridge
  - Future adapters must emit source capability, pushdown proof, fidelity loss, materialization risk, and native I/O certificate evidence.
- Effectful input bridge
  - Future effectful inputs must participate in `EffectBudgetReport` and security/governance reporting.

## Symmetry With Output Contract

- Output planning tracks output target, fidelity, metadata loss, commit requirements, and materialization.
- Input planning tracks input source, fidelity, metadata availability, pushdown capability, materialization risk, and effect level.
- CG-19 unifies these through native work envelopes and native I/O certificates.

## Feature Gates

- `input-vortex`
- `input-vortex-file-io`
- `input-parquet`
- `input-arrow-ipc`
- `input-csv`
- `input-jsonl`
- `input-iceberg-compatible`
- `input-delta-compatible`
- `input-api`
- `input-llm`
- `input-embeddings`
- `input-vector`

## Guardrails

- Do not add readers from this document alone.
- Do not add object-store input from this document alone.
- Do not add external effects from this document alone.
- Do not add fallback engines.
- Do not compile all inputs by default.
- Promote implementation work into `phased-execution-plan.md` before changing behavior.
