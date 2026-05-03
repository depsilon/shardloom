# Universal Input Contract

## Purpose
`ShardLoom` supports universal inputs through adapter contracts and normalized planning metadata, not by compiling every reader by default.

## Core principles
- `Vortex` is native input.
- Compatibility inputs are explicit and feature-gated later.
- Effectful inputs require explicit enablement.
- Input adapters normalize metadata into `ShardLoom` domain types.
- Input adapters do not imply fallback execution.
- Default build stays lightweight.
- No reader should silently decode/materialize by default.

## Input families
- Native `Vortex`
- Compatibility structured files
- Catalog/table refs
- Object-store manifests
- Unstructured data
- API/LLM/embedding/vector effectful inputs
- In-memory/boundary inputs

## Symmetry with output contract
Output planning tracks output target, fidelity, and metadata-loss.
Input planning tracks input source, fidelity, metadata availability, materialization risk, and effect level.

## Feature gates
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

## Do not do yet
- Do not add readers.
- Do not add object-store input.
- Do not add external effects.
- Do not add fallback engines.
- Do not compile all inputs by default.


## Input planning bridge
- Universal input reports now feed scan, explain, and estimate planning surfaces.
- The bridge is plan-only and side-effect-free.
- It does not read files, inspect object stores, or execute external effects.
- Compatibility and effectful inputs remain explicit contracts.
- `Vortex` remains native input.
- No fallback execution is introduced.

## Native Vortex input bridge

Native `Vortex` universal inputs can now be routed through the `shardloom-vortex` metadata path via a dedicated universal input bridge.

The bridge remains plan-only and side-effect-free. It may call the feature-gated metadata-only open contract when enabled, and it does not scan, decode, materialize, write, or inspect object stores.

Compatibility/effectful inputs remain explicit and unsupported/deferred in this `Vortex`-specific bridge. No fallback execution is introduced.

## Input gap alignment

- Universal inputs should remain adapter contracts, not eager dependency compilation.
- Effectful inputs require explicit enablement.
- Compatibility inputs are not fallback execution.
- Future API/LLM/embedding/vector inputs should participate in EffectBudgetReport.
