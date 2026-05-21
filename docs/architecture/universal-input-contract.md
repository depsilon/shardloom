# Universal Input Contract

## Purpose

`ShardLoom` supports universal inputs through adapter contracts and normalized planning metadata,
not by compiling every reader by default. Active implementation status for input work lives in
`docs/architecture/phased-execution-plan.md`; this document is the supporting contract reference.

## Core Principles

- `Vortex` is native input.
- Compatibility inputs are explicit and feature-gated.
- Effectful inputs require explicit enablement.
- Input adapters normalize metadata into `ShardLoom` domain types.
- Input adapters do not imply fallback execution.
- Default build stays lightweight.
- No reader should silently decode or materialize by default.

## Alignment With CG-21, CG-22, and CG-23

- CG-21 user workflows use this contract for read/import UX, schema discovery,
  adapter maturity, materialization reports, data-quality gates, and source/sink
  diagnostics.
- CG-22 engine selection uses input boundedness, update mode, ordering,
  partitioning, source freshness, and change/delta metadata to decide whether a
  path can run in batch, live, hybrid, or auto mode.
- CG-23 remote APIs may expose input discovery, schema discovery, plan
  validation, and result references, but discovery endpoints remain
  side-effect-free unless a request explicitly authorizes probing or execution.
- None of the CG-21, CG-22, or CG-23 surfaces turns a compatibility input,
  remote system, catalog, event stream, or API call into fallback execution.

## Input Family Map

- Native `Vortex`
  - Native bridge is represented through `shardloom-vortex` planning/reporting surfaces.
  - Approved IO remains narrow and feature-gated.
- Compatibility structured files
  - CSV, JSON/NDJSON, Parquet, Arrow IPC, Avro, and ORC have feature-gated local benchmark bridge
    coverage.
  - CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet, Arrow IPC, Avro, and ORC
    have scoped direct-transient `sql-local-source-smoke` runtime coverage for local
    projection/filter/limit style workflows. Feature-gated structured readers report local
    SourceState read-plan evidence, requested/materialized columns, reader projection columns, and
    reader-level projection status when `shardloom-cli --features universal-format-io` is enabled;
    default builds report deterministic Parquet, Arrow IPC, Avro, or ORC adapter blockers. The
    same gate admits scoped flat scalar local Parquet/Arrow IPC/Avro/ORC output for that SQL
    local-source smoke and reports deterministic sink blockers in default builds.
  - Production-certified adapters remain separate phases and must emit full capability, pushdown,
    fidelity, and certificate evidence.
- Catalog/table refs
  - Iceberg-compatible metadata, Delta-compatible metadata, Hive-style partitions, snapshots,
    catalogs, and schema evolution require explicit metadata and security/governance contracts.
- Object-store manifests
  - Local filesystem, S3-compatible, Google Cloud Storage, Azure Blob/ADLS, and safe HTTP range
    reads require request budgets, range policy, retries, credentials policy where applicable, and
    no-fallback diagnostics.
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
  - Bridge remains plan-only and side-effect-free unless an explicitly feature-gated metadata-only
    path is enabled.
  - It does not scan, decode, materialize, write, or inspect object stores.
- Compatibility adapter bridge
  - Future adapters must emit source capability, pushdown proof, fidelity loss, materialization
    risk, and native I/O certificate evidence.
- Reusable SourceState bridge
  - `GAR-IOREUSE-1A` defines the first benchmark/report `SourceState` contract as a reusable,
    format-neutral input preparation artifact for source discovery metadata, schema/dtype metadata,
    format-specific adapter state, content fingerprinting, parse/decode planning, and source-state
    digest evidence.
  - Current benchmark row fields include `source_state_contract_schema_version`,
    `source_state_status`, `source_state_id`, `source_state_digest`, `source_format`,
    `source_location`, `source_fingerprint_kind`, `schema_digest`, `row_count_known`, `file_count`,
    `byte_size`, `partition_columns`, `compression`,
    `source_state_reuse_allowed`, `source_state_reuse_hit`, and `source_state_reuse_reason`.
  - SourceState reuse is preparation evidence only. It does not imply Vortex-native execution,
    output support, performance, object-store runtime, or table/lakehouse support.
- Reusable VortexPreparedState bridge
  - `GAR-IOREUSE-1B` defines the first benchmark/report `VortexPreparedState` contract as the
    prepared Vortex bridge between admitted source state and future execution/output plans.
  - Current benchmark row fields include `prepared_state_contract_schema_version`,
    `prepared_state_status`, `prepared_state_id`, `prepared_state_digest`,
    `prepared_state_source_state_id`, `vortex_artifact_ref`, `vortex_artifact_digest`,
    `prepared_state_reuse_allowed`, `prepared_state_reuse_hit`,
    `prepared_state_reuse_reason`, `preparation_included_in_timing`, and
    `vortex_prepare_millis`.
  - VortexPreparedState evidence records scoped local prepared artifact identity, digest,
    preparation timing separation, source-state linkage, and reuse posture. It does not imply
    output support, encoded-native coverage, performance, object-store runtime, or table/lakehouse
    support.
- Benchmark-only local compatibility-to-Vortex smoke bridge
  - `vortex-traditional-analytics-benchmark` enables a narrow local benchmark path that parses
    deterministic traditional analytics fixtures in CSV, JSONL/NDJSON, Parquet, Arrow IPC, Avro, or
    ORC form, writes local Vortex files, reopens and scans those files through upstream Vortex, and
    emits native I/O evidence fields.
  - The same feature-gated bridge can emit compatibility outputs in CSV, JSONL, Parquet, Arrow IPC,
    Avro, or ORC from Vortex-derived tables for roundtrip and troubleshooting evidence.
  - This bridge exists to make universal-I/O benchmark work visible while production adapter
    certification, SQL, DataFrame/API, object-store, catalog, table-format, and distributed adapter
    coverage remain deferred.
  - Resource sizing is automatic by default: applied parallelism is derived from local parallelism,
    and batch/partition sizing is derived from resource budget and source footprint unless memory or
    parallelism caps are provided.
  - Temporary traditional analytics operators currently consume Vortex-derived arrays after an
    explicit materialization boundary; this bridge is not mature encoded-native operator coverage.
- Effectful input bridge
  - Future effectful inputs must participate in the core `EffectBudgetReport` and
    security/governance reporting.
  - Default `EffectBudgetReport` creation remains no-probe and denies external effects, destructive
    effects, network egress, credential resolution, and fallback execution.

## Symmetry With Output Contract

- Output planning tracks output target, fidelity, metadata loss, commit requirements, and
  materialization.
- Input planning tracks input source, fidelity, metadata availability, pushdown capability,
  materialization risk, and effect level.
- CG-19 unifies these through native work envelopes and native I/O certificates.

## Feature Gates

- `input-vortex`
- `input-vortex-file-io`
- `input-csv`
- `input-jsonl`
- `input-parquet`
- `input-arrow-ipc`
- `input-avro`
- `input-orc`
- `input-iceberg-compatible`
- `input-delta-compatible`
- `input-local-filesystem`
- `input-s3-compatible`
- `input-gcs`
- `input-azure-blob-adls`
- `input-http-range`
- `input-local-catalog`
- `input-hive-compatible-catalog`
- `input-iceberg-rest-compatible-catalog`
- `input-glue-like-catalog`
- `input-nessie-like-catalog`
- `input-unstructured-text`
- `input-document`
- `input-image`
- `input-audio`
- `input-video`
- `input-binary-blob`
- `input-api`
- `input-llm`
- `input-embeddings`
- `input-vector`
- `vortex-traditional-analytics-benchmark` (benchmark-only local compatibility-file-to-Vortex smoke
  path)

## Guardrails

- Do not add readers from this document alone.
- Do not add object-store input from this document alone.
- Do not add external effects from this document alone.
- Do not add fallback engines.
- Do not compile all inputs by default.
- Promote implementation work into `phased-execution-plan.md` before changing behavior.
