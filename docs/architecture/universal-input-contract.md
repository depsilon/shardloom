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
    same direct-transient SQL path now preserves the structured-reader Arrow `RecordBatch`
    SourceState boundary until the explicit scalar-row expression-runtime materialization boundary,
    reporting `user_surface_runtime_scope=format_neutral_sql_python_runtime`,
    `format_specific_boundary_scope=read_ingest_and_write_only`,
    `format_specific_compute_path=false`, `source_state_columnar_preserved`,
    `source_state_record_batch_count`, `source_to_columnar_millis`,
    `source_state_runtime_consumption_layout`, and
    `source_state_scalar_runtime_materialization_required` so columnar ingress is not hidden inside
    generic parse time and is not misreported as per-format, Arrow, or Vortex-native query
    execution. The same gate admits scoped flat scalar local Parquet/Arrow IPC/Avro/ORC output for
    that SQL local-source smoke and reports deterministic sink blockers in default builds.
    Normal public workflows do not use this direct transient path as the execution middle: local
    compatibility inputs normalize through `SourceState -> vortex_ingest -> VortexPreparedState`
    before prepared/native Vortex execution, or fail closed when the required feature gate is absent.
  - When `vortex-ingest-smoke` is built with both `vortex-write` and `universal-format-io`, flat
    scalar Parquet/Arrow IPC/Avro/ORC inputs preserve an Arrow `RecordBatch` columnar SourceState
    through the prepare-once boundary and use upstream Vortex
    `ArrayRef::from_arrow(RecordBatch)` as the admitted array-build provider for non-empty flat
    batches before the scoped local `VortexPreparedState` writer. Reports emit
    `source_state_columnar_preserved`, `source_state_record_batch_count`,
    `source_to_columnar_millis`, `vortex_array_build_millis`,
    `vortex_array_build_provider_kind`, `vortex_array_build_provider_surface`,
    `vortex_array_build_strategy`, `vortex_array_build_input_layout`,
    `vortex_array_build_record_batch_count`, and
    `vortex_array_build_manual_scalar_copy_avoided` so ingest timing and provider attribution are
    not collapsed into generic scalar parse time.
    CSV/JSON/JSONL use the text SourceState adapter boundary and may carry public schema hints into
    Vortex preparation. JSONL/NDJSON remain distinct public input formats rather than being collapsed
    to generic JSON. All-null local text columns without explicit dtype default to nullable UTF-8,
    mixed integer/float text-source numeric families promote to float64, and selected nested JSON
    object/array values normalize as UTF-8 JSON payload strings for field-scan workflows. This is
    source normalization only: it does not claim recursive nested JSON query semantics or create a
    hidden Arrow-default execution model.
  - Traditional benchmark compatibility-import rows now use the same SourceState vocabulary for
    structured local inputs: Parquet/Arrow IPC/Avro/ORC rows report columnar preservation, record
    batch count, source-to-columnar timing, and the remaining traditional row-normalization boundary
    separately from Vortex array build/write timing.
  - Planned `PERF-DESIGN-6R-A` work narrows CSV/JSONL cold-ingest optimization to direct typed
    column builders for admitted local text-source shapes. That path should use SourceState scout
    evidence to classify schema, delimiters, nullability, projected fields, malformed rows, and
    coercion policy before appending values directly into typed buffers for Vortex preparation.
    Admitted rows must report typed-builder status, projected/full/skipped column counts, row
    materialization status, zero row assembly or zero source-row materialization where supported,
    source-to-Vortex handoff timing, correctness digest status,
    `fallback_attempted=false`, `external_engine_invoked=false`, and
    `external_parser_engine_invoked=false`. Unsupported CSV/JSONL shapes beyond the admitted source
    normalization contract must produce deterministic source-scout blockers; the planned path does
    not authorize hidden row-object assembly, decode-to-Arrow execution, broad JSON support, new
    source-format support, or external parser engine fallback.
  - Planned `PERF-DESIGN-6R-B` work makes projection a source-admission contract rather than a
    benchmark shortcut. Required fields must be derived from predicates, outputs, joins, grouping,
    ordering, certificates, diagnostics, and proof-tier needs before decode/handoff; skipped columns
    are valid only when the row records field masks, blocker posture, unchanged digests, and
    no-fallback evidence. Lazy external engines such as Polars may remain projection-efficiency
    baselines only; they are not ShardLoom execution providers.
  - `PERF-DESIGN-6R-C` keeps Parquet and Arrow IPC under an already-columnar source handoff
    contract. The local runtime/reporting path now emits `source_columnar_*` provider evidence for
    direct provider rows and row-boundary adapters, including input format, provider surface,
    projected mask, preserved/skipped column counts, materialized source rows, record batch count,
    null/validity posture, unsupported dtype reason, source-to-Vortex handoff timing,
    correctness-digest posture, and no-fallback/no-external-engine fields. Admitted Parquet/Arrow
    IPC direct provider rows are still scoped benchmark evidence until targeted artifacts are
    refreshed; Avro/ORC rows remain visible but outside the 6R-C timing claim scope. This does not
    authorize Polars/PyArrow/DuckDB execution fallback or lossy conversion.
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
  - Planned cold-lane follow-through extends SourceState with scout ingress/triage, Vortex
    source/split refs, capillary task refs, differential delta manifests, and copy-budget evidence
    only when those surfaces have deterministic blockers and no-fallback fields.
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
  - Planned differential preparation may update or overlay a prepared state only from a complete
    base/delta manifest, schema compatibility proof, changed-range evidence, Native I/O replay, and
    deterministic invalidation policy.
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
