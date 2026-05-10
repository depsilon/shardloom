# Canonical Terminology

## Purpose

ShardLoom intentionally keeps related concepts at different layers (planning, execution, streaming boundaries, translation, and adapters). This document defines canonical meanings so terminology stays consistent without collapsing useful layer boundaries too early.

This document owns vocabulary definitions, not phase status. Active implementation status and queue placement live in `docs/architecture/phased-execution-plan.md`.

## Glossary ownership and index

This file is the authoritative ShardLoom glossary and concept index. It should stay concise enough to scan and link outward to RFCs or architecture references for deep contracts.

- **Entry-point summary**: `README.md` keeps only the short core-concepts doorway and links here.
- **External lessons**: `docs/architecture/systems-learning-map.md` records technique transfer from Vortex, Spark, DataFusion, Modal's GPU glossary, and other systems; it does not own ShardLoom vocabulary definitions.
- **Deep contracts**: RFCs define field-level contracts, acceptance criteria, non-goals, and verification plans.
- **Active status**: `docs/architecture/phased-execution-plan.md` remains the only mutable source of truth for current status, queue placement, completed phase ledger, and CG closeout state.

Concept groups:

- **Native execution and work avoidance**: `metadata_only`, `segment_pruning`, `zero_decode`, `partial_decode`, `late_materialization`, `full_materialization`, `DataWorkLevel`, `ExecutionState`.
- **I/O, representation, and translation**: `native_vortex_input`, `native_vortex_output`, `compatibility_output`, `foreign_encoded`, `universal native I/O envelope`, `native work envelope`, `native I/O certificate`.
- **Materialization and fidelity**: `MaterializationPolicy`, `MaterializationRequirement`, `MaterializationBoundary`, `FidelityLevel`, `VortexOutputFidelity`, `metadata_loss`, `fidelity_loss`.
- **Planning, diagnostics, and provenance**: `pushdown proof`, `residual expression`, `lowering provenance`, `portability loss`, `intermediate artifact`, `layout health report`, `compaction planning report`, `object-store range planning report`, `object-store request coalescing report`, `object-store distributed scheduling report`, `object-store checkpoint/retry report`, `object-store commit protocol report`, `fallback_attempted`, `unsupported`.
- **Capability and certification**: `capability certification surface`, `workload constitution`, `operator certification`, `function certification`, `SQL coverage tier`, `adapter maturity level`, `semantic compatibility profile`, `migration compatibility report`, `delete/tombstone compatibility report`, `table compatibility aggregation report`, `CDC incremental planning report`, `best-choice scorecard`.
- **User capability surfaces**: `data/ETL capability report`, `Python surface report`, `unstructured media capability report`, `universal adapter catalog`, `API surface certification`, `observability certification`, `deployment readiness report`, `extension capability report`, `security governance report`.
- **Agent/context capability**: `functional context scope`, `evidence routing`, `context structure preservation`, `stateful certificate history`.
- **agent contract pack**: machine-readable inventory of agent-safe command surfaces, schemas, recommended inspection order, no-probe defaults, effect defaults, fallback status, and JSON authority.

Primary governing references:

- No-fallback and Vortex I/O: `docs/rfcs/0002-no-fallback-and-vortex-io.md`
- Diagnostics and capability discovery: `docs/rfcs/0012-diagnostics-explain-estimate-capabilities.md`
- Streaming, zero-copy, zero-decode, and boundaries: `docs/rfcs/0013-streaming-zero-copy-boundary-interoperability.md`
- Universal native I/O envelope: `docs/rfcs/0031-universal-native-io-envelope.md`
- World-class capability surface: `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`

## Core principles

- Keep layer-specific types when they model different decisions.
- Add mapping helpers instead of prematurely merging types.
- Use "native Vortex" for highest-fidelity input/output.
- Use "compatibility output" for Parquet/Arrow/Iceberg/Delta/JSONL/CSV exports.
- Use "fallback execution" only for prohibited delegation to another engine.
- Use "translation" for output conversion, not execution.
- Use "zero-decode" for Vortex-native encoded execution.
- Use "zero-copy" for boundary/interoperability sharing.
- Use "materialization" only when values/rows/columns become concrete for an operator/sink/boundary.

## Materialization family

- `MaterializationPolicy`: user/planner intent from encoded execution layer.
- `MaterializationRequirement`: translation/output requirement from a sink or target.
- `MaterializationBoundary`: streaming/runtime boundary where materialization becomes required.

These remain separate because they represent intent, contractual requirements, and runtime boundary points at different layers. Mapping helpers should exist between these concepts where needed.

## Fidelity family

- `FidelityLevel`: canonical core output fidelity concept.
- `VortexOutputFidelity`: Vortex adapter-local fidelity concept.

`VortexOutputFidelity` should map into `FidelityLevel`. Vortex native full fidelity corresponds to `FidelityLevel::NativeFullFidelity`.

## Execution/work state family

- `ExecutionState`: canonical plan/execution-state label.
- `DataWorkLevel`: streaming/data-work ranking where lower rank means less work.

`DataWorkLevel` is an optimization/work ranking, while `ExecutionState` is a descriptive execution state. Streaming code can map `DataWorkLevel` into `ExecutionState` when needed.

## Dataset/output format family

- `DatasetFormat`: input/reference format identification.
- `OutputTargetKind`: requested output target.

`DatasetFormat` can map into `OutputTargetKind`, but they remain separate because input references and output contracts represent different boundaries.

## Memory/resource family

- `ResourceBudget`: runtime task-level limits.
- `AdaptiveSizingPolicy`: task sizing policy.
- `BoundedMemoryPolicy`: streaming bounded-memory requirement.
- `MemoryBudget`: memory pool/spill/OOM budget.

These concepts remain separate but should expose clear summaries and mapping helpers where useful.

## Agent-facing vocabulary

Preferred terms:

- `native_vortex_input`
- `native_vortex_output`
- `compatibility_output`
- `fallback_execution_allowed`
- `fallback_attempted`
- `metadata_only`
- `segment_pruning`
- `zero_decode`
- `zero_copy_boundary`
- `partial_decode`
- `late_materialization`
- `full_materialization`
- `spill_required`
- `unsupported`

## Do not use

Discouraged terms:

- "fallback" when meaning "translation"
- "Arrow-native" for internal execution
- "Spark-compatible fallback"
- "free execution" without benchmark/cost context
- "zero-copy" when data is actually decoded
- "native output" when target is compatibility export

## Cleanup backlog terminology families

R3.1 inventory keeps existing public type names stable while prioritizing mapping/helper cleanup.

- **Materialization family** (`MaterializationPolicy`, `MaterializationRequirement`, `MaterializationBoundary`): keep distinct by layer; add mapping helpers where cross-layer rendering is needed.
- **Execution/work family** (`ExecutionState`, `DataWorkLevel`): keep both; strengthen deterministic mapping helpers for agent/CLI/report consistency.
- **Format/target family** (`DatasetFormat`, `OutputTargetKind`): keep separate input/output boundaries; add explicit mapping where interoperability summaries require it.
- **Fidelity family** (`FidelityLevel`, `VortexOutputFidelity`): keep canonical + adapter-local split; improve explicit mapping helpers and report parity checks.
- **Resource/memory family** (`ResourceBudget`, `MemoryBudget`, `BoundedMemoryPolicy`): keep policy boundaries explicit; avoid premature consolidation.
- **Plan skeleton family** (`RuntimePlanSkeleton`, `StreamingPlanSkeleton`, `ScanPlanSkeleton`): keep separate contracts for now; evaluate shared field helpers in targeted cleanup PRs.

## Terminology consolidation backlog

The R3.4 audit keeps terminology families distinct while documenting mapping-helper-first follow-ups.

- Materialization (`MaterializationPolicy`, `MaterializationRequirement`, `MaterializationBoundary`)
- Execution/data-work (`ExecutionState`, `DataWorkLevel`)
- Input/output/fidelity (`DatasetFormat`, `OutputTargetKind`, `FidelityLevel`, `VortexOutputFidelity`)
- Resource/memory (`ResourceBudget`, `MemoryBudget`, `BoundedMemoryPolicy`)
- Plan skeletons (`RuntimePlanSkeleton`, `StreamingPlanSkeleton`, `ScanPlanSkeleton`)
- Status/report suffixes (`Plan`, `Request`, `Report`, `Status`, `Mode`, `Signal`, `Effect`, `Ref`, `Id`)

See `docs/architecture/terminology-consolidation-backlog.md` for the full audit backlog and helper candidates.

## R5.1 glossary additions

- **fallback execution**: prohibited runtime delegation to an external engine.
- **compatibility baseline**: non-native engine/output reference used for comparison or interoperability checks, never as runtime delegation.
- **external baseline**: external system used only for correctness/benchmark reference, not execution fallback.
- **pushdown proof**: structured evidence describing whether a predicate/projection was pushed exactly, with residual, or rejected.
- **residual expression**: the remainder that must still run natively after partial pushdown.
- **lowering provenance**: trace of high-level plan constructs lowered into lower-level task/operator forms.
- **portability loss**: explicit representational loss when mapping native plan semantics to an interchange form.
- **intermediate artifact**: explicit typed runtime/planning artifact (spill, exchange, runtime-filter, staged-commit, profile sample) with stable identity.



## R5.2 glossary additions

- **capability certification surface**: the complete user-visible capability area used to evaluate evidence-backed best-default-engine certification for declared workloads.
- **capability supremacy surface**: older shorthand for capability certification surface; prefer the evidence-gated term in new docs.
- **universal native I/O envelope**: A ShardLoom-native contract that preserves representation state, pushdown evidence, and sink constraints without default decode.
- **native work envelope**: A single unit of planned/executed work carrying representation, stats, boundaries, and diagnostics.
- **foreign encoded**: Non-Vortex encoded representation preserved as encoded data when possible, without implying fallback execution.
- **semantic profile**: A named semantics compatibility target (for example ShardLoomNative, SparkCompatible, DataFusionCompatible).
- **compatibility baseline**: External engine or format behavior used for comparison and conformance checks, never runtime fallback.
- **external baseline**: Non-ShardLoom comparison oracle used for correctness/benchmark evidence only.
- **certification report**: Machine-readable evidence artifact describing tested capability level and conformance boundaries.
- **best-choice scorecard**: Evidence-backed summary of why ShardLoom is or is not the best default for a declared workload constitution.


## Additional capability-certification terms
- **workload constitution**: the declared workload categories used to scope claims and certification.
- **operator certification**: status evidence that an operator family meets correctness, semantics, and performance gates for a claim level.
- **function certification**: status evidence that a function definition meets semantic and execution-contract requirements for a claim level.
- **SQL coverage tier**: staged status level (for example S0-S7) describing SQL capability maturity.
- **adapter maturity level**: staged adapter certification level (for example A0-A7) for discovery/read/write/commit and evidence depth.
- **semantic compatibility profile**: named semantic mode (for example ShardLoomNative, SparkCompatible) with explicit behavior dimensions.
- **migration compatibility report**: structured report mapping supported/unsupported constructs and semantic deltas for migration planning.
- **approximate aggregate sketch**: mergeable probabilistic aggregate state used for functions such as approximate distinct counting; it must declare error bounds, serialization format, hash policy, and no-fallback evidence before certification.
- **encoded sketch strategy**: evidence describing how an approximate aggregate updates sketch state from encoded layouts such as dictionary ids, run values, validity masks, selection vectors, or partial-decode boundaries.
- **delete/tombstone compatibility report**: structured table-compatibility evidence for declared delete models, tombstones, row/position/equality deletes, metadata-loss boundaries, and no-fallback status.
- **native delete/tombstone rule**: ShardLoom-owned handling rule required before a delete model can be treated as supported; external delete files or metadata are never fallback execution.
- **table compatibility aggregation report**: structured evidence bundle combining schema, partition, and delete/tombstone compatibility reports before catalog or table metadata IO is allowed.
- **CDC incremental planning report**: structured evidence for declared change sets and CDC event summaries before CDC execution, table metadata IO, data reads, writes, or fallback execution are allowed.
- **CDC event summary**: count-bearing declaration of CDC event families such as insert, update, delete, tombstone, schema change, partition change, metadata-only, or unknown.
- **layout health report**: structured manifest evidence for small files, small segments, missing statistics, missing byte ranges, mixed formats/layouts/encodings, and compaction recommendations before any maintenance execution or storage IO is allowed.
- **compaction planning report**: structured recommendation evidence that groups declared small-file/small-segment candidates and blockers before any compaction execution, write IO, catalog IO, object-store IO, or fallback execution is allowed.
- **table intelligence report**: aggregate CG-9 evidence surface covering schema evolution, partition evolution, delete/tombstone semantics, table compatibility, CDC, layout health, compaction, snapshots/manifests, catalog compatibility, and commit/recovery status before catalog IO, table metadata IO, data IO, writes, table-format dependencies, or fallback execution are allowed.
- **correctness differential harness report**: aggregate CG-5 evidence surface combining fixture inventory, golden/reference output coverage, semantic edge-case coverage, unsupported-diagnostic expectations, external oracle policy, property/fuzz gaps, and benchmark-claim blockers before decoded-reference execution, external engine invocation, data reads, writes, or fallback execution are allowed.
- **object-store range planning report**: structured request-shape evidence derived from declared segment byte ranges before any object-store IO, full-file read, retry execution, distributed scheduling, or fallback execution is allowed.
- **dynamic work shaping report**: aggregate CG-8 evidence surface combining adaptive sizing policy, runtime feedback signals, target-task policy, backpressure policy, bounded-memory policy, scheduler queue policy, runtime application blockers, benchmark evidence blockers, and no-fallback policy before feedback loops mutate execution policy.
- **object-store request coalescing report**: structured comparison evidence showing how declared byte-range request shapes can be reduced before any object-store IO, retry execution, benchmark claim, or fallback execution is allowed.
- **object-store distributed scheduling report**: structured task-shape evidence derived from coalesced object-store request plans before any coordinator start, worker start, task execution, object-store IO, checkpoint write, retry execution, or fallback execution is allowed.
- **object-store checkpoint/retry report**: structured reliability-readiness evidence for distributed task retry policy, checkpoint plan, idempotency keys, attempt records, and cleanup policy before any retry execution, checkpoint write, cleanup execution, object-store IO, or fallback execution is allowed.
- **object-store commit protocol report**: structured readiness evidence for declared staging, manifest pointer, commit record, idempotency, cleanup, and atomicity signals before any object-store write, commit execution, provider probe, cleanup execution, distributed coordination, or fallback execution is allowed.
- **object-store request planner report**: aggregate CG-10 evidence surface combining range planning, request coalescing, distributed scheduling, checkpoint/retry/idempotency, and commit protocol status before full-file reads, coordinator/worker startup, task execution, retry execution, checkpoint writes, commit execution, object-store IO, writes, or fallback execution are allowed.
- **benchmark claim evidence report**: aggregate CG-6 evidence surface combining benchmark plans, required metrics, correctness evidence, measured result rows, external comparison rows, reproducibility metadata, and no-fallback policy before performance, superiority, cost, replacement, or best-default claims are allowed.
- **source pushdown exactness**: the declared guarantee quality for source pushdown (exact, exact with residual, conservative, unsupported, unsafe rejected).
- **native I/O certificate**: structured evidence object capturing source/sink capability, transitions, materialization boundaries, and no-fallback status.
- **data/ETL coverage entry**: per-capability evidence row for an ETL family such as ingestion, schema contracts, cleaning/quality, transformation, enrichment, incremental state, write/export, or pipeline operations.
- **data/ETL capability report**: structured evidence for ingestion, schema contracts, transformation, cleaning, data quality, incremental processing, write/export, lineage/provenance, memory/spill, orchestration, governance, and pipeline observability coverage.
- **Python surface report**: structured evidence for the Python wrapper/API, including protocol version, notebook/DataFrame/query-builder status, materialization boundaries, UDF boundaries, packaging, diagnostics, and no-fallback behavior.
- **unstructured media capability report**: structured evidence for document, text, image, audio, video, archive, extracted-field, chunk, and metadata handling with explicit effect/materialization boundaries.
- **universal adapter catalog**: workload-scoped inventory of source, sink, catalog, object-store, relational, warehouse, event/API/SaaS, client/server, Python/notebook, and unstructured/media adapters with maturity and certificate status.
- **API surface certification**: evidence that a CLI, Rust, Python, DataFrame, notebook, server, BI, or agent surface exposes native capability safely and explicitly.
- **observability certification**: evidence that explain, estimate, profile, metrics, certificates, and diagnostics reveal what ShardLoom did, avoided, or rejected.
- **deployment readiness report**: structured evidence for packaging, configuration, resource limits, reproducibility, compatibility, and operational constraints.
- **extension capability report**: structured UDF/plugin evidence for type metadata, effect level, sandboxing, permissions, materialization, and no-fallback behavior.
- **security governance report**: structured evidence for credentials, permissions, external effects, destructive-operation policy, redaction, audit, and agent safety.
- **effect budget report**: no-probe planning evidence for allowed or denied external effects, destructive effects, network egress, credentials, redaction, audit, materialization boundaries, cost, and fallback status.
- **functional context scope**: the declared amount and shape of source, plan, history, and evidence context a user/API/agent surface can reliably use, not merely accept as input.
- **evidence routing**: content- and capability-dependent selection of sources, segments, fields, operators, or artifacts to inspect or skip, backed by proof, uncertainty, or explicit unsupported diagnostics.
- **context structure preservation**: keeping source references, field paths, row/segment identity, ordering, partitioning, provenance, and neighboring context visible across planning, migration, and agent-facing reports.
- **stateful certificate history**: reusable record of prior execution/capability certificates, invalidation causes, and plan decisions that can be consumed by future sessions without relying on lossy summaries.
