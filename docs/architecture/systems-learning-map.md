# Systems Learning Map

## Purpose

This document captures conceptual lessons from mature systems and translates them into
ShardLoom-native contracts. It is reference material only. Active implementation status and queue
placement live in `docs/architecture/phased-execution-plan.md`.

These systems are pressure tests for ShardLoom-native architecture and diagnostics. They are not
dependency targets, and they are not fallback execution targets.

## Non-Goals

- No Spark dependency.
- No DataFusion dependency.
- No Trino dependency.
- No Dask dependency.
- No Ray dependency.
- No DuckDB dependency.
- No Calcite dependency.
- No Arrow Acero/Substrait dependency.
- No external engine execution.
- No fallback execution.
- No SQL parser implementation from this document alone.
- No distributed execution from this document alone.

## Lesson Map

- Trino lessons
  - Connector capability boundaries should be explicit.
  - Pushdown acceptance and residual responsibilities must be diagnosable.
  - Split/task lifecycle should be first-class.
  - Runtime dynamic filtering lifecycle should be visible.
  - System introspection surfaces should be queryable.
  - Intermediate exchange/spooling semantics should be explicit.
  - ShardLoom vocabulary: `PushdownProof`, `PushdownGuarantee`, `ProofBasis`,
    `RuntimeFilterLifecycle`, `SplitSource`, `TaskLease`, `IntermediateArtifactRef`, `system.*`
    diagnostics surfaces.
- Dask lessons
  - Keep graph layering explicit.
  - Distinguish high-level graph intent from low-level task execution shape.
  - Preserve lowering provenance.
  - Keep scheduler policy distinct from plan semantics.
  - Make task granularity policy explicit and auditable.
  - ShardLoom vocabulary: `LoweringTrace`, `LoweringRuleId`, `PlanGuarantee`, `InformationLoss`,
    `TaskGranularityPolicy`, fuse/split/coalesce decision records.
- Ray lessons
  - Resource vectors should be explicit inputs to scheduling decisions.
  - Placement hints should be visible and overridable by policy.
  - Object-like references should preserve lineage.
  - Recovery should distinguish retry, reconstruct, and reuse.
  - ShardLoom vocabulary: `ResourceVector`, `PlacementHint`, `RecoveryStrategy`, `LineageRef`,
    `ReconstructFromLineage`.
- DuckDB lessons
  - Vectorized execution ergonomics must remain developer-visible.
  - Operator profile outputs should be easy to read and compare.
  - Planned versus actual cardinality should be explicit.
  - Pipeline breakers should be explicit diagnostics boundaries.
  - ShardLoom vocabulary: `OperatorProfile`, `PlannedVsActualOperatorProfile`,
    `PipelineBreakerKind`, bytes/decode/materialization avoided.
- Calcite lessons
  - SQL frontend parsing, binding, and validation must be explicit boundaries.
  - Relational algebra boundary should be well-defined.
  - Adapters are capability surfaces, not hidden execution delegation.
  - Planner rules should be diagnosable with stable identifiers.
  - ShardLoom vocabulary: SQL frontend boundary, parse/bind/validate-only phase, ShardLoom Plan IR
    semantics, unsupported SQL diagnostics.
- Arrow Acero/Substrait lessons
  - Operator graph portability is useful but must preserve native semantics.
  - Validation without execution is essential.
  - Export/import must report loss boundaries explicitly.
  - ShardLoom vocabulary: `PlanPortabilityReport`, native-only nodes, representable nodes, lossy
    nodes, unsupported nodes, portability diagnostics.
- Spark and DataFusion capability lessons
  - Spark and DataFusion are capability baselines, not fallback engines.
  - Spark lesson: broad platform capability across SQL, Python-style workflows, APIs, deployment,
    monitoring, streaming, ETL, lakehouse workflows, and operational integrations.
  - DataFusion lesson: extensible local SQL/DataFrame capability with operators, functions,
    adapters, UDFs, and Arrow-oriented ecosystem habits.
  - DataFusion/Polars approximate-function lesson: approximate distinct and approximate unique
    functions are user-visible capability expectations, but ShardLoom should translate them into
    mergeable sketch-state, exact-reference/error-bound evidence, and encoded-aware
    dictionary/run-length/selection-vector update strategies rather than a generic decoded aggregate
    shortcut.
  - ShardLoom translation: SQL coverage matrix, operator coverage matrix, function coverage matrix,
    adapter certification, data/ETL capability reports, Python surface reports, unstructured/media
    capability reports, semantic profiles, migration analyzers, capability discovery.
- Vortex upstream lessons
  - Vortex's public direction emphasizes compressed arrays, logical DTypes, physical encodings,
    extensible layouts, Scan API source/sink boundaries, lazy operator evaluation, IO
    coalescing/backpressure, GPU/device-array exploration, footer/cache reuse, wide-table
    optimization, nested/list support, and benchmark visibility.
  - ShardLoom translation: keep ShardLoom's admission, policy, diagnostics, certificate, and
    capability model independent while allowing upstream Vortex array, compute, Scan API, source,
    sink, IO, write, or device concepts to serve as native providers behind approved ShardLoom
    boundaries.
  - Vortex integration adapters may bridge upstream Scan API, array/operator, IO, write, or device
    concepts only through ShardLoom-native reports, certificates, materialization boundaries, and
    explicit unsupported diagnostics.
  - Guardrail: upstream Vortex-native providers are distinct from Vortex query-engine integrations.
    Vortex DataFusion, DuckDB, Spark, Trino, and similar integrations may inform baselines,
    migration, or interoperability references, but must not execute unsupported ShardLoom residual
    work as fallback.
  - Vortex's Arrow/vector-oriented integration paths are compatibility boundaries for ShardLoom
    unless a later RFC certifies a native use; they are not the default internal execution
    substrate.
  - Upstream-alignment lessons: Scan API compatibility, composite pushdown, deferred/iterative
    execution, device residency, extension dtypes, push/streaming sinks, IO backend evidence,
    telemetry facets, compression advice, integrity/encryption status, PyVortex interop, and Vortex
    benchmark interop should become explicit ShardLoom evidence surfaces before support or
    performance claims.
  - Compression lessons: cascading BtrBlocks-style selection, deterministic sampling, per-column
    strategy, ALP/ALP-RD float semantics, FSST random-access strings, FastLanes integer layout,
    German strings/StringView, sparse/constant/dictionary/run-end/zigzag encodings, and temporal
    decomposition require encoding-specific correctness, null/NaN/negative-zero, patch, and
    benchmark evidence before production claims.
  - Metadata/pruning lessons: zone maps should be logical-row-zone evidence, not merely physical
    page evidence; clustering and missing statistics must be diagnosable, and pruning must never
    exclude valid rows.
  - Layout lessons: Vortex layouts and segments are not equivalent to file byte ranges;
    CG-1/CG-8/CG-10/CG-19 planning must distinguish row splits, layout splits, byte ranges, segment
    ranges, and task scheduling hints.
  - Versioning lessons: Vortex editions, postscript/footer feature sets, forward compatibility,
    compression/encryption specs, and reader capability negotiation should feed adapter
    certification and native I/O certificates before ShardLoom broadens accepted file features.
  - Object-storage lessons: tail latency, hedged reads, cache economics, request budgets,
    coalescing, prefetch, and endpoint/provider variability belong in object-store plans and
    estimates; they must be surfaced as planning evidence rather than hidden retry behavior.
  - Lakehouse lessons: Iceberg integration needs row-splittability, deletion-vector pushdown,
    encryption, safe native-handle lifecycles across language boundaries, and source/sink
    certificate evidence; Spark/Iceberg demos remain external baselines, not fallback paths.
  - Machine-scale data lessons: embeddings, images, video, PDFs, and other large or small multimodal
    objects reinforce CG-20 unstructured/media, governance, Python/API, adapter, and security
    reports, but do not authorize hidden external effects.
- User workflow, engine fabric, and remote API lessons
  - Source scope: Flink dynamic tables
    (`https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/sql-table-concepts/dynamic_tables/`),
    Kafka Streams architecture
    (`https://docs.confluent.io/platform/current/streams/architecture.html`), Apache Paimon
    (`https://paimon.apache.org/docs/master/`), OpenAPI
    (`https://spec.openapis.org/oas/v3.2.0.html`), AsyncAPI
    (`https://www.asyncapi.com/docs/reference/specification/latest`), CloudEvents
    (`https://cloudevents.io/`), OpenTelemetry OTLP (`https://opentelemetry.io/docs/specs/otlp/`),
    OpenLineage (`https://openlineage.io/`), Apache Arrow Flight/ADBC
    (`https://arrow.apache.org/docs/format/Flight.html`, `https://arrow.apache.org/adbc/`), and MCP
    (`https://modelcontextprotocol.io/specification/2025-06-18`).
  - Flink dynamic-table lesson: batch and continuous query semantics can share relational/table
    abstractions, but unbounded inputs need explicit update mode, output mode, watermarks, state,
    and query restrictions. ShardLoom translation: CG-22 must expose batch/live/hybrid support per
    operator rather than pretending live mode can run every batch query.
  - Kafka Streams state lesson: stateful stream processing needs local state, changelogs,
    checkpoints/recovery, partition/task ownership, and memory/cache policy. ShardLoom translation:
    live and hybrid engines require state certificates, checkpoint evidence, lag/freshness
    reporting, and idempotency before certification.
  - Paimon-style lake format lesson: streaming and batch lakehouse workloads converge around
    real-time updates, LSM-like hot state, and analytical storage. ShardLoom translation: hybrid
    execution should combine hot deltas, warm Vortex micro-segments, cold Vortex segments,
    tombstones, and snapshot certificates rather than becoming a general OLTP store.
  - OpenAPI/AsyncAPI/CloudEvents lesson: remote products need machine-readable HTTP contracts, event
    contracts, and common event envelopes. ShardLoom translation: CG-23 should treat REST as the
    control/proof plane, AsyncAPI/CloudEvents as event-plane contracts, and data-plane formats as
    explicit result boundaries.
  - OpenTelemetry/OpenLineage lesson: production data systems need traces, metrics, logs, lineage,
    run/dataset/job metadata, and governance hooks. ShardLoom translation: execution certificates,
    native I/O certificates, materialization boundaries, representation state, and no-fallback
    evidence should be exportable to telemetry and lineage systems.
  - MCP/agent lesson: agent integrations are powerful only if execution authority, credentials,
    effects, and diagnostics are constrained. ShardLoom translation: MCP should expose resources and
    dry-run/explain/estimate/certify tools by default, with execute/write/cancel/benchmark requiring
    explicit policy.
  - Guardrail: these lessons do not authorize Flink, Kafka, Paimon, OpenAPI server dependencies,
    AsyncAPI generators, telemetry exporters, lineage clients, MCP servers, object stores, streaming
    runtimes, or fallback execution. They are synthesis inputs for CG-21, CG-22, and CG-23 planning.
- Foundry availability and platform-integration lessons
  - Source scope: Foundry Code Repositories, Artifact Repositories, lightweight transforms, virtual
    tables, compute pushdown, datasets, transactions, branches, Data Health/Expectations, Data
    Lineage, schedules, Data Connection, Foundry S3-compatible dataset API, media sets, Ontology,
    AIP, model integration, Compute Modules, Marketplace, BYOC, and platform security/markings.
  - Package lesson: Foundry adoption starts with boring package proof. ShardLoom translation: Conda
    CLI/Python/metapackage install, deterministic CLI resolution, smoke output envelopes,
    checksums/SBOM/provenance, and certificate output examples should precede platform-specific
    runtime claims.
  - Dataset/transaction lesson: Foundry datasets carry transaction, branch,
    preview/build/incremental, lineage, and governance context. ShardLoom translation: Foundry
    evidence must identify dataset RIDs, transactions, branches, build mode, staging/materialization
    boundaries, and no-fallback status.
  - Virtual-table lesson: Foundry virtual tables are governed pointers to external systems and may
    support external compute pushdown. ShardLoom translation: virtual tables are workflow handles;
    Snowflake/Databricks/BigQuery/Foundry Spark compute is baseline/oracle/migration behavior unless
    ShardLoom stages or natively accesses data and emits certificates.
  - Operational lesson: Foundry Data Health, Data Expectations, Lineage, schedules, Ontology, AIP,
    Marketplace, and Compute Modules are adoption surfaces around evidence. ShardLoom translation:
    `shardloom-foundry` should make certificates, diagnostics, benchmark rows, governance, and
    unsupported blockers visible to those surfaces without becoming an execution engine.
  - Guardrail: these lessons do not authorize Foundry dependencies, Foundry transform execution,
    virtual-table native execution, Snowflake/Databricks/BigQuery/Spark pushdown as ShardLoom
    execution, package publication, Marketplace publication, Compute Module deployment, or fallback
    execution. They are synthesis inputs for RFC 0036 and late-stage Priority 8/9 work.
- Modal GPU Glossary technique-transfer lessons
  - Source scope: Modal's GPU Glossary index and topic pages across device hardware, device
    software, host software, and performance. Reference examples include the glossary README
    (`https://modal.com/gpu-glossary/readme`), PTX
    (`https://modal.com/gpu-glossary/device-software/parallel-thread-execution`), CUDA Graphs
    (`https://modal.com/gpu-glossary/host-software/cuda-graph`), roofline model
    (`https://modal.com/gpu-glossary/perf/roofline-model`), and memory coalescing
    (`https://modal.com/gpu-glossary/perf/memory-coalescing`).
  - Cross-layer vocabulary lesson: a high-quality technical system needs linked terminology spanning
    physical representation, execution model, host/runtime boundaries, tooling, and performance.
    ShardLoom translation: README links to core terms, `canonical-terminology.md` owns the
    glossary/index, RFCs own deep contracts, and this systems-learning map owns external lessons.
  - Capability compatibility lesson: CUDA separates virtual/portable IR, device-specific lowerings,
    compute capability, runtime APIs, driver APIs, and library primitives. ShardLoom translation:
    keep SQL/frontend, native plan IR, physical operators, kernel capability, adapter maturity,
    semantic profile, and deployment/runtime capability as separate evidence surfaces.
  - Bottleneck taxonomy lesson: roofline-style analysis names compute-bound, memory-bound,
    arithmetic intensity, overhead, latency hiding, occupancy, issue efficiency, stalls, divergence,
    memory coalescing, bank conflicts, and register pressure before optimizing. ShardLoom
    translation: explain/profile/certification reports should name whether work is blocked by bytes
    read, decode cost, materialization pressure, selection-vector density, object-store latency,
    scheduler occupancy, operator state pressure, spill pressure, or host/orchestration overhead.
  - Granularity lesson: GPU thread/warp/block/grid hierarchy makes execution granularity explicit.
    ShardLoom translation: segment, chunk, batch, task, operator, stage, stream, and sink boundaries
    should be first-class planning/certificate fields rather than incidental implementation details.
  - Launch/capture lesson: CUDA Graphs reduce repeated host submission overhead by capturing a
    replayable graph of device work. ShardLoom translation: future execution plans and certificates
    should distinguish planning overhead, task-submission overhead, reusable/captured plan shapes,
    and safe replay constraints without adding hidden side effects.
  - Tooling lesson: NVML, `nvidia-smi`, CUPTI, Nsight, binary utilities, cuBLAS, and cuDNN show that
    observability, inspection, profiling, and curated primitive libraries are part of the product
    surface. ShardLoom translation: `explain`, `estimate`, `profile`, `doctor`, `capabilities`,
    `certify`, kernel-registry reports, and benchmark dossiers belong in the core roadmap, not in
    polish-only work.
  - Guardrail: these lessons do not authorize GPU dependencies, CUDA runtime integration, external
    kernel libraries, hidden acceleration, or benchmark claims. They are technique-transfer
    references for vocabulary, evidence, profiling taxonomy, and capability certification.
- SubQ long-context technique-transfer lessons
  - Source scope: Subquadratic's May 5, 2026 launch article (`https://subq.ai/introducing-subq`),
    technical SSA article (`https://subq.ai/how-ssa-makes-long-context-practical`), and product page
    (`https://subq.ai/`). The model card and technical report are described as forthcoming by the
    source, so ShardLoom should treat benchmark and product claims as external context only until
    independently reproducible evidence exists.
  - Nominal versus functional context lesson: the technical article distinguishes a nominal context
    window from a functional context window that can reliably retrieve and reason across distributed
    evidence. ShardLoom translation: CG-20 capability claims must distinguish nominal feature
    acceptance from functional capability certification over declared workload constitutions, with
    correctness, semantic, adapter, benchmark, and no-fallback evidence.
  - Content-dependent routing lesson: SSA is presented as selecting relevant positions based on
    content rather than fixed position patterns or all-pairs work. ShardLoom translation: optimizer,
    pushdown, pruning, runtime-filter, selection-vector, and adapter planning should route work by
    proven content/capability evidence rather than fixed file/page/chunk heuristics, and every
    skipped unit needs proof or an explicit uncertainty label.
  - Structure-preservation lesson: the SubQ article argues that chunking/retrieval pipelines can
    lose position, hierarchy, neighboring context, and reference structure. ShardLoom translation:
    native work envelopes, migration reports, and unstructured/media capability reports should
    preserve source refs, field paths, row/segment identity, ordering/partitioning, provenance, and
    residual context instead of flattening everything into anonymous decoded batches.
  - Exact-evidence lesson: SubQ frames long-context enterprise work as multi-hop reasoning over
    fragmented evidence, not simple lookup. ShardLoom translation: best-choice dossiers should
    include multi-hop analytical workloads where answers depend on schema evolution, partition
    metadata, adapter pushdown, source statistics, table semantics, and execution certificates
    across multiple artifacts.
  - Scaffolding lesson: the SubQ narrative treats RAG/orchestration as useful but failure-prone
    scaffolding around model limits. ShardLoom translation: adapters, migration analyzers, and
    Python/API surfaces should reduce coordination burden through native reports and certificates
    rather than hiding limitations behind external-engine fallback, ad hoc orchestration, or lossy
    summaries.
  - Iteration-economics lesson: lower long-context cost is described as making experimentation
    routine instead of reserved. ShardLoom translation: work-avoidance, metadata-first execution,
    reusable certificates, and stable benchmark harnesses should reduce the cost of repeated
    correctness/benchmark/differential runs, making evidence refresh cheap enough for CI rather than
    occasional release exercises.
  - Agent-state lesson: the product framing includes full repositories, months of PRs, long
    histories, and persistent state. ShardLoom translation: CG-17 stateful reuse and CG-20
    agent/Python/API surfaces should preserve exact prior constraints, certificates, invalidation
    causes, and plan decisions rather than relying on compressed human summaries.
  - Guardrail: these lessons do not authorize calling SubQ, adding model dependencies, replacing
    ShardLoom diagnostics with long-context prompts, accepting vendor benchmark claims as ShardLoom
    evidence, or weakening no-fallback policy. They are technique-transfer references for functional
    capability, evidence routing, structure preservation, and agent-facing certification.

## Placement Guidance

- Now/docs-only
  - Systems-learning map.
  - Pushdown proof vocabulary.
  - Lowering provenance vocabulary.
  - Task granularity vocabulary.
- Near phase
  - Diagnostics report schemas.
  - Capability report extensions.
  - Explain/estimate additions.
- Before real execution
  - Task lifecycle.
  - Resource vector.
  - Operator profile.
  - Runtime filter lifecycle.
- Before distributed/object-store execution
  - Split source.
  - Task lease.
  - Placement hints.
  - Intermediate artifact refs.
  - Recovery strategy.
- Before SQL UX
  - SQL frontend RFC.
  - Bind/validate/unsupported diagnostics.
  - Tiny SQL subset.
- Before agent/API context expansion
  - Functional context scope.
  - Exact source/reference preservation.
  - Evidence-routing diagnostics.
  - Stateful certificate/invalidation history.
- Before CG-21 user workflow execution
  - Side-effect-free install/import/capability discovery.
  - Source/sink registry and adapter maturity visibility.
  - Data contracts, quality gates, observability, migration, benchmark, notebook, UDF, governance,
    and workload scorecards.
- Before CG-22 live/hybrid execution
  - Engine mode, boundedness, update mode, and output mode contracts.
  - Per-engine capability matrix.
  - Change-record, watermark, state, checkpoint, freshness, hot/cold, and delta-overlay
    certificates.
- Before CG-23 remote API execution
  - OpenAPI contract.
  - Problem-details diagnostics.
  - Result delivery policy and data-plane boundaries.
  - AsyncAPI/CloudEvents event contracts.
  - Auth, scopes, audit, redaction, and agent policy.

## User-Surface Lessons

- Mature engines are selected through product surfaces as much as kernels.
- API ergonomics, notebook access, BI/server access, observability, deployment posture,
  security/governance, and extension safety all affect default-engine adoption.
- ShardLoom translates those lessons into native certification reports rather than hidden
  integration shortcuts:
  - `ApiSurfaceReport`
  - `DataEtlCapabilityReport`
  - `PythonSurfaceReport`
  - `UnstructuredMediaCapabilityReport`
  - `UniversalAdapterCatalog`
  - `ObservabilityCertificationReport`
  - `DeploymentReadinessReport`
  - `ExtensionCapabilityReport`
  - `SecurityGovernanceReport`
- Client/server, Python/notebook, BI, UDF/plugin, common ETL, unstructured/media, universal-adapter,
  and external-effect surfaces must expose capability checks and diagnostics before execution.
- External systems can be sources, sinks, baselines, or effect boundaries, but not fallback
  execution engines.

## Guardrails

- No fallback engines.
- No default Arrow conversion.
- No external execution delegation.
- No new dependencies from this document alone.
- Vortex remains native first-class input and output.
- ShardLoom owns runtime, optimizer, diagnostics, and policy.
- CG-20 covers capability breadth across SQL, operators, functions, adapters, semantics, migration,
  Python, UDFs, common ETL, unstructured/media, and user-facing certification; it is not SQL-only.
