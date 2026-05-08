# Systems Learning Map

## Purpose

This document captures conceptual lessons from mature systems and translates them into ShardLoom-native contracts. It is reference material only. Active implementation status and queue placement live in `docs/architecture/phased-execution-plan.md`.

These systems are pressure tests for ShardLoom-native architecture and diagnostics. They are not dependency targets, and they are not fallback execution targets.

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
  - ShardLoom vocabulary: `PushdownProof`, `PushdownGuarantee`, `ProofBasis`, `RuntimeFilterLifecycle`, `SplitSource`, `TaskLease`, `IntermediateArtifactRef`, `system.*` diagnostics surfaces.
- Dask lessons
  - Keep graph layering explicit.
  - Distinguish high-level graph intent from low-level task execution shape.
  - Preserve lowering provenance.
  - Keep scheduler policy distinct from plan semantics.
  - Make task granularity policy explicit and auditable.
  - ShardLoom vocabulary: `LoweringTrace`, `LoweringRuleId`, `PlanGuarantee`, `InformationLoss`, `TaskGranularityPolicy`, fuse/split/coalesce decision records.
- Ray lessons
  - Resource vectors should be explicit inputs to scheduling decisions.
  - Placement hints should be visible and overridable by policy.
  - Object-like references should preserve lineage.
  - Recovery should distinguish retry, reconstruct, and reuse.
  - ShardLoom vocabulary: `ResourceVector`, `PlacementHint`, `RecoveryStrategy`, `LineageRef`, `ReconstructFromLineage`.
- DuckDB lessons
  - Vectorized execution ergonomics must remain developer-visible.
  - Operator profile outputs should be easy to read and compare.
  - Planned versus actual cardinality should be explicit.
  - Pipeline breakers should be explicit diagnostics boundaries.
  - ShardLoom vocabulary: `OperatorProfile`, `PlannedVsActualOperatorProfile`, `PipelineBreakerKind`, bytes/decode/materialization avoided.
- Calcite lessons
  - SQL frontend parsing, binding, and validation must be explicit boundaries.
  - Relational algebra boundary should be well-defined.
  - Adapters are capability surfaces, not hidden execution delegation.
  - Planner rules should be diagnosable with stable identifiers.
  - ShardLoom vocabulary: SQL frontend boundary, parse/bind/validate-only phase, ShardLoom Plan IR semantics, unsupported SQL diagnostics.
- Arrow Acero/Substrait lessons
  - Operator graph portability is useful but must preserve native semantics.
  - Validation without execution is essential.
  - Export/import must report loss boundaries explicitly.
  - ShardLoom vocabulary: `PlanPortabilityReport`, native-only nodes, representable nodes, lossy nodes, unsupported nodes, portability diagnostics.
- Spark and DataFusion capability lessons
  - Spark and DataFusion are capability baselines, not fallback engines.
  - Spark lesson: broad platform capability across SQL, Python-style workflows, APIs, deployment, monitoring, streaming, ETL, lakehouse workflows, and operational integrations.
  - DataFusion lesson: extensible local SQL/DataFrame capability with operators, functions, adapters, UDFs, and Arrow-oriented ecosystem habits.
  - ShardLoom translation: SQL coverage matrix, operator coverage matrix, function coverage matrix, adapter certification, data/ETL capability reports, Python surface reports, unstructured/media capability reports, semantic profiles, migration analyzers, capability discovery.
- Vortex upstream lessons
  - Vortex's public direction emphasizes compressed arrays, logical DTypes, physical encodings, extensible layouts, Scan API source/sink boundaries, lazy operator evaluation, IO coalescing/backpressure, GPU/device-array exploration, footer/cache reuse, wide-table optimization, nested/list support, and benchmark visibility.
  - ShardLoom translation: treat those as native-design references for CG-1/CG-7/CG-8/CG-19/CG-20 while keeping ShardLoom's internal execution independent, zero-decode-first, Vortex-native, and no-fallback.
  - Vortex integration adapters may bridge upstream Scan API, array/operator, IO, write, or device concepts only through ShardLoom-native reports, certificates, materialization boundaries, and explicit unsupported diagnostics.
  - Vortex's Arrow/vector-oriented integration paths are compatibility boundaries for ShardLoom unless a later RFC certifies a native use; they are not the default internal execution substrate.
  - Compression lessons: cascading BtrBlocks-style selection, deterministic sampling, per-column strategy, ALP/ALP-RD float semantics, FSST random-access strings, FastLanes integer layout, German strings/StringView, sparse/constant/dictionary/run-end/zigzag encodings, and temporal decomposition require encoding-specific correctness, null/NaN/negative-zero, patch, and benchmark evidence before production claims.
  - Metadata/pruning lessons: zone maps should be logical-row-zone evidence, not merely physical page evidence; clustering and missing statistics must be diagnosable, and pruning must never exclude valid rows.
  - Layout lessons: Vortex layouts and segments are not equivalent to file byte ranges; CG-1/CG-8/CG-10/CG-19 planning must distinguish row splits, layout splits, byte ranges, segment ranges, and task scheduling hints.
  - Versioning lessons: Vortex editions, postscript/footer feature sets, forward compatibility, compression/encryption specs, and reader capability negotiation should feed adapter certification and native I/O certificates before ShardLoom broadens accepted file features.
  - Object-storage lessons: tail latency, hedged reads, cache economics, request budgets, coalescing, prefetch, and endpoint/provider variability belong in object-store plans and estimates; they must be surfaced as planning evidence rather than hidden retry behavior.
  - Lakehouse lessons: Iceberg integration needs row-splittability, deletion-vector pushdown, encryption, safe native-handle lifecycles across language boundaries, and source/sink certificate evidence; Spark/Iceberg demos remain external baselines, not fallback paths.
  - Machine-scale data lessons: embeddings, images, video, PDFs, and other large or small multimodal objects reinforce CG-20 unstructured/media, governance, Python/API, adapter, and security reports, but do not authorize hidden external effects.

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

## User-Surface Lessons

- Mature engines are selected through product surfaces as much as kernels.
- API ergonomics, notebook access, BI/server access, observability, deployment posture, security/governance, and extension safety all affect default-engine adoption.
- ShardLoom translates those lessons into native certification reports rather than hidden integration shortcuts:
  - `ApiSurfaceReport`
  - `DataEtlCapabilityReport`
  - `PythonSurfaceReport`
  - `UnstructuredMediaCapabilityReport`
  - `UniversalAdapterCatalog`
  - `ObservabilityCertificationReport`
  - `DeploymentReadinessReport`
  - `ExtensionCapabilityReport`
  - `SecurityGovernanceReport`
- Client/server, Python/notebook, BI, UDF/plugin, common ETL, unstructured/media, universal-adapter, and external-effect surfaces must expose capability checks and diagnostics before execution.
- External systems can be sources, sinks, baselines, or effect boundaries, but not fallback execution engines.

## Guardrails

- No fallback engines.
- No default Arrow conversion.
- No external execution delegation.
- No new dependencies from this document alone.
- Vortex remains native first-class input and output.
- ShardLoom owns runtime, optimizer, diagnostics, and policy.
- CG-20 covers capability breadth across SQL, operators, functions, adapters, semantics, migration, Python, UDFs, common ETL, unstructured/media, and user-facing certification; it is not SQL-only.
