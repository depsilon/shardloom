# ShardLoom Global Architecture Review

## Purpose

This file reviews every ShardLoom RFC plus the compute-engine flow reference against the current
repository implementation. It is a staging checklist for architecture follow-through.

Rules for reading this file:

- Checked items are implemented or represented by evidence-backed report surfaces in the repo.
- Unchecked items are not implemented enough to claim; promote actionable follow-up into
  `docs/architecture/phased-execution-plan.md` before implementation begins.
- This file is not the active execution queue. The active queue remains
  `docs/architecture/phased-execution-plan.md`.
- Every unchecked item in this review is mirrored into the phased execution plan under the Global
  Architecture Review carry-forward block so it can be worked in order.
- External engines are baselines, oracles, comparison rows, or migration references only. They are
  never ShardLoom fallback execution.
- Vortex-native claims require workload-scoped correctness, benchmark, execution-certificate,
  Native I/O, materialization/decode, policy, and no-fallback evidence.

## Unchecked Promotion Themes

These themes summarize the unchecked RFC-level items below. The concrete unchecked items are
mirrored into `docs/architecture/phased-execution-plan.md` under the Global Architecture Review
carry-forward block. When implementation needs a smaller slice, split the mirrored item in the phase
plan before coding.

- Runtime and execution: SQL/DataFrame execution, direct transient runtime, broad expression/kernel
  execution, generalized encoded filter/project execution, live/hybrid engines, distributed
  object-store runtime, adaptive/skew/runtime-filter execution, and real SIMD dispatch.
- I/O, output, and tables: object-store reads/writes/commits, broad Vortex read/write support,
  catalog/table metadata integration, delete/tombstone/CDC execution, compatibility export writers,
  and lakehouse transaction semantics.
- Product and ecosystem: public package publication, release matrix evidence, API compatibility
  windows/signing decisions, generated client/wrapper ecosystem, REST/remote API server, and
  Foundry production integration.
- Governance and extension: credential lifecycle, runtime policy enforcement, sandbox execution,
  plugin loading, UDF execution, LLM/API-call execution, and embeddings execution.
- Benchmarks and claims: full CG-5/CG-6/CG-20/competitive claim evidence, managed-platform lanes,
  source-backed comparative reruns, and any public performance or superiority claims.

## RFC Reviews

### RFC 0001 - ShardLoom Architecture

- Source: [`docs/rfcs/0001-architecture.md`](../rfcs/0001-architecture.md)
- Current read: Partially implemented architecture spine.
- Evidence: `shardloom-core/src/architecture_spine.rs`, `shardloom-plan`, `shardloom-exec`,
  `shardloom-vortex`, `shardloom-cli`, `Cargo.toml`
- [x] Core crate boundaries, architecture vocabulary, and ShardLoom/Vortex layering exist.
- [x] Vortex-first/no-fallback identity is represented in code, docs, diagnostics, and tests.
- [x] GAR-0001A-A adds a report-only SQL/DataFrame planner-readiness matrix through
  `capabilities sql`, `capabilities dataframe`, and Python `CapabilityView` accessors. SQL text,
  SQL parse/bind/plan/execute, DataFrame lazy-plan/expression/join/aggregate/window, diagnostics,
  and unsupported execution states are visible as `support_status=report_only|unsupported`,
  `claim_gate_status=not_claim_grade`, with deterministic diagnostic codes, blocker ids, no parser,
  no binder, no planner, no runtime, no external engine, and no fallback.
- [x] GAR-0001A-B adds `global-architecture-gate`, a report-only runtime-claim gate for
  distributed coordinator/worker/task execution, object-store range/full-file read, object-store
  write/commit, lakehouse catalog metadata, lakehouse transaction commit, and CDC/delete/tombstone
  execution. The gate is release/readiness evidence, keeps `claim_gate_status=not_claim_grade`,
  lists deterministic diagnostic codes and blocker ids, and preserves no runtime execution, no
  credentials, no data reads, no object-store/table/catalog/write I/O, no external engine, and no
  fallback.
- [ ] Executable SQL/DataFrame runtime, distributed runtime, broad lakehouse-compatible output, and
  general object-store execution remain incomplete; GAR-0001A-B blocks those claims until the
  narrower GAR-0008, GAR-0020, and GAR-0028 evidence slices land.
- [ ] Spark-displacement or engine-replacement claims remain not claimable until runtime and output
  evidence closes.

### RFC 0002 - No-Fallback Execution and Native Vortex I/O Contract

- Source: [`docs/rfcs/0002-no-fallback-and-vortex-io.md`](../rfcs/0002-no-fallback-and-vortex-io.md)
- Current read: Implemented as a governing invariant with incomplete native coverage.
- Evidence: `shardloom-core/src/capabilities.rs`, `shardloom-core/src/diagnostics.rs`,
  `shardloom-core/src/benchmark.rs`, `shardloom-core/src/benchmark_suite.rs`,
  `shardloom-contract-tests/tests/no_fallback_invariants.rs`, `shardloom-vortex/Cargo.toml`
- [x] Fallback-disabled policy, no-fallback diagnostics, and external-baseline-only posture are
  represented.
- [x] Upstream Vortex use is feature-gated and treated as a native provider only when admitted.
- [x] Unsupported native source, sink, operator, and workload coverage now has deterministic
  diagnostics for the current matrix via `compute-capability-matrix` native unsupported coverage
  rows, with `support_status=unsupported`, `claim_gate_status=not_claim_grade`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [x] One scoped native Vortex admission lane is explicit via
  `compute-capability-matrix` `native_vortex_admission` rows and
  `vortex-count-benchmark` `native_vortex_admission_*` fields for local Vortex file scan,
  `CountAll`, and typed scalar result evidence only.
- [ ] Native Vortex support is not universal across every source, sink, operator, and workload.

### RFC 0003 - Encoded Segment Model

- Source: [`docs/rfcs/0003-encoded-segment-model.md`](../rfcs/0003-encoded-segment-model.md)
- Current read: Core model and narrow encoded execution slices exist.
- Evidence: `shardloom-core/src/encoded.rs`,
  `shardloom-vortex/src/encoded_predicate_evaluation.rs`,
  `shardloom-vortex/src/encoded_projection_execution.rs`,
  `shardloom-vortex/src/selection_vector_filter_kernel.rs`
- [x] Encoded segment contracts, encoded predicate surfaces, and encoded projection/filter slices
  are present.
- [x] Local encoded read/query primitive evidence records materialization and decode boundaries.
- [x] GAR-0003-A adds a sparse patch/fill segment extraction admission report through
  `vortex-api-inventory` and benchmark coverage refs. Sparse extraction is deterministically blocked
  with required correctness, execution-certificate, Native I/O, materialization/decode, and
  no-fallback evidence before support claims.
- [x] GAR-0003-B adds a shared materialization/decode policy through `compute-capability-matrix`,
  Python accessors, and benchmark coverage refs so encoded-native, residual-native,
  materialized-temporary, and unsupported paths report decode/materialization posture and claim
  boundaries.
- [ ] Full production Vortex segment extraction and broad operator coverage remain incomplete.

### RFC 0004 - Native Dataset, Manifest, Snapshot, and Incremental Change Model

- Source:
  [`docs/rfcs/0004-native-dataset-manifest-snapshot-incremental.md`](../rfcs/0004-native-dataset-manifest-snapshot-incremental.md)
- Current read: Contracts and local staged helpers exist; table/incremental execution is not broad.
- Evidence: `shardloom-core/src/manifest.rs`, `shardloom-core/src/table_intelligence.rs`,
  `shardloom-cli/tests/table_intelligence_plan_snapshots.rs`,
  `shardloom-vortex/src/staged_manifest.rs`, `shardloom-vortex/src/manifest_finalization.rs`,
  `shardloom-vortex/src/commit_protocol.rs`, `shardloom-vortex/tests/staged_write_readiness.rs`
- [x] Manifest, snapshot, change, staged write, and commit-protocol report contracts exist.
- [x] Local staged write/readiness evidence covers a narrow Vortex artifact path.
- [x] `GAR-0004-A` adds `shardloom.cdc_manifest_transaction_gate.v1` to the table-intelligence
  and CDC incremental CLI surfaces, classifying CDC read intent as report-only while CDC write
  intent, generalized manifest serialization, manifest metadata reads, object-store commits,
  table/catalog commits, and transaction execution stay unsupported with deterministic info-level
  diagnostics, `claim_gate_status=not_claim_grade`, no I/O, no fallback, and no external engine.
- [ ] Table/catalog metadata reads, object-store commits, generalized manifest serialization, CDC
  write execution, and broad transaction semantics remain incomplete.

### RFC 0005 - Vortex-Native File IO and Output Contract

- Source: [`docs/rfcs/0005-vortex-native-file-io-output.md`](../rfcs/0005-vortex-native-file-io-output.md)
- Current read: Vortex is first-class. GAR-0005-A exposes scoped local reader/writer coverage, and
  GAR-0005-B exposes object-store/upstream-write admission blockers, but broad runtime support
  remains gated.
- Evidence: `shardloom-vortex/src/file_io.rs`, `shardloom-vortex/src/metadata_async_boundary.rs`,
  `shardloom-vortex/src/read_planning.rs`, `shardloom-vortex/src/write_intent.rs`,
  `shardloom-vortex/src/output_payload.rs`, `shardloom-vortex/src/adapter.rs`,
  `shardloom-cli/src/vortex_planning.rs`, `shardloom-cli/src/object_store_planning.rs`,
  `shardloom-cli/tests/vortex_api_inventory_snapshots.rs`,
  `shardloom-cli/tests/object_store_request_plan_snapshots.rs`, `shardloom-cli/Cargo.toml`,
  `shardloom-vortex/Cargo.toml`
- [x] Vortex-native file I/O, metadata-first planning, staged output, and write-intent surfaces
  exist.
- [x] Feature-gated Vortex write support is explicitly separated from unsupported paths.
- [x] GAR-0005-A adds `shardloom.vortex_local_io_coverage.v1` through `vortex-api-inventory`,
  classifying the scoped local primitive scan reader lane, the feature-gated native CountAll output
  payload writer lane, broad local writer blockers, claim boundaries, and no-fallback/no-external
  engine fields.
- [x] GAR-0005-B adds `shardloom.vortex_object_store_io_gate.v1` through `vortex-api-inventory`
  and `object-store-request-plan`, classifying object-store Vortex read/write providers,
  credentials, range request budgets, write idempotency, upstream sink API evidence, Native I/O
  certificate requirements, and unsupported diagnostics as report-only/unsupported with no network
  I/O, no credentials, no writes, no fallback, and `claim_gate_status=not_claim_grade`.
- [ ] Broad Vortex reader/writer execution, object-store Vortex I/O execution, general
  schema/encoding writes, table/catalog integration, lakehouse output, and production writer claims
  remain incomplete.

### RFC 0006 - Statistics, Pruning, and Metadata-Only Execution

- Source:
  [`docs/rfcs/0006-statistics-pruning-metadata-only-execution.md`](../rfcs/0006-statistics-pruning-metadata-only-execution.md)
- Current read: Narrow metadata-only and pruning paths exist, and the compute capability matrix
  now exposes predicate/DType/null/nested/statistics coverage rows with explicit support status,
  evidence gaps, deterministic unsupported diagnostics, and no-fallback/no-external-engine fields.
- Evidence: `shardloom-core/src/encoded.rs`, `shardloom-vortex/src/metadata_pruning.rs`,
  `shardloom-vortex/src/metadata_executor.rs`,
  `shardloom-vortex/src/metadata_physical_kernel.rs`,
  `shardloom-vortex/src/encoded_read_executor.rs`, `shardloom-vortex/src/query_trace.rs`,
  `shardloom-cli/src/status_capabilities.rs`, `python/src/shardloom/client.py`,
  `shardloom-cli/tests/compute_capability_matrix_snapshots.rs`
- [x] Metadata pruning, metadata-only execution reporting, and encoded read readiness are present.
- [x] CLI snapshots preserve metadata and materialization/decode evidence for supported rows.
- [x] GAR-0006-A adds the predicate, DType, nested, null, and statistics coverage matrix for the
  current runtime posture.
- [ ] Claim-grade broad predicate, DType, nested, null, and production metadata-only runtime
  coverage remains incomplete.

### RFC 0007 - Translation Layer Contract

- Source: [`docs/rfcs/0007-translation-layer-contract.md`](../rfcs/0007-translation-layer-contract.md)
- Current read: Translation/report contracts exist, and GAR-0007-A/B now makes current writer
  support explicit. Local fixture-smoke compatibility writers exist for the traditional analytics
  path; production sinks and lakehouse table commits remain gated.
- Evidence: `shardloom-core/src/translation.rs`, `shardloom-cli/src/vortex_planning.rs`,
  `shardloom-contract-tests/tests/fidelity_invariants.rs`,
  `shardloom-vortex/src/traditional_analytics.rs`,
  `benchmarks/traditional_analytics/README.md`
- [x] Plan/report contracts distinguish Vortex native, compatibility export, and unsupported paths.
- [x] Compatibility surfaces preserve no-fallback and evidence terminology.
- [x] GAR-0007-A adds `shardloom.compatibility_output_writer_matrix.v1` through
  `translation-plan`, classifying native Vortex reference output, local fixture-smoke compatibility
  output rows, Iceberg/Delta table-commit blockers, metadata-loss posture, feature gates,
  implementation/evidence refs, and no-fallback/no-external-engine fields.
- [x] GAR-0007-B hardens the existing feature-gated traditional analytics writer smoke so CSV,
  JSONL, Parquet, Arrow IPC, Avro, and ORC local compatibility outputs are written non-empty,
  replayed, and kept separate from production sink claims.
- [ ] Production output sink APIs, object-store output, broad user-facing write methods,
  Iceberg/Delta table commit semantics, catalog integration, lakehouse transaction support, and
  performance/superiority claims remain incomplete.

### RFC 0008 - Object-Store Runtime and Distributed Task Model

- Source:
  [`docs/rfcs/0008-object-store-runtime-distributed-tasks.md`](../rfcs/0008-object-store-runtime-distributed-tasks.md)
- Current read: Planning model exists, including a byte-range provider gate that blocks runtime
  reads until provider, credential, retry, idempotency, execution-certificate, Native I/O, and
  benchmark evidence exist, plus an object-store runtime blocker matrix for coordinator, worker,
  task, checkpoint, retry, cleanup, and commit-record actions. Runtime is not implemented.
- Evidence: `shardloom-plan/src/object_store.rs`, `shardloom-cli/src/object_store_planning.rs`,
  `shardloom-cli/tests/object_store_request_plan_snapshots.rs`,
  `shardloom-cli/tests/cg10_object_store_runtime_gate.rs`,
  `docs/architecture/object-store-request-planner.md`,
  `docs/architecture/rfc-phase-traceability.md`
- [x] Byte-range, task, retry, checkpoint, and commit planning contracts exist.
- [x] Object-store surfaces are explicitly report-only where runtime execution is absent.
- [x] GAR-0008-A adds the byte-range provider gate with credential, retry, idempotency, provider
  probe, no-I/O, no-fallback, and claim-boundary fields.
- [x] GAR-0008-B adds the coordinator/worker/task/checkpoint/retry/cleanup/commit blocker matrix
  with deterministic diagnostic codes, required evidence, no-I/O, no-fallback, and no-external-engine
  fields.
- [ ] Object-store I/O providers, probes, coordinator/worker runtime, checkpoint writes, retry
  execution, distributed execution, and object-store commits remain incomplete.

### RFC 0009 - Benchmark Methodology and Spark-Displacement Workloads

- Source:
  [`docs/rfcs/0009-benchmark-methodology-spark-displacement.md`](../rfcs/0009-benchmark-methodology-spark-displacement.md)
- Current read: Methodology and local harness exist; replacement claims remain not claimable.
- Evidence: `shardloom-core/src/benchmark.rs`, `shardloom-core/src/benchmark_suite.rs`,
  `benchmarks/traditional_analytics/run.py`, `benchmarks/common/scenario_catalog.json`,
  `shardloom-cli/tests/traditional_benchmark_harness.rs`
- [x] Benchmark scenario catalog, methodology reports, and harness snapshots exist.
- [x] External engines are labeled as comparison-only baselines, not execution fallback.
- [ ] Broad claim-grade Spark-displacement evidence and public performance claims remain gated.

### RFC 0010 - Developer Experience, API Ergonomics, and Agent Usability

- Source:
  [`docs/rfcs/0010-developer-experience-agent-usability.md`](../rfcs/0010-developer-experience-agent-usability.md)
- Current read: CLI/Python/report ergonomics and typed capability posture accessors are in place;
  mature runtime APIs are not.
- Evidence: `shardloom-cli/src/main.rs`, `shardloom-core/src/output.rs`,
  `shardloom-core/src/wrapper_architecture.rs`, `python/src/shardloom/context.py`,
  `python/tests/test_cli_client.py`, `docs/architecture/rfc-coverage-followthrough.md`
- [x] CLI JSON, Python wrapper, typed outputs, and agent-facing contract packs exist.
- [x] `GAR-0010-A` adds Python `CapabilityPosture` accessors for support, claim gate, runtime,
  data/write/object-store/catalog I/O, no-fallback, external-engine, blockers, and required
  evidence so Python users can inspect capability state without scraping CLI text.
- [x] Deterministic unsupported diagnostics preserve no-fallback semantics.
- [ ] `GAR-DOCS-1` adds the non-expert Use Case Atlas follow-through. The atlas must keep
  `ready_local`, `smoke_supported`, `report_only`, `planned`, `blocked`, and `unsupported` statuses
  distinct, map every public capability family to references and claim boundaries, and prevent
  planned/blocked paths from being mistaken for runtime support.
- [ ] Mature ergonomic runtime APIs, DataFrame/notebook surfaces, REST runtime, and user-facing
  package publication remain incomplete.

### RFC 0011 - Modular Extensibility for SQL, UDFs, Unstructured Data, LLM Calls, API Calls, and Embeddings

- Source:
  [`docs/rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md`](../rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md)
- Current read: Manifest/report contracts exist; effectful execution is not implemented.
- Evidence: `shardloom-core/src/extension.rs`, `shardloom-core/src/effect_budget.rs`,
  `shardloom-core/src/unstructured_workflow.rs`, `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`
- [x] Extension manifests, permissions, provenance, effect budgets, and materialization metadata are
  represented.
- [x] Tests lock the current non-executing extension posture.
- [ ] Extension execution, UDF execution, LLM/API calls, embeddings, and external effects remain
  unsupported/report-only.

### RFC 0012 - Diagnostics, Explain, Estimate, Doctor, and Capability Discovery

- Source:
  [`docs/rfcs/0012-diagnostics-explain-estimate-capabilities.md`](../rfcs/0012-diagnostics-explain-estimate-capabilities.md)
- Current read: Diagnostic/report surfaces are implemented for current commands; the workflow
  unsupported family now uses helper-backed category normalization for invalid-input, unsupported,
  materialization, object-store, and no-fallback diagnostics, and the CG-10 object-store/distributed
  runtime blockers now propagate through JSON/text/Python boundaries.
- Evidence: `shardloom-core/src/diagnostics.rs`, `shardloom-core/src/capabilities.rs`,
  `shardloom-plan/src/explain.rs`, `shardloom-plan/src/estimate.rs`,
  `shardloom-cli/src/object_store_planning.rs`, `shardloom-cli/src/workflow_planning.rs`,
  `shardloom-cli/tests/cg10_object_store_runtime_gate.rs`,
  `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`,
  `shardloom-cli/tests/workflow_query_builder_plan_snapshots.rs`, `python/src/shardloom/client.py`,
  `python/tests/test_cli_client.py`
- [x] Typed JSON/text diagnostics, explain, estimate, doctor, and capability surfaces exist.
- [x] No-fallback status appears in envelopes and snapshots.
- [x] `GAR-0012-A` normalizes one end-user command family onto stable diagnostic helper categories.
- [x] `GAR-0012-B` propagates CG-10 object-store/distributed runtime blocker diagnostics through
  info-level JSON envelope diagnostics, text summary fields, and the Python typed result view.

### RFC 0013 - Streaming, Zero-Copy, Zero-Decode, and Boundary Interoperability

- Source:
  [`docs/rfcs/0013-streaming-zero-copy-boundary-interoperability.md`](../rfcs/0013-streaming-zero-copy-boundary-interoperability.md)
- Current read: Streaming contracts and a GAR-0013 capability matrix exist; full streaming runtime
  is planned work.
- Evidence: `shardloom-exec/src/streaming.rs`, `shardloom-plan/src/plan_ir.rs`,
  `shardloom-vortex/src/lib.rs`, `shardloom-cli/tests/streaming_batch_plan_snapshots.rs`
- [x] Streaming plan, backpressure, encoded streaming batch, zero-decode, and boundary contracts
  exist.
- [x] Report surfaces expose representation and materialization boundaries.
- [x] `GAR-0013-A` exposes a streaming capability matrix across local streaming, object-store
  streaming reads, zero-copy, zero-decode, bounded backpressure, and live/hybrid broker-backed
  runtime with deterministic blocked/materialization diagnostics and no-fallback/no-external-engine
  fields.
- [ ] Full streaming runtime and object-store streaming reads remain gated/report-only.

### RFC 0014 - Memory Management, Spill, and OOM Safety

- Source:
  [`docs/rfcs/0014-memory-management-spill-oom-safety.md`](../rfcs/0014-memory-management-spill-oom-safety.md)
- Current read: Memory and spill contracts exist; broad runtime enforcement is incomplete.
- Evidence: `shardloom-exec/src/memory.rs`, `shardloom-exec/src/spill_lifecycle.rs`,
  `shardloom-exec/src/spill_payload.rs`, `shardloom-vortex/src/memory_bridge.rs`
- [x] Bounded memory, reservations, spill lifecycle, spill payload, and Vortex memory bridge
  surfaces exist.
- [x] CLI/report surfaces describe OOM/spill posture for supported slices.
- [x] GAR-0014-A closes the spill/OOM enforcement promotion gate through
  `cg14-memory-runtime-hardening-gate`: the report carries `gar_id=GAR-0014-A`,
  `support_status=report_only`, `claim_gate_status=not_claim_grade`,
  reservation-release/native-spill-read-write/cleanup/allocator/fail-before-OOM evidence refs,
  spill artifact path-safety refs, and no runtime execution, no spill I/O, no external engine, and
  no fallback.
- [ ] Actual runtime spill/OOM production enforcement remains limited to synthetic or local
  constraints beyond the promotion gate.

### RFC 0015 - Correctness, Semantics, Differential Testing, and Fuzzing

- Source:
  [`docs/rfcs/0015-correctness-semantics-differential-testing-fuzzing.md`](../rfcs/0015-correctness-semantics-differential-testing-fuzzing.md)
- Current read: Correctness contracts and local evidence exist; fuzz breadth remains limited.
- Evidence: `shardloom-core/src/correctness.rs`, `shardloom-contract-tests`,
  `shardloom-contract-tests/tests/no_fallback_invariants.rs`
- [x] Fixtures, semantic profiles, differential harness surfaces, and no-fallback tests exist.
- [x] Local primitive certificates support scoped correctness evidence.
- [ ] Fuzz/property expansion and claim-grade benchmark superiority coverage remain incomplete.

### RFC 0016 - Optimizer, Adaptive Execution, Runtime Filters, and Skew Handling

- Source:
  [`docs/rfcs/0016-optimizer-adaptive-execution-runtime-filters-skew.md`](../rfcs/0016-optimizer-adaptive-execution-runtime-filters-skew.md)
- Current read: Optimizer/adaptive report contracts exist; runtime adaptivity is incomplete.
- Evidence: `shardloom-plan/src/optimizer.rs`, `shardloom-cli/src/optimizer_planning.rs`,
  `shardloom-cli/tests/adaptive_optimizer_memory_snapshots.rs`, `shardloom-exec/src/sizing.rs`,
  `shardloom-core/src/manifest.rs`
- [x] Optimizer, sizing, adaptive planning, dynamic work-shaping, layout health, and compaction
  planning surfaces exist.
- [x] Metadata-driven reports avoid claiming unsupported runtime behavior.
- [x] GAR-0016-A: `optimizer-adaptive-memory-plan` now exposes a report-only adaptive runtime gate
  with stable runtime-filter, dynamic-pruning, skew, adaptive-parallelism, and compaction-write
  blocker fields plus `support_status=report_only`, `claim_gate_status=not_claim_grade`,
  `fallback_attempted=false`, and `external_engine_execution=false`.
- [x] `GAR-PERF-2B` adds the report-only evidence-aware logical optimizer rule registry and trace
  for predicate/projection/slice pushdown, common subplan/source-state reuse, expression
  simplification, constant folding, type coercion, join ordering, and cardinality estimation.
  Current rows apply no rewrites, use report-only plan-digest placeholders, preserve no-fallback
  fields, and keep `claim_gate_status=not_claim_grade`.
- [ ] Runtime adaptive execution, runtime filters, skew handling, and compaction writes remain
  incomplete.

### RFC 0017 - Fault Tolerance, Cancellation, and Recovery

- Source:
  [`docs/rfcs/0017-fault-tolerance-cancellation-recovery.md`](../rfcs/0017-fault-tolerance-cancellation-recovery.md)
- Current read: Recovery and commit contracts exist; broad execution is incomplete.
- Evidence: `shardloom-exec/src/recovery.rs`, `shardloom-cli/src/operational_hardening.rs`,
  `shardloom-cli/tests/fault_tolerance_promotion_gate.rs`, `shardloom-vortex/src/commit_intent.rs`,
  `shardloom-vortex/src/commit_protocol.rs`
- [x] Recovery, cleanup, retry, cancellation, commit-intent, and commit-protocol reports exist.
- [x] CLI gates distinguish planned recovery from executed recovery.
- [x] GAR-0017-A: `fault-tolerance-promotion-gate` now separates request validation,
  cancellation signal, retry allowance, checkpoint write, cleanup execution, and commit execution
  with deterministic report-only blockers, no-effect booleans, and `fallback_attempted=false`.
- [ ] Broad retry, cancellation, and commit execution remain incomplete.

### RFC 0018 - Observability, Tracing, Profiling, and Runtime Introspection

- Source:
  [`docs/rfcs/0018-observability-tracing-profiling.md`](../rfcs/0018-observability-tracing-profiling.md)
- Current read: Observability schema and report surfaces exist.
- Evidence: `shardloom-core/src/observability.rs`, `shardloom-cli/src/diagnostics.rs`,
  `shardloom-cli/tests/observability_schema_coverage.rs`, `shardloom-vortex/src/query_trace.rs`,
  `shardloom-vortex/src/runtime_utilization.rs`
- [x] Trace schema, profile/runtime report commands, and Vortex query trace evidence exist.
- [x] Observability tests cover current report contracts.
- [x] GAR-0018-A: `runtime-report` now exposes a report-only local benchmark stage-timing
  introspection schema, unsupported live profiling and distributed introspection blockers, and
  no-effect/no-fallback fields.
- [ ] `GAR-NOVEL-1C` adds the planned OpenTelemetry execution trace export contract. The next work
  is report-only span/attribute mapping for request admission, source read, compatibility parse,
  Vortex import/scan, operator compute, result sink, evidence render, and claim gate with export
  opt-in, no default network exporter, and secret/path/query redaction.
- [ ] Live profiling collectors, profile artifacts, debug bundles, metrics exporters, trace
  backends, and distributed runtime introspection execution remain incomplete.

### RFC 0019 - Security, Secrets, Governance, and Agent Safety

- Source:
  [`docs/rfcs/0019-security-secrets-governance-agent-safety.md`](../rfcs/0019-security-secrets-governance-agent-safety.md)
- Current read: Report-level security posture exists; production enforcement remains incomplete.
- Evidence: `shardloom-core/src/security.rs`, `docs/security/threat-model.md`,
  `docs/security/runtime-exploit-regression-suite.md`,
  `shardloom-contract-tests/tests/release_readiness_metadata.rs`
- [x] Security/governance reports, secrets-unloaded defaults, and side-effect-free agent/dry-run
  posture exist.
- [x] Release metadata and security docs record no-fallback and governance evidence.
- [ ] Credential lifecycle, runtime policy enforcement, sandbox execution, and production
  governance remain incomplete.

### RFC 0020 - Schema Evolution, Catalog Integration, and Table Compatibility

- Source:
  [`docs/rfcs/0020-schema-evolution-catalog-table-compatibility.md`](../rfcs/0020-schema-evolution-catalog-table-compatibility.md)
- Current read: Typed reports, a report-only catalog/table metadata admission gate, and one scoped
  local manifest-backed metadata smoke exist; broad table/catalog runtime integration is
  incomplete.
- Evidence: `shardloom-core/src/schema.rs`, `shardloom-core/src/table_intelligence.rs`,
  `shardloom-cli/tests/table_intelligence_plan_snapshots.rs`,
  `shardloom-cli/tests/cg9_catalog_metadata_gate.rs`,
  `shardloom-cli/tests/local_table_metadata_read_smoke.rs`,
  `shardloom-cli/tests/local_delete_tombstone_read_smoke.rs`,
  `shardloom-cli/tests/local_append_only_cdc_overlay_smoke.rs`
- [x] Schema, partition, delete/tombstone, and aggregate table evidence reports exist.
- [x] Current table-intelligence surfaces are no-IO and typed.
- [x] GAR-0020-A: `CatalogMetadataIntegrationGateReport` exposes deterministic, report-only
  admission diagnostics for catalog resolution, snapshot/manifest reads, table metadata reads,
  partition/delete/CDC metadata reads, external table-format dependency admission, commit recovery
  metadata binding, and metadata cache invalidation with `support_status=unsupported`,
  `claim_gate_status=not_claim_grade`, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] GAR-0020-C: `LocalTableMetadataReadSmokeReport` and
  `local-table-metadata-read-smoke` expose one runtime-supported, in-memory local manifest-backed
  table metadata summary with scoped evidence refs, deterministic blocked-path diagnostics,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [x] GAR-0020-B: `TableMaintenanceExecutionMatrixReport` is embedded in
  `table-intelligence-plan` and classifies delete/tombstone, CDC, compaction, table metadata write,
  and table-maintenance commit lanes with `report_only_available` or
  `unsupported_until_certified` status, required fixture/commit/evidence fields,
  deterministic unsupported diagnostics, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] GAR-0020-D: `LocalDeleteTombstoneReadSmokeReport` and
  `local-delete-tombstone-read-smoke` expose one fixture-smoke-only, in-memory local manifest path
  that applies file-level delete and segment tombstone admission, emits a correctness digest, keeps
  row/position/equality/CDC/object-store/table-format delete models deterministically blocked, and
  reports `fallback_attempted=false` and `external_engine_invoked=false`.
- [x] GAR-0020-E: `LocalAppendOnlyCdcOverlaySmokeReport` and
  `local-append-only-cdc-overlay-smoke` expose one fixture-smoke-only, in-memory local append-only
  CDC overlay path that combines base rows plus append-delta rows, emits a correctness digest, keeps
  update/delete/tombstone CDC plus manifest/transaction/commit paths deterministically blocked, and
  reports `fallback_attempted=false` and `external_engine_invoked=false`.
- [ ] Broad catalog/table metadata integration, real table data I/O, delete/tombstone execution,
  CDC execution, maintenance writes, and table/lakehouse commits remain incomplete; the current
  matrix keeps those lanes unsupported until evidence-bearing promotion slices such as `GAR-0028-A`
  are completed.

### RFC 0021 - Expression Engine and Kernel Registry

- Source:
  [`docs/rfcs/0021-expression-engine-kernel-registry.md`](../rfcs/0021-expression-engine-kernel-registry.md)
- Current read: Registry contracts and narrow kernels exist.
- Evidence: `shardloom-core/src/expression.rs`, `shardloom-core/src/approx_sketch.rs`,
  `shardloom-cli/tests/cg20_approx_sketch_gate.rs`,
  `shardloom-core/src/physical_operator_kernel_contracts.rs`,
  `shardloom-cli/tests/kernel_registry_snapshots.rs`,
  `shardloom-vortex/src/encoded_count_physical_kernel.rs`
- [x] Native expression and kernel registry domain types, diagnostics, and admission reports exist.
- [x] Narrow physical kernels such as encoded count have evidence-backed slices.
- [x] GAR-0021-A: `cg20-approx-sketch-gate` now exposes a report-only approximate/sketch admission
  contract with GAR/support/claim fields, deterministic unsupported status, required evidence
  booleans, and no-fallback/no-external-engine fields.
- [ ] Broad expression execution, full function/kernel coverage, and UDF/effectful expression
  runtime remain incomplete.

### RFC 0022 - Plan IR and Substrait-Compatible Interoperability

- Source:
  [`docs/rfcs/0022-plan-ir-substrait-compatible-interoperability.md`](../rfcs/0022-plan-ir-substrait-compatible-interoperability.md)
- Current read: Native Plan IR exists; real Substrait import/export execution is not implemented.
- Evidence: `shardloom-plan/src/plan_ir.rs`, `shardloom-cli/src/workflow_planning.rs`,
  `shardloom-cli/tests/plan_portability_snapshots.rs`
- [x] Native-first Plan IR, serialization skeletons, and imported-plan capability gates exist.
- [x] Imported plan surfaces preserve no-fallback and capability diagnostics.
- [x] `GAR-PERF-2B` adds optimizer trace follow-through over Plan IR. Current trace rows keep
  report-only before/after plan-digest placeholders, rewrite safety, evidence preservation,
  materialization boundaries, no-fallback status, and claim gates visible, and no rewrite is treated
  as runtime-supported.
- [ ] Real Substrait import/export and imported-plan execution remain incomplete.

### RFC 0023 - Extension, Plugin ABI, and Sandboxing

- Source:
  [`docs/rfcs/0023-extension-plugin-abi-sandboxing.md`](../rfcs/0023-extension-plugin-abi-sandboxing.md)
- Current read: Manifest-first ABI reports exist; runtime plugin loading is not implemented.
- Evidence: `shardloom-core/src/extension.rs`, `shardloom-cli/tests/plan_only_invariants.rs`,
  `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`
- [x] Extension metadata, permissions, effect declarations, sandbox posture, and provenance are
  represented.
- [x] Tests lock the current non-executing inspection posture.
- [ ] Real plugin ABI loading, sandbox runtime, and UDF execution remain incomplete.

### RFC 0024 - Release Engineering, API Compatibility, and Packaging

- Source:
  [`docs/rfcs/0024-release-engineering-api-compatibility-packaging.md`](../rfcs/0024-release-engineering-api-compatibility-packaging.md)
- Current read: Release gates and dry-run evidence exist; publication is not complete.
- Evidence: `shardloom-core/src/release.rs`, `shardloom-cli/src/packaging_deployment.rs`,
  `scripts/check_release_readiness.py`, `scripts/release_dry_run_proof.py`,
  `scripts/release_provenance_dry_run.py`, `scripts/run_release_validation_evidence.py`
- [x] Release-readiness, provenance, SBOM, security, packaging, and no-fallback gate evidence
  surfaces exist.
- [x] Local dry-run workflows avoid package publication and external side effects.
- [ ] `GAR-COMMERCIAL-1A` and `GAR-COMMERCIAL-1B` add adoption and package-channel readiness
  follow-through: one documented local smoke path plus a channel matrix for GitHub pre-release,
  TestPyPI, PyPI, Homebrew, Scoop/winget, conda-forge, GHCR, and future public Rust API crates. No
  channel may be marked ready without install/uninstall, clean install, smoke, SBOM/checksum,
  provenance, and rollback/yank evidence; PyPI must use Trusted Publisher/OIDC.
- [ ] First public release/package publication, stable API/schema windows, and signing decisions
  remain incomplete.

### RFC 0025 - Competitive Engine Track and No-Fallback Replacement Strategy

- Source:
  [`docs/rfcs/0025-competitive-engine-track-no-fallback-replacement.md`](../rfcs/0025-competitive-engine-track-no-fallback-replacement.md)
- Current read: Competitive gates are defined; replacement claims remain gated.
- Evidence: `docs/architecture/phased-execution-plan.md`,
  `docs/architecture/rfc-phase-traceability.md`,
  `docs/architecture/benchmark-competitive-claim-evidence.md`,
  `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`
- [x] Competitive-gate language, no-fallback requirements, and claim publication blockers are
  represented.
- [x] Claim publication is tied to workload-specific evidence rather than broad marketing language.
- [ ] Full competitive replacement remains incomplete until correctness, benchmark, Native I/O,
  certificate, capability, and no-fallback evidence is broad enough.

### RFC 0026 - Encoded-Native Reads, Query Primitives, and Compressed Execution

- Source:
  [`docs/rfcs/0026-encoded-native-reads-query-primitives-compressed-execution.md`](../rfcs/0026-encoded-native-reads-query-primitives-compressed-execution.md)
- Current read: Encoded read boundary, local query primitives, and scoped prepared/native
  filter-project-limit, grouped aggregate, multi-key group-by, hash-join, join-aggregate, distinct
  count, null-heavy aggregate, clean/cast/filter/write, malformed timestamp / dirty CSV, nested JSON
  field scan, CDC-overlay small change over large base, global sort/top-k,
  top-N-per-group/row-number/string-group/date-range scan execution exist.
- Evidence: `shardloom-vortex/src/encoded_read_api.rs`,
  `shardloom-vortex/src/encoded_read_boundary.rs`,
  `shardloom-vortex/src/encoded_read_executor.rs`,
  `shardloom-vortex/src/encoded_path_selection.rs`,
  `shardloom-vortex/src/generalized_encoded_filter_execution.rs`,
  `shardloom-vortex/src/traditional_analytics.rs`,
  `benchmarks/traditional_analytics/README.md`
- [x] Vortex encoded-read boundary, local encoded count, path selection, and query primitive
  evidence exist.
- [x] Evidence records zero-decode, no-materialization, and no-fallback fields for scoped paths.
- [x] Scoped prepared/native `filter + projection + limit` uses Vortex scan filter/projection
  pushdown and bounded top-N state without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `group by aggregation` uses Vortex scan projection pushdown over
  `group_key`/`metric` and ShardLoom-native grouped residual state without full fact-table
  materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `multi-key group by` uses Vortex scan projection pushdown over
  `group_key`/`category`/`metric` and ShardLoom-native composite-key residual state without full
  fact-table materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `hash join` uses Vortex scan projection pushdown over dimension
  `dim_key`/`dim_label` and fact `dim_key`/`metric`, then ShardLoom-native bounded dimension state
  and residual grouped join output without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `join + aggregate` uses Vortex scan projection/filter pushdown over
  dimension `dim_key`/`dim_label` and fact `dim_key`/`category`/`metric` with a fact-side
  `value >= 2500` filter, then ShardLoom-native bounded dimension state and residual grouped
  aggregation without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `top-N per group` uses Vortex scan projection pushdown over
  `group_key`/`id`/`metric`, then bounded ShardLoom-native per-group ranking state without full
  fact-table materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `row number window` uses Vortex scan projection pushdown over
  `group_key`/`id`/`metric`, then bounded ShardLoom-native rank-1 per-group state without full
  fact-table materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `high-cardinality string group/distinct` uses Vortex scan projection
  pushdown over `category`/`metric`, then ShardLoom-native string grouping state without full
  fact-table materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `distinct count` uses Vortex scan projection pushdown over `category`,
  then ShardLoom-native distinct state without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `null-heavy aggregate` uses Vortex scan projection pushdown over
  `nullable_metric_00`, then ShardLoom-native null-skipping aggregate state without full fact-table
  materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `clean/cast/filter/write` uses Vortex scan projection pushdown over
  `raw_event_time`, `dirty_numeric`, and `dirty_flag`, then ShardLoom-native cleanup/filter/aggregate
  state without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `malformed timestamp / dirty CSV` uses Vortex scan projection pushdown
  over `raw_event_time` and `dirty_numeric`, then ShardLoom-native validation/parse/aggregate state
  without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `nested JSON field scan` uses Vortex scan projection pushdown over
  `nested_payload`, then ShardLoom-native generated-field extraction state without full fact-table
  materialization while preserving `operator_encoded_native_claim_allowed=false`.
- [x] Scoped prepared/native `small change over large base` uses Vortex scan projection pushdown
  over base `id`/`metric` plus CDC delta `id`/`op`/`value`/`metric`/`effective_ts`, then
  ShardLoom-native overlay state without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`.
- [x] Prepared/native rows emit explicit `source_backed_scan_*` evidence for local source roles,
  source refs/digests, projected columns, provider scope, Native I/O certificate status,
  materialization boundary rows, residual executor, `fallback_attempted=false`, and
  `external_engine_invoked=false`; this remains scoped scan evidence, not a generalized source API
  runtime or encoded-native operator claim.
- [x] Prepared/native `selective filter` rows emit explicit `encoded_predicate_provider_*` blocker
  fields with `flag,value` filter-only columns, `metric` projected output, required future
  evidence, and no-fallback/no-external-engine status; Vortex scan filter pushdown is not reported
  as an admitted encoded predicate provider.
- [x] GAR-0026-R adds reader-backed bridge follow-through for that row: non-empty filtered scans
  record projected reader chunks such as `metric:vortex.filter`, zero-result scans report no reader
  chunks, and filter-only `flag,value` batches remain unclaimed before encoded-native predicate
  support can be claimed.
- [x] GAR-0026-S adds the ShardLoom-native reader-generated conjunctive selection-vector bridge for
  supplied encoded kernel inputs. Those rows distinguish the available bridge contract from
  benchmark-row provider blockers while preserving
  `encoded_predicate_provider_encoded_native_claim_allowed=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [x] GAR-0026-T updates prepared/native `selective filter` rows to v4 provider fields and adds a
  scoped local filter-column probe. The probe observes real `flag,value` reader chunks without
  decode/materialization, records `flag:fastlanes.bitpacked` and `value:vortex.sequence`, and keeps
  the conjunctive bridge blocked before selection-vector intersection until those encodings have
  admitted kernel-input lowering and certificates.
- [x] GAR-0026-U adds scoped encoding-specific lowering for observed `flag:fastlanes.bitpacked` and
  `value:vortex.sequence` filter-column chunks. Prepared/native `selective filter` rows can now
  report `encoded_predicate_provider_kernel_input_count=2`,
  `encoded_predicate_provider_conjunctive_bridge_status=intersected_selection_vectors`, and
  `encoded_predicate_provider_selection_vector_intersection_status=selection_vectors_intersected`
  while preserving `encoded_predicate_provider_operator_execution_class=residual_native`,
  `encoded_predicate_provider_encoded_native_claim_allowed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`. This was the prerequisite for scoped selected-metric
  aggregation, not a broad encoded-native predicate claim.
- [x] GAR-0026-V consumes the admitted conjunctive bridge selection vector for the scoped
  prepared/native `selective filter` metric aggregation. Rows can now report
  `encoded_predicate_provider_status=reader_generated_filter_column_batches_and_selected_metric_aggregation_admitted`,
  `encoded_predicate_provider_selected_metric_aggregation_status=selection_vector_consumed`,
  selected row count, selected metric sum, scan split count, and decode/materialization boundary
  fields while preserving `encoded_predicate_provider_operator_execution_class=residual_native`,
  `encoded_predicate_provider_encoded_native_claim_allowed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] Scoped prepared/native `partition pruning` uses Vortex scan projection/filter pushdown over
  `event_date`/`metric` with a local date-range predicate, then ShardLoom-native residual scalar
  aggregation without full fact-table materialization while preserving
  `operator_encoded_native_claim_allowed=false`. This is not object-store partition-pruning,
  layout-pruning, or statistics-pruning evidence.
- [x] Scoped prepared/native `sort and top-k` uses Vortex scan projection pushdown over
  `id`/`metric`, then bounded ShardLoom-native global top-k state without full fact-table
  materialization while preserving `operator_encoded_native_claim_allowed=false`. This is not
  distributed sort, encoded-native sort/top-k, or performance/superiority evidence.
- [ ] Generalized direct encoded count/filter/project execution and production compressed-execution
  claims remain incomplete.

### RFC 0027 - CPU Vectorized Kernels, Streaming, and Runtime Adaptivity

- Source:
  [`docs/rfcs/0027-cpu-vectorized-kernels-streaming-runtime-adaptivity.md`](../rfcs/0027-cpu-vectorized-kernels-streaming-runtime-adaptivity.md)
- Current read: Report surfaces exist, including side-effect-free host CPU feature probing and a
  blocked filter/encoded vector-kernel admission diagnostic; real vectorized dispatch and runtime
  adaptivity are not broad.
- Evidence: `shardloom-core/src/cpu_specialization.rs`,
  `shardloom-cli/src/optimizer_planning.rs`,
  `shardloom-cli/tests/cpu_specialization_snapshots.rs`,
  `shardloom-vortex/src/streaming_batch_runtime.rs`
- [x] CPU specialization, streaming, sizing, and runtime-promotion evidence surfaces exist.
- [x] Current reports keep production and fallback claims not claimable.
- [x] `cpu-specialization-plan` records deterministic host CPU architecture/feature labels and a
  filter/encoded vector-kernel admission status without dispatching CPU-specialized kernels,
  reading data, invoking unsafe code, or attempting fallback.
- [ ] Real SIMD/vectorized dispatch, production vectorized kernel path, adaptive parallelism
  runtime, and broad streaming runtime remain incomplete.

### RFC 0028 - Output Payloads, Manifest Finalization, Commit Execution, and Lakehouse Semantics

- Source:
  [`docs/rfcs/0028-output-payloads-finalization-commit-lakehouse.md`](../rfcs/0028-output-payloads-finalization-commit-lakehouse.md)
- Current read: Local Vortex staged-output slice exists; lakehouse semantics are incomplete.
- Evidence: `shardloom-vortex/src/output_payload.rs`, `shardloom-vortex/src/staged_output.rs`,
  `shardloom-vortex/src/staged_manifest.rs`,
  `shardloom-vortex/src/manifest_finalization.rs`,
  `shardloom-vortex/src/commit_marker.rs`, `shardloom-vortex/src/commit_protocol.rs`,
  `shardloom-vortex/src/commit_execution_gate.rs`, `shardloom-cli/src/vortex_output_commit.rs`
- [x] Local Vortex staged output, manifest draft/finalization, commit marker, and commit evidence
  exist.
- [x] Commit execution gates keep unsupported object-store/table/catalog paths unsupported.
- [x] `GAR-0028-A` expands `CommitExecutionPromotionGateReport` into a deterministic RFC 0028
  object-store/lakehouse commit-semantics gate. It records `GAR-0028-A`,
  `support_status=report_only_with_blocked_runtime_paths`, `claim_gate_status=not_claim_grade`,
  existing local staged-output/manifest/commit/object-store/table-maintenance evidence refs, and
  info-level no-fallback diagnostics for generalized manifest serialization, generalized sink
  commit, object-store commit, table/catalog commit, lakehouse transaction commit, native
  source/sink commit, Foundry dataset transaction commit, upstream Vortex write API execution,
  live/hybrid checkpoint commit, and output-payload fidelity claims.
- [x] Unsupported RFC 0028 commit surfaces remain side-effect-free:
  `runtime_execution=false`, `manifest_write_io=false`, `write_io=false`, `object_store_io=false`,
  `catalog_io=false`, `upstream_vortex_write_api_invoked=false`, `external_engine_invoked=false`,
  `fallback_attempted=false`, and `fallback_execution_allowed=false`.

### RFC 0029 - Correctness, Benchmarks, Execution Certificates, and Stateful Reuse

- Source:
  [`docs/rfcs/0029-correctness-benchmarks-execution-certificates-stateful-reuse.md`](../rfcs/0029-correctness-benchmarks-execution-certificates-stateful-reuse.md)
- Current read: Workload-scoped evidence surfaces exist; global claims remain not claimable.
- Evidence: `shardloom-cli/src/evidence_certificates.rs`,
  `shardloom-cli/src/benchmark_planning.rs`,
  `shardloom-cli/src/workload_certification.rs`, `shardloom-core/src/stateful_reuse.rs`,
  `shardloom-cli/tests/cg17_stateful_reuse_gate.rs`
- [x] Execution certificates, benchmark rows, workload dossiers, and stateful reuse gate reports
  exist.
- [x] Evidence is workload-scoped and links to plan/input/output/artifact/native-I/O references.
- [ ] `GAR-NOVEL-1D` adds the planned Bayesian claim-confidence and regression model. The next work
  is an advisory/report-only schema for posterior runtime distribution, credible interval,
  probability of regression, minimum iterations for claim-grade consideration, and uncertainty
  reason. Bayesian output may block release/performance claims when uncertainty is high, but it
  cannot upgrade claim status alone.
- [x] `GAR-PERF-2A` adds scoped evidence-level runtime tiering for
  `traditional-analytics-vortex-batch-run`. Prepared/native batch rows now emit
  `evidence_level=minimal_runtime|certified|full_replay`, keep no-fallback and no-external-engine
  fields visible in every level, prevent `minimal_runtime` rows from becoming claim-grade by
  accident, and separate evidence overhead from execution-mode timing.
- [x] `GAR-PERF-2G` adds scoped allocation/resource-profile and buffer-pool blocker evidence.
  Session-backed prepared/native batch rows now report allocation profile status/scope, allocation
  count/byte/peak-RSS `not_available` statuses, `buffer_pool_enabled=false`,
  `buffer_reuse_count=0`, deterministic reuse blockers, correctness/evidence posture, no unsafe
  lifetime shortcuts, and no-fallback/no-external-engine fields.
- [ ] `GAR-IOREUSE-1` adds planned I/O reuse and cross-format fanout evidence. `GAR-IOREUSE-1A`,
  `GAR-IOREUSE-1B`, and `GAR-IOREUSE-1C` now establish SourceState, VortexPreparedState, and
  OutputPlan benchmark/report contracts for local source discovery/schema/parse posture, prepared
  artifact identity/digest/reuse posture, and scoped local Vortex result-sink output posture.
  `GAR-IOREUSE-1D` now adds a report-only fanout benchmark matrix for the required cross-format
  cases. `GAR-IOREUSE-1E` now adds a cache invalidation/fingerprint benchmark contract for current
  local source/prepared/plan/output posture. `GAR-IOREUSE-1F` now adds evidence-safe reuse-level
  rows so discovery, schema, parse-plan, prepared-state, operator-source-state, output-plan, and
  result-replay reuse stay separate from execution mode, evidence level, output format, and claim
  gate. The next work is runtime fanout, broader sink artifact proof, benchmark timing fields, and
  no-fallback/no-external-engine evidence so reuse cannot silently become claim-grade, cache-hit
  proof, or performance proof.
- [ ] `GAR-SCALE-1` adds the Spark-level scale contract and any-volume readiness follow-through.
  ShardLoom must not claim literal "any volume" support; future scale work must classify rows as
  `local_smoke`, `local_claim_grade`, `larger_than_memory_local`, `split_parallel_local`,
  `object_store_read_report_only`, `object_store_runtime`, `table_metadata_report_only`,
  `table_runtime`, `distributed_report_only`, `distributed_runtime`, `foundry_dev_stack_proof`, or
  `managed_platform_proof`. `GAR-SCALE-1A` now establishes the report-only
  `shardloom.traditional_analytics.scale_claim_gate.v1` row contract and keeps current rows limited
  to local smoke/local claim evidence with `scale_claim_gate_status=not_scale_grade`.
  `GAR-SCALE-1B` now adds the report-only
  `shardloom.traditional_analytics.split_manifest.v1` row contract with SplitManifest IDs/digests,
  SourceState linkage, split IDs, byte/row ranges, estimated rows/bytes, projection masks, filter
  pushdown posture, retry/runtime/row/spill/output refs, no-fallback fields, and
  `split_claim_gate_status=not_split_scale_grade`. `GAR-SCALE-1C` now adds the fail-closed
  `shardloom.traditional_analytics.memory_spill_backpressure.v1` row contract with memory budget,
  operator memory budget, peak memory, budget-exceeded, spill location/read/write/file/cleanup,
  backpressure, OOM-prevention, no-fallback fields, and
  `memory_spill_claim_gate_status=not_larger_than_memory_grade`. The remaining follow-through must
  add shuffle/repartition, object-store/table ladder, distributed report-only protocol, scale
  benchmark, and Foundry scale proof boundaries before any scale claim can be promoted beyond local
  evidence. Synthetic
  metadata-only evidence, report-only protocol rows, external baselines, and managed-platform
  orchestration cannot satisfy ShardLoom runtime scale claims, Spark-replacement claims, or
  no-fallback/no-external-engine proof.
- [ ] Broad CG-5/CG-6 coverage, production stateful reuse runtime, and performance/superiority
  claims remain incomplete.

### RFC 0030 - Universal API, Plan Portability, Import/Deployment, and External Baselines

- Source:
  [`docs/rfcs/0030-universal-api-plan-portability-import-deployment-baselines.md`](../rfcs/0030-universal-api-plan-portability-import-deployment-baselines.md)
- Current read: Universal API contracts exist; imported-plan runtime is not implemented.
- Evidence: `shardloom-core/src/output.rs`, `shardloom-cli/src/typed_envelope.rs`,
  `shardloom-cli/src/main.rs`, `shardloom-cli/tests/api_protocol_snapshots.rs`,
  `shardloom-cli/tests/plan_portability_snapshots.rs`, `python/tests/test_cli_client.py`
- [x] CLI JSON protocol, typed envelope, Python wrapper, plan import/export, and harness reports
  exist.
- [x] External engines remain comparison/oracle references only.
- [ ] `GAR-COMMERCIAL-1A` turns the source-local dry-run and first-10-minutes flow into a single
  adoption path for install/local build, smoke, generated-source posture, tiny prepared/native
  example, and evidence inspection without requiring architecture-doc reading.
- [ ] Imported-plan execution and universal harness execution remain unimplemented without capability,
  certificate, Native I/O, and no-fallback evidence.

### RFC 0031 - Universal Native I/O Envelope

- Source: [`docs/rfcs/0031-universal-native-io-envelope.md`](../rfcs/0031-universal-native-io-envelope.md)
- Current read: Native I/O envelope and provider terminology exist; coverage is not universal.
- Evidence: `shardloom-cli/src/evidence_certificates.rs`,
  `shardloom-vortex/src/vortex_compute_provider.rs`,
  `shardloom-vortex/src/vortex_scan_compatibility.rs`,
  `shardloom-vortex/src/vortex_compatibility.rs`,
  `shardloom-vortex/src/runtime_utilization.rs`,
  `shardloom-vortex/src/vortex_operational_facets.rs`
- [x] Vortex-native provider terminology, Scan API alignment reports, and Native I/O certificate
  references exist.
- [x] Per-source/sink evidence is required; run-level summaries do not satisfy the envelope.
- [x] The `native-io-envelope-plan` source/sink coverage matrix now enumerates local Vortex,
  compatibility import, object-store/range-read, table/catalog, streaming, unstructured/media, and
  external-adapter source/sink families with support status, certificate refs, deterministic
  unsupported diagnostics, blockers, future evidence, no-fallback fields, and claim boundaries.
- [ ] `GAR-COMPAT-1` adds the universal compatibility completion follow-through. The initial
  scoreboard now classifies CSV, JSONL/JSON, Parquet, Arrow IPC, Avro, ORC, Excel, SQLite,
  Postgres/MySQL, JDBC/ODBC, S3/GCS/ADLS, Iceberg/Delta/Hudi, Vortex, generated/source-free
  outputs, Python rows/DataFrame, SQL VALUES/literals, REST/Flight/ADBC, and Foundry as
  runtime-supported, smoke-supported, report-only, blocked, or not-planned. It still needs typed
  website/status and Python capability projection before users or agents can consume the matrix as
  a stable machine surface.
- [x] `GAR-IOREUSE-1F` adds the remaining Native I/O reuse ladder after
  `GAR-IOREUSE-1A` established the universal local SourceState benchmark/report contract and
  `GAR-IOREUSE-1B` established the scoped VortexPreparedState benchmark/report contract and
  `GAR-IOREUSE-1C` established the scoped OutputPlan benchmark/report contract:
  `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan ->
  SinkArtifact`. `GAR-IOREUSE-1D` established report-only fanout case visibility, and
  `GAR-IOREUSE-1E` established current local fingerprint/invalidation posture. Evidence-safe reuse
  levels are now machine-readable, no-fallback, and `not_claim_grade`; the next work is to keep
  sink artifacts reusable and independently certified without coupling input format to output
  format.
- [ ] CG-19 is not universal across object-store/range-read, streaming sinks, table/catalog,
  external adapters, and all production source/sink paths.

### RFC 0032 - World-Class SQL, Operator, Function, Adapter, and User Capability Surface

- Source:
  [`docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`](../rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md)
- Current read: Capability and sufficiency reports exist; world-class claims remain not claimable.
- Evidence: `shardloom-core/src/certification.rs`, `shardloom-cli/src/status_capabilities.rs`,
  `shardloom-cli/src/workload_certification.rs`,
  `shardloom-cli/tests/cg20_user_capability_gate.rs`,
  `shardloom-cli/tests/world_class_sufficiency_plan_snapshots.rs`,
  `python/src/shardloom/context.py`, `python/tests/test_cli_client.py`, `python/README.md`
- [x] CG-20 sufficiency reports, capability discovery, and selected local evidence surfaces exist.
- [x] Capability scopes are report-only unless workload-specific certification says otherwise.
- [x] `GAR-0032-B` adds a Python DataFrame/query-builder method capability matrix for advertised
  source declarations, lazy filter/select/limit/group-by declarations, unsupported joins,
  aggregations, windows, writes, schema/data-quality helpers, materialization/notebook display,
  input boundaries, SQL frontend posture, claim boundaries, required evidence, and
  no-fallback/no-external-engine posture.
- [ ] `GAR-GEN-1` source-free generated-output runtime remains partially planned. `GAR-GEN-1A/1B`
  add the report-only `GeneratedSourceCertificate` contract and capability rows that separate
  `no_dataset_smoke`, `user_generated_source`, and `engine_native_generated_source`; `GAR-GEN-1C`
  adds one scoped local user-row JSONL smoke path with generated-source and output evidence, and
  `GAR-GEN-1D` adds one scoped local engine-native range JSONL smoke path. `GAR-GEN-1E` adds
  `shardloom.generated_source_api_admission.v1` rows for Python, SQL, DataFrame, and API
  source-free forms without parsing SQL, executing broad DataFrame runtime, writing output for
  report-only rows, or invoking external engines. Remaining GAR-GEN work is other engine-native
  generators plus broader output/API proof without promoting broad SQL/DataFrame runtime,
  object-store/Foundry runtime, performance, production, or package claims before evidence exists.
- [ ] `GAR-NOVEL-1A` keeps `GeneratedSourceCertificate` aligned with Python/API docs,
  SQL/DataFrame capability rows, Foundry proof docs, and future lineage/telemetry/confidence refs
  without adding generated-output runtime.
- [ ] `GAR-COMPAT-1` keeps broad adapter and user-surface compatibility separate from runtime
  support. Python rows/DataFrame, SQL VALUES/literals, REST/Flight/ADBC, external databases, and
  generated/source-free output remain report-only or blocked unless a narrower evidence-bearing
  slice upgrades the row.
- [ ] Broad SQL parser/binder/runtime, DataFrame execution, UDF, notebook runtime, universal
  adapter, unstructured/media, and best-default certification remain incomplete.

### RFC 0033 - User Data Workflow and ETL Surface

- Source:
  [`docs/rfcs/0033-user-data-workflow-etl-surface.md`](../rfcs/0033-user-data-workflow-etl-surface.md)
- Current read: Workflow/report and local helper surfaces exist; production ETL is not complete.
- Evidence: `python/src/shardloom/query.py`, `python/src/shardloom/context.py`,
  `python/src/shardloom/quickstart.py`, `python/src/shardloom/client.py`,
  `python/tests/test_query_builder.py`, `python/examples/workflow_readiness_smoke.py`
- [x] Python workflow/query builder, quickstart, capability views, and unsupported diagnostics
  exist.
- [x] Python and CLI workflow views preserve Vortex-native/no-fallback diagnostics.
- [x] `GAR-0032-B` exposes method-level support status and claim boundaries for Python
  DataFrame/query-builder affordances without reading data, materializing rows, writing outputs,
  invoking external engines, or upgrading support to claim-grade runtime.
- [ ] Source-free generated-output workflows such as `ctx.from_rows(...).write(...)` and
  `ctx.range(...).write(...)` now have scoped local JSONL smoke paths, and
  `shardloom.generated_source_api_admission.v1` exposes deterministic admission rows for
  `ctx.literal_table`, `ctx.calendar`, SQL literal `SELECT`, SQL `VALUES`, SQL source-free
  projection, SQL `generate_series`/`range`, DataFrame source-free projection, and generated
  `with_column`. Calendar/date dimension generation, literal tables, SQL execution, and
  reference/lookup table generation remain report-only/planned under `GAR-GEN-1`. No-input smoke
  does not count as generated-output execution.
- [ ] `GAR-COMPAT-1` is the user-workflow compatibility scoreboard for source/sink/adapters. It
  separates plan/report coverage from runtime coverage for local files, Vortex, generated-output
  APIs, external databases, object stores, table formats, REST/Flight/ADBC, and Foundry.
- [ ] `GAR-COMMERCIAL-1C` and `GAR-COMMERCIAL-1F` add buyer-facing status and workflow recipes so
  users can quickly determine whether ShardLoom supports, smoke-supports, reports-only, blocks, or
  does not plan common workflows without hiding unsupported paths.
- [x] `GAR-IOREUSE-1D` adds a report-only cross-format fanout benchmark and workflow lane. It shows
  required fanout case IDs, reusable source/prepared/output planning fields, deterministic blocker
  IDs/reasons, timing/reuse columns, `fanout_output_count=0`, and no-fallback/no-external-engine
  evidence without treating fanout as runtime support, performance proof, or a requirement that
  input and output formats match.
- [ ] `GAR-DOCS-1` adds non-expert workflow documentation coverage, recipe generation, glossary
  links, exact references, and a future website "Can I use this?" status matrix. It must document
  current local/smoke paths and blockers without creating production ETL, SQL/DataFrame,
  object-store/lakehouse, Foundry, performance, or Spark-replacement claims.
- [ ] Mature DataFrame execution, SQL execution, joins, aggregations, windows, data-quality
  runtime, object-store/table runtime, publication, production ETL certification, and
  comparison-only baseline/oracle views remain incomplete.

### RFC 0034 - Three-Engine Certified Data Execution Fabric

- Source:
  [`docs/rfcs/0034-three-engine-certified-data-execution-fabric.md`](../rfcs/0034-three-engine-certified-data-execution-fabric.md)
- Current read: Batch/live/hybrid contracts exist; production engines are not complete.
- Evidence: `shardloom-cli/src/engine_fabric_planning.rs`, `shardloom-core/src/engine_modes.rs`,
  `python/src/shardloom/context.py`, `python/src/shardloom/client.py`,
  `shardloom-cli/tests/cg22_engine_fabric_snapshots.rs`,
  `shardloom-vortex/src/streaming_batch_runtime.rs`
- [x] Batch, live, and hybrid modes are represented as ShardLoom-native contracts and report
  surfaces.
- [x] External systems are represented as sources, sinks, or baselines only.
- [ ] Production live/hybrid engines, broker/state-store dependencies, object-store execution,
  freshness/exactly-once claims, and comparison-only baseline/oracle surfaces remain incomplete.

### RFC 0035 - REST, Event, and Remote API Surface

- Source: [`docs/rfcs/0035-rest-event-remote-api-surface.md`](../rfcs/0035-rest-event-remote-api-surface.md)
- Current read: Contract-first API docs and reports exist; no production server exists.
- Evidence: `docs/api/shardloom-openapi-v1.yaml`, `docs/api/shardloom-asyncapi-events-v1.yaml`,
  `shardloom-cli/src/rest_api_planning.rs`, `python/src/shardloom/client.py`,
  `shardloom-cli/tests/api_protocol_snapshots.rs`, `python/tests/test_cli_client.py`
- [x] OpenAPI/AsyncAPI docs, REST planning reports, Python wrapper views, and protocol snapshots
  exist.
- [x] Discovery/server contract paths preserve `server_started=false` where no server starts.
- [ ] `GAR-NOVEL-1B` and `GAR-NOVEL-1C` add planned report-only OpenLineage facet and
  OpenTelemetry trace mapping follow-through. No lineage event, telemetry exporter, network call,
  backend integration, or production API claim is authorized by those rows alone.
- [ ] `GAR-COMMERCIAL-1D` adds the planned enterprise evidence export pack: ShardLoom JSON,
  OpenLineage facets, OpenTelemetry spans/metrics, and optional Markdown summary. Export remains
  opt-in, no-network by default, redacted, and not a production observability claim.
- [ ] HTTP listener, remote execution, Flight/ADBC runtime bridge, broker integration, production
  API, and dependency-expanded server remain incomplete.

### RFC 0036 - Foundry Integration Pack and Availability Surface

- Source:
  [`docs/rfcs/0036-foundry-integration-pack-availability-surface.md`](../rfcs/0036-foundry-integration-pack-availability-surface.md)
- Current read: Optional local/report proof exists; production Foundry integration is incomplete.
- Evidence: `docs/foundry/integration-pack-readiness.md`,
  `docs/foundry/proof-of-use-certification.md`, `shardloom-core/src/universal_harness.rs`,
  `shardloom-cli/src/evidence_certificates.rs`, `shardloom-cli/src/typed_envelope.rs`,
  `shardloom-cli/src/workload_certification.rs`
- [x] Foundry availability docs, local proof posture, optional harness flags, and boundary report
  shapes exist.
- [x] ShardLoom core remains Vortex-native/no-fallback while Foundry stays optional integration.
- [ ] `GAR-COMMERCIAL-1E` adds the planned Foundry dev-stack starter kit. It remains personal
  dev-stack/local-style proof only and must expose `foundry_runtime_invoked`,
  `foundry_compute_invoked`, and `foundry_spark_invoked` fields without invoking Foundry,
  credentials, direct S3/object-store runtime, Spark, or external compute.
- [x] `GAR-IOREUSE-1G` adds Foundry no-input generated-output fanout posture to the local
  proof report through `shardloom.foundry_generated_output_fanout_posture.v1`. It remains
  report-only: `generated_output_execution_performed=false`, generated-source and output
  certificates are not emitted, `fanout_output_count=0`, and direct S3/object-store writes,
  Foundry Spark, external engines, and Foundry production claims remain blocked.
- [ ] Production `shardloom-foundry`, package publication, Foundry service invocation, Artifact
  Repository publication, Compute Module, virtual-table native execution, Foundry dataset
  transaction runtime, and F10 workload-certified deployment remain incomplete.

### RFC 0037 - Client, Wrapper, SDK, and Ecosystem Integration Surface

- Source:
  [`docs/rfcs/0037-client-wrapper-sdk-ecosystem-surface.md`](../rfcs/0037-client-wrapper-sdk-ecosystem-surface.md)
- Current read: Wrapper architecture, the Python CLI wrapper, and typed Python capability posture
  accessors exist; ecosystem clients are planned work.
- Evidence: `shardloom-core/src/wrapper_architecture.rs`,
  `shardloom-cli/tests/python_wrapper_snapshots.rs`, `python/src/shardloom/client.py`,
  `python/src/shardloom/context.py`, `python/tests/test_cli_client.py`
- [x] One-protocol/many-thin-wrappers architecture and no-fallback wrapper reports exist.
- [x] Python wrapper reads CLI JSON rather than creating an alternate execution path.
- [x] `GAR-0010-A` exposes no-scraping Python capability posture over existing `OutputEnvelope`
  fields while preserving side-effect-free capability discovery and no runtime expansion.
- [ ] Generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight,
  ADBC, and BI connector implementations remain incomplete.

### RFC 0038 - Top-Level Plan and Execution Facade

- Source:
  [`docs/rfcs/0038-top-level-plan-execution-facade.md`](../rfcs/0038-top-level-plan-execution-facade.md)
- Current read: Top-level facade contracts and a typed compatibility matrix exist; broad runtime
  facade remains incomplete.
- Evidence: `shardloom-plan/src/execution_facade.rs`, `shardloom-vortex/src/top_level_facade.rs`,
  `shardloom-vortex/tests`, `shardloom-exec/src/lib.rs`, `python/src/shardloom/client.py`,
  `python/tests/test_cli_client.py`
- [x] Vortex-native top-level provider dispatch and artifact-rich result surfaces exist.
- [x] Facade reports preserve explicit provider selection and no-fallback evidence.
- [x] GAR-0038-A facade compatibility matrix separates executable provider-dispatched paths,
  report-only paths, unsupported SQL/DataFrame/object-store/write runtimes, removed/unsupported
  legacy placeholders, and prohibited external-engine fallback.
- [ ] SQL/DataFrame runtime, object-store runtime, writes, and any executable legacy facade shim
  remain incomplete; external engines remain baseline/oracle only.

### RFC 0039 - Typed Command/Result Envelope and CLI Modularity

- Source:
  [`docs/rfcs/0039-typed-command-result-envelope-cli-modularity.md`](../rfcs/0039-typed-command-result-envelope-cli-modularity.md)
- Current read: Typed envelope and modular CLI work are mostly implemented; migration tail remains.
- Evidence: `shardloom-cli/src/typed_envelope.rs`, `shardloom-cli/src/cli_output.rs`,
  `shardloom-cli/src/command_family.rs`, `python/src/shardloom/models.py`
- [x] Typed output v2, renderer, lifecycle taxonomy, command-family routing, and Python typed
  models exist.
- [x] Tests lock typed envelope compatibility for current command families.
- [x] `GAR-PERF-2A` adds typed-envelope and benchmark-row fields for
  `evidence_level=minimal_runtime|certified|full_replay` on the scoped prepared/native batch
  runner, so callers can distinguish proof level from execution mode and engine mode without
  inferring claim status from prose.
- [ ] Legacy flat `fields` mirror, remaining command-family result migration, some golden fixtures,
  Foundry boundary fixture, and additional physical handler splits remain incomplete.

### RFC 0040 - Benchmark Suite and Platform-Learning Hardening

- Source:
  [`docs/rfcs/0040-benchmark-suite-platform-learning-hardening.md`](../rfcs/0040-benchmark-suite-platform-learning-hardening.md)
- Current read: Benchmark taxonomy exists; full comparative promotion remains incomplete.
- Evidence: `benchmarks/common/scenario_catalog.json`,
  `benchmarks/traditional_analytics/run.py`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `shardloom-cli/tests/traditional_benchmark_harness.rs`
- [x] Local taxonomy, dataset profiles, coverage rows, benchmark constitution metadata, and
  baseline-only labeling exist.
- [x] Benchmark docs prevent external baseline rows from satisfying ShardLoom-native claims.
- [x] `GAR-PERF-2A` adds evidence-level rows for `minimal_runtime`, `certified`, and
  `full_replay` in the scoped prepared/native batch runner. This lets benchmark readers compare
  evidence overhead without turning evidence-light runtime rows into public speed rankings or
  claim-grade benchmark proof.
- [x] `GAR-PERF-2B` adds the report-only evidence-aware logical optimizer. Benchmark and explain
  rows report optimizer trace IDs, rule statuses, report-only before/after plan-digest placeholders,
  rewrite safety, and evidence-preservation fields without implying broad lazy optimizer,
  SQL/DataFrame, or performance claims.
- [x] `GAR-PERF-2C` adds the Vortex Scan API pushdown completion pass. Prepared/native scenario
  families report filter/projection/limit pushdown evidence or a deterministic blocker, including
  filter-only versus output column distinction, without treating pushdown evidence as an
  encoded-native operator claim.
- [x] `GAR-PERF-2D` adds scoped compressed/encoded kernel registry evidence. Initial
  encoding/operator pairs include bitpacked boolean/integer filter, sequence equality/range
  predicate, dictionary equality/group-by, constant array count/filter, sorted min/max range
  pruning, and FSST/dictionary string equality where available. Current selective-filter rows admit
  observed bitpacked and sequence filter inputs, keep other initial pairs blocked or not available,
  and preserve `encoded_native_claim_allowed=false`.
- [x] `GAR-PERF-2E` adds scoped fused operator pipeline evidence. Prepared/native rows now execute
  scoped residual-native fusion evidence for filter + projection + limit, filter + aggregate via
  selective-filter selection vectors, and top-k with projection; filter + group-by remains a
  deterministic blocker until a scoped filtered grouped scenario exists. Fusion evidence reports
  family statuses, row counts, materialization/decode posture, correctness digest parity fields,
  no-fallback/no-external-engine fields, and `encoded_native_claim_allowed=false`, and must not be
  treated as encoded-native execution or public performance proof.
- [x] `GAR-PERF-2F` adds the scoped in-process session runtime slice for prepared/native local
  artifacts. It connects report-only `ShardLoomSessionModelReport`, scoped prepared/native batch
  runner evidence, and source-state reuse into caller-owned local session evidence with explicit
  close/drop lifecycle, cache-hit/miss evidence, and no daemon/server claim.
- [x] `GAR-PERF-2G` adds the scoped allocation and buffer-pool blocker evidence pass. It reports
  allocation/resource posture for result buffers, temporary vectors, hash tables,
  dictionary/string state, and source-state arrays while keeping reuse disabled, correctness/evidence
  posture explicit, and unsafe lifetime shortcuts prohibited.
- [x] `GAR-PERF-2H` adds the optimized build-profile and PGO benchmark lane. It defines
  `release-lto`, `release-pgo`, and `release-native-benchmark` posture, keeps `target-cpu=native`
  benchmark-only, records build profile/LTO/PGO/native status in benchmark artifacts, and keeps
  performance claims blocked until claim-grade gates pass.
- [x] `GAR-PERF-2I` adds the native microbenchmark suite expansion for Vortex scan-only,
  filter predicate-only, projection-only, group-by kernel, hash-join kernel, top-k, result-sink
  write, and evidence-render primitives. These rows must remain subsystem evidence, not end-to-end
  benchmark claims, and skipped/unsupported primitives must be visible deterministic rows.
- [x] `GAR-IOREUSE-1D` adds the I/O reuse and cross-format fanout benchmark bundle visibility:
  `io_reuse_and_fanout`, `source_state_reuse`, `prepared_state_reuse`, `output_plan_reuse`,
  `cross_format_output`, and `generated_source_output`. Current rows report required fanout cases,
  source/prepared/output reuse-hit fields, timing columns, `fanout_output_count=0`,
  no-fallback/no-external-engine fields, and claim-gate fields without presenting the bundle as
  runtime fanout support or a speed leaderboard.
- [ ] Full comparative reruns, source-backed claim-grade promotion, managed-platform lanes,
  credentials, new managed dependencies, and public performance claims remain incomplete.

### RFC 0041 - Feature/Build Matrix and Crate Posture

- Source:
  [`docs/rfcs/0041-feature-build-matrix-crate-posture.md`](../rfcs/0041-feature-build-matrix-crate-posture.md)
- Current read: Feature/build matrix and crate posture are implemented as reports/docs.
- Evidence: `shardloom-core/src/release.rs`,
  `docs/architecture/workspace-feature-build-matrix.md`,
  `docs/architecture/crate-posture-public-exports.md`
- [x] Workspace feature/build matrix and crate-posture public export reports exist.
- [x] Docs record current executable, report-only, unsupported, planned, and prohibited-fallback export
  posture.
- [x] `GAR-PERF-2H` adds optimized build-profile and PGO benchmark posture. Feature/build matrix
  evidence distinguishes portable release artifacts from `target-cpu=native` benchmark-only
  artifacts and records LTO/PGO/native profile status before optimized benchmark rows are
  interpreted.
- [ ] Release claims remain not claimable until required matrix rows have attached passing evidence.

### RFC 0042 - Vortex Runtime Utilization and Execution Spine

- Source:
  [`docs/rfcs/0042-vortex-runtime-utilization-execution-spine.md`](../rfcs/0042-vortex-runtime-utilization-execution-spine.md)
- Current read: Runtime utilization reporting exists; the scoped Source/Split admission proof is
  now explicit, while generalized Source/Split and layout execution remain incomplete.
- Evidence: `shardloom-vortex/src/runtime_utilization.rs`,
  `shardloom-vortex/src/vortex_scan_compatibility.rs`,
  `shardloom-cli/src/vortex_planning.rs`,
  `docs/architecture/vortex-runtime-utilization-audit.md`,
  `docs/architecture/vortex-upstream-alignment-hardening.md`
- [x] Report-only Vortex-native utilization and execution-spine evidence exists.
- [x] Provider terminology keeps upstream Vortex APIs policy-admitted and certificate-backed.
- [x] `vortex-api-inventory` now exposes a GAR-0042A source/split admission proof for the scoped
  local Vortex scan fixture path, with provider/version/feature-gate/API surface, split-ref status,
  field-mask and predicate-ordering blockers, certificate refs, Native I/O refs, no-fallback fields,
  and a fixture-only claim boundary.
- [x] GAR-0042B adds a report-only boundary matrix for layout/write, device execution, object-store
  I/O, and managed-platform comparisons. Every row is `not_claim_grade`, managed-platform rows are
  comparison-only, device/object-store lanes cannot satisfy native claims without evidence, and
  benchmark/claim-gate metadata carries the boundary ref.
- [ ] `GAR-PERF-2C` adds Vortex Scan API pushdown completion across prepared/native scenario
  families. It should map supported filter/projection/limit intent into source-backed scan evidence,
  keep unsupported expressions deterministic blockers, and preserve no-fallback/no-external-engine
  fields.
- [x] `GAR-PERF-2D` adds compressed/encoded kernel registry follow-through across scoped
  selective-filter Vortex array encodings. It classifies initial encoding/operator pairs as
  admitted/executed, blocked, unsupported, or not available while preserving canonicalization,
  decode, materialization, validity, no-fallback, and encoded-native claim-gate evidence.
- [x] `GAR-PERF-2E` adds scoped fused local prepared/native operator-pipeline evidence for admitted
  filter/projection/limit, filter/aggregate, and top-k/projection families, with filter/group-by
  blocked deterministically until a scoped filtered grouped scenario exists. It preserves
  Vortex/source-backed provider evidence, ShardLoom-native residual ownership, correctness digest
  parity fields, and no-fallback/no-external-engine fields without promoting encoded-native or
  performance claims.
- [x] `GAR-PERF-2F` adds the scoped in-process session runtime follow-through from report-only
  `ShardLoomSessionModelReport` for prepared/native local artifacts. It keeps registries/session
  state explicit, local, caller-owned, and no-fallback, and it does not imply a daemon, service,
  remote server, hidden global cache, or production runtime claim.
- [x] `GAR-PERF-2G` adds allocation and buffer-pool optimization follow-through for prepared/native
  local runtime paths. It reports allocation profile status, scoped buffer-pool status,
  buffer-reuse count/blocker, peak RSS `not_available` status, correctness/evidence-regression
  posture, no unsafe lifetime shortcuts, and no-fallback/no-external-engine fields.
- [x] `GAR-PERF-2H` adds optimized build-profile and PGO benchmark follow-through. It keeps Cargo
  custom profiles, rustc PGO flags, host-native codegen, benchmark evidence, and release portability
  boundaries explicit so optimized builds do not become hidden performance or release claims.
- [ ] `GAR-IOREUSE-1` adds planned I/O reuse and fanout follow-through across Vortex-adjacent
  source/sink boundaries. It must check Vortex Source/Sink/Split, file I/O, prepared artifact, and
  output concepts before inventing parallel abstractions; any wrapper must preserve Native I/O,
  materialization/decode, no-fallback, output metadata, and claim-gate evidence.
- [ ] Generalized Source/Split runtime paths, field-mask/predicate-ordering proof, layout/write
  runtime evidence, object-store runtime I/O, GPU/device execution, and managed-platform benchmark
  lanes remain incomplete.

### RFC 0043 - Security, Vulnerability, Exploit, and Supply-Chain Hardening

- Source:
  [`docs/rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md`](../rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md)
- Current read: Security hardening is substantially represented; final hard release gate and
  publication remain incomplete.
- Evidence: `SECURITY.md`, `shardloom-core/src/security.rs`, `scripts/check_dependency_audit.py`,
  `scripts/release_provenance_dry_run.py`, `scripts/check_release_security_gate.py`,
  `.github/workflows`
- [x] Security reports, malicious-input/path/redaction tests, dependency/provenance/security gates,
  CodeQL/Scorecard workflows, and OSS security docs exist.
- [x] Release metadata preserves no-fallback and supply-chain evidence requirements.
- [ ] Full hard release-readiness gate, actual publication, and final attestation remain
  incomplete.

## Compute Engine Flow Review

### Compute Engine Flow Reference

- Source:
  [`docs/architecture/compute-engine-flow-reference.md`](compute-engine-flow-reference.md)
- Current read: The flow terminology is aligned with the current repo, but not every mode is fully
  implemented.
- Evidence: `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `shardloom-core/src/output.rs`, `shardloom-core/src/engine_modes.rs`,
  `shardloom-cli/src/typed_envelope.rs`, `shardloom-cli/src/evidence_certificates.rs`,
  `shardloom-vortex/src/runtime_utilization.rs`
- [x] Top-level flow stages are represented: request, policy, capability discovery, semantic
  binding, explicit execution mode, provider admission, result/ref, evidence, claim gate, and typed
  output envelope.
- [x] Current modes and benchmark lanes preserve explicit mode names, selected-mode reasoning,
  materialization/decode boundaries, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] `compatibility_import_certified`, `prepared_vortex`, and `native_vortex` are represented in
  current report/benchmark/evidence surfaces.
- [x] `auto` is constrained to transparent mode selection and must report selected mode plus reason.
- [x] `direct_compatibility_transient` now has deterministic admission diagnostics across
  capability rows, CLI JSON envelopes, Python typed accessors, and benchmark coverage rows, plus
  scoped local CSV smoke paths for `selective filter` and `filter + projection + limit`; broader
  formats, operators, result sinks, and SQL/DataFrame direct transient runtime remain blocked or
  report-only and are not Vortex-native, performance, production, or package-release claims.
- [ ] Prepared/native Vortex rows now carry a typed operator blocker matrix, explicit
  `source_backed_scan_*` evidence, and scoped batch source-metadata reuse, but still rely on
  temporary materialized or residual ShardLoom-native operator paths for some scenarios until
  encoded/native operator coverage matures.
  - [x] GAR-FLOW-2E aligns `compute-capability-matrix` with the scoped prepared/native
        residual-runtime evidence already present for grouped aggregate, join, and row-number/window
        rows. Those rows now report `support_status=fixture_certified`,
        `operator_execution_class=residual_native`,
        `operator_admission_status=residual_native_fixture_admitted`, and
        `operator_encoded_native_claim_allowed=false`; encoded-native, spill-safe, broad runtime,
        performance, SQL/DataFrame, object-store, lakehouse, and production claims remain blocked.
  - [x] GAR-FLOW-2F adds `traditional-analytics-vortex-batch-run`, a scoped single-process
        prepared/native Vortex batch runner that executes multiple requested scenarios against the
        same caller-supplied prepared Vortex artifacts, preserves per-scenario typed evidence,
        Native I/O certificate fields, operator blocker fields, result-sink replay evidence when
        requested, and `fallback_attempted=false` / `external_engine_invoked=false`. This closes
        only process-reuse runtime support for local prepared/native fixture paths; persistent
        daemon/service runtime, default benchmark harness integration, encoded-native operator
        claims, performance claims, SQL/DataFrame, object-store/lakehouse, Spark-displacement, and
        production claims remain blocked.
  - [x] GAR-FLOW-2G wires the comparative Python benchmark harness to consume
        `traditional-analytics-vortex-batch-run` for eligible prepared/native Vortex scenario
        groups, one batch process per format/iteration. Rows now report
        `persistent_runner_status=single_process_batch_runner_supported`,
        shared batch process wall timing, per-scenario `scenario_compute_micros` and
        `vortex_scan_micros`, child evidence fields, Native I/O certificate status,
        operator/source-backed-scan fields, result-sink evidence when requested, and
        `fallback_attempted=false` / `external_engine_invoked=false`. This closes benchmark harness
        integration for scoped local prepared/native batch process reuse only; encoded-native
        operator claims, persistent daemon/service runtime, performance claims, SQL/DataFrame,
        object-store/lakehouse, Spark-displacement, and production claims remain blocked.
  - [x] GAR-FLOW-2H reuses one per-batch fact/dimension/CDC Vortex source metadata snapshot inside
        `traditional-analytics-vortex-batch-run`, so child prepared/native scenario reports reuse
        source size/digest evidence rather than recomputing it for each scenario. The batch envelope
        emits `source_metadata_snapshot_*` fields plus no-fallback/no-external-engine policy fields.
        This closes scoped source-metadata reuse only; row-state reuse, encoded-native operator
        claims, persistent daemon/service runtime, performance claims, SQL/DataFrame,
        object-store/lakehouse, Spark-displacement, and production claims remain blocked.
  - [x] GAR-FLOW-2I reuses one per-batch dimension-label lookup state for hash-join and
        join-aggregate child scenarios inside `traditional-analytics-vortex-batch-run`. The batch
        envelope emits `source_state_reuse_*`, `source_state_prepare_micros`, and
        `source_state_prepare_timing_scope=batch_shared_pre_scenario` so shared setup remains
        visible. This closes scoped join-dimension source-state reuse only; broader row-state reuse,
        encoded-native operator claims, persistent daemon/service runtime, performance claims,
        SQL/DataFrame, object-store/lakehouse, Spark-displacement, and production claims remain
        blocked.
  - [x] GAR-FLOW-2J reuses one per-batch category/metric grouped state for distinct-count and
        high-cardinality string-group/distinct child scenarios inside
        `traditional-analytics-vortex-batch-run`. The batch envelope emits aggregate
        `source_state_reuse_*` fields plus family-specific `source_state_category_metric_*` fields
        so shared setup remains visible. This closes scoped category/metric source-state reuse only;
        generalized row-state reuse, encoded-native operator claims, persistent daemon/service
        runtime, performance claims, SQL/DataFrame, object-store/lakehouse, Spark-displacement, and
        production claims remain blocked.
  - [x] GAR-FLOW-2K reuses one per-batch ranked-metric state for sort/top-k, top-N per group, and
        row-number/window child scenarios inside `traditional-analytics-vortex-batch-run`. The
        batch envelope emits aggregate `source_state_reuse_*` fields plus family-specific
        `source_state_ranked_metric_*` fields so shared setup remains visible. This closes scoped
        ranked residual-state reuse only; generalized encoded/native operators, distributed sort,
        persistent daemon/service runtime, performance claims, SQL/DataFrame, object-store/lakehouse,
        Spark-displacement, and production claims remain blocked.
  - [x] GAR-FLOW-2L reuses one per-batch group/category/metric grouped state for group-by
        aggregation and multi-key group-by child scenarios inside
        `traditional-analytics-vortex-batch-run`. The batch envelope emits aggregate
        `source_state_reuse_*` fields plus family-specific
        `source_state_group_category_metric_*` fields so shared setup remains visible. This closes
        scoped grouped residual-state reuse only; generalized encoded/native operators, performance
        claims, SQL/DataFrame, object-store/lakehouse, Spark-displacement, and production claims
        remain blocked.
  - [x] GAR-FLOW-2M reuses one per-batch dirty-input cleanup state for clean/cast/filter/write and
        malformed timestamp / dirty CSV child scenarios inside
        `traditional-analytics-vortex-batch-run`. The batch envelope emits aggregate
        `source_state_reuse_*` fields plus family-specific `source_state_dirty_input_*` fields so
        shared dirty-column parsing and timestamp validation setup remains visible. This closes
        scoped dirty-input residual-state reuse only; generalized encoded/native operators,
        performance claims, SQL/DataFrame, object-store/lakehouse, Spark-displacement, and
        production claims remain blocked.
  - [x] GAR-FLOW-2N reuses one per-batch filtered `id,value,metric` state for selective-filter and
        filter/projection/limit child scenarios inside `traditional-analytics-vortex-batch-run`.
        The batch envelope emits aggregate `source_state_reuse_*` fields plus family-specific
        `source_state_selective_filter_*` fields so shared predicate/filter setup remains visible.
        Selective-filter rows retain scoped `encoded_predicate_provider_*` evidence, but the
        shared-state metric aggregate is reported as residual-native
        `batch_source_state_metric_aggregation_used`. This closes scoped selective-filter
        residual-state reuse only; generalized encoded/native operators, performance claims,
        SQL/DataFrame, object-store/lakehouse, Spark-displacement, and production claims remain
        blocked.
- [x] `GAR-PERF-1` completes the end-to-end prepared/native performance architecture queue:
  post-source-state-reuse benchmark refresh, complete source-state reuse coverage classification,
  fused filter/project/limit plus selection-vector follow-through, and the report-only Bayesian
  performance/layout advisor contract. These items are evidence/architecture/runtime-plumbing
  surfaces and do not authorize public performance, superiority, Spark-displacement, production,
  SQL/DataFrame, object-store/lakehouse, or encoded-native claims.
- [x] `GAR-PERF-2A` adds evidence-level runtime tiering across `minimal_runtime`, `certified`, and
  `full_replay` for the scoped prepared/native batch runner. The flow shows evidence level as
  independent from execution mode and engine mode, keeps `fallback_attempted=false` and
  `external_engine_invoked=false` visible in every level, and treats `minimal_runtime` as
  `not_claim_grade` unless a later scoped gate approves otherwise.
- [x] `GAR-PERF-2B` adds evidence-aware logical optimizer follow-through. The flow keeps optimizer
  rule registry, admitted/applied/blocked/unsupported/not-applicable/report-only status,
  report-only before/after plan-digest placeholders, rewrite safety, evidence preservation,
  no-fallback fields, and claim gates visible without implying Polars/DataFusion parity or broad
  SQL/DataFrame runtime.
- [ ] `GAR-PERF-2C` adds Vortex Scan API pushdown completion. The flow must keep scan filter,
  projection, and limit pushdown evidence independent from encoded-native operator claims, and every
  prepared/native scenario family must report pushed-down fields or deterministic blockers.
- [x] `GAR-PERF-2D` adds compressed/encoded kernel registry follow-through. The flow keeps
  encoding ID, operator family, admission/execution, canonicalization, decode, materialization,
  validity, no-fallback, and claim-gate evidence visible without treating registry admission as
  encoded-native support.
- [x] `GAR-PERF-2E` adds fused operator pipeline follow-through. The flow shows fused residual
  pipelines as scoped prepared/native runtime evidence with family statuses, row counts,
  intermediate materialization avoidance, correctness digest parity fields, deterministic blockers,
  and no-fallback status, not as broad SQL/DataFrame, encoded-native, or public performance claims.
- [x] `GAR-PERF-2F` adds scoped in-process session follow-through. The flow keeps session state
  scoped and explicit, reports `session_id`, cache hit/miss fields, source-state/prepared artifact
  reuse counts, close/drop status, and no-fallback/no-external-engine fields, and does not imply a
  daemon, service, remote server, or hidden global cache.
- [x] `GAR-PERF-2G` adds allocation and buffer-pool optimization follow-through. The flow keeps
  resource evidence separate from performance claims, reports allocation profile and disabled buffer
  reuse/blocker fields, preserves correctness/evidence posture, and prohibits hidden global pools or
  unsafe lifetime shortcuts.
- [x] `GAR-PERF-2H` adds optimized build-profile and PGO benchmark follow-through. The flow keeps
  build profile, LTO, PGO, target triple, target CPU posture, benchmark-only native artifacts,
  release portability, correctness digest, no-fallback fields, and claim gate visible.
- [x] `GAR-PERF-2I` adds native microbenchmark suite follow-through. The flow keeps kernel-level
  native microbenchmark rows distinct from compatibility-import, prepared/native end-to-end, and
  external baseline rows, and must not let microbenchmark timing imply public performance,
  superiority, production, SQL/DataFrame, object-store/lakehouse, Foundry, or Spark-replacement
  claims.
- [ ] `GAR-IOREUSE-1` adds I/O reuse and cross-format fanout follow-through. `GAR-IOREUSE-1A`
  now exposes SourceState identity, digest, source-format/location/fingerprint/schema fields,
  parse/decode plan digest, reuse hit/reason, no-fallback fields, and claim boundaries in benchmark
  artifacts. `GAR-IOREUSE-1B` now exposes VortexPreparedState identity, digest, prepared artifact
  refs/digests, source-state linkage, preparation timing separation, reuse hit/reason, no-fallback
  fields, and claim boundaries in benchmark artifacts. `GAR-IOREUSE-1C` now exposes OutputPlan
  identity, digest, target format/schema/location posture, metadata preservation/materialization
  fields, local Vortex write/replay refs, sink artifact refs/digests, no-fallback fields, and claim
  boundaries in benchmark artifacts. `GAR-IOREUSE-1D` now exposes report-only fanout benchmark case
  IDs, requested outputs, blocker IDs/reasons, timing/reuse columns, `fanout_output_count=0`,
  no-fallback fields, and claim boundaries in benchmark artifacts. `GAR-IOREUSE-1E` now exposes
  cache invalidation/fingerprint fields, source mtime/size, source/prepared/plan/output digests,
  object-store ETag posture, cache validity, invalidation reason, credential-redaction status,
  no-fallback fields, and claim boundaries in benchmark artifacts. `GAR-IOREUSE-1F` now exposes
  evidence-safe reuse levels, per-level status/hit/digest/blocker fields, evidence-level linkage,
  output-format separation, invalidation reasons, `claim_grade_requirements_met=false`,
  no-fallback fields, and claim boundaries in benchmark artifacts. The remaining flow must add
  broader sink evidence while preserving distinct direct-transient, compatibility-import-certified,
  prepared-vortex, and native-vortex lanes without adding persistent cache, object-store/lakehouse,
  performance, or fallback claims.
- [ ] `GAR-NOVEL-1` adds the evidence-native generated execution, lineage, observability, and
  confidence follow-up. OpenLineage facets and OpenTelemetry spans remain opt-in/report-only, and
  Bayesian confidence can block claims but cannot upgrade claim status by itself.
- [ ] `GAR-COMMERCIAL-1` adds the adoption and commercial-readiness friction-reduction follow-up.
  One-command local proof, package-channel readiness, buyer-facing status, enterprise evidence
  export, Foundry dev-stack, and recipes must remain claim-safe and evidence-gated before any public
  release or production/commercial readiness claim.
- [ ] `GAR-DOCS-1` adds the Use Case Atlas and website status-matrix follow-up. The flow must be
  explainable to non-experts by use case, status, execution mode, engine mode, input, output,
  evidence fields, and blockers without requiring readers to inspect RFCs or benchmark internals.
- [ ] `GAR-COMPAT-1` is now the compute-flow follow-up for universal source/sink/adapter/user-surface
  compatibility coverage. The flow must keep compatibility coverage status distinct from runtime
  support for local files, Vortex, generated outputs, Python/DataFrame, SQL, databases, object
  stores, table formats, REST/Flight/ADBC, and Foundry.
- [ ] `GAR-GEN-1` is now the compute-flow follow-up for source-free generated-output execution.
  The flow now has report-only contract rows that distinguish no-dataset smoke from user-generated
  rows and engine-native generator nodes, plus scoped user-row and range local JSONL smoke paths. It
  still requires generated-source plus output-sink evidence before any broader generated-output
  runtime claim.
- [ ] REST parity must emit the same policy, mode-selection, evidence, claim-gate, and
  no-fallback fields as CLI/Python surfaces before it can be treated as an equivalent API.

## Follow-Up Rule

Before implementing any unchecked item from this review, use the corresponding `GAR-*` checklist
item in `docs/architecture/phased-execution-plan.md`. If the item is still too broad, split it in
the phase plan first. Keep the implementation slice narrow enough to verify with focused tests,
evidence snapshots, or release/readiness checks.
