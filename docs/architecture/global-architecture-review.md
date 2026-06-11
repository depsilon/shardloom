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
- [x] `GAR-0001B-A` adds `shardloom.engine_replacement_claim_inventory.v1`, a report-only
  engine-replacement claim inventory that maps Spark-displacement, general replacement,
  best-default, SQL/DataFrame, object-store/lakehouse, and managed-platform claim families to
  required runtime, output, correctness, benchmark, certificate, Native I/O, release, and
  no-fallback evidence. The release-plan fields keep every row `claim_gate_status=not_claim_grade`
  with no replacement claim, no public displacement language, no benchmark rerun, no runtime
  execution, `fallback_attempted=false`, and `external_engine_invoked=false`.

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
- [x] `GAR-0009-A` adds `shardloom.spark_displacement_benchmark_evidence_matrix.v1`, a
  report-only matrix tying compatibility-import, prepared/native, messy-data ETL, scale/table
  boundary, and public-claim attachment rows to workload refs, baseline/oracle lanes, correctness
  refs, timing refs, environment refs, execution-mode refs, missing evidence, and claim status.
  Every row remains `claim_gate_status=not_claim_grade`; external engines are
  baseline/oracle-only; no benchmark rerun, public performance claim, Spark-displacement claim,
  external engine invocation, or fallback execution is authorized.
- [x] `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication` adds
  `shardloom.front_door_benchmark_publication_gate.v1`, a fail-closed static admission gate that
  composes SQL/Python/DataFrame parity with public benchmark route rows and keeps front-door
  performance equivalence blocked until measured equivalent rows, correctness digests, execution
  certificates, and rerun approval exist.
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
- [x] `GAR-DOCS-1` adds the non-expert Use Case Atlas follow-through. The atlas keeps
  `ready_local`, `smoke_supported`, `report_only`, `planned`, `blocked`, and `unsupported` statuses
  distinct, maps every public capability family to references and claim boundaries, and prevents
  planned/blocked paths from being mistaken for runtime support through generated docs, website
  pages, validators, glossary coverage, and backlink checks.
- [x] `GAR-0010-B` adds a report-only DataFrame/notebook/package readiness matrix so local package
  metadata, editable install smoke, DataFrame method posture, notebook display blockers, public
  package publication blockers, and deterministic unsupported diagnostics are inspectable without
  claiming runtime or publication support.
- [x] `GAR-0010-C` closes the ergonomic runtime API freshness row for the currently admitted
  surfaces: `ShardLoomClient` and `ShardLoomContext` expose explicit helpers for local Vortex
  prepare, scoped local object-store read/write smokes, scoped local table metadata read and append
  commit rehearsal smokes, local SQLite import/export smoke, source-free generated-output writes,
  scoped local SQL/DataFrame query-builder collect/write, live/hybrid fixture runs, REST planning
  contracts, capability matrices, and deterministic unsupported diagnostics. Each executable helper
  routes through a ShardLoom CLI/runtime command and preserves no-fallback/no-external-engine
  evidence. Broad DataFrame/notebook runtime, production REST listener/runtime, public package
  publication, object-store/lakehouse production runtime, Foundry runtime, performance, and
  Spark-displacement claims remain carried by their owning broad rows and claim gates.
- [x] `GAR-0010-D` records the repo-wide readiness and user-surface audit baseline. The audit
  confirms that the current command registry is classified and side-effect explicit, adds standard
  CLI help aliases (`shardloom --help`, `shardloom -h`, and `shardloom <command> --help`), fixes
  stale completed-ledger PR references, and splits follow-through items for user-surface graduation
  and true runtime gap burn-down. This does not close broad SQL/DataFrame, object-store/lakehouse,
  dynamic UDF/plugin/effect, package/deploy, performance, or production runtime blockers.

### RFC 0011 - Modular Extensibility for SQL, UDFs, Unstructured Data, LLM Calls, API Calls, and Embeddings

- Source:
  [`docs/rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md`](../rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md)
- Current read: Manifest/report contracts exist; extension/effect capability rows are deterministic
  and report-only; effectful execution is not implemented.
- Evidence: `shardloom-core/src/extension.rs`, `shardloom-core/src/effect_budget.rs`,
  `shardloom-core/src/unstructured_workflow.rs`,
  `docs/architecture/extension-manifest-effect-capability-matrix.md`,
  `shardloom-cli/tests/extension_manifest_effect_matrix_snapshots.rs`,
  `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`
- [x] Extension manifests, permissions, provenance, effect budgets, and materialization metadata are
  represented.
- [x] Tests lock the current non-executing extension posture.
- [x] `GAR-0011-A` adds `shardloom.extension_manifest_effect_capability_matrix.v1` so extension
  manifest families, required permissions, sandbox posture, effect metadata, materialization
  boundaries, deterministic blockers, `claim_gate_status=not_claim_grade`,
  `fallback_attempted=false`, and `external_engine_invoked=false` are emitted through extension,
  UDF, and security/governance capability surfaces.
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
- [x] GAR-RUNTIME-IMPL-6D adds `pre-oom-memory-guard-smoke`, a bounded local reservation-denial
  fixture with `shardloom.pre_oom_memory_guard_fixture.v1`, `SL_RESOURCE_BUDGET_EXCEEDED`,
  cleanup/release evidence, and explicit false fields for query-data spill, distributed execution,
  native spill IO, fallback, and external engines.
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
- [x] `GAR-0015-A` adds string-semantics property/fuzz metadata and a declared deferred
  fixture-family gap: `property-string-utf8-predicate-consistency`,
  `string_utf8_predicate_consistency`, and `string-semantics` are exposed through correctness plan
  and harness fields with no query execution, no external engine invocation, no fallback, and no
  claim-grade benchmark status.
- [ ] Broad property/fuzz execution and claim-grade benchmark superiority coverage remain blocked
  and are carried by the later benchmark/claim-gate slices in the phased plan.

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
- [x] `GAR-NOVEL-1C` adds `shardloom.opentelemetry_trace_export_contract.v1` as a report-only
  observability capability. It maps request admission, source read, compatibility parse, Vortex
  import/scan, operator compute, result sink, evidence render, and claim gate timing/evidence fields
  into future OpenTelemetry internal span placeholders while keeping exporter/backend/collector
  configuration disabled, trace/metric/log emission disabled, SDK dependency expansion disabled,
  network calls disabled, allowlisted attributes required, and secret/path/query redaction required.
- [ ] Live profiling collectors, profile artifacts, debug bundles, metrics exporters, trace
  backends, and distributed runtime introspection execution remain incomplete.

### RFC 0019 - Security, Secrets, Governance, and Agent Safety

- Source:
  [`docs/rfcs/0019-security-secrets-governance-agent-safety.md`](../rfcs/0019-security-secrets-governance-agent-safety.md)
- Current read: Report-level security posture, credential-policy blockers, and sandbox/governance
  readiness blockers exist; production enforcement remains incomplete.
- Evidence: `shardloom-core/src/security.rs`, `docs/security/threat-model.md`,
  `docs/architecture/credential-policy-enforcement-gate.md`,
  `docs/architecture/sandbox-governance-runtime-readiness.md`,
  `docs/security/runtime-exploit-regression-suite.md`,
  `shardloom-contract-tests/tests/release_readiness_metadata.rs`
- [x] Security/governance reports, secrets-unloaded defaults, and side-effect-free agent/dry-run
  posture exist.
- [x] Release metadata and security docs record no-fallback and governance evidence.
- [x] `GAR-0019-A` adds `shardloom.credential_policy_enforcement_gate.v1` so credential reference
  inventory, secret loading, environment/file/external-manager/cloud-IAM providers, workspace
  policy, runtime permission checks, redaction policy, deterministic unsupported diagnostics,
  `claim_gate_status=not_claim_grade`, `credential_resolution_performed=false`,
  `secret_loading_performed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false` are visible through security/governance surfaces.
- [x] `GAR-0019-B` adds `shardloom.sandbox_governance_readiness_gate.v1` so sandbox profile
  inventory, filesystem/network/environment/secret/process permissions, resource limits,
  execution timeout, audit log, dependency isolation, deterministic unsupported diagnostics,
  `claim_gate_status=not_claim_grade`, `sandbox_runtime_supported=false`,
  `extension_code_executed=false`, `udf_code_executed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false` are visible through security/governance surfaces.
- [x] Credential resolution, secret loading, sandbox execution, plugin/UDF execution, production
  runtime policy enforcement, and production governance runtime remain blocked until later
  runtime-enabling slices attach evidence.

### RFC 0020 - Schema Evolution, Catalog Integration, and Table Compatibility

- Source:
  [`docs/rfcs/0020-schema-evolution-catalog-table-compatibility.md`](../rfcs/0020-schema-evolution-catalog-table-compatibility.md)
- Current read: Typed reports, a report-only catalog/table metadata admission gate, and one scoped
  local manifest-backed metadata smoke exist; scoped local-manifest append commit and recovery replay
  proofs exist separately. Broad table/catalog runtime integration is incomplete.
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
- [x] GAR-RUNTIME-IMPL-6D: `local-table-commit-recovery-smoke` replays the committed local-manifest
  artifact plus sidecar commit record emitted by the append commit rehearsal, verifies manifest,
  correctness, and idempotency evidence, and keeps catalog/object-store I/O, production recovery,
  fallback, and external-engine execution false.
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
- [x] GAR-RUNTIME-IMPL-4D-S1 adds scoped ShardLoom-native UTF-8 string predicate runtime for
  `utf8_starts_with` / `utf8_contains` / `utf8_ends_with` and local SQL `LIKE 'prefix%'` /
  `LIKE '%contains%'` / `LIKE '%suffix'` lowering; GAR-RUNTIME-IMPL-6D broadens scoped `LIKE`
  wildcard admission to `%` and `_` patterns through ShardLoom-owned predicate lowering while
  keeping custom `ESCAPE`, case-folding, locale/collation, and fallback execution outside the
  claim boundary.
- [x] GAR-RUNTIME-IMPL-4D-S2 adds scoped ISO Date32 runtime support for parsing/formatting,
  `date_year` / `date_month` / `date_day`, UTF-8/Date32 casts, local SQL
  `DATE 'YYYY-MM-DD'` predicates, CSV ISO date inference, and deterministic invalid-date or
  non-Date32 operand blockers with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-S3 adds scoped local SQL logical `AND` predicate runtime over already
  admitted predicate leaves, recursive evidence for string/date/cast leaves, logical predicate
  evidence fields, and deterministic `OR` blockers with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-S4 adds scoped local SQL logical `OR` predicate runtime over already
  admitted predicate leaves, preserves SQL `AND` precedence inside `OR` branches, emits the same
  logical predicate evidence fields, and keeps unsupported compound predicate forms outside this
  slice without fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-S5 adds scoped local SQL logical `NOT` predicate runtime over already
  admitted predicate leaves, preserves SQL `NOT`/`AND`/`OR` precedence for unparenthesized scoped
  predicates, emits logical predicate evidence fields, and keeps parentheses/arbitrary predicate
  completeness outside this slice without fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-S6 adds scoped local SQL balanced predicate-grouping parentheses over
  already admitted comparison, cast, date-literal, null, string, and logical predicate leaves,
  makes top-level `AND`/`OR` detection ignore grouped subexpressions, preserves recursive evidence,
  and emits deterministic unbalanced-parenthesis blockers with no fallback/external engine
  invocation.
- [x] GAR-RUNTIME-IMPL-4D generic numeric expression projections add parenthesized and chained
  `+` / `-` / `*` / `/` expression trees plus nested `ABS` / `FLOOR` / `CEIL` / `ROUND` calls over
  admitted local numeric columns and finite numeric literals, emit
  `generic_expression_projection_*` evidence, and keep missing-column/division-by-zero and
  unsupported-shape blockers deterministic with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D generic numeric expression predicates add parenthesized and chained
  `+` / `-` / `*` / `/` predicate expression trees plus nested `ABS` / `FLOOR` / `CEIL` / `ROUND`
  calls over admitted local numeric columns and finite numeric literals, compare them to admitted
  numeric expressions or finite numeric literals, emit `generic_expression_predicate_*` evidence,
  preserve SQL `WHERE` null-filter semantics, and keep missing-column/division-by-zero and
  unsupported-shape blockers deterministic with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D scoped local IN-subquery predicates add
  `column [NOT] IN (SELECT <column> FROM '<local-source>')` over bounded local scalar sources,
  materialize the subquery set through ShardLoom-owned local readers, emit `in_subquery_*`
  evidence, preserve SQL three-valued `WHERE` null-filter semantics, and keep missing-column or
  oversized materialized sets deterministic blockers with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-F3 advanced predicate/subquery closeout extends bounded local scalar
  IN-subqueries with admitted subquery `WHERE`, `ORDER BY`, and `LIMIT` clauses, executes HAVING
  IN-subquery predicates over aggregate output rows, exposes subquery filter/order/limit and
  input/filtered/materialization-bound evidence through SQL/Python reports. Later 6D slices promote
  row-value, nested scalar, joined/grouped projected scalar/row-value IN/NOT IN,
  `EXISTS`/`NOT EXISTS`, quantified, source-qualified local subquery references, and scoped correlated
  `outer.<column>` scalar/row-value IN/NOT IN, EXISTS/NOT EXISTS, and quantified subquery families; scalar-left multi-column,
  unbound qualified, or broad correlated subquery shapes remain deterministic blockers with no
  fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D scoped UTF-8 string functions add native `CONCAT`, `SUBSTR` /
  `SUBSTRING`, and `REPLACE` predicate/projection execution for admitted local-source SQL/Python
  paths, emit `string_function_*` and `string_function_projection_*` evidence, preserve
  null-propagating UTF-8 semantics, and keep literal-only calls, invalid substring bounds, empty
  replace search strings, non-UTF-8 operands, and unsupported shapes deterministic blockers with no
  fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D/5B/5C composed UTF-8 string expression runtime admits nested
  `LOWER` / `UPPER` / `TRIM`, `CONCAT`, `SUBSTR` / `SUBSTRING`, `LEFT` / `RIGHT`, `REPLACE`, and
  `LENGTH` expression trees for scoped local-source SQL/Python predicates and projections, walks
  expression trees for source-column evidence, preserves null-propagating UTF-8 semantics, and
  keeps source-free or unsupported string expression shapes deterministic blockers with no
  fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D scoped UTC-or-fixed-offset timestamp second arithmetic adds native
  `TIMESTAMP_ADD_SECONDS` / `TIMESTAMP_SUB_SECONDS` predicate/projection execution for admitted
  local-source SQL/Python paths, emits `timestamp_arithmetic_*` and
  `timestamp_arithmetic_projection_*` evidence, preserves null-propagating timestamp-micros
  semantics, admits scoped `INTERVAL '<n>' SECOND|MINUTE|HOUR|DAY` literals inside those helper
  functions, and keeps invalid counts, non-timestamp operands, malformed literals, unsupported
  units, and arbitrary interval arithmetic deterministic blockers with no fallback/external engine
  invocation.
- [x] GAR-RUNTIME-IMPL-4D scoped temporal-difference expressions add native
  `DATE_DIFF_DAYS` and `TIMESTAMP_DIFF_SECONDS` predicate/projection execution through the generic
  expression evidence path for admitted local-source SQL/Python paths, preserve null-propagating
  Date32 and UTC-or-fixed-offset timestamp delta semantics, coerce admitted ISO UTC-or-fixed-offset timestamp source strings, and keep
  arity/type/arbitrary interval or timezone shapes deterministic blockers with no fallback/external
  engine invocation.
- [x] GAR-RUNTIME-IMPL-5B/5C scoped local-source join aggregates admit scalar and grouped
  aggregates over the existing single-/multi-key inner equi-join runtime, emit
  `join_aggregate_runtime_execution`, `join_aggregate_operator_family`, and
  `join_aggregate_group_count` evidence, and keep outer join families, expression joins, and broad
  SQL/DataFrame joins deterministic blockers with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-5B/5C scoped local-source joined computed projection/top-N runtime admits
  computed projections over qualified joined rows and single-key numeric `ORDER BY ... LIMIT ...`
  for non-aggregate joined projection rows, emits `join_computed_projection_runtime_execution`,
  `join_order_by_top_n_runtime_execution`, and `join_projection_operator_family` evidence, and
  keeps aggregate join ordering, outer join families, expression joins, multi-key/null/collation
  ordering, and broad SQL/DataFrame joins deterministic blockers with no fallback/external engine
  invocation.
- [x] GAR-RUNTIME-IMPL-4D/5B/5C scoped local-source HAVING aggregate expressions admit
  unprojected `COUNT`, `COUNT(DISTINCT column)`, `SUM`, `AVG`, `MIN`, and `MAX` aggregate functions
  inside post-aggregate `HAVING`, evaluate them as hidden HAVING-only aggregate columns, strip those
  hidden columns from user result rows, emit `having_aggregate_*` evidence, expose Python typed
  report accessors, and keep unsupported DISTINCT aggregate shapes or non-output source columns
  deterministic blockers with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D/5G expression/operator closeout admits core bytewise binary equality/
  inequality, promotes UTC-or-fixed-offset timestamp/Date32/binary semantic conformance rows to executed
  fixtures, and expands the admitted semantics matrix to release-gate composed string functions,
  temporal arithmetic/difference, CASE projections, IN-list NULL semantics, scalar IN subqueries,
  grouped COUNT(DISTINCT), hidden aggregate HAVING, mixed window, and multi-key join fixtures
  without fallback or external runtime engines. Later GAR-RUNTIME-IMPL-6D evidence promotes scoped
  binary cast bytewise ordering, scoped feature-gated columnar binary source
  projection/predicate/order evidence, and scoped Parquet/Arrow IPC/Avro/ORC flat scalar binary
  sink preservation plus local Vortex known flat scalar sink preservation for boolean, int64,
  uint64, float64, utf8, binary, decimal128, date32, and timestamp_micros, including nullable/
  all-null rows when dtype/family evidence is present. Later scoped 6D evidence also admits
  scoped binary `CAST`/`TRY_CAST(<utf8-column-or-admitted-utf8-expression> AS binary)` routes plus
  `UNHEX(<utf8-column-or-admitted-utf8-expression>)` and
  `FROM_BASE64(<utf8-column-or-admitted-utf8-expression>)` binary helper projections/predicates
  against explicit binary literals through ShardLoom-owned local-source runtime fields. Unknown or
  unsupported NULL-bearing Vortex output batches still block before writer conversion, and broader
  binary sinks, broader binary execution beyond admitted helper/cast/source routes, and non-binary
  source columns compared directly to binary literals remain deterministic blockers.
- [x] GAR-RUNTIME-IMPL-6D also admits scoped local-source SQL `IS DISTINCT FROM` and
  `IS NOT DISTINCT FROM` predicate and predicate-projection grammar for column-literal,
  date/timestamp/binary literal, NULL literal, and column-column operands by lowering to existing
  ShardLoom-owned null/comparison/logical predicate primitives. Python query-builder helpers render
  the same grammar for admitted filter and predicate-projection use. Broad ANSI null-safe
  comparison parity remains outside the claim boundary.
- [x] GAR-RUNTIME-IMPL-6D also admits scoped local-source SQL explicit null top-N ordering through
  `ORDER BY <column> [ASC|DESC] NULLS FIRST|LAST LIMIT <n>` over already-admitted scalar sort keys.
  The runtime keeps null precedence independent from sort direction, Python local-source sort
  aliases expose `nulls="first"|"last"`, and implicit null ordering remains blocked until a broader
  SQL sort semantics slice is admitted.
- [x] GAR-RUNTIME-IMPL-4D-F1 advanced scalar closeout adds executed conformance fixtures and
  admitted-matrix rows for decimal precision/scale casts, fixed-offset timestamp normalization,
  timezone database blockers, interval arithmetic outside scoped temporal helpers, and
  locale-aware collation. Scoped ANSI interval literals inside temporal helper functions and scoped
  UTF-8 regex predicates were later promoted through ShardLoom-owned evaluation; later
  GAR-RUNTIME-IMPL-6D evidence also promotes scoped `decimal128(p,s)` / `decimal(p,s)` /
  `numeric(p,s)` casts through exact fixed-scale projection/predicate runtime plus scoped
  mixed-scale decimal add/subtract/multiply, mixed-scale comparison, and exact fixed-scale division
  runtime, exact exponent notation that normalizes to the declared target scale, plus feature-gated
  Parquet/Arrow IPC/Avro typed decimal sink preservation plus scoped local Vortex known flat scalar
  output, including nullable/all-null decimal rows when dtype evidence is present, while keeping
  non-exact division, broad ANSI coercion beyond exact exponent normalization, decimal/float
  comparison, and ORC typed decimal sinks blocked. The remaining advanced scalar blockers still fail
  through shared policy guards before execution with no fallback/external engine invocation.
- [x] GAR-RUNTIME-IMPL-4D-F2 complex dtype closeout added executed conformance blockers and
  admitted-matrix unsupported rows for list/array literals and accessors, struct/row constructors,
  variant access, SQL UNION/union dtype semantics, parent/child null policy, schema field identity,
  and binary source/runtime decoding. Later GAR-RUNTIME-IMPL-6D evidence promotes scoped
  `ARRAY[...]` literal and `STRUCT(<source column>, ...)` projections through the JSONL/result
  boundary only, plus scoped `SELECT DISTINCT` and `UNION DISTINCT` structural equality over those
  already-materialized projection values, and scoped canonical structural `ORDER BY` over those
  complex result-boundary values, and scoped scalar-expression `JOIN ON` predicates over qualified
  local-source rows, including scoped logical `OR` over admitted qualified scalar leaves. Current
  GAR-RUNTIME-IMPL-6D evidence also admits scoped feature-gated Arrow list/large-list/
  fixed-size-list and struct source decoding into JSONL and CSV JSON-text result boundaries, plus
  scoped typed nested compatibility sink preservation through Parquet, Arrow IPC, Avro, and scoped
  local Vortex when one stable nested Arrow dtype can be inferred from non-null values or carried
  from raw source-column child-schema evidence. All-null typed nested structured sinks without
  child-schema evidence now fail closed with
  `typed_complex_child_schema_not_admitted`; complex accessors, casts, subquery membership, broad
  nested ordering, ORC nested output, all-null typed nested output without child-schema evidence,
  complex-key joins, broader non-scalar join predicates, and broader variant/union semantics still
  fail before fallback.
- [x] Parent `GAR-RUNTIME-IMPL-4D`/`GAR-RUNTIME-IMPL-5G` is complete for admitted local expression/
  operator scope, including bounded local scalar IN-subquery/HAVING subquery closeout; residual
  broad encoded-kernel/operator coverage and non-IN-subquery families are split into explicit
  follow-up runtime items in the phased plan rather than hidden in the parent item.
- [x] GAR-RUNTIME-IMPL-6D follow-through promotes bounded row-value local-source IN-subqueries over
  local sources with admitted subquery `WHERE`, `ORDER BY`, and `LIMIT` tails, source-column arity
  validation, row-value null-semantics evidence, SQL/Python query-builder access, and
  source-qualified local refs for explicit source aliases or SQL-identifier file stems and keeps
  deterministic blockers for scalar-left multi-column, unbound qualified, or broad correlated
  subquery shapes with no fallback/external engine invocation. Later 6D slices now admit nested
  scalar IN-subqueries, joined and grouped/HAVING projected scalar, row-value, and quantified
  subqueries through the full local-source runtime path, scoped local `EXISTS` / `NOT EXISTS`,
  scoped quantified `ANY` / `ALL`, scoped correlated `outer.<column>` scalar/row-value/EXISTS/
  quantified filters, and HAVING-level `EXISTS` / quantified subqueries over aggregate output rows
  through ShardLoom-owned runtime routes.

### RFC 0022 - Plan IR and Substrait-Compatible Interoperability

- Source:
  [`docs/rfcs/0022-plan-ir-substrait-compatible-interoperability.md`](../rfcs/0022-plan-ir-substrait-compatible-interoperability.md)
- Current read: Native Plan IR exists and Substrait import/export requests now have deterministic
  report-only diagnostics; real Substrait parser/exporter support and imported-plan execution are
  not implemented.
- Evidence: `shardloom-plan/src/plan_ir.rs`, `shardloom-cli/src/workflow_planning.rs`,
  `shardloom-cli/tests/plan_portability_snapshots.rs`,
  `docs/architecture/substrait-report-only-contract.md`
- [x] Native-first Plan IR, serialization skeletons, and imported-plan capability gates exist.
- [x] Imported plan surfaces preserve no-fallback and capability diagnostics.
- [x] `GAR-PERF-2B` adds optimizer trace follow-through over Plan IR. Current trace rows keep
  report-only before/after plan-digest placeholders, rewrite safety, evidence preservation,
  materialization boundaries, no-fallback status, and claim gates visible, and no rewrite is treated
  as runtime-supported.
- [x] `GAR-0022-A` adds the Substrait report-only import/export contract with
  `substrait_report_contract_schema_version=shardloom.substrait_report_only_contract.v1`,
  parser/exporter dependency status, imported-plan execution blockers,
  `substrait_external_engine_invoked=false`, `substrait_fallback_attempted=false`, and
  `substrait_claim_gate_status=not_claim_grade`.
- [ ] Real Substrait parser/exporter support, dependency adoption, round-trip fixtures, and
  imported-plan execution remain incomplete.

### RFC 0023 - Extension, Plugin ABI, and Sandboxing

- Source:
  [`docs/rfcs/0023-extension-plugin-abi-sandboxing.md`](../rfcs/0023-extension-plugin-abi-sandboxing.md)
- Current read: Manifest-first ABI reports and plugin/UDF sandbox blockers exist; runtime plugin
  loading is not implemented.
- Evidence: `shardloom-core/src/extension.rs`,
  `docs/architecture/plugin-abi-udf-sandbox-blocker.md`,
  `shardloom-cli/tests/extension_manifest_effect_matrix_snapshots.rs`,
  `shardloom-cli/tests/capability_discovery_snapshots.rs`,
  `shardloom-cli/tests/plan_only_invariants.rs`,
  `shardloom-cli/tests/typed_envelope_compatibility_lock.rs`
- [x] Extension metadata, permissions, effect declarations, sandbox posture, and provenance are
  represented.
- [x] Tests lock the current non-executing inspection posture.
- [x] `GAR-0023-A` adds `shardloom.plugin_abi_udf_sandbox_blocker.v1`; Plugin/UDF runtime
  admission exposes deterministic blockers for ABI inventory, dynamic library loading, Rust/WASM/
  Python/SQL/external/table-function UDFs, plugin lifecycle transitions, sandbox evidence binding,
  license/provenance attestation, and unsupported diagnostics with
  `claim_gate_status=not_claim_grade`, `abi_loading_supported=false`,
  `dynamic_loading_performed=false`, `extension_code_executed=false`,
  `udf_execution_performed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] Plugin/UDF runtime admission remains a blocker contract, not runtime support.
- [x] `GAR-0023-B` closes the duplicate plugin/UDF sandbox row as a completed admission and
  user-surface contract: `ShardLoomClient` and `ShardLoomContext` expose side-effect-free extension
  registry/inspection helpers, UDF runtime posture helpers, and the scoped built-in deterministic
  nullable-int64 scalar UDF fixture smoke. The fixture executes only ShardLoom's built-in
  deterministic UDF path and preserves explicit effect, no-fallback, and external-engine evidence.
  Real dynamic plugin ABI loading, sandboxed third-party code execution, arbitrary Python/WASM/Rust/
  SQL/table-function UDF execution, LLM/API calls, embeddings, and external effects remain blocked
  by the RFC 0011 modular-extensibility row, CG-20/CG-21 capability gates, and UDF external-effect
  blocker matrices rather than duplicated here.

### RFC 0024 - Release Engineering, API Compatibility, and Packaging

- Source:
  [`docs/rfcs/0024-release-engineering-api-compatibility-packaging.md`](../rfcs/0024-release-engineering-api-compatibility-packaging.md)
- Current read: Release gates and dry-run evidence exist; publication is not complete.
- Evidence: `shardloom-core/src/release.rs`, `shardloom-cli/src/packaging_deployment.rs`,
  `scripts/check_release_readiness.py`, `scripts/release_dry_run_proof.py`,
  `scripts/release_provenance_dry_run.py`, `scripts/run_release_validation_evidence.py`,
  `scripts/check_package_channel_readiness.py`,
  `docs/release/package-channel-readiness-matrix.json`
- [x] Release-readiness, provenance, SBOM, security, packaging, and no-fallback gate evidence
  surfaces exist.
- [x] Local dry-run workflows avoid package publication and external side effects.
- [x] `GAR-COMMERCIAL-1A` and `GAR-COMMERCIAL-1B` add adoption and package-channel readiness
  follow-through: one documented local smoke path plus a channel matrix for GitHub pre-release,
  TestPyPI, PyPI, Homebrew, Scoop/winget, conda-forge, GHCR, and future public Rust API crates. No
  channel may be marked ready without install/uninstall, clean install, smoke, SBOM/checksum,
  provenance, and rollback/yank evidence; PyPI must use Trusted Publisher/OIDC.
- [x] `GAR-0024-A` adds `shardloom.publication_api_schema_stability_gate.v1`; `release-plan`
  exposes fail-closed rows for `api_compatibility_window`, `schema_compatibility_window`,
  `package_identity_approval`, `signing_policy_decision`, `checksum_manifest`, `sbom_bundle`, and
  `publication_approval` with `publication_api_schema_gate_status=blocked`,
  `claim_gate_status=not_claim_grade`, `public_release_claim_allowed=false`,
  `public_package_claim_allowed=false`, `package_publication_performed=false`, `tag_created=false`,
  `signing_key_used=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
- [ ] First public release/package publication remains incomplete and is carried by
  `GAR-0043-B`; stable API/schema windows and signing decisions now have explicit fail-closed gate
  rows but are not approved for public claims.

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
- [x] `GAR-0025-A` adds `shardloom.competitive_replacement_sufficiency_gate.v1`; `release-plan`
  exposes fail-closed rows for `correctness_evidence`, `benchmark_evidence`, `native_io_evidence`,
  `execution_certificate_evidence`, `capability_coverage_evidence`, `no_fallback_policy_evidence`,
  and `release_publication_evidence` with `claim_gate_status=not_claim_grade`,
  `competitive_replacement_sufficiency_gate_all_claims_blocked=true`,
  `public_engine_replacement_claim_allowed=false`, `spark_displacement_claim_allowed=false`,
  `superiority_claim_allowed=false`, `production_platform_claim_allowed=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [ ] Full competitive replacement remains incomplete until every sufficiency row has
  workload-scoped evidence broad enough for the exact public claim.

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
- [x] GAR-0026-V scoped the conjunctive bridge selection-vector metric aggregation proof, then the
  all-features route evidence was narrowed back to the claim boundary the reopened reader can
  actually prove. Current CSV/JSON prepared/native `selective filter` rows report primitive reopened
  reader chunks with
  `encoded_predicate_provider_status=blocked_until_reader_generated_kernel_input_certificate`;
  selection-vector-backed metric aggregation remains a scoped fixture/direct-kernel proof until a
  generated encoded-kernel input certificate is present. Rows preserve
  `encoded_predicate_provider_operator_execution_class=residual_native`,
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
  existing local staged-output/manifest/commit/rollback/recovery/object-store/table-maintenance
  evidence refs, and info-level no-fallback diagnostics for generalized manifest serialization,
  generalized sink commit, object-store commit, table/catalog commit, lakehouse transaction commit,
  native source/sink commit, Foundry dataset transaction commit, upstream Vortex write API
  execution, live/hybrid checkpoint commit, and output-payload fidelity claims.
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
- [x] `GAR-NOVEL-1D` adds
  `shardloom.traditional_analytics.bayesian_claim_confidence.v1` as a report-only/not-fit Bayesian
  claim-confidence and regression schema in benchmark artifacts. It records posterior runtime
  distribution, credible interval, probability of regression, minimum iterations for claim-grade
  consideration, benchmark population refs, release policy refs, and uncertainty reason while
  keeping posterior fitting, benchmark recomputation, runtime/layout decisioning, claim upgrades,
  external engines, and fallback execution disabled.
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
- [x] `GAR-IOREUSE-1A` through `GAR-IOREUSE-1L`, `GAR-PERF-2J`, and `GAR-PERF-2K` now close the
  scoped local I/O-reuse evidence family for SourceState, VortexPreparedState, OutputPlan, local
  fanout, cache invalidation/fingerprint, evidence-safe reuse levels, session fingerprint reuse,
  cold-lane attribution, Vortex-native preparation, differential preparation, capillary I/O,
  scout ingress/triage, layout/write advice, and copy-budget/buffer-lifecycle evidence. Scoped
  local SQL/Python and generated-output writes emit first-class `sink_artifact_*`
  refs/digests/counts/replay/commit evidence for primary and fanout local sinks. Remaining
  object-store/table/real-Foundry sink artifact proof, benchmark-family fanout promotion,
  persistent cache/session promotion, and claim-grade gates are carried by the canonical
  compute-flow `GAR-IOREUSE-1` row below instead of this duplicate blocker.
- [x] `GAR-SCALE-1` adds the Spark-level scale contract and any-volume readiness follow-through.
  ShardLoom does not claim literal "any volume" support; current scale evidence classifies rows as
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
  `memory_spill_claim_gate_status=not_larger_than_memory_grade`. `GAR-SCALE-1D` now adds the
  report-only `shardloom.traditional_analytics.shuffle_repartition.v1` row contract with shuffle
  requirement/strategy, partitioning, local-combine/global-merge, broadcast, skew, shuffle spill,
  retry, correctness-digest, no-fallback fields, and
  `shuffle_claim_gate_status=not_shuffle_scale_grade`. `GAR-SCALE-1E` now adds the report-only
  `shardloom.traditional_analytics.object_table_scale_ladder.v1` row contract with object-store
  URI/listing/split-planning/read/write/commit statuses, table metadata/runtime/commit/rollback
  statuses, credential/network/ETag/commit/rollback evidence, separate object-store read/write and
  table runtime/commit claim gates, no-fallback fields, and
  `object_table_ladder_claim_gate_status=not_object_table_scale_grade`. `GAR-SCALE-1F` now adds the
  report-only `shardloom.traditional_analytics.distributed_protocol.v1` row contract with
  coordinator, worker, task lease, task attempt, split, retry, result-fragment, merge, no-fallback
  fields, `distributed_claim_status=report_only`, and
  `distributed_claim_gate_status=not_distributed_runtime_grade`. `GAR-SCALE-1G` now adds
  `shardloom.traditional_analytics.scale_benchmark_profile.v1` with scale benchmark profile
  definitions, required scenario/metric vocabulary, row-level publishing posture, synthetic
  metadata-only boundaries, real-input-byte/correctness proof requirements, public-leaderboard
  separation, no-fallback fields, and `scale_benchmark_claim_gate_status=not_scale_benchmark_grade`.
  `GAR-SCALE-1H` now adds `shardloom.foundry_scale_proof_boundary.v1` to the local Foundry proof
  report with Foundry runtime/compute/Spark, input/output dataset counts, staged input bytes,
  execution mode, split count, memory budget, evidence dataset, no-fallback, external-engine, and
  public-claim fields while keeping Foundry scale proof report-only. Synthetic
  metadata-only evidence, report-only protocol rows, external baselines, and managed-platform
  orchestration cannot satisfy ShardLoom runtime scale claims, Spark-replacement claims, or
  no-fallback/no-external-engine proof.
- [x] `GAR-0029-A` adds
  `shardloom.cg5_cg6_stateful_reuse_evidence_expansion.v1` across `correctness-harness-plan`,
  `benchmark-claim-evidence-plan`, and `stateful-reuse-plan`; the shared report exposes blocked
  rows for `cg5_correctness_closeout`, `cg6_benchmark_closeout`,
  `cg16_execution_certificate_linkage`, `cg19_native_io_linkage`,
  `cg17_stateful_reuse_boundary_evidence`, `cg17_stable_reuse_key_invalidation`,
  `cg17_reuse_benchmark_constitution`, and `public_claim_attachment` with
  `gar_0029_evidence_expansion_claim_gate_status=not_claim_grade`,
  `gar_0029_evidence_expansion_cache_read_allowed=false`,
  `gar_0029_evidence_expansion_cache_write_allowed=false`,
  `gar_0029_evidence_expansion_cache_replay_allowed=false`,
  `gar_0029_evidence_expansion_incremental_execution_allowed=false`,
  `gar_0029_evidence_expansion_stateful_reuse_runtime_supported=false`,
  `gar_0029_evidence_expansion_performance_claim_allowed=false`,
  `gar_0029_evidence_expansion_superiority_claim_allowed=false`,
  `gar_0029_evidence_expansion_fallback_attempted=false`, and
  `gar_0029_evidence_expansion_external_engine_invoked=false`.
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
- [x] `GAR-COMMERCIAL-1A` turns the source-local dry-run and first-10-minutes flow into a single
  adoption path for install/local build, smoke, generated-source posture, tiny prepared/native
  example, and evidence inspection without requiring architecture-doc reading.
- [x] `GAR-0030-A` adds `universal_harness_execution_gate_status`,
  `universal_harness_execution_allowed=false`, attached/missing evidence refs, and required
  capability, execution-certificate, Native I/O, policy/no-fallback, output, correctness, and
  benchmark evidence fields to keep harness execution blocked until evidence is attached.
- [ ] Imported-plan execution and actual universal harness execution remain unimplemented without
  capability, certificate, Native I/O, and no-fallback evidence.

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
- [x] `GAR-COMPAT-1` adds the universal compatibility completion follow-through. `GAR-COMPAT-1A`
  now projects the scoreboard into `shardloom.universal_compatibility_coverage_scoreboard.v1`, CLI
  `capabilities compatibility --format json`, Python `ctx.compatibility_scoreboard()`, website
  status rendering, and release-readiness checks. The matrix classifies CSV, JSONL/JSON, Parquet,
  Arrow IPC, Avro, ORC, Excel, SQLite, Postgres/MySQL, JDBC/ODBC, S3/GCS/ADLS,
  Iceberg/Delta/Hudi, Vortex, generated/source-free outputs, Python rows/DataFrame, SQL
  VALUES/literals, REST/Flight/ADBC, and Foundry as runtime-supported, smoke-supported,
  report-only, blocked, or not-planned while preserving `fallback_attempted=false` and
  `external_engine_invoked=false`. `GAR-COMPAT-1B` adds
  `shardloom.universal_compatibility.generated_output_contract.v1` so no-dataset smoke, scoped
  Python generated-output smokes, SQL/DataFrame report-only rows, local-output-only posture, and
  object-store/Foundry blockers are visible from compatibility/status surfaces. `GAR-COMPAT-1C`
  adds `shardloom.universal_compatibility.object_store_admission_ladder.v1` so S3/GCS/ADLS URI
  parse, credential policy, public read, authenticated read, byte-range read, full-file read, local
  cache, write staging, and commit protocol are visible as separate report-only or blocked gates
  while preserving no credential, network, provider-probe, object-store I/O, write, fallback, or
  external-engine effects. `GAR-COMPAT-1D` adds
  `shardloom.universal_compatibility.table_format_boundary_matrix.v1` so Iceberg, Delta, and Hudi
  metadata read, table scan, snapshot/time-travel, partition evolution, delete/tombstone, append,
  merge/update/delete, commit, rollback, catalog interaction, and object-store coupling are visible
  as separate report-only or blocked gates while preserving no catalog, object-store, table
  metadata, table data, write, commit, rollback, fallback, or external-engine effects.
  `GAR-COMPAT-1E` adds
  `shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1` so SQLite, Postgres,
  MySQL, JDBC/ODBC, Snowflake, BigQuery, and Databricks SQL import/export/query-pushdown posture is
  visible. SQLite is now the only local fixture exception through `sqlite-local-import-export-smoke`
  named-table import/export; all rows still preserve no credential resolution, network probe, driver
  loading, query pushdown, fallback, or external-engine effects, and the network database/warehouse
  rows remain blocked.
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
- [x] `GAR-RUNTIME-IMPL-4F-S1` adds scoped flat JSONL/NDJSON local input runtime to
  `sql-local-source-smoke`. Rows now emit `source_format=jsonl`, content-digest fingerprinting,
  SourceState-style id/digest/reuse posture, schema digest, local read/parse timing, deterministic
  blockers for nested JSON values and unsupported extensions, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
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
- [x] `GAR-RUNTIME-IMPL-6D` extends the DataFrame method matrix and query builder with scoped
  pandas-style selection/dtype affordances: `query(...)`, schema-declared `dropna(...)`,
  schema-declared `astype(...)`, and `nlargest(...)` / `nsmallest(...)` lower to existing
  ShardLoom local-source routes, while duplicate-mask, conditional-replacement, and index-state
  methods fail through deterministic no-fallback diagnostics.
- [x] `GAR-0032-A` adds `docs/architecture/sql-parser-binder-readiness.md` and strengthens
  `workflow-unsupported-plan sql-parse|sql-bind|sql-plan|sql-execute --format json` with
  `support_status=unsupported`, `claim_gate_status=not_claim_grade`, `parser_executed=false`,
  `binder_executed=false`, and `planner_executed=false` so SQL text requests have deterministic
  diagnostics without parser, binder, planner, runtime, external engine, or fallback execution.
- [x] `GAR-0032-C` adds `shardloom.external_effect_blocker_matrix.v1` and
  `docs/architecture/udf-external-effect-blocker-matrix.md` so UDFs, API calls, LLM calls,
  embedding generation, vector search, plugin execution, media extraction, and network egress are
  classified with deterministic permission/effect blockers. The rows keep
  `support_status=blocked`, `permission_status=policy_required`,
  `effect_status=denied_by_default`, `runtime_execution=false`, `effect_executed=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [x] `GAR-0032-D` adds `shardloom.unstructured_adapter_capability_matrix.v1` and
  `docs/architecture/unstructured-adapter-capability-matrix.md` so document references, text
  extraction, media decode/extraction, embeddings, vector search, local file adapters,
  database/warehouse adapters, object-store/table adapters, event/API/SaaS adapters, and
  source/sink metadata are classified as report-only or blocked. The rows keep
  `runtime_execution=false`, `source_io_performed=false`, `sink_io_performed=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.
- [x] `GAR-0032-E` adds `shardloom.best_default_certification_gate.v1` and
  `docs/architecture/best-default-certification-gate.md` to the world-class sufficiency and
  certification capability surfaces. The gate lists missing workload constitution, correctness,
  benchmark, execution-certificate, Native I/O, materialization/decode, no-fallback policy,
  release/security, UX/install, capability snapshot, scorecard, and dossier evidence before any
  best-default language is allowed. It keeps `claim_gate_status=not_claim_grade`,
  `best_default_language_allowed=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`; best-default certification evidence remains incomplete.
- [x] `GAR-GEN-1A` through `GAR-GEN-1E` now close the generated-source contract/API-admission
  surface: `GeneratedSourceCertificate` rows separate `no_dataset_smoke`, `user_generated_source`,
  and `engine_native_generated_source`; scoped local user-row, literal-table, calendar, range,
  sequence, SQL literal/VALUES, SQL generator/range-projection, and generated `with_column` paths
  carry generated-source and output evidence without broad SQL/DataFrame runtime, object-store or
  Foundry execution, external engines, or fallback. Remaining broader generated-output runtime work
  is carried by the canonical compute-flow `GAR-GEN-1` row below.
- [x] `GAR-NOVEL-1A` keeps `GeneratedSourceCertificate` aligned with Python/API docs,
  SQL/DataFrame capability rows, Foundry proof docs, and future lineage/telemetry/confidence refs
  through `shardloom.generated_source_evidence_alignment.v1`. The rows cover
  `no_dataset_smoke`, `python_generated_source_write`, `sql_dataframe_source_free`, and
  `foundry_generated_output`; OpenLineage/OTel exports stay disabled/report-only, Bayesian
  confidence remains disabled/advisory, and no SQL/DataFrame, object-store/lakehouse, Foundry,
  production, performance, or package claim is promoted.
- [x] `GAR-NOVEL-1B` adds `shardloom.openlineage_facet_mapping.v1` as a report-only
  observability capability. It maps execution mode, no-fallback, Native I/O certificate,
  materialization boundary, claim gate, generated-source, and Vortex artifact evidence into
  ShardLoom-owned future OpenLineage custom facet placeholders while keeping export disabled,
  event emission disabled, schema publication disabled, backend/client dependency disabled,
  network calls disabled, `fallback_attempted=false`, `external_engine_invoked=false`, and
  `claim_gate_status=not_claim_grade`.
- [x] `GAR-NOVEL-1C` adds `shardloom.opentelemetry_trace_export_contract.v1` as the companion
  report-only observability capability for future OTel spans and metrics. It maps benchmark/runtime
  stage-timing fields to span placeholders without adding an OTel dependency, configuring OTLP,
  configuring a collector/backend, emitting telemetry, making network calls, or changing runtime
  support.
- [x] `GAR-COMPAT-1` keeps broad adapter and user-surface compatibility separate from runtime
  support. Python rows/DataFrame, SQL VALUES/literals, REST/Flight/ADBC, external databases, and
  generated/source-free output remain report-only or blocked unless a narrower evidence-bearing
  slice upgrades the row.
- [ ] Executable SQL parser/binder/runtime, DataFrame execution, UDF runtime, notebook runtime,
  universal-adapter runtime, unstructured/media runtime, and best-default certification evidence
  remain incomplete.

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
- [x] Scoped DataFrame breadth now includes supported `query`, `dropna`, `astype`,
  `nlargest`, and `nsmallest` routes plus deterministic blockers for duplicated-row masks,
  conditional replacement, and index-state APIs. Broad pandas parity, production DataFrame
  execution, and performance equivalence remain gated.
- [x] Source-free generated-output workflows such as `ctx.from_rows(...).write(...)`,
  `ctx.from_rows(...).with_column(literal).write(...)`,
  `ctx.literal_table(...).write(...)`, `ctx.calendar(...).write(...)`, and
  `ctx.range(...).write(...)`/`ctx.range(...).with_column(int64_expression).write(...)`/
  `ctx.sequence(...).write(...)` now have scoped local JSONL/CSV smoke paths, and
  `shardloom.generated_source_api_admission.v1` exposes deterministic admission rows for
  SQL literal `SELECT`, SQL `VALUES`, scoped SQL source-free range projection, SQL
  `generate_series`/`range`, scoped DataFrame literal projection, and scoped generated
  `with_column`. `GAR-COMPAT-1B` projects the same posture into the universal compatibility
  scoreboard and website/status rows without broadening runtime. This closes the duplicated
  user-workflow generated-output blocker; broad SQL execution beyond the scoped source-free
  literal/VALUES/range-generator smokes, broad expression-backed DataFrame generation,
  engine-native values/synthetic generators, object-store output, and Foundry generated-output
  runtime remain guarded by their broad SQL/DataFrame/object-store/Foundry rows. No-input smoke
  does not count as generated-output execution.
- [x] `GAR-COMPAT-1` is the user-workflow compatibility scoreboard for source/sink/adapters. It
  separates plan/report coverage from runtime coverage for local files, Vortex, generated-output
  APIs, external databases, object stores, table formats, REST/Flight/ADBC, and Foundry.
- [x] `GAR-COMMERCIAL-1C` adds a generated buyer-facing website/status matrix so users can quickly
  determine whether ShardLoom supports, smoke-supports, reports-only, blocks, plans, or does not
  plan common surfaces without reading the phase plan.
- [x] `GAR-COMMERCIAL-1F` adds workflow recipes without hiding unsupported paths.
- [x] `GAR-IOREUSE-1D` adds a report-only cross-format fanout benchmark and workflow lane. It shows
  required fanout case IDs, reusable source/prepared/output planning fields, deterministic blocker
  IDs/reasons, timing/reuse columns, `fanout_output_count=0`, and no-fallback/no-external-engine
  evidence without treating fanout as runtime support, performance proof, or a requirement that
  input and output formats match.
- [x] `GAR-DOCS-1` adds non-expert workflow documentation coverage, recipe generation, glossary
  links, exact references, and website "Can I use this?" status-matrix pages. It documents current
  local/smoke paths and blockers without creating production ETL, SQL/DataFrame,
  object-store/lakehouse, Foundry, performance, or Spark-replacement claims.
- [x] `GAR-0033-A` adds `shardloom.etl_workflow_capability_matrix.v1` to the CLI workflow
  capability view and Python `ctx.etl_workflow_matrix()`. The matrix separates local ready/smoke
  workflow rows, report-only SQL/DataFrame and data-quality API posture, and blocked object-store,
  table/lakehouse, and production ETL certification rows while preserving
  `fallback_attempted=false`, `external_engine_invoked=false`, and
  `claim_gate_status=not_claim_grade`.
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
- [x] `GAR-0034-A` adds `shardloom.live_hybrid_fabric_freshness_gate.v1` to
  `engine-capability-matrix`, `capabilities engines`, and Python `ctx.engine_capability_matrix()`.
  The gate keeps broker, durable checkpoint/state-store, unbounded scheduler, object-store commit,
  table/catalog snapshot, production freshness, exactly-once, benchmark, and Spark-displacement
  claims blocked while preserving fixture-scoped freshness evidence, baseline/oracle-only posture,
  `fallback_attempted=false`, `external_engine_invoked=false`, and
  `claim_gate_status=not_claim_grade`.
- [ ] Production live/hybrid engines, broker/state-store runtime, object-store execution,
  production freshness/exactly-once claims, and runtime baseline/oracle integrations remain
  incomplete.

### RFC 0035 - REST, Event, and Remote API Surface

- Source: [`docs/rfcs/0035-rest-event-remote-api-surface.md`](../rfcs/0035-rest-event-remote-api-surface.md)
- Current read: Contract-first API docs and reports exist; no production server exists.
- Evidence: `docs/api/shardloom-openapi-v1.yaml`, `docs/api/shardloom-asyncapi-events-v1.yaml`,
  `shardloom-cli/src/rest_api_planning.rs`, `python/src/shardloom/client.py`,
  `shardloom-cli/tests/api_protocol_snapshots.rs`, `python/tests/test_cli_client.py`
- [x] OpenAPI/AsyncAPI docs, REST planning reports, Python wrapper views, and protocol snapshots
  exist.
- [x] Discovery/server contract paths preserve `server_started=false` where no server starts.
- [x] `GAR-NOVEL-1B` adds planned report-only OpenLineage facet mapping follow-through. No lineage
  event, schema publication, OpenLineage client dependency, network call, backend integration, or
  production API/lineage claim is authorized by those rows alone.
- [x] `GAR-NOVEL-1C` adds planned report-only OpenTelemetry trace mapping follow-through. No
  telemetry exporter, network call, backend integration, or production API claim is authorized by
  those rows alone.
- [x] `GAR-COMMERCIAL-1D` adds the report-only enterprise evidence export pack with
  `shardloom.enterprise_evidence_export_pack.v1`, ShardLoom JSON evidence, OpenLineage facet
  payload previews, OpenTelemetry span/metric payload previews, optional Markdown summary,
  redaction report, opt-in/no-network/no-backend posture, and no production observability claim.
- [x] `GAR-0035-A` adds `shardloom.rest_api_runtime_unsupported_contract.v1` to
  `rest-api-contract-plan` and Python `ctx.rest_api_contract_plan()`. The gate keeps HTTP listener,
  remote execution, Flight/ADBC transport, external broker integration, dependency-expanded server,
  production API, benchmark, and Spark-displacement claims blocked while preserving
  OpenAPI/plan-preview/result-delivery report-only rows, `server_started=false`,
  `network_listener_opened=false`, `fallback_attempted=false`, `external_engine_invoked=false`, and
  `claim_gate_status=not_claim_grade`.
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
- [x] `GAR-COMMERCIAL-1E` adds the Foundry dev-stack starter kit with
  `shardloom.foundry_dev_stack_starter_kit.v1`, exact local proof commands, staged-input posture,
  generated-output/no-dataset separation, local certificate-style output, and explicit blockers for
  real Foundry output API evidence. It remains local-style proof only and exposes
  `foundry_runtime_invoked=false`, `foundry_compute_invoked=false`, and
  `foundry_spark_invoked=false` without invoking Foundry, credentials, direct S3/object-store
  runtime, Spark, or external compute.
- [x] `GAR-IOREUSE-1G` adds Foundry no-input generated-output fanout posture to the local
  proof report through `shardloom.foundry_generated_output_fanout_posture.v1`. It remains
  report-only: `generated_output_execution_performed=false`, generated-source and output
  certificates are not emitted, `fanout_output_count=0`, and direct S3/object-store writes,
  Foundry Spark, external engines, and Foundry production claims remain blocked.
- [x] `GAR-GEN-1F` adds the dedicated Foundry generated-output proof boundary through
  `shardloom.foundry_generated_output_boundary.v1`. It requires future admitted generated-output
  proof to write result/evidence datasets through Foundry output APIs while current local proof
  keeps `foundry_output_api_invoked=false`, `foundry_result_dataset_written=false`,
  `foundry_evidence_dataset_written=false`, `direct_s3_read_invoked=false`,
  `direct_s3_write_invoked=false`, `object_store_read_invoked=false`,
  `object_store_commit_invoked=false`, `fallback_attempted=false`, and
  `external_engine_invoked=false`.
- [x] `GAR-0036-A` adds `shardloom.foundry_package_proof_boundary_matrix.v1` and
  `docs/foundry/package-proof-boundary-matrix.md` so local Foundry-style proof rows are separated
  from blocked `shardloom-foundry` package publication, Artifact Repository publication, Foundry
  service invocation, Compute Module invocation, virtual-table native execution, dataset transaction
  runtime, and F10 workload-certified deployment. The matrix keeps `support_status=report_only`,
  `claim_gate_status=not_claim_grade`, `foundry_runtime_invoked=false`,
  `foundry_compute_invoked=false`, `foundry_spark_invoked=false`, `fallback_attempted=false`,
  `external_engine_invoked=false`, and `public_foundry_claim_allowed=false`.
- [ ] Production `shardloom-foundry`, package publication, Foundry service invocation, Artifact
  Repository publication, Compute Module, virtual-table native execution, Foundry dataset
  transaction runtime, and F10 workload-certified deployment remain runtime-incomplete.

### RFC 0037 - Client, Wrapper, SDK, and Ecosystem Integration Surface

- Source:
  [`docs/rfcs/0037-client-wrapper-sdk-ecosystem-surface.md`](../rfcs/0037-client-wrapper-sdk-ecosystem-surface.md)
- Current read: Wrapper architecture, the Python CLI wrapper, typed Python capability posture
  accessors, and a wrapper/connector implementation registry exist; ecosystem clients remain
  blocked or report-only unless later implementation evidence admits them.
- Evidence: `shardloom-core/src/wrapper_architecture.rs`,
  `shardloom-cli/tests/python_wrapper_snapshots.rs`, `python/src/shardloom/client.py`,
  `python/src/shardloom/context.py`, `python/tests/test_cli_client.py`,
  [`docs/architecture/wrapper-connector-implementation-registry.md`](wrapper-connector-implementation-registry.md)
- [x] One-protocol/many-thin-wrappers architecture and no-fallback wrapper reports exist.
- [x] Python wrapper reads CLI JSON rather than creating an alternate execution path.
- [x] `GAR-0010-A` exposes no-scraping Python capability posture over existing `OutputEnvelope`
  fields while preserving side-effect-free capability discovery and no runtime expansion.
- [x] `GAR-0037-A` exposes `shardloom.wrapper_connector_implementation_registry.v1` through
  `capabilities api-surfaces --format json` and Python `ctx.wrapper_connector_registry()`.
- [x] Generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight SQL,
  ADBC, JDBC/ODBC, BI, Grafana, and Foundry package rows are explicitly `report_only` or `blocked`
  with deterministic diagnostics, no dependency expansion, no network listener, no data-plane
  bridge, `fallback_attempted=false`, `external_engine_invoked=false`, and
  `claim_gate_status=not_claim_grade`.

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
- [x] GAR-0039-A migrates the API-surface capability family further into typed envelope slots:
  `capabilities api-surfaces --format json` now emits an inline
  `api_surface_capability_report` artifact for the wrapper/connector registry, and Python
  `OutputEnvelope.field_map` prefers typed payload fields before the temporary mirror.
- [x] GAR-0039-B centralizes typed-envelope integration-test helpers and adds an inline
  `universal_harness_report` artifact for the Foundry optional-harness boundary without enabling
  Foundry runtime, universal harness execution, external baselines as runtime, or fallback.
- [x] `GAR-PERF-2A` adds typed-envelope and benchmark-row fields for
  `evidence_level=minimal_runtime|certified|full_replay` on the scoped prepared/native batch
  runner, so callers can distinguish proof level from execution mode and engine mode without
  inferring claim status from prose.
- [ ] Legacy flat `fields` mirror, remaining command-family result migration beyond the
  API-surface and universal-harness report families, remaining golden fixtures, and additional
  physical handler splits remain incomplete.

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
  pruning, and FSST/dictionary string equality where available. Current prepared/native fixtures
  execute observed bitpacked, sequence, and constant filter inputs plus dictionary group-by evidence
  from real Vortex reader chunks, keep sorted/min-max and FSST/string pairs blocked or not
  available, and preserve `encoded_native_claim_allowed=false`.
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
- [x] `GAR-0040-A` adds
  `shardloom.comparative_rerun_managed_platform_gate.v1` across
  `benchmark-claim-evidence-plan` and `release-plan`, separating
  `local_full_comparative_rerun`, `external_baseline_oracle_rows`,
  `managed_platform_design_reference_rows`, `managed_platform_credential_policy`,
  `claim_grade_artifact_publication`, and `fallback_and_external_execution_boundary`. The gate
  records
  `comparative_rerun_managed_platform_gate_local_comparative_rerun_performed=false`,
  `comparative_rerun_managed_platform_gate_external_baselines_comparison_only=true`,
  `comparative_rerun_managed_platform_gate_managed_platform_lanes_comparison_only=true`,
  `comparative_rerun_managed_platform_gate_managed_platform_credentials_required=true`,
  `comparative_rerun_managed_platform_gate_managed_platform_credentials_resolved=false`,
  `comparative_rerun_managed_platform_gate_managed_platform_dependencies_added=false`,
  `comparative_rerun_managed_platform_gate_managed_platform_execution_performed=false`,
  `comparative_rerun_managed_platform_gate_performance_claim_allowed=false`,
  `comparative_rerun_managed_platform_gate_fallback_attempted=false`, and
  `comparative_rerun_managed_platform_gate_external_engine_invoked=false`.
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
- [x] `GAR-0041-A` adds `shardloom.per_claim_evidence_attachment_matrix.v1` to release-plan and
  hard release-readiness surfaces. It binds public release, package, performance/superiority,
  Spark-displacement, engine-replacement, production SQL/DataFrame, object-store/lakehouse, and
  Foundry/platform claim rows to required test, benchmark, certificate, Native I/O, security,
  provenance, unsupported-path, no-fallback, and approval evidence while reporting
  `per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade`,
  `per_claim_evidence_attachment_matrix_all_claims_blocked=true`,
  `per_claim_evidence_attachment_matrix_public_release_claim_allowed=false`,
  `per_claim_evidence_attachment_matrix_public_package_claim_allowed=false`,
  `per_claim_evidence_attachment_matrix_performance_claim_allowed=false`,
  `per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false`,
  `per_claim_evidence_attachment_matrix_fallback_attempted=false`, and
  `per_claim_evidence_attachment_matrix_external_engine_invoked=false`.
- [ ] Passing public release/package/performance/production/platform claims remain incomplete until
  every matrix row has attached passing evidence and explicit human approval.

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
- [x] `GAR-PERF-2C` adds Vortex Scan API pushdown completion across prepared/native scenario
  families. Prepared/native rows now map supported filter/projection/limit intent into
  source-backed scan evidence, keep unsupported expressions and order-sensitive limits as
  deterministic blockers or ShardLoom-native residuals, and preserve no-fallback/no-external-engine
  fields through runtime rows, benchmark artifacts, CLI capability rows, Python accessors, and
  contract tests.
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
- [x] The Vortex-adjacent `GAR-IOREUSE-1` source/sink concept check is complete for the scoped
  SourceState, VortexPreparedState, OutputPlan, local fanout, cache invalidation, cold-lane,
  capillary, scout-ingress, layout/write-advice, and copy-budget evidence surfaces. Current wrappers
  preserve Native I/O, materialization/decode, no-fallback, output metadata, and claim-gate evidence
  without inventing a hidden fallback path. Remaining generalized Source/Split, broader sink,
  object-store/table/Foundry, benchmark-family fanout, and persistent-cache work is carried by the
  canonical compute-flow `GAR-IOREUSE-1` row and the generalized Source/Split row below instead of
  this duplicate RFC-local blocker.
- [ ] Generalized Source/Split runtime paths, field-mask/predicate-ordering proof, layout/write
  runtime evidence, object-store runtime I/O, GPU/device execution, and managed-platform benchmark
  lanes remain incomplete.

### RFC 0043 - Security, Vulnerability, Exploit, and Supply-Chain Hardening

- Source:
  [`docs/rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md`](../rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md)
- Current read: Security hardening, fail-closed release architecture tracking, and local
  no-publication release rehearsal are substantially represented; public publication remains
  incomplete.
- Evidence: `SECURITY.md`, `shardloom-core/src/security.rs`, `scripts/check_dependency_audit.py`,
  `scripts/release_provenance_dry_run.py`, `scripts/check_release_security_gate.py`,
  `scripts/check_release_architecture_tracker.py`, `scripts/final_release_rehearsal.py`,
  `.github/workflows`
- [x] Security reports, malicious-input/path/redaction tests, dependency/provenance/security gates,
  CodeQL/Scorecard workflows, and OSS security docs exist.
- [x] Release metadata preserves no-fallback and supply-chain evidence requirements.
- [x] `GAR-0043-A` adds `shardloom.release_architecture_tracker_report.v1` and hard
  release-readiness integration. The tracker checks unchecked Global Architecture Review rows,
  unchecked phased-plan rows, RFC traceability, known unsupported paths, release security refs,
  provenance refs, and per-claim evidence refs while keeping
  `architecture_tracker_status=blocked`, `claim_gate_status=not_claim_grade`,
  `public_release_claim_allowed=false`, `public_package_claim_allowed=false`,
  `publication_attempted=false`, `tag_created=false`, `secrets_required=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false` until every required item and
  evidence attachment is closed.
- [x] `GAR-0043-B` adds `shardloom.final_release_rehearsal_report.v1` and
  `shardloom.local_publication_attestation_plan.v1`. The no-publication rehearsal aggregates local
  artifact, checksum, SBOM, provenance, security, architecture-tracker, package-channel,
  unsupported-path, per-claim, and publication/API/schema refs while keeping
  `rehearsal_status=passed`, `claim_gate_status=not_claim_grade`,
  `publication_authorization_status=human_approval_required`,
  `publication_human_approved=false`, `public_release_claim_allowed=false`,
  `public_package_claim_allowed=false`, `package_upload_attempted=false`,
  `feedstock_submission_attempted=false`, `marketplace_submission_attempted=false`,
  `signing_key_used=false`, `publication_attempted=false`, `tag_created=false`,
  `secrets_required=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
- [ ] Actual public package publication, release tags, signing, uploaded attestations, and final
  maintainer approval remain incomplete and outside autonomous execution.

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
  scoped local CSV, JSONL/NDJSON, and feature-gated Parquet/Arrow IPC/Avro/ORC smoke paths for
  `selective filter` and `filter + projection + limit`; broader operators, result sinks, and
  SQL/DataFrame direct transient runtime remain bounded by their specific evidence rows and are not
  Vortex-native, performance, production, or package-release claims.
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
        `source_state_prepare_timing_scope=batch_shared_session_open_only_deferred_family_build_reported_separately`
        so shared setup remains visible while deferred family build timing is reported separately.
        This closes scoped join-dimension source-state reuse only; broader row-state reuse,
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
- [x] `GAR-PERF-2C` adds Vortex Scan API pushdown completion. The flow keeps scan filter,
  projection, and limit pushdown evidence independent from encoded-native operator claims, and every
  prepared/native scenario family reports pushed-down fields or deterministic blockers through the
  `scan_pushdown_*` and `prepared_vortex_scan_pushdown_*` evidence contracts.
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
  no-fallback fields, and claim boundaries in benchmark artifacts. Scoped local SQL/Python and
  generated-output fanout now add local runtime sink evidence outside the benchmark matrix. The
  remaining flow is broader VortexPreparedState/session reuse, persistent OutputPlan/cache
  promotion, broad metadata fidelity, object-store/table and real Foundry sink proof, and
  claim-grade output/replay evidence while preserving distinct direct-transient,
  compatibility-import-certified, prepared-vortex, and native-vortex lanes without adding
  persistent cache, object-store/lakehouse, performance, or fallback claims ahead of their owning
  evidence. Cold-lane attribution, Vortex-native preparation, differential preparation, capillary
  I/O, scout ingress/triage, layout/write advice, and copy-budget/buffer evidence are closed as
  scoped evidence slices rather than remaining blockers on this canonical row.
- [x] `GAR-NOVEL-1` adds the evidence-native generated execution, lineage, observability, and
  confidence follow-up. OpenLineage facets are now mapped as opt-in/report-only placeholders;
  OpenTelemetry spans remain opt-in/report-only, and Bayesian confidence can block claims but
  cannot upgrade claim status by itself.
- [x] `GAR-COMMERCIAL-1` adds the adoption and commercial-readiness friction-reduction follow-up.
  One-command local proof, package-channel readiness, buyer-facing status, enterprise evidence
  export, Foundry dev-stack, and workflow recipes now remain claim-safe and evidence-gated before
  any public release or production/commercial readiness claim.
- [x] `GAR-DOCS-1` adds the Use Case Atlas and website status-matrix follow-up. The flow is
  explainable to non-experts by use case, status, execution mode, engine mode, input, output,
  evidence fields, and blockers without requiring readers to inspect RFCs or benchmark internals.
- [x] `GAR-COMPAT-1` is now the compute-flow follow-up for universal source/sink/adapter/user-surface
  compatibility coverage. The flow must keep compatibility coverage status distinct from runtime
  support for local files, Vortex, generated outputs, Python/DataFrame, SQL, databases, object
  stores, table formats, REST/Flight/ADBC, and Foundry.
- [x] `GAR-GEN-1` closes the scoped source-free generated-output execution follow-up. The flow now
  distinguishes no-dataset smoke from generated-output execution and admits scoped local user-row,
  literal-table, calendar/date-dimension, range, sequence, SQL `VALUES`, SQL literal `SELECT`, SQL
  `generate_series`/`range`, scoped SQL range projection, scoped generated `with_column`, and
  scoped DataFrame literal projection JSONL/CSV paths with generated-source, output-sink,
  execution, sink-artifact, and no-fallback evidence. Broader SQL/DataFrame expression runtime,
  object-store/lakehouse output, Foundry production output, broad Vortex writer behavior,
  package/production claims, and performance/Spark-displacement claims remain blocked by their
  owning broad rows rather than by this scoped generated-output row.
- [x] REST parity now emits the same policy, mode-selection, evidence, claim-gate, and
  no-fallback field families as CLI/Python surfaces through
  `shardloom.rest_api_surface_parity.v1` fields on every REST contract command. Python typed REST
  views expose the same common parity accessors, while
  `rest_api_runtime_equivalent_api_claim_allowed=false` keeps HTTP listener, remote execution,
  Flight/ADBC, broker, production API, package, and performance claims blocked.

## Follow-Up Rule

Before implementing any unchecked item from this review, use the corresponding `GAR-*` checklist
item in `docs/architecture/phased-execution-plan.md`. If the item is still too broad, split it in
the phase plan first. Keep each implementation batch cohesive enough to verify with focused tests,
evidence snapshots, or release/readiness checks without turning shared-context work into sliver PRs.
