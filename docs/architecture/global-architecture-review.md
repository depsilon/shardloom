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
- [ ] Executable SQL/DataFrame runtime, distributed runtime, broad lakehouse-compatible output, and
  general object-store execution remain incomplete.
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
- [ ] Full production Vortex segment extraction, broad operator coverage, and generalized
  materialization policy remain incomplete.

### RFC 0004 - Native Dataset, Manifest, Snapshot, and Incremental Change Model

- Source:
  [`docs/rfcs/0004-native-dataset-manifest-snapshot-incremental.md`](../rfcs/0004-native-dataset-manifest-snapshot-incremental.md)
- Current read: Contracts and local staged helpers exist; table/incremental execution is not broad.
- Evidence: `shardloom-core/src/manifest.rs`, `shardloom-vortex/src/staged_manifest.rs`,
  `shardloom-vortex/src/manifest_finalization.rs`, `shardloom-vortex/src/commit_protocol.rs`,
  `shardloom-vortex/tests/staged_write_readiness.rs`
- [x] Manifest, snapshot, change, staged write, and commit-protocol report contracts exist.
- [x] Local staged write/readiness evidence covers a narrow Vortex artifact path.
- [ ] CDC planning, table/catalog metadata reads, object-store commits, generalized manifest
  serialization, and broad transaction semantics remain incomplete.

### RFC 0005 - Vortex-Native File IO and Output Contract

- Source: [`docs/rfcs/0005-vortex-native-file-io-output.md`](../rfcs/0005-vortex-native-file-io-output.md)
- Current read: Vortex is first-class, but broad writer support remains gated.
- Evidence: `shardloom-vortex/src/file_io.rs`, `shardloom-vortex/src/metadata_async_boundary.rs`,
  `shardloom-vortex/src/read_planning.rs`, `shardloom-vortex/src/write_intent.rs`,
  `shardloom-vortex/src/output_payload.rs`, `shardloom-vortex/Cargo.toml`
- [x] Vortex-native file I/O, metadata-first planning, staged output, and write-intent surfaces
  exist.
- [x] Feature-gated Vortex write support is explicitly separated from unsupported paths.
- [ ] Broad Vortex reader/writer support, object-store Vortex I/O, general schema/encoding writes,
  and upstream Vortex write integration remain incomplete.

### RFC 0006 - Statistics, Pruning, and Metadata-Only Execution

- Source:
  [`docs/rfcs/0006-statistics-pruning-metadata-only-execution.md`](../rfcs/0006-statistics-pruning-metadata-only-execution.md)
- Current read: Narrow metadata-only and pruning paths exist.
- Evidence: `shardloom-core/src/encoded.rs`, `shardloom-vortex/src/metadata_pruning.rs`,
  `shardloom-vortex/src/metadata_executor.rs`,
  `shardloom-vortex/src/metadata_physical_kernel.rs`,
  `shardloom-vortex/src/encoded_read_executor.rs`, `shardloom-vortex/src/query_trace.rs`
- [x] Metadata pruning, metadata-only execution reporting, and encoded read readiness are present.
- [x] CLI snapshots preserve metadata and materialization/decode evidence for supported rows.
- [ ] Broad predicate, DType, nested, null, and production metadata-only coverage remains
  incomplete.

### RFC 0007 - Translation Layer Contract

- Source: [`docs/rfcs/0007-translation-layer-contract.md`](../rfcs/0007-translation-layer-contract.md)
- Current read: Translation/report contracts exist; compatibility writers are planned work.
- Evidence: `shardloom-core/src/translation.rs`, `shardloom-cli/src/vortex_planning.rs`,
  `shardloom-cli/tests/correctness_plan_snapshots.rs`
- [x] Plan/report contracts distinguish Vortex native, compatibility export, and unsupported paths.
- [x] Compatibility surfaces preserve no-fallback and evidence terminology.
- [ ] Actual Parquet, Arrow, Iceberg, Delta, and related compatibility output writers remain
  unimplemented or unsupported.

### RFC 0008 - Object-Store Runtime and Distributed Task Model

- Source:
  [`docs/rfcs/0008-object-store-runtime-distributed-tasks.md`](../rfcs/0008-object-store-runtime-distributed-tasks.md)
- Current read: Planning model exists; runtime is not implemented.
- Evidence: `shardloom-plan/src/object_store.rs`, `shardloom-cli/src/object_store_planning.rs`,
  `docs/architecture/rfc-phase-traceability.md`
- [x] Byte-range, task, retry, checkpoint, and commit planning contracts exist.
- [x] Object-store surfaces are explicitly report-only where runtime execution is absent.
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
- Current read: CLI/Python/report ergonomics are in place; mature runtime APIs are not.
- Evidence: `shardloom-cli/src/main.rs`, `shardloom-core/src/output.rs`,
  `shardloom-core/src/wrapper_architecture.rs`, `python/tests/test_cli_client.py`,
  `docs/architecture/rfc-coverage-followthrough.md`
- [x] CLI JSON, Python wrapper, typed outputs, and agent-facing contract packs exist.
- [x] Deterministic unsupported diagnostics preserve no-fallback semantics.
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
- Current read: Diagnostic/report surfaces are implemented for current commands.
- Evidence: `shardloom-core/src/diagnostics.rs`, `shardloom-core/src/capabilities.rs`,
  `shardloom-plan/src/explain.rs`, `shardloom-plan/src/estimate.rs`, `shardloom-cli/src/main.rs`
- [x] Typed JSON/text diagnostics, explain, estimate, doctor, and capability surfaces exist.
- [x] No-fallback status appears in envelopes and snapshots.
- [ ] Runtime-wide diagnostic propagation for planned distributed and object-store paths remains
  incomplete.

### RFC 0013 - Streaming, Zero-Copy, Zero-Decode, and Boundary Interoperability

- Source:
  [`docs/rfcs/0013-streaming-zero-copy-boundary-interoperability.md`](../rfcs/0013-streaming-zero-copy-boundary-interoperability.md)
- Current read: Streaming contracts exist; full streaming runtime is planned work.
- Evidence: `shardloom-exec/src/streaming.rs`, `shardloom-plan/src/plan_ir.rs`,
  `shardloom-vortex/src/lib.rs`, `shardloom-cli/tests/streaming_batch_plan_snapshots.rs`
- [x] Streaming plan, backpressure, encoded streaming batch, zero-decode, and boundary contracts
  exist.
- [x] Report surfaces expose representation and materialization boundaries.
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
- [ ] Broad runtime spill/OOM promotion and production enforcement remain limited to synthetic or
  local constraints.

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
- Evidence: `shardloom-plan/src/optimizer.rs`, `shardloom-exec/src/sizing.rs`,
  `shardloom-core/src/manifest.rs`
- [x] Optimizer, sizing, adaptive planning, dynamic work-shaping, layout health, and compaction
  planning surfaces exist.
- [x] Metadata-driven reports avoid claiming unsupported runtime behavior.
- [ ] Runtime adaptive execution, runtime filters, skew handling, and compaction writes remain
  incomplete.

### RFC 0017 - Fault Tolerance, Cancellation, and Recovery

- Source:
  [`docs/rfcs/0017-fault-tolerance-cancellation-recovery.md`](../rfcs/0017-fault-tolerance-cancellation-recovery.md)
- Current read: Recovery and commit contracts exist; broad execution is incomplete.
- Evidence: `shardloom-exec/src/recovery.rs`, `shardloom-vortex/src/commit_intent.rs`,
  `shardloom-vortex/src/commit_protocol.rs`
- [x] Recovery, cleanup, retry, cancellation, commit-intent, and commit-protocol reports exist.
- [x] CLI gates distinguish planned recovery from executed recovery.
- [ ] Broad retry, cancellation, and commit execution remain incomplete.

### RFC 0018 - Observability, Tracing, Profiling, and Runtime Introspection

- Source:
  [`docs/rfcs/0018-observability-tracing-profiling.md`](../rfcs/0018-observability-tracing-profiling.md)
- Current read: Observability schema and report surfaces exist.
- Evidence: `shardloom-core/src/observability.rs`, `shardloom-vortex/src/query_trace.rs`,
  `shardloom-vortex/src/runtime_utilization.rs`
- [x] Trace schema, profile/runtime report commands, and Vortex query trace evidence exist.
- [x] Observability tests cover current report contracts.
- [ ] Live profiling and distributed runtime introspection remain incomplete.

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
- Current read: Typed reports exist; real table/catalog integration is incomplete.
- Evidence: `shardloom-core/src/schema.rs`, `shardloom-core/src/table_intelligence.rs`,
  `shardloom-cli/tests/table_intelligence_plan_snapshots.rs`
- [x] Schema, partition, delete/tombstone, and aggregate table evidence reports exist.
- [x] Current table-intelligence surfaces are no-IO and typed.
- [ ] Catalog/table metadata integration, real table I/O, delete/tombstone execution, and CDC
  execution remain incomplete.

### RFC 0021 - Expression Engine and Kernel Registry

- Source:
  [`docs/rfcs/0021-expression-engine-kernel-registry.md`](../rfcs/0021-expression-engine-kernel-registry.md)
- Current read: Registry contracts and narrow kernels exist.
- Evidence: `shardloom-core/src/expression.rs`,
  `shardloom-core/src/physical_operator_kernel_contracts.rs`,
  `shardloom-cli/tests/kernel_registry_snapshots.rs`,
  `shardloom-vortex/src/encoded_count_physical_kernel.rs`
- [x] Native expression and kernel registry domain types, diagnostics, and admission reports exist.
- [x] Narrow physical kernels such as encoded count have evidence-backed slices.
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
- Current read: Encoded read boundary and local query primitives exist.
- Evidence: `shardloom-vortex/src/encoded_read_api.rs`,
  `shardloom-vortex/src/encoded_read_boundary.rs`,
  `shardloom-vortex/src/encoded_read_executor.rs`,
  `shardloom-vortex/src/encoded_path_selection.rs`,
  `shardloom-vortex/src/generalized_encoded_filter_execution.rs`
- [x] Vortex encoded-read boundary, local encoded count, path selection, and query primitive
  evidence exist.
- [x] Evidence records zero-decode, no-materialization, and no-fallback fields for scoped paths.
- [ ] Generalized direct encoded count/filter/project execution and production compressed-execution
  claims remain incomplete.

### RFC 0027 - CPU Vectorized Kernels, Streaming, and Runtime Adaptivity

- Source:
  [`docs/rfcs/0027-cpu-vectorized-kernels-streaming-runtime-adaptivity.md`](../rfcs/0027-cpu-vectorized-kernels-streaming-runtime-adaptivity.md)
- Current read: Report surfaces exist; real vectorized dispatch and runtime adaptivity are not broad.
- Evidence: `shardloom-core/src/cpu_specialization.rs`,
  `shardloom-cli/tests/cpu_specialization_snapshots.rs`,
  `shardloom-vortex/src/streaming_batch_runtime.rs`
- [x] CPU specialization, streaming, sizing, and runtime-promotion evidence surfaces exist.
- [x] Current reports keep production and fallback claims not claimable.
- [ ] Real SIMD/vectorized dispatch, host CPU probing, production vectorized kernel path, adaptive
  parallelism runtime, and broad streaming runtime remain incomplete.

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
- [ ] Object-store commit, table/catalog/lakehouse commit semantics, generalized sink commit,
  Foundry dataset transaction commit, upstream Vortex write API execution, and production
  output-payload fidelity remain incomplete.

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
  `python/src/shardloom/context.py`
- [x] CG-20 sufficiency reports, capability discovery, and selected local evidence surfaces exist.
- [x] Capability scopes are report-only unless workload-specific certification says otherwise.
- [ ] Broad SQL, DataFrame, UDF, notebook, universal adapter, unstructured/media, and best-default
  certification remain incomplete.

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
- [ ] Mature DataFrame methods, SQL execution, joins, aggregations, windows, data-quality APIs,
  object-store/table runtime, publication, production ETL certification, and comparison-only
  baseline/oracle views remain incomplete.

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
- [ ] Production `shardloom-foundry`, package publication, Foundry service invocation, Artifact
  Repository publication, Compute Module, virtual-table native execution, Foundry dataset
  transaction runtime, and F10 workload-certified deployment remain incomplete.

### RFC 0037 - Client, Wrapper, SDK, and Ecosystem Integration Surface

- Source:
  [`docs/rfcs/0037-client-wrapper-sdk-ecosystem-surface.md`](../rfcs/0037-client-wrapper-sdk-ecosystem-surface.md)
- Current read: Wrapper architecture and Python CLI wrapper exist; ecosystem clients are planned work.
- Evidence: `shardloom-core/src/wrapper_architecture.rs`,
  `shardloom-cli/tests/python_wrapper_snapshots.rs`, `python/src/shardloom/client.py`,
  `python/tests/test_cli_client.py`
- [x] One-protocol/many-thin-wrappers architecture and no-fallback wrapper reports exist.
- [x] Python wrapper reads CLI JSON rather than creating an alternate execution path.
- [ ] Generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP, Flight,
  ADBC, and BI connector implementations remain incomplete.

### RFC 0038 - Top-Level Plan and Execution Facade

- Source:
  [`docs/rfcs/0038-top-level-plan-execution-facade.md`](../rfcs/0038-top-level-plan-execution-facade.md)
- Current read: Top-level facade contracts exist; broad runtime facade is incomplete.
- Evidence: `shardloom-plan/src/execution_facade.rs`, `shardloom-vortex/src/top_level_facade.rs`,
  `shardloom-vortex/tests`
- [x] Vortex-native top-level provider dispatch and artifact-rich result surfaces exist.
- [x] Facade reports preserve explicit provider selection and no-fallback evidence.
- [ ] SQL/DataFrame runtime, object-store runtime, writes, and legacy facade compatibility remain
  incomplete; external engines remain baseline/oracle only.

### RFC 0039 - Typed Command/Result Envelope and CLI Modularity

- Source:
  [`docs/rfcs/0039-typed-command-result-envelope-cli-modularity.md`](../rfcs/0039-typed-command-result-envelope-cli-modularity.md)
- Current read: Typed envelope and modular CLI work are mostly implemented; migration tail remains.
- Evidence: `shardloom-cli/src/typed_envelope.rs`, `shardloom-cli/src/cli_output.rs`,
  `shardloom-cli/src/command_family.rs`, `python/src/shardloom/models.py`
- [x] Typed output v2, renderer, lifecycle taxonomy, command-family routing, and Python typed
  models exist.
- [x] Tests lock typed envelope compatibility for current command families.
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
- [ ] `direct_compatibility_transient` now has deterministic admission diagnostics across
  capability rows, CLI JSON envelopes, Python typed accessors, and benchmark coverage rows, plus
  one scoped local CSV selective-filter smoke path; broader formats, operators, result sinks, and
  SQL/DataFrame direct transient runtime remain incomplete.
- [ ] Prepared/native Vortex rows now carry a typed operator blocker matrix, but still rely on
  temporary materialized or residual ShardLoom-native operator paths for some scenarios until
  encoded/native operator coverage matures.
- [ ] REST parity must emit the same policy, mode-selection, evidence, claim-gate, and
  no-fallback fields as CLI/Python surfaces before it can be treated as an equivalent API.

## Follow-Up Rule

Before implementing any unchecked item from this review, use the corresponding `GAR-*` checklist
item in `docs/architecture/phased-execution-plan.md`. If the item is still too broad, split it in
the phase plan first. Keep the implementation slice narrow enough to verify with focused tests,
evidence snapshots, or release/readiness checks.
