# Operational Evidence, Policy, Workload, and Protocol Hardening

## Purpose

This document defines cross-surface contracts that keep CG-20, CG-21, CG-22,
and CG-23 coherent as implementation fans out across CLI, Python, future REST,
benchmarks, agents, and governance surfaces.

Active implementation status and queue placement live in
`docs/architecture/phased-execution-plan.md`. This document is a contract
reference only.

## Scope

These contracts are docs/report-only until a phase-plan item promotes them into
implementation.

They do not authorize:

- runtime behavior
- SQL/DataFrame execution
- adapter execution
- HTTP server behavior
- package publication
- object-store IO
- benchmark execution
- external engine invocation
- fallback execution

## EvidenceArtifactEnvelope

Every certificate, benchmark row, workload scorecard, profile, lineage event,
and future REST result should be able to share a common artifact identity.

Required fields:

- `artifact_id`
- `artifact_type`
- `schema_version`
- `producer_component`
- `engine_version`
- `protocol_version`
- `created_at`
- `workload_constitution_ref`
- `plan_id`
- `query_id`
- `run_id`
- `input_refs`
- `output_refs`
- `evidence_refs`
- `policy_refs`
- `redaction_policy`
- `retention_policy`
- `invalidation_refs`
- `digest`
- `diagnostics`
- `fallback_attempted=false`

Rules:

- Artifact identity must be stable enough for CLI, Python, future REST, and
  agent surfaces to reference the same evidence.
- Artifact digests are evidence integrity metadata; they do not by themselves
  prove correctness or performance.
- Artifact retention and redaction must be explicit before evidence is exported
  to logs, notebooks, REST, lineage, or agent surfaces.

## EvidenceArtifactSafety

Evidence artifacts can leak paths, schema names, query text, samples, or
credentials if treated as harmless logs.

Required fields:

- `data_classification`
- `contains_user_values`
- `contains_paths`
- `contains_credentials=false`
- `contains_samples`
- `contains_query_text`
- `contains_schema_names`
- `redaction_policy`
- `retention_policy`
- `export_allowed`
- `agent_visible`

Certification blockers:

- credentials or secrets appear in unredacted artifact fields
- governed workloads lack artifact retention policy
- notebook/API/agent previews expose values without an explicit preview boundary
- artifact export is allowed without data classification

## ShardLoomExecutionPolicy

Execution policy must be the same concept across CLI, Python, future REST, and
agent surfaces.

Required fields:

- `requested_engine`
- `allowed_engines`
- `fallback_policy`
- `materialization_policy`
- `decode_policy`
- `result_delivery_policy`
- `evidence_policy`
- `effect_policy`
- `credential_policy`
- `redaction_policy`
- `retention_policy`
- `memory_policy`
- `spill_policy`
- `network_policy`
- `destructive_operation_policy`
- `benchmark_policy`
- `agent_policy`

Rules:

- Default fallback policy is deny.
- Default discovery/explain/estimate policy is side-effect-free.
- Decode and materialization policies must be separate.
- External effects, remote reads/writes, credentials, destructive operations,
  and benchmark baselines require explicit opt-in policy.

## QueryLifecycleContract

Local execution, future REST queries, live subscriptions, hybrid materialized
views, and agent-triggered runs need one lifecycle vocabulary.

States:

- `accepted`
- `validating`
- `planned`
- `blocked`
- `queued`
- `running`
- `cancelling`
- `cancelled`
- `failed`
- `succeeded`
- `expired`

Required fields:

- `query_id`
- `idempotency_key`
- `plan_id`
- `selected_engine`
- `policy_ref`
- `cancellation_mode`
- `retry_policy`
- `result_retention`
- `certificate_retention`
- `cleanup_status`
- `side_effect_status`
- `ambiguous_commit_status`
- `fallback_attempted=false`

Rules:

- `blocked` is the correct state for unsupported or uncertified work.
- `failed` is reserved for attempted execution that did not succeed.
- `ambiguous_commit_status` must be explicit before any side-effecting sink can
  be certified.

## ProtocolSurfaceParityReport

ShardLoom should expose the same facts through CLI JSON, Python, future REST,
future MCP, and future data-plane metadata.

Surfaces:

- `cli_json`
- `python_wrapper`
- `rest_openapi`
- `mcp_resources`
- `flight_adbc_metadata`

Required fields:

- `report_id`
- `schema_version`
- `field_parity`
- `unsupported_field_mappings`
- `diagnostic_parity`
- `certificate_ref_parity`
- `result_policy_parity`
- `fallback_field_parity`
- `known_surface_gaps`
- `diagnostics`
- `fallback_attempted=false`

Acceptance:

- Every capability, certificate, error, result, and policy field exposed in CLI
  JSON has a Python mapping, future REST mapping, or explicit unavailable
  reason.
- Support status cannot differ silently across surfaces.
- Unsupported diagnostics carry the same blocker, required gate,
  materialization, rewrite, and no-fallback fields.

## Workload Constitution Catalog

`WorkloadConstitution` should become concrete before claims broaden.

Starter catalog entries:

- `local_vortex_primitives`
- `local_file_etl`
- `conda_import_smoke`
- `python_dataframe_local_etl`
- `rest_discovery_only`
- `batch_vortex_analytics`
- `hybrid_base_delta_fixture`
- `adapter_vortex_read_write_local`
- `traditional_analytics_benchmark`

Each entry should declare:

- required sources
- required sinks
- required operators
- required functions
- semantic profile
- allowed engine modes
- allowed materialization boundaries
- required certificates
- required correctness fixtures
- required benchmark scenarios
- required governance policy
- claim level
- disallowed effects

## ShardLoomNative semantic profile floor

Before serious SQL/DataFrame semantics, `ShardLoomNative` needs a concrete
dimension table.

Initial dimensions:

- null comparison and three-valued logic
- null sort ordering
- NaN equality and ordering
- signed zero
- integer overflow
- decimal precision and scale
- timestamp unit and timezone behavior
- date parsing
- string collation
- case sensitivity
- binary equality
- empty aggregate behavior
- count-null behavior
- join-null semantics
- window frame defaults
- duplicate column behavior
- nested/list equality
- schema field identity

## StandardsDependencyDecision

External standards and tools may be references, schemas, optional features, or
dependencies. The status must be explicit.

Required fields:

- `name`
- `category`
- `current_status`
- `license`
- `dependency_type`
- `runtime_required`
- `default_enabled`
- `conda_available`
- `security_review_status`
- `fallback_risk`
- `approved_by_rfc`

`current_status` values:

- `reference_only`
- `schema_only`
- `optional_feature`
- `dependency_approved`
- `rejected`

## BenchmarkConstitution

Benchmark fairness should be workload-scoped and explicit.

Required fields:

- `workload_constitution_ref`
- `engine_mode`
- `input_format`
- `native_vortex_or_compatibility_import`
- `startup_included`
- `conversion_included`
- `result_delivery_included`
- `cache_policy`
- `object_store_policy`
- `warmup_policy`
- `iterations`
- `correctness_oracle`
- `result_materialization_policy`
- `api_transport_policy`
- `resource_limits`
- `claim_level`

Rules:

- Native Vortex replay, Spark cold Parquet reads, REST serialization, and
  compatibility import must not be mixed without explicit accounting.
- External benchmark engines remain optional comparison baselines only.
- A benchmark constitution does not authorize runtime fallback.

## RustPerformanceProfileEvidence

High-performance claims need reproducible Rust/compiler context.

Required fields:

- `rustc_version`
- `target_triple`
- `target_cpu`
- `opt_level`
- `lto_mode`
- `codegen_units`
- `panic_strategy`
- `allocator`
- `simd_feature_flags`
- `pgo_status`
- `bolt_status`
- `binary_size`
- `benchmark_refs`
- `correctness_refs`

Acceptance:

- Missing compiler/profile evidence blocks release-grade performance claims.
- CPU/GPU/SIMD claims must cite matching correctness and benchmark refs.

## Acceptance criteria

- These contracts are referenced by the relevant RFCs and phase plan.
- No implementation is implied by this document.
- No dependency, runtime, server, benchmark, adapter, or package publication is
  authorized by this document.
- Every contract keeps `fallback_attempted=false` or an explicit unsupported
  diagnostic.
