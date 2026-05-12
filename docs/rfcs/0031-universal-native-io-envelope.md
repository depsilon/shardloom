# RFC 0031: Universal Native I/O Envelope

## Summary
This RFC defines a ShardLoom-native universal I/O contract for planning and execution boundaries. Universal I/O does not mean universal decoded Arrow batches. Universal I/O means ShardLoom-native work envelopes preserving physical information, encoded representation, statistics, selection vectors, materialization state, pushdown proof, and sink requirements. No implementation is added in this PR.

## Motivation
ShardLoom needs a portable, deterministic contract for source-to-sink data flow that preserves encoded semantics and avoids hidden decode/materialization pressure.

## Goals
- Define canonical envelope contracts for native work and results.
- Preserve encoded representation and materialization state across boundaries.
- Keep Vortex as the highest-fidelity persistence target.
- Allow foreign encoded representations to be preserved when possible.
- Keep diagnostics deterministic and machine-readable.

## Non-goals
- No runtime implementation in this PR.
- No new dependencies.
- No fallback/delegation execution.

## Core concept
Universal I/O is a native contract layer, not a decoded-batch normalization step.

ShardLoom is standalone from external query-engine fallback, not isolated from
Vortex compute. Upstream Vortex array, compute, scan, source, and sink APIs may
serve as Vortex-native execution providers when they are admitted through
ShardLoom policy, version-recorded, feature-gated, and reported through
ShardLoom certificates. Vortex query-engine integrations that delegate residual
work to DataFusion, DuckDB, Spark, Polars, Velox, or similar engines remain
external baselines or interoperability references only.

## Vortex Scan API alignment
Vortex's Scan API direction is an important design reference for CG-19, especially its source/sink boundary, split-based work units, filter/projection pushdown, compressed data flow, and independently executable split model.

ShardLoom should preserve those lessons without coupling the internal execution model to an unstable upstream API surface. A future Vortex source/sink adapter may implement or bridge Scan API concepts, but it must still emit ShardLoom-native envelopes, capability reports, pushdown proofs, materialization boundaries, and per-source/sink-path native I/O certificates.

CG-19 adapters should also preserve range-read, coalescing, prefetch, backpressure, and segment-cache evidence where the source can provide it. These are scheduling and I/O-planning signals, not permission to delegate query execution to an external engine.

Vortex's own operator/vector, GPU, Arrow device array, cuDF, and Arrow-oriented integration work should remain a compatibility and implementation-reference signal for ShardLoom unless a later RFC certifies a native boundary. CG-19 must not treat fully decoded Arrow vectors, GPU output arrays, or external engine integrations as the implicit universal path.

## Contract vocabulary
- `NativeWorkEnvelope`
- `NativeWorkStream`
- `NativeResultStream`
- `RepresentationState`
- `SourceCapabilityReport`
- `SourcePushdownReport`
- `SinkRequirementReport`
- `AdapterFidelityReport`
- `MaterializationBoundaryReport`
- `NativeIoCertificate`
- `VortexCompatibilityMatrix`
- `VortexScanCompatibilityReport`
- `VortexComputeProviderReport`
- `ResidualBoundaryReport`
- `VortexIntegrationBoundaryReport`
- `IoBackendEvidence`
- `StreamingSinkCertificate`
- `ExecutionTelemetryFacet`
- `IntegrityAndEncryptionReport`

## Field-level contract sketches

### NativeWorkEnvelope
Required fields:
- `envelope_id`: stable unique identifier.
- `source_ref`: source identity and scope.
- `schema_ref`: schema identity or digest.
- `representation_state`: current `RepresentationState`.
- `statistics_ref`: statistics handle used for pruning/proofs.
- `selection_vector_ref`: optional encoded selection handle.
- `pushdown_proof_ref`: proof object or diagnostic reference.
- `materialization_boundary_ref`: optional last/next boundary reference.
- `ordering`: declared ordering contract.
- `partitioning`: declared partitioning contract.
- `semantic_profile`: applicable semantic compatibility profile.
- `diagnostics`: stable machine-readable diagnostics.
- `fallback_attempted=false`: explicit no-fallback invariant.

### NativeWorkStream
Required fields:
- `stream_id`: stable stream identifier.
- `source_capability_report`: embedded or referenced `SourceCapabilityReport`.
- `envelopes`: ordered set of `NativeWorkEnvelope` units.
- `backpressure_policy`: bounded-memory/backpressure contract.
- `streaming_mode`: batch, micro-batch, or record-stream mode declaration.
- `task_granularity_policy`: partition/chunk/task sizing policy.
- `diagnostics`: stream-level diagnostics.

### NativeResultStream
Required fields:
- `stream_id`: stable result stream identifier.
- `sink_requirement_report`: sink constraints and required representation.
- `result_envelopes`: output `NativeWorkEnvelope`-compatible payload units.
- `materialization_boundary_reports`: one or more `MaterializationBoundaryReport` entries.
- `native_io_certificates`: one `NativeIoCertificate` per source/sink path represented in the result stream.
- `native_io_certificate_summary`: optional aggregate certificate summary for the full run/report scope.
- `diagnostics`: stable output diagnostics.

## RepresentationState
`RepresentationState` values:
- `metadata_only`
- `pruned`
- `vortex_encoded`
- `foreign_encoded`
- `selection_vector_encoded`
- `partially_decoded`
- `decoded_columnar`
- `materialized_rows`
- `external_effect`
- `unsupported`

### RepresentationState semantics and transitions

#### metadata_only
- Meaning: answerability/proof boundary from metadata only.
- Allowed transitions: `pruned`, `vortex_encoded`, `foreign_encoded`, `unsupported`.
- Forbidden assumptions: no data bytes imply no semantic loss.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.
- Clarification: `metadata_only` is a proof/answerability boundary, not an execution terminal state.
- If metadata is sufficient, planner flow may transition to `pruned` or return a metadata-only result.
- If metadata is insufficient but the source remains supported, planning may continue to `vortex_encoded` or `foreign_encoded`.
- `unsupported` is reserved for capability-proof failure, not merely for metadata insufficiency.

#### pruned
- Meaning: work eliminated by statistics/metadata proof.
- Allowed transitions: terminal, or `unsupported` if proof invalidated.
- Forbidden assumptions: do not assume row ordering or emitted rows.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### vortex_encoded
- Meaning: Vortex-native encoded representation preserved.
- Allowed transitions: `selection_vector_encoded`, `partially_decoded`, `unsupported`.
- Forbidden assumptions: do not assume full decode is required.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### foreign_encoded
- Meaning: non-Vortex encoded representation preserved through adapter boundary.
- Allowed transitions: `partially_decoded`, `selection_vector_encoded`, `unsupported`.
- Forbidden assumptions: do not assume compatibility with all native kernels.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### selection_vector_encoded
- Meaning: encoded payload with active selection/projection vector state.
- Allowed transitions: `partially_decoded`, `decoded_columnar`, `unsupported`.
- Forbidden assumptions: selection is not equivalent to materialized filtering.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: yes.

#### partially_decoded
- Meaning: partial decode/materialization for required columns or operators.
- Allowed transitions: `decoded_columnar`, `materialized_rows`, `unsupported`.
- Forbidden assumptions: partially decoded data is not fully normalized.
- Implies decode: yes (partial).
- Implies row materialization: no.
- Can remain encoded: partially.

#### decoded_columnar
- Meaning: decoded columnar representation.
- Allowed transitions: `materialized_rows`, `unsupported`.
- Forbidden assumptions: decoded columnar is not row materialization.
- Implies decode: yes.
- Implies row materialization: no.
- Can remain encoded: no.

#### materialized_rows
- Meaning: row-oriented materialization boundary crossed.
- Allowed transitions: terminal or `unsupported`.
- Forbidden assumptions: row materialization must not be implicit/default.
- Implies decode: yes.
- Implies row materialization: yes.
- Can remain encoded: no.

#### external_effect
- Meaning: boundary involving external side effects or effectful adapters.
- Allowed transitions: any supported state based on contract.
- Forbidden assumptions: effect completion does not imply commit success.
- Implies decode: not necessarily.
- Implies row materialization: not necessarily.
- Can remain encoded: possibly.

#### unsupported
- Meaning: capability proof failed or required contract unsupported.
- Allowed transitions: terminal.
- Forbidden assumptions: no fallback/delegation allowed.
- Implies decode: no.
- Implies row materialization: no.
- Can remain encoded: N/A.

### Explicit transition examples
- `metadata_only -> pruned`
- `metadata_only -> vortex_encoded`
- `metadata_only -> foreign_encoded`
- `vortex_encoded -> selection_vector_encoded`
- `foreign_encoded -> partially_decoded`
- `partially_decoded -> decoded_columnar`
- `decoded_columnar -> materialized_rows`
- `any state -> unsupported` when capability proof fails

## Source capability and pushdown

### SourceCapabilityReport
Required fields:
- `source_kind`
- `adapter_id`
- `adapter_maturity_level`
- `schema_discovery_status`
- `metadata_discovery_status`
- `statistics_availability`
- `pushdown_capabilities`
- `encoded_representation_preserved`
- `range_read_capability`
- `streaming_capability`
- `object_store_capability`
- `read_supported`
- `write_supported`
- `commit_supported`
- `diagnostics`
- `fallback_attempted=false`

### SourcePushdownReport
Required fields:
- `accepted_operations`
- `rejected_operations`
- `guarantee`
- `proof_basis`
- `residual_expression`
- `conservative_false_positive_policy`
- `unsafe_rejected_reason`
- `semantic_profile`
- `residual_required`
- `residual_executor`
- `fallback_attempted=false`

Pushdown boundaries:
- `accepted_operations` must name only operations the source can apply with the declared `guarantee`.
- `residual_expression` must be present whenever `guarantee` is not fully exact for the whole predicate/projection.
- Conservative pushdown may include false positives but must not exclude valid rows.
- Unsafe source behavior must be rejected instead of delegated or retried through another execution engine.
- If a source or Vortex-native provider accepts only part of a predicate/projection, the residual must be executed by ShardLoom-native code or blocked with diagnostics.
- `residual_executor` values are `none`, `shardloom_native`, `unsupported_blocked`, `external_baseline_only`, and `prohibited_external_fallback`.
- Pushdown proof is source capability evidence; it is not fallback execution.

## Vortex-native provider and upstream alignment

CG-19 must distinguish a Vortex-native execution provider from external
query-engine fallback.

`ExecutionProviderKind` values:
- `shardloom_kernel`
- `shardloom_metadata`
- `vortex_array_kernel`
- `vortex_compute_function`
- `vortex_scan`
- `vortex_source`
- `vortex_sink`
- `compatibility_import`
- `compatibility_export`
- `external_baseline`
- `prohibited_external_fallback`

`VortexComputeProviderReport` fields:
- `provider_kind`
- `vortex_version`
- `feature_gate`
- `provider_api_surface`
- `operation`
- `dtype_support`
- `encoding_support`
- `layout_support`
- `null_semantics`
- `selection_vector_behavior`
- `materialization_behavior`
- `decoded_reference_status`
- `residual_required`
- `residual_executor`
- `external_engine_invoked=false`
- `fallback_attempted=false`
- `diagnostics`

Vortex-native provider rules:
- Upstream Vortex array, compute, scan, source, and sink APIs may be native
  providers when invoked through approved ShardLoom boundaries.
- Provider execution must record Vortex version, feature gate, API surface,
  representation transitions, materialization boundaries, residual executor,
  and fallback status.
- Vortex DataFusion, DuckDB, Spark, Trino, or similar integrations must not be
  used to execute unsupported ShardLoom residual work.
- External integrations may be benchmark, oracle, migration, or interoperability
  references only.

## Vortex compatibility and scan alignment

`VortexCompatibilityMatrix` fields:
- `vortex_crate_version`
- `vortex_file_format_assumption`
- `rust_toolchain_compatibility`
- `crate_feature_set_enabled`
- `local_file_read_support`
- `local_file_write_support`
- `scan_api_status`
- `source_sink_api_status`
- `split_serialization_status`
- `dtype_mapping_status`
- `layout_mapping_status`
- `statistics_mapping_status`
- `dictionary_rle_constant_sparse_status`
- `nested_list_status`
- `arrow_boundary_status`
- `python_pyvortex_compatibility`
- `object_store_status`
- `gpu_device_status`
- `extension_dtype_status`
- `known_unsupported_vortex_apis`
- `external_integration_status`

`VortexScanCompatibilityReport` fields:
- `scan_request_fields`
- `projection_status`
- `filter_status`
- `limit_status`
- `field_mask_status`
- `split_estimates`
- `split_serialization_status`
- `sink_requirement_mapping`
- `pushdown_decision`
- `residual_expression`
- `residual_executor`
- `native_io_certificate_refs`
- `fallback_attempted=false`

`VortexIntegrationBoundaryReport` fields:
- `integration_name`
- `role`
- `allowed_in_core`
- `allowed_in_benchmark`
- `allowed_in_oracle`
- `may_execute_shardloom_plan`
- `may_execute_residual`
- `fallback_attempted=false`

Integration roles include `upstream_vortex_native_api_allowed`,
`vortex_datafusion_baseline_only`, `vortex_duckdb_baseline_only`,
`vortex_spark_baseline_only`, `vortex_trino_baseline_only`,
`unsupported_as_runtime`, and `prohibited_fallback`.

## IO, sink, telemetry, and integrity evidence

`IoBackendEvidence` fields:
- `backend_kind`
- `read_at_count`
- `object_request_count`
- `coalesced_request_count`
- `requested_bytes`
- `returned_bytes`
- `useful_bytes`
- `read_amplification_ratio`
- `prefetch_registered`
- `prefetch_resolved`
- `prefetch_dropped`
- `segment_cache_hits`
- `segment_cache_misses`
- `backend_concurrency`
- `coalescing_policy`
- `sub_segment_read_supported`
- `fallback_attempted=false`

`StreamingSinkCertificate` fields:
- `writer_mode`
- `flush_policy`
- `buffered_rows`
- `buffered_bytes`
- `emitted_micro_segments`
- `compression_strategy`
- `backpressure_state`
- `sink_commit_status`
- `recovery_status`
- `output_manifest_ref`
- `fallback_attempted=false`

`ExecutionTelemetryFacet` fields:
- `trace_id`
- `span_refs`
- `operator_metric_refs`
- `io_metric_refs`
- `certificate_refs`
- `profile_refs`
- `perfetto_trace_ref`
- `fallback_attempted=false`

`IntegrityAndEncryptionReport` fields:
- `checksum_present`
- `checksum_verified`
- `encryption_present`
- `encryption_supported`
- `key_policy_ref`
- `decrypted_boundary`
- `integrity_error_policy`
- `unsupported_encryption_diagnostic`
- `fallback_attempted=false`

## Sink requirements

### SinkRequirementReport
Required fields:
- `target_format`
- `accepts_encoded`
- `requires_decoded_columnar`
- `requires_rows`
- `preserves_metadata`
- `requires_ordering`
- `requires_partitioning`
- `requires_commit`
- `supports_streaming`
- `max_chunk_size`
- `backpressure_policy`
- `commit_requirement`
- `side_effect_boundary`
- `metadata_loss_policy`
- `fidelity_loss_policy`

Sink boundaries:
- Sinks that require decoded columnar data or materialized rows must force an explicit `MaterializationBoundaryReport`.
- Compatibility sinks must report metadata/fidelity loss instead of silently dropping physical information.
- Commit-capable sinks must declare idempotency, recovery, cleanup, and visibility semantics before certification.
- Vortex sinks remain the highest-fidelity native path when their requirements can preserve encoded representation.

## Adapter fidelity

### AdapterFidelityReport
Required fields:
- `adapter_id`
- `source_kind`
- `sink_kind`
- `metadata_preserved`
- `statistics_preserved`
- `encoded_representation_preserved`
- `materialization_required`
- `fidelity_loss`
- `metadata_loss`
- `statistics_loss`
- `ordering_loss`
- `partitioning_loss`
- `semantic_loss_risk`
- `commit_semantic_loss`
- `fallback_attempted=false`

Fidelity boundaries:
- Fidelity loss must distinguish metadata loss from representation loss and semantic-risk loss.
- Foreign encoded preservation should be reported separately from Vortex-native encoded preservation.
- An adapter that reads or writes data but drops statistics, ordering, partitioning, field identity, or layout hints must report that loss.
- Adapter fidelity reports feed `NativeIoCertificate` evidence and cannot be replaced by a run-level summary.

## Materialization boundary

### MaterializationBoundaryReport
Required fields:
- `boundary_id`
- `from_state`
- `to_state`
- `required_by`
- `reason`
- `bytes_decoded`
- `rows_materialized`
- `fidelity_loss`
- `fallback_attempted=false`

## Native I/O certificate

### NativeIoCertificate
Required fields:
- `certificate_id`
- `path_id`
- `certificate_scope`
- `workload_constitution_ref`
- `source_path_ref`
- `sink_path_ref`
- `source_capability_report`
- `source_pushdown_report`
- `representation_transitions`
- `sink_requirement_report`
- `adapter_fidelity_report`
- `materialization_boundaries`
- `side_effects`
- `evidence_refs`
- `known_limits`
- `certificate_decision`
- `diagnostics`
- `fallback_attempted=false`

Certificate-to-adapter alignment:
- Each adapter source/sink path must reference the matching source capability, source pushdown, sink requirement, and adapter fidelity evidence.
- Multi-source and multi-sink flows require one certificate per path, plus an optional run-level summary.
- Adapter certification cannot claim read, write, pushdown, streaming, object-store-range, commit, or benchmark maturity without matching certificate evidence.
- Certificate evidence must remain explicit even when the source or sink is local and side-effect-free.
- `certificate_decision` must be one of `not_certified`, `partial_for_path`, `certified_for_path`, or `blocked`.
- `known_limits` must include unsupported pushdown, representation loss, metadata loss, materialization, commit/recovery gaps, object-store limitations, and semantic risks when present.

## Acceptance criteria
- CG-19 cannot complete until every source/sink path emits a `NativeIoCertificate`.
- A run-level certificate summary cannot replace per-path certificates.
- Universal I/O must preserve `vortex_encoded` or `foreign_encoded` state whenever possible.
- Universal I/O must not silently normalize to decoded Arrow.
- All transitions to `decoded_columnar` or `materialized_rows` must include a `MaterializationBoundaryReport`.

## CG-19 sufficiency gates

CG-19 is a prerequisite evidence surface for CG-20 best-default certification. It must prove that broad adapters and sinks do not erase the native execution contract.

CG-19 cannot be marked sufficient for a workload unless:

- Every required source/sink path has a certificate with source capability, pushdown, sink requirement, adapter fidelity, materialization boundary, side-effect, diagnostics, and no-fallback evidence.
- Multi-source and multi-sink plans preserve per-path evidence instead of collapsing the run into a single summary.
- Foreign encoded representations are preserved or explicitly reported as partially decoded, decoded columnar, or materialized rows with reason and cost fields.
- Source pushdown is exact, exact-with-residual, conservative, unsupported, or unsafe-rejected with proof; hidden remote execution is not accepted.
- Sinks that require decoded columnar batches, rows, ordering, partitioning, or commit behavior declare those requirements before planning is certified.
- Metadata, statistics, ordering, partitioning, field identity, and layout-hint loss are reported before any adapter path can count as certified.
- Object-store/range-read, streaming/backpressure, retry/idempotency, commit/recovery, and cleanup semantics are declared where the workload requires them.
- Any unsupported path fails explicitly with deterministic diagnostics and `fallback_attempted=false`.

Disqualifiers:

- A decoded Arrow normalization step is used as the implicit universal path.
- A transition to `decoded_columnar` or `materialized_rows` lacks a materialization boundary.
- A source or adapter executes unsupported residual ShardLoom logic as fallback.
- Adapter certification refers only to run-level summaries and omits per-path certificates.
- A certificate omits known fidelity, metadata, representation, commit, or semantic losses.

## Relationship to RFC 0013
This RFC complements RFC 0013 by formalizing I/O envelope contracts that support streaming and zero-decode priorities.

## Relationship to CG-19
This RFC defines the contract foundation for CG-19 (Universal Native I/O Envelope).

## No-fallback and no-delegation policy
Universal I/O contracts must never imply execution fallback. Unsupported paths fail explicitly with deterministic diagnostics.

Using a policy-admitted upstream Vortex array, compute, scan, source, or sink
API as a Vortex-native execution provider is allowed when evidence is recorded.
Using a Vortex query-engine integration, DataFusion, DuckDB, Spark, Polars,
Velox, or another external engine to execute unsupported residual work is
fallback/delegation and remains prohibited.

## Future implementation phases
Future phases may implement these contracts incrementally in planner diagnostics, explain/estimate outputs, adapter interfaces, and execution certificates.
