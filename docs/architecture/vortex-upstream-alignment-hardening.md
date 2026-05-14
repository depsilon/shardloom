# Vortex Upstream Alignment and Native Provider Hardening

## Purpose

This document records how ShardLoom should stay aligned with upstream Vortex
while preserving ShardLoom's no-fallback execution identity.

Active implementation status and queue placement live in
`docs/architecture/phased-execution-plan.md`. This document is a contract and
reference surface only.

## Corrected framing

ShardLoom is standalone from external query-engine fallback, not isolated from
Vortex compute.

Canonical sentence:

> ShardLoom is a certifying, no-fallback execution/workflow layer over
> Vortex-native compressed computation. It may use upstream Vortex array,
> compute, scan, source, and sink APIs as native providers, but it never
> delegates unsupported ShardLoom execution to Spark, DataFusion, DuckDB,
> Polars, Velox, or Vortex query-engine integrations.

## Vortex-native execution provider

A Vortex-native execution provider is native execution implemented by upstream
Vortex APIs, Vortex array kernels, Vortex compute functions, Vortex
scan/source/sink APIs, or ShardLoom-owned Vortex-aware kernels.

It is not fallback execution when:

- it is invoked through `shardloom-vortex` or another approved native boundary
- the invoked API surface is recorded
- the Vortex version and feature gate are recorded
- representation transitions are recorded
- materialization/decode boundaries are explicit
- unsupported residual work is blocked or executed by ShardLoom-native code
- Spark, DataFusion, DuckDB, Polars, Velox, `vortex-datafusion`, and similar
  engines are not used as execution fallback
- `fallback_attempted=false`

## ExecutionProviderKind

Suggested values:

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

Every execution certificate should be able to report provider kind, provider
scope, provider crate, provider version, provider API surface, ShardLoom
admission policy, external-query-engine status, and fallback status.

## VortexComputeProviderReport

Required fields:

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

## ResidualBoundaryReport

If upstream Vortex accepts only part of a predicate, projection, or scan request:

- the accepted part may be certified as Vortex-native pushdown
- the residual part must be executed by ShardLoom-native code or rejected
- the residual part must not be delegated to DataFusion, DuckDB, Spark, Polars,
  Velox, or another query engine

`residual_executor` values:

- `none`
- `shardloom_native`
- `unsupported_blocked`
- `external_baseline_only`
- `prohibited_external_fallback`

## VortexCompatibilityMatrix

Required fields:

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

Rules:

- Vortex file-format compatibility and Vortex API compatibility are separate.
- Query-engine integrations are tracked as baseline/reference surfaces unless a
  later RFC approves a different role.
- Matrix rows are evidence, not support claims.

## VortexScanCompatibilityReport

Required fields:

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

The report aligns `NativeWorkStream` / `NativeResultStream` with Vortex
Source/Sink/Split concepts without treating external integrations as execution.

## VortexSourceSplitRuntimeAdmissionProof

GAR-0042A adds a report-only admission proof for one scoped Source/Split path. The proof must record:

- `schema_version`
- `proof_id`
- `path_id`
- selected-path status
- generalized-runtime admission status
- provider kind, crate, version, feature gate, API surface, and ShardLoom admission policy
- source and split surfaces
- split-ref, split-estimate, and split-serialization status
- field-mask and predicate-ordering status
- projection/filter/limit pushdown status
- residual executor for the selected path and generalized path
- correctness, benchmark, execution-certificate, Native I/O, predicate-ordering, and policy refs
- unsupported diagnostic code, blocker id, and required future evidence
- claim gate and claim boundary
- `runtime_execution=false`
- `object_store_io=false`
- `table_catalog_io=false`
- `write_io=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`

The current admitted evidence is fixture-scoped only. It does not authorize generalized
Source/Split runtime, object-store scan, table/catalog scan, writes, device/GPU execution,
managed-platform lanes, external engines, or fallback execution.

## VortexSegmentExtractionAdmissionReport

GAR-0003-A adds a report-only admission report for one concrete encoded layout family. The first
row covers `sparse_patch_fill` and records:

- `schema_version`
- `report_id`
- selected layout family and selected layout status
- upstream Vortex concepts checked
- ShardLoom admission surface and decision
- materialization and decode boundary status
- correctness, benchmark, execution-certificate, Native I/O, materialization/decode, and policy refs
- unsupported diagnostic code, blocker id, and required future evidence
- claim gate and claim boundary
- `runtime_execution=false`
- `data_read=false`
- `data_decoded=false`
- `data_materialized=false`
- `object_store_io=false`
- `table_catalog_io=false`
- `write_io=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`

The report does not implement sparse segment extraction. It keeps sparse patch/fill extraction
blocked until correctness fixtures, execution certificates, Native I/O certificates,
materialization/decode evidence, and no-fallback evidence exist.

## VortexLayoutDeviceManagedBoundaryMatrix

GAR-0042B adds a report-only boundary matrix for layout/write, device execution, object-store I/O,
and managed-platform comparison lanes. Every row must preserve:

- `claim_gate_status=not_claim_grade`
- explicit evidence requirements
- unsupported diagnostic code
- blocker id
- `runtime_execution=false`
- `write_io=false`
- `object_store_io=false`
- `device_execution=false`
- `managed_platform_execution=false`
- `external_engine_invoked=false`
- `fallback_attempted=false`

Managed-platform rows are comparison-only. They do not authorize credentials, dependencies, platform
execution, external engines, or ShardLoom-native claims.

## CompositePushdownCapabilityMatrix

Track combinations separately from primitive support.

Initial combinations:

- filter + projection
- filter + limit
- projection + limit
- filter + projection + limit
- ordered limit
- reverse scan
- top-N
- range predicate + projection
- zone-pruned filter + residual predicate
- filter-only columns discarded after mask

Acceptance:

- Capability reports distinguish primitive support from composite support.
- Unsupported combinations produce deterministic diagnostics.
- Composite support requires correctness, benchmark, execution-certificate, and
  Native I/O evidence.

## ExecuteStepEvidence

Deferred/iterative execution evidence should distinguish work that remained
deferred, fused, reduced, canonicalized, decoded, or materialized.

Required fields:

- `initial_representation`
- `deferred_operations`
- `executed_operations`
- `fused_operations`
- `reduce_steps`
- `canonicalization_steps`
- `materialization_steps`
- `execution_context_id`
- `trace_span_refs`
- `final_representation`
- `fallback_attempted=false`

## DeviceResidencyReport

Device/GPU support is report-only until runtime evidence exists.

Required fields:

- `device_kind`
- `device_buffer_refs`
- `host_to_device_bytes`
- `device_to_host_bytes`
- `direct_storage_candidate`
- `gpu_memory_pool`
- `kernel_registry`
- `fused_expression_candidate`
- `output_boundary`
- `fallback_attempted=false`

Rules:

- CPU remains the default execution target.
- GPU/cuDF/Arrow-device paths are optional and never fallback.
- Certificates must distinguish CPU-native, GPU-native, and host-materialized
  paths.

## ExtensionTypeCapabilityMatrix

Initial categories:

- vector
- tensor / matrix
- fixed-size binary
- map
- variant / JSON
- UUID
- geospatial WKB / future GeoArrow
- raster / image reference
- embedding reference
- document/media reference

Each row must distinguish dtype recognition, metadata preservation, scan
support, expression support, write support, and certified execution.

Vector similarity scan, ANN, top-k, indexing, GIS processing, and media
processing are separate capability rows.

## StreamingSinkCertificate

Required fields:

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

`writer_mode` values:

- `pull`
- `push`
- `streaming`

Streaming sink support cannot be claimed without flush, commit, and recovery
evidence.

## IoBackendEvidence

Required fields:

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

These fields feed object-store cost/performance claims but do not authorize
object-store IO.

## ExecutionTelemetryFacet

Certificates are proof artifacts. Traces, metrics, and profiles are supporting
telemetry.

Required fields:

- `trace_id`
- `span_refs`
- `operator_metric_refs`
- `io_metric_refs`
- `certificate_refs`
- `profile_refs`
- `perfetto_trace_ref`
- `fallback_attempted=false`

Missing telemetry may block performance or cost claims even when execution
correctness is certified.

## VortexIntegrationBoundaryReport

Categories:

- `upstream_vortex_native_api_allowed`
- `vortex_datafusion_baseline_only`
- `vortex_duckdb_baseline_only`
- `vortex_spark_baseline_only`
- `vortex_trino_baseline_only`
- `unsupported_as_runtime`
- `prohibited_fallback`

Required fields:

- `integration_name`
- `role`
- `allowed_in_core`
- `allowed_in_benchmark`
- `allowed_in_oracle`
- `may_execute_shardloom_plan`
- `may_execute_residual`
- `fallback_attempted=false`

## CompressionAdvisorReport

Required fields:

- `approximate_cardinality`
- `null_count`
- `run_count`
- `sortedness`
- `value_width`
- `string_length_distribution`
- `selected_encoding`
- `rejected_encodings`
- `estimated_size`
- `observed_size`
- `confidence`
- `fallback_attempted=false`

Approximate statistics can guide layout/encoding choices. They are not exact
correctness evidence.

## IntegrityAndEncryptionReport

Required fields:

- `checksum_present`
- `checksum_verified`
- `encryption_present`
- `encryption_supported`
- `key_policy_ref`
- `decrypted_boundary`
- `integrity_error_policy`
- `unsupported_encryption_diagnostic`
- `fallback_attempted=false`

Unsupported encrypted artifacts must fail or report deterministically; plaintext
or decrypted materialization must never be hidden.

## ForeignRuntimePosture

Future non-Python embedding surfaces:

- C FFI
- C++ wrapper
- Java/JVM
- WASM component
- Arrow C Stream / PyCapsule
- ADBC / Flight

Python/Conda remains the current distribution priority. Foreign runtimes must
preserve no-fallback and certificate semantics.

## PythonVortexInteropReport

Required fields:

- `shardloom_package_version`
- `vortex_data_package_version`
- `python_version`
- `import_side_effects`
- `conversion_boundaries`
- `materialization_boundaries`
- `optional_extras_detected`
- `fallback_attempted=false`

`vortex-data` is optional. PyVortex conversions are explicit source/sink,
test, or reference boundaries, not fallback execution.

## VortexBenchmarkInterop

Required fields:

- `scenario_name`
- `input_format`
- `engine`
- `file_format`
- `startup_policy`
- `conversion_policy`
- `result_policy`
- `correctness_oracle`
- `shardloom_native_execution`
- `vortex_integration_execution`
- `fallback_attempted=false`

Rules:

- Upstream Vortex benchmark scenarios can inform ShardLoom benchmark inputs.
- Upstream Vortex benchmark results are not ShardLoom performance claims.
- ShardLoom-native rows must be distinct from Vortex+DataFusion,
  Vortex+DuckDB, or other integration rows.

## Acceptance criteria

- Docs and RFCs distinguish Vortex-native providers from external query-engine
  fallback.
- Vortex integrations remain baseline/reference/oracle surfaces unless a later
  RFC explicitly changes their role.
- No runtime behavior, dependency, GPU claim, vector/geospatial/media claim,
  object-store claim, write claim, benchmark claim, or fallback execution is
  authorized by this document.
