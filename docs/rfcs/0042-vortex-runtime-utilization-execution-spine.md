# RFC 0042: Vortex Runtime Utilization and Execution Spine

## Purpose

Make ShardLoom's Vortex alignment operational: ShardLoom should be able to report whether it uses
Vortex as a compressed-array execution substrate, not merely as a file-format fixture.

ShardLoom should use or wrap upstream Vortex concepts first when they fit the no-fallback policy:

```text
arrays and encodings
deferred execution
execution-step layers
Scan API Source/Sink/Split work units
field masks and predicate ordering
layout strategy
I/O coalescing and prefetch
sessions and registries
device residency
extension DTypes
benchmark discipline
```

## Status

Accepted as Vortex-first runtime utilization and execution-spine guidance.

This RFC does not authorize new upstream Vortex API calls, dependency expansion, object-store
execution, writes, GPU/device execution, managed-platform benchmark lanes, external engine
invocation, or fallback execution.

## Vortex Capability Utilization

Add a report that answers how much of Vortex ShardLoom actually uses:

```text
VortexCapabilityUtilizationReport
- vortex_crate_version
- file_format_version_assumption
- arrays_used
- layouts_used
- scan_api_used
- source_sink_used
- split_execution_used
- expression_pushdown_used
- field_masks_used
- zone_pruning_used
- dynamic_predicate_reordering_used
- deferred_execution_used
- execute_parent_kernels_used
- native_provider_kind
- materialization_boundary
- decode_boundary
- arrow_boundary
- object_store_io
- gpu_device_status
- fallback_attempted=false
```

Each row must classify the Vortex area as:

```text
not_used
report_only_wrapped
partial_runtime_evidence
planned_runtime_provider
blocked_until_evidence
baseline_only
```

## Array Execution Certificate

Vortex arrays are not just decoded containers. They can represent deferred work and layered
execution. ShardLoom certificates should therefore distinguish:

```text
array_tree_before
deferred_operations
reduce_steps
reduce_parent_steps
execute_parent_kernel_steps
execute_steps
canonicalization_steps
materialization_steps
array_tree_after
final_representation
```

Support claims require trace-backed evidence. Until then, `VortexArrayExecutionCertificate` is a
report-only blocker.

## Scan Execution Spine

Vortex Scan API should be the preferred model for source-backed Vortex work units when the upstream
API is admitted:

```text
VortexScanPlanNode
VortexSourceRef
VortexSplitTask
VortexScanRequest
VortexScanExecutionSpineReport
```

The spine must record:

```text
Source/Sink usage
split refs and estimates
split serialization
compressed IPC transport status
projection/filter/limit pushdown
residual executor
Native I/O certificate requirement
external_engine_invoked=false
fallback_attempted=false
```

Residual work must be ShardLoom-native or deterministically blocked. Vortex query-engine
integrations, DataFusion, DuckDB, Spark, Polars, Velox, Trino, Dask, Ray, Snowflake, Databricks,
BigQuery, and similar engines may not execute residual work as fallback.

## Field Masks And Predicate Ordering

ShardLoom should represent Scan-specific optimization evidence explicitly:

```text
VortexFieldMaskEvidence
- filter_columns
- output_columns
- union_read_columns
- filter_only_columns_discarded

VortexPredicateOrderingEvidence
- conjuncts
- observed_selectivity
- dynamic_reorder_decisions
- row_or_segment_reduction
```

No support claim is allowed until real scan-request evidence populates these fields.

## Layout Advisor

Vortex layouts are out-of-memory execution/layout objects, not merely file-format metadata.

ShardLoom should add a Vortex-native layout advisory posture:

```text
VortexLayoutAdvisorReport
- target workload
- candidate layouts
- chunking policy
- zone/statistics policy
- dictionary strategy
- expected pruning benefit
- expected random-access benefit
- write/read tradeoff
- object-store request shape
- device-read friendliness
- compaction recommendation
```

The advisor is report-only until workload constitutions, layout refs, write/read metrics, layout
health evidence, and Native I/O evidence exist.

## Object-Store I/O Evidence

Vortex I/O concepts should feed ShardLoom Native I/O evidence:

```text
read_at_count
object_request_count
coalesced_request_count
requested_bytes
returned_bytes
useful_bytes
read_amplification_ratio
prefetch_registered
prefetch_resolved
prefetch_dropped
backend_concurrency
coalescing_policy
```

Runtime object-store reads remain blocked until a later object-store runtime phase.

## Device Residency

Device/GPU paths remain report-only but first-class:

```text
DeviceResidencyReport
- device_kind
- host_to_device_bytes
- device_to_host_bytes
- device_buffer_refs
- direct_storage_candidate
- kernel_registry
- fused_expression_candidate
- output_boundary
```

No GPU/device claim is allowed without device residency evidence and explicit CPU fallback status.

## Session And Registry Model

ShardLoom should adopt the design lesson from explicit Vortex sessions and registries without
copying the implementation prematurely:

```text
ShardLoomSessionModelReport
- operator registry
- function registry
- aggregate registry
- sketch registry
- source/sink adapter registry
- execution provider registry
- semantic profile registry
- evidence artifact registry
- policy/effect registry
```

Registries must be explicit session context, not hidden global mutable state. Runtime mutation stays
blocked until admission policy exists.

## Benchmark Discipline

ShardLoom benchmark work should distinguish:

```text
microbenchmarks for kernels/providers/encodings
suite benchmarks for end-to-end ETL/query workflows
```

Every benchmark row must declare setup/timing/materialization/result-delivery/cache policy through
the benchmark constitution. Vortex integrations may be benchmark baselines only:

```text
vortex_integration_baseline_only
not_shardloom_execution
not_fallback
```

## Acceptance

```text
VortexCapabilityUtilizationReport exists and distinguishes used, wrapped, planned, blocked, and
baseline-only Vortex capabilities.
VortexScanExecutionSpineReport records Source/Sink/Split, pushdown, field-mask, predicate-ordering,
residual, and Native I/O requirements without enabling runtime behavior.
VortexArrayExecutionCertificate records the execution-layer evidence required before claims.
VortexLayoutAdvisorReport records layout/write/read/pruning evidence requirements without claiming
layout optimization.
ShardLoomSessionModelReport records explicit registry/session posture without hidden globals.
Vortex integrations are baseline/oracle/reference only.
No runtime expansion, object-store execution, writes, GPU execution, managed-platform benchmark lane,
external engine invocation, or fallback execution is authorized by this RFC alone.
fallback_attempted=false remains visible.
```

