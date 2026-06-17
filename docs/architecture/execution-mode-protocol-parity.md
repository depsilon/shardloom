# Execution Mode Protocol Parity

Status: Completed P7.5.8 protocol parity reference; GAR-FLOW-3A REST/OpenAPI parity artifact
Applies to: CLI JSON, Python client, benchmark rows, report-only REST/OpenAPI surfaces

## Purpose

ShardLoom must not let CLI, Python, benchmark, and planned REST surfaces invent separate execution
mode vocabularies. All surfaces use the same shared selection report and compute-flow evidence
fields.

This document is a protocol parity contract. It does not authorize a REST server, external engine
fallback, package publication, or new execution modes.

## Execution Mode Vocabulary

```text
auto
compatibility_import_certified
prepared_vortex
native_vortex
direct_compatibility_transient
```

`direct_compatibility_transient` is internal smoke-only for scoped local adapter safeguards. It is
not an admitted public workflow route: public local-file `auto` requests must route through Vortex
preparation/native input or fail closed, and explicit public `direct` requests are blocked. It must
not satisfy Vortex-native claim gates, broad SQL/DataFrame claims, object-store/table claims, or
performance claims.

## Shared Selection Report Fields

Every protocol surface that exposes an executed or admitted local compute request should preserve:

```text
execution_mode_selection_schema_version
requested_execution_mode
selected_execution_mode
execution_mode
mode_selection_reason
execution_mode_family
source_format
workload_constitution_id
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
vortex_native_claim_allowed
certification_requested
result_sink_requested
prepared_artifact_available
native_vortex_provider_available
mode_supported
unsupported_diagnostic_code
blocker_id
required_future_evidence
claim_gate_status
claim_gate_reason
fallback_attempted
external_engine_invoked
```

## Shared Compute-Flow Evidence Fields

Where available, protocol surfaces should also preserve:

```text
prepared_artifact_ref
prepared_artifact_fact_ref
prepared_artifact_dim_ref
prepared_artifact_digest
prepared_artifact_fact_digest
prepared_artifact_dim_digest
prepared_artifact_lifecycle_status
prepared_artifact_cleanup_policy
prepared_artifact_reuse_eligible
prepared_artifact_workspace
provider_admission_report_id
provider_admission_classification
provider_kind
provider_api_surface
residual_executor
residual_boundary
encoded_native_execution_status
fusion_status
fusion_blocker
materialization_boundary_report_emitted
materialization_boundary_rows
data_decoded
data_materialized
row_read
computed_result_sink_requested
computed_result_sink_written
computed_result_sink_replay_verified
computed_result_sink_write_micros
computed_result_sink_native_io_certificate_status
result_sink_claim_gate_status
result_sink_claim_gate_reason
fallback_attempted
external_engine_invoked
```

Missing optional evidence must be represented as absent, `none`, or `evidence_incomplete`; it must
not be silently promoted to success.

## Python Contract

The Python client may request supported local modes through:

```text
ShardLoomClient.traditional_analytics_run(..., execution_mode="auto|compatibility_import_certified")
ShardLoomClient.traditional_analytics_vortex_run(..., execution_mode="auto|prepared_vortex|native_vortex")
```

The Python result view exposes read-side parity through `ExecutionResultEnvelopeView`:

```text
execution_mode_selection_fields
compute_flow_evidence_fields
requested_execution_mode
selected_execution_mode
mode_selection_reason
execution_mode_family
mode_supported
claim_gate_status
claim_gate_reason
unsupported_diagnostic_code
blocker_id
required_future_evidence
vortex_native_claim_allowed
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
computed_result_sink_replay_verified
computed_result_sink_native_io_certificate_status
result_sink_claim_gate_status
result_sink_claim_gate_reason
fallback_attempted
external_engine_invoked
```

## REST/OpenAPI Contract Artifact

The checked-in OpenAPI contract at `docs/api/shardloom-openapi-v1.yaml` is still report-only: it
does not authorize a listener, server process, remote execution, dependency expansion, or REST
runtime path. It now carries the same execution-mode vocabulary and selection report field names
used by CLI JSON, Python, and benchmark rows so future REST work cannot invent a separate protocol.

REST request schemas should carry:

```text
requested_execution_mode
engine_mode
certification_requested
result_sink_requested
source_format
workload_constitution_id
```

REST responses should embed `execution_mode_selection` with the same selection report and
compute-flow evidence field names used by CLI JSON and Python. REST must not introduce a different
enum, rename claim gates, omit the selected mode, or hide auto-mode selection reasons.

Unsupported REST mode requests must return deterministic diagnostics with:

```text
mode_supported=false
unsupported_diagnostic_code
blocker_id
required_future_evidence
fallback_attempted=false
external_engine_invoked=false
```

Until a real REST runtime/server slice is implemented and evidenced, the REST parity artifact uses:

```text
support_status=report_only
claim_gate_status=report_only
fallback_attempted=false
external_engine_invoked=false
```

## No-Fallback Rule

REST, Python, CLI, and benchmark rows may use external engines only as baselines or oracles. They
must never use Spark, DataFusion, DuckDB, Polars, Dask, Ray, Trino, Velox, or platform engines as
ShardLoom runtime fallback.
