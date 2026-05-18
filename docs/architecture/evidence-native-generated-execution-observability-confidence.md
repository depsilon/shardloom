# Evidence-Native Generated Execution, Lineage, Observability, And Confidence

## Purpose

This document is the report-only architecture reference for `GAR-NOVEL-1`. It ties four
cross-cutting work streams into one evidence-native model:

- source-free generated-output execution and `GeneratedSourceCertificate` evidence
- OpenLineage-compatible custom facets
- OpenTelemetry-compatible execution traces
- Bayesian claim-confidence and regression reports

The goal is not to add exporters or runtime behavior immediately. The goal is to make ShardLoom's
evidence model exportable, explainable, and claim-gated without weakening no-fallback policy.

## External Model Grounding

OpenLineage's core model is built around runs, jobs, and datasets, with facets as atomic metadata
attached to those entities. Custom facets need their own namespace/prefix and schema URL/version
discipline. ShardLoom should therefore map evidence into explicit ShardLoom-owned facets rather than
overloading standard dataset or job metadata.

OpenTelemetry represents trace work as spans with attributes/events, and export requires an SDK,
processor, exporter, or collector path. ShardLoom should therefore treat tracing export as opt-in
and should not configure a network exporter by default.

References:

- OpenLineage specification: `https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md`
- OpenLineage custom facets: `https://github.com/OpenLineage/OpenLineage/blob/main/website/docs/spec/facets/custom-facets.md`
- OpenTelemetry concepts: `https://opentelemetry.io/docs/concepts/`
- OpenTelemetry OTLP: `https://opentelemetry.io/docs/specs/otlp/`

## Generated-Source Contract

Source-free generated output is not no-dataset smoke.
The current executable surface is intentionally narrow: Python `ctx.from_rows([...]).write(...)`
and `ctx.range(...).write(...)` can write scoped local JSONL fixture-smoke outputs with
`GeneratedSourceCertificate` and output Native I/O evidence. SQL literal/`VALUES`, broad DataFrame
builders, Foundry generated-output, object-store output, and lakehouse/table output remain
report-only or blocked until separate evidence lands.

| Case | Meaning | Evidence posture | Claim boundary |
| --- | --- | --- | --- |
| `no_dataset_smoke` | Capability/status/proof run with no input dataset and no output data claim. | No generated rows, no source Native I/O certificate, no output data certificate. | Smoke only. |
| `user_generated_source` | User code creates rows and ShardLoom consumes those rows as a generated/literal source. | Requires deterministic generated-source evidence and local output sink evidence before a data claim. | Scoped local generated-output smoke only after runtime evidence lands. |
| `engine_native_generated_source` | ShardLoom plan contains generator nodes such as `range`, `sequence`, `values`, `literal_table`, `calendar`, or deterministic synthetic profile. | Requires generated-source evidence, execution evidence, and output sink evidence. | No broad SQL/DataFrame claim without separate parser/API runtime proof. |

Required generated-source fields:

```text
input_dataset_count=0
source_io_performed=false
generated_source_created
generated_source_kind
generated_source_schema_digest
generated_source_row_count
generated_source_plan_digest
generated_source_seed
generation_deterministic
output_io_performed
output_native_io_certificate_status
generated_source_certificate_status
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

No source Native I/O certificate is emitted when no source dataset exists. Output evidence is still
required for output claims.

## GAR-NOVEL-1A Cross-Surface Alignment Report

`GAR-NOVEL-1A` adds the report-only capability alignment schema:

```text
schema_version=shardloom.generated_source_evidence_alignment.v1
report_id=gar-novel-1a.generated_source_cross_surface_alignment
generated_source_contract_ref=shardloom.generated_source_certificate_contract.v1
generated_source_api_admission_ref=shardloom.generated_source_api_admission.v1
openlineage_facets_ref=GAR-NOVEL-1B.report_only_facets
opentelemetry_spans_ref=GAR-NOVEL-1C.report_only_spans
bayesian_confidence_ref=GAR-NOVEL-1D.report_only_confidence
openlineage_export_enabled=false
opentelemetry_export_enabled=false
opentelemetry_network_exporter_enabled=false
bayesian_confidence_enabled=false
foundry_runtime_invoked=false
object_store_io_performed=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

The row order is:

```text
no_dataset_smoke
python_generated_source_write
sql_dataframe_source_free
foundry_generated_output
```

This alignment report is a coordination layer. It lets CLI and Python capability views show that
generated-source evidence, report-only OpenLineage facets, report-only OpenTelemetry spans, future Bayesian
confidence, and Foundry generated-output boundaries all use the same no-fallback/no-external-engine
vocabulary. It does not emit OpenLineage events, configure an OpenTelemetry exporter, fit a Bayesian
model, invoke Foundry, or write object-store output.

## OpenLineage Facet Mapping

OpenLineage export is opt-in. No lineage event is emitted by default, no network call is made by
default, and no lineage backend integration is implied by this report-only design.

`GAR-NOVEL-1B` adds the report-only capability schema:

```text
schema_version=shardloom.openlineage_facet_mapping.v1
report_id=gar-novel-1b.openlineage_facet_mapping
gar_id=GAR-NOVEL-1B
openlineage_object_model_ref=https://openlineage.io/docs/spec/object-model/
openlineage_facets_ref=https://openlineage.io/docs/spec/facets/
openlineage_custom_facets_ref=https://openlineage.io/docs/spec/facets/custom-facets/
producer_placeholder=https://github.com/depsilon/shardloom
schema_url_base_placeholder=https://shardloom.io/schemas/openlineage/
export_enabled=false
event_emitted=false
network_call_performed=false
backend_configured=false
client_dependency_added=false
schema_published=false
redaction_policy_required=true
retention_policy_required=true
opt_in_required=true
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

The row order is:

```text
execution_mode
no_fallback
native_io_certificate
materialization_boundary
claim_gate
generated_source
vortex_artifact
```

The mapping follows OpenLineage's run/job/dataset object model and custom-facet extensibility
rules: ShardLoom evidence maps to ShardLoom-owned facets with a distinct `shardloom_*` key,
a ShardLoom producer URI, and a future immutable schema URL. The current slice deliberately leaves
schemas unpublished and export disabled, so capability discovery can expose the mapping without
adding an OpenLineage dependency, backend, event emitter, or network effect.

| ShardLoom facet | OpenLineage entity | Source evidence | Notes |
| --- | --- | --- | --- |
| `shardloom.ExecutionModeFacet` | Run facet | `execution_mode`, `engine_mode`, selected-mode reason, provider kind | Records explicit execution mode; never hides `auto` selection. |
| `shardloom.NoFallbackFacet` | Run facet | `fallback_attempted`, `fallback_execution_allowed`, `external_engine_invoked` | Must preserve no-fallback/no-external-engine posture. |
| `shardloom.NativeIoCertificateFacet` | Input/output dataset facet and run facet refs | Native I/O certificate refs, source/sink capability, transition evidence | Dataset facets carry path-specific evidence; run facet may carry refs. |
| `shardloom.MaterializationBoundaryFacet` | Run or dataset facet | `data_decoded`, `data_materialized`, `stayed_encoded`, representation transitions | Does not imply zero-decode unless evidence says so. |
| `shardloom.ClaimGateFacet` | Run facet | `claim_gate_status`, workload constitution refs, claim blockers | Cannot upgrade claim status. |
| `shardloom.GeneratedSourceFacet` | Run facet and output dataset facet | `GeneratedSourceCertificate` fields | Used only when generated rows exist. |
| `shardloom.VortexArtifactFacet` | Dataset facet | Vortex artifact refs, digest, schema, layout/version evidence | Must not imply official Vortex endorsement. |

Facet schema rules:

- Use a ShardLoom-owned prefix or namespace.
- Include `_producer` and immutable `_schemaURL` fields once schemas are published.
- Preserve redaction and retention policy before exporting paths, query text, schema names, samples,
  or artifact refs.
- Keep `openlineage_export_enabled=false` unless the user explicitly opts in.

## OpenTelemetry Trace Mapping

OpenTelemetry trace export is opt-in. No SDK dependency, OTLP exporter, collector, backend, runtime
collection, trace emission, metric emission, log emission, or network exporter is configured by
default. A future local JSON trace preview may be acceptable before any OTLP exporter is added.

`GAR-NOVEL-1C` adds the report-only capability schema:

```text
schema_version=shardloom.opentelemetry_trace_export_contract.v1
report_id=gar-novel-1c.opentelemetry_trace_export_contract
gar_id=GAR-NOVEL-1C
opentelemetry_traces_ref=https://opentelemetry.io/docs/concepts/signals/traces/
opentelemetry_common_ref=https://opentelemetry.io/docs/specs/otel/common/
otlp_spec_ref=https://opentelemetry.io/docs/specs/otlp/
otlp_exporter_ref=https://opentelemetry.io/docs/specs/otel/protocol/exporter/
schema_url_base_placeholder=https://shardloom.io/schemas/opentelemetry/
trace_export_enabled=false
metric_export_enabled=false
log_export_enabled=false
otlp_exporter_configured=false
network_exporter_enabled=false
collector_configured=false
sdk_dependency_added=false
runtime_collection_enabled=false
trace_emitted=false
metric_emitted=false
log_emitted=false
network_call_performed=false
attribute_allowlist_required=true
redaction_policy_required=true
retention_policy_required=true
opt_in_required=true
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

The report maps ShardLoom timing/evidence fields into future OTel span placeholders. It follows
OpenTelemetry's trace/span signal vocabulary, attribute key/value model, and OTLP exporter
configuration model, but it deliberately does not add an SDK or exporter. Every row uses
`span_kind=internal`, `span_status=report_only_not_emitted`, `export_enabled=false`,
`span_emitted=false`, `metric_emitted=false`, `log_emitted=false`, and
`network_exporter_enabled=false`.

| Span placeholder | Timing fields | Attribute allowlist | Claim boundary |
| --- | --- | --- | --- |
| `shardloom.request_admission` | `request_admission_millis`, `total_runtime_millis` | execution mode, engine mode, admission status, selected-mode reason, claim gate, no-fallback fields | Does not emit traces or admit runtime support. |
| `shardloom.source_read` | `source_read_millis`, `source_discovery_millis`, `schema_inference_millis`, `source_parse_millis` | source format, source I/O status, source-state digest, row/file/byte counts | Does not enable object-store reads, credential resolution, or external source probes. |
| `shardloom.compatibility_parse` | `compatibility_parse_millis`, `compatibility_to_vortex_import_millis` | source format, parse status, generated-source status, source I/O status | Compatibility rows are certification/workflow timing, not pure query speed. |
| `shardloom.vortex_import` | `compatibility_to_vortex_import_millis`, `vortex_prepare_millis`, `vortex_write_millis`, `vortex_reopen_millis` | prepared-state digest, Vortex artifact digest, layout/encoding/statistics summaries | Does not publish artifacts or imply object-store/table runtime. |
| `shardloom.vortex_scan` | `vortex_scan_millis`, `source_backed_scan_millis` | scan pushdown flags, decode/materialization flags, source-backed scan status | Does not imply encoded-native execution without separate evidence. |
| `shardloom.operator_compute` | `operator_compute_millis` | operator execution class, fused-pipeline flag, row counts, encoded-native claim flag | Does not enable UDFs, external effects, SQL/DataFrame runtime, or performance claims. |
| `shardloom.result_sink` | `result_sink_write_millis`, `output_write_millis`, `output_replay_millis` | output I/O status, output format, output Native I/O certificate, result replay, output digest | Does not imply object-store write, lakehouse commit, or Foundry output support. |
| `shardloom.evidence_render` | `evidence_render_millis` | execution certificate, Native I/O certificate, materialization boundary, generated-source certificate, claim gate | Does not publish telemetry or upgrade claim status. |
| `shardloom.claim_gate` | `claim_gate_millis`, `evidence_render_millis`, `total_runtime_millis` | claim gate, claim boundary, performance/production/scale claim flags | Telemetry cannot create production, performance, Spark-replacement, Foundry, package, or scale claims. |

Attribute safety rules:

- Do not put secrets, credentials, full local paths, unredacted query text, or unbounded samples into
  span attributes.
- Use digest/ref fields where possible.
- Enforce `attribute_allowlist_required=true`, `redaction_policy_required=true`, and
  `retention_policy_required=true` before exporting.
- Add `opentelemetry_trace_export_trace_export_enabled=false`,
  `opentelemetry_trace_export_otlp_exporter_configured=false`,
  `opentelemetry_trace_export_network_exporter_enabled=false`, and
  `opentelemetry_trace_export_network_call_performed=false` by default.
- A future exporter must require explicit policy, redaction, retention, and endpoint configuration.

## Bayesian Confidence Schema

Bayesian claim confidence is advisory. It cannot upgrade a claim from not-claim-grade to
claim-grade by itself. It may block or hold a release/performance claim when uncertainty is high.
GAR-PERF-1D now provides the adjacent benchmark-row advisor contract
`shardloom.traditional_analytics.bayesian_advisor.v1` for report-only confidence and uncertainty
fields around mode/reuse/sizing/layout decision surfaces. The GAR-NOVEL-1D model below is the later
claim-confidence and regression layer; it still needs a fitted posterior model and release/claim
gate integration before it can block claims from statistical uncertainty.

Report-only fields:

```text
bayesian_confidence_report_version
input_evidence_refs
benchmark_constitution_ref
posterior_runtime_distribution
credible_interval
probability_of_regression
minimum_iterations_for_claim_grade
uncertainty_reason
advisor_version
claim_gate_status=advisory_only|not_claim_grade
```

Rules:

- The model never silently changes execution mode, layout choice, or optimization policy.
- The model must name the evidence population it was fit over.
- High uncertainty may block a performance/release claim.
- A favorable posterior cannot create a performance, superiority, or replacement claim without the
  existing correctness, benchmark, Native I/O, execution-certificate, materialization, policy, and
  no-fallback evidence gates.

## Non-Goals

- No additional generated-output runtime beyond the scoped local Python JSONL fixture smokes.
- No SQL/DataFrame runtime claim.
- No Foundry production claim.
- No OpenLineage backend integration.
- No OpenTelemetry network exporter by default.
- No runtime auto-optimization.
- No performance, superiority, Spark-replacement, object-store/lakehouse, or production claim.
- No dependency expansion without a separate dependency/license review.

## Acceptance

- Generated output remains distinct from no-dataset smoke.
- No source Native I/O certificate is emitted when no source exists.
- Output evidence remains required for output data claims.
- OpenLineage facets are report-only and opt-in.
- OpenTelemetry export is report-only and opt-in, with no default network exporter.
- Bayesian confidence is advisory and cannot upgrade claim status alone.
- All surfaces preserve `fallback_attempted=false` and `external_engine_invoked=false`.

## Verification

Current report-only validation:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```
