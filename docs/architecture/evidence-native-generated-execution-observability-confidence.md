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

## OpenLineage Facet Mapping

OpenLineage export is opt-in. No lineage event is emitted by default, no network call is made by
default, and no lineage backend integration is implied by this report-only design.

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

OpenTelemetry trace export is opt-in. No network exporter is configured by default. A future local
JSON trace preview may be acceptable before any OTLP exporter is added.

Planned spans:

| Span | Parent | Required attributes |
| --- | --- | --- |
| `request_admission` | trace root | request id, API surface, capability status, policy status, selected execution mode |
| `source_read` | request | source kind, source I/O performed, bytes/rows if known, Native I/O certificate ref |
| `compatibility_parse` | request | format, parse status, materialization/decode status |
| `vortex_import` | request | artifact refs, write/reopen timing, schema/layout digest |
| `vortex_scan` | request | projected columns, predicate/provider status, materialization/decode fields |
| `operator_compute` | request | operator family, execution class, residual executor, row counts |
| `result_sink` | request | sink kind, output I/O performed, output certificate ref |
| `evidence_render` | request | evidence artifact refs, redaction policy, render timing |
| `claim_gate` | request | claim gate status, blockers, workload refs |

Attribute safety rules:

- Do not put secrets, credentials, full local paths, unredacted query text, or unbounded samples into
  span attributes.
- Use digest/ref fields where possible.
- Add `otel_export_enabled=false` and `otel_network_exporter_enabled=false` by default.
- A future exporter must require explicit policy, redaction, retention, and endpoint configuration.

## Bayesian Confidence Schema

Bayesian claim confidence is advisory. It cannot upgrade a claim from not-claim-grade to
claim-grade by itself. It may block or hold a release/performance claim when uncertainty is high.

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

- No generated-output runtime in this report-only design.
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
