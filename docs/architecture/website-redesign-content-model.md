# Website Redesign Content Model

Status: accepted direction for `GAR-WEB-REDESIGN-2B`.

## Decision

The redesigned website should be data-driven even if it stays on the current Python static
generator. Pages should render from small structured registries rather than hand-maintained,
inconsistent HTML.

The initial content model should cover:

- routes;
- field-guide terms;
- use cases;
- status rows;
- benchmark summary cards;
- reference files;
- claim boundaries.

## Shared Status Vocabulary

Use these public statuses consistently:

```text
runtime_supported
smoke_supported
fixture_smoke_only
report_only
blocked
unsupported
not_planned
```

Every status chip must include a text label. Color alone is not sufficient.

## Route Definition

Each route definition should include:

```yaml
id: prepared_vortex
label: Prepared Vortex route
summary: Executes from VortexPreparedState after preparation exists.
access_surfaces:
  - Python
  - CLI
  - future SQL/DataFrame
input_requirements:
  - VortexPreparedState
output_routes:
  - OutputPlan
evidence_fields:
  - selected_execution_mode
  - prepared_state_id
  - prepared_state_digest
  - fallback_attempted
  - external_engine_invoked
claim_boundary: Prepared execution support is workload-scoped and does not imply broad SQL/DataFrame or production support.
fallback_boundary: No external engine fallback.
references:
  - docs/architecture/compute-engine-flow-reference.md
```

Canonical route IDs:

- `certified_import_stage`;
- `vortex_ingest_prepare_once`;
- `prepared_vortex`;
- `native_vortex`;
- `direct_compatibility_transient`;
- `generated_source`;
- `output_fanout`;
- `external_baseline_context`.

## Field Guide Term

Each Field Guide term should include:

```yaml
slug: vortex-ingest
title: vortex_ingest
category: Vortex Ingest
status: smoke_supported
meaning: Prepares an admitted non-Vortex source into VortexPreparedState.
related_route: vortex_ingest_prepare_once
evidence_fields:
  - ingress_route
  - vortex_ingest_status
  - prepared_state_id
related_terms:
  - UniversalIngress
  - SourceState
  - VortexPreparedState
related_use_cases:
  - prepare-vortex-once-local
references:
  - docs/architecture/compute-engine-flow-reference.md
  - docs/architecture/phased-execution-plan.md
```

Dossier sections:

- Plain-English meaning.
- Why it matters.
- How ShardLoom uses it.
- Current support.
- Evidence fields.
- What it does not claim.
- Try it / related use cases.
- Related concepts.
- Reference files.

Minimum initial terms:

- ShardLoom;
- evidence-gated compute;
- no fallback;
- UniversalIngress;
- SourceState;
- vortex_ingest;
- VortexPreparedState;
- compatibility_import_certified;
- prepared_vortex;
- native_vortex;
- direct_compatibility_transient;
- GeneratedSourceCertificate;
- OutputPlan;
- SinkArtifact;
- result-sink replay;
- Native I/O certificate;
- claim gate;
- benchmark evidence;
- external baseline;
- object-store boundary;
- table/lakehouse boundary;
- Foundry boundary.

## Use Case

Each use case should include:

```yaml
id: local-csv-to-parquet
title: Read local CSV and write a local result
audience: local evaluator
status: smoke_supported
execution_route: direct_compatibility_transient
engine_mode: batch
inputs:
  - local CSV
outputs:
  - local Parquet
quick_example: |
  import shardloom as sl
  ctx = sl.context()
  report = ctx.read_csv("orders.csv").select(["id", "amount"]).write_parquet("out.parquet")
evidence_fields:
  - source_adapter_status
  - output_native_io_certificate_status
  - fallback_attempted
  - external_engine_invoked
claim_boundary: Local smoke support only; not broad Parquet, SQL/DataFrame, object-store, or production support.
blocked_explanation: null
references:
  - README.md
  - python/README.md
  - docs/getting-started/first-10-minutes.md
related_use_cases:
  - prepare-vortex-once-local
```

Use cases must always include either:

- a runnable command/code snippet for runtime/smoke-supported paths; or
- a blocker explanation for report-only/blocked/unsupported/not-planned paths.

## Status Row

Each status row should include:

```yaml
capability: S3/GCS/ADLS
status: blocked
surface:
  - CLI
  - Python capability view
input_types:
  - s3
  - gcs
  - adls
output_types: []
execution_routes:
  - future object_store_runtime
what_works: Report-only planning and boundaries.
what_is_blocked: Runtime object-store read/write and table commit.
evidence_required:
  - credential_policy_status
  - object_store_io
  - fallback_attempted
  - external_engine_invoked
claim_boundary: No object-store runtime claim.
references:
  - docs/architecture/universal-compatibility-coverage-scoreboard.md
```

Required initial capability rows:

- local CSV;
- local JSONL/NDJSON;
- local JSON;
- local Parquet;
- Arrow IPC;
- Avro;
- ORC;
- Vortex input;
- generated/source-free output;
- Python;
- SQL/DataFrame;
- S3/GCS/ADLS;
- Iceberg/Delta/Hudi;
- Foundry;
- benchmarks;
- package/release.

## Benchmark Summary Card

Each benchmark summary card should include:

```yaml
id: prepared-warm-route
title: Prepared warm query route
timing_scope: warm_prepared_query
summary: Runs after VortexPreparedState exists.
shows:
  - vortex_scan_millis
  - operator_compute_millis
  - output_write_millis
  - evidence_render_millis
warnings:
  - Does not include source read unless preparation_included=true.
claim_boundary: Local benchmark evidence only, not a leaderboard.
```

Required benchmark cards:

- Certified cold ingest/stage route.
- Prepared warm query route.
- Native Vortex route.
- Direct transient route.
- External baseline context.
- Artifact completeness.
- Claim-gate distribution.

## Reference File Block

Every Field Guide and use-case page should render exact repo paths:

```yaml
references:
  - path: docs/architecture/compute-engine-flow-reference.md
    proves: canonical route vocabulary and compute-flow interpretation
  - path: docs/benchmarks/local-taxonomy-benchmark.md
    proves: benchmark evidence boundary
```

Avoid vague links like "see docs." Exact repo paths are required.

## Claim Boundary Block

Every page or card that describes support must include a claim boundary. Standard public boundary:

```text
This is local technical-preview evidence. It does not imply production readiness, performance or
superiority, Spark replacement, broad SQL/DataFrame runtime, object-store/lakehouse/Foundry runtime,
package publication, or external-engine fallback.
```

## Generator Requirements

If the current Python generator remains:

- put structured registries near the top of `website/build_static_pages.py` only while small;
- move registries into separate JSON/YAML files when they become hard to review;
- generate pages from templates/helpers instead of repeated string fragments;
- keep `website/validate_static_assets.js` route-aware;
- keep `scripts/check_website_readiness.py` claim-aware.

If Astro is later approved:

- model these as content collections;
- keep static output;
- preserve existing route redirects;
- preserve validation and Cloudflare deployment;
- avoid runtime external fetches.

## Acceptance For This Content Model

- Every public concept has a status.
- Every support statement has a claim boundary.
- Every blocked/report-only path has a visible blocker.
- Every user-facing route links to exact reference files.
- Every external engine mention states baseline-only context.
- Every benchmark interpretation states evidence-not-leaderboard.

