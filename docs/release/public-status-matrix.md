<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public Status Matrix

Status: canonical public status and claim-boundary owner.

Schema marker: `shardloom.public_status_matrix.v1`.

This matrix is the source-owned public posture summary for the README, getting-started docs,
Python README, release readiness docs, and website wording. It describes what a public reader may
infer from the current repository state. v0.4.0 package channels are published where explicitly
listed, but that does not authorize production support, benchmark superiority, Spark displacement,
external execution, or fallback execution.

Finished-product v1 scope is defined in
[`docs/release/finished-product-scope.md`](finished-product-scope.md). Public wording should start
from supported ShardLoom surfaces, not from broad external-engine replacement framing.
The active v1 queue and feasibility firewall is tracked in
[`docs/release/v1-inclusion-scope-matrix.md`](v1-inclusion-scope-matrix.md).
The selected local/source/package v1 release track is tracked in
[`docs/release/v1-local-source-package-release.md`](v1-local-source-package-release.md).
Selected package channels are published for v0.4.0; real production environment gates remain
blocked.
The scoped local SQL/Python/DataFrame front-door runtime boundary is defined in
[`docs/architecture/v1-front-door-runtime-scope.md`](../architecture/v1-front-door-runtime-scope.md).
The scoped local/prepared v1 Vortex runtime boundary is defined in
[`docs/architecture/v1-vortex-runtime-scope.md`](../architecture/v1-vortex-runtime-scope.md).
The scoped local SourceState and prepared-state reuse boundary is defined in
[`docs/architecture/v1-source-prepared-state-scope.md`](../architecture/v1-source-prepared-state-scope.md).
The scoped local output/sink scope is defined in
[`docs/architecture/v1-local-output-sink-scope.md`](../architecture/v1-local-output-sink-scope.md);
append remains unsupported outside a later closed gate.
The scoped local observability/supportability boundary is defined in
[`docs/architecture/v1-observability-support.md`](../architecture/v1-observability-support.md);
support bundles remain local/redacted and remote telemetry/upload surfaces remain unsupported.

## Local Engine Maturity

**Published local engine; operational hardening in progress.** This is the current public
support label. The technical-preview designation describes the support promise, not whether
the engine or packages exist. v0.4.0 is available through GitHub, TestPyPI, PyPI and Homebrew;
the [publication verification](v0.4.0-publication-verification.md) closes the selected-channel
proof requirement.

The local engine executes admitted SQL, Python/DataFrame and CLI workflows through one native
pipeline, including typed and nested results, exact decimal reductions, analytic frames and
local writers. Native ordering spill, cancellation, corrupt-run and quota rejection, slow or
failed consumers, source-generation checks and owned cleanup have executable evidence. Their
scope is recorded in the [resource contract](../architecture/native-relational-resources-2026-10-02.md)
and [current release acceptance](../benchmarks/release-candidate-fresh-uat-2026-10-05.md).

Current source builds add reviewed FSST/Zstd buffer ownership,
public resident batch intake and incremental results, plus two retained native
CPU optimizations. Its [acceptance report](../benchmarks/native-engine-acceptance-2026-10-06.md)
records the complete source/public/Full43 checks and the three dropped hardware
prototypes. PR #1526 merged after all 39 hosted checks passed. Published v0.4.0
predates these additions. Target-cohort gains are not a broad engine performance claim.

The subsequent [Zstd workspace acceptance](../benchmarks/native-codec-workspaces-2026-10-07.md)
adds actual decoder and by-reference prepared-dictionary admission to the same
native memory owner. Local ownership and complete regression checks pass;
PR #1528 merged after all 39 hosted checks passed with the accepted runtime
source unchanged. Compression/training state and other
unreviewed allocations remain outside that finite resource proof. This does
not change the published version or public support label.

The [builder acceptance](../benchmarks/native-builder-resources-2026-10-07.md)
further covers native primitive/Boolean/decimal Chunked output and builder
finalization buffers, preserving independent value/validity ownership. Complete
local source/public/Full43 checks and packet inspection pass; hosted integration
is pending. Child decoder scratch, structural metadata and general operator
spill remain separate. Its measured cost screen is a resource-correction check,
not a speedup or process-RSS guarantee.

The remaining maturity requirements are an explicit supported local workload/platform envelope,
accounting and safe failure through readers/codecs/operators/writers, and accepted workload-wide
pressure, cancellation, fault and recovery evidence. A query memory grant is not a process-RSS
ceiling; general aggregate/join/window spill remains incomplete. The
[local-engine exit criteria](production-certification-gate.md#local-engine-preview-exit-criteria)
define the required evidence and approval. Missing cloud integrations or complete SQL parity do
not, by themselves, block a stable release for a narrower declared local scope.

## Claim Boundary

Public claim booleans remain fail-closed unless a later release-approved gate changes them:
the publication-attempt fields below describe this status/gate surface's side effects, not
the history of the already published package channels.

```text
package_publication_status=published_v0.4.0_selected_channels
public_package_release_claim_allowed=true
public_release_claim_allowed=false
public_package_claim_allowed=false
performance_claim_allowed=false
performance_superiority_claim_allowed=false
production_claim_allowed=false
spark_replacement_claim_allowed=false
broad_engine_replacement_claim_allowed=false
drop_in_replacement_claim_allowed=false
production_platform_claim_allowed=false
publication_attempted=false
tag_created=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
```

## Current Public Posture

| Surface | Current posture | What this permits | What remains blocked |
| --- | --- | --- | --- |
| Source checkout | Supported for local development and admitted native workflows. | Clone, build, run local CLI/Python workflows, inspect evidence. | Production support and performance claims beyond accepted evidence; a source build alone does not verify package channels. |
| Local first-10-minutes path | Supported through getting-started examples, `scripts/release_dry_run_proof.py`, and local smoke reports. | Local execution proof over installed or source-built artifacts. | Production or performance claims; the proof command itself does not publish, tag, sign or use secrets. |
| CLI and Python front doors | Scoped local CSV, JSON/JSONL/NDJSON, generated rows, local Vortex, and feature-gated flat-scalar Parquet, Arrow IPC/Feather, Avro, and ORC paths through `ctx.read(...)` or explicit helpers. | Evidence-backed local route use with no-fallback fields visible; feature-gated structured readers return deterministic blockers when unavailable. | Broad SQL/DataFrame parity, server/API production support, object-store/table readers, hidden external execution. |
| SQL/DataFrame-style surface | Selected local-source projections, filters, joins, aggregates, bounded collects, and local writes are admitted through ShardLoom routes. | Scoped local workflow evidence and deterministic unsupported blockers. | PySpark/pandas/Polars parity, arbitrary SQL/DataFrame runtime, performance equivalence. |
| Vortex preparation and local primitives | Feature-gated local `vortex_ingest` creates `VortexPreparedState` evidence for scoped flat local inputs. Scoped local Vortex primitives are covered by the feature-gated local Vortex runtime scope. | Explicit preparation, prepared/native route inspection, and local primitive count/filter/project route reports. | Broad writer support, object-store/table/catalog preparation, generalized Vortex Source/Sink runtime, production staging. |
| SourceState and prepared-state reuse | The declarative source/prepared-state scope, golden fixtures, and invalidation matrix define local reuse through `ResidentVortexSession`, with source-generation and declaration validation on each execution and no query-answer cache. This scope report is declarative specification, not runtime evidence; current runtime evidence remains in live tests and harnesses. | Local SourceState normalization and explicit session or VortexPreparedState reuse, with deterministic invalidation behavior where exercised by live tests. | Global hidden cache, external cache service, object-store/table prepared-state reuse, broad non-local preparation, production cache claims. |
| Local output/sink scope | The declarative output/sink scope records the single `native_vortex_query` route through `shared_native_workflow`, its admitted format/method/policy declarations, and no-fallback posture. The scope report is not runtime verification; complete-value readback and writer behavior must be established by live tests or harnesses. | Local output through the shared native workflow for admitted formats and shapes, with runtime sink evidence reported by live executions. | Append mode, object-store output paths, table/catalog writes, Iceberg/Delta transactions, remote URI sinks, broad nested/complex sink claims. |
| Observability/supportability | Scoped local doctor, support-bundle, agent-contract, capability, runtime-report, schema-coverage, plan-only explain/estimate, diagnostic-code, issue-template, and benchmark-field checks are closed by the v1 observability/support boundary. | Local redacted support bundles, deterministic troubleshooting, side-effect-free support surfaces, and no-fallback/no-external-engine support evidence. | OpenTelemetry/OpenLineage export, remote support upload, live profiling collection, production observability/SRE claims, and performance claims. |
| Benchmarks | The parameterized local harness runs one ShardLoom candidate through the public native workflow, with declared input state and output format. It retains source, executable, command, timing, and complete-result evidence. Comparison engines provide independent references only. | Local correctness comparisons with explicit timing boundaries; published historical evidence keeps its original scope. | Public performance superiority, Spark displacement, stale-artifact claims, timing-surface substitution. |
| Object store, lakehouse, Foundry, live/hybrid | Mostly report-only, fixture-scoped, or blocked for broader platform routes. | Capability posture, local fixture proof where explicitly named, blocked diagnostics. | Production platform/runtime claims and managed-service integrations. |
| Website | Static public interpretation layer over checked-in source/evidence. | Claim-safe docs, use-case, benchmark, status, and architecture views. | Runtime expansion, package publication, public benchmark freshness beyond promoted artifacts. |
| Package/release channels | v0.4.0 selected channels are published and proof-backed: GitHub pre-release assets, TestPyPI, PyPI, and Homebrew. | Install access through `gh release download v0.4.0`, `python -m pip install shardloom==0.4.0`, and `brew install depsilon/tap/shardloom`; channel proof refs remain checked in. | Scoop, winget, conda-forge, GHCR, crates.io, real production environment gates, signing/attestation expansion, and production/package-maturity claims remain blocked. |

## Public Docs Ownership

| Public surface | Role | Required source |
| --- | --- | --- |
| `README.md` | Compact public entry point and support-posture summary. | This matrix plus `docs/architecture/compute-engine-flow-reference.md`. |
| `docs/release/finished-product-scope.md` | Canonical v1 support boundary and allowed public claim language. | Phase plan plus per-claim evidence matrix. |
| `docs/release/v1-inclusion-scope-matrix.md` | V1 required/candidate/deferred row classification and unsupported-surface firewall. | Phase plan plus known unsupported paths. |
| `docs/architecture/v1-front-door-runtime-scope.md` | Scoped local v1 SQL/Python/DataFrame front-door boundary. | Python parity matrix, user route capability report, and local scenario runner. |
| `docs/architecture/v1-vortex-runtime-scope.md` | Scoped feature-gated local/prepared v1 Vortex runtime boundary. | Declarative local Vortex primitive and user route reports; live tests and the public workflow harness provide runtime evidence. |
| `docs/architecture/v1-source-prepared-state-scope.md` | Scoped local SourceState and prepared-state reuse boundary. | Declarative source prepared-state scope report, golden fixtures, invalidation matrix, user route capability report; live tests/harnesses provide runtime evidence. |
| `docs/architecture/v1-local-output-sink-scope.md` | Scoped local output/sink scope. | Declarative local output sink scope report, golden fixtures, user route capability report; live tests/harnesses provide complete-value readback and runtime writer evidence. |
| `docs/architecture/v1-observability-support.md` | Scoped local observability/supportability boundary. | Observability support report, diagnostic-code stability doc, troubleshooting guide, issue templates, route capability report, API/schema stability report, and benchmark timing-surface fields. |
| `docs/getting-started/install.md` | Source checkout and local install path. | This matrix plus package-channel readiness docs. |
| `docs/getting-started/source-checkout-install.md` | Source checkout build and local proof path. | This matrix plus release dry-run proof docs. |
| `docs/getting-started/package-user-install.md` | Published package-channel availability, uninstall, upgrade and future-channel boundaries. | Package-channel readiness matrix plus this matrix. |
| `docs/getting-started/first-10-minutes.md` | Local proof walkthrough. | This matrix plus release dry-run proof docs. |
| `docs/getting-started/examples.md` | Copyable scoped examples and blockers. | This matrix plus relevant capability docs. |
| `docs/getting-started/v1-supported-unsupported.md` | Generated current supported/unsupported surface. | Runs-today support matrix plus package-channel matrix. |
| `docs/getting-started/troubleshooting-support.md` | Local troubleshooting and support-bundle guide. | V1 observability/support scope plus diagnostic-code stability docs. |
| `python/README.md` | Python wrapper/API posture. | This matrix plus Python user-surface and parity gates. |
| `docs/release/public-technical-preview-readiness.md` | Historical public-preview readiness pass and checklist. | This matrix for current posture. |
| `website-src/` | Public web interpretation layer. | This matrix, synced data artifacts, and website readiness checks. |

## Maintenance Rules

- Keep this file as the canonical public status matrix; do not create parallel README-only or
  website-only status tables with different claim boundaries.
- Public docs may summarize the matrix, but they must link back here when they mention package,
  production, performance, broad engine replacement, Spark-displacement, SQL/DataFrame,
  object-store/lakehouse, Foundry, or release posture.
- Positive broad replacement, drop-in parity, production platform, performance superiority, or
  broad SQL/DataFrame parity language is blocked unless a matching claim row is closed. Otherwise
  the wording must be framed as blocked, unsupported, baseline-only, historical, or no-fallback
  policy.
- Benchmark wording must state `timing_surface` and `claim_gate_status` before comparing numbers.
- Package wording must distinguish source checkout, local wheel dry run, selected published
  v0.4.0 channels, blocked future channels, and production/package-maturity claims.
- Website and docs changes must preserve `fallback_attempted=false` and
  `external_engine_invoked=false` for ShardLoom execution evidence.
