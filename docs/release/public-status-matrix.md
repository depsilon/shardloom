<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public Status Matrix

Status: canonical public status and claim-boundary owner.

Schema marker: `shardloom.public_status_matrix.v1`.

This matrix is the source-owned public posture summary for the README, getting-started docs,
Python README, release readiness docs, and website wording. It describes what a public reader may
infer from the current repository state. It does not authorize package publication, release tags,
production support, benchmark superiority, Spark displacement, external execution, or fallback
execution.

Finished-product v1 scope is defined in
[`docs/release/finished-product-scope.md`](finished-product-scope.md). Public wording should start
from supported ShardLoom surfaces, not from broad external-engine replacement framing.
The active v1 queue and feasibility firewall is tracked in
[`docs/release/v1-inclusion-scope-matrix.md`](v1-inclusion-scope-matrix.md).
The scoped local SQL/Python/DataFrame front-door runtime boundary is defined in
[`docs/architecture/v1-front-door-runtime-scope.md`](../architecture/v1-front-door-runtime-scope.md).
The scoped local/prepared v1 Vortex runtime boundary is defined in
[`docs/architecture/v1-vortex-runtime-scope.md`](../architecture/v1-vortex-runtime-scope.md).

## Claim Boundary

Public claim booleans remain fail-closed unless a later release-approved gate changes them:

```text
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
| Source checkout | Supported for local development and smoke proof. | Clone, build, run local CLI/Python smokes, inspect evidence. | Public package/release claims, production support, benchmark claims. |
| Local first-10-minutes path | Supported through `scripts/release_dry_run_proof.py`, getting-started examples, and local smoke reports. | Local technical-preview proof over source-built artifacts. | Package publication, tags, signing, secrets, production or performance claims. |
| CLI and Python front doors | Scoped local CSV, JSONL/NDJSON, flat JSON, generated rows, local Vortex, and selected feature-gated paths. | Evidence-backed local route use with no-fallback fields visible. | Broad SQL/DataFrame parity, server/API production support, hidden external execution. |
| SQL/DataFrame-style surface | Selected local-source projections, filters, joins, aggregates, bounded collects, and local writes are admitted through ShardLoom routes. | Scoped local workflow evidence and deterministic unsupported blockers. | PySpark/pandas/Polars parity, arbitrary SQL/DataFrame runtime, performance equivalence. |
| Vortex preparation and local primitives | Feature-gated local `vortex_ingest` creates `VortexPreparedState` evidence for scoped flat local inputs. Scoped local Vortex primitives are covered by the feature-gated local Vortex runtime scope. | Explicit preparation, prepared/native route inspection, and local primitive count/filter/project route reports. | Broad writer support, object-store/table/catalog preparation, generalized Vortex Source/Sink runtime, production staging. |
| Benchmarks | Promoted artifacts separate route lanes, timing surfaces, evidence tiers, and claim gates. | Evidence interpretation for hot runtime, replay proof, publication proof, and external baselines. | Public performance superiority, Spark displacement, stale-artifact claims, timing-surface substitution. |
| Object store, lakehouse, Foundry, live/hybrid | Mostly report-only, fixture-scoped, or blocked for broader platform routes. | Capability posture, local fixture proof where explicitly named, blocked diagnostics. | Production platform/runtime claims and managed-service integrations. |
| Website | Static public interpretation layer over checked-in source/evidence. | Claim-safe docs, use-case, benchmark, status, and architecture views. | Runtime expansion, package publication, public benchmark freshness beyond promoted artifacts. |
| Package/release channels | Local no-publication rehearsals and package-channel posture reports exist. | Local wheel/sdist build and install smoke evidence. | PyPI, Conda, Homebrew, crates.io, GitHub release, GHCR, signing, tags, uploads. |

## Public Docs Ownership

| Public surface | Role | Required source |
| --- | --- | --- |
| `README.md` | Compact public entry point and support-posture summary. | This matrix plus `docs/architecture/compute-engine-flow-reference.md`. |
| `docs/release/finished-product-scope.md` | Canonical v1 support boundary and allowed public claim language. | Phase plan plus per-claim evidence matrix. |
| `docs/release/v1-inclusion-scope-matrix.md` | V1 required/candidate/deferred row classification and unsupported-surface firewall. | Phase plan plus known unsupported paths. |
| `docs/architecture/v1-front-door-runtime-scope.md` | Scoped local v1 SQL/Python/DataFrame front-door boundary. | Python parity matrix, user route capability report, and local scenario runner. |
| `docs/architecture/v1-vortex-runtime-scope.md` | Scoped feature-gated local/prepared v1 Vortex runtime boundary. | Local Vortex primitive route report, user route capability report, and local benchmark route report. |
| `docs/getting-started/install.md` | Source checkout and local install path. | This matrix plus package-channel readiness docs. |
| `docs/getting-started/first-10-minutes.md` | Local proof walkthrough. | This matrix plus release dry-run proof docs. |
| `docs/getting-started/examples.md` | Copyable scoped examples and blockers. | This matrix plus relevant capability docs. |
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
- Package wording must distinguish source checkout, local wheel dry run, package-channel posture,
  and actual publication.
- Website and docs changes must preserve `fallback_attempted=false` and
  `external_engine_invoked=false` for ShardLoom execution evidence.
