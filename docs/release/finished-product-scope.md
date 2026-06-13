<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finished Product Scope

Status: v1 public claim-boundary source.

Schema marker: `shardloom.finished_product_scope.v1`.

ShardLoom v1 is a Vortex-first, no-fallback, evidence-certified compute engine for explicitly
supported local/runtime surfaces. Broader platform or runtime families may be included in v1 only
when they pass feasibility review, implementation evidence, benchmark/correctness gates, safety
gates, and release approval for the exact supported surface.

This document defines what v1 may claim directly about ShardLoom. It keeps external-engine names
available for policy, diagnostics, baselines, migration/oracle references, and historical context
without turning those names into replacement, superiority, or drop-in parity claims.

## V1 Product Boundary

The v1 product boundary is:

- ShardLoom-native execution with `fallback_attempted=false`.
- Vortex-native input and output as first-class surfaces.
- Explicit compatibility imports and exports when named by the support row.
- Local CLI/Python front-door workflows that have runtime, certificate, and no-fallback evidence.
- SQL/DataFrame-style usage only for the admitted ShardLoom routes and semantics documented by the
  current support rows.
- Benchmark interpretation only when the timing surface, evidence tier, route lane, and claim-gate
  status are named.
- Broader platform families only after their phase-plan rows close with concrete evidence.

The boundary is not a claim that ShardLoom is production-ready, a drop-in replacement, faster than
external engines, a Spark displacement engine, or a broad SQL/DataFrame parity layer.

## Required V1 Claim Rows

These rows must exist in the per-claim evidence matrix before the project can describe a finished v1
support boundary:

| Claim row | V1 role | Evidence boundary |
| --- | --- | --- |
| `local_runtime_product_claim` | Defines the supported source-built local compute engine product surface. | Local runtime proof, certificates, deterministic diagnostics, no-fallback evidence, and user-facing docs. |
| `api_schema_stability_claim` | Defines API/schema compatibility promises for public surfaces. | API stability gate, schema compatibility window, package identity, and migration notes. |
| `supported_front_door_scope_claim` | Defines supported CLI, Python, and SQL/DataFrame-style front doors. | Front-door support matrix, examples, unsupported diagnostics, and route capability evidence. |
| `supported_vortex_route_claim` | Defines supported Vortex-native preparation/query routes. | Vortex input/output evidence, timing surface, certificates, and source/route admission proof. |
| `supported_output_sink_claim` | Defines supported local output/sink routes. | Vortex, JSONL/CSV, evidence artifact, overwrite, digest, and no-fallback sink proof. |
| `security_supply_chain_claim` | Defines source, dependency, CI, package, and release safety posture. | Security gate, dependency provenance, SBOM/checksum, signing/OIDC posture, and known unsupported paths. |
| `external_baseline_comparison_claim` | Defines how external baselines may be interpreted. | Benchmark profile, correctness baseline status, no-fallback evidence, timing surface labels, and no superiority language. |

`public_release_claim` and `public_package_claim` remain separate release-channel rows. They do not
become true merely because v1 support rows exist.

## Out-of-V1 Claim Rows

These rows remain blocked or historical until a later phase explicitly promotes them with evidence:

- `performance_superiority_claim`
- `spark_displacement_claim`
- `engine_replacement_claim`
- `production_sql_dataframe_claim`
- `object_store_lakehouse_claim`
- `foundry_platform_claim`

They may remain in evidence matrices and historical ledgers so reviewers can see the boundary, but
they must not be used as the finished-product v1 center of gravity.

## Allowed External Engine Contexts

External engine names such as Spark, DuckDB, Polars, DataFusion, pandas, PySpark, Trino, Dask, Ray,
Velox, databases, and warehouses are allowed only in these contexts:

- No-fallback policy and architecture rules.
- Unsupported diagnostics and capability blockers.
- Benchmark baseline labels and benchmark methodology.
- Migration references and test-oracle references.
- Historical RFC, completed ledger, and design-background context.

They are not allowed as positive public claims that ShardLoom is a replacement, drop-in parity
layer, best default, or performance-superior engine.

## ShardLoom Technique Review

Before any new support row is accepted, the owning phase-plan item must record whether these
ShardLoom-specific techniques apply:

- PulseWeave execution or route coordination.
- capillary work-unit sizing, memory shaping, or operator flow.
- dynamic admission, adaptive planning, or runtime work shaping.
- Metadata-first execution and segment/statistics pruning.
- Encoded-columnar execution, late materialization, and zero-decode opportunities.
- Vortex-native preparation, scan, output, or replay evidence.
- timing-surface and evidence-tier reporting.

If a technique is not applicable, the row should say why. The review is not a demand to force every
technique into every feature; it is a guardrail against missing native performance opportunities.

## Public Language Rule

Public wording must state what ShardLoom supports. It may state what remains blocked, unsupported,
or out of scope. It must not imply broad replacement, superiority, drop-in parity, production
platform support, or package/public release status unless the matching claim row is closed with
evidence.

The validator for this rule is `scripts/check_public_claim_language.py`.
