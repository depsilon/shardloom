<!-- SPDX-License-Identifier: Apache-2.0 -->

# Competitive Replacement Sufficiency Gate

Status: implemented fail-closed report contract for `GAR-0025-A`. This gate does not authorize
replacement, superiority, Spark-displacement, production platform, package-publication, runtime,
benchmark-rerun, external-engine, or fallback claims.

Schema:

```text
shardloom.competitive_replacement_sufficiency_gate.v1
```

Release-plan fields:

```text
competitive_replacement_sufficiency_gate_support_status=blocked
competitive_replacement_sufficiency_gate_claim_gate_status=not_claim_grade
competitive_replacement_sufficiency_gate_all_claims_blocked=true
competitive_replacement_sufficiency_gate_public_engine_replacement_claim_allowed=false
competitive_replacement_sufficiency_gate_spark_displacement_claim_allowed=false
competitive_replacement_sufficiency_gate_superiority_claim_allowed=false
competitive_replacement_sufficiency_gate_production_platform_claim_allowed=false
competitive_replacement_sufficiency_gate_fallback_attempted=false
competitive_replacement_sufficiency_gate_external_engine_invoked=false
```

## Purpose

RFC 0025 allows competitive replacement language only when ShardLoom has workload-scoped evidence
across correctness, benchmarks, Native I/O, execution certificates, capability coverage,
no-fallback policy, and release/publication gates. The current repo has strong scoped evidence and
explicit blockers, but it is not sufficient for broad replacement language.

This gate makes that insufficiency visible as one release-claim gate. It complements:

- `shardloom.engine_replacement_claim_inventory.v1`
- `shardloom.spark_displacement_benchmark_evidence_matrix.v1`
- `shardloom.publication_api_schema_stability_gate.v1`
- `shardloom.workspace_feature_build_matrix.v1`
- `WorldClassSufficiencyReport`

## Required Evidence Rows

| Row | Status | Required evidence | Current blocker |
| --- | --- | --- | --- |
| `correctness_evidence` | blocked | CG-5 semantic conformance, differential fixtures, fuzz/property coverage, edge-case fixtures, decoded-reference linkage. | Current correctness evidence is scoped and still has deferred fixture families. |
| `benchmark_evidence` | blocked | CG-6 full benchmark profile, complete competitor lane artifact, reproducible environment, workload-scoped timing and coverage rows. | Current benchmark evidence is local/pre-release and not claim-grade for replacement language. |
| `native_io_evidence` | blocked | CG-19 source/sink Native I/O certificates for every claimed input/output path. | Native I/O coverage is scoped and does not cover broad local, object-store, lakehouse, or platform paths. |
| `execution_certificate_evidence` | blocked | CG-16 execution certificates with plan/input/output hashes, segment traces, side-effect manifest, and reproducibility metadata. | Execution-certificate evidence is not complete across replacement claim workloads. |
| `capability_coverage_evidence` | blocked | CG-20 capability coverage for SQL, operators, functions, adapters, Python/DataFrame, user workflow, and blocked unsupported paths. | Capability coverage is mixed ready/smoke/report-only/blocked and cannot support broad replacement claims. |
| `no_fallback_policy_evidence` | blocked | No-fallback policy, external-baseline-only classification, dependency audit, and release no-fallback checks. | No-fallback policy is represented, but it cannot compensate for missing claim-grade workload evidence. |
| `release_publication_evidence` | blocked | Hard release gate, package-channel readiness, API/schema stability gate, provenance, SBOM, checksum, signing, and human approval. | Public release/package and API/schema stability gates remain blocked. |

## Claim Rule

No competitive replacement, Spark-displacement, superiority, production-platform, or best-default
claim may pass until every row above is `present`, evidence is workload-scoped, no fallback was
attempted, external systems are baseline/oracle-only, and the release/publication gate allows the
exact claim.

The current gate intentionally reports:

```text
correctness_sufficient=false
benchmark_sufficient=false
native_io_sufficient=false
execution_certificate_sufficient=false
capability_coverage_sufficient=false
no_fallback_sufficient=false
release_evidence_sufficient=false
runtime_execution_performed=false
benchmark_rerun_performed=false
fallback_attempted=false
external_engine_invoked=false
```

## Non-Goals

- No replacement claim.
- No Spark replacement or Spark-displacement claim.
- No performance/superiority claim.
- No production SQL/DataFrame claim.
- No object-store/lakehouse/Foundry production claim.
- No runtime expansion.
- No benchmark rerun.
- No package publication.
- No external engine fallback.
