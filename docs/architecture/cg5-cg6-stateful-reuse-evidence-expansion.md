<!-- SPDX-License-Identifier: Apache-2.0 -->

# CG-5 / CG-6 / Stateful Reuse Evidence Expansion

Status: implemented fail-closed report contract for `GAR-0029-A`. This contract does not execute
correctness fixtures, benchmarks, cache reads, cache writes, cache replay, incremental recompute,
external engines, or fallback execution.

Schema:

```text
shardloom.cg5_cg6_stateful_reuse_evidence_expansion.v1
```

Shared surface fields:

```text
gar_0029_evidence_expansion_support_status=blocked
gar_0029_evidence_expansion_claim_gate_status=not_claim_grade
gar_0029_evidence_expansion_blocking_row_count=8
gar_0029_evidence_expansion_deterministic_blocker_report=true
gar_0029_evidence_expansion_correctness_evidence_attached=false
gar_0029_evidence_expansion_benchmark_evidence_attached=false
gar_0029_evidence_expansion_execution_certificate_evidence_attached=false
gar_0029_evidence_expansion_native_io_evidence_attached=false
gar_0029_evidence_expansion_stateful_reuse_evidence_attached=false
gar_0029_evidence_expansion_reuse_benchmark_evidence_attached=false
gar_0029_evidence_expansion_selected_workload_evidence_attached=false
gar_0029_evidence_expansion_stateful_reuse_runtime_supported=false
gar_0029_evidence_expansion_cache_read_allowed=false
gar_0029_evidence_expansion_cache_write_allowed=false
gar_0029_evidence_expansion_cache_replay_allowed=false
gar_0029_evidence_expansion_incremental_execution_allowed=false
gar_0029_evidence_expansion_performance_claim_allowed=false
gar_0029_evidence_expansion_superiority_claim_allowed=false
gar_0029_evidence_expansion_production_reuse_claim_allowed=false
gar_0029_evidence_expansion_claim_grade_closeout_allowed=false
gar_0029_evidence_expansion_benchmark_rerun_performed=false
gar_0029_evidence_expansion_runtime_execution_performed=false
gar_0029_evidence_expansion_fallback_attempted=false
gar_0029_evidence_expansion_external_engine_invoked=false
```

## Purpose

RFC 0029 requires CG-5 correctness evidence, CG-6 benchmark evidence, CG-16 execution
certificates, and CG-17 stateful reuse proof before stateful reuse, incremental recompute, or
performance/superiority claims can be promoted. The current repo has strong report surfaces, scoped
fixtures, and explicit blocker vocabulary, but it does not yet have claim-grade evidence for broad
stateful reuse or benchmark claims.

`GAR-0029-A` makes that state visible through the three surfaces users and agents already inspect:

- `correctness-harness-plan`
- `benchmark-claim-evidence-plan`
- `stateful-reuse-plan`

## Evidence Rows

| Row | Surface | Status | Current blocker |
| --- | --- | --- | --- |
| `cg5_correctness_closeout` | `correctness-harness-plan` | blocked | Deferred fixture-family artifacts, external-oracle artifacts, and property/fuzz execution are not populated as claim-grade evidence. |
| `cg6_benchmark_closeout` | `benchmark-claim-evidence-plan` | blocked | Measured result rows, external comparison rows, reproducibility manifest, and full benchmark profile evidence are incomplete. |
| `cg16_execution_certificate_linkage` | `execution-certificate-plan` | blocked | Execution certificates are not linked across the selected reuse/benchmark workload. |
| `cg19_native_io_linkage` | `native-io-envelope-plan` | blocked | Source/sink Native I/O refs are scoped and not attached to broad reuse workloads. |
| `cg17_stateful_reuse_boundary_evidence` | `stateful-reuse-plan` | blocked | Boundary vocabulary exists, but cache lookup/write/replay is not supported. |
| `cg17_stable_reuse_key_invalidation` | `cg17-stateful-reuse-gate` | blocked | Stable keys, manifest-diff input evidence, and invalidation decisions are not certified. |
| `cg17_reuse_benchmark_constitution` | `benchmark-claim-evidence-plan` | blocked | Reuse benchmark constitution and source-state evidence are not populated with claim-grade runs. |
| `public_claim_attachment` | `release-plan` | blocked | Per-claim evidence attachment across correctness, benchmark, certificate, Native I/O, no-fallback, and release gates is incomplete. |

## Claim Rule

No stateful reuse, incremental recompute, performance, superiority, replacement, or production reuse
claim may pass until all rows have workload-scoped evidence, the selected workload has correctness
and benchmark proof, execution certificates and Native I/O refs are attached, cache invalidation is
certified, no fallback was attempted, and external systems remain baseline/oracle-only.

## Non-Goals

- No correctness fixture execution.
- No benchmark rerun.
- No cache read, cache write, cache replay, or incremental recompute execution.
- No external engine invocation.
- No fallback execution.
- No performance, superiority, replacement, or production reuse claim.
