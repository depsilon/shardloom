# Evidence-Aware Logical Optimizer

Status: completed report-only contract for `GAR-PERF-2B`.

## Summary

`GAR-PERF-2B` adds the first-class optimizer rule registry and report-only optimizer trace that
ShardLoom needs before broader logical optimizer claims can be made. The target is not lazy
optimizer parity with Polars, SQL engines, or DataFusion. The current surface explains which
rewrite families are admitted, applied, blocked, unsupported, not applicable, or report-only while
preserving no-fallback policy, materialization boundaries, execution-mode evidence, and claim
gates.

This document does not authorize runtime rewrites or SQL/DataFrame execution. The current
implementation applies no rewrites and emits `claim_gate_status=not_claim_grade`.

## Source References

- Polars lazy query optimization overview:
  <https://docs.pola.rs/user-guide/concepts/lazy-api/>
- Polars query plan/explain guide:
  <https://docs.pola.rs/user-guide/lazy/query-plan/>
- Polars Python `LazyFrame.explain` API:
  <https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.explain.html>
- Polars Python `QueryOptFlags` API:
  <https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html>
- ShardLoom optimizer RFC:
  [`docs/rfcs/0016-optimizer-adaptive-execution-runtime-filters-skew.md`](../rfcs/0016-optimizer-adaptive-execution-runtime-filters-skew.md)
- ShardLoom Plan IR RFC:
  [`docs/rfcs/0022-plan-ir-substrait-compatible-interoperability.md`](../rfcs/0022-plan-ir-substrait-compatible-interoperability.md)

Polars is a design reference only. ShardLoom must not call Polars as an optimizer or execution
fallback.

## Current State

ShardLoom has execution modes, capability posture, Plan IR surfaces, explain/estimate diagnostics,
and report-only adaptive optimizer/memory planning. It also has scoped source-backed scan evidence
for selected prepared/native benchmark rows.

`GAR-PERF-2B` now adds:

- `plan_evidence_aware_optimizer_trace()` in the plan crate.
- `optimizer-plan --format json|text|markdown` as a side-effect-free CLI trace surface.
- Python `ShardLoomClient.optimizer_plan()` and `EvidenceAwareOptimizerTraceReport` typed
  accessors.
- ShardLoom benchmark row fields linking rows to the report-only optimizer trace contract.
- Snapshot/contract tests for the CLI envelope, Python wrapper, plan report, and benchmark row
  schema.

What is still not claimable:

- no general lazy optimizer parity.
- no runtime optimizer rewrite.
- no before/after plan digest for an applied rewrite.
- no SQL/DataFrame runtime.
- no performance/superiority claim.

## Initial Rule Registry

The first registry includes these rule families:

- predicate pushdown.
- projection pushdown.
- slice/limit pushdown.
- common subplan/source-state reuse.
- expression simplification.
- constant folding.
- type coercion.
- join ordering.
- cardinality estimation.

Current status:

| Rule family | Current status | Runtime rewrite |
| --- | --- | --- |
| predicate pushdown | `report_only` | not applied |
| projection pushdown | `report_only` | not applied |
| slice/limit pushdown | `blocked` | not applied |
| common subplan/source-state reuse | `admitted` | not applied |
| expression simplification | `unsupported` | not applied |
| constant folding | `unsupported` | not applied |
| type coercion | `blocked` | not applied |
| join ordering | `blocked` | not applied |
| cardinality estimation | `not_applicable` | not applied |

Report-only and admitted rows are evidence/classification only until a future slice adds
before/after plan digests, correctness smoke, materialization/decode proof, and claim-gate evidence
for an applied rewrite.

## Evidence Contract

Optimizer trace, CLI output, Python accessors, and benchmark rows expose:

```text
optimizer_trace_id
optimizer_registry_version
optimizer_phase
optimizer_rule_id
optimizer_rule_family
optimizer_rule_status
optimizer_rule_admitted
optimizer_rule_applied
optimizer_rule_blocked_reason
before_plan_digest
after_plan_digest
rewrite_safety_status
evidence_preserved=true
no_fallback_preserved=true
claim_boundary_preserved=true
materialization_boundary_preserved
source_state_reuse_admitted
estimated_input_cardinality
estimated_output_cardinality
cardinality_estimation_status
correctness_smoke_ref
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

`optimizer_rule_status` should distinguish at least `admitted`, `applied`, `blocked`,
`unsupported`, `not_applicable`, and `report_only`.

The current report also exposes aggregate counts, the registry version
`gar-perf-2b.optimizer_registry.v1`, trace ID
`optimizer_trace.gar_perf_2b.report_only_registry`, and benchmark ref
`optimizer-trace://gar-perf-2b.report-only-registry`.

## Rewrite Safety Rules

An optimizer rewrite may be applied only when:

- the rewrite preserves logical semantics for the active semantic profile.
- before/after plan digests are recorded.
- evidence and claim boundaries are preserved.
- no source, sink, external effect, credential, object-store probe, or fallback execution is
  performed during explain/optimization.
- materialization/decode boundaries remain explicit.
- unsupported expressions remain ShardLoom-native residual work or deterministic blockers.
- correctness smoke exists before a rewrite becomes runtime-supported.

## User-Visible Surface

Current surfaces:

- CLI `optimizer-plan`.
- Python `ShardLoomClient.optimizer_plan()` typed view.
- benchmark rows that link to optimizer trace IDs.
- compute-flow docs and website compute-flow rendering.

## Claim Boundary

An optimizer trace can claim only that ShardLoom classified or applied a scoped rewrite under
explicit evidence. It cannot claim:

- broad SQL runtime.
- broad DataFrame runtime.
- Polars/DataFusion optimizer parity.
- performance/superiority.
- Spark replacement.
- production readiness.
- object-store/lakehouse/Foundry runtime.

## Verification Plan

Current verification includes:

- plan report tests for report-only, admitted, blocked, unsupported, and not-applicable rows.
- CLI JSON snapshot tests for the `optimizer-plan` envelope.
- Python typed accessor tests.
- benchmark harness row-contract tests for optimizer trace fields on ShardLoom rows.

Future runtime rewrite implementation should include:

- plan snapshot tests for admitted/applied/blocked/unsupported rule rows.
- before/after digest stability tests.
- correctness smoke comparing optimized and unoptimized paths for every applied rewrite.
- no-fallback tests that prove optimizer/explain did not call external engines or perform effects.
- benchmark harness row-contract tests when optimizer trace refs are emitted.

## Risks

- A rewrite that looks locally safe can change null, ordering, limit, join, or type-coercion
  semantics.
- Explain surfaces can accidentally imply runtime support if report-only rows are not explicit.
- Cardinality estimates can be read as facts unless estimation status and uncertainty remain visible.
- Common subplan/source-state reuse can be misread as hidden caching unless reuse scope and digests
  are reported.
