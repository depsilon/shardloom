# Bayesian Performance And Layout Advisor

Status: implemented report-only contract for GAR-PERF-1D.

## Purpose

The Bayesian performance and layout advisor is an advisory evidence layer for ShardLoom benchmark
artifacts. It names the future decision surfaces that need confidence modeling without letting a
confidence row silently change runtime behavior or upgrade claims.

The current implementation is intentionally conservative:

- It emits `shardloom.traditional_analytics.bayesian_advisor.v1` fields on ShardLoom benchmark rows.
- It records `advisor_version=gar-perf-1d.report_only.v1`.
- It reports confidence and uncertainty from available local benchmark evidence.
- It keeps `bayesian_advisor_claim_gate_status=advisory_only`.
- It sets `bayesian_advisor_runtime_decision_applied=false`.
- It preserves `bayesian_advisor_fallback_attempted=false` and
  `bayesian_advisor_external_engine_invoked=false`.

`GAR-RUNTIME-IMPL-6E-3` separately promotes one scoped cold layout/write decision inside
`vortex_ingest`: the workspace-safe local single-artifact Vortex writer can report
`vortex_layout_write_advisor_runtime_decision_applied=true` after the real writer path validates the
provider kind/surface, sink, admission policy, and verification boundary. That runtime decision does
not come from the Bayesian advisor and does not change the Bayesian contract above: Bayesian
confidence remains report-only until a later claim gate fits and validates a model.

## Vortex-First Provider Check

- Subject area: performance/layout recommendation evidence for prepared/native Vortex and
  compatibility-import benchmark rows.
- Upstream Vortex concept checked: layout strategy, statistics, source/sink posture, scan/pushdown
  posture, and local Vortex artifact preparation.
- Decision: `wrap_vortex_concept`.
- Vortex API/provider surface: no new Vortex provider is invoked in this slice.
- ShardLoom provider/report/certificate surface: benchmark row fields, existing Vortex layout/write
  advisor evidence, SourceState, VortexPreparedState, OutputPlan, build-profile evidence, and
  execution-mode attribution contracts.
- Residual handling: no residual runtime is executed by the advisor.
- Materialization/decode boundary: advisor rows consume existing evidence refs; they do not decode,
  materialize, write, compact, or rewrite layouts.
- Evidence added: report-only confidence/uncertainty fields and decision-surface fields.
- Gates still blocked: runtime decisioning, automatic layout writes, production recommendations,
  performance claims, Spark-displacement claims, SQL/DataFrame runtime, object-store/lakehouse, and
  Foundry support.
- `fallback_attempted=false`: required.

## Decision Surfaces

The contract covers these future advisory surfaces:

- execution-mode recommendation,
- source-state reuse threshold,
- batch rows,
- target partition bytes,
- max parallelism,
- layout/write choice.

The implemented row fields are:

```text
bayesian_advisor_schema_version
bayesian_advisor_version
bayesian_advisor_status
bayesian_advisor_confidence
bayesian_advisor_uncertainty_reason
bayesian_advisor_input_evidence_refs
bayesian_advisor_claim_gate_status
bayesian_advisor_execution_mode_recommendation_status
bayesian_advisor_requested_execution_mode
bayesian_advisor_selected_execution_mode
bayesian_advisor_mode_recommendation
bayesian_advisor_source_state_reuse_threshold_status
bayesian_advisor_source_state_reuse_threshold
bayesian_advisor_batch_rows_status
bayesian_advisor_batch_rows_recommendation
bayesian_advisor_target_partition_bytes_status
bayesian_advisor_target_partition_bytes
bayesian_advisor_max_parallelism_status
bayesian_advisor_max_parallelism
bayesian_advisor_layout_write_choice_status
bayesian_advisor_layout_write_choice
bayesian_advisor_runtime_decision_applied
bayesian_advisor_auto_mode_transparent
bayesian_advisor_fallback_attempted
bayesian_advisor_external_engine_invoked
bayesian_advisor_claim_boundary
```

## Current Behavior

The benchmark harness emits advisory fields for every ShardLoom row. The current advisor contract
is not a fitted posterior model; current rows are stable schema and claim-boundary evidence for
future posterior evidence.

The current confidence status is one of:

```text
low_report_only
insufficient_evidence
not_applicable
```

The uncertainty reason explicitly says the posterior model is not fit and that the benchmark
population is local smoke evidence. This keeps current benchmark rows useful for interpretation
without implying a performance recommendation.

GAR-NOVEL-1D adds the companion artifact-level report
`shardloom.traditional_analytics.bayesian_claim_confidence.v1`. That report records future
claim-confidence fields for posterior runtime distribution, credible interval, regression
probability, minimum-run policy, benchmark population refs, release policy refs, uncertainty reason,
and claim boundary. It is also report-only/not-fit: current artifacts set
`posterior_runtime_distribution=not_fit`, `credible_interval=not_computed`,
`probability_of_regression=not_computed`, `claim_upgrade_allowed=false`,
`runtime_decision_applied=false`, `layout_decision_applied=false`, `benchmark_recomputed=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

## Claim Boundary

The advisor cannot:

- change execution mode,
- enable or disable source-state reuse,
- change batch rows,
- change target partition bytes,
- change max parallelism,
- select or optimize a layout/write strategy from Bayesian confidence,
- upgrade `claim_gate_status`,
- create performance, superiority, or Spark-replacement claims,
- authorize package/public release claims,
- authorize production SQL/DataFrame, object-store/lakehouse, or Foundry support.

Future Bayesian output can block a claim when uncertainty is high. It cannot make a claim valid by
itself.

## Acceptance

- Benchmark artifacts include `bayesian_advisor_contract`.
- ShardLoom benchmark rows include all `bayesian_advisor_*` fields.
- ShardLoom rows keep `bayesian_advisor_claim_gate_status=advisory_only`.
- ShardLoom rows keep `bayesian_advisor_runtime_decision_applied=false`.
- ShardLoom rows keep no-fallback/no-external-engine fields false.
- External baseline rows remain context only.

## Verification

```powershell
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q benchmarks/traditional_analytics
git diff --check
```
