# Runtime Evidence-Level Tiering

## Purpose

This document is the report-only architecture reference for `GAR-PERF-2A`. It formalizes
evidence-level runtime tiering so ShardLoom can measure and explain different evidence costs without
creating a hidden fast mode or weakening no-fallback policy.

The goal is to separate runtime/evidence intent from execution mode:

```text
execution_mode = how the data path runs
evidence_level = how much proof the caller requested and received
```

Every evidence level must continue to report no-fallback and no-external-engine status.

## Evidence Levels

| Evidence level | Intended use | Required proof | Claim posture |
| --- | --- | --- | --- |
| `minimal_runtime` | Local runtime/benchmark development where heavy replay proof is not requested. | Execution mode, fallback/external-engine status, claim gate, source-state digest if available, output digest if available. | `not_claim_grade` unless later explicit gates approve a scoped promotion. |
| `certified` | Normal certificate-bearing execution for claim-safe local evidence. | Existing execution certificate, Native I/O certificate, materialization/decode, source/sink evidence, no-fallback fields, claim gate. | May become claim-grade only when all workload-scoped gates pass. |
| `full_replay` | Result-sink proof and replay-heavy certification workflows. | All `certified` evidence plus result-sink write/reopen/replay proof and replay certificate refs. | Claim-grade only for the scoped replay-certified workload. |

`minimal_runtime` is not a hidden fast mode. It is an explicit, visible evidence level that trades
away heavy result-sink replay evidence unless requested. It must not silently omit policy fields or
be promoted into public benchmark/performance claims.

## Required Fields

All execution envelopes, benchmark rows, and future Python/API capability views that expose evidence
leveling should include:

```text
execution_mode
evidence_level
fallback_attempted=false
external_engine_invoked=false
source_state_digest
output_digest
claim_gate_status
```

Additional fields remain level-dependent:

```text
execution_certificate_status
source_native_io_certificate_status
result_native_io_certificate_status
materialization_decode_evidence_present
computed_result_sink_requested
computed_result_sink_written
computed_result_sink_replay_verified
result_sink_claim_gate_status
```

Unknown or unavailable evidence should be reported explicitly as `not_available`, `not_requested`,
`not_applicable`, `unsupported`, or `blocked` rather than omitted.

## Runtime Rules

- `minimal_runtime` may omit heavy result-sink replay unless the caller explicitly requests replay.
- `certified` emits the normal certificate surfaces for the selected execution mode.
- `full_replay` emits result-sink replay proof and keeps write/replay timing visible.
- Every level preserves `fallback_attempted=false` and `external_engine_invoked=false`.
- `auto` execution-mode selection must still report the selected mode and reason.
- Evidence level never changes execution semantics by itself.
- Evidence level never invokes Spark, DataFusion, DuckDB, Polars, Vortex query-engine integrations,
  or another engine as fallback.

## Benchmark Interpretation

Benchmark artifacts should make evidence level visible beside execution mode. A `minimal_runtime`
row can help isolate runtime development overhead, but it is not claim-grade by default and must not
be presented as a public speed ranking.

Comparisons should remain scoped:

```text
compatibility_import_certified + certified
compatibility_import_certified + full_replay
prepared_vortex + minimal_runtime
prepared_vortex + certified
native_vortex + minimal_runtime
native_vortex + certified
```

Compatibility-import rows still include ingest/stage costs. Prepared/native rows remain the runtime
optimization lane. Evidence-level tiering explains proof overhead; it does not create a performance,
superiority, Spark-replacement, SQL/DataFrame, object-store/lakehouse, Foundry, or production claim.

## User Surface Rules

Future CLI, Python, website benchmark, and capability views should present evidence level as a
first-class field:

```text
evidence_level=minimal_runtime|certified|full_replay
claim_gate_status=not_claim_grade|claim_grade|...
fallback_attempted=false
external_engine_invoked=false
```

The user should be able to tell:

- what execution mode ran
- what evidence level was requested
- whether source-state and output digests are available
- whether result-sink replay was requested and verified
- why a row is or is not claim-grade

## Non-Goals

- No hidden global fast mode.
- No fallback engine.
- No runtime behavior change in this report-only document.
- No benchmark recomputation.
- No performance, superiority, Spark-replacement, production, SQL/DataFrame, object-store/lakehouse,
  or Foundry claim.
- No package publication.

## Acceptance

- `minimal_runtime`, `certified`, and `full_replay` are documented as distinct evidence levels.
- No-fallback and no-external-engine fields remain required for every level.
- `minimal_runtime` is explicitly `not_claim_grade` unless a future scoped gate says otherwise.
- `full_replay` is the only level that requires result-sink replay proof by default.
- Benchmark docs distinguish evidence overhead from pure operator/runtime timing.

## Verification Plan

Planning-only updates should run:

```text
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```

Future implementation slices should add contract tests for all three evidence levels and benchmark
smoke coverage for `minimal_runtime` versus `full_replay`.
