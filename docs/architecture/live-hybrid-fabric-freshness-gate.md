# Live/Hybrid Fabric Freshness Gate

## Purpose

`GAR-0034-A` adds the report-only
`shardloom.live_hybrid_fabric_freshness_gate.v1` contract for ShardLoom's CG-22 live and hybrid
engine fabric. The gate keeps current fixture evidence separate from production live/hybrid claims
and makes every missing broker, state-store, object-store, catalog, freshness, and exactly-once
dependency explicit.

This is a diagnostic and capability surface only. It does not execute live workloads, open brokers,
create checkpoints, read object stores, write catalogs, invoke external engines, or attempt
fallback.

## User-Visible Surfaces

- `shardloom engine-capability-matrix --format json`
- `shardloom live-hybrid-state-transition-smoke --format json`
- `shardloom capabilities engines --format json`
- Python `ctx.engine_capability_matrix()` through `EngineCapabilityMatrix`
- Python `ctx.live_hybrid_state_transition_smoke()`
- this architecture reference

The CLI and Python surfaces expose:

- `live_hybrid_fabric_gate_schema_version=shardloom.live_hybrid_fabric_freshness_gate.v1`
- `live_hybrid_fabric_gate_report_id=gar-0034-a.live_hybrid_fabric_freshness_gate`
- `live_hybrid_fabric_gate_claim_gate_status=not_claim_grade`
- `live_hybrid_fabric_gate_fallback_attempted=false`
- `live_hybrid_fabric_gate_external_engine_invoked=false`

## Gate Rows

| Row | Status | Applies to | Boundary |
| --- | --- | --- | --- |
| `live_broker_adapter` | `blocked` | live | No broker adapter runtime or production live ingestion claim. |
| `live_durable_checkpoint_store` | `blocked` | live | No durable checkpoint or recovery claim beyond scoped in-memory fixtures. |
| `live_unbounded_scheduler` | `blocked` | live | No production unbounded stream scheduler claim. |
| `live_freshness_certificate` | `fixture_smoke_only` | live | Freshness evidence is fixture-scoped and cannot become production freshness proof. |
| `live_exactly_once_claim` | `blocked` | live | No exactly-once claim without checkpoint, commit, retry, and sink idempotency proof. |
| `live_hybrid_state_transition_fixture` | `fixture_smoke_only` | live, hybrid | Bounded in-memory retry/cancellation/cleanup proof only. |
| `hybrid_micro_segment_flush` | `blocked` | hybrid | Hybrid micro-segment flush remains evidence/planning only without write and commit proof. |
| `hybrid_object_store_commit` | `blocked` | hybrid | No object-store hybrid runtime or commit claim. |
| `hybrid_catalog_snapshot` | `blocked` | hybrid | No external catalog or table snapshot runtime claim. |
| `baseline_oracle_boundary` | `report_only` | live, hybrid | External systems may be baselines or oracles only, never fallback engines. |

The summary counts are:

- `live_hybrid_fabric_gate_blocked_row_count=7`
- `live_hybrid_fabric_gate_report_only_row_count=1`
- `live_hybrid_fabric_gate_fixture_smoke_row_count=2`

## Bounded State-Transition Fixture

`live-hybrid-state-transition-smoke` executes only the checked-in in-memory fixture. It records a
hybrid selected mode, source and target snapshot refs, snapshot epoch, freshness and state
certificates, a cooperative cancellation on the first attempt, cleanup completion, and a certified
retry on the second attempt. It does not use a broker, durable checkpoint store, object store,
external state service, plugin, external engine, or fallback path.

Key fields include:

```text
schema_version=shardloom.live_hybrid_state_transition_fixture.v1
mode=live_hybrid_state_transition_smoke
selected_engine_mode=hybrid
snapshot_epoch=11
freshness_certificate_status=certified
state_certificate_status=certified
state_transition_certificate_status=certified
retry_policy=single_retry_after_cooperative_cancellation
attempt_count=2
cancellation_cleanup_completed=true
partial_output_committed=false
durable_checkpoint_store_used=false
durable_checkpoint_write_performed=false
exactly_once_claim_allowed=false
production_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

## Evidence Required Before Promotion

Future slices cannot promote a row from blocked/report-only posture without workload-scoped evidence.
The gate names the evidence classes that must exist before production live/hybrid claims can be
considered:

- `broker_adapter_contract`
- `durable_checkpoint_store`
- `unbounded_scheduler`
- `object_store_runtime`
- `commit_protocol`
- `freshness_certificate`
- `state_certificate`
- `delta_overlay_certificate`
- `exactly_once_idempotency`
- `baseline_oracle_policy`
- `no_fallback_evidence`

## Claim Boundary

The current claim boundary is:

```text
fixture-scoped live/hybrid evidence only; production live/hybrid freshness, exactly-once,
object-store, table/catalog, broker, state-store, benchmark, and Spark-displacement claims remain
blocked
```

The following booleans remain `false`:

- `live_hybrid_fabric_gate_freshness_claim_allowed`
- `live_hybrid_fabric_gate_exactly_once_claim_allowed`
- `live_hybrid_fabric_gate_production_live_claim_allowed`
- `live_hybrid_fabric_gate_production_hybrid_claim_allowed`
- `live_hybrid_fabric_gate_object_store_runtime_supported`
- `live_hybrid_fabric_gate_broker_runtime_supported`
- `live_hybrid_fabric_gate_state_store_runtime_supported`

`live_hybrid_fabric_gate_baseline_oracle_only=true` means external engines or managed systems may
only provide comparison/oracle context. They cannot satisfy ShardLoom execution evidence and cannot
act as fallback engines.

## Non-Goals

This slice does not add:

- broker-backed live ingestion
- durable state-store or checkpoint runtime
- unbounded scheduler runtime
- production freshness or exactly-once claims
- durable recovery beyond the in-memory fixture lifecycle proof
- hybrid object-store writes or commits
- table/catalog snapshot runtime
- external baseline/oracle execution
- benchmark, performance, or Spark replacement claims

## Fallback Boundary

Every row preserves:

```text
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

Any future live/hybrid runtime slice must keep these fields visible and fail closed when required
evidence is missing.
