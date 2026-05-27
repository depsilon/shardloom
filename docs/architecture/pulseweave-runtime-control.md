# PulseWeave Runtime Control

Status: first prepared/local runtime implementation slice landed under `GAR-RUNTIME-IMPL-5R`;
remaining broader scopes stay blocked by their owning runtime items.

## Summary

PulseWeave is the implementation name for a certificate-gated runtime control layer that
makes ShardLoom's existing `auto` behavior more useful without adding user knobs, AI, persistent
learning, hidden maintenance state, or external-engine fallback.

The first implementation target is the prepared/local Vortex route that already has scoped runtime
evidence: local reader chunks, prepared/native batch runs, memory reservations, bounded
queue/backpressure fields, split-operator certificates, and runtime envelope validators. The user
surface should remain the same:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .write_jsonl("target/orders-out.jsonl", allow_overwrite=True)
)
```

No new required argument should be introduced. Existing optional `memory_gb=` and
`max_parallelism=` values remain explicit caps for jobs or benchmarks that need them; PulseWeave
must operate inside those caps rather than requiring the user to understand the control loop.

## Implementation Status

`GAR-RUNTIME-IMPL-5R` adds the first applied slice:

- provider-neutral policy in `shardloom-exec/src/pulseweave.rs`
- prepared/native batch wiring in `shardloom-vortex/src/traditional_analytics.rs`
- per-child `pulseweave_*`, `flow_inventory_*`, `scarcity_ledger_*`, `endopulse_*`, and
  `proofbound_*` evidence on the real prepared Vortex processing route
- `prepare_batch_scale_pulseweave_*` aggregate rollups on the existing prepare/batch envelope
- Python runtime-envelope validation that blocks incomplete or uncertified PulseWeave claims
- benchmark artifact promotion passthrough for future refreshed evidence

The implementation remains scoped to local prepared/native batch routes over certified Vortex
artifacts. Live object-store provider runtime, live/hybrid runtime, distributed execution,
effectful adapters, real query-data spill, production readiness, and performance claims remain
non-goals here.

`GAR-IOREUSE-1K` extends PulseWeave to the local cold-preparation route, but only through the
existing `vortex_ingest` SourceState -> VortexPreparedState path. The route now emits typed
capillary task evidence for source split discovery, read chunks, columnarize/encode, Vortex segment
write, reopen verification, and sink evidence. PulseWeave sees this task graph as
`vortex_ingest_cold_preparation` / `vortex_cold_preparation_local_capillary_io` and applies only
when ProofBound sees complete task, correctness, output, execution-certificate, Native I/O, and
no-fallback evidence. Missing Native I/O proof leaves the route in report-only blocked status.
This evidence still must not be read as a benchmark-backed speedup for source read, parse, Vortex
write, reopen, or evidence-rendering cost.

## Invention-Disclosure Names

These names are technical handles for implementation and invention-disclosure notes. They are not
legal advice and do not by themselves assert patentability.

- **PulseWeave Runtime**: the aggregate automatic control layer.
- **FlowInventory Scheduler**: bounded work-in-progress control for local prepared tasks.
- **ScarcityLedger Allocator**: deterministic resource-price accounting for memory, queue slots,
  decode/materialization risk, sink pressure, and unsupported spill risk.
- **EndoPulse Governor**: slow feedback loop that adjusts task shape only from observed local
  runtime signals inside a run.
- **ProofBound Auto Gate**: evidence/certificate gate that decides whether automatic policy
  application is allowed, blocked, or only reported.

Recommended invention-disclosure title:

```text
Certificate-gated resource-scarcity and bounded-inventory control for encoded columnar compute
engines.
```

## Current State

ShardLoom already has most of the primitives needed for a feasible first implementation:

- `TraditionalAnalyticsResourcePolicy` derives automatic memory, parallelism, batch rows, target
  partition bytes, and target partition count.
- The prepared/local Vortex evidence path already emits reader-chunk scheduler fields, queue limit
  enforcement, bounded backpressure status, memory reservation counts, spill blockers, retry and
  cancellation gates, split manifests, split-operator runtime evidence, and execution certificates.
- `MemoryPoolPlan` already admits or denies reservations before process OOM and reports pressure.
- `BackpressurePlanReport` and `DynamicWorkShapingReport` define advisory bounded-memory and
  feedback surfaces.
- Python already exposes a normal context/query-builder surface and lower-level prepared/batch
  helpers without requiring Vortex concepts for the common path.

The gap is that these surfaces are not yet one applied runtime loop. Existing scheduling in the
prepared/local path still largely uses deterministic chunks sized by `max_parallelism`. Dynamic
work shaping remains advisory or narrowly scoped, and resource scarcity is not yet a first-class
decision object attached to execution evidence.

## Goals

- Make `auto` more effective without new user inputs.
- Reduce avoidable stalls, excess in-flight work, tiny-task overhead, memory pressure, and
  unnecessary materialization/decode risk.
- Keep policy deterministic, replayable, certificate-backed, and explainable.
- Reuse existing ShardLoom resource, memory, backpressure, evidence-level, and no-fallback
  vocabulary.
- Apply first only to the prepared/local Vortex route where evidence and correctness gates already
  exist.
- Preserve existing result semantics and evidence contracts.
- Emit enough fields that benchmark artifacts can prove whether PulseWeave changed behavior and
  whether the change improved runtime metrics.

## Non-Goals

- No AI, model calls, learned policies, or Bayesian runtime decisions.
- No persistent cross-run tuning database.
- No hidden global state, daemon, service, or background maintenance loop.
- No live object-store provider runtime, distributed runtime, live/hybrid runtime, Foundry runtime,
  or remote worker promotion in the first slice.
- No Spark, DataFusion, DuckDB, Polars, pandas, Velox, Dask, Ray, or Vortex query-engine fallback.
- No public performance, superiority, production, or Spark-displacement claim from the first slice.
- No spill implementation for real query data unless a separate spill item authorizes it.
- No hidden fast mode that bypasses evidence levels, certificates, replay, or no-fallback fields.

## First Runtime Scope

The first implementation should target only:

```text
compatibility input -> certified Vortex preparation -> prepared_vortex local batch execution
existing native .vortex input -> prepared/native local batch execution
```

Concrete initial surfaces:

- `traditional-analytics-prepare-batch-run`
- `traditional-analytics-vortex-batch-run`
- prepared/local benchmark lane rows that already carry `prepared_vortex_scale_*` evidence
- Python helpers that call those routes, without changing their signatures

Do not apply PulseWeave to:

- direct compatibility transient paths
- object-store reads
- distributed worker scheduling
- live/hybrid state
- effectful adapters, UDFs, external APIs, LLM calls, embeddings, or plugins
- broad SQL/DataFrame paths that lack matching execution evidence

## Architecture

PulseWeave has four implementation layers. They should be implemented as pure deterministic policy
first, then wired into the prepared/local runner.

```text
task/source evidence
  -> FlowInventory Scheduler
  -> ScarcityLedger Allocator
  -> EndoPulse Governor
  -> ProofBound Auto Gate
  -> prepared/local execution certificate and evidence fields
```

### FlowInventory Scheduler

FlowInventory is the Kanban-style layer. It controls the amount of unfinished work in the engine.

Inputs:

```text
task_id
task_label
operator_memory_class
estimated_memory_bytes
estimated_rows
source_ref
split_ref
can_reorder
can_split
can_coalesce
requires_full_materialization
downstream_sink_class
```

Core policy:

- Maintain explicit queues: `ready`, `in_flight`, `held_for_memory`, `held_for_downstream`,
  `completed`, `failed`.
- Admit a task only when both the WIP slot limit and memory reservation gate allow it.
- Use `max_parallelism` as a hard cap, not necessarily the target in-flight count.
- Compute `wip_limit` as the minimum of:
  - requested or auto-derived `max_parallelism`;
  - memory-safe in-flight count from budget and estimated task bytes;
  - downstream sink in-flight limit when a result sink is requested;
  - one or more deterministic safety floors.
- Preserve task identity and batch identity so retry, replay, cancellation, and certificate fields
  remain stable.
- If the policy cannot classify a task, hold or block with a deterministic diagnostic rather than
  falling back.

Initial behavior should be conservative: if FlowInventory cannot prove a different shape is safe,
it emits evidence and preserves the existing chunked scheduler behavior.

### ScarcityLedger Allocator

ScarcityLedger is the electricity-market-style layer. It converts local resource pressure into a
deterministic decision record.

Resource prices should be integer scores, not floating-point learned values:

```text
memory_price_bps
queue_price_bps
decode_price_bps
sink_price_bps
spill_price_bps
total_scarcity_price_bps
```

Recommended first scoring:

```text
memory_pressure normal    -> memory_price_bps=0
memory_pressure elevated  -> memory_price_bps=2500
memory_pressure high      -> memory_price_bps=6000
memory_pressure critical  -> memory_price_bps=9000
memory_pressure exhausted -> memory_price_bps=10000
```

Additional prices:

- `queue_price_bps`: proportional to peak in-flight tasks divided by `wip_limit`.
- `decode_price_bps`: non-zero when the route requires decode/materialization or loses
  zero-decode eligibility.
- `sink_price_bps`: non-zero when a result sink is requested and sink buffering can become the
  limiting resource.
- `spill_price_bps`: high when a task may require real spill and real query-data spill remains
  unsupported.

Decision vocabulary:

```text
keep_current_shape
split_large_task
coalesce_small_tasks
hold_for_memory
hold_for_downstream
fail_before_oom
blocked_by_missing_estimate
blocked_by_unsupported_spill
blocked_by_policy
```

The ledger must always explain why the action was selected and must produce a stable digest over
the decision inputs.

### EndoPulse Governor

EndoPulse is the endocrine-style slow control loop. It reacts to observed pressure after a completed
batch/window and changes only the next local window. It does not persist policy across process
runs.

Observed signals:

```text
memory_pressure_before
memory_pressure_after
memory_reservations_requested
memory_reservations_granted
memory_reservations_denied
peak_memory_bytes
queue_limit
peak_in_flight_tasks
completed_task_count
held_task_count
small_task_count
large_task_count
sink_pressure_detected
spill_blocker_detected
```

Initial adjustment policy:

- If any reservation is denied, halve `target_task_bytes` down to the existing minimum.
- If pressure is high or critical without denial, reduce `target_task_bytes` by one step.
- If many tiny tasks complete with low pressure, double `target_task_bytes` up to the existing
  maximum.
- If sink pressure is detected, reduce in-flight tasks before changing task bytes.
- If spill is required but unsupported, stop before OOM and emit a blocker.
- Never exceed user-provided `memory_gb` or `max_parallelism` caps.

Hysteresis:

- The first slice should use one-window adjustment only inside the command.
- A later slice may require two consecutive same-direction windows before applying adjustment.
- Any future hysteresis must remain deterministic and local to the run unless a separate session
  policy explicitly authorizes persistence.

### ProofBound Auto Gate

ProofBound controls whether the automatic policy can apply.

Pre-application requirements:

- route is prepared/local Vortex or native/local Vortex;
- fallback is disabled by policy;
- task estimates are present or the route has a deterministic preserve-existing-shape path;
- memory budget and max parallelism are known;
- effectful external work is not involved;
- real query-data spill is not required unless spill support has been separately admitted;
- materialization/decode boundary is explicit.

Post-application requirements:

- output correctness digest is unchanged from the admitted semantics;
- runtime execution certificate is present and certified when the row is claimable;
- Native I/O certificate status remains present and certified where required;
- materialization/decode evidence remains explicit;
- `fallback_attempted=false`;
- `external_engine_invoked=false`;
- PulseWeave fields say whether policy was applied, reported only, or blocked.

If any precondition fails, the row should set:

```text
pulseweave_runtime_decision_applied=false
pulseweave_status=blocked
pulseweave_blocker=<stable blocker id>
```

and continue only through an already admitted non-PulseWeave ShardLoom-native path.

## New Evidence Contract

Every PulseWeave-capable row should expose this aggregate surface, using `not_applicable` when the
route is outside scope:

```text
pulseweave_schema_version
pulseweave_status
pulseweave_application_scope
pulseweave_runtime_decision_applied
pulseweave_policy_mutated
pulseweave_decision_digest
pulseweave_blocker
pulseweave_claim_gate_status
pulseweave_fallback_attempted=false
pulseweave_external_engine_invoked=false
native_io_certificate_status=certified
```

FlowInventory fields:

```text
flow_inventory_schema_version
flow_inventory_wip_limit
flow_inventory_peak_in_flight
flow_inventory_ready_task_count
flow_inventory_held_for_memory_count
flow_inventory_held_for_downstream_count
flow_inventory_completed_task_count
flow_inventory_failed_task_count
flow_inventory_backpressure_event_count
flow_inventory_existing_scheduler_preserved
```

ScarcityLedger fields:

```text
scarcity_ledger_schema_version
scarcity_ledger_memory_price_bps
scarcity_ledger_queue_price_bps
scarcity_ledger_decode_price_bps
scarcity_ledger_sink_price_bps
scarcity_ledger_spill_price_bps
scarcity_ledger_total_price_bps
scarcity_ledger_selected_action
scarcity_ledger_decision_reason
scarcity_ledger_decision_digest
```

EndoPulse fields:

```text
endopulse_schema_version
endopulse_signal_set
endopulse_previous_target_task_bytes
endopulse_next_target_task_bytes
endopulse_previous_wip_limit
endopulse_next_wip_limit
endopulse_adjustment_applied
endopulse_hysteresis_state
endopulse_persistent_state_used=false
```

ProofBound fields:

```text
proofbound_schema_version
proofbound_pre_application_status
proofbound_post_application_status
proofbound_required_evidence
proofbound_missing_evidence
proofbound_certificate_status
proofbound_no_fallback_status
proofbound_claim_allowed
```

Field values must be stable strings or integers suitable for benchmark artifacts, release gates,
Python typed reports, website status rendering, and runtime envelope validation.

## Rust API

The first implementation adds a provider-neutral module in `shardloom-exec`:

```text
shardloom-exec/src/pulseweave.rs
```

Implemented types:

```rust
pub struct PulseWeaveInput { ... }
pub struct PulseWeaveTaskShape { ... }
pub struct FlowInventoryReport { ... }
pub struct ScarcityLedgerDecision { ... }
pub struct EndoPulseDecision { ... }
pub struct ProofBoundAutoGate { ... }
pub struct PulseWeaveReport { ... }
```

Implemented pure functions:

```rust
pub fn plan_flow_inventory(input: &PulseWeaveInput) -> Result<FlowInventoryReport>;
pub fn compute_scarcity_ledger(input: &PulseWeaveInput, flow: &FlowInventoryReport)
    -> ScarcityLedgerDecision;
pub fn plan_endopulse_adjustment(
    input: &PulseWeaveInput,
    flow: &FlowInventoryReport,
    ledger: &ScarcityLedgerDecision,
) -> EndoPulseDecision;
pub fn evaluate_proofbound_auto_gate(input: &PulseWeaveInput) -> ProofBoundAutoGate;
pub fn plan_pulseweave(input: PulseWeaveInput) -> Result<PulseWeaveReport>;
```

The functions must be side-effect-free. They should not read files, execute tasks, probe providers,
call external systems, write outputs, mutate global state, or invoke fallback engines.

## Prepared/Local Integration

The pure policy module is wired into these prepared/local areas:

```text
shardloom-vortex/src/traditional_analytics.rs
```

Integration points:

- `TraditionalPreparedVortexLocalSplitRuntimeEvidence::build`
- `TraditionalPreparedVortexLocalScaleEvidence::build`
- helper that builds `traditional_prepared_vortex_scale_tasks`
- field emission for `prepared_vortex_scale_*` and `prepare_batch_scale_*` row prefixes

The implementation replaces direct `tasks.chunks(max_parallelism)` scheduling with a helper that
asks PulseWeave for a bounded local batch plan. When PulseWeave reports
`pulseweave_runtime_decision_applied=false`, that helper must preserve existing behavior and emit
blocked/report-only evidence rather than changing runtime behavior silently.

Suggested helper names:

```rust
fn plan_prepared_vortex_pulseweave_batches(...) -> Result<PreparedVortexPulseWeavePlan>;
fn execute_prepared_vortex_pulseweave_batches(...) -> Result<PreparedVortexPulseWeaveEvidence>;
```

The first slice may keep execution sequential while changing batch admission and evidence. A later
slice can add true bounded parallel execution only when certificate, retry, cancellation, and memory
evidence are ready.

## Python Surface

Python should not gain a new required parameter.

Initial Python work should be only:

- preserve new fields in `OutputEnvelope`;
- expose optional typed summary helpers after the field names stabilize;
- document that `memory_gb` and `max_parallelism` are caps, not required tuning knobs;
- keep unsupported/report-only behavior deterministic.

Suggested later typed helper:

```python
result.evidence_summary.pulseweave
```

or, if the existing typed report pattern fits better:

```python
result.pulseweave.runtime_decision_applied
result.pulseweave.selected_action
result.pulseweave.claim_gate_status
```

Do not add a `pulseweave=True` user option. The point is automatic backend behavior under `auto`
with transparent evidence.

## Benchmark Plan

PulseWeave must not be claimed as faster until benchmark evidence exists.

Required first benchmark rows:

- same dataset profile;
- same scenarios;
- same source format;
- same evidence level;
- same hardware/runtime profile;
- existing auto scheduler row;
- PulseWeave auto scheduler row;
- correctness digest comparison;
- no-fallback and no-external-engine fields.

Minimum metrics:

```text
total_runtime_millis
scenario_compute_millis
memory_reservations_requested
memory_reservations_denied
peak_memory_bytes
runtime_queue_limit
runtime_backpressure_bounded
flow_inventory_peak_in_flight
scarcity_ledger_selected_action
endopulse_adjustment_applied
rows_scanned
rows_materialized
bytes_read
data_decoded
data_materialized
spill_io_performed
fallback_attempted
external_engine_invoked
correctness_digest
```

Benchmark interpretation:

- A faster row without matching correctness and evidence is not claimable.
- A memory-stable row without speedup is still useful if it reduces denied reservations or avoids
  OOM risk.
- A PulseWeave row must not hide preparation timing, evidence rendering cost, result-sink cost, or
  compatibility import cost.

## Implementation Slices

Slices 1-4 are implemented by `GAR-RUNTIME-IMPL-5R` for the prepared/local batch route. They remain
here as the design-to-code trace, not as a live unchecked queue.

### Implemented Slice 1 - Policy And Evidence Contract

Outcome:

- Add `shardloom-exec/src/pulseweave.rs`.
- Export the module from `shardloom-exec/src/lib.rs`.
- Add pure unit tests for FlowInventory, ScarcityLedger, EndoPulse, and ProofBound decisions.
- Add contract-test coverage for side-effect-free planning and no-fallback fields.

Acceptance:

- policy functions are deterministic and side-effect-free;
- no external provider, file, network, spill, or execution behavior occurs;
- invalid input produces deterministic diagnostics;
- field names and status values are stable.

Verification:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-exec pulseweave --lib
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test dynamic_work_shaping
git diff --check
```

### Implemented Slice 2 - Prepared/Local Evidence Wiring

Outcome:

- Convert prepared/local task shape into `PulseWeaveInput`.
- Emit PulseWeave, FlowInventory, ScarcityLedger, EndoPulse, and ProofBound fields on
  prepared/local rows.
- Preserve existing runtime behavior when policy application is blocked.

Acceptance:

- current prepared/native local tests still pass;
- new fields appear under both scenario and `prepare_batch_*` prefixes where appropriate;
- existing scheduler evidence remains present;
- no row reports fallback or external engine invocation.

Verification:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-vortex --features vortex-traditional-analytics-benchmark prepared_batch_run_emits_real_byte_local_scale_evidence_in_vortex_route --lib
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-vortex --features vortex-traditional-analytics-benchmark prepared_batch_run_certifies_stateless_split_operator_for_sequence_selective_filter --lib
python -m pytest python/tests/test_cli_client.py -k "traditional_analytics_prepare_batch_run or runtime_execution_field_validation"
git diff --check
```

### Implemented Slice 3 - Runtime Application For Local Prepared Batches

Outcome:

- Enable PulseWeave to choose bounded batch windows for local prepared tasks when ProofBound admits
  the route.
- Apply EndoPulse adjustments within a single command only.
- Keep `memory_gb` and `max_parallelism` as hard caps.

Acceptance:

- local prepared run can show `pulseweave_runtime_decision_applied=true` only when required evidence
  exists;
- applied policy has a decision digest and matching certificate/evidence refs;
- denied memory reservations trigger smaller next-window shape or fail-before-OOM behavior;
- tiny independent tasks can coalesce only when semantic ordering/result rules allow it.

Verification:

```powershell
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-vortex --features vortex-traditional-analytics-benchmark prepared_batch --lib
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python -m compileall -q python/src python/tests scripts examples
git diff --check
```

### Implemented Slice 4 - Python And Benchmark Ergonomics

Outcome:

- Preserve PulseWeave fields through Python typed envelopes.
- Add optional summary accessors only after field names are stable.
- Add benchmark artifact validation so a PulseWeave row cannot be promoted without correctness,
  no-fallback, and evidence fields.

Acceptance:

- ordinary Python usage remains unchanged;
- users can inspect the selected action and whether PulseWeave applied without scraping raw maps;
- benchmark artifacts fail validation if PulseWeave rows omit correctness, certificate, or
  no-fallback fields.

Verification:

```powershell
python -m pytest python/tests/test_cli_client.py -k "pulseweave or evidence_summary or traditional_analytics"
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python scripts\check_runtime_execution_envelopes.py
git diff --check
```

### Slice 5 - Benchmark Evidence Refresh

Outcome:

- Add an explicit benchmark profile or lane for existing auto vs PulseWeave auto on admitted
  prepared/local scenarios.
- Publish only workload-scoped evidence, not broad claims.

Acceptance:

- artifact records the same source data, scenario list, source format, and evidence level for both
  rows;
- correctness digests match;
- PulseWeave deltas are visible for runtime, memory, queue, and work-shaping fields;
- claim gate remains workload-scoped and evidence-backed.

Verification:

```powershell
python benchmarks\traditional_analytics\run.py --profile tiny_smoke --engines shardloom-prepare-batch --iterations 2
$env:RUSTUP_TOOLCHAIN='1.91.1'; cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python scripts\check_website_readiness.py
git diff --check
```

## Failure Behavior

PulseWeave must fail closed:

- Missing memory budget: `pulseweave_status=blocked`,
  `proofbound_pre_application_status=blocked_missing_memory_budget`.
- Missing task estimate: `pulseweave_status=blocked`,
  `proofbound_pre_application_status=blocked_missing_task_estimate`.
- Unsupported spill requirement: `pulseweave_status=blocked`,
  `proofbound_pre_application_status=blocked_unsupported_spill`.
- External effect present: `pulseweave_status=report_only_blocked`,
  `proofbound_pre_application_status=blocked_fallback_policy`.
- Fallback attempted or allowed: validation failure.
- Missing certificate on claimable row: validation failure.

If a policy is blocked but the non-PulseWeave route is otherwise admitted, the engine may execute
the existing ShardLoom-native route and report `pulseweave_runtime_decision_applied=false`. It must
not call an external fallback engine.

## Claim Boundary

After Slice 1:

- ShardLoom can claim only that PulseWeave policy planning exists and is side-effect-free.

After Slice 2:

- ShardLoom can claim only that prepared/local rows expose PulseWeave readiness evidence.

After Slice 3:

- ShardLoom can claim only that PulseWeave applied to scoped prepared/local workloads with
  certificate-backed evidence.

After Slice 5:

- ShardLoom can make only workload-scoped benchmark statements supported by the refreshed artifact.

No slice authorizes broad SQL/DataFrame, object-store, distributed, production, or
Spark-displacement claims.

## Risks

- A control loop can obscure performance attribution if fields do not show selected action,
  decision digest, and stage timing.
- Coalescing can change memory pressure or result ordering if task boundaries are not semantic.
- Splitting can increase overhead on small jobs.
- Existing benchmark rows may be too small to show meaningful speed deltas.
- Public wording can overstate the mechanism before benchmark evidence exists.

## Open Questions

- Should the first applied policy only alter batch admission, or also alter `target_partition_bytes`
  inside the prepared/local command?
- Should PulseWeave share code with the existing dynamic work shaping report immediately, or should
  it land as a separate `shardloom-exec` module and then converge?
- Should benchmark artifacts use a separate `pulseweave_auto` lane or a field on the existing
  ShardLoom prepared/batch lane?
- Which Python typed-report family should own the eventual summary helper?
