# Compute Engine Flow Repo Alignment Review

Status: historical alignment review.

## Purpose

This review compared `docs/architecture/compute-engine-flow-reference.md` against the then-current
ShardLoom repository. It records where the flow was represented, where it remained scoped to
benchmark-local or report-only surfaces, and how the P7.5 overhaul steps closed the original
alignment gaps for that release branch.

This file is historical only. Current compute-flow vocabulary lives in
`docs/architecture/compute-engine-flow-reference.md`; active implementation and cleanup routing
lives in `docs/architecture/phased-execution-plan.md`.

This document does not authorize new runtime behavior, package publication, external engine
fallback, public performance claims, or managed-platform benchmark lanes.

## Inputs Reviewed

Primary flow and benchmark docs:

```text
docs/architecture/compute-engine-flow-reference.md
docs/architecture/performance-attribution-and-execution-structure.md
docs/architecture/benchmark-suite-catalog.md
docs/architecture/phased-execution-plan.md
benchmarks/traditional_analytics/README.md
```

Primary implementation surfaces:

```text
shardloom-core/src/output.rs
shardloom-cli/src/benchmark_runtime.rs
shardloom-cli/src/typed_envelope.rs
shardloom-vortex/src/traditional_analytics.rs
shardloom-vortex/src/source_backed_encoded_execution.rs
shardloom-vortex/src/vortex_scan_compatibility.rs
shardloom-plan/src/execution_facade.rs
shardloom-exec/src/lib.rs
python/src/shardloom/client.py
python/src/shardloom/context.py
benchmarks/traditional_analytics/run.py
shardloom-contract-tests/tests/traditional_benchmark_harness.rs
```

## Bottom Line

The flow reference is aligned with the current compute-engine vocabulary and benchmark/reporting
surfaces. The repo now has concrete coverage for:

```text
explicit execution-mode names
benchmark row mode fields
prepared-vortex benchmark rows under requested source-format rows
compatibility-import stage attribution
Native I/O and execution certificate evidence for current certified workflows
no-fallback and no-external-engine fields
materialization/decode evidence for current paths
```

The important boundary is scope: this is a credible local Vortex analytics compute-engine slice,
not a broad all-workloads product claim. Execution-mode selection, prepared artifact lifecycle,
typed envelope routing, capability reporting, provider admission, result-sink replay, timing
attribution, Python parity, and file-format preparation evidence are implemented for the current
traditional analytics/local Vortex surfaces. Broad SQL/DataFrame runtime, direct transient runtime,
object-store/table/catalog runtime, live/hybrid production behavior, and public best-default claims
remain outside this completed slice.

## Aligned Surfaces

### Execution-mode vocabulary exists

`shardloom-core/src/output.rs` defines:

```text
auto
compatibility_import_certified
prepared_vortex
direct_compatibility_transient
native_vortex
```

The traditional benchmark CLI and harness now emit `requested_execution_mode`,
`selected_execution_mode`, `mode_selection_reason`, `execution_mode_family`,
`vortex_native_claim_allowed`, `compatibility_import_included`, `vortex_prepare_included`,
`vortex_write_reopen_included`, `direct_transient_execution`, `fallback_attempted`,
`external_engine_invoked`, and `claim_gate_status`.

### Compatibility-import timing is no longer hidden

`benchmarks/traditional_analytics/run.py` and
`shardloom-vortex/src/traditional_analytics.rs` expose separate fields for source read, compatibility
parse/import, Vortex write/reopen/scan, operator compute, result-sink write, evidence rendering,
startup/warmup, and preparation timing.

### Prepared-vortex benchmark lane exists

The harness prepares Vortex artifacts once per dataset/profile/format and reports
`shardloom-vortex` and `shardloom-prepared-vortex` rows under the requested CSV/JSONL/Parquet/Arrow
IPC/Avro/ORC source-format rows rather than under standalone `.vortex` rows.

This matches the user-facing benchmark requirement: prepared/native timing stays attached to the
source-format comparison while preparation refs and digests record the Vortex boundary.

### Native I/O and materialization evidence exists for current certified workflows

`shardloom-vortex/src/traditional_analytics.rs` emits source capability, pushdown, sink
requirement, adapter fidelity, materialization boundary, representation transition, result-sink, and
no-fallback fields for the current compatibility and native Vortex benchmark paths.

### Source-backed encoded execution exists as a richer provider foundation

`shardloom-vortex/src/source_backed_encoded_execution.rs` already has source-backed and
reader-backed encoded filter/projection evidence, residual boundary reporting, split refs,
certificate-pair reports, and deterministic blockers. This is the right substrate for future
prepared/native provider admission.

## Original Gaps And Completion Status

The following gaps were identified at P7.5.0 intake. They remain listed for auditability, but the
completion status under each item is authoritative for the current branch.

### G1 - Execution-mode selection is not a shared admission layer

The flow reference says:

```text
policy + capability admission -> explicit execution mode -> provider admission
```

Current repo state:

- `ShardLoomExecutionMode` is a stable enum.
- `traditional-analytics-run` allows only `auto` and `compatibility_import_certified`.
- `traditional-analytics-vortex-run` allows only `auto`, `native_vortex`, and `prepared_vortex`.
- The shared `ShardLoomExecutionModeSelectionReport` now centralizes the current selection facts for
  traditional compatibility and native/prepared Vortex runners.
- The benchmark harness carries those selection facts into rows instead of relying on anonymous
  mode strings.

Original gap:

There is no shared `ExecutionModeSelectionReport` or reusable admission function that combines
input format, requested mode, certification requirements, workload constitution, capability rows,
and provider availability into a deterministic selected mode.

Original impact:

`auto` is transparent at the row level, but not yet a real cross-surface selection service. Future
CLI, Python, and REST surfaces could drift or duplicate the benchmark-specific selection rules.

Completion status:

Complete for the current local traditional analytics and prepared/native Vortex surfaces. Future
non-benchmark surfaces must reuse the same report contract before they claim mode parity.

### G2 - Prepared Vortex is a benchmark harness workflow, not a reusable artifact lifecycle

Current repo state:

- The Python harness prepares Vortex artifacts before scenario timing.
- The prepared artifact refs, digests, workspace, cleanup policy, lifecycle status, and reuse
  eligibility are copied into result rows.
- `traditional-analytics-vortex-run` executes from supplied `.vortex` paths.
- Python exposes prepared-artifact helpers for the current local workflow.

Original gap:

There is no general ShardLoom `prepare`/`register prepared artifact` command or Python API that
creates, validates, caches, reuses, expires, and cleans up prepared Vortex artifacts outside the
traditional benchmark harness.

Original impact:

`prepared_vortex` is the right performance lane, but it is not yet a first-class user workflow or
engine service.

Completion status:

Complete for local traditional analytics artifacts with caller-owned cleanup. Broader lifecycle
features such as cross-workload caching, expiry, and object-store artifact management remain future
product work.

### G3 - Typed envelopes still carry most execution-mode evidence as flat fields

Current repo state:

- `OutputEnvelope` has typed result, artifact, certificate, policy, lifecycle, and capability slots.
- `typed_envelope.rs` routes several local primitive Native I/O and execution certificates into
typed artifacts/refs.
- CLI typed envelopes now attach inline `execution_mode_selection_report` and
  `compute_flow_evidence` artifacts when execution-mode fields are present, while preserving flat
  fields for compatibility.

Original gap:

Mode selection, prepared artifact refs, materialization/decode boundaries, result-sink refs, source
refs, and benchmark evidence refs are not consistently promoted into typed envelope slots.

Original impact:

Python and future REST clients can read the fields, but they do not yet get a stable typed protocol
for the flow reference's `result/result sink -> certificates + evidence -> claim gate` layer.

Completion status:

Complete for current CLI/Python typed-envelope parsing. Future REST surfaces should expose the same
typed artifacts rather than inventing a parallel mode/evidence schema.

### G4 - Capability discovery is not execution-mode aware

Current repo state:

- `ComputeCapabilityMatrix` rows include support status, engine mode, provider kind, semantic
  profile, materialization/decode requirement, memory/spill requirement, correctness refs, benchmark
  refs, execution certificate refs, Native I/O refs, unsupported diagnostic code, blocker id, future
  evidence, `fallback_attempted`, and `external_engine_invoked`.
- Rows now include execution-mode vocabulary, mode-specific claim-gate status, and
  `vortex_native_claim_allowed` so direct transient, compatibility, prepared, native, and auto rows
  are distinguishable.

Original gap:

Rows do not distinguish the flow-reference modes:

```text
compatibility_import_certified
prepared_vortex
native_vortex
direct_compatibility_transient
auto
```

Original impact:

Users can ask "what is supported?" but cannot cleanly ask "what is supported in prepared_vortex
versus native_vortex versus compatibility_import_certified?"

Completion status:

Complete for the report-only capability matrix and Python/CLI discovery views.

### G5 - Native Vortex query rows still rely on temporary materialized operators

Current repo state:

- Traditional native/prepared rows start from `.vortex` artifacts.
- The current benchmark evidence says the operator path streams projected Vortex chunks but still
  decodes required scalar columns and materializes Vortex-derived arrays for current scenarios.
- Source-backed encoded filter/projection reports exist, but the traditional scenario provider path
  does not yet select them as the main provider spine.

Original gap:

The flow reference's intended native target:

```text
Vortex Source / Scan / Split -> pushdown -> encoded/native operator -> result/evidence
```

is not yet the default prepared/native scenario execution path.

Original impact:

Prepared/native rows are structurally better than repeated compatibility import, but they remain
fixture/local smoke or partial native rows until provider admission moves more work onto encoded or
source-backed paths.

### G6 - Direct transient compatibility mode is parse-level only

Current repo state:

- The enum accepts `direct_compatibility_transient`.
- The flow reference correctly states it must not be Vortex-native.
- Traditional CLI commands reject unsupported mode requests deterministically.
- The mode-aware capability matrix emits direct-transient unsupported rows with stable blockers,
  `claim_gate_status=not_vortex_native`, and no-fallback evidence.

Original gap:

There is no report-only direct-transient capability surface that emits
`claim_gate_status=not_vortex_native`, `direct_transient_execution=true`, and the required blocker
or implementation gate for each user-facing source format and operator family.

Original impact:

The mode exists in terminology, but users cannot yet discover a complete no-fallback unsupported
parity view for it.

### G7 - Prepared/native result-sink replay proof is incomplete

Current repo state:

- `traditional-analytics-run --write-result-vortex` can write and replay a result sink for the
  certified compatibility-import workflow.
- `traditional-analytics-vortex-run` now exposes result-sink/replay proof fields for prepared/native
  query rows when result sinks are requested, while keeping sink timing separate from operator
  timing.

Original gap:

Prepared/native claim-grade rows need result Native I/O evidence when result sinks are requested,
but the result-sink proof is currently strongest on the compatibility-import workflow.

Original impact:

Prepared/native timing rows should remain fixture-smoke or not-claim-grade unless their result-sink
and replay evidence is explicitly present.

### G8 - Stage timing attribution is useful but still partially inferred

Current repo state:

- Timing fields exist for source read, parse/import, Vortex write/reopen/scan, operator compute,
  result sink, evidence render, startup/warmup, and total runtime.
- The harness records `persistent_runner_status=process_per_scenario_attributed_not_reduced`.
- The persistent-runner decision is explicit: keep the Python-driven per-scenario CLI runner for
  now and attribute process/Python overhead rather than weakening typed envelope/no-fallback proof.

Original gap:

Several timing fields are `null`, derived, or included in larger buckets rather than measured
independently. CLI process startup, Rust binary startup, Python harness overhead, engine warmup,
artifact preparation, and evidence rendering need a stricter timing contract.

Original impact:

The current report is honest, but deeper performance conclusions still need stronger attribution
before comparative claims are made.

### G9 - Python and future REST surfaces do not yet select modes

Current repo state:

- Python can request explicit execution modes for current local workflows, inspect selected mode and
  blockers, and read the same typed selection/evidence facts as CLI envelopes.
- Future REST surfaces are documented to preserve the same selection report contract.

Original gap:

Python does not yet expose a first-class execution-mode parameter or prepared-artifact lifecycle for
local workflow APIs. Future REST surfaces have the typed-envelope direction, but no mode-selection
contract that reuses a shared admission report.

Original impact:

The flow reference says CLI, Python, and future REST should agree. The current repo has read-side
parity for some fields, not full request/selection parity.

### G10 - File-format comparisons need a preparation-focused matrix

Current repo state:

- The harness can generate CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC fixture inputs.
- Prepared/native rows are reported under those source formats.
- The benchmark artifact emits `format_preparation_matrix` so format preparation/staging costs are
  visible without treating compatibility formats as native execution formats.

Original gap:

The suite should explicitly separate:

```text
compatibility parser cost
compatibility-to-Vortex import cost
Vortex write/reopen/scan cost
prepared/native query cost
result-sink cost
```

for each format. This should be a file-format preparation matrix, not a claim that CSV, Parquet,
Avro, or ORC are native execution formats.

Original impact:

The current structure is correct, but a dedicated preparation matrix will make future comparisons
harder to misread.

## Vortex-First Provider Check

- Subject area: execution-mode selection, prepared Vortex artifact lifecycle, source-backed encoded
  provider admission, Scan/source/sink timing, and benchmark claim gates.
- Upstream Vortex concept checked: arrays, encodings, source/sink/split, scan pushdown,
  representation transitions, file write/reopen/scan, and I/O timing concepts already represented
  in ShardLoom Vortex docs and source-backed reports.
- Decision:
  - `wrap_vortex_concept` for shared execution-mode selection and typed evidence routing.
  - `use_vortex_native_provider` for admitted prepared/native local Vortex provider paths with
    certificates and materialization/decode evidence.
  - `blocked_until_vortex_or_shardloom_evidence` for direct transient execution, unfused
    filter/project/limit, unsupported source-backed operators, and incomplete result-sink replay.
- Vortex API/provider surface: current local Vortex artifact write/reopen/scan path, source-backed
  encoded execution reports, and future Scan API Source/Sink/Split alignment.
- ShardLoom provider/report/certificate surface: `ExecutionModeSelectionReport`, mode-aware
  capability matrix rows, typed envelope slots, prepared artifact refs/digests, execution
  certificates, Native I/O certificates, materialization/decode boundaries, and claim gates.
- Residual handling: residuals must be ShardLoom-native or deterministically blocked; external
  engines remain baselines/oracles only. In short, external engines remain baselines/oracles only.
- Materialization/decode boundary: every promoted prepared/native row must say whether it stayed
  encoded/native, canonicalized, decoded, or materialized.
- Evidence added by this review: gap mapping and phase-plan overhaul steps only.
- Gates still blocked: direct transient runtime, broad SQL/DataFrame runtime, object-store/table
  runtime, broad performance claims, and production/Spark-displacement claims.
- `fallback_attempted=false`: remains mandatory for every ShardLoom mode.

## Recommended Overhaul Sequence

These steps are now tracked in `docs/architecture/phased-execution-plan.md` under Priority 7.5.

### P7.5.1 - Shared Execution-Mode Admission And Selection Report

Create a shared report that consumes requested mode, source format, workload constitution,
certification policy, result-sink policy, capability rows, and provider availability. It should emit:

Implementation status: complete for the current traditional compatibility and native/prepared
Vortex benchmark runners. The shared Rust report is `ShardLoomExecutionModeSelectionReport` in
`shardloom-core/src/output.rs`; follow-up slices still need to route it into richer typed envelope
artifact slots and mode-aware capability rows.

```text
requested_execution_mode
selected_execution_mode
mode_selection_reason
execution_mode_family
source_format
workload_constitution_id
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
vortex_native_claim_allowed
unsupported_diagnostic_code
blocker_id
required_future_evidence
claim_gate_status
claim_gate_reason
fallback_attempted=false
external_engine_invoked=false
```

Acceptance:

- CLI, Python, benchmark, and future REST surfaces use the same selection result.
- `auto` is a transparent selection request, not a special hidden mode.
- Unsupported requested modes return deterministic diagnostics.

### P7.5.2 - Typed Envelope Evidence Routing For Flow Fields

Promote execution-mode, prepared artifact, result sink, certificate, materialization/decode,
provider, lifecycle, and claim-gate facts from flat fields into typed envelope slots where possible.

Implementation status: complete for inline typed artifacts. CLI typed envelopes now attach
`execution_mode_selection_report` and `compute_flow_evidence` artifacts when execution-mode fields
are present. Missing compute-flow slots are represented as `evidence_incomplete`; follow-up slices
may still add narrower typed refs for prepared-artifact lifecycle and prepared/native result-sink
replay once those surfaces become reusable engine concepts.

Acceptance:

- Flat fields remain for compatibility, but typed slots become the preferred client surface.
- Prepared artifacts and source/result certificates appear as typed refs or inline artifacts.
- Missing evidence is represented explicitly as evidence-incomplete.

### P7.5.3 - Prepared Vortex Artifact Lifecycle

Make prepared Vortex artifacts a reusable engine concept rather than a benchmark-local setup step.

Implementation status: complete for local traditional analytics artifacts. Compatibility
ingest/stage reports emit prepared artifact refs, digests, workspace, lifecycle status, reuse
eligibility, source Native I/O status, and cleanup policy. Python exposes
`PreparedVortexArtifacts` with prepare/reuse helpers. Cleanup remains caller-owned and explicit.

Acceptance:

- Add CLI and Python surfaces to prepare, inspect, reuse, and clean up local prepared artifacts.
- Record artifact refs, digests, source Native I/O certificate status, preparation timing, and
  cleanup policy.
- Scenario timing can start from registered prepared artifacts without re-importing compatibility
  inputs.

### P7.5.4 - Mode-Aware Capability Matrix And Direct-Transient Unsupported Parity

Extend compute capability rows so support is reported by execution mode.

Implementation status: complete for the report-only compute capability matrix. Rows expose
`execution_mode`, `claim_gate_status`, and `vortex_native_claim_allowed`, and direct transient is an
explicit unsupported row with stable diagnostics and no-fallback evidence.

Acceptance:

- Capability rows distinguish `compatibility_import_certified`, `prepared_vortex`, `native_vortex`,
  `direct_compatibility_transient`, and `auto`.
- Direct transient has deterministic unsupported/report-only rows with `not_vortex_native` claim
  status until a ShardLoom-native transient path is actually implemented.
- No external engine can satisfy direct transient support.

### P7.5.5 - Native Provider Admission For Prepared/Native Operators

Move prepared/native scenarios toward source-backed encoded and Vortex-native provider paths where
evidence exists.

Implementation status: complete for current traditional analytics rows as explicit provider
admission evidence. The Vortex scan/source boundary is admitted; residual scenario operators remain
explicitly ShardLoom-native/materialized, and filter/project/limit fusion carries a blocker until a
true fused path exists.

Acceptance:

- Provider admission checks source-backed encoded filter/projection/count paths before temporary
  materialized operators.
- Fused filter/project/limit records actual fused execution or a stable blocker.
- Multi-key group by, join+aggregate, top-N per group, and row-number window either execute through
  admitted ShardLoom/Vortex providers or emit deterministic unsupported diagnostics.

### P7.5.6 - Prepared/Native Result-Sink Replay Proof

Add result-sink write/replay proof for prepared/native rows without mixing sink timing into pure
query timing unless requested.

Acceptance:

- Prepared/native rows can emit result Native I/O certificate refs when result sink is enabled.
- Result-sink timing is separate from operator timing.
- Claim-grade promotion remains blocked when result-sink evidence is required but missing.

### P7.5.7 - Benchmark Attribution And Persistent Runner Decision

Harden stage timing attribution and decide whether to reduce process overhead or continue reporting
it explicitly.

Acceptance:

- CLI process startup, binary startup, Python harness overhead, engine warmup, preparation, query,
  result sink, and evidence rendering are separated where feasible.
- If an in-process or batched runner is added, typed envelopes and no-fallback evidence remain
  intact.
- If no runner is added, process overhead remains a visible measured field.

### P7.5.8 - Python And Future REST Mode Parity

Expose execution-mode request and result concepts through Python and preserve the same contract for
future REST/API surfaces.

Acceptance:

- Python can request explicit mode or `auto` for supported local workflows.
- Python can inspect selected mode, reason, typed refs, evidence status, and blockers.
- Future REST docs reference the same mode-selection report and typed envelope fields.

### P7.5.9 - File-Format Preparation Matrix

Add a format-focused matrix that compares compatibility preparation and staging costs without
turning compatibility formats into native execution claims.

Acceptance:

- CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC rows separate parse/import/write/reopen/scan/query
  costs.
- Prepared/native query timing remains separate from format-preparation timing.
- The report states that Vortex is the native execution format.

## Historical Next Move At Completion

P7.5.1 through P7.5.9 are complete for the scoped local traditional analytics and Vortex workflow
surfaces. The remaining work is not another flow-alignment category; it is future product breadth:
broad SQL/DataFrame execution, direct transient runtime, object-store/table/catalog runtime,
live/hybrid production behavior, and wider encoded/native operator maturity.

The current benchmark and release evidence support a scoped local Vortex analytics claim. They do
not support public "best default in all scenarios", broad superiority, Spark-displacement, or
production claims. Those claims must remain gated on future workload-specific correctness,
benchmark, capability, release, and no-fallback evidence.
