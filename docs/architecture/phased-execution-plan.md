# ShardLoom Phased Execution Plan

## How to maintain this file
- Keep actionable working items in Planned.
- Keep Completed as a pointer to `docs/architecture/phased-execution-completed-ledger.md`; do not
  place detailed completed session blocks in this file.
- Keep Planned in logical implementation order even when CG or phase numbers are out of order.
- Do not keep a separate Active section; the next autonomous work should be the next unchecked
  Planned checklist item after the queue has been ordered by current dependency and user value.
  If the top item no longer matches the current implementation priority, reorder Planned first.
- Move completed session blocks to the top of
  `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion; do not
  reshuffle older completed history unless the content is incorrect.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Planned Item Detail Standard

Every unchecked Planned item must be detailed enough for an autonomous Codex session to execute
without guessing.

A Planned item is sufficiently detailed only if it names:

- Source: governing RFC, architecture doc, benchmark report, issue, PR, or review finding.
- Current state: what exists today and what is still unsupported/report-only.
- Next slice outcome: the exact result expected from the next PR/session.
- User-visible surface: CLI, Python, benchmark, docs, API, capability view, evidence artifact, or
  release gate.
- Implementation scope: files/modules/commands expected to change.
- Evidence required: correctness, benchmark, execution-certificate, Native I/O,
  materialization/decode, policy, no-fallback, release/security evidence as applicable.
- Acceptance: observable conditions that make the item done.
- Verification: exact commands/tests/snapshots expected.
- Non-goals: what must not be implemented in this slice.
- Claim boundary: what can and cannot be claimed after completion.
- Fallback boundary: expected `fallback_attempted=false` and `external_engine_invoked=false`
  behavior.
- Ledger rule: when complete, move the detailed completed session to
  `docs/architecture/phased-execution-completed-ledger.md`.

Do not leave planned work as a bare statement such as "`<thing>` remains incomplete." Convert broad
items into one or more evidence-bearing implementation slices. Split a Planned item when it includes
`full`, `broad`, `general`, `production`, `universal`, `distributed`, `runtime`, `platform`,
`lakehouse`, `object-store`, `SQL/DataFrame`, `claim`, `release`, `Foundry`, or `REST` without an
immediate concrete scope. A split item should use child IDs such as `GAR-0032-A`; each child must be
implementable in one focused PR or explicitly marked `report-only`, `planning-only`, or
`diagnostic-only`.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests/snapshots/release checks exist, evidence refs are attached where claims are made,
unsupported paths remain explicit, no fallback engine was invoked, completed details are moved to the
completed ledger, and supporting docs are updated without becoming a second active queue.

No item may create or imply a public claim unless it explicitly lists the evidence that supports the
claim. Performance, superiority, Spark-displacement, production, SQL/DataFrame, object-store,
Foundry, REST, live/hybrid, and package-release claims require workload-scoped evidence and release
gates. If evidence is missing, the item must say `claim_gate_status=not_claim_grade` or
`support_status=unsupported|blocked|report_only`.

Status reading order:
1. Planned: next work in logical implementation order.
2. Completed ledger: recently finished sessions first, then historical provenance ledgers in
   `docs/architecture/phased-execution-completed-ledger.md`.
3. Competitive Engine Gate detailed checklists: attribution detail only; promote new actionable work
   into Planned before implementation.

## Architecture Document Ownership
- This file is the mutable source of truth for planned sequence, deferred work, and CG closeout
  ordering.
- `docs/architecture/phased-execution-completed-ledger.md` is the mutable source of truth for
  detailed completed session and historical phase ledgers.
- Supporting architecture docs may contain rationale, inventories, traceability, and historical
  ledgers, but they must not introduce a second "current" queue.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- If a supporting doc records completed history, keep it clearly labeled as a completed ledger or
  historical note, and do not let it become a current queue.
- Supporting docs must not keep unchecked implementation checklists outside this file and
  `docs/architecture/global-architecture-review.md`. Scope-boundary lists may remain, but real work
  must be carried by a `GAR-*` item below.

Supporting docs:
- `README.md`
  - Role: project entry point, stable orientation, and compact core-concepts doorway.
  - Status rule: points to this phase plan and the completed ledger for current planned/completed
    state; must not duplicate working checklists or become the full glossary.
- `docs/architecture/phased-execution-completed-ledger.md`
  - Role: detailed completed session ledger and historical phase provenance split out of this phase
    plan.
  - Status rule: may record completed work only; it must not introduce planned work or a second
    current queue.
- `docs/architecture/rfc-phase-traceability.md`
  - Role: maps phases and CG work to governing RFCs.
  - Status rule: may record traceability history, but this file owns current work state.
- `docs/architecture/global-architecture-review.md`
  - Role: checkbox audit of every RFC plus the compute-engine flow against the repo.
  - Status rule: every unchecked item in that review must be mirrored into this Planned queue before
    implementation; checking a review item requires checking off the corresponding phase-plan item
    or moving the completed session to the ledger.
- `docs/architecture/compute-engine-flow-reference.md`
  - Role: canonical end-to-end flow for users, CLI/Python/REST access, adapters, I/O, execution
    modes, sinks, downstream consumers, evidence, and claim gates.
  - Status rule: planned nodes in the flow do not authorize implementation or claims until the
    corresponding item exists in this Planned queue and is completed with evidence.
- `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `benchmark-persistent-runner-decision.md`, and
  `performance-attribution-and-execution-structure.md`
  - Role: historical P7.5 repo-alignment review, persistent-runner decision, and benchmark timing
    attribution reference.
  - Status rule: these files record completed alignment decisions only; current compute-flow follow-up
    is represented by the GAR flow items in this Planned queue.
- `docs/architecture/runtime-evidence-level-tiering.md`
  - Role: GAR-PERF-2A reference for evidence-level runtime tiering across
    `minimal_runtime`, `certified`, and `full_replay`.
  - Status rule: the scoped `traditional-analytics-vortex-batch-run` path now emits first-class
    evidence-level fields. Future Python/API capability views and broader execution envelopes must
    remain represented by later evidence-bearing slices before broader support can be claimed.
- `docs/architecture/evidence-aware-logical-optimizer.md`
  - Role: GAR-PERF-2B reference for the completed report-only optimizer rule registry and optimizer
    trace contract.
  - Status rule: records current rule families, CLI/Python/benchmark trace fields, report-only
    before/after plan-digest placeholders, and no-fallback/claim boundaries. Future runtime
    rewrites, correctness smoke, and claim-grade optimizer promotion must remain represented by
    later evidence-bearing slices.
- `docs/architecture/vortex-scan-pushdown-completion.md`
  - Role: report-only GAR-PERF-2C reference for Vortex Scan API filter/projection/limit pushdown
    completion across prepared/native scenario families.
  - Status rule: defines pushdown evidence and deterministic blocker requirements only. Scan
    request builder work, filter expression lowering, projection mask computation, limit/slice
    pushdown, capability matrix projection, and benchmark row schema changes must remain
    represented by `GAR-PERF-2C` or later evidence-bearing slices.
- `docs/architecture/compressed-encoded-kernel-registry.md`
  - Role: GAR-PERF-2D reference for scoped compressed/encoded kernel registry evidence over
    selective-filter prepared/native rows.
  - Status rule: scoped bitpacked and sequence reader-generated encoded inputs now emit registry
    evidence with deterministic blockers for the remaining initial pairs. Broader encoded-native
    operator coverage, capability-matrix promotion, and claim-grade use must remain represented by
    later evidence-bearing slices.
- `docs/architecture/fused-operator-pipeline.md`
  - Role: GAR-PERF-2E reference for scoped fused local prepared/native operator-pipeline evidence.
  - Status rule: scoped filter/projection/limit, filter/aggregate, and top-k/projection evidence is
    complete with deterministic filter/group-by blockers. Stronger independent unfused runtime
    certificates, broader families, encoded-native promotion, and claim-grade use must remain
    represented by later evidence-bearing slices.
- `docs/architecture/in-process-session-runtime.md`
  - Role: GAR-PERF-2F reference for scoped in-process session-backed prepared/native local-artifact
    runtime evidence and future public `ShardLoomSession` boundaries.
  - Status rule: scoped CLI batch runtime evidence is complete and recorded in the ledger. Python
    client exposure, broader public session APIs, buffer-pool ownership, and claim-grade use must
    remain represented by later evidence-bearing slices.
- `docs/architecture/io-reuse-and-fanout-architecture.md`
  - Role: report-only GAR-IOREUSE-1 reference for universal source-state reuse, decoupled
    Vortex-prepared-state reuse, output-plan reuse, cross-format local fanout, cache invalidation,
    evidence-safe reuse levels, and Foundry generated-output fanout posture.
  - Status rule: defines the
    `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact`
    architecture and benchmark field vocabulary. GAR-IOREUSE-1A through GAR-IOREUSE-1E have
    established SourceState, VortexPreparedState, OutputPlan, and report-only fanout benchmark row
    contracts plus cache/fingerprint invalidation evidence. Runtime state caches, fanout writers,
    evidence-safe reuse levels, Foundry generated-output smoke, object-store I/O, table commits,
    and claim-grade use must remain represented by `GAR-IOREUSE-1*` or later evidence-bearing
    slices.
- `docs/architecture/allocation-buffer-pool-optimization.md`
  - Role: GAR-PERF-2G reference for scoped allocation/resource-profile evidence and buffer-pool
    blocker semantics across prepared/native local runtime paths.
  - Status rule: current session-backed batch rows emit allocation/resource fields and deterministic
    blockers. Runtime buffer pools, allocator hooks, safe reuse implementation, memory-efficiency
    claims, and claim-grade use remain unclaimed until represented by later evidence-bearing
    slices.
- `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`
  - Role: GAR-PERF-2H reference for optimized Cargo build profiles and a reproducible PGO/native
    benchmark lane.
  - Status rule: build-profile vocabulary, Cargo profiles, benchmark row fields, PGO helper flow,
    target-CPU-native boundaries, and release portability rules are complete and recorded in the
    ledger. Actual PGO profile artifacts, benchmark reruns under optimized profiles, and any
    claim-grade use must remain represented by later evidence-bearing slices.
- `docs/architecture/bayesian-performance-layout-advisor.md`
  - Role: GAR-PERF-1D reference for the report-only Bayesian performance and layout advisor
    contract.
  - Status rule: benchmark artifacts now emit `bayesian_advisor_*` fields for advisory-only
    confidence/uncertainty and future mode/reuse/sizing/layout decision surfaces. Runtime
    decisioning, automatic layout writes, fitted posterior models, and claim-grade confidence use
    must remain represented by later evidence-bearing slices before they can affect behavior or
    public claims.
- `docs/architecture/capability-certification-sequencing.md`
  - Role: CG-20 sequencing ledger and implementation-order reference.
  - Status rule: phase-plan checklist owns planned CG-20 work items. Remaining approximate/sketch
    function and certification-scope coverage details are carried by `GAR-0021` and `GAR-0032`.
- `docs/architecture/vortex-public-api-inventory.md`
  - Role: Vortex public API evidence and adapter-boundary inventory.
  - Status rule: API findings inform CG-1/CG-2/CG-3 queue items here.
- `docs/architecture/vortex-runtime-utilization-audit.md`
  - Role: Vortex-first runtime utilization audit for arrays, execution layers, Scan
    Source/Sink/Split, layouts, I/O, sessions/registries, device posture, extension types, and
    benchmark discipline.
  - Status rule: report/code surfaces here do not authorize runtime provider promotion; actionable
    provider or benchmark work must remain represented in this phase plan.
- `docs/architecture/vortex-adapter-integration-plan.md`
  - Role: Vortex adapter rationale, boundaries, and historical integration notes.
  - Status rule: adapter work is actionable only after represented in this phase plan.
- `docs/architecture/repo-cleanup-backlog.md`, `diagnostics-normalization-backlog.md`,
  `terminology-consolidation-backlog.md`, and `feature-footprint-doctor-plan.md`
  - Role: cleanup inventories and completed cleanup ledgers.
  - Status rule: cleanup must be promoted into this file as a concrete checklist item. Remaining
    diagnostic, terminology, command-registry, traceability, and acceptance-checker details are
    carried by `GAR-0012`, `GAR-0039`, and `GAR-0043`.
- `docs/architecture/canonical-terminology.md`
  - Role: authoritative glossary and concept index for ShardLoom vocabulary.
  - Status rule: defines terms and links to governing RFCs, but does not mark current phase or CG
    completion.
- `docs/architecture/systems-learning-map.md`
  - Role: technique-transfer map from external systems and design references into ShardLoom-native
    contracts.
  - Status rule: records lessons and guardrails only; it does not authorize dependencies, runtime
    behavior, or CG completion.
- `docs/architecture/benchmark-suite-catalog.md`
  - Role: CG-6.25 benchmark-suite catalog and Priority 2.7 source-backed correctness/benchmark
    matrix orientation.
  - Status rule: records matrix/catalog report surfaces, the executable local taxonomy runner
    status, and claim blockers; full comparative benchmark reruns and performance claims remain
    separate planned/release-readiness actions.
- `docs/architecture/crate-posture-public-exports.md`
  - Role: Priority 2.8 crate posture and public export grouping reference.
  - Status rule: documents current executable/report-only/unsupported/planned/prohibited-fallback export
    posture only; it does not authorize runtime or dependency expansion.
- `docs/architecture/workspace-feature-build-matrix.md`
  - Role: Priority 3.5 workspace feature/build validation matrix reference.
  - Status rule: records required validation rows and release blockers; it does not authorize
    package publication, dependency expansion, runtime expansion, or fallback execution.
- `docs/architecture/universal-import-deployment-baseline-harness.md`
  - Role: Priority 3.5 / CG-18 universal import, deployment, and baseline harness maturity
    reference.
  - Status rule: records required local/CI/container/optional Foundry/optional benchmark harness
    rows and comparison-only baseline environment boundaries; it does not authorize harness
    execution, package publication, external engine invocation, or fallback execution.
- `docs/architecture/rfc-coverage-followthrough.md`
  - Role: Priority 3.6 RFC coverage follow-through reference for RFC 0010, RFC 0011, RFC 0020,
    RFC 0022, and RFC 0023 before broader user/runtime expansion.
  - Status rule: records report-only coverage gates for developer/agent usability, modular
    extensibility, table/catalog compatibility, plan interop, and extension sandboxing; it does not
    authorize parser expansion, dependency expansion, imported-plan execution, extension execution,
    external effects, external engine invocation, or fallback execution.
- `docs/architecture/typed-command-result-envelope.md`
  - Role: Priority 3.9 typed command/result envelope reference for the `shardloom.output.v2`
    protocol slice and remaining command-family migration work.
  - Status rule: records the typed envelope slots and temporary legacy field mirror; it does not
    authorize runtime expansion, command effects, external engine invocation, REST/server behavior,
    or fallback execution.
- `docs/architecture/incumbent-gap-opportunity-map.md`, `lakehouse-value-prop-compatibility.md`,
  `universal-input-contract.md`, and `spill-reservation-lifecycle-integration.md`
  - Role: reference maps and constraints.
  - Status rule: they guide design decisions but do not mark CG completion.
- `docs/architecture/operational-evidence-policy-hardening.md`
  - Role: shared evidence, policy, workload, lifecycle, protocol-parity, benchmark-constitution, and
    artifact-safety contracts for CG-20 through CG-23.
  - Status rule: contract reference only; actionable implementation work must be represented in the
    Planned queue.
- `docs/architecture/evidence-native-generated-execution-observability-confidence.md`
  - Role: report-only GAR-NOVEL-1 reference for generated-source evidence, OpenLineage facets,
    OpenTelemetry trace mapping, and Bayesian confidence.
  - Status rule: describes export and confidence contracts only. Generated-output runtime,
    lineage/telemetry exporters, Bayesian release blockers, and any dependency changes must remain
    represented by `GAR-NOVEL-1`, `GAR-GEN-1`, `GAR-PERF-1D`, `GAR-0018`, `GAR-0029`, or
    release-gate slices before implementation.
- `docs/architecture/adoption-commercial-readiness-friction-reduction.md`
  - Role: report-only GAR-COMMERCIAL-1 reference for adoption friction, one-command local proof,
    package-channel readiness, buyer-facing status, enterprise evidence export, Foundry starter, and
    workflow recipes.
  - Status rule: describes adoption/commercial readiness only. Package publication, release tags,
    OCI pushes, package-channel submissions, Foundry runtime proof, export backends, recipe runtime
    expansion, and public readiness claims must remain represented by `GAR-COMMERCIAL-1`,
    `GAR-0024`, `GAR-0033`, `GAR-0036`, `GAR-NOVEL-1`, or release-gate slices before
    implementation.
- `docs/use-cases/README.md`, `docs/use-cases/use-case-index.yml`, and
  `docs/use-cases/templates/use-case-template.md`
  - Role: non-expert Use Case Atlas for answering "Can ShardLoom do my thing?", "How do I try it?",
    "What evidence do I get?", and "What is not supported yet?" without reading the phase plan,
    RFCs, or benchmark internals.
  - Status rule: may describe current supported, smoke-supported, report-only, planned, blocked, and
    unsupported use-case posture only. Runtime expansion, website status projection, generated page
    output, recipe implementation, and all-capability coverage enforcement must remain represented by
    `GAR-DOCS-1*` or later evidence-bearing slices before implementation.
- `docs/architecture/object-store-request-planner.md`
  - Role: CG-10 request-planning, range/coalescing/scheduling/checkpoint/retry/commit evidence
    reference.
  - Status rule: object-store runtime work is represented by `GAR-0008`, `GAR-0028`, and `GAR-0031`.
- `docs/architecture/table-intelligence-layer.md`
  - Role: CG-9 schema/table/catalog/CDC/layout/compaction evidence reference.
  - Status rule: table/catalog runtime work is represented by `GAR-0020` and `GAR-0028`.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md`
  - Role: report-only universal source/sink/adapter/user-surface compatibility map covering local
    files, Vortex, generated/source-free output, databases, warehouses, object stores, table
    formats, REST/Flight/ADBC, and Foundry.
  - Status rule: scoreboard rows classify runtime-supported, smoke-supported, report-only, blocked,
    or not-planned posture only. Actionable compatibility/runtime work remains represented by
    `GAR-COMPAT-1`, `GAR-GEN-1`, `GAR-0008`, `GAR-0020`, `GAR-0028`, `GAR-0031`, and related
    evidence-bearing slices.
- `docs/architecture/dynamic-work-shaping.md`,
  `spill-reservation-lifecycle-integration.md`, and `effect-budget-plan.md`
  - Role: runtime shaping, memory/spill lifecycle, and side-effect policy references.
  - Status rule: runtime implementation must be represented by `GAR-0014`, `GAR-0016`,
    `GAR-0019`, or `GAR-0011` before code changes.
- `docs/architecture/correctness-differential-harness.md`,
  `benchmark-competitive-claim-evidence.md`, and `benchmark-suite-catalog.md`
  - Role: correctness, benchmark, claim-evidence, and catalog references.
  - Status rule: claim-grade execution, fuzz/property expansion, comparative reruns, and public
    claims are represented by `GAR-0015`, `GAR-0029`, `GAR-0040`, and `GAR-0041`.
- `docs/architecture/agent-contract-pack.md`
  - Role: agent protocol and no-fallback task contract reference.
  - Status rule: protocol changes must remain represented by `GAR-0010`, `GAR-0037`, or `GAR-0039`.
- `docs/architecture/vortex-upstream-alignment-hardening.md`
  - Role: Vortex compatibility, Scan API, compute-provider, residual-boundary, device,
    extension-type, object-store telemetry, integration-boundary, and benchmark-interoperability
    contract reference.
  - Status rule: contract reference only; provider promotion, Vortex-native execution, and
    dependency changes must remain represented by the relevant GAR provider/runtime/release item
    before implementation.
- `docs/skills/vortex/vortex-first-provider-check.md`
  - Role: Vortex-adjacent implementation guard requiring agents to check upstream Vortex concepts
    and classify decisions before inventing new ShardLoom abstractions.
  - Status rule: process guard only; it does not authorize new Vortex API use, dependency changes,
    runtime behavior, support claims, external engine invocation, or fallback execution.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

### Global Architecture Review Carry-Forward

Source: `docs/architecture/global-architecture-review.md`.

Scope: every unchecked RFC and compute-flow review item is mirrored here so no planned,
unsupported, or not-claimable architecture work exists only in a supporting document. Complete these
items in logical implementation order, update the global review checkbox when evidence closes, and
move the completed session details to `docs/architecture/phased-execution-completed-ledger.md`.

Default GAR verification for planning-only/docs slices:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
git diff --check
```

Code-bearing GAR slices must add the focused Rust/Python/benchmark tests named in the slice and
usually end with:

```powershell
cargo fmt --all -- --check
cargo test --workspace --all-targets
python -m compileall -q python/src python/tests scripts examples
git diff --check
```

#### GAR-P0 - Execution Mode, Provider Admission, And Vortex Spine

P0 slices must preserve the canonical execution-mode vocabulary from
`docs/architecture/compute-engine-flow-reference.md`: `auto`, `compatibility_import_certified`,
`prepared_vortex`, `native_vortex`, and `direct_compatibility_transient`. Benchmark interpretation
must continue to report stage timing fields (`source_read_millis`, `compatibility_parse_millis`,
`compatibility_to_vortex_import_millis`, `vortex_write_millis`, `vortex_reopen_millis`,
`vortex_scan_millis`, `operator_compute_millis`, `result_sink_write_millis`,
`evidence_render_millis`, and `total_runtime_millis`) so compatibility rows are interpreted as
ingest/stage/certification work, not pure query speed. Do not add a hidden global fast-mode toggle.

#### GAR-BENCH-PUB-1 - Complete Competitor Benchmark Publishing And Static Artifact Ingestion

GAR-BENCH-PUB-1A through GAR-BENCH-PUB-1H are complete and recorded in
`docs/architecture/phased-execution-completed-ledger.md`. The remaining benchmark-publishing work is
future evidence refresh or new benchmark families, not an active GAR-BENCH-PUB-1 planned item.

#### GAR-IOREUSE-1 - I/O Reuse And Cross-Format Fanout

These slices expand the prepared/native runtime roadmap from scenario-local source-state reuse into
decoupled source, preparation, execution, output, and sink evidence. Input and output formats must
remain independent. The stable path is:

```text
InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact
```

The benchmark bundle vocabulary is `io_reuse_and_fanout`, `source_state_reuse`,
`prepared_state_reuse`, `output_plan_reuse`, `cross_format_output`, and
`generated_source_output`. SourceState, VortexPreparedState, OutputPlan, report-only fanout
benchmark rows, cache/fingerprint invalidation rows, evidence-safe reuse-level rows, and
report-only Foundry generated-output fanout posture are now established by GAR-IOREUSE-1A through
GAR-IOREUSE-1G and recorded in the completed ledger. Runtime fanout writers, persistent caches,
multi-output correctness/replay evidence, object-store output, lakehouse/table commit semantics,
hidden fast modes, external engine fallback, and performance claims remain out of scope until
separate evidence-bearing slices admit them.

#### GAR-SCALE-1 - Spark-Level Scale Contract And Any-Volume Execution Readiness

Goal: ShardLoom must not claim literal "any volume" support. Instead, scale work must define and
prove bounded-memory, split-based, spill-safe, shuffle-aware, object-store/table-aware, retryable,
idempotent execution under an explicit resource envelope. Scale profiles must remain separate from
local benchmark leaderboards, and synthetic metadata-only evidence cannot become a runtime scale
claim.

Required scale classes:

```text
local_smoke
local_claim_grade
larger_than_memory_local
split_parallel_local
object_store_read_report_only
object_store_runtime
table_metadata_report_only
table_runtime
distributed_report_only
distributed_runtime
foundry_dev_stack_proof
managed_platform_proof
```

Required shared scale evidence fields:

```text
scale_profile
scale_claim_status
data_volume_bytes
row_count_estimate
file_count
partition_count
split_count
memory_budget_bytes
peak_memory_bytes
spill_allowed
spill_bytes_written
spill_bytes_read
shuffle_required
shuffle_strategy
shuffle_bytes_written
shuffle_bytes_read
skew_detected
retry_count
idempotency_key
output_commit_status
object_store_involved
table_format_involved
remote_workers_involved
foundry_runtime_invoked
foundry_spark_invoked
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

GAR-SCALE-1A through GAR-SCALE-1F are complete and recorded in the completed ledger. The active
follow-through begins with scale benchmark profiles and synthetic scale evidence; all non-local scale
classes remain blocked or report-only until later slices attach runtime evidence.

- [ ] GAR-SCALE-1G scale benchmark profiles and synthetic scale evidence
  - Source:
    - Benchmark profile matrix and static benchmark publishing workflow.
    - GAR-SCALE-1A scale taxonomy.
    - GAR-SCALE-1B split manifests.
    - GAR-SCALE-1C memory/spill/backpressure contract.
    - GAR-SCALE-1D shuffle/repartition evidence.
    - GAR-SCALE-1E object-store/table ladder.
  - Current state:
    - Current benchmark profiles focus on smoke, full local, Spark-context, extended local, GPU
      optional, object-store optional, and I/O reuse/fanout planning.
    - Scale-oriented profiles are not first-class.
    - Current public benchmark page must remain evidence, not a scale/performance leaderboard.
  - Next slice outcome:
    - Add scale-oriented benchmark profile definitions and synthetic metadata-only evidence rules
      without requiring immediate massive hardware or changing current local benchmark volumes.
  - User-visible surface:
    - Benchmark docs, website/benchmarks, benchmark manifests, and release-readiness gates.
  - Implementation scope:
    - Benchmark profile docs/schema, artifact manifest fields, completeness checks, website
      interpretation text, and deterministic unsupported/synthetic rows. Runtime large-volume
      execution is a later slice.
  - Evidence required:
    - `scale_profile`
    - `rows`
    - `input_bytes`
    - `file_count`
    - `split_count`
    - `peak_memory_bytes`
    - `spill_bytes`
    - `shuffle_bytes`
    - `retry_count`
    - `correctness_digest`
    - `fallback_attempted=false`
    - `external_engine_invoked=false`
    - `claim_gate_status`
  - Acceptance:
    - Profiles are defined for `local_stress`, `larger_than_memory_local`, `many_small_files`,
      `partitioned_table_metadata`, `object_store_report_only`, `table_metadata_report_only`,
      `foundry_dev_stack_scale_proof`, and `distributed_report_only`.
    - Required scenarios include 10M/100M row local stress where feasible, data larger than a
      configured memory budget, many-small-files scan, partition pruning, skewed group-by, broadcast
      candidate join, shuffle join, CDC overlay over a large base, dirty/schema-drift write path, and
      output fanout.
    - Scale benchmarks are separated from local smoke and public leaderboard rows.
    - Synthetic metadata-only scale evidence cannot become a runtime scale claim.
    - Actual large-volume evidence requires real input bytes and correctness proof.
  - Verification:
    - Benchmark profile validation tests.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No benchmark volume change in this planning slice, no performance/superiority claim, no
      object-store runtime, no distributed runtime, and no Spark replacement claim.
  - Dependencies/blockers:
    - Benchmark profile registry, artifact manifest schema, completeness gate, hardware/resource
      disclosure, correctness proof, and website benchmark interpretation updates.
  - Claim boundary:
    - Scale profiles describe evidence posture. They cannot claim scale readiness unless backed by
      real workload bytes, correctness proof, no-fallback evidence, and the relevant runtime gates.
  - Fallback boundary:
    - External benchmark engines remain baseline-only and cannot satisfy ShardLoom scale evidence.
  - Ledger rule:
    - When complete, move profile/schema details and benchmark interpretation boundaries to the
      completed ledger.

- [ ] GAR-SCALE-1H Foundry scale proof boundary
  - Source:
    - RFC 0036 Foundry Integration Pack and availability surface.
    - Foundry proof-of-use docs.
    - Foundry generated-output fanout posture.
    - GAR-SCALE-1A scale taxonomy and GAR-SCALE-1G scale benchmark profiles.
  - Current state:
    - Foundry proof is local/style-only and report-only.
    - Foundry no-input generated-output fanout posture exists, but generated-output execution is not
      a Foundry production claim.
    - No real Foundry runtime, Foundry compute, Foundry Spark, or managed-platform scale proof is
      claimable.
  - Next slice outcome:
    - Define what a real Foundry scale proof must emit while keeping local/dev-stack proof separate
      from production Foundry support.
  - User-visible surface:
    - Foundry proof docs, website/status, capability matrix, and future Foundry starter workflows.
  - Implementation scope:
    - Foundry scale proof schema/docs, deterministic report-only blockers, release-readiness checks,
      and claim boundary text. No Foundry platform invocation or package publication.
  - Evidence required:
    - `foundry_runtime_invoked`
    - `foundry_compute_invoked`
    - `foundry_spark_invoked=false`
    - `foundry_input_dataset_count`
    - `foundry_output_dataset_count`
    - `staged_input_bytes`
    - `shardloom_execution_mode`
    - `split_count`
    - `memory_budget_bytes`
    - `output_evidence_dataset_written`
    - `fallback_attempted=false`
    - `external_engine_invoked=false`
    - `public_foundry_claim_allowed=false`
    - `claim_gate_status`
  - Acceptance:
    - Foundry can orchestrate a transform only when evidence distinguishes orchestration from
      ShardLoom execution.
    - Spark/Foundry compute cannot be silently reported as ShardLoom execution.
    - Evidence dataset output is mandatory for any proof claim.
    - Foundry scale proof remains separate from production support and package/channel claims.
  - Verification:
    - Foundry proof docs/schema checks.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No Foundry production support, Marketplace/package claim, Foundry runtime invocation, Foundry
      Spark use as ShardLoom execution, object-store direct write, or performance claim.
  - Dependencies/blockers:
    - Real Foundry environment proof, transform output/evidence dataset wiring, Foundry runtime and
      compute invocation fields, package-channel evidence, and platform claim approval gates.
  - Claim boundary:
    - This slice may claim only a Foundry scale proof boundary. Real Foundry scale support remains
      blocked until platform evidence exists.
  - Fallback boundary:
    - Foundry Spark, virtual tables, Snowflake, Databricks, BigQuery, or other managed compute cannot
      execute ShardLoom work as fallback or satisfy ShardLoom no-fallback evidence.
  - Ledger rule:
    - When complete, record the Foundry proof boundary and any remaining blockers in the completed
      ledger.

#### GAR-P1 - Core Runtime, Operators, And Execution Safety

#### GAR-P2 - I/O, Tables, Output, And Lakehouse Semantics
#### GAR-P3 - User Surfaces, APIs, Adapters, And Workflow

#### GAR-COMPAT-1 - Universal Compatibility Completion Matrix

- [ ] GAR-COMPAT-1A universal source/sink coverage scoreboard
  - Source: RFC 0007; RFC 0008; RFC 0020; RFC 0028; RFC 0030; RFC 0031; RFC 0032; RFC 0033;
    RFC 0035; RFC 0036; RFC 0037; DataFusion and Polars local I/O expectations as comparison
    pressure only; `docs/architecture/universal-compatibility-coverage-scoreboard.md`;
    `docs/architecture/compute-engine-flow-reference.md`;
    `docs/architecture/benchmark-suite-catalog.md`.
  - Current state:
    - The report-only scoreboard doc now classifies local files, Vortex, generated/source-free
      outputs, Python rows/DataFrame, SQL literals/VALUES, databases, object stores, table formats,
      REST/Flight/ADBC, and Foundry as runtime-supported, smoke-supported, report-only, blocked, or
      not-planned.
    - ShardLoom has scoped local compatibility import and scoped local Vortex runtime evidence, but
      runtime support is not universal for databases, Excel, object stores, table formats,
      generated-source APIs, SQL/DataFrame execution, REST/Flight/ADBC, or Foundry.
    - The scoreboard is not yet projected into website/status or Python capability views.
  - Next slice outcome:
    - Promote the scoreboard into stable machine-readable capability/status surfaces while keeping
      the Markdown doc as the human review source.
  - User-visible surface:
    - Docs, website/status, Python capability view, CLI capability/status JSON, and release-readiness
      checks.
  - Implementation scope:
    - Add typed compatibility scoreboard rows or generator input, website/status projection, Python
      typed capability accessors, snapshot tests, and docs links. Do not add runtime connectors.
  - Evidence required:
    - correctness refs: snapshot rows proving unsupported surfaces stay blocked/report-only.
    - benchmark refs: none for the scoreboard; benchmark refs required before any performance claim.
    - execution certificate refs: present only for runtime-supported scoped rows.
    - Native I/O certificate refs: required only where actual source/sink runtime exists.
    - materialization/decode refs: required for local file/Vortex rows that claim scoped runtime.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - Scoreboard distinguishes plan/report coverage from runtime coverage.
    - Every listed surface has a status, blocker or next evidence, and claim boundary.
    - No unsupported source, sink, adapter, database, object-store path, table format, SQL/DataFrame
      API, REST/Flight/ADBC bridge, or Foundry integration is advertised as supported.
    - Website/status and Python views use typed fields instead of scraping prose.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - Python typed capability tests if Python accessors change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No database, Excel, JDBC/ODBC, object-store, table-format, SQL/DataFrame, REST/Flight/ADBC,
      Foundry, or generated-output runtime in this slice.
    - No new dependencies, package publication, benchmark rerun, or performance claim.
  - Claim boundary:
    - Compatibility coverage is a capability map and evidence inventory, not a production,
      performance, Spark-replacement, object-store/lakehouse, Foundry, SQL/DataFrame, or package
      readiness claim.
  - Fallback boundary:
    - External engines and external databases may be baselines, oracles, migration references, or
      import/export endpoints only; they cannot execute unsupported ShardLoom work as fallback.
  - Dependencies/blockers:
    - Typed capability view ownership, website/status data model, release-readiness claim checker,
      and per-surface runtime evidence for any status upgrade.
- [ ] GAR-COMPAT-1B source-free generated output contract
  - Source: GAR-COMPAT-1A; GAR-GEN-1A; RFC 0031; RFC 0032; RFC 0033; RFC 0036; RFC 0037; RFC 0039;
    no-dataset smoke reports; Python/DataFrame capability posture; Foundry proof docs.
  - Current state:
    - No-input smoke exists and remains status/capability/proof only.
    - Benchmark synthetic fixtures exist, but they are benchmark setup and do not count as
      user-facing generated-output runtime.
    - First-class Python, SQL, or DataFrame-style generated output execution is not
      runtime-supported.
    - GAR-GEN-1A/1B now expose a report-only `GeneratedSourceCertificate` contract and keep
      no-dataset smoke separate from generated-output execution through core, CLI, and Python
      capability views.
  - Next slice outcome:
    - Add a compatibility-level source-free generated-output contract row that links the scoreboard,
      Python capability surfaces, SQL/DataFrame posture, and Foundry proof boundary to GAR-GEN-1.
  - User-visible surface:
    - Python API capability view, SQL/DataFrame capability matrix, docs, website/status, and Foundry
      no-input/generated-output proof docs.
  - Implementation scope:
    - Compatibility status rows for `ctx.range`, `ctx.from_rows`, `ctx.literal_table`,
      `ctx.calendar`, `ctx.write`, SQL literal `SELECT`, SQL `VALUES`, source-free projection, and
      local-output-only generated-source posture.
  - Evidence required:
    - correctness refs: deterministic generated rows/schema/output only for runtime-promoted rows.
    - execution certificate refs: required for any generated-output execution.
    - Native I/O certificate refs: no source Native I/O certificate when no source dataset is read;
      output certificate required when output is written.
    - generated-source refs: `input_dataset_count=0`, `source_io_performed=false`,
      `generated_source_created=true`, `generated_source_kind`, `generated_source_schema_digest`,
      `generated_source_row_count`, `generated_source_plan_digest`, optional
      `generated_source_seed`, `generation_deterministic`, `generated_source_certificate_status`.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - No-input smoke remains distinct from generated-output execution.
    - No source Native I/O certificate is claimed when no source was read.
    - Output sink evidence is still required for output data claims.
    - SQL/DataFrame runtime support is not overclaimed.
    - S3/object-store remains blocked/report-only unless future evidence admits it.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - capability/DataFrame matrix snapshot tests when surfaces change.
    - `git diff --check`
  - Non-goals:
    - No S3 write, object-store write, broad SQL/DataFrame runtime, Foundry production claim,
      package publication, or external engine fallback.
  - Claim boundary:
    - Compatibility may say generated-output support is planned/report-only until GAR-GEN runtime
      slices land. It cannot claim source-free SQL/DataFrame generation or object-store/Foundry
      generated-output runtime.
  - Fallback boundary:
    - No hidden pandas, Polars, DuckDB, DataFusion, Spark, database, object-store, or Foundry compute
      execution.
  - Dependencies/blockers:
    - Output sink evidence model, Python API decision, SQL/DataFrame admission rows, compatibility
      scoreboard/status projection, and Foundry output API proof boundary.
- [ ] GAR-COMPAT-1C S3/GCS/ADLS runtime admission ladder
  - Source: RFC 0008; RFC 0019; RFC 0028; RFC 0031; current object-store report-only planner;
    object-store byte-range gates; `docs/architecture/object-store-request-planner.md`;
    `docs/architecture/universal-compatibility-coverage-scoreboard.md`.
  - Current state:
    - Object-store range planning and request-shape reports exist.
    - Runtime object-store I/O remains blocked: no credential resolution, network probe,
      provider probe, byte-range read, full-file read, write staging, or commit protocol runtime.
  - Next slice outcome:
    - Define and expose a staged admission ladder for S3/GCS/ADLS runtime support without enabling
      runtime I/O.
  - User-visible surface:
    - CLI object-store plan, Python capability view, website/status, object-store docs, and
      deterministic unsupported diagnostics.
  - Implementation scope:
    - Capability/report rows for object-store URI parse, credential policy, signed or
      no-credential public read, byte-range read, full-file read, local cache, write staging, and
      commit protocol. Keep all runtime effects blocked in this slice.
  - Evidence required:
    - credential refs: `credential_policy_status`, `credential_resolution_performed=false`.
    - network refs: `network_probe_allowed`, `provider_probe_allowed`, `object_store_io=false`.
    - read refs: `byte_range_read_allowed`, full-file read blocker, cache blocker.
    - write refs: `write_io=false`, staging/commit blocker, idempotency/rollback blocker.
    - Native I/O certificate refs: absent or `not_applicable` until runtime proof exists.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - S3/GCS/ADLS are not advertised as runtime-supported until read/write proof exists.
    - Public no-credential read and authenticated read are separated.
    - Read support, local cache, write staging, and write/commit support are separated.
    - Unsupported requests return deterministic blockers and do not perform network or credential
      effects.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - object-store gate/report snapshot tests when report rows change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No object-store runtime, no network probe, no credential resolution, no byte-range read,
      no full-file read, no object-store write, no commit protocol runtime, and no lakehouse claim.
  - Claim boundary:
    - Admission ladder/report-only visibility only; no object-store runtime support claim.
  - Fallback boundary:
    - No external engine, external database, object-store provider, or query-engine integration may
      execute unsupported work.
  - Dependencies/blockers:
    - Credential/effect policy, provider selection, byte-range runtime proof, local cache safety,
      write idempotency, commit/rollback semantics, and Native I/O certificates.
- [ ] GAR-COMPAT-1D table-format boundary matrix: Iceberg, Delta, Hudi
  - Source: RFC 0020; RFC 0028; RFC 0031; object-store/lakehouse commit semantics gate;
    table/catalog compatibility expectations;
    `docs/architecture/table-intelligence-layer.md`;
    `docs/architecture/universal-compatibility-coverage-scoreboard.md`.
  - Current state:
    - Table/lakehouse commit semantics are gated/report-only.
    - Local manifest-backed metadata smoke exists, but it does not imply Iceberg, Delta, or Hudi
      runtime support.
  - Next slice outcome:
    - Add a table-format boundary matrix for Iceberg, Delta, and Hudi behaviors and claim blockers.
  - User-visible surface:
    - Docs, website/status, capability views, table/catalog diagnostics, and release-readiness
      claim checks.
  - Implementation scope:
    - Matrix rows for table scan, metadata read, snapshot/time travel, partition evolution,
      delete/tombstone, append, merge/update/delete, commit, rollback, catalog interaction, and
      object-store coupling.
  - Evidence required:
    - metadata refs: table metadata source, snapshot refs, schema/partition refs.
    - data refs: scan/read support status, delete/tombstone status, CDC/overlay boundary.
    - write refs: append, merge/update/delete, commit, rollback, idempotency, and cleanup blockers.
    - Native I/O certificate refs: only for scoped runtime-supported rows.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - Each table-format behavior is classified as runtime-supported, smoke-supported, report-only,
      blocked, or not-planned.
    - Metadata smoke does not imply table scan, table write, table commit, or production lakehouse
      runtime.
    - Object-store read/write/commit dependencies remain explicit blockers.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - table/catalog report snapshot tests when rows change.
    - `git diff --check`
  - Non-goals:
    - No table-format dependency expansion, object-store runtime, catalog probe, table scan, table
      write, commit, rollback, or production lakehouse claim.
  - Claim boundary:
    - Table-format boundary matrix only; no Iceberg, Delta, Hudi, lakehouse, or catalog runtime
      support claim.
  - Fallback boundary:
    - External table engines, query engines, catalogs, and platform runtimes are not fallback
      execution paths.
  - Dependencies/blockers:
    - Table-format dependency/license approval, object-store runtime ladder, commit protocol,
      catalog policy, metadata fidelity, and table workload certification.
- [ ] GAR-COMPAT-1E database and warehouse import/export boundary
  - Source: RFC 0030; RFC 0031; RFC 0032; RFC 0033; RFC 0037; user compatibility expectations;
    Python client/adapters; `docs/architecture/universal-compatibility-coverage-scoreboard.md`.
  - Current state:
    - Database and warehouse connectors are not first-class ShardLoom runtime paths.
    - Existing external systems remain baselines, oracles, migration references, or future
      import/export endpoints only.
  - Next slice outcome:
    - Add a report-only database/warehouse import/export matrix for SQLite, Postgres, MySQL,
      ODBC/JDBC, Snowflake, BigQuery, Databricks SQL, and similar endpoints.
  - User-visible surface:
    - Docs, website/status, Python capability view, connector diagnostics, and release-readiness
      claim checks.
  - Implementation scope:
    - Report rows for connector type, credential requirement, network requirement, supported import
      or export posture, query pushdown boundary, external-baseline-only status, and deterministic
      blockers. Do not add connector runtime.
  - Evidence required:
    - connector refs: `connector_type`, driver/dependency posture, dialect boundary.
    - credential/network refs: `credential_required`, `network_required`,
      `credential_resolution_performed=false`, `network_probe_performed=false`.
    - runtime refs: `supported_path`, import/export status, query pushdown status,
      `external_engine_invoked=false`, `fallback_attempted=false`.
    - certificate refs: Native I/O and execution certificates only if a future scoped runtime path
      exists.
  - Acceptance:
    - Import/export is separated from query pushdown.
    - External databases and warehouses are not fallback engines.
    - Credential and network requirements are visible before any future runtime admission.
    - Unsupported requests return deterministic diagnostics.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - connector/capability snapshot tests when report rows change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No database connector runtime, no warehouse connector runtime, no JDBC/ODBC driver loading, no
      credentials, no network probes, no query pushdown, no external engine fallback, and no
      production connector claim.
  - Claim boundary:
    - Report-only import/export boundary matrix; no database/warehouse runtime or query-pushdown
      claim.
  - Fallback boundary:
    - Database and warehouse engines cannot execute unsupported ShardLoom plans as fallback.
  - Dependencies/blockers:
    - Connector dependency/license review, credential/effect policy, snapshot semantics, import and
      export certificate model, and Python/CLI capability projection.

- [ ] GAR-GEN-1F Foundry generated-output proof boundary
  - Source: GAR-GEN-1A; RFC 0036; `docs/foundry/proof-of-use-certification.md`.
  - Current state:
    - Foundry proof is local Foundry-style only; it includes no-dataset smoke and local Vortex smoke,
      but it does not implement Foundry generated-output runtime.
    - GAR-GEN-1E now exposes source-free Python, SQL, DataFrame, and API admission rows through
      `shardloom.generated_source_api_admission.v1`; Foundry generated-output remains
      report-only and must not be inferred from those local admission rows.
  - Next slice outcome:
    - Add a report-only Foundry generated-output boundary that requires future generated-output smoke
      to write through Foundry output APIs instead of direct S3/object-store paths.
  - User-visible surface:
    - Foundry proof docs, future Foundry proof report, release readiness docs.
  - Implementation scope:
    - Foundry boundary report fields, docs, optional proof-script diagnostics, tests.
  - Evidence required:
    - Foundry output API refs for future admitted smoke.
    - no direct S3/object-store credential, network, read, write, or commit evidence in this slice.
    - policy/no-fallback refs.
  - Acceptance:
    - Foundry generated-output support remains `support_status=report_only|blocked`.
    - Direct S3 write/read, object-store commit, lakehouse claim, and Foundry production claim remain
      blocked unless future platform evidence admits them.
  - Verification:
    - Foundry proof tests if report fields change.
    - release readiness metadata tests.
    - `git diff --check`
  - Non-goals:
    - No Foundry invocation, direct S3 runtime, object-store write, object-store commit, lakehouse
      output, package publication, or production Foundry claim.
  - Claim boundary:
    - Foundry generated-output is future validation target only.
  - Fallback boundary:
    - No external compute, no external engine, no hidden object-store path.
  - Dependencies/blockers:
    - GAR-GEN-1A, real Foundry environment proof, explicit Foundry output API integration.

#### GAR-NOVEL-1 - Evidence-Native Generated Execution, Lineage, Observability, And Confidence

- [ ] GAR-NOVEL-1A GeneratedSourceCertificate and source-free output execution alignment
  - Source: GAR-GEN-1A; GAR-COMPAT-1B; RFC 0031; RFC 0032; RFC 0033; RFC 0036; RFC 0037;
    no-dataset smoke reports; Python/DataFrame capability matrix; Foundry proof docs;
    `docs/architecture/evidence-native-generated-execution-observability-confidence.md`.
  - Current state:
    - No-input smoke exists, but generated-output execution is not first-class.
    - GAR-GEN-1 already owns the detailed generated-source certificate/runtime lane.
    - This GAR-NOVEL slice is the cross-surface alignment layer so Python, SQL/DataFrame, Foundry
      proof, lineage, telemetry, and confidence docs use the same evidence vocabulary.
  - Next slice outcome:
    - Add report-only capability rows and docs that align `GeneratedSourceCertificate` with
      source-free output, OpenLineage generated-source facets, OpenTelemetry spans, and Bayesian
      claim-confidence refs without creating runtime execution.
  - User-visible surface:
    - Python API docs, SQL/DataFrame capability matrix, Foundry dev-stack smoke docs, compute-flow
      docs, and future evidence export docs.
  - Implementation scope:
    - Capability/report rows, docs, Python typed view accessors if available, snapshot tests, and
      Foundry proof wording. Do not implement generated-output runtime in this slice.
  - Evidence required:
    - generated-source refs: `input_dataset_count=0`, `source_io_performed=false`,
      `generated_source_created`, `generated_source_kind`, `generated_source_schema_digest`,
      `generated_source_row_count`, `generated_source_plan_digest`, optional seed,
      deterministic status, and `generated_source_certificate_status`.
    - output refs: `output_io_performed`, output sink ref, output Native I/O certificate status.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
    - export refs: OpenLineage/OTel refs remain disabled or report-only.
  - Acceptance:
    - Generated output is not confused with no-dataset smoke.
    - Source I/O certificate is not emitted when no source exists.
    - Output evidence is required for output data claims.
    - SQL/DataFrame and Foundry generated-output support remain report-only/blocked unless narrower
      runtime proof exists.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - capability and Python accessor tests if report surfaces change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No generated-output runtime, SQL/DataFrame runtime, Foundry invocation, object-store write,
      package publication, or external engine fallback.
  - Claim boundary:
    - Report-only generated-source alignment only; no production SQL/DataFrame/Foundry claim.
  - Fallback boundary:
    - No hidden pandas, Polars, DuckDB, DataFusion, Spark, database, object-store, or Foundry compute
      execution.
  - Dependencies/blockers:
    - GAR-GEN-1A certificate contract, GAR-COMPAT-1B compatibility row, output sink certificate
      model, and Python/SQL/DataFrame capability ownership.
- [ ] GAR-NOVEL-1B OpenLineage evidence facets
  - Source: RFC 0018; RFC 0035; RFC 0036; operational evidence policy; ShardLoom evidence envelope;
    OpenLineage run/job/dataset/facet model;
    `docs/architecture/evidence-native-generated-execution-observability-confidence.md`.
  - Current state:
    - ShardLoom evidence is internal/JSON oriented.
    - RFC 0035 names OpenLineage posture, and Python/report surfaces expose that facets are mapped,
      but no OpenLineage export exists.
  - Next slice outcome:
    - Add report-only design and optional future schema placeholders for ShardLoom-owned custom
      OpenLineage facets: `ExecutionModeFacet`, `NoFallbackFacet`,
      `NativeIoCertificateFacet`, `MaterializationBoundaryFacet`, `ClaimGateFacet`,
      `GeneratedSourceFacet`, and `VortexArtifactFacet`.
  - User-visible surface:
    - Docs, future CLI export docs, future lineage capability rows, and release/readiness claim
      checks.
  - Implementation scope:
    - Facet mapping docs/report rows, capability fields, redaction/export policy fields, and tests.
      Do not emit lineage events or add a backend/client dependency in this slice.
  - Evidence required:
    - mapping refs: ShardLoom evidence field to run/job/input dataset/output dataset facet.
    - schema refs: ShardLoom-owned facet name, producer, schema URL/version placeholder.
    - safety refs: redaction policy, retention policy, export opt-in policy.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - No lineage event is emitted by default.
    - Export remains opt-in.
    - No external network call occurs without explicit policy.
    - Facets preserve claim gate, generated-source, materialization, Native I/O, Vortex artifact, and
      no-fallback evidence without implying backend integration.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - protocol/capability snapshot tests if report fields change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No lineage backend integration, event emitter, network client, Foundry lineage claim, schema
      publication, or dependency expansion.
  - Claim boundary:
    - Report-only lineage facet design; not production lineage support.
  - Fallback boundary:
    - Lineage export can never authorize fallback execution or external engine invocation.
  - Dependencies/blockers:
    - Evidence artifact safety, redaction/retention policy, schema publication decision, and explicit
      export policy.
- [ ] GAR-NOVEL-1C OpenTelemetry execution trace export contract
  - Source: RFC 0018; RFC 0035; benchmark stage timing model; runtime timing/evidence fields;
    OpenTelemetry trace/span/attribute/exporter concepts;
    `docs/architecture/evidence-native-generated-execution-observability-confidence.md`.
  - Current state:
    - ShardLoom has internal timing/evidence fields and benchmark stage timing fields.
    - RFC 0035 names OpenTelemetry/OTLP posture, but no OTel trace, metric, log, exporter, or
      collector integration exists.
  - Next slice outcome:
    - Define a report-only trace/span model for `request_admission`, `source_read`,
      `compatibility_parse`, `vortex_import`, `vortex_scan`, `operator_compute`, `result_sink`,
      `evidence_render`, and `claim_gate`.
  - User-visible surface:
    - Docs, future CLI env/config docs, future `runtime-report` or profile/certificate report rows.
  - Implementation scope:
    - Trace model docs/report rows, attribute allowlist, redaction rules, opt-in config schema, and
      snapshot tests. Do not add an OTel dependency or exporter in this slice.
  - Evidence required:
    - span refs: timing field to span/attribute mapping.
    - safety refs: redaction policy, secret/path/query-text handling, retention policy.
    - export refs: `otel_export_enabled=false`, `otel_network_exporter_enabled=false` by default.
    - policy/no-fallback refs: selected execution mode, claim gate, no-fallback/no-external-engine
      fields.
  - Acceptance:
    - OTel export is opt-in.
    - No network exporter is configured by default.
    - Evidence fields map to trace attributes without leaking secrets.
    - Observability support remains separate from runtime support and claim support.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - observability/protocol snapshot tests if report fields change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No OTel SDK dependency, OTLP exporter, collector config, live backend integration, production
      tracing claim, or runtime profiling collector.
  - Claim boundary:
    - Observability support does not imply production runtime, performance, or platform support.
  - Fallback boundary:
    - Trace export cannot create external execution, fallback execution, credential resolution, or
      network effects without explicit future policy.
  - Dependencies/blockers:
    - Evidence artifact safety, redaction/retention policy, dependency/license review, and explicit
      opt-in configuration.
- [ ] GAR-NOVEL-1D Bayesian claim-confidence and regression model
  - Source: GAR-PERF-1D; RFC 0029; RFC 0040; claim gate model; benchmark evidence model;
    `docs/architecture/evidence-native-generated-execution-observability-confidence.md`.
  - Current state:
    - Claim-grade gates are rule/evidence based.
    - GAR-PERF-1D now emits adjacent report-only Bayesian performance/layout advisor fields for
      confidence and uncertainty, but those fields are not a fitted posterior regression model.
  - Next slice outcome:
    - Add a report-only Bayesian claim-confidence schema with `posterior_runtime_distribution`,
      `credible_interval`, `probability_of_regression`, `minimum_iterations_for_claim_grade`, and
      `uncertainty_reason`.
  - User-visible surface:
    - Benchmark evidence docs, claim-gate docs, future release readiness report, website benchmark
      interpretation when evidence exists.
  - Implementation scope:
    - Confidence schema docs/report rows, claim-gate integration plan, benchmark evidence refs, and
      tests. Do not use the model to change runtime or claims in this slice.
  - Evidence required:
    - benchmark refs: benchmark constitution, run manifest, local environment, scenario population.
    - statistical refs: posterior model version, credible interval, sample size, uncertainty reason.
    - claim refs: current claim gate status, blockers, release/performance claim policy.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - Bayesian output is advisory.
    - It cannot upgrade claim status alone.
    - It can block release/performance claims when uncertainty is high.
    - It names the benchmark population and evidence refs used to compute uncertainty.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - benchmark/claim-gate snapshot tests if report fields change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No runtime auto-optimization, no hidden mode/layout decisioning, no performance claim, no
      benchmark recomputation, and no public superiority/replacement claim.
  - Claim boundary:
    - Advisory confidence can block claims but cannot create claim-grade status without the existing
      correctness, benchmark, execution-certificate, Native I/O, materialization, policy, and
      no-fallback gates.
  - Fallback boundary:
    - Confidence modeling cannot invoke external engines, external services, object-store I/O,
      credentials, or fallback execution.
  - Dependencies/blockers:
    - Stable benchmark evidence schema, release claim-gate policy, minimum-run policy, and
      statistical model review.

#### GAR-COMMERCIAL-1 - Adoption And Commercial-Readiness Friction Reduction

- [ ] GAR-COMMERCIAL-1A one-command local install and smoke proof
  - Source: RFC 0024; RFC 0030; RFC 0033; release dry-run proof; package publication plan; website
    public-preview readiness; `docs/getting-started/first-10-minutes.md`;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Source-local dry-run proof exists and builds local artifacts, installs a local wheel in a clean
      virtual environment, resolves the local CLI, runs smoke checks, and records no-publication
      safety fields.
    - Public package publication is not complete.
    - First-10-minutes docs are source-checkout oriented and still require readers to know which
      proof path to choose.
    - Generated/source-free output execution is not first-class yet; no-dataset smoke cannot be used
      as a generated-output claim.
  - Next slice outcome:
    - Make one documented local user path that runs install or local build, smoke, a tiny
      generated/source-free capability or runtime step, a tiny prepared/native example, and evidence
      inspection without requiring architecture-doc reading.
  - User-visible surface:
    - README, website/get-started, `docs/getting-started/first-10-minutes.md`, release dry-run docs,
      and example transcripts.
  - Implementation scope:
    - Docs, one wrapper command or script if needed, expected transcript shape, website links,
      release-readiness checks, and examples. Runtime behavior changes are out of scope.
  - Evidence required:
    - install refs: local build/wheel path or future package artifact ref.
    - smoke refs: CLI status, Python smoke, capabilities, no-fallback fields.
    - generated refs: generated-output runtime evidence when available or deterministic blocked
      capability diagnostics while GAR-GEN remains incomplete.
    - prepared/native refs: tiny local Vortex/prepared-native example evidence.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - User can complete a smoke path without reading architecture docs.
    - Docs clearly distinguish local package proof from public package release.
    - Generated-output proof is not confused with no-dataset smoke.
    - Prepared/native example exposes evidence and claim boundary.
  - Verification:
    - `python scripts/release_dry_run_proof.py --rows 64 --iterations 1` when the script changes.
    - `python scripts/check_website_readiness.py`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `git diff --check`
  - Non-goals:
    - No PyPI/TestPyPI/Conda/Homebrew/Scoop/winget/GHCR/crates.io publication unless release gates
      pass.
    - No generated-output runtime, broad SQL/DataFrame runtime, performance claim, or production
      claim in this slice.
  - Claim boundary:
    - Local technical-preview proof only; not public package release or production readiness.
  - Fallback boundary:
    - No external engine, fallback engine dependency, object-store runtime, Foundry runtime, or
      network service is required for the local proof.
  - Dependencies/blockers:
    - GAR-GEN generated-output posture, prepared/native tiny example stability, release dry-run proof
      ownership, and website get-started routing.
- [ ] GAR-COMMERCIAL-1B package channel readiness matrix
  - Source: RFC 0024; RFC 0030; release security/provenance docs; package-name readiness; PyPI
    Trusted Publishing/OIDC docs; TestPyPI, GitHub Releases, Homebrew, Scoop, winget, conda-forge,
    GHCR, and crates.io channel expectations;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Internal Rust crates are `publish=false`.
    - Python package metadata exists.
    - Release provenance, SBOM/checksum dry-run, package-name readiness, and local dry-run proof
      exist.
    - Public package/channel publication is not complete.
  - Next slice outcome:
    - Add a channel matrix for GitHub pre-release, TestPyPI, PyPI, Homebrew tap, Scoop/winget,
      conda-forge, GHCR container, and future crates.io public API crates.
  - User-visible surface:
    - Release docs, README release posture, website/status or release page, package readiness report.
  - Implementation scope:
    - Channel readiness docs/report rows, release gate metadata, expected commands, rollback/yank
      policy, and validation hooks. Do not publish packages.
  - Evidence required:
    - install command.
    - uninstall command.
    - clean install proof.
    - smoke check.
    - SBOM/checksum/provenance.
    - rollback/yank/delete/deprecate policy.
    - channel-specific auth/provenance refs, including PyPI Trusted Publisher/OIDC for PyPI.
  - Acceptance:
    - No channel is marked ready without proof.
    - PyPI uses Trusted Publisher/OIDC.
    - Internal Rust crates remain unpublished.
    - crates.io is limited to future stable public API crates, not current internal crates.
  - Verification:
    - release readiness metadata tests.
    - package/provenance dry-run tests if report fields change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No package publication, release tag, OCI push, package-channel submission, signing key use,
      secret creation, or dependency expansion.
  - Claim boundary:
    - Package access does not imply production readiness.
  - Fallback boundary:
    - Package channels cannot add Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, or another
      external query engine as a runtime fallback dependency.
  - Dependencies/blockers:
    - Hard release-readiness gate, trusted publisher setup, maintainer approval, SBOM/provenance,
      clean install proof, and API/schema stability gate.
- [ ] GAR-COMMERCIAL-1C compatibility scorecard and buyer-facing status page
  - Source: GAR-COMPAT-1A; universal compatibility scoreboard; known unsupported paths;
    website/status; public technical-preview readiness;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Status exists across docs and website/status, but users still have to infer maturity from
      architecture language.
    - Universal compatibility scoreboard exists as a report-only architecture doc.
  - Next slice outcome:
    - Publish a human-readable scorecard/status page organized by `supported`, `smoke-supported`,
      `report-only`, `blocked`, `planned`, and `not planned` so users can answer "Can I use this
      for X?" quickly.
  - User-visible surface:
    - `website/status.html`, README, website get-started, and compatibility docs.
  - Implementation scope:
    - Website/status generation, README links, status labels, compatibility scoreboard projection,
      and claim-safety validation. No runtime expansion.
  - Evidence required:
    - status refs: scoreboard row refs and known unsupported path refs.
    - claim refs: public-preview and release claim boundaries.
    - policy/no-fallback refs: fallback and external-engine status for supported/smoke rows.
    - freshness refs: source doc timestamp or generated artifact metadata.
  - Acceptance:
    - Users can answer "Can I use this for X?" in under 2 minutes.
    - Unsupported paths are not hidden.
    - Status labels distinguish runtime support from report-only/planned posture.
    - Public page does not imply production, performance, Spark replacement, SQL/DataFrame,
      object-store/lakehouse, Foundry, package, or external platform readiness.
  - Verification:
    - `python scripts/check_website_readiness.py`
    - website static asset validation if generated pages change.
    - release readiness metadata tests.
    - `git diff --check`
  - Non-goals:
    - No runtime expansion, benchmark rerun, package publication, or new claim.
  - Claim boundary:
    - Buyer-facing status is a maturity map, not a production support commitment.
  - Fallback boundary:
    - Status rows must preserve no-fallback/no-external-engine posture and must not hide unsupported
      diagnostics.
  - Dependencies/blockers:
    - GAR-COMPAT-1A typed scoreboard projection, known unsupported path ownership, website generator,
      and claim-safety checks.
- [ ] GAR-COMMERCIAL-1D enterprise evidence export pack
  - Source: GAR-NOVEL-1B; GAR-NOVEL-1C; operational evidence policy; OpenLineage; OpenTelemetry;
    ShardLoom evidence envelope;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Evidence is ShardLoom-native JSON.
    - OpenLineage and OpenTelemetry mappings are planned/report-only.
    - No export pack, backend integration, or network exporter exists.
  - Next slice outcome:
    - Define an opt-in enterprise evidence export pack containing ShardLoom JSON, OpenLineage facets,
      OpenTelemetry spans/metrics, and an optional Markdown summary.
  - User-visible surface:
    - Docs, future CLI export command docs, enterprise evaluation checklist, and sample local
      artifact bundle.
  - Implementation scope:
    - Export pack design, redaction policy, local artifact layout, capability/status rows, and tests.
      Do not add network export or backend integration in this slice.
  - Evidence required:
    - ShardLoom JSON evidence refs.
    - OpenLineage facet mapping refs.
    - OpenTelemetry span/metric mapping refs.
    - Markdown summary refs.
    - redaction, retention, export opt-in, and no-network policy refs.
    - policy/no-fallback refs.
  - Acceptance:
    - Export is opt-in.
    - No network calls happen by default.
    - Secret/path/query/schema/sample redaction policy exists.
    - Export pack does not upgrade support or claim status.
  - Verification:
    - release readiness metadata tests.
    - evidence artifact safety tests if report fields change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No lineage backend integration, OTel exporter, collector config, network call, external
      service dependency, Foundry claim, or production observability claim.
  - Claim boundary:
    - Commercial value is evidence portability into existing stacks; it is not production readiness
      or managed-platform certification.
  - Fallback boundary:
    - Evidence export cannot invoke external engines, external services, credentials, object-store
      I/O, or fallback execution by default.
  - Dependencies/blockers:
    - GAR-NOVEL-1B/C mappings, evidence artifact safety, redaction/retention policy, and explicit
      opt-in config.
- [ ] GAR-COMMERCIAL-1E Foundry dev-stack starter kit
  - Source: RFC 0036; Foundry proof-of-use docs; local Foundry-style transform example;
    GAR-GEN-1F; GAR-COMMERCIAL-1A;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Foundry proof is local/style-only.
    - Real Foundry runtime proof is still needed.
    - Generated-output runtime is not first-class yet.
  - Next slice outcome:
    - Add a personal dev-stack starter that imports the package, resolves the CLI, demonstrates
      source-free generated-output posture, runs a staged input example, and writes or documents an
      evidence dataset output boundary.
  - User-visible surface:
    - Foundry docs, examples, README/website integration notes, and local proof transcript.
  - Implementation scope:
    - Docs, local-style example, proof-script metadata fields, expected output snapshots, and
      no-fallback/no-external-compute diagnostics. Do not invoke Foundry.
  - Evidence required:
    - package/CLI resolution refs.
    - generated-output posture refs.
    - staged input refs.
    - evidence dataset output refs or deterministic blocker.
    - `foundry_runtime_invoked=false`, `foundry_compute_invoked=false`,
      `foundry_spark_invoked=false`.
    - `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - Clearly says no Foundry production claim.
    - No Spark fallback.
    - No external compute pushdown.
    - Evidence includes `foundry_runtime_invoked`, `foundry_compute_invoked`, and
      `foundry_spark_invoked` fields.
  - Verification:
    - Foundry proof tests if proof fields change.
    - release readiness metadata tests.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No Foundry Marketplace/package claim, Foundry invocation, credentials, direct S3 runtime,
      external compute, virtual table native execution, or production Foundry support.
  - Claim boundary:
    - Personal dev-stack starter only; no Foundry-native/certified/production claim.
  - Fallback boundary:
    - Foundry, Spark, external compute, S3/object-store, and platform services cannot execute
      unsupported ShardLoom work as fallback.
  - Dependencies/blockers:
    - GAR-GEN generated-output posture, Foundry proof field ownership, package proof, and future real
      Foundry environment evidence.
- [ ] GAR-COMMERCIAL-1F workflow recipes library
  - Source: RFC 0033; common ETL/user workflows; getting-started examples; benchmark scenarios;
    known unsupported paths; universal compatibility scoreboard;
    `docs/architecture/adoption-commercial-readiness-friction-reduction.md`.
  - Current state:
    - Examples exist, but recipes are not broad or organized around user adoption.
    - Current benchmark scenarios cover useful families such as dirty CSV, nested JSON, CDC overlay,
      prepared Vortex, result-sink replay, and unsupported diagnostics, but not all are exposed as
      copyable user recipes.
  - Next slice outcome:
    - Add recipe docs for generated reference table, dirty CSV cleanup, nested JSON extraction, CDC
      overlay, prepared Vortex query, local result-sink replay, unsupported diagnostic example, and
      object-store blocked example.
  - User-visible surface:
    - `docs/getting-started/`, README, website field guide/get-started/status pages, and examples.
  - Implementation scope:
    - Recipe docs, minimal commands/code, expected outputs, evidence field checklist, claim boundary,
      and validation docs. Runtime expansion is out of scope.
  - Evidence required:
    - recipe refs: command/code path and expected output artifact.
    - evidence refs: execution mode, materialization/decode, Native I/O, generated-source where
      relevant, output sink, claim gate, no-fallback fields.
    - unsupported refs: deterministic blocker diagnostics for blocked recipes.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - Each recipe includes user goal, code, expected output, evidence fields, and claim boundary.
    - Recipes reduce adoption friction by showing real workflows without hiding unsupported paths.
    - Blocked recipes are useful diagnostics examples, not fake success paths.
  - Verification:
    - docs link/readiness checks.
    - example smoke tests if executable examples change.
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No broad SQL/DataFrame runtime, object-store runtime, Foundry runtime, package publication,
      benchmark recomputation, performance claim, or production ETL claim.
  - Claim boundary:
    - Recipes demonstrate scoped local technical-preview workflows and blocked diagnostics only.
  - Fallback boundary:
    - Recipes must not call Spark, DataFusion, DuckDB, Polars, pandas, external databases, object
      stores, or Foundry as hidden fallback execution.
  - Dependencies/blockers:
    - GAR-GEN for generated reference table runtime, prepared/native smoke stability, website
      recipe routing, and known unsupported path freshness.

#### GAR-DOCS-1 - Non-Expert Use Case Atlas

These slices make ShardLoom explainable to non-experts without requiring readers to inspect the
phase plan, RFCs, or benchmark internals. The atlas must answer whether a use case is supported,
how to try it, what evidence appears, and what remains unsupported. Documentation must stay
claim-safe and keep `ready_local`, `smoke_supported`, `report_only`, `planned`, `blocked`, and
`unsupported` statuses distinct.

- [ ] GAR-DOCS-1A non-expert use-case atlas
  - Source:
    - README technical-preview posture.
    - `docs/getting-started/first-10-minutes.md`.
    - `docs/getting-started/examples.md`.
    - `docs/getting-started/certified-local-workload.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/use-cases/README.md`.
    - `docs/use-cases/use-case-index.yml`.
    - RFC 0010 developer experience and RFC 0033 workflows.
  - Current state:
    - Getting-started, benchmark, Python, Foundry, and architecture docs exist, but non-expert
      readers still need to infer which use case maps to which surface.
    - Initial Use Case Atlas scaffolding can describe status and references, but it is not yet a
      generated per-use-case documentation system.
  - Next slice outcome:
    - Expand the atlas into a stable non-expert landing document that summarizes every capability
      family, status, runnable path or blocker, evidence field family, and claim boundary.
  - User-visible surface:
    - `docs/use-cases/README.md`, README links, getting-started docs, future website status routing,
      and capability docs.
  - Implementation scope:
    - Use-case atlas README, use-case index maintenance, reference path normalization, and README or
      getting-started cross-links. Runtime code is out of scope.
  - Evidence required:
    - coverage refs: one use case per capability family.
    - reference refs: every use case maps to at least one exact repo path.
    - runnable refs: every `ready_local` or `smoke_supported` row has a command or code snippet.
    - blocker refs: every `planned`, `blocked`, `unsupported`, or `report_only` row explains the
      missing evidence.
    - policy/no-fallback refs: no use case hides fallback or external-engine invocation.
  - Acceptance:
    - A non-expert can answer "Can ShardLoom do my thing?", "How do I try it?", "What evidence do I
      get?", and "What is not supported yet?" from the atlas.
    - The atlas covers onboarding, local file ETL, compatibility import certified, prepared/native
      Vortex, Python wrapper/client, SQL/DataFrame posture, source-free generation, messy data,
      query scenarios, output/fanout, object-store, table/lakehouse, Foundry, evidence/claim gates,
      benchmark interpretation, and package channels.
    - No unsupported surface is advertised as runtime-supported.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, generated website pages, package publication, benchmark recomputation,
      performance claim, Spark-replacement claim, broad SQL/DataFrame claim, object-store/lakehouse
      claim, or Foundry production claim.
  - Claim boundary:
    - The atlas is a navigation and explanation surface only; it cannot upgrade support status or
      claim grade.
  - Fallback boundary:
    - Atlas examples must preserve explicit `fallback_attempted=false` and
      `external_engine_invoked=false` semantics and must never route unsupported work through
      external engines.
  - Dependencies/blockers:
    - Current capability docs, stable use-case status vocabulary, claim-safety review, and reference
      file freshness.
  - Ledger rule:
    - Move the completed atlas expansion session to
      `docs/architecture/phased-execution-completed-ledger.md` with validation output.

- [ ] GAR-DOCS-1B machine-readable use-case index
  - Source:
    - GAR-DOCS-1A.
    - `docs/use-cases/use-case-index.yml`.
    - `scripts/check_use_case_index.py`.
    - `scripts/check_use_case_coverage.py`.
  - Current state:
    - The initial index is a constrained dependency-free YAML-style file with use-case rows and
      validators.
    - The index is not yet projected into website/status, Python capability views, or generated
      per-use-case pages.
  - Next slice outcome:
    - Harden the use-case index schema and validation as a stable docs contract.
  - User-visible surface:
    - docs, future website "Can I use this?" status matrix, README/getting-started links, and
      eventual generated pages.
  - Implementation scope:
    - Index schema, validation scripts, field naming, status normalization, exact-reference checks,
      related-use-case checks, and claim-boundary checks.
  - Evidence required:
    - schema refs: required fields for title, audience, status, execution mode, engine mode, inputs,
      outputs, evidence fields, claim boundary, runnable example or blocker, expected evidence,
      common mistakes, `references`, and related use cases.
    - coverage refs: every capability family maps to at least one use case.
    - policy refs: no forbidden positive claim phrases.
  - Acceptance:
    - Adding a use case without references, evidence fields, claim boundary, runnable example for
      supported smoke, or blocker for planned/blocked paths fails validation.
    - The allowed statuses remain limited to `ready_local`, `smoke_supported`, `report_only`,
      `planned`, `blocked`, and `unsupported`.
    - Exact repo paths are required; wildcard references do not pass validation.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `python -m compileall -q scripts`
    - `git diff --check`
  - Non-goals:
    - No runtime feature changes, website generator, external YAML dependency, package publication,
      or capability promotion.
  - Claim boundary:
    - A machine-readable row is capability documentation, not evidence that a path is supported.
  - Fallback boundary:
    - Index rows must expose no-fallback/no-external-engine fields where relevant and may not point
      to external baselines as ShardLoom fallback execution.
  - Dependencies/blockers:
    - Stable schema review and agreement on whether future generated pages consume this file
      directly.
  - Ledger rule:
    - Move completed schema hardening to the completed ledger with validator output.

- [ ] GAR-DOCS-1C use-case page generator
  - Source:
    - GAR-DOCS-1A and GAR-DOCS-1B.
    - `docs/use-cases/templates/use-case-template.md`.
    - Existing website field-guide generation patterns.
  - Current state:
    - A page template exists.
    - Generated docs and website pages must stay reproducible from the use-case index.
  - Next slice outcome:
    - Add a deterministic generator that renders one Markdown page and one static website page per
      use case from the index and template.
  - User-visible surface:
    - `docs/use-cases/generated/<id>.md`, `website/use-cases/index.html`,
      `website/use-cases/<id>.html`, README/getting-started links, and website navigation.
  - Implementation scope:
    - Generator script, generated Markdown output directory, stable slugs, backlink/index updates,
      deterministic formatting, and validation.
  - Evidence required:
    - generated-page refs: one output page per use-case id.
    - template refs: all required sections render with status-specific text.
    - reference refs: page backlinks to exact source files.
    - policy refs: blocked/planned pages show blockers before examples or claims.
  - Acceptance:
    - Generated pages are reproducible from `use-case-index.yml`.
    - Every generated page includes plain-English summary, status table, quick example or blocked
      explanation, internal flow, expected evidence fields, common mistakes, related use cases, and
      reference files.
    - Supported/smoke pages include runnable commands or snippets.
    - Planned/blocked/report-only pages include blockers and do not imply runtime support.
    - Regeneration produces a clean diff when no index content changed.
  - Verification:
    - `python website/build_static_pages.py`
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `python -m compileall -q scripts`
    - `git diff --check`
  - Non-goals:
    - No website publication, runtime execution, benchmark recomputation, external dependency, or
      package release.
  - Claim boundary:
    - Generated pages inherit index claim boundaries and cannot add unindexed claims.
  - Fallback boundary:
    - Page generation is docs-only and must not execute ShardLoom, datasets, object stores, or
      external engines.
  - Dependencies/blockers:
    - GAR-DOCS-1B schema stability and template review.
  - Ledger rule:
    - Move generator implementation and generated-page validation to the completed ledger.

- [ ] GAR-DOCS-1D all-capabilities documentation coverage gate
  - Source:
    - GAR-DOCS-1B.
    - Python capability surfaces.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/architecture/universal-compatibility-coverage-scoreboard.md`.
    - `scripts/check_use_case_coverage.py`.
  - Current state:
    - The initial coverage validator checks the 16 top-level capability families.
    - It does not yet compare against every CLI/Python capability surface or universal compatibility
      scoreboard row.
  - Next slice outcome:
    - Add an all-capabilities coverage gate that fails when a public capability family, surface, or
      known unsupported boundary lacks a use-case row.
  - User-visible surface:
    - docs validation, release readiness, README/status docs, and future website status matrix.
  - Implementation scope:
    - Coverage script expansion, capability-source inventory, ignored/internal-only surface list,
      validation docs, and release/readiness integration if appropriate.
  - Evidence required:
    - capability refs: CLI/Python capability scopes and universal compatibility categories.
    - coverage refs: each public surface maps to a use-case id and status.
    - blocker refs: unsupported/report-only surfaces include deterministic blocker explanations.
  - Acceptance:
    - New public capability rows cannot be added without use-case documentation coverage.
    - The checker covers every execution mode, every engine mode, supported input families,
      supported output families, blocked major families, evidence concepts, and every example
      directory.
    - Internal-only implementation details can be excluded only through an explicit documented
      allowlist.
    - The gate does not force runtime claims for planned or blocked capabilities.
  - Verification:
    - `python scripts/check_use_case_coverage.py`
    - release readiness metadata tests if integrated.
    - `git diff --check`
  - Non-goals:
    - No runtime capability expansion, website rendering, or package publication.
  - Claim boundary:
    - Coverage means documented posture, not supported runtime.
  - Fallback boundary:
    - Coverage checks must preserve no-fallback/no-external-engine wording for all supported and
      blocked paths.
  - Dependencies/blockers:
    - Stable capability-source inventory and agreement on internal-only exclusions.
  - Ledger rule:
    - Move coverage-gate completion to the completed ledger with validation output and any allowlist.

- [ ] GAR-DOCS-1E non-expert recipe library
  - Source:
    - GAR-COMMERCIAL-1F.
    - GAR-DOCS-1A through GAR-DOCS-1D.
    - `docs/getting-started/examples.md`.
    - `benchmarks/traditional_analytics/README.md`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
  - Current state:
    - Examples and benchmark scenarios exist, but recipes are not organized by non-expert workflow
      goals.
  - Next slice outcome:
    - Add a recipe library for common user goals, with code/commands, expected outputs, evidence
      fields, claim boundaries, and blocked examples.
  - User-visible surface:
    - `docs/use-cases/recipes/`, getting-started docs, README links, future website Field Guide.
  - Implementation scope:
    - Recipe docs for no-dataset smoke, local CSV certified result, local Parquet certified result,
      prepared Vortex batch run, native Vortex input, source-free generated reference table,
      dirty CSV cleanup, nested JSON scan, CDC overlay, output fanout, object-store blocked
      diagnostic, Foundry dev-stack smoke, and benchmark evidence interpretation.
  - Evidence required:
    - runnable refs for current local/smoke recipes.
    - expected-output refs for artifacts or diagnostic reports.
    - claim-boundary refs for each recipe.
    - no-fallback refs for every command path.
  - Acceptance:
    - Recipes are short enough for non-experts and explicit enough for agents.
    - Every recipe has user goal, command or code snippet, expected output, evidence fields, claim
      boundary, and reference links.
    - Blocked recipes are presented as useful diagnostics, not fake success paths.
    - Every recipe links back to its use-case index id.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - recipe link/readiness checks once added.
    - example smoke tests only when executable examples change.
    - `git diff --check`
  - Non-goals:
    - No new runtime scenarios, benchmark recomputation, package publication, performance claim, or
      production workflow claim.
  - Claim boundary:
    - Recipes demonstrate scoped local technical-preview behavior and deterministic blockers only.
  - Fallback boundary:
    - Recipes must not call Spark, DataFusion, DuckDB, Polars, pandas, object stores, databases, or
      Foundry as hidden fallback execution.
  - Dependencies/blockers:
    - GAR-DOCS-1B schema stability and freshness of benchmark scenario docs.
  - Ledger rule:
    - Move completed recipe docs to the completed ledger with validation output.

- [ ] GAR-DOCS-1F non-expert field guide glossary
  - Source:
    - `docs/architecture/canonical-terminology.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - Website field guide pages.
    - GAR-DOCS-1A.
  - Current state:
    - Canonical terminology exists, and website field-guide pages explain selected concepts.
    - Non-expert docs still need concise definitions attached to use cases.
  - Next slice outcome:
    - Add a glossary layer for non-expert use-case terms such as execution mode, engine mode,
      Vortex-native, compatibility import, prepared Vortex, native Vortex, direct transient, no
      fallback, materialization boundary, Native I/O certificate, result-sink replay, claim gate,
      fixture smoke, report-only, external baseline, residual-native, encoded-native,
      source-state reuse, and output-plan reuse.
  - User-visible surface:
    - `docs/use-cases/field-guide/`, generated use-case pages, website field guide/status pages.
  - Implementation scope:
    - Glossary doc, glossary links from use-case pages, reference-file citations, and claim-safe
      wording review.
  - Evidence required:
    - term refs: each glossary term cites canonical docs or source references.
    - backlink refs: use cases link to relevant terms.
    - claim refs: definitions avoid support-status upgrades.
  - Acceptance:
    - A non-expert can read any use-case page without opening an RFC to decode key terms.
    - Every required term has one-sentence explanation, why it matters, how to inspect it, related
      use cases, and reference files.
    - Terms are aligned with canonical terminology and compute-flow docs.
    - Glossary entries clearly distinguish supported, smoke-supported, report-only, planned, and
      blocked concepts.
  - Verification:
    - `python scripts/check_use_case_glossary.py`
    - use-case validators.
    - docs link/readiness checks once available.
    - `git diff --check`
  - Non-goals:
    - No terminology rename, runtime behavior, API change, or claim promotion.
  - Claim boundary:
    - Glossary explanations are educational and cannot imply new support.
  - Fallback boundary:
    - Terms must preserve no-fallback/no-external-engine policy semantics.
  - Dependencies/blockers:
    - Canonical terminology freshness and generated-page backlink support.
  - Ledger rule:
    - Move completed glossary work to the completed ledger with validation output.

- [ ] GAR-DOCS-1G reference citation and backlink system
  - Source:
    - GAR-DOCS-1A through GAR-DOCS-1F.
    - Exact reference-path requirement in `docs/use-cases/use-case-index.yml`.
    - Existing README/getting-started/benchmark/Foundry/Python docs.
  - Current state:
    - Use-case rows list exact `references`, but references are not yet rendered as a citation
      graph or backlinked from source docs.
  - Next slice outcome:
    - Add a citation/backlink system so use-case pages show source docs and source docs can point
      back to the relevant use cases.
  - User-visible surface:
    - Generated use-case pages, source docs citation blocks, README/getting-started navigation, and
      future website pages.
  - Implementation scope:
    - Citation model, backlink generation or manual backlink sections, validation that references
      exist, and stale-reference checks.
  - Evidence required:
    - citation refs: exact repo paths for every source reference.
    - backlink refs: each canonical non-expert source doc lists related use-case ids where useful.
    - validation refs: missing references fail local checks.
  - Acceptance:
    - Readers can move from a use case to canonical evidence docs and back.
    - Wildcard references are rejected in machine-readable data.
    - Backlinks do not turn source docs into a second active work queue.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_backlinks.py`
    - `git diff --check`
  - Non-goals:
    - No runtime changes, website deployment changes, external docs service, or package publication.
  - Claim boundary:
    - Citations provide provenance only and cannot convert planned/blocked rows into supported rows.
  - Fallback boundary:
    - Citation/backlink docs must preserve no-fallback policy and external-baseline-only language.
  - Dependencies/blockers:
    - GAR-DOCS-1C generated page shape and selected backlink strategy.
  - Ledger rule:
    - Move completed citation/backlink work to the completed ledger with validation output.

- [ ] GAR-DOCS-1H website "Can I use this?" status matrix
  - Source:
    - GAR-DOCS-1A through GAR-DOCS-1G.
    - `website/status.html`.
    - `website/field-guide/`.
    - `docs/use-cases/use-case-index.yml`.
    - `scripts/check_website_readiness.py`.
  - Current state:
    - Website status and field-guide pages exist, but they do not yet consume or mirror the complete
      Use Case Atlas.
  - Next slice outcome:
    - Add a claim-safe website status matrix that lets users answer "Can I use this?" by status,
      input type, output type, execution mode, evidence level, platform, and blocker.
  - User-visible surface:
    - `website/use-cases/index.html`, `website/use-cases/<id>.html`, `website/status.html`,
      `website/field-guide/`, `website/README.md`, sitemap, and website readiness validation.
  - Implementation scope:
    - Static website status matrix, optional generated static data snapshot from use-case index,
      nav links, copy, sitemap updates, asset/reference validation, and no-runtime-GitHub-fetch
      checks.
  - Evidence required:
    - status refs: every row maps to a use-case id and status.
    - evidence refs: supported/smoke rows show expected evidence fields.
    - blocker refs: planned/blocked/report-only rows show blockers.
    - claim refs: website copy preserves technical-preview boundaries.
  - Acceptance:
    - A non-expert can answer common support questions from the website in under two minutes.
    - Users can filter by status, input type, output type, execution mode, evidence level, and
      platform.
    - Every card links to a use-case page and blocked/planned cards remain visible.
    - Website status labels match the index vocabulary exactly.
    - No unsupported path is described as runtime-supported.
    - Website keeps benchmarks framed as evidence, not a leaderboard.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `python scripts/check_website_readiness.py`
    - `node website/validate_static_assets.js`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, benchmark recomputation, external JS framework, runtime GitHub fetch,
      package publication, performance claim, Spark-replacement claim, or production claim.
  - Claim boundary:
    - The website matrix is a public posture guide for a technical preview, not a production support
      or performance proof.
  - Fallback boundary:
    - Website examples and status rows must keep `fallback_attempted=false` and
      `external_engine_invoked=false` visible where relevant and must never route users to external
      engines as fallback.
  - Dependencies/blockers:
    - GAR-DOCS-1B schema stability, website generator/readiness ownership, and current website
      claim-safety checks.
  - Ledger rule:
    - Move website matrix completion to the completed ledger with website readiness output.

#### GAR-WEB-ATLAS-1 - Modal-Style Field Guide And Use Case Atlas

Source:
- Current ShardLoom website under `website/`, including homepage, Field Guide, benchmark telemetry,
  compute-flow, status, rendered README, local assets, and static validation.
- `docs/use-cases/README.md`, `docs/use-cases/use-case-index.yml`,
  `docs/use-cases/generated/`, and `docs/use-cases/field-guide/`.
- `docs/architecture/compute-engine-flow-reference.md`.
- `docs/benchmarks/local-taxonomy-benchmark.md` and
  `docs/benchmarks/baseline-comparison-boundary.md`.
- Modal GPU Glossary structural reference: category table of contents, atomic glossary entries,
  dense concept navigation, and contributor/source posture.
- Pagefind static-search docs for backend-free static search over built HTML.
- Astro/Starlight/content-collection docs for the later framework migration decision gate.

Goal:
Turn `shardloom.io` into a dense, searchable, source-linked technical atlas for auditable compute:
concept-first like a technical glossary, workflow-first like a use-case atlas, and still aligned to
ShardLoom's original retro-future command-deck / field-guide identity.

Technique transfer:
- Use glossary information architecture patterns: category table of contents, atomic entries,
  reading paths, crosslinked technical dossiers, source references, and search.
- Do not copy Modal text, CSS, layout code, imagery, typography, brand identity, or trade dress.
- Do not copy Fallout, Bethesda, Pip-Boy, Vault-Tec, Vortex, Palantir, Apache, or other third-party
  brand assets or trade dress.

Runtime boundary:
Website and atlas work must not change ShardLoom runtime behavior, benchmark data, package
publication state, execution claims, fallback policy, or release gates.

- [ ] GAR-WEB-ATLAS-1A Field Guide taxonomy expansion
  - Source:
    - `website/field-guide/index.html`.
    - `website/build_static_pages.py`.
    - `docs/use-cases/field-guide/README.md`.
    - `docs/architecture/canonical-terminology.md`.
    - Modal GPU Glossary category/table-of-contents structure.
  - Current state:
    - The website has a useful Field Guide index and retro-future visual components.
    - The current Field Guide is still too small and card-oriented to behave like a full technical
      atlas.
  - Next slice outcome:
    - Add a complete ShardLoom Field Guide taxonomy with category groups and at least 50 planned or
      initial entries.
  - User-visible surface:
    - `/field-guide/`, website navigation, sitemap, status/readme cross-links, and future search
      indexing.
  - Implementation scope:
    - Add `website/content/field-guide-index.yml` or the existing equivalent content source.
    - Regenerate `website/field-guide/index.html`.
    - Preserve existing dossier links and ShardLoom visual identity.
  - Evidence required:
    - taxonomy refs: every entry has title, slug, category, summary, status, related terms, related
      use cases, and exact reference files.
    - claim refs: entries preserve technical-preview support posture.
    - source refs: Modal is used only as an information-architecture reference, not copied design.
  - Acceptance:
    - Field Guide index is organized by category, not only as a flat card grid.
    - Required categories exist: Start Here, Execution Modes, Engine Modes, Vortex Runtime,
      Evidence And Claims, Benchmark Telemetry, User Workflows, I/O And Output, Platform
      Boundaries, Performance Architecture, and Release And Trust.
    - Existing Field Guide URLs still resolve.
    - No unsupported path is described as runtime-supported.
  - Verification:
    - `python website/build_static_pages.py`
    - `node website/validate_static_assets.js`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No Pagefind integration, Astro migration, runtime behavior, benchmark recomputation, package
      publication, or new support claim.
  - Claim boundary:
    - Taxonomy entries are navigation and explanation only; they cannot upgrade any support or
      claim status.
  - Fallback boundary:
    - Entries must preserve no-fallback/no-external-engine language and must not point unsupported
      work to external engines as fallback.
  - Dependencies/blockers:
    - Current Field Guide generator shape, canonical terminology freshness, and use-case ids.
  - Ledger rule:
    - Move completed taxonomy expansion to the completed ledger with website readiness output.

- [ ] GAR-WEB-ATLAS-1B Field Guide dossier page template
  - Source:
    - GAR-WEB-ATLAS-1A.
    - Existing website Field Guide pages.
    - `docs/use-cases/templates/use-case-template.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
  - Current state:
    - Field Guide concepts exist, but the per-term page structure is not yet a full reusable dossier
      system.
  - Next slice outcome:
    - Add one reusable dossier template for generated or hand-authored Field Guide terms.
  - User-visible surface:
    - `/field-guide/<slug>`, related use-case pages, search results, and reference links.
  - Implementation scope:
    - Add or update the Field Guide dossier template, content schema, generated static pages, and
      website readiness validation.
  - Evidence required:
    - template refs: every dossier renders plain-English meaning, why it matters, how ShardLoom uses
      it, current support, evidence fields, what it does not claim, related use cases, related
      concepts, and reference files.
    - status refs: every dossier has explicit support posture and claim boundary.
    - source refs: references use exact repo paths.
  - Acceptance:
    - Every dossier can be understood by a non-expert without reading an RFC first.
    - Every dossier has exact reference files and related concepts.
    - Every claim-sensitive dossier states what ShardLoom does not claim.
    - Generated pages remain deterministic.
  - Verification:
    - `python website/build_static_pages.py`
    - `node website/validate_static_assets.js`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, package publication, benchmark recomputation, Pagefind integration, or
      framework migration.
  - Claim boundary:
    - Dossiers explain current posture and cannot create performance, production, Spark-replacement,
      SQL/DataFrame, object-store/lakehouse, Foundry, or package-publication claims.
  - Fallback boundary:
    - Dossiers must keep external engines as baselines/oracles only and never fallback execution.
  - Dependencies/blockers:
    - GAR-WEB-ATLAS-1A taxonomy and stable reference-file paths.
  - Ledger rule:
    - Move completed template work to the completed ledger with generated-page validation output.

- [ ] GAR-WEB-ATLAS-1C Field Guide category TOC and reading paths
  - Source:
    - GAR-WEB-ATLAS-1A and GAR-WEB-ATLAS-1B.
    - Current `website/field-guide/index.html`.
    - Modal GPU Glossary dense category TOC pattern.
  - Current state:
    - Field Guide navigation is useful but still reads more like a project-site card grid than a
      glossary table of contents.
  - Next slice outcome:
    - Add a dense category TOC and reading-path entrypoints above the raw card grid.
  - User-visible surface:
    - `/field-guide/`, homepage Field Guide preview, status/use-case cross-links, and mobile
      navigation.
  - Implementation scope:
    - Field Guide index template/content, CSS for compact glossary rows, category anchors, and
      reading-path cards or rows.
  - Evidence required:
    - navigation refs: every category anchor resolves.
    - reading-path refs: each path links to relevant dossiers and use cases.
    - claim refs: reading paths preserve support-status language.
  - Acceptance:
    - Users can jump directly to each concept category before scrolling through cards.
    - Required reading paths exist: New to ShardLoom; run a local workflow; understand benchmarks;
      understand Vortex-native paths; use Python/SQL/DataFrame; know what is blocked; and
      Foundry/platform context.
    - Mobile remains readable and does not hide blocked/report-only states.
  - Verification:
    - `python website/build_static_pages.py`
    - `node website/validate_static_assets.js`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No search backend, external framework, benchmark data changes, runtime changes, or new
      capability claims.
  - Claim boundary:
    - Reading paths are educational navigation only and must not imply production support.
  - Fallback boundary:
    - Reading paths must not route blocked work through external engines or external services.
  - Dependencies/blockers:
    - Field Guide taxonomy and use-case page availability.
  - Ledger rule:
    - Move completed TOC/reading-path work to the completed ledger with website readiness output.

- [ ] GAR-WEB-ATLAS-1D static Field Guide search with Pagefind
  - Source:
    - Pagefind docs.
    - `website/field-guide/`, `website/use-cases/`, `website/status.html`,
      `website/benchmarks.html`, and `website/compute-engine-flow.html`.
    - `wrangler.toml` static asset deployment.
  - Current state:
    - The site has static pages and validation, but no first-class search across concepts, use
      cases, evidence fields, and status rows.
  - Next slice outcome:
    - Add a static search lane, preferably Pagefind, after static HTML generation.
  - User-visible surface:
    - Search box on `/field-guide/`, optional global search trigger, search result pages/assets, and
      Cloudflare static assets.
  - Implementation scope:
    - Build script integration, committed or generated search assets as policy decides, CSP/header
      validation, local asset checks, and search UI styling.
  - Evidence required:
    - search refs: index covers Field Guide, Use Cases, Status, Benchmarks, and Compute Flow.
    - asset refs: search bundle/assets are local static assets.
    - policy refs: no runtime GitHub fetch or external search SaaS.
  - Acceptance:
    - Search works without backend infrastructure.
    - Results include concepts, use cases, evidence fields, references, and status labels.
    - Filtering by category or status is included when feasible; otherwise the blocker is explicit.
    - No network dependency is introduced at page-render time.
  - Verification:
    - `python website/build_static_pages.py`
    - Pagefind indexing command selected by the implementation.
    - `node website/validate_static_assets.js`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - No server-side search, external search service, Astro migration, runtime code, benchmark
      recomputation, or package publication.
  - Claim boundary:
    - Search discoverability does not imply support or claim grade for indexed terms.
  - Fallback boundary:
    - Search results must preserve blocked/report-only labels and must not hide no-fallback
      boundaries.
  - Dependencies/blockers:
    - Field Guide and Use Case generated pages, dependency/license review for adding Pagefind, and
      deployment asset policy.
  - Ledger rule:
    - Move completed search integration to the completed ledger with indexing and website readiness
      output.

- [ ] GAR-WEB-ATLAS-1E Use Case Atlas integration
  - Source:
    - GAR-DOCS-1A through GAR-DOCS-1H.
    - `docs/use-cases/use-case-index.yml`.
    - `docs/use-cases/generated/`.
    - `website/use-cases/`.
  - Current state:
    - The Use Case Atlas exists in docs and generated pages, but the website Field Guide is not yet
      the primary cross-linked entrypoint into those workflows.
  - Next slice outcome:
    - Connect Field Guide terms and use-case pages bidirectionally.
  - User-visible surface:
    - `/field-guide/`, `/field-guide/<slug>`, `/use-cases/`, `/use-cases/<id>`, `/status`, and
      website navigation.
  - Implementation scope:
    - Term-to-use-case links, use-case-to-term links, generated page metadata, index cards, sitemap,
      and readiness checks.
  - Evidence required:
    - backlink refs: every use case links to relevant terms and every relevant term links back to
      use cases.
    - status refs: ready/smoke/report-only/planned/blocked/unsupported remain distinct.
    - reference refs: all pages cite exact source docs.
  - Acceptance:
    - Required use-case categories are represented: onboarding, local file ETL, prepared/native
      Vortex, Python wrapper, SQL/DataFrame/report-only, source-free generated output, messy data,
      output/fanout, object-store/lakehouse boundaries, Foundry, benchmark interpretation, and
      package/release.
    - Blocked and report-only use cases remain visible.
    - Crosslinks are deterministic and validated.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `python scripts/check_use_case_backlinks.py`
    - `python website/build_static_pages.py`
    - `python scripts/check_website_readiness.py`
    - `node website/validate_static_assets.js`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, website framework migration, package publication, benchmark
      recomputation, or capability promotion.
  - Claim boundary:
    - Crosslinking improves navigation only and cannot change use-case support status.
  - Fallback boundary:
    - Use-case and term pages must keep fallback/external-engine fields explicit where relevant.
  - Dependencies/blockers:
    - Stable use-case ids, generated use-case page shape, and Field Guide taxonomy.
  - Ledger rule:
    - Move completed integration to the completed ledger with use-case and website validation output.

- [ ] GAR-WEB-ATLAS-1F Can-I-use-this status matrix
  - Source:
    - GAR-DOCS-1H.
    - `website/status.html`.
    - `docs/use-cases/use-case-index.yml`.
    - `docs/architecture/universal-compatibility-coverage-scoreboard.md`.
  - Current state:
    - Website status exists, but non-experts still need a compact matrix for capability, status,
      input, output, execution mode, evidence level, platform, and references.
  - Next slice outcome:
    - Add or refine a filterable public status matrix that answers "Can I use ShardLoom for X?"
      without requiring phase-plan reading.
  - User-visible surface:
    - `/status`, `/use-cases/`, homepage status links, and Field Guide related-status links.
  - Implementation scope:
    - Static data snapshot or generated table, filters, status chips, reference links, blocked-path
      visibility, sitemap, and readiness validation.
  - Evidence required:
    - status refs: every matrix row maps to a use-case id or scoreboard row.
    - blocker refs: planned/blocked/report-only rows explain missing evidence.
    - reference refs: every row links exact source docs.
    - policy refs: no unsupported row is described as runtime-supported.
  - Acceptance:
    - Users can filter by status, input type, output type, execution mode, evidence level, and
      platform.
    - S3/object-store, lakehouse/table, Foundry, SQL/DataFrame, package/release, and benchmark
      claim boundaries are explicit.
    - Blocked and report-only states are visible rather than hidden.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_coverage.py`
    - `python website/build_static_pages.py`
    - `python scripts/check_website_readiness.py`
    - `node website/validate_static_assets.js`
    - `git diff --check`
  - Non-goals:
    - No runtime support expansion, object-store runtime, lakehouse/table commit, Foundry runtime,
      package publication, benchmark rerun, or production claim.
  - Claim boundary:
    - The matrix is a public technical-preview posture guide, not a support, production, or
      performance guarantee.
  - Fallback boundary:
    - Matrix rows must preserve `fallback_attempted=false` and `external_engine_invoked=false`
      semantics where applicable and must not represent external baselines as ShardLoom execution.
  - Dependencies/blockers:
    - Stable use-case index, compatibility scoreboard freshness, and website filter implementation.
  - Ledger rule:
    - Move completed status-matrix work to the completed ledger with website readiness output.

- [ ] GAR-WEB-ATLAS-1G source-linked reference and citation blocks
  - Source:
    - GAR-DOCS-1G.
    - `docs/use-cases/use-case-index.yml`.
    - `docs/use-cases/reference-backlinks.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/benchmarks/baseline-comparison-boundary.md`.
  - Current state:
    - Use cases and docs include references, but the website atlas does not yet render systematic
      citation blocks for every dossier and workflow page.
  - Next slice outcome:
    - Add source-linked citation blocks to every Field Guide dossier and use-case page.
  - User-visible surface:
    - `/field-guide/<slug>`, `/use-cases/<id>`, `/status`, and rendered docs/readme pages.
  - Implementation scope:
    - Citation data model, generated reference blocks, "what this proves" labels, backlink checks,
      and stale-reference validation.
  - Evidence required:
    - citation refs: every cited source is an exact repo path or approved external documentation
      reference.
    - proof refs: each citation states what posture or definition it supports.
    - claim refs: citations do not create support status by themselves.
  - Acceptance:
    - Every dossier and use-case page has a `Reference files` block.
    - Reference blocks explain what the source proves.
    - No page uses vague "see docs" references.
    - Missing local references fail validation.
  - Verification:
    - `python scripts/check_use_case_index.py`
    - `python scripts/check_use_case_backlinks.py`
    - `python website/build_static_pages.py`
    - `python scripts/check_website_readiness.py`
    - `node website/validate_static_assets.js`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, external docs service, package publication, benchmark rerun, or support
      status promotion.
  - Claim boundary:
    - Citations provide provenance only; claim status still comes from evidence gates and support
      posture fields.
  - Fallback boundary:
    - Citation text must preserve external-baseline-only and no-fallback policy language.
  - Dependencies/blockers:
    - Dossier template, use-case generated pages, and backlink strategy.
  - Ledger rule:
    - Move completed citation work to the completed ledger with validation output.

- [ ] GAR-WEB-ATLAS-1H Astro/Starlight migration decision gate
  - Source:
    - Current Python static generator and vanilla HTML/CSS/JS website.
    - Astro content collections documentation.
    - Astro Starlight documentation.
    - Page count and generated-page maintenance experience from GAR-WEB-ATLAS-1A through
      GAR-WEB-ATLAS-1G.
  - Current state:
    - The existing static generator works and should remain the short-term path.
    - A larger Field Guide/Use Case Atlas may eventually need schema-backed content collections,
      MDX, integrated docs navigation, and search.
  - Next slice outcome:
    - Add a report-only migration decision doc comparing the current generator, Astro custom site,
      and Astro Starlight.
  - User-visible surface:
    - Docs/architecture decision record only; no website runtime change in this slice.
  - Implementation scope:
    - Add `docs/architecture/website-atlas-framework-decision.md` or equivalent report-only doc,
      with criteria, risks, migration blockers, and recommendation.
  - Evidence required:
    - decision refs: page count, content schema needs, search needs, design flexibility, Cloudflare
      deployment compatibility, contributor workflow, dependency/license review, and maintenance
      cost.
    - source refs: Pagefind, Astro, and Starlight references are cited as candidate tooling only.
  - Acceptance:
    - The decision doc recommends one path for the next phase and explains why.
    - The default short-term recommendation remains current generator unless evidence justifies a
      migration.
    - Any migration remains blocked until explicitly approved by a later implementation slice.
  - Verification:
    - `python scripts/check_website_readiness.py` if website docs are linked.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `git diff --check`
  - Non-goals:
    - No Astro/Starlight migration, dependency addition, runtime code, benchmark data change,
      package publication, or production claim.
  - Claim boundary:
    - Framework choice does not imply product maturity, performance, or support readiness.
  - Fallback boundary:
    - Website framework decisions must not alter no-fallback runtime policy or introduce runtime
      external fetches.
  - Dependencies/blockers:
    - Page count and content-model evidence from earlier atlas slices.
  - Ledger rule:
    - Move completed decision gate to the completed ledger with the recommendation and validation
      output.

- [ ] GAR-WEB-ATLAS-1I visual density and readability refinement
  - Source:
    - `website/assets/site.css`.
    - Current Field Guide, benchmark, status, and use-case pages.
    - GAR-WEB-ATLAS-1A through GAR-WEB-ATLAS-1G.
  - Current state:
    - The site has strong command-deck/retro-future tokens and components.
    - Dense glossary/list views are not yet fully optimized for 80+ entries and crosslinks.
  - Next slice outcome:
    - Refine the visual system for high-density glossary and status browsing while preserving
      readability and original ShardLoom identity.
  - User-visible surface:
    - All website pages, especially `/field-guide/`, `/use-cases/`, `/status`, `/benchmarks`, and
      `/compute-engine-flow`.
  - Implementation scope:
    - CSS components for category TOC band, compact term row, dossier card, status chip, reference
      badge, related-concepts rail, sticky in-page TOC, and raw-data drawers.
  - Evidence required:
    - accessibility refs: contrast, keyboard navigation, readable mobile type, reduced-motion support
      if motion exists.
    - claim refs: visual hierarchy does not hide blocked/report-only status.
    - brand refs: original ShardLoom logo/visual tokens are used without third-party trade-dress
      copying.
  - Acceptance:
    - Field Guide can show 80+ entries without becoming overwhelming.
    - Header sizes and cards remain proportionate on desktop and mobile.
    - Blocked/report-only/planned states are visible.
    - No Modal/Fallout/Bethesda/third-party visual copying is introduced.
  - Verification:
    - `python website/build_static_pages.py`
    - `node website/validate_static_assets.js`
    - `python scripts/check_website_readiness.py`
    - browser/manual visual smoke where feasible.
    - `git diff --check`
  - Non-goals:
    - No external CSS framework, runtime behavior, benchmark recomputation, package publication, or
      capability promotion.
  - Claim boundary:
    - Visual polish cannot imply production readiness, performance, Spark replacement, SQL/DataFrame
      runtime, object-store/lakehouse runtime, Foundry production support, or package publication.
  - Fallback boundary:
    - Visual labels and badges must preserve no-fallback/no-external-engine semantics.
  - Dependencies/blockers:
    - Expanded taxonomy, dossier pages, use-case integration, and status matrix shape.
  - Ledger rule:
    - Move completed visual refinement to the completed ledger with website readiness and visual
      smoke notes.

- [ ] GAR-WEB-ATLAS-1J Field Guide / Use Case public-readiness gate
  - Source:
    - `scripts/check_website_readiness.py`.
    - `website/validate_static_assets.js`.
    - GAR-WEB-ATLAS-1A through GAR-WEB-ATLAS-1I.
    - Public technical-preview readiness docs.
  - Current state:
    - Website readiness checks exist for assets, local compute-flow snapshot, no raw GitHub fetches,
      metadata, and forbidden claims.
    - Atlas-specific dossier/use-case quality gates are not yet complete.
  - Next slice outcome:
    - Extend public-readiness validation for Field Guide and Use Case Atlas pages.
  - User-visible surface:
    - Public site quality, CI/checks, generated pages, status matrix, sitemap, and deployment safety.
  - Implementation scope:
    - Website readiness script, static asset validator, optional use-case/field-guide validators,
      generated metadata checks, and public-post readiness docs.
  - Evidence required:
    - metadata refs: every generated page has title, description, canonical URL, OG metadata, and
      local assets.
    - content refs: every dossier has status, references, claim boundary, and related concepts.
    - use-case refs: every use case has a runnable example or blocker explanation.
    - policy refs: no runtime `raw.githubusercontent.com` fetch, no forbidden claim phrases, no
      unsupported production claims, and no copied third-party brand/trade-dress references.
  - Acceptance:
    - Readiness fails when generated atlas pages omit required metadata, references, status,
      claim-boundary, or blocker/example content.
    - Readiness fails on runtime GitHub fetches or forbidden public claims.
    - Readiness covers Field Guide, Use Cases, Status, Benchmarks, Compute Flow, and rendered README.
  - Verification:
    - `python scripts/check_website_readiness.py`
    - `node website/validate_static_assets.js`
    - `python -m compileall -q scripts website`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `git diff --check`
  - Non-goals:
    - No runtime behavior, package publication, benchmark recomputation, external SEO service,
      network crawl, or framework migration.
  - Claim boundary:
    - Public-readiness means technical-preview-safe website posture, not release-launch,
      production, performance, or support readiness.
  - Fallback boundary:
    - Readiness checks must preserve no-fallback/no-external-engine policy and fail closed when
      wording blurs external baselines with ShardLoom execution.
  - Dependencies/blockers:
    - Expanded Field Guide and Use Case generated pages from earlier atlas slices.
  - Ledger rule:
    - Move completed readiness-gate work to the completed ledger with validator output.

- [ ] GAR-0010-B DataFrame/notebook and package surface readiness report
  - Source: RFC 0010; RFC 0024; RFC 0032.
  - Current state: package dry-run docs exist; mature DataFrame/notebook surfaces and publication are
    not claimable.
  - Next slice outcome: report-only readiness matrix for DataFrame/notebook APIs, package surface,
    examples, and unsupported diagnostics.
  - User-visible surface: docs, Python capability view, release gate.
  - Implementation scope: docs/report fields, Python package metadata checks, tests.
  - Evidence required: release/package refs and diagnostic/no-fallback refs.
  - Acceptance: readiness report distinguishes installed package smoke from runtime support.
  - Verification: release readiness metadata tests, Python compileall, default GAR verification.
  - Non-goals: no PyPI/Conda publication or DataFrame execution.
  - Fallback/claim boundary: no package-release or DataFrame-runtime claim.
  - Dependencies/blockers: GAR-0024 release/package slices.
- [ ] GAR-0022-A Substrait import/export report-only contract
  - Source: RFC 0022; plan IR docs; rfc-coverage followthrough.
  - Current state: native Plan IR exists; real Substrait import/export and imported-plan execution
    are not implemented.
  - Next slice outcome: deterministic report for parse/export support status, unsupported diagnostics,
    and imported-plan evidence requirements.
  - User-visible surface: CLI plan import/export, docs.
  - Implementation scope: plan portability report, CLI output, tests.
  - Evidence required: diagnostic/no-fallback refs; no execution evidence in this slice.
  - Acceptance: Substrait requests report support status without executing imported plans.
  - Verification: plan portability tests and default GAR verification.
  - Non-goals: no Substrait execution or dependency expansion.
  - Fallback/claim boundary: imported plans are not runtime-supported.
  - Dependencies/blockers: dependency/license approval for any parser library.
- [ ] GAR-0030-A universal harness execution gate
  - Source: RFC 0030; universal import/deployment baseline harness; RFC 0029.
  - Current state: universal harness is report-only; imported-plan execution needs capability,
    certificate, Native I/O, and no-fallback evidence.
  - Next slice outcome: gate that blocks harness execution until the evidence set is attached.
  - User-visible surface: CLI universal harness plan, docs, release gate.
  - Implementation scope: harness report fields, CLI output, tests.
  - Evidence required: capability refs, execution certificate refs, Native I/O refs, policy/no-fallback
    refs.
  - Acceptance: harness execution cannot be confused with environment/report readiness.
  - Verification: universal harness tests, release readiness metadata tests.
  - Non-goals: no harness execution, external engine invocation, or container publication.
  - Fallback/claim boundary: external baselines remain comparison-only.
  - Dependencies/blockers: GAR-0022 import/export status.
- [ ] GAR-0032-A SQL parser/binder report-only readiness
  - Source: RFC 0032; RFC 0010; rfc-coverage followthrough.
  - Current state: SQL capability concepts exist; SQL parser/binder/execution is not a broad runtime.
  - Next slice outcome: SQL text can be classified into parsed/bound/planned/unsupported diagnostics
    without execution.
  - User-visible surface: CLI capability output, docs, Python capability view.
  - Implementation scope: SQL capability report, diagnostic helpers, tests.
  - Evidence required: diagnostic/no-fallback refs.
  - Acceptance: SQL requests return deterministic support status and no runtime execution.
  - Verification: capability snapshot tests, diagnostic stability tests.
  - Non-goals: no SQL execution.
  - Fallback/claim boundary: `support_status=report_only|unsupported`.
  - Dependencies/blockers: parser dependency approval if a real parser is introduced.
- [ ] GAR-0032-C UDF and external-effect blocker matrix
  - Source: RFC 0032; RFC 0011; RFC 0019.
  - Current state: UDF/effectful operations are report-only/unsupported.
  - Next slice outcome: classify UDFs, API calls, LLM calls, embeddings, and external effects with
    permission/effect blockers.
  - User-visible surface: CLI capability view, docs, diagnostics.
  - Implementation scope: capability report, effect budget report, diagnostics, tests.
  - Evidence required: policy/security/no-fallback refs.
  - Acceptance: every external effect defaults to blocked and cannot execute without explicit policy.
  - Verification: effect budget tests, security policy tests.
  - Non-goals: no UDF, network, model, embedding, or external effect execution.
  - Fallback/claim boundary: no external-effect runtime claim.
  - Dependencies/blockers: GAR-0019 credential/policy and GAR-0023 sandbox.
- [ ] GAR-0032-D unstructured/media and universal adapter capability matrix
  - Source: RFC 0032; RFC 0033; RFC 0037.
  - Current state: unstructured/media/universal adapter surfaces are not executable broadly.
  - Next slice outcome: report-only matrix for documents, media, vectors, universal adapters, and
    source/sink metadata.
  - User-visible surface: CLI capability view, docs, Python view.
  - Implementation scope: capability report, docs, tests.
  - Evidence required: diagnostic/no-fallback refs and effect-policy refs.
  - Acceptance: no vector search, media extraction, or model call is implied by capability rows.
  - Verification: capability snapshot tests and default GAR verification.
  - Non-goals: no unstructured runtime.
  - Fallback/claim boundary: `support_status=report_only|unsupported`.
  - Dependencies/blockers: external-effect blocker matrix.
- [ ] GAR-0032-E best-default certification gate
  - Source: RFC 0032; operational evidence policy hardening; benchmark-suite catalog.
  - Current state: user capability and sufficiency reports exist; best-default claims are not
    claim-grade.
  - Next slice outcome: gate defining required correctness, benchmark, certificate, Native I/O,
    policy, release, and UX evidence before best-default language is allowed.
  - User-visible surface: CLI capability view, docs, release gate.
  - Implementation scope: certification gate report, tests, docs.
  - Evidence required: all claim evidence categories named in the gate.
  - Acceptance: missing evidence yields `claim_gate_status=not_claim_grade`.
  - Verification: certification gate tests, release readiness metadata tests.
  - Non-goals: no best-default claim.
  - Fallback/claim boundary: no best-default, performance, or replacement claim.
  - Dependencies/blockers: benchmark/correctness/release GAR-P5 slices.
- [ ] GAR-0033-A ETL workflow and data-quality capability slice
  - Source: RFC 0033; user data workflow docs; benchmark-suite catalog.
  - Current state: local workflow surfaces exist; mature joins, aggregations, windows, data-quality
    APIs, object-store/table runtime, and production ETL certification are not broad.
  - Next slice outcome: ETL workflow matrix for supported local paths, report-only APIs, and
    unsupported object-store/table paths.
  - User-visible surface: CLI workflow plan, Python docs, examples.
  - Implementation scope: workflow report, Python view, tests.
  - Evidence required: correctness refs for supported local paths, diagnostic/no-fallback refs for
    unsupported paths.
  - Acceptance: production ETL claims are blocked unless all evidence is attached.
  - Verification: workflow/table planning tests, Python compileall if touched.
  - Non-goals: no production ETL runtime or publication.
  - Fallback/claim boundary: local workflow claims only for already certified paths.
  - Dependencies/blockers: operator/table/output GAR slices.
- [ ] GAR-0034-A live/hybrid fabric blocker and freshness gate
  - Source: RFC 0034; live/hybrid event API docs; operational evidence policy hardening.
  - Current state: live/hybrid engines and freshness/exactly-once claims are planning/report-only.
  - Next slice outcome: gate for broker/state-store dependencies, object-store execution, freshness,
    exactly-once, and baseline/oracle boundaries.
  - User-visible surface: CLI live/hybrid plan, docs, release gate.
  - Implementation scope: fabric report fields, diagnostics, tests.
  - Evidence required: policy/no-fallback refs and freshness evidence if any lane is admitted.
  - Acceptance: all live/hybrid runtime claims default to not claim-grade.
  - Verification: live/hybrid contract tests, release readiness metadata tests.
  - Non-goals: no broker, state store, object-store runtime, or streaming production behavior.
  - Fallback/claim boundary: baselines/oracles remain comparison-only.
  - Dependencies/blockers: GAR-0013 streaming and GAR-0008 object-store gates.
- [ ] GAR-0035-A REST server/runtime unsupported contract
  - Source: RFC 0035; execution mode protocol parity; typed envelope docs.
  - Current state: REST/Event/API contracts are documented; HTTP listener, remote execution,
    Flight/ADBC bridge, broker integration, and dependency-expanded server are not implemented.
  - Next slice outcome: unsupported/runtime-readiness contract that mirrors CLI/Python fields and
    blocks server claims.
  - User-visible surface: REST/OpenAPI docs/schema, release readiness gate.
  - Implementation scope: schema/docs, contract tests, release metadata.
  - Evidence required: protocol parity refs and diagnostic/no-fallback refs.
  - Acceptance: REST docs expose shared mode fields and deterministic unsupported diagnostics.
  - Verification: default GAR verification plus schema snapshot tests if generated.
  - Non-goals: no HTTP listener, remote execution, Flight/ADBC, broker, or server dependency.
  - Fallback/claim boundary: REST remains report-only/unsupported for runtime execution.
  - Dependencies/blockers: GAR-FLOW-3A parity report.
- [ ] GAR-0036-A Foundry package and proof boundary matrix
  - Source: RFC 0036; Foundry integration pack docs; release docs.
  - Current state: local Foundry proof docs exist; production `shardloom-foundry`, package
    publication, service invocation, Artifact Repository publication, Compute Module, virtual-table
    native execution, dataset transaction runtime, and F10 deployment are not certified.
  - Next slice outcome: matrix separating local proof, package readiness, service invocation,
    virtual tables, dataset transaction, and deployment evidence.
  - User-visible surface: Foundry docs, release gate, example outputs.
  - Implementation scope: docs/report fields, proof script metadata, tests.
  - Evidence required: release/package refs, Native I/O refs for any native lane, policy/no-fallback
    refs.
  - Acceptance: Foundry external compute is never reported as ShardLoom execution.
  - Verification: Foundry proof script tests if touched, release readiness metadata tests.
  - Non-goals: no Foundry invocation, publication, or platform credential.
  - Fallback/claim boundary: Foundry remains optional integration, not a fallback engine.
  - Dependencies/blockers: package publication and credentials gates.
- [ ] GAR-0037-A wrapper and connector implementation registry
  - Source: RFC 0037; client wrapper architecture docs.
  - Current state: wrapper architecture is documented; generated clients, DB-API, SQLAlchemy, Ibis,
    dbt, Airflow, Dagster, Prefect, MCP, Flight, ADBC, and BI connectors are not implemented.
  - Next slice outcome: registry of wrappers/connectors with support status, transport, evidence,
    and unsupported diagnostics.
  - User-visible surface: docs, CLI capability view, Python package docs.
  - Implementation scope: registry/report docs, tests.
  - Evidence required: protocol parity refs and diagnostic/no-fallback refs.
  - Acceptance: connectors are not advertised as runtime-supported without implementation evidence.
  - Verification: docs/contract tests and Python compileall if models change.
  - Non-goals: no connector implementation.
  - Fallback/claim boundary: no wrapper ecosystem claim.
  - Dependencies/blockers: GAR-0035 REST/transport and GAR-0010 Python API.
- [ ] GAR-0039-A typed envelope migration and legacy field mirror closeout
  - Source: RFC 0039; typed command result envelope docs; agent contract pack.
  - Current state: typed output v2 exists; legacy flat `fields` mirror and some command families
    remain.
  - Next slice outcome: migrate one command family from legacy mirror reliance to typed refs or add
    deterministic blockers where migration is not ready.
  - User-visible surface: CLI JSON output, Python typed models, golden fixtures.
  - Implementation scope: CLI renderer/typed envelope, one command-family handler, Python model/test.
  - Evidence required: typed envelope snapshots and no-fallback refs.
  - Acceptance: migrated family exposes typed payloads without losing backward-compatible fields where
    still required.
  - Verification: typed envelope snapshot tests, Python tests, `cargo test --workspace --all-targets`.
  - Non-goals: no runtime behavior changes.
  - Fallback/claim boundary: output migration cannot imply new support.
  - Dependencies/blockers: command-family priority.
- [ ] GAR-0039-B golden fixtures, Foundry boundary fixture, and helper centralization
  - Source: RFC 0039; repo cleanup backlog; terminology consolidation backlog; diagnostics backlog.
  - Current state: some golden fixtures, Foundry boundary fixture, command/help registry,
    terminology mapping helpers, diagnostic constants, and report field helpers remain pending.
  - Next slice outcome: add one focused fixture/helper centralization slice with tests and no runtime
    behavior.
  - User-visible surface: CLI JSON snapshots, docs, agent contract.
  - Implementation scope: snapshot fixtures, helper module, tests.
  - Evidence required: snapshot refs and no-fallback refs.
  - Acceptance: helpers reduce duplication without changing public semantics.
  - Verification: focused snapshot tests, default GAR verification.
  - Non-goals: no command rename, public schema break, or runtime expansion.
  - Fallback/claim boundary: cleanup does not create support claims.
  - Dependencies/blockers: compatibility expectations for existing CLI outputs.

#### GAR-P4 - Extension, Governance, And Runtime Policy

- [ ] GAR-0011-A extension manifest and external-effect capability matrix
  - Source: RFC 0011; effect budget plan; RFC 0019.
  - Current state: extension manifests/effect budgets are represented; execution, UDFs, LLM/API calls,
    embeddings, and external effects are unsupported/report-only.
  - Next slice outcome: matrix for extension types, required permissions, materialization/effect
    metadata, and default blockers.
  - User-visible surface: CLI extension plan, capability view, docs.
  - Implementation scope: extension/effect report fields, diagnostics, tests.
  - Evidence required: policy/security/no-fallback refs.
  - Acceptance: all external effects default to blocked with deterministic diagnostics.
  - Verification: extension planning tests, effect budget tests.
  - Non-goals: no extension execution, network call, model call, or embedding runtime.
  - Fallback/claim boundary: no external-effect support claim.
  - Dependencies/blockers: GAR-0019 credential/policy.
- [ ] GAR-0019-A credential lifecycle and policy enforcement gate
  - Source: RFC 0019; operational evidence policy hardening; security docs.
  - Current state: security/policy reports exist; production credential lifecycle and runtime policy
    enforcement are not complete.
  - Next slice outcome: gate for credential resolution, secret loading, redaction, workspace policy,
    runtime permission checks, and unsupported diagnostics.
  - User-visible surface: CLI security/governance plan, release security gate.
  - Implementation scope: security report fields, CLI output, tests.
  - Evidence required: security policy refs, redaction/path-safety refs, no-fallback refs.
  - Acceptance: credential use defaults to denied unless a slice explicitly admits it with evidence.
  - Verification: security/path-safety tests, release security gate tests.
  - Non-goals: no secret loading, network credential use, or production policy runtime.
  - Fallback/claim boundary: no governed production runtime claim.
  - Dependencies/blockers: security release gate.
- [ ] GAR-0019-B sandbox and governance runtime readiness
  - Source: RFC 0019; RFC 0023; effect budget plan.
  - Current state: sandbox/governance concepts exist; sandbox execution is not a production runtime.
  - Next slice outcome: readiness report for sandbox isolation, filesystem/network permissions,
    audit logs, and deny-by-default behavior.
  - User-visible surface: CLI governance plan, docs, release gate.
  - Implementation scope: governance report, diagnostics, tests.
  - Evidence required: security/no-fallback refs and audit artifact refs.
  - Acceptance: sandbox-dependent work remains blocked until isolation evidence exists.
  - Verification: security contract tests and default GAR verification.
  - Non-goals: no sandbox process runtime.
  - Fallback/claim boundary: no sandbox execution claim.
  - Dependencies/blockers: plugin ABI and credential gates.
- [ ] GAR-0023-A plugin ABI loading and UDF sandbox blocker
  - Source: RFC 0023; RFC 0011; RFC 0019.
  - Current state: plugin ABI/sandbox/UDF execution are represented as planned/report-only surfaces.
  - Next slice outcome: ABI loading contract, sandbox evidence requirements, and UDF execution
    blockers.
  - User-visible surface: CLI plugin/extension plan, capability view.
  - Implementation scope: plugin report fields, diagnostics, tests.
  - Evidence required: sandbox refs, policy/no-fallback refs.
  - Acceptance: plugins cannot load or execute without explicit policy and sandbox evidence.
  - Verification: plugin/extension planning tests, security tests.
  - Non-goals: no dynamic loading or UDF execution.
  - Fallback/claim boundary: plugin support remains report-only.
  - Dependencies/blockers: GAR-0019 sandbox and credential gates.

#### GAR-P5 - Correctness, Benchmarks, Claims, And Release

- [ ] GAR-0001B-A engine-replacement claim inventory
  - Source: RFC 0001; RFC 0025; global architecture review.
  - Current state: Spark-displacement/engine-replacement claims are not claimable.
  - Next slice outcome: inventory mapping each replacement claim to required runtime, output,
    correctness, benchmark, certificate, Native I/O, and no-fallback evidence.
  - User-visible surface: release claim gate, docs.
  - Implementation scope: release gate docs/report, tests.
  - Evidence required: all claim categories as checklist refs; no execution evidence in inventory
    slice.
  - Acceptance: missing evidence yields `claim_gate_status=not_claim_grade`.
  - Verification: release readiness metadata tests and default GAR verification.
  - Non-goals: no replacement claim or benchmark rerun.
  - Fallback/claim boundary: no public displacement language.
  - Dependencies/blockers: GAR-0009 and GAR-0041 claim gates.
- [ ] GAR-0009-A Spark-displacement benchmark evidence matrix
  - Source: RFC 0009; benchmark competitive claim evidence; benchmark-suite catalog.
  - Current state: local benchmark evidence exists; broad Spark-displacement evidence and public
    performance claims are gated.
  - Next slice outcome: evidence matrix tying workloads, baselines/oracles, correctness, timing,
    environment, mode, and claim status.
  - User-visible surface: benchmark report, docs, release claim gate.
  - Implementation scope: benchmark metadata/report, docs, contract tests.
  - Evidence required: correctness refs, benchmark refs, policy/no-fallback refs, environment refs.
  - Acceptance: every claim row says claim-grade or not-claim-grade and explains why.
  - Verification: benchmark contract tests, release readiness metadata tests.
  - Non-goals: no public performance claim.
  - Fallback/claim boundary: external engines are comparison baselines/oracles only.
  - Dependencies/blockers: reproducible benchmark reruns and release gate.
- [ ] GAR-0015-A fuzz/property and semantic differential expansion
  - Source: RFC 0015; correctness differential harness; correctness fixture manifest.
  - Current state: selected correctness fixtures exist; fuzz/property expansion is not broad.
  - Next slice outcome: add one fixture family or report-only gap with expected reference artifacts.
  - User-visible surface: correctness harness, docs, release gate.
  - Implementation scope: fixtures, harness metadata, tests.
  - Evidence required: correctness refs, oracle/reference refs, no-fallback refs.
  - Acceptance: selected family has deterministic references or an explicit gap.
  - Verification: correctness harness tests, fixture manifest tests.
  - Non-goals: no superiority/performance claim.
  - Fallback/claim boundary: correctness expansion alone does not create performance claims.
  - Dependencies/blockers: operator/source coverage.
- [ ] GAR-0024-A publication and API/schema stability gate
  - Source: RFC 0024; release docs; workspace feature build matrix.
  - Current state: dry-run package proof exists; first public publication, stable API/schema windows,
    and signing decisions are not complete.
  - Next slice outcome: release gate rows for API/schema compatibility, package identities, signing,
    checksums, SBOM, and publication approval.
  - User-visible surface: release readiness check, docs.
  - Implementation scope: release scripts/docs/tests.
  - Evidence required: release/provenance/security refs, no-fallback refs.
  - Acceptance: gate fails closed without explicit publication evidence.
  - Verification: release readiness tests, dependency audit gate, provenance dry-run tests.
  - Non-goals: no package publication, tags, or signing key use.
  - Fallback/claim boundary: no public release claim.
  - Dependencies/blockers: security/provenance gates.
- [ ] GAR-0025-A competitive replacement sufficiency gate
  - Source: RFC 0025; RFC 0029; RFC 0041.
  - Current state: full competitive replacement is not supported by broad evidence.
  - Next slice outcome: sufficiency gate requiring correctness, benchmark, Native I/O, certificates,
    capability coverage, no-fallback, and release evidence.
  - User-visible surface: release claim gate and docs.
  - Implementation scope: claim gate report, tests.
  - Evidence required: all claim-grade evidence categories.
  - Acceptance: replacement claims fail closed until every required evidence row passes.
  - Verification: release readiness metadata tests and claim gate tests.
  - Non-goals: no replacement claim or runtime expansion.
  - Fallback/claim boundary: `claim_gate_status=not_claim_grade`.
  - Dependencies/blockers: GAR-0009, GAR-0015, GAR-0029, GAR-0041.
- [ ] GAR-0029-A CG-5/CG-6 and stateful reuse evidence expansion
  - Source: RFC 0029; correctness differential harness; benchmark-suite catalog.
  - Current state: current CG-5/CG-6 evidence is scoped; production stateful reuse runtime and
    performance/superiority claims are not broad.
  - Next slice outcome: one evidence expansion for correctness/benchmark/stateful reuse, or a
    deterministic blocker report.
  - User-visible surface: correctness harness, benchmark report, stateful reuse plan.
  - Implementation scope: harness/benchmark metadata, CLI plan, tests.
  - Evidence required: correctness refs, benchmark refs, execution certificates, Native I/O refs,
    no-fallback refs.
  - Acceptance: selected workload has attached evidence or an explicit claim blocker.
  - Verification: correctness and benchmark contract tests, focused stateful reuse tests.
  - Non-goals: no production cache/reuse runtime unless a separate implementation slice admits it.
  - Fallback/claim boundary: no superiority claim.
  - Dependencies/blockers: workload fixtures and Native I/O coverage.
- [ ] GAR-0040-A comparative rerun and managed-platform posture gate
  - Source: RFC 0040; benchmark-suite catalog; benchmark competitive claim evidence.
  - Current state: full comparative reruns, source-backed claim-grade promotion, managed-platform
    lanes, credentials, dependencies, and public performance claims are not enabled.
  - Next slice outcome: gate separating local reruns, external baseline/oracle comparisons,
    managed-platform lanes, credential requirements, and claim blockers.
  - User-visible surface: benchmark report, release claim gate, docs.
  - Implementation scope: benchmark metadata/report, release gate, tests.
  - Evidence required: benchmark refs, environment refs, credential policy refs, no-fallback refs.
  - Acceptance: managed-platform lanes require explicit credentials and remain comparison-only unless
    claim evidence passes.
  - Verification: benchmark contract tests, release readiness metadata tests.
  - Non-goals: no managed-platform run, credential use, dependency addition, or public claim.
  - Fallback/claim boundary: external systems are never ShardLoom execution.
  - Dependencies/blockers: GAR-0019 credential policy and GAR-0041 claim matrix.
- [ ] GAR-0041-A per-claim evidence attachment matrix
  - Source: RFC 0041; workspace feature build matrix; release security gate.
  - Current state: release claims are not claimable until required matrix rows have attached passing
    evidence.
  - Next slice outcome: matrix that binds each public claim to test, benchmark, certificate, Native
    I/O, security, provenance, and unsupported-path evidence.
  - User-visible surface: release gate output, docs.
  - Implementation scope: release check scripts/docs/tests.
  - Evidence required: all evidence categories named per claim.
  - Acceptance: any missing row fails the claim gate.
  - Verification: release readiness tests, workspace feature matrix tests.
  - Non-goals: no new claim or publication.
  - Fallback/claim boundary: claims fail closed by default.
  - Dependencies/blockers: evidence-producing GAR slices.
- [ ] GAR-0043-A hard release-readiness validators and architecture tracker
  - Source: RFC 0043; release security gate; global architecture review.
  - Current state: hard release-readiness gate exists, but final publication/attestation and
    architecture tracker validation need full evidence.
  - Next slice outcome: validator that checks traceability matrix, RFC acceptance, architecture
    tracker status, unsupported paths, and security/provenance evidence.
  - User-visible surface: release readiness script/report, docs.
  - Implementation scope: release scripts, contract tests, docs.
  - Evidence required: release/security/provenance refs, no-fallback refs, architecture review refs.
  - Acceptance: release gate fails closed when global review unchecked items or missing evidence block
    a claim.
  - Verification: release readiness metadata tests, release security gate tests, default GAR
    verification.
  - Non-goals: no publication, tags, secrets, or package upload.
  - Fallback/claim boundary: no final release/public claim.
  - Dependencies/blockers: every required claim/evidence slice.
- [ ] GAR-0043-B publication attestation and final release rehearsal
  - Source: RFC 0043; RFC 0024; release provenance docs.
  - Current state: dry-run/provenance scaffolding exists; actual publication and final attestation
    are not performed.
  - Next slice outcome: no-publication rehearsal that proves package artifacts, checksums, SBOM,
    attestations, and unsupported-path evidence without creating tags or uploads.
  - User-visible surface: release rehearsal report, docs.
  - Implementation scope: release scripts/docs/tests.
  - Evidence required: SBOM/checksum/provenance refs, security refs, no-fallback refs.
  - Acceptance: rehearsal produces local artifacts only and marks publication as human-approved.
  - Verification: release provenance dry-run tests, release readiness tests.
  - Non-goals: no package publication, tag, feedstock, marketplace, or secret use.
  - Fallback/claim boundary: rehearsal does not authorize release claims.
  - Dependencies/blockers: GAR-0043-A validators and GAR-0024 publication gate.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
