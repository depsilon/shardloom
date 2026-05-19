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
  - Status rule: object-store runtime work is represented by `GAR-0008`, `GAR-0028`, `GAR-0031`,
    and the GAR-COMPAT-1C admission ladder projection in the universal compatibility scoreboard.
- `docs/architecture/table-intelligence-layer.md`
  - Role: CG-9 schema/table/catalog/CDC/layout/compaction evidence reference.
  - Status rule: table/catalog runtime work is represented by `GAR-0020`, `GAR-0028`, and the
    GAR-COMPAT-1D table-format boundary projection in the universal compatibility scoreboard.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md`
  - Role: report-only universal source/sink/adapter/user-surface compatibility map covering local
    files, Vortex, generated/source-free output, databases, warehouses, object stores, table
    formats, REST/Flight/ADBC, and Foundry.
  - Status rule: scoreboard rows classify runtime-supported, smoke-supported, report-only, blocked,
    or not-planned posture only. Actionable compatibility/runtime work remains represented by
    `GAR-COMPAT-1`, `GAR-GEN-1`, `GAR-0008`, `GAR-0020`, `GAR-0028`, `GAR-0031`, and related
    evidence-bearing slices. GAR-COMPAT-1E adds the database/warehouse boundary projection without
    connector runtime.
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

GAR-SCALE-1A through GAR-SCALE-1H are complete and recorded in the completed ledger. Non-local scale
classes remain blocked or report-only until later scoped runtime slices attach real workload bytes,
correctness proof, no-fallback evidence, and the relevant runtime gates.

#### GAR-P1 - Core Runtime, Operators, And Execution Safety

#### GAR-P2 - I/O, Tables, Output, And Lakehouse Semantics
#### GAR-P3 - User Surfaces, APIs, Adapters, And Workflow

#### GAR-COMPAT-1 - Universal Compatibility Completion Matrix

GAR-COMPAT-1A is complete and recorded in the completed ledger. The universal compatibility
scoreboard now has a machine-readable JSON projection, CLI capability fields, Python typed
accessors, website/status rendering, and release-readiness metadata checks. The scoreboard remains a
capability map only; it does not add database, Excel, JDBC/ODBC, object-store, table-format,
SQL/DataFrame, REST/Flight/ADBC, Foundry, package, production, performance, or Spark-replacement
claims.

GAR-COMPAT-1B is complete and recorded in the completed ledger. The compatibility scoreboard now
projects `shardloom.universal_compatibility.generated_output_contract.v1` across CLI, Python,
website/status, docs, and Foundry proof wording. The projection keeps no-dataset smoke separate from
generated-output execution, marks current Python `ctx.from_rows(...).write(...)` and
`ctx.range(...).write(...)` paths as scoped local JSONL fixture smokes, keeps SQL/DataFrame rows
report-only, requires output evidence for output claims, emits no source Native I/O certificate when
no source dataset is read, and keeps object-store/Foundry generated-output runtime blocked.

GAR-COMPAT-1C is complete and recorded in the completed ledger. The compatibility scoreboard now
projects `shardloom.universal_compatibility.object_store_admission_ladder.v1` across CLI, Python,
website/status, object-store docs, compute-flow, GAR, and release-readiness checks. The ladder keeps
S3/GCS/ADLS URI parse, credential policy, public read, authenticated read, byte-range read,
full-file read, local cache, write staging, and commit protocol as separate report-only or blocked
gates with credential resolution, network probes, provider probes, object-store I/O, writes,
commits, fallback, and external engine invocation disabled.

GAR-COMPAT-1D is complete and recorded in the completed ledger. The compatibility scoreboard now
projects `shardloom.universal_compatibility.table_format_boundary_matrix.v1` across CLI, Python,
website/status, table-intelligence docs, compute-flow, GAR, and release-readiness checks. The matrix
keeps Iceberg/Delta/Hudi metadata read, table scan, snapshot/time-travel, partition evolution,
delete/tombstone, append, merge/update/delete, commit, rollback, catalog interaction, and
object-store coupling as separate report-only or blocked gates. Local metadata and delete/tombstone
smokes remain related evidence only, with catalog I/O, object-store I/O, table metadata/data reads,
writes, commits, rollbacks, fallback, and external engine invocation disabled.

GAR-COMPAT-1E is complete and recorded in the completed ledger. The compatibility scoreboard now
projects `shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1` across CLI,
Python, website/status, compute-flow, GAR, traceability, Python README, and release-readiness
checks. The matrix keeps SQLite, Postgres, MySQL, JDBC/ODBC, Snowflake, BigQuery, and Databricks
SQL as report-only or blocked import/export/query-pushdown gates, with credential resolution,
network probes, driver loading, import/export runtime, query pushdown, fallback, and external engine
invocation disabled.

GAR-GEN-1F is complete and recorded in the completed ledger. The Foundry proof report now emits
`shardloom.foundry_generated_output_boundary.v1` as a report-only boundary requiring future admitted
generated-output proof to write result/evidence datasets through Foundry output APIs. Current local
proof keeps `foundry_output_api_invoked=false`, direct S3/object-store paths disabled,
`fallback_attempted=false`, `external_engine_invoked=false`, and public Foundry generated-output
claims blocked.

#### GAR-NOVEL-1 - Evidence-Native Generated Execution, Lineage, Observability, And Confidence

GAR-NOVEL-1A is complete and recorded in the completed ledger. Capability views now expose
`shardloom.generated_source_evidence_alignment.v1`, a report-only cross-surface matrix that aligns
GeneratedSourceCertificate/source-free API rows with future OpenLineage, OpenTelemetry, Bayesian
confidence, and Foundry generated-output boundary refs. The alignment preserves no-fallback and
no-external-engine fields and does not enable exporters, telemetry network calls, Bayesian runtime
decisioning, SQL/DataFrame runtime, Foundry runtime, object-store writes, package publication, or
production/performance claims.

GAR-NOVEL-1B is complete and recorded in the completed ledger. Observability capability views now
expose `shardloom.openlineage_facet_mapping.v1`, a report-only mapping from ShardLoom execution
mode, no-fallback, Native I/O certificate, materialization boundary, claim gate, generated-source,
and Vortex artifact evidence into ShardLoom-owned future OpenLineage custom facet placeholders. The
mapping keeps export disabled, event emission disabled, schema publication disabled, backend/client
dependency disabled, network calls disabled, `fallback_attempted=false`,
`external_engine_invoked=false`, and `claim_gate_status=not_claim_grade`.

GAR-NOVEL-1C is complete and recorded in the completed ledger. Observability capability views now
expose `shardloom.opentelemetry_trace_export_contract.v1`, a report-only mapping from request
admission, source read, compatibility parse, Vortex import, Vortex scan, operator compute, result
sink, evidence render, and claim gate timing/evidence fields into future OpenTelemetry internal
span placeholders. The mapping keeps trace/metric/log export disabled, OTLP exporter configuration
disabled, collector/backend configuration disabled, SDK dependency expansion disabled, runtime
collection disabled, network calls disabled, allowlisted attributes required, redaction/retention
policy required, `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`.

GAR-NOVEL-1D is complete and recorded in the completed ledger. Benchmark artifacts now expose
`shardloom.traditional_analytics.bayesian_claim_confidence.v1` as a report-only/not-fit
claim-confidence schema for posterior runtime distribution, credible interval, regression
probability, minimum-run policy, benchmark population refs, release policy refs, uncertainty
reason, and claim boundary. The schema can only block future claims after a fitted model and
release gate exist; it cannot upgrade `claim_gate_status`, recompute benchmarks, apply runtime or
layout decisions, invoke external engines or services, weaken no-fallback evidence, or create
performance, superiority, Spark-replacement, package, SQL/DataFrame, object-store/lakehouse,
Foundry, production, or release claims.

#### GAR-COMMERCIAL-1 - Adoption And Commercial-Readiness Friction Reduction

GAR-COMMERCIAL-1A is complete and recorded in the completed ledger. The local release dry-run proof
now builds source artifacts, installs the exact local wheel in a clean virtual environment, runs
CLI/Python smoke checks, runs scoped `ctx.from_rows(...).write(local_jsonl)` and
`ctx.range(...).write(local_jsonl)` generated-source output smokes from the clean installed wheel,
runs a tiny `shardloom` plus `shardloom-prepared-vortex` local benchmark smoke, records transcript
fields for no-publication/no-release/no-fallback/no-external-engine posture, and updates README,
getting-started docs, release proof docs, example metadata, and contract tests. The slice remains
local technical-preview evidence only; it does not publish packages, create release tags, claim
production readiness, claim performance/Spark replacement, expand SQL/DataFrame/object-store/
lakehouse/Foundry runtime support, or weaken no-fallback policy.
GAR-COMMERCIAL-1B is complete and recorded in the completed ledger.
`docs/release/package-channel-readiness-matrix.json` now provides
`shardloom.package_channel_readiness_matrix.v1` for GitHub pre-release, TestPyPI, PyPI, Homebrew
tap, Scoop, winget, conda-forge, GHCR container, and future crates.io public API crates. The matrix
is validated by `scripts/check_package_channel_readiness.py` and consumed by
`scripts/check_release_readiness.py`; every channel remains blocked until channel-specific install,
uninstall, clean-install, smoke, SBOM/checksum/provenance, rollback/yank/delete/deprecate, and
authorization proof exists. PyPI/TestPyPI require Trusted Publisher/OIDC posture, internal Rust
crates remain unpublished, crates.io is limited to future stable public API crates, and package
access does not imply production, performance, Spark-replacement, SQL/DataFrame, object-store/
lakehouse, Foundry, or fallback-execution readiness.
GAR-COMMERCIAL-1C is complete and recorded in the completed ledger. `website/status.html` now
includes a generated buyer-facing "Can I use this?" matrix sourced from the universal compatibility
scoreboard and package-channel readiness matrix, with rows grouped across `runtime-supported`,
`smoke-supported`, `report-only`, `blocked`, `planned`, and `not-planned` posture. The matrix keeps
unsupported paths visible, links every row to source refs, preserves `fallback_attempted=false` and
`external_engine_invoked=false`, and is enforced by `scripts/check_website_readiness.py`. It is a
maturity map only and does not add runtime support, publish packages, rerun benchmarks, or create
production, performance, Spark-replacement, SQL/DataFrame, object-store/lakehouse, Foundry, package,
or fallback-execution claims.
GAR-COMMERCIAL-1D is complete and recorded in the completed ledger. The report-only enterprise
evidence export pack is now defined by `docs/release/enterprise-evidence-export-pack.md` and
`docs/release/enterprise-evidence-export-pack.json` with schema
`shardloom.enterprise_evidence_export_pack.v1`, a local artifact layout for ShardLoom JSON,
OpenLineage facet payloads, OpenTelemetry span/metric payloads, optional Markdown summaries, and a
required redaction report. `scripts/check_enterprise_evidence_export_pack.py` validates opt-in,
local-only, no-network/no-backend/no-event/no-trace/no-metric/no-log posture, redaction/retention
policy, `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`. This slice does not implement exporters, configure collectors,
emit events/traces/metrics/logs, invoke Foundry, resolve credentials, call object stores, publish
packages, add dependencies, expand runtime support, or create production observability,
managed-platform, performance, Spark-displacement, SQL/DataFrame, object-store/lakehouse, Foundry,
package, or fallback-execution claims.
GAR-COMMERCIAL-1E is complete and recorded in the completed ledger.
`docs/foundry/dev-stack-starter-kit.md` and `docs/foundry/dev-stack-starter-kit.json` now define
the local Foundry-style starter path with schema `shardloom.foundry_dev_stack_starter_kit.v1`,
commands for local CLI build, Foundry-style transform smoke, and `scripts/foundry_proof_of_use.py`,
plus source-free generated-output posture, staged input, local certificate-style output, and
evidence-dataset blocker boundaries. `scripts/check_foundry_dev_stack_starter.py` validates that
Foundry runtime, Foundry compute, Foundry Spark, Foundry output APIs, result/evidence datasets,
direct S3/object-store I/O, credentials, network probes, external compute, external engines,
fallback execution, production Foundry, Marketplace, and package claims remain disabled.
GAR-COMMERCIAL-1F is complete and recorded in the completed ledger.
`docs/use-cases/recipes/README.md` and `docs/use-cases/recipes/recipe-index.json` now provide a
validated workflow recipe library with schema `shardloom.workflow_recipe_library.v1` for no-dataset
smoke, local CSV/Parquet certification, prepared/native direction, native Vortex posture,
source-free generated reference tables, dirty CSV cleanup, nested JSON scan, CDC overlay, output
fanout, object-store blocked diagnostics, Foundry dev-stack smoke, and benchmark interpretation.
`scripts/check_workflow_recipes.py` validates recipe ids, use-case links, exact references, evidence
fields, claim boundaries, blockers for report-only/blocked recipes, `fallback_attempted=false`, and
`external_engine_invoked=false`. The recipe library is documentation/adoption surface only; it does
not authorize runtime expansion, package publication, benchmark recomputation, object-store/runtime
I/O, Foundry production support, performance claims, or fallback execution.

#### GAR-DOCS-1 - Non-Expert Use Case Atlas

GAR-DOCS-1 is complete and recorded in the completed ledger. The Use Case Atlas now provides:

- `docs/use-cases/README.md` as the non-expert hub for "Can ShardLoom do my thing?", "How do I
  try it?", "What evidence do I get?", and "What is not supported yet?"
- `docs/use-cases/use-case-index.yml` as the machine-readable capability/use-case contract with
  `ready_local`, `smoke_supported`, `report_only`, `planned`, `blocked`, and `unsupported`
  vocabulary.
- `docs/use-cases/generated/` and `website/use-cases/` pages generated from the index by
  `website/build_static_pages.py`.
- `docs/use-cases/field-guide/README.md` for non-expert terminology attached to use-case surfaces.
- `docs/use-cases/reference-backlinks.md` for source-of-truth citation/backlink coverage.
- `docs/use-cases/recipes/README.md` and `docs/use-cases/recipes/recipe-index.json` for the
  non-expert recipe library completed with GAR-COMMERCIAL-1F.
- Validators for index schema, capability coverage, glossary coverage, backlinks, workflow recipes,
  website readiness, and static assets.

The completed atlas remains documentation/status surface only. It does not add runtime behavior,
package publication, benchmark recomputation, production SQL/DataFrame support, object-store or
lakehouse runtime, Foundry production support, performance claims, Spark-displacement claims, or
fallback execution. All supported and blocked examples preserve `fallback_attempted=false` and
`external_engine_invoked=false` semantics.

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
GAR-0030-A is complete and recorded in the completed ledger. The completed slice adds explicit
universal-harness execution-admission fields to `UniversalHarnessReport` and
`universal-harness-plan --format json`: execution gate status, execution allowed/attempted flags,
required/attached/missing evidence refs, and required capability, execution-certificate, Native I/O,
policy/no-fallback, output, correctness, and benchmark evidence. Harness execution remains blocked
and environment readiness remains separate from runtime execution.
GAR-0032-A SQL parser/binder report-only readiness is complete.
GAR-0032-A is complete and recorded in the completed ledger. The completed slice documents the
SQL parser/binder readiness posture in
`docs/architecture/sql-parser-binder-readiness.md` and strengthens
`workflow-unsupported-plan sql-parse|sql-bind|sql-plan|sql-execute --format json` with
`support_status=unsupported`, `claim_gate_status=not_claim_grade`, and explicit
`parser_executed=false`, `binder_executed=false`, and `planner_executed=false` fields. SQL remains
diagnostic/report-only with no parser dependency, binder, planner, runtime execution, external
engine invocation, or fallback.
GAR-0032-C UDF and external-effect blocker matrix is complete.
GAR-0032-C is complete and recorded in the completed ledger. The completed slice adds
`shardloom.external_effect_blocker_matrix.v1` to `effect-budget-plan --format json` and relevant
capability views, documents the posture in
`docs/architecture/udf-external-effect-blocker-matrix.md`, and keeps SQL/Rust/WASM/Python/external
service UDFs, API calls, LLM calls, embedding generation, vector search, plugin execution, media
extraction, and network egress `blocked` with `permission_status=policy_required`,
`effect_status=denied_by_default`, `runtime_execution=false`, `effect_executed=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.
GAR-0032-D unstructured/media and universal adapter capability matrix is complete.
GAR-0032-D is complete and recorded in the completed ledger. The completed slice adds
`shardloom.unstructured_adapter_capability_matrix.v1` to `capabilities unstructured-media`,
`capabilities universal-adapters`, `capabilities event-api-saas-adapters`, and
`capabilities api-surfaces`, documents the posture in
`docs/architecture/unstructured-adapter-capability-matrix.md`, and keeps document references, text
extraction, media decode/extraction, embeddings, vector search, local file adapters,
database/warehouse adapters, object-store/table adapters, event/API/SaaS adapters, and source/sink
metadata report-only or blocked with `runtime_execution=false`, `source_io_performed=false`,
`sink_io_performed=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.
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
GAR-0033-A is complete and recorded in the completed ledger. The ETL workflow capability matrix is
now exposed through `capabilities workflow --format json`, Python `ctx.etl_workflow_matrix()`, and
`docs/architecture/etl-workflow-capability-matrix.md`. It keeps local ready/smoke workflow rows,
report-only SQL/DataFrame/data-quality API posture, and blocked object-store/table/production ETL
rows explicit without adding runtime behavior, package publication, production ETL claims, or
fallback execution.
GAR-0034-A is complete and recorded in the completed ledger. The live/hybrid fabric freshness gate
is now exposed through `engine-capability-matrix --format json`, `capabilities engines --format
json`, Python `ctx.engine_capability_matrix()`, and
`docs/architecture/live-hybrid-fabric-freshness-gate.md`. It keeps fixture-scoped freshness
evidence separate from production live/hybrid claims and keeps broker, state-store, object-store,
catalog, exactly-once, benchmark, and Spark-displacement claims blocked without adding runtime
behavior, external baselines as fallback, or new I/O effects.
GAR-0035-A is complete and recorded in the completed ledger. The REST server/runtime unsupported
contract is now exposed through `rest-api-contract-plan --format json`, Python
`ctx.rest_api_contract_plan()`, and
`docs/architecture/rest-server-runtime-unsupported-contract.md`. It keeps OpenAPI/report-only REST
planning separate from HTTP listener, remote execution, Flight/ADBC, broker, dependency-expanded
server, production API, benchmark, and Spark-displacement claims while preserving no server start,
no listener, no external engine, and no fallback.
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
GAR-0037-A is complete and recorded in the completed ledger. The completed slice adds
`shardloom.wrapper_connector_implementation_registry.v1`, exposes it through
`capabilities api-surfaces --format json` and Python `ctx.wrapper_connector_registry()`, and keeps
generated clients, DB-API, SQLAlchemy, Ibis, orchestration wrappers, MCP, Flight/ADBC, JDBC/ODBC,
BI/Grafana, Foundry package, REST server, dependency expansion, data-plane bridge, external engine,
and fallback support blocked unless later scoped evidence admits them.
GAR-0039-A is complete and recorded in the completed ledger. The completed slice migrates the
API-surface capability family further into typed payloads by adding an inline
`api_surface_capability_report` artifact for the wrapper/connector registry, adding typed
`capability_snapshot` fields for registry counts and claim boundaries, and making Python
`OutputEnvelope.field_map` prefer typed payload fields before the temporary legacy mirror. The
legacy `fields` mirror remains present for compatibility while later command families migrate.
GAR-0039-B is complete and recorded in the completed ledger. The completed slice centralizes shared
typed-envelope integration-test helpers, strengthens the Foundry optional-harness golden fixture,
and routes `universal-harness-plan --format json` into an inline `universal_harness_report`
artifact without changing runtime behavior, command semantics, public schema, external baseline
policy, or no-fallback boundaries.

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

#### GAR-RUNTIME-COMPLETE-1 - Full Compute-Engine Usability Runtime Track

This track exists because ShardLoom's goal is a fully functional and usable compute engine, not a
repo that only contains tests, report-only matrices, and architecture scaffolding. These items are
placed at the end of the current Planned queue as explicit implementation-ready runtime slices so
future autonomous work cannot treat report-only posture as completion. Each slice must preserve
`fallback_attempted=false`, `external_engine_invoked=false`, no hidden fast mode, and claim-safe
public wording until evidence gates pass.

- [ ] GAR-RUNTIME-COMPLETE-1B minimal SQL frontend to ShardLoom-native plans
  - Source: RFC 0032, GAR-0032-A, source-free generated-output contract, operator capability
    matrix, diagnostics/explain RFCs.
  - Current state: SQL text admission, parse/bind/plan/execute rows are diagnostic/report-only;
    no parser, binder, planner, or SQL runtime execution is currently claimable.
  - Next slice outcome: implement a narrow SQL runtime path for `SELECT` literal expressions,
    `VALUES`, simple projection/filter/limit over admitted local sources, and deterministic
    unsupported diagnostics for everything else.
  - User-visible surface: CLI SQL/explain command, Python SQL helper if exposed, capability views,
    docs/use-cases, website status.
  - Implementation scope: SQL parser/binder/planner module, ShardLoom logical plan lowering,
    explain output, generated-source and local-source admission, tests.
  - Evidence required: parser/binder/planner executed flags, before/after plan digests,
    unsupported diagnostic codes, execution mode, source/sink evidence, correctness digest,
    materialization/decode fields, no-fallback/no-external-engine fields, claim gate.
  - Acceptance: admitted SQL cases execute through ShardLoom-native code only; unsupported SQL
    constructs fail with stable diagnostics; SQL rows never call DataFusion, DuckDB, Spark, Polars,
    pandas, SQLite, or another engine as fallback.
  - Verification: SQL parser/binder/planner unit tests, CLI/Python SQL smoke tests, unsupported
    diagnostic snapshots, release readiness metadata tests, default GAR verification.
  - Non-goals: no broad SQL compatibility, no joins/windows/UDFs unless explicitly admitted, no
    production SQL claim.
  - Claim boundary: narrow local SQL frontend runtime only; not a production SQL engine.
  - Fallback boundary: external SQL engines and Vortex query-engine integrations are prohibited.
  - Dependencies/blockers: operator coverage from GAR-RUNTIME-COMPLETE-1D and source/sink evidence.
  - Ledger rule: move completed details to the completed ledger after merge.

- [ ] GAR-RUNTIME-COMPLETE-1C Python DataFrame/query-builder runtime surface
  - Source: RFC 0010, RFC 0032, RFC 0033, Python README, use-case atlas, current
    `LazyFrame`/capability posture.
  - Current state: Python wrapper and capability views exist; broad DataFrame methods remain
    report-only or unsupported; users cannot yet rely on a complete ergonomic DataFrame-like
    workflow.
  - Next slice outcome: implement a scoped DataFrame/query-builder runtime over admitted local
    sources and generated sources for select/project/filter/limit/basic aggregate/write, with
    explain/capability diagnostics for unsupported methods.
  - User-visible surface: `shardloom.context()`, `ctx.read_*`, generated-source builders,
    DataFrame/query-builder methods, docs/getting-started, recipes, website use cases.
  - Implementation scope: Python query builder, CLI command mapping, typed result envelopes,
    capability rows, examples, tests.
  - Evidence required: method admission row, logical/physical plan digest, execution mode,
    source/sink certificate refs, output refs, correctness digest, no-fallback/no-external-engine
    fields, claim gate.
  - Acceptance: a non-expert can import ShardLoom, build a small local workflow, write output, and
    inspect evidence without reading architecture docs; unsupported methods are explicit and
    actionable.
  - Verification: Python unit/integration tests, CLI smoke, examples compile/run smoke,
    use-case coverage checks, website readiness, release readiness metadata.
  - Non-goals: no pandas/Polars execution fallback, no notebook production claim, no broad DataFrame
    parity claim.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: pandas, Polars, DuckDB, DataFusion, Spark, and Vortex query-engine
    integrations are not execution backends.
  - Dependencies/blockers: GAR-RUNTIME-COMPLETE-1D operator coverage and output evidence.
  - Ledger rule: record completed workflow coverage and evidence in the ledger after merge.

- [ ] GAR-RUNTIME-COMPLETE-1D physical operator, expression, and semantics coverage
  - Source: RFC 0015, RFC 0016, RFC 0021, benchmark scenario catalog, correctness fixture manifest.
  - Current state: selected prepared/native residual-native scenarios exist; generalized operator,
    expression, null/type, join, window, top-N, and fused pipeline coverage is incomplete.
  - Next slice outcome: expand ShardLoom-native physical operators and expression semantics by
    focused families: filter/project/limit, aggregates, group-by, joins, top-k/top-N, windows,
    casts, nulls, strings, decimals/timestamps where admitted, and deterministic blockers where not.
  - User-visible surface: CLI/Python workflows, SQL/DataFrame lowered plans, benchmark rows,
    correctness reports, capability views.
  - Implementation scope: operator kernels/residual executors, expression IR, null/type semantics,
    correctness fixtures, benchmark scenario rows.
  - Evidence required: correctness digest, reference/oracle policy, operator family, input/output
    schema, null/type behavior, materialization/decode status, selected row counts,
    `fallback_attempted=false`, `external_engine_invoked=false`, claim gate.
  - Acceptance: each supported operator family has success tests, edge-case tests, unsupported
    diagnostics, and benchmark/correctness evidence; unsupported operators cannot be silently
    delegated.
  - Verification: unit/property/correctness tests, fixture manifest checks, traditional benchmark
    harness tests, fuzz/property expansion where applicable, clippy/fmt/workspace tests.
  - Non-goals: no performance/superiority claim from correctness expansion alone.
  - Claim boundary: operator support is workload-scoped until claim-grade gates pass.
  - Fallback boundary: external engines may be testing oracles only, never runtime fallback.
  - Dependencies/blockers: semantic fixtures, expression registry, benchmark row schema.
  - Ledger rule: ledger entry must list operator families promoted and remaining blockers.

- [ ] GAR-RUNTIME-COMPLETE-1E prepared/native Vortex execution optimization completion
  - Source: GAR-PERF-2A through GAR-PERF-2H, Vortex Scan API docs, encoded predicate provider
    evidence, in-process session runtime reference.
  - Current state: prepared/native batch runner and scoped source-state reuse exist; Scan pushdown,
    encoded kernels, fused pipelines, session reuse, buffer pools, and optimized build lanes are not
    complete enough for broad runtime or performance claims.
  - Next slice outcome: make prepared/native paths the default runtime-development lane for local
    Vortex artifacts with complete filter/projection/limit pushdown where supported, encoded-kernel
    admission/execution evidence, fused pipeline evidence, session reuse, allocation profiling, and
    build-profile attribution.
  - User-visible surface: benchmark rows, CLI prepared/native batch commands, Python capability
    views, compute-flow docs, website benchmarks.
  - Implementation scope: Vortex scan request builder, projection/filter/limit lowering, encoded
    kernel registry, fused pipeline executor, `ShardLoomSession`, buffer-pool/resource metrics,
    benchmark row schema and docs.
  - Evidence required: scan pushdown fields, encoded predicate/kernel fields, fused pipeline fields,
    session/cache hit fields, allocation/buffer metrics, source/prepared-state digests,
    materialization/decode status, correctness digest, no-fallback/no-external-engine fields.
  - Acceptance: every prepared/native scenario either uses the admitted optimized path or emits a
    deterministic blocker; compatibility-import timing remains separated from prepared/native query
    timing; no encoded-native or performance claim is made without end-to-end evidence.
  - Verification: source-backed scan tests, selective filter and filter/project/limit smokes,
    differential correctness tests, benchmark smoke, traditional benchmark harness tests,
    workspace fmt/clippy/tests.
  - Non-goals: no hidden global fast mode, no performance/superiority claim, no external provider
    fallback.
  - Claim boundary: runtime-development and local benchmark evidence only until claim gates pass.
  - Fallback boundary: upstream Vortex native APIs are allowed only through certified boundaries;
    Vortex query-engine integrations and external engines are prohibited.
  - Dependencies/blockers: Vortex API capability, correctness fixtures, benchmark refresh.
  - Ledger rule: record promoted scenarios, blocked scenarios, and benchmark evidence refs.

- [ ] GAR-RUNTIME-COMPLETE-1F output, sink, and cross-format fanout runtime
  - Source: GAR-IOREUSE-1, result-sink proof, compatibility output writer matrix, universal input
    contract.
  - Current state: OutputPlan and fanout benchmark rows are report/evidence scaffolding; real
    multi-output fanout writers, replay proof, and persistent output planning are incomplete.
  - Next slice outcome: implement local output fanout from one admitted source/prepared state into
    Vortex plus selected compatibility outputs with replay/correctness evidence and deterministic
    blockers for unsupported formats.
  - User-visible surface: Python `.write(...)`/fanout helper, CLI output command, benchmark rows,
    use-case recipes, website status.
  - Implementation scope: `OutputPlan`, sink writer registry, local CSV/JSONL/Parquet/Arrow/Vortex
    output paths as admitted, result replay, output digests, benchmark fanout rows.
  - Evidence required: output plan digest, output format/location/schema, write mode, write timing,
    result replay status, output Native I/O certificate, correctness digest per output,
    metadata-loss/fidelity fields, no-fallback/no-external-engine fields.
  - Acceptance: input and output formats are decoupled; one prepared source can write multiple local
    outputs where supported; unsupported output formats block deterministically.
  - Verification: local output smoke per supported format, result replay tests, fanout benchmark
    smoke, capability snapshots, website/use-case validation.
  - Non-goals: no S3/object-store write, no lakehouse/table commit, no production sink claim.
  - Claim boundary: scoped local output/fanout support only.
  - Fallback boundary: compatibility output is translation/export, not fallback execution.
  - Dependencies/blockers: source/prepared-state reuse and supported writer implementations.
  - Ledger rule: completed entry must list supported formats and blocked formats.

- [ ] GAR-RUNTIME-COMPLETE-1G object-store, table, and lakehouse runtime ladder implementation
  - Source: GAR-COMPAT-1C, GAR-COMPAT-1D, GAR-SCALE-1E, object-store request planner, table
    intelligence docs.
  - Current state: S3/GCS/ADLS and Iceberg/Delta/Hudi support is report-only/blocked; URI parsing,
    credential policy, byte-range reads, writes, commits, table scans, and table commits are not
    runtime-supported.
  - Next slice outcome: implement the ladder in strict stages: URI parse, no-credential public read
    policy, credential policy, byte-range read, streaming/full-file read, write staging, commit
    protocol, table metadata read, snapshot scan, append/merge/delete, commit, rollback.
  - User-visible surface: CLI/Python capability views, object-store/table commands, compute-flow,
    website status, docs/use-cases.
  - Implementation scope: object-store adapter boundary, credential/effect policy, request planner,
    split manifest, local cache policy, table metadata parser/adapters, commit/rollback evidence.
  - Evidence required: credential policy status, network/effect policy, object version/ETag,
    split manifest id, byte ranges, table snapshot/manifest counts, commit protocol, idempotency
    key, rollback status, Native I/O certificate, no-fallback/no-external-engine fields.
  - Acceptance: read/write/commit/table metadata/table runtime are separate gates; each stage either
    emits proof or a deterministic blocker; no table metadata smoke implies table commit support.
  - Verification: policy tests, object-store mocked/local emulator tests when admitted, table
    metadata fixtures, commit/retry tests, release readiness checks.
  - Non-goals: no blanket S3/lakehouse production claim, no credentials by default, no managed
    platform claim.
  - Claim boundary: per-provider/per-table-format scoped runtime only after evidence.
  - Fallback boundary: object-store/table connectors do not authorize external query engines.
  - Dependencies/blockers: credential policy, security review, dependency/license approval.
  - Ledger rule: ledger must record exact provider/format/stage promoted.

- [ ] GAR-RUNTIME-COMPLETE-1H scale-grade local and distributed execution runtime
  - Source: GAR-SCALE-1, RFC 0014, RFC 0016, RFC 0017, split/source model, benchmark scale profiles.
  - Current state: scale classes and evidence fields exist; larger-than-memory local, split-parallel
    local, spill-safe execution, shuffle, retries, distributed runtime, and managed-platform proof
    remain blocked/report-only.
  - Next slice outcome: implement scale-grade execution in stages: split manifests, bounded memory,
    spill/backpressure, shuffle/repartition/join scale, retry/idempotency, local split parallelism,
    and report-only-to-runtime promotion for distributed workers only with explicit evidence.
  - User-visible surface: benchmark scale profiles, CLI/Python execution envelopes, status/claim
    gates, website scale explanation.
  - Implementation scope: split manifest, scheduler, memory budget, spill manager, shuffle plan,
    retry/cancel/recovery, output commit status, scale benchmark profiles.
  - Evidence required: scale profile/status, data volume/row/file/partition/split counts, memory and
    spill fields, shuffle fields, retry/idempotency fields, output commit status, remote worker
    fields, no-fallback/no-external-engine fields.
  - Acceptance: larger-than-memory and split-parallel claims require real bytes and correctness
    proof; synthetic metadata-only evidence cannot become runtime scale claim; distributed support
    remains separate from local split parallelism.
  - Verification: scale profile contract tests, local stress smoke, spill/backpressure tests,
    shuffle correctness tests, retry/idempotency tests, benchmark harness tests.
  - Non-goals: no literal any-volume claim, no Spark replacement claim, no distributed runtime claim
    until remote-worker proof exists.
  - Claim boundary: declared resource-envelope scale only.
  - Fallback boundary: external engines can be baselines/oracles only.
  - Dependencies/blockers: object-store/table gates for non-local scale, operator coverage.
  - Ledger rule: ledger must list resource envelope, data volume, and claim status.

- [ ] GAR-RUNTIME-COMPLETE-1I external surfaces, adapters, observability, and extension runtime
  - Source: RFC 0011, RFC 0012, RFC 0023, RFC 0035, RFC 0036, GAR-NOVEL-1, extension/governance
    planned items.
  - Current state: REST, Flight/ADBC, MCP, wrappers/connectors, Foundry package/runtime, UDF/plugin
    execution, OpenLineage export, OpenTelemetry export, and enterprise evidence export are
    report-only or blocked.
  - Next slice outcome: implement only the first safe runtime surface in each family when admitted:
    local/loopback control-plane API, opt-in local evidence export, typed adapter wrappers, and
    deterministic blockers for effectful UDFs/plugins/network/platform integrations.
  - User-visible surface: REST/control API if admitted, Python/CLI export commands, capability
    views, docs, website status.
  - Implementation scope: control-plane lifecycle schemas, export pack writer, adapter registry,
    extension manifest validation, sandbox/effect policy, Foundry local proof boundary.
  - Evidence required: lifecycle state, API schema version, effect policy, redaction report, export
    path, no-network-by-default fields, permission status, runtime invoked flags, no-fallback fields.
  - Acceptance: safe surfaces are usable without hidden side effects; effectful/network/platform
    paths remain explicitly blocked until policy and runtime proof exist.
  - Verification: API/export contract tests, redaction/effect policy tests, capability snapshots,
    website readiness, release readiness metadata.
  - Non-goals: no production server, no marketplace/package claim, no UDF sandbox execution until
    approved.
  - Claim boundary: local technical-preview surfaces only.
  - Fallback boundary: adapters and extensions must not execute external engines as ShardLoom work.
  - Dependencies/blockers: credential/security policy and dependency/license reviews.
  - Ledger rule: record exact surface promoted and effects kept disabled.

- [ ] GAR-RUNTIME-COMPLETE-1J package, install, and release usability completion
  - Source: GAR-COMMERCIAL-1A/B, GAR-0024-A, GAR-0043-A/B, package-channel readiness matrix.
  - Current state: local release dry-run and channel matrix exist; public package channels,
    installation proofs, API/schema stability windows, signing, SBOM/checksum/provenance, and
    rollback/yank policies remain gated.
  - Next slice outcome: make ShardLoom installable and smoke-testable through approved public or
    pre-release channels only after hard gates pass, with no-publication rehearsals before human
    approval.
  - User-visible surface: README, docs/getting-started, package metadata, release docs, website
    status.
  - Implementation scope: package build scripts, wheel/sdist metadata, release validation scripts,
    SBOM/checksum/provenance dry run, install/uninstall smoke, channel docs.
  - Evidence required: install/uninstall commands, clean install proof, smoke output, checksums,
    SBOM, provenance, API/schema compatibility report, rollback/yank policy, authorization record.
  - Acceptance: users can install from an approved channel and run a smoke path without architecture
    docs; no channel is marked ready without proof and human approval.
  - Verification: package dry-run, clean venv install smoke, release readiness tests, package
    channel checker, dependency/security gates.
  - Non-goals: no upload, tag, signing key, feedstock, or marketplace submission without explicit
    human approval.
  - Claim boundary: package access does not imply production readiness or performance superiority.
  - Fallback boundary: packaging must not add fallback engine dependencies.
  - Dependencies/blockers: release gates, security/provenance, stable API/schema decision.
  - Ledger rule: ledger must include artifact paths and no-publication posture unless release was
    explicitly approved.

- [ ] GAR-RUNTIME-COMPLETE-1K correctness, benchmark, and public-claim evidence completion
  - Source: GAR-P5, benchmark publishing, correctness differential harness, public technical-preview
    readiness.
  - Current state: local evidence and benchmark publishing scaffolding exist; full correctness,
    fuzz/property, comparative benchmark, managed-platform, scale, and per-claim evidence are not
    complete enough for production/performance/replacement claims.
  - Next slice outcome: attach every public/runtime claim to required correctness, benchmark,
    Native I/O, execution certificate, materialization/decode, no-fallback, security, package, and
    release evidence.
  - User-visible surface: release readiness report, benchmark page, README, website status, docs.
  - Implementation scope: correctness harness, fuzz/property fixtures, benchmark artifacts,
    completeness gates, claim matrix, release validators, website/README wording checks.
  - Evidence required: per-claim refs to passing tests, benchmark profile, environment fingerprint,
    certificate ids, Native I/O refs, unsupported-path diagnostics, package/security/provenance refs.
  - Acceptance: missing evidence blocks claims automatically; old artifacts cannot be presented as
    latest proof; external baselines are visible as baselines only.
  - Verification: correctness harness tests, benchmark artifact completeness checker,
    release-readiness tests, website readiness, workspace fmt/clippy/tests.
  - Non-goals: no performance/superiority/Spark-replacement/production claim unless every gate
    passes.
  - Claim boundary: claim-grade only per workload and per evidence matrix row.
  - Fallback boundary: external engines remain baselines/oracles only.
  - Dependencies/blockers: evidence from runtime slices 1A through 1J.
  - Ledger rule: ledger must list passed/blocked claims and exact evidence refs.

- [ ] GAR-RUNTIME-COMPLETE-1L website and human-learning surface parity with runtime state
  - Source: GAR-DOCS-1, GAR-WEB-ATLAS-1, public technical-preview readiness, website readiness
    scripts.
  - Current state: website and atlas are strong, but static generated pages can become stale when
    runtime status changes.
  - Next slice outcome: keep homepage, Field Guide, Use Case Atlas, benchmark page, compute-flow,
    status matrix, rendered README, sitemap, and readiness checks synchronized with every runtime
    promotion or blocker.
  - User-visible surface: `shardloom.io`, README, docs/use-cases, website status/telemetry pages.
  - Implementation scope: static page generator, use-case/status indexes, compute-flow local
    snapshot, website readiness checks, asset validation.
  - Evidence required: page/source refs, generated timestamp where applicable, no stale report-only
    wording for promoted runtime paths, no unsupported claim phrases, no runtime GitHub fetches,
    valid assets/canonicals/OG/sitemap.
  - Acceptance: a non-expert can answer "Can ShardLoom do my thing?", "How do I try it?", "What
    evidence do I get?", and "What is not supported?" without reading RFCs or the phase plan.
  - Verification: `python website/build_static_pages.py`, `python scripts/check_website_readiness.py`,
    `node website/validate_static_assets.js`, use-case coverage/backlink checks, `git diff --check`.
  - Non-goals: no marketing overclaims, no live benchmark execution on Cloudflare, no external JS
    framework migration unless separately approved.
  - Claim boundary: website explains technical-preview runtime status; it does not create claims.
  - Fallback boundary: website must preserve no-fallback/no-external-engine semantics.
  - Dependencies/blockers: every runtime promotion must update the source docs/indexes before merge.
  - Ledger rule: completed runtime slices must note corresponding website/doc parity updates.

#### GAR-RUNTIME-IMPL-1 - Implementation-Ready Runtime Slice Queue

The broad `GAR-RUNTIME-COMPLETE-1B` through `GAR-RUNTIME-COMPLETE-1L` items above are umbrella
tracks. The following child slices are the concrete implementation queue for making the engine
usable end to end. They are intentionally PR-sized, runtime-oriented, and evidence-bearing. Complete
them in dependency order unless a later unblocker or review finding changes the sequence.

- [ ] GAR-RUNTIME-IMPL-1C Python query-builder first complete local workflow
  - Source: `GAR-RUNTIME-COMPLETE-1C`, Python README, Use Case Atlas.
  - Current state: Python has useful generated-source helpers and capability surfaces, but a
    complete local DataFrame-like workflow is not yet broad enough for non-expert use.
  - Next slice outcome: support `ctx.read_*().select().filter().limit().write()` for one local
    source family and generated sources, with typed evidence accessors.
  - User-visible surface: Python API, first-10-minutes docs, examples, website use cases.
  - Implementation scope: Python query builder, CLI invocation bridge, typed result/evidence
    objects, examples, docs.
  - Evidence required: method admission, logical/physical plan digest, source/output refs,
    correctness digest, no-fallback/no-external-engine fields, claim gate.
  - Acceptance: a user can import ShardLoom, run the workflow, write local output, and inspect
    evidence without invoking another engine.
  - Verification: Python unit/integration tests, example compile/run smoke,
    `python -m compileall -q python/src python/tests scripts examples`, website readiness.
  - Non-goals: no pandas/Polars execution backend, no broad DataFrame parity, no notebook claim.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: Python helpers may marshal arguments only; they must not execute with pandas,
    Polars, DuckDB, Spark, DataFusion, or Dask.
  - Dependencies/blockers: `GAR-RUNTIME-IMPL-1D`, local source readers, and output writer evidence.
  - Ledger rule: ledger entry must include supported methods and blocked methods.

- [ ] GAR-RUNTIME-IMPL-1E aggregate, group-by, join, top-N, and window operator expansion
  - Source: `GAR-RUNTIME-COMPLETE-1D`, benchmark scenario catalog, correctness fixture manifest.
  - Current state: aggregate, group-by, join, top-N, and window coverage is partial and not complete
    across user surfaces.
  - Next slice outcome: promote one focused family at a time with correctness fixtures, runtime
    evidence, unsupported diagnostics, and benchmark rows.
  - User-visible surface: CLI/Python/SQL/DataFrame workflows, benchmark rows, capability view.
  - Implementation scope: native operator kernels, expression lowering, fixture manifests,
    benchmark scenario rows.
  - Evidence required: family name, row counts, selected/output rows, correctness digest,
    materialization/decode status, no-fallback/no-external-engine fields.
  - Acceptance: every promoted family has deterministic semantics and equal decoded-reference
    results for admitted types; unsupported variants block before execution.
  - Verification: focused Rust tests per family, differential correctness tests, benchmark smoke,
    `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
  - Non-goals: no all-SQL parity and no performance/superiority claim.
  - Claim boundary: workload-scoped operator support only.
  - Fallback boundary: no external residual execution.
  - Dependencies/blockers: `GAR-RUNTIME-IMPL-1D` and benchmark/correctness fixture coverage.
  - Ledger rule: complete each operator family with its own ledger evidence block.

- [ ] GAR-RUNTIME-IMPL-1F prepared/native Vortex scan pushdown runtime completion
  - Source: `GAR-RUNTIME-COMPLETE-1E`, `docs/architecture/vortex-scan-pushdown-completion.md`.
  - Current state: source-backed scan evidence exists, but pushdown is not complete across prepared
    and native scenario families.
  - Next slice outcome: implement filter/projection/limit pushdown where admitted and emit
    deterministic blockers everywhere else.
  - User-visible surface: prepared/native benchmark rows, compute-flow docs, Python/CLI capability
    views.
  - Implementation scope: Vortex scan request builder, predicate lowering, projection masks,
    limit/slice pushdown, benchmark row fields.
  - Evidence required: `scan_filter_pushed_down`, `scan_projection_pushed_down`,
    `scan_limit_pushed_down`, columns read/output, data decoded/materialized, correctness digest.
  - Acceptance: admitted scenarios avoid reading unused columns; filter-only columns do not leak
    into output; unsupported expressions block.
  - Verification: selective filter smoke, filter/project/limit smoke, source-backed scan tests,
    benchmark harness tests.
  - Non-goals: no encoded-native claim from pushdown alone.
  - Claim boundary: pushdown evidence only for admitted scenarios.
  - Fallback boundary: no Vortex query-engine integration or external provider fallback.
  - Dependencies/blockers: upstream Vortex Scan API capability and expression-lowering support.
  - Ledger rule: ledger entry must list each scenario family as pushed down, blocked, or unsupported.

- [ ] GAR-RUNTIME-IMPL-1G encoded-kernel registry execution pairs
  - Source: `GAR-RUNTIME-COMPLETE-1E`, `docs/architecture/compressed-encoded-kernel-registry.md`.
  - Current state: scoped encoded-predicate evidence exists, but broad encoded-native operator
    coverage is not complete.
  - Next slice outcome: implement first runtime kernel pairs for bitpacked filters, sequence
    equality/range, dictionary equality/group-by, constant arrays, and sorted min/max pruning where
    Vortex evidence admits them.
  - User-visible surface: benchmark evidence, capability matrix, compute-flow.
  - Implementation scope: encoded-kernel registry, admission policy, canonicalization diagnostics,
    benchmark/correctness rows.
  - Evidence required: encoding id, kernel admitted/executed, canonicalization required, decoded,
    materialized, encoded-native claim status, correctness digest.
  - Acceptance: admitted pairs run without unnecessary decode; unsupported encodings block
    deterministically; encoded-native claims stay false until end-to-end evidence passes.
  - Verification: unit tests per encoding/operator pair, selective-filter/group-by benchmark smoke.
  - Non-goals: no blanket encoded-native engine claim.
  - Claim boundary: per encoding/operator pair only.
  - Fallback boundary: no external kernels or query engines as fallback.
  - Dependencies/blockers: Vortex encoding metadata availability and correctness fixtures for each
    encoding/operator pair.
  - Ledger rule: ledger entry must list promoted and blocked encoding/operator pairs.

- [ ] GAR-RUNTIME-IMPL-1H fused pipelines, session reuse, and buffer reuse
  - Source: `GAR-RUNTIME-COMPLETE-1E`, fused pipeline, session runtime, allocation/buffer-pool docs.
  - Current state: scoped evidence exists for fusion/session/resource fields, but general runtime
    fusion and reusable session state are incomplete.
  - Next slice outcome: implement scoped in-process `ShardLoomSession` reuse, fused pipelines for
    admitted operator chains, and safe buffer reuse evidence.
  - User-visible surface: CLI batch command, Python session API if admitted, benchmark telemetry.
  - Implementation scope: session registry, source/prepared-state cache, fused executor, buffer
    pool/resource metrics, cleanup/close semantics.
  - Evidence required: session id, cache hits/misses, source/prepared reuse counts, fused pipeline
    fields, allocation/buffer metrics, correctness digest, no-fallback fields.
  - Acceptance: repeated runs reuse admitted state without stale results; fusion avoids intermediate
    full-table materialization where supported; session close releases scoped state.
  - Verification: batch smoke, differential correctness tests, memory/resource report, benchmark
    smoke.
  - Non-goals: no daemon/service, no remote server, no hidden fast mode.
  - Claim boundary: scoped local session optimization only.
  - Fallback boundary: session reuse cannot change execution provider or claim status.
  - Dependencies/blockers: prepared/native row schema, source/prepared-state reuse, and correctness
    diff harness.
  - Ledger rule: ledger entry must include reuse/fusion evidence and disabled paths.

- [ ] GAR-RUNTIME-IMPL-1I local output writer registry and replay proof
  - Source: `GAR-RUNTIME-COMPLETE-1F`, OutputPlan contract, result-sink proof.
  - Current state: generated-source local output smokes exist; a general local sink writer registry
    and replay proof across source types are incomplete.
  - Next slice outcome: implement local Vortex, JSONL, CSV, and one columnar compatibility output
    writer with `OutputPlan`, output digest, and replay verification.
  - User-visible surface: Python `.write(...)`, CLI write command, recipes, website status.
  - Implementation scope: sink writer registry, output schema mapping, output digests, replay
    verifier, capability rows.
  - Evidence required: output plan id/digest, output format/location/schema, write mode/timing,
    replay status, output Native I/O certificate, metadata fidelity/loss, no-fallback fields.
  - Acceptance: local outputs are decoupled from input format; replay proof exists where claimed;
    unsupported formats block.
  - Verification: local output smoke per supported format, replay tests, capability snapshots,
    use-case checks.
  - Non-goals: no object-store write, no table commit, no production sink claim.
  - Claim boundary: scoped local writer support only.
  - Fallback boundary: compatibility output is export/translation, not fallback execution.
  - Dependencies/blockers: output schema mapping, writer dependency approval, and replay verifier.
  - Ledger rule: ledger entry must list exact writer formats and replay status.

- [ ] GAR-RUNTIME-IMPL-1J cross-format output fanout runtime
  - Source: `GAR-RUNTIME-COMPLETE-1F`, `GAR-IOREUSE-1D`, fanout benchmark plan.
  - Current state: fanout is represented by report/evidence scaffolding, not complete runtime.
  - Next slice outcome: fan out one admitted SourceState or VortexPreparedState into multiple local
    outputs in a single workflow with shared preparation evidence.
  - User-visible surface: Python fanout helper, CLI fanout command, benchmark family
    `io_reuse_and_fanout`, website status.
  - Implementation scope: fanout planner, OutputPlan reuse, sink artifact list, per-output replay,
    benchmark rows.
  - Evidence required: source/prepared/output reuse hits, fanout output count, output digests,
    per-output correctness/replay, output timing decomposition, no-fallback fields.
  - Acceptance: one prepared source can write multiple admitted local formats; one-shot timing and
    reuse/fanout timing remain separate.
  - Verification: fanout smoke, correctness digest per output, replay checks, benchmark harness.
  - Non-goals: no object-store fanout, no lakehouse commit, no performance claim.
  - Claim boundary: local fanout only.
  - Fallback boundary: no external write engines.
  - Dependencies/blockers: `GAR-RUNTIME-IMPL-1I`, SourceState/VortexPreparedState reuse, and
    fanout benchmark row schema.
  - Ledger rule: ledger entry must include fanout formats, timings, and blocked outputs.

- [ ] GAR-RUNTIME-IMPL-1K object-store runtime ladder, read side first
  - Source: `GAR-RUNTIME-COMPLETE-1G`, `GAR-COMPAT-1C`, `GAR-SCALE-1E`.
  - Current state: S3/GCS/ADLS are report-only/blocked; object-store runtime I/O is not supported.
  - Next slice outcome: implement the first safe read-side ladder stages only when admitted:
    URI parse, credential/effect policy, no-credential public-read policy, byte-range read, and
    streaming/full-file read evidence.
  - User-visible surface: CLI/Python object-store capability and diagnostics, status/use-case docs.
  - Implementation scope: URI parser, credential policy, network-effect gate, request planner,
    byte-range adapter, local cache boundary.
  - Evidence required: provider, credential policy status, network effect status, byte ranges,
    ETag/version, Native I/O certificate, fallback/external-engine fields.
  - Acceptance: public read and authenticated read are separate gates; unsupported providers or
    credentials block without probing unless explicitly enabled.
  - Verification: policy tests, mocked/local-emulator read tests if admitted, capability snapshots,
    release readiness checks.
  - Non-goals: no object-store write, no table commit, no production object-store claim.
  - Claim boundary: per-provider read-stage support only after proof.
  - Fallback boundary: object-store access does not authorize external compute engines.
  - Dependencies/blockers: credential policy, network-effect policy, dependency/license review, and
    provider test harness.
  - Ledger rule: ledger entry must name provider, stage, credential posture, and proof refs.

- [ ] GAR-RUNTIME-IMPL-1L table metadata and snapshot-scan runtime ladder
  - Source: `GAR-RUNTIME-COMPLETE-1G`, `GAR-COMPAT-1D`, table/lakehouse boundary docs.
  - Current state: Iceberg/Delta/Hudi table metadata and runtime are report-only/blocked.
  - Next slice outcome: implement a local or fixture-backed table metadata read and snapshot scan
    for one table format, with deterministic blockers for append/merge/delete/commit.
  - User-visible surface: capability view, CLI/Python table diagnostics, status/use-case docs.
  - Implementation scope: table metadata parser, manifest/snapshot reader, scan admission,
    unsupported commit diagnostics.
  - Evidence required: table format, snapshot id, manifest/data-file counts, scan status, commit
    status, rollback status, Native I/O refs, no-fallback fields.
  - Acceptance: metadata scan does not imply table write or commit support; unsupported table
    behaviors block with stable codes.
  - Verification: metadata fixture tests, snapshot scan smoke, release readiness metadata, website
    status checks.
  - Non-goals: no lakehouse commit, no object-store runtime unless separately admitted.
  - Claim boundary: table metadata/snapshot read only for the admitted format.
  - Fallback boundary: no external lakehouse engine or catalog engine executes ShardLoom work.
  - Dependencies/blockers: table-format dependency review, metadata fixtures, and object-store
    runtime only if non-local table refs are admitted.
  - Ledger rule: ledger entry must list exact format/stage and blocked table operations.

- [ ] GAR-RUNTIME-IMPL-1M local split manifests and bounded-memory execution
  - Source: `GAR-RUNTIME-COMPLETE-1H`, `GAR-SCALE-1B`, `GAR-SCALE-1C`.
  - Current state: scale evidence fields exist, but split-native execution and bounded-memory
    runtime are not claimable.
  - Next slice outcome: process one large local input through a split manifest under an explicit
    memory budget with deterministic blockers when operators cannot honor the envelope.
  - User-visible surface: scale benchmark profile, CLI/Python execution envelope, status docs.
  - Implementation scope: split manifest, split scheduler, memory budget checks, per-split evidence,
    correctness aggregation.
  - Evidence required: scale profile, data volume, split count, memory budget, peak memory,
    per-split rows/timing/output refs, no-fallback fields.
  - Acceptance: split execution does not imply distributed execution; memory envelope violations
    block or spill deterministically before process OOM.
  - Verification: split manifest tests, local stress smoke, scale benchmark contract tests.
  - Non-goals: no remote workers, no object-store split execution, no any-volume claim.
  - Claim boundary: declared local resource envelope only.
  - Fallback boundary: no external engine for partitioning, scheduling, or execution.
  - Dependencies/blockers: SourceState split metadata, operator row partitioning, and correctness
    aggregation.
  - Ledger rule: ledger entry must record resource envelope and data volume.

- [ ] GAR-RUNTIME-IMPL-1N spill, shuffle, retry, and commit safety for scale-grade local runtime
  - Source: `GAR-RUNTIME-COMPLETE-1H`, `GAR-SCALE-1C`, `GAR-SCALE-1D`, `GAR-SCALE-1E`.
  - Current state: spill/backpressure/shuffle/retry evidence contracts exist, but scale-grade local
    runtime is not implemented.
  - Next slice outcome: add scoped spill/backpressure plus shuffle/repartition/retry/idempotency
    evidence for one group-by/join/fanout scenario.
  - User-visible surface: scale benchmark rows, execution envelope, status docs.
  - Implementation scope: spill manager, shuffle plan, retry/idempotency keys, output commit status,
    cleanup semantics.
  - Evidence required: spill bytes/files/cleanup, shuffle strategy/bytes, skew status, retry count,
    idempotency key, output commit status, correctness digest, no-fallback fields.
  - Acceptance: operators either run under budget, spill with proof, or block; retries are explicit;
    no hidden full materialization occurs under scale-grade mode.
  - Verification: spill/backpressure tests, shuffle correctness tests, retry/idempotency tests,
    scale benchmark smoke.
  - Non-goals: no distributed shuffle claim, no object-store spill unless separately admitted.
  - Claim boundary: local scale-grade scenario only.
  - Fallback boundary: external engines remain baselines/oracles only.
  - Dependencies/blockers: `GAR-RUNTIME-IMPL-1M`, spill storage policy, shuffle correctness fixtures,
    and output commit evidence.
  - Ledger rule: ledger entry must include spill/shuffle/retry evidence refs.

- [ ] GAR-RUNTIME-IMPL-1O distributed execution remains report-only with runtime-safe blockers
  - Source: `GAR-RUNTIME-COMPLETE-1H`, `GAR-SCALE-1F`.
  - Current state: ShardLoom is local/in-process; distributed protocol fields are report-only.
  - Next slice outcome: expose coordinator/worker/task/split protocol diagnostics and fail closed for
    remote execution requests until a real distributed runtime is approved.
  - User-visible surface: CLI/Python capability views, status matrix, compute-flow docs.
  - Implementation scope: distributed request schema, protocol diagnostics, blocker codes,
    readiness checks.
  - Evidence required: coordinator/worker invoked flags, task attempt fields, distributed claim
    status, remote worker invoked false, fallback/external-engine fields.
  - Acceptance: remote worker requests never run accidentally; report-only protocol is visible and
    cannot be confused with runtime support.
  - Verification: capability snapshots, unsupported diagnostic tests, website status checks.
  - Non-goals: no daemon, cluster, remote worker, managed platform proof, or distributed claim.
  - Claim boundary: report-only protocol and blockers.
  - Fallback boundary: remote engines cannot be used as ShardLoom fallback.
  - Dependencies/blockers: no distributed runtime is admitted until a later approved design and proof
    suite exists.
  - Ledger rule: ledger entry must record blocker behavior and remaining runtime proof requirements.

- [ ] GAR-RUNTIME-IMPL-1P local control-plane API and opt-in evidence export runtime
  - Source: `GAR-RUNTIME-COMPLETE-1I`, RFC 0035, `GAR-NOVEL-1B`, `GAR-NOVEL-1C`,
    `GAR-COMMERCIAL-1D`.
  - Current state: REST/remote API, OpenLineage, OpenTelemetry, and enterprise export surfaces are
    report-only or blocked.
  - Next slice outcome: implement safe local file export for ShardLoom JSON plus optional
    OpenLineage/OTel-shaped documents, and add fail-closed loopback control-plane diagnostics.
  - User-visible surface: CLI/Python evidence export commands, local control-plane capability view,
    docs.
  - Implementation scope: export pack writer, schema versions, redaction policy, local path
    validation, control-plane diagnostics.
  - Evidence required: export format/version/path, redaction report, no-network-by-default,
    permissions, lifecycle status, no-fallback fields.
  - Acceptance: exports are opt-in local files; no network exporter runs by default; secret/path
    redaction is enforced.
  - Verification: export contract tests, redaction tests, capability snapshots, website readiness.
  - Non-goals: no production REST server, no lineage backend, no telemetry collector integration.
  - Claim boundary: local evidence export only.
  - Fallback boundary: observability/export cannot trigger external compute.
  - Dependencies/blockers: evidence envelope stability, redaction policy, and export schema version.
  - Ledger rule: ledger entry must list supported export formats and blocked network effects.

- [ ] GAR-RUNTIME-IMPL-1Q adapter, extension, UDF, and Foundry runtime boundaries
  - Source: `GAR-RUNTIME-COMPLETE-1I`, RFC 0011, RFC 0023, RFC 0036.
  - Current state: wrappers/connectors, UDF/plugin execution, effectful operations, and Foundry
    runtime/package proof are report-only or blocked.
  - Next slice outcome: implement typed local adapter wrappers and extension manifests as
    inspectable capabilities, with deterministic blockers for UDF execution, network effects, and
    Foundry production/runtime claims.
  - User-visible surface: capability view, Python/CLI adapters, Foundry dev-stack docs.
  - Implementation scope: adapter registry, extension manifest validation, sandbox/effect policy
    diagnostics, Foundry local proof flags.
  - Evidence required: adapter id/version, permission/effect status, manifest digest, runtime
    invoked flags, foundry/spark invoked flags, no-fallback fields.
  - Acceptance: users can inspect adapters/extensions without executing them; effectful paths remain
    explicitly blocked unless separately admitted.
  - Verification: manifest tests, capability snapshots, Foundry proof docs checks, release readiness.
  - Non-goals: no UDF sandbox runtime, no marketplace claim, no Foundry production support.
  - Claim boundary: capability/inspection and local proof only.
  - Fallback boundary: adapters/extensions must never execute external engines as ShardLoom work.
  - Dependencies/blockers: extension security policy, dependency/license review, and Foundry proof
    environment.
  - Ledger rule: ledger entry must list adapters admitted and effects blocked.

- [ ] GAR-RUNTIME-IMPL-1R package install smoke and release-channel proof
  - Source: `GAR-RUNTIME-COMPLETE-1J`, package-channel readiness matrix, release engineering docs.
  - Current state: local release dry-run and package metadata exist; public package channels are not
    ready or approved.
  - Next slice outcome: create a no-publication clean install proof that installs the Python
    package from a local artifact and runs source-free plus prepared/native smoke commands.
  - User-visible surface: README, getting-started docs, package metadata, release report.
  - Implementation scope: package build/dry-run scripts, clean venv install smoke, uninstall smoke,
    checksum/SBOM/provenance dry run.
  - Evidence required: build artifacts, install/uninstall commands, smoke output, checksums, SBOM,
    provenance, API/schema compatibility status.
  - Acceptance: a user can follow one documented local install path; no channel is marked ready
    without proof and explicit human approval.
  - Verification: clean venv package smoke, release readiness tests, package channel checker,
    `python -m compileall -q python/src python/tests scripts`.
  - Non-goals: no upload, tag, feedstock, marketplace submission, signing key, or public release.
  - Claim boundary: local install proof only.
  - Fallback boundary: packaging must not add fallback engine dependencies.
  - Dependencies/blockers: release gates, package metadata stability, provenance/SBOM tooling, and
    explicit human approval for any publication.
  - Ledger rule: ledger entry must include artifact paths and no-publication posture.

- [ ] GAR-RUNTIME-IMPL-1S claim-grade correctness and benchmark gate
  - Source: `GAR-RUNTIME-COMPLETE-1K`, benchmark publishing, correctness harness, release readiness.
  - Current state: evidence scaffolding exists, but runtime claims are not automatically tied to all
    required correctness/benchmark/certificate refs.
  - Next slice outcome: add a claim matrix that maps every supported runtime path to required tests,
    benchmark artifacts, execution certificates, Native I/O refs, unsupported diagnostics, and
    website/docs copy gates.
  - User-visible surface: release readiness report, benchmark page, status matrix, docs.
  - Implementation scope: claim matrix schema, validators, benchmark artifact completeness gate,
    README/website claim checks.
  - Evidence required: per-claim refs, benchmark profile/environment, certificate ids,
    materialization/decode refs, security/package refs, no-fallback fields.
  - Acceptance: missing evidence fails closed; old artifacts cannot be presented as latest proof;
    external baselines stay baseline-only.
  - Verification: release readiness tests, benchmark completeness checker, website readiness,
    workspace fmt/clippy/tests.
  - Non-goals: no performance, superiority, Spark-replacement, or production claim unless gates pass.
  - Claim boundary: claim-grade only per workload/evidence row.
  - Fallback boundary: external engines remain baselines/oracles only.
  - Dependencies/blockers: evidence emitted by runtime slices, benchmark artifact completeness, and
    release readiness validators.
  - Ledger rule: ledger entry must list passed and blocked claims.

- [ ] GAR-RUNTIME-IMPL-1T website, atlas, and docs parity gate for every runtime promotion
  - Source: `GAR-RUNTIME-COMPLETE-1L`, Use Case Atlas, Field Guide, website readiness checks.
  - Current state: public docs and website can lag runtime status unless updated with each
    implementation PR.
  - Next slice outcome: make runtime promotions fail readiness unless matching use-case, status,
    compute-flow, benchmark, and README/website surfaces are updated.
  - User-visible surface: `shardloom.io`, README, docs/use-cases, status and benchmark pages.
  - Implementation scope: website static generator, use-case index/status matrix, compute-flow
    snapshot, readiness scripts.
  - Evidence required: source refs, generated timestamps where applicable, status mappings, no
    stale report-only wording, asset/canonical/OG/sitemap checks.
  - Acceptance: non-experts can answer what is supported, how to try it, what evidence exists, and
    what remains blocked after every runtime promotion.
  - Verification: `python website/build_static_pages.py`, `python scripts/check_website_readiness.py`,
    `node website/validate_static_assets.js`, use-case coverage/backlink/glossary checks.
  - Non-goals: no marketing overclaims, no live Cloudflare benchmark execution, no framework
    migration unless separately approved.
  - Claim boundary: website explains runtime state; it does not create support claims.
  - Fallback boundary: public pages must keep no-fallback/no-external-engine semantics visible.
  - Dependencies/blockers: runtime promotion metadata, use-case index coverage, and static page
    generator support.
  - Ledger rule: every completed runtime slice must reference the corresponding website/docs parity
    update or explain why none was required.

#### GAR-RUNTIME-IMPL-2 - Full Runtime Gap Closure Queue

This closure queue converts the remaining "fully usable compute engine" gaps into explicit
implementation-ready runtime slices. It does not replace `GAR-RUNTIME-IMPL-1B` through
`GAR-RUNTIME-IMPL-1T`; it makes hidden subwork concrete so none of the broad runtime tracks can be
closed by documentation, report-only matrices, or partial smoke tests alone. When a closure item
overlaps an earlier runtime slice, the completed ledger must name both IDs and attach the exact
runtime/evidence proof.

- [ ] GAR-RUNTIME-IMPL-2A universal local input adapter runtime coverage
  - Source: `GAR-RUNTIME-COMPLETE-1F`, `GAR-IOREUSE-1A`, universal compatibility scoreboard,
    `docs/architecture/universal-input-contract.md`.
  - Current state: selected local compatibility imports and SourceState evidence exist; broad local
    CSV/JSONL/JSON/Parquet/Arrow IPC/Avro/ORC runtime adapter coverage is not complete, and
    unsupported formats can still be easier to see in capability reports than to use.
  - Next slice outcome: implement a first-class local input adapter registry that admits supported
    local file formats, emits `SourceState` for every requested source, and blocks unsupported local
    and non-local formats with deterministic diagnostics before execution.
  - User-visible surface: CLI read/execute commands, Python `ctx.read_*` helpers, capability/status
    views, use-case recipes, benchmark source-format rows.
  - Implementation scope: adapter registry, format sniffing/admission, schema/dtype inference,
    SourceState digest/fingerprint, local file readers, deterministic blocker codes.
  - Evidence required: source format/location/fingerprint, schema digest, row-count knowledge,
    SourceState id/digest, source read/decode/materialization fields, no-fallback/no-external-engine
    fields, claim gate.
  - Acceptance: each local format is classified as runtime-supported, smoke-supported, blocked, or
    unsupported with a stable reason; SourceState support does not imply Vortex-native execution;
    remote/object-store/database/table inputs fail closed unless separately admitted.
  - Verification: adapter unit tests, source-state snapshot tests, CLI/Python smoke per admitted
    format, unsupported diagnostic snapshots, release readiness metadata.
  - Non-goals: no object-store reads, database connectors, table/lakehouse runtime, or production
    universal adapter claim.
  - Claim boundary: scoped local input adapter support only, per admitted format.
  - Fallback boundary: adapters cannot call pandas, Polars, DuckDB, DataFusion, Spark, or Vortex
    query-engine integrations for execution.
  - Dependencies/blockers: dependency/license review for non-default formats, SourceState schema,
    correctness fixtures.
  - Ledger rule: ledger entry must list every format promoted and every major format still blocked.

- [ ] GAR-RUNTIME-IMPL-2B Vortex-native read, prepared-state, and write lifecycle
  - Source: `GAR-RUNTIME-COMPLETE-1E`, `GAR-RUNTIME-COMPLETE-1F`, `GAR-IOREUSE-1B`,
    Vortex-first provider docs.
  - Current state: prepared/native smoke and Vortex output posture exist, but the full local Vortex
    lifecycle is not yet a simple user-facing path from read or prepare through compute, write,
    reopen, scan, and evidence verification.
  - Next slice outcome: implement a compact Vortex lifecycle command/API path that prepares or reads
    a local Vortex artifact, executes an admitted projection/filter/limit or aggregate, writes a
    Vortex output, reopens it, and verifies scan/result/evidence digests.
  - User-visible surface: CLI Vortex workflow command, Python helper, benchmark rows, README/getting
    started, website Field Guide and status.
  - Implementation scope: VortexPreparedState registry, artifact refs/digests, source-backed scan,
    Vortex sink writer, reopen verifier, lifecycle evidence envelope.
  - Evidence required: prepared state id/digest, Vortex artifact ref/digest, layout/encoding/stats
    summary, source-backed scan fields, output Native I/O certificate, reopen digest,
    materialization/decode fields, no-fallback/no-external-engine fields.
  - Acceptance: a user can run one local Vortex-native lifecycle without external engines; each
    unsupported Vortex feature blocks with a stable reason; compatibility-import timing remains
    separate from prepared/native runtime timing.
  - Verification: lifecycle smoke, source-backed scan tests, Vortex writer/reopen tests,
    benchmark harness contract tests, website/readme readiness.
  - Non-goals: no object-store Vortex artifacts, encoded-native blanket claim, or performance claim.
  - Claim boundary: local Vortex lifecycle only for admitted operators and artifact layout.
  - Fallback boundary: upstream Vortex native APIs may be providers only through certified
    boundaries; Vortex query-engine integrations remain prohibited.
  - Dependencies/blockers: Vortex dependency/version gate, writer implementation, operator support.
  - Ledger rule: ledger entry must include artifact refs, operator scope, and reopen proof.

- [ ] GAR-RUNTIME-IMPL-2C SQL planner expansion beyond first local smoke
  - Source: `GAR-RUNTIME-COMPLETE-1B`, `GAR-RUNTIME-IMPL-1B`, `GAR-RUNTIME-IMPL-1E`,
    SQL/DataFrame capability matrix.
  - Current state: source-free SQL is scoped, and local-source projection/filter/limit is planned or
    in progress; aggregates, group-by, joins, order/top-N, casts, functions, and multi-source SQL are
    not broadly supported.
  - Next slice outcome: expand SQL lowering in staged groups: aggregate/group-by, order/top-N,
    simple equi-join, casts/null predicates, and deterministic unsupported diagnostics for
    functions, subqueries, windows, catalogs, UDFs, and effectful operations not yet admitted.
  - User-visible surface: CLI SQL command, Python `ctx.sql(...)`, explain output, recipes, status
    matrix.
  - Implementation scope: parser grammar, binder, logical plan nodes, physical lowering, operator
    admission, diagnostic codes, correctness fixtures.
  - Evidence required: parser/binder/planner flags, logical/physical plan digests, operator families,
    source/sink certificates, correctness digest, unsupported diagnostic code, no-fallback fields.
  - Acceptance: every admitted SQL shape executes through ShardLoom-native operators only; every
    unsupported construct is rejected before execution; SQL docs clearly distinguish scoped runtime
    support from production SQL compatibility.
  - Verification: parser/binder/planner tests, SQL smoke tests per admitted shape, unsupported
    snapshot tests, release readiness metadata, website/use-case checks.
  - Non-goals: no ANSI SQL parity, catalog runtime, broad UDF support, or production SQL claim.
  - Claim boundary: scoped SQL frontend runtime per admitted syntax family.
  - Fallback boundary: no external SQL/query engine may execute residual work.
  - Dependencies/blockers: operator semantics, input/output adapters, planner explain schema.
  - Ledger rule: ledger entry must list syntax families admitted and blockers preserved.

- [ ] GAR-RUNTIME-IMPL-2D Python first-class end-user workflow completion
  - Source: `GAR-RUNTIME-COMPLETE-1C`, Use Case Atlas, `python/README.md`, first-10-minutes docs.
  - Current state: Python wrappers and capability views exist, but a non-expert cannot yet rely on
    one ergonomic Python path for generated source, local source, prepared Vortex, transform/write,
    evidence inspection, and unsupported diagnostics.
  - Next slice outcome: ship one documented Python API path covering `ctx.range` or `ctx.from_rows`,
    `ctx.read_csv` or admitted local read, projection/filter/limit/basic aggregate, local write, and
    evidence inspection, with blocked diagnostics for unsupported methods.
  - User-visible surface: Python package API, examples, README, getting-started docs, website use
    cases.
  - Implementation scope: Python context/session, query-builder methods, CLI bridge or native
    bindings as established locally, typed report objects, examples/tests.
  - Evidence required: method admission, plan digest, execution mode, input/output evidence refs,
    generated/source-state refs, correctness digest, fallback/external-engine fields.
  - Acceptance: first-10-minute Python workflow runs end to end from import through evidence
    inspection; unsupported API calls are actionable and do not fall back to pandas/Polars.
  - Verification: Python unit/integration tests, example smoke, compileall, website/use-case
    coverage, release readiness metadata.
  - Non-goals: no pandas/Polars backend, notebook production claim, package publication, or broad
    DataFrame parity claim.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: Python may orchestrate ShardLoom; it must not execute compute in external
    engines.
  - Dependencies/blockers: SQL/operator coverage, local input/output writers, package metadata.
  - Ledger rule: ledger entry must include exact runnable Python snippets and evidence output.

- [ ] GAR-RUNTIME-IMPL-2E execution envelope and certificate contract unification
  - Source: execution certificates, Native I/O certificates, GeneratedSourceCertificate,
    benchmark/report schemas, claim gates.
  - Current state: many surfaces emit no-fallback and claim-boundary evidence, but runtime promotion
    can still require bespoke fields per command or benchmark row.
  - Next slice outcome: define and implement a shared runtime execution envelope used by CLI,
    Python, benchmark rows, website artifacts, and release gates for every supported runtime path.
  - User-visible surface: JSON reports, Python typed reports, benchmark artifacts, release
    readiness, website evidence pages.
  - Implementation scope: shared schema/version, field mapping, report adapters, validators,
    backward-compatible aliases where needed.
  - Evidence required: execution mode, engine mode, evidence level, source/generated/source-free
    refs, output refs, materialization/decode refs, fallback/external-engine flags, claim gate.
  - Acceptance: every runtime command has the required envelope fields; missing no-fallback,
    certificate, or claim-gate fields fail readiness; report-only rows cannot masquerade as
    runtime-supported.
  - Verification: schema contract tests, release readiness metadata tests, benchmark artifact
    completeness, website readiness.
  - Non-goals: no new runtime behavior by itself, no claim upgrade from schema consolidation.
  - Claim boundary: envelope standardizes evidence; it does not create support claims.
  - Fallback boundary: envelope must always expose fallback/external-engine status.
  - Dependencies/blockers: stable field names and migration policy.
  - Ledger rule: ledger entry must record schema version and migrated surfaces.

- [ ] GAR-RUNTIME-IMPL-2F local output writers and fanout promoted to ordinary user workflows
  - Source: `GAR-RUNTIME-IMPL-1I`, `GAR-RUNTIME-IMPL-1J`, OutputPlan, result-sink replay,
    fanout benchmark plan.
  - Current state: output writer and fanout work is planned as runtime infrastructure; it is not yet
    exposed as ordinary CLI/Python workflows across all admitted local sources.
  - Next slice outcome: make local writes and multi-output fanout a first-class user flow from
    generated, local-file, and prepared Vortex inputs into Vortex plus selected compatibility
    outputs with replay proof.
  - User-visible surface: Python `.write(...)` and `.fanout(...)`, CLI write/fanout command,
    recipes, benchmark rows, website status.
  - Implementation scope: OutputPlan builder, writer registry, schema translation, output digests,
    replay verifier, multi-output orchestration.
  - Evidence required: output plan id/digest, format/location/schema, output timing, replay status,
    metadata fidelity/loss, correctness digest per output, no-fallback fields.
  - Acceptance: input and output formats are decoupled; unsupported writers block; every supported
    writer has replay/correctness evidence or a documented lower evidence level.
  - Verification: writer smoke per format, fanout smoke, replay tests, use-case coverage, benchmark
    harness tests.
  - Non-goals: no object-store write, table commit, production sink claim, or performance claim.
  - Claim boundary: local output/fanout support per format and evidence level.
  - Fallback boundary: compatibility writes are exports, not external-engine execution.
  - Dependencies/blockers: local writers, schema mapping, replay verifier.
  - Ledger rule: ledger entry must list promoted writers and fanout combinations.

- [ ] GAR-RUNTIME-IMPL-2G database and warehouse import/export runtime boundary
  - Source: `GAR-COMPAT-1E`, universal compatibility scoreboard, adapter/governance docs.
  - Current state: SQLite, Postgres, MySQL, ODBC/JDBC, Snowflake, BigQuery, Databricks SQL, and
    warehouse-style connectors are report-only or blocked; they are not first-class runtime paths.
  - Next slice outcome: implement a safe boundary for the first local database import/export smoke
    if admitted, preferably SQLite fixture import/export, while keeping networked databases and
    warehouses blocked with explicit credential/effect diagnostics.
  - User-visible surface: capability view, CLI/Python adapter diagnostics, use cases, status page.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    import/export report schema, blocked network connector diagnostics.
  - Evidence required: connector type, credential requirement/status, network requirement/status,
    import/export direction, rows/bytes, output certificate refs, fallback/external-engine fields.
  - Acceptance: external databases are never used as fallback engines; import/export is separated
    from query pushdown; networked providers fail closed unless explicitly admitted later.
  - Verification: SQLite/local fixture smoke if admitted, connector capability snapshots,
    unsupported network diagnostic tests, release readiness.
  - Non-goals: no query pushdown, warehouse execution, JDBC/ODBC production support, or credential
    resolution by default.
  - Claim boundary: scoped import/export boundary only, per connector.
  - Fallback boundary: connectors cannot execute ShardLoom query plans.
  - Dependencies/blockers: dependency/license review, credential policy, fixture data.
  - Ledger rule: ledger entry must separate local fixture support from blocked network connectors.

- [ ] GAR-RUNTIME-IMPL-2H object-store write, commit, and recovery promotion after read proof
  - Source: `GAR-RUNTIME-IMPL-1K`, `GAR-SCALE-1E`, object-store request planner,
    commit/retry/idempotency planning.
  - Current state: object-store read/write/commit remains blocked or report-only; request planning
    evidence exists but does not perform provider I/O.
  - Next slice outcome: after read-side proof, implement staged object-store write planning and then
    one safe write/commit/recovery smoke only in an approved provider/emulator profile.
  - User-visible surface: object-store capability view, CLI/Python diagnostics, status/use-case docs,
    scale benchmark rows when applicable.
  - Implementation scope: provider abstraction, credential/effect gates, write staging, commit
    protocol, idempotency, cleanup/retry, mocked or local-emulator harness.
  - Evidence required: provider/profile, credential policy, network effect status, staged object
    refs, commit protocol, idempotency key, rollback/cleanup status, no-fallback fields.
  - Acceptance: object-store read, write, and commit are separate gates; no credentials or network
    probes run by default; failed writes have deterministic cleanup/recovery evidence.
  - Verification: policy tests, emulator/mocked write smoke, idempotency/retry tests, release
    readiness, website status checks.
  - Non-goals: no blanket S3/GCS/ADLS support, lakehouse table commit, production object-store
    claim, or managed-platform claim.
  - Claim boundary: provider/profile-specific technical-preview proof only.
  - Fallback boundary: object-store providers are storage effects, not query engines.
  - Dependencies/blockers: read-side proof, credential/security review, provider harness.
  - Ledger rule: ledger entry must record provider, profile, commit status, and cleanup evidence.

- [ ] GAR-RUNTIME-IMPL-2I table/lakehouse append and commit runtime after snapshot proof
  - Source: `GAR-RUNTIME-IMPL-1L`, `GAR-COMPAT-1D`, table/lakehouse commit semantics gate.
  - Current state: table metadata and snapshot scans are planned; append, merge/update/delete,
    tombstone/delete-file handling, commit, rollback, and catalog integration are blocked.
  - Next slice outcome: after local metadata/snapshot proof, implement one fixture-backed append
    or metadata-only commit rehearsal for an admitted table format, with merge/delete/rollback
    blockers remaining explicit unless separately proved.
  - User-visible surface: table capability view, CLI/Python diagnostics, use-case docs, status page.
  - Implementation scope: table format adapter, manifest/snapshot writer or rehearsal, commit
    record, rollback blocker, schema evolution diagnostics.
  - Evidence required: table format, snapshot id, manifest/data-file counts, operation type,
    commit/rollback status, object-store involvement, Native I/O refs, no-fallback fields.
  - Acceptance: metadata scan, table read, append, merge/delete, and commit are distinct gates;
    a local fixture proof does not imply production lakehouse or object-store support.
  - Verification: table fixture tests, commit rehearsal smoke, unsupported operation diagnostics,
    release readiness.
  - Non-goals: no production Iceberg/Delta/Hudi claim, no catalog service, no object-store table
    runtime unless separately admitted.
  - Claim boundary: one table-format operation in a declared fixture/profile only.
  - Fallback boundary: no external catalog, lakehouse engine, or query engine executes work.
  - Dependencies/blockers: dependency/license review, table fixtures, object-store gates if remote.
  - Ledger rule: ledger entry must list table format, operation, and blocked table behaviors.

- [ ] GAR-RUNTIME-IMPL-2J live, hybrid, remote API, and control-plane promotion ladder
  - Source: RFC 0034, RFC 0035, compute-flow engine modes, status board.
  - Current state: batch mode has local smoke evidence; live/hybrid modes, REST/event APIs, and
    remote result delivery are report-only or blocked.
  - Next slice outcome: implement only the first safe local control-plane surface and engine-mode
    diagnostics, then define runtime blockers for live/hybrid state, event delivery, and remote
    data-plane behavior until explicit proofs exist.
  - User-visible surface: CLI/Python engine-mode status, optional loopback/local API, compute-flow,
    website status.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, API schema,
    blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, data-plane status, lifecycle state,
    network effect policy, remote worker invoked status, fallback/external-engine fields.
  - Acceptance: batch/live/hybrid labels cannot imply unsupported runtime; any local API is
    opt-in and side-effect scoped; remote execution/data transfer stays blocked until proved.
  - Verification: API/engine-mode contract tests, unsupported diagnostics, website readiness,
    release readiness.
  - Non-goals: no production REST service, daemon, streaming engine, remote worker runtime, or
    exactly-once claim.
  - Claim boundary: local control-plane diagnostics or explicitly admitted technical-preview API
    only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle policy, evidence envelope, security review.
  - Ledger rule: ledger entry must record exposed API surface and blocked live/hybrid behaviors.

- [ ] GAR-RUNTIME-IMPL-2K UDF, extension, and effectful-operation runtime admission
  - Source: RFC 0011, RFC 0023, extension/plugin safety docs, modular extensibility docs.
  - Current state: extensions, UDFs, LLM/API/embedding/vector effects, and plugin execution are
    capability/report-only or blocked; inspectable manifests may exist without runtime execution.
  - Next slice outcome: add a strict runtime admission ladder: manifest inspection, pure local
    deterministic scalar UDF fixture if approved, sandbox/effect blockers, network/effect denial,
    and explicit capability rows for blocked LLM/API/embedding/vector operations.
  - User-visible surface: capability view, Python/CLI extension inspection, diagnostics, use cases.
  - Implementation scope: extension registry, manifest schema, deterministic/effect policy,
    sandbox boundary, UDF admission, blocked effect diagnostics.
  - Evidence required: extension id/version/digest, permission/effect status, determinism/null/type
    contract, sandbox status, materialization requirement, runtime invoked flags, no-fallback fields.
  - Acceptance: users can inspect extensions without executing them; effectful operations are
    blocked by default; any admitted UDF is local, deterministic, typed, and evidence-backed.
  - Verification: manifest validation tests, UDF admission/blocker tests, capability snapshots,
    release readiness.
  - Non-goals: no plugin marketplace, network effects, arbitrary Python execution, LLM/API calls, or
    production UDF sandbox claim.
  - Claim boundary: inspected capability or one scoped deterministic local UDF only.
  - Fallback boundary: extensions/UDFs must not delegate compute to external engines or services.
  - Dependencies/blockers: security/sandbox review, license/provenance review, effect policy.
  - Ledger rule: ledger entry must list admitted extension/UDF behaviors and denied effects.

- [ ] GAR-RUNTIME-IMPL-2L full public technical-preview usability release gate
  - Source: `GAR-RUNTIME-IMPL-1R`, `GAR-RUNTIME-IMPL-1S`, `GAR-RUNTIME-IMPL-1T`, release
    readiness docs, website public-preview readiness.
  - Current state: runtime capabilities are being added slice by slice, but final usability requires
    install, command/API examples, docs, website, benchmarks, security, and claim gates to agree.
  - Next slice outcome: add a single no-publication technical-preview release gate that proves the
    repo is usable from clean checkout or local package artifact through CLI/Python workflows,
    benchmark interpretation, docs/website status, and unsupported-path diagnostics.
  - User-visible surface: README, docs/getting-started, website, package metadata, release report.
  - Implementation scope: clean install/run scripts, example smoke matrix, package dry-run,
    benchmark artifact completeness, website/use-case readiness, security/legal checks.
  - Evidence required: install command, smoke commands, supported workflow matrix, blocked workflow
    matrix, benchmark manifest, website readiness report, SECURITY/LICENSE/NOTICE status,
    no-fallback fields.
  - Acceptance: a non-expert can install locally, run an admitted workflow, inspect evidence, and
    see blocked unsupported paths without reading phase-plan internals; no package is published
    without explicit human approval.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, git diff check.
  - Non-goals: no public package publication, production/platform/performance claim, Spark
    replacement claim, object-store/lakehouse/Foundry production claim, or hidden fast mode.
  - Claim boundary: public technical preview only, with workload-scoped claims.
  - Fallback boundary: release gates must fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime workflows, package metadata, docs/website
    parity, benchmark artifact policy.
  - Ledger rule: ledger entry must include the exact usability matrix and remaining unsupported
    paths.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
