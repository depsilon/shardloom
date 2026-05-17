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
  - Role: report-only GAR-PERF-2A reference for evidence-level runtime tiering across
    `minimal_runtime`, `certified`, and `full_replay`.
  - Status rule: defines evidence-level vocabulary only. Execution envelope schema, benchmark row
    schema, CLI/Python capability surfacing, website benchmark interpretation, and runtime behavior
    must remain represented by `GAR-PERF-2A` or later evidence-bearing slices before
    implementation.
- `docs/architecture/evidence-aware-logical-optimizer.md`
  - Role: report-only GAR-PERF-2B reference for optimizer rule registry and optimizer trace
    planning.
  - Status rule: defines rule families, before/after plan digest evidence, explain/benchmark trace
    fields, and no-fallback/claim boundaries only. Runtime rewrites, CLI/Python explain surfacing,
    benchmark row schema changes, and correctness smoke must remain represented by `GAR-PERF-2B` or
    later evidence-bearing slices.
- `docs/architecture/vortex-scan-pushdown-completion.md`
  - Role: report-only GAR-PERF-2C reference for Vortex Scan API filter/projection/limit pushdown
    completion across prepared/native scenario families.
  - Status rule: defines pushdown evidence and deterministic blocker requirements only. Scan
    request builder work, filter expression lowering, projection mask computation, limit/slice
    pushdown, capability matrix projection, and benchmark row schema changes must remain
    represented by `GAR-PERF-2C` or later evidence-bearing slices.
- `docs/architecture/compressed-encoded-kernel-registry.md`
  - Role: report-only GAR-PERF-2D reference for encoding-specific compressed/encoded kernel
    registry planning.
  - Status rule: defines registry rows, kernel admission evidence, deterministic blockers,
    materialization/decode boundaries, and claim gates only. Runtime kernel execution, benchmark row
    schema changes, capability matrix surfacing, and encoded-native claim promotion must remain
    represented by `GAR-PERF-2D` or later evidence-bearing slices.
- `docs/architecture/fused-operator-pipeline.md`
  - Role: report-only GAR-PERF-2E reference for fused local prepared/native operator pipelines.
  - Status rule: defines fused-pipeline evidence, correctness, materialization, and blocker
    requirements only. Runtime fusion, benchmark row schema changes, differential correctness tests,
    and claim-grade use must remain represented by `GAR-PERF-2E` or later evidence-bearing slices.
- `docs/architecture/in-process-session-runtime.md`
  - Role: report-only GAR-PERF-2F reference for the planned in-process `ShardLoomSession` runtime
    over prepared/native local artifacts.
  - Status rule: describes session state, cache, lifecycle, CLI/Python evidence, and no-fallback
    rules only. Runtime implementation, Python client exposure, benchmark row schema changes, and
    claim-grade use must remain represented by `GAR-PERF-2F` or later evidence-bearing slices.
- `docs/architecture/io-reuse-and-fanout-architecture.md`
  - Role: report-only GAR-IOREUSE-1 reference for universal source-state reuse, decoupled
    Vortex-prepared-state reuse, output-plan reuse, cross-format local fanout, cache invalidation,
    evidence-safe reuse levels, and Foundry generated-output fanout posture.
  - Status rule: defines the
    `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact`
    architecture and benchmark field vocabulary only. Runtime state caches, fanout writers,
    benchmark row schema changes, Foundry generated-output smoke, object-store I/O, table commits,
    and claim-grade use must remain represented by `GAR-IOREUSE-1*` or later evidence-bearing
    slices.
- `docs/architecture/allocation-buffer-pool-optimization.md`
  - Role: report-only GAR-PERF-2G reference for allocation profiling and scoped buffer-pool
    optimization across prepared/native local runtime paths.
  - Status rule: defines allocation/resource fields, buffer-reuse families, safety rules, and
    claim boundaries only. Runtime buffer pools, allocator hooks, benchmark row schema changes,
    memory/resource reports, and claim-grade use must remain represented by `GAR-PERF-2G` or later
    evidence-bearing slices.
- `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`
  - Role: report-only GAR-PERF-2H reference for optimized Cargo build profiles and a reproducible
    PGO/native benchmark lane.
  - Status rule: defines build-profile vocabulary, PGO workflow requirements, target-CPU-native
    boundaries, benchmark row fields, and release portability rules only. Cargo profile
    implementation, benchmark script changes, PGO smoke artifacts, and release-gate enforcement must
    remain represented by `GAR-PERF-2H` or later evidence-bearing slices.
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

- [ ] GAR-FLOW-1C direct transient format/operator/result-sink expansion gate
  - Source: `docs/architecture/compute-engine-flow-reference.md`; RFC 0033; RFC 0042.
  - Current state: `direct_compatibility_transient` has deterministic admission diagnostics and one
    scoped local CSV selective-filter smoke path; broader formats, operators, result sinks, and
    SQL/DataFrame direct transient runtime remain incomplete.
  - Next slice outcome: split direct transient follow-through into explicit unsupported diagnostics
    or one new scoped smoke path with evidence, depending on the selected format/operator.
  - User-visible surface: CLI benchmark rows, Python typed accessors if capability fields change,
    benchmark docs, and compute-flow docs.
  - Implementation scope: direct-transient admission/report fields, traditional benchmark runner
    coverage, focused Rust/Python tests, and docs.
  - Evidence required: correctness refs for any admitted smoke path; execution certificate refs;
    materialization/decode refs; policy/no-fallback refs; result-sink refs if output is written.
  - Acceptance: every newly named direct-transient path reports `support_status`,
    `claim_gate_status`, `fallback_attempted=false`, `external_engine_invoked=false`, and either
    deterministic unsupported diagnostics or scoped fixture evidence.
  - Verification: focused traditional-analytics direct-transient tests, benchmark harness contract
    test, Python compileall if Python fields change, and default GAR verification.
  - Non-goals: no broad direct-transient runtime, no SQL/DataFrame execution, no external engine,
    no Vortex-native claim, and no performance claim.
  - Fallback/claim boundary: direct transient remains scoped and cannot satisfy Vortex-native,
    production, Spark-displacement, or performance claims.
  - Dependencies/blockers: source/format-specific parser support, result-sink evidence, and
    operator coverage.
- [ ] GAR-FLOW-2O prepared/native encoded operator or next source-state follow-through
  - Source: `docs/architecture/compute-engine-flow-reference.md`;
    `docs/architecture/benchmark-suite-catalog.md`; RFC 0026; RFC 0042.
  - Current state: prepared/native Vortex rows have process reuse, source metadata reuse,
    hash/join dimension-label source-state reuse, distinct-count/high-cardinality
    category/metric source-state reuse, group-by/multi-key group/category/metric source-state
    reuse, sort/top-N/window ranked-metric source-state reuse, clean/cast plus dirty-CSV
    dirty-input source-state reuse, selective-filter/filter-projection source-state reuse,
    source-backed scan evidence, and scoped residual-native fixture paths; generalized row-state
    reuse beyond these families and encoded/native operator coverage remain incomplete.
  - Next slice outcome: add the next narrow prepared/native runtime improvement, such as another
    repeated-source-state family, a fused residual operator path with explicit timing, or one
    encoded/native operator/provider promotion with attached evidence.
  - User-visible surface: `traditional-analytics-vortex-batch-run`, comparative benchmark evidence
    rows, compute-flow docs, and benchmark docs.
  - Implementation scope: `shardloom-vortex/src/traditional_analytics.rs`, benchmark harness field
    mapping if needed, Rust tests, and architecture docs.
  - Evidence required: correctness refs, benchmark refs, execution certificate refs, Native I/O
    certificate refs, materialization/decode refs, source-backed scan refs, and policy/no-fallback
    refs.
  - Acceptance: the selected prepared/native path exposes explicit reuse/operator evidence,
    preserves child typed envelopes, and keeps residual-native versus encoded-native claim status
    unambiguous.
  - Verification: focused `shardloom-vortex` tests, traditional benchmark harness tests, cargo fmt,
    cargo test for touched crates, and `git diff --check`.
  - Non-goals: no persistent daemon/service runtime, no hidden fast mode, no SQL/DataFrame runtime,
    no object-store/lakehouse runtime, no broad CDC/table transaction claim, and no public
    performance/superiority claim.
  - Fallback/claim boundary: `fallback_attempted=false` and `external_engine_invoked=false`; only
    encoded/native operator claims with complete evidence may set encoded-native claim fields true.
  - Dependencies/blockers: Vortex-first provider check, correctness fixture coverage, benchmark
    evidence, Native I/O certificates, and claim gate evidence.

#### GAR-PERF-1 - End-To-End Prepared/Native Performance Architecture

- [ ] GAR-PERF-1A prepared/native batch benchmark refresh after source-state reuse
  - Source:
    - latest prepared/native batch runner and source-state reuse PRs, including PR #655.
    - `website/benchmarks.html`.
    - `benchmarks/traditional_analytics`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/architecture/benchmark-suite-catalog.md`.
  - Current state:
    - Compatibility rows prove universal I/O/evidence but include import, write, reopen, scan, and
      evidence costs.
    - Prepared/native batch runner exists.
    - Source metadata and source-state reuse now exists across selected scenario families.
    - Existing uploaded benchmark artifact may predate the latest source-state reuse work.
  - Next slice outcome:
    - Regenerate a fresh claim-safe benchmark artifact after PR #655.
    - Separate `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, and
      batch-runner rows.
    - Include `source_metadata_snapshot_*` and `source_state_*` evidence fields in comparative rows.
  - User-visible surface:
    - `website/benchmarks.html`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - benchmark JSON/Markdown artifacts under `target/`.
  - Implementation scope:
    - `benchmarks/traditional_analytics` runner and artifact handling.
    - website benchmark generator/readiness checks if generated output changes.
    - benchmark docs and claim-boundary wording.
  - Evidence required:
    - timing rows.
    - coverage rows.
    - `claim_gate_status`.
    - `execution_mode`.
    - source metadata and source-state reuse evidence.
    - materialization/decode evidence.
    - Native I/O evidence.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
  - Acceptance:
    - Fresh artifact clearly says it is evidence, not a leaderboard.
    - Prepared/native batch rows are visible separately from compatibility import rows.
    - Compatibility-import rows are not presented as pure query speed.
    - Source-state reuse fields propagate into comparative rows.
    - External engines remain baseline context only.
  - Verification:
    - focused benchmark smoke.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `python -m compileall -q benchmarks/traditional_analytics`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no performance/superiority claim.
    - no Spark replacement claim.
    - no production claim.
    - no benchmark cherry-picking or hidden fast mode.
  - Claim boundary:
    - Local pre-release benchmark evidence only.
  - Fallback boundary:
    - External engines are comparison baselines only; no external engine execution may be reported as
      ShardLoom execution.
  - Dependencies/blockers:
    - PR #655 or later source-state reuse merge, benchmark artifacts, website generation inputs.
- [ ] GAR-PERF-1B source-state reuse across all benchmark scenario families
  - Source:
    - GAR-FLOW-2H through GAR-FLOW-2N source-state reuse slices.
    - `traditional-analytics-vortex-batch-run`.
    - `docs/architecture/benchmark-suite-catalog.md`.
  - Current state:
    - Source metadata/source-state reuse exists for selected scenario families.
    - Remaining scenario families may still prepare or scan independently.
  - Next slice outcome:
    - Add a complete source-state reuse coverage matrix.
    - Identify missing families and add deterministic blockers, `source-state-not-needed`, or runtime
      reuse.
  - User-visible surface:
    - benchmark evidence rows.
    - compute-flow docs.
    - benchmark catalog.
  - Implementation scope:
    - `traditional-analytics-vortex-batch-run`.
    - benchmark row schema and Python harness propagation.
    - source-state evidence structs and docs.
  - Evidence required:
    - per-family `source_state_*` fields.
    - preparation timing.
    - reuse count.
    - source-state digest where applicable.
    - no-fallback evidence.
  - Acceptance:
    - Every scenario family is one of `source-state-reused`, `source-state-not-needed`,
      `blocked-with-reason`, or `unsupported-with-reason`.
    - No scenario silently redoes source preparation when reuse is available.
    - Coverage matrix names which families remain intentionally single-scenario or non-reusable.
  - Verification:
    - focused Rust tests per family.
    - benchmark harness contract test.
    - `git diff --check`
  - Non-goals:
    - no encoded-native claim.
    - no production performance claim.
    - no persistent daemon/service runtime.
  - Claim boundary:
    - Runtime-plumbing evidence only; not a public performance claim.
  - Fallback boundary:
    - `fallback_attempted=false`, `external_engine_invoked=false`.
  - Dependencies/blockers:
    - source-state family inventory, fixture coverage, and stable benchmark row schema.
- [ ] GAR-PERF-1C fused filter/project/limit and selection-vector execution path
  - Source:
    - Vortex Scan API and pushdown concepts.
    - encoded predicate provider evidence.
    - selection-vector metric aggregation work.
    - RFC 0026 and RFC 0042.
  - Current state:
    - Selective-filter encoded-predicate evidence is scoped.
    - Residual-native metric aggregation remains explicit.
  - Next slice outcome:
    - Implement or deterministically block a fused local prepared/native path for filter, projection,
      limit, and selected metric aggregation.
  - User-visible surface:
    - prepared/native benchmark rows.
    - `encoded_predicate_provider_*` fields.
    - compute capability matrix.
  - Implementation scope:
    - Vortex scan/projection/filter provider bridge.
    - selection-vector application.
    - benchmark row evidence.
    - focused Rust tests and benchmark smoke.
  - Evidence required:
    - filter-column encoding summary.
    - selected row count.
    - projection columns.
    - `data_decoded`.
    - `data_materialized`.
    - `operator_execution_class`.
    - `encoded_native_claim_allowed`.
    - no-fallback evidence.
  - Acceptance:
    - Fused path avoids unnecessary full-row materialization where evidence supports it.
    - Unsupported encodings remain deterministic blockers.
    - Claim fields distinguish residual-native, bridge-backed, and encoded-native evidence.
  - Verification:
    - selective-filter tests.
    - filter/projection/limit benchmark smoke.
    - benchmark harness contract test if row fields change.
    - `git diff --check`
  - Non-goals:
    - no generalized encoded-native claim without complete evidence.
    - no broad SQL/DataFrame runtime.
    - no public performance/superiority claim.
  - Claim boundary:
    - Encoded-native claim remains blocked unless end-to-end evidence supports it.
  - Fallback boundary:
    - No external-engine execution or fallback; unsupported encodings fail with deterministic
      diagnostics.
  - Dependencies/blockers:
    - Vortex-first provider check, encoded predicate evidence, correctness refs, materialization
      evidence.
- [ ] GAR-PERF-1D Bayesian performance and layout advisor report-only contract
  - Source:
    - benchmark evidence model.
    - resource sizing evidence fields.
    - prepared/native batch runner.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
  - Current state:
    - Resource sizing is currently rule/policy based.
    - Benchmark confidence is not Bayesian.
  - Next slice outcome:
    - Add a report-only advisor design for execution-mode recommendation, source-state reuse
      threshold, batch rows, target partition bytes, max parallelism, and layout/write choice.
  - User-visible surface:
    - advisor report docs, future CLI capability/report row, benchmark interpretation docs.
  - Implementation scope:
    - report schema/design doc, capability row, docs, and tests if a report command is added.
  - Evidence required:
    - `confidence`.
    - `uncertainty_reason`.
    - input evidence refs.
    - `advisor_version`.
    - `claim_gate_status=advisory_only`.
    - no-fallback evidence.
  - Acceptance:
    - Advisor never silently changes mode.
    - `auto` mode remains transparent and reports selected mode plus reason.
    - Advisor never upgrades claim status.
    - Advisory rows identify missing evidence instead of fabricating certainty.
  - Verification:
    - report snapshot tests if implemented.
    - release readiness metadata tests if claim gates are touched.
    - `git diff --check`
  - Non-goals:
    - no runtime decisioning without explicit opt-in.
    - no performance claims.
    - no object-store write, layout rewrite, or production layout recommendation.
  - Claim boundary:
    - Advisory/report-only; not claim-grade and not an optimizer decision.
  - Fallback boundary:
    - Advisor cannot invoke external engines, probes, credentials, or object-store I/O.
  - Dependencies/blockers:
    - stable benchmark evidence schema, resource sizing fields, and claim-gate policy.

#### GAR-PERF-2 - Evidence-Level Runtime Tiering

- [ ] GAR-PERF-2A evidence-level runtime tiering
  - Source:
    - benchmark evidence overhead.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/architecture/runtime-evidence-level-tiering.md`.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - result-sink replay evidence and Native I/O certificate rows.
  - Current state:
    - ShardLoom has full evidence and result-sink proof paths for scoped local workflows.
    - Benchmark rows separate execution modes and stage timing, but evidence-cost intent is not yet
      a first-class `evidence_level` contract.
    - Non-evidence/evidence-light runtime paths are not formalized; any future runtime-light path
      could be misread as a hidden fast mode or claim-grade benchmark row.
  - Next slice outcome:
    - Add explicit evidence levels:
      - `minimal_runtime`
      - `certified`
      - `full_replay`
    - Preserve no-fallback and no-external-engine evidence in every level.
    - Add deterministic claim-gate rules so `minimal_runtime` cannot become claim-grade by
      accident.
  - User-visible surface:
    - CLI benchmark rows.
    - Python capability view.
    - website benchmark explanation.
    - typed execution envelope and future REST/API protocol parity surfaces.
  - Implementation scope:
    - execution envelope schema.
    - benchmark row schema.
    - benchmark harness contract tests.
    - Python typed accessors/capability view if the field is exposed there.
    - website benchmark generator/explanation.
    - docs and readiness metadata.
  - Evidence required:
    - `execution_mode`.
    - `evidence_level`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `source_state_digest` if available.
    - `output_digest` if available.
    - `claim_gate_status`.
    - result-sink replay refs for `full_replay`.
    - certificate refs for `certified` and `full_replay`.
  - Acceptance:
    - `minimal_runtime` omits heavy result-sink replay unless requested.
    - `certified` emits normal certificates for the selected execution mode.
    - `full_replay` emits result-sink replay proof.
    - `minimal_runtime` rows cannot become claim-grade by accident.
    - Every row still exposes no-fallback and no-external-engine fields.
    - Evidence level is reported separately from execution mode and engine mode.
  - Verification:
    - contract tests for all three evidence levels.
    - benchmark smoke for `minimal_runtime` versus `full_replay`.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no hidden fast mode.
    - no fallback engine.
    - no performance/superiority claim.
    - no Spark replacement claim.
    - no production claim.
    - no SQL/DataFrame, object-store/lakehouse, or Foundry runtime expansion.
  - Claim boundary:
    - `evidence_level=minimal_runtime` means `claim_gate_status=not_claim_grade` unless a later
      explicit workload-scoped gate approves otherwise.
    - Evidence-level tiering explains proof overhead and runtime-development modes; it does not
      authorize performance, superiority, Spark-displacement, production, SQL/DataFrame,
      object-store/lakehouse, Foundry, or package claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      evidence level.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with tests, benchmark smoke
      artifacts, and website/readiness evidence refs.
  - Dependencies/blockers:
    - stable execution envelope schema, benchmark row schema, result-sink replay evidence, Native
      I/O certificate refs, Python capability-view surfacing, website benchmark generator, and
      claim-gate policy.

- [ ] GAR-PERF-2B evidence-aware logical optimizer
  - Source:
    - Polars lazy optimizer feature class:
      `https://docs.pola.rs/user-guide/concepts/lazy-api/`,
      `https://docs.pola.rs/user-guide/lazy/query-plan/`,
      `https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.explain.html`,
      and `https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.QueryOptFlags.html`.
    - RFC 0016 optimizer/adaptive execution.
    - RFC 0022 logical/physical Plan IR.
    - ShardLoom logical/physical plan docs and explain/estimate posture.
    - `docs/architecture/evidence-aware-logical-optimizer.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
  - Current state:
    - ShardLoom has execution modes, capability posture, Plan IR surfaces, explain/estimate
      diagnostics, and report-only adaptive optimizer/memory planning.
    - Scoped prepared/native benchmark rows already expose selected source-backed scan and
      source-state reuse evidence, but no general optimizer rule registry or rewrite trace exists.
    - Full lazy optimizer parity is not implemented and must not be claimed.
  - Next slice outcome:
    - Add an optimizer rule registry and report-only optimizer trace.
    - Make CLI/Python explain and benchmark rows capable of reporting which optimizer rules were
      admitted, applied, blocked, unsupported, not applicable, or report-only.
    - Preserve before/after plan digests, no-fallback fields, materialization boundaries, and claim
      gates for every optimizer decision.
  - User-visible surface:
    - CLI plan explain.
    - Python explain / typed capability view if surfaced.
    - benchmark rows with optimizer trace refs.
    - compute-flow and benchmark docs.
  - Initial rules:
    - predicate pushdown.
    - projection pushdown.
    - slice/limit pushdown.
    - common subplan/source-state reuse.
    - expression simplification.
    - constant folding.
    - type coercion.
    - join ordering.
    - cardinality estimation.
  - Implementation scope:
    - optimizer rule structs.
    - optimizer registry versioning.
    - plan rewrite trace.
    - before/after logical plan digest.
    - explain output.
    - benchmark row trace refs.
    - snapshot tests and correctness smoke for any applied rewrite.
  - Evidence required:
    - `optimizer_trace_id`.
    - `optimizer_registry_version`.
    - `optimizer_phase`.
    - `optimizer_rule_id`.
    - `optimizer_rule_family`.
    - `optimizer_rule_status`.
    - `optimizer_rule_admitted`.
    - `optimizer_rule_applied`.
    - `optimizer_rule_blocked_reason`.
    - `before_plan_digest`.
    - `after_plan_digest`.
    - `rewrite_safety_status`.
    - `evidence_preserved=true`.
    - `no_fallback_preserved=true`.
    - `claim_boundary_preserved=true`.
    - `materialization_boundary_preserved`.
    - `source_state_reuse_admitted`.
    - `estimated_input_cardinality`.
    - `estimated_output_cardinality`.
    - `cardinality_estimation_status`.
    - `correctness_smoke_ref`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Optimizer can explain which rules were admitted, applied, blocked, unsupported, not
      applicable, or report-only.
    - Every applied rewrite records before/after plan digests.
    - Rewrites preserve no-fallback, materialization/decode, evidence, and claim boundaries.
    - Unsupported rules produce deterministic blockers rather than invoking fallback execution.
    - Polars remains a design reference only and is never invoked as optimizer or runtime.
  - Verification:
    - plan snapshot tests.
    - before/after digest stability tests.
    - correctness smoke before/after rewrite for each applied runtime rewrite.
    - no-fallback tests for explain/optimizer paths.
    - benchmark row contract tests if optimizer trace refs are emitted.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics python/src python/tests`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no broad SQL runtime claim.
    - no broad DataFrame runtime claim.
    - no lazy optimizer parity claim.
    - no hidden fallback.
    - no external optimizer dependency.
    - no performance/superiority claim.
    - no object-store/lakehouse or Foundry runtime.
  - Claim boundary:
    - Optimizer trace evidence may claim only scoped rewrite classification or scoped rewrite
      application where before/after digests, correctness, no-fallback, materialization, and claim
      gates exist.
    - It does not authorize broad SQL/DataFrame runtime, Polars/DataFusion parity, performance,
      superiority, Spark-displacement, production, object-store/lakehouse, Foundry, package, or
      release claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every admitted,
      applied, blocked, unsupported, not-applicable, or report-only optimizer rule row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with plan snapshots, correctness
      smoke refs, benchmark trace refs where applicable, deterministic blockers, and no-fallback
      evidence.
  - Dependencies/blockers:
    - stable Plan IR digesting, explain output envelope, optimizer registry schema,
      materialization/decode evidence, semantic-profile safety, correctness smoke fixtures,
      benchmark row schema, and no-fallback tests.

- [ ] GAR-PERF-2C Vortex Scan API pushdown completion
  - Source:
    - Vortex Scan API concepts and repo Vortex Scan API skill.
    - current prepared/native `source_backed_scan_*` evidence.
    - `docs/architecture/vortex-scan-pushdown-completion.md`.
    - `docs/architecture/vortex-runtime-utilization-audit.md`.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
    - `benchmarks/traditional_analytics/run.py`.
  - Current state:
    - Source-backed scan evidence exists for scoped local prepared/native rows.
    - Several scenario families already report projected scan boundaries and avoid full fact-table
      materialization.
    - Pushdown is not complete or uniformly reported across every prepared/native scenario family.
    - Existing pushdown evidence must not be confused with an encoded-native operator claim.
  - Next slice outcome:
    - Ensure every prepared/native scenario either maps filter, projection, and limit/slice intent
      into Vortex Scan/source-backed scan evidence or emits a deterministic blocker.
    - Distinguish filter-only columns from output columns.
    - Keep unsupported expressions blocked rather than delegated to fallback execution.
  - User-visible surface:
    - benchmark rows.
    - compute-flow docs.
    - capability matrix / `compute-capability-matrix` posture.
    - website benchmark and compute-flow interpretation.
  - Implementation scope:
    - scan request builder.
    - filter expression lowering.
    - projection mask computation.
    - limit/slice pushdown.
    - benchmark row schema and Markdown renderer.
    - capability matrix rows and source-backed scan tests.
  - Evidence required:
    - `scan_filter_pushed_down`.
    - `scan_projection_pushed_down`.
    - `scan_limit_pushed_down`.
    - `filter_columns_read`.
    - `output_columns_read`.
    - `data_materialized`.
    - `data_decoded`.
    - deterministic unsupported/blocker reason where pushdown is unavailable.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Every prepared/native scenario family has pushdown evidence or a deterministic blocker.
    - Filter/project/limit scenarios avoid reading unused columns when evidence supports it.
    - Filter-only columns do not appear in the output stream unless requested.
    - Unsupported expressions are blocked, not executed through fallback.
    - Capability matrix rows distinguish scan pushdown evidence from encoded-native operator
      admission.
  - Verification:
    - selective filter smoke.
    - filter/projection/limit smoke.
    - source-backed scan tests.
    - benchmark row contract test if row fields change.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no encoded-native operator claim.
    - no broad SQL/DataFrame runtime.
    - no object-store/lakehouse runtime.
    - no production or performance/superiority claim.
    - no external engine fallback.
  - Claim boundary:
    - Scan pushdown evidence may claim scoped local Vortex scan/source-backed pushdown only.
    - Pushdown evidence does not imply encoded-native operator execution, generalized Source/Split
      runtime, production SQL/DataFrame support, object-store/lakehouse support, or public
      performance claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      pushdown-supported, blocked, or unsupported row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with smoke artifact refs,
      source-backed scan test refs, capability matrix evidence, and deterministic blocker examples.
  - Dependencies/blockers:
    - Vortex-first provider check, stable source-backed scan row schema, filter expression lowering,
      projection mask model, limit/slice semantics, materialization/decode evidence, and
      no-fallback diagnostics.

- [ ] GAR-PERF-2D compressed/encoded kernel registry
  - Source:
    - Vortex array encodings and layout model.
    - encoded predicate provider work.
    - current selective-filter encoded predicate evidence.
    - `docs/architecture/compressed-encoded-kernel-registry.md`.
    - `docs/architecture/vortex-runtime-utilization-audit.md`.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
    - `benchmarks/traditional_analytics/run.py`.
  - Current state:
    - Selective-filter encoded-predicate evidence exists for scoped local paths.
    - Current evidence can report admitted filter-column encodings and selection-vector bridge
      status, while keeping metric aggregation residual-native.
    - Encoded-native operator coverage is not broad, and current registry-like history does not
      provide a uniform benchmark/capability contract for encoding-specific operators.
  - Next slice outcome:
    - Add a compressed/encoded kernel registry for encoding-specific operator support.
    - Classify each initial encoding/operator pair as admitted, executed, blocked, unsupported, or
      not available with deterministic evidence.
    - Keep `encoded_native_claim_allowed=false` unless end-to-end evidence passes.
  - User-visible surface:
    - benchmark evidence.
    - capability matrix / `compute-capability-matrix` posture.
    - compute-flow docs.
    - website benchmark and compute-flow interpretation after artifact refresh.
  - Initial encoding/operator pairs:
    - bitpacked boolean/integer filter.
    - sequence equality/range predicate.
    - dictionary equality/group-by.
    - constant array count/filter.
    - sorted min/max range pruning.
    - FSST/dictionary string equality if available.
  - Implementation scope:
    - encoded/compressed kernel registry structs or report rows.
    - kernel admission and deterministic blockers.
    - encoding/operator capability matrix rows.
    - benchmark row schema and Markdown renderer if fields change.
    - unit tests per encoding/operator pair.
  - Evidence required:
    - `encoding_id`.
    - `logical_dtype`.
    - `physical_encoding`.
    - `operator_family`.
    - `kernel_admitted`.
    - `kernel_executed`.
    - `canonicalization_required`.
    - `decoded`.
    - `materialized`.
    - `selection_vector_emitted`.
    - `validity_semantics`.
    - deterministic unsupported/blocker reason where the kernel is unavailable.
    - `encoded_native_claim_allowed`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Unsupported encodings block deterministically.
    - Encoded-native claim remains false until end-to-end evidence passes.
    - Initial encoding/operator pairs are visible in benchmark/capability evidence as admitted,
      executed, blocked, unsupported, or not available.
    - Rows distinguish canonicalization, decode, materialization, and selection-vector behavior.
    - Registry admission does not silently promote residual-native paths to encoded-native support.
  - Verification:
    - unit tests per encoding/operator pair.
    - null, empty, all-null, and high-cardinality cases where relevant.
    - decoded-reference correctness comparison.
    - benchmark smoke for selective filter and group-by.
    - traditional benchmark row contract tests if row fields change.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no broad SQL/DataFrame runtime claim.
    - no broad encoded-native operator coverage claim.
    - no object-store/lakehouse runtime.
    - no production or performance/superiority claim.
    - no external engine fallback.
  - Claim boundary:
    - Kernel-registry rows may claim only scoped encoding/operator admission or execution where the
      row carries correctness, materialization/decode, no-fallback, and claim-gate evidence.
    - Registry admission does not imply broad encoded-native operator coverage, SQL/DataFrame
      runtime, object-store/lakehouse runtime, production readiness, or public performance claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every admitted,
      executed, blocked, unsupported, or not-available row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with unit test refs, selective filter
      and group-by benchmark smoke artifacts, capability matrix evidence, and deterministic blocker
      examples.
  - Dependencies/blockers:
    - Vortex-first provider check, stable encoding identifiers, validity/null semantics,
      selection-vector evidence, decoded-reference correctness fixtures, materialization/decode
      policy, and no-fallback diagnostics.

- [ ] GAR-PERF-2E fused operator pipeline
  - Source:
    - benchmark scenario catalog.
    - prepared/native batch runner.
    - current residual-native prepared/native operator paths.
    - `docs/architecture/fused-operator-pipeline.md`.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
    - `benchmarks/traditional_analytics/run.py`.
  - Current state:
    - Operators are increasingly residual-native, and several prepared/native rows avoid full
      fact-table materialization through projected local Vortex scans and ShardLoom-native state.
    - The benchmark harness has narrow fusion vocabulary such as `filter_project_limit_fused`.
    - Fusion is not yet a stable cross-family pipeline contract with correctness digest parity,
      row-count evidence, and uniform benchmark fields.
  - Next slice outcome:
    - Implement or deterministically block fused local prepared/native pipelines for:
      - filter + projection + limit.
      - filter + aggregate.
      - filter + group-by.
      - top-k with projection.
    - Keep the fused path local, prepared/native, no-fallback, and evidence-bearing.
  - User-visible surface:
    - benchmark rows.
    - timing attribution.
    - work-avoidance evidence.
    - compute-flow and benchmark docs.
    - website benchmark interpretation after artifact refresh.
  - Implementation scope:
    - prepared/native scenario execution paths.
    - fused pipeline admission and deterministic blockers.
    - fused/unfused correctness digest comparison.
    - benchmark row schema and Markdown renderer.
    - traditional analytics smoke commands and contract tests.
  - Evidence required:
    - `fused_pipeline_used`.
    - `fused_operator_family`.
    - `intermediate_materialization_avoided`.
    - `rows_scanned`.
    - `rows_selected`.
    - `rows_output`.
    - `unfused_correctness_digest`.
    - `fused_correctness_digest`.
    - `correctness_digest_match`.
    - `data_materialized`.
    - `data_decoded`.
    - deterministic unsupported/blocker reason where fusion is unavailable.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - No intermediate full-table materialization occurs when fusion applies.
    - Pipeline output has an identical correctness digest to the unfused ShardLoom-native path.
    - Every planned pipeline family is implemented with evidence or blocked with a deterministic
      reason.
    - Benchmark rows distinguish fused residual-native pipelines from encoded-native operator
      execution.
    - Unsupported or unsafe fusion paths are blocked, not delegated to another engine.
  - Verification:
    - differential correctness tests for fused versus unfused paths.
    - benchmark smoke before and after fusion.
    - traditional benchmark row contract tests if row fields change.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no broad SQL/DataFrame runtime claim.
    - no encoded-native operator claim unless later end-to-end representation evidence proves it.
    - no object-store/lakehouse runtime.
    - no production or performance/superiority claim.
    - no external engine fallback.
  - Claim boundary:
    - Fused pipeline rows may claim only scoped local prepared/native residual-native fusion where
      correctness digest parity and materialization evidence exist.
    - Fusion evidence does not imply broad operator coverage, encoded-native execution, SQL/DataFrame
      runtime, object-store/lakehouse runtime, production readiness, or public performance claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every fused,
      blocked, or unsupported row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with differential correctness tests,
      benchmark smoke artifacts, row-schema evidence, and deterministic blocker examples.
  - Dependencies/blockers:
    - stable prepared/native scenario row schema, source-backed scan fields, row-count evidence,
      unfused ShardLoom-native reference path, materialization/decode evidence, and no-fallback
      diagnostics.

- [ ] GAR-PERF-2F in-process ShardLoom session runtime
  - Source:
    - prepared/native batch runner.
    - source-state reuse work.
    - `ShardLoomSessionModelReport` in `shardloom-core/src/session.rs`.
    - `docs/architecture/in-process-session-runtime.md`.
    - `docs/architecture/benchmark-persistent-runner-decision.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
  - Current state:
    - `traditional-analytics-vortex-batch-run` exists for scoped traditional analytics and can reuse
      prepared local Vortex artifacts, source metadata snapshots, and selected source-state families
      inside one process.
    - `ShardLoomSessionModelReport` exists as a report-only explicit session/registry posture with
      runtime execution disabled and hidden globals disallowed.
    - A general reusable `ShardLoomSession` runtime is not formalized or exposed to Python.
  - Next slice outcome:
    - Add a `ShardLoomSession` design and, if safe, a scoped implementation for prepared/native
      local artifacts.
    - Keep the scope in-process, caller-owned, explicit-close, and local-artifact-only.
    - Preserve typed envelopes and per-run evidence for each scenario executed through the session.
  - User-visible surface:
    - CLI batch command.
    - Python client typed session/capability view if surfaced.
    - benchmark rows with session/cache evidence.
    - compute-flow and benchmark docs.
  - Session state:
    - prepared artifact registry.
    - source metadata cache.
    - source-state cache.
    - schema cache.
    - dictionary cache.
    - buffer pool.
    - kernel registry.
    - evidence recorder.
  - Implementation scope:
    - core session contract and lifecycle state if runtime implementation is safe.
    - prepared/native local artifact registry and reuse counters.
    - benchmark batch command session fields.
    - Python client typed view only after CLI/evidence fields stabilize.
    - docs and contract tests.
  - Evidence required:
    - `session_id`.
    - cache hit/miss fields for prepared artifacts, source metadata, source-state, schema,
      dictionary, and buffers where applicable.
    - `source_state_reuse_count`.
    - `prepared_artifact_reuse_count`.
    - session close/drop status.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Multiple scoped prepared/native scenario executions do not respawn the CLI or re-open /
      reprepare unnecessary state when a session owns the reusable state.
    - Session state remains scoped, caller-owned, and explicitly closed.
    - Session rows preserve execution mode, evidence level, Native I/O refs, materialization/decode
      boundaries, result-sink evidence when requested, and no-fallback fields.
    - Unsupported or incompatible state reuse emits deterministic diagnostics rather than falling
      back or silently recomputing.
  - Verification:
    - batch smoke.
    - Python client smoke if surfaced.
    - benchmark row contract tests for session/cache fields.
    - session close/drop lifecycle test if runtime implementation lands.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics python/src python/tests`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no daemon/service.
    - no remote server claim.
    - no REST listener.
    - no hidden global cache.
    - no object-store/lakehouse runtime.
    - no SQL/DataFrame runtime expansion.
    - no performance/superiority claim.
  - Claim boundary:
    - `ShardLoomSession` evidence can show local in-process reuse and reduced redundant setup only.
      It does not authorize performance, superiority, Spark-displacement, production,
      SQL/DataFrame, object-store/lakehouse, Foundry, REST, package, or remote-runtime claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      session-backed run.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with batch smoke, Python smoke if
      applicable, lifecycle tests, and benchmark session-field evidence refs.
  - Dependencies/blockers:
    - stable typed envelope, prepared/native batch command, source-state reuse counters, explicit
      session lifecycle, cache invalidation/digest policy, Python typed result model, and
      no-fallback policy tests.

- [ ] GAR-PERF-2G allocation and buffer-pool optimization
  - Source:
    - resource metrics.
    - prepared/native runtime work.
    - `docs/architecture/allocation-buffer-pool-optimization.md`.
    - `docs/architecture/in-process-session-runtime.md`.
    - `docs/architecture/performance-attribution-and-execution-structure.md`.
    - `benchmarks/traditional_analytics/README.md`.
  - Current state:
    - No global allocation or buffer-pool optimization pass is claimable.
    - Prepared/native benchmark rows expose stage timing and selected resource evidence, but they
      do not yet expose a uniform allocation profile or scoped buffer-reuse contract.
    - `GAR-PERF-2F` plans a caller-owned `ShardLoomSession` that may later own a buffer pool, but
      buffer ownership, reuse families, release behavior, and memory/resource report fields remain
      planned.
  - Next slice outcome:
    - Add allocation profiling and scoped buffer reuse planning for prepared/native local runtime
      paths.
    - Classify result buffers, temporary vectors, hash tables, dictionary/string state, and
      source-state arrays as measurable, not measurable, reusable, blocked, unsupported, or not
      needed.
    - Add deterministic blockers where allocation counting or safe reuse cannot yet be supported.
  - User-visible surface:
    - benchmark resource/evidence rows.
    - memory/resource report.
    - CLI batch/session evidence if surfaced.
    - Python typed capability view if surfaced after CLI fields stabilize.
    - compute-flow, benchmark, and website benchmark interpretation after artifact refresh.
  - Implementation scope:
    - result buffers.
    - temporary vectors.
    - hash tables.
    - dictionary/string state.
    - source-state arrays.
    - benchmark row schema and Markdown renderer if fields change.
    - memory/resource report schema.
    - session/batch lifecycle evidence if runtime implementation is safe.
  - Evidence required:
    - `allocation_profile_status`.
    - `allocation_profile_scope`.
    - `allocation_count` if measurable.
    - `allocation_bytes` if measurable.
    - `buffer_pool_enabled`.
    - `buffer_pool_scope`.
    - `buffer_reuse_count`.
    - `buffer_reuse_family`.
    - `peak_rss_delta` if measurable.
    - `source_state_digest` if available.
    - `output_digest` if available.
    - `correctness_digest`.
    - `evidence_regression_status`.
    - `unsafe_lifetime_shortcut_used=false`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Buffer reuse is opt-in or scoped to an explicit run/session.
    - No correctness digest regression versus the non-reuse path.
    - No certificate, materialization/decode, output digest, or no-fallback evidence regression.
    - No unsafe lifetime shortcuts are used to avoid allocations.
    - Non-measurable allocation counts or peak RSS fields are reported as `not_available`, not
      silently treated as zero.
    - Unsupported or incompatible reuse emits deterministic blockers rather than fallback
      execution or hidden recomputation.
  - Verification:
    - focused unit tests for ownership, reset, reuse, and release behavior.
    - differential correctness tests against a no-reuse path.
    - benchmark smoke for allocation/resource fields.
    - memory/resource report.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics python/src python/tests`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no global allocator replacement.
    - no hidden process-wide buffer pool.
    - no daemon/service or remote runtime.
    - no unsafe lifetime shortcuts.
    - no spill implementation in this slice.
    - no SQL/DataFrame runtime expansion.
    - no object-store/lakehouse or Foundry runtime.
    - no performance/superiority claim.
  - Claim boundary:
    - Allocation/buffer-pool evidence may claim only scoped local resource-profile visibility and
      explicitly reported buffer reuse for a specific prepared/native run or session.
    - It does not authorize performance, memory-efficiency, superiority, Spark-displacement,
      production, SQL/DataFrame, object-store/lakehouse, Foundry, REST, package, or remote-runtime
      claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      allocation-profiled, buffer-reuse, blocked, unsupported, or not-measurable row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with unit tests, differential
      correctness refs, benchmark smoke artifacts, memory/resource report refs, and deterministic
      blocker examples.
  - Dependencies/blockers:
    - stable session/batch lifecycle, resource metric collection, benchmark row schema, source-state
      digests, output/correctness digest parity, memory report schema, platform-specific peak RSS
      measurement policy, and no-fallback tests.

- [ ] GAR-PERF-2H optimized build profiles and PGO benchmark lane
  - Source:
    - Cargo release/profile docs:
      `https://doc.rust-lang.org/cargo/reference/profiles.html`.
    - rustc PGO docs:
      `https://doc.rust-lang.org/rustc/profile-guided-optimization.html`.
    - rustc codegen options:
      `https://doc.rust-lang.org/rustc/codegen-options/index.html`.
    - `Cargo.toml`.
    - `benchmarks/traditional_analytics/run.py`.
    - `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`.
    - `docs/release/hard-release-readiness-gate.md`.
  - Current state:
    - The workspace uses the normal Cargo release profile for optimized local builds.
    - No formal `release-lto`, `release-pgo`, or `release-native-benchmark` profile is established.
    - The benchmark harness already records `shardloom_build_profile`, but it does not yet record a
      complete build-profile evidence contract for LTO, PGO, target CPU, reproducibility, or release
      portability.
  - Next slice outcome:
    - Add performance build-profile planning and, when safe, implement explicit build lanes:
      - `release-lto`.
      - `release-pgo`.
      - `release-native-benchmark`.
    - Document and script a reproducible PGO flow from instrumented build, training workload,
      `llvm-profdata` merge, and profile-use rebuild.
    - Keep portable release artifacts separate from host-native benchmark artifacts.
  - User-visible surface:
    - benchmark docs.
    - release docs.
    - benchmark fairness parameters and JSON/Markdown artifacts.
    - future release-readiness profile checks.
  - Implementation scope:
    - `Cargo.toml` custom profiles where manifest settings are appropriate.
    - benchmark scripts and harness row schema.
    - release/readiness docs and optional release gate checks.
    - PGO helper script or documented command sequence.
  - Evidence required:
    - `build_profile`.
    - `build_profile_kind`.
    - `rustc_version`.
    - `cargo_version`.
    - `target_triple`.
    - `target_cpu_policy`.
    - `target_cpu_native_enabled`.
    - `lto_enabled`.
    - `lto_mode`.
    - `codegen_units`.
    - `pgo_status`.
    - `pgo_profile_generate_status`.
    - `pgo_profile_use_status`.
    - `pgo_profile_artifact_ref`.
    - `pgo_training_workload_ref`.
    - `pgo_training_workload_digest`.
    - `build_reproducibility_status`.
    - `portable_release_artifact`.
    - `benchmark_only_build`.
    - `correctness_digest`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Portable release artifacts remain portable and do not use `target-cpu=native`.
    - `target-cpu=native` is benchmark-only and clearly labeled as host-local.
    - PGO lane is documented and reproducible from checked-in commands or scripts.
    - Benchmark harness records build profile and LTO/PGO/native status.
    - `cargo build --profile release-lto` succeeds after profile implementation.
    - Claims remain blocked until a claim-grade benchmark gate passes.
  - Verification:
    - build `release-lto`.
    - optional PGO smoke with `profile-generate`, benchmark training run, `llvm-profdata merge`, and
      `profile-use` rebuild.
    - benchmark harness row-contract test for build-profile fields.
    - release-readiness test that portable artifacts do not use `target-cpu=native`.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - `python -m compileall -q benchmarks/traditional_analytics scripts`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no replacement of the default release build.
    - no hidden `RUSTFLAGS` in release workflows.
    - no `target-cpu=native` for portable release artifacts.
    - no package publication or release tag.
    - no performance/superiority claim.
  - Claim boundary:
    - Build-profile evidence may say which local binary/profile produced a benchmark row and which
      compiler settings were recorded.
    - It does not authorize performance, superiority, Spark-displacement, production,
      SQL/DataFrame, object-store/lakehouse, Foundry, package, or public release claims.
  - Fallback boundary:
    - Build profiles and PGO lanes cannot add or invoke Spark, DataFusion, DuckDB, Polars, Velox,
      pandas, or any other external engine as runtime fallback.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with profile build logs, optional PGO
      smoke artifacts, benchmark row-contract refs, release-readiness refs, and claim-boundary
      evidence.
  - Dependencies/blockers:
    - Cargo profile design, rustc/rustup toolchain policy, `llvm-tools-preview`/`llvm-profdata`
      availability for PGO, benchmark training workload selection, build-profile row schema, and
      release portability gate.

- [ ] GAR-PERF-2I native microbenchmark suite for kernel-level competition
  - Source:
    - benchmark report rows where native microbenchmarks were skipped in older artifacts.
    - `benchmarks/traditional_analytics/run.py`.
    - `benchmarks/traditional_analytics/README.md`.
    - `docs/architecture/benchmark-suite-catalog.md`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
  - Current state:
    - Traditional benchmark and prepared/native batch benchmark paths exist.
    - The harness already has a ShardLoom native microbenchmark lane, but it is optional/skippable
      through `--skip-shardloom-native`.
    - Current native microbenchmark coverage is centered on local encoded count, Vortex-run count,
      projection, validity/comparison count predicates, filter-project, and local commit smoke.
    - Older public/comparative artifacts may show native microbenchmark rows as skipped, so the
      website and docs need a first-class claim-safe microbenchmark interpretation before the lane
      is used for optimization planning.
  - Next slice outcome:
    - Add or explicitly block native microbenchmark rows for:
      - Vortex scan only.
      - filter predicate only.
      - projection only.
      - group-by kernel.
      - hash join kernel.
      - top-k.
      - result-sink write.
      - evidence render.
    - Keep rows separate from traditional end-to-end benchmark rows and compatibility-import rows.
    - Emit deterministic skipped/unsupported rows when a primitive is not implemented.
  - User-visible surface:
    - benchmark JSON artifacts.
    - benchmark Markdown reports.
    - website benchmark page.
    - benchmark docs and compute-flow benchmark interpretation.
  - Implementation scope:
    - `benchmarks/traditional_analytics/run.py`.
    - `benchmarks/traditional_analytics/README.md`.
    - benchmark artifact schema and Markdown renderer.
    - website benchmark generator/readiness checks if rendered output changes.
    - focused CLI primitives or deterministic blockers only where already available.
  - Evidence required:
    - `benchmark_category=native_microbenchmark`.
    - primitive name.
    - input rows.
    - rows scanned/selected/materialized where available.
    - decoded/materialized status.
    - timing scope.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
    - unsupported/skipped reason when the primitive is unavailable.
  - Acceptance:
    - Microbenchmarks are clearly labeled as subsystem/kernel evidence, not end-to-end product
      claims.
    - Each row identifies the subsystem under test and the optimization question it answers.
    - Skipped/unsupported rows are deterministic and visible.
    - Website benchmark copy does not treat native microbenchmark rows as public rankings.
    - Missing group-by, hash-join, top-k, result-sink write, or evidence-render microbenchmarks do
      not block traditional benchmark execution.
  - Verification:
    - native microbenchmark smoke with at least one implemented primitive.
    - benchmark micro smoke that exercises skipped/unsupported primitive rows.
    - `python -m compileall -q benchmarks/traditional_analytics`
    - `python scripts/check_website_readiness.py`
    - `git diff --check`
  - Non-goals:
    - no end-to-end performance/superiority claim.
    - no Spark replacement claim.
    - no production claim.
    - no broad SQL/DataFrame benchmark claim.
    - no object-store/lakehouse/Foundry benchmark claim.
    - no external engine fallback.
  - Claim boundary:
    - Native microbenchmark rows identify subsystem optimization opportunities only.
    - They cannot make `ShardLoom is faster`, best-default, Spark-displacement, production,
      SQL/DataFrame, object-store/lakehouse, Foundry, or package claims.
    - `claim_gate_status` remains `not_claim_grade` unless a later workload-scoped benchmark gate
      explicitly promotes a row with correctness, Native I/O, materialization/decode, policy,
      reproducibility, and no-fallback evidence.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      ShardLoom native microbenchmark row, including skipped/unsupported rows.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with benchmark smoke artifact refs,
      website/readiness evidence, and skipped/unsupported row examples.
  - Dependencies/blockers:
    - stable native primitive commands, benchmark row schema, work-avoidance evidence fields,
      website benchmark generator, and deterministic unsupported-row renderer.

#### GAR-IOREUSE-1 - I/O Reuse And Cross-Format Fanout

These slices expand the prepared/native runtime roadmap from scenario-local source-state reuse into
decoupled source, preparation, execution, output, and sink evidence. Input and output formats must
remain independent. The stable path is:

```text
InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact
```

The benchmark bundle vocabulary is `io_reuse_and_fanout`, `source_state_reuse`,
`prepared_state_reuse`, `output_plan_reuse`, `cross_format_output`, and
`generated_source_output`. Required future timing/reuse fields include `source_discovery_millis`,
`schema_inference_millis`, `source_parse_millis`, `vortex_prepare_millis`,
`operator_compute_millis`, `output_plan_millis`, `output_write_millis`,
`output_replay_millis`, `total_runtime_millis`, `source_state_reuse_hit`,
`prepared_state_reuse_hit`, `output_plan_reuse_hit`, `fanout_output_count`,
`fallback_attempted=false`, `external_engine_invoked=false`, and `claim_gate_status`.

- [ ] GAR-IOREUSE-1A universal SourceState abstraction
  - Source:
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/architecture/benchmark-suite-catalog.md`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - RFC 0031 universal Native I/O envelope.
    - RFC 0033 user workflow and ETL surface.
    - RFC 0040 benchmark suite hardening.
    - RFC 0042 Vortex runtime utilization.
    - compatibility import lanes.
    - prepared/native batch source-state reuse work.
    - global non-Vortex parity plan.
    - Existing scoped source metadata/source-state reuse in the prepared/native batch runner.
  - Current state:
    - Prepared/native batch rows reuse selected per-batch source metadata and scenario-family
      source-state for dimension labels, category/metric state, group/category/metric state,
      ranked-metric state, selective-filter state, and dirty-input cleanup state.
    - No universal `SourceState` abstraction covers source discovery, schema/dtype inference,
      parse/decode planning, source fingerprints, adapter capability, and deterministic blockers
      across local CSV/JSONL/Parquet/Arrow IPC/Avro/ORC, Vortex, generated sources, or future
      source adapters.
  - Next slice outcome:
    - Define and, if safe, expose a report-only `SourceState` contract that can be reused across
      workloads without depending on the requested output format.
    - Classify every source family as `source_state_reuse_supported`, `not_needed`, `blocked`,
      `unsupported`, or `report_only`.
    - Add deterministic blocker language for adapters whose source discovery, schema inference, or
      parse/decode plan cannot yet be reused.
  - User-visible surface:
    - CLI capability/explain output.
    - Python typed capability view if exposed.
    - benchmark JSON/Markdown rows.
    - website/status and compute-flow docs after generation.
  - Implementation scope:
    - source discovery metadata.
    - schema/dtype metadata.
    - format-specific adapter state.
    - content fingerprinting.
    - source-state digest.
    - benchmark row schema and Markdown renderer if fields are emitted.
    - capability matrix/status projection.
    - focused docs and deterministic unsupported diagnostics.
  - Evidence required:
    - `source_state_id`.
    - `source_state_digest`.
    - `source_format`.
    - `source_location`.
    - `source_fingerprint_kind`.
    - `schema_digest`.
    - `row_count_known`.
    - `file_count`.
    - `byte_size`.
    - `partition_columns`.
    - `compression`.
    - `source_state_reuse_allowed`.
    - `source_discovery_millis`.
    - `schema_inference_millis`.
    - `source_parse_millis`.
    - `parse_decode_plan_digest`.
    - `source_state_reuse_hit`.
    - `source_state_reuse_reason`.
    - materialization/decode boundary refs.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - `SourceState` is defined independently from output target and execution mode.
    - CSV, Parquet, JSONL, Arrow IPC, Avro, and ORC can all report a SourceState posture.
    - Existing source-state families can map into the contract without losing their family-specific
      fields.
    - Unsupported or incompatible source adapters emit deterministic blockers instead of silently
      recomputing or delegating to external engines.
    - SourceState reuse never implies Vortex-native execution by itself.
    - No source Native I/O certificate is claimed where no source dataset was read.
  - Verification:
    - source-state report snapshot tests.
    - benchmark row contract tests if fields are emitted.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no object-store runtime.
    - no table/lakehouse commit.
    - no broad SQL/DataFrame runtime.
    - no performance/superiority claim.
    - no hidden global cache.
    - no external engine fallback.
  - Claim boundary:
    - `SourceState` evidence may claim only scoped source discovery, inference, parse/decode
      planning, and source fingerprint reuse where evidence exists.
    - It does not authorize output support, Vortex preparation support, performance, production,
      Spark-displacement, SQL/DataFrame, object-store/lakehouse, Foundry, package, or release
      claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      source-state-supported, blocked, unsupported, invalidated, or report-only row.
  - Ledger rule:
    - When complete, move the detailed completed session to
      `docs/architecture/phased-execution-completed-ledger.md` with source-state snapshots,
      benchmark row refs if emitted, deterministic blockers, and no-fallback evidence.
  - Dependencies/blockers:
    - stable source adapter report schema, local format capability rows, source fingerprint policy,
      schema/dtype inference metadata, benchmark row schema, Python capability projection, and
      deterministic unsupported diagnostics.

- [ ] GAR-IOREUSE-1B decoupled VortexPreparedState reuse
  - Source:
    - GAR-IOREUSE-1A.
    - `prepared_vortex` execution mode.
    - prepared/native batch source-state reuse PRs.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/architecture/compute-engine-flow-reference.md`.
    - `docs/architecture/vortex-runtime-utilization-audit.md`.
    - RFC 0031, RFC 0040, and RFC 0042.
  - Current state:
    - Prepared/native batch runner rows reuse caller-supplied Vortex artifacts and selected
      source-state, but prepared artifact reuse is not yet represented as a general
      `VortexPreparedState` layer decoupled from the input and output formats.
    - Compatibility-import-certified rows include import/write/reopen/scan costs and must not be
      interpreted as pure query speed or as reusable prepared-state support unless explicit
      evidence exists.
  - Next slice outcome:
    - Define `VortexPreparedState` as the stable bridge between any supported input and any
      supported output.
    - Make the prepared state reusable across multiple queries and output plans when evidence
      permits.
    - Keep `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, and
      `direct_compatibility_transient` lanes distinct.
  - User-visible surface:
    - benchmark rows.
    - CLI explain/capability output.
    - Python typed result/capability view if exposed.
    - compute-flow and benchmark docs.
  - Implementation scope:
    - prepared artifact registry/report fields.
    - Native I/O certificate refs.
    - materialization/decode refs.
    - source-backed scan refs.
    - benchmark row schema and Markdown renderer if fields are emitted.
    - deterministic blockers for unsupported preparation reuse.
  - Evidence required:
    - `prepared_state_id`.
    - `prepared_state_digest`.
    - `source_state_id`.
    - `vortex_artifact_ref`.
    - `vortex_artifact_digest`.
    - `layout_summary`.
    - `encoding_summary`.
    - `statistics_summary`.
    - `prepared_state_reuse_hit`.
    - `prepared_state_reuse_reason`.
    - `preparation_included_in_timing`.
    - `vortex_prepare_millis`.
    - source Native I/O refs where a source was read.
    - materialization/decode refs.
    - `execution_mode`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Prepared Vortex state is reusable without coupling to a specific output target.
    - Prepared state can be reused across multiple queries and outputs.
    - Rows distinguish preparation cost from operator compute and output write/replay cost.
    - Benchmark rows separate `vortex_prepare_millis` from query/runtime timing.
    - Direct transient rows cannot report prepared-state reuse.
    - Prepared state reuse does not weaken no-fallback policy.
    - Unsupported preparation reuse reports deterministic blockers.
  - Verification:
    - prepared-state reuse snapshot/contract tests.
    - focused prepared/native benchmark smoke if runtime fields are emitted.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no new preparation runtime in the planning slice.
    - no hidden fast mode.
    - no object-store Vortex artifact runtime unless separately admitted.
    - no object-store/lakehouse runtime.
    - no table commit.
    - no performance or superiority claim.
    - no external engine fallback.
  - Claim boundary:
    - `VortexPreparedState` evidence may claim scoped reusable Vortex preparation only when
      prepared artifact refs, digests, Native I/O refs, materialization/decode refs, and
      no-fallback evidence exist.
    - It does not authorize encoded-native operator coverage, cross-format output support,
      performance, production, Spark-displacement, SQL/DataFrame, object-store/lakehouse, Foundry,
      package, or release claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      prepared-state-supported, blocked, unsupported, invalidated, or report-only row.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with
      prepared-state evidence refs, benchmark smoke artifacts if emitted, and deterministic blocker
      examples.
  - Dependencies/blockers:
    - SourceState contract, prepared artifact registry/report schema, Vortex artifact refs/digests,
      Native I/O certificate refs, materialization/decode refs, source-backed scan evidence, and
      execution-mode timing attribution.

- [ ] GAR-IOREUSE-1C output-side OutputPlan reuse
  - Source:
    - GAR-IOREUSE-1A and GAR-IOREUSE-1B.
    - result-sink proof.
    - compatibility output writer matrix.
    - global output sink parity plan.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/skills/translation-layer.md`.
    - `docs/skills/vortex/vortex-native-output.md`.
    - RFC 0031, RFC 0033, and RFC 0040.
  - Current state:
    - Vortex output and selected result-sink proof paths exist for scoped local workflows.
    - Output planning is not yet a reusable first-class layer across local Vortex and compatibility
      export targets, and metadata preservation/degradation reports are not uniformly tied to
      reusable output-plan fingerprints.
  - Next slice outcome:
    - Define `OutputPlan` as the output-side planning layer that maps an execution result into one
      or more local sink targets without coupling to input format.
    - Add reuse/invalidation vocabulary for schema mapping, metadata preservation, layout/write
      strategy, materialization requirements, replay policy, and unsupported sink diagnostics.
  - User-visible surface:
    - CLI write/plan/explain output.
    - Python typed result metadata if exposed.
    - Python write APIs after API surface admission.
    - Foundry output examples where report-only/proof docs are updated.
    - benchmark rows.
    - website/status and compute-flow docs.
  - Output formats:
    - Vortex.
    - CSV.
    - JSONL.
    - Parquet.
    - Arrow IPC.
    - Avro.
    - ORC.
    - Foundry output dataset, via transform wrapper.
    - S3/object-store, blocked until runtime proof.
  - Implementation scope:
    - output plan report structs.
    - sink artifact report fields.
    - Vortex-native output and compatibility export metadata-loss reports.
    - result-sink replay evidence refs.
    - benchmark row schema and Markdown renderer if fields are emitted.
  - Evidence required:
    - `output_plan_id`.
    - `output_plan_digest`.
    - `output_format`.
    - `output_location`.
    - `output_schema_digest`.
    - `output_partitioning`.
    - `output_compression`.
    - `output_encoding`.
    - `output_write_mode`.
    - `output_plan_reuse_allowed`.
    - `output_metadata_preservation_status`.
    - `output_materialization_required`.
    - `output_plan_reuse_hit`.
    - `output_write_millis`.
    - `result_replay_verified`.
    - `output_native_io_certificate_status`.
    - `sink_artifact_ref`.
    - `sink_artifact_digest`.
    - output Native I/O certificate refs.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Output planning is independent from input adapter and source format.
    - One prepared source can fan out to multiple output formats.
    - Vortex remains the highest-fidelity output target.
    - Compatibility outputs report preserved, mapped, degraded, or dropped metadata.
    - Local output and object-store/table commit remain separate.
    - Unsupported sink targets emit deterministic diagnostics.
  - Verification:
    - output-plan report snapshot tests.
    - result-sink replay contract tests if fields are emitted.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no object-store write.
    - no lakehouse/table commit.
    - no S3/GCS/ADLS credential resolution.
    - no performance claim.
    - no external writer engine.
    - no package/release claim.
  - Claim boundary:
    - `OutputPlan` evidence may claim scoped local output planning only when target schema,
      metadata preservation/degradation, write, replay, output certificate, and no-fallback evidence
      exist.
    - It does not authorize object-store/lakehouse, table commit, production, performance,
      Spark-displacement, Foundry, SQL/DataFrame, package, or release claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      output-plan-supported, blocked, unsupported, invalidated, or report-only row.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with output-plan
      snapshots, sink artifact refs, replay refs where applicable, and blocker examples.
  - Dependencies/blockers:
    - VortexPreparedState contract, output writer/support matrix, result-sink replay evidence,
      output Native I/O certificate refs, metadata preservation/degradation reporting, local write
      policy, and unsupported sink diagnostics.

- [ ] GAR-IOREUSE-1D cross-format fanout benchmark
  - Source:
    - GAR-IOREUSE-1A through GAR-IOREUSE-1C.
    - global non-Vortex parity benchmark cube.
    - OutputPlan reuse.
    - VortexPreparedState reuse.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `benchmarks/traditional_analytics/README.md`.
    - `benchmarks/traditional_analytics/run.py`.
    - `docs/architecture/benchmark-suite-catalog.md`.
    - `docs/benchmarks/local-taxonomy-benchmark.md`.
    - RFC 0040 benchmark suite hardening.
  - Current state:
    - Traditional benchmark rows separate compatibility import, prepared/native paths, timing
      stages, result-sink proof, and no-fallback evidence.
    - There is no first-class benchmark family that starts from reusable source/prepared state and
      fans out to multiple local output formats while reporting output-plan reuse separately.
  - Next slice outcome:
    - Add benchmark scenario families:
      - `io_reuse_and_fanout`.
      - `source_state_reuse`.
      - `prepared_state_reuse`.
      - `output_plan_reuse`.
      - `cross_format_output`.
      - `generated_source_output`.
    - Add explicit fanout cases:
      - CSV input -> Parquet + JSONL + Vortex outputs.
      - Parquet input -> CSV + Vortex outputs.
      - JSONL input -> Parquet + Vortex outputs.
      - generated source -> CSV + Parquet + Vortex outputs.
      - prepared Vortex -> multiple output formats.
    - Emit deterministic unsupported/skipped rows where a source, preparation, output format, or
      fanout combination is not runtime-supported.
  - User-visible surface:
    - benchmark JSON/Markdown artifacts.
    - website benchmark page.
    - compute-flow benchmark interpretation.
    - local taxonomy benchmark docs.
  - Implementation scope:
    - benchmark scenario catalog.
    - benchmark harness row schema.
    - Markdown/HTML renderer.
    - correctness digest refs per output where runtime rows are added.
    - readiness checks for new rendered fields.
  - Evidence required:
    - `benchmark_family=io_reuse_and_fanout`.
    - `source_discovery_millis`.
    - `schema_inference_millis`.
    - `source_parse_millis`.
    - `vortex_prepare_millis`.
    - `operator_compute_millis`.
    - `output_plan_millis`.
    - `output_write_millis`.
    - `output_replay_millis`.
    - `total_runtime_millis`.
    - `source_state_reuse_hit`.
    - `prepared_state_reuse_hit`.
    - `output_plan_reuse_hit`.
    - `fanout_output_count`.
    - per-output artifact refs/digests.
    - correctness refs when output data is written.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Benchmark rows do not require input and output formats to match.
    - Benchmark demonstrates when source/prepared state is reused across outputs.
    - Raw one-shot speed and reuse/fanout speed are separated.
    - No output sink is marked supported without replay/evidence proof.
    - Compatibility-import, prepared-vortex, native-vortex, and direct-transient lanes remain
      distinct.
    - Cross-format output rows are framed as local workflow/evidence coverage, not speed rankings.
    - Unsupported output formats and fanout combinations remain visible deterministic rows.
  - Verification:
    - focused benchmark smoke for one supported or report-only fanout row.
    - benchmark harness contract test for required metrics.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no benchmark recomputation in the planning slice.
    - no performance/superiority claim.
    - no object-store output.
    - no lakehouse/table commit.
    - no hidden fast mode.
    - no external engine fallback.
  - Claim boundary:
    - Fanout benchmark rows are local pre-release workflow/evidence rows only.
    - They cannot support public performance, superiority, Spark-displacement, production,
      SQL/DataFrame, object-store/lakehouse, Foundry, package, or release claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for every
      fanout-supported, blocked, unsupported, skipped, or report-only row.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with benchmark
      artifact refs, renderer/readiness refs, unsupported row examples, and no-fallback evidence.
  - Dependencies/blockers:
    - SourceState, VortexPreparedState, and OutputPlan contracts; benchmark scenario catalog;
      benchmark row schema; per-output correctness/replay evidence; local writer support; website
      benchmark renderer; and deterministic unsupported-row renderer.

- [ ] GAR-IOREUSE-1E cache invalidation and fingerprint contract
  - Source:
    - GAR-IOREUSE-1A through GAR-IOREUSE-1D.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/architecture/in-process-session-runtime.md`.
    - RFC 0017 fault tolerance/cancellation/recovery.
    - RFC 0029 certificates and state reuse.
    - RFC 0031 Native I/O.
  - Current state:
    - Current prepared/native source-state reuse is scoped to one batch process and selected
      scenario families, with source-state digests and preparation timing visible.
    - There is no cross-layer invalidation contract for source fingerprints, prepared-state
      fingerprints, output-plan fingerprints, Vortex version changes, policy changes, schema drift,
      or target output changes.
  - Next slice outcome:
    - Define content-addressed fingerprints and invalidation rules for `SourceState`,
      `VortexPreparedState`, `ExecutionPlan`, `OutputPlan`, and `SinkArtifact`.
    - Ensure stale or mismatched state produces deterministic invalidation diagnostics rather than
      hidden recomputation, stale reuse, or fallback.
  - User-visible surface:
    - CLI explain/capability output.
    - benchmark rows.
    - Python typed result/capability fields if exposed.
    - docs/status.
  - Implementation scope:
    - fingerprint schema.
    - cache-key/input-key report fields.
    - invalidation reason taxonomy.
    - benchmark row schema if fields are emitted.
    - deterministic stale-state diagnostics.
  - Evidence required:
    - `source_fingerprint_kind`.
    - `source_content_digest`.
    - `source_mtime`.
    - `source_size`.
    - `object_etag`.
    - `manifest_version`.
    - `schema_digest`.
    - `plan_digest`.
    - `output_plan_digest`.
    - `cache_valid`.
    - `invalidation_reason`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - Every reusable layer has a stable fingerprint or explicit `not_fingerprintable` reason.
    - State is invalidated on source changes, schema/dtype changes, Vortex version/API changes,
      policy changes, output target changes, or evidence-level mismatch.
    - Reuse is blocked when the source fingerprint changes.
    - Reuse is blocked when schema or plan digest changes.
    - Object-store ETag/version handling is planned but not runtime-claimed.
    - Invalidated state cannot be counted as a reuse hit.
    - Invalidation is visible in benchmark and explain surfaces when those fields are emitted.
  - Verification:
    - local file invalidation tests.
    - schema-change tests.
    - plan-change tests.
    - fingerprint stability tests.
    - invalidation matrix tests.
    - benchmark row contract tests if fields are emitted.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no persistent disk cache.
    - no daemon/service cache.
    - no distributed cache.
    - no object-store cache.
    - no runtime behavior in the planning slice.
    - no performance claim.
    - no external fallback.
  - Claim boundary:
    - Fingerprint evidence may claim only deterministic reuse eligibility, reuse rejection, or
      invalidation reason for scoped local state.
    - It does not authorize cache performance claims, production cache correctness, remote cache,
      object-store/lakehouse, Foundry, package, or release claims.
  - Security boundary:
    - No secrets or credentials may appear in cache keys, fingerprints, digests, explain output, or
      benchmark evidence.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are required for reuse hits,
      misses, invalidations, blockers, unsupported rows, and report-only rows.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with fingerprint
      stability refs, invalidation matrix refs, and no-fallback evidence.
  - Dependencies/blockers:
    - source/prepared/output fingerprint schema, schema/plan/output digest stability, policy/version
      refs, local file metadata handling, object-store ETag/version posture, secret redaction
      policy, and invalidation diagnostics.

- [ ] GAR-IOREUSE-1F evidence-safe reuse levels
  - Source:
    - GAR-IOREUSE-1A through GAR-IOREUSE-1E.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/architecture/runtime-evidence-level-tiering.md`.
    - `docs/architecture/operational-evidence-policy-hardening.md`.
    - RFC 0029 certificates and state reuse.
    - RFC 0039 typed command/result envelope.
  - Current state:
    - Evidence-level runtime tiering is planned under `GAR-PERF-2A`, but reuse levels are not yet
      first-class fields across source state, prepared state, operator source-state, output plan,
      and sink artifacts.
    - Minimal evidence or reuse-hit rows must not accidentally become claim-grade.
  - Next slice outcome:
    - Define evidence-safe reuse levels for each layer:
      - `discovery_reuse`.
      - `schema_reuse`.
      - `parse_plan_reuse`.
      - `prepared_vortex_reuse`.
      - `operator_source_state_reuse`.
      - `output_plan_reuse`.
      - `result_replay_reuse`.
    - Define allowed statuses: `reuse_hit`, `reuse_miss`, `not_needed`, `blocked`, `unsupported`,
      `invalidated`, and `report_only`.
    - Keep evidence level, execution mode, reuse status, and claim gate independent.
  - User-visible surface:
    - typed CLI envelopes.
    - Python typed models if exposed.
    - benchmark rows.
    - website benchmark explanation.
    - compute-flow docs.
  - Implementation scope:
    - typed evidence envelope fields.
    - benchmark row schema.
    - Python typed accessors if fields are surfaced.
    - website benchmark copy/readiness rules.
    - release metadata checks if claims could be affected.
  - Evidence required:
    - `reuse_level`.
    - `reuse_hit`.
    - `reuse_digest`.
    - `reuse_allowed`.
    - `reuse_blocker`.
    - `execution_mode`.
    - `evidence_level`.
    - layer-specific reuse status fields.
    - layer-specific reuse hit fields.
    - layer-specific invalidation reason fields.
    - `claim_gate_status`.
    - `claim_grade_requirements_met=false` for evidence-light/reuse-only rows unless later gates
      explicitly approve.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
  - Acceptance:
    - Reuse never hides execution mode.
    - Reuse never upgrades claim status by itself.
    - Reuse evidence is visible in `minimal_runtime` and `certified` modes.
    - Reuse-hit evidence is not treated as correctness, output, or performance evidence by itself.
    - `minimal_runtime` and reuse-only rows remain `not_claim_grade` unless a later scoped gate
      attaches correctness, Native I/O, materialization/decode, output, reproducibility, and
      no-fallback evidence.
    - Unsupported, blocked, invalidated, and report-only statuses remain machine-readable.
    - Execution mode, evidence level, reuse level, and output format remain separate fields.
  - Verification:
    - typed envelope snapshot tests.
    - benchmark row contract tests.
    - release-readiness metadata tests.
    - `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no runtime speed mode.
    - no hidden cache.
    - no claim-grade promotion.
    - no performance/superiority claim.
    - no object-store/lakehouse or Foundry runtime.
    - no external fallback.
  - Claim boundary:
    - Evidence-safe reuse levels may claim only that reuse status and evidence completeness are
      visible and machine-readable.
    - They do not authorize performance, superiority, Spark-displacement, production,
      SQL/DataFrame, object-store/lakehouse, Foundry, package, or release claims.
  - Fallback boundary:
    - `fallback_attempted=false` and `external_engine_invoked=false` are mandatory for every reuse
      level and every evidence level.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with envelope
      snapshots, benchmark row refs, readiness refs, and no-fallback evidence.
  - Dependencies/blockers:
    - evidence-level runtime tiering, typed envelope schema, benchmark row schema, claim-gate
      policy, SourceState/VortexPreparedState/OutputPlan reuse status fields, Python typed models,
      and release/readiness metadata checks.

- [ ] GAR-IOREUSE-1G Foundry no-input generated-output fanout
  - Source:
    - GAR-GEN-1.
    - GAR-COMPAT-1B.
    - GAR-IOREUSE-1A through GAR-IOREUSE-1F.
    - `docs/architecture/io-reuse-and-fanout-architecture.md`.
    - `docs/foundry/proof-of-use-certification.md`.
    - RFC 0036 Foundry integration pack.
  - Current state:
    - Foundry no-dataset smoke exists.
    - No-input smoke and generated-output planning are distinct from source-read execution.
    - Source-free generated output is not first-class.
    - Foundry proof is local/style-only, and real Foundry runtime proof remains gated.
    - S3/object-store is report-only/gated and must not be used as a direct runtime write path for
      this slice.
  - Next slice outcome:
    - Add Foundry-style generated-output smoke posture:
      - no input dataset.
      - generate deterministic source.
      - prepare through ShardLoom/Vortex.
      - write result dataset.
      - write evidence dataset.
    - Define report-only Foundry generated-output fanout posture for source-free generated output
      that writes through Foundry output APIs where applicable, not direct S3.
    - Preserve generated-source certificate fields, output-plan evidence, fanout output count, and
      Foundry runtime boundary fields.
  - User-visible surface:
    - Foundry proof docs.
    - generated-output capability matrix.
    - website/status after projection.
    - future Python/API examples.
  - Implementation scope:
    - Foundry proof docs.
    - generated-source/output-plan capability rows.
    - local Foundry-style smoke diagnostics if implementation is later admitted.
    - evidence envelope fields for Foundry runtime boundaries.
  - Evidence required:
    - `input_dataset_count=0`.
    - `source_io_performed=false`.
    - `generated_source_created=true`.
    - `generated_source_kind`.
    - `generated_source_schema_digest`.
    - `generated_source_row_count`.
    - `generated_source_plan_digest`.
    - `generated_source_certificate_status`.
    - `output_plan_id`.
    - `output_plan_reuse_hit`.
    - `fanout_output_count`.
    - `output_io_performed`.
    - `output_native_io_certificate_status`.
    - `foundry_runtime_invoked`.
    - `foundry_compute_invoked`.
    - `foundry_spark_invoked=false`.
    - `direct_s3_write_invoked=false`.
    - `fallback_attempted=false`.
    - `external_engine_invoked=false`.
    - `claim_gate_status`.
  - Acceptance:
    - No-input smoke remains separate from generated-output execution.
    - No-input smoke and generated-output execution are separate.
    - Generated-source evidence remains separate from source Native I/O certificate evidence.
    - Foundry generated-output fanout is report-only unless a future admitted smoke writes through
      Foundry output APIs and emits output evidence.
    - Foundry output is via transform output APIs, not direct object-store/S3 write.
    - Direct S3/object-store writes remain blocked.
    - No Foundry production or Marketplace claim is implied.
  - Verification:
    - Foundry proof doc checks.
    - generated-output capability/report snapshot tests if rows are emitted.
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`.
    - `python scripts/check_website_readiness.py`.
    - `git diff --check`.
  - Non-goals:
    - no S3/object-store runtime.
    - no direct object-store write.
    - no Foundry production claim.
    - no Foundry Marketplace/package claim.
    - no broad SQL/DataFrame runtime.
    - no external engine fallback.
  - Claim boundary:
    - This slice may claim only report-only Foundry generated-output fanout posture or a future
      scoped local/dev-stack smoke if evidence is attached.
    - It does not authorize Foundry production, object-store/lakehouse, performance, superiority,
      Spark-displacement, package, release, or broad generated-output runtime claims.
  - Fallback boundary:
    - `fallback_attempted=false`, `external_engine_invoked=false`,
      `foundry_spark_invoked=false`, and `direct_s3_write_invoked=false` are required for every
      Foundry generated-output fanout row.
  - Ledger rule:
    - When complete, move the detailed completed session to the completed ledger with Foundry proof
      refs, generated-source/output evidence refs, deterministic blockers, and no-fallback
      evidence.
  - Dependencies/blockers:
    - GeneratedSourceCertificate contract, OutputPlan reuse contract, Foundry proof docs,
      Foundry-style transform wrapper examples, generated-output capability rows, output evidence
      dataset posture, and no-direct-S3/no-Spark boundary fields.

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
    - GAR-GEN-1 adds the deeper `GeneratedSourceCertificate` planning lane.
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
    - GAR-GEN-1A certificate contract, output sink evidence model, Python API decision, SQL/DataFrame
      admission rows, and Foundry output API proof boundary.
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

- [ ] GAR-GEN-1A generated-source certificate and capability contract
  - Source: RFC 0031; RFC 0032; RFC 0033; RFC 0036; RFC 0037; RFC 0039;
    `docs/architecture/compute-engine-flow-reference.md`;
    `docs/foundry/proof-of-use-certification.md`; `python/README.md`.
  - Current state:
    - No-input smoke/capability behavior exists, but it is status/capability/smoke only.
    - Benchmark synthetic fixtures exist, but they are benchmark setup and must not be reported as
      user-facing generated-output runtime.
    - DataFrame/query-builder and SQL posture is report-only; source-free SQL/DataFrame execution is
      not claimable.
    - S3/object-store read/write remains report-only/gated.
  - Next slice outcome:
    - Add a report-only `GeneratedSourceCertificate` contract and capability rows that distinguish
      `no_dataset_smoke`, `user_generated_source`, and `engine_native_generated_source`.
  - User-visible surface:
    - CLI capability/report output, Python capability/DataFrame matrix, compute-flow docs, Foundry
      proof docs, and typed envelope certificate references.
  - Implementation scope:
    - Certificate/report structs, capability rows, Python typed accessors, snapshot tests, docs, and
      release-readiness metadata.
  - Evidence required:
    - correctness refs: deterministic generated-row fixture expectations for promoted runtime paths.
    - benchmark refs: none required for report-only contract; required before performance claims.
    - execution certificate refs: required for any future generated-output runtime.
    - Native I/O certificate refs: no source Native I/O certificate when `input_dataset_count=0`;
      output Native I/O certificate refs required when output is written.
    - materialization/decode refs: generated-row materialization and sink boundary refs.
    - policy/no-fallback refs: `fallback_attempted=false`, `external_engine_invoked=false`.
  - Acceptance:
    - `no_dataset_smoke` remains separate from generated-output execution and reports no source Native
      I/O certificate, no generated rows, and no output data claim.
    - `user_generated_source` reports that user Python code created rows and ShardLoom consumed them
      as generated/literal input only when deterministic generation evidence exists.
    - `engine_native_generated_source` names generator nodes such as `range`, `sequence`, `values`,
      `literal_table`, `calendar`, and deterministic synthetic profiles without claiming runtime
      support before implementation evidence.
    - Required fields are defined: `input_dataset_count=0`, `source_io_performed=false`,
      `generated_source_created`, `generated_source_kind`, `generated_source_schema_digest`,
      `generated_source_row_count`, `generated_source_plan_digest`, `generated_source_seed`,
      `generation_deterministic`, `output_io_performed`, `output_native_io_certificate_status`,
      `generated_source_certificate_status`, `fallback_attempted=false`,
      `external_engine_invoked=false`, and `claim_gate_status`.
  - Verification:
    - `cargo test -p shardloom-contract-tests --test release_readiness_metadata`
    - capability snapshot tests when report structs/accessors are added.
    - Python typed-view tests when Python accessors are added.
    - `git diff --check`
  - Non-goals:
    - No generated-output execution, SQL parser/runtime, DataFrame runtime, S3 runtime,
      object-store write, Foundry invocation, package publication, or external engine fallback in
      the contract-only slice.
  - Claim boundary:
    - This slice may claim only that source-free generated-output support has a report-only planning
      contract. It does not allow output data, SQL/DataFrame runtime, object-store/lakehouse,
      Foundry production, performance, or Spark-displacement claims.
  - Fallback boundary:
    - Report-only surfaces must keep `fallback_attempted=false`,
      `fallback_execution_allowed=false`, and `external_engine_invoked=false`.
  - Dependencies/blockers:
    - Typed envelope certificate-slot compatibility, capability matrix ownership, Python wrapper
      accessors, and output sink certificate model.
- [ ] GAR-GEN-1B no-dataset smoke separation hardening
  - Source: GAR-GEN-1A; RFC 0012; RFC 0036; Foundry proof docs; release-readiness docs.
  - Current state:
    - No-input smoke appears in capability/proof surfaces, but future generated-output work needs a
      stronger distinction from actual generated rows and written output.
  - Next slice outcome:
    - Harden diagnostics, docs, and report fields so `no_dataset_smoke` cannot be confused with
      source-free generated-output execution.
  - User-visible surface:
    - CLI status/capability output, Foundry proof report, release readiness docs, Python typed views.
  - Implementation scope:
    - Existing no-dataset smoke reports, proof scripts, docs, and snapshot tests.
  - Evidence required:
    - correctness refs: no data execution assertion.
    - execution certificate refs: absent or `not_applicable` for data execution.
    - Native I/O certificate refs: source certificate absent; output certificate absent unless a
      non-data proof artifact is explicitly written.
    - policy/no-fallback refs: no source I/O, no output data I/O, no external engine.
  - Acceptance:
    - Reports expose `input_dataset_count=0`, `source_io_performed=false`,
      `generated_source_created=false`, `output_io_performed=false`,
      `generated_source_certificate_status=not_applicable`, and
      `claim_gate_status=smoke_only|not_claim_grade`.
    - No source Native I/O certificate is claimed.
    - No output data claim is created.
  - Verification:
    - existing no-dataset smoke/proof tests.
    - release readiness metadata tests.
    - Python compileall if Python surfaces change.
    - `git diff --check`
  - Non-goals:
    - No generated rows, SQL/DataFrame execution, output dataset write, S3/object-store write, or
      Foundry runtime invocation.
  - Claim boundary:
    - May claim only no-dataset status/proof smoke. It is not generated-output runtime evidence.
  - Fallback boundary:
    - `fallback_attempted=false`, `external_engine_invoked=false`, and no external compute.
  - Dependencies/blockers:
    - GAR-GEN-1A certificate vocabulary.
- [ ] GAR-GEN-1C user-generated source local-output fixture smoke
  - Source: GAR-GEN-1A; RFC 0032; RFC 0033; RFC 0037; RFC 0039.
  - Current state:
    - Python can declare workflow/report surfaces, but user-created rows are not consumed as a
      ShardLoom generated/literal source with output sink evidence.
  - Next slice outcome:
    - Add one narrow local fixture-smoke path where user Python rows are serialized into a
      ShardLoom generated/literal source, written to a local output target, and certified without
      source I/O.
  - User-visible surface:
    - Future Python `ctx.from_rows([...]).write(...)` path or equivalent report command, CLI JSON,
      output artifact refs, and Python typed result.
  - Implementation scope:
    - Generated-source ingestion contract, local output sink, certificate emission, Python wrapper
      affordance, tests, and docs.
  - Evidence required:
    - correctness refs: digest over rows/schema/output.
    - execution certificate refs: ShardLoom-native generated-source execution.
    - Native I/O certificate refs: no source certificate; output certificate required.
    - materialization/decode refs: row materialization boundary and local sink boundary.
    - policy/no-fallback refs: no external engine, no object-store/network I/O.
  - Acceptance:
    - Emits `input_dataset_count=0`, `source_io_performed=false`,
      `generated_source_created=true`, `generated_source_kind=user_rows`,
      `generated_source_schema_digest`, `generated_source_row_count`,
      `generated_source_plan_digest`, `generation_deterministic=true`, `output_io_performed=true`,
      `output_native_io_certificate_status`, `generated_source_certificate_status=present`,
      `fallback_attempted=false`, and `external_engine_invoked=false`.
    - Local output is written through an evidence-backed sink.
    - SQL/DataFrame runtime is not implied.
  - Verification:
    - focused Rust/Python fixture tests.
    - output certificate snapshot tests.
    - Python compileall.
    - `git diff --check`
  - Non-goals:
    - No S3/object-store write, Foundry write, SQL execution, DataFrame expression execution,
      performance claim, or broad output sink support.
  - Claim boundary:
    - May claim one scoped local user-row generated-output smoke only after evidence lands.
  - Fallback boundary:
    - No external engine, object-store, network, or hidden pandas/Polars/DuckDB/DataFusion/Spark
      execution.
  - Dependencies/blockers:
    - GAR-GEN-1A, local output sink certificate support, Python wrapper API decision.
- [ ] GAR-GEN-1D engine-native generated source local-output runtime slice
  - Source: GAR-GEN-1A; RFC 0021; RFC 0026; RFC 0032; RFC 0033.
  - Current state:
    - Engine-native generator nodes for `range`, `sequence`, `values`, `literal_table`, calendar/date
      dimensions, and deterministic synthetic profiles are not runtime-supported.
  - Next slice outcome:
    - Implement one narrow ShardLoom-native generator node, such as `range` or `values`, that writes a
      local output with generated-source and output evidence.
  - User-visible surface:
    - Future Python `ctx.range(...)`, `ctx.literal_table(...)`, `ctx.calendar(...)`, SQL literal or
      `VALUES` posture, DataFrame-style source-free builder, CLI JSON, and output artifact refs.
  - Implementation scope:
    - Plan IR generator node, generator execution, deterministic seed/plan digest, local sink,
      certificate emission, Python/SQL/DataFrame capability rows, tests, and docs.
  - Evidence required:
    - correctness refs: generated output digest and schema assertions.
    - execution certificate refs: ShardLoom-native generator execution.
    - Native I/O certificate refs: no source certificate; output certificate required.
    - materialization/decode refs: generator materialization and sink boundary.
    - policy/no-fallback refs: no external engine and no hidden fixture import.
  - Acceptance:
    - Runtime output reports `generated_source_kind=range|values|literal_table|calendar|synthetic`,
      row count, schema digest, plan digest, optional seed, deterministic status, output sink
      evidence, and claim gate status.
    - SQL `SELECT` literals and `VALUES` remain report-only until parser/binder/runtime evidence
      exists.
    - DataFrame-style builder remains report-only until Python/API evidence exists.
  - Verification:
    - focused generator runtime tests.
    - typed envelope/certificate snapshot tests.
    - Python tests if API accessor lands.
    - `cargo fmt --all -- --check`
    - `cargo test --workspace --all-targets`
    - `git diff --check`
  - Non-goals:
    - No broad SQL/DataFrame runtime, non-local output, object-store/lakehouse write, Foundry
      production claim, package publication, or performance claim.
  - Claim boundary:
    - May claim only the implemented local generator node and local output sink, not general
      source-free SQL/DataFrame generation.
  - Fallback boundary:
    - `fallback_attempted=false`, `external_engine_invoked=false`, no external runtime delegation.
  - Dependencies/blockers:
    - GAR-GEN-1A, output sink evidence, Plan IR generator-node design, semantic tests.
- [ ] GAR-GEN-1E source-free SQL/DataFrame/API admission matrix
  - Source: GAR-GEN-1A; GAR-0032-A; GAR-0032-B; RFC 0032; RFC 0033; RFC 0037.
  - Current state:
    - SQL and DataFrame/query-builder capability rows are report-only and do not yet name
      source-free generated-output methods as first-class rows.
  - Next slice outcome:
    - Extend SQL/DataFrame/Python capability surfaces to classify `ctx.range`, `ctx.from_rows`,
      `ctx.literal_table`, `ctx.calendar`, `ctx.write`, SQL literal `SELECT`, SQL `VALUES`,
      source-free projection, and admitted `generate_series`/`range` vocabulary.
  - User-visible surface:
    - Python capability view, DataFrame method matrix, SQL capability rows, docs, and diagnostics.
  - Implementation scope:
    - Capability report rows, Python typed accessors, docs, and snapshot tests.
  - Evidence required:
    - diagnostic/no-fallback refs for report-only rows.
    - generated-source/output certificate refs only for runtime-supported rows.
  - Acceptance:
    - Each method/query form reports `support_status`, `runtime_execution`, `data_read`,
      `write_io`, `source_io_performed`, `generated_source_created`, blocker ID, required evidence,
      and claim boundary.
    - Report-only rows never parse or execute SQL, materialize rows, write output, resolve
      credentials, probe object stores, or invoke external engines.
  - Verification:
    - capability snapshot tests.
    - Python typed capability tests.
    - release readiness metadata tests.
    - `git diff --check`
  - Non-goals:
    - No SQL parser/binder/runtime, broad DataFrame runtime, package publication, object-store
      runtime, or external engine fallback.
  - Claim boundary:
    - Capability vocabulary only unless a child runtime slice adds evidence.
  - Fallback boundary:
    - `fallback_attempted=false`, `external_engine_invoked=false`, and
      `fallback_execution_allowed=false`.
  - Dependencies/blockers:
    - GAR-GEN-1A, GAR-0032-A SQL readiness, GAR-0032-B DataFrame method matrix.
- [ ] GAR-GEN-1F Foundry generated-output proof boundary
  - Source: GAR-GEN-1A; RFC 0036; `docs/foundry/proof-of-use-certification.md`.
  - Current state:
    - Foundry proof is local Foundry-style only; it includes no-dataset smoke and local Vortex smoke,
      but it does not implement Foundry generated-output runtime.
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
    - Benchmark confidence is not probabilistic.
    - GAR-PERF-1D is the adjacent report-only Bayesian performance/layout advisor slice.
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
