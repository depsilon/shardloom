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

Section-completion rule:

- Prefer one substantial PR/session that completes an entire runtime section over a sequence of
  tiny row/format/operator PRs. Split work only when the remaining section has independent safety,
  dependency, or verification boundaries that cannot reasonably land together.
- For a section-completion PR, derive the full checklist from the owning `GAR-*` item, companion
  5-series runtime equivalent, status/capability files, route taxonomy, tests, and user-visible
  surfaces before editing. The PR should close the section across runtime code, typed reports,
  Python/CLI surfaces, docs/status artifacts, and verification evidence together.
- Avoid wording such as "promote one format/operator at a time" unless a format/operator truly has
  a separate external dependency or deterministic blocker. When the engine architecture expects a
  unified route, complete the unified route and keep per-format differences confined to read/ingest
  and write/sink boundaries.

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
  detailed session history and historical phase ledgers.
- Supporting docs may contain rationale, inventories, traceability, and historical notes, but they
  must not introduce a second current queue.
- Repeated support, claim-boundary, benchmark-interpretation, and runtime-state explanations should
  be owned by one canonical doc or generated data artifact; other pages should link to or render
  that source instead of restating parallel wording.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- Supporting docs must not keep unchecked implementation checklists outside this file and
  `docs/architecture/global-architecture-review.md`. Scope-boundary lists may remain, but real work
  must be carried by a `GAR-*` item below.

Reference index:
- Status source: `README.md`, `docs/architecture/phased-execution-completed-ledger.md`,
  `docs/architecture/rfc-phase-traceability.md`, `docs/architecture/global-architecture-review.md`,
  `docs/architecture/compute-engine-flow-reference.md`, and
  `docs/architecture/website-minimal-public-surface-reset.md`.
- Website redesign references:
  `docs/architecture/website-redesign-reference-synthesis.md`,
  `docs/architecture/website-redesign-information-architecture.md`, and
  `docs/architecture/website-redesign-content-model.md`, and
  `docs/architecture/website-redesign-framework-decision.md`.
- Compute-flow and benchmark references:
  `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `docs/architecture/benchmark-persistent-runner-decision.md`,
  `docs/architecture/performance-attribution-and-execution-structure.md`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/benchmark-competitive-claim-evidence.md`, and `docs/benchmarks/*`.
- Runtime architecture references:
  `docs/architecture/runtime-evidence-level-tiering.md`,
  `docs/architecture/evidence-aware-logical-optimizer.md`,
  `docs/architecture/vortex-scan-pushdown-completion.md`,
  `docs/architecture/compressed-encoded-kernel-registry.md`,
  `docs/architecture/fused-operator-pipeline.md`,
  `docs/architecture/in-process-session-runtime.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/architecture/allocation-buffer-pool-optimization.md`,
  `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`,
  `docs/architecture/dynamic-work-shaping.md`,
  `docs/architecture/pulseweave-runtime-control.md`,
  `docs/architecture/spill-reservation-lifecycle-integration.md`, and
  `docs/architecture/effect-budget-plan.md`.
- Claim, release, and adoption references:
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/operational-evidence-policy-hardening.md`,
  `docs/architecture/evidence-native-generated-execution-observability-confidence.md`,
  `docs/architecture/adoption-commercial-readiness-friction-reduction.md`,
  `docs/architecture/workspace-feature-build-matrix.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/competitive-replacement-sufficiency-gate.md`,
  `docs/architecture/cg5-cg6-stateful-reuse-evidence-expansion.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/release-architecture-tracker-gate.md`,
  `docs/release/final-release-rehearsal.md`,
  `docs/architecture/universal-import-deployment-baseline-harness.md`,
  `docs/architecture/extension-manifest-effect-capability-matrix.md`,
  `docs/architecture/credential-policy-enforcement-gate.md`,
  `docs/architecture/sandbox-governance-runtime-readiness.md`,
  `docs/architecture/plugin-abi-udf-sandbox-blocker.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/architecture/rfc-coverage-followthrough.md`,
  `docs/architecture/typed-command-result-envelope.md`,
  `docs/architecture/crate-posture-public-exports.md`, and `docs/release/*`.
- Compatibility, adapters, and platform references:
  `docs/architecture/universal-input-contract.md`,
  `docs/architecture/universal-compatibility-coverage-scoreboard.md`,
  `docs/architecture/object-store-request-planner.md`,
  `docs/architecture/table-intelligence-layer.md`,
  `docs/architecture/lakehouse-value-prop-compatibility.md`,
  `docs/architecture/incumbent-gap-opportunity-map.md`,
  `docs/architecture/agent-contract-pack.md`, and `docs/use-cases/*`.
- Vortex and project hygiene references:
  `docs/architecture/vortex-public-api-inventory.md`,
  `docs/architecture/vortex-runtime-utilization-audit.md`,
  `docs/architecture/vortex-adapter-integration-plan.md`,
  `docs/architecture/vortex-upstream-alignment-hardening.md`,
  `docs/architecture/canonical-terminology.md`, `docs/architecture/systems-learning-map.md`,
  `docs/architecture/repo-cleanup-backlog.md`,
  `docs/architecture/diagnostics-normalization-backlog.md`,
  `docs/architecture/terminology-consolidation-backlog.md`,
  `docs/architecture/feature-footprint-doctor-plan.md`, and
  `docs/skills/vortex/vortex-first-provider-check.md`.

Reference-doc rule: these files are evidence, guardrails, or inventories. They do not authorize
runtime behavior, support claims, dependency expansion, package publication, external effects, or
fallback execution unless a matching unchecked item below is completed with evidence and moved to
the ledger.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

Autonomous ordering rule:

1. Finish the unchecked non-runtime closeout queue first.
2. Then work the runtime implementation queue.
3. Runtime queue items must explicitly enable an end-user runtime path, a runtime admission/blocker
   that protects user-visible behavior, or a validator that gates runtime claims. Docs-only or
   report-only work cannot complete a runtime item unless the item is explicitly a runtime-safety
   blocker.

Live plan hygiene:

- Planned must contain only unchecked actionable work. Completed checklist items, completed
  sections, and completed session details belong only in
  `docs/architecture/phased-execution-completed-ledger.md`.
- If a completed item is found in Planned, remove it from this file after confirming the matching
  ledger entry exists or adding that ledger entry.
- Do not leave a completed parent section in Planned just to preserve history. Keep only active
  child work or a short pointer to the ledger when history is needed.
- Do not start a runtime implementation item while unchecked non-runtime closeout items remain
  above it unless the user explicitly reprioritizes and the reprioritization is recorded here.
- A runtime item is valid only when it has a `Runtime enablement:` field that names the runnable
  path, admission/blocker, or validator it enables. If that field cannot be made concrete, the item
  belongs in non-runtime planning or the completed ledger, not the runtime queue.

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

#### Non-Runtime Closeout Queue

Complete these documentation, capability, security, release, and claim-gate items before starting
new runtime implementation work unless the user explicitly reprioritizes. These items must not add
runtime behavior or support claims. Add a concrete unchecked item here only when a new
documentation, website, security, release, or claim-gate blocker must interrupt runtime work.

Current non-runtime sequence: complete the review-derived action items below before new runtime
expansion unless the user explicitly reprioritizes. Completed non-runtime history belongs in
`docs/architecture/phased-execution-completed-ledger.md`.

#### Runtime Implementation Queue - Runtime-Enabling Work Only

The earlier broad runtime rollup queues have been consolidated into the implementation-ready
`GAR-RUNTIME-IMPL-4*` and `GAR-RUNTIME-IMPL-5*` queues below. Work these only after the
unchecked non-runtime closeout items above are complete or explicitly reprioritized by the user.

Runtime completion rule:

- Every runtime item must enable a concrete runtime behavior, runtime admission/blocker, or
  runtime-claim validator that directly protects a usable workflow.
- Every runtime item below must include a `Runtime enablement:` field naming the behavior,
  admission/blocker, or validator it enables.
- Runtime work should be grouped by completed engine section, not by the smallest testable sliver.
  The default PR shape is a complete source/adapter/ingest, expression-family, output/fanout,
  envelope/session, or scan/kernel section with all companion docs and validators updated.
- A docs-only or report-only update cannot complete a runtime item unless the item explicitly says
  it is a runtime-safety blocker or validator.
- Completed runtime details belong in `docs/architecture/phased-execution-completed-ledger.md`, not
  in this live queue.

#### GAR-RUNTIME-IMPL-4 - Final Full-Runtime Implementation Leaf Queue
Current runtime ordering note (2026-05-26): prioritize engine-internal completion first. The
`GAR-RUNTIME-IMPL-4I` scan/pushdown matrix, `GAR-RUNTIME-IMPL-4K` runtime-envelope validator
rollout, `GAR-RUNTIME-IMPL-4L/5I` scoped session/cache lifecycle,
`GAR-RUNTIME-IMPL-5F` prepared/native Vortex lifecycle, the `GAR-RUNTIME-IMPL-4F/4F1/5D`
local adapter/ingest parity closeout, `GAR-RUNTIME-IMPL-4P/5M` declared local scale runtime
closeout, `GAR-RUNTIME-IMPL-5H` runtime evidence/claim validator closeout, and
`GAR-RUNTIME-IMPL-5R` PulseWeave automatic prepared/local runtime control,
`GAR-RUNTIME-IMPL-5C` Python workflow/method-matrix alignment,
`GAR-RUNTIME-IMPL-5K` public no-credential object-store fixture read admission,
`GAR-RUNTIME-IMPL-4Q/5N` live/hybrid loopback control-plane and distributed-blocker admission,
`GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout, and the parent
`GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`
advanced scalar deterministic semantics closeout and `GAR-RUNTIME-IMPL-4D-F2` complex dtype
deterministic blocker closeout plus `GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery
semantics closeout plus `GAR-RUNTIME-IMPL-5P` Foundry dev-stack generated-output and transform
proof are complete and recorded in the ledger.
The remaining internal-engine follow-ups below stay ahead of SQL/Python surface backstops,
benchmark gates, and release usability.
Completed queue blocks have moved to
`docs/architecture/phased-execution-completed-ledger.md`; this live queue should show only remaining
work.

This queue exists to keep the remaining "fully functional / usable compute engine" work from
hiding inside broad architecture items. Treat these as the explicit runtime implementation slices
that must be worked before any full-runtime readiness claim. Each item below must land runnable
runtime behavior, deterministic runtime admission/blockers, or runtime-claim validation; planning
or documentation updates alone are insufficient.

The remaining 5-series closeout items are interleaved with their owning 4-series runtime sections
below. They are coverage-assurance backstops, not a second parallel runtime queue. Work a 5-series
item only after the matching 4-series runtime item has landed or when the 4-series item explicitly
splits residual runtime gaps into this queue. Completing a 5-series item requires evidence,
validators, docs/website parity, and a completed-ledger entry.

- [ ] GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down
  - Source: active user objective, `docs/architecture/global-architecture-review.md`,
    `docs/architecture/compute-engine-flow-reference.md`, and
    `target/compute-engine-completion-gate.json`.
  - Current state: `GAR-RUNTIME-IMPL-5J benchmark publishing, profile, and claim-grade refresh
    gate` is complete for the current `full_local` benchmark publication. The first residual
    blocker burn-down promoted benchmark sub-evidence for optimizer posture, SourceState,
    VortexPreparedState, reuse level, copy-budget, preparation-spine, capillary, layout, and local
    split-operator status fields to runtime-ready local evidence where the rows already had
    top-level `success`, `claim_grade`, runtime-validation `passed`, and no fallback/external engine
    invocation. A follow-up freshness pass closed the two stale `GAR-PERF-2C` global architecture
    review rows against the already-landed scan-pushdown completion evidence. Full compute-engine
    completion remains blocked by 51 unchecked global architecture review items and this unchecked
    phase-plan item.
  - Next slice outcome: close or split the 51 global architecture review items into runtime-ready
    evidence slices until the completion gate no longer has unchecked review/phase-plan blockers.
  - Runtime enablement: strict whole-engine completion validator plus the next runtime section that
    removes blocker rows from the validator instead of merely documenting them.
  - User-visible surface: completion gate JSON, benchmark evidence, phase plan, global architecture
    review, release readiness, and eventually package/deploy readiness.
  - Implementation scope: completion validator, residual-blocker reducer in runtime/benchmark
    evidence producers, focused Rust/Python tests, and docs/website freshness updates for the
    specific blocker family being closed.
  - Evidence required: zero unchecked planned/review items for the claimed scope, top-level
    ShardLoom rows `success`/`claim_grade`/runtime-validation `passed`, no fallback/external engine
    invocation, and zero residual `blocked`, `unsupported`, `not_claim_grade`, `fixture_smoke_only`,
    or `report_only` statuses in the claimed completion surface.
  - Acceptance: `scripts/check_compute_engine_completion_gate.py` passes without
    `--allow-incomplete` for the completed scope; residual blocker counts decrease monotonically as
    runtime sections land; no public/package/production/performance claim is made until the whole
    gate passes.
  - Verification:
    ```powershell
    python scripts\check_compute_engine_completion_gate.py --output target\compute-engine-completion-gate.json
    python -m unittest python.tests.test_compute_engine_completion_gate
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --all-targets
    git diff --check
    ```
  - Non-goals: no hidden fallback, no external query engine execution, no package publication, no
    broad public production/performance claim while the gate is blocked.
  - Dependencies/blockers: this item depends on the already-published 5J full-local benchmark
    artifact, current global review inventory, and the residual blocker families enumerated by the
    completion gate. It is blocked until each residual blocker family is converted into runtime
    evidence or deterministic out-of-scope diagnostics accepted by the claimed surface.
  - Claim boundary: completion is claimable only when the gate passes without
    `--allow-incomplete`.
  - Fallback boundary: `fallback_attempted=false` and `external_engine_invoked=false` remain
    required for every ShardLoom row and completion artifact.
  - Ledger rule: when this item closes, add the gate report, residual blocker deltas, and validation
    commands to `docs/architecture/phased-execution-completed-ledger.md`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
