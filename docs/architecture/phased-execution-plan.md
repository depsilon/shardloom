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
semantics closeout are complete and recorded in the ledger.
The remaining internal-engine follow-ups below stay ahead of SQL/Python surface backstops,
benchmark and Foundry gates, and release usability.
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

- [ ] GAR-RUNTIME-IMPL-5P Foundry dev-stack generated-output and transform proof
  - Source: `GAR-COMMERCIAL-1E`, `GAR-IOREUSE-1G`, Foundry proof docs.
  - Current state: Foundry proof remains local/style-only or report-only; no production Foundry
    runtime/package/certified claim exists.
  - Next slice outcome: implement a personal dev-stack proof that imports the local package,
    resolves the CLI, runs source-free generated output and one staged-input transform, writes a
    result dataset and evidence dataset through Foundry-style output APIs, and preserves blocked
    flags.
  - Runtime enablement: local/dev-stack Foundry-style transform proof that runs ShardLoom locally
    and writes evidence datasets without Spark fallback.
  - User-visible surface: Foundry proof docs, examples, capability/status pages, release readiness.
  - Implementation scope: local Foundry-style transform wrapper, generated-source workflow,
    staged-input workflow, evidence dataset writer, runtime flag reporting.
  - Evidence required: input/output dataset counts, generated-source certificate, output Native I/O
    certificate, Foundry runtime/compute/Spark invoked flags, staged bytes, no-fallback fields.
  - Acceptance: Foundry can orchestrate a local proof without Spark fallback; evidence dataset
    output is mandatory; direct S3/object-store writes are not used.
  - Verification: local Foundry-style smoke, proof doc checks, release readiness metadata, website
    status checks.
  - Non-goals: no Foundry production support, package publication, marketplace listing, certified
    Foundry claim, or direct object-store path.
  - Claim boundary: local/dev-stack proof only.
  - Fallback boundary: Foundry/Spark compute cannot be reported as ShardLoom execution.
  - Dependencies/blockers: local package proof, generated-source runtime, output evidence writer.
  - Ledger rule: ledger entry must include proof commands, output/evidence refs, and blocked claims.

- [ ] GAR-RUNTIME-IMPL-4S clean install production usability and release rehearsal gate
  - Source: public preview readiness, package-channel matrix, website
    readiness, Use Case Atlas.
  - Current state: runtime slices are being promoted incrementally; production usability still
    requires complete runtime coverage, clean install proof, docs/website parity, examples, current
    benchmark evidence, and claim gates. A preview posture is not the target state.
  - Next slice outcome: run a no-publication production-readiness rehearsal from clean checkout or
    local package artifact through CLI/Python workflows, unsupported diagnostics, benchmarks,
    website/status, security/legal, and release metadata.
  - Runtime enablement: end-to-end usability validator proving admitted runtime paths from clean
    install through evidence inspection.
  - User-visible surface: README, docs/getting-started, website, package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow
    matrix, benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload or release tag without explicit human approval; no
    production/platform/performance/Spark-replacement claim until all matching runtime and evidence
    gates pass; no hidden fast mode.
  - Claim boundary: production readiness requires complete runtime coverage and workload-scoped
    evidence. Do not substitute a technical-preview target for the production engine goal.
  - Fallback boundary: release gates must fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, clean install script, docs/website
    parity, benchmark artifact policy, and security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, release-gate evidence, and
    remaining unsupported paths.

- [ ] GAR-RUNTIME-IMPL-5Q final production usability and website learning gate
  - Source: `GAR-RUNTIME-IMPL-4S`, `GAR-DOCS-1`, `GAR-WEB-ATLAS-1`, public-readiness,
    package-channel matrix.
  - Current state: repo, website, and docs are strong, but final usability requires clean install
    proof, examples, website/status parity, benchmark interpretation, security/legal/release checks,
    and a non-expert learning path after runtime slices land.
  - Next slice outcome: run a no-publication production-readiness rehearsal from clean checkout/local
    artifact through CLI/Python workflows, unsupported diagnostics, benchmarks, website/status,
    SECURITY/LICENSE/NOTICE checks, and release metadata.
  - Runtime enablement: final production usability validator across install, examples,
    runtime evidence, unsupported diagnostics, and website learning paths.
  - User-visible surface: README, docs/getting-started, website Field Guide/Use Case Atlas/status,
    package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks, docs link
    validation.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow matrix,
    benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals; website pages explain current runtime
    state without overclaiming.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload or release tag without explicit human approval; no
    production/platform/performance/Spark-replacement claim until all matching runtime and evidence
    gates pass; no hidden fast mode.
  - Claim boundary: production readiness requires complete runtime coverage and workload-scoped
    evidence. Do not substitute a technical-preview target for the production engine goal.
  - Fallback boundary: release gates fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, docs/website parity, benchmark
    artifact policy, security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, website readiness evidence,
    release-gate evidence, and remaining unsupported paths.

- [ ] GAR-RUNTIME-IMPL-5J benchmark publishing, profile, and claim-grade refresh gate
  - Source: `GAR-RUNTIME-IMPL-4M`, `GAR-BENCH-PUB-1`, benchmark publishing runbook.
  - Current state: benchmark publishing has a structured artifact model and the current public
    benchmark artifact is `full_local_plus_spark`. That profile requires
    `pyspark`, `spark-default`, and `spark-local-tuned` baseline lanes alongside ShardLoom,
    ShardLoom prepared Vortex, ShardLoom native Vortex, `shardloom-prepare-batch`, pandas, Polars
    eager/lazy, DuckDB, DataFusion, and Dask. The latest promoted CSV/Parquet artifact has all
    required lanes available, preserves PulseWeave and result-sink evidence fields, removes the
    alias-only `native-vortex` lane from profile accounting, adds cold-lane attribution blocking,
    and keeps external lanes baseline-only. The benchmark runner now has smoke-proven support for
    CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC across the required local/Spark baselines. The
    public broad-format timing-data refresh remains intentionally deferred until the remaining
    runtime/user-surface closeouts that can change benchmark interpretation are done, including
    `GAR-RUNTIME-IMPL-5P`, `GAR-RUNTIME-IMPL-4S`, and `GAR-RUNTIME-IMPL-5Q`.
  - Next slice outcome: require a current benchmark/correctness/evidence artifact for every
    promoted runtime path and block stale or incomplete public claims. The next public comparative
    refresh should preserve `full_local_plus_spark` required PySpark/Spark lane enforcement,
    publish broad-format coverage for CSV, Parquet, JSONL, Arrow IPC, Avro, and ORC, carry the
    SourceState, VortexPreparedState, scout-ingress, preparation-spine, differential, capillary,
    layout/write, copy-budget, SQL ladder, DataFrame workflow, Foundry/dev-stack, and
    clean-install usability fields, and move the main ShardLoom comparative roster toward
    `claim_grade` rows only for admitted runtime paths.
  - Runtime enablement: runtime-claim publishing validator that keeps public support status tied to
    fresh evidence.
  - User-visible surface: website benchmarks, docs/benchmarks, status page, release readiness.
  - Implementation scope: artifact freshness checker, profile matrix, runtime claim matrix,
    benchmark page ingestion from canonical generated artifacts, release validators, Spark/JVM
    profile publishing checks, format coverage checks, and claim-gate closeout diagnostics.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, correctness
    refs, certificate refs, no-fallback fields, claim gate, Spark lane availability, format
    coverage, and source-state/prepared-state/runtime-workflow coverage.
  - Acceptance: promoted paths are not presented publicly without current evidence; missing
    required lanes/scenarios/formats are visible and block claim-grade status; Spark lanes are
    required and available for `full_local_plus_spark` artifacts; broad formats are visible as
    supported by the runner until the deferred public refresh promotes their data; prepared/native
    source-state coverage is rendered from batch evidence instead of a misleading scalar count; the
    raw comparative roster renders all promoted rows, not a sample; the main ShardLoom comparative
    roster has no `blocked`, `unsupported`, `not_claim_grade`, or `fixture_smoke_only` rows before
    any broad claim-grade benchmark publication, while external lanes remain
    `external_baseline_only` and never satisfy ShardLoom evidence; the benchmark page reuses the
    runs-today support matrix for support posture and the promoted benchmark bundle for
    timing/coverage context.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests, `full_local_plus_spark` preflight/runbook evidence.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: workload-scoped local benchmark evidence only.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: remaining runtime/user-surface closeouts, benchmark manifest schema,
    runtime envelope validators, scenario fixtures, website renderer support.
  - Ledger rule: ledger entry must include artifact refs, profile, freshness, and public claim
    status.

#### GAR-USER-SURFACE-1 PySpark-like Python And SQL User Surface Completion Backstop

This bundle is the explicit completion backstop for the desired end-user shape: ShardLoom should be
as simple to enter from Python as PySpark is to Spark, while remaining honest that ShardLoom is not a
Spark API clone, Spark replacement, distributed runtime claim, production SQL/DataFrame claim, or
external-engine fallback. Completed `GAR-RUNTIME-IMPL-5B` SQL ladder evidence supplies the scoped
SQL footing; completed `GAR-RUNTIME-IMPL-5C` alignment supplies the scoped Python/DataFrame method
map; existing runtime item `GAR-RUNTIME-IMPL-5Q` owns final usability proof; completed
`GAR-RUNTIME-IMPL-5I` session/cache evidence supplies the scoped lifecycle footing. This section
keeps the user-surface parity target visible until the full import/context/session/SQL/DataFrame
path is runnable, documented, tested, and claim-safe.

- [ ] GAR-USER-SURFACE-1C DataFrame/query-builder parity for ordinary local workflows
  - Source: PySpark DataFrame usability reference, `GAR-RUNTIME-IMPL-5C`, Use Case Atlas, Python
    capability matrix, `docs/getting-started/examples.md`.
  - Current state: Python `read(path)` now infers the local source adapter from the extension over
    the same registry as explicit `read_csv(...)`, local flat JSON/JSONL/NDJSON `read_json(...)`,
    and feature-gated local flat scalar `read_parquet(...)` / `read_arrow_ipc(...)` /
    `read_avro(...)` / `read_orc(...)`
    query-builder chains support scoped projection/optional-filter/limit, preview/select-star, explicit-projection
    literal `with_column(...)`, `where(...)`, Python `sl.col(...).between(...)` and
    `sl.col(...).not_in(...)`, `head(...)`/
    `take(...)`, `count()`, scalar aggregate/optional-filter/limit with aliases, multi-key grouped
    aggregate/optional-filter/limit, and multi-key top-N plus aggregate-output top-N collect/write
    workflows. Scoped local-source joins, joined computed projection/multi-key top-N, joined
    aggregate-output top-N, local
    `write_jsonl(...)`/`write_csv(...)` sink aliases, and generated-output
    helpers also exist for scoped local workflows. Engine-native range/sequence generated sources
    now support `limit(...)`, `head(...)`, and `take(...)` bound adjustment before local writes, with
    DataFrame capability rows separating generic `write`, JSONL, and CSV evidence requirements.
    The DataFrame method matrix now marks scoped local-source `with_column(...)`, `.join(...)`,
    `.agg(...)`/`.aggregate(...)`, `.sort(...)`, scoped ranking `.window(...)`, bounded
    `.to_python_objects()`, bounded
    `.schema()`/`.describe_schema()`/`.validate_schema(...)`, and bounded
    `.data_quality_summary()`/`.data_quality_check(...)` as fixture-smoke-supported where they
    lower through ShardLoom's shared format-neutral SQL local-source runtime. Generalized joins,
    expression projection beyond admitted scoped families, broader data-quality rules,
    pandas/Arrow/NumPy materialization, richer outputs, and parity-like method coverage remain
    unsupported/report-only.
  - Next slice outcome: keep only user-surface polish that is not already owned by completed runtime
    alignment: simpler examples, concise evidence accessors, install/import ergonomics, and
    deterministic blockers for pandas/Arrow materialization, notebook display, object-store/table
    sources, and broad DataFrame parity.
  - Runtime enablement: familiar DataFrame/query-builder workflows that execute through ShardLoom
    native runtime paths for admitted local inputs and outputs.
  - User-visible surface: `ctx.read`, `ctx.read_csv`, `ctx.read_json`, `ctx.read_parquet`,
    `ctx.read_arrow_ipc`, `ctx.read_avro`, `ctx.read_orc`, `ctx.read_vortex`,
    `.select`, `.filter`, `.with_column`, `.group_by`, `.agg`, `.join`, `.sort`, `.window`,
    `.limit`, `.collect`, `.write`, `.explain`, method capability matrix.
  - Implementation scope: Python query builder, SQL/local runtime lowering, expression IR, local
    input adapters, output writers, typed unsupported reports, examples.
  - Evidence required: method family, source format, execution mode, operator family,
    materialization/decode boundary, output evidence, `fallback_attempted=false`,
    `external_engine_invoked=false`, method-level `claim_gate_status`.
  - Acceptance: each public method is either genuinely runnable for a documented subset or returns
    a deterministic unsupported report with blocker id, required evidence, and next action; no
    method silently routes to pandas/Polars/Spark/DataFusion.
  - Verification: Python query-builder tests per method, CLI/runtime smoke tests, capability matrix
    snapshots, use-case coverage, release readiness metadata.
  - Dependencies/blockers: concise evidence accessors, package/install workflow proof, and broad
    SQL/DataFrame claim gates.
  - Non-goals: no pandas/Polars backend, Spark-compatible DataFrame API promise, notebook
    production claim, full SQL optimizer parity, or performance claim.
  - Claim boundary: method-by-method scoped local runtime support only until production evidence is complete.
  - Fallback boundary: DataFrame methods must lower to ShardLoom runtime or deterministic blockers.
  - Ledger rule: ledger entry must include method support table, runnable examples, and blockers.

- [ ] GAR-USER-SURFACE-1D one-command local install, import, and first workflow proof
  - Source: `GAR-COMMERCIAL-1A`, package channel matrix, `README.md`, `docs/getting-started/*`,
    `GAR-RUNTIME-IMPL-5Q`.
  - Current state: local source-tree and editable Python usage are documented, but public package
    publication is not complete and a non-expert install/import/run path still needs final proof.
  - Next slice outcome: provide a clean local path from install to import to first
    SQL/DataFrame/generated-source workflow without reading architecture docs.
  - Runtime enablement: local install/import proof that reaches admitted runtime workflows and
    returns evidence.
  - User-visible surface: README first screen, `docs/getting-started/first-10-minutes.md`,
    Python README, website get-started/status/use-cases.
  - Implementation scope: install script/runbook, editable/local wheel proof, binary resolution,
    quickstart command, example data creation, evidence printout.
  - Evidence required: install command, uninstall/cleanup command, import success, resolved binary,
    smoke workflow output, evidence fields, unsupported-path example, `fallback_attempted=false`,
    `external_engine_invoked=false`.
  - Acceptance: a new user can complete one local runtime workflow and one unsupported diagnostic
    path in under ten minutes with exact commands.
  - Verification: clean venv smoke, Python quickstart test, README command smoke where feasible,
    package metadata checks, website readiness.
  - Dependencies/blockers: local wheel/source checkout proof, binary resolution stability, package
    channel readiness matrix, and release security gates for any public package publication.
  - Non-goals: no PyPI/TestPyPI/conda/Homebrew publication unless release gates separately pass.
  - Claim boundary: local install/import proof only until production evidence is complete.
  - Fallback boundary: install helpers must not install or invoke fallback engines.
  - Ledger rule: ledger entry must include clean-environment commands and outputs.

- [ ] GAR-USER-SURFACE-1E evidence-first result ergonomics for non-expert users
  - Source: ShardLoom evidence envelope, Python typed reports, Use Case Atlas, website Field Guide,
    benchmark claim-boundary docs.
  - Current state: runtime reports expose rich evidence fields, and Python typed reports now expose
    compact `evidence_summary`/`claim_summary` helpers for scoped SQL/generated-source surfaces.
    Scoped SQL local-source reports also expose `result_rows` and `first_result_row` helpers so
    users do not need to parse bounded inline JSONL manually. Remaining result families still need
    the same ergonomic coverage and examples.
  - Next slice outcome: make every Python runtime result expose simple row/output access plus a
    compact evidence summary and stable full evidence object.
  - Runtime enablement: user-facing evidence ergonomics for every admitted runtime workflow.
  - User-visible surface: Python result objects, CLI JSON fields, docs examples, website use-case
    recipes.
  - Implementation scope: typed report helpers, `evidence_summary`/`claim_summary` accessors,
    row/result accessors, docs snippets, use-case output examples.
  - Evidence required: output row count/path, execution mode, engine mode, source/output
    certificates, materialization/decode boundary, no-fallback fields, claim gate, unsupported
    blockers where applicable.
  - Acceptance: users can inspect rows/output and evidence without scraping JSON field maps; every
    example prints at least one result field and one evidence/claim field.
  - Verification: Python typed-report tests, generated docs/use-case checks, website readiness,
    release readiness metadata.
  - Dependencies/blockers: typed report field normalization, compact evidence summary helpers,
    generated docs examples, and stable claim-gate terminology.
  - Non-goals: no claim upgrade, performance dashboard claim, or broad SQL/DataFrame support from
    ergonomic wrappers alone.
  - Claim boundary: clearer evidence presentation only; support status still comes from runtime
    evidence gates.
  - Fallback boundary: evidence summaries must preserve `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: ledger entry must show before/after user examples and evidence accessors.

- [ ] GAR-USER-SURFACE-1F PySpark-like surface completion validator
  - Source: this `GAR-USER-SURFACE-1` bundle, `GAR-RUNTIME-IMPL-5Q`, Use Case Atlas, public
    production-readiness posture, Python capability matrix.
  - Current state: individual runtime slices can land without a single final validator answering
    whether the Python/SQL surface is simple and complete enough for production users.
  - Next slice outcome: add a completion gate that checks the import/context/session/SQL/DataFrame/
    generated-output path against the public usability target.
  - Runtime enablement: release/usability validator that blocks a PySpark-like simplicity claim
    unless every admitted path has runnable proof and every unsupported path has deterministic
    diagnostics.
  - User-visible surface: release readiness report, README/status matrix, website "Can I use this?"
    pages, Python capability matrix.
  - Implementation scope: validation script or contract test, capability/use-case cross-checks,
    example smoke matrix, website/readme claim checks.
  - Evidence required: matrix of Python entrypoints, runnable examples, blocked examples, evidence
    fields per result, claim boundaries, no-fallback/no-external-engine fields.
  - Acceptance: the validator fails if `ctx.sql`, DataFrame/query-builder, generated-output,
    session, install/import, docs, or website surfaces overclaim or lack runnable/blocked proof.
  - Verification: `python scripts/check_use_case_coverage.py`, `python scripts/check_website_readiness.py`,
    Python unit/smoke tests, release readiness metadata, `git diff --check`.
  - Dependencies/blockers: completion of the preceding `GAR-USER-SURFACE-1A` through
    `GAR-USER-SURFACE-1E` slices, use-case coverage, website readiness, and release readiness
    metadata checks.
  - Non-goals: no compatibility with Spark internals, no distributed Spark-scale claim, no package
    publication, no object-store/lakehouse/Foundry production claim.
  - Claim boundary: only after this validator passes may docs say ShardLoom has a PySpark-like
    simple Python front door for its admitted runtime scope.
  - Fallback boundary: any fallback attempt or external-engine invocation fails the completion gate.
  - Ledger rule: ledger entry must include the completion matrix and remaining non-parity gaps.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
