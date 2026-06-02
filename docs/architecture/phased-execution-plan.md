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

The earlier broad runtime rollup queues have been consolidated into the implementation-ready runtime
queues below. Current runtime sequence after PR #1031 is `GAR-RUNTIME-IMPL-6E`, then
`GAR-RUNTIME-IMPL-6F`, then the remaining `GAR-RUNTIME-IMPL-6D:last_order.*` user-surface breadth
items unless a specific 6D blocker must be pulled forward to unblock 6E/6F. The remaining 4/5-series
queue stays as internal-engine backstop work after the route/reuse/output boundary work.

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

#### GAR-RUNTIME-IMPL-6E - Automatic Dynamic Preparation Runtime Promotion

Ordering decision (2026-06-02): work this before the remaining 6D last-order breadth. Automatic
preparation/reuse is the route-level spine that lets later SQL, Python, DataFrame, benchmark, and
claim-surface work connect to the same ShardLoom runtime path instead of repeating explicit
prepare/run bookkeeping per front door.

Source: user-approved follow-through on 2026-06-01 from the novel-concepts review,
`docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
`docs/architecture/pulseweave-runtime-control.md`, `docs/architecture/dynamic-work-shaping.md`,
`docs/architecture/io-reuse-and-fanout-architecture.md`,
`docs/architecture/bayesian-performance-layout-advisor.md`,
`docs/architecture/vortex-runtime-utilization-audit.md`, Vortex Scan/I/O docs, and Database
Cracking research.

Current state:

- ShardLoom already has SourceState, VortexPreparedState, scout ingress, capillary preparation,
  layout/write advisor, copy-budget, differential-preparation, and PulseWeave evidence surfaces.
- PulseWeave is deterministic, local, certificate-gated, and already scoped to prepared/local and
  capillary preparation evidence.
- Several dynamic ideas are still only partially promoted from evidence into runtime behavior:
  prepared-state reuse is visible and has scoped manifest-backed paths, but is not yet the default
  higher-level `auto` front-door reuse spine; capillary pre-write work shaping now drives the first
  local scalar/columnar SourceState -> `vortex_ingest` route before local array build/write, while
  broader object-store/distributed/spill shaping remains gated; layout/write advice is
  report/advisory-only; differential preparation is explicit but not yet a cracking-style automatic
  refinement path for changed local sources.
- The next work must preserve automatic behavior without adding required user knobs, hidden global
  state, persistent learning, external execution fallback, or unsupported performance claims.

Runtime enablement: this section promotes the local automatic route:

```text
user expression or local prepare request
  -> UniversalIngress / InputAdapter
  -> SourceState
  -> automatic reuse / invalidation / refinement decision
  -> vortex_ingest
  -> VortexPreparedState
  -> prepared_vortex
  -> output/evidence/certificate
```

The default user posture should remain `auto`: ShardLoom chooses reuse, capillary work shaping,
layout/write admission, or differential refinement only when ProofBound/certificates admit the
decision. Otherwise it preserves the existing ShardLoom-native path or fails with deterministic
diagnostics.

Implementation checklist, in required order:

- [ ] GAR-RUNTIME-IMPL-6E-1 automatic SourceState and VortexPreparedState reuse spine.
  Source: `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/universal-input-contract.md`, and Database Cracking's reuse/refinement
  principle.
  Current state: benchmark/report rows expose SourceState, VortexPreparedState, reuse-level, cache
  invalidation, and fingerprint fields. The runtime can create scoped prepared Vortex artifacts,
  but repeated local workflows still do too much explicit preparation unless callers manually keep
  track of the prepared artifact.
  In-progress update: Python `CompatibilityPreparedVortexRoute` handles now write an explicit
  workspace manifest at `<workspace>/.shardloom/prepared-vortex-reuse-manifest.json` after a
  successful local compatibility preparation. A repeated compatible `ctx.prepare_vortex(...,
  workspace=...).run_batch(...)` or session route call can skip compatibility preparation and run
  `traditional-analytics-vortex-batch-run` over the existing local Vortex artifacts when source,
  artifact, and prepare-policy fingerprints match. Source or artifact drift invalidates the
  manifest and re-enters the normal prepare/batch route. The user-route capability report and
  local-file benchmark route report now expose the prepared-state reuse scope, manifest path,
  reuse policy, runtime hit/reason placeholders, manifest digest placeholder, and invalidation
  reason placeholder, and release readiness requires that prepared route rows keep the workspace
  manifest contract visible. Benchmark promotion now also preserves prepared-state reuse
  diagnostics in public rows and rejects reuse rows whose scope, reason, digest, or invalidation
  evidence is missing; in-process prepared-batch reuse, explicit warm-prepared reuse, and workspace
  manifest reuse are labeled as distinct scopes. Rust/CLI traditional analytics reports now emit the
  same first-class reuse fields for cold first-preparation rows
  (`prepared_state_created_not_reused`), caller-supplied warm/prepared Vortex rows
  (`explicit_prepared_state_input`), native Vortex rows (`not_applicable_native_vortex_input`), and
  single-process prepare/batch rows (`in_process_prepared_batch_vortex_artifacts`). The typed
  `compute_flow_evidence` envelope includes those reuse fields so CLI JSON consumers no longer need
  benchmark-promotion inference to determine scope, policy, hit status, digest, or invalidation
  reason. Rust/CLI `vortex-ingest-smoke` now adds a real artifact-adjacent prepared-state reuse
  path: successful local `.vortex` preparation writes a deterministic
  `.shardloom/<artifact>.prepared-state-reuse.manifest`, repeated identical local ingest emits a
  dedicated reuse report instead of rewriting the artifact, and source/artifact/plan/policy drift
  invalidates reuse before any prepared-state claim. Rust/CLI
  `traditional-analytics-prepare-batch-run` now also participates in workspace-manifest reuse
  outside the Python handle: it validates
  `<workspace>/.shardloom/prepared-vortex-reuse-manifest.json`, skips compatibility preparation on
  fingerprint/policy/artifact hits, calls the prepared Vortex batch route over existing local
  artifacts, and records workspace hit/reason/digest/invalidation evidence. Remaining 6E-1 work is
  broader local-source auto-route wiring across the higher-level `auto` front doors. Python
  `LazyFrame.prepare_vortex(...)` now covers the first high-level local-source auto front door for
  single-source CSV/JSONL/Parquet/Arrow IPC/Avro/ORC paths: the
  `ctx.read_csv(...).prepare_vortex(workspace=...)` call derives a caller-owned local `.vortex`
  target, calls the real
  `vortex-ingest-smoke` route, and exposes typed `prepared_state_reuse_hit`,
  `prepared_state_reuse_reason`, `prepared_state_reuse_manifest_digest`, and
  `prepared_state_invalidation_reason` fields from the artifact-adjacent manifest decision.
  Remaining 6E-1 work is generated-local-source preparation, benchmark/public row promotion for the
  new auto front door, and any additional CLI/Python route-report wiring needed for route-comparable
  prepared execution.
  Next slice outcome: add an automatic, evidence-safe prepared-state reuse spine for local `auto`
  workflows. Reuse must be session/workspace scoped, fingerprint-backed, and fail-closed on
  source/schema/plan/output-policy drift.
  Runtime enablement: local CSV/JSONL/Parquet/Arrow IPC/Avro/ORC or generated local rows can move
  through SourceState -> VortexPreparedState once, then reuse the prepared state for subsequent
  prepared execution when the reuse manifest is valid.
  User-visible surface: Python context/session helpers, CLI `vortex-ingest-smoke`/local-source
  runtime reports, benchmark rows, and route capability reports show `prepared_state_reuse_hit`,
  `prepared_state_reuse_reason`, `prepared_state_reuse_manifest_digest`, and invalidation reason.
  Implementation scope: add a `VortexPreparedStateReuseRequest` /
  `VortexPreparedStateReuseReport` or equivalent in `shardloom-vortex/src/vortex_ingest.rs`; add
  explicit reuse manifest fields for source path/ref, source content digest, mtime/size where safe,
  schema digest, parse/decode plan digest, selected columns, output policy, prepared artifact
  ref/digest, Vortex provider/version, feature gates, and certificate refs; wire lookup into
  `shardloom-cli/src/sql_local_source_runtime.rs` before rewriting a prepared artifact; preserve
  no hidden global cache by limiting the first slice to session-local plus explicit workspace or
  artifact-adjacent manifest; surface typed Python fields in `python/src/shardloom/client.py` /
  result models; extend benchmark artifact promotion to reject reuse claims without manifest and
  invalidation evidence.
  Evidence required: reuse-hit fixture where same source/schema/plan reuses an existing prepared
  artifact without rewriting it; reuse-miss fixture where changed source digest invalidates reuse;
  reuse-blocked fixture where schema drift or feature-gate mismatch blocks before prepared
  execution; no-fallback evidence for every reuse decision.
  Acceptance: repeated local `auto` preparation can reuse a valid VortexPreparedState
  automatically; invalidated prepared state is never reused silently; a user or agent can tell why
  reuse hit, missed, or blocked from structured fields; the existing explicit prepare path still
  works.
  Verification:
  ```bash
  cargo test -p shardloom-vortex --features vortex-write,universal-format-io vortex_ingest --lib
  cargo test -p shardloom-cli --features vortex-write,universal-format-io vortex_ingest
  python3 -m unittest python/tests/test_cli_client.py
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  git diff --check
  ```
  Non-goals: no daemon, no process-global cache, no persistent learning database, no object-store
  cache, and no performance claim.
  Dependencies/blockers: depends on SourceState/VortexPreparedState fingerprint stability,
  artifact-adjacent or workspace-scoped manifest storage, Vortex write/read certificate evidence,
  Python result model compatibility, and invalidation diagnostics for source/schema/plan drift.
  Claim boundary: may claim only scoped local prepared-state reuse with manifest/certificate
  evidence.
  Fallback boundary: reuse never invokes DuckDB, Polars, Spark, DataFusion, Velox, pandas, or a
  Vortex query-engine integration.
  Ledger rule: move completed details and validation output to
  `docs/architecture/phased-execution-completed-ledger.md`.
- [ ] GAR-RUNTIME-IMPL-6E-3 runtime-admitted layout/write advisor for one local route.
  Source: `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/skills/vortex/vortex-first-provider-check.md`, and Vortex layout/write provider
  boundaries.
  Current state: the layout/write advisor emits scoped local evidence, but current rows keep
  runtime decisions advisory/report-only. Advisor output must not silently change writer behavior
  until the selected strategy is supported by existing provider capabilities and certificate
  evidence.
  Next slice outcome: promote one narrow local layout/write decision from advisory to
  runtime-admitted and applied. The first applied decision must use only already-supported local
  writer behavior and verification depth.
  Runtime enablement: `vortex_ingest` can automatically choose an admitted local write strategy for
  a flat local SourceState, record the decision, apply it to the writer/reopen path, and expose
  whether the decision was applied or blocked.
  User-visible surface: rows expose `vortex_layout_write_advisor_runtime_decision_applied`,
  `vortex_layout_write_advisor_selected_strategy`,
  `vortex_layout_write_advisor_strategy_decision_digest`,
  `vortex_layout_write_advisor_provider_admitted`, and `vortex_layout_write_advisor_blocker`.
  Implementation scope: add a layout/write decision object in
  `shardloom-vortex/src/vortex_ingest.rs`; limit first applied strategies to current supported
  provider behavior, such as local single-artifact write, columnar SourceState preservation when
  available, safe writer defaults, and certified reopen depth; block dictionary/statistics/
  chunking/layout choices when upstream Vortex does not expose a stable admitted provider surface;
  wire the selected decision into `shardloom-cli/src/sql_local_source_runtime.rs`; keep Bayesian
  advisor confidence report-only unless a separate claim gate later fits and validates a model.
  Evidence required: applied local scalar route; applied local columnar route when
  `universal-format-io` and `vortex-write` are enabled; blocked unsupported layout strategy
  fixture; reopen/correctness evidence proving the selected strategy wrote the expected prepared
  state.
  Acceptance: one local route reports an applied layout/write decision that actually governs the
  write path; unsupported layout choices block deterministically with no fallback; advisor-applied
  status does not upgrade performance or production claims.
  Verification:
  ```bash
  cargo test -p shardloom-vortex --features vortex-write,universal-format-io layout_write_advisor --lib
  cargo test -p shardloom-cli --features vortex-write,universal-format-io vortex_ingest
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  python3 -m unittest python/tests/test_cli_client.py
  git diff --check
  ```
  Non-goals: no fitted Bayesian runtime model, no arbitrary Vortex layout rewrite, no compaction,
  no object-store write, and no performance claim.
  Dependencies/blockers: depends on Vortex provider capability checks, current local writer/reopen
  support, layout/write advisor evidence, certificate-backed reopen verification, and explicit
  blockers for unsupported dictionary/statistics/chunking/layout choices.
  Claim boundary: may claim only scoped local runtime admission of one writer strategy with
  certificate evidence.
  Fallback boundary: unsupported layout/write strategies block before execution and never delegate
  to another engine.
  Ledger rule: move completed details and validation output to the completed ledger.
- [ ] GAR-RUNTIME-IMPL-6E-4 cracking-style differential prepared-state refinement.
  Source: `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`, Database Cracking research, and the
  existing `vortex_differential_preparation_*` report surface.
  Current state: `vortex_ingest` can report append-only differential preparation and blocks
  update/delete/upsert/schema mismatch cases. The next useful step is automatic refinement: when a
  local source changes in an append-only way, ShardLoom should prepare only the delta and attach an
  overlay/refinement manifest instead of rebuilding the base prepared state.
  Next slice outcome: add a scoped automatic append-only delta refinement path for local prepared
  state reuse. The first executable consumer should be narrow: count/filter/project/limit or the
  smallest prepared benchmark-range family that can read base plus delta artifacts correctly.
  Runtime enablement:
  ```text
  existing SourceState + existing VortexPreparedState
    -> changed local source recognized as append-only delta
    -> delta SourceState
    -> delta vortex_ingest
    -> overlay/refinement manifest
    -> prepared_vortex consumer for admitted scenario family
  ```
  User-visible surface: CLI/Python/benchmark rows expose base/delta source IDs, base/delta
  prepared-state IDs, overlay manifest digest, changed ranges, refinement mode, reuse status,
  correctness digest, and deterministic blockers.
  Implementation scope: extend existing differential preparation evidence into a runtime refinement
  manifest; add automatic append-only delta detection from source fingerprint, size/row-count
  movement, schema digest, and parse-plan digest; reuse the base prepared artifact only when schema,
  source family, update mode, and certificate evidence match; add a prepared consumer for the first
  admitted overlay scenario family; add deterministic blockers for update, delete, upsert, schema
  drift, missing base manifest, changed compression/format posture, and unsupported operators.
  Evidence required: append-only delta fixture that avoids base reprepare; full-reprepare reference
  fixture used only for correctness comparison in tests; schema-mismatch fixture that blocks;
  update/delete/upsert fixtures that block; overlay consumer correctness digest parity for the
  first admitted scenario family.
  Acceptance: append-only local changes refine an existing prepared state without rebuilding the
  base; the overlay manifest is explicit, digest-backed, and invalidatable; unsupported CDC shapes
  block before prepared execution; no hidden mutation of the base prepared artifact occurs.
  Verification:
  ```bash
  cargo test -p shardloom-vortex --features vortex-write,universal-format-io differential_preparation --lib
  cargo test -p shardloom-cli --features vortex-write,universal-format-io differential
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  python3 -m unittest python/tests/test_cli_client.py
  git diff --check
  ```
  Non-goals: no broad CDC/table transaction support, no deletes/updates/upserts, no object-store
  manifests, no production incremental-processing claim, and no performance claim.
  Dependencies/blockers: depends on prepared-state reuse manifests from 6E-1, differential
  preparation report fields, append-only source-change detection, overlay manifest semantics, and a
  narrow prepared consumer that can validate base-plus-delta correctness without fallback.
  Claim boundary: may claim only scoped local append-only prepared-state refinement for the
  admitted scenario family.
  Fallback boundary: decoded/full-reprepare reference paths may be used in tests only; runtime
  refinement must remain ShardLoom-native with `fallback_attempted=false` and
  `external_engine_invoked=false`.
  Ledger rule: move completed details and validation output to the completed ledger.

#### GAR-RUNTIME-IMPL-6F - Bidirectional Format Conversion And Output Fanout Performance Promotion

Ordering decision (2026-06-02): work this immediately after 6E and before the remaining 6D
last-order breadth. Sink-driven result and fanout boundaries should exist before more broad
front-door coverage is admitted, otherwise new SQL/Python/DataFrame routes keep inheriting
row-shaped conversion and duplicated per-sink output work.

Source: user-approved follow-through on output-conversion bottlenecks;
`docs/architecture/io-reuse-and-fanout-architecture.md`,
`docs/architecture/universal-input-contract.md`,
`docs/architecture/compute-engine-flow-reference.md`, `docs/skills/translation-layer.md`,
`docs/skills/streaming-zero-copy.md`, `docs/skills/vortex/vortex-native-output.md`, and
`docs/skills/vortex/vortex-arrow-interop.md`.

Current state: local SQL/Python output and fanout paths can write JSONL, CSV, Vortex, and
feature-gated Parquet/Arrow IPC/Avro/ORC, but the compatibility-output path is still row-shaped in
key places. `SqlLocalSourceOutputFormat::render_rows` and `encode_*_output_rows` repeatedly walk
`Vec<Vec<(String, ScalarValue)>>` for each output target. Vortex output has a separate writer path,
but downstream compatibility conversion can still pay repeated scalar formatting, repeated schema
normalization, and duplicate per-sink materialization.

Runtime enablement: this section promotes the output side of the route:

```text
VortexPreparedState / ResultState
  -> ResultBatchState
  -> sink-driven OutputPlan
  -> shared fanout conversion DAG
  -> SinkFormatState
  -> SinkArtifact + replay/certificate/evidence
```

Performance objective: reduce repeated row rendering, repeated scalar conversion, duplicate fanout
work, unnecessary materialization, sink write stalls, and metadata-loss ambiguity. These items may
improve measured runtime only after benchmark evidence lands; until then they are
optimization-enabling runtime work, not public speed claims.

Implementation checklist, in required order:

- [ ] GAR-RUNTIME-IMPL-6F-1 ResultBatchState columnar output boundary.
  Source: output-conversion bottleneck review, translation-layer skill, streaming/zero-decode
  skill, and Vortex-native output skill.
  Current state: local result output is primarily row-shaped before compatibility sinks, causing
  every sink to re-walk rows and re-normalize scalar values. Vortex output remains separate and
  higher fidelity, but there is no shared columnar result boundary for all local sinks.
  Next slice outcome: add a `ResultBatchState` or equivalent internal columnar result boundary
  after execution and before output conversion. Compatibility sinks consume that boundary when
  supported; CSV/JSONL remain late text materialization targets.
  Runtime enablement: local SQL/Python/generated-output routes can produce one columnar result
  state, then write Vortex, Parquet, Arrow IPC, Avro, ORC, CSV, or JSONL from that state without
  rebuilding the logical result per sink.
  User-visible surface: evidence fields report `result_batch_state_status`,
  `result_batch_state_digest`, `result_batch_state_layout`,
  `result_batch_state_materialization_required`, `result_batch_state_decode_required`, and
  per-sink conversion timing.
  Implementation scope: `shardloom-cli/src/sql_local_source_runtime.rs`,
  `shardloom-vortex/src/universal_format_io.rs`, `shardloom-vortex/src/vortex_ingest.rs` where
  useful, Python result models, benchmark artifacts, and contract tests.
  Evidence required: one-output and fanout fixtures proving identical output digests before/after;
  row-shaped blocker evidence for unsupported schemas; no-fallback fields; per-sink timing
  attribution.
  Acceptance: at least one local flat scalar route writes multiple formats from one shared result
  boundary; Vortex remains the highest-fidelity sink; unsupported nested/wide shapes block or
  report explicitly.
  Verification:
  ```bash
  cargo test -p shardloom-cli --features vortex-write,universal-format-io sql_local_source
  cargo test -p shardloom-vortex --features vortex-write,universal-format-io universal_format_io --lib
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  python3 -m unittest python/tests/test_cli_client.py
  git diff --check
  ```
  Non-goals: no external engine writer, no Arrow-as-default execution substrate, no broad
  nested-schema claim, no performance claim.
  Dependencies/blockers: depends on stable result schema/digest semantics, existing local
  SQL/Python/generated-output result rows, Vortex and universal-format writer coverage, Python
  result model compatibility, and static artifact validators that can reject mixed row/columnar
  evidence.
  Claim boundary: may claim only a scoped local columnar output boundary with correctness
  evidence.
  Fallback boundary: `fallback_attempted=false`, `external_engine_invoked=false`.
  Ledger rule: move completed details to the completed ledger.
- [ ] GAR-RUNTIME-IMPL-6F-2 sink-driven OutputPlan materialization requirements.
  Source: `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/skills/streaming-zero-copy.md`, and the compute-flow route model.
  Current state: output planning records useful sink artifact fields, but execution does not yet
  use sink requirements early enough to avoid producing unused or prematurely materialized
  representations.
  Next slice outcome: make every local sink declare required columns, ordering, type/nullability
  support, dictionary/statistics needs, compression/encoding posture, replay depth, and
  materialization requirements before result conversion begins.
  Runtime enablement: planner can avoid building byte payloads, formatted strings, or decoded
  scalar rows that no requested sink needs.
  User-visible surface: `output_plan_materialization_required`, `output_plan_required_columns`,
  `output_plan_ordering_required`, `output_plan_statistics_required`,
  `output_plan_text_materialization_boundary`, and `output_plan_conversion_blocker`.
  Implementation scope: OutputPlan evidence structs/helpers in CLI runtime, fanout planning,
  Python fields, benchmark route rows, and static validators.
  Evidence required: fixture where Parquet/Arrow/Vortex sinks avoid text rendering; fixture where
  CSV/JSONL explicitly require late text materialization; unsupported schema diagnostics.
  Acceptance: output plans explain why materialization happened or was avoided; sinks never
  silently coerce unsupported data; route timing separates planning, conversion, write, replay, and
  evidence.
  Verification:
  ```bash
  cargo test -p shardloom-cli --features vortex-write,universal-format-io output_plan
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  python3 -m unittest python/tests/test_cli_client.py
  git diff --check
  ```
  Non-goals: no object-store/table commit planning, no Iceberg/Delta transaction semantics, no
  production sink claim.
  Dependencies/blockers: depends on ResultBatchState or equivalent output boundary, existing
  OutputPlan evidence fields, per-sink capability declarations, replay/certificate requirements,
  and deterministic blockers for unsupported sink/type/materialization combinations.
  Claim boundary: may claim only deterministic local sink materialization planning.
  Fallback boundary: compatibility export is translation, never fallback execution.
  Ledger rule: move completed details to the completed ledger.
- [ ] GAR-RUNTIME-IMPL-6F-3 shared fanout conversion DAG.
  Source: fanout architecture, output-conversion bottleneck review, and translation-layer skill.
  Current state: multi-output fanout can write several local sink artifacts, but each sink can
  still trigger independent conversion/rendering work.
  Next slice outcome: replace per-output independent rendering with a shared conversion DAG: one
  result state, one schema/type normalization pass, one optional cast/nullability pass, then
  format-specific terminal encoders.
  Runtime enablement: `prepared_vortex` or local result routes can write Vortex + Parquet + Arrow
  IPC + CSV/JSONL fanout while sharing all conversion stages that are semantically identical.
  User-visible surface: `fanout_conversion_dag_status`, `fanout_shared_stage_count`,
  `fanout_terminal_sink_count`, `fanout_shared_conversion_millis`,
  `fanout_terminal_conversion_millis`, and `fanout_duplicate_conversion_avoided`.
  Implementation scope: `prepare_sql_outputs`, `write_sql_outputs`, format encoders, sink artifact
  evidence, benchmark fanout rows, Python fanout result envelopes.
  Evidence required: fanout fixture proving shared stages are used; digest parity with old
  per-sink rendering; one blocked fixture where sinks require incompatible materialization.
  Acceptance: repeated schema/scalar normalization is done once per fanout plan where safe;
  terminal sinks still emit separate artifacts, digests, replay statuses, and certificate
  statuses.
  Verification:
  ```bash
  cargo test -p shardloom-cli --features vortex-write,universal-format-io fanout
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  python3 -m unittest python/tests/test_cli_client.py
  git diff --check
  ```
  Non-goals: no remote fanout, no table/lakehouse commits, no hidden sink batching that changes
  output semantics.
  Dependencies/blockers: depends on shared result boundary, sink-driven OutputPlan requirements,
  terminal encoder parity tests, per-sink artifact/digest/replay evidence, and compatibility
  blockers for sinks that require incompatible materialization.
  Claim boundary: may claim scoped local shared fanout conversion, not benchmark speedup.
  Fallback boundary: no DuckDB/Polars/Spark/DataFusion/Velox or Vortex query-engine integration.
  Ledger rule: move completed details to the completed ledger.
- [ ] GAR-RUNTIME-IMPL-6F-4 output capillary scheduling with PulseWeave admission.
  Source: `docs/architecture/pulseweave-runtime-control.md`, capillary I/O work, and output fanout
  bottleneck review.
  Current state: PulseWeave and capillary task evidence exist for prepared/local and
  cold-preparation surfaces, but output conversion/write/replay stages are not yet first-class
  scheduled capillary tasks.
  Next slice outcome: represent output conversion as typed tasks: schema map, columnar export,
  terminal encode, compression, local write, digest, replay, and evidence render.
  Runtime enablement: large or multi-sink local outputs can use bounded output windows controlled
  by FlowInventory, ScarcityLedger, EndoPulse, and ProofBound instead of unbounded per-sink
  conversion.
  User-visible surface: `output_capillary_status`, `output_capillary_task_roles`,
  `output_capillary_window_count`, `output_sink_pressure_status`,
  `output_memory_pressure_status`, and `pulseweave_output_policy_applied`.
  Implementation scope: PulseWeave input task shapes, CLI output writer, shardloom-vortex format
  encoders, benchmark output timing fields, Python envelope validation.
  Evidence required: small-output fixture where capillary remains below threshold; large/fanout
  fixture where output capillary scheduling applies; blocked fixture when certificates or replay
  evidence are incomplete.
  Acceptance: at least one local fanout route proves output conversion/write windows are actually
  governed by capillary scheduling; blocked policy preserves existing safe behavior or fails
  explicitly.
  Verification:
  ```bash
  cargo test -p shardloom-exec pulseweave --lib
  cargo test -p shardloom-cli --features vortex-write,universal-format-io output_capillary
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  git diff --check
  ```
  Non-goals: no distributed writer, no object-store sink, no real query-data spill, no performance
  claim.
  Dependencies/blockers: depends on output task shape definitions, PulseWeave ProofBound admission,
  FlowInventory/ScarcityLedger/EndoPulse task-window controls, result/fanout conversion evidence,
  and bounded-memory estimates for conversion/write/replay stages.
  Claim boundary: may claim only certificate-gated local output work shaping.
  Fallback boundary: PulseWeave cannot authorize external engine execution.
  Ledger rule: move completed details to the completed ledger.
- [ ] GAR-RUNTIME-IMPL-6F-5 format-aware output layout/write advisor.
  Source: Vortex-native output skill, translation-layer skill, Parquet/ORC/Arrow
  compatibility-output research, and existing layout/write advisor posture.
  Current state: cold-ingest layout/write advisor exists, but compatibility-output writers do not
  yet expose enough target-specific layout choices or metadata-preservation accounting to optimize
  downstream conversion.
  Next slice outcome: add an output-side layout/write advisor that starts advisory/report-only,
  then admits one narrow local route when provider support, correctness, replay, and benchmark
  evidence exist.
  Runtime enablement: OutputPlan can choose or report safe settings for Vortex
  chunk/layout/statistics, Parquet row groups/dictionary/statistics/compression, ORC stripe/index
  posture, Arrow IPC batch/dictionary posture, and CSV/JSONL streaming chunk size.
  User-visible surface: `output_layout_write_advisor_status`,
  `output_layout_write_advisor_selected_strategy`,
  `output_layout_write_advisor_runtime_decision_applied`, `output_metadata_preservation_map`, and
  `output_metadata_loss`.
  Implementation scope: output plan structs/evidence, Vortex writer request fields where
  supported, universal-format encoders, benchmark markdown/artifact rows, Python result surfaces.
  Evidence required: advisory rows for all local sink formats; one applied local route with
  correctness/replay proof; blocked rows for unsupported provider choices; metadata-loss reports
  for compatibility targets.
  Acceptance: advisor never silently changes output semantics; one supported local route can apply
  a write/layout choice; all other strategies remain explicit advisory or blocked statuses.
  Verification:
  ```bash
  cargo test -p shardloom-vortex --features vortex-write,universal-format-io
  cargo test -p shardloom-cli --features vortex-write,universal-format-io output_layout
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  git diff --check
  ```
  Non-goals: no fitted Bayesian runtime model, no arbitrary layout rewrite, no broad
  format-fidelity claim, no public benchmark claim.
  Dependencies/blockers: depends on sink-driven OutputPlan requirements, provider capability
  checks, Vortex/universal-format writer knobs that are actually supported, replay/correctness
  fixtures, metadata-preservation accounting, and explicit blockers for advisory-only strategies.
  Claim boundary: may claim only scoped local advisor evidence and one admitted route if
  implemented.
  Fallback boundary: unsupported layout/write choices block before execution.
  Ledger rule: move completed details to the completed ledger.

#### GAR-RUNTIME-IMPL-6D - Runtime-Ready User Surface And Benchmark-Range Completion

Ordering note (2026-06-02): this remains the user-surface and benchmark-range breadth queue, but
after PR #1031 it follows 6E/6F unless a specific 6D item blocks those route/reuse/output boundary
items. Resume the last-order checklist here after the automatic preparation/reuse and output fanout
contracts are landed, or pull a narrow 6D blocker forward with explicit justification.

Source: user runtime-go request on 2026-05-31; `docs/rfcs/0033-user-data-workflow-etl-surface.md`;
`docs/rfcs/0034-three-engine-certified-data-execution-fabric.md`;
`docs/architecture/sql-python-dataframe-front-door-parity.md`;
`docs/architecture/benchmark-suite-catalog.md`; `benchmarks/common/scenario_catalog.json`; and
`benchmarks/traditional_analytics/run.py`.

User reprioritization: the user explicitly moved runtime readiness ahead of the remaining
non-runtime closeout queue. Work this item before more docs-only closeout when it removes misleading
unsupported user-surface posture or connects an already-proven benchmark/runtime path to SQL,
Python, DataFrame, context, session, CLI, diagnostics, or docs.

Current state:

- ShardLoom has runnable local-source SQL/Python/DataFrame paths for scoped local file workflows,
  generated-output workflows, bounded decoded interop, and local Vortex primitive report paths.
- These front doors are not separate execution engines. Native `.vortex` inputs start at the
  Vortex-native boundary, while CSV/JSONL/Parquet/Arrow/Avro/ORC, generated rows, and materialized
  Python/Arrow inputs must be treated as adapters into an explicit Vortex-normalized ShardLoom
  runtime path before the route is runtime-ready or claim-grade.
- The benchmark harness exercises broader ShardLoom runtime families through
  `direct_compatibility_transient`, `compatibility_import_certified`, `prepared_vortex`,
  `native_vortex`, and `shardloom-prepare-batch` lanes.
- Benchmark publication now separates route execution, runtime support, and claim readiness.
  Public rows use route-runtime fields, route-comparable lane names, and stage-attribution tables;
  current published artifacts report `ShardLoom unsupported rows: 0` and six unsupported external
  DataFusion baseline rows.
- The first 6D child slice landed in #998. It added the deterministic
  `target/user-surface-runtime-gap-inventory.json` validator/artifact, classified 31 current
  user-surface gap rows, separated ShardLoom runtime posture from six external DataFusion baseline
  limitations, and wired the inventory into release-readiness/CI checks.
- The second 6D child slice landed in #999. It added the deterministic
  `target/user-route-capability-report.json` validator/artifact and the
  `ShardLoomContext.user_route_capability_report()` surface so users and agents can ask which
  ShardLoom route applies, where it normalizes to Vortex, what output/evidence it emits, and what
  claim boundary applies.
- The local Vortex primitive front-door slice landed in #1000. It added
  `ShardLoomContext.local_vortex_primitive_route_report()` and release-readiness validation for
  operation-level count, count-where, filter, project, select-star, filter-project, and
  source-order limit route coverage across SQL, Python, DataFrame-style, context, session, and CLI
  surfaces.
- The third 6D child slice adds `ShardLoomContext.local_file_benchmark_route_report()` and
  release-readiness validation for scenario-level local compatibility-file benchmark route coverage.
  Selective filter and filter/projection/limit are mapped to scoped direct transient routes; group,
  multi-key aggregate, join aggregate, sort/top-k, row-number window, top-N per group, dirty
  clean/cast/filter/write, partition-pruning, many-small-files, null-heavy aggregate,
  high-cardinality group/distinct, nested JSON fixture, and CDC overlay fixture rows are mapped to
  prepare-once ShardLoom routes with explicit Vortex normalization, output/evidence, no-fallback,
  and claim-boundary fields.
- The fourth 6D child slice adds the additive benchmark route timing ledger, first-class
  `ShardLoom Prepare-Once First Query` route rows, and `N=1/5/10/50/100` prepared-route
  amortization summaries. Public benchmark artifacts now publish 600 ShardLoom scoped-runtime rows
  across cold certified, prepare-once first query, prepare-once batch, warm prepared, and native
  Vortex routes. Each published row records `route_total_formula`, `route_timing_scope`,
  `stage_parent_id`, included/excluded timing stage IDs, timing-inclusion booleans, and a
  zero-delta route-total reproduction check.
- The fifth 6D child slice adds `shardloom.traditional_analytics.cold_bottleneck.v1` row fields
  and website summaries for cold certified and prepare-once routes. Cold rows now expose primary
  and secondary bottleneck stages, diagnostic-only optimization hints, source split/open/read
  pressure, projection posture, and prepared-state fingerprint/reuse fields. Warm prepared/native
  rows explicitly report cold bottlenecks as not applicable instead of inheriting cold labels.
- The sixth 6D child slice adds source/prepared route diagnostics to user-route packets and
  benchmark rows: source-state fingerprint, schema fingerprint, parse-plan ID, split-manifest ID,
  anomaly/quarantine posture, prepared-state fingerprint, nearest runnable route, required feature
  gate, and deterministic runtime blocker code. These fields are guidance and evidence pointers;
  they do not execute fallback work.
- The seventh 6D child slice makes the benchmark website route-first. The page now leads with
  route cards for cold certified, prepare-once first query, prepare-once batch, warm prepared,
  native Vortex, and external baseline end-to-end rows; stage attribution follows the cards; route
  filters separate comparable end-to-end, prepared-state steady-state, native Vortex, and
  diagnostic/stage views. Runtime, evidence, and claim badges are visually distinct and covered by
  website/static validators so warm prepared sub-ms rows cannot be mistaken for cold raw-source
  end-to-end timing.
- The eighth 6D child slice adds `shardloom.route_fast_path_attribution.v1` to promoted benchmark
  rows and website summaries. Route evidence now separates `runtime_execution_ms`,
  `output_delivery_ms`, `evidence_capture_ms`, `evidence_render_ms`, and `certificate_link_ms`;
  claim-grade ShardLoom rows must carry `evidence_required_for_claim=true`,
  `certificate_link_status=linked_certified_runtime_execution`, and a certified
  runtime-execution certificate when evidence rendering is excluded from route totals. The
  benchmark page now shows runtime/evidence attribution after stage attribution so sub-ms
  prepared/native query rows are not visually charged for publication-only evidence work.
- The ninth 6D child slice adds `shardloom.operator_mode_inventory.v1` to promoted benchmark rows
  and website summaries. Public rows now distinguish `operator_execution_mode` from
  route-runtime support and publish `encoded_native_operators`, `residual_native_operators`,
  `materialized_temporary_operators`, `operator_blocker_code`, and
  `operator_hot_path_candidate`. Current ShardLoom benchmark rows remain residual-native or
  materialized-temporary, not encoded-native; the selected first promotion candidate is
  `selective_filter_selection_vector_metric_aggregation` with deterministic blocker
  `blocked_selection_vector_metric_aggregation_not_admitted`.
- The tenth 6D child slice adds `scripts/check_benchmark_publish_doctor.py`, a static
  publication/handoff doctor that wraps benchmark artifact completeness, publication claim-gate,
  website mirror-drift, route row counts, route-runtime-status, operator-mode, timing-ledger, and
  nearest-next-command checks. It also writes compact JSON/Markdown route packets for agent
  handoff without pulling the full benchmark corpus into prompt context.
- Some user-facing capability/parity surfaces still say `unsupported`, `blocked`, or `not complete`
  where the accurate problem is front-door connection, output ergonomics, claim-grade evidence, or
  benchmark publication rather than engine impossibility.
- The user target is runtime-go: for any capability in the local benchmark range, users should have
  a clear SQL/Python/DataFrame/context/session/CLI route that runs ShardLoom, emits structured
  evidence, preserves `fallback_attempted=false`, and makes input/output boundaries obvious.

Runtime enablement: this item enables the end-to-end user route:

```text
user expression
  -> ShardLoom front door: SQL, Python, DataFrame, context/session helper, or CLI
  -> declared input: local file, local .vortex, prepared Vortex artifact, generated rows, or
     explicit materialized input snapshot
  -> input normalization: already-native Vortex, compatibility import to prepared Vortex,
     generated rows to Vortex-preparable batches, or materialized snapshot to Vortex-preparable rows
  -> ShardLoom runtime mode: direct compatibility transient, compatibility import certified,
     prepared Vortex, native Vortex, or generated-source smoke
  -> output: report rows, bounded decoded preview, local compatibility output, native Vortex
     artifact/result sink, fanout, or deterministic runtime-expansion checklist item
  -> evidence: runtime execution, Native I/O, execution certificate where available,
     materialization/decode boundary, no-fallback/no-external-engine fields
```

Next slice outcome: remove misleading unsupported posture from engine-capable benchmark-range
workflows by wiring the missing user-facing routes or reclassifying them as concrete
runtime-expansion checklist items with file/module ownership and verification. Do not weaken
claim-gates; `not_claim_grade` remains valid until benchmark/correctness/certificate evidence is
attached.

Implementation checklist, in required order:

- [x] For compatibility imports, expose an intuitive user route for
  `compatibility_import_certified -> prepared_vortex` and `shardloom-prepare-batch` so users can
  start from CSV/JSONL/Parquet/Arrow IPC/Avro/ORC, prepare once, run benchmark-range scenarios from
  prepared Vortex artifacts, and understand whether timing includes preparation. This is the primary
  non-Vortex input-to-Vortex transition and should be visible in reports instead of hidden behind a
  generic read helper. Closed by the context/session `prepare_vortex(..., workspace=...)` route
  handle and `CompatibilityPreparedVortexRoute`/`PreparedVortexQuery` tests.
- [x] For native `.vortex` input, expose a user route that runs the same native/prepared Vortex
  runtime family used by benchmark rows, not piecemeal artificial helpers. The surface must make
  source, selected execution mode, scenario/operator, memory/parallelism, and result-sink choice
  explicit. Closed by `ctx.native_vortex_route(...)` / `session.native_vortex_route(...)`,
  `NativeVortexRoute`/`NativeVortexQuery`, and CLI/client resource-policy arguments for
  `traditional-analytics-vortex-run` and `traditional-analytics-vortex-batch-run`.
- [x] For outputs, ensure every admitted benchmark-range route has at least one clear output option:
  machine-readable report, bounded preview, local compatibility output, native Vortex output,
  result-sink replay proof, or fanout. Missing output wiring is a runtime-output checklist item, not
  a vague unsupported user surface. Closed by the user-route capability output-option classifier,
  admitted-route output maps, and fail-closed route-report regression tests.
- [x] Reclassify engine-capable but unwired front-door gaps away from generic `unsupported` language
  in the Python context matrices, parity validator payload, benchmark coverage table, and docs.
  Use precise labels such as `front_door_connection_pending`, `output_route_pending`,
  `claim_evidence_pending`, or `benchmark_publication_pending`. Closed by
  `FrontDoorParityRow.runtime_gap_status`, parity/user-route/gap-inventory report fields, the
  `benchmark_publication_pending` route status, and docs that distinguish front-door gaps from
  unsupported engine paths.
- [x] Add regression tests that fail if any benchmark-range local ShardLoom route reports
  `unsupported` merely because SQL/Python/DataFrame/context/session wiring is missing. Closed by
  user-route, local-file benchmark, parity, and runtime-gap inventory fail-closed tests.
- [x] Keep claim boundaries strict: performance equivalence, production support, Spark
  displacement, object-store/table runtime, and broad arbitrary language support remain
  `not_claim_grade` until their correctness, Native I/O, execution-certificate, no-fallback, and
  benchmark evidence exists. Closed by the parity and route validators requiring
  `claim_gate_status=not_claim_grade` and false performance/production/Spark replacement claim
  flags while these rows remain pending.

Last-order runtime expansion checklist, not to be left as vague unsupported prose:

- [ ] GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar: Broad SQL grammar over
  Vortex-normalized runtime paths.
  Current state: local-source SQL supports bounded collect, selected projections/filters/aggregates,
  row-level `SELECT DISTINCT` deduplication over projection, aggregate/HAVING, join, and window
  output rows, joins, sorting, aliases, and deterministic no-fallback diagnostics for unbounded
  collect. Row-value literal `IN` / `NOT IN` predicates such as
  `(id, label) IN ((1, 'alpha'), (3, 'gamma'))` are now promoted through the same executable
  ShardLoom-owned runtime path with SQL three-valued row comparison evidence. Bounded local
  row-value `IN (SELECT ...)` / `NOT IN (SELECT ...)` predicates now materialize scoped
  multi-column local sources through the same ShardLoom-owned IN-subquery path with arity,
  null-semantics, source-format, filter/order/limit, and no-fallback evidence. Scoped top-level SQL
  `UNION` and `UNION ALL` composition is now promoted over already-admitted local-source branch
  `SELECT` plans, with matching output-column/dtype checks, fail-closed branch bounds, optional
  global `ORDER BY`, global `LIMIT`, Python/DataFrame `union(...)` / `union_all(...)` lowering,
  and no-fallback evidence fields. Scoped local `EXISTS` / `NOT EXISTS` predicates are now
  promoted as bounded two-valued presence tests over admitted local sources, with projection kind,
  source-format, filter/order/limit, row-count, result, Python helper, and no-fallback evidence
  fields. Scoped ANSI interval literals are now admitted only inside
  `DATE_ADD_DAYS`/`DATE_SUB_DAYS` and `TIMESTAMP_ADD_SECONDS`/`TIMESTAMP_SUB_SECONDS`, with
  malformed literals, unsupported units, and out-of-range values blocked before fallback. The
  remaining broad grammar blockers are explicit rows in
  `docs/status/admitted-semantics-matrix.json`: non-UTC/timezone semantics, arbitrary
  interval arithmetic outside scoped temporal helpers, locale/collation,
  complex equality/accessors/casts/nested source decoding/flat sinks outside the scoped JSONL
  `ARRAY[...]`/`STRUCT(...)` result-boundary projection route, variant/union-dtype shapes, broad
  binary source dtype decoding/ordering,
  scalar-left multi-column IN-subqueries, unbound qualified references, and remaining non-admitted
  broad ANSI subquery families.
  Scoped quantified `ANY` / `ALL` subquery
  predicates over bounded local scalar
  sources are now part of the admitted ShardLoom-owned route with SQL three-valued null-semantics,
  materialization-bound, source-format, filter/order/limit, Python helper, and no-fallback evidence.
  HAVING-level scoped local `EXISTS` and quantified `ANY` / `ALL` subqueries over aggregate output
  rows are now admitted through the same aggregate/HAVING route, with decoded-reference fixtures and
  Python query-builder lowering.
  Scoped nested local scalar `IN` subqueries are now promoted through depth-first materialization:
  inner bounded local values are materialized before the parent subquery filter executes, with
  nested predicate count, max depth, materialization order, source-format, row-count, and
  no-fallback evidence.
  Scoped joined and grouped/HAVING projected subqueries are now promoted by routing the subquery
  through the same ShardLoom local-source parser, binder, join/group/HAVING evaluator, and bounded
  materializer used by top-level SQL routes. The admitted projected cases cover scalar `IN`,
  row-value `IN`, `EXISTS`, and quantified `ANY` / `ALL` materialization where the projected output
  arity matches the membership/quantified operand; projected `EXISTS` uses the same bounded
  projected output as a two-valued presence test. Report rows expose
  `projected_subquery_runtime_execution`, statement kind, output-column count, and
  join/group/HAVING execution flags so these are visible runtime-supported routes rather than hidden
  mini-evaluators.
  Scoped correlated local-source subquery filters are now admitted when the subquery predicate uses
  the reserved `outer.<column>` alias in column-to-column comparisons. The admitted runtime family
  covers scalar `IN`, row-value `IN`, `EXISTS`, and quantified `ANY` / `ALL` predicates through
  per-outer-row bounded materialization, with correlated runtime, outer-column, evaluation-strategy,
  and no-fallback evidence fields. Python/DataFrame front doors now expose the reserved
  `sl.outer(...)` helper and typed correlated-subquery report fields for those admitted routes.
  Scoped correlated joined and grouped/HAVING projected subqueries are now admitted for scalar
  `IN`, row-value `IN`, `EXISTS`, and quantified `ANY` / `ALL` predicates when the projected
  local-source plan carries admitted `outer.<column>` column-to-column comparisons in its
  filter/HAVING path. These routes reuse the existing ShardLoom local-source parser, binder,
  join/group/HAVING evaluators, hidden HAVING aggregate rewrites, and bounded per-outer-row
  materializer, and report both correlated and projected subquery evidence.
  Source-qualified local subquery references are now admitted for the subquery's explicit `AS`
  alias or SQL-identifier file stem in selected columns, filters, and bounded ordering; Python
  helpers bind explicit aliases with `source_alias=` and render those refs with
  `sl.col("alias.column")`. Unbound source aliases, outer references outside column-to-column
  predicates, scalar-left multi-column subqueries, and remaining non-admitted broad ANSI subquery
  families remain deterministic blockers.
  Scoped UTF-8 `LIKE` predicates now admit `%` and `_` wildcard shapes plus single-character
  `ESCAPE` clauses through ShardLoom-owned predicate lowering, with deterministic blockers for
  malformed escape literals, trailing escapes, and escape misuse. Case-folding and
  locale/collation semantics remain blocked. Scoped UTF-8 `RLIKE` / `REGEXP` / `REGEXP_LIKE`
  predicates are admitted separately through ShardLoom-owned regex evaluation while locale-aware
  collation/regex semantics remain blocked.
  Scoped SQL `X'<hex>'` binary literal projections are now admitted as ShardLoom-owned binary
  scalar values with exact byte-count/hex evidence and no fallback. Scoped `BINARY '<utf8>'` and
  `BLOB '<utf8>'` text literal projections are now admitted as ShardLoom-owned binary scalar bytes.
  Scoped `CAST`/`TRY_CAST` to `binary`/`blob`/`varbinary` projections and equality/inequality
  predicates are now admitted through ShardLoom-owned scalar cast and binary comparison semantics,
  including Python/DataFrame cast aliases. Scoped
  `UNHEX(<utf8-column>)` and `FROM_BASE64(<utf8-column>)` projections are now admitted as
  ShardLoom-owned binary helper decoding over direct UTF-8 source columns, with strict invalid-input
  blockers, binary output evidence, null propagation, Python/DataFrame helpers, and no-fallback
  fields. Broad binary source dtype decoding, binary ordering, and nested binary helper expressions
  remain blocked.
  Scoped `CAST`/`TRY_CAST` to `decimal128(p,s)` / `decimal(p,s)` / `numeric(p,s)` projections and
  predicates are now admitted through ShardLoom-owned exact fixed-scale `Decimal128` scalar
  semantics, with Python/DataFrame cast aliases, decimal-specific precision/scale/mode evidence,
  exact JSONL string and CSV text output boundaries, and explicit blockers for typed decimal sink
  preservation until Parquet/Arrow/Vortex decimal encoders are admitted. Scoped same-scale
  `decimal128` add/subtract/multiply projections over decimal and integer operands are now admitted
  through the same ShardLoom-owned local-source route. Decimal division, mixed-scale decimal
  arithmetic/coercion, broad ANSI decimal coercion, exponent notation, and typed decimal sinks remain
  deterministic blockers.
  Python/DataFrame front doors now expose grouped/HAVING projected source-subquery parity for
  admitted source-backed IN, row-value IN, EXISTS, and quantified ANY/ALL helpers through explicit
  `group_by=` and `having=` clauses. These helpers lower to the same ShardLoom SQL local-source
  runtime evidence as the admitted SQL routes, including correlated `outer.<column>` HAVING
  predicates, and keep non-admitted joined or broader derived-table builder shapes outside this
  slice.
  Scoped complex projections are now admitted for `ARRAY[...]` scalar-literal arrays and
  `STRUCT(<source column>, ...)` source-column payloads over bounded local-source routes, with
  explicit JSONL/result evidence, Python `sl.array(...)` / `sl.struct(...)` lowering helpers, and
  flat-sink blockers where CSV, Parquet, Arrow IPC, Avro, ORC, and local Vortex output cannot yet
  preserve nested values. Complex equality, DISTINCT, subquery membership, accessors, casts, nested
  source decoding, and broader row/list/struct functions remain deterministic blockers. Scalar-left
  multi-column subqueries now report a deterministic invalid-shape diagnostic instead of an
  unsupported engine gap. Numeric division by zero likewise reports a deterministic runtime-error
  diagnostic instead of an unsupported arithmetic feature. The admitted-semantics matrix now
  distinguishes `unsupported_diagnostic_count=5`, `runtime_error_diagnostic_count=1`, and
  `invalid_shape_diagnostic_count=1` while preserving no-fallback evidence for all diagnostic rows.
  Scoped `decimal128` add/subtract/multiply projections are now admitted over same-scale decimal
  operands and integer operands through the generic-expression local-source runtime, exact
  JSONL/CSV text result boundary, Python/DataFrame cast-plus-arithmetic lowering, and
  admitted-semantics evidence. Decimal division, mixed-scale decimal arithmetic/coercion, broad ANSI
  decimal coercion, and typed decimal sink preservation remain deterministic blockers.
  Next slice outcome: choose the next broad SQL grammar family from the remaining runtime blockers;
  likely candidates are typed decimal sink follow-through, timezone/locale blocker refinement, broad
  binary source dtype refinement, complex access/equality follow-through after a dedicated semantics
  contract, decimal division/mixed-scale coercion after a dedicated semantics contract, or another
  front-door parity gap only after the runtime route is already admitted.
  User-visible surface: CLI SQL local-source runtime, Python `sql(...)`, DataFrame aliases,
  capability matrices, docs, and benchmark-range route reports.
  Implementation scope: `shardloom-cli/src/sql_local_source_runtime.rs`, Python query/session
  lowering, SQL/DataFrame parity validators, route capability reports, and docs.
  Evidence required: positive SQL fixtures and decoded-reference expectations for every newly
  admitted syntax family; unsupported diagnostics for still-non-admitted shapes; Python/DataFrame
  alias/lowering tests where a familiar user surface exists; parity docs; no-fallback fields; and
  claim gates for every newly admitted syntax family.
  Acceptance: admitted SQL grammar reaches an existing ShardLoom runtime route; non-admitted grammar
  fails deterministically without external engines.
  Verification: focused Rust CLI tests, Python parity tests, `scripts/check_sql_python_dataframe_parity.py`,
  `scripts/check_user_route_capability_report.py`, and `git diff --check`.
  Non-goals: no external SQL engine, no broad optimizer/performance claim, no object-store/table SQL
  runtime.
  Dependencies/blockers: parser/binder coverage, expression capability mapping, runtime operator
  evidence, and deterministic diagnostics.
  Claim boundary: scoped grammar/runtime admission only; no production SQL, performance,
  Spark-replacement, or external-fallback claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth: Full Python/DataFrame API
  breadth.
  Current state: familiar aliases lower to admitted ShardLoom runtime paths where available;
  `schema_contract(...)`, `profile(...)`, and scoped `quarantine(...)` have bounded local-source
  evidence, while broad DataFrame parity remains gated.
  Next slice outcome: promote the next coherent Python/DataFrame API family only when it lowers to
  existing ShardLoom runtime evidence or returns deterministic unsupported diagnostics.
  User-visible surface: `shardloom` Python package, session/query builders, docs, parity matrix, and
  route capability report.
  Implementation scope: `python/src/shardloom/query.py`, `context.py`, `session.py`, Python tests,
  CLI lowering where needed, and docs.
  Evidence required: Python tests proving alias/canonical equivalence, no hidden pandas/Polars
  execution, fallback/external-engine false fields, and capability rows.
  Acceptance: the new API family is intuitive from Python while still mapping to real ShardLoom
  runtime or explicit unsupported output.
  Verification: `python3 -m unittest python/tests/test_query_builder.py`,
  `python3 -m unittest python/tests/test_sql_python_dataframe_parity.py`,
  `scripts/check_python_user_surface_completion.py`, and `git diff --check`.
  Non-goals: no broad pandas/Polars backend, no production DataFrame claim, no unbounded
  materialization convenience.
  Dependencies/blockers: SQL/runtime capability coverage, output-plan support, and typed Python
  result models.
  Claim boundary: scoped Python ergonomic surface only; no performance, production DataFrame, or
  external-fallback claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime: Object-store,
  lakehouse/table, catalog, partition discovery, commit, rollback, recovery, and remote result
  delivery runtime.
  Current state: object-store URI parsing, public/no-credential fixture reads, local-emulator
  read/write smokes, and table/lakehouse boundary reports exist; live providers and table commits are
  gated.
  Next slice outcome: add the next local or credential-safe runtime promotion with explicit
  admission, commit/recovery evidence, and no-fallback diagnostics.
  User-visible surface: CLI object-store/table commands, Python helpers, capability matrices,
  docs, and release gates.
  Implementation scope: object-store runtime modules, table boundary reports, credential policy,
  output/replay validators, and docs.
  Evidence required: local or isolated provider fixture, commit/rollback/recovery proof,
  credential/effect policy fields, Native I/O evidence, and no-fallback status.
  Acceptance: one previously gated object-store/table workflow executes through ShardLoom-native
  boundaries or fails with deterministic diagnostics.
  Verification: object-store/table smoke tests, credential/effect validators, release readiness
  checks, and `git diff --check`.
  Non-goals: no live cloud write by default, no hidden credential probing, no table production claim.
  Dependencies/blockers: credential policy, commit protocol, replay verification, cleanup semantics,
  and Native I/O certificates.
  Claim boundary: scoped fixture/local runtime only; no production lakehouse/object-store,
  performance, or fallback claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime: Promote the remaining
  generated-output platform routes only after their effect boundary is real.
  Current state: generated rows can write local outputs; local-emulator object-store and
  Foundry-style dev-stack proofs exist, while live platform APIs remain gated.
  Next slice outcome: promote the next generated-output platform route only with explicit effect,
  credential, output, and replay evidence.
  User-visible surface: Python generated-output helpers, CLI generated-source commands, Foundry and
  object-store proof docs, capability rows, and release checks.
  Implementation scope: generated-source runtime, output/fanout helpers, platform boundary reports,
  validators, and docs.
  Evidence required: generated-source certificate, output artifact proof, replay/fidelity evidence,
  effect policy, and no-fallback fields.
  Acceptance: promoted generated-output route writes through an admitted ShardLoom boundary and
  reports the exact platform/effect scope.
  Verification: generated-source runtime smokes, platform proof scripts, production usability gate,
  and `git diff --check`.
  Non-goals: no real Foundry/cloud write without explicit approval, no Marketplace/package claim.
  Dependencies/blockers: effect budget, credential policy, output-plan support, and platform-specific
  boundary reports.
  Claim boundary: scoped generated-output proof only; no production platform, performance, or
  fallback claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.data_quality_quarantine_profile_runtime: Promote
  remaining data-quality observability and quarantine surfaces only where they are backed by
  ShardLoom runtime evidence.
  Current state: bounded local-source `profile(...)` and scoped `quarantine(...)` use admitted
  local-source runtime evidence; broader table/object-store remediation remains gated.
  Next slice outcome: add the next bounded data-quality check or quarantine action with ShardLoom
  runtime proof and explicit unsupported diagnostics for non-admitted checks.
  User-visible surface: Python query builder, CLI local-source smoke, output/fanout reports, docs,
  and capability matrices.
  Implementation scope: Python query API, CLI runtime fields, local sink outputs, validators, and
  docs.
  Evidence required: positive bounded checks, no-fallback fields, output/replay evidence where a sink
  is written, and report-only classification for blocked checks.
  Acceptance: users can run the promoted check without external profiling engines, and unsupported
  checks remain explicit.
  Verification: Python query tests, SQL/DataFrame parity validator, user-surface completion checker,
  and `git diff --check`.
  Non-goals: no production governance workflow, no object-store/table quarantine, no broad profiling
  claim.
  Dependencies/blockers: expression capability mapping, output-plan replay proof, and data-quality
  diagnostic vocabulary.
  Claim boundary: scoped bounded data-quality runtime only; no production governance, performance,
  or fallback claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.effectful_operations: Effectful operations: UDFs, LLM/API
  calls, embeddings, vector search, external writes, credentials, sandboxing, and deterministic
  effect budgets.
  Current state: effectful-operation admission reports and local deterministic UDF/SQLite fixture
  boundaries exist; arbitrary effects remain blocked.
  Next slice outcome: promote one effect family through explicit policy, capability, sandbox, and
  no-fallback evidence.
  User-visible surface: CLI effect/extension reports, Python helpers, docs, capability matrices, and
  security/release validators.
  Implementation scope: effect budget plan, extension/UDF boundaries, credential policy, diagnostics,
  tests, and docs.
  Evidence required: side-effect declaration, permission policy, deterministic diagnostics, sandbox
  status, no-fallback fields, and security review where needed.
  Acceptance: admitted effects are explicit and inspectable; non-admitted effects cannot execute
  silently.
  Verification: effect-budget tests, security/effect validators, relevant Python/CLI tests, and
  `git diff --check`.
  Non-goals: no hidden network/API calls, no arbitrary plugin execution, no credential discovery by
  default.
  Dependencies/blockers: sandbox policy, credential governance, extension manifests, and security
  review.
  Claim boundary: scoped effect admission only; no production UDF/LLM/vector/platform claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime: Live/hybrid runtime state, incremental
  processing, CDC beyond scoped overlay fixtures, freshness/snapshot contracts, state cleanup,
  cancellation, retry, and recovery.
  Current state: engine-selection and hybrid overlay reports exist with fixture-scoped evidence; no
  production broker/state-store or exactly-once runtime is admitted.
  Next slice outcome: promote the next bounded live/hybrid state transition with freshness,
  cancellation/retry, cleanup, and no-fallback evidence.
  User-visible surface: engine capability matrix, Python/CLI hybrid reports, docs, and release gates.
  Implementation scope: hybrid runtime reports, state cleanup/recovery logic, diagnostics, tests, and
  docs.
  Evidence required: bounded state fixture, freshness/snapshot proof, retry/cancellation evidence,
  cleanup proof, and fallback/external-engine false fields.
  Acceptance: one live/hybrid workflow has explicit runtime state and deterministic failure behavior.
  Verification: hybrid/runtime tests, capability validators, release readiness checks, and
  `git diff --check`.
  Non-goals: no broker-backed production runtime, no exactly-once claim, no object-store/table commit
  promotion.
  Dependencies/blockers: state store semantics, commit/recovery model, cleanup policy, and
  correctness fixtures.
  Claim boundary: fixture-scoped live/hybrid evidence only; no production streaming or Spark
  replacement claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime: Distributed/shuffle/spill/OOM
  production runtime, including resource governance and deterministic pre-OOM diagnostics.
  Current state: spill/OOM plans, memory declarations, and blocked diagnostics exist; real query-data
  spill and distributed execution remain gated.
  Next slice outcome: promote the next local bounded memory/spill guard that fails before process OOM
  or writes admitted ShardLoom-native spill evidence.
  User-visible surface: CLI diagnostics, benchmark rows, memory/spill reports, docs, and release
  readiness gates.
  Implementation scope: memory reservation, spill diagnostics, operator declarations, tests,
  validators, and docs.
  Evidence required: bounded-memory fixture, deterministic pre-OOM/blocker evidence, cleanup proof,
  and no-fallback fields.
  Acceptance: memory pressure is explicit and deterministic for the promoted path.
  Verification: memory/spill tests, release readiness checks, focused benchmark artifact validators
  when rows change, and `git diff --check`.
  Non-goals: no distributed runtime, no broad shuffle support, no performance claim.
  Dependencies/blockers: reservation model, spill format/persistence policy, cleanup semantics, and
  correctness parity.
  Claim boundary: scoped memory/spill safety only; no production distributed/spill or performance
  claim.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication: Claim-grade
  performance-equivalence benchmark publication across equivalent SQL, Python, and DataFrame
  workloads.
  Current state: route-first benchmark artifacts and publication validators exist, but front-door
  performance equivalence remains not claim-grade.
  Next slice outcome: publish a laptop-safe, reproducible front-door equivalence artifact only after
  SQL/Python/DataFrame route parity and benchmark safety gates are satisfied.
  User-visible surface: benchmark artifacts, website benchmark page, README/docs, Python examples,
  and release gates.
  Implementation scope: benchmark harness, promotion scripts, website data/components, docs, and
  validators.
  Evidence required: reproducible artifact, route parity, correctness digests, hardware/runtime
  context, sequential/safety controls, and no-fallback fields.
  Acceptance: published rows distinguish runtime support, evidence grade, and performance claims
  without unsupported ShardLoom gaps or external fallback.
  Verification: benchmark artifact validators, website readiness/static checks, focused benchmark
  smoke when approved, and `git diff --check`.
  Non-goals: no broad benchmark suite on an unsafe laptop path, no superiority/Spark-replacement
  claim without CG-5/CG-6 evidence.
  Dependencies/blockers: route parity, claim gates, benchmark safety redesign, current generated
  artifacts, and documentation alignment.
  Claim boundary: no performance-equivalence claim until the artifact is claim-grade and published
  through approved gates.

User-visible surface: `shardloom` Python package (`context`, `session`, `sql`, `read_*`,
`read_vortex`, output helpers), ShardLoom CLI Vortex/local-source/runtime commands, benchmark
coverage rows, front-door parity matrix, docs, and examples.

Implementation scope: `python/src/shardloom/query.py`, `python/src/shardloom/client.py`,
`python/src/shardloom/context.py`, `python/src/shardloom/session.py`, `python/tests/*`,
`shardloom-cli/src/*`, `shardloom-vortex/src/*`, `benchmarks/traditional_analytics/run.py`,
`benchmarks/common/scenario_catalog.json`, `scripts/check_sql_python_dataframe_parity.py`,
`scripts/check_release_readiness.py`, `docs/architecture/sql-python-dataframe-front-door-parity.md`,
`docs/architecture/benchmark-suite-catalog.md`, examples, and README/quickstart docs as touched by
the changed route.

Evidence required: focused Python tests for each user route, Rust CLI/Vortex tests when command
behavior changes, parity validator output, release-readiness validator output when statuses move,
runtime envelope/no-fallback evidence, Native I/O and execution certificate fields where available,
and benchmark-harness coverage-table validation for any benchmark-range claim.

Acceptance:

- Every local benchmark-range ShardLoom capability has a documented user route and deterministic
  evidence output.
- Every non-Vortex input route names the adapter-to-Vortex normalization/preparation boundary before
  it is treated as runtime-ready.
- No user-facing surface calls an engine-capable benchmark-range path `unsupported` merely because
  the front door or output route was missing.
- True runtime-expansion items appear only in the last-order checklist above or in more detailed
  child items derived from it.
- `fallback_attempted=false` and `external_engine_invoked=false` remain explicit for ShardLoom
  runtime rows.
- Performance and production claims remain blocked unless the required evidence is attached.

Verification:

```bash
python3 scripts/check_user_surface_runtime_gap_inventory.py --output target/user-surface-runtime-gap-inventory.json
python3 scripts/check_sql_python_dataframe_parity.py --output target/sql-python-dataframe-parity-gate.json
python3 -m unittest python/tests/test_query_builder.py python/tests/test_cli_client.py python/tests/test_sql_python_dataframe_parity.py
python -m compileall -q python/src python/tests scripts examples
cargo fmt --all -- --check
cargo test -p shardloom-cli vortex_
cargo test -p shardloom-vortex local_primitive --features vortex-local-primitives
cargo test --workspace --all-targets
git diff --check
```

Non-goals: do not add Spark/DataFusion/DuckDB/Polars/Velox fallback; do not claim broad arbitrary
language support before the checklist is closed; do not publish packages/releases; do not run broad
benchmarks unless the current slice explicitly needs benchmark evidence and uses the laptop-safe
sequential controls.

Claim boundary: this item can claim runtime-ready user paths only for explicitly wired
benchmark-range workflows with passing validation. It cannot claim broad SQL/Python/DataFrame
flexibility, object-store/table production readiness, live/hybrid production readiness, or
performance equivalence until the last-order checklist items are implemented and validated.

Fallback boundary: every admitted route must report `fallback_attempted=false` and
`external_engine_invoked=false`; external engines remain benchmark baselines or test oracles only.

Ledger rule: when a child slice is completed and merged, move the completed details to
`docs/architecture/phased-execution-completed-ledger.md`, then keep only remaining unchecked work
here.

#### GAR-RUNTIME-IMPL-4 - Final Full-Runtime Implementation Leaf Queue
Current runtime ordering note (updated 2026-06-02): the completed engine-internal queue remains
recorded here as backstop context, but the active runtime sequence above now intentionally works 6E,
then 6F, then the remaining 6D user-surface breadth before this residual 4/5-series queue unless a
specific internal-engine item blocks the active route/reuse/output work. The
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
The remaining internal-engine follow-ups below are residual backstops after the active
route/reuse/output and user-surface breadth sequence, not the current top runtime priority.
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
    invocation. Follow-up freshness passes closed stale `GAR-PERF-2C`, `GAR-SCALE-1`,
    `GAR-COMPAT-1`, `GAR-NOVEL-1`, and scoped `GAR-GEN-1` generated-output rows against
    already-landed scan-pushdown, scale-readiness, compatibility-scoreboard, evidence-native
    report-lane, and DataFrame literal projection/generated-with-column generated-output evidence.
    The Python/runtime user-surface freshness pass added explicit context/client helpers for the
    already-admitted local object-store, table metadata/append, and SQLite fixture smokes, closing
    the stale ergonomic API row while keeping broad runtime/package claims in their owning rows.
    The extension/UDF context-surface pass closed the duplicate plugin/UDF sandbox row by exposing
    the existing non-executing extension inspection and built-in deterministic scalar UDF fixture
    helpers through the high-level context while leaving arbitrary plugin/UDF/effect execution in
    the owning modular-extensibility gates. The repo-wide readiness/user-surface audit baseline
    found no benchmark blockers, 38 global architecture review blockers, one active phase-plan
    blocker, 194 registered CLI commands, 40 executable commands, 12 feature-gated commands,
    8 diagnostic-only commands, 134 report-only commands, 99 public `ShardLoomClient` methods,
    73 public `ShardLoomContext` methods, two stale completed-ledger PR references, and one
    concrete CLI discovery ergonomics bug around standard `--help` aliases. Full compute-engine
    completion remains blocked by 38 unchecked global architecture review items plus the phase-plan
    follow-through queue below.
  - Next slice outcome: close or split the 38 global architecture review items into runtime-ready
    evidence slices, and graduate the user-surface matrix so every report-only/feature-gated
    surface has a deliberate high-level, low-level, diagnostic, or blocked posture.
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

- [x] GAR-RUNTIME-IMPL-6B repo-wide readiness and user-surface audit baseline
  - Source: active user objective, `docs/architecture/repo-readiness-user-surface-audit.md`,
    `shardloom-cli/src/command_registry.rs`, Python client/context method inventories, and
    `scripts/check_compute_engine_completion_gate.py`.
  - Completed state: the audit establishes that the repo is not ready for a full "no gaps"
    completion claim, classifies remaining blocker families as true runtime gaps versus stale
    cleanup, records the command/Python user-surface inventory, and fixes the first concrete
    ergonomics defect by making `shardloom --help`, `shardloom -h`, and
    `shardloom <command> --help` route through the registry-backed help surface.
  - Cleanup state: stale completed-ledger `pending` PR references for
    `codex/gar-perf-2c-review-freshness` and `codex/gar-completed-lane-review-freshness` are
    replaced with #983 and #984.
  - Claim boundary: this is an audit and user-surface cleanup slice, not evidence that the engine is
    complete, package-ready, production-ready, or free of true runtime blockers.
  - Evidence required: standard help aliases pass through the real CLI, command registry/status
    surfaces expose the aliases, completion-gate counts remain explicit, and stale ledger references
    no longer use `pending`.
  - Acceptance: focused CLI tests cover the help aliases, command registry docs/tests cover the
    alias metadata, and release/readiness validators continue to report the remaining blockers
    explicitly.

- [ ] GAR-RUNTIME-IMPL-6C user-surface graduation matrix and ergonomic runtime promotion
  - Source: `docs/architecture/repo-readiness-user-surface-audit.md`,
    `shardloom-cli/src/command_registry.rs`, `python/src/shardloom/client.py`,
    `python/src/shardloom/context.py`, `python/README.md`, and current use-case/website surfaces.
  - Current state: the repo exposes broad CLI and Python surfaces, but only part of that surface is
    ergonomic high-level user workflow API; many report-only, feature-gated, and effectful rows are
    intentionally present but not yet separated into a single source-of-truth graduation matrix.
    The first 6C artifact is the SQL/Python/DataFrame front-door parity matrix, which makes scoped
    local parity versus broad language/runtime/performance gaps explicit.
  - Next slice outcome: every registered CLI command family and Python user workflow is assigned one
    of `high_level_context`, `client_only`, `diagnostic_only`, `feature_gated`, or
    `not_user_facing`, with deterministic criteria for promotion and no implied runtime support.
  - User-visible surface: `shardloom help`, `command-metadata`, Python `ShardLoomClient`,
    `ShardLoomContext`, README examples, use-case index entries, and website readiness narratives.
  - Implementation scope: add the graduation matrix, wire a validator for CLI/Python/doc posture,
    and promote only surfaces with real ShardLoom CLI/runtime evidence into high-level context
    helpers; keep report-only or unsafe/effectful families diagnostic-only until evidence lands.
  - Evidence required: matrix artifact, Python tests for promoted helpers, docs/examples for
    admitted user workflows, and no-fallback/external-engine fields preserved.
  - Acceptance: a validator fails if an executable or feature-gated user-facing command lacks a
    deliberate Python/context posture or if docs imply support beyond the matrix.
  - Verification:
    ```bash
    python3 scripts/check_sql_python_dataframe_parity.py --output target/sql-python-dataframe-parity-gate.json
    python3 scripts/check_use_case_index.py
    python3 scripts/check_website_readiness.py
    cargo test -p shardloom-cli --all-targets
    cargo test -p shardloom-contract-tests --test release_readiness_metadata
    ```
  - Non-goals: no promotion of report-only planners to runtime execution, no hidden external engine
    delegation, no package publication, and no broad performance or production-readiness claim.
  - Dependencies/blockers: depends on the 6B audit inventory, current command registry metadata,
    Python client/context inventories, and stable no-fallback diagnostics for unsupported surfaces.
  - Claim boundary: graduation means the user surface is deliberately classified and validated; it
    does not mean all classified surfaces are supported runtime capabilities.
  - Fallback boundary: every promoted surface must preserve explicit no-fallback and
    external-engine-not-invoked evidence where execution or certification is involved.

- [ ] GAR-RUNTIME-IMPL-6D true runtime gap family burn-down plan
  - Source: the 38 unchecked global architecture review rows and the runtime gap families listed in
    `docs/architecture/repo-readiness-user-surface-audit.md`.
  - Current state: the global review still has 38 unchecked rows; the completion gate is explicitly
    blocked because broad architectural items have not all been converted into runtime evidence,
    deterministic unsupported diagnostics, or reclassified out-of-scope surfaces with validators.
  - Next slice outcome: split each broad global blocker into family-owned runtime implementation
    slices with acceptance criteria for supported behavior, deterministic blockers, validators,
    docs/website parity, and benchmark/release evidence when relevant.
  - User-visible surface: capability discovery, diagnostics, Python/context workflows, CLI runtime
    commands, release/readiness gates, benchmark/readiness docs, and website/use-case claims.
  - Implementation scope: prioritize SQL/DataFrame runtime breadth, Vortex source/sink/operator
    coverage, object-store/lakehouse execution, table/catalog commits, streaming/spill/retry
    runtime, and package/deploy readiness according to current claim risk and user value.
  - Evidence required: each split item names the owning module, public surface, no-fallback
    invariant, validator, and completion-gate field it is expected to reduce.
  - Acceptance: global review blocker count decreases only when a family has implementation,
    deterministic admission evidence, or a documented reclassification with validator coverage.
  - Verification:
    ```bash
    python3 scripts/check_compute_engine_completion_gate.py --allow-incomplete --output target/compute-engine-completion-gate.json
    python3 scripts/check_release_architecture_tracker.py --allow-blocked
    python3 scripts/final_release_rehearsal.py --allow-blocked
    cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
    cargo test -p shardloom-contract-tests --test release_readiness_metadata
    ```
  - Non-goals: no unsupported work hidden as supported, no superiority/performance claims without
    CG-5/CG-6 evidence, no fallback engines, no release publication, and no bundling every runtime
    family into one oversized implementation PR.
  - Dependencies/blockers: depends on the 6B audit, the 6C graduation matrix, the active global
    architecture review inventory, and family-specific RFC/skill routing before runtime promotion.
  - Claim boundary: blocker burn-down is claimable only for families with concrete implementation
    evidence or validated unsupported diagnostics; placeholder artifacts do not satisfy runtime
    support.
  - Fallback boundary: runtime gap closure must keep ShardLoom execution native and explicit; DuckDB,
    Polars, Spark, DataFusion, Velox, and Vortex query-engine integrations remain comparison or
    external-boundary surfaces only, never fallback execution.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
