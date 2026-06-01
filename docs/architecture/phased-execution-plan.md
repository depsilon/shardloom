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

#### GAR-RUNTIME-IMPL-6D - Runtime-Ready User Surface And Benchmark-Range Completion

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

- [ ] For local file input, confirm SQL/Python/DataFrame routes cover the local benchmark scenario
  families with a clear user path: selective filter, filter/projection/limit, group aggregate,
  multi-key aggregate, join aggregate, sort/top-k, row-number window, top-N per group, dirty
  clean/cast/filter/write, partition-pruning fixture, many-small-files fixture, null-heavy
  aggregate, high-cardinality string group/distinct, nested JSON field scan, and CDC overlay where
  the benchmark lane executes ShardLoom runtime evidence. Each route must identify whether it is
  still transient compatibility execution or has crossed into prepared/native Vortex execution.
- [ ] For compatibility imports, expose an intuitive user route for
  `compatibility_import_certified -> prepared_vortex` and `shardloom-prepare-batch` so users can
  start from CSV/JSONL/Parquet/Arrow IPC/Avro/ORC, prepare once, run benchmark-range scenarios from
  prepared Vortex artifacts, and understand whether timing includes preparation. This is the primary
  non-Vortex input-to-Vortex transition and should be visible in reports instead of hidden behind a
  generic read helper.
- [ ] For native `.vortex` input, expose a user route that runs the same native/prepared Vortex
  runtime family used by benchmark rows, not piecemeal artificial helpers. The surface must make
  source, selected execution mode, scenario/operator, memory/parallelism, and result-sink choice
  explicit.
- [ ] For outputs, ensure every admitted benchmark-range route has at least one clear output option:
  machine-readable report, bounded preview, local compatibility output, native Vortex output,
  result-sink replay proof, or fanout. Missing output wiring is a runtime-output checklist item, not
  a vague unsupported user surface.
- [ ] Reclassify engine-capable but unwired front-door gaps away from generic `unsupported` language
  in the Python context matrices, parity validator payload, benchmark coverage table, and docs.
  Use precise labels such as `front_door_connection_pending`, `output_route_pending`,
  `claim_evidence_pending`, or `benchmark_publication_pending`.
- [ ] Add regression tests that fail if any benchmark-range local ShardLoom route reports
  `unsupported` merely because SQL/Python/DataFrame/context/session wiring is missing.
- [ ] Keep claim boundaries strict: performance equivalence, production support, Spark
  displacement, object-store/table runtime, and broad arbitrary language support remain
  `not_claim_grade` until their correctness, Native I/O, execution-certificate, no-fallback, and
  benchmark evidence exists.

Last-order runtime expansion checklist, not to be left as vague unsupported prose:

- [ ] Broad SQL grammar over Vortex-normalized runtime paths: arbitrary `ORDER BY`, grouped
  aggregates, joins, windows, expressions, casts, functions, subqueries, aliases, and planner/binder
  coverage routed to ShardLoom-native execution instead of external engines.
- [ ] Full Python/DataFrame API breadth: expression registry parity, computed columns, multi-stage
  pipelines, joins, aggregations, windows, sorting, UDF-safe policy, and predictable method aliases
  across supported input/output families.
- [ ] Object-store, lakehouse/table, catalog, partition discovery, commit, rollback, recovery, and
  remote result delivery runtime.
- [ ] Effectful operations: UDFs, LLM/API calls, embeddings, vector search, external writes,
  credentials, sandboxing, and deterministic effect budgets.
- [ ] Live/hybrid runtime state, incremental processing, CDC beyond scoped overlay fixtures,
  freshness/snapshot contracts, state cleanup, cancellation, retry, and recovery.
- [ ] Distributed/shuffle/spill/OOM production runtime, including resource governance and
  deterministic pre-OOM diagnostics.
- [ ] Claim-grade performance-equivalence benchmark publication across equivalent SQL, Python, and
  DataFrame workloads, including reproducibility floors and laptop-safe sequential execution.

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
