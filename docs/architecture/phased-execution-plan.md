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
Current runtime ordering note (2026-05-25): prioritize engine-internal completion first. The
`GAR-RUNTIME-IMPL-4I` scan/pushdown matrix, `GAR-RUNTIME-IMPL-4K` runtime-envelope validator
rollout, `GAR-RUNTIME-IMPL-4L/5I` scoped session/cache lifecycle,
`GAR-RUNTIME-IMPL-5F` prepared/native Vortex lifecycle, the `GAR-RUNTIME-IMPL-4F/4F1/5D`
local adapter/ingest parity closeout, `GAR-RUNTIME-IMPL-4P/5M` declared local scale runtime
closeout, `GAR-RUNTIME-IMPL-5H` runtime evidence/claim validator closeout, and
`GAR-RUNTIME-IMPL-5R` PulseWeave automatic prepared/local runtime control are complete and recorded
in the ledger. Continue through object-store/control-plane/effectful-operation gates (`5K`,
`4Q`/`5N`, `4R`/`5O`), then expression/operator closeout (`4D`/`5G`) as the last 4-series
runtime-family closeout before SQL/Python surface backstops, benchmark and Foundry gates, and
release usability. Completed queue blocks have moved to
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

- [ ] GAR-RUNTIME-IMPL-5K object-store read runtime admission
  - Source: `GAR-RUNTIME-IMPL-4N`, `GAR-COMPAT-1C`, `GAR-SCALE-1E`,
    `docs/architecture/object-store-request-planner.md`.
  - Current state: `GAR-RUNTIME-IMPL-4N` admits `object-store-read-smoke` for an explicit
    `local-emulator` local fixture profile with SourceState and Native I/O evidence. Public
    no-credential object-store reads, authenticated reads, credential policy, network policy,
    listing, local cache, and real provider proofs remain blocked.
  - Next slice outcome: extend beyond the local-emulator fixture into provider URI parse,
    effect/credential policy, optional listing, byte-range/full-file read, local cache boundary,
    and SourceState evidence for an approved public no-credential fixture profile.
  - Runtime enablement: provider/profile-scoped object-store read runtime with credential/network
    admission and no-default-effect policy.
  - User-visible surface: CLI/Python object-store diagnostics, capability/status pages, use cases.
  - Implementation scope: provider abstraction, policy gate, credential redaction, request planner,
    byte-range adapter, cache boundary, emulator/public-fixture tests.
  - Evidence required: provider/profile, credential/network status, object version/ETag, byte
    ranges, SourceState id, Native I/O certificate, no-fallback fields.
  - Acceptance: public and authenticated read gates are separate; no network probe or credential
    resolution runs by default; unsupported providers fail closed.
  - Verification: policy tests, mocked/emulator read smoke, SourceState snapshot tests, release
    readiness, website status checks.
  - Non-goals: no object-store write, table commit, production object-store claim, or managed
    platform claim.
  - Claim boundary: provider/profile-specific read proof only.
  - Fallback boundary: storage provider access does not authorize external query execution.
  - Dependencies/blockers: security/effect policy, provider test harness, dependency/license review,
    emulator or public no-credential fixture.
  - Ledger rule: ledger entry must record provider, credential posture, proof refs, and blocked
    providers.

- [ ] GAR-RUNTIME-IMPL-4Q live, hybrid, loopback control-plane, and distributed blockers
  - Source: RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - Runtime enablement: engine-mode admission and loopback-only runtime controls, plus fail-closed
    distributed blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane proof only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, and distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-5N live, hybrid, control-plane, and distributed-runtime promotion
  - Source: `GAR-RUNTIME-IMPL-4Q`, RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - Runtime enablement: admitted local live/hybrid/control-plane runtime plus distributed execution
    blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote-worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane proof only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-4R adapters, databases, UDFs, extensions, and effectful operations
  - Source: RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, UDFs, plugins, LLM/API/embedding/vector
    effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - Runtime enablement: scoped local adapter/UDF execution or inspection with effectful/networked
    paths blocked by runtime policy.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, and dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-5O adapters, databases, UDFs, extensions, and effectful operations
  - Source: `GAR-RUNTIME-IMPL-4R`, RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, wrappers/connectors, UDFs, plugins,
    LLM/API/embedding/vector effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - Runtime enablement: scoped adapter/UDF runtime or safe inspection, with all effectful external
    paths denied by default.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-4D expression, cast, null, string, date, and timestamp runtime families
  - Source: RFC 0021, SQL/Python local runtime smokes, expression/operator semantics,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: scoped SQL/Python local-source expression coverage has moved well past the first
    predicate/projection leaves; detailed completed 4D slices live in
    `docs/architecture/phased-execution-completed-ledger.md`. Scoped local-source computed
    projections now also admit `SELECT *` plus computed/literal projection outputs, so Python
    `read_csv(...)`, flat `read_json(...)`, and feature-gated flat scalar structured readers can
    lower `with_column(...).filter(...).sort(...).limit(...)` without requiring an explicit
    `select(...)`; computed-projection top-N now sorts projected rows by computed aliases and can
    still sort by source columns when the source column is not projected. Scoped scalar/grouped
    aggregate `HAVING` now evaluates admitted predicates over emitted aggregate output rows for
    local-source SQL/Python and join-aggregate paths. Scoped local-source aggregate HAVING also
    admits unprojected `COUNT(*)`, `COUNT(column)`, `COUNT(DISTINCT column)`, `SUM`, `AVG`, `MIN`,
    and `MAX` aggregate functions as hidden HAVING-only evaluation columns, strips those columns
    from user output, and keeps unsupported aggregate shapes or non-output source-column references
    deterministically blocked. Scoped UTF-8 string predicates/projections now also admit composed
    expression trees across `LOWER` / `UPPER` / `TRIM`, `CONCAT`, `SUBSTR` / `SUBSTRING`,
    `LEFT` / `RIGHT`, `REPLACE`, and `LENGTH` for local-source SQL/Python paths while preserving
    deterministic blockers for source-free or unsupported string expression shapes.
    The remaining work is the parity gap
    around broader non-UTF-8 non-numeric expression families, broader coercion/function coverage,
    broader HAVING expression trees, interval/date-time and timezone-database semantics,
    correlated/multi-column/nested subquery semantics, arbitrary predicate-tree completeness beyond
    the currently admitted leaves, and final SQL/Python ergonomics. Unsupported residual work must
    continue to fail with deterministic no-fallback diagnostics.
  - Closeout posture: this parent item remains open for the residual parity gaps above.
    A future closeout PR must either implement those gaps or split each non-goal into separate
    follow-on runtime items before marking `GAR-RUNTIME-IMPL-4D` complete.
  - Next slice outcome: add one implementation PR per remaining expression family: remaining
    non-numeric expression/function families, richer IN semantics only where evidence-backed,
    timestamp/timezone helpers, interval/date-time completeness where admitted, and broader typed
    coercions/functions.
  - Runtime enablement: executable ShardLoom-native expression families or deterministic runtime
    blockers for unsupported operators.
  - User-visible surface: SQL/Python query builder, explain output, capability matrix, docs.
  - Implementation scope: expression IR, type coercion policy, null semantics, parser lowering,
    native evaluators, diagnostics.
  - Vortex 0.71/0.72 opportunity mapping:
    - Pluggable struct cast informs ShardLoom-native cast/coercion admission only after local
      correctness tests and output evidence exist.
    - Variant array and `VariantGet` inform nested/semi-structured expression blockers and later
      scoped runtime support.
    - `DType::Union` must remain explicit unsupported/runtime-blocked until union semantics,
      nullability, schema reporting, and output evidence are implemented.
    - Statistic expression support can inform metadata-first expression planning, but cannot become
      a correctness or performance claim by itself.
  - Evidence required: expression family, input/output dtype, null policy, cast status, decoded/
    materialized flags, correctness digest, no-fallback fields.
  - Acceptance: every admitted expression has fixture coverage and unsupported expressions report a
    deterministic diagnostic.
  - Verification: expression unit tests, SQL/Python smoke tests, unsupported snapshots, release
    readiness metadata.
  - Non-goals: no arbitrary UDFs, regex parity, timezone completeness, or ANSI SQL claim.
  - Claim boundary: expression-family support per admitted dtype/operator.
  - Fallback boundary: expression evaluation must remain ShardLoom-native.
  - Dependencies/blockers: expression IR stability, dtype coercion policy, decoded-reference
    fixtures, and SQL/Python lowering.
  - Ledger rule: ledger entry must enumerate expression families, dtypes, and blockers.

- [ ] GAR-RUNTIME-IMPL-5G physical operator, function, and encoded-kernel coverage
  - Source: `GAR-RUNTIME-IMPL-4D`, `GAR-RUNTIME-IMPL-4J`, RFC 0015, RFC 0016, RFC 0021.
  - Current state: selected residual-native operators exist; broad type/null/string/date/decimal,
    join/window/top-k, fused, and encoded-kernel coverage remains incomplete. Initial encoded
    registry pairs now execute for scoped bitpacked, sequence, constant, and dictionary Vortex
    reader inputs, but this is still pair-level runtime evidence rather than broad operator/function
    coverage. Scoped local-source
    ranking, offset, and distribution window projections now cover `ROW_NUMBER()`, `RANK()`,
    `DENSE_RANK()`, `LAG()`, `LEAD()`, `NTILE()`, `PERCENT_RANK()`, and `CUME_DIST()` with native
    partition/order evaluation, peer-group tie semantics, offset lookups, bucket assignment, and
    cumulative/percent rank evidence, but general window value functions, frames, encoded window
    kernels, and
    distributed/object-store window execution remain open. Scoped
    `COUNT(DISTINCT column)` is runtime-admitted for local scalar and grouped aggregate rows with
    `distinct_aggregate_*` evidence, SQL `NULL`-ignoring distinct-count semantics, Python
    `sl.count_distinct(...)` aggregate lowering, deterministic blockers for unsupported
    `DISTINCT` aggregate shapes such as `SUM(DISTINCT ...)` or `COUNT(DISTINCT *)`, and no external
    fallback.
  - Next slice outcome: promote operator families one at a time with decoded-reference correctness,
    unsupported diagnostics, and encoded-kernel admission where available.
  - Runtime enablement: ShardLoom-native operator/function execution coverage with deterministic
    blockers for unsupported families.
  - User-visible surface: CLI/Python/SQL/DataFrame workflows, benchmark rows, capability matrix.
  - Implementation scope: expression IR, scalar/aggregate operators, join/window/top-k operators,
    type coercion, null/string/date policy, encoded kernel registry, blockers.
  - Evidence required: operator/function family, input/output schema, type/null policy, encoding id,
    decoded/materialized flags, correctness digest, encoded-native claim flag, no-fallback fields.
  - Acceptance: each supported operator family has success tests, edge-case tests, unsupported
    diagnostics, and correctness evidence; unsupported encodings block deterministically.
  - Verification: unit/property/correctness tests, fixture manifest checks, encoded-kernel tests,
    benchmark smoke per family.
  - Non-goals: no arbitrary UDFs, ANSI parity, blanket encoded-native claim, or performance claim.
  - Claim boundary: operator/function/encoding-pair support only.
  - Fallback boundary: external engines may be test oracles only, never runtime evaluators.
  - Dependencies/blockers: semantic fixture corpus, expression registry, benchmark row schema,
    decoded-reference harness.
  - Ledger rule: ledger entry must list promoted families, type/null behavior, and blockers.

- [ ] GAR-RUNTIME-IMPL-5B SQL frontend runtime ladder
  - Source: `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4C`, `GAR-RUNTIME-IMPL-4D`, RFC 0032.
  - Current state: scoped local CSV/flat JSONL SQL smoke paths exist for
    projection/optional-filter/limit, preview/select-star, scalar and grouped aggregates with
    optional filters and output aliases, multi-key scalar top-N over projection rows, aggregate
    output aliases, and group keys, explicit single- or multi-key inner equi-join,
    left/right/full outer equi-join, left semi/anti equi-join, cross join, scoped
    column-comparison and generic numeric-expression ON joins, scoped UTF-8 string
    predicate/projection expression trees, scoped computed join projections, multi-key scalar
    joined top-N, and scalar/grouped join-aggregate ordering by aggregate output aliases or group
    keys. Scalar/grouped aggregate and join-aggregate rows also admit scoped
    post-aggregate `HAVING` predicates bound to emitted aggregate output aliases, selected group
    keys, or admitted unprojected aggregate functions evaluated as hidden HAVING-only columns.
    Scoped local-source `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`,
    `NTILE()`, `PERCENT_RANK()`, and `CUME_DIST()` window projections now execute through the same
    format-neutral SQL runtime with deterministic partitioned ranking, offset semantics,
    distribution semantics, peer-group tie evidence, and typed report evidence;
    richer expressions, casts, dates, broader string semantics, broad window functions/frames,
    subqueries, catalogs, arbitrary join predicates, null/collation ordering, and broad planner
    behavior remain incomplete or blocked.
  - Next slice outcome: implement a staged SQL ladder that admits only supported syntax families
    and emits stable blockers for unsupported syntax.
  - Runtime enablement: ShardLoom-native SQL execution for admitted syntax families plus stable
    runtime blockers for unsupported SQL.
  - User-visible surface: CLI SQL command, SQL explain/capability output, docs/use-cases, website
    status.
  - Implementation scope: parser/binder/planner admission, local logical plan lowering, expression
    type/null policy, join/order/aggregate blockers, explain snapshots, tests.
  - Evidence required: parser/binder/planner flags, admitted syntax family, before/after plan
    digests, source/output refs, correctness digest, unsupported diagnostic code, no-fallback
    fields, claim gate.
  - Acceptance: each admitted SQL shape executes through ShardLoom-native code only; every
    unsupported SQL construct fails closed with actionable diagnostics.
  - Verification: SQL parser/binder unit tests, CLI smoke per admitted family, unsupported
    diagnostic snapshots, release readiness metadata, benchmark harness where applicable.
  - Non-goals: no ANSI SQL parity, catalog runtime, production SQL claim, or external SQL engine.
  - Claim boundary: syntax-family scoped local SQL runtime only.
  - Fallback boundary: DataFusion, DuckDB, Spark, SQLite, Polars, pandas, and Vortex query-engine
    integrations are prohibited as execution backends.
  - Dependencies/blockers: operator semantics, local adapter registry, output writers, execution
    envelope validators.
  - Ledger rule: ledger entry must enumerate admitted SQL grammar families and blocked families.

- [ ] GAR-RUNTIME-IMPL-5C Python DataFrame and query-builder workflow parity
  - Source: `GAR-RUNTIME-IMPL-4A`, `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4E`, Python README,
    Use Case Atlas.
  - Current state: Python wrapper and selected query-builder methods exist. The local CSV/flat
    JSONL query builder now covers projection/filter/limit, preview, scalar aggregate, multi-key
    group-by, multi-key scalar top-N, aggregate-output top-N, scoped local-source equi/cross and
    expression-condition joins, computed projections and multi-key scalar top-N over joined rows, scalar/grouped join
    aggregate, post-aggregate `having(...)` / post-`agg(...)` `filter(...)` over aggregate output
    aliases or admitted unprojected aggregate functions, scoped
    `.window(sl.row_number(...), sl.rank(...), sl.dense_rank(...))` projections,
    composed UTF-8 string helper chains for predicates/projections, explicit-projection literal
    `with_column(...)`, and `count()` workflows, but
    complete end-to-end generated/local/Vortex workflows and
    unsupported-method diagnostics are not yet ordinary user-grade coverage.
  - Next slice outcome: make one import path support generated, local file, and prepared/native
    Vortex workflows with select/filter/project/limit/preview/aggregate/group/order/write where
    admitted.
  - Runtime enablement: ordinary Python context/query-builder workflows that invoke ShardLoom
    runtime instead of external Python engines.
  - User-visible surface: `import shardloom`, context/session object, `LazyFrame`, typed reports,
    getting-started docs, recipes, website use cases.
  - Implementation scope: Python builders, method admission matrix, CLI lowering, typed report
    accessors, examples, packaging smoke.
  - Evidence required: method admission, execution mode, engine mode, source/generated/prepared refs,
    output refs, correctness digest, certificate refs, no-fallback fields, claim gate.
  - Acceptance: a non-expert can run documented Python workflows and inspect evidence without
    architecture docs; unsupported methods are explicit and actionable.
  - Verification: Python unit/integration tests, clean-venv smoke, example smoke, compileall,
    use-case coverage, website readiness.
  - Non-goals: no pandas/Polars backend, notebook production claim, broad DataFrame parity claim, or
    public package upload.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: Python orchestrates ShardLoom runtime and must not compute through external
    engines.
  - Dependencies/blockers: CLI runtime coverage, typed execution envelope, local outputs, generated
    source builders, Vortex lifecycle.
  - Ledger rule: ledger entry must include runnable Python snippets, admitted methods, and blocked
    methods.

- [ ] GAR-RUNTIME-IMPL-5J benchmark publishing, profile, and claim-grade refresh gate
  - Source: `GAR-RUNTIME-IMPL-4M`, `GAR-BENCH-PUB-1`, benchmark publishing runbook.
  - Current state: benchmark publishing has a structured artifact model, but every runtime
    promotion still needs fresh, profile-scoped evidence and public website/docs rendering. The
    current public benchmark artifact is `full_local` and therefore shows CSV/Parquet comparative
    rows without Spark profile rows; the website must keep `spark-default` and
    `spark-local-tuned` visible as `full_local_plus_spark` lanes even when the current artifact did
    not request them. The benchmark registry and release gate now require `shardloom-prepare-batch`
    for full local published profiles, and the current promoted `full_local` artifact includes the
    ShardLoom cold route, prepared route, single-process prepare/batch route, native Vortex route,
    and local comparison baselines across CSV/Parquet required scenarios. Current promoted rows
    still include ShardLoom `blocked`, `fixture_smoke_only`, and external `external_baseline_only`
    rows, and the main artifact lacks broad-format JSONL/Arrow IPC/Avro/ORC comparative coverage.
    Benchmark pages must also pull current support and claim-boundary context from generated
    status/evidence data instead of carrying their own explanatory copy.
  - Next slice outcome: require a current benchmark/correctness/evidence artifact for every
    promoted runtime path and block stale or incomplete public claims. The next public comparative
    refresh should run or explicitly gate `full_local_plus_spark`, include Spark lane availability,
    publish broad-format coverage for CSV, Parquet, JSONL, Arrow IPC, Avro, and ORC, and move the
    main ShardLoom comparative roster toward `claim_grade` rows only for admitted runtime paths.
  - Runtime enablement: runtime-claim publishing validator that keeps public support status tied to
    fresh evidence.
  - User-visible surface: website benchmarks, docs/benchmarks, status page, release readiness.
  - Implementation scope: artifact freshness checker, profile matrix, runtime claim matrix,
    benchmark page ingestion from canonical generated artifacts, release validators, Spark/JVM
    profile publishing checks, format coverage checks, and claim-gate closeout diagnostics.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, correctness
    refs, certificate refs, no-fallback fields, claim gate, Spark lane availability, format
    coverage, and source-state/prepared-state coverage.
  - Acceptance: promoted paths are not presented publicly without current evidence; missing
    required lanes/scenarios/formats are visible and block claim-grade status; Spark lanes are visible in
    artifact lane availability; broad formats are visible as available or missing; prepared/native
    source-state coverage is rendered from batch evidence instead of a misleading scalar count; the
    raw comparative roster renders all promoted rows, not a sample; the main ShardLoom comparative
    roster has no `blocked`, `unsupported`, `not_claim_grade`, or `fixture_smoke_only` rows before
    any broad claim-grade benchmark publication, while external lanes remain `external_baseline_only`
    and never satisfy ShardLoom evidence; the benchmark page reuses the runs-today support matrix
    for support posture and the promoted benchmark bundle for timing/coverage context.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests, `full_local_plus_spark` preflight/runbook evidence.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: workload-scoped local benchmark evidence only.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: benchmark manifest schema, runtime envelope validators, scenario
    fixtures, website renderer support.
  - Ledger rule: ledger entry must include artifact refs, profile, freshness, and public claim
    status.

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

#### GAR-USER-SURFACE-1 PySpark-like Python And SQL User Surface Completion Backstop

This bundle is the explicit completion backstop for the desired end-user shape: ShardLoom should be
as simple to enter from Python as PySpark is to Spark, while remaining honest that ShardLoom is not a
Spark API clone, Spark replacement, distributed runtime claim, production SQL/DataFrame claim, or
external-engine fallback. Existing runtime items (`GAR-RUNTIME-IMPL-5B`, `GAR-RUNTIME-IMPL-5C`,
and `GAR-RUNTIME-IMPL-5Q`) own much of the remaining implementation; completed
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
  - Next slice outcome: promote DataFrame-style methods in user-value order with either runnable
    runtime or deterministic blockers: pandas/Arrow materialization boundaries, broader
    data-quality rules, broader expression projection, richer output writers, and collect/write
    ergonomics.
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
  - Dependencies/blockers: method-level runtime lowerings, expression IR completion, output writer
    support, local join/runtime expansion, and broad SQL/DataFrame claim gates.
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
