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

Current autonomous execution order:

1. Work the benchmark timing and cold-route performance split queue before returning to the
   6-series runtime breadth queue. Completed hotpath implementation, freshness, rerun, and
   publication slices now live in `docs/architecture/phased-execution-completed-ledger.md`. The
   current promoted engine route geomeans are shardloom 177.25 ms, shardloom-vortex 7.47 ms,
   shardloom-prepared-vortex 7.87 ms, and shardloom-prepare-batch 11.43 ms. The artifact has
   `performance_claim_allowed=false`; benchmark rows remain evidence and optimization direction,
   not superiority or Spark-replacement claims.
2. Preserve end-to-end route totals as the primary comparison surface. Stage grids are attribution
   aids only, so future stage-level claims require exclusive timing fields, an inclusive
   compatibility view, and an auditable residual before superiority wording moves.
3. Return to the residual `GAR-RUNTIME-IMPL-4/6A` completion gate only after the benchmark split
   queue and active 6D breadth queue have reduced or explicitly blocked the runtime families they
   own.

Remaining work snapshot:

| Order | Work item | Remaining outcome |
| --- | --- | --- |
| 1 | `PERF-SPLIT-3` | Defer prepared/native source-state family construction until the first consuming scenario. |
| 2 | `PERF-SPLIT-4` | Add projection-aware CSV/JSONL scout and typed decode so cold lanes avoid unused-column work. |
| 3 | `PERF-SPLIT-5` | Reuse/coalesce local Vortex writer context for cold result/preparation writes with explicit proof. |
| 4 | `PERF-SPLIT-6` | Make prepared-state identity content-addressed and role-repairable. |
| 5 | `PERF-SPLIT-7` | Refresh benchmark artifacts only after split counters are actionable and claim gates remain safe. |
| 6 | `PERF-SPLIT-8` | Add compact evidence and tiered result-sink modes for hot prepared/native benchmark lanes. |
| 7 | `PERF-SPLIT-9` | Admit scoped append-only delta overlays without full prepared-state rebuild. |
| 8 | `6D:last_order.broad_sql_grammar` | Promote the next admitted SQL grammar family or add deterministic unsupported diagnostics. |
| 9 | `6D:last_order.python_dataframe_api_breadth` | Promote the next Python/DataFrame alias family that lowers to admitted ShardLoom runtime evidence. |
| 10 | `6D:last_order.object_store_lakehouse_runtime` | Promote the next credential-safe object-store/table fixture or keep it explicitly gated. |
| 11 | `6D:last_order.generated_output_platform_runtime` | Promote the next generated-output platform route only with effect, credential, output, and replay evidence. |
| 12 | `6D:last_order.data_quality_quarantine_profile_runtime` | Add the next bounded data-quality/profile/quarantine runtime proof. |
| 13 | `6D:last_order.effectful_operations` | Admit one effect family through explicit policy, capability, sandbox, and no-fallback evidence. |
| 14 | `6D:last_order.live_hybrid_runtime` | Promote one bounded live/hybrid state transition with freshness, retry/cancellation, and cleanup proof. |
| 15 | `6D:last_order.distributed_spill_oom_runtime` | Add the next deterministic memory/spill/OOM guard or admitted spill proof. |
| 16 | `6D:last_order.front_door_performance_benchmark_publication` | Publish claim-grade front-door equivalence evidence only after route parity and benchmark safety gates pass. |
| Backstop | `GAR-RUNTIME-IMPL-4/6A` | Burn down residual compute-engine completion blockers after the active 6D queue. |

Closed 6E, 6F, 6C, 6D, and related runtime-control burn-down details are recorded in
`docs/architecture/phased-execution-completed-ledger.md`; they are not active Planned work.
Required traceability labels retained for release/readiness tests:
`GAR-RUNTIME-IMPL-6E` automatic dynamic preparation;
`GAR-RUNTIME-IMPL-6F` output/fanout conversion;
`GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout;
`GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`;
`GAR-RUNTIME-IMPL-4D-F2` complex dtype; and
`GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery. Keep their detailed status in the completed
ledger unless a new blocker is promoted into an unchecked item here.

Read order: the runtime implementation queue appears first below. Cross-cutting global-review,
P0, and non-runtime closeout context follows the active runtime queues so the next autonomous
session starts with the first unchecked runtime item instead of drifting into deferred cleanup.

Runtime queue items must explicitly enable an end-user runtime path, a runtime admission/blocker
that protects user-visible behavior, or a validator that gates runtime claims. Docs-only or
report-only work cannot complete a runtime item unless the item is explicitly a runtime-safety
blocker.

Live plan hygiene:

- Planned must keep actionable parent work unchecked until the parent is closed. Compact checked
  status checklist rows may appear under an active parent only as pointers to existing ledger,
  status, matrix, or validator evidence.
- Completed session blocks, implementation narratives, and historical proof detail belong only in
  `docs/architecture/phased-execution-completed-ledger.md`.
- If a completed parent item is found in Planned, remove it from this file after confirming the
  matching ledger entry exists or adding that ledger entry.
- Do not leave a completed parent section in Planned just to preserve history. Keep only active
  child work, compact status checklist pointers, or a short pointer to the ledger when history is
  needed.
- Do not let docs-only, report-only, or claim-copy cleanup interrupt the runtime sequence above
  unless it is a release, safety, security, or claim-integrity blocker for the next runtime item.
- A runtime item is valid only when it has a `Runtime enablement:` field that names the runnable
  path, admission/blocker, or validator it enables. If that field cannot be made concrete, the item
  belongs in non-runtime planning or the completed ledger, not the runtime queue.

#### Runtime Implementation Queue - Runtime-Enabling Work Only

The earlier broad runtime rollup queues have been consolidated into the implementation-ready runtime
queues below. After the 6E automatic preparation/reuse closeout, 6F output/fanout closeout, 6C
user-surface graduation closeout, and 6D gap-family burn-down closeout, the current runtime
sequence is the benchmark timing and cold-route performance split queue followed by the remaining
`GAR-RUNTIME-IMPL-6D:last_order.*` user-surface breadth. Pull a 6D breadth item forward only after
the `PERF-SPLIT-*` queue is complete or explicitly blocked. The remaining 4/5-series queue stays as
internal-engine backstop work after the route/reuse/output boundary work.

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

#### Benchmark Timing And Cold-Route Performance Split Queue

This queue is the active benchmark-driven optimization lane and must be worked in numeric order
before the 6-series runtime breadth queue resumes. Completed HOTPATH/HOTPATH-14 history remains in
the completed ledger; the items below are the remaining implementation slices needed to make route
timings actionable and then reduce the ShardLoom hot/cold overheads they expose. Benchmark reruns
belong only at `PERF-SPLIT-7` or after later code-bearing split items have landed, and any rerun
must preserve no-fallback evidence and claim gates.

- [ ] PERF-SPLIT-3 lazy source-state family construction for prepared/native sessions:
  - Source: `TraditionalVortexBatchSourceState::from_paths` and
    `TraditionalPreparedNativeSession::open` in `shardloom-vortex/src/traditional_analytics.rs`.
  - Current state:
    - [x] Per-batch source-state family reuse evidence exists for dimension, category/metric,
      grouped, ranked, filter, dirty, and date/null families.
    - [ ] Remaining: session open still has to prove that unused state families are not eagerly
      built for batches that never consume them.
  - Runtime enablement: prepared/native batch session -> lazy family accessor -> first consumer
    builds state -> later consumers reuse state with evidence.
  - Objective: avoid eagerly building reusable state during session open when the scenario batch may
    not consume it.
  - Implementation scope: replace eager `from_path` state-family creation with lazy, memoized
    family accessors. Keep consumer-count evidence, but build each family only on first use. Emit
    `source_state_family_build_micros`, `source_state_family_reuse_hit`, and
    `source_state_family_recompute_avoided` per family.
  - User-visible surface: prepared/native benchmark timing fields and route attribution page.
  - Evidence required: per-family build/reuse counters, timing splits, unchanged scenario result
    digests, and no-fallback fields.
  - Acceptance: session open time excludes deferred family work; first consumer pays the family
    build; later consumers reuse it. Existing correctness and evidence fields remain stable or
    receive compatibility aliases.
  - Verification:
    ```powershell
    cargo test -p shardloom-vortex traditional_vortex_batch_source_state
    cargo test -p shardloom-vortex traditional_prepared_native_session
    git diff --check
    ```
  - Non-goals: no semantic changes to scenario results; no encoded-native claim from this
    plumbing.
  - Claim boundary: runtime work-avoidance evidence only; no performance claim until rerun.
  - Fallback boundary: no external engine may build or cache source-state families for ShardLoom
    rows.
  - Ledger rule: after merge, move completed details and validator evidence to the completed ledger.

- [ ] PERF-SPLIT-4 projection-aware CSV/JSONL scout and typed column decode:
  - Source: `read_text_source_with_scout`, text provider batch readers, and scalar row readers in
    `shardloom-vortex/src/traditional_analytics.rs`.
  - Current state:
    - [x] Text-source scout and malformed/nested/dirty posture evidence exists.
    - [ ] Remaining: supported CSV/JSONL scenarios need proof that unused columns are skipped
      instead of being parsed into row structs.
  - Runtime enablement: local text source -> scout projected/filter masks -> typed column builders
    for admitted primitive fields -> explicit unsupported/quarantine path for dirty/nested shapes.
  - Objective: reduce cold parse/decode by avoiding full UTF-8/string/row materialization for
    columns not needed by predicates, outputs, or certification.
  - Implementation scope: add a scout pass that records delimiter/header/schema/nullability/anomaly
    posture, projected field mask, filter-only field mask, and unsupported-shape diagnostics.
    Implement CSV and JSONL typed column builders for admitted primitive fields, feeding
    Arrow/Vortex-compatible buffers directly. Use structural-index/SIMD-inspired parsing only behind
    an approved dependency/provenance gate; otherwise keep a Rust-native scalar parser with the same
    evidence contract. Fall back only to explicit ShardLoom row materialization when required by
    unsupported shape, diagnostics, or full-fidelity output.
  - User-visible surface: cold benchmark rows, source-read scout attribution, dirty/nested source
    diagnostics, and website benchmark page.
  - Evidence required: bytes read/decoded, columns decoded/skipped, projected decode status,
    malformed/quarantine posture, row-materialization status, and no-fallback fields.
  - Acceptance: for supported CSV/JSONL scenarios, unused columns are not parsed into row structs.
    Unsupported nested or dirty shapes fail or route to explicit quarantine/output boundaries.
  - Verification:
    ```powershell
    cargo test -p shardloom-vortex traditional_fact_text_vortex_provider
    cargo test -p shardloom-vortex traditional_jsonl
    cargo test -p shardloom-vortex traditional_dirty_csv
    git diff --check
    ```
  - Non-goals: no silent row dropping, no external engine parser fallback, no broad JSON engine
    claim.
  - Claim boundary: scoped local text-source decode optimization only; no broad SQL/DataFrame or
    performance claim until benchmark evidence exists.
  - Fallback boundary: no pandas/Polars/DuckDB/DataFusion/Spark parser may execute ShardLoom text
    decoding as fallback.
  - Ledger rule: after merge, move completed details and validator evidence to the completed ledger.

- [ ] PERF-SPLIT-5 Capillary Vortex writer runtime/session reuse and write coalescing:
  - Source: `write_vortex_array` in `shardloom-vortex/src/vortex_ingest.rs` and cold fact/dim/CDC
    writes in `shardloom-vortex/src/traditional_analytics.rs`.
  - Current state:
    - [x] Vortex capillary preparation evidence and local task-window reports exist.
    - [ ] Remaining: writer runtime/session reuse and write coalescing must be tied to real write
      timing fields and replay proof before it can affect cold-route optimization.
  - Runtime enablement: local Vortex write request -> bounded Capillary write context -> per-artifact
    or coalesced write/reopen/replay evidence -> no-fallback certificate fields.
  - Objective: reduce Vortex write cost by turning separate artifact writes into a bounded,
    evidence-bearing Capillary write plan with shared Vortex runtime/session and explicit stage
    timers.
  - Implementation scope: introduce a local `CapillaryVortexWriteContext` that owns the Vortex
    runtime, writer session, buffer pools, dtype/layout write policy, and workspace-safe producer
    validation. Coalesce tiny fact/dim/CDC writes where Vortex layout policy allows it, or run
    independent writes concurrently under bounded memory/sink pressure. Emit
    `vortex_array_build_micros`, `vortex_writer_open_micros`,
    `vortex_segment_write_micros`, `workspace_stage_micros`, `digest_micros`,
    `reopen_footer_micros`, and `replay_scan_micros`.
  - User-visible surface: cold benchmark rows, Vortex preparation evidence, result-sink capillary
    page, and Native I/O certificates.
  - Evidence required: per-artifact digests, paths, bytes, row counts, writer context reuse fields,
    Native I/O certificates, replay proof, and no-fallback fields.
  - Acceptance: cold route preserves per-artifact digests, paths, bytes, row counts, no-fallback
    evidence, and Native I/O certificates. Writer context reuse is visible in evidence. Small writes
    are batched or scheduled without weakening workspace-safe validation.
  - Verification:
    ```powershell
    cargo test -p shardloom-vortex vortex_ingest
    cargo test -p shardloom-vortex traditional_analytics_vortex_write
    cargo test -p shardloom-core workspace_safe_local_write
    git diff --check
    ```
  - Non-goals: no bypass of Vortex writer APIs, no unsafe output path handling, no hidden fast mode.
  - Claim boundary: writer-stage optimization evidence only; no performance claim until rerun and
    claim gates pass.
  - Fallback boundary: no external writer, query engine, or object-store service may perform hidden
    ShardLoom writes.
  - Ledger rule: after merge, move completed details and validator evidence to the completed ledger.

- [ ] PERF-SPLIT-6 content-addressed prepared-state index and role-scoped partial repair:
  - Source: prepared-batch reuse logic in `shardloom-vortex/src/traditional_analytics.rs` and
    Python prepared route manifests in `python/src/shardloom/prepared_route.py`.
  - Current state:
    - [x] Workspace-manifest reuse and dependency/invalidation fields exist.
    - [x] Partial-repair guard fields exist.
    - [ ] Remaining: content-addressed prepared-state identity and role-scoped partial repair need
      admitted repair behavior rather than report-only guard posture.
  - Runtime enablement: prepared-state index lookup -> changed-role detection -> admitted role
    repair or deterministic full invalidation -> prepared/native execution with no-fallback fields.
  - Objective: reduce first-query prepared lookup/create cost by making prepared-state identity
    content-addressed and repairable at fact/dim/CDC role granularity.
  - Implementation scope: create a prepared-state index keyed by source-admission packet digest,
    schema hash, route family, layout policy, Native I/O status, artifact refs/digests, and prepare
    policy. On cache miss, emit the exact changed role. Add partial repair for admitted changes:
    unchanged roles reuse existing Vortex artifacts; changed role is re-prepared; dependent derived
    state is invalidated deterministically. Add delta/overlay hooks for append-only fact changes
    where existing prefix verification succeeds.
  - User-visible surface: prepare-once first-query and prepare-batch benchmark lanes, Python
    prepared route manifests, reuse diagnostics, and website attribution.
  - Evidence required: reused roles, repaired roles, invalidated derived states, repair timing,
    replay proof, index digest, and `external_engine_invoked=false`.
  - Acceptance: cache misses no longer force full rebuild when only one role changed and partial
    repair is certified. Manifest fields show reused roles, repaired roles, invalidated derived
    states, repair timing, replay proof, and `external_engine_invoked=false`.
  - Verification:
    ```powershell
    cargo test -p shardloom-vortex traditional_prepared_batch_workspace_reuse
    cargo test -p shardloom-vortex traditional_prepared_partial_repair
    $env:PYTHONPATH='python/src'; python -m unittest python.tests.test_cli_client.ShardLoomClientTests.test_context_prepared_route_reuses_workspace_manifest_without_reprepare
    git diff --check
    ```
  - Non-goals: no broad CDC/table-transaction claim, no stale artifact reuse, no
    DataFusion/Spark/etc. fallback.
  - Claim boundary: scoped prepared-state repair only; no benchmark/performance claim until rerun.
  - Fallback boundary: no external engine may repair, validate, or execute prepared-state misses.
  - Ledger rule: after merge, move completed details and validator evidence to the completed ledger.

- [ ] PERF-SPLIT-7 benchmark refresh and optimization-readiness gate:
  - Source: benchmark harness, website benchmark artifact, publication claim validators, and the
    completed `PERF-SPLIT-1` through `PERF-SPLIT-6` code slices.
  - Current state:
    - [x] The latest promoted benchmark artifact is route-first and explicitly not claim-grade.
    - [ ] Remaining: rerun only after split counters are actionable and the code changes that can
      affect benchmark data have landed.
  - Runtime enablement: benchmark harness -> refreshed artifact -> publication validators -> website
    route/stage attribution page with no-fallback evidence.
  - Objective: prove the split timings are actionable before making any speedup claim.
  - Implementation scope: refresh local benchmark artifacts after the split counters land; add route
    cards for cold certified, prepare-once first query, prepare-once batch, warm prepared query, and
    native Vortex query. Each route must expose included route total, excluded shared setup, cold
    preparation, query execution, output, evidence, and harness overhead separately.
  - User-visible surface: benchmark manifest, website benchmark page, local taxonomy report, and
    publication claim gate.
  - Evidence required: reproducible artifact, correctness digests, hardware/runtime context, route
    timing inclusion fields, no-fallback fields, and claim-gate output.
  - Acceptance: benchmark page can answer "what should be optimized next?" without reading code.
    Claim gates remain blocked unless correctness, reproducibility, hardware/runtime context, and
    benchmark methodology are complete.
  - Verification:
    ```powershell
    python benchmarks\traditional_analytics\run.py --preset local-taxonomy --iterations 3
    python scripts\promote_benchmark_artifact.py
    python scripts\check_benchmark_artifact_completeness.py --manifest website\assets\benchmarks\latest\manifest.json
    python scripts\check_benchmark_publication_claim_gate.py --manifest website\assets\benchmarks\latest\manifest.json
    git diff --check
    ```
  - Non-goals: no superiority claim, no leaderboard claim, no Spark replacement claim, no
    claim-grade route until validators say the evidence supports it.
  - Claim boundary: benchmark refresh and optimization direction only unless claim validators pass.
  - Fallback boundary: ShardLoom rows must execute through ShardLoom runtime; external engines are
    baselines only.
  - Ledger rule: after merge, move completed details, artifact refs, and validator evidence to the
    completed ledger.

- [ ] PERF-SPLIT-8 compact evidence and tiered result-sink modes for hot benchmark lanes:
  - Source: `shardloom-cli/src/benchmark_runtime.rs`,
    `shardloom-vortex/src/traditional_analytics.rs`, `benchmarks/traditional_analytics/run.py`,
    `TraditionalRuntimeEvidenceLevel`, and
    `docs/architecture/runtime-evidence-level-tiering.md`.
  - Current state:
    - [x] `minimal_runtime`, `certified`, and `full_replay` evidence levels exist for scoped
      prepared/native batch rows.
    - [x] Existing benchmark artifacts show evidence rendering as a dominant hot-lane attribution
      target.
    - [ ] Remaining: hot lanes need explicit evidence/sink tiers, separate render/envelope/report
      timers, and skip-reason fields that distinguish non-publication rows from claim-grade rows.
  - Runtime enablement: prepared/native benchmark lane -> requested evidence tier and sink tier ->
    compact JSON-only evidence or replay/certificate proof based on claim/publication posture.
  - Objective: prevent result-sink replay and human evidence rendering from dominating hot
    warm/prepared/native benchmark lanes while preserving full claim-grade evidence when requested.
  - Implementation scope: add explicit evidence/sink tiers: `runtime_minimal` for compact
    machine-readable timing, `metadata_sink` for digest/count/path proof without replay,
    `full_vortex_replay` for write/readback/certificate proof, and `publication_full` for human
    text plus full field rendering. Split current evidence/render/sink timing into
    `human_evidence_render_micros`, `json_envelope_emit_micros`,
    `report_fields_build_micros`, `result_sink_plan_micros`,
    `result_sink_write_micros`, and `result_sink_replay_micros`.
  - User-visible surface: CLI benchmark JSON, benchmark harness rows, website benchmark page, and
    publication validators.
  - Evidence required: requested evidence tier, actual evidence tier, sink tier,
    `sink_timing_included_in_route_total`, replay/human-render skip reasons, claim-grade proof
    enforcement, and no-fallback fields.
  - Acceptance: every row records requested evidence tier, actual evidence tier, sink tier, whether
    sink timing is included in route total, and why any replay/human render work was skipped.
    Claim-grade rows cannot skip required proof. Hot local rows can avoid full replay/human
    rendering only when marked non-publication or non-claim-grade.
  - Verification:
    ```powershell
    cargo test -p shardloom-cli benchmark_runtime
    cargo test -p shardloom-vortex traditional_runtime_evidence_level
    python benchmarks\traditional_analytics\run.py --preset local-taxonomy --iterations 3
    python scripts\check_benchmark_artifact_completeness.py --manifest website\assets\benchmarks\latest\manifest.json
    python scripts\check_benchmark_publication_claim_gate.py --manifest website\assets\benchmarks\latest\manifest.json
    git diff --check
    ```
  - Non-goals: no weakening of publication evidence, no hidden benchmark-only shortcut, no
    claim-grade route without replay/certificate evidence when that evidence is required.
  - Claim boundary: compact hot-lane rows are non-publication/non-claim-grade unless full required
    proof is present and validators pass.
  - Fallback boundary: evidence tiering cannot invoke or hide external-engine execution.
  - Ledger rule: after merge, move completed details, artifact refs, and validator evidence to the
    completed ledger.

- [ ] PERF-SPLIT-9 append-only delta overlay prepared-state path:
  - Source: prepared-batch reuse manifests, `TraditionalPreparedNativeSession`, cold
    `vortex_ingest` preparation paths, and the completed differential-preparation ledger entry.
  - Current state:
    - [x] Scoped append-only differential-preparation evidence exists in the `vortex_ingest` route.
    - [x] Update/delete/upsert/schema-drift blockers emit deterministic no-fallback diagnostics.
    - [ ] Remaining: prepared/native consumers need the prepared-state overlay path tied into
      reuse/admission so local CSV/JSONL append-only changes can avoid full rebuild when safe.
  - Runtime enablement: local append-only source change -> prefix digest proof -> base prepared
    artifact reuse plus delta artifact/refinement manifest -> scoped prepared/native consumer.
  - Objective: avoid full prepared-state rebuild when a local CSV/JSONL fact source only appended
    bytes/rows and the existing prepared artifact remains a valid prefix.
  - Implementation scope: detect append-only source changes by size/mtime plus prefix digest proof;
    write a delta source-state artifact and delta Vortex artifact; attach a refinement manifest
    linking base prepared-state digest, delta digest, changed byte range, row range, schema hash, and
    route family. Admit only scoped consumers that can combine base row count/state plus delta
    overlay without rewriting base artifacts.
  - User-visible surface: prepare-once first-query and prepare-batch rows, Python prepared route
    manifests, differential preparation evidence, and benchmark attribution.
  - Evidence required: `delta_overlay_admitted=true`, base artifact reused, delta artifact written,
    replay/correctness digest, invalidation reasons, no-fallback fields, and update/delete/schema
    blockers.
  - Acceptance: append-only changes report `delta_overlay_admitted=true`, base artifact reused,
    delta artifact written, replay/correctness digest matched, and unsupported update/delete/schema
    changes fail closed with deterministic invalidation reasons.
  - Verification:
    ```powershell
    cargo test -p shardloom-vortex traditional_prepared_delta_overlay
    cargo test -p shardloom-vortex traditional_prepared_batch_workspace_reuse
    git diff --check
    ```
  - Non-goals: no broad CDC/table transaction claim, no in-place mutation of prepared artifacts, no
    stale base artifact reuse.
  - Claim boundary: scoped local append-only prepared-state overlay only; no broad CDC, table,
    object-store, production, or performance claim until benchmark/release gates pass.
  - Fallback boundary: no external engine may detect, merge, repair, or execute delta overlays for
    ShardLoom rows.
  - Ledger rule: after merge, move completed details, artifact refs, and validator evidence to the
    completed ledger.

#### 6-Series Runtime Breadth Queue

The 6-series queue resumes only after the benchmark timing/performance split queue above is worked
in order or explicitly blocked. Completed HOTPATH implementation, freshness, rerun, publication,
and shared public workflow route facade history lives only in the completed ledger. This section now
owns the remaining user-surface runtime breadth: SQL grammar,
Python/DataFrame API breadth, object-store/lakehouse runtime, generated-output platform routes,
data-quality/profile/quarantine runtime, effectful operations, live/hybrid runtime,
distributed/spill/OOM runtime, and front-door benchmark publication.

Each item below uses the same sub-checklist shape:

- Current state: compact checked checklist rows summarize what is already proven by the ledger,
  matrix, or validators; unchecked rows name what remains.
- Execution checklist: unchecked implementation/validation steps for the next cohesive PR.
- Boundaries: claim, fallback, non-goal, and ledger rules stay attached to the active item.

- [ ] GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar: Broad SQL grammar over
  Vortex-normalized runtime paths.
  - Source: SQL/Python/DataFrame front-door parity docs, admitted semantics matrix, user-route
    capability report, completed runtime ledger entries, and remaining broad grammar blockers.
  - Current state:
    - [x] Scoped grammar evidence lives in `docs/status/admitted-semantics-matrix.json` and the
      completed ledger.
    - [x] Timezone syntax/function/type, locale/case-folding, complex boundary, list/array/struct,
      binary source, decimal literal, scalar-expression `JOIN ON`, complex-key join blockers, and
      set-operation slices are no longer the next active 6D grammar blockers.
    - [x] Local-source scalar and row-value `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS`, quantified,
      projected, correlated, source-qualified, predicate-projection, CASE-projection, and scoped
      HAVING `NOT IN` / correlated `NOT EXISTS` subquery surfaces have admitted evidence or
      deterministic blockers where scoped.
    - [x] HAVING-level row-value `IN`/`NOT IN` and correlated quantified variants now have matrix
      fixtures, Rust smokes, docs, and no-fallback report fields.
    - [x] Scoped `ARRAY[...]` / `STRUCT(...)` result-boundary projections can write local CSV as
      JSON text cells with replay/no-fallback evidence.
    - [ ] Remaining: nested source decoding and typed structured sinks beyond scoped
      JSONL/result-boundary and CSV JSON-text projections.
    - [ ] Remaining: variant/union dtype shapes, broader binary execution/preservation,
      non-binary-source-to-binary-literal comparisons, ORC typed decimal sinks, broad ANSI decimal
      coercion, scalar-left multi-column subqueries, and unbound source aliases outside admitted
      source-qualified surfaces.
  - Runtime enablement: public route facade -> SQL parse/bind request -> ShardLoom capability
    admission -> native runtime lowering or deterministic unsupported diagnostic -> no-fallback
    evidence row.
  - Next slice outcome: choose the next coherent grammar family from the unchecked remaining rows;
    current candidates are
    nested source decoding, typed nested sinks beyond CSV JSON text, ORC typed decimal sink
    preservation once writer evidence exists, broader binary preservation, broad ANSI decimal
    coercion, or scalar-left multi-column subquery diagnostics depending on the next runtime route
    evidence.
  - Execution checklist:
    - [ ] Derive the exact admitted and unsupported shapes from the matrix, parser/runtime code, and
      existing CLI smokes before editing.
    - [ ] Add positive SQL fixtures and decoded-reference expectations for every newly admitted
      shape.
    - [ ] Add deterministic unsupported diagnostics for still-non-admitted shapes.
    - [ ] Update matrix/status/release docs, route/capability reports, and Python/DataFrame parity
      docs when a familiar user surface exists.
    - [ ] Validate no-fallback fields: `fallback_attempted=false` and
      `external_engine_invoked=false`.
  - User-visible surface: CLI SQL local-source runtime, Python `sql(...)`, DataFrame aliases,
    capability matrices, docs, and benchmark-range route reports.
  - Implementation scope: `shardloom-cli/src/sql_local_source_runtime.rs`, Python query/session
    lowering, SQL/DataFrame parity validators, route capability reports, and docs.
  - Evidence required: positive SQL fixtures, decoded-reference expectations, unsupported
    diagnostics for non-admitted shapes, parity docs where relevant, no-fallback fields, and claim
    gates for every newly admitted syntax family.
  - Acceptance: admitted SQL grammar reaches an existing ShardLoom runtime route; non-admitted
    grammar fails deterministically without external engines.
  - Verification: focused Rust CLI tests, Python parity tests where relevant,
    `scripts/check_sql_python_dataframe_parity.py`, `scripts/check_user_route_capability_report.py`,
    and `git diff --check`.
  - Non-goals: no external SQL engine, no broad optimizer/performance claim, no object-store/table
    SQL runtime.
  - Dependencies/blockers: parser/binder coverage, expression capability mapping, runtime operator
    evidence, and deterministic diagnostics.
  - Claim boundary: scoped grammar/runtime admission only; no production SQL, performance,
    Spark-replacement, or external-fallback claim.
  - Fallback boundary: no external SQL, DataFusion, DuckDB, Spark, Polars, Velox, or query-engine
    fallback execution; external engines may appear only as tests or benchmark baselines.
  - Ledger rule: when the chosen grammar slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual grammar blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth: Full Python/DataFrame API
  breadth.
  - Source: Python query/session API docs, SQL/Python/DataFrame parity docs, user-route capability
    report, completed Python surface ledger entries, and remaining DataFrame parity blockers.
  - Current state:
    - [x] Familiar aliases lower to admitted ShardLoom runtime paths where evidence exists.
    - [x] `schema_contract(...)`, `profile(...)`, and scoped `quarantine(...)` have bounded
      local-source evidence.
    - [x] Python SQL result reports expose parsed typed decimal sink support for admitted
      Parquet/Arrow IPC/Avro/Vortex paths and the ORC-blocked boundary.
    - [x] `LazyFrame.join(condition=...)` accepts ShardLoom predicate objects for scoped
      scalar-expression and logical `OR` join predicates over qualified local-source columns.
    - [x] Python SQL smoke reports expose runtime unsupported `status`, `diagnostics`, and
      `unsupported_reasons` for non-admitted correlated subquery shapes while preserving
      no-fallback fields.
    - [ ] Remaining: promote the next coherent API family that lowers to an admitted runtime route
      or deterministic unsupported diagnostic.
    - [ ] Remaining: broad DataFrame parity remains gated.
  - Runtime enablement: Python/DataFrame-style API call -> shared public route facade ->
    deterministic ShardLoom query lowering -> admitted runtime route or explicit unsupported
    diagnostic -> no-fallback evidence row.
  - Next slice outcome: promote the next coherent Python/DataFrame API family only when it lowers to
    existing ShardLoom runtime evidence or returns deterministic unsupported diagnostics.
  - Execution checklist:
    - [ ] Add alias/canonical equivalence tests for the chosen API family.
    - [ ] Preserve no hidden pandas/Polars execution.
    - [ ] Update capability rows and user-facing docs for admitted or blocked behavior.
  - User-visible surface: `shardloom` Python package, session/query builders, docs, parity matrix,
    and route capability report.
  - Implementation scope: `python/src/shardloom/query.py`, `context.py`, `session.py`, Python tests,
    CLI lowering where needed, and docs.
  - Evidence required: Python tests proving alias/canonical equivalence, no hidden pandas/Polars
    execution, fallback/external-engine false fields, and capability rows.
  - Acceptance: the new API family is intuitive from Python while still mapping to real ShardLoom
    runtime or explicit unsupported output.
  - Verification: `python3 -m unittest python/tests/test_query_builder.py`,
    `python3 -m unittest python/tests/test_sql_python_dataframe_parity.py`,
    `scripts/check_python_user_surface_completion.py`, and `git diff --check`.
  - Non-goals: no broad pandas/Polars backend, no production DataFrame claim, no unbounded
    materialization convenience.
  - Dependencies/blockers: SQL/runtime capability coverage, output-plan support, and typed Python
    result models.
  - Claim boundary: scoped Python ergonomic surface only; no performance, production DataFrame, or
    external-fallback claim.
  - Fallback boundary: no hidden pandas, Polars, DuckDB, DataFusion, Spark, or external DataFrame
    backend execution; Python remains a front door into ShardLoom runtime or explicit blockers.
  - Ledger rule: when the chosen Python/DataFrame slice is complete, move the completed details to
    the ledger and leave the next unchecked 6-series item or residual Python blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime: Object-store,
  lakehouse/table, catalog, partition discovery, commit, rollback, recovery, and remote result
  delivery runtime.
  - Source: Native I/O contracts, object-store/runtime docs, table/lakehouse boundary reports,
    credential/effect policy docs, and completed object-store fixture ledger entries.
  - Current state:
    - [x] Object-store URI parsing exists.
    - [x] Public/no-credential fixture reads exist.
    - [x] Local-emulator read/write smokes exist.
    - [x] Table/lakehouse boundary reports exist.
    - [ ] Remaining: live providers remain gated.
    - [ ] Remaining: table commits, rollback, recovery, partition discovery, catalog integration, and
      remote result delivery need scoped admission or deterministic blockers.
  - Runtime enablement: credential-safe object/table request -> ShardLoom Native I/O admission ->
    bounded read/write/commit/recovery proof or deterministic blocker -> no-fallback evidence row.
  - Next slice outcome: add the next local or credential-safe runtime promotion with explicit
    admission, commit/recovery evidence, and no-fallback diagnostics.
  - Execution checklist:
    - [ ] Use a local, isolated, or credential-safe fixture.
    - [ ] Record credential/effect policy fields and Native I/O evidence.
    - [ ] Prove commit/rollback/recovery or keep the path explicitly blocked.
  - User-visible surface: CLI object-store/table commands, Python helpers, capability matrices,
    docs, and release gates.
  - Implementation scope: object-store runtime modules, table boundary reports, credential policy,
    output/replay validators, and docs.
  - Evidence required: local or isolated provider fixture, commit/rollback/recovery proof where the
    path writes state, credential/effect policy fields, Native I/O evidence, and no-fallback status.
  - Acceptance: one previously gated object-store/table workflow executes through ShardLoom-native
    boundaries or fails with deterministic diagnostics.
  - Verification: object-store/table smoke tests, credential/effect validators, release readiness
    checks, and `git diff --check`.
  - Non-goals: no live cloud write by default, no hidden credential probing, no table production
    claim.
  - Dependencies/blockers: credential policy, commit protocol, replay verification, cleanup
    semantics, and Native I/O certificates.
  - Claim boundary: scoped fixture/local runtime only; no production lakehouse/object-store,
    performance, or fallback claim.
  - Fallback boundary: no Spark, DataFusion, DuckDB, Polars, external lakehouse engine, warehouse, or
    catalog service may execute ShardLoom work; external platforms remain explicit boundaries only.
  - Ledger rule: when the chosen object/table slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual object-store blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime: Promote the remaining
  generated-output platform routes only after their effect boundary is real.
  - Source: generated-source runtime docs, output/fanout contracts, platform proof docs, effect
    policy, replay/fidelity evidence, and completed generated-output ledger entries.
  - Current state:
    - [x] Generated rows can write local outputs.
    - [x] Local-emulator object-store proofs exist.
    - [x] Foundry-style dev-stack proofs exist.
    - [ ] Remaining: live platform APIs remain gated.
    - [ ] Remaining: platform routes need explicit effect, credential, output, replay, and fidelity
      evidence before promotion.
  - Runtime enablement: generated-output request -> explicit effect/output admission -> local or
    platform-bound output proof plus replay/fidelity evidence or deterministic blocker.
  - Next slice outcome: promote the next generated-output platform route only with explicit effect,
    credential, output, and replay evidence.
  - Execution checklist:
    - [ ] Attach generated-source certificate and output artifact proof.
    - [ ] Add replay/fidelity evidence and no-fallback fields.
    - [ ] Keep live platform writes blocked unless explicitly approved.
  - User-visible surface: Python generated-output helpers, CLI generated-source commands, Foundry
    and object-store proof docs, capability rows, and release checks.
  - Implementation scope: generated-source runtime, output/fanout helpers, platform boundary
    reports, validators, and docs.
  - Evidence required: generated-source certificate, output artifact proof, replay/fidelity
    evidence, effect policy, credential policy where relevant, and no-fallback fields.
  - Acceptance: promoted generated-output route writes through an admitted ShardLoom boundary and
    reports the exact platform/effect scope.
  - Verification: generated-source runtime smokes, platform proof scripts, production usability gate,
    and `git diff --check`.
  - Non-goals: no real Foundry/cloud write without explicit approval, no Marketplace/package claim.
  - Dependencies/blockers: effect budget, credential policy, output-plan support, and
    platform-specific boundary reports.
  - Claim boundary: scoped generated-output proof only; no production platform, performance, or
    fallback claim.
  - Fallback boundary: no external platform, Spark, warehouse, or integration runtime may perform
    hidden execution; effectful writes require explicit admission and evidence.
  - Ledger rule: when the chosen generated-output slice is complete, move the completed details to
    the ledger and leave the next unchecked 6-series item or residual platform blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.data_quality_quarantine_profile_runtime: Promote
  remaining data-quality observability and quarantine surfaces only where they are backed by
  ShardLoom runtime evidence.
  - Source: data-quality/profile/quarantine docs, Python query builder parity docs, output/fanout
    reports, capability matrices, and completed data-quality ledger entries.
  - Current state:
    - [x] Bounded local-source `profile(...)` uses admitted local-source runtime evidence.
    - [x] Scoped `quarantine(...)` uses admitted local-source runtime evidence.
    - [ ] Remaining: broader table/object-store remediation remains gated.
    - [ ] Remaining: the next check or quarantine action needs bounded ShardLoom runtime proof and
      deterministic unsupported diagnostics for non-admitted checks.
  - Runtime enablement: data-quality/profile/quarantine request -> admitted bounded ShardLoom
    runtime check or explicit unsupported diagnostic -> output/replay evidence when a sink is
    written.
  - Next slice outcome: add the next bounded data-quality check or quarantine action with ShardLoom
    runtime proof and explicit unsupported diagnostics for non-admitted checks.
  - Execution checklist:
    - [ ] Add positive bounded checks and negative unsupported diagnostics.
    - [ ] Attach output/replay evidence when a sink is written.
    - [ ] Keep no-fallback fields visible in reports.
  - User-visible surface: Python query builder, CLI local-source smoke, output/fanout reports, docs,
    and capability matrices.
  - Implementation scope: Python query API, CLI runtime fields, local sink outputs, validators, and
    docs.
  - Evidence required: positive bounded checks, no-fallback fields, output/replay evidence where a
    sink is written, and report-only or unsupported classification for blocked checks.
  - Acceptance: users can run the promoted check without external profiling engines, and unsupported
    checks remain explicit.
  - Verification: Python query tests, SQL/DataFrame parity validator, user-surface completion
    checker, and `git diff --check`.
  - Non-goals: no production governance workflow, no object-store/table quarantine, no broad
    profiling claim.
  - Dependencies/blockers: expression capability mapping, output-plan replay proof, and data-quality
    diagnostic vocabulary.
  - Claim boundary: scoped bounded data-quality runtime only; no production governance, performance,
    or fallback claim.
  - Fallback boundary: no hidden pandas/Polars profiling, DuckDB SQL, Spark quality engine, or
    external remediation runtime; unsupported checks fail explicitly.
  - Ledger rule: when the chosen data-quality slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual data-quality blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.effectful_operations: Effectful operations: UDFs, LLM/API
  calls, embeddings, vector search, external writes, credentials, sandboxing, and deterministic
  effect budgets.
  - Source: modular extensibility RFCs, extension/plugin safety docs, effect policy docs,
    security/release validators, and completed effectful-operation ledger entries.
  - Current state:
    - [x] Effectful-operation admission reports exist.
    - [x] Local deterministic UDF and SQLite fixture boundaries exist.
    - [ ] Remaining: arbitrary effects remain blocked.
    - [ ] Remaining: one effect family needs explicit policy, capability, sandbox, and no-fallback
      evidence before admission.
  - Runtime enablement: effectful operation declaration -> capability/permission/effect-budget
    admission -> sandboxed local proof or deterministic blocker -> no-fallback evidence row.
  - Next slice outcome: promote one effect family through explicit policy, capability, sandbox, and
    no-fallback evidence.
  - Execution checklist:
    - [ ] Add side-effect declaration and permission policy evidence.
    - [ ] Add deterministic diagnostics for non-admitted effects.
    - [ ] Record sandbox status and security review evidence where needed.
  - User-visible surface: CLI effect/extension reports, Python helpers, docs, capability matrices,
    and security/release validators.
  - Implementation scope: effect budget plan, extension/UDF boundaries, credential policy,
    diagnostics, tests, and docs.
  - Evidence required: side-effect declaration, permission policy, deterministic diagnostics,
    sandbox status, no-fallback fields, and security review where needed.
  - Acceptance: admitted effects are explicit and inspectable; non-admitted effects cannot execute
    silently.
  - Verification: effect-budget tests, security/effect validators, relevant Python/CLI tests, and
    `git diff --check`.
  - Non-goals: no hidden network/API calls, no arbitrary plugin execution, no credential discovery
    by default.
  - Dependencies/blockers: sandbox policy, credential governance, extension manifests, and security
    review.
  - Claim boundary: scoped effect admission only; no production UDF/LLM/vector/platform claim.
  - Fallback boundary: no hidden external service call, plugin execution, query engine fallback, or
    credential probing; all effects require explicit user/policy admission.
  - Ledger rule: when the chosen effectful slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual effect blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime: Live/hybrid runtime state, incremental
  processing, CDC beyond scoped overlay fixtures, freshness/snapshot contracts, state cleanup,
  cancellation, retry, and recovery.
  - Source: three-engine execution fabric RFC, fault-tolerance/recovery docs, live/hybrid state
    reports, CDC overlay evidence, and completed hybrid-runtime ledger entries.
  - Current state:
    - [x] Engine-selection and hybrid overlay reports exist with fixture-scoped evidence.
    - [ ] Remaining: production broker/state-store runtime is not admitted.
    - [ ] Remaining: exactly-once runtime is not admitted.
    - [ ] Remaining: the next bounded live/hybrid state transition needs freshness, snapshot,
      retry/cancellation, cleanup, and no-fallback evidence.
  - Runtime enablement: bounded live/hybrid request -> freshness/snapshot admission -> state
    transition/retry/cancellation/cleanup proof or deterministic blocker -> no-fallback evidence
    row.
  - Next slice outcome: promote the next bounded live/hybrid state transition with freshness,
    cancellation/retry, cleanup, and no-fallback evidence.
  - Execution checklist:
    - [ ] Add bounded state fixture and freshness/snapshot proof.
    - [ ] Add retry/cancellation and cleanup evidence.
    - [ ] Keep broker-backed or production semantics blocked unless explicitly admitted.
  - User-visible surface: engine capability matrix, Python/CLI hybrid reports, docs, and release
    gates.
  - Implementation scope: hybrid runtime reports, state cleanup/recovery logic, diagnostics, tests,
    and docs.
  - Evidence required: bounded state fixture, freshness/snapshot proof, retry/cancellation evidence,
    cleanup proof, and fallback/external-engine false fields.
  - Acceptance: one live/hybrid workflow has explicit runtime state and deterministic failure
    behavior.
  - Verification: hybrid/runtime tests, capability validators, release readiness checks, and
    `git diff --check`.
  - Non-goals: no broker-backed production runtime, no exactly-once claim, no object-store/table
    commit promotion.
  - Dependencies/blockers: state store semantics, commit/recovery model, cleanup policy, and
    correctness fixtures.
  - Claim boundary: fixture-scoped live/hybrid evidence only; no production streaming or Spark
    replacement claim.
  - Fallback boundary: no Kafka/Flink/Spark/Ray/Dask/state-store delegation, no hidden broker
    runtime, and no external streaming fallback; live/hybrid gaps remain deterministic blockers.
  - Ledger rule: when the chosen live/hybrid slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual state blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime: Distributed/shuffle/spill/OOM
  production runtime, including resource governance and deterministic pre-OOM diagnostics.
  - Source: memory/spill/OOM RFCs, optimizer/adaptive execution docs, resource governance reports,
    release-readiness gates, and completed memory/spill ledger entries.
  - Current state:
    - [x] Spill/OOM plans exist.
    - [x] Memory declarations exist.
    - [x] Blocked diagnostics exist.
    - [ ] Remaining: real query-data spill remains gated.
    - [ ] Remaining: distributed execution remains gated.
    - [ ] Remaining: the next local bounded memory/spill guard must fail before process OOM or write
      admitted ShardLoom-native spill evidence.
  - Runtime enablement: bounded resource-pressure request -> memory/spill admission or pre-OOM
    diagnostic -> cleanup evidence and no-fallback evidence row.
  - Next slice outcome: promote the next local bounded memory/spill guard that fails before process
    OOM or writes admitted ShardLoom-native spill evidence.
  - Execution checklist:
    - [ ] Add bounded-memory fixture or admitted spill proof.
    - [ ] Attach deterministic pre-OOM/blocker evidence and cleanup proof.
    - [ ] Avoid distributed/shuffle claims unless the runtime is explicitly admitted.
  - User-visible surface: CLI diagnostics, benchmark rows, memory/spill reports, docs, and release
    readiness gates.
  - Implementation scope: memory reservation, spill diagnostics, operator declarations, tests,
    validators, and docs.
  - Evidence required: bounded-memory fixture, deterministic pre-OOM/blocker evidence, cleanup
    proof, and no-fallback fields.
  - Acceptance: memory pressure is explicit and deterministic for the promoted path.
  - Verification: memory/spill tests, release readiness checks, focused benchmark artifact validators
    when rows change, and `git diff --check`.
  - Non-goals: no distributed runtime, no broad shuffle support, no performance claim.
  - Dependencies/blockers: reservation model, spill format/persistence policy, cleanup semantics,
    and correctness parity.
  - Claim boundary: scoped memory/spill safety only; no production distributed/spill or performance
    claim.
  - Fallback boundary: no Spark/Dask/Ray/Trino/warehouse shuffle, spill, or distributed execution
    fallback; unsupported memory pressure fails before hidden delegation.
  - Ledger rule: when the chosen memory/spill slice is complete, move the completed details to the
    ledger and leave the next unchecked 6-series item or residual resource blocker in Planned.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication: Claim-grade
  performance-equivalence benchmark publication across equivalent SQL, Python, and DataFrame
  workloads.
  - Source: benchmark suite catalog, front-door parity docs, benchmark publication validators,
    website benchmark page, and completed benchmark-publication ledger entries.
  - Current state:
    - [x] Route-first benchmark artifacts exist.
    - [x] Benchmark publication validators exist.
    - [x] HOTPATH-14 promoted artifact is the current evidence surface, with
      `performance_claim_allowed=false`.
    - [ ] Remaining: front-door performance equivalence remains not claim-grade.
    - [ ] Remaining: SQL/Python/DataFrame route parity and benchmark safety gates must pass before
      any approved rerun/promotion.
    - [ ] Remaining: website benchmark publication must distinguish runtime support, evidence
      grade, and performance claims.
  - Runtime enablement: route-parity evidence -> approved benchmark rerun/promotion -> validated
    website artifact -> claim-gated front-door benchmark publication.
  - Next slice outcome: publish a laptop-safe, reproducible front-door equivalence artifact only
    after SQL/Python/DataFrame route parity and benchmark safety gates are satisfied.
  - Execution checklist:
    - [ ] Confirm benchmark rerun approval and laptop-safe sequential controls before running.
    - [ ] Attach reproducible artifact, correctness digests, hardware/runtime context, and
      no-fallback fields.
    - [ ] Update website data/components, docs, and validators together.
  - User-visible surface: benchmark artifacts, website benchmark page, README/docs, Python examples,
    and release gates.
  - Implementation scope: benchmark harness, promotion scripts, website data/components, docs, and
    validators.
  - Evidence required: reproducible artifact, route parity, correctness digests, hardware/runtime
    context, sequential/safety controls, and no-fallback fields.
  - Acceptance: published rows distinguish runtime support, evidence grade, and performance claims
    without unsupported ShardLoom gaps or external fallback.
  - Verification: benchmark artifact validators, website readiness/static checks, focused benchmark
    smoke when approved, and `git diff --check`.
  - Non-goals: no broad benchmark suite on an unsafe laptop path, no superiority/Spark-replacement
    claim without CG-5/CG-6 evidence.
  - Dependencies/blockers: route parity, claim gates, benchmark safety redesign, current generated
    artifacts, and documentation alignment.
  - Claim boundary: no performance-equivalence claim until the artifact is claim-grade and published
    through approved gates.
  - Fallback boundary: ShardLoom rows must retain no-fallback/no-external-engine evidence; external
    engines remain baselines only and cannot satisfy ShardLoom route parity.
  - Ledger rule: when the benchmark-publication slice is complete, move the completed details to the
    ledger and continue to the residual backstop or release closeout only if the 6-series blockers
    are reduced for the claimed scope.

Shared 6-series completion criteria:

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
language support before the relevant HOTPATH, freshness/rerun, and 6-series checklist items are
closed; do not publish packages/releases; do not run broad benchmarks unless the current slice
explicitly needs benchmark evidence and uses the laptop-safe sequential controls.

Claim boundary: this item can claim runtime-ready user paths only for explicitly wired
benchmark-range workflows with passing validation. It cannot claim broad SQL/Python/DataFrame
flexibility, object-store/table production readiness, live/hybrid production readiness, or
performance equivalence until the last-order checklist items are implemented and validated.

Fallback boundary: every admitted route must report `fallback_attempted=false` and
`external_engine_invoked=false`; external engines remain benchmark baselines or test oracles only.

Ledger rule: when a child slice is completed and merged, move the completed details to
`docs/architecture/phased-execution-completed-ledger.md`, then keep only remaining unchecked work
here.

#### GAR-RUNTIME-IMPL-4/6A - Residual Completion Gate And Internal Backstop Queue
Current runtime ordering note (updated 2026-06-03): this section is a residual backstop after the
active 6D user-surface breadth queue. Completed engine-internal closeouts are recorded in
`docs/architecture/phased-execution-completed-ledger.md`; do not copy their titles back into this
live queue. Work this section only when a specific remaining 6A/completion-gate blocker must be
reduced or when it directly blocks the active 6D route/runtime work.

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
  - Current state: completed benchmark/profile, sub-evidence, user-surface, and UDF/extension
    freshness passes are recorded in the completed ledger. The live backstop state is still blocked:
    the release architecture tracker currently reports 38 unchecked global architecture review
    items and 10 unchecked phase-plan runtime items before whole-engine completion can be claimed.
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

### Global Architecture Review Carry-Forward

Ordering note: this cross-cutting context intentionally follows the active runtime implementation
queue. Use it to verify and mirror runtime work as each section lands; do not let it reorder the
next session ahead of the active 6D runtime breadth queue unless it identifies a concrete release,
safety, security, or
claim-integrity blocker for the next runtime item.

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

#### Deferred Non-Runtime Closeout Queue

Documentation, capability, security, release, and claim-gate cleanup belongs here only when it is
not runtime-enabling. These items must not add runtime behavior or support claims. Add a concrete
unchecked item here only when a new documentation, website, security, release, or claim-gate blocker
must interrupt runtime work.

Current non-runtime sequence: deferred behind the active runtime-readiness queue unless a specific
blocker must be pulled forward with explicit justification. Completed non-runtime history belongs in
`docs/architecture/phased-execution-completed-ledger.md`.

## Final Pre-Release Sequential Closeout Queue

This queue is intentionally last. It does not reorder the active runtime-readiness work above, and
it does not authorize package publication, release tags, signing, package-channel submission, public
claims, or fallback execution. Use it only after the runtime queue has reduced the claimed release
scope to a concrete, evidence-backed candidate.

Release sequencing rule: work these items one by one before any public release or package-channel
claim. A passing local proof, production-usability gate, or final rehearsal is not a publication
approval. Public release and public package publication still require explicit maintainer approval
after the hard gate and channel-specific evidence pass.

- [ ] RELEASE-SEQUENCE-1 release scope freeze and claim inventory:
  Source: `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/known-unsupported-paths.md`, `docs/status/runs-today-support-matrix.json`, and
  `docs/architecture/phased-execution-plan.md`.
  Objective: freeze the exact public release scope before package work begins. The release candidate
  must state which workflows are included, which remain technical-preview only, which are blocked,
  and which claims are explicitly false.
  Implementation scope: release notes draft, README/status copy, runs-today support matrix,
  per-claim evidence matrix, known unsupported paths, website start/status/docs pages, and hard
  release-readiness report inputs.
  Acceptance: release candidate language preserves `public_release_claim_allowed=false`,
  `public_package_claim_allowed=false`, `production_claim_allowed=false`,
  `performance_claim_allowed=false`, `spark_replacement_claim_allowed=false`,
  `fallback_attempted=false`, and `external_engine_invoked=false` until later sequence items attach
  passing evidence and human approval.
  Verification:
  ```powershell
  python scripts\check_website_readiness.py
  python scripts\check_release_architecture_tracker.py --allow-blocked
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no package publication, no release tag, no claim upgrade, no runtime expansion.

- [ ] RELEASE-SEQUENCE-2 runtime-support blocker closeout for the claimed scope:
  Source: `docs/architecture/runtime-gap-family-burn-down.md`,
  `docs/status/runtime-execution-envelope-validation.md`,
  `docs/architecture/phased-execution-plan.md`, and
  `docs/release/hard-release-readiness-gate.md`.
  Objective: reduce every runtime blocker that affects the selected release scope to either
  runnable evidence, deterministic unsupported diagnostics, or an explicitly out-of-scope row.
  Implementation scope: runtime gap family burn-down report, user-surface runtime gap inventory,
  user route capability report, runs-today support matrix, runtime execution envelopes, and the
  relevant runtime code/tests for each remaining blocker family.
  Acceptance: the claimed release scope has no ambiguous `blocked`, `unsupported`, `report_only`,
  or `fixture_smoke_only` row masquerading as runtime-ready. Every remaining gap has an owning
  blocker ID, deterministic diagnostics, and no-fallback evidence.
  Verification:
  ```powershell
  python scripts\check_runtime_gap_family_burn_down.py
  python scripts\check_user_surface_runtime_gap_inventory.py
  python scripts\check_user_route_capability_report.py
  python scripts\check_runtime_execution_envelopes.py
  cargo test --workspace --all-targets
  git diff --check
  ```
  Non-goals: no broad runtime claim outside the selected release scope.

- [ ] RELEASE-SEQUENCE-3 API, CLI, schema, and typed-envelope stability decision:
  Source: `docs/release/publication-api-schema-stability-gate.md`,
  `docs/rfcs/0024-release-engineering-api-compatibility-packaging.md`,
  `docs/architecture/crate-posture-public-exports.md`, and
  `docs/architecture/typed-command-result-envelope.md`.
  Objective: decide which Rust, CLI, Python, JSON, benchmark, diagnostic, and capability surfaces
  are internal, experimental, stable, deprecated, or removed for the release candidate.
  Implementation scope: publication/API/schema stability gate, CLI JSON schemas, Python accessors,
  capability reports, route/evidence schemas, benchmark schema versioning, and release notes.
  Acceptance: every public or package-visible surface has a stability tier, version marker where
  applicable, compatibility window status, migration note or blocker, and no unsupported stability
  promise.
  Verification:
  ```powershell
  cargo test -p shardloom-contract-tests --test release_readiness_metadata
  python -m unittest python.tests.test_release_scripts
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no stable public API claim for experimental surfaces.

- [ ] RELEASE-SEQUENCE-4 package identity, version, metadata, and release-note audit:
  Source: `docs/release/package-name-readiness.md`, `docs/release/package-metadata-audit.md`,
  `python/pyproject.toml`, workspace `Cargo.toml`, and `packaging/conda/README.md`.
  Objective: confirm version alignment, package names, metadata, README links, license files,
  package descriptions, classifiers, and release notes before channel-specific work begins.
  Implementation scope: Python package metadata, Rust workspace metadata, Conda recipe scaffolds,
  package-name readiness docs, release notes draft, README, LICENSE/NOTICE references, and website
  package posture.
  Acceptance: package metadata is internally consistent and still describes a pre-release,
  Vortex-first, no-fallback local compute engine. Current internal Rust crates remain
  `publish=false`; future crates.io candidates remain blocked unless extracted and approved.
  Verification:
  ```powershell
  python -m build python
  python -m twine check python\dist\*
  cargo test -p shardloom-contract-tests --test release_readiness_metadata
  python scripts\check_package_channel_readiness.py --require-local-evidence
  git diff --check
  ```
  Non-goals: no upload to PyPI, TestPyPI, crates.io, conda-forge, or another channel.

- [ ] RELEASE-SEQUENCE-5 dependency, license, security, and provenance preflight:
  Source: `docs/skills/license-provenance.md`, `docs/security/release-security-gate.md`,
  `docs/security/supply-chain-response.md`, `docs/release/release-provenance-dry-run.md`, and
  `docs/release/sbom-generation-plan.md`.
  Objective: prove the release candidate has acceptable dependency license posture, security
  posture, provenance dry-run evidence, local SBOM/checksum refs, and no forbidden fallback
  dependency.
  Implementation scope: dependency audit report, security posture report, release security gate,
  release provenance dry run, SBOM/checksum dry-run outputs, workflow policy snapshot, and supply
  chain response docs.
  Acceptance: dependency/security/provenance reports pass or fail closed with explicit blockers;
  no Spark/DataFusion/DuckDB/Polars/Velox/Pandas/Dask/Trino/Ray dependency is introduced as a
  ShardLoom runtime fallback.
  Verification:
  ```powershell
  python scripts\check_dependency_audit.py --release-gate --json-output target\dependency-audit-report.json
  python scripts\check_security_posture.py
  python scripts\release_provenance_dry_run.py
  python scripts\check_release_security_gate.py
  git diff --check
  ```
  Non-goals: no signing, public attestation, package upload, or tag creation.

- [ ] RELEASE-SEQUENCE-6 local build, install, first-10-minutes, and clean Conda proof:
  Source: `docs/release/release-dry-run-proof.md`,
  `docs/release/first-10-minutes-smoke-snapshot.md`,
  `docs/release/production-usability-gate.md`, and
  `docs/release/hard-release-readiness-gate.md`.
  Objective: prove the candidate can be built locally, installed from local artifacts, imported
  from a clean virtual environment, exercised through the first-10-minutes user path, and installed
  in a clean Conda-compatible environment when required.
  Implementation scope: `scripts/release_dry_run_proof.py`, local wheel/sdist, CLI binary,
  quickstart examples, generated-source output smokes, local benchmark smoke, clean venv proof,
  clean Conda proof, and production-usability gate.
  Acceptance: release dry-run transcript records clean install/import/client smoke, CLI
  status/capabilities smoke, local Python smoke, generated output smoke, benchmark smoke,
  provenance dry run, and `clean_conda_env_install_status=passed` before public package/release
  claims are considered.
  Verification:
  ```powershell
  python scripts\release_dry_run_proof.py --rows 64 --iterations 1 --require-clean-conda
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\check_production_usability_gate.py
  git diff --check
  ```
  Non-goals: local install proof is not a public package claim.

- [ ] RELEASE-SEQUENCE-7 GitHub pre-release channel proof:
  Source: `docs/release/package-channel-readiness-matrix.md`,
  `docs/release/package-channel-readiness-matrix.json`, and
  `docs/release/final-release-rehearsal.md`.
  Objective: prepare GitHub pre-release distribution evidence before any package registry channel.
  Implementation scope: reviewed source archive, release artifact list, checksum refs, SBOM refs,
  provenance refs, install/download transcript, smoke transcript, rollback/delete policy, and
  maintainer approval field.
  Acceptance: the GitHub pre-release matrix row has channel-specific install, uninstall/delete or
  rollback, smoke, checksum, SBOM, provenance, artifact refs, and authorization proof. Until human
  approval exists, the row remains blocked.
  Verification:
  ```powershell
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\final_release_rehearsal.py --allow-blocked
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no GitHub release creation or tag creation by autonomous agents.

- [ ] RELEASE-SEQUENCE-8 Python package channel proof for TestPyPI and PyPI:
  Source: `.github/workflows/pypi-publish-draft.yml`,
  `docs/release/package-name-readiness.md`,
  `docs/release/package-channel-readiness-matrix.md`, and
  `docs/release/publication-api-schema-stability-gate.md`.
  Objective: close Python package channel evidence in the safe order: metadata/build/twine check,
  TestPyPI rehearsal, TestPyPI clean install/uninstall/smoke, PyPI Trusted Publisher/OIDC proof,
  PyPI clean install/uninstall/smoke, and maintainer approval.
  Implementation scope: Python wheel/sdist, PyPI/TestPyPI Trusted Publisher configuration,
  workflow hardening, package upload proof, clean install transcript, uninstall transcript, smoke
  transcript, yank policy, and package-channel matrix rows.
  Acceptance: TestPyPI and PyPI rows have Trusted Publisher/OIDC posture, no committed token, clean
  install/uninstall/smoke transcripts, SBOM/checksum/provenance refs, rollback/yank policy, and
  explicit maintainer approval before any package claim flips.
  Verification:
  ```powershell
  python -m build python
  python -m twine check python\dist\*
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no upload to TestPyPI or PyPI without explicit maintainer approval.

- [ ] RELEASE-SEQUENCE-9 CLI installer channel proof for Homebrew, Scoop, winget, and conda-forge:
  Source: `docs/release/package-channel-readiness-matrix.md`,
  `docs/release/package-name-readiness.md`, `packaging/conda/README.md`, and
  `docs/architecture/workspace-feature-build-matrix.md`.
  Objective: close each CLI/package-manager channel separately instead of treating one channel as
  proof for all installers.
  Implementation scope: Homebrew tap formula, Scoop manifest, winget manifest, Conda staged-recipes
  or feedstock proof, tagged source archive hash, installer checksums, clean install/uninstall
  transcripts, smoke transcripts, rollback/deprecate policies, and channel authorization state.
  Acceptance: each channel row becomes ready only with channel-specific artifact, checksum,
  install, uninstall, smoke, rollback, provenance, no-fallback dependency, and maintainer approval
  evidence. Local Conda recipe scaffolds are not treated as conda-forge proof.
  Verification:
  ```powershell
  cargo test -p shardloom-contract-tests --test conda_packaging_recipes
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no package-manager submission before approval; no fallback dependencies in recipes or
  manifests.

- [ ] RELEASE-SEQUENCE-10 container and future Rust public-crate channel proof:
  Source: `docs/release/package-channel-readiness-matrix.md`,
  `docs/release/package-metadata-audit.md`, `docs/architecture/crate-posture-public-exports.md`,
  and `docs/release/sbom-generation-plan.md`.
  Objective: decide whether the release candidate includes a GHCR image or future crates.io crates.
  If not, keep those rows explicitly blocked. If yes, close them with channel-specific proof.
  Implementation scope: Dockerfile or image build workflow, pinned base image, image SBOM, image
  provenance, vulnerability scan, pull/run smoke, digest pin, extracted future
  `shardloom-protocol`/`shardloom-client` crates, API/schema stability proof, and
  `cargo publish --dry-run` evidence.
  Acceptance: GHCR is blocked unless image build, SBOM/provenance/vulnerability scan, pull/run
  smoke, digest, and approval evidence exist. crates.io is blocked unless public crates are
  extracted, stable, approved, and dry-run published. Current internal crates remain unpublished.
  Verification:
  ```powershell
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no OCI push, no crates.io publication, no internal crate publication without explicit
  approval.

- [ ] RELEASE-SEQUENCE-11 publication-grade SBOM, checksum, signing, and attestation decision:
  Source: `docs/release/sbom-generation-plan.md`,
  `docs/release/release-provenance-dry-run.md`,
  `docs/release/publication-api-schema-stability-gate.md`, and
  `docs/security/supply-chain-response.md`.
  Objective: upgrade local dry-run SBOM/checksum/provenance evidence into publication-grade release
  attachments, or keep publication blocked until maintainers approve signing and attestation.
  Implementation scope: Rust workspace SBOM, Python artifact SBOM, CLI binary SBOM, optional OCI
  SBOM, checksum manifest tied to source revision and release artifacts, signing policy, key
  custody decision, SLSA/provenance attestation decision, release notes attachment refs, and
  incident response linkage.
  Acceptance: checksum and SBOM rows are publication-grade or still explicitly `dry_run_only`;
  signing/attestation rows are approved or blocked; no signing key is used before approval.
  Verification:
  ```powershell
  python scripts\release_provenance_dry_run.py
  python scripts\check_release_security_gate.py
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no signing, public attestation, upload, or tag creation by autonomous agents.

- [ ] RELEASE-SEQUENCE-12 documentation, website, unsupported-path, and per-claim evidence closeout:
  Source: `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/known-unsupported-paths.md`,
  `docs/release/website-public-post-readiness.md`, and `docs/release/public-technical-preview-readiness.md`.
  Objective: ensure public docs, website pages, README, benchmark pages, release notes, and known
  unsupported paths match the exact release scope and do not imply unsupported production,
  performance, package, Spark-replacement, object-store/lakehouse, Foundry, or broad
  SQL/DataFrame claims.
  Implementation scope: README, website, release docs, known unsupported paths, per-claim evidence
  matrix, benchmark publication language, status pages, examples, and release notes.
  Acceptance: every claim-bearing sentence maps to a row in the per-claim matrix; missing evidence
  keeps the row blocked; unsupported paths remain deterministic and explicit.
  Verification:
  ```powershell
  python scripts\check_website_readiness.py
  node website\validate_static_assets.js
  python scripts\check_benchmark_publication_claim_gate.py --manifest website\assets\benchmarks\latest\manifest.json
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: no marketing or superiority language not backed by claim-grade evidence.

- [ ] RELEASE-SEQUENCE-13 release CI, validation evidence, hard gate, and final rehearsal:
  Source: `docs/release/ci-gate-matrix.md`, `docs/release/hard-release-readiness-gate.md`,
  `docs/release/final-release-rehearsal.md`, and `.github/workflows/ci.yml`.
  Objective: run the full release validation matrix and final no-publication rehearsal after all
  previous release sequence items have closed for the selected scope.
  Implementation scope: CI gate matrix, release validation evidence report, dependency/security
  reports, package-channel report, production-usability report, architecture tracker, final release
  rehearsal, hard release-readiness gate, and uploaded CI artifacts.
  Acceptance: `scripts/check_release_readiness.py` passes without relying on stale, missing,
  blocked, or `--allow-blocked` evidence for the selected release scope. If the gate remains
  blocked, the blocker report names the next sequence item to return to.
  Verification:
  ```powershell
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace --all-targets
  python -m unittest discover python/tests
  python -m build python
  python scripts\run_release_validation_evidence.py
  python scripts\final_release_rehearsal.py
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: final rehearsal is still no-publication unless maintainers separately approve release
  execution.

- [ ] RELEASE-SEQUENCE-14 maintainer approval and publication handoff:
  Source: `docs/release/final-release-rehearsal.md`,
  `docs/release/package-channel-readiness-matrix.md`,
  `docs/release/publication-api-schema-stability-gate.md`, and
  `docs/security/supply-chain-response.md`.
  Objective: produce the final maintainer handoff packet for public release or package-channel
  publication. This item is the approval boundary, not an autonomous publication instruction.
  Implementation scope: final release notes, release candidate commit/tag proposal, package-channel
  readiness report, publication/API/schema stability report, SBOM/checksum/provenance refs,
  signing/attestation plan, rollback/yank/delete policy refs, hard release-readiness report, and
  explicit human approval record.
  Acceptance: maintainers have a single handoff packet that says exactly what will be published,
  where it will be published, which artifacts and checksums apply, which claims are allowed, which
  claims remain blocked, how rollback works, and which approval action is required.
  Verification:
  ```powershell
  python scripts\check_package_channel_readiness.py --require-local-evidence
  python scripts\final_release_rehearsal.py
  python scripts\check_release_readiness.py
  git diff --check
  ```
  Non-goals: Codex agents must not publish packages, create tags, sign artifacts, push containers,
  upload SBOMs, submit feedstocks, or create public release assets unless the user explicitly gives
  that approval for the active release step.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
