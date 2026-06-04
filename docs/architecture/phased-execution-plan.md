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

1. Continue `GAR-RUNTIME-IMPL-6D:last_order.benchmark_driven_prepare_path_optimization` from the
   2026-06-03 safe-writer code/text benchmark evidence and the follow-up component optimization
   note attached to that run. The sink-artifact sentinel fix,
   scenario-aware optional text-column selection, shared prepared/native artifact optional-column
   preservation, post-hotpath benchmark/site refresh, streaming workspace-safe Vortex writer helper,
   safe-writer benchmark/site refresh, exclusive stage-ledger de-overlap, and post-ledger writer
   validation safety fix are complete. The refreshed safe-writer artifact reports published
   ShardLoom route geomeans of 137.71 ms for the cold certified route, 58.00 ms for prepare-once
   first query, 8.37 ms for prepare-once batch, 5.57 ms for warm prepared query, and 5.58 ms for
   native Vortex query. The existing safe-writer artifact redecorated with the exclusive schema,
   without a benchmark rerun, reports cold certified-route exclusive stage-sum geomean 133.88 ms,
   residual geomean 2.48 ms, `vortex_write_ms` geomean 76.79 ms,
   `source_parse_or_columnar_decode_ms` geomean 29.48 ms, de-overlapped `source_read_ms` geomean
   7.80 ms, `vortex_scan_ms` geomean 2.32 ms, `result_sink_write_ms` geomean 1.90 ms, and
   `evidence_render_ms` geomean 0.08 ms. The inclusive compatibility-import audit bundle remains
   127.94 ms and must not be treated as an exclusive stage. The active remaining benchmark-driven
   sequence is now: Vortex writer/safe artifact emission, cold source parse/read pipeline,
   prepared-state admission/lookup, result sink/evidence render, then scan/operator attribution and
   pushdown. Do not rerun the expensive benchmark suite until the current code/docs/site batch is
   complete or the user explicitly approves a rerun.
2. Preserve end-to-end route totals as the primary comparison surface. Stage grids are attribution
   aids only, so future stage-level claims require exclusive timing fields, an inclusive
   compatibility view, and an auditable residual before superiority wording moves.
3. Return to the residual `GAR-RUNTIME-IMPL-4/6A` completion gate only after the active 6D breadth
   queue has reduced or explicitly blocked the runtime families it owns.

Remaining work snapshot:

| Order | Work item | Remaining outcome |
| --- | --- | --- |
| 1 | `6D:last_order.benchmark_driven_prepare_path_optimization` | Exclusive stage-ledger de-overlap is complete; next finish the remaining refreshed benchmark-driven sequence: writer/safe-artifact work, source parse/read, prepared admission/lookup, sink/evidence hot path, and scan/operator attribution. |
| 2 | `6D:last_order.broad_sql_grammar` | Promote the next admitted SQL grammar family or add deterministic unsupported diagnostics. |
| 3 | `6D:last_order.python_dataframe_api_breadth` | Promote the next Python/DataFrame alias family that lowers to admitted ShardLoom runtime evidence. |
| 4 | `6D:last_order.object_store_lakehouse_runtime` | Promote the next credential-safe object-store/table fixture or keep it explicitly gated. |
| 5 | `6D:last_order.generated_output_platform_runtime` | Promote the next generated-output platform route only with effect, credential, output, and replay evidence. |
| 6 | `6D:last_order.data_quality_quarantine_profile_runtime` | Add the next bounded data-quality/profile/quarantine runtime proof. |
| 7 | `6D:last_order.effectful_operations` | Admit one effect family through explicit policy, capability, sandbox, and no-fallback evidence. |
| 8 | `6D:last_order.live_hybrid_runtime` | Promote one bounded live/hybrid state transition with freshness, retry/cancellation, and cleanup proof. |
| 9 | `6D:last_order.distributed_spill_oom_runtime` | Add the next deterministic memory/spill/OOM guard or admitted spill proof. |
| 10 | `6D:last_order.front_door_performance_benchmark_publication` | Publish claim-grade front-door equivalence evidence only after route parity and benchmark safety gates pass. |
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

- Planned must contain only unchecked actionable work. Completed checklist items, completed
  sections, and completed session details belong only in
  `docs/architecture/phased-execution-completed-ledger.md`.
- If a completed item is found in Planned, remove it from this file after confirming the matching
  ledger entry exists or adding that ledger entry.
- Do not leave a completed parent section in Planned just to preserve history. Keep only active
  child work or a short pointer to the ledger when history is needed.
- Do not let docs-only, report-only, or claim-copy cleanup interrupt the runtime sequence above
  unless it is a release, safety, security, or claim-integrity blocker for the next runtime item.
- A runtime item is valid only when it has a `Runtime enablement:` field that names the runnable
  path, admission/blocker, or validator it enables. If that field cannot be made concrete, the item
  belongs in non-runtime planning or the completed ledger, not the runtime queue.

#### Runtime Implementation Queue - Runtime-Enabling Work Only

The earlier broad runtime rollup queues have been consolidated into the implementation-ready runtime
queues below. After the 6E automatic preparation/reuse closeout, 6F output/fanout closeout, 6C
user-surface graduation closeout, and 6D gap-family burn-down closeout, the current runtime
sequence is the remaining `GAR-RUNTIME-IMPL-6D:last_order.*` user-surface breadth. Pull a 6D
breadth item forward only when it unblocks the next runtime slice or prevents a misleading runtime
posture. The
remaining 4/5-series queue stays as internal-engine backstop work after the route/reuse/output
boundary work.

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

Ordering note (updated 2026-06-02 after the 6D burn-down closeout): this remains the user-surface
and benchmark-range breadth queue. The 6E automatic preparation/reuse, 6F output/fanout, 6C
graduation matrix, and 6D family burn-down gates are closed; resume the last-order checklist here,
or pull a narrow 6D blocker forward with explicit justification.

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

- ShardLoom has runnable local-source SQL/Python/DataFrame/context/session/CLI routes for scoped
  local file workflows, generated-output workflows, bounded decoded interop, local Vortex primitive
  report paths, prepared Vortex artifacts, native `.vortex` inputs, and benchmark-range route
  capability reports.
- Remaining work in this section is the last-order breadth below: benchmark-driven hot-path
  optimization, broad SQL grammar, Python/DataFrame API breadth, object-store/lakehouse runtime,
  generated-output platform runtime, data-quality/quarantine/profile runtime, effectful operations,
  live/hybrid runtime, distributed/spill/OOM runtime, and claim-grade front-door benchmark
  publication.
- Completed route/reuse/output/evidence foundations are recorded only in
  `docs/architecture/phased-execution-completed-ledger.md`.
- Native `.vortex` inputs start at the Vortex-native boundary. CSV/JSONL/Parquet/Arrow/Avro/ORC,
  generated rows, and materialized Python/Arrow inputs remain adapters into explicit
  Vortex-normalized ShardLoom routes before they can be called runtime-ready or claim-grade.
- The user target is runtime-go: for every local benchmark-range capability, users should have a
  clear route that runs ShardLoom, emits structured evidence, preserves
  `fallback_attempted=false`, and makes input/output boundaries obvious.

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

Next slice outcome: complete or measurably reduce the first unchecked last-order item, then move to
the next last-order item without reintroducing already-closed route-surface history into Planned.
Do not weaken claim gates; `not_claim_grade` remains valid until benchmark/correctness/certificate
evidence is attached.

Last-order runtime expansion checklist, not to be left as vague unsupported prose:

- [ ] GAR-RUNTIME-IMPL-6D:last_order.benchmark_driven_prepare_path_optimization:
  Benchmark-driven preparation, Vortex I/O, output/evidence, and encoded-operator hot-path
  optimization for runtime-ready local routes.
  Source: 2026-06-03 local code/text benchmark research and attached component optimization memo
  against the current promoted artifact and current branch sources; no vision-based benchmark
  tooling; `docs/architecture/phased-execution-plan.md`,
  `docs/architecture/phased-execution-completed-ledger.md`,
  `website-src/src/data/benchmark-evidence.json`, `scripts/promote_benchmark_artifact.py`,
  `shardloom-vortex/src/traditional_analytics.rs`, and
  `shardloom-cli/src/sql_local_source_runtime.rs`.
  Current state: benchmark publication and prior optimization evidence are recorded in
  `docs/architecture/phased-execution-completed-ledger.md`. The 2026-06-03 safe-writer full-local
  benchmark bundle reports published ShardLoom route geomeans of 137.71 ms for the cold certified
  route, 58.00 ms for prepare-once first query, 8.37 ms for prepare-once batch, 5.57 ms for warm
  prepared query, and 5.58 ms for native Vortex query. External baseline end-to-end geomeans in the
  same artifact are pandas 191.21 ms, Polars eager 38.78 ms, Polars lazy 28.63 ms, DuckDB 68.57 ms,
  DataFusion 32.42 ms across 114 successful rows, and Dask 270.90 ms. Scenario-aware text
  normalization, shared prepared/native artifact preservation, sink-artifact sentinel preservation,
  and the streaming workspace-safe Vortex writer helper are complete and should stay in the ledger
  rather than the live queue. The writer-helper benchmark/site refresh is also complete.
  Current attribution state: exclusive stage-ledger de-overlap is complete and recorded in
  `docs/architecture/phased-execution-completed-ledger.md`. The existing safe-writer artifact has
  been redecorated with exclusive stage fields and website rows without a benchmark rerun. Across
  the 120 ShardLoom cold certified-route rows, `exclusive_stage_timing_status=complete`, the
  exclusive stage-sum geomean is 133.88 ms, the residual geomean is 2.48 ms, the inclusive
  compatibility-import audit bundle is 127.94 ms, `vortex_write_ms` geomean is 76.79 ms,
  `source_parse_or_columnar_decode_ms` geomean is 29.48 ms, de-overlapped `source_read_ms` geomean
  is 7.80 ms, `vortex_scan_ms` geomean is 2.32 ms, `result_sink_write_ms` geomean is 1.90 ms, and
  `evidence_render_ms` geomean is 0.08 ms. The primary cold bottleneck is `vortex_write` in
  83/120 rows and `source_parse_or_decode` in 37/120 rows; the secondary bottleneck is
  source parse/decode in 61/120 rows, Vortex write in 37/120 rows, and source read in 22/120 rows.
  The old
  `source_read_millis` inclusive/geomean cell must not be used as an exclusive read target.
  Note reconciliation: the earlier component note's 144.27 ms cold route, 81.73 ms Vortex write,
  and 44.69 ms source-read figures are historical inclusive/post-hotpath readings. Use the newer
  safe-writer and exclusive attribution figures above for current ordering. Sparse warm/native
  query rows now remain `blocked_missing_query_split` unless both Vortex scan and operator-compute
  substages are present, so one-sided query timing cannot be published as complete exclusive
  prepared-query evidence.
  Runtime enablement: this item keeps the same user-visible route family:
  raw compatibility source, local `.vortex`, or prepared Vortex artifact -> explicit
  `SourceState`/`VortexPreparedState` boundary -> ShardLoom-owned prepared/native runtime ->
  report/result sink/evidence, with `fallback_attempted=false` and
  `external_engine_invoked=false`.
  Next slice outcome: reduce the next dominant ShardLoom-owned stage, starting with Vortex
  writer/safe-artifact emission, without changing route semantics, workspace safety, no-fallback
  evidence, route-total accounting, prepared artifact replay, or compatibility-output behavior.
  Source parse/read pressure is the next target after writer cost is no longer dominant.
  Benchmark-driven execution sequence:
  1. Stage ledger de-overlap: complete. Keep route totals as the primary comparison surface and
     keep exclusive stage sums, inclusive compatibility views, residuals, and baseline-only
     external rows as attribution fields rather than a second benchmark definition.
  2. Vortex writer and safe artifact emission: active next. Reduce `vortex_write_ms` first while it
     remains dominant by coalescing write, digest, and metadata capture; avoiding readback where
     certificate policy permits; reusing layout/write advisor choices; and reducing per-artifact
     open/close overhead without bypassing workspace-safe staging.
  3. Cold source parse and read: active after the writer-metadata coalescing slice. Reduce
     `source_parse_or_columnar_decode_ms` and de-overlapped `source_read_ms` by splitting
     `bytes_read`, `lex_parse`, `type_decode`, and `row_assembly` evidence, then adding
     streaming/projected CSV/JSONL paths where scenario-local certification does not require full
     optional-column artifact preservation, while retaining full-artifact paths for shared
     prepared/native artifacts. Current PR progress: projected text decode evidence is emitted for
     scenario-local fact imports, row-assembly strategy is reported, and the canonical JSONL fast
     path stops scanning unselected optional tail blocks once the route projection has enough
     fields.
  4. Prepared-state admission and lookup: separate `manifest_lookup`, `cache_hit`,
     `cache_miss_create`, `artifact_write`, and `artifact_register`; reuse source-state/admission
     packets across prepared/native lanes without hiding first-query preparation cost.
  5. Result sink and evidence render: move result-batch/output-capillary/fanout/layout-advisor work
     into the benchmark route path and keep render-heavy website formatting outside hot query
     timing or separately labeled, while preserving certificate material.
  6. Scan/operator attribution and encoded pushdown: keep warm/native scan paths protected because
     they are already sub-ms; split cold `footer_open`, `metadata_verify`, `scan_open`,
     `scenario_scan`, and operator fields before adding provider-admitted Vortex scan
     projection/filter/limit tests.
  7. Benchmark publication refresh: after each completed code/docs/site slice, run artifact
     validators, website/static validators, and claim gates before updating public benchmark
     language. Run the full benchmark suite only at the end of the current optimization batch or
     when explicitly approved for the slice.
  Conversion rules from the component memo:
  - Treat the pasted route grid as source context, not as a new benchmark artifact. The newer
    exclusive safe-writer values in this item are the current authoritative timing basis until a
    full benchmark rerun is approved.
  - Preserve end-to-end route geomeans as the comparison surface. Stage grids explain route cost;
    they do not become alternate product lanes or superiority claims.
  - Every hot-path change must keep ShardLoom rows backed by real ShardLoom runtime execution,
    certificate/evidence material where required, and explicit `fallback_attempted=false` /
    `external_engine_invoked=false` fields.
  - Website benchmark data, static pages, promotion scripts, release validators, and phase-plan
    text must move in the same coherent PR batch when stage semantics or published fields change.
  - Do not rerun the expensive benchmark suite until the current code/docs/site optimization batch is
    complete or the user explicitly approves the rerun.
  Component optimization map:

  | Component | Current attribution posture | Remaining implementation target |
  | --- | --- | --- |
  | Route rows/lane shape | Exclusive ShardLoom stage fields are complete; inclusive compatibility import remains audit-only; sparse query rows are blocked until both scan and operator substages exist. | Keep route totals primary; keep validator, release-script, website schema, and generated artifact contracts aligned as later timing fields move. |
  | Source admission | Current PR batch now adds source-admission packet evidence to prepared/native batch rows and prepared-batch workspace manifests: packet schema, route family, format/schema hash, local path size/mtime/content fingerprint, observed row estimates, artifact-manifest hash, fresh/reuse/mismatch status, and explicit no-fallback fields. Published timing artifacts have not been rerun yet. | After the full current code/docs/site batch is complete, rerun the benchmark suite and refresh the public artifact/page so packet reuse can be interpreted beside route totals without hiding first-query preparation cost. |
  | Prepared/native batch metadata | Prepared/native batch source state now caches the dimension Vortex row count once per session and reuses it across row-count-only prepared/native batch scenarios, including source-state-backed and non-source-state fallback scenarios, with `source_state_dim_rows` and `source_state_dim_row_count_cache_*` evidence. | Extend the same amortization pattern to other metadata-only facts after footer/layout evidence is split from scan timing and covered by validators. |
  | Source read | Current de-overlapped cold `source_read_ms` geomean is 7.80 ms; source read is secondary in 22/120 cold rows. | Split byte acquisition from parse/decode, then add streaming/projected CSV/JSONL reads where scenario certification does not require full optional-column preservation. |
  | Parse/decode | Current cold `source_parse_or_columnar_decode_ms` geomean is 29.48 ms; parse/decode is primary in 37/120 rows and secondary in 61/120 rows. Projected text decode evidence and row-assembly strategy fields now identify scenario-local text paths, and canonical JSONL fast-path parsing skips unselected optional tails instead of scanning every optional block. | Continue splitting lexical parse, type decode, and row assembly timing; optimize parsed row construction without changing decoded-reference correctness. |
  | Source to Vortex array/import | Inclusive compatibility import is 127.94 ms and intentionally overlaps parse/write work; array build itself is not the dominant exclusive stage. | Preserve `compat_import_bundle` as an inclusive audit view; use `source_to_batch`, `batch_to_vortex_array`, and `compat_import_bundle` labels before optimizing any import subpath. |
  | Vortex write/safe artifact | Current dominant exclusive stage: 76.79 ms geomean and primary bottleneck in 83/120 cold rows. The writer now returns stream digest, byte count, and row count from the workspace-safe write outcome so traditional analytics import and computed-result sink certificates do not stat Vortex artifacts immediately after write. | Continue reducing writer cost by reusing layout advisor choices, batching safe artifact emission where permitted, and trimming per-artifact open/close overhead without weakening same-directory staging. |
  | Reopen/verify | Cold attribution still needs finer reopen versus scan boundaries; warm/native query paths should not pay full verification work. | Split footer open, metadata verify, scan open, and scenario scan; prefer Vortex footer/layout metadata for verification where certificate policy admits it. |
  | Prepared lookup/create | Current PR batch emits prepared-state lookup evidence for prepare/batch routes: manifest lookup, cache hit, cache miss create, artifact write, artifact register, replay verification, stable source/schema/layout/certificate attractor key, and explicit no-fallback fields. Published timing artifacts have not been rerun yet. | After the full current code/docs/site batch is complete, rerun the benchmark suite and use the refreshed artifact to optimize cache-hit lookup and first-query creation paths separately. |
  | Vortex scan | Warm/native scan is already sub-ms in the memo and remains small in current rows; cold scan geomean is 2.32 ms. | Protect the fast path and add provider-admitted Vortex scan projection/filter/limit tests before changing scan behavior. |
  | Operator compute | Warm/native compute is tiny and cold operator fields are still absent/zero in places. | Add cold operator attribution so scan, pruning, residual compute, and encoded-kernel wins are separately visible before any encoded-pushdown claim. |
  | Result sink | Current PR batch emits schema-versioned result-sink capillary evidence for requested native Vortex result sinks, no-sink rows, and compatibility-fanout rows: scalar JSON byte/digest evidence, replay digest parity, native Vortex output selection, compatibility fanout selection, metadata-loss status, provider classification, claim boundary, and explicit no-fallback fields. Published timing artifacts have not been rerun yet. | After the full current code/docs/site batch is complete, rerun the benchmark suite and use refreshed route totals to decide whether further shared result-batch/fanout/layout-advisor work is still dominant. |
  | Evidence render | Current cold evidence render geomean is 0.08 ms, while memo warm/native evidence was a visible share of total. | Keep certificate data available, but use compact hot-path evidence or separate website/render formatting outside timed query routes when render work grows. |
  | Total route/publication | Current published route geomeans remain cold 137.71 ms, first query 58.00 ms, batch 8.37 ms, warm 5.57 ms, and native 5.58 ms until rerun. | Publish refreshed route totals only after the full current code/docs/site batch and benchmark rerun; keep unsupported rows out of runtime-ready posture and keep external engines baseline-only. |

  Benchmark-driven hot-path child execution items: these child items convert the component-timing
  research into feasible post-merge implementation slices under this existing 6D item. They are not
  new top-level phase IDs. Route totals remain the comparison surface; stage timings are attribution
  evidence. All slices must preserve ShardLoom-owned execution, Vortex-native boundaries,
  `fallback_attempted=false`, and `external_engine_invoked=false`.

  - [ ] HOTPATH-1 route-lane and row-shape stratification:
    - Concept transfer: experimental design and ecological niches. Treat cold certification,
      prepare-once first query, prepare-once batch, warm prepared, and native Vortex as distinct
      route ecologies instead of forcing one optimizer interpretation across all lanes.
    - Current timing target: all 120-row route families, especially the gap between cold 137.71 ms,
      prepare-once first query 58.00 ms, prepare-once batch 8.37 ms, warm prepared 5.57 ms, and
      native Vortex 5.58 ms.
    - Implementation scope: benchmark scenario catalog, benchmark artifact promotion, website
      stage grid, route capability report, and validators.
    - Work: add route-family stratification fields for cold certification, first-query
      preparation, amortized prepared batch, warm prepared query, and native Vortex query; add
      row-shape/scenario tags for tiny/small/wide/skewed/sparse-null/high-cardinality/
      low-cardinality cases where fixture metadata already supports it; keep end-to-end route
      geomeans primary and stage attribution secondary; make the website distinguish route totals,
      exclusive stage timings, and inclusive audit bundles.
    - Acceptance: every ShardLoom timing row has route-family and scenario-shape metadata, existing
      external baselines remain `external_baseline_only`, and no stage table is presented as a
      separate superiority benchmark.
    - Verification: benchmark artifact completeness validator, benchmark publication claim-gate
      validator, and website readiness/static checks if benchmark display changes.
  - [ ] HOTPATH-2 source-admission prediction packet:
    - Concept transfer: predictive coding and surprise minimization. Admission should become a
      cheap prediction-confirmation step when the source, schema, route, and prepared artifact have
      not changed.
    - Current timing target: source admission around 42 ms in prepared/native context.
    - Implementation scope: source-state creation, prepared/native route helpers, admission
      evidence, benchmark report fields, and route capability report.
    - Work: introduce or reuse an admission packet containing source path, size, mtime, schema
      hash, route family, row estimate, format, and artifact manifest hash; record whether
      admission was a fresh probe, packet reuse, or packet mismatch; reuse admission packets across
      warm prepared and native Vortex lanes without hiding first-query preparation cost; emit
      deterministic diagnostics when packet reuse is rejected.
    - Current PR batch: Rust evidence now emits
      `source_admission_packet_*` on prepared/native batch rows and
      `prepare_batch_source_admission_packet_*` on compatibility prepare-plus-batch rows; the
      workspace manifest persists the packet digest, observed row estimates, artifact-manifest
      hash, and fresh/reuse/mismatch status without adding another source-hash pass.
    - Remaining before rerun: run the focused/broad validators, then refresh benchmark artifacts only
      after the whole current optimization batch is complete or the user explicitly approves.
    - Acceptance: warm/native lanes no longer make source-state work look like query execution,
      first-query route still reports real preparation cost, and packet reuse never bypasses
      certificate-required validation.
    - Verification: focused source-state/admission tests, benchmark artifact validators, and
      no-fallback/evidence field checks.
  - [ ] HOTPATH-3 cold source-read scout path:
    - Concept transfer: compressed sensing and scout sampling. Read only enough to decide the route
      before full source acquisition.
    - Current timing target: de-overlapped cold `source_read_ms` geomean 7.80 ms; secondary
      bottleneck in 22/120 rows.
    - Implementation scope: CSV/JSONL/local source readers, source-read evidence, benchmark report
      fields, and compatibility-import diagnostics.
    - Work: split source-read timing into header/scout read, byte acquisition, and full body read;
      add header/schema/byte-range scout evidence where the format supports it; avoid full source
      body reads for route decisions that can be made from scout metadata; preserve full-artifact
      read paths where shared prepared/native artifact preservation requires all columns.
    - Acceptance: source-read timing is independently auditable from parse/decode, scout reuse is
      explicit in evidence, and no route silently downgrades from certified full-artifact behavior.
    - Verification: focused CSV/JSONL read tests, traditional benchmark harness tests, and
      benchmark artifact completeness validator.
  - [ ] HOTPATH-4 projection-aware parse/decode:
    - Concept transfer: sparse coding and selective attention. Decode only activated fields needed
      for the route, predicate, grouping, result, and certificate obligations.
    - Current timing target: cold `source_parse_or_columnar_decode_ms` geomean 29.48 ms; primary
      bottleneck in 37/120 rows and secondary in 61/120 rows.
    - Implementation scope: CSV/JSONL parser paths, typed decode, row assembly, source evidence,
      and decoded-reference correctness tests.
    - Work: split parse/decode evidence into lexical parse, type decode, and row assembly; add
      projection-aware CSV/JSONL decode paths for scenario-local routes; avoid constructing unused
      row fields when the route and artifact policy do not require them; retain full optional-column
      preservation for shared prepared/native artifacts. Current PR progress: report fields expose
      projected text decode and row-assembly strategy, and canonical JSONL fast-path parsing returns
      after the selected optional boundary for core-only, nullable-only, partition-date, dirty, and
      nested projections instead of scanning unselected optional tail fields.
    - Acceptance: parse/decode reduction is visible as a separate stage, decoded-reference
      correctness remains unchanged, and null, empty, sparse, and high-cardinality cases remain
      covered.
    - Verification: focused parser tests, decoded-reference comparison tests, and traditional
      benchmark harness tests.
  - [ ] HOTPATH-5 source-to-Vortex-array shape preservation:
    - Concept transfer: morphological computation. Preserve the physical shape of source data so
      the data layout does part of the work.
    - Current timing target: source-to-Vortex array is already near zero in prepared lanes, but the
      inclusive compatibility-import audit bundle remains large and can be misread.
    - Implementation scope: Vortex array build evidence, compatibility-import bundle labels,
      promotion script, website attribution table, and validators.
    - Work: keep `source_to_batch`, `batch_to_vortex_array`, and `compat_import_bundle` distinct;
      add a regression guard so array build cannot quietly become row-object construction; preserve
      inclusive compatibility import as audit context, not exclusive timing; document that array
      build is not the current dominant bottleneck.
    - Acceptance: array build remains near-zero on known fixtures and website/artifacts no longer
      imply inclusive import is a standalone stage.
    - Verification: benchmark artifact validators, focused compatibility-import report test, and
      website readiness checks if labels change.
  - [ ] HOTPATH-6 Vortex write centrifuge:
    - Concept transfer: centrifugation, vascular flow, and irreversible-work minimization. Separate
      write work by density, permanence, proof need, and reuse probability.
    - Current timing target: dominant cold stage, `vortex_write_ms` geomean 76.79 ms; primary
      bottleneck in 83/120 rows.
    - Implementation scope: `shardloom-vortex` traditional analytics writer, shared Vortex ingest
      helper, workspace-safe writer, digest/metadata capture, and Vortex replay verification.
    - Work: coalesce write, digest, byte count, row count, and metadata capture into one pass where
      policy permits; avoid artifact readback/reopen when writer-returned metadata and certificate
      policy are enough; reuse layout/write advisor decisions across repeated compatible artifacts;
      reduce per-artifact open/close overhead without bypassing same-directory workspace-safe
      staging; keep native Vortex output as the highest-fidelity target. Current PR progress:
      writer-returned digest, byte count, and row count now feed traditional analytics import
      report/certificate fields and computed-result sink certificates, avoiding immediate
      post-write Vortex artifact stat/readback for those metadata values while preserving replay
      verification.
    - Acceptance: cold route improvement comes primarily through `vortex_write_ms`, artifact digest,
      workspace safety, and replay verification remain intact, and no compatibility output is
      reported as native Vortex execution.
    - Verification: Vortex write/reopen tests, workspace-safe writer tests, traditional analytics
      benchmark harness tests, and benchmark artifact validators.
  - [ ] HOTPATH-7 reopen/verify immune-recognition path:
    - Concept transfer: immune recognition. Known artifact signatures should be recognized quickly
      before expensive verification is attempted.
    - Current timing target: reopen/verify is hidden or sparse in the current grid and needs clearer
      separation from scan.
    - Implementation scope: Vortex artifact footer/open verification, certificate evidence,
      benchmark timing fields, and promotion script.
    - Work: split footer open, metadata verify, scan open, and scenario scan timings; use
      footer/layout/certificate metadata for fast recognition where policy admits it; record when
      full reopen/scan verification was required and why; keep deterministic diagnostics for
      missing or stale verification evidence.
    - Acceptance: reopen/verify is not conflated with Vortex scan, and warm/native paths do not pay
      full cold verification cost unless required.
    - Verification: Vortex replay verification tests, benchmark artifact completeness validator,
      and claim-gate validator.
  - [ ] HOTPATH-8 prepared-state attractor lookup:
    - Concept transfer: Hopfield networks and attractor basins. Source hash, schema hash, route
      hash, layout policy, and certificate state should converge directly to the prepared artifact
      or a minimal repair plan.
    - Current timing target: prepare-once first-query `Prepared lookup/create` 51.84 ms; batch
      amortized 2.59 ms.
    - Implementation scope: prepared-state lookup/create path, prepared artifact manifest,
      source-state evidence, benchmark fields, and Python/context helpers.
    - Work: split prepared lifecycle timing into manifest lookup, cache hit, cache miss create,
      artifact write, artifact register, and replay verification; add content-addressed prepared
      artifact keys using source/schema/route/layout/certificate state; make cache hits cheap and
      explicitly evidenced; keep first-query creation cost visible and separate from warm query
      execution.
    - Current PR batch: Rust evidence now emits
      `prepare_batch_prepared_state_lookup_*` fields on compatibility prepare-plus-batch rows,
      including manifest lookup, cache hit, cache miss create, artifact write, artifact register,
      replay verification, stable prepared-state attractor key, claim boundary, and explicit
      no-fallback/external-engine fields. The workspace-reuse regression covers first-run create,
      second-run manifest hit, and source-change refresh.
    - Remaining before rerun: refresh benchmark artifacts only after the whole current optimization
      batch is complete or the user explicitly approves, then use the new fields to separate cache-hit
      optimization from first-query creation optimization.
    - Acceptance: prepare-once first-query route explains where the 51.84 ms is spent,
      prepare-once batch remains honest about amortization, and prepared reuse never bypasses
      changed-source detection.
    - Verification: prepared artifact lookup/reuse tests, Python/context prepared-route tests where
      applicable, and benchmark artifact validators.
  - [ ] HOTPATH-9 prepared-state regeneration repair:
    - Concept transfer: starfish/planarian regeneration and positional memory. Changed prepared
      artifacts should repair affected segments instead of rebuilding the whole body.
    - Current timing target: prepared lookup/create and Vortex write during first-query preparation.
    - Implementation scope: prepared artifact manifest, segment/column invalidation evidence,
      source-state delta detection, and prepared replay.
    - Work: add manifest evidence for segment, column, schema, and certificate dependencies; detect
      which prepared-state regions are invalidated by source changes; rebuild only invalidated
      segments where correctness and Vortex artifact policy permit; fail explicitly when partial
      repair is unsupported.
    - Acceptance: incremental prepared repair is visible as a distinct route state, full rebuild
      remains available and explicit when required, and no stale segment can be reused silently.
    - Verification: prepared repair/invalidation tests, replay correctness tests, and no-fallback
      evidence checks.
  - [ ] HOTPATH-10 Vortex scan IO-aware protection:
    - Concept transfer: FlashAttention-style IO-aware tiling. Optimize data movement before
      arithmetic.
    - Current timing target: Vortex scan is already fast, about 0.25 ms warm/native and 2.32 ms
      cold.
    - Implementation scope: Vortex scan timing, projection/filter/limit admission, decoded-value
      counters, and scan diagnostics.
    - Work: add scan counters for bytes touched, segments skipped, columns touched, and decoded
      values; protect the fast path with regression thresholds on known benchmark fixtures; add
      provider-admitted Vortex scan projection/filter/limit tests before changing scan behavior;
      preserve segment pruning and late materialization diagnostics.
    - Acceptance: scan remains sub-ms on warm/native fixtures, and any scan optimization proves
      reduced data movement rather than only wall-clock variance.
    - Verification: Vortex scan tests, encoded/pruning diagnostics tests, and benchmark artifact
      validators.
  - [ ] HOTPATH-11 operator micro-kernel discovery:
    - Concept transfer: AlphaDev/AlphaEvolve-style algorithm discovery for tiny repeated kernels.
      Operator compute is already small but will become more important after sink/evidence/write
      shrink.
    - Current timing target: operator compute about 0.25 ms warm/native.
    - Implementation scope: encoded operator kernels, null-mask handling, group/count/min/max/hash
      hot loops, correctness tests, and microbenchmarks.
    - Work: inventory hot operator loops used by traditional benchmark scenarios; add candidate
      branchless or layout-aware kernels for comparisons, counts, min/max, null-mask combination,
      and hash accumulation; promote only kernels that beat the existing path in reproducible
      focused benchmarks; keep decoded-reference tests for correctness.
    - Acceptance: kernel changes are correctness-backed and benchmark-backed, and unsupported
      encoded cases still fail explicitly rather than falling back.
    - Verification: encoded operator tests over empty, null, sparse, dense, low-cardinality, and
      high-cardinality inputs; focused microbenchmarks; workspace tests for affected crates.
  - [ ] HOTPATH-12 result-sink capillary routing:
    - Concept transfer: capillary networks and pressure routing. Small scalar results, report rows,
      native Vortex output, compatibility exports, and certificate attachments should not share one
      heavy sink path.
    - Current timing target: result sink around 1.90-2.05 ms; a large share of warm/native totals.
    - Implementation scope: result-batch state, output-capillary planner, fanout conversion DAG,
      layout/write advisor, benchmark route sink path, and evidence fields.
    - Current PR batch: adds `result_sink_capillary_*` evidence for compatibility-import and
      prepared/native benchmark routes, including scalar result JSON bytes, compact result and
      replay digests, native Vortex output selection, compatibility fanout selection, metadata-loss
      status, provider classification, claim boundary, and explicit no-fallback/no-external-engine
      fields. Direct transient/no-sink rows emit deterministic `not_requested` evidence. Focused
      Rust tests cover direct no-sink, native result sink replay, prepared/native result sink replay,
      and compatibility export fanout. Published timing artifacts have not been rerun yet.
    - Work: route benchmark result sinks through shared result-batch/output-capillary
      infrastructure; separate scalar/small-result JSON output from native Vortex output and
      compatibility fanout; avoid repeated JSON/string materialization in timed query routes; emit
      sink materialization and metadata-loss evidence for compatibility outputs.
    - Acceptance: warm/native route total drops through sink reduction without losing output
      evidence, and native Vortex output remains distinct from compatibility export.
    - Verification: SQL/local-source output tests where applicable, traditional benchmark sink
      tests, and benchmark artifact validators.
  - [ ] HOTPATH-13 evidence-render proof regeneration:
    - Concept transfer: regeneration and positional memory. Store compact proof tissue once and
      regenerate human-readable evidence lazily.
    - Current timing target: evidence render around 2.42-2.57 ms in warm/native grid; cold currently
      0.08 ms.
    - Implementation scope: execution certificate facts, benchmark evidence JSON, website render
      path, CLI/report formatting, and promotion script.
    - Work: split compact machine evidence emission from human/website evidence rendering; keep
      certificate facts in the hot route but move prose/table expansion outside timed query work or
      label it separately; add stable evidence schema fields so website rendering can regenerate
      from compact facts; preserve claim-boundary and no-fallback fields.
    - Acceptance: warm/native routes do not spend most of total time rendering human evidence, and
      website and CLI evidence remain complete and deterministic.
    - Verification: evidence schema tests, website readiness/static checks, and benchmark artifact
      completeness validator.
  - [ ] HOTPATH-14 total-route Amdahl gate:
    - Concept transfer: Amdahl's law and systems biology. Optimize by route share, not intuition.
    - Current timing target: total route geomeans remain cold 137.71 ms, first query 58.00 ms,
      batch 8.37 ms, warm 5.57 ms, and native 5.58 ms until a rerun.
    - Implementation scope: benchmark harness, promotion script, website benchmark page, release
      validators, and phase-plan/ledger text after completed slices.
    - Work: after each child slice, record which route total and which stage should move; do not
      publish new public performance language until the full benchmark rerun is complete; keep
      external rows baseline-only and unsupported rows visibly non-runtime-ready unless runtime
      evidence exists; add a route-share dashboard table showing remaining route-dominant stages
      after each refresh.
    - Acceptance: every optimization has a measurable target and post-change route-total
      interpretation, route totals remain the only comparative performance surface, and claim
      boundaries stay aligned with benchmark evidence.
    - Verification: full benchmark rerun only when approved for the completed optimization batch,
      benchmark publication claim-gate validator, website readiness/static checks, and
      `git diff --check`.

  Current PR batch completed through code/docs/site evidence slices: HOTPATH-6, HOTPATH-4,
  HOTPATH-2, HOTPATH-8, and HOTPATH-12. Suggested remaining execution order: HOTPATH-13,
  HOTPATH-3, HOTPATH-7, HOTPATH-10, HOTPATH-11, HOTPATH-9, HOTPATH-1, HOTPATH-5, then HOTPATH-14.
  Keep this order flexible only when new benchmark evidence changes the dominant route
  share. Non-goals for all child items: no Spark, DataFusion, DuckDB, Polars, Velox, or Vortex
  query-engine fallback; no hidden fast mode that skips claim-required evidence; no public
  superiority, production, broad SQL/DataFrame, object-store/table, or Spark-replacement claim from
  a single optimization slice; no benchmark rerun unless explicitly approved for the slice or final
  optimization batch.
  User-visible surface: benchmark route totals and stage attribution, CLI traditional-analytics
  routes, Python/context prepared/native route helpers, result-sink evidence, route capability
  reports, and release-readiness benchmark validators.
  Implementation scope: `shardloom-vortex/src/traditional_analytics.rs`,
  `shardloom-vortex/src/vortex_ingest.rs` if shared helper extraction is needed,
  `shardloom-cli/src/sql_local_source_runtime.rs` for source-read/parse timing and cold local-source
  reader work, `scripts/promote_benchmark_artifact.py` and benchmark/static validators when stage
  semantics change, `docs/architecture/phased-execution-plan.md` when the active queue drifts,
  Python/CLI docs when user behavior changes, website benchmark components/data, and
  benchmark/release evidence artifacts after runtime changes.
  Evidence required: focused tests or smokes that prove Vortex write/read/replay parity, workspace
  safety, artifact digests, route ledger zero deltas, no-fallback/no-external-engine fields,
  exclusive stage-sum/residual parity when attribution semantics change, benchmark artifact
  completeness, copy-budget/layout fields updated when measurements improve, and a refreshed local
  benchmark after all code/docs/site changes and before any public performance claim.
  Acceptance: route behavior and evidence remain identical or stricter, fixable ShardLoom-owned hot
  loops avoid unnecessary reads, decodes, allocations, string clones, or materialization, hot-stage
  timing fields still reproduce route totals, exclusive and inclusive stage views are defensible,
  and the refreshed benchmark run reports no ShardLoom fallback/external-engine flags.
  Verification: focused Rust tests for traditional analytics Vortex I/O paths, `cargo fmt --all
  -- --check`, relevant `cargo test` package/feature targets, benchmark artifact validators
  (`scripts/check_benchmark_artifact_completeness.py`,
  `scripts/check_benchmark_publication_claim_gate.py`,
  `scripts/check_benchmark_constitution.py`), website readiness/static checks when benchmark pages
  change, and `git diff --check`.
  Non-goals: no Spark/DataFusion/Polars/DuckDB fallback, no public performance/superiority claim
  from this slice alone, no hidden fast mode that skips claim-required evidence, no broad object-store
  or production claim, and no dependency expansion unless separately justified.
  Dependencies/blockers: refreshed post-merge benchmark artifacts, route-total/stage timing parity,
  no-fallback certificate evidence, copy-budget/layout measurements, and follow-up admission work
  for result-sink/evidence overhead, scan runtime/session reuse, and encoded operator hot paths.
  Claim boundary: scoped local runtime optimization only; performance, production, broad
  SQL/DataFrame parity, object-store/table, and Spark-replacement claims remain blocked until the
  refreshed workload-scoped correctness, Native I/O, benchmark, release, and claim-gate evidence
  exists.
- [ ] GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar: Broad SQL grammar over
  Vortex-normalized runtime paths.
  Current state: admitted scoped SQL syntax families and their evidence rows live in
  `docs/status/admitted-semantics-matrix.json` and the completed ledger. The live remaining
  blockers are the non-admitted broad grammar families that still need either ShardLoom-owned
  runtime promotion or deterministic diagnostics: non-UTC/timezone semantics, locale/collation,
  arbitrary interval arithmetic outside scoped temporal helpers, complex equality/accessors/casts,
  nested source decoding and flat sinks beyond scoped result-boundary projections,
  variant/union-dtype shapes, broad binary source dtype decoding/ordering, local Vortex typed
  decimal output, Avro/ORC typed decimal sinks, broad ANSI decimal coercion/exponent notation,
  scalar-left multi-column subqueries, outer references outside admitted column-to-column
  correlations, unbound source aliases, and remaining broad ANSI subquery families.
  Next slice outcome: choose the next broad SQL grammar family from the remaining runtime blockers;
  likely candidates are timezone/locale blocker refinement, broad binary source dtype refinement,
  complex access/equality follow-through after a dedicated semantics contract, local Vortex typed
  decimal output once Vortex writer/reopen evidence is available, or another front-door parity gap
  only after the runtime route is already admitted.
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
next session ahead of 6E/6F unless it identifies a concrete release, safety, security, or
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

Current non-runtime sequence: deferred behind 6E/6F and the runtime-readiness queue unless a
specific blocker must be pulled forward with explicit justification. Completed non-runtime history
belongs in `docs/architecture/phased-execution-completed-ledger.md`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

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
