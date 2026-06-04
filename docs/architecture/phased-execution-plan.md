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
  Source: 2026-06-03 local code/text benchmark research against the current promoted artifact and
  current branch sources; `docs/architecture/phased-execution-plan.md`,
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
  safe-writer and exclusive attribution figures above for current ordering.
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
  3. Cold source parse and read: reduce `source_parse_or_columnar_decode_ms` and de-overlapped
     `source_read_ms` by splitting `bytes_read`, `lex_parse`, `type_decode`, and `row_assembly`
     evidence, then adding streaming/projected CSV/JSONL paths where scenario-local certification
     does not require full optional-column artifact preservation, while retaining full-artifact
     paths for shared prepared/native artifacts.
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
  Component optimization map:

  | Component | Current attribution posture | Remaining implementation target |
  | --- | --- | --- |
  | Route rows/stage ledger | Exclusive ShardLoom stage fields are complete; inclusive compatibility import remains audit-only. | Keep validator, release-script, website schema, and generated artifact contracts aligned as later timing fields move. |
  | Source admission | Warm/native admission evidence can still look like query cost when source-state work repeats. | Add/reuse admission packets for source stat, schema, row estimate, mtime/size, and fingerprint across prepared/native lanes. |
  | Cold source parse/read | Current exclusive cold geomeans show parse/decode ahead of read: 29.48 ms parse/decode, 7.80 ms de-overlapped read. | Split bytes read, lexical parse, type decode, and row assembly, then add streaming/projected readers where certificate scope permits. |
  | Source to Vortex array/import | Inclusive compatibility import is 127.94 ms and intentionally overlaps parse/write work. | Keep it as an inclusive compatibility audit bundle; do not use it as an exclusive optimization target. |
  | Vortex write/safe artifact | Current dominant exclusive stage: 76.79 ms geomean and primary bottleneck in 83/120 cold rows. | Coalesce write/digest/metadata capture, reduce reopen/readback where policy allows, reuse layout advisor choices, and trim per-artifact open/close overhead. |
  | Reopen/verify and scan | Warm/native scan is already tiny; cold attribution still needs finer reopen versus scan boundaries. | Split footer open, metadata verify, scan open, and scenario scan before introducing provider-admitted projection/filter/limit tests. |
  | Prepared lookup/create | Prepare-once first query and batch paths need lifecycle separation rather than hidden amortization. | Emit manifest lookup, cache hit/miss create, artifact write, and artifact register evidence, then optimize hit and creation paths separately. |
  | Result sink/evidence render | Warm/native totals are increasingly dominated by sink/evidence rather than scan/compute. | Route result-batch/output-capillary/fanout/layout-advisor work into benchmark paths and keep website formatting outside hot timing or separately labeled. |
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
