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

Updated after the GAR-RUNTIME-IMPL-6C user-surface graduation matrix closeout.

1. `GAR-RUNTIME-IMPL-6E` automatic dynamic preparation is closed through completed 6E-4 ledger
   evidence.
2. `GAR-RUNTIME-IMPL-6F` output/fanout conversion and sink-driven performance promotion is closed
   through completed 6F-5 ledger evidence.
3. `GAR-RUNTIME-IMPL-6C` user-surface graduation matrix is closed through completed 6C ledger
   evidence.
4. `GAR-RUNTIME-IMPL-6D:gap-family-burn-down` is the next unchecked runtime item, to split
   remaining true runtime blockers into implementable slices.
5. Remaining `GAR-RUNTIME-IMPL-6D:last_order.*` SQL/Python/DataFrame/object-store/effect/live/spill
   breadth work after the route/reuse/output contracts are landed.
6. Residual 4/5-series internal-engine backstops and the 6A completion gate after the active
   runtime queue has reduced the blockers it depends on.

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
queues below. After the 6E automatic preparation/reuse closeout and the 6F output/fanout closeout,
the current runtime sequence is `GAR-RUNTIME-IMPL-6D:gap-family-burn-down`, which classifies the
remaining true blocker
families, then the remaining `GAR-RUNTIME-IMPL-6D:last_order.*` user-surface breadth. Pull a 6D
breadth item forward only when it unblocks blocker-family classification or prevents a misleading
runtime posture. The
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

- [ ] GAR-RUNTIME-IMPL-6D:gap-family-burn-down true runtime gap family burn-down plan.
  Source: the 38 unchecked global architecture review rows and the runtime gap families listed in
  `docs/architecture/repo-readiness-user-surface-audit.md`.
  Current state: the global review still has 38 unchecked rows; the completion gate is explicitly
  blocked because broad architectural items have not all been converted into runtime evidence,
  deterministic unsupported diagnostics, or reclassified out-of-scope surfaces with validators.
  Next slice outcome: split each broad global blocker into family-owned runtime implementation
  slices with acceptance criteria for supported behavior, deterministic blockers, validators,
  docs/website parity, and benchmark/release evidence when relevant.
  Runtime enablement: this is a runtime-safety planning gate that converts broad blocker language
  into runnable implementation slices before broad SQL/DataFrame/object-store/effect/live/spill
  work resumes.
  User-visible surface: capability discovery, diagnostics, Python/context workflows, CLI runtime
  commands, release/readiness gates, benchmark/readiness docs, and website/use-case claims.
  Implementation scope: prioritize SQL/DataFrame runtime breadth, Vortex source/sink/operator
  coverage, object-store/lakehouse execution, table/catalog commits, streaming/spill/retry runtime,
  and package/deploy readiness according to current claim risk and user value.
  Evidence required: each split item names the owning module, public surface, no-fallback invariant,
  validator, and completion-gate field it is expected to reduce.
  Acceptance: global review blocker count decreases only when a family has implementation,
  deterministic admission evidence, or a documented reclassification with validator coverage.
  Verification:
  ```bash
  python3 scripts/check_compute_engine_completion_gate.py --allow-incomplete --output target/compute-engine-completion-gate.json
  python3 scripts/check_release_architecture_tracker.py --allow-blocked
  python3 scripts/final_release_rehearsal.py --allow-blocked
  cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
  cargo test -p shardloom-contract-tests --test release_readiness_metadata
  git diff --check
  ```
  Non-goals: no unsupported work hidden as supported, no superiority/performance claims without
  CG-5/CG-6 evidence, no fallback engines, no release publication, and no bundling every runtime
  family into one oversized implementation PR.
  Dependencies/blockers: depends on the 6B audit, the 6C graduation matrix, the active global
  architecture review inventory, and family-specific RFC/skill routing before runtime promotion.
  Claim boundary: blocker burn-down is claimable only for families with concrete implementation
  evidence or validated unsupported diagnostics; placeholder artifacts do not satisfy runtime
  support.
  Fallback boundary: runtime gap closure must keep ShardLoom execution native and explicit; DuckDB,
  Polars, Spark, DataFusion, Velox, and Vortex query-engine integrations remain comparison or
  external-boundary surfaces only, never fallback execution.
  Ledger rule: move completed details and validation output to the completed ledger.

#### GAR-RUNTIME-IMPL-6D - Runtime-Ready User Surface And Benchmark-Range Completion

Ordering note (updated 2026-06-02 after PR #1037): this remains the user-surface and
benchmark-range breadth queue, but it follows 6E, 6F, and the narrow 6C/gap-family classification
work unless a specific 6D item blocks those route/reuse/output boundary items. Resume the last-order
checklist here after the automatic preparation/reuse and output fanout contracts are landed, or pull
a narrow 6D blocker forward with explicit justification.

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

- Closed: For compatibility imports, expose an intuitive user route for
  `compatibility_import_certified -> prepared_vortex` and `shardloom-prepare-batch` so users can
  start from CSV/JSONL/Parquet/Arrow IPC/Avro/ORC, prepare once, run benchmark-range scenarios from
  prepared Vortex artifacts, and understand whether timing includes preparation. This is the primary
  non-Vortex input-to-Vortex transition and should be visible in reports instead of hidden behind a
  generic read helper. Closed by the context/session `prepare_vortex(..., workspace=...)` route
  handle and `CompatibilityPreparedVortexRoute`/`PreparedVortexQuery` tests.
- Closed: For native `.vortex` input, expose a user route that runs the same native/prepared Vortex
  runtime family used by benchmark rows, not piecemeal artificial helpers. The surface must make
  source, selected execution mode, scenario/operator, memory/parallelism, and result-sink choice
  explicit. Closed by `ctx.native_vortex_route(...)` / `session.native_vortex_route(...)`,
  `NativeVortexRoute`/`NativeVortexQuery`, and CLI/client resource-policy arguments for
  `traditional-analytics-vortex-run` and `traditional-analytics-vortex-batch-run`.
- Closed: For outputs, ensure every admitted benchmark-range route has at least one clear output option:
  machine-readable report, bounded preview, local compatibility output, native Vortex output,
  result-sink replay proof, or fanout. Missing output wiring is a runtime-output checklist item, not
  a vague unsupported user surface. Closed by the user-route capability output-option classifier,
  admitted-route output maps, and fail-closed route-report regression tests.
- Closed: Reclassify engine-capable but unwired front-door gaps away from generic `unsupported` language
  in the Python context matrices, parity validator payload, benchmark coverage table, and docs.
  Use precise labels such as `front_door_connection_pending`, `output_route_pending`,
  `claim_evidence_pending`, or `benchmark_publication_pending`. Closed by
  `FrontDoorParityRow.runtime_gap_status`, parity/user-route/gap-inventory report fields, the
  `benchmark_publication_pending` route status, and docs that distinguish front-door gaps from
  unsupported engine paths.
- Closed: Add regression tests that fail if any benchmark-range local ShardLoom route reports
  `unsupported` merely because SQL/Python/DataFrame/context/session wiring is missing. Closed by
  user-route, local-file benchmark, parity, and runtime-gap inventory fail-closed tests.
- Closed: Keep claim boundaries strict: performance equivalence, production support, Spark
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

#### GAR-RUNTIME-IMPL-4/6A - Residual Completion Gate And Internal Backstop Queue
Current runtime ordering note (updated 2026-06-02): the completed engine-internal queue remains
recorded here as backstop context, but the active runtime sequence above now intentionally works 6E,
then 6F, then the 6C/gap-family classification step, then the remaining 6D user-surface breadth
before this residual completion/backstop queue unless a specific internal-engine item blocks the
active route/reuse/output work. The
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
route/reuse/output, user-surface classification, and user-surface breadth sequence, not the current
top runtime priority.
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
