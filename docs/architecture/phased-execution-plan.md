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
implementable as one coherent implementation batch or explicitly marked `report-only`,
`planning-only`, or `diagnostic-only`.

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
  `docs/architecture/website-current-state-public-reference.md`.
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

- [ ] `PERF-DESIGN-2` - Encoded-native operator promotion and stage-timing attribution cleanup.
- [ ] `PERF-DESIGN-4R` - PulseWeave session/runtime coalescing follow-through.
- [ ] `PERF-DESIGN-1R` - Dynamic prepared-state reuse and role-repair optimization follow-through.
- [ ] `PERF-DESIGN-5R` - Capillary preparation spine write/reopen/copy optimization follow-through.
- [ ] `PERF-DESIGN-6R` - Dynamic source-adapter parse/decode and scout-ingress optimization follow-through.
- [ ] `PERF-DESIGN-3` - Publication-proof sink/evidence pipeline optimization.

Benchmark timing evidence snapshot for the `PERF-DESIGN-*` queue:

- Source artifact:
  `website/assets/benchmarks/latest/published-row-runs/rows-b81bbdc3217d3209`, 1,920
  published rows, including 1,200 ShardLoom rows and 720 external-baseline rows. External rows are
  baselines only, never fallback execution. The ShardLoom-family row set includes
  `shardloom`, `shardloom-vortex`, `shardloom-prepared-vortex`, and
  `shardloom-prepare-batch` engine IDs; filtering only `engine=shardloom` selects the 240-row cold
  certified lane, not the full 1,200-row ShardLoom timing surface.
- Prepared role-repair evidence:
  `website-public/assets/benchmarks/latest/prepare-batch-role-repair-evidence.json` records 5
  targeted runs and 10 rows across full prepare, manifest reuse, fact role repair, dim role repair,
  and CDC-delta role repair. It is targeted runtime evidence for the completed `PERF-DESIGN-1`
  item, not a full benchmark-suite refresh or performance claim.
- ShardLoom timing surfaces: 600 `hot_runtime` rows and 600 `publication_proof` rows.
- Claim-gate split: 600 `publication_proof` rows are `claim_grade`; 600 `hot_runtime`
  `metadata_sink` rows are intentionally `not_claim_grade` compact timing rows, not runtime
  failures. The benchmark page now labels this timing-surface split explicitly.
- Hot/runtime lane shape: cold certified route geomean is about `63.13 ms`; native Vortex,
  warm-prepared, and prepare-once-batch hot geomeans are about `0.11-0.12 ms`. The refreshed
  PERF-DESIGN-4 CSV hot-runtime rows report
  `session_route_used=true`, `process_spawn_count=1`, and `batch_process_wall_shared=true`, so
  process-wall measurements remain visible without redefining hot route totals.
- Cold certified route bottlenecks: `source_parse_or_columnar_decode_ms` and `vortex_write_ms`
  remain material diagnostic targets; refreshed JSONL outliers reach `221.95 ms` hot route total
  with about `174.27 ms` source parse/decode, but source-read scout timing is now complete and row
  assembly is measured at `0.0 ms` for the refreshed JSONL/AVRO source-adapter rows.
- Operator posture: all 600 hot/runtime ShardLoom rows remain `residual_native` or
  `materialized_temporary`; multi-key group by, nested JSON scan, high-cardinality string
  group/distinct, join+aggregate, and group-by aggregation are the highest measured
  `operator_compute_ms` families.
- Local optimization rerun guardrail: the diagnostic artifacts under
  `target/perf-design-2-operator-attribution/` are not promoted evidence unless the current-code
  rerun is both configuration-comparable and faster or attribution-correct. A June 12, 2026
  current-code CSV rerun over group-by/multi-key/join/string/hash scenarios repeated higher
  hot-query wall timings than the published rows, so it remains a regression/attribution
  investigation input only. Do not refresh latest website benchmark rows from those artifacts.
- Optimization-target validator posture: `python3 scripts/check_benchmark_optimization_targets.py`
  over `website-public/assets/benchmarks/latest/benchmark-results.json` now loads full summary-only
  row chunks and reports additive hot-runtime targets for `jsonl_parse_decode_hot_runtime`,
  `avro_hot_runtime_outliers`, `prepared_state_lookup_or_create`,
  `vortex_write_and_reopen_verify`, and `source_read_scout_timing`. `operator_materialization`
  remains visible but is timing-contract-blocked because current `operator_compute_ms` fields are
  excluded diagnostic timing for the selected hot/runtime surface rather than additive route-share
  evidence. The refreshed CSV metadata-sink rows report manifest reuse with
  `prepare_batch_preparation_millis=0.0`, `prepare_batch_prepared_state_lookup_or_create_millis`
  about `0.517 ms`, two reused artifacts, zero rewritten artifacts, and no Vortex reopen.
- Publication/proof posture: prepared/native publication-proof geomeans sit around `4.7-7.2 ms`,
  while prepare-once-first-query publication proof is about `42.91 ms` because it includes
  first-query preparation plus result-sink/evidence work.
- Claim boundary: these numbers are current local benchmark evidence and optimization direction,
  not performance, production, Spark-displacement, superiority, or package-release claims.

Lane-to-design mapping from the 1,200 ShardLoom-family rows:

- `PERF-DESIGN-1` is closed for correctness and evidence, but reopened as `PERF-DESIGN-1R` for
  optimization follow-through: SourceState and VortexPreparedState reuse unchanged fact/dim/CDC
  artifacts and repair changed roles with targeted evidence, but the benchmark lanes still need a
  dynamic reuse policy that avoids manifest/digest/write work when a run-local capillary has already
  proven the same source/prepared-state dependency set.
- `PERF-DESIGN-6` is closed for attribution, but reopened as `PERF-DESIGN-6R` for optimization
  follow-through: `jsonl_parse_decode_hot_runtime`, `avro_hot_runtime_outliers`, and
  `source_read_scout_timing` remain measured diagnostic targets. The source adapters expose
  projection-aware scout plans, byte acquisition, typed decode, row assembly, and columnar handoff
  stages; the next work must reduce typed text decode/source parse cost through dynamic scout
  admission and capillary chunk sizing rather than hiding it inside route totals.
- `PERF-DESIGN-4` is closed for session evidence, but reopened as `PERF-DESIGN-4R` for
  optimization follow-through: repeated warm/native/prepared benchmark groups report caller-owned
  session route use, process-spawn count, shared batch wall timing, and no hidden daemon/global
  cache posture separately from hot route totals. The next work should let PulseWeave coalesce
  admitted local scenario groups inside the existing session/runtime envelope so repeated
  route-open, scan-open, and result assembly overhead is bounded by evidence rather than repeated by
  default.
- `PERF-DESIGN-5` is closed for timing attribution, but reopened as `PERF-DESIGN-5R` for
  optimization follow-through: prepare-batch lifecycle timing now separates
  `prepared_state_lookup_or_create`, narrow preparation/create timing, and full prepare route total
  instead of sourcing narrow preparation from `total_runtime_micros`. Targeted CSV metadata-sink
  rows show manifest metadata verification with no new writer/reopen work; the next work should
  reduce cold/prepared write, reopen, and copy cost through capillary preparation windows, writer
  context reuse, and dynamic metadata-first verification admission.
- `PERF-DESIGN-2`: all hot/runtime ShardLoom rows still report `residual_native` or
  `materialized_temporary` operator posture, and the highest operator families are multi-key group
  by, nested JSON scan, high-cardinality string group/distinct, join+aggregate, and group-by
  aggregation. Route-share attribution now fails closed when residual operator timing is excluded
  from or non-additive to the selected hot/runtime surface, so the next global design change is to
  promote the highest-value family to encoded-native execution with decoded-reference parity before
  ranking operator timing as additive route-share evidence.
- `PERF-DESIGN-3`: `publication_proof` rows intentionally include result-sink and evidence-render
  work; prepared publication rows are slower than hot-runtime rows because they do more proof/output
  work. The global design change is to add an incremental publication-proof artifact path so
  unchanged sink/replay/evidence records can be reused while keeping proof totals separate from
  hot-runtime totals.

Timing aggregation guardrail:

- Route optimization analysis must group by `(route_lane_id, timing_surface)` and honor
  `route_timing_stage_inclusion_classes` before ranking stages. Diagnostic stage fields may remain
  present on a row even when excluded from the selected surface; they must not silently redefine hot
  runtime totals or trigger a performance claim.
- The generated review command for this mapping is:
  `python3 scripts/check_benchmark_optimization_targets.py --artifact website-public/assets/benchmarks/latest/benchmark-results.json --output target/benchmark-optimization-targets-review.json --top-n 12`.
  The report is diagnostic evidence only and does not authorize public performance, production,
  package-release, or Spark-displacement claims.

### PERF-DESIGN-2 - Encoded-native operator promotion and stage-timing attribution cleanup

- Source: PR #1174 route rows; current published row chunks
  `website/assets/benchmarks/latest/published-row-runs/rows-b81bbdc3217d3209`; operator
  mode inventory fields; `operator_hot_path_candidate`; `route_timing_exclusive_stage_sum_ms`;
  `route_timing_exclusive_residual_ms`.
- Current state: prepared/native hot-route query totals are around `0.11-0.12 ms` geomean, but
  rows still report `operator_execution_mode=residual_native` for 480 hot/runtime rows and
  `materialized_temporary` for 120 cold compatibility rows. Highest measured operator families are
  multi-key group by, nested JSON field scan, high-cardinality string group/distinct,
  join+aggregate, and group-by aggregation. Diagnostic source/import/write fields can remain
  present on native/warm rows while excluded from authoritative hot totals, so stage attribution
  must stay explicit before using those fields for optimization ranking. Route-share and the
  optimization-target validator now reject excluded or non-additive timing fields before selecting a
  target, leaving residual operator promotion as a real encoded-native implementation item instead
  of a route-share labeling problem. A local current-code optimization rerun exposed a
  query-wall/stage-timing inconsistency and slower repeated CSV operator rows; treat that as an open
  timing-contract issue, not a publishable performance refresh.
- Next slice outcome: select the highest-value operator family from the benchmark scenarios and
  promote it from residual/materialized execution toward encoded-native execution with correctness
  evidence, while normalizing exclusive stage timing so diagnostic stage costs cannot contradict
  authoritative route totals and current-code reruns cannot regress silently.
- User-visible surface: benchmark route-share attribution, explain/capability diagnostics,
  operator inventory, and encoded-native claim gates.
- Implementation scope: operator registry/capability selection, encoded kernel implementation for
  the selected family, decoded-reference correctness tests, route timing stage attribution
  contracts, benchmark validators, and website route-share labels.
- Evidence required: decoded reference parity, null/type edge cases, encoded/native admission
  diagnostics, route rows showing the promoted operator family, before/after current-code timing for
  comparable scenarios, and validators proving exclusive stage sums/residuals are coherent.
- Acceptance: promoted rows stop reporting the selected family as residual/materialized; unsupported
  operators keep deterministic blockers; route-share attribution ranks measured exclusive stage
  costs without >100% diagnostic contradictions; current-code reruns do not show a slower hot route
  for admitted comparable rows; performance artifacts separate operator compute from source
  preparation and publication proof.
- Verification: Rust unit/integration tests for the promoted kernel, Python release-script tests for
  row contract changes, targeted benchmark rerun for scenarios using the promoted operator, and
  `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace --all-targets` when Rust behavior changes.
- Non-goals: do not promote every operator in one PR; do not remove residual/materialized paths
  before supported encoded-native equivalents exist; do not make performance claims from diagnostic
  stage timing alone.
- Claim boundary: encoded-native claims are family/scenario-scoped until CG-5/CG-6 evidence covers
  broader correctness and benchmark claims.
- Fallback boundary: unsupported operator families must fail or report deterministic blockers; no
  external engine residual evaluation is allowed.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-4R - PulseWeave session/runtime coalescing follow-through

- Source: completed `PERF-DESIGN-4` session-routing evidence, `docs/architecture/pulseweave-runtime-control.md`,
  current published row chunks
  `website/assets/benchmarks/latest/published-row-runs/rows-b81bbdc3217d3209`, and benchmark
  rows showing repeated native/prepared scenario groups with residual route-open/scan-open/result
  assembly overhead.
- Current state: session route evidence is present and correctly separates process wall, shared
  batch wall, hot route totals, and no hidden daemon/global cache posture. The runtime still mostly
  executes scenario work as independently shaped chunks inside the benchmark harness, and PulseWeave
  fields are evidence-bearing but not yet used to coalesce compatible local scenario groups into a
  bounded run-local work plan.
- Next slice outcome: apply PulseWeave only inside admitted local prepared/native scenario groups so
  FlowInventory can coalesce compatible scenario scans, ScarcityLedger can account for repeated
  route-open/scan-open/result-assembly pressure, EndoPulse can keep the policy run-local, and
  ProofBound can block coalescing when certificate, no-fallback, or timing-surface evidence is
  incomplete.
- User-visible surface: benchmark route rows, session/runtime envelope fields, Python simulation
  timing reports, optimization-target validator, and benchmark website route/lane attribution.
- Implementation scope: `shardloom-vortex/src/traditional_analytics.rs` session/batch route
  planning, `shardloom-exec` PulseWeave policy if needed, Python benchmark promotion passthrough,
  route timing validators, focused Rust tests, and targeted benchmark artifacts.
- Evidence required: before/after targeted local benchmark rows for repeated warm/native/prepared
  scenario groups, PulseWeave applied/blocked fields, unchanged correctness digests, no fallback,
  no external engine, and explicit timing-surface inclusion flags.
- Acceptance: compatible local scenario groups report coalesced PulseWeave route use with reduced
  repeated route-open/scan-open/result-assembly overhead or a deterministic `blocked_*` reason;
  unsupported groups continue through the non-PulseWeave ShardLoom-native path without hidden cache
  state; no publication-proof timing is mixed into hot-runtime totals.
- Verification: focused Rust session/PulseWeave tests, targeted benchmark rerun over repeated
  warm/native/prepared scenario groups, `python3 scripts/check_benchmark_optimization_targets.py`,
  benchmark publication claim gate, and broad Rust validation when runtime behavior changes.
- Non-goals: no daemon, global cache, persistent tuning database, cross-run learning, object-store
  runtime, distributed execution, live/hybrid runtime, or public performance claim.
- Claim boundary: may claim only scoped local PulseWeave coalescing evidence for the measured
  scenario groups; no broad runtime, production, Spark-displacement, or superiority claim.
- Fallback boundary: PulseWeave policy must keep `fallback_attempted=false` and
  `external_engine_invoked=false`; blocked policy can continue only through admitted ShardLoom-native
  execution.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-1R - Dynamic prepared-state reuse and role-repair optimization follow-through

- Source: completed `PERF-DESIGN-1` prepared-state role repair evidence,
  `docs/architecture/io-reuse-and-fanout-architecture.md`, current manifest
  `website/assets/benchmarks/latest/manifest.json`, and benchmark rows where prepared-state
  lookup/metadata verification remains visible even when artifacts are reused.
- Current state: SourceState and VortexPreparedState dependency checks, manifest reuse, and role
  repair are correctness/evidence complete for scoped local traditional analytics. The optimized
  path still needs dynamic reuse admission that can recognize a run-local prepared dependency set
  already proven by capillary tasks and skip redundant digest, manifest, metadata-open, and writer
  context work inside the same prepared benchmark/workflow.
- Next slice outcome: implement a run-local dynamic prepared-state reuse controller that batches
  dependency checks, records capillary proof refs for each source/prepared role, and avoids repeated
  lookup/create work when the source/prepared digest tuple is unchanged and ProofBound evidence is
  complete.
- User-visible surface: prepare-batch rows, Python prepared benchmark simulation timing, benchmark
  manifest, website prepared-route attribution, and release-script validators.
- Implementation scope: prepared workspace manifest logic, SourceState/VortexPreparedState reuse
  helpers, role-repair dependency checking, benchmark row promotion, focused Rust tests, and Python
  release-script tests.
- Evidence required: full prepare, manifest hit, role repair, same-run repeated prepared lookup,
  digest drift fail-closed, no fallback, no external engine, and timing fields showing lookup/create
  separately from full route totals.
- Acceptance: repeated same-run prepared lookups report dynamic reuse admission with no rewritten
  artifacts, no full reopen verify, no duplicate writer context, and stable correctness/certificate
  refs; changed source/dim/CDC roles still force deterministic repair or full reprepare.
- Verification: focused Rust prepared-state reuse/repair tests, targeted prepare-batch benchmark,
  Python release-script tests for promotion fields, benchmark optimization-target validator, and
  broad Rust validation when runtime behavior changes.
- Non-goals: no process-global cache, no hidden stale reuse, no partial repair for unsupported
  dependency shapes, no object-store/table cache, and no public performance claim.
- Claim boundary: may claim only scoped local dynamic prepared-state reuse evidence; no package,
  production, or superiority claim.
- Fallback boundary: all reuse and repair paths must keep `fallback_attempted=false` and
  `external_engine_invoked=false`; unsupported dependency shapes must block or fully reprepare.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-5R - Capillary preparation spine write/reopen/copy optimization follow-through

- Source: completed `PERF-DESIGN-5` timing-attribution fix,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`, and current benchmark rows where cold
  certified routes still expose material `vortex_write_ms`, reopen/verify, and copy-budget cost.
- Current state: preparation timing is no longer sourced from route totals, and the artifact reports
  narrow `prepared_state_lookup_or_create`, preparation/create, full prepare route, writer context,
  metadata-first verification, and copy-budget fields. The path remains mostly evidence-bearing and
  does not yet use capillary preparation windows to reduce write/reopen/copy work for admissible
  local chunks.
- Next slice outcome: add a capillary preparation execution window for local cold/prepared Vortex
  preparation that coalesces compatible source split discovery, columnarize/encode, Vortex segment
  write, metadata-first verification, and sink evidence tasks under PulseWeave/ProofBound admission.
- User-visible surface: cold certified benchmark route rows, prepare-batch timing fields, copy-budget
  evidence, Native I/O certificate refs, benchmark website stage attribution, and release validators.
- Implementation scope: `vortex_ingest` preparation spine, writer context lifecycle, metadata-first
  reopen verification strategy, capillary task manifest fields, benchmark promotion, and tests.
- Evidence required: before/after targeted cold/prepared route rows, unchanged output digests,
  writer/reopen count fields, copy-budget counters, Native I/O and execution certificate status,
  and fail-closed diagnostics for unsupported capillary activation.
- Acceptance: admitted local preparation rows show fewer duplicate writer/reopen/copy operations
  for equivalent input shapes or report a deterministic capillary block reason; hot-runtime totals
  remain separate from publication-proof totals; buffer reuse remains scoped and explicit.
- Verification: focused Rust preparation-spine tests, targeted preparation benchmark, benchmark
  artifact completeness/claim gate, optimization-target validator, and broad Rust validation when
  runtime behavior changes.
- Non-goals: no hidden buffer pool, unsafe lifetime reuse, object-store writes, table/lakehouse
  commits, real query-data spill, or broad performance claim.
- Claim boundary: may claim only scoped local capillary preparation evidence and measured targeted
  timing; no production/package/superiority claim.
- Fallback boundary: capillary preparation must keep `fallback_attempted=false` and
  `external_engine_invoked=false`; unsupported source/sink/split shapes must block or use the
  existing admitted ShardLoom-native preparation route.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-6R - Dynamic source-adapter parse/decode and scout-ingress optimization follow-through

- Source: completed `PERF-DESIGN-6` source-adapter attribution, `docs/architecture/dynamic-work-shaping.md`,
  `docs/architecture/vortex-runtime-utilization-audit.md`, current benchmark rows where
  `source_parse_or_columnar_decode_ms` remains a cold-lane bottleneck, and optimization-target
  reports for JSONL/AVRO/source-scout timing.
- Current state: source adapters expose projection-aware scout plans, byte acquisition, typed
  decode, row assembly, and columnar handoff stages, and refreshed rows avoid unused row-buffer
  assembly where supported. Remaining overhead is still real typed text/binary decode and
  source-to-Vortex handoff work, not a labeling problem.
- Next slice outcome: implement dynamic scout-ingress admission that chooses lightweight metadata,
  projected typed decode, or capillary chunked decode based on observed bytes/rows/columns and
  scenario-required fields, with explicit block reasons when the source format cannot safely use the
  optimized path.
- User-visible surface: source-adapter benchmark rows, source-read scout fields, website stage
  attribution, Python ETL snippets/timing review, and optimization-target validator.
- Implementation scope: source adapter read/parse/decode code, scout ingress fields, capillary
  activation thresholds, benchmark promoter passthrough, focused source tests, and targeted source
  benchmark artifacts.
- Evidence required: per-format targeted benchmark rows for JSONL/AVRO/CSV where relevant, row
  assembly remains zero when avoided, decoded columns/skipped columns are correct, correctness
  digests stable, and unsupported formats fail closed without external engines.
- Acceptance: source-heavy lanes show reduced parse/decode or handoff timing for admitted projected
  workloads or a deterministic `blocked_*` source-scout reason; no source adapter reports hidden
  fallback or loses required columns/null semantics.
- Verification: focused Rust/Python source adapter tests, targeted source-heavy benchmark rerun,
  `python3 scripts/check_benchmark_optimization_targets.py`, website readiness/static validation,
  and broad validation when shared adapters move.
- Non-goals: no external parser engine fallback, no object-store runtime, no lossy decode, no
  broad source-format support claim, and no publication freshness claim without clean-source
  benchmark refresh.
- Claim boundary: may claim only scoped dynamic source-adapter optimization for measured formats and
  scenarios; no production/Spark-displacement/superiority claim.
- Fallback boundary: optimized source paths must keep `fallback_attempted=false` and
  `external_engine_invoked=false`; blocked formats must remain explicit.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-3 - Publication-proof sink/evidence pipeline optimization

- Source: `publication_proof` rows in PR #1174 and current published row chunks
  `website/assets/benchmarks/latest/published-row-runs/rows-b81bbdc3217d3209`;
  `PERF-SPLIT-FIX-1`; user request to reduce benchmark errors and write values incrementally.
- Current state: publication-proof rows intentionally include result-sink and evidence-render work.
  Prepared/native publication rows add roughly `2.8-3.1 ms` evidence render geomean and
  `0.4-0.6 ms` result-sink/write geomean, while prepare-once-first-query publication rows show
  about `42.91 ms` publication-proof route geomean because first-query preparation is included.
  The page now labels this correctly, but the proof path is still a candidate for incremental
  sink/evidence storage and replay reuse.
- Next slice outcome: design and implement an incremental proof artifact path where result-sink
  writes, replay proofs, certificate links, and human evidence render metadata are persisted as
  stable sidecar records and reused when row inputs and route evidence digests have not changed.
- User-visible surface: publication-proof benchmark rows, result-sink replay diagnostics, evidence
  reports, website benchmark proof tables, and release validators.
- Implementation scope: result-sink artifact writer, evidence-render sidecar schema, digest/replay
  cache checks, validator updates, benchmark publication rows, and website proof labels.
- Evidence required: tests for unchanged proof reuse, changed-row invalidation, digest mismatch
  fail-closed behavior, replay proof attachment, and route totals that explicitly include or exclude
  sink/evidence work by timing surface.
- Acceptance: publication proof remains visible and slower when doing real proof work; unchanged
  proof records are reused rather than re-rendered/replayed; digest drift blocks promotion; route
  formulas continue to state `timing_surface` and inclusion flags.
- Verification: focused proof-cache tests, targeted publication benchmark rerun, benchmark
  publication claim gate, website readiness, and static asset validation.
- Non-goals: do not remove `publication_full`, do not mix proof work back into hot-runtime totals,
  and do not weaken evidence requirements to make publication rows faster.
- Claim boundary: this may improve publication-proof overhead for unchanged evidence only; it does
  not change hot-runtime performance claims.
- Fallback boundary: result-sink replay and evidence rendering remain ShardLoom-native proof
  surfaces with `fallback_attempted=false` and `external_engine_invoked=false`.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

Plan state after PR #1174 benchmark row/readiness refresh:

- The `SECURITY-DEEP-SCAN-R3-FOLLOWUP` item completed in PR #1167 and its detailed session record
  lives in `docs/architecture/phased-execution-completed-ledger.md`.
- `REPO-WIDE-AUDIT-1` produced `docs/architecture/repo-wide-audit.md`,
  `docs/architecture/repo-wide-audit-inventory.json`, and `scripts/check_repo_wide_audit.py`.
  The audit body records 994 tracked files after the `REPO-WIDE-AUDIT-2` refresh, zero skipped
  files, and exactly three requested
  sections: `Architecture/Documentation`, `Shardloom Code`, and `Website`.
- `REPO-WIDE-AUDIT-2` completed the first Architecture/Documentation cleanup batch: public status
  wording now routes through `docs/release/public-status-matrix.md`, compute-flow vocabulary is
  owned by `docs/architecture/compute-engine-flow-reference.md`, the overhaul review is historical,
  and CI validates those public-status doc anchors.
- `REPO-WIDE-AUDIT-3A` completed the first Shardloom Code cleanup batch by adding shared
  release-report helpers and benchmark-driven optimization target evidence.
- `REPO-WIDE-AUDIT-3B` completed the first hot-runtime code optimization batch by reducing JSONL
  source parse/decode work for benchmark-shaped profile tails and selected generic fallback rows.
  Remaining benchmark targets are diagnostic-only until a refreshed artifact identifies a concrete
  claim-blocking runtime regression or a new phase-plan item is promoted.
- `REPO-WIDE-AUDIT-4` completed the first Website cleanup batch: the benchmark page now names
  benchmark static data ownership, keeps timing-surface views separate, and states the retired
  optimization-target policy. Website readiness validates benchmark mirror ownership and the
  optimization target report's diagnostic disappearance policy.
- Completed runtime and release details live in
  `docs/architecture/phased-execution-completed-ledger.md`; keep this file as the compact planned
  queue.
- The 38 unchecked rows in `docs/architecture/global-architecture-review.md` are mapped by
  `docs/architecture/runtime-gap-family-burn-down.md` and
  `target/runtime-gap-family-burn-down.json` to claim-boundary evidence families. They are not
  autonomous implementation rows until a new concrete item is promoted here.
- Hard release readiness remains fail-closed for public package/release approval, API/schema
  stability, per-claim evidence, and current benchmark publication freshness. Those are approval or
  artifact-refresh gates, not unchecked phase-plan rows.
- Benchmark rows remain evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no public freshness
  claim until a clean-tree benchmark artifact is regenerated from the source revision being claimed.

Remaining work snapshot:

| Order | Work item | Remaining outcome |
| --- | --- | --- |
| Closed | `REPO-WIDE-AUDIT-4` | Website/public benchmark surface cleanup and data ownership. |
| Closed | `REPO-WIDE-AUDIT-3B` | Hot-runtime JSONL source parse/decode optimization from benchmark target evidence. |
| Closed | `REPO-WIDE-AUDIT-3A` | Release-report helper modularization and benchmark optimization target evidence. |
| Closed | `REPO-WIDE-AUDIT-2` | Architecture/documentation coherence and claim-boundary cleanup. |
| Closed | `GAR-RUNTIME-IMPL-4/6A` | Residual completion gate closes with global-review rows mapped to claim-boundary evidence. |
| Closed | `RELEASE-SEQUENCE-1` through `RELEASE-SEQUENCE-14` | Local proof, package-channel posture, final rehearsal, and maintainer handoff are complete for the no-publication scope. |
| Deferred approval/artifact gate | Public release/package and current benchmark publication | Requires maintainer approval, channel-specific install/upload evidence, and a clean-source benchmark refresh before any public claim. |

Runtime and release queue status:

- Runtime Implementation Queue - Runtime-Enabling Work Only: closed for the current scoped compute
  engine completion pass. Future runtime work must be promoted as a new unchecked item here before
  implementation.
- Completed Benchmark Timing And Performance Innovation Queue: closed for current runtime
  sequencing. Hot route timing is timing-surface aware; proof/publication timing remains separate.
- 6-Series Runtime Breadth Queue: closed for the scoped user-surface breadth pass. Completed
  benchmark/profile, sub-evidence, user-surface, and UDF/extension blocker detail lives in the
  completed ledger and generated status artifacts.
- Production usability closeout anchor: completed benchmark/profile, sub-evidence, user-surface,
  and package-readiness proof detail lives in the completed ledger.
- Deferred Non-Runtime Closeout Queue: the current repo-wide audit follow-up batch is closed.
  Completed non-runtime history lives in the completed ledger; any additional work discovered by
  manual review must be promoted here as a concrete unchecked item before editing behavior.
- Final Pre-Release Sequential Closeout Queue: closed as no-publication evidence. Publication,
  signing, tags, uploads, package-channel submission, release assets, and public claims still require
  explicit maintainer approval and passing hard gates.
- Local release dry-run simulation follow-up: package proof must resolve and record a Python
  interpreter satisfying the Python package `requires-python` floor before wheel build, clean-venv
  install, benchmark smoke, or provenance dry-run steps. This is local pre-release readiness
  hardening only; it does not authorize publication or package-channel claims.

Traceability anchors retained for validators and future routing:

Global Architecture Review Carry-Forward:

- `GAR-RUNTIME-IMPL-6E` automatic dynamic preparation;
  `GAR-RUNTIME-IMPL-6F` output/fanout conversion;
  `GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout;
  `GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`;
  `GAR-RUNTIME-IMPL-4D-F2` complex dtype; `GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery;
  `GAR-RUNTIME-IMPL-6A` compute-engine completion gate; and the closed 6D runtime breadth families.
- Runtime gap-family burn-down phase strings retained for validator mapping:
  `GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar`;
  `GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth`;
  `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down`;
  `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication`;
  `GAR-RUNTIME-IMPL-6D:last_order.effectful_operations`;
  `GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime`.
- Reference docs that own deferred claim, release, benchmark, or runtime-boundary evidence include
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/architecture/pulseweave-runtime-control.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/release/release-architecture-tracker-gate.md`, and
  `docs/release/final-release-rehearsal.md`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
