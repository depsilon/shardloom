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

- [ ] `PERF-DESIGN-1` - Prepared-source and compatibility-ingest amortization design.
- [ ] `PERF-DESIGN-4` - Session-native route and process-wall amortization.
- [ ] `PERF-DESIGN-5` - Vortex preparation write/reopen and copy-budget optimization.
- [ ] `PERF-DESIGN-2` - Encoded-native operator promotion and stage-timing attribution cleanup.
- [ ] `PERF-DESIGN-3` - Publication-proof sink/evidence pipeline optimization.

Benchmark timing evidence snapshot for the `PERF-DESIGN-*` queue:

- Source artifact:
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`, 1,920
  published rows, including 1,200 ShardLoom rows and 720 external-baseline rows. External rows are
  baselines only, never fallback execution.
- ShardLoom timing surfaces: 600 `hot_runtime` rows and 600 `publication_proof` rows.
- Hot/runtime lane shape: cold certified route geomean is about `63.90 ms`; native Vortex,
  warm-prepared, prepare-once-batch, and prepare-once-first-query hot geomeans are about
  `0.10-0.12 ms` but have process-wall measurements around `34-40 ms` when driven through the
  CLI/harness boundary.
- Cold certified route bottlenecks: `source_parse_or_columnar_decode_ms` averages about `37.48 ms`
  and `vortex_write_ms` averages about `23.86 ms`; JSONL outliers reach `219.49 ms` hot route
  total with `174.20 ms` source parse/decode.
- Operator posture: all 600 hot/runtime ShardLoom rows remain `residual_native` or
  `materialized_temporary`; multi-key group by, nested JSON scan, high-cardinality string
  group/distinct, join+aggregate, and group-by aggregation are the highest measured
  `operator_compute_ms` families.
- Publication/proof posture: prepared/native publication-proof geomeans sit around `4.7-7.2 ms`,
  while prepare-once-first-query publication proof is about `42.91 ms` because it includes
  first-query preparation plus result-sink/evidence work.
- Claim boundary: these numbers are current local benchmark evidence and optimization direction,
  not performance, production, Spark-displacement, superiority, or package-release claims.

### PERF-DESIGN-1 - Prepared-source and compatibility-ingest amortization design

- Source: PR #1174 promoted artifact route timing; `website/assets/benchmarks/latest/benchmark-results.json`;
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`;
  runtime docs for UniversalIngress, SourceState, VortexPreparedState, and Vortex-native output.
- Current state: among the 1,200 ShardLoom rows, cold certified `hot_runtime` route geomean is
  about `63.90 ms`, with compatibility/import stages dominating:
  `source_parse_or_columnar_decode_ms` averages about `37.48 ms`, `vortex_write_ms` averages about
  `23.86 ms`, and JSONL outliers reach `219.49 ms` hot route total with `174.20 ms`
  source parse/decode. Prepared/native query lanes are sub-ms hot-route geomeans, so the clear
  global design opportunity is to avoid repeating source parse/decode/write work and make
  preparation reuse the normal path.
- Next slice outcome: design and implement the next coherent prepared-source reuse batch: stable
  SourceState fingerprints, manifest-keyed VortexPreparedState reuse, partial role repair for fact,
  dim, event, and CDC inputs, and benchmark evidence that cold import work is amortized or skipped
  when source fingerprints match.
- User-visible surface: Python/CLI prepare/query flow, benchmark rows, explain/diagnostic fields,
  and source/prepared-state evidence.
- Implementation scope: source identity/fingerprint helpers, prepared-state manifest/index logic,
  role-scoped repair, benchmark fixture setup, route diagnostics, and tests covering unchanged,
  changed, missing, and stale source roles.
- Evidence required: correctness tests for source-state reuse and repair, no stale artifact reuse,
  benchmark rows proving preparation skip/repair status, and route timing fields separating
  `prepared_state_lookup_or_create_ms`, `prepare_route_total_ms`, and `prepare_cli_wall_ms`.
- Acceptance: repeated prepared routes do not reparse/rewrite unchanged source roles; changed roles
  repair deterministically; stale/missing artifacts fail closed; cold route timing remains explicit
  when preparation really occurs; hot/prepared routes keep query/runtime totals separate from
  preparation.
- Verification: focused Python/Rust tests for prepared-state manifest behavior; benchmark targeted
  rerun over `shardloom-prepare-batch`, `shardloom-prepared-vortex`, and `shardloom-vortex`;
  publication claim gate with `--allow-stale-git`; no-fallback validators.
- Non-goals: do not claim all cold routes become sub-ms, do not bypass Vortex write/verify when a
  new prepared artifact is actually required, and do not use DuckDB/Polars/DataFusion/Spark as
  preparation fallback.
- Claim boundary: may claim workload-scoped preparation reuse only when the source fingerprint and
  benchmark evidence show reuse; no broad engine superiority claim.
- Fallback boundary: preparation, repair, and query execution remain ShardLoom/Vortex-native with
  `fallback_attempted=false` and `external_engine_invoked=false`.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-4 - Session-native route and process-wall amortization

- Source: current published row chunks
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`;
  `docs/architecture/in-process-session-runtime.md`;
  `docs/architecture/benchmark-persistent-runner-decision.md`; Python context/session surface.
- Current state: warm/prepared/native `hot_runtime` route geomeans are about `0.10-0.12 ms`, but
  the same rows still expose `cli_process_wall_millis` around `34-40 ms` and
  `preparation_cli_process_wall_millis` around `34-39 ms` through the benchmark/Python harness
  boundary. Scoped caller-owned session evidence exists, but the primary repeated-workflow route is
  not yet treated as the default performance-sensitive path for Python/user benchmarks.
- Next slice outcome: make repeated local workflows route through an explicit caller-owned
  `ShardLoomSession` by default where the source/prepared-state policy admits it, and update the
  benchmark harness to run scenario groups through one session without creating a hidden daemon or
  process-global cache.
- User-visible surface: Python `context(...).session(...)` and default repeated `collect()`/
  `write_vortex()` behavior where session routing is admitted, benchmark route rows,
  explain/diagnostic fields, and CLI batch smoke reports.
- Implementation scope: Python context/session orchestration, CLI batch route plumbing,
  benchmark harness route selection, row promotion fields for `process_spawn_count`,
  `session_route_used`, cache hit/miss counts, close/drop status, and tests covering explicit
  session close plus stale fingerprint invalidation.
- Evidence required: repeated scenario benchmark rows showing one caller-owned session per admitted
  group, no hidden global cache, stable source/prepared/output digests, process-wall attribution
  separated from hot route totals, and deterministic blockers when session reuse is unsafe.
- Acceptance: repeated warm/native/prepared routes no longer require one CLI process spawn per
  scenario in the admitted batch path; process-wall overhead is amortized and reported separately;
  session close/drop cleanup is observable; route totals remain timing-surface aware; unsupported
  session reuse fails closed.
- Verification: session-cache smoke snapshots, Python session smoke tests for repeated local
  workloads, benchmark row contract tests, targeted traditional-analytics rerun for warm/native and
  prepare-batch lanes, `python scripts/check_benchmark_artifact_completeness.py --allow-stale-git`
  when artifact rows are refreshed, and no-fallback validators.
- Non-goals: do not introduce a daemon, service runtime, remote API, hidden fast mode, hidden
  process-global cache, object-store/table cache, or public performance claim.
- Claim boundary: may claim only workload-scoped process/session amortization when row evidence
  shows the selected timing surface, session reuse posture, and process-spawn count. It does not
  authorize production, Spark-displacement, package-release, or broad superiority claims.
- Fallback boundary: session execution remains ShardLoom/Vortex-native with
  `fallback_attempted=false` and `external_engine_invoked=false`; external baselines remain
  comparison-only.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-5 - Vortex preparation write/reopen and copy-budget optimization

- Source: current published row chunks
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`;
  `docs/architecture/io-reuse-and-fanout-architecture.md`;
  `docs/architecture/allocation-buffer-pool-optimization.md`;
  `docs/architecture/vortex-adapter-integration-plan.md`.
- Current state: cold certified `hot_runtime` rows spend about `23.86 ms` average in
  `vortex_write_ms` with p95 around `30.10 ms`, and cold row totals are repeatedly dominated by
  source parse/decode plus Vortex write/reopen/verify work. Existing rows expose writer context,
  layout/write advisor, copy-budget, and reopen/verify vocabulary, but the current benchmark
  evidence still shows the preparation spine doing material write/reopen work for each cold or
  newly prepared artifact.
- Next slice outcome: implement one cohesive Vortex preparation-spine optimization batch covering
  writer context reuse, segment/write coalescing, metadata-first reopen verification, copy-budget
  accounting, and buffer-pool reuse across fact/dim/event/CDC roles without weakening Native I/O
  certificates.
- User-visible surface: benchmark preparation rows, VortexPreparedState evidence, Native I/O
  certificate refs, Python/CLI prepare diagnostics, and website timing stage attribution.
- Implementation scope: `shardloom-vortex` writer/preparation helpers, Vortex adapter admission
  policy, prepared-state manifest refs, copy-budget/buffer-pool accounting, reopen/verify timing
  split, benchmark row promotion, and focused tests for reused writer context plus fail-closed
  digest mismatch behavior.
- Evidence required: rows showing `vortex_writer_context_reuse_hit_count`,
  `vortex_write_plan_coalescing_status`, copy-budget counters, reopen/verify timing split,
  unchanged correctness digests, Native I/O certificate status, and explicit timing-surface
  inclusion flags.
- Acceptance: cold/preparation rows reduce repeated writer open/stage/reopen work when source and
  layout policy match; verification stays explicit and fail-closed; copy and buffer counters are
  populated; unchanged artifacts can reuse metadata verification without silently skipping required
  proof.
- Verification: Rust tests in `shardloom-vortex` for writer context/coalescing/copy-budget behavior,
  Python release-script tests for row fields, targeted benchmark rerun for cold certified and
  prepare-batch lanes, `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and
  `cargo test --workspace --all-targets` when Rust behavior changes.
- Non-goals: do not skip Vortex write/verify for new artifacts, do not bypass Native I/O
  certificates, do not add object-store/table commits, and do not claim all cold routes become
  sub-ms.
- Claim boundary: may claim only measured preparation-spine optimization for the selected local
  routes and timing surface; no production, object-store, package-release, Spark-displacement, or
  broad superiority claim.
- Fallback boundary: Vortex preparation remains ShardLoom/Vortex-native; no Vortex query-engine
  integration, DuckDB, Polars, Spark, DataFusion, or external engine may satisfy write/reopen work.
- Ledger rule: when complete, move the completed session summary to
  `docs/architecture/phased-execution-completed-ledger.md`.

### PERF-DESIGN-2 - Encoded-native operator promotion and stage-timing attribution cleanup

- Source: PR #1174 route rows; current published row chunks
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`; operator
  mode inventory fields; `operator_hot_path_candidate`; `route_timing_exclusive_stage_sum_ms`;
  `route_timing_exclusive_residual_ms`.
- Current state: prepared/native hot-route query totals are around `0.11-0.12 ms` geomean, but
  rows still report `operator_execution_mode=residual_native` for 480 hot/runtime rows and
  `materialized_temporary` for 120 cold compatibility rows. Highest measured operator families are
  multi-key group by, nested JSON field scan, high-cardinality string group/distinct,
  join+aggregate, and group-by aggregation. Diagnostic source/import/write fields can remain
  present on native/warm rows while excluded from authoritative hot totals, so stage attribution
  must stay explicit before using those fields for optimization ranking.
- Next slice outcome: select the highest-value operator family from the benchmark scenarios and
  promote it from residual/materialized execution toward encoded-native execution with correctness
  evidence, while normalizing exclusive stage timing so diagnostic stage costs cannot contradict
  authoritative route totals.
- User-visible surface: benchmark route-share attribution, explain/capability diagnostics,
  operator inventory, and encoded-native claim gates.
- Implementation scope: operator registry/capability selection, encoded kernel implementation for
  the selected family, decoded-reference correctness tests, route timing stage attribution
  contracts, benchmark validators, and website route-share labels.
- Evidence required: decoded reference parity, null/type edge cases, encoded/native admission
  diagnostics, route rows showing the promoted operator family, and validators proving exclusive
  stage sums/residuals are coherent.
- Acceptance: promoted rows stop reporting the selected family as residual/materialized; unsupported
  operators keep deterministic blockers; route-share attribution ranks measured exclusive stage
  costs without >100% diagnostic contradictions; performance artifacts separate operator compute
  from source preparation and publication proof.
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

### PERF-DESIGN-3 - Publication-proof sink/evidence pipeline optimization

- Source: `publication_proof` rows in PR #1174 and current published row chunks
  `website-public/assets/benchmarks/latest/published-row-runs/rows-101a09da6437eac2`;
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
