# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work is the first unchecked Planned
  checkbox after this file has been reordered.
- Use one unchecked checkbox per active item or child slice. Put acceptance detail in nested plain
  bullets, not additional unchecked boxes, so release/completion gates report the real open-item
  count.
- Move a completed item summary to the completed ledger after merge or session completion.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Planned Item Detail Standard

Every unchecked Planned item must be executable by an autonomous Codex session without guessing.

Each item should name:

- Source: governing RFC, architecture doc, benchmark report, issue, PR, or review finding.
- Current state: what exists today and what is still unsupported, diagnostic-only, or report-only.
- Next outcome: the concrete result expected from the next cohesive PR/session.
- User-visible surface: CLI, Python, benchmark, docs, API, capability view, evidence artifact, or
  release gate.
- Implementation scope: files, modules, commands, and generated artifacts expected to change.
- Evidence required: correctness, benchmark, execution-certificate, Native I/O, materialization,
  decode, policy, no-fallback, release, security, or website evidence as applicable.
- Acceptance: observable conditions that make the item done.
- Verification: exact tests, validators, benchmark reruns, snapshots, or build commands expected.
- Non-goals: what must not be implemented in the slice.
- Claim boundary: what can and cannot be claimed after completion.
- Fallback boundary: expected `fallback_attempted=false` and `external_engine_invoked=false`
  behavior.
- Ledger rule: completed detail moves to
  `docs/architecture/phased-execution-completed-ledger.md`.

Do not leave planned work as a bare statement such as "`<thing>` remains incomplete." Convert broad
items into evidence-bearing implementation slices. Split a Planned item only when one coherent
reviewable PR/session would be unsafe, blocked by an external dependency, or too broad to validate.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests or validators exist, evidence refs are attached where claims are made, unsupported
paths remain explicit, no fallback engine was invoked, completed details are moved to the ledger,
and supporting docs are updated without becoming a second active queue.

Section-completion rule:

- Prefer one substantial PR/session that completes an entire runtime section over tiny row, format,
  or operator slivers.
- Split only for concrete safety, dependency, generated-artifact, or verification boundaries.
- For a section-completion PR, derive the full checklist from the owning item, companion runtime
  equivalent, status/capability files, route taxonomy, tests, and user-visible surfaces before
  editing.
- Avoid wording such as "promote one format/operator at a time" unless that format or operator has a
  separate dependency or deterministic blocker.

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
- `docs/architecture/global-architecture-review.md` may carry global audit rows, but actionable
  implementation must be promoted here before execution.
- Supporting docs may contain rationale, inventories, traceability, and historical notes, but they
  must not introduce a second current queue.
- Repeated support, claim-boundary, benchmark-interpretation, and runtime-state explanations should
  be owned by one canonical doc or generated data artifact; other pages should link to or render
  that source instead of restating parallel wording.

Reference index:

- Status source: `README.md`, `docs/architecture/phased-execution-completed-ledger.md`,
  `docs/architecture/rfc-phase-traceability.md`, `docs/architecture/global-architecture-review.md`,
  `docs/architecture/compute-engine-flow-reference.md`, and
  `docs/architecture/website-current-state-public-reference.md`.
- Benchmark and route evidence:
  `docs/architecture/performance-attribution-and-execution-structure.md`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `docs/architecture/benchmark-competitive-claim-evidence.md`,
  `docs/architecture/benchmark-persistent-runner-decision.md`, and `docs/benchmarks/*`.
- Runtime optimization references:
  `docs/architecture/runtime-evidence-level-tiering.md`,
  `docs/architecture/evidence-aware-logical-optimizer.md`,
  `docs/architecture/vortex-scan-pushdown-completion.md`,
  `docs/architecture/compressed-encoded-kernel-registry.md`,
  `docs/architecture/fused-operator-pipeline.md`,
  `docs/architecture/in-process-session-runtime.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/architecture/allocation-buffer-pool-optimization.md`,
  `docs/architecture/dynamic-work-shaping.md`,
  `docs/architecture/pulseweave-runtime-control.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/universal-input-contract.md`,
  `docs/architecture/vortex-adapter-integration-plan.md`, and
  `docs/architecture/vortex-runtime-utilization-audit.md`.
- Claim, release, package, and adoption references:
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/operational-evidence-policy-hardening.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/release-architecture-tracker-gate.md`,
  `docs/release/final-release-rehearsal.md`, and `docs/release/*`.

Reference-doc rule: these files are evidence, guardrails, or inventories. They do not authorize
runtime behavior, support claims, dependency expansion, package publication, external effects, or
fallback execution unless a matching unchecked item below is completed with evidence and moved to
the ledger.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value.
The first unchecked checkbox is the next default autonomous slice.

Current autonomous execution order:

### Open Work Checklist

- [ ] `PERF-RUNTIME-7A` Cold compatibility-to-certified route hot-runtime burn-down.
  - Source: current promoted `full_local` benchmark artifact generated
    `2026-06-13T09:18:58Z` from source revision
    `2e8518f0660c99336f39d7df385ba54193e292ac`; route-share Amdahl table and row-level
    inspection of `website/assets/benchmarks/latest/benchmark-results.json`.
  - Current state: `cold_certified_route` is the only broad multi-ms ShardLoom hot runtime lane:
    hot-route geomean `6.08 ms`, p95 `17.55 ms`, max `25.11 ms`. Included hot stages are
    source admission, source read, parse/decode, Vortex write, and reopen/verify. Current
    stage geomeans are roughly Vortex write `2.35 ms`, parse/decode `1.94 ms`, source read
    `0.82 ms`, reopen/verify `0.46 ms`; JSONL is the slowest format at `11.60 ms`.
  - Execution checklist:
    - [x] Add direct selected-tail parsing for canonical benchmark JSONL optional fields so
      nested/dirty/timestamp scenarios avoid scanning and splitting every unselected tail field.
    - [x] Keep non-canonical/whitespace JSONL tails on the existing general scanner to preserve
      fail-closed correctness for irregular JSONL.
    - [x] Cache the Vortex table/flat layout strategy lazily inside the shared writer context so
      multi-artifact cold writes reuse the same strategy object after the first artifact.
    - [x] Remove no-op/stale cold-route source clutter uncovered by clippy after runtime changes.
    - [x] Run a targeted one-iteration local smoke artifact for changed cold-route JSONL/join
      paths (`target/perf-runtime-7-smoke.json`,
      `target/perf-runtime-7-nested-json-smoke.json`); treat as validation only, not a published
      benchmark claim.
    - [ ] Run a targeted cold-route benchmark refresh after source validation to measure JSONL
      parse/write movement.
    - [ ] Confirm promoted rows expose non-zero writer context/write-plan counters from current
      source, replacing stale `not_reported_by_engine` artifact rows.
  - Next outcome: reduce real cold-route work without changing timing-surface semantics by
    batching/coalescing Vortex writes, tightening schema-driven typed source builders for text
    formats, and preserving reader-boundary projection for columnar formats. Apply dynamic
    admission only when source shape justifies the optimization, capillary windows for bounded
    ingest/write units, and PulseWeave-style coalescing for run-local writer/reopen work.
  - User-visible surface: benchmark route rows, benchmark stage attribution, route-share Amdahl
    table, website benchmark page, Python/CLI cold ingest path, and release readiness gates.
  - Implementation scope: `shardloom-vortex/src/traditional_analytics.rs`,
    `benchmarks/traditional_analytics/run.py`, benchmark promotion/readiness scripts if fields
    require schema additions, and focused Rust/Python regression tests. Generated benchmark
    artifacts are refreshed only after source behavior and validators pass.
  - Evidence required: correctness parity for compatibility inputs, no-fallback execution
    certificate fields, route timing stage-inclusion evidence, source-read/decode/write/reopen
    split evidence, and benchmark rows grouped by `(route_lane_id, timing_surface)`.
  - Acceptance: cold route still reports `fallback_attempted=false` and
    `external_engine_invoked=false`; hot-runtime total excludes result-sink/evidence render;
    JSONL/text and columnar paths keep projection/typed-builder evidence; Vortex writer context
    rows report useful non-zero writer/open/stage counters where applicable; route-share table no
    longer identifies broad missing attribution before optimization claims.
  - Verification: focused Rust tests for cold compatibility ingest, columnar projection, text
    typed builders, writer/reopen evidence; `cargo fmt --all -- --check`; targeted
    `cargo test -p shardloom-vortex --features vortex-traditional-analytics-benchmark --lib ...`;
    release-script tests covering timing fields; benchmark publication/readiness validators; full
    benchmark refresh only at the end of the cohesive runtime chunk.
  - Non-goals: no Spark/DataFusion/DuckDB/Polars/Velox fallback; no object-store/distributed
    runtime expansion; no public superiority or Spark-displacement claim; no synthetic shortcut
    rows.
  - Claim boundary: may claim only workload-scoped cold-route implementation and evidence
    improvements after benchmark refresh; no performance claim without current artifact evidence.
  - Fallback boundary: all ShardLoom rows must remain native, policy-admitted, and explicit with
    `fallback_attempted=false` and `external_engine_invoked=false`.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    `docs/architecture/phased-execution-completed-ledger.md`.
- [ ] `PERF-RUNTIME-7B` Heavy residual operator tail promotion for multi-key group-by and
  join-aggregate.
  - Source: current promoted `full_local` benchmark artifact row-level timing. Heavy hot tails:
    cold `multi_key_group_by` geomean `18.28 ms` with diagnostic operator compute around
    `7.22 ms` and reopen/verify around `6.40 ms`; cold `join_aggregate` geomean `10.57 ms`;
    prepared/native `join_aggregate` rows still spike near `5 ms` despite low warm/native
    geomeans.
  - Current state: operator mode inventory still reports residual-native operator promotion
    blockers: `residual_native_operator_encoding_promotion`,
    `selective_filter_selection_vector_metric_aggregation`, and
    `compatibility_import_materialization_elimination`. Diagnostic operator fields are visible but
    many operator timings are not additive to selected route totals.
  - Execution checklist:
    - [x] Reuse the packed dense group accumulator for `join_aggregate`, replacing the older
      dense-left/per-dimension `HashMap` category accumulator in the hot loop.
    - [x] Add a dense-contiguous dimension-key membership fast path for compact dimension domains.
    - [x] Promote packed join result rendering to runtime code and preserve deterministic label
      coalescing/fail-closed category checks.
    - [x] Remove the obsolete dense-left category accumulator and old BTreeMap renderer instead
      of keeping unused legacy code.
    - [x] Run a targeted one-iteration local smoke for `join + aggregate` across CSV/JSONL cold and
      prepare-batch lanes; treat the result as route validation only until refreshed benchmark rows
      are promoted.
    - [ ] Refresh targeted prepared/native `join_aggregate`, `multi_key_group_by`, and
      high-cardinality rows to determine whether remaining opportunities are significant or
      marginal.
    - [ ] If refreshed rows still show multi-ms prepared/native operator spikes, add the next
      native kernel family with decoded-reference parity tests before claiming promotion.
  - Next outcome: promote a cohesive heavy-operator family rather than isolated scenario slivers:
    multi-key grouping, join+aggregate, high-cardinality distinct/group, and their prepared/native
    residual tails. Add encoded or partially encoded kernels where correctness evidence supports
    them; otherwise emit deterministic blocked diagnostics with precise next-step fields.
  - User-visible surface: benchmark operator hot-path candidates, runtime certificates, Python/SQL
    scenario behavior, benchmark route rows, and capability/diagnostic fields.
  - Implementation scope: operator/kernel code in `shardloom-vortex/src/traditional_analytics.rs`
    or extracted local helpers if needed, encoded-kernel evidence fields, route/operator
    diagnostics, and regression tests for nulls, high cardinality, ordering-sensitive top/join
    outputs, and decoded-reference parity.
  - Evidence required: decoded-reference correctness, null/missing-key semantics, no-fallback
    certificates, operator execution mode transition evidence, timing-surface-safe route fields,
    and benchmark rows proving whether the tail changed.
  - Acceptance: supported heavy operator rows no longer remain generic
    `residual_native_operator_not_encoded_native` when a native kernel exists; unsupported shapes
    fail or remain blocked with deterministic blocker codes; prepared/native `join_aggregate`
    spikes are explained by additive timing fields or reduced by native execution; route totals
    remain authoritative.
  - Verification: focused Rust unit/integration tests for heavy operators, decoded-reference
    parity tests, benchmark publication claim gate, route timing instrument readiness, and full
    workspace gates when shared operator contracts move.
  - Non-goals: no broad SQL planner rewrite, no distributed shuffle, no external engine execution,
    no hidden decode-to-Arrow fallback.
  - Claim boundary: encoded/operator improvements may be claimed only per supported operator family
    with correctness and benchmark evidence.
  - Fallback boundary: external engines remain baselines only and never execute ShardLoom work.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.
- [ ] `PERF-RUNTIME-7C` Prepared lookup/create and route-total attribution cleanup.
  - Source: current route-share Amdahl and stage-inclusion tables. `prepare_once_first_query`
    hot-route geomean is `0.67 ms`, dominated by `prepared_state_lookup_or_create` around
    `0.56 ms` (`84%` route share). `prepare_once_batch`, warm, and native lanes have very low
    geomeans but still carry diagnostic stage fields larger than selected route totals.
  - Current state: prepared lookup/create is a moderate absolute cost and a large relative cost for
    first-query prepared routes. Route-share rows are optimization-ready, but some diagnostic
    fields are intentionally non-additive and can distract optimization targeting.
  - Execution checklist:
    - [x] Confirm `preparation_engine_millis` prefers narrow prepared-state/import fields and does
      not use `total_runtime_micros` as the narrow prepare timing source.
    - [x] Keep `prepare_route_total_ms` separate for full route totals.
    - [x] Hash serialized JSON bytes directly for source-admission, prepared-state manifest, and
      index digests to avoid an intermediate UTF-8 string allocation.
    - [x] Run a targeted one-iteration local prepare-batch smoke showing
      `prepared_state_lookup_or_create` remains separate from `prepare_route_total`.
    - [ ] Refresh prepared-route benchmark rows to measure whether manifest lookup/create moved.
    - [ ] If lookup remains material, evaluate a manifest/index read-through cache that still
      verifies manifest digest, source fingerprints, artifact fingerprints, native I/O
      certificates, and no-fallback fields before reuse.
  - Next outcome: split manifest lookup, cache-hit, cache-miss create, dependency-packet
    verification, artifact write, and register-update timings into additive and diagnostic fields;
    remove avoidable lookup/create work on cache hits; keep first-query and amortized formulas
    explicit.
  - User-visible surface: prepared-state reuse evidence, benchmark route formulas, Python
    front-door prepared-route examples, and release evidence reports.
  - Implementation scope: prepared-state manifest/register helpers, session cache counters,
    timing field promotion in `benchmarks/traditional_analytics/run.py`, Rust tests for
    cache-hit/miss/stale-packet behavior, and website data fields if schema-safe.
  - Evidence required: cache hit/miss counters, stale-packet rejection evidence, additive timing
    formulas, no result-sink/evidence render in hot-runtime totals, and benchmark rows showing
    lookup/create attribution.
  - Acceptance: first-query prepared route reports precise lookup/create subcomponents; cache-hit
    path avoids unnecessary register/write work; prepared batch amortized route remains formula
    backed; no `total_runtime_micros` fallback is used as a narrow prepare timing source.
  - Verification: focused prepared-state Rust tests, release-script tests for timing promotion,
    publication claim gate, route timing instrument readiness, and targeted benchmark refresh when
    source behavior changes.
  - Non-goals: no package/public release claim, no external cache service, no distributed session
    runtime.
  - Claim boundary: may claim attribution and scoped first-query prepared-route improvements only
    with benchmark evidence.
  - Fallback boundary: prepared-state reuse must remain ShardLoom-native and fail closed on stale
    dependency packets.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.
- [ ] `PERF-RUNTIME-7D` Publication-proof sink/evidence overhead burn-down without redefining hot
  runtime.
  - Source: current promoted `full_local` artifact. Publication-proof routes add roughly
    `2.7-3.1 ms` evidence render and about `0.35 ms` result-sink work to warm/native/prepared
    lanes; this is significant for proof/publication throughput but not a core hot-runtime
    regression.
  - Current state: `publication_proof` rows are correctly separated from `hot_runtime`, but the
    proof path still spends more time rendering human evidence than executing warm/native queries.
  - Execution checklist:
    - [x] Confirm Rust runtime rows emit compact machine evidence and mark human evidence render as
      outside the Rust timed route.
    - [x] Confirm benchmark promotion already writes an incremental publication-proof sidecar with
      reused/written/removed record counts and no-fallback fields.
    - [ ] After benchmark promotion, confirm the sidecar reports reuse for unchanged publication
      records and that website labels keep proof overhead out of hot runtime.
    - [ ] If publication-proof rows still spend multi-ms in repeated human formatting after sidecar
      reuse, optimize the Python/website render surface rather than the ShardLoom hot runtime.
  - Next outcome: coalesce and cache publication-proof render work, reuse machine evidence digests,
    keep full Vortex replay/result-sink timing explicit, and avoid repeating human formatting when
    the compact machine evidence is unchanged.
  - User-visible surface: benchmark website, publication-proof sidecar, release readiness reports,
    and result-sink/evidence-render timing fields.
  - Implementation scope: publication-proof sidecar writer/reuser, benchmark promotion scripts,
    website data ingestion, readiness validators, and Python tests for stale/reused proof records.
  - Evidence required: sidecar reused/written/stale counts, no-fallback proof fields, explicit
    `sink_timing_included_in_route_total=true` for proof surfaces, and unchanged hot-runtime totals.
  - Acceptance: publication-proof rows remain visible and slower for stated reasons; repeated
    publication over unchanged machine evidence reuses proof records; website labels continue to
    distinguish hot route geomean from publication-proof route geomean.
  - Verification: release-script tests, benchmark publication/front-door/readiness validators,
    website readiness, and targeted artifact promotion after source changes.
  - Non-goals: no hiding proof cost in hot runtime, no removal of publication-proof rows, no public
    performance claim from proof-path-only improvements.
  - Claim boundary: may claim only proof-publication overhead reduction or attribution quality,
    not core runtime speed, unless a refreshed artifact proves core runtime changed.
  - Fallback boundary: proof generation must not call external compute engines or use external
    fallback execution.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Closed | `RELEASE-PACKAGE-15` | Completed in the ledger with clean-source benchmark publication evidence for source revision `74a2e7d4f77eed0686971518e010463da26f2cdf`; no autonomous implementation item remains. |
| Historical | PR #1174 benchmark row/readiness context, repo-wide audit closeout, release-sequence closeout, and completed benchmark/profile, sub-evidence, user-surface proof | Preserved in `docs/architecture/phased-execution-completed-ledger.md`; do not treat as active work. |
| Mapped, not autonomous queue | Unchecked global architecture review rows | Governed by `docs/architecture/global-architecture-review.md` and `docs/architecture/runtime-gap-family-burn-down.md`; promote concrete implementation items here before work begins. |
| Deferred approval/artifact gate | Public release/package approval | Clean local Conda proof, dependency/security/package local-gate evidence, and current benchmark-publication evidence now pass locally; remaining blockers are package-channel approval/proof, publication/API/schema stability approval, and per-claim evidence promotion before any public claim. |

Deferred Non-Runtime Closeout Queue: closed for the current cleanup batch. Completed non-runtime history
lives in `docs/architecture/phased-execution-completed-ledger.md`; any future work from manual
review must be promoted here as a concrete unchecked item before editing behavior.

### Evidence Pointers

- Current benchmark timing snapshot and PR #1174 route/readiness context are preserved in the
  completed ledger entry `Phase-plan open-queue cleanup and completed-state ledger migration`.
- Performance route, stage, and timing-surface contracts live in
  `docs/architecture/performance-attribution-and-execution-structure.md`.
- Current source/input evidence contracts live in `docs/architecture/universal-input-contract.md`.
- Benchmark artifacts are evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no package-release
  claim, and no public freshness claim until a clean-source artifact is regenerated from the source
  revision being claimed.

### Reopen Policy

- Completed `PERF-DESIGN-*` items may return to Planned only as explicit `*R` optimization passes
  when current benchmark rows, validator output, or targeted local simulation identify a measured
  bottleneck.
- A reopened `*R` item must preserve the original closeout contract and add a narrower optimization
  contract: control surface, timing rows/fields proving it is still worth changing, fail-closed
  blocker vocabulary, and benchmark/test evidence.
- Use dynamic admission for repeated dependency/source decisions, PulseWeave for run-local
  coalescing and bounded work-in-progress, and capillary windows for small typed
  source/preparation/sink work units only where the bottleneck shape justifies those controls.
- There are no current direct open implementation items. Reopen completed `PERF-DESIGN-*` or
  `PERF-DESIGN-*R` passes only with new current artifact, validator, CI, UAT simulation, or
  maintainer-review evidence.

### Global Architecture Review Carry-Forward

- Runtime gap-family burn-down and validator mapping still own historical/global references:
  `GAR-RUNTIME-IMPL-6E` automatic dynamic preparation,
  `GAR-RUNTIME-IMPL-6F` output/fanout conversion,
  `GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout,
  `GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`,
  `GAR-RUNTIME-IMPL-4D-F2` complex dtype,
  `GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery, `GAR-RUNTIME-IMPL-6A`, and closed 6D
  runtime breadth families.
- Phase strings retained for routing and validator compatibility:
  `GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar`,
  `GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth`,
  `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down`,
  `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime`,
  `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime`,
  `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication`,
  `GAR-RUNTIME-IMPL-6D:last_order.effectful_operations`,
  `GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime`, and
  `GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime`.

### Guardrails

- No Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask, Ray, pandas, PyArrow, or another engine
  may execute unsupported ShardLoom work as fallback.
- Vortex is the highest-fidelity native input/output target.
- Compatibility inputs and outputs are explicit translation/admission surfaces, not execution
  fallback.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Do not make performance, production, package, Spark-displacement, superiority, object-store,
  Foundry, REST, live/hybrid, SQL/DataFrame, or public release claims without the required
  workload-scoped evidence and approval gates.
- Benchmark route analysis must group by `(route_lane_id, timing_surface)` and honor
  `route_timing_stage_inclusion_classes`; diagnostic stage fields must not silently redefine hot
  runtime totals.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
