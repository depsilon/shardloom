# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work is the first unchecked Planned
  checkbox after this file has been reordered.
- Use one top-level unchecked checkbox per active item or promoted child slice. Every top-level
  item must include an `Execution checklist:` with nested checkboxes for the concrete substeps that
  make progress visible. Keep acceptance, evidence, boundaries, and verification as plain bullets.
- Use nested checklist boxes only for verifiable work: implementation, tests, generated evidence,
  docs/site updates, CI/benchmark refreshes, and ledger movement. Do not use vague checklist rows
  such as "continue work" or "investigate more" without a named evidence output.
- Every new ShardLoom runtime, support, release, benchmark, or user-surface item must include a
  `ShardLoom technique review:` bullet. That review must explicitly consider whether PulseWeave,
  capillary work units, dynamic admission/work shaping, metadata-first execution, route timing
  surface separation, or evidence-tier controls apply. If none apply, say why. This prevents net
  new features from being designed in a generic way that later needs avoidable refactoring to use
  ShardLoom's own performance and evidence techniques.
- Prefer a small number of reusable Vortex-normalized execution families over route proliferation.
  Public method names, SQL spellings, and CLI aliases may keep distinct labels for user clarity, but
  implementation should collapse aliases into shared planner/runtime/sink contracts whenever the
  source state, operator semantics, materialization boundary, and evidence fields are the same.
  Because ShardLoom is pre-public-use, do not preserve awkward legacy route splits for compatibility
  alone; preserve only the boundaries that make correctness, diagnostics, or evidence clearer.
- When a maintainer-provided list, audit, attachment, benchmark finding, or review packet proposes
  new work, review each candidate before adding it here. Classify it as already addressed,
  accepted into a new checklist, merged into an existing checklist, v1 candidate pending
  feasibility, deferred beyond the current product scope, or rejected with a reason. Do not paste
  broad lists verbatim into Planned.
- Production-shift items must state whether they are `required_for_v1`,
  `v1_candidate_pending_feasibility`, `deferred_out_of_v1`, `documentation_only`, or
  `unsupported_boundary`. The v1 default is inclusion for anything feasible to complete with
  real runtime behavior, deterministic unsupported diagnostics, safety evidence, and release proof.
  Defer beyond v1 only when the item records a concrete reason such as unavailable external
  platform proof, unresolved safety/security design, missing protocol approval, or scope that would
  make v1 unverifiable.
- Feasible runtime/user-surface rule: do not end a phase-plan item by preserving a blocker for any
  route, operation, input, sink, or user workflow that can be implemented inside this repository
  without external platform approval or unavailable infrastructure. Convert those rows into
  implementation checklist items and create the shared runtime family, even if that requires
  redesigning the route structure. `unsupported_boundary` is reserved for external dependencies,
  effectful/platform-gated environments, explicitly rejected unsafe semantics, or work that has a
  recorded feasibility reason and a replacement design path.
- Leave the top-level item unchecked until every required nested checkbox is checked, validation is
  recorded, unsupported paths remain explicit, and the completed summary has been moved to the
  completed ledger after merge or session completion.
- When a nested checkbox becomes too large for one coherent PR/session, promote it to its own
  top-level Planned item and replace the nested row with a link to that promoted item.
- Move a completed item summary to the completed ledger after merge or session completion. The
  ledger entry must name the closed checklist, evidence commands/artifacts, PR or commit, claim
  boundary, and any residual work that was promoted to a new Planned item.
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
- Intake review: for externally supplied lists or audits, which candidate rows were accepted,
  merged with existing work, already addressed, or deferred, and why.
- V1 scope classification: `required_for_v1`, `v1_candidate_pending_feasibility`,
  `deferred_out_of_v1`, `documentation_only`, or `unsupported_boundary` for
  production-shift items.
- ShardLoom technique review: whether PulseWeave, capillary work units, dynamic admission/work
  shaping, metadata-first execution, timing-surface separation, or evidence-tier controls apply; if
  not applicable, the item must explain why.
- Execution checklist: nested checkbox rows for the concrete implementation, test, evidence,
  benchmark, docs/site, and ledger steps needed to close the item.
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
  `docs/release/ci-work-shaping.md`,
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

- [ ] `CLICKBENCH-OLAP-RUNTIME-COVERAGE-1` Add real Vortex-native OLAP runtime coverage for the
  ClickBench query family.
  - Source: June 18, 2026 user requirement plus the official ClickHouse ClickBench repository and
    canonical `queries.sql` surface. ClickBench currently describes a 43-query OLAP workload over
    the `hits` table covering clickstream/web analytics, structured logs, and events data. The
    canonical query set exercises full scans, filtered scans, point lookup, aggregates, distinct
    aggregates, multi-key group-by, top-K order/limit, offset, string search, length, regex replace,
    arithmetic expressions, date/time extraction/truncation, `CASE`, `IN`, `HAVING`, and wide
    repeated aggregate projection.
  - Current state: ShardLoom has real native/prepared Vortex routes for scoped Python/DataFrame
    operations and several named traditional-analytics benchmark families, but broad ClickBench SQL
    coverage is not yet a production runtime contract. Any ClickBench work must land as ShardLoom
    planner/operator/runtime support, not as benchmark-only scenarios, smoke-only caps, or external
    engine delegation.
  - Intake review: accepted as a new required runtime item rather than merging into DataFrame
    blocker coverage. The DataFrame item owns user-method parity; this item owns SQL/OLAP operator
    breadth and ClickBench query-family evidence. Existing aggregate/top-N/contains routes are
    reusable but not sufficient because ClickBench needs generalized SQL lowering and reusable
    operator kernels across all 43 query shapes.
  - V1 scope classification: `required_for_v1` for local/source and prepared/native Vortex OLAP
    runtime coverage that is feasible without external managed environments. Public superiority
    claims remain gated until full benchmark evidence exists.
  - ShardLoom technique review: metadata-first execution applies to count/min/max and segment
    pruning; dynamic admission/work shaping should split each ClickBench query into reusable scan,
    predicate, projection, aggregate, top-K, and sink units; capillary work units apply to wide
    group-by/top-K pipelines, count-distinct state, repeated aggregate projection, and offset
    draining; PulseWeave should track high-cardinality grouping, distinct state pressure,
    string-search cost, and spill-required diagnostics; timing-surface/evidence-tier controls must
    distinguish hot runtime from load/prepare/publication proof.
  - Execution checklist:
    - [x] Add a ClickBench query manifest under repo-managed benchmark/runtime fixtures containing
      all 43 canonical query texts, query IDs, required operators, input columns, expected result
      cardinality shape, and feature tags.
    - [x] Add a SQL capability classifier that maps every ClickBench query to admitted,
      implementation-required, feature-gated, or externally blocked state with stable route/work
      IDs and no fallback/external engine evidence.
    - [ ] Generalize SQL lowering for single-table OLAP plans over Vortex/prepared Vortex sources:
      projection lists, aliases, scalar arithmetic, predicates, `LIKE`/`NOT LIKE`, `IN`, `CASE`,
      `length`, regex replace admission, date/time extract/trunc, `GROUP BY`, `HAVING`,
      `ORDER BY`, `LIMIT`, and `OFFSET`.
      - [x] Promote integer not-equal predicates (`<>` / `!=`) into the native Vortex tiny
        predicate syntax so ClickBench filtered-count SQL lowers to `native_vortex_count_where`
        instead of a scenario-specific route.
      - [x] Promote no-group scalar aggregate SQL projections (`count`, `sum`, `avg`, `min`,
        `max`) over direct Vortex inputs into `native_vortex_aggregate` with a typed aggregate
        payload and ShardLoom-owned aggregate state.
      - [x] Promote simple `GROUP BY ... LIMIT` aggregate SQL projections over direct Vortex inputs
        into the same typed aggregate primitive with grouped ShardLoom-owned state and bounded
        result-row evidence.
      - [x] Promote conjunctive predicate groups, string non-empty predicates, and date-string
        comparison predicates into native expression planning for aggregate routes with explicit
        no-fallback evidence.
      - [x] Promote grouped-aggregate `ORDER BY ... LIMIT/OFFSET` into a reusable bounded
        top-K/offset route over aggregate result rows.
      - [ ] Promote raw-row `ORDER BY ... LIMIT/OFFSET` over filter/project/star projections into
        a reusable native sorted-row route; this owns ClickBench Q24-Q27 and must not be left as a
        permanent string-predicate blocker.
      - [ ] Promote SQL `LIKE`/`NOT LIKE`/substring predicates and `IN` predicates into native
        predicate kernels with deterministic case-sensitivity semantics and no external engine
        delegation.
      - [ ] Promote scalar SQL functions and expression aliases (`length`, `REGEXP_REPLACE`,
        `extract`, `DATE_TRUNC`, `CASE`, arithmetic projection) into native expression-project
        kernels before grouped/top-K execution.
    - [ ] Implement or reuse native operator kernels for plain aggregates (`count`, `sum`, `avg`,
      `min`, `max`), multi-aggregate projection, multi-key group-by, grouped top-K, count-distinct,
      point lookup, filtered string contains/LIKE, arithmetic expression projection, and bounded
      order/limit/offset.
      - [x] Create the scalar/no-group aggregate primitive, public route, SQL lowering, evidence
        fields, and focused runtime tests for `count`, `sum`, `avg`, `min`, and `max`.
      - [x] Add scalar aggregate JSONL/CSV result-row export through
        `native_vortex_primitive_row_export` with explicit decode/materialization and no-fallback
        evidence.
      - [x] Create grouped aggregate state and bounded grouped result-row routes for
        non-ordered/non-filtered grouped aggregate shapes.
      - [x] Create predicate-in-aggregate support plus grouped top-K/order/offset routes rather
        than leaving ordered or filtered ClickBench grouped rows in implementation-required state.
      - [x] Create count-distinct state for scalar and grouped aggregate routes rather than leaving
        distinct aggregate rows in implementation-required state.
      - [x] Create wide repeated `SUM(column +/- constant)` aggregate projection support through
        the shared aggregate state rather than a wide-query scenario shim.
      - [ ] Add capillary/PulseWeave memory/spill diagnostics for count-distinct and high-cardinality
        grouped top-K state now that those runtime routes exist.
      - [ ] Create remaining ClickBench expression families as real runtime routes: date/time
        extract/trunc group keys, `length` + `HAVING`, regex replace group keys, group ordinals and
        constant projections, arithmetic group-key projection, `CASE` group keys, and `IN` list
        predicates.
    - [ ] Add capillary/PulseWeave state budgeting for high-cardinality group-by, count-distinct,
      top-K heap/state, string-search scan pressure, and offset-drain cost; fail closed with spill
      diagnostics where memory/spill support is not yet certified.
    - [ ] Add ClickBench-scale fixture strategy: small deterministic local fixture for correctness,
      medium sequential UAT fixture for route/stress checks, and optional full 100M-row artifact
      runner that is never required for PR fast lanes.
    - [ ] Add correctness tests for representative query classes and a manifest coverage test that
      requires every ClickBench query to have an admitted runtime route unless it requires an
      external environment that is unavailable in local v1. In-repo feasible rows must remain open
      implementation checklist work, not accepted final blockers.
    - [ ] Add runtime/evidence tests proving `fallback_attempted=false`,
      `external_engine_invoked=false`, Vortex-native input/prepared normalization, materialization
      boundaries, and per-query route IDs.
    - [ ] Update README/docs/architecture/user-surface references so ClickBench readiness is stated
      as local evidence coverage, not as a public performance/superiority claim.
    - [ ] Add benchmark/site data fields for ClickBench lane readiness only after runtime routes and
      evidence exist; keep ClickBench performance claims claim-gated until an approved rerun.
    - [ ] Move completed detail to the ledger after validation and PR handling.
  - Next outcome: a cohesive OLAP runtime PR that introduces the manifest/classifier and promotes
    the first broad SQL operator families needed by ClickBench without creating scenario-only
    routes.
  - User-visible surface: SQL facade, Python `ctx.sql(...)`, route/capability reports, benchmark
    evidence artifacts, README/docs, and website readiness views.
  - Implementation scope: SQL parser/lowering, native Vortex primitive/provider routing,
    traditional analytics/runtime kernels where reusable, benchmark manifest generation, Python
    facade payload wiring, capability/status reports, tests, and docs.
  - Evidence required: correctness fixtures, route/evidence certificates, Native I/O evidence,
    materialization/decode evidence, memory/state diagnostics, no-fallback/no-external-engine
    fields, and benchmark manifest freshness.
  - Acceptance: all 43 ClickBench queries are represented in the manifest; every locally feasible
    query has an admitted native/prepared Vortex runtime route; any row still marked
    `implementation_required` is treated as open Planned work, not a completed boundary; no
    ClickBench-adjacent public route uses DuckDB, Polars, pandas, Spark, DataFusion, or another
    external engine; docs clearly state claim boundaries.
  - Verification: targeted SQL lowering tests, ClickBench manifest classifier tests, focused Rust
    operator tests, focused Python facade tests, `cargo check -p shardloom-cli --features
    vortex-local-primitives`, `python3 -m py_compile` for touched Python, and later full workspace
    validation only after the runtime surface is complete.
  - Non-goals: do not publish ClickBench performance claims, do not run full 100M-row benchmark in
    normal PR checks, do not add external execution engines, and do not create 43 bespoke
    scenario-only route shims.
  - Claim boundary: completion supports "ShardLoom has local Vortex-native runtime coverage for the
    ClickBench query family" only to the extent proven by admitted routes and artifacts. It does not
    support "ShardLoom is faster than ClickHouse/DuckDB/etc." without approved benchmark evidence.
  - Fallback boundary: all admitted and blocked routes must report `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [ ] `PY-DATAFRAME-DETERMINISTIC-BLOCKER-COVERAGE-1` Give every remaining deterministic
  Python/DataFrame blocker an explicit implementation track, not just the 7 locally solvable
  closeout rows.
  - Source: June 18, 2026 user-surface audit follow-up. The Python method matrix now reports 113
    method rows: 98 available, boundary-supported, or production-admitted rows and 15 deterministic
    unsupported diagnostic rows. The earlier 7 current-state solvable runtime expansion rows are
    covered by `PY-VORTEX-LOCAL-EXPORT-DISTINCT-CLOSEOUT-1`; this pass also promoted scoped
    `describe` through the existing metadata-first profile route and scoped bounded `tail(limit)`
    through a native/prepared Vortex source-order tail primitive, and scoped
    deterministic `sample(n=..., seed=...)` / `sample(n=..., random_state=<int>, replace=False)`
    and no-replacement `sample(frac|fraction=..., seed|random_state=...)` through a native/prepared
    Vortex sample primitive with explicit bounded materialization, plus
    scoped `reset_index(drop=True)` /
    `sort_index(ascending=True)` through index-state-free source-order preservation, and scoped
    `set_index(keys, drop=False)` through explicit index-state metadata. The remaining
    15 rows must not
    remain as an unowned "intentional blocker" bucket.
  - V1 scope classification: `required_for_v1`, with implementation priority after
    the Vortex/export/distinct closeout. Each method below must either become an admitted native or
    prepared Vortex route, or be narrowed to a more precise sub-shape blocker with the missing
    semantic/runtime evidence recorded. Generic "unsupported" wording is not acceptable.
  - Blocked method coverage map:
    - [ ] Sampling/order/index track: scoped deterministic `sample(n=..., seed=...)`,
      integer `random_state` aliasing, `replace=False` without-replacement semantics, and
      no-replacement fractional sampling are admitted; weighted/replacement sample variants, pandas
      RNG object parity, and broader `duplicated` variants remain open; scoped
      `duplicated(subset=..., keep="first")` is admitted through a native/prepared Vortex
      duplicate-mask primitive with explicit row-key state evidence.
      Scoped `set_index(keys, drop=False)`, `reset_index(drop=True)`, and
      `sort_index(ascending=True)` are admitted as explicit/index-state-free
      source-order-preserving shapes.
      - Admitted components: seeded no-replacement sampling, source-order `tail`, index metadata
        recording without hidden pandas state, and ascending source-order preservation.
      - Open components: weighted sampling, replacement sampling, portable RNG parity,
        `duplicated(keep="last"|False)` and nullable/nested equality parity, and index-state
        materialization when users ask for index values as data.
      - Unique ShardLoom components: source-order evidence, deterministic seed contracts,
        metadata-first duplicate checks where possible, and PulseWeave bounded selection state.
    - [ ] Reshape/nested-expansion track: scoped `melt`, scoped single-column scalar-list
      `explode`, scoped single-index/single-pivot/single-value `pivot`, and scoped
      `pivot_table` with explicit sum/count/mean aggregate policy are admitted; broader reshape
      variants remain open.
      - [x] Scoped `explode`: single declared scalar list/fixed-size-list column row expansion,
        empty-list zero-row behavior, scalar companion-column repetition, source-order limit,
        native/prepared Vortex primitive routing, JSONL/CSV row export, and explicit
        decode/materialization evidence.
      - Open `explode` variants: nullable list rows/elements, nested element structs/lists,
        multi-column explode, hidden index-state reshape, and broad pandas parity.
      - [x] Scoped `melt`: flat scalar column-to-row expansion with explicit id/value columns,
        same-typed value columns, bounded source-order output limit, and native/prepared Vortex
        row-expansion evidence.
      - Open `melt` variants: heterogeneous value type policy, nested values, hidden index-state
        reshape, and broad pandas parity.
      - Scoped `pivot` / `pivot_table`: grouping, collision handling, aggregate selection, and
        wide-output schema materialization through the native/prepared Vortex pivot primitive.
      - Open `pivot` / `pivot_table` variants: multi-index/multi-value pivots,
        custom/callable aggregates, margins/fill/dropna/index parity, and broad pandas reshape
        parity.
      - Unique ShardLoom components: capillary expansion units, spill/memory diagnostics,
        dynamic fanout admission, and no external reshape engine execution.
    - [ ] Window/summary track: scoped row-count `rolling(...).sum(...)` and scoped no-kwargs
      `describe` are admitted, while broader pandas-style percentile/options/window semantics
      remain outside the claim.
      - Admitted components: scoped `describe` over native/prepared profile evidence; scoped
        `rolling(window=<positive int>, min_periods<=window, center=False).sum(column, alias=...)`
        over one scalar numeric source-order column through the native/prepared Vortex
        rolling-window primitive.
      - Open components: time-like/calendar windows when a declared order column exists, centered
        or arbitrary frame bounds, null-skipping/validity-specific semantics, additional rolling
        aggregates beyond `sum`, and bounded state spill diagnostics.
      - Unique ShardLoom components: metadata-first summary routes, PulseWeave bounded window state,
        and capillary state fragments that avoid whole-frame materialization.
    - [ ] Conditional/value rewrite track: `mask`, `replace`.
      - `mask`: predicate-to-expression lowering, scalar replacement, null/type coercion, and
        evidence for changed columns.
      - `replace`: full-cell value rewrite, null-aware equality, scoped string replacement, and
        column/type admission.
      - Required runtime component: add a native/prepared Vortex expression-projection primitive
        that can evaluate scoped CASE/full-cell rewrite expressions; Python-only CASE lowering is
        not sufficient because public `collect()` correctly blocks computed projections without a
        native route.
      - Unique ShardLoom components: native expression registry kernels, per-column capillary
        rewrite units, and materialization evidence for rewritten outputs.
    - [ ] Callable/expression track: `apply`, scoped `pipe`, `transform`, `applymap`, `map`,
      `map_rows`, `eval`.
      - Expression-only components: scoped `eval("col = col + scalar")` numeric scalar assignment
        and `transform({"col": col("col") + scalar})` mapping-style numeric assignment are
        admitted through the native/prepared Vortex expression-project primitive; scoped
        `map(sl.column_transform(...))`, `applymap(sl.column_transform(...))`, and
        `map_rows(sl.row_transform(...))` reuse the same declarative rewrite route. Broader
        eval/string/transform/map/applymap/map_rows expressions still need ShardLoom expression IR
        parsing with deterministic type/null semantics.
      - Typed UDF components: admit registered scalar or row UDFs only when determinism,
        materialization, effects, and encoded capability are declared.
      - Plan-transform components: scoped `apply(sl.plan_transform(...))` and
        `pipe(sl.plan_transform(...))` now admit lazy ShardLoom plan transforms that return
        `LazyFrame`; arbitrary unwrapped Python callables still block.
      - Unique ShardLoom components: typed expression registry, effect policy, capability
        discovery, and stable blockers for arbitrary Python callables.
  - ShardLoom technique review: Vortex-first provider review applies before adding any new
    operator surface. Dynamic admission should split methods into admitted safe shapes and precise
    blockers instead of method-wide yes/no gates. Capillary work units should break complex
    operations into source scan, expression/kernel evaluation, stateful operator work, and sink or
    bounded collect boundaries. PulseWeave controls apply to sampling, rolling/window state,
    reshape fanout, and index/order-sensitive operators once partitioning or bounded-memory state
    is required. Metadata-first planning now answers scoped `describe` through the native/prepared
    profile route and should also answer duplicate/index/order checks from schema/statistics where
    possible before row reads.
  - Execution checklist:
    - [x] Add a design section or architecture note for the 21-method coverage map describing
      semantics, ordering/null behavior, bounded-memory expectations, and Vortex-native provider
      candidates for each track.
    - [x] Promote scoped no-kwargs `describe(...)` with optional column projection to the existing
      metadata-first native/prepared Vortex profile route; keep pandas-style percentile/options
      semantics outside the scoped claim.
    - [x] Promote scoped bounded `tail(limit)` over native/prepared Vortex read/select shapes with
      source-order final-row window evidence, explicit decode/materialization fields, and no
      fallback or external engine invocation.
    - [x] Promote scoped index metadata shapes: `set_index(keys, drop=False)` records explicit
      ShardLoom index metadata while preserving encoded row data; `reset_index(drop=True)` and
      `sort_index(ascending=True)` preserve the normalized ShardLoom plan when no hidden
      pandas-style index state exists. Default/drop=True set-index, materialized reset-index,
      descending index sort, and index-value output remain deterministic blockers.
    - [x] Implement scoped deterministic `sample(n=..., seed=...)` and
      `sample(n=..., random_state=<int>, replace=False)` through native/prepared Vortex scan plus
      ShardLoom seeded bounded row selection without replacement, with explicit
      decode/materialization evidence and no fallback or external engine invocation.
    - [x] Implement scoped deterministic no-replacement `sample(frac|fraction=..., seed|random_state=...)`
      through native/prepared Vortex scan plus ShardLoom seeded fractional row selection, with
      explicit decode/materialization evidence and no fallback or external engine invocation.
    - [ ] Implement remaining deterministic sampling/order/index sub-shapes where semantics are
      explicit: weighted/replacement sample variants, pandas RNG object parity if a portable seed
      contract exists, row-mask `duplicated`, and explicit index-state
      creation/materialization if it can be represented without hidden pandas-style state.
      - [ ] Decide and document the portable RNG contract for non-integer RNG objects; if parity
        would require pandas/numpy execution semantics, keep it blocked with a precise blocker ID.
      - [ ] Add weighted sampling only after weight-column type/null validation, cumulative-weight
        kernel evidence, and deterministic bounded selection evidence exist.
      - [ ] Add replacement sampling only after duplicate output provenance, seed replay, and bounded
        materialization evidence exist.
      - [x] Add scoped `duplicated(subset=..., keep="first")` row-mask output through a
        ShardLoom-native row-key state kernel over native/prepared Vortex scans, including
        explicit decode/materialization evidence and no external engine execution.
      - [ ] Add broader `duplicated` semantics only after nullable/nested equality,
        `keep="last"`, `keep=False`, and index-state parity evidence exists.
      - [ ] Promote explicit index-state materialization only where index values can be represented
        as ordinary ShardLoom columns without hidden frame state.
    - [ ] Implement reshape/nested-expansion routes for feasible flat/nested Vortex shapes:
      broader `explode`, broader `pivot` / `pivot_table`, and broader `melt` variants, with
      explicit cardinality expansion, memory/spill diagnostics, and no external engine execution.
      - [x] Add a scoped `explode` route for admitted scalar list/fixed-size-list shapes with
        output-row provenance, empty-list behavior, source-order limiting, native/prepared Vortex
        primitive routing, row export, and no-fallback evidence.
      - [ ] Add broader `explode` semantics only after nullable/nested/multi-column/index-state
        behavior, memory/spill diagnostics, and dynamic fanout policies are certified.
      - [x] Add a scoped `melt` route for flat scalar columns with declared id/value columns,
        same-typed value columns, native/prepared Vortex primitive routing, JSONL/CSV row export,
        and explicit decode/materialization evidence.
      - [ ] Add broader `melt` type-union policy and capillary expansion-unit diagnostics for
        heterogeneous/nested value shapes.
      - [x] Add `pivot` for one-value-per-key shapes where duplicate cell collisions are impossible
        or deterministically blocked.
      - [x] Add `pivot_table` only with explicit aggregate kernels, grouping-state evidence, and
        wide-schema materialization diagnostics.
      - [x] Add scoped `pivot` / `pivot_table` JSONL/CSV row export through the native Vortex
        primitive row-export route, including sparse wide-cell serialization and no-fallback
        evidence.
      - [ ] Add reshape-specific blockers for unbounded cardinality, nested types without provider
        support, unsupported aggregate semantics, and spill-required shapes without spill evidence.
    - [ ] Implement rolling/window routes for feasible scalar columns using native stateful kernels
      where row windows are required.
      - [x] Admit scoped row-count rolling sum over declared source-order input with positive
        integer window/min-period validation, bounded state, deterministic ordering evidence,
        native/prepared Vortex primitive routing, and explicit decode/materialization evidence.
      - [x] Add capillary window-state fragments so scoped rolling work can stream without whole-frame
        materialization.
      - [ ] Add PulseWeave pressure feedback for window-state memory and spill-required diagnostics.
      - [ ] Keep time/calendar/window-function variants, centered windows, null-skipping validity
        variants, and additional rolling aggregates blocked until their order/time-zone/null/spill
        semantics are explicitly certified.
    - [ ] Implement conditional/value rewrite routes for `mask` and `replace` through the native
      expression/kernel registry with explicit null/type semantics and materialization evidence.
      - [x] Add a native/prepared Vortex expression-projection primitive for scoped CASE/full-cell
        rewrite expressions, including parser/admission evidence and no external execution.
      - [x] Lower simple `mask(predicate, scalar)` into native conditional expression kernels with
        per-column type validation.
      - [x] Lower scalar `replace(old, new)` into full-cell non-null equality rewrite kernels with
        per-column type validation.
      - [ ] Add null-aware equality and null rewrite semantics for `mask`/`replace` variants that
        require explicit validity handling.
      - [x] Preserve scoped string replacement separately from full-cell replacement and report the
        chosen rewrite family in route evidence.
      - [x] Record changed-column evidence, materialization posture, and no-fallback/no-external
        engine status in collect and write outputs.
    - [ ] Implement callable/expression surfaces through typed ShardLoom UDF/expression registry
      contracts rather than arbitrary Python execution: `apply`, scoped `pipe`, `transform`,
      `applymap`, `map`, `map_rows`, and `eval`.
      - [x] Admit scoped `eval("col = col + scalar")` numeric scalar assignments through the
        native/prepared Vortex expression-project primitive with explicit materialization evidence.
      - [x] Admit scoped `transform({"col": col("col") + scalar})` mapping-style numeric scalar
        assignments through the same expression-project primitive.
      - [x] Admit scoped `map(sl.column_transform(...))` and
        `applymap(sl.column_transform(...))` declarative column rewrites through the same
        expression-project primitive.
      - [x] Admit scoped `map_rows(sl.row_transform(...))` declarative row-shaped rewrites through
        the same expression-project primitive while keeping Python row callables blocked.
      - [ ] Admit broader `eval` expressions only after they parse into ShardLoom expression IR and
        reject Python/numexpr object execution with stable diagnostics.
      - [ ] Add typed scalar UDF registration for broad `map`/`applymap`/`transform` only after
        determinism, null behavior, materialization, and effect policy are declared.
      - [ ] Add row UDF and broad `map_rows` callable support only after row materialization,
        schema, sandbox/effect policy, and memory contracts are explicit.
      - [x] Add scoped `apply`/`pipe` support for explicit `sl.plan_transform(...)` wrappers that
        return a normalized ShardLoom `LazyFrame`; terminal routes preserve Vortex/no-fallback
        evidence.
      - [ ] Keep arbitrary Python callable execution blocked with next actions that point to the
        typed UDF or expression-registry path.
    - [ ] For every method still not broadly admitted after its track lands, replace the current
      broad blocker with sub-shape-specific stable blocker IDs, required evidence, and concrete
      `next_action` diagnostics.
    - [ ] Add Python and Rust tests for each promoted method shape and each remaining sub-shape
      blocker, asserting `fallback_attempted=false` and `external_engine_invoked=false`.
    - [ ] Update `python/src/shardloom/context.py`, user-route capability reports, user-surface
      reference index, README/Python README, and docs generated from capability matrices so these
      remaining methods are no longer described as an unowned unsupported bucket.
    - [ ] Move completed detail to the ledger after validation and PR handling.
  - User-visible surface: `LazyFrame.sample`, `explode`, `pivot`, `pivot_table`, `melt`,
    `rolling`, `duplicated`, scoped `tail`, scoped `describe`, `mask`, `replace`, `apply`, `pipe`,
    `transform`, `applymap`, `map`, `map_rows`, `eval`, `set_index`, `reset_index`, and
    `sort_index`; matching SQL/expression front doors where relevant; capability reports and
    machine-readable diagnostics.
  - Evidence required: method-by-method capability rows, positive fixtures for admitted sub-shapes,
    negative fixtures for unsupported sub-shapes, Vortex-native provider or ShardLoom-native kernel
    evidence, memory/spill/order/null semantics where applicable, and no-fallback/no-external-engine
    proof.
  - Acceptance: all remaining blocked/pending Python method rows are owned by explicit planned or
    completed phase items; locally solvable rows are closed by
    `PY-VORTEX-LOCAL-EXPORT-DISTINCT-CLOSEOUT-1`, scoped `describe` is admitted through the
    profile family, scoped bounded `tail(limit)` is admitted through the native/prepared Vortex
    tail primitive, scoped deterministic `sample(n=..., seed=...)` / integer `random_state`
    aliasing with `replace=False` and no-replacement fractional sampling are admitted through the
    native/prepared Vortex sample primitive, scoped index metadata no-op shapes, scoped
    `melt(id_vars=..., value_vars=...)`, and scoped row-count
    `rolling(...).sum(...)` are admitted, and remaining broad variants have
    track-level implementation checklists with no generic
    unowned blocker bucket.
  - Non-goals: hidden pandas/Polars/DuckDB/DataFusion/Spark fallback, arbitrary unsafe Python
    callable execution, broad production claims without fixtures, object-store/table output, or
    performance superiority claims.
  - Fallback boundary: every admitted and blocked path must report `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `PY-VORTEX-RESIDUAL-ROUTE-PROMOTION-1` Promote residual prepared-local Python/SQL
  operators from product-local SQL runtime to native Vortex middle routes.
  - Source: June 17, 2026 local release-feature Python UAT showing
    `ctx.read(...).filter(...).select(...).limit(...).collect()` now performs Vortex preparation
    and native primitive execution. Follow-up route hardening found residual row-level
    `distinct()`/`drop_duplicates()`/`unique()`, transformed row profiling, primitive row-stream
    JSONL/CSV sinks, fanout, and broad compatibility exports must either promote to a real
    native/prepared Vortex route or block; they must not execute through scoped product-local
    `sql-local-source-smoke` as a public runtime middle.
  - Current state: universal ingest, Vortex preparation, release-user-surface feature gates, native
    primitive routes, and exact provider-backed aggregate/join/top-N/cast/contains/`write_vortex`
    routes are connected. Exact provider-backed JSONL/CSV result-summary exports are connected.
    Scoped primitive filter/project/filter-project JSONL/CSV row-stream exports and JSONL+CSV
    fanout are connected through `native_vortex_primitive_row_export` after native/prepared Vortex
    input.
    Direct SQL local-source collect routes now infer declared or extension-based inputs, prepare
    local files into Vortex, and execute the same native primitive/provider route as the equivalent
    DataFrame/Python shape. Remaining arbitrary residual operators, non-JSONL/CSV row-stream
    sinks, invalid/duplicate/unsafe fanout targets, and broad compatibility exports block before
    execution with no-fallback evidence.
  - Intake review: accepted the UAT finding as a real v1 readiness gap because ShardLoom's public
    surface should not require users to distinguish input normalization from actual runtime middle
    execution. Already-addressed candidates: direct local one-shot auto routing is blocked for
    public collect; native primitive filter/project/limit and exact benchmark-family provider routes
    are already admitted. Merged candidate: compatibility sinks must share this route-promotion
    work because their current post-prepare product-local sink boundary has the same evidence issue.
  - V1 scope classification: `required_for_v1` for any feasible local residual route; if a route
    cannot be implemented without a missing upstream Vortex provider or unsafe materialization
    contract, keep it explicitly blocked with stable diagnostics and record the blocker.
  - ShardLoom technique review: metadata-first execution applies to `profile()` via Vortex metadata,
    schema, and statistics before row materialization; dynamic admission applies to selecting native
    primitive/provider/residual-native routes versus deterministic blockers; capillary work units
    apply to splitting Vortex preparation, operator execution, and compatibility export tasks while
    preserving sequential `max_parallelism=1` defaults; PulseWeave applies to future larger local
    data shapes where prepared-state reuse, partition sizing, and export tasks can be scheduled
    without changing Python APIs; evidence-tier controls must distinguish native runtime,
    product-local post-prepare compatibility, and internal smoke-only routes.
  - Execution checklist:
    - [x] Inventory all public Python/SQL/DataFrame methods whose real UAT route has
      `vortex_ingest_performed=true` but `activation_summary.command=sql-local-source-smoke`, and
      classify each as native-route feasible, decode/export feasible, or deterministic blocker.
      June 17, 2026 pass classified residual row-level `distinct`/`drop_duplicates`/`unique`,
      bounded `profile`, fanout, non-Vortex compatibility sinks, and local-source SQL/DataFrame
      collect/write/profile wrappers; exact native/provider families remain below.
    - [x] Add route-facade diagnostics so post-prepare product-local SQL execution cannot be
      mistaken for native Vortex middle execution in activation summaries, capability rows, or
      generated docs.
      The public facade now blocks residual local-source Python/SQL collect/write/profile/fanout
      paths instead of invoking `sql-local-source-smoke`; native Vortex route fields admit
      payload-less no-argument `distinct` when it maps to the scoped primitive row-stream contract
      and keep broader distinct/dedup shapes explicitly blocked; metadata-first `profile` now has
      an admitted native Vortex route.
    - [x] Classify row-level `distinct()`/`drop_duplicates()`/`unique()` aliases at the public
      native Vortex boundary instead of routing them through product-local SQL smoke execution.
      Current state admits no-argument row-level dedup over supported scalar, boolean, and UTF-8
      native/prepared Vortex row streams with explicit decode/materialization evidence. Broader
      subset/keep variants, nested equality, nullable equality, and arbitrary SQL/DataFrame
      distinct semantics remain blocked with
      `py-vortex-route-unify-1.native_vortex_distinct_route_missing` until their own evidence
      exists.
    - [x] Implement or wrap a native Vortex bounded profile/schema/statistics route that uses
      metadata-first evidence where possible and reports any decode/materialization boundary
      explicitly.
      Current state admits base `read_vortex(...).profile()` and optional projection/limit metadata
      profiles through `native_vortex_user_profile` backed by `vortex-metadata-summary`; transformed
      row profiling remains blocked until a row-materialization profile contract exists.
    - [x] Implement Vortex-derived compatibility export routes for JSONL/CSV outputs, or keep those
      sinks blocked when a safe decode/export contract is missing; `write_vortex` remains the
      highest-fidelity native sink.
      Current state admits exact provider-backed native Vortex `result_json` exports to
      workspace-safe JSONL/CSV sinks after Vortex execution, plus scoped primitive
      filter/project/filter-project/distinct/tail/sample row-stream exports and JSONL+CSV fanout through
      `native_vortex_primitive_row_export` with explicit selected-column decode/materialization
      evidence. Broader local-source compatibility writes, unsupported fanout formats, invalid or
      duplicate fanout targets, and non-JSONL/CSV provider exports still block until their own
      Vortex-derived typed export contracts exist.
    - [x] Add Python UAT-style fixtures that sequentially exercise inferred `ctx.read(...)`,
      explicit readers, SQL, and DataFrame aliases across the promoted residual operators and assert
      `fallback_attempted=false`, `external_engine_invoked=false`, and native/runtime-middle
      evidence.
      Initial focused fixtures cover local DataFrame collect, row-level distinct collect, profile,
      and write blockers without the fake-CLI legacy public-run rewrite.
    - [x] Add Rust route/admission tests proving residual operators either select a real native
      Vortex route or fail with stable blocker IDs; no public path may silently execute
      product-local SQL as a native claim.
      Current focused route tests cover default feature-gated blockers, release-user-surface
      prepared/native execution for DataFrame and SQL local-source collect routes, extensionless
      SQL with declared `--input-format`, provider-backed JSONL result export, primitive sink
      blockers, and stale prepared-artifact reuse invalidation.
    - [x] Refresh capability/status/generated docs so `production_admitted_local_workflow`,
      `scoped_runtime_supported`, `runtime_expansion_pending`, `internal_smoke_only`, and
      `feature_gated` remain distinct and source-grounded.
      Current state separates metadata-first native profile, internal direct-compatibility smoke,
      public native `write_vortex`, provider-result JSONL/CSV exports, scoped primitive
      row-stream JSONL/CSV/fanout exports, broad compatibility sink blockers, and local
      compatibility input normalization
      into explicit capability/doc rows.
    - [x] Migrate legacy Python unit fixtures that still expect direct `sql-local-source-smoke`
      result envelopes for public local-source workflows; either promote each covered expression or
      sink shape to a real native/prepared Vortex route, or update the fixture to assert the stable
      public workflow blocker and no-fallback/no-external-engine evidence.
      Current broad-file probe
      `PYTHONPATH=python/src python3 -m unittest python.tests.test_cli_client python.tests.test_query_builder`
      passes with exact stale public direct-smoke fixtures explicitly retired, while active
      replacement fixtures cover Vortex-prepared/native collect, native profile, compatibility sink
      blockers, and no-fallback/no-external-engine route evidence.
    - [x] Move completed detail to the phased execution completed ledger after validation and PR
      handling.
  - Next outcome: a cohesive runtime PR/session that either promotes the remaining feasible
    prepared-local residual operators to native Vortex middle routes or leaves them blocked with
    deterministic diagnostics and no overclaiming.
  - User-visible surface: `ctx.read(...)`, explicit local readers, `LazyFrame.collect`,
    `LazyFrame.write_*`, SQL facade routes, `activation_summary`, capability reports, user-surface
    docs, and UAT scripts.
  - Implementation scope: `python/src/shardloom/query.py`, `python/src/shardloom/context.py`,
    `python/tests/*`, `shardloom-cli/src/public_workflow_route.rs`,
    `shardloom-cli/src/sql_local_source_runtime.rs`, `shardloom-vortex/*` provider wrappers where
    applicable, status/capability generators, and generated docs/status artifacts.
  - Evidence required: UAT transcript or test output for promoted methods; route certificates or
    native runtime envelopes; decode/materialization/export contracts for compatibility outputs;
    no-fallback and no-external-engine assertions; deterministic blocker rows for anything not
    feasible.
  - Acceptance: promoted residual methods no longer run `sql-local-source-smoke` as their runtime
    middle; compatibility sinks either write from Vortex-derived results with explicit
    materialization/export evidence or block; public status rows do not claim production/native
    support for product-local post-prepare execution.
  - Verification: focused Python UAT/unit tests; focused Rust route tests; capability/status
    validators; `cargo fmt --all -- --check`; relevant clippy/test gates for touched crates.
  - Non-goals: arbitrary SQL/DataFrame parity, object-store/table/Foundry/runtime distribution,
    external engine fallback, or broad performance claims.
  - Claim boundary: completion may claim only the named residual local routes with their exact
    native/export evidence. It does not imply performance superiority, unbounded materialization,
    arbitrary Vortex SQL/DataFrame planning, or production platform support.
  - Fallback boundary: every successful and blocked route must continue to report
    `fallback_attempted=false` and `external_engine_invoked=false`; pandas, Polars, DuckDB, Spark,
    DataFusion, and Vortex query-engine integrations remain disallowed as execution fallbacks.
  - Ledger rule: move completed detail to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `PY-RUNTIME-ACTIVATION-PROVIDER-PROMOTION-1` Make normal Python runtime activation and
  provider-backed Vortex execution unambiguous.
  - Source: maintainer June 17, 2026 runtime activation gap note and follow-up observation that the
    traditional analytics native provider appears to contain the production operator behavior but is
    still exposed through benchmark/smoke naming and package feature gates.
  - Current state: direct `.vortex` primitive and exact benchmark-family native operator shapes can
    route through the shared public workflow facade, and the provider-backed aggregate, join,
    top-N, cast/try-cast, contains, and native `write_vortex` paths reuse
    `vortex-production-runtime-run`. Normal local CSV/JSONL Python workflows are no longer
    admitted through a decoded direct local-source route; `auto` fails closed with
    `cg21.route.local_file_vortex_middle_required` unless the workflow starts from native Vortex or
    uses an admitted Vortex preparation/prepared-state route, and explicit `direct` fails with
    `cg21.route.direct_local_file_blocked`. Package/release builds now use the
    `release-user-surfaces` feature set, which enables `vortex-production-runtime` and the promoted
    provider lane; the benchmark-named `vortex-traditional-analytics-benchmark` feature remains only
    a legacy/internal compatibility alias for benchmark harness code.
  - Intake review: accepted runtime activation visibility and provider promotion as one cohesive
    readiness item because both problems share the same user-facing confusion: a successful Python
    workflow does not currently make it obvious whether ShardLoom used product-local compatibility
    execution, native Vortex primitive execution, or the provider-backed traditional analytics
    runtime. The fix must not copy benchmark harness code blindly or rebrand smoke caps as
    production behavior; reusable provider runtime pieces should be extracted, wrapped, or
    re-feature-gated behind a production runtime boundary with unchanged no-fallback evidence.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: metadata-first evidence summary applies to every Python result;
    dynamic admission applies to choosing product-local, native primitive, promoted provider, or
    deterministic blocker; capillary extraction applies to splitting reusable provider runtime
    kernels/contracts away from benchmark publication harness code; PulseWeave applies where the
    provider already owns parallelism, split planning, source-state reuse, and build/probe state.
    Do not introduce a new per-format compute stack; CSV/JSONL/Parquet/Arrow/Avro/ORC/JSON should
    remain input adapters that normalize into the shared ShardLoom logical/runtime contract.
  - Execution checklist:
    - [x] Add a compact Python `activation_summary` surface derived from the existing
      `OutputEnvelope`, covering route ID/status, execution mode, native Vortex activation status,
      required feature gate, source format, Vortex read path, scan/pushdown signals,
      source-state reuse, parallelism, decode/materialization, sink status, fallback/external
      engine flags, claim gate, and unsupported diagnostics.
    - [x] Expose `activation_summary` on normal result wrappers returned by route inspection,
      public workflow execution, local Python collect/write, direct Vortex collect/write, and
      unsupported workflow reports.
    - [x] Add a first-class public route field for `native_vortex_required_feature_gate` and label
      provider-route missing-feature blockers as `feature_gated` instead of generic missing runtime
      support.
    - [x] Update README/Python/user-surface docs so normal examples show
      `result.activation_summary.as_dict()` or selected activation fields instead of implying users
      must inspect large raw envelopes or internal benchmark commands.
    - [x] Extract or promote the reusable traditional analytics provider runtime into a
      production-named provider boundary such as `vortex-production-runtime`, leaving benchmark
      harness/publication timing logic under benchmark-specific commands.
    - [x] Decide and implement package/Homebrew/PyPI feature posture for the promoted provider so
      installed binaries either support the advertised provider lane or emit a clear
      `feature_gated` activation summary with exact installation/build next action.
    - [x] Add route-certificate and Python UAT coverage proving the normal Python benchmark/product
      surface can activate the same promoted provider route as the benchmark lane when the source is
      already Vortex or explicitly prepared.
    - [x] Promote admitted provider-backed native Vortex user routes
      (`native_vortex_user_aggregate`, `native_vortex_user_join`, `native_vortex_user_top_n`,
      `native_vortex_user_cast`, `native_vortex_user_contains`, `native_vortex_user_sink`) from
      benchmark/smoke wording to `production_admitted_local_workflow` release-surface evidence while
      retaining benchmark command aliases for benchmark harnesses.
    - [x] Add schema-shape diagnostics so benchmark-specific provider assumptions are reported as
      deterministic input/schema blockers instead of surprising users during normal Python runs.
    - [x] Move completed detail to the phased execution completed ledger after validation and PR
      handling.
  - User-visible surface: `result.activation_summary`, `route.activation_summary`,
    `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_vortex(...)`,
    DataFrame-style lazy chains, SQL facade routes, package/Homebrew capability messages, README,
    Python README, user-surface index, and v1 runtime-scope docs.
  - Evidence required: Python activation-summary tests; Rust route-field tests; no-fallback
    assertions; package feature-gate diagnostics; route-certificate proof for promoted providers;
    docs updated to distinguish product-local, native primitive, provider-backed native Vortex, and
    benchmark publication surfaces.
  - Acceptance: every normal Python result can explain what actually ran without envelope scraping;
    package/default binary native-provider blockers name the missing feature gate and next action;
    product-local compatibility workflows are not falsely described as native Vortex middle
    execution; traditional analytics runtime reuse is promoted through a production boundary rather
    than copied as benchmark harness code.
  - Verification: focused Python model/query tests; focused `shardloom-cli` public route tests;
    `cargo fmt --all -- --check`; relevant package/readiness validators before release packaging.
  - Fallback boundary: all successful and blocked paths must keep `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `PY-LOCAL-WORKFLOW-1M-PRODUCT-ROUTE-1` Promote released Python local CSV/JSONL
  workflows out of smoke-only caps.
  - Source: maintainer request on June 16, 2026 to remove synthetic caps from the released Python
    front door for 1M-row local chart/post workflows while preserving existing
    `sql-local-source-smoke` safeguards, no-fallback policy, and Vortex-native input/output
    direction.
  - Current state: `sql-local-source-smoke` intentionally caps local smoke inputs at 50,000 rows
    and must remain available as a smoke-route safeguard. The released Python
    `ctx.read(...)`/`ctx.read_csv(...)`/`ctx.read_json(...)` `LazyFrame.collect()` and
    `write_jsonl()` paths still route admitted local workflows through the smoke facade, which can
    surface `scoped SQL local-source smoke supports at most 50000 ... data rows` for normal
    product-looking local workflows. Existing native Vortex Python paths cover scoped
    primitive/report routes, but product local compatibility-source workflows need a distinct
    large-local route and clear evidence while the native Vortex middle is unified. The cap fix is
    a product-route admission boundary, not a new per-format execution stack and not proof that
    compatibility-source execution has already converged on the final native Vortex middle.
  - Intake review: accepted selective filter, filter/projection/limit, grouped count/sum, hash
    join, global top-N, clean/cast/filter/write JSONL, malformed timestamp fail-closed behavior,
    null-heavy aggregate, and nested JSON field scan as product-route admission targets rather than
    a new benchmark/test-scenario matrix. They share the same source-adapter admission, Python/SQL
    lowering, no-fallback diagnostics, and cap-removal boundary. The request to preserve existing
    smoke caps is accepted as a non-negotiable boundary; the request to avoid simply increasing
    `MAX_INPUT_ROWS` is accepted as the route-design constraint.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: capillary work units apply to input adapters, bounded output
    writes, and hash-join/group-state ownership; dynamic admission/work shaping applies to selecting
    product-local versus smoke-local versus deterministic blocker routes from plan shape and source
    format; PulseWeave applies to sequential local work scheduling, join/build/probe state, and
    local memory posture even when `max_parallelism=1` is the default; metadata-first execution
    applies to schema, required-column planning, null/dropna, and limit pushdown; evidence-tier
    controls apply to separating `smoke_supported`, `scoped_runtime_supported`, `feature_gated`,
    and `production_admitted_local_workflow`. User surfaces must remain format-neutral:
    `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, SQL, and DataFrame-style builders
    should share a logical ShardLoom plan after the input-adapter boundary. The next native-route
    item owns convergence into a Vortex-normalized middle; this item must not hide a direct
    compatibility-source path behind Vortex wording. This item must not use DuckDB, Polars, pandas, Spark,
    DataFusion, Vortex query-engine integrations, or decode-first shortcuts as execution fallback.
  - Execution checklist:
    - [x] Add a product-grade local workflow route distinct from `sql-local-source-smoke`; keep the
      smoke command and `MAX_INPUT_ROWS` cap intact for smoke-route safeguards.
    - [x] Route Python `ctx.read(...)`/`ctx.read_csv(...)`/`ctx.read_json(...)`
      `LazyFrame.collect()` and `write_jsonl()` workflows through the public facade without
      admitting the direct decoded local-source smoke route as a product runtime; successful public
      local-file execution must use native Vortex or an admitted Vortex preparation/prepared-state
      route, with `max_parallelism=1` default unless explicitly overridden.
    - [x] Admit the listed local workflow shapes by removing production-route synthetic caps
      without adding or changing benchmark/test scenario definitions: selective filter; filter +
      projection + limit; group by count/sum aggregation; hash join; global top-N;
      clean/cast/filter/write JSONL; malformed timestamp cast fail-closed behavior; null-heavy
      aggregate; nested JSON field scan.
    - [x] Treat raw compatibility-source Vortex normalization as an internal/public workflow
      lifecycle detail with evidence fields instead of exposing benchmark preparation as a normal
      Python user step; do not create separate per-format user-surface components beyond input
      adapters and output sinks.
    - [x] Expand `ctx.read_vortex(...)` only where existing ShardLoom-native benchmark-family
      operators have evidence; otherwise emit deterministic blockers with stable blocker IDs and
      concrete `next_action`.
    - [x] Emit machine-readable evidence for public local-file workflows and blockers, including
      route ID, support status, source format, row-count posture, `fallback_attempted=false`,
      `external_engine_invoked=false`, normalization point, materialization/decode boundary,
      `max_parallelism`, and route claim boundary.
    - [x] Update capability reports/docs so `smoke-supported`, `scoped runtime supported`,
      `feature-gated`, and `production-admitted local workflow` are distinct and not used
      interchangeably.
    - [x] Add a release/package/Homebrew readiness note for feature gates:
      `universal-format-io`, `vortex-write`, and `vortex-traditional-analytics-benchmark`.
      Do not publish.
    - [x] Keep validation focused on route/runtime contracts: direct decoded local-source smoke
      remains internal-only and capped; public local-file facade routes fail closed unless they use
      Vortex preparation/prepared-state or native Vortex input; fail-closed errors preserve
      `fallback_attempted=false` and `external_engine_invoked=false`.
    - [x] Add minimal Rust/Python regression coverage only where needed for route admission,
      evidence fields, and preserved smoke-route cap boundaries.
    - [x] Update `docs/architecture/v1-front-door-runtime-scope.md` and
      `docs/architecture/v1-vortex-runtime-scope.md` with current route scope, blockers, and
      feature-gate posture.
    - [x] Move completed detail to the phased execution completed ledger after merge/session
      completion.
  - Next outcome: a cohesive runtime/docs/tests PR where normal released Python local CSV/JSONL
    workflows either exercise listed scenarios through an admitted Vortex-prepared/native route or
    fail closed with deterministic Vortex-middle diagnostics, without touching external engines or
    removing the internal smoke route.
  - User-visible surface: Python `ctx.read_csv`, `ctx.read_json`, `LazyFrame.collect`,
    `LazyFrame.write_jsonl`, capability reports, docs, and machine-readable execution evidence.
  - Implementation scope: `python/src/shardloom/query.py`, `python/src/shardloom/client.py`,
    `python/tests/*`, `shardloom-cli/src/public_workflow_route.rs`,
    `shardloom-cli/src/sql_local_source_runtime.rs` or a new product-local runtime module,
    `shardloom-cli/src/status_capabilities.rs`, command registry/capability validators, and
    affected docs/readiness artifacts.
  - Evidence required: route/admission tests; no-fallback evidence; explicit unsupported
    diagnostics; local-source Native I/O and materialization/decode evidence; feature-gate
    readiness note.
  - Acceptance: admitted 1M-row workflows do not emit the 50k smoke-cap error; unsupported plans
    block before execution with stable diagnostics; no external engine is invoked; existing
    `sql-local-source-smoke` behavior remains capped; docs/capabilities no longer conflate smoke
    support with product-admitted local workflow support.
  - Verification: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D
    warnings`; `cargo test --workspace --all-targets`; `PYTHONPATH=python/src python -m unittest
    python.tests.test_cli_client`; focused route/cap regression tests run sequentially.
  - Non-goals: no package publication; no object-store/table/distributed/Foundry proof; no
    hidden pandas/Polars/DuckDB/Spark/DataFusion fallback; no broad arbitrary SQL/DataFrame parity
    claim; no increase-only patch to `MAX_INPUT_ROWS`.
  - Claim boundary: public local-file Python CSV/JSONL workflow support only when the route includes
    Vortex preparation/prepared-state or native Vortex input with recorded evidence; no production
    object-store, lakehouse, Foundry, or superiority claim without separate benchmark/release
    evidence.
  - Fallback boundary: all successful and blocked paths must report `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `PY-VORTEX-ROUTE-UNIFY-1` Native Vortex route unification for Python and SQL user
  operations.
  - Source: maintainer request on June 16, 2026 to make
    `ctx.read_vortex(...).filter(...).select(...).group_by(...).join(...).nlargest(...).write_*()`
    and equivalent SQL lower into the native Vortex runtime instead of only scoped primitives,
    report paths, or named benchmark scenario routes.
  - Current state: v0.1.x Python exposes familiar lazy query-builder methods and some Vortex
    collect paths already use local primitive commands for filter/project/count. Exact
    benchmark-family Python/DataFrame and SQL shapes now lower through real native Vortex
    provider-backed routes for aggregate, hash join, top-N, cast, contains, and `write_vortex`.
    Broader arbitrary Python/SQL ETL chains, general multi-input joins, multi-output sinks, and
    benchmark/publication route-certificate refresh are still not unified under one general route
    contract. Input formats should be unique only at source-adapter boundaries; Python, SQL, and
    DataFrame-style builders should lower into the same ShardLoom logical plan and native Vortex
    runtime contract after normalization. Unsupported behavior must remain deterministic and
    no-fallback.
  - Intake review: accepted the route unification, operator coverage, native Vortex join/state,
    typed result/sink contract, capability gating, and evidence/test candidates as one coherent
    runtime section because they share the same Python/SQL lowering, native-route admission, and
    no-fallback validation surface.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: metadata-first execution and Vortex-first provider checks apply to
    source/schema/statistics handling; capillary work units apply to input normalization,
    per-operator route admission, and bounded sink writes; dynamic admission/work shaping applies to
    choosing native primitive, benchmark-equivalent native route, or deterministic blocker by
    operator set; PulseWeave applies to multi-input join/build/probe state and bounded execution
    evidence; timing-surface and evidence-tier controls apply when comparing Python-route rows to
    benchmark route rows. Do not add generic DataFrame fallback, per-format execution stacks, or
    decode-first shortcuts.
  - Execution checklist:
    - [x] Add a native Vortex user-route capability contract that maps Python and SQL operations to
      `supported`, `supported_with_materialization_boundary`, or deterministic blocker states with
      stable diagnostic codes, `fallback_attempted=false`, and `external_engine_invoked=false`.
      - Evidence note: `public_workflow_route` and `public_workflow_run` now emit
        `native_vortex_user_route_contract_schema_version`,
        `native_vortex_operation_family`, `native_vortex_capability_status`,
        `native_vortex_required_evidence`, `native_vortex_next_action`,
        `typed_result_contract`, `typed_sink_contract`, and
        `decode_materialization_boundary`; public run envelopes attach the same fields with the
        `public_workflow_` prefix.
    - [x] Define a single user-surface lowering path where `ctx.read(...)`, explicit format readers,
      SQL, and DataFrame-style lazy chains produce the same logical plan after source-adapter
      normalization; keep format-specific code at input adapters and output sinks.
      - Evidence note: public local-file workflows, direct `.vortex` primitives, exact native
        Vortex provider-backed shapes, and SQL/DataFrame-style surfaces now enter the same
        `public_workflow_route`/`public_workflow_run` facade with source/format-specific handling
        kept at adapter and sink boundaries. Broad arbitrary Vortex SQL/DataFrame planning remains
        explicitly blocked outside the exact admitted shapes.
    - [x] Route `ctx.read_vortex(...).filter(...).select(...).limit(...).collect()` and equivalent
      SQL through the same native primitive path and evidence fields, including decode/materialize
      boundaries for bounded Python rows.
      - Evidence note: Python/DataFrame and SQL Vortex primitive collect/count paths now call the
        shared `public_workflow_run` facade with real `surface`, `plan_summary`/`sql_statement`,
        `execution_policy=native_vortex`, and primitive payloads before dispatching to
        `vortex-run`, `vortex-count-where`, `vortex-filter`, `vortex-project`, or
        `vortex-filter-project`. Follow-up review resolution moved primitive payload inference into
        `public_workflow_route`/`public_workflow_run` as well, so payload-less facade calls such as
        `ctx.read_vortex(...).select(...).limit(...).route()` admit the real native Vortex route
        instead of reporting a synthetic missing-route blocker.
    - [x] Admit grouped count/sum, null-heavy aggregate, cast/try-cast, substring contains, global
      top-N, and declared JSONL/CSV/Vortex sink chains only when the Python expression lowering and
      native route support are present; otherwise block before execution with actionable
      diagnostics.
      - [x] Add Python/native Vortex deterministic blocker routing for aggregate, join state,
        global top-N, cast/try-cast, substring contains, and declared sinks without invoking
        external engines or adding per-format user-surface APIs.
      - [x] Add route-level operation-family blockers for unshaped native Vortex query, aggregate,
        join, top-N, cast/try-cast, substring contains, and sink families so route inspection and
        run envelopes agree before execution.
      - [x] Promote exact benchmark-family Python/DataFrame and SQL shapes for grouped count/sum,
        null-heavy aggregate, hash join, global top-N, clean/cast/filter, malformed timestamp
        cast, substring contains, and `write_vortex` result sinks to real native Vortex
        provider-backed routes via `vortex-production-runtime-run`, with provider
        scenario/right-input evidence in public run envelopes.
        - Evidence note: route support/status fields now classify admitted provider routes
          (`native_vortex_user_aggregate`, `native_vortex_user_join`, `native_vortex_user_top_n`,
          `native_vortex_user_cast`, `native_vortex_user_contains`, and
          `native_vortex_user_sink`) as `scoped_runtime_supported` with
          `native_vortex_user_operator_provider` middle status, matching the admitted runtime path.
      - [x] Promote exact provider-backed JSONL/CSV result exports from Vortex-native workflows once
        the bounded result/decode/export contract is explicit; `write_vortex` remains the
        highest-fidelity native sink route.
    - [x] Add v1-scoped native Vortex multi-input join state for the admitted Python/SQL hash-join
      provider route, keeping declared right-input/build-probe state inside ShardLoom-native
      execution instead of compatibility fallback.
      - Evidence note: exact hash-join Python/DataFrame and SQL shapes now pass
        `native_vortex_right_input` through the public workflow facade to
        `vortex-production-runtime-run`, with route certificate rows proving
        `native_vortex_user_join`, provider scenario `hash-join`, `fallback_attempted=false`, and
        `external_engine_invoked=false`. Arbitrary multi-input native Vortex joins remain outside
        the v1 claim and continue to block deterministically.
    - [x] Define the typed result/sink contract for bounded `collect()` rows versus
      `write_jsonl`, `write_csv`, `write_parquet`, `write_arrow_ipc`, and `write_vortex`, including
      explicit decode/materialization and metadata-loss evidence.
      - Evidence note: current supported primitives expose bounded scalar/row result contracts;
        product-local workflows expose declared compatibility sink contracts; exact provider-backed
        native Vortex `write_vortex` chains expose
        `native_vortex_result_sink_with_replay_verified_artifact`, while provider-backed
        `write_jsonl`/`write_csv` chains expose
        `native_vortex_provider_result_json_export_with_workspace_safe_sink`. Scoped primitive
        filter/project/filter-project/distinct/tail/sample row-stream `write_jsonl`/`write_csv`
        and JSONL+CSV fanout chains expose
        `native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink` with explicit
        selected-column decode/materialization evidence.
    - [x] Add minimal Python/SQL contract fixtures, not new benchmark scenario definitions,
      covering single input, multiple inputs, multiple outputs, chained operations, blocked
      unsupported operators, and no external engine invocation.
      - [x] Add/update positive single-input Python/DataFrame, session, and SQL fixtures proving
        native Vortex count/filter/project/filter-project/limit routes use the public facade with
        `fallback_attempted=false` and `external_engine_invoked=false`.
      - [x] Add/update provider-backed fixtures proving the exact Python/DataFrame and SQL
        benchmark-family aggregate, join, top-N, cast, contains, and `write_vortex` sink shapes
        lower through the public native Vortex route facade with `fallback_attempted=false` and
        `external_engine_invoked=false`.
      - [x] Keep blocked fixtures for unadmitted Vortex shapes and broad compatibility sinks.
      - [x] Add broader fixtures only when real multi-input joins, multi-output sinks, and typed
        native Vortex sink contracts are implemented.
        - Evidence note: v1-scoped hash join, native `write_vortex`, and provider-result JSONL/CSV
          sink fixtures now cover the admitted multi-input and provider sink routes; primitive
          row-stream JSONL/CSV and fanout fixtures cover the admitted multi-output route. Broader
          arbitrary multi-input joins, non-JSONL/CSV row-stream sinks, and unsupported fanout
          targets remain explicit blockers.
    - [x] Add benchmark/publication evidence or route certificate rows proving the native Python
      route matches the named benchmark route for the admitted scenario families before making any
      performance claim.
      - Evidence note: `ShardLoomContext.native_vortex_provider_route_certificate_report()` and
        `scripts/check_v1_vortex_runtime_scope.py` now record certificate rows for exact
        grouped-aggregation, null-heavy aggregate, hash join, global top-N, clean/malformed casts,
        substring contains, and native `write_vortex` sink provider routes. The report is
        side-effect-free and keeps `performance_claim_allowed=false`.
    - [x] Update README, Python README, user-surface index, architecture docs, website examples, and
      generated/reference artifacts so normal user snippets and advanced benchmark snippets no
      longer conflict.
      - Evidence note: README, Python README, the user-surface index, website Python field guide,
        v1 front-door scope, and v1 Vortex runtime scope now point to the simple
        `sl.context()`/`ctx.read(...)` user surface and the exact native provider route
        certificate report for advanced direct-Vortex shapes.
    - [x] Move completed details to the ledger after implementation, validation, PR handling, and
      any required benchmark or route-certificate refresh.
  - Next outcome: a cohesive runtime PR/session that starts by unifying native Vortex filter,
    project, limit, collect, and deterministic capability/blocker evidence for Python and SQL, then
    promotes additional operator families only where ShardLoom-native support is real.
  - User-visible surface: Python `sl.context()`, `ctx.read_vortex`, `ctx.read`, lazy query-builder
    methods, `ctx.sql`, result objects, write helpers, capability reports, README/docs, website
    examples, and benchmark route evidence.
  - Implementation scope: `python/src/shardloom/query.py`, `python/src/shardloom/context.py`,
    `python/src/shardloom/native_route.py`, `python/src/shardloom/client.py`, Python tests,
    `shardloom-cli` local primitive commands, `shardloom-vortex` native primitive/join/sink
    surfaces, route/evidence validators, docs/reference, README/Python README, website source, and
    generated static artifacts as behavior moves.
  - Evidence required: Python unit/UAT tests, SQL parity tests, Rust primitive/join/sink tests where
    runtime behavior changes, execution certificates, Native I/O evidence, typed result/sink
    evidence, deterministic unsupported diagnostics, no-fallback fields, and route/benchmark
    evidence before performance claims.
  - Acceptance: admitted Python and SQL Vortex chains execute through ShardLoom-native routes with
    correct results, explicit materialization/decode boundaries, no external engines, and stable
    fallback-disabled evidence; unsupported operators fail before execution with deterministic
    blockers; docs and website examples reflect the same supported surface.
  - Verification: focused Python tests for query builder/native route lowering, SQL/Vortex parity
    tests, targeted Rust primitive/join/sink tests for changed runtime code, user-surface reference
    validator, docs/static checks, `cargo fmt --all -- --check`, focused `cargo test` packages, and
    broader CI-equivalent checks when runtime contracts move.
  - Non-goals: no Spark/DataFusion/DuckDB/Polars/pandas execution fallback, no arbitrary ANSI SQL
    claim, no broad DataFrame parity claim, no unbounded materialization convenience path, no
    object-store/table/distributed/live/Foundry production claim, and no performance superiority
    claim without current benchmark evidence.
  - Claim boundary: scoped native Vortex user-route support only for explicitly admitted operators,
    inputs, and sinks. Report-only, planned, and unsupported surfaces remain blocked until they have
    runtime evidence.
  - Fallback boundary: every admitted route and blocker must report `fallback_attempted=false` and
    `external_engine_invoked=false`; external engines remain baselines/oracles only.
  - Ledger rule: completed details move to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `RELEASE-PACKAGE-0.1X-BUNDLED-CLI-1` Python package bundled CLI binary resolution for
  managed development environments.
  - Source: June 16, 2026 package/UAT feedback after live package simulation showed normal
    `sl.context()` works when `shardloom` is on `PATH`, but PyPI-only managed environments such as
    Foundry dev repos still need `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, or a source checkout
    binary.
  - Current state: published v0.1.x PyPI is a Python client surface over an external CLI
    installation. This branch teaches `ShardLoomClient` to resolve an explicit `binary`,
    `SHARDLOOM_BIN`, source-checkout `repo_root` binaries, bundled wheel CLI resources, then
    `shardloom` on `PATH`; the release dry-run stages a bundled platform wheel and proves clean
    venv resolution without `SHARDLOOM_BIN` or `SHARDLOOM_REPO_ROOT`. Public publication of the
    bundled patch wheel is still a release action, not an implementation claim. README, package
    docs, and website examples distinguish normal `ctx.read(...)` code from schema-pinned
    benchmark/source checkout code.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: dynamic admission applies to binary resolution and selected wheel
    platform tags; metadata-first release evidence applies to wheel contents, checksums, SBOM,
    provenance, and no-download policy. PulseWeave and capillary runtime controls do not apply
    directly because this is package resolution, not query execution, but release evidence-tier
    controls must keep package access separate from production/performance claims.
  - Execution checklist:
    - [x] Decide and document the package strategy: bundled platform wheels in `shardloom` versus a
      companion platform package; reject runtime binary download unless a later explicit RFC
      approves network/provenance side effects.
    - [x] Add a packaged-binary resolver path that checks bundled wheel resources before `PATH`
      while preserving explicit `binary`, `SHARDLOOM_BIN`, and source-checkout `repo_root`
      precedence.
    - [x] Add platform wheel build/release wiring for the selected patch release scope, starting
      with the platforms that can support Foundry/dev-env and local maintainer proof without
      weakening Apache-2.0 license/provenance or no-fallback constraints.
      - Evidence note: `scripts/release_dry_run_proof.py` now stages the source package under
        `target/release-dry-run-proof/python-package-stage`, copies the built `shardloom` CLI into
        `shardloom/bin/<system-arch>/`, builds the wheel/sdist from that stage, and
        `python/setup.py` marks wheels as platform-specific when the bundled native CLI is present.
        The PyPI Trusted Publisher draft workflow now uses the same staging/build helpers and
        uploads `target/pypi-python-package-stage/dist/*`, so release publication cannot bypass the
        bundled-CLI wheel contract.
    - [x] Add resolver tests proving bundled binary discovery, explicit override precedence,
      deterministic missing-binary diagnostics, executable-bit handling, and no runtime download.
      - [x] Add focused resolver tests for bundled-before-`PATH`, `SHARDLOOM_BIN` override
        precedence, non-executable bundled resource handling, deterministic missing-binary
        diagnostics, and the no-runtime-download resolver order.
    - [x] Add clean-venv package smoke proof with no `SHARDLOOM_BIN`, no `SHARDLOOM_REPO_ROOT`, and
      no Homebrew dependency: `import shardloom as sl; ctx = sl.context(); ctx.smoke_check();
      ctx.read(...).limit(...).collect()`.
      - Evidence note: clean venv and optional clean Conda smokes now remove
        `SHARDLOOM_BIN`/`SHARDLOOM_REPO_ROOT`, assert that `ShardLoomClient().binary_command()`
        resolves through installed `shardloom/bin/<system-arch>/`, and record
        `wheel_client_resolved_bundled_cli`.
    - [x] Update README, Python README, package install docs, release/channel matrices, website
      source, generated website output, and maintainer handoff docs so managed Python installs no
      longer require user code to pass binary paths when a supported bundled wheel is installed.
      - Evidence note: README, Python README, package install docs, package readiness docs, release
        dry-run proof docs, and inclusion matrix distinguish historical external-CLI installs from
        the current bundled-wheel package path; website source already uses the simple
        `sl.context()` surface and does not require binary-path examples.
    - [x] Add release validators/channel proof fields for bundled CLI wheel contents, checksums,
      SBOM/provenance, clean install/uninstall, Homebrew coexistence, and rollback/yank policy.
      - Evidence note: the dry-run transcript now emits `bundled_cli_stage_status`,
        `bundled_cli_platform_tag`, `bundled_cli_resource`,
        `wheel_import_and_client_smoke_without_shardloom_bin`, and
        `wheel_client_resolved_bundled_cli`; channel matrices remain publication records and do not
        claim package publication for unreleased future wheels.
    - [x] Move completed details to the ledger after implementation, validation, and patch-release
      PR handling.
  - Next outcome: a cohesive patch-release packaging PR that makes `sl.context()` usable from a
    Python-only managed environment on supported wheel platforms without `SHARDLOOM_BIN`.
  - User-visible surface: PyPI wheel, Python `sl.context()`, package install docs, release matrix,
    website quickstart, and Foundry/dev-env package smoke instructions.
  - Implementation scope: `python/src/shardloom/client.py`, Python package metadata/build config,
    release scripts, package-channel validators, docs/release, README/Python README, website source
    and generated static output, and focused resolver/package smoke tests.
  - Evidence required: resolver unit tests, clean-venv wheel smoke, package metadata proof,
    checksum/SBOM/provenance proof, no-fallback fields, no external-engine fields, and no runtime
    network download.
  - Acceptance: on supported platform wheels, `python -m pip install shardloom==<patch>` followed
    by `import shardloom as sl; ctx = sl.context(); ctx.smoke_check()` works without
    `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, Homebrew, or source checkout; unsupported platforms
    fail with deterministic installation or binary-resolution diagnostics.
  - Verification: focused Python resolver tests, clean package install smoke, release-channel
    validator, website/static checks, and CI package build jobs for the selected wheel platforms.
  - Non-goals: no runtime binary downloads, no hidden network effects, no Spark/DataFusion fallback,
    no production Foundry claim, no object-store/table/live/distributed production claim, no
    performance superiority claim, and no broad SQL/DataFrame parity expansion.
  - Claim boundary: package ergonomics only. This may support Foundry/dev-env trials after install,
    but it is not production Foundry proof until a real Foundry environment supplies workload
    evidence.
  - Fallback boundary: `fallback_attempted=false` and `external_engine_invoked=false` remain
    required in smoke reports; the bundled binary is ShardLoom's own CLI, not an execution fallback.
  - Ledger rule: completed details move to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `RELEASE-V1-LOCAL-SOURCE-PACKAGE-1` Public-source/package release track without production
  environments.
  - Source: maintainer decision on June 15, 2026 to proceed with the feasible release workstreams
    after real production object-store/table/distributed/live/Foundry environments were ruled out
    for v1 certification.
  - Current state: local v1 runtime, docs/website, Python/package dry-run, and full-local benchmark
    evidence exist; actual package uploads, release tags, GitHub Release object/assets, registry
    install/uninstall smoke transcripts, signing/attestation, and production/platform claims remain
    blocked until the selected final publication event.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: timing-surface separation and evidence-tier controls apply to the
    local benchmark publication wording; dynamic admission applies to selected package channels;
    PulseWeave/capillary runtime work does not apply because this item is release orchestration,
    local proof, docs/site, and package-channel gating rather than engine operator execution.
  - Execution checklist:
    - [x] Add a machine-readable selected-track contract for source checkout, GitHub pre-release,
      TestPyPI, PyPI, local API/schema stability, Python scenario proof, local benchmark evidence,
      and docs/website/readme cleanup.
    - [x] Fix package-publication workflow validation so dynamic Python package versioning is read
      from `python/src/shardloom/_version.py`, not a nonexistent static `project.version`.
    - [x] Update README, package install docs, public-status docs, handoff docs, and website source
      to state the selected path and final-event boundary without exposing live package commands
      early.
    - [x] Add validator and regression tests for the selected-track contract and dynamic-version
      workflow guard.
    - [x] Run local/source install proof, Python user-surface proof, local benchmark scenario
      proof, timing review, package-channel/local release validators, and website/static checks.
    - [x] Move completed summary to the ledger after validation and publication-prep PR handling.
  - Next outcome: a merged release-prep PR that makes the feasible v1 source/package path explicit
    and locally validated while keeping publication and production claims fail-closed.
  - User-visible surface: README, getting-started docs, release docs, website start/home/about
    pages, package workflow, local proof transcripts, and validator reports.
  - Implementation scope: `.github/workflows/pypi-publish-draft.yml`, `scripts/*release*`,
    `docs/release/*`, `docs/getting-started/*`, `README.md`, `website-src/*`, tests, generated
    website output.
  - Evidence required: no-fallback fields, selected-channel readiness report, stable API/schema
    report, local package dry-run, Python scenario proof, timing review, benchmark manifest
    freshness, website build/static validation, and CI.
  - Acceptance: selected channels are GitHub pre-release/TestPyPI/PyPI; production environment gates
    remain blocked; package commands remain withheld until the final event; dynamic Python version
    is used by the publication workflow; local validators pass.
  - Verification: `python scripts/check_v1_local_source_package_release.py`,
    `python scripts/release_dry_run_proof.py --rows 64 --iterations 1`,
    `python examples/local-python-smoke/run.py --repo-root .`,
    `python examples/local-python-benchmark-scenarios/run.py --repo-root .`,
    `python examples/local-python-benchmark-scenarios/timing_review.py --repo-root .`,
    website build/readiness checks, focused Python release-script tests, and broad CI-equivalent
    checks as risk warrants.
  - Non-goals: no package upload, no release tag, no GitHub Release object/assets, no signing key,
    no production object-store/table/distributed/live/Foundry claim, no Spark/DataFusion fallback.
  - Claim boundary: local/source/package release preparation only; publication, production,
    performance, superiority, and Spark-displacement claims remain false until a later approved
    final event supplies proof.
  - Fallback boundary: `fallback_attempted=false` and `external_engine_invoked=false` must remain
    visible in local proof and validator reports.
  - Ledger rule: completed details move to
    `docs/architecture/phased-execution-completed-ledger.md`.

### v1 Local Closeout Status

The June 15, 2026 v1-local closeout remains current for generated docs/website output,
Python/package smoke evidence, and the committed full-local benchmark artifact. One post-release
package ergonomics item is now open above to remove the explicit CLI-binary setup burden for
supported Python-only managed environments. Production cloud/object-store, production lakehouse,
production distributed, production live/hybrid, and real Foundry claims remain fail-closed until
maintainers provide the external approvals and real service environments listed below.

- [x] `PROD-V1-5A-LOCAL` Local finished-product gate, package-channel matrix, hard-release gate,
  release rehearsal, release boundary, security/dependency/provenance, and final approval/post
  release verification scripts exist and pass in no-publication mode. Public package/release claims
  remain blocked by package-channel approval/proof, publication/API/schema approval, and per-claim
  evidence promotion.
- [x] `PROD-READY-1B-LOCAL` Object-store v1 candidate local scope is closed with provider
  abstraction, credential/redaction/no-probe policy, local-emulator/public-fixture read evidence,
  scoped staged write/sidecar commit/recovery evidence, Native I/O certificates, and explicit
  absence fields for approved real backends and production claims.
- [x] `PROD-READY-1C-LOCAL` Table/lakehouse v1 candidate local scope is closed with ShardLoom-owned
  local-manifest metadata/read/append rehearsal, Iceberg metadata/manifest/split/read evidence,
  Delta/Hudi metadata readers, source-spec review refs, local translation/no-loss reporting, and
  explicit blocked diagnostics for protocol writes, remote catalogs, delete semantics, and
  production table claims.
- [x] `PROD-READY-1D-LOCAL` Distributed v1 candidate local scope is closed with scoped in-process
  coordinator/worker fixture runtime, capillary split units, PulseWeave attempt graph evidence,
  local repartition/combine/merge, skew/backpressure evidence, fault-injection cases, Python
  wrappers, execution certificates, and explicit blocked diagnostics for remote/multi-host claims.
- [x] `PROD-READY-1E-LOCAL` Live/hybrid v1 candidate local scope is closed with bounded
  in-memory/live/hybrid fixtures, local durable checkpoint/changelog/state-store/microsegment/cold
  promotion manifests, restore/replay/partial-checkpoint evidence, Python wrappers, certificates,
  and explicit blocked diagnostics for broker, exactly-once, object-store/catalog checkpoint, and
  production streaming claims.
- [x] `PROD-READY-1G-LOCAL` Foundry v1 candidate local scope is closed with optional integration
  posture, local dev-stack proof-of-use, generated-output result/evidence dataset-shaped paths,
  Python `foundry_generated_output(...)`, and explicit blocked diagnostics for real `foundry://`,
  Artifact Repository, Compute Module, Spark/platform compute, and production Foundry claims.
- [x] `BENCH-FRESH-2026-06-15` The full-local benchmark bundle was rerun and promoted into
  `website/assets/benchmarks/latest/manifest.json`. The manifest is the source of truth for
  `benchmark_git_sha`, `generated_at_utc`, chunk refs, and row admission evidence. The promoted
  bundle has 1,920 admitted published rows, 1,200 successful ShardLoom rows, 600 hot-runtime rows,
  600 publication-proof rows, zero blocked/unsupported ShardLoom rows, and
  `performance_claim_allowed=false`.

### External Approval And Environment Gates

These are not autonomous local implementation items. Promote one back into `## Planned` only after
the required external approval, credential, publication channel, or real service environment is
available and the item can be implemented and validated without weakening no-fallback policy.

| Gate | Required external input | Current fail-closed owner |
| --- | --- | --- |
| Selected GitHub/TestPyPI/PyPI/Homebrew proof | v0.1.4 selected-channel proof is complete: GitHub pre-release assets, TestPyPI, PyPI, and Homebrew all have checked-in channel transcripts. Keep future package publication work out of Planned until a maintainer approves a new channel or patch release. | `docs/release/package-channel-readiness-matrix.json`, `docs/release/channel-proofs/*v0.1.4-transcript.json`, `.github/workflows/pypi-publish-draft.yml`, `scripts/check_package_channel_readiness.py`, `scripts/check_finished_product_readiness.py --require-public-release-ready` |
| Other package/distribution channels | Scoop, winget, conda-forge, GHCR, and crates.io require explicit future channel approval or out-of-v1 decision with transcript/provenance. Current workspace Rust crates remain unpublished. | `docs/release/package-channel-readiness-matrix.json`, `docs/release/maintainer-publication-handoff.md` |
| Public release/API/schema approval | Functional v1 surfaces are approved as stable for the v0.1.4 technical preview. Production, performance, broad runtime, Spark-displacement, future-channel, and per-claim public-support claims remain blocked until their workload-scoped evidence exists. | `docs/release/publication-api-schema-stability-gate.md`, `docs/release/per-claim-evidence-attachment-matrix.md`, `scripts/check_release_readiness.py` |
| Production object-store claim | Approved real S3/GCS/ADLS-compatible backend profile, credentials, read/write/fault/retry/backpressure evidence, production Native I/O certificates | `docs/release/production-certification-workloads.json`, object-store readiness reports, `scripts/check_production_certification_gate.py` |
| Production table/lakehouse claim | Source-spec-approved protocol write/commit scope, real conflict/rollback/recovery proof, delete/evolution semantics evidence, optional object-store table environment | `docs/release/production-certification-workloads.json`, table protocol docs, `scripts/check_production_certification_gate.py` |
| Production distributed claim | Remote worker service/environment, network coordinator, multi-host fault injection, remote shuffle/spill/backpressure, workload benchmark proving benefit | `docs/release/production-certification-workloads.json`, distributed runtime certificates, `scripts/check_production_certification_gate.py` |
| Production live/hybrid claim | Durable state/checkpoint/changelog store beyond local fixture, broker/source replay environment, idempotent output proof, benchmark/fault evidence | `docs/release/production-certification-workloads.json`, live/hybrid state reports, `scripts/check_production_certification_gate.py` |
| Real Foundry integration claim | Real Foundry Code Repository/package/import proof, transform run, dataset source/sink reports, governance/lineage/metrics datasets, Artifact Repository proof | `docs/release/production-certification-workloads.json`, RFC 0036 proof docs, Foundry proof reports |
| Benchmark publication claim | Clean committed worktree after benchmark promotion plus live authenticated pre-5J dependency freshness check immediately before claiming publication freshness | `website/assets/benchmarks/latest/manifest.json`, `scripts/check_benchmark_publication_claim_gate.py`, `scripts/check_pre_5j_dependency_freshness.py --require-live-github` |

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Closed local v1 | Package/readiness, object-store, table/lakehouse, distributed, live/hybrid, Foundry local candidate scopes, docs/website, and current full-local benchmark refresh | Completed details live in `docs/architecture/phased-execution-completed-ledger.md`; keep public/production claims blocked until the external gates above have real evidence. |
| Closed selected channel | Public package/release channels | GitHub/TestPyPI/PyPI/Homebrew v0.1.4 are published and proof-backed for technical-preview install access only. Future channels and future patch releases require a new approved release train and channel proof. |
| External gate | Real cloud/object-store, table, distributed, live/hybrid, and Foundry production environments | Maintainer must provide the real environment and approval to run credentialed/platform tests before these can become claim-grade. |
| Claim-safe current evidence | `full_local` benchmark refresh | Current website bundle freshness is recorded in `website/assets/benchmarks/latest/manifest.json`; it is evidence and optimization direction only, not a public performance/superiority/Spark-displacement claim. |

### Evidence Pointers

- Current benchmark timing snapshot and PR #1174 route/readiness context are preserved in the
  completed ledger entry `Phase-plan open-queue cleanup and completed-state ledger migration`.
- Performance route, stage, and timing-surface contracts live in
  `docs/architecture/performance-attribution-and-execution-structure.md`.
- Current source/input evidence contracts live in `docs/architecture/universal-input-contract.md`.
- Benchmark artifacts are evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no package-release
  claim, and no public freshness claim outside the promoted manifest source revision and validation
  evidence being cited.

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
- Current direct open implementation items are the v1 product/release queue, remaining
  `PERF-RUNTIME-*` optimization items, and v1-candidate production-family rows above. Reopen
  completed `PERF-DESIGN-*` or `PERF-DESIGN-*R` passes only with new current artifact, validator,
  CI, UAT simulation, or maintainer-review evidence.

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
