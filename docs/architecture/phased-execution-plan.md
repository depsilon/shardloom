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

- The ClickBench route-readiness polish, Python/DataFrame runtime-surface polish, future-contract
  blocker field alignment, and native Vortex-derived structured export closeout requested on
  June 19 were completed and moved to
  `docs/architecture/phased-execution-completed-ledger.md`. The broader native Vortex route
  unification is now closed through the `native_vortex_unified_plan` contract and ledgered in
  `docs/architecture/phased-execution-completed-ledger.md`. The scalar/null rewrite closeout,
  benchmark-equivalence constitution, external-environment gate split, and `UAT-RUNTIME-9`
  universal ingest front-door UAT hardening are also ledgered. There are no unchecked Planned items
  in this file after the latest merge; add the next cohesive runtime, release, benchmark, or cleanup
  item here before continuing implementation work.

- [x] `RUNTIME-CLOSEOUT-3` Broad SQL/Python/DataFrame language surface burn-down and residual
  promotion.
  - Source: `arbitrary_sql_python_dataframe_breadth` row in
    `target/sql-python-dataframe-parity-continuation.json`.
  - Current state: the documented local SQL/Python/DataFrame-style subset is now admitted through
    the shared Vortex-normalized runtime family, and method-level DataFrame blockers are at zero for
    the scoped matrix. The future-contract classifier is now source-of-truth in
    `python/src/shardloom/context.py`: 22 broad variants are classified as 13 repo-feasible
    contract/profile expansions, 6 unsafe callable/UDF boundaries, and 3 scoped product boundaries.
    This pass also promoted additional null-cleanup/fill shapes to runtime:
    `dropna(how="all")`, `dropna(thresh=<int>)`, and `fillna`/`fill_null` with
    `axis=0`/`index` or projection-equivalent `axis=1`/`columns` plus `inplace=False`, scoped
    schema-declared `mask(predicate, scalar-or-null)` with
    `axis=0`/`index`, `inplace=False`, and `level=None`, scoped schema-declared scalar
    `replace(...)` with `inplace=False`, no method/limit policy, scoped UTF-8 regex replacement, inferred
    heterogeneous-scalar `melt(id_vars=...)` value columns, single-aggregate `pivot_table(...)` list/mapping
    forms across `sum`/`count`/`mean`/`min`/`max`, `ignore_index=True` reshape
    no-hidden-index routes, column-nested scalar replacement mappings, plus scoped multi-assignment
    `eval("amount = amount + 5; tax = tax * 2")` over existing numeric columns. This closeout pass
    also promoted scalar/null rewrite contracts and moved larger residual repo-feasible semantic
    families into explicit follow-on runtime-design items below instead of leaving ambiguous
    blockers in the method matrix. It does not claim broad pandas/Polars compatibility or ANSI SQL
    compliance.
  - V1 scope classification: `required_for_v1` for repo-implementable deterministic language
    semantics; `unsupported_boundary` only for unsafe arbitrary Python execution, external effects
    without policy, or platform-gated integrations.
  - ShardLoom technique review: use the expression/kernel registry as the shared lowering layer,
    capillary operator units for function families, dynamic admission for semantic profiles,
    metadata-first rewrites where possible, and evidence-tier controls for effectful/UDF routes.
  - Execution checklist:
    - [x] Generate the authoritative unsupported/future-contract operation list from current
      capability reports and classify every row as implemented, repo-feasible, unsafe, or
      external-gated.
    - [x] Promote facade-level aliases and scoped parameter variants that already lower to existing
      Vortex-normalized primitives: `dropna(how="all"|thresh=...)`,
      `fillna(axis=0/index|1/columns)`, `mask(axis=0/index, inplace=False, level=None)`,
      scalar/nested-mapping `replace(...)`,
      inferred heterogeneous-scalar `melt(...)`, `ignore_index=True` reshape options,
      `pivot_table(...)` scalar/list/mapping aggregates over `sum`/`count`/`mean`/`min`/`max`,
      and multi-assignment numeric `eval(...)`.
    - [x] Add native weighted sampling: typed positive weight-column admission, deterministic
      seeded weighted selection with/without replacement, bounded state-budget evidence, decoded
      reference fixtures, and Python/SQL/DataFrame route exposure.
    - [x] Add broad row-key equality closeout split:
      - [x] Promote retained-row `drop_duplicates(subset=..., keep="first"|"last"|False)` over
        declared/projection scalar columns through native/prepared Vortex row-key retention state.
      - [x] Add nullable scalar equality profiles for duplicate removal and duplicate masks.
      - [x] Promote nested/list/struct equality and explicit hidden-index duplicate policy to
        `RUNTIME-CLOSEOUT-6` because they require a shared typed row-key value model, not a
        metadata-only label change.
    - [x] Promote broad reshape contracts to `RUNTIME-CLOSEOUT-7`: multi-column/nested explode,
      heterogeneous melt value representation, pivot/pivot_table duplicate/fill/dropna/margins
      policy, and sparse/wide state-budget evidence.
    - [x] Promote broader window/order contracts to `RUNTIME-CLOSEOUT-6` and
      `RUNTIME-CLOSEOUT-7`: time/calendar rolling windows, additional rolling aggregates,
      null-validity behavior, deterministic top-N tie policies for `keep="last"|"all"`, and
      source-order/index evidence.
    - [x] Add typed null rewrite contracts for `mask(..., other=None)`, `replace(..., value=None)`,
      null/coercion behavior across expression-project routes, and the required scalar/null
      representation model.
    - [x] Promote explicit row-number/index materialization profiles to `RUNTIME-CLOSEOUT-6`; keep
      hidden pandas-style indexes out of the current claim boundary.
    - [x] Promote typed UDF/plan-transform contracts to `RUNTIME-CLOSEOUT-8`; preserve fail-closed
      diagnostics for arbitrary Python callables and external effects until the typed contract and
      sandbox/effect policy exists.
    - [x] Promote broad semantic conformance fixtures for nulls, ordering, equality, casts, nested
      values, windows, joins, and write boundaries into the owning follow-on runtime-design items
      below.
    - [x] Update docs, capability reports, reference surface index, README, and ledger.
  - Evidence required: semantic conformance report, capability report, parity/gap validators,
    no-fallback evidence, and focused runtime tests.
  - Verification: SQL/Python/DataFrame parity tests, user-surface completion/gap validators,
    expression/kernel tests, and targeted CLI route tests.
  - Non-goals: hidden pandas/Polars execution, unsafe arbitrary Python execution, or external
    effects without explicit policy.
  - Claim boundary: broad language surface support only for implemented and certified semantic
    profiles.
  - Fallback boundary: unsupported residuals must be native ShardLoom diagnostics, not delegated
    execution.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `RUNTIME-CLOSEOUT-6` Shared row-key, row-number, and deterministic order-state runtime
  contract.
  - Source: promoted residuals from `RUNTIME-CLOSEOUT-3` plus the former
    row-key/index/top-N blocker family.
  - Current state: scalar and nullable scalar row-key deduplication, duplicate masks, source-order
    tail, fixed top/bottom-N `keep="first"`, explicit index metadata, scoped sort-index routes,
    nested/list/struct row-key equality, visible row-number projection, and top/bottom-N
    `keep="last"|"all"` are admitted through shared typed row-key/order-state runtime paths.
    Hidden pandas-style index semantics remain a scoped product boundary, while visible ShardLoom
    row-number/index metadata is runtime supported.
  - V1 scope classification: `required_for_v1` for visible row-number and deterministic typed
    row-key/order contracts that can be built locally; `scoped_product_boundary` for hidden
    pandas-style index behavior unless a separate product decision admits it.
  - ShardLoom technique review: use capillary row-key state units, PulseWeave bounded state
    accounting, dynamic admission based on key dtype/cardinality, metadata-first key schema
    validation, and evidence-tier fields for materialization/decode boundaries.
  - Execution checklist:
    - [x] Replace scalar-only dedup/top-N row-key internals with a shared typed row-key value model
      that can represent null, scalar, list, struct, and mixed nested keys deterministically.
    - [x] Add row-key equality and hashing tests for nested/list/struct values, nulls, duplicate
      keys, mixed ordering, and stable serialized key digests.
    - [x] Add a visible row-number projection primitive and Python/DataFrame/SQL lowering for
      explicit `reset_index` materialization without hidden pandas index semantics.
    - [x] Add deterministic top/bottom-N tie policies for `keep="last"` and `keep="all"` with
      source-order evidence and state-budget diagnostics.
    - [x] Update capability matrices, reference docs, and semantic conformance fixtures.
    - [x] Move completion evidence to the ledger.
  - Evidence required: native/prepared Vortex runtime tests, decoded-reference fixtures,
    materialization/decode evidence, no-fallback certificates, and user-surface validators.
  - Verification: targeted Rust primitive tests, Python query-builder tests, Python user-surface
    completion validator, SQL/Python/DataFrame parity validator, and relevant route tests.
  - Non-goals: hidden pandas index parity without explicit product approval; external engine
    fallback.
  - Claim boundary: visible ShardLoom row-key/order semantics only, not broad pandas index parity.
  - Fallback boundary: no pandas/Polars/DuckDB/Spark/DataFusion/Velox fallback.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `RUNTIME-CLOSEOUT-7` Broad reshape, rolling/window, and null-profile runtime expansion.
  - Source: promoted residuals from `RUNTIME-CLOSEOUT-3` plus
    `cg21.workflow.explode.nested_expansion_unsupported`,
    `cg21.workflow.melt.nested_or_broad_index_contract_missing`,
    `cg21.workflow.pivot.broad_reshape_contract_missing`,
    `cg21.workflow.pivot_table.broad_aggregate_reshape_contract_missing`,
    `cg21.workflow.rolling.broad_window_semantics_unsupported`,
    `cg21.workflow.dropna.null_cleanup_semantics_contract_missing`,
    `cg21.workflow.fillna.null_fill_semantics_unsupported`,
    `cg21.workflow.isna.null_mask_semantics_unsupported`,
    `cg21.workflow.notna.null_mask_semantics_unsupported`,
    `cg21.workflow.mask.alignment_callable_or_nested_contract_missing`,
    `cg21.workflow.replace.method_nested_or_mixed_dtype_contract_missing`, and
    `cg21.workflow.fanout.multi_sink_atomicity_contract_missing`.
  - Current state: scoped scalar-list and same-length multi-column list/fixed-size-list explode now
    admit scalar, nullable, list, and struct element values with explicit null-shape and typed
    nested-value evidence; heterogeneous-scalar melt with optional explicit row-number
    materialization, single-index pivot/pivot_table with duplicate fail-closed, fill, dropna,
    margins, sparse/wide state-budget evidence, source-order rolling sum/mean/count/min/max with
    valid-observation null handling and bounded centered-window lookahead, row-axis
    dropna/isna/notna, scalar/per-column literal
    fillna/fill_null for row/index and projection-equivalent columns-axis spellings,
    source-order forward-fill null profiles with optional positive limits, scalar/null mask/replace,
    scoped UTF-8 regex replace, and local JSONL/CSV fanout with staged multi-target commit and
    partial-write cleanup evidence are admitted. Single-level list-of-struct dotted explode
    projections such as `explode("items.code")` are admitted with explicit field projection
    evidence. Multi-level nested-field accessor reshape,
    time/calendar/custom rolling, column-axis result-shape null
    operations, backfill/broad fill profiles, stateful method/limit replace profiles, and non-local/effectful fanout
    destinations require explicit runtime contracts.
  - V1 scope classification: `required_for_v1` for locally implementable deterministic reshape,
    window, null, rewrite, and fanout profiles; external/effectful writes remain gated by the
    output/effect policy.
  - ShardLoom technique review: use shared reshape/window primitives, capillary state-budget units,
    PulseWeave spill/memory diagnostics, dynamic admission for cardinality and dtype widening,
    metadata-first schema validation, and evidence-tier controls for sink atomicity.
  - Execution checklist:
    - [x] Add same-length multi-column list/fixed-size-list explode with explicit cardinality-expansion
      evidence.
    - [x] Promote nullable list rows and nested list/struct explode element values through the
      shared typed nested-value model with explicit element schema and null-shape evidence.
    - [x] Add single-level dotted list-of-struct explode/projection contracts where field-path
      semantics are explicit and memory-bounded.
    - [x] Record multi-level nested-field accessor explode/projection as a future contract boundary
      requiring recursive field-path semantics and bounded memory evidence.
    - [x] Add heterogeneous melt representation and output schema policy with decoded-reference
      fixtures.
    - [x] Add pivot/pivot_table duplicate, fill, dropna, margins, wide/sparse state-budget, and
      aggregate-profile evidence.
    - [x] Add fixed-row rolling `min` and `max` aggregates with numeric input contracts.
    - [x] Add valid-observation null handling for fixed-row rolling sum/mean/count/min/max.
    - [x] Add centered fixed-row rolling windows with bounded lookahead, source-order semantics, and
      state-budget evidence.
    - [x] Record time/calendar rolling windows plus custom frames as a future contract boundary
      requiring explicit ordering, frame, timezone, and spill semantics.
    - [x] Add projection-equivalent column-axis spelling for scalar/per-column literal fillna/fill_null
      where semantics are deterministic and memory-bounded.
    - [x] Record column-axis result-shape null cleanup profiles as a future contract boundary
      requiring dynamic output-schema and full-column null-state evidence.
    - [x] Add source-order forward-fill null profiles with optional positive `limit` where state
      semantics are explicit and bounded.
    - [x] Record backfill and broad method/limit null-fill profiles as future contract boundaries
      requiring bounded lookahead and result-shape evidence.
    - [x] Add scoped UTF-8 regex replacement without Python or external-engine
      execution.
    - [x] Add scoped mixed-dtype and nested column-mapping scalar/null rewrite contracts without Python
      or external-engine execution.
    - [x] Record stateful method/limit replace contracts as future contract boundaries requiring
      source-order state semantics without Python or external-engine execution.
    - [x] Add atomic fanout commit/recovery/partial-write cleanup evidence for scoped local JSONL/CSV
      multi-sink writes.
    - [x] Update capability matrices, docs, benchmarks/site readiness fields, and ledger for the
      completed reshape/window/null/fanout surfaces.
  - Evidence required: runtime correctness fixtures, state-budget reports, output/effect policy
    evidence, no-fallback certificates, and validators.
  - Verification: targeted Rust/Python primitive tests, Python query-builder tests, user-surface
    completion validator, parity validator, and output sink validators.
  - Non-goals: arbitrary pandas/Polars parity, hidden external engine execution, or uncontrolled
    effectful writes.
  - Claim boundary: documented deterministic profiles only.
  - Fallback boundary: unsupported variants fail before execution with stable diagnostics.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `RUNTIME-CLOSEOUT-8` Typed UDF, callable, and effect policy runtime contract.
  - Source: promoted residuals from `RUNTIME-CLOSEOUT-3` plus
    `cg21.workflow.apply.python_callable_unsupported`,
    `cg21.workflow.pipe.python_callable_unsupported`,
    `cg21.workflow.transform.python_callable_unsupported`,
    `cg21.workflow.applymap.python_callable_unsupported`,
    `cg21.workflow.map.python_callable_unsupported`, and
    `cg21.workflow.map_rows.python_callable_or_row_udf_unsupported`.
  - Current state: explicit ShardLoom plan transforms, declarative column/row transform wrappers,
    and the safe in-repo `sl_fixture_double_i64` typed scalar UDF fixture are admitted through
    native expression-project lowering. Unwrapped Python callables, arbitrary UDFs, side effects,
    and plugin execution fail closed because they need typed determinism, null, sandbox, and effect
    contracts.
  - V1 scope classification: `required_for_v1` for typed deterministic in-repo UDF contracts if
    they can be made safe and auditable; `unsupported_boundary` for arbitrary untyped Python
    callable execution.
  - ShardLoom technique review: use the expression/kernel registry, dynamic admission by declared
    capability/effect, evidence-tier controls, deterministic sandbox metadata, and capillary UDF
    work units with explicit materialization boundaries.
  - Execution checklist:
    - [x] Define typed UDF metadata: input/output dtype, null behavior, determinism, effects,
      encoded capability, materialization need, sandbox policy, and license/provenance.
    - [x] Add a safe in-repo typed scalar UDF runtime fixture that lowers through ShardLoom-native
      execution and reports no fallback/external engine.
    - [x] Add Python wrapper APIs for declared typed UDFs while continuing to reject unwrapped
      Python callables with stable diagnostics.
    - [x] Add effect policy gates for filesystem/network/external writes and plugin inspection.
    - [x] Add semantic conformance fixtures, docs, capability reports, and ledger movement.
  - Evidence required: safety/threat-model notes, runtime tests, deterministic diagnostics,
    no-fallback evidence, and release-surface docs.
  - Verification: UDF/effect unit tests, Python wrapper tests, security/policy validators, and
    user-surface completion validators.
  - Non-goals: arbitrary Python execution, hidden pandas/Polars apply/map execution, or implicit
    side effects.
  - Claim boundary: typed ShardLoom UDF profiles only.
  - Fallback boundary: no external execution engines or untyped callable fallback.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `RUNTIME-CLOSEOUT-4` Front-door performance-equivalence benchmark evidence.
  - Source: `performance_equivalence` row in
    `target/sql-python-dataframe-parity-continuation.json`.
  - Current state: scoped front doors share runtime families and
    `docs/architecture/front-door-performance-equivalence-constitution.json` defines the local
    benchmark constitution. `website/assets/benchmarks/latest/front-door-performance-equivalence.json`
    now carries 27 SQL/Python/DataFrame front-door rows over the nine local benchmark scenarios,
    matched correctness digests, hot-runtime timing fields, `metadata_sink` evidence tier, and
    no-fallback/no-external-engine evidence.
  - V1 scope classification: `required_for_v1` for local technical-preview evidence; external
    publication/superiority claims remain claim-gated.
  - ShardLoom technique review: use timing-surface separation, PulseWeave run-local coalescing,
    capillary fixture slices, metadata-first unchanged-artifact reuse, and evidence-tier controls
    so benchmark overhead is attributed to front-door lowering versus runtime execution.
  - Execution checklist:
    - [x] Add a front-door equivalence benchmark constitution covering the same operations through
      SQL, Python, and DataFrame shapes.
    - [x] Define required route identity, timing surface, evidence tier, preparation, query, sink,
      decode, and lowering overhead fields for each front door.
    - [x] Add validators that fail when the constitution stops naming the shared runtime family,
      timing fields, or evidence fields.
    - [x] Regenerate scoped local benchmark artifacts and website data only after runtime
      closeout items above are complete.
    - [x] Update README/docs/website labels so claims name the selected timing surface and evidence
      tier.
    - [x] Move completion evidence to the ledger.
  - Evidence required: reproducible benchmark artifact, website/static generated data, validator
    output, and no-fallback route evidence.
  - Verification: benchmark constitution checks, benchmark artifact completeness, website
    readiness, and front-door benchmark publication gates.
  - Non-goals: public superiority/Spark-displacement claim without separate CG-5/CG-6 approval.
  - Claim boundary: local front-door equivalence evidence only; public performance, production,
    superiority, or Spark-displacement claims remain blocked until separate claim gates pass.
  - Fallback boundary: benchmark rows must execute ShardLoom runtime routes, not external engines.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `RUNTIME-CLOSEOUT-5` Object-store/lakehouse/catalog front-door runtime closure.
  - Source: `object_store_lakehouse_catalog`, `input_object_store_cloud`, and production I/O rows
    in `target/user-surface-runtime-gap-inventory-continuation.json`.
  - Current state: local object-store/table/lakehouse fixture scopes exist and capability/parity
    rows now split those local fixtures from real cloud, remote catalog, production commit, and
    Foundry production claims, which remain external-environment gates.
  - V1 scope classification: `required_for_v1` for local emulated/object-store-compatible runtime
    and table-manifest workflows that can be implemented in-repo; `unsupported_boundary` for real
    credentialed cloud, managed catalogs, and production platform claims until maintainers provide
    environments.
  - ShardLoom technique review: use capillary split planning for object ranges/files, PulseWeave
    retry/backpressure and bounded work-in-progress, metadata-first manifest/stat pruning,
    dynamic admission based on credential/effect policy, and evidence-tier controls for local
    fixture versus production claims.
  - Execution checklist:
    - [x] Split local-emulated runtime work from real external production proof in capability and
      parity rows.
    - [x] Ensure local object-store/table front doors lower through the same Vortex-normalized
      planner as file and native Vortex routes with executable fixture evidence.
    - [x] Add route/evidence fields for range reads, manifest pruning, commit sidecars,
      credential redaction, retry/backpressure, and no-fallback execution.
    - [x] Preserve deterministic blockers for real S3/GCS/ADLS/catalog/Foundry production routes
      until approved environments exist.
    - [x] Add local fixture status tests, docs, capability reports, and ledger movement.
  - Evidence required: local fixture Native I/O certificates, commit/recovery evidence,
    credential/no-probe policy evidence, no-fallback evidence, and explicit external-gate rows.
  - Verification: object-store/table/lakehouse focused tests, production certification gate in
    local/no-publication mode, release architecture tracker, and user-surface gap inventory.
  - Non-goals: credentialed cloud execution, managed catalog production writes, or Foundry
    production proof without maintainer-provided environments.
  - Claim boundary: local/emulated object-store and table workflow readiness only.
  - Fallback boundary: no Spark/DataFusion/DuckDB/Polars/Velox execution fallback.
  - Ledger rule: move completed detail after merge/session completion.

### v1 Local Closeout Status

The June 15, 2026 v1-local closeout remains current for generated docs/website output,
Python/package smoke evidence, the committed full-local benchmark artifact, and the later
package/runtime-surface polish recorded in the completed ledger. Production cloud/object-store,
production lakehouse, production distributed, production live/hybrid, and real Foundry claims
remain fail-closed until maintainers provide the external approvals and real service environments
listed below.

- `PROD-V1-5A-LOCAL`: Local finished-product gate, package-channel matrix, hard-release gate,
  release rehearsal, release boundary, security/dependency/provenance, and final approval/post
  release verification scripts exist and pass in no-publication mode. Public package/release claims
  remain blocked by package-channel approval/proof, publication/API/schema approval, and per-claim
  evidence promotion.
- `PROD-READY-1B-LOCAL`: Object-store v1 candidate local scope is closed with provider
  abstraction, credential/redaction/no-probe policy, local-emulator/public-fixture read evidence,
  scoped staged write/sidecar commit/recovery evidence, Native I/O certificates, and explicit
  absence fields for approved real backends and production claims.
- `PROD-READY-1C-LOCAL`: Table/lakehouse v1 candidate local scope is closed with ShardLoom-owned
  local-manifest metadata/read/append rehearsal, Iceberg metadata/manifest/split/read evidence,
  Delta/Hudi metadata readers, source-spec review refs, local translation/no-loss reporting, and
  explicit blocked diagnostics for protocol writes, remote catalogs, delete semantics, and
  production table claims.
- `PROD-READY-1D-LOCAL`: Distributed v1 candidate local scope is closed with scoped in-process
  coordinator/worker fixture runtime, capillary split units, PulseWeave attempt graph evidence,
  local repartition/combine/merge, skew/backpressure evidence, fault-injection cases, Python
  wrappers, execution certificates, and explicit blocked diagnostics for remote/multi-host claims.
- `PROD-READY-1E-LOCAL`: Live/hybrid v1 candidate local scope is closed with bounded
  in-memory/live/hybrid fixtures, local durable checkpoint/changelog/state-store/microsegment/cold
  promotion manifests, restore/replay/partial-checkpoint evidence, Python wrappers, certificates,
  and explicit blocked diagnostics for broker, exactly-once, object-store/catalog checkpoint, and
  production streaming claims.
- `PROD-READY-1G-LOCAL`: Foundry v1 candidate local scope is closed with optional integration
  posture, local dev-stack proof-of-use, generated-output result/evidence dataset-shaped paths,
  Python `foundry_generated_output(...)`, and explicit blocked diagnostics for real `foundry://`,
  Artifact Repository, Compute Module, Spark/platform compute, and production Foundry claims.
- `BENCH-FRESH-2026-06-15`: The full-local benchmark bundle was rerun and promoted into
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
| Selected GitHub/TestPyPI/PyPI/Homebrew proof | v0.1.8 selected-channel proof is complete: GitHub pre-release assets, TestPyPI, PyPI, and Homebrew all have checked-in channel transcripts. Keep future package publication work out of Planned until a maintainer approves a new channel or patch release. | `docs/release/package-channel-readiness-matrix.json`, `docs/release/channel-proofs/*v0.1.8-transcript.json`, `.github/workflows/pypi-publish-draft.yml`, `scripts/check_package_channel_readiness.py`, `scripts/check_finished_product_readiness.py --require-public-release-ready` |
| Other package/distribution channels | Scoop, winget, conda-forge, GHCR, and crates.io require explicit future channel approval or out-of-v1 decision with transcript/provenance. Current workspace Rust crates remain unpublished. | `docs/release/package-channel-readiness-matrix.json`, `docs/release/maintainer-publication-handoff.md` |
| Public release/API/schema approval | Functional v1 surfaces are approved as stable for the v0.1.8 technical preview. Production, performance, broad runtime, Spark-displacement, future-channel, and per-claim public-support claims remain blocked until their workload-scoped evidence exists. | `docs/release/publication-api-schema-stability-gate.md`, `docs/release/per-claim-evidence-attachment-matrix.md`, `scripts/check_release_readiness.py` |
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
| Closed selected channel | Public package/release channels | GitHub/TestPyPI/PyPI/Homebrew v0.1.8 are published and proof-backed for technical-preview install access only. Future channels and future patch releases require a new approved release train and channel proof. |
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
