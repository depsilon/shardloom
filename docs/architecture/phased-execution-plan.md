# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work follows the `Current autonomous
  execution order` list below. Completed implementation rows that only await post-merge ledger
  movement must not block the next implementation dependency.
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
- Public Python, SQL, DataFrame-style, and CLI surfaces are wrappers over the same admitted runtime
  families, not separate engines. New plan items must state which shared runtime family they lower
  into, how aliases converge, and which evidence fields prove `fallback_attempted=false` and
  `external_engine_invoked=false`. Do not create parallel capability rows for each front door when a
  shared planner/operator/sink contract is the real behavior.
- Treat input and output formats as adapter boundaries around a Vortex-normalized middle. CSV,
  JSONL/NDJSON, Parquet, Arrow IPC, Avro, ORC, Vortex, generated rows, ranges, and future sources may
  need source-specific parse/scan/write policy, but they should not receive independent user-surface
  execution stacks unless the semantics, materialization boundary, or safety evidence is genuinely
  different. Future entries must check universal ingest, SourceState/prepared-state reuse, native
  Vortex scan/provider surfaces, and declared sink contracts before adding new route names.
- Smoke-only commands, fixture caps, and test harness shortcuts are not production routes. Keep them
  only as internal/dev safeguards with explicit names and diagnostics. A future item that touches a
  public workflow must either route through the product Vortex-normalized/prepared/native path or
  implement that path; it must not raise a smoke cap, expose a smoke route as product support, or
  count smoke success as runtime readiness.
- Local transport optimizations, including the session-scoped Python worker, are transport layers
  only. They must dispatch the same command handlers, return the same typed envelopes, preserve
  route/evidence fields, and never be recorded as a separate execution provider or benchmark route.
  Plan items involving package, Python, or managed-environment performance must distinguish
  transport overhead from engine/runtime timing.
- Benchmark and UAT entries must separate official engine timing from wrapper ergonomics. ClickBench
  or other external benchmark submissions should time the ShardLoom CLI/runtime path unless a
  separate wrapper-specific entry is intentionally declared; Python UAT proves public API parity,
  no-fallback evidence, and wrapper overhead, not the primary engine ranking by default.
- Heavy local replacement-ingest UAT, full 43-query ClickBench UAT, and full workspace/release
  gates run at the end of a cohesive implementation batch, not after every intermediate
  optimization. While runtime rows are still changing, use focused unit/integration checks and
  targeted probes only when they are needed to ship/drop a specific technique.
- Focused validation entries must use exact test targets before broad gates. Rust unit filters must
  target the exact crate surface: `cargo test -p <crate> --bin <name> <filter>` for binary crates
  and `cargo test -p <crate> --lib <filter>` for library crates. Rust integration filters must use
  `cargo test -p <crate> --test <target> <filter>`, and Python checks should name the concrete
  unittest module/class/test. Prefer `python3 scripts/run_focused_checks.py` profiles for local
  agent work. Do not use bare package-level Cargo filters as focused proof because Cargo still
  enumerates integration test targets and creates avoidable slow-tail work.
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
When checkbox order and workflow order differ because a completed row is waiting only for
post-merge ledger movement, follow `Current autonomous execution order`.

Current autonomous execution order:

1. Finish the current implementation batch: Universal Ingest capillary/source-specific preparation
   gaps first, then embedded metadata/operator optimization gaps that consume those prepared
   artifacts.
2. Update docs, generated status surfaces, and focused validators from the implemented evidence.
3. Run focused PR validation only; do not run the full workspace suite or full ClickBench UAT while
   implementation rows are still changing.
4. Create/merge the cohesive PR when required checks are green.
5. After the current optimization batch is complete, run the heavy local Desktop UAT once on the
   merged build/artifact, replacing the existing prepared `.vortex` file rather than creating
   duplicate massive artifacts.
6. Start any version/release train only after that end-of-batch UAT result is acceptable.

- [ ] `COMPOUND-SHARDLOOM-RUNTIME-TECHNIQUES-1` Add zero-overhead nested technique
  composition for slow native/prepared runtime families.
  - Source: ClickBench 100M slow-family review plus follow-up architecture question about embedding
    ShardLoom techniques inside ShardLoom techniques at a finer/nanoscale level. The accepted
    design is hierarchical composition at natural Vortex/source/vector/block/segment boundaries,
    not per-row adaptive bookkeeping.
  - Current state: ShardLoom already has the right technique vocabulary: Universal Ingest,
    SourceState, VortexPreparedState, capillary work units, PulseWeave FlowInventory/ScarcityLedger/
    EndoPulse/ProofBound evidence, metadata-first pruning, encoded/dictionary execution, and late
    materialization. The remaining risk is using these as isolated route labels instead of
    composing them where a nested layer removes a dominant cost class.
  - Intake review: accepted as a runtime design item only where the nested layer eliminates or
    sharply reduces a measured dominant cost: string scanning, full group-state retention,
    wide-row reread, payload materialization, exact distinct state, expression recomputation, or
    sink/write pressure. Reject overhead-only nanoscale instrumentation, extra route splitting, or
    evidence churn around unchanged work.
  - Zero-overhead rule: do not add hot-path classification, per-row counters, extra route-control
    branches, evidence serialization, or required analysis code that can slow runtime or the normal
    execution workflow. Cost attribution may be read manually/offline from existing route fields,
    UAT transcripts, benchmark reports, or compile-time/static plan metadata only. If a proposed
    field/tool/check cannot be proven free and useful, skip it and optimize the runtime directly.
  - V1 scope classification: `required_for_v1` for runtime optimization posture and future
    ClickBench/native OLAP improvements.
  - ShardLoom technique review: compound techniques in this order: prepare-time exact
    indexes/summaries through Universal Ingest; segment/block-level pruning before reading strings
    or payload columns; encoded/dictionary/packed-key execution inside retained units; capillary
    partial state plus merge for high-cardinality groups; PulseWeave evidence at unit boundaries,
    not inside the innermost per-value loop.
  - Nanoscale boundary rule: allowed nesting levels are source, prepared artifact, segment, block/
    vector, encoded chunk, retained row reference, partial state shard, merge stage, and explicit
    output boundary. Per-row dynamic routing, per-value evidence fields, or benchmark-answer caches
    are not allowed.
  - Execution checklist:
    - [x] Remove cost-class summarization as a required deliverable; use existing route evidence
      and UAT artifacts only as manual/offline input when choosing optimizations, with no new
      runtime or normal-workflow cost.
    - [x] For string-scan costs, compose Universal Ingest prepared indexes with segment/block
      absence certificates and encoded predicate masks before row/string materialization.
      - [x] Added exact empty-substring predicate reduction in the shared native Vortex predicate
        rewrite layer: `contains("")` now becomes a validity pushdown and negated empty contains
        becomes an always-false Vortex predicate, avoiding row/string scans and materialization for
        this reusable SQL/Python/DataFrame family. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_ -- --nocapture`.
      - [x] Added exact empty `IN`-list predicate reduction in the same shared planner layer:
        positive empty `IN` becomes always false and negated empty `IN` becomes always true, avoiding
        residual predicate materialization for `isin([])`-style user-surface shapes while preserving
        current ShardLoom predicate semantics.
      - [x] Added exact singleton `IN` predicate reduction for safe cases: positive single-value
        `IN` becomes Vortex equality pushdown, `IN (NULL)` becomes null-validity pushdown, and
        `NOT IN (NULL)` becomes not-null validity pushdown.
      - [x] Added exact native Vortex expression lowering for the broader `IN`/`NOT IN` family:
        non-null membership uses equality disjunctions, mixed-null positive lists use
        `is_null OR equality`, mixed-null negated lists use `is_not_null AND NOT equality`, and
        negated non-null lists use `is_null OR NOT equality` to preserve the current ShardLoom
        null-admitting predicate contract without residual materialization.
      - [x] Added dictionary-counted UTF-8 comparison predicate counting and row-index selection:
        native Vortex count/filter/export paths now evaluate `=`, `<>`, and range comparisons once
        per used dictionary value, reuse dictionary code counts or row-code scans, skip unused
        dictionary values, and preserve SQL null comparison semantics without per-row string
        materialization. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_utf8_dictionary_ -- --nocapture`.
      - [x] Added typed primitive comparison predicate counting and row-index selection:
        native Vortex count/filter/export paths now coerce comparison literals once for
        direct/nullable `u64`, `i64`, and `f64` accessors, compare typed slices directly, and use
        row validity masks for nullable values without constructing `StatValue` rows. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_primitive_accessors_ -- --nocapture`.
      - [x] Added validity-only UTF-8 dictionary null predicate counting and row-index selection:
        `is null` / `is not null` now consult dictionary value nulls plus row null masks directly
        for native Vortex count/filter/export paths, avoiding non-null string materialization in
        missing-data and dropna-style predicates. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_null_utf8_dictionary_ -- --nocapture`.
      - [x] Added primitive validity-posture null predicate counting and row-index selection:
        direct primitive accessors answer `is null` / `is not null` from non-null type posture, and
        nullable primitive accessors answer from row validity masks without constructing
        `StatValue` rows. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_null_primitive_accessors_ -- --nocapture`.
      - [x] Added dictionary-counted `IN`/`NOT IN` predicate counting and row-index selection for
        UTF-8 dictionary accessors: native Vortex count/filter/export paths now evaluate membership
        once per dictionary value, sum exact row counts or emit row indices from dictionary codes/
        null masks, and preserve null plus negated semantics without per-row string membership
        materialization. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_ -- --nocapture`
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_multi_value_in_list_uses_vortex_equality_disjunction -- --nocapture`.
      - [x] Added typed primitive `IN`/`NOT IN` predicate counting and row-index selection:
        native Vortex count/filter/export paths now coerce membership lists once for direct/
        nullable `u64`, `i64`, and `f64` accessors, reuse typed membership state, and preserve
        null/negated semantics through validity masks without constructing `StatValue` rows.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_primitive_accessors_ -- --nocapture`.
      - [x] Added direct boolean accessor predicate/count support: native Vortex Bool filter,
        collect, aggregate, and exact distinct paths now read boolean values and validity masks
        directly for null checks, comparisons, `IN`/`NOT IN`, identity grouping, and
        count-distinct instead of constructing materialized `StatValue` rows. Numeric/date/string
        transforms still reject boolean input through deterministic ShardLoom-native errors.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_boolean_accessors_ -- --nocapture`
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib boolean_vortex_arrays_promote_to_direct_accessors -- --nocapture`.
      - [x] Added embedded UTF-8 byte-length prefilters for positive non-empty
        `contains(...)` predicates when the prepared `.vortex` artifact has the generic hidden
        length column: Vortex pushdown now applies `hidden_length >= needle_byte_len` before the
        exact ShardLoom substring residual, and impossible needles can footer-prune without opening
        the residual string scan.
      - [x] Threaded embedded-derived rewrite evidence through local Vortex count/filter scan
        results so `count_where` summaries and public workflow lifting can report
        `embedded_derived_column_rewrite_status=applied` plus the exact hidden length/domain
        columns consumed by the route. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_ -- --nocapture`.
      - [x] Added `AND` simplification after predicate rewrites so always-false exact conjuncts
        short-circuit residual string predicates and always-true exact conjuncts disappear before
        Vortex pushdown/residual splitting.
      - [x] Extended embedded-derived string pruning into row-state collect operators:
        `distinct_rows`, `drop_duplicate_rows`, and `sample_rows` now preserve hidden length
        rewrite evidence, consult Vortex footer pruning before opening the scan, and report zero
        read/decode/materialization when impossible substring predicates prune the entire artifact
        before row-key or sample-candidate state construction. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib distinct_rows_ -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib drop_duplicate_rows_ -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sample_rows_ -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
    - [ ] For group-state costs, compose capillary segment-local partials, packed/dictionary keys,
      bounded top-K retention where semantics allow, and a merge stage with memory/spill evidence.
      - [x] Added packed typed/composite grouped states, compact count/numeric measure slots,
        transformed dictionary grouping, and materialized UTF-8 chunk-local partial count merges for
        shared high-cardinality/string grouped families.
      - [x] Added transformed UTF-8 dictionary general-measure updates for grouped domain/length/min
        families so query shapes like Q29 update `avg(length(string))`, `count(*)`, and `min(string)`
        from dictionary value/count pairs instead of row-state updates. Targeted 100M local UAT over
        the retained single `.vortex` artifact moved Q29 from the previous
        `row_state_update` evidence at `35.95s` to
        `transformed_dictionary_general_measure_group_update` at `20.52s`, then a post-cleanup
        rerun over the same public native Vortex CLI surface recorded `16.48s`, with
        `fallback_attempted=false`, `external_engine_invoked=false`, and evidence saved at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q29_retained_transformed_general_probe.json`
        and `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q29_current_retained_after_cleanup_probe.json`.
        A same-column dictionary predicate shortcut was tested and removed because it did not
        produce a measured win over the retained updater.
      - [x] Avoid redundant transformed-dictionary admission scans for the common non-null
        dictionary case: grouped length/domain routes now use the dictionary null-mask posture as
        the proof and leave code bounds validation to the code-counting pass, and scalar/direct
        `COUNT(utf8_dictionary_column)` can return the selected row count directly when the same
        non-null posture is proven, removing full pre-count row walks without weakening no-fallback
        diagnostics. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_transformed_dictionary_reuses_group_transform_per_dictionary_value -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_compact_measures_transformed_dictionary_reuses_group_and_measure_dictionaries -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_general_measures_transformed_dictionary_reuses_selected_dictionary_counts -- --nocapture`.
      - [x] Reuse prepared dictionary `Arc<str>` storage inside shared aggregate string interners:
        dictionary-backed identity groups, row-wise dictionary group keys, and numeric/minute/string
        compact-state dictionary-code binding now intern the prepared dictionary value directly
        instead of allocating a fresh string-backed `Arc` on first insert. The interner also
        reserves from prepared dictionary cardinality before compact-state hot updates. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_string_interner_reuses_prepared_dictionary_arc_on_insert -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_minute_string_uses_streaming_topk_compact_state -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Move numeric+UTF8 proof-bound top-K exact recount candidate filtering from per-row
        full key construction to a per-dictionary-code retained numeric-part map. The second pass
        still proves exact retained groups, but rows whose string dictionary code is not retained
        never allocate/clone the full `(numeric, UTF8)` candidate key. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8_candidate_prefilter_maps_dictionary_codes_to_numeric_parts -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Reserve exact recount hash-map capacity from retained candidate cardinality for
        proof-bound string top-K, string count-distinct top-K, and numeric+UTF8 top-K routes at the
        second-pass prepare boundary before any exact recount updates run. This reduces exact
        proof-state growth churn without adding route branches, sidecars, or approximate answers.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Tested and rejected the naive exact count-threshold second-scan approach for
        transformed dictionary HAVING families. It reduced materialized group values from about
        `1.8M` to `74`, but the extra full native Vortex pass slowed Q29 from the retained
        `20.52s` general-measure path to `29.999s`; the rejected evidence is recorded at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q29_count_threshold_second_pass_probe.json`.
        Do not reintroduce this shape unless the count-threshold decision comes from embedded
        single-artifact metadata or prepared layout state and proves faster without sidecars,
        query-answer caches, or an extra full scan.
      - [x] Added exact typed HAVING/ORDER comparison for compact count and numeric measure aliases
        before generic result-row JSON/value materialization. Grouped aggregate HAVING now checks
        `count(*)`/compact count aliases through the same typed count ordering used by grouped
        top-K, prepares HAVING literals once per grouped result pass instead of reparsing them for
        every candidate group, avoids temporary JSON-number construction for compact count/sum/avg
        candidate comparisons, exposes typed `Float64` order values for compact sum/avg aliases,
        and reuses the same typed comparison helpers for grouped count/numeric ORDER BY
        comparisons. Source-order grouped result enumeration now applies prepared/group-aware
        HAVING before constructing output rows, and the path falls back to existing JSON comparison
        only when no typed compact value exists. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib compact_count_having_prepared_comparison_matches_json_ordering -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_order_value_comparison_reuses_typed_u64_ordering -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib compact_numeric_measure_order_value_comparison_matches_json_ordering -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_filters_orders_and_limits_rows_without_fallback -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_source_order_limit_avoids_full_group_sort_without_fallback -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_applies_expression_groups_value_transforms_and_having_without_fallback -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_missing_having_or_order_column_fails_closed -- --nocapture`,
        and `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_having_can_filter_scalar_result_without_fallback -- --nocapture`.
      - [x] Defer generic count-star top-K group-key cloning until a candidate survives the
        retained window: the shared grouped aggregate route now compares non-retained candidates
        against the current worst retained count/key by reference, clones only surviving retained
        keys for the small-window streaming path, and reports
        `topk_candidate_key_clone_strategy=clone_only_surviving_retained_candidates`. The
        select-nth large-window path keeps its bulk candidate materialization evidence separate.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_ordered_topk_uses_compact_counts_without_fallback -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_offset_topk_uses_select_nth_retention -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Defer proof-bound string top-K candidate `Arc<str>` cloning until a string candidate
        survives the retained window: string count top-K and string count-distinct top-K now compare
        borrowed candidate values against the current worst retained candidate, clone only retained
        survivors, and report clone-deferred candidate strategy fields while preserving exact
        recount/proof semantics. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Defer proof-bound numeric+UTF8 top-K composite-key cloning until a candidate survives
        the retained window: `UserID/SearchPhrase`-style count top-K routes now compare borrowed
        composite keys against the retained worst candidate, clone only retained survivors, and
        report `numeric_utf8_topk_candidate_clone_strategy` while preserving exact late recount.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount -- --nocapture`
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Move proof-bound numeric+UTF8 top-K first-pass state onto dictionary/interner IDs
        instead of full string-backed composite keys: the shared heavy-hitter route now reuses
        dictionary-bound interned string IDs during the first scan, converts to normal string keys
        only at the retained candidate/exact proof boundary, and reports
        `numeric_utf8_topk_dictionary_code_reuse=true`. This composes encoded dictionary
        execution, capillary retained-candidate proof, and late string materialization without
        changing public SQL/Python/DataFrame semantics. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_topk_preserves_declared_group_key_ties -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8_dictionary_interner_reuses_prepared_dictionary_arc -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Carry proof-bound numeric+UTF8 top-K exact recount state as interned
        `(numeric, utf8_id)` keys through the second pass, expanding UTF-8 strings only for
        retained output candidates. The route now reports
        `numeric_utf8_topk_candidate_id_prefilter=true` and
        `numeric_utf8_topk_exact_count_key_storage=interned_numeric_utf8_id`, preserving exact
        proof semantics while reducing second-pass string-key pressure for shared
        `UserID/SearchPhrase`-style SQL/Python/DataFrame routes. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8 -- --nocapture`
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Add a generic direct-accessor grouped aggregate update path for non-compact grouped
        measures when group keys and measure columns are already available as typed primitive or
        dictionary accessors. This promotes grouped `count_distinct` and other direct-measure
        families out of row-export state updates without claiming materialized accessors as direct,
        and exposes `direct_accessor_count_distinct_group_update`,
        `generic_direct_accessor_group_state`, `direct_accessor_hot_hash_map`,
        `direct_accessor_general_state+direct_count_distinct`, and
        `row_materialization_bypass` evidence. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_distinct_direct_accessor_update_avoids_row_materialization -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`.
      - [x] Added partitioned grouped aggregate regression coverage proving multi-file prepared
        Vortex routes reuse the same typed numeric-pair compact state family as the single-file
        runtime instead of falling back to row-export state. The test intentionally does not claim
        direct key-slice bypass for provider-miss/materialized integer chunks; that remains a
        separate provider-visible primitive-slice optimization. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib partitioned_grouped_aggregate_reuses_numeric_pair_compact_state -- --nocapture`.
      - [x] Added transformed-dictionary compact code-pair partials for shared domain/length
        aggregate families: dictionary-backed group transforms and dictionary-backed length
        measures now count unique `(group dictionary code, measure dictionary code)` pairs first,
        then update compact count/sum/avg state once per pair with weighted exact semantics. This
        preserves source-order grouping when needed, admits only bounded dictionary-pair cardinality
        so it does not allocate near-row-count side maps, avoids per-row compact measure updates for
        repeated code pairs, and reports
        `transformed_dictionary_compact_code_pair_partial_group_update`,
        `chunk_dictionary_transformed_compact_code_pair_group_state`,
        `transformed_chunk_dictionary_compact_code_pair_map`,
        `transformed_dictionary_code_pair_partial`, and `dictionary_pair_state_reuse` evidence.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_compact_measures_transformed_dictionary_reuses_group_and_measure_dictionaries -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib transformed_dictionary -- --nocapture`,
        `cargo fmt --all -- --check`, and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Add a metadata-backed exact HAVING/selectivity path for transformed dictionary groups
        only if it can use embedded `.vortex` layout/statistics to avoid both row-state updates and
        an extra full scan while preserving exact SQL semantics.
        - [x] Added exact count-star HAVING pre-update filtering for transformed-dictionary
          count-star and compact code-pair partials: dictionary count groups and dictionary pair
          counts compare count-star HAVING predicates against exact transformed group totals before
          group-state allocation/update. Mixed `count(*)` plus compact measure sets keep final
          generic HAVING for non-count predicates while avoiding row-state updates or extra full
          scans. Evidence reports `+count_having_prefilter`,
          `count_having_preupdate_group_filter`, and `having_selectivity_before_state_update`.
          Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_transformed_dictionary_prefilters_count_having_before_state_update -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_compact_measures_transformed_dictionary_prefilters_count_having_before_state_update -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_compact_measures_transformed_dictionary_ -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib transformed_dictionary -- --nocapture`,
          `cargo fmt --all -- --check`, and
          `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [ ] Add a real partitioned/spill-backed exact merge path for near-unique group state when
        bounded memory is insufficient; current spill posture remains diagnostic/fail-closed.
    - [ ] For wide-row/top-K costs, compose predicate posting lists or selection vectors with
      row-position locality and final-K late materialization only.
      - [x] Added dynamic row-reference top-K materialization for admitted large bounded payload
        projections: candidate scans keep predicate/order columns plus source ordinals, then reopen
        the same `.vortex` artifact only for final retained rows. Evidence reports
        `row_ref_topk_materialization_policy`, `late_materialization_retained_cap`, source row
        count, and state-budget capillary units.
      - [x] Tighten final-K wide materialization to selected row refs inside each retained chunk
        instead of decoding the whole wide chunk. Evidence reports
        `late_materialization_selected_row_refs_used` through the public route evidence lift and
        composes with existing pushdown early-stop when the filtered selected ordinal is reached.
        State-budget evidence now names `selected_row_ref_materialization` and `selected_row_refs`
        rather than hiding the work inside generic final-row materialization.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_ -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib partitioned_sort_rows_wide_output_materializes_selected_partition_ordinals -- --nocapture`,
        `cargo test -q -p shardloom-cli --features release-user-surfaces local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Promote safe root-source final-K materialization from chunk-internal selected-row export
        to upstream Vortex `ScanBuilder::with_row_indices`, and carry Vortex `row_idx()` through
        pure pushdown-filtered candidate scans so filtered top-K can still materialize final rows by
        root source ID. Residual-filtered paths remain conservative until their candidate ordinals
        are proven as root row IDs. Public evidence now reports row-index selection admission,
        requested row-index counts, source-row-id projection use, and PulseWeave/capillary work units
        `vortex_row_idx_projection` plus `row_index_selected_payload_scan`. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_late_materialization_policy_uses_row_refs_for_large_payload_topk -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_wide_projection_with_pushdown_keeps_filtered_ordinals -- --nocapture`,
        `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo clippy -q -p shardloom-cli --features release-user-surfaces --bin shardloom -- -D warnings`.
      - [ ] Add prepared predicate posting-list/row-position locality metadata inside the Vortex
        layout when Vortex exposes a safe single-artifact contract for it.
    - [x] For exact distinct costs, compose per-segment dictionary unions or dense-ID bitsets with
      exact merge contracts and decoded-reference null/duplicate parity tests.
      - [x] Corrected UTF-8 dictionary exact distinct to union only dictionary codes that are
        actually used by each chunk, preserving exactness without materializing unused dictionary
        values.
      - [x] Add a non-null UTF-8 dictionary exact-distinct fast path: when the accessor proves
        dictionary values and rows are non-null, exact distinct marks dictionary codes directly and
        skips the redundant per-row null probe before inserting used dictionary values; insertion
        reserves hash-set capacity from the exact used-code count. Nullable dictionaries keep the
        null-aware path and decoded-reference semantics. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture` and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`.
      - [x] Added a proof-bound string `count_distinct` top-K route for large exact grouped
        distinct queries: first pass builds a weighted string heavy-hitter candidate sketch from
        chunk UTF-8 dictionaries, the second pass recounts exact distinct values only for retained
        candidate strings using direct primitive accessors, and the route falls back to the
        existing exact ShardLoom-native path if the proof threshold is not satisfied. Targeted
        local 100M UAT over the retained single `.vortex` artifact moved `CB-Q14` from the latest
        full-run `24.65s` / targeted `24.77s` range to `11.15s`, then `13.88s` after public field
        lifting, with exact result parity, `fallback_attempted=false`, `external_engine_invoked=false`,
        `SearchPhrase:chunk_utf8_dictionary,UserID:direct_i64`,
        `proofbound_candidate_exact_distinct_sets`, and
        `proofbound_string_count_distinct_exact_topk`. Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_count_distinct_topk_q14_20260622T_now/summary.json`
        and
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_count_distinct_topk_q14_field_lift_20260622T_now/summary.json`.
      - [x] Move the string `count_distinct` top-K exact recount candidate filter from per-row
        string signature/hash probes to route-local interner IDs: the retained exact recount still
        scans rows for the distinct column, but dictionary group-code lookup maps to an interned
        candidate ID before the distinct-value update. This keeps exact SQL semantics, avoids
        query-answer caches or sidecars, and reuses the same ShardLoom interner key space as the
        count-only string top-K route. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Retain Vortex dictionary `Arc<str>` values inside exact UTF-8 distinct state instead
        of cloning string bytes into owned `String` keys for every used dictionary value. Decoded
        materialization still emits normal UTF-8 strings, while the hot exact distinct/group-state
        path preserves dictionary-owned allocations and reports `dictionary_arc_direct_exact`,
        `dictionary_arc_distinct_state`, and `dictionary_string_clone_bypass` evidence when that
        route is actually used. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib scalar_string_count_distinct_reports_dictionary_arc_state -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
    - [x] For repeated-expression costs, compose expression-plan fingerprinting, one-time measure
      evaluation, and shared aggregate/update state across SQL/Python/DataFrame spellings.
      - [x] Added direct and materialized fused numeric additive aggregate updates for repeated
        SUM/AVG measures over the same column/accessor, with `expression_fusion_strategy`,
        `expression_plan_fingerprint_status`, capillary reuse units, and PulseWeave pressure
        signals.
      - [x] Added grouped UTF-8 dictionary weighted transform fusion for repeated string-derived
        measures over the same prepared dictionary value: grouped aggregates now compute
        length/identity once per dictionary value for admitted SUM/AVG/MIN/MAX consumers, report
        `dictionary_weighted_transform_fusion` and
        `shared_dictionary_value_transform_update`, and lift the evidence through the public
        workflow facade.
      - [x] Reused the same weighted dictionary transform path for scalar string aggregates:
        `sum/avg/min/max(length(utf8))` over a direct UTF-8 dictionary now updates from dictionary
        value/count pairs instead of row-by-row string length recomputation, reports
        `dictionary_weighted_transform_fusion`, and preserves the public native Vortex
        no-fallback route. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib scalar_string_length_aggregate_reuses_dictionary_value_counts -- --nocapture`.
      - [x] Trimmed the retained dictionary transform fusion hot path so repeated length-only UTF-8
        aggregates do not allocate identity `StatValue::Utf8` payloads unless an identity
        `min`/`max` consumer is actually present. This keeps the same native Vortex route,
        `dictionary_weighted_transform_fusion` evidence, and deterministic no-fallback errors for
        impossible fused states while reducing string-heavy aggregate allocation.
      - [x] Reuse embedded single-artifact derived columns for aggregate measures, not only group
        keys: `url_domain(...)`, `extract_minute(...)`, and `date_trunc_minute(...)` measures now
        rewrite to the exact hidden `.vortex` columns when Universal Ingest persisted them, so
        scalar/grouped measure paths consume prepared domain/time metadata through the same
        ShardLoom-native aggregate route instead of recomputing source transforms. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_rewrites_time_transform_measures_to_embedded_vortex_columns -- --nocapture` and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_rewrites_url_domain_measures_to_embedded_vortex_column -- --nocapture`.
      - [x] Extend expression reuse to shared string/date/cast transforms across grouped operators
        after the transform registry has reusable prepared metadata instead of per-query sidecars.
        - [x] Added grouped aggregate measure coverage for embedded time transform reuse:
          grouped `sum(extract_minute(EventTime))` and `avg(date_trunc_minute(EventTime))`
          rewrite to the single-artifact hidden `.vortex` columns, preserving the shared native
          aggregate route and avoiding per-row source timestamp transform recomputation. Focused
          validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_rewrites_time_transform_measures_to_embedded_vortex_columns -- --nocapture`.
        - [x] Locked the already-promoted cast provider route at the public route boundary:
          `native_vortex_user_cast` is admitted for the shaped clean numeric cast/filter/limit and
          malformed timestamp cast/limit collect paths through the same provider-backed native
          Vortex route as the sink path, with no fallback or external engine invocation. This
          closes the route-admission ambiguity; grouped reusable transform sharing is implemented
          where generic prepared metadata exists, and cast remains a native provider route rather
          than a hidden per-query sidecar. Focused validation:
          `cargo test -q -p shardloom-cli --features release-user-surfaces --test public_workflow_route public_route_admits_provider_backed_native_vortex_cast_collect_shapes -- --nocapture`
          and `cargo fmt --all -- --check`.
    - [x] Keep PulseWeave/ProofBound evidence at source, unit, partial-state, merge, and output
      boundaries; do not add per-value evidence or route-control work inside hot loops.
      - [x] Audited the retained runtime shape and kept evidence at source/prewrite/prepared-state,
        state-budget/capillary-work-unit, merge/finalization, and explicit output boundaries. The
        existing capillary preparation gates prove PulseWeave decisions are certificate-gated and
        fail closed without native I/O proof; no per-value evidence or route-control branches were
        added to hot loops. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-write --lib capillary_preparation_applies_pulseweave_when_certified -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-write --lib capillary_preparation_blocks_pulseweave_without_native_io_certificate -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-write --lib local_flat_scalar_rows_apply_capillary_prewrite_control_before_write -- --nocapture`.
    - [x] Add an offline ship/drop cadence for retained optimizations: focused correctness tests
      must pass when the runtime changes, targeted 100M UAT must run after each coherent
      optimization batch and before a release train, and any technique that is slower, marginal, or
      not reusable across public SQL/Python/DataFrame routes must be dropped or reworked before
      shipment.
    - [ ] Add regression tests and targeted UAT before/after evidence proving each nested technique
      removes a dominant cost class; revert or do not keep changes that are marginal or slower than
      the prior state.
    - [ ] Update docs/README/architecture only with proven reusable runtime patterns and move
      completed details to the ledger after merge/session completion.
  - Next outcome: optimization decisions are driven by existing measured evidence and reusable
    prepared/native runtime composition, not by one-off ClickBench rewrites, generic loop tweaking,
    or extra hot-path observability.
  - User-visible surface: native/prepared Vortex SQL/operator routes, Python/DataFrame-style
    front doors after preparation, route evidence, UAT summaries, docs, and public capability
    reports.
  - Implementation scope: Universal Ingest prepared-state policy, aggregate/string/top-K/distinct/
    expression runtime helpers, focused tests, and offline UAT summarizers. Runtime evidence schema
    changes are allowed only when they reuse already-computed values and have no measurable cost.
  - Evidence required: focused correctness tests, targeted before/after UAT for affected rows,
    offline summaries naming the dominant cost class from existing fields, and no-fallback/no-
    external-engine fields.
  - Acceptance: every retained nested technique has a measured dominant-cost reduction or a
    correctness/diagnostic purpose; no optimization introduces hidden external execution,
    approximate answers for exact SQL, benchmark-specific answer caches, or slower retained paths.
  - Verification: focused Rust tests for each changed runtime family, targeted 100M UAT for the
    affected rows, and full 43-query local UAT after the dependent optimization rows are complete.
  - Non-goals: benchmark-only shims, extra public route vocabulary without shared runtime benefit,
    hot-path cost classification, per-row adaptive bookkeeping, official ClickBench submission, or
    broad performance claims without refreshed evidence.
  - Claim boundary: internal/local UAT runtime optimization evidence only; no superiority claim
    until benchmark methodology, correctness, hardware, and publication gates are approved.
  - Fallback boundary: all nested techniques remain ShardLoom-native/Vortex-native and must report
    `fallback_attempted=false` and `external_engine_invoked=false`.
  - Ledger rule: move completed detail after merge/session completion.

- [ ] `UNIVERSAL-INGEST-CAPILLARY-PIPELINE-1` Make Universal Ingest a source-aware,
  parallel, single-artifact Vortex preparation pipeline.
  - Source: current 100M ClickBench UAT replacement run over official `hits.parquet`. The current
    Parquet-to-Vortex replacement is correct enough to produce one `.vortex` artifact and uses an
    atomic temporary output path. The current budget reserves `max_parallelism=2` for
    source-to-Vortex normalization plus the writer/layout boundary; additional source-reader lanes
    are admitted only when more budget is explicitly available. Remaining ingest work is throughput,
    source-specific work avoidance, and a replacement UAT that completes inside the local safety
    policy.
  - Current state: stale sidecar-era `.prepared-olap-state.d` files were cleaned from the Desktop
    UAT workspace before the replacement run. The new prepare writes through a temporary
    `.vortex` path and should atomically replace the destination. The remaining issue is throughput
    and source-specific work avoidance, not public route selection.
  - Intake review: accepted as required runtime work because every local CSV/JSONL/Parquet/Arrow/
    Avro/ORC/Python/SQL/DataFrame front door depends on Universal Ingest before native Vortex
    execution. Do not add parallel source-specific public routes; improve the shared
    `UniversalIngress -> SourceState -> VortexPreparedState -> native_vortex_unified_plan` spine.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: use capillary source units, bounded queues, PulseWeave memory/decode
    scarcity signals, dynamic unit sizing, Vortex-first writer/layout strategy, metadata-first
    source inspection, and ProofBound evidence at source/prepare/write boundaries only.
  - Execution checklist:
    - [x] Add source-specific columnar handoff for Parquet/Arrow IPC so Vortex preparation preserves
      column chunks, dictionaries, and typed arrays without row/`StatValue` materialization.
      - Evidence: non-empty columnar and streaming sources use
        `ArrayRef::from_arrow(RecordBatch)` / `ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator`,
        report `vortex_array_kernel`, avoid scalar row decode, and preserve dictionary/typed values
        through focused Vortex reopen tests.
    - [x] Add typed column builders for CSV/JSONL that are projection/schema aware and avoid
      string/row materialization for unused columns.
      - [x] Promote admitted text compatibility preparation onto the same streaming Vortex writer
        path as columnar inputs: CSV/JSONL rows are converted into lazy, schema-stable Arrow
        `RecordBatch` units, wrapped with bounded capillary prefetch, and reported as
        `typed_text_rows_to_streaming_arrow_record_batch_source_state` with
        `text_adapter_to_typed_record_batch_stream` normalization. This closed the public
        smoke-route/cap gap for the conservative bridge while the later schema-declared and
        inferred product paths replaced the normal CSV/JSONL parser with direct file-backed
        parser-to-builder streams.
      - [x] Add a schema-declared CSV/JSONL fast path for normal Python/public examples:
        declared scalar schemas that match a CSV header or define a JSONL field set stream directly
        from a file-backed reader into lazy Arrow `RecordBatch` builders and the shared Vortex
        writer after a streaming source-fingerprint pass, reporting
        `schema_declared_text_to_streaming_arrow_record_batch_source_state`,
        `schema_declared_text_to_record_batch_stream`, bounded source buffering evidence, and
        `compatibility_parse_millis=0`. Incompatible schema shapes keep the conservative parser path.
      - [x] Promote no-schema CSV/JSONL product preparation out of the whole-source scalar-row
        bridge: infer a stable flat scalar schema in a streaming source pass, then reopen the same
        source into lazy Arrow `RecordBatch` builders and the shared Vortex writer, reporting
        `inferred_text_to_streaming_arrow_record_batch_source_state`,
        `inferred_text_to_record_batch_stream`, `inferred_*_record_batch_stream_batch_size_65536_rows`,
        `compatibility_parse_millis=0`, and no-fallback/no-external-engine evidence. Smoke
        profiles still enforce their diagnostic row caps; product/local public profiles do not.
    - [x] Implement a real capillary ingest pipeline: read row groups/source splits, build typed
      Vortex arrays, encode/layout, and write segments concurrently through bounded queues while
      respecting `max_parallelism` and memory pressure.
      - [x] Add product columnar stream shaping and source-unit evidence without adding route
        proliferation: Parquet uses product stream batches plus row-group unit hints, large
        columnar sources use an adaptive 262,144-row capillary batch policy to reduce writer
        handoff/segment churn, Arrow IPC preserves source-defined batch units, Avro/ORC use the
        same product stream planning family, and `SourceState` split refs now use source-native
        unit hints instead of a fake single split when the adapter exposes them. Public workflow
        evidence carries
        `source_state_stream_batch_size`, `source_state_stream_unit_count_hint`,
        `source_state_stream_unit_hint_kind`, `source_state_stream_policy`, and
        `source_state_dictionary_preservation_status`.
      - [x] Promote Parquet product preparation from generic single-reader prefetch to
        source-native row-group capillary work: `stream_flat_parquet_columnar_source_with_parallelism`
        uses upstream Parquet row-group selection, coalesces row groups into bounded ordered tasks,
        preserves source row order, records `bounded_capillary_row_group_parallel_writer_budgeted`,
        and lets the public CLI preserve that executor evidence instead of double-wrapping it as a
        serial source. Row-group workers now reserve lanes for Vortex normalization and writer/layout
        first: `max_parallelism=2` uses source-to-Vortex normalization plus writer flush, while
        higher values admit additional coalesced row-group source tasks.
      - [x] Expose the actual adaptive Parquet capillary task units, not only raw row groups, in
        `SourceState`: parallel product preparation now reports
        `source_stream_unit_hint_kind=parquet_adaptive_row_group_task_count`, coalesced task counts,
        and task row ranges that match the units feeding the Vortex writer. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_row_group_task_row_ranges_report_adaptive_source_units -- --nocapture`
        and
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_row_group_parallel_stream_preserves_order_and_records_executor -- --nocapture`.
      - [x] Route text source preparation through a shared lazy RecordBatch stream plus capillary
        prefetch boundary, so the Vortex writer consumes typed batches instead of the scalar Vortex
        writer path for public builds with `vortex-write,universal-format-io`.
      - [x] Add a source-native embedded metadata wrapper for public columnar preparation without
        reviving broad per-row synthesis: dictionary-backed URL-like Arrow columns derive compact
        byte-length and domain columns by transforming dictionary values once and remapping existing
        codes, preserving `Int8`/`Int16`/`Int32`/`Int64` and unsigned Arrow dictionary key widths
        instead of silently narrowing the source layout to `Int32`; typed numeric/time fields and
        Arrow timestamp columns across second/millisecond/microsecond/nanosecond units may derive
        compact minute keys, and plain UTF-8 columnar batches remain on the no-synthesis path with
        `source_native_embedded_derived_columns=not_available_for_current_arrow_layout`. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_typed_time_stream_embeds_compact_minute_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_timestamp_units_embed_compact_minute_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_product_stream_reports_no_physical_derived_column_synthesis -- --nocapture`,
        and
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom public_workflow_preparation_fields_keep_product_stream_source_evidence -- --nocapture`.
      - [x] Collapse URL dictionary hidden length/domain derivation into a single dictionary-value
        pass when both metadata columns are requested: Universal Ingest now computes byte length and
        URL domain from the same prepared dictionary values, preserves the source dictionary key
        width for both hidden columns, and avoids rescanning dictionary strings during source-native
        Vortex normalization. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_dictionary_backed_embedded_length_metadata -- --nocapture`,
        `cargo fmt --all -- --check`, and
        `cargo clippy -q -p shardloom-vortex --features universal-format-io --lib -- -D warnings`.
      - [x] Collapse typed time hidden minute/bucket derivation into a single source-array pass:
        when Universal Ingest attaches both `__shardloom_derived_extract_minute_*` and
        `__shardloom_derived_date_trunc_minute_*` for the same Arrow time-like column, the derived
        batch wrapper now performs one typed downcast and one row scan, carrying nulls through both
        hidden columns and preserving the single `.vortex` artifact contract. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_typed_time_stream_embeds_compact_minute_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_timestamp_units_embed_compact_minute_metadata -- --nocapture`,
        `cargo fmt --all -- --check`, and
        `cargo clippy -q -p shardloom-vortex --features universal-format-io --lib -- -D warnings`.
      - [x] Avoid rebuilding dictionary row-key vectors for non-null source dictionaries: hidden
        UTF-8 length metadata now reuses the source dictionary keys whenever dictionary values are
        all non-null, while preserving the null-value rewrite path for dictionaries that contain
        null values. This keeps source-native URL/search metadata construction at dictionary-value
        scale instead of row scale for the common case. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        `cargo fmt --all -- --check`, and
        `cargo clippy -q -p shardloom-vortex --features universal-format-io --lib -- -D warnings`.
      - [x] Compact dictionary-backed UTF-8 byte-length metadata in the single `.vortex` artifact:
        source-native dictionary URL/search/title fields now keep the source dictionary key vector
        and store hidden length metadata as dictionary values (`Dictionary<source_key, UInt32>`)
        instead of expanding to a full per-row `UInt32` array when dictionary values are non-null.
        Null-valued dictionary entries preserve the dictionary-shaped schema by nulling affected
        output keys rather than switching the stream to a primitive row fallback. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_dictionary_backed_embedded_length_metadata -- --nocapture`.
        - [x] Prove dictionary-backed hidden length metadata remains executable through Vortex
          predicate pushdown, not just persisted: `URL <> ''` rewrites to
          `__shardloom_derived_utf8_len_URL`, executes against `Dictionary<UInt8, UInt32>`
          metadata, and keeps no-decode/no-materialization evidence. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_rewrites_non_empty_string_predicate_to_dictionary_backed_length_column -- --nocapture`.
      - [x] Bound hidden URL-domain derivation allocation to expected domain cardinality rather
        than source URL dictionary cardinality: Universal Ingest now uses ShardLoom's hot-state
        hash profile for source-native domain-code assignment and caps initial domain string/code
        reserves, preserving exact dictionary remapping while avoiding large upfront allocations
        for URL-heavy prepared Vortex artifacts. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib embedded_url_domain_capacity_is_bounded_by_expected_domain_cardinality -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features universal-format-io --lib -- -D warnings`.
      - [x] Reuse the same bounded ShardLoom domain-code builder for non-dictionary URL-like
        source batches: plain UTF-8 row streams now build compact `Dictionary<Int32, Utf8>` domain
        metadata through a source-borrowed `FxHashMap` and append row keys directly, while the
        fused length/domain path computes length and domain in one pass. This keeps URL-domain
        metadata inside the single `.vortex` artifact and avoids Arrow's generic row dictionary
        builder on the hot Universal Ingest path. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib text_rows_stream_source_embeds_exact_hidden_string_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`.
    - [x] Reuse one writer/runtime context per prepare job, coalesce small source units when
      scheduling overhead dominates, and split large units when decode/write pressure requires it.
      - [x] Preserve one `LocalVortexWriteContext` per prepared-artifact write and report
        `writer_context_open_micros` plus `writer_context_reuse_status`; the remaining open portion
        is adaptive split/coalesce beyond the proven fixed Parquet row-group coalescing policy.
      - [x] Reject both large-source uncompressed fast-load and all-column balanced BtrBlocks
        compression as default product writer profiles after UAT showed the first could balloon a
        100M Parquet-derived `.vortex` artifact to ~50G and the second made only ~144M progress in
        three minutes. The retained product writer now aligns the large-source writer row block
        with the 262,144-row product source batch so the writer does not split every large source
        batch into four smaller row blocks per column; it keeps the fast flat/zoned/stat path for
        most typed columns, and applies
        Vortex dictionary-Zstd compression to source-schema text-heavy/source-derived fields rather
        than only a hand-selected query subset. It reports
        `writer_layout_strategy_applied=vortex_write_strategy_row_block_262144_source_text_dictionary_zstd_embedded_olap_layout_statistics`.
        Hidden numeric length metadata is no longer routed through the text-specific writer
        override; `__shardloom_derived_utf8_len_*` fields stay on the normal typed/stat layout
        path while URL-domain hidden text fields remain eligible for the text dictionary-Zstd
        profile. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-write,universal-format-io --lib local_flat_scalar_rows_use_source_text_large_source_layout_row_blocks_when_advised -- --nocapture`
        and
        `cargo test -q -p shardloom-vortex --features vortex-write,universal-format-io --lib source_text_large_source_writer_profile_covers_clickbench_text_fields -- --nocapture`.
        The first fast-load Desktop 100M replacement ingest attempt was not retained because the
        local CPU guard killed the process at exactly 100.0% after 3.041s:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/prepare_fast_load_uncompressed_20260622T040020Z/summary.json`.
        A later normalization-prefetch run proved the pipeline was much faster but confirmed the
        uncompressed writer-size regression:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T102409Z/prepare_summary.json`.
        The all-column balanced compression follow-up was rejected because it re-entered the slow
        compression-selection path:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T104108Z/prepare_summary.json`.
        The first narrow text-compression follow-up improved progress to ~2.54GB temp output
        within the three-minute UAT cap but still did not complete, so it was cleaned up and not
        retained:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T105615Z/prepare_summary.json`.
        Later broad source-text dictionary-Zstd UAT completed with a single final `.vortex` artifact,
        no sidecars, and explicit no-fallback evidence; do not retain a load-speed claim until writer
        timing improves instead of only artifact shape.
      - [x] Rejected a bounded adaptive Parquet row-group task-shape patch after local 100M
        replacement-ingest UAT crossed the three-minute safety cap without completing; the
        previously proven fixed row-group coalescing path is retained until a new approach reduces
        data work instead of increasing source-reader churn.
      - [x] Rejected physical Arrow batch coalescing plus a 262,144-row/8 MiB writer target after
        replacement-ingest UAT showed worse progress than the retained 65,536-row/1 MiB profile:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T092003Z/prepare_summary.json`
        stopped after 220.938s before artifact swap, left the previous 14G `.vortex` target intact,
        and removed temporary files. The retained path keeps compact embedded derived columns and
        removes only the regressing writer/coalescer change.
      - [x] Rejected default physical derived-column synthesis for large Parquet product streams
        after replacement-ingest UAT again made poor progress before artifact swap:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T093523Z/prepare_summary.json`
        stopped after 237.907s with the previous 14G `.vortex` target intact and no remaining temp
        files. The retained path keeps the compact derived-column implementation for admitted typed
        sources, but product columnar adapters now report
        `embedded_derived_columns=not_synthesized_source_native_columnar_adapter` until a
        source-native/dictionary-aware generator proves faster than baseline.
      - [x] Tested the retained custom large streaming Vortex writer strategy after the
        65,536-row/1 MiB path still showed poor 100M Parquet replacement-ingest progress:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T094514Z/prepare_summary.json`
        stopped after 169.848s and left the previous 14G target intact. This remains the better
        measured writer path versus upstream default, but its pace is not yet acceptable, so the
        next optimization target is source/writer overlap and dynamic unit scheduling.
      - [x] Rejected upstream-default writer substitution after UAT showed it was even slower for
        the 100M Parquet streaming shape:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T095654Z/prepare_summary.json`
        stopped after 119.009s with only ~46M temp output, versus ~333M after ~170s for the retained
        custom writer path. The retained product route keeps the custom large-source writer strategy
        and moves optimization to dynamic Capillary/PulseWeave source/writer overlap.
      - [x] Convert Parquet row-group capillary tasks from whole-task buffering to ordered
        per-`RecordBatch` emission. Follow-up 100M UAT showed this source-only overlap was still
        too slow:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T100544Z/prepare_summary.json`
        stopped after 201.353s with only ~412M temp output and left the previous 14G target intact.
        The retained policy now reserves `max_parallelism=2` for source-to-Vortex normalization plus
        writer flush, and assigns extra Parquet row-group source lanes only above that budget.
      - [x] Move streaming Arrow-to-Vortex normalization into a bounded Capillary worker before the
        writer when parallelism is available. The writer still consumes the upstream Vortex
        `ArrayIterator` contract and writes a single `.vortex` file, but it no longer owns all
        source read, projection validation, Arrow-to-Vortex conversion, and write/layout work on one
        pull path. Evidence uses
        `array_build_strategy=capillary_vortex_array_prefetch_window_from_arrow_record_batch_stream`
        plus `vortex_array_build_prefetch_window` on the public prepare evidence surface.
      - [x] Make the columnar capillary prefetch wrapper idempotent so a source that already has a
        bounded prefetch or Parquet row-group capillary executor is not wrapped a second time by a
        higher-level public facade. This preserves the original executor evidence and avoids
        duplicate queue/thread overhead across CLI/Python/DataFrame front doors. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib capillary_prefetch_does_not_double_wrap_existing_capillary_source -- --nocapture`.
      - [x] Align product CSV/JSONL typed-text stream units with the large-source writer profile:
        schema-declared, schema-hinted, inferred, and conservative typed-text bridges now use
        262,144-row `RecordBatch` units when the public product profile disables the smoke row cap,
        while smoke profiles retain the diagnostic 65,536-row stream policy. This reduces
        source-to-writer handoff and writer-unit churn for production text ingestion without
        adding a source-specific public route, changing SQL/Python/DataFrame semantics, or
        reintroducing whole-source scalar materialization. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib text_rows_stream_source_admits_large_product_batch_units -- --nocapture`,
        `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom schema_declared_text_stream_preserves_smoke_cap_and_product_no_cap -- --nocapture`,
        `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom text_sources_stream_without_scalar_row_bridge -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features universal-format-io --lib -- -D warnings`,
        `cargo clippy -q -p shardloom-cli --features release-user-surfaces --bin shardloom -- -D warnings`,
        and `cargo fmt --all -- --check`.
      - [x] Replace fixed Parquet row-group task chunking with metadata-aware capillary task
        sizing: when row-group row counts are known, source tasks coalesce tiny row groups up to a
        bounded row budget and split larger groups at the same budget while preserving source order
        and the single `.vortex` writer path. Missing metadata keeps the old fixed fallback. This
        closes another adaptive coalesce/split slice without introducing source-specific public
        routes or extra hot-path evidence overhead. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_row_group_task_builder_ -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_row_group_parallel_stream_ -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib parquet_row_group_stream_uses_default_second_lane_for_bounded_source_overlap -- --nocapture`.
    - [x] Compute artifact digest and write evidence during the streaming write path so prepare
      does not need a full-file reread/reopen just to prove identity.
      - Evidence: `LocalVortexWriteContext::write_array` and `write_array_iterator` return
        `artifact_digest_source=workspace_safe_streaming_sha256_digest`,
        `digest_micros=0`, and the workspace-safe output digest as the prepared artifact digest.
    - [x] Make atomic replacement lifecycle explicit and tested: write `<target>.shardloom-tmp-*`,
      validate the single `.vortex` artifact, atomically rename, and remove stale `.shardloom`
      prepared-state/sidecar-era files for that target.
      - Evidence: workspace-safe writer tests assert same-directory atomic rename, streaming
        digest, and staging cleanup; `local_vortex_write_removes_legacy_prepared_olap_sidecars_for_target`
        proves successful Vortex artifact writes remove target-derived legacy
        `.prepared-olap-state.manifest` and `.prepared-olap-state.d` artifacts.
    - [x] Apply Vortex 0.75 writer strategy/field hooks where safe: row-block sizing,
      dictionary/zoned statistics/compression, and field-aware encoding policy based on source
      schema/statistics rather than benchmark query text.
      - Evidence: `local_flat_scalar_rows_apply_layout_write_advisor_before_write` verifies the
        admitted Vortex writer strategy, row-block sizing, layout inventory fields, and fail-closed
        behavior for blocked layout strategies.
      - Evidence:
        `local_flat_scalar_rows_use_source_text_large_source_layout_row_blocks_when_advised`
        verifies the non-streaming large-source 65,536-row block policy plus source-text
        dictionary-Zstd writer policy evidence.
      - Evidence:
        `local_flat_scalar_rows_use_source_text_large_source_layout_row_blocks_when_advised`,
        `local_flat_scalar_rows_use_fast_load_large_source_layout_when_not_text_domain`, and
        `vortex_ingest_parquet_public_prepare_uses_row_group_capillary_executor` verify the
        source/profile-aware writer branch: product ingest no longer reports a stale fixture
        workload, large URL/text-domain OLAP sources retain the source-text dictionary-Zstd writer
        override, and large non-text sources use the fast-load layout profile without creating a
        separate public route.
      - Evidence: `parquet_row_group_parallel_stream_preserves_order_and_records_executor` and
        `parquet_row_group_stream_reserves_vortex_normalization_and_writer_at_parallelism_two`
        verify ordered Parquet capillary source units, Vortex-normalization/writer reservation at
        `max_parallelism=2`, and source-order preservation after the per-`RecordBatch` emission
        change.
    - [x] Persist only generic layout/statistics/domain evidence in the single `.vortex` artifact;
      do not create query-answer sidecars, materialized views, or query-summary payloads.
      - Evidence: prepared OLAP evidence reports
        `single_vortex_artifact_embedded_vortex_layout_statistics_v1`,
        `exact_sidecar_family_count=0`, and
        `query_answer_sidecar_status=disabled_rejected_for_public_default_runtime`; the runtime
        layout fields come from the prepared `.vortex` artifact and public surfaces, not adjacent
        query-answer files.
    - [x] Add focused tests for Parquet replacement, stale sidecar cleanup, no hidden fallback,
      no external engine invocation, digest/evidence stability, and `max_parallelism` propagation.
      - [x] Add focused tests for product stream batch policy, capillary prefetch evidence
        preservation, source-unit row-range splitting, and public preparation field filtering:
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io --lib
        product_columnar_stream_batch_size_uses_product_policy_not_smoke_cap -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io --lib
        stream_source -- --nocapture`, `cargo test -p shardloom-cli --features
        vortex-write,universal-format-io --bin shardloom
        source_unit_split_row_ranges_use_known_source_units -- --nocapture`, and
        `cargo test -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        public_workflow_preparation_fields_keep_product_stream_source_evidence -- --nocapture`.
      - [x] Add focused Parquet row-group executor coverage:
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io --lib
        parquet_row_group_parallel_stream_preserves_order_and_records_executor -- --nocapture`
        proves generated multi-row-group Parquet input reads back in source order while reporting
        coalesced row-group parallel executor evidence.
      - [x] Add focused legacy cleanup coverage:
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io --lib
        local_vortex_write_removes_legacy_prepared_olap_sidecars_for_target -- --nocapture`.
      - [x] Add focused public single-artifact/digest/evidence stability coverage:
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_public_prepare_writes_only_single_vortex_artifact -- --nocapture`.
      - [x] Add focused text-stream preparation coverage:
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io --lib
        text_rows_stream_source_builds_lazy_schema_stable_batches -- --nocapture` and
        `cargo test -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_text_sources_use_typed_record_batch_stream -- --nocapture`.
      - [x] Add focused schema-declared direct-stream coverage:
        `cargo test -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_schema_declared_text_sources_stream_without_scalar_row_bridge --
        --nocapture`.
      - [x] Add focused no-schema inferred text-stream coverage:
        `cargo test -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_inferred_text_sources_stream_without_scalar_row_bridge -- --nocapture`.
      - [x] Add focused smoke/product row-budget coverage for both schema-declared and inferred
        text streams:
        `cargo test -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        schema_declared_text_stream_preserves_smoke_cap_and_product_no_cap -- --nocapture`.
      - [x] Add focused public max-parallelism propagation coverage proving an explicit
        `max_parallelism=4` reaches Vortex ingest request evidence, source-state capillary
        prefetch evidence, `vortex_array_build_prefetch_window=3`, Vortex capillary preparation
        evidence, and no-fallback/no-external-engine fields:
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_max_parallelism_propagates_to_public_prepare_evidence -- --nocapture`.
      - [x] Add focused public Parquet preparation coverage proving the actual `prepare dataframe`
        / public Vortex preparation path uses the source-native Parquet row-group capillary
        executor at `max_parallelism=2`, not only the lower adapter helper. This keeps current code
        from regressing to the older UAT evidence shape where retained artifacts reported a serial
        source reader. Focused validation:
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom
        vortex_ingest_parquet_public_prepare_uses_row_group_capillary_executor -- --nocapture`.
      - [x] Centralize public-runtime resource defaults so the `max_parallelism=2` local public
        runtime policy is declared in `python/src/shardloom/runtime_defaults.py`, re-exported by
        the Python package/client, mirrored as a named Rust public-prepare parser constant, and
        consumed by the ClickBench route-readiness validator instead of duplicated as route-local
        literals. Internal smoke helpers keep a separate named one-lane diagnostic default.
        Focused validation:
        `PYTHONPATH=python/src python - <<'PY'
        import shardloom as sl
        from shardloom.client import DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM as client_default
        from shardloom.runtime_defaults import DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM as source_default
        assert sl.DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM == source_default == client_default == 2
        PY`,
        `PYTHONPATH=python/src python -m unittest
        python.tests.test_query_builder.LazyWorkflowBuilderTests.test_read_json_jsonl_collect_passes_jsonl_to_public_facade
        python.tests.test_query_builder.LazyWorkflowBuilderTests.test_workflow_prepare_uses_public_facade_with_attached_route
        python.tests.test_query_builder.LazyWorkflowBuilderTests.test_vortex_query_builder_filter_limit_uses_local_primitive_runtime
        python.tests.test_query_builder.LazyWorkflowBuilderTests.test_sql_vortex_project_and_star_limits_use_local_primitive_runtime
        python.tests.test_query_builder.LazyWorkflowBuilderTests.test_python_and_session_vortex_star_and_filter_project_limits_use_local_runtime`
        and
        `PYTHONPATH=python/src python -m unittest
        python.tests.test_cli_client.ShardLoomClientTests.test_runtime_activation_summary_labels_blocked_local_file_middle`.
    - [x] Run a targeted UAT replacement ingest over the official 100M Parquet source and record
      elapsed time, output size, CPU utilization, sidecar absence, and route evidence before
      retaining the approach.
      - [x] Resolve the local CPU-safety interpretation before replacement UAT:
        macOS `ps %CPU` is core-relative, so `178%` means about 1.78 cores rather than 178% of
        total machine CPU. Two-lane ingest is therefore consistent with the restored
        `max_parallelism=2` public default; UAT transcripts should record core-equivalent CPU and
        guard against total-machine saturation instead of treating any value above `100%` as unsafe.
        Earlier guarded current-branch attempts preserved the existing `.vortex` artifact and
        cleaned temporary files, but were killed before JSON route evidence because the guard used
        the incorrect total-CPU interpretation:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/current_branch_bounded_writer_20260622T024100Z`
        peaked at `184.4%`,
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/current_branch_writer_budget_serial_source_20260622T024537Z`
        peaked at `122.0%`, and
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/current_branch_writer_zoned_budget_20260622T024816Z`
        peaked at `121.1%`.
      - [x] Latest guarded replacement attempt after the source-lane budget correction hit the same
        strict per-core guard before route JSON was emitted:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/current_branch_replacement_uat_20260622T051151Z/prepare_guard_summary.json`
        stopped after `2.096s` at `100.0%` CPU. The existing 14G `.vortex` artifact was preserved
        byte-for-byte, no temp files remained, and no prepared-OLAP sidecars were observed.
      - [x] Re-run replacement UAT with the corrected two-lane safety interpretation, replacing the
        Desktop `.vortex` artifact in place and verifying no hidden temp files or prepared sidecars
        remain.
        - [x] Reject the first corrected-safety replacement attempt before artifact swap:
          `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T034259Z/prepare_summary.json`
          ran for `555.036s`, peaked at `184.8%` process CPU on a one-core basis
          (`18.48%` of 10 logical CPUs), preserved the previous 14G target, and was stopped when
          the hidden atomic temp artifact grew to roughly 32G. The oversized temp was removed and
          no sidecars remained.
        - [x] Fix the identified inflation flaw before retrying: embedded UTF-8 length columns now
          write as compact `UInt32` only for admitted high-value URL/search/title text fields,
          URL/Referer/URI domain columns write as Arrow dictionary arrays (`Int32` keys over UTF-8
          values), and a streaming writer/reopen regression proves the compact schema persists in
          the single `.vortex` artifact. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_compact_embedded_derived_columns -- --nocapture`
          and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib`.
        - [x] Retain the first successful broad source-text single-artifact replacement UAT as the
          current correct-product baseline:
          `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T115230Z/prepare_summary.json`
          completed in `870.375s`, wrote a single `28,221,878,552` byte `.vortex`, reported
          `fallback_attempted=false`, `external_engine_invoked=false`,
          `external_manifest_written=false`, no query-answer sidecars, no leftover hidden temp
          files, `source_state_record_batch_count=1526`, and
          `layout_footer_segment_count=65527`.
        - [x] Retain the adaptive large-source capillary stream-batch variant as an artifact-shape
          improvement with an explicit load-time tradeoff:
          `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/single_artifact_ingest_replace_full_20260622T122003Z/prepare_summary.json`
          completed in `990.389s`, wrote a single `28,005,713,616` byte `.vortex`, reported
          `source_state_stream_batch_size=262144`,
          `source_state_stream_policy=product_columnar_stream_batch_size_262144_rows`,
          `source_state_record_batch_count=382`, `layout_footer_segment_count=33495`,
          `layout_footer_approx_bytes=1520112`, `fallback_attempted=false`,
          `external_engine_invoked=false`, `external_manifest_written=false`, and no leftover temp
          or sidecar files. This reduces batch/segment churn and likely helps read-side metadata
          overhead, but it is not a load-speed closeout because `prepare_once_millis` increased
          from `834654` to `950957`; keep writer profile tuning open as a knob/dial item.
  - Next outcome: official-source local prepare is fast enough to be a credible load step and the
    generated `.vortex` artifact is the only runtime data artifact.
  - User-visible surface: `prepare dataframe`, Python `ctx.read_*` normalization, local package use,
    ClickBench load/UAT workflow, capability evidence, docs.
  - Implementation scope: `shardloom-vortex/src/vortex_ingest.rs`,
    `shardloom-cli/src/public_workflow_route.rs`, Universal Ingest adapters, Python wrappers only
    for evidence transport, focused tests, and docs.
  - Evidence required: focused Rust/Python tests, UAT replacement transcript, no stale sidecar
    files, no-fallback/no-external-engine fields, and route capability validators.
  - Claim boundary: local UAT/load-step evidence only until benchmark methodology approval.
  - Fallback boundary: no DataFusion/Spark/DuckDB/Polars/pandas execution and no query-result
    sidecar/cache.
  - Ledger rule: move completed detail after merge/session completion.

- [ ] `CLICKBENCH-100M-ARCHITECTURAL-OPTIMIZATION-3` Apply shared high-cardinality,
  string-metadata, bounded top-K, exact-distinct, layout-advisor, and expression-fusion
  optimizations before the next full 100M UAT.
  - Source: local 100M ClickBench UAT slow-family review. Slow side over ten seconds was
    concentrated in `CB-Q19`, `CB-Q33`, `CB-Q34`, `CB-Q35`, `CB-Q23`, `CB-Q17`, `CB-Q24`,
    `CB-Q18`, `CB-Q29`, `CB-Q22`, `CB-Q16`, `CB-Q28`, `CB-Q21`, and `CB-Q14`; follow-up
    architecture review identified high-cardinality aggregate state, string predicate/string-derived
    grouping, bounded top-N materialization, exact distinct, prepared Vortex layout shape, and
    expression fusion/reuse as the significant levers.
  - Current state: the native Vortex route is functionally broad and the current full local 100M
    UAT pass records zero failed or unsupported ClickBench rows after the materialized string-partial
    admission fix. Evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_current_20260621T222424Z/targeted-summary.json`.
    The replacement load uses one prepared `.vortex` artifact generated from official
    `hits.parquet`; the prepare transcript reports 99,997,497 rows, 70,503 segments, 14G output,
    `exact_sidecar_family_count=0`, and no adjacent query-summary/OLAP sidecar. Correctness/routing
    is now ahead of performance: all 43 rows execute through native Vortex routes with
    `fallback_attempted=false` and `external_engine_invoked=false`, but the slow side is still
    dominated by full high-cardinality state rebuilds and string scan/materialization:
    `CB-Q33` 173.236s, `CB-Q35` 157.799s, `CB-Q34` 154.694s, `CB-Q23` 50.124s, `CB-Q17` 50.072s,
    `CB-Q24` 38.666s, `CB-Q29` 23.456s, `CB-Q19` 19.103s, `CB-Q22` 16.380s, `CB-Q21` 13.370s,
    `CB-Q28` 12.982s, `CB-Q14` 12.887s, and `CB-Q18` 10.579s. The next pass must reduce dominant
    data work through reusable single-artifact layout/statistics, encoded dictionaries, segment
    pruning, capillary partial state, and row-ref late materialization rather than one-query
    rewrites, query-answer sidecars, or hot-loop instrumentation.
  - Intake review: accepted all listed optimization targets as feasible runtime work because they
    improve shared OLAP operator families and public SQL/Python/DataFrame wrappers once route
    unification is verified. Rows are grouped by architectural lever rather than query number so
    improvements apply across ClickBench, Python DataFrame-style, SQL, CLI, and future benchmark
    surfaces.
  - V1 scope classification: `required_for_v1` for local native Vortex OLAP performance posture;
    official ClickBench publication remains gated by separate methodology and claim approval.
  - ShardLoom technique review: apply hierarchical capillary aggregation, PulseWeave
    state/decode/sink scarcity evidence, dynamic work shaping by cardinality/selectivity/dtype,
    metadata-first string/layout pruning, dictionary/encoded execution, late materialized row-ref
    top-K, route timing-surface separation, and ProofBound evidence before reporting an optimized
    route.
  - Current implementation order from the corrected full-43 UAT:
    1. high-cardinality numeric/composite group-state partitioning and merge for `CB-Q33`;
    2. embedded URL/string domain metadata plus dictionary/code grouping for `CB-Q34`/`CB-Q35`;
    3. string predicate plus aggregate-state pruning for `CB-Q23`, `CB-Q22`, `CB-Q21`, `CB-Q28`,
       and `CB-Q29`;
    4. wide bounded top-K row-ref/payload locality for `CB-Q24`-`CB-Q27`;
    5. exact-distinct and repeated-expression cleanup for the remaining 1-10s rows.
  - Sub-second target rule: treat `sub_1s_query_time` as a prepared/indexed execution target, not
    a micro-optimization target. Rows that still require a full 100M-row string scan, full
    high-cardinality state build, or wide-row payload reread at query time must move work into
    reusable, generic Vortex layout/statistics/encoding policy during load/prepare, with
    source-hash invalidation and Native I/O evidence. Do not cache benchmark answers, write
    query-specific sidecars, add ClickBench-only routes, use approximate semantics for exact SQL,
    or keep loop tweaks that fail measured before/after UAT.
  - Latest targeted ship/drop evidence after the current full-43 pass:
    - `CB-Q33` numeric-pair state reservation improved from 173.236s to 143.428s in
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_hashstate_20260621T230205Z/summary.json`;
      retain the typed numeric state reservation while continuing partitioned/exact merge work.
    - A broader string hash-table change was not retained because mixed evidence included a
      `CB-Q35` regression to 161.730s. After rolling the string-path hash changes back, the guard
      run `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_guard_20260621T231356Z/summary.json`
      recorded `CB-Q35` at 143.407s with native Vortex route evidence and no fallback/external
      engine invocation.
    - Current conclusion: state reservation helps typed numeric/composite groups, but URL/string
      grouping still reports `chunk_materialized_partial_map`, 18,342,019 decoded strings, and
      `supported_with_materialization_boundary`; the next implementation must consume encoded
      string/domain metadata or improve the Vortex reader/accessor boundary.
    - Current-branch ship/drop update: the shared UTF-8 chunk-dictionary accessor plus generic
      count-star streaming top-K finalizer is retained because targeted local 100M UAT moved the
      URL grouping family from the 154-158s full-43 baseline to `CB-Q34` 92.982s and `CB-Q35`
      100.037s in
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_slow6_current_branch_20260622T033654Z/summary.json`.
      A narrower probe over the same route recorded `url_group_top10` 88.834s and
      `const_url_group_top10` 92.978s in
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_streaming_count_topk_20260622T033253Z/summary.json`.
      The route still decodes 18,342,019 strings, so this is a retained state/output improvement,
      not completion of the embedded string metadata work.
    - Rejected current-branch ship/drop update: a decoded primitive aggregate accessor probe for
      `CB-Q33` was removed after targeted local UAT regressed the row from 149.269s to 153.257s
      while still materializing most hot columns:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_decoded_primitive_20260622T034758Z/summary.json`.
      The next `CB-Q33` work must reduce near-unique state volume or merge cost, not add another
      decode boundary.
    - Rejected current-branch ship/drop update: maintaining the `CB-Q33` numeric-pair retained
      top-K candidate window during every group update was removed after targeted local 100M UAT
      exceeded the 180-second cap:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_update_time_topk_20260622T042051Z/summary.json`.
      The probe reached 180.994s with max CPU 98.9% versus the prior current-branch 149.269s, so
      the next `CB-Q33` work must reduce near-unique state volume, partition/merge cost, or layout
      work instead of adding per-update top-K maintenance.
	    - Rejected current-branch ship/drop update: the candidate `URLHash` grouping surrogate for
	      `CB-Q34`/`CB-Q35` was removed after targeted local 100M UAT did not activate a faster route
	      (`CB-Q34` 93.956s, `CB-Q35` 103.938s) and a bounded exactness check found 4 URLs with
	      multiple `URLHash` values in 1,000 sampled rows. Existing source columns cannot be assumed to
	      be exact URL group-key surrogates. Future URL/string grouping work must use exact embedded
	      domain/dictionary metadata or source-derived columns created by Universal Ingest with
	      source-hash invalidation, artifact-size accounting, and Native I/O evidence.
	    - Retained current-branch ship/drop update: proof-bound string count top-K heavy hitters are now
	      admitted for large exact count-only string top-K routes. The route builds a first-pass
	      weighted heavy-hitter candidate sketch from chunk UTF-8 dictionaries, proves whether exact
	      top-K is possible from the sketch lower bound, recounts only candidate strings on a second
	      scan, and falls back to the existing exact native Vortex dictionary route if proof is not
	      possible. Targeted local 100M UAT retained exact result parity with the prior route while
	      moving `CB-Q34` to 33.958s and `CB-Q35` to 34.064s from the prior 92.982s/100.037s
	      current-branch path:
	      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_heavy_hitter_guard_20260622T072212Z/summary.json`
	      and `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_heavy_hitter_20260622T071417Z/summary.json`.
	      This is a retained exact ShardLoom-native operator strategy, not completion of embedded
	      string/domain metadata or sub-second URL grouping.
    - Retained current-branch ship/drop update: string top-K exact recount now builds a compact
      candidate signature prefilter from retained heavy-hitter strings, then still verifies exact
      candidate membership before counting. This keeps exact SQL semantics, uses the same single
      `.vortex` artifact, and avoids adding query sidecars or result caches. Focused validation:
      `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib string_count_topk -- --nocapture`
      and `cargo test -q -p shardloom-cli --features release-user-surfaces
      local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`. Targeted 100M
      local UAT retained native Vortex/no-fallback evidence while moving the string heavy-hitter
      lane from the prior targeted `CB-Q34` `32.984s` / `CB-Q35` `33.896s` to `CB-Q34` `29.777s`
      and `CB-Q35` `26.522s`:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_signature_prefilter_q34_clean_20260622T_now/summary.json`
      and `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_string_signature_prefilter_20260622T_now/summary.json`.
    - Retained current-branch ship/drop update: proof-bound numeric+UTF8 count top-K heavy hitters
      are now admitted for two-key ordered count-only grouped routes such as `CB-Q17`
      (`UserID, SearchPhrase ORDER BY count DESC LIMIT K`). The route keeps first-pass state
      bounded to heavy-hitter candidates, reopens the same single `.vortex` artifact for an exact
      candidate recount, and falls back to the existing exact ShardLoom-native route if the
      ProofBound threshold is not satisfied. Focused validation:
      `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8 -- --nocapture`
      and `cargo test -q -p shardloom-cli --features release-user-surfaces
      local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`.
      Targeted local 100M UAT moved `CB-Q17` from the prior full-run `67.435s` to `20.648s`
      (`/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_numeric_utf8_topk_20260622T_now/summary.json`)
      and then `22.720s` with flattened proof fields
      (`/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_numeric_utf8_topk_fields_20260622T_now/summary.json`).
      The subsequent full sequential 43-query pass retained the route at `20.982s`, with
      `numeric_utf8_count_topk_heavy_hitter_late_recount`,
      `proofbound_heavy_hitter_numeric_utf8_count_topk_late_recount`,
      `numeric_utf8_topk_heavy_hitter_exact_proof=true`, and no fallback/external-engine
      invocation.
  - Compound-technique dependency: implement changes under
    `COMPOUND-SHARDLOOM-RUNTIME-TECHNIQUES-1` when a nested SourceState/VortexPreparedState/
    capillary/PulseWeave/encoded/late-materialized layer removes a dominant cost class; do not add
    nanoscale routing, cost classification, counters, or evidence inside hot loops when the same
    amount of data work remains.
  - Execution checklist:
    - [x] Apply the single-artifact prepared OLAP correction: remove public/default
      query-summary sidecar execution, stop `prepare sql`/`prepare dataframe` from executing
      profile queries to generate sidecars, stop primitive reruns from consuming query-summary
      payloads, keep prepared OLAP admission derived from the single `.vortex` artifact with no
      adjacent OLAP manifest or sidecar directory, and add focused Rust/Python tests proving no
      sidecar hit/write/consume fields are emitted on the public runtime paths.
    - [x] Implement single-artifact prepared OLAP layout/statistics in Universal Ingest: source
      digest, schema digest, row-count digest, embedded Vortex layout/footer statistics posture,
      Vortex writer/layout policy, invalidation policy, and ProofBound evidence. This must be a
      reusable Vortex layout feature, not a ClickBench answer cache.
      - [x] Add the fail-closed prepared OLAP single-artifact report contract with source digest, schema
        digest, row count, embedded layout/statistics status, query-time contract, deterministic
        blockers, no-fallback/no-external-engine fields, and read-through admission evaluation that
        derives state from source evidence plus prepared artifact size/digest.
      - [x] Correct the product policy so plain prepared Vortex artifacts with embedded layout
        posture are admitted, exact sidecar family count is zero, and query-summary sidecar
        declarations are cleared by the bundle writer.
      - [x] Remove public/default query-summary sidecar APIs and evidence propagation from native
        primitive execution, public workflow prepare-profile planning, direct `vortex-run`, and the
        Python SQL/DataFrame/session convergence tests.
      - [x] Add focused Rust/Python tests proving primitive reruns stay on native Vortex scan paths,
        prepared OLAP evidence admits a single `.vortex` artifact, same-size artifact mutations
        update derived state without stale external manifests, no `.prepared-olap-state.manifest` or
        `.prepared-olap-state.d` path is created, and public surfaces converge on single-artifact
        native Vortex evidence.
      - [x] Add Vortex-footer-derived OLAP layout inventory evidence from the single prepared
        `.vortex` artifact: row count, segment count, statistics status, encoding/layout status,
        footer byte estimate, dtype summary, inventory digest, and
        `layout_metadata_persisted_in_artifact=true`; thread those fields through Vortex writes,
        prepared OLAP read-through evaluation, public workflow route evidence, and Python surface
        tests without creating adjacent OLAP files.
      - [x] Consume embedded Vortex footer statistics in the shared local primitive planner through
        `VortexFile::can_prune` before scan creation for count/filter, scalar/group aggregate, and
        bounded top-K/sort routes; add route evidence fields for footer row count, segment count,
        statistics status, planner consumption status, selected/skipped segments,
        no-query-answer-cache posture, and no-read metadata-pruned results; lift those fields
        through public workflow and `vortex-run` outputs with focused Rust tests.
      - [x] Add the actual embedded/generic Vortex OLAP layout strategy beyond current footer
        inventory: writer/layout policy choices, segment/zone statistics inventory suitable for
        broader pruning, dictionary/statistics preservation evidence, and planner consumption fields
        beyond whole-file footer pruning.
        - Evidence: the admitted single-artifact layout advisor now applies upstream Vortex write
          strategy configuration with a finer row-block size, local Vortex opens use
          `with_layout_reader_cache()`, and write/primitive/public evidence reports
          `writer_layout_strategy_applied`, `writer_layout_row_block_size`,
          `layout_encoding_inventory`, `segment_membership_status`,
          `domain_dictionary_status`, `derived_layout_stats_status`,
          `row_position_locality_status`, and `layout_reader_cache_status`. Focused validation:
          `cargo test -p shardloom-vortex --features
          vortex-write,vortex-local-primitives,universal-format-io
          local_flat_scalar_rows_apply_layout_write_advisor_before_write -- --nocapture`,
          `prepared_olap_state_admits_plain_vortex_artifact_with_embedded_layout_status`,
          and `count_where_footer_pruning_avoids_scan_decode_and_materialization`.
      - [x] Re-run current 100M UAT after the embedded-layout strategy exists and after replacing
        the old Desktop Vortex artifact. Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_current_20260621T222424Z/targeted-summary.json`.
        The pass reports 43/43 successful native Vortex rows, zero fallback/external-engine
        violations, and no query-answer/OLAP sidecars; it does not close performance acceptance
        because `CB-Q33`, `CB-Q34`, and `CB-Q35` remain dominant slow rows.
      - [ ] Rework the embedded-layout/string/group-state path before release retention: the
        single-artifact path preserves correctness and the right product contract, but current
        timing shows it still does not prune enough string/high-cardinality work.
      - [x] Move the rejected exact query-summary sidecar prototype evidence to the completed ledger
        as non-runtime rejected-design provenance. Current JSON records it under
        `rejected_prepared_olap_query_summary_sidecar_uat`; it must not be used as runtime
        acceptance or ClickBench methodology.
    - [ ] Add embedded generic string/domain metadata for URL/Title/Referer predicates inside the
      prepared Vortex artifact or its Vortex-native metadata tree: segment-level absence
      certificates, dictionary/domain sketches, byte-length statistics, and safe substring/LIKE
      pruning evidence. This must prune or encode runtime work, not store query answers.
      - [x] Compact hidden URL-domain dictionaries during Universal Ingest when the source already
        arrives as dictionary UTF-8: hidden domain columns now remap row keys to unique domain
        codes instead of preserving URL-code cardinality, while hidden length columns keep the
        source dictionary keys for cheap byte-length lookup. This keeps the single `.vortex`
        artifact contract, reduces domain-group state for URL-heavy rows, and avoids any sidecar or
        query-specific cache. Focused validation:
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib
        source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis --
        --nocapture`,
        `cargo test -q -p shardloom-vortex --features universal-format-io --lib
        source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io
        --lib simple_aggregate_rewrites_url_domain_measures_to_embedded_vortex_column --
        --nocapture`, and
        `cargo clippy -q -p shardloom-vortex --features
        vortex-local-primitives,universal-format-io --lib -- -D warnings`.
      - [x] Add a first-class per-column embedded OLAP metadata contract consumed by the native
        planner: logical dtype, physical encoding, dictionary/code availability, value cardinality,
        segment membership, min/max/statistics, byte-length statistics, derived domain availability,
        row-position locality, and whether each field is used for pruning, encoded execution,
        late materialization, or diagnostic-only evidence.
        - [x] Added a deterministic per-column metadata contract to the embedded Vortex layout
          report, Universal Ingest prepared-state inventory, public prepared-OLAP attachment
          fields, and native route fields. The contract is derived once from the single `.vortex`
          artifact dtype/layout inventory, classifies source versus hidden derived columns,
          dictionary/code availability, file-statistics availability, and roles such as pruning,
          encoded execution, aggregate measure, group key, late materialization, or diagnostic-only.
          Aggregate layout/accessor correlation summaries now carry the same contract plus
          operator-selection metadata status, tying the report to planner-facing evidence instead
          of leaving it as a file-level label. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_rewrites_url_domain_measures_to_embedded_vortex_column -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-write,universal-format-io --lib prepared_olap_state_admits_plain_vortex_artifact_with_embedded_layout_status -- --nocapture`,
          `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom local_primitive_embedded_layout_lifts_footer_pruning_fields -- --nocapture`,
          `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom prepared_local_route_marks_admitted_olap_state_attached_to_embedded_layout_metadata -- --nocapture`,
          `cargo clippy -q -p shardloom-vortex --features vortex-write,universal-format-io --lib -- -D warnings`,
          `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
          `cargo clippy -q -p shardloom-cli --features release-user-surfaces --bin shardloom -- -D warnings`,
          and `cargo fmt --all -- --check`.
        - [x] Rename the public prepared-OLAP attachment field away from
          `query_answer_sidecar_consumed` and toward
          `public_workflow_prepared_olap_embedded_layout_metadata_consumed`, so CLI/Python/agent
          evidence describes single-artifact metadata consumption instead of an obsolete rejected
          sidecar concept. Focused validation:
          `cargo test -q -p shardloom-cli prepared_local_route_marks_admitted_olap_state_attached_to_embedded_layout_metadata -- --nocapture`
          and
          `PYTHONPATH=python/src python -m unittest python.tests.test_query_builder.LazyWorkflowBuilderTests.test_single_artifact_native_vortex_evidence_converges_across_public_surfaces`.
      - [x] Thread that metadata into operator selection before row export so
        `domain_dictionary_status` and `layout_encoding_inventory` are not just report fields. The
        aggregate planner must choose `Utf8Dictionary`/dictionary-code accessors, dictionary unions,
        segment-membership masks, or deterministic materialization boundaries from the same
        evidence.
        - [x] First exact single-artifact metadata consumption slice: Universal Ingest now embeds
          hidden compact `UInt32` UTF-8 byte-length columns for admitted high-value URL/search/title
          text fields and exact dictionary-encoded URL-domain columns for URL/Referer/URI-like
          fields directly in the prepared `.vortex` artifact when the source adapter can produce
          them without regressing large-source preparation. The typed-text bridge generates URL-like
          length/domain columns in one source-string pass; product columnar adapters do not
          synthesize physical derived columns from plain UTF-8 batches after 100M Parquet UAT
          rejected the per-row preprocessing cost. Public columnar preparation now admits only the
          source-native subset: dictionary-backed URL-like columns transform dictionary values once
          and remap existing codes into compact length/domain columns, while typed numeric/
          timestamp time fields can produce compact minute keys without string scans. The native
          aggregate planner rewrites `length(...)` measures,
          URL-domain group expressions, and non-empty string predicates to those embedded columns
          when they exist, while
          `ProjectionRequest::All` hides `__shardloom_derived_*` columns from user output.
          Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib embedded -- --nocapture`
          and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_compact_embedded_derived_columns -- --nocapture`.
        - [x] Tighten single-artifact layout inventory classification so prepared `.vortex`
          artifacts with ShardLoom-owned embedded URL-domain and UTF-8 length columns report
          `embedded_utf8_domain_columns_present_with_length_stats_dictionary_layout_not_observed`
          instead of collapsing into generic UTF-8 presence. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-write,universal-format-io --lib
          streaming_vortex_write_preserves_compact_embedded_derived_columns -- --nocapture`.
        - [x] Keep compact embedded numeric dictionary metadata consumable by native operators:
          Vortex `Dictionary<key, UInt32>` hidden length fields now lower at the aggregate accessor
          boundary to direct typed numeric values instead of materialized `StatValue` rows, so
          count/sum/avg/min/max/count-distinct/group-key paths can consume dictionary-backed
          derived length metadata without a facade-specific route. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_keeps_numeric_vortex_dictionary_direct -- --nocapture`.
        - [x] Tighten the common non-null numeric dictionary accessor path for embedded numeric
          metadata: dictionary values are converted once, row codes reuse the converted dictionary
          values, and no per-row null bitmap is allocated when both dictionary codes and values are
          proven non-null. Nullable dictionary paths keep the explicit null mask. Focused
          validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_keeps_numeric_vortex_dictionary_direct -- --nocapture`.
      - [x] Add per-route/per-column evidence explaining why a slow string route used
        `chunk_dictionary_code_map`, `transformed_chunk_dictionary_code_map`,
        `chunk_materialized_partial_map`, or a residual row-state path; include blocker codes when
        a dictionary/layout was present globally but not consumable for the specific column chunk.
        - [x] Add operator-level aggregate accessor evidence and public workflow lifting for
          `aggregate_accessor_summary`, `aggregate_accessor_materialization_status`,
          `aggregate_vortex_dictionary_accessor_columns`, `aggregate_chunk_dictionary_accessor_columns`,
          `aggregate_primitive_accessor_columns`, and `aggregate_materialized_accessor_columns`.
          This separates true Vortex `DictArray` code consumption from chunk-local UTF-8 dictionaries
          and materialized values; focused validation:
          `cargo test -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_vortex_dict_array_child_uses_dictionary_accessor -- --nocapture`
          and `cargo test -p shardloom-cli local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`.
        - [x] Add stable blocker codes for the column/chunk reason when the operator still observes
          `materialized_stat_values`: materialized aggregate accessors now report
          `cg21.aggregate_accessor.materialized_after_direct_provider_miss` through
          `aggregate_accessor_blockers`/`local_primitive_aggregate_accessor_blockers`. The next UAT
          should correlate these codes with artifact-level layout inventory before choosing the next
          layout/accessor optimization.
      - [x] Add generic derived-column metadata in the single `.vortex` artifact for exact URL
        domain, URL byte length, timestamp minute/date buckets, and non-empty string masks when
        generated during Universal Ingest. These are storage/layout features, not query-specific
        summaries.
        - [x] Implement and test the first exact derived-column family: URL/Referer domain and
          UTF-8 byte length are generated during Universal Ingest, persisted into the single
          `.vortex` file as scoped compact/dictionary-aware hidden columns, and consumed by native
          count/filter/aggregate planning. Non-empty string predicate support is represented by
          exact `length > 0` rewrites when an admitted length column exists instead of a separate
          mask file. Timestamp buckets and broader substring/posting metadata remain open pending
          UAT-driven proof.
        - [x] Add exact compact minute-of-hour derived keys for admitted time-like fields inside the
          same `.vortex` artifact and consume them from grouped aggregate planning.
          `extract(minute FROM EventTime)` rewrites to
          `__shardloom_derived_extract_minute_EventTime` when present, while the existing
          numeric-minute-string compact state still recognizes the key as a minute role instead of
          demoting to a generic identity group. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_rewrites_extract_minute_to_embedded_vortex_column -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_minute_string_uses_embedded_prepared_minute_key -- --nocapture`,
          and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_compact_embedded_derived_columns -- --nocapture`.
        - [x] Add exact date-trunc minute bucket metadata for admitted typed time-like fields inside
          the same `.vortex` artifact and consume it from grouped aggregate planning.
          `DATE_TRUNC('minute', EventTime)` / `date_trunc_minute(EventTime)` rewrites to
          `__shardloom_derived_date_trunc_minute_EventTime` when present, using source-native
          integer/timestamp units during Universal Ingest rather than a query sidecar or
          per-row runtime transform. Focused validation:
          `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_typed_time_stream_embeds_compact_minute_metadata -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features universal-format-io --lib source_native_timestamp_units_embed_compact_minute_metadata -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib streaming_vortex_write_preserves_compact_embedded_derived_columns -- --nocapture`,
          and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib simple_aggregate_rewrites_date_trunc_minute_to_embedded_vortex_column -- --nocapture`.
        - [x] Extend the same embedded non-empty string rewrite into bounded sort/top-K planning,
          including partitioned prepared Vortex sources, so `SearchPhrase <> ''`-style candidate
          scans can consume exact hidden length columns before string row materialization. Public
          evidence lifts `local_primitive_embedded_derived_column_rewrite_status` and
          `local_primitive_embedded_derived_column_rewrites`.
        - [x] Extend the embedded non-empty string rewrite into generic filter row-export and
          sampled row-export planning so materializing routes consume exact hidden length metadata
          before writing rows, while hidden `__shardloom_derived_*` fields remain excluded from
          user-visible output. Residual sample predicates are applied before sampling; distinct and
          drop-duplicate row exports now apply residual selected-row filters before exact row-key
          state, while still keeping nested/list key columns out of compatibility output
          materialization.
        - [x] Apply metadata-first footer pruning to filtered materializing row-export routes:
          if `VortexFile::can_prune` proves an admitted filter cannot match, filter,
          filter-project, distinct, drop-duplicate, and sample outputs write the requested empty
          JSONL/CSV compatibility sink atomically without opening a scan, decoding rows, building
          row-key state, sampling candidates, or materializing helper columns. Evidence reports
          `upstream_scan_called=false`, `data_read=false`, `data_decoded=false`,
          `data_materialized=false`, `write_io=true`, and no fallback/external execution.
          Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib row_export -- --nocapture`
          and `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sample_rows -- --nocapture`.
        - [x] Extend the same no-scan empty-output contract to schema-known structured row exports:
          expression-project writes to Parquet/Arrow IPC/Avro/native Vortex can derive source-column
          logical dtypes from the `.vortex` schema, write empty schema-correct outputs when footer
          stats prove no rows match, and preserve `upstream_scan_called=false` evidence. Focused
          validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib structured_ -- --nocapture`
          and
          `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives,universal-format-io,vortex-write --lib -- -D warnings`.
        - [x] Extend the shared Vortex-normalized source predicate boundary into row-transform
          routes: expression-project, melt, explode, pivot, and rolling-window collect/export paths
          now attach source predicates through the same scan plan, consult footer pruning before
          scans when possible, apply residual predicates before typed rewrites, expansion, pivot
          state, or rolling-window state, and keep predicate-only columns out of visible output.
          Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib melt_rows -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib explode_rows -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib pivot_rows -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib rolling_window_rows -- --nocapture`,
          and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib row_export -- --nocapture`.
        - [x] Preserve embedded-derived rewrite evidence through the same row-transform collect
          routes after the shared predicate boundary runs: expression-project, melt, explode,
          pivot, and rolling-window scans now carry hidden length/domain rewrite lists on both
          metadata-pruned and scanned paths, and result summaries expose the same applied rewrite
          suffix used by row-state operators. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib expression_project_rows_ -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib melt_rows_ -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib explode_rows_ -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib pivot_rows_ -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib rolling_window_rows_ -- --nocapture`,
          and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Ensure any metadata-derived optimization is exact, source-hash invalidated, counted in
        artifact size/load time, exposed through Native I/O evidence, and disabled for official
        benchmark comparison if it would amount to a query-answer cache or materialized view.
        Evidence: prepared OLAP state now reports source content/schema digests, prepared artifact
        digest/size, embedded layout metadata persistence, `exact_sidecar_family_count=0`,
        `query_answer_sidecar_status=disabled_rejected_for_public_default_runtime`,
        `external_manifest_written=false`, and no fallback/external-engine execution. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-write,universal-format-io --lib prepared_olap -- --nocapture`.
    - [ ] Add generic prepared layout families selected by source/profile evidence: date/counter
      clustering, URL/domain dictionary preservation, low-cardinality dictionary-union metadata,
      and high-cardinality key layout hints. Do not create pre-aggregated summaries,
      materialized-view equivalents, or query-specific projections as public/default runtime.
      - [x] Added explicit `prepared_layout_family` and `key_profile` advisor evidence for
        URL/text/time/counter/high-cardinality source shapes and selected the fields into public
        workflow preparation output. This is the non-physical layout-family admission layer; the
        parent remains open for physical partition/posting-list/spill-backed layout work that must
        prove a real runtime win before retention.
    - [ ] Add row-reference locality for wide bounded top-N through Vortex layout/page metadata and
      late materialization: keep ordered candidate row refs until final output, then materialize
      only retained rows from the single `.vortex` artifact.
      - [x] Added the runtime row-reference half of this contract for admitted large bounded
        payload projections: candidate scans project predicate/order columns, preserve source
        ordinals, and materialize only the final retained rows from the same `.vortex` artifact.
      - [ ] Add prepared layout/page-level row-position or predicate posting metadata when the
        single-artifact Vortex metadata surface can represent it without query-answer sidecars.
    - [ ] Implement hierarchical capillary aggregate state for high-cardinality grouped count/sum/
      avg/top-K families: segment-local partials, packed typed/composite keys, memory-budgeted
      merge, state pressure evidence, and optional spill diagnostics before process OOM risk.
      - [x] Added packed typed/composite keys, compact count/numeric measure slots, transformed
        dictionary grouping, materialized UTF-8 chunk-local exact partial counts, streaming
        retained-candidate top-K, and state-pressure evidence for shared grouped aggregate routes.
      - [x] Added generic streaming count-star top-K finalization for count-only ordered grouped
        routes so exact `ORDER BY count DESC LIMIT K` keeps only the retained candidate window
        before final row materialization. Evidence reports `capillary_streaming_count_star_topk`,
        `streaming_count_star_topk_retention`, `typed_count_star_key_topk`, and zero materialized
        group values; targeted local 100M UAT retained the improvement for `CB-Q34`/`CB-Q35`.
      - [x] Admit residual-free pushdown-filtered aggregate/top-K routes into existing
        heavy-hitter and late-measure families: Vortex-pushed filters no longer disqualify string
        count top-K, numeric+UTF-8 top-K, or numeric-pair late-measure routes, and every second
        pass/exact fallback scan reapplies the same Vortex pushdown predicate. Residual predicates
        remain on exact residual-safe paths. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib heavy_hitter_routes_admit_pushdown_only_predicates -- --nocapture`.
      - [ ] Add real partitioned/spill-backed exact merge execution beyond diagnostics for
        near-unique grouped state such as `CB-Q33` when in-memory state is not enough.
    - [ ] Rework bounded ordered group output only with a proven lower-cost strategy than the
      current `capillary_ordered_topk` partial selection. A first retained-candidate scan attempt
      was rejected because partial UAT
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_archopt_20260621T065434/partial-summary.json`
      regressed `CB-Q16`/`CB-Q17`/`CB-Q18`; do not reintroduce it without targeted before/after
      evidence.
    - [ ] Improve `CB-Q19`-class exact triple-key groups with `UserID`/minute/`SearchPhrase`
      specific typed-key packing, local top-K retention where legal, and final string
      materialization only for surviving groups.
      - [x] Add a shared exact typed numeric/minute/string count-state route inside the native
        grouped aggregate implementation, with dictionary-backed string IDs, direct minute
        extraction, streaming retained-candidate top-K, final-row-only string materialization, and
        focused regression coverage. This is admitted by shape through the existing Vortex-native
        aggregate path, not by a ClickBench-only route.
      - [ ] Re-run targeted 100M UAT for the direct numeric/minute/string count-state route before
        it can be retained in a release train. Targeted 100M UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_recent_changes_20260621T084607/summary.json`
        timed out `CB-Q19` at the 180-second cap before the shared compact route landed. Keep or
        revise the approach based on measured post-change evidence; if it still does not move a
        dominant cost class, replace it with segment-local partials/dictionary-code grouping or
        another exact generic state strategy.
    - [ ] Improve `CB-Q33`-class nearly-unique numeric-pair aggregation with partitioned or
      budget-aware exact state, packed pair keys, measure-plan reuse, and merge evidence that does
      not allocate generic per-group state.
      - [x] Add explicit typed hash-state reservation for single-numeric, numeric/minute/string,
        and numeric-pair aggregate maps before hot updates. Targeted 100M UAT retained the
        numeric-pair result (`CB-Q33` 173.236s -> 143.428s) and backed out adjacent string hash
        changes that did not show clean improvement.
      - [x] Add a direct primitive key-slice loop for numeric-pair compact and late-measure
        grouped aggregate updates. The hot path now admits non-null direct `u64`/`i64` key columns
        once per chunk, constructs packed pair keys from primitive slices instead of re-matching
        generic accessors per row, and reports
        `numeric_pair_direct_key_slice_update`,
        `numeric_pair_key_accessor_match_bypass`, and
        `+direct_key_slices` state-family evidence. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_pair_uses_streaming_topk_compact_state -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_pair_late_measure_uses_count_topk_second_pass -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
    - [ ] Add prepare-time exact metadata/indexes for string predicate and string-derived grouping
      families: literal substring posting lists or segment membership indexes, URL-domain
      dictionaries, URL/referrer byte-length columns, derived encoded columns where semantics are
      exact, and evidence for when the prepared layout lacks the required encoding. A byte-search
      loop-only attempt was rejected after targeted 100M UAT failed to move `CB-Q21`.
    - [x] Promote URL/string grouping from interned string retention to exact dictionary/code or
      chunk-local partial grouping over the actual current Vortex layout, including
      `chunk_dictionary_count_star_group_update` activation evidence when it runs.
      - [x] Add a shared transformed dictionary count-star path for single-key `length(URL)` and
        `url_domain(URL)` grouped routes, computing the transform once per chunk dictionary value
        and reusing counts by dictionary ID. Evidence reports
        `chunk_dictionary_transformed_count_star_group_state`,
        `transformed_chunk_dictionary_code_map`, and
        `string_transform_reuse_by_dictionary_id`; focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_count_star_transformed_dictionary_reuses_group_transform_per_dictionary_value`.
      - [x] Add the shared materialized-UTF-8 chunk partial path for the same single-key URL/string
        group families when the current Vortex layout does not surface dictionary codes: chunk-local
        exact partial counts are merged into compact count-star group state without per-row string
        transform recomputation. Evidence reports
        `chunk_materialized_partial_count_star_group_state`,
        `transformed_chunk_materialized_partial_map`,
        `chunk_materialized_string_partial_group_update`, and
        `string_transform_reuse_by_chunk_value`; focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_count_star_materialized_url_domain_uses_chunk_local_partials -- --nocapture`.
      - [x] Add direct aggregate accessor support for Vortex `DictArray` UTF-8 children so struct
        chunks can choose `Utf8Dictionary` from Vortex codes/values before rebuilding a materialized
        string dictionary. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_star_vortex_dict_array_child_uses_dictionary_accessor -- --nocapture`.
        Evidence now records `URL:vortex_utf8_dictionary` versus chunk-local/materialized accessors
        through the primitive summary and public workflow fields.
      - [x] Extend the direct string accessor to safe non-null UTF-8 chunks surfaced by the current
        reader even when Vortex does not expose a stable `DictArray` code view, building exact
        chunk-local dictionaries before generic row materialization. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_uses_utf8_chunk_dictionary_before_materialized_rows -- --nocapture`.
        Targeted local 100M UAT shows `URL:chunk_utf8_dictionary`,
        `chunk_dictionary_count_star_group_update`, and `chunk_dictionary_code_map` for the URL
        grouping routes.
      - [x] Extend the direct dictionary accessor to nullable dictionary diagnostics, FSST/string
        dictionary provider checks, and column-specific blockers when global layout inventory says
        dictionary is present but the route chunk still materializes.
        - [x] Attach stable materialization blocker IDs to aggregate accessors themselves, so
          grouped routes report nullable Vortex dictionary codes, nullable Vortex dictionary
          values, nullable UTF-8 chunk dictionaries, UTF-8 provider unavailability, and generic
          provider misses as column-specific blockers instead of collapsing every slow path into
          `materialized_after_direct_provider_miss`. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
          aggregate_accessor -- --nocapture` and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
          grouped_count_star -- --nocapture`.
        - [x] Correlate per-column provider-miss blocker IDs with the artifact-level
          `layout_encoding_inventory`/`domain_dictionary_status` fields so a route can explain
          when the prepared `.vortex` artifact advertises dictionary/FSST-like layout globally but
          the current chunk still cannot expose a consumable dictionary/code accessor. Implemented
          as aggregate result-summary fields
          `aggregate_accessor_layout_correlation_status`,
          `aggregate_accessor_layout_correlation_columns`,
          `aggregate_accessor_layout_correlation_blockers`,
          `aggregate_accessor_layout_correlation_dictionary_status`, and
          `aggregate_accessor_layout_correlation_encoding_inventory` on the local primitive
          aggregate route, with matching `local_primitive_aggregate_accessor_layout_correlation_*`
          fields lifted by the public workflow route evidence layer.
        - [x] Promote nullable Vortex and chunk-local UTF-8 dictionaries from a
          materialization/blocker diagnostic into the direct exact aggregate path: dictionary codes
          remain encoded, value/row validity masks are carried with the accessor,
          `COUNT(column)`/`count_distinct(column)` skip nulls, grouped `COUNT(*)` admits the SQL
          null group key exactly, and packed/proof-bound dictionary-code routes decline nullable
          dictionaries unless their representation can prove non-null rows. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_keeps_nullable_utf8_chunk_dictionary_direct -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_nullable_utf8_dictionary_stays_direct_without_materialization -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_reuses_chunk_dictionary_counts -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`, and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`.
        - [x] Extend the same null-aware UTF-8 dictionary/direct accessor contract to string
          predicate execution: nullable host UTF-8, nullable Vortex dictionary values/codes, and
          filtered/masked dictionary arrays now skip SQL-null rows for both positive and negated
          `contains` predicates without falling back to materialized string rows. True Vortex
          dictionary row-index selection is admitted, while the previously rejected chunk-local
          row-index fallback remains out until prepared row-position metadata can prove it is
          cheaper for bounded top-K/materialization routes. Focused validation:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_skips_nulls_without_materialized_string_fallback -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_count_only_admits_host_varbinview_without_decode -- --nocapture`,
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib filtered_utf8_contains_uses_mask_first_host_and_dictionary_paths -- --nocapture`, and
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_reuses_chunk_dictionary_counts -- --nocapture`.
    - [ ] Replace bounded top-N/sort wide-row work with prepared row-position/payload-locality
      indexes, row-ref or selection-vector heaps, and final-K payload materialization. A raw
      `select_nth_unstable` retention-loop attempt was rejected after targeted 100M UAT did not
      improve `CB-Q24`; the next attempt must reduce source string scan or wide payload reread
      work, not just comparator cost.
      - [x] Replace periodic full-sort retention with capillary `select_nth_unstable` retained
        windows for bounded sort/top-K routes, while preserving final deterministic sort of the
        retained window and existing wide-output second-pass materialization. Evidence reports
        `capillary_select_nth_retention_window`; focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives sort_rows_reports_topk_offset_state_budget_without_fallback`.
      - [x] Promote the bounded sort materialization policy from a fixed wide-column heuristic to a
        dynamic row-reference policy for large bounded payload projections: candidate scans project
        predicate/order columns, retain source ordinals, and materialize only final rows from the
        same `.vortex` artifact when `row_ref_topk_materialization_policy` admits the shape.
        Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives sort_rows_ -- --nocapture`.
      - [x] Use selected-row materialization inside the final wide chunk so retained row refs pull
        only final-K ordinals from Vortex arrays; this avoids whole-chunk payload decode for
        admitted bounded top-K output and reports
        `late_materialization_selected_row_refs_used` through the public route evidence lift.
    - [x] Add dictionary-aware exact distinct: dense-ID bitsets or per-segment dictionary unions
      where available, encoded-key distinct before decode, and decoded-reference parity for nulls,
      all-null, duplicate, and mixed-type cases.
      - Evidence: scalar `count_distinct` now reports
        `scalar_count_distinct_state+direct_dictionary_or_typed+dense_integer_preunion` with
        `typed_dense_integer_preunion_exact`, UTF-8 dictionary count-distinct reports
        `scalar_count_distinct_state+direct_dictionary_arc` with
        `dictionary_arc_direct_exact`, grouped count-distinct uses
        `direct_accessor_count_distinct_group_update`, and null/mixed typed distinct cases preserve
        exact typed keys without string-prefix conflation. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
        aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback --
        --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
        scalar_string_count_distinct_reports_dictionary_arc_state -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
        grouped_count_distinct_direct_accessor_update_avoids_row_materialization -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib
        direct_count_distinct_integer_uses_dense_id_preunion_for_compact_ranges -- --nocapture`.
      - [x] Add a scalar direct exact count/count-distinct path over Vortex chunks for unfiltered
        aggregate routes, using typed primitive accessors and chunk dictionaries before row export.
        Evidence reports `scalar_count_distinct_state+direct_dictionary_or_typed`,
        `typed_or_dictionary_direct_exact`, and
        `dictionary_or_typed_direct_count_distinct`; focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback`.
      - [x] Extend the same scalar direct path to typed/dictionary scalar `count`, `sum`, `avg`,
        `min`, and `max` updates, plus exact UTF-8 `length(...)` numeric measures, before row
        export. Evidence reports `scalar_aggregate_state+direct_dictionary_or_typed`,
        `dictionary_or_typed_direct_scalar_update`, and
        `dictionary_or_typed_direct_scalar_aggregate`; focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives simple_aggregate_accumulates_scalar_measures_without_fallback`.
      - [x] Correct exact UTF-8 dictionary distinct to union the used dictionary codes rather than
        assuming every dictionary value appears in the chunk, preserving exactness when a Vortex
        dictionary contains unused values and keeping the update in the dictionary-code path.
        Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture`.
      - [x] Pre-reserve shared general direct grouped aggregate state before direct
        count-distinct/general updates, matching the specialized high-cardinality routes'
        bounded state posture without adding another execution route. Evidence now reports
        `+pre_reserved`, `direct_general_group_state_pre_reservation`, and
        `direct_general_group_capacity_pressure` alongside existing no-materialization
        count-distinct evidence. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_distinct_direct_accessor_update_avoids_row_materialization -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Move scalar/direct exact `count_distinct` state from the standard hash set to the
        existing ShardLoom `FxHashSet` hot-state profile. This keeps semantics unchanged because
        the scalar route only observes exact cardinality, but reduces hash-state overhead for typed
        and dictionary distinct routes. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`, and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`.
      - [x] Promote retained-null primitive aggregates into direct nullable primitive accessors
        instead of materialized `StatValue` rows: validity masks now stay with typed `i64`/`u64`/
        `f64` accessors, scalar `count`/`sum`/`avg`/`min`/`max`/`count_distinct` skip nulls
        exactly, and compact grouped-key fast paths still decline nullable primitive keys unless a
        separate non-null proof exists. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_keeps_selected_nullable_primitives_direct_with_retained_nulls -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib nullable_primitive_direct_aggregate_semantics_skip_nulls -- --nocapture`, and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback -- --nocapture`.
      - [x] Add dense-ID per-chunk exact distinct pre-union for compact integer ranges before
        inserting into the scalar/direct exact distinct state. This collapses duplicates over
        dense `u64`/`i64` segments with validity masks, bails out early for sparse/wide integer
        ranges to preserve the previous exact-set cost profile, and reports
        `typed_dense_integer_preunion_exact`,
        `scalar_count_distinct_state+direct_dictionary_or_typed+dense_integer_preunion`, and the
        `dense_integer_distinct_preunion` capillary unit when used. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_integer_uses_dense_id_preunion_for_compact_ranges -- --nocapture`.
      - [x] Add proof-bound string count-distinct top-K for `CB-Q14`-class grouped exact distinct:
        candidate sketch over chunk UTF-8 dictionary values, exact candidate distinct recount over
        direct primitive values, public proof-field lifting, and exact fallback when the ProofBound
        threshold is not met. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib aggregate_accessor_keeps_filtered_primitives_typed -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_distinct -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib string_count_topk -- --nocapture`, and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8 -- --nocapture`.
        Targeted local 100M UAT retained exact row parity with the previous route and moved `CB-Q14`
        to `11.15s` / `13.88s` from the latest `24.65s` full-run result, with no fallback or
        external-engine invocation and machine-readable public fields for
        `local_primitive_distinct_state_strategy`,
        `local_primitive_string_count_distinct_topk_heavy_hitter_*`, and
        `local_primitive_uniqueness_proof_status`. PR #1350 then closed the selected-valid
        nullable primitive accessor gap for this family: `CB-Q14` targeted local UAT retained
        `UserID:direct_i64`, no aggregate accessor blockers, and zero materialized group values;
        the remaining exact-distinct work is broader dense-ID/per-segment dictionary union coverage
        and rows that still expose real provider-miss/materialization evidence.
      - [x] Retain dictionary-backed UTF-8 values as `Arc<str>` inside shared exact distinct state
        instead of cloning dictionary strings into owned keys for each used value. This applies to
        the scalar/direct exact distinct path and the grouped direct-accessor path because both use
        the same `AggregateDistinctValue` representation, while decoded output still materializes
        plain UTF-8 strings at the boundary. Route evidence now distinguishes the path with
        `dictionary_arc_direct_exact`, `dictionary_arc_distinct_state`, and
        `dictionary_string_clone_bypass`. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib scalar_string_count_distinct_reports_dictionary_arc_state -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib direct_count_distinct_uses_used_dictionary_codes_not_all_dictionary_values -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_count_distinct_direct_accessor_update_avoids_row_materialization -- --nocapture`,
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
    - [ ] Extend the prepared Vortex layout advisor so universal ingest can choose
      ClickBench-like OLAP layout policy from data/profile evidence: date/counter partitions,
      URL/search dictionaries, low-cost exact derived columns, segment stats, and read/write
      tradeoff evidence without creating benchmark-only shortcuts.
      - [x] Replace stale fixture workload evidence with a reusable product source/profile
        contract on public Vortex preparation: layout/write advisor evidence now records source
        format, scale, adapter family, URL/text-domain presence, time-bucket presence,
        counter/high-cardinality hints, and dictionary posture. The writer decision consumes that
        profile so large text-domain OLAP sources keep the field-aware dictionary-Zstd profile,
        while large non-text sources can use the fast-load profile. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-write --lib local_flat_scalar_rows_use_source_text_large_source_layout_row_blocks_when_advised -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-write --lib local_flat_scalar_rows_use_fast_load_large_source_layout_when_not_text_domain -- --nocapture`,
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom vortex_ingest_parquet_public_prepare_uses_row_group_capillary_executor -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-write --lib -- -D warnings`,
        `cargo clippy -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom -- -D warnings`,
        and `cargo fmt --all -- --check`.
      - [x] Promote the source/profile layout decision out of an opaque workload string into
        first-class advisor evidence fields: source scale, profile family, text-domain/time-bucket/
        counter booleans, prepared-layout family, high-cardinality/text/time key profile,
        dictionary profile, and source-profile-specific read/write tradeoff labels. This lets
        public workflow validators and future physical planners consume the prepared-layout family
        without parsing `workload_constitution`, while preserving the
        single `.vortex` artifact and no query-answer sidecar rule. The same fields are selected
        into `public_workflow_preparation_*` output so Python/SQL/DataFrame front doors can see the
        prepared-layout decision after local-source normalization. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-write --lib layout_write_advisor_ -- --nocapture`
        and
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom public_workflow_preparation_keeps_layout_profile_fields -- --nocapture`,
        plus
        `cargo test -q -p shardloom-cli --features vortex-write,universal-format-io --bin shardloom layout_writer_provider_uses_streaming_vortex_provider_for_non_empty_columnar_source -- --nocapture`.
    - [x] Add expression fusion/reuse for repeated SQL/DataFrame expressions and aggregate measure
      plans so repeated SUM/length/domain/minute/cast expressions compile once and share
      intermediate state across operators.
      - [x] Added direct/materialized numeric additive fusion for repeated SUM/AVG measures over
        the same Vortex accessor, including expression-plan fingerprint evidence and shared update
        capillary units.
      - [x] Reused dictionary value/count pairs for repeated scalar string length aggregate
        measures over UTF-8 dictionaries, matching the grouped dictionary transform path and
        reporting `dictionary_weighted_transform_fusion` without row-string materialization.
      - [x] Extend reuse to repeated string/domain/date/cast transforms where generic prepared
        metadata exists and the shared runtime can prove exact semantics.
        - [x] Scalar and grouped aggregate measures now rewrite prepared `url_domain`,
          `extract_minute`, and `date_trunc_minute` transforms to embedded single-artifact
          derived columns, with focused coverage for grouped transformed measures:
          `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_rewrites_time_transform_measures_to_embedded_vortex_columns -- --nocapture`.
    - [x] Add or update route evidence fields for every optimization: aggregate state strategy,
      key encoding mode, dictionary/sketch/derived-column availability, retained candidate count,
      decoded string count, materialized row count, memory/state byte estimate, spill posture,
      prepared layout policy, and timing surface.
      - Evidence: Vortex primitive and public workflow evidence now includes embedded layout
        inventory fields, direct aggregate update strategy/state-budget fields, capillary
        select-nth top-K retention fields, row-reference final-K materialization fields,
        transformed dictionary/materialized chunk-partial grouping fields, per-column aggregate
        accessor consumption fields, expression fusion fields, and writer layout strategy fields.
        Focused validation: Vortex writer/layout tests,
        partitioned
        aggregate/top-K tests, route capability validator, SQL/Python/DataFrame parity validator,
        and ClickBench OLAP coverage validator.
    - [x] Add focused native Vortex primitive tests for streaming single-numeric count top-K,
      source-order limited group admission, public result-summary evidence lifting, and
      metadata-preserving Vortex output count behavior.
    - [ ] Add focused correctness tests for high-cardinality grouped aggregates, exact distinct,
      string contains/domain/length, top-K/offset tie ordering, expression reuse, null behavior,
      and decoded-reference parity.
      - [x] Added focused tests for row-reference final-K materialization state budgeting,
        direct/materialized repeated numeric aggregate fusion, exact dictionary distinct with
        unused dictionary values, and materialized URL-domain chunk-local partial grouping.
      - [x] Added a row-result regression for `sort_rows` with `offset` plus `limit`, proving the
        selected second ordered row is returned through the native Vortex path with
        `fallback_attempted=false`:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_offset_returns_second_ordered_row_without_fallback -- --nocapture`.
    - [ ] Run targeted local 100M UAT for every previously >10s or timeout-prone row, then run the
      full 43-query native Vortex UAT with the agreed local safety cap after targeted rows are
      stable.
    - [ ] Update README/docs/architecture/capability evidence only from measured route output and
      move completed detail to the ledger after merge/session completion.
  - Next outcome: the remaining slow ClickBench families get shared runtime improvements and a
    focused-validation-backed PR/merge; the fresh full 100M UAT transcript is captured after merge
    and before any later version/release train.
  - User-visible surface: native Vortex SQL/operator runtime, Python/DataFrame-style front doors
    after preparation, CLI `run dataframe`, local ClickBench UAT evidence, route/capability
    reports, and docs.
  - Implementation scope: `shardloom-vortex/src/local_primitives.rs`, aggregate/string/top-K/
    distinct/layout-advisor helpers, universal ingest prepared-layout policy, public workflow
    evidence transport, tests, docs, and UAT artifacts.
  - Evidence required: focused correctness tests, route/evidence snapshots, targeted 100M UAT rows,
    full 43-query UAT transcript, no-fallback/no-external-engine fields, and claim-boundary docs.
  - Acceptance: all feasible slow families run through shared native/prepared Vortex routes with
    no hidden external execution; any row still over one second has concrete state/layout/decode
    evidence explaining the bottleneck and an explicit reason if no further in-repo optimization is
    feasible in the current pass.
  - Verification: focused Rust tests for changed operator families, focused Python/CLI route tests
    if public evidence changes, targeted 100M UAT for the affected rows when needed, then the full
    43-query 100M local UAT after merge and before any version/release train.
  - Non-goals: ClickBench-only query shims, official leaderboard submission, unsupported semantic
    shortcuts, external query-engine fallbacks, lowering correctness to improve timing, or full
    workspace test runs before implementation is complete.
  - Claim boundary: local UAT optimization evidence only; no public superiority claim until
    benchmark methodology, hardware, correctness, and publication gates are complete.
  - Fallback boundary: all optimization remains ShardLoom-native/Vortex-native; residuals are
    ShardLoom-owned or deterministically rejected with `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: move completed detail after merge/session completion.

- [ ] `CLICKBENCH-100M-RUNTIME-BURNDOWN-2` Full-scale native Vortex aggregate, string, distinct,
  and bounded top-K runtime burndown.
  - Source: post-merge 100M local ClickBench UAT against
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m.vortex` using the
    merged `target/release/shardloom run dataframe ... --execution-policy native_vortex` CLI route.
    Combined local evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_post_merge_combined_summary.json`.
    Post-`#1336` follow-up evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_post_1336_20260621T001627/summary.json`.
    Compact-state targeted evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_compact_state_20260621T001/summary.json`.
    Fixed-key targeted evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_fixed_key_20260621T002/summary.json`.
    URL chunk-dictionary probe:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_url_chunkdict_20260621T003/summary.json`.
    Direct minute-key probe:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_direct_minute_20260621T004/summary.json`.
    Typed/state-elision targeted evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_state_elision_20260621T020609/summary.json`.
    Numeric-pair measure-plan targeted evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_numeric_pair_measure_plan_20260621T025508/summary.json`.
    Current six-row targeted follow-up:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_former_timeouts_current_20260621T025936/summary.json`.
    Current full-43 replacement-artifact UAT:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_current_20260621T222424Z/targeted-summary.json`.
  - Current state: the 100M native Vortex route now has 43/43 successful local ClickBench rows with
    zero runtime errors, zero unsupported rows, zero timeouts at the 180-second local UAT cap, and
    zero fallback/external-engine violations. `CB-Q01` is confirmed as a metadata-preserving
    native Vortex count route after using the checked-in comment-safe ClickBench statement splitter;
    `CB-Q08` and `CB-Q36` were corrected by narrowing materialized string-partial admission so
    numeric identity group keys fall through to numeric/generic aggregate paths instead of failing
    with an invalid UTF-8 key diagnostic. The remaining slow family is performance, not route
    coverage: `CB-Q33` 173.236s, `CB-Q35` 157.799s, `CB-Q34` 154.694s, `CB-Q23` 50.124s,
    `CB-Q17` 50.072s, `CB-Q24` 38.666s, `CB-Q29` 23.456s, `CB-Q19` 19.103s, `CB-Q22` 16.380s,
    `CB-Q21` 13.370s, `CB-Q28` 12.982s, `CB-Q14` 12.887s, and `CB-Q18` 10.579s.
  - Intake review: accepted every >1s UAT row into this burndown rather than preserving them as
    advisory notes. Rows are grouped by shared runtime family so the fix converges public SQL,
    DataFrame, Python, and CLI wrappers into reusable native Vortex operators instead of adding
    ClickBench-only routes.
  - V1 scope classification: `required_for_v1` for local native Vortex OLAP readiness; not a
    public ClickBench submission, superiority claim, or production object-store proof.
  - ShardLoom technique review: apply dictionary/encoded execution, metadata-first pruning,
    capillary aggregate/state units, PulseWeave memory/spill/pressure diagnostics, dynamic
    admission by cardinality/selectivity/dtype, bounded top-K state, late materialization, and
    evidence-tier controls. The implementation must prefer Vortex-native array/scan/provider
    surfaces when available and otherwise implement ShardLoom kernels with explicit
    no-fallback evidence.
  - Execution checklist:
    - [x] Add a checked-in 100M UAT optimization intake artifact or validator that reads the
      combined local summary, preserves the timeout/>1s row list, and emits query-family targets
      without making a public benchmark claim:
      `docs/benchmarks/clickbench-100m-uat-burndown.json`.
    - [x] Implement the first shared native Vortex runtime optimization batch without adding
      ClickBench-only routes: typed exact distinct keys, source-order limited group output,
      capillary bounded-sort retention, direct UTF-8 contains counts for `COUNT WHERE`, residual
      materialization evidence, and functional-dependency group-key pruning for deterministic
      offset-derived group expressions.
    - [x] Resolve post-merge route/evidence correctness findings before the next rebuild or UAT
      pass so runtime optimizations cannot over-report native/no-decode posture:
      - [x] Certify metadata-only `count_all` as a native Vortex metadata route without requiring a
        scan/read/streaming flag, while preserving `data_read=false`, `data_decoded=false`, and
        no-fallback evidence.
      - [x] Keep UTF-8 `COUNT WHERE contains` in the no-decode fast path only for host
        `VarBinView` arrays; encoded/dictionary layouts must use encoded kernels or materialized
        evidence instead of silently executing to decoded UTF-8 while reporting no decode.
      - [x] Disable wide-output late materialization for sort rows when Vortex predicate pushdown
        already changed source ordinals, so selected ordinals cannot be applied to the unfiltered
        source.
      - [x] Reject grouped aggregate `HAVING`/`ORDER BY` references to missing output columns with a
        deterministic diagnostic instead of comparing JSON nulls.
      - [x] Route empty streaming columnar Vortex writes through the empty writer path so layout
        advisor/provider evidence stays consistent with the actual writer.
      - [x] Keep streaming public-preparation SourceState digests stable between prewrite layout
        planning and postwrite evidence while still recording observed row/batch counts.
      - [x] Validate the rewritten native route before public prepare-once run artifacts are
        created, so blocked routes cannot produce `.vortex` side effects before failing.
      - [x] Keep mixed-predicate `contains` residuals native after Vortex pushdown by combining
        Vortex `Filter` masks with encoded/FSST string-kernel matches, preserving compact filtered
        row ordinals and no-decode evidence for ClickBench-style URL search predicates.
      - [x] Preserve partitioned Vortex `count_all` as metadata-only capillary work: combine per-file
        Vortex row-count metadata without scan/read/decode/materialization and certify the native
        metadata I/O route.
    - [x] Re-verify the public-route invariant for every optimized row: compatibility inputs are
      source adapters only, `auto` and explicit native routes normalize into an admitted
      Vortex-prepared/native middle, direct local diagnostic paths remain internal safeguards, and
      no product route can report `sql-local-source-smoke`, `direct_compatibility_transient`,
      `fallback_attempted=true`, or `external_engine_invoked=true`.
      - Evidence: `python3 scripts/check_user_route_capability_report.py --output
        target/user-route-capability-report.json` reports zero stale public runtime labels and the
        route-reuse matrix preserves the shared Vortex-prepared/native spine.
    - [x] Re-run the route-invariant audit after the prepared OLAP query-time consumption path
      lands, proving the same optimized aggregate/string/top-K/distinct route is selected from
      CLI SQL, Python `ctx.sql(...)`, Python/DataFrame-style lazy methods, native `.vortex`, and
      prepared local compatibility inputs.
      - Evidence: `python3 scripts/check_sql_python_dataframe_parity.py --output
        target/sql-python-dataframe-parity.json` and
        `python3 scripts/check_clickbench_olap_runtime_coverage.py --output
        target/clickbench-olap-runtime-coverage.json` pass with all 43 ClickBench rows admitted and
        no fallback/external-engine invocation.
    - [x] Harden the local ClickBench UAT runner/parser before the next broad pass: strip SQL line
      and block comments before splitting statements so benchmark header comments cannot pollute
      `CB-Q01` or any other query text.
    - [x] Fix the full-43 UAT functional blockers from the current replacement-artifact pass:
      rerun `CB-Q01` with the checked-in comment-safe splitter and admit it through
      `native_vortex_count_all` in 0.850s, and narrow the materialized string-partial grouped
      aggregate path so numeric identity keys decline that string-only optimization instead of
      failing. Focused validation:
      `cargo test -p shardloom-vortex --features vortex-local-primitives --lib
      grouped_count_star_materialized_string_partials_decline_numeric_identity_keys -- --nocapture`.
      Local 100M rerun evidence: `CB-Q08` 0.073s and `CB-Q36` 8.267s, both successful
      `native_vortex_aggregate` rows with no fallback or external engine invocation.
    - [x] Preserve one shared runtime family across CLI, SQL, Python, and DataFrame-style wrappers:
      update lowering/evidence transport only when needed so aliases converge into the same
      Vortex-native aggregate, string-predicate, bounded-sort, distinct, and sink contracts.
      - Evidence: public route capability, SQL/Python/DataFrame parity, and ClickBench OLAP
        runtime coverage validators all report the same prepared/native Vortex runtime spine for
        equivalent admitted operations.
    - [ ] Implement high-cardinality integer and composite-key group-by/top-K improvements for
      `CB-Q16`, `CB-Q17`, `CB-Q18`, `CB-Q19`, `CB-Q31`, `CB-Q32`, `CB-Q33`, and `CB-Q36`: compact
      typed tuple keys, partitioned hash state where beneficial, bounded top-K heaps, source-order
      tie evidence, and state-budget/spill diagnostics.
      - [x] Add functional-dependency hash-key pruning for offset-derived integer group expressions
        such as `ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3`, while preserving full output
        group columns and no-fallback evidence.
      - [x] Add capillary ordered-candidate selection for grouped top-K/offset finalization so
        ordered aggregate routes retain only the required candidate window before final sort and
        row materialization.
      - [x] Add a direct count-star grouped update path for high-cardinality native Vortex aggregate
        routes, with typed primitive/string identity key extraction, count-state updates that bypass
        generic row-state evaluation, and `count_star_direct_group_update` evidence.
      - [x] Replace generic per-group aggregate-state cloning for count-only grouped routes with a
        compact count-star state slot and ordered-candidate comparator that compares integer counts
        without JSON value allocation.
      - [x] Add compact multi-key storage for high-cardinality count-only grouped routes
        (`UserID/SearchPhrase`, `UserID/minute/SearchPhrase`, `WatchID/ClientIP`) so the route
        stores and compares typed key tuples without materializing every output group value until
        the retained top-K/source-order window is known.
      - [x] Record compact group-state evidence: `compact_group_state_strategy`,
        `group_key_storage`, `topk_retention_after_update`, `materialized_group_value_count`, and
        state-byte/pressure estimates for the affected rows.
      - [x] Add compact count/sum/avg grouped state for high-cardinality numeric aggregate routes
        (`CB-Q32`, `CB-Q33`) so count/order aliases compare raw counts and numeric measures avoid
        generic per-group state cloning.
      - [x] Replace heap-backed per-row group keys for common one/two/three-key grouped routes with
        fixed-width typed keys so `CB-Q17`, `CB-Q19`, `CB-Q33`, `CB-Q34`, and `CB-Q35` avoid tuple
        `Vec` allocation in the hot update path.
      - [x] Tested and rejected a dictionary-code-bound `numeric + string` compact count top-K
        variant for `CB-Q17`/`CB-Q15`-class two-key routes. It activated
        `numeric_string_dictionary_code_direct_group_update` but regressed the current 100M
        `CB-Q17` local UAT from the latest full-run `52.277s` to `62.675s` while still carrying
        about `24M` candidate groups; the rejected evidence is recorded at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q17_numeric_string_probe.json`.
        Future work for this family must reduce state volume through partitioned/spill-backed
        merge, prepared grouping layout, or metadata-assisted pruning rather than adding another
        in-memory key spelling over the same rows.
      - [x] Elide source-order key retention for ordered grouped aggregate routes and compare
        retained candidates with typed key values instead of per-group string tie-breaker material,
        preserving deterministic ordering while reducing high-cardinality top-K memory pressure.
      - [x] Replace per-group compact numeric aggregate `Vec` allocation with inline measure slots
        for the common 1-4 measure case, targeting `CB-Q33` count/sum/avg high-cardinality groups
        without creating a query-specific route.
      - [x] Replace the grouped aggregate state's wide optional-field struct with a compact enum so
        count-only, compact numeric, and general states carry only the fields they actually need in
        high-cardinality maps.
      - [x] Add a typed numeric-pair exact aggregate state for `CB-Q33`-class routes with compact
        signed/unsigned integer key storage, a precompiled count/sum/avg measure update plan,
        streaming ordered top-K retention, and evidence fields
        (`grouped_aggregate_state+topk+compact_numeric_measures+numeric_pair`,
        `typed_numeric_pair_group_state`, `streaming_numeric_pair_topk_retention`). Targeted 100M
        UAT moved `CB-Q33` from timeout/barely-passing 177.746s to 120.573s in a one-shot probe
        and 128.962s in the current six-row follow-up, with no fallback or external engine
        invocation. Latest current-branch UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_recent_changes_20260621T084607/summary.json`
        regressed to 152.673s while observing 99,997,493 candidate groups; do not treat this as
        complete until prepared summary/partitioned merge work removes the near-unique full-state
        rebuild cost.
      - [x] Reject the per-update numeric-pair retained-candidate experiment after targeted 100M
        UAT timed out at the 180-second cap, proving it added hot-loop overhead for the near-unique
        `CB-Q33` state shape instead of reducing dominant work. Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_update_time_topk_20260622T042051Z/summary.json`.
      - [x] Reject fixed partitioned numeric-pair group maps after targeted 100M UAT regressed
        `CB-Q33` from the current-branch 149.269s to 175.602s:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_partitioned_numeric_pair_20260622T044156Z/summary.json`.
        The fixed partitions created useful evidence for future capillary/spill design but did not
        remove the dominant 99,997,493-group state volume, so the code path was removed before
        shipment.
      - [x] Add a direct single-numeric count top-K state for `CB-Q16`-class routes with typed
        numeric key storage, direct count updates, streaming retained-candidate selection, and
        evidence fields
        (`grouped_aggregate_state+topk+count_star_direct+compact_group_state+single_numeric`,
        `typed_single_numeric_group_state`, `streaming_single_numeric_topk_retention`). Targeted
        local 100M UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_archopt_retained_20260621T075312/summary.json`
        recorded `CB-Q16` at 2.965s with no fallback or external engine invocation.
      - [x] Extend the single-numeric and numeric-pair retained top-K paths with a capillary
        select-nth retention split for larger retained windows: small limits keep the existing
        streaming retained-candidate path, while larger `offset + limit` windows bulk-select the
        retained candidate boundary once and avoid repeated worst-candidate scans. Evidence now
        reports `topk_retention_strategy=select_nth_single_numeric_retained_window` or
        `select_nth_numeric_pair_retained_window` when that branch is used. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_single_numeric_large_retained_window_uses_select_nth_topk -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_pair_large_retained_window_uses_select_nth_topk -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_single_numeric_count_uses_streaming_topk_compact_state -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_pair_uses_streaming_topk_compact_state -- --nocapture`.
      - [x] Add source-order limited group admission for unordered `GROUP BY ... LIMIT K` shapes so
        admitted routes retain only the first source-order group window plus existing-key updates
        instead of building full high-cardinality state when SQL semantics do not request ordering.
        Targeted local 100M UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_archopt_retained_20260621T075312/summary.json`
        recorded `CB-Q18` at 9.265s, and evidence projection
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_evidence_projection_20260621T075629/summary.json`
        lifted `source_order_limited_group_admission_no_sort` and
        `first_k_source_order_groups_then_existing_key_updates` into public route fields.
      - [x] Add a capillary two-pass numeric-pair late-measure route for `CB-Q33`-class bounded
        ordered count/sum/avg aggregates: first build exact count state, retain the top-K
        numeric-pair keys, then reopen the same single `.vortex` artifact and materialize SUM/AVG
        measures only for retained keys. Targeted 100M UAT recorded exact result-row parity with
        the previous one-pass route while reducing `CB-Q33` to 14.860s from the current-branch
        149.269s / prior ship-drop 127.950s range:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_late_measure_20260622T065457Z/summary.json`.
        Evidence reports `capillary_two_pass_numeric_pair_count_topk_late_measures`,
        `numeric_pair_count_state_then_retained_measure_state`,
        `late_measure_count_state_retained_in_memory_second_pass_exact`,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
      - [x] Reuse the exact retained numeric-pair candidates prepared by the first pass and release
        the full high-cardinality count map before the second measure scan. The route no longer
        rescans the full candidate count state during result-summary/materialization and records
        `numeric_pair_prepared_retained_candidate_reuse`,
        `numeric_pair_retained_candidate_rescan_bypass`,
        `numeric_pair_count_state_release_before_second_pass`,
        `numeric_pair_count_state_memory_released_before_measure_scan`,
        `retained_candidate_selection_source=prepared_first_pass_retained_candidates`, and
        `retained_candidate_rescan_bypassed=true`, preserving exact ordering and no-fallback
        evidence while removing one full output-time pass over the candidate count state and
        reducing live state before the retained-measure pass. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_pair_late_measure_uses_count_topk_second_pass -- --nocapture`
        plus the adjacent numeric-pair family
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_pair -- --nocapture`
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
        Route-coverage validation:
        `python3 scripts/check_clickbench_olap_runtime_coverage.py --output target/clickbench-olap-runtime-coverage.json`
        passed with 43/43 admitted and 0 implementation-required rows.
      - [ ] Finish spill/state-budget hardening for exact high-cardinality aggregate families after
        retained two-pass or source-order strategies are exhausted. Do not reintroduce fixed
        partitions unless a measured approach reduces dominant work without regressing UAT.
        - [x] Harden shared direct grouped aggregate state budgeting by pre-reserving the chunk's
          direct-update capacity for generic grouped/count-distinct routes and surfacing capillary
          capacity-pressure evidence. This removes avoidable hash-map reallocation on admitted
          exact grouped distinct/general direct routes while keeping native spill fail-closed until
          a measured spill-backed merge path is worth retaining.
        - [x] Add centralized state-pressure classification to the shared native Vortex
          state-budget report and public workflow evidence. High-cardinality grouped routes now
          distinguish low/moderate/high/near-input-cardinality in-memory pressure, emit stable
          review diagnostics such as `SL_STATE_BUDGET_HIGH_PRESSURE_NATIVE_SPILL_PENDING` only
          when the observed exact state really warrants spill/partitioned-merge review, and still
          report `spill_required=false`, `spill_supported=false`, and no spill I/O until a real
          native spill-backed exact merge path exists. The ClickBench OLAP route-readiness
          validator now emits `shardloom.clickbench_olap_state_budget.v2` with
          `state_pressure_class_counts` so synthetic route-readiness evidence uses the same
          pressure vocabulary as runtime route output.
      - [x] Add a direct transformed-key builder for derived numeric/time keys in `CB-Q19` so
        `extract(minute FROM EventTime)` is computed into the typed key without intermediate
        `StatValue` construction, and pair it with high-cardinality triple-key state-budget
        diagnostics.
      - [x] Add a stronger exact triple-key aggregate strategy for `CB-Q19`: direct
        `UserID`/minute/`SearchPhrase` compact state stores a typed integer key, minute byte, and
        interned dictionary-backed search phrase ID, then performs streaming retained-candidate
        top-K and decodes strings only for surviving output rows. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_numeric_minute_string_uses_streaming_topk_compact_state`.
      - [x] Bind the `CB-Q19`-class string key to Vortex dictionary codes when the direct accessor
        exposes a dictionary, so the hot update path uses one prepared dictionary-to-interner map
        instead of looking up and hashing the UTF-8 value on every row. Evidence reports
        `typed_numeric_minute_dictionary_code_hash_key`,
        `numeric_minute_string_dictionary_code_direct_group_update`,
        `typed_numeric_minute_dictionary_code_key`, and the capillary work unit
        `dictionary_code_bound_numeric_minute_string_key`.
        Targeted local 100M UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_slow6_current_branch_20260622T033654Z/summary.json`
        recorded `CB-Q19` at 17.160s with no fallback or external engine invocation; retain the
        dictionary-code path, but keep the row open because the remaining cost still exceeds the
        sub-second target.
      - [x] Reuse prepared UTF-8 dictionary `Arc<str>` values for the numeric+UTF8 heavy-hitter
        key path instead of allocating a fresh string-backed `Arc` per observed row. This keeps the
        same exact single-artifact native Vortex route while pushing the public/runtime path closer
        to true dictionary-code execution. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8_dictionary_key_reuses_prepared_dictionary_arc -- --nocapture`.
      - [x] Carry numeric+UTF8 top-K exact recount counts as interned numeric/string-ID keys rather
        than string-backed composite keys, and expose the state-shape evidence in the route
        payload. This keeps the proof-bound heavy-hitter route exact while making the retained
        candidate path cheaper for `CB-Q33`/`UserID, SearchPhrase`-style groups. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib numeric_utf8 -- --nocapture`
        and
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Reserve string interner map/vector capacity from the prepared dictionary cardinality
        before numeric/minute/string compact-state hot updates. This keeps the retained
        dictionary-code route exact while reducing interner rehash/growth work under
        high-cardinality state pressure. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_minute_string_uses_streaming_topk_compact_state -- --nocapture`.
      - [x] Extend the fast-map candidate to materializing row-state operators that still used the
        default hasher: distinct row keys, duplicate masks, and drop-duplicate count/position maps
        now use the same `FxHashMap`/`FxHashSet` family as grouped aggregate state while output
        order remains governed by existing scan-order and first/last-position vectors. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib duplicate_mask_row_export -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib drop_duplicate_row_export -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib distinct_rows_ -- --nocapture`.
      - [ ] Evaluate the current exact hash-state fast-map candidate before retaining it for
        release: grouped aggregate general state, UTF-8 interning, materialized string partials,
        and direct UTF-8 dictionary builders now use `FxHashMap`, with deterministic output still
        controlled by existing sorted/source-order result paths. Focused correctness tests pass,
        but prior broad string hash-table changes had mixed UAT evidence, so targeted 100M
        ship/drop evidence is required before this is treated as a completed optimization.
    - [ ] Implement dictionary/string-aware group-by and exact distinct improvements for `CB-Q05`,
      `CB-Q06`, `CB-Q09`, `CB-Q10`, `CB-Q11`, `CB-Q12`, `CB-Q13`, `CB-Q14`, `CB-Q15`, `CB-Q22`,
      `CB-Q34`, and `CB-Q35`: group by dictionary/code IDs where available, keep distinct state
      encoded/compact where possible, and decode strings only for final output rows.
      - [x] Replace string-prefix distinct/group keys with typed hash keys for exact distinct and
        grouped state, preserving null/bool/int/uint/float/string type separation.
      - [x] Avoid full result sorting for grouped aggregate output when the SQL shape has no
        `ORDER BY`; source-order limited output now stops once the requested groups are emitted.
      - [x] Add a single-key grouped aggregate fast path for identity, UTF-8 length, and URL-domain
        transformed keys so common string-group profiles avoid generic row-key string formatting.
      - [x] Replace ordered row-key distinct/duplicate state with hash-backed state where output
        order is already scan-order controlled, preserving deterministic row output while reducing
        state-update cost for exact-distinct and duplicate-mask families.
      - [x] Add URL/string group interning or dictionary-code grouping for high-cardinality URL
        grouped routes (`CB-Q34`, `CB-Q35`) so repeated key storage avoids per-row owned string
        allocation and final string decode is limited to retained output groups.
      - [x] Promote URL grouping from interned string keys to exact dictionary/code grouping over
        the current Vortex layout: use Vortex dictionary codes when present, add chunk-local partial
        aggregation for materialized URL columns when dictionary codes are unavailable, and expose
        whether `chunk_dictionary_count_star_group_update` actually ran.
      - [x] Add transformed chunk-dictionary grouping for single-key URL-domain/length expressions
        so repeated URL transforms are computed once per dictionary value and counted by code before
        final group output.
      - [x] Add scalar direct exact count-distinct updates over typed primitive and UTF-8 dictionary
        Vortex chunks so unfiltered exact distinct can update set state without exporting rows.
      - [x] Rework the URL chunk-local path after the 100M probe showed
        `chunk_dictionary_count_star_group_update` did not activate on the current Vortex URL
        layout; the shared grouped aggregate path now operates on materialized UTF-8 chunks surfaced
        by the reader with exact chunk-local partial counts and separate evidence from dictionary
        code grouping. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_count_star_ -- --nocapture`.
	      - [x] Add generic streaming count-star top-K output for URL/string count-only groups, reducing
	        full candidate sorting/materialization while preserving exact ordering by count. Targeted
	        local 100M UAT retained the route for `CB-Q34`/`CB-Q35`; remaining work is deeper
	        prepared string/domain metadata to avoid the full runtime string decode.
	      - [x] Add proof-bound string count top-K heavy-hitter recount for large exact count-only
	        string groups: first-pass weighted candidate sketch over chunk UTF-8 dictionaries,
	        lower-bound proof before the second pass, candidate-only exact recount on the same single
	        `.vortex` artifact, and existing exact native Vortex dictionary fallback when proof is not
	        possible. Targeted 100M UAT retained exact row parity and moved `CB-Q34` to 33.958s and
	        `CB-Q35` to 34.064s with `fallback_attempted=false` and `external_engine_invoked=false`.
      - [x] Replace string and string-count-distinct top-K heavy-hitter first-pass sketch keys with
        route-local interner IDs rather than retained `Arc<str>` keys, and apply the same
        dictionary-bound key model to numeric+UTF8 top-K candidates. This keeps chunk dictionary
        codes local, uses ShardLoom's route-local interner as the stable cross-chunk key space, and
        expands strings only at exact-proof/recount/output boundaries. Evidence fields now report
        `string_count_topk_dictionary_code_reuse`,
        `string_count_distinct_topk_dictionary_code_reuse`,
        `numeric_utf8_topk_dictionary_code_reuse`, and dictionary-code capillary work units.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_skips_recount_when_first_pass_is_exact -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_skips_recount_when_retained_exact_boundary_is_proved -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_numeric_utf8_count_topk_uses_proofbound_recount -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Carry count-only string top-K retained candidates and exact recount counts as
        route-local interner IDs through the second pass, not `Arc<str>` keys. The exact recount now
        maps chunk dictionary codes to route IDs once, updates `u64 -> count` state, and expands
        strings only for retained output ordering/materialization. Evidence reports
        `string_count_topk_candidate_id_prefilter` plus
        `string_topk_candidate_id_prefilter`, reducing string hashing/cloning in the
        `CB-Q34`/`CB-Q35` family without adding sidecars, query-answer caches, or facade-specific
        routes. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_skips_recount_when_first_pass_is_exact -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_skips_recount_when_retained_exact_boundary_is_proved -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Extend the proof-bound string count top-K route to mixed retained measures such as
        `MIN(URL)` and `COUNT(DISTINCT UserID)` without building full group state for every string
        key. The first pass still tracks dictionary/interner IDs and count lower bounds; the second
        pass reapplies exact ShardLoom residual row filters, updates exact counts, and materializes
        full aggregate state only for candidate IDs that survived the proof set. Evidence reports
        `proofbound_heavy_hitter_string_count_topk_late_measure_recount`,
        `string_dictionary_code_count_topk_late_measure_recount`,
        `string_dictionary_code_heavy_hitter_candidate_late_measure_group_state`,
        `proofbound_candidate_late_measure_exact`, and
        `string_topk_candidate_late_measure_recount`. This targets `CB-Q22`/`CB-Q23`-class
        filtered phrase groups with late URL/distinct measures through the shared native Vortex
        aggregate route, not a ClickBench-only route or query-answer sidecar.
      - [x] Add a chunk-local dictionary-code candidate prefilter inside the same mixed
        late-measure route: retained candidate IDs are converted once to dictionary-code flags for
        the second pass, so retained `MIN(URL)` / `COUNT(DISTINCT UserID)` measure updates avoid a
        per-row candidate hash lookup while preserving exact residual row filtering and candidate
        recount semantics. Evidence reports `string_count_topk_candidate_code_prefilter` plus the
        `string_topk_candidate_dictionary_code_prefilter` capillary unit; this is focused-runtime
        evidence only until the end-of-batch UAT rerun. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_late_measures_use_candidate_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_skips_recount_when_first_pass_is_exact -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib heavy_hitter_routes_admit_pushdown_only_predicates -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Carry string count-distinct top-K retained candidates and exact distinct sets as
        route-local interner IDs through the second pass, not `Arc<str>` group keys. The route still
        stores exact distinct values for each retained group, but the group side of the exact set is
        `u64 -> distinct_value_set`, and final string materialization happens only after retained
        candidate ordering. Evidence reports `string_count_distinct_topk_candidate_id_prefilter`
        and `string_count_distinct_candidate_id_prefilter`, preserving exact no-fallback semantics
        while reducing string-key hashing in the `CB-Q14` family.
      - [x] Add the same chunk-local dictionary-code candidate prefilter to string
        count-distinct top-K exact recount: candidate IDs are converted once to dictionary-code
        flags, so exact distinct updates skip non-candidate rows without a per-row candidate hash
        lookup while preserving exact distinct sets and no-fallback semantics. Evidence reports
        `string_count_distinct_topk_candidate_code_prefilter` plus
        `string_count_distinct_candidate_dictionary_code_prefilter`; this remains focused-runtime
        evidence until the end-of-batch UAT rerun. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_distinct_topk_uses_proofbound_recount -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
    - [ ] Implement faster string predicate and URL expression kernels for `CB-Q21`, `CB-Q22`,
      `CB-Q23`, `CB-Q24`, `CB-Q28`, and `CB-Q29`: exact prepared literal membership indexes for
      `LIKE '%literal%'`, shared positive/negative string predicate masks, prepared string length
      metadata where exact, and a specialized URL host extraction/index path for the ClickBench
      regex-domain shape.
      - [x] Count-only UTF-8 substring predicates now count directly from Vortex `VarBinViewArray`
        bytes when the column is non-null UTF-8, avoiding row export/materialization and reporting
        `data_decoded=false` / `data_materialized=false`; nullable or unsupported shapes still use
        deterministic ShardLoom-owned materialized evaluation.
      - [x] Split mixed predicates into Vortex-pushable and ShardLoom residual conjuncts so safe
        filters still execute in the native Vortex scan while UTF-8 residual work remains explicit,
        audited, and no-fallback.
      - [x] Keep ClickBench string expression transforms (`length`, URL-domain extraction, minute
        extraction/truncation, and scoped CASE expression grouping) inside the shared native
        aggregate transform registry rather than a parallel benchmark-only evaluator.
      - [x] Admit exact `length`-transformed sum/avg measures into compact grouped aggregate state
        so `AVG(length(URL))`-style OLAP routes can use direct compact count/sum/avg slots instead
        of generic per-group aggregate-state clones. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_applies_expression_groups_value_transforms_and_having_without_fallback`.
        Targeted 100M UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_recent_changes_20260621T084607/summary.json`
        recorded `CB-Q28` at 13.634s, which is not a meaningful improvement over the prior slow
        class; keep the correctness/general-state work only if it remains clean, but performance
        completion requires prepared URL-length metadata or exact string-layout indexes that avoid
        the full runtime string scan.
      - [x] Add capillary selected-row candidate masks for sparse UTF-8 residual predicates, including
        `AND` predicates with a fast string child, so aggregate and sort routes materialize only
        candidate rows before final predicate evaluation.
      - [x] Route file-scan FSST UTF-8 contains predicates through Vortex's FSST DFA `LIKE` kernel for
        count and selected-row masks, with escaped literal patterns and nullable/unsupported shapes
        still falling back to explicit materialized ShardLoom evaluation evidence.
      - [x] Add a non-null UTF-8 dictionary row-selection fast path for contains and IN-list
        residual filters. Prepared Vortex dictionaries now convert dictionary-value match flags
        directly to row-index masks when row/value nulls are absent, while nullable dictionaries
        keep the exact existing null-aware path. This reduces per-row null/check dispatch for
        shared SQL/Python/DataFrame residual predicate routes and remains focused-runtime evidence
        until the end-of-batch UAT rerun. Focused validation is the local primitive string
        predicate test family plus the later end-of-batch UAT; no broad rerun was performed here by
        design.
      - [x] Avoid owned `String` allocation in UTF-8 dictionary IN-list match flags when the RHS is
        a string/null literal list. Dictionary values are compared as borrowed `&str` values; mixed
        typed lists keep the existing coercion path.
      - [x] Remove the extra dictionary-frequency pass from UTF-8 comparison row-index selection.
        `SearchPhrase != ''` and similar residual filters now build dictionary value match flags
        once, then emit row-index masks directly over dictionary codes; count-only comparison still
        uses value counts. This targets shared string predicate work in `CB-Q21`-`CB-Q23` and keeps
        nullable semantics on the existing null-aware branch.
      - [x] Avoid owned `String` allocation while building UTF-8 dictionary comparison match flags
        for common string-literal predicates. Dictionary value flags now compare borrowed `&str`
        values directly for `Eq`/`NotEq`/range comparisons and reserve the generic `StatValue`
        coercion path for non-string literals.
      - [x] Add metadata-first UTF-8 dictionary null-predicate shortcuts. When value and row nulls
        are absent, `IS NULL`/`IS NOT NULL` returns from dictionary null posture without scanning
        row codes; nullable dictionaries still use the exact existing per-row null checks.
      - [x] Add metadata-first UTF-8 dictionary all/none count shortcuts for count-only contains,
        comparison, and `IN`/`NOT IN` predicates. Non-null dictionaries now build dictionary-value
        match flags first and return immediately when those flags prove no row or every row can
        match; mixed cases still scan row codes once, and nullable dictionaries keep the existing
        exact null-aware frequency path. This targets shared SQL/Python/DataFrame string predicate
        families without adding query sidecars, answer caches, or facade-specific routes. Focused
        validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib non_null_utf8_dictionary_match_count_shortcuts_preserve_masks -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_reuses_chunk_dictionary_counts -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_masked_dictionary_count_reuses_value_counts -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_utf8_dictionary_counts_dictionary_values_once -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_utf8_dictionary_preserves_null_semantics -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_counts_dictionary_values_once -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_preserves_null_and_negated_semantics -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo fmt --all -- --check`. No broad UAT rerun was performed here; save it for the
        end-of-batch pass.
      - [x] Apply the same metadata-first all/none proof to non-null UTF-8 dictionary row-index
        selection, including the IN-list-specific helper. Count/filter/export routes now return an
        empty selection or full row/masked-row index range directly when dictionary match flags
        prove all rows or no rows match, instead of scanning dictionary codes only to discover the
        same result. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib non_null_utf8_dictionary_match_count_shortcuts_preserve_masks -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_counts_dictionary_values_once -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo fmt --all -- --check`. End-of-batch UAT remains deferred by design.
      - [x] Extend the all/none row-index proof to nullable UTF-8 dictionary comparison predicates
        where exact row-null semantics are still preserved: if dictionary value flags prove no
        value can match, residual candidate selection returns an empty row set immediately; if all
        dictionary values match, the route emits the exact non-null row indices from dictionary
        null posture instead of scanning row codes and match flags. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_utf8_dictionary_preserves_null_semantics -- --nocapture`.
      - [x] Extend the same all/none proof to nullable UTF-8 dictionary `IN`/`NOT IN` row-index
        predicates. The shared proof maps dictionary-value selection plus null-selection posture to
        exact all-row, empty-row, null-row, or non-null-row selections before scanning row codes;
        mixed dictionary selections keep the existing null-aware exact row loop. This preserves
        SQL/Python/DataFrame null semantics while reducing residual predicate work for shared
        prepared Vortex string filters. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_preserves_null_and_negated_semantics -- --nocapture`.
      - [x] Add a shared non-null dictionary frequency fast path beneath predicate and aggregate
        helpers. When dictionary values and row codes are already proven non-null, exact frequency
        builders now count dictionary codes directly for full, selected-row, and masked scans
        instead of calling the null-aware row checker for every row; nullable dictionaries keep the
        existing exact path. This composes with count-only string predicates, comparison/`IN`
        predicates, chunk-dictionary grouping, and exact aggregate updates without adding a
        parallel route. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib non_null_dictionary_frequency_counts_use_direct_code_path -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_reuses_chunk_dictionary_counts -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_masked_dictionary_count_reuses_value_counts -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_compare_utf8_dictionary_counts_dictionary_values_once -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_counts_dictionary_values_once -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_in_list_count_utf8_dictionary_preserves_null_and_negated_semantics -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo fmt --all -- --check`. End-of-batch UAT will decide whether this exact
        hash/dictionary-state candidate is retained for release performance claims.
      - [x] Add a generic mask-first contains helper for Vortex `Filter` arrays so admitted mixed
        predicates can scan only selected rows for host UTF-8 and dictionary UTF-8 children before
        falling back to the older child-scan/intersection path. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib contains -- --nocapture`.
        Targeted 100M UAT kept the route correct with no fallback/external engine and no meaningful
        regression: `CB-Q21` ran in `13.264s` and `CB-Q22` ran in `15.891s`
        (`/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q21_maskfirst_20260622T051302Z/summary.json`,
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_maskfirst_20260622T051345Z/summary.json`).
        This is retained as a reusable filtered-array data-work reduction, not as completion of the
        string predicate family; the dominant cost still requires prepared string/domain metadata
        or exact segment/block pruning.
      - [x] Add the retained count-only chunk-dictionary contains path for `count_where` so
        string count predicates can reuse the same Vortex/ShardLoom chunk UTF-8 dictionary accessor
        family as grouped aggregate routes instead of materializing full residual rows. Targeted
        100M local UAT over the current single `.vortex` artifact moved `CB-Q21` from the fresh
        full-run `16.442s` to `12.166s` with `data_decoded=false` and
        `data_materialized=false`, while the adjacent filtered aggregate/sort rows stayed usable
        (`CB-Q22` 8.212s, `CB-Q24` 19.330s) in
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_route_family_string_count_only_20260622T170000Z/summary.json`.
        The broader row-index chunk-dictionary contains variant was tested and removed because it
        regressed `CB-Q22`/`CB-Q24`; keep this optimization count-only until prepared predicate
        row indexes or exact segment/block pruning exist.
      - [x] Add a masked FSST contains helper for filtered string predicates. Vortex `Filter`
        arrays over FSST UTF-8 children now consume the FSST LIKE result directly with the filter
        mask for count and row-index selection, avoiding the older child row-index allocation plus
        mask-intersection path when the upstream FSST kernel is available. This remains a shared
        string-predicate optimization for SQL/Python/DataFrame routes, not a benchmark-specific
        path. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_count_only_admits_host_varbinview_without_decode -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_skips_nulls_without_materialized_string_fallback -- --nocapture`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo fmt --all -- --check`; end-of-batch UAT must confirm whether the current
        prepared artifact exposes this FSST/filter shape in slow ClickBench rows.
      - [x] Replace UTF-8 dictionary `contains` count and selected-row accessors with
        dictionary-value match flags plus masked code counts, so mixed predicate rows that already
        produced a filter mask do not re-run substring matching per selected row. This is a shared
        operator improvement for filtered string-count/sort families and preserves the existing
        materialization boundary until prepared row-position metadata exists. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_masked_dictionary_count_reuses_value_counts -- --nocapture` and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib filtered_utf8_contains_uses_mask_first_host_and_dictionary_paths -- --nocapture`,
        plus nullable/negated dictionary coverage in
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_skips_nulls_without_materialized_string_fallback -- --nocapture`.
      - [x] Add an exact ASCII-literal byte-search path for UTF-8 contains predicates over host
        string chunks and dictionary values. Non-ASCII needles keep the existing UTF-8 string
        semantics; ASCII `LIKE '%literal%'` predicates use `memchr` byte search over already-valid
        Vortex UTF-8 buffers and preserve the same no-fallback route boundary. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_string_contains_uses_native_utf8_count_without_fallback -- --nocapture`,
        and
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib mixed_predicate_count_pushes_safe_conjunct_and_keeps_utf8_residual_unmaterialized -- --nocapture`.
        Targeted local 100M UAT over the retained single `.vortex` artifact moved `CB-Q21` from
        `13.158s` to `12.564s`, `CB-Q22` from `10.118s` to `8.969s`, and `CB-Q23` from `18.267s`
        to `15.470s`; `CB-Q24` remained in its noisy row-ref materialization range at `22.256s`.
        Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_ascii_contains_byte_search_20260622T_now/summary.json`
        and
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_ascii_contains_sort_q24_20260622T_now/summary.json`.
      - [x] Promote nullable string predicate rows into the same encoded/direct string path:
        nullable host UTF-8, nullable Vortex dictionary values/codes, and filtered dictionary
        children now skip nulls under both positive and negated contains semantics; true Vortex
        dictionary children also return filtered-output row references without row-string fallback.
        The previously rejected chunk-local row-index fallback remains disabled until prepared
        row-position metadata can make it cheaper than the existing materialization boundary. This
        keeps URL/search predicates aligned with the null-aware aggregate dictionary contract above.
        Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains_skips_nulls_without_materialized_string_fallback -- --nocapture`.
      - [x] Reuse a compiled ASCII substring matcher across host UTF-8, masked UTF-8, dictionary, and
        chunk-dictionary value loops instead of rebuilding `memmem` search state per row/value.
        Split all-valid/all-invalid/nullable validity lanes so non-null host and masked UTF-8 scans
        avoid per-row validity branching while null rows still skip under both positive and negated
        contains semantics. Empty, negated, and non-ASCII semantics remain exact; non-ASCII bytes
        still fail closed to the materialized fallback boundary where required. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib utf8_contains_matcher_preserves_ascii_unicode_negated_and_empty_semantics -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib fast_utf8_contains -- --nocapture`,
        and `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`.
      - [x] Add exact residual candidate narrowing for comparison, null, and `IN`-list predicates
        over typed, dictionary, and materialized single-column Vortex accessors, then feed exact
        candidates directly into scalar/grouped aggregate accessors when every residual predicate
        child is exact. This avoids selected-row export/re-check for admitted equality/range,
        `IN`/`.isin()`, and null-filtered aggregate families while preserving advisory candidate
        materialization for mixed residual predicates that still need a full ShardLoom predicate
        check. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib count_where_in_list -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib grouped_aggregate_in_list -- --nocapture`,
        and `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib residual_predicate_exact_candidates -- --nocapture`.
        Targeted local 100M UAT retained the change for the filtered date/counter group/top-K
        family:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_filtered_family_current_20260622T054637Z/summary.json`
        recorded `CB-Q37` 0.261s, `CB-Q38` 0.116s, `CB-Q39` 0.121s, `CB-Q40` 0.841s,
        `CB-Q41` 0.065s, `CB-Q42` 0.041s, and `CB-Q43` 0.054s, all on native Vortex routes with
        no fallback or external engine invocation.
      - [x] Reject the FSST count-only bool-mask counting micro-optimization after targeted local
        100M UAT regressed `CB-Q21` from the prior 13.370s full-43 baseline to 14.310s:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q21_fsst_count_mask_20260622T043513Z/summary.json`.
        The next string-predicate work must reduce full string/layout scanning through prepared
        string/domain metadata or exact segment/block pruning rather than optimizing the final bool
        mask count.
    - [ ] Implement bounded sort/materialization improvements for `CB-Q24`, `CB-Q25`, `CB-Q26`,
      `CB-Q27`, `CB-Q40`, and `CB-Q41`: top-N/offset heaps, projection-aware row materialization,
      late payload decode, and deterministic ordering/tie fields.
      - [ ] Replace wide-output top-N string-scan/payload-reread work with prepared predicate row
        indexes and row-position payload locality; the current capillary select-nth retention
        window remains a diagnostic baseline and is not sufficient for `sub_1s_query_time`.
      - [x] Replace bounded sort/top-K periodic full-sort truncation with capillary select-nth
        retention windows and final deterministic sort over the retained candidate set.
      - [x] Replace generic count-star grouped top-K's linear retained-window scan with dynamic
        capillary select-nth retention when `offset + limit > 128`, preserving the small-`LIMIT`
        streaming window while avoiding `O(groups x offset)` comparisons for offset-heavy rows.
        Targeted 100M UAT moved `CB-Q40` from a post-candidate 2.88s probe to 0.841s with
        `group_output_strategy=capillary_select_nth_count_star_topk`.
      - [x] Preserve original scan ordinals while using selected-row predicate masks for bounded
        sort/top-N, so wide-output late materialization stays correct without full payload decode.
      - [x] Apply embedded derived-column predicate rewrites before bounded sort/top-K candidate
        scans so exact non-empty string filters can use persisted length metadata while hidden
        `__shardloom_derived_*` fields stay out of user output. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_rewrites_non_empty_string_predicate_to_embedded_length_column -- --nocapture`
        and `cargo test -q -p shardloom-cli --bin shardloom local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`.
      - [x] Apply the same embedded rewrite path to filter, sampled, distinct, and drop-duplicate
        row-export scans so materializing compatibility outputs can use metadata-first filtering
        before row decode/write when the predicate is exact and pushdown-safe; residual predicates
        use ShardLoom selected-row filtering before deterministic sampling or exact row-key state,
        and nested/list row keys remain encoded row-key state instead of being decoded as output
        columns.
      - [x] For wide-output top-N with predicates, keep the first pass narrow by evaluating the
        predicate as a ShardLoom residual over predicate/order columns, preserve source ordinals, and
        reopen only retained rows for final payload materialization; apply the same strategy to
        partitioned local Vortex sources.
      - [x] Extend the row-reference materialization policy to large bounded payload projections
        even when the projection is not "wide" by column count, while preserving Vortex filter
        pushdown for fully pushable predicates via the same filtered source-ordinal stream. Evidence
        reports `late_output_materialization`, `row_ref_topk_materialization_policy`,
        `late_materialization_payload_columns`, and `late_materialization_retained_cap`.
      - [x] Promote root-source final-K payload materialization to upstream Vortex row-index
        selection for safe row-ref paths, so the second pass asks Vortex for only retained source
        rows instead of iterating prefix chunks and discarding them in ShardLoom. Pure
        pushdown-filtered first passes now project Vortex `row_idx()` as hidden candidate metadata,
        allowing filtered top-K to preserve root row IDs without exposing internal columns. Evidence reports
        `late_materialization_row_index_selection_applied`,
        `late_materialization_requested_row_indices`, min/max selected source ordinals, and
        `late_materialization_source_row_id_projection_applied` through the public route evidence
        lift, plus state-budget work units `vortex_row_idx_projection` and
        `row_index_selected_payload_scan`. Focused validation:
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_late_materialization_policy_uses_row_refs_for_large_payload_topk -- --nocapture`,
        `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows_wide_projection_with_pushdown_keeps_filtered_ordinals -- --nocapture`,
        `cargo test -q -p shardloom-cli --features release-user-surfaces --bin shardloom local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`,
        `cargo fmt --all -- --check`,
        `cargo clippy -q -p shardloom-vortex --features vortex-local-primitives --lib -- -D warnings`,
        and `cargo clippy -q -p shardloom-cli --features release-user-surfaces --bin shardloom -- -D warnings`.
    - [ ] Apply PulseWeave work shaping in the optimized routes: record `FlowInventory`-style
      source/execution/writer work, `ScarcityLedger` memory/decode/sink pressure, `EndoPulse`
      run-local feedback, and `ProofBound` evidence so adaptive behavior remains certificate-gated.
      - [x] Added PulseWeave/ScarcityLedger-style pressure signals for row-reference top-K
        materialization, compact grouped state, chunk-local partial grouping, direct scalar
        aggregate updates, and repeated numeric expression fusion.
    - [ ] Apply capillary work-unit semantics where the scan/operator can split or coalesce work:
      record source range, projected columns, filter mask posture, target artifact/output boundary,
      materialization posture, retry/idempotency state, sink pressure, memory pressure, and
      no-fallback evidence for ingest/prep/execution/output units.
      - [x] Record capillary retention and ordered-candidate work units for bounded sort and grouped
        ordered aggregate routes, including retained candidate counts and pressure signals.
      - [x] Record direct count-star grouped-update and wide-output second-pass work-shaping
        evidence so local UAT rows can distinguish rows scanned, rows selected, and rows actually
        materialized for output.
      - [x] Preserve direct non-null Vortex dictionary/run-end kernel admission while blocking
        nullable encoded layouts from being silently decoded into reader-generated kernel inputs.
      - [x] Record capillary work units for repeated numeric expression reuse and row-reference
        final-K materialization: `expression_plan_fingerprint_reuse`, `row_ref_candidate_scan`, and
        `final_retained_row_materialization`.
    - [ ] Apply dynamic work shaping without route proliferation: coalesce small units when
      scheduling overhead dominates, split large high-cardinality/string units when state/decode
      pressure requires it, and expose the chosen unit sizing and admission reason in evidence.
      - [x] Added dynamic row-reference top-K admission based on input rows, retained cap, and
        payload projection shape, plus numeric expression fusion only when two or more compatible
        measures share the same accessor.
    - [ ] Apply metadata-first and late-materialized execution before row decode: statistics/pruning
      checks before row reads where supported, encoded/dictionary kernels before string decode,
      and explicit bounded decode/materialization evidence at collect or compatibility-write
      boundaries.
      - [x] Added runtime row-reference final-K materialization and embedded Vortex layout/stat
        consumption fields so bounded top-K routes can report exactly where decode/materialization
        occurs.
      - [x] Added metadata-pruned compatibility-write output for filtered materializing row
        exports: impossible predicates proven by Vortex footer statistics produce an empty
        requested sink without scan/decode/materialization, row-key state, or sampling candidate
        work, while preserving atomic write and no-fallback evidence.
      - [x] Added metadata-pruned structured sink output for schema-known expression-project
        exports: impossible predicates can write empty Parquet/Arrow IPC/Avro/native Vortex outputs
        without scan/decode/materialization, while preserving schema from the single `.vortex`
        artifact and no-fallback evidence.
      - [x] Added metadata-pruned expression-project collect and source-filter-before-rewrite
        semantics: source predicates share the same embedded layout/footer-pruning plan as row
        export, impossible predicates return without scan/decode/materialization, and collect/write
        residual source-string predicates are evaluated before replacement/mask/row-number
        transforms.
    - [x] Preserve timing-surface discipline in route output and refreshed artifacts: hot runtime,
      replay proof, and publication proof remain separate, and no evidence render/result-sink work
      is folded into a query-runtime claim.
      - [x] Added explicit timing-surface/evidence-tier fields to public workflow and `vortex-run`
        route output without introducing benchmark timings: successful runtime routes report
        `hot_runtime`/`metadata_sink`, blocked/inspection rows report no-timing route-readiness,
        route-total timing is explicitly not reported, and sink/render timing is never included in
        a query-runtime total. The lower `vortex-run` surface now also lifts
        `shardloom.local_vortex_state_budget.v2` with `local_primitive_state_pressure_class`,
        `local_primitive_budget_scope`, `local_primitive_spill_io_performed`, and
        `local_primitive_state_budget_next_action`, keeping runtime pressure diagnostics aligned
        with public workflow output and the ClickBench route-readiness validator. Focused
        validation:
        `cargo test -q -p shardloom-cli --features release-user-surfaces --test public_workflow_route public_run_native_vortex_aggregate_emits_state_budget_and_pulseweave_evidence -- --nocapture`,
        `cargo test -q -p shardloom-cli --features vortex-local-primitives --bin shardloom vortex_run_timing_surface_fields_stay_hot_runtime_only -- --nocapture`,
        `cargo clippy -q -p shardloom-cli --features release-user-surfaces --test public_workflow_route -- -D warnings`,
        `cargo clippy -q -p shardloom-cli --features vortex-local-primitives --bin shardloom -- -D warnings`,
        and `python3 scripts/check_clickbench_olap_runtime_coverage.py --output target/clickbench-olap-runtime-coverage.json`.
    - [x] Add focused correctness fixtures for the changed operator families: exact distinct
      null/drop semantics, duplicate/high-cardinality keys, string predicate case behavior,
      URL/regex replacement equivalence for the admitted profile, top-K/offset ties, empty/all-null
      groups, and decoded-reference parity.
      - [x] Add focused fixtures for typed distinct key separation, functional-dependency group-key
        pruning, source-order group output/source-order group admission, direct UTF-8 contains
        count, bounded-sort diagnostics, and residual materialization evidence.
      - [x] Add mixed predicate fixtures proving Vortex filter pushdown is preserved for safe
        conjuncts while UTF-8 residual predicates stay ShardLoom-owned and evidence-backed.
      - [x] Add/regression-fix fixtures for nullable dictionary/run-end encoded input blocking,
        bounded row-export observed-count semantics, and metadata-only count evidence.
      - [x] Add focused fixtures for direct count-star grouped updates, wide-output sort predicate
        source-ordinal preservation, and partitioned wide-output second-pass materialization.
      - [x] Add focused fixtures for numeric/minute/string grouped top-K state, transformed
        dictionary URL-domain grouping, scalar direct exact distinct, and capillary select-nth
        bounded sort retention.
      - [x] Add focused fixture coverage for dictionary-code-bound numeric/minute/string grouped
        top-K state so a Vortex dictionary accessor cannot regress to per-row string lookup/hash in
        the compact triple-key route.
      - [x] Add focused fixture coverage proving numeric+UTF8 top-K keys clone the prepared
        dictionary value `Arc<str>` instead of constructing row-local string allocations.
      - [x] Add focused fixtures for materialized URL-domain chunk-local partial grouping,
        direct/materialized numeric expression fusion, exact dictionary distinct over used codes,
        and row-reference final-K materialization state-budget evidence.
      - [x] Add focused fixtures for metadata-pruned expression-project collect and residual
        source-string filtering before expression rewrites in both collect and row-export paths.
      - [x] Add focused fixture coverage for scalar UTF-8 dictionary length aggregate reuse so
        repeated string-derived scalar measures cannot regress to per-row transform evaluation.
      - [x] Add focused fixture coverage for count-only UTF-8 contains over chunk dictionaries so
        the count route can use dictionary value/count pairs without activating the slower
        row-index path for filtered aggregate or sort routes.
      - [x] Add focused fixture coverage for proof-bound string count-distinct top-K routes,
        including candidate sketch admission, exact candidate recount, proof status, retained row
        ordering, and public evidence-field lifting.
      - [x] Add focused fixture coverage for nullable Vortex and chunk-local UTF-8 dictionary
        aggregate semantics, proving the route stays `vortex_utf8_dictionary` or
        `chunk_utf8_dictionary`, avoids materialized accessors/blockers, and emits an exact null
        group key.
    - [ ] Rerun targeted local 100M UAT for the affected timeout rows (`CB-Q17`, `CB-Q18`,
      `CB-Q19`, `CB-Q33`, `CB-Q34`, `CB-Q35`) under the 180-second cap, then rerun the full
      43-query native Vortex UAT only after targeted rows no longer timeout or regress.
      Replace or reuse the existing local prepared Vortex artifact in the Desktop UAT folder rather
      than creating duplicate massive files, and record whether each row used embedded
      layout/statistics pruning, plain prepared Vortex scan state, or raw native Vortex scan state.
      - [x] Current-branch targeted local UAT for the six affected rows completed with zero
        timeouts and zero fallback/external-engine violations:
        `CB-Q17` 2.026s, `CB-Q18` 11.109s, `CB-Q19` 17.160s, `CB-Q33` 149.269s, `CB-Q34`
        92.982s, and `CB-Q35` 100.037s in
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_slow6_current_branch_20260622T033654Z/summary.json`.
        This proves route stability but not performance completion for `CB-Q18`, `CB-Q19`,
        `CB-Q33`, `CB-Q34`, or `CB-Q35`.
      - [x] Targeted `CB-Q33` rerun after the two-pass numeric-pair late-measure route completed in
        14.860s with exact result parity to the prior route and no fallback/external-engine
        invocation:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_late_measure_20260622T065457Z/summary.json`.
      - [x] Full sequential 43-query local UAT over the current single `.vortex` artifact completed
        with 43/43 successes, zero timeouts, and zero fallback/external-engine violations after the
        grouped count-distinct compact-admission guard fix:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_current_branch_bounded_after_guardfix_20260622T164414Z/summary.json`.
        Route-family baseline from that run:
        `native_vortex_count_all` (`CB-Q01`), `native_vortex_count_where` (`CB-Q02`, `CB-Q21`),
        `native_vortex_filter_project` (`CB-Q20`), `native_vortex_sort_rows`
        (`CB-Q24`-`CB-Q27`), scalar/direct aggregate (`CB-Q03`-`CB-Q07`, `CB-Q30`), generic
        grouped row-state (`CB-Q09`-`CB-Q12`, `CB-Q14`, `CB-Q22`, `CB-Q23`), compact count-star
        grouped routes (`CB-Q08`, `CB-Q15`, `CB-Q17`, `CB-Q18`, `CB-Q36`, `CB-Q40`-`CB-Q43`),
        chunk-dictionary count-star (`CB-Q13`, `CB-Q37`-`CB-Q39`), numeric-pair compact
        (`CB-Q31`, `CB-Q32`), numeric-pair late-measure (`CB-Q33`), numeric-minute-string
        (`CB-Q19`), transformed dictionary general-measure (`CB-Q29`), and string heavy-hitter
        (`CB-Q34`, `CB-Q35`).
      - [x] Before returning to individual slow-lane work, run a route-family embedded-Vortex pass:
        metadata-only count already uses file row-count metadata; count-where now has the retained
        count-only chunk-dictionary contains path; filter/project is already sub-100ms over native
        Vortex; sort rows already uses row-reference late materialization and rejected the slower
        row-index contains variant; scalar/direct aggregate already uses direct primitive/dictionary
        updates; transformed dictionary general-measure now uses dictionary value/count pairs; and
        compact grouped routes already expose capillary/selection-vector/embedded-layout evidence.
        Remaining heavy routes should now resume as deeper slow-lane work rather than route
        availability work: generic grouped count-distinct/string row-state, high-cardinality
        compact count-star state, string heavy-hitter recount, and exact predicate row-index
        metadata.
      - [x] Rerun the full sequential 43-query native Vortex UAT after the route-family pass and
        numeric+UTF8 proof-bound route, using max parallelism 2 and the same retained single
        `.vortex` artifact. Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_embedded_layout_route_pass_fixed_20260622T_now/summary.json`.
        Result: 43/43 success, zero timeouts, zero fallback violations. Every public ClickBench
        route family has now had at least one applicable embedded-layout/native-operator
        optimization pass before returning to slower lanes: metadata count (`CB-Q01`), string
        count-where (`CB-Q21`), filter/project (`CB-Q20`), sort/top-K (`CB-Q24`-`CB-Q27`),
        scalar aggregate (`CB-Q03`-`CB-Q07`, `CB-Q30`), exact/grouped row-state
        (`CB-Q09`-`CB-Q14`, `CB-Q22`, `CB-Q23`), compact count-star grouped routes
        (`CB-Q08`, `CB-Q15`-`CB-Q19`, `CB-Q36`, `CB-Q40`-`CB-Q43`), numeric pair compact/late
        measure (`CB-Q31`-`CB-Q33`), transformed dictionary general measure (`CB-Q29`), and string
        heavy-hitter recount (`CB-Q34`, `CB-Q35`).
      - [x] Recheck apparent regressions from the full sequential pass with targeted reruns before
        changing code. Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_regression_check_20260622T_now/summary.json`.
        `CB-Q14` (`22.046s`), `CB-Q31` (`2.468s`), and `CB-Q32` (`3.372s`) returned to the prior
        range, so those were run-order/cache variance rather than retained regressions. `CB-Q33`
        (`19.573s` targeted, `27.711s` full), `CB-Q34` (`32.984s` targeted), and `CB-Q35`
        (`33.896s` targeted) remain the dominant slow lanes.
      - [x] Retain row-position locality early-stop for wide bounded sort materialization. The
        sort/top-K route already retained row references and delayed wide payload decoding; the
        new pass stops the final single-artifact `.vortex` materialization scan once every selected
        source ordinal has been found, then exposes
        `late_materialization_chunks_scanned`,
        `late_materialization_early_stop_applied`, and
        `late_materialization_max_selected_source_ordinal` through the public route fields.
        Focused validation: `cargo test -q -p shardloom-vortex --features vortex-local-primitives --lib sort_rows -- --nocapture`
        and `cargo test -q -p shardloom-cli --features release-user-surfaces
        local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`. Targeted
        100M local UAT over the retained single `.vortex` artifact showed the bounded sort family
        remains cleanly routed with no fallback or external engine invocation and produced
        `CB-Q24` `18.607s`, `CB-Q25` `2.373s`, `CB-Q26` `3.701s`, `CB-Q27` `3.701s` in
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_sort_early_stop_20260622T_now/summary.json`.
        Follow-up flattened-field probes showed why this is a reusable optimization but not a
        universal Q24 fix: `CB-Q24` selected rows near source ordinal `98,655,788` and remained
        noisy (`27.597s`) while `CB-Q25` stopped after `272` materialization chunks with max
        selected ordinal `9,440,339` and completed in `2.513s`
        (`/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q24_sort_early_stop_flattened_fields_20260622T_now/summary.json`,
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q25_sort_early_stop_flattened_fields_20260622T_now/summary.json`).
        The remaining Q24-class work is prepared predicate/row-position locality metadata, not
        another final-pass materialization tweak.
    - [ ] Update README/docs/capability reports only from the admitted runtime evidence; move the
      completed summary to the ledger after merge/session completion.
  - Next outcome: the 100M native Vortex route no longer has timeout rows for feasible local OLAP
    shapes, and the remaining >1s rows have evidence-backed state/materialization diagnostics plus
    measured before/after UAT timing.
  - User-visible surface: CLI `run dataframe`, Python `ctx.sql(...)` / DataFrame front doors,
    native Vortex route evidence, ClickBench local UAT artifacts, README/docs capability posture.
  - Implementation scope: `shardloom-vortex/src/local_primitives.rs`, aggregate/sort/string
    helpers, `shardloom-cli/src/public_workflow_route.rs`, capability/benchmark validators,
    Python wrappers only if evidence fields need transport exposure, docs, and UAT artifacts.
  - Evidence required: local operator correctness tests, route/evidence snapshots, no-fallback
    fields, targeted 100M UAT before/after rows, and explicit claim-boundary docs.
  - Verification: focused Rust tests for aggregate/distinct/string/top-K primitives, public
    workflow route tests for evidence propagation, the 100M targeted UAT harness with 180-second
    cap, and only then broader CI/release validators when the runtime family changes are complete.
  - Non-goals: adding external engines, creating ClickBench-only scenario shims, lowering the
    public route through diagnostic smoke commands, increasing caps to hide slow rows, or publishing
    official ClickBench claims from laptop-local UAT.
  - Claim boundary: local UAT/runtime-readiness evidence only until official benchmark methodology,
    hardware context, correctness validation, and publication gates approve a public claim.
  - Fallback boundary: all admitted work remains ShardLoom-native/Vortex-native; unsupported
    residuals must be deterministic diagnostics and may not call DuckDB, Polars, pandas, Spark,
    DataFusion, Velox, or Vortex query-engine integrations.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `PY-RUNTIME-OVERHEAD-1` Session-scoped persistent local runtime for Python public routes.
  - Source: live UAT showed raw CLI native Vortex Q22/Q23 routes at single-digit to low-teens
    milliseconds while Python `ctx.sql(..., input=...)` adds visible per-call subprocess overhead.
    `ShardLoomClient` currently resolves and launches the CLI for each operation; `ShardLoomSession`
    reuses SourceState/VortexPreparedState/result state but explicitly does not keep a Rust runtime
    process alive.
  - Goal: make normal Python, DataFrame, and SQL front doors feel as simple as `sl.context()` while
    avoiding per-operation CLI process launch when a caller keeps a context/session open.
  - V1/v0.2 scope classification: `required_for_release_quality` for local Python performance
    posture; not a production daemon, network service, external fallback, or separate execution
    engine.
  - ShardLoom technique review: use a caller-owned local worker as a capillary execution coordinator
    over the existing public workflow route contract; preserve PulseWeave/state-budget evidence,
    no-fallback fields, native Vortex input/output certificates, and deterministic diagnostics.
  - Execution checklist:
    - [x] Add the immediate low-risk Python client binary-resolution cache so repeated calls do not
      redo env/PATH/repo/bundled binary lookup.
    - [x] Design and implement the session-owned worker protocol: newline JSON over stdin/stdout, no
      network listener by default, explicit close, crash diagnostics, and unchanged
      `fallback_attempted=false` / `external_engine_invoked=false` route evidence in each delegated
      command envelope.
    - [x] Keep the existing compact CLI `OutputEnvelope` renderer as the in-process worker response
      boundary instead of refactoring every handler to return envelopes first; this removes
      per-operation process launch while preserving the same handler output contract.
    - [x] Add `ShardLoomClient` persistent-worker mode that is automatic for context/session-owned
      local use when supported, with an opt-out environment switch and deterministic
      fallback-to-subprocess only as transport fallback, never execution fallback.
    - [x] Prove representative Python ClickBench-style native Vortex routes reuse one worker, emit
      the same route/evidence fields, and reduce per-call wrapper overhead materially. Local UAT on
      `/Users/dylan/Desktop/shardloom-clickbench-uat/vortex/hits.vortex` with
      `target/release/shardloom`: Q25-style `COUNT WHERE URL LIKE` warm median improved from
      `9.028 ms` subprocess to `1.794 ms` persistent worker; Q23-style grouped top-N improved from
      `48.767 ms` to `36.833 ms`; both reported `native_vortex_aggregate`,
      `fallback_attempted=false`, and `external_engine_invoked=false`. The current release binary
      also ran the 43-query local ClickBench UAT through the Python persistent worker against that
      local 25k-row Vortex fixture with 43/43 successes, zero diagnostics, zero fallback/external
      invocations, route distribution `native_vortex_aggregate=36`,
      `native_vortex_sort_rows=4`, `native_vortex_count_all=1`,
      `native_vortex_count_where=1`, `native_vortex_filter_project=1`, median `7.616 ms`, max
      `365.632 ms`, and transcript
      `/Users/dylan/Desktop/shardloom-clickbench-uat/transcripts/uat_after_python_worker_43_summary.json`.
    - [x] Update README, Python docs, user-surface index, and release readiness docs so users get the
      fast path through the normal `sl.context()` / `with sl.session()` surfaces without knowing
      internal worker commands.
  - Evidence required: focused Rust route-envelope tests, Python worker lifecycle tests, UAT timing
    comparison, no-fallback evidence, and docs updates.
  - Verification: targeted CLI/Python tests first; full workspace gate after the worker contract is
    complete.
  - Non-goals: background network daemon, external query engine, Python in-process execution of
    unsupported plans, or broad production service claim.
  - Claim boundary: Python overhead reduction only for admitted local ShardLoom/Vortex routes.
  - Fallback boundary: persistent worker is a transport optimization; execution remains
    ShardLoom-native/Vortex-native with no external engine fallback.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `PERF-ATTRIBUTION-POST-UAT-1` Hot/cold/proof timing attribution cleanup before the next
  optimization pass.
  - Source: maintainer timing review after local ClickBench/UAT and benchmark-page inspection:
    cold source read, parse/decode, Vortex write, prepared lookup/create, evidence render, result
    sink, operator compute, selection-vector aggregation, and columnar-source rows need clearer
    included/excluded attribution before broad optimization claims.
  - Current state: timing surfaces separate hot runtime from proof/publication work. The
    benchmark optimization-target validator now confirms the promoted artifact has additive
    hot-runtime stage evidence for source read, parse/decode, prepared lookup/create, Vortex write,
    operator compute, and publication/proof separation. The current validation pass reported 600
    ShardLoom hot-runtime rows, 600 publication-proof rows, six measured hotspot targets, and zero
    timing-contract blockers in `target/benchmark-optimization-targets-report.json`.
  - Intake review: accepted timing-attribution row as a precondition for the source, writer,
    prepared-state, proof-lane, and encoded-operator optimization items below; this item has no
    direct runtime-gain claim, but prevents optimizing the wrong timing number.
  - V1/v0.2 scope classification: `required_for_release_quality` before publishing refreshed
    performance claims.
  - ShardLoom technique review: use route timing surface separation, evidence-tier controls,
    capillary per-stage work units, and PulseWeave state/pressure fields so hot runtime, cold
    ingest, prepared lookup, proof replay, and publication rendering remain distinct.
  - Execution checklist:
    - [x] Add stable fields for the promoted artifact stage surface where those stages execute.
      Canonical promoted fields include `source_read_ms`,
      `source_parse_or_columnar_decode_ms`, `source_read_byte_acquisition_millis`,
      `vortex_write_ms`, `vortex_digest_millis`, `vortex_reopen_verify_millis`,
      `prepared_state_lookup_or_create_ms`, `operator_compute_ms`,
      `result_sink_write_millis`, and `evidence_render_ms`.
    - [x] Mark each timing component as `included_hot_runtime`, `included_cold_route`,
      `included_prepare_once`, `included_publication_proof`, or `diagnostic_only` through
      `route_timing_stage_inclusion_classes` and the timing-surface row contract.
    - [x] Split cold-route totals so source read, parse/decode, write/register/reopen, and
      first-query prepared lookup are additive enough for regression triage. The validator confirms
      additive hot-route stage evidence for `source_read_scout_timing`,
      `jsonl_parse_decode_hot_runtime`, `prepared_state_lookup_or_create`, and
      `vortex_write_and_reopen_verify`.
    - [x] Add validator coverage that proof/publication components cannot silently redefine hot
      route totals and that excluded diagnostic fields are labeled. Current evidence:
      `python3 scripts/check_benchmark_optimization_targets.py` passed with no blockers.
    - [x] Refresh docs/website labels only from the canonical timing-surface fields; website and
      benchmark docs consume `timing_surface`, route-stage inclusion classes, and target reports
      instead of substituting publication proof rows into hot runtime.
  - Next outcome: benchmark/UAT artifacts explain which component changed before runtime claims are
    updated.
  - User-visible surface: benchmark JSON, website benchmark page, timing docs, phase evidence.
  - Implementation scope: benchmark row promotion, timing attribution structs, website/static
    generated assets, docs.
  - Evidence required: schema/golden tests, artifact validator output, and one targeted regenerated
    benchmark/UAT artifact.
  - Verification: focused timing-surface tests, static benchmark validator, website readiness check,
    and targeted UAT artifact inspection.
  - Non-goals: claiming faster runtime solely from relabeling or changing formulas.
  - Claim boundary: attribution clarity only until paired with measured runtime improvements.
  - Fallback boundary: no execution-engine changes or external fallback.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `PERF-SOURCE-ADAPTER-POST-UAT-1` Unified cold source-read, parse/decode, and columnar adapter
  fast path.
  - Source: maintainer optimization table: cold source read is about `9.49 ms` and roughly `49%` of
    a `19.41 ms` hot cold route; parse/decode averages about `8.44 ms` with worse JSONL outliers;
    columnar Parquet/ORC/Arrow IPC cold routes still show nontrivial totals.
  - Current state: universal ingest and source-specific route work exist. This batch removed
    another allocation-heavy text path by streaming CSV records after the header and materializing
    JSONL rows incrementally through the shared read plan instead of accumulating all parsed records
    before row assembly. The shared source adapter now emits local source-read scout evidence
    (`source_read_scout_schema_version=shardloom.local_source_read_scout.v1`) with metadata-scout,
    byte-acquisition, full-body, mmap/borrowed-buffer, read-buffer-carry, and many-small-file
    batching status fields through Vortex preparation, prepared-state reuse, public-workflow
    preparation, and the lower-level diagnostic local-source runtime.
  - Intake review: accepted cold source read, parse/decode, JSONL outlier, and columnar-source fast
    path rows into one source-adapter item because they share universal ingest, SourceState,
    projection/predicate admission, and Vortex-normalized handoff contracts.
  - V1/v0.2 scope classification: `required_for_release_quality` for local CSV/JSONL/columnar
    runtime performance; no external object-store requirement.
  - ShardLoom technique review: apply metadata-first source scouting, capillary source chunks,
    dynamic projection-aware decode, PulseWeave read/parse pressure signals, source-state reuse,
    read-once buffer carry, and native Vortex array handoff where Vortex/source APIs support it.
  - Expected optimization envelope: target `25-45%` source-read reduction (`~2.4-4.3 ms` on the
    observed cold route), `30-60%` parse/decode reduction (`~2.5-5.1 ms` average), larger JSONL
    outlier wins where structural-index or projection-aware parsing avoids unused fields, and
    `10-30%` columnar cold-row reduction depending on format/scenario.
  - Execution checklist:
    - [x] Split scout, byte acquisition, mmap/borrowed-buffer eligibility, full-read fallback, and
      many-small-file batching evidence in universal ingest. Current fields include
      `source_read_metadata_scout_millis`, `source_read_byte_acquisition_millis`,
      `source_read_full_body_millis`, `source_read_mmap_eligibility_status`,
      `source_read_buffer_carry_status`, and `source_read_many_small_file_batching_status`.
    - [x] Add projection-aware CSV decode that skips unused columns, uses typed column builders, and
      avoids row/string materialization for admitted scalar columns. The current text adapter now
      streams CSV records after the header, applies `LocalSourceReadPlan::should_materialize` before
      row insertion, preserves product-profile no-synthetic-cap behavior, and keeps smoke caps as
      internal safeguards.
    - [x] Add projection-aware JSONL/NDJSON structural indexing for admitted fields, typed builders,
      and deterministic diagnostics for unsupported nested paths rather than row-materialized
      parsing. The current parser skips unselected JSON values through the existing structural
      scanner, incrementally assembles materialized rows, preserves missing-field/null semantics for
      later-discovered columns, and retains deterministic unsupported diagnostics.
    - [x] Add read-once buffer carry evidence where safe, with explicit lifetime/materialization
      boundaries. Text sources report `read_once_buffer_carried_to_text_parser`; columnar sources
      report `digest_bytes_recorded_reader_reopens_columnar_source` or
      `not_used_columnar_reader_owns_buffer_lifetime` rather than claiming unsafe borrowed-buffer
      reuse.
    - [x] Add columnar-source fast paths for Parquet/ORC/Arrow IPC that preserve columnar buffers,
      reuse schema/metadata, and hand off directly to Vortex-normalized arrays where admitted.
      Current universal-format readers use reader-level projection helpers and report
      `source_state_columnar_preserved`, `source_state_record_batch_count`, and reader projection
      columns.
    - [x] Update public Python/SQL/DataFrame local-source routes to use the shared adapter contract,
      not source-specific parallel execution stacks. Public local files route through
      Vortex-normalized prepare/prepared/native flow; direct local-source compatibility remains an
      internal diagnostic safeguard only.
    - [x] Add fixtures for CSV, JSONL, Parquet, ORC, Arrow IPC, Vortex, partitioned/many-small-file
      inputs, and projected/nested scenarios.
      Current coverage is split across public workflow partitioned/native Vortex fixtures,
      text/structured universal-ingest fixtures, nested typed sink fixtures, and the traditional
      analytics many-small-files dataset profile.
    - [x] Refresh benchmark/site artifacts after the attribution item can prove component movement.
      Evidence: `python3 scripts/promote_benchmark_artifact.py --input
      target/benchmark-artifacts/traditional-full-local-final.json --profile full_local ...`,
      `python3 scripts/check_benchmark_artifact_completeness.py --manifest
      website/assets/benchmarks/latest/manifest.json --output
      target/benchmark-artifact-completeness-report.json`, and
      `python3 scripts/check_benchmark_optimization_targets.py --artifact
      website/assets/benchmarks/latest/benchmark-results.json --output
      target/benchmark-optimization-targets-report.json`; local full-size UAT remains tracked by
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the implementation PR merges and before the next release
      train.
  - Next outcome: cold source and parse/decode routes improve without changing the public front
    door or introducing smoke-route execution.
  - User-visible surface: `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`,
    `ctx.read_parquet(...)`, SQL input binding, CLI prepare/run route evidence, benchmark artifacts.
  - Implementation scope: universal ingest/source adapters, Python lowering, CLI public workflow
    route, Vortex normalization helpers, benchmark fixtures, docs.
  - Evidence required: correctness fixtures, no-fallback evidence, materialization/decode boundary
    fields, timing attribution, and local UAT comparison.
  - Verification: focused Rust/Python source-adapter tests, public workflow route tests, local
    ClickBench/UAT subset, and benchmark artifact validator. Current focused checks:
    `cargo test -p shardloom-cli --features release-user-surfaces --bin shardloom
    local_source_runtime_reports_source_read_split_fields -- --nocapture`,
    `cargo test -p shardloom-cli --features release-user-surfaces --bin shardloom source_read_plan
    -- --nocapture`, `python3 scripts/run_focused_checks.py --profile current-native-vortex`,
    `python3 scripts/check_benchmark_optimization_targets.py`, and
    `python3 scripts/check_ci_gate_matrix.py`.
  - Non-goals: object-store distributed reads, external parser engines, or increasing smoke caps.
  - Claim boundary: source-format-specific local cold-route improvement only after measured
    artifacts exist.
  - Fallback boundary: no DuckDB/Polars/pandas/Spark/DataFusion/Velox parse or execution fallback.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `PERF-PREPARED-WRITER-PROOF-POST-UAT-1` Prepared-state, Vortex writer, sink, and proof-lane
  compaction.
  - Source: maintainer optimization table: Vortex write averages about `3.49 ms`, prepared
    lookup/create about `1.09 ms` and roughly `69%` of the Prepare-Once First Query hot route,
    evidence render about `4-5.6 ms` in proof lanes, and result sink about `0.49-0.66 ms`.
  - Current state: Vortex write, prepared-state lookup/create, result sinks, and evidence render are
    now distinct and optimized enough for the local v0.2 runtime pass without changing query
    semantics. The local Vortex writer uses a thread-local single-threaded runtime/session context
    for repeated writes in one process, emits writer-context/segment/workspace-stage/digest/reopen
    timing splits, and reports scoped buffer-carry reuse through the copy-budget evidence once
    writer row-count plus digest/reopen proof exists. Artifact-adjacent prepared-state reuse
    manifests now expose a content-addressed read-through index view with cache hit/miss/repair
    fields. Native Vortex row exports already expose typed sink contracts and metadata-only route
    fields, while publication-proof evidence is separated from hot runtime through the timing
    surface/evidence-tier contract.
  - Intake review: accepted Vortex write, prepared lookup/create, evidence render, and result sink
    rows into one persistence/proof item because they share artifact lifecycle, cache, sidecar,
    digest, sink, and evidence-tier contracts.
  - V1/v0.2 scope classification: `required_for_release_quality` for prepared-route and proof-lane
    performance; publication proof remains slower only when it is explicitly doing more work.
  - ShardLoom technique review: use capillary artifact work units, PulseWeave state-budget and
    spill/memory diagnostics, content-addressed prepared-state indexes, evidence-tier sidecar
    reuse, metadata-only sink fast paths, and route timing-surface separation.
  - Expected optimization envelope: target `25-50%` Vortex-write reduction (`~0.9-1.7 ms` average),
    `40-70%` prepared lookup/create reduction (`~0.4-0.8 ms`), `50-80%` proof-lane evidence-render
    reduction (`~2-4.5 ms` outside hot runtime), and `20-50%` result-sink reduction
    (`~0.1-0.3 ms`).
  - Execution checklist:
    - [x] Add shared writer/runtime context with coalesced artifact writes and batch segment layout
      evidence. Current implementation uses a thread-local single-threaded Vortex runtime/session
      for repeated local writes, workspace-safe staged writes, capillary prewrite role gates, and
      `vortex_writer_context_reuse_status` evidence.
    - [x] Split Vortex write, digest, register/workspace-stage, and reopen timing; avoid reopen on
      hot/minimal paths when the artifact digest and writer row-count proof are sufficient, and
      reuse the workspace-safe streaming SHA-256 digest instead of rereading the completed artifact
      for local Vortex prepare. Current fields include `vortex_writer_context_open_millis`,
      `vortex_segment_write_millis`, `vortex_workspace_stage_millis`, `vortex_digest_millis`,
      `vortex_artifact_digest_source`, `vortex_reopen_verify_millis`, and
      `vortex_reopen_hot_path_status`.
    - [x] Add content-addressed prepared-state index with manifest read-through cache and explicit
      cache hit/miss/repair fields. Current fields include
      `vortex_prepared_state_reuse_index_key`,
      `vortex_prepared_state_reuse_index_lookup_status`,
      `vortex_prepared_state_reuse_index_cache_scope`, and
      `vortex_prepared_state_reuse_index_repair_status`.
    - [x] Add role-scoped prepared-state repair visibility for stale/missing sidecars without full
      recreate when content digests prove reuse is safe. Local single-artifact routes report
      `vortex_prepared_state_reuse_role_scoped_repair_status`; broader traditional prepared-batch
      routes keep role reuse/repair evidence in the workspace manifest family.
    - [x] Add compact evidence tiers and sidecar reuse so hot/default lanes do not render full human
      publication evidence. Current benchmark evidence separates `hot_runtime`,
      `full_replay_proof`, and `publication_proof`, with compact machine evidence on hot/default
      surfaces and human publication render deferred unless requested.
    - [x] Add metadata-only result sink fast path, compact JSON sink mode, and native sink reuse
      where output semantics allow it. Current native Vortex result sinks expose
      `route_metadata_only`, bounded JSONL/CSV row export, structured Vortex-derived export, and
      typed sink contract fields without external-engine execution.
    - [x] Refresh package/release docs, website labels, and benchmark artifacts so
      publication-proof costs remain visible but are not conflated with hot runtime. Evidence:
      refreshed website benchmark data, `docs/release/ci-work-shaping.md`,
      `docs/release/ci-gate-matrix.md`, package/readiness docs in this branch, and the passed
      artifact validators above; local full-size UAT remains tracked by
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the implementation PR merges and before the next release
      train.
  - Next outcome: prepared-first-query and proof/publication lanes get faster and more explainable
    without weakening evidence.
  - User-visible surface: prepared routes, write APIs, result sinks, benchmark proof lanes, website
    evidence views.
  - Implementation scope: Vortex writer helpers, prepared-state registry/cache, sink contracts,
    evidence rendering, timing fields, docs/website.
  - Evidence required: artifact lifecycle tests, cache hit/miss fixtures, sink correctness,
    no-fallback fields, timing artifacts, and proof-lane validators.
  - Verification: focused Rust writer/prepared-state tests, Python write/collect tests, benchmark
    artifact validator, and targeted local UAT rerun. Current focused checks:
    `cargo test -p shardloom-vortex --features vortex-write
    copy_budget_reports_unmeasured_segments_and_blocks_unsafe_reuse -- --nocapture` and
    `cargo test -p shardloom-cli --features release-user-surfaces --test
    sql_local_source_runtime_smoke vortex_prepare_writes_reopens_vortex_prepared_state --
    --nocapture`.
  - Non-goals: reducing proof work by removing required evidence, external stores, or hidden
    compatibility expansion.
  - Claim boundary: prepared/write/proof-lane improvements only for measured local artifact paths.
  - Fallback boundary: native ShardLoom/Vortex artifact lifecycle only; no external execution
    engines.
  - Ledger rule: move completed detail after merge/session completion.

- [x] `PERF-ENCODED-OPERATORS-POST-UAT-1` Encoded-native operator and selection-vector metric
  aggregation expansion.
  - Source: maintainer optimization table: operator compute geomeans are currently low, but the
    pre-refresh published inventory reported `0` encoded-native rows, `960` residual-native rows,
    `240` materialized-temporary rows, and `48` blocked selection-vector metric aggregation rows.
  - Current state: broad SQL/Python/DataFrame operator surfaces are admitted through shared runtime
    contracts, and the normal Vortex local primitive route now exposes encoded/native rows for
    count/filter/project/filter-project, residual-native state for scalar/grouped aggregate,
    bounded sort/top-N, joins, rolling/window, distinct/duplicates, reshape, and typed expression
    rewrites, plus explicit state-budget evidence. The compute capability matrix now reports
    encoded-native, residual-native, materialized-temporary, unsupported/report-only, and blocker
    counts by operator family, and `vortex_sink_write` is no longer mislabeled as unsupported.
    Benchmark promotion now treats selection-vector-backed metric aggregation evidence as an
    admitted residual-native bridge instead of a blocked hot-path candidate. Full encoded-native
    aggregate/join/sort claims still require physical encoding proof and refreshed benchmark
    artifacts before being stated publicly.
  - Intake review: accepted operator compute and selection-vector metric aggregation rows into one
    encoded-operator item because both require kernel registry/admission changes rather than source
    adapter or writer changes.
  - V1/v0.2 scope classification: `required_for_release_quality` for operator-heavy rows that can
    use in-repo encoded kernels without external infrastructure.
  - ShardLoom technique review: use encoded-columnar kernels, selection vectors, capillary operator
    work units, dynamic admission by dtype/cardinality/selectivity, metadata-first pruning,
    PulseWeave memory/spill diagnostics, and late materialized aggregate/join/top-N state.
  - Expected optimization envelope: modest geomean movement on current small rows, but target
    `20-70%` operator-heavy row reductions and possible `5-30 ms` wins on group/join/nested
    outliers; selection-vector metric aggregation should target `20-50%` selective-filter query
    work reduction where admitted.
  - Execution checklist:
    - [x] Convert the inventory from residual/materialized rows into prioritized encoded-native
      kernel families by scenario, dtype, null profile, and route lane. The compute matrix now
      emits `operator_family_execution_summary` plus per-family counts.
    - [x] Add encoded-native count/filter/project/filter-project kernels where Vortex
      encodings/statistics permit direct or no-row-materialization execution; keep sum/min/max/mean
      broad encoded-native claims behind physical encoding evidence.
    - [x] Add selection-vector metric aggregation admission for rows with native state-budget
      evidence and deterministic diagnostics for missing dtype/null evidence. Benchmark promotion
      now reports `admitted_selection_vector_metric_aggregation_residual_native` or
      `admitted_selected_metric_aggregation_residual_native` when runtime fields prove the bridge.
    - [x] Add late-materialized aggregate/top-N/join helpers that keep projected payload columns
      deferred until after selection/group state is finalized. Current Vortex local primitive
      reports expose state-budget/capillary/PulseWeave evidence for scalar/grouped aggregates,
      bounded sort/top-N, rolling/window, row-key, reshape, and duplicate-state paths.
    - [x] Add operator benchmark/evidence fields showing encoded-native, residual-native,
      materialized-temporary, and blocked counts by route family.
    - [x] Add correctness fixtures for nulls, empty/all-null groups, duplicate keys,
      nested/string predicates, and decoded-reference parity where the current native primitive
      route is admitted. Current focused fixtures prove `COUNT(col)`, `COUNT_DISTINCT(col)`,
      `SUM`, and `AVG` skip null measures correctly, all-null groups return null aggregates
      without fallback, SQL-style null comparison semantics do not select null rows, and
      `contains` filter/project uses ShardLoom residual predicate evaluation with projection
      pushdown and explicit decode/materialization evidence rather than blocking or calling an
      external engine.
    - [x] Refresh benchmark artifacts and website operator-inventory views after kernels are
      admitted. Evidence: regenerated `website/assets/benchmarks/latest` and
      `website-src/src/data/benchmark-evidence.json`; direct generated-row scan reports zero stale
      `blocked_selection_vector_metric_aggregation_not_admitted` or
      `pending_selection_vector_metric_aggregation` rows. Local full-size UAT remains tracked by
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the implementation PR merges and before the next release
      train.
  - Next outcome: operator-heavy rows use ShardLoom's encoded/runtime features by default through
    the normal front doors.
  - User-visible surface: Python/DataFrame/SQL groupby, joins, top-N, nested scans, metric
    aggregations, route evidence, benchmark/site operator inventory.
  - Implementation scope: expression/kernel registry, Vortex local primitives, operator admission,
    route/evidence fields, tests, docs/website.
  - Evidence required: encoded-kernel correctness tests, decoded-reference parity, no-fallback
    certificates, state-budget evidence, and refreshed operator inventory.
  - Verification: targeted Rust operator tests, Python parity tests, capability matrix validator,
    local UAT subset, and benchmark artifact validator. Current focused checks:
    `cargo test -p shardloom-cli --test compute_capability_matrix_snapshots -- --nocapture`,
    `cargo test -p shardloom-vortex --features vortex-local-primitives --lib
    simple_aggregate_skips_null_measures_without_fallback -- --nocapture`,
    `cargo test -p shardloom-vortex --features vortex-local-primitives --lib
    grouped_aggregate_skips_all_null_group_measures_without_fallback -- --nocapture`,
    `cargo test -p shardloom-vortex --features vortex-local-primitives --lib
    filter_and_project_contains_uses_shardloom_residual_without_fallback -- --nocapture`,
    `python3 -m unittest
    python.tests.test_release_scripts.ReleaseScriptTests.test_benchmark_promoter_emits_operator_mode_inventory
    python.tests.test_compute_engine_completion_gate.ComputeEngineCompletionGateTests.test_completion_gate_classifies_optimization_statuses_separately`,
    and `python3 -m py_compile scripts/promote_benchmark_artifact.py`.
  - Non-goals: external query-engine residual execution, broad unsafe UDF execution, or claiming
    encoded-native support where physical encoding evidence is absent.
  - Claim boundary: encoded-native support only for admitted kernels/encodings/dtypes with evidence.
  - Fallback boundary: residuals are ShardLoom-native or explicit diagnostics; no external fallback.
  - Ledger rule: move completed detail after merge/session completion.

### Post-Merge UAT Runbook: `CLICKBENCH-UAT-FULL-FORMAT-1`

Sequential local full-format ClickBench UAT after the implementation PR is merged and before any
version/release train.
  - Source: user request to test full dataset sizes, selected formats, and all 43 ClickBench
    scenarios after current implementation work and PR/merge, then use that result as the gate for
    any later version/release train.
  - Goal: run a local, sequential, laptop-safe UAT over the repo-managed
    `benchmarks/clickbench/queries.sql` using the current public/native Vortex route surface and the
    available local format preparations: CSV -> Vortex, JSONLines -> Vortex, Vortex, partitioned
    Vortex, and Parquet/partitioned Parquet when official fixtures are present or reproducibly
    generated through an admitted adapter.
  - V1/v0.2 scope classification: `post_merge_pre_release_uat_evidence`; not a public ClickBench
    submission, leaderboard result, performance superiority claim, or replacement for benchmark
    claim gates.
  - ShardLoom technique review: use native Vortex middle, capillary/state-budget route fields,
    PulseWeave pressure signals, sequential query execution for laptop safety, public-runtime
    requested/effective max-parallelism evidence, timing-surface labels, no-fallback evidence, and
    explicit format-preparation timing.
  - Pre-release checklist:
    - [x] Inventory local Desktop ClickBench fixtures and record row counts, bytes, source format,
      prepared Vortex artifacts, partition layout, and missing official-format fixtures. Current
      `/Users/dylan/Desktop/shardloom-clickbench-uat` evidence is local-scale, not official full
      ClickBench scale: `data/hits.csv` has 25,001 lines including header and 14.7 MB;
      `data/hits.jsonl` has 25,000 rows and 53.5 MB; prepared Vortex artifacts include
      `vortex/hits.vortex` at 3.0 MB, `vortex/hits_jsonl.vortex` at 3.0 MB, and four
      EventDate-partitioned Vortex files around 3.0-3.2 MB each. The folder does not currently
      contain official Parquet or partitioned-Parquet fixtures, so those remain preparation inputs
      rather than completed local UAT evidence.
    - [x] Run all 43 queries sequentially against native Vortex input and capture route id,
      wall-clock timing, output rows, capillary/PulseWeave/state-budget fields, fallback/external
      fields, and diagnostics. Current local-scale evidence: CSV-prepared Vortex transcript
      `uat_after_python_worker_43_summary.json` passed 43/43 with median `7.616 ms`, max
      `365.632 ms`; JSONL-prepared Vortex transcript
      `uat_after_python_worker_43_jsonl_prepared_vortex.json` passed 43/43 with median `6.192 ms`,
      max `65.932 ms`; both had zero diagnostics and zero fallback/external invocations.
    - [x] Repeat the 43-query run for partitioned Vortex when the public route can bind the
      partition set without falling back; otherwise add the exact implementation item instead of
      hiding the gap. Current evidence: native Vortex manifest route binding ran all 43 queries
      through the Python public SQL surface with 43/43 successes, zero diagnostics, zero
      fallback/external invocations, route distribution `native_vortex_aggregate=36`,
      `native_vortex_sort_rows=4`, `native_vortex_count_all=1`, `native_vortex_count_where=1`,
      `native_vortex_filter_project=1`, manifest binding mode on all rows, and partitioned binding
      evidence on all rows. Local transcripts:
      `/Users/dylan/Desktop/shardloom-clickbench-uat/transcripts/uat_current_partitioned_manifest_declared_input_43.json`
      and
      `/Users/dylan/Desktop/shardloom-clickbench-uat/transcripts/uat_current_partitioned_manifest_path_literal_43_after_python_bridge.json`.
    - [x] Add native partitioned/multi-file input binding for local Vortex manifests or partition
      sets, with source-state reuse, additive partition timing fields, and no-fallback evidence, so
      `shardloom-vortex-partitioned` can run as one public workflow instead of a set of manual
      single-file probes. The current route binding accepts single `.vortex` files, local
      directories containing `.vortex` parts, and manifest files with `inputs`/`paths`; evidence
      fields include `native_vortex_input_binding_mode`, `native_vortex_input_binding_count`,
      `native_vortex_partitioned_input_binding`, `native_vortex_input_binding_strategy`, and
      `native_vortex_input_binding_sources`.
  - Post-merge run steps:
    - Generate or refresh public-format preparations from official full ClickBench sources through
      the public preparation flow where needed; do not expose diagnostic smoke routes as product
      runtime.
    - Run CSV/JSONLines/Parquet format preparation timings separately from query timings so load
      cost is not mixed into native-query timing.
    - Run the full 100M-row 43-query UAT sequentially for the selected released formats:
      `shardloom-parquet`, `shardloom-parquet-partitioned`, `shardloom-vortex`,
      `shardloom-vortex-partitioned`, `shardloom-csv`, and `shardloom-jsonlines`.
    - Compare results to the current route-readiness classifier and record every discrepancy as a
      fix or explicit phase-plan item.
    - Summarize results in a local UAT artifact under `target/` or Desktop transcripts; do not
      promote to website/benchmark claims until claim gates approve publication.
  - Evidence required: local UAT JSON summary, per-query transcripts, route-readiness comparison,
    and no-fallback evidence.
  - Verification: `scripts/check_clickbench_olap_runtime_coverage.py`, sequential UAT runner output,
    and focused fixes for any failed scenario.
  - Non-goals: public benchmark publication, hidden external baselines, unsupported route masking, or
    full workspace CI while runtime gaps remain.
  - Claim boundary: local UAT only.
  - Fallback boundary: no DuckDB/Polars/pandas/Spark/DataFusion/Velox execution.
  - Ledger rule: move completed detail after merge/session completion.

- The ClickBench route-readiness polish, Python/DataFrame runtime-surface polish, future-contract
  blocker field alignment, and native Vortex-derived structured export closeout requested on
  June 19 were completed and moved to
  `docs/architecture/phased-execution-completed-ledger.md`. The broader native Vortex route
  unification is now closed through the `native_vortex_unified_plan` contract and ledgered in
  `docs/architecture/phased-execution-completed-ledger.md`. The scalar/null rewrite closeout,
  benchmark-equivalence constitution, external-environment gate split, and `UAT-RUNTIME-9`
  universal ingest front-door UAT hardening are also ledgered.

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
