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
The first unchecked checkbox is the next default autonomous slice.

Current autonomous execution order:

- [ ] `CLICKBENCH-100M-RUNTIME-BURNDOWN-2` Full-scale native Vortex aggregate, string, distinct,
  and bounded top-K runtime burndown.
  - Source: post-merge 100M local ClickBench UAT against
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m.vortex` using the
    merged `target/release/shardloom run dataframe ... --execution-policy native_vortex` CLI route.
    Combined local evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_post_merge_combined_summary.json`.
  - Current state: the 100M native Vortex route is functionally broad but not yet release-quality
    for every OLAP family. The cap-only local run recorded 34/43 successful queries, 9 rows timed
    out at the 180-second UAT cap (`CB-Q16`, `CB-Q17`, `CB-Q18`, `CB-Q19`, `CB-Q23`, `CB-Q32`,
    `CB-Q33`, `CB-Q34`, `CB-Q35`), 22 successful rows still over 1s, zero completed runtime
    failures, and no completed fallback/external-engine violations. Successful rows reported
    `native_vortex` execution and observed `max_parallelism=2`.
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
    - [ ] Re-verify the public-route invariant for every optimized row: compatibility inputs are
      source adapters only, `auto` and explicit native routes normalize into an admitted
      Vortex-prepared/native middle, direct local diagnostic paths remain internal safeguards, and
      no product route can report `sql-local-source-smoke`, `direct_compatibility_transient`,
      `fallback_attempted=true`, or `external_engine_invoked=true`.
    - [ ] Preserve one shared runtime family across CLI, SQL, Python, and DataFrame-style wrappers:
      update lowering/evidence transport only when needed so aliases converge into the same
      Vortex-native aggregate, string-predicate, bounded-sort, distinct, and sink contracts.
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
    - [ ] Implement faster string predicate and URL expression kernels for `CB-Q21`, `CB-Q22`,
      `CB-Q23`, `CB-Q24`, `CB-Q28`, and `CB-Q29`: byte-level contains for `LIKE '%literal%'`,
      shared positive/negative string predicate evaluation, string length without full decode where
      possible, and a specialized URL host extraction path for the ClickBench regex-domain shape.
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
    - [ ] Implement bounded sort/materialization improvements for `CB-Q24`, `CB-Q25`, `CB-Q26`,
      `CB-Q27`, `CB-Q40`, and `CB-Q41`: top-N/offset heaps, projection-aware row materialization,
      late payload decode, and deterministic ordering/tie fields.
      - [x] Replace tiny per-chunk truncation with a capillary retention window so bounded top-K
        sort routes reduce repeated resort/truncate work while preserving deterministic ties.
    - [ ] Apply PulseWeave work shaping in the optimized routes: record `FlowInventory`-style
      source/execution/writer work, `ScarcityLedger` memory/decode/sink pressure, `EndoPulse`
      run-local feedback, and `ProofBound` evidence so adaptive behavior remains certificate-gated.
    - [ ] Apply capillary work-unit semantics where the scan/operator can split or coalesce work:
      record source range, projected columns, filter mask posture, target artifact/output boundary,
      materialization posture, retry/idempotency state, sink pressure, memory pressure, and
      no-fallback evidence for ingest/prep/execution/output units.
      - [x] Record capillary retention and ordered-candidate work units for bounded sort and grouped
        ordered aggregate routes, including retained candidate counts and pressure signals.
      - [x] Preserve direct non-null Vortex dictionary/run-end kernel admission while blocking
        nullable encoded layouts from being silently decoded into reader-generated kernel inputs.
    - [ ] Apply dynamic work shaping without route proliferation: coalesce small units when
      scheduling overhead dominates, split large high-cardinality/string units when state/decode
      pressure requires it, and expose the chosen unit sizing and admission reason in evidence.
    - [ ] Apply metadata-first and late-materialized execution before row decode: statistics/pruning
      checks before row reads where supported, encoded/dictionary kernels before string decode,
      and explicit bounded decode/materialization evidence at collect or compatibility-write
      boundaries.
    - [ ] Add evidence fields that make the optimized path auditable: aggregate key encoding mode,
      dictionary-code grouping status, distinct-state strategy, bounded-top-k strategy,
      materialized-row count, decoded-string count, state-byte estimate, spill status, and
      `fallback_attempted=false` / `external_engine_invoked=false`.
      - [x] Add/verify evidence for `typed_hash_exact`, `typed_hash_key_functional_dependency`,
        `source_order_no_sort`, `capillary_retention_window`, residual materialization posture, and
        no-fallback/no-external-engine status on changed primitive paths.
      - [x] Add/verify evidence for partial predicate pushdown with residual materialization posture
        plus grouped ordered candidate strategy fields: `capillary_ordered_topk`,
        `candidate_groups`, and `retained_candidate_groups`.
      - [x] Add/verify metadata-only count evidence so local engine count routes do not report data
        reads when the route answered from Vortex file row-count metadata.
    - [ ] Preserve timing-surface discipline in route output and refreshed artifacts: hot runtime,
      replay proof, and publication proof remain separate, and no evidence render/result-sink work
      is folded into a query-runtime claim.
    - [ ] Add focused correctness fixtures for the changed operator families: exact distinct
      null/drop semantics, duplicate/high-cardinality keys, string predicate case behavior,
      URL/regex replacement equivalence for the admitted profile, top-K/offset ties, empty/all-null
      groups, and decoded-reference parity.
      - [x] Add focused fixtures for typed distinct key separation, functional-dependency group-key
        pruning, source-order group output, direct UTF-8 contains count, bounded-sort retention, and
        residual materialization evidence.
      - [x] Add mixed predicate fixtures proving Vortex filter pushdown is preserved for safe
        conjuncts while UTF-8 residual predicates stay ShardLoom-owned and evidence-backed.
      - [x] Add/regression-fix fixtures for nullable dictionary/run-end encoded input blocking,
        bounded row-export observed-count semantics, and metadata-only count evidence.
    - [ ] Rerun targeted local 100M UAT for the affected rows under the 180-second cap, then rerun
      the full 43-query native Vortex UAT only after targeted rows no longer timeout or regress.
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
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the release train.
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
      hot/minimal paths when the artifact digest and writer row-count proof are sufficient. Current
      fields include `vortex_writer_context_open_millis`, `vortex_segment_write_millis`,
      `vortex_workspace_stage_millis`, `vortex_digest_millis`, `vortex_reopen_verify_millis`, and
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
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the release train.
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
      `CLICKBENCH-UAT-FULL-FORMAT-1` after the release train.
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

### Post-Release UAT Runbook: `CLICKBENCH-UAT-FULL-FORMAT-1`

Sequential local full-format ClickBench UAT after the v0.2 release train.
  - Source: user request to test full dataset sizes, selected formats, and all 43 ClickBench
    scenarios after current implementation work, PR/merge, and the v0.2.0 release train.
  - Goal: run a local, sequential, laptop-safe UAT over the repo-managed
    `benchmarks/clickbench/queries.sql` using the current public/native Vortex route surface and the
    available local format preparations: CSV -> Vortex, JSONLines -> Vortex, Vortex, partitioned
    Vortex, and Parquet/partitioned Parquet when official fixtures are present or reproducibly
    generated through an admitted adapter.
  - V1/v0.2 scope classification: `post_release_uat_evidence`; not a public ClickBench submission,
    leaderboard result, performance superiority claim, or replacement for benchmark claim gates.
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
  - Post-release run steps:
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
