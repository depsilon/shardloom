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
- Treat the user's surface choice as preference-level syntax after source admission. SQL text,
  Python lazy calls, DataFrame-style method chains, and CLI commands may have small parsing or
  ergonomics differences, but they must converge before execution on the same Vortex-normalized
  physical plan, state budget, sink, and evidence vocabulary wherever semantics match. ClickBench
  UAT optimizations are therefore only acceptable when they benefit that shared runtime path and
  are visible to the other user surfaces through the same route evidence.
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
- Performance optimization items must be decision-gated, not open-ended. Each target must record
  the current measured timing or cost signal, the dominant cost class, the shared runtime component
  to improve, the proposed fix, the retain/drop threshold, and the exact evidence that decides
  whether the technique ships, is revised, or is removed. Do not retain a slower optimization because
  it is architecturally interesting.
- Performance fixes must improve shared ShardLoom/Vortex-normalized components rather than
  one-off query routes. If a targeted ClickBench lane motivates the work, the implementation still
  belongs in reusable ingest, metadata, dictionary, encoded predicate, aggregate, top-K, writer,
  sink, or evidence components unless a documented semantic boundary proves otherwise.
- Performance fixes must prefer shared/reused components over parallel implementations. A
  source-specific adapter may tune read/decode policy, but once data reaches the Vortex-normalized
  middle it should reuse the same prepared-state, writer, segment-layout, metadata, physical-plan,
  operator, sink, and evidence helpers wherever semantics allow. Do not create CSV/Parquet/JSONL,
  SQL/Python/DataFrame, benchmark/UAT, or ClickBench-only variants for the same runtime behavior.
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
  `docs/architecture/clickbench-ingest-optimization-ledger.md`,
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

1. Keep `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active as the standing owner for unchecked global
   architecture runtime-gap families until those rows are closed or promoted into concrete runtime
   work.
2. Use `CLICKBENCH-STRING-PREDICATE-MEMBERSHIP-17` as the next material performance owner. Current
   Q21-Q24-style evidence shows broad URL/string predicate scans before aggregation or row-ref
   top-K. Apply the Vortex-first provider path before inventing new metadata: upstream Vortex 0.75
   exposes SQL LIKE/NOT LIKE expressions with dictionary/FSST provider support, so ShardLoom
   substring predicates should lower into Vortex scan filters first; embedded membership metadata is
   reserved for lanes that still scan broadly after this exact pushdown.
3. Use `VORTEX-075-NATIVE-PROVIDER-UTILIZATION-18` as the shared-provider audit/implementation
   owner for Vortex 0.75 surfaces that can materially improve the common Vortex-normalized runtime:
   grouped count/sum provider adapters, byte-length transforms, dictionary take/filter/mask, layout
   reader/file-open cache reuse, and writer/encoding policy. Do not add a Vortex SQL or external
   query-engine route.
4. Keep `CLICKBENCH-ROW-REF-TOPK-SEGMENT-PRUNING-14` tied to embedded order-key metadata for true
   pre-read segment pruning. The completed branch implements safe chunk-threshold pruning after the
   key-only scan; do not claim segment pruning until exact order-key min/max/locality metadata is
   available inside the single `.vortex` artifact.
5. Treat the replacement-ingest ship/drop UAT for
   `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15` and
   `CLICKBENCH-ARTIFACT-SIZE-ENCODING-POLICY-16` as completed for this batch. The retained broad
   source-text profile is the current baseline; selective payload-only compression and the ultra
   row-block profile were both dropped.
6. Run focused PR validation for any remaining branch edits; avoid another full workspace or full
   ClickBench run unless a new implementation batch materially changes runtime behavior.
7. Create/merge the cohesive PR when required checks are green.
8. Start any version/release train only after merged checks and explicit maintainer approval.

- [ ] `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active owner for unchecked global architecture runtime
  gaps.
  - V1 scope classification: `required_for_v1`.
  - Source: `scripts/check_runtime_gap_family_burn_down.py`, `docs/architecture/global-architecture-review.md`,
    and the `#1363` review finding that completed-ledger items must not count as active owners for
    unchecked global review rows.
  - Current state: runtime gap-family mappings preserve provenance back to completed GAR items, but
    unchecked global architecture review rows need a current active owner while concrete runtime
    work remains open.
  - ShardLoom technique review: evidence-tier controls and no-fallback discipline apply. This row
    is ownership/governance only; concrete implementation still belongs in shared Vortex-normalized
    runtime, ingest, operator, sink, or evidence components, not one-off route splits.
  - Execution checklist:
    - [ ] Keep this active owner present while any mapped global architecture review runtime gap
      remains unchecked.
    - [ ] For each mapped gap family, either close the global review row with runtime evidence or
      promote the next concrete shared-runtime implementation item before removing this owner.
    - [ ] Run `python3 scripts/check_runtime_gap_family_burn_down.py` whenever this owner,
      global-review rows, or runtime gap-family mappings change.
    - [ ] Move this item to the completed ledger only after all mapped unchecked global review rows
      are closed or replaced by more specific active phase-plan owners.
  - Acceptance: runtime gap-family reports always show both historical provenance and at least one
    active phase-plan owner for unchecked global architecture review rows.
  - Status: active carry-forward owner.

- [ ] `CLICKBENCH-STRING-PREDICATE-MEMBERSHIP-17` Push URL/string substring predicates into the
  native Vortex provider first, then add embedded dictionary/segment membership only where broad
  scans remain.
  - V1 scope classification: `required_for_v1`.
  - Source: local 100M Q21-Q24-style UAT evidence. Official URL/string predicate lanes spent
    material time scanning URL/title/search strings before count, group/top-K, or wide row-ref
    materialization. Pre-fix manual evidence for
    `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` reports
    `local_primitive_rows_scanned=99997497`, `local_primitive_arrays_read_count=800`,
    `local_primitive_candidate_rows_seen=15911`, and `sort_predicate_strategy=residual_predicate_source_ordinals`.
  - Current state: ShardLoom has fast UTF-8 contains helpers for direct, FSST, masked, and
    dictionary arrays, plus embedded domain/length derived columns. The Vortex-first review found
    upstream Vortex 0.75 SQL LIKE/NOT LIKE expression support with dictionary/FSST execution
    providers; this should become the shared SQL/Python/DataFrame substring predicate lowering
    before introducing new ShardLoom-specific metadata.
  - ShardLoom technique review: metadata-first execution and capillary work units apply directly.
    Universal Ingest should persist generic, query-independent membership/sketch metadata inside the
    single `.vortex` artifact; the physical planner should consume that metadata before scanning
    strings; PulseWeave evidence should report membership hits/skips at unit boundaries; timing
    surfaces stay hot-runtime only for the query path. No query-answer sidecar, materialized view,
    or ClickBench-only route is allowed.
  - Execution checklist:
    - [x] Confirm Vortex 0.75 has a native exact LIKE/NOT LIKE expression/provider surface suitable
      for ShardLoom `StringContains` pushdown without adding a query-engine fallback.
    - [x] Lower ShardLoom `StringContains` predicates to Vortex LIKE/NOT LIKE scan expressions and
      classify them as exact row filters so wide top-K routes can keep row-id late materialization.
    - [x] Run targeted UAT for Q21, Q22, Q23, and Q24 plus Q25-Q27 order-by guards against the
      retained 100M single `.vortex` artifact.
      - Evidence: `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_vortex_like_route_aware_20260628T_now`.
        Q21 improved materially (`5.682s` local run, zero decode/materialization), Q24 improved
        materially (`3.856s`, row-ref late materialization retained), Q25-Q27 remained successful,
        and Q22/Q23 stayed slow on grouped string aggregation (`14.700s` / `34.197s`) without Vortex
        filter pushdown.
    - [x] Decide ship/drop for the Vortex LIKE pushdown pass from UAT evidence. Ship route-aware
      Vortex LIKE pushdown for count/filter and wide row-ref top-K; do not force it into grouped
      aggregate lanes until provider-backed grouped/runtime evidence beats the existing
      dictionary/residual path.
    - [ ] If broad scans remain after Vortex LIKE pushdown, design the exact membership contract for
      high-value UTF-8 columns (`URL`, `Title`,
      `SearchPhrase`, `Referer`, and source-derived URL-domain/length columns) as generic
      artifact metadata, not query-specific `google` columns.
    - [ ] If needed, implement Universal Ingest production of the metadata inside the single `.vortex`
      artifact or an existing Vortex-native metadata/encoding surface; if Vortex cannot embed the
      structure directly yet, record the Vortex-first decision and use a compact hidden encoded
      column only when it is generic and reusable.
    - [ ] If needed, teach count/filter/group/top-K physical planning to consult membership metadata before
      broad string scans and to fall back to existing exact encoded/dictionary/FSST row checks only
      for segments that cannot be pruned.
    - [ ] Emit reusable evidence: membership columns/families, segments/chunks skipped, rows
      avoided, residual string checks, exactness status, no query-answer cache, and
      `fallback_attempted=false` / `external_engine_invoked=false`.
  - Next outcome: official URL/string predicate lanes use native Vortex LIKE pushdown before
    aggregation or row-ref top-K, and any remaining broad scan gets a follow-up embedded metadata
    design only if it is material.
  - User-visible surface: shared SQL/Python/DataFrame/CLI native Vortex runtime evidence; no new
    public route family.
  - Implementation scope: local primitive predicate planning and Vortex expression lowering first;
    Universal Ingest metadata production, Vortex embedded metadata/hidden encoded column policy, and
    extra evidence fields only if the Vortex LIKE pass does not materially reduce the lane.
  - Evidence required: focused Rust tests for exact Vortex LIKE semantics and wide row-ref top-K;
    targeted local 100M UAT for Q21-Q24 plus Q25-Q27 guard; no full workspace until the cohesive
    implementation batch is complete.
  - Non-goals: no `google`-specific field, no query-answer sidecar, no approximate LIKE semantics,
    no external engine fallback, and no row-block-only writer retuning as a substitute.
  - Claim boundary: local UAT optimization evidence only; no official ClickBench or superiority
    claim.
  - Fallback boundary: every successful or blocked route must keep `fallback_attempted=false` and
    `external_engine_invoked=false`.

- [ ] `VORTEX-075-NATIVE-PROVIDER-UTILIZATION-18` Convert the material Vortex 0.75 provider
  surfaces into shared ShardLoom runtime improvements.
  - V1 scope classification: `required_for_v1`.
  - Source: source-grounded Vortex 0.75 review against pinned local crates and upstream release
    notes. Usable provider candidates include `vortex.like`/`not_like` with dictionary/FSST
    reducers, `vortex.byte_length`, grouped `count`/primitive `sum` kernels, dictionary
    take/filter/mask reducers, `VortexOpenOptions::with_layout_reader_cache`, file-stat pruning,
    mask/zip improvements, and writer/encoding surfaces. JSON/geospatial/GPU/device and
    DataFusion-related changes are not hot-runtime providers for ShardLoom v1.
  - Current state: Vortex LIKE is partially admitted through
    `CLICKBENCH-STRING-PREDICATE-MEMBERSHIP-17`; layout-reader cache and file-stat evidence already
    appear in local primitive reports; grouped aggregate, byte-length, dictionary take/filter/mask,
    and writer/encoding policy need explicit ship/drop passes before broader claims.
  - ShardLoom technique review: this item must strengthen the single Vortex-normalized physical
    runtime shared by SQL/Python/DataFrame/CLI. Metadata-first planning, capillary units,
    PulseWeave state-budget evidence, route-aware admission, and timing-surface separation all
    apply. Provider adoption must be feature-gated inside `shardloom-vortex`, version-recorded, and
    certificate-backed; external Vortex query-engine integrations remain prohibited.
  - Execution checklist:
    - [ ] Add or refresh the Vortex 0.75 provider utilization report so every row below exposes a
      stable `ship`, `drop`, or `pending` disposition and the evidence needed to change it.
    - [ ] `ship/drop: vortex.like/not_like dictionary/FSST reducers`:
      - [ ] Keep the already useful route-aware LIKE pushdown for count/filter and wide row-ref
        top-K lanes only when focused evidence stays positive.
      - [ ] Keep grouped aggregate lanes off this pushdown until dictionary-code exact recount or
        provider-backed grouped evidence proves it is faster and exact.
      - [ ] Evidence: focused Rust route-aware LIKE tests plus targeted Q21/Q24 UAT; retain only if
        the route improves or stays neutral without broadening string residual work.
    - [ ] `ship/drop: grouped count/sum kernels`:
      - [ ] Test upstream grouped `count` and primitive `sum` as providers inside ShardLoom
        capillary grouped aggregate work units, not as a separate Vortex SQL/group-by route.
      - [ ] Compare against existing compact/dictionary/radix aggregate paths for Q17/Q19/Q33/Q34/
        Q35-style state shapes.
      - [ ] Ship only if the provider reduces state/update/merge cost without losing null/key
        semantics; otherwise drop and keep the ShardLoom-native grouped state.
    - [ ] `ship/drop: vortex.byte_length`:
      - [ ] Test provider-backed `byte_length` for SQL/Python/DataFrame `length`/`byte_length`
        transforms and Q28/Q29-style lanes.
      - [ ] Verify UTF-8 byte-versus-character semantics, binary behavior, nullability, overflow,
        and decoded-reference parity before shipping.
      - [ ] Ship only if it materially reduces transform cost or encoded/materialized work;
        otherwise keep the current ShardLoom transform-code path.
    - [ ] `ship/drop: dictionary take/filter/mask reducers`:
      - [ ] Test dictionary `take`, `filter`, and mask reducers for final-K materialization,
        candidate filtering, and retaining dictionary codes through row-ref paths.
      - [ ] Reuse the same path from SQL/Python/DataFrame/CLI after Vortex normalization.
      - [ ] Ship only if it reduces decode/materialization or final-K reread work with exact output.
    - [ ] `ship/drop: layout-reader/file cache and file-stat pruning`:
      - [ ] Centralize safe layout-reader/file-open cache reuse for sequential native Vortex scans.
      - [ ] Emit evidence that distinguishes file/layout cache reuse from query-result caching.
      - [ ] Ship only for source-fingerprint-safe reuse; drop any cache shape that could preserve
        query answers or stale prepared state.
    - [ ] `ship/drop: writer/encoding policy surfaces`:
      - [ ] Test Vortex 0.75 zstd/binary/row-encoder/interleave-related writer options through
        the existing single `.vortex` artifact writer policy.
      - [ ] Measure artifact size, prepare/load time, and affected query lanes together.
      - [ ] Ship only if it reduces size or write time without slowing retained query lanes; drop
        any compression/encoding profile that makes load or hot lanes worse.
    - [ ] `drop/document: JSON, geospatial, GPU/device, and DataFusion 0.75 surfaces`:
      - [ ] Keep JSON/geospatial as translation/preservation candidates only unless a separate
        generic runtime contract is added.
      - [ ] Keep GPU/device paths future-track only until device residency, packaging, and
        no-fallback certificates exist.
      - [ ] Keep DataFusion-related Vortex changes as baseline/oracle-only and never route
        ShardLoom runtime work through them.
    - [ ] Run focused Rust tests for every adopted provider and targeted UAT on affected lanes
      before retaining the optimization; do not run full workspace/full ClickBench until the
      cohesive provider batch is complete.
  - Acceptance: any adopted Vortex 0.75 surface is used through the shared native provider boundary,
    improves or preserves measured performance for its target lane, reports no-fallback evidence,
    and avoids route proliferation.
  - Evidence required: local pinned-source citations in docs or report rows, focused Rust tests,
    targeted UAT for affected lanes, and no broad workspace/full ClickBench run until this cohesive
    implementation batch is complete.
  - Non-goals: no Vortex SQL route, no `vortex-datafusion`, no query-answer sidecars, no GPU/device
    claim, no broad JSON/geospatial runtime claim, and no provider adoption that is slower than the
    previous ShardLoom-native path.
  - Claim boundary: Vortex 0.75 provider opportunity and local UAT evidence only until benchmark
    artifacts are refreshed.

- [x] `CLICKBENCH-STRING-HEAVY-HITTER-EXACT-RECOUNT-11` Remove the second broad string scan from
  `string_heavy_hitter_topk` lanes while preserving exact `ORDER BY ... LIMIT` results.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_current_branch_physical_policy_20260627T231853Z/summary.json`
    shows `CB-Q35` `33.017s`, `CB-Q34` `31.052s`, and `CB-Q23` `11.161s`; all use the
    `string_heavy_hitter_topk` policy and current evidence includes second-pass exact recount.
  - Five material ideas reviewed:
    - [x] Dictionary-code exact recount: retain candidate dictionary codes and recount over code
      vectors instead of UTF-8 values.
    - [x] Candidate-code chunk membership: derive candidate-code membership so non-candidate chunks
      are skipped in the exact pass with explicit skipped-row evidence.
    - [x] Heavy-hitter sketch with proof-bound thresholds: stop widening the candidate set once a
      retained-bound proof is exact.
    - [x] Interned group-key arena reuse: share string key storage across first pass, exact recount,
      and output materialization.
    - [x] Functional-dependency pruning for deterministic reversible dependency families already
      admitted today: retain the source key for constant/additive-offset derived groups and
      reconstruct the dependent output values without broadening the key.
    - [x] Do not promote artifact-backed string dependency pruning unless embedded dictionary
      relation metadata proves reversible string-derived outputs without scanning or approximation;
      current code keeps this out of the hot route rather than adding an unsafe approximation.
    - [x] Sparse `AND` predicate candidate-set filtering for Q23-style residual predicates: after
      an exact selective predicate produces a small candidate set, later predicates evaluate only
      that candidate set. This is a shared predicate helper, not a Q23-specific route.
  - Ship/drop checklist:
    - [x] Implement dictionary-code exact recount for string heavy-hitter top-K groups.
    - [x] Add candidate-code chunk skip evidence and skip accounting.
    - [x] Emit exactness evidence proving no query-answer cache, no sidecar, no external engine, and
      no approximate result.
    - [x] Run targeted UAT for `CB-Q23`, `CB-Q34`, and `CB-Q35` against the latest retained baseline.
      Evidence:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_phase_batch_20260628T003354Z/summary.json`
      and
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_shipdrop_batch_20260628T011000Z/summary.json`.
    - [x] Ship if at least one tail lane materially improves and no target lane regresses beyond
      run-variance tolerance; otherwise drop/revert the approach and record why.
      Decision: ship the Q34/Q35 string heavy-hitter slice (`31.052s` -> `26.625s`,
      `33.017s` -> `26.560s` in the retained ship/drop batch) and keep the sparse predicate helper
      as a low-risk shared improvement. Do not claim Q23 is solved by the string heavy-hitter slice:
      it remains a sparse count-distinct/top-K predicate lane (`11.161s` retained baseline,
      `12.473s` one-query rerun, no fallback) whose material next lever is embedded dictionary/
      membership pruning inside the single `.vortex` artifact.
  - Non-goals: no query-specific summary, no sidecar, no approximate top-K, and no DataFusion/
    DuckDB/Spark/Polars/pandas fallback.
  - Status: completed and recorded in `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `CLICKBENCH-TRANSFORM-CODE-MAP-12` Make transformed dictionary aggregates operate on reusable
  transform-code maps rather than repeated per-row/per-value string transforms.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q29` `17.439s` on
    `transformed_dictionary_aggregate`; the route is correct, but the transformed grouping family
    is still a tail contributor.
  - Five material ideas reviewed:
    - [x] Persist and consume dictionary-derived transform code maps for `length`, `url_domain`,
      minute/date bucket, and admitted derived families inside the single `.vortex` artifact when
      the source exposes dictionary or typed time arrays; row-string transforms remain only for
      non-dictionary sources.
    - [x] Aggregate over bounded transformed dictionary keys, not repeated transformed strings,
      with visible value reconstruction from keys at output.
    - [x] Share run-local transform-code/key maps between grouped aggregate states reached through
      SQL, Python, DataFrame-style helpers, and CLI after Vortex normalization.
    - [x] Add transform null/error/overflow contracts so invalid or null transform inputs stay
      exact and deterministic.
    - [x] Keep transform reuse below the front-door surface through bounded run-local transform-key
      maps and artifact-derived column rewrites; do not add a process-global query-answer cache or
      unbounded transform cache.
  - Ship/drop checklist:
    - [x] Add a generic run-local transform-code/key map contract for admitted dictionary-derived
      transforms.
    - [x] Rewire transformed dictionary aggregate state to group/merge over transformed keys.
    - [x] Emit transform-code evidence: transform family, map entries/cap/hits/misses/saturation,
      null handling, and materialization boundary.
    - [x] Run targeted UAT for `CB-Q29` against the latest retained baseline. Evidence:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_shipdrop_batch_20260628T011000Z/summary.json`
      (`17.439s` -> `15.349s`, no fallback, no external engine).
    - [x] Ship if runtime materially improves or remains neutral with materially lower string
      transform/materialization evidence; otherwise drop/revert and record why.
  - Non-goals: no arbitrary regex engine fallback, no Python callable/UDF shortcut, and no
    transform result sidecar.
  - Status: completed and recorded in `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `CLICKBENCH-HIGH-CARDINALITY-RADIX-CAPILLARY-13` Redesign high-cardinality exact aggregate
  state around radix/capillary partitioned partials and merge-local packed keys.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q17` `17.177s`, `CB-Q19` `11.074s`, and `CB-Q33`
    `15.042s`; the current policy correctly separates `numeric_utf8_heavy_hitter_topk` from
    `near_input_cardinality_numeric_pair_aggregate`, but state construction and merge locality
    remain material.
  - Five material ideas reviewed:
    - [x] Prefer chunk-local capillary partials over a new radix partitioner for the current
      single-node path: the implemented slice compacts repeated numeric-pair chunks, preserves
      packed keys, and avoids adding another partition/merge layer until UAT shows it is material.
    - [x] Segment/chunk-local capillary partials with deterministic merge order and bounded memory
      accounting for repeated numeric-pair chunks.
    - [x] Packed composite keys for numeric/numeric, numeric/string, and dictionary-code/string
      groups.
    - [x] Late measure materialization for retained candidate groups, especially numeric-pair
      second-pass measure lanes.
    - [x] Memory-budgeted exact spill boundary that fails closed until certified native spill is
      available.
  - Ship/drop checklist:
    - [x] Implement capillary partial aggregation for repeated high-cardinality numeric-pair chunks.
    - [x] Add packed-key merge evidence and memory-pressure accounting shared by SQL/Python/
      DataFrame/CLI.
    - [x] Preserve exact count, sum, avg, min, max, count-distinct, HAVING, ORDER BY, LIMIT, and
      OFFSET semantics.
    - [x] Run targeted UAT for `CB-Q17`, `CB-Q19`, and `CB-Q33` against the latest retained baseline.
      Evidence:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_phase_batch_20260628T003354Z/summary.json`
      and
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_shipdrop_batch_20260628T011000Z/summary.json`.
    - [x] Ship if runtime or memory pressure materially improves without widening candidate-window
      regressions; otherwise drop/revert and record why.
      Decision: ship the numeric-pair direct-key/late-measure slice because Q33 remains better than
      the retained `15.042s` baseline (`12.028s` in the first targeted batch, `14.055s` in the
      ship/drop rerun) with exact/native/no-fallback evidence. Q17 and Q19 are treated as neutral
      variance lanes, not new performance claims.
  - Non-goals: no approximate aggregates, no hidden external engine, no query-specific partition
    plan, and no uncertified spill writes.
  - Status: completed and recorded in `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `CLICKBENCH-ROW-REF-TOPK-SEGMENT-PRUNING-14` Add segment/block order-key pruning before
  row-ref top-K materialization.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q24` `14.065s`; the route already uses
    `row_ref_sort_topk`, but still scans broadly before final materialization.
  - Five material ideas reviewed:
    - [x] Use the current embedded footer/file pruning and row-position locality where exact today;
      do not claim pre-read order-key segment pruning because current Vortex footer inventory does
      not expose exact per-order-key segment min/max metadata for this route.
    - [x] Persist/consume source-order locality metadata so retained row refs can seek fewer chunks during
      final materialization.
    - [x] Maintain a monotonic top-K threshold that tightens as candidate rows are discovered and
      skips candidate construction for chunks that cannot enter the retained window.
    - [x] Split sort into key-only candidate scan and selected-row materialization with bounded
      row-ref buffers.
    - [x] Drop block-level order-key metadata from this PR because adding a parallel non-Vortex
      block index would violate the single `.vortex` artifact direction; retain the implemented
      chunk-threshold pruning and revisit only if Vortex exposes embedded block stats.
  - Ship/drop checklist:
    - [x] Confirm current single `.vortex` artifact evidence exposes footer/file pruning and
      row-position locality, but not exact per-order-key segment min/max sufficient for pre-read
      segment pruning.
    - [x] Implement conservative chunk-threshold pruning for bounded `ORDER BY ... LIMIT` after the
      key-only scan has an exact retained window.
    - [x] Emit pruning evidence: threshold-pruned chunks/rows, retained row refs, and final
      materialization boundary.
    - [x] Run targeted UAT for `CB-Q24` plus `CB-Q25`-`CB-Q27` regression guards. Evidence:
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_shipdrop_batch_20260628T011000Z/summary.json`
      (`CB-Q24` `14.065s` -> `13.563s`, `CB-Q25` `0.629s` -> `0.469s`,
      `CB-Q26` `2.871s` -> `2.498s`, `CB-Q27` `3.285s` -> `2.678s`; all no fallback/no external
      engine).
    - [x] Ship if `CB-Q24` materially improves and `CB-Q25`-`CB-Q27` stay neutral; otherwise
      drop/revert and record why.
  - Non-goals: no order-changing approximation, no precomputed query result, and no broad sort
    materialization before pruning.
  - Status: completed and recorded in `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15` Reduce prepare/load time by improving
  writer segment economics and single-pass ingest accounting.
  - V1 scope classification: `required_for_v1`.
  - Source: replacement ingest evidence records prepare/load as writer-dominant, with previous
    prepare around `515s` and Vortex write/segment write around `455s`.
  - Iteration ledger: `docs/architecture/clickbench-ingest-optimization-ledger.md`.
  - Five material ideas reviewed:
    - [x] Adaptive larger row blocks and fewer segments based on source shape, target pruning value,
      and memory envelope.
    - [x] Coalesced typed-batch handoff into the Vortex writer with bounded queues and ordered
      atomic commit.
    - [x] Source-native Parquet/Arrow dictionary preservation and CSV/JSONL projection-aware typed
      builders.
    - [x] Single-pass write/digest: the product Vortex writer uses the workspace-safe streaming
      SHA-256 digest and reports `digest_micros=0`, so output bytes are not reread for artifact
      digest after commit.
    - [x] Writer timing split for read, typed build, derived metadata, encode/layout, segment write,
      footer/register, digest, and reopen verification.
    - [x] Size-aware layout inventory defer: large public prepares use the upstream Vortex writer
      row-count summary plus streaming artifact digest as prepare-time proof and defer expensive
      layout inventory opening until query/open time. This preserves the single `.vortex` artifact,
      avoids public sidecars, and keeps small fixtures on the stricter reopen path.
    - [x] Prefer source-native dictionary/typed-time derived metadata for large columnar inputs so
      ClickBench-style Parquet ingest does not default to full per-row hidden UTF-8 length/domain
      synthesis when the adapter exposes dictionary-backed or typed-time derived metadata.
  - Ship/drop checklist:
    - [x] Implement adaptive row-block/segment sizing and coalesced writer handoff behind the
      existing single `.vortex` artifact path.
    - [x] Add timing-surface fields that isolate segment write, footer/register, digest, and reopen
      costs.
    - [x] Preserve ordered atomic replacement and no-sidecar behavior.
    - [x] Add explicit evidence for `writer_summary_row_count_verified_layout_inventory_deferred`
      so deferred large-artifact inventory cannot be mistaken for missing proof or hidden fallback.
    - [x] Add a reusable gated local CLI ingest UAT runner
      (`scripts/run_clickbench_ingest_uat.sh`) so ship/drop passes run the ShardLoom CLI directly,
      file-back stdout/stderr, enforce artifact-size/runtime/stable-idle cutoffs, and stop
      measuring harness pipe blocking as engine runtime.
    - [x] Add a minimum-progress gate for ingest experiments so a profile is dropped early when it
      remains CPU-heavy but emits negligible artifact bytes after a configured window.
    - [x] Test and drop the selective source-text compression profile that kept fast Zstd only on
      high-value URL/search/title/free-text payload columns: replacement ingest returned code `137`
      after `215s` and produced no canonical `.vortex` target, so the prior broad source-text
      fast-Zstd profile remains the active baseline.
    - [x] Emit writer compression field-count and field-name evidence so selective profiles can be
      verified from public preparation output without opening code.
    - [x] Run replacement ingest UAT for this batch and record load time, artifact size, segment
      count, and writer evidence in
      `docs/architecture/clickbench-ingest-optimization-ledger.md`.
    - [x] Ship/drop this batch: retained the restored broad source-text fast-Zstd profile with
      `421s` local CLI replacement evidence; dropped the selective payload-only compression profile
      after return code `137`.
    - [x] Test and drop the ultra row-block/segment-economy profile: it reduced isolated load time
      to `360s` and segment count to `25038`, but regressed the saved Q25 row-order/top-K guard to
      `13.734s`, so it was reverted and the canonical artifact was restored with
      `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T040813Z`
      (`390s`, `34.93GB`, `36660` segments).
  - Non-goals: no duplicate massive artifacts, no multi-file OLAP sidecar, and no source-format
    route fork that bypasses Universal Ingest.
  - Status: completed for the current batch; future ingest improvements must be promoted as larger
    writer/layout architecture changes, not field-list-only tweaks.

- [x] `CLICKBENCH-ARTIFACT-SIZE-ENCODING-POLICY-16` Reduce single-artifact size without making
  load or query runtime worse.
  - V1 scope classification: `required_for_v1`.
  - Source: current local artifact is about `34.9GB` from a `14.8GB` Parquet source; ClickBench
    scoring includes storage size, and large artifacts also increase scan/write pressure.
  - Five material ideas reviewed:
    - [x] Replace eligible full hidden derived columns with dictionary-derived metadata and compact
      code maps where exact semantics allow.
    - [x] Use column-family encoding policy: dictionary/Zstd for high-value text, fast compact
      encodings for numeric/derived metadata, and low-effort load profile where compression is not
      worth the CPU.
    - [x] Deduplicate derived URL/domain/length work at the source-dictionary boundary: Universal
      Ingest derives length/domain from source dictionary values once and remaps row codes instead
      of rescanning repeated strings when the adapter exposes dictionary arrays.
    - [x] Add layout advisor feedback that reports size contribution by column family and hidden
      metadata family.
    - [x] Drop optional compact/repack mode from active v1 work; it is a later packaging/storage
      mode, not required for the default fast-load runtime path.
  - Ship/drop checklist:
    - [x] Add size-attribution evidence for source columns, derived metadata, dictionaries, and
      footer/layout metadata.
    - [x] Convert eligible hidden derived columns to compact dictionary/code metadata inside the
      `.vortex` artifact.
    - [x] Add column-specific compression policy with explicit load/runtime tradeoff evidence.
    - [x] Run replacement ingest UAT and compare artifact size and load time against the latest
      retained baseline for this batch.
    - [x] Ship/drop this batch: retained the `34.93GB` broad source-text artifact because load time
      improved materially to `421s`; did not ship the selective payload-only size experiment because
      it failed before producing a canonical artifact.
    - [x] Drop the ultra segment-economy profile despite the slight artifact-size reduction
      (`34.86GB`) because it regressed row-order/top-K query runtime; size-only wins are not enough.
  - Non-goals: no external compression-only artifact, no post-query compacting, and no hidden
    multi-file index.
  - Status: completed for the current batch; retained baseline remains the broad source-text
    `34.93GB` single `.vortex` artifact.


## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
