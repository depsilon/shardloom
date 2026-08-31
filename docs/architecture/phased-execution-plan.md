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

1. Finalize `RUNTIME-EVIDENCE-COLLECTOR-1`; the shared native Vortex loops now expose
   control-plane/evidence micros plus compact split-signature evidence while preserving replay
   split records.
2. Close `AGGREGATE-PARTIAL-PARALLELISM-1` around the existing shared capillary aggregate runtime:
   chunk dictionary counts, transformed dictionary code-pair partials, materialized string partials,
   count/sum/avg compact state, top-K retained windows, and state-budget evidence are the current
   shipped path. Do not add a second cross-thread aggregate state fork until targeted evidence shows
   it beats the existing route without Q33/Q34/Q35-style regressions.
3. Keep `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active as the standing owner for unchecked global
   architecture runtime-gap families until those rows are closed or promoted into concrete runtime
   work.
4. Treat `UNIVERSAL-INGEST-WRITER-RESOURCE-ARTIFACT-1` and
   `SOURCE-FINGERPRINT-POLICY-1` as implemented rows waiting only for post-merge ledger movement.
5. Do not keep provider-bound experiments in the active queue. `EXACT-STRING-SUMMARY-LAYOUT-1` is
   closed as a current Vortex 0.75 provider-bound drop decision because the public writer exposes
   standard file statistics but no stable arbitrary in-file frequency-summary payload; re-open only
   if upstream Vortex adds a stable in-file metadata/custom-stat provider or ShardLoom has an
   approved single-file extension-column design that does not bloat or regress load/query time.
6. Keep optimized build profiles out of the active runtime queue. The PGO/native benchmark lane is
   already completed in the ledger under `GAR-PERF-2H`; optional allocator experiments need fresh
   evidence before becoming active release work.
7. After the next targeted UAT or review packet identifies a concrete, feasible shared-runtime gap,
   promote that work from the standing owner into a checklist item before implementation.
8. Heavy replacement ingest, full 43-query ClickBench UAT, and broad release gates run at the end
   of a cohesive implementation batch, not after every evidence cleanup.

- [x] `UNIVERSAL-INGEST-WRITER-RESOURCE-ARTIFACT-1` make Universal Ingest writer/runtime policy
  honor the shared resource envelope and reduce single-artifact bloat.
  - V1 scope classification: `required_for_v1`.
  - Source: Desktop 100M ClickBench replacement-ingest evidence on `2026-08-30`, where
    `prepare_once` was writer dominated (`vortex_write/segment_write` hundreds of seconds) and
    artifact size was materially larger than the official Parquet source even though query routing
    was clean.
  - Prior state: public prepare routes passed `max_parallelism` into SourceState and
    layout-advisor evidence, but the Vortex writer runtime still drove through a single-thread
    blocking runtime; hidden derived metadata was compact for dictionary-shaped batches but could
    still be emitted as physical hidden columns where no compact source dictionary/code boundary
    existed; timing folded compression into segment write and derived metadata into stream source
    pull.
  - ShardLoom technique review: PulseWeave/resource policy applies directly; capillary ingest
    should size source/read/derive/encode/write units from one resource envelope; dynamic work
    shaping should select writer runtime, row-block/coalescing, and compression policy from source
    shape; metadata-first execution requires compact dictionary/code-derived metadata where
    possible; timing-surface discipline requires source read, decode/derive, Arrow-to-Vortex,
    compression, segment write, and final commit to be visible without changing claims.
  - Execution checklist:
    - [x] Wire `VortexLayoutWriteRuntimeDecision` writer parallelism into the actual Vortex writer
      runtime instead of always using a single-thread runtime.
    - [x] Reuse the shared local resource-envelope writer row-block/coalescing defaults for
      layout-advisor decisions instead of duplicating hardcoded ingest-only constants.
    - [x] Preserve dictionary/code-derived hidden metadata for columns that already arrive as
      dictionaries and make large non-dictionary derived metadata selection explicit in evidence.
    - [x] Add a lean source-native runtime metadata profile for large product columnar ingest so
      runtime-critical URL, Referer, SearchPhrase, and EventTime helpers are retained while broader
      candidate columns are omitted unless a cheaper adapter path is available.
    - [x] Rework text compression overrides so only source-schema-admitted UTF-8 and
      dictionary-UTF8 payload/text columns pay explicit fast-Zstd compression cost while numeric and
      generated derived metadata stay on typed layout paths.
    - [x] Replace static benchmark-specific compression field lists with dynamic advisor-selected
      candidates derived from source column names and Arrow dtypes.
    - [x] Add measured timing fields for derived metadata build, Arrow-to-Vortex conversion,
      Vortex compression/encode work, segment write, and final commit/workspace stage.
    - [x] Update tests and readiness expectations for writer runtime parallelism, compression
      field selection, compact derived metadata evidence, and timing split fields.
    - [x] Run focused validation for the changed Rust/Python evidence surfaces.
    - [x] Rebuild the local release CLI and rerun a targeted Desktop ingest/UAT pass to compare
      prepare/load time, artifact size, and no-fallback evidence.
    - Post-merge ledger movement remains after this branch lands.
  - Acceptance: public prepare/load evidence shows the requested/applied writer runtime
    parallelism, product-local Vortex writer admission policy, lean compact derived metadata policy
    for large columnar sources, selected source-schema compression fields, stage timing splits,
    single `.vortex` artifact persistence,
    `fallback_attempted=false`, and
    `external_engine_invoked=false`; artifact-size and load-time changes are retained only if the
    targeted retest does not show a meaningful regression.
  - User-visible surface: CLI/public workflow prepare evidence, package/Homebrew runtime behavior,
    and Desktop ClickBench UAT artifacts.
  - Implementation scope: `shardloom-vortex/src/vortex_ingest.rs`,
    `shardloom-vortex/src/universal_format_io.rs`, `shardloom-cli/src/sql_local_source_runtime.rs`,
    focused tests, and architecture docs.
  - Evidence required: focused Rust tests plus targeted ingest/UAT evidence after rebuild.
  - Verification: `cargo test -p shardloom-vortex --lib <focused filters>`,
    `cargo test -p shardloom-cli --bin shardloom <focused filters>`, then a bounded Desktop
    replacement-ingest/UAT run when the build is ready.
  - Non-goals: no benchmark-page rebuild, no query-result sidecars, no external query-engine
    fallback, and no full workspace suite until the cohesive ingest batch is complete.
  - Claim boundary: ingest/write resource-policy and artifact-shape evidence only; no superiority
    claim until a full benchmark/UAT refresh supports it.
  - Fallback boundary: no DuckDB, Polars, pandas, Spark, DataFusion, or Vortex query-engine
    integration executes runtime work.

- [x] `SOURCE-FINGERPRINT-POLICY-1` make public prepare source fingerprinting metadata-first by
  default with explicit content-digest opt-in.
  - V1 scope classification: `required_for_v1`.
  - Source: Desktop 100M ClickBench replacement-ingest UAT on `2026-08-30`, which showed public
    `prepare dataframe` spending several minutes in `fingerprint_local_source_file_with_budget_report`
    reading the full source before any `.vortex` write progress.
  - Current state: source identity is useful evidence, but a mandatory full source-content digest is
    too expensive for normal public/local prepare. Prepared artifact digests and Vortex writer
    evidence remain mandatory. Full source-content digest should be an explicit proof-tier opt-in,
    not default route work.
  - ShardLoom technique review: metadata-first execution applies directly. PulseWeave policy should
    choose the source identity tier before ingest; capillary ingest must not be blocked by an
    up-front whole-file read; timing-surface evidence must separate metadata scout time from optional
    content digest time.
  - Execution checklist:
    - [x] Add a public prepare source fingerprint policy with `metadata_only` default and
      `content_digest` opt-in.
    - [x] Thread the policy through CLI `prepare dataframe`, public workflow route/run, and
      Universal Ingest request evidence.
    - [x] Emit machine-readable evidence for fingerprint kind, policy, identity source, and whether
      full source-content fingerprinting was requested/performed.
    - [x] Reuse Parquet `ArrowReaderMetadata` across row-group workers and use metadata-first
      Parquet reader options during source discovery.
    - [x] Update architecture docs so SourceState identity is described as metadata-first by
      default with explicit content-digest proof.
    - [x] Add or update focused regression tests for default metadata-only public prepare and
      explicit content-digest opt-in.
    - [x] Run focused validation and rebuild the release CLI before replacement-ingest UAT.
    - [x] Rerun the gated Desktop replacement-ingest UAT probe; the harness now fails fast when the
      source file is sparse/nonresident instead of entering ShardLoom with misleading zero-progress
      evidence.
    - [x] Rerun the full Desktop replacement-ingest UAT after the official ClickBench source is
      physically materialized locally and confirm the route begins streaming/writing without a
      full-source fingerprinting stall.
    - Post-merge ledger movement remains after this branch lands.
  - Acceptance: public local prepare defaults to `source_fingerprint_policy=metadata_only`,
    `source_content_fingerprint_performed=false`, and no full source read during source identity
    scout for columnar sources; `--source-fingerprint-policy content_digest` still performs an
    explicit full content digest with evidence.
  - User-visible surface: CLI/public workflow evidence and local prepare behavior.
  - Verification: focused Rust check/test targets for `shardloom-cli` ingest/public workflow paths
    plus one Desktop replacement-ingest UAT run.
  - Claim boundary: source identity policy only; no benchmark or superiority claim.
  - Fallback boundary: no external engine fallback; blocked and admitted paths keep
    `fallback_attempted=false` and `external_engine_invoked=false`.

- [x] `AGGREGATE-PARTIAL-PARALLELISM-1` close the feasible associative partial aggregate work on
  the existing shared capillary grouped aggregate runtime.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - Source: external review packet `2026-08-31` and UAT showing Q17, Q19, and Q29 still spend
    meaningful time after Vortex scan concurrency because aggregate state mutation is mostly serial.
  - Current state: Vortex scans use bounded concurrency and the grouped aggregate runtime already
    selects merge-safe chunk-local partial paths for dictionary counts, transformed dictionary
    code-pair measures, materialized string partials after direct-provider misses, compact
    count/sum/avg state, top-K retained windows, exact dictionary/count-distinct paths, and
    state-budget evidence. Prior fixed partitioning for Q33 regressed and must not be revived as a
    generic split.
  - Intake review: accepted only as a selective shared-runtime closure. A second cross-thread
    aggregate engine is not active because it would duplicate the existing grouped-state runtime and
    has a known regression risk on near-input-cardinality and heavy-hitter lanes.
  - ShardLoom technique review: capillary units should split chunk-local aggregate work, PulseWeave
    should share the worker budget with Vortex scan/decode, dynamic work shaping should admit only
    shapes where partial states reduce merge volume, and ProofBound must block unsafe order-sensitive
    or count-distinct shapes until correctness evidence exists.
  - Execution checklist:
    - [x] Keep the physical-policy classifier route-aware for scalar, transformed dictionary,
      numeric-pair, numeric/UTF-8 heavy-hitter, string heavy-hitter, and general grouped aggregate
      families.
    - [x] Exclude Q33-like near-input-cardinality, source-order-sensitive, general distinct, and
      proofbound heavy-hitter lanes from any broad worker-state fork.
    - [x] Use existing bounded capillary partial paths inside the shared grouped-state runtime
      instead of adding a duplicate aggregate route.
    - [x] Emit deterministic route evidence for aggregate update strategy, compact group-state
      strategy, accessor/materialization posture, spill posture, capillary work units, PulseWeave
      pressure signals, and rejected/provider-bound alternatives.
    - [x] Preserve focused correctness coverage for nulls, empty groups, duplicate keys,
      count-distinct, top-K/source-order, transformed dictionary groups, materialized string
      partials, and compact numeric measures.
    - [x] Drop the generic cross-thread worker-local fork for this batch because it is not a clear
      material improvement and would risk reintroducing the prior Q33 regression.
    - Post-merge ledger movement remains after this branch lands.
  - User-visible surface: all admitted SQL/Python/DataFrame grouped aggregate calls that lower into
    the shared native Vortex aggregate runtime.
  - Implementation scope: `shardloom-vortex/src/local_primitives.rs`, physical-policy helpers,
    focused tests, and docs/evidence summaries.
  - Evidence required: aggregate parity tests, no-fallback route evidence, and targeted UAT.
  - Acceptance: shared capillary partial aggregation is selected only for proven merge-safe routes,
    while Q33-like and heavy-hitter routes keep their specialized policies instead of receiving a
    slower generic split.
  - Verification: focused `shardloom-vortex` aggregate tests; Desktop UAT remains an end-of-batch
    verification step, not a prerequisite for adding a duplicate route.
  - Non-goals: no external parallel execution engine, no Q33 fixed-partition retry, no unsafe
    source-order or distinct-state parallelization.
  - Claim boundary: workload-scoped query optimization evidence only.
  - Fallback boundary: no fallback/external engine execution.

- [x] `RUNTIME-EVIDENCE-COLLECTOR-1` measure and compact per-chunk runtime evidence construction
  without weakening proof depth.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - Source: external review packet `2026-08-31`; candidate control-plane overhead from per-chunk
    split/kernel evidence construction in scan loops.
  - Current state: evidence records are useful and proof-preserving, but repeated string/layout/
    encoding construction may be measurable on fast or highly segmented routes.
  - Intake review: accepted as measurement-first. It should be dropped if a no-op collector probe
    shows less than `2-3%` runtime cost.
  - ShardLoom technique review: evidence-tier controls apply directly; hot runtime should aggregate
    repeated signatures while full replay/publication proof can retain deeper records; timing
    surfaces must expose control-plane and evidence collection time separately.
  - Execution checklist:
    - [x] Add explicit `control_plane_micros` and `evidence_collection_micros` fields for relevant
      native Vortex runtime loops.
    - [x] Add a compact signature collector keyed by layout/encoding signatures with counts,
      representative first/last splits, row totals, and child/buffer totals.
    - [x] Preserve full per-chunk evidence for replay/publication proof tiers where required.
    - [x] Add focused tests for metadata-pruned and scanned local Vortex paths plus explicit compact
      collector signature behavior.
    - [x] Ship compact collection as an evidence-size/control-plane clarity improvement, with
      runtime overhead exposed through route fields rather than hidden in query timing.
    - [ ] Move this item to the completed ledger after merge.
  - User-visible surface: route evidence JSON and diagnostics.
  - Implementation scope: native runtime evidence collectors, timing fields, tests, docs.
  - Evidence required: focused tests and timing comparison.
  - Acceptance: proof semantics remain intact and evidence collection cost is measurable,
    controlled, and non-regressing.
  - Verification: focused evidence tests and targeted fast/segmented-route UAT.
  - Non-goals: no removal of required certificate evidence and no benchmark-only proof weakening.
  - Claim boundary: evidence-overhead clarity only.
  - Fallback boundary: no runtime fallback involved.

- [ ] `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active owner for unchecked global architecture runtime
  gaps.
  - V1 scope classification: `required_for_v1`.
  - Source: `scripts/check_runtime_gap_family_burn_down.py`,
    `docs/architecture/global-architecture-review.md`, and the release/readiness validators that
    require active ownership for unchecked global review rows.
  - Current state: this is a governance owner, not a separate implementation surface. Runtime
    gap-family mappings preserve provenance back to completed GAR items, but unchecked global
    architecture review rows still need a current active owner while concrete runtime work remains
    open or external-gated.
  - ShardLoom technique review: evidence-tier controls and no-fallback discipline apply. Concrete
    implementation still belongs in shared Vortex-normalized runtime, ingest, operator, sink, or
    evidence components, not one-off route splits.
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
  - Claim boundary: governance traceability only; no runtime, performance, production, or
    superiority claim.
  - Fallback boundary: this owner does not execute runtime work and preserves
    `fallback_attempted=false` / `external_engine_invoked=false` in its validators.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
