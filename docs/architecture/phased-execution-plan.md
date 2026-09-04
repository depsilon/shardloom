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
When a work item is implemented, move the detailed completed record to
`docs/architecture/phased-execution-completed-ledger.md` and leave this section focused on open
checkbox work only.

Global-runtime rule: performance entries in this queue are shared ShardLoom engine/runtime
enhancements, not ClickBench-only fixes. Implement them below the SQL/Python/DataFrame/CLI front
doors after Vortex normalization so every admitted public surface benefits from the same prepared
Vortex layout, metadata-first planning, capillary work shaping, resource envelope, writer, and
evidence contracts. Retain an optimization only when focused or UAT evidence shows a material
improvement without correctness, no-fallback, or single-artifact regressions; otherwise drop or
revise it before moving on.

The 2026-09-02 completion-claim audit reopened the ClickBench domain-transfer optimization work as
real production implementation items. The prior ledgered work added useful policy, evidence,
front-door, and partial runtime improvements, but the fresh replacement-ingest UAT evidence proves
the writer/ingest overhaul was not complete:

- Retained pre-audit ingest baseline: `301s` local Desktop replacement-ingest wall time.
- Fresh current-branch rerun before the rejected tuning patch:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260902T201343Z`,
  `331s`.
- Rejected experimental tuning patch rerun:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260902T234603Z`,
  `360s`, with `vortex_write_millis=311716`,
  `universal_ingest_derived_metadata_build_millis=121578`,
  `vortex_compression_millis=102253`, and one `37,918,138,164` byte `.vortex` artifact.
- Current clean implementation UAT after the 2026-09-03 production implementation slice:
  replacement ingest
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260903T112348Z`
  completed in `271s` and produced one `38,147,848,068` byte `.vortex` artifact; full 43-query
  UAT
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_clean_ingest_current_impl_20260903T112902Z/summary.json`
  completed `129/129` CLI runs with best-of-3 query total `189.197s`, geomean `1.142898s`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.

These entries use the official ClickBench generated leaderboard data and public system docs as
directional strategy input only. ShardLoom local Desktop UAT is not official ClickBench hardware and
must not be reported as a leaderboard or superiority claim.

The 2026-09-03 material-optimization ideation intake is now incorporated below as implementation
packets rather than a separate idea list. The retained local reference point for this intake is
replacement ingest `271s`, full 43-query best-of-3 total `189.197s`, and the remaining slow rows:
`Q35 25.658s`, `Q34 25.657s`, `Q17 15.931s`, `Q29 10.730s`, `Q23 10.257s`,
`Q19 9.731s`, `Q33 9.721s`, and `Q10 8.909s`. Every `H`/`VH` idea is mapped into the existing
production ownership items so implementation can happen in shared ShardLoom/Vortex-normalized
components rather than duplicate ClickBench-only phase IDs. Borderline ideas are included only
where they ride on the same exactness, metadata, or scheduler contract and are retain/drop gated.

- [ ] `CLICKBENCH-PRODUCTION-WRITER-PHYSICAL-DESIGN-1` build a real production Universal Ingest
      writer physical-design pipeline instead of writer-policy constants.
  - Source: maintainer correction on 2026-09-02 that the domain-transfer writer overhaul was marked
    complete without a real writer architecture change; official ClickBench generated data showing
    load/size leaders benefit from load-time physical design; local UAT evidence above showing the
    current writer path is still the dominant long pole.
  - Current state: `vortex_ingest` has writer profile selection, row-block/coalescing constants,
    broad source-text Zstd overrides, worker-pool evidence, compression timing, and a streaming
    `ArrayIterator`. It still sends one ordered stream through one final
    `VortexSession::write_options().write(ArrayStream)` boundary, and the rejected 2026-09-02
    tuning patch worsened replacement ingest from the retained `301s` baseline to `360s`.
  - Intake review:
    - Accepted from the five technique review: treat ingest as physical design, not serialization.
    - Merged with prior writer/resource evidence rows: `UNIVERSAL-INGEST-WRITER-RESOURCE-ARTIFACT-1`,
      `GLOBAL-WRITER-ENCODE-COALESCING-1`, `CLICKBENCH-100M-INGEST-WRITER-COALESCING-10`, and
      `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15`.
    - Corrected audit disposition: those rows remain historical evidence/policy work and cannot be
      cited as production writer-overhaul completion.
  - V1 scope classification: `required_for_v1` for credible local production ingest posture and
    `v1_candidate_pending_feasibility` for any provider feature that requires upstream Vortex
    writer API changes.
  - ShardLoom technique review: apply dynamic work shaping to choose row-group tasks, row-block
    size, bytes-per-block, compression, and derived metadata budget; use capillary work units for
    source read, derived build, Arrow-to-Vortex conversion, compression, and final sink feed; use
    PulseWeave only at stage boundaries for backpressure/starvation evidence; keep metadata-first
    layout advice from Parquet/Vortex footers; keep evidence-tier controls so rejected tuning
    patches are automatically marked dropped.
  - Execution checklist:
    - [x] Add a `PhysicalIngestPlan`/`WriterPhysicalDesignPlan` data model with source footer facts,
      schema classes, source row-group/task topology, row-count/byte estimates, desired Vortex
      layout shape, compression plan, derived metadata plan, resource-envelope budget, and
      retain/drop thresholds.
    - [x] Replace hardcoded large-source writer constants with planner decisions that are stable,
      digestible, and explainable in public prepare evidence.
    - [ ] Implement H/VH ingest packet `decoupled ordered pre-writer pipeline`.
      - Ideas covered: decoupled ordered ingest pipeline, CPU-heavy pre-writer parallelism, ordered
        final Vortex sink feed, and production writer overhaul completion.
      - Current evidence: the clean `271s` ingest still reports `prepare_once=231107ms`,
        `vortex_write=230761ms`, `compression=100702ms`, `encode/write=130036ms`,
        `decode+derive=111319ms`, and the derived wrapper is inside
        `EmbeddedDerivedColumnRecordBatchReader::next()` while workers pull through a shared reader
        lock.
      - Implementation steps: split ingest into `SourceBatchTask`, `DerivedBatchTask`,
        `VortexArrayTask`, `CompressedLayoutTask`, and `OrderedWriterFeedTask`; move derived-column
        construction outside the shared reader lock; preserve source row order with sequence IDs;
        keep a bounded channel per stage; propagate deterministic errors and cleanup through the
        existing single-artifact commit boundary.
      - Tests/evidence: unit tests for ordering, bounded queues, early error propagation,
        source-lock release before derived work, and single-artifact cleanup; prepare evidence for
        per-stage wall/CPU time, queue depth, worker utilization, final writer wait, and
        `fallback_attempted=false` / `external_engine_invoked=false`.
      - Retain/drop gate: retain only if replacement-ingest UAT beats `271s` or improves stage
        attribution without artifact-size/query-UAT regression that the maintainer explicitly
        accepts.
      - Rejected 2026-09-03 implementation slice: an ordered embedded-derived prefetch reader moved
        URL/length/minute derived construction outside the shared writer pull lock, added bounded
        sequence-preserving derived channels, and raised the planned array-prefetch window for
        12-lane ingest from `4` to `8`. Focused `shardloom-vortex` ordering/error tests and the
        CLI Parquet prepare evidence test passed, but replacement-ingest UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260903T224544Z`
        completed in `361s` versus the protected `271s` clean-ingest reference with the same
        `38,147,848,068` byte artifact. The runtime change was dropped. Do not retry derived
        prefetch by adding another post-reader queue or widening the array queue unless the source
        row-group reader, derived workers, and Vortex writer share one measured lane/memory
        governor and the old run is cleaned before UAT.
    - [ ] Implement H/VH ingest packet `dictionary-lifted derived-column construction`.
      - Ideas covered: dictionary-lifted derivation, URL-domain and UTF-8 length computation against
        unique dictionary values, and reuse of derived dictionary ids across chunks where safe.
      - Current evidence: derived metadata build is `110037ms` while decode itself is only
        `1283ms`; current code computes appended derived columns per record batch rather than
        lifting transforms to source dictionaries and replaying codes.
      - Implementation steps: detect dictionary-compatible source columns before derive; compute
        `url_domain`, `utf8_length`, `extract_minute`, and `date_trunc_minute` over unique values or
        typed temporal dictionaries; build derived arrays by remapping codes with validity
        preserved; fall back only to ShardLoom-native row/batch derivation with explicit evidence
        when dictionary lifting is unsupported.
      - Tests/evidence: decoded-reference parity for empty, null, all-null, mixed-null,
        low-cardinality, high-cardinality, direct UTF-8, dictionary UTF-8, temporal, and unsupported
        source shapes; evidence fields for unique values transformed, codes replayed, rows avoided,
        and derived build millis by transform.
      - Retain/drop gate: retain only if it materially reduces derived-build time or enables the
        staged pipeline without correctness, null, ordering, or artifact regressions.
      - Rejected 2026-09-04 implementation slice: a large-OLAP Parquet schema-plan change requested
        Arrow dictionary arrays only for lean runtime derived text inputs (`URL`, `Referer`, and
        `SearchPhrase`) while leaving broader payload text, `Title`, and `OriginalURL` plain for the
        retained source-text Zstd writer profile. Focused dictionary-derived tests passed and the
        run used a candidate target so the protected `hits-parquet-100m.vortex` artifact remained
        intact. Candidate ingest
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260904T020627Z`
        was manually stopped after progress showed only `34.466GB` staged at `331s`; at the
        protected `271s` reference point it had staged only `19.633GB`, so it could not beat the
        retained `271s` full replacement ingest. The runtime change was dropped and the partial
        candidate artifact was cleaned. Do not retry large-source Arrow dictionary hints for source
        text columns unless the writer portfolio gate first proves whole-artifact size and write
        ramp behavior; the current better path is a native Vortex/ShardLoom compact derived
        metadata representation that does not force source text dictionary layout through the final
        writer.
      - Rejected 2026-09-04 implementation attempt: a hidden-derived plain-UTF8 batch replay cache
        specialized the existing `utf8_length`/`url_domain` builders for plain `Utf8`, `LargeUtf8`,
        and `Utf8View` batches, activated a bounded per-batch full-value cache only when a sample
        showed repeated source values, and preserved the same hidden derived dictionary output
        schema. Focused validation passed:
        `rustfmt --edition 2024 shardloom-vortex/src/universal_format_io.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io plain_utf8 -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io embedded_derived -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io streaming_vortex_write_preserves -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Side-by-side candidate replacement-ingest UAT used
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/bin/shardloom-plain-value-cache-candidate`
        with `--max-parallelism 12` and candidate target
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m-plain-value-cache-candidate.vortex`.
        The run was stopped at `301s` because the retained `271s` comparison point still showed
        `candidate_total_gb=0.000`, with progress samples at `120s`, `150s`, `181s`, `211s`,
        `241s`, `271s`, and `301s` all reporting `0.000GB` written. The temp candidate artifact was
        cleaned and the runtime change was reverted. Future dictionary-lift work should not add
        row-level replay caches inside the current embedded-derived reader; it must either lift from
        real source dictionaries without changing writer layout, or move hidden derivatives into a
        compact native metadata representation outside the row-batch writer payload.
      - Rejected 2026-09-04 implementation attempt: a narrower typed plain-UTF8 derived-builder
        specialization removed per-row dynamic UTF-8 array dispatch for plain `Utf8`, `LargeUtf8`,
        and `Utf8View` batches while preserving the same hidden dictionary output schema and the
        retained writer codec/profile. Focused validation passed:
        `cargo fmt --all -- --check`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io embedded_plain_utf8_paths_preserve_compact_derived_metadata -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io embedded_derived -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io plain_utf8 -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io streaming_vortex_write_preserves -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Candidate replacement-ingest UAT used the separate target
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m-typed-plain-derived-candidate.vortex`
        with logs at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260904T042153Z`.
        The run was stopped after `180s` because progress samples through `150s` still reported
        `candidate_total_gb=0.000` and no final target artifact, while the retained `271s`
        reference had already staged `18.016GB` at `151s`. The temp candidate artifact was cleaned
        and the runtime change was reverted. Do not retry plain-array micro-specialization inside
        the current embedded-derived reader unless it is paired with a measured native compact
        derived metadata representation or a writer API change that proves earlier byte emission.
    - [ ] Implement H/VH ingest packet `single resource governor`.
      - Ideas covered: one governor for source workers, derived builders, Arrow-to-Vortex
        conversion, compression, writer feed, and final Vortex sink pressure.
      - Current evidence: ingest reports source executor applied parallelism `11` and writer
        background workers `11`, but the slowest stage is still serialized work plus overlapping
        writer/compression timing rather than a globally shaped 12-core plan.
      - Implementation steps: add an `IngestResourceGovernor` derived from
        `VortexWriterPhysicalDesignPlan`; reserve lanes for final sink feed and compression; let
        source/derive/convert stages borrow idle capacity with starvation limits; expose memory
        reservations per queued batch and fail before OOM when bounded memory cannot be honored.
      - Tests/evidence: deterministic tests for `max_parallelism=1,2,3,12`, low-memory admission,
        writer-reserved lane behavior, queue backpressure, and cancellation/error release;
        evidence fields for requested/applied lanes by stage, idle/starved millis, max in-flight
        batches, memory budget, and pressure decisions.
      - Retain/drop gate: retain only if UAT shows improved ingest or stable ingest with clearer
        production-grade attribution needed to retain later writer/layout changes.
      - Rejected 2026-09-04 implementation attempt: an evidence/admission-only
        `VortexIngestResourceGovernorPlan` was threaded through `VortexWriterPhysicalDesignPlan`
        and public CLI prepare evidence, with source/array prefetch capped by the source executor,
        writer-slot reservation evidence, low-memory fail-closed admission before target creation,
        and no external-engine/fallback invocation. Focused validation passed:
        `cargo fmt --all -- --check`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io ingest_resource_governor -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io local_flat_columnar_stream_source_writes_without_buffered_batch_source -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io local_flat_columnar_stream_source_prefetches_vortex_arrays_when_parallelism_available -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-write,universal-format-io local_flat_columnar_stream_resource_governor_blocks_too_small_memory_budget -- --nocapture`,
        `cargo test -p shardloom-cli --features release-user-surfaces vortex_ingest_max_parallelism_propagates_to_public_prepare_evidence -- --nocapture`,
        `cargo clippy -p shardloom-vortex --features vortex-write,universal-format-io --all-targets -- -D warnings`,
        `cargo clippy -p shardloom-cli --features release-user-surfaces --all-targets -- -D warnings`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Candidate replacement-ingest UAT used
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/bin/shardloom-resource-governor-candidate`
        against the separate target
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m-resource-governor-candidate.vortex`
        with logs at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260904T053525Z`.
        The harness stopped at `181s` with `stop_reason=min_progress_gb_not_reached`,
        `candidate_total_gb=0.000`, `target_exists=false`, and `stdout_json_ok=false`, while the
        protected `271s` replacement-ingest reference had visible staged output before this point.
        The runtime and CLI evidence changes were reverted. Do not retry a governor-only evidence
        wrapper in the current writer path; the next viable ingest work should either change real
        source/derive/writer scheduling or use the retained layout/codec portfolio gate to prove a
        candidate before it reaches the public replacement path.
    - [ ] Implement H/VH ingest packet `retained layout/codec portfolio admission`.
      - Ideas covered: sampled layout/codec portfolio, derived-column admission, and load-time
        physical design without committing to unproven writer constants.
      - Current evidence: the rejected 2026-09-02 tuning patch regressed ingest to `360s`, so
        future layout/codec work needs an automatic ship/drop loop instead of manual constant
        tweaking.
      - Implementation steps: run tiny deterministic source samples through candidate writer
        profiles inside an isolated temp workspace; compare profile digest, artifact bytes,
        segment count, compression time, and downstream targeted-query probes; store only the
        selected single `.vortex` artifact; prune derived columns only when the workload profile
        proves they are not needed for admitted queries or public workflow contracts.
      - Tests/evidence: unit tests for profile enumeration, deterministic profile choice, cleanup
        after rejected profiles, workload-derived-column admission, and tie-breaking; evidence fields
        for candidate count, rejected reasons, chosen profile, expected read/write tradeoff, and
        `query_answer_sidecar_status=disabled`.
      - Retain/drop gate: no candidate can ship if it is slower than `271s`, grows artifact size
        materially, or worsens full 43-query UAT unless explicitly accepted as a correctness or
        artifact-fidelity tradeoff.
      - Rejected 2026-09-03 implementation slice: a writer-boundary query-hot compression admission
        gate kept only URL/Referer/SearchPhrase/Title/OriginalURL/UserAgent-family fields compressed
        for very large high-cardinality text sources while leaving cold text fields visible as
        skipped writer decisions. Focused writer/CLI tests passed, but replacement ingest was
        terminated at `385.01s` after the partial artifact reached `75,723,542,388` bytes versus the
        protected `38,147,848,068` byte artifact. Evidence is recorded at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_queryhot_writer_admission_20260903T212035Z/`.
        The runtime change was dropped and the protected artifact was restored. Do not retry
        compression narrowing without a portfolio gate that accounts for whole-artifact byte growth
        before committing a full replacement write.
      - Rejected 2026-09-03 implementation slice: raising the source-text fast-Zstd
        values-per-frame setting from `8,192` to `32,768` kept the same selected fields, codec, and
        one-artifact writer boundary, but replacement-ingest UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260903T231822Z`
        had already reached the protected `271s` elapsed line with only `7.587GB` written in
        `progress.jsonl`. The run was stopped, the runtime patch was dropped, and the retained
        artifact was rebuilt from clean `HEAD` at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260903T232520Z`,
        producing the expected `38,147,848,068` byte `.vortex` artifact. Do not retry simple
        compression frame-size tuning outside the retained layout/codec portfolio gate.
      - Rejected 2026-09-04 implementation slice: a report-only retained layout/codec portfolio
        admission evidence object was added to the Vortex layout write advisor and projected through
        the public prepare evidence, including selected retained profile, known rejected profiles,
        query-answer-sidecar disabled status, and no-fallback/no-external-engine fields. Focused
        layout advisor, CLI projection, numeric UTF8, transformed-dictionary, and count-distinct
        tests passed, as did `cargo fmt --all -- --check`,
        `cargo clippy --workspace --all-targets -- -D warnings`,
        `cargo test --workspace --all-targets`,
        `cargo clippy -p shardloom-vortex --features vortex-write,universal-format-io --all-targets -- -D warnings`,
        and
        `cargo clippy -p shardloom-cli --features release-user-surfaces --all-targets -- -D warnings`.
        Replacement-ingest UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260904T060114Z`
        completed successfully but took `301s` for the same `38,147,848,068` byte artifact versus
        the protected `271s` reference. The runtime and CLI changes were reverted and the failed
        candidate artifact was deleted. Do not retry an evidence-only retained-profile wrapper; the
        next viable portfolio implementation must run isolated pre-public candidate writes or change
        the real source/derive/writer pipeline enough to improve the replacement-ingest gate.
    - [ ] Build a staged pipeline for source read, embedded-derived construction, Arrow-to-Vortex
      conversion, compression/layout preparation, ordered final writer feed, digest, and atomic
      commit, with bounded queues and explicit single-artifact cleanup.
    - [ ] Parallelize CPU-heavy pre-writer work under ShardLoom control before the final Vortex sink
      boundary, and report how much time is source wait, derived-build CPU, compression CPU,
      writer-feed wait, final Vortex write, and commit.
    - [ ] Add a retain/drop gate that rejects writer profiles slower than the retained replacement
      ingest baseline or that regress artifact size/query UAT without a documented reason.
    - [ ] Remove or revert the 2026-09-02 experimental tuning patch if its only evidence remains the
      `360s` rejected UAT run.
    - [ ] Add unit tests for planner decisions across small sources, large numeric sources,
      ClickBench-like wide text sources, source-dictionary-heavy sources, missing row-count sources,
      and memory-constrained profiles.
    - [ ] Add integration/UAT harness fields for stage CPU utilization, queue depth/backpressure,
      rows/sec by stage, artifact bytes, segment count, compression bytes/time, and
      `fallback_attempted=false` / `external_engine_invoked=false`.
    - [x] Run replacement-ingest UAT and full 43-query UAT after implementation before moving the
      item to the completed ledger; current clean implementation evidence is
      `ingest_cli_uat_gated_20260903T112348Z` at `271s` and
      `full43_after_clean_ingest_current_impl_20260903T112902Z` at `189.197s` best-of-3 query
      total.
  - Next outcome: ShardLoom has a production writer physical-design implementation that can be
    retained only if it beats the `301s` local replacement-ingest baseline or provides an explicit
    correctness/single-artifact tradeoff accepted by the maintainer.
  - User-visible surface: `prepare dataframe`, SQL/Python/DataFrame prepare-once flows, ClickBench
    ingest UAT, public workflow preparation evidence, and benchmark docs.
  - Implementation scope: `shardloom-vortex/src/vortex_ingest.rs`,
    `shardloom-vortex/src/universal_format_io.rs`, public prepare evidence lifting in
    `shardloom-cli`, ingest UAT scripts, benchmark JSON artifacts, and this ledger/plan pair.
  - Evidence required: focused planner tests, writer integration tests, replacement-ingest UAT,
    full 43-query UAT, artifact-size comparison, segment-count comparison, CPU/stage attribution,
    no-fallback/no-external-engine assertions, and clear retain/drop decision.
  - Acceptance: a single `.vortex` artifact is produced; unsupported writer shapes fail explicitly;
    no query-answer sidecars or external engines are introduced; current retained correctness holds;
    replacement-ingest wall time improves versus `301s` or the optimization is dropped and the plan
    records why.
  - Verification: `cargo fmt --all -- --check`, focused `shardloom-vortex` writer/ingest tests,
    focused `shardloom-cli` public prepare evidence tests,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, replacement-ingest UAT with
    `scripts/run_clickbench_ingest_uat.sh --replace-existing`, and full 43-query local UAT.
  - Non-goals: no Spark/DataFusion/DuckDB/Polars/Velox runtime fallback; no `vortex-datafusion`;
    no temporary multi-file public artifact; no query-answer cache; no copied competitor code; no
    official ClickBench claim from local Desktop evidence.
  - Claim boundary: local implementation and UAT evidence only until official benchmark hardware,
    reproducible scripts, and release claim gates exist.
  - Fallback boundary: every success or rejection must record `fallback_attempted=false`,
    `external_engine_invoked=false`, and `fallback_execution_allowed=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with the before/after ingest timing and
    retain/drop decision.

- [ ] `CLICKBENCH-PRODUCTION-SEGMENT-METADATA-PRIMITIVE-1` promote segment/granule metadata from
      evidence strings into a reusable execution primitive.
  - Source: official/public ClickHouse sparse primary-index and skip-index documentation, Firebolt
    primary-index documentation, ClickBench size/cold leaders, ShardLoom Vortex-first metadata and
    pruning guardrails, and the 2026-09-02 ledger audit finding that several completed rows reported
    metadata availability without a reusable production primitive.
  - Current state: ShardLoom records row-group extents, footer segment counts, basic statistics
    posture, layout inventories, some derived-column metadata, and query-lane evidence. It does not
    yet have a single production `SegmentMetadata`/`GranuleMetadata` contract consumed uniformly by
    pruning, grouping, top-K, writer layout advice, and public certificates.
  - Intake review:
    - Accepted from the five technique review: make segment/granule metadata an execution
      primitive.
    - Merged with prior evidence rows:
      `PARQUET-ROWGROUP-EXTENT-COMPILER-1`, `GLOBAL-METADATA-FIRST-FAST-LANES-1`,
      `GLOBAL-DICTIONARY-DERIVED-METADATA-1`, `CLICKBENCH-100M-SINGLE-ARTIFACT-LAYOUT-ADVISOR-9`,
      and `CLICKBENCH-100M-STRING-DOMAIN-PREDICATE-8`.
    - Corrected audit disposition: earlier rows provided useful evidence and partial query use, but
      not a shared production metadata primitive with false-negative-safe semantics.
  - V1 scope classification: `required_for_v1` for local prepared Vortex execution; remote/object
    store/table metadata remains `unsupported_boundary` until separately admitted.
  - ShardLoom technique review: use metadata-first execution before source reads or decoded scans;
    attach metadata to capillary segment units; apply dynamic work shaping from segment selectivity
    and byte estimates; use evidence-tier controls to separate exact metadata, conservative
    summaries, approximations, and diagnostic-only rows; PulseWeave applies only to planning and
    pruning evidence.
  - Execution checklist:
    - [x] Define a typed `SegmentMetadata` model with row range, physical byte range, row count,
      null count/posture, min/max where ordered, byte-length bounds, dictionary membership summary,
      string/domain absence certificates, cardinality sketch status, and source provenance.
    - [x] Define exactness levels: exact, conservative-may-read, approximate-for-advice-only, and
      diagnostic-only, with explicit fail-closed behavior when a query would need exact metadata.
    - [x] Persist or reconstruct the metadata through the single `.vortex` artifact contract; any
      temporary ingest-only structure must either be embedded, derivable from Vortex footer/layout
      facts, or marked non-production.
    - [ ] Implement H/VH metadata packet `false-negative-safe string absence summaries`.
      - Ideas covered: Q23 ngram absence pruning, Q23 adaptive predicate ordering, and Q34/Q35
        segment-level URL/string candidate pruning where conservative metadata can remove work.
      - Current evidence: Q23 selects only `7128` rows but still scans every row/segment; Q34/Q35
        read all `34371` segments and report zero candidate-free chunks skipped.
      - Implementation steps: build per-segment conservative ngram/token absence summaries for
        admitted literal predicates; include dictionary-derived summaries when source chunks are
        dictionary encoded; store exactness/provenance with the metadata primitive; make planner
        admission fail closed to read the segment when the summary is missing, approximate-only, or
        incompatible with predicate semantics.
      - Tests/evidence: decoded-reference predicate parity for empty strings, nulls, mixed case,
        UTF-8, dictionary/direct string chunks, predicates shorter than the ngram width, and missing
        metadata; public evidence for `segments_pruned`, `rows_pruned`, `bytes_pruned`,
        `false_negative_policy=prohibited`, and fallback status.
      - Retain/drop gate: retain only when targeted Q23/Q34/Q35 UAT shows measurable work avoided
        without any false-negative risk or artifact-size regression that outweighs query gain.
    - [ ] Implement H/VH metadata packet `stable transform and frequency summaries`.
      - Ideas covered: Q29 exact segment dictionary summaries, Q29 persisted transform code,
        Q34/Q35 segment frequency metadata, and global URL/domain identity synopsis.
      - Current evidence: Q29 has only `74` final groups but still transforms dictionary values and
        updates general measure state; Q34/Q35 heavy-hitter first pass already builds chunk-local
        histograms but has no reusable segment/global value identity or persisted exact transform
        codes.
      - Implementation steps: assign stable value identities for admitted derived URL-domain and
        UTF-8 length columns within the prepared artifact; persist per-segment value counts or
        exact transform-code maps when bounded; expose a conservative summary status for values too
        large to persist; feed these summaries into transformed grouping, heavy-hitter candidate
        generation, and exact replay.
      - Tests/evidence: parity tests for URL parsing, length transforms, null propagation, repeated
        values across chunks, dictionary id reuse, overflow/budget blockers, and artifact reopen;
        evidence for transform-code hits, metadata summary bytes, candidate rows skipped, and
        exactness level.
      - Retain/drop gate: retain only if targeted Q29/Q34/Q35 UAT improves or the summary is reused
        by at least two slow-row families without increasing the single artifact beyond the accepted
        size budget.
    - [ ] Implement H/VH metadata packet `candidate segment directories for duplicate-heavy lanes`.
      - Ideas covered: Q33 candidate segment directory, Q10 global identity support, Q19 duplicate
        promotion metadata, and metadata-first exact distinct planning.
      - Current evidence: Q33 has `99997493` candidate groups for `99997497` rows, Q10 exact
        distinct scans every row for only `9040` groups, and Q19 builds tens of millions of
        candidate groups with large key/string storage.
      - Implementation steps: build per-segment key-frequency and duplicate-presence summaries for
        admitted packed numeric pair, tri-key, and distinct-user families; use summaries to choose
        direct exact aggregation, duplicate-promotion, radix sort/RLE, or conservative full scan;
        record when metadata is not exact enough and the runtime must read all segments.
      - Tests/evidence: exactness tests for all-unique, all-duplicate, skewed, sparse-null,
        mixed-null, and missing-summary cases; evidence fields for duplicate directory hits,
        candidate-free segments, promoted duplicate keys, chosen aggregate strategy, and no-fallback
        status.
      - Retain/drop gate: retain only if Q10/Q19/Q33 targeted UAT improves or if the metadata
        becomes required by a later exact kernel that separately passes its retain gate.
    - [ ] Feed the metadata primitive into predicate pruning, URL/domain grouping, heavy-hitter
      candidate generation, exact distinct, row-ref top-K, writer layout advice, and estimate/explain
      evidence.
    - [ ] Add tests proving no false negatives for min/max, null, byte-length, dictionary membership,
      domain absence, empty/all-null segments, mixed-null segments, and missing-metadata blockers.
    - [x] Add public primitive evidence fields for row count, segment count, exactness/provenance,
      metadata read plan, per-family admission, sidecar/fallback status, and deterministic digest.
    - [ ] Add public query-consumption evidence fields for `segments_total`, `segments_pruned`, `segments_read`,
      `rows_pruned`, `bytes_pruned`, metadata exactness, metadata source, and fallback status.
    - [ ] Run targeted ClickBench lanes that should benefit from metadata before full UAT: Q21-Q24
      predicate/top-K, Q29 transformed grouping, Q33/Q34/Q35 heavy-hitter/domain rows, and exact
      distinct rows.
  - Next outcome: query planning and execution can make conservative segment decisions from one
    reusable metadata primitive instead of ad hoc per-route evidence strings.
  - User-visible surface: explain/estimate/capability reports, public workflow evidence, query UAT
    rows, and prepared-state certificates.
  - Implementation scope: Vortex prepared-state metadata types, source adapter extent metadata,
    local primitive planners, predicate/group/top-K execution, CLI evidence lifting, benchmark
    artifacts, and docs.
  - Evidence required: unit tests for exactness and blockers, decoded-reference parity tests,
    targeted query UAT, full 43-query UAT if retained, no-fallback evidence, and artifact-size
    impact.
  - Acceptance: metadata-backed pruning never drops matching rows; metadata absence fails closed or
    reads conservatively; query evidence shows actual segment/row/byte reduction where expected;
    no sidecar is required for the product artifact.
  - Verification: focused metadata primitive tests, focused local primitive pruning tests, CLI
    public evidence tests, `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full UAT after the retained
    batch.
  - Non-goals: no Bloom/filter implementation with unproven false-negative behavior; no query
    answer caches; no external search/index engine; no object-store/table metadata claim.
  - Claim boundary: local prepared Vortex metadata execution only, not official ClickBench or
    broad production/table support.
  - Fallback boundary: all metadata decisions must preserve `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with exactness tests and UAT evidence.

- [ ] `CLICKBENCH-PRODUCTION-MORSEL-SCHEDULER-THREADLOCAL-MERGE-1` replace route-local partial
      parallelism with a reusable morsel scheduler, thread-local state, and deterministic merge
      contract.
  - Source: public CedarDB/HyPer/Umbra lineage around morsel-driven parallelism and data-centric
    execution, ClickHouse processing-lane documentation, and local evidence that `max_parallelism=12`
    does not currently imply all hot stages use all cores.
  - Current state: ShardLoom has capillary work-unit vocabulary, Parquet row-group source workers,
    some aggregate partial-state routes, and Vortex writer worker-pool evidence. It does not have a
    common scheduler that makes every segment/morsel operator pull bounded work, keep thread-local
    state, report utilization, and merge deterministically.
  - Intake review:
    - Accepted from the five technique review: morsel-style scheduling with thread-local state and
      deterministic merge.
    - Merged with prior rows:
      `AGGREGATE-PARTIAL-PARALLELISM-1`, `CLICKBENCH-100M-HIGH-CARDINALITY-AGGREGATE-7`,
      `CLICKBENCH-100M-TAIL-LANE-OPTIMIZATION-20`, `GLOBAL-HIGH-CARDINALITY-AGGREGATION-1`, and
      `GLOBAL-EXACT-DISTINCT-GROUPED-1`.
    - Corrected audit disposition: earlier partial implementations remain valid per-route
      improvements but do not close a production shared scheduling substrate.
  - V1 scope classification: `required_for_v1` for local analytic runtime families; distributed
    scheduling remains `unsupported_boundary`.
  - ShardLoom technique review: use capillary morsels as the unit of parallel work; PulseWeave
    should record starvation, skew, and backpressure at stage boundaries; dynamic work shaping should
    adapt morsel size from row/byte/selectivity evidence; metadata-first pruning should reduce the
    morsel queue before execution; evidence tiers must distinguish single-thread, source-overlap,
    operator-parallel, and final-merge work.
  - Execution checklist:
    - [x] Define a reusable `MorselScheduler` abstraction over prepared Vortex segments, Parquet
      row-group tasks, generated sources, and future local compatibility sources, with bounded
      queueing and stable work IDs.
    - [ ] Define thread-local state traits for count, sum/avg, grouped aggregate, exact distinct,
      string heavy-hitter, numeric/string top-K, row-ref top-K, and predicate count families.
    - [ ] Implement H/VH scheduler packet `mergeable string heavy-hitter top-K states`.
      - Ideas covered: Q34/Q35 morsel-local sketches, parallel mergeable heavy hitters, compact
        first-pass replay, and shared scheduler execution for string count-distinct top-K families.
      - Current evidence: Q34 and Q35 both spend about `25.657s`, build `48086` active candidates,
        and scan all rows/segments; the current heavy-hitter sketch is route-local and the exact
        recount path is not yet a shared scheduler state family.
      - Implementation steps: define a `ThreadLocalStringHeavyHitterState` with mergeable
        SpaceSaving/Misra-Gries-style candidate state plus exact recount hooks; partition segment
        morsels by stable work id; merge candidates in deterministic worker-index order; run the
        second pass over only admitted candidates with shared interner identity and tie policy.
      - Tests/evidence: deterministic output across `max_parallelism=1,2,3,12`, exact tie ordering,
        all-null/empty string behavior, candidate eviction correctness, exact recount parity, and
        scheduler evidence for worker time, skew, merge time, candidate count, and rows avoided.
      - Retain/drop gate: retain only if Q34/Q35 targeted UAT improves or if it unlocks the shared
        reclaimable interner and exact replay packets with no timing regression.
    - [ ] Implement H/VH scheduler packet `numeric-plus-UTF8 heavy-hitter candidate and recount`.
      - Ideas covered: Q17 parallel candidate generation, partitioned recount, and reusable
        composite-key top-K scheduler state.
      - Current evidence: Q17 remains `15.931s` with `59979` candidates and hundreds of MB of
        retained string storage; the current route uses specialized helpers but not a fully
        scheduler-driven candidate/recount contract.
      - Implementation steps: shard candidate generation by numeric key hash and segment id; keep
        thread-local packed numeric plus interned UTF-8 ids; merge sketches deterministically; run
        exact recount by candidate partition; preserve null, signedness, and tie policy in the
        state trait.
      - Tests/evidence: decoded-reference parity for signed/unsigned numerics, null strings,
        duplicate strings, high cardinality, low cardinality, tie ordering, and worker-count
        determinism; evidence for candidate partitions, recount partitions, string bytes retained,
        and scheduler utilization.
      - Retain/drop gate: retain only if Q17 targeted UAT improves and string memory does not exceed
        the current active-candidate footprint after reclaiming evictions.
    - [ ] Implement H/VH scheduler packet `tri-key grouped aggregate state`.
      - Ideas covered: Q19 three-key heavy-hitter path, morsel-local sketches, duplicate-promotion
        filtering, and radix fallback admission.
      - Current evidence: Q19 remains `9.731s` with `56384822` candidate groups and large key/string
        storage; it uses `numeric_minute_string_dictionary_code_direct_group_update`, not a
        thread-local merge family.
      - Implementation steps: define thread-local tri-key states over packed numeric, minute, and
        dictionary/string ids; choose sketch, duplicate-promotion, or exact radix path from segment
        metadata; merge partial counts with deterministic ordering and exact top-K proof.
      - Tests/evidence: parity tests for minute extraction, dictionary id reuse, null strings,
        duplicate-heavy data, all-unique data, radix fallback equivalence, and scheduler
        determinism; evidence for promoted duplicate keys, exact group count, memory bytes, and
        chosen strategy.
      - Retain/drop gate: retain only if Q19 targeted UAT improves or if exactness/memory evidence
        proves the route is safer under the same timing.
    - [ ] Implement H/VH scheduler packet `packed pair and grouped distinct states`.
      - Ideas covered: Q33 parallel partitioned aggregation and Q10 partition-by-UserID exact
        distinct, with shared duplicate-promotion work distribution.
      - Current evidence: Q33 remains `9.721s` with almost one group per row, while Q10 remains
        `8.909s` using `direct_accessor_count_distinct_group_update` over all rows.
      - Implementation steps: route packed numeric-pair and `(group, UserID)` distinct morsels
        through the scheduler; partition by high bits of the packed key or UserID hash; keep
        thread-local dense/sparse containers; merge deterministic partitions without global lock
        contention.
      - Tests/evidence: exact parity for all-unique, duplicate-heavy, skewed, empty, all-null,
        mixed-null, and high-group-count fixtures; evidence for partitions, duplicate promotion,
        merge time, local container kind, and global distinct count.
      - Retain/drop gate: retain only if Q33/Q10 targeted UAT improves or if it enables a later
        packed/radix kernel with a separate UAT retain decision.
    - [x] Implement deterministic merge ordering for the shared scheduler state contract, including
      stable worker-index merge ordering and test coverage across requested worker counts. Every
      concrete top-K/distinct/aggregate family still needs its own tie/null/floating-point policy
      before this item can close.
    - [ ] Implement deterministic merge ordering for every state family, including ties, null
      ordering, floating-point accumulation policy, source-order preservation, and limit/offset.
    - [x] Add memory-envelope admission per worker and per merge stage, with spill/fail-closed
      diagnostics when exact state exceeds budget.
    - [x] Add utilization evidence: requested/applied workers, runnable morsels, completed morsels,
      mean/max stage time, skew ratio, merge time, worker summaries, and rows/sec.
    - [ ] Add worker idle/starved/backpressure time once route operators run directly through the
      shared scheduler observer rather than only validating the consumed morsel contract.
    - [ ] Convert at least the Q10/Q14/Q17/Q19/Q33/Q34/Q35-style families to the shared scheduler
      before claiming this item complete.
    - [x] Test deterministic row-count scheduling at parallelism `1`, `2`, `3`, and `12`, plus
      memory-budget blockers, metadata-pruned no-work behavior, custom observer state execution,
      and no-fallback evidence fields.
    - [ ] Test intentionally skewed segment distributions and deterministic tie cases for each
      concrete top-K/distinct/aggregate state family after those states are routed through the
      shared scheduler.
  - Next outcome: hot query families use one production parallel execution contract rather than
    isolated per-route worker logic or policy evidence.
  - User-visible surface: query runtime, public workflow execution evidence, explain/estimate
    operator plans, benchmark/UAT artifacts, and resource diagnostics.
  - Implementation scope: local primitive execution, aggregate/top-K state types, scheduler/resource
    helpers, CLI evidence lifts, test fixtures, and UAT artifacts.
  - Evidence required: decoded-reference parity, determinism tests across worker counts, resource
    admission tests, targeted slow-lane UAT, full UAT if retained, and no-fallback evidence.
  - Acceptance: changed query families produce byte-for-byte stable outputs across parallelism
    settings; evidence proves operator-stage parallel work, not only source prefetch or writer pool
    configuration; slow lanes improve or the conversion is dropped/revised.
  - Verification: focused scheduler/state tests, local primitive family tests, CLI evidence tests,
    `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no distributed runtime, no approximate answers for exact SQL semantics, no external
    execution engine, no hidden row-materialized fallback.
  - Claim boundary: local single-node scheduler/runtime evidence only; no official ClickBench,
    production cluster, object-store, or Spark-displacement claim.
  - Fallback boundary: unsupported scheduling paths must fail or use existing ShardLoom-native
    serial execution with explicit evidence, never external fallback.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with worker-utilization and
    determinism evidence.

- [ ] `CLICKBENCH-PRODUCTION-SPECIALIZED-KERNEL-REGISTRY-1` turn hot-lane policy dispatch into a
      production native kernel registry with encoded preconditions and correctness contracts.
  - Source: public HyPer/CedarDB code-generation and specialized execution references, DuckDB vector
    format documentation, Vortex encoded-array/provider capabilities, and the 2026-09-02 audit
    finding that some completed rows described native dispatch evidence without a centralized
    production kernel contract.
  - Current state: ShardLoom has many specialized helpers and route-family labels for dictionary
    predicates, heavy-hitter recount, exact distinct, transformed grouping, packed keys, and
    row-ref top-K. Preconditions, null behavior, encoding requirements, materialization boundaries,
    and retain/drop evidence are spread across route-specific code and evidence strings.
  - Intake review:
    - Accepted from the five technique review: specialized kernels without adopting an external
      query engine.
    - Merged with prior rows: `CODE-SPACE-OLAP-RUNTIME-1`,
      `RUNTIME-GAP-NATIVE-VORTEX-OPERATOR-COVERAGE-1`,
      `GLOBAL-STRING-DOMAIN-EXECUTION-1`, `GLOBAL-HIGH-CARDINALITY-AGGREGATION-1`,
      `GLOBAL-EXACT-DISTINCT-GROUPED-1`, and `CLICKBENCH-100M-TAIL-LANE-OPTIMIZATION-20`.
    - Corrected audit disposition: previous rows remain useful shipped helpers/evidence, but
      production completion requires one registry with admitted/executed/blocked semantics.
  - V1 scope classification: `required_for_v1` for local prepared/native kernels; LLVM/Cranelift or
    generated machine code is `deferred_out_of_v1` unless separately approved by RFC and dependency
    review.
  - ShardLoom technique review: use encoded execution and metadata-first admission before kernel
    selection; capillary scheduling should feed kernels segment/morsel batches; dynamic work shaping
    should choose between dictionary, dense, sparse, direct-primitive, and row-ref kernels; evidence
    tiers must distinguish registry admission, execution, decoded-reference checks, and performance
    retention.
  - Execution checklist:
    - [x] Define a kernel registry schema with operator family, input dtype, encoding/layout
      preconditions, null semantics, determinism, memory envelope, materialization level, Vortex
      provider surface, fallback prohibition, and unsupported diagnostic.
    - [ ] Implement H/VH kernel packet `reclaimable string identity and active-candidate arena`.
      - Ideas covered: Q35 reclaimable arena, Q34 candidate lifetime fix, Q17 reclaimed composite
        candidates, active-candidate memory accounting, and exact top-K eviction safety.
      - Current evidence: `AggregateStringInterner::forget_id()` removes an id from the active map
        but leaves the `Arc<str>` in `values`; Q34/Q35 retain about `3.889GB` of string storage for
        `48086` active candidates, and Q17 retains about `392MB` for `59979` candidates.
      - Implementation steps: replace append-only string storage with a generation-checked
        reclaimable arena or slab; keep stable ids for active candidates; recycle dropped slots
        only after no state references remain; separate `active_bytes`, `historical_bytes`, and
        `arena_capacity_bytes`; update every group-key, sketch, result-rendering, and comparison
        path to reject stale generation ids deterministically.
      - Tests/evidence: unit tests for intern/reuse/forget, stale id rejection, result rendering
        after eviction, exact recount after candidate churn, empty/null strings, and memory counters;
        targeted Q34/Q35/Q17 evidence for active candidates, reclaimed bytes, peak arena bytes, and
        fallback status.
      - Retain/drop gate: retain only if correctness holds and targeted UAT improves or memory drops
        materially without any Q17/Q34/Q35 timing regression.
      - Rejected 2026-09-03 implementation slice: a generation-checked reclaimable interner arena
        with mirror-only cleanup reduced Q34/Q35 active string storage to about `4.9MB` from about
        `3.916GB` historical string bytes, but targeted UAT regressed Q34 from `25.657s` to
        `32.117s` best and Q35 from `25.658s` to `32.430s` best. Do not reattempt this design
        without moving reclamation out of the hot eviction loop or proving a lower-overhead
        dictionary-id lifetime path first.
    - [ ] Implement H/VH kernel packet `Q35 constant-key canonicalization`.
      - Ideas covered: Q35 constant grouping-key elimination, Q34/Q35 shared fingerprinting, and
        planner proof that constant keys do not change grouping semantics.
      - Current evidence: Q34 and Q35 have effectively identical timing and physical work, which
        suggests Q35's constant grouping dimension can be canonicalized to the Q34 heavy-hitter
        route when SQL semantics prove the key is constant and non-null.
      - Implementation steps: add a planner rule that recognizes constant group expressions,
        records their output rendering requirement separately from physical grouping, and reuses
        the same string heavy-hitter route fingerprint as the non-constant-key plan; preserve
        grouping output shape by reattaching the constant column at result assembly.
      - Tests/evidence: planner and decoded-reference tests for constant int/string/null group
        expressions, mixed constants with real keys, order/tie stability, explain evidence, and
        non-application when an expression is not provably constant.
      - Retain/drop gate: retain if Q35 converges to the Q34 physical route or removes measurable
        planner/state overhead without changing output JSON shape.
      - Completed implementation slice: the ClickBench Q35 literal-int ordinal-group shape is already
        admitted through the shared proof-bound URL string heavy-hitter route. Current evidence shows
        `SELECT 1, URL, COUNT(*) ... GROUP BY 1, URL` lowering to physical `group_by=["URL"]` plus
        a reconstructable `constant_int` group expression, and runtime state evidence includes
        `functional_dependency_key_pruning` with `fallback_attempted=false` and
        `external_engine_invoked=false`. This avoids duplicating the Q34/Q35 physical route. The
        remaining packet scope is generalized constant string/null and non-Q35 expression variants,
        not the ClickBench Q35 hot path.
    - [ ] Implement H/VH kernel packet `exact string heavy-hitter histogram replay`.
      - Ideas covered: Q34/Q35 exact histogram replay, compact first-pass replay, stable dictionary
        ids, and global URL identity synopsis when exact and bounded.
      - Current evidence: the first pass already computes chunk dictionary counts and a heavy-hitter
        sketch, but exact replay still performs broad candidate recount work and keeps a large
        append-only string interner.
      - Implementation steps: introduce a registry-admitted kernel that consumes per-segment
        dictionary histograms, maps dictionary values to stable active ids, merges weighted counts,
        emits a bounded candidate set with exactness proof, and replays only segments that can
        affect top-K proof thresholds; use full segment reads when exact metadata is absent.
      - Tests/evidence: decoded-reference parity for dictionary/direct chunks, candidate eviction,
        ties, nulls, non-UTF8 blockers, missing metadata, and proof-bound recount decisions;
        evidence for histogram entries consumed, exact first-pass candidates, second-pass rows,
        skipped segments, active bytes, and selected kernel.
      - Retain/drop gate: retain only if Q34/Q35 targeted UAT improves with unchanged exactness; if
        metadata is insufficient, keep the existing exact route and record blocked kernel evidence.
      - Rejected 2026-09-03 implementation slice: replacing the count-only exact recount's
        dictionary-sized `Vec<Option<u64>>` candidate map with a sparse candidate-code list kept
        correctness but did not beat the protected local timing reference. Targeted Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_sparse_recount_20260903T204734Z/summary.json`
        produced Q34 best `26.297s` and Q35 best `26.122s` versus the protected Q34/Q35
        `25.657s`/`25.658s` best-of-3 reference, so the runtime code was dropped. Do not retry
        this shape unless paired with a larger recount scan reduction or persisted exact histogram
        summary.
      - Completed 2026-09-03 implementation slice: the Q34/Q35 string count top-K result builder now
        caches the current worst retained candidate and avoids fetching/cloning URL strings for
        exact-count candidates that cannot enter the bounded retained window by count. Ties still
        fetch and compare the actual string, so `COUNT(*) DESC, URL ASC` ordering remains exact.
        This keeps the existing dictionary-histogram recount, candidate signature/id/code
        prefilters, and no-fallback route unchanged; it is retained-window bookkeeping, not the full
        persisted histogram-replay kernel. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`.
        Targeted local 100M Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_cached_worst_string_topk_20260903T215808Z/summary.json`
        produced Q34 best `25.385012249986175s` versus protected `25.657463999988977s`
        (`1.06%` faster) and Q35 best `25.479101791977882s` versus protected
        `25.6583917090029s` (`0.70%` faster), with
        `topk_retention_strategy=cached_worst_string_count_retained_window`,
        `string_count_topk_dictionary_histogram_recount=true`, and
        `fallback_attempted=false` / `external_engine_invoked=false`.
      - Completed 2026-09-03 implementation slice: the count-only string heavy-hitter bounded exact
        mirror is now capped at `4x` the heavy-hitter window instead of `32x`. For Q34/Q35 this
        avoids retaining a large speculative string-id/count mirror that still failed to remove the
        exact second-pass recount; exact SQL semantics are unchanged because disabled mirrors
        continue through the existing proof-bound candidate recount. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_string_count_topk_uses_proofbound_heavy_hitter_recount -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_string_count_topk_skips_recount_when_first_pass_is_exact -- --nocapture`,
        and
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_string_count_topk_skips_candidate_free_count_only_chunks -- --nocapture`.
        Targeted local 100M Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_exact_mirror_cap_20260903T_now/summary.json`
        produced Q34 best `25.462161874980666s` versus retained `25.385012249986175s`
        (`0.30%` slower) and Q35 best `24.893747250025626s` versus retained
        `25.479101791977882s` (`2.30%` faster). The shared two-row best-of-3 sum improved from
        `50.864114041963` to `50.355909125006` seconds (`1.00%` faster), with
        `fallback_attempted=false` / `external_engine_invoked=false`. Retain this as a
        Q34/Q35-family improvement, but require the next full-43 UAT to watch the slight Q34
        regression.
      - Rejected 2026-09-04 implementation slice: replacing the string top-K retained-boundary
        proof precheck's string-valued lower-bound candidate sort with a count-only nth-selection
        proof avoided candidate string fetch/clone during proof admission, but targeted local 100M
        Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_count_only_lower_bound_20260904T014554Z/summary.json`
        regressed Q34 to best `26.193807667004876s` and Q35 to best
        `25.68156395899132s`; the shared best-of-3 sum was `51.8753716259962s` versus retained
        `50.355909125006s`. The route still reported
        `string_dictionary_code_count_topk_heavy_hitter_late_recount`,
        `string_count_topk_candidate_signature_prefilter=true`,
        `string_count_topk_candidate_code_prefilter=true`, zero candidate-free chunks, and
        `fallback_attempted=false` / `external_engine_invoked=false`. The runtime change was
        dropped. Future Q34/Q35 work should avoid proof-only micro-optimizations unless paired with
        a measured reduction in second-pass dictionary binding or exact recount work.
      - Completed 2026-09-04 implementation slice: count-only string heavy-hitter policy now uses
        a `32,768` route-local candidate window while leaving string count-distinct and
        numeric+UTF8 heavy-hitter windows at `65,536`. This reduces Q34/Q35 exact recount
        candidate pressure without weakening the proof-bound route. Focused validation:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives physical_policy_classifies_string_and_numeric_utf8_heavy_hitters -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted local 100M Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_count_only_window_20260904T030659Z/summary.json`
        produced Q34 best `24.3785597080132s` and Q35 best `23.853227040963247s`; the
        shared best-of-3 sum improved from retained `50.355909125006s` to
        `48.23178674897645s` (`4.22%` faster). Evidence reported
        `string_count_topk_heavy_hitter_capacity=32768`,
        `string_count_topk_heavy_hitter_candidate_groups=20544` versus prior `48086`,
        `string_count_topk_heavy_hitter_exact_proof=true`,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
      - Rejected 2026-09-04 implementation attempt: tightening the count-only string
        heavy-hitter window again from `32,768` to `16,384` kept count-distinct and numeric+UTF8
        windows unchanged and passed focused validation:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives physical_policy_classifies_string_and_numeric_utf8_heavy_hitters -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted Q34/Q35 candidate UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_window_16k_20260904T043439Z/summary.json`
        timed out on the first Q34 run at the `180s` per-query limit with `0/6` completed runs,
        versus the retained `32,768`-window Q34/Q35 best-of-3 sum `48.23178674897645s`. The runtime
        change was reverted. Do not reduce the Q34/Q35 count-only heavy-hitter window below
        `32,768` without a new exact-proof design that preserves the retained route's proof margin.
      - Rejected 2026-09-04 implementation attempt: replacing the retained count-only
        dictionary-histogram second pass with a compact candidate-slot recount removed the
        dictionary-sized candidate id vector, but it still scanned the same row-id payload and added
        a per-row candidate-slot branch on the Q34/Q35 hot path. Focused validation passed:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `git diff --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        `cargo clippy -p shardloom-vortex --features vortex-local-primitives --all-targets -- -D warnings`,
        and adjacent numeric/string/top-K/count-distinct focused tests, but targeted local 100M
        Q34/Q35 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q34_q35_compact_candidate_recount_20260904T063612Z/summary.json`
        produced Q34 best `24.949315375008155s` and Q35 best `24.4874254160095s`; the shared
        best-of-3 sum was `49.436740791017655s` versus the retained
        `48.23178674897645s`. The runtime change was reverted. Future histogram-replay work should
        reduce row-id scanning or persist exact segment/dictionary counts; simply compacting the
        candidate map is not enough.
    - [ ] Implement H/VH kernel packet `Q17 packed numeric-plus-UTF8 top-K`.
      - Ideas covered: Q17 packed composite identity, parallel candidate generation, partitioned
        recount, and adaptive radix exact path for high-cardinality numeric+string grouping.
      - Current evidence: Q17 uses the numeric+UTF8 heavy-hitter family but still retains large
        string state and does not route through a packed generation-safe state across scheduler
        partitions.
      - Implementation steps: pack numeric key bits, signedness, and string arena id/generation into
        a compact candidate key; provide a direct dictionary-code path when string chunks expose
        reusable codes; choose sketch-plus-recount or radix exact by cardinality/selectivity; render
        final strings only after exact top-K ordering is known.
      - Tests/evidence: parity tests for signedness, null ordering, UTF-8 ties, dictionary/direct
        encodings, adaptive path selection, overflow blockers, and worker-count determinism;
        evidence for packed key bytes, string active bytes, path selected, recount rows, and
        materialized strings.
      - Retain/drop gate: retain only if Q17 targeted UAT improves and final output remains
        byte-for-byte stable across parallelism settings.
      - Completed 2026-09-03 implementation slice: Q17 candidate recount now builds a reusable
        interned UTF-8-id to numeric-partition map once after heavy-hitter candidate freeze, then
        binds each chunk dictionary code to that cached partition and reuses the interned UTF-8 id
        during exact recount key construction. This removes the repeated per-chunk candidate-set
        scan and row-level recount string lookup while preserving the existing proof-bound late
        recount route, null/signedness checks, and no-fallback diagnostics. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_utf8 -- --nocapture`.
        Targeted local 100M Q17 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q17_utf8_id_partition_reuse_20260903T_now/summary.json`
        produced best-of-3 `14.60625249997247s` versus protected `15.930786915996578s`
        (`8.31%` faster), with `59979` candidate groups, `1871` candidate string-id partitions,
        `numeric_utf8_topk_candidate_utf8_id_partition_reuse=true`, and no fallback/external-engine
        invocation.
      - Rejected 2026-09-04 implementation slice: extending the string candidate
        length/prefix/suffix signature prefilter to the Q17 numeric+UTF8 exact recount did activate
        `local_primitive_numeric_utf8_topk_candidate_signature_prefilter=true` before full interner
        lookup, but targeted local 100M Q17 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q17_signature_prefilter_20260904T012355Z/summary.json`
        produced runs `16.15695070900336s`, `14.758894916973077s`, and
        `14.922318750002887s`; best `14.758894916973077s` was slower than the retained
        `14.60625249997247s` Q17 route and the local `14.371960791002493s` hint. The runtime change
        was dropped. Do not retry dictionary-value signature screening for Q17 unless it is moved
        out of the per-chunk second-pass setup or paired with a measured reduction in row-level
        exact recount work.
      - Rejected 2026-09-04 implementation slice: replacing every Q17 candidate string-id
        numeric-part hash set with an inline-small/promoted-hash partition kept exact second-pass
        recount semantics but regressed targeted local 100M Q17 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q17_compact_candidate_partitions_20260904T013542Z/summary.json`.
        Runs were `17.079304874991067s`, `15.693582999985665s`, and `15.686179957992863s`;
        best `15.686179957992863s` was slower than the retained `14.60625249997247s` route.
        Evidence from the best run showed `1871` string-id partitions, `1844` inline numeric
        partitions, `27` promoted hash partitions, `59979` candidate numeric parts, and
        `56378` parts in the largest single string-id partition, with no fallback/external-engine
        invocation. The runtime change was dropped. Future Q17 work should attack the dominant
        large-partition recount pressure, candidate generation, or row filtering instead of
        optimizing tiny partition allocation in isolation.
      - Rejected 2026-09-04 implementation slice: specializing the Q17 second-pass exact recount
        hot loop for direct `Int64`/`UInt64` numeric slices preserved signedness, dictionary-code
        candidate filtering, and proof-bound semantics in focused tests, but regressed targeted
        local 100M Q17 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q17_direct_slice_recount_20260904T041043Z/summary.json`.
        Runs were `16.378674332983792s`, `15.454450499964878s`, and
        `15.297637875017244s`; best `15.297637875017244s` was slower than the retained
        `14.60625249997247s` route. Focused validation before UAT passed
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_utf8 -- --nocapture`,
        and `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        The runtime change was dropped. Do not retry simple typed numeric extraction specialization
        for Q17 unless it also removes a full hash-table recount pass, reduces dominant-partition
        membership probes, or changes candidate generation materially.
      - Rejected 2026-09-04 implementation slice: a dense numeric bitmap membership representation
        was added for large Q17 candidate string-id partitions so the second-pass exact recount
        could replace large `(UserID, SearchPhrase)` candidate hash-set probes with range-checked
        bit tests when the numeric ids were same-signed and span-bounded. Focused validation passed
        `rustfmt --edition 2024 --check shardloom-vortex/src/local_primitives.rs shardloom-cli/src/public_workflow_route.rs`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_utf8 -- --nocapture`,
        `cargo clippy -p shardloom-vortex --features vortex-local-primitives --all-targets -- -D warnings`,
        and `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`,
        but targeted local 100M Q17 UAT was stopped during run 1 after roughly 90 seconds without
        producing any completed run, versus the retained `14.60625249997247s` Q17 best. The
        transient UAT directory reported `completed_runs=0`, `fallback_attempted=false`, and
        `external_engine_invoked=false` before cleanup, and the runtime/CLI changes were reverted.
        Do not retry bitmap membership for Q17 unless bitmap admission can be decided from
        persisted numeric-range evidence before building per-partition bitsets, or unless it is
        paired with a route that removes the second-pass hash-count table entirely.
    - [ ] Implement H/VH kernel packet `Q29 fused weighted dictionary-domain aggregate`.
      - Ideas covered: Q29 fused weighted-dictionary kernel, cross-chunk transform memo, persisted
        exact transform code, dense domain partial states, and exact segment dictionary summaries.
      - Current evidence: Q29 has only `74` output groups but `transformed_dictionary_group_key_uncached()`
        creates fresh `Arc<str>` domain keys per chunk dictionary value and the route uses general
        measure-state update logic.
      - Implementation steps: register a transformed dictionary aggregate kernel that computes URL
        domain once per dictionary value, maps domain to dense ids, accumulates `count`, `sum`,
        `min`, `max`, and `avg(length(...))` with vector/dictionary weights, memoizes transform
        codes across chunks, and persists exact transform summaries when bounded by the prepared
        artifact policy.
      - Tests/evidence: decoded-reference parity for URL-domain parsing, length measures, nulls,
        empty domains, dictionary/direct strings, repeated domains across chunks, dense id overflow,
        persisted summary reopen, and unsupported transforms; evidence for dictionary values
        transformed, memo hits, dense groups, measure updates, rows materialized, and selected
        kernel.
      - Retain/drop gate: retain only if Q29 targeted UAT improves or if the same kernel also
        improves Q34/Q35 domain-heavy lanes without artifact-size regression.
      - Completed implementation slice: the Q29-class transformed-dictionary general-measure path
        now interns transformed URL-domain group keys instead of allocating a fresh owned UTF-8
        group key for every source dictionary value. This preserves the existing weighted
        dictionary update contract, keeps exact HAVING/order semantics, and changes key evidence to
        `typed_single_key+interned_utf8`. Targeted local 100M Q29 UAT improved best-of-3 from the
        protected `10.730s` reference to `10.52268858399475s` at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_interned_domain_keys_20260903T210441Z/summary.json`.
        The same run reported `74` output candidate groups after HAVING/order, `1,798,248`
        interned transformed strings, `14,385,984` estimated group-key bytes, and `137,156,417`
        estimated group-string bytes, so the remaining Q29 work should focus on HAVING-aware
        delayed measure materialization and dense domain-id accumulators rather than another
        string-key allocation-only pass.
      - Rejected 2026-09-03 implementation slice: a two-pass HAVING-aware late-min route stored
        only `(count, sum(length))` for interned URL-domain groups in pass one, retained the
        ordered/HAVING-matched domains, then rescanned selected `Referer` dictionary values to fill
        exact `MIN(Referer)` only for retained output groups. Focused transformed-dictionary tests
        passed, but targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_late_min_20260903T222456Z/summary.json`
        produced best-of-3 `17.43963470804738s`, worse than the retained
        `10.52268858399475s` interned-domain route and the older protected `10.730s` reference.
        The runtime change was removed. Do not retry a full second scan for this shape unless
        persisted exact transform summaries or segment-level min metadata can avoid rereading the
        `81,032,736` selected referer rows.
      - Completed 2026-09-03 implementation slice: the Q29-class URL-domain general-measure path
        now admits a dense transformed-dictionary accumulator keyed by interned domain id. The
        retained route stores direct `count`, weighted `length` sum/count, and exact UTF-8
        min/max candidates instead of allocating a cloned generic aggregate-state vector per
        transformed domain. It preserves exact HAVING/order/LIMIT semantics, keeps final string
        rendering at result construction, and falls through to the older generic route for
        non-URL-domain, nullable, count-distinct, offset, or unsupported transform shapes. Focused
        validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives transformed_dictionary -- --nocapture`.
        Targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_dense_domain_accumulator_20260903T231012Z/summary.json`
        produced best-of-3 `9.870339999964926s` and mean `10.399488527657619s`, improving the
        retained `10.52268858399475s` best by `6.20%` and the older protected `10.730s` reference
        by `8.01%`. Evidence reports
        `aggregate_update_strategy=transformed_dictionary_dense_general_measure_group_update`,
        `compact_group_state_strategy=dense_transformed_dictionary_general_measure_group_state`,
        `group_state_mode=dense_transformed_dictionary_general_accumulators`,
        `group_key_storage=dense_transformed_dictionary_interned_utf8_key`,
        `group_output_strategy=capillary_transformed_dictionary_dense_general_topk`, `74` selected
        candidate groups, `25` retained rows, `1,798,248` decoded/interned domain strings, and
        `fallback_attempted=false` / `external_engine_invoked=false`. Remaining Q29 work should
        focus on persisted exact transform summaries or segment-level domain/min metadata that can
        reduce the `1,798,248` transformed group keys themselves, not another second pass.
      - Rejected 2026-09-04 implementation slice: routing the retained dense Q29 URL-domain key-id
        lookup through the existing bounded transformed-dictionary cache preserved the single-pass
        dense accumulator route but regressed targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_dense_transform_key_cache_20260904T015511Z/summary.json`.
        Runs were `11.519518750021234s`, `10.3173550000065s`, and `10.33113541599596s`;
        best `10.3173550000065s` was slower than retained `9.870339999964926s`. Raw summary
        evidence showed the full-URL cache saturated at `1048576` entries with `24899191` misses
        and only `430290` hits while still emitting `74` selected groups and no
        fallback/external-engine invocation. The runtime change was dropped. Future Q29 transform
        memoization should be keyed by persisted dictionary/domain summaries or lower-cardinality
        domain identities, not source `Referer` URL identity.
      - Rejected 2026-09-04 implementation attempt: the dense URL-domain accumulator was changed to
        reserve group-state capacity from a bounded domain-oriented initial reserve instead of the
        source dictionary value count. The focused transformed-dictionary regression suite passed
        (`cargo test -p shardloom-vortex --features vortex-local-primitives transformed_dictionary -- --nocapture`),
        and the release CLI built with
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`,
        but targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_dense_domain_bounded_reserve_20260904T025014Z/summary.json`
        timed out on run 1 at the `240s` harness limit before any completed run
        (`completed_runs=0`, `query_rows` null), versus retained Q29 best
        `9.870339999964926s`. Summary evidence still reported `fallback_attempted=false` and
        `external_engine_invoked=false`. The runtime change was reverted. Future Q29 work should
        avoid capacity-policy-only changes and instead attack the remaining source dictionary
        transform count with persisted domain identities or segment summaries.
      - Rejected 2026-09-04 implementation attempt: a prepared-domain dictionary dense route was
        prototyped for Q29 so rewritten aggregates could group on
        `__shardloom_derived_url_domain_Referer` while still computing exact `AVG(length(Referer))`,
        `COUNT(*)`, and `MIN(Referer)` from the source `Referer` dictionary by a verified
        source-code-to-domain-code map. Focused validation passed:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives transformed_dictionary -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives simple_aggregate_uses_prepared_domain_dictionary_for_ordered_source_minmax -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_prepared_domain_dictionary_20260904T032749Z/summary.json`
        failed the retain gate: best-of-3 regressed to `10.943624250008725s` versus retained
        `9.870339999964926s`. Payload evidence confirmed the new route selected
        `aggregate_update_strategy=prepared_domain_dictionary_dense_general_measure_group_update`
        and rewrote both `__shardloom_derived_url_domain_Referer` and
        `__shardloom_derived_utf8_len_Referer`, but arrays read rose from `1170` to `1456` and
        `decoded_string_count` remained `1798248`. The runtime change was reverted. This confirms
        the hidden derived domain column is not yet dictionary-lifted enough to reduce Q29's
        transformed group-key cardinality; move this idea to the ingest/writer dictionary-lift
        work rather than retrying it in query execution.
      - Completed 2026-09-04 implementation slice: the retained dense Q29 URL-domain accumulator
        now admits a chunk-local exact partial-aggregate path when a sampled source dictionary has
        enough repeated transformed domains to amortize global interner probes and per-domain merge
        work. Each chunk builds exact URL-domain partial states from active dictionary values,
        accumulates weighted `COUNT(*)`, `AVG(length(Referer))`, and exact UTF-8 min/max locally,
        then interns each chunk domain once and merges the partial into the global dense
        transformed-dictionary state. This preserves exact HAVING/order/LIMIT semantics and falls
        back to the retained dense direct path for high-cardinality chunk dictionaries where the
        sample does not show enough reuse. Focused validation passed:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_dense_general_measures_transformed_dictionary_uses_chunk_partials -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives transformed_dictionary -- --nocapture`,
        and `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Full workspace validation passed with `cargo fmt --all -- --check`,
        `cargo clippy --workspace --all-targets -- -D warnings`, and
        `cargo test --workspace --all-targets`. Targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_chunk_domain_partials_20260904T034014Z/summary.json`
        produced runs `10.42336300003808s`, `9.274564541992731s`, and
        `9.27978608297417s`; best `9.274564541992731s` improves the retained dense-domain
        baseline `9.870339999964926s` by `0.595775457972195s` (`6.04%`). Evidence reports
        `aggregate_update_strategy=transformed_dictionary_dense_general_chunk_partial_group_update`,
        `compact_group_state_strategy=dense_transformed_dictionary_chunk_partial_measure_group_state`,
        `group_state_mode=dense_transformed_dictionary_chunk_partial_accumulators`,
        `group_key_storage=dense_transformed_dictionary_interned_utf8_key`, `74` selected candidate
        groups, `25` retained rows, `1,798,248` decoded/interned domain strings, and
        `fallback_attempted=false` / `external_engine_invoked=false`. Remaining Q29 work still needs
        persisted exact transform summaries or writer-lifted domain dictionaries to reduce the
        source dictionary transform count itself.
      - Rejected 2026-09-04 implementation attempt: the retained dense Q29 URL-domain accumulator
        was micro-specialized for the common `COUNT(*)`, `AVG(length(Referer))`, and `MIN(Referer)`
        measure shape by replacing the generic dense-plan update/merge branches with a named
        length-additive/min-UTF8 inner update. Focused validation passed:
        `cargo test -p shardloom-vortex --features vortex-local-primitives transformed_dictionary -- --nocapture`,
        `cargo test -p shardloom-cli local_primitive_result_summary_lifts_runtime_strategy_fields -- --nocapture`,
        and `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        The first targeted local 100M Q29 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_length_additive_min_utf8_20260904T050232Z/summary.json`
        showed a small best-of-3 win at `9.234600208001211s`, but the reviewed final-code rerun at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_length_additive_min_utf8_final_20260904T050552Z/summary.json`
        regressed to best `9.335183416027576s`, and the confirmation rerun at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q29_length_additive_min_utf8_confirm_20260904T050635Z/summary.json`
        produced best `9.29998174996581s` versus the retained Q29 chunk-partial baseline
        `9.274564541992731s`. Evidence confirmed the candidate still selected the same retained
        `transformed_dictionary_dense_general_chunk_partial_group_update` route with `74` selected
        groups, `1,798,248` decoded/interned domain strings, and no fallback/external-engine
        invocation. The runtime and CLI flattening changes were reverted. Do not retry Q29
        branch-elision micro-specialization unless it is paired with lower transform count,
        persisted transform summaries, or writer-lifted domain dictionaries.
    - [ ] Implement H/VH kernel packet `Q23 encoded predicate and selected-row aggregate`.
      - Ideas covered: Q23 ngram absence pruning, existing encoded predicate routing, adaptive
        conjunct pipeline, true late measure materialization, and selected-row aggregate kernel.
      - Current evidence: Q23 selects `7128` rows and `3673` groups but reports generic
        `row_state_update`; existing local primitives have dictionary/FSST/memmem predicate helpers
        that are not yet admitted through the route used by this query.
      - Implementation steps: register predicate kernels for dictionary, FSST, and direct UTF-8
        contains/LIKE literals; order conjuncts by conservative selectivity and kernel cost; build a
        selection vector before reading measure columns; run aggregate/group updates only over
        selected row refs and materialize payload strings at the sink boundary.
      - Tests/evidence: parity tests for literal contains, LIKE, empty predicates, nulls,
        dictionary/direct/FSST encodings, conjunct non-application, selected-row aggregation,
        materialization counters, and unsupported pattern diagnostics; evidence for predicate
        kernel selected, rows tested, rows selected, measure rows decoded, groups emitted, and
        fallback status.
      - Retain/drop gate: retain only if Q23 targeted UAT improves and the route preserves exact
        predicate semantics; no regex/LIKE approximation can ship without a decoded-reference proof.
      - Completed implementation slice: Q23-class residual `AND` predicates now refine admitted
        candidate row lists through column-scoped predicate materialization before falling back to
        the older all-projected-column candidate export. The retained change keeps exact predicate
        semantics, preserves the no-fallback path, and limits each refinement step to the predicate
        column it actually needs. Focused coverage exercises `LIKE`/contains plus `SearchPhrase <>
        ''` candidate filtering and existing grouped aggregate residual row filtering. Targeted
        local 100M Q23 UAT improved best-of-3 from the protected `10.257s` reference to
        `10.207678083039355s` at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q23_column_scoped_predicate_20260903T205926Z/summary.json`.
      - Completed 2026-09-03 implementation slice: simple aggregate `StringContains` predicates now
        remain eligible for upstream Vortex scan pushdown instead of being forced into ShardLoom
        residual filtering. The route still uses ShardLoom-native aggregate/update state and the
        upstream provider boundary is Vortex scan/LIKE expression evaluation, not a query-engine
        fallback. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_contains -- --nocapture`.
        Targeted local 100M Q22/Q23 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_vortex_contains_pushdown_20260903T220500Z/summary.json`
        produced Q22 best `2.691731749975588s` versus protected `5.710917625023285s`
        (`52.87%` faster) and Q23 best `9.49373608303722s` versus the protected full-run
        `10.256865292001748s` / retained column-scoped `10.207678083039355s` references
        (`7.44%` / `7.00%` faster). Evidence reports `local_primitive_mode=vortex_scan_pushdown`,
        `local_primitive_filter_pushdown_applied=true`, selected rows `1038` for Q22 and `7128`
        for Q23, and `fallback_attempted=false` / `external_engine_invoked=false`.
      - Completed 2026-09-03 retained slice: bounded selective string top-K late measures can now
        be captured during the first filtered pass when exact counts remain provable and the
        provisional measure state stays under a `32,768` row / `16,384` group admission cap. This
        closes the Q23 second-scan gap without changing broad string top-K recount semantics: if the
        cap is exceeded or exact first-pass counts are not provable, ShardLoom discards the
        provisional measure groups before the existing candidate recount. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`.
        Targeted local 100M Q22/Q23 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_first_pass_late_measure_flattened_20260904T000520Z/summary.json`
        produced Q22 best `1.2129830840276554s` versus retained `2.691731749975588s` (`54.94%`
        faster) and Q23 best `4.2945536660263315s` versus retained `9.49373608303722s` (`54.77%`
        faster). Evidence reports
        `group_output_strategy=proofbound_heavy_hitter_string_count_topk_first_pass_late_measure_exact`,
        `aggregate_update_strategy=string_dictionary_code_count_topk_first_pass_late_measure_exact`,
        `distinct_state_strategy=proofbound_first_pass_late_measure_exact`,
        `string_count_topk_heavy_hitter_second_pass=false`,
        `string_count_topk_late_measure_second_pass=false`,
        `string_count_topk_first_pass_late_measures=true`,
        `string_count_topk_first_pass_late_measure_rows=7128`, and `fallback_attempted=false` /
        `external_engine_invoked=false`.
      - Rejected 2026-09-04 implementation attempt: a compact active-dictionary-code path for tiny
        filtered chunks was prototyped to avoid full dictionary count vectors, all-value interning,
        per-chunk active hash sets, and candidate flag vectors when Q23/Q22-style filtered chunks
        carry only a small selected row set. Focused correctness/build checks passed before UAT:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted local 100M Q22/Q23 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_compact_active_code_20260904T023018Z/summary.json`
        failed the retain gate: Q22 run 1 hit the `240s` harness timeout before any completed run
        (`completed_runs=0`, `query_rows` all null), versus the retained Q22 best
        `1.2129830840276554s` and retained Q23 best `4.2945536660263315s`. Summary evidence still
        reported `fallback_attempted=false` and `external_engine_invoked=false`. The runtime change
        was reverted. Do not retry row-direct compact active-code late-measure updates without first
        proving the per-row group-state update path cannot dominate the selective-string route.
      - Rejected 2026-09-04 implementation attempt: a compact late-measure state for the Q23-shaped
        string top-K route was prototyped to keep `COUNT(*)`, dictionary UTF-8 `MIN(...)`, and
        direct integer `COUNT(DISTINCT ...)` in a specialized map keyed by interned string id, with
        inline one-to-four-value distinct storage and direct result rendering. Focused validation
        passed before UAT:
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        `cargo clippy -p shardloom-vortex --features vortex-local-primitives --all-targets -- -D warnings`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        The targeted local 100M Q22/Q23 UAT candidate did not produce a valid summary: the first
        harness run exceeded the retain gate, a bounded-capture rerun remained stuck beyond the
        per-run timeout, and the empty candidate output directory plus candidate binary were
        removed. The runtime and public evidence-mapping changes were reverted, leaving the retained
        Q22/Q23 reference at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_first_pass_late_measure_flattened_20260904T000520Z/summary.json`
        unchanged (`Q22` best `1.2129830840276554s`, `Q23` best `4.2945536660263315s`). Do not
        retry a final compact late-measure map for this route without first proving the current
        general late-measure state is the dominant cost under bounded-output UAT.
      - Rejected 2026-09-04 implementation attempt: a planner-level conjunct-ordering rule was
        prototyped to sort pure `AND` predicate parts so embedded derived-length comparisons ran
        before positive string `LIKE`/contains predicates, with negated string predicates last. The
        intent was to transfer classic predicate-cost ordering into the Vortex pushdown expression
        handed to Q23 while preserving exact SQL semantics. Focused validation passed:
        `rustfmt --edition 2024 shardloom-vortex/src/local_primitives.rs --check`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_contains_pushdown -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_count_topk -- --nocapture`,
        and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted local 100M Q22/Q23 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_pushdown_conjunct_order_20260904T031638Z/summary.json`
        failed the retain gate: Q22 best regressed to `1.2619268329581246s` versus retained
        `1.2129830840276554s`, and Q23 best regressed to `4.3739531670007855s` versus retained
        `4.2945536660263315s`. Summary evidence reported `completed_runs=6`,
        `fallback_attempted=false`, and `external_engine_invoked=false`. The runtime change was
        reverted. Future Q23 work should target actual encoded predicate work avoided or scan
        projection/measure decoding, not Vortex pushdown expression ordering.
      - Rejected 2026-09-04 implementation attempt: a Q23-shaped mixed string predicate policy was
        prototyped to keep positive-plus-negated string top-K late-measure predicates in ShardLoom's
        exact residual row-filter path instead of pushing them into the upstream Vortex scan filter.
        The intent was to avoid filtered-scan fragmentation and fuse exact dictionary/FSST/direct
        row-index predicate kernels with the first-pass late-measure aggregate. Focused validation
        passed before UAT:
        `cargo test -p shardloom-vortex --features vortex-local-primitives mixed_string_topk_late_measure_prefers_shardloom_exact_residual_filter -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives string_contains_pushdown_is_route_aware_for_aggregate_regression_guard -- --nocapture`,
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_mixed_predicate_preserves_pushdown_and_residual_boundary -- --nocapture`,
        `cargo fmt --all -- --check`, and
        `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`.
        Targeted local 100M Q22/Q23 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q22_q23_shardloom_residual_filter_20260904T044822Z/summary.json`
        failed the retain gate: Q22 best improved to `1.1738246669992805s` but Q23 regressed to
        `24.825983082992025s` versus retained `4.2945536660263315s`. Q23 evidence reported
        `local_primitive_filter_pushdown_applied=false`, `local_primitive_arrays_read_count=1456`,
        `local_primitive_max_chunk_rows=99864`, and selected rows remained `7128`. The runtime
        change was reverted. Do not replace Q23's retained Vortex filtered-scan path with a full
        ShardLoom residual exact row-filter scan unless a future candidate first proves lower array
        read count, lower row-id traversal, or a true fused scan/provider boundary.
    - [ ] Implement H/VH kernel packet `Q19 fixed-width tri-key grouped top-K`.
      - Ideas covered: Q19 three-key heavy-hitter, fixed-width tri-key, duplicate-promotion filter,
        morsel-local sketches, and radix exact fallback.
      - Current evidence: Q19 uses a numeric-minute-string dictionary-code state but still creates
        many candidate groups and retains large key/string storage.
      - Implementation steps: pack numeric id, minute bucket, and string dictionary/arena id into a
        fixed-width key; choose exact hash, sketch-plus-recount, duplicate-promotion, or radix/RLE
        path from segment metadata; keep measures in dense partial arrays where cardinality permits;
        render string keys only for retained final groups.
      - Tests/evidence: parity tests for minute extraction, numeric overflow, null strings,
        dictionary id reuse, all-unique/all-duplicate/skewed data, path selection, and deterministic
        ties; evidence for packed key width, candidate count, promoted duplicates, radix runs,
        string active bytes, and selected kernel.
      - Retain/drop gate: retain only if Q19 targeted UAT improves or if memory drops materially
        with no full-43 regression.
      - Completed 2026-09-03 implementation slice: Q19 numeric-minute-string exact output retention
        now caches the current worst retained candidate and only recomputes the retained-window worst
        when a new exact group displaces it. This keeps the single-pass exact hash-state contract and
        final tie semantics intact while removing the repeated per-group retained-window scan across
        `56384822` candidate groups. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_minute_string -- --nocapture`.
        Targeted local 100M Q19 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q19_cached_worst_topk_20260903T_now/summary.json`
        produced best-of-3 `9.213867459038738s` versus protected `9.730688542011194s`
        (`5.31%` faster), retained `topk_retention_strategy=cached_worst_numeric_minute_string_retained_window`,
        and kept `fallback_attempted=false` / `external_engine_invoked=false`.
      - Completed 2026-09-04 implementation slice: the Q19 numeric-minute-string dictionary-code
        route now admits a typed direct-slice update loop for non-null `Int64`/`UInt64` numeric
        keys, raw or prepared `Int64`/`UInt64` minute columns, and non-null UTF-8 dictionary row ids.
        The existing exact hash state, dictionary-bound string ids, cached-worst final top-K
        retention, and deterministic tie semantics are unchanged; the slice removes per-row
        accessor dispatch and records `numeric_minute_string_direct_slice_updates` plus updated row
        counts in local primitive and CLI evidence. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_numeric_minute_string -- --nocapture`.
        Targeted local 100M Q19 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q19_direct_slice_20260904T004113Z/summary.json`
        produced runs `8.62326970801223s`, `7.022884582984261s`, and
        `7.00159087497741s`; best improved from the protected retained
        `9.213867459038738s` to `7.00159087497741s` (`24.00%` faster) while preserving
        `candidate_groups=56384822`, `retained_candidate_groups=10`,
        `local_primitive_numeric_minute_string_direct_slice_updates=true`,
        `local_primitive_numeric_minute_string_direct_slice_update_rows=99997497`,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
      - Rejected 2026-09-04 implementation slice: Q19 packed the numeric-minute-string key from
        three stored fields into two `u64` words and added key-width evidence plus explicit
        minute/string-id overflow checks. Focused Q19 tests passed
        (`cargo test -p shardloom-vortex --features vortex-local-primitives grouped_aggregate_numeric_minute_string -- --nocapture`
        and
        `cargo test -p shardloom-vortex --features vortex-local-primitives aggregate_numeric_minute_string_key_is_packed_and_validated -- --nocapture`),
        and the release build passed
        (`cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`).
        Targeted local 100M Q19 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q19_packed_key_20260904T081128Z/summary.json`
        was stopped after roughly one minute with `completed_runs=0`, compared with the retained
        direct-slice route's warm `7.00159087497741s` best; the transient log directory and runtime
        changes were removed. Do not retry key repacking alone for Q19 unless it is paired with a
        changed hash/update strategy, because the exact direct-slice route is currently highly
        sensitive to layout/codegen changes.
    - [ ] Implement H/VH kernel packet `Q33 duplicate-promotion packed-pair aggregate`.
      - Ideas covered: Q33 duplicate-promotion Bloom/filter strategy, packed pair table, radix
        sort/RLE, parallel partitioned aggregation, and candidate segment directory.
      - Current evidence: Q33 has almost one candidate group per row, so a normal hash table pays
        per-row group-state cost even when only duplicates can affect `count > 1` style top-K
        outcomes.
      - Implementation steps: pack the numeric pair into fixed-width keys; run a first duplicate
        promotion pass using a false-negative-free exact directory or a Bloom-assisted prefilter
        followed by exact verification; aggregate only promoted candidates; choose radix sort/RLE
        when the all-unique signal makes hashing wasteful; merge partitions deterministically.
      - Tests/evidence: parity tests for all-unique, one-duplicate, many-duplicate, nulls,
        high-cardinality skew, Bloom false-positive handling, exact verification, radix/hash path
        equivalence, and worker-count determinism; evidence for keys promoted, false positives,
        exact verification rows, candidate groups avoided, and selected kernel.
      - Retain/drop gate: retain only if Q33 targeted UAT improves and exact verification proves no
        false negatives.
      - Completed 2026-09-03 implementation slice: Q33 numeric-pair late-measure retained candidate
        selection now caches the current worst retained pair from the first exact count pass and only
        recomputes that slot when a new exact pair enters the retained top-K window. This keeps the
        two-pass exact count-then-measure route, count-state release, retained-key second pass, and
        tie ordering unchanged while avoiding a retained-window scan for each of the `99997493`
        first-pass candidate groups. This is not yet the Bloom/radix duplicate-promotion packet.
        Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_pair -- --nocapture`.
        Targeted local 100M Q33 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_cached_worst_topk_20260903T_now/summary.json`
        produced best-of-3 `8.731495374988299s` versus protected `9.72064512502402s`
        (`10.18%` faster), retained `topk_retention_strategy=cached_worst_numeric_pair_retained_window`,
        and kept exact route evidence with `fallback_attempted=false` / `external_engine_invoked=false`.
      - Completed 2026-09-04 implementation slice: Q33 numeric-pair late-measure top-K now admits
        a direct-slice exact near-unique directory when the retained window is small and a uniform
        sample shows at least `99%` unique packed `(WatchID, ClientIP)` keys. The first pass stores
        every seen packed key once and promotes only duplicate keys into exact counts; retained
        candidates are then selected from exact duplicate counts plus singleton tie-fill keys, after
        which the directory is released before the retained-key measure second pass. This preserves
        exact two-pass semantics, deterministic count/key tie ordering, late measure materialization,
        and explicit failure if the direct-slice accessor contract is lost. Vortex-first provider
        check: this is classified as `implement_shardloom_kernel` because the work is a
        ShardLoom-owned grouped-top-K state strategy over admitted local Vortex direct accessors;
        no upstream query-engine integration, decoded Arrow residual, or external fallback is used.
        Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives numeric_pair -- --nocapture`.
        Targeted local 100M Q33 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q33_near_unique_directory_20260904T011000Z/summary.json`
        produced runs `9.311350875010248s`, `7.123132416978478s`, and
        `6.794421416998375s`; best improved from the retained
        `8.731495374988299s` to `6.794421416998375s` (`22.18%` faster) while preserving
        `candidate_groups=99997493`, `retained_candidate_groups=10`,
        `local_primitive_numeric_pair_late_measure_near_unique_directory_updates=true`,
        `local_primitive_numeric_pair_late_measure_near_unique_rows=99997497`,
        `local_primitive_numeric_pair_late_measure_near_unique_seen_keys=99997493`,
        `local_primitive_numeric_pair_late_measure_near_unique_duplicate_keys=4`,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
    - [ ] Implement H/VH kernel packet `Q10 exact distinct family split`.
      - Ideas covered: Q10 separate aggregate families, partition-by-UserID hash, radix pair
        sort/dedup, adaptive exact containers, and global ids/Roaring-style bitmap evaluation.
      - Current evidence: Q10 emits only `9040` groups but exact distinct still updates per row
        through `direct_accessor_count_distinct_group_update`.
      - Completed implementation slice: Q10-like mixed aggregates with exactly one identity integer
        group key and exactly one identity integer `COUNT(DISTINCT ...)` measure now use a
        chunk-local packed `(group, distinct)` preunion before updating the existing exact per-group
        distinct sets. Non-distinct measures still update per input row, the legacy single-state
        grouped count-distinct route remains unchanged, and unsupported/non-integer/null-admitting
        shapes fall through to the existing exact path without fallback. Evidence emits
        `direct_accessor_count_distinct_pair_preunion_group_update`,
        `typed_hash_exact+packed_integer_pair_preunion`, and
        `grouped_aggregate_state+count_distinct+topk+direct_accessor_general_state+pre_reserved+direct_count_distinct+packed_integer_pair_preunion`.
        Targeted local 100M Q10 UAT improved best-of-3 from `8.909421833988745s` to
        `4.80588679201901s` at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q10_preunion_20260903T194121Z/summary.json`.
      - Completed implementation slice: the retained packed-pair Q10 route now also performs
        chunk-local group partial aggregation before touching the global group map. The row loop
        still applies non-distinct measures exactly for every input row inside the chunk, applies
        the count-distinct measure only for unique packed `(group, distinct)` pairs, then merges
        each chunk group into the global exact state once. This preserves the existing
        `implement_shardloom_kernel` direct-accessor boundary over admitted local Vortex accessors,
        avoids decoded Arrow or external-engine residuals, and keeps non-matching/null-admitting
        shapes on the existing explicit exact path. Evidence emits
        `direct_accessor_count_distinct_pair_preunion_chunk_group_update`,
        `generic_direct_accessor_group_state+packed_integer_pair_preunion+chunk_group_partials`,
        `typed_single_key+packed_integer_pair_preunion+chunk_group_partials`, and
        `direct_accessor_count_distinct_pair_preunion_chunk_group_partials`. Focused validation:
        `cargo test -p shardloom-vortex --features vortex-local-primitives count_distinct -- --nocapture`.
        Full validation: `cargo fmt --all -- --check`,
        `cargo clippy --workspace --all-targets -- -D warnings`, and
        `cargo test --workspace --all-targets`. Targeted local 100M Q10 UAT at
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_q10_chunk_group_preunion_20260904T020000Z/summary.json`
        produced runs `5.013928040978499s`, `3.693268208007794s`, and
        `3.7760477080009878s`; best improved from the retained `4.80588679201901s` to
        `3.6932682080077943s` (`23.15%` faster) while preserving
        `local_primitive_grouped_count_distinct_pair_preunion_input_rows=99997497`,
        `local_primitive_grouped_count_distinct_pair_preunion_unique_pairs=21900477`,
        `local_primitive_grouped_count_distinct_pair_preunion_chunk_groups=859298`,
        `local_primitive_grouped_count_distinct_pair_preunion_duplicate_rows_elided=78097020`,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
      - Implementation steps: split count-distinct execution into dense-group, sparse-group,
        high-cardinality, and packed-pair strategies; partition by `UserID` hash to deduplicate
        before group insertion; choose radix sort/dedup when exact container pressure is high; use
        Roaring-style containers only after license/provenance review or implement a local
        Apache-compatible bitmap container.
      - Tests/evidence: parity tests for empty/all-null/mixed-null, duplicate-heavy users,
        all-unique users, dense groups, sparse groups, high group count, container switching, and
        worker-count determinism; evidence for container kind, dedup pairs avoided, partitions,
        peak memory, selected kernel, and dependency/provenance status.
      - Retain/drop gate: retain only if Q10 targeted UAT improves and dependency/provenance review
        confirms any third-party bitmap crate is Apache-2.0-compatible; otherwise keep a local
        implementation or the existing exact route.
    - [ ] Register first production kernels for string heavy-hitter top-K, transformed
      dictionary URL/domain grouping, numeric-pair aggregate, numeric+UTF8 grouped top-K, dense
      exact distinct, row-ref top-K, exact predicate count, and direct primitive aggregate.
    - [x] Replace route-local policy labels with registry selection where semantics match, while
      keeping public route names stable.
    - [ ] Add decoded-reference parity tests for every admitted kernel, including empty, all-null,
      mixed-null, high-cardinality, low-cardinality, dictionary, direct primitive, and unsupported
      layout cases.
    - [x] Add explain/evidence fields for kernel candidate set, selected kernel, admission reason,
      execution level, decoded/materialized rows, and blocked alternatives.
    - [ ] Add retain/drop gates so kernel specialization that fails correctness, degrades target UAT,
      or only changes evidence cannot be marked complete.
  - Next outcome: ShardLoom can make defensible kernel-selection decisions from one native registry
    without hand-waving route labels as implementation.
  - User-visible surface: explain/capabilities, public workflow evidence, query runtime, benchmark
    artifacts, and diagnostics.
  - Implementation scope: kernel registry types, local primitive planner/executor integration,
    existing specialized aggregate/predicate/top-K helpers, CLI evidence lifting, tests, and docs.
  - Evidence required: per-kernel correctness tests, unsupported diagnostics, targeted slow-lane UAT,
    full UAT if retained, no-fallback evidence, and materialization/decode counters.
  - Acceptance: every admitted specialized kernel has explicit preconditions and decoded-reference
    proof; every blocked shape has a stable diagnostic; public evidence proves real kernel execution
    rather than label-only dispatch.
  - Verification: focused kernel registry tests, local primitive execution tests, CLI evidence tests,
    `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no DataFusion/Spark/DuckDB/Polars/Velox execution; no code copied from ClickHouse,
    DuckDB, HyPer, Umbra, CedarDB, Firebolt, Doris, Arc, or another system; no JIT/compiler
    dependency until an RFC approves it.
  - Claim boundary: local native-kernel execution evidence only, not broad SQL/operator parity or
    official benchmark superiority.
  - Fallback boundary: registry miss must produce deterministic unsupported or ShardLoom-native
    lower-tier execution evidence, never external fallback.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with kernel-by-kernel proof.

- [ ] `CLICKBENCH-PRODUCTION-COLUMNAR-RESULT-DATAPLANE-1` keep result and retained-row data columnar
      until the declared sink boundary.
  - Source: public ClickHouse lazy materialization documentation, DuckDB vector format documentation,
    Arrow Flight SQL/result-plane design signal, ShardLoom late-materialization guardrails, and the
    2026-09-02 audit finding that prior columnar data-plane work was mostly evidence without a
    complete result/sink contract.
  - Current state: ShardLoom has row-ref top-K paths, selected-row materialization evidence, compact
    result fields, and JSON/CSV/Vortex sink evidence. Several public routes still materialize row
    values earlier than necessary or report columnar posture without a shared internal
    `ColumnarResultBatch` contract through top-K, sort, aggregate, and sink handoff.
  - Intake review:
    - Accepted from the five technique review: columnar result/data-plane discipline.
    - Merged with prior rows: `GLOBAL-ROWREF-TOPK-PRUNING-1`,
      `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1`, `RUNTIME-GAP-OUTPUT-SINK-FANOUT-1`,
      `CLICKBENCH-100M-PHYSICAL-POLICY-PLANNER-6`, and
      `CLICKBENCH-DOMAIN-TRANSFER-1` columnar result evidence.
    - Corrected audit disposition: prior rows remain valid route-specific improvements, but the
      production result data-plane must be implemented as a shared sink-facing contract.
  - V1 scope classification: `required_for_v1` for local CLI/Python/DataFrame/SQL result correctness
    and performance posture; remote result delivery remains `unsupported_boundary`.
  - ShardLoom technique review: combine row-ref top-K, selection vectors, metadata-first column
    pruning, capillary batch execution, and evidence-tier materialization certificates; dynamic
    work shaping should choose retained-row batch sizes; PulseWeave should report materialization
    pressure at sink boundaries only.
  - Execution checklist:
    - [x] Define a shared `ColumnarResultBatch`/`RetainedRowSet` contract that carries selected
      columns, row refs/source ordinals, validity/null semantics, order/tie metadata, and sink
      materialization requirements.
    - [ ] Implement H/VH result packet `selected-row aggregate and retained-row handoff`.
      - Ideas covered: Q23 true late measure materialization, Q23 selected-row aggregate kernel,
        row-ref retained result flow, and sink-boundary materialization proof.
      - Current evidence: Q23 selects only `7128` rows but still reports generic row-state update
        posture; several hot routes expose columnar evidence without routing every retained-row and
        aggregate path through the shared result dataplane.
      - Implementation steps: make predicate kernels emit `RetainedRowSet` selections; make grouped
        aggregate/top-K paths consume selected column batches instead of row JSON values; keep
        measure columns decoded only for selected row refs; render JSON/CSV/CLI/Python rows from
        `ColumnarResultBatch` at the declared sink boundary.
      - Tests/evidence: parity tests for JSON output shape, null rendering, output order/ties,
        selected-column projection, grouped aggregate results, distinct results, and wide payload
        top-K; evidence for rows retained, rows materialized, columns decoded, payload bytes
        decoded, sink adapter selected, and fallback status.
      - Retain/drop gate: retain only if Q23 targeted UAT improves or if broad result-path tests
        prove lower materialization with no timing regression across Q23/Q24/Q25/top-K rows.
    - [ ] Implement H/VH result packet `columnar sink parity for exact aggregate families`.
      - Ideas covered: sink-facing handoff for Q10 exact distinct, Q17/Q19/Q34/Q35 top-K outputs,
        Q29 transformed grouping outputs, and route-wide columnar result completion.
      - Current evidence: `ColumnarResultBatch` and materialization certificates exist, but the
        broad route conversion remains open; query families can still build row-shaped result
        values earlier than the declared JSON/CSV/user-row boundary.
      - Implementation steps: add adapters from every admitted aggregate/top-K state family into
        `ColumnarResultBatch`; preserve group-key dictionary/string ids until final rendering; make
        Vortex/Arrow-compatible sinks consume columnar batches directly where admitted; keep
        compatibility-output metadata-loss reports explicit.
      - Tests/evidence: parity tests for Q10/Q17/Q19/Q29/Q33/Q34/Q35-style result rows, JSON/CSV
        rendering, Vortex/Arrow-compatible local sink replay, null/group-key ordering, and blocked
        remote sinks; evidence for per-family row-materialization boundary and no-fallback status.
      - Retain/drop gate: retain only if full 43-query UAT remains stable and at least one slow-row
        family improves or reports materially lower decoded/materialized bytes without output drift.
    - [ ] Route filter/project/limit, sort/top-K, grouped aggregate, distinct, and row export
      through columnar result batches until JSON/CSV/user-row rendering requires row materialization.
    - [x] Add sink adapters that can consume columnar batches for Vortex/Arrow-compatible local
      outputs and explicitly materialize only at JSONL/CSV/CLI text boundaries.
    - [x] Add materialization certificates with rows considered, rows retained, rows materialized,
      columns decoded, payload bytes decoded, sink boundary, and no-fallback status.
    - [ ] Add tests for order stability, null rendering, selected-column projection, wide payload
      top-K, grouped output, distinct output, JSON/CSV parity, and unsupported remote delivery.
    - [ ] Run targeted UAT on Q23/Q24/Q25 and broad top-K/grouped rows, then full UAT for retained
      changes.
  - Next outcome: ShardLoom result flow is columnar and sink-aware internally, with row materializing
    only at the declared user-visible boundary.
  - User-visible surface: CLI JSON/text output, Python collection, DataFrame-style collection,
    local sinks, benchmark timing evidence, and materialization diagnostics.
  - Implementation scope: result model types, local primitive executors, sink/export helpers, CLI
    renderer/evidence lifting, Python wrapper expectations, tests, benchmark artifacts, and docs.
  - Evidence required: row/column materialization counters, sink-boundary certificates,
    decoded-reference parity, targeted query UAT, full UAT if retained, no-fallback evidence, and
    artifact/output replay checks.
  - Acceptance: wide payload/top-K routes avoid full wide-row materialization; JSON/CSV/Python
    outputs remain identical; Vortex/Arrow-compatible sinks consume columnar batches where admitted;
    evidence names the exact materialization boundary.
  - Verification: focused result/sink tests, public route evidence tests, Python parity tests where
    wrappers are touched, `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no remote Flight service, no hidden pandas/Polars materialization, no broad Arrow
    execution substrate claim, no query-answer sidecar.
  - Claim boundary: local result-path implementation evidence only, not broad public API,
    cloud/result-service, or official benchmark claim.
  - Fallback boundary: every result path must keep `fallback_attempted=false` and
    `external_engine_invoked=false`; unsupported sinks fail explicitly.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with materialization counters and
    sink-parity evidence.

Current autonomous execution order:

1. Do not start implementation from the rejected 2026-09-02 writer-tuning diff. If implementation is
   resumed, first retain/drop or remove that experimental patch using the `360s` UAT evidence.
2. Start with the `CLICKBENCH-PRODUCTION-WRITER-PHYSICAL-DESIGN-1` H/VH packets in this order:
   decoupled ordered pre-writer pipeline, dictionary-lifted derived-column construction, single
   resource governor, then retained layout/codec portfolio admission. The `271s` replacement-ingest
   run is the protected local reference until a fresh UAT improves or explicitly supersedes it.
3. Do not retry the rejected hot-loop string identity/reclaimable arena packet. Revisit that packet
   only through a lower-overhead dictionary-id lifetime design that moves reclamation out of the
   eviction path; otherwise proceed to the next measured Q34/Q35/Q17 heavy-hitter packet with the
   memory caveat visible in evidence.
4. Then proceed through metadata summaries, scheduler state-family routing, specialized kernels, and
   columnar result packets in the order that maximizes shared reuse: Q34/Q35 shared string
   heavy-hitter, Q29 transformed dictionary-domain aggregate, Q23 encoded predicate/selected-row
   path, Q17 packed numeric-plus-UTF8 top-K, Q33 packed-pair duplicate promotion, Q10 exact distinct,
   and Q19 tri-key grouped aggregate unless fresh targeted evidence changes the dependency order.
5. After each retained implementation batch, run targeted UAT for the touched rows first and reserve
   replacement-ingest plus full 43-query UAT for the cohesive batch boundary.
6. Keep all runtime work attached to shared ShardLoom/Vortex-native execution surfaces, with
   external engines restricted to baseline/oracle evidence only.

Validator ownership note: `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` remains named here as the active
global-review runtime-gap owner required by `scripts/check_runtime_gap_family_burn_down.py`. It is a
traceability owner, not an open implementation item; concrete completed work lives in the completed
ledger.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
