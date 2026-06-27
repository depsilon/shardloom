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
2. Implement `CLICKBENCH-STRING-HEAVY-HITTER-EXACT-RECOUNT-11` first because `CB-Q34`/`CB-Q35`
   are the largest current query-runtime tail.
3. Implement `CLICKBENCH-TRANSFORM-CODE-MAP-12` next because `CB-Q29` is still expensive despite
   using transformed dictionary aggregate routing.
4. Implement `CLICKBENCH-HIGH-CARDINALITY-RADIX-CAPILLARY-13` for `CB-Q17`/`CB-Q19`/`CB-Q33`
   after the string/transform code paths are stable.
5. Implement `CLICKBENCH-ROW-REF-TOPK-SEGMENT-PRUNING-14` for `CB-Q24` once order-key segment
   statistics are confirmed usable in the single `.vortex` artifact.
6. Implement `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15` and
   `CLICKBENCH-ARTIFACT-SIZE-ENCODING-POLICY-16` as one cohesive ingest/layout batch because they
   share writer, encoding, dictionary, and artifact-size evidence.
7. Run focused PR validation for any remaining branch edits; avoid another full workspace or full
   ClickBench run unless a new implementation batch materially changes runtime behavior.
8. Create/merge the cohesive PR when required checks are green.
9. Start any version/release train only after merged checks and explicit maintainer approval.

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

- [ ] `CLICKBENCH-STRING-HEAVY-HITTER-EXACT-RECOUNT-11` Remove the second broad string scan from
  `string_heavy_hitter_topk` lanes while preserving exact `ORDER BY ... LIMIT` results.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_current_branch_physical_policy_20260627T231853Z/summary.json`
    shows `CB-Q35` `33.017s`, `CB-Q34` `31.052s`, and `CB-Q23` `11.161s`; all use the
    `string_heavy_hitter_topk` policy and current evidence includes second-pass exact recount.
  - Five material ideas reviewed:
    - [ ] Dictionary-code exact recount: retain candidate dictionary codes and recount over code
      vectors instead of UTF-8 values.
    - [ ] Per-segment candidate bitsets: persist/derive candidate-code membership so non-candidate
      segments or chunks are skipped in the exact pass.
    - [ ] Heavy-hitter sketch with proof-bound thresholds: stop widening the candidate set once a
      retained-bound proof is exact.
    - [ ] Interned group-key arena reuse: share string key storage across first pass, exact recount,
      and output materialization.
    - [ ] Functional-dependency pruning: when one grouped string is derived from another key, retain
      only the narrower key through aggregation and materialize dependent values at output.
  - Ship/drop checklist:
    - [ ] Implement dictionary-code exact recount for string heavy-hitter top-K groups.
    - [ ] Add per-segment candidate-code membership evidence and skip accounting.
    - [ ] Emit exactness evidence proving no query-answer cache, no sidecar, no external engine, and
      no approximate result.
    - [ ] Run targeted UAT for `CB-Q23`, `CB-Q34`, and `CB-Q35` against the latest retained baseline.
    - [ ] Ship if at least one tail lane materially improves and no target lane regresses beyond
      run-variance tolerance; otherwise drop/revert the approach and record why.
  - Non-goals: no query-specific summary, no sidecar, no approximate top-K, and no DataFusion/
    DuckDB/Spark/Polars/pandas fallback.
  - Status: open material performance item.

- [ ] `CLICKBENCH-TRANSFORM-CODE-MAP-12` Make transformed dictionary aggregates operate on reusable
  transform-code maps rather than repeated per-row/per-value string transforms.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q29` `17.439s` on
    `transformed_dictionary_aggregate`; the route is correct, but the transformed grouping family
    is still a tail contributor.
  - Five material ideas reviewed:
    - [ ] Persist dictionary-value transform maps for `length`, `url_domain`, minute/date bucket,
      and regex-like admitted transforms inside the single `.vortex` artifact metadata.
    - [ ] Aggregate over transform codes, not transformed strings, with late visible value
      materialization only for retained groups.
    - [ ] Share transform-code maps between predicates, grouped aggregates, sort/top-K, Python,
      DataFrame-style helpers, and SQL.
    - [ ] Add transform null/error/overflow contracts so invalid or null transform inputs stay
      exact and deterministic.
    - [ ] Cache transform planner decisions by artifact metadata digest during one process without
      caching query answers.
  - Ship/drop checklist:
    - [ ] Add a generic transform-code map contract for admitted dictionary-derived transforms.
    - [ ] Rewire transformed dictionary aggregate state to group/merge over transform codes.
    - [ ] Emit transform-code evidence: transform family, dictionary cardinality, code cardinality,
      null handling, and materialization boundary.
    - [ ] Run targeted UAT for `CB-Q29` against the latest retained baseline.
    - [ ] Ship if runtime materially improves or remains neutral with materially lower string
      transform/materialization evidence; otherwise drop/revert and record why.
  - Non-goals: no arbitrary regex engine fallback, no Python callable/UDF shortcut, and no
    transform result sidecar.
  - Status: open material performance item.

- [ ] `CLICKBENCH-HIGH-CARDINALITY-RADIX-CAPILLARY-13` Redesign high-cardinality exact aggregate
  state around radix/capillary partitioned partials and merge-local packed keys.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q17` `17.177s`, `CB-Q19` `11.074s`, and `CB-Q33`
    `15.042s`; the current policy correctly separates `numeric_utf8_heavy_hitter_topk` from
    `near_input_cardinality_numeric_pair_aggregate`, but state construction and merge locality
    remain material.
  - Five material ideas reviewed:
    - [ ] Radix partition group state by packed key prefix so exact merge is cache-local and
      spill-ready.
    - [ ] Segment-local capillary partials with deterministic merge order and bounded memory
      accounting.
    - [ ] Packed composite keys for numeric/numeric, numeric/string, and dictionary-code/string
      groups.
    - [ ] Late measure materialization for retained candidate groups, especially numeric-pair
      second-pass measure lanes.
    - [ ] Memory-budgeted exact spill boundary that fails closed until certified native spill is
      available.
  - Ship/drop checklist:
    - [ ] Implement radix/capillary partial aggregation for high-cardinality grouped aggregates.
    - [ ] Add packed-key merge evidence and memory-pressure accounting shared by SQL/Python/
      DataFrame/CLI.
    - [ ] Preserve exact count, sum, avg, min, max, count-distinct, HAVING, ORDER BY, LIMIT, and
      OFFSET semantics.
    - [ ] Run targeted UAT for `CB-Q17`, `CB-Q19`, and `CB-Q33` against the latest retained baseline.
    - [ ] Ship if runtime or memory pressure materially improves without widening candidate-window
      regressions; otherwise drop/revert and record why.
  - Non-goals: no approximate aggregates, no hidden external engine, no query-specific partition
    plan, and no uncertified spill writes.
  - Status: open material performance item.

- [ ] `CLICKBENCH-ROW-REF-TOPK-SEGMENT-PRUNING-14` Add segment/block order-key pruning before
  row-ref top-K materialization.
  - V1 scope classification: `required_for_v1`.
  - Source: latest local 100M UAT shows `CB-Q24` `14.065s`; the route already uses
    `row_ref_sort_topk`, but still scans broadly before final materialization.
  - Five material ideas reviewed:
    - [ ] Use embedded per-segment min/max order-key stats to skip segments that cannot beat the
      current top-K boundary.
    - [ ] Persist source-order locality metadata so retained row refs can seek fewer chunks during
      final materialization.
    - [ ] Maintain a monotonic top-K threshold that tightens as candidate rows are discovered.
    - [ ] Split sort into key-only candidate scan and selected-row materialization with bounded
      row-ref buffers.
    - [ ] Add block-level metadata inside larger segments when segment-level pruning is too coarse.
  - Ship/drop checklist:
    - [ ] Confirm order-key min/max and row-position locality metadata are available in the
      current single `.vortex` artifact.
    - [ ] Implement conservative segment pruning for bounded `ORDER BY ... LIMIT` where ordering
      direction and null semantics are exact.
    - [ ] Emit pruning evidence: consulted segments, skipped segments, retained row refs, and final
      materialization boundary.
    - [ ] Run targeted UAT for `CB-Q24` plus `CB-Q25`-`CB-Q27` regression guards.
    - [ ] Ship if `CB-Q24` materially improves and `CB-Q25`-`CB-Q27` stay neutral; otherwise
      drop/revert and record why.
  - Non-goals: no order-changing approximation, no precomputed query result, and no broad sort
    materialization before pruning.
  - Status: open material performance item.

- [ ] `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15` Reduce prepare/load time by improving
  writer segment economics and single-pass ingest accounting.
  - V1 scope classification: `required_for_v1`.
  - Source: replacement ingest evidence records prepare/load as writer-dominant, with previous
    prepare around `515s` and Vortex write/segment write around `455s`.
  - Five material ideas reviewed:
    - [ ] Adaptive larger row blocks and fewer segments based on source shape, target pruning value,
      and memory envelope.
    - [ ] Coalesced typed-batch handoff into the Vortex writer with bounded queues and ordered
      atomic commit.
    - [ ] Source-native Parquet/Arrow dictionary preservation and CSV/JSONL projection-aware typed
      builders.
    - [ ] Single-pass derive/write/digest so strings and output bytes are not rescanned when
      metadata/digest can be accumulated during write.
    - [ ] Writer timing split for read, typed build, derived metadata, encode/layout, segment write,
      footer/register, digest, and reopen verification.
  - Ship/drop checklist:
    - [ ] Implement adaptive row-block/segment sizing and coalesced writer handoff behind the
      existing single `.vortex` artifact path.
    - [ ] Add timing-surface fields that isolate segment write, footer/register, digest, and reopen
      costs.
    - [ ] Preserve ordered atomic replacement and no-sidecar behavior.
    - [ ] Run replacement ingest UAT only after the query-lane changes settle; record load time,
      artifact size, segment count, and full-query effect together.
    - [ ] Ship if load time, segment economics, artifact shape, or downstream query runtime
      materially improves without unacceptable regression in the others; otherwise drop/revert and
      record why.
  - Non-goals: no duplicate massive artifacts, no multi-file OLAP sidecar, and no source-format
    route fork that bypasses Universal Ingest.
  - Status: open material performance item.

- [ ] `CLICKBENCH-ARTIFACT-SIZE-ENCODING-POLICY-16` Reduce single-artifact size without making
  load or query runtime worse.
  - V1 scope classification: `required_for_v1`.
  - Source: current local artifact is about `34.9GB` from a `14.8GB` Parquet source; ClickBench
    scoring includes storage size, and large artifacts also increase scan/write pressure.
  - Five material ideas reviewed:
    - [ ] Replace full hidden derived columns with dictionary-derived metadata and compact code
      maps where exact semantics allow.
    - [ ] Use column-family encoding policy: dictionary/Zstd for high-value text, fast compact
      encodings for numeric/derived metadata, and low-effort load profile where compression is not
      worth the CPU.
    - [ ] Deduplicate derived URL/domain/length dictionaries across related columns when source
      dictionaries prove compatible.
    - [ ] Add layout advisor feedback that reports size contribution by column family and hidden
      metadata family.
    - [ ] Add optional compact/repack mode later, separate from default fast-load mode, only after
      default runtime stays competitive.
  - Ship/drop checklist:
    - [ ] Add size-attribution evidence for source columns, derived metadata, dictionaries, and
      footer/layout metadata.
    - [ ] Convert eligible hidden derived columns to compact dictionary/code metadata inside the
      `.vortex` artifact.
    - [ ] Add column-specific compression policy with explicit load/runtime tradeoff evidence.
    - [ ] Run replacement ingest UAT and compare artifact size, load time, and full-query runtime
      against the latest retained baseline.
    - [ ] Ship only if size improves without material load/query regression, or if a documented
      query/load win justifies neutral size; otherwise drop/revert and record why.
  - Non-goals: no external compression-only artifact, no post-query compacting, and no hidden
    multi-file index.
  - Status: open material performance item.


## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
