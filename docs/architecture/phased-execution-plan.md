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

- [ ] `PERF-DESIGN-2` - Encoded-native operator promotion and stage-timing attribution cleanup.
  - Source: PR #1174 route rows, current published row chunks, operator mode inventory,
    `operator_hot_path_candidate`, and route timing exclusive/residual fields.
  - Current state: hot/native rows remain fast but still carry residual/materialized operator
    posture. Multi-key group by, nested JSON field scan, high-cardinality string group/distinct,
    join+aggregate, and group-by aggregation remain the highest-value operator families.
  - Next outcome: promote the highest-value residual family toward encoded-native or
    certificate-backed native execution while making exclusive stage timing additive and
    surface-aware.
  - Acceptance: admitted rows stop overstating residual/materialized work for the selected family;
    unsupported families keep deterministic blockers; timing attribution cannot contradict
    authoritative route totals.
  - Progress: benchmark promotion now emits `operator_compute_route_relation_*` fields so
    diagnostic-only operator timings cannot be mistaken for additive hot-route totals; remaining
    work is runtime/operator-family promotion.
  - Progress: prepared/native group-category state now preassembles the exact
    `group_by_aggregation` and `multi_key_group_by` result payloads once per shared source-state
    family, reports result-cache/preassembly timing fields, and keeps hot scenario execution from
    rebuilding aggregate maps into JSON on each consumer.
  - Progress: nested JSON field scan parser now keeps marker detection and boolean/numeric field
    extraction on byte-level hot paths while preserving deterministic malformed-payload diagnostics.
  - Progress: prepared/native category-metric state now preassembles
    `high_cardinality_string_group_distinct` and `distinct_count` result payloads once per shared
    source-state family and reports category result-cache/preassembly fields.
  - Verification: focused Rust operator/correctness tests, Python row-contract tests, targeted
    benchmark rerun for the selected family, `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`, and
    `cargo test --workspace --all-targets` when runtime behavior changes.
  - Claim/fallback boundary: claims stay family/scenario-scoped; no external engine residual
    evaluation is allowed.

- [ ] `PERF-DESIGN-2-A` - Prepared/native scan, operator, and result-path tightening.
  - Source: `PERF-DESIGN-2`, `PERF-DESIGN-4R`, and prepared/native lanes where scan-open, result
    assembly, residual hot-loop materialization, and optional result-sink work remain visible.
  - Next outcome: reduce repeated scan/open, operator dispatch, allocation, result-envelope, and
    compact result-sink overhead without changing result digests or publication-proof semantics.
  - Acceptance: prepared/native hot-runtime geomeans improve or remain stable with clearer
    attribution; result digests remain unchanged; any encoded/native promotion is scoped and
    certificate-backed.
  - Verification: focused Rust tests for selected operator families and result envelope stability,
    repeated-session tests, targeted warm prepared/native rerun, and broad Rust validation when
    shared runtime behavior changes.
  - Claim/fallback boundary: no broad encoded-native claim; blocked promotion continues only through
    admitted ShardLoom-native residual execution.

- [ ] `PERF-DESIGN-4R` - PulseWeave session/runtime coalescing optimization pass.
  - Source: completed `PERF-DESIGN-4`, `docs/architecture/pulseweave-runtime-control.md`, and rows
    showing repeated native/prepared route-open, scan-open, and result-assembly pressure.
  - Current state: split-inventory reuse evidence exists for compatible prepared/native batch runs;
    result assembly remains per-scenario with an explicit blocked status.
  - Next outcome: extend run-local PulseWeave coalescing to remaining safe repeated
    prepared/native pressure points using FlowInventory, ScarcityLedger, EndoPulse, and ProofBound.
  - Acceptance: compatible local scenario groups report coalesced route use with reduced repeated
    overhead or deterministic `blocked_*` reasons; no hot-runtime/publication-proof timing mix.
  - Verification: focused Rust session/PulseWeave tests, targeted repeated warm/native/prepared
    benchmark, optimization-target validator, claim gate, and broad Rust validation when behavior
    changes.
  - Claim/fallback boundary: run-local scoped coalescing only; no daemon, global cache, cross-run
    learning, distributed runtime, or external engine.

- [ ] `PERF-DESIGN-1R` - Dynamic prepared-state reuse and role-repair optimization pass.
  - Source: completed `PERF-DESIGN-1`, `docs/architecture/io-reuse-and-fanout-architecture.md`, and
    rows where prepared-state lookup/metadata verification remains visible despite reuse.
  - Next outcome: add a run-local dynamic prepared-state reuse controller that batches dependency
    checks, records capillary proof refs, and skips redundant lookup/create work when the digest
    tuple is unchanged.
  - Acceptance: repeated same-run prepared lookups report dynamic reuse with no rewritten artifacts,
    no full reopen verify, no duplicate writer context, stable certificate refs, and deterministic
    repair/reprepare when source roles drift.
  - Verification: focused Rust prepared-state reuse/repair tests, targeted prepare-batch benchmark,
    Python promotion-field tests, optimization-target validator, and broad Rust validation when
    behavior changes.
  - Claim/fallback boundary: scoped local prepared-state reuse only; no process-global cache, hidden
    stale reuse, object-store cache, or external engine.

- [ ] `PERF-DESIGN-5R` - Capillary preparation spine write/reopen/copy optimization pass.
  - Source: completed `PERF-DESIGN-5`, cold-ingestion carryforward docs, and rows where
    `vortex_write_ms`, reopen/verify, and copy-budget cost remain material.
  - Next outcome: coalesce compatible source split discovery, columnarize/encode, Vortex segment
    write, metadata-first verification, and sink evidence tasks under capillary
    PulseWeave/ProofBound admission.
  - Acceptance: admitted local preparation rows show fewer duplicate writer/reopen/copy operations
    or deterministic capillary block reasons; hot-runtime totals remain separate from
    publication-proof totals.
  - Verification: focused Rust preparation-spine tests, targeted preparation benchmark,
    artifact-completeness/claim gates, optimization-target validator, and broad Rust validation when
    runtime behavior changes.
  - Claim/fallback boundary: scoped local capillary preparation only; no object-store writes, table
    commits, real query-data spill, hidden buffer pool, or external engine.

- [ ] `PERF-DESIGN-5R-A` - Vortex write coalescing and reusable writer context.
  - Source: `PERF-DESIGN-5R`, cold routes where local Vortex write remains material, and existing
    capillary preparation-spine/copy-budget fields.
  - Next outcome: introduce a reusable writer/runtime context for compatible preparation groups,
    coalesce small artifact writes, separate digest/verification accounting, and expose fail-closed
    coalescing blockers.
  - Acceptance: cold/prepared rows show admitted writer-context reuse where safe; Vortex output
    digests remain stable; unsupported artifact role, schema, workspace, or proof shapes block
    coalescing.
  - Verification: focused Rust writer-context, role isolation, digest stability, cleanup, and
    workspace-safe staging tests plus targeted cold-certified/prepare-batch reruns.
  - Claim/fallback boundary: no weakening of Vortex-native persistence, no object-store write
    semantics, no Vortex query-engine integration, and no external engine.

- [ ] `PERF-DESIGN-6R` - Dynamic source-adapter parse/decode and scout-ingress optimization pass.
  - Source: completed `PERF-DESIGN-6`, `docs/architecture/dynamic-work-shaping.md`,
    `docs/architecture/universal-input-contract.md`, and rows where
    `source_parse_or_columnar_decode_ms` remains a cold-lane bottleneck.
  - Next outcome: choose lightweight metadata, projected typed decode, or capillary chunked decode
    based on observed bytes/rows/columns and scenario-required fields, with explicit block reasons.
  - Acceptance: source-heavy lanes show reduced parse/decode or handoff timing for admitted
    projected workloads or deterministic `blocked_*` source-scout reasons; required columns and null
    semantics remain intact.
  - Verification: focused Rust/Python source adapter tests, targeted source-heavy benchmark,
    optimization-target validator, website readiness/static validation, and broad validation when
    shared adapters move.
  - Claim/fallback boundary: scoped dynamic source-adapter optimization only; no external parser
    engine, object-store runtime, lossy decode, or broad source-format claim.

- [ ] `PERF-DESIGN-6R-A` - Direct typed column builders for CSV/JSONL cold ingest.
  - Source: `PERF-DESIGN-6R`, the universal input contract, and route attribution where text-source
    parse/decode dominates source-heavy cold rows.
  - Current state: local runtime/reporting emits `source_typed_column_builder_*` and
    `source_typed_builder_*` evidence for admitted CSV/JSONL rows; focused Rust coverage proves full
    CSV decode, projection-aware JSONL nested-payload decode, and direct-transient non-admission.
  - Next outcome: regenerate targeted source-heavy artifacts, confirm typed-builder fields reach
    promotion/website data, and decide whether more decode runtime work is needed from current
    timing rather than stale artifacts.
  - Acceptance: admitted CSV/JSONL rows report direct typed builder execution, correct projected or
    full column counts, zero row assembly where supported, stable correctness digests, and
    deterministic blockers for malformed/coercion/nested unsupported shapes.
  - Verification: focused source-adapter tests for nulls, malformed values, timestamps, dirty CSV,
    nested JSON, projection masks, type coercions, benchmark harness smoke, and targeted source-heavy
    reruns.
  - Claim/fallback boundary: scoped CSV/JSONL typed-builder evidence only after clean artifacts; no
    external parser engine or hidden row-object fallback.

- [ ] `PERF-DESIGN-6R-B` - Projection-aware and source-aware decode admission.
  - Source: `PERF-DESIGN-6R`, source-read scout attribution, and projection-sensitive gaps where
    lazy baselines avoid unnecessary column work. Those engines remain baselines only.
  - Current state: local runtime/reporting promotes `source_projection_*` admission evidence with
    required, predicate, output, certificate, diagnostic, field-mask, decoded/skipped-count,
    blocker, correctness, and no-fallback fields.
  - Next outcome: refresh projection-sensitive artifacts and use the field-mask evidence to reduce
    parse/decode or handoff work where the current data proves an opportunity.
  - Acceptance: projection-sensitive workloads show reduced timing without digest changes;
    full-width workloads report no projection opportunity; unsupported nested/malformed cases block
    deterministically.
  - Verification: required-field derivation tests, CSV/JSONL projection tests, Parquet/Arrow field
    mask tests where supported, and targeted reruns for wide projection, filter/projection/limit,
    selective filter, distinct count, group-by, and nested JSON scan.
  - Claim/fallback boundary: projection-aware decode claims require admitted row field-mask evidence;
    no predicate-pushdown claim unless predicate-pushdown evidence exists.

- [ ] `PERF-DESIGN-6R-C` - Already-columnar source fast paths for Parquet and Arrow IPC.
  - Source: `PERF-DESIGN-6R`, current already-columnar source gaps, Vortex-first provider check, and
    Parquet/Arrow IPC cold rows. Polars, DuckDB, PyArrow, and similar tools remain baselines only.
  - Current state: this working tree emits stable `source_columnar_*` provider evidence for
    direct-provider Parquet/Arrow IPC rows, including projection, preserved/skipped columns, record
    batch count, zero materialized rows, handoff timing, correctness posture, and no-fallback fields.
    Direct-transient row-boundary adapters remain non-admitted; Avro/ORC are visible but out of the
    6R-C claim scope.
  - Next outcome: merge the runtime/reporting evidence, then refresh targeted Parquet/Arrow IPC
    artifacts before making any timing claim.
  - Acceptance: admitted Parquet/Arrow IPC rows report direct columnar handoff, no row
    materialization, stable correctness digests, reduced parse/decode timing where current artifacts
    prove it, and explicit blockers for unsupported dtypes, encodings, nested fields, or compression.
  - Verification: focused tests for primitive types, null-heavy columns, dictionary-like values where
    supported, timestamps, projection masks, empty inputs, unsupported nested fields, promoter
    passthrough tests, and targeted Parquet/Arrow IPC reruns.
  - Claim/fallback boundary: scoped direct-columnar fast-path evidence only after a clean benchmark
    refresh; no fallback to Polars/PyArrow execution or lossy conversion.

- [ ] `PERF-DESIGN-3` - Publication-proof sink/evidence pipeline optimization.
  - Source: `publication_proof` rows, `PERF-SPLIT-FIX-1`, and the user request to reduce benchmark
    errors and write values incrementally.
  - Current state: publication-proof rows intentionally include result-sink and evidence-render
    work. The page labels this correctly, but proof artifacts can still be written/replayed/rendered
    more incrementally.
  - Next outcome: implement incremental proof sidecar records for result-sink writes, replay proofs,
    certificate links, and human evidence render metadata that can be reused when row inputs and
    route evidence digests have not changed.
  - Acceptance: publication proof remains visible and slower when doing real proof work; unchanged
    proof records are reused; digest drift blocks promotion; route formulas state timing surface and
    inclusion flags.
  - Verification: focused proof-cache tests, targeted publication benchmark, publication claim gate,
    website readiness, and static asset validation.
  - Claim/fallback boundary: this may improve publication-proof overhead only; it does not change
    hot-runtime timing claims or allow external proof engines.

- [ ] `PERF-DESIGN-3-A` - Certified route tiering for hot-runtime versus publication-proof work.
  - Source: `PERF-DESIGN-3`, the route timing surface split, and claim-grade proof requirements.
  - Next outcome: define a tier admission ledger where hot runtime uses compact machine evidence,
    full replay proof requests machine replay/sink proof, and publication proof requests full
    digest/result-sink replay/human evidence render.
  - Acceptance: hot-runtime totals exclude human/publication work by contract; publication-proof
    rows remain claim-grade; no row silently upgrades or downgrades timing surface; website tables
    show tier semantics clearly.
  - Verification: validator tests for stage inclusion, route formulas, proof linkage, stale-proof
    detection, publication artifact regeneration, targeted benchmark promotion checks, and website
    static validation.
  - Claim/fallback boundary: hot-runtime rows are optimization timing context; publication-proof
    rows remain the claim-grade evidence surface. Proof deferral affects claim status only, never
    execution semantics.

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Open | `PERF-DESIGN-2`, `PERF-DESIGN-2-A`, `PERF-DESIGN-4R`, `PERF-DESIGN-1R`, `PERF-DESIGN-5R`, `PERF-DESIGN-5R-A`, `PERF-DESIGN-6R`, `PERF-DESIGN-6R-A`, `PERF-DESIGN-6R-B`, `PERF-DESIGN-6R-C`, `PERF-DESIGN-3`, `PERF-DESIGN-3-A` | Execute in cohesive runtime/performance batches, not slivers. |
| Historical | PR #1174 benchmark row/readiness context, repo-wide audit closeout, release-sequence closeout, and completed benchmark/profile, sub-evidence, user-surface proof | Preserved in `docs/architecture/phased-execution-completed-ledger.md`; do not treat as active work. |
| Mapped, not autonomous queue | Unchecked global architecture review rows | Governed by `docs/architecture/global-architecture-review.md` and `docs/architecture/runtime-gap-family-burn-down.md`; promote concrete implementation items here before work begins. |
| Deferred approval/artifact gate | Public release/package and current benchmark publication | Requires maintainer approval, channel-specific proof, clean-source benchmark refresh, and passing hard gates before any public claim. |

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
- `PERF-DESIGN-1R`, `PERF-DESIGN-4R`, `PERF-DESIGN-5R`, and `PERF-DESIGN-6R` are the only current
  reopen passes. `PERF-DESIGN-2` and `PERF-DESIGN-3` are direct open implementation items.

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
