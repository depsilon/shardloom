# ShardLoom Phased Execution Plan

## How to maintain this file
- Keep actionable working items in Planned.
- Keep Completed as a pointer to `docs/architecture/phased-execution-completed-ledger.md`; do not
  place detailed completed session blocks in this file.
- Keep Planned in logical implementation order even when CG or phase numbers are out of order.
- Do not keep a separate Active section; the next autonomous work should be the next unchecked
  Planned checklist item after the queue has been ordered by current dependency and user value.
  If the top item no longer matches the current implementation priority, reorder Planned first.
- Move completed session blocks to the top of
  `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion; do not
  reshuffle older completed history unless the content is incorrect.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Planned Item Detail Standard

Every unchecked Planned item must be detailed enough for an autonomous Codex session to execute
without guessing.

A Planned item is sufficiently detailed only if it names:

- Source: governing RFC, architecture doc, benchmark report, issue, PR, or review finding.
- Current state: what exists today and what is still unsupported/report-only.
- Next slice outcome: the exact result expected from the next PR/session.
- User-visible surface: CLI, Python, benchmark, docs, API, capability view, evidence artifact, or
  release gate.
- Implementation scope: files/modules/commands expected to change.
- Evidence required: correctness, benchmark, execution-certificate, Native I/O,
  materialization/decode, policy, no-fallback, release/security evidence as applicable.
- Acceptance: observable conditions that make the item done.
- Verification: exact commands/tests/snapshots expected.
- Non-goals: what must not be implemented in this slice.
- Claim boundary: what can and cannot be claimed after completion.
- Fallback boundary: expected `fallback_attempted=false` and `external_engine_invoked=false`
  behavior.
- Ledger rule: when complete, move the detailed completed session to
  `docs/architecture/phased-execution-completed-ledger.md`.

Do not leave planned work as a bare statement such as "`<thing>` remains incomplete." Convert broad
items into one or more evidence-bearing implementation slices. Split a Planned item when it includes
`full`, `broad`, `general`, `production`, `universal`, `distributed`, `runtime`, `platform`,
`lakehouse`, `object-store`, `SQL/DataFrame`, `claim`, `release`, `Foundry`, or `REST` without an
immediate concrete scope. A split item should use child IDs such as `GAR-0032-A`; each child must be
implementable in one focused PR or explicitly marked `report-only`, `planning-only`, or
`diagnostic-only`.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests/snapshots/release checks exist, evidence refs are attached where claims are made,
unsupported paths remain explicit, no fallback engine was invoked, completed details are moved to the
completed ledger, and supporting docs are updated without becoming a second active queue.

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
- Supporting docs may contain rationale, inventories, traceability, and historical notes, but they
  must not introduce a second current queue.
- Repeated support, claim-boundary, benchmark-interpretation, and runtime-state explanations should
  be owned by one canonical doc or generated data artifact; other pages should link to or render
  that source instead of restating parallel wording.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- Supporting docs must not keep unchecked implementation checklists outside this file and
  `docs/architecture/global-architecture-review.md`. Scope-boundary lists may remain, but real work
  must be carried by a `GAR-*` item below.

Reference index:
- Status source: `README.md`, `docs/architecture/phased-execution-completed-ledger.md`,
  `docs/architecture/rfc-phase-traceability.md`, `docs/architecture/global-architecture-review.md`,
  `docs/architecture/compute-engine-flow-reference.md`, and
  `docs/architecture/website-minimal-public-surface-reset.md`.
- Website redesign references:
  `docs/architecture/website-redesign-reference-synthesis.md`,
  `docs/architecture/website-redesign-information-architecture.md`, and
  `docs/architecture/website-redesign-content-model.md`, and
  `docs/architecture/website-redesign-framework-decision.md`.
- Compute-flow and benchmark references:
  `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `docs/architecture/benchmark-persistent-runner-decision.md`,
  `docs/architecture/performance-attribution-and-execution-structure.md`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `docs/architecture/benchmark-competitive-claim-evidence.md`, and `docs/benchmarks/*`.
- Runtime architecture references:
  `docs/architecture/runtime-evidence-level-tiering.md`,
  `docs/architecture/evidence-aware-logical-optimizer.md`,
  `docs/architecture/vortex-scan-pushdown-completion.md`,
  `docs/architecture/compressed-encoded-kernel-registry.md`,
  `docs/architecture/fused-operator-pipeline.md`,
  `docs/architecture/in-process-session-runtime.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/architecture/allocation-buffer-pool-optimization.md`,
  `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`,
  `docs/architecture/dynamic-work-shaping.md`,
  `docs/architecture/spill-reservation-lifecycle-integration.md`, and
  `docs/architecture/effect-budget-plan.md`.
- Claim, release, and adoption references:
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/operational-evidence-policy-hardening.md`,
  `docs/architecture/evidence-native-generated-execution-observability-confidence.md`,
  `docs/architecture/adoption-commercial-readiness-friction-reduction.md`,
  `docs/architecture/workspace-feature-build-matrix.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/competitive-replacement-sufficiency-gate.md`,
  `docs/architecture/cg5-cg6-stateful-reuse-evidence-expansion.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/release-architecture-tracker-gate.md`,
  `docs/release/final-release-rehearsal.md`,
  `docs/architecture/universal-import-deployment-baseline-harness.md`,
  `docs/architecture/extension-manifest-effect-capability-matrix.md`,
  `docs/architecture/credential-policy-enforcement-gate.md`,
  `docs/architecture/sandbox-governance-runtime-readiness.md`,
  `docs/architecture/plugin-abi-udf-sandbox-blocker.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/architecture/rfc-coverage-followthrough.md`,
  `docs/architecture/typed-command-result-envelope.md`,
  `docs/architecture/crate-posture-public-exports.md`, and `docs/release/*`.
- Compatibility, adapters, and platform references:
  `docs/architecture/universal-input-contract.md`,
  `docs/architecture/universal-compatibility-coverage-scoreboard.md`,
  `docs/architecture/object-store-request-planner.md`,
  `docs/architecture/table-intelligence-layer.md`,
  `docs/architecture/lakehouse-value-prop-compatibility.md`,
  `docs/architecture/incumbent-gap-opportunity-map.md`,
  `docs/architecture/agent-contract-pack.md`, and `docs/use-cases/*`.
- Vortex and project hygiene references:
  `docs/architecture/vortex-public-api-inventory.md`,
  `docs/architecture/vortex-runtime-utilization-audit.md`,
  `docs/architecture/vortex-adapter-integration-plan.md`,
  `docs/architecture/vortex-upstream-alignment-hardening.md`,
  `docs/architecture/canonical-terminology.md`, `docs/architecture/systems-learning-map.md`,
  `docs/architecture/repo-cleanup-backlog.md`,
  `docs/architecture/diagnostics-normalization-backlog.md`,
  `docs/architecture/terminology-consolidation-backlog.md`,
  `docs/architecture/feature-footprint-doctor-plan.md`, and
  `docs/skills/vortex/vortex-first-provider-check.md`.

Reference-doc rule: these files are evidence, guardrails, or inventories. They do not authorize
runtime behavior, support claims, dependency expansion, package publication, external effects, or
fallback execution unless a matching unchecked item below is completed with evidence and moved to
the ledger.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

Autonomous ordering rule:

1. Finish the unchecked non-runtime closeout queue first.
2. Then work the runtime implementation queue.
3. Runtime queue items must explicitly enable an end-user runtime path, a runtime admission/blocker
   that protects user-visible behavior, or a validator that gates runtime claims. Docs-only or
   report-only work cannot complete a runtime item unless the item is explicitly a runtime-safety
   blocker.

Live plan hygiene:

- Planned must contain only unchecked actionable work. Completed checklist items, completed
  sections, and completed session details belong only in
  `docs/architecture/phased-execution-completed-ledger.md`.
- If a completed item is found in Planned, remove it from this file after confirming the matching
  ledger entry exists or adding that ledger entry.
- Do not leave a completed parent section in Planned just to preserve history. Keep only active
  child work or a short pointer to the ledger when history is needed.
- Do not start a runtime implementation item while unchecked non-runtime closeout items remain
  above it unless the user explicitly reprioritizes and the reprioritization is recorded here.
- A runtime item is valid only when it has a `Runtime enablement:` field that names the runnable
  path, admission/blocker, or validator it enables. If that field cannot be made concrete, the item
  belongs in non-runtime planning or the completed ledger, not the runtime queue.

### Global Architecture Review Carry-Forward

Source: `docs/architecture/global-architecture-review.md`.

Scope: every unchecked RFC and compute-flow review item is mirrored here so no planned,
unsupported, or not-claimable architecture work exists only in a supporting document. Complete these
items in logical implementation order, update the global review checkbox when evidence closes, and
move the completed session details to `docs/architecture/phased-execution-completed-ledger.md`.

Default GAR verification for planning-only/docs slices:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
git diff --check
```

Code-bearing GAR slices must add the focused Rust/Python/benchmark tests named in the slice and
usually end with:

```powershell
cargo fmt --all -- --check
cargo test --workspace --all-targets
python -m compileall -q python/src python/tests scripts examples
git diff --check
```

#### GAR-P0 - Execution Mode, Provider Admission, And Vortex Spine

P0 slices must preserve the canonical execution-mode vocabulary from
`docs/architecture/compute-engine-flow-reference.md`: `auto`, `compatibility_import_certified`,
`prepared_vortex`, `native_vortex`, and `direct_compatibility_transient`. Benchmark interpretation
must continue to report stage timing fields (`source_read_millis`, `compatibility_parse_millis`,
`compatibility_to_vortex_import_millis`, `vortex_write_millis`, `vortex_reopen_millis`,
`vortex_scan_millis`, `operator_compute_millis`, `result_sink_write_millis`,
`evidence_render_millis`, and `total_runtime_millis`) so compatibility rows are interpreted as
ingest/stage/certification work, not pure query speed. Do not add a hidden global fast-mode toggle.

#### Non-Runtime Closeout Queue

Complete these documentation, capability, security, release, and claim-gate items before starting
new runtime implementation work unless the user explicitly reprioritizes. These items must not add
runtime behavior or support claims. Add a concrete unchecked item here only when a new
documentation, website, security, release, or claim-gate blocker must interrupt runtime work.

Current non-runtime sequence: complete the review-derived action items below before new runtime
expansion unless the user explicitly reprioritizes. Completed non-runtime history belongs in
`docs/architecture/phased-execution-completed-ledger.md`.

#### Runtime Implementation Queue - Runtime-Enabling Work Only

The earlier broad runtime rollup queues have been consolidated into the implementation-ready
`GAR-RUNTIME-IMPL-4*` and `GAR-RUNTIME-IMPL-5*` queues below. Work these only after the
unchecked non-runtime closeout items above are complete or explicitly reprioritized by the user.

Runtime completion rule:

- Every runtime item must enable a concrete runtime behavior, runtime admission/blocker, or
  runtime-claim validator that directly protects a usable workflow.
- Every runtime item below must include a `Runtime enablement:` field naming the behavior,
  admission/blocker, or validator it enables.
- A docs-only or report-only update cannot complete a runtime item unless the item explicitly says
  it is a runtime-safety blocker or validator.
- Completed runtime details belong in `docs/architecture/phased-execution-completed-ledger.md`, not
  in this live queue.

#### GAR-RUNTIME-IMPL-4 - Final Full-Runtime Implementation Leaf Queue

This queue exists to keep the remaining "fully functional / usable compute engine" work from
hiding inside broad architecture items. Treat these as the explicit runtime implementation slices
that must be worked before any full-runtime readiness claim. Each item below must land runnable
runtime behavior, deterministic runtime admission/blockers, or runtime-claim validation; planning
or documentation updates alone are insufficient.

- [ ] GAR-RUNTIME-IMPL-4D expression, cast, null, string, date, and timestamp runtime families
  - Source: RFC 0021, SQL/Python local runtime smokes, expression/operator semantics,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: scoped SQL/Python local-source expression coverage has moved well past the first
    predicate/projection leaves; detailed completed 4D slices live in
    `docs/architecture/phased-execution-completed-ledger.md`. Scoped local-source computed
    projections now also admit `SELECT *` plus computed/literal projection outputs, so Python
    `read_csv(...)`, flat `read_json(...)`, and feature-gated flat scalar structured readers can
    lower `with_column(...).filter(...).sort(...).limit(...)` without requiring an explicit
    `select(...)`; computed-projection top-N now sorts projected rows by computed aliases and can
    still sort by source columns when the source column is not projected. Scoped scalar/grouped
    aggregate `HAVING` now evaluates admitted predicates over emitted aggregate output rows for
    local-source SQL/Python and join-aggregate paths, with deterministic blockers when `HAVING`
    references non-output source columns.
    The remaining work is the parity gap
    around broader non-numeric/generalized expression families, broader coercion/function coverage,
    HAVING expressions over unprojected aggregate functions, interval/date-time and
    timezone-database semantics, correlated/multi-column/nested subquery semantics, arbitrary
    predicate-tree completeness beyond the currently admitted leaves, and final SQL/Python
    ergonomics. Unsupported residual work must continue to fail with deterministic
    no-fallback diagnostics.
  - Closeout posture: this parent item remains open for the residual parity gaps above.
    A future closeout PR must either implement those gaps or split each non-goal into separate
    follow-on runtime items before marking `GAR-RUNTIME-IMPL-4D` complete.
  - Next slice outcome: add one implementation PR per remaining expression family: remaining
    non-numeric expression/function families, richer IN semantics only where evidence-backed,
    timestamp/timezone helpers, interval/date-time completeness where admitted, and broader typed
    coercions/functions.
  - Runtime enablement: executable ShardLoom-native expression families or deterministic runtime
    blockers for unsupported operators.
  - User-visible surface: SQL/Python query builder, explain output, capability matrix, docs.
  - Implementation scope: expression IR, type coercion policy, null semantics, parser lowering,
    native evaluators, diagnostics.
  - Vortex 0.71/0.72 opportunity mapping:
    - Pluggable struct cast informs ShardLoom-native cast/coercion admission only after local
      correctness tests and output evidence exist.
    - Variant array and `VariantGet` inform nested/semi-structured expression blockers and later
      scoped runtime support.
    - `DType::Union` must remain explicit unsupported/runtime-blocked until union semantics,
      nullability, schema reporting, and output evidence are implemented.
    - Statistic expression support can inform metadata-first expression planning, but cannot become
      a correctness or performance claim by itself.
  - Evidence required: expression family, input/output dtype, null policy, cast status, decoded/
    materialized flags, correctness digest, no-fallback fields.
  - Acceptance: every admitted expression has fixture coverage and unsupported expressions report a
    deterministic diagnostic.
  - Verification: expression unit tests, SQL/Python smoke tests, unsupported snapshots, release
    readiness metadata.
  - Non-goals: no arbitrary UDFs, regex parity, timezone completeness, or ANSI SQL claim.
  - Claim boundary: expression-family support per admitted dtype/operator.
  - Fallback boundary: expression evaluation must remain ShardLoom-native.
  - Dependencies/blockers: expression IR stability, dtype coercion policy, decoded-reference
    fixtures, and SQL/Python lowering.
  - Ledger rule: ledger entry must enumerate expression families, dtypes, and blockers.

- [ ] GAR-RUNTIME-IMPL-4E generated-source builders as ordinary local runtime
  - Source: `GAR-GEN-1`, `GAR-COMPAT-1B`, Use Case Atlas generated-source rows.
  - Current state: scoped local JSONL/CSV generated-output smokes now exist for `from_rows`,
    `literal_table`, `calendar`, `range`, `sequence`, SQL `VALUES`, SQL literal `SELECT`, scoped
    SQL `SELECT * FROM generate_series/range(...)`, scoped SQL value-column/int64 arithmetic
    projections from `generate_series/range(...)`, and Python generated-row projection/literal
    `with_column` before local writes with generated-source/output/no-fallback evidence. Those
    generated-source surfaces also expose feature-gated flat scalar Parquet, Arrow IPC, Avro, and
    ORC local sinks through `write_parquet(...)`, `write_arrow_ipc(...)`, `write_avro(...)`,
    `write_orc(...)`, and `--output-format` when `shardloom-cli` is built with
    `--features universal-format-io`; default builds return deterministic structured-sink blockers.
    The same generated-source commands and Python helpers now admit local `.vortex` output through
    `--output-format vortex` and `write_vortex(...)` when built with `--features vortex-write`,
    emitting Vortex artifact digest, reopen proof, upstream Vortex writer/scan flags, and
    `certified_local_vortex_sink`; default builds return a deterministic Vortex sink blocker.
    Generated-source commands also admit `--fanout-output format=local-path`; Python generated
    rows, generated range/sequence, generated range SQL, and source-free SQL expose `.fanout(...)`
    by treating the first requested sink as the primary output and all remaining sinks as write
    boundary fanouts over the same computed generated rows. Each written generated artifact emits
    replay/digest evidence, fidelity-loss evidence, workspace path-safety evidence, fanout result
    reuse fields, and no-fallback fields.
    Python range/sequence builders also support `limit(...)`, `head(...)`, and `take(...)` by
    adjusting generator bounds before invoking the same engine-native range/sequence smoke. Scoped
    source-free SQL `generate_series`/`range` runtime now admits direct range-column output, int64
    literals, range-column `+`/`-`/`*` int64 arithmetic, single-branch int64 CASE projections, and
    optional `WHERE <range-column> <comparison> <int64>`, `ORDER BY <range-or-output-column>
    [ASC|DESC]`, and `LIMIT <count>` clauses with `sql_source_free_projection_*`,
    `sql_source_free_filter_*`, `sql_source_free_order_by_*`, `sql_source_free_top_n_*`, and
    `sql_source_free_limit_*` evidence. The Python
    `ctx.range(...).filter(...).with_column(...).sort(...).limit(...).write(...)` workflow lowers
    to that same generated-source SQL runtime while preserving the caller-facing range column alias.
    Remaining gaps are broader SQL source-free projection beyond that admitted range-generator
    subset, arbitrary SQL table functions, broad DataFrame expression-backed projection/
    `with_column`, object-store/Foundry generated-output paths, broader structured-format fidelity,
    persistent OutputPlan reuse, and claim-grade output coverage.
  - Next slice outcome: implement broader generator/expression coverage and persistent reusable
    OutputPlan replay policy where admitted.
  - Runtime enablement: end-user generated-source execution that writes local output and emits a
    GeneratedSourceCertificate.
  - User-visible surface: Python `ctx.range`, `ctx.from_rows`, `ctx.literal_table`, `ctx.calendar`,
    generated-source `write_vortex(...)`, fluent `ctx.range(...)` filter/with-column/sort/limit
    writes and fanout, `ctx.sql(...).write(...)`, `ctx.sql(...).fanout(...)`, SQL `VALUES`, SQL
    `generate_series`/`range`, generated-output recipes.
  - Implementation scope: generator nodes, schema inference, deterministic seed/row-count handling,
    output writer bridge, report/certificate fields.
  - Evidence required: `input_dataset_count=0`, `source_io_performed=false`,
    `generated_source_created=true`, generated source kind/schema/row/plan digest, seed,
    determinism flag, output certificate, Vortex output artifact/reopen fields where admitted,
    no-fallback fields.
  - Acceptance: no-input smoke remains separate; each admitted generator writes local output and
    exposes a GeneratedSourceCertificate.
  - Verification: CLI/Python/SQL generator tests, output/fanout/replay smoke, use-case coverage,
    release readiness metadata.
  - Non-goals: no object-store write, Foundry production claim, package publication, or broad
    SQL/DataFrame claim.
  - Claim boundary: local deterministic generated-output runtime only.
  - Fallback boundary: no generated rows or expressions may be produced by an external engine.
  - Dependencies/blockers: generated-source schema contract, local output writer registry,
    expression semantics, and Python/SQL surface admission.
  - Ledger rule: ledger entry must list generator kind, output format/fanout combinations, Vortex
    output feature gate, replay proof, and unsupported generators.

- [ ] GAR-RUNTIME-IMPL-4F UniversalIngress local/non-Vortex adapter runtime coverage by format
  - Source: `GAR-IOREUSE-1A`, universal compatibility scoreboard, local input adapter docs,
    `docs/architecture/vortex-public-api-inventory.md`,
    `docs/architecture/universal-ingress-route-taxonomy.md`.
  - Current state: CSV is the strongest local smoke path; scoped flat JSONL/NDJSON and flat
    top-level `.json` object/array local input are now runtime-admitted through
    `sql-local-source-smoke` with SourceState-style evidence, route fields, content fingerprints,
    schema digests, source-format-aware adapter IDs, and deterministic blockers for nested JSON
    values. Feature-gated local flat scalar `.parquet`, `.arrow`, `.ipc`, `.feather`, `.avro`,
    and `.orc` input is also runtime-admitted when `shardloom-cli` is built with
    `--features universal-format-io`; default builds return deterministic Parquet, Arrow IPC,
    Avro, or ORC adapter blockers. The Python
    query-builder can lower local flat `.json`, `.jsonl`, `.ndjson`, and feature-gated
    `.parquet`/Arrow IPC/Avro/ORC
    projection/optional-filter/limit,
    preview/select-star, scalar aggregate/optional-filter/limit with aliases, multi-key group-by
    aggregate/optional-filter/limit plus post-aggregate HAVING over aggregate output rows,
    multi-key scalar top-N workflows, scoped local-source
    join bridges covering inner, left/right/full outer, left semi/anti, and cross joins,
    computed projections and multi-key scalar
    top-N over joined rows, and scalar/grouped join aggregates with optional post-aggregate HAVING
    into that runtime path.
    Local-source evidence labels for CSV versus JSON versus JSONL/NDJSON versus admitted
    Parquet/Arrow IPC/Avro/ORC
    source certificate refs, execution certificate refs, materialization boundaries, pushdown status,
    adapter status, route status, and claim reasons are source-format-aware. Direct transient SQL
    reads now derive a local SourceState read plan from parsed projections, predicates, aggregates,
    group-by, top-N, computed projections, and IN-subquery source columns; reports emit
    `shardloom.local_source_state.v1`, local adapter-registry version, requested/materialized
    columns, reader projection columns, pruning status, projection-pushdown status,
    parse-normalization family, and scalar-row materialization layout. Feature-gated
    Parquet/Arrow IPC/Avro/ORC adapters now apply reader-level projection before scalar-row
    conversion for required-column read plans; Avro `COUNT(*)` uses a one-column row-count anchor
    because the Arrow Avro reader does not emit zero-column record batches. The feature-gated
    `vortex_ingest` prepare-once path now preserves flat scalar Parquet/Arrow IPC/Avro/ORC inputs
    as Arrow `RecordBatch` columnar SourceState when both `vortex-write` and
    `universal-format-io` are enabled, emits columnar-preservation, record-batch,
    source-to-columnar, and Vortex array-build evidence, and still falls back to scalar paths only
    where scalar rows are the admitted representation. Feature-gated direct-transient
    `sql-local-source-smoke` structured readers now carry the same columnar SourceState boundary
    through reader projection and disclose the explicit scalar-row expression-runtime consumption
    boundary with format-neutral SQL/Python runtime scope, `format_specific_compute_path=false`,
    `source_state_columnar_preserved`, record-batch count, source-to-columnar timing, runtime
    consumption layout, and scalar materialization-required evidence.
    Nested/general JSON, broader Parquet/Arrow IPC/Avro/ORC type/nesting and output coverage does
    not all have ordinary user-facing SourceState runtime parity.
  - Next slice outcome: continue promoting remaining local input and operator combinations one at
    a time into UniversalIngress/InputAdapter registry coverage with SourceState evidence,
    `vortex_ingest_status`, certified route status, and deterministic blockers for unsupported
    formats/features. The next optimization step is extending columnar SourceState reuse from the
    prepare-once route into repeated prepared workflows and benchmark rows without adding a hidden
    Arrow-default execution model. Recent join slices should keep using the same local-source
    admission universe instead of creating CSV-only islands unless a format has a deterministic
    blocker.
  - Runtime enablement: admitted local input adapters that create reusable SourceState evidence for
    actual user reads and can feed `vortex_ingest` into `VortexPreparedState` when preparation is
    admitted.
  - User-visible surface: CLI/Python read helpers, use cases, capability/status matrix, benchmark
    source-format rows.
  - Implementation scope: format detection, local reader, schema/dtype inference, fingerprinting,
    SourceState digest, decode/materialization evidence.
  - Vortex 0.71/0.72 opportunity mapping:
    - Pluggable Arrow input kernel registry is a candidate adapter boundary, not a default
      decode-to-Arrow execution path.
    - `DType::Union`, Variant, and extension dtype metadata fixes should feed schema/dtype
      diagnostics and SourceState schema reporting before any runtime admission.
    - Arrow-to-Vortex C FFI conversion remains blocked for current Rust local runtime and belongs
      to future ABI/interoperability review only.
  - Evidence required: source format/location/fingerprint, source adapter status/blocker, schema
    digest, SourceState id/digest, `vortex_ingest_status`, `compatibility_import_certified_status`,
    row count/file count/bytes, decode/materialization status, no-fallback fields.
  - Acceptance: each listed format is either runnable with evidence or blocked with actionable
    diagnostics; adapter support never implies Vortex-native execution and `prepared_vortex` never
    accepts the non-Vortex source directly.
  - Verification: per-format smoke tests, schema snapshot tests, unsupported diagnostics,
    benchmark harness contract tests.
  - Non-goals: no object-store, database, table/lakehouse, or universal adapter claim.
  - Claim boundary: local file support per admitted format and feature subset.
  - Fallback boundary: no external engine may parse, plan, or execute input workloads.
  - Dependencies/blockers: reader dependency/license approval, source fixtures, schema inference
    coverage, and SourceState schema fields.
  - Ledger rule: ledger entry must include the per-format support table.

- [ ] GAR-RUNTIME-IMPL-4F1 compatibility import certified optimization and `vortex_ingest`
      attribution
  - Source: review of `compatibility_import_certified` bottlenecks, `traditional-analytics-run`,
    `shardloom-cli/src/vortex_ingest.rs`, benchmark timing evidence,
    `GAR-RUNTIME-IMPL-4F`, `GAR-RUNTIME-IMPL-4K`, and `GAR-RUNTIME-IMPL-4M`.
  - Current state: `compatibility_import_certified` is the certified cold ingest/stage route,
    feature-gated `vortex_ingest` creates scoped local `VortexPreparedState` artifacts, and local
    prepared/native benchmark rows can reuse those artifacts without treating preparation time as
    warm-query timing. The active CLI/Python surface includes
    `traditional-analytics-prepare-batch-run`,
    `ShardLoomClient.traditional_analytics_prepare_batch_run(...)`,
    `ShardLoomClient.prepare_and_run_traditional_analytics_vortex_batch(...)`, and the
    `shardloom-prepare-batch` comparative-harness lane, which prepare local compatibility inputs
    once and run a prepared Vortex scenario batch in the same process with explicit preparation,
    query, reuse, no-fallback, and claim-boundary evidence.
  - Remaining gap: direct-transient local CSV/JSON paths and several generated/admitted
    local-source workflows still cross scalar row-map normalization without a reusable columnar
    SourceState boundary, and feature-gated Parquet/Arrow IPC/Avro/ORC direct-transient workflows
    still materialize scalar rows for the scoped expression runtime after preserving reader-level
    columnar ingress evidence. The prepare-once route is local/scoped evidence only; it is not a
    persistent cache, object-store/table workflow, SQL/DataFrame production runtime, performance
    claim, or package-readiness claim.
  - Next slice outcome: keep reducing the UniversalIngress/adapter bottleneck by carrying
    columnar SourceState into more direct-transient and generated/admitted local-source paths, while
    preserving certification-depth policy and claim-safe cold/warm timing separation.
  - Runtime enablement: certified ingest/stage execution remains supported, and repeated local
    benchmark/workflow commands can certify or prepare once and then run `prepared_vortex` from
    `VortexPreparedState`.
  - User-visible surface: benchmark rows, CLI JSON evidence, Python typed reports, website
    benchmark interpretation, compute-flow docs.
  - Implementation scope: `traditional-analytics-run`, `vortex_ingest` evidence, benchmark harness
    row schema, website benchmark rendering, Python report surfaces, contract tests.
  - Evidence required: exclusive `source_stat_millis`, `source_read_millis`,
    `source_parse_millis`, `source_to_columnar_millis`, `vortex_array_build_millis`,
    `vortex_write_millis`, `vortex_digest_millis`, `vortex_reopen_verify_millis`,
    `vortex_scan_millis`, `operator_compute_millis`, `result_sink_write_millis`,
    `evidence_render_millis`, `total_runtime_millis`, `timing_scope`,
    `preparation_included`, `query_timing_starts_after_preparation`, `certification_level`,
    `source_state_id`, `source_state_digest`, `source_state_materialization_layout`,
    `source_state_parse_normalization`, `source_state_columnar_preserved`,
    `source_state_record_batch_count`, `prepared_state_id`, `prepared_state_digest`,
    `vortex_array_build_provider_kind`, `vortex_array_build_provider_surface`,
    `vortex_array_build_strategy`, `vortex_array_build_input_layout`,
    `vortex_array_build_record_batch_count`,
    `vortex_array_build_manual_scalar_copy_avoided`,
    `prepared_state_created`, `prepared_state_reused`, `prepared_state_reuse_hit`,
    `invalidation_reason`, `source_fingerprint_kind`, `source_content_digest`, `schema_digest`,
    `plan_digest`, `fallback_attempted=false`, `external_engine_invoked=false`, and
    `claim_gate_status`.
  - Acceptance: `compatibility_import_certified` rows disclose `timing_scope=
    cold_certified_end_to_end`, `preparation_included=true`, and certification depth; legacy
    cumulative fields are either replaced with exclusive fields or explicitly marked as cumulative;
    `prepared_vortex` rows reference `VortexPreparedState` and do not perform `vortex_ingest` inside
    warm-query timing; `ingest_minimal` cannot become claim-grade; `ingest_full_replay` requires
    replay/output evidence; unsupported format/features emit deterministic blockers. Remaining
    optimization work must not remove compatibility certification, weaken no-fallback fields, or
    present attribution-only changes as performance improvements.
  - Verification: `cargo test -p shardloom-contract-tests --test traditional_benchmark_harness`,
    `cargo test -p shardloom-contract-tests --test release_readiness_metadata`, focused
    `vortex_ingest` tests, Python report tests, website readiness, benchmark artifact completeness,
    `git diff --check`.
  - Non-goals: do not remove `compatibility_import_certified`; do not make it the default speed
    route; do not claim performance improvement without fresh evidence; do not add object-store,
    table/lakehouse, Foundry production, broad SQL/DataFrame, or package-publication support.
  - Claim boundary: route/timing/evidence optimization only; performance and superiority claims
    remain blocked until fresh workload-scoped claim-grade evidence exists.
  - Fallback boundary: no source parse, Vortex ingest, certification, replay, or prepared query may
    use pandas, Polars, DuckDB, DataFusion, Spark, Dask, Ray, databases, warehouses, or managed
    platforms as fallback execution.
  - Dependencies/blockers: stable `vortex_ingest` lifecycle, SourceState/VortexPreparedState
    digests, OutputPlan separation, certification-depth policy, and benchmark renderer support.
  - Ledger rule: ledger entry must include the timing field contract, certification levels, reuse
    fields, unsupported blockers, and any measured artifact refs; do not mark complete from docs
    alone.

- [ ] GAR-RUNTIME-IMPL-4G local output writer registry and fanout promotion
  - Source: OutputPlan, result-sink replay proof, cross-format fanout architecture,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: scoped local SQL/Python output can write local JSONL and CSV sinks with
    format-specific certificate fields, and feature-gated flat scalar Parquet, Arrow IPC, Avro,
    and ORC sink slices are admitted through `sql-local-source-smoke` / Python
    `write_parquet(...)`, `write_arrow_ipc(...)`, `write_avro(...)`, and `write_orc(...)` when
    `shardloom-cli --features universal-format-io` is used. Scoped local-source SQL/Python output
    can also write local `.vortex` sinks through `--output-format vortex` / `write_vortex(...)`
    when `shardloom-cli --features vortex-write` is used, emitting artifact digest, row/column
    proof, upstream Vortex writer/reopen flags, and `certified_local_vortex_sink`; default builds
    return deterministic Vortex sink blockers. Scoped local-source SQL/Python fanout can write one
    computed result to multiple local JSONL/CSV sinks, plus feature-gated flat scalar
    Parquet/Arrow IPC/Avro/ORC/Vortex sinks, with per-output bytes, digest, certificate evidence,
    Vortex output evidence where applicable, an `output_plan_digest`, local artifact replay
    verification, replay timing/status fields, and scoped output fidelity/loss reporting for
    admitted local sinks. Source-free generated-output fanout now follows the same write-boundary
    contract for generated rows, generated range/sequence, generated range SQL, and source-free SQL
    by reusing one computed generated result for primary plus fanout sinks. Broader type/nesting and
    metadata fidelity for those compatibility exports, persistent OutputPlan registry reuse, and
    claim-grade fanout are not complete.
  - Next slice outcome: add persistent reusable OutputPlan registry consolidation, replay policy
    levels, and claim-grade fanout admission for formats whose metadata/fidelity proof is complete.
  - Runtime enablement: local output writers and fanout execution with OutputPlan evidence and
    replay proof where admitted.
  - User-visible surface: CLI/Python `.write` and `.fanout`, recipes, benchmark
    `io_reuse_and_fanout`, website status.
  - Implementation scope: OutputPlan builder, Vortex writer promotion, schema translation,
    output digests, replay verifier, fanout orchestration, and broader writer registry
    consolidation.
  - Vortex 0.71/0.72 opportunity mapping:
    - Pluggable Arrow export kernels may inform compatibility-output boundaries, but export remains
      translation/fanout and cannot execute unsupported compute.
    - Local async file write behavior and `Executor::spawn_io` are candidates for later explicit
      output lifecycle admission with side-effect, certificate, and replay evidence.
    - Struct cast and extension dtype metadata fixes should inform schema-translation blockers
      before any writer/fanout support is claimed.
  - Evidence required: output plan id/digest, format/location/schema, write timing, replay status,
    metadata fidelity/loss, correctness digest, no-fallback fields.
  - Acceptance: one admitted input/prepared state can write multiple local outputs; unsupported
    writers and object-store sinks block deterministically.
  - Verification: writer smoke per format, fanout smoke, replay tests, use-case coverage,
    benchmark contract tests.
  - Non-goals: no object-store write, table commit, production sink claim, or performance claim.
  - Claim boundary: local output/fanout support per admitted format.
  - Fallback boundary: compatibility output is export, not external-engine execution.
  - Dependencies/blockers: OutputPlan registry consolidation, schema translation/fidelity reports,
    replay verifier, generated/local/Vortex source evidence, and fanout benchmark fields.
  - Ledger rule: ledger entry must list format combinations and replay proof refs.

- [ ] GAR-RUNTIME-IMPL-4I Vortex scan pushdown and encoded-predicate runtime completion
  - Source: `GAR-PERF-2C`, Vortex Scan API docs, encoded predicate provider evidence,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: source-backed scan and encoded predicate evidence are scoped; CLI local Vortex
    primitive `vortex-project`, `vortex-filter`, and `vortex-filter-project` rows now emit a
    shared `scan_pushdown_*` contract with filter/projection/materialization/no-fallback fields and
    deterministic blockers. `vortex-filter-project --limit` now executes an admitted local Vortex
    filter/project scan with filter and projection pushed into upstream Vortex Scan, followed by an
    explicit ShardLoom-native source-order residual limit. The residual limit reports
    `scan_limit_pushed_down=false`, `scan_limit_pushdown_status=blocked_no_scan_limit_admission`,
    `scan_residual_limit_applied=true`, residual executor `shardloom_native`,
    `fallback_attempted=false`, and `external_engine_invoked=false`. Runtime provider evidence now
    records the active optional Vortex `0.72` dependency. Broad Vortex Scan limit/slice pushdown,
    encoded-native operator admission, and prepared/native scenario coverage are still incomplete.
  - Next slice outcome: lower filter, projection, and limit into Vortex Scan where admitted, and
    emit deterministic blockers or ShardLoom-native residual evidence when a predicate, projection,
    or limit cannot be pushed down.
  - Runtime enablement: prepared/native Vortex Scan pushdown for admitted filters, projections, and
    limits, with explicit ShardLoom-native residual execution or fail-closed blockers for
    unsupported shapes.
  - User-visible surface: prepared/native benchmark rows, explain output, capability matrix.
  - Implementation scope: scan request builder, filter expression lowering, projection mask, limit/
    slice pushdown, evidence fields.
  - Vortex 0.71/0.72 opportunity mapping:
    - Statistic expressions, stats rewrite sessions, `NullCount`, and `UncompressedSize` are
      candidates for metadata-first planning and scan evidence, not standalone runtime claims.
    - `register_splits` offset/relative row-range fixes should feed split-aware scan evidence and
      blockers.
    - `IsSorted` dtype fixes may inform sorted/min-max pruning and top-k blockers before any
      sorted-kernel runtime claim.
  - Evidence required: filter/projection/limit pushdown status, residual limit executor and
    row-count fields where limit is not admitted into Vortex Scan, filter/output columns read,
    encoded predicate provider fields, data decoded/materialized, Vortex provider version,
    no-fallback fields.
  - Acceptance: supported scenarios avoid reading unused output columns; unsupported pushdown does
    not silently fall back to full materialization; limit-like operators either push down through an
    admitted Vortex provider surface, execute as an explicitly reported ShardLoom-native residual,
    or block deterministically.
  - Verification: selective-filter smoke, filter/projection/limit smoke, source-backed scan tests,
    benchmark contract tests.
  - Non-goals: no encoded-native claim from pushdown evidence alone.
  - Claim boundary: pushdown support per admitted predicate/projection/limit shape.
  - Fallback boundary: residual work must be ShardLoom-native or blocked.
  - Dependencies/blockers: Vortex Scan API provider boundary, expression lowering, projection mask
    support, and source-backed scan evidence.
  - Ledger rule: ledger entry must list pushed-down and blocked expression shapes.

- [ ] GAR-RUNTIME-IMPL-4J encoded kernel registry execution pairs
  - Source: `GAR-PERF-2D`, RFC 0021, encoded execution docs,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: encoded-kernel evidence exists for selected scoped inputs; broad encoded-native
    operator coverage remains incomplete.
  - Next slice outcome: implement or block one encoding/operator pair at a time, starting with
    bitpacked boolean/integer filters, sequence equality/range, dictionary equality/group-by, and
    constant array count/filter.
  - Runtime enablement: executable encoded-kernel pairs backed by decoded-reference correctness, or
    deterministic blockers.
  - User-visible surface: benchmark evidence, explain output, capability matrix.
  - Implementation scope: kernel registry, admission policy, encoded evaluator, decoded reference
    comparison, blockers.
  - Vortex 0.71/0.72 opportunity mapping:
    - FastLanes signed bases, SparseArray iterative execution, mask/rank intersection
      improvements, smallvec performance fixes, and TurboQuant are candidate inputs for
      encoding/operator-pair admission. The `0.72` dependency update keeps TurboQuant blocked as
      capability metadata (`vortex_turboquant_vector_encoding`) until feature proof, vector dtype
      semantics, lossy-quantization policy, decode correctness, and no-fallback evidence land.
    - Sparse traversal remains blocked until source-backed segment extraction and certificate
      evidence exist.
    - CUDA/FSST and GPU fixes remain blocked future accelerator context, not CPU-local runtime
      support or a performance claim.
  - Evidence required: encoding id, operator family, kernel admitted/executed, canonicalization
    required, decoded/materialized flags, correctness digest, encoded-native claim flag.
  - Acceptance: supported pairs pass decoded-reference correctness; unsupported encodings block
    deterministically.
  - Verification: unit tests per pair, selective-filter/group-by benchmark smoke, capability
    snapshots.
  - Non-goals: no blanket encoded-native, vectorized parity, or performance claim.
  - Claim boundary: encoding/operator-pair support only.
  - Fallback boundary: decoded reference is a test oracle, not runtime fallback.
  - Dependencies/blockers: encoding fixtures, kernel registry admission, decoded-reference
    correctness harness, and benchmark row schema.
  - Ledger rule: ledger entry must enumerate pairs, claim flags, and blockers.

- [ ] GAR-RUNTIME-IMPL-4K unified execution envelope and certificate validators
  - Source: release readiness metadata, benchmark artifact policy, runtime evidence-level docs.
  - Current state: runtime reports have useful fields, but command, Python, benchmark, and website
    envelopes can diverge.
  - Next slice outcome: add a versioned execution-envelope schema and validators for every runtime
    path.
  - Runtime enablement: runtime-claim validator that rejects paths missing certificate,
    materialization/decode, claim-gate, or no-fallback fields.
  - User-visible surface: CLI JSON, Python typed reports, benchmark artifacts, website evidence,
    release readiness.
  - Implementation scope: shared schema, adapters, aliases/migrations, readiness checks, website
    renderer updates.
  - Evidence required: execution/engine/evidence mode, source/generated/output refs,
    route fields, SourceState/VortexPreparedState/OutputPlan refs, materialization/decode refs,
    certificate refs, no-fallback fields, claim gate.
  - Acceptance: missing fallback/certificate/claim fields fail validation; `prepared_vortex` rows
    without `VortexPreparedState` fail validation; compatibility certified rows without
    `cold_certified_end_to_end` timing fail validation; report-only rows cannot masquerade as
    runtime support.
  - Verification: schema contract tests, release readiness metadata, benchmark completeness,
    website readiness, Python typed-report tests.
  - Non-goals: no runtime capability or claim upgrade from schema work alone.
  - Claim boundary: evidence standardization only.
  - Fallback boundary: every envelope must expose `fallback_attempted` and
    `external_engine_invoked`; route fields must not hide an external engine or fallback boundary.
  - Dependencies/blockers: stable field naming, compatibility aliases, Python report migration, and
    benchmark/website validators.
  - Ledger rule: ledger entry must record schema version and migrated surfaces.

- [ ] GAR-RUNTIME-IMPL-4L ShardLoomSession, SourceState, PreparedState, and OutputPlan reuse runtime
  - Source: `GAR-IOREUSE-1`, `GAR-PERF-2F`, in-process session runtime docs.
  - Current state: scoped batch/session evidence exists, and Python now exposes a caller-owned
    `ShardLoomSession` for local `vortex_ingest` prepared-state reuse plus admitted local
    query-builder collect/write/fanout result reuse when source, output, and prepared-artifact
    fingerprints still match. Session SQL results surface SourceState id/digest, read-plan,
    projection-pushdown, and materialized/reader projection columns from the local source runtime.
    Broader CLI batch/session reuse, cross-workflow OutputPlan reuse, schema/dictionary cache reuse,
    buffer pools, object-store/table reuse, and non-local workflows are still planned.
  - Next slice outcome: extend the scoped in-process `ShardLoomSession` from prepared-state reuse
    into admitted SourceState, VortexPreparedState, schema/dictionary state, and OutputPlan reuse
    where fingerprints remain valid.
  - Runtime enablement: scoped in-process session runtime with safe source/prepared/output reuse and
    explicit invalidation.
  - User-visible surface: CLI batch/session command, Python context/session, benchmark timing rows.
  - Implementation scope: session lifecycle, cache keys/fingerprints, invalidation policy, cache
    hit/miss evidence, explicit close/cleanup.
  - Evidence required: session id, cache hit/miss, reuse digest/reason, source/prepared/output
    state ids, invalidation reason, no-fallback fields.
  - Acceptance: repeated admitted workflows reuse state safely; stale source/schema/plan changes
    invalidate cache; session state is explicitly scoped and closed; Python prepared-state and
    local query/output reuse remain fingerprint-gated and do not imply broad runtime/session
    support.
  - Verification: session smoke, invalidation tests, source/prepared/output reuse tests, benchmark
    harness contract tests.
  - Non-goals: no daemon/service, distributed cache, hidden fast mode, or performance claim.
  - Claim boundary: scoped in-process reuse only.
  - Fallback boundary: cache/session cannot change execution provider to an external engine.
  - Dependencies/blockers: fingerprint/invalidation contract, SourceState/VortexPreparedState/
    OutputPlan ids, explicit session lifecycle, and cache cleanup policy.
  - Ledger rule: ledger entry must list cache artifacts, invalidation rules, and disabled paths.

- [ ] GAR-RUNTIME-IMPL-4O object-store write and table/lakehouse commit ladder
  - Source: table/lakehouse commit semantics gate, object-store scale ladder.
  - Current state: object-store writes, table metadata/snapshot scans, append, merge/delete, commit,
    rollback, and catalog integration are blocked or report-only.
  - Next slice outcome: after read proof, implement staged write/commit/recovery in an approved
    provider/emulator, then one fixture-backed table metadata/snapshot operation and one append or
    commit rehearsal where admitted.
  - Runtime enablement: staged object-store write/table operation runtime in declared fixture
    profiles, with commit and rollback evidence.
  - User-visible surface: table/object-store capability views, CLI/Python diagnostics, status/use
    cases, scale benchmark rows.
  - Implementation scope: write staging, commit protocol, idempotency, cleanup/retry, table metadata
    adapter, snapshot reader, manifest writer or commit rehearsal.
  - Evidence required: provider/profile, table format, snapshot id, manifest/data-file counts,
    commit protocol/status, rollback/cleanup status, idempotency key, no-fallback fields.
  - Acceptance: read/write/commit and metadata/read/append/commit are separate gates; fixture proof
    does not imply production lakehouse support.
  - Verification: policy tests, emulator write smoke, table fixture tests, commit rehearsal smoke,
    unsupported diagnostics, release readiness.
  - Non-goals: no blanket S3/GCS/ADLS support, production Iceberg/Delta/Hudi claim, catalog
    service, or production table claim.
  - Claim boundary: provider/table-format operation in declared fixture/profile only.
  - Fallback boundary: no external catalog, lakehouse engine, or query engine executes work.
  - Dependencies/blockers: object-store read proof, commit/recovery policy, table fixtures,
    dependency/license review, and idempotency evidence.
  - Ledger rule: ledger entry must list provider, table format, operation, and blocked behaviors.

- [ ] GAR-RUNTIME-IMPL-4P scale-grade local split, memory, spill, shuffle, and retry runtime
  - Source: `GAR-SCALE-1`, RFC 0014, RFC 0016, RFC 0017,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: scale contracts exist, but larger-than-memory, split-parallel, spill, shuffle,
    retry, and idempotent output commit runtime are not claimable.
  - Next slice outcome: implement a declared-resource local scale profile with SplitManifest,
    bounded memory checks, per-split execution, spill/backpressure where admitted, one shuffle
    family, retry/idempotency, and output commit evidence.
  - Runtime enablement: local scale-grade execution under a declared resource envelope, including
    split, memory, spill, shuffle, retry, and commit gates.
  - User-visible surface: scale benchmark profiles, CLI/Python execution envelopes, status page.
  - Implementation scope: split scheduler, memory budget, spill manager, shuffle plan, retry/
    cancellation/recovery, output commit status, scale benchmark rows.
  - Vortex 0.71/0.72 opportunity mapping:
    - `register_splits` offset and relative row-range support should inform `SplitManifest`
      row-range evidence and per-split blockers.
    - `VortexReadAt::read_at` validation and async I/O hooks are candidate inputs for local split
      read validation and spill/output lifecycle evidence.
    - These hooks do not imply distributed, object-store, table, or larger-than-memory runtime
      support until scale-grade execution and correctness proof land.
  - Evidence required: scale profile/status, data volume, split/file/partition counts,
    memory/spill/shuffle fields, retry/idempotency, output commit status, correctness digest.
  - Acceptance: larger-than-memory and split-parallel claims require real bytes and correctness
    proof; synthetic metadata cannot become runtime scale claim.
  - Verification: split manifest tests, local stress smoke, spill/backpressure tests, shuffle
    correctness tests, retry/idempotency tests, scale benchmark contract tests.
  - Non-goals: no literal any-volume, Spark replacement, distributed runtime, or object-store scale
    claim without separate proof.
  - Claim boundary: declared local resource envelope only.
  - Fallback boundary: external engines are baselines/oracles only.
  - Dependencies/blockers: SourceState split metadata, operator coverage, spill storage policy,
    shuffle correctness fixtures, and output commit proof.
  - Ledger rule: ledger entry must include resource envelope, data volume, and claim status.

- [ ] GAR-RUNTIME-IMPL-4Q live, hybrid, loopback control-plane, and distributed blockers
  - Source: RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - Runtime enablement: engine-mode admission and loopback-only runtime controls, plus fail-closed
    distributed blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane proof only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, and distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-4R adapters, databases, UDFs, extensions, and effectful operations
  - Source: RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, UDFs, plugins, LLM/API/embedding/vector
    effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - Runtime enablement: scoped local adapter/UDF execution or inspection with effectful/networked
    paths blocked by runtime policy.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, and dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-4S clean install production usability and release rehearsal gate
  - Source: public preview readiness, package-channel matrix, website
    readiness, Use Case Atlas.
  - Current state: runtime slices are being promoted incrementally; production usability still
    requires complete runtime coverage, clean install proof, docs/website parity, examples, current
    benchmark evidence, and claim gates. A preview posture is not the target state.
  - Next slice outcome: run a no-publication production-readiness rehearsal from clean checkout or
    local package artifact through CLI/Python workflows, unsupported diagnostics, benchmarks,
    website/status, security/legal, and release metadata.
  - Runtime enablement: end-to-end usability validator proving admitted runtime paths from clean
    install through evidence inspection.
  - User-visible surface: README, docs/getting-started, website, package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow
    matrix, benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload or release tag without explicit human approval; no
    production/platform/performance/Spark-replacement claim until all matching runtime and evidence
    gates pass; no hidden fast mode.
  - Claim boundary: production readiness requires complete runtime coverage and workload-scoped
    evidence. Do not substitute a technical-preview target for the production engine goal.
  - Fallback boundary: release gates must fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, clean install script, docs/website
    parity, benchmark artifact policy, and security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, release-gate evidence, and
    remaining unsupported paths.

#### GAR-RUNTIME-IMPL-5 - Runtime Coverage Assurance Implementation Slices

This final queue exists to make the "fully functional / usable compute engine" goal explicit at the
end of Planned. These are coverage-assurance backstops, not a second parallel runtime queue. Work a
5-series item only after the matching 4-series runtime item has landed or when the 4-series item
explicitly splits residual runtime gaps into this queue. Each 5-series item must either prove the
surface is broadly usable through real runtime evidence or split the remaining runtime gaps into
smaller implementation slices. Completing a 5-series item requires evidence, validators,
docs/website parity, and a completed-ledger entry.

- [ ] GAR-RUNTIME-IMPL-5A generated-source end-user runtime builders
  - Source: `GAR-RUNTIME-IMPL-4E`, `GAR-GEN-1`, `GAR-COMPAT-1B`, Use Case Atlas generated-source
    rows.
  - Current state: no-dataset smoke remains separate. Scoped local generated-output runtime now
    covers `ctx.from_rows`, generated-row `.select(...)`/literal `.with_column(...)`,
    `ctx.literal_table`, `ctx.calendar`, `ctx.range`, `ctx.sequence`, SQL `VALUES`, SQL literal
    `SELECT`, scoped SQL `SELECT *`, value-column/int64 arithmetic and CASE projections from
    `generate_series/range(...)`, and optional source-free SQL range `WHERE`/`LIMIT`. Python also
    exposes a fluent `ctx.range(...)` filter/with-column/limit/write path that lowers to the same
    generated-source SQL runtime. JSONL/CSV are the default local sinks; flat scalar
    Parquet/Arrow IPC/Avro/ORC and Vortex sinks are feature-gated local smokes with deterministic
    blockers in default builds. Broad DataFrame expression-backed source-free output, arbitrary
    source-free projection, object-store sinks, Foundry generated-output runtime, and claim-grade
    structured-format fidelity remain incomplete or blocked.
  - Next slice outcome: promote one coherent local generated-source workflow set across CLI,
    Python, and SQL/DataFrame admission, writing local output with generated-source evidence.
  - Runtime enablement: ordinary end-user generated-source workflows that execute locally and write
    evidence-backed outputs.
  - User-visible surface: `ctx.range(...)`, `ctx.sequence(...)`, `ctx.from_rows(...)`,
    `ctx.literal_table(...)`, `ctx.calendar(...)`, SQL `VALUES`/literal `SELECT`/
    `generate_series`/`range`, CLI generated-source command, recipes, website status.
  - Implementation scope: generated-source plan nodes, schema inference, deterministic seed/row
    accounting, local output writer integration, typed Python report fields, unsupported
    diagnostics.
  - Evidence required: `input_dataset_count=0`, `source_io_performed=false`,
    `generated_source_created=true`, generated-source kind/schema/row/plan digests, deterministic
    seed, output certificate, fallback/no-external-engine fields, claim gate.
  - Acceptance: no-input smoke stays separate from generated-output runtime; admitted generated
    workflows write local output and evidence; unsupported generators/sinks block deterministically.
  - Verification: generated-source CLI tests, Python builder tests, SQL literal/VALUES tests,
    output replay smoke, use-case coverage, website readiness.
  - Non-goals: no S3/object-store sink, Foundry production path, public package publication, or
    broad SQL/DataFrame claim.
  - Claim boundary: local deterministic generated-output runtime only.
  - Fallback boundary: generated rows and expressions must be produced by ShardLoom-native code.
  - Dependencies/blockers: generated-source certificate schema, local output writers, expression
    semantics, and Python/CLI envelope parity.
  - Ledger rule: ledger entry must list each admitted builder, output format, evidence refs, and
    blocked generator/sink shapes.

- [ ] GAR-RUNTIME-IMPL-5B SQL frontend runtime ladder
  - Source: `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4C`, `GAR-RUNTIME-IMPL-4D`, RFC 0032.
  - Current state: scoped local CSV/flat JSONL SQL smoke paths exist for
    projection/optional-filter/limit, preview/select-star, scalar and grouped aggregates with
    optional filters and output aliases, multi-key scalar top-N over projection rows, aggregate
    output aliases, and group keys, explicit single- or multi-key inner equi-join,
    left/right/full outer equi-join, left semi/anti equi-join, cross join, scoped
    column-comparison and generic numeric-expression ON joins, scoped computed join projections,
    multi-key scalar joined top-N, and scalar/grouped join-aggregate ordering by aggregate output
    aliases or group keys. Scalar/grouped aggregate and join-aggregate rows also admit scoped
    post-aggregate `HAVING` predicates bound to emitted aggregate output aliases or selected group
    keys; richer expressions, casts, dates, strings, windows, subqueries,
    catalogs, arbitrary join predicates, null/collation ordering, and broad planner behavior remain
    incomplete or blocked.
  - Next slice outcome: implement a staged SQL ladder that admits only supported syntax families
    and emits stable blockers for unsupported syntax.
  - Runtime enablement: ShardLoom-native SQL execution for admitted syntax families plus stable
    runtime blockers for unsupported SQL.
  - User-visible surface: CLI SQL command, SQL explain/capability output, docs/use-cases, website
    status.
  - Implementation scope: parser/binder/planner admission, local logical plan lowering, expression
    type/null policy, join/order/aggregate blockers, explain snapshots, tests.
  - Evidence required: parser/binder/planner flags, admitted syntax family, before/after plan
    digests, source/output refs, correctness digest, unsupported diagnostic code, no-fallback
    fields, claim gate.
  - Acceptance: each admitted SQL shape executes through ShardLoom-native code only; every
    unsupported SQL construct fails closed with actionable diagnostics.
  - Verification: SQL parser/binder unit tests, CLI smoke per admitted family, unsupported
    diagnostic snapshots, release readiness metadata, benchmark harness where applicable.
  - Non-goals: no ANSI SQL parity, catalog runtime, production SQL claim, or external SQL engine.
  - Claim boundary: syntax-family scoped local SQL runtime only.
  - Fallback boundary: DataFusion, DuckDB, Spark, SQLite, Polars, pandas, and Vortex query-engine
    integrations are prohibited as execution backends.
  - Dependencies/blockers: operator semantics, local adapter registry, output writers, execution
    envelope validators.
  - Ledger rule: ledger entry must enumerate admitted SQL grammar families and blocked families.

- [ ] GAR-RUNTIME-IMPL-5C Python DataFrame and query-builder workflow parity
  - Source: `GAR-RUNTIME-IMPL-4A`, `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4E`, Python README,
    Use Case Atlas.
  - Current state: Python wrapper and selected query-builder methods exist. The local CSV/flat
    JSONL query builder now covers projection/filter/limit, preview, scalar aggregate, multi-key
    group-by, multi-key scalar top-N, aggregate-output top-N, scoped local-source equi/cross and
    expression-condition joins, computed projections and multi-key scalar top-N over joined rows, scalar/grouped join
    aggregate, post-aggregate `having(...)` / post-`agg(...)` `filter(...)`, explicit-projection
    literal `with_column(...)`, and `count()` workflows, but
    complete end-to-end generated/local/Vortex workflows and
    unsupported-method diagnostics are not yet ordinary user-grade coverage.
  - Next slice outcome: make one import path support generated, local file, and prepared/native
    Vortex workflows with select/filter/project/limit/preview/aggregate/group/order/write where
    admitted.
  - Runtime enablement: ordinary Python context/query-builder workflows that invoke ShardLoom
    runtime instead of external Python engines.
  - User-visible surface: `import shardloom`, context/session object, `LazyFrame`, typed reports,
    getting-started docs, recipes, website use cases.
  - Implementation scope: Python builders, method admission matrix, CLI lowering, typed report
    accessors, examples, packaging smoke.
  - Evidence required: method admission, execution mode, engine mode, source/generated/prepared refs,
    output refs, correctness digest, certificate refs, no-fallback fields, claim gate.
  - Acceptance: a non-expert can run documented Python workflows and inspect evidence without
    architecture docs; unsupported methods are explicit and actionable.
  - Verification: Python unit/integration tests, clean-venv smoke, example smoke, compileall,
    use-case coverage, website readiness.
  - Non-goals: no pandas/Polars backend, notebook production claim, broad DataFrame parity claim, or
    public package upload.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: Python orchestrates ShardLoom runtime and must not compute through external
    engines.
  - Dependencies/blockers: CLI runtime coverage, typed execution envelope, local outputs, generated
    source builders, Vortex lifecycle.
  - Ledger rule: ledger entry must include runnable Python snippets, admitted methods, and blocked
    methods.

- [ ] GAR-RUNTIME-IMPL-5D local input adapter runtime parity
  - Source: `GAR-RUNTIME-IMPL-4F`, `GAR-IOREUSE-1A`, universal compatibility scoreboard.
  - Current state: local CSV plus scoped flat JSONL/NDJSON, flat top-level `.json`, and
    feature-gated flat scalar `.parquet`/Arrow IPC/Avro/ORC local SQL
    smokes exist, the Python query-builder now bridges local CSV, flat JSON/JSONL/NDJSON, and
    feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC
    projection/optional-filter/limit,
    preview/select-star, scalar-aggregate/optional-filter/limit with aliases, multi-key group-by
    aggregate/optional-filter/limit, multi-key scalar top-N workflows, aggregate-output top-N
    workflows, post-aggregate HAVING over aggregate output rows, joined computed projection/top-N
    workflows, scalar/grouped join aggregates with optional HAVING, and local-source
    evidence labels are source-format-aware for CSV versus JSON versus JSONL/NDJSON versus admitted
    Parquet/Arrow IPC/Avro/ORC rows. Nested JSON/JSONPath, broader
    Parquet/Arrow IPC/Avro/ORC type/nesting coverage,
    Excel, database files, and unsupported formats are
    not uniformly represented by runtime SourceState adapters.
  - Next slice outcome: promote one local input format at a time into a SourceState adapter registry
    with deterministic blockers for unsupported formats.
  - Runtime enablement: local SourceState adapter runtime for admitted file formats and explicit
    blockers for unsupported formats.
  - User-visible surface: CLI/Python read APIs, capability/status views, benchmark rows, use cases.
  - Implementation scope: adapter registry, format detection, schema/dtype inference, fingerprints,
    row-count posture, parse/decode planning, diagnostics.
  - Evidence required: source format/location/fingerprint, SourceState id/digest, schema digest,
    row-count posture, parse/decode/materialization fields, Native I/O certificate posture,
    no-fallback fields.
  - Acceptance: each admitted local format can run at least one certified local workload or explicit
    smoke; unsupported formats produce blockers instead of silent fallback.
  - Verification: adapter snapshot tests, CLI/Python smoke per admitted format, unsupported format
    snapshots, benchmark row contract tests.
  - Non-goals: no object-store, database server, table/lakehouse, or universal adapter claim.
  - Claim boundary: local file adapter support per admitted format only.
  - Fallback boundary: adapters cannot use external engines to parse, plan, or execute user work.
  - Dependencies/blockers: dependency/license review, fixture data, SourceState schema, output
    correctness checks.
  - Ledger rule: ledger entry must list admitted formats, evidence refs, and unsupported formats.

- [ ] GAR-RUNTIME-IMPL-5E local output writers, replay proof, and fanout runtime
  - Source: `GAR-RUNTIME-IMPL-4G`, `GAR-IOREUSE-1C`, `GAR-IOREUSE-1D`, result-sink proof docs.
  - Current state: result-sink evidence exists, but local cross-format output and fanout are not
    complete as ordinary user workflows.
  - Next slice outcome: implement local OutputPlan-backed writes for admitted formats and a
    cross-format fanout smoke with replay/correctness proof.
  - Runtime enablement: local OutputPlan writer and fanout runtime with replay evidence.
  - User-visible surface: CLI/Python `write(...)`, recipes, benchmark fanout rows, website status.
  - Implementation scope: OutputPlan registry, writer adapters, schema compatibility, replay proof,
    output digest, fanout benchmark harness.
  - Evidence required: output plan id/digest, format/location/schema, write mode, output bytes/rows,
    replay status, output Native I/O certificate, no-fallback fields, claim gate.
  - Acceptance: one prepared/generated/local source can write multiple admitted local outputs;
    unsupported sinks block; replay proof is explicit where claimable.
  - Verification: writer smoke per format, replay tests, fanout benchmark smoke, output schema
    snapshots.
  - Non-goals: no object-store write, table commit, production sink claim, or performance claim.
  - Claim boundary: local output writer and fanout support per admitted format only.
  - Fallback boundary: output writers translate ShardLoom results and cannot invoke external compute.
  - Dependencies/blockers: OutputPlan schema, local adapter data, result replay harness, generated
    source/local/Vortex source evidence.
  - Ledger rule: ledger entry must list output formats, replay status, and blocked sinks.

- [ ] GAR-RUNTIME-IMPL-5F prepared/native Vortex runtime lifecycle
  - Source: `GAR-RUNTIME-IMPL-4H`, `GAR-RUNTIME-IMPL-4I`, Vortex provider docs, compute-flow
    reference.
  - Current state: prepared/native batch paths and scoped source-backed scan evidence exist; the
    complete prepare/read/write/reopen/scan/pushdown lifecycle is still not broad runtime support.
  - Next slice outcome: make local Vortex artifacts a first-class runtime path with explicit
    preparation, write/reopen, scan, pushdown, materialization/decode, and output evidence.
  - Runtime enablement: first-class local Vortex artifact runtime lifecycle from preparation through
    scan and output evidence.
  - User-visible surface: CLI/Python Vortex workflows, benchmark rows, compute-flow docs, status
    matrix.
  - Implementation scope: Vortex preparation manager, artifact registry, scan request builder,
    source-backed scan adapter, pushdown admission, local output integration.
  - Evidence required: Vortex artifact ref/digest, preparation timing, write/reopen/scan timing,
    pushed-down filters/projections/limits, encoded predicate fields, materialization/decode fields,
    no-fallback fields.
  - Acceptance: prepared/native rows are clearly separated from compatibility import rows; supported
    pushdown avoids unnecessary output-column reads; unsupported pushdown blocks deterministically.
  - Verification: prepared/native batch smoke, source-backed scan tests, filter/projection/limit
    smoke, benchmark harness contract tests.
  - Non-goals: no object-store Vortex artifact, blanket encoded-native claim, or performance claim.
  - Claim boundary: declared local Vortex artifact workflows only.
  - Fallback boundary: Vortex array/scan/source/sink APIs may be native providers; Vortex
    query-engine integrations may not execute unsupported work.
  - Dependencies/blockers: Vortex dependency/version gate, provider boundary, SourceState/
    PreparedState ids, output evidence.
  - Ledger rule: ledger entry must include artifact lifecycle evidence and blocked Vortex paths.

- [ ] GAR-RUNTIME-IMPL-5G physical operator, function, and encoded-kernel coverage
  - Source: `GAR-RUNTIME-IMPL-4D`, `GAR-RUNTIME-IMPL-4J`, RFC 0015, RFC 0016, RFC 0021.
  - Current state: selected residual-native operators exist; broad type/null/string/date/decimal,
    join/window/top-k, fused, and encoded-kernel coverage remains incomplete. Scoped
    `COUNT(DISTINCT column)` is runtime-admitted for local scalar and grouped aggregate rows with
    `distinct_aggregate_*` evidence, SQL `NULL`-ignoring distinct-count semantics, Python
    `sl.count_distinct(...)` aggregate lowering, deterministic blockers for unsupported
    `DISTINCT` aggregate shapes such as `SUM(DISTINCT ...)` or `COUNT(DISTINCT *)`, and no external
    fallback.
  - Next slice outcome: promote operator families one at a time with decoded-reference correctness,
    unsupported diagnostics, and encoded-kernel admission where available.
  - Runtime enablement: ShardLoom-native operator/function execution coverage with deterministic
    blockers for unsupported families.
  - User-visible surface: CLI/Python/SQL/DataFrame workflows, benchmark rows, capability matrix.
  - Implementation scope: expression IR, scalar/aggregate operators, join/window/top-k operators,
    type coercion, null/string/date policy, encoded kernel registry, blockers.
  - Evidence required: operator/function family, input/output schema, type/null policy, encoding id,
    decoded/materialized flags, correctness digest, encoded-native claim flag, no-fallback fields.
  - Acceptance: each supported operator family has success tests, edge-case tests, unsupported
    diagnostics, and correctness evidence; unsupported encodings block deterministically.
  - Verification: unit/property/correctness tests, fixture manifest checks, encoded-kernel tests,
    benchmark smoke per family.
  - Non-goals: no arbitrary UDFs, ANSI parity, blanket encoded-native claim, or performance claim.
  - Claim boundary: operator/function/encoding-pair support only.
  - Fallback boundary: external engines may be test oracles only, never runtime evaluators.
  - Dependencies/blockers: semantic fixture corpus, expression registry, benchmark row schema,
    decoded-reference harness.
  - Ledger rule: ledger entry must list promoted families, type/null behavior, and blockers.

- [ ] GAR-RUNTIME-IMPL-5H evidence envelope, evidence levels, and claim validators
  - Source: `GAR-RUNTIME-IMPL-4K`, `GAR-PERF-2A`, release readiness metadata, benchmark publishing
    policy.
  - Current state: reports expose many useful fields, but CLI, Python, benchmark, website, docs, and
    release gates can still diverge as runtime surfaces expand or when mirrored website artifacts
    restate support/claim language independently.
  - Next slice outcome: add a versioned execution-envelope schema, evidence levels, and validators
    that every runtime path must satisfy.
  - Runtime enablement: shared runtime evidence validator that blocks unsupported/report-only rows
    from being treated as supported runtime.
  - User-visible surface: CLI JSON, Python typed reports, benchmark artifacts, website evidence,
    release readiness.
  - Implementation scope: shared schema, report adapters, typed aliases/migrations, readiness
    checks, source-of-truth mirror checks, website renderer, benchmark completeness gate.
  - Evidence required: execution/engine/evidence mode, source/generated/output refs, certificate
    refs, materialization/decode refs, no-fallback fields, claim gate, evidence level.
  - Acceptance: missing fallback/certificate/claim fields fail validation; `minimal_runtime` cannot
    become claim-grade by accident; report-only rows cannot masquerade as runtime support; public
    docs/website surfaces render canonical support/evidence data instead of maintaining duplicated
    claim-boundary prose or stale benchmark status copies.
  - Verification: schema contract tests, release readiness metadata, benchmark completeness,
    website readiness and mirror-drift checks, Python typed-report tests.
  - Non-goals: no runtime capability upgrade from schema work alone.
  - Claim boundary: evidence standardization and claim gating only.
  - Fallback boundary: every envelope exposes `fallback_attempted=false` and
    `external_engine_invoked=false` or fails.
  - Dependencies/blockers: stable field names, compatibility aliases, Python report migration,
    benchmark/website validators.
  - Ledger rule: ledger entry must record schema version, migrated surfaces, source-of-truth mirrors,
    and validation failures now blocked.

- [ ] GAR-RUNTIME-IMPL-5I optimizer, session runtime, reuse, and buffer-pool promotion
  - Source: `GAR-RUNTIME-IMPL-4L`, `GAR-PERF-2B`, `GAR-PERF-2F`, `GAR-PERF-2G`,
    `GAR-IOREUSE-1`.
  - Current state: optimizer traces, source-state reuse, and batch/session evidence exist in scoped
    forms. Prepared/native benchmark batches emit evidence-only SourceState digests and per-family
    reuse digests, while Python local sessions expose SourceState identity/read-plan evidence for
    admitted query/output reuse. Ordinary workflows do not yet have a reusable session/cache
    lifecycle.
  - Next slice outcome: implement a scoped in-process session with optimizer trace, SourceState/
    VortexPreparedState/OutputPlan reuse, invalidation, and buffer reuse evidence.
  - Runtime enablement: scoped optimizer/session/cache runtime that safely reuses work across
    admitted local workflows.
  - User-visible surface: CLI batch/session command, Python context/session, explain output,
    benchmark timing rows.
  - Implementation scope: session lifecycle, optimizer rule registry, cache keys/fingerprints,
    invalidation policy, buffer-pool hooks, explicit close/cleanup.
  - Evidence required: session id, optimizer rules admitted/applied/blocked, before/after plan
    digests, cache hit/miss, reuse digest/reason, invalidation reason, buffer reuse count,
    no-fallback fields.
  - Acceptance: repeated admitted workflows reuse state safely; stale source/schema/plan changes
    invalidate cache; optimizer decisions are explainable and semantics-preserving.
  - Verification: optimizer snapshot tests, session smoke, invalidation tests, source/prepared/output
    reuse tests, benchmark contract tests.
  - Non-goals: no daemon/service, distributed cache, hidden fast mode, or performance claim.
  - Claim boundary: scoped in-process reuse and explainable optimization only.
  - Fallback boundary: optimizer/session/cache cannot change provider to an external engine.
  - Dependencies/blockers: fingerprint contract, plan digest stability, cache cleanup policy,
    envelope validators.
  - Ledger rule: ledger entry must list admitted optimizer rules, reuse artifacts, and invalidation
    rules.

- [ ] GAR-RUNTIME-IMPL-5J benchmark publishing, profile, and claim-grade refresh gate
  - Source: `GAR-RUNTIME-IMPL-4M`, `GAR-BENCH-PUB-1`, benchmark publishing runbook.
  - Current state: benchmark publishing has a structured artifact model, but every runtime
    promotion still needs fresh, profile-scoped evidence and public website/docs rendering. The
    current public benchmark artifact is `full_local` and therefore shows CSV/Parquet comparative
    rows without Spark profile rows; the website must keep `spark-default` and
    `spark-local-tuned` visible as `full_local_plus_spark` lanes even when the current artifact did
    not request them. The benchmark registry and release gate now require `shardloom-prepare-batch`
    for full local published profiles, and the current promoted `full_local` artifact includes the
    ShardLoom cold route, prepared route, single-process prepare/batch route, native Vortex route,
    and local comparison baselines across CSV/Parquet required scenarios. Current promoted rows
    still include ShardLoom `blocked`, `fixture_smoke_only`, and external `external_baseline_only`
    rows, and the main artifact lacks broad-format JSONL/Arrow IPC/Avro/ORC comparative coverage.
    Benchmark pages must also pull current support and claim-boundary context from generated
    status/evidence data instead of carrying their own explanatory copy.
  - Next slice outcome: require a current benchmark/correctness/evidence artifact for every
    promoted runtime path and block stale or incomplete public claims. The next public comparative
    refresh should run or explicitly gate `full_local_plus_spark`, include Spark lane availability,
    publish broad-format coverage for CSV, Parquet, JSONL, Arrow IPC, Avro, and ORC, and move the
    main ShardLoom comparative roster toward `claim_grade` rows only for admitted runtime paths.
  - Runtime enablement: runtime-claim publishing validator that keeps public support status tied to
    fresh evidence.
  - User-visible surface: website benchmarks, docs/benchmarks, status page, release readiness.
  - Implementation scope: artifact freshness checker, profile matrix, runtime claim matrix,
    benchmark page ingestion from canonical generated artifacts, release validators, Spark/JVM
    profile publishing checks, format coverage checks, and claim-gate closeout diagnostics.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, correctness
    refs, certificate refs, no-fallback fields, claim gate, Spark lane availability, format
    coverage, and source-state/prepared-state coverage.
  - Acceptance: promoted paths are not presented publicly without current evidence; missing
    required lanes/scenarios/formats are visible and block claim-grade status; Spark lanes are visible in
    artifact lane availability; broad formats are visible as available or missing; prepared/native
    source-state coverage is rendered from batch evidence instead of a misleading scalar count; the
    raw comparative roster renders all promoted rows, not a sample; the main ShardLoom comparative
    roster has no `blocked`, `unsupported`, `not_claim_grade`, or `fixture_smoke_only` rows before
    any broad claim-grade benchmark publication, while external lanes remain `external_baseline_only`
    and never satisfy ShardLoom evidence; the benchmark page reuses the runs-today support matrix
    for support posture and the promoted benchmark bundle for timing/coverage context.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests, `full_local_plus_spark` preflight/runbook evidence.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: workload-scoped local benchmark evidence only.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: benchmark manifest schema, runtime envelope validators, scenario
    fixtures, website renderer support.
  - Ledger rule: ledger entry must include artifact refs, profile, freshness, and public claim
    status.

- [ ] GAR-RUNTIME-IMPL-5K object-store read runtime admission
  - Source: `GAR-RUNTIME-IMPL-4N`, `GAR-COMPAT-1C`, `GAR-SCALE-1E`,
    `docs/architecture/object-store-request-planner.md`.
  - Current state: `GAR-RUNTIME-IMPL-4N` admits `object-store-read-smoke` for an explicit
    `local-emulator` local fixture profile with SourceState and Native I/O evidence. Public
    no-credential object-store reads, authenticated reads, credential policy, network policy,
    listing, local cache, and real provider proofs remain blocked.
  - Next slice outcome: extend beyond the local-emulator fixture into provider URI parse,
    effect/credential policy, optional listing, byte-range/full-file read, local cache boundary,
    and SourceState evidence for an approved public no-credential fixture profile.
  - Runtime enablement: provider/profile-scoped object-store read runtime with credential/network
    admission and no-default-effect policy.
  - User-visible surface: CLI/Python object-store diagnostics, capability/status pages, use cases.
  - Implementation scope: provider abstraction, policy gate, credential redaction, request planner,
    byte-range adapter, cache boundary, emulator/public-fixture tests.
  - Evidence required: provider/profile, credential/network status, object version/ETag, byte
    ranges, SourceState id, Native I/O certificate, no-fallback fields.
  - Acceptance: public and authenticated read gates are separate; no network probe or credential
    resolution runs by default; unsupported providers fail closed.
  - Verification: policy tests, mocked/emulator read smoke, SourceState snapshot tests, release
    readiness, website status checks.
  - Non-goals: no object-store write, table commit, production object-store claim, or managed
    platform claim.
  - Claim boundary: provider/profile-specific read proof only.
  - Fallback boundary: storage provider access does not authorize external query execution.
  - Dependencies/blockers: security/effect policy, provider test harness, dependency/license review,
    emulator or public no-credential fixture.
  - Ledger rule: ledger entry must record provider, credential posture, proof refs, and blocked
    providers.

- [ ] GAR-RUNTIME-IMPL-5L object-store write and table/lakehouse operation ladder
  - Source: `GAR-RUNTIME-IMPL-4O`, `GAR-COMPAT-1D`, `GAR-SCALE-1E`.
  - Current state: object-store writes, table metadata/snapshot scans, append, merge/delete, commit,
    rollback, and catalog integration are blocked or report-only.
  - Next slice outcome: after read proof, implement staged write/commit/recovery in an approved
    profile, then one fixture-backed table metadata/snapshot operation and one append or commit
    rehearsal where admitted.
  - Runtime enablement: staged object-store write and table/lakehouse operation runtime for declared
    fixture profiles only.
  - User-visible surface: table/object-store capability views, CLI/Python diagnostics, status/use
    cases, scale benchmark rows.
  - Implementation scope: write staging, commit protocol, idempotency, cleanup/retry, table metadata
    adapter, snapshot reader, manifest writer or commit rehearsal.
  - Evidence required: provider/profile, table format, snapshot id, manifest/data-file counts,
    commit protocol/status, rollback/cleanup status, idempotency key, no-fallback fields.
  - Acceptance: object-store read/write/commit and table metadata/read/append/commit are separate
    gates; fixture proof does not imply production lakehouse support.
  - Verification: policy tests, emulator write smoke, table fixture tests, commit rehearsal smoke,
    unsupported diagnostics, release readiness.
  - Non-goals: no blanket S3/GCS/ADLS support, production Iceberg/Delta/Hudi claim, catalog service,
    or production table claim.
  - Claim boundary: provider/table-format operation in declared fixture/profile only.
  - Fallback boundary: no external catalog, lakehouse engine, or query engine executes work.
  - Dependencies/blockers: object-store read proof, commit/recovery policy, table fixtures,
    dependency/license review, idempotency evidence.
  - Ledger rule: ledger entry must list provider, table format, operation, and blocked behaviors.

- [ ] GAR-RUNTIME-IMPL-5M scale-grade local execution runtime
  - Source: `GAR-RUNTIME-IMPL-4P`, `GAR-SCALE-1`, RFC 0014, RFC 0016, RFC 0017.
  - Current state: scale contracts and evidence fields exist, but larger-than-memory,
    split-parallel, spill, shuffle, retry, and idempotent output commit runtime are not claimable.
  - Next slice outcome: implement a declared-resource local scale profile with SplitManifest,
    bounded memory checks, per-split execution, spill/backpressure where admitted, one shuffle
    family, retry/idempotency, and output commit evidence.
  - Runtime enablement: local scale-grade runtime under declared resource envelopes with real-byte
    correctness proof.
  - User-visible surface: scale benchmark profiles, CLI/Python execution envelopes, status page.
  - Implementation scope: split scheduler, memory budget, spill manager, shuffle plan, retry/
    cancellation/recovery, output commit status, scale benchmark rows.
  - Evidence required: scale profile/status, real data volume, split/file/partition counts,
    memory/spill/shuffle fields, retry/idempotency, output commit status, correctness digest,
    no-fallback fields.
  - Acceptance: larger-than-memory and split-parallel claims require real bytes and correctness
    proof; synthetic metadata cannot become runtime scale claim.
  - Verification: split manifest tests, local stress smoke, spill/backpressure tests, shuffle
    correctness tests, retry/idempotency tests, scale benchmark contract tests.
  - Non-goals: no literal any-volume, Spark replacement, distributed runtime, or object-store scale
    claim without separate proof.
  - Claim boundary: declared local resource envelope only.
  - Fallback boundary: external engines are baselines/oracles only.
  - Dependencies/blockers: SourceState split metadata, operator coverage, spill storage policy,
    shuffle correctness fixtures, output commit proof.
  - Ledger rule: ledger entry must include resource envelope, data volume, and scale claim status.

- [ ] GAR-RUNTIME-IMPL-5N live, hybrid, control-plane, and distributed-runtime promotion
  - Source: `GAR-RUNTIME-IMPL-4Q`, RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - Runtime enablement: admitted local live/hybrid/control-plane runtime plus distributed execution
    blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote-worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane proof only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-5O adapters, databases, UDFs, extensions, and effectful operations
  - Source: `GAR-RUNTIME-IMPL-4R`, RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, wrappers/connectors, UDFs, plugins,
    LLM/API/embedding/vector effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - Runtime enablement: scoped adapter/UDF runtime or safe inspection, with all effectful external
    paths denied by default.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-5P Foundry dev-stack generated-output and transform proof
  - Source: `GAR-COMMERCIAL-1E`, `GAR-IOREUSE-1G`, Foundry proof docs.
  - Current state: Foundry proof remains local/style-only or report-only; no production Foundry
    runtime/package/certified claim exists.
  - Next slice outcome: implement a personal dev-stack proof that imports the local package,
    resolves the CLI, runs source-free generated output and one staged-input transform, writes a
    result dataset and evidence dataset through Foundry-style output APIs, and preserves blocked
    flags.
  - Runtime enablement: local/dev-stack Foundry-style transform proof that runs ShardLoom locally
    and writes evidence datasets without Spark fallback.
  - User-visible surface: Foundry proof docs, examples, capability/status pages, release readiness.
  - Implementation scope: local Foundry-style transform wrapper, generated-source workflow,
    staged-input workflow, evidence dataset writer, runtime flag reporting.
  - Evidence required: input/output dataset counts, generated-source certificate, output Native I/O
    certificate, Foundry runtime/compute/Spark invoked flags, staged bytes, no-fallback fields.
  - Acceptance: Foundry can orchestrate a local proof without Spark fallback; evidence dataset
    output is mandatory; direct S3/object-store writes are not used.
  - Verification: local Foundry-style smoke, proof doc checks, release readiness metadata, website
    status checks.
  - Non-goals: no Foundry production support, package publication, marketplace listing, certified
    Foundry claim, or direct object-store path.
  - Claim boundary: local/dev-stack proof only.
  - Fallback boundary: Foundry/Spark compute cannot be reported as ShardLoom execution.
  - Dependencies/blockers: local package proof, generated-source runtime, output evidence writer.
  - Ledger rule: ledger entry must include proof commands, output/evidence refs, and blocked claims.

- [ ] GAR-RUNTIME-IMPL-5Q final production usability and website learning gate
  - Source: `GAR-RUNTIME-IMPL-4S`, `GAR-DOCS-1`, `GAR-WEB-ATLAS-1`, public-readiness,
    package-channel matrix.
  - Current state: repo, website, and docs are strong, but final usability requires clean install
    proof, examples, website/status parity, benchmark interpretation, security/legal/release checks,
    and a non-expert learning path after runtime slices land.
  - Next slice outcome: run a no-publication production-readiness rehearsal from clean checkout/local
    artifact through CLI/Python workflows, unsupported diagnostics, benchmarks, website/status,
    SECURITY/LICENSE/NOTICE checks, and release metadata.
  - Runtime enablement: final production usability validator across install, examples,
    runtime evidence, unsupported diagnostics, and website learning paths.
  - User-visible surface: README, docs/getting-started, website Field Guide/Use Case Atlas/status,
    package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks, docs link
    validation.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow matrix,
    benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals; website pages explain current runtime
    state without overclaiming.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload or release tag without explicit human approval; no
    production/platform/performance/Spark-replacement claim until all matching runtime and evidence
    gates pass; no hidden fast mode.
  - Claim boundary: production readiness requires complete runtime coverage and workload-scoped
    evidence. Do not substitute a technical-preview target for the production engine goal.
  - Fallback boundary: release gates fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, docs/website parity, benchmark
    artifact policy, security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, website readiness evidence,
    release-gate evidence, and remaining unsupported paths.

#### GAR-USER-SURFACE-1 PySpark-like Python And SQL User Surface Completion Backstop

This bundle is the explicit completion backstop for the desired end-user shape: ShardLoom should be
as simple to enter from Python as PySpark is to Spark, while remaining honest that ShardLoom is not a
Spark API clone, Spark replacement, distributed runtime claim, production SQL/DataFrame claim, or
external-engine fallback. Existing runtime items (`GAR-RUNTIME-IMPL-5B`, `GAR-RUNTIME-IMPL-5C`,
`GAR-RUNTIME-IMPL-5I`, and `GAR-RUNTIME-IMPL-5Q`) own much of the implementation; this section keeps
the user-surface parity target visible until the full import/context/session/SQL/DataFrame path is
runnable, documented, tested, and claim-safe.

- [ ] GAR-USER-SURFACE-1A import, context, and session entrypoint completion
  - Source: PySpark `SparkSession` user model as a usability reference, `python/README.md`,
    `python/src/shardloom/context.py`, `python/src/shardloom/query.py`, `GAR-RUNTIME-IMPL-4L`,
    `GAR-RUNTIME-IMPL-5I`.
  - Current state: users can `import shardloom as sl`, create `ctx = sl.context()`, run smoke/
    capability commands, execute scoped CLI-backed workflows, and create caller-owned
    `ctx.session()` / `sl.session(...)` objects for local `vortex_ingest` prepared-state reuse and
    admitted local query-builder collect/write/fanout reuse. The Python layer is not yet a broad
    long-lived runtime session with reusable SourceState, PreparedState, OutputPlan,
    schema/dictionary, and buffer-pool caches across all workflows.
  - Next slice outcome: implement a user-owned `ShardLoomSession`/context lifecycle that feels as
    simple as `SparkSession.builder...getOrCreate()` without creating a daemon, global hidden cache,
    or remote service.
  - Runtime enablement: explicit local session lifecycle for admitted runtime workflows, including
    session id, close/cleanup, cache hit/miss, invalidation, and no-fallback evidence.
  - User-visible surface: `import shardloom as sl`, `sl.context(...)`, `sl.session(...)`,
    `ctx.session()`, Python README, getting-started docs, use-case pages.
  - Implementation scope: Python context/session classes, Rust/CLI session command or local batch
    surface, session evidence fields, cleanup semantics, examples.
  - Evidence required: `session_id`, `session_state_scope`, `cache_hit`, `cache_miss`,
    `source_state_reuse_count`, `prepared_artifact_reuse_count`, `output_plan_reuse_count`,
    `session_closed`, `fallback_attempted=false`, `external_engine_invoked=false`,
    `claim_gate_status`.
  - Acceptance: a Python user can start a local ShardLoom context/session, run multiple admitted
    workflows, inspect session reuse evidence, and explicitly close the session; missing binaries or
    unsupported runtime features produce deterministic diagnostics.
  - Verification: Python session smoke tests, session invalidation tests, `cargo test -p
    shardloom-contract-tests --test release_readiness_metadata`, Python compileall, README/use-case
    examples.
  - Dependencies/blockers: `GAR-RUNTIME-IMPL-4L`, `GAR-RUNTIME-IMPL-5I`, SourceState/
    VortexPreparedState/OutputPlan reuse, and session cache invalidation evidence.
  - Non-goals: no daemon/service, remote cluster, distributed scheduler, hidden global session,
    Spark-compatible API promise, or performance claim.
  - Claim boundary: PySpark-like simplicity of entry only; not PySpark API parity, Spark
    replacement, or distributed runtime support.
  - Fallback boundary: session reuse must never switch execution to Spark/DataFusion/DuckDB/Polars
    or another engine.
  - Ledger rule: ledger entry must include session API examples, lifecycle evidence, cleanup
    behavior, and unsupported session boundaries.

- [ ] GAR-USER-SURFACE-1C DataFrame/query-builder parity for ordinary local workflows
  - Source: PySpark DataFrame usability reference, `GAR-RUNTIME-IMPL-5C`, Use Case Atlas, Python
    capability matrix, `docs/getting-started/examples.md`.
  - Current state: Python `read_csv(...)`, local flat JSON/JSONL/NDJSON `read_json(...)`, and
    feature-gated local flat scalar `read_parquet(...)` / `read_arrow_ipc(...)` /
    `read_avro(...)` / `read_orc(...)`
    query-builder chains support scoped projection/optional-filter/limit, preview/select-star, explicit-projection
    literal `with_column(...)`, `where(...)`, Python `sl.col(...).between(...)` and
    `sl.col(...).not_in(...)`, `head(...)`/
    `take(...)`, `count()`, scalar aggregate/optional-filter/limit with aliases, multi-key grouped
    aggregate/optional-filter/limit, and multi-key top-N plus aggregate-output top-N collect/write
    workflows. Scoped local-source joins, joined computed projection/multi-key top-N, joined
    aggregate-output top-N, local
    `write_jsonl(...)`/`write_csv(...)` sink aliases, and generated-output
    helpers also exist for scoped local workflows. Engine-native range/sequence generated sources
    now support `limit(...)`, `head(...)`, and `take(...)` bound adjustment before local writes, with
    DataFrame capability rows separating generic `write`, JSONL, and CSV evidence requirements.
    The DataFrame method matrix now marks scoped local-source `with_column(...)`, `.join(...)`,
    `.agg(...)`/`.aggregate(...)`, `.sort(...)`, bounded `.to_python_objects()`, bounded
    `.schema()`/`.describe_schema()`/`.validate_schema(...)`, and bounded
    `.data_quality_summary()`/`.data_quality_check(...)` as fixture-smoke-supported where they
    lower through ShardLoom's shared format-neutral SQL local-source runtime. Generalized joins,
    expression projection beyond admitted scoped families, broader data-quality rules,
    pandas/Arrow/NumPy materialization, richer outputs, and parity-like method coverage remain
    unsupported/report-only.
  - Next slice outcome: promote DataFrame-style methods in user-value order with either runnable
    runtime or deterministic blockers: pandas/Arrow materialization boundaries, broader
    data-quality rules, broader expression projection, richer output writers, and collect/write
    ergonomics.
  - Runtime enablement: familiar DataFrame/query-builder workflows that execute through ShardLoom
    native runtime paths for admitted local inputs and outputs.
  - User-visible surface: `ctx.read_csv`, `ctx.read_json`, `ctx.read_parquet`,
    `ctx.read_arrow_ipc`, `ctx.read_avro`, `ctx.read_orc`, `ctx.read_vortex`,
    `.select`, `.filter`, `.with_column`, `.group_by`, `.agg`, `.join`, `.sort`, `.limit`,
    `.collect`, `.write`, `.explain`, method capability matrix.
  - Implementation scope: Python query builder, SQL/local runtime lowering, expression IR, local
    input adapters, output writers, typed unsupported reports, examples.
  - Evidence required: method family, source format, execution mode, operator family,
    materialization/decode boundary, output evidence, `fallback_attempted=false`,
    `external_engine_invoked=false`, method-level `claim_gate_status`.
  - Acceptance: each public method is either genuinely runnable for a documented subset or returns
    a deterministic unsupported report with blocker id, required evidence, and next action; no
    method silently routes to pandas/Polars/Spark/DataFusion.
  - Verification: Python query-builder tests per method, CLI/runtime smoke tests, capability matrix
    snapshots, use-case coverage, release readiness metadata.
  - Dependencies/blockers: method-level runtime lowerings, expression IR completion, output writer
    support, local join/runtime expansion, and broad SQL/DataFrame claim gates.
  - Non-goals: no pandas/Polars backend, Spark-compatible DataFrame API promise, notebook
    production claim, full SQL optimizer parity, or performance claim.
  - Claim boundary: method-by-method scoped local runtime support only until production evidence is complete.
  - Fallback boundary: DataFrame methods must lower to ShardLoom runtime or deterministic blockers.
  - Ledger rule: ledger entry must include method support table, runnable examples, and blockers.

- [ ] GAR-USER-SURFACE-1D one-command local install, import, and first workflow proof
  - Source: `GAR-COMMERCIAL-1A`, package channel matrix, `README.md`, `docs/getting-started/*`,
    `GAR-RUNTIME-IMPL-5Q`.
  - Current state: local source-tree and editable Python usage are documented, but public package
    publication is not complete and a non-expert install/import/run path still needs final proof.
  - Next slice outcome: provide a clean local path from install to import to first
    SQL/DataFrame/generated-source workflow without reading architecture docs.
  - Runtime enablement: local install/import proof that reaches admitted runtime workflows and
    returns evidence.
  - User-visible surface: README first screen, `docs/getting-started/first-10-minutes.md`,
    Python README, website get-started/status/use-cases.
  - Implementation scope: install script/runbook, editable/local wheel proof, binary resolution,
    quickstart command, example data creation, evidence printout.
  - Evidence required: install command, uninstall/cleanup command, import success, resolved binary,
    smoke workflow output, evidence fields, unsupported-path example, `fallback_attempted=false`,
    `external_engine_invoked=false`.
  - Acceptance: a new user can complete one local runtime workflow and one unsupported diagnostic
    path in under ten minutes with exact commands.
  - Verification: clean venv smoke, Python quickstart test, README command smoke where feasible,
    package metadata checks, website readiness.
  - Dependencies/blockers: local wheel/source checkout proof, binary resolution stability, package
    channel readiness matrix, and release security gates for any public package publication.
  - Non-goals: no PyPI/TestPyPI/conda/Homebrew publication unless release gates separately pass.
  - Claim boundary: local install/import proof only until production evidence is complete.
  - Fallback boundary: install helpers must not install or invoke fallback engines.
  - Ledger rule: ledger entry must include clean-environment commands and outputs.

- [ ] GAR-USER-SURFACE-1E evidence-first result ergonomics for non-expert users
  - Source: ShardLoom evidence envelope, Python typed reports, Use Case Atlas, website Field Guide,
    benchmark claim-boundary docs.
  - Current state: runtime reports expose rich evidence fields, and Python typed reports now expose
    compact `evidence_summary`/`claim_summary` helpers for scoped SQL/generated-source surfaces.
    Scoped SQL local-source reports also expose `result_rows` and `first_result_row` helpers so
    users do not need to parse bounded inline JSONL manually. Remaining result families still need
    the same ergonomic coverage and examples.
  - Next slice outcome: make every Python runtime result expose simple row/output access plus a
    compact evidence summary and stable full evidence object.
  - Runtime enablement: user-facing evidence ergonomics for every admitted runtime workflow.
  - User-visible surface: Python result objects, CLI JSON fields, docs examples, website use-case
    recipes.
  - Implementation scope: typed report helpers, `evidence_summary`/`claim_summary` accessors,
    row/result accessors, docs snippets, use-case output examples.
  - Evidence required: output row count/path, execution mode, engine mode, source/output
    certificates, materialization/decode boundary, no-fallback fields, claim gate, unsupported
    blockers where applicable.
  - Acceptance: users can inspect rows/output and evidence without scraping JSON field maps; every
    example prints at least one result field and one evidence/claim field.
  - Verification: Python typed-report tests, generated docs/use-case checks, website readiness,
    release readiness metadata.
  - Dependencies/blockers: typed report field normalization, compact evidence summary helpers,
    generated docs examples, and stable claim-gate terminology.
  - Non-goals: no claim upgrade, performance dashboard claim, or broad SQL/DataFrame support from
    ergonomic wrappers alone.
  - Claim boundary: clearer evidence presentation only; support status still comes from runtime
    evidence gates.
  - Fallback boundary: evidence summaries must preserve `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: ledger entry must show before/after user examples and evidence accessors.

- [ ] GAR-USER-SURFACE-1F PySpark-like surface completion validator
  - Source: this `GAR-USER-SURFACE-1` bundle, `GAR-RUNTIME-IMPL-5Q`, Use Case Atlas, public
    production-readiness posture, Python capability matrix.
  - Current state: individual runtime slices can land without a single final validator answering
    whether the Python/SQL surface is simple and complete enough for production users.
  - Next slice outcome: add a completion gate that checks the import/context/session/SQL/DataFrame/
    generated-output path against the public usability target.
  - Runtime enablement: release/usability validator that blocks a PySpark-like simplicity claim
    unless every admitted path has runnable proof and every unsupported path has deterministic
    diagnostics.
  - User-visible surface: release readiness report, README/status matrix, website "Can I use this?"
    pages, Python capability matrix.
  - Implementation scope: validation script or contract test, capability/use-case cross-checks,
    example smoke matrix, website/readme claim checks.
  - Evidence required: matrix of Python entrypoints, runnable examples, blocked examples, evidence
    fields per result, claim boundaries, no-fallback/no-external-engine fields.
  - Acceptance: the validator fails if `ctx.sql`, DataFrame/query-builder, generated-output,
    session, install/import, docs, or website surfaces overclaim or lack runnable/blocked proof.
  - Verification: `python scripts/check_use_case_coverage.py`, `python scripts/check_website_readiness.py`,
    Python unit/smoke tests, release readiness metadata, `git diff --check`.
  - Dependencies/blockers: completion of the preceding `GAR-USER-SURFACE-1A` through
    `GAR-USER-SURFACE-1E` slices, use-case coverage, website readiness, and release readiness
    metadata checks.
  - Non-goals: no compatibility with Spark internals, no distributed Spark-scale claim, no package
    publication, no object-store/lakehouse/Foundry production claim.
  - Claim boundary: only after this validator passes may docs say ShardLoom has a PySpark-like
    simple Python front door for its admitted runtime scope.
  - Fallback boundary: any fallback attempt or external-engine invocation fails the completion gate.
  - Ledger rule: ledger entry must include the completion matrix and remaining non-parity gaps.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
