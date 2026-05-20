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
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- Supporting docs must not keep unchecked implementation checklists outside this file and
  `docs/architecture/global-architecture-review.md`. Scope-boundary lists may remain, but real work
  must be carried by a `GAR-*` item below.

Reference index:
- Status source: `README.md`, `docs/architecture/phased-execution-completed-ledger.md`,
  `docs/architecture/rfc-phase-traceability.md`, `docs/architecture/global-architecture-review.md`,
  and `docs/architecture/compute-engine-flow-reference.md`.
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
runtime behavior or support claims. The GAR-P0/P4/P5 groups in this section are the active
non-runtime queue; do not start the runtime implementation queue below until these are closed or
explicitly reprioritized.

Current non-runtime sequence:

1. Non-runtime closeout is complete through GAR-0043-B and GAR-VORTEX-071D.
2. Autonomous work proceeds to the runtime implementation queue below.
3. Continue moving completed runtime sessions to the ledger immediately after PR/session close.

##### Non-Runtime GAR-P5 - Correctness, Benchmarks, Claims, And Release

All currently defined non-runtime GAR-P5 closeout slices are complete. Continue with the runtime
implementation queue below unless a new non-runtime blocker is explicitly added here first.

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

- [ ] GAR-RUNTIME-IMPL-4D expression, cast, null, string, and date runtime families
  - Source: RFC 0021, SQL/Python local runtime smokes, expression/operator semantics,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: expression support exists in scoped smoke paths; scoped UTF-8 prefix, contains,
    and suffix string predicates are runtime-admitted through core expression semantics and local
    SQL `LIKE` smoke paths; scoped ISO Date32 parsing/formatting, extraction helpers,
    UTF-8/Date32 casts, and
    local SQL `DATE 'YYYY-MM-DD'` predicates are runtime-admitted for the local CSV smoke path,
    scoped local SQL `CAST(column AS dtype)` predicates for `int64`, `float64`, `utf8`, `boolean`,
    and `date32` are runtime-admitted for local row smoke paths, scoped bounded
    `column IN (<literal>,...)` predicates are runtime-admitted with `in_predicate_*` evidence and
    blockers for empty, NULL, mixed DATE/non-DATE, oversized, and subquery-backed lists, and scoped
    local SQL logical `AND`/`OR`/`NOT` predicates plus balanced grouping parentheses are
    runtime-admitted over already admitted leaf predicates. Scoped Date32 extracts through
    `DATE_YEAR(column)` / `DATE_MONTH(column)` / `DATE_DAY(column)` predicates are runtime-admitted
    with `date_extract_*` evidence and deterministic blockers for unsupported non-Date32 or
    non-integer comparison shapes. Scoped Date32 day arithmetic through `DATE_ADD_DAYS(column, days)`
    / `DATE_SUB_DAYS(column, days)` predicates is runtime-admitted with `date_arithmetic_*`
    evidence and deterministic blockers for invalid day counts or unsupported non-Date32 shapes.
    Python now exposes `sl.col(...)`
    predicate helpers that lower admitted comparison, inclusive `between(...)`, null, string `LIKE`,
    bounded `IN`, cast/date, Date32 extracts, Date32 day arithmetic, and logical predicates into the
    same local SQL smoke path, plus `where(...)` as a familiar filter alias. User workflows still lack broad typed
    coercions, timestamp/timezone helpers, interval/date-time completeness, NULL/subquery-backed IN semantics,
    arbitrary predicate-tree completeness beyond the scoped admitted leaves, and broader expression
    family coverage.
  - Next slice outcome: add one implementation PR per expression family: null/is-not-null hardening
    where gaps remain, remaining admitted string predicates, richer IN semantics only where
    evidence-backed, timestamp/timezone helpers, interval/date-time completeness where admitted,
    and broader typed coercions/functions.
  - Runtime enablement: executable ShardLoom-native expression families or deterministic runtime
    blockers for unsupported operators.
  - User-visible surface: SQL/Python query builder, explain output, capability matrix, docs.
  - Implementation scope: expression IR, type coercion policy, null semantics, parser lowering,
    native evaluators, diagnostics.
  - Vortex 0.71 opportunity mapping:
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
    SQL `SELECT * FROM generate_series/range(...)`, and Python generated-row projection/literal
    `with_column` before local writes with generated-source/output/no-fallback evidence. Python
    range/sequence builders also support `limit(...)`, `head(...)`, and `take(...)` by adjusting
    generator bounds before invoking the same engine-native range/sequence smoke. Remaining gaps are
    broader SQL source-free projection, arbitrary SQL table functions, broad DataFrame
    expression-backed projection/`with_column`, broader sink formats, object-store/Foundry
    generated-output paths, and claim-grade output coverage.
  - Next slice outcome: implement generated-source builders across CLI/Python/SQL with local
    JSONL/CSV or Vortex output where admitted.
  - Runtime enablement: end-user generated-source execution that writes local output and emits a
    GeneratedSourceCertificate.
  - User-visible surface: Python `ctx.range`, `ctx.from_rows`, `ctx.literal_table`, `ctx.calendar`,
    `ctx.sql(...).write(...)`, SQL `VALUES`, SQL `generate_series`/`range`, generated-output
    recipes.
  - Implementation scope: generator nodes, schema inference, deterministic seed/row-count handling,
    output writer bridge, report/certificate fields.
  - Evidence required: `input_dataset_count=0`, `source_io_performed=false`,
    `generated_source_created=true`, generated source kind/schema/row/plan digest, seed,
    determinism flag, output certificate, no-fallback fields.
  - Acceptance: no-input smoke remains separate; each admitted generator writes local output and
    exposes a GeneratedSourceCertificate.
  - Verification: CLI/Python/SQL generator tests, output smoke, use-case coverage, release
    readiness metadata.
  - Non-goals: no object-store write, Foundry production claim, package publication, or broad
    SQL/DataFrame claim.
  - Claim boundary: local deterministic generated-output runtime only.
  - Fallback boundary: no generated rows or expressions may be produced by an external engine.
  - Dependencies/blockers: generated-source schema contract, local output writer registry,
    expression semantics, and Python/SQL surface admission.
  - Ledger rule: ledger entry must list generator kind, output format, and unsupported generators.

- [ ] GAR-RUNTIME-IMPL-4F UniversalIngress local/non-Vortex adapter runtime coverage by format
  - Source: `GAR-IOREUSE-1A`, universal compatibility scoreboard, local input adapter docs,
    `docs/architecture/vortex-public-api-inventory.md`,
    `docs/architecture/universal-ingress-route-taxonomy.md`.
  - Current state: CSV is the strongest local smoke path; scoped flat JSONL/NDJSON and flat
    top-level `.json` object/array local input are now runtime-admitted through
    `sql-local-source-smoke` with SourceState-style evidence, route fields, content fingerprints,
    schema digests, source-format-aware adapter IDs, and deterministic blockers for nested JSON
    values. Feature-gated local flat scalar `.parquet` input is also runtime-admitted when
    `shardloom-cli` is built with `--features universal-format-io`; default builds return a
    deterministic Parquet adapter blocker. The Python query-builder can lower local flat `.json`,
    `.jsonl`, `.ndjson`, and feature-gated `.parquet`
    projection/optional-filter/limit,
    preview/select-star, scalar aggregate/optional-filter/limit, one-column group-by
    aggregate/optional-filter/limit, and single-key numeric top-N workflows into that runtime path.
    Local-source evidence labels for CSV versus JSON versus JSONL/NDJSON versus admitted Parquet
    source certificate refs, execution certificate refs, materialization boundaries, pushdown status,
    adapter status, route status, and claim reasons are source-format-aware. Nested/general JSON,
    broader Parquet type/nesting coverage, Arrow IPC, Avro, and ORC do not all have ordinary
    user-facing SourceState runtime parity.
  - Next slice outcome: promote one remaining local input format at a time into UniversalIngress/
    InputAdapter registry coverage with SourceState evidence, `vortex_ingest_status`, certified
    route status, and deterministic blockers for unsupported formats/features.
  - Runtime enablement: admitted local input adapters that create reusable SourceState evidence for
    actual user reads and can feed `vortex_ingest` into `VortexPreparedState` when preparation is
    admitted.
  - User-visible surface: CLI/Python read helpers, use cases, capability/status matrix, benchmark
    source-format rows.
  - Implementation scope: format detection, local reader, schema/dtype inference, fingerprinting,
    SourceState digest, decode/materialization evidence.
  - Vortex 0.71 opportunity mapping:
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

- [ ] GAR-RUNTIME-IMPL-4G local output writer registry and fanout promotion
  - Source: OutputPlan, result-sink replay proof, cross-format fanout architecture,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: scoped local SQL/Python output can write local JSONL and CSV sinks with
    format-specific certificate fields, and one feature-gated flat scalar Parquet sink slice is
    admitted through `sql-local-source-smoke` / Python `write_parquet(...)` when
    `shardloom-cli --features universal-format-io` is used. Broader Parquet type/nesting output,
    Arrow IPC, Vortex, replay proof, and multi-output fanout are not ordinary user-facing runtime
    features.
  - Next slice outcome: add the remaining local writer registry and fanout for admitted formats,
    with per-output digest, replay status, and metadata fidelity/loss.
  - Runtime enablement: local output writers and fanout execution with OutputPlan evidence and
    replay proof where admitted.
  - User-visible surface: CLI/Python `.write` and `.fanout`, recipes, benchmark
    `io_reuse_and_fanout`, website status.
  - Implementation scope: OutputPlan builder, writers, schema translation, output digests, replay
    verifier, fanout orchestration.
  - Vortex 0.71 opportunity mapping:
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
  - Dependencies/blockers: local writer dependencies, schema translation, replay verifier,
    generated/local/Vortex source evidence, and fanout benchmark fields.
  - Ledger rule: ledger entry must list format combinations and replay proof refs.

- [ ] GAR-RUNTIME-IMPL-4I Vortex scan pushdown and encoded-predicate runtime completion
  - Source: `GAR-PERF-2C`, Vortex Scan API docs, encoded predicate provider evidence,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: source-backed scan and encoded predicate evidence are scoped; pushdown is not
    complete across admitted prepared/native scenarios.
  - Next slice outcome: lower filter, projection, and limit into Vortex Scan where admitted, and
    emit deterministic blockers when a predicate/projection cannot be pushed down.
  - Runtime enablement: prepared/native Vortex Scan pushdown for admitted filters, projections, and
    limits, with fail-closed blockers for unsupported shapes.
  - User-visible surface: prepared/native benchmark rows, explain output, capability matrix.
  - Implementation scope: scan request builder, filter expression lowering, projection mask, limit/
    slice pushdown, evidence fields.
  - Vortex 0.71 opportunity mapping:
    - Statistic expressions, stats rewrite sessions, `NullCount`, and `UncompressedSize` are
      candidates for metadata-first planning and scan evidence, not standalone runtime claims.
    - `register_splits` offset/relative row-range fixes should feed split-aware scan evidence and
      blockers.
    - `IsSorted` dtype fixes may inform sorted/min-max pruning and top-k blockers before any
      sorted-kernel runtime claim.
  - Evidence required: filter/projection/limit pushdown status, filter/output columns read,
    encoded predicate provider fields, data decoded/materialized, no-fallback fields.
  - Acceptance: supported scenarios avoid reading unused output columns; unsupported pushdown does
    not silently fall back to full materialization.
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
  - Vortex 0.71 opportunity mapping:
    - FastLanes signed bases, SparseArray iterative execution, mask/rank intersection
      improvements, smallvec performance fixes, and TurboQuant are candidate inputs for
      encoding/operator-pair admission.
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
  - Current state: scoped batch/session evidence exists; ordinary user workflows do not yet share a
    reusable session/cache lifecycle.
  - Next slice outcome: implement a scoped in-process `ShardLoomSession` that reuses SourceState,
    VortexPreparedState, schema/dictionary state, and OutputPlan where fingerprints remain valid.
  - Runtime enablement: scoped in-process session runtime with safe source/prepared/output reuse and
    explicit invalidation.
  - User-visible surface: CLI batch/session command, Python context/session, benchmark timing rows.
  - Implementation scope: session lifecycle, cache keys/fingerprints, invalidation policy, cache
    hit/miss evidence, explicit close/cleanup.
  - Evidence required: session id, cache hit/miss, reuse digest/reason, source/prepared/output
    state ids, invalidation reason, no-fallback fields.
  - Acceptance: repeated admitted workflows reuse state safely; stale source/schema/plan changes
    invalidate cache; session state is explicitly scoped and closed.
  - Verification: session smoke, invalidation tests, source/prepared/output reuse tests, benchmark
    harness contract tests.
  - Non-goals: no daemon/service, distributed cache, hidden fast mode, or performance claim.
  - Claim boundary: scoped in-process reuse only.
  - Fallback boundary: cache/session cannot change execution provider to an external engine.
  - Dependencies/blockers: fingerprint/invalidation contract, SourceState/VortexPreparedState/
    OutputPlan ids, explicit session lifecycle, and cache cleanup policy.
  - Ledger rule: ledger entry must list cache artifacts, invalidation rules, and disabled paths.

- [ ] GAR-RUNTIME-IMPL-4M benchmark refresh and runtime claim gate after each promoted workflow
  - Source: `GAR-BENCH-PUB-1`, benchmark publishing runbook, release claim gates.
  - Current state: benchmark publishing is structured, but each newly promoted runtime path needs a
    fresh artifact, scenario coverage, and public claim boundary update.
  - Next slice outcome: require every runtime-promotion PR to update or attach a focused benchmark/
    correctness/evidence artifact and refresh website/docs only when the artifact is claim-safe.
  - Runtime enablement: runtime-promotion validator that blocks stale or missing evidence before a
    path is represented as supported.
  - User-visible surface: website benchmarks, docs/benchmarks, release readiness, status matrix.
  - Implementation scope: artifact freshness checker, runtime claim matrix, benchmark page
    ingestion, release validators, artifact-derived comparative website publishing, and local
    benchmark artifact promotion into committed static website data.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, certificate
    refs, correctness refs, no-fallback fields, claim gate.
  - Acceptance: no promoted path is presented publicly without current evidence; stale or incomplete
    artifacts block claim-grade status; website comparative tables derive from the promoted artifact
    manifest rather than an external dashboard scrape; full-local profiles split `polars-eager` and
    `polars-lazy` and expose missing DuckDB/DataFusion/Dask/Spark lanes with reasons; compatibility
    rows are labeled as certified cold route; prepared rows are labeled as prepared warm route from
    `VortexPreparedState`; prepare-once timing and warm-query timing stay separate.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: evidence gate only; claims remain workload-scoped.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: benchmark manifest schema, runtime envelope validators, scenario
    fixtures, and website renderer support.
  - Ledger rule: ledger entry must include artifact refs and public claim status.

- [ ] GAR-RUNTIME-IMPL-4N object-store read admission with local emulator/public fixture proof
  - Source: `GAR-COMPAT-1C`, `GAR-SCALE-1E`, object-store request planner,
    `docs/architecture/vortex-public-api-inventory.md`.
  - Current state: object-store planning/report-only surfaces exist; runtime reads are blocked.
  - Next slice outcome: implement URI parse, credential/effect policy, optional listing, byte-range
    read, streaming/full-file read, and SourceState evidence in an approved emulator or public
    no-credential fixture profile.
  - Runtime enablement: provider/profile-scoped object-store read admission with policy gates and
    SourceState evidence.
  - User-visible surface: CLI/Python object-store diagnostics, capability/status pages, use cases.
  - Implementation scope: provider abstraction, effect gate, credential policy, request planner,
    byte-range adapter, local cache boundary, tests.
  - Vortex 0.71 opportunity mapping:
    - `VortexReadAt::read_at` validation and async I/O hooks may inform byte-range/object-store
      read evidence after policy admission.
    - 0.71 I/O hooks do not admit object-store runtime by themselves; provider policy, credentials,
      fixtures, and Native I/O certificates remain required.
  - Evidence required: provider/profile, credential/network status, object version/ETag, byte
    ranges, SourceState id, Native I/O certificate, no-fallback fields.
  - Acceptance: public and authenticated read gates are separate; no network probe or credential
    resolution runs by default; unsupported providers fail closed.
  - Verification: policy tests, mocked/emulator read smoke, SourceState snapshot tests, release
    readiness, website status checks.
  - Non-goals: no object-store write, table commit, production object-store claim, or managed
    platform claim.
  - Claim boundary: provider/profile-specific technical-preview read proof only.
  - Fallback boundary: storage provider access does not authorize external query execution.
  - Dependencies/blockers: security/effect policy, provider test harness, dependency/license
    review, and emulator or public no-credential fixture availability.
  - Ledger rule: ledger entry must record provider, credential posture, and proof refs.

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
  - Vortex 0.71 opportunity mapping:
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
  - Claim boundary: fixture/local control-plane technical preview only.
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

- [ ] GAR-RUNTIME-IMPL-4S clean install public technical-preview usability gate
  - Source: public preview readiness, package-channel matrix, website
    readiness, Use Case Atlas.
  - Current state: runtime slices are being promoted incrementally; final public usability still
    requires clean install proof, docs/website parity, examples, benchmark evidence, and claim gates.
  - Next slice outcome: run a no-publication technical-preview rehearsal from clean checkout or
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
  - Non-goals: no public package upload, tag, production/platform/performance claim, Spark
    replacement claim, object-store/lakehouse/Foundry production claim, or hidden fast mode.
  - Claim boundary: public technical preview only with workload-scoped claims.
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
  - Current state: no-dataset smoke remains separate. Scoped local JSONL/CSV generated-output runtime
    now covers `ctx.from_rows`, generated-row `.select(...)`/literal `.with_column(...)`,
    `ctx.literal_table`, `ctx.calendar`, `ctx.range`, `ctx.sequence`, SQL `VALUES`, and SQL literal
    `SELECT`, plus scoped SQL `SELECT * FROM generate_series/range(...)`. Broad DataFrame
    expression-backed source-free output, broader source-free projection, broader output formats
    beyond JSONL/CSV, object-store sinks, and Foundry generated-output runtime remain incomplete or
    blocked.
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
    optional filters, top-N, and one explicit inner equi-join shape; richer expressions, casts,
    dates, strings, windows, subqueries, catalogs, Python/DataFrame joins, multi-key/expression/
    outer/semi/anti/cross joins, and broad planner behavior remain incomplete or blocked.
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
    JSONL query builder now covers projection/filter/limit, preview, scalar aggregate, one-column
    group-by, single-key top-N, scoped local CSV inner equi-join, explicit-projection literal
    `with_column(...)`, and `count()` workflows, but complete end-to-end generated/local/Vortex workflows and
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
    feature-gated flat scalar `.parquet` local SQL
    smokes exist, the Python query-builder now bridges local CSV, flat JSON/JSONL/NDJSON, and
    feature-gated flat scalar Parquet
    projection/optional-filter/limit,
    preview/select-star, scalar-aggregate/optional-filter/limit, one-column group-by
    aggregate/optional-filter/limit, and single-key numeric top-N workflows, and local-source
    evidence labels are source-format-aware for CSV versus JSON versus JSONL/NDJSON versus admitted
    Parquet rows. Nested JSON/JSONPath, broader Parquet type/nesting coverage, Arrow IPC, Avro, ORC,
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
    join/window/top-k, fused, and encoded-kernel coverage remains incomplete.
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
  - Current state: reports expose many useful fields, but CLI, Python, benchmark, website, and
    release gates can still diverge as runtime surfaces expand.
  - Next slice outcome: add a versioned execution-envelope schema, evidence levels, and validators
    that every runtime path must satisfy.
  - Runtime enablement: shared runtime evidence validator that blocks unsupported/report-only rows
    from being treated as supported runtime.
  - User-visible surface: CLI JSON, Python typed reports, benchmark artifacts, website evidence,
    release readiness.
  - Implementation scope: shared schema, report adapters, typed aliases/migrations, readiness
    checks, website renderer, benchmark completeness gate.
  - Evidence required: execution/engine/evidence mode, source/generated/output refs, certificate
    refs, materialization/decode refs, no-fallback fields, claim gate, evidence level.
  - Acceptance: missing fallback/certificate/claim fields fail validation; `minimal_runtime` cannot
    become claim-grade by accident; report-only rows cannot masquerade as runtime support.
  - Verification: schema contract tests, release readiness metadata, benchmark completeness,
    website readiness, Python typed-report tests.
  - Non-goals: no runtime capability upgrade from schema work alone.
  - Claim boundary: evidence standardization and claim gating only.
  - Fallback boundary: every envelope exposes `fallback_attempted=false` and
    `external_engine_invoked=false` or fails.
  - Dependencies/blockers: stable field names, compatibility aliases, Python report migration,
    benchmark/website validators.
  - Ledger rule: ledger entry must record schema version, migrated surfaces, and validation failures
    now blocked.

- [ ] GAR-RUNTIME-IMPL-5I optimizer, session runtime, reuse, and buffer-pool promotion
  - Source: `GAR-RUNTIME-IMPL-4L`, `GAR-PERF-2B`, `GAR-PERF-2F`, `GAR-PERF-2G`,
    `GAR-IOREUSE-1`.
  - Current state: optimizer traces, source-state reuse, and batch/session evidence exist in scoped
    forms; ordinary workflows do not yet have a reusable session/cache lifecycle.
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
    promotion still needs fresh, profile-scoped evidence and public website/docs rendering.
  - Next slice outcome: require a current benchmark/correctness/evidence artifact for every
    promoted runtime path and block stale or incomplete public claims.
  - Runtime enablement: runtime-claim publishing validator that keeps public support status tied to
    fresh evidence.
  - User-visible surface: website benchmarks, docs/benchmarks, status page, release readiness.
  - Implementation scope: artifact freshness checker, profile matrix, runtime claim matrix,
    benchmark page ingestion, release validators.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, correctness
    refs, certificate refs, no-fallback fields, claim gate.
  - Acceptance: promoted paths are not presented publicly without current evidence; missing
    required lanes/scenarios are visible and block claim-grade status.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests.
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
  - Current state: object-store planning/report-only surfaces exist; runtime reads, credentials,
    network policy, and provider proofs are blocked.
  - Next slice outcome: implement provider URI parse, effect/credential policy, optional listing,
    byte-range/full-file read, local cache boundary, and SourceState evidence in an approved
    emulator or public no-credential fixture profile.
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
  - Claim boundary: provider/profile-specific technical-preview read proof only.
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
  - Claim boundary: fixture/local control-plane technical preview only.
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

- [ ] GAR-RUNTIME-IMPL-5Q final public technical-preview usability and website learning gate
  - Source: `GAR-RUNTIME-IMPL-4S`, `GAR-DOCS-1`, `GAR-WEB-ATLAS-1`, public-preview readiness,
    package-channel matrix.
  - Current state: repo, website, and docs are strong, but final usability requires clean install
    proof, examples, website/status parity, benchmark interpretation, security/legal/release checks,
    and a non-expert learning path after runtime slices land.
  - Next slice outcome: run a no-publication technical-preview rehearsal from clean checkout/local
    artifact through CLI/Python workflows, unsupported diagnostics, benchmarks, website/status,
    SECURITY/LICENSE/NOTICE checks, and release metadata.
  - Runtime enablement: final technical-preview usability validator across install, examples,
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
  - Non-goals: no public package upload, tag, production/platform/performance claim, Spark
    replacement claim, object-store/lakehouse/Foundry production claim, or hidden fast mode.
  - Claim boundary: public technical preview only with workload-scoped claims.
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
    capability commands, and execute scoped CLI-backed workflows. `ShardLoomSession` is still
    planned, and the Python layer is not yet a long-lived in-process session with reusable
    SourceState/PreparedState/OutputPlan caches.
  - Next slice outcome: implement a user-owned `ShardLoomSession`/context lifecycle that feels as
    simple as `SparkSession.builder...getOrCreate()` without creating a daemon, global hidden cache,
    or remote service.
  - Runtime enablement: explicit local session lifecycle for admitted runtime workflows, including
    session id, close/cleanup, cache hit/miss, invalidation, and no-fallback evidence.
  - User-visible surface: `import shardloom as sl`, `sl.context(...)`, future `sl.session(...)` or
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
    feature-gated local flat scalar `read_parquet(...)`
    query-builder chains support scoped projection/optional-filter/limit, preview/select-star, explicit-projection
    literal `with_column(...)`, `where(...)`, Python `sl.col(...).between(...)`, `head(...)`/
    `take(...)`, `count()`, scalar aggregate/optional-filter/limit, one-column grouped
    aggregate/optional-filter/limit, and single-key top-N collect/write workflows. Scoped local CSV
    inner equi-join, local `write_jsonl(...)`/`write_csv(...)` sink aliases, and generated-output
    helpers also exist for scoped local workflows. Engine-native range/sequence generated sources
    now support `limit(...)`, `head(...)`, and `take(...)` bound adjustment before local writes, with
    DataFrame capability rows separating generic `write`, JSONL, and CSV evidence requirements.
    Broad Python DataFrame joins, non-literal `with_column` over input-backed rows, broader
    expression projection, richer outputs, and parity-like method coverage remain
    unsupported/report-only.
  - Next slice outcome: promote DataFrame-style methods in user-value order with either runnable
    runtime or deterministic blockers: joins, `with_column`, expression projection, schema helpers,
    output writers, and collect/write ergonomics.
  - Runtime enablement: familiar DataFrame/query-builder workflows that execute through ShardLoom
    native runtime paths for admitted local inputs and outputs.
  - User-visible surface: `ctx.read_csv`, `ctx.read_json`, `ctx.read_parquet`, `ctx.read_vortex`,
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
  - Claim boundary: method-by-method local technical-preview support only.
  - Fallback boundary: DataFrame methods must lower to ShardLoom runtime or deterministic blockers.
  - Ledger rule: ledger entry must include method support table, runnable examples, and blockers.

- [ ] GAR-USER-SURFACE-1D one-command local install, import, and first workflow proof
  - Source: `GAR-COMMERCIAL-1A`, package channel matrix, `README.md`, `docs/getting-started/*`,
    `GAR-RUNTIME-IMPL-5Q`.
  - Current state: local source-tree and editable Python usage are documented, but public package
    publication is not complete and a non-expert install/import/run path still needs final proof.
  - Next slice outcome: provide a clean local technical-preview path from install to import to first
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
  - Claim boundary: local technical-preview install/import proof only.
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
    technical-preview readiness, Python capability matrix.
  - Current state: individual runtime slices can land without a single final validator answering
    whether the Python/SQL surface is simple and complete enough for technical-preview users.
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
