# ShardLoom Phased Execution Plan

## How to maintain this file
- Keep actionable working items in Planned.
- Keep Completed as a pointer to `docs/architecture/phased-execution-completed-ledger.md`; do not
  place detailed completed session blocks in this file.
- Keep Planned in logical implementation order even when CG or phase numbers are out of order.
- Do not keep a separate Active section; the next autonomous work should be the next unchecked
  Planned checklist item.
- Move completed session blocks to the top of
  `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion; do not
  reshuffle older completed history unless the content is incorrect.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

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
  detailed completed session and historical phase ledgers.
- Supporting architecture docs may contain rationale, inventories, traceability, and historical
  ledgers, but they must not introduce a second "current" queue.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- If a supporting doc records completed history, keep it clearly labeled as a completed ledger or
  historical note, and do not let it become a current queue.

Supporting docs:
- `README.md`
  - Role: project entry point, stable orientation, and compact core-concepts doorway.
  - Status rule: points to this phase plan and the completed ledger for current planned/completed
    state; must not duplicate working checklists or become the full glossary.
- `docs/architecture/phased-execution-completed-ledger.md`
  - Role: detailed completed session ledger and historical phase provenance split out of this phase
    plan.
  - Status rule: may record completed work only; it must not introduce planned work or a second
    current queue.
- `docs/architecture/rfc-phase-traceability.md`
  - Role: maps phases and CG work to governing RFCs.
  - Status rule: may record traceability history, but this file owns current work state.
- `docs/architecture/global-architecture-review.md`
  - Role: checkbox audit of every RFC plus the compute-engine flow against the repo.
  - Status rule: every unchecked item in that review must be mirrored into this Planned queue before
    implementation; checking a review item requires checking off the corresponding phase-plan item
    or moving the completed session to the ledger.
- `docs/architecture/compute-engine-flow-reference.md`
  - Role: canonical end-to-end flow for users, CLI/Python/REST access, adapters, I/O, execution
    modes, sinks, downstream consumers, evidence, and claim gates.
  - Status rule: planned nodes in the flow do not authorize implementation or claims until the
    corresponding item exists in this Planned queue and is completed with evidence.
- `docs/architecture/capability-certification-sequencing.md`
  - Role: CG-20 sequencing ledger and implementation-order reference.
  - Status rule: phase-plan checklist owns planned CG-20 work items.
- `docs/architecture/vortex-public-api-inventory.md`
  - Role: Vortex public API evidence and adapter-boundary inventory.
  - Status rule: API findings inform CG-1/CG-2/CG-3 queue items here.
- `docs/architecture/vortex-runtime-utilization-audit.md`
  - Role: Vortex-first runtime utilization audit for arrays, execution layers, Scan
    Source/Sink/Split, layouts, I/O, sessions/registries, device posture, extension types, and
    benchmark discipline.
  - Status rule: report/code surfaces here do not authorize runtime provider promotion; actionable
    provider or benchmark work must remain represented in this phase plan.
- `docs/architecture/vortex-adapter-integration-plan.md`
  - Role: Vortex adapter rationale, boundaries, and historical integration notes.
  - Status rule: adapter work is actionable only after represented in this phase plan.
- `docs/architecture/repo-cleanup-backlog.md`, `diagnostics-normalization-backlog.md`,
  `terminology-consolidation-backlog.md`, and `feature-footprint-doctor-plan.md`
  - Role: cleanup inventories and completed cleanup ledgers.
  - Status rule: cleanup must be promoted into this file as a concrete checklist item.
- `docs/architecture/canonical-terminology.md`
  - Role: authoritative glossary and concept index for ShardLoom vocabulary.
  - Status rule: defines terms and links to governing RFCs, but does not mark current phase or CG
    completion.
- `docs/architecture/systems-learning-map.md`
  - Role: technique-transfer map from external systems and design references into ShardLoom-native
    contracts.
  - Status rule: records lessons and guardrails only; it does not authorize dependencies, runtime
    behavior, or CG completion.
- `docs/architecture/benchmark-suite-catalog.md`
  - Role: CG-6.25 benchmark-suite catalog and Priority 2.7 source-backed correctness/benchmark
    matrix orientation.
  - Status rule: records matrix/catalog report surfaces, the executable local taxonomy runner
    status, and claim blockers; full comparative benchmark reruns and performance claims remain
    separate planned/release-readiness actions.
- `docs/architecture/crate-posture-public-exports.md`
  - Role: Priority 2.8 crate posture and public export grouping reference.
  - Status rule: documents current executable/report-only/unsupported/planned/prohibited-fallback export
    posture only; it does not authorize runtime or dependency expansion.
- `docs/architecture/workspace-feature-build-matrix.md`
  - Role: Priority 3.5 workspace feature/build validation matrix reference.
  - Status rule: records required validation rows and release blockers; it does not authorize
    package publication, dependency expansion, runtime expansion, or fallback execution.
- `docs/architecture/universal-import-deployment-baseline-harness.md`
  - Role: Priority 3.5 / CG-18 universal import, deployment, and baseline harness maturity
    reference.
  - Status rule: records required local/CI/container/optional Foundry/optional benchmark harness
    rows and comparison-only baseline environment boundaries; it does not authorize harness
    execution, package publication, external engine invocation, or fallback execution.
- `docs/architecture/rfc-coverage-followthrough.md`
  - Role: Priority 3.6 RFC coverage follow-through reference for RFC 0010, RFC 0011, RFC 0020,
    RFC 0022, and RFC 0023 before broader user/runtime expansion.
  - Status rule: records report-only coverage gates for developer/agent usability, modular
    extensibility, table/catalog compatibility, plan interop, and extension sandboxing; it does not
    authorize parser expansion, dependency expansion, imported-plan execution, extension execution,
    external effects, external engine invocation, or fallback execution.
- `docs/architecture/typed-command-result-envelope.md`
  - Role: Priority 3.9 typed command/result envelope reference for the `shardloom.output.v2`
    protocol slice and remaining command-family migration work.
  - Status rule: records the typed envelope slots and temporary legacy field mirror; it does not
    authorize runtime expansion, command effects, external engine invocation, REST/server behavior,
    or fallback execution.
- `docs/architecture/incumbent-gap-opportunity-map.md`, `lakehouse-value-prop-compatibility.md`,
  `universal-input-contract.md`, and `spill-reservation-lifecycle-integration.md`
  - Role: reference maps and constraints.
  - Status rule: they guide design decisions but do not mark CG completion.
- `docs/architecture/operational-evidence-policy-hardening.md`
  - Role: shared evidence, policy, workload, lifecycle, protocol-parity, benchmark-constitution, and
    artifact-safety contracts for CG-20 through CG-23.
  - Status rule: contract reference only; actionable implementation work must be represented in the
    Planned queue.
- `docs/architecture/vortex-upstream-alignment-hardening.md`
  - Role: Vortex compatibility, Scan API, compute-provider, residual-boundary, device,
    extension-type, object-store telemetry, integration-boundary, and benchmark-interoperability
    contract reference.
  - Status rule: contract reference only; it does not authorize new Vortex APIs, dependencies,
    runtime behavior, claims, or fallback execution.
- `docs/skills/vortex/vortex-first-provider-check.md`
  - Role: Vortex-adjacent implementation guard requiring agents to check upstream Vortex concepts
    and classify decisions before inventing new ShardLoom abstractions.
  - Status rule: process guard only; it does not authorize new Vortex API use, dependency changes,
    runtime behavior, support claims, external engine invocation, or fallback execution.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

### Global Architecture Review Carry-Forward

Source: `docs/architecture/global-architecture-review.md`.

Scope: every unchecked RFC and compute-flow review item is mirrored here so no planned,
unsupported, or not-claimable architecture work exists only in a supporting document. Complete these
items in logical implementation order, update the global review checkbox when evidence closes, and
move the completed session details to `docs/architecture/phased-execution-completed-ledger.md`.

Completion criteria for each item:

- Implement the behavior or add a deterministic unsupported diagnostic where implementation is still
  intentionally out of scope for the slice.
- Preserve `fallback_attempted=false` and `external_engine_invoked=false`.
- Attach workload-scoped correctness, benchmark, execution-certificate, Native I/O,
  materialization/decode, policy, and no-fallback evidence when a claim is made.
- Add or update focused tests, snapshots, or release/readiness checks for the touched surface.

#### GAR-P0 - Execution Mode, Provider Admission, And Vortex Spine

- [ ] GAR-FLOW-1: `direct_compatibility_transient` currently has vocabulary and deterministic
      unsupported/report-only capability rows, but no direct transient runtime path.
- [ ] GAR-FLOW-2: Prepared/native Vortex rows still rely on temporary materialized or residual
      ShardLoom-native operator paths for some scenarios until encoded/native operator coverage
      matures.
- [ ] GAR-FLOW-3: REST parity must emit the same policy, mode-selection, evidence, claim-gate, and
      no-fallback fields as CLI/Python surfaces before it can be treated as an equivalent API.
- [ ] GAR-0002A: Unsupported native coverage still produces unsupported/report-only surfaces for
      many paths.
- [ ] GAR-0002B: Native Vortex support is not universal across every source, sink, operator, and
      workload.
- [ ] GAR-0031: CG-19 is not universal across object-store/range-read, streaming sinks,
      table/catalog, external adapters, and all production source/sink paths.
- [ ] GAR-0042: Real Source/Split runtime paths, field-mask/predicate-ordering proof, layout/write
      evidence, object-store I/O, GPU/device execution, and managed-platform benchmark lanes remain
      incomplete.

#### GAR-P1 - Core Runtime, Operators, And Execution Safety

- [ ] GAR-0001A: Full SQL/DataFrame planner, distributed runtime, broad lakehouse-compatible output,
      and general object-store execution remain incomplete.
- [ ] GAR-0003: Full production Vortex segment extraction, broad operator coverage, and generalized
      materialization policy remain incomplete.
- [ ] GAR-0006: Broad predicate, DType, nested, null, and production metadata-only coverage remains
      incomplete.
- [ ] GAR-0008: Object-store I/O providers, probes, coordinator/worker runtime, checkpoint writes,
      retry execution, distributed execution, and object-store commits remain incomplete.
- [ ] GAR-0012: Runtime-wide diagnostic propagation for planned distributed and object-store paths
      remains incomplete.
- [ ] GAR-0013: Full streaming runtime and object-store streaming reads remain gated/report-only.
- [ ] GAR-0014: Broad runtime spill/OOM promotion and production enforcement remain limited to
      synthetic or local constraints.
- [ ] GAR-0016: Runtime adaptive execution, runtime filters, skew handling, and compaction writes
      remain incomplete.
- [ ] GAR-0017: Broad retry, cancellation, and commit execution remain incomplete.
- [ ] GAR-0018: Live profiling and distributed runtime introspection remain incomplete.
- [ ] GAR-0021: Broad expression execution, full function/kernel coverage, and UDF/effectful
      expression runtime remain incomplete.
- [ ] GAR-0026: Generalized direct encoded count/filter/project execution and production
      compressed-execution claims remain incomplete.
- [ ] GAR-0027: Real SIMD/vectorized dispatch, host CPU probing, production vectorized kernel path,
      adaptive parallelism runtime, and broad streaming runtime remain incomplete.
- [ ] GAR-0038: SQL/DataFrame runtime, object-store runtime, writes, and legacy facade compatibility
      remain incomplete; external engines remain baseline/oracle only.

#### GAR-P2 - I/O, Tables, Output, And Lakehouse Semantics

- [ ] GAR-0004: CDC planning, table/catalog metadata reads, object-store commits, generalized
      manifest serialization, and broad transaction semantics remain incomplete.
- [ ] GAR-0005: Broad Vortex reader/writer support, object-store Vortex I/O, general
      schema/encoding writes, and upstream Vortex write integration remain incomplete.
- [ ] GAR-0007: Actual Parquet, Arrow, Iceberg, Delta, and related compatibility output writers
      remain unimplemented or unsupported.
- [ ] GAR-0020: Catalog/table metadata integration, real table I/O, delete/tombstone execution, and
      CDC execution remain incomplete.
- [ ] GAR-0028: Object-store commit, table/catalog/lakehouse commit semantics, generalized sink
      commit, Foundry dataset transaction commit, upstream Vortex write API execution, and
      production output-payload fidelity remain incomplete.

#### GAR-P3 - User Surfaces, APIs, Adapters, And Workflow

- [ ] GAR-0010: Mature ergonomic runtime APIs, DataFrame/notebook surfaces, REST runtime, and
      user-facing package publication remain incomplete.
- [ ] GAR-0022: Real Substrait import/export and imported-plan execution remain incomplete.
- [ ] GAR-0030: Imported-plan execution and universal harness execution remain unimplemented
      without capability, certificate, Native I/O, and no-fallback evidence.
- [ ] GAR-0032: Broad SQL, DataFrame, UDF, notebook, universal adapter, unstructured/media, and
      best-default certification remain incomplete.
- [ ] GAR-0033: Mature DataFrame methods, SQL execution, joins, aggregations, windows, data-quality
      APIs, object-store/table runtime, publication, production ETL certification, and external
      baseline/oracle views remain incomplete.
- [ ] GAR-0034: Production live/hybrid engines, broker/state-store dependencies, object-store
      execution, freshness/exactly-once claims, and comparison-only baseline/oracle surfaces remain
      incomplete.
- [ ] GAR-0035: HTTP listener, remote execution, Flight/ADBC runtime bridge, broker integration,
      production API, and dependency-expanded server remain incomplete.
- [ ] GAR-0036: Production `shardloom-foundry`, package publication, Foundry service invocation,
      Artifact Repository publication, Compute Module, virtual-table native execution, Foundry
      dataset transaction runtime, and F10 workload-certified deployment remain incomplete.
- [ ] GAR-0037: Generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect, MCP,
      Flight, ADBC, and BI connector implementations remain incomplete.
- [ ] GAR-0039: Legacy flat `fields` mirror, remaining command-family result migration, some golden
      fixtures, Foundry boundary fixture, and additional physical handler splits remain incomplete.

#### GAR-P4 - Extension, Governance, And Runtime Policy

- [ ] GAR-0011: Extension execution, UDF execution, LLM/API calls, embeddings, and external effects
      remain unsupported/report-only.
- [ ] GAR-0019: Credential lifecycle, runtime policy enforcement, sandbox execution, and production
      governance remain incomplete.
- [ ] GAR-0023: Real plugin ABI loading, sandbox runtime, and UDF execution remain incomplete.

#### GAR-P5 - Correctness, Benchmarks, Claims, And Release

- [ ] GAR-0001B: Spark-displacement or engine-replacement claims remain not claimable until runtime
      and output evidence closes.
- [ ] GAR-0009: Broad claim-grade Spark-displacement evidence and public performance claims remain
      gated.
- [ ] GAR-0015: Fuzz/property expansion and claim-grade benchmark superiority coverage remain
      incomplete.
- [ ] GAR-0024: First public release/package publication, stable API/schema windows, and signing
      decisions remain incomplete.
- [ ] GAR-0025: Full competitive replacement remains incomplete until correctness, benchmark,
      Native I/O, certificate, capability, and no-fallback evidence is broad enough.
- [ ] GAR-0029: Broad CG-5/CG-6 coverage, production stateful reuse runtime, and
      performance/superiority claims remain incomplete.
- [ ] GAR-0040: Full comparative reruns, source-backed claim-grade promotion, managed-platform
      lanes, credentials, new managed dependencies, and public performance claims remain incomplete.
- [ ] GAR-0041: Release claims remain not claimable until required matrix rows have attached passing
      evidence.
- [ ] GAR-0043: Full hard release-readiness gate, actual publication, and final attestation remain
      incomplete.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
