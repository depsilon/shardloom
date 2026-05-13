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
  - Status rule: future cleanup must be promoted into this file as a concrete checklist item.
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
  - Status rule: documents current executable/report-only/blocked/future/prohibited-fallback export
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

### Top Findings Intake From Subagent Audits

These items came from the May 13, 2026 subagent audits and sit ahead of the normal P7/P8/P9 queue
until each finding is fixed, proven already fixed, or folded into a named child slice below. Review
threads must be re-read with `gh` before implementation because later merged work may already have
made some comments stale even when GitHub still shows them unresolved.

Thread-state rule: code fixes and doc reconciliation do not automatically close GitHub review
threads. Before release readiness, re-query PR #360 and up, classify each still-unresolved thread
as fixed, stale, intentionally deferred, or still actionable, and resolve/comment only with evidence
from the merged code and tests.

- [ ] Priority 7.4 - claim-grade compute-engine completion
  - Outcome: close the gap between executable/report-backed local paths and a claim-grade
    ShardLoom compute engine, without broad SQL/DataFrame/product claims.
  - Slice rule: group by evidence-bearing compute outcome. Each slice must either make a concrete
    compute path more inspectable/certifiable or add deterministic unsupported diagnostics for a
    named unsupported compute path.
  - Claim rule: no performance, best-default, Spark-displacement, production, or broad workflow
    claim may be emitted unless the workload constitution has correctness evidence, benchmark rows,
    execution certificates, Native I/O evidence where data is read/written,
    materialization/decode-boundary evidence, `fallback_attempted=false`, and
    `external_engine_invoked=false`.
  - [ ] P7.4.1 compute capability coverage matrix and operator-family ladder bundle.
    - User-visible surface: a machine-readable compute capability matrix across operator,
      expression, source, sink, and engine-mode surfaces, plus a family ladder for scalar
      expressions, predicates, projection, filter-project fusion, aggregates, grouped aggregates,
      approximate/sketch aggregates, sort/top-N/limit, joins, semi/anti joins, windows, set
      operations, nested/extension operations, and sink/write operators.
    - Acceptance: every row records support status (`unsupported`, `planned`, `report_only`,
      `executable_uncertified`, `fixture_certified`, `workload_certified`,
      `production_certified`), engine mode, provider kind (`shardloom_kernel`,
      `vortex_array_kernel`, `vortex_scan`, `vortex_source`, `vortex_sink`,
      `compatibility_boundary`, `external_baseline_only`), semantic profile,
      materialization/decode requirement, memory/spill requirement, correctness/benchmark/
      certificate/Native I/O refs, unsupported diagnostic code, and no-fallback evidence.
    - Verification: CLI/Python capability snapshots, matrix schema tests, no-fallback invariants,
      and benchmark/workflow/API view consistency checks.
  - [ ] P7.4.2 ShardLoomNative semantic conformance and unsupported API parity bundle.
    - User-visible surface: executable semantic conformance tests for supported operators plus
      complete report-only diagnostics for planned DataFrame and SQL surfaces before broad runtime
      implementation.
    - Acceptance: supported compute operators cover null comparison, three-valued logic, null sort
      ordering, NaN equality/order, signed zero, integer overflow, decimal precision/scale,
      timestamp/date behavior, string collation/case sensitivity, binary equality, empty/count-null
      aggregate behavior, join null semantics, window defaults, duplicate columns, nested/list
      equality, and schema field identity where applicable. Unsupported DataFrame/SQL methods expose
      stable diagnostics, blockers, required evidence, rewrite suggestions, and no-fallback status
      across CLI, Python, and future REST views.
    - Verification: semantic fixture suites, unsupported diagnostic snapshots, Python parity tests,
      and external-oracle-reference-only policy checks.
  - [ ] P7.4.3 execution artifact richness and provider-evidence preservation bundle.
    - User-visible surface: top-level `ShardLoomExecutionResult` and typed CLI/Python envelopes
      preserve rich provider evidence from local, Vortex primitive, prepared encoded,
      source-backed, reader-backed, and report-only plans.
    - Acceptance: execution results carry result refs, artifact refs, inline artifacts where
      appropriate, execution/Native I/O certificate refs, materialization/residual boundary refs,
      representation transitions, provider API surface/version, source refs, split refs, lifecycle
      status, and fallback status. Missing evidence is explicit evidence-incomplete state, not an
      omitted or flattened string.
    - Verification: provider-result contract tests, typed-envelope golden cases, Python typed view
      tests, and evidence-incomplete snapshots.
  - [ ] P7.4.4 benchmark taxonomy execution and measured source-backed row population bundle.
    - User-visible surface: benchmark taxonomy coverage that is executable enough to validate
      common compute-engine workload shapes, plus measured source-backed rows for eligible prepared,
      source-bound, and reader-backed encoded filter/projection/filter-project paths.
    - Acceptance: add executable dataset profiles such as wide/very-wide tables, null-heavy data,
      many-small/few-large files, date partitioning, clustered/unclustered layouts, schema drift,
      dirty CSV, nested JSON, and CDC delta overlay where supported. Add executable scenarios for
      filter/projection/limit, multi-key group by, join+aggregate, row-number window,
      clean/cast/filter/write, dirty timestamps, small-change-over-large-base, partition pruning,
      many-small-files scan, null-heavy aggregate, high-cardinality string group/distinct, wide
      projection, and top-N per group. Blocked source-backed rows remain deterministic unsupported
      diagnostics with required future evidence.
    - Verification: benchmark catalog/schema tests, measured-row manifests, coverage-table
      snapshots, reproducibility checks, and external-baseline-local-only assertions.
  - [ ] P7.4.5 sink/write, replayability, and first workload-certified compute workflow bundle.
    - User-visible surface: at least one complete source -> supported compute -> Vortex sink path
      with inspectable output artifact, commit/recovery status, replay verification, execution
      certificate, Native I/O certificate, benchmark row, coverage row, and CLI/Python invocation.
    - Acceptance: output artifacts include digest/hash, schema/dtype summary, Native I/O
      certificate, expected row/stat/schema replay evidence, materialization/decode evidence, and
      deterministic failure when replay verification fails. The first candidate workload
      constitution is scoped as `local_vortex_analytics_v1`; any claim is workload-scoped only.
    - Verification: end-to-end fixture, output reopen/replay tests, certificate checks, benchmark
      row checks, cleanup/rollback assertions, and no external-engine replay checks.
  - [ ] P7.4.6 local scheduler/runtime and memory/spill operator maturity bundle.
    - User-visible surface: supported local workloads execute through task/split graph scheduling
      with runtime sizing, memory reservation, bounded queues, cancellation/retry evidence, and
      operator-specific memory/spill maturity for group by, distinct, sort/top-N, joins, windows,
      sink/write, sketch/approx aggregates, and live/hybrid state where supported.
    - Acceptance: scheduler decisions are recorded in execution certificates; backpressure and
      queue limits are enforced; safe/idempotent retry and cancellation paths are testable; every
      supported stateful operator declares spill support or a deterministic no-spill blocker;
      fail-before-OOM diagnostics are tested; spill cleanup/recovery semantics are explicit.
    - Verification: scheduler integration tests, cancellation/retry tests, memory reservation
      assertions, spill lifecycle tests, benchmark task metrics, and no large-workload claim checks.
  - [ ] P7.4.7 Vortex layout/write advisor feedback loop bundle.
    - User-visible surface: report-only advisor that consumes workload constitutions and benchmark/
      runtime evidence to recommend Vortex layout, encoding, chunking, statistics, clustering,
      micro-segment flush, and compaction strategies.
    - Acceptance: advisor reports read/write tradeoffs, required evidence, and whether each
      recommendation is measured, simulated, or blocked. It does not claim improvement without
      evidence and remains report-only until write/layout execution is certified.
    - Verification: advisor report snapshots, evidence-source validation, measured/simulated status
      tests, and no-claim/no-write policy checks.
- [ ] Priority 8 - general availability and external proof-of-use
  - Outcome: make a non-maintainer able to install, import, smoke, inspect capabilities, and run a
    certified local path without relying on unpublished assumptions or hidden local state.
  - Slice rule: package/release PRs must include an install or proof artifact. Documentation-only
    edits are acceptable only when they are tied to runnable smoke commands or release gate fixtures.
  - [ ] P8.1 release identity, packaging contract, and artifact integrity bundle.
    - User-visible surface: public release identity and versioning policy for PyPI `shardloom`,
      conda-forge `shardloom-cli`, `shardloom-python`, `shardloom` metapackage, GitHub Release
      artifacts, GHCR/OCI image posture, and selected crates.io protocol/client crates.
    - Acceptance: release workflow contracts cover Git tag, source archive, platform binaries,
      Python wheel/sdist, Conda recipe/feedstock status, checksums, SBOM, artifact attestation,
      changelog, compatibility matrix, known unsupported paths, and no-fallback release checks.
      Trusted publishing/OIDC is preferred; long-lived tokens, publication, release tags, feedstock
      submission, crates.io publication, OCI pushes, and Marketplace publication remain
      human-approved and release-gated.
    - Verification: package metadata checks, dry-run artifact manifests, checksum/SBOM fixtures,
      release-gate snapshots, and no-secret policy tests.
  - [ ] P8.2 clean install and first-10-minutes proof bundle.
    - User-visible surface: Conda-first clean-environment proof for `shardloom-cli`,
      `shardloom-python`, and `shardloom` metapackage, plus the public first-10-minutes path:
      `conda install shardloom`, `import shardloom`, `ShardLoomClient.from_env().smoke_check()`,
      `client.capabilities()`, `shardloom status --format json`, and
      `shardloom capabilities --format json`.
    - Acceptance: CLI binary resolution, Python import, status/capability output, and
      `fallback_attempted=false` smoke evidence are documented and reproducible from a clean
      environment. The scripted proof creates a clean env, installs ShardLoom from release
      artifacts, imports `shardloom`, runs `ShardLoomClient.from_env().smoke_check()`, runs
      `shardloom status --format json` and `shardloom capabilities --format json`, executes one
      local Vortex fixture, inspects execution and Native I/O certificates, verifies
      `fallback_attempted=false`, and runs a local benchmark smoke without external engines or
      Foundry.
    - Verification: clean-env smoke transcript, Python import tests, CLI resolution diagnostics,
      install docs checks, and workspace validation.
  - [ ] P8.3 external examples, docs, and baseline-comparison boundary bundle.
    - User-visible surface: `examples/local-python-smoke/`, `examples/local-vortex-benchmark/`, and
      `examples/foundry-lightweight-transform/` each include README, environment file, input
      fixture, expected output, expected certificate fields, and known limitations.
    - Acceptance: user-facing docs cover install, quickstart, Python client, CLI, Conda packages,
      Foundry usage, benchmarking, certificates, no-fallback policy, Vortex compatibility, maturity
      statuses, and unsupported diagnostics. Benchmark extras and Spark/DataFusion/DuckDB/Polars/
      pandas comparison tooling stay out of the core install path and are reported as baselines only.
    - Verification: example smoke scripts, docs link checks, expected-output snapshots, dependency
      posture checks, and full workspace validation.
  - [ ] P8.4 hard release-readiness gate bundle.
    - User-visible surface: release gate report/command that refuses public-release readiness when
      runtime, protocol, packaging, benchmark, provenance, and known-unsupported-path gates are
      incomplete.
    - Acceptance: gate covers `cargo fmt`, clippy, workspace tests, feature/build matrix
      default/all/no-default/key combinations, Python tests, wheel/sdist build, clean venv install,
      clean Conda env install, CLI binary resolution, smoke check, benchmark smoke, typed-envelope
      compatibility, package metadata/license review, SBOM/provenance/checksum generation, runtime
      package no-fallback dependency audit, and release notes with known unsupported paths. Public
      claims are generated from evidence artifacts, not prose.
    - Verification: release-gate snapshots, package build/install dry runs, feature/build matrix
      checks, provenance fixtures, no-secret/no-fallback dependency checks, and docs/claim
      consistency tests.
- [ ] Priority 9 - RFC 0036 Foundry integration pack and platform availability
  - Outcome: make Foundry an optional integration pack with certificate-aware staging and proof
    surfaces, not a new core execution engine and not a shortcut around ShardLoom-native evidence.
  - Slice rule: group Foundry work by usable platform lane. Each slice must include package posture,
    report schemas, example or smoke evidence, and explicit no-fallback/external-compute boundaries.
  - Runtime rule: Foundry virtual tables and external compute are workflow handles, baselines,
    or migration/oracle references. ShardLoom-native execution still requires staged/native data plus
    certificates; no Snowflake/Databricks/BigQuery/Spark/Foundry compute pushdown may be reported as
    ShardLoom execution.
  - [ ] P9.1 Foundry package, execution context, and maturity ladder bundle.
    - User-visible surface: `shardloom-foundry` helper package posture, deterministic
      `SHARDLOOM_BIN` resolution, Foundry transform metadata capture, input/output RID capture,
      certificate output writing, benchmark metrics writing, staging/materialization reports, and
      no-fallback propagation without adding execution semantics.
    - Acceptance: the Foundry maturity ladder covers `F0` declared only through `F10`
      workload-certified deployment; `FoundryExecutionContext`, `FoundryDatasetTransactionReport`,
      `FoundryBranchContextReport`, `FoundryPreviewModeReport`, and
      `FoundryReleaseReadinessReport` identify transform, branch, preview/build/incremental mode,
      transactions, package versions, workload constitution, and expected evidence.
    - Verification: package-resolution smoke, maturity matrix snapshots, transform metadata fixtures,
      and no-execution/no-fallback policy tests.
  - [ ] P9.2 dataset source/sink staging, certificate output, and incremental run bundle.
    - User-visible surface: `FoundryDatasetSource`, `FoundryDatasetSink`,
      `FoundryCertificateOutput`, and `FoundryIncrementalRunReport` for staged local files,
      table-compatible outputs, certificate/metrics datasets, optional Vortex artifact sidecars,
      materialization/fidelity reports, commit/recovery status, and batch/live/hybrid evidence
      alignment.
    - Acceptance: Foundry incremental builds are aligned with ShardLoom evidence but are not treated
      as live/hybrid certification by themselves; all sources/sinks keep `fallback_attempted=false`
      and explicit materialization policy.
    - Verification: source/sink schema snapshots, certificate-output fixtures, incremental evidence
      fixtures, commit/recovery blocker tests, and package smoke.
  - [ ] P9.3 Data Health, lineage, governance, and platform boundary bundle.
    - User-visible surface: `FoundryDataHealthBridge`, Data Expectations mapping,
      `FoundryLineageFacet`, `FoundryScheduleBuildReport`, `FoundryDataConnectionBoundaryReport`,
      and `FoundryGovernanceBoundaryReport`.
    - Acceptance: reports cover certificate presence, no-fallback status, Native I/O evidence,
      schema digest, output row requirements, data-quality checks, materialization policy,
      benchmark-claim blockers, datasets, virtual tables, media sets, artifacts, schedules, syncs,
      exports, webhooks, external transforms, credential refs, egress policy, markings,
      organizations, inherited markings, certificate visibility, redaction, export policy, agent
      visibility, and artifact safety.
    - Verification: data-health fixtures, lineage/governance snapshots, redaction checks,
      credential-reference assertions, and no-egress diagnostics.
  - [ ] P9.4 virtual table, S3/Iceberg/media, and external-compute boundary bundle.
    - User-visible surface: `FoundryS3DatasetAdapter`, `FoundryVirtualTableSource`,
      `FoundryVirtualTableSink`, `FoundryVirtualTableRef`, `FoundryExternalComputeBoundaryReport`,
      `FoundryIcebergTableSource`, `FoundryIcebergTableSink`, `FoundryMediaSetSource`,
      `FoundryVirtualMediaSetSource`, `FoundryMediaSetSink`,
      `FoundryMediaExtractionBoundaryReport`, `FoundryModelCallBoundaryReport`, and
      `FoundryEmbeddingBoundaryReport`.
    - Acceptance: S3-compatible dataset access records dataset RID, branch, object key, range-read
      support, multipart/write posture, bytes/request counts, credential mode, and Native I/O
      certificates. Virtual tables for Snowflake, Databricks, BigQuery, S3, ADLS, GCS, Iceberg, and
      similar systems are governed external handles with metadata, staging, update-detection,
      security, and materialization policy. External compute pushdown is classified as baseline,
      oracle, migration reference, or prohibited fallback, never ShardLoom-native execution. Media
      sources/sinks declare MIME/schema, OCR/extraction/model/materialization boundaries,
      provenance/confidence, incremental media status, redaction, and no silent OCR/transcription/
      embedding/model calls.
    - Verification: external-boundary matrix snapshots, materialization/fidelity assertions,
      credential-mode fixtures, media no-silent-model-call tests, and no-fallback policy checks.
  - [ ] P9.5 ontology/functions/model, Compute Module/BYOC, marketplace, and benchmark bundle.
    - User-visible surface: `FoundryOntologyMappingReport`, `FoundryFunctionSurface`,
      `FoundryAipLogicBridge`, `FoundryAipLogicBoundaryReport`,
      `FoundryModelBoundaryReport`, `FoundryUnstructuredWorkflowCertificate`,
      `FoundryScenarioBoundaryReport`, `FoundryByocImageReport`, `FoundryComputeModuleSurface`,
      `FoundryComputeModuleReadinessReport`, `FoundryMarketplaceStarterProduct`, and Foundry
      benchmark schema.
    - Acceptance: Compute Modules remain blocked until CG-23 API/security/package evidence exists;
      Marketplace starter product includes Conda dependency instructions, smoke transform, benchmark
      transform, certificate output dataset, Data Expectations bridge, optional virtual-table staging
      example, optional external-compute baseline example, optional Compute Module API example,
      schedule, and docs. Benchmarks label ShardLoom lightweight, Polars lightweight,
      DataFusion/DuckDB baseline, Spark distributed, and Snowflake/Databricks/BigQuery pushdown rows
      separately with compute mode, materialization boundary, certificates, correctness digest, and
      versions.
    - Verification: marketplace fixture smoke, benchmark schema snapshots, model/function boundary
      tests, Compute Module blocker snapshots, and release-readiness policy checks.
  - [ ] P9.6 Foundry proof-of-use certification bundle.
    - User-visible surface: proof that ShardLoom can be installed/imported and used inside a
      Foundry Python code repository without making Foundry compute an execution fallback.
    - Acceptance: proof covers Conda/internal artifact install, transform import, deterministic CLI
      binary resolution, no-dataset smoke, explicit local/staged dataset path, supported
      local/native ShardLoom execution, certificate/metrics dataset output, materialization/staging
      boundary reports, and `fallback_attempted=false`. Foundry Spark, Snowflake, Databricks,
      BigQuery, virtual tables, and external compute remain external boundaries, governed handles,
      baselines, or migration/oracle references, not ShardLoom-native execution.
    - Verification: Foundry-style transform fixture, package/import smoke, staged dataset fixture,
      certificate dataset snapshots, Data Health/Data Expectations bridge checks where practical,
      and no-fallback boundary tests.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
