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

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

### Near-term Implementation Priority
- [x] Priority 1 - generalized encoded primitive execution loop
  - [x] Generalized local direct encoded `CountAll` execution beyond the original checked-in fixture
        proof path, with copied/non-fixture local `.vortex` targets execution-allowed but
        uncertified.
  - [x] Fixture-backed local `CountWhere` execution evidence through `vortex-count-where` with
        explicit selection-vector guarantee.
  - [x] Fixture-backed and non-fixture local `FilterPredicate` execution evidence through
        `vortex-filter` with explicit selection-vector guarantee and correctness-certification
        split.
  - [x] Fixture-backed and non-fixture local `ProjectColumns` execution evidence through
        `vortex-project` with explicit encoded-projection guarantee and correctness-certification
        split.
  - [x] Fixture-backed and non-fixture local `FilterAndProject` execution evidence through
        `vortex-filter-project` with explicit selection-vector/projection guarantee and
        correctness-certification split.
  - [x] Reusable generalized local CountWhere/FilterPredicate scan-pushdown execution surface beyond
        CLI-only helpers.
  - [x] Reusable generalized local ProjectColumns/FilterAndProject scan-pushdown execution surface
        beyond CLI-only helpers.
  - [x] Encoded-value predicate kernel foundation for constant, dictionary-coded, and run-length
        encoded batches with sparse selection-vector output.
  - [x] Vortex encoded-value predicate bridge feeds sparse selections into selection-vector
        filter-kernel evidence without reader wiring.
  - [x] Multi-segment Vortex encoded-value filter evidence aggregates prepared encoded batches into
        complete selection-vector filter-kernel evidence without reader wiring.
  - [x] Prepared encoded projection/filter-project evidence preserves encoded projected batches and
        safe selection-vector evidence without reader wiring.
  - [x] Prepared encoded-batch generalized encoded-value predicate/filter evidence beyond local
        scan-pushdown and explicit CLI primitive paths, without reader-backed/source-backed
        execution claims.
  - [x] Prepared encoded-batch generalized projection and filter-project evidence beyond local
        scan-pushdown and explicit CLI primitive paths, with no row reads, no Arrow conversion, no
        object-store IO, no writes, no spill, and no fallback.
- [x] Priority 1.1 - reader-backed/source-backed generalized encoded execution
  - [x] Add source-bound prepared encoded filter/projection envelopes that require native Vortex
        source URI refs, stable split refs, source/batch URI matching, existing prepared encoded
        execution evidence, no external effects, and no fallback.
  - [x] Wire real local Vortex reader/source/split evidence into the prepared encoded-batch
        predicate, projection, and filter-project execution surfaces by binding prepared batches to
        reader-emitted split refs while preserving explicit no-decode/no-materialization/no-fallback
        diagnostics.
  - [x] Generate reader-backed prepared chunk envelopes from actual local Vortex scan chunks with
        provider refs, split refs, representation transitions, residual boundary status, and no
        decode/materialization/fallback evidence while keeping simplified encoded kernel-input
        lowering blocked for opaque chunks.
  - [x] Add reader-generated encoded kernel-input admission for explicitly mapped encoded batches,
        validating source URI, split refs, row counts, dtype/encoding/value mapping evidence, reader
        effects, and no fallback before executing filter/projection/filter-project paths through
        reader-backed prepared execution.
  - [x] Extract typed constant encoded kernel inputs directly from actual local Vortex reader chunks
        when upstream `ArrayRef::as_constant()` exposes values without decode/materialization, then
        wire those local reader-generated inputs into local primitive reports.
  - [x] Expand direct reader-chunk kernel-input extraction beyond constants for non-null host
        primitive dictionary and run-end arrays when upstream dtype/encoding-specific APIs expose
        slots without decode, scalar row reads, Arrow conversion, or materialization.
  - [x] Keep sparse, nullable dictionary/RLE, device-buffer, nested, extension, and other encoded
        reader-chunk extraction blocked until ShardLoom has matching encoded-batch variants,
        validity semantics, device evidence, or explicit layout certificates without
        decode/materialization.
  - [x] Keep upstream Vortex-native providers allowed only through feature-gated, version-recorded,
        policy-admitted, certificate-backed provider boundaries.
  - [x] Reject or ShardLoom-native-execute residual expressions; never delegate residual evaluation
        to DataFusion, DuckDB, Spark, Polars, Velox, Trino, Dask, Ray, or Vortex query-engine
        integrations.
  - [x] Pair each source-backed execution expansion with CG-5 correctness fixtures/reference
        outputs, CG-6 benchmark rows, CG-16 execution certificates, CG-19 Native I/O certificates,
        and explicit no-fallback evidence.
- [x] Priority 1.5 - CG-20 distribution/importability certification lane
  - [x] Publishable pure-Python wrapper package with wheel/sdist build evidence.
  - [x] Platform-specific Conda CLI binary recipe scaffold, pure-Python wrapper recipe scaffold, and
        optional one-command metapackage scaffold.
  - [x] Clean Conda package build/install certification and package publication remain release-gated
        until tag/hash/version/provenance and human approval gates pass.
  - [x] Fresh-environment import/run smoke evidence for `import shardloom`, CLI binary resolution,
        `smoke_check()`, protocol/version reporting, and `fallback_attempted=false`.
  - [x] `python-wrapper-plan`, `capabilities python`, `capabilities deployment`, and
        `world-class-sufficiency-plan` expose packaging/importability state without probes or
        runtime expansion.
  - [x] Benchmark extras remain optional comparison-only environments and are not dependencies of
        `conda install shardloom`.
- [x] Priority 1.6 - modular compute-engine architecture spine
  - [x] Keep the implementation architecture explicitly layered: frontends and adapters lower into
        ShardLoom logical IR, semantic/profile binding, capability admission, optimizer rewrites,
        physical planning, execution-provider selection, scheduler/runtime execution, sink delivery,
        and evidence artifact emission.
  - [x] Maintain crisp crate/module boundaries for contracts, logical/physical plans, expressions,
        operator/function registries, Vortex provider adapters, source/sink adapters,
        scheduler/runtime, memory/spill, live/hybrid state, catalog/table semantics,
        observability/evidence, governance/policy, API surfaces, benchmarks, and packaging.
  - [x] Promote execution providers as a first-class abstraction so ShardLoom kernels, ShardLoom
        metadata paths, upstream Vortex-native providers, compatibility boundaries, and external
        baselines cannot be confused.
  - [x] Build operator, function, aggregate, sketch, window, join, sort/top-N, and sink registries
        around typed capability descriptors, semantic profiles, state contracts, memory
        declarations, materialization requirements, and certificate requirements before broad API
        claims.
  - [x] Treat DTypes, encoded representation state, selection vectors, segment statistics, null
        semantics, dictionary/run-length/sparse encodings, materialization/decode boundaries, and
        Native I/O envelopes as shared data-model primitives, not per-command special cases.
  - [x] Give the runtime a task/split graph with dynamic sizing, target-task policy, bounded queues,
        cancellation, retry, backpressure, memory/spill reservations, object-store request budgets,
        and cost/fairness accounting before large-workload or live/hybrid claims.
  - [x] Make evidence artifacts, diagnostics, lineage, profiles, and benchmark rows first-class
        outputs of execution rather than log side effects.
  - [x] Preserve deterministic unsupported behavior: no hidden external query-engine execution, no
        silent materialization, no unreported residual evaluation, and no claim-grade support
        without correctness, benchmark, certificate, Native I/O, policy, and workload evidence.
- [x] Priority 2 - evidence loop paired with every execution expansion
  - [x] CG-5/CG-16 fixture-backed execution certificates wired into generalized local primitive
        filter/projection surfaces.
  - [x] CG-16 evidence-incomplete execution certificates wired into prepared encoded
        filter/projection surfaces while CG-5 fixture certification remains blocked.
  - [x] CG-5/CG-16 prepared encoded filter/projection fixtures certify the narrow prepared encoded
        execution reports.
  - [x] CG-5 decoded-reference artifact metadata covers prepared encoded
        filter/projection/filter-project fixture outputs without decoded-reference execution.
  - [x] CG-5 decoded-reference artifact metadata covers every current executable fixture family
        without decoded-reference execution.
  - [x] CG-5 fixtures, reference outputs, correctness certificates, and edge-case coverage for each
        widened primitive path.
    - [x] CG-5.15 generated edge-case executable fixture matrix covers empty, single-row, all-null,
          mixed-null, duplicate, low/high-cardinality, sorted/unsorted, dictionary, run-length,
          sparse-validity, and temporal primitive cases with test-only decoded-reference artifacts.
    - [x] CG-5.16 generated property fixture families and reproducible fuzz seeds are present
          without executing property/fuzz tests.
    - [x] CG-5.17 source-backed edge fixture manifest coverage and declared external-oracle result
          artifact slots are present without executing external engines.
    - [x] CG-5.18 benchmark claim gate explicitly stays blocked until external-oracle artifacts are
          populated and property/fuzz execution is performed.
    - [x] CG-5.19 ambiguous `NotYetDefined` fixtures are replaced with explicit deferred
          fixture-family blockers.
    - [x] CG-5.20 deferred fixture-family artifact slots are declared with population status,
          required refs, and no-execution/no-fallback evidence.
    - [x] CG-5.21 source-backed reader-chunk dictionary/run-end kernel-input fixtures,
          decoded-reference artifact slots, and external-oracle artifact slots are present without
          executing decoded references or external engines.
    - [x] Keep populated/executed external-oracle results, property/fuzz execution, and populated
          executable deferred fixture-family artifacts as explicit closed gates before claim-grade
          correctness closeout.
  - [x] CG-6 query-runtime benchmark rows, reproducibility metadata, work-avoidance evidence, and
        claim-gate blockers for each new primitive path.
    - [x] CG-6.24 source-backed reader-chunk dictionary/run-end benchmark scenarios, required
          metrics, expected ShardLoom/Vortex-integration result slots, reproducibility metadata,
          work-avoidance metrics, and no-claim blockers are present without executing benchmarks or
          external engines.
    - [x] Keep measured benchmark result rows, populated reproducibility manifests, and approved
          comparison rows as explicit closed gates before claim-grade source-backed benchmark
          closeout.
  - [x] CG-16 execution certificates and CG-19 per-path Native I/O certificates for each supported
        source/sink path.
    - [x] Source-backed filter/projection reports expose `VortexSourceBackedCertificatePairReport`
          with execution-certificate IDs/statuses, per-path Native I/O certificate IDs/path
          IDs/statuses, certificate-pair completeness, no-fallback status, and benchmark-claim
          blockers.
- [x] Priority 2.5 - Vortex upstream alignment and compatibility hardening
  - [x] Promote `VortexCompatibilityMatrix` into report/code surfaces covering crate version,
        file-format assumption, Rust/toolchain fit, enabled features, local read/write posture, Scan
        API, Source/Sink, split serialization, DType/layout/statistics mapping, encoding/layout
        coverage, Arrow boundaries, PyVortex, object-store, GPU/device, extension dtype, and known
        unsupported Vortex APIs.
  - [x] Promote `VortexScanCompatibilityReport` into report/code surfaces aligning ShardLoom Native
        I/O envelopes with Vortex Source/Sink/Split concepts for projection, filter, limit, field
        masks, split estimates, split serialization, sink requirements, pushdown decisions,
        residuals, and Native I/O certificates.
  - [x] Promote `CompositePushdownCapabilityMatrix` into capability/report surfaces so
        filter/projection/limit/order/reverse/top-N/range/zone-pruned combinations are tracked
        separately from individual primitive support.
  - [x] Promote `ExecuteStepEvidence` into certificate/report surfaces for deferred, fused, reduced,
        canonicalized, materialized, and final-representation stages.
  - [x] Promote `DeviceResidencyReport` into report surfaces for future CPU/GPU/device evidence
        while keeping CPU default and GPU claims blocked until runtime evidence exists.
  - [x] Promote `ExtensionTypeCapabilityMatrix` into capability/report surfaces for vector, tensor,
        map, variant/JSON, UUID, geospatial, raster, embedding, document, and media-reference types
        without implying execution support.
  - [x] Promote `StreamingSinkCertificate`, `IoBackendEvidence`, `ExecutionTelemetryFacet`,
        `ApproxAnalyticsCertificate`, `CompressionAdvisorReport`, `IntegrityAndEncryptionReport`,
        `PythonVortexInteropReport`, `ForeignRuntimePosture`, and `VortexBenchmarkInterop` into
        report/code surfaces.
  - [x] Keep `ApproxAnalyticsCertificate` separate from `CompressionAdvisorReport`: approximate
        query answers require exact-reference/error-bound evidence, while compression-advisor
        sketches guide encoding/layout choices and never count as exact correctness proof.
  - [x] Keep all items docs/report-only unless later promoted; no Vortex integration fallback, GPU
        claim, vector/geospatial/media claim, object-store/write/streaming claim, or benchmark
        superiority claim.
- [x] Priority 2.6 - Vortex compute-provider alignment
  - [x] Keep README, RFCs, terminology, AGENTS, and Vortex inventory language aligned that
        "standalone" means standalone from external query-engine fallback, not isolated from
        upstream Vortex compute.
  - [x] Use `Vortex-native execution provider` terminology for upstream Vortex array, compute, scan,
        source, and sink APIs admitted through ShardLoom policy and certificate evidence.
  - [x] Promote `ExecutionProviderKind` fields into execution certificates so each path can
        distinguish ShardLoom kernels, ShardLoom metadata, Vortex array kernels, Vortex compute
        functions, Vortex scan/source/sink providers, compatibility import/export, external
        baselines, and prohibited external fallback.
  - [x] Promote `VortexComputeProviderReport` into report/code surfaces with provider kind, Vortex
        version, feature gate, API surface, operation,
        dtype/encoding/layout/null/selection-vector/materialization behavior, residual status,
        external-engine status, and no-fallback evidence.
  - [x] Promote `ResidualBoundaryReport` and `residual_executor` values into report/code surfaces:
        `none`, `shardloom_native`, `unsupported_blocked`, `external_baseline_only`, and
        `prohibited_external_fallback`.
  - [x] Promote `VortexIntegrationBoundaryReport` into report/code surfaces classifying upstream
        Vortex native APIs as allowed native providers while Vortex DataFusion, DuckDB, Spark,
        Trino, and similar integrations remain baseline/reference/oracle-only.
  - [x] Keep Vortex public API inventory and dependency-review top matter current when executable
        Vortex support changes.
  - [x] Require Vortex-native providers to be feature-gated, version-recorded, policy-admitted, and
        certificate-backed before support claims.
- [ ] Priority 3 - broader platform work after the primitive/evidence loop advances
  - [x] CG-4 broader commit execution.
    - [x] Add `CommitExecutionPromotionGateReport` so broader commit surfaces are named and kept
          blocked behind output manifest, sink requirements, materialization/fidelity,
          idempotency, recovery/rollback, ambiguous-commit diagnostics, backend atomicity,
          table/catalog transaction, credential/effect policy, execution-certificate, Native I/O,
          and no-fallback evidence.
    - [x] Keep existing feature-gated local committed-manifest copy and local rollback cleanup
          visible as narrow local paths while keeping generalized local sink, object-store,
          table/catalog, source/sink, Foundry dataset transaction, and live/hybrid checkpoint commit
          promotion blocked.
    - [x] Expose the gate through `commit-execution-promotion-gate` without runtime execution,
          write IO, object-store IO, catalog IO, external effects, claim publication, or fallback
          execution.
  - [x] CG-8 dynamic sizing feedback execution and bounded parallel encoded/read runtime.
    - [x] Add `DynamicRuntimePromotionGateReport` so runtime application of dynamic sizing
          feedback and bounded parallel encoded/source-backed reads remains blocked until runtime
          metrics, target-task policy, scheduler queue policy, memory/spill reservation,
          backpressure, cancellation/retry, execution-certificate, Native I/O, benchmark, and
          no-fallback evidence exists.
    - [x] Keep existing local streaming scan, bounded metadata/no-op, and local filter-project
          bounded scan evidence visible as narrow local evidence while keeping dynamic feedback
          mutation, bounded parallel encoded reads, source-backed reader split parallelism,
          scheduler requeue, bounded backpressure runtime, memory/spill runtime, and object-store
          request-budget runtime blocked.
    - [x] Expose the gate through `cg8-runtime-promotion-gate` without runtime execution, task
          execution, data reads, materialization, object-store IO, write IO, spill IO, policy
          mutation, large-workload claims, or fallback execution.
  - [x] CG-9 catalog/table metadata integration.
    - [x] Add `CatalogMetadataIntegrationGateReport` so catalog/table metadata integration
          surfaces are named and kept blocked behind table-intelligence reports, catalog refs,
          snapshot refs, schema/partition/delete policy evidence, dependency/license approval,
          credential/effect policy, materialization boundaries, execution certificates, Native I/O
          certificates, benchmark evidence, and no-fallback evidence.
    - [x] Keep existing `TableIntelligenceReport`, schema/partition/delete/table compatibility,
          CDC/layout/compaction planning, and `CatalogRef` skeleton evidence visible while keeping
          snapshot/manifest metadata reads, catalog table resolution, table metadata reads,
          dependency admission, commit/recovery metadata binding, and metadata cache invalidation
          runtime blocked.
    - [x] Expose the gate through `cg9-catalog-metadata-gate` without catalog IO, table metadata IO,
          object-store IO, data reads, writes, credential resolution, table-format dependency
          activation, metadata-cache runtime, claim publication, or fallback execution.
  - [x] CG-10 object-store/distributed runtime execution.
    - [x] Add `ObjectStoreRuntimePromotionGateReport` so object-store byte-range reads,
          request-coalescing runtime, coordinator/worker startup, distributed task execution,
          checkpoint writes, retry execution, cleanup execution, object-store commit execution,
          credential runtime, and benchmark/certificate closeout remain blocked until provider,
          request-budget, scheduler, reliability, atomicity, credential/effect, benchmark,
          execution-certificate, Native I/O, and no-fallback evidence exists.
    - [x] Keep existing object-store request planner, range planning, request coalescing,
          distributed scheduling, checkpoint/retry, and commit protocol evidence visible as
          report-only evidence without promoting object-store IO or distributed runtime behavior.
    - [x] Expose the gate through `cg10-object-store-runtime-gate` without byte-range reads,
          full-file reads, object-store IO, data reads, writes, coordinator/worker startup,
          task execution, checkpoint/retry/cleanup/commit execution, credential resolution, runtime
          claims, or fallback execution.
  - [x] CG-20 SQL/DataFrame/UDF/unstructured/media/adapters once the encoded primitive evidence loop
        and importability lane are no longer the bottleneck.
    - [x] Add `UserCapabilityPromotionGateReport` so broad SQL frontend, DataFrame query-builder,
          notebook, UDF/plugin, unstructured/media, universal adapter, event/API adapter,
          adapter read/write/commit, semantic-profile conformance, workload-certified closeout, and
          best-default dossier publication surfaces are named and kept blocked behind world-class
          sufficiency, semantic-profile, coverage, adapter-certification, correctness, benchmark,
          execution-certificate, Native I/O, workload-constitution, materialization, effect-policy,
          governance, protocol-parity, and no-fallback evidence.
    - [x] Preserve existing report-only world-class sufficiency, Python wrapper, input adapter
          registry, and unstructured workflow boundary contracts as narrow evidence without
          promoting SQL/DataFrame/UDF/media/adapter runtime behavior.
    - [x] Expose the gate through `cg20-user-capability-gate` without SQL parsing/execution,
          DataFrame runtime, notebook runtime, UDF/plugin execution, OCR/transcription/embedding/LLM
          calls, adapter runtime, external API calls, catalog probes, object-store IO, writes,
          claim publication, external engine invocation, or fallback execution.
  - [ ] CG-20 approximate aggregate/sketch function implementation after function-registry,
        aggregate-state, sketch-serialization, correctness, benchmark, execution-certificate, and
        Native I/O evidence gates are ready.
    - [ ] Support `approx_count_distinct`, grouped approximate distinct, partial sketch merge,
          sketch serialization/deserialization, stable hash/seed policy, error bounds, confidence,
          null/string/temporal/dictionary/nested-ish value handling, and exact-reference comparison.
    - [ ] Add encoded-aware sketch strategies for dictionary values plus row/filter evidence,
          run-length updates once per run value, sparse selection-vector-aware updates, and explicit
          materialization boundaries when an encoding-aware path is unavailable.
    - [ ] Keep DataFusion, Polars, Spark, DuckDB, Dask, pandas, and generic sketch libraries as
          comparison/reference inputs only unless a dependency/RFC approval explicitly permits a
          native implementation dependency.
- [ ] Priority 3.5 - cross-RFC platform hardening and release-readiness lane
  - [ ] RFC 0014 / CG-14 memory, spill, and OOM-safe execution
    - [ ] Turn resource-derived chunk sizing, parallelism, memory reservation, pressure detection,
          spill policy, and fail-before-OOM diagnostics into runtime behavior after primitive
          execution is stable.
      - [x] Add runtime-facing memory reservation admission that grants requests under the hard
            budget and denies over-budget requests before process OOM with pressure
            before/after, reservation evidence, diagnostics, and `fallback_attempted=false`.
    - [ ] Require operator-level memory/spill declarations for joins, aggregations, sorts, windows,
          repartition, shuffle, UDFs, sinks, and external-effect boundaries before large-workload
          claims.
      - [x] Add an operator memory/spill declaration gate report covering required large-workload
            operator classes, bounded-memory/spill/effect-boundary requirements, claim blockers,
            diagnostics, no runtime execution, no spill IO, and `fallback_attempted=false`.
  - [x] RFC 0017 fault tolerance, cancellation, recovery, and idempotency
    - [x] Promote retry, cancellation, cleanup, ambiguous commit, idempotency, and recovery plans
          into execution paths only after side-effect boundaries and commit semantics are certified.
    - [x] Keep exactly-once, resumability, and recovery claims blocked until CG-4, CG-8, CG-10,
          CG-16, and CG-22 evidence exists for the declared workload.
  - [x] RFC 0018 observability, tracing, profiling, and debug bundles
    - [x] Add trace/event/profile/log schema coverage for plan, execution, Vortex IO, object-store
          IO, memory/spill, translation/output, benchmark, certificate, and unsupported diagnostics.
      - [x] Add an observability schema coverage report for trace spans, structured events,
            profiles, logs, and debug bundles across plan, execution, Vortex IO, object-store IO,
            memory/spill, translation/output, benchmark, certificate, and unsupported diagnostics,
            with local JSON, redaction, certificate-link, no-exporter, no-runtime-collection, and
            `fallback_attempted=false` evidence.
    - [x] Keep OpenTelemetry/exporter integration optional and later; first make local
          CLI/Python/JSON reports complete, redacted, and certificate-linked.
  - [x] RFC 0019 security, governance, credentials, egress, and agent safety
    - [x] Add credential-reference, permission, redaction, audit, external-effect,
          destructive-operation, data-egress, and agent-policy evidence before
          object-store/API/LLM/media/UDF/server claims.
    - [x] Default effectful features to denied or dry-run unless an explicit policy authorizes
          execution.
  - [x] RFC 0024 release engineering, API compatibility, and packaging discipline
    - [x] Add schema-version, API-stability, dependency/license, SBOM/provenance, reproducible
          build, release-note, benchmark-accountability, and no-fallback release checks before
          public package/release claims.
    - [x] Keep container/server/package publication distinct from local development support and
          optional benchmark extras.
  - [x] CG-15 CPU operator specialization
    - [x] Add SIMD/cache-aware and encoding-aware operator specialization only after correctness
          fixtures and benchmark evidence can prove the specialization is safe and useful.
  - [ ] CG-17 stateful reuse and incremental execution
    - [ ] Implement stable reuse keys, invalidation, manifest-diff inputs, cache safety, state
          certificates, and reuse benchmarks before publishing reuse or incremental performance
          claims.
  - [ ] CG-18 universal import/deployment/baseline harness
    - [ ] Mature import/deployment/baseline harnesses for local, CI, container, optional Foundry,
          and optional benchmark environments without turning external engines into runtime
          dependencies.
- [ ] Priority 3.6 - RFC coverage follow-through before broader user/runtime expansion
  - [ ] RFC 0010 developer and agent usability
    - [ ] Keep every new CLI, Python, future REST, capability, diagnostic, benchmark, and
          certificate surface deterministic, machine-readable, human-readable, and
          side-effect-explicit.
    - [ ] Preserve import/discovery/dry-run safety for agent-facing workflows before execution/write
          permissions are exposed.
  - [ ] RFC 0011 modular extensibility
    - [ ] Treat SQL, UDFs, unstructured/media, LLM/API calls, embeddings, vector operations, and
          external effects as explicit ShardLoom-native extension surfaces with
          typed/effect/materialization metadata.
    - [ ] Keep effectful or Python/external extension execution blocked until sandboxing,
          governance, correctness, and certificate evidence exists.
  - [ ] RFC 0020 schema evolution, catalog integration, and table compatibility
    - [ ] Promote table/catalog metadata integration only after the existing CG-9 compatibility
          reports can attach real snapshot/schema/partition/delete/catalog evidence without unsafe
          coercion.
    - [ ] Keep metadata discovery separate from read/write/commit certification and block
          update/delete/merge claims until table semantics and recovery evidence exist.
  - [ ] RFC 0022 native-first plan IR and Substrait-compatible interoperability
    - [ ] Expand native plan import/export and capability-gate evidence before imported plans can
          execute.
    - [ ] Keep Substrait-like import/export optional, dependency-free until approved, and never a
          fallback bridge to another execution engine.
  - [ ] RFC 0023 extension/plugin ABI and sandboxing
    - [ ] Add manifest, lifecycle, permission, provenance, signing, sandbox, resource-limit, and
          agent-inspection evidence before plugin or UDF execution.
    - [ ] Inspect extension manifests without executing extension code and keep unsafe extension
          behavior deterministically unsupported.
- [x] Priority 3.7 - evidence, policy, workload, and protocol hardening
  - [x] Promote `EvidenceArtifactEnvelope` into report/code surfaces as the shared
        identity/provenance/digest/redaction/retention wrapper for certificates, benchmark rows,
        scorecards, profiles, lineage events, and future API artifacts.
  - [x] Promote `EvidenceArtifactSafety` into report/code surfaces for classification,
        value/path/query/schema exposure, credential absence, redaction, retention, export, and
        agent visibility.
  - [x] Promote `ShardLoomExecutionPolicy` into CLI, Python, future REST, and agent surfaces so they
        share requested/allowed engine, fallback, materialization, decode, result, evidence, effect,
        credential, redaction, retention, memory, spill, network, destructive-operation, benchmark,
        and agent policy fields.
  - [x] Promote `QueryLifecycleContract` into lifecycle report surfaces for accepted, validating,
        planned, blocked, queued, running, cancelling, cancelled, failed, succeeded, and expired
        states.
  - [x] Promote `ProtocolSurfaceParityReport` across CLI JSON, Python wrapper, future REST/OpenAPI,
        future MCP resources, and future Flight/ADBC metadata.
  - [x] Add machine-readable starter workload constitution catalog entries for local Vortex
        primitives, local file ETL, Conda import smoke, Python DataFrame local ETL, REST
        discovery-only, batch Vortex analytics, hybrid base/delta fixture, local Vortex read/write
        adapter, and traditional analytics benchmark workloads.
  - [x] Add the concrete `ShardLoomNative` semantic-profile floor as a reportable/table-backed
        contract before serious SQL/DataFrame execution.
  - [x] Promote `StandardsDependencyDecision`, `BenchmarkConstitution`, `CostSimulationReport`, and
        `RustPerformanceProfileEvidence` into report/code surfaces.
  - [x] Make benchmark constitutions explicit about workload constitution, engine mode, input
        format, native Vortex versus compatibility import, startup/conversion/result/API transport
        inclusion, cache/object-store policy, warmup/iterations, correctness oracle, resource
        limits, cost assumptions, and claim level.
  - [x] Track standards and ecosystem posture for OpenAPI, AsyncAPI, CloudEvents,
        OpenTelemetry/OTLP, OpenLineage, Arrow Flight/ADBC, Arrow C Stream/PyCapsule, Iceberg
        REST/Polaris/Gravitino, Delta Sharing, Substrait, WASI/WebAssembly components, MCP, and
        event substrates as reference-only, schema-only, optional-feature, approved-dependency, or
        rejected.
  - [x] Add a Markdown physical-formatting cleanup pass for phase-plan, RFC, skill, and architecture
        docs so future diffs, citations, search, and agent review use normal line structure without
        content churn.
  - [x] Keep this lane docs/report-only: no runtime, parser, adapter, server, package publication,
        benchmark execution, external engine invocation, dependency, or fallback execution.
- [x] Priority 3.8 - CG-20/CG-23 client and wrapper surface architecture
  - [x] Add RFC 0037 as the formal traceability home for one canonical protocol, many thin wrappers,
        wrapper maturity, transports, SDK registries, ecosystem wrappers, golden contract fixtures,
        and no-fallback wrapper invariants.
  - [x] Define one canonical client/wrapper architecture: protocol schemas, transport adapters,
        client core, language SDKs, and ecosystem wrappers.
  - [x] Add a wrapper maturity ladder: `W0` declared only, `W1` package/import smoke, `W2`
        side-effect-free capability discovery, `W3` typed envelope parsing, `W4`
        plan/explain/validate support, `W5` execute certified local paths, `W6` result delivery and
        certificate access, and `W7` workload-certified integration.
  - [x] Promote shared protocol schema artifacts for `OutputEnvelope`, `CapabilitySnapshot`,
        `ExecutionCertificate`, `NativeIoCertificate`, `EvidenceArtifactEnvelope`,
        `ShardLoomExecutionPolicy`, `ResultRef`, problem-details/unsupported diagnostics,
        `EngineSelectionReport`, `MaterializationBoundaryReport`, `AdapterFidelityReport`, and
        `BenchmarkClaimEvidenceReport`.
  - [x] Add transport abstractions for CLI subprocess, future REST HTTP, future Flight/ADBC data
        plane, mock transport, and recording/replay transport so wrappers do not parse ad hoc
        command output.
  - [x] Define client-core operations for status, capabilities, adapter discovery, plan validation,
        explain, execute, query status, cancel, results, certificates, profile, benchmark,
        migration, and diagnostics independent of transport.
  - [x] Track a language SDK registry for the current Python client plus planned Rust,
        TypeScript/JavaScript, Go, Java/JVM, .NET, R, and future generated OpenAPI clients.
  - [x] Track a Python ecosystem wrapper registry for Python DB-API, SQLAlchemy dialect, Ibis
        backend, pandas/Arrow helpers, and notebook display surfaces, all preserving explicit
        materialization and no-fallback evidence.
  - [x] Track an orchestration and workflow wrapper registry for dbt, Airflow, Dagster, Prefect, and
        CI/report-viewer integrations, with explain/validate/certificate-first behavior before
        execution/write support.
  - [x] Track remote/data-plane wrapper posture for ADBC, Flight SQL, JDBC via Arrow Flight SQL,
        ODBC later only if needed, Superset/BI readiness through SQLAlchemy, and Grafana/data-source
        plugin posture after SQL/API maturity.
  - [x] Track safe agent-wrapper posture for MCP resources and tools: read-only resources and
        dry-run/explain/estimate/certify tools by default; execute/write/cancel require explicit
        policy and credentials.
  - [x] Add `WrapperCapabilityReport` and `ProtocolSurfaceParityReport` coverage so every wrapper
        reports wrapper version, protocol version, maturity level, supported transports, exposed
        fields, unavailable fields, materialization behavior, certificate access, and
        `fallback_attempted`.
  - [x] Add golden contract fixtures for envelopes, errors, capabilities, result refs,
        materialization reports, and certificates before any wrapper can move beyond discovery
        maturity.
  - [x] Preserve wrapper invariants: package import has no execution side effects, client
        construction does not probe datasets, capability discovery is side-effect-free, unsupported
        behavior is structured, large results use artifact refs/Arrow/Flight/Vortex outputs instead
        of giant JSON, and no wrapper may execute unsupported work through external engines.
- [ ] Priority 4 - CG-21 user data workflow and ETL surface implementation lane
  - [ ] CG-21A install/import/runtime discovery
    - [ ] Provide a one-command local install path once packaging approval is complete.
    - [ ] Keep `import shardloom` side-effect-free with no dataset, filesystem, network, catalog,
          adapter, SQL, benchmark, or execution probing.
    - [ ] Report Python package version, CLI binary version, protocol version, feature gates,
          platform, and `fallback_attempted=false`.
    - [ ] Make missing-binary, version-mismatch, disabled-feature, and unsupported-runtime
          diagnostics deterministic and actionable.
    - [ ] Certify fresh venv/Conda environment smoke checks before public package claims.
  - [ ] CG-21B context and capability API
    - [ ] Add side-effect-free Python context constructors for local and future remote use.
    - [ ] Expose `capabilities`, `adapters`, `functions`, `operators`, `sql_support`, `deployment`,
          and `certification` through stable machine-readable structures.
    - [ ] Keep planned, partial, unsupported, feature-gated, effect-gated, materialization-gated,
          and certified states distinct.
    - [ ] Include rewrite suggestions, required gates, materialization boundaries, and no-fallback
          fields in unsupported responses.
  - [ ] CG-21C source/sink registry and adapter maturity
    - [ ] Expose source and sink registries without probing datasets by default.
    - [ ] Track adapter maturity A0-A7 separately for discovery, schema/metadata, read, pushdown,
          write, commit/recovery, and benchmark certification.
    - [ ] Separate Vortex-native paths from compatibility import/export paths.
    - [ ] Require native I/O certificate requirements, pushdown proof, fidelity loss, metadata loss,
          and materialization risk per adapter path.
  - [ ] CG-21D Python DataFrame/query-builder
    - [ ] Build lazy plan objects for `read`, `filter`, `select`, `with_column`, `group_by`, `agg`,
          `join`, `sort`, `limit`, and write operations.
    - [ ] Lower all actions through ShardLoom-native capability checks before execution.
    - [ ] Keep pandas, Polars, Spark, DataFusion, DuckDB, and other engines out of runtime fallback
          paths.
    - [ ] Provide `explain`, `estimate`, `profile`, `certify`, `collect`, `to_pandas`, `to_arrow`,
          `write_vortex`, and compatibility write boundaries with explicit materialization reports.
  - [ ] CG-21E SQL frontend workflow
    - [ ] Stage parse, bind, validation, native logical planning, native physical planning, native
          execution, encoded-capable execution, and workload certification separately.
    - [ ] Reject unsupported constructs with stable diagnostics rather than delegating to external
          engines.
    - [ ] Tie SQL behavior to semantic profiles, catalog/schema availability, function/operator
          capability, and native I/O evidence.
  - [ ] CG-21F pandas, Arrow, and NumPy interop boundaries
    - [ ] Implement `from_pandas`, `to_pandas`, `from_arrow`, `to_arrow`, and NumPy-style boundary
          helpers only as explicit materialization/source/sink boundaries.
    - [ ] Emit materialization, fidelity, representation-state, and no-fallback evidence for every
          conversion.
    - [ ] Never use pandas, Arrow, or NumPy to execute unsupported ShardLoom plan fragments
          silently.
  - [ ] CG-21G data contracts and data quality
    - [ ] Add required-column, required-type, nullability, uniqueness, ordering, freshness,
          duplicate-key, parse-failure, and constraint-violation contracts.
    - [ ] Support count/reject/quarantine policies with explicit rejected-row output contracts
          before claiming data-quality support.
    - [ ] Include data-quality summaries, diagnostics, and certificate refs in workflow output.
  - [ ] CG-21H local structured adapters
    - [ ] Mature Vortex read/write first as highest-fidelity source and sink.
    - [ ] Promote CSV, JSON/NDJSON, Parquet, Arrow IPC, Avro, ORC, compressed wrappers, and
          partitioned directories only through approved dependency/reader boundaries.
    - [ ] Keep compatibility ingestion honest about decode/materialization and Vortex conversion.
    - [ ] Add independent read, write, pushdown, commit, correctness, benchmark, and native I/O
          evidence per format.
  - [ ] CG-21I output and commit UX
    - [ ] Surface sink requirements before execution for Vortex, Parquet, CSV, JSON/NDJSON, Arrow
          IPC, partitioned outputs, append, overwrite, merge, upsert, delete, copy, and export.
    - [ ] Report temporary path policy, atomicity, idempotency, rollback cleanup, partition layout,
          side effects, metadata/statistics preservation, and materialization requirements.
    - [ ] Keep unsafe writes policy-gated and aligned with CG-3/CG-4/CG-19 evidence.
  - [ ] CG-21J object-store and remote data UX
    - [ ] Expose S3-compatible, GCS, Azure Blob/ADLS, HTTP range-read, credential, range-planning,
          coalescing, prefetch, retry, idempotency, and request-budget evidence before remote IO
          claims.
    - [ ] Keep object-store paths distinct from distributed execution support.
    - [ ] Report bytes requested/read, estimated requests, credential boundaries, network effects,
          and fallback status.
  - [ ] CG-21K table/catalog/lakehouse UX
    - [ ] Expose Hive-style partitions, Iceberg-compatible metadata, Delta-compatible metadata,
          snapshots, schema evolution, delete/tombstone handling, manifests, partition pruning,
          layout health, and compaction planning as separate maturity surfaces.
    - [ ] Keep metadata discovery separate from table read/write/commit certification.
    - [ ] Block update/delete/merge claims until table semantics, commit/recovery, correctness, and
          no-fallback evidence exist.
  - [ ] CG-21S relational, warehouse, and snapshot/export UX
    - [ ] Track PostgreSQL, MySQL/MariaDB, SQLite, Snowflake-like exports, BigQuery-like exports,
          JDBC/ODBC bridges, and warehouse/table snapshots as metadata, snapshot/export, oracle,
          migration, or source-pushdown surfaces before runtime support.
    - [ ] Report remote-system pushdown as source behavior with residual boundaries, not as
          ShardLoom-native execution unless ShardLoom executes the residual natively and
          certificates prove it.
    - [ ] Prohibit remote SQL engines from executing unsupported ShardLoom plan fragments as
          fallback.
  - [ ] CG-21T logs, events, and bounded event ETL UX
    - [ ] Model line logs, JSON logs, application events, event ids, timestamp parsing,
          sessionization, deduplication, watermarks, late data, invalid-line quarantine, and bounded
          micro-batch behavior as explicit ETL workflow surfaces.
    - [ ] Distinguish finite log/event file processing, bounded micro-batches, and true live streams
          with state/checkpoint/watermark evidence.
    - [ ] Keep broker/runtime substrates as adapter/reference candidates, not core fallback
          dependencies.
  - [ ] CG-21L observability UX
    - [ ] Make `explain`, `estimate`, `profile`, and `certify` available from CLI, Python, and later
          REST for supported plans.
    - [ ] Report planned versus executed work, work avoided, rows/bytes scanned, segments pruned,
          bytes decoded, rows materialized, selection-vector use, object-store requests,
          memory/spill, representation state, and fallback status.
  - [ ] CG-21M benchmark and migration UX
    - [ ] Keep Spark, DataFusion, Polars, DuckDB, Dask, and pandas optional benchmark/correctness
          baselines only.
    - [ ] Add migration reports with supported/unsupported constructs, semantic/function/adapter
          differences, rewrite suggestions, materialization requirements, Vortex conversion payback,
          and no-fallback status.
    - [ ] Require benchmark rows to carry correctness, materialization, native I/O,
          execution-certificate, and reproducibility evidence before claims.
  - [ ] CG-21N notebook UX
    - [ ] Add rich display for schemas, plan trees, capability states, unsupported reasons, sample
          previews, certificate summaries, benchmark tables, and materialization/fidelity warnings.
    - [ ] Treat previews as explicit materialization with row limits and redaction policy.
  - [ ] CG-21O UDF and extension UX
    - [ ] Support typed Rust-native UDF metadata before broader WASM/Python/external-service UDF
          execution.
    - [ ] Require type, null behavior, determinism, volatility, effect level, resource limits,
          sandbox policy, materialization requirement, failure behavior, timeout, retry, redaction,
          license/provenance, and no-fallback fields.
    - [ ] Treat Python/external/LLM/API/model-call UDFs as explicit effect/materialization
          boundaries until certified native paths exist.
  - [ ] CG-21P unstructured/media UX
    - [ ] Model documents, logs, HTML/XML, PDFs, office documents, images, audio, video, archives,
          binary blobs, extracted text, chunks, metadata, embeddings, and manifests as typed
          references and explicit effect stages.
    - [ ] Prohibit silent OCR, media decode, embedding generation, LLM calls, or API calls.
    - [ ] Report extractor provenance, confidence, redaction, credential boundaries, cost,
          materialization, and no-fallback evidence.
    - [x] CG-21P.1 unstructured/media/model boundary contracts
      - [x] Add `MediaRef`, `MediaManifest`, `TextChunkTable`, and `EmbeddingTable` contracts so
            unstructured and model-derived outputs become structured artifacts without making
            ShardLoom the media/model runtime.
      - [x] Add `ExtractionBoundaryReport`, `ModelCallBoundaryReport`, `EmbeddingBoundaryReport`,
            and `UnstructuredWorkflowCertificate` so OCR, transcription, embedding, LLM/model
            inference, chunking, redaction, cost, provenance, and downstream analytics are explicit
            evidence boundaries.
      - [x] Add Foundry-specific media/model boundary posture for `FoundryMediaSetSource`,
            `FoundryMediaSetSink`, `FoundryVirtualMediaSetSource`,
            `FoundryMediaExtractionBoundaryReport`, `FoundryModelCallBoundaryReport`,
            `FoundryEmbeddingBoundaryReport`, and `FoundryAipLogicBoundaryReport`.
      - [x] Keep pipeline code, Foundry media transforms, AIP Logic, or governed model services
            responsible for OCR, transcription, media conversion, embedding generation, LLM calls,
            model inference, prompt handling, retries, rate limits, human review, and Ontology
            edits.
      - [x] Treat embedding/vector tables as structured outputs; vector similarity scan, ANN/top-K,
            vector indexes, and native vector execution remain separately certified
            extension-type/vector capability work.
  - [ ] CG-21Q security/governance UX
    - [ ] Add credential boundary reporting, secret redaction, audit events, external read/write
          permission, destructive operation policy, data classification, PII redaction,
          data-retention policy, and safe agent-facing API behavior.
    - [ ] Block governed-workload certification when required governance evidence is missing.
  - [ ] CG-21R workload scorecards
    - [ ] Publish workload-scoped scorecards for correctness, performance, cost, memory safety,
          SQL/function/operator/adapter coverage, Python usability, observability, migration,
          deployment, governance, extension safety, and no-fallback integrity.
    - [ ] Allow scorecards to report `not_certified` or `partial_for_workload` without implying
          broad support.
- [ ] Priority 5 - CG-22 three-engine certified data execution fabric
  - [ ] CG-22A engine-mode contract surface
    - [ ] Add `EngineMode` values `batch`, `live`, `hybrid`, and `auto`.
    - [ ] Add `Boundedness`, `UpdateMode`, and `OutputMode` vocabulary and capability discovery.
    - [ ] Emit `EngineSelectionReport` with requested, allowed, and selected engine modes plus
          rejection reasons.
    - [ ] Preserve `external_engine_invoked=false` and `fallback_attempted=false` for every internal
          engine choice.
  - [ ] CG-22B per-engine capability matrix
    - [ ] Track operator/function/source/sink support separately for batch, live, and hybrid.
    - [ ] Distinguish bounded snapshot support, append-only stream support, upsert/delete/tombstone
          support, changelog support, and continuous materialized view support.
    - [ ] Block live/hybrid claims for unsupported global sort, unbounded join, state, checkpoint,
          or output modes.
  - [ ] CG-22C live source/change contract
    - [ ] Define ShardLoom-native `ChangeRecord` with key, operation, sequence, event time,
          processing time, source offset, schema digest, and payload reference.
    - [ ] Add append/upsert/delete/retract/tombstone semantics, watermark policy, late-data policy,
          state TTL, checkpoint policy, and output changelog vocabulary.
    - [ ] Keep broker/runtime integrations as adapters or future dependencies, not core fallback
          execution.
  - [ ] CG-22D narrow in-memory live prototype
    - [ ] Start with fixture-backed bounded streams for filter, project, count, count_where, and
          simple group_count.
    - [ ] Emit state, checkpoint, watermark, lag, output changelog, execution certificate, Native
          I/O certificate, and no-fallback evidence.
    - [ ] Avoid calling file polling real streaming until state, watermark, checkpoint, and recovery
          semantics exist.
  - [ ] CG-22E hybrid base plus delta overlay
    - [ ] Combine a local Vortex base with fixture-backed hot deltas, tombstones, deletion vectors,
          snapshot epoch, and certified merged result.
    - [ ] Emit `DeltaOverlayCertificate`, `HotColdContributionReport`, snapshot certificate refs,
          base snapshot id, hot changelog range, warm/cold segment counts, tombstone counts,
          freshness lag, and no-fallback evidence.
    - [ ] Keep object-store/table production claims blocked until CG-4/CG-9/CG-10 evidence exists.
  - [ ] CG-22F Vortex micro-segment flush
    - [ ] Flush hot append/upsert batches into Vortex micro-segments with local manifest and
          recovery evidence.
    - [ ] Preserve representation, statistics, deletion/tombstone, checkpoint, commit, and Native
          I/O certificate fields.
    - [ ] Keep compaction and table maintenance separate until commit/recovery paths are certified.
  - [ ] CG-22G compaction and layout-health planner
    - [ ] Plan compaction from small-segment pressure, tombstone pressure, partition skew, stale
          statistics, and layout health.
    - [ ] Produce compaction recommendations without executing maintenance until
          write/commit/recovery support is ready.
  - [ ] CG-22H Python/API engine UX
    - [ ] Surface `engine="batch"`, `engine="live"`, `engine="hybrid"`, and `engine="auto"`
          consistently in Python, CLI, and later REST.
    - [ ] Explain why an engine is selected or rejected, including freshness, consistency,
          boundedness, state, memory, sink, and unsupported-feature reasons.
  - [ ] CG-22I state, checkpoint, and freshness certification
    - [ ] Add `FreshnessCertificate`, `StateCertificate`, and `ContinuousViewCertificate` fields for
          watermarks, checkpoint ids, state bytes, changelog offsets, recovery status, lag, output
          mode, and exactly-once/idempotency blockers.
    - [ ] Do not claim exactly-once, freshness, recovery, or continuous-view correctness without
          CG-4/CG-5/CG-8/CG-16 evidence.
- [ ] Priority 6 - CG-23 REST, event, and remote API surface
  - [ ] CG-23A REST API contract surface
    - [ ] Define OpenAPI 3.2 or approved-successor contract files for `/v1` resources before server
          behavior.
    - [ ] Represent health, version, capabilities, adapters, sources, sinks, plans, queries,
          results, certificates, profiles, benchmarks, migration, lineage, and governance resources.
    - [ ] Require engine mode, fallback policy, materialization policy, result policy, and evidence
          policy on execution-capable requests.
    - [ ] Include `fallback_attempted=false` or explicit unsupported/failure reason in every
          response.
    - [ ] Add an API maturity ladder from declared contract through discovery, plan/explain, local
          certified batch lifecycle, result delivery, source/sink certification, live/hybrid events,
          security/governance, and production-certified workload support.
  - [ ] CG-23B REST discovery server
    - [ ] Add optional local `shardloom serve --mode discovery` only after dependency and security
          approval.
    - [ ] Serve health, version, capabilities, adapters, deployment readiness, and no-dataset smoke
          checks.
    - [ ] Prohibit dataset probing, object-store access, catalog access, query execution, and
          fallback in discovery mode.
  - [ ] CG-23C plan/explain/validate API
    - [ ] Add plan handles and validate/explain/estimate/certification-preview endpoints.
    - [ ] Return parser/binder/native logical/native physical/execution/certification stages
          separately.
    - [ ] Use deterministic unsupported diagnostics and problem-details errors without execution
          delegation.
  - [ ] CG-23D async query lifecycle API
    - [ ] Add execute/status/cancel/retry/profile/certificates/lineage/results/artifacts lifecycle
          for already-certified local batch paths first.
    - [ ] Keep non-certified paths blocked or explicitly uncertified.
    - [ ] Link result handles to execution certificates, Native I/O certificates, materialization
          reports, profile reports, and no-fallback evidence.
  - [ ] CG-23E result delivery and spooling
    - [ ] Support inline JSON only for tiny previews and diagnostics.
    - [ ] Add paged JSON and JSON Lines for row-oriented small/medium result or log streams.
    - [ ] Treat Arrow IPC as explicit decoded-columnar boundary unless a future native boundary
          proves otherwise.
    - [ ] Prefer Vortex artifacts or object references for highest-fidelity large analytical
          outputs.
    - [ ] Define result TTL, retention, cleanup, artifact refs, representation state,
          materialization, and fidelity fields.
  - [ ] CG-23F live/hybrid event API
    - [ ] Use SSE for one-way progress events and WebSocket only where bidirectional live
          interaction is required.
    - [ ] Define AsyncAPI event contracts and CloudEvents-style envelopes for progress, state,
          checkpoint, watermark, certificate, lineage, benchmark, and hybrid hot/cold contribution
          events.
    - [ ] Block live/hybrid API certification until CG-22, CG-8, CG-4, and CG-16 evidence exists.
  - [ ] CG-23G security, governance, and API policy
    - [ ] Define local-only, token, mTLS, OIDC, and service-account auth modes before remote
          execution.
    - [ ] Separate scopes for read, plan, execute, write, cancel, admin, benchmark, migration, and
          agent operations.
    - [ ] Keep credentials as references, redact secrets, require explicit destructive-operation
          policies, and audit plan/execute/write/cancel/certify.
  - [ ] CG-23H Flight/ADBC and columnar data-plane bridge
    - [ ] Keep REST as control plane while allowing future Flight tickets or ADBC endpoints for
          high-throughput columnar transfer.
    - [ ] Make Flight/ADBC optional and never required for basic local use or import.
    - [ ] Report Arrow transfers as decoded-columnar materialization unless later certified
          otherwise.
  - [ ] CG-23I MCP agent API
    - [ ] Expose capabilities, schemas, plans, certificates, benchmark reports, and diagnostics as
          safe agent resources.
    - [ ] Keep MCP tools dry-run/explain/estimate/certify by default.
    - [ ] Require explicit policy and credentials for execute, write, cancel, benchmark, and
          migration operations.
    - [ ] Preserve no-fallback and external-effect diagnostics in agent-facing responses.
  - [ ] CG-23J remote standards, lineage, and ecosystem interop posture
    - [ ] Map OpenTelemetry traces/metrics/logs, OpenLineage runs/jobs/datasets/facets,
          problem-details errors, CloudEvents, and certificate refs into a single API evidence
          model.
    - [ ] Treat Iceberg REST Catalog, Polaris, Gravitino, Delta Sharing, Substrait, WASI/WebAssembly
          components, NATS JetStream, Redpanda, Kafka-compatible systems, Paimon, Fluss, and similar
          systems as standards/reference/adapter candidates until a dependency decision and
          capability contract approve runtime use.
    - [ ] Keep REST as control plane and proof surface; large payload transfer must use explicit
          result policies such as Vortex artifact refs, object refs, Arrow IPC boundaries, JSON
          Lines, or future Flight/ADBC tickets.
    - [ ] Do not let remote API support weaken local Python/CLI protocol parity, no-fallback
          execution, materialization reporting, or governance policy.
- [ ] Priority 7 - CG-21/CG-22/CG-23 integrated certification closeout
  - [ ] Add cross-CG capability snapshots proving CG-21 workflow, CG-22 engine mode, and CG-23
        remote API states are visible through capability discovery.
  - [ ] Add cross-CG unsupported diagnostics showing the same blocker across CLI, Python, and future
        REST.
  - [ ] Add workload-scoped certification dossiers that combine CG-5 correctness, CG-6 benchmarks,
        CG-16 execution certificates, CG-19 Native I/O certificates, CG-20 capability evidence,
        CG-21 workflow evidence, CG-22 engine evidence, and CG-23 API evidence.
  - [ ] Keep CG-21, CG-22, and CG-23 logically after the current planned CG-1 through CG-20 work
        unless a later implementation item is explicitly pulled forward as a contract/report-only
        lane.
  - [ ] Preserve no-runtime, no-dependency, no-fallback, and no-claim posture for docs/report-only
        synthesis.
- [ ] Priority 8 - general availability and external proof-of-use
  - [ ] Define public release identity and versioning policy for PyPI `shardloom`, conda-forge
        `shardloom-cli`, `shardloom-python`, `shardloom` metapackage, GitHub Release artifacts,
        GHCR/OCI image posture, and selected crates.io protocol/client crates.
  - [ ] Add release workflow contracts for Git tag, source archive, platform binaries, Python
        wheel/sdist, Conda recipe/feedstock status, checksums, SBOM, artifact attestation,
        changelog, compatibility matrix, known unsupported paths, and no-fallback release checks.
  - [ ] Prefer PyPI trusted publishing/OIDC and prohibit long-lived package tokens in release
        automation unless explicitly approved by maintainers.
  - [ ] Keep package publication, release tags, feedstock submission, crates.io publication, OCI
        pushes, and Marketplace publication human-approved and release-gated.
  - [ ] Make Conda the primary "it just works" path by proving clean-environment installs for
        `shardloom-cli`, `shardloom-python`, and `shardloom` metapackage with CLI binary resolution
        and `fallback_attempted=false` smoke evidence.
  - [ ] Add the public first-10-minutes proof: `conda install shardloom`, `import shardloom`,
        `ShardLoomClient.from_env().smoke_check()`, `client.capabilities()`, `shardloom status
        --format json`, and `shardloom capabilities --format json`.
  - [ ] Add external proof examples with README, environment file, input fixture, expected output,
        expected certificate fields, and known limitations for `examples/local-python-smoke/`,
        `examples/local-vortex-benchmark/`, and `examples/foundry-lightweight-transform/`.
  - [ ] Publish user-facing docs for install, quickstart, Python client, CLI, Conda packages,
        Foundry usage, benchmarking, certificates, no-fallback policy, Vortex compatibility,
        maturity statuses, and unsupported diagnostics.
  - [ ] Keep benchmark extras, Spark/DataFusion/DuckDB/Polars/pandas baselines, and optional
        comparison tooling out of the core install path and report them as baselines only.
- [ ] Priority 9 - RFC 0036 Foundry integration pack and platform availability
  - [ ] Treat Foundry as an optional integration pack, not the primary engine target and not a new
        core engine gate.
  - [ ] Add `shardloom-foundry` helper package posture for deterministic `SHARDLOOM_BIN` resolution,
        Foundry transform metadata capture, input/output RID capture, certificate output writing,
        benchmark metrics writing, staging/materialization reports, and no-fallback propagation
        without adding execution semantics.
  - [ ] Add the Foundry maturity ladder: `F0` declared only, `F1` package/import in Code Repository,
        `F2` smoke transform with CLI resolution, `F3` dataset source/sink staging with certificate
        output, `F4` Data Expectations/Data Health bridge, `F5` lineage and transaction/branch
        evidence, `F6` virtual-table/external-compute boundary awareness, `F7` Marketplace starter
        product, `F8` Compute Module/REST service, `F9` Ontology/AIP/Workshop integration, and `F10`
        workload-certified Foundry deployment.
  - [ ] Add `FoundryExecutionContext`, `FoundryDatasetTransactionReport`,
        `FoundryBranchContextReport`, `FoundryPreviewModeReport`, and
        `FoundryReleaseReadinessReport` surfaces so certificates identify transform, branch,
        preview/build/incremental mode, transactions, package versions, workload constitution, and
        expected evidence.
  - [ ] Add `FoundryDatasetSource`, `FoundryDatasetSink`, and `FoundryCertificateOutput` schema
        surfaces for staged local files, table-compatible outputs, certificate/metrics datasets,
        optional Vortex artifact sidecars, materialization/fidelity reports, commit/recovery status,
        and `fallback_attempted=false`.
  - [ ] Add `FoundryIncrementalRunReport` aligning Foundry incremental builds with ShardLoom
        batch/live/hybrid evidence without treating Foundry incremental mode as live/hybrid
        certification by itself.
  - [ ] Add `FoundryDataHealthBridge` and Data Expectations mapping for certificate presence,
        no-fallback status, Native I/O evidence, schema digest, output row requirements,
        data-quality checks, materialization policy, and benchmark-claim blockers.
  - [ ] Add `FoundryLineageFacet`, `FoundryScheduleBuildReport`, and
        `FoundryDataConnectionBoundaryReport` for datasets, virtual tables, media sets, artifacts,
        schedules, syncs, exports, webhooks, external transforms, credential refs, egress policy,
        and ShardLoom role classification.
  - [ ] Add `FoundryS3DatasetAdapter` posture for future S3-compatible dataset access with dataset
        RID, branch, object key, range-read support, multipart/write support where allowed,
        bytes/request counts, credential mode, and Native I/O certificates.
  - [ ] Add `FoundryVirtualTableSource`, `FoundryVirtualTableSink`, and `FoundryVirtualTableRef`
        surfaces so Snowflake, Databricks, BigQuery, S3, ADLS, GCS, Iceberg, and similar virtual
        tables are governed external handles with metadata, staging, update-detection, security, and
        materialization policy.
  - [ ] Classify Snowflake/Databricks/BigQuery/Foundry Spark/Snowpark/Databricks Connect/Ibis
        compute pushdown through `FoundryExternalComputeBoundaryReport` as baseline, oracle,
        migration reference, or prohibited fallback, never as ShardLoom-native execution.
  - [ ] Add `FoundryIcebergTableSource` and `FoundryIcebergTableSink` posture for catalog/table
        metadata, snapshot/manifest awareness, schema/partition evidence, compatibility reads,
        `TranslationReport` requirements, and commit/recovery evidence.
  - [ ] Add `FoundryMediaSetSource` and `FoundryMediaSetSink` posture for media item refs,
        MIME/schema, OCR/extraction/model/materialization boundaries, provenance/confidence,
        incremental media status, redaction, and explicit no silent
        OCR/transcription/embedding/model calls.
  - [ ] Add Foundry Ontology, Functions, AIP Logic, model, and scenario report-first surfaces:
        `FoundryOntologyMappingReport`, `FoundryFunctionSurface`, `FoundryAipLogicBridge`,
        `FoundryModelBoundaryReport`, and `FoundryScenarioBoundaryReport`.
  - [ ] Add BYOC and Compute Module posture through `FoundryByocImageReport`,
        `FoundryComputeModuleSurface`, and `FoundryComputeModuleReadinessReport`, keeping Compute
        Modules blocked until CG-23 API/security/package evidence exists.
  - [ ] Add `FoundryGovernanceBoundaryReport` for markings, organizations, inherited markings,
        certificate visibility, redaction, export policy, agent visibility, and artifact safety.
  - [ ] Add `FoundryMarketplaceStarterProduct` as an adoption artifact with Conda dependency
        instructions, smoke transform, benchmark transform, certificate output dataset, Data
        Expectations bridge, optional virtual-table staging example, optional external-compute
        baseline example, optional Compute Module API example, schedule, and docs.
  - [ ] Add Foundry benchmark schema and lanes that label ShardLoom lightweight, Polars lightweight,
        DataFusion/DuckDB baseline, Spark distributed, and Snowflake/Databricks/BigQuery pushdown
        rows separately with compute mode, materialization boundary, certificates, correctness
        digest, and versions.
  - [ ] Preserve the central Foundry rule: virtual tables and external compute are first-class
        workflow handles and comparison boundaries, but ShardLoom-native execution requires
        staged/native data plus certificates; no Snowflake/Databricks/BigQuery/Spark/Foundry compute
        pushdown may be reported as ShardLoom execution.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
