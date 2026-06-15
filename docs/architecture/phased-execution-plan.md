# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work is the first unchecked Planned
  checkbox after this file has been reordered.
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
- When a maintainer-provided list, audit, attachment, benchmark finding, or review packet proposes
  new work, review each candidate before adding it here. Classify it as already addressed,
  accepted into a new checklist, merged into an existing checklist, v1 candidate pending
  feasibility, deferred beyond the current product scope, or rejected with a reason. Do not paste
  broad lists verbatim into Planned.
- Production-shift items must state whether they are `required_for_v1`,
  `v1_candidate_pending_feasibility`, `deferred_after_v1`, `documentation_only_for_v1`, or
  `unsupported_boundary_for_v1`. The v1 default is inclusion for anything feasible to complete with
  real runtime behavior, deterministic unsupported diagnostics, safety evidence, and release proof.
  Defer beyond v1 only when the item records a concrete reason such as unavailable external
  platform proof, unresolved safety/security design, missing protocol approval, or scope that would
  make v1 unverifiable.
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
  `deferred_after_v1`, `documentation_only_for_v1`, or `unsupported_boundary_for_v1` for
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

### Finished Product v1 Required Work

These items are the reviewed intake from the attached production-shift list dated June 13, 2026.
They are ordered as runtime/product closure first, API/schema stability second, release/package
channels near the end, and public-language cleanup throughout. Already-addressed rows remain
referenced in the completed ledger or existing release gates. Broad platform/runtime ambitions
should be included in v1 when they can be made real, safe, and evidence-backed; defer them only
with a recorded infeasibility reason, not merely because they are broad.

- [ ] `PROD-V1-5A` Package-channel readiness and finished-product hard gate.
  - Source: attached production-shift review sections 16 and 17; RFC 0024;
    `docs/release/package-channel-readiness-matrix.json`;
    `scripts/check_release_readiness.py`; final release rehearsal and hard release gates.
  - Current state: package-channel matrices and hard release readiness exist, but public
    publication is blocked. v1 needs a final finished-product readiness aggregator that consumes
    runtime, schema, security, package, docs, benchmark, and approval evidence and fails closed
    without `--allow-blocked` for public release tags.
  - Intake review: accepted package/release channel order and finished-product gate; retain current
    package-channel evidence requirements and mark channels not included in v1 as
    `not_in_v1_scope` only after feasibility review records why they cannot be included.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: control-plane applicable. The finished-product gate must consume
    timing/evidence surface fields and require runtime support rows to have documented
    dynamic/capillary/PulseWeave decisions where applicable; package publication itself should not
    introduce runtime shortcuts.
  - Execution checklist:
    - [x] Ensure public package publication is blocked until v1 runtime scope, API/schema gate,
      security/provenance, docs, and release validation are ready.
    - [x] Add a workspace Rust/Vortex version-source contract so Rust MSRV derives from root
      `[workspace.package].rust-version`, upstream Vortex evidence derives from root
      `[workspace.dependencies].vortex`, and CI/release/benchmark surfaces reuse the shared
      manifest-derived helper rather than duplicating current-version literals; local shell docs
      must use `scripts/write_ci_version_env.py --format powershell | Invoke-Expression` or the
      equivalent POSIX/JSON exporter instead of pinning Rust or Vortex versions.
    - [x] Record and machine-check package identities: Python package `shardloom`, current
      workspace Rust crates unpublished, and crates.io limited to future stable public
      protocol/client crates until separate API/schema and maintainer approval exists.
    - [x] Implement local GitHub pre-release artifact staging: source archive, CLI binaries, wheel,
      sdist, checksums, SBOM, provenance, release notes, and an asset manifest are generated as
      no-publication dry-run evidence while approved tags/releases/uploads remain blocked.
    - [ ] Add TestPyPI upload, clean install, uninstall, and smoke proof before PyPI proof.
      - [x] Prepare fail-closed TestPyPI/PyPI registry proof tooling and validators that can
        verify a previously uploaded package through clean install, no-fallback client smoke, and
        uninstall without performing publication.
      - [x] Gate PyPI Trusted Publisher workflow dispatch on a prior TestPyPI proof reference.
      - [ ] After explicit maintainer approval and TestPyPI Trusted Publisher setup, run the
        TestPyPI upload and attach the registry install/uninstall/smoke transcript.
    - [ ] Add PyPI clean install, uninstall, and smoke proof only after TestPyPI passes and
      approval exists.
    - [x] Review Homebrew, Scoop, winget, conda-forge, and GHCR for v1 feasibility; include any
      feasible channel with real artifacts/proof and mark `not_in_v1_scope` only with a recorded
      reason.
    - [ ] Update package-channel matrix row by row with install/uninstall transcript, clean
      environment proof, smoke proof, SBOM, checksum, provenance, rollback/yank/delete/deprecate
      policy, authorization proof, and maintainer approval for every ready channel.
    - [ ] Change `python/pyproject.toml` development classifier only after public package
      readiness is real.
    - [x] Add `scripts/check_finished_product_readiness.py` consuming production usability, hard
      release readiness, package-channel readiness, API/schema stability, per-claim evidence,
      security, dependency audit, SBOM/checksum/provenance, golden workflows, admitted semantics,
      website readiness, docs-example proof, and CI matrix reports.
    - [x] Integrate the finished-product gate into release-readiness CI and expose
      `--require-public-release-ready` for future public release/tag commands; the gate has no
      `--allow-blocked` bypass.
    - [x] Add final release approval artifact and post-release verification for package install,
      first-10-minutes, golden workflow, no-fallback smoke, docs links, and website support matrix.
    - [ ] Move finished-product gate and package-channel closeout to the completed ledger.
  - Next outcome: release/package publication has one final fail-closed gate and channel-specific
    proof requirements.
  - User-visible surface: release commands, package channels, package metadata, release notes,
    release readiness reports, website publication, docs.
  - Implementation scope: release scripts, package-channel matrices, CI/release workflows,
    package artifacts, docs, tests, and post-release verification scripts.
  - Evidence required: finished-product readiness report, package-channel reports,
    SBOM/checksum/provenance, TestPyPI/PyPI transcripts, install/uninstall/smoke proofs,
    approval artifact, and post-release verification.
  - Acceptance: public release cannot proceed with blocked v1 runtime rows, failing docs examples,
    missing schema fixtures, missing package proof, unsupported public claims, fallback execution,
    or missing approval; deferred rows pass only with explicit out-of-v1 scope and deterministic
    blockers.
  - Verification: finished-product readiness gate, hard release readiness without `--allow-blocked`
    for release tags, package-channel readiness, release validation evidence, docs/site validation,
    CI required checks, and post-release verification after publication approval.
  - Non-goals: no publication without human approval, no release tag creation during planning, no
    package-channel claim from local dry-run evidence only.
  - Claim boundary: may claim only that the final gate exists until all package/release evidence
    and approval pass.
  - Fallback boundary: package smoke and release verification must assert
    `fallback_attempted=false` and `external_engine_invoked=false` for ShardLoom runtime paths.
  - Ledger rule: ledger entry must include gate inputs, channel rows, approval refs, package
    evidence artifacts, and post-release verification outcomes.

### Open Work Checklist

#### Production Readiness / Release Track

- [ ] `PROD-READY-1B` Object-store runtime production path.
  - Source: attached Object-Store Runtime review, `docs/architecture/scale-readiness-contract.md`,
    RFC 0017, RFC 0014, `docs/skills/object-store-runtime.md`, and object-store readiness gates.
  - Current state: `docs/release/production-certification-workloads.json` now declares the scoped
    `object_store_local_emulator_runtime_v1_candidate` profile with local-emulator fixture evidence,
    public no-credential URI-shape evidence, scoped read/write/recovery request and byte counters,
    fixture digest validation, local sidecar commit-manifest recovery evidence, provider-admission
    evidence, and deterministic blockers for live S3/GCS/ADLS, credentialed access, table commits,
    distributed runtime, production backpressure, and claim-grade benchmarks.
    The object-store runtime promotion gate also exposes explicit approved-real-backend absence
    fields: `approved_real_backend_profile_declared=false`,
    `approved_real_backend_profile_status=missing_approved_real_backend_profile`,
    `production_object_store_native_io_certificate_present=false`, and
    `production_object_store_claim_allowed=false`.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    object-store workload/backend in v1 if emulator plus approved real-backend proof, credential
    safety, bounded streaming, commit/cleanup, and certificate evidence can close; otherwise
    narrow or defer only with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: strongly applicable. Object-store work should use capillary split
    windows, dynamic request coalescing/prefetch/backpressure admission, PulseWeave task-graph
    control for bounded in-flight work, metadata-first listing/statistics decisions, and explicit
    hot-runtime versus proof/commit timing surfaces.
  - Execution checklist:
    - [x] Define the first supported object-store workload/environment and review each other
      scheme/backend for v1 feasibility before deferring it.
    - [x] Implement provider abstraction for selected schemes with credential policy, redaction,
      request signing boundary, and no-probe defaults for explain/estimate/doctor/capabilities.
    - [x] Add scoped local-emulator/public-fixture single-object listing, fixture-derived
      object version/ETag posture, requested-byte digest validation, byte-range/full-object read,
      single-request coalescing/prefetch posture, single-attempt retry/rate-limit posture, and
      bounded fixture read-budget evidence.
    - [ ] Extend listing, object version/ETag capture, checksum validation, request coalescing,
      prefetch, retry/backoff, rate-limit handling, and bounded streaming reads to the approved
      real backend profile before any production claim.
    - [x] Add scoped local-emulator single-object staged writes, sidecar commit protocol,
      rollback/cleanup, idempotency keys, recovery replay/mismatch diagnostics, non-multipart
      posture, and ambiguous-commit evidence fields.
    - [x] Add machine-readable real-backend absence fields so local-emulator/public-fixture
      evidence cannot be mistaken for production S3/GCS/ADLS runtime support.
    - [ ] Extend staged/multipart writes, commit protocol, rollback/cleanup, idempotency keys, and
      ambiguous commit diagnostics to the approved real backend profile before any production
      claim.
    - [x] Emit scoped object-store Native I/O certificates with request counts, bytes
      requested/read/written, retry attempts, cache hits, credential posture, and no-fallback
      fields for the local-emulator and public no-credential fixture profiles.
    - [ ] Emit production object-store Native I/O certificates after approved backend, cache,
      streaming/backpressure, and retry evidence exists.
    - [ ] Test against a local emulator and one approved real backend profile before any
      production claim.
    - [ ] Move closed object-store workload evidence and deferred backend scope to the ledger after
      merge.
  - Next outcome: object-store support advances from report-only ladder to a declared,
    certificate-backed runtime path for one scoped environment.
  - User-visible surface: CLI/Python object-store reads/writes, diagnostics, capability reports,
    release readiness, docs, and benchmark scale profiles.
  - Implementation scope: object-store runtime, credential/redaction policy, retry/backoff,
    streaming read/write paths, commit/rollback evidence, tests, and release validators.
  - Evidence required: local emulator tests, approved backend proof, Native I/O certificates,
    request/byte/retry counters, fault-injection tests, cleanup evidence, and no-fallback proof.
  - Acceptance: selected object-store workload can read/write with bounded memory and deterministic
    failure/cleanup behavior; unsupported backends/effects remain blocked before credential or
    network access.
  - Verification: object-store smoke/integration tests, credential redaction tests, fault-injection
    tests, release validators, and benchmark profile evidence before claims.
  - Non-goals: no table semantics, no distributed runtime, no hidden local-file mirroring claim,
    no broad multi-cloud production claim from one backend.
  - Claim boundary: may claim only the declared backend/workload profile with evidence.
  - Fallback boundary: object-store runtime must not use external query engines or platform compute
    as execution fallback.
  - Ledger rule: ledger entry must capture backend profile, credential policy, fault cases,
    certificate artifacts, and unsupported backends.
- [ ] `PROD-READY-1C` Lakehouse/table runtime production path.
  - Source: attached Lakehouse/Table Runtime review, `docs/architecture/scale-readiness-contract.md`,
    `docs/skills/translation-layer.md`, universal input/output contracts,
    `docs/architecture/table-protocol-source-review.md`, and primary table protocol specs.
  - Current state: external table metadata/reporting is separate from production table runtime.
    Scoped ShardLoom-owned `local-manifest` fixture evidence now covers in-memory metadata read,
    snapshot/manifest summary, local append commit rehearsal, rollback cleanup, sidecar recovery
    replay/mismatch diagnostics, request/byte/retry/boundedness evidence, and native
    table-translation/no-loss posture. The first source-reviewed external profile also has a
    scoped local Iceberg table metadata JSON read smoke through `iceberg-metadata-read-smoke`: it
    reads one local metadata JSON file, selects the current, explicit, or as-of timestamp snapshot,
    reports schema/partition/sort/snapshot/manifest-list references, and blocks delete-file
    semantics with deterministic no-fallback diagnostics. The same command now also supports an
    explicitly requested, feature-gated local Avro manifest-list summary read through
    `--manifest-list` when `universal-format-io` is enabled. That manifest-list path reports
    manifest summary pruning, manifest-level split counts, data/delete/unknown manifest counts, and
    delete-manifest blockers. The same command also supports an explicitly requested, feature-gated
    local Avro manifest-file split-plan read through `--manifest`, reporting data-file split counts,
    bytes, record counts, and deleted/delete/unknown entry blockers without scanning data files. It
    now also performs metadata-level schema evolution comparison by Iceberg field IDs, partition
    evolution comparison by partition field IDs/spec IDs, manifest partition-spec ID admission, and
    delete admission classification for position deletes, equality deletes, deletion-vector-shaped
    entries, deleted data-file entries, and delete manifests. Safe ID-based schema/partition
    evolution is admitted only as metadata/split-planning evidence; projection/filter execution,
    delete application, Puffin/deletion-vector reads, and data-file scans remain blocked. That does
    not imply Iceberg data scans, external catalog/runtime, object-store table commit, write
    semantics, distributed, production, or performance support. Current source-reviewed external
    candidates are Iceberg table metadata, Iceberg REST, Delta transaction logs, Hudi
    timeline/metadata, Nessie, Polaris, and Gravitino; Glue-like and Hive-like catalog profiles are
    not selected for the first external candidate and still require separate source/profile review
    before implementation.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    table protocol/workload in v1 if source-checked specs, scan semantics, write/commit scope,
    rollback/recovery, conflict handling, and no-fallback evidence can close; otherwise narrow or
    defer with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable after source-spec review. Table work should consider
    metadata-first snapshot/manifest pruning, capillary split/manifests for bounded scans and
    commits, dynamic admission for delete/schema/evolution features, and PulseWeave-style
    coordination only where it improves ShardLoom-native task/retry/commit evidence.
  - Execution checklist:
    - [x] Select the first scoped table workload profile:
      `local_manifest_table_runtime_v1_candidate`, a ShardLoom-owned local-manifest fixture
      profile; Iceberg, Delta, Hudi, external catalogs, object-store tables, and mutation
      semantics remain blocked until their source-spec and runtime evidence exists.
    - [x] Implement scoped local-manifest metadata read, snapshot/manifest summary, append commit
      rehearsal, rollback cleanup, sidecar commit recovery replay/mismatch diagnostics,
      Native I/O request/byte/retry/boundedness evidence, idempotency evidence, and native
      table-translation/no-loss posture.
    - [x] Source-check current primary external protocol specs before external implementation:
      Iceberg, Delta, Hudi, Iceberg REST, Nessie, Polaris, and Gravitino-style APIs. Glue-like and
      Hive-like catalog profiles are not selected for the first external candidate and require
      separate source/profile review before implementation.
    - [x] Implement the first selected external profile as a scoped local Iceberg metadata JSON
      reader with current snapshot, explicit snapshot-id, and as-of timestamp selection, metadata
      summary digest, source-review refs, dependency boundary fields, and deterministic blockers for
      catalog, object-store, manifest-list, manifest, data-file, delete-file, write/commit, broad
      Iceberg, Delta/Hudi, production, performance, fallback, and external-engine paths.
    - [x] Extend the selected Iceberg profile to a scoped, explicitly requested local Avro
      manifest-list summary read when `universal-format-io` is enabled, with manifest-summary
      pruning evidence, manifest-level split planning counts, delete/unknown manifest blockers, and
      deterministic default-build feature-disabled diagnostics.
    - [x] Extend from manifest-list summary into scoped local Iceberg manifest-file parsing and
      data-file split planning with no-fallback diagnostics for deleted, delete-file, unknown
      content, and unknown-status entries.
    - [x] Implement metadata-level schema/partition evolution semantics beyond visibility:
      field-ID schema comparison, partition field/spec-ID comparison, manifest partition-spec
      admission, safe metadata-only evolution status, and fail-closed blockers for projection or
      filter semantics that are not admitted.
    - [x] Implement delete/tombstone/deletion-vector admission beyond summary/count blockers:
      position-delete, equality-delete, deletion-vector-shaped, deleted data-file, delete-manifest,
      and unknown-content classifiers with deterministic no-fallback blockers.
    - [ ] Implement Delta log and Hudi timeline/metadata readers only after their source-profile
      contracts are narrowed to fixture, credential, object-store, and no-fallback evidence.
    - [ ] Lower planned Iceberg data-file splits into ShardLoom-native scan execution with Native
      I/O certificates and deterministic unsupported diagnostics for unadmitted table features.
    - [ ] Implement writes only for proven semantics: append/overwrite first; merge/update/delete
      only after correctness, conflict, rollback, and recovery evidence exists.
    - [ ] Add optimistic concurrency/conflict handling, commit/rollback/recovery evidence, and
      TranslationReport coverage for metadata/statistics/layout loss.
    - [ ] Move closed table protocol/workload evidence and deferred protocols to the ledger after
      merge.
  - Next outcome: table support becomes a scoped runtime path instead of metadata/report-only
    posture.
  - User-visible surface: table reads/writes, catalog diagnostics, capability reports, release
    readiness, docs, and benchmark table profiles.
  - Implementation scope: table metadata/runtime modules, catalog adapters, scan planning, commit
    protocol, translation reports, correctness fixtures, and release validators.
  - Evidence required: protocol conformance tests, scan correctness, write/commit/rollback tests,
    conflict handling, Native I/O certificates, TranslationReports, and no-fallback proof.
  - Acceptance: selected table workload can scan and, if in scope, write/commit with explicit
    semantics; unsupported operations fail before hidden materialization or external execution.
  - Verification: protocol fixture tests, local table integration tests, object-store tests if
    remote tables are admitted, release validators, and benchmark evidence for table claims.
  - Non-goals: no blanket Iceberg/Delta/Hudi support, no Foundry virtual-table claim, no external
    warehouse/lakehouse engine fallback.
  - Claim boundary: table claims are protocol/workload-specific and require source-checked specs
    and runtime evidence.
  - Fallback boundary: Spark, DataFusion, DuckDB, Polars, Velox, Trino, warehouse engines, and
    platform compute remain external baselines or handles, never ShardLoom execution.
  - Ledger rule: ledger entry must include selected protocol, supported operations, deferred
    semantics, command evidence, and source/spec review refs.
- [ ] `PROD-READY-1D` Distributed runtime production path.
  - Source: attached Distributed Runtime review, `docs/architecture/scale-readiness-contract.md`,
    RFC 0016, RFC 0017, `docs/skills/object-store-runtime.md`, and split/shuffle readiness docs.
  - Current state: distributed runtime is report-only. No real coordinator/worker service,
    leases, heartbeats, task attempts, remote result fragments, deterministic merge, or
    multi-worker benchmark proof exists.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include a local or scoped
    multi-worker runtime in v1 if coordinator/worker lifecycle, fault cases, deterministic merge,
    cleanup, and benchmark evidence can close; otherwise narrow to deterministic unsupported
    diagnostics with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: strongly applicable. Distributed runtime should be designed around
    capillary task units, PulseWeave runtime control, dynamic work shaping, metadata-first split
    pruning, and explicit execution certificates so later optimization does not require reworking
    the scheduler contract.
  - Execution checklist:
    - [ ] Define the first distributed workload/environment and minimum scale where single-node is
      insufficient.
    - [ ] Implement a local coordinator process/service with worker lifecycle, leases, heartbeats,
      task attempts, cancellation, cleanup, and deterministic diagnostics.
    - [ ] Execute real `SplitManifest` units across workers with bounded memory, result fragments,
      idempotency keys, retries, duplicate-attempt protection, and deterministic merge.
    - [ ] Add shuffle/repartition strategy, skew detection/handling, local combine/global merge,
      and spill/backpressure integration for stateful operators in scope.
    - [ ] Emit distributed execution certificates linking input splits, worker attempts,
      retries/cancellations, fragments, merge output, and no-fallback evidence.
    - [ ] Add fault-injection tests for worker crash, retry, duplicate attempt, partial result,
      cancelled query, stale lease, and cleanup failure.
    - [ ] Add benchmark profile proving correctness and multi-worker benefit for the declared
      workload before any distributed performance claim.
    - [ ] Move completed distributed workload evidence and deferred scale/runtime gaps to the
      ledger after merge.
  - Next outcome: distributed support moves from protocol vocabulary to one certified
    multi-worker runtime path.
  - User-visible surface: CLI/API distributed execution, diagnostics, capability reports,
    benchmark scale profiles, release readiness, and docs.
  - Implementation scope: coordinator/worker runtime, split scheduler, fragment writer/merger,
    retry/cancellation/cleanup, shuffle/backpressure, tests, and benchmark harness.
  - Evidence required: execution certificates, fault-injection results, benchmark evidence,
    correctness parity, cleanup evidence, and no-fallback proof.
  - Acceptance: declared distributed workload completes with deterministic fragments/merge and
    survives fault cases; unsupported distributed shapes fail explicitly.
  - Verification: unit/integration/fault-injection tests, scale benchmark profile, release
    validators, and workspace gates.
  - Non-goals: no distributed claim for every operator, no managed-platform claim, no
    object-store/table support unless those items are separately closed.
  - Claim boundary: may claim only declared multi-worker workload/environment with benchmark and
    fault-tolerance evidence.
  - Fallback boundary: no Ray/Dask/Spark/Flink/Trino or external distributed engine execution.
  - Ledger rule: ledger entry must name workload, worker topology, fault cases, benchmark artifacts,
    and unsupported distributed families.
- [ ] `PROD-READY-1E` Streaming/live/hybrid runtime production path.
  - Source: attached Streaming / Live / Hybrid review, RFC 0034, RFC 0017, RFC 0014,
    optimizer/adaptive execution docs, and live/hybrid capability gates.
  - Current state: scoped CG-22 fixture runtime evidence exists for deterministic in-memory
    live and hybrid workloads. `live-change-contract-plan` declares change records,
    append/upsert/delete/retract/tombstone operations, fixture event-time watermarks, reject-late
    policy, state TTL, in-memory checkpoint policy, and output changelog modes.
    `live-fixture-run` executes filter/project/count/count-where/group-count over a bounded
    in-memory change fixture and emits freshness, state, continuous-view, execution-certificate,
    Native I/O, no-fallback, and no-external-engine evidence. `hybrid-overlay-run` emits scoped
    delta-overlay, base/merged snapshot, hot changelog, flush, and certificate evidence.
    `live-hybrid-state-transition-smoke` emits retry, cancellation, cleanup, partial-output,
    state-transition, freshness, and state evidence. This is not production streaming: there is no
    broker, unbounded scheduler, durable state/checkpoint store, object-store/catalog checkpoint,
    exactly-once claim, external connector, or benchmark performance claim.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    live/hybrid workload in v1 if state, checkpoint, recovery, freshness, output mode, and
    certificate evidence can close; otherwise narrow or defer with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable. Live/hybrid work should use capillary micro-segments,
    dynamic mode/update admission, PulseWeave-style bounded work-in-progress where it preserves
    recovery semantics, metadata-first state/freshness checks, and separate timing/evidence
    surfaces for hot update paths versus checkpoint/proof work.
  - Execution checklist:
    - [x] Define scoped `EngineMode` semantics for batch/live/hybrid/auto and
      boundedness/update-mode/output-mode classification in CG-22 planning surfaces.
    - [x] Implement a scoped in-memory change record model covering append, upsert, delete,
      retract, tombstone, event time, processing time, fixture watermarks, late-data policy, and
      deterministic unsupported diagnostics for unadmitted fixture predicates/columns.
    - [x] Implement scoped in-memory state, changelog, checkpoint-ref, live output, hybrid delta
      overlay, base/merged snapshot, and tombstone/delete/retract handling for bounded fixture
      workloads.
    - [x] Define scoped fixture sink output modes and continuous-view/update evidence.
    - [x] Emit freshness, state, checkpoint-ref, delta-overlay, execution-certificate,
      Native I/O, and no-fallback evidence for the scoped live/hybrid fixture paths.
    - [x] Add recovery/fault evidence for cooperative cancellation, retry, partial-output tracking,
      cleanup completion, late-data counting, and unsupported predicate/column rejection.
    - [ ] Implement production state store, durable changelog, durable checkpoint/restore,
      hot/warm/cold storage model, Vortex micro-segment persistence, cold Vortex segment promotion,
      and deletion-vector/tombstone persistence for an admitted live/hybrid workload.
    - [ ] Add production recovery/fault tests for restart, duplicate records, partial durable
      checkpoint, durable restore, broker replay, cancellation, cleanup, and idempotent output.
    - [ ] Add benchmark/profile evidence for declared live/hybrid workload before claims.
    - [x] Move scoped live/hybrid fixture evidence and unsupported production modes to the ledger
      while keeping production streaming/runtime gaps open.
    - [ ] Move production live/hybrid workload evidence and unsupported production modes to the ledger after
      merge.
  - Next outcome: a scoped live/hybrid runtime can be certified without implying exactly-once or
    arbitrary streaming support.
  - User-visible surface: Python/CLI/API engine-mode selection, diagnostics, capability reports,
    benchmarks, docs, and release readiness.
  - Implementation scope: engine-mode runtime, state/checkpoint store, delta overlay, sink output
    modes, certificates, tests, and benchmark harness.
  - Evidence required: state/checkpoint/freshness certificates, recovery tests, correctness
    fixtures, benchmark evidence, and no-fallback proof.
  - Acceptance: selected live/hybrid workload has deterministic state and recovery behavior;
    unsupported modes fail explicitly; exactly-once is not claimed unless source/state/sink
    idempotency evidence exists.
  - Verification: state/recovery tests, live/hybrid smoke tests, release validators, benchmark
    profile, and workspace gates.
  - Non-goals: no arbitrary streaming connectors, no exactly-once claim by default, no external
    streaming engine fallback.
  - Claim boundary: may claim only the declared live/hybrid workload and delivery semantics proven
    by certificates.
  - Fallback boundary: no Flink/Spark Streaming/Kafka Streams/Ray/Dask or external engine fallback.
  - Ledger rule: ledger entry must list delivery semantics, state model, recovery cases, benchmark
    artifacts, and unsupported modes.
- [ ] `PROD-READY-1G` Foundry integration production pack.
  - Source: attached Foundry Integration review, RFC 0036,
    `docs/architecture/scale-readiness-contract.md`, Foundry proof docs, and release/package
    readiness gates.
  - Current state: Foundry support is local/dev-stack proof and optional integration posture only.
    Existing scoped evidence includes the RFC 0036 maturity ladder, package/proof boundary matrix,
    `scripts/foundry_proof_of_use.py`, `examples/foundry-lightweight-transform/`, the local
    dev-stack starter kit, local Foundry-style result/evidence dataset output, the
    `shardloom.foundry_generated_output_fanout_posture.v1` and
    `shardloom.foundry_generated_output_boundary.v1` reports, and Python
    `ctx.foundry_generated_output(...)` support for local dataset-shaped paths. Real
    `foundry://...` targets still return deterministic unsupported diagnostics before staging rows.
    There is no real Foundry Code Repository package/import proof, real platform transform wrapper,
    dataset source/sink certificate, Artifact Repository publication proof, Compute Module runtime,
    or production Foundry evidence dataset output.
  - Intake review: accepted as a v1 candidate if real Foundry environment proof is available.
    Include a scoped Foundry integration pack in v1 if package/import, transform, dataset
    source/sink, governance, and no-fallback evidence can close; otherwise defer only because the
    real platform proof is unavailable or incomplete.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable at the integration boundary. Foundry work should
    consider capillary dataset chunks, dynamic platform-handle admission, PulseWeave-style
    transform/task coalescing only with real platform evidence, metadata-first lineage/governance
    checks, and strict separation of Foundry platform handles from ShardLoom execution.
  - Execution checklist:
    - [x] Define the optional Foundry integration posture, RFC 0036 maturity ladder, and
      package/proof boundary matrix without making Foundry a core runtime dependency.
    - [x] Implement local Foundry-style proof-of-use evidence that resolves the ShardLoom CLI,
      exercises source-free generated output plus a staged local CSV transform, writes local
      certificate/metrics artifacts, and records local Foundry-style result/evidence dataset output
      without invoking Foundry services.
    - [x] Emit local proof boundaries for generated-output fanout, real Foundry output APIs,
      scale-proof readiness, package-proof readiness, direct S3/object-store writes, Foundry Spark,
      external compute, fallback execution, and public Foundry claims.
    - [x] Expose Python `ctx.foundry_generated_output(...)` and generated-output capability rows as
      local dataset-shaped smoke support while keeping real `foundry://...` platform targets
      blocked with deterministic diagnostics.
    - [x] Move scoped local/dev-stack Foundry proof evidence and unsupported real platform claims
      to the ledger while keeping production Foundry gaps open.
    - [ ] Define `shardloom-foundry` package boundary, install/import/CLI resolution, and version
      compatibility inside real Foundry Code Repositories.
    - [ ] Implement transform wrapper that records Foundry execution context, build mode,
      transaction/build refs, dataset RIDs, branches, and no-fallback evidence.
    - [ ] Emit dataset source/sink reports, certificate datasets, metrics datasets, Data Health /
      Expectations bridge evidence, lineage facets, and governance/marking/redaction policy.
    - [ ] Classify virtual tables and external systems explicitly as platform handles or
      external-compute boundaries unless staged/native ShardLoom execution evidence exists.
    - [ ] Add Artifact Repository publication proof before package availability claims.
    - [ ] Add Compute Module support only after REST/control-plane runtime item is real and
      certificate-backed.
    - [ ] Test in real Foundry environment with evidence datasets and deterministic blocked
      diagnostics for unsupported transforms/connectors.
    - [ ] Move real Foundry workload evidence and deferred platform claims to the ledger after
      merge.
  - Next outcome: Foundry integration moves from local proof posture to a scoped platform
    integration pack with real package/runtime evidence.
  - User-visible surface: Foundry package, transform wrapper, datasets, metrics/certificates,
    lineage/governance output, docs, and release readiness.
  - Implementation scope: optional Foundry package, transform helper, evidence dataset writers,
    governance/lineage reports, release/package validators, docs, and platform tests.
  - Evidence required: real Foundry package/import proof, transform run evidence, dataset
    source/sink reports, governance/redaction evidence, publication proof, and no-fallback proof.
  - Acceptance: selected Foundry workflow runs inside Foundry with ShardLoom-native execution
    evidence; unsupported platform handles remain explicit and non-claim-grade.
  - Verification: Foundry integration tests/proof artifacts, package validators, release readiness
    gates, docs/site validation, and no-fallback checks.
  - Non-goals: no Foundry Spark/Snowflake/Databricks/BigQuery execution as ShardLoom runtime; no
    production Foundry claim from local dev-stack proof; no Compute Module until REST/control plane
    exists.
  - Claim boundary: may claim only the specific Foundry workflow/package path proven in a real
    Foundry environment.
  - Fallback boundary: Foundry virtual tables and external systems are governed handles or
    external-compute boundaries, never fallback execution.
  - Ledger rule: ledger entry must include package/version proof, Foundry environment evidence,
    dataset refs, governance artifacts, and deferred platform claims.

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Closed | `PERF-RUNTIME-7B` | Completed in the ledger with full-local publication refresh evidence generated `2026-06-14T12:37:12Z`; remaining prepared/native operator tails are classified for future optimization direction, not an open 7B blocker. |
| Closed | `RELEASE-PACKAGE-15` | Completed in the ledger with clean-source benchmark publication evidence for source revision `74a2e7d4f77eed0686971518e010463da26f2cdf`; no autonomous implementation item remains. |
| Historical | PR #1174 benchmark row/readiness context, repo-wide audit closeout, release-sequence closeout, and completed benchmark/profile, sub-evidence, user-surface proof | Preserved in `docs/architecture/phased-execution-completed-ledger.md`; do not treat as active work. |
| Current evidence | `full_local` benchmark refresh | Promoted website benchmark bundle generated `2026-06-14T12:37:12Z` from source revision `64cae36e49085511b756508d0ad56807b821b2ef`; `performance_claim_allowed=false`; use for freshness and optimization direction only. |
| Mapped, not autonomous queue | Unchecked global architecture review rows | Governed by `docs/architecture/global-architecture-review.md` and `docs/architecture/runtime-gap-family-burn-down.md`; promote concrete implementation items here before work begins. |
| Deferred approval/artifact gate | Public release/package approval | Clean local Conda proof, dependency/security/package local-gate evidence, and current benchmark-publication evidence now pass locally; remaining blockers are package-channel approval/proof, publication/API/schema stability approval, and per-claim evidence promotion before any public claim. |

Deferred Non-Runtime Closeout Queue: closed for the prior cleanup batch. Completed non-runtime history
lives in `docs/architecture/phased-execution-completed-ledger.md`; newly reviewed v1
product/release work is now represented by concrete unchecked items above.

### Evidence Pointers

- Current benchmark timing snapshot and PR #1174 route/readiness context are preserved in the
  completed ledger entry `Phase-plan open-queue cleanup and completed-state ledger migration`.
- Performance route, stage, and timing-surface contracts live in
  `docs/architecture/performance-attribution-and-execution-structure.md`.
- Current source/input evidence contracts live in `docs/architecture/universal-input-contract.md`.
- Benchmark artifacts are evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no package-release
  claim, and no public freshness claim outside the promoted manifest source revision and validation
  evidence being cited.

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
- Current direct open implementation items are the v1 product/release queue, remaining
  `PERF-RUNTIME-*` optimization items, and v1-candidate production-family rows above. Reopen
  completed `PERF-DESIGN-*` or `PERF-DESIGN-*R` passes only with new current artifact, validator,
  CI, UAT simulation, or maintainer-review evidence.

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
