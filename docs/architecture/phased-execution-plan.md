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
  `v1_candidate_pending_feasibility`, `deferred_out_of_v1`, `documentation_only`, or
  `unsupported_boundary`. The v1 default is inclusion for anything feasible to complete with
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
The first unchecked checkbox is the next default autonomous slice.

Current autonomous execution order:

- [ ] `RELEASE-PACKAGE-0.1X-BUNDLED-CLI-1` Python package bundled CLI binary resolution for
  managed development environments.
  - Source: June 16, 2026 package/UAT feedback after live package simulation showed normal
    `sl.context()` works when `shardloom` is on `PATH`, but PyPI-only managed environments such as
    Foundry dev repos still need `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, or a source checkout
    binary.
  - Current state: v0.1.0 PyPI is a pure Python client surface. `ShardLoomClient` resolves an
    explicit `binary`, `SHARDLOOM_BIN`, source-checkout `repo_root` binaries, then `shardloom` on
    `PATH`. It does not ship a CLI binary inside the Python wheel, so Python-only package installs
    cannot run CLI-backed commands without an external binary. README, package docs, and website
    examples now distinguish normal `ctx.read(...)` code from schema-pinned benchmark/source
    checkout code, but the binary-resolution ergonomics remain a real packaging gap.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: dynamic admission applies to binary resolution and selected wheel
    platform tags; metadata-first release evidence applies to wheel contents, checksums, SBOM,
    provenance, and no-download policy. PulseWeave and capillary runtime controls do not apply
    directly because this is package resolution, not query execution, but release evidence-tier
    controls must keep package access separate from production/performance claims.
  - Execution checklist:
    - [ ] Decide and document the package strategy: bundled platform wheels in `shardloom` versus a
      companion platform package; reject runtime binary download unless a later explicit RFC
      approves network/provenance side effects.
    - [ ] Add a packaged-binary resolver path that checks bundled wheel resources before `PATH`
      while preserving explicit `binary`, `SHARDLOOM_BIN`, and source-checkout `repo_root`
      precedence.
    - [ ] Add platform wheel build/release wiring for the selected patch release scope, starting
      with the platforms that can support Foundry/dev-env and local maintainer proof without
      weakening Apache-2.0 license/provenance or no-fallback constraints.
    - [ ] Add resolver tests proving bundled binary discovery, explicit override precedence,
      deterministic missing-binary diagnostics, executable-bit handling, and no runtime download.
    - [ ] Add clean-venv package smoke proof with no `SHARDLOOM_BIN`, no `SHARDLOOM_REPO_ROOT`, and
      no Homebrew dependency: `import shardloom as sl; ctx = sl.context(); ctx.smoke_check();
      ctx.read(...).limit(...).collect()`.
    - [ ] Update README, Python README, package install docs, release/channel matrices, website
      source, generated website output, and maintainer handoff docs so managed Python installs no
      longer require user code to pass binary paths when a supported bundled wheel is installed.
    - [ ] Add release validators/channel proof fields for bundled CLI wheel contents, checksums,
      SBOM/provenance, clean install/uninstall, Homebrew coexistence, and rollback/yank policy.
    - [ ] Move completed details to the ledger after implementation, validation, and patch-release
      PR handling.
  - Next outcome: a cohesive patch-release packaging PR that makes `sl.context()` usable from a
    Python-only managed environment on supported wheel platforms without `SHARDLOOM_BIN`.
  - User-visible surface: PyPI wheel, Python `sl.context()`, package install docs, release matrix,
    website quickstart, and Foundry/dev-env package smoke instructions.
  - Implementation scope: `python/src/shardloom/client.py`, Python package metadata/build config,
    release scripts, package-channel validators, docs/release, README/Python README, website source
    and generated static output, and focused resolver/package smoke tests.
  - Evidence required: resolver unit tests, clean-venv wheel smoke, package metadata proof,
    checksum/SBOM/provenance proof, no-fallback fields, no external-engine fields, and no runtime
    network download.
  - Acceptance: on supported platform wheels, `python -m pip install shardloom==<patch>` followed
    by `import shardloom as sl; ctx = sl.context(); ctx.smoke_check()` works without
    `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, Homebrew, or source checkout; unsupported platforms
    fail with deterministic installation or binary-resolution diagnostics.
  - Verification: focused Python resolver tests, clean package install smoke, release-channel
    validator, website/static checks, and CI package build jobs for the selected wheel platforms.
  - Non-goals: no runtime binary downloads, no hidden network effects, no Spark/DataFusion fallback,
    no production Foundry claim, no object-store/table/live/distributed production claim, no
    performance superiority claim, and no broad SQL/DataFrame parity expansion.
  - Claim boundary: package ergonomics only. This may support Foundry/dev-env trials after install,
    but it is not production Foundry proof until a real Foundry environment supplies workload
    evidence.
  - Fallback boundary: `fallback_attempted=false` and `external_engine_invoked=false` remain
    required in smoke reports; the bundled binary is ShardLoom's own CLI, not an execution fallback.
  - Ledger rule: completed details move to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [x] `RELEASE-V1-LOCAL-SOURCE-PACKAGE-1` Public-source/package release track without production
  environments.
  - Source: maintainer decision on June 15, 2026 to proceed with the feasible release workstreams
    after real production object-store/table/distributed/live/Foundry environments were ruled out
    for v1 certification.
  - Current state: local v1 runtime, docs/website, Python/package dry-run, and full-local benchmark
    evidence exist; actual package uploads, release tags, GitHub Release object/assets, registry
    install/uninstall smoke transcripts, signing/attestation, and production/platform claims remain
    blocked until the selected final publication event.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: timing-surface separation and evidence-tier controls apply to the
    local benchmark publication wording; dynamic admission applies to selected package channels;
    PulseWeave/capillary runtime work does not apply because this item is release orchestration,
    local proof, docs/site, and package-channel gating rather than engine operator execution.
  - Execution checklist:
    - [x] Add a machine-readable selected-track contract for source checkout, GitHub pre-release,
      TestPyPI, PyPI, local API/schema stability, Python scenario proof, local benchmark evidence,
      and docs/website/readme cleanup.
    - [x] Fix package-publication workflow validation so dynamic Python package versioning is read
      from `python/src/shardloom/_version.py`, not a nonexistent static `project.version`.
    - [x] Update README, package install docs, public-status docs, handoff docs, and website source
      to state the selected path and final-event boundary without exposing live package commands
      early.
    - [x] Add validator and regression tests for the selected-track contract and dynamic-version
      workflow guard.
    - [x] Run local/source install proof, Python user-surface proof, local benchmark scenario
      proof, timing review, package-channel/local release validators, and website/static checks.
    - [x] Move completed summary to the ledger after validation and publication-prep PR handling.
  - Next outcome: a merged release-prep PR that makes the feasible v1 source/package path explicit
    and locally validated while keeping publication and production claims fail-closed.
  - User-visible surface: README, getting-started docs, release docs, website start/home/about
    pages, package workflow, local proof transcripts, and validator reports.
  - Implementation scope: `.github/workflows/pypi-publish-draft.yml`, `scripts/*release*`,
    `docs/release/*`, `docs/getting-started/*`, `README.md`, `website-src/*`, tests, generated
    website output.
  - Evidence required: no-fallback fields, selected-channel readiness report, stable API/schema
    report, local package dry-run, Python scenario proof, timing review, benchmark manifest
    freshness, website build/static validation, and CI.
  - Acceptance: selected channels are GitHub pre-release/TestPyPI/PyPI; production environment gates
    remain blocked; package commands remain withheld until the final event; dynamic Python version
    is used by the publication workflow; local validators pass.
  - Verification: `python scripts/check_v1_local_source_package_release.py`,
    `python scripts/release_dry_run_proof.py --rows 64 --iterations 1`,
    `python examples/local-python-smoke/run.py --repo-root .`,
    `python examples/local-python-benchmark-scenarios/run.py --repo-root .`,
    `python examples/local-python-benchmark-scenarios/timing_review.py --repo-root .`,
    website build/readiness checks, focused Python release-script tests, and broad CI-equivalent
    checks as risk warrants.
  - Non-goals: no package upload, no release tag, no GitHub Release object/assets, no signing key,
    no production object-store/table/distributed/live/Foundry claim, no Spark/DataFusion fallback.
  - Claim boundary: local/source/package release preparation only; publication, production,
    performance, superiority, and Spark-displacement claims remain false until a later approved
    final event supplies proof.
  - Fallback boundary: `fallback_attempted=false` and `external_engine_invoked=false` must remain
    visible in local proof and validator reports.
  - Ledger rule: completed details move to
    `docs/architecture/phased-execution-completed-ledger.md`.

### v1 Local Closeout Status

The June 15, 2026 v1-local closeout remains current for generated docs/website output,
Python/package smoke evidence, and the committed full-local benchmark artifact. One post-release
package ergonomics item is now open above to remove the explicit CLI-binary setup burden for
supported Python-only managed environments. Production cloud/object-store, production lakehouse,
production distributed, production live/hybrid, and real Foundry claims remain fail-closed until
maintainers provide the external approvals and real service environments listed below.

- [x] `PROD-V1-5A-LOCAL` Local finished-product gate, package-channel matrix, hard-release gate,
  release rehearsal, release boundary, security/dependency/provenance, and final approval/post
  release verification scripts exist and pass in no-publication mode. Public package/release claims
  remain blocked by package-channel approval/proof, publication/API/schema approval, and per-claim
  evidence promotion.
- [x] `PROD-READY-1B-LOCAL` Object-store v1 candidate local scope is closed with provider
  abstraction, credential/redaction/no-probe policy, local-emulator/public-fixture read evidence,
  scoped staged write/sidecar commit/recovery evidence, Native I/O certificates, and explicit
  absence fields for approved real backends and production claims.
- [x] `PROD-READY-1C-LOCAL` Table/lakehouse v1 candidate local scope is closed with ShardLoom-owned
  local-manifest metadata/read/append rehearsal, Iceberg metadata/manifest/split/read evidence,
  Delta/Hudi metadata readers, source-spec review refs, local translation/no-loss reporting, and
  explicit blocked diagnostics for protocol writes, remote catalogs, delete semantics, and
  production table claims.
- [x] `PROD-READY-1D-LOCAL` Distributed v1 candidate local scope is closed with scoped in-process
  coordinator/worker fixture runtime, capillary split units, PulseWeave attempt graph evidence,
  local repartition/combine/merge, skew/backpressure evidence, fault-injection cases, Python
  wrappers, execution certificates, and explicit blocked diagnostics for remote/multi-host claims.
- [x] `PROD-READY-1E-LOCAL` Live/hybrid v1 candidate local scope is closed with bounded
  in-memory/live/hybrid fixtures, local durable checkpoint/changelog/state-store/microsegment/cold
  promotion manifests, restore/replay/partial-checkpoint evidence, Python wrappers, certificates,
  and explicit blocked diagnostics for broker, exactly-once, object-store/catalog checkpoint, and
  production streaming claims.
- [x] `PROD-READY-1G-LOCAL` Foundry v1 candidate local scope is closed with optional integration
  posture, local dev-stack proof-of-use, generated-output result/evidence dataset-shaped paths,
  Python `foundry_generated_output(...)`, and explicit blocked diagnostics for real `foundry://`,
  Artifact Repository, Compute Module, Spark/platform compute, and production Foundry claims.
- [x] `BENCH-FRESH-2026-06-15` The full-local benchmark bundle was rerun and promoted into
  `website/assets/benchmarks/latest/manifest.json`. The manifest is the source of truth for
  `benchmark_git_sha`, `generated_at_utc`, chunk refs, and row admission evidence. The promoted
  bundle has 1,920 admitted published rows, 1,200 successful ShardLoom rows, 600 hot-runtime rows,
  600 publication-proof rows, zero blocked/unsupported ShardLoom rows, and
  `performance_claim_allowed=false`.

### External Approval And Environment Gates

These are not autonomous local implementation items. Promote one back into `## Planned` only after
the required external approval, credential, publication channel, or real service environment is
available and the item can be implemented and validated without weakening no-fallback policy.

| Gate | Required external input | Current fail-closed owner |
| --- | --- | --- |
| TestPyPI/PyPI publication proof | Maintainer approval is recorded for v0.1.0; remaining evidence is Trusted Publisher upload, registry install/uninstall/smoke transcript, rollback/yank policy evidence, and proof refs | `docs/release/package-channel-readiness-matrix.json`, `.github/workflows/pypi-publish-draft.yml`, `scripts/check_package_channel_readiness.py`, `scripts/check_finished_product_readiness.py --require-public-release-ready` |
| Other package/distribution channels | Homebrew is selected for v0.1.0 after GitHub source-archive proof; Scoop/winget/conda-forge/GHCR/crates.io require explicit future channel approval or out-of-v1 decision with transcript/provenance | `docs/release/package-channel-readiness-matrix.json`, `docs/release/maintainer-publication-handoff.md` |
| Public release/API/schema approval | Functional v1 surfaces are approved as stable for v0.1.0; release claims still require checksum/SBOM bundle, per-claim evidence attachment, channel proof, and public release/tag creation | `docs/release/publication-api-schema-stability-gate.md`, `docs/release/per-claim-evidence-attachment-matrix.md`, `scripts/check_release_readiness.py` |
| Production object-store claim | Approved real S3/GCS/ADLS-compatible backend profile, credentials, read/write/fault/retry/backpressure evidence, production Native I/O certificates | `docs/release/production-certification-workloads.json`, object-store readiness reports, `scripts/check_production_certification_gate.py` |
| Production table/lakehouse claim | Source-spec-approved protocol write/commit scope, real conflict/rollback/recovery proof, delete/evolution semantics evidence, optional object-store table environment | `docs/release/production-certification-workloads.json`, table protocol docs, `scripts/check_production_certification_gate.py` |
| Production distributed claim | Remote worker service/environment, network coordinator, multi-host fault injection, remote shuffle/spill/backpressure, workload benchmark proving benefit | `docs/release/production-certification-workloads.json`, distributed runtime certificates, `scripts/check_production_certification_gate.py` |
| Production live/hybrid claim | Durable state/checkpoint/changelog store beyond local fixture, broker/source replay environment, idempotent output proof, benchmark/fault evidence | `docs/release/production-certification-workloads.json`, live/hybrid state reports, `scripts/check_production_certification_gate.py` |
| Real Foundry integration claim | Real Foundry Code Repository/package/import proof, transform run, dataset source/sink reports, governance/lineage/metrics datasets, Artifact Repository proof | `docs/release/production-certification-workloads.json`, RFC 0036 proof docs, Foundry proof reports |
| Benchmark publication claim | Clean committed worktree after benchmark promotion plus live authenticated pre-5J dependency freshness check immediately before claiming publication freshness | `website/assets/benchmarks/latest/manifest.json`, `scripts/check_benchmark_publication_claim_gate.py`, `scripts/check_pre_5j_dependency_freshness.py --require-live-github` |

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Closed local v1 | Package/readiness, object-store, table/lakehouse, distributed, live/hybrid, Foundry local candidate scopes, docs/website, and current full-local benchmark refresh | Completed details live in `docs/architecture/phased-execution-completed-ledger.md`; keep public/production claims blocked until the external gates above have real evidence. |
| External gate | Public package/release channels | Maintainer approval is recorded for GitHub/TestPyPI/PyPI/Homebrew v0.1.0; trusted-publisher uploads, GitHub release assets, Homebrew formula proof, and post-release verification still must pass before claims are allowed. |
| External gate | Real cloud/object-store, table, distributed, live/hybrid, and Foundry production environments | Maintainer must provide the real environment and approval to run credentialed/platform tests before these can become claim-grade. |
| Claim-safe current evidence | `full_local` benchmark refresh | Current website bundle freshness is recorded in `website/assets/benchmarks/latest/manifest.json`; it is evidence and optimization direction only, not a public performance/superiority/Spark-displacement claim. |

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
