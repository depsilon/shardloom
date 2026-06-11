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
implementable as one coherent implementation batch or explicitly marked `report-only`,
`planning-only`, or `diagnostic-only`.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests/snapshots/release checks exist, evidence refs are attached where claims are made,
unsupported paths remain explicit, no fallback engine was invoked, completed details are moved to the
completed ledger, and supporting docs are updated without becoming a second active queue.

Section-completion rule:

- Prefer one substantial PR/session that completes an entire runtime section over a sequence of
  tiny row/format/operator PRs. Split work only when the remaining section has independent safety,
  dependency, or verification boundaries that cannot reasonably land together.
- For a section-completion PR, derive the full checklist from the owning `GAR-*` item, companion
  5-series runtime equivalent, status/capability files, route taxonomy, tests, and user-visible
  surfaces before editing. The PR should close the section across runtime code, typed reports,
  Python/CLI surfaces, docs/status artifacts, and verification evidence together.
- Avoid wording such as "promote one format/operator at a time" unless a format/operator truly has
  a separate external dependency or deterministic blocker. When the engine architecture expects a
  unified route, complete the unified route and keep per-format differences confined to read/ingest
  and write/sink boundaries.

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
  `docs/architecture/website-current-state-public-reference.md`.
- Compute-flow and benchmark references:
  `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `docs/architecture/benchmark-persistent-runner-decision.md`,
  `docs/architecture/performance-attribution-and-execution-structure.md`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
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
  `docs/architecture/pulseweave-runtime-control.md`,
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

Current autonomous execution order:

1. [ ] `SECURITY-DEEP-SCAN-R3-FOLLOWUP` - validate and burn down the round-3-bounded
   Codex Security candidate inventory.
   - Source: Codex Security deep scan
     `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep`.
     Round merge record:
     `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep/artifacts/deep_discovery/round-03/round_merge_record.json`.
   - Current state: three discovery rounds are complete and merged. The canonical inventory has
     28 discovery-stage candidates, including three new round-3 clusters:
     `SL-DEEP-026` local-source read authorization, `SL-DEEP-027` release dry-run cleanup
     authority, and `SL-DEEP-028` dependency-freshness live-GitHub token routing. Centralized
     validation and attack-path analysis have not run, and saturation is not proven because round 3
     added novelty and discovery was stopped at round 3 by user instruction.
   - Next slice outcome: produce a validated candidate-disposition matrix, then remediate or
     suppress each candidate in cohesive control-family batches with tests and release/security
     evidence.
   - User-visible surface: CLI/Python local-source and output behavior, evidence/redaction
     artifacts, benchmark/publication metadata, release and CI security gates, workflow scripts,
     and documentation status text for explicit security posture.
   - Implementation scope:
     - Workspace/path containment and symlink-safe file effects across spill payload/lifecycle,
       object-store local emulator, table append rehearsal, generated-source, SQL/SQLite/Vortex
       ingest, Foundry proof output, prepared-route state, traditional analytics sidecars, and
       release dry-run cleanup.
     - Local-source authority and SQL admission around Python query-builder/public workflow APIs
       and SQL local-source reads.
     - Resource budgets and streaming caps for SQL local-source, Vortex ingest, object-store
       read/write/recovery, partition discovery, and SQLite local import/export smokes.
     - Sensitive-output redaction for object-store URIs, planning/benchmark surfaces, Python
       exception argv, release/generated evidence paths, and token-bearing request surfaces.
     - Evidence/provenance integrity for prepared-state digests, release-artifact producer/digest
       binding, and local-emulator recovery sidecar trust.
     - CI supply-chain hardening for PyPI trusted publishing, mutable privileged actions, analyzer
       permissions, and `scripts/check_pre_5j_dependency_freshness.py`.
   - Evidence required: per-`SL-DEEP-*` validation notes, attack-path notes for reportable
     candidates, suppression counterevidence where applicable, before/after tests for fixed
     controls, redaction snapshots, path/symlink and byte-budget regression tests, updated
     release/security gate outputs, and no-fallback execution evidence.
   - Acceptance: every `SL-DEEP-001` through `SL-DEEP-028` candidate is marked
     `reportable_fixed`, `suppressed_with_counterevidence`, `not_applicable`, or
     `deferred_with_owner_and_gate`; no `candidate_needs_validation` row remains without a
     disposition; validated issues have cohesive fixes or explicit release-blocking gates; release
     and security posture checks reflect the new controls.
   - Verification:
     - `python3 -m compileall -q python/src scripts benchmarks`
     - `python3 -m pytest python/tests`
     - `python3 scripts/check_security_posture.py --repo-root .`
     - `python3 scripts/check_ci_gate_matrix.py --repo-root .`
     - `python3 scripts/check_release_security_gate.py --repo-root . --allow-blocked`
     - `cargo fmt --all -- --check`
     - `cargo clippy --workspace --all-targets -- -D warnings`
     - `cargo test --workspace --all-targets`
   - Non-goals: no package publication, public vulnerability advisory, release tag, external
     fallback engine, new query-engine dependency, or broad runtime redesign. Do not continue deep
     discovery beyond round 3 unless explicitly requested.
   - Claim boundary: discovery candidates are not confirmed vulnerabilities until validation and
     attack-path analysis complete. No public security, production-readiness, or release-readiness
     claim is allowed from this item until candidate dispositions and gate outputs are attached.
   - Fallback boundary: fixes must preserve `fallback_attempted=false` and
     `external_engine_invoked=false`; external engines remain baseline/test oracles only.
   - Ledger rule: when complete, move the detailed completed session and candidate-disposition
     summary to `docs/architecture/phased-execution-completed-ledger.md`, then keep this file as
     the compact planned queue.
   - Execution checklist:
     - [x] Stabilize the scan evidence by recording the round-3 merge counts, canonical inventory
       path, and new `SL-DEEP-026` through `SL-DEEP-028` titles in the candidate-disposition
       matrix: `docs/security/deep-security-scan-round-3-disposition.md`.
     - [x] Validate or suppress all 28 candidates before severity labels or security claims are
       made.
     - [x] Land one cohesive path/workspace containment batch for validated file-effect and
       sidecar candidates.
       - Progress: `SL-DEEP-024` and `SL-DEEP-027` are fixed; `SL-DEEP-001`, `SL-DEEP-002`,
         `SL-DEEP-003`, `SL-DEEP-004`, `SL-DEEP-005`, `SL-DEEP-007`, and `SL-DEEP-018` have
         symlink-safe/staged-write mitigations and are terminally deferred to the broader
         workspace-root authority gate.
     - [x] Land one local-source/query-control batch for validated SQL/path read and raw-fragment
       candidates.
       - Progress: `SL-DEEP-006` blocks raw SQL clause, set, and separator breakouts while
         preserving typed predicate scoping; `SL-DEEP-026` is terminally deferred to a future
         less-trusted local-source allow-root/authority gate.
     - [x] Land one resource-budget batch for validated full-buffering, recursion, and byte-cap
       candidates.
       - Progress: `SL-DEEP-009` through `SL-DEEP-014` are fixed with local-source, Vortex ingest,
         object-store, partition-discovery, and SQLite byte/row/depth budgets.
     - [x] Land one evidence/redaction/provenance batch for validated URI, argv, absolute-path,
       digest, artifact-binding, and recovery-sidecar candidates.
       - Progress: `SL-DEEP-015`, `SL-DEEP-017`, `SL-DEEP-019`, and `SL-DEEP-021` are fixed;
         `SL-DEEP-016`, `SL-DEEP-023`, and `SL-DEEP-025` are terminally deferred to public-evidence
         field-classification or producer-bound authenticity gates.
     - [x] Land one CI/workflow hardening batch for validated OIDC, mutable-action, analyzer
       permission, and live-GitHub token-routing candidates.
     - [x] Update docs/status/capability artifacts only where behavior or release gates changed.
     - [ ] Run the verification commands above and attach exact outputs or artifact paths before
       checking this item complete.

Plan state after prior closeout:

- One security follow-up item is now promoted above. No active runtime implementation,
  release-sequence, benchmark-timing, performance-innovation, or non-runtime closeout item remains
  unchecked beyond that item in this file.
- Completed runtime and release details live in
  `docs/architecture/phased-execution-completed-ledger.md`; keep this file as the compact planned
  queue.
- The 38 unchecked rows in `docs/architecture/global-architecture-review.md` are mapped by
  `docs/architecture/runtime-gap-family-burn-down.md` and
  `target/runtime-gap-family-burn-down.json` to claim-boundary evidence families. They are not
  autonomous implementation rows until a new concrete item is promoted here.
- Hard release readiness remains fail-closed for public package/release approval, API/schema
  stability, per-claim evidence, and current benchmark publication freshness. Those are approval or
  artifact-refresh gates, not unchecked phase-plan rows.
- Benchmark rows remain evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no public freshness
  claim until a clean-tree benchmark artifact is regenerated from the source revision being claimed.

Remaining work snapshot:

| Order | Work item | Remaining outcome |
| --- | --- | --- |
| Closed | `GAR-RUNTIME-IMPL-4/6A` | Residual completion gate closes with global-review rows mapped to claim-boundary evidence. |
| Closed | `RELEASE-SEQUENCE-1` through `RELEASE-SEQUENCE-14` | Local proof, package-channel posture, final rehearsal, and maintainer handoff are complete for the no-publication scope. |
| Deferred approval/artifact gate | Public release/package and current benchmark publication | Requires maintainer approval, channel-specific install/upload evidence, and a clean-source benchmark refresh before any public claim. |

Runtime and release queue status:

- Runtime Implementation Queue - Runtime-Enabling Work Only: closed for the current scoped compute
  engine completion pass. Future runtime work must be promoted as a new unchecked item here before
  implementation.
- Completed Benchmark Timing And Performance Innovation Queue: closed for current runtime
  sequencing. Hot route timing is timing-surface aware; proof/publication timing remains separate.
- 6-Series Runtime Breadth Queue: closed for the scoped user-surface breadth pass. Completed
  benchmark/profile, sub-evidence, user-surface, and UDF/extension blocker detail lives in the
  completed ledger and generated status artifacts.
- Production usability closeout anchor: completed benchmark/profile, sub-evidence, user-surface,
  and package-readiness proof detail lives in the completed ledger.
- Deferred Non-Runtime Closeout Queue: no active item. Completed non-runtime history lives in the
  completed ledger; add a concrete unchecked item here only for a new documentation, website,
  security, release, or claim-integrity blocker.
- Final Pre-Release Sequential Closeout Queue: closed as no-publication evidence. Publication,
  signing, tags, uploads, package-channel submission, release assets, and public claims still require
  explicit maintainer approval and passing hard gates.

Traceability anchors retained for validators and future routing:

Global Architecture Review Carry-Forward:

- `GAR-RUNTIME-IMPL-6E` automatic dynamic preparation;
  `GAR-RUNTIME-IMPL-6F` output/fanout conversion;
  `GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout;
  `GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`;
  `GAR-RUNTIME-IMPL-4D-F2` complex dtype; `GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery;
  `GAR-RUNTIME-IMPL-6A` compute-engine completion gate; and the closed 6D runtime breadth families.
- Runtime gap-family burn-down phase strings retained for validator mapping:
  `GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar`;
  `GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth`;
  `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down`;
  `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication`;
  `GAR-RUNTIME-IMPL-6D:last_order.effectful_operations`;
  `GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime`;
  `GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime`.
- Reference docs that own deferred claim, release, benchmark, or runtime-boundary evidence include
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/architecture/pulseweave-runtime-control.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/release/release-architecture-tracker-gate.md`, and
  `docs/release/final-release-rehearsal.md`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
