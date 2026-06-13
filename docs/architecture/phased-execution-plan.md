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

- [ ] `RELEASE-PACKAGE-15` - Clean-source benchmark publication refresh and strict release evidence
      finalization.
  - Source:
    - `target/release-readiness-audit/benchmark-publication-claim-gate-strict-after-live-pre5j.json`
      from the strict benchmark publication gate.
    - `target/release-readiness-audit/benchmark-publication-claim-gate-strict-after-static-descendant-contract.json`
      from the strict benchmark publication gate after the static-publication descendant contract
      fix.
    - `target/pre-5j-dependency-freshness-gate.json`.
    - `target/release-readiness-audit/release-validation-evidence-conda-pip-audit-current.json`.
    - `target/release-readiness-audit/hard-release-readiness-gate-current-final.json`.
  - Current state:
    - Current runtime/performance planned queue is otherwise closed.
    - Live pre-5J dependency freshness now passes with
      `benchmark_refresh_allowed=true`, `open_dependabot_check_status=passed`, and
      `open_dependabot_pr_count=0`.
    - Clean local Conda proof, target-local `pip-audit` dependency audit, release security,
      package-channel local gate, production-usability gate, compute-engine completion gate, and
      release architecture tracker pass locally without publication, tag creation, secrets,
      fallback execution, or external engine invocation.
    - Strict benchmark publication remains blocked because the public benchmark manifest records
      `benchmark_git_sha=a693e299988830b0587d66df0f088a80b6038f75` and
      `shardloom_git_sha=a693e299988830b0587d66df0f088a80b6038f75`, while current `HEAD` is
      `173f88c25b36736aa51a6c50bafe0c6ec9bf5fed`; strict output now reports
      `git_currentness_status=blocked_mismatched_source_revision`,
      non-publication source deltas after the benchmark source revision, and tracked local changes.
    - The strict publication validator now distinguishes the benchmarked source revision from a
      later checked-in static publication commit: a clean descendant is current only when all
      post-source changes are checked-in generated website/public static publication artifacts or
      benchmark data mirrors, plus the phase-plan ledger/handoff release bookkeeping needed to
      record the completed refresh.
    - Public package-channel readiness, publication/API/schema stability, and per-claim evidence
      approval remain maintainer/publication gates, not autonomous Codex publication actions.
  - Next outcome:
    - After the current cohesive source changes are committed/merged or otherwise stabilized at the
      exact source revision to be claimed, regenerate or promote the benchmark publication bundle
      from that clean source revision, refresh website/public mirrors and generated site output,
      and rerun strict publication and release evidence without dirty/stale-git allowances. The
      final publication commit may be a clean static-publication descendant of the benchmarked
      source revision, but no code, tests, scripts, benchmark harness source, README/public docs, or
      website source may change after the source revision recorded in the manifest.
  - User-visible surface:
    - Website benchmark page/static assets, benchmark manifest/results, release-readiness reports,
      maintainer publication handoff, and hard release gate output.
  - Implementation scope:
    - Source and generated benchmark assets under `website/assets/benchmarks/latest/`,
      `website-public/assets/benchmarks/latest/`, `website/assets/data/`,
      `website-public/assets/data/`, and `website-src/src/data/`.
    - Checked-in generated static website output under `website/` and `website-public/` when the
      benchmark data refresh changes rendered pages.
    - Release evidence under `target/release-readiness-audit/` for local proof only.
    - Documentation updates only for current blocker/evidence references.
  - Evidence required:
    - Live pre-5J dependency freshness immediately before refresh.
    - Full or approved benchmark-publication artifact refresh from the exact clean source revision.
    - Artifact completeness, publication claim gate without `--allow-stale-git` and without
      `--allow-dirty-worktree`, website readiness/static asset validation, release validation
      evidence, and hard release-readiness aggregate.
  - Acceptance:
    - `python3 scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json`
      passes without stale/dirty allowances.
    - Manifest `benchmark_git_sha` and `shardloom_git_sha` identify the clean claimed source
      revision; the strict report has `git_currentness_status=current_head` or
      `git_currentness_status=static_publication_descendant`.
    - If the final commit is a static-publication descendant, the strict report's
      `static_publication_nonpublic_delta_paths` is empty and the changed paths are limited to
      checked-in generated website/public static publication artifacts, benchmark data mirrors, and
      the phase-plan ledger/handoff release bookkeeping files.
    - Worktree dirty/currentness blockers are absent from the strict report.
    - Benchmark mirrors remain digest-identical across website, website-public, and website-src
      data refs.
    - Hard release-readiness blocker list no longer contains benchmark currentness or required
      validation command blockers; any remaining blockers are maintainer/publication gates.
  - Verification:
    - `python3 scripts/check_pre_5j_dependency_freshness.py --require-live-github --output target/pre-5j-dependency-freshness-gate.json`
    - Benchmark refresh/promote command for the approved source revision and profile.
    - `python3 scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json`
    - `python3 scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json`
    - `python3 scripts/check_benchmark_publish_doctor.py`
    - `python3 scripts/check_website_readiness.py`
    - `python3 scripts/run_release_validation_evidence.py --require-clean-conda --conda-executable /opt/homebrew/bin/micromamba --pip-audit-python target/release-readiness-audit/pip-audit-venv/bin/python`
    - `python3 scripts/check_release_readiness.py`
  - Non-goals:
    - No package publication, release tag, signing key use, package-channel upload, feedstock
      submission, OCI push, public API/schema stability approval, or per-claim promotion.
    - No benchmark or performance claim until strict clean-source artifact and claim gates pass.
  - Claim boundary:
    - This item can make benchmark/publication evidence current for the claimed source revision.
      It does not authorize package availability, production, Spark-displacement, superiority,
      SQL/DataFrame production, object-store/lakehouse, Foundry/platform, or broad encoded-native
      claims.
  - Fallback boundary:
    - Refresh must preserve `fallback_attempted=false` and `external_engine_invoked=false` for
      ShardLoom rows; external engines remain benchmark baselines only.
  - Ledger rule:
    - Move completed detail to `docs/architecture/phased-execution-completed-ledger.md` after the
      strict clean-source benchmark-publication evidence is generated and validated.

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Open | `RELEASE-PACKAGE-15` | Stabilize the current cohesive source changes, refresh benchmark publication artifacts from the exact clean source revision, and rerun strict release evidence. |
| Historical | PR #1174 benchmark row/readiness context, repo-wide audit closeout, release-sequence closeout, and completed benchmark/profile, sub-evidence, user-surface proof | Preserved in `docs/architecture/phased-execution-completed-ledger.md`; do not treat as active work. |
| Mapped, not autonomous queue | Unchecked global architecture review rows | Governed by `docs/architecture/global-architecture-review.md` and `docs/architecture/runtime-gap-family-burn-down.md`; promote concrete implementation items here before work begins. |
| Deferred approval/artifact gate | Public release/package and current benchmark publication | Clean local Conda proof and dependency/security/package local-gate evidence now pass in `target/release-readiness-audit/`; remaining blockers are package-channel approval/proof, publication/API/schema stability approval, per-claim evidence promotion, and strict clean-source benchmark-publication validation for the exact source revision before any public claim. |

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
- There are no current direct open implementation items. Reopen completed `PERF-DESIGN-*` or
  `PERF-DESIGN-*R` passes only with new current artifact, validator, CI, UAT simulation, or
  maintainer-review evidence.

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
