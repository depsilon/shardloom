<!-- SPDX-License-Identifier: Apache-2.0 -->

# Hard Release Readiness Gate

Status: P8.4 release gate command. This gate is fail-closed and does not publish packages, create
tags, add secrets, or authorize fallback execution.

## Command

```powershell
python scripts\check_release_readiness.py
```

Do not run the hard release gate with `--allow-blocked` in CI or release-readiness evidence. The
`release-readiness` CI job may mark this specific step `continue-on-error: true` while blockers
remain so PR validation can still publish the blocker report. Inspect the emitted blocker report or
the component gates when evidence is still incomplete.

The script writes:

```text
target/hard-release-readiness-gate.json
```

## CI Execution Shape

The hard release gate is an artifact aggregation step in CI. Upstream evidence is produced in
parallel where possible, then the final `release-readiness` job downloads those reports into
`target/downloads`, runs `Merge downloaded release evidence`, and copies the normalized files into
`target/` before running the final rehearsal, production usability gate, and this hard aggregate.

Producer evidence artifacts:

- `release-local-smoke-evidence`: local package smoke, dry-run transcript, local provenance,
  `target/debug/shardloom`, and `python/dist` from `python-package`; this carries every local
  artifact path referenced by the provenance report.
- `dependency-security-evidence`: dependency audit, security posture, provenance dry run, and
  release security gate from `dependency-security`.
- `release-runtime-core-evidence`: golden workflow, admitted semantics, and release architecture
  tracker reports. The tracker classifies unchecked global-review rows through the runtime
  gap-family burn-down map instead of treating mapped claim-boundary rows as raw blockers.
- `release-package-governance-evidence`: contribution governance and package-channel readiness.
- `release-user-surface-evidence`: Python user-surface, SQL/DataFrame parity, runtime-gap,
  graduation, burn-down, and route-capability reports; produced after reusing the local dry-run
  transcript from `release-local-smoke-evidence`.
- `release-benchmark-claim-evidence`: pre-5J dependency freshness, benchmark artifact
  completeness, benchmark publication claim gate, and front-door benchmark publication gate
  reports. The final aggregate consumes the precomputed benchmark completeness/publication reports
  when present instead of rescanning the large public benchmark bundle, and verifies
  manifest/artifact digests before trusting the precomputed completeness report.
- `website-docs-evidence`: website readiness report.
- `ci-gate-matrix-report`: CI matrix drift contract.
- `v1-security-ci-hardening-report`: dependency audit, license, forbidden-fallback dependency,
  SBOM/checksum/provenance, package-artifact scan, security posture, compatibility-matrix, and
  release-evidence bundle closeout for the v1 local release-hardening surface.

This split reduces PR wall-clock time without weakening the hard gate. The final report still
requires the same JSON evidence files, keeps public release/package claims blocked until their
own gates pass, and preserves `fallback_attempted=false` and `external_engine_invoked=false`.
Downloaded artifacts are normalized through `python scripts/merge_release_evidence_artifacts.py`
before downstream validators run, so artifact-root differences cannot hide missing package or
provenance files. The final job also runs a strict downloaded-evidence existence check before
aggregate gates so a missing producer artifact fails quickly instead of being obscured by a later
blocker report.

## Gate Coverage

The gate aggregates:

- clean install, first-10-minutes, and local benchmark smoke transcript
- clean Conda environment install proof
- release security gate report
- contribution governance intake report
- workspace Rust/Vortex version source report proving Rust MSRV derives from root
  `[workspace.package].rust-version`, Vortex provider evidence derives from root
  `[workspace.dependencies].vortex`, and active CI/evidence surfaces reuse the shared helper instead
  of hard-coded current-version literals
- golden local runtime workflow validator report
- admitted semantics fixture matrix validator report
- package metadata, license, repository, and homepage metadata
- package-channel readiness matrix and channel-specific install/smoke/provenance/rollback proof
- per-claim evidence attachment matrix for release, package, performance, Spark-displacement,
  engine-replacement, production SQL/DataFrame, object-store/lakehouse, and Foundry/platform claims
- release architecture tracker report for mapped Global Architecture Review claim boundaries,
  phased-plan closure, traceability, unsupported-path, security, provenance, and per-claim evidence
  blockers
- final no-publication release rehearsal report for local artifact, checksum, SBOM, provenance,
  attestation-plan, package-channel, and human-approval blockers
- Python user-surface completion report for import/context/session/SQL/DataFrame/generated-output
  proof, deterministic unsupported-path blockers, and no-fallback/no-external-engine fields
- SQL/Python/DataFrame front-door parity report for scoped shared-runtime rows, broad parity gap
  rows, and performance-equivalence claim blockers
- local v1 observability/supportability report for doctor/support-bundle/capability/runtime
  surfaces, redaction, issue-template intake, plan-only explain/estimate diagnostics, benchmark
  timing-surface fields, and no-network/no-effect boundaries
- v1 security/CI hardening report for dependency audit, license classification,
  forbidden-fallback dependency absence, SBOM, checksum manifest, provenance, vulnerability scan,
  package artifact scan, no-signing rationale, Trusted Publisher/OIDC posture, Python 3.10 through
  3.13 compatibility, OS matrix coverage, Rust MSRV derived from root Cargo.toml validation,
  artifact retention, and
  release evidence bundle upload
- user route capability report for input/output route selection, Vortex-normalization boundaries,
  materialization/decode boundaries, output/evidence routes, and local benchmark-range route
  posture, including the scenario-level local-file benchmark route map
- publication/API/schema stability gate for public compatibility windows, package identities,
  signing policy, checksums, SBOM, and publication approval
- feature/build matrix execution evidence
- typed-envelope compatibility posture
- required validation command evidence
- global architecture runtime-claim gate evidence for distributed, object-store, and lakehouse
  public-claim boundaries

Required validation commands before public release:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
python -m unittest discover python/tests
python -m build python
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
cargo run -q -p shardloom-cli -- global-architecture-gate --format json
python scripts\check_workspace_version_sources.py
python scripts\check_release_security_gate.py
python scripts\check_release_architecture_tracker.py --allow-blocked
python scripts\check_contribution_governance.py
python scripts\check_package_channel_readiness.py --require-local-evidence
python scripts\check_python_user_surface_completion.py
python scripts\check_sql_python_dataframe_parity.py
python scripts\check_v1_front_door_runtime_scope.py
python scripts\check_v1_vortex_runtime_scope.py
python scripts\check_v1_source_prepared_state_scope.py
python scripts\check_v1_local_output_sink_scope.py
python scripts\check_v1_local_resource_safety.py
python scripts\check_v1_observability_support.py
python scripts\check_v1_api_schema_stability.py
python scripts\check_v1_example_replay.py
python scripts\check_v1_correctness_conformance.py
python scripts\check_v1_security_ci_hardening.py
python scripts\check_user_surface_runtime_gap_inventory.py
python scripts\check_user_route_capability_report.py
python scripts\check_pre_5j_dependency_freshness.py
python scripts\check_golden_workflows.py
python scripts\check_admitted_semantics_matrix.py
python scripts\final_release_rehearsal.py --allow-blocked
```

The local evidence runner records the feature/build matrix and required validation command status:

```powershell
python scripts\run_release_validation_evidence.py `
  --python-executable <python-3.10-or-newer> `
  --pip-audit-python <python-with-pip-audit> `
  --require-clean-conda `
  --conda-executable <conda-or-mamba-or-micromamba>
```

Use Python 3.10 or newer for package/Python release evidence. On macOS, the Command Line Tools
`python3` can still be Python 3.9, which is not a supported ShardLoom package runtime and will fail
the clean wheel install proof.

It writes:

```text
target/release-validation-evidence.json
```

That report uses schema `shardloom.release_validation_evidence.v1` and contains:

```text
feature_build_matrix_status
required_validation_status
supporting_security_dependency_status
feature_build_matrix_rows
required_validation_commands
command_results
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

The global architecture gate uses schema
`shardloom.global_architecture_runtime_claim_gate.v1` and must keep
`runtime_claim_allowed=false`, `public_claim_allowed=false`, `fallback_attempted=false`, and
`external_engine_invoked=false` unless distributed, object-store, and lakehouse claims have their
own workload-scoped evidence.

The Python user-surface completion gate uses schema
`shardloom.python_user_surface_completion_gate.v1`:

```powershell
python scripts\check_python_user_surface_completion.py
```

It writes:

```text
target/python-user-surface-completion-gate.json
```

The gate checks the local import/context/session surface, scoped `ctx.sql(...)` bridge,
DataFrame/query-builder method matrix, source-free generated output rows, local Python smoke
transcript markers, unsupported materialization/input blockers, docs/website claim language, and
status-matrix public-claim blockers. It intentionally reports:

```text
scoped_python_front_door_claim_allowed=true
production_sql_dataframe_claim_allowed=false
spark_compatibility_claim_allowed=false
package_publication_claim_allowed=false
performance_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This is a scoped admitted-local-runtime front-door claim only. It does not authorize PySpark API
parity, broad SQL/DataFrame production support, decoded pandas/Arrow/NumPy materialization,
object-store/lakehouse/table production I/O, package publication, or performance claims.

The SQL/Python/DataFrame parity gate uses schema
`shardloom.sql_python_dataframe_parity_gate.v1`:

```powershell
python scripts\check_sql_python_dataframe_parity.py
```

It writes:

```text
target/sql-python-dataframe-parity-gate.json
```

The gate checks `ShardLoomContext.front_door_parity_matrix()` and requires scoped local
SQL/Python/DataFrame rows to name their shared ShardLoom runtime path while broad language,
Vortex, object-store/lakehouse, unbounded materialization, and performance-equivalence gaps remain
explicit. It intentionally reports:

```text
scoped_local_front_door_parity_supported=true
flexible_anything_claim_allowed=false
performance_equivalence_claim_allowed=false
all_no_fallback_no_external_engine=true
```

Passing this gate means the repo is honest about front-door parity. It is not a broad
SQL/Python/DataFrame completion claim.

The user-surface runtime gap inventory uses schema
`shardloom.user_surface_runtime_gap_inventory.v1`:

```powershell
python scripts\check_user_surface_runtime_gap_inventory.py
```

It writes:

```text
target/user-surface-runtime-gap-inventory.json
```

The inventory classifies every structured `unsupported`, `blocked`, `not complete`, and
`front_door_gap` status in the current user-surface path into one of:

```text
runtime_available_needs_front_door
runtime_available_needs_output_route
runtime_available_needs_claim_evidence
true_runtime_expansion_item
policy_rejected
```

It also proves that benchmark unsupported rows are external-baseline limitations rather than
ShardLoom runtime gaps. The current expected acceptance summary keeps:

```text
shardloom_benchmark_unsupported_rows=0
all_inventory_rows_classified=true
all_inventory_rows_no_fallback_no_external_engine=true
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

Passing this gate means the user surface is machine-readable about remaining runtime gaps. It does
not close the gaps or authorize broad runtime, production, or performance claims.

The user route capability report uses schema
`shardloom.user_route_capability_report.v1`:

```powershell
python scripts\check_user_route_capability_report.py
```

It writes:

```text
target/user-route-capability-report.json
```

The report is the agent-facing route selector for scoped local ShardLoom workflows. Each row names
the input family, desired outputs, start state, Vortex normalization point, execution mode,
execution route, output route, evidence route, materialization/decode boundary, runtime status,
prepared-state reuse scope/manifest diagnostics, claim boundary, and no-fallback/no-external-engine
fields. Prepared compatibility routes must expose the workspace manifest contract at
`<workspace>/.shardloom/prepared-vortex-reuse-manifest.json`; non-prepared routes must mark reuse
as not applicable instead of implying hidden cache behavior. It intentionally keeps:

The CLI evidence surface uses the same vocabulary: cold compatibility preparation reports
`prepared_state_created_not_reused`, warm prepared rows report `explicit_prepared_state_input`,
native Vortex rows report `not_applicable_native_vortex_input`, and single-process prepare/batch
rows report `in_process_prepared_batch_vortex_artifacts` for the first prepare/batch call.
Repeated compatible `traditional-analytics-prepare-batch-run` calls may report
`workspace_manifest_local_vortex_artifacts` when the workspace manifest validates and compatibility
preparation is skipped; source or artifact drift must reprepare and record the invalidation reason.
Feature-gated `vortex-ingest-smoke`
reports `artifact_adjacent_manifest_local_vortex_artifacts` when a repeated local ingest reuses an
existing `.vortex` artifact, and it must keep source/artifact drift visible through
`prepared_state_invalidation_reason`.

```text
all_no_fallback_no_external_engine=true
flexible_anything_claim_allowed=false
performance_equivalence_claim_allowed=false
production_claim_allowed=false
spark_replacement_claim_allowed=false
claim_gate_status=not_claim_grade
unsupported_local_benchmark_route_ids=[]
local_file_benchmark_unsupported_scenario_ids=[]
local_file_benchmark_all_mapped_without_generic_unsupported=true
```

Passing this gate means agents and users can choose a scoped route without inferring from scattered
parity, benchmark, and inventory artifacts. It does not authorize broad arbitrary
SQL/Python/DataFrame runtime, production readiness, package publication, performance equivalence,
or Spark replacement claims.

The pre-5J dependency freshness gate uses schema
`shardloom.pre_5j_dependency_freshness_gate.v1`:

```powershell
python scripts\check_pre_5j_dependency_freshness.py
```

It writes:

```text
target/pre-5j-dependency-freshness-gate.json
```

The default CI-safe mode verifies that the currently admitted Dependabot dependency updates are
present in manifests, `Cargo.lock`, and dependency-review docs, while keeping
`benchmark_refresh_allowed=false` until a live open-Dependabot check is performed. Immediately
before any `GAR-RUNTIME-IMPL-5J` benchmark-publication refresh, run:

```powershell
python scripts\check_pre_5j_dependency_freshness.py --require-live-github
```

That live mode checks the open Dependabot PR set for `depsilon/shardloom`, blocks unknown or
unincorporated dependency PRs, and is required by the benchmark publication claim gate before
benchmark data can be treated as current publication evidence. The gate never runs benchmarks and
reports:

In GitHub Actions, the CI workflow supplies the scoped Actions token to this step and grants only
`pull-requests: read`; the gate uses the token for the live PR query so rate limits do not replace
the actual dependency freshness decision.

```text
benchmark_run_performed=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

The broader release process must also attach clean Conda proof, benchmark smoke evidence,
package metadata/license proof, package-channel proof, SBOM/checksum/provenance evidence, runtime
no-fallback dependency audit, and release notes or known-unsupported-path evidence before public
claims are allowed.

Contribution governance uses schema `shardloom.contribution_governance_report.v1`:

```powershell
python scripts\check_contribution_governance.py
```

It writes:

```text
target/contribution-governance-report.json
```

The gate checks that `CONTRIBUTING.md`, `docs/legal/contributor-policy.md`,
`docs/legal/contribution-intake-readiness.md`, `.github/PULL_REQUEST_TEMPLATE.md`, CI, and the CI
gate matrix agree on the required signoff/CLA/DCO state, review-state reporting, maintainer
decision escalation, dependency/license/provenance checklist, security/release/RFC checklist, claim
boundary checklist, and no-fallback policy. It intentionally reports:

```text
contribution_intake_status=documented_and_ci_checked
external_contribution_acceptance_status=maintainer_approval_required
cla_assistant_status=not_active
dco_policy_status=not_active
legal_claim_status=documented_policy_only
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

This is a governance drift check only. It does not activate CLA/DCO automation, publish packages,
transfer maintainer governance, or satisfy public release/package approval.

The local golden workflow validator uses schema
`shardloom.golden_workflow_validation_report.v1`:

```powershell
python scripts\check_golden_workflows.py
```

It writes:

```text
target/golden-workflow-report.json
target/golden-workflows
```

The validator builds the CLI with `vortex-write,vortex-local-primitives`, then executes local
CSV-to-`vortex_ingest`, prepared Vortex primitive replay, JSONL/CSV fanout, generated-source local
Vortex output/replay, and fixture-certified count/project/filter-project primitive workflows. It
also checks the docs and website `runs-today` matrix rows for those surfaces. It intentionally
reports:

```text
golden_workflow_validator_status=passed
workflow_count=3
stage_count>=9
support_matrix_status=passed
runtime_support_claim=local_runtime_workflow_proof_only
production_claim_allowed=false
performance_claim_allowed=false
public_release_claim_allowed=false
public_package_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This is a local runtime proof only. It does not authorize production workflow,
object-store/lakehouse/Foundry, package-publication, distributed-runtime, or performance-superiority
claims.

The admitted semantics matrix validator uses schema
`shardloom.admitted_semantics_matrix_report.v1`:

```powershell
python scripts\check_admitted_semantics_matrix.py
```

It writes:

```text
target/admitted-semantics-matrix-report.json
target/admitted-semantics-matrix
```

The validator checks `docs/status/admitted-semantics-matrix.json`, executes scoped SQL
local-source fixtures, compares ShardLoom output against decoded reference JSONL, runs the
declared deterministic seeded property lane set plus deterministic v1 fuzz lanes, verifies unsupported
diagnostics, checks semantic conformance and the non-executing correctness-harness boundary, and
intentionally reports:

```text
admitted_semantics_validator_status=passed
matrix_status=passed
matrix_row_count=144
executable_fixture_count=117
diagnostic_case_count=25
unsupported_diagnostic_count=23
runtime_error_diagnostic_count=1
invalid_shape_diagnostic_count=1
property_lane_count=10
property_execution_performed=true
deterministic_fuzz_execution_performed=true
deterministic_fuzz_case_count=5
decoded_reference_differential_execution_performed=true
semantic_conformance_suite_status=passed
correctness_harness_boundary_status=passed
production_claim_allowed=false
ansi_sql_claim_allowed=false
performance_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This is admitted-expression correctness evidence only. It does not authorize ANSI SQL parity,
production semantic parity, external-oracle execution, package publication, or performance claims.

Benchmark rows also pass through the fail-closed constitution validator:

```powershell
python scripts\check_benchmark_constitution.py
```

The release gate checks `shardloom.benchmark_constitution_validation.v1` manifest fields and keeps
`benchmark_constitution_performance_claim_allowed=false` until claim-bearing rows include source
admission, preparation/execution/output routes, correctness proof, hardware/build metadata,
cold/warm attribution, stage timings, cost/unit fields where available, no-fallback proof, and
external-baseline boundary evidence.

For `full_local`, `full_local_plus_spark`, and `extended_local` benchmark manifests, the release
gate also requires current ShardLoom runtime lanes to be present in both `expected_lanes` and
`available_lanes`:

```text
shardloom
shardloom-prepared-vortex
shardloom-prepare-batch
shardloom-vortex
```

`shardloom-prepare-batch` is the single-process `UniversalIngress -> SourceState ->
vortex_ingest -> VortexPreparedState -> prepared_vortex batch` route. Omitting it from benchmark
artifacts hides a real runtime path and keeps the benchmark/release evidence incomplete.

The hard release gate consumes the reports produced by
`scripts/check_benchmark_artifact_completeness.py` and
`scripts/check_benchmark_publication_claim_gate.py` so missing profile-required formats, scenarios,
published row evidence, broad-format row coverage, ShardLoom engine/format cells, capillary
activation evidence, runtime-envelope proof, independent reproducibility/correctness/timing/replay
proof, and no-fallback/no-external-engine proof block release readiness through the same canonical
benchmark validators that protect the website/public bundle. The front-door benchmark publication
gate, `scripts/check_front_door_benchmark_publication.py`, composes those public artifacts with the
SQL/Python/DataFrame parity report and keeps performance equivalence
`blocked_pending_measured_equivalence_artifact` until measured equivalent front-door rows, rerun
approval, correctness digests, and execution certificates exist. Local runs without the precomputed
reports fall back to direct manifest scans. The validators inspect static benchmark artifacts only;
they do not rerun benchmarks.

The package-channel matrix uses schema `shardloom.package_channel_readiness_matrix.v1`:

```powershell
python scripts\check_package_channel_readiness.py
```

It writes:

```text
target/package-channel-readiness-report.json
```

The matrix is valid when blocked channels are explicit. Release-readiness runs the stricter
package-gate mode:

```powershell
python scripts\check_package_channel_readiness.py --require-local-evidence
```

That mode also consumes `target/dependency-audit-report.json`,
`target/release-dry-run-proof/transcript.json`, and
`target/release-provenance-dry-run/supply-chain-release-evidence.json`, requiring dependency
inventory, license classification, forbidden-fallback dependency absence, local package smoke,
SBOM refs, checksum refs, provenance status, rollback policy refs, and human publication
authorization state. The hard release gate remains blocked until each channel has
channel-specific install, uninstall, clean-install, smoke, SBOM/checksum/provenance,
rollback/yank/delete/deprecate, and authorization evidence. PyPI and TestPyPI require Trusted
Publisher/OIDC posture. Internal Rust crates remain unpublished; crates.io is limited to future
stable public API crates.

Trusted Publisher/OIDC remains the required release-grade posture for PyPI and TestPyPI package
channels.

`GAR-0024-A` adds the publication/API/schema stability gate with schema
`shardloom.publication_api_schema_stability_gate.v1`. The current gate intentionally reports:

```text
publication_api_schema_gate_status=blocked
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
package_publication_performed=false
tag_created=false
signing_key_used=false
fallback_attempted=false
external_engine_invoked=false
```

The gate rows are `api_compatibility_window`, `schema_compatibility_window`,
`package_identity_approval`, `signing_policy_decision`, `checksum_manifest`, `sbom_bundle`, and
`publication_approval`. `scripts\check_release_readiness.py` must keep the hard release gate
blocked while this gate reports `publication_api_schema_gate_status=blocked`.

`PROD-V1-2A` adds local v1 API/schema stability evidence under the same release boundary:

```text
python scripts\check_v1_api_schema_stability.py
target/v1-api-schema-stability-report.json
shardloom.v1_api_schema_stability_matrix.v1
stable_surface_count=12
diagnostic_code_count=22
diagnostic_code_doc_ref=docs/release/diagnostic-code-stability.md
compatibility_window=v1_additive_compatibility
legacy_flat_field_policy=stable_aliases_for_v1_with_documented_deprecation_window
fallback_attempted=false
external_engine_invoked=false
```

This local schema evidence makes the source-built v1 machine-readable fields testable. It does not
approve package identity, signing, channel publication, tag creation, checksum/SBOM publication
grade, or public API/schema claims without the remaining publication rows.

`PROD-V1-2B` adds local v1 correctness/conformance aggregation under the same release boundary:

```text
python scripts\check_v1_example_replay.py
python scripts\check_v1_correctness_conformance.py
docs/release/v1-correctness-conformance-matrix.json
target/v1-example-replay-report.json
target/v1-correctness-conformance-report.json
shardloom.v1_example_replay_report.v1
shardloom.v1_correctness_conformance_matrix.v1
shardloom.v1_correctness_conformance_report.v1
docs_marker_source_count=6
runtime_command_count=3
golden_workflow_replay_verified_count=3
benchmark_scenario_count=9
benchmark_expected_error_scenario_count=1
unsupported_failure_fixture_count=2
all_no_fallback_no_external_engine=true
input_report_count=8
matrix_status=passed
v1_correctness_matrix_status=passed
scope_report_status=passed
golden_workflow_validator_status=passed
admitted_semantics_validator_status=passed
example_replay_validator_status=passed
docs_example_execution_status=passed
unsupported_path_test_status=passed
decoded_reference_differential_execution_performed=true
property_execution_performed=true
correctness_claim_allowed=true
runtime_support_claim_allowed=false
public_release_claim_allowed=false
public_package_claim_allowed=false
performance_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This aggregate report proves that the current v1 correctness evidence is present and coherent
across golden workflows, admitted semantics, front-door scope, Vortex scope, source/prepared-state
scope, local output/sink scope, Python user-surface scope, and the bounded docs/README/website
example replay gate. It does not approve broad
SQL/DataFrame parity, production
readiness, package publication, performance claims, or external-runtime fallback.

`PROD-V1-2C` adds local v1 resource-safety, cancellation, and cleanup evidence under the same
release boundary:

```text
python scripts/check_v1_local_resource_safety.py
docs/architecture/v1-local-resource-safety.md
target/v1-local-resource-safety-report.json
shardloom.v1_local_resource_safety.v1
shardloom.v1_local_resource_safety_report.v1
runtime_command_count=5
runtime_command_pass_count=5
prerequisite_report_count=2
memory_budget_config_status=passed
pre_oom_guard_status=passed
retry_gate_status=passed
cancellation_cleanup_status=passed
memory_runtime_hardening_status=passed
fault_tolerance_gate_status=passed
prepared_state_cleanup_status=passed
local_output_cleanup_status=passed
v1_scope_ready=true
local_resource_safety_evidence_ready=true
unsupported_paths_blocked_without_writes=true
all_no_fallback_no_external_engine=true
larger_than_memory_claim_allowed=false
native_spill_runtime_claim_allowed=false
distributed_resource_claim_allowed=false
spill_io_performed=false
object_store_io=false
output_dataset_write_by_resource_gate=false
public_release_claim_allowed=false
public_package_claim_allowed=false
performance_claim_allowed=false
production_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This gate proves the local v1 resource-safety boundary: pre-OOM reservation denial and cleanup,
side-effect-free retry/cancellation gate planning, prepared-state non-persistence/reuse evidence,
and local output/sink write-policy evidence. It does not approve larger-than-memory execution,
native spill runtime, distributed resource handling, object-store recovery, package publication, or
production claims.

`PROD-V1-2D` adds local v1 observability, supportability, and troubleshooting evidence under the
same release boundary:

```text
python scripts/check_v1_observability_support.py
docs/architecture/v1-observability-support.md
docs/release/troubleshooting-diagnostics.md
target/v1-observability-support-report.json
shardloom.v1_observability_support.v1
shardloom.v1_observability_support_report.v1
runtime_command_count=8
runtime_command_pass_count=8
doctor_status=passed
support_bundle_status=passed
agent_contract_status=passed
capability_discovery_status=passed
runtime_observability_status=passed
observability_schema_status=passed
explain_plan_only_status=passed
estimate_plan_only_status=passed
route_capability_status=passed
api_schema_stability_status=passed
docs_status=passed
issue_template_status=passed
benchmark_observability_status=passed
v1_scope_ready=true
observability_support_evidence_ready=true
side_effect_free_support_surfaces=true
support_bundle_redaction_ready=true
all_no_fallback_no_external_engine=true
telemetry_exporter_enabled=false
remote_support_upload_enabled=false
runtime_profile_collection_enabled=false
public_release_claim_allowed=false
public_package_claim_allowed=false
performance_claim_allowed=false
production_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

This gate proves the local v1 supportability boundary: stable route/stage/timing-surface
observability fields, side-effect-free doctor/capability/support commands, deterministic
unsupported plan-only explanations, redacted local support bundles, issue-template intake fields,
and benchmark timing-surface/evidence-tier field coverage. It does not approve
OpenTelemetry/OpenLineage exporters, remote support upload, live profiling collection, production
observability, package publication, or performance claims.

`GAR-0041-A` adds the per-claim evidence attachment matrix with schema
`shardloom.per_claim_evidence_attachment_matrix.v1`. The release gate consumes
`docs/release/per-claim-evidence-attachment-matrix.md` and keeps public claims blocked while that
matrix reports:

```text
per_claim_evidence_attachment_matrix_support_status=blocked
per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade
per_claim_evidence_attachment_matrix_all_claims_blocked=true
per_claim_evidence_attachment_matrix_public_release_claim_allowed=false
per_claim_evidence_attachment_matrix_public_package_claim_allowed=false
per_claim_evidence_attachment_matrix_performance_claim_allowed=false
per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false
per_claim_evidence_attachment_matrix_fallback_attempted=false
per_claim_evidence_attachment_matrix_external_engine_invoked=false
```

Every public claim row must name `required_test_evidence`, `required_benchmark_evidence`,
`required_certificate_evidence`, `required_native_io_evidence`, `required_security_evidence`,
`required_provenance_evidence`, `required_unsupported_path_evidence`,
`required_no_fallback_evidence`, and `required_release_approval`. Any missing attachment keeps the
claim gate blocked.

`GAR-0043-A` adds the release architecture tracker with schema
`shardloom.release_architecture_tracker_report.v1`. The hard release gate consumes:

```text
target/release-architecture-tracker-report.json
```

The tracker may pass when unchecked Global Architecture Review rows are fully mapped to runtime
gap-family claim boundaries and no phased-plan rows remain unchecked. It still does not authorize
public release or package claims:

```text
architecture_tracker_status=passed|blocked
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
unchecked_global_architecture_review_count
unchecked_phase_plan_count
global_review_mapping_status
global_review_unchecked_rows_block_release
runtime_gap_family_burn_down_status
traceability_matrix_present
known_unsupported_paths_present
release_security_refs_present
release_provenance_refs_present
per_claim_evidence_matrix_present
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

Unchecked GAR IDs must be visible in the active phased plan or completed ledger, and every missing
release/security/provenance/unsupported-path marker remains a release blocker. This validation does
not close the unchecked work; it prevents the release gate from passing while the architecture
tracker is still blocked.

`GAR-0043-B` adds the final no-publication release rehearsal with schema
`shardloom.final_release_rehearsal_report.v1`. The hard release gate consumes:

```text
target/final-release-rehearsal/final-release-rehearsal-report.json
```

The local no-publication rehearsal is expected to pass once local artifact, SBOM, checksum,
provenance, security, contribution-governance, architecture, package-channel, unsupported-path,
per-claim, and
publication/API/schema refs are present and internally consistent. It requires the package-channel
report to be generated with `--require-local-evidence` so dependency audit, package smoke, and
SBOM/checksum/provenance evidence cannot be skipped. It still keeps publication and claim flags
blocked:

```text
rehearsal_status=passed
claim_gate_status=not_claim_grade
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_authorization_status=human_approval_required
publication_human_approved=false
local_artifacts_only=true
final_attestation_status=not_signed_local_rehearsal
package_upload_attempted=false
feedstock_submission_attempted=false
marketplace_submission_attempted=false
signing_key_used=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

The rehearsal writes a local attestation plan with schema
`shardloom.local_publication_attestation_plan.v1`. It is an approval checklist, not a public
attestation or signature. Real artifact signing, public attestations, release tags, uploads, and
package-channel submissions remain maintainer-approved release actions outside autonomous Codex
execution.

`GAR-RUNTIME-IMPL-4S` / `GAR-RUNTIME-IMPL-5Q` add the local production-usability aggregate with
schema `shardloom.production_usability_gate.v1`. It consumes the release dry-run transcript,
package-channel strict report, release security report, contribution-governance report, final
release rehearsal, website readiness report, benchmark artifact completeness, runs-today status
matrix, and docs/security/legal learning-path refs. The local usability gate can pass while this
hard public-release gate remains blocked; it must keep `public_release_claim_allowed=false`,
`public_package_claim_allowed=false`, `production_claim_allowed=false`,
`performance_claim_allowed=false`, `publication_attempted=false`, `tag_created=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

Run:

```powershell
python scripts\check_production_usability_gate.py
```

The report is written to `target/production-usability-gate.json` and is consumed by this hard gate.

`GAR-PERF-2H` adds the optimized build-profile and PGO benchmark lane. Portable release artifacts
remain the normal `release` profile artifacts unless a separate release gate explicitly admits a
portable optimized profile. `release-lto` is portable ThinLTO benchmark evidence, `release-pgo` is
benchmark-only unless a merged profile artifact is supplied through `SHARDLOOM_PGO_PROFILE`, and
`release-native-benchmark` applies `target-cpu=native` only in the benchmark harness. Any
`release-native-benchmark` or `target-cpu=native` build is benchmark-only and cannot satisfy public release/package evidence. PGO artifacts must record training workload refs, profile artifact refs,
`profile-generate`, `llvm-profdata` merge, `profile-use`, and claim gates before they can appear in
benchmark evidence.

`clean_conda_env_install_status=passed` is required for a public-release pass. A source-local clean
venv install is useful P8.2 evidence, but it is not a substitute for the clean Conda proof required
before public package/release claims.

`scripts\release_dry_run_proof.py` records the clean Conda status as part of
`target/release-dry-run-proof/transcript.json`. When `mamba`, `conda`, or `micromamba` is not
available locally, the transcript records `clean_conda_env_install_status=skipped_tool_missing` and
the hard gate remains blocked. Maintainers can make missing or failing Conda proof fail the dry run
directly with:

```powershell
python scripts\release_dry_run_proof.py --require-clean-conda
```

## Claim Rule

`public_release_claim_allowed` and `public_package_claim_allowed` must remain false unless every
gate passes, including package-channel readiness. Public claims must be generated from evidence
artifacts, not prose.

## Current Expected State

When proof artifacts are missing, stale, lack clean Conda evidence, or lack package-channel
readiness evidence, the gate is expected to emit:

```text
status=blocked
public_release_claim_allowed=false
```

That blocked result is correct release behavior. It prevents accidental publication when any runtime,
protocol, packaging, benchmark, provenance, security, or unsupported-path proof is missing.

With current validation evidence, release security evidence, a dry-run transcript containing
`clean_conda_env_install_status=passed`, and a fully ready package-channel matrix, the gate emits:

```text
status=passed
public_release_claim_allowed=true
public_package_claim_allowed=true
```

That pass is still local release-readiness evidence only. It does not publish packages, create tags,
upload artifacts, add secrets, or authorize unsupported-path claims.
