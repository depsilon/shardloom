# CI Gate Matrix

## Purpose and release boundary

`shardloom.ci_gate_matrix_report.v1` is the release-grade GitHub Actions gate contract, synchronized
with `.github/workflows/ci.yml` and `scripts/check_ci_gate_matrix.py`. Its 26 required lanes cover
work shaping, Rust, native Vortex and patched providers, Python, native packages, dependency and
security review, release evidence, website/docs, and drift validation.

This gate does not publish packages, create release tags, use signing keys, upload artifacts to
package channels, expand runtime support, or authorize production, performance, Spark-replacement,
object-store, lakehouse, Foundry, broad SQL, or broad DataFrame claims. This change makes no remote
performance claim.

```text
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
skipped_gate=clean_conda_release_environment
skipped_gate=real_publication
```

The workflow uses `concurrency:`, `cancel-in-progress: true`, and read-only `actions: read`
permissions. No external query-engine fallback is permitted.

## Execution topology

- `rust-baseline` runs formatting, workspace Clippy, and all-target workspace tests as three
  matrix lanes.
- `rust-feature-matrix` retains default/all/no-default checks, upstream Vortex and Vortex IO/local/
  encoded-read features, release user surfaces, observation, spill/resource, CLI, lint, and
  Conda-recipe checks.
- Native tests use one checked optimized build with the `ci-native` profile inheriting dev
  opt-level 2, debug level 1, assertions and overflow checks enabled, and LTO disabled. The exact
  test inventory is split into four duration-balanced partitions, each run serially per runner;
  aggregation validates ownership and failure propagation.
- Eight patched-provider test/lint configurations run in three groups (`arrays`, `io`,
  `buffer`) using committed locks and a shared Cargo target directory.
- CLI debug and release artifacts are produced in separate jobs and verified by release evidence.
- Linux builds one abi3 wheel plus a separate observation product. Python 3.10 and 3.13 proof jobs
  consume and identify the same Linux wheel artifact. macOS builds and proves its own Python 3.13
  artifact. Windows compatibility, bundled native CLI, and native resource-admission checks remain
  active.
- The native-wheel aggregate verifies all three platform/interpreter proofs and identical Linux
  wheel identity across Python 3.10 and 3.13.
- Evidence producers run independently; the always-running `release-readiness` job aggregates
  their outputs and executes final release gates.

The canonical Linux wheel still audits both its extension and bundled CLI against
`manylinux_2_39_x86_64`. The pinned auditwheel tool must succeed without patching
binaries or adding libraries; every payload except WHEEL/RECORD metadata remains
byte-identical. Each interpreter verifies the sealed audit evidence, dependency
notices and payload identities, then performs a fresh isolated installation,
complete binding suite and uninstall verification. Zero tests or any skipped
binding test fails the proof. Package and proof directories remain outside the
Cargo cache and reject pre-existing destinations.

The separate observation extension runs all 13 queue, acknowledgment,
cancellation and retained-owner checks on each interpreter/platform. Its getters
remain absent from the ordinary wheel. The native Rust observation configuration
and ordinary debug public-surface tests remain separate coverage obligations;
the checked optimized library profile does not claim to duplicate debug coverage.
Windows extension support remains unclaimed. Provider lock provenance is recorded
in [the standalone validation lock review](../../.github/provider-locks/README.md).

## Required lanes

Each lane below records its required commands, artifact references, release blocker references,
and workflow markers verbatim for contract checking.

### ci_work_shaping_contract — ci-work-shaping

- Commands: `cargo run -q -p shardloom-cli -- ci-work-shaping-plan`; `python scripts/ci_execution_plan.py plan`
- Artifacts: `target/ci-work-shaping-plan.json`; `target/ci-work-shaping-changed-files.txt`; `ci-work-shaping-evidence`; `target/ci-execution-plan.json`
- Release blockers: `metadata-first CI work shaping`; `capillary changed-file selection`; `pulseweave evidence fingerprint`; `source-aware benchmark rerun recommendations`
- Workflow markers: `fetch-depth: 0`; `Collect changed files`; `SHARDLOOM_CI_WORK_SHAPING_MODE=pull_request`; `SHARDLOOM_CI_WORK_SHAPING_MODE=merge`; `--changed-paths-file target/ci-work-shaping-changed-files.txt`; `retention-days: 14`; `selected_jobs:`; `full_validation:`; `git diff --no-renames --name-only`

### rust_baseline — rust-baseline

- Commands: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --all-targets`
- Artifacts: none
- Release blockers: `default Rust formatting, linting, and tests`
- Workflow markers: none

### rust_feature_matrix — rust-feature-matrix

- Commands: `cargo check --workspace`; `cargo check --workspace --all-features`; `cargo check --workspace --no-default-features`; `cargo check -p shardloom-vortex --features upstream-vortex`; `cargo check -p shardloom-vortex --features vortex-file-io`; `cargo check -p shardloom-vortex --features vortex-local-primitives`; `cargo check -p shardloom-vortex --features vortex-encoded-read-spike`; `cargo check -p shardloom-vortex --features release-user-surfaces`; `cargo test --locked -p shardloom-vortex --lib --features release-user-surfaces,output-window-observation output_window_observation::tests`; `cargo test --locked -p shardloom-vortex --lib --features release-user-surfaces,output-window-observation credit_window::tests`; `cargo test -p shardloom-exec --lib --features spill-payload-fs spill_payload::tests -- --test-threads=1`; `cargo test -p shardloom-vortex --lib --features shardloom-exec/spill-payload-fs bounded_execution::tests -- --test-threads=1`; `cargo test -p shardloom-cli --test required_execution_resources --features shardloom-exec/spill-payload-fs -- --test-threads=1`; `cargo test -p shardloom-cli --all-targets --no-default-features --features vortex-write`; `cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings`; `cargo test -p shardloom-contract-tests --test conda_packaging_recipes`
- Artifacts: none
- Release blockers: `workspace feature/build matrix`
- Workflow markers: `native_vortex_observation`; `fail-fast: false`

### native_optimized_build — native-test-build

- Commands: `python scripts/ci_build_artifact.py build --product native-tests --directory "$RUNNER_TEMP/ci-native-tests"`; `python scripts/ci_native_tests.py prepare --bundle "$RUNNER_TEMP/ci-native-tests"`
- Artifacts: `native-test-executable`; `${{ runner.temp }}/ci-native-tests`
- Release blockers: `checked optimized native build and complete inventory`
- Workflow markers: `ubuntu-24.04`; `if-no-files-found: error`

### native_test_partitions — native-test-partitions

- Commands: `python scripts/ci_native_tests.py run --bundle target/ci-native-tests --partition ${{ matrix.partition }} --output target/ci-native-results/${{ matrix.partition }}`
- Artifacts: `native-test-executable`; `native-test-partition-${{ matrix.partition }}`; `target/ci-native-results/${{ matrix.partition }}`
- Release blockers: `duration-balanced complete native execution`
- Workflow markers: `partition: [0, 1, 2, 3]`; `fail-fast: false`; `native-test-build`

### patched_provider_tests — patched-provider-tests

- Commands: `python scripts/ci_provider_tests.py --group ${{ matrix.group }} --output target/provider-evidence/${{ matrix.group }}`
- Artifacts: `provider-evidence-${{ matrix.group }}`; `target/provider-evidence/${{ matrix.group }}`
- Release blockers: `all eight patched-provider test and lint configurations`
- Workflow markers: `group: [arrays, io, buffer]`; `CARGO_TARGET_DIR: ${{ github.workspace }}/target/providers`; `.github/provider-locks/*.lock`

### native_test_coverage — native-tests

- Commands: `python scripts/ci_native_tests.py aggregate --bundle target/ci-native-tests --output target/ci-native-results`
- Artifacts: `native-test-executable`; `native-test-partition-*`; `native-test-coverage`; `target/ci-native-results/coverage.json`
- Release blockers: `native test ownership and failure propagation`
- Workflow markers: `Rust feature matrix (native_vortex_tests)`; `always()`; `native-test-build`; `native-test-partitions`; `patched-provider-tests`; `rust-feature-matrix`

### native_cli_debug — native-cli-build-debug

- Commands: `cargo test --locked -p shardloom-cli --lib --bin shardloom --test vortex_prepare --test public_memory_inputs --test public_workflow_route --test resident_worker --features release-user-surfaces`; `python scripts/ci_build_artifact.py build --product cli-debug --directory "$RUNNER_TEMP/ci-cli-debug"`
- Artifacts: `native-cli-debug-linux`; `${{ runner.temp }}/ci-cli-debug`
- Release blockers: `ordinary debug native public-surface coverage`
- Workflow markers: `Rust feature matrix (native_public_surface_tests)`; `ubuntu-24.04`

### native_cli_release — native-cli-build-release

- Commands: `python scripts/ci_build_artifact.py build --product cli-release --directory "$RUNNER_TEMP/ci-cli-release"`
- Artifacts: `native-cli-release-linux`; `${{ runner.temp }}/ci-cli-release`
- Release blockers: `exact release CLI artifact`
- Workflow markers: `ubuntu-24.04`

### rust_msrv_validation — rust-msrv

- Commands: `cargo check --workspace --no-default-features`; `cargo check --workspace --all-targets --features release-user-surfaces`; `python scripts/write_release_compatibility_lane_report.py`; `--lane "$SHARDLOOM_RUST_MSRV_LANE"`; `--rust-toolchain "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"`
- Artifacts: `target/release-compatibility/rust_msrv_*.json`; `release-compatibility-rust-msrv`
- Release blockers: `Rust MSRV derived from root Cargo.toml validation`
- Workflow markers: `python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"`; `rustup toolchain install "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"`; `rustup default "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"`; `key: rust-msrv-${{ hashFiles('Cargo.toml') }}`; `SHARDLOOM_RUST_MSRV_LANE`; `retention-days: 14`

### python_test_shards — python-test-shards

- Commands: `python scripts/run_python_test_shard.py --shard ${{ matrix.shard }}`
- Artifacts: `target/python-test-shards/${{ matrix.shard }}.json`; `python-test-shard-${{ matrix.shard }}`
- Release blockers: `Python test shards`
- Workflow markers: `core`; `front_door_benchmark_publication`; `release_scripts`; `fail-fast: false`

### python_tests — python-tests

- Commands: `python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics`; `python scripts/merge_python_test_shard_evidence.py`
- Artifacts: `python-test-shard-*`; `target/python-test-evidence.json`; `python-test-evidence`
- Release blockers: `Python tests`; `Python compile check`
- Workflow markers: `needs:`; `python-test-shards`; `if: always()`; `actions/download-artifact@v8`; `merge-multiple: true`

### python_compatibility_matrix — python-compatibility-matrix

- Commands: `python -m compileall -q python/src scripts examples benchmarks/traditional_analytics`; `python -m build python`; `cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces`; `./target/release/shardloom.exe --version`; `cargo test --release -p shardloom-cli --test non_unix_resource_admission --features release-user-surfaces`; `python scripts/write_release_compatibility_lane_report.py --lane ${{ matrix.lane }} --surface python --python-version ${{ matrix.python-version }} --os-name ${{ matrix.os }}`
- Artifacts: `target/release-compatibility/${{ matrix.lane }}.json`; `release-compatibility-${{ matrix.lane }}`
- Release blockers: `Python 3.10 through 3.13 compatibility`; `OS matrix`; `Windows bundled native CLI compilation`; `Windows native resource-admission refusal`
- Workflow markers: `fail-fast: false`; `python-version: "3.10"`; `python-version: "3.11"`; `python-version: "3.12"`; `python-version: "3.13"`; `ubuntu-latest`; `macos-latest`; `windows-latest`; `if: ${{ matrix.os == 'windows-latest' }}`; `key: windows-release-user-surfaces`; `retention-days: 14`

### native_package_build_linux — native-package-build-linux

- Commands: `python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt`; `python scripts/ci_native_package.py build --output "$RUNNER_TEMP/native-package" --cli-bundle target/ci-cli-release`
- Artifacts: `native-package-linux`; `${{ runner.temp }}/native-package/manifest.json`; `${{ runner.temp }}/native-package/wheel-build/report.json`; `${{ runner.temp }}/native-package/wheel-build/dist/*.whl`
- Release blockers: `platform native package and distinct observation product`
- Workflow markers: `ubuntu-24.04`; `python-version: "3.10"`; `if-no-files-found: error`; `native-cli-build-release`; `native-cli-release-linux`

### native_package_build_macos — native-package-build-macos

- Commands: `python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt`; `python scripts/ci_native_package.py build --output "$RUNNER_TEMP/native-package"`
- Artifacts: `native-package-macos`; `${{ runner.temp }}/native-package/manifest.json`; `${{ runner.temp }}/native-package/wheel-build/report.json`; `${{ runner.temp }}/native-package/wheel-build/dist/*.whl`
- Release blockers: `platform native package and distinct observation product`
- Workflow markers: `macos-latest`; `python-version: "3.13"`; `if-no-files-found: error`

### native_package_proof_linux — python-native-wheel-linux

- Commands: `python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt`; `python -m pip download --require-hashes --only-binary=:all: --no-deps -r .github/requirements/ci-native-python.txt --dest "$RUNNER_TEMP/native-wheelhouse"`; `python -B -m unittest discover -s scripts -p 'test_*native_python*.py' -v`; `python -B -m unittest discover -s scripts -p test_python_license_bundle.py -v`; `python scripts/ci_native_package.py test --package "$RUNNER_TEMP/native-package" --output "$RUNNER_TEMP/native-proof" --wheelhouse "$RUNNER_TEMP/native-wheelhouse"`
- Artifacts: `native-package-linux`; `${{ runner.temp }}/native-proof/ci-package-proof.json`; `${{ runner.temp }}/native-proof/proof/report.json`; `${{ runner.temp }}/native-proof/proof/integration.json`; `${{ runner.temp }}/native-proof/output-window-tests.json`; `native-python-wheel-${{ matrix.lane }}`
- Release blockers: `native Python wheel integrity and isolated installation`
- Workflow markers: `ubuntu-24.04`; `native-package-build-linux`; `if-no-files-found: error`; `python-version: "3.10"`; `python-version: "3.13"`

### native_package_proof_macos — python-native-wheel-macos

- Commands: `python -m pip install --require-hashes -r .github/requirements/ci-native-python-build.txt`; `python -m pip download --require-hashes --only-binary=:all: --no-deps -r .github/requirements/ci-native-python.txt --dest "$RUNNER_TEMP/native-wheelhouse"`; `python -B -m unittest discover -s scripts -p 'test_*native_python*.py' -v`; `python -B -m unittest discover -s scripts -p test_python_license_bundle.py -v`; `python scripts/ci_native_package.py test --package "$RUNNER_TEMP/native-package" --output "$RUNNER_TEMP/native-proof" --wheelhouse "$RUNNER_TEMP/native-wheelhouse"`
- Artifacts: `native-package-macos`; `${{ runner.temp }}/native-proof/ci-package-proof.json`; `${{ runner.temp }}/native-proof/proof/report.json`; `${{ runner.temp }}/native-proof/proof/integration.json`; `${{ runner.temp }}/native-proof/output-window-tests.json`; `native-python-wheel-py313_macos`
- Release blockers: `native Python wheel integrity and isolated installation`
- Workflow markers: `macos-latest`; `native-package-build-macos`; `if-no-files-found: error`; `python-version: "3.13"`

### native_python_wheel — python-native-wheel

- Commands: `python scripts/ci_native_package.py aggregate --reports target/native-package-results --output target/ci-native-package-coverage.json`
- Artifacts: `native-python-wheel-py*`; `native-python-wheel-coverage`; `target/ci-native-package-coverage.json`
- Release blockers: `all platform proofs and identical Linux wheel acceptance`
- Workflow markers: `always()`; `python-native-wheel-linux`; `python-native-wheel-macos`

### python_package_smoke — python-package

- Commands: `python -m build python`; `python scripts/release_dry_run_proof.py --rows 8 --iterations 1 --skip-clean-conda`
- Artifacts: `target/release-dry-run-proof/transcript.json`; `target/debug/shardloom`; `python/dist/*.whl`; `python/dist/*.tar.gz`; `release-local-smoke-evidence`
- Release blockers: `package/install smoke`; `local provenance dry run`
- Workflow markers: `Python and package smoke`

### dependency_security — dependency-security

- Commands: `python scripts/check_dependency_audit.py --release-gate --json-output target/dependency-audit-report.json`; `python scripts/check_security_posture.py`; `python scripts/release_provenance_dry_run.py`; `python scripts/check_release_security_gate.py`
- Artifacts: `target/dependency-audit-report.json`; `target/security-posture-report.json`; `target/release-provenance-dry-run`; `target/release-security-gate-report.json`
- Release blockers: `dependency/license audit`; `security posture`; `release security gate`
- Workflow markers: none

### release_runtime_core_evidence — release-runtime-core

- Commands: `python scripts/check_golden_workflows.py`; `python scripts/check_admitted_semantics_matrix.py`; `python scripts/check_release_architecture_tracker.py --allow-blocked`
- Artifacts: `target/golden-workflow-report.json`; `target/golden-workflows`; `target/admitted-semantics-matrix-report.json`; `target/admitted-semantics-matrix`; `target/release-architecture-tracker-report.json`
- Release blockers: `golden workflow validator`; `admitted semantics matrix`; `release architecture tracker`
- Workflow markers: none

### release_package_governance_evidence — release-package-governance

- Commands: `python scripts/merge_release_evidence_artifacts.py`; `python scripts/check_contribution_governance.py`; `python scripts/check_workspace_version_sources.py`; `python scripts/check_package_channel_readiness.py --require-local-evidence`
- Artifacts: `target/contribution-governance-report.json`; `target/workspace-version-source-report.json`; `target/package-channel-readiness-report.json`
- Release blockers: `contribution governance`; `workspace Rust/Vortex version source contract`; `package channel matrix`
- Workflow markers: `needs:`; `dependency-security`; `python-package`; `actions/download-artifact@v8`; `dependency-security-evidence`; `release-local-smoke-evidence`; `target/downloads`; `Merge package/governance input evidence`; `merge_release_evidence_artifacts.py`; `Workspace version source contract`

### release_user_surface_evidence — release-user-surface

- Commands: `python scripts/ci_build_artifact.py verify --product cli-debug --directory target/ci-cli-debug --restore target/debug/shardloom --replace-existing`; `python scripts/ci_build_artifact.py verify --product cli-release --directory target/ci-cli-release --restore target/release/shardloom --replace-existing`; `python scripts/check_python_user_surface_completion.py`; `python scripts/check_sql_python_dataframe_parity.py`; `python scripts/check_v1_front_door_runtime_scope.py`; `python scripts/check_v1_vortex_runtime_scope.py`; `python scripts/check_v1_source_prepared_state_scope.py`; `python scripts/check_v1_local_output_sink_scope.py`; `python scripts/check_v1_local_resource_safety.py --skip-build`; `python scripts/check_v1_observability_support.py --skip-build`; `python scripts/check_v1_example_replay.py --binary target/release/shardloom --profile-order release,debug --skip-build`; `python scripts/check_user_surface_runtime_gap_inventory.py`; `python scripts/check_user_surface_graduation_matrix.py`; `python scripts/check_runtime_gap_family_burn_down.py`; `python scripts/check_user_route_capability_report.py`
- Artifacts: `target/python-user-surface-completion-gate.json`; `target/sql-python-dataframe-parity-gate.json`; `target/v1-front-door-runtime-scope-report.json`; `target/v1-vortex-runtime-scope-report.json`; `target/v1-source-prepared-state-scope-report.json`; `target/v1-local-output-sink-scope-report.json`; `target/v1-local-resource-safety-report.json`; `target/v1-observability-support-report.json`; `target/v1-example-replay-report.json`; `target/v1-example-replay`; `target/user-surface-runtime-gap-inventory.json`; `target/user-surface-graduation-matrix.json`; `target/runtime-gap-family-burn-down.json`; `target/user-route-capability-report.json`; `native-cli-debug-linux`; `native-cli-release-linux`
- Release blockers: `Python user-surface completion gate`; `SQL/Python/DataFrame parity gate`; `v1 front-door runtime scope gate`; `v1 Vortex runtime scope gate`; `v1 SourceState/prepared-state scope gate`; `v1 local resource-safety gate`; `v1 observability/supportability gate`; `user-surface runtime gap inventory`; `v1 example replay gate`; `user-surface graduation matrix`; `runtime gap family burn-down`; `user route capability report`
- Workflow markers: `needs:`; `python-package`; `release-runtime-core`; `actions/download-artifact@v8`; `release-local-smoke-evidence`; `release-runtime-core-evidence`; `target/downloads`; `Merge local package smoke evidence`; `merge_release_evidence_artifacts.py`; `native-cli-build-debug`; `native-cli-build-release`

### release_readiness_reports — release-readiness

- Commands: `python scripts/ci_execution_plan.py summary --plan target/downloads/ci-execution-plan/ci-execution-plan.json --output target/ci-execution-summary.json`; `python scripts/merge_release_evidence_artifacts.py`; `python scripts/final_release_rehearsal.py --allow-blocked`; `python scripts/check_production_usability_gate.py`; `python scripts/check_v1_api_schema_stability.py`; `python scripts/check_v1_correctness_conformance.py`; `python scripts/check_v1_security_ci_hardening.py`; `python scripts/check_v1_release_boundary.py`; `python scripts/check_production_certification_gate.py`; `python scripts/check_release_readiness.py`; `python scripts/check_finished_product_readiness.py`
- Artifacts: `target/dependency-audit-report.json`; `target/security-posture-report.json`; `target/release-dry-run-proof`; `target/release-provenance-dry-run`; `target/debug/shardloom`; `python/dist`; `target/python-test-evidence.json`; `target/release-security-gate-report.json`; `target/contribution-governance-report.json`; `target/package-channel-readiness-report.json`; `target/golden-workflow-report.json`; `target/golden-workflows`; `target/admitted-semantics-matrix-report.json`; `target/admitted-semantics-matrix`; `target/release-architecture-tracker-report.json`; `target/final-release-rehearsal`; `target/public-status-docs-report.json`; `target/website-readiness-report.json`; `target/workspace-version-source-report.json`; `target/production-usability-gate.json`; `target/v1-api-schema-stability-report.json`; `target/v1-example-replay-report.json`; `target/v1-example-replay`; `target/v1-correctness-conformance-report.json`; `target/v1-security-ci-hardening-report.json`; `target/v1-release-boundary-report.json`; `target/production-certification-gate.json`; `target/python-user-surface-completion-gate.json`; `target/sql-python-dataframe-parity-gate.json`; `target/v1-front-door-runtime-scope-report.json`; `target/v1-vortex-runtime-scope-report.json`; `target/v1-source-prepared-state-scope-report.json`; `target/v1-local-output-sink-scope-report.json`; `target/v1-local-resource-safety-report.json`; `target/v1-observability-support-report.json`; `target/user-surface-runtime-gap-inventory.json`; `target/user-surface-graduation-matrix.json`; `target/runtime-gap-family-burn-down.json`; `target/user-route-capability-report.json`; `target/ci-gate-matrix-report.json`; `target/ci-work-shaping-plan.json`; `target/ci-work-shaping-changed-files.txt`; `target/hard-release-readiness-gate.json`; `target/finished-product-readiness-report.json`; `target/ci-execution-summary.json`; `ci-execution-summary`
- Release blockers: `final rehearsal`; `production usability gate`; `v1 API/schema stability gate`; `v1 correctness/conformance gate`; `v1 security/CI hardening gate`; `v1 release boundary firewall`; `production certification gate`; `hard release readiness gate`; `finished product readiness gate`; `release readiness artifact aggregation`
- Workflow markers: `needs:`; `ci-work-shaping`; `ci-gate-matrix`; `dependency-security`; `python-tests`; `python-package`; `release-runtime-core`; `release-package-governance`; `release-user-surface`; `website-docs`; `actions/download-artifact@v8`; `ci-work-shaping-evidence`; `dependency-security-evidence`; `release-local-smoke-evidence`; `python-test-evidence`; `target/downloads`; `target/downloads/ci-work-shaping-evidence`; `ci-gate-matrix-report`; `release-runtime-core-evidence`; `release-package-governance-evidence`; `release-user-surface-evidence`; `website-docs-evidence`; `Merge downloaded release evidence`; `Verify downloaded release evidence`; `continue-on-error: true`; `Finished product readiness gate`; `always()`; `needs.ci-work-shaping.outputs.full_validation == 'true'`; `native-tests`; `python-native-wheel`; `native-cli-build-debug`; `native-cli-build-release`

### website_docs_validation — website-docs

- Commands: `npm audit --audit-level=low`; `npm run build`; `npm run check`; `python scripts/check_public_status_docs.py`; `python -m unittest discover -s scripts -p test_website_links.py`; `python scripts/check_website_readiness.py`; `node scripts/validate_static_assets.cjs`
- Artifacts: `target/public-status-docs-report.json`; `target/website-readiness-report.json`
- Release blockers: `website build`; `public status docs`; `docs/status generated assets`
- Workflow markers: none

### ci_gate_matrix_contract — ci-gate-matrix

- Commands: `python scripts/check_ci_gate_matrix.py`; `python -B -m unittest discover -s scripts -p 'test_ci_*.py' -v`
- Artifacts: `target/ci-gate-matrix-report.json`
- Release blockers: `CI matrix drift contract`
- Workflow markers: none


## Compatibility and provenance contracts

The MSRV toolchain is derived from the workspace manifest. Python compatibility covers 3.10 through
3.13 on Ubuntu, plus macOS and Windows. Windows compiles and invokes the bundled native CLI and
checks native resource admission refusal.

```text
python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"
SHARDLOOM_RUST_MSRV_TOOLCHAIN
SHARDLOOM_RUST_MSRV_LANE
rustup toolchain install "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"
rustup default "$SHARDLOOM_RUST_MSRV_TOOLCHAIN"
key: rust-msrv-${{ hashFiles('Cargo.toml') }}
python-version: "3.10"
python-version: "3.11"
python-version: "3.12"
python-version: "3.13"
key: windows-release-user-surfaces
cargo build --release -p shardloom-cli --bin shardloom --features release-user-surfaces
./target/release/shardloom.exe --version
cargo test --release -p shardloom-cli --test non_unix_resource_admission --features release-user-surfaces
retention-days: 14
rust_msrv_source=Cargo.toml#[workspace.package].rust-version
upstream_vortex_manifest_source=Cargo.toml#[workspace.dependencies].vortex
upstream_vortex_provider_source=Cargo.toml#[workspace.dependencies].vortex via shardloom-vortex/build.rs
```

`python scripts/check_finished_product_readiness.py` remains part of the release boundary. Clean
Conda environment proof, real publication, tag creation, signing-key use, and package upload remain
skipped or unauthorized. Placeholder artifact paths do not prove real Vortex output payload
coverage. Public performance claims require CG-5 correctness and CG-6 reproducible benchmark
evidence.
