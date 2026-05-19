// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("contract test crate should live under the repo root")
        .to_path_buf()
}

fn read_repo_file(path: impl AsRef<Path>) -> String {
    let path = repo_root().join(path);
    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn planned_gar_slices(plan: &str) -> Vec<String> {
    let lines = plan.lines().collect::<Vec<_>>();
    let mut slices = Vec::new();
    let mut start = None;
    for (index, line) in lines.iter().enumerate() {
        if line.starts_with("- [ ] GAR-") {
            if let Some(previous_start) = start {
                slices.push(lines[previous_start..index].join("\n"));
            }
            start = Some(index);
        }
    }
    if let Some(previous_start) = start {
        slices.push(lines[previous_start..].join("\n"));
    }
    slices
}

fn completed_gar_session_count(completed_ledger: &str) -> usize {
    completed_ledger
        .lines()
        .filter(|line| line.starts_with("- [x] Session label: GAR-"))
        .count()
}

fn yaml_top_level_section_item_count(content: &str, section: &str) -> usize {
    let section_header = format!("{section}:");
    let mut in_section = false;
    let mut count = 0;
    for line in content.lines() {
        if line == section_header {
            in_section = true;
            continue;
        }
        if in_section && !line.starts_with(' ') && !line.is_empty() {
            break;
        }
        if in_section && line.starts_with("  - id:") {
            count += 1;
        }
    }
    count
}

#[test]
fn python_package_metadata_is_discoverable_without_runtime_dependencies() {
    let pyproject = read_repo_file("python/pyproject.toml");
    assert!(pyproject.contains("name = \"shardloom\""));
    assert!(
        pyproject.contains("Pre-release Python client for ShardLoom, a Vortex-first no-fallback evidence-certified local compute engine")
    );
    for keyword in [
        "analytics",
        "columnar",
        "vortex",
        "data-engine",
        "etl",
        "benchmark",
        "no-fallback",
        "rust",
        "python",
    ] {
        assert!(
            pyproject.contains(keyword),
            "missing PyPI keyword {keyword}"
        );
    }
    assert!(pyproject.contains("license = \"Apache-2.0\""));
    assert!(pyproject.contains("license-files = [\"LICENSE\", \"NOTICE\"]"));
    assert!(!pyproject.contains("License :: OSI Approved :: Apache Software License"));
    assert!(pyproject.contains("Topic :: Database"));
    assert!(pyproject.contains("Topic :: Scientific/Engineering :: Information Analysis"));
    assert!(pyproject.contains("Homepage = \"https://shardloom.io\""));
    assert!(pyproject.contains("dependencies = []"));

    let readme = read_repo_file("python/README.md");
    assert!(readme.contains("Vortex-native"));
    assert!(readme.contains("no-fallback"));
    assert!(readme.contains("evidence-certified local compute engine"));
}

#[test]
fn cargo_metadata_marks_current_workspace_crates_internal() {
    let workspace = read_repo_file("Cargo.toml");
    for expected in [
        "description = \"Pre-release Vortex-first no-fallback local compute engine",
        "homepage = \"https://shardloom.io\"",
        "documentation = \"https://github.com/depsilon/shardloom/tree/main/docs\"",
        "readme = \"README.md\"",
        "keywords = [\"analytics\", \"columnar\", \"vortex\", \"etl\", \"no-fallback\"]",
        "categories = [\"command-line-utilities\", \"database\", \"encoding\", \"science\"]",
    ] {
        assert!(
            workspace.contains(expected),
            "missing workspace metadata {expected}"
        );
    }

    for manifest in [
        "shardloom-core/Cargo.toml",
        "shardloom-plan/Cargo.toml",
        "shardloom-exec/Cargo.toml",
        "shardloom-vortex/Cargo.toml",
        "shardloom-cli/Cargo.toml",
        "shardloom-contract-tests/Cargo.toml",
    ] {
        let content = read_repo_file(manifest);
        assert!(content.contains("description.workspace = true"));
        assert!(content.contains("homepage.workspace = true"));
        assert!(content.contains("documentation.workspace = true"));
        assert!(content.contains("keywords.workspace = true"));
        assert!(content.contains("categories.workspace = true"));
        assert!(content.contains("publish = false"));
    }
}

#[test]
fn optimized_build_profiles_preserve_portable_release_boundary() {
    let workspace = read_repo_file("Cargo.toml");
    for required in [
        "[profile.release-lto]",
        "inherits = \"release\"",
        "lto = \"thin\"",
        "codegen-units = 1",
        "[profile.release-pgo]",
        "inherits = \"release-lto\"",
        "[profile.release-native-benchmark]",
    ] {
        assert!(
            workspace.contains(required),
            "missing optimized Cargo profile field {required}"
        );
    }
    assert!(
        !workspace.contains("target-cpu=native") && !workspace.contains("target-cpu = \"native\""),
        "portable Cargo profiles must not encode target-cpu=native"
    );

    let benchmark = read_repo_file("benchmarks/traditional_analytics/run.py");
    for required in [
        "BUILD_PROFILE_FIELDS",
        "shardloom.traditional_analytics.build_profile.v1",
        "release-lto",
        "release-pgo",
        "release-native-benchmark",
        "-Ctarget-cpu=native",
        "SHARDLOOM_PGO_PROFILE",
        "release-native-benchmark is host-native and benchmark-only",
        "build_profile_fallback_attempted",
        "build_profile_external_engine_invoked",
        "build_profile_claim_gate_status",
    ] {
        assert!(
            benchmark.contains(required),
            "missing build-profile benchmark contract text {required}"
        );
    }

    let hard_gate = read_repo_file("docs/release/hard-release-readiness-gate.md");
    for required in [
        "release-lto",
        "release-pgo",
        "release-native-benchmark",
        "target-cpu=native",
        "benchmark-only",
        "cannot satisfy public release/package evidence",
        "profile-generate",
        "llvm-profdata",
        "profile-use",
    ] {
        assert!(
            hard_gate.contains(required),
            "missing hard release build-profile boundary text {required}"
        );
    }

    let pgo_script = read_repo_file("scripts/build_shardloom_pgo.py");
    for required in [
        "shardloom.pgo_build_helper.v1",
        "profile-generate",
        "llvm-profdata",
        "-Cprofile-use",
        "SHARDLOOM_PGO_PROFILE",
        "benchmark_only_build",
        "portable_release_artifact",
        "fallback_attempted",
        "external_engine_invoked",
    ] {
        assert!(
            pgo_script.contains(required),
            "missing PGO helper script field {required}"
        );
    }
}

#[test]
fn bayesian_performance_layout_advisor_remains_report_only() {
    let benchmark = read_repo_file("benchmarks/traditional_analytics/run.py");
    for required in [
        "BAYESIAN_ADVISOR_SCHEMA_VERSION",
        "shardloom.traditional_analytics.bayesian_advisor.v1",
        "gar-perf-1d.report_only.v1",
        "BAYESIAN_ADVISOR_FIELDS",
        "bayesian_advisor_confidence",
        "bayesian_advisor_uncertainty_reason",
        "bayesian_advisor_input_evidence_refs",
        "bayesian_advisor_claim_gate_status",
        "bayesian_advisor_runtime_decision_applied",
        "bayesian_advisor_fallback_attempted",
        "bayesian_advisor_external_engine_invoked",
        "def bayesian_advisor_contract_metadata(",
        "def bayesian_advisor_contract(",
        "def render_bayesian_advisor_contract(",
        "BAYESIAN_CLAIM_CONFIDENCE_SCHEMA_VERSION",
        "shardloom.traditional_analytics.bayesian_claim_confidence.v1",
        "gar-novel-1d.bayesian_claim_confidence",
        "gar-novel-1d.report_only.v1",
        "BAYESIAN_CLAIM_CONFIDENCE_FIELDS",
        "bayesian_claim_confidence_posterior_runtime_distribution",
        "bayesian_claim_confidence_credible_interval",
        "bayesian_claim_confidence_probability_of_regression",
        "bayesian_claim_confidence_minimum_iterations_for_claim_grade",
        "bayesian_claim_confidence_input_evidence_refs",
        "bayesian_claim_confidence_claim_blocking_allowed",
        "bayesian_claim_confidence_claim_upgrade_allowed",
        "bayesian_claim_confidence_runtime_decision_applied",
        "bayesian_claim_confidence_layout_decision_applied",
        "bayesian_claim_confidence_benchmark_recomputed",
        "bayesian_claim_confidence_fallback_attempted",
        "bayesian_claim_confidence_external_engine_invoked",
        "bayesian_claim_confidence_claim_gate_status",
        "def bayesian_claim_confidence_report(",
        "def render_bayesian_claim_confidence_report(",
        "report_only_not_fit",
        "advisory_only_not_claim_grade",
        "advisory_only",
    ] {
        assert!(
            benchmark.contains(required),
            "missing Bayesian advisor benchmark contract text {required}"
        );
    }

    let doc = read_repo_file("docs/architecture/bayesian-performance-layout-advisor.md");
    for required in [
        "Status: implemented report-only contract for GAR-PERF-1D",
        "Decision: `wrap_vortex_concept`",
        "bayesian_advisor_runtime_decision_applied=false",
        "bayesian_advisor_claim_gate_status=advisory_only",
        "bayesian_advisor_fallback_attempted=false",
        "bayesian_advisor_external_engine_invoked=false",
        "is not a fitted posterior model",
        "Future Bayesian output can block a claim",
        "claim valid by",
    ] {
        assert!(
            doc.contains(required),
            "missing Bayesian advisor doc field {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(plan.contains("docs/architecture/bayesian-performance-layout-advisor.md"));
    assert!(!plan.contains("- [ ] GAR-PERF-1D Bayesian performance"));
    assert!(!plan.contains("- [ ] GAR-NOVEL-1D Bayesian claim-confidence"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("- [x] `GAR-PERF-1`"));
    assert!(gar.contains("- [x] `GAR-NOVEL-1D`"));
}

#[test]
fn gar_0022_a_substrait_report_only_contract_remains_claim_safe() {
    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0022-A Substrait import/export report-only contract"));
    assert!(plan.contains("docs/architecture/substrait-report-only-contract.md"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    for required in [
        "GAR-0022-A Substrait import/export report-only contract",
        "shardloom.substrait_report_only_contract.v1",
        "substrait_report_contract_support_status=report_only",
        "substrait_dependency_status=not_added",
        "substrait_import_parser_status=not_implemented",
        "substrait_export_serializer_status=not_implemented",
        "substrait_imported_plan_execution_allowed=false",
        "substrait_external_engine_invoked=false",
        "substrait_fallback_attempted=false",
        "substrait_claim_gate_status=not_claim_grade",
        "Substrait compatibility",
        "imported-plan execution",
        "external engine fallback",
    ] {
        assert!(
            completed.contains(required),
            "missing GAR-0022-A completed ledger field {required}"
        );
    }

    let doc = read_repo_file("docs/architecture/substrait-report-only-contract.md");
    for required in [
        "Status: implemented report-only contract for `GAR-0022-A`",
        "substrait_report_contract_support_status=report_only",
        "substrait_dependency_status=not_added",
        "substrait_imported_plan_execution_allowed=false",
        "substrait_external_engine_invoked=false",
        "substrait_fallback_attempted=false",
        "substrait_claim_gate_status=not_claim_grade",
        "No Substrait dependency is added",
        "No imported plan is executed",
        "It may not claim Substrait compatibility",
    ] {
        assert!(
            doc.contains(required),
            "missing Substrait report-only doc field {required}"
        );
    }

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("- [x] `GAR-0022-A`"));
    assert!(gar.contains("Real Substrait parser/exporter support"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("GAR-0022-A"));
    assert!(traceability.contains("shardloom.substrait_report_only_contract.v1"));
}

#[test]
fn dependency_audit_scaffolding_documents_policy_and_tools() {
    let deny = read_repo_file("deny.toml");
    for allowed in [
        "Apache-2.0",
        "MIT",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "0BSD",
        "CC0-1.0",
        "Unicode-3.0",
        "Zlib",
    ] {
        assert!(
            deny.contains(allowed),
            "missing cargo-deny allow license {allowed}"
        );
    }
    assert!(deny.contains("RUSTSEC-2024-0436"));
    assert!(deny.contains("Transitive unmaintained paste 1.0.15"));
    assert!(deny.contains("multiple-versions = \"warn\""));
    assert!(deny.contains("unknown-registry = \"deny\""));
    assert!(deny.contains("unknown-git = \"deny\""));
    for denied in [
        "GPL-3.0-only",
        "LGPL-3.0-only",
        "AGPL-3.0-only",
        "SSPL-1.0",
        "BUSL-1.1",
    ] {
        assert!(
            deny.contains(denied),
            "missing cargo-deny denied license {denied}"
        );
    }

    let script = read_repo_file("scripts/check_dependency_audit.py");
    assert!(script.contains("cargo deny check licenses advisories bans sources"));
    assert!(script.contains("cargo audit"));
    assert!(script.contains("--release-gate"));
    assert!(script.contains("strict missing tools"));
    assert!(script.contains("--include-python-packaging"));
    assert!(script.contains("not as a ShardLoom runtime dependency assumption"));
    assert!(script.contains("PYTHON_PROJECT"));
    assert!(script.contains("-m\", \"pip_audit\", str(PYTHON_PROJECT)"));
    assert!(script.contains("shardloom.dependency_audit_report.v1"));
    assert!(script.contains("DependencyAuditReport"));
    assert!(script.contains("fallback_dependency_absent"));
    assert!(script.contains("FORBIDDEN_FALLBACK_DEPENDENCIES"));
    assert!(script.contains("benchmark_only_external_baselines"));
    assert!(script.contains("target_tables"));
    assert!(script.contains(" @ "));
    assert!(script.contains("skipped_missing"));
    assert!(script.contains("missing"));

    let dry_run = read_repo_file("scripts/release_dry_run_proof.py");
    assert!(dry_run.contains("build_python_artifacts"));
    assert!(dry_run.contains("venv"));
    assert!(dry_run.contains("pip"));
    assert!(dry_run.contains("--no-index"));
    assert!(dry_run.contains("SHARDLOOM_BIN"));
    assert!(dry_run.contains("ShardLoomClient.from_env()"));
    assert!(dry_run.contains("smoke_check()"));
    assert!(dry_run.contains("generated_source_user_rows_local_output_smoke"));
    assert!(dry_run.contains("generated_source_range_local_output_smoke"));
    assert!(dry_run.contains("ctx.from_rows(["));
    assert!(dry_run.contains("ctx.range(0, 8"));
    assert!(dry_run.contains("generated_source_certificate_status"));
    assert!(dry_run.contains("output_native_io_certificate_status"));
    assert!(dry_run.contains("external_engine_invoked"));
    assert!(dry_run.contains("clean_conda_env_install_status"));
    assert!(dry_run.contains("--require-clean-conda"));
    assert!(dry_run.contains("mamba"));
    assert!(dry_run.contains("micromamba"));
    assert!(dry_run.contains("examples/local-python-smoke/run.py"));
    assert!(dry_run.contains("examples/local-vortex-benchmark/run.py"));
    assert!(dry_run.contains("publication_attempted"));
    assert!(dry_run.contains("tag_created"));
    assert!(dry_run.contains("secrets_required"));
    assert!(dry_run.contains("fallback_engine_dependency_added"));
    assert!(dry_run.contains("public_package_release_claim_allowed"));
    assert!(dry_run.contains("generated_output_proof_distinct_from_no_dataset_smoke"));
    assert!(dry_run.contains("prepared_native_benchmark_smoke_performed"));
    assert!(dry_run.contains("scripts/release_provenance_dry_run.py"));
    assert!(dry_run.contains("provenance_dry_run_performed"));
    assert!(dry_run.contains("sbom_checksum_manifest_generated"));

    let provenance_script = read_repo_file("scripts/release_provenance_dry_run.py");
    for required in [
        "shardloom.supply_chain_release_evidence.v1",
        "shardloom-rust-workspace.cdx.json",
        "shardloom-python-artifacts.cdx.json",
        "shardloom-cli-binary.cdx.json",
        "checksums.sha256",
        "workflow-policy-snapshot.json",
        "publication_attempted",
        "tag_created",
        "secrets_required",
        "fallback_engine_dependency_added",
        "waived_until_real_publication",
    ] {
        assert!(
            provenance_script.contains(required),
            "missing release provenance script field {required}"
        );
    }

    let posture_script = read_repo_file("scripts/check_security_posture.py");
    for required in [
        "shardloom.open_source_security_posture_report.v1",
        ".github/workflows/codeql-analysis.yml",
        ".github/workflows/scorecard.yml",
        ".github/dependabot.yml",
        "docs/security/open-source-security-posture.md",
        "publication_attempted",
        "fallback_attempted",
        "external_engine_invoked",
    ] {
        assert!(
            posture_script.contains(required),
            "missing security posture script field {required}"
        );
    }

    let security_gate_script = read_repo_file("scripts/check_release_security_gate.py");
    for required in [
        "shardloom.release_security_gate_report.v1",
        "SecurityThreatModelReport",
        "DependencyAuditReport",
        "SupplyChainReleaseEvidence",
        "RuntimeInputSafetyReport",
        "OpenSourceSecurityPostureReport",
        "KnownUnsupportedPathsReport",
        "public_release_claim_allowed",
        "fallback_attempted",
        "external_engine_invoked",
        "--allow-blocked",
    ] {
        assert!(
            security_gate_script.contains(required),
            "missing release security gate script field {required}"
        );
    }

    let readiness_script = read_repo_file("scripts/check_release_readiness.py");
    for required in [
        "shardloom.hard_release_readiness_gate.v1",
        "release-dry-run-proof/transcript.json",
        "release-security-gate-report.json",
        "clean_conda_env_install_status",
        "public_release_claim_allowed",
        "package-channel readiness matrix",
        "package-channel-readiness-matrix.json",
        "python scripts/check_package_channel_readiness.py",
        "feature/build matrix execution evidence",
        "typed_envelope_compatibility",
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace --all-targets",
        "python -m build python",
        "global-architecture-gate",
    ] {
        assert!(
            readiness_script.contains(required),
            "missing hard release readiness script field {required}"
        );
    }

    let foundry_script = read_repo_file("scripts/foundry_proof_of_use.py");
    for required in [
        "shardloom.foundry_proof_of_use_report.v1",
        "shardloom.foundry_generated_output_fanout_posture.v1",
        "shardloom.foundry_generated_output_boundary.v1",
        "shardloom.foundry_scale_proof_boundary.v1",
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "shardloom.foundry_dev_stack_starter_kit.v1",
        "foundry_dev_stack_starter_kit_status",
        "foundry_dev_stack_starter_kit_ref",
        "foundry_dev_stack_starter_kit_schema_version",
        "foundry_generated_output_fanout_posture",
        "foundry_generated_output_fanout_status",
        "foundry_generated_output_boundary",
        "foundry_generated_output_boundary_status",
        "foundry_output_api_required",
        "foundry_output_api_invoked",
        "foundry_result_dataset_written",
        "foundry_evidence_dataset_written",
        "foundry_scale_proof_boundary",
        "foundry_scale_proof_boundary_status",
        "foundry_package_proof_boundary_matrix",
        "foundry_package_proof_boundary_matrix_status",
        "foundry_package_proof_boundary_matrix_ref",
        "shardloom_foundry_package",
        "artifact_repository_publication",
        "foundry_service_invocation",
        "compute_module_surface",
        "virtual_table_native_execution",
        "dataset_transaction_runtime",
        "f10_workload_certified_deployment",
        "generated_output_execution_performed",
        "generated_source_certificate_status",
        "output_native_io_certificate_status",
        "direct_s3_write_invoked",
        "foundry_runtime_invoked",
        "foundry_compute_invoked",
        "foundry_spark_invoked",
        "foundry_input_dataset_count",
        "foundry_output_dataset_count",
        "staged_input_bytes",
        "shardloom_execution_mode",
        "output_evidence_dataset_written",
        "not_foundry_scale_grade",
        "snowflake_databricks_bigquery_invoked",
        "direct_s3_read_invoked",
        "object_store_read_invoked",
        "object_store_commit_invoked",
        "public_foundry_generated_output_claim_allowed",
        "fallback_attempted",
        "external_engine_invoked",
        "certificate_metrics_dataset_output_written",
        "supported_local_native_execution_smoke_performed",
        "public_foundry_claim_allowed",
        "local_foundry_style_proof_claim_allowed",
    ] {
        assert!(
            foundry_script.contains(required),
            "missing Foundry proof script field {required}"
        );
    }

    let policy = read_repo_file("docs/legal/dependency-audit.md");
    assert!(policy.contains("Runtime Versus Benchmark-Only Dependencies"));
    assert!(policy.contains("Vortex Dependency Boundaries"));
    assert!(policy.contains("must not"));
    assert!(policy.contains("execute unsupported ShardLoom work"));
    assert!(policy.contains("GPL, LGPL, AGPL, SSPL, BUSL"));
    assert!(policy.contains("python scripts/check_dependency_audit.py --release-gate"));
    assert!(policy.contains("DependencyAuditReport"));
    assert!(policy.contains("Current Release-Gate Exceptions"));
    assert!(policy.contains("0BSD"));
    assert!(policy.contains("CC0-1.0"));
    assert!(policy.contains("RUSTSEC-2024-0436"));

    let release_gate = read_repo_file("docs/security/dependency-audit-release-gate.md");
    for required in [
        "python scripts\\check_dependency_audit.py --release-gate",
        "cargo deny check licenses advisories bans sources",
        "cargo audit",
        "pip-audit",
        "shardloom.dependency_audit_report.v1",
        "cargo_deny_status",
        "cargo_audit_status",
        "pip_audit_status",
        "fallback_dependency_absent",
        "external_baseline_only",
        "not ShardLoom runtime dependencies",
        "Current Waivers",
        "RUSTSEC-2024-0436",
        "0BSD",
        "CC0-1.0",
    ] {
        assert!(
            release_gate.contains(required),
            "missing release gate doc field {required}"
        );
    }

    let benchmark_requirements =
        read_repo_file("benchmarks/traditional_analytics/requirements.txt");
    for benchmark_only in [
        "pandas",
        "polars",
        "duckdb",
        "datafusion",
        "dask",
        "pyspark",
    ] {
        assert!(benchmark_requirements.contains(benchmark_only));
        assert!(
            !read_repo_file("python/pyproject.toml").contains(&format!("{benchmark_only}>")),
            "{benchmark_only} must not become a Python runtime dependency"
        );
    }
}

#[test]
fn release_package_docs_workflow_and_examples_are_present() {
    let workflow = read_repo_file(".github/workflows/pypi-publish-draft.yml");
    assert!(workflow.contains("workflow_dispatch"));
    assert!(workflow.contains("environment: pypi"));
    assert!(workflow.contains("id-token: write"));
    assert!(workflow.contains("pypa/gh-action-pypi-publish"));
    assert!(!workflow.to_ascii_lowercase().contains("password:"));
    assert!(!workflow.to_ascii_lowercase().contains("api-token:"));
    assert!(!workflow.to_ascii_lowercase().contains("pypi-token"));

    let package_names = read_repo_file("docs/release/package-name-readiness.md");
    assert!(package_names.contains("PyPI: `shardloom`"));
    assert!(package_names.contains("`shardloom-cli`, `shardloom-python`, `shardloom`"));
    assert!(package_names.contains("`shardloom-protocol`, `shardloom-client`"));
    assert!(package_names.contains("package-channel-readiness-matrix.md"));
    assert!(package_names.contains("shardloom.package_channel_readiness_matrix.v1"));
    assert!(package_names.contains("TestPyPI Dry Run"));
    assert!(package_names.contains("Do not publish current internal crates"));
    assert!(package_names.contains("publish-approved"));
    assert!(package_names.contains("scripts\\release_dry_run_proof.py"));

    let package_channel_doc = read_repo_file("docs/release/package-channel-readiness-matrix.md");
    for required in [
        "shardloom.package_channel_readiness_matrix.v1",
        "python scripts\\check_package_channel_readiness.py",
        "GitHub pre-release",
        "TestPyPI",
        "PyPI Trusted Publisher/OIDC",
        "Homebrew tap",
        "Scoop",
        "winget",
        "conda-forge",
        "GHCR container",
        "crates.io future",
        "Internal Rust crates remain unpublished",
        "Package access does not imply production readiness",
        "runtime fallback dependency",
    ] {
        assert!(
            package_channel_doc.contains(required),
            "missing package channel readiness doc field {required}"
        );
    }

    let package_channel_matrix =
        read_repo_file("docs/release/package-channel-readiness-matrix.json");
    for required in [
        "shardloom.package_channel_readiness_matrix.v1",
        "\"public_package_release_claim_allowed\": false",
        "\"publication_attempted\": false",
        "\"tag_created\": false",
        "\"secrets_required\": false",
        "\"fallback_engine_dependency_added\": false",
        "\"external_engine_runtime_dependency_added\": false",
        "\"package_access_implies_production_readiness\": false",
        "\"github_prerelease\"",
        "\"testpypi\"",
        "\"pypi\"",
        "\"homebrew_tap\"",
        "\"scoop\"",
        "\"winget\"",
        "\"conda_forge\"",
        "\"ghcr_container\"",
        "\"crates_io_future\"",
        "Trusted Publisher/OIDC",
        "\"internal_crates_publish_allowed\": false",
        "\"runtime_fallback_dependency_allowed\": false",
        "future stable public protocol/client crates",
    ] {
        assert!(
            package_channel_matrix.contains(required),
            "missing package channel readiness matrix field {required}"
        );
    }

    let package_channel_script = read_repo_file("scripts/check_package_channel_readiness.py");
    for required in [
        "shardloom.package_channel_readiness_matrix.v1",
        "shardloom.package_channel_readiness_report.v1",
        "EXPECTED_CHANNEL_IDS",
        "github_prerelease",
        "testpypi",
        "pypi",
        "homebrew_tap",
        "scoop",
        "winget",
        "conda_forge",
        "ghcr_container",
        "crates_io_future",
        "Trusted Publisher",
        "OIDC",
        "no internal crate publication",
        "public_package_release_claim_allowed",
        "runtime_fallback_dependency_allowed",
        "external_engine_runtime_dependency_allowed",
        "publication_attempted",
        "tag_created",
        "secrets_required",
    ] {
        assert!(
            package_channel_script.contains(required),
            "missing package channel readiness script field {required}"
        );
    }

    let sbom = read_repo_file("docs/release/sbom-generation-plan.md");
    assert!(sbom.contains("Rust Workspace SBOM"));
    assert!(sbom.contains("Python Wheel And Sdist SBOM"));
    assert!(sbom.contains("Release Binary SBOM"));
    assert!(sbom.contains("Optional OCI Image SBOM"));

    let audit = read_repo_file("docs/release/package-metadata-audit.md");
    assert!(audit.contains("Target package name: `shardloom`"));
    assert!(audit.contains("Current workspace crates are internal and marked `publish = false`"));
    assert!(audit.contains("Target recipe names"));

    for doc in [
        "docs/getting-started/install.md",
        "docs/getting-started/first-10-minutes.md",
        "docs/getting-started/examples.md",
        "docs/getting-started/certified-local-workload.md",
        "docs/benchmarks/baseline-comparison-boundary.md",
        "docs/benchmarks/local-taxonomy-benchmark.md",
        "docs/release/github-topic-recommendations.md",
        "docs/release/release-dry-run-proof.md",
        "docs/release/release-provenance-dry-run.md",
        "docs/release/known-unsupported-paths.md",
        "docs/release/hard-release-readiness-gate.md",
        "docs/release/first-10-minutes-smoke-snapshot.md",
        "docs/security/release-security-gate.md",
        "docs/security/open-source-security-posture.md",
        "docs/foundry/integration-pack-readiness.md",
        "docs/foundry/proof-of-use-certification.md",
        "docs/foundry/package-proof-boundary-matrix.md",
        "docs/foundry/package-proof-boundary-matrix.json",
        "examples/local-python-smoke/README.md",
        "examples/local-python-smoke/run.py",
        "examples/local-vortex-benchmark/README.md",
        "examples/local-vortex-benchmark/run.py",
        "examples/foundry-lightweight-transform/README.md",
        "examples/foundry-lightweight-transform/run.py",
    ] {
        assert!(repo_root().join(doc).exists(), "missing {doc}");
    }
}

#[test]
fn readme_links_website_and_first_user_docs() {
    let readme = read_repo_file("README.md");
    assert!(readme.contains("https://shardloom.io"));
    assert!(readme.contains("docs/getting-started/install.md"));
    assert!(readme.contains("docs/getting-started/first-10-minutes.md"));
    assert!(readme.contains("docs/getting-started/examples.md"));
    assert!(readme.contains("docs/getting-started/certified-local-workload.md"));
    assert!(readme.contains("docs/benchmarks/local-taxonomy-benchmark.md"));
    assert!(readme.contains("docs/benchmarks/baseline-comparison-boundary.md"));
}

#[test]
fn release_dry_run_docs_describe_clean_venv_and_no_publication_proof() {
    let proof = read_repo_file("docs/release/release-dry-run-proof.md");
    assert!(proof.contains("clean virtual environment"));
    assert!(proof.contains("pip --no-index <wheel>"));
    assert!(proof.contains("exact local wheel artifact"));
    assert!(proof.contains("clean venv interpreter"));
    assert!(proof.contains("SHARDLOOM_BIN"));
    assert!(proof.contains("examples/local-vortex-benchmark"));
    assert!(proof.contains("publication_attempted"));
    assert!(proof.contains("fallback_engine_dependency_added"));
    assert!(proof.contains("release_provenance_dry_run"));
    assert!(proof.contains("provenance_dry_run_performed"));
    assert!(proof.contains("sbom_checksum_manifest_generated"));
    assert!(proof.contains("generated_source_user_rows_smoke_performed=true"));
    assert!(proof.contains("generated_source_range_smoke_performed=true"));
    assert!(proof.contains("prepared_native_benchmark_smoke_performed=true"));
    assert!(proof.contains("generated_source_certificate_status=present"));
    assert!(proof.contains("output_native_io_certificate_status=certified_local_file_sink"));
    assert!(proof.contains("external_engine_invoked=False"));
    assert!(proof.contains("clean_conda_env_install_status"));
    assert!(proof.contains("--require-clean-conda"));

    let snapshot = read_repo_file("docs/release/first-10-minutes-smoke-snapshot.md");
    assert!(snapshot.contains("schema_version: shardloom.release_dry_run_proof.v1"));
    assert!(snapshot.contains("proof_status: passed"));
    assert!(snapshot.contains("public_package_release_claim_allowed: false"));
    assert!(snapshot.contains("generated_output_proof_distinct_from_no_dataset_smoke: true"));
    assert!(snapshot.contains("generated_source_user_rows_smoke_performed: true"));
    assert!(snapshot.contains("generated_source_range_smoke_performed: true"));
    assert!(snapshot.contains("prepared_native_benchmark_smoke_performed: true"));
    assert!(snapshot.contains("clean_conda_env_install_status"));
    assert!(snapshot.contains("fallback_attempted=False"));
    assert!(snapshot.contains("generated_source_user_rows_local_output_smoke -> 0"));
    assert!(snapshot.contains("generated_source_range_local_output_smoke -> 0"));
    assert!(snapshot.contains("generated_source_kind=user_rows"));
    assert!(snapshot.contains("generated_source_kind=range"));
    assert!(snapshot.contains("output_native_io_certificate_status=certified_local_file_sink"));
    assert!(snapshot.contains("example_local_vortex_benchmark_smoke -> 0"));
    assert!(snapshot.contains("release_provenance_dry_run -> 0"));
    assert!(snapshot.contains("provenance_dry_run_performed: true"));
    assert!(snapshot.contains("sbom_checksum_manifest_generated: true"));

    let first_ten = read_repo_file("docs/getting-started/first-10-minutes.md");
    assert!(first_ten.contains("scripts\\release_dry_run_proof.py"));
    assert!(first_ten.contains("target/release-dry-run-proof/transcript.json"));
    assert!(first_ten.contains("ctx.from_rows"));
    assert!(first_ten.contains("ctx.range"));
    assert!(first_ten.contains("shardloom-prepared-vortex"));
    assert!(first_ten.contains("public package release"));
}

#[test]
fn release_provenance_docs_and_workflow_policy_are_traceable() {
    let doc = read_repo_file("docs/release/release-provenance-dry-run.md");
    for required in [
        "SupplyChainReleaseEvidence",
        "target/release-provenance-dry-run/manifest.json",
        "target/release-provenance-dry-run/checksums.sha256",
        "workflow-policy-snapshot.json",
        "publication_attempted=false",
        "tag_created=false",
        "secrets_required=false",
        "fallback_engine_dependency_added=false",
        "waived_until_real_publication",
        "pinned to commit SHAs",
    ] {
        assert!(
            doc.contains(required),
            "missing provenance doc field {required}"
        );
    }

    let sbom = read_repo_file("docs/release/sbom-generation-plan.md");
    assert!(sbom.contains("python scripts\\release_provenance_dry_run.py"));
    assert!(sbom.contains("supply-chain-release-evidence.json"));
    assert!(sbom.contains("workflow-policy-snapshot.json"));
    assert!(sbom.contains("pinned to commit SHAs"));

    let workflow = read_repo_file(".github/workflows/pypi-publish-draft.yml");
    assert!(workflow.contains("workflow_dispatch"));
    assert!(workflow.contains("publish_approved"));
    assert!(workflow.contains("environment: pypi"));
    assert!(workflow.contains("id-token: write"));
    assert!(!workflow.to_ascii_lowercase().contains("password:"));
    assert!(!workflow.to_ascii_lowercase().contains("api-token:"));
    assert!(!workflow.to_ascii_lowercase().contains("pypi-token"));
}

#[test]
fn open_source_security_posture_config_is_present() {
    let codeql = read_repo_file(".github/workflows/codeql-analysis.yml");
    for required in [
        "workflow_dispatch:",
        "pull_request:",
        "security-events: write",
        "github/codeql-action/init@v4",
        "github/codeql-action/analyze@v4",
        "language: rust",
        "language: python",
        "build-mode: none",
    ] {
        assert!(codeql.contains(required), "missing CodeQL field {required}");
    }

    let scorecard = read_repo_file(".github/workflows/scorecard.yml");
    for required in [
        "workflow_dispatch:",
        "publish_results: false",
        "github/codeql-action/upload-sarif@v4",
        "security-events: write",
        "persist-credentials: false",
    ] {
        assert!(
            scorecard.contains(required),
            "missing Scorecard field {required}"
        );
    }
    let scorecard_action = scorecard
        .lines()
        .find(|line| line.contains("ossf/scorecard-action@v"))
        .expect("missing Scorecard action pinned version tag");
    assert!(
        !scorecard_action.contains("@main") && !scorecard_action.contains("@master"),
        "Scorecard action must stay pinned to a version tag"
    );

    let dependabot = read_repo_file(".github/dependabot.yml");
    for required in [
        "package-ecosystem: \"cargo\"",
        "package-ecosystem: \"pip\"",
        "package-ecosystem: \"github-actions\"",
        "directory: \"/\"",
        "directory: \"/python\"",
        "interval: \"weekly\"",
    ] {
        assert!(
            dependabot.contains(required),
            "missing Dependabot field {required}"
        );
    }

    let doc = read_repo_file("docs/security/open-source-security-posture.md");
    for required in [
        "CodeQL",
        "OpenSSF Scorecard",
        "Dependabot",
        "secret scanning",
        "push protection",
        "branch protection",
        "required checks",
        "protected `pypi` environment",
        "protected release tags",
        "no-fallback",
    ] {
        assert!(
            doc.contains(required),
            "missing open-source security posture doc field {required}"
        );
    }
}

#[test]
fn universal_compatibility_scoreboard_projection_is_discoverable() {
    let scoreboard =
        read_repo_file("docs/architecture/universal-compatibility-coverage-scoreboard.json");
    for required in [
        "shardloom.universal_compatibility_coverage_scoreboard.v1",
        "gar-compat-1.universal_compatibility_coverage_scoreboard",
        "\"surface_id\": \"object_store_s3_gcs_adls\"",
        "\"surface_id\": \"table_lakehouse_iceberg_delta_hudi\"",
        "\"surface_id\": \"sql_values_literals\"",
        "\"surface_id\": \"foundry\"",
        "\"source_free_generated_output_contract\"",
        "\"schema_version\": \"shardloom.universal_compatibility.generated_output_contract.v1\"",
        "\"row_id\": \"python_ctx_from_rows\"",
        "\"row_id\": \"local_output_only_generated_source_posture\"",
        "\"row_id\": \"sql_values\"",
        "\"object_store_admission_ladder\"",
        "\"schema_version\": \"shardloom.universal_compatibility.object_store_admission_ladder.v1\"",
        "\"row_id\": \"public_no_credential_read\"",
        "\"row_id\": \"authenticated_read\"",
        "\"row_id\": \"byte_range_read\"",
        "\"row_id\": \"commit_protocol\"",
        "\"credential_resolution_performed\": false",
        "\"network_probe_allowed\": false",
        "\"provider_probe_allowed\": false",
        "\"object_store_io\": false",
        "\"write_io\": false",
        "\"all_rows_no_effects\": true",
        "\"table_format_boundary_matrix\"",
        "\"schema_version\": \"shardloom.universal_compatibility.table_format_boundary_matrix.v1\"",
        "\"row_id\": \"table_metadata_read\"",
        "\"row_id\": \"table_scan\"",
        "\"row_id\": \"snapshot_time_travel\"",
        "\"row_id\": \"delete_tombstone\"",
        "\"row_id\": \"commit\"",
        "\"row_id\": \"rollback\"",
        "\"row_id\": \"object_store_coupling\"",
        "\"local_metadata_smoke_available\": true",
        "\"table_metadata_read_allowed\": false",
        "\"table_data_read_allowed\": false",
        "\"commit_allowed\": false",
        "\"rollback_allowed\": false",
        "\"all_rows_no_io_no_fallback\": true",
        "\"database_warehouse_boundary_matrix\"",
        "\"schema_version\": \"shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1\"",
        "\"row_id\": \"sqlite_file\"",
        "\"row_id\": \"postgres\"",
        "\"row_id\": \"mysql\"",
        "\"row_id\": \"jdbc_odbc\"",
        "\"row_id\": \"snowflake\"",
        "\"row_id\": \"bigquery\"",
        "\"row_id\": \"databricks_sql\"",
        "\"credential_resolution_performed\": false",
        "\"network_probe_performed\": false",
        "\"driver_loaded\": false",
        "\"import_runtime_supported\": false",
        "\"export_runtime_supported\": false",
        "\"query_pushdown_supported\": false",
        "\"external_baseline_only\": true",
        "\"fallback_attempted\": false",
        "\"external_engine_invoked\": false",
        "\"support_status\": \"runtime-supported\"",
        "\"support_status\": \"smoke-supported\"",
        "\"support_status\": \"report-only\"",
        "\"support_status\": \"blocked\"",
        "No object-store runtime",
        "No production lakehouse",
        "Only source-free SQL VALUES/literal",
        "Future validation target only",
    ] {
        assert!(
            scoreboard.contains(required),
            "missing universal compatibility scoreboard field {required}"
        );
    }

    let doc = read_repo_file("docs/architecture/universal-compatibility-coverage-scoreboard.md");
    for required in [
        "docs/architecture/universal-compatibility-coverage-scoreboard.json",
        "schema_version=shardloom.universal_compatibility_coverage_scoreboard.v1",
        "typed capability views",
        "S3/GCS/ADLS",
        "S3/GCS/ADLS remain blocked",
        "Foundry remains a future validation target",
        "Compatibility-Level Generated-Output Rows",
        "universal_compatibility_generated_output_no_dataset_smoke_separate=true",
        "S3/GCS/ADLS Object-Store Admission Ladder",
        "credential_resolution_performed=false",
        "provider_probe_allowed=false",
        "object_store_io=false",
        "Iceberg/Delta/Hudi Table-Format Boundary Matrix",
        "table_metadata_read_allowed=false",
        "table_data_read_allowed=false",
        "commit_allowed=false",
        "rollback_allowed=false",
        "Database/Warehouse Import-Export Boundary Matrix",
        "credential_resolution_performed=false",
        "network_probe_performed=false",
        "driver_loaded=false",
        "import_runtime_supported=false",
        "export_runtime_supported=false",
        "query_pushdown_supported=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            doc.contains(required),
            "missing universal compatibility scoreboard doc field {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    for required in [
        "ctx.compatibility_scoreboard()",
        "object_store_s3_gcs_adls",
        "runtime-supported",
        "smoke-supported",
        "report-only",
        "blocked",
        "It is a capability map only",
        "source_free_generated_output_contract",
        "local_output_only_generated_source_posture",
        "object_store_admission_ladder",
        "byte_range_read",
        "authenticated_read",
        "table_format_boundary_matrix",
        "table_metadata_read",
        "snapshot/time-travel",
        "object-store coupling",
        "database_warehouse_boundary_matrix",
        "sqlite_file",
        "jdbc_odbc",
        "databricks_sql",
        "performance, SQL/DataFrame, object-store/lakehouse, Foundry, or package claim",
    ] {
        assert!(
            python_readme.contains(required),
            "missing Python compatibility scoreboard field {required}"
        );
    }

    let status_page = read_repo_file("website/status.html");
    for required in [
        "Answer common capability questions in under two minutes.",
        "runtime supported",
        "smoke supported",
        "report only",
        "blocked",
        "planned",
        "not planned",
        "Public package channels",
        "Enterprise evidence export pack",
        "Foundry dev-stack starter",
        "Workflow recipe library",
        "Hidden fallback engine execution",
        "Spark-displacement claim",
        "Production SQL/DataFrame, object-store, lakehouse, or Foundry claim",
        "docs/architecture/universal-compatibility-coverage-scoreboard.json",
        "docs/release/package-channel-readiness-matrix.json",
        "docs/release/enterprise-evidence-export-pack.json",
        "docs/foundry/dev-stack-starter-kit.json",
        "docs/use-cases/recipes/recipe-index.json",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "public_package_release_claim_allowed=false",
    ] {
        assert!(
            status_page.contains(required),
            "missing website status scorecard field {required}"
        );
    }

    let website_readiness = read_repo_file("scripts/check_website_readiness.py");
    for required in [
        "Answer common capability questions in under two minutes.",
        "Public package channels",
        "Enterprise evidence export pack",
        "Foundry dev-stack starter",
        "Workflow recipe library",
        "docs/architecture/universal-compatibility-coverage-scoreboard.json",
        "docs/release/package-channel-readiness-matrix.json",
        "docs/release/enterprise-evidence-export-pack.json",
        "docs/foundry/dev-stack-starter-kit.json",
        "docs/use-cases/recipes/recipe-index.json",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            website_readiness.contains(required),
            "missing website readiness status-scorecard check {required}"
        );
    }
}

#[test]
fn enterprise_evidence_export_pack_remains_report_only_and_local_first() {
    let doc = read_repo_file("docs/release/enterprise-evidence-export-pack.md");
    for required in [
        "shardloom.enterprise_evidence_export_pack.v1",
        "python scripts\\check_enterprise_evidence_export_pack.py",
        "shardloom.openlineage_facet_mapping.v1",
        "shardloom.opentelemetry_trace_export_contract.v1",
        "target/enterprise-evidence-export-pack/<run-id>/",
        "shardloom-evidence.json",
        "openlineage-facets.json",
        "opentelemetry-trace.json",
        "summary.md",
        "redaction-report.json",
        "strict_local_enterprise_redaction",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "network_calls_by_default=false",
        "backend_integration_configured=false",
        "claim_gate_status=not_claim_grade",
    ] {
        assert!(
            doc.contains(required),
            "missing enterprise evidence export-pack doc field {required}"
        );
    }

    let manifest = read_repo_file("docs/release/enterprise-evidence-export-pack.json");
    for required in [
        "\"schema_version\": \"shardloom.enterprise_evidence_export_pack.v1\"",
        "\"gar_id\": \"GAR-COMMERCIAL-1D\"",
        "\"status\": \"report-only\"",
        "\"claim_gate_status\": \"not_claim_grade\"",
        "\"export_pack_runtime_supported\": false",
        "\"export_pack_enabled_by_default\": false",
        "\"opt_in_required\": true",
        "\"network_calls_by_default\": false",
        "\"backend_integration_configured\": false",
        "\"lineage_event_emitted\": false",
        "\"telemetry_trace_emitted\": false",
        "\"telemetry_metric_emitted\": false",
        "\"telemetry_log_emitted\": false",
        "\"fallback_attempted\": false",
        "\"external_engine_invoked\": false",
        "\"object_store_io_performed\": false",
        "\"credential_resolution_performed\": false",
        "\"shardloom_json_evidence_bundle\"",
        "\"openlineage_custom_facets\"",
        "\"opentelemetry_spans_metrics\"",
        "\"markdown_summary\"",
        "\"redaction_report\"",
        "\"strict_local_enterprise_redaction\"",
        "\"full_local_paths\"",
        "\"query_text\"",
        "\"sample_values\"",
        "\"future_cli_command\": \"shardloom evidence export-pack --output <dir> --local-only\"",
    ] {
        assert!(
            manifest.contains(required),
            "missing enterprise evidence export-pack manifest field {required}"
        );
    }

    let script = read_repo_file("scripts/check_enterprise_evidence_export_pack.py");
    for required in [
        "shardloom.enterprise_evidence_export_pack.v1",
        "shardloom.enterprise_evidence_export_pack_report.v1",
        "EXPECTED_COMPONENT_IDS",
        "REQUIRED_FALSE_FIELDS",
        "network_calls_by_default",
        "backend_integration_configured",
        "lineage_event_emitted",
        "telemetry_trace_emitted",
        "telemetry_metric_emitted",
        "telemetry_log_emitted",
        "fallback_attempted",
        "external_engine_invoked",
        "strict_local_enterprise_redaction",
    ] {
        assert!(
            script.contains(required),
            "missing enterprise evidence export-pack validator field {required}"
        );
    }
}

#[test]
fn release_security_gate_docs_and_known_unsupported_paths_are_present() {
    let doc = read_repo_file("docs/security/release-security-gate.md");
    for required in [
        "SecurityThreatModelReport",
        "VulnerabilityResponseReport",
        "DependencyAuditReport",
        "SupplyChainReleaseEvidence",
        "RuntimeInputSafetyReport",
        "OpenSourceSecurityPostureReport",
        "KnownUnsupportedPathsReport",
        "python scripts\\check_release_security_gate.py",
        "public release claims cannot pass",
        "fallback_attempted=true",
        "external_engine_invoked=true",
        "status=blocked",
    ] {
        assert!(
            doc.contains(required),
            "missing release security gate doc field {required}"
        );
    }

    let unsupported = read_repo_file("docs/release/known-unsupported-paths.md");
    for required in [
        "broad SQL/DataFrame execution",
        "live/hybrid production behavior",
        "object-store runtime",
        "global_architecture_runtime_claim_gate",
        "Foundry proof-of-use",
        "direct transient compatibility execution as a Vortex-native claim",
        "vortex_layout_device_managed_boundary_ref",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            unsupported.contains(required),
            "missing known unsupported path field {required}"
        );
    }
}

#[test]
fn hard_release_readiness_gate_docs_are_present() {
    let doc = read_repo_file("docs/release/hard-release-readiness-gate.md");
    for required in [
        "python scripts\\check_release_readiness.py",
        "python scripts\\run_release_validation_evidence.py",
        "shardloom.release_validation_evidence.v1",
        "target/hard-release-readiness-gate.json",
        "target/release-validation-evidence.json",
        "clean install",
        "release security gate report",
        "feature/build matrix execution evidence",
        "typed-envelope compatibility",
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace --all-targets",
        "python -m build python",
        "shardloom.global_architecture_runtime_claim_gate.v1",
        "shardloom.package_channel_readiness_matrix.v1",
        "python scripts\\check_package_channel_readiness.py",
        "target/package-channel-readiness-report.json",
        "Trusted Publisher/OIDC",
        "Internal Rust crates remain unpublished",
        "global-architecture-gate",
        "public_release_claim_allowed=false",
        "status=blocked",
    ] {
        assert!(
            doc.contains(required),
            "missing hard release gate doc field {required}"
        );
    }
}

#[test]
fn foundry_integration_pack_and_proof_docs_are_present() {
    let readiness = read_repo_file("docs/foundry/integration-pack-readiness.md");
    for required in [
        "F0",
        "F10",
        "FoundryExecutionContext",
        "FoundryDatasetTransactionReport",
        "FoundryDataHealthBridge",
        "FoundryVirtualTableSource",
        "FoundryExternalComputeBoundaryReport",
        "FoundryMediaSetSource",
        "FoundryAipLogicBoundaryReport",
        "FoundryMarketplaceStarterProduct",
        "shardloom.foundry_dev_stack_starter_kit.v1",
        "docs/foundry/dev-stack-starter-kit.md",
        "docs/foundry/package-proof-boundary-matrix.md",
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "foundry_package_proof_boundary_matrix_status=report_only",
        "foundry_package_proof_boundary_matrix_ref=foundry_package_proof_boundary_matrix",
        "python scripts\\foundry_proof_of_use.py",
        "shardloom.foundry_generated_output_boundary.v1",
        "foundry_output_api_required=true",
        "foundry_output_api_invoked=false",
        "object_store_commit_invoked=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "foundry_compute_invoked=false",
    ] {
        assert!(
            readiness.contains(required),
            "missing Foundry readiness field {required}"
        );
    }

    let proof = read_repo_file("docs/foundry/proof-of-use-certification.md");
    for required in [
        "shardloom.foundry_proof_of_use_report.v1",
        "shardloom.foundry_generated_output_fanout_posture.v1",
        "shardloom.foundry_generated_output_boundary.v1",
        "shardloom.foundry_scale_proof_boundary.v1",
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "shardloom.foundry_dev_stack_starter_kit.v1",
        "shardloom.generated_source_certificate_contract.v1",
        "package_install_mode",
        "transform_import_proven",
        "cli_binary_resolved",
        "staged_dataset_path_explicit",
        "supported_local_native_execution_smoke_performed",
        "certificate_metrics_dataset_output_written",
        "foundry_dev_stack_starter_kit_status",
        "foundry_dev_stack_starter_kit_ref",
        "foundry_dev_stack_starter_kit_schema_version",
        "foundry_generated_output_fanout_status",
        "foundry_generated_output_boundary_status",
        "foundry_scale_proof_boundary_status",
        "foundry_package_proof_boundary_matrix_status",
        "foundry_package_proof_boundary_matrix_ref",
        "foundry_package_proof_boundary_matrix",
        "generated_output_execution_performed=false",
        "generated_source_certificate_status=not_applicable_no_generated_rows",
        "generated_source_certificate_status=not_emitted_report_only",
        "output_native_io_certificate_status=not_emitted_report_only",
        "foundry_output_api_required=true",
        "foundry_output_api_invoked=false",
        "foundry_result_dataset_written=false",
        "foundry_evidence_dataset_written=false",
        "direct_s3_read_invoked=false",
        "direct_s3_write_invoked=false",
        "object_store_read_invoked=false",
        "object_store_commit_invoked=false",
        "foundry_runtime_invoked=false",
        "foundry_compute_invoked=false",
        "foundry_input_dataset_count=0",
        "foundry_output_dataset_count=0",
        "shardloom_execution_mode=local_foundry_style_smoke_only",
        "output_evidence_dataset_written=false",
        "claim_gate_status=not_foundry_scale_grade",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "public_foundry_generated_output_claim_allowed=false",
        "public_foundry_claim_allowed=false",
        "shardloom_foundry_package",
        "artifact_repository_publication",
        "foundry_service_invocation",
        "compute_module_surface",
        "virtual_table_native_execution",
        "dataset_transaction_runtime",
        "f10_workload_certified_deployment",
        "local_foundry_style_proof_claim_allowed",
    ] {
        assert!(
            proof.contains(required),
            "missing Foundry proof doc field {required}"
        );
    }

    let compute_flow = read_repo_file("docs/architecture/compute-engine-flow-reference.md");
    for required in [
        "shardloom.generated_source_certificate_contract.v1",
        "no_dataset_smoke",
        "user_generated_source",
        "engine_native_generated_source",
        "not_applicable_no_generated_rows",
        "generated-source-user-rows-smoke",
        "generated-source-range-smoke",
        "ctx.from_rows([",
        "ctx.range(",
        "none_scoped_local_range_jsonl_smoke_only",
        "shardloom.generated_source_api_admission.v1",
        "shardloom.generated_source_evidence_alignment.v1",
        "shardloom.openlineage_facet_mapping.v1",
        "shardloom.opentelemetry_trace_export_contract.v1",
        "GAR-NOVEL-1A",
        "GAR-NOVEL-1B",
        "GAR-NOVEL-1C",
        "GAR-NOVEL-1D",
        "shardloom.traditional_analytics.bayesian_claim_confidence.v1",
        "posterior_runtime_distribution=not_fit",
        "credible_interval=not_computed",
        "probability_of_regression=not_computed",
        "runtime_decision_applied=false",
        "layout_decision_applied=false",
        "benchmark_recomputed=false",
        "claim_gate_status=advisory_only_not_claim_grade",
        "python_ctx_from_rows",
        "python_ctx_range",
        "python_generated_source_write",
        "sql_values",
        "sql_dataframe_source_free",
        "foundry_generated_output",
        "dataframe_generated_with_column",
        "openlineage_export_enabled=false",
        "openlineage_facet_mapping_event_emitted=false",
        "openlineage_facet_mapping_network_call_performed=false",
        "opentelemetry_trace_export_trace_export_enabled=false",
        "opentelemetry_trace_export_otlp_exporter_configured=false",
        "opentelemetry_trace_export_network_exporter_enabled=false",
        "opentelemetry_trace_export_network_call_performed=false",
        "opentelemetry_export_enabled=false",
        "opentelemetry_network_exporter_enabled=false",
        "bayesian_confidence_enabled=false",
    ] {
        assert!(
            compute_flow.contains(required),
            "missing compute-flow generated-source contract field {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    assert!(python_readme.contains("generated_source_contract"));
    assert!(python_readme.contains("generated_source_api_admission"));
    assert!(python_readme.contains("generated_source_evidence_alignment"));
    assert!(python_readme.contains("openlineage_facet_mapping"));
    assert!(python_readme.contains("ctx.from_rows("));
    assert!(python_readme.contains("ctx.range("));
    assert!(python_readme.contains("no_dataset_smoke_separate_from_generated_output"));

    let python_context = read_repo_file("python/src/shardloom/context.py");
    assert!(python_context.contains("GeneratedSourceCertificateContract"));
    assert!(python_context.contains("GeneratedSourceApiAdmissionMatrix"));
    assert!(python_context.contains("GeneratedSourceEvidenceAlignmentReport"));
    assert!(python_context.contains("OpenLineageFacetMappingReport"));
    assert!(python_context.contains("OpenTelemetryTraceExportContractReport"));
    assert!(python_context.contains("GeneratedSourceCaseCapability"));
    assert!(python_context.contains("GeneratedRowsSource"));
    assert!(python_context.contains("GeneratedRangeSource"));
    assert!(python_context.contains("all_no_fallback_no_external_engine"));

    let generated_architecture = read_repo_file(
        "docs/architecture/evidence-native-generated-execution-observability-confidence.md",
    );
    for required in [
        "shardloom.generated_source_evidence_alignment.v1",
        "gar-novel-1a.generated_source_cross_surface_alignment",
        "shardloom.openlineage_facet_mapping.v1",
        "gar-novel-1b.openlineage_facet_mapping",
        "shardloom.opentelemetry_trace_export_contract.v1",
        "gar-novel-1c.opentelemetry_trace_export_contract",
        "shardloom.traditional_analytics.bayesian_claim_confidence.v1",
        "gar-novel-1d.bayesian_claim_confidence",
        "posterior_runtime_distribution=not_fit",
        "credible_interval=not_computed",
        "probability_of_regression=not_computed",
        "runtime_decision_applied=false",
        "layout_decision_applied=false",
        "benchmark_recomputed=false",
        "claim_gate_status=advisory_only_not_claim_grade",
        "request_admission",
        "source_read",
        "compatibility_parse",
        "vortex_import",
        "vortex_scan",
        "operator_compute",
        "result_sink",
        "evidence_render",
        "claim_gate",
        "trace_export_enabled=false",
        "metric_export_enabled=false",
        "log_export_enabled=false",
        "otlp_exporter_configured=false",
        "network_exporter_enabled=false",
        "collector_configured=false",
        "sdk_dependency_added=false",
        "runtime_collection_enabled=false",
        "trace_emitted=false",
        "metric_emitted=false",
        "log_emitted=false",
        "ExecutionModeFacet",
        "NoFallbackFacet",
        "NativeIoCertificateFacet",
        "MaterializationBoundaryFacet",
        "ClaimGateFacet",
        "GeneratedSourceFacet",
        "VortexArtifactFacet",
        "event_emitted=false",
        "network_call_performed=false",
        "client_dependency_added=false",
        "schema_published=false",
        "redaction_policy_required=true",
        "retention_policy_required=true",
        "openlineage_export_enabled=false",
        "opentelemetry_network_exporter_enabled=false",
        "bayesian_confidence_enabled=false",
        "foundry_runtime_invoked=false",
        "object_store_io_performed=false",
        "foundry_generated_output",
    ] {
        assert!(
            generated_architecture.contains(required),
            "missing GAR-NOVEL-1A architecture field {required}"
        );
    }
}

#[test]
fn foundry_package_proof_boundary_matrix_remains_report_only() {
    let doc = read_repo_file("docs/foundry/package-proof-boundary-matrix.md");
    for required in [
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "GAR-0036-A",
        "support_status=report_only",
        "claim_gate_status=not_claim_grade",
        "foundry_runtime_invoked=false",
        "foundry_compute_invoked=false",
        "foundry_spark_invoked=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "public_foundry_claim_allowed=false",
        "local_style_transform_fixture",
        "local_certificate_metrics_output",
        "shardloom_foundry_package",
        "artifact_repository_publication",
        "foundry_service_invocation",
        "compute_module_surface",
        "virtual_table_native_execution",
        "dataset_transaction_runtime",
        "f10_workload_certified_deployment",
        "no `shardloom-foundry` package claim",
        "no dataset transaction runtime claim",
        "python scripts\\check_foundry_package_proof_boundary.py",
    ] {
        assert!(
            doc.contains(required),
            "missing Foundry package proof boundary doc field {required}"
        );
    }

    let matrix = read_repo_file("docs/foundry/package-proof-boundary-matrix.json");
    for required in [
        "\"schema_version\": \"shardloom.foundry_package_proof_boundary_matrix.v1\"",
        "\"gar_id\": \"GAR-0036-A\"",
        "\"support_status\": \"report_only\"",
        "\"claim_gate_status\": \"not_claim_grade\"",
        "\"row_count\": 9",
        "\"local_style_claim_allowed_count\": 2",
        "\"blocked_count\": 7",
        "\"public_foundry_claim_allowed\": false",
        "\"foundry_package_publication_allowed\": false",
        "\"artifact_repository_publication_allowed\": false",
        "\"foundry_service_invocation_allowed\": false",
        "\"compute_module_invoked\": false",
        "\"virtual_table_native_execution_claimed\": false",
        "\"dataset_transaction_runtime_allowed\": false",
        "\"f10_deployment_certified\": false",
        "\"foundry_runtime_invoked\": false",
        "\"foundry_compute_invoked\": false",
        "\"foundry_spark_invoked\": false",
        "\"foundry_output_api_invoked\": false",
        "\"fallback_attempted\": false",
        "\"external_engine_invoked\": false",
    ] {
        assert!(
            matrix.contains(required),
            "missing Foundry package proof boundary matrix field {required}"
        );
    }

    let script = read_repo_file("scripts/check_foundry_package_proof_boundary.py");
    for required in [
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "shardloom.foundry_package_proof_boundary_matrix_report.v1",
        "REQUIRED_ROWS",
        "REQUIRED_FALSE_FIELDS",
        "public_foundry_claim_allowed",
        "local_style_claim_allowed",
        "fixture_smoke_only",
        "not_claim_grade",
    ] {
        assert!(
            script.contains(required),
            "missing Foundry package proof boundary validator field {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0036-A Foundry package and proof boundary matrix"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    for required in [
        "GAR-0036-A Foundry package and proof boundary matrix",
        "shardloom.foundry_package_proof_boundary_matrix.v1",
        "foundry_package_proof_boundary_matrix_status=report_only",
        "foundry_package_proof_boundary_matrix_ref=foundry_package_proof_boundary_matrix",
        "support_status=report_only",
        "claim_gate_status=not_claim_grade",
        "foundry_runtime_invoked=false",
        "foundry_compute_invoked=false",
        "foundry_spark_invoked=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "public_foundry_claim_allowed=false",
    ] {
        assert!(
            completed.contains(required),
            "missing GAR-0036-A completed ledger field {required}"
        );
    }

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("- [x] `GAR-0036-A`"));
    assert!(gar.contains("shardloom.foundry_package_proof_boundary_matrix.v1"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("GAR-0036-A"));
    assert!(traceability.contains("docs/foundry/package-proof-boundary-matrix.md"));
}

#[test]
fn external_examples_include_fixtures_expected_outputs_and_boundaries() {
    for example in [
        "examples/local-python-smoke",
        "examples/local-vortex-benchmark",
        "examples/foundry-lightweight-transform",
    ] {
        for file in [
            "README.md",
            "environment.yml",
            "expected-output.json",
            "expected-certificate-fields.json",
            "known-limitations.md",
        ] {
            let path = format!("{example}/{file}");
            assert!(repo_root().join(&path).exists(), "missing {path}");
        }
    }

    assert!(
        repo_root()
            .join("examples/local-python-smoke/fixtures/no-dataset-smoke.json")
            .exists()
    );
    assert!(
        repo_root()
            .join("examples/local-vortex-benchmark/fixtures/benchmark-request.json")
            .exists()
    );
    assert!(
        repo_root()
            .join("examples/foundry-lightweight-transform/fixtures/staged_input.csv")
            .exists()
    );

    let vortex_example = read_repo_file("examples/local-vortex-benchmark/run.py");
    assert!(vortex_example.contains("shardloom,shardloom-prepared-vortex"));
    assert!(vortex_example.contains("prepared Vortex"));

    let vortex_expected = read_repo_file("examples/local-vortex-benchmark/expected-output.json");
    assert!(vortex_expected.contains("\"shardloom-prepared-vortex\""));
    assert!(vortex_expected.contains("\"fallback_attempted\": false"));
    assert!(vortex_expected.contains("\"external_engine_invoked\": false"));

    let foundry = read_repo_file("examples/foundry-lightweight-transform/run.py");
    assert!(foundry.contains("foundry_runtime_invoked"));
    assert!(foundry.contains("foundry_compute_invoked"));
    assert!(foundry.contains("external_compute_invoked"));
    assert!(foundry.contains("fallback_attempted"));
    assert!(foundry.contains("not_emitted_no_dataset_smoke"));

    let boundary = read_repo_file("docs/benchmarks/baseline-comparison-boundary.md");
    assert!(boundary.contains("external_baseline_only"));
    assert!(boundary.contains("fallback_attempted=false"));
    assert!(boundary.contains("external_engine_invoked=false"));
    assert!(boundary.contains("never ShardLoom runtime dependencies"));
}

#[test]
fn foundry_dev_stack_starter_remains_local_style_report_only() {
    let doc = read_repo_file("docs/foundry/dev-stack-starter-kit.md");
    for required in [
        "shardloom.foundry_dev_stack_starter_kit.v1",
        "python scripts\\check_foundry_dev_stack_starter.py",
        "cargo build -p shardloom-cli --bin shardloom",
        "python examples\\foundry-lightweight-transform\\run.py --repo-root .",
        "python scripts\\foundry_proof_of_use.py --rows 64 --iterations 1",
        "no_dataset_smoke_separate_from_generated_output=true",
        "generated_output_execution_performed=false",
        "foundry_runtime_invoked=false",
        "foundry_compute_invoked=false",
        "foundry_spark_invoked=false",
        "foundry_output_api_invoked=false",
        "foundry_result_dataset_written=false",
        "foundry_evidence_dataset_written=false",
        "direct_s3_write_invoked=false",
        "object_store_write_invoked=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "local_foundry_style_transform_and_local_vortex_execution_smoke_only",
    ] {
        assert!(
            doc.contains(required),
            "missing Foundry dev-stack starter doc field {required}"
        );
    }

    let manifest = read_repo_file("docs/foundry/dev-stack-starter-kit.json");
    for required in [
        "\"schema_version\": \"shardloom.foundry_dev_stack_starter_kit.v1\"",
        "\"gar_id\": \"GAR-COMMERCIAL-1E\"",
        "\"status\": \"local_style_report_only\"",
        "\"claim_gate_status\": \"not_claim_grade\"",
        "\"real_foundry_runtime_supported\": false",
        "\"foundry_runtime_invoked\": false",
        "\"foundry_compute_invoked\": false",
        "\"foundry_spark_invoked\": false",
        "\"foundry_output_api_invoked\": false",
        "\"foundry_result_dataset_written\": false",
        "\"foundry_evidence_dataset_written\": false",
        "\"direct_s3_write_invoked\": false",
        "\"object_store_write_invoked\": false",
        "\"credential_resolution_performed\": false",
        "\"network_probe_performed\": false",
        "\"external_engine_invoked\": false",
        "\"fallback_attempted\": false",
        "\"public_foundry_claim_allowed\": false",
        "\"foundry_marketplace_claim_allowed\": false",
        "\"no_dataset_smoke_separate_from_generated_output\": true",
        "\"generated_source_certificate_status\": \"not_emitted_report_only\"",
        "\"deterministic_blocker\": \"blocked_until_real_foundry_output_api_evidence\"",
    ] {
        assert!(
            manifest.contains(required),
            "missing Foundry dev-stack starter manifest field {required}"
        );
    }

    let script = read_repo_file("scripts/check_foundry_dev_stack_starter.py");
    for required in [
        "shardloom.foundry_dev_stack_starter_kit.v1",
        "shardloom.foundry_dev_stack_starter_kit_report.v1",
        "REQUIRED_FALSE_FIELDS",
        "EXPECTED_COMMAND_IDS",
        "foundry_runtime_invoked",
        "foundry_compute_invoked",
        "foundry_spark_invoked",
        "foundry_output_api_invoked",
        "fallback_attempted",
        "external_engine_invoked",
    ] {
        assert!(
            script.contains(required),
            "missing Foundry dev-stack starter validator field {required}"
        );
    }
}

#[test]
fn workflow_recipe_library_remains_claim_safe_and_indexed() {
    let readme = read_repo_file("docs/use-cases/recipes/README.md");
    for required in [
        "shardloom.workflow_recipe_library.v1",
        "python scripts\\check_workflow_recipes.py",
        "No-Dataset Smoke",
        "Local CSV Certified Result",
        "Prepared Vortex Batch Run",
        "Source-Free Generated Reference Table",
        "Dirty CSV Cleanup",
        "Nested JSON Scan",
        "CDC Overlay",
        "Object-Store Blocked Diagnostic",
        "Foundry Dev-Stack Smoke",
        "Benchmark Evidence Interpretation",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            readme.contains(required),
            "missing workflow recipe README field {required}"
        );
    }

    let index = read_repo_file("docs/use-cases/recipes/recipe-index.json");
    for required in [
        "\"schema_version\": \"shardloom.workflow_recipe_library.v1\"",
        "\"gar_id\": \"GAR-COMMERCIAL-1F\"",
        "\"status\": \"report_only_documentation_surface\"",
        "\"claim_gate_status\": \"not_claim_grade\"",
        "\"fallback_attempted\": false",
        "\"external_engine_invoked\": false",
        "\"id\": \"source-free-generated-reference-table\"",
        "\"id\": \"object-store-blocked-diagnostic\"",
        "\"use_case_id\": \"object-store-boundary-report\"",
        "\"use_case_id\": \"benchmark-interpretation-evidence-not-leaderboard\"",
    ] {
        assert!(
            index.contains(required),
            "missing workflow recipe index field {required}"
        );
    }

    let script = read_repo_file("scripts/check_workflow_recipes.py");
    for required in [
        "shardloom.workflow_recipe_library.v1",
        "shardloom.workflow_recipe_library_report.v1",
        "REQUIRED_RECIPE_IDS",
        "SUPPORTED_STATUSES",
        "EXPLANATION_STATUSES",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "Spark replacement",
    ] {
        assert!(
            script.contains(required),
            "missing workflow recipe validator field {required}"
        );
    }
}

#[test]
fn use_case_atlas_closeout_remains_generated_and_validated() {
    let atlas = read_repo_file("docs/use-cases/README.md");
    for required in [
        "Can ShardLoom do my thing?",
        "How do I try it?",
        "What evidence do I get?",
        "What is not supported yet?",
        "ready_local",
        "smoke_supported",
        "report_only",
        "planned",
        "blocked",
        "unsupported",
        "python scripts\\check_use_case_index.py",
        "python scripts\\check_use_case_coverage.py",
        "python scripts\\check_use_case_glossary.py",
        "python scripts\\check_use_case_backlinks.py",
        "python scripts\\check_workflow_recipes.py",
    ] {
        assert!(
            atlas.contains(required),
            "missing Use Case Atlas README field {required}"
        );
    }

    let index = read_repo_file("docs/use-cases/use-case-index.yml");
    for required in [
        "schema_version: 1",
        "onboarding_first_10_minutes",
        "local_file_etl",
        "compatibility_import_certified",
        "prepared_native_vortex",
        "python_wrapper_client",
        "sql_dataframe_report_only",
        "source_free_generated_output",
        "messy_data_dirty_json_cdc",
        "query_scenario_cookbook",
        "output_and_fanout",
        "object_store_boundaries",
        "table_lakehouse_boundaries",
        "foundry_dev_stack_local_proof",
        "evidence_audit_claim_gates",
        "benchmark_interpretation",
        "package_release_install_channels",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "claim_boundary:",
        "references:",
        "related_use_cases:",
    ] {
        assert!(
            index.contains(required),
            "missing Use Case Atlas index field {required}"
        );
    }

    let template = read_repo_file("docs/use-cases/templates/use-case-template.md");
    for required in [
        "## Quick Answer",
        "## Can ShardLoom Do This?",
        "## How To Try It",
        "## Blocker",
        "## Inputs",
        "## Outputs",
        "## Evidence You Should See",
        "## Expected Output Or Evidence",
        "## Common Mistakes",
        "## Reference Files",
        "## Related Use Cases",
    ] {
        assert!(
            template.contains(required),
            "missing use-case template section {required}"
        );
    }

    let generated_doc = read_repo_file("docs/use-cases/generated/first-10-minutes-local-smoke.md");
    for required in [
        "## Quick Answer",
        "## Can ShardLoom Do This?",
        "## How To Try It",
        "## Internal Flow",
        "## Evidence You Should See",
        "## Expected Output Or Evidence",
        "## Reference Files",
        "`README.md`",
        "`docs/getting-started/first-10-minutes.md`",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            generated_doc.contains(required),
            "missing generated use-case docs field {required}"
        );
    }

    let generated_dir = repo_root().join("docs/use-cases/generated");
    let expected_generated_count = yaml_top_level_section_item_count(&index, "use_cases");
    let generated_count = fs::read_dir(&generated_dir)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", generated_dir.display()))
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "md"))
        .count();
    assert_eq!(
        generated_count, expected_generated_count,
        "generated use-case docs must match use-case index count"
    );

    let glossary = read_repo_file("docs/use-cases/field-guide/README.md");
    for required in [
        "execution mode",
        "engine mode",
        "Vortex-native",
        "compatibility import",
        "prepared Vortex",
        "native Vortex",
        "direct transient",
        "no fallback",
        "materialization boundary",
        "Native I/O certificate",
        "result-sink replay",
        "claim gate",
        "fixture smoke",
        "report-only",
        "external baseline",
        "residual-native",
        "encoded-native",
        "source-state reuse",
        "output-plan reuse",
    ] {
        assert!(
            glossary.contains(required),
            "missing use-case glossary term {required}"
        );
    }

    let backlinks = read_repo_file("docs/use-cases/reference-backlinks.md");
    for required in [
        "`README.md`",
        "`docs/architecture/compute-engine-flow-reference.md`",
        "`docs/benchmarks/local-taxonomy-benchmark.md`",
        "`docs/foundry/proof-of-use-certification.md`",
        "`python/README.md`",
        "`examples/local-python-smoke/README.md`",
        "`examples/local-vortex-benchmark/README.md`",
        "`foundry-local-proof-boundary`",
        "`benchmark-interpretation-evidence-not-leaderboard`",
    ] {
        assert!(
            backlinks.contains(required),
            "missing use-case backlink field {required}"
        );
    }

    let generator = read_repo_file("website/build_static_pages.py");
    for required in [
        "write_use_case_pages",
        "use_case_markdown",
        "use_case_page",
        "use_cases_index_page",
        "DOC_USE_CASES",
        "USE_CASE_PAGES",
    ] {
        assert!(
            generator.contains(required),
            "missing website use-case generator field {required}"
        );
    }

    let website_index = read_repo_file("website/use-cases/index.html");
    for required in [
        "Can I use this?",
        "data-use-case-filter=\"status\"",
        "data-use-case-filter=\"input\"",
        "data-use-case-filter=\"output\"",
        "data-use-case-filter=\"execution\"",
        "data-use-case-filter=\"evidence\"",
        "data-use-case-filter=\"platform\"",
        "data-use-case-grid",
        "ready_local",
        "smoke_supported",
        "report_only",
        "blocked",
        "planned",
        "unsupported",
    ] {
        assert!(
            website_index.contains(required),
            "missing website use-case matrix field {required}"
        );
    }

    let website_page = read_repo_file("website/use-cases/first-10-minutes-local-smoke.html");
    for required in [
        "First 10 minutes local smoke",
        "Reference Files",
        "Claim gate",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            website_page.contains(required),
            "missing generated website use-case field {required}"
        );
    }

    let index_validator = read_repo_file("scripts/check_use_case_index.py");
    for required in [
        "ALLOWED_STATUSES",
        "REQUIRED_USE_CASE_FIELDS",
        "claim_boundary",
        "FORBIDDEN_CLAIM_PATTERNS",
        "references",
        "reference must be exact",
    ] {
        assert!(
            index_validator.contains(required),
            "missing use-case index validator field {required}"
        );
    }

    let coverage_validator = read_repo_file("scripts/check_use_case_coverage.py");
    for required in [
        "EXPECTED_CAPABILITY_FAMILIES",
        "EXPECTED_EXECUTION_MODES",
        "EXPECTED_ENGINE_MODES",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "native_io_certificate_status",
        "claim_gate_status",
    ] {
        assert!(
            coverage_validator.contains(required),
            "missing use-case coverage validator field {required}"
        );
    }

    let glossary_validator = read_repo_file("scripts/check_use_case_glossary.py");
    for required in [
        "REQUIRED_TERMS",
        "no fallback",
        "claim gate",
        "external baseline",
        "Reference Files",
    ] {
        assert!(
            glossary_validator.contains(required),
            "missing use-case glossary validator field {required}"
        );
    }

    let backlink_validator = read_repo_file("scripts/check_use_case_backlinks.py");
    for required in [
        "reference-backlinks.md",
        "## Reference Files",
        "missing reference",
        "backlink ledger missing reference",
    ] {
        assert!(
            backlink_validator.contains(required),
            "missing use-case backlink validator field {required}"
        );
    }
}

#[test]
fn field_guide_atlas_closeout_remains_generated_and_claim_safe() {
    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1A"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1B"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1C"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1D"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1E"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1F"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1G"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1H"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1I"));
    assert!(!plan.contains("- [ ] GAR-WEB-ATLAS-1J"));
    assert!(
        !plan.contains("- [ ] GAR-0010-B DataFrame/notebook and package surface readiness report")
    );

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    for required in [
        "GAR-0010-B DataFrame/notebook and package surface readiness report",
        "shardloom.dataframe_notebook_package_readiness.v1",
        "dataframe_notebook_package_readiness",
        "python_package_metadata",
        "editable_install_smoke",
        "dataframe_method_matrix",
        "notebook_display_surface",
        "public_package_publication",
        "unsupported_diagnostics",
        "local package/DataFrame/notebook posture",
        "broad DataFrame runtime",
        "GAR-WEB-ATLAS-1J Field Guide / Use Case public-readiness gate",
        "GAR-WEB-ATLAS-1I visual density and readability refinement",
        "GAR-WEB-ATLAS-1H Astro/Starlight migration decision gate",
        "GAR-WEB-ATLAS-1G source-linked reference and citation blocks",
        "GAR-WEB-ATLAS-1F Can-I-use-this status matrix",
        "GAR-WEB-ATLAS-1E Use Case Atlas integration",
        "GAR-WEB-ATLAS-1D static Field Guide search with Pagefind",
        "GAR-WEB-ATLAS-1C Field Guide reading paths",
        "GAR-WEB-ATLAS-1A/1B Field Guide taxonomy and dossier generator",
        "What this proves",
        "data-citation-block=\"reference-files\"",
        "citation-card",
        "atlas-density-note",
        "category-toc-band",
        "compact-term-row",
        "status-chip",
        "reference-badge",
        "related-concepts-rail",
        "sticky-in-page-toc",
        "FIELD_GUIDE_DOSSIER_REQUIRED_FIELDS",
        "USE_CASE_PAGE_REQUIRED_FIELDS",
        "fieldGuideDossierRequiredFields",
        "useCasePublicReadinessFields",
        "website-atlas-public-readiness.md",
        "runnable example or blocker explanation",
        "Claim Boundary",
        "75 compact dossier rows",
        "11 concept families",
        "blocked_pending_explicit_approval",
        "website-atlas-framework-decision.md",
        "99 HTML files discovered by Pagefind",
        "97 indexed",
        "current Python static generator",
        "Astro custom",
        "Astro Starlight",
        "Capability status matrix",
        "docs/use-cases/use-case-index.yml",
        "status, input type, output type, execution mode, evidence level, and",
        "node --check website/assets/use-cases.js",
        "website/content/field-guide-index.json",
        "website/pagefind/",
        "Related Field Guide Terms",
        "scripts/check_use_case_backlinks.py",
        "python scripts/check_use_case_backlinks.py",
        "Pagefind 1.5.2",
        "section, status, category, execution mode, and engine mode",
        "75 entries",
        "seven reading paths",
        "python website/build_static_pages.py",
        "python -m pagefind --site website",
        "node website/validate_static_assets.js",
        "python scripts/check_website_readiness.py",
        "No runtime behavior",
        "fallback execution",
    ] {
        assert!(
            completed.contains(required),
            "missing Field Guide completed-ledger field {required}"
        );
    }

    let framework_decision =
        read_repo_file("docs/architecture/website-atlas-framework-decision.md");
    for required in [
        "GAR-WEB-ATLAS-1H",
        "Decision status: `report_only`",
        "Recommendation: keep the current Python static generator",
        "Migration status: `blocked_pending_explicit_approval`",
        "99 HTML files discovered by Pagefind",
        "97 indexed pages",
        "Astro content collections",
        "Astro custom site",
        "Astro Starlight",
        "Pagefind",
        "Cloudflare Workers Static Assets",
        "No Astro/Starlight migration may begin",
        "dependency/license review",
        "lockfile",
        "validator parity",
        "runtime GitHub raw fetches",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "https://docs.astro.build/en/guides/content-collections/",
        "https://starlight.astro.build/",
        "https://pagefind.app/docs/",
    ] {
        assert!(
            framework_decision.contains(required),
            "missing framework decision field {required}"
        );
    }

    let index = read_repo_file("website/content/field-guide-index.json");
    assert!(index.contains("\"schema_version\": \"shardloom.field_guide_index.v1\""));
    assert!(
        index.matches("\"slug\":").count() >= 50,
        "Field Guide index should contain at least 50 entries"
    );
    for required in [
        "\"Start Here\"",
        "\"Execution Modes\"",
        "\"Engine Modes\"",
        "\"Vortex Runtime\"",
        "\"Evidence And Claims\"",
        "\"Benchmark Telemetry\"",
        "\"User Workflows\"",
        "\"I/O And Output\"",
        "\"Platform Boundaries\"",
        "\"Performance Architecture\"",
        "\"Release And Trust\"",
        "\"related_use_cases\"",
        "\"reference_files\"",
        "\"claim_boundary\"",
        "\"reading_paths\"",
        "\"new-to-shardloom\"",
        "\"run-a-local-workflow\"",
        "\"understand-benchmarks\"",
        "\"understand-vortex-native-paths\"",
        "\"use-python-sql-dataframe\"",
        "\"know-what-is-blocked\"",
        "\"foundry-and-platform-context\"",
    ] {
        assert!(
            index.contains(required),
            "missing Field Guide index field {required}"
        );
    }

    let generator = read_repo_file("website/build_static_pages.py");
    for required in [
        "FIELD_GUIDE_INDEX_PATH",
        "load_field_guide_concepts",
        "load_field_guide_reading_paths",
        "render_public_status_scorecard_section(use_cases",
        "data-status-matrix-filters",
        "data-status-matrix-filter",
        "data-status-matrix-grid",
        "data-status-matrix-count",
        "docs/use-cases/use-case-index.yml",
        "PAGEFIND_HEAD",
        "pagefind_filter_spans",
        "citation_proof",
        "render_citation_links",
        "data-citation-block=\"reference-files\"",
        "What this proves:",
        "atlas-density-note",
        "category-toc-band",
        "compact-term-row",
        "status-chip",
        "reference-badge",
        "related-concepts-rail",
        "sticky-in-page-toc",
        "REQUIRED_FIELD_GUIDE_CATEGORIES",
        "FIELD_GUIDE_READING_PATHS",
        "field_guide_concepts_for_use_case",
        "related_field_guide_term_links",
        "reading_path_term_links",
        "field_guide_concepts_by_category",
        "Plain-English meaning",
        "Current support",
        "Evidence fields",
        "What it does not claim",
        "Try it / related use cases",
        "Reference files",
        "pagefind-filter-dropdown",
        "Claim Boundary",
    ] {
        assert!(
            generator.contains(required),
            "missing Field Guide generator field {required}"
        );
    }

    let status_page = read_repo_file("website/status.html");
    for required in [
        "Capability status matrix",
        "data-status-matrix-filters",
        "data-status-matrix-filter=\"status\"",
        "data-status-matrix-filter=\"input\"",
        "data-status-matrix-filter=\"output\"",
        "data-status-matrix-filter=\"execution\"",
        "data-status-matrix-filter=\"evidence\"",
        "data-status-matrix-filter=\"platform\"",
        "data-status-matrix-grid",
        "data-status-matrix-count",
        "docs/use-cases/use-case-index.yml",
        "/use-cases/object-store-boundary-report",
        "/use-cases/table-lakehouse-boundary-report",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ] {
        assert!(
            status_page.contains(required),
            "missing generated status matrix field {required}"
        );
    }

    let use_case_filter_script = read_repo_file("website/assets/use-cases.js");
    for required in [
        "setupFilterSet",
        "data-use-case-filter",
        "data-status-matrix-filter",
        ".status-matrix-row",
        "status row",
    ] {
        assert!(
            use_case_filter_script.contains(required),
            "missing reusable filter script field {required}"
        );
    }

    let website_index = read_repo_file("website/field-guide/index.html");
    for required in [
        "Reading paths",
        "New to ShardLoom",
        "Run a local workflow",
        "Understand benchmarks",
        "Understand Vortex-native paths",
        "Use Python, SQL, or DataFrame surfaces",
        "Know what is blocked",
        "Foundry and platform context",
        "Table of contents",
        "Start Here",
        "Execution Modes",
        "Engine Modes",
        "Vortex Runtime",
        "Evidence And Claims",
        "Benchmark Telemetry",
        "User Workflows",
        "I/O And Output",
        "Platform Boundaries",
        "Performance Architecture",
        "Release And Trust",
        "Static search",
        "pagefind-component-ui.css",
        "pagefind-component-ui.js",
        "pagefind-modal-trigger",
        "pagefind-filter-dropdown",
        "data-pagefind-filter=\"section\"",
        "data-pagefind-filter=\"status\"",
        "atlas-density-note",
        "category-toc-band",
        "compact-term-row",
        "status-chip",
        "reference-badge",
        "75 dossiers",
        "11 concept families",
        "what-is-shardloom",
        "output-plan-reuse",
    ] {
        assert!(
            website_index.contains(required),
            "missing generated Field Guide index field {required}"
        );
    }

    let use_case_page =
        read_repo_file("website/use-cases/prepared-native-vortex-runtime-direction.html");
    for required in [
        "Related Field Guide Terms",
        "/field-guide/prepared-vortex",
        "/field-guide/native-vortex",
        "/field-guide/source-backed-scan",
        "data-citation-block=\"reference-files\"",
        "What this proves:",
        "Claim Boundary",
        "Internal Flow",
        "Expected Evidence Fields",
        "Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.",
    ] {
        assert!(
            use_case_page.contains(required),
            "missing use-case reverse Field Guide link {required}"
        );
    }

    let generated_use_case =
        read_repo_file("docs/use-cases/generated/prepared-native-vortex-runtime-direction.md");
    for required in [
        "What this proves:",
        "## Related Field Guide Terms",
        "`website/field-guide/prepared-vortex.html`",
        "`website/field-guide/native-vortex.html`",
    ] {
        assert!(
            generated_use_case.contains(required),
            "missing generated use-case reverse Field Guide link {required}"
        );
    }

    let backlink_validator = read_repo_file("scripts/check_use_case_backlinks.py");
    for required in [
        "Related Field Guide Terms",
        "What this proves:",
        "Field Guide dossier missing citation block",
        "website use-case page missing citation block",
        "use case has no related Field Guide terms",
        "website page {use_case_id} missing Field Guide term link",
        "website/content/field-guide-index.json",
    ] {
        assert!(
            backlink_validator.contains(required),
            "missing use-case integration validator field {required}"
        );
    }

    let dossier = read_repo_file("website/field-guide/prepared-vortex.html");
    for required in [
        "Plain-English meaning",
        "Why it matters",
        "How ShardLoom uses it",
        "Current support",
        "Evidence fields",
        "What it does not claim",
        "Try it / related use cases",
        "Related concepts",
        "Reference files",
        "sticky-in-page-toc",
        "related-concepts-rail",
        "reference-badge",
        "data-citation-block=\"reference-files\"",
        "What this proves:",
        "Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.",
        "claim-gated",
    ] {
        assert!(
            dossier.contains(required),
            "missing generated Field Guide dossier field {required}"
        );
    }

    let sitemap = read_repo_file("website/sitemap.xml");
    for required in [
        "https://shardloom.io/field-guide/what-is-shardloom",
        "https://shardloom.io/field-guide/output-plan-reuse",
        "https://shardloom.io/field-guide/scale-profile",
    ] {
        assert!(
            sitemap.contains(required),
            "missing Field Guide sitemap URL {required}"
        );
    }

    let pagefind_entry = read_repo_file("website/pagefind/pagefind-entry.json");
    assert!(pagefind_entry.contains("\"version\":\"1.5.2\""));
    assert!(pagefind_entry.contains("\"page_count\":"));

    let headers = read_repo_file("website/_headers");
    for required in [
        "/pagefind/*",
        "script-src 'self' 'wasm-unsafe-eval'",
        "worker-src 'self'",
        "Cache-Control: public, max-age=300",
    ] {
        assert!(
            headers.contains(required),
            "missing Pagefind header {required}"
        );
    }

    let website_validator = read_repo_file("website/validate_static_assets.js");
    for required in [
        "pagefind/pagefind-component-ui.css",
        "pagefind/pagefind-component-ui.js",
        "pagefind/pagefind-entry.json",
        "pagefind-filter-dropdown",
        "Committed Pagefind static bundle",
        "Related Field Guide Terms",
        "must render reverse Field Guide term links",
        "fieldGuideDossierRequiredFields",
        "useCasePublicReadinessFields",
        "missing public-readiness field",
        "runnable example or blocker explanation",
        "source-linked citation block",
        "What this proves:",
        "data-status-matrix-filters",
        "Status page must include",
        "atlas-density-note",
        "compact-term-row",
        "reference-badge",
    ] {
        assert!(
            website_validator.contains(required),
            "missing Pagefind static validator field {required}"
        );
    }

    let readiness = read_repo_file("scripts/check_website_readiness.py");
    for required in [
        "pagefind/pagefind-component-ui.css",
        "pagefind/pagefind-component-ui.js",
        "pagefind/pagefind-entry.json",
        "Pagefind index must cover generated website pages",
        "_headers missing Pagefind static-search policy",
        "data-citation-block=\"reference-files\"",
        "What this proves:",
        "data-status-matrix-filters",
        "data-status-matrix-grid",
        "atlas-density-note",
        "category-toc-band",
        "compact-term-row",
        "reference-badge",
        "FIELD_GUIDE_DOSSIER_REQUIRED_FIELDS",
        "USE_CASE_PAGE_REQUIRED_FIELDS",
        "missing public-readiness field",
        "runnable example or blocker explanation",
        "Claim Boundary",
    ] {
        assert!(
            readiness.contains(required),
            "missing Pagefind readiness field {required}"
        );
    }

    let third_party = read_repo_file("docs/legal/static-website-third-party-assets.md");
    for required in [
        "Pagefind",
        "pagefind_bin_extended",
        "1.5.2",
        "MIT",
        "website/pagefind/",
        "not ShardLoom execution logic",
    ] {
        assert!(
            third_party.contains(required),
            "missing Pagefind third-party asset notice {required}"
        );
    }

    let notice = read_repo_file("NOTICE");
    assert!(notice.contains("generated Pagefind static-search"));
    assert!(notice.contains("docs/legal/static-website-third-party-assets.md"));

    let atlas_readiness = read_repo_file("docs/release/website-atlas-public-readiness.md");
    for required in [
        "GAR-WEB-ATLAS-1J complete",
        "Field Guide Dossier Gate",
        "Use Case Atlas Gate",
        "Quick Example or blocker explanation",
        "Claim Boundary",
        "raw.githubusercontent.com",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "python scripts\\check_website_readiness.py",
        "node.exe website\\validate_static_assets.js",
    ] {
        assert!(
            atlas_readiness.contains(required),
            "missing atlas public readiness field {required}"
        );
    }
}

#[test]
fn gar_0033_a_etl_workflow_capability_matrix_remains_claim_safe() {
    let doc = read_repo_file("docs/architecture/etl-workflow-capability-matrix.md");
    for required in [
        "shardloom.etl_workflow_capability_matrix.v1",
        "GAR-0033-A",
        "first_10_minutes_local_smoke",
        "local_csv_parquet_certified_workload",
        "prepared_native_vortex_batch_smoke",
        "source_free_user_rows_jsonl",
        "source_free_range_jsonl",
        "dirty_csv_fixture",
        "nested_json_fixture",
        "cdc_overlay_fixture",
        "sql_dataframe_capability_posture",
        "data_quality_api",
        "object_store_runtime",
        "table_lakehouse_runtime",
        "production_etl_certification",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "claim_gate_status=not_claim_grade",
        "does not add production ETL",
        "broad SQL/DataFrame runtime",
        "object-store/lakehouse runtime",
        "Foundry production support",
        "performance or superiority",
        "Spark replacement",
    ] {
        assert!(
            doc.contains(required),
            "missing GAR-0033-A ETL matrix doc field {required}"
        );
    }

    let cli = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    for required in [
        "ETL_WORKFLOW_MATRIX_SCHEMA_VERSION",
        "gar-0033-a.etl_workflow_capability_matrix",
        "etl_workflow_row_order",
        "etl_workflow_supported_local_count",
        "etl_workflow_report_only_count",
        "etl_workflow_blocked_count",
        "etl_workflow_fallback_attempted",
        "etl_workflow_external_engine_invoked",
        "etl_workflow_production_etl_claim_allowed",
    ] {
        assert!(
            cli.contains(required),
            "missing CLI ETL workflow matrix field {required}"
        );
    }

    let python_context = read_repo_file("python/src/shardloom/context.py");
    for required in [
        "ETLWorkflowCapabilityRow",
        "ETLWorkflowCapabilityMatrix",
        "ETL_WORKFLOW_CAPABILITY_ROWS",
        "def etl_workflow_matrix",
        "production_etl_claim_allowed",
        "object_store_or_table_runtime_supported",
        "all_no_fallback_no_external_engine",
    ] {
        assert!(
            python_context.contains(required),
            "missing Python ETL workflow matrix field {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    assert!(python_readme.contains("ctx.etl_workflow_matrix()"));
    assert!(python_readme.contains("object_store_runtime"));
    assert!(python_readme.contains("does not run production"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0033-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0033-A ETL workflow and data-quality capability matrix"));
    assert!(completed.contains("capabilities workflow --format json"));
    assert!(completed.contains("ctx.etl_workflow_matrix()"));
    assert!(completed.contains("fallback_attempted=false"));
    assert!(completed.contains("external_engine_invoked=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0033-A` adds `shardloom.etl_workflow_capability_matrix.v1`"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(
        traceability.contains("| GAR-0033-A | ETL workflow and data-quality capability matrix")
    );
    assert!(traceability.contains("`ctx.etl_workflow_matrix()`"));
}

#[test]
fn gar_0034_a_live_hybrid_fabric_gate_remains_claim_safe() {
    let doc = read_repo_file("docs/architecture/live-hybrid-fabric-freshness-gate.md");
    for required in [
        "shardloom.live_hybrid_fabric_freshness_gate.v1",
        "GAR-0034-A",
        "engine-capability-matrix --format json",
        "capabilities engines --format json",
        "ctx.engine_capability_matrix()",
        "live_broker_adapter",
        "live_durable_checkpoint_store",
        "live_unbounded_scheduler",
        "live_freshness_certificate",
        "live_exactly_once_claim",
        "hybrid_micro_segment_flush",
        "hybrid_object_store_commit",
        "hybrid_catalog_snapshot",
        "baseline_oracle_boundary",
        "live_hybrid_fabric_gate_blocked_row_count=7",
        "live_hybrid_fabric_gate_report_only_row_count=1",
        "live_hybrid_fabric_gate_fixture_smoke_row_count=1",
        "broker_adapter_contract",
        "durable_checkpoint_store",
        "object_store_runtime",
        "exactly_once_idempotency",
        "baseline_oracle_policy",
        "live_hybrid_fabric_gate_freshness_claim_allowed",
        "live_hybrid_fabric_gate_exactly_once_claim_allowed",
        "live_hybrid_fabric_gate_object_store_runtime_supported",
        "live_hybrid_fabric_gate_broker_runtime_supported",
        "live_hybrid_fabric_gate_state_store_runtime_supported",
        "live_hybrid_fabric_gate_baseline_oracle_only=true",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "claim_gate_status=not_claim_grade",
        "No broker adapter runtime",
        "No object-store hybrid runtime",
        "never fallback engines",
    ] {
        assert!(
            doc.contains(required),
            "missing GAR-0034-A live/hybrid gate doc field {required}"
        );
    }

    let core = read_repo_file("shardloom-core/src/engine_modes.rs");
    for required in [
        "LiveHybridFabricFreshnessGateReport",
        "LiveHybridFabricGateRow",
        "gar0034a_current",
        "shardloom.live_hybrid_fabric_freshness_gate.v1",
        "gar-0034-a.live_hybrid_fabric_freshness_gate",
        "live_broker_adapter",
        "live_freshness_certificate",
        "baseline_oracle_boundary",
        "FallbackStatus::disabled_by_policy()",
        "external_engine_invoked: false",
        "runtime_execution: false",
        "data_read: false",
        "write_io: false",
    ] {
        assert!(
            core.contains(required),
            "missing core live/hybrid gate field {required}"
        );
    }

    let cli_engine = read_repo_file("shardloom-cli/src/engine_fabric_planning.rs");
    for required in [
        "append_live_hybrid_fabric_gate_fields",
        "live_hybrid_fabric_gate_schema_version",
        "live_hybrid_fabric_gate_row_order",
        "live_hybrid_fabric_gate_blocker_ids",
        "live_hybrid_fabric_gate_required_evidence",
        "live_hybrid_fabric_gate_claim_boundary",
        "live_hybrid_fabric_gate_claim_gate_status",
        "live_hybrid_fabric_gate_freshness_claim_allowed",
        "live_hybrid_fabric_gate_exactly_once_claim_allowed",
        "live_hybrid_fabric_gate_object_store_runtime_supported",
        "live_hybrid_fabric_gate_broker_runtime_supported",
        "live_hybrid_fabric_gate_state_store_runtime_supported",
        "live_hybrid_fabric_gate_baseline_oracle_only",
        "live_hybrid_fabric_gate_fallback_attempted",
        "live_hybrid_fabric_gate_external_engine_invoked",
    ] {
        assert!(
            cli_engine.contains(required),
            "missing CLI engine live/hybrid gate field {required}"
        );
    }

    let cli_caps = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    assert!(cli_caps.contains("append_live_hybrid_fabric_gate_fields"));

    let python_client = read_repo_file("python/src/shardloom/client.py");
    for required in [
        "live_hybrid_fabric_gate_schema_version",
        "live_hybrid_fabric_gate_rows",
        "live_hybrid_fabric_gate_report_only_row_count",
        "live_hybrid_fabric_gate_claim_gate_status",
        "live_hybrid_freshness_claim_allowed",
        "live_hybrid_exactly_once_claim_allowed",
        "live_hybrid_object_store_runtime_supported",
        "live_hybrid_broker_runtime_supported",
        "live_hybrid_state_store_runtime_supported",
        "live_hybrid_fabric_gate_no_fallback_no_external_engine",
    ] {
        assert!(
            python_client.contains(required),
            "missing Python live/hybrid gate accessor {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    assert!(python_readme.contains("live_hybrid_fabric_gate_rows"));
    assert!(python_readme.contains("GAR-0034-A live/hybrid fabric"));
    assert!(python_readme.contains("claim_gate_status=not_claim_grade"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0034-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0034-A live/hybrid fabric blocker and freshness gate"));
    assert!(completed.contains("shardloom.live_hybrid_fabric_freshness_gate.v1"));
    assert!(completed.contains("baseline/oracle posture"));
    assert!(completed.contains("fallback_attempted=false"));
    assert!(completed.contains("external_engine_invoked=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0034-A` adds `shardloom.live_hybrid_fabric_freshness_gate.v1`"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("| GAR-0034-A | Live/hybrid fabric blocker and freshness gate"));
    assert!(traceability.contains("`ctx.engine_capability_matrix()`"));
}

#[test]
fn gar_0035_a_rest_runtime_unsupported_contract_remains_claim_safe() {
    let doc = read_repo_file("docs/architecture/rest-server-runtime-unsupported-contract.md");
    for required in [
        "shardloom.rest_api_runtime_unsupported_contract.v1",
        "GAR-0035-A",
        "rest-api-contract-plan --format json",
        "ctx.rest_api_contract_plan()",
        "http_listener_runtime",
        "remote_execution_runtime",
        "flight_adbc_transport_runtime",
        "external_broker_integration",
        "dependency_expanded_server",
        "openapi_discovery_contract",
        "plan_preview_contract",
        "result_delivery_contract",
        "SL_REST_SERVER_UNSUPPORTED",
        "SL_REMOTE_EXECUTION_UNSUPPORTED",
        "SL_COLUMNAR_TRANSPORT_UNSUPPORTED",
        "SL_EXTERNAL_BROKER_UNSUPPORTED",
        "SL_SERVER_DEPENDENCY_UNSUPPORTED",
        "rest_runtime_unsupported_blocked_row_count=5",
        "rest_runtime_unsupported_report_only_row_count=3",
        "server_dependency_audit",
        "listener_lifecycle_evidence",
        "execution_certificate",
        "native_io_certificate",
        "columnar_transport_certificate",
        "broker_policy",
        "rest_runtime_http_listener_supported",
        "rest_runtime_remote_execution_supported",
        "rest_runtime_flight_adbc_transport_supported",
        "rest_runtime_external_broker_supported",
        "rest_runtime_dependency_expansion_allowed",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "claim_gate_status=not_claim_grade",
        "No HTTP listener",
        "No remote execution claim",
        "cannot act as fallback engines",
    ] {
        assert!(
            doc.contains(required),
            "missing GAR-0035-A REST runtime doc field {required}"
        );
    }

    let core = read_repo_file("shardloom-core/src/remote_api.rs");
    for required in [
        "REST_API_RUNTIME_UNSUPPORTED_SCHEMA_VERSION",
        "RestApiRuntimeUnsupportedReport",
        "RestApiRuntimeUnsupportedRow",
        "gar0035a_current",
        "shardloom.rest_api_runtime_unsupported_contract.v1",
        "gar-0035-a.rest_api_runtime_unsupported_contract",
        "http_listener_runtime",
        "remote_execution_runtime",
        "dependency_expanded_server",
        "server_started: false",
        "network_listener_opened: false",
        "external_engine_invoked: false",
        "fallback_attempted: false",
    ] {
        assert!(
            core.contains(required),
            "missing core REST runtime field {required}"
        );
    }

    let cli = read_repo_file("shardloom-cli/src/rest_api_planning.rs");
    for required in [
        "RestApiRuntimeUnsupportedReport",
        "append_rest_api_runtime_unsupported_fields",
        "rest_runtime_unsupported_schema_version",
        "rest_runtime_unsupported_row_order",
        "rest_runtime_unsupported_diagnostic_codes",
        "rest_runtime_unsupported_claim_gate_status",
        "rest_runtime_http_listener_supported",
        "rest_runtime_remote_execution_supported",
        "rest_runtime_flight_adbc_transport_supported",
        "rest_runtime_external_broker_supported",
        "rest_runtime_dependency_expansion_allowed",
        "rest_runtime_external_engine_invoked",
        "rest_runtime_fallback_attempted",
    ] {
        assert!(
            cli.contains(required),
            "missing CLI REST runtime field {required}"
        );
    }

    let python_client = read_repo_file("python/src/shardloom/client.py");
    for required in [
        "rest_runtime_unsupported_schema_version",
        "rest_runtime_unsupported_rows",
        "rest_runtime_unsupported_blocked_row_count",
        "rest_runtime_unsupported_report_only_row_count",
        "rest_runtime_unsupported_diagnostic_codes",
        "rest_runtime_unsupported_claim_gate_status",
        "rest_runtime_http_listener_supported",
        "rest_runtime_remote_execution_supported",
        "rest_runtime_flight_adbc_transport_supported",
        "rest_runtime_external_broker_supported",
        "rest_runtime_dependency_expansion_allowed",
        "rest_runtime_no_server_no_fallback_no_external_engine",
    ] {
        assert!(
            python_client.contains(required),
            "missing Python REST runtime accessor {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    assert!(python_readme.contains("rest_runtime_unsupported_rows"));
    assert!(python_readme.contains("GAR-0035-A REST runtime unsupported gate"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0035-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0035-A REST server/runtime unsupported contract"));
    assert!(completed.contains("shardloom.rest_api_runtime_unsupported_contract.v1"));
    assert!(completed.contains("server_started=false"));
    assert!(completed.contains("external_engine_invoked=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0035-A` adds `shardloom.rest_api_runtime_unsupported_contract.v1`"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("| GAR-0035-A | REST server/runtime unsupported contract"));
    assert!(traceability.contains("`ctx.rest_api_contract_plan()`"));
}

#[test]
fn gar_0037_a_wrapper_connector_registry_remains_claim_safe() {
    let doc = read_repo_file("docs/architecture/wrapper-connector-implementation-registry.md");
    for required in [
        "shardloom.wrapper_connector_implementation_registry.v1",
        "GAR-0037-A",
        "shardloom capabilities api-surfaces --format json",
        "ctx.capabilities().wrapper_connector_registry",
        "ctx.wrapper_connector_registry()",
        "python_cli_json_client",
        "python_typed_capability_views",
        "python_generated_source_helpers",
        "rest_openapi_generated_client",
        "sqlalchemy",
        "flight_sql",
        "mcp",
        "wrapper_connector_registry_dependency_expansion_allowed=false",
        "wrapper_connector_registry_wrapper_ecosystem_claim_allowed=false",
        "wrapper_connector_registry_fallback_attempted=false",
        "wrapper_connector_registry_external_engine_invoked=false",
        "wrapper_connector_registry_claim_gate_status=not_claim_grade",
        "No generated clients",
        "No external engine",
        "No fallback",
    ] {
        assert!(
            doc.contains(required),
            "missing GAR-0037-A registry doc field {required}"
        );
    }

    let core = read_repo_file("shardloom-core/src/wrapper_architecture.rs");
    for required in [
        "WrapperConnectorImplementationRegistryReport",
        "WrapperConnectorRegistryRow",
        "WrapperConnectorSupportStatus",
        "gar0037a_current",
        "shardloom.wrapper_connector_implementation_registry.v1",
        "gar-0037-a.wrapper_connector_implementation_registry",
        "python_cli_json_client",
        "python_generated_source_helpers",
        "rest_openapi_generated_client",
        "SL_SQLALCHEMY_CONNECTOR_UNSUPPORTED",
        "SL_COLUMNAR_TRANSPORT_UNSUPPORTED",
        "dependency_added: false",
        "network_listener_started: false",
        "data_plane_bridge_supported",
        "external_engine_invoked: false",
        "fallback_attempted: false",
        "claim_gate_status: \"not_claim_grade\"",
    ] {
        assert!(
            core.contains(required),
            "missing core wrapper registry field {required}"
        );
    }

    let cli = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    for required in [
        "append_wrapper_connector_registry_fields",
        "wrapper_connector_registry_schema_version",
        "wrapper_connector_registry_report_id",
        "wrapper_connector_registry_row_order",
        "wrapper_connector_registry_ready_local_count",
        "wrapper_connector_registry_report_only_count",
        "wrapper_connector_registry_blocked_count",
        "wrapper_connector_registry_diagnostic_codes",
        "wrapper_connector_registry_dependency_expansion_allowed",
        "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
        "wrapper_connector_registry_all_rows_no_fallback_no_external_engine",
        "wrapper_connector_registry_claim_gate_status",
    ] {
        assert!(
            cli.contains(required),
            "missing CLI wrapper registry field {required}"
        );
    }

    let python_context = read_repo_file("python/src/shardloom/context.py");
    for required in [
        "WrapperConnectorRegistryRow",
        "WrapperConnectorRegistry",
        "def wrapper_connector_registry",
        "ready_local_count",
        "blocked_count",
        "diagnostic_codes",
        "wrapper_ecosystem_claim_allowed",
        "all_rows_no_fallback_no_external_engine",
        "no_dependency_network_or_fallback",
    ] {
        assert!(
            python_context.contains(required),
            "missing Python wrapper registry accessor {required}"
        );
    }

    let python_readme = read_repo_file("python/README.md");
    assert!(python_readme.contains("GAR-0037-A"));
    assert!(python_readme.contains("ctx.wrapper_connector_registry()"));
    assert!(python_readme.contains("SQLAlchemy"));
    assert!(python_readme.contains("claim_gate_status=not_claim_grade"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0037-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0037-A wrapper and connector implementation registry"));
    assert!(completed.contains("shardloom.wrapper_connector_implementation_registry.v1"));
    assert!(completed.contains("capabilities api-surfaces --format json"));
    assert!(completed.contains("ctx.wrapper_connector_registry()"));
    assert!(completed.contains("fallback_attempted=false"));
    assert!(completed.contains("external_engine_invoked=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(
        gar.contains(
            "`GAR-0037-A` exposes `shardloom.wrapper_connector_implementation_registry.v1`"
        )
    );

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("| GAR-0037-A | Wrapper and connector implementation registry"));
    assert!(traceability.contains("`ctx.wrapper_connector_registry()`"));
}

#[test]
fn gar_0030_a_universal_harness_execution_gate_remains_report_only() {
    let core = read_repo_file("shardloom-core/src/universal_harness.rs");
    for required in [
        "UniversalHarnessExecutionGateStatus",
        "BlockedMissingEvidence",
        "execution_gate_required_evidence_refs",
        "execution_gate_missing_evidence_refs",
        "capability_refs",
        "execution_certificate_refs",
        "native_io_certificate_refs",
        "policy_no_fallback_refs",
        "output_artifact_refs",
        "correctness_evidence_refs",
        "benchmark_evidence_refs",
        "execution_allowed: false",
        "execution_attempted: false",
    ] {
        assert!(
            core.contains(required),
            "missing universal harness execution-gate marker {required}"
        );
    }

    let cli = read_repo_file("shardloom-cli/src/evidence_certificates.rs");
    for required in [
        "universal_harness_execution_gate_status",
        "universal_harness_execution_allowed",
        "universal_harness_execution_attempted",
        "universal_harness_required_evidence_refs",
        "universal_harness_attached_evidence_refs",
        "universal_harness_missing_evidence_refs",
        "capability_evidence_required",
        "execution_certificate_required",
        "native_io_certificate_required",
        "policy_no_fallback_evidence_required",
    ] {
        assert!(
            cli.contains(required),
            "missing universal harness CLI gate field {required}"
        );
    }

    let typed_envelope = read_repo_file("shardloom-cli/src/typed_envelope.rs");
    assert!(typed_envelope.contains("universal_harness_execution_gate_status"));
    assert!(typed_envelope.contains("universal_harness_missing_evidence_refs"));

    let harness_doc =
        read_repo_file("docs/architecture/universal-import-deployment-baseline-harness.md");
    assert!(harness_doc.contains("GAR-0030-A"));
    assert!(harness_doc.contains("blocked_missing_evidence"));
    assert!(harness_doc.contains("universal_harness_execution_allowed=false"));
    assert!(harness_doc.contains("External baseline environments remain comparison-only"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0030-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0030-A universal harness execution gate"));
    assert!(completed.contains("universal_harness_execution_gate_status=blocked_missing_evidence"));
    assert!(completed.contains("External baselines remain comparison-only"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0030-A` adds `universal_harness_execution_gate_status`"));
    assert!(gar.contains("actual universal harness execution remain unimplemented"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("GAR-0030-A"));
    assert!(traceability.contains("explicit `universal_harness_execution_gate_status`"));
    assert!(traceability.contains("actual harness execution"));
}

#[test]
fn gar_0032_a_sql_parser_binder_readiness_remains_diagnostic_only() {
    let workflow = read_repo_file("shardloom-cli/src/workflow_planning.rs");
    for required in [
        "workflow_unsupported_sql_parse",
        "workflow_unsupported_sql_bind",
        "workflow_unsupported_sql_plan",
        "workflow_unsupported_sql_execute",
        "support_status",
        "claim_gate_status",
        "parser_executed",
        "binder_executed",
        "planner_executed",
        "external_engine_invoked",
        "fallback_attempted",
    ] {
        assert!(
            workflow.contains(required),
            "missing SQL readiness workflow marker {required}"
        );
    }

    let snapshots = read_repo_file("shardloom-cli/tests/workflow_query_builder_plan_snapshots.rs");
    for required in [
        "sql-parse",
        "sql-bind",
        "sql-plan",
        "sql-execute",
        "support_status",
        "claim_gate_status",
        "parser_executed",
        "binder_executed",
        "planner_executed",
    ] {
        assert!(
            snapshots.contains(required),
            "missing SQL readiness snapshot marker {required}"
        );
    }

    let doc = read_repo_file("docs/architecture/sql-parser-binder-readiness.md");
    assert!(doc.contains("GAR-0032-A"));
    assert!(doc.contains("support_status=unsupported"));
    assert!(doc.contains("claim_gate_status=not_claim_grade"));
    assert!(doc.contains("parser_executed=false"));
    assert!(doc.contains("no parser dependency"));
    assert!(doc.contains("fallback_attempted=false"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0032-A"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0032-A SQL parser/binder report-only readiness"));
    assert!(completed.contains("support_status=unsupported"));
    assert!(completed.contains("claim_gate_status=not_claim_grade"));
    assert!(completed.contains("parser_executed=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0032-A` adds `docs/architecture/sql-parser-binder-readiness.md`"));
    assert!(gar.contains("Executable SQL parser/binder/runtime"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("CG-20, GAR-0032-A"));
    assert!(traceability.contains("explicit parser/binder/planner not-executed fields"));
}

#[test]
fn gar_0032_c_udf_external_effect_blockers_remain_denied_by_default() {
    let effect_budget = read_repo_file("shardloom-core/src/effect_budget.rs");
    for required in [
        "ExternalEffectBlockerMatrix",
        "shardloom.external_effect_blocker_matrix.v1",
        "gar-0032-c.udf_external_effect_blockers",
        "support_status: \"blocked\"",
        "permission_status: \"policy_required\"",
        "effect_status: \"denied_by_default\"",
        "runtime_execution: false",
        "effect_executed: false",
        "fallback_attempted: false",
        "external_engine_invoked: false",
        "sql_udf",
        "python_udf",
        "external_service_udf",
        "api_call",
        "llm_call",
        "embedding_generation",
        "vector_search",
        "plugin_execution",
        "media_extraction",
        "network_egress",
    ] {
        assert!(
            effect_budget.contains(required),
            "missing external-effect blocker contract marker {required}"
        );
    }

    let operational = read_repo_file("shardloom-cli/src/operational_hardening.rs");
    for required in [
        "append_external_effect_blocker_matrix_fields",
        "external_effect_blocker_matrix_schema_version",
        "external_effect_blocker_all_effects_blocked",
        "external_effect_blocker_external_engine_invoked",
    ] {
        assert!(
            operational.contains(required),
            "missing effect-budget matrix field marker {required}"
        );
    }

    let capabilities = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    assert!(capabilities.contains("append_external_effect_blocker_matrix_fields"));
    assert!(capabilities.contains("CapabilityDiscoveryScope::Udfs"));
    assert!(capabilities.contains("CapabilityDiscoveryScope::SecurityGovernance"));

    let doc = read_repo_file("docs/architecture/udf-external-effect-blocker-matrix.md");
    for required in [
        "GAR-0032-C",
        "support_status=blocked",
        "permission_status=policy_required",
        "effect_status=denied_by_default",
        "runtime_execution=false",
        "effect_executed=false",
        "external_effect_blocker_external_engine_invoked=false",
        "no UDF registry",
        "no fallback execution",
    ] {
        assert!(
            doc.contains(required),
            "missing external-effect blocker doc marker {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0032-C"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0032-C UDF and external-effect blocker matrix"));
    assert!(completed.contains("shardloom.external_effect_blocker_matrix.v1"));
    assert!(completed.contains("runtime_execution=false"));
    assert!(completed.contains("effect_executed=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0032-C` adds `shardloom.external_effect_blocker_matrix.v1`"));
    assert!(gar.contains("UDF runtime, notebook runtime"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("CG-20, GAR-0032-A, GAR-0032-C"));
    assert!(traceability.contains("policy-required diagnostics"));
}

#[test]
fn gar_0032_d_unstructured_adapter_matrix_remains_report_only() {
    let capabilities = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    for required in [
        "UnstructuredAdapterCapabilityRow",
        "UNSTRUCTURED_ADAPTER_CAPABILITY_ROWS",
        "shardloom.unstructured_adapter_capability_matrix.v1",
        "gar-0032-d.unstructured_media_universal_adapter_matrix",
        "support_status: \"report-only\"",
        "support_status: \"blocked\"",
        "runtime_execution: false",
        "source_io_performed: false",
        "sink_io_performed: false",
        "append_unstructured_adapter_capability_matrix_fields",
        "CapabilityDiscoveryScope::UnstructuredMedia",
        "CapabilityDiscoveryScope::UniversalAdapters",
        "CapabilityDiscoveryScope::EventApiSaasAdapters",
        "CapabilityDiscoveryScope::ApiSurfaces",
        "document_reference",
        "text_extraction",
        "image_audio_video",
        "embedding_vector_generation",
        "vector_search",
        "universal_file_adapter",
        "database_warehouse_adapter",
        "object_store_table_adapter",
        "event_api_saas_adapter",
        "source_sink_metadata",
    ] {
        assert!(
            capabilities.contains(required),
            "missing unstructured adapter capability marker {required}"
        );
    }

    let snapshots = read_repo_file("shardloom-cli/tests/capability_discovery_snapshots.rs");
    for required in [
        "unstructured_and_adapter_capabilities_expose_report_only_matrix",
        "unstructured-media",
        "universal-adapters",
        "event-api-saas-adapters",
        "api-surfaces",
        "unstructured_adapter_capability_external_engine_invoked",
        "unstructured_adapter_capability_row_{row}_runtime_execution",
    ] {
        assert!(
            snapshots.contains(required),
            "missing unstructured adapter snapshot marker {required}"
        );
    }

    let doc = read_repo_file("docs/architecture/unstructured-adapter-capability-matrix.md");
    for required in [
        "GAR-0032-D",
        "shardloom.unstructured_adapter_capability_matrix.v1",
        "unstructured_adapter_capability_claim_gate_status=not_claim_grade",
        "unstructured_adapter_capability_runtime_execution=false",
        "unstructured_adapter_capability_source_io_performed=false",
        "unstructured_adapter_capability_sink_io_performed=false",
        "unstructured_adapter_capability_external_engine_invoked=false",
        "no document parser",
        "no fallback execution",
    ] {
        assert!(
            doc.contains(required),
            "missing unstructured adapter doc marker {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0032-D"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(
        completed.contains("GAR-0032-D unstructured/media and universal adapter capability matrix")
    );
    assert!(completed.contains("shardloom.unstructured_adapter_capability_matrix.v1"));
    assert!(completed.contains("source_io_performed=false"));
    assert!(completed.contains("sink_io_performed=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(
        gar.contains("`GAR-0032-D` adds `shardloom.unstructured_adapter_capability_matrix.v1`")
    );
    assert!(gar.contains("unstructured/media runtime"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("CG-20, GAR-0032-A, GAR-0032-C, GAR-0032-D"));
    assert!(traceability.contains("document/media/vector/adapter/source-sink metadata rows"));
}

#[test]
fn gar_0032_e_best_default_certification_gate_blocks_claim_language() {
    let core = read_repo_file("shardloom-core/src/certification.rs");
    for required in [
        "BestDefaultCertificationGateReport",
        "shardloom.best_default_certification_gate.v1",
        "gar-0032-e.best_default_certification_gate",
        "support_status: \"blocked\"",
        "claim_gate_status: \"not_claim_grade\"",
        "best_default_language_allowed: false",
        "best_default_claim_allowed: false",
        "performance_claim_allowed: false",
        "spark_replacement_claim_allowed: false",
        "fallback_attempted: false",
        "external_engine_invoked: false",
    ] {
        assert!(
            core.contains(required),
            "missing best-default core gate marker {required}"
        );
    }

    let evidence = read_repo_file("shardloom-cli/src/evidence_certificates.rs");
    for required in [
        "append_best_default_certification_gate_fields",
        "best_default_certification_gate_required_evidence",
        "best_default_certification_gate_missing_evidence",
        "best_default_language_allowed",
        "best_default_certification_gate_external_engine_invoked",
    ] {
        assert!(
            evidence.contains(required),
            "missing best-default evidence field marker {required}"
        );
    }

    let capabilities = read_repo_file("shardloom-cli/src/status_capabilities.rs");
    assert!(capabilities.contains("plan_best_default_certification_gate"));
    assert!(capabilities.contains("append_best_default_certification_gate_fields"));

    let snapshots = read_repo_file("shardloom-cli/tests/capability_discovery_snapshots.rs");
    assert!(
        snapshots.contains("certification_capabilities_expose_best_default_gate_without_claims")
    );
    assert!(snapshots.contains("best_default_certification_gate_claim_gate_status"));
    assert!(snapshots.contains("best_default_certification_gate_runtime_execution"));

    let world_class_snapshots =
        read_repo_file("shardloom-cli/tests/world_class_sufficiency_plan_snapshots.rs");
    assert!(world_class_snapshots.contains("best_default_certification_gate_required_evidence"));
    assert!(world_class_snapshots.contains("best_default_language_allowed"));

    let python_tests = read_repo_file("python/tests/test_cli_client.py");
    assert!(python_tests.contains("best_default_certification_gate_schema_version"));
    assert!(python_tests.contains("best_default_certification_gate_external_engine_invoked"));

    let doc = read_repo_file("docs/architecture/best-default-certification-gate.md");
    for required in [
        "GAR-0032-E",
        "claim_gate_status=not_claim_grade",
        "best_default_language_allowed=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "no best-default claim",
        "no performance claim",
        "no Spark replacement claim",
    ] {
        assert!(
            doc.contains(required),
            "missing best-default gate doc marker {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0032-E"));
    assert!(plan.contains("docs/architecture/best-default-certification-gate.md"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0032-E best-default certification gate"));
    assert!(completed.contains("shardloom.best_default_certification_gate.v1"));
    assert!(completed.contains("best_default_language_allowed=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("`GAR-0032-E` adds `shardloom.best_default_certification_gate.v1`"));
    assert!(gar.contains("best-default certification evidence remains incomplete"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("CG-20, GAR-0032-A, GAR-0032-C, GAR-0032-D, GAR-0032-E"));
    assert!(traceability.contains("best-default certification gate"));
}

#[test]
fn gar_0039_a_typed_envelope_api_surface_migration_remains_claim_safe() {
    let typed_doc = read_repo_file("docs/architecture/typed-command-result-envelope.md");
    for required in [
        "GAR-0039-A",
        "GAR-0039-B",
        "capabilities api-surfaces --format json",
        "api_surface_capability_report",
        "universal_harness_report",
        "universal-harness-plan --format json",
        "capability_snapshot",
        "OutputEnvelope.field_map",
        "legacy_field_map",
        "protocol payloads only",
        "do not execute benchmarks",
        "turn report-only surfaces into runtime support",
    ] {
        assert!(
            typed_doc.contains(required),
            "missing typed-envelope GAR-0039-A doc field {required}"
        );
    }

    let cli = read_repo_file("shardloom-cli/src/typed_envelope.rs");
    for required in [
        "API_SURFACE_CAPABILITY_REPORT_PAYLOAD_KEYS",
        "CAPABILITIES_API_SURFACE_SNAPSHOT_KEYS",
        "UNIVERSAL_HARNESS_REPORT_PAYLOAD_KEYS",
        "api_surface_capability_report",
        "capabilities.api_surfaces",
        "universal_harness_report",
        "universal-harness-plan.report",
        "wrapper_connector_registry_schema_version",
        "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
        "wrapper_connector_registry_fallback_attempted",
        "wrapper_connector_registry_external_engine_invoked",
        "scoped_capability_report_payload",
    ] {
        assert!(
            cli.contains(required),
            "missing typed-envelope CLI migration field {required}"
        );
    }

    let python_models = read_repo_file("python/src/shardloom/models.py");
    for required in [
        "typed `result`, `policy`, `lifecycle`, and `capability_snapshot`",
        "legacy_field_map",
        "_typed_payload_field_map",
    ] {
        assert!(
            python_models.contains(required),
            "missing Python typed-field migration marker {required}"
        );
    }

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(!plan.contains("- [ ] GAR-0039-A"));
    assert!(!plan.contains("- [ ] GAR-0039-B"));

    let completed = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(completed.contains("GAR-0039-A typed envelope API-surface migration"));
    assert!(completed.contains("GAR-0039-B typed-envelope fixtures and helper centralization"));
    assert!(completed.contains("api_surface_capability_report"));
    assert!(completed.contains("universal_harness_report"));
    assert!(completed.contains("OutputEnvelope.field_map"));
    assert!(completed.contains("Preserve the flat `fields` mirror"));
    assert!(completed.contains("external_engine_invoked=false"));

    let gar = read_repo_file("docs/architecture/global-architecture-review.md");
    assert!(gar.contains("GAR-0039-A migrates the API-surface capability family"));
    assert!(gar.contains("GAR-0039-B centralizes typed-envelope integration-test helpers"));
    assert!(gar.contains("remaining command-family result migration beyond the"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("Priority 3.9, GAR-0039-A, GAR-0039-B"));
    assert!(traceability.contains("inline `api_surface_capability_report` artifact"));
    assert!(traceability.contains("inline `universal_harness_report` artifact"));
    assert!(traceability.contains("No REST server"));
    assert!(traceability.contains("external engine invocation, or fallback execution"));
}

#[test]
fn security_rfc_and_p80_completion_are_traceable() {
    let rfc =
        read_repo_file("docs/rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md");
    assert!(rfc.contains("SEC-0 declared only"));
    assert!(rfc.contains("SEC-9 workload-certified security posture"));
    for report in [
        "SecurityThreatModelReport",
        "DependencyAuditReport",
        "SupplyChainReleaseEvidence",
        "RuntimeInputSafetyReport",
        "WorkspacePathSafetyReport",
        "EvidenceArtifactSafetyReport",
        "VulnerabilityResponseReport",
    ] {
        assert!(rfc.contains(report), "missing RFC report {report}");
    }
    for source in [
        "https://slsa.dev/spec/v1.1/requirements",
        "https://github.com/ossf/scorecard",
        "https://scvs.owasp.org/",
        "https://docs.github.com/en/code-security/concepts/code-scanning/about-code-scanning",
    ] {
        assert!(rfc.contains(source), "missing source reference {source}");
    }
    assert!(rfc.contains("fallback_attempted=false"));
    assert!(rfc.contains("external_engine_invoked=false"));
    assert!(rfc.contains("Release Blockers"));

    let plan = read_repo_file("docs/architecture/phased-execution-plan.md");
    assert!(plan.contains("docs/architecture/phased-execution-completed-ledger.md"));
    assert!(plan.contains("Global Architecture Review Carry-Forward"));
    assert!(plan.contains("docs/architecture/global-architecture-review.md"));
    assert!(plan.contains("Planned Item Detail Standard"));
    assert!(plan.contains("claim_gate_status=not_claim_grade"));
    assert!(plan.contains("support_status=unsupported|blocked|report_only"));
    assert!(plan.contains("GAR-0024-A publication and API/schema stability gate"));
    assert!(plan.contains("GAR-0043-B publication attestation and final release rehearsal"));
    let completed_ledger = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    let planned_gar_slices = planned_gar_slices(&plan);
    assert!(planned_gar_slices.len() + completed_gar_session_count(&completed_ledger) >= 32);
    assert!(
        planned_gar_slices
            .iter()
            .all(|slice| slice.contains("Evidence required:"))
    );
    assert!(!plan.contains(
        "- [x] P8.0 security, vulnerability, exploit, and supply-chain hardening bundle."
    ));

    assert!(
        completed_ledger
            .contains("GAR-0001A-B distributed/object-store/lakehouse architecture gate")
    );
    assert!(completed_ledger.contains("GAR-0003-A Vortex segment extraction admission slice"));
    assert!(completed_ledger.contains("GAR-0003-B materialization policy generalization"));
    assert!(completed_ledger.contains("GAR-0004-A CDC and manifest transaction planning gate"));
    assert!(
        completed_ledger.contains("GAR-0006-A predicate, dtype, nested, and null coverage matrix")
    );
    assert!(completed_ledger.contains("GAR-0008-A object-store byte-range provider gate"));
    assert!(completed_ledger.contains("GAR-0008-B object-store runtime blocker matrix"));
    assert!(completed_ledger.contains("GAR-0012-A diagnostic category and helper normalization"));
    assert!(completed_ledger.contains(
        "GAR-0012-B envelope status and distributed/object-store diagnostic propagation"
    ));
    assert!(
        completed_ledger
            .contains("GAR-0013-A streaming runtime capability and unsupported diagnostics")
    );
    assert!(completed_ledger.contains("GAR-0005-A local Vortex reader/writer coverage lane"));
    assert!(
        completed_ledger
            .contains("GAR-0005-B object-store Vortex I/O and upstream write integration gate")
    );
    assert!(completed_ledger.contains("GAR-0020-A table/catalog metadata admission gate"));
    assert!(
        completed_ledger.contains("GAR-0020-C local manifest-backed table metadata read smoke")
    );
    assert!(completed_ledger.contains(
        "GAR-0007-A/B compatibility output writer matrix and local fixture-smoke evidence"
    ));
    assert!(completed_ledger.contains("GAR-0016-A adaptive runtime gate consolidation"));
    assert!(completed_ledger.contains("GAR-0017-A fault-tolerance execution gate split"));
    assert!(
        completed_ledger.contains("GAR-0018-A live profiling and runtime introspection report")
    );
    assert!(
        completed_ledger.contains("GAR-0021-A approximate aggregate and sketch function admission")
    );
    assert!(
        completed_ledger.contains("GAR-0038-A facade compatibility and legacy boundary matrix")
    );
    assert!(
        completed_ledger
            .contains("GAR-0026-V selective-filter selection-vector-backed metric aggregation")
    );
    assert!(completed_ledger.contains("GAR-0014-A spill/OOM enforcement promotion gate closeout"));
    assert!(completed_ledger.contains("GAR-0026-J prepared/native global sort/top-k"));
    assert!(completed_ledger.contains("GAR-0027-A CPU/SIMD/vectorization admission slice"));
    for child in ["P8.0A/P8.0B", "P8.0C", "P8.0D", "P8.0E", "P8.0F", "P8.0G"] {
        assert!(
            completed_ledger.contains(&format!("Session label: {child}")),
            "missing completed {child}"
        );
    }
    assert!(completed_ledger.contains("P8.4 hard release-readiness gate bundle"));
    assert!(completed_ledger.contains("weaken no-fallback policy"));

    let traceability = read_repo_file("docs/architecture/rfc-phase-traceability.md");
    assert!(traceability.contains("P8.0 - security, vulnerability, exploit"));
    assert!(traceability.contains("RFC 0043 Security/Vulnerability/Exploit/Supply-Chain"));
    assert!(traceability.contains("P8.4 hard release-readiness gate is complete"));
    assert!(traceability.contains("GAR-0043 hard release-readiness validators"));
    assert!(traceability.contains("No package publication"));
    assert!(traceability.contains("docs/security/runtime-exploit-regression-suite.md"));
    assert!(traceability.contains("docs/security/release-security-gate.md"));
}

#[test]
fn security_policy_threat_model_and_supply_chain_response_are_present() {
    let security = read_repo_file("SECURITY.md");
    for required in [
        "Supported Versions",
        "Reporting A Vulnerability",
        "private security advisory",
        "acknowledgement target",
        "initial triage target",
        "Severity Categories",
        "Advisory And CVE Policy",
        "Security Release Policy",
        "User Notification Policy",
        "Compromised Package Or Dependency Response",
        "No-Fallback Security Invariant",
    ] {
        assert!(
            security.contains(required),
            "missing SECURITY.md field {required}"
        );
    }
    assert!(security.contains("Freeze publication"));
    assert!(security.contains("Verify source, package contents, checksums, SBOMs, and provenance"));
    assert!(security.contains("external engine as runtime fallback"));

    let threat_model = read_repo_file("docs/security/threat-model.md");
    for required in [
        "Malicious Vortex artifact",
        "Malformed CSV/JSONL/Parquet/Arrow/Avro/ORC",
        "Path traversal",
        "Unsafe symlink or hardlink writes",
        "Credential leakage",
        "Poisoned benchmark artifact",
        "Compromised CI/publishing workflow",
        "SecurityThreatModelReport",
        "RuntimeInputSafetyReport",
        "WorkspacePathSafetyReport",
        "EvidenceArtifactSafetyReport",
        "SEC-4 deterministic regression",
    ] {
        assert!(
            threat_model.contains(required),
            "missing threat model field {required}"
        );
    }

    let response = read_repo_file("docs/security/supply-chain-response.md");
    for required in [
        "Compromised dependency",
        "Yanked crate or package",
        "Malicious package version",
        "Compromised PyPI release",
        "Compromised Conda package",
        "Compromised GitHub release",
        "Compromised CI workflow",
        "Compromised maintainer account",
        "Freeze publication",
        "Revoke or rotate credentials",
        "Verify source, package contents, checksums, SBOMs, and provenance",
        "No-Fallback Incident Rule",
    ] {
        assert!(
            response.contains(required),
            "missing response field {required}"
        );
    }
}

#[test]
fn runtime_exploit_regression_suite_documents_report_level_security_tests() {
    let doc = read_repo_file("docs/security/runtime-exploit-regression-suite.md");
    for required in [
        "RuntimeInputSafetyReport",
        "WorkspacePathSafetyReport",
        "EvidenceArtifactSafetyReport",
        "malformed Vortex/local compatibility input blockers",
        "invalid UTF-8 blockers",
        "oversized or deeply nested input blockers",
        "path traversal rejection",
        "outside the declared workspace",
        "unsafe symlink/hardlink policy",
        "credential-like redaction",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "P8.0G/P8.4 runtime wiring",
    ] {
        assert!(
            doc.contains(required),
            "missing runtime exploit doc field {required}"
        );
    }

    let security = read_repo_file("shardloom-core/src/security.rs");
    for required in [
        "pub struct RuntimeInputSafetyReport",
        "pub struct WorkspacePathSafetyReport",
        "pub struct EvidenceArtifactSafetyReport",
        "redact_credential_like_values",
        "malformed_without_panic",
        "invalid_utf8_without_panic",
        "oversized_or_deeply_nested_blocker",
        "workspace_path_safety_rejects_parent_traversal_and_external_outputs",
        "evidence_artifact_safety_redacts_credential_like_values",
    ] {
        assert!(
            security.contains(required),
            "missing security code contract {required}"
        );
    }
}
