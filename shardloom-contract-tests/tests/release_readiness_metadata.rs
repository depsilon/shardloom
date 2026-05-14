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

#[test]
fn python_package_metadata_is_discoverable_without_runtime_dependencies() {
    let pyproject = read_repo_file("python/pyproject.toml");
    assert!(pyproject.contains("name = \"shardloom\""));
    assert!(
        pyproject.contains("Vortex-native no-fallback evidence-certified local compute engine")
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
        "description = \"Vortex-native no-fallback local compute engine",
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
        "foundry_runtime_invoked",
        "foundry_compute_invoked",
        "foundry_spark_invoked",
        "snowflake_databricks_bigquery_invoked",
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
    assert!(package_names.contains("TestPyPI Dry Run"));
    assert!(package_names.contains("Do not publish current internal crates"));
    assert!(package_names.contains("publish-approved"));
    assert!(package_names.contains("scripts\\release_dry_run_proof.py"));

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
    assert!(proof.contains("clean_conda_env_install_status"));
    assert!(proof.contains("--require-clean-conda"));

    let snapshot = read_repo_file("docs/release/first-10-minutes-smoke-snapshot.md");
    assert!(snapshot.contains("schema_version: shardloom.release_dry_run_proof.v1"));
    assert!(snapshot.contains("proof_status: passed"));
    assert!(snapshot.contains("clean_conda_env_install_status"));
    assert!(snapshot.contains("fallback_attempted=False"));
    assert!(snapshot.contains("example_local_vortex_benchmark_smoke -> 0"));
    assert!(snapshot.contains("release_provenance_dry_run -> 0"));
    assert!(snapshot.contains("provenance_dry_run_performed: true"));
    assert!(snapshot.contains("sbom_checksum_manifest_generated: true"));

    let first_ten = read_repo_file("docs/getting-started/first-10-minutes.md");
    assert!(first_ten.contains("scripts\\release_dry_run_proof.py"));
    assert!(first_ten.contains("target/release-dry-run-proof/transcript.json"));
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
        "python scripts\\foundry_proof_of_use.py",
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
        "package_install_mode",
        "transform_import_proven",
        "cli_binary_resolved",
        "staged_dataset_path_explicit",
        "supported_local_native_execution_smoke_performed",
        "certificate_metrics_dataset_output_written",
        "foundry_runtime_invoked=false",
        "foundry_compute_invoked=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "public_foundry_claim_allowed=false",
        "local_foundry_style_proof_claim_allowed",
    ] {
        assert!(
            proof.contains(required),
            "missing Foundry proof doc field {required}"
        );
    }
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
    assert!(plan.matches("- [ ] GAR-").count() >= 49);
    assert!(!plan.contains(
        "- [x] P8.0 security, vulnerability, exploit, and supply-chain hardening bundle."
    ));

    let completed_ledger = read_repo_file("docs/architecture/phased-execution-completed-ledger.md");
    assert!(
        completed_ledger
            .contains("GAR-0001A-B distributed/object-store/lakehouse architecture gate")
    );
    assert!(completed_ledger.contains("GAR-0003-A Vortex segment extraction admission slice"));
    assert!(completed_ledger.contains("GAR-0003-B materialization policy generalization"));
    assert!(
        completed_ledger.contains("GAR-0006-A predicate, dtype, nested, and null coverage matrix")
    );
    assert!(completed_ledger.contains("GAR-0008-A object-store byte-range provider gate"));
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
