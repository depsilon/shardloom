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
        "Unicode-3.0",
        "Zlib",
    ] {
        assert!(
            deny.contains(allowed),
            "missing cargo-deny allow license {allowed}"
        );
    }
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
    assert!(script.contains("--include-python-packaging"));
    assert!(script.contains("not as a ShardLoom runtime dependency assumption"));

    let dry_run = read_repo_file("scripts/release_dry_run_proof.py");
    assert!(dry_run.contains("build_python_artifacts"));
    assert!(dry_run.contains("venv"));
    assert!(dry_run.contains("pip"));
    assert!(dry_run.contains("--no-index"));
    assert!(dry_run.contains("SHARDLOOM_BIN"));
    assert!(dry_run.contains("ShardLoomClient.from_env()"));
    assert!(dry_run.contains("smoke_check()"));
    assert!(dry_run.contains("examples/local-python-smoke/run.py"));
    assert!(dry_run.contains("examples/local-vortex-benchmark/run.py"));
    assert!(dry_run.contains("publication_attempted"));
    assert!(dry_run.contains("tag_created"));
    assert!(dry_run.contains("secrets_required"));
    assert!(dry_run.contains("fallback_engine_dependency_added"));

    let policy = read_repo_file("docs/legal/dependency-audit.md");
    assert!(policy.contains("Runtime Versus Benchmark-Only Dependencies"));
    assert!(policy.contains("Vortex Dependency Boundaries"));
    assert!(policy.contains("must not"));
    assert!(policy.contains("execute unsupported ShardLoom work"));
    assert!(policy.contains("GPL, LGPL, AGPL, SSPL, BUSL"));
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
        "docs/release/first-10-minutes-smoke-snapshot.md",
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
    assert!(proof.contains("pip --no-index --find-links python/dist"));
    assert!(proof.contains("SHARDLOOM_BIN"));
    assert!(proof.contains("examples/local-vortex-benchmark"));
    assert!(proof.contains("publication_attempted"));
    assert!(proof.contains("fallback_engine_dependency_added"));

    let snapshot = read_repo_file("docs/release/first-10-minutes-smoke-snapshot.md");
    assert!(snapshot.contains("schema_version: shardloom.release_dry_run_proof.v1"));
    assert!(snapshot.contains("proof_status: passed"));
    assert!(snapshot.contains("fallback_attempted=False"));
    assert!(snapshot.contains("example_local_vortex_benchmark_smoke -> 0"));

    let first_ten = read_repo_file("docs/getting-started/first-10-minutes.md");
    assert!(first_ten.contains("scripts\\release_dry_run_proof.py"));
    assert!(first_ten.contains("target/release-dry-run-proof/transcript.json"));
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
