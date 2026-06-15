mod support;

use std::{fs, path::PathBuf};

use support::{assert_common_typed_slots, field, run_command};

fn run_ci_work_shaping(args: &[&str]) -> String {
    let mut command_args = vec!["ci-work-shaping-plan"];
    command_args.extend_from_slice(args);
    command_args.extend_from_slice(&["--format", "json"]);
    run_command(&command_args, true)
}

fn temp_changed_paths_file(name: &str, content: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("shardloom-{}-{name}", std::process::id()));
    fs::write(&path, content).expect("changed paths fixture can be written");
    path
}

#[test]
fn docs_only_change_uses_fast_metadata_and_website_lane_without_benchmark_rerun() {
    let output = run_ci_work_shaping(&[
        "--changed-path",
        "docs/getting-started/install.md",
        "--changed-path",
        "README.md",
    ]);

    assert_common_typed_slots(&output, "ci-work-shaping-plan", "success");
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.ci_work_shaping_plan.v1"
    )));
    assert!(output.contains(&field("ci_mode", "pull_request")));
    assert!(output.contains(&field("capillary_selection_status", "enabled")));
    assert!(output.contains(&field("capillary_family_order", "website_docs,docs_only")));
    assert!(output.contains(&field("docs_only_candidate", "true")));
    assert!(output.contains(&field("benchmark_rerun_required", "false")));
    assert!(output.contains(&field("benchmark_artifact_scan_required", "false")));
    assert!(output.contains(&field("website_smoke_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,website-docs"
    )));
    assert!(output.contains(&field("merge_hard_lane_required", "false")));
    assert!(output.contains(&field("fast_lane_authorizes_merge", "false")));
}

#[test]
fn runtime_change_requires_hard_lane_and_source_aware_benchmark_rerun() {
    let output = run_ci_work_shaping(&[
        "--mode",
        "pull_request",
        "--changed-path",
        "shardloom-exec/src/lib.rs",
    ]);

    assert!(output.contains(&field("capillary_family_order", "rust_runtime")));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field("release_proof_lane_required", "false")));
    assert!(output.contains(&field("benchmark_rerun_required", "true")));
    assert!(output.contains(&field("benchmark_artifact_scan_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn python_surface_hard_lane_preserves_python_shard_before_aggregate_order() {
    let output = run_ci_work_shaping(&["--changed-path", "python/src/shardloom/__init__.py"]);

    assert!(output.contains(&field("capillary_family_order", "python_surface")));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
}

#[test]
fn benchmark_artifact_change_scans_public_artifacts_without_declaring_rerun() {
    let output = run_ci_work_shaping(&[
        "--changed-path",
        "website/assets/benchmarks/latest/benchmark-results.json",
    ]);

    assert!(output.contains(&field(
        "capillary_family_order",
        "website_docs,benchmark_artifact"
    )));
    assert!(output.contains(&field("benchmark_rerun_required", "false")));
    assert!(output.contains(&field("benchmark_artifact_scan_required", "true")));
    assert!(output.contains(&field("benchmark_metadata_gate_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,release-benchmark-claim,website-docs"
    )));
}

#[test]
fn workflow_and_release_change_escalates_to_release_proof_lane() {
    let changed_paths = temp_changed_paths_file(
        "ci-work-shaping-release-paths.txt",
        ".github/workflows/ci.yml\ndocs/release/ci-gate-matrix.md\n",
    );
    let changed_paths_arg = changed_paths.to_string_lossy().to_string();
    let output = run_ci_work_shaping(&[
        "--mode",
        "release",
        "--changed-paths-file",
        changed_paths_arg.as_str(),
    ]);

    assert!(output.contains(&field("ci_mode", "release")));
    assert!(output.contains(&field(
        "capillary_family_order",
        "release_packaging,ci_workflow,docs_only"
    )));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field("release_proof_lane_required", "true")));
    assert!(output.contains(&field("hard_gate_preserved", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
    assert!(output.contains(&field("publication_attempted", "false")));
    assert!(output.contains(&field("tag_created", "false")));
    assert!(output.contains(&field("package_upload_attempted", "false")));
}

#[test]
fn unknown_paths_fail_closed_into_hard_lane() {
    let output = run_ci_work_shaping(&["--changed-path", "tools/local-helper.sh"]);

    assert!(output.contains(&field("capillary_family_order", "other")));
    assert!(output.contains(&field("docs_only_candidate", "false")));
    assert!(output.contains(&field("unknown_path_hard_gate_required", "true")));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
}

#[test]
fn merge_mode_docs_change_recommends_hard_lane_producers() {
    let output = run_ci_work_shaping(&[
        "--mode",
        "merge",
        "--changed-path",
        "docs/getting-started/install.md",
    ]);

    assert!(output.contains(&field("ci_mode", "merge")));
    assert!(output.contains(&field("capillary_family_order", "website_docs,docs_only")));
    assert!(output.contains(&field("docs_only_candidate", "true")));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
}

#[test]
fn release_packaging_change_recommends_release_proof_producers_before_readiness() {
    let output = run_ci_work_shaping(&[
        "--mode",
        "pull_request",
        "--changed-path",
        "python/pyproject.toml",
    ]);

    assert!(output.contains(&field("capillary_family_order", "release_packaging")));
    assert!(output.contains(&field("merge_hard_lane_required", "true")));
    assert!(output.contains(&field("release_proof_lane_required", "true")));
    assert!(output.contains(&field(
        "recommended_job_order",
        "ci-work-shaping,ci-gate-matrix,rust-baseline,rust-feature-matrix,rust-msrv,python-test-shards,python-tests,python-compatibility-matrix,python-package,dependency-security,release-runtime-core,release-benchmark-claim,website-docs,release-package-governance,release-user-surface,release-readiness"
    )));
}

#[test]
fn pulseweave_fields_are_present_and_side_effect_free() {
    let output = run_ci_work_shaping(&["--changed-path", "python/src/shardloom/__init__.py"]);

    assert!(output.contains("\"key\":\"pulseweave_cache_key\",\"value\":\"ci-work-shaping-"));
    assert!(output.contains(&field(
        "pulseweave_cache_fingerprint_kind",
        "fnv1a64_non_crypto_change_set_and_contract_inputs"
    )));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("filesystem_write_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("benchmark_run_performed", "false")));
}
