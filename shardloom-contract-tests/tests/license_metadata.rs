// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

const INCOMPATIBLE_LICENSE_MARKERS: &[&str] = &[
    "AGPL",
    "BUSL",
    "Business Source",
    "GPL",
    "Proprietary",
    "SSPL",
    "source-available",
];

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

fn collect_files_named(root: &Path, file_name: &str, out: &mut Vec<PathBuf>) {
    for entry in
        fs::read_dir(root).unwrap_or_else(|err| panic!("failed to read {}: {err}", root.display()))
    {
        let entry = entry.expect("directory entry should be readable");
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if entry
            .file_type()
            .expect("entry type should be readable")
            .is_dir()
        {
            if matches!(
                name.as_ref(),
                ".git" | ".github" | ".pytest_cache" | "target" | "__pycache__"
            ) || name.starts_with("target-")
            {
                continue;
            }
            collect_files_named(&path, file_name, out);
        } else if name == file_name {
            out.push(path);
        }
    }
}

fn assert_no_incompatible_license_metadata(path: &Path, content: &str) {
    for line in content.lines() {
        let trimmed = line.trim();
        let is_license_metadata = trimmed.starts_with("license")
            || trimmed.starts_with("License")
            || trimmed.contains("license:")
            || trimmed.contains("license =");
        if !is_license_metadata {
            continue;
        }

        let haystack = trimmed.to_ascii_lowercase();
        for marker in INCOMPATIBLE_LICENSE_MARKERS {
            assert!(
                !haystack.contains(&marker.to_ascii_lowercase()),
                "{} contains incompatible license metadata marker `{marker}` in `{trimmed}`",
                path.display()
            );
        }
    }
}

#[test]
fn top_level_license_notice_and_reuse_posture_are_apache_2() {
    let license = read_repo_file("LICENSE");
    assert!(license.contains("Apache License"));
    assert!(license.contains("Version 2.0, January 2004"));
    assert!(license.contains("TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION"));
    assert!(license.contains("END OF TERMS AND CONDITIONS"));

    let notice = read_repo_file("NOTICE");
    assert!(notice.contains("ShardLoom"));
    assert!(notice.contains("Copyright 2026 Dylan Justin Heinrich"));
    assert!(notice.contains("Apache License, Version 2.0"));
    assert!(notice.contains("Third-party"));
    assert!(notice.contains("dependencies remain under their own licenses"));

    let reuse = read_repo_file("REUSE.toml");
    assert!(reuse.contains("SPDX-License-Identifier: Apache-2.0"));
    assert!(reuse.contains("version = 1"));
    assert!(reuse.contains("SPDX-License-Identifier = \"Apache-2.0\""));

    let provenance = read_repo_file("docs/legal/license-provenance.md");
    assert!(provenance.starts_with("<!-- SPDX-License-Identifier: Apache-2.0 -->"));
    assert!(provenance.contains("Benchmark-only dependencies must stay isolated"));
    assert!(provenance.contains("must not copy implementation code from GPL, AGPL, SSPL, BUSL"));
    assert!(provenance.contains("AI-assisted contributions are allowed"));
}

#[test]
fn cargo_workspace_and_crate_manifests_use_apache_2_metadata() {
    let root = repo_root();
    let workspace_manifest = read_repo_file("Cargo.toml");
    assert!(workspace_manifest.contains("[workspace.package]"));
    assert!(workspace_manifest.contains("license = \"Apache-2.0\""));

    let mut manifests = Vec::new();
    collect_files_named(&root, "Cargo.toml", &mut manifests);
    manifests.sort();
    assert!(
        manifests.len() >= 7,
        "expected root plus workspace crate manifests, got {manifests:?}"
    );

    for manifest in manifests {
        let content = fs::read_to_string(&manifest)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", manifest.display()));
        assert_no_incompatible_license_metadata(&manifest, &content);

        if manifest == root.join("Cargo.toml") {
            assert!(content.contains("license = \"Apache-2.0\""));
        } else {
            assert!(
                content.contains("license.workspace = true")
                    || content.contains("license = \"Apache-2.0\""),
                "{} must inherit or declare Apache-2.0 license metadata",
                manifest.display()
            );
        }
    }
}

#[test]
fn python_package_metadata_is_pypi_ready_and_apache_2() {
    let pyproject = read_repo_file("python/pyproject.toml");
    assert!(pyproject.contains("name = \"shardloom\""));
    assert!(pyproject.contains("description = "));
    assert!(pyproject.contains("readme = \"README.md\""));
    assert!(pyproject.contains("requires-python = \">=3.10\""));
    assert!(pyproject.contains("license = \"Apache-2.0\""));
    assert!(pyproject.contains("license-files = [\"LICENSE\", \"NOTICE\"]"));
    assert!(pyproject.contains("dependencies = []"));
    assert!(pyproject.contains("[project.urls]"));
    assert!(pyproject.contains("Repository = \"https://github.com/depsilon/shardloom\""));
    assert_no_incompatible_license_metadata(&repo_root().join("python/pyproject.toml"), &pyproject);

    let python_license = read_repo_file("python/LICENSE");
    assert!(python_license.contains("Apache License"));
    assert!(python_license.contains("Version 2.0, January 2004"));

    let python_notice = read_repo_file("python/NOTICE");
    assert!(python_notice.contains("ShardLoom Python package"));
    assert!(python_notice.contains("Apache License, Version 2.0"));
}

#[test]
fn conda_recipes_keep_apache_2_license_metadata() {
    let root = repo_root();
    let recipes = [
        root.join("packaging/conda/shardloom/meta.yaml"),
        root.join("packaging/conda/shardloom-cli/meta.yaml"),
        root.join("packaging/conda/shardloom-python/meta.yaml"),
    ];

    for recipe in recipes {
        let content = fs::read_to_string(&recipe)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", recipe.display()));
        assert!(content.contains("license: Apache-2.0"));
        assert!(content.contains("license_file"));
        assert!(content.contains("LICENSE"));
        assert_no_incompatible_license_metadata(&recipe, &content);
    }
}

#[test]
fn contributor_policy_requires_future_cla_or_approved_dco_route() {
    let root = repo_root();
    assert!(
        !root.join("DCO-1.1.txt").exists(),
        "DCO text should not be added until DCO is activated as an accepted route"
    );

    let contributing = read_repo_file("CONTRIBUTING.md");
    assert!(contributing.starts_with("<!-- SPDX-License-Identifier: Apache-2.0 -->"));
    assert!(contributing.contains("Outside contributions are not automatically accepted"));
    assert!(
        contributing
            .contains("acceptance of the ShardLoom Individual Contributor License Agreement")
    );
    assert!(contributing.contains("future DCO policy"));
    assert!(contributing.contains("No external CLA Assistant"));
    assert!(contributing.contains("Contribution Governance Controls"));
    assert!(contributing.contains("Maintainer Roles And Review States"));
    assert!(contributing.contains("Decision Escalation"));
    assert!(contributing.contains("docs/legal/contribution-intake-readiness.md"));
    assert!(contributing.contains("AI/Codex-generated content was reviewed"));
    assert!(contributing.contains("no Spark, DataFusion, DuckDB, Polars, Velox"));

    let cla = read_repo_file("CLA.md");
    assert!(cla.starts_with("<!-- SPDX-License-Identifier: Apache-2.0 -->"));
    assert!(cla.contains("Individual Contributor License Agreement"));
    assert!(cla.contains("You retain ownership of Your Contribution"));
    assert!(cla.contains("use, reproduce, modify, prepare derivative works"));
    assert!(cla.contains("distribute, sublicense"));
    assert!(cla.contains("relicense Your Contribution as part of Apache-2.0 ShardLoom"));
    assert!(cla.contains("copyright and patent grants"));
    assert!(cla.contains("use, distribute, sublicense, modify, and relicense"));
    assert!(cla.contains("patent license"));
    assert!(cla.contains("You have the legal right to submit"));
    assert!(cla.contains("employer, client, university"));
    assert!(cla.contains("AI-assisted and Codex-generated contributions"));
    assert!(cla.contains("does not change the project license away from Apache-2.0"));
    assert!(cla.contains("No external CLA Assistant is active"));

    let policy = read_repo_file("docs/legal/contributor-policy.md");
    assert!(policy.starts_with("<!-- SPDX-License-Identifier: Apache-2.0 -->"));
    assert!(policy.contains("sole maintainer using"));
    assert!(policy.contains("Codex-assisted development"));
    assert!(
        policy.contains("acceptance of the ShardLoom Individual Contributor License Agreement")
    );
    assert!(policy.contains("maintainer-approved DCO policy"));
    assert!(policy.contains("Bots, dependency update services"));
    assert!(policy.contains("exempted only by explicit maintainer policy"));
    assert!(policy.contains("must not include copied implementation code from GPL, AGPL, SSPL,"));
    assert!(policy.contains("No external CLA Assistant is active"));
    assert!(policy.contains("Contribution Intake Governance Gate"));
    assert!(policy.contains("shardloom.contribution_governance_report.v1"));
    assert!(policy.contains("legal_claim_status=documented_policy_only"));
    assert!(policy.contains("does not"));
    assert!(policy.contains("change the project license away from Apache-2.0"));

    let template = read_repo_file(".github/PULL_REQUEST_TEMPLATE.md");
    assert!(template.starts_with("<!-- SPDX-License-Identifier: Apache-2.0 -->"));
    assert!(template.contains("- [ ] I have the right to submit this contribution."));
    assert!(template.contains("Contribution Route"));
    assert!(template.contains("required signoff/CLA/DCO state"));
    assert!(template.contains("does not include copied implementation code"));
    assert!(template.contains("AI/Codex-assisted content"));
    assert!(template.contains("No-Fallback And Dependency Check"));
    assert!(template.contains("Security, Release, And RFC Impact"));
    assert!(template.contains("Claim Boundary"));
    assert!(template.contains("Reviewer State"));
    assert!(template.contains("runtime fallback dependency"));
    assert!(template.contains("Tests Run"));

    let readiness = read_repo_file("docs/legal/contribution-intake-readiness.md");
    assert!(readiness.contains("shardloom.contribution_governance_report.v1"));
    assert!(readiness.contains("contribution_intake_status=documented_and_ci_checked"));
    assert!(
        readiness.contains("external_contribution_acceptance_status=maintainer_approval_required")
    );
    assert!(readiness.contains("cla_assistant_status=not_active"));
    assert!(readiness.contains("dco_policy_status=not_active"));
    assert!(readiness.contains("legal_claim_status=documented_policy_only"));
    assert!(readiness.contains("automated_control=ci_contribution_governance_validator"));
    assert!(readiness.contains("blocked_control=external_cla_assistant"));
    assert!(readiness.contains("fallback_attempted=false"));
    assert!(readiness.contains("external_engine_invoked=false"));
}
