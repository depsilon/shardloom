// SPDX-License-Identifier: Apache-2.0

use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
    let workspace_manifest = manifest_dir
        .parent()
        .expect("shardloom-vortex lives under the workspace root")
        .join("Cargo.toml");
    println!("cargo:rerun-if-changed={}", workspace_manifest.display());

    let workspace_manifest_text = fs::read_to_string(&workspace_manifest)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", workspace_manifest.display()));
    let vortex_version = workspace_dependency_version(&workspace_manifest_text, "vortex")
        .unwrap_or_else(|| {
            panic!(
                "{} is missing [workspace.dependencies] vortex version",
                workspace_manifest.display()
            )
        });
    println!("cargo:rustc-env=SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION={vortex_version}");
}

fn workspace_dependency_version(manifest: &str, dependency: &str) -> Option<String> {
    let mut section = "";
    for line in manifest.lines() {
        let stripped = line.split('#').next().unwrap_or("").trim();
        if stripped.starts_with('[') && stripped.ends_with(']') {
            section = stripped.trim_matches(['[', ']']);
            continue;
        }
        if section != "workspace.dependencies" {
            continue;
        }
        let Some((key, raw_value)) = stripped.split_once('=') else {
            continue;
        };
        if key.trim() != dependency {
            continue;
        }
        return dependency_version(raw_value.trim());
    }
    None
}

fn dependency_version(raw_value: &str) -> Option<String> {
    if raw_value.starts_with('"') {
        return quoted_value(raw_value);
    }
    let version_index = raw_value.find("version")?;
    quoted_value(&raw_value[version_index..])
}

fn quoted_value(raw_value: &str) -> Option<String> {
    let start = raw_value.find('"')? + 1;
    let rest = &raw_value[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}
