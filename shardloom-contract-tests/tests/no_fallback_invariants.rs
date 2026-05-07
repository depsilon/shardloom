use std::fs;
use std::path::PathBuf;

use shardloom_core::{
    BaselineEngine, CapabilityCertificationReport, CorrectnessValidationPlan, Diagnostic,
    DiagnosticCode, DifferentialBaseline, EngineCapabilities, NoFallbackReleaseCheck,
    OutputEnvelope, ReleasePlan,
};

const FORBIDDEN_FALLBACK_PACKAGES: [&str; 10] = [
    "spark",
    "datafusion",
    "vortex-datafusion",
    "duckdb",
    "polars",
    "velox",
    "trino",
    "dask",
    "ray",
    "calcite",
];

#[test]
fn fallback_execution_remains_disabled_everywhere() {
    assert!(!EngineCapabilities::current().fallback_execution_allowed());
    assert!(!BaselineEngine::Spark.is_fallback_allowed());
    assert!(!BaselineEngine::DataFusion.is_fallback_allowed());
    assert!(!BaselineEngine::DuckDb.is_fallback_allowed());
    assert!(!DifferentialBaseline::new(BaselineEngine::Spark).is_fallback_allowed());
    assert!(!CorrectnessValidationPlan::default_foundation_plan().fallback_execution_allowed());

    let envelope = OutputEnvelope::success("status", "ok", "ok");
    assert!(!envelope.fallback.allowed);

    let d = Diagnostic::unsupported(DiagnosticCode::NotImplemented, "x", "y", Some("z".into()));
    assert!(!d.fallback.attempted);

    assert!(NoFallbackReleaseCheck::clean().is_clean());
    let plan = ReleasePlan::default_foundation_plan();
    assert!(plan.no_fallback_check.is_clean());
    assert!(!plan.publish_allowed());

    let certification = CapabilityCertificationReport::contract_only();
    assert!(!certification.fallback_attempted());
    assert!(!certification.can_publish_best_choice_claim());
    assert!(!certification.sql_coverage.fallback_attempted);
    assert!(!certification.operator_coverage.fallback_attempted);
    assert!(!certification.function_coverage.fallback_attempted);
    assert!(!certification.adapter_certification.fallback_attempted);
    assert!(!certification.best_choice_scorecard.fallback_attempted);
    assert!(
        certification
            .sql_coverage
            .entries
            .iter()
            .all(|entry| !entry.fallback_attempted)
    );
    assert!(
        certification
            .operator_coverage
            .entries
            .iter()
            .all(|entry| !entry.fallback_attempted)
    );
    assert!(
        certification
            .function_coverage
            .entries
            .iter()
            .all(|entry| !entry.fallback_attempted)
    );
    assert!(
        certification
            .adapter_certification
            .entries
            .iter()
            .all(|entry| !entry.fallback_attempted)
    );
}

#[test]
fn workspace_manifests_do_not_declare_forbidden_fallback_dependencies() {
    for manifest_path in workspace_manifest_paths() {
        let manifest = read(&manifest_path);
        for forbidden_name in FORBIDDEN_FALLBACK_PACKAGES {
            assert!(
                !manifest_has_forbidden_dependency(&manifest, forbidden_name),
                "forbidden dependency '{forbidden_name}' found in {}",
                manifest_path.display()
            );
        }
    }
}

#[test]
fn cargo_lock_has_no_forbidden_fallback_packages() {
    let lockfile_path = workspace_root().join("Cargo.lock");
    let lockfile = read(&lockfile_path);

    for forbidden_name in FORBIDDEN_FALLBACK_PACKAGES {
        assert!(
            !lockfile_has_forbidden_package(&lockfile, forbidden_name),
            "forbidden package '{forbidden_name}' found in {}",
            lockfile_path.display()
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("contract tests crate is in workspace root")
        .to_path_buf()
}

fn workspace_manifest_paths() -> Vec<PathBuf> {
    vec![
        workspace_root().join("Cargo.toml"),
        workspace_root().join("shardloom-core/Cargo.toml"),
        workspace_root().join("shardloom-plan/Cargo.toml"),
        workspace_root().join("shardloom-exec/Cargo.toml"),
        workspace_root().join("shardloom-vortex/Cargo.toml"),
        workspace_root().join("shardloom-cli/Cargo.toml"),
        workspace_root().join("shardloom-contract-tests/Cargo.toml"),
    ]
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn manifest_has_forbidden_dependency(text: &str, name: &str) -> bool {
    let dep_line = format!("{name} =");
    let quoted_dep_line = format!("\"{name}\" =");
    let dep_table = format!("[dependencies.{name}]");
    let workspace_dep_table = format!("[workspace.dependencies.{name}]");

    let mut in_runtime_dep_section = false;
    text.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_runtime_dep_section =
                matches!(trimmed, "[dependencies]" | "[workspace.dependencies]");
            return trimmed == dep_table || trimmed == workspace_dep_table;
        }

        in_runtime_dep_section
            && (trimmed.starts_with(&dep_line) || trimmed.starts_with(&quoted_dep_line))
    })
}

fn lockfile_has_forbidden_package(text: &str, name: &str) -> bool {
    let package_name_prefix = format!("name = \"{name}");
    text.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with(&package_name_prefix)
            && trimmed.ends_with('"')
            && matches!(
                trimmed
                    .as_bytes()
                    .get(package_name_prefix.len())
                    .copied()
                    .map(char::from),
                Some('"') | Some('-') | Some('_')
            )
    })
}

// Dependency invariant checks intentionally inspect only Cargo manifests and Cargo.lock.
// Documentation references (for example systems-learning conceptual references) are excluded.
