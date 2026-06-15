//! Metadata-first CI work-shaping planner.
//!
//! The planner classifies changed repository paths into capillary work families,
//! records pulseweave-style evidence fingerprints, and recommends CI lanes. It
//! does not execute runtime code, run benchmarks, publish packages, create tags,
//! probe networks, or invoke external engines.

use std::{
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError};

use crate::cli_output::{emit, emit_error};

const COMMAND: &str = "ci-work-shaping-plan";
const SCHEMA_VERSION: &str = "shardloom.ci_work_shaping_plan.v1";
const REPORT_ID: &str = "ci-work-shaping.metadata-first.v1";
const DOCS_REF: &str = "docs/release/ci-work-shaping.md";
const CHANGE_FAMILY_ORDER: &[ChangeFamily] = &[
    ChangeFamily::RustRuntime,
    ChangeFamily::RustTests,
    ChangeFamily::PythonSurface,
    ChangeFamily::WebsiteDocs,
    ChangeFamily::BenchmarkHarness,
    ChangeFamily::BenchmarkArtifact,
    ChangeFamily::ReleasePackaging,
    ChangeFamily::CiWorkflow,
    ChangeFamily::DependencySecurity,
    ChangeFamily::DocsOnly,
    ChangeFamily::Other,
];
const ALWAYS_ON_METADATA_GATE_ORDER: &[&str] = &[
    "no_fallback_invariant",
    "unsupported_row_scan",
    "claim_grade_metadata",
    "benchmark_publication_claim_metadata",
    "ci_matrix_drift",
    "release_boundary",
];
const CONTRACT_INPUTS: &[&str] = &[
    ".github/workflows/ci.yml",
    "docs/release/ci-gate-matrix.md",
    "Cargo.toml",
    "python/pyproject.toml",
    "website/assets/benchmarks/latest/manifest.json",
];
const MERGE_HARD_LANE_JOB_ORDER: &[&str] = &[
    "rust-baseline",
    "rust-feature-matrix",
    "rust-msrv",
    "python-test-shards",
    "python-tests",
    "python-compatibility-matrix",
    "python-package",
    "dependency-security",
    "release-runtime-core",
    "release-benchmark-claim",
    "website-docs",
    "release-package-governance",
    "release-user-surface",
    "release-readiness",
];
const RELEASE_PROOF_LANE_JOB_ORDER: &[&str] = &[
    "dependency-security",
    "python-test-shards",
    "python-tests",
    "python-package",
    "release-runtime-core",
    "release-benchmark-claim",
    "website-docs",
    "release-package-governance",
    "release-user-surface",
    "release-readiness",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CiMode {
    PullRequest,
    Merge,
    Release,
}

impl CiMode {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value {
            "pull_request" | "pull-request" | "pr" => Ok(Self::PullRequest),
            "merge" | "main" | "push" => Ok(Self::Merge),
            "release" | "release_proof" | "release-proof" => Ok(Self::Release),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported ci-work-shaping mode: {value}"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::PullRequest => "pull_request",
            Self::Merge => "merge",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeFamily {
    RustRuntime,
    RustTests,
    PythonSurface,
    WebsiteDocs,
    BenchmarkHarness,
    BenchmarkArtifact,
    ReleasePackaging,
    CiWorkflow,
    DependencySecurity,
    DocsOnly,
    Other,
}

impl ChangeFamily {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RustRuntime => "rust_runtime",
            Self::RustTests => "rust_tests",
            Self::PythonSurface => "python_surface",
            Self::WebsiteDocs => "website_docs",
            Self::BenchmarkHarness => "benchmark_harness",
            Self::BenchmarkArtifact => "benchmark_artifact",
            Self::ReleasePackaging => "release_packaging",
            Self::CiWorkflow => "ci_workflow",
            Self::DependencySecurity => "dependency_security",
            Self::DocsOnly => "docs_only",
            Self::Other => "other",
        }
    }

    const fn is_hard_gate_family(self) -> bool {
        matches!(
            self,
            Self::RustRuntime
                | Self::RustTests
                | Self::PythonSurface
                | Self::BenchmarkHarness
                | Self::ReleasePackaging
                | Self::CiWorkflow
                | Self::DependencySecurity
                | Self::Other
        )
    }

    const fn is_release_proof_family(self) -> bool {
        matches!(
            self,
            Self::ReleasePackaging | Self::CiWorkflow | Self::DependencySecurity
        )
    }

    const fn requires_benchmark_rerun(self) -> bool {
        matches!(
            self,
            Self::RustRuntime | Self::PythonSurface | Self::BenchmarkHarness
        )
    }
}

#[derive(Debug)]
struct CiWorkShapingInput {
    mode: CiMode,
    changed_paths: Vec<String>,
}

#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
struct CiWorkShapingPlan {
    mode: CiMode,
    changed_paths: Vec<String>,
    families: Vec<ChangeFamily>,
    selected_jobs: Vec<&'static str>,
    merge_hard_lane_required: bool,
    release_proof_lane_required: bool,
    benchmark_rerun_required: bool,
    benchmark_artifact_scan_required: bool,
    website_smoke_required: bool,
    docs_only_candidate: bool,
    unknown_path_hard_gate_required: bool,
    fingerprint: EvidenceFingerprint,
}

#[derive(Debug)]
struct EvidenceFingerprint {
    cache_key: String,
    file_read_count: usize,
    missing_input_count: usize,
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct JobSelectionRequirements {
    merge_hard_lane_required: bool,
    release_proof_lane_required: bool,
    benchmark_artifact_scan_required: bool,
    website_smoke_required: bool,
}

pub(crate) fn handle_ci_work_shaping_plan(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let input = match parse_args(args) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                COMMAND,
                format,
                "CI work-shaping plan argument parsing failed",
                &error,
            );
        }
    };
    let plan = build_plan(input);
    emit(
        COMMAND,
        format,
        CommandStatus::Success,
        "CI work-shaping plan".to_string(),
        human_text(&plan),
        vec![],
        fields(&plan),
    );
    ExitCode::SUCCESS
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<CiWorkShapingInput, ShardLoomError> {
    let mut mode = CiMode::PullRequest;
    let mut changed_paths = Vec::new();
    let mut iter = args;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--mode" => {
                let Some(value) = iter.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "ci-work-shaping-plan missing --mode value".to_string(),
                    ));
                };
                mode = CiMode::parse(&value)?;
            }
            "--changed-path" => {
                let Some(value) = iter.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "ci-work-shaping-plan missing --changed-path value".to_string(),
                    ));
                };
                add_changed_path(&mut changed_paths, &value);
            }
            "--changed-paths-file" => {
                let Some(value) = iter.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "ci-work-shaping-plan missing --changed-paths-file value".to_string(),
                    ));
                };
                let raw = fs::read_to_string(&value).map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to read changed-paths file {value}: {error}"
                    ))
                })?;
                for line in raw.lines() {
                    add_changed_path(&mut changed_paths, line);
                }
            }
            value => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "ci-work-shaping-plan unknown argument/value: {value}"
                )));
            }
        }
    }
    Ok(CiWorkShapingInput {
        mode,
        changed_paths,
    })
}

fn add_changed_path(paths: &mut Vec<String>, value: &str) {
    let normalized = normalize_path(value);
    if !normalized.is_empty() && !paths.contains(&normalized) {
        paths.push(normalized);
    }
}

fn normalize_path(value: &str) -> String {
    value.trim().trim_start_matches("./").replace('\\', "/")
}

fn build_plan(input: CiWorkShapingInput) -> CiWorkShapingPlan {
    let mut families = Vec::new();
    for path in &input.changed_paths {
        for family in classify_path(path) {
            push_unique(&mut families, family);
        }
    }
    if families.is_empty() {
        families.push(ChangeFamily::Other);
    }
    sort_families(&mut families);

    let docs_only_candidate = families
        .iter()
        .all(|family| matches!(family, ChangeFamily::DocsOnly | ChangeFamily::WebsiteDocs))
        && families.contains(&ChangeFamily::DocsOnly);
    let unknown_path_hard_gate_required = families.contains(&ChangeFamily::Other);
    let benchmark_rerun_required = families
        .iter()
        .any(|family| family.requires_benchmark_rerun());
    let benchmark_artifact_scan_required = families.iter().any(|family| {
        matches!(
            family,
            ChangeFamily::BenchmarkArtifact
                | ChangeFamily::BenchmarkHarness
                | ChangeFamily::RustRuntime
                | ChangeFamily::PythonSurface
        )
    });
    let website_smoke_required = families.iter().any(|family| {
        matches!(
            family,
            ChangeFamily::WebsiteDocs | ChangeFamily::BenchmarkArtifact
        )
    });
    let merge_hard_lane_required = input.mode != CiMode::PullRequest
        || families.iter().any(|family| family.is_hard_gate_family());
    let release_proof_lane_required = input.mode == CiMode::Release
        || families
            .iter()
            .any(|family| family.is_release_proof_family());

    let selected_jobs = selected_jobs(
        &families,
        JobSelectionRequirements {
            merge_hard_lane_required,
            release_proof_lane_required,
            benchmark_artifact_scan_required,
            website_smoke_required,
        },
    );
    let fingerprint = fingerprint(input.mode, &input.changed_paths);

    CiWorkShapingPlan {
        mode: input.mode,
        changed_paths: input.changed_paths,
        families,
        selected_jobs,
        merge_hard_lane_required,
        release_proof_lane_required,
        benchmark_rerun_required,
        benchmark_artifact_scan_required,
        website_smoke_required,
        docs_only_candidate,
        unknown_path_hard_gate_required,
        fingerprint,
    }
}

fn classify_path(path: &str) -> Vec<ChangeFamily> {
    let path = normalize_path(path);
    let mut families = Vec::new();
    if path.starts_with(".github/workflows/")
        || path == ".github/dependabot.yml"
        || path == "scripts/check_ci_gate_matrix.py"
        || path == "scripts/check_ci_work_shaping.py"
    {
        families.push(ChangeFamily::CiWorkflow);
    }
    if path == "Cargo.toml"
        || path == "Cargo.lock"
        || path.starts_with("shardloom-core/src/")
        || path.starts_with("shardloom-exec/src/")
        || path.starts_with("shardloom-plan/src/")
        || path.starts_with("shardloom-vortex/src/")
        || path.starts_with("shardloom-cli/src/")
    {
        families.push(ChangeFamily::RustRuntime);
    }
    if path.starts_with("shardloom-cli/tests/")
        || path.starts_with("shardloom-contract-tests/tests/")
        || path.starts_with("shardloom-vortex/tests/")
    {
        families.push(ChangeFamily::RustTests);
    }
    if path.starts_with("python/src/")
        || path.starts_with("python/tests/")
        || path.starts_with("examples/")
    {
        families.push(ChangeFamily::PythonSurface);
    }
    if path == "README.md"
        || path.starts_with("website-src/")
        || path.starts_with("website/")
        || path.starts_with("docs/getting-started/")
        || path.starts_with("docs/status/")
    {
        families.push(ChangeFamily::WebsiteDocs);
    }
    if path.starts_with("benchmarks/")
        || (path.starts_with("scripts/") && path.contains("benchmark"))
        || path.starts_with("docs/architecture/performance-")
        || path.starts_with("docs/architecture/benchmark-")
    {
        families.push(ChangeFamily::BenchmarkHarness);
    }
    if path.starts_with("website/assets/benchmarks/") || path.starts_with("docs/benchmarks/") {
        families.push(ChangeFamily::BenchmarkArtifact);
    }
    if path.starts_with("docs/release/")
        || path == "python/pyproject.toml"
        || path == "python/src/shardloom/_version.py"
        || path.starts_with(".github/workflows/pypi")
        || path.starts_with("scripts/release")
        || path.starts_with("scripts/check_release")
        || path.starts_with("scripts/check_package")
    {
        families.push(ChangeFamily::ReleasePackaging);
    }
    if path == "deny.toml"
        || path == "Cargo.lock"
        || path.ends_with("requirements.txt")
        || path.ends_with("package-lock.json")
        || path.contains("dependency")
        || path.contains("security")
    {
        families.push(ChangeFamily::DependencySecurity);
    }
    if path == "README.md"
        || path.starts_with("docs/")
        || Path::new(&path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
    {
        families.push(ChangeFamily::DocsOnly);
    }
    if families.is_empty() {
        families.push(ChangeFamily::Other);
    }
    families
}

fn selected_jobs(
    families: &[ChangeFamily],
    requirements: JobSelectionRequirements,
) -> Vec<&'static str> {
    let mut jobs = vec!["ci-work-shaping", "ci-gate-matrix"];
    if families.iter().any(|family| {
        matches!(
            family,
            ChangeFamily::RustRuntime | ChangeFamily::RustTests | ChangeFamily::CiWorkflow
        )
    }) {
        push_unique_str(&mut jobs, "rust-baseline");
        push_unique_str(&mut jobs, "rust-feature-matrix");
    }
    if families
        .iter()
        .any(|family| matches!(family, ChangeFamily::PythonSurface))
    {
        push_unique_str(&mut jobs, "python-tests");
        push_unique_str(&mut jobs, "python-package");
    }
    if requirements.website_smoke_required {
        push_unique_str(&mut jobs, "website-docs");
    }
    if requirements.benchmark_artifact_scan_required {
        push_unique_str(&mut jobs, "release-benchmark-claim");
    }
    if requirements.merge_hard_lane_required {
        push_unique_strs(&mut jobs, MERGE_HARD_LANE_JOB_ORDER);
    }
    if requirements.release_proof_lane_required {
        push_unique_strs(&mut jobs, RELEASE_PROOF_LANE_JOB_ORDER);
    }
    jobs
}

#[allow(clippy::too_many_lines)]
fn fields(plan: &CiWorkShapingPlan) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), "ci_work_shaping_plan".to_string()),
        ("schema_version".to_string(), SCHEMA_VERSION.to_string()),
        ("report_id".to_string(), REPORT_ID.to_string()),
        ("docs_ref".to_string(), DOCS_REF.to_string()),
        ("ci_mode".to_string(), plan.mode.as_str().to_string()),
        (
            "changed_path_count".to_string(),
            plan.changed_paths.len().to_string(),
        ),
        (
            "changed_path_order".to_string(),
            join_strings(&plan.changed_paths),
        ),
        (
            "capillary_selection_status".to_string(),
            "enabled".to_string(),
        ),
        (
            "capillary_family_order".to_string(),
            family_order(&plan.families),
        ),
        (
            "capillary_family_count".to_string(),
            plan.families.len().to_string(),
        ),
        (
            "dynamic_admission_strategy".to_string(),
            "metadata_first_changed_file_map".to_string(),
        ),
        (
            "pulseweave_incremental_evidence_status".to_string(),
            "enabled_with_content_fingerprint".to_string(),
        ),
        (
            "pulseweave_cache_key".to_string(),
            plan.fingerprint.cache_key.clone(),
        ),
        (
            "pulseweave_cache_fingerprint_kind".to_string(),
            "fnv1a64_non_crypto_change_set_and_contract_inputs".to_string(),
        ),
        (
            "pulseweave_input_file_read_count".to_string(),
            plan.fingerprint.file_read_count.to_string(),
        ),
        (
            "pulseweave_missing_input_count".to_string(),
            plan.fingerprint.missing_input_count.to_string(),
        ),
        (
            "always_on_metadata_gate_order".to_string(),
            ALWAYS_ON_METADATA_GATE_ORDER.join(","),
        ),
        (
            "no_fallback_metadata_gate_required".to_string(),
            "true".to_string(),
        ),
        (
            "unsupported_rows_metadata_gate_required".to_string(),
            "true".to_string(),
        ),
        (
            "claim_grade_metadata_gate_required".to_string(),
            "true".to_string(),
        ),
        (
            "benchmark_metadata_gate_required".to_string(),
            "true".to_string(),
        ),
        (
            "benchmark_rerun_required".to_string(),
            plan.benchmark_rerun_required.to_string(),
        ),
        (
            "benchmark_artifact_scan_required".to_string(),
            plan.benchmark_artifact_scan_required.to_string(),
        ),
        (
            "source_aware_benchmark_policy".to_string(),
            "rerun_on_runtime_fixture_runner_or_timing_surface_changes;metadata_only_for_docs_copy_changes".to_string(),
        ),
        (
            "website_smoke_required".to_string(),
            plan.website_smoke_required.to_string(),
        ),
        (
            "docs_only_candidate".to_string(),
            plan.docs_only_candidate.to_string(),
        ),
        (
            "unknown_path_hard_gate_required".to_string(),
            plan.unknown_path_hard_gate_required.to_string(),
        ),
        (
            "pr_fast_lane_required".to_string(),
            "true".to_string(),
        ),
        (
            "merge_hard_lane_required".to_string(),
            plan.merge_hard_lane_required.to_string(),
        ),
        (
            "release_proof_lane_required".to_string(),
            plan.release_proof_lane_required.to_string(),
        ),
        (
            "recommended_job_order".to_string(),
            plan.selected_jobs.join(","),
        ),
        (
            "hard_gate_preserved".to_string(),
            "true".to_string(),
        ),
        (
            "fast_lane_authorizes_merge".to_string(),
            "false".to_string(),
        ),
        (
            "release_lane_authorizes_publication".to_string(),
            "false".to_string(),
        ),
        ("runtime_execution".to_string(), "false".to_string()),
        ("benchmark_run_performed".to_string(), "false".to_string()),
        ("publication_attempted".to_string(), "false".to_string()),
        ("tag_created".to_string(), "false".to_string()),
        ("package_upload_attempted".to_string(), "false".to_string()),
        ("filesystem_write_performed".to_string(), "false".to_string()),
        ("network_probe_performed".to_string(), "false".to_string()),
        ("fallback_execution_allowed".to_string(), "false".to_string()),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("side_effect_free".to_string(), "true".to_string()),
    ]
}

fn human_text(plan: &CiWorkShapingPlan) -> String {
    format!(
        "ShardLoom CI work shaping\nci_mode={}\ncapillary_family_order={}\nrecommended_job_order={}\nmerge_hard_lane_required={}\nrelease_proof_lane_required={}\nbenchmark_rerun_required={}\nbenchmark_artifact_scan_required={}\nhard_gate_preserved=true\nruntime_execution=false\nfallback_attempted=false",
        plan.mode.as_str(),
        family_order(&plan.families),
        plan.selected_jobs.join(","),
        plan.merge_hard_lane_required,
        plan.release_proof_lane_required,
        plan.benchmark_rerun_required,
        plan.benchmark_artifact_scan_required,
    )
}

fn fingerprint(mode: CiMode, changed_paths: &[String]) -> EvidenceFingerprint {
    let mut state = Fnv1a64::new();
    state.update(mode.as_str().as_bytes());
    let mut file_read_count = 0;
    let mut missing_input_count = 0;
    for path in changed_paths {
        update_path_fingerprint(
            path,
            &mut state,
            &mut file_read_count,
            &mut missing_input_count,
        );
    }
    for path in CONTRACT_INPUTS {
        update_path_fingerprint(
            path,
            &mut state,
            &mut file_read_count,
            &mut missing_input_count,
        );
    }
    EvidenceFingerprint {
        cache_key: format!("ci-work-shaping-{:016x}", state.finish()),
        file_read_count,
        missing_input_count,
    }
}

fn update_path_fingerprint(
    path: &str,
    state: &mut Fnv1a64,
    file_read_count: &mut usize,
    missing_input_count: &mut usize,
) {
    state.update(path.as_bytes());
    let local_path = PathBuf::from(path);
    if let Some(bytes) = read_file_if_present(&local_path) {
        *file_read_count += 1;
        state.update(b":present:");
        state.update(&bytes);
    } else {
        *missing_input_count += 1;
        state.update(b":missing:");
    }
}

fn read_file_if_present(path: &Path) -> Option<Vec<u8>> {
    if path.is_file() {
        fs::read(path).ok()
    } else {
        None
    }
}

struct Fnv1a64(u64);

impl Fnv1a64 {
    const fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    const fn finish(&self) -> u64 {
        self.0
    }
}

fn family_order(families: &[ChangeFamily]) -> String {
    families
        .iter()
        .map(|family| family.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_strings(values: &[String]) -> String {
    values.join(",")
}

fn sort_families(families: &mut [ChangeFamily]) {
    families.sort_by_key(|family| {
        CHANGE_FAMILY_ORDER
            .iter()
            .position(|candidate| candidate == family)
            .unwrap_or(usize::MAX)
    });
}

fn push_unique(families: &mut Vec<ChangeFamily>, family: ChangeFamily) {
    if !families.contains(&family) {
        families.push(family);
    }
}

fn push_unique_str(values: &mut Vec<&'static str>, value: &'static str) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn push_unique_strs(values: &mut Vec<&'static str>, candidates: &[&'static str]) {
    for candidate in candidates {
        push_unique_str(values, candidate);
    }
}
