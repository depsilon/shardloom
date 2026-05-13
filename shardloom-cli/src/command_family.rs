//! CLI command family taxonomy for the Priority 3.9 handler split.
//!
//! This module is intentionally classification-only. It does not dispatch,
//! execute, probe, write, or authorize effects; it gives the typed envelope and
//! future handler modules a shared vocabulary for command families.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandFamily {
    StatusCapabilities,
    VortexPrimitiveExecution,
    PreparedSourceBackedExecution,
    VortexPlanning,
    VortexRuntimePlanning,
    VortexOutputCommit,
    EvidenceCertificates,
    Benchmarks,
    PackagingDeployment,
    Foundry,
    ObjectStorePlanning,
    OperationalHardening,
    Diagnostics,
    RestApiPlanning,
    WorkflowPlanning,
    InputPlanning,
    EngineRuntimePlanning,
    OptimizerPlanning,
    ExtensionPlanning,
    Other,
}

impl CommandFamily {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StatusCapabilities => "status_capabilities",
            Self::VortexPrimitiveExecution => "vortex_primitive_execution",
            Self::PreparedSourceBackedExecution => "prepared_source_backed_execution",
            Self::VortexPlanning => "vortex_planning",
            Self::VortexRuntimePlanning => "vortex_runtime_planning",
            Self::VortexOutputCommit => "vortex_output_commit",
            Self::EvidenceCertificates => "evidence_certificates",
            Self::Benchmarks => "benchmarks",
            Self::PackagingDeployment => "packaging_deployment",
            Self::Foundry => "foundry",
            Self::ObjectStorePlanning => "object_store_planning",
            Self::OperationalHardening => "operational_hardening",
            Self::Diagnostics => "diagnostics",
            Self::RestApiPlanning => "rest_api_planning",
            Self::WorkflowPlanning => "workflow_planning",
            Self::InputPlanning => "input_planning",
            Self::EngineRuntimePlanning => "engine_runtime_planning",
            Self::OptimizerPlanning => "optimizer_planning",
            Self::ExtensionPlanning => "extension_planning",
            Self::Other => "other",
        }
    }
}

#[must_use]
pub(crate) fn classify_command(command: &str) -> CommandFamily {
    if is_status_capabilities_command(command) {
        CommandFamily::StatusCapabilities
    } else if is_vortex_primitive_command(command) {
        CommandFamily::VortexPrimitiveExecution
    } else if is_prepared_source_backed_command(command) {
        CommandFamily::PreparedSourceBackedExecution
    } else if is_vortex_output_commit_command(command) {
        CommandFamily::VortexOutputCommit
    } else if is_vortex_runtime_planning_command(command) {
        CommandFamily::VortexRuntimePlanning
    } else if is_vortex_planning_command(command) {
        CommandFamily::VortexPlanning
    } else if is_evidence_certificate_command(command) {
        CommandFamily::EvidenceCertificates
    } else if is_benchmark_command(command) {
        CommandFamily::Benchmarks
    } else if is_packaging_deployment_command(command) {
        CommandFamily::PackagingDeployment
    } else if is_foundry_command(command) {
        CommandFamily::Foundry
    } else if is_object_store_planning_command(command) {
        CommandFamily::ObjectStorePlanning
    } else if is_operational_hardening_command(command) {
        CommandFamily::OperationalHardening
    } else if is_diagnostics_command(command) {
        CommandFamily::Diagnostics
    } else if command == "api-compat-plan" {
        CommandFamily::RestApiPlanning
    } else if is_input_planning_command(command) {
        CommandFamily::InputPlanning
    } else if is_workflow_planning_command(command) || command.starts_with("cg9-") {
        CommandFamily::WorkflowPlanning
    } else if is_engine_runtime_planning_command(command) {
        CommandFamily::EngineRuntimePlanning
    } else if is_optimizer_planning_command(command) {
        CommandFamily::OptimizerPlanning
    } else if is_extension_planning_command(command) {
        CommandFamily::ExtensionPlanning
    } else {
        CommandFamily::Other
    }
}

fn is_status_capabilities_command(command: &str) -> bool {
    matches!(command, "status" | "capabilities")
}

fn is_vortex_primitive_command(command: &str) -> bool {
    matches!(
        command,
        "vortex-count"
            | "vortex-count-where"
            | "vortex-project"
            | "vortex-filter"
            | "vortex-filter-project"
            | "vortex-query-trace"
            | "vortex-run"
            | "vortex-local-exec"
            | "vortex-bounded-local-exec"
    )
}

fn is_prepared_source_backed_command(command: &str) -> bool {
    matches!(
        command,
        "vortex-encoded-read-api"
            | "vortex-encoded-read-boundary"
            | "vortex-encoded-read-metadata-probe"
            | "vortex-encoded-read-readiness"
            | "vortex-encoded-read-probe"
            | "vortex-encoded-read-execute"
            | "vortex-encoded-read-spike"
    )
}

fn is_vortex_planning_command(command: &str) -> bool {
    matches!(
        command,
        "vortex-encoded-path-selection-plan"
            | "vortex-generalized-encoded-primitive-gate"
            | "vortex-metadata-execute"
            | "vortex-dry-run"
            | "vortex-plan"
            | "translation-plan"
            | "vortex-output-plan"
            | "vortex-readiness"
            | "vortex-dtype-mapping"
            | "vortex-encoding-layout-mapping"
            | "vortex-statistics-mapping"
            | "vortex-file-metadata-open"
            | "vortex-metadata-summary"
            | "vortex-query-primitive-plan"
            | "vortex-metadata-physical-kernel-plan"
            | "vortex-count-readiness-plan"
            | "vortex-encoded-count-approval-plan"
            | "vortex-layout-driver-approval-plan"
            | "vortex-filtered-count-readiness-plan"
            | "vortex-projection-readiness-plan"
            | "vortex-metadata-plan"
            | "vortex-pruning-plan"
            | "vortex-metadata-probe"
            | "vortex-api-inventory"
    )
}

fn is_vortex_runtime_planning_command(command: &str) -> bool {
    matches!(
        command,
        "vortex-adaptive-sizing"
            | "vortex-memory-plan"
            | "vortex-schedule-plan"
            | "vortex-execution-readiness"
    )
}

fn is_vortex_output_commit_command(command: &str) -> bool {
    matches!(
        command,
        "vortex-write-intent-plan"
            | "vortex-commit-intent-plan"
            | "vortex-manifest-finalization-plan"
            | "vortex-output-payload-plan"
            | "vortex-finalized-manifest-artifact-write"
            | "vortex-output-payload-artifact-write"
            | "vortex-native-count-payload-write"
            | "vortex-commit-marker-plan"
            | "vortex-commit-marker-write"
            | "vortex-commit-protocol-plan"
            | "vortex-local-commit-execute"
            | "vortex-local-commit-recovery-plan"
            | "vortex-local-commit-rollback-execute"
            | "vortex-staged-workspace-setup"
            | "vortex-staged-marker-write"
            | "vortex-staged-manifest-file-plan"
            | "vortex-staged-manifest-file-write"
    )
}

fn is_evidence_certificate_command(command: &str) -> bool {
    matches!(
        command,
        "correctness-plan"
            | "correctness-harness-plan"
            | "execution-certificate-plan"
            | "native-io-envelope-plan"
            | "benchmark-claim-evidence-plan"
            | "world-class-sufficiency-plan"
            | "cg20-user-capability-gate"
            | "cg20-approx-sketch-gate"
            | "universal-harness-plan"
            | "rfc-coverage-followthrough-plan"
    )
}

fn is_benchmark_command(command: &str) -> bool {
    matches!(
        command,
        "benchmark-plan"
            | "traditional-analytics-run"
            | "traditional-analytics-vortex-run"
            | "vortex-count-benchmark"
    )
}

fn is_packaging_deployment_command(command: &str) -> bool {
    matches!(
        command,
        "release-plan" | "package-plan" | "python-wrapper-plan" | "agent-contract-pack"
    )
}

fn is_foundry_command(command: &str) -> bool {
    matches!(
        command,
        "foundry-plan" | "foundry-smoke-plan" | "foundry-benchmark-plan"
    )
}

fn is_operational_hardening_command(command: &str) -> bool {
    matches!(
        command,
        "security-plan"
            | "security-governance-evidence-gate"
            | "effect-budget-plan"
            | "agent-safety-plan"
            | "redaction-plan"
            | "cg14-memory-runtime-hardening-gate"
            | "operator-memory-spill-declarations"
            | "spill-lifecycle"
            | "spill-reservation-plan"
            | "spill-payload-roundtrip"
            | "memory-plan"
            | "spill-plan"
            | "cleanup-synthetic-payload"
            | "fault-tolerance-promotion-gate"
            | "commit-execution-promotion-gate"
            | "recovery-plan"
            | "retry-plan"
            | "cancellation-plan"
            | "retry-gate-plan"
            | "cancellation-gate-plan"
    )
}

fn is_diagnostics_command(command: &str) -> bool {
    matches!(
        command,
        "feature-footprint"
            | "doctor"
            | "explain"
            | "estimate"
            | "profile-plan"
            | "runtime-report"
            | "observability-plan"
            | "observability-schema-coverage"
    )
}

fn is_input_planning_command(command: &str) -> bool {
    matches!(
        command,
        "input-adapters"
            | "input-plan"
            | "vortex-input-plan"
            | "vortex-read-plan"
            | "vortex-task-graph"
    )
}

fn is_workflow_planning_command(command: &str) -> bool {
    matches!(
        command,
        "schema-plan"
            | "translation-plan"
            | "plan-ir"
            | "plan-import"
            | "plan-export"
            | "catalog-plan"
            | "table-compat-plan"
            | "layout-health-plan"
            | "compaction-plan"
            | "table-intelligence-plan"
            | "incremental-plan"
            | "stateful-reuse-plan"
            | "cg17-stateful-reuse-gate"
            | "write-intent"
            | "scan-plan"
    )
}

fn is_engine_runtime_planning_command(command: &str) -> bool {
    matches!(
        command,
        "engine-selection-plan"
            | "engine-capability-matrix"
            | "live-change-contract-plan"
            | "live-fixture-run"
            | "streaming-plan"
            | "streaming-batch-plan"
            | "backpressure-plan"
            | "runtime-plan"
            | "task-plan"
            | "sizing-plan"
            | "sizing-feedback-plan"
            | "dynamic-work-shaping-plan"
            | "cg8-runtime-promotion-gate"
    )
}

fn is_optimizer_planning_command(command: &str) -> bool {
    matches!(
        command,
        "kernel-registry"
            | "optimizer-plan"
            | "optimizer-adaptive-memory-plan"
            | "cpu-specialization-plan"
    )
}

fn is_object_store_planning_command(command: &str) -> bool {
    command.starts_with("object-store-") || command.starts_with("cg10-")
}

fn is_extension_planning_command(command: &str) -> bool {
    matches!(
        command,
        "extension-registry" | "extension-inspect" | "udf-runtime-plan"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_representative_priority_39_families() {
        assert_eq!(
            classify_command("status"),
            CommandFamily::StatusCapabilities
        );
        assert_eq!(
            classify_command("vortex-filter-project"),
            CommandFamily::VortexPrimitiveExecution
        );
        assert_eq!(
            classify_command("vortex-query-trace"),
            CommandFamily::VortexPrimitiveExecution
        );
        assert_eq!(
            classify_command("vortex-encoded-read-boundary"),
            CommandFamily::PreparedSourceBackedExecution
        );
        assert_eq!(
            classify_command("vortex-count-readiness-plan"),
            CommandFamily::VortexPlanning
        );
        assert_eq!(
            classify_command("vortex-execution-readiness"),
            CommandFamily::VortexRuntimePlanning
        );
        assert_eq!(
            classify_command("vortex-output-payload-plan"),
            CommandFamily::VortexOutputCommit
        );
        assert_eq!(
            classify_command("execution-certificate-plan"),
            CommandFamily::EvidenceCertificates
        );
        assert_eq!(
            classify_command("benchmark-plan"),
            CommandFamily::Benchmarks
        );
        assert_eq!(
            classify_command("release-plan"),
            CommandFamily::PackagingDeployment
        );
        assert_eq!(
            classify_command("security-governance-evidence-gate"),
            CommandFamily::OperationalHardening
        );
        assert_eq!(
            classify_command("object-store-request-plan"),
            CommandFamily::ObjectStorePlanning
        );
        assert_eq!(
            classify_command("feature-footprint"),
            CommandFamily::Diagnostics
        );
        assert_eq!(
            classify_command("api-compat-plan"),
            CommandFamily::RestApiPlanning
        );
        assert_eq!(
            classify_command("input-adapters"),
            CommandFamily::InputPlanning
        );
        assert_eq!(
            classify_command("engine-selection-plan"),
            CommandFamily::EngineRuntimePlanning
        );
        assert_eq!(
            classify_command("engine-capability-matrix"),
            CommandFamily::EngineRuntimePlanning
        );
        assert_eq!(
            classify_command("live-fixture-run"),
            CommandFamily::EngineRuntimePlanning
        );
        assert_eq!(
            classify_command("kernel-registry"),
            CommandFamily::OptimizerPlanning
        );
        assert_eq!(
            classify_command("udf-runtime-plan"),
            CommandFamily::ExtensionPlanning
        );
    }

    #[test]
    fn unknown_commands_are_explicitly_other() {
        assert_eq!(classify_command("not-a-command"), CommandFamily::Other);
    }
}
