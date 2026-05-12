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
    EvidenceCertificates,
    Benchmarks,
    PackagingDeployment,
    Foundry,
    OperationalHardening,
    Diagnostics,
    RestApiPlanning,
    WorkflowPlanning,
    EngineRuntimePlanning,
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
            Self::EvidenceCertificates => "evidence_certificates",
            Self::Benchmarks => "benchmarks",
            Self::PackagingDeployment => "packaging_deployment",
            Self::Foundry => "foundry",
            Self::OperationalHardening => "operational_hardening",
            Self::Diagnostics => "diagnostics",
            Self::RestApiPlanning => "rest_api_planning",
            Self::WorkflowPlanning => "workflow_planning",
            Self::EngineRuntimePlanning => "engine_runtime_planning",
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
    } else if is_prepared_source_backed_command(command) || command.starts_with("vortex-") {
        CommandFamily::PreparedSourceBackedExecution
    } else if is_evidence_certificate_command(command) {
        CommandFamily::EvidenceCertificates
    } else if is_benchmark_command(command) {
        CommandFamily::Benchmarks
    } else if is_packaging_deployment_command(command) {
        CommandFamily::PackagingDeployment
    } else if is_foundry_command(command) {
        CommandFamily::Foundry
    } else if is_operational_hardening_command(command)
        || command.starts_with("object-store-")
        || command.starts_with("cg10-")
    {
        CommandFamily::OperationalHardening
    } else if is_diagnostics_command(command) {
        CommandFamily::Diagnostics
    } else if command == "api-compat-plan" {
        CommandFamily::RestApiPlanning
    } else if is_workflow_planning_command(command) || command.starts_with("cg9-") {
        CommandFamily::WorkflowPlanning
    } else if is_engine_runtime_planning_command(command) {
        CommandFamily::EngineRuntimePlanning
    } else if is_extension_planning_command(command) {
        CommandFamily::ExtensionPlanning
    } else {
        CommandFamily::Other
    }
}

fn is_status_capabilities_command(command: &str) -> bool {
    matches!(
        command,
        "status" | "capabilities" | "kernel-registry" | "feature-footprint"
    )
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
        "vortex-encoded-path-selection-plan"
            | "vortex-generalized-encoded-primitive-gate"
            | "vortex-encoded-read-api"
            | "vortex-encoded-read-boundary"
            | "vortex-encoded-read-metadata-probe"
            | "vortex-encoded-read-readiness"
            | "vortex-encoded-read-probe"
            | "vortex-encoded-read-execute"
            | "vortex-encoded-read-spike"
            | "vortex-read-plan"
            | "vortex-task-graph"
            | "vortex-execution-readiness"
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
            | "cleanup-synthetic-payload"
            | "fault-tolerance-promotion-gate"
            | "commit-execution-promotion-gate"
            | "retry-gate-plan"
            | "cancellation-gate-plan"
    )
}

fn is_diagnostics_command(command: &str) -> bool {
    matches!(
        command,
        "doctor"
            | "explain"
            | "estimate"
            | "profile-plan"
            | "runtime-report"
            | "observability-plan"
            | "observability-schema-coverage"
    )
}

fn is_workflow_planning_command(command: &str) -> bool {
    matches!(
        command,
        "schema-plan"
            | "input-adapters"
            | "input-plan"
            | "translation-plan"
            | "plan-ir"
            | "plan-import"
            | "plan-export"
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
        "streaming-plan"
            | "streaming-batch-plan"
            | "backpressure-plan"
            | "runtime-plan"
            | "task-plan"
            | "sizing-plan"
            | "sizing-feedback-plan"
            | "dynamic-work-shaping-plan"
            | "cg8-runtime-promotion-gate"
            | "memory-plan"
            | "vortex-adaptive-sizing"
            | "vortex-memory-plan"
            | "vortex-schedule-plan"
    )
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
            classify_command("api-compat-plan"),
            CommandFamily::RestApiPlanning
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
