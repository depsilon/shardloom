//! Operational hardening, security, and effect-policy CLI handlers.
//!
//! These handlers are report-only planning and governance surfaces. They do not
//! resolve credentials, load secrets, execute effects, write data, or provide
//! fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    AgentSafetyMode, CommandStatus, EffectBudgetReport, OutputFormat, RedactionPolicy,
    SecurityGovernanceEvidenceGateReport, SecurityPlan, ShardLoomError,
    plan_security_governance_evidence_gate,
};
use shardloom_exec::{
    AttemptId, ByteSize, CancellationReason, CancellationRequest, CancellationScope,
    CommitExecutionPromotionGateReport, FaultTolerancePromotionGateReport, MemoryBudget,
    MemoryOwner, MemoryPoolPlan, MemoryRuntimeHardeningGateReport, OomSafetyPlan,
    OperatorMemoryClass, OperatorMemorySpillDeclarationReport, RecoveryPlan, RetryPlan,
    ShardLoomCancellationExecutionGateReport, ShardLoomCleanupExecutionRequest,
    ShardLoomRetryExecutionGateReport, SpillLifecycleRequest, SpillPayloadFsRef, SpillPayloadId,
    SpillPayloadPath, SpillPayloadRef, SpillPayloadRoundTripReport, SpillPayloadRoundTripRequest,
    SpillPayloadWriteRequest, SpillPlan, SpillPolicy, SpillReservationIntegrationRequest,
    SpillWorkspaceId, SpillWorkspacePath, SyntheticSpillPayload, TaskAttemptRecord,
    plan_cancellation_execution_gate, plan_commit_execution_promotion_gate,
    plan_fault_tolerance_promotion_gate, plan_memory_runtime_hardening_gate,
    plan_operator_memory_spill_declarations, plan_retry_execution_gate, plan_spill_lifecycle,
    plan_spill_reservation_integration, roundtrip_spill_payload, spill_payload_fs_feature_enabled,
};

use crate::{
    cancellation_gate_plan_fields,
    cli_output::{emit, emit_error},
    commit_execution_promotion_gate_fields, fault_tolerance_promotion_gate_fields,
    memory_runtime_hardening_gate_fields, operator_memory_spill_declaration_fields,
    retry_gate_plan_fields,
};

pub(crate) fn handle_security_plan(format: OutputFormat) -> ExitCode {
    let plan = SecurityPlan::default_safe();
    emit_security_style_plan(
        "security-plan",
        "security plan skeleton",
        "security_plan",
        plan.to_human_text(),
        format,
    )
}

pub(crate) fn handle_security_governance_evidence_gate(format: OutputFormat) -> ExitCode {
    let report = plan_security_governance_evidence_gate();
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "security-governance-evidence-gate",
        format,
        status,
        "security governance evidence gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        security_governance_evidence_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_effect_budget_plan(format: OutputFormat) -> ExitCode {
    let report = EffectBudgetReport::planning_default();
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "effect-budget-plan",
        format,
        status,
        "effect budget plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        effect_budget_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_agent_safety_plan(format: OutputFormat) -> ExitCode {
    let mut plan = SecurityPlan::default_safe();
    plan.agent_mode = AgentSafetyMode::AgentDryRunOnly;
    emit_security_style_plan(
        "agent-safety-plan",
        "agent safety plan skeleton",
        "agent_safety_plan",
        plan.to_human_text(),
        format,
    )
}

pub(crate) fn handle_redaction_plan(format: OutputFormat) -> ExitCode {
    let redaction = RedactionPolicy::strict();
    emit_security_style_plan(
        "redaction-plan",
        "redaction plan skeleton",
        "redaction_plan",
        redaction.summary(),
        format,
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_spill_lifecycle(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_id_text) = args.next() else {
        eprintln!("usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>");
        return ExitCode::from(2);
    };
    let Some(workspace_path_text) = args.next() else {
        eprintln!("usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>");
        return ExitCode::from(2);
    };
    let Some(mode_text) = args.next() else {
        eprintln!("usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>");
        return ExitCode::from(2);
    };
    let workspace_id = match SpillWorkspaceId::new(workspace_id_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
        }
    };
    let workspace_path = match SpillWorkspacePath::new(workspace_path_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
        }
    };
    let request = match mode_text.as_str() {
        "report-only" => SpillLifecycleRequest::report_only(workspace_id, workspace_path),
        "local-workspace" => SpillLifecycleRequest::local_workspace(workspace_id, workspace_path),
        "cleanup-only" => SpillLifecycleRequest::cleanup_only(workspace_id, workspace_path),
        _ => {
            return emit_error(
                "spill-lifecycle",
                format,
                "spill lifecycle failed",
                &ShardLoomError::InvalidOperation(
                    "mode must be report-only, local-workspace, or cleanup-only".to_string(),
                ),
            );
        }
    };
    let report = match plan_spill_lifecycle(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
        }
    };
    emit(
        "spill-lifecycle",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "spill lifecycle report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "spill_lifecycle".to_string()),
            ("spill_lifecycle_mode".to_string(), mode_text),
            (
                "workspace_created".to_string(),
                report.workspace_created.as_bool().to_string(),
            ),
            (
                "marker_created".to_string(),
                report.marker_created.as_bool().to_string(),
            ),
            (
                "cleanup_performed".to_string(),
                report.cleanup_performed.as_bool().to_string(),
            ),
            ("spill_data_written".to_string(), "false".to_string()),
            ("spill_data_read".to_string(), "false".to_string()),
            (
                "reservation_integration_status".to_string(),
                "not_applicable".to_string(),
            ),
            ("reservation_granted".to_string(), "false".to_string()),
            ("estimated_bytes_known".to_string(), "false".to_string()),
            (
                "reservation_lifecycle_integration".to_string(),
                "true".to_string(),
            ),
            ("memory_integration".to_string(), "true".to_string()),
            (
                "vortex_memory_bridge_integration".to_string(),
                "true".to_string(),
            ),
            (
                "bounded_execution_integration".to_string(),
                "true".to_string(),
            ),
            ("object_store_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    exit_for_errors(report.has_errors())
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_spill_reservation_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(label) = args.next() else {
        eprintln!(
            "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
        );
        return ExitCode::from(2);
    };
    let Some(policy_text) = args.next() else {
        eprintln!(
            "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
        );
        return ExitCode::from(2);
    };
    let Some(estimated_text) = args.next() else {
        eprintln!(
            "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
        );
        return ExitCode::from(2);
    };
    let policy = match policy_text.as_str() {
        "never" => SpillPolicy::Never,
        "best-effort" => SpillPolicy::BestEffort,
        "required" => SpillPolicy::Required,
        _ => {
            return emit_error(
                "spill-reservation-plan",
                format,
                "spill reservation plan failed",
                &ShardLoomError::InvalidOperation(
                    "policy must be never, best-effort, or required".to_string(),
                ),
            );
        }
    };
    let mut request = match SpillReservationIntegrationRequest::new(label, policy) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-reservation-plan",
                format,
                "spill reservation plan failed",
                &error,
            );
        }
    };
    if estimated_text != "unknown" {
        let bytes: u64 = match estimated_text.parse() {
            Ok(v) => v,
            Err(_) => {
                return emit_error(
                    "spill-reservation-plan",
                    format,
                    "spill reservation plan failed",
                    &ShardLoomError::InvalidOperation(
                        "estimated_bytes must be unknown or unsigned integer".to_string(),
                    ),
                );
            }
        };
        request = request.with_estimated_bytes(ByteSize::from_bytes(bytes));
    }
    let report = match plan_spill_reservation_integration(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-reservation-plan",
                format,
                "spill reservation plan failed",
                &error,
            );
        }
    };
    emit(
        "spill-reservation-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "spill reservation integration report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "spill_reservation_plan".to_string()),
            (
                "reservation_integration_status".to_string(),
                report.status.as_str().to_string(),
            ),
            (
                "reservation_granted".to_string(),
                report.reservation_granted.to_string(),
            ),
            (
                "estimated_bytes_known".to_string(),
                report.estimated_bytes_known.to_string(),
            ),
            ("spill_data_written".to_string(), "false".to_string()),
            ("spill_data_read".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    exit_for_errors(report.has_errors())
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_spill_payload_roundtrip(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_text) = args.next() else {
        eprintln!(
            "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
        );
        return ExitCode::from(2);
    };
    let Some(payload_id_text) = args.next() else {
        eprintln!(
            "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
        );
        return ExitCode::from(2);
    };
    let Some(payload_text) = args.next() else {
        eprintln!(
            "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
        );
        return ExitCode::from(2);
    };
    let mut cleanup_after = false;
    if let Some(extra) = args.next() {
        if extra == "--cleanup" {
            cleanup_after = true;
        } else {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &ShardLoomError::InvalidOperation(
                    "unknown trailing argument; expected optional --cleanup".to_string(),
                ),
            );
        }
        if args.next().is_some() {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &ShardLoomError::InvalidOperation(
                    "too many arguments for spill-payload-roundtrip".to_string(),
                ),
            );
        }
    }
    let workspace_path = match SpillPayloadPath::new(workspace_path_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &error,
            );
        }
    };
    let payload_id = match SpillPayloadId::new(payload_id_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &error,
            );
        }
    };
    let payload = match SyntheticSpillPayload::from_text(payload_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &error,
            );
        }
    };
    let payload_ref = match SpillPayloadRef::new(payload_id, "shardloom_cli_workspace") {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &error,
            );
        }
    };
    let fs_ref = SpillPayloadFsRef::new(payload_ref, workspace_path);
    let write_request = SpillPayloadWriteRequest::new(fs_ref, payload);
    let request = SpillPayloadRoundTripRequest::new(write_request).cleanup_after(cleanup_after);
    let report = match roundtrip_spill_payload(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-payload-roundtrip",
                format,
                "spill payload roundtrip failed",
                &error,
            );
        }
    };
    emit_spill_payload_roundtrip(format, &report)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_cleanup_synthetic_payload(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_text) = args.next() else {
        eprintln!("usage: shardloom cleanup-synthetic-payload <workspace_path> <payload_id>");
        return ExitCode::from(2);
    };
    let Some(payload_id_text) = args.next() else {
        eprintln!("usage: shardloom cleanup-synthetic-payload <workspace_path> <payload_id>");
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        return emit_error(
            "cleanup-synthetic-payload",
            format,
            "synthetic spill payload cleanup failed",
            &ShardLoomError::InvalidOperation(
                "too many arguments for cleanup-synthetic-payload".to_string(),
            ),
        );
    }
    let workspace_path = match SpillPayloadPath::new(workspace_path_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cleanup-synthetic-payload",
                format,
                "synthetic spill payload cleanup failed",
                &error,
            );
        }
    };
    let payload_id = match SpillPayloadId::new(payload_id_text) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cleanup-synthetic-payload",
                format,
                "synthetic spill payload cleanup failed",
                &error,
            );
        }
    };
    let payload_ref = match SpillPayloadRef::new(payload_id.clone(), "shardloom_cli_workspace") {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cleanup-synthetic-payload",
                format,
                "synthetic spill payload cleanup failed",
                &error,
            );
        }
    };
    let fs_ref = SpillPayloadFsRef::new(payload_ref, workspace_path);
    let request = ShardLoomCleanupExecutionRequest::synthetic_payload(
        shardloom_exec::recovery::RecoveryArtifactRef::synthetic_spill_payload(&fs_ref),
        fs_ref,
    )
    .allow_synthetic_payload_cleanup(true);
    let report = match shardloom_exec::recovery::execute_cleanup_plan(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cleanup-synthetic-payload",
                format,
                "synthetic spill payload cleanup failed",
                &error,
            );
        }
    };
    emit(
        "cleanup-synthetic-payload",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "synthetic spill payload cleanup report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "cleanup_synthetic_payload".to_string()),
            (
                "cleanup_executed".to_string(),
                report.cleanup_executed().to_string(),
            ),
            (
                "cleanup_performed".to_string(),
                report.cleanup_executed().to_string(),
            ),
            (
                "retry_executed".to_string(),
                report.retry_executed().to_string(),
            ),
            (
                "cancellation_executed".to_string(),
                report.cancellation_executed().to_string(),
            ),
            (
                "external_effects_executed".to_string(),
                report.external_effects_executed().to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "output_dataset_write".to_string(),
                report.output_dataset_write().to_string(),
            ),
            (
                "execution".to_string(),
                "cleanup_or_not_performed".to_string(),
            ),
            (
                "artifact_kind".to_string(),
                "synthetic_spill_payload".to_string(),
            ),
            ("payload_id".to_string(), payload_id.as_str().to_string()),
        ],
    );
    exit_for_errors(report.has_errors())
}

pub(crate) fn handle_memory_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(memory_gb) = args.next() else {
        eprintln!("usage: shardloom memory-plan <memory_gb>");
        return ExitCode::from(2);
    };
    let memory_gb = match memory_gb.parse::<u64>() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "memory-plan",
                format,
                "invalid memory_gb",
                &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
            );
        }
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("memory-plan", format, "invalid memory budget", &error);
        }
    };
    let plan = OomSafetyPlan::new(MemoryPoolPlan::new(budget));
    emit(
        "memory-plan",
        format,
        CommandStatus::Success,
        "memory plan".to_string(),
        plan.to_human_text(),
        vec![],
        vec![("mode".to_string(), "plan_only".to_string())],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_operator_memory_spill_declarations(format: OutputFormat) -> ExitCode {
    let report = plan_operator_memory_spill_declarations();
    emit_operator_memory_spill_declarations(format, &report)
}

pub(crate) fn handle_memory_runtime_hardening_gate(format: OutputFormat) -> ExitCode {
    let report = plan_memory_runtime_hardening_gate();
    emit_memory_runtime_hardening_gate(format, &report)
}

pub(crate) fn handle_spill_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(operator_label) = args.next() else {
        eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
        return ExitCode::from(2);
    };
    let Some(memory_gb) = args.next() else {
        eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
        return ExitCode::from(2);
    };
    let memory_gb = match memory_gb.parse::<u64>() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "spill-plan",
                format,
                "invalid memory_gb",
                &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
            );
        }
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("spill-plan", format, "invalid memory budget", &error);
        }
    };
    let pool = MemoryPoolPlan::new(budget);
    let lower = operator_label.to_lowercase();
    let class = if lower.contains("sort") {
        OperatorMemoryClass::Sort
    } else if lower.contains("join") {
        OperatorMemoryClass::Join
    } else if lower.contains("agg") || lower.contains("aggregate") {
        OperatorMemoryClass::Aggregate
    } else {
        OperatorMemoryClass::Unknown
    };
    let owner = match MemoryOwner::new(class, operator_label) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("spill-plan", format, "invalid operator label", &error);
        }
    };
    let spill_plan = SpillPlan::spill_not_implemented(owner, SpillPolicy::BestEffort);
    let mut plan = OomSafetyPlan::new(pool);
    plan.add_spill_plan(spill_plan);
    let status = if plan.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "spill-plan",
        format,
        status,
        "spill plan".to_string(),
        plan.to_human_text(),
        vec![],
        vec![("mode".to_string(), "plan_only".to_string())],
    );
    exit_for_errors(plan.has_errors())
}

pub(crate) fn handle_recovery_plan(format: OutputFormat) -> ExitCode {
    let plan = RecoveryPlan::recovery_not_implemented(
        "recovery_execution",
        "Recovery planning skeleton exists, but actual recovery execution is not implemented yet.",
    );
    emit(
        "recovery-plan",
        format,
        CommandStatus::Unsupported,
        "recovery plan skeleton".to_string(),
        plan.to_human_text(),
        plan.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "recovery_plan".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    ExitCode::from(1)
}

pub(crate) fn handle_commit_execution_promotion_gate(format: OutputFormat) -> ExitCode {
    let report = plan_commit_execution_promotion_gate();
    emit_commit_execution_promotion_gate(format, &report)
}

pub(crate) fn handle_fault_tolerance_promotion_gate(format: OutputFormat) -> ExitCode {
    let report = plan_fault_tolerance_promotion_gate();
    emit_fault_tolerance_promotion_gate(format, &report)
}

pub(crate) fn handle_cancellation_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scope = match args.next().as_deref() {
        Some("query") => CancellationScope::Query,
        Some("task") => CancellationScope::Task,
        Some("scan") => CancellationScope::Scan,
        Some("output-write") => CancellationScope::OutputWrite,
        Some("external-effect") => CancellationScope::ExternalEffect,
        Some("spill-cleanup") => CancellationScope::SpillCleanup,
        Some("runtime" | _) | None => CancellationScope::Runtime,
    };
    let request = CancellationRequest::new(scope, CancellationReason::UserRequested);
    emit(
        "cancellation-plan",
        format,
        CommandStatus::Success,
        "cancellation plan skeleton".to_string(),
        request.summary(),
        request.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "cancellation_plan".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_retry_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(task_id) = args.next() else {
        eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
        return ExitCode::from(2);
    };
    let Some(attempt_id) = args.next() else {
        eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
        return ExitCode::from(2);
    };
    let task_id = match shardloom_exec::TaskId::new(task_id) {
        Ok(v) => v,
        Err(error) => return emit_error("retry-plan", format, "invalid task id", &error),
    };
    let attempt_id = match AttemptId::new(attempt_id) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("retry-plan", format, "invalid attempt id", &error);
        }
    };
    let attempt = TaskAttemptRecord::new(task_id, attempt_id);
    let plan =
        RetryPlan::from_attempt(shardloom_exec::RetryPolicy::default_read_retries(), attempt);
    emit(
        "retry-plan",
        format,
        CommandStatus::Success,
        "retry plan skeleton".to_string(),
        plan.summary(),
        plan.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "retry_plan".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_retry_gate_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(raw) = args.next() else {
        return emit_retry_gate_signal_error(format, "retry-gate-plan requires <signals>");
    };
    if raw.trim().is_empty() {
        return emit_retry_gate_signal_error(format, "retry-gate-plan requires <signals>");
    }
    if args.next().is_some() {
        return emit_retry_gate_signal_error(format, "too many arguments for retry-gate-plan");
    }
    let request = match crate::parse_retry_gate_signals(&raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "retry-gate-plan",
                format,
                "invalid retry gate signal list",
                &error,
            );
        }
    };
    let report = match plan_retry_execution_gate(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "retry-gate-plan",
                format,
                "retry gate planning failed",
                &error,
            );
        }
    };
    emit_retry_gate_plan(format, &report)
}

pub(crate) fn handle_cancellation_gate_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(raw) = args.next() else {
        return emit_cancellation_gate_signal_error(
            format,
            "cancellation-gate-plan requires <signals>",
        );
    };
    if raw.trim().is_empty() {
        return emit_cancellation_gate_signal_error(
            format,
            "cancellation-gate-plan requires <signals>",
        );
    }
    if args.next().is_some() {
        return emit_cancellation_gate_signal_error(
            format,
            "too many arguments for cancellation-gate-plan",
        );
    }
    let request = match crate::parse_cancellation_gate_signals(&raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cancellation-gate-plan",
                format,
                "invalid cancellation gate signal list",
                &error,
            );
        }
    };
    let report = match plan_cancellation_execution_gate(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "cancellation-gate-plan",
                format,
                "cancellation gate planning failed",
                &error,
            );
        }
    };
    emit_cancellation_gate_plan(format, &report)
}

fn emit_security_style_plan(
    command: &str,
    summary: &str,
    mode: &str,
    human_text: String,
    format: OutputFormat,
) -> ExitCode {
    emit(
        command,
        format,
        CommandStatus::Success,
        summary.to_string(),
        human_text,
        vec![],
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), mode.to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("external_effects".to_string(), "disabled".to_string()),
            ("credentials_resolved".to_string(), "false".to_string()),
            ("secrets_loaded".to_string(), "false".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

fn emit_spill_payload_roundtrip(
    format: OutputFormat,
    report: &SpillPayloadRoundTripReport,
) -> ExitCode {
    let bytes_read = report.read_report.as_ref().map_or(0, |v| v.bytes_read);
    let verification_passed = report
        .read_report
        .as_ref()
        .is_some_and(|v| v.verification_passed);
    emit(
        "spill-payload-roundtrip",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "spill payload roundtrip report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "spill_payload_roundtrip".to_string()),
            (
                "spill_payload_feature_enabled".to_string(),
                spill_payload_fs_feature_enabled().to_string(),
            ),
            (
                "payload_written".to_string(),
                report.payload_written().to_string(),
            ),
            (
                "payload_read".to_string(),
                report.payload_read().to_string(),
            ),
            (
                "cleanup_performed".to_string(),
                report.cleanup_performed().to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "output_dataset_write".to_string(),
                report.output_dataset_write().to_string(),
            ),
            (
                "execution".to_string(),
                "spill_payload_roundtrip_or_not_performed".to_string(),
            ),
            (
                "bytes_written".to_string(),
                report.write_report.bytes_written.to_string(),
            ),
            ("bytes_read".to_string(), bytes_read.to_string()),
            (
                "verification_passed".to_string(),
                verification_passed.to_string(),
            ),
        ],
    );
    exit_for_errors(report.has_errors())
}

fn emit_operator_memory_spill_declarations(
    format: OutputFormat,
    report: &OperatorMemorySpillDeclarationReport,
) -> ExitCode {
    emit(
        "operator-memory-spill-declarations",
        format,
        CommandStatus::Success,
        "operator memory/spill declaration gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        operator_memory_spill_declaration_fields(report),
    );
    ExitCode::SUCCESS
}

fn emit_memory_runtime_hardening_gate(
    format: OutputFormat,
    report: &MemoryRuntimeHardeningGateReport,
) -> ExitCode {
    emit(
        "cg14-memory-runtime-hardening-gate",
        format,
        CommandStatus::Success,
        "CG-14 memory runtime hardening gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        memory_runtime_hardening_gate_fields(report),
    );
    ExitCode::SUCCESS
}

fn emit_commit_execution_promotion_gate(
    format: OutputFormat,
    report: &CommitExecutionPromotionGateReport,
) -> ExitCode {
    emit(
        "commit-execution-promotion-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "commit execution promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        commit_execution_promotion_gate_fields(report),
    );
    exit_for_errors(report.has_errors())
}

fn emit_fault_tolerance_promotion_gate(
    format: OutputFormat,
    report: &FaultTolerancePromotionGateReport,
) -> ExitCode {
    emit(
        "fault-tolerance-promotion-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "fault tolerance promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        fault_tolerance_promotion_gate_fields(report),
    );
    exit_for_errors(report.has_errors())
}

fn emit_retry_gate_plan(
    format: OutputFormat,
    report: &ShardLoomRetryExecutionGateReport,
) -> ExitCode {
    emit(
        "retry-gate-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "retry execution gate plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        retry_gate_plan_fields(report),
    );
    exit_for_errors(report.has_errors())
}

fn emit_cancellation_gate_plan(
    format: OutputFormat,
    report: &ShardLoomCancellationExecutionGateReport,
) -> ExitCode {
    emit(
        "cancellation-gate-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "cancellation execution gate plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        cancellation_gate_plan_fields(report),
    );
    exit_for_errors(report.has_errors())
}

fn emit_retry_gate_signal_error(format: OutputFormat, message: &str) -> ExitCode {
    emit_error(
        "retry-gate-plan",
        format,
        "invalid retry gate signal list",
        &ShardLoomError::InvalidOperation(message.to_string()),
    )
}

fn emit_cancellation_gate_signal_error(format: OutputFormat, message: &str) -> ExitCode {
    emit_error(
        "cancellation-gate-plan",
        format,
        "invalid cancellation gate signal list",
        &ShardLoomError::InvalidOperation(message.to_string()),
    )
}

fn exit_for_errors(has_errors: bool) -> ExitCode {
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn effect_budget_fields(report: &EffectBudgetReport) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), "effect_budget_plan".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("report_id".to_string(), report.report_id.to_string()),
        ("budget_mode".to_string(), report.budget_mode.to_string()),
        ("entry_count".to_string(), report.entries.len().to_string()),
        (
            "denied_scope_count".to_string(),
            report.denied_scope_count().to_string(),
        ),
        (
            "approved_scope_count".to_string(),
            report.approved_scope_count().to_string(),
        ),
        (
            "approval_required_scope_count".to_string(),
            report.approval_required_scope_count().to_string(),
        ),
        (
            "credential_required_scope_count".to_string(),
            report.credential_required_scope_count().to_string(),
        ),
        (
            "materialization_boundary_required_scope_count".to_string(),
            report
                .materialization_boundary_required_scope_count()
                .to_string(),
        ),
        ("scope_order".to_string(), report.scope_order().join(",")),
        (
            "external_effects_allowed".to_string(),
            report.external_effects_allowed.to_string(),
        ),
        (
            "destructive_effects_allowed".to_string(),
            report.destructive_effects_allowed.to_string(),
        ),
        (
            "network_egress_allowed".to_string(),
            report.network_egress_allowed.to_string(),
        ),
        (
            "credentials_resolved".to_string(),
            report.credentials_resolved.to_string(),
        ),
        (
            "secrets_loaded".to_string(),
            report.secrets_loaded.to_string(),
        ),
        (
            "redaction_policy_required".to_string(),
            report.redaction_policy_required.to_string(),
        ),
        (
            "audit_required".to_string(),
            report.audit_required.to_string(),
        ),
        (
            "runtime_execution".to_string(),
            report.runtime_execution_performed.to_string(),
        ),
        (
            "filesystem_probe".to_string(),
            report.filesystem_probe.to_string(),
        ),
        (
            "network_probe".to_string(),
            report.network_probe.to_string(),
        ),
        (
            "catalog_probe".to_string(),
            report.catalog_probe.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.side_effect_free().to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

pub(crate) fn security_governance_evidence_gate_fields(
    report: &SecurityGovernanceEvidenceGateReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_security_governance_evidence_gate_summary_fields(&mut fields, report);
    append_security_governance_evidence_gate_entry_fields(&mut fields, report);
    fields
}

fn append_security_governance_evidence_gate_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &SecurityGovernanceEvidenceGateReport,
) {
    push_field(fields, "mode", "security_governance_evidence_gate");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_count_field(fields, "evidence_area_count", report.evidence_area_count());
    push_count_field(
        fields,
        "report_only_area_count",
        report.report_only_area_count(),
    );
    push_count_field(
        fields,
        "effectful_claim_allowed_count",
        report.effectful_claim_allowed_count(),
    );
    push_field(fields, "area_order", &report.area_order().join(","));
    push_bool_field(
        fields,
        "all_evidence_surfaces_present",
        report.all_evidence_surfaces_present(),
    );
    push_bool_field(
        fields,
        "effectful_features_default_denied",
        report.effectful_features_default_denied,
    );
    push_bool_field(
        fields,
        "dry_run_required_without_policy",
        report.dry_run_required_without_policy,
    );
    push_bool_field(
        fields,
        "credential_references_only",
        report.credential_references_only,
    );
    push_bool_field(fields, "credentials_resolved", report.credentials_resolved);
    push_bool_field(fields, "secrets_loaded", report.secrets_loaded);
    push_bool_field(fields, "redaction_required", report.redaction_required);
    push_bool_field(fields, "audit_required", report.audit_required);
    push_bool_field(
        fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "external_effect_claims_allowed",
        report.external_effect_claims_allowed,
    );
    push_bool_field(
        fields,
        "destructive_operations_allowed",
        report.destructive_operations_allowed,
    );
    push_bool_field(fields, "data_egress_allowed", report.data_egress_allowed);
    push_bool_field(
        fields,
        "object_store_claims_blocked",
        report.object_store_claims_blocked,
    );
    push_bool_field(
        fields,
        "api_server_claims_blocked",
        report.api_server_claims_blocked,
    );
    push_bool_field(
        fields,
        "llm_media_udf_claims_blocked",
        report.llm_media_udf_claims_blocked,
    );
    push_bool_field(
        fields,
        "agent_execute_write_cancel_allowed",
        report.agent_execute_write_cancel_allowed,
    );
    push_bool_field(
        fields,
        "runtime_execution",
        report.runtime_execution_performed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(
        fields,
        "claims_blocked_by_default",
        report.claims_blocked_by_default(),
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn append_security_governance_evidence_gate_entry_fields(
    fields: &mut Vec<(String, String)>,
    report: &SecurityGovernanceEvidenceGateReport,
) {
    for (idx, entry) in report.entries.iter().enumerate() {
        let prefix = format!("security_evidence_area_{idx}");
        push_field(fields, &format!("{prefix}_name"), entry.area.as_str());
        push_field(fields, &format!("{prefix}_status"), entry.status.as_str());
        push_field(
            fields,
            &format!("{prefix}_default_policy"),
            entry.default_policy,
        );
        push_field(
            fields,
            &format!("{prefix}_required_for_claims"),
            entry.required_for_claims,
        );
        push_field(
            fields,
            &format!("{prefix}_evidence_field"),
            entry.evidence_field,
        );
        push_bool_field(
            fields,
            &format!("{prefix}_effectful_claim_allowed"),
            entry.effectful_claim_allowed,
        );
    }
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}
