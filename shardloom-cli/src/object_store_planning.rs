//! Object-store planning CLI handlers.
//!
//! These handlers emit report-only object-store planning surfaces. They do not
//! read remote objects, open credentials, execute distributed tasks, write
//! outputs, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    ByteRange, ColumnRef, CommandStatus, DatasetFormat, DatasetManifest, DatasetRef, DatasetUri,
    EncodedSegment, EncodingKind, FileDescriptor, FileRole, LayoutKind, LogicalDType, ManifestId,
    ManifestSegment, Nullability, OutputFormat, SegmentId, SegmentLayout, SegmentStats,
    ShardLoomError, SnapshotId, SnapshotRef,
};
use shardloom_plan::{
    ObjectStoreByteRangeProviderGateReport, ObjectStoreCheckpointRetryInput,
    ObjectStoreCheckpointRetryReport, ObjectStoreCommitProtocolInput,
    ObjectStoreCommitProtocolReport, ObjectStoreDistributedSchedulingPolicy,
    ObjectStoreDistributedSchedulingReport, ObjectStoreRangePlanningPolicy,
    ObjectStoreRangePlanningReport, ObjectStoreRequestCoalescingReport,
    ObjectStoreRequestPlannerReport, ObjectStoreRuntimeBlockerMatrixRow,
    ObjectStoreRuntimePromotionGateReport, plan_object_store_checkpoint_retry,
    plan_object_store_commit_protocol, plan_object_store_distributed_scheduling,
    plan_object_store_ranges, plan_object_store_request_coalescing,
    plan_object_store_request_planner, plan_object_store_runtime_promotion_gate,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

pub(crate) fn handle_object_store_request_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "ready".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-request-plan",
            format,
            "object-store request planning failed",
            &cli_unknown_arg_error("object-store-request-plan", &extra),
        );
    }
    emit_object_store_request_plan(format, &scenario)
}

pub(crate) fn handle_cg10_object_store_runtime_gate(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "cg10-object-store-runtime-gate",
            format,
            "CG-10 object-store runtime gate failed",
            &cli_unknown_arg_error("cg10-object-store-runtime-gate", &extra),
        );
    }
    let report = plan_object_store_runtime_promotion_gate();
    emit(
        "cg10-object-store-runtime-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "CG-10 object-store runtime promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_runtime_promotion_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_object_store_range_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-range-plan",
            format,
            "object-store range planning failed",
            &cli_unknown_arg_error("object-store-range-plan", &extra),
        );
    }
    emit_object_store_range_plan(format, &scenario)
}

pub(crate) fn handle_object_store_coalesce_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-coalesce-plan",
            format,
            "object-store request coalescing failed",
            &cli_unknown_arg_error("object-store-coalesce-plan", &extra),
        );
    }
    emit_object_store_coalesce_plan(format, &scenario)
}

pub(crate) fn handle_object_store_schedule_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-schedule-plan",
            format,
            "object-store scheduling planning failed",
            &cli_unknown_arg_error("object-store-schedule-plan", &extra),
        );
    }
    emit_object_store_schedule_plan(format, &scenario)
}

pub(crate) fn handle_object_store_checkpoint_retry_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "ready".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-checkpoint-retry-plan",
            format,
            "object-store checkpoint/retry planning failed",
            &cli_unknown_arg_error("object-store-checkpoint-retry-plan", &extra),
        );
    }
    emit_object_store_checkpoint_retry_plan(format, &scenario)
}

pub(crate) fn handle_object_store_commit_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "ready".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "object-store-commit-plan",
            format,
            "object-store commit planning failed",
            &cli_unknown_arg_error("object-store-commit-plan", &extra),
        );
    }
    emit_object_store_commit_plan(format, &scenario)
}

fn emit_object_store_request_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let report = match object_store_request_planner_fixture(scenario) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "object-store-request-plan",
                format,
                "object-store request planning failed",
                &error,
            );
        }
    };
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-request-plan",
        format,
        status,
        "object-store request planner report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_request_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_request_output_fields(
    report: &ObjectStoreRequestPlannerReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "object_store_request_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(
        &mut fields,
        "object_store_request_status",
        report.status.as_str(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &ObjectStoreRequestPlannerReport::surface_order().join(","),
    );
    push_count_field(
        &mut fields,
        "planned_surface_count",
        report.planned_surface_count,
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count,
    );
    push_field(
        &mut fields,
        "range_status",
        report.range_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "coalescing_status",
        report.coalescing_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "scheduling_status",
        report.scheduling_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "checkpoint_retry_status",
        report.checkpoint_retry_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "commit_status",
        report.commit_report.status.as_str(),
    );
    append_object_store_request_count_fields(&mut fields, report);
    append_object_store_request_requirement_fields(&mut fields, report);
    append_byte_range_provider_gate_fields(&mut fields, &report.byte_range_provider_gate);
    append_object_store_request_side_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_object_store_request_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRequestPlannerReport,
) {
    push_count_field(
        fields,
        "planned_request_count",
        report.planned_request_count,
    );
    push_count_field(
        fields,
        "coalesced_request_count",
        report.coalesced_request_count,
    );
    push_count_field(fields, "planned_task_count", report.planned_task_count);
    push_count_field(fields, "retryable_task_count", report.retryable_task_count);
    push_count_field(
        fields,
        "planned_checkpoint_record_count",
        report.planned_checkpoint_record_count,
    );
    push_count_field(
        fields,
        "planned_attempt_record_count",
        report.planned_attempt_record_count,
    );
    push_u64_field(
        fields,
        "estimated_request_bytes",
        report.estimated_request_bytes,
    );
}

fn append_object_store_request_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRequestPlannerReport,
) {
    push_bool_field(fields, "requires_byte_ranges", report.requires_byte_ranges);
    push_bool_field(
        fields,
        "requires_request_budget_review",
        report.requires_request_budget_review,
    );
    push_bool_field(
        fields,
        "requires_checkpoint_plan",
        report.requires_checkpoint_plan,
    );
    push_bool_field(
        fields,
        "requires_retry_policy",
        report.requires_retry_policy,
    );
    push_bool_field(
        fields,
        "requires_idempotency_keys",
        report.requires_idempotency_keys,
    );
    push_bool_field(
        fields,
        "requires_attempt_records",
        report.requires_attempt_records,
    );
    push_bool_field(
        fields,
        "requires_cleanup_policy",
        report.requires_cleanup_policy,
    );
    push_bool_field(
        fields,
        "requires_atomic_commit_evidence",
        report.requires_atomic_commit_evidence,
    );
    push_bool_field(
        fields,
        "full_file_read_allowed",
        report.full_file_read_allowed,
    );
}

fn append_object_store_request_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRequestPlannerReport,
) {
    push_bool_field(fields, "coordinator_started", report.coordinator_started);
    push_bool_field(fields, "worker_started", report.worker_started);
    push_bool_field(
        fields,
        "task_execution_allowed",
        report.task_execution_allowed,
    );
    push_bool_field(
        fields,
        "retry_execution_allowed",
        report.retry_execution_allowed,
    );
    push_bool_field(
        fields,
        "checkpoint_write_allowed",
        report.checkpoint_write_allowed,
    );
    push_bool_field(
        fields,
        "cleanup_execution_allowed",
        report.cleanup_execution_allowed,
    );
    push_bool_field(
        fields,
        "commit_execution_allowed",
        report.commit_execution_allowed,
    );
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
}

fn object_store_runtime_promotion_gate_fields(
    report: &ObjectStoreRuntimePromotionGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "object_store_runtime_promotion_gate");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_count_field(&mut fields, "surface_count", report.surface_count());
    push_count_field(
        &mut fields,
        "existing_evidence_surface_count",
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_field(
        &mut fields,
        "existing_report_refs",
        &report.existing_report_refs.join(","),
    );
    append_object_store_runtime_existing_fields(&mut fields, report);
    append_byte_range_provider_gate_fields(&mut fields, &report.byte_range_provider_gate);
    append_object_store_runtime_blocker_matrix_fields(&mut fields, report);
    append_object_store_runtime_allowed_fields(&mut fields, report);
    append_object_store_runtime_required_fields(&mut fields, report);
    append_object_store_runtime_status_fields(&mut fields, report);
    fields
}

fn append_object_store_runtime_blocker_matrix_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    append_object_store_runtime_blocker_matrix_summary_fields(fields, report);
    for row in &report.runtime_blocker_matrix {
        append_object_store_runtime_blocker_matrix_row_fields(fields, row);
    }
}

fn append_object_store_runtime_blocker_matrix_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    push_field(
        fields,
        "runtime_blocker_matrix_schema_version",
        "shardloom.object_store_runtime_blocker_matrix.v1",
    );
    push_field(
        fields,
        "runtime_blocker_matrix_report_id",
        "gar0008b.object_store_runtime_blocker_matrix",
    );
    push_field(
        fields,
        "runtime_blocker_matrix_status",
        "blocked_until_certified",
    );
    push_count_field(
        fields,
        "runtime_blocker_matrix_row_count",
        report.runtime_blocker_matrix.len(),
    );
    push_field(
        fields,
        "runtime_blocker_matrix_row_order",
        &report.runtime_blocker_matrix_row_order().join(","),
    );
    push_bool_field(
        fields,
        "runtime_blocker_matrix_all_allowed_false",
        report.runtime_blocker_matrix_all_allowed_false(),
    );
    push_bool_field(
        fields,
        "runtime_blocker_matrix_all_no_io",
        report.runtime_blocker_matrix_all_no_io(),
    );
    push_bool_field(
        fields,
        "runtime_blocker_matrix_all_no_fallback",
        report.runtime_blocker_matrix_all_no_fallback(),
    );
    push_bool_field(
        fields,
        "runtime_blocker_matrix_all_no_external_engine",
        report.runtime_blocker_matrix_all_no_external_engine(),
    );
}

fn append_object_store_runtime_blocker_matrix_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &ObjectStoreRuntimeBlockerMatrixRow,
) {
    let prefix = format!("runtime_blocker_matrix_row_{}", row.action);
    push_field(fields, &format!("{prefix}_status"), row.status);
    push_field(
        fields,
        &format!("{prefix}_diagnostic_code"),
        row.diagnostic_code.as_str(),
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_required_evidence"),
        row.required_evidence,
    );
    append_object_store_runtime_blocker_matrix_row_effect_fields(fields, &prefix, row);
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
}

fn append_object_store_runtime_blocker_matrix_row_effect_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &ObjectStoreRuntimeBlockerMatrixRow,
) {
    for (key, value) in [
        ("allowed", row.allowed),
        ("coordinator_started", row.coordinator_started),
        ("worker_started", row.worker_started),
        ("task_executed", row.task_executed),
        ("checkpoint_written", row.checkpoint_written),
        ("retry_attempted", row.retry_attempted),
        ("cleanup_executed", row.cleanup_executed),
        ("commit_record_written", row.commit_record_written),
        ("data_read", row.data_read),
        ("object_store_io", row.object_store_io),
        ("write_io", row.write_io),
        ("fallback_attempted", row.fallback_attempted),
        ("fallback_execution_allowed", row.fallback_execution_allowed),
        ("external_engine_invoked", row.external_engine_invoked),
        ("side_effect_free", row.side_effect_free()),
    ] {
        push_bool_field(fields, &format!("{prefix}_{key}"), value);
    }
}

fn append_byte_range_provider_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreByteRangeProviderGateReport,
) {
    append_byte_range_provider_gate_identity_fields(fields, report);
    append_byte_range_provider_gate_requirement_fields(fields, report);
    append_byte_range_provider_gate_disabled_effect_fields(fields, report);
    append_byte_range_provider_gate_status_fields(fields, report);
}

fn append_byte_range_provider_gate_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreByteRangeProviderGateReport,
) {
    push_field(
        fields,
        "byte_range_provider_gate_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "byte_range_provider_gate_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "byte_range_provider_gate_status",
        report.status.as_str(),
    );
    push_field(fields, "byte_range_provider_gate_scope", report.scope);
    push_field(
        fields,
        "byte_range_provider_gate_provider_family",
        report.provider_family,
    );
    push_field(
        fields,
        "byte_range_provider_gate_blocker_id",
        report.blocker_id,
    );
    push_field(
        fields,
        "byte_range_provider_gate_required_evidence",
        report.required_evidence,
    );
}

fn append_byte_range_provider_gate_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreByteRangeProviderGateReport,
) {
    push_bool_field(
        fields,
        "byte_range_provider_gate_range_planning_evidence_present",
        report.range_planning_evidence_present,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_request_budget_policy_required",
        report.request_budget_policy_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_provider_capability_policy_required",
        report.provider_capability_policy_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_credential_policy_required",
        report.credential_policy_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_retry_policy_required",
        report.retry_policy_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_idempotency_key_required",
        report.idempotency_key_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
}

fn append_byte_range_provider_gate_disabled_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreByteRangeProviderGateReport,
) {
    push_bool_field(
        fields,
        "byte_range_provider_gate_range_read_execution_allowed",
        report.range_read_execution_allowed,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_full_file_read_allowed",
        report.full_file_read_allowed,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_credential_resolution_allowed",
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_credentials_resolved",
        report.credentials_resolved,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_retry_execution_allowed",
        report.retry_execution_allowed,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_provider_probe",
        report.provider_probe,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_network_probe",
        report.network_probe,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_object_store_io",
        report.object_store_io,
    );
    push_bool_field(fields, "byte_range_provider_gate_write_io", report.write_io);
    push_bool_field(
        fields,
        "byte_range_provider_gate_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_external_engine_invoked",
        report.external_engine_invoked,
    );
}

fn append_byte_range_provider_gate_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreByteRangeProviderGateReport,
) {
    push_field(
        fields,
        "byte_range_provider_gate_claim_gate_status",
        report.claim_gate_status,
    );
    push_bool_field(
        fields,
        "byte_range_provider_gate_side_effect_free",
        report.side_effect_free(),
    );
}

fn append_object_store_runtime_existing_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    push_bool_field(
        fields,
        "existing_request_planner_evidence_present",
        report.existing_request_planner_evidence_present,
    );
    push_bool_field(
        fields,
        "existing_range_planning_evidence_present",
        report.existing_range_planning_evidence_present,
    );
    push_bool_field(
        fields,
        "existing_coalescing_evidence_present",
        report.existing_coalescing_evidence_present,
    );
    push_bool_field(
        fields,
        "existing_distributed_scheduling_evidence_present",
        report.existing_distributed_scheduling_evidence_present,
    );
    push_bool_field(
        fields,
        "existing_checkpoint_retry_evidence_present",
        report.existing_checkpoint_retry_evidence_present,
    );
    push_bool_field(
        fields,
        "existing_commit_protocol_evidence_present",
        report.existing_commit_protocol_evidence_present,
    );
}

fn append_object_store_runtime_allowed_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    push_bool_field(
        fields,
        "range_read_execution_allowed",
        report.range_read_execution_allowed,
    );
    push_bool_field(
        fields,
        "full_file_read_allowed",
        report.full_file_read_allowed,
    );
    push_bool_field(
        fields,
        "request_coalescing_runtime_allowed",
        report.request_coalescing_runtime_allowed,
    );
    push_bool_field(
        fields,
        "coordinator_start_allowed",
        report.coordinator_start_allowed,
    );
    push_bool_field(fields, "worker_start_allowed", report.worker_start_allowed);
    push_bool_field(
        fields,
        "task_execution_allowed",
        report.task_execution_allowed,
    );
    push_bool_field(
        fields,
        "retry_execution_allowed",
        report.retry_execution_allowed,
    );
    push_bool_field(
        fields,
        "checkpoint_write_allowed",
        report.checkpoint_write_allowed,
    );
    push_bool_field(
        fields,
        "cleanup_execution_allowed",
        report.cleanup_execution_allowed,
    );
    push_bool_field(
        fields,
        "commit_execution_allowed",
        report.commit_execution_allowed,
    );
    push_bool_field(
        fields,
        "credential_resolution_allowed",
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        "object_store_io_allowed",
        report.object_store_io_allowed,
    );
    push_bool_field(fields, "data_read_allowed", report.data_read_allowed);
    push_bool_field(fields, "write_io_allowed", report.write_io_allowed);
    push_bool_field(
        fields,
        "object_store_runtime_claim_allowed",
        report.object_store_runtime_claim_allowed,
    );
    push_bool_field(
        fields,
        "distributed_runtime_claim_allowed",
        report.distributed_runtime_claim_allowed,
    );
}

fn append_object_store_runtime_required_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    push_bool_field(
        fields,
        "range_planning_evidence_required",
        report.range_planning_evidence_required,
    );
    push_bool_field(
        fields,
        "request_budget_policy_required",
        report.request_budget_policy_required,
    );
    push_bool_field(
        fields,
        "provider_capability_policy_required",
        report.provider_capability_policy_required,
    );
    push_bool_field(
        fields,
        "credential_effect_policy_required",
        report.credential_effect_policy_required,
    );
    push_bool_field(
        fields,
        "scheduler_policy_required",
        report.scheduler_policy_required,
    );
    push_bool_field(
        fields,
        "worker_identity_required",
        report.worker_identity_required,
    );
    push_bool_field(
        fields,
        "checkpoint_plan_required",
        report.checkpoint_plan_required,
    );
    push_bool_field(
        fields,
        "retry_policy_required",
        report.retry_policy_required,
    );
    push_bool_field(
        fields,
        "idempotency_keys_required",
        report.idempotency_keys_required,
    );
    push_bool_field(
        fields,
        "attempt_records_required",
        report.attempt_records_required,
    );
    push_bool_field(
        fields,
        "cleanup_policy_required",
        report.cleanup_policy_required,
    );
    push_bool_field(
        fields,
        "atomic_commit_evidence_required",
        report.atomic_commit_evidence_required,
    );
    push_bool_field(
        fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
}

fn append_object_store_runtime_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRuntimePromotionGateReport,
) {
    push_bool_field(
        fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(fields, "claim_blocked", report.claim_blocked());
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
}

fn emit_object_store_range_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match object_store_range_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "object-store-range-plan",
                format,
                "object-store range planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_ranges(manifest, ObjectStoreRangePlanningPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-range-plan",
        format,
        status,
        "object-store range planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_range_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_range_output_fields(
    report: &ObjectStoreRangePlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_range_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_range_status",
        report.status.as_str(),
    );
    append_object_store_range_count_fields(&mut fields, report);
    append_object_store_range_requirement_fields(&mut fields, report);
    append_object_store_range_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_object_store_range_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(
        fields,
        "object_store_file_count",
        report.object_store_file_count,
    );
    push_count_field(
        fields,
        "non_object_store_file_count",
        report.non_object_store_file_count,
    );
    push_count_field(fields, "ranged_segment_count", report.ranged_segment_count);
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(fields, "invalid_range_count", report.invalid_range_count);
    push_count_field(
        fields,
        "oversized_range_count",
        report.oversized_range_count,
    );
    push_count_field(
        fields,
        "planned_request_count",
        report.planned_request_count,
    );
    push_count_field(fields, "planned_range_count", report.planned_range_count);
    push_count_field(
        fields,
        "coalesced_range_count",
        report.coalesced_range_count,
    );
    push_u64_field(
        fields,
        "estimated_request_bytes",
        report.estimated_request_bytes,
    );
}

fn append_object_store_range_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_bool_field(fields, "requires_byte_ranges", report.requires_byte_ranges);
    push_bool_field(
        fields,
        "requires_request_budget_review",
        report.requires_request_budget_review,
    );
    push_bool_field(
        fields,
        "full_file_read_required",
        report.full_file_read_required,
    );
    push_bool_field(
        fields,
        "full_file_read_allowed",
        report.full_file_read_allowed,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_object_store_range_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
}

fn emit_object_store_coalesce_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest =
        match object_store_range_fixture_for_command("object-store-coalesce-plan", scenario) {
            Ok(manifest) => manifest,
            Err(error) => {
                return emit_error(
                    "object-store-coalesce-plan",
                    format,
                    "object-store request coalescing failed",
                    &error,
                );
            }
        };
    let report =
        plan_object_store_request_coalescing(manifest, ObjectStoreRangePlanningPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-coalesce-plan",
        format,
        status,
        "object-store request coalescing report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_coalesce_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_coalesce_output_fields(
    report: &ObjectStoreRequestCoalescingReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_coalesce_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_coalescing_status",
        report.status.as_str(),
    );
    push_count_field(
        &mut fields,
        "input_request_count",
        report.input_request_count,
    );
    push_count_field(
        &mut fields,
        "output_request_count",
        report.output_request_count,
    );
    push_count_field(
        &mut fields,
        "request_reduction_count",
        report.request_reduction_count,
    );
    push_count_field(&mut fields, "input_range_count", report.input_range_count);
    push_count_field(
        &mut fields,
        "coalesced_range_count",
        report.coalesced_range_count,
    );
    push_count_field(&mut fields, "decision_count", report.decisions.len());
    push_u64_field(
        &mut fields,
        "estimated_request_bytes_before",
        report.estimated_request_bytes_before,
    );
    push_u64_field(
        &mut fields,
        "estimated_request_bytes_after",
        report.estimated_request_bytes_after,
    );
    push_bool_field(&mut fields, "coalescing_applied", report.coalescing_applied);
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn emit_object_store_schedule_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (manifest, range_policy, scheduling_policy) = match object_store_schedule_fixture(scenario)
    {
        Ok(fixture) => fixture,
        Err(error) => {
            return emit_error(
                "object-store-schedule-plan",
                format,
                "object-store scheduling planning failed",
                &error,
            );
        }
    };
    let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
    let report = plan_object_store_distributed_scheduling(coalescing_report, scheduling_policy);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-schedule-plan",
        format,
        status,
        "object-store distributed scheduling report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_schedule_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_schedule_output_fields(
    report: &ObjectStoreDistributedSchedulingReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_schedule_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_schedule_status",
        report.status.as_str(),
    );
    push_count_field(
        &mut fields,
        "max_requests_per_task",
        report.policy.max_requests_per_task,
    );
    push_count_field(&mut fields, "max_task_count", report.policy.max_task_count);
    push_count_field(
        &mut fields,
        "input_request_count",
        report.input_request_count,
    );
    push_count_field(&mut fields, "planned_task_count", report.planned_task_count);
    push_u64_field(
        &mut fields,
        "estimated_request_bytes",
        report.estimated_request_bytes,
    );
    push_bool_field(
        &mut fields,
        "requires_checkpoint_plan",
        report.requires_checkpoint_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_retry_policy",
        report.requires_retry_policy,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_keys",
        report.requires_idempotency_keys,
    );
    push_bool_field(
        &mut fields,
        "scheduler_execution_allowed",
        report.scheduler_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "coordinator_started",
        report.coordinator_started,
    );
    push_bool_field(&mut fields, "worker_started", report.worker_started);
    push_bool_field(
        &mut fields,
        "task_execution_allowed",
        report.task_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_schedule_fixture(
    scenario: &str,
) -> Result<
    (
        DatasetManifest,
        ObjectStoreRangePlanningPolicy,
        ObjectStoreDistributedSchedulingPolicy,
    ),
    ShardLoomError,
> {
    let default_range_policy = ObjectStoreRangePlanningPolicy::default();
    let default_scheduling_policy = ObjectStoreDistributedSchedulingPolicy::default();
    let spaced_range_policy = ObjectStoreRangePlanningPolicy {
        max_coalesce_gap_bytes: 0,
        ..ObjectStoreRangePlanningPolicy::default()
    };
    let spaced_manifest = || {
        object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
        )
    };

    match scenario {
        "s3-ranges" => Ok((
            object_store_range_fixture("s3-ranges")?,
            default_range_policy,
            default_scheduling_policy,
        )),
        "multi-task" => Ok((
            spaced_manifest()?,
            spaced_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 4,
            },
        )),
        "missing-ranges" => Ok((
            object_store_range_fixture("missing-ranges")?,
            default_range_policy,
            default_scheduling_policy,
        )),
        "task-budget" => Ok((
            spaced_manifest()?,
            spaced_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 2,
            },
        )),
        "invalid-policy" => Ok((
            object_store_range_fixture("s3-ranges")?,
            default_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 0,
                max_task_count: 1,
            },
        )),
        value => Err(cli_unknown_arg_error("object-store-schedule-plan", value)),
    }
}

fn emit_object_store_checkpoint_retry_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let input = match object_store_checkpoint_retry_fixture(scenario) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "object-store-checkpoint-retry-plan",
                format,
                "object-store checkpoint/retry planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_checkpoint_retry(input);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-checkpoint-retry-plan",
        format,
        status,
        "object-store checkpoint/retry report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_checkpoint_retry_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_checkpoint_retry_output_fields(
    report: &ObjectStoreCheckpointRetryReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_checkpoint_retry_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_checkpoint_retry_status",
        report.status.as_str(),
    );
    push_count_field(&mut fields, "task_count", report.task_count);
    push_count_field(
        &mut fields,
        "retryable_task_count",
        report.retryable_task_count,
    );
    push_count_field(
        &mut fields,
        "planned_checkpoint_record_count",
        report.planned_checkpoint_record_count,
    );
    push_count_field(
        &mut fields,
        "planned_attempt_record_count",
        report.planned_attempt_record_count,
    );
    push_bool_field(
        &mut fields,
        "requires_retry_policy",
        report.requires_retry_policy,
    );
    push_bool_field(
        &mut fields,
        "requires_checkpoint_plan",
        report.requires_checkpoint_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_keys",
        report.requires_idempotency_keys,
    );
    push_bool_field(
        &mut fields,
        "requires_attempt_records",
        report.requires_attempt_records,
    );
    push_bool_field(
        &mut fields,
        "requires_cleanup_policy",
        report.requires_cleanup_policy,
    );
    push_bool_field(
        &mut fields,
        "retry_execution_allowed",
        report.retry_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "checkpoint_write_allowed",
        report.checkpoint_write_allowed,
    );
    push_bool_field(
        &mut fields,
        "cleanup_execution_allowed",
        report.cleanup_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "coordinator_started",
        report.coordinator_started,
    );
    push_bool_field(&mut fields, "worker_started", report.worker_started);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_checkpoint_retry_fixture(
    scenario: &str,
) -> Result<ObjectStoreCheckpointRetryInput, ShardLoomError> {
    let scheduling_report = |schedule_scenario: &str| -> Result<
        ObjectStoreDistributedSchedulingReport,
        ShardLoomError,
    > {
        let (manifest, range_policy, scheduling_policy) =
            object_store_schedule_fixture(schedule_scenario)?;
        let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
        Ok(plan_object_store_distributed_scheduling(
            coalescing_report,
            scheduling_policy,
        ))
    };
    let ready = || -> Result<ObjectStoreCheckpointRetryInput, ShardLoomError> {
        Ok(
            ObjectStoreCheckpointRetryInput::new(scheduling_report("multi-task")?)
                .with_retry_policy(true)
                .with_checkpoint_plan(true)
                .with_idempotency_keys(true)
                .with_attempt_record(true)
                .with_cleanup_policy(true),
        )
    };

    match scenario {
        "ready" => ready(),
        "missing-retry" => Ok(ready()?.with_retry_policy(false)),
        "missing-checkpoint" => Ok(ready()?.with_checkpoint_plan(false)),
        "missing-idempotency" => Ok(ready()?.with_idempotency_keys(false)),
        "missing-attempt" => Ok(ready()?.with_attempt_record(false)),
        "missing-cleanup" => Ok(ready()?.with_cleanup_policy(false)),
        "blocked-scheduling" => Ok(ObjectStoreCheckpointRetryInput::new(scheduling_report(
            "task-budget",
        )?)
        .with_retry_policy(true)
        .with_checkpoint_plan(true)
        .with_idempotency_keys(true)
        .with_attempt_record(true)
        .with_cleanup_policy(true)),
        value => Err(cli_unknown_arg_error(
            "object-store-checkpoint-retry-plan",
            value,
        )),
    }
}

fn emit_object_store_commit_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let input = match object_store_commit_fixture(scenario) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "object-store-commit-plan",
                format,
                "object-store commit planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_commit_protocol(input);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-commit-plan",
        format,
        status,
        "object-store commit protocol report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_commit_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_commit_output_fields(
    report: &ObjectStoreCommitProtocolReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_commit_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_commit_status",
        report.status.as_str(),
    );
    push_bool_field(
        &mut fields,
        "object_store_target",
        report.object_store_target,
    );
    push_bool_field(
        &mut fields,
        "requires_staging_prefix",
        report.requires_staging_prefix,
    );
    push_bool_field(
        &mut fields,
        "requires_manifest_pointer_update",
        report.requires_manifest_pointer_update,
    );
    push_bool_field(
        &mut fields,
        "requires_commit_record",
        report.requires_commit_record,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_key",
        report.requires_idempotency_key,
    );
    push_bool_field(
        &mut fields,
        "requires_cleanup_plan",
        report.requires_cleanup_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_atomic_commit_evidence",
        report.requires_atomic_commit_evidence,
    );
    push_bool_field(
        &mut fields,
        "commit_execution_allowed",
        report.commit_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_commit_fixture(
    scenario: &str,
) -> Result<ObjectStoreCommitProtocolInput, ShardLoomError> {
    let ready = || -> Result<ObjectStoreCommitProtocolInput, ShardLoomError> {
        Ok(
            ObjectStoreCommitProtocolInput::new(DatasetUri::new("s3://bucket/table/_commit")?)
                .with_staging_prefix(true)
                .with_manifest_pointer_update(true)
                .with_commit_record(true)
                .with_idempotency_key(true)
                .with_cleanup_plan(true)
                .with_provider_atomic_commit(true),
        )
    };
    match scenario {
        "ready" => ready(),
        "missing-staging" => Ok(ready()?.with_staging_prefix(false)),
        "missing-idempotency" => Ok(ready()?.with_idempotency_key(false)),
        "missing-atomicity" => Ok(ready()?.with_provider_atomic_commit(false)),
        "local-file" => Ok(ObjectStoreCommitProtocolInput::new(DatasetUri::new(
            "file://tmp/table/_commit",
        )?)
        .with_staging_prefix(true)
        .with_manifest_pointer_update(true)
        .with_commit_record(true)
        .with_idempotency_key(true)
        .with_cleanup_plan(true)
        .with_provider_atomic_commit(true)),
        value => Err(cli_unknown_arg_error("object-store-commit-plan", value)),
    }
}

fn object_store_request_planner_fixture(
    scenario: &str,
) -> Result<ObjectStoreRequestPlannerReport, ShardLoomError> {
    let schedule_scenario = match scenario {
        "ready" | "missing-idempotency" | "commit-missing-idempotency" => "multi-task",
        "missing-ranges" => "missing-ranges",
        "task-budget" => "task-budget",
        value => return Err(cli_unknown_arg_error("object-store-request-plan", value)),
    };
    let (manifest, range_policy, scheduling_policy) =
        object_store_schedule_fixture(schedule_scenario)?;
    let range_report = plan_object_store_ranges(manifest.clone(), range_policy);
    let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
    let scheduling_report =
        plan_object_store_distributed_scheduling(coalescing_report.clone(), scheduling_policy);
    let checkpoint_retry_input = ObjectStoreCheckpointRetryInput::new(scheduling_report.clone())
        .with_retry_policy(true)
        .with_checkpoint_plan(true)
        .with_idempotency_keys(scenario != "missing-idempotency")
        .with_attempt_record(true)
        .with_cleanup_policy(true);
    let checkpoint_retry_report = plan_object_store_checkpoint_retry(checkpoint_retry_input);
    let commit_scenario = if scenario == "commit-missing-idempotency" {
        "missing-idempotency"
    } else {
        "ready"
    };
    let commit_report =
        plan_object_store_commit_protocol(object_store_commit_fixture(commit_scenario)?);

    Ok(plan_object_store_request_planner(
        range_report,
        coalescing_report,
        scheduling_report,
        checkpoint_retry_report,
        commit_report,
    ))
}

fn object_store_range_fixture(scenario: &str) -> Result<DatasetManifest, ShardLoomError> {
    object_store_range_fixture_for_command("object-store-range-plan", scenario)
}

fn object_store_range_fixture_for_command(
    command: &str,
    scenario: &str,
) -> Result<DatasetManifest, ShardLoomError> {
    match scenario {
        "s3-ranges" => object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 1024), ByteRange::new(2048, 1024)],
        ),
        "missing-ranges" => object_store_range_manifest("s3://bucket/table.vortex", vec![]),
        "local-file" => object_store_range_manifest(
            "file://object-store-range/table.vortex",
            vec![ByteRange::new(0, 1024)],
        ),
        "invalid-range" => {
            object_store_range_manifest("s3://bucket/table.vortex", vec![ByteRange::new(0, 0)])
        }
        "oversized-range" => object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 32 * 1024 * 1024)],
        ),
        "empty" => Ok(object_store_range_base_manifest()?),
        value => Err(cli_unknown_arg_error(command, value)),
    }
}

fn object_store_range_base_manifest() -> Result<DatasetManifest, ShardLoomError> {
    Ok(DatasetManifest::new(
        ManifestId::new("object-store-range-manifest")?,
        DatasetRef::from_uri(DatasetUri::new("s3://bucket/table.vortex")?)?,
        SnapshotRef::new(SnapshotId::new("object-store-range-snapshot")?),
    ))
}

fn object_store_range_manifest(
    uri: &str,
    ranges: Vec<ByteRange>,
) -> Result<DatasetManifest, ShardLoomError> {
    let dataset_uri = DatasetUri::new(uri)?;
    let mut manifest = DatasetManifest::new(
        ManifestId::new("object-store-range-manifest")?,
        DatasetRef::from_uri(dataset_uri.clone())?,
        SnapshotRef::new(SnapshotId::new("object-store-range-snapshot")?),
    );
    let file = FileDescriptor::new(
        dataset_uri,
        DatasetFormat::Vortex,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(128 * 1024 * 1024);
    let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
    layout.byte_ranges = ranges;
    layout.physical_size_bytes = Some(8 * 1024 * 1024);
    let segment = EncodedSegment::new(
        SegmentId::new("object-store-range-segment")?,
        ColumnRef::new("value")?,
        LogicalDType::Int64,
        Nullability::Nullable,
        layout,
        SegmentStats::with_row_count(64_000),
    );
    manifest.add_file(file.clone());
    manifest.add_segment(ManifestSegment::new(segment, file));
    Ok(manifest)
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_coalesce_unknown_scenario_uses_coalesce_command_name() {
        let error = object_store_range_fixture_for_command("object-store-coalesce-plan", "unknown")
            .expect_err("unknown scenario");

        assert!(
            error
                .message()
                .contains("object-store-coalesce-plan unknown argument/value: unknown")
        );
    }

    #[test]
    fn object_store_request_fields_include_no_io_no_fallback() {
        let report = object_store_request_planner_fixture("ready").expect("request planner");
        let fields = object_store_request_output_fields(&report, "ready");

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.object_store_request_planner.v1"
        );
        assert_eq!(
            output_field(&fields, "report_id"),
            "cg10.object_store_request_planner.aggregate"
        );
        assert_eq!(
            output_field(&fields, "object_store_request_status"),
            "planned"
        );
        assert_eq!(output_field(&fields, "planned_surface_count"), "5");
        assert_eq!(output_field(&fields, "blocked_surface_count"), "0");
        assert_eq!(output_field(&fields, "range_status"), "planned");
        assert_eq!(output_field(&fields, "scheduling_status"), "planned");
        assert_eq!(output_field(&fields, "checkpoint_retry_status"), "ready");
        assert_eq!(output_field(&fields, "commit_status"), "ready");
        assert_eq!(output_field(&fields, "coordinator_started"), "false");
        assert_eq!(output_field(&fields, "worker_started"), "false");
        assert_eq!(output_field(&fields, "task_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "object_store_io"), "false");
        assert_eq!(output_field(&fields, "write_io"), "false");
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map_or("", |(_, value)| value.as_str())
    }
}
