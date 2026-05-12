//! Object-store planning CLI handlers.
//!
//! These handlers emit report-only object-store planning surfaces. They do not
//! read remote objects, open credentials, execute distributed tasks, write
//! outputs, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, OutputFormat};
use shardloom_plan::plan_object_store_runtime_promotion_gate;

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, emit_object_store_checkpoint_retry_plan,
    emit_object_store_coalesce_plan, emit_object_store_commit_plan, emit_object_store_range_plan,
    emit_object_store_request_plan, emit_object_store_schedule_plan,
    object_store_runtime_promotion_gate_fields,
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
