//! Benchmark planning CLI handlers.
//!
//! These commands are report-only benchmark contract surfaces. They do not run
//! benchmarks, invoke external engines, publish performance claims, or provide
//! fallback execution.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError, plan_benchmark_claim_evidence};

use crate::{
    benchmark_claim_evidence_fields, benchmark_plan_fields, benchmark_plan_for_scope,
    cli_output::{emit, emit_error},
};

pub(crate) fn handle_benchmark_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scope = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "benchmark-plan",
            format,
            "benchmark plan failed",
            &ShardLoomError::InvalidOperation(format!(
                "unknown extra benchmark-plan argument: {extra}"
            )),
        );
    }
    let plan = match benchmark_plan_for_scope(scope.as_deref()) {
        Ok(plan) => plan,
        Err(error) => {
            return emit_error("benchmark-plan", format, "benchmark plan failed", &error);
        }
    };
    emit(
        "benchmark-plan",
        format,
        CommandStatus::Success,
        "benchmark plan".to_string(),
        plan.to_human_text(),
        vec![],
        benchmark_plan_fields(&plan),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_benchmark_claim_evidence_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scope = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "benchmark-claim-evidence-plan",
            format,
            "benchmark claim evidence plan failed",
            &ShardLoomError::InvalidOperation(format!(
                "unknown extra benchmark-claim-evidence-plan argument: {extra}"
            )),
        );
    }
    let plan = match benchmark_plan_for_scope(scope.as_deref()) {
        Ok(plan) => plan,
        Err(error) => {
            return emit_error(
                "benchmark-claim-evidence-plan",
                format,
                "benchmark claim evidence plan failed",
                &error,
            );
        }
    };
    let scope_label = scope.unwrap_or_else(|| "foundation".to_string());
    let report = plan_benchmark_claim_evidence(scope_label, &plan);
    emit(
        "benchmark-claim-evidence-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "benchmark claim evidence plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        benchmark_claim_evidence_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
