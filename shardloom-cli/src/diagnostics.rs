//! Diagnostic and explain/estimate CLI handlers.
//!
//! These handlers expose report-only diagnostics. They do not inspect user
//! datasets, collect runtime profiles, execute plans, invoke external engines,
//! or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, FeatureFootprintReport, OutputFormat};
use shardloom_plan::{EstimateReport, ExplainReport};

use crate::{cli_output::emit, feature_footprint_fields};

pub(crate) fn handle_feature_footprint(format: OutputFormat) -> ExitCode {
    let report = FeatureFootprintReport::contract_only();
    emit_feature_footprint_report(
        "feature-footprint",
        "feature footprint report",
        &report,
        format,
    )
}

pub(crate) fn handle_doctor(format: OutputFormat) -> ExitCode {
    let report = FeatureFootprintReport::contract_only();
    let mut fields = feature_footprint_fields(&report);
    fields.push(("native_input".to_string(), "vortex".to_string()));
    fields.push(("native_output".to_string(), "vortex".to_string()));
    fields.push((
        "doctor_uses_feature_footprint".to_string(),
        "true".to_string(),
    ));
    emit(
        "doctor",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "doctor checks".to_string(),
        format!("ShardLoom doctor\n{}", report.to_human_text()),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_explain(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let operation = args
        .next()
        .unwrap_or_else(|| "<unspecified operation>".to_string());
    let report = ExplainReport::unsupported(
        operation,
        "planning",
        "Real planning is not implemented yet.",
    );
    emit_unsupported_plan_report(
        "explain",
        "explain plan",
        report.to_human_text(),
        report.diagnostics.clone(),
        report.has_errors(),
        format,
    )
}

pub(crate) fn handle_estimate(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let operation = args
        .next()
        .unwrap_or_else(|| "<unspecified operation>".to_string());
    let report = EstimateReport::unsupported(
        operation,
        "estimation",
        "Real estimation is not implemented yet.",
    );
    emit_unsupported_plan_report(
        "estimate",
        "estimate plan",
        report.to_human_text(),
        report.diagnostics.clone(),
        report.has_errors(),
        format,
    )
}

fn emit_feature_footprint_report(
    command: &str,
    summary: &str,
    report: &FeatureFootprintReport,
    format: OutputFormat,
) -> ExitCode {
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        command,
        format,
        status,
        summary.to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        feature_footprint_fields(report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn emit_unsupported_plan_report(
    command: &str,
    summary: &str,
    human_text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    has_errors: bool,
    format: OutputFormat,
) -> ExitCode {
    emit(
        command,
        format,
        CommandStatus::Unsupported,
        summary.to_string(),
        human_text,
        diagnostics,
        vec![("mode".to_string(), "plan_only".to_string())],
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
