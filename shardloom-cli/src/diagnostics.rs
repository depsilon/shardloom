//! Diagnostic and explain/estimate CLI handlers.
//!
//! These handlers expose report-only diagnostics. They do not inspect user
//! datasets, collect runtime profiles, execute plans, invoke external engines,
//! or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, FeatureFootprintReport, ObservabilityPlan, OutputFormat,
    RuntimeObservabilityReport, plan_observability_schema_coverage,
};
use shardloom_plan::{EstimateReport, ExplainReport};

use crate::{cli_output::emit, feature_footprint_fields, observability_schema_coverage_fields};

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

pub(crate) fn handle_observability_plan(format: OutputFormat) -> ExitCode {
    let plan = ObservabilityPlan::default_foundation_plan();
    emit_observability_style_report(
        "observability-plan",
        "observability plan",
        plan.to_human_text(),
        plan.diagnostics.clone(),
        "observability_plan",
        "not_performed",
        format,
    )
}

pub(crate) fn handle_observability_schema_coverage(format: OutputFormat) -> ExitCode {
    let report = plan_observability_schema_coverage();
    emit(
        "observability-schema-coverage",
        format,
        CommandStatus::Success,
        "observability schema coverage".to_string(),
        report.to_human_text(),
        vec![],
        observability_schema_coverage_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_runtime_report(format: OutputFormat) -> ExitCode {
    let report = RuntimeObservabilityReport::not_run();
    emit_observability_style_report(
        "runtime-report",
        "runtime observability report",
        report.to_human_text(),
        report.diagnostics.clone(),
        "runtime_report",
        "not_performed",
        format,
    )
}

pub(crate) fn handle_profile_plan(format: OutputFormat) -> ExitCode {
    let plan = ObservabilityPlan::collection_not_implemented(
        "profiling",
        "Profiling domain types exist, but runtime profiling collection is not implemented yet.",
    );
    emit(
        "profile-plan",
        format,
        CommandStatus::Unsupported,
        "profiling collection not implemented".to_string(),
        plan.to_human_text(),
        plan.diagnostics.clone(),
        observability_style_fields("profile_plan", "not_performed"),
    );
    ExitCode::SUCCESS
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

fn emit_observability_style_report(
    command: &str,
    summary: &str,
    human_text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    mode: &str,
    metrics_collection: &str,
    format: OutputFormat,
) -> ExitCode {
    emit(
        command,
        format,
        CommandStatus::Success,
        summary.to_string(),
        human_text,
        diagnostics,
        observability_style_fields(mode, metrics_collection),
    );
    ExitCode::SUCCESS
}

fn observability_style_fields(mode: &str, metrics_collection: &str) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), mode.to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "metrics_collection".to_string(),
            metrics_collection.to_string(),
        ),
    ]
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
