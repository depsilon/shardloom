//! Evidence, correctness, certificate, and Native I/O planning handlers.
//!
//! These commands emit report-only evidence planning surfaces. They do not run
//! correctness harnesses, read data, emit runtime certificates from execution,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, CorrectnessValidationPlan, OutputFormat, plan_correctness_differential_harness,
    plan_execution_certificate_evidence_surface, plan_native_io_envelope, plan_universal_harness,
};

use crate::{
    cli_output::emit, correctness_harness_fields, correctness_plan_fields,
    execution_certificate_surface_fields, native_io_envelope_fields, universal_harness_fields,
};

pub(crate) fn handle_correctness_plan(format: OutputFormat) -> ExitCode {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    emit(
        "correctness-plan",
        format,
        CommandStatus::Success,
        "correctness validation foundation plan".to_string(),
        plan.to_human_text(),
        vec![],
        correctness_plan_fields(&plan),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_correctness_harness_plan(format: OutputFormat) -> ExitCode {
    let report =
        plan_correctness_differential_harness(CorrectnessValidationPlan::default_foundation_plan());
    emit_report(
        "correctness-harness-plan",
        "correctness and differential harness plan",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        correctness_harness_fields(&report),
        format,
    )
}

pub(crate) fn handle_execution_certificate_plan(format: OutputFormat) -> ExitCode {
    let report = plan_execution_certificate_evidence_surface();
    emit_report(
        "execution-certificate-plan",
        "execution certificate evidence surface",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        execution_certificate_surface_fields(&report),
        format,
    )
}

pub(crate) fn handle_universal_harness_plan(format: OutputFormat) -> ExitCode {
    let report = plan_universal_harness();
    emit_report(
        "universal-harness-plan",
        "universal harness plan",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        universal_harness_fields(&report),
        format,
    )
}

pub(crate) fn handle_native_io_envelope_plan(format: OutputFormat) -> ExitCode {
    let report = plan_native_io_envelope();
    emit_report(
        "native-io-envelope-plan",
        "native I/O envelope plan",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        native_io_envelope_fields(&report),
        format,
    )
}

fn emit_report(
    command: &str,
    summary: &str,
    has_errors: bool,
    human_text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    fields: Vec<(String, String)>,
    format: OutputFormat,
) -> ExitCode {
    emit(
        command,
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        summary.to_string(),
        human_text,
        diagnostics,
        fields,
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
