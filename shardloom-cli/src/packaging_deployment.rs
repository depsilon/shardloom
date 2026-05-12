//! Packaging, release, wrapper, and agent-contract CLI handlers.
//!
//! These commands are report-only planning surfaces. They do not publish
//! packages, push artifacts, execute external engines, or perform fallback work.

use std::process::ExitCode;

use shardloom_core::{
    AgentContractPack, CommandStatus, OutputFormat, PythonWrapperFoundationReport, ReleasePlan,
};

use crate::{
    agent_contract_pack_fields, cli_output::emit, python_wrapper_fields, release_plan_fields,
};

pub(crate) fn handle_release_plan(format: OutputFormat) -> ExitCode {
    emit_release_or_package_plan(
        "release-plan",
        "release plan skeleton",
        "release_plan",
        format,
    )
}

pub(crate) fn handle_package_plan(format: OutputFormat) -> ExitCode {
    emit_release_or_package_plan(
        "package-plan",
        "package plan skeleton",
        "package_plan",
        format,
    )
}

fn emit_release_or_package_plan(
    command: &str,
    summary: &str,
    mode: &str,
    format: OutputFormat,
) -> ExitCode {
    let plan = ReleasePlan::default_foundation_plan();
    let evidence = plan.release_readiness_evidence();
    let publication = plan.publication_boundary_report();
    emit(
        command,
        format,
        CommandStatus::Success,
        summary.to_string(),
        format!(
            "{}\n\n{}\n\n{}",
            plan.to_human_text(),
            evidence.to_human_text(),
            publication.to_human_text()
        ),
        plan.diagnostics.clone(),
        release_plan_fields(&plan, &evidence, &publication, mode),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_agent_contract_pack(format: OutputFormat) -> ExitCode {
    let report = AgentContractPack::default_pack();
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "agent-contract-pack",
        format,
        status,
        "agent contract pack".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        agent_contract_pack_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_python_wrapper_plan(format: OutputFormat) -> ExitCode {
    let report = PythonWrapperFoundationReport::contract_only();
    emit(
        "python-wrapper-plan",
        format,
        report.status(),
        "python wrapper foundation".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        python_wrapper_fields(&report),
    );
    ExitCode::SUCCESS
}
