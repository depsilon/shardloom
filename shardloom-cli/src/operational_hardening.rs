//! Operational hardening, security, and effect-policy CLI handlers.
//!
//! These handlers are report-only planning and governance surfaces. They do not
//! resolve credentials, load secrets, execute effects, write data, or provide
//! fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    AgentSafetyMode, CommandStatus, EffectBudgetReport, OutputFormat, RedactionPolicy,
    SecurityPlan, plan_security_governance_evidence_gate,
};

use crate::{cli_output::emit, effect_budget_fields, security_governance_evidence_gate_fields};

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
