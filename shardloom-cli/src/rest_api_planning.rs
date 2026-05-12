//! REST/API planning CLI handlers.
//!
//! This module owns the current report-only API protocol planning command. It
//! does not start a server, open sockets, or authorize remote execution.

use std::process::ExitCode;

use shardloom_core::{CliApiJsonProtocolReport, OutputFormat, ReleasePlan};

use crate::{api_protocol_fields, cli_output::emit};

pub(crate) fn handle_api_compat_plan(format: OutputFormat) -> ExitCode {
    let plan = ReleasePlan::default_foundation_plan();
    let protocol = CliApiJsonProtocolReport::contract_only();
    let mut diagnostics = plan.diagnostics.clone();
    diagnostics.extend(protocol.diagnostics.clone());
    emit(
        "api-compat-plan",
        format,
        protocol.status(),
        "api compatibility and cli json protocol foundation".to_string(),
        format!("{}\n\n{}", plan.to_human_text(), protocol.to_human_text()),
        diagnostics,
        api_protocol_fields(&protocol),
    );
    ExitCode::SUCCESS
}
