//! Status and capability-discovery CLI handlers.
//!
//! This is the first physical command-family handler split for Priority 3.9.
//! It keeps behavior identical to the old `main.rs` match arms while routing
//! output through the shared typed-envelope renderer.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{
    CapabilityCertificationReport, CommandStatus, EngineCapabilities, OutputFormat,
    plan_world_class_sufficiency,
};

use crate::{
    CapabilityDiscoveryScope,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, emit_capability_certification, emit_world_class_surface_capability,
};

pub(crate) fn handle_status(format: OutputFormat) -> ExitCode {
    let status = shardloom_exec::status();
    emit(
        "status",
        format,
        CommandStatus::Success,
        "engine status".to_string(),
        format!("{}\nfallback execution: disabled", status.summary),
        vec![],
        vec![(
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        )],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_capabilities(mut args: IntoIter<String>, format: OutputFormat) -> ExitCode {
    let scope = match CapabilityDiscoveryScope::parse(args.next().as_deref()) {
        Ok(scope) => scope,
        Err(error) => {
            return emit_error(
                "capabilities",
                format,
                "capability discovery failed",
                &error,
            );
        }
    };
    if let Some(extra) = args.next() {
        return emit_error(
            "capabilities",
            format,
            "capability discovery failed",
            &cli_unknown_arg_error("capabilities", &extra),
        );
    }
    if scope.world_class_dimension().is_some() {
        let report = plan_world_class_sufficiency();
        emit_world_class_surface_capability(scope, format, &report);
        return ExitCode::SUCCESS;
    }
    if scope != CapabilityDiscoveryScope::Engine {
        let report = CapabilityCertificationReport::contract_only();
        emit_capability_certification(scope, format, &report);
        return ExitCode::SUCCESS;
    }
    let capabilities = EngineCapabilities::current();
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        "engine capabilities".to_string(),
        capabilities.to_human_text(),
        vec![],
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("native_input".to_string(), "vortex".to_string()),
            ("native_output".to_string(), "vortex".to_string()),
        ],
    );
    ExitCode::SUCCESS
}
