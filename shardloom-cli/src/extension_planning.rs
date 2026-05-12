//! Extension and UDF planning CLI handlers.
//!
//! These handlers emit metadata-only extension and UDF reports. They do not
//! dynamically load extension code, execute UDFs, write data, invoke external
//! services, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, ExtensionId, ExtensionInspectionReport, ExtensionLicenseKind, ExtensionManifest,
    ExtensionProvenance, ExtensionRegistrySnapshot, ExtensionVersion, OutputFormat, ShardLoomError,
    UdfRuntimeKind,
};

use crate::cli_output::{emit, emit_error};

pub(crate) fn handle_extension_registry(format: OutputFormat) -> ExitCode {
    let snapshot = ExtensionRegistrySnapshot::empty();
    emit(
        "extension-registry",
        format,
        CommandStatus::Success,
        "extension registry metadata-only snapshot".to_string(),
        snapshot.to_human_text(),
        snapshot.diagnostics.clone(),
        extension_report_only_fields("extension_registry"),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_extension_inspect(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(extension_id) = args.next() else {
        return emit_error(
            "extension-inspect",
            format,
            "extension inspect failed",
            &ShardLoomError::InvalidOperation("missing extension_id".to_string()),
        );
    };
    let id = match ExtensionId::new(extension_id.clone()) {
        Ok(v) => v,
        Err(e) => return emit_error("extension-inspect", format, "extension inspect failed", &e),
    };
    let manifest = match ExtensionManifest::new(
        id,
        extension_id,
        ExtensionVersion::new(0, 1, 0),
        shardloom_core::ExtensionCategory::Unknown,
        ExtensionProvenance::new(ExtensionLicenseKind::Unknown),
    ) {
        Ok(v) => v,
        Err(e) => return emit_error("extension-inspect", format, "extension inspect failed", &e),
    };
    let report = ExtensionInspectionReport::requires_review(
        manifest,
        "Extension inspection is metadata-only and requires provenance review.",
    );
    let status = if report.has_errors() {
        CommandStatus::Warning
    } else {
        CommandStatus::Success
    };
    emit(
        "extension-inspect",
        format,
        status,
        "extension inspection metadata-only report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        extension_report_only_fields("extension_inspect"),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_udf_runtime_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let runtime = match args.next().as_deref() {
        Some("rust") => UdfRuntimeKind::RustNative,
        Some("wasm") => UdfRuntimeKind::Wasm,
        Some("python") => UdfRuntimeKind::Python,
        Some("sql") => UdfRuntimeKind::SqlDefined,
        Some("external") => UdfRuntimeKind::ExternalService,
        Some(_) | None => UdfRuntimeKind::Unknown,
    };
    let text = format!(
        "udf runtime={} available_initially={} sandboxing_required={} execution=not_performed fallback_execution=disabled",
        runtime.as_str(),
        runtime.is_available_initially(),
        runtime.requires_sandboxing()
    );
    emit(
        "udf-runtime-plan",
        format,
        CommandStatus::Success,
        "udf runtime availability skeleton".to_string(),
        text,
        vec![],
        extension_report_only_fields("udf_runtime_plan"),
    );
    ExitCode::SUCCESS
}

fn extension_report_only_fields(mode: &str) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), mode.to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        ("extension_code_executed".to_string(), "false".to_string()),
        ("dynamic_loading".to_string(), "false".to_string()),
    ]
}
