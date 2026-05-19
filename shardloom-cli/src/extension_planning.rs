//! Extension and UDF planning CLI handlers.
//!
//! These handlers emit metadata-only extension and UDF reports. They do not
//! dynamically load extension code, execute UDFs, write data, invoke external
//! services, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, ExtensionId, ExtensionInspectionReport, ExtensionLicenseKind, ExtensionManifest,
    ExtensionManifestEffectCapabilityMatrix, ExtensionManifestEffectCapabilityRow,
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
    let mut fields = vec![
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
    ];
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    fields
}

pub(crate) fn append_extension_manifest_effect_capability_matrix_fields(
    fields: &mut Vec<(String, String)>,
) {
    let matrix = ExtensionManifestEffectCapabilityMatrix::report_only();
    let row_order = matrix.row_order().join(",");
    let blocker_ids = matrix.blocker_ids().join(",");
    let required_evidence = matrix.required_evidence().join("|");
    for (key, value) in [
        (
            "extension_manifest_effect_matrix_schema_version",
            matrix.schema_version,
        ),
        ("extension_manifest_effect_matrix_id", matrix.matrix_id),
        ("extension_manifest_effect_docs_ref", matrix.docs_ref),
        (
            "extension_manifest_effect_support_status_vocabulary",
            "report_only,blocked,unsupported",
        ),
        (
            "extension_manifest_effect_claim_gate_status",
            matrix.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "extension_manifest_effect_row_count",
        matrix.rows.len(),
    );
    for (key, value) in [
        ("extension_manifest_effect_row_order", row_order.as_str()),
        (
            "extension_manifest_effect_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "extension_manifest_effect_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_extension_manifest_effect_matrix_bool_fields(fields, &matrix);
    for row in &matrix.rows {
        append_extension_manifest_effect_capability_row_fields(fields, row);
    }
}

fn append_extension_manifest_effect_matrix_bool_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &ExtensionManifestEffectCapabilityMatrix,
) {
    for (key, value) in [
        (
            "extension_manifest_effect_all_runtime_blocked",
            matrix.all_runtime_blocked(),
        ),
        (
            "extension_manifest_effect_all_external_effects_blocked",
            matrix.all_external_effects_blocked(),
        ),
        (
            "extension_manifest_effect_runtime_execution",
            matrix.runtime_execution,
        ),
        (
            "extension_manifest_effect_extension_code_executed",
            matrix.extension_code_executed,
        ),
        (
            "extension_manifest_effect_dynamic_loading",
            matrix.dynamic_loading,
        ),
        (
            "extension_manifest_effect_udf_execution",
            matrix.udf_execution,
        ),
        (
            "extension_manifest_effect_external_effect_executed",
            matrix.external_effect_executed,
        ),
        (
            "extension_manifest_effect_credential_resolution_performed",
            matrix.credential_resolution_performed,
        ),
        (
            "extension_manifest_effect_network_probe_performed",
            matrix.network_probe_performed,
        ),
        (
            "extension_manifest_effect_dependency_expansion_allowed",
            matrix.dependency_expansion_allowed,
        ),
        (
            "extension_manifest_effect_fallback_attempted",
            matrix.fallback_attempted,
        ),
        (
            "extension_manifest_effect_external_engine_invoked",
            matrix.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_extension_manifest_effect_capability_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &ExtensionManifestEffectCapabilityRow,
) {
    let prefix = format!("extension_manifest_effect_row_{}", row.row_id);
    for (suffix, value) in [
        ("extension_type", row.extension_type),
        ("support_status", row.support_status),
        ("manifest_status", row.manifest_status),
        ("required_permissions", row.required_permissions),
        ("sandbox_policy", row.sandbox_policy),
        ("effect_metadata", row.effect_metadata),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_boundary_required"),
        row.materialization_boundary_required,
    );
    for (suffix, value) in [
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    append_extension_manifest_effect_row_bool_fields(fields, row, &prefix);
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_extension_manifest_effect_row_bool_fields(
    fields: &mut Vec<(String, String)>,
    row: &ExtensionManifestEffectCapabilityRow,
    prefix: &str,
) {
    for (suffix, value) in [
        ("runtime_execution", row.runtime_execution),
        ("extension_code_executed", row.extension_code_executed),
        ("dynamic_loading", row.dynamic_loading),
        ("udf_execution", row.udf_execution),
        ("external_effect_executed", row.external_effect_executed),
        (
            "credential_resolution_performed",
            row.credential_resolution_performed,
        ),
        ("network_probe_performed", row.network_probe_performed),
        (
            "dependency_expansion_allowed",
            row.dependency_expansion_allowed,
        ),
        ("fallback_attempted", row.fallback_attempted),
        ("external_engine_invoked", row.external_engine_invoked),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}
