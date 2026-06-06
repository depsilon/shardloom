//! Extension and UDF planning CLI handlers.
//!
//! These handlers emit metadata-only extension and UDF reports. They do not
//! dynamically load extension code, execute UDFs, write data, invoke external
//! services, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DeterministicEmbeddingVectorFixtureReport, DeterministicScalarUdfFixtureReport,
    EffectfulOperationAdmissionMatrix, EffectfulOperationAdmissionRow, ExtensionId,
    ExtensionInspectionReport, ExtensionLicenseKind, ExtensionManifest,
    ExtensionManifestEffectCapabilityMatrix, ExtensionManifestEffectCapabilityRow,
    ExtensionProvenance, ExtensionRegistrySnapshot, ExtensionVersion, OutputFormat,
    PluginAbiUdfSandboxBlockerReport, PluginAbiUdfSandboxBlockerRow, ShardLoomError,
    UdfRuntimeKind, plan_plugin_abi_udf_sandbox_blocker,
    run_deterministic_embedding_vector_fixture, run_deterministic_scalar_udf_fixture,
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
        Some("fixture" | "builtin-fixture" | "builtin-deterministic") => {
            UdfRuntimeKind::BuiltinDeterministicFixture
        }
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
        udf_runtime_plan_fields(runtime),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_udf_local_scalar_fixture_smoke(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(values_raw) = args.next() else {
        return emit_error(
            "udf-local-scalar-fixture-smoke",
            format,
            "udf local scalar fixture failed",
            &ShardLoomError::InvalidOperation(
                "missing comma-separated int64/null values".to_string(),
            ),
        );
    };
    let values = match parse_nullable_i64_values(&values_raw) {
        Ok(values) => values,
        Err(error) => {
            return emit_error(
                "udf-local-scalar-fixture-smoke",
                format,
                "udf local scalar fixture failed",
                &error,
            );
        }
    };
    let report = match run_deterministic_scalar_udf_fixture(&values) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "udf-local-scalar-fixture-smoke",
                format,
                "udf local scalar fixture failed",
                &error,
            );
        }
    };
    emit(
        "udf-local-scalar-fixture-smoke",
        format,
        CommandStatus::Success,
        "deterministic scalar UDF fixture smoke".to_string(),
        report.to_human_text(),
        vec![],
        udf_local_scalar_fixture_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_embedding_vector_local_fixture_smoke(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(texts_raw) = args.next() else {
        return emit_error(
            "embedding-vector-local-fixture-smoke",
            format,
            "embedding/vector local fixture failed",
            &ShardLoomError::InvalidOperation(
                "missing semicolon-separated text values".to_string(),
            ),
        );
    };
    let mut query_text = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--query" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        "embedding-vector-local-fixture-smoke",
                        format,
                        "embedding/vector local fixture failed",
                        &ShardLoomError::InvalidOperation("missing --query value".to_string()),
                    );
                };
                query_text = Some(value);
            }
            other => {
                return emit_error(
                    "embedding-vector-local-fixture-smoke",
                    format,
                    "embedding/vector local fixture failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown embedding/vector fixture argument: {other}"
                    )),
                );
            }
        }
    }
    let texts = match parse_semicolon_text_values(&texts_raw) {
        Ok(texts) => texts,
        Err(error) => {
            return emit_error(
                "embedding-vector-local-fixture-smoke",
                format,
                "embedding/vector local fixture failed",
                &error,
            );
        }
    };
    let query = query_text.unwrap_or_else(|| texts[0].clone());
    let report = match run_deterministic_embedding_vector_fixture(&texts, &query) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "embedding-vector-local-fixture-smoke",
                format,
                "embedding/vector local fixture failed",
                &error,
            );
        }
    };
    emit(
        "embedding-vector-local-fixture-smoke",
        format,
        CommandStatus::Success,
        "deterministic embedding/vector fixture smoke".to_string(),
        report.to_human_text(),
        vec![],
        embedding_vector_local_fixture_fields(&report),
    );
    ExitCode::SUCCESS
}

fn parse_nullable_i64_values(values_raw: &str) -> Result<Vec<Option<i64>>, ShardLoomError> {
    if values_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "comma-separated values must not be empty".to_string(),
        ));
    }
    let mut values = Vec::new();
    for token in values_raw.split(',') {
        let token = token.trim();
        if token.eq_ignore_ascii_case("null") {
            values.push(None);
        } else if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "comma-separated values must not contain empty entries".to_string(),
            ));
        } else {
            let value = token.parse::<i64>().map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "invalid int64 UDF fixture value {token:?}: {error}"
                ))
            })?;
            values.push(Some(value));
        }
    }
    Ok(values)
}

fn parse_semicolon_text_values(values_raw: &str) -> Result<Vec<String>, ShardLoomError> {
    if values_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "semicolon-separated text values must not be empty".to_string(),
        ));
    }
    let mut values = Vec::new();
    for token in values_raw.split(';') {
        let text = token.trim();
        if text.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "semicolon-separated text values must not contain empty entries".to_string(),
            ));
        }
        values.push(text.to_string());
    }
    Ok(values)
}

fn udf_runtime_plan_fields(runtime: UdfRuntimeKind) -> Vec<(String, String)> {
    let mut fields = extension_report_only_fields("udf_runtime_plan");
    fields.extend([
        ("udf_runtime_kind".to_string(), runtime.as_str().to_string()),
        (
            "udf_runtime_available_initially".to_string(),
            runtime.is_available_initially().to_string(),
        ),
        (
            "udf_runtime_sandboxing_required".to_string(),
            runtime.requires_sandboxing().to_string(),
        ),
        (
            "udf_runtime_fixture_command".to_string(),
            if runtime.is_available_initially() {
                "udf-local-scalar-fixture-smoke"
            } else {
                "none"
            }
            .to_string(),
        ),
    ]);
    fields
}

fn udf_local_scalar_fixture_fields(
    report: &DeterministicScalarUdfFixtureReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "udf_local_scalar_fixture_smoke".to_string(),
        ),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("udf_id".to_string(), report.udf_id.to_string()),
        ("udf_version".to_string(), report.udf_version.to_string()),
        (
            "udf_runtime_kind".to_string(),
            report.runtime_kind.as_str().to_string(),
        ),
        ("udf_type".to_string(), "scalar".to_string()),
        ("input_dtype".to_string(), report.input_dtype.to_string()),
        ("output_dtype".to_string(), report.output_dtype.to_string()),
        ("determinism".to_string(), report.determinism.to_string()),
        ("null_policy".to_string(), report.null_policy.to_string()),
        (
            "input_row_count".to_string(),
            report.input_row_count.to_string(),
        ),
        (
            "output_row_count".to_string(),
            report.output_row_count.to_string(),
        ),
        ("input_digest".to_string(), report.input_digest.clone()),
        ("output_digest".to_string(), report.output_digest.clone()),
        ("output_values".to_string(), report.output_values_summary()),
        (
            "overflow_policy_enforced".to_string(),
            report.overflow_policy_enforced.to_string(),
        ),
        (
            "overflow_blocked".to_string(),
            report.overflow_blocked.to_string(),
        ),
        (
            "sandbox_required".to_string(),
            report.sandbox_required.to_string(),
        ),
        (
            "network_allowed".to_string(),
            report.network_allowed.to_string(),
        ),
        (
            "credential_resolution_performed".to_string(),
            report.credential_resolution_performed.to_string(),
        ),
        (
            "dynamic_loading_performed".to_string(),
            report.dynamic_loading_performed.to_string(),
        ),
        (
            "extension_code_executed".to_string(),
            report.extension_code_executed.to_string(),
        ),
        (
            "external_effect_executed".to_string(),
            report.external_effect_executed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "no_fallback_invariant_holds".to_string(),
            report.no_fallback_invariant_holds().to_string(),
        ),
        (
            "claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
    ];
    append_effectful_operation_admission_matrix_fields(&mut fields);
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    fields
}

#[allow(clippy::too_many_lines)]
fn embedding_vector_local_fixture_fields(
    report: &DeterministicEmbeddingVectorFixtureReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "embedding_vector_local_fixture_smoke".to_string(),
        ),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("fixture_id".to_string(), report.fixture_id.to_string()),
        (
            "fixture_version".to_string(),
            report.fixture_version.to_string(),
        ),
        ("input_dtype".to_string(), report.input_dtype.to_string()),
        ("output_dtype".to_string(), report.output_dtype.to_string()),
        ("determinism".to_string(), report.determinism.to_string()),
        (
            "embedding_model_id".to_string(),
            report.embedding_model_id.to_string(),
        ),
        (
            "vector_index_kind".to_string(),
            report.vector_index_kind.to_string(),
        ),
        ("vector_metric".to_string(), report.metric.to_string()),
        ("vector_dimension".to_string(), report.dimension.to_string()),
        (
            "input_row_count".to_string(),
            report.input_row_count.to_string(),
        ),
        (
            "vector_row_count".to_string(),
            report.vector_row_count.to_string(),
        ),
        ("query_text".to_string(), report.query_text.clone()),
        ("query_vector".to_string(), report.query_vector_summary()),
        (
            "nearest_index".to_string(),
            report.nearest_index.to_string(),
        ),
        ("nearest_text".to_string(), report.nearest_text.clone()),
        (
            "nearest_distance_squared".to_string(),
            report.nearest_distance_squared.to_string(),
        ),
        ("nearest_summary".to_string(), report.nearest_summary()),
        ("input_digest".to_string(), report.input_digest.clone()),
        ("vector_digest".to_string(), report.vector_digest.clone()),
        (
            "model_call_performed".to_string(),
            report.model_call_performed.to_string(),
        ),
        (
            "credential_resolution_performed".to_string(),
            report.credential_resolution_performed.to_string(),
        ),
        (
            "network_probe_performed".to_string(),
            report.network_probe_performed.to_string(),
        ),
        (
            "dynamic_loading_performed".to_string(),
            report.dynamic_loading_performed.to_string(),
        ),
        (
            "extension_code_executed".to_string(),
            report.extension_code_executed.to_string(),
        ),
        (
            "external_effect_executed".to_string(),
            report.external_effect_executed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "no_fallback_invariant_holds".to_string(),
            report.no_fallback_invariant_holds().to_string(),
        ),
        (
            "claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
    ];
    append_effectful_operation_admission_matrix_fields(&mut fields);
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    fields
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
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    append_effectful_operation_admission_matrix_fields(&mut fields);
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

pub(crate) fn append_plugin_abi_udf_sandbox_blocker_fields(fields: &mut Vec<(String, String)>) {
    let report = plan_plugin_abi_udf_sandbox_blocker();
    let row_order = report.row_order().join(",");
    let blocker_ids = report.blocker_ids().join(",");
    let required_evidence = report.required_evidence().join("|");
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_schema_version",
            report.schema_version,
        ),
        ("plugin_abi_udf_sandbox_blocker_id", report.blocker_id),
        ("plugin_abi_udf_sandbox_blocker_docs_ref", report.docs_ref),
        (
            "plugin_abi_udf_sandbox_blocker_support_status",
            report.support_status,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_claim_gate_status",
            report.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "plugin_abi_udf_sandbox_blocker_row_count",
        report.rows.len(),
    );
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_row_order",
            row_order.as_str(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_plugin_abi_udf_sandbox_blocker_bool_fields(fields, &report);
    for row in &report.rows {
        append_plugin_abi_udf_sandbox_blocker_row_fields(fields, row);
    }
}

pub(crate) fn append_effectful_operation_admission_matrix_fields(
    fields: &mut Vec<(String, String)>,
) {
    let matrix = EffectfulOperationAdmissionMatrix::current();
    let row_order = matrix.row_order().join(",");
    let blocker_ids = matrix.blocker_ids().join(",");
    let required_evidence = matrix.required_evidence().join("|");
    for (key, value) in [
        (
            "effectful_operation_admission_matrix_schema_version",
            matrix.schema_version,
        ),
        ("effectful_operation_admission_matrix_id", matrix.matrix_id),
        ("effectful_operation_admission_docs_ref", matrix.docs_ref),
        (
            "effectful_operation_admission_support_status_vocabulary",
            "fixture_smoke_supported,metadata_only_supported,blocked",
        ),
        (
            "effectful_operation_admission_claim_gate_status",
            matrix.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "effectful_operation_admission_row_count",
        matrix.rows.len(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_admitted_local_fixture_count",
        matrix.admitted_local_fixture_count(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_metadata_only_count",
        matrix.metadata_only_count(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_blocked_count",
        matrix.blocked_count(),
    );
    for (key, value) in [
        (
            "effectful_operation_admission_row_order",
            row_order.as_str(),
        ),
        (
            "effectful_operation_admission_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "effectful_operation_admission_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_effectful_operation_admission_matrix_bool_fields(fields, &matrix);
    for row in &matrix.rows {
        append_effectful_operation_admission_row_fields(fields, row);
    }
}

fn append_effectful_operation_admission_matrix_bool_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &EffectfulOperationAdmissionMatrix,
) {
    for (key, value) in [
        (
            "effectful_operation_admission_all_external_and_sandboxed_paths_blocked",
            matrix.all_external_and_sandboxed_paths_blocked(),
        ),
        (
            "effectful_operation_admission_credential_resolution_performed",
            matrix.credential_resolution_performed,
        ),
        (
            "effectful_operation_admission_network_probe_performed",
            matrix.network_probe_performed,
        ),
        (
            "effectful_operation_admission_dynamic_loading_performed",
            matrix.dynamic_loading_performed,
        ),
        (
            "effectful_operation_admission_extension_code_executed",
            matrix.extension_code_executed,
        ),
        (
            "effectful_operation_admission_external_effect_executed",
            matrix.external_effect_executed,
        ),
        (
            "effectful_operation_admission_dependency_expansion_allowed",
            matrix.dependency_expansion_allowed,
        ),
        (
            "effectful_operation_admission_fallback_attempted",
            matrix.fallback_attempted,
        ),
        (
            "effectful_operation_admission_external_engine_invoked",
            matrix.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_effectful_operation_admission_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &EffectfulOperationAdmissionRow,
) {
    let prefix = format!("effectful_operation_admission_row_{}", row.row_id);
    for (suffix, value) in [
        ("family", row.family),
        ("operation", row.operation),
        ("support_status", row.support_status),
        ("admission_scope", row.admission_scope),
        ("permission_status", row.permission_status),
        ("effect_status", row.effect_status),
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    for (suffix, value) in [
        ("credential_required", row.credential_required),
        ("network_required", row.network_required),
        ("sandbox_required", row.sandbox_required),
        (
            "local_filesystem_io_allowed",
            row.local_filesystem_io_allowed,
        ),
        ("runtime_fixture_available", row.runtime_fixture_available),
        ("extension_code_executed", row.extension_code_executed),
        ("dynamic_loading_performed", row.dynamic_loading_performed),
        ("external_effect_executed", row.external_effect_executed),
        ("fallback_attempted", row.fallback_attempted),
        ("external_engine_invoked", row.external_engine_invoked),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_plugin_abi_udf_sandbox_blocker_bool_fields(
    fields: &mut Vec<(String, String)>,
    report: &PluginAbiUdfSandboxBlockerReport,
) {
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
            report.all_plugin_runtime_blocked(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_abi_loading_supported",
            report.abi_loading_supported,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_dynamic_loading_performed",
            report.dynamic_loading_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_extension_code_executed",
            report.extension_code_executed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_udf_execution_performed",
            report.udf_execution_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_sandbox_evidence_required",
            report.sandbox_evidence_required,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_sandbox_enforced",
            report.sandbox_enforced,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_permission_policy_enforced",
            report.permission_policy_enforced,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_runtime_execution",
            report.runtime_execution,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_external_effect_executed",
            report.external_effect_executed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_credential_resolution_performed",
            report.credential_resolution_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_network_probe_performed",
            report.network_probe_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_dependency_expansion_allowed",
            report.dependency_expansion_allowed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
            report.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_plugin_abi_udf_sandbox_blocker_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &PluginAbiUdfSandboxBlockerRow,
) {
    let prefix = format!("plugin_abi_udf_sandbox_blocker_row_{}", row.row_id);
    for (suffix, value) in [
        ("plugin_surface", row.plugin_surface),
        ("support_status", row.support_status),
        ("abi_status", row.abi_status),
        ("sandbox_requirement", row.sandbox_requirement),
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
        ("user_visible_surface", row.user_visible_surface),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    append_plugin_abi_udf_sandbox_blocker_row_bool_fields(fields, row, &prefix);
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_plugin_abi_udf_sandbox_blocker_row_bool_fields(
    fields: &mut Vec<(String, String)>,
    row: &PluginAbiUdfSandboxBlockerRow,
    prefix: &str,
) {
    for (suffix, value) in [
        ("dynamic_loading_performed", row.dynamic_loading_performed),
        ("extension_code_executed", row.extension_code_executed),
        ("udf_execution_performed", row.udf_execution_performed),
        ("sandbox_enforced", row.sandbox_enforced),
        ("permission_policy_enforced", row.permission_policy_enforced),
        ("runtime_execution", row.runtime_execution),
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
