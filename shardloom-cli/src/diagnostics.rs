//! Diagnostic and explain/estimate CLI handlers.
//!
//! These handlers expose report-only diagnostics. They do not inspect user
//! datasets, collect runtime profiles, execute plans, invoke external engines,
//! or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, FeatureFootprintReport, ObservabilityPlan, ObservabilitySchemaCoverageReport,
    OutputFormat, RuntimeObservabilityReport, plan_observability_schema_coverage,
};
use shardloom_plan::{EstimateReport, ExplainReport};

use crate::cli_output::emit;

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
    emit(
        "runtime-report",
        format,
        CommandStatus::Success,
        "runtime observability report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        runtime_observability_fields(&report),
    );
    ExitCode::SUCCESS
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
        profile_plan_fields(),
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

fn runtime_observability_fields(report: &RuntimeObservabilityReport) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_runtime_observability_identity_fields(&mut fields, report);
    append_runtime_observability_benchmark_fields(&mut fields, report);
    append_runtime_observability_blocker_fields(&mut fields, report);
    fields
}

fn append_runtime_observability_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RuntimeObservabilityReport,
) {
    push_field(fields, "mode", "runtime_report");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "gar_id", report.gar_id);
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "runtime_report_status", report.status.as_str());
}

fn append_runtime_observability_benchmark_fields(
    fields: &mut Vec<(String, String)>,
    report: &RuntimeObservabilityReport,
) {
    push_bool_field(
        fields,
        "local_benchmark_span_schema_present",
        report.local_benchmark_span_schema_present,
    );
    push_bool_field(
        fields,
        "local_benchmark_stage_timing_schema_present",
        report.local_benchmark_stage_timing_schema_present,
    );
    push_count_field(
        fields,
        "local_benchmark_stage_timing_field_count",
        report.local_benchmark_stage_timing_field_count(),
    );
    push_field(
        fields,
        "local_benchmark_stage_timing_field_order",
        &report.local_benchmark_stage_timing_fields().join(","),
    );
    push_bool_field(
        fields,
        "benchmark_metadata_surface_present",
        report.benchmark_metadata_surface_present,
    );
    push_bool_field(
        fields,
        "local_benchmark_spans_measured",
        report.local_benchmark_spans_measured,
    );
}

fn append_runtime_observability_blocker_fields(
    fields: &mut Vec<(String, String)>,
    report: &RuntimeObservabilityReport,
) {
    push_field(fields, "live_profiling_status", "unsupported");
    push_field(
        fields,
        "distributed_runtime_introspection_status",
        "unsupported",
    );
    push_bool_field(
        fields,
        "live_profiling_supported",
        report.live_profiling_supported,
    );
    push_bool_field(
        fields,
        "distributed_runtime_introspection_supported",
        report.distributed_runtime_introspection_supported,
    );
    push_bool_field(
        fields,
        "profiler_backend_enabled",
        report.profiler_backend_enabled,
    );
    push_bool_field(
        fields,
        "trace_backend_enabled",
        report.trace_backend_enabled,
    );
    push_bool_field(
        fields,
        "exporter_integration_enabled",
        report.exporter_integration_enabled,
    );
    push_bool_field(
        fields,
        "runtime_collection_enabled",
        report.runtime_collection_enabled,
    );
    push_bool_field(
        fields,
        "profile_artifact_generated",
        report.profile_artifact_generated,
    );
    push_bool_field(
        fields,
        "debug_bundle_generated",
        report.debug_bundle_generated,
    );
    push_count_field(
        fields,
        "runtime_blocker_count",
        report.runtime_blocker_count(),
    );
    push_field(
        fields,
        "runtime_blocker_order",
        &report.runtime_blocker_order().join(","),
    );
    push_bool_field(
        fields,
        "no_runtime_collection_or_external_effects",
        report.no_runtime_collection_or_external_effects(),
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_field(fields, "execution", "not_performed");
    push_field(fields, "metrics_collection", "not_performed");
    push_field(fields, "plan_only", "true");
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn profile_plan_fields() -> Vec<(String, String)> {
    let mut fields = observability_style_fields("profile_plan", "not_performed");
    push_field(&mut fields, "gar_id", "GAR-0018-A");
    push_field(&mut fields, "support_status", "unsupported");
    push_field(&mut fields, "claim_gate_status", "not_claim_grade");
    push_field(&mut fields, "live_profiling_status", "unsupported");
    push_field(&mut fields, "profiler_backend_enabled", "false");
    push_field(&mut fields, "runtime_collection_enabled", "false");
    push_field(&mut fields, "profile_artifact_generated", "false");
    push_field(&mut fields, "external_engine_invoked", "false");
    push_field(&mut fields, "fallback_attempted", "false");
    fields
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
        vec![
            ("mode".to_string(), "plan_only".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            (
                "materialization_boundary_reported".to_string(),
                "false".to_string(),
            ),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
        ],
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn feature_footprint_fields(report: &FeatureFootprintReport) -> Vec<(String, String)> {
    let all_gates = report.all_gates();
    vec![
        ("mode".to_string(), "feature_footprint".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("engine_version".to_string(), report.engine_version.clone()),
        (
            "crate_version_count".to_string(),
            report.crate_versions.len().to_string(),
        ),
        (
            "compiled_feature_count".to_string(),
            report.compiled_features.len().to_string(),
        ),
        (
            "enabled_feature_count".to_string(),
            report.enabled_features.len().to_string(),
        ),
        (
            "disabled_feature_count".to_string(),
            report.disabled_features.len().to_string(),
        ),
        (
            "upstream_vortex_dependency_status".to_string(),
            report.upstream_vortex_dependency_status.clone(),
        ),
        (
            "upstream_vortex_version".to_string(),
            report
                .upstream_vortex_version
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        ("all_gate_count".to_string(), all_gates.len().to_string()),
        (
            "vortex_gate_count".to_string(),
            report.vortex_gates.len().to_string(),
        ),
        (
            "encoded_read_gate_count".to_string(),
            report.encoded_read_gates.len().to_string(),
        ),
        (
            "metadata_io_gate_count".to_string(),
            report.metadata_io_gates.len().to_string(),
        ),
        (
            "write_gate_count".to_string(),
            report.write_gates.len().to_string(),
        ),
        (
            "object_store_gate_count".to_string(),
            report.object_store_gates.len().to_string(),
        ),
        (
            "distributed_execution_gate_count".to_string(),
            report.distributed_execution_gates.len().to_string(),
        ),
        (
            "gate_status_order".to_string(),
            all_gates
                .iter()
                .map(|gate| format!("{}:{}", gate.name, gate.status.as_str()))
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "external_baseline_count".to_string(),
            report.external_baseline_availability.len().to_string(),
        ),
        (
            "external_baseline_runtime_fallback_count".to_string(),
            report
                .external_baseline_availability
                .iter()
                .filter(|baseline| baseline.runtime_fallback_allowed)
                .count()
                .to_string(),
        ),
        (
            "fallback_engines_absent".to_string(),
            report.fallback_engines_absent.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

fn observability_schema_coverage_fields(
    report: &ObservabilitySchemaCoverageReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_field(&mut fields, "mode", "observability_schema_coverage");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_count_field(&mut fields, "observability_area_count", report.area_count());
    push_count_field(
        &mut fields,
        "complete_observability_area_count",
        report.complete_area_count(),
    );
    push_count_field(
        &mut fields,
        "missing_observability_area_count",
        report.missing_area_count(),
    );
    push_bool_field(
        &mut fields,
        "schema_coverage_complete",
        report.schema_coverage_complete(),
    );
    push_bool_field(
        &mut fields,
        "local_json_required",
        report.local_json_required,
    );
    push_bool_field(
        &mut fields,
        "exporter_integration_enabled",
        report.exporter_integration_enabled,
    );
    push_bool_field(
        &mut fields,
        "runtime_collection_enabled",
        report.runtime_collection_enabled,
    );
    push_bool_field(
        &mut fields,
        "debug_bundle_schema_present",
        report.debug_bundle_schema_present,
    );
    push_bool_field(&mut fields, "redaction_required", report.redaction_required);
    push_bool_field(
        &mut fields,
        "certificate_link_required",
        report.certificate_link_required,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    for (index, entry) in report.entries.iter().enumerate() {
        let prefix = format!("observability_area_{index}");
        push_field(&mut fields, &format!("{prefix}_name"), entry.area.as_str());
        push_field(
            &mut fields,
            &format!("{prefix}_trace_span_schema"),
            entry.trace_span_schema.as_str(),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_structured_event_schema"),
            entry.structured_event_schema.as_str(),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_profile_schema"),
            entry.profile_schema.as_str(),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_log_schema"),
            entry.log_schema.as_str(),
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_certificate_link_required"),
            entry.certificate_link_required,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_redaction_required"),
            entry.redaction_required,
        );
    }
    fields
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}
