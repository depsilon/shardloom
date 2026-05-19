//! Benchmark planning CLI handlers.
//!
//! These commands are report-only benchmark contract surfaces. They do not run
//! benchmarks, invoke external engines, publish performance claims, or provide
//! fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    BenchmarkClaimEvidenceReport, BenchmarkPlan, CommandStatus, OutputFormat, ShardLoomError,
    SparkDisplacementBenchmarkEvidenceMatrixReport, SparkDisplacementBenchmarkEvidenceRow,
    plan_benchmark_claim_evidence, plan_spark_displacement_benchmark_evidence_matrix,
};

use crate::cli_output::{emit, emit_error};

const VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF: &str =
    "vortex-runtime-utilization-audit://layout_device_managed_boundary.v1";
const VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_ROW_ORDER: &str = "layout_write_boundary,device_execution_boundary,object_store_io_boundary,managed_platform_comparison_boundary";

pub(crate) fn handle_benchmark_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scope = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "benchmark-plan",
            format,
            "benchmark plan failed",
            &ShardLoomError::InvalidOperation(format!(
                "unknown extra benchmark-plan argument: {extra}"
            )),
        );
    }
    let plan = match benchmark_plan_for_scope(scope.as_deref()) {
        Ok(plan) => plan,
        Err(error) => {
            return emit_error("benchmark-plan", format, "benchmark plan failed", &error);
        }
    };
    emit(
        "benchmark-plan",
        format,
        CommandStatus::Success,
        "benchmark plan".to_string(),
        plan.to_human_text(),
        vec![],
        benchmark_plan_fields(&plan),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_benchmark_claim_evidence_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scope = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "benchmark-claim-evidence-plan",
            format,
            "benchmark claim evidence plan failed",
            &ShardLoomError::InvalidOperation(format!(
                "unknown extra benchmark-claim-evidence-plan argument: {extra}"
            )),
        );
    }
    let plan = match benchmark_plan_for_scope(scope.as_deref()) {
        Ok(plan) => plan,
        Err(error) => {
            return emit_error(
                "benchmark-claim-evidence-plan",
                format,
                "benchmark claim evidence plan failed",
                &error,
            );
        }
    };
    let scope_label = scope.unwrap_or_else(|| "foundation".to_string());
    let report = plan_benchmark_claim_evidence(scope_label, &plan);
    emit(
        "benchmark-claim-evidence-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "benchmark claim evidence plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        benchmark_claim_evidence_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn benchmark_plan_fields(plan: &BenchmarkPlan) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_benchmark_plan_overview_fields(&mut fields, plan);
    append_benchmark_plan_scenario_fields(&mut fields, plan);
    append_benchmark_plan_metric_fields(&mut fields, plan);
    append_benchmark_plan_claim_fields(&mut fields, plan);
    fields
}

#[allow(clippy::too_many_lines)]
pub(crate) fn benchmark_claim_evidence_fields(
    report: &BenchmarkClaimEvidenceReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_field(&mut fields, "mode", "benchmark_claim_evidence");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "scope", &report.scope);
    push_field(&mut fields, "claim_evidence_status", report.status.as_str());
    push_field(
        &mut fields,
        "surface_order",
        &BenchmarkClaimEvidenceReport::surface_order().join(","),
    );
    push_count_field(
        &mut fields,
        "surface_count",
        BenchmarkClaimEvidenceReport::surface_order().len(),
    );
    push_count_field(
        &mut fields,
        "planned_surface_count",
        report.planned_surface_count,
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count,
    );
    push_field(
        &mut fields,
        "blocked_surface_order",
        &report.blocked_surface_order.join(","),
    );
    push_count_field(&mut fields, "scenario_count", report.scenario_count);
    push_field(
        &mut fields,
        "scenario_name_order",
        &report.scenario_name_order.join(","),
    );
    push_field(
        &mut fields,
        "workload_class_order",
        &report.workload_class_order.join(","),
    );
    push_count_field(
        &mut fields,
        "required_metric_count",
        report.required_metric_count,
    );
    push_field(
        &mut fields,
        "required_metric_order",
        &report.required_metric_order.join(","),
    );
    push_count_field(
        &mut fields,
        "required_foundation_metric_count",
        report.required_foundation_metric_count,
    );
    push_count_field(
        &mut fields,
        "covered_required_foundation_metric_count",
        report.covered_required_foundation_metric_count,
    );
    push_field(
        &mut fields,
        "missing_required_foundation_metrics",
        &report.missing_required_foundation_metrics.join(","),
    );
    push_count_field(&mut fields, "baseline_count", report.baseline_count);
    push_field(
        &mut fields,
        "baseline_engine_order",
        &report.baseline_engine_order.join(","),
    );
    push_count_field(
        &mut fields,
        "external_baseline_count",
        report.external_baseline_count,
    );
    push_field(
        &mut fields,
        "external_baseline_engine_order",
        &report.external_baseline_engine_order.join(","),
    );
    push_count_field(
        &mut fields,
        "expected_result_count",
        report.expected_result_count,
    );
    push_count_field(&mut fields, "result_count", report.result_count);
    push_count_field(
        &mut fields,
        "missing_result_count",
        report.missing_result_count,
    );
    push_count_field(
        &mut fields,
        "missing_external_result_count",
        report.missing_external_result_count,
    );
    push_count_field(
        &mut fields,
        "missing_metric_count",
        report.missing_metric_count,
    );
    push_field(
        &mut fields,
        "run_manifest_status",
        report.run_manifest_status.as_str(),
    );
    push_bool_field(
        &mut fields,
        "run_manifest_emitted",
        report.run_manifest_emitted,
    );
    push_count_field(
        &mut fields,
        "missing_engine_version_count",
        report.missing_engine_version_count,
    );
    push_count_field(
        &mut fields,
        "dataset_profile_count",
        report.dataset_profile_count,
    );
    push_count_field(
        &mut fields,
        "incomplete_dataset_profile_count",
        report.incomplete_dataset_profile_count,
    );
    push_count_field(
        &mut fields,
        "reproduction_step_count",
        report.reproduction_step_count,
    );
    push_field(&mut fields, "cache_state", report.cache_state.as_str());
    push_field(
        &mut fields,
        "comparison_report_status",
        report.comparison_report_status.as_str(),
    );
    push_bool_field(
        &mut fields,
        "comparison_report_emitted",
        report.comparison_report_emitted,
    );
    push_field(
        &mut fields,
        "claim_gate_status",
        report.claim_gate_status.as_str(),
    );
    push_field(
        &mut fields,
        "claim_gate_correctness_evidence",
        report.correctness_evidence.as_str(),
    );
    push_field(
        &mut fields,
        "claim_gate_benchmark_evidence",
        report.benchmark_evidence.as_str(),
    );
    push_field(
        &mut fields,
        "claim_gate_required_metrics",
        report.required_metrics_evidence.as_str(),
    );
    push_field(
        &mut fields,
        "claim_gate_comparison_report",
        report.comparison_report_evidence.as_str(),
    );
    push_field(
        &mut fields,
        "claim_gate_reproducibility_evidence",
        report.reproducibility_evidence.as_str(),
    );
    push_bool_field(
        &mut fields,
        "claim_grade_source_backed_benchmark_closeout_required",
        report.claim_grade_source_backed_benchmark_closeout_required,
    );
    push_bool_field(
        &mut fields,
        "claim_grade_source_backed_benchmark_closeout_allowed",
        report.claim_grade_source_backed_benchmark_closeout_allowed,
    );
    push_field(
        &mut fields,
        "claim_grade_source_backed_benchmark_closeout_blocker_order",
        &report
            .claim_grade_source_backed_benchmark_closeout_blocker_order
            .join(","),
    );
    push_bool_field(
        &mut fields,
        "measured_benchmark_result_rows_required",
        report.measured_benchmark_result_rows_required,
    );
    push_bool_field(
        &mut fields,
        "measured_benchmark_result_rows_present",
        report.measured_benchmark_result_rows_present,
    );
    push_bool_field(
        &mut fields,
        "reproducibility_manifest_population_required",
        report.reproducibility_manifest_population_required,
    );
    push_bool_field(
        &mut fields,
        "reproducibility_manifest_populated",
        report.reproducibility_manifest_populated,
    );
    push_bool_field(
        &mut fields,
        "approved_comparison_rows_required",
        report.approved_comparison_rows_required,
    );
    push_bool_field(
        &mut fields,
        "approved_comparison_rows_present",
        report.approved_comparison_rows_present,
    );
    push_bool_field(
        &mut fields,
        "benchmark_execution_implemented",
        report.benchmark_execution_implemented,
    );
    push_bool_field(
        &mut fields,
        "benchmark_execution_performed",
        report.benchmark_execution_performed,
    );
    push_bool_field(
        &mut fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(&mut fields, "query_execution", report.query_execution);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        &mut fields,
        "baselines_fallback_free",
        report.baselines_fallback_free,
    );
    push_bool_field(
        &mut fields,
        "performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "superiority_claim_allowed",
        report.superiority_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "best_default_claim_allowed",
        report.best_default_claim_allowed,
    );
    append_spark_displacement_benchmark_evidence_matrix_fields(
        &mut fields,
        &plan_spark_displacement_benchmark_evidence_matrix(),
    );
    append_vortex_boundary_claim_fields(&mut fields);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields.extend(crate::gar_0029_evidence::gar_0029_evidence_expansion_fields());
    fields
}

fn append_spark_displacement_benchmark_evidence_matrix_fields(
    fields: &mut Vec<(String, String)>,
    report: &SparkDisplacementBenchmarkEvidenceMatrixReport,
) {
    append_spark_displacement_matrix_identity_fields(fields, report);
    append_spark_displacement_matrix_claim_fields(fields, report);
    append_spark_displacement_matrix_row_fields(fields, report);
}

fn append_spark_displacement_matrix_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &SparkDisplacementBenchmarkEvidenceMatrixReport,
) {
    push_field(
        fields,
        "spark_displacement_matrix_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "spark_displacement_matrix_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "spark_displacement_matrix_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "spark_displacement_matrix_source_refs",
        report.source_refs,
    );
    push_field(
        fields,
        "spark_displacement_matrix_support_status",
        report.support_status,
    );
    push_field(
        fields,
        "spark_displacement_matrix_claim_gate_status",
        report.claim_gate_status,
    );
}

fn append_spark_displacement_matrix_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &SparkDisplacementBenchmarkEvidenceMatrixReport,
) {
    push_count_field(
        fields,
        "spark_displacement_matrix_row_count",
        report.rows.len(),
    );
    push_field(
        fields,
        "spark_displacement_matrix_row_order",
        &report.row_order().join(","),
    );
    push_field(
        fields,
        "spark_displacement_matrix_missing_evidence",
        &report.missing_evidence().join(" | "),
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_all_rows_not_claim_grade",
        report.all_rows_not_claim_grade(),
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_all_external_lanes_baseline_only",
        report.all_external_lanes_baseline_only(),
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_superiority_claim_allowed",
        report.superiority_claim_allowed,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_spark_displacement_claim_allowed",
        report.spark_displacement_claim_allowed,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_benchmark_rerun_performed",
        report.benchmark_rerun_performed,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "spark_displacement_matrix_side_effect_free",
        report.side_effect_free(),
    );
}

fn append_spark_displacement_matrix_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &SparkDisplacementBenchmarkEvidenceMatrixReport,
) {
    for row in &report.rows {
        let prefix = format!("spark_displacement_matrix_row_{}", row.row_id);
        append_spark_displacement_matrix_row_text_fields(fields, &prefix, row);
        append_spark_displacement_matrix_row_status_fields(fields, &prefix, row);
    }
}

fn append_spark_displacement_matrix_row_text_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &SparkDisplacementBenchmarkEvidenceRow,
) {
    for (suffix, value) in [
        ("workload_family", row.workload_family),
        ("workload_ref", row.workload_ref),
        ("shardloom_lane", row.shardloom_lane),
        ("baseline_oracle_lanes", row.baseline_oracle_lanes),
        ("correctness_ref", row.correctness_ref),
        ("timing_ref", row.timing_ref),
        ("environment_ref", row.environment_ref),
        ("execution_mode_ref", row.execution_mode_ref),
        ("policy_ref", row.policy_ref),
        ("claim_gate_status", row.claim_gate_status),
        ("missing_evidence", row.missing_evidence),
        ("claim_boundary", row.claim_boundary),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

fn append_spark_displacement_matrix_row_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &SparkDisplacementBenchmarkEvidenceRow,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_external_baseline_only"),
        row.external_baseline_only,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        row.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        row.external_engine_invoked,
    );
}

fn append_benchmark_plan_overview_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    let claim_gate = plan.claim_gate();
    push_field(fields, "mode", "benchmark_plan");
    push_field(fields, "status", "planned");
    push_bool_field(
        fields,
        "benchmark_execution_implemented",
        plan.benchmark_execution_implemented(),
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        claim_gate.can_publish_performance_claim(),
    );
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_field(fields, "external_baselines", "comparison_only");
}

fn append_benchmark_plan_scenario_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    push_count_field(fields, "scenario_count", plan.scenario_count());
    push_field(
        fields,
        "scenario_name_order",
        &plan.scenario_name_order().join(","),
    );
    push_field(
        fields,
        "workload_class_order",
        &plan.workload_class_order().join(","),
    );
    push_field(
        fields,
        "correctness_validation_order",
        &plan.correctness_validation_order().join(","),
    );
    push_count_field(
        fields,
        "scenario_with_correctness_validation_count",
        plan.scenario_with_correctness_validation_count(),
    );
    push_count_field(
        fields,
        "scenario_with_required_metrics_count",
        plan.scenario_with_required_metrics_count(),
    );
    push_count_field(
        fields,
        "scenario_with_baselines_count",
        plan.scenario_with_baselines_count(),
    );
}

fn append_benchmark_plan_metric_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    push_count_field(
        fields,
        "required_metric_count",
        plan.required_metrics().len(),
    );
    push_field(
        fields,
        "required_metric_order",
        &plan.required_metric_order().join(","),
    );
    push_count_field(
        fields,
        "required_foundation_metric_count",
        BenchmarkPlan::required_foundation_metrics().len(),
    );
    push_count_field(
        fields,
        "covered_required_foundation_metric_count",
        plan.covered_required_foundation_metric_count(),
    );
    push_field(
        fields,
        "missing_required_foundation_metrics",
        &plan.missing_required_foundation_metrics().join(","),
    );
    push_bool_field(
        fields,
        "required_foundation_metrics_covered",
        plan.required_foundation_metrics_covered(),
    );
    push_bool_field(
        fields,
        "runtime_metrics_covered",
        plan.runtime_metrics_covered(),
    );
    push_bool_field(
        fields,
        "peak_memory_metric_covered",
        plan.peak_memory_metric_covered(),
    );
    push_bool_field(
        fields,
        "bytes_read_written_metrics_covered",
        plan.bytes_read_written_metrics_covered(),
    );
    push_bool_field(
        fields,
        "startup_latency_metric_covered",
        plan.startup_latency_metric_covered(),
    );
    push_bool_field(
        fields,
        "query_runtime_metric_covered",
        plan.query_runtime_metric_covered(),
    );
    push_bool_field(
        fields,
        "write_commit_latency_metric_covered",
        plan.write_commit_latency_metric_covered(),
    );
    push_bool_field(
        fields,
        "spill_metrics_covered",
        plan.spill_metrics_covered(),
    );
    push_bool_field(
        fields,
        "object_store_request_metric_covered",
        plan.object_store_request_metric_covered(),
    );
    push_bool_field(
        fields,
        "materialization_metrics_covered",
        plan.materialization_metrics_covered(),
    );
}

fn append_benchmark_plan_claim_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    let claim_gate = plan.claim_gate();
    push_field(
        fields,
        "baseline_engine_order",
        &plan.baseline_engine_order().join(","),
    );
    push_field(
        fields,
        "external_baseline_engine_order",
        &plan.external_baseline_engine_order().join(","),
    );
    push_count_field(
        fields,
        "external_baseline_count",
        plan.external_baseline_count(),
    );
    push_count_field(
        fields,
        "expected_result_count",
        plan.expected_result_count(),
    );
    push_field(fields, "claim_gate_status", claim_gate.status.as_str());
    push_field(
        fields,
        "claim_gate_correctness_evidence",
        claim_gate.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "claim_gate_benchmark_evidence",
        claim_gate.benchmark_evidence.as_str(),
    );
    push_field(
        fields,
        "claim_gate_required_metrics",
        claim_gate.required_metrics.as_str(),
    );
    push_field(
        fields,
        "claim_gate_comparison_report",
        claim_gate.comparison_report.as_str(),
    );
    push_field(
        fields,
        "claim_gate_reproducibility_evidence",
        claim_gate.reproducibility_evidence.as_str(),
    );
    push_field(fields, "claim_gate_fallback", claim_gate.fallback.as_str());
    push_bool_field(
        fields,
        "baselines_fallback_free",
        plan.baselines_are_fallback_free(),
    );
    append_vortex_boundary_claim_fields(fields);
}

fn append_vortex_boundary_claim_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "vortex_layout_device_managed_boundary_ref",
        VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF,
    );
    push_field(
        fields,
        "vortex_layout_device_managed_boundary_row_order",
        VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_ROW_ORDER,
    );
    push_field(
        fields,
        "vortex_layout_device_managed_boundary_claim_gate_status",
        "not_claim_grade",
    );
    push_bool_field(fields, "vortex_managed_platform_rows_comparison_only", true);
    push_bool_field(
        fields,
        "vortex_device_object_store_claims_blocked_without_evidence",
        true,
    );
    push_bool_field(
        fields,
        "vortex_layout_write_claim_blocked_without_evidence",
        true,
    );
    push_bool_field(fields, "vortex_boundary_external_engine_invoked", false);
    push_bool_field(fields, "vortex_boundary_fallback_attempted", false);
}

pub(crate) fn benchmark_plan_for_scope(
    scope: Option<&str>,
) -> shardloom_core::Result<BenchmarkPlan> {
    match scope {
        None | Some("foundation") => Ok(BenchmarkPlan::default_foundation_plan()),
        Some("traditional-analytics" | "traditional_analytics") => {
            Ok(BenchmarkPlan::traditional_analytics_plan())
        }
        Some(other) => Err(ShardLoomError::InvalidOperation(format!(
            "unknown benchmark plan scope: {other}"
        ))),
    }
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
