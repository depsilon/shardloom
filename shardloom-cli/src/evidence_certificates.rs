//! Evidence, correctness, certificate, and Native I/O planning handlers.
//!
//! These commands emit report-only evidence planning surfaces. They do not run
//! correctness harnesses, read data, emit runtime certificates from execution,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    ApproxSketchFunctionGateReport, CommandStatus, CorrectnessDifferentialHarnessReport,
    CorrectnessValidationPlan, ExecutionCertificateEvidenceSurfaceReport,
    ExecutionEvidenceArtifactKind, NativeIoEnvelopeReport, OutputFormat,
    RfcCoverageFollowThroughReport, UniversalHarnessReport, UserCapabilityPromotionGateReport,
    WorldClassSufficiencyDimensionKind, WorldClassSufficiencyReport,
    plan_approx_sketch_function_gate, plan_correctness_differential_harness,
    plan_execution_certificate_evidence_surface, plan_native_io_envelope,
    plan_rfc_coverage_followthrough, plan_universal_harness, plan_user_capability_promotion_gate,
    plan_world_class_sufficiency,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
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

pub(crate) fn handle_rfc_coverage_followthrough_plan(format: OutputFormat) -> ExitCode {
    let report = plan_rfc_coverage_followthrough();
    emit_report(
        "rfc-coverage-followthrough-plan",
        "RFC coverage follow-through plan",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rfc_coverage_followthrough_fields(&report),
        format,
    )
}

pub(crate) fn handle_world_class_sufficiency_plan(format: OutputFormat) -> ExitCode {
    let report = plan_world_class_sufficiency();
    emit_report(
        "world-class-sufficiency-plan",
        "world-class sufficiency plan",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        world_class_sufficiency_fields(&report),
        format,
    )
}

pub(crate) fn handle_cg20_user_capability_gate(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "cg20-user-capability-gate",
            format,
            "CG-20 user capability gate failed",
            &cli_unknown_arg_error("cg20-user-capability-gate", &extra),
        );
    }
    let report = plan_user_capability_promotion_gate();
    emit_report(
        "cg20-user-capability-gate",
        "CG-20 user capability promotion gate",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        user_capability_promotion_gate_fields(&report),
        format,
    )
}

pub(crate) fn handle_cg20_approx_sketch_gate(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "cg20-approx-sketch-gate",
            format,
            "CG-20 approximate sketch gate failed",
            &cli_unknown_arg_error("cg20-approx-sketch-gate", &extra),
        );
    }
    let report = plan_approx_sketch_function_gate();
    emit_report(
        "cg20-approx-sketch-gate",
        "CG-20 approximate sketch function gate",
        report.has_errors(),
        report.to_human_text(),
        report.diagnostics.clone(),
        approx_sketch_function_gate_fields(&report),
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
fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}

#[allow(clippy::too_many_lines)]
pub(crate) fn correctness_plan_fields(plan: &CorrectnessValidationPlan) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), "correctness_plan".to_string()),
        ("status".to_string(), plan.status.as_str().to_string()),
        (
            "fallback_execution_allowed".to_string(),
            plan.fallback_execution_allowed().to_string(),
        ),
        (
            "external_baselines".to_string(),
            "test_oracles_only".to_string(),
        ),
        (
            "fixture_count".to_string(),
            plan.fixture_count().to_string(),
        ),
        (
            "fixture_id_order".to_string(),
            plan.fixture_id_order().join(","),
        ),
        (
            "semantic_area_order".to_string(),
            plan.semantic_area_order().join(","),
        ),
        (
            "edge_case_order".to_string(),
            plan.edge_case_order().join(","),
        ),
        (
            "reference_role_order".to_string(),
            plan.reference_role_order().join(","),
        ),
        (
            "fixtures_with_source_ref_count".to_string(),
            plan.fixtures_with_source_ref_count().to_string(),
        ),
        (
            "source_backed_edge_fixture_count".to_string(),
            plan.source_backed_edge_fixture_count().to_string(),
        ),
        (
            "source_backed_edge_fixture_id_order".to_string(),
            plan.source_backed_edge_fixture_id_order().join(","),
        ),
        (
            "golden_fixture_count".to_string(),
            plan.golden_fixture_count().to_string(),
        ),
        (
            "reference_artifact_count".to_string(),
            plan.reference_artifact_count().to_string(),
        ),
        (
            "decoded_reference_output_count".to_string(),
            plan.decoded_reference_output_count().to_string(),
        ),
        (
            "decoded_reference_artifact_id_order".to_string(),
            plan.decoded_reference_artifact_id_order().join(","),
        ),
        (
            "decoded_reference_output_coverage_complete".to_string(),
            plan.decoded_reference_output_coverage_complete()
                .to_string(),
        ),
        (
            "executable_expected_output_count".to_string(),
            plan.executable_expected_output_count().to_string(),
        ),
        (
            "not_yet_defined_fixture_count".to_string(),
            plan.not_yet_defined_fixture_count().to_string(),
        ),
        (
            "deferred_fixture_family_count".to_string(),
            plan.deferred_fixture_family_count().to_string(),
        ),
        (
            "deferred_fixture_family_id_order".to_string(),
            plan.deferred_fixture_family_id_order().join(","),
        ),
        (
            "deferred_fixture_family_artifact_count".to_string(),
            plan.deferred_fixture_family_artifact_count().to_string(),
        ),
        (
            "deferred_fixture_family_artifact_populated_count".to_string(),
            plan.deferred_fixture_family_artifact_populated_count()
                .to_string(),
        ),
        (
            "deferred_fixture_family_artifacts_populated".to_string(),
            plan.deferred_fixture_family_artifacts_populated()
                .to_string(),
        ),
        (
            "deferred_fixture_family_artifact_id_order".to_string(),
            plan.deferred_fixture_family_artifact_id_order().join(","),
        ),
        (
            "deferred_fixture_family_artifact_status_order".to_string(),
            plan.deferred_fixture_family_artifact_status_order()
                .join(","),
        ),
        (
            "deferred_fixture_family_artifacts_test_only".to_string(),
            plan.deferred_fixture_family_artifacts_are_test_only()
                .to_string(),
        ),
        (
            "diagnostic_expected_output_count".to_string(),
            plan.diagnostic_expected_output_count().to_string(),
        ),
        (
            "unsupported_expected_output_count".to_string(),
            plan.unsupported_expected_output_count().to_string(),
        ),
        (
            "baseline_count".to_string(),
            plan.baseline_count().to_string(),
        ),
        (
            "external_oracle_result_artifact_count".to_string(),
            plan.external_oracle_result_artifact_count().to_string(),
        ),
        (
            "external_oracle_result_populated_count".to_string(),
            plan.external_oracle_result_populated_count().to_string(),
        ),
        (
            "external_oracle_results_populated".to_string(),
            plan.external_oracle_results_populated().to_string(),
        ),
        (
            "external_oracle_result_artifact_id_order".to_string(),
            plan.external_oracle_result_artifact_id_order().join(","),
        ),
        (
            "external_oracle_result_artifact_status_order".to_string(),
            plan.external_oracle_result_artifact_status_order()
                .join(","),
        ),
        (
            "external_oracle_artifacts_test_only".to_string(),
            plan.external_oracle_artifacts_are_test_only().to_string(),
        ),
        (
            "covered_required_foundation_edge_case_count".to_string(),
            plan.covered_required_foundation_edge_case_count()
                .to_string(),
        ),
        (
            "required_foundation_edge_case_count".to_string(),
            CorrectnessValidationPlan::required_foundation_edge_cases()
                .len()
                .to_string(),
        ),
        (
            "missing_required_foundation_edge_cases".to_string(),
            plan.missing_required_foundation_edge_cases().join(","),
        ),
        (
            "required_foundation_edge_cases_covered".to_string(),
            plan.required_foundation_edge_cases_covered().to_string(),
        ),
        (
            "reference_roles_test_only".to_string(),
            plan.reference_roles_are_test_only().to_string(),
        ),
        (
            "baselines_fallback_free".to_string(),
            plan.baselines_are_fallback_free().to_string(),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
pub(crate) fn correctness_harness_fields(
    report: &CorrectnessDifferentialHarnessReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_field(&mut fields, "mode", "correctness_differential_harness");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "plan_name", &report.plan_name);
    push_field(&mut fields, "plan_mode", report.plan_mode.as_str());
    push_field(&mut fields, "harness_status", report.status.as_str());
    push_field(
        &mut fields,
        "surface_order",
        &CorrectnessDifferentialHarnessReport::surface_order().join(","),
    );
    push_count_field(
        &mut fields,
        "surface_count",
        CorrectnessDifferentialHarnessReport::surface_order().len(),
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
    push_field(
        &mut fields,
        "required_validation_mode_order",
        &CorrectnessDifferentialHarnessReport::required_validation_mode_order().join(","),
    );
    push_field(
        &mut fields,
        "missing_validation_mode_order",
        &report.missing_validation_mode_order().join(","),
    );
    push_count_field(&mut fields, "fixture_count", report.fixture_count);
    push_count_field(
        &mut fields,
        "fixtures_with_source_ref_count",
        report.fixtures_with_source_ref_count,
    );
    push_count_field(
        &mut fields,
        "source_backed_edge_fixture_count",
        report.source_backed_edge_fixture_count,
    );
    push_field(
        &mut fields,
        "source_backed_edge_fixture_id_order",
        &report.source_backed_edge_fixture_id_order.join(","),
    );
    push_count_field(
        &mut fields,
        "golden_fixture_count",
        report.golden_fixture_count,
    );
    push_count_field(
        &mut fields,
        "reference_artifact_count",
        report.reference_artifact_count,
    );
    push_count_field(
        &mut fields,
        "decoded_reference_output_count",
        report.decoded_reference_output_count,
    );
    push_field(
        &mut fields,
        "decoded_reference_artifact_id_order",
        &report.decoded_reference_artifact_id_order.join(","),
    );
    push_bool_field(
        &mut fields,
        "decoded_reference_output_coverage_complete",
        report.decoded_reference_output_coverage_complete,
    );
    push_count_field(
        &mut fields,
        "executable_expected_output_count",
        report.executable_expected_output_count,
    );
    push_count_field(
        &mut fields,
        "not_yet_defined_fixture_count",
        report.not_yet_defined_fixture_count,
    );
    push_count_field(
        &mut fields,
        "deferred_fixture_family_count",
        report.deferred_fixture_family_count,
    );
    push_field(
        &mut fields,
        "deferred_fixture_family_id_order",
        &report.deferred_fixture_family_id_order.join(","),
    );
    push_count_field(
        &mut fields,
        "deferred_fixture_family_artifact_count",
        report.deferred_fixture_family_artifact_count,
    );
    push_count_field(
        &mut fields,
        "deferred_fixture_family_artifact_populated_count",
        report.deferred_fixture_family_artifact_populated_count,
    );
    push_bool_field(
        &mut fields,
        "deferred_fixture_family_artifacts_populated",
        report.deferred_fixture_family_artifacts_populated,
    );
    push_field(
        &mut fields,
        "deferred_fixture_family_artifact_id_order",
        &report.deferred_fixture_family_artifact_id_order.join(","),
    );
    push_field(
        &mut fields,
        "deferred_fixture_family_artifact_status_order",
        &report
            .deferred_fixture_family_artifact_status_order
            .join(","),
    );
    push_bool_field(
        &mut fields,
        "deferred_fixture_family_artifacts_test_only",
        report.deferred_fixture_family_artifacts_test_only,
    );
    push_count_field(
        &mut fields,
        "unsupported_diagnostic_fixture_count",
        report.unsupported_diagnostic_fixture_count,
    );
    push_count_field(
        &mut fields,
        "required_edge_case_count",
        report.required_edge_case_count,
    );
    push_count_field(
        &mut fields,
        "covered_required_edge_case_count",
        report.covered_required_edge_case_count,
    );
    push_field(
        &mut fields,
        "missing_required_edge_cases",
        &report.missing_required_edge_cases.join(","),
    );
    push_count_field(&mut fields, "baseline_count", report.baseline_count);
    push_field(
        &mut fields,
        "baseline_engine_order",
        &report.baseline_engine_order.join(","),
    );
    push_count_field(
        &mut fields,
        "external_oracle_result_artifact_count",
        report.external_oracle_result_artifact_count,
    );
    push_count_field(
        &mut fields,
        "external_oracle_result_populated_count",
        report.external_oracle_result_populated_count,
    );
    push_bool_field(
        &mut fields,
        "external_oracle_results_populated",
        report.external_oracle_results_populated,
    );
    push_field(
        &mut fields,
        "external_oracle_result_artifact_id_order",
        &report.external_oracle_result_artifact_id_order.join(","),
    );
    push_field(
        &mut fields,
        "external_oracle_result_artifact_status_order",
        &report
            .external_oracle_result_artifact_status_order
            .join(","),
    );
    push_bool_field(
        &mut fields,
        "external_oracle_artifacts_test_only",
        report.external_oracle_artifacts_test_only,
    );
    push_field(
        &mut fields,
        "reference_role_order",
        &report.reference_role_order.join(","),
    );
    push_count_field(
        &mut fields,
        "generated_property_fixture_count",
        report.generated_property_fixture_count,
    );
    push_count_field(&mut fields, "fuzz_seed_count", report.fuzz_seed_count);
    push_bool_field(
        &mut fields,
        "property_fuzz_execution_performed",
        report.property_fuzz_execution_performed,
    );
    push_field(
        &mut fields,
        "benchmark_claim_blocker_order",
        &report.benchmark_claim_blocker_order.join(","),
    );
    push_bool_field(
        &mut fields,
        "claim_grade_correctness_closeout_required",
        report.claim_grade_correctness_closeout_required,
    );
    push_bool_field(
        &mut fields,
        "claim_grade_correctness_closeout_allowed",
        report.claim_grade_correctness_closeout_allowed,
    );
    push_field(
        &mut fields,
        "claim_grade_correctness_closeout_blocker_order",
        &report
            .claim_grade_correctness_closeout_blocker_order
            .join(","),
    );
    push_bool_field(
        &mut fields,
        "external_oracle_execution_required",
        report.external_oracle_execution_required,
    );
    push_bool_field(
        &mut fields,
        "deferred_fixture_family_artifact_population_required",
        report.deferred_fixture_family_artifact_population_required,
    );
    push_bool_field(
        &mut fields,
        "decoded_reference_outputs_required",
        report.decoded_reference_outputs_required,
    );
    push_bool_field(
        &mut fields,
        "differential_oracles_required",
        report.differential_oracles_required,
    );
    push_bool_field(
        &mut fields,
        "property_fuzzing_required",
        report.property_fuzzing_required,
    );
    push_bool_field(
        &mut fields,
        "benchmark_claim_gate_required",
        report.benchmark_claim_gate_required,
    );
    push_bool_field(
        &mut fields,
        "reference_roles_test_only",
        report.reference_roles_test_only,
    );
    push_bool_field(
        &mut fields,
        "baselines_fallback_free",
        report.baselines_fallback_free,
    );
    push_bool_field(
        &mut fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "benchmark_claims_blocked_by_correctness",
        report.benchmark_claims_blocked_by_correctness,
    );
    push_bool_field(&mut fields, "query_execution", report.query_execution);
    push_bool_field(
        &mut fields,
        "decoded_reference_execution_performed",
        report.decoded_reference_execution_performed,
    );
    push_bool_field(
        &mut fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

pub(crate) fn execution_certificate_surface_fields(
    report: &ExecutionCertificateEvidenceSurfaceReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_execution_certificate_surface_identity_fields(&mut fields, report);
    append_execution_certificate_surface_artifact_fields(&mut fields, report);
    append_execution_certificate_surface_requirement_fields(&mut fields, report);
    append_execution_certificate_surface_side_effect_fields(&mut fields, report);
    fields
}

fn append_execution_certificate_surface_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_field(fields, "mode", "execution_certificate_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "certificate_surface_status", report.status.as_str());
    push_field(
        fields,
        "certificate_schema_version",
        report.certificate_schema_version,
    );
}

fn append_execution_certificate_surface_artifact_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_count_field(fields, "artifact_count", report.artifact_count());
    push_count_field(
        fields,
        "required_artifact_count",
        report.required_artifact_count(),
    );
    push_count_field(fields, "hash_required_count", report.hash_required_count());
    push_count_field(
        fields,
        "machine_readable_required_count",
        report.machine_readable_required_count(),
    );
    push_count_field(
        fields,
        "plan_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::Plan),
    );
    push_count_field(
        fields,
        "input_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::InputSnapshot),
    );
    push_count_field(
        fields,
        "output_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::OutputPayload),
    );
    push_count_field(
        fields,
        "segment_trace_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::SegmentTrace),
    );
    push_count_field(
        fields,
        "side_effect_manifest_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::SideEffectManifest),
    );
    push_count_field(
        fields,
        "reproducibility_metadata_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::ReproducibilityMetadata),
    );
    push_field(fields, "artifact_order", &report.artifact_order());
}

fn append_execution_certificate_surface_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_bool_field(fields, "plan_hash_required", report.plan_hash_required);
    push_bool_field(
        fields,
        "input_snapshot_hash_required",
        report.input_snapshot_hash_required,
    );
    push_bool_field(fields, "output_hash_required", report.output_hash_required);
    push_bool_field(
        fields,
        "selected_segment_trace_required",
        report.selected_segment_trace_required,
    );
    push_bool_field(
        fields,
        "skipped_segment_trace_required",
        report.skipped_segment_trace_required,
    );
    push_bool_field(
        fields,
        "side_effect_manifest_required",
        report.side_effect_manifest_required,
    );
    push_bool_field(
        fields,
        "reproducibility_metadata_required",
        report.reproducibility_metadata_required,
    );
    push_bool_field(
        fields,
        "correctness_fixture_required",
        report.correctness_fixture_required,
    );
    push_bool_field(
        fields,
        "machine_readable_certificate_surface",
        report.machine_readable_certificate_surface,
    );
    push_bool_field(
        fields,
        "deterministic_field_order_required",
        report.deterministic_field_order_required,
    );
}

fn append_execution_certificate_surface_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_bool_field(
        fields,
        "certificate_evaluation_performed",
        report.certificate_evaluation_performed,
    );
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

pub(crate) fn universal_harness_fields(report: &UniversalHarnessReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_universal_harness_identity_fields(&mut fields, report);
    append_universal_harness_requirement_fields(&mut fields, report);
    append_universal_harness_side_effect_fields(&mut fields, report);
    fields
}

fn append_universal_harness_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_field(fields, "mode", "universal_harness_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "universal_harness_status", report.status.as_str());
    push_count_field(fields, "surface_count", report.surface_count());
    push_count_field(
        fields,
        "external_baseline_count",
        report.external_baseline_count(),
    );
    push_count_field(
        fields,
        "harness_environment_count",
        report.harness_environment_count(),
    );
    push_field(
        fields,
        "runner_contract_field_order",
        &report.runner_contract_field_order(),
    );
    push_field(fields, "surface_kind_order", &report.surface_kind_order());
    push_field(
        fields,
        "harness_environment_kind_order",
        &report.harness_environment_kind_order(),
    );
    push_field(
        fields,
        "baseline_engine_order",
        &report.baseline_engine_order(),
    );
}

fn append_universal_harness_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_bool_field(
        fields,
        "output_envelope_required",
        report.output_envelope_required,
    );
    push_bool_field(
        fields,
        "stable_command_schema_required",
        report.stable_command_schema_required,
    );
    push_bool_field(fields, "exit_code_required", report.exit_code_required);
    push_bool_field(fields, "diagnostics_required", report.diagnostics_required);
    push_bool_field(
        fields,
        "side_effect_manifest_required",
        report.side_effect_manifest_required,
    );
    push_bool_field(
        fields,
        "output_artifacts_required",
        report.output_artifacts_required,
    );
    push_bool_field(fields, "metrics_required", report.metrics_required);
    push_bool_field(
        fields,
        "comparison_dataset_required",
        report.comparison_dataset_required,
    );
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(fields, "foundry_required", report.foundry_required);
    push_bool_field(
        fields,
        "foundry_optional_example",
        report.foundry_optional_example,
    );
    push_bool_field(
        fields,
        "local_harness_required",
        report.local_harness_required,
    );
    push_bool_field(fields, "ci_harness_required", report.ci_harness_required);
    push_bool_field(
        fields,
        "container_harness_required",
        report.container_harness_required,
    );
    push_bool_field(
        fields,
        "foundry_optional_harness_required",
        report.foundry_optional_harness_required,
    );
    push_bool_field(
        fields,
        "optional_benchmark_environment_required",
        report.optional_benchmark_environment_required,
    );
    push_bool_field(
        fields,
        "external_engines_as_runtime_dependencies_allowed",
        report.external_engines_as_runtime_dependencies_allowed,
    );
    push_bool_field(
        fields,
        "required_harness_environments_present",
        report.has_required_harness_environments(),
    );
    push_bool_field(
        fields,
        "baselines_comparison_only_runtime_dependency_free",
        report.baselines_are_comparison_only_and_runtime_dependency_free(),
    );
}

fn append_universal_harness_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_bool_field(
        fields,
        "package_import_performed",
        report.package_import_performed,
    );
    push_bool_field(fields, "deployment_performed", report.deployment_performed);
    push_bool_field(
        fields,
        "external_baseline_execution",
        report.external_baseline_execution,
    );
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(fields, "network_probe", report.network_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "read_io", report.read_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "external_publish", report.external_publish);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

pub(crate) fn rfc_coverage_followthrough_fields(
    report: &RfcCoverageFollowThroughReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_rfc_coverage_followthrough_identity_fields(&mut fields, report);
    append_rfc_coverage_followthrough_requirement_fields(&mut fields, report);
    append_rfc_coverage_followthrough_side_effect_fields(&mut fields, report);
    fields
}

fn append_rfc_coverage_followthrough_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RfcCoverageFollowThroughReport,
) {
    push_field(fields, "mode", "rfc_coverage_followthrough_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "rfc_coverage_status", report.status.as_str());
    push_count_field(fields, "rfc_coverage_entry_count", report.entry_count());
    push_field(fields, "rfc_order", &report.rfc_order());
    push_field(fields, "area_order", &report.area_order());
    push_field(fields, "rfc0010_status", report.status_for_rfc("rfc_0010"));
    push_field(fields, "rfc0011_status", report.status_for_rfc("rfc_0011"));
    push_field(fields, "rfc0020_status", report.status_for_rfc("rfc_0020"));
    push_field(fields, "rfc0022_status", report.status_for_rfc("rfc_0022"));
    push_field(fields, "rfc0023_status", report.status_for_rfc("rfc_0023"));
}

fn append_rfc_coverage_followthrough_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &RfcCoverageFollowThroughReport,
) {
    push_bool_field(
        fields,
        "deterministic_machine_readable_required",
        report.deterministic_machine_readable_required,
    );
    push_bool_field(
        fields,
        "human_readable_required",
        report.human_readable_required,
    );
    push_bool_field(
        fields,
        "side_effect_explicit_required",
        report.side_effect_explicit_required,
    );
    push_bool_field(
        fields,
        "import_discovery_dry_run_safety_required",
        report.import_discovery_dry_run_safety_required,
    );
    push_bool_field(
        fields,
        "typed_effect_materialization_metadata_required",
        report.typed_effect_materialization_metadata_required,
    );
    push_bool_field(
        fields,
        "effectful_extensions_blocked",
        report.effectful_extensions_blocked,
    );
    push_bool_field(
        fields,
        "metadata_discovery_separate_from_read_write_commit",
        report.metadata_discovery_separate_from_read_write_commit,
    );
    push_bool_field(
        fields,
        "table_write_commit_claims_blocked",
        report.table_write_commit_claims_blocked,
    );
    push_bool_field(
        fields,
        "imported_plan_execution_blocked",
        report.imported_plan_execution_blocked,
    );
    push_bool_field(
        fields,
        "substrait_bridge_fallback_blocked",
        report.substrait_bridge_fallback_blocked,
    );
    push_bool_field(
        fields,
        "extension_manifest_inspection_only",
        report.extension_manifest_inspection_only,
    );
    push_bool_field(
        fields,
        "extension_code_execution_blocked",
        report.extension_code_execution_blocked,
    );
    push_bool_field(
        fields,
        "all_entries_runtime_expansion_blocked",
        report.all_entries_runtime_expansion_blocked(),
    );
    push_bool_field(
        fields,
        "all_entries_dependency_expansion_blocked",
        report.all_entries_dependency_expansion_blocked(),
    );
    push_bool_field(
        fields,
        "all_entries_external_effects_blocked",
        report.all_entries_external_effects_blocked(),
    );
}

fn append_rfc_coverage_followthrough_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RfcCoverageFollowThroughReport,
) {
    push_bool_field(
        fields,
        "runtime_expansion_performed",
        report.runtime_expansion_performed,
    );
    push_bool_field(
        fields,
        "parser_expansion_performed",
        report.parser_expansion_performed,
    );
    push_bool_field(
        fields,
        "adapter_expansion_performed",
        report.adapter_expansion_performed,
    );
    push_bool_field(
        fields,
        "dependency_expansion_performed",
        report.dependency_expansion_performed,
    );
    push_bool_field(
        fields,
        "external_effect_performed",
        report.external_effect_performed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

pub(crate) fn native_io_envelope_fields(report: &NativeIoEnvelopeReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_native_io_envelope_identity_fields(&mut fields, report);
    append_native_io_envelope_requirement_fields(&mut fields, report);
    append_native_io_envelope_side_effect_fields(&mut fields, report);
    fields
}

fn world_class_sufficiency_fields(report: &WorldClassSufficiencyReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_world_class_sufficiency_identity_fields(&mut fields, report);
    append_world_class_sufficiency_surface_status_fields(&mut fields, report);
    append_world_class_sufficiency_evidence_status_fields(&mut fields, report);
    append_world_class_sufficiency_metric_fields(&mut fields, report);
    append_world_class_sufficiency_side_effect_fields(&mut fields, report);
    fields
}

fn append_world_class_sufficiency_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_field(fields, "mode", "world_class_sufficiency_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(
        fields,
        "workload_constitution_ref",
        &report.workload_constitution_ref,
    );
    push_field(fields, "claim_level", report.claim_level.as_str());
    push_field(
        fields,
        "publication_decision",
        report.publication_decision.as_str(),
    );
    push_count_field(fields, "dimension_count", report.dimension_count());
    push_count_field(
        fields,
        "required_dimension_count",
        report.required_dimension_count(),
    );
    push_count_field(
        fields,
        "evidence_insufficient_dimension_count",
        report.evidence_insufficient_dimension_count(),
    );
    push_field(
        fields,
        "dimension_kind_order",
        &report.dimension_kind_order(),
    );
}

fn append_world_class_sufficiency_surface_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_world_class_sufficiency_status_field(
        fields,
        "sql_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::SqlSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "operator_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::OperatorSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "function_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::FunctionSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "adapter_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::AdapterSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "python_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::PythonSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "data_etl_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::DataEtlSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "unstructured_media_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::UnstructuredMedia,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "universal_adapter_catalog_status",
        report,
        WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog,
    );
    push_field(
        fields,
        "python_package_status",
        "source_tree_wheel_sdist_ready",
    );
    push_field(
        fields,
        "fresh_environment_smoke_status",
        "local_smoke_ready",
    );
    push_field(fields, "conda_package_split_status", "recipe_scaffolded");
    push_field(fields, "conda_cli_package_status", "recipe_scaffolded");
    push_field(fields, "conda_python_package_status", "recipe_scaffolded");
    push_field(fields, "conda_metapackage_status", "recipe_scaffolded");
    push_field(fields, "conda_recipe_root", "packaging/conda");
    push_field(fields, "benchmark_extras_status", "optional_planned");
}

fn append_world_class_sufficiency_evidence_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_world_class_sufficiency_status_field(
        fields,
        "correctness_evidence_status",
        report,
        WorldClassSufficiencyDimensionKind::CorrectnessEvidence,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "benchmark_evidence_status",
        report,
        WorldClassSufficiencyDimensionKind::BenchmarkEvidence,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "native_io_certificate_coverage",
        report,
        WorldClassSufficiencyDimensionKind::NativeIoCertificateCoverage,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "execution_certificate_coverage",
        report,
        WorldClassSufficiencyDimensionKind::ExecutionCertificateCoverage,
    );
    push_field(
        fields,
        "performance_regression_budget_status",
        report.performance_regression_budget_status.as_str(),
    );
}

fn append_world_class_sufficiency_metric_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_field(
        fields,
        "unsupported_rate",
        &report
            .unsupported_rate
            .clone()
            .unwrap_or_else(|| "not_measured".to_string()),
    );
    push_field(
        fields,
        "materialization_rate",
        &report
            .materialization_rate
            .clone()
            .unwrap_or_else(|| "not_measured".to_string()),
    );
    push_count_field(fields, "known_limit_count", report.known_limits.len());
    push_count_field(fields, "blocking_gap_count", report.blocking_gaps.len());
    push_count_field(
        fields,
        "capability_snapshot_ref_count",
        report.capability_snapshot_refs.len(),
    );
    push_count_field(
        fields,
        "external_baseline_ref_count",
        report.external_baseline_refs.len(),
    );
    push_bool_field(
        fields,
        "best_default_claim_allowed",
        report.can_publish_best_default_claim(),
    );
    push_field(fields, "scorecard_ref", &report.scorecard_ref);
    push_field(
        fields,
        "best_default_dossier_ref",
        &report.best_default_dossier_ref,
    );
}

fn push_world_class_sufficiency_status_field(
    fields: &mut Vec<(String, String)>,
    key: &str,
    report: &WorldClassSufficiencyReport,
    kind: WorldClassSufficiencyDimensionKind,
) {
    push_field(fields, key, report.status_for(kind).as_str());
}

fn append_world_class_sufficiency_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "parser_executed", report.parser_executed);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(fields, "network_probe", report.network_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn user_capability_promotion_gate_fields(
    report: &UserCapabilityPromotionGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "cg20_user_capability_promotion_gate");
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_count_field(&mut fields, "surface_count", report.surface_count());
    push_count_field(
        &mut fields,
        "existing_evidence_surface_count",
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_field(
        &mut fields,
        "existing_report_refs",
        &report.existing_report_refs.join(","),
    );
    append_user_capability_existing_fields(&mut fields, report);
    append_user_capability_allowed_fields(&mut fields, report);
    append_user_capability_required_fields(&mut fields, report);
    append_user_capability_status_fields(&mut fields, report);
    fields
}

fn append_user_capability_existing_fields(
    fields: &mut Vec<(String, String)>,
    report: &UserCapabilityPromotionGateReport,
) {
    push_bool_field(
        fields,
        "existing_world_class_sufficiency_report_present",
        report.existing_world_class_sufficiency_report_present,
    );
    push_bool_field(
        fields,
        "existing_python_wrapper_foundation_present",
        report.existing_python_wrapper_foundation_present,
    );
    push_bool_field(
        fields,
        "existing_input_adapter_registry_present",
        report.existing_input_adapter_registry_present,
    );
    push_bool_field(
        fields,
        "existing_unstructured_workflow_boundary_contracts_present",
        report.existing_unstructured_workflow_boundary_contracts_present,
    );
}

fn append_user_capability_allowed_fields(
    fields: &mut Vec<(String, String)>,
    report: &UserCapabilityPromotionGateReport,
) {
    push_bool_field(fields, "sql_runtime_allowed", report.sql_runtime_allowed);
    push_bool_field(
        fields,
        "dataframe_runtime_allowed",
        report.dataframe_runtime_allowed,
    );
    push_bool_field(
        fields,
        "notebook_runtime_allowed",
        report.notebook_runtime_allowed,
    );
    push_bool_field(
        fields,
        "udf_execution_allowed",
        report.udf_execution_allowed,
    );
    push_bool_field(
        fields,
        "plugin_execution_allowed",
        report.plugin_execution_allowed,
    );
    push_bool_field(
        fields,
        "unstructured_media_decode_allowed",
        report.unstructured_media_decode_allowed,
    );
    push_bool_field(
        fields,
        "ocr_transcription_embedding_llm_allowed",
        report.ocr_transcription_embedding_llm_allowed,
    );
    push_bool_field(
        fields,
        "adapter_runtime_allowed",
        report.adapter_runtime_allowed,
    );
    push_bool_field(
        fields,
        "external_api_call_allowed",
        report.external_api_call_allowed,
    );
    push_bool_field(
        fields,
        "catalog_probe_allowed",
        report.catalog_probe_allowed,
    );
    push_bool_field(
        fields,
        "object_store_io_allowed",
        report.object_store_io_allowed,
    );
    push_bool_field(fields, "write_io_allowed", report.write_io_allowed);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "best_default_claim_allowed",
        report.best_default_claim_allowed,
    );
    push_bool_field(
        fields,
        "user_capability_claim_allowed",
        report.user_capability_claim_allowed,
    );
}

fn append_user_capability_required_fields(
    fields: &mut Vec<(String, String)>,
    report: &UserCapabilityPromotionGateReport,
) {
    push_bool_field(
        fields,
        "world_class_sufficiency_report_required",
        report.world_class_sufficiency_report_required,
    );
    push_bool_field(
        fields,
        "semantic_profile_required",
        report.semantic_profile_required,
    );
    push_bool_field(
        fields,
        "sql_coverage_required",
        report.sql_coverage_required,
    );
    push_bool_field(
        fields,
        "operator_coverage_required",
        report.operator_coverage_required,
    );
    push_bool_field(
        fields,
        "function_coverage_required",
        report.function_coverage_required,
    );
    push_bool_field(
        fields,
        "adapter_certification_required",
        report.adapter_certification_required,
    );
    push_bool_field(
        fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(
        fields,
        "workload_constitution_required",
        report.workload_constitution_required,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required",
        report.materialization_boundary_required,
    );
    push_bool_field(
        fields,
        "effect_policy_required",
        report.effect_policy_required,
    );
    push_bool_field(
        fields,
        "security_governance_required",
        report.security_governance_required,
    );
    push_bool_field(
        fields,
        "protocol_surface_parity_required",
        report.protocol_surface_parity_required,
    );
}

fn append_user_capability_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &UserCapabilityPromotionGateReport,
) {
    push_bool_field(
        fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(fields, "claim_blocked", report.claim_blocked());
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn approx_sketch_function_gate_fields(
    report: &ApproxSketchFunctionGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "cg20_approx_sketch_function_gate");
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(
        &mut fields,
        "canonical_function_name",
        report.canonical_function_name,
    );
    push_field(&mut fields, "alias_names", &report.alias_names.join(","));
    push_field(
        &mut fields,
        "value_handling_order",
        &report.value_handling_order(),
    );
    push_count_field(&mut fields, "surface_count", report.surface_count());
    push_count_field(
        &mut fields,
        "existing_evidence_surface_count",
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_field(
        &mut fields,
        "existing_report_refs",
        &report.existing_report_refs.join(","),
    );
    append_approx_sketch_existing_fields(&mut fields, report);
    append_approx_sketch_allowed_fields(&mut fields, report);
    append_approx_sketch_required_fields(&mut fields, report);
    append_approx_sketch_status_fields(&mut fields, report);
    fields
}

fn append_approx_sketch_existing_fields(
    fields: &mut Vec<(String, String)>,
    report: &ApproxSketchFunctionGateReport,
) {
    push_bool_field(
        fields,
        "existing_function_coverage_matrix_entry_present",
        report.existing_function_coverage_matrix_entry_present,
    );
    push_bool_field(
        fields,
        "existing_rfc_sequencing_contract_present",
        report.existing_rfc_sequencing_contract_present,
    );
}

fn append_approx_sketch_allowed_fields(
    fields: &mut Vec<(String, String)>,
    report: &ApproxSketchFunctionGateReport,
) {
    push_bool_field(
        fields,
        "function_registry_entry_allowed",
        report.function_registry_entry_allowed,
    );
    push_bool_field(
        fields,
        "sketch_state_runtime_allowed",
        report.sketch_state_runtime_allowed,
    );
    push_bool_field(
        fields,
        "sketch_merge_runtime_allowed",
        report.sketch_merge_runtime_allowed,
    );
    push_bool_field(
        fields,
        "sketch_serialization_runtime_allowed",
        report.sketch_serialization_runtime_allowed,
    );
    push_bool_field(
        fields,
        "grouped_aggregate_runtime_allowed",
        report.grouped_aggregate_runtime_allowed,
    );
    push_bool_field(
        fields,
        "encoded_dictionary_strategy_allowed",
        report.encoded_dictionary_strategy_allowed,
    );
    push_bool_field(
        fields,
        "encoded_run_length_strategy_allowed",
        report.encoded_run_length_strategy_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_strategy_allowed",
        report.selection_vector_strategy_allowed,
    );
    push_bool_field(
        fields,
        "partial_decode_execution_allowed",
        report.partial_decode_execution_allowed,
    );
    push_bool_field(
        fields,
        "materialization_without_report_allowed",
        report.materialization_without_report_allowed,
    );
    push_bool_field(
        fields,
        "generic_sketch_dependency_allowed",
        report.generic_sketch_dependency_allowed,
    );
    push_bool_field(fields, "exact_claim_allowed", report.exact_claim_allowed);
    push_bool_field(
        fields,
        "approximate_function_claim_allowed",
        report.approximate_function_claim_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
}

fn append_approx_sketch_required_fields(
    fields: &mut Vec<(String, String)>,
    report: &ApproxSketchFunctionGateReport,
) {
    push_bool_field(
        fields,
        "function_registry_required",
        report.function_registry_required,
    );
    push_bool_field(
        fields,
        "aggregate_state_required",
        report.aggregate_state_required,
    );
    push_bool_field(
        fields,
        "sketch_serialization_required",
        report.sketch_serialization_required,
    );
    push_bool_field(
        fields,
        "stable_hash_seed_policy_required",
        report.stable_hash_seed_policy_required,
    );
    push_bool_field(
        fields,
        "error_bounds_required",
        report.error_bounds_required,
    );
    push_bool_field(
        fields,
        "confidence_model_required",
        report.confidence_model_required,
    );
    push_bool_field(
        fields,
        "exact_reference_fixtures_required",
        report.exact_reference_fixtures_required,
    );
    push_bool_field(
        fields,
        "encoded_dictionary_strategy_required",
        report.encoded_dictionary_strategy_required,
    );
    push_bool_field(
        fields,
        "encoded_run_length_strategy_required",
        report.encoded_run_length_strategy_required,
    );
    push_bool_field(
        fields,
        "selection_vector_strategy_required",
        report.selection_vector_strategy_required,
    );
    push_bool_field(
        fields,
        "partial_decode_materialization_boundary_required",
        report.partial_decode_materialization_boundary_required,
    );
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(
        fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
}

fn append_approx_sketch_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &ApproxSketchFunctionGateReport,
) {
    push_bool_field(
        fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(fields, "claim_blocked", report.claim_blocked());
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn append_native_io_envelope_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_field(fields, "mode", "native_io_envelope_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "native_io_envelope_status", report.status.as_str());
    push_count_field(fields, "contract_count", report.contract_count());
    push_count_field(
        fields,
        "representation_state_count",
        report.representation_state_count(),
    );
    push_count_field(
        fields,
        "transition_example_count",
        report.transition_example_count(),
    );
    push_count_field(
        fields,
        "certificate_path_requirement_count",
        report.certificate_path_requirement_count(),
    );
    push_field(fields, "contract_kind_order", &report.contract_kind_order());
    push_field(
        fields,
        "representation_state_order",
        &report.representation_state_order(),
    );
    push_field(
        fields,
        "transition_example_order",
        &report.transition_example_order(),
    );
    push_field(
        fields,
        "certificate_path_order",
        &report.certificate_path_order(),
    );
}

fn append_native_io_envelope_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_bool_field(
        fields,
        "per_path_certificate_required",
        report.per_path_certificate_required,
    );
    push_bool_field(
        fields,
        "aggregate_certificate_not_sufficient",
        report.aggregate_certificate_not_sufficient,
    );
    push_bool_field(
        fields,
        "preserve_encoded_or_foreign_encoded_when_possible",
        report.preserve_encoded_or_foreign_encoded_when_possible,
    );
    push_bool_field(
        fields,
        "decoded_arrow_normalization_allowed",
        report.decoded_arrow_normalization_allowed,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required_for_decoded_columnar",
        report.materialization_boundary_required_for_decoded_columnar,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required_for_rows",
        report.materialization_boundary_required_for_rows,
    );
    push_bool_field(
        fields,
        "source_pushdown_proof_required",
        report.source_pushdown_proof_required,
    );
    push_bool_field(
        fields,
        "sink_requirement_propagation_required",
        report.sink_requirement_propagation_required,
    );
    push_bool_field(
        fields,
        "adapter_fidelity_report_required",
        report.adapter_fidelity_report_required,
    );
}

fn append_native_io_envelope_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}
