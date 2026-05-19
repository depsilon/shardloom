//! Packaging, release, wrapper, and agent-contract CLI handlers.
//!
//! These commands are report-only planning surfaces. They do not publish
//! packages, push artifacts, execute external engines, or perform fallback work.

use std::process::ExitCode;

use shardloom_core::{
    AgentContractPack, CommandStatus, ComparativeRerunManagedPlatformGateReport,
    CompetitiveReplacementSufficiencyGateReport, CompetitiveReplacementSufficiencyGateRow,
    CondaBuildInstallCertificationReport, EngineReplacementClaimInventoryReport,
    EngineReplacementClaimInventoryRow, OutputFormat, PythonWrapperFoundationReport,
    ReleaseEvidenceRequirementKind, ReleasePlan, ReleasePublicationApiSchemaGateReport,
    ReleasePublicationBoundaryKind, ReleasePublicationBoundaryReport,
    ReleaseReadinessEvidenceReport, plan_comparative_rerun_managed_platform_gate,
    plan_competitive_replacement_sufficiency_gate, plan_engine_replacement_claim_inventory,
};

use crate::cli_output::emit;

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
    let publication_api_schema = plan.publication_api_schema_stability_gate();
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
        release_plan_fields(
            &plan,
            &evidence,
            &publication,
            &publication_api_schema,
            mode,
        ),
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

pub(crate) fn agent_contract_pack_fields(report: &AgentContractPack) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), "agent_contract_pack".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("pack_id".to_string(), report.pack_id.to_string()),
        (
            "surface_count".to_string(),
            report.surfaces.len().to_string(),
        ),
        (
            "available_surface_count".to_string(),
            report.available_surface_count().to_string(),
        ),
        (
            "side_effect_free_surface_count".to_string(),
            report.side_effect_free_surface_count().to_string(),
        ),
        (
            "fallback_allowed_surface_count".to_string(),
            report.fallback_allowed_surface_count().to_string(),
        ),
        (
            "surface_order".to_string(),
            report.surface_order().join(","),
        ),
        (
            "recommended_sequence".to_string(),
            report.recommended_sequence.join(" -> "),
        ),
        (
            "deterministic_json_required".to_string(),
            report.deterministic_json_required.to_string(),
        ),
        (
            "text_is_authoritative".to_string(),
            report.text_is_authoritative.to_string(),
        ),
        (
            "no_probe_default".to_string(),
            report.no_probe_default.to_string(),
        ),
        (
            "external_effects_default_denied".to_string(),
            report.external_effects_default_denied.to_string(),
        ),
        (
            "destructive_effects_default_denied".to_string(),
            report.destructive_effects_default_denied.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.side_effect_free().to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

pub(crate) fn release_plan_fields(
    plan: &ReleasePlan,
    evidence: &ReleaseReadinessEvidenceReport,
    publication: &ReleasePublicationBoundaryReport,
    publication_api_schema: &ReleasePublicationApiSchemaGateReport,
    mode: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", mode);
    push_field(&mut fields, "schema_version", evidence.schema_version);
    push_field(&mut fields, "report_id", evidence.report_id);
    push_field(&mut fields, "release_version", &evidence.release_version);
    push_field(
        &mut fields,
        "release_channel",
        evidence.release_channel.as_str(),
    );
    push_field(
        &mut fields,
        "release_readiness",
        evidence.release_readiness.as_str(),
    );
    push_bool_field(&mut fields, "publish_allowed", plan.publish_allowed());
    push_bool_field(
        &mut fields,
        "public_release_claim_allowed",
        evidence.public_release_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "public_package_claim_allowed",
        evidence.public_package_claim_allowed,
    );
    push_count_field(
        &mut fields,
        "blocking_release_requirement_count",
        evidence.blocking_requirement_count(),
    );
    push_count_field(
        &mut fields,
        "public_surface_count",
        plan.public_surfaces.len(),
    );
    push_count_field(&mut fields, "schema_count", plan.schemas.len());
    push_count_field(
        &mut fields,
        "package_target_count",
        plan.package_targets.len(),
    );
    push_count_field(&mut fields, "artifact_count", plan.artifacts.len());
    push_count_field(
        &mut fields,
        "dependency_review_count",
        plan.dependency_reviews.len(),
    );
    push_count_field(&mut fields, "release_checklist_count", plan.checklist.len());
    append_release_evidence_requirement_fields(&mut fields, evidence);
    append_release_publication_boundary_fields(&mut fields, publication);
    append_publication_api_schema_gate_fields(&mut fields, publication_api_schema);
    append_conda_build_install_certification_fields(
        &mut fields,
        &plan.conda_build_install_certification(),
    );
    append_engine_replacement_claim_inventory_fields(
        &mut fields,
        &plan_engine_replacement_claim_inventory(),
    );
    append_competitive_replacement_sufficiency_gate_fields(
        &mut fields,
        &plan_competitive_replacement_sufficiency_gate(),
    );
    append_comparative_rerun_managed_platform_gate_release_fields(
        &mut fields,
        &plan_comparative_rerun_managed_platform_gate(),
    );
    push_field(&mut fields, "published", "false");
    push_field(&mut fields, "write_io", "false");
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "external_publish", "not_performed");
    push_bool_field(
        &mut fields,
        "external_publish_performed",
        evidence.external_publish_performed,
    );
    push_bool_field(&mut fields, "runtime_execution", evidence.runtime_execution);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        evidence.fallback_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "fallback_attempted",
        evidence.fallback_attempted,
    );
    fields
}

fn append_engine_replacement_claim_inventory_fields(
    fields: &mut Vec<(String, String)>,
    report: &EngineReplacementClaimInventoryReport,
) {
    append_engine_replacement_claim_inventory_identity_fields(fields, report);
    append_engine_replacement_claim_inventory_summary_fields(fields, report);
    append_engine_replacement_claim_inventory_row_fields(fields, report);
}

fn append_engine_replacement_claim_inventory_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &EngineReplacementClaimInventoryReport,
) {
    push_field(
        fields,
        "engine_replacement_claim_inventory_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_source_refs",
        report.source_refs,
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_support_status",
        report.support_status,
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_claim_gate_status",
        report.claim_gate_status,
    );
}

fn append_engine_replacement_claim_inventory_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &EngineReplacementClaimInventoryReport,
) {
    push_count_field(
        fields,
        "engine_replacement_claim_inventory_row_count",
        report.rows.len(),
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_row_order",
        &report.row_order().join(","),
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_claim_families",
        &report.claim_families().join(","),
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_dependency_gate_refs",
        &report.dependency_gate_refs().join(" | "),
    );
    push_field(
        fields,
        "engine_replacement_claim_inventory_missing_evidence",
        &report.missing_evidence().join(" | "),
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_all_rows_not_claim_grade",
        report.all_rows_not_claim_grade(),
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_all_claims_blocked",
        report.all_engine_replacement_claims_blocked(),
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_side_effect_free",
        report.side_effect_free(),
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_public_engine_replacement_claim_allowed",
        report.public_engine_replacement_claim_allowed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_spark_displacement_claim_allowed",
        report.spark_displacement_claim_allowed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_best_default_claim_allowed",
        report.best_default_claim_allowed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_performance_superiority_claim_allowed",
        report.performance_superiority_claim_allowed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_production_platform_claim_allowed",
        report.production_platform_claim_allowed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_runtime_execution_performed",
        report.runtime_execution_performed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_benchmark_rerun_performed",
        report.benchmark_rerun_performed,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "engine_replacement_claim_inventory_external_engine_invoked",
        report.external_engine_invoked,
    );
}

fn append_engine_replacement_claim_inventory_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &EngineReplacementClaimInventoryReport,
) {
    for row in &report.rows {
        let prefix = format!("engine_replacement_claim_inventory_row_{}", row.claim_id);
        append_engine_replacement_claim_inventory_row_identity_fields(fields, &prefix, row);
        append_engine_replacement_claim_inventory_row_evidence_fields(fields, &prefix, row);
        append_engine_replacement_claim_inventory_row_status_fields(fields, &prefix, row);
    }
}

fn append_engine_replacement_claim_inventory_row_identity_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &EngineReplacementClaimInventoryRow,
) {
    push_field(fields, &format!("{prefix}_claim_family"), row.claim_family);
    push_field(
        fields,
        &format!("{prefix}_claim_language"),
        row.claim_language,
    );
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_release_gate_ref"),
        row.release_gate_ref,
    );
    push_field(
        fields,
        &format!("{prefix}_dependency_gate_refs"),
        row.dependency_gate_refs,
    );
}

fn append_engine_replacement_claim_inventory_row_evidence_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &EngineReplacementClaimInventoryRow,
) {
    for (suffix, value) in [
        ("required_runtime_evidence", row.required_runtime_evidence),
        ("required_output_evidence", row.required_output_evidence),
        (
            "required_correctness_evidence",
            row.required_correctness_evidence,
        ),
        (
            "required_benchmark_evidence",
            row.required_benchmark_evidence,
        ),
        (
            "required_execution_certificate_evidence",
            row.required_execution_certificate_evidence,
        ),
        (
            "required_native_io_evidence",
            row.required_native_io_evidence,
        ),
        (
            "required_no_fallback_evidence",
            row.required_no_fallback_evidence,
        ),
        ("missing_evidence", row.missing_evidence),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

fn append_engine_replacement_claim_inventory_row_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &EngineReplacementClaimInventoryRow,
) {
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_public_claim_allowed"),
        row.public_claim_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_evidence_complete"),
        row.evidence_complete,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_execution_performed"),
        row.runtime_execution_performed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_benchmark_rerun_performed"),
        row.benchmark_rerun_performed,
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
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_competitive_replacement_sufficiency_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompetitiveReplacementSufficiencyGateReport,
) {
    append_competitive_replacement_sufficiency_gate_summary_fields(fields, report);
    append_competitive_replacement_sufficiency_gate_row_fields(fields, report);
}

fn append_competitive_replacement_sufficiency_gate_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompetitiveReplacementSufficiencyGateReport,
) {
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_support_status",
        report.support_status,
    );
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_claim_gate_status",
        report.claim_gate_status,
    );
    push_count_field(
        fields,
        "competitive_replacement_sufficiency_gate_row_count",
        report.rows.len(),
    );
    push_count_field(
        fields,
        "competitive_replacement_sufficiency_gate_blocking_row_count",
        report.blocking_row_count(),
    );
    push_field(
        fields,
        "competitive_replacement_sufficiency_gate_row_ids",
        &report.row_ids().join(","),
    );
    append_competitive_replacement_sufficiency_gate_claim_fields(fields, report);
}

fn append_competitive_replacement_sufficiency_gate_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompetitiveReplacementSufficiencyGateReport,
) {
    for (field, value) in [
        (
            "competitive_replacement_sufficiency_gate_correctness_sufficient",
            report.correctness_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_benchmark_sufficient",
            report.benchmark_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_native_io_sufficient",
            report.native_io_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_execution_certificate_sufficient",
            report.execution_certificate_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_capability_coverage_sufficient",
            report.capability_coverage_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_no_fallback_sufficient",
            report.no_fallback_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_release_evidence_sufficient",
            report.release_evidence_sufficient,
        ),
        (
            "competitive_replacement_sufficiency_gate_all_claims_blocked",
            report.all_claims_blocked(),
        ),
        (
            "competitive_replacement_sufficiency_gate_side_effect_free",
            report.side_effect_free(),
        ),
        (
            "competitive_replacement_sufficiency_gate_public_engine_replacement_claim_allowed",
            report.public_engine_replacement_claim_allowed,
        ),
        (
            "competitive_replacement_sufficiency_gate_spark_displacement_claim_allowed",
            report.spark_displacement_claim_allowed,
        ),
        (
            "competitive_replacement_sufficiency_gate_superiority_claim_allowed",
            report.superiority_claim_allowed,
        ),
        (
            "competitive_replacement_sufficiency_gate_production_platform_claim_allowed",
            report.production_platform_claim_allowed,
        ),
        (
            "competitive_replacement_sufficiency_gate_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "competitive_replacement_sufficiency_gate_external_engine_invoked",
            report.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, field, value);
    }
}

fn append_competitive_replacement_sufficiency_gate_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompetitiveReplacementSufficiencyGateReport,
) {
    for row in &report.rows {
        let prefix = format!(
            "competitive_replacement_sufficiency_gate_row_{}",
            row.evidence_id
        );
        append_competitive_replacement_sufficiency_gate_row(fields, &prefix, row);
    }
}

fn append_competitive_replacement_sufficiency_gate_row(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &CompetitiveReplacementSufficiencyGateRow,
) {
    push_field(fields, &format!("{prefix}_family"), row.evidence_family);
    push_field(fields, &format!("{prefix}_status"), row.status);
    push_field(
        fields,
        &format!("{prefix}_required_evidence"),
        row.required_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_current_evidence_ref"),
        row.current_evidence_ref,
    );
    push_field(fields, &format!("{prefix}_blocker"), row.blocker);
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_public_claim_allowed"),
        row.public_claim_allowed,
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

fn append_comparative_rerun_managed_platform_gate_release_fields(
    fields: &mut Vec<(String, String)>,
    report: &ComparativeRerunManagedPlatformGateReport,
) {
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_support_status",
        report.support_status,
    );
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_claim_gate_status",
        report.claim_gate_status,
    );
    push_count_field(
        fields,
        "comparative_rerun_managed_platform_gate_row_count",
        report.rows.len(),
    );
    push_count_field(
        fields,
        "comparative_rerun_managed_platform_gate_blocking_row_count",
        report.blocking_row_count(),
    );
    push_field(
        fields,
        "comparative_rerun_managed_platform_gate_row_ids",
        &report.row_ids().join(","),
    );
    append_comparative_rerun_managed_platform_gate_release_boolean_fields(fields, report);
}

fn append_comparative_rerun_managed_platform_gate_release_boolean_fields(
    fields: &mut Vec<(String, String)>,
    report: &ComparativeRerunManagedPlatformGateReport,
) {
    for (field, value) in [
        (
            "comparative_rerun_managed_platform_gate_local_comparative_rerun_required",
            report.local_comparative_rerun_required,
        ),
        (
            "comparative_rerun_managed_platform_gate_local_comparative_rerun_performed",
            report.local_comparative_rerun_performed,
        ),
        (
            "comparative_rerun_managed_platform_gate_external_baselines_comparison_only",
            report.external_baselines_comparison_only,
        ),
        (
            "comparative_rerun_managed_platform_gate_managed_platform_lanes_comparison_only",
            report.managed_platform_lanes_comparison_only,
        ),
        (
            "comparative_rerun_managed_platform_gate_managed_platform_credentials_required",
            report.managed_platform_credentials_required,
        ),
        (
            "comparative_rerun_managed_platform_gate_managed_platform_credentials_resolved",
            report.managed_platform_credentials_resolved,
        ),
        (
            "comparative_rerun_managed_platform_gate_managed_platform_dependencies_added",
            report.managed_platform_dependencies_added,
        ),
        (
            "comparative_rerun_managed_platform_gate_managed_platform_execution_performed",
            report.managed_platform_execution_performed,
        ),
        (
            "comparative_rerun_managed_platform_gate_benchmark_artifact_claim_grade",
            report.benchmark_artifact_claim_grade,
        ),
        (
            "comparative_rerun_managed_platform_gate_performance_claim_allowed",
            report.performance_claim_allowed,
        ),
        (
            "comparative_rerun_managed_platform_gate_superiority_claim_allowed",
            report.superiority_claim_allowed,
        ),
        (
            "comparative_rerun_managed_platform_gate_spark_displacement_claim_allowed",
            report.spark_displacement_claim_allowed,
        ),
        (
            "comparative_rerun_managed_platform_gate_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "comparative_rerun_managed_platform_gate_external_engine_invoked",
            report.external_engine_invoked,
        ),
        (
            "comparative_rerun_managed_platform_gate_all_claims_blocked",
            report.all_claims_blocked(),
        ),
        (
            "comparative_rerun_managed_platform_gate_side_effect_free",
            report.side_effect_free(),
        ),
    ] {
        push_bool_field(fields, field, value);
    }
}

fn append_release_publication_boundary_fields(
    fields: &mut Vec<(String, String)>,
    publication: &ReleasePublicationBoundaryReport,
) {
    push_field(
        fields,
        "publication_boundary_schema_version",
        publication.schema_version,
    );
    push_field(
        fields,
        "publication_boundary_report_id",
        publication.report_id,
    );
    for (field, kind) in [
        (
            "local_development_boundary",
            ReleasePublicationBoundaryKind::LocalDevelopment,
        ),
        (
            "public_package_boundary",
            ReleasePublicationBoundaryKind::PublicPackage,
        ),
        (
            "github_release_boundary",
            ReleasePublicationBoundaryKind::GitHubRelease,
        ),
        (
            "container_image_boundary",
            ReleasePublicationBoundaryKind::ContainerImage,
        ),
        (
            "server_mode_boundary",
            ReleasePublicationBoundaryKind::ServerMode,
        ),
        (
            "benchmark_extras_boundary",
            ReleasePublicationBoundaryKind::BenchmarkExtras,
        ),
    ] {
        push_field(fields, field, publication.status_for(kind).as_str());
    }
    push_bool_field(
        fields,
        "local_development_available",
        publication.local_development_available,
    );
    push_bool_field(
        fields,
        "package_publication_distinct_from_local_development",
        publication.package_publication_distinct_from_local_development,
    );
    push_bool_field(
        fields,
        "container_publication_distinct_from_local_development",
        publication.container_publication_distinct_from_local_development,
    );
    push_bool_field(
        fields,
        "server_publication_distinct_from_local_development",
        publication.server_publication_distinct_from_local_development,
    );
    push_bool_field(
        fields,
        "benchmark_extras_optional",
        publication.benchmark_extras_optional,
    );
    push_bool_field(
        fields,
        "benchmark_extras_comparison_only",
        publication.benchmark_extras_comparison_only,
    );
    push_bool_field(fields, "benchmark_extras_core_dependency", false);
    push_bool_field(
        fields,
        "publication_fallback_dependency_allowed",
        publication.fallback_dependency_allowed,
    );
}

fn append_conda_build_install_certification_fields(
    fields: &mut Vec<(String, String)>,
    report: &CondaBuildInstallCertificationReport,
) {
    append_conda_certification_identity_fields(fields, report);
    append_conda_certification_required_gate_fields(fields, report);
    append_conda_certification_gate_status_fields(fields, report);
    append_conda_certification_side_effect_fields(fields, report);
}

fn append_conda_certification_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &CondaBuildInstallCertificationReport,
) {
    push_field(
        fields,
        "conda_certification_schema_version",
        report.schema_version,
    );
    push_field(fields, "conda_certification_report_id", report.report_id);
    push_count_field(fields, "conda_package_count", report.package_count());
    push_count_field(
        fields,
        "conda_recipe_scaffold_count",
        report.recipe_scaffold_count(),
    );
    push_count_field(
        fields,
        "conda_certified_package_count",
        report.certified_package_count(),
    );
    push_count_field(
        fields,
        "conda_release_gate_blocking_count",
        report.release_gate_blocking_count(),
    );
}

fn append_conda_certification_required_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &CondaBuildInstallCertificationReport,
) {
    push_bool_field(
        fields,
        "conda_tagged_archive_required",
        report.tagged_archive_required,
    );
    push_bool_field(
        fields,
        "conda_source_hash_required",
        report.source_hash_required,
    );
    push_bool_field(
        fields,
        "conda_version_alignment_required",
        report.version_alignment_required,
    );
    push_bool_field(
        fields,
        "conda_provenance_attestation_required",
        report.provenance_attestation_required,
    );
    push_bool_field(
        fields,
        "conda_human_approval_required",
        report.human_approval_required,
    );
}

fn append_conda_certification_gate_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &CondaBuildInstallCertificationReport,
) {
    push_bool_field(
        fields,
        "conda_tagged_archive_present",
        report.tagged_archive_present,
    );
    push_bool_field(
        fields,
        "conda_source_hash_verified",
        report.source_hash_verified,
    );
    push_bool_field(
        fields,
        "conda_version_alignment_verified",
        report.version_alignment_verified,
    );
    push_bool_field(
        fields,
        "conda_provenance_attestation_present",
        report.provenance_attestation_present,
    );
    push_bool_field(
        fields,
        "conda_human_approval_present",
        report.human_approval_present,
    );
    push_bool_field(
        fields,
        "conda_clean_build_certified",
        report.clean_build_certified,
    );
    push_bool_field(
        fields,
        "conda_clean_install_certified",
        report.clean_install_certified,
    );
    push_bool_field(
        fields,
        "conda_package_publication_allowed",
        report.package_publication_allowed,
    );
}

fn append_conda_certification_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CondaBuildInstallCertificationReport,
) {
    push_bool_field(fields, "conda_build_invoked", report.conda_build_invoked);
    push_bool_field(
        fields,
        "conda_install_invoked",
        report.conda_install_invoked,
    );
    push_bool_field(
        fields,
        "conda_external_publish_performed",
        report.external_publish_performed,
    );
    push_bool_field(fields, "conda_release_gated", report.release_gated());
    push_bool_field(
        fields,
        "conda_side_effect_free",
        report.is_side_effect_free(),
    );
    push_bool_field(
        fields,
        "conda_fallback_dependency_allowed",
        report.fallback_dependency_allowed,
    );
}

fn append_release_evidence_requirement_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &ReleaseReadinessEvidenceReport,
) {
    for (field, kind) in [
        (
            "schema_version_check",
            ReleaseEvidenceRequirementKind::SchemaVersion,
        ),
        (
            "api_stability_check",
            ReleaseEvidenceRequirementKind::ApiStability,
        ),
        (
            "dependency_license_check",
            ReleaseEvidenceRequirementKind::DependencyLicense,
        ),
        ("sbom_check", ReleaseEvidenceRequirementKind::Sbom),
        (
            "provenance_attestation_check",
            ReleaseEvidenceRequirementKind::ProvenanceAttestation,
        ),
        (
            "reproducible_build_check",
            ReleaseEvidenceRequirementKind::ReproducibleBuild,
        ),
        (
            "release_notes_check",
            ReleaseEvidenceRequirementKind::ReleaseNotes,
        ),
        (
            "benchmark_accountability_check",
            ReleaseEvidenceRequirementKind::BenchmarkAccountability,
        ),
        (
            "no_fallback_release_check",
            ReleaseEvidenceRequirementKind::NoFallback,
        ),
        (
            "human_approval_check",
            ReleaseEvidenceRequirementKind::HumanApproval,
        ),
    ] {
        push_field(fields, field, evidence.status_for(kind).as_str());
    }
}

fn append_publication_api_schema_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &ReleasePublicationApiSchemaGateReport,
) {
    append_publication_api_schema_gate_summary_fields(fields, report);
    append_publication_api_schema_gate_row_fields(fields, report);
}

fn append_publication_api_schema_gate_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &ReleasePublicationApiSchemaGateReport,
) {
    push_field(
        fields,
        "publication_api_schema_gate_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "publication_api_schema_gate_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "publication_api_schema_gate_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "publication_api_schema_gate_status",
        report.gate_status,
    );
    push_field(
        fields,
        "publication_api_schema_gate_claim_gate_status",
        report.claim_gate_status,
    );
    push_count_field(
        fields,
        "publication_api_schema_gate_row_count",
        report.rows.len(),
    );
    push_count_field(
        fields,
        "publication_api_schema_gate_blocking_row_count",
        report.blocking_row_count(),
    );
    push_field(
        fields,
        "publication_api_schema_gate_row_ids",
        &report.row_ids().join(","),
    );
    append_publication_api_schema_gate_claim_fields(fields, report);
}

fn append_publication_api_schema_gate_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ReleasePublicationApiSchemaGateReport,
) {
    push_bool_field(
        fields,
        "publication_api_schema_gate_api_schema_stability_claim_allowed",
        report.api_schema_stability_claim_allowed,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_public_release_claim_allowed",
        report.public_release_claim_allowed,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_public_package_claim_allowed",
        report.public_package_claim_allowed,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_package_publication_performed",
        report.package_publication_performed,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_tag_created",
        report.tag_created,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_signing_key_used",
        report.signing_key_used,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_checksum_manifest_publication_grade",
        report.checksum_manifest_publication_grade,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_sbom_publication_grade",
        report.sbom_publication_grade,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_runtime_execution",
        report.runtime_execution,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_fails_closed",
        report.fails_closed(),
    );
    push_bool_field(
        fields,
        "publication_api_schema_gate_side_effect_free",
        report.side_effect_free(),
    );
}

fn append_publication_api_schema_gate_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &ReleasePublicationApiSchemaGateReport,
) {
    for row in &report.rows {
        let prefix = format!("publication_api_schema_gate_row_{}", row.gate_id);
        push_field(fields, &format!("{prefix}_status"), row.current_status);
        push_field(
            fields,
            &format!("{prefix}_required_evidence"),
            row.required_evidence,
        );
        push_field(fields, &format!("{prefix}_evidence_ref"), row.evidence_ref);
        push_field(fields, &format!("{prefix}_blocker"), row.blocker);
        push_bool_field(
            fields,
            &format!("{prefix}_public_release_claim_allowed"),
            row.public_release_claim_allowed,
        );
        push_bool_field(
            fields,
            &format!("{prefix}_public_package_claim_allowed"),
            row.public_package_claim_allowed,
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
}

pub(crate) fn python_wrapper_fields(
    report: &PythonWrapperFoundationReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_python_wrapper_identity_fields(&mut fields, report);
    append_python_wrapper_distribution_fields(&mut fields, report);
    append_python_wrapper_runtime_boundary_fields(&mut fields, report);
    append_python_wrapper_side_effect_fields(&mut fields, report);
    fields
}

fn append_python_wrapper_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &PythonWrapperFoundationReport,
) {
    push_field(fields, "mode", "python_wrapper_plan");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "wrapper_id", report.wrapper_id);
    push_field(fields, "wrapper_status", report.wrapper_status);
    push_field(
        fields,
        "transport_protocol_id",
        report.transport_protocol_id,
    );
    push_field(
        fields,
        "output_envelope_schema_version",
        report.output_envelope_schema_version,
    );
    push_field(fields, "invocation_model", report.invocation_model);
    push_field(
        fields,
        "initial_command_scope",
        &report.initial_command_scope.join(","),
    );
    push_field(
        fields,
        "required_client_behaviors",
        &report.required_client_behaviors.join(","),
    );
}

fn append_python_wrapper_distribution_fields(
    fields: &mut Vec<(String, String)>,
    report: &PythonWrapperFoundationReport,
) {
    push_field(fields, "package_status", report.package_status);
    push_field(
        fields,
        "native_binding_status",
        report.native_binding_status,
    );
    push_bool_field(
        fields,
        "wheel_sdist_build_ready",
        report.wheel_sdist_build_ready,
    );
    push_bool_field(
        fields,
        "fresh_environment_smoke_required",
        report.fresh_environment_smoke_required,
    );
    push_bool_field(
        fields,
        "missing_binary_diagnostic_ready",
        report.missing_binary_diagnostic_ready,
    );
    push_bool_field(
        fields,
        "conda_cli_package_required",
        report.conda_cli_package_required,
    );
    push_bool_field(
        fields,
        "conda_python_package_planned",
        report.conda_python_package_planned,
    );
    push_bool_field(
        fields,
        "conda_metapackage_planned",
        report.conda_metapackage_planned,
    );
    push_field(fields, "conda_recipe_root", report.conda_recipe_root);
    push_bool_field(
        fields,
        "conda_cli_recipe_created",
        report.conda_cli_recipe_created,
    );
    push_bool_field(
        fields,
        "conda_python_recipe_created",
        report.conda_python_recipe_created,
    );
    push_bool_field(
        fields,
        "conda_metapackage_recipe_created",
        report.conda_metapackage_recipe_created,
    );
    push_bool_field(
        fields,
        "benchmark_extras_optional",
        report.benchmark_extras_optional,
    );
}

fn append_python_wrapper_runtime_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &PythonWrapperFoundationReport,
) {
    push_bool_field(fields, "pyo3_maturin_allowed", report.pyo3_maturin_allowed);
    push_bool_field(
        fields,
        "python_package_created",
        report.python_package_created,
    );
    push_bool_field(
        fields,
        "native_extension_required",
        report.native_extension_required,
    );
    push_bool_field(
        fields,
        "dataframe_api_implemented",
        report.dataframe_api_implemented,
    );
    push_bool_field(
        fields,
        "notebook_api_implemented",
        report.notebook_api_implemented,
    );
    push_bool_field(
        fields,
        "python_udf_runtime_implemented",
        report.python_udf_runtime_implemented,
    );
    push_bool_field(
        fields,
        "materialization_boundary_reporting_required",
        report.materialization_boundary_reporting_required,
    );
    push_bool_field(
        fields,
        "diagnostics_passthrough_required",
        report.diagnostics_passthrough_required,
    );
}

fn append_python_wrapper_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &PythonWrapperFoundationReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free);
    push_bool_field(fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(fields, "network_probe", report.network_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "parser_executed", report.parser_executed);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "write_io", report.write_io);
    push_field(fields, "external_publish", "not_performed");
    push_bool_field(
        fields,
        "external_publish_performed",
        report.external_publish,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
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
