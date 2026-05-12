//! Packaging, release, wrapper, and agent-contract CLI handlers.
//!
//! These commands are report-only planning surfaces. They do not publish
//! packages, push artifacts, execute external engines, or perform fallback work.

use std::process::ExitCode;

use shardloom_core::{
    AgentContractPack, CommandStatus, CondaBuildInstallCertificationReport, OutputFormat,
    PythonWrapperFoundationReport, ReleaseEvidenceRequirementKind, ReleasePlan,
    ReleasePublicationBoundaryKind, ReleasePublicationBoundaryReport,
    ReleaseReadinessEvidenceReport,
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
        release_plan_fields(&plan, &evidence, &publication, mode),
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
    append_conda_build_install_certification_fields(
        &mut fields,
        &plan.conda_build_install_certification(),
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
