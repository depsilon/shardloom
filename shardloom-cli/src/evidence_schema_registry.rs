//! Typed evidence-field schema registry and CLI projection.
//!
//! The registry is side-effect-free. It documents and validates high-value
//! typed-envelope artifact payload fields without running commands, probing
//! datasets, or authorizing runtime support.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, Diagnostic, OutputFormat, ShardLoomError};

use crate::{
    cli_output::{emit, emit_error},
    typed_envelope::typed_envelope_artifact_payload_keys,
};

pub(crate) const REGISTRY_SCHEMA_VERSION: &str = "shardloom.evidence_field_schema_registry.v1";
const REGISTRY_REPORT_ID: &str = "review-p1-2.evidence_field_schema_registry";
const REGISTRY_SOURCE: &str = "shardloom-cli/src/evidence_schema_registry.rs";
const REGISTRY_DOCS_REF: &str = "docs/status/evidence-field-schema-registry.md";
const REGISTRY_COMMAND: &str = "shardloom evidence-schema [surface] --format json";
const DEPRECATION_POLICY: &str = "additive_v1_requires_compatibility_note_for_removal_or_rename";
const CLAIM_BOUNDARY: &str =
    "schema consistency and drift prevention only; not runtime support or public API stability";
const FALLBACK_BOUNDARY: &str =
    "schema rendering is side-effect-free and never invokes fallback or external engines";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvidenceSchemaSurfaceSpec {
    pub surface_id: &'static str,
    pub artifact_kind: &'static str,
    pub command_examples: &'static str,
    pub support_state: &'static str,
    pub claim_boundary: &'static str,
    pub python_accessor_mapping: &'static str,
    pub required_no_fallback_fields: &'static str,
}

pub(crate) const EVIDENCE_SCHEMA_SURFACES: &[EvidenceSchemaSurfaceSpec] = &[
    EvidenceSchemaSurfaceSpec {
        surface_id: "execution_mode_selection_report",
        artifact_kind: "execution_mode_selection_report",
        command_examples: "traditional-analytics-run,traditional-analytics-vortex-run,rest-api-plan-preview",
        support_state: "schema_declared",
        claim_boundary: "mode_admission_metadata_only",
        python_accessor_mapping: "TraditionalAnalyticsRun.execution_mode_selection_fields",
        required_no_fallback_fields: "fallback_attempted,external_engine_invoked",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "compute_flow_evidence",
        artifact_kind: "compute_flow_evidence",
        command_examples: "traditional-analytics-run,traditional-analytics-vortex-run,traditional-analytics-vortex-batch-run,traditional-analytics-prepare-batch-run",
        support_state: "schema_declared",
        claim_boundary: "compute_flow_evidence_only_not_performance_claim",
        python_accessor_mapping: "TraditionalAnalyticsRun.compute_flow_evidence_fields",
        required_no_fallback_fields: "fallback_attempted,external_engine_invoked",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "execution_certificate_report",
        artifact_kind: "execution_certificate_report",
        command_examples: "execution-certificate-plan,vortex-run,vortex-project,vortex-filter-project",
        support_state: "schema_declared",
        claim_boundary: "certificate_surface_metadata_only_until_certificate_emitted",
        python_accessor_mapping: "OutputEnvelope.artifacts[execution_certificate_report]",
        required_no_fallback_fields: "fallback_execution_allowed,fallback_attempted",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "native_io_report",
        artifact_kind: "native_io_report",
        command_examples: "native-io-envelope-plan,vortex-run,vortex-project,vortex-filter-project",
        support_state: "schema_declared",
        claim_boundary: "native_io_contract_metadata_only_until_certificate_emitted",
        python_accessor_mapping: "OutputEnvelope.artifacts[native_io_report]",
        required_no_fallback_fields: "fallback_execution_allowed,fallback_attempted",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "benchmark_plan_report",
        artifact_kind: "benchmark_plan_report",
        command_examples: "benchmark-plan",
        support_state: "schema_declared",
        claim_boundary: "benchmark_plan_only_not_superiority_claim",
        python_accessor_mapping: "OutputEnvelope.artifacts[benchmark_plan_report]",
        required_no_fallback_fields: "fallback_execution_allowed",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "benchmark_constitution_report",
        artifact_kind: "benchmark_constitution_report",
        command_examples: "benchmark-constitution",
        support_state: "schema_declared",
        claim_boundary: "benchmark_constitution_validator_only_not_claim_grade_without_rows",
        python_accessor_mapping: "OutputEnvelope.artifacts[benchmark_constitution_report]",
        required_no_fallback_fields: "benchmark_constitution_fallback_attempted,benchmark_constitution_external_engine_invoked",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "benchmark_claim_evidence_report",
        artifact_kind: "benchmark_claim_evidence_report",
        command_examples: "benchmark-claim-evidence-plan",
        support_state: "schema_declared",
        claim_boundary: "benchmark_claim_evidence_only_not_claim_grade_without_rows",
        python_accessor_mapping: "OutputEnvelope.artifacts[benchmark_claim_evidence_report]",
        required_no_fallback_fields: "fallback_execution_allowed,fallback_attempted",
    },
    EvidenceSchemaSurfaceSpec {
        surface_id: "compute_capability_matrix_report",
        artifact_kind: "compute_capability_matrix_report",
        command_examples: "compute-capability-matrix",
        support_state: "schema_declared",
        claim_boundary: "capability_matrix_metadata_only_not_compute_engine_completion_claim",
        python_accessor_mapping: "OutputEnvelope.artifacts[compute_capability_matrix_report]",
        required_no_fallback_fields: "all_rows_fallback_attempted_false,all_rows_external_engine_invoked_false,fallback_attempted",
    },
];

pub(crate) fn handle_evidence_schema(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let selected = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "evidence-schema",
            format,
            "unexpected evidence schema argument",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected evidence-schema argument: {extra}"
            )),
        );
    }
    let selected_surface = match selected.as_deref() {
        Some(surface_id) => match lookup_surface(surface_id) {
            Some(surface) => Some(surface),
            None => {
                return emit_error(
                    "evidence-schema",
                    format,
                    "unknown evidence schema surface",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown evidence-schema surface: {surface_id}"
                    )),
                );
            }
        },
        None => None,
    };
    emit(
        "evidence-schema",
        format,
        CommandStatus::Success,
        "evidence field schema registry rendered without side effects".to_string(),
        evidence_schema_text(selected_surface),
        Vec::<Diagnostic>::new(),
        evidence_schema_fields(selected_surface),
    );
    ExitCode::SUCCESS
}

pub(crate) fn lookup_surface(surface_id: &str) -> Option<EvidenceSchemaSurfaceSpec> {
    EVIDENCE_SCHEMA_SURFACES
        .iter()
        .copied()
        .find(|surface| surface.surface_id == surface_id)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn evidence_schema_fields(
    selected: Option<EvidenceSchemaSurfaceSpec>,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "evidence_schema_registry_schema_version".to_string(),
            REGISTRY_SCHEMA_VERSION.to_string(),
        ),
        (
            "evidence_schema_registry_report_id".to_string(),
            REGISTRY_REPORT_ID.to_string(),
        ),
        (
            "evidence_schema_registry_source".to_string(),
            REGISTRY_SOURCE.to_string(),
        ),
        (
            "evidence_schema_registry_docs_ref".to_string(),
            REGISTRY_DOCS_REF.to_string(),
        ),
        (
            "evidence_schema_registry_command".to_string(),
            REGISTRY_COMMAND.to_string(),
        ),
        (
            "evidence_schema_registry_surface_count".to_string(),
            EVIDENCE_SCHEMA_SURFACES.len().to_string(),
        ),
        (
            "evidence_schema_registry_field_count".to_string(),
            total_field_count().to_string(),
        ),
        (
            "evidence_schema_registry_surface_order".to_string(),
            EVIDENCE_SCHEMA_SURFACES
                .iter()
                .map(|surface| surface.surface_id)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "evidence_schema_registry_dtype_vocabulary".to_string(),
            "string,boolean,integer".to_string(),
        ),
        (
            "evidence_schema_registry_cardinality_vocabulary".to_string(),
            "scalar,list_or_csv".to_string(),
        ),
        (
            "evidence_schema_registry_deprecation_policy".to_string(),
            DEPRECATION_POLICY.to_string(),
        ),
        (
            "evidence_schema_registry_claim_boundary".to_string(),
            CLAIM_BOUNDARY.to_string(),
        ),
        (
            "evidence_schema_registry_fallback_boundary".to_string(),
            FALLBACK_BOUNDARY.to_string(),
        ),
        (
            "evidence_schema_registry_claim_gate_status".to_string(),
            "metadata_only_not_claim_grade".to_string(),
        ),
        (
            "evidence_schema_registry_schema_drift_detection_status".to_string(),
            "rust_docs_python_contracts_checked".to_string(),
        ),
        (
            "evidence_schema_registry_python_accessor_contract_status".to_string(),
            "typed_accessor_mapping_declared".to_string(),
        ),
        (
            "evidence_schema_registry_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "evidence_schema_registry_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ];
    if let Some(surface) = selected {
        fields.extend([
            (
                "selected_surface".to_string(),
                surface.surface_id.to_string(),
            ),
            (
                "selected_surface_artifact_kind".to_string(),
                surface.artifact_kind.to_string(),
            ),
            (
                "selected_surface_field_count".to_string(),
                payload_keys(surface).len().to_string(),
            ),
            (
                "selected_surface_field_order".to_string(),
                payload_keys(surface).join(","),
            ),
            (
                "selected_surface_python_accessor_mapping".to_string(),
                surface.python_accessor_mapping.to_string(),
            ),
            (
                "selected_surface_required_no_fallback_fields".to_string(),
                surface.required_no_fallback_fields.to_string(),
            ),
        ]);
    }
    for surface in EVIDENCE_SCHEMA_SURFACES {
        append_surface_fields(&mut fields, *surface);
        append_field_rows(&mut fields, *surface);
    }
    fields
}

pub(crate) fn append_evidence_schema_registry_capability_fields(
    fields: &mut Vec<(String, String)>,
) {
    fields.extend(evidence_schema_fields(None));
}

fn append_surface_fields(fields: &mut Vec<(String, String)>, surface: EvidenceSchemaSurfaceSpec) {
    let prefix = format!("evidence_schema_surface_{}", surface.surface_id);
    fields.extend([
        (
            format!("{prefix}_artifact_kind"),
            surface.artifact_kind.to_string(),
        ),
        (
            format!("{prefix}_command_examples"),
            surface.command_examples.to_string(),
        ),
        (
            format!("{prefix}_support_state"),
            surface.support_state.to_string(),
        ),
        (
            format!("{prefix}_field_count"),
            payload_keys(surface).len().to_string(),
        ),
        (
            format!("{prefix}_field_order"),
            payload_keys(surface).join(","),
        ),
        (
            format!("{prefix}_python_accessor_mapping"),
            surface.python_accessor_mapping.to_string(),
        ),
        (
            format!("{prefix}_required_no_fallback_fields"),
            surface.required_no_fallback_fields.to_string(),
        ),
        (
            format!("{prefix}_claim_boundary"),
            surface.claim_boundary.to_string(),
        ),
        (
            format!("{prefix}_deprecation_policy"),
            DEPRECATION_POLICY.to_string(),
        ),
    ]);
}

fn append_field_rows(fields: &mut Vec<(String, String)>, surface: EvidenceSchemaSurfaceSpec) {
    for key in payload_keys(surface) {
        let prefix = format!(
            "evidence_schema_field_{}_{}",
            surface.surface_id,
            field_id(key)
        );
        fields.extend([
            (format!("{prefix}_key"), (*key).to_string()),
            (format!("{prefix}_dtype"), field_dtype(key).to_string()),
            (
                format!("{prefix}_cardinality"),
                field_cardinality(key).to_string(),
            ),
            (
                format!("{prefix}_required_when"),
                "artifact_emitted_or_field_present".to_string(),
            ),
            (
                format!("{prefix}_owning_surface"),
                surface.surface_id.to_string(),
            ),
            (
                format!("{prefix}_owning_artifact_kind"),
                surface.artifact_kind.to_string(),
            ),
            (
                format!("{prefix}_owning_commands"),
                surface.command_examples.to_string(),
            ),
            (
                format!("{prefix}_support_state"),
                surface.support_state.to_string(),
            ),
            (
                format!("{prefix}_no_fallback_semantics"),
                field_no_fallback_semantics(key).to_string(),
            ),
            (
                format!("{prefix}_deprecation_policy"),
                DEPRECATION_POLICY.to_string(),
            ),
            (
                format!("{prefix}_python_accessor_mapping"),
                surface.python_accessor_mapping.to_string(),
            ),
            (
                format!("{prefix}_claim_boundary"),
                surface.claim_boundary.to_string(),
            ),
        ]);
    }
}

fn payload_keys(surface: EvidenceSchemaSurfaceSpec) -> &'static [&'static str] {
    typed_envelope_artifact_payload_keys(surface.artifact_kind)
        .expect("evidence schema surface is backed by typed-envelope payload keys")
}

fn total_field_count() -> usize {
    EVIDENCE_SCHEMA_SURFACES
        .iter()
        .map(|surface| payload_keys(*surface).len())
        .sum()
}

fn field_id(field_key: &str) -> String {
    field_key.replace('-', "_")
}

fn field_dtype(field_key: &str) -> &'static str {
    if is_boolean_field(field_key) {
        "boolean"
    } else if is_integer_field(field_key) {
        "integer"
    } else {
        "string"
    }
}

fn field_cardinality(field_key: &str) -> &'static str {
    if field_key.ends_with("_order")
        || field_key.ends_with("_refs")
        || field_key.ends_with("_claims")
        || field_key.ends_with("_vocabulary")
        || field_key.contains("_columns")
        || field_key.contains("_operations")
        || field_key.contains("_surfaces")
        || field_key.contains("_evidence")
    {
        "list_or_csv"
    } else {
        "scalar"
    }
}

fn field_no_fallback_semantics(field_key: &str) -> &'static str {
    if field_key.contains("fallback_attempted")
        || field_key.contains("external_engine_invoked")
        || field_key.contains("external_query_engine_invoked")
    {
        "must_remain_false"
    } else if field_key.contains("fallback_execution_allowed") {
        "must_remain_false_for_fallback_execution"
    } else if field_key.contains("claim") {
        "claim_gate_field_not_support_claim_by_itself"
    } else {
        "inherits_surface_no_fallback_boundary"
    }
}

fn is_boolean_field(field_key: &str) -> bool {
    field_key == "runtime_execution"
        || field_key.starts_with("all_")
        || field_key.ends_with("_allowed")
        || field_key.ends_with("_attempted")
        || field_key.ends_with("_invoked")
        || field_key.ends_with("_included")
        || field_key.ends_with("_available")
        || field_key.ends_with("_supported")
        || field_key.ends_with("_emitted")
        || field_key.ends_with("_performed")
        || field_key.ends_with("_required")
        || field_key.ends_with("_preserved")
        || field_key.ends_with("_present")
        || field_key.ends_with("_blocked")
        || field_key.ends_with("_certified")
        || field_key.ends_with("_verified")
        || field_key.ends_with("_converted")
        || field_key.ends_with("_materialized")
        || field_key.ends_with("_executed")
        || field_key.ends_with("_read")
        || field_key.ends_with("_written")
        || field_key.ends_with("_requested")
        || field_key.ends_with("_eligible")
        || field_key.ends_with("_used")
        || field_key.ends_with("_enabled")
        || field_key.ends_with("_implemented")
}

fn is_integer_field(field_key: &str) -> bool {
    field_key.ends_with("_count")
        || field_key.ends_with("_rows")
        || field_key.ends_with("_bytes")
        || field_key.ends_with("_micros")
        || field_key.ends_with("_mib")
        || field_key.ends_with("_iterations")
}

fn evidence_schema_text(selected: Option<EvidenceSchemaSurfaceSpec>) -> String {
    if let Some(surface) = selected {
        return format!(
            "evidence_schema_registry_schema_version={REGISTRY_SCHEMA_VERSION}\nselected_surface={}\nartifact_kind={}\nfield_count={}\nfield_order={}\nclaim_boundary={}\nfallback_attempted=false\nexternal_engine_invoked=false",
            surface.surface_id,
            surface.artifact_kind,
            payload_keys(surface).len(),
            payload_keys(surface).join(","),
            surface.claim_boundary
        );
    }
    format!(
        "evidence_schema_registry_schema_version={REGISTRY_SCHEMA_VERSION}\nsurface_count={}\nfield_count={}\nsurface_order={}\nfallback_attempted=false\nexternal_engine_invoked=false",
        EVIDENCE_SCHEMA_SURFACES.len(),
        total_field_count(),
        EVIDENCE_SCHEMA_SURFACES
            .iter()
            .map(|surface| surface.surface_id)
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_surfaces_are_backed_by_typed_envelope_payload_keys() {
        for surface in EVIDENCE_SCHEMA_SURFACES {
            let keys = payload_keys(*surface);
            assert!(
                !keys.is_empty(),
                "surface={} has no keys",
                surface.surface_id
            );
            assert!(
                keys.iter().any(|key| key.contains("fallback")),
                "surface={} lacks fallback evidence",
                surface.surface_id
            );
            for required in surface.required_no_fallback_fields.split(',') {
                assert!(
                    keys.contains(&required),
                    "surface={} missing required no-fallback field {required}",
                    surface.surface_id
                );
            }
        }
    }

    #[test]
    fn selected_surface_fields_are_agent_visible() {
        let surface = lookup_surface("execution_mode_selection_report").expect("registered");
        let fields = evidence_schema_fields(Some(surface));
        assert!(fields.contains(&(
            "selected_surface".to_string(),
            "execution_mode_selection_report".to_string()
        )));
        assert!(fields.contains(&(
            "selected_surface_field_count".to_string(),
            payload_keys(surface).len().to_string()
        )));
        assert!(fields.contains(&(
            "evidence_schema_field_execution_mode_selection_report_fallback_attempted_no_fallback_semantics".to_string(),
            "must_remain_false".to_string()
        )));
    }

    #[test]
    fn field_classification_covers_runtime_execution_and_vocabularies() {
        assert_eq!(field_dtype("runtime_execution"), "boolean");
        assert_eq!(
            field_cardinality("support_status_vocabulary"),
            "list_or_csv"
        );
    }

    #[test]
    fn docs_status_snippet_tracks_registry_summary() {
        let docs = include_str!("../../docs/status/evidence-field-schema-registry.md");
        assert!(docs.contains(REGISTRY_SCHEMA_VERSION));
        assert!(docs.contains(REGISTRY_SOURCE));
        assert!(docs.contains(REGISTRY_COMMAND));
        assert!(docs.contains(&format!(
            "Surface count: {}",
            EVIDENCE_SCHEMA_SURFACES.len()
        )));
        assert!(docs.contains(&format!("Field count: {}", total_field_count())));
        assert!(docs.contains("fallback_attempted=false"));
        assert!(docs.contains("external_engine_invoked=false"));
    }
}
