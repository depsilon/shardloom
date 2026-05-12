//! Vortex metadata and report-only planning CLI handlers.
//!
//! These handlers expose Vortex metadata, pruning, probe, and API-inventory
//! planning surfaces. They remain metadata/report-only and do not execute
//! tasks, read data beyond explicit metadata probe contracts, materialize
//! outputs, write data, invoke external engines, or allow fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetUri, OutputFormat, PhysicalOperatorKind, ShardLoomError,
};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexEncodedExecutionPathSelectionReport,
    VortexGeneralizedEncodedPrimitiveGateReport, VortexMetadataProbeReport,
    metadata_planning_is_side_effect_free, metadata_pruning_is_side_effect_free,
    plan_from_vortex_metadata_summary, plan_vortex_encoded_execution_path_selection,
    plan_vortex_generalized_encoded_primitive_gate, plan_vortex_metadata_pruning,
    probe_vortex_metadata_only, summarize_vortex_metadata_probe,
};

use crate::cli_output::{emit, emit_error};

pub(crate) fn handle_vortex_metadata_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_text) = args.next() else {
        return emit_error(
            "vortex-metadata-plan",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let uri = match DatasetUri::new(uri_text) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-metadata-plan",
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            );
        }
    };
    let probe = probe_vortex_metadata_only(uri)
        .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
    let summary = summarize_vortex_metadata_probe(&probe);
    let report = match plan_from_vortex_metadata_summary(summary) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-metadata-plan",
                format,
                "vortex metadata plan failed",
                &error,
            );
        }
    };
    emit(
        "vortex-metadata-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata planning".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_metadata_plan".to_string()),
            ("metadata_only".to_string(), "true".to_string()),
            ("plan_only".to_string(), report.is_plan_only().to_string()),
            ("data_executed".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            (
                "side_effect_free".to_string(),
                metadata_planning_is_side_effect_free(&report).to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_pruning_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_arg) = args.next() else {
        return emit_error(
            "vortex-pruning-plan",
            format,
            "vortex pruning plan failed",
            &ShardLoomError::InvalidOperation("missing <dataset_uri> argument".to_string()),
        );
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-pruning-plan",
                format,
                "vortex pruning plan failed",
                &error,
            );
        }
    };
    let probe = match probe_vortex_metadata_only(uri) {
        Ok(p) => p,
        Err(error) => {
            return emit_error(
                "vortex-pruning-plan",
                format,
                "vortex pruning plan failed",
                &error,
            );
        }
    };
    let summary = summarize_vortex_metadata_probe(&probe);
    let planning = match plan_from_vortex_metadata_summary(summary) {
        Ok(p) => p,
        Err(error) => {
            return emit_error(
                "vortex-pruning-plan",
                format,
                "vortex pruning plan failed",
                &error,
            );
        }
    };
    let report = match plan_vortex_metadata_pruning(planning, None) {
        Ok(r) => r,
        Err(error) => {
            return emit_error(
                "vortex-pruning-plan",
                format,
                "vortex pruning plan failed",
                &error,
            );
        }
    };
    emit(
        "vortex-pruning-plan",
        format,
        if report.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        },
        "vortex metadata pruning plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_pruning_plan".to_string()),
            ("metadata_only".to_string(), "true".to_string()),
            ("plan_only".to_string(), report.is_plan_only().to_string()),
            (
                "data_executed".to_string(),
                report.data_executed.to_string(),
            ),
            (
                "data_materialized".to_string(),
                report.data_materialized.to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io.to_string(),
            ),
            ("write_io".to_string(), report.write_io.to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            (
                "side_effect_free".to_string(),
                metadata_pruning_is_side_effect_free(&report).to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_metadata_probe(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_text) = args.next() else {
        return emit_error(
            "vortex-metadata-probe",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let uri = match DatasetUri::new(uri_text) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-metadata-probe",
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            );
        }
    };
    let report = probe_vortex_metadata_only(uri)
        .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
    emit(
        "vortex-metadata-probe",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata-only probe".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_metadata_probe".to_string()),
            ("metadata_only".to_string(), "true".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            (
                "metadata_io_status".to_string(),
                report.status.as_str().to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_api_inventory(format: OutputFormat) -> ExitCode {
    let report = VortexAdapterCapabilityReport::foundation();
    emit(
        "vortex-api-inventory",
        format,
        CommandStatus::Success,
        "vortex API inventory".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_api_inventory".to_string()),
            (
                "upstream_vortex_dependency".to_string(),
                "linked".to_string(),
            ),
            ("actual_io".to_string(), "not_implemented".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_encoded_path_selection_plan(format: OutputFormat) -> ExitCode {
    let command = "vortex-encoded-path-selection-plan";
    let report = plan_vortex_encoded_execution_path_selection();
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded path selection plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_encoded_path_selection_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_generalized_encoded_primitive_gate(format: OutputFormat) -> ExitCode {
    let command = "vortex-generalized-encoded-primitive-gate";
    let report = plan_vortex_generalized_encoded_primitive_gate();
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex generalized encoded primitive gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_generalized_encoded_primitive_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn vortex_encoded_path_selection_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    let mut fields = vortex_encoded_path_selection_identity_fields(report);
    fields.extend(vortex_encoded_path_selection_candidate_fields(report));
    fields.extend(vortex_encoded_path_selection_discovery_fields(report));
    fields.extend(vortex_encoded_path_selection_side_effect_fields(report));
    fields
}

fn vortex_encoded_path_selection_identity_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "mode".to_string(),
            "vortex_encoded_path_selection_plan".to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("report_id".to_string(), report.report_id.clone()),
        (
            "profile_matrix_id".to_string(),
            report.profile_matrix_id.clone(),
        ),
        (
            "selection_status".to_string(),
            report.status.as_str().to_string(),
        ),
        ("entry_count".to_string(), report.entry_count().to_string()),
        (
            "operator_order".to_string(),
            report.operator_order().join(","),
        ),
        (
            "selected_execution_levels".to_string(),
            report.selected_execution_levels().join(","),
        ),
        (
            "evidence_sources".to_string(),
            report.evidence_sources().join(","),
        ),
    ]
}

fn vortex_encoded_path_selection_candidate_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "direct_count_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::CountAggregate)
                .to_string(),
        ),
        (
            "direct_filter_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::Filter)
                .to_string(),
        ),
        (
            "direct_project_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::Project)
                .to_string(),
        ),
        (
            "metadata_only_candidate_count".to_string(),
            report.metadata_only_candidate_count().to_string(),
        ),
        (
            "encoded_native_candidate_count".to_string(),
            report.encoded_native_candidate_count().to_string(),
        ),
        (
            "hybrid_native_candidate_count".to_string(),
            report.hybrid_native_candidate_count().to_string(),
        ),
        (
            "native_decoded_candidate_count".to_string(),
            report.native_decoded_candidate_count().to_string(),
        ),
        (
            "decode_avoided_candidate_count".to_string(),
            report.decode_avoided_candidate_count().to_string(),
        ),
        (
            "materialization_avoided_candidate_count".to_string(),
            report.materialization_avoided_candidate_count().to_string(),
        ),
        (
            "selection_vector_preserved_count".to_string(),
            report.selection_vector_preserved_count().to_string(),
        ),
    ]
}

fn vortex_encoded_path_selection_discovery_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "encoded_count_discovery_present".to_string(),
            report.encoded_count_discovery_present.to_string(),
        ),
        (
            "encoded_predicate_discovery_present".to_string(),
            report.encoded_predicate_discovery_present.to_string(),
        ),
        (
            "selection_vector_filter_discovery_present".to_string(),
            report.selection_vector_filter_discovery_present.to_string(),
        ),
        (
            "encoded_projection_evidence_present".to_string(),
            report.encoded_projection_evidence_present.to_string(),
        ),
    ]
}

fn vortex_encoded_path_selection_side_effect_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        ("row_read".to_string(), report.row_read.to_string()),
        (
            "arrow_converted".to_string(),
            report.arrow_converted.to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("write_io".to_string(), report.write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            report.spill_io_performed.to_string(),
        ),
        (
            "runtime_execution_allowed".to_string(),
            report.runtime_execution_allowed.to_string(),
        ),
        (
            "external_engine_execution".to_string(),
            report.external_engine_execution.to_string(),
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
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    let mut fields = vortex_generalized_encoded_primitive_gate_identity_fields(report);
    fields.extend(vortex_generalized_encoded_primitive_gate_evidence_fields(
        report,
    ));
    fields.extend(vortex_generalized_encoded_primitive_gate_requirement_fields(report));
    fields.extend(vortex_generalized_encoded_primitive_gate_side_effect_fields(report));
    fields
}

fn vortex_generalized_encoded_primitive_gate_identity_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "mode".to_string(),
            "vortex_generalized_encoded_primitive_gate".to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("report_id".to_string(), report.report_id.clone()),
        (
            "gate_status".to_string(),
            report.status.as_str().to_string(),
        ),
        ("entry_count".to_string(), report.entry_count().to_string()),
        (
            "primitive_order".to_string(),
            report.primitive_order().join(","),
        ),
        (
            "primitive_statuses".to_string(),
            report.primitive_statuses().join(","),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_evidence_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "local_count_all_only".to_string(),
            report.local_count_all_only.to_string(),
        ),
        (
            "entries_with_local_count_support".to_string(),
            report.entries_with_local_count_support().to_string(),
        ),
        (
            "entries_with_local_filter_scan_pushdown_support".to_string(),
            report
                .entries_with_local_filter_scan_pushdown_support()
                .to_string(),
        ),
        (
            "entries_with_prepared_encoded_filter_execution_support".to_string(),
            report
                .entries_with_prepared_encoded_filter_execution_support()
                .to_string(),
        ),
        (
            "entries_with_source_backed_prepared_encoded_filter_execution_support".to_string(),
            report
                .entries_with_source_backed_prepared_encoded_filter_execution_support()
                .to_string(),
        ),
        (
            "entries_with_local_projection_scan_pushdown_support".to_string(),
            report
                .entries_with_local_projection_scan_pushdown_support()
                .to_string(),
        ),
        (
            "entries_with_prepared_encoded_projection_execution_support".to_string(),
            report
                .entries_with_prepared_encoded_projection_execution_support()
                .to_string(),
        ),
        (
            "entries_with_source_backed_prepared_encoded_projection_execution_support".to_string(),
            report
                .entries_with_source_backed_prepared_encoded_projection_execution_support()
                .to_string(),
        ),
        (
            "entries_with_metadata_proof".to_string(),
            report.entries_with_metadata_proof().to_string(),
        ),
        (
            "entries_with_readiness_contract".to_string(),
            report.entries_with_readiness_contract().to_string(),
        ),
        (
            "implementation_blocker_count".to_string(),
            report.implementation_blocker_count().to_string(),
        ),
        (
            "required_next_evidence_count".to_string(),
            report.required_next_evidence_count().to_string(),
        ),
        (
            "generalized_count_ready".to_string(),
            report.generalized_count_ready.to_string(),
        ),
        (
            "filtered_count_execution_ready".to_string(),
            report.filtered_count_execution_ready.to_string(),
        ),
        (
            "projection_execution_ready".to_string(),
            report.projection_execution_ready.to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_requirement_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "requires_public_scan_or_read_start_path".to_string(),
            report.requires_public_scan_or_read_start_path.to_string(),
        ),
        (
            "requires_encoded_predicate_path".to_string(),
            report.requires_encoded_predicate_path.to_string(),
        ),
        (
            "requires_encoded_projection_path".to_string(),
            report.requires_encoded_projection_path.to_string(),
        ),
        (
            "requires_selection_vector_pipeline".to_string(),
            report.requires_selection_vector_pipeline.to_string(),
        ),
        (
            "requires_native_io_certificate".to_string(),
            report.requires_native_io_certificate.to_string(),
        ),
        (
            "requires_execution_certificate".to_string(),
            report.requires_execution_certificate.to_string(),
        ),
        (
            "requires_correctness_evidence".to_string(),
            report.requires_correctness_evidence.to_string(),
        ),
        (
            "requires_benchmark_evidence".to_string(),
            report.requires_benchmark_evidence.to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_side_effect_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        ("row_read".to_string(), report.row_read.to_string()),
        (
            "arrow_converted".to_string(),
            report.arrow_converted.to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("write_io".to_string(), report.write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            report.spill_io_performed.to_string(),
        ),
        (
            "runtime_execution_allowed".to_string(),
            report.runtime_execution_allowed.to_string(),
        ),
        (
            "external_engine_execution".to_string(),
            report.external_engine_execution.to_string(),
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
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}
