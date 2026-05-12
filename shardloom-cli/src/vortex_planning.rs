//! Vortex metadata and report-only planning CLI handlers.
//!
//! These handlers expose Vortex metadata, pruning, probe, and API-inventory
//! planning surfaces. They remain metadata/report-only and do not execute
//! tasks, read data beyond explicit metadata probe contracts, materialize
//! outputs, write data, invoke external engines, or allow fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetUri, OutputFormat, OutputTarget, PhysicalOperatorKind, ShardLoomError,
    TranslationPlan, UniversalInputSource,
};
use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexAdapterReadiness, VortexDTypeMappingReport,
    VortexEncodedExecutionPathSelectionReport, VortexEncodingLayoutMappingReport,
    VortexExecutionReadinessReport, VortexFileRef, VortexGeneralizedEncodedPrimitiveGateReport,
    VortexMetadataOpenRequest, VortexMetadataProbeReport, VortexQueryPrimitiveSignal,
    VortexReadPlan, VortexStatisticsMappingReport, VortexWriteOptions, VortexWritePlan,
    build_vortex_runtime_task_graph, evaluate_vortex_execution_readiness,
    execute_vortex_metadata_only, metadata_planning_is_side_effect_free,
    metadata_pruning_is_side_effect_free, metadata_summary_is_plan_only, open_vortex_metadata_only,
    plan_from_vortex_metadata_summary, plan_native_vortex_universal_input,
    plan_vortex_encoded_execution_path_selection, plan_vortex_generalized_encoded_primitive_gate,
    plan_vortex_metadata_pruning, plan_vortex_query_primitive,
    plan_vortex_read_from_universal_input, probe_vortex_metadata_only,
    summarize_vortex_metadata_probe, vortex_file_io_feature_enabled,
    vortex_metadata_executor_feature_enabled,
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

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_metadata_execute(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-metadata-execute";
    let (memory_gb, max_parallelism, readiness_report) = match plan_vortex_execution_readiness(
        command,
        args,
        format,
        "vortex metadata execute planning failed",
    ) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let exec_report = match execute_vortex_metadata_only(readiness_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex metadata execute planning failed",
                &error,
            );
        }
    };
    emit(
        command,
        format,
        if exec_report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata-only executor report".to_string(),
        exec_report.to_human_text(),
        exec_report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_metadata_execute".to_string()),
            (
                "executor_feature_enabled".to_string(),
                vortex_metadata_executor_feature_enabled().to_string(),
            ),
            ("metadata_only".to_string(), "true".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "metadata_only_or_not_performed".to_string(),
            ),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if exec_report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_dry_run(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-dry-run";
    let (memory_gb, max_parallelism, readiness_report) = match plan_vortex_execution_readiness(
        command,
        args,
        format,
        "vortex readiness planning failed",
    ) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let text = readiness_report.dry_run_contract.to_human_text();
    emit(
        command,
        format,
        if readiness_report.has_errors() || crate::readiness_is_blocked(readiness_report.status) {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex dry-run contract".to_string(),
        text,
        readiness_report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_dry_run".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("dry_run_only".to_string(), "true".to_string()),
            ("tasks_executed".to_string(), "false".to_string()),
            ("data_executed".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if readiness_report.has_errors() || crate::readiness_is_blocked(readiness_report.status) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let file_ref = match VortexFileRef::from_uri(uri) {
        Ok(file_ref) => file_ref,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    emit(
        "vortex-plan",
        format,
        CommandStatus::Success,
        "vortex read plan".to_string(),
        VortexReadPlan::metadata_only(file_ref).to_human_text(),
        vec![],
        vec![("mode".to_string(), "metadata_only".to_string())],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_translation_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom translation-plan <target_uri>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let target = OutputTarget::from_uri(uri);
    let plan = TranslationPlan::for_target(target);
    emit(
        "translation-plan",
        format,
        if plan.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "translation plan".to_string(),
        plan.to_human_text(),
        plan.diagnostics.clone(),
        vec![],
    );
    if plan.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_output_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-output-plan <target_uri>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let file_ref = match VortexFileRef::from_uri(uri) {
        Ok(file_ref) => file_ref,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    emit(
        "vortex-output-plan",
        format,
        CommandStatus::Success,
        "vortex output plan".to_string(),
        VortexWritePlan::planned(file_ref, VortexWriteOptions::native_defaults()).to_human_text(),
        vec![],
        vec![("target_format".to_string(), "vortex".to_string())],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_readiness(format: OutputFormat) -> ExitCode {
    let readiness = VortexAdapterReadiness::dependency_added_compile_only();
    emit(
        "vortex-readiness",
        format,
        CommandStatus::Success,
        "vortex dependency readiness".to_string(),
        readiness.to_human_text(),
        readiness.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_readiness".to_string()),
            (
                "upstream_vortex_dependency".to_string(),
                readiness.dependency_status.as_str().to_string(),
            ),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("io".to_string(), "not_performed".to_string()),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_dtype_mapping(format: OutputFormat) -> ExitCode {
    let report = if shardloom_vortex::typed_vortex_dtype_mapping_available() {
        VortexDTypeMappingReport::implemented("vortex::DType")
    } else {
        VortexDTypeMappingReport::deferred_api_unclear()
    };
    emit(
        "vortex-dtype-mapping",
        format,
        CommandStatus::Success,
        "vortex dtype mapping".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_dtype_mapping".to_string()),
            (
                "upstream_vortex_dependency".to_string(),
                "linked".to_string(),
            ),
            ("actual_io".to_string(), "not_implemented".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            (
                "name_based_mapping_available".to_string(),
                "true".to_string(),
            ),
            (
                "typed_mapping_status".to_string(),
                report.status.as_str().to_string(),
            ),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_encoding_layout_mapping(format: OutputFormat) -> ExitCode {
    let report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
    emit(
        "vortex-encoding-layout-mapping",
        format,
        CommandStatus::Success,
        "vortex encoding/layout mapping".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_encoding_layout_mapping".to_string(),
            ),
            (
                "upstream_vortex_dependency".to_string(),
                "linked".to_string(),
            ),
            ("actual_io".to_string(), "not_implemented".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            (
                "name_based_mapping_available".to_string(),
                "true".to_string(),
            ),
            (
                "encoding_mapping_status".to_string(),
                report.encoding_status.as_str().to_string(),
            ),
            (
                "layout_mapping_status".to_string(),
                report.layout_status.as_str().to_string(),
            ),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_statistics_mapping(format: OutputFormat) -> ExitCode {
    let report = if shardloom_vortex::typed_vortex_statistics_mapping_available() {
        VortexStatisticsMappingReport::implemented("vortex::statistics::<public_api>")
    } else {
        VortexStatisticsMappingReport::deferred_api_unclear()
    };
    emit(
        "vortex-statistics-mapping",
        format,
        CommandStatus::Success,
        "vortex statistics mapping".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_statistics_mapping".to_string()),
            (
                "upstream_vortex_dependency".to_string(),
                "linked".to_string(),
            ),
            ("actual_io".to_string(), "not_implemented".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("segment_stats_available".to_string(), "true".to_string()),
            (
                "statistics_mapping_status".to_string(),
                report.status.as_str().to_string(),
            ),
        ],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_vortex_file_metadata_open(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_arg) = args.next() else {
        eprintln!("usage: shardloom vortex-file-metadata-open <dataset_uri>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(err) => {
            return emit_error(
                "vortex-file-metadata-open",
                format,
                "vortex file metadata open failed",
                &err,
            );
        }
    };
    let request = VortexMetadataOpenRequest::metadata_only(uri);
    let report = match open_vortex_metadata_only(request) {
        Ok(report) => report,
        Err(err) => {
            return emit_error(
                "vortex-file-metadata-open",
                format,
                "vortex file metadata open failed",
                &err,
            );
        }
    };
    let status = if report.has_errors() {
        CommandStatus::Error
    } else {
        CommandStatus::Success
    };
    emit(
        "vortex-file-metadata-open",
        format,
        status,
        "vortex file metadata-only open".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            ("mode".to_string(), "vortex_file_metadata_open".to_string()),
            ("metadata_only".to_string(), "true".to_string()),
            (
                "file_io_feature_enabled".to_string(),
                vortex_file_io_feature_enabled().to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "file_io_performed".to_string(),
                report.file_io_performed.to_string(),
            ),
            (
                "data_io_performed".to_string(),
                report.data_io_performed.to_string(),
            ),
            (
                "object_store_io_performed".to_string(),
                report.object_store_io_performed.to_string(),
            ),
            (
                "write_io_performed".to_string(),
                report.write_io_performed.to_string(),
            ),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    if matches!(status, CommandStatus::Error) {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_metadata_summary(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_text) = args.next() else {
        return emit_error(
            "vortex-metadata-summary",
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
                "vortex-metadata-summary",
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            );
        }
    };
    let probe = probe_vortex_metadata_only(uri)
        .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
    let report = summarize_vortex_metadata_probe(&probe);
    emit(
        "vortex-metadata-summary",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata summary".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_metadata_summary".to_string()),
            (
                "metadata_summary_plan_only".to_string(),
                metadata_summary_is_plan_only(&report).to_string(),
            ),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_query_primitive_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(primitive_arg) = args.next() else {
        return emit_error(
            "vortex-query-primitive-plan",
            format,
            "missing primitive",
            &ShardLoomError::InvalidOperation("missing required argument: <primitive>".to_string()),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            "vortex-query-primitive-plan",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let primitive = match primitive_arg.as_str() {
        "count" => shardloom_vortex::VortexQueryPrimitiveBoundaryKind::Count,
        "filtered-count" | "filtered_count" => {
            shardloom_vortex::VortexQueryPrimitiveBoundaryKind::FilteredCount
        }
        "projection" => shardloom_vortex::VortexQueryPrimitiveBoundaryKind::Projection,
        "predicate-filter" | "predicate_filter" => {
            shardloom_vortex::VortexQueryPrimitiveBoundaryKind::PredicateFilter
        }
        _ => {
            return emit_error(
                "vortex-query-primitive-plan",
                format,
                "invalid primitive",
                &ShardLoomError::InvalidOperation(format!("invalid primitive: {primitive_arg}")),
            );
        }
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-query-primitive-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let mut request = shardloom_vortex::VortexQueryPrimitiveBoundaryRequest::new(uri, primitive);
    for token in args {
        match token.as_str() {
            "--feature-gate" => {
                request.add_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled);
            }
            "--metadata-footer-ready" => {
                request.add_signal(VortexQueryPrimitiveSignal::MetadataFooterReady);
            }
            "--encoded-data-path-ready" => {
                request.add_signal(VortexQueryPrimitiveSignal::EncodedDataPathReady);
            }
            "--predicate-provided" => {
                request.add_signal(VortexQueryPrimitiveSignal::PredicateProvided);
            }
            "--projection-provided" => {
                request.add_signal(VortexQueryPrimitiveSignal::ProjectionProvided);
            }
            "--predicate-unsupported" => {
                request.add_signal(VortexQueryPrimitiveSignal::PredicateUnsupported);
            }
            "--object-store-target" => {
                request.add_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget);
            }
            "--decode-risk" => request.add_signal(VortexQueryPrimitiveSignal::DecodeRisk),
            "--materialization-risk" => {
                request.add_signal(VortexQueryPrimitiveSignal::MaterializationRisk);
            }
            "--arrow-default-risk" => {
                request.add_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk);
            }
            "--write-risk" => request.add_signal(VortexQueryPrimitiveSignal::WriteRisk),
            "--scan-execution-risk" => {
                request.add_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk);
            }
            "--fallback-policy-blocked" => {
                request.add_signal(VortexQueryPrimitiveSignal::FallbackPolicyBlocked);
            }
            "--format" => {}
            _ => {
                return emit_error(
                    "vortex-query-primitive-plan",
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                );
            }
        }
    }
    let report = match plan_vortex_query_primitive(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-query-primitive-plan",
                format,
                "query primitive planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-query-primitive-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex query primitive planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "primitive".to_string(),
                report.request.primitive.as_str().to_string(),
            ),
            ("query_executed".to_string(), "false".to_string()),
            (
                "primitive_ready".to_string(),
                report.status.primitive_ready().to_string(),
            ),
            (
                "feature_gate_enabled".to_string(),
                report
                    .request
                    .signals
                    .contains(&VortexQueryPrimitiveSignal::FeatureGateEnabled)
                    .to_string(),
            ),
            (
                "metadata_footer_ready".to_string(),
                report
                    .request
                    .signals
                    .contains(&VortexQueryPrimitiveSignal::MetadataFooterReady)
                    .to_string(),
            ),
            (
                "encoded_data_path_ready".to_string(),
                report
                    .request
                    .signals
                    .contains(&VortexQueryPrimitiveSignal::EncodedDataPathReady)
                    .to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("encoded_data_read".to_string(), "false".to_string()),
            ("row_read".to_string(), "false".to_string()),
            ("array_decoded".to_string(), "false".to_string()),
            ("values_materialized".to_string(), "false".to_string()),
            ("arrow_converted".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("data_written".to_string(), "false".to_string()),
            ("upstream_scan_called".to_string(), "false".to_string()),
            ("status".to_string(), report.status.as_str().to_string()),
            ("mode".to_string(), report.mode.as_str().to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
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

fn plan_vortex_execution_readiness(
    command: &str,
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
    error_context: &str,
) -> std::result::Result<(u64, usize, VortexExecutionReadinessReport), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return Err(emit_error(
                command,
                format,
                error_context,
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            ));
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return Err(emit_error(
                command,
                format,
                error_context,
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            ));
        }
    };
    let source = match UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
        return Err(ExitCode::from(1));
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let sizing_report = match shardloom_vortex::size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let memory_report = match shardloom_vortex::plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    let scheduler_report =
        match shardloom_vortex::plan_vortex_scheduler_queue(memory_report, max_parallelism) {
            Ok(v) => v,
            Err(error) => return Err(emit_error(command, format, error_context, &error)),
        };
    let readiness_report = match evaluate_vortex_execution_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => return Err(emit_error(command, format, error_context, &error)),
    };
    Ok((memory_gb, max_parallelism, readiness_report))
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
