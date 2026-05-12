//! Vortex primitive execution CLI handlers.
//!
//! This module starts the physical split for Vortex primitive command handlers
//! while preserving existing no-fallback execution contracts. This slice only
//! owns `vortex-count`, `vortex-count-where`, `vortex-project`,
//! `vortex-filter`, `vortex-filter-project`, `vortex-run`,
//! `vortex-local-exec`, `vortex-bounded-local-exec`, and
//! `vortex-query-trace`; broader non-primitive handler extraction remains
//! staged.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetUri, ExecutionCertificate, NativeIoCertificate, OutputFormat,
    PredicateExpr, ShardLoomError,
};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexBoundedExecutionPolicy, VortexLocalEngineReport, VortexLocalEngineRequest,
    VortexLocalEngineWhyReport, VortexMetadataOpenRequest, VortexMetadataProbeReport,
    VortexQueryPrimitiveRequest, VortexQueryPrimitiveValue, VortexWorkAvoidedReport,
    evaluate_vortex_query_primitive, evaluate_vortex_query_primitive_with_analysis,
    execute_vortex_bounded_local_query, execute_vortex_local_query_primitive,
    local_primitive_execution_certificate, local_primitive_native_io_certificate,
    open_vortex_metadata_only, parse_vortex_local_engine_primitive, run_vortex_local_engine,
    summarize_vortex_metadata_probe,
};

use crate::cli_output::{emit, emit_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VortexCountExecutionRequest {
    MetadataOnly,
    LocalEncodedCount {
        memory_gb: u64,
        max_parallelism: usize,
    },
}

pub(crate) fn parse_vortex_count_args(
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, VortexCountExecutionRequest), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> [--execute-local-encoded-count <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(dataset_uri).map_err(|_| ExitCode::from(2))?;
    let Some(option) = args.next() else {
        return Ok((uri, VortexCountExecutionRequest::MetadataOnly));
    };
    if option != "--execute-local-encoded-count" {
        eprintln!("unknown option for shardloom vortex-count: {option}");
        return Err(ExitCode::from(2));
    }
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    if let Some(extra) = args.next() {
        eprintln!("unknown extra argument for shardloom vortex-count: {extra}");
        return Err(ExitCode::from(2));
    }
    let memory_gb = memory_gb_text.parse().map_err(|_| ExitCode::from(2))?;
    let max_parallelism = max_parallelism_text
        .parse()
        .map_err(|_| ExitCode::from(2))?;
    Ok((
        uri,
        VortexCountExecutionRequest::LocalEncodedCount {
            memory_gb,
            max_parallelism,
        },
    ))
}

pub(crate) fn handle_vortex_count(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let (uri, execution_request) = match parse_vortex_count_args(args) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    match execution_request {
        VortexCountExecutionRequest::MetadataOnly => handle_vortex_count_metadata(uri, format),
        VortexCountExecutionRequest::LocalEncodedCount {
            memory_gb,
            max_parallelism,
        } => handle_vortex_count_local_encoded(uri, memory_gb, max_parallelism, format),
    }
}

pub(crate) fn handle_vortex_query_trace(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_arg) = args.next() else {
        eprintln!("usage: shardloom vortex-query-trace <dataset_uri> <primitive>");
        return ExitCode::from(2);
    };
    let Some(primitive_arg) = args.next() else {
        eprintln!("usage: shardloom vortex-query-trace <dataset_uri> <primitive>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error("vortex-query-trace", format, "query trace failed", &error);
        }
    };
    let request = match crate::parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("vortex-query-trace", format, "query trace failed", &error);
        }
    };
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary)
        .unwrap_or_else(|| {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        });
    let analysis = match evaluate_vortex_query_primitive_with_analysis(request, &summary) {
        Ok(v) => v,
        Err(error) => {
            return emit_error("vortex-query-trace", format, "query trace failed", &error);
        }
    };
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_query_trace".to_string()),
        ("primitive".to_string(), primitive_arg),
        ("data_read".to_string(), "false".to_string()),
        ("data_decoded".to_string(), "false".to_string()),
        ("data_materialized".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("spill_io_performed".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        (
            "decision_trace_entries".to_string(),
            analysis.decision_trace.entry_count().to_string(),
        ),
        (
            "result_known".to_string(),
            analysis.result.value.is_known().to_string(),
        ),
    ];
    crate::append_vortex_work_avoided_fields(&mut fields, Some(&analysis.work_avoided));
    emit(
        "vortex-query-trace",
        format,
        if analysis.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex query trace primitive analysis".to_string(),
        analysis.to_human_text(),
        analysis.result.diagnostics.clone(),
        fields,
    );
    if analysis.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_count_where(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let (uri, predicate_arg, predicate, local_execution_request) =
        match parse_vortex_count_where_args(args, format) {
            Ok(parsed) => parsed,
            Err(code) => return code,
        };
    let request = VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate.clone());
    let open = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri));
    let summary = if let Ok(report) = open {
        report.metadata_summary.unwrap_or_else(|| {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        })
    } else {
        summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
    };
    let result = match evaluate_vortex_query_primitive(request.clone(), &summary) {
        Ok(result) => result,
        Err(error) => {
            return emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            );
        }
    };
    let local_execution = match local_execution_request.as_ref() {
        Some(local_request) => {
            match crate::vortex_count_where_local_execution_evidence(&request, local_request) {
                Ok(evidence) => Some(evidence),
                Err(error) => {
                    return emit_error(
                        "vortex-count-where",
                        format,
                        "vortex count where local primitive execution failed",
                        &error,
                    );
                }
            }
        }
        None => None,
    };
    let evidence = match crate::vortex_count_where_filter_evidence(&predicate, &summary) {
        Ok(evidence) => evidence,
        Err(error) => {
            return emit_error(
                "vortex-count-where",
                format,
                "vortex count where filter evidence failed",
                &error,
            );
        }
    };
    let command_has_errors = local_execution.as_ref().map_or_else(
        || result.has_errors(),
        crate::VortexCountWhereLocalExecutionEvidence::has_errors,
    );
    let status = if command_has_errors {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    let metadata_count = match result.value {
        VortexQueryPrimitiveValue::Count(v) => Some(v),
        _ => None,
    };
    let count = local_execution
        .as_ref()
        .and_then(crate::VortexCountWhereLocalExecutionEvidence::count)
        .or(metadata_count);
    let mut diagnostics = result.diagnostics.clone();
    if let Some(local) = &local_execution {
        diagnostics.extend(local.report.diagnostics.clone());
        diagnostics.extend(local.native_io_certificate.diagnostics.clone());
        if let Some(certificate) = &local.execution_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
    }
    emit(
        "vortex-count-where",
        format,
        status,
        "vortex count where primitive".to_string(),
        crate::vortex_count_where_human_text(&result, &evidence, local_execution.as_ref()),
        diagnostics,
        crate::vortex_count_where_fields(
            &result,
            count,
            predicate_arg,
            &evidence,
            local_execution.as_ref(),
        ),
    );
    if command_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_project(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-project <dataset_uri> <columns> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return ExitCode::from(2);
    };
    let Some(columns_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-project <dataset_uri> <columns> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error("vortex-project", format, "vortex project failed", &error);
        }
    };
    let projection = match crate::parse_projection_columns(&columns_arg) {
        Ok(projection) => projection,
        Err(error) => {
            return emit_error("vortex-project", format, "vortex project failed", &error);
        }
    };
    let local_execution_request =
        match crate::parse_vortex_local_primitive_cli_execution_args(&mut args) {
            Ok(request) => request,
            Err(error) => {
                return emit_error("vortex-project", format, "vortex project failed", &error);
            }
        };
    let request = VortexQueryPrimitiveRequest::project(uri.clone(), projection);
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary)
        .unwrap_or_else(|| {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        });
    let result = match evaluate_vortex_query_primitive(request.clone(), &summary) {
        Ok(result) => result,
        Err(error) => {
            return emit_error("vortex-project", format, "vortex project failed", &error);
        }
    };
    let local_execution = match local_execution_request.as_ref() {
        Some(local_request) => {
            match crate::vortex_local_primitive_cli_execution_evidence(&request, local_request) {
                Ok(evidence) => Some(evidence),
                Err(error) => {
                    return emit_error(
                        "vortex-project",
                        format,
                        "vortex project local primitive execution failed",
                        &error,
                    );
                }
            }
        }
        None => None,
    };
    let command_has_errors = local_execution.as_ref().map_or_else(
        || result.has_errors(),
        crate::VortexLocalPrimitiveCliExecutionEvidence::has_errors,
    );
    let mut diagnostics = result.diagnostics.clone();
    if let Some(local) = &local_execution {
        diagnostics.extend(local.report.diagnostics.clone());
        diagnostics.extend(local.native_io_certificate.diagnostics.clone());
        if let Some(certificate) = &local.execution_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
    }
    emit(
        "vortex-project",
        format,
        if command_has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex project primitive".to_string(),
        crate::vortex_project_human_text(&result, local_execution.as_ref()),
        diagnostics,
        crate::vortex_project_fields(&result, columns_arg, local_execution.as_ref()),
    );
    if command_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

struct VortexFilterProjectArgs {
    uri: DatasetUri,
    predicate_arg: String,
    columns_arg: String,
    predicate: PredicateExpr,
    projection: ProjectionRequest,
    local_execution_request: Option<crate::VortexLocalPrimitiveCliExecutionRequest>,
}

struct VortexFilterArgs {
    uri: DatasetUri,
    predicate_arg: String,
    predicate: PredicateExpr,
    local_execution_request: Option<crate::VortexLocalPrimitiveCliExecutionRequest>,
}

struct VortexBoundedLocalExecArgs {
    uri: DatasetUri,
    primitive_arg: String,
    request: VortexQueryPrimitiveRequest,
    memory_gb: u64,
    max_parallelism: usize,
}

pub(crate) fn handle_vortex_filter_project(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_vortex_filter_project_args(args, format) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    let VortexFilterProjectArgs {
        uri,
        predicate_arg,
        columns_arg,
        predicate,
        projection,
        local_execution_request,
    } = parsed;
    let request =
        VortexQueryPrimitiveRequest::filter_and_project(uri.clone(), predicate, projection);
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary)
        .unwrap_or_else(|| {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        });
    let result = match evaluate_vortex_query_primitive(request.clone(), &summary) {
        Ok(result) => result,
        Err(error) => {
            return emit_error(
                "vortex-filter-project",
                format,
                "vortex filter project failed",
                &error,
            );
        }
    };
    let local_execution = match vortex_filter_project_local_execution(
        &request,
        local_execution_request.as_ref(),
        format,
    ) {
        Ok(local_execution) => local_execution,
        Err(code) => return code,
    };
    let command_has_errors = local_execution.as_ref().map_or_else(
        || result.has_errors(),
        crate::VortexLocalPrimitiveCliExecutionEvidence::has_errors,
    );
    let mut diagnostics = result.diagnostics.clone();
    if let Some(local) = &local_execution {
        diagnostics.extend(local.report.diagnostics.clone());
        diagnostics.extend(local.native_io_certificate.diagnostics.clone());
        if let Some(certificate) = &local.execution_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
    }
    emit(
        "vortex-filter-project",
        format,
        if command_has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex filter project primitive".to_string(),
        crate::vortex_filter_project_human_text(&result, local_execution.as_ref()),
        diagnostics,
        crate::vortex_filter_project_fields(
            &result,
            predicate_arg,
            columns_arg,
            local_execution.as_ref(),
        ),
    );
    if command_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_filter(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_vortex_filter_args(args, format) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    let VortexFilterArgs {
        uri,
        predicate_arg,
        predicate,
        local_execution_request,
    } = parsed;
    let request = VortexQueryPrimitiveRequest::filter(uri.clone(), predicate);
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary)
        .unwrap_or_else(|| {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        });
    let result = match evaluate_vortex_query_primitive(request.clone(), &summary) {
        Ok(result) => result,
        Err(error) => {
            return emit_error("vortex-filter", format, "vortex filter failed", &error);
        }
    };
    let local_execution =
        match vortex_filter_local_execution(&request, local_execution_request.as_ref(), format) {
            Ok(local_execution) => local_execution,
            Err(code) => return code,
        };
    let command_has_errors = local_execution.as_ref().map_or_else(
        || result.has_errors(),
        crate::VortexLocalPrimitiveCliExecutionEvidence::has_errors,
    );
    let mut diagnostics = result.diagnostics.clone();
    if let Some(local) = &local_execution {
        diagnostics.extend(local.report.diagnostics.clone());
        diagnostics.extend(local.native_io_certificate.diagnostics.clone());
        if let Some(certificate) = &local.execution_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
    }
    emit(
        "vortex-filter",
        format,
        if command_has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex filter primitive".to_string(),
        crate::vortex_filter_human_text(&result, local_execution.as_ref()),
        diagnostics,
        crate::vortex_filter_fields(&result, predicate_arg, local_execution.as_ref()),
    );
    if command_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_local_exec(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(uri_arg) = args.next() else {
        eprintln!("usage: shardloom vortex-local-exec <dataset_uri> <primitive>");
        return ExitCode::from(2);
    };
    let Some(primitive_arg) = args.next() else {
        eprintln!("usage: shardloom vortex-local-exec <dataset_uri> <primitive>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-local-exec",
                format,
                "vortex local exec failed",
                &error,
            );
        }
    };
    let request = match crate::parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-local-exec",
                format,
                "vortex local exec failed",
                &error,
            );
        }
    };
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary);
    let report = match execute_vortex_local_query_primitive(request, summary) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-local-exec",
                format,
                "vortex local exec failed",
                &error,
            );
        }
    };
    emit(
        "vortex-local-exec",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local execution loop skeleton".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_local_exec".to_string()),
            ("primitive".to_string(), primitive_arg),
            ("tasks_executed".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "metadata_only_or_not_performed".to_string(),
            ),
            (
                "result_known".to_string(),
                report.value.is_known().to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_bounded_local_exec(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_vortex_bounded_local_exec_args(args, format) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    let VortexBoundedLocalExecArgs {
        uri,
        primitive_arg,
        request,
        memory_gb,
        max_parallelism,
    } = parsed;
    let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
        .ok()
        .and_then(|report| report.metadata_summary);
    let local = match execute_vortex_local_query_primitive(request, summary) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-bounded-local-exec",
                format,
                "vortex bounded local exec failed",
                &error,
            );
        }
    };
    let policy = match VortexBoundedExecutionPolicy::memory_limited(memory_gb, max_parallelism) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-bounded-local-exec",
                format,
                "vortex bounded local exec failed",
                &error,
            );
        }
    };
    let report = match execute_vortex_bounded_local_query(local, policy) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-bounded-local-exec",
                format,
                "vortex bounded local exec failed",
                &error,
            );
        }
    };
    emit(
        "vortex-bounded-local-exec",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex bounded local execution".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        crate::bounded_local_execution_fields(&report, &primitive_arg, memory_gb, max_parallelism),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_run(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_vortex_run_args(args, format) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    let VortexRunArgs {
        primitive_arg,
        memory_gb,
        max_parallelism,
        request,
    } = parsed;
    let report = match run_vortex_local_engine(request) {
        Ok(v) => v,
        Err(error) => return emit_error("vortex-run", format, "vortex run failed", &error),
    };
    let runtime_work_avoided = report.runtime_work_avoided_report();
    let mut why_report = report.why_report();
    let certificates = match vortex_run_certificates(&report, format) {
        Ok(certificates) => certificates,
        Err(code) => return code,
    };
    crate::reconcile_vortex_local_engine_why_with_execution_certificate(
        &mut why_report,
        certificates.execution.as_ref(),
    );
    let fields = vortex_run_fields(
        &report,
        &VortexRunFieldContext {
            primitive_arg: &primitive_arg,
            memory_gb,
            max_parallelism,
            runtime_work_avoided: &runtime_work_avoided,
            certificates: &certificates,
            why_report: &why_report,
        },
    );
    emit(
        "vortex-run",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local engine surface".to_string(),
        vortex_run_human_text(&report, &why_report, certificates.execution.as_ref()),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

struct VortexRunArgs {
    primitive_arg: String,
    memory_gb: u64,
    max_parallelism: usize,
    request: VortexLocalEngineRequest,
}

struct VortexRunCertificates {
    native_io: Option<NativeIoCertificate>,
    execution: Option<ExecutionCertificate>,
}

struct VortexRunFieldContext<'a> {
    primitive_arg: &'a str,
    memory_gb: u64,
    max_parallelism: usize,
    runtime_work_avoided: &'a VortexWorkAvoidedReport,
    certificates: &'a VortexRunCertificates,
    why_report: &'a VortexLocalEngineWhyReport,
}

fn parse_vortex_run_args(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> std::result::Result<VortexRunArgs, ExitCode> {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(primitive_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(uri_arg)
        .map_err(|error| emit_error("vortex-run", format, "vortex run failed", &error))?;
    let primitive = parse_vortex_local_engine_primitive(&primitive_arg)
        .map_err(|error| emit_error("vortex-run", format, "vortex run failed", &error))?;
    let memory_gb = parse_vortex_run_memory_gb(&memory_gb_text, format)?;
    let max_parallelism = parse_vortex_run_max_parallelism(&max_parallelism_text, format)?;
    let request = VortexLocalEngineRequest::new(uri, primitive, memory_gb, max_parallelism)
        .map_err(|error| emit_error("vortex-run", format, "vortex run failed", &error))?;
    Ok(VortexRunArgs {
        primitive_arg,
        memory_gb,
        max_parallelism,
        request,
    })
}

fn parse_vortex_run_memory_gb(
    text: &str,
    format: OutputFormat,
) -> std::result::Result<u64, ExitCode> {
    text.parse().map_err(|_| {
        emit_error(
            "vortex-run",
            format,
            "vortex run failed",
            &ShardLoomError::InvalidOperation("memory_gb must be an unsigned integer".to_string()),
        )
    })
}

fn parse_vortex_run_max_parallelism(
    text: &str,
    format: OutputFormat,
) -> std::result::Result<usize, ExitCode> {
    text.parse().map_err(|_| {
        emit_error(
            "vortex-run",
            format,
            "vortex run failed",
            &ShardLoomError::InvalidOperation(
                "max_parallelism must be an unsigned integer".to_string(),
            ),
        )
    })
}

fn vortex_run_certificates(
    report: &VortexLocalEngineReport,
    format: OutputFormat,
) -> std::result::Result<VortexRunCertificates, ExitCode> {
    let native_io = match (
        report.query_request.as_ref(),
        report.local_primitive_execution_report.as_ref(),
    ) {
        (Some(query_request), Some(local_report)) => Some(
            local_primitive_native_io_certificate(query_request, local_report).map_err(
                |error| {
                    emit_error(
                        "vortex-run",
                        format,
                        "vortex local primitive native I/O certificate failed",
                        &error,
                    )
                },
            )?,
        ),
        _ => None,
    };
    let execution = match (
        report.query_request.as_ref(),
        report.local_primitive_execution_report.as_ref(),
    ) {
        (Some(query_request), Some(local_report)) => {
            vortex_run_execution_certificate(query_request, local_report, format)?
        }
        _ => None,
    };
    Ok(VortexRunCertificates {
        native_io,
        execution,
    })
}

fn vortex_run_execution_certificate(
    query_request: &VortexQueryPrimitiveRequest,
    local_report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    format: OutputFormat,
) -> std::result::Result<Option<ExecutionCertificate>, ExitCode> {
    let Some(fixture) =
        crate::local_primitive_correctness_fixture_for_request(query_request, local_report)
    else {
        return Ok(None);
    };
    local_primitive_execution_certificate(&fixture, query_request, local_report)
        .map(Some)
        .map_err(|error| {
            emit_error(
                "vortex-run",
                format,
                "vortex local primitive execution certificate failed",
                &error,
            )
        })
}

fn vortex_run_fields(
    report: &VortexLocalEngineReport,
    context: &VortexRunFieldContext<'_>,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_vortex_run_identity_fields(
        &mut fields,
        context.primitive_arg,
        context.memory_gb,
        context.max_parallelism,
    );
    append_vortex_run_metadata_fields(&mut fields, report);
    append_vortex_run_effect_fields(&mut fields, report);
    append_vortex_run_local_primitive_fields(&mut fields, report);
    fields.push((
        "execution".to_string(),
        if report.data_read {
            "local_vortex_primitive_performed".to_string()
        } else {
            "metadata_only_or_not_performed".to_string()
        },
    ));
    crate::append_vortex_work_avoided_fields(&mut fields, Some(context.runtime_work_avoided));
    crate::append_vortex_local_primitive_native_io_certificate_fields(
        &mut fields,
        context.certificates.native_io.as_ref(),
    );
    crate::append_vortex_local_primitive_execution_certificate_fields(
        &mut fields,
        context.certificates.execution.as_ref(),
    );
    crate::append_vortex_local_engine_why_fields(&mut fields, context.why_report);
    fields
}

fn append_vortex_run_identity_fields(
    fields: &mut Vec<(String, String)>,
    primitive_arg: &str,
    memory_gb: u64,
    max_parallelism: usize,
) {
    fields.extend([
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_run".to_string()),
        ("primitive".to_string(), primitive_arg.to_string()),
        ("memory_gb".to_string(), memory_gb.to_string()),
        ("max_parallelism".to_string(), max_parallelism.to_string()),
    ]);
}

fn append_vortex_run_metadata_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexLocalEngineReport,
) {
    fields.extend([
        (
            "metadata_open_report_present".to_string(),
            report.metadata_open_report.is_some().to_string(),
        ),
        (
            "metadata_open_status".to_string(),
            report.metadata_open_report.as_ref().map_or_else(
                || "none".to_string(),
                |open| open.open_status.as_str().to_string(),
            ),
        ),
        (
            "metadata_open_feature_enabled".to_string(),
            report.metadata_open_report.as_ref().map_or_else(
                || "false".to_string(),
                |open| open.feature_status.is_enabled().to_string(),
            ),
        ),
        (
            "file_io_performed".to_string(),
            report.metadata_open_report.as_ref().map_or_else(
                || "false".to_string(),
                |open| open.file_io_performed.to_string(),
            ),
        ),
    ]);
}

fn append_vortex_run_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexLocalEngineReport,
) {
    let row_read = report
        .local_primitive_execution_report
        .as_ref()
        .is_some_and(|local| local.row_read);
    let arrow_converted = report
        .local_primitive_execution_report
        .as_ref()
        .is_some_and(|local| local.arrow_converted);
    fields.extend([
        (
            "data_io_performed".to_string(),
            report.data_read.to_string(),
        ),
        (
            "object_store_io_performed".to_string(),
            report.object_store_io.to_string(),
        ),
        (
            "write_io_performed".to_string(),
            report.write_io.to_string(),
        ),
        ("result_known".to_string(), report.result_known.to_string()),
        (
            "tasks_executed".to_string(),
            report.tasks_executed.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        ("row_read".to_string(), row_read.to_string()),
        ("arrow_converted".to_string(), arrow_converted.to_string()),
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
            "external_effects_executed".to_string(),
            report.external_effects_executed.to_string(),
        ),
    ]);
}

fn append_vortex_run_local_primitive_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexLocalEngineReport,
) {
    let local = report.local_primitive_execution_report.as_ref();
    append_vortex_run_local_primitive_status_fields(fields, local);
    append_vortex_run_local_primitive_row_fields(fields, local);
    append_vortex_run_local_primitive_execution_fields(fields, local);
}

fn append_vortex_run_local_primitive_status_fields(
    fields: &mut Vec<(String, String)>,
    local: Option<&shardloom_vortex::VortexLocalPrimitiveExecutionReport>,
) {
    fields.extend([
        (
            "local_primitive_report_present".to_string(),
            local.is_some().to_string(),
        ),
        (
            "local_primitive_status".to_string(),
            local.map_or_else(
                || "none".to_string(),
                |local| local.status.as_str().to_string(),
            ),
        ),
        (
            "local_primitive_mode".to_string(),
            local.map_or_else(
                || "none".to_string(),
                |local| local.mode.as_str().to_string(),
            ),
        ),
    ]);
}

fn append_vortex_run_local_primitive_row_fields(
    fields: &mut Vec<(String, String)>,
    local: Option<&shardloom_vortex::VortexLocalPrimitiveExecutionReport>,
) {
    fields.extend([
        (
            "local_primitive_rows_scanned".to_string(),
            local.map_or_else(|| "0".to_string(), |local| local.rows_scanned.to_string()),
        ),
        (
            "local_primitive_rows_selected".to_string(),
            local
                .and_then(|local| local.rows_selected)
                .map_or_else(|| "none".to_string(), |rows| rows.to_string()),
        ),
        (
            "local_primitive_projected_columns".to_string(),
            local.map_or_else(String::new, |local| local.projected_columns.join(",")),
        ),
        (
            "local_primitive_arrays_read_count".to_string(),
            local.map_or_else(
                || "0".to_string(),
                |local| local.arrays_read_count.to_string(),
            ),
        ),
        (
            "local_primitive_max_chunk_rows".to_string(),
            local.map_or_else(|| "0".to_string(), |local| local.max_chunk_rows.to_string()),
        ),
    ]);
}

fn append_vortex_run_local_primitive_execution_fields(
    fields: &mut Vec<(String, String)>,
    local: Option<&shardloom_vortex::VortexLocalPrimitiveExecutionReport>,
) {
    fields.extend([
        (
            "local_primitive_streaming_scan_used".to_string(),
            local
                .is_some_and(|local| local.streaming_scan_used)
                .to_string(),
        ),
        (
            "local_primitive_full_stream_collected".to_string(),
            local
                .is_some_and(|local| local.full_stream_collected)
                .to_string(),
        ),
        (
            "local_primitive_max_parallelism_requested".to_string(),
            local.map_or_else(
                || "0".to_string(),
                |local| local.max_parallelism_requested.to_string(),
            ),
        ),
        (
            "local_primitive_scan_concurrency_per_worker".to_string(),
            local.map_or_else(
                || "0".to_string(),
                |local| local.scan_concurrency_per_worker.to_string(),
            ),
        ),
        (
            "local_primitive_filter_pushdown_applied".to_string(),
            local
                .is_some_and(|local| local.filter_pushdown_applied)
                .to_string(),
        ),
        (
            "local_primitive_projection_pushdown_applied".to_string(),
            local
                .is_some_and(|local| local.projection_pushdown_applied)
                .to_string(),
        ),
        (
            "local_primitive_upstream_filter_expression_used".to_string(),
            local
                .is_some_and(|local| local.upstream_filter_expression_used)
                .to_string(),
        ),
        (
            "local_primitive_upstream_projection_expression_used".to_string(),
            local
                .is_some_and(|local| local.upstream_projection_expression_used)
                .to_string(),
        ),
        (
            "local_primitive_materialization_boundary_reported".to_string(),
            local
                .is_some_and(|local| local.materialization_boundary_reported)
                .to_string(),
        ),
    ]);
}

fn vortex_run_human_text(
    report: &VortexLocalEngineReport,
    why_report: &VortexLocalEngineWhyReport,
    execution_certificate: Option<&ExecutionCertificate>,
) -> String {
    let certificate_text = execution_certificate.map_or_else(String::new, |certificate| {
        format!("\n\n{}", certificate.to_human_text())
    });
    format!(
        "{}\n{}{}",
        report.to_human_text(),
        why_report.to_human_text(),
        certificate_text
    )
}

fn parse_vortex_filter_project_args(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> std::result::Result<VortexFilterProjectArgs, ExitCode> {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(predicate_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(columns_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(uri_arg).map_err(|error| {
        emit_error(
            "vortex-filter-project",
            format,
            "vortex filter project failed",
            &error,
        )
    })?;
    let predicate = crate::parse_tiny_predicate(&predicate_arg).map_err(|error| {
        emit_error(
            "vortex-filter-project",
            format,
            "vortex filter project failed",
            &error,
        )
    })?;
    let projection = crate::parse_projection_columns(&columns_arg).map_err(|error| {
        emit_error(
            "vortex-filter-project",
            format,
            "vortex filter project failed",
            &error,
        )
    })?;
    let local_execution_request = crate::parse_vortex_local_primitive_cli_execution_args(&mut args)
        .map_err(|error| {
            emit_error(
                "vortex-filter-project",
                format,
                "vortex filter project failed",
                &error,
            )
        })?;
    Ok(VortexFilterProjectArgs {
        uri,
        predicate_arg,
        columns_arg,
        predicate,
        projection,
        local_execution_request,
    })
}

fn vortex_filter_project_local_execution(
    request: &VortexQueryPrimitiveRequest,
    local_execution_request: Option<&crate::VortexLocalPrimitiveCliExecutionRequest>,
    format: OutputFormat,
) -> std::result::Result<Option<crate::VortexLocalPrimitiveCliExecutionEvidence>, ExitCode> {
    let Some(local_request) = local_execution_request else {
        return Ok(None);
    };
    crate::vortex_local_primitive_cli_execution_evidence(request, local_request)
        .map(Some)
        .map_err(|error| {
            emit_error(
                "vortex-filter-project",
                format,
                "vortex filter project local primitive execution failed",
                &error,
            )
        })
}

fn parse_vortex_filter_args(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> std::result::Result<VortexFilterArgs, ExitCode> {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter <dataset_uri> <predicate> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(predicate_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter <dataset_uri> <predicate> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(uri_arg)
        .map_err(|error| emit_error("vortex-filter", format, "vortex filter failed", &error))?;
    let predicate = crate::parse_tiny_predicate(&predicate_arg)
        .map_err(|error| emit_error("vortex-filter", format, "vortex filter failed", &error))?;
    let local_execution_request = crate::parse_vortex_local_primitive_cli_execution_args(&mut args)
        .map_err(|error| emit_error("vortex-filter", format, "vortex filter failed", &error))?;
    Ok(VortexFilterArgs {
        uri,
        predicate_arg,
        predicate,
        local_execution_request,
    })
}

fn parse_vortex_bounded_local_exec_args(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> std::result::Result<VortexBoundedLocalExecArgs, ExitCode> {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(primitive_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(uri_arg).map_err(|error| {
        emit_error(
            "vortex-bounded-local-exec",
            format,
            "vortex bounded local exec failed",
            &error,
        )
    })?;
    let request =
        crate::parse_vortex_primitive_request(uri.clone(), &primitive_arg).map_err(|error| {
            emit_error(
                "vortex-bounded-local-exec",
                format,
                "vortex bounded local exec failed",
                &error,
            )
        })?;
    let memory_gb = parse_bounded_local_u64(&memory_gb_text, "memory_gb", format)?;
    let max_parallelism =
        parse_bounded_local_usize(&max_parallelism_text, "max_parallelism", format)?;
    Ok(VortexBoundedLocalExecArgs {
        uri,
        primitive_arg,
        request,
        memory_gb,
        max_parallelism,
    })
}

fn parse_bounded_local_u64(
    value: &str,
    field: &str,
    format: OutputFormat,
) -> std::result::Result<u64, ExitCode> {
    value.parse().map_err(|_| {
        emit_error(
            "vortex-bounded-local-exec",
            format,
            "vortex bounded local exec failed",
            &ShardLoomError::InvalidOperation(format!("{field} must be an unsigned integer")),
        )
    })
}

fn parse_bounded_local_usize(
    value: &str,
    field: &str,
    format: OutputFormat,
) -> std::result::Result<usize, ExitCode> {
    value.parse().map_err(|_| {
        emit_error(
            "vortex-bounded-local-exec",
            format,
            "vortex bounded local exec failed",
            &ShardLoomError::InvalidOperation(format!("{field} must be an unsigned integer")),
        )
    })
}

fn vortex_filter_local_execution(
    request: &VortexQueryPrimitiveRequest,
    local_execution_request: Option<&crate::VortexLocalPrimitiveCliExecutionRequest>,
    format: OutputFormat,
) -> std::result::Result<Option<crate::VortexLocalPrimitiveCliExecutionEvidence>, ExitCode> {
    let Some(local_request) = local_execution_request else {
        return Ok(None);
    };
    crate::vortex_local_primitive_cli_execution_evidence(request, local_request)
        .map(Some)
        .map_err(|error| {
            emit_error(
                "vortex-filter",
                format,
                "vortex filter local primitive execution failed",
                &error,
            )
        })
}

fn parse_vortex_count_where_args(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> std::result::Result<
    (
        DatasetUri,
        String,
        PredicateExpr,
        Option<crate::VortexCountWhereLocalExecutionRequest>,
    ),
    ExitCode,
> {
    let Some(uri_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count-where <dataset_uri> <predicate> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(predicate_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count-where <dataset_uri> <predicate> [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            );
            return Err(ExitCode::from(1));
        }
    };
    let predicate = match crate::parse_tiny_predicate(&predicate_arg) {
        Ok(predicate) => predicate,
        Err(error) => {
            emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            );
            return Err(ExitCode::from(1));
        }
    };
    let local_execution_request = match crate::parse_vortex_count_where_local_execution_args(args) {
        Ok(request) => request,
        Err(error) => {
            emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            );
            return Err(ExitCode::from(1));
        }
    };
    Ok((uri, predicate_arg, predicate, local_execution_request))
}

fn handle_vortex_count_metadata(uri: DatasetUri, format: OutputFormat) -> ExitCode {
    let request = VortexQueryPrimitiveRequest::count_all(uri.clone());
    let open = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri));
    let summary = if let Ok(report) = open {
        if let Some(summary) = report.metadata_summary {
            summary
        } else if report.has_errors() {
            let mut degraded =
                summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear());
            degraded.diagnostics.extend(report.diagnostics.clone());
            degraded
        } else {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
        }
    } else {
        summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
    };
    let result = match evaluate_vortex_query_primitive(request, &summary) {
        Ok(result) => result,
        Err(error) => {
            return emit_error("vortex-count", format, "vortex count failed", &error);
        }
    };
    let count = match result.value {
        VortexQueryPrimitiveValue::Count(v) => Some(v),
        _ => None,
    };
    let status = if result.has_errors() || count.is_none() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "vortex-count",
        format,
        status,
        "vortex count primitive".to_string(),
        result.to_human_text(),
        result.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_count".to_string()),
            ("primitive".to_string(), "count_all".to_string()),
            (
                "explicit_local_encoded_count_requested".to_string(),
                "false".to_string(),
            ),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "metadata_only_or_not_performed".to_string(),
            ),
            ("result_known".to_string(), count.is_some().to_string()),
            (
                "count".to_string(),
                count.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
            ),
        ],
    );
    if result.has_errors() || count.is_none() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn handle_vortex_count_local_encoded(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
    format: OutputFormat,
) -> ExitCode {
    let (encoded_report, local_report) = match crate::run_vortex_approved_local_encoded_count(
        uri.clone(),
        memory_gb,
        max_parallelism,
    ) {
        Ok(reports) => reports,
        Err(error) => {
            return emit_error("vortex-count", format, "vortex count failed", &error);
        }
    };
    let streaming_plan =
        match crate::build_vortex_count_local_streaming_batch_plan(uri, memory_gb, max_parallelism)
        {
            Ok(report) => report,
            Err(error) => {
                return emit_error(
                    "vortex-count",
                    format,
                    "vortex streaming-batch runtime evidence failed",
                    &error,
                );
            }
        };
    let streaming_report =
        shardloom_vortex::execute_vortex_streaming_batches_from_local_encoded_count(
            streaming_plan,
            encoded_report.clone(),
        );
    let local_execution_failed = local_report.has_errors();
    let evidence = match crate::vortex_count_local_encoded_evidence(&encoded_report, &local_report)
    {
        Ok(evidence) => evidence,
        Err(error) => {
            return emit_error(
                "vortex-count",
                format,
                "vortex count evidence failed",
                &error,
            );
        }
    };
    let mut diagnostics = encoded_report.diagnostics.clone();
    diagnostics.extend(local_report.diagnostics.clone());
    diagnostics.extend(streaming_report.diagnostics.clone());
    diagnostics.extend(evidence.diagnostics());
    let mut human_sections = vec![encoded_report.to_human_text(), local_report.to_human_text()];
    human_sections.push(streaming_report.to_human_text());
    human_sections.extend(evidence.human_sections());
    let human_text = human_sections.join("\n\n");
    let fields = crate::vortex_count_local_encoded_fields(
        memory_gb,
        max_parallelism,
        &encoded_report,
        &local_report,
        &streaming_report,
        &evidence,
    );
    emit(
        "vortex-count",
        format,
        if encoded_report.has_errors()
            || local_execution_failed
            || streaming_report.has_errors()
            || evidence.has_errors()
        {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local encoded count execution".to_string(),
        human_text,
        diagnostics,
        fields,
    );
    if encoded_report.has_errors()
        || local_execution_failed
        || streaming_report.has_errors()
        || evidence.has_errors()
    {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
