//! Vortex metadata and report-only planning CLI handlers.
//!
//! These handlers expose Vortex metadata, pruning, probe, and API-inventory
//! planning surfaces. They remain metadata/report-only and do not execute
//! tasks, read data beyond explicit metadata probe contracts, materialize
//! outputs, write data, invoke external engines, or allow fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, CommandStatus,
    CompatibilityOutputWriterMatrixReport, DatasetUri, OperatorMemoryCertification, OutputFormat,
    OutputTarget, PhysicalOperatorKind, PredicateExpr, ShardLoomError, TranslationPlan,
    UniversalInputSource,
};
use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};
use shardloom_vortex::{
    Vortex075HeavyOperatorProviderDispositionReport, Vortex075LocalIoProviderDispositionReport,
    VortexAdapterCapabilityReport, VortexAdapterReadiness, VortexComputeProviderReport,
    VortexCountCandidateSource, VortexCountReadinessSignal, VortexDTypeMappingReport,
    VortexEncodedExecutionPathSelectionReport, VortexEncodingLayoutMappingReport,
    VortexExecutionReadinessReport, VortexFileRef, VortexFilteredCountCandidateSource,
    VortexFilteredCountReadinessSignal, VortexGeneralizedEncodedPrimitiveGateReport,
    VortexLayoutReaderDriverApprovalInput, VortexLayoutReaderDriverApprovalSignal,
    VortexLocalIoCoverageReport, VortexMetadataCountKernelAdmissionReport,
    VortexMetadataFilterKernelAdmissionReport, VortexMetadataOpenRequest,
    VortexMetadataProbeReport, VortexNativeWriterSchemaCertificationReport,
    VortexObjectStoreIoGateReport, VortexProjectionCandidateSource,
    VortexProjectionReadinessSignal, VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult,
    VortexQueryPrimitiveSignal, VortexQueryPrimitiveValue, VortexReadPlan,
    VortexScanCompatibilityReport, VortexStatisticsMappingReport, VortexWriteOptions,
    VortexWritePlan, admit_vortex_metadata_count_kernel, admit_vortex_metadata_filter_kernel,
    build_vortex_runtime_task_graph, evaluate_vortex_execution_readiness,
    evaluate_vortex_metadata_physical_kernels,
    execute_vortex_count_all_from_encoded_count_data_path_approval, execute_vortex_metadata_only,
    metadata_planning_is_side_effect_free, metadata_pruning_is_side_effect_free,
    metadata_summary_is_plan_only, open_vortex_metadata_only, plan_from_vortex_metadata_summary,
    plan_native_vortex_universal_input, plan_vortex_count_readiness,
    plan_vortex_encoded_count_data_path_approval,
    plan_vortex_encoded_count_data_path_approval_with_layout_driver,
    plan_vortex_encoded_execution_path_selection, plan_vortex_filtered_count_readiness,
    plan_vortex_generalized_encoded_primitive_gate, plan_vortex_layout_reader_driver_approval,
    plan_vortex_metadata_pruning, plan_vortex_projection_readiness, plan_vortex_query_primitive,
    plan_vortex_query_primitive_result_physical_operators_with_evidence,
    plan_vortex_read_from_universal_input, plan_vortex_scan_compatibility,
    probe_vortex_metadata_only, summarize_vortex_metadata_probe,
    vortex_encoded_read_public_api_boundary, vortex_file_io_feature_enabled,
    vortex_metadata_executor_feature_enabled,
};

use crate::{
    cli_missing_arg_error,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, cli_unknown_signal_error,
};

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn safe_metadata_kernel_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[must_use]
fn metadata_kernel_memory_safe(memory: OperatorMemoryCertification) -> bool {
    memory.oom_safe
        && !memory.requires_full_materialization
        && (memory.streaming || memory.bounded_memory || memory.spillable)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn run_vortex_metadata_physical_kernel_plan(
    format: OutputFormat,
    args: Vec<String>,
) -> ExitCode {
    let command = "vortex-metadata-physical-kernel-plan";
    let mut args = args.into_iter();
    let Some(primitive_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing primitive",
            &cli_missing_arg_error(command, "primitive"),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing dataset uri",
            &cli_missing_arg_error(command, "dataset_uri"),
        );
    };
    let Some(value_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing metadata value",
            &cli_missing_arg_error(command, "metadata_value"),
        );
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => return emit_error(command, format, "invalid dataset uri", &error),
    };
    let (request, value) = match primitive_arg.as_str() {
        "count" | "count-all" | "count_all" => {
            let Ok(count) = value_arg.parse::<u64>() else {
                return emit_error(
                    command,
                    format,
                    "invalid count metadata value",
                    &ShardLoomError::InvalidOperation(format!(
                        "count metadata value must be u64: {value_arg}"
                    )),
                );
            };
            (
                VortexQueryPrimitiveRequest::count_all(uri),
                VortexQueryPrimitiveValue::Count(count),
            )
        }
        "filtered-count" | "filtered_count" => {
            let Ok(count) = value_arg.parse::<u64>() else {
                return emit_error(
                    command,
                    format,
                    "invalid filtered count metadata value",
                    &ShardLoomError::InvalidOperation(format!(
                        "filtered count metadata value must be u64: {value_arg}"
                    )),
                );
            };
            (
                VortexQueryPrimitiveRequest::count_where(uri, PredicateExpr::AlwaysTrue),
                VortexQueryPrimitiveValue::Count(count),
            )
        }
        "filter" | "predicate-filter" | "predicate_filter" => {
            let value = match value_arg.as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return emit_error(
                        command,
                        format,
                        "invalid filter metadata value",
                        &ShardLoomError::InvalidOperation(format!(
                            "filter metadata value must be true or false: {value_arg}"
                        )),
                    );
                }
            };
            (
                VortexQueryPrimitiveRequest::filter(uri, PredicateExpr::AlwaysFalse),
                VortexQueryPrimitiveValue::Boolean(value),
            )
        }
        _ => {
            return emit_error(
                command,
                format,
                "invalid primitive",
                &ShardLoomError::InvalidOperation(format!("invalid primitive: {primitive_arg}")),
            );
        }
    };
    let mut correctness_evidence = BenchmarkEvidenceState::Missing;
    let mut benchmark_evidence = BenchmarkEvidenceState::Missing;
    let mut memory = OperatorMemoryCertification::unsupported();
    let mut fallback = BenchmarkFallbackState::NotAttempted;
    for token in args {
        match token.as_str() {
            "--correctness-evidence" | "--correctness-passed" => {
                correctness_evidence = BenchmarkEvidenceState::Present;
            }
            "--benchmark-evidence" | "--benchmark-passed" => {
                benchmark_evidence = BenchmarkEvidenceState::Present;
            }
            "--memory-safe" => {
                memory = safe_metadata_kernel_memory();
            }
            "--fallback-attempted" => {
                fallback = BenchmarkFallbackState::Attempted;
            }
            _ => {
                return emit_error(
                    command,
                    format,
                    "unknown option",
                    &cli_unknown_arg_error(command, &token),
                );
            }
        }
    }
    let result = VortexQueryPrimitiveResult::metadata_answered(request, value);
    let bridge = match plan_vortex_query_primitive_result_physical_operators_with_evidence(
        &result,
        correctness_evidence,
        benchmark_evidence,
        memory,
        fallback,
    ) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(command, format, "physical bridge planning failed", &error);
        }
    };
    let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);
    let count_admission = if matches!(
        report.primitive_kind,
        shardloom_vortex::VortexQueryPrimitiveKind::CountAll
            | shardloom_vortex::VortexQueryPrimitiveKind::CountWhere
    ) {
        match admit_vortex_metadata_count_kernel(&report) {
            Ok(admission) => Some(admission),
            Err(error) => {
                return emit_error(command, format, "count kernel admission failed", &error);
            }
        }
    } else {
        None
    };
    let filter_admission =
        if report.primitive_kind == shardloom_vortex::VortexQueryPrimitiveKind::FilterPredicate {
            match admit_vortex_metadata_filter_kernel(&report) {
                Ok(admission) => Some(admission),
                Err(error) => {
                    return emit_error(command, format, "filter kernel admission failed", &error);
                }
            }
        } else {
            None
        };
    let report_has_errors = report.has_errors()
        || count_admission
            .as_ref()
            .is_some_and(VortexMetadataCountKernelAdmissionReport::has_errors)
        || filter_admission
            .as_ref()
            .is_some_and(VortexMetadataFilterKernelAdmissionReport::has_errors);
    let mut diagnostics = report.diagnostics.clone();
    if let Some(count_admission) = &count_admission {
        diagnostics.extend(count_admission.diagnostics.clone());
    }
    if let Some(filter_admission) = &filter_admission {
        diagnostics.extend(filter_admission.diagnostics.clone());
    }
    let mut fields = vec![
        (
            "primitive".to_string(),
            report.primitive_kind.as_str().to_string(),
        ),
        ("status".to_string(), report.status.as_str().to_string()),
        (
            "certificate_status".to_string(),
            report.certificate_status.as_str().to_string(),
        ),
        (
            "metadata_kernel_count".to_string(),
            report.metadata_kernel_count.to_string(),
        ),
        (
            "kernel_kind".to_string(),
            report.kernel_kind.as_str().to_string(),
        ),
        ("value".to_string(), report.value.as_str()),
        (
            "correctness_evidence".to_string(),
            correctness_evidence.as_str().to_string(),
        ),
        (
            "benchmark_evidence".to_string(),
            benchmark_evidence.as_str().to_string(),
        ),
        (
            "memory_safe".to_string(),
            metadata_kernel_memory_safe(memory).to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            fallback.attempted().to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
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
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
    ];
    if let Some(count_admission) = &count_admission {
        append_metadata_count_kernel_admission_fields(&mut fields, count_admission);
    }
    if let Some(filter_admission) = &filter_admission {
        append_metadata_filter_kernel_admission_fields(&mut fields, filter_admission);
    }
    emit(
        command,
        format,
        if report_has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata physical kernel report".to_string(),
        report.to_human_text(),
        diagnostics,
        fields,
    );
    if report_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn parse_vortex_layout_driver_approval_signals(
    signals_raw: &str,
) -> Result<Vec<VortexLayoutReaderDriverApprovalSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "layout driver approval signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "layout driver approval signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "local-fixture-only" => VortexLayoutReaderDriverApprovalSignal::LocalFixtureOnly,
            "caller-session-allowed" => {
                VortexLayoutReaderDriverApprovalSignal::CallerSessionAllowed
            }
            "runtime-driver-start-allowed" => {
                VortexLayoutReaderDriverApprovalSignal::RuntimeDriverStartAllowed
            }
            "layout-row-count-only-intent" => {
                VortexLayoutReaderDriverApprovalSignal::LayoutRowCountOnlyIntent
            }
            "scan-forbidden" => VortexLayoutReaderDriverApprovalSignal::ScanForbidden,
            "evaluation-forbidden" => VortexLayoutReaderDriverApprovalSignal::EvaluationForbidden,
            "data-read-forbidden" => VortexLayoutReaderDriverApprovalSignal::DataReadForbidden,
            "decode-forbidden" => VortexLayoutReaderDriverApprovalSignal::DecodeForbidden,
            "materialization-forbidden" => {
                VortexLayoutReaderDriverApprovalSignal::MaterializationForbidden
            }
            "arrow-forbidden" => VortexLayoutReaderDriverApprovalSignal::ArrowForbidden,
            "object-store-forbidden" => {
                VortexLayoutReaderDriverApprovalSignal::ObjectStoreForbidden
            }
            "write-forbidden" => VortexLayoutReaderDriverApprovalSignal::WriteForbidden,
            "fallback-forbidden" => VortexLayoutReaderDriverApprovalSignal::FallbackForbidden,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-layout-driver-approval-plan",
                    "layout-driver-approval",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

pub(crate) fn vortex_projection_readiness_fields(
    report: &shardloom_vortex::VortexProjectionReadinessReport,
) -> Vec<(String, String)> {
    vec![
        (
            "candidate_source".to_string(),
            report.request.candidate_source.as_str().to_string(),
        ),
        ("status".to_string(), report.status.as_str().to_string()),
        ("mode".to_string(), report.mode.as_str().to_string()),
        (
            "projection_ready".to_string(),
            report.projection_ready().to_string(),
        ),
        (
            "projection_executed".to_string(),
            report.projection_executed().to_string(),
        ),
        (
            "projection_applied".to_string(),
            report.projection_applied().to_string(),
        ),
        (
            "feature_gate_enabled".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::FeatureGateEnabled)
                .to_string(),
        ),
        (
            "query_primitive_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady)
                .to_string(),
        ),
        (
            "metadata_footer_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::MetadataFooterReady)
                .to_string(),
        ),
        (
            "encoded_data_path_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::EncodedDataPathReady)
                .to_string(),
        ),
        (
            "projection_primitive".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionPrimitive)
                .to_string(),
        ),
        (
            "projection_provided".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionProvided)
                .to_string(),
        ),
        (
            "projection_supported".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionSupported)
                .to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("metadata_read".to_string(), "false".to_string()),
        ("encoded_data_read".to_string(), "false".to_string()),
        ("row_read".to_string(), "false".to_string()),
        ("array_decoded".to_string(), "false".to_string()),
        ("values_materialized".to_string(), "false".to_string()),
        ("arrow_converted".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("data_written".to_string(), "false".to_string()),
        ("upstream_scan_called".to_string(), "false".to_string()),
    ]
}

fn append_metadata_filter_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    filter_admission: &VortexMetadataFilterKernelAdmissionReport,
) {
    push_bool_field(fields, "metadata_filter_kernel_admission_emitted", true);
    push_field(
        fields,
        "metadata_filter_kernel_admission_schema_version",
        filter_admission.schema_version,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_id",
        &filter_admission.admission_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_kernel_report_id",
        &filter_admission.metadata_kernel_report_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_slot_id",
        &filter_admission.slot_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_operator_kind",
        filter_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_required_kernel_kind",
        filter_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_candidate_kernel_kind",
        filter_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_status",
        filter_admission.status.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_correctness_evidence",
        filter_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_benchmark_evidence",
        filter_admission.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_streaming",
        filter_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_bounded",
        filter_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_oom_safe",
        filter_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_full_materialization",
        filter_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_fallback_state",
        filter_admission.fallback.as_str(),
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_slot_marked_present",
        filter_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_production_claim_allowed",
        filter_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_runtime_execution",
        filter_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_fallback_execution_allowed",
        filter_admission.fallback_execution_allowed,
    );
}

fn append_metadata_count_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(fields, "metadata_count_kernel_admission_emitted", true);
    push_field(
        fields,
        "metadata_count_kernel_admission_schema_version",
        count_admission.schema_version,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_id",
        &count_admission.admission_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_kernel_report_id",
        &count_admission.metadata_kernel_report_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_primitive_kind",
        count_admission.primitive_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_slot_id",
        &count_admission.slot_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_operator_kind",
        count_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_required_kernel_kind",
        count_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_candidate_kernel_kind",
        count_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_status",
        count_admission.status.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_correctness_evidence",
        count_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_benchmark_evidence",
        count_admission.benchmark_evidence.as_str(),
    );
    append_metadata_count_kernel_admission_resource_fields(fields, count_admission);
    append_metadata_count_kernel_admission_outcome_fields(fields, count_admission);
}

fn append_metadata_count_kernel_admission_resource_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_streaming",
        count_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_bounded",
        count_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_oom_safe",
        count_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_full_materialization",
        count_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_fallback_state",
        count_admission.fallback.as_str(),
    );
}

fn append_metadata_count_kernel_admission_outcome_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_slot_marked_present",
        count_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_production_claim_allowed",
        count_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_runtime_execution",
        count_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_fallback_execution_allowed",
        count_admission.fallback_execution_allowed,
    );
}

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

fn vortex_api_inventory_fields(
    scan_report: &VortexScanCompatibilityReport,
    provider_report: &VortexComputeProviderReport,
    local_io_report: &VortexLocalIoCoverageReport,
    writer_schema_certification: &VortexNativeWriterSchemaCertificationReport,
    object_store_io_gate: &VortexObjectStoreIoGateReport,
    heavy_operator_disposition: &Vortex075HeavyOperatorProviderDispositionReport,
    local_io_provider_disposition: &Vortex075LocalIoProviderDispositionReport,
) -> Vec<(String, String)> {
    let mut fields = vortex_api_inventory_base_fields();
    fields.extend(vortex_api_inventory_report_fields(
        scan_report,
        provider_report,
    ));
    fields.extend(vortex_source_split_identity_fields(
        scan_report,
        provider_report,
    ));
    fields.extend(vortex_source_split_pushdown_fields(scan_report));
    fields.extend(vortex_source_split_evidence_fields(scan_report));
    fields.extend(vortex_source_split_effect_fields(scan_report));
    fields.extend(vortex_segment_extraction_admission_fields(scan_report));
    fields.extend(vortex_local_io_coverage_fields(local_io_report));
    fields.extend(vortex_native_writer_schema_certification_fields(
        writer_schema_certification,
    ));
    fields.extend(vortex_object_store_io_gate_fields(object_store_io_gate));
    fields.extend(vortex075_heavy_operator_disposition_fields(
        heavy_operator_disposition,
    ));
    fields.extend(vortex075_local_io_provider_disposition_fields(
        local_io_provider_disposition,
    ));
    fields
}

fn vortex_api_inventory_base_fields() -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("mode".to_string(), "vortex_api_inventory".to_string()),
        (
            "upstream_vortex_dependency".to_string(),
            "linked".to_string(),
        ),
        ("actual_io".to_string(), "not_implemented".to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("table_catalog_io".to_string(), "false".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
        ("data_read".to_string(), "false".to_string()),
        ("data_decoded".to_string(), "false".to_string()),
        ("data_materialized".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
    ]
}

fn vortex_api_inventory_report_fields(
    scan_report: &VortexScanCompatibilityReport,
    provider_report: &VortexComputeProviderReport,
) -> Vec<(String, String)> {
    vec![
        (
            "vortex_scan_compatibility_schema_version".to_string(),
            scan_report.schema_version.to_string(),
        ),
        (
            "vortex_scan_compatibility_report_id".to_string(),
            scan_report.report_id.to_string(),
        ),
        (
            "vortex_compute_provider_schema_version".to_string(),
            provider_report.schema_version.to_string(),
        ),
        (
            "vortex_compute_provider_report_id".to_string(),
            provider_report.report_id.to_string(),
        ),
    ]
}

fn vortex_source_split_identity_fields(
    scan_report: &VortexScanCompatibilityReport,
    provider_report: &VortexComputeProviderReport,
) -> Vec<(String, String)> {
    let proof = scan_report.source_split_runtime_admission_proof;
    vec![
        (
            "vortex_source_split_admission_schema_version".to_string(),
            proof.schema_version.to_string(),
        ),
        (
            "vortex_source_split_admission_proof_id".to_string(),
            proof.proof_id.to_string(),
        ),
        (
            "vortex_source_split_admission_path_id".to_string(),
            proof.path_id.to_string(),
        ),
        (
            "vortex_source_split_admission_selected_path_status".to_string(),
            proof.selected_path_status.as_str().to_string(),
        ),
        (
            "vortex_source_split_admission_generalized_runtime_status".to_string(),
            proof
                .generalized_runtime_admission_status
                .as_str()
                .to_string(),
        ),
        (
            "vortex_source_split_admission_provider_kind".to_string(),
            proof.provider_kind.to_string(),
        ),
        (
            "vortex_source_split_admission_provider_kind_confirmed".to_string(),
            provider_report.provider_kind.as_str().to_string(),
        ),
        (
            "vortex_source_split_admission_provider_crate".to_string(),
            proof.provider_crate.to_string(),
        ),
        (
            "vortex_source_split_admission_provider_version".to_string(),
            proof.provider_version.to_string(),
        ),
        (
            "vortex_source_split_admission_feature_gate".to_string(),
            proof.feature_gate.to_string(),
        ),
        (
            "vortex_source_split_admission_provider_api_surface".to_string(),
            proof.provider_api_surface.to_string(),
        ),
        (
            "vortex_source_split_admission_policy".to_string(),
            proof.shardloom_admission_policy.to_string(),
        ),
        (
            "vortex_source_split_admission_source_surface".to_string(),
            proof.source_surface.to_string(),
        ),
        (
            "vortex_source_split_admission_split_surface".to_string(),
            proof.split_surface.to_string(),
        ),
    ]
}

fn vortex_source_split_pushdown_fields(
    scan_report: &VortexScanCompatibilityReport,
) -> Vec<(String, String)> {
    let proof = scan_report.source_split_runtime_admission_proof;
    vec![
        (
            "vortex_source_split_admission_split_ref_status".to_string(),
            proof.split_ref_status.to_string(),
        ),
        (
            "vortex_source_split_admission_split_estimate_status".to_string(),
            proof.split_estimate_status.to_string(),
        ),
        (
            "vortex_source_split_admission_split_serialization_status".to_string(),
            proof.split_serialization_status.to_string(),
        ),
        (
            "vortex_source_split_admission_field_mask_status".to_string(),
            proof.field_mask_status.to_string(),
        ),
        (
            "vortex_source_split_admission_predicate_ordering_status".to_string(),
            proof.predicate_ordering_status.to_string(),
        ),
        (
            "vortex_source_split_admission_projection_pushdown_status".to_string(),
            proof.projection_pushdown_status.to_string(),
        ),
        (
            "vortex_source_split_admission_filter_pushdown_status".to_string(),
            proof.filter_pushdown_status.to_string(),
        ),
        (
            "vortex_source_split_admission_limit_pushdown_status".to_string(),
            proof.limit_pushdown_status.to_string(),
        ),
        (
            "vortex_source_split_admission_residual_executor".to_string(),
            proof.residual_executor.as_str().to_string(),
        ),
        (
            "vortex_source_split_admission_generalized_residual_executor".to_string(),
            proof.generalized_residual_executor.as_str().to_string(),
        ),
    ]
}

fn vortex_source_split_evidence_fields(
    scan_report: &VortexScanCompatibilityReport,
) -> Vec<(String, String)> {
    let proof = scan_report.source_split_runtime_admission_proof;
    vec![
        (
            "vortex_source_split_admission_correctness_refs".to_string(),
            proof.correctness_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_benchmark_refs".to_string(),
            proof.benchmark_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_execution_certificate_refs".to_string(),
            proof.execution_certificate_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_native_io_certificate_refs".to_string(),
            proof.native_io_certificate_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_predicate_ordering_refs".to_string(),
            proof.predicate_ordering_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_policy_refs".to_string(),
            proof.policy_refs.to_string(),
        ),
        (
            "vortex_source_split_admission_unsupported_diagnostic_code".to_string(),
            proof.unsupported_diagnostic_code.to_string(),
        ),
        (
            "vortex_source_split_admission_blocker_id".to_string(),
            proof.blocker_id.to_string(),
        ),
        (
            "vortex_source_split_admission_required_future_evidence".to_string(),
            proof.required_future_evidence.to_string(),
        ),
        (
            "vortex_source_split_admission_claim_gate_status".to_string(),
            proof.claim_gate_status.to_string(),
        ),
        (
            "vortex_source_split_admission_claim_boundary".to_string(),
            proof.claim_boundary.to_string(),
        ),
    ]
}

fn vortex_source_split_effect_fields(
    scan_report: &VortexScanCompatibilityReport,
) -> Vec<(String, String)> {
    let proof = scan_report.source_split_runtime_admission_proof;
    vec![
        (
            "vortex_source_split_admission_runtime_execution".to_string(),
            proof.runtime_execution.to_string(),
        ),
        (
            "vortex_source_split_admission_object_store_io".to_string(),
            proof.object_store_io.to_string(),
        ),
        (
            "vortex_source_split_admission_table_catalog_io".to_string(),
            proof.table_catalog_io.to_string(),
        ),
        (
            "vortex_source_split_admission_write_io".to_string(),
            proof.write_io.to_string(),
        ),
        (
            "vortex_source_split_admission_external_engine_invoked".to_string(),
            proof.external_engine_invoked.to_string(),
        ),
        (
            "vortex_source_split_admission_fallback_attempted".to_string(),
            proof.fallback_attempted.to_string(),
        ),
    ]
}

fn vortex_segment_extraction_admission_fields(
    scan_report: &VortexScanCompatibilityReport,
) -> Vec<(String, String)> {
    let report = &scan_report.segment_extraction_admission_report;
    vec![
        (
            "vortex_segment_extraction_admission_schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "vortex_segment_extraction_admission_report_id".to_string(),
            report.report_id.to_string(),
        ),
        (
            "vortex_segment_extraction_selected_layout_family".to_string(),
            report.selected_layout_family.to_string(),
        ),
        (
            "vortex_segment_extraction_selected_layout_status".to_string(),
            report.selected_layout_status.as_str().to_string(),
        ),
        (
            "vortex_segment_extraction_row_count".to_string(),
            report.rows.len().to_string(),
        ),
        (
            "vortex_segment_extraction_row_order".to_string(),
            report.row_order.join(","),
        ),
        (
            "vortex_segment_extraction_supported_layout_count".to_string(),
            report.supported_layout_count().to_string(),
        ),
        (
            "vortex_segment_extraction_blocked_layout_count".to_string(),
            report.blocked_layout_count().to_string(),
        ),
        (
            "vortex_segment_extraction_unsupported_diagnostic_codes".to_string(),
            report.unsupported_diagnostic_codes().join(","),
        ),
        (
            "vortex_segment_extraction_blocker_ids".to_string(),
            report.blocker_ids().join(","),
        ),
        (
            "vortex_segment_extraction_required_evidence".to_string(),
            report.required_evidence.to_string(),
        ),
        (
            "vortex_segment_extraction_required_future_evidence".to_string(),
            report.required_future_evidence().join(";"),
        ),
        (
            "vortex_segment_extraction_claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "vortex_segment_extraction_claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
        (
            "vortex_segment_extraction_runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        (
            "vortex_segment_extraction_data_read".to_string(),
            report.data_read.to_string(),
        ),
        (
            "vortex_segment_extraction_data_decoded".to_string(),
            report.data_decoded.to_string(),
        ),
        (
            "vortex_segment_extraction_data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        (
            "vortex_segment_extraction_object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        (
            "vortex_segment_extraction_table_catalog_io".to_string(),
            report.table_catalog_io.to_string(),
        ),
        (
            "vortex_segment_extraction_write_io".to_string(),
            report.write_io.to_string(),
        ),
        (
            "vortex_segment_extraction_external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "vortex_segment_extraction_fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn vortex_local_io_coverage_fields(report: &VortexLocalIoCoverageReport) -> Vec<(String, String)> {
    let reader = report
        .rows
        .iter()
        .find(|row| row.lane_id == report.selected_reader_lane);
    let writer = report
        .rows
        .iter()
        .find(|row| row.lane_id == report.selected_writer_lane);
    let broad_writer = report
        .rows
        .iter()
        .find(|row| row.lane_id == "general_local_schema_encoding_writer");
    let mut fields = vec![
        (
            "vortex_local_io_schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "vortex_local_io_report_id".to_string(),
            report.report_id.to_string(),
        ),
        (
            "vortex_local_io_gar_id".to_string(),
            report.gar_id.to_string(),
        ),
        (
            "vortex_local_io_selected_reader_lane".to_string(),
            report.selected_reader_lane.to_string(),
        ),
        (
            "vortex_local_io_selected_writer_lane".to_string(),
            report.selected_writer_lane.to_string(),
        ),
        (
            "vortex_local_io_row_order".to_string(),
            report.row_order().join(","),
        ),
        (
            "vortex_local_io_runtime_lane_count".to_string(),
            report.runtime_lane_count().to_string(),
        ),
        (
            "vortex_local_io_blocked_lane_count".to_string(),
            report.blocked_lane_count().to_string(),
        ),
        (
            "vortex_local_io_runtime_lane_ids".to_string(),
            report.runtime_lane_ids().join(","),
        ),
        (
            "vortex_local_io_blocked_lane_ids".to_string(),
            report.blocked_lane_ids().join(","),
        ),
        (
            "vortex_local_io_claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "vortex_local_io_claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
        (
            "vortex_local_io_inventory_runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        (
            "vortex_local_io_inventory_data_read".to_string(),
            report.data_read.to_string(),
        ),
        (
            "vortex_local_io_inventory_data_written".to_string(),
            report.data_written.to_string(),
        ),
        (
            "vortex_local_io_object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        (
            "vortex_local_io_table_catalog_io".to_string(),
            report.table_catalog_io.to_string(),
        ),
        (
            "vortex_local_io_external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "vortex_local_io_fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
    ];
    if let Some(row) = reader {
        fields.extend([
            (
                "vortex_local_io_reader_status".to_string(),
                row.support_status.as_str().to_string(),
            ),
            (
                "vortex_local_io_reader_feature_gate".to_string(),
                row.feature_gate.to_string(),
            ),
            (
                "vortex_local_io_reader_surface".to_string(),
                row.user_surface.to_string(),
            ),
            (
                "vortex_local_io_reader_claim_boundary".to_string(),
                row.claim_boundary.to_string(),
            ),
        ]);
    }
    if let Some(row) = writer {
        fields.extend([
            (
                "vortex_local_io_writer_status".to_string(),
                row.support_status.as_str().to_string(),
            ),
            (
                "vortex_local_io_writer_feature_gate".to_string(),
                row.feature_gate.to_string(),
            ),
            (
                "vortex_local_io_writer_surface".to_string(),
                row.user_surface.to_string(),
            ),
            (
                "vortex_local_io_writer_claim_boundary".to_string(),
                row.claim_boundary.to_string(),
            ),
            (
                "vortex_local_io_writer_native_io_refs".to_string(),
                row.native_io_certificate_refs.to_string(),
            ),
            (
                "vortex_local_io_writer_upstream_api_surface".to_string(),
                row.upstream_api_surface.to_string(),
            ),
        ]);
    }
    if let Some(row) = broad_writer {
        fields.extend([
            (
                "vortex_local_io_broad_writer_status".to_string(),
                row.support_status.as_str().to_string(),
            ),
            (
                "vortex_local_io_broad_writer_blocker_id".to_string(),
                row.blocker_id.to_string(),
            ),
            (
                "vortex_local_io_broad_writer_required_future_evidence".to_string(),
                row.required_future_evidence.to_string(),
            ),
            (
                "vortex_local_io_broad_writer_claim_boundary".to_string(),
                row.claim_boundary.to_string(),
            ),
        ]);
    }
    fields
}

#[allow(clippy::too_many_lines)]
fn vortex_native_writer_schema_certification_fields(
    report: &VortexNativeWriterSchemaCertificationReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_schema_version",
        report.schema_version,
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_report_id",
        report.report_id,
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_row_order",
        &report.row_order().join(","),
    );
    push_count_field(
        &mut fields,
        "vortex_native_writer_schema_certification_scoped_runtime_row_count",
        report.scoped_runtime_row_count(),
    );
    push_count_field(
        &mut fields,
        "vortex_native_writer_schema_certification_provider_candidate_row_count",
        report.provider_candidate_row_count(),
    );
    push_count_field(
        &mut fields,
        "vortex_native_writer_schema_certification_blocked_row_count",
        report.blocked_row_count(),
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_scoped_runtime_row_ids",
        &report.scoped_runtime_row_ids().join(","),
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_provider_candidate_row_ids",
        &report.provider_candidate_row_ids().join(","),
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_blocked_row_ids",
        &report.blocked_row_ids().join(","),
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_claim_gate_status",
        report.claim_gate_status,
    );
    push_field(
        &mut fields,
        "vortex_native_writer_schema_certification_claim_boundary",
        report.claim_boundary,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_broad_schema_encoding_certification_complete",
        report.broad_schema_encoding_certification_complete,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_metadata_statistics_broadly_certified",
        report.metadata_statistics_broadly_certified,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_local_runtime_claim_allowed",
        report.local_runtime_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_object_store_io",
        report.object_store_io,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_table_catalog_io",
        report.table_catalog_io,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        &mut fields,
        "vortex_native_writer_schema_certification_no_external_fallback",
        report.no_external_fallback(),
    );
    for row in &report.rows {
        let row_prefix = format!(
            "vortex_native_writer_schema_certification_row_{}",
            row.row_id
        );
        fields.extend([
            (
                format!("{row_prefix}_writer_lane_id"),
                row.writer_lane_id.to_string(),
            ),
            (
                format!("{row_prefix}_status"),
                row.status.as_str().to_string(),
            ),
            (
                format!("{row_prefix}_feature_gate"),
                row.feature_gate.to_string(),
            ),
            (
                format!("{row_prefix}_provider_decision"),
                row.provider_decision.to_string(),
            ),
            (
                format!("{row_prefix}_provider_surface"),
                row.provider_surface.to_string(),
            ),
            (
                format!("{row_prefix}_schema_family"),
                row.schema_family.to_string(),
            ),
            (
                format!("{row_prefix}_dtype_scope"),
                row.dtype_scope.to_string(),
            ),
            (
                format!("{row_prefix}_validity_scope"),
                row.validity_scope.to_string(),
            ),
            (
                format!("{row_prefix}_encoding_scope"),
                row.encoding_scope.to_string(),
            ),
            (
                format!("{row_prefix}_metadata_preservation_status"),
                row.metadata_preservation_status.to_string(),
            ),
            (
                format!("{row_prefix}_statistics_preservation_status"),
                row.statistics_preservation_status.to_string(),
            ),
            (
                format!("{row_prefix}_materialization_boundary"),
                row.materialization_boundary.to_string(),
            ),
            (
                format!("{row_prefix}_replay_evidence"),
                row.replay_evidence.to_string(),
            ),
            (
                format!("{row_prefix}_unsupported_diagnostic_code"),
                row.unsupported_diagnostic_code.to_string(),
            ),
            (
                format!("{row_prefix}_required_future_evidence"),
                row.required_future_evidence.to_string(),
            ),
            (
                format!("{row_prefix}_claim_gate_status"),
                row.claim_gate_status.to_string(),
            ),
            (
                format!("{row_prefix}_claim_boundary"),
                row.claim_boundary.to_string(),
            ),
            (
                format!("{row_prefix}_local_write_runtime"),
                row.local_write_runtime.to_string(),
            ),
            (
                format!("{row_prefix}_reopen_verified"),
                row.reopen_verified.to_string(),
            ),
            (
                format!("{row_prefix}_metadata_statistics_broadly_certified"),
                row.metadata_statistics_broadly_certified.to_string(),
            ),
            (
                format!("{row_prefix}_object_store_io"),
                row.object_store_io.to_string(),
            ),
            (
                format!("{row_prefix}_table_catalog_io"),
                row.table_catalog_io.to_string(),
            ),
            (
                format!("{row_prefix}_external_engine_invoked"),
                row.external_engine_invoked.to_string(),
            ),
            (
                format!("{row_prefix}_fallback_attempted"),
                row.fallback_attempted.to_string(),
            ),
        ]);
    }
    fields
}

fn vortex_object_store_io_gate_fields(
    report: &VortexObjectStoreIoGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_vortex_object_store_io_gate_summary_fields(&mut fields, report);
    append_vortex_object_store_io_gate_requirement_fields(&mut fields, report);
    append_vortex_object_store_io_gate_effect_fields(&mut fields, report);
    append_vortex_object_store_io_gate_diagnostic_fields(&mut fields, report);
    for row in &report.rows {
        let row_prefix = format!("vortex_object_store_io_gate_row_{}", row.surface.as_str());
        fields.extend([
            (
                format!("{row_prefix}_status"),
                row.status.as_str().to_string(),
            ),
            (
                format!("{row_prefix}_user_surface"),
                row.user_surface.to_string(),
            ),
            (
                format!("{row_prefix}_upstream_api_surface"),
                row.upstream_api_surface.to_string(),
            ),
            (
                format!("{row_prefix}_required_evidence"),
                row.required_evidence.to_string(),
            ),
            (
                format!("{row_prefix}_claim_gate_status"),
                row.claim_gate_status.to_string(),
            ),
        ]);
    }
    fields
}

fn append_vortex_object_store_io_gate_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexObjectStoreIoGateReport,
) {
    push_field(
        fields,
        "vortex_object_store_io_gate_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_report_id",
        report.report_id,
    );
    push_field(fields, "vortex_object_store_io_gate_gar_id", report.gar_id);
    push_field(
        fields,
        "vortex_object_store_io_gate_status",
        report.gate_status,
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_support_status",
        report.support_status,
    );
    push_count_field(
        fields,
        "vortex_object_store_io_gate_row_count",
        report.rows.len(),
    );
    push_count_field(
        fields,
        "vortex_object_store_io_gate_unsupported_surface_count",
        report.unsupported_surface_count(),
    );
    push_count_field(
        fields,
        "vortex_object_store_io_gate_report_only_surface_count",
        report.report_only_surface_count(),
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_row_order",
        &report.row_order().join(","),
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_required_policy_refs",
        report.required_policy_refs,
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_claim_gate_status",
        report.claim_gate_status,
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_claim_boundary",
        report.claim_boundary,
    );
}

fn append_vortex_object_store_io_gate_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexObjectStoreIoGateReport,
) {
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_provider_capability_policy_required",
        report.provider_capability_policy_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_credential_policy_required",
        report.credential_policy_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_range_request_budget_required",
        report.range_request_budget_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_idempotency_key_required",
        report.idempotency_key_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_upstream_api_reference_required",
        report.upstream_api_reference_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
}

fn append_vortex_object_store_io_gate_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexObjectStoreIoGateReport,
) {
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_object_store_read_execution_allowed",
        report.object_store_read_execution_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_object_store_write_execution_allowed",
        report.object_store_write_execution_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_upstream_vortex_read_allowed",
        report.upstream_vortex_read_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_upstream_vortex_write_allowed",
        report.upstream_vortex_write_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_credential_resolution_allowed",
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_credentials_resolved",
        report.credentials_resolved,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_provider_probe",
        report.provider_probe,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_network_probe",
        report.network_probe,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_runtime_execution",
        report.runtime_execution,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_data_written",
        report.data_written,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_object_store_io",
        report.object_store_io,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_write_io",
        report.write_io,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_side_effect_free",
        report.side_effect_free(),
    );
}

fn append_vortex_object_store_io_gate_diagnostic_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexObjectStoreIoGateReport,
) {
    push_bool_field(
        fields,
        "vortex_object_store_io_gate_deterministic_unsupported_diagnostics_ready",
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_count_field(
        fields,
        "vortex_object_store_io_gate_unsupported_diagnostic_count",
        report.unsupported_diagnostic_count(),
    );
    push_field(
        fields,
        "vortex_object_store_io_gate_unsupported_diagnostic_code_order",
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_count_field(
        fields,
        "vortex_object_store_io_gate_diagnostic_count",
        report.diagnostics.len(),
    );
}

#[allow(clippy::too_many_lines)]
fn vortex075_heavy_operator_disposition_fields(
    report: &Vortex075HeavyOperatorProviderDispositionReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "vortex075_heavy_operator_schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "vortex075_heavy_operator_report_id".to_string(),
            report.report_id.to_string(),
        ),
        (
            "vortex075_heavy_operator_phase_id".to_string(),
            report.phase_id.to_string(),
        ),
        (
            "vortex075_heavy_operator_provider_version".to_string(),
            report.upstream_vortex_provider_version.to_string(),
        ),
        (
            "vortex075_heavy_operator_gate_status".to_string(),
            report.gate_status.to_string(),
        ),
        (
            "vortex075_heavy_operator_support_status".to_string(),
            report.support_status.to_string(),
        ),
        (
            "vortex075_heavy_operator_row_order".to_string(),
            report.row_order().join(","),
        ),
        (
            "vortex075_heavy_operator_provider_candidate_count".to_string(),
            report.provider_candidate_count().to_string(),
        ),
        (
            "vortex075_heavy_operator_wrapped_shardloom_kernel_count".to_string(),
            report.wrapped_shardloom_kernel_count().to_string(),
        ),
        (
            "vortex075_heavy_operator_blocked_external_integration_count".to_string(),
            report.blocked_external_integration_count().to_string(),
        ),
        (
            "vortex075_heavy_operator_claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "vortex075_heavy_operator_claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
        (
            "vortex075_heavy_operator_runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        (
            "vortex075_heavy_operator_data_read".to_string(),
            report.data_read.to_string(),
        ),
        (
            "vortex075_heavy_operator_data_decoded".to_string(),
            report.data_decoded.to_string(),
        ),
        (
            "vortex075_heavy_operator_data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        (
            "vortex075_heavy_operator_object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        (
            "vortex075_heavy_operator_write_io".to_string(),
            report.write_io.to_string(),
        ),
        (
            "vortex075_heavy_operator_external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "vortex075_heavy_operator_fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "vortex075_heavy_operator_fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "vortex075_heavy_operator_side_effect_free".to_string(),
            report.side_effect_free().to_string(),
        ),
        (
            "vortex075_heavy_operator_diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ];
    for row in &report.rows {
        let row_prefix = format!("vortex075_heavy_operator_row_{}", row.surface.as_str());
        fields.extend([
            (
                format!("{row_prefix}_status"),
                row.status.as_str().to_string(),
            ),
            (
                format!("{row_prefix}_operator_family"),
                row.operator_family.to_string(),
            ),
            (
                format!("{row_prefix}_upstream_api_surface"),
                row.upstream_api_surface.to_string(),
            ),
            (
                format!("{row_prefix}_shardloom_disposition"),
                row.shardloom_disposition.to_string(),
            ),
            (
                format!("{row_prefix}_required_evidence"),
                row.required_evidence.to_string(),
            ),
            (
                format!("{row_prefix}_provider_gate_required"),
                row.provider_gate_required.to_string(),
            ),
            (
                format!("{row_prefix}_decoded_reference_required"),
                row.decoded_reference_required.to_string(),
            ),
            (
                format!("{row_prefix}_claim_gate_status"),
                row.claim_gate_status.to_string(),
            ),
        ]);
    }
    fields
}

#[allow(clippy::too_many_lines)]
fn vortex075_local_io_provider_disposition_fields(
    report: &Vortex075LocalIoProviderDispositionReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(
        &mut fields,
        "vortex075_local_io_schema_version",
        report.schema_version,
    );
    push_field(
        &mut fields,
        "vortex075_local_io_report_id",
        report.report_id,
    );
    push_field(&mut fields, "vortex075_local_io_phase_id", report.phase_id);
    push_field(
        &mut fields,
        "vortex075_local_io_provider_version",
        report.upstream_vortex_provider_version,
    );
    push_field(
        &mut fields,
        "vortex075_local_io_gate_status",
        report.gate_status,
    );
    push_field(
        &mut fields,
        "vortex075_local_io_support_status",
        report.support_status,
    );
    push_field(
        &mut fields,
        "vortex075_local_io_row_order",
        &report.row_order().join(","),
    );
    push_count_field(
        &mut fields,
        "vortex075_local_io_provider_candidate_count",
        report.provider_candidate_count(),
    );
    push_count_field(
        &mut fields,
        "vortex075_local_io_blocked_future_device_count",
        report.blocked_future_device_count(),
    );
    push_count_field(
        &mut fields,
        "vortex075_local_io_deterministic_blocker_required_count",
        report.deterministic_blocker_required_count(),
    );
    push_field(
        &mut fields,
        "vortex075_local_io_claim_gate_status",
        report.claim_gate_status,
    );
    push_field(
        &mut fields,
        "vortex075_local_io_claim_boundary",
        report.claim_boundary,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_runtime_execution",
        report.runtime_execution,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_data_read",
        report.data_read,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_data_written",
        report.data_written,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_data_materialized",
        report.data_materialized,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_object_store_io",
        report.object_store_io,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_table_catalog_io",
        report.table_catalog_io,
    );
    push_bool_field(&mut fields, "vortex075_local_io_write_io", report.write_io);
    push_bool_field(
        &mut fields,
        "vortex075_local_io_external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "vortex075_local_io_side_effect_free",
        report.side_effect_free(),
    );
    push_count_field(
        &mut fields,
        "vortex075_local_io_diagnostic_count",
        report.diagnostics.len(),
    );
    for row in &report.rows {
        let row_prefix = format!("vortex075_local_io_row_{}", row.surface.as_str());
        fields.extend([
            (
                format!("{row_prefix}_status"),
                row.status.as_str().to_string(),
            ),
            (
                format!("{row_prefix}_format_family"),
                row.format_family.to_string(),
            ),
            (
                format!("{row_prefix}_upstream_api_surface"),
                row.upstream_api_surface.to_string(),
            ),
            (
                format!("{row_prefix}_shardloom_disposition"),
                row.shardloom_disposition.to_string(),
            ),
            (
                format!("{row_prefix}_required_evidence"),
                row.required_evidence.to_string(),
            ),
            (
                format!("{row_prefix}_provider_gate_required"),
                row.provider_gate_required.to_string(),
            ),
            (
                format!("{row_prefix}_fidelity_report_required"),
                row.fidelity_report_required.to_string(),
            ),
            (
                format!("{row_prefix}_deterministic_blocker_required"),
                row.deterministic_blocker_required.to_string(),
            ),
            (
                format!("{row_prefix}_native_io_certificate_required"),
                row.native_io_certificate_required.to_string(),
            ),
            (
                format!("{row_prefix}_claim_gate_status"),
                row.claim_gate_status.to_string(),
            ),
        ]);
    }
    fields
}

pub(crate) fn handle_vortex_api_inventory(format: OutputFormat) -> ExitCode {
    let report = VortexAdapterCapabilityReport::foundation();
    let scan_report = plan_vortex_scan_compatibility();
    let provider_report = VortexComputeProviderReport::local_scan_provider();
    let local_io_report = VortexLocalIoCoverageReport::current();
    let writer_schema_certification = VortexNativeWriterSchemaCertificationReport::current();
    let object_store_io_gate = VortexObjectStoreIoGateReport::current();
    let heavy_operator_disposition = Vortex075HeavyOperatorProviderDispositionReport::current();
    let local_io_provider_disposition = Vortex075LocalIoProviderDispositionReport::current();
    let mut diagnostics = report.diagnostics.clone();
    diagnostics.extend(object_store_io_gate.diagnostics.clone());
    diagnostics.extend(heavy_operator_disposition.diagnostics.clone());
    diagnostics.extend(local_io_provider_disposition.diagnostics.clone());
    emit(
        "vortex-api-inventory",
        format,
        CommandStatus::Success,
        "vortex API inventory".to_string(),
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            report.to_human_text(),
            scan_report
                .source_split_runtime_admission_proof
                .to_human_text(),
            scan_report
                .segment_extraction_admission_report
                .to_human_text(),
            local_io_report.to_human_text(),
            writer_schema_certification.to_human_text(),
            object_store_io_gate.to_human_text(),
            heavy_operator_disposition.to_human_text(),
            local_io_provider_disposition.to_human_text()
        ),
        diagnostics,
        vortex_api_inventory_fields(
            &scan_report,
            &provider_report,
            &local_io_report,
            &writer_schema_certification,
            &object_store_io_gate,
            &heavy_operator_disposition,
            &local_io_provider_disposition,
        ),
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
        if readiness_report.has_errors()
            || crate::vortex_runtime_planning::readiness_is_blocked(readiness_report.status)
        {
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
    if readiness_report.has_errors()
        || crate::vortex_runtime_planning::readiness_is_blocked(readiness_report.status)
    {
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
    let writer_matrix = CompatibilityOutputWriterMatrixReport::current();
    let text = format!(
        "{}\n{}",
        plan.to_human_text(),
        writer_matrix.to_human_text_for_target(&plan.target.kind)
    );
    emit(
        "translation-plan",
        format,
        if plan.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "translation plan".to_string(),
        text,
        plan.diagnostics.clone(),
        translation_plan_fields(&plan, &writer_matrix),
    );
    if plan.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
fn translation_plan_fields(
    plan: &TranslationPlan,
    writer_matrix: &CompatibilityOutputWriterMatrixReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "target_kind".to_string(),
            plan.target.kind.as_str().to_string(),
        ),
        (
            "target_output_mode".to_string(),
            plan.target.kind.canonical_label().to_string(),
        ),
        (
            "translation_status".to_string(),
            plan.status.as_str().to_string(),
        ),
        (
            "translation_fidelity".to_string(),
            plan.fidelity.as_str().to_string(),
        ),
        (
            "materialization_requirement".to_string(),
            plan.materialization.canonical_label().to_string(),
        ),
        (
            "compatibility_output_writer_matrix_schema".to_string(),
            writer_matrix.schema_version.clone(),
        ),
        (
            "compatibility_output_writer_matrix_report_id".to_string(),
            writer_matrix.report_id.clone(),
        ),
        (
            "compatibility_output_writer_row_count".to_string(),
            writer_matrix.rows.len().to_string(),
        ),
        (
            "compatibility_output_writer_target_order".to_string(),
            writer_matrix.target_kind_order().join(","),
        ),
        (
            "compatibility_output_writer_local_smoke_count".to_string(),
            writer_matrix.local_fixture_smoke_count().to_string(),
        ),
        (
            "compatibility_output_writer_local_smoke_targets".to_string(),
            writer_matrix.local_fixture_smoke_kind_order().join(","),
        ),
        (
            "compatibility_output_writer_blocked_count".to_string(),
            writer_matrix.blocked_count().to_string(),
        ),
        (
            "compatibility_output_writer_blocked_targets".to_string(),
            writer_matrix.blocked_kind_order().join(","),
        ),
        (
            "fallback_execution_allowed".to_string(),
            writer_matrix.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            writer_matrix.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            writer_matrix.external_engine_invoked.to_string(),
        ),
        (
            "performance_claim_allowed".to_string(),
            writer_matrix.performance_claim_allowed.to_string(),
        ),
        (
            "production_output_claim_allowed".to_string(),
            writer_matrix.production_output_claim_allowed.to_string(),
        ),
        (
            "lakehouse_table_commit_claim_allowed".to_string(),
            writer_matrix
                .lakehouse_table_commit_claim_allowed
                .to_string(),
        ),
    ];
    if let Some(row) = writer_matrix.row_for_kind(&plan.target.kind) {
        push_field(
            &mut fields,
            "compatibility_output_writer_target_status",
            row.support_status.as_str(),
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_claim_gate_status",
            row.support_status.claim_gate_status(),
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_writer_id",
            &row.writer_id,
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_feature_gate",
            row.feature_gate.as_deref().unwrap_or("none"),
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_implementation_ref",
            &row.implementation_ref,
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_evidence_ref",
            &row.evidence_ref,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_metadata_loss_reported",
            row.metadata_loss_reported,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_local_file_output",
            row.local_file_output,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_object_store_output",
            row.object_store_output,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_table_commit_semantics",
            row.table_commit_semantics,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_fallback_attempted",
            row.fallback_attempted,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_external_engine_invoked",
            row.external_engine_invoked,
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_claim_boundary",
            &row.claim_boundary,
        );
    } else {
        push_field(
            &mut fields,
            "compatibility_output_writer_target_status",
            "unsupported",
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_claim_gate_status",
            "not_claim_grade",
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_writer_id",
            "none",
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_feature_gate",
            "none",
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_implementation_ref",
            "none",
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_evidence_ref",
            "none",
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_metadata_loss_reported",
            false,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_local_file_output",
            false,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_object_store_output",
            false,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_table_commit_semantics",
            false,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_fallback_attempted",
            false,
        );
        push_bool_field(
            &mut fields,
            "compatibility_output_writer_target_external_engine_invoked",
            false,
        );
        push_field(
            &mut fields,
            "compatibility_output_writer_target_claim_boundary",
            "unsupported output target",
        );
    }
    fields
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
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    handle_vortex_metadata_summary_with_facade(args, format, "vortex-metadata-summary", Vec::new())
}

pub(crate) fn handle_vortex_metadata_summary_with_facade(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
    emit_command: &'static str,
    mut extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(uri_text) = args.next() else {
        return emit_error(
            emit_command,
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
                emit_command,
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            );
        }
    };
    let probe = probe_vortex_metadata_only(uri)
        .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
    let report = summarize_vortex_metadata_probe(&probe);
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("mode".to_string(), "vortex_metadata_summary".to_string()),
        (
            "metadata_summary_plan_only".to_string(),
            metadata_summary_is_plan_only(&report).to_string(),
        ),
        ("data_materialized".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("write_io".to_string(), "false".to_string()),
        (
            "execution".to_string(),
            "metadata_profile_summary".to_string(),
        ),
        ("plan_only".to_string(), "true".to_string()),
    ];
    fields.append(&mut extra_fields);
    emit(
        emit_command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata summary".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        fields,
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

pub(crate) fn handle_vortex_metadata_physical_kernel_plan(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    run_vortex_metadata_physical_kernel_plan(format, args.collect())
}
#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_count_readiness_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source_arg) = args.next() else {
        return emit_error(
            "vortex-count-readiness-plan",
            format,
            "missing candidate source",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <candidate_source>".to_string(),
            ),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            "vortex-count-readiness-plan",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let candidate_source = match source_arg.as_str() {
        "metadata-footer" | "metadata_footer" => VortexCountCandidateSource::MetadataFooter,
        "encoded-data-path" | "encoded_data_path" => VortexCountCandidateSource::EncodedDataPath,
        "unknown" => VortexCountCandidateSource::Unknown,
        _ => {
            return emit_error(
                "vortex-count-readiness-plan",
                format,
                "invalid candidate source",
                &ShardLoomError::InvalidOperation(format!(
                    "invalid candidate source: {source_arg}"
                )),
            );
        }
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-count-readiness-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let mut request = shardloom_vortex::VortexCountReadinessRequest::new(uri, candidate_source);
    for token in args {
        match token.as_str() {
            "--feature-gate" => {
                request.add_signal(VortexCountReadinessSignal::FeatureGateEnabled);
            }
            "--query-primitive-ready" => {
                request.add_signal(VortexCountReadinessSignal::QueryPrimitiveReady);
            }
            "--metadata-footer-ready" => {
                request.add_signal(VortexCountReadinessSignal::MetadataFooterReady);
            }
            "--encoded-data-path-ready" => {
                request.add_signal(VortexCountReadinessSignal::EncodedDataPathReady);
            }
            "--count-primitive" => {
                request.add_signal(VortexCountReadinessSignal::CountPrimitive);
            }
            "--filtered-count-requested" => {
                request.add_signal(VortexCountReadinessSignal::FilteredCountRequested);
            }
            "--predicate-provided" => {
                request.add_signal(VortexCountReadinessSignal::PredicateProvided);
            }
            "--object-store-target" => {
                request.add_signal(VortexCountReadinessSignal::ObjectStoreTarget);
            }
            "--decode-risk" => request.add_signal(VortexCountReadinessSignal::DecodeRisk),
            "--materialization-risk" => {
                request.add_signal(VortexCountReadinessSignal::MaterializationRisk);
            }
            "--arrow-default-risk" => {
                request.add_signal(VortexCountReadinessSignal::ArrowDefaultRisk);
            }
            "--write-risk" => request.add_signal(VortexCountReadinessSignal::WriteRisk),
            "--scan-execution-risk" => {
                request.add_signal(VortexCountReadinessSignal::ScanExecutionRisk);
            }
            "--fallback-policy-blocked" => {
                request.add_signal(VortexCountReadinessSignal::FallbackPolicyBlocked);
            }
            _ => {
                return emit_error(
                    "vortex-count-readiness-plan",
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                );
            }
        }
    }
    let report = match plan_vortex_count_readiness(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-count-readiness-plan",
                format,
                "count readiness planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-count-readiness-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex count readiness planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "candidate_source".to_string(),
                report.request.candidate_source.as_str().to_string(),
            ),
            ("status".to_string(), report.status.as_str().to_string()),
            ("mode".to_string(), report.mode.as_str().to_string()),
            (
                "count_ready".to_string(),
                report.status.count_ready().to_string(),
            ),
            ("count_executed".to_string(), "false".to_string()),
            (
                "feature_gate_enabled".to_string(),
                report
                    .request
                    .has_signal(VortexCountReadinessSignal::FeatureGateEnabled)
                    .to_string(),
            ),
            (
                "query_primitive_ready".to_string(),
                report
                    .request
                    .has_signal(VortexCountReadinessSignal::QueryPrimitiveReady)
                    .to_string(),
            ),
            (
                "metadata_footer_ready".to_string(),
                report
                    .request
                    .has_signal(VortexCountReadinessSignal::MetadataFooterReady)
                    .to_string(),
            ),
            (
                "encoded_data_path_ready".to_string(),
                report
                    .request
                    .has_signal(VortexCountReadinessSignal::EncodedDataPathReady)
                    .to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("metadata_read".to_string(), "false".to_string()),
            ("encoded_data_read".to_string(), "false".to_string()),
            ("row_read".to_string(), "false".to_string()),
            ("array_decoded".to_string(), "false".to_string()),
            ("values_materialized".to_string(), "false".to_string()),
            ("arrow_converted".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("data_written".to_string(), "false".to_string()),
            ("upstream_scan_called".to_string(), "false".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_encoded_count_approval_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-count-approval-plan";
    let Some(source_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing candidate source",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <candidate_source>".to_string(),
            ),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let candidate_source = match source_arg.as_str() {
        "metadata-footer" | "metadata_footer" => VortexCountCandidateSource::MetadataFooter,
        "encoded-data-path" | "encoded_data_path" => VortexCountCandidateSource::EncodedDataPath,
        "unknown" => VortexCountCandidateSource::Unknown,
        _ => {
            return emit_error(
                command,
                format,
                "invalid candidate source",
                &ShardLoomError::InvalidOperation(format!(
                    "invalid candidate source: {source_arg}"
                )),
            );
        }
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(command, format, "invalid dataset uri", &error);
        }
    };
    let mut request = shardloom_vortex::VortexCountReadinessRequest::new(uri, candidate_source);
    let mut layout_row_count_approved = false;
    for token in args {
        match token.as_str() {
            "--feature-gate" => {
                request.add_signal(VortexCountReadinessSignal::FeatureGateEnabled);
            }
            "--query-primitive-ready" => {
                request.add_signal(VortexCountReadinessSignal::QueryPrimitiveReady);
            }
            "--metadata-footer-ready" => {
                request.add_signal(VortexCountReadinessSignal::MetadataFooterReady);
            }
            "--encoded-data-path-ready" => {
                request.add_signal(VortexCountReadinessSignal::EncodedDataPathReady);
            }
            "--count-primitive" => {
                request.add_signal(VortexCountReadinessSignal::CountPrimitive);
            }
            "--filtered-count-requested" => {
                request.add_signal(VortexCountReadinessSignal::FilteredCountRequested);
            }
            "--predicate-provided" => {
                request.add_signal(VortexCountReadinessSignal::PredicateProvided);
            }
            "--object-store-target" => {
                request.add_signal(VortexCountReadinessSignal::ObjectStoreTarget);
            }
            "--decode-risk" => request.add_signal(VortexCountReadinessSignal::DecodeRisk),
            "--materialization-risk" => {
                request.add_signal(VortexCountReadinessSignal::MaterializationRisk);
            }
            "--arrow-default-risk" => {
                request.add_signal(VortexCountReadinessSignal::ArrowDefaultRisk);
            }
            "--write-risk" => request.add_signal(VortexCountReadinessSignal::WriteRisk),
            "--scan-execution-risk" => {
                request.add_signal(VortexCountReadinessSignal::ScanExecutionRisk);
            }
            "--fallback-policy-blocked" => {
                request.add_signal(VortexCountReadinessSignal::FallbackPolicyBlocked);
            }
            "--layout-row-count-approved" => {
                layout_row_count_approved = true;
            }
            _ => {
                return emit_error(
                    command,
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                );
            }
        }
    }
    let count_report = match plan_vortex_count_readiness(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(command, format, "count readiness planning failed", &error);
        }
    };
    let api_boundary = vortex_encoded_read_public_api_boundary();
    let report = if layout_row_count_approved {
        let layout_report = match plan_vortex_layout_reader_driver_approval(
            VortexLayoutReaderDriverApprovalInput::new(api_boundary.clone())
                .local_fixture_only(true)
                .caller_session_allowed(true)
                .runtime_driver_start_allowed(true)
                .layout_row_count_only_intent(true)
                .scan_forbidden(true)
                .evaluation_forbidden(true)
                .data_read_forbidden(true)
                .decode_forbidden(true)
                .materialization_forbidden(true)
                .arrow_forbidden(true)
                .object_store_forbidden(true)
                .write_forbidden(true)
                .fallback_forbidden(true),
        ) {
            Ok(report) => report,
            Err(error) => {
                return emit_error(
                    command,
                    format,
                    "layout driver approval planning failed",
                    &error,
                );
            }
        };
        plan_vortex_encoded_count_data_path_approval_with_layout_driver(
            count_report,
            api_boundary,
            layout_report,
        )
    } else {
        plan_vortex_encoded_count_data_path_approval(count_report, api_boundary)
    };
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                command,
                format,
                "encoded count approval planning failed",
                &error,
            );
        }
    };
    let local_execution_report = if report.approved() {
        match execute_vortex_count_all_from_encoded_count_data_path_approval(&report) {
            Ok(local) => Some(local),
            Err(error) => {
                return emit_error(
                    command,
                    format,
                    "encoded count local guard planning failed",
                    &error,
                );
            }
        }
    } else {
        None
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded count approval planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "candidate_source".to_string(),
                report
                    .input
                    .count_readiness_report
                    .request
                    .candidate_source
                    .as_str()
                    .to_string(),
            ),
            ("status".to_string(), report.status.as_str().to_string()),
            ("mode".to_string(), report.mode.as_str().to_string()),
            ("approved".to_string(), report.approved().to_string()),
            (
                "metadata_count_surface_ready".to_string(),
                report.metadata_count_surface_ready.to_string(),
            ),
            (
                "execution_usable_data_path_count".to_string(),
                report.execution_usable_data_path_count.to_string(),
            ),
            (
                "layout_driver_approval_status".to_string(),
                report
                    .layout_driver_approval_status
                    .clone()
                    .unwrap_or_else(|| "absent".to_string()),
            ),
            (
                "layout_row_count_path_approved".to_string(),
                report.layout_row_count_path_approved.to_string(),
            ),
            (
                "api_boundary_blocker_count".to_string(),
                report.api_boundary_blockers.len().to_string(),
            ),
            (
                "encoded_data_path_ready".to_string(),
                report
                    .input
                    .count_readiness_report
                    .encoded_data_path_ready()
                    .to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                report.fallback_execution_allowed.to_string(),
            ),
            (
                "count_executed".to_string(),
                report.count_executed.to_string(),
            ),
            (
                "encoded_data_read".to_string(),
                report.encoded_data_read.to_string(),
            ),
            ("row_read".to_string(), report.row_read.to_string()),
            (
                "array_decoded".to_string(),
                report.array_decoded.to_string(),
            ),
            (
                "values_materialized".to_string(),
                report.values_materialized.to_string(),
            ),
            (
                "arrow_converted".to_string(),
                report.arrow_converted.to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io.to_string(),
            ),
            ("data_written".to_string(), report.data_written.to_string()),
            (
                "upstream_scan_called".to_string(),
                report.upstream_scan_called.to_string(),
            ),
            (
                "local_execution_status".to_string(),
                local_execution_report.as_ref().map_or_else(
                    || "not_planned".to_string(),
                    |local| local.status.as_str().to_string(),
                ),
            ),
            (
                "local_execution_result_known".to_string(),
                local_execution_report
                    .as_ref()
                    .is_some_and(|local| local.value.is_known())
                    .to_string(),
            ),
            (
                "local_execution_data_read".to_string(),
                local_execution_report
                    .as_ref()
                    .is_some_and(|local| local.data_read)
                    .to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_layout_driver_approval_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-layout-driver-approval-plan";
    let Some(signals_raw) = args.next() else {
        return emit_error(
            command,
            format,
            "missing signals",
            &ShardLoomError::InvalidOperation("missing required argument: <signals>".to_string()),
        );
    };
    if let Some(extra) = args.next() {
        return emit_error(
            command,
            format,
            "unknown option",
            &ShardLoomError::InvalidOperation(format!("unknown option: {extra}")),
        );
    }
    let signals = match parse_vortex_layout_driver_approval_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(command, format, "invalid signals", &error);
        }
    };
    let mut input =
        VortexLayoutReaderDriverApprovalInput::new(vortex_encoded_read_public_api_boundary());
    for signal in signals {
        input.add_signal(signal);
    }
    let report = match plan_vortex_layout_reader_driver_approval(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                command,
                format,
                "layout driver approval planning failed",
                &error,
            );
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex layout driver approval planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            ("status".to_string(), report.status.as_str().to_string()),
            ("mode".to_string(), report.mode.as_str().to_string()),
            ("approved".to_string(), report.approved().to_string()),
            (
                "layout_reader_surface_present".to_string(),
                report.layout_reader_surface_present.to_string(),
            ),
            (
                "layout_row_count_surface_present".to_string(),
                report.layout_row_count_surface_present.to_string(),
            ),
            (
                "runtime_driver_risk_present".to_string(),
                report.runtime_driver_risk_present.to_string(),
            ),
            (
                "layout_reader_constructed".to_string(),
                report.layout_reader_constructed.to_string(),
            ),
            (
                "runtime_driver_started".to_string(),
                report.runtime_driver_started.to_string(),
            ),
            ("scan_called".to_string(), report.scan_called.to_string()),
            (
                "evaluation_called".to_string(),
                report.evaluation_called.to_string(),
            ),
            ("data_read".to_string(), report.data_read.to_string()),
            ("row_read".to_string(), report.row_read.to_string()),
            ("data_decoded".to_string(), report.data_decoded.to_string()),
            (
                "data_materialized".to_string(),
                report.data_materialized.to_string(),
            ),
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
                "fallback_execution_allowed".to_string(),
                report.fallback_execution_allowed.to_string(),
            ),
            (
                "side_effect_free".to_string(),
                report.is_side_effect_free().to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_filtered_count_readiness_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source_arg) = args.next() else {
        return emit_error(
            "vortex-filtered-count-readiness-plan",
            format,
            "missing candidate source",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <candidate_source>".to_string(),
            ),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            "vortex-filtered-count-readiness-plan",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let candidate_source = match source_arg.as_str() {
        "metadata-predicate-proof" | "metadata_predicate_proof" => {
            VortexFilteredCountCandidateSource::MetadataPredicateProof
        }
        "encoded-predicate-path" | "encoded_predicate_path" => {
            VortexFilteredCountCandidateSource::EncodedPredicatePath
        }
        "unknown" => VortexFilteredCountCandidateSource::Unknown,
        _ => {
            return emit_error(
                "vortex-filtered-count-readiness-plan",
                format,
                "invalid candidate source",
                &ShardLoomError::InvalidOperation(format!(
                    "invalid candidate source: {source_arg}"
                )),
            );
        }
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-filtered-count-readiness-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let mut request =
        shardloom_vortex::VortexFilteredCountReadinessRequest::new(uri, candidate_source);
    for token in args {
        match token.as_str() {
            "--feature-gate" => {
                request.add_signal(VortexFilteredCountReadinessSignal::FeatureGateEnabled);
            }
            "--query-primitive-ready" => {
                request.add_signal(VortexFilteredCountReadinessSignal::QueryPrimitiveReady);
            }
            "--metadata-footer-ready" => {
                request.add_signal(VortexFilteredCountReadinessSignal::MetadataFooterReady);
            }
            "--encoded-data-path-ready" => {
                request.add_signal(VortexFilteredCountReadinessSignal::EncodedDataPathReady);
            }
            "--filtered-count-primitive" => {
                request.add_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive);
            }
            "--predicate-provided" => {
                request.add_signal(VortexFilteredCountReadinessSignal::PredicateProvided);
            }
            "--predicate-metadata-proof-ready" => {
                request.add_signal(VortexFilteredCountReadinessSignal::PredicateMetadataProofReady);
            }
            "--predicate-unsupported" => {
                request.add_signal(VortexFilteredCountReadinessSignal::PredicateUnsupported);
            }
            "--object-store-target" => {
                request.add_signal(VortexFilteredCountReadinessSignal::ObjectStoreTarget);
            }
            "--decode-risk" => {
                request.add_signal(VortexFilteredCountReadinessSignal::DecodeRisk);
            }
            "--materialization-risk" => {
                request.add_signal(VortexFilteredCountReadinessSignal::MaterializationRisk);
            }
            "--arrow-default-risk" => {
                request.add_signal(VortexFilteredCountReadinessSignal::ArrowDefaultRisk);
            }
            "--write-risk" => {
                request.add_signal(VortexFilteredCountReadinessSignal::WriteRisk);
            }
            "--scan-execution-risk" => {
                request.add_signal(VortexFilteredCountReadinessSignal::ScanExecutionRisk);
            }
            "--fallback-policy-blocked" => {
                request.add_signal(VortexFilteredCountReadinessSignal::FallbackPolicyBlocked);
            }
            _ => {
                return emit_error(
                    "vortex-filtered-count-readiness-plan",
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                );
            }
        }
    }
    let report = match plan_vortex_filtered_count_readiness(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-filtered-count-readiness-plan",
                format,
                "filtered count readiness planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-filtered-count-readiness-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex filtered count readiness planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "candidate_source".to_string(),
                report.request.candidate_source.as_str().to_string(),
            ),
            ("status".to_string(), report.status.as_str().to_string()),
            ("mode".to_string(), report.mode.as_str().to_string()),
            (
                "filtered_count_ready".to_string(),
                report.status.filtered_count_ready().to_string(),
            ),
            ("filtered_count_executed".to_string(), "false".to_string()),
            ("predicate_evaluated".to_string(), "false".to_string()),
            (
                "feature_gate_enabled".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::FeatureGateEnabled)
                    .to_string(),
            ),
            (
                "query_primitive_ready".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::QueryPrimitiveReady)
                    .to_string(),
            ),
            (
                "metadata_footer_ready".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::MetadataFooterReady)
                    .to_string(),
            ),
            (
                "encoded_data_path_ready".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::EncodedDataPathReady)
                    .to_string(),
            ),
            (
                "filtered_count_primitive".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive)
                    .to_string(),
            ),
            (
                "predicate_provided".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::PredicateProvided)
                    .to_string(),
            ),
            (
                "predicate_metadata_proof_ready".to_string(),
                report
                    .request
                    .has_signal(VortexFilteredCountReadinessSignal::PredicateMetadataProofReady)
                    .to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("metadata_read".to_string(), "false".to_string()),
            ("encoded_data_read".to_string(), "false".to_string()),
            ("row_read".to_string(), "false".to_string()),
            ("array_decoded".to_string(), "false".to_string()),
            ("values_materialized".to_string(), "false".to_string()),
            ("arrow_converted".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("data_written".to_string(), "false".to_string()),
            ("upstream_scan_called".to_string(), "false".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_projection_readiness_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source_arg) = args.next() else {
        return emit_error(
            "vortex-projection-readiness-plan",
            format,
            "missing candidate source",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <candidate_source>".to_string(),
            ),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            "vortex-projection-readiness-plan",
            format,
            "missing dataset uri",
            &ShardLoomError::InvalidOperation(
                "missing required argument: <dataset_uri>".to_string(),
            ),
        );
    };
    let candidate_source = match source_arg.as_str() {
        "metadata-schema-projection" | "metadata_schema_projection" => {
            VortexProjectionCandidateSource::MetadataSchemaProjection
        }
        "encoded-column-path" | "encoded_column_path" => {
            VortexProjectionCandidateSource::EncodedColumnPath
        }
        "unknown" => VortexProjectionCandidateSource::Unknown,
        _ => {
            return emit_error(
                "vortex-projection-readiness-plan",
                format,
                "invalid candidate source",
                &ShardLoomError::InvalidOperation(format!(
                    "invalid candidate source: {source_arg}"
                )),
            );
        }
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-projection-readiness-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let mut request =
        shardloom_vortex::VortexProjectionReadinessRequest::new(uri, candidate_source);
    for token in args {
        match token.as_str() {
            "--feature-gate" => {
                request.add_signal(VortexProjectionReadinessSignal::FeatureGateEnabled);
            }
            "--query-primitive-ready" => {
                request.add_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady);
            }
            "--metadata-footer-ready" => {
                request.add_signal(VortexProjectionReadinessSignal::MetadataFooterReady);
            }
            "--encoded-data-path-ready" => {
                request.add_signal(VortexProjectionReadinessSignal::EncodedDataPathReady);
            }
            "--projection-primitive" => {
                request.add_signal(VortexProjectionReadinessSignal::ProjectionPrimitive);
            }
            "--projection-provided" => {
                request.add_signal(VortexProjectionReadinessSignal::ProjectionProvided);
            }
            "--projection-supported" => {
                request.add_signal(VortexProjectionReadinessSignal::ProjectionSupported);
            }
            "--projection-unsupported" => {
                request.add_signal(VortexProjectionReadinessSignal::ProjectionUnsupported);
            }
            "--object-store-target" => {
                request.add_signal(VortexProjectionReadinessSignal::ObjectStoreTarget);
            }
            "--decode-risk" => {
                request.add_signal(VortexProjectionReadinessSignal::DecodeRisk);
            }
            "--materialization-risk" => {
                request.add_signal(VortexProjectionReadinessSignal::MaterializationRisk);
            }
            "--arrow-default-risk" => {
                request.add_signal(VortexProjectionReadinessSignal::ArrowDefaultRisk);
            }
            "--write-risk" => {
                request.add_signal(VortexProjectionReadinessSignal::WriteRisk);
            }
            "--scan-execution-risk" => {
                request.add_signal(VortexProjectionReadinessSignal::ScanExecutionRisk);
            }
            "--fallback-policy-blocked" => {
                request.add_signal(VortexProjectionReadinessSignal::FallbackPolicyBlocked);
            }
            _ => {
                return emit_error(
                    "vortex-projection-readiness-plan",
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                );
            }
        }
    }
    let report = match plan_vortex_projection_readiness(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-projection-readiness-plan",
                format,
                "projection readiness planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-projection-readiness-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex projection readiness planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_projection_readiness_fields(&report),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(name, _)| name == key)
            .map_or_else(|| panic!("missing field {key}"), |(_, value)| value)
    }

    #[test]
    fn translation_plan_fields_expose_arrow_ipc_writer_smoke_boundaries() {
        let plan = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("file://tmp/out.arrow").expect("valid uri"),
        ));
        let matrix = CompatibilityOutputWriterMatrixReport::current();
        let fields = translation_plan_fields(&plan, &matrix);

        assert_eq!(output_field(&fields, "target_kind"), "arrow_ipc");
        assert_eq!(
            output_field(&fields, "compatibility_output_writer_target_status"),
            "local_fixture_smoke"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_claim_gate_status"
            ),
            "fixture_smoke_only"
        );
        assert_eq!(
            output_field(&fields, "compatibility_output_writer_target_feature_gate"),
            "vortex-traditional-analytics-benchmark"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_local_file_output"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_object_store_output"
            ),
            "false"
        );
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "external_engine_invoked"), "false");
        assert_eq!(
            output_field(&fields, "production_output_claim_allowed"),
            "false"
        );
    }

    #[test]
    fn translation_plan_fields_block_iceberg_table_commit_claims() {
        let plan = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("file://tmp/table/metadata/v1.metadata.json").expect("valid uri"),
        ));
        let matrix = CompatibilityOutputWriterMatrixReport::current();
        let fields = translation_plan_fields(&plan, &matrix);

        assert_eq!(output_field(&fields, "target_kind"), "iceberg_compatible");
        assert_eq!(
            output_field(&fields, "compatibility_output_writer_target_status"),
            "report_only_blocked"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_table_commit_semantics"
            ),
            "false"
        );
        assert_eq!(
            output_field(&fields, "lakehouse_table_commit_claim_allowed"),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_fallback_attempted"
            ),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_external_engine_invoked"
            ),
            "false"
        );
    }

    #[test]
    fn translation_plan_fields_include_full_writer_matrix_for_unknown_targets() {
        let plan = TranslationPlan::for_target(OutputTarget::from_uri(
            DatasetUri::new("file://tmp/out.custom").expect("valid uri"),
        ));
        let matrix = CompatibilityOutputWriterMatrixReport::current();
        let fields = translation_plan_fields(&plan, &matrix);

        assert_eq!(
            output_field(&fields, "compatibility_output_writer_target_status"),
            "unsupported"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_implementation_ref"
            ),
            "none"
        );
        assert_eq!(
            output_field(&fields, "compatibility_output_writer_target_evidence_ref"),
            "none"
        );
        assert_eq!(
            output_field(
                &fields,
                "compatibility_output_writer_target_external_engine_invoked"
            ),
            "false"
        );
    }
}
