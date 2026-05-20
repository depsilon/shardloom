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
    ColumnRef, CommandStatus, ComparisonOp, CorrectnessFixture, CorrectnessValidationPlan,
    DatasetRef, DatasetUri, ExecutionCertificate, ExpectedOutcome, NativeIoCertificate,
    OutputFormat, PredicateExpr, ShardLoomError, StatValue,
};
use shardloom_exec::{
    AdaptiveSizingPolicy, BoundedMemoryPolicy, ByteSize, EncodedStreamingBatchPlanInput,
    EncodedStreamingBatchPlanReport, MemoryBudget, StreamingSink, StreamingSource,
    plan_encoded_streaming_batches,
};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexBoundedExecutionPolicy, VortexBoundedExecutionReport, VortexCountCandidateSource,
    VortexCountReadinessRequest, VortexEncodedCountKernelAdmissionReport,
    VortexEncodedCountPhysicalKernelReport, VortexEncodedPredicateEvaluationReport,
    VortexEncodedPredicateEvaluationStatus, VortexEncodedReadExecutionMode,
    VortexEncodedReadExecutionStatus, VortexEncodedReadExecutorFeatureStatus,
    VortexLocalEngineReport, VortexLocalEngineRequest, VortexLocalEngineWhyReport,
    VortexLocalExecutionReport, VortexLocalExecutionStatus, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexMetadataOpenRequest, VortexMetadataProbeReport,
    VortexMetadataSummaryReport, VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult,
    VortexQueryPrimitiveValue, VortexSelectionVectorFilterKernelAdmissionReport,
    VortexSelectionVectorFilterKernelReport, VortexStreamingBatchRuntimeReport,
    VortexTaskSchedulingDecision, VortexWorkAvoidedMetricKind, VortexWorkAvoidedReport,
    admit_vortex_encoded_count_kernel, admit_vortex_selection_vector_filter_kernel,
    build_vortex_runtime_task_graph, evaluate_vortex_encoded_predicate_segments,
    evaluate_vortex_encoded_read_readiness, evaluate_vortex_local_encoded_count_physical_kernel,
    evaluate_vortex_query_primitive, evaluate_vortex_query_primitive_with_analysis,
    evaluate_vortex_selection_vector_filter_kernel, execute_vortex_bounded_local_query,
    execute_vortex_count_all_from_approved_local_scan,
    execute_vortex_count_all_from_approved_local_scan_result,
    execute_vortex_local_primitive_with_policy, execute_vortex_local_query_primitive,
    local_encoded_count_execution_certificate, local_encoded_count_native_io_certificate,
    local_primitive_execution_certificate, local_primitive_native_io_certificate,
    open_vortex_metadata_only, parse_vortex_local_engine_primitive,
    plan_native_vortex_universal_input, plan_vortex_count_readiness,
    plan_vortex_encoded_count_data_path_approval, plan_vortex_memory_safety,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue, run_vortex_local_engine,
    size_vortex_runtime_task_graph, summarize_vortex_metadata_probe,
    vortex_encoded_read_local_scan_count_api_boundary, vortex_encoded_read_spike_feature_enabled,
};

use crate::cli_output::{emit, emit_error};

const VORTEX_PRIMITIVE_SCAN_PUSHDOWN_SCHEMA_VERSION: &str =
    "shardloom.vortex_primitive.scan_pushdown_contract.v1";

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}

pub(crate) fn append_vortex_work_avoided_fields(
    fields: &mut Vec<(String, String)>,
    report: Option<&VortexWorkAvoidedReport>,
) {
    let Some(report) = report else {
        push_count_field(fields, "work_avoided_metrics", 0);
        push_count_field(fields, "work_avoided_known_metrics", 0);
        push_count_field(fields, "work_avoided_unknown_metrics", 0);
        return;
    };
    push_count_field(fields, "work_avoided_metrics", report.metric_count());
    push_count_field(
        fields,
        "work_avoided_known_metrics",
        report.known_metric_count(),
    );
    push_count_field(
        fields,
        "work_avoided_unknown_metrics",
        report.unknown_metric_count(),
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::DecodeAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::MaterializationAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::ObjectStoreRequestsAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::SpillAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::FallbackBlocked,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::RowsNotScanned,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::SegmentsPruned,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::BytesNotRead,
    );
}

fn append_vortex_work_avoided_metric_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexWorkAvoidedReport,
    kind: VortexWorkAvoidedMetricKind,
) {
    let stem = format!("work_avoided_{}", kind.as_str());
    push_field(fields, &stem, &report.metric_value_summary(kind));
    push_field(
        fields,
        &format!("{stem}_known"),
        &report.metric_known_summary(kind),
    );
    push_field(
        fields,
        &format!("{stem}_reason"),
        &report.metric_reason_summary(kind),
    );
}

pub(crate) fn reconcile_vortex_local_engine_why_with_execution_certificate(
    report: &mut VortexLocalEngineWhyReport,
    certificate: Option<&ExecutionCertificate>,
) {
    if !certificate.is_some_and(ExecutionCertificate::is_certified) {
        return;
    }
    report
        .next_actions
        .retain(|action| action != "attach CG-16 execution certificate evidence");
    if !report
        .supporting_evidence
        .iter()
        .any(|evidence| evidence == "cg16_execution_certificate=certified")
    {
        report
            .supporting_evidence
            .push("cg16_execution_certificate=certified".to_string());
    }
}

pub(crate) fn append_vortex_local_engine_why_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexLocalEngineWhyReport,
) {
    push_field(fields, "why_report_present", "true");
    push_field(fields, "why_schema_version", report.schema_version);
    push_field(fields, "why_report_id", report.report_id);
    push_field(fields, "why_claim_gate_status", report.claim_gate_status);
    push_field(fields, "why_primary_reason", &report.primary_reason);
    push_count_field(fields, "why_blocker_count", report.blocker_count());
    push_field(fields, "why_blockers", &report.blockers_summary());
    push_count_field(
        fields,
        "why_supporting_evidence_count",
        report.supporting_evidence_count(),
    );
    push_field(
        fields,
        "why_supporting_evidence",
        &report.supporting_evidence_summary(),
    );
    push_count_field(fields, "why_next_action_count", report.next_action_count());
    push_field(fields, "why_next_actions", &report.next_actions_summary());
    push_count_field(
        fields,
        "decision_trace_entries",
        report.decision_trace_entries,
    );
    push_count_field(
        fields,
        "why_work_avoided_metrics",
        report.work_avoided_metrics,
    );
    push_bool_field(fields, "why_fallback_attempted", report.fallback_attempted);
}

pub(crate) fn bounded_local_execution_fields(
    report: &VortexBoundedExecutionReport,
    primitive: &str,
    memory_gb: u64,
    max_parallelism: usize,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "vortex_bounded_local_exec");
    push_field(
        &mut fields,
        "bounded_execution_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "bounded_execution_mode", report.mode.as_str());
    push_field(&mut fields, "primitive", primitive);
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_count_field(
        &mut fields,
        "metadata_tasks_completed",
        report.metadata_tasks_completed,
    );
    push_count_field(
        &mut fields,
        "noop_tasks_completed",
        report.noop_tasks_completed,
    );
    push_count_field(
        &mut fields,
        "encoded_read_tasks_deferred",
        report.encoded_read_tasks_deferred,
    );
    push_count_field(&mut fields, "blocked_task_count", report.blocked_task_count);
    push_count_field(
        &mut fields,
        "bounded_decision_count",
        report.decisions.len(),
    );
    push_field(
        &mut fields,
        "local_execution_status",
        report.local_execution_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "local_execution_mode",
        report.local_execution_report.mode.as_str(),
    );
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", "metadata_only_or_not_performed");
    push_bool_field(
        &mut fields,
        "result_known",
        report.local_execution_report.value.is_known(),
    );
    fields
}

pub(crate) fn parse_tiny_predicate(value: &str) -> Result<PredicateExpr, ShardLoomError> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["is_null", column] => Ok(PredicateExpr::IsNull {
            column: ColumnRef::new(*column)?,
        }),
        ["is_not_null", column] => Ok(PredicateExpr::IsNotNull {
            column: ColumnRef::new(*column)?,
        }),
        [op, column, int_value] => {
            let parsed: i64 = int_value.parse().map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "predicate integer literal must be valid i64".to_string(),
                )
            })?;
            let op = match *op {
                "eq" => ComparisonOp::Eq,
                "gt" => ComparisonOp::Gt,
                "gte" => ComparisonOp::GtEq,
                "lt" => ComparisonOp::Lt,
                "lte" => ComparisonOp::LtEq,
                _ => {
                    return Err(ShardLoomError::InvalidOperation(
                        "unsupported predicate operator".to_string(),
                    ));
                }
            };
            Ok(PredicateExpr::Compare {
                column: ColumnRef::new(*column)?,
                op,
                value: StatValue::Int64(parsed),
            })
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "invalid predicate format; expected is_null:<column>, is_not_null:<column>, or <op>:<column>:<integer>".to_string(),
        )),
    }
}

pub(crate) struct VortexCountWhereFilterEvidence {
    pub(crate) predicate_evaluation: VortexEncodedPredicateEvaluationReport,
    pub(crate) filter_kernel: VortexSelectionVectorFilterKernelReport,
    pub(crate) filter_kernel_admission: VortexSelectionVectorFilterKernelAdmissionReport,
}

pub(crate) struct VortexLocalPrimitiveCliExecutionRequest {
    pub(crate) memory_gb: u64,
    pub(crate) max_parallelism: usize,
}

pub(crate) type VortexCountWhereLocalExecutionRequest = VortexLocalPrimitiveCliExecutionRequest;
pub(crate) type VortexCountWhereLocalExecutionEvidence = VortexLocalPrimitiveCliExecutionEvidence;

pub(crate) struct VortexLocalPrimitiveCliExecutionEvidence {
    pub(crate) memory_gb: u64,
    pub(crate) max_parallelism: usize,
    pub(crate) report: VortexLocalPrimitiveExecutionReport,
    pub(crate) native_io_certificate: NativeIoCertificate,
    pub(crate) execution_certificate: Option<ExecutionCertificate>,
}

struct VortexFilterProjectCliOptions {
    local_execution_request: Option<VortexLocalPrimitiveCliExecutionRequest>,
    source_order_limit: Option<usize>,
}
impl VortexLocalPrimitiveCliExecutionEvidence {
    pub(crate) fn has_errors(&self) -> bool {
        self.report.has_errors() || !self.native_io_certificate.is_certified()
    }

    pub(crate) fn count(&self) -> Option<u64> {
        self.report.rows_selected
    }

    pub(crate) fn selected_rows(&self) -> Option<u64> {
        self.report.rows_selected
    }

    pub(crate) fn projected_rows(&self) -> Option<u64> {
        self.report.rows_projected
    }

    pub(crate) fn selection_vector_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->selection_vector_encoded"
            && self.report.filter_pushdown_applied
            && self.report.upstream_filter_expression_used
            && self.report.rows_selected.is_some()
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    pub(crate) fn projection_encoded_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->vortex_encoded"
            && self.projection_evidence_guaranteed()
            && self.report.rows_projected.is_some()
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    pub(crate) fn filter_project_encoded_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->selection_vector_encoded"
            && self.report.filter_pushdown_applied
            && self.report.upstream_filter_expression_used
            && self.projection_evidence_guaranteed()
            && self.report.rows_selected.is_some()
            && self.report.rows_projected.is_some()
            && self.report.rows_selected == self.report.rows_projected
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    fn projection_evidence_guaranteed(&self) -> bool {
        !self.report.projected_columns.is_empty()
            && ((self.report.projection_pushdown_applied
                && self.report.upstream_projection_expression_used)
                || (self.report.projected_columns.len() == 1
                    && self.report.projected_columns[0] == "value"
                    && !self.report.projection_pushdown_applied
                    && !self.report.upstream_projection_expression_used))
    }
}

pub(crate) fn parse_vortex_count_where_local_execution_args(
    mut args: impl Iterator<Item = String>,
) -> shardloom_core::Result<Option<VortexCountWhereLocalExecutionRequest>> {
    parse_vortex_local_primitive_cli_execution_args(&mut args)
}

pub(crate) fn parse_vortex_local_primitive_cli_execution_args(
    args: &mut impl Iterator<Item = String>,
) -> shardloom_core::Result<Option<VortexLocalPrimitiveCliExecutionRequest>> {
    let Some(option) = args.next() else {
        return Ok(None);
    };
    if option != "--execute-local-primitive" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unknown option: {option}"
        )));
    }
    let Some(memory_gb_text) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(
            "missing memory_gb after --execute-local-primitive".to_string(),
        ));
    };
    let Some(max_parallelism_text) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(
            "missing max_parallelism after --execute-local-primitive".to_string(),
        ));
    };
    if let Some(extra) = args.next() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unknown option: {extra}"
        )));
    }
    let memory_gb = memory_gb_text.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation("memory_gb must be an unsigned integer".to_string())
    })?;
    if memory_gb == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "memory_gb must be >= 1".to_string(),
        ));
    }
    let max_parallelism = max_parallelism_text.parse::<usize>().map_err(|_| {
        ShardLoomError::InvalidOperation("max_parallelism must be an unsigned integer".to_string())
    })?;
    VortexLocalPrimitiveExecutionPolicy::new(max_parallelism)?;
    Ok(Some(VortexLocalPrimitiveCliExecutionRequest {
        memory_gb,
        max_parallelism,
    }))
}

pub(crate) fn vortex_count_where_filter_evidence(
    predicate: &PredicateExpr,
    summary: &VortexMetadataSummaryReport,
) -> shardloom_core::Result<VortexCountWhereFilterEvidence> {
    let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(predicate, summary);
    let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);
    let filter_kernel_admission = admit_vortex_selection_vector_filter_kernel(&filter_kernel)?;
    Ok(VortexCountWhereFilterEvidence {
        predicate_evaluation,
        filter_kernel,
        filter_kernel_admission,
    })
}

pub(crate) fn vortex_count_where_local_execution_evidence(
    request: &VortexQueryPrimitiveRequest,
    local_request: &VortexCountWhereLocalExecutionRequest,
) -> shardloom_core::Result<VortexCountWhereLocalExecutionEvidence> {
    vortex_local_primitive_cli_execution_evidence(request, local_request)
}

pub(crate) fn vortex_local_primitive_cli_execution_evidence(
    request: &VortexQueryPrimitiveRequest,
    local_request: &VortexLocalPrimitiveCliExecutionRequest,
) -> shardloom_core::Result<VortexLocalPrimitiveCliExecutionEvidence> {
    let _memory_budget = MemoryBudget::from_gib(local_request.memory_gb)?;
    let policy = VortexLocalPrimitiveExecutionPolicy::new(local_request.max_parallelism)?;
    let report = execute_vortex_local_primitive_with_policy(request, policy)?;
    let native_io_certificate = local_primitive_native_io_certificate(request, &report)?;
    let execution_certificate = local_primitive_correctness_fixture_for_request(request, &report)
        .map(|fixture| local_primitive_execution_certificate(&fixture, request, &report))
        .transpose()?;
    Ok(VortexLocalPrimitiveCliExecutionEvidence {
        memory_gb: local_request.memory_gb,
        max_parallelism: local_request.max_parallelism,
        report,
        native_io_certificate,
        execution_certificate,
    })
}

pub(crate) fn vortex_count_where_human_text(
    result: &VortexQueryPrimitiveResult,
    evidence: &VortexCountWhereFilterEvidence,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) -> String {
    let mut sections = vec![
        result.to_human_text(),
        evidence.predicate_evaluation.to_human_text(),
        evidence.filter_kernel.to_human_text(),
        evidence.filter_kernel_admission.to_human_text(),
    ];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_count_where_fields(
    result: &VortexQueryPrimitiveResult,
    count: Option<u64>,
    predicate_arg: String,
    evidence: &VortexCountWhereFilterEvidence,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let execution = local_execution.map_or(
        "metadata_or_selection_vector_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_count_where_primitive_performed".to_string()
            } else {
                "local_vortex_count_where_primitive_not_performed".to_string()
            }
        },
    );
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_count_where".to_string()),
        ("primitive".to_string(), "count_where".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        (
            "metadata_query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), count.is_some().to_string()),
        (
            "count".to_string(),
            count.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
    ];
    append_vortex_count_where_filter_evidence_fields(&mut fields, evidence);
    append_vortex_count_where_local_execution_fields(&mut fields, local_execution);
    fields
}

fn append_vortex_count_where_filter_evidence_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountWhereFilterEvidence,
) {
    append_vortex_count_where_predicate_evidence_fields(fields, &evidence.predicate_evaluation);
    append_vortex_count_where_filter_kernel_fields(fields, &evidence.filter_kernel);
    append_vortex_count_where_filter_admission_fields(fields, &evidence.filter_kernel_admission);
    push_bool_field(
        fields,
        "filtered_count_selection_vector_evidence_present",
        evidence
            .filter_kernel
            .is_safe_native_filter_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "filtered_count_generalized_execution_allowed",
        false,
    );
    push_bool_field(fields, "filtered_count_production_claim_allowed", false);
    push_bool_field(
        fields,
        "filtered_count_requires_encoded_value_kernel",
        evidence.predicate_evaluation.status
            == VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues,
    );
    push_bool_field(fields, "filtered_count_requires_benchmark_evidence", true);
    push_bool_field(fields, "filtered_count_cg2_closeout_allowed", false);
    push_bool_field(fields, "filtered_count_cg13_closeout_allowed", false);
}

pub(crate) fn append_vortex_count_where_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) {
    append_vortex_count_where_local_execution_request_fields(fields, local_execution);
    match local_execution {
        Some(local) => append_vortex_count_where_local_execution_present_fields(fields, local),
        None => append_vortex_count_where_local_execution_absent_fields(fields),
    }
}

fn append_vortex_count_where_local_execution_request_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
}

fn append_vortex_count_where_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "filtered_count_local_execution_status",
        "not_requested",
    );
    push_field(
        fields,
        "filtered_count_local_execution_mode",
        "not_requested",
    );
    push_u64_field(fields, "filtered_count_local_execution_memory_gb", 0);
    push_count_field(fields, "filtered_count_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filtered_count_local_execution_result_known", false);
    push_field(fields, "filtered_count_local_execution_count", "unknown");
    append_vortex_count_where_local_execution_absent_effect_fields(fields);
    append_vortex_count_where_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_count_where_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    append_vortex_count_where_local_execution_report_fields(fields, local);
    append_vortex_count_where_local_execution_effect_fields(fields, local);
    append_vortex_count_where_local_execution_claim_fields(
        fields,
        local.selection_vector_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_count_where_local_execution_report_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    push_field(
        fields,
        "filtered_count_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(
        fields,
        "filtered_count_local_execution_memory_gb",
        local.memory_gb,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_result_known",
        local.count().is_some(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_count",
        &local
            .count()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_u64_field(
        fields,
        "filtered_count_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_field(
        fields,
        "filtered_count_local_execution_rows_selected",
        &local
            .report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
}

fn append_vortex_count_where_local_execution_absent_effect_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_bool_field(fields, "filtered_count_local_execution_data_read", false);
    push_bool_field(fields, "filtered_count_local_execution_data_decoded", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_materialized",
        false,
    );
    push_bool_field(fields, "filtered_count_local_execution_row_read", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_arrow_converted",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_object_store_io",
        false,
    );
    push_bool_field(fields, "filtered_count_local_execution_write_io", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_spill_io_performed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_attempted",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_count_where_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    selection_vector_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_selection_vector_guarantee",
        selection_vector_guarantee,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_project_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_project_fields(
    result: &VortexQueryPrimitiveResult,
    columns_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let row_read = local_execution.is_some_and(|local| local.report.row_read);
    let arrow_converted = local_execution.is_some_and(|local| local.report.arrow_converted);
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_or_projection_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_project_primitive_performed".to_string()
            } else {
                "local_vortex_project_primitive_not_performed".to_string()
            }
        },
    );
    let output_columns = split_column_arg(&columns_arg);
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_project".to_string()),
        ("primitive".to_string(), "project_columns".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("row_read".to_string(), row_read.to_string()),
        ("arrow_converted".to_string(), arrow_converted.to_string()),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_projected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("columns".to_string(), columns_arg),
    ];
    append_vortex_project_local_execution_fields(&mut fields, local_execution);
    append_vortex_primitive_scan_pushdown_fields(
        &mut fields,
        "vortex_project",
        &[],
        &output_columns,
        None,
        local_execution,
    );
    fields
}

pub(crate) fn append_vortex_project_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "project_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "project_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "project_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_project_local_execution_present_fields(fields, local),
        None => append_vortex_project_local_execution_absent_fields(fields),
    }
}

fn append_vortex_project_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(fields, "project_local_execution_status", "not_requested");
    push_field(fields, "project_local_execution_mode", "not_requested");
    push_u64_field(fields, "project_local_execution_memory_gb", 0);
    push_count_field(fields, "project_local_execution_max_parallelism", 0);
    push_bool_field(fields, "project_local_execution_result_known", false);
    push_field(fields, "project_local_execution_rows_projected", "unknown");
    push_field(fields, "project_local_execution_projected_columns", "");
    append_vortex_project_local_execution_absent_effect_fields(fields);
    append_vortex_project_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_project_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "project_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "project_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(fields, "project_local_execution_memory_gb", local.memory_gb);
    push_count_field(
        fields,
        "project_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "project_local_execution_result_known",
        local.projected_rows().is_some(),
    );
    push_field(
        fields,
        "project_local_execution_rows_projected",
        &local
            .projected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "project_local_execution_projected_columns",
        &local.report.projected_columns.join(","),
    );
    push_u64_field(
        fields,
        "project_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "project_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "project_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "project_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "project_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "project_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "project_local_execution_projection_pushdown_applied",
        local.report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "project_local_execution_upstream_projection_expression_used",
        local.report.upstream_projection_expression_used,
    );
    append_vortex_project_local_execution_effect_fields(fields, local);
    append_vortex_project_local_execution_claim_fields(
        fields,
        local.projection_encoded_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_project_local_execution_absent_effect_fields(fields: &mut Vec<(String, String)>) {
    push_bool_field(fields, "project_local_execution_data_read", false);
    push_bool_field(fields, "project_local_execution_data_decoded", false);
    push_bool_field(fields, "project_local_execution_data_materialized", false);
    push_bool_field(fields, "project_local_execution_row_read", false);
    push_bool_field(fields, "project_local_execution_arrow_converted", false);
    push_bool_field(fields, "project_local_execution_object_store_io", false);
    push_bool_field(fields, "project_local_execution_write_io", false);
    push_bool_field(fields, "project_local_execution_spill_io_performed", false);
    push_bool_field(fields, "project_local_execution_fallback_attempted", false);
    push_bool_field(
        fields,
        "project_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_project_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "project_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "project_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "project_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "project_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "project_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "project_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "project_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "project_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "project_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "project_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "project_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_project_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    encoded_projection_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "project_local_execution_encoded_projection_guarantee",
        encoded_projection_guarantee,
    );
    push_bool_field(
        fields,
        "project_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "project_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "project_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_filter_project_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_filter_project_fields(
    result: &VortexQueryPrimitiveResult,
    predicate_arg: String,
    columns_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let row_read = local_execution.is_some_and(|local| local.report.row_read);
    let arrow_converted = local_execution.is_some_and(|local| local.report.arrow_converted);
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_filter_project_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_filter_project_primitive_performed".to_string()
            } else {
                "local_vortex_filter_project_primitive_not_performed".to_string()
            }
        },
    );
    let filter_columns = predicate_columns_from_arg(&predicate_arg);
    let output_columns = split_column_arg(&columns_arg);
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_filter_project".to_string()),
        ("primitive".to_string(), "filter_and_project".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("row_read".to_string(), row_read.to_string()),
        ("arrow_converted".to_string(), arrow_converted.to_string()),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_selected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        (
            "rows_projected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
        ("columns".to_string(), columns_arg),
        (
            "source_order_limit".to_string(),
            result
                .request
                .source_order_limit
                .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
        ),
    ];
    append_vortex_filter_project_local_execution_fields(&mut fields, local_execution);
    append_vortex_primitive_scan_pushdown_fields(
        &mut fields,
        "vortex_filter_project",
        &filter_columns,
        &output_columns,
        result.request.source_order_limit,
        local_execution,
    );
    fields
}

pub(crate) fn append_vortex_filter_project_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filter_project_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filter_project_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_filter_project_local_execution_present_fields(fields, local),
        None => append_vortex_filter_project_local_execution_absent_fields(fields),
    }
}

fn append_vortex_filter_project_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "filter_project_local_execution_status",
        "not_requested",
    );
    push_field(
        fields,
        "filter_project_local_execution_mode",
        "not_requested",
    );
    push_u64_field(fields, "filter_project_local_execution_memory_gb", 0);
    push_count_field(fields, "filter_project_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filter_project_local_execution_result_known", false);
    push_field(
        fields,
        "filter_project_local_execution_rows_selected",
        "unknown",
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_projected",
        "unknown",
    );
    push_field(
        fields,
        "filter_project_local_execution_projected_columns",
        "",
    );
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_requested",
        "none",
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_source_order_limit_applied",
        false,
    );
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_input_rows",
        "unknown",
    );
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_rows_output",
        "unknown",
    );
    append_vortex_filter_project_local_execution_absent_effect_fields(fields);
    append_vortex_filter_project_local_execution_claim_fields(fields, None);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_filter_project_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "filter_project_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filter_project_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(
        fields,
        "filter_project_local_execution_memory_gb",
        local.memory_gb,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_result_known",
        local.projected_rows().is_some(),
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_selected",
        &local
            .selected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_projected",
        &local
            .projected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "filter_project_local_execution_projected_columns",
        &local.report.projected_columns.join(","),
    );
    append_vortex_filter_project_local_execution_limit_fields(fields, local);
    append_vortex_filter_project_local_execution_scan_fields(fields, local);
    append_vortex_filter_project_local_execution_effect_fields(fields, local);
    append_vortex_filter_project_local_execution_claim_fields(fields, Some(local));
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_filter_project_local_execution_limit_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_requested",
        &local
            .report
            .source_order_limit_requested
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_source_order_limit_applied",
        local.report.source_order_limit_applied,
    );
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_input_rows",
        &local
            .report
            .source_order_limit_input_rows
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "filter_project_local_execution_source_order_limit_rows_output",
        &local
            .report
            .source_order_limit_rows_output
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
}

fn append_vortex_filter_project_local_execution_scan_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_u64_field(
        fields,
        "filter_project_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_projection_pushdown_applied",
        local.report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_upstream_projection_expression_used",
        local.report.upstream_projection_expression_used,
    );
}

fn append_vortex_filter_project_local_execution_absent_effect_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_bool_field(fields, "filter_project_local_execution_data_read", false);
    push_bool_field(fields, "filter_project_local_execution_data_decoded", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_data_materialized",
        false,
    );
    push_bool_field(fields, "filter_project_local_execution_row_read", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_arrow_converted",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_object_store_io",
        false,
    );
    push_bool_field(fields, "filter_project_local_execution_write_io", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_spill_io_performed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_attempted",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_filter_project_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filter_project_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_filter_project_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    let encoded_guarantee = local_execution
        .is_some_and(VortexLocalPrimitiveCliExecutionEvidence::filter_project_encoded_guaranteed);
    let native_io_certified =
        local_execution.is_some_and(|local| local.native_io_certificate.is_certified());
    let correctness_certified = local_execution.is_some_and(|local| {
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified)
    });
    push_bool_field(
        fields,
        "filter_project_local_execution_selection_vector_guarantee",
        encoded_guarantee,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_projection_pushdown_guarantee",
        encoded_guarantee,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_filter_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_filter_fields(
    result: &VortexQueryPrimitiveResult,
    predicate_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_or_selection_vector_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_filter_primitive_performed".to_string()
            } else {
                "local_vortex_filter_primitive_not_performed".to_string()
            }
        },
    );
    let filter_columns = predicate_columns_from_arg(&predicate_arg);
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_filter".to_string()),
        ("primitive".to_string(), "filter_predicate".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_selected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
    ];
    append_vortex_filter_local_execution_fields(&mut fields, local_execution);
    append_vortex_primitive_scan_pushdown_fields(
        &mut fields,
        "vortex_filter",
        &filter_columns,
        &[],
        None,
        local_execution,
    );
    fields
}

pub(crate) fn append_vortex_filter_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filter_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filter_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filter_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_filter_local_execution_present_fields(fields, local),
        None => append_vortex_filter_local_execution_absent_fields(fields),
    }
}

fn append_vortex_filter_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(fields, "filter_local_execution_status", "not_requested");
    push_field(fields, "filter_local_execution_mode", "not_requested");
    push_u64_field(fields, "filter_local_execution_memory_gb", 0);
    push_count_field(fields, "filter_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filter_local_execution_result_known", false);
    push_field(fields, "filter_local_execution_rows_selected", "unknown");
    append_vortex_filter_local_execution_absent_effect_fields(fields);
    append_vortex_filter_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_filter_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "filter_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filter_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(fields, "filter_local_execution_memory_gb", local.memory_gb);
    push_count_field(
        fields,
        "filter_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filter_local_execution_result_known",
        local.selected_rows().is_some(),
    );
    push_field(
        fields,
        "filter_local_execution_rows_selected",
        &local
            .selected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_u64_field(
        fields,
        "filter_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "filter_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filter_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filter_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filter_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filter_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filter_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
    append_vortex_filter_local_execution_effect_fields(fields, local);
    append_vortex_filter_local_execution_claim_fields(
        fields,
        local.selection_vector_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_filter_local_execution_absent_effect_fields(fields: &mut Vec<(String, String)>) {
    push_bool_field(fields, "filter_local_execution_data_read", false);
    push_bool_field(fields, "filter_local_execution_data_decoded", false);
    push_bool_field(fields, "filter_local_execution_data_materialized", false);
    push_bool_field(fields, "filter_local_execution_row_read", false);
    push_bool_field(fields, "filter_local_execution_arrow_converted", false);
    push_bool_field(fields, "filter_local_execution_object_store_io", false);
    push_bool_field(fields, "filter_local_execution_write_io", false);
    push_bool_field(fields, "filter_local_execution_spill_io_performed", false);
    push_bool_field(fields, "filter_local_execution_fallback_attempted", false);
    push_bool_field(
        fields,
        "filter_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_filter_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filter_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filter_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filter_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filter_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filter_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filter_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filter_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filter_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filter_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filter_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filter_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_filter_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    selection_vector_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "filter_local_execution_selection_vector_guarantee",
        selection_vector_guarantee,
    );
    push_bool_field(
        fields,
        "filter_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filter_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filter_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(fields, "filter_local_execution_cg2_closeout_allowed", false);
    push_bool_field(
        fields,
        "filter_local_execution_cg13_closeout_allowed",
        false,
    );
}

fn append_vortex_primitive_scan_pushdown_fields(
    fields: &mut Vec<(String, String)>,
    primitive: &str,
    filter_columns: &[String],
    output_columns: &[String],
    source_order_limit: Option<usize>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    let filter_required = !filter_columns.is_empty();
    let projection_required = !output_columns.is_empty();
    let limit_required = source_order_limit.is_some();
    let scan_admitted = local_execution.is_some_and(|local| {
        local.report.mode == shardloom_vortex::VortexLocalPrimitiveExecutionMode::VortexScanPushdown
            && local.report.upstream_scan_called
    });
    let filter_pushed_down = local_execution.is_some_and(|local| {
        scan_admitted && filter_required && local.report.filter_pushdown_applied
    });
    let projection_pushed_down = local_execution.is_some_and(|local| {
        scan_admitted && projection_required && local.report.projection_pushdown_applied
    });
    let limit_pushed_down = false;
    let filter = ScanPushdownDimensionEvidence::new(
        scan_admitted,
        filter_required,
        filter_pushed_down,
        "filter",
    );
    let projection = ScanPushdownDimensionEvidence::new(
        scan_admitted,
        projection_required,
        projection_pushed_down,
        "projection",
    );
    let limit = ScanPushdownDimensionEvidence::new(
        scan_admitted,
        limit_required,
        limit_pushed_down,
        "limit",
    );
    let filter_only_columns = filter_columns
        .iter()
        .filter(|column| !output_columns.iter().any(|output| output == *column))
        .cloned()
        .collect::<Vec<_>>();
    let admission = scan_pushdown_admission(local_execution.is_some(), scan_admitted);
    let mut runtime = ScanPushdownRuntimeEvidence {
        admission,
        any_pushdown: filter.pushed_down || projection.pushed_down || limit.pushed_down,
        has_blocker: false,
    };
    let blocker_id = scan_pushdown_blocker_id(&runtime, filter, projection, limit);
    runtime.has_blocker = blocker_id != "none";
    let pushdown_status = scan_pushdown_status(&runtime);

    push_field(
        fields,
        "scan_pushdown_schema_version",
        VORTEX_PRIMITIVE_SCAN_PUSHDOWN_SCHEMA_VERSION,
    );
    push_field(
        fields,
        "scan_pushdown_report_id",
        &format!("gar-runtime-impl-4i.scan_pushdown.{primitive}"),
    );
    push_field(fields, "scan_pushdown_status", pushdown_status);
    push_scan_pushdown_dimension_fields(fields, filter, projection, limit);
    push_scan_pushdown_column_fields(fields, filter_columns, output_columns, &filter_only_columns);
    push_scan_pushdown_residual_limit_fields(
        fields,
        source_order_limit,
        limit_pushed_down,
        local_execution,
    );
    push_scan_pushdown_guardrail_fields(fields, local_execution, &blocker_id);
}

#[derive(Clone, Copy)]
struct ScanPushdownDimensionEvidence {
    required: bool,
    pushed_down: bool,
    status: &'static str,
}

impl ScanPushdownDimensionEvidence {
    fn new(scan_admitted: bool, required: bool, pushed_down: bool, dimension: &str) -> Self {
        Self {
            required,
            pushed_down,
            status: scan_pushdown_dimension_status(scan_admitted, required, pushed_down, dimension),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ScanPushdownAdmission {
    NotExecuted,
    UnsupportedNoScan,
    Admitted,
}

struct ScanPushdownRuntimeEvidence {
    admission: ScanPushdownAdmission,
    any_pushdown: bool,
    has_blocker: bool,
}

fn push_scan_pushdown_dimension_fields(
    fields: &mut Vec<(String, String)>,
    filter: ScanPushdownDimensionEvidence,
    projection: ScanPushdownDimensionEvidence,
    limit: ScanPushdownDimensionEvidence,
) {
    push_bool_field(fields, "scan_filter_required", filter.required);
    push_bool_field(fields, "scan_projection_required", projection.required);
    push_bool_field(fields, "scan_limit_required", limit.required);
    push_bool_field(fields, "scan_filter_pushed_down", filter.pushed_down);
    push_bool_field(
        fields,
        "scan_projection_pushed_down",
        projection.pushed_down,
    );
    push_bool_field(fields, "scan_limit_pushed_down", limit.pushed_down);
    push_field(fields, "scan_filter_pushdown_status", filter.status);
    push_field(fields, "scan_projection_pushdown_status", projection.status);
    push_field(fields, "scan_limit_pushdown_status", limit.status);
}

fn push_scan_pushdown_column_fields(
    fields: &mut Vec<(String, String)>,
    filter_columns: &[String],
    output_columns: &[String],
    filter_only_columns: &[String],
) {
    push_field(
        fields,
        "scan_filter_columns_read",
        &comma_join_columns_or_none(filter_columns),
    );
    push_field(
        fields,
        "scan_output_columns_read",
        &comma_join_columns_or_none(output_columns),
    );
    push_field(
        fields,
        "scan_filter_only_columns_read",
        &comma_join_columns_or_none(filter_only_columns),
    );
}

fn push_scan_pushdown_residual_limit_fields(
    fields: &mut Vec<(String, String)>,
    source_order_limit: Option<usize>,
    limit_pushed_down: bool,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    let residual_required = source_order_limit.is_some() && !limit_pushed_down;
    let residual_applied = local_execution
        .is_some_and(|local| local.report.source_order_limit_applied && residual_required);
    push_field(
        fields,
        "scan_limit_requested_rows",
        &source_order_limit.map_or_else(|| "none".to_string(), |limit| limit.to_string()),
    );
    push_bool_field(fields, "scan_residual_limit_required", residual_required);
    push_bool_field(fields, "scan_residual_limit_applied", residual_applied);
    push_field(
        fields,
        "scan_residual_limit_status",
        match (residual_required, residual_applied) {
            (false, _) => "not_needed",
            (true, true) => "applied_by_shardloom_native_residual",
            (true, false) => "blocked_or_not_executed",
        },
    );
    push_field(
        fields,
        "scan_residual_limit_executor",
        if residual_applied {
            "shardloom_native"
        } else if residual_required {
            "unsupported_blocked_or_not_executed"
        } else {
            "none"
        },
    );
    push_field(
        fields,
        "scan_residual_limit_input_rows",
        &local_execution
            .and_then(|local| local.report.source_order_limit_input_rows)
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "scan_residual_limit_rows_output",
        &local_execution
            .and_then(|local| local.report.source_order_limit_rows_output)
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
}

fn push_scan_pushdown_guardrail_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
    blocker_id: &str,
) {
    push_bool_field(
        fields,
        "scan_data_materialized",
        local_execution.is_some_and(|local| local.report.data_materialized),
    );
    push_bool_field(
        fields,
        "scan_data_decoded",
        local_execution.is_some_and(|local| local.report.data_decoded),
    );
    push_field(fields, "scan_pushdown_blocker_id", blocker_id);
    push_field(
        fields,
        "scan_pushdown_claim_gate_status",
        "fixture_smoke_only",
    );
    push_field(
        fields,
        "scan_pushdown_claim_boundary",
        "scoped local Vortex primitive scan pushdown evidence only; no encoded-native, SQL/DataFrame, object-store/lakehouse, production, performance, or Spark-replacement claim",
    );
    push_bool_field(fields, "scan_pushdown_fallback_attempted", false);
    push_bool_field(fields, "scan_pushdown_external_engine_invoked", false);
}

fn scan_pushdown_status(runtime: &ScanPushdownRuntimeEvidence) -> &'static str {
    match runtime.admission {
        ScanPushdownAdmission::NotExecuted => "not_executed",
        ScanPushdownAdmission::UnsupportedNoScan => "scan_pushdown_unsupported",
        ScanPushdownAdmission::Admitted if !runtime.has_blocker && runtime.any_pushdown => {
            "scan_pushdown_supported"
        }
        ScanPushdownAdmission::Admitted if !runtime.has_blocker => "scan_pushdown_not_needed",
        ScanPushdownAdmission::Admitted if runtime.any_pushdown => {
            "scan_pushdown_partially_supported"
        }
        ScanPushdownAdmission::Admitted => "scan_pushdown_blocked",
    }
}

fn scan_pushdown_blocker_id(
    runtime: &ScanPushdownRuntimeEvidence,
    filter: ScanPushdownDimensionEvidence,
    projection: ScanPushdownDimensionEvidence,
    limit: ScanPushdownDimensionEvidence,
) -> String {
    let mut blocker_ids = Vec::new();
    match runtime.admission {
        ScanPushdownAdmission::NotExecuted => {
            blocker_ids.push("gar-runtime-impl-4i.local_primitive_scan_not_executed");
        }
        ScanPushdownAdmission::UnsupportedNoScan => {
            blocker_ids.push("gar-runtime-impl-4i.vortex_scan_not_admitted");
        }
        ScanPushdownAdmission::Admitted => {}
    }
    if runtime.admission == ScanPushdownAdmission::Admitted
        && filter.required
        && !filter.pushed_down
    {
        blocker_ids.push("gar-runtime-impl-4i.filter_pushdown_not_lowered");
    }
    if runtime.admission == ScanPushdownAdmission::Admitted
        && projection.required
        && !projection.pushed_down
    {
        blocker_ids.push("gar-runtime-impl-4i.projection_pushdown_not_lowered");
    }
    if runtime.admission == ScanPushdownAdmission::Admitted && limit.required && !limit.pushed_down
    {
        blocker_ids.push("gar-runtime-impl-4i.limit_pushdown_not_admitted");
    }

    if blocker_ids.is_empty() {
        "none".to_string()
    } else {
        blocker_ids.join(";")
    }
}

fn scan_pushdown_admission(
    local_execution_present: bool,
    scan_admitted: bool,
) -> ScanPushdownAdmission {
    match (local_execution_present, scan_admitted) {
        (false, _) => ScanPushdownAdmission::NotExecuted,
        (true, false) => ScanPushdownAdmission::UnsupportedNoScan,
        (true, true) => ScanPushdownAdmission::Admitted,
    }
}

fn scan_pushdown_dimension_status(
    scan_admitted: bool,
    required: bool,
    pushed_down: bool,
    dimension: &str,
) -> &'static str {
    match (scan_admitted, required, pushed_down) {
        (false, _, _) => "unsupported_no_vortex_scan",
        (true, false, _) => "not_needed",
        (true, true, true) => "pushed_down",
        (true, true, false) if dimension == "filter" => "blocked_filter_not_lowered",
        (true, true, false) if dimension == "limit" => "blocked_no_scan_limit_admission",
        (true, true, false) => "blocked_projection_not_lowered",
    }
}

fn predicate_columns_from_arg(predicate_arg: &str) -> Vec<String> {
    match predicate_arg.split(':').collect::<Vec<_>>().as_slice() {
        ["is_null" | "is_not_null", column] | [_, column, _] => vec![(*column).to_string()],
        _ => Vec::new(),
    }
}

fn split_column_arg(columns_arg: &str) -> Vec<String> {
    columns_arg
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn comma_join_columns_or_none(columns: &[String]) -> String {
    if columns.is_empty() {
        "none".to_string()
    } else {
        columns.join(",")
    }
}

fn append_vortex_count_where_predicate_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexEncodedPredicateEvaluationReport,
) {
    push_bool_field(fields, "encoded_predicate_evidence_emitted", true);
    push_field(fields, "encoded_predicate_status", report.status.as_str());
    push_count_field(
        fields,
        "encoded_predicate_segment_report_count",
        report.segment_report_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selected_all_count",
        report.selected_all_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selected_none_count",
        report.selected_none_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_needs_encoded_values_count",
        report.needs_encoded_values_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selection_vectors_emitted",
        report.selection_vectors_emitted,
    );
    push_field(
        fields,
        "encoded_predicate_selected_rows_metadata_count",
        &report
            .selected_rows_metadata_count
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(fields, "encoded_predicate_data_read", report.data_read);
    push_bool_field(
        fields,
        "encoded_predicate_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_predicate_data_materialized",
        report.data_materialized,
    );
    push_bool_field(fields, "encoded_predicate_row_read", report.row_read);
    push_bool_field(
        fields,
        "encoded_predicate_arrow_converted",
        report.arrow_converted,
    );
    push_bool_field(
        fields,
        "encoded_predicate_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "encoded_predicate_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_filter_kernel_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexSelectionVectorFilterKernelReport,
) {
    push_bool_field(fields, "selection_vector_filter_kernel_emitted", true);
    push_field(
        fields,
        "selection_vector_filter_kernel_status",
        report.status.as_str(),
    );
    push_count_field(
        fields,
        "selection_vector_filter_kernel_segment_count",
        report.segment_count,
    );
    push_count_field(
        fields,
        "selection_vector_filter_kernel_selection_vector_count",
        report.selection_vector_count,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_selected_row_count",
        &report
            .selected_row_count
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_safe_evidence",
        report.is_safe_native_filter_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_materialized",
        report.data_materialized,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_filter_admission_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexSelectionVectorFilterKernelAdmissionReport,
) {
    push_bool_field(fields, "selection_vector_filter_admission_emitted", true);
    push_field(
        fields,
        "selection_vector_filter_admission_status",
        report.status.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_slot_marked_present",
        report.slot_marked_present,
    );
    push_field(
        fields,
        "selection_vector_filter_admission_correctness_evidence",
        report.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_admission_benchmark_evidence",
        report.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_runtime_execution_allowed",
        report.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

pub(crate) fn parse_projection_columns(value: &str) -> Result<ProjectionRequest, ShardLoomError> {
    if value == "*" {
        return Ok(ProjectionRequest::all());
    }
    let columns: Result<Vec<_>, _> = value
        .split(',')
        .map(str::trim)
        .map(|name| {
            if name.is_empty() {
                return Err(ShardLoomError::InvalidOperation(
                    "projection contains empty column name".to_string(),
                ));
            }
            shardloom_core::ColumnRef::new(name)
        })
        .collect();
    Ok(ProjectionRequest::columns(columns?))
}

pub(crate) fn parse_vortex_primitive_request(
    uri: DatasetUri,
    primitive_arg: &str,
) -> Result<shardloom_vortex::VortexQueryPrimitiveRequest, ShardLoomError> {
    if primitive_arg == "count" {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::count_all(
            uri,
        ))
    } else if let Some(pred) = primitive_arg.strip_prefix("count-where:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::count_where(
            uri,
            parse_tiny_predicate(pred)?,
        ))
    } else if let Some(cols) = primitive_arg.strip_prefix("project:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::project(
            uri,
            parse_projection_columns(cols)?,
        ))
    } else if let Some(pred) = primitive_arg.strip_prefix("filter:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::filter(
            uri,
            parse_tiny_predicate(pred)?,
        ))
    } else {
        for prefix in ["filter-project:", "filter-and-project:"] {
            if let Some(value) = primitive_arg.strip_prefix(prefix) {
                let Some((predicate, columns)) = value.split_once('|') else {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project requires <predicate>|<columns>".to_string(),
                    ));
                };
                if predicate.is_empty() {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project predicate must not be empty".to_string(),
                    ));
                }
                if columns.is_empty() {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project columns must not be empty".to_string(),
                    ));
                }
                return Ok(
                    shardloom_vortex::VortexQueryPrimitiveRequest::filter_and_project(
                        uri,
                        parse_tiny_predicate(predicate)?,
                        parse_projection_columns(columns)?,
                    ),
                );
            }
        }
        Err(ShardLoomError::InvalidOperation("invalid primitive; expected count, count-where:<predicate>, project:<columns>, filter:<predicate>, filter-project:<predicate>|<columns>".to_string()))
    }
}

pub(crate) fn build_vortex_encoded_count_readiness(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<shardloom_vortex::VortexEncodedReadReadinessReport> {
    let source = shardloom_core::UniversalInputSource::from_dataset_uri(uri)?;
    let input_plan = plan_native_vortex_universal_input(source)?;
    let read_report = plan_vortex_read_from_universal_input(input_plan)?;
    let runtime_report = build_vortex_runtime_task_graph(read_report)?;
    let sizing_report = size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    )?;
    let budget = MemoryBudget::from_gib(memory_gb)?;
    let memory_report = plan_vortex_memory_safety(sizing_report, budget)?;
    let mut scheduler_report = plan_vortex_scheduler_queue(memory_report, max_parallelism)?;
    if scheduler_report.scheduled_task_count == 0 {
        scheduler_report
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "approved local encoded count execution",
            ));
        scheduler_report.recompute_counts();
    }
    evaluate_vortex_encoded_read_readiness(scheduler_report)
}

pub(crate) fn run_vortex_approved_local_encoded_count_from_readiness(
    uri: DatasetUri,
    readiness_report: &shardloom_vortex::VortexEncodedReadReadinessReport,
) -> shardloom_core::Result<(
    shardloom_vortex::VortexEncodedReadExecutionReport,
    VortexLocalExecutionReport,
)> {
    let count_report = plan_vortex_count_readiness(
        VortexCountReadinessRequest::new(uri, VortexCountCandidateSource::EncodedDataPath)
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .count_primitive(true)
            .encoded_data_path_ready(true),
    )?;
    let approval = plan_vortex_encoded_count_data_path_approval(
        count_report,
        vortex_encoded_read_local_scan_count_api_boundary(),
    )?;
    let report = execute_vortex_count_all_from_approved_local_scan(&approval, readiness_report)?;
    let local_execution_report =
        execute_vortex_count_all_from_approved_local_scan_result(&approval, &report)?;
    Ok((report, local_execution_report))
}

pub(crate) fn run_vortex_approved_local_encoded_count(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<(
    shardloom_vortex::VortexEncodedReadExecutionReport,
    VortexLocalExecutionReport,
)> {
    let readiness_report =
        build_vortex_encoded_count_readiness(uri.clone(), memory_gb, max_parallelism)?;
    run_vortex_approved_local_encoded_count_from_readiness(uri, &readiness_report)
}

pub(crate) fn build_vortex_count_local_streaming_batch_plan(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<EncodedStreamingBatchPlanReport> {
    let dataset = DatasetRef::from_uri(uri)?;
    let input = EncodedStreamingBatchPlanInput::new(
        StreamingSource::vortex_dataset(dataset),
        StreamingSink::null_benchmark(),
        BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb)),
        max_parallelism,
    )?;
    plan_encoded_streaming_batches(input)
}

pub(crate) struct VortexCountLocalEncodedEvidence {
    pub(crate) target_policy: VortexCountLocalEncodedTargetPolicy,
    pub(crate) fixture_id: Option<String>,
    pub(crate) fixture_source_ref: Option<String>,
    pub(crate) native_io_certificate: Option<NativeIoCertificate>,
    pub(crate) certificate: Option<ExecutionCertificate>,
    pub(crate) physical_kernel: Option<VortexEncodedCountPhysicalKernelReport>,
    pub(crate) kernel_admission: Option<VortexEncodedCountKernelAdmissionReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VortexCountLocalEncodedTargetPolicy {
    KnownFixtureCertified,
    LocalVortexUncertified,
    Blocked,
}

impl VortexCountLocalEncodedTargetPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::KnownFixtureCertified => "known_fixture_certified",
            Self::LocalVortexUncertified => "local_vortex_uncertified",
            Self::Blocked => "blocked",
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::KnownFixtureCertified => {
                "target matches the repository encoded CountAll correctness fixture and has certified execution evidence"
            }
            Self::LocalVortexUncertified => {
                "target is an approved local .vortex CountAll execution but lacks fixture correctness and benchmark certification"
            }
            Self::Blocked => {
                "target does not have successful side-effect-free local encoded CountAll execution evidence"
            }
        }
    }

    const fn execution_allowed(self) -> bool {
        matches!(
            self,
            Self::KnownFixtureCertified | Self::LocalVortexUncertified
        )
    }

    const fn non_fixture_target(self) -> bool {
        matches!(self, Self::LocalVortexUncertified)
    }
}

impl VortexCountLocalEncodedEvidence {
    fn unavailable(
        encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
        local_report: &VortexLocalExecutionReport,
    ) -> shardloom_core::Result<Self> {
        let target_policy =
            if local_encoded_count_target_execution_allowed(encoded_report, local_report) {
                VortexCountLocalEncodedTargetPolicy::LocalVortexUncertified
            } else {
                VortexCountLocalEncodedTargetPolicy::Blocked
            };
        let native_io_certificate = target_policy
            .execution_allowed()
            .then(|| local_encoded_count_native_io_certificate(encoded_report, local_report))
            .transpose()?;
        Ok(Self {
            target_policy,
            fixture_id: None,
            fixture_source_ref: None,
            native_io_certificate,
            certificate: None,
            physical_kernel: None,
            kernel_admission: None,
        })
    }

    fn from_fixture(
        fixture: &CorrectnessFixture,
        native_io_certificate: NativeIoCertificate,
        certificate: ExecutionCertificate,
        physical_kernel: VortexEncodedCountPhysicalKernelReport,
        kernel_admission: VortexEncodedCountKernelAdmissionReport,
    ) -> Self {
        let target_policy = if certificate.is_certified() {
            VortexCountLocalEncodedTargetPolicy::KnownFixtureCertified
        } else {
            VortexCountLocalEncodedTargetPolicy::Blocked
        };
        Self {
            target_policy,
            fixture_id: Some(fixture.id.as_str().to_string()),
            fixture_source_ref: fixture.source_ref.clone(),
            native_io_certificate: Some(native_io_certificate),
            certificate: Some(certificate),
            physical_kernel: Some(physical_kernel),
            kernel_admission: Some(kernel_admission),
        }
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.target_policy == VortexCountLocalEncodedTargetPolicy::Blocked
            || self
                .native_io_certificate
                .as_ref()
                .is_some_and(NativeIoCertificate::has_errors)
            || self
                .certificate
                .as_ref()
                .is_some_and(|certificate| !certificate.is_certified())
            || self
                .physical_kernel
                .as_ref()
                .is_some_and(VortexEncodedCountPhysicalKernelReport::has_errors)
            || self
                .kernel_admission
                .as_ref()
                .is_some_and(VortexEncodedCountKernelAdmissionReport::has_errors)
    }

    pub(crate) fn diagnostics(&self) -> Vec<shardloom_core::Diagnostic> {
        let mut diagnostics = Vec::new();
        if let Some(native_io_certificate) = &self.native_io_certificate {
            diagnostics.extend(native_io_certificate.diagnostics.clone());
        }
        if let Some(certificate) = &self.certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
        if let Some(physical_kernel) = &self.physical_kernel {
            diagnostics.extend(physical_kernel.diagnostics.clone());
        }
        if let Some(kernel_admission) = &self.kernel_admission {
            diagnostics.extend(kernel_admission.diagnostics.clone());
        }
        diagnostics
    }

    pub(crate) fn human_sections(&self) -> Vec<String> {
        let mut sections = vec![format!(
            "Vortex local encoded CountAll target policy\npolicy: {}\nreason: {}\nexecution allowed: {}\ncorrectness certified: {}\nproduction claim allowed: false\nCG-2 closeout allowed: false\nCG-13 closeout allowed: false",
            self.target_policy.as_str(),
            self.target_policy.reason(),
            self.target_policy.execution_allowed(),
            self.certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        )];
        if let Some(native_io_certificate) = &self.native_io_certificate {
            sections.push(local_count_native_io_certificate_human_text(
                native_io_certificate,
            ));
        }
        if let Some(certificate) = &self.certificate {
            sections.push(certificate.to_human_text());
        }
        if let Some(physical_kernel) = &self.physical_kernel {
            sections.push(physical_kernel.to_human_text());
        }
        if let Some(kernel_admission) = &self.kernel_admission {
            sections.push(kernel_admission.to_human_text());
        }
        sections
    }
}

pub(crate) fn vortex_count_local_encoded_evidence(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
) -> shardloom_core::Result<VortexCountLocalEncodedEvidence> {
    let Some(fixture) = local_encoded_count_correctness_fixture_for_report(encoded_report) else {
        return VortexCountLocalEncodedEvidence::unavailable(encoded_report, local_report);
    };
    let native_io_certificate =
        local_encoded_count_native_io_certificate(encoded_report, local_report)?;
    let certificate =
        local_encoded_count_execution_certificate(&fixture, encoded_report, local_report)?;
    let physical_kernel = evaluate_vortex_local_encoded_count_physical_kernel(
        encoded_report,
        local_report,
        &certificate,
    );
    let kernel_admission = admit_vortex_encoded_count_kernel(&physical_kernel)?;
    Ok(VortexCountLocalEncodedEvidence::from_fixture(
        &fixture,
        native_io_certificate,
        certificate,
        physical_kernel,
        kernel_admission,
    ))
}

fn local_encoded_count_target_execution_allowed(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
) -> bool {
    let count_result_matches = encoded_report
        .count_result
        .is_some_and(|count| encoded_report.rows_counted == count);
    encoded_report.feature_status == VortexEncodedReadExecutorFeatureStatus::Enabled
        && encoded_report.status == VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        && encoded_report.mode == VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount
        && !encoded_report.has_errors()
        && encoded_report.data_read
        && encoded_report.upstream_scan_called
        && !encoded_report.data_decoded
        && !encoded_report.data_materialized
        && !encoded_report.row_read
        && !encoded_report.arrow_converted
        && !encoded_report.object_store_io
        && !encoded_report.write_io
        && !encoded_report.spill_io_performed
        && !encoded_report.external_effects_executed
        && !encoded_report.fallback_execution_allowed
        && encoded_report.local_scan_source_uri_matches_target
        && encoded_report.local_scan_target_uri.is_some()
        && encoded_report.local_scan_readiness_source_uri == encoded_report.local_scan_target_uri
        && count_result_matches
        && local_report.status == VortexLocalExecutionStatus::LocalEncodedCountExecuted
        && !local_report.has_errors()
        && local_report.tasks_executed
        && local_report.data_read
        && !local_report.data_decoded
        && !local_report.data_materialized
        && !local_report.object_store_io
        && !local_report.write_io
        && !local_report.spill_io_performed
        && !local_report.external_effects_executed
        && !local_report.fallback_execution_allowed
}

fn local_encoded_count_correctness_fixture_for_report(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
) -> Option<CorrectnessFixture> {
    if !encoded_report.local_scan_source_uri_matches_target {
        return None;
    }
    let target_uri = encoded_report.local_scan_target_uri.as_ref()?;
    local_encoded_count_correctness_fixture_for_target(target_uri)
}

pub(crate) fn local_encoded_count_correctness_fixture_for_target(
    target_uri: &DatasetUri,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            matches!(fixture.expected, ExpectedOutcome::EncodedCount { .. })
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

fn local_foundation_fixture_for_target(
    target_uri: &DatasetUri,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            fixture.id.as_str() == fixture_id
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

pub(crate) fn local_primitive_correctness_fixture_for_request(
    request: &VortexQueryPrimitiveRequest,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) -> Option<CorrectnessFixture> {
    if request.kind != report.primitive_kind
        || report.status != shardloom_vortex::VortexLocalPrimitiveExecutionStatus::Executed
        || report.has_errors()
    {
        return None;
    }
    match request.kind {
        shardloom_vortex::VortexQueryPrimitiveKind::CountAll => request
            .source_uri
            .as_ref()
            .and_then(local_encoded_count_correctness_fixture_for_target),
        shardloom_vortex::VortexQueryPrimitiveKind::CountWhere => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request)
                && local_struct_no_source_order_limit(request),
            "vortex-local-count-where-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::ProjectColumns => local_primitive_fixture_if(
            request,
            local_struct_metric_projection(request) && local_struct_no_source_order_limit(request),
            "vortex-local-project-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::FilterPredicate => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request)
                && local_struct_no_source_order_limit(request),
            "vortex-local-filter-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::FilterAndProject => {
            if local_struct_value_gte_three_predicate(request)
                && local_struct_metric_projection(request)
                && local_struct_source_order_limit(request, 2)
            {
                local_primitive_fixture_if(
                    request,
                    true,
                    "vortex-local-filter-project-limit-struct-five",
                )
            } else {
                local_primitive_fixture_if(
                    request,
                    local_struct_value_gte_three_predicate(request)
                        && local_struct_metric_projection(request)
                        && local_struct_no_source_order_limit(request),
                    "vortex-local-filter-project-struct-five",
                )
            }
        }
        shardloom_vortex::VortexQueryPrimitiveKind::SimpleAggregate
        | shardloom_vortex::VortexQueryPrimitiveKind::Unsupported => None,
    }
}

fn local_primitive_fixture_if(
    request: &VortexQueryPrimitiveRequest,
    matches_fixture_shape: bool,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    matches_fixture_shape
        .then_some(request.source_uri.as_ref())
        .flatten()
        .and_then(|source_uri| local_foundation_fixture_for_target(source_uri, fixture_id))
}

fn local_struct_value_gte_three_predicate(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        request.predicate.as_ref(),
        Some(PredicateExpr::Compare {
            column,
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3)
        }) if column.as_str() == "value"
    )
}

fn local_struct_metric_projection(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        &request.projection,
        ProjectionRequest::Columns(columns)
            if columns.len() == 1 && columns[0].as_str() == "metric"
    )
}

fn local_struct_source_order_limit(request: &VortexQueryPrimitiveRequest, limit: usize) -> bool {
    request.source_order_limit == Some(limit)
}

fn local_struct_no_source_order_limit(request: &VortexQueryPrimitiveRequest) -> bool {
    request.source_order_limit.is_none()
}

fn local_fixture_ref_matches(target_uri: &DatasetUri, source_ref: &str) -> bool {
    let Some(target_ref) = canonical_local_fixture_ref(target_uri.as_str()) else {
        return false;
    };
    let Some(workspace_source_ref) = canonical_workspace_fixture_ref(source_ref) else {
        return false;
    };
    target_ref == workspace_source_ref
}

fn canonical_workspace_fixture_ref(source_ref: &str) -> Option<String> {
    let source_ref = normalized_local_fixture_ref(source_ref);
    let source_path = std::path::Path::new(&source_ref);
    let absolute = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        workspace_root().join(source_path)
    };
    canonical_path_string(&absolute)
}

fn canonical_local_fixture_ref(value: &str) -> Option<String> {
    let target_ref = normalized_local_fixture_ref(value);
    let target_path = std::path::Path::new(&target_ref);
    let absolute = if target_path.is_absolute() {
        target_path.to_path_buf()
    } else {
        workspace_root().join(target_path)
    };
    canonical_path_string(&absolute)
}

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn canonical_path_string(path: &std::path::Path) -> Option<String> {
    path.canonicalize()
        .ok()
        .map(|path| path.to_string_lossy().replace('\\', "/"))
}

fn normalized_local_fixture_ref(value: &str) -> String {
    let without_fragment = value
        .split_once(['?', '#'])
        .map_or(value, |(prefix, _)| prefix);
    let without_scheme = without_fragment
        .strip_prefix("file:///")
        .or_else(|| without_fragment.strip_prefix("file://"))
        .unwrap_or(without_fragment);
    without_scheme
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string()
}

fn local_count_native_io_certificate_human_text(certificate: &NativeIoCertificate) -> String {
    format!(
        "Vortex local encoded CountAll Native I/O certificate\ncertificate: {}\npath: {}\nstatus: {}\nsource adapter: {}\npushdown guarantee: {}\nrepresentation transitions: {}\nmaterialization boundaries: {}\nfallback execution allowed: {}",
        certificate.certificate_id,
        certificate.path_id,
        certificate.status(),
        certificate.source_capability_report.adapter_id,
        certificate.source_pushdown_report.guarantee,
        certificate.representation_transition_order(),
        certificate.materialization_boundary_order(),
        certificate.side_effects.fallback_execution_allowed
    )
}

fn local_primitive_native_io_certificate_human_text(certificate: &NativeIoCertificate) -> String {
    format!(
        "Vortex local primitive Native I/O certificate\ncertificate: {}\npath: {}\nstatus: {}\nsource adapter: {}\npushdown guarantee: {}\naccepted operations: {}\nrepresentation transitions: {}\nmaterialization boundaries: {}\nfallback execution allowed: {}",
        certificate.certificate_id,
        certificate.path_id,
        certificate.status(),
        certificate.source_capability_report.adapter_id,
        certificate.source_pushdown_report.guarantee,
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        certificate.representation_transition_order(),
        certificate.materialization_boundary_order(),
        certificate.side_effects.fallback_execution_allowed
    )
}

pub(crate) fn vortex_count_local_encoded_fields(
    memory_gb: u64,
    max_parallelism: usize,
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
    streaming_report: &VortexStreamingBatchRuntimeReport,
    evidence: &VortexCountLocalEncodedEvidence,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_bool_field(&mut fields, "fallback_execution_allowed", false);
    push_field(&mut fields, "mode", "vortex_count");
    push_field(&mut fields, "primitive", "count_all");
    push_bool_field(&mut fields, "explicit_local_encoded_count_requested", true);
    push_field(&mut fields, "feature_gate", "vortex-encoded-read-spike");
    push_bool_field(
        &mut fields,
        "feature_enabled",
        vortex_encoded_read_spike_feature_enabled(),
    );
    push_bool_field(
        &mut fields,
        "encoded_read_attempted",
        encoded_report.upstream_scan_called,
    );
    push_bool_field(&mut fields, "data_read", encoded_report.data_read);
    push_bool_field(&mut fields, "data_decoded", encoded_report.data_decoded);
    push_bool_field(
        &mut fields,
        "data_materialized",
        encoded_report.data_materialized,
    );
    push_bool_field(
        &mut fields,
        "object_store_io",
        encoded_report.object_store_io,
    );
    push_bool_field(&mut fields, "write_io", encoded_report.write_io);
    push_bool_field(
        &mut fields,
        "spill_io_performed",
        encoded_report.spill_io_performed,
    );
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        encoded_report.external_effects_executed,
    );
    push_field(&mut fields, "execution", encoded_report.status.as_str());
    fields.push(("memory_gb".to_string(), memory_gb.to_string()));
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_count_field(
        &mut fields,
        "arrays_read_count",
        encoded_report.arrays_read_count,
    );
    fields.push((
        "rows_counted".to_string(),
        encoded_report.rows_counted.to_string(),
    ));
    fields.push((
        "result_known".to_string(),
        encoded_report.count_result.is_some().to_string(),
    ));
    fields.push((
        "count".to_string(),
        encoded_report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    fields.push((
        "local_scan_target_uri".to_string(),
        encoded_report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    fields.push((
        "local_scan_readiness_source_uri".to_string(),
        encoded_report
            .local_scan_readiness_source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    push_bool_field(
        &mut fields,
        "local_scan_source_uri_matches_target",
        encoded_report.local_scan_source_uri_matches_target,
    );
    crate::prepared_source_backed_execution::append_vortex_encoded_read_spike_local_execution_fields(&mut fields, local_report);
    append_vortex_streaming_batch_runtime_fields(&mut fields, streaming_report);
    append_vortex_count_local_encoded_evidence_fields(&mut fields, evidence);
    fields
}

fn append_vortex_streaming_batch_runtime_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    append_vortex_streaming_batch_runtime_contract_fields(fields, report);
    append_vortex_streaming_batch_runtime_source_fields(fields, report);
    append_vortex_streaming_batch_runtime_execution_fields(fields, report);
}

fn append_vortex_streaming_batch_runtime_contract_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_bool_field(fields, "streaming_batch_runtime_report_emitted", true);
    push_field(
        fields,
        "streaming_batch_runtime_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "streaming_batch_runtime_status",
        report.status.as_str(),
    );
    push_field(fields, "streaming_batch_runtime_mode", report.mode.as_str());
    push_field(
        fields,
        "streaming_batch_runtime_representation",
        report.representation.as_str(),
    );
    push_field(
        fields,
        "streaming_batch_runtime_zero_decode",
        report.zero_decode.as_str(),
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_encoded_representation_preserved",
        report.encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_bounded_parallelism",
        report.bounded_parallelism,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_bounded_memory",
        report.bounded_memory,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_backpressure_bounded",
        report.backpressure_bounded,
    );
}

fn append_vortex_streaming_batch_runtime_source_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_field(
        fields,
        "streaming_batch_runtime_source_uri",
        &report
            .source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    );
    push_field(
        fields,
        "streaming_batch_runtime_local_scan_target_uri",
        &report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_source_uri_matches_local_scan",
        report.source_uri_matches_local_scan,
    );
}

fn append_vortex_streaming_batch_runtime_execution_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_count_field(
        fields,
        "streaming_batch_runtime_batches_executed",
        report.batches_executed,
    );
    fields.push((
        "streaming_batch_runtime_rows_processed".to_string(),
        report.rows_processed.to_string(),
    ));
    fields.push((
        "streaming_batch_runtime_count_result".to_string(),
        report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    push_bool_field(
        fields,
        "streaming_batch_runtime_streams_executed",
        report.streams_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_tasks_executed",
        report.tasks_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_materialized",
        report.data_materialized,
    );
    push_bool_field(fields, "streaming_batch_runtime_row_read", report.row_read);
    push_bool_field(
        fields,
        "streaming_batch_runtime_arrow_converted",
        report.arrow_converted,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_object_store_io",
        report.object_store_io,
    );
    push_bool_field(fields, "streaming_batch_runtime_write_io", report.write_io);
    push_bool_field(
        fields,
        "streaming_batch_runtime_spill_io_performed",
        report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_external_effects_executed",
        report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

pub(crate) fn append_vortex_count_local_encoded_evidence_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    append_vortex_count_local_encoded_target_policy_fields(fields, evidence);
    append_vortex_count_local_native_io_certificate_fields(fields, evidence);
    push_bool_field(
        fields,
        "correctness_fixture_matched",
        evidence.fixture_id.is_some(),
    );
    push_field(
        fields,
        "correctness_fixture_id",
        evidence.fixture_id.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "correctness_fixture_source_ref",
        evidence.fixture_source_ref.as_deref().unwrap_or("none"),
    );
    if let Some(certificate) = &evidence.certificate {
        append_execution_certificate_fields(fields, certificate);
    } else {
        push_bool_field(fields, "execution_certificate_emitted", false);
        push_field(
            fields,
            "execution_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "execution_certificate_correctness_passed", false);
        push_bool_field(
            fields,
            "execution_certificate_unsafe_effect_detected",
            false,
        );
        push_bool_field(fields, "execution_certificate_fallback_attempted", false);
        push_bool_field(
            fields,
            "execution_certificate_fallback_execution_allowed",
            false,
        );
    }
    if let Some(physical_kernel) = &evidence.physical_kernel {
        append_encoded_count_physical_kernel_fields(fields, physical_kernel);
    } else {
        push_bool_field(fields, "encoded_count_physical_kernel_emitted", false);
        push_field(
            fields,
            "encoded_count_physical_kernel_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "encoded_count_physical_kernel_safe_evidence", false);
        push_bool_field(
            fields,
            "encoded_count_physical_kernel_production_claim_allowed",
            false,
        );
    }
    if let Some(kernel_admission) = &evidence.kernel_admission {
        append_encoded_count_kernel_admission_fields(fields, kernel_admission);
    } else {
        push_bool_field(fields, "encoded_count_kernel_admission_emitted", false);
        push_field(
            fields,
            "encoded_count_kernel_admission_status",
            "evidence_unavailable",
        );
        push_bool_field(
            fields,
            "encoded_count_kernel_admission_slot_marked_present",
            false,
        );
        push_bool_field(
            fields,
            "encoded_count_kernel_admission_production_claim_allowed",
            false,
        );
    }
}

fn append_vortex_count_local_native_io_certificate_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    let Some(certificate) = &evidence.native_io_certificate else {
        push_bool_field(fields, "local_count_native_io_certificate_emitted", false);
        push_field(
            fields,
            "local_count_native_io_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "local_count_native_io_certified", false);
        return;
    };
    append_vortex_count_local_native_io_identity_fields(fields, certificate);
    append_vortex_count_local_native_io_source_fields(fields, certificate);
    append_vortex_count_local_native_io_pushdown_fields(fields, certificate);
    append_vortex_count_local_native_io_sink_fields(fields, certificate);
    append_vortex_count_local_native_io_side_effect_fields(fields, certificate);
}

fn append_vortex_count_local_native_io_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_bool_field(fields, "local_count_native_io_certificate_emitted", true);
    push_field(
        fields,
        "local_count_native_io_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_path_id",
        &certificate.path_id,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_status",
        certificate.status(),
    );
    push_bool_field(
        fields,
        "local_count_native_io_certified",
        certificate.is_certified(),
    );
}

fn append_vortex_count_local_native_io_source_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let source = &certificate.source_capability_report;
    push_field(
        fields,
        "local_count_native_io_source_kind",
        &source.source_kind,
    );
    push_field(
        fields,
        "local_count_native_io_adapter_id",
        &source.adapter_id,
    );
    push_bool_field(
        fields,
        "local_count_native_io_encoded_representation_preserved",
        source.encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "local_count_native_io_streaming_capability",
        source.streaming_capability,
    );
}

fn append_vortex_count_local_native_io_pushdown_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let pushdown = &certificate.source_pushdown_report;
    push_field(
        fields,
        "local_count_native_io_pushdown_accepted_operations",
        &pushdown.accepted_operation_order(),
    );
    push_field(
        fields,
        "local_count_native_io_pushdown_rejected_operations",
        &pushdown.rejected_operation_order(),
    );
    push_field(
        fields,
        "local_count_native_io_pushdown_guarantee",
        &pushdown.guarantee,
    );
    push_field(
        fields,
        "local_count_native_io_representation_transitions",
        &certificate.representation_transition_order(),
    );
    push_field(
        fields,
        "local_count_native_io_materialization_boundaries",
        &certificate.materialization_boundary_order(),
    );
    push_bool_field(
        fields,
        "local_count_native_io_materializing_transitions_have_boundaries",
        certificate.materializing_transitions_have_boundaries(),
    );
}

fn append_vortex_count_local_native_io_sink_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let sink = &certificate.sink_requirement_report;
    let fidelity = &certificate.adapter_fidelity_report;
    push_field(
        fields,
        "local_count_native_io_sink_target_format",
        &sink.target_format,
    );
    push_bool_field(
        fields,
        "local_count_native_io_sink_accepts_encoded",
        sink.accepts_encoded,
    );
    push_bool_field(
        fields,
        "local_count_native_io_adapter_materialization_required",
        fidelity.materialization_required,
    );
}

fn append_vortex_count_local_native_io_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let side_effects = &certificate.side_effects;
    push_bool_field(
        fields,
        "local_count_native_io_data_read",
        side_effects.data_read,
    );
    push_bool_field(
        fields,
        "local_count_native_io_data_decoded",
        side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "local_count_native_io_data_materialized",
        side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "local_count_native_io_row_read",
        side_effects.row_read,
    );
    push_bool_field(
        fields,
        "local_count_native_io_arrow_converted",
        side_effects.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_count_native_io_object_store_io",
        side_effects.object_store_io,
    );
    push_bool_field(
        fields,
        "local_count_native_io_write_io",
        side_effects.write_io,
    );
    push_bool_field(
        fields,
        "local_count_native_io_spill_io_performed",
        side_effects.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_count_native_io_fallback_attempted",
        side_effects.fallback_attempted || certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_count_native_io_fallback_execution_allowed",
        side_effects.fallback_execution_allowed,
    );
}

pub(crate) fn append_vortex_local_primitive_native_io_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: Option<&NativeIoCertificate>,
) {
    let Some(certificate) = certificate else {
        push_bool_field(
            fields,
            "local_primitive_native_io_certificate_emitted",
            false,
        );
        push_field(
            fields,
            "local_primitive_native_io_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "local_primitive_native_io_certified", false);
        return;
    };

    append_vortex_local_primitive_native_io_identity_fields(fields, certificate);
    append_vortex_local_primitive_native_io_source_fields(fields, certificate);
    append_vortex_local_primitive_native_io_pushdown_fields(fields, certificate);
    append_vortex_local_primitive_native_io_sink_fields(fields, certificate);
    append_vortex_local_primitive_native_io_side_effect_fields(fields, certificate);
}

fn append_vortex_local_primitive_native_io_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_native_io_certificate_emitted",
        true,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_path_id",
        &certificate.path_id,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_status",
        certificate.status(),
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_certified",
        certificate.is_certified(),
    );
}

fn append_vortex_local_primitive_native_io_source_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_source_kind",
        &certificate.source_capability_report.source_kind,
    );
    push_field(
        fields,
        "local_primitive_native_io_adapter_id",
        &certificate.source_capability_report.adapter_id,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_encoded_representation_preserved",
        certificate
            .source_capability_report
            .encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_streaming_capability",
        certificate.source_capability_report.streaming_capability,
    );
}

fn append_vortex_local_primitive_native_io_pushdown_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_pushdown_accepted_operations",
        &certificate
            .source_pushdown_report
            .accepted_operation_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_pushdown_rejected_operations",
        &certificate
            .source_pushdown_report
            .rejected_operation_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_pushdown_guarantee",
        &certificate.source_pushdown_report.guarantee,
    );
    push_field(
        fields,
        "local_primitive_native_io_representation_transitions",
        &certificate.representation_transition_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_materialization_boundaries",
        &certificate.materialization_boundary_order(),
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_materializing_transitions_have_boundaries",
        certificate.materializing_transitions_have_boundaries(),
    );
}

fn append_vortex_local_primitive_native_io_sink_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_sink_target_format",
        &certificate.sink_requirement_report.target_format,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_accepts_encoded",
        certificate.sink_requirement_report.accepts_encoded,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_requires_decoded_columnar",
        certificate
            .sink_requirement_report
            .requires_decoded_columnar,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_requires_rows",
        certificate.sink_requirement_report.requires_rows,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_supports_streaming",
        certificate.sink_requirement_report.supports_streaming,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_adapter_materialization_required",
        certificate.adapter_fidelity_report.materialization_required,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_adapter_encoded_representation_preserved",
        certificate
            .adapter_fidelity_report
            .encoded_representation_preserved,
    );
}

fn append_vortex_local_primitive_native_io_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let side_effects = &certificate.side_effects;
    push_bool_field(
        fields,
        "local_primitive_native_io_data_read",
        side_effects.data_read,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_data_decoded",
        side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_data_materialized",
        side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_row_read",
        side_effects.row_read,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_arrow_converted",
        side_effects.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_object_store_io",
        side_effects.object_store_io,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_write_io",
        side_effects.write_io,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_spill_io_performed",
        side_effects.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_fallback_attempted",
        side_effects.fallback_attempted || certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_fallback_execution_allowed",
        side_effects.fallback_execution_allowed,
    );
}

pub(crate) fn append_vortex_local_primitive_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: Option<&ExecutionCertificate>,
) {
    let Some(certificate) = certificate else {
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_emitted",
            false,
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_schema_version",
            "none",
        );
        push_field(fields, "local_primitive_execution_certificate_id", "none");
        push_field(
            fields,
            "local_primitive_execution_certificate_execution_kind",
            "none",
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_status",
            "not_available",
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_fixture_id",
            "none",
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_correctness_passed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_read",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_decoded",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_materialized",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_row_read",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_arrow_converted",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_object_store_io",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_write_io",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_spill_io_performed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_external_effects_executed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_unsafe_effect_detected",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_fallback_attempted",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_fallback_execution_allowed",
            false,
        );
        return;
    };

    append_vortex_local_primitive_execution_certificate_identity_fields(fields, certificate);
    append_vortex_local_primitive_execution_certificate_effect_fields(fields, certificate);
}

fn append_vortex_local_primitive_execution_certificate_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_emitted",
        true,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_execution_kind",
        &certificate.execution_kind,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_kind",
        certificate.execution_provider_kind.as_str(),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_scope",
        &certificate.provider_scope,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_crate",
        certificate.provider_crate.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_version",
        certificate.provider_version.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_api_surface",
        certificate
            .provider_api_surface
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_shardloom_admission_policy",
        certificate
            .shardloom_admission_policy
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_fixture_id",
        certificate
            .correctness_fixture_id
            .as_deref()
            .unwrap_or("none"),
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_correctness_passed",
        certificate.correctness_passed,
    );
}

fn append_vortex_local_primitive_execution_certificate_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_read",
        certificate.data_read,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_decoded",
        certificate.data_decoded,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_materialized",
        certificate.data_materialized,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_row_read",
        certificate.row_read,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_arrow_converted",
        certificate.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_object_store_io",
        certificate.object_store_io,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_write_io",
        certificate.write_io,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_spill_io_performed",
        certificate.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_external_query_engine_invoked",
        certificate.external_query_engine_invoked,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_external_effects_executed",
        certificate.external_effects_executed,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_unsafe_effect_detected",
        certificate.unsafe_effect_detected,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_fallback_execution_allowed",
        certificate.fallback_execution_allowed,
    );
}

fn append_vortex_count_local_encoded_target_policy_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    push_bool_field(
        fields,
        "generalized_local_count_target_policy_report_emitted",
        true,
    );
    push_field(
        fields,
        "generalized_local_count_target_policy",
        evidence.target_policy.as_str(),
    );
    push_field(
        fields,
        "generalized_local_count_target_policy_reason",
        evidence.target_policy.reason(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_execution_allowed",
        evidence.target_policy.execution_allowed(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_non_fixture_target",
        evidence.target_policy.non_fixture_target(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_correctness_certified",
        evidence
            .certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    push_bool_field(
        fields,
        "generalized_local_count_requires_correctness_fixture",
        !evidence
            .certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    push_bool_field(
        fields,
        "generalized_local_count_requires_benchmark_evidence",
        true,
    );
    push_bool_field(
        fields,
        "generalized_local_count_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "generalized_local_count_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "generalized_local_count_cg13_closeout_allowed",
        false,
    );
}

fn append_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(fields, "execution_certificate_emitted", true);
    append_execution_certificate_identity_fields(fields, certificate);
    append_execution_certificate_provider_fields(fields, certificate);
    append_execution_certificate_io_fields(fields, certificate);
    append_execution_certificate_effect_fields(fields, certificate);
}

fn append_execution_certificate_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_field(
        fields,
        "execution_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "execution_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "execution_certificate_execution_kind",
        &certificate.execution_kind,
    );
    push_field(
        fields,
        "execution_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "execution_certificate_input_ref",
        certificate.input_ref.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_output_ref",
        certificate.output_ref.as_deref().unwrap_or("none"),
    );
    push_bool_field(
        fields,
        "execution_certificate_correctness_passed",
        certificate.correctness_passed,
    );
}

fn append_execution_certificate_provider_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_field(
        fields,
        "execution_certificate_provider_kind",
        certificate.execution_provider_kind.as_str(),
    );
    push_field(
        fields,
        "execution_certificate_provider_scope",
        &certificate.provider_scope,
    );
    push_field(
        fields,
        "execution_certificate_provider_crate",
        certificate.provider_crate.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_provider_version",
        certificate.provider_version.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_provider_api_surface",
        certificate
            .provider_api_surface
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_shardloom_admission_policy",
        certificate
            .shardloom_admission_policy
            .as_deref()
            .unwrap_or("none"),
    );
}

fn append_execution_certificate_io_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "execution_certificate_data_read",
        certificate.data_read,
    );
    push_bool_field(
        fields,
        "execution_certificate_data_decoded",
        certificate.data_decoded,
    );
    push_bool_field(
        fields,
        "execution_certificate_data_materialized",
        certificate.data_materialized,
    );
    push_bool_field(
        fields,
        "execution_certificate_row_read",
        certificate.row_read,
    );
    push_bool_field(
        fields,
        "execution_certificate_arrow_converted",
        certificate.arrow_converted,
    );
    push_bool_field(
        fields,
        "execution_certificate_object_store_io",
        certificate.object_store_io,
    );
    push_bool_field(
        fields,
        "execution_certificate_write_io",
        certificate.write_io,
    );
    push_bool_field(
        fields,
        "execution_certificate_spill_io_performed",
        certificate.spill_io_performed,
    );
}

fn append_execution_certificate_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "execution_certificate_external_query_engine_invoked",
        certificate.external_query_engine_invoked,
    );
    push_bool_field(
        fields,
        "execution_certificate_external_effects_executed",
        certificate.external_effects_executed,
    );
    push_bool_field(
        fields,
        "execution_certificate_unsafe_effect_detected",
        certificate.unsafe_effect_detected,
    );
    push_bool_field(
        fields,
        "execution_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "execution_certificate_fallback_execution_allowed",
        certificate.fallback_execution_allowed,
    );
}

fn append_encoded_count_physical_kernel_fields(
    fields: &mut Vec<(String, String)>,
    physical_kernel: &VortexEncodedCountPhysicalKernelReport,
) {
    push_bool_field(fields, "encoded_count_physical_kernel_emitted", true);
    push_field(
        fields,
        "encoded_count_physical_kernel_schema_version",
        physical_kernel.schema_version,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_id",
        &physical_kernel.kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_status",
        physical_kernel.status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_execution_certificate_id",
        physical_kernel
            .execution_certificate_id
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_count",
        &physical_kernel
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_safe_evidence",
        physical_kernel.is_safe_native_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_production_claim_allowed",
        physical_kernel.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_read",
        physical_kernel.data_read,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_decoded",
        physical_kernel.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_materialized",
        physical_kernel.data_materialized,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_row_read",
        physical_kernel.row_read,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_arrow_converted",
        physical_kernel.arrow_converted,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_object_store_io",
        physical_kernel.object_store_io,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_write_io",
        physical_kernel.write_io,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_spill_io_performed",
        physical_kernel.spill_io_performed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_external_effects_executed",
        physical_kernel.external_effects_executed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_attempted",
        physical_kernel.fallback_attempted,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_execution_allowed",
        physical_kernel.fallback_execution_allowed,
    );
}

fn append_encoded_count_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    kernel_admission: &VortexEncodedCountKernelAdmissionReport,
) {
    push_bool_field(fields, "encoded_count_kernel_admission_emitted", true);
    push_field(
        fields,
        "encoded_count_kernel_admission_schema_version",
        kernel_admission.schema_version,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_id",
        &kernel_admission.admission_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_physical_kernel_report_id",
        &kernel_admission.physical_kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_slot_id",
        &kernel_admission.slot_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_operator_kind",
        kernel_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_required_kernel_kind",
        kernel_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_candidate_kernel_kind",
        kernel_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_status",
        kernel_admission.status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_correctness_evidence",
        kernel_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_benchmark_evidence",
        kernel_admission.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_streaming",
        kernel_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_bounded",
        kernel_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_oom_safe",
        kernel_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_full_materialization",
        kernel_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_fallback_state",
        kernel_admission.fallback.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_slot_marked_present",
        kernel_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_production_claim_allowed",
        kernel_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_runtime_execution",
        kernel_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_fallback_execution_allowed",
        kernel_admission.fallback_execution_allowed,
    );
}

#[allow(clippy::too_many_lines)]
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
    let request = match parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
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
    append_vortex_work_avoided_fields(&mut fields, Some(&analysis.work_avoided));
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
            match vortex_count_where_local_execution_evidence(&request, local_request) {
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
    let evidence = match vortex_count_where_filter_evidence(&predicate, &summary) {
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
        VortexCountWhereLocalExecutionEvidence::has_errors,
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
        .and_then(VortexCountWhereLocalExecutionEvidence::count)
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
        vortex_count_where_human_text(&result, &evidence, local_execution.as_ref()),
        diagnostics,
        vortex_count_where_fields(
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
    let projection = match parse_projection_columns(&columns_arg) {
        Ok(projection) => projection,
        Err(error) => {
            return emit_error("vortex-project", format, "vortex project failed", &error);
        }
    };
    let local_execution_request = match parse_vortex_local_primitive_cli_execution_args(&mut args) {
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
            match vortex_local_primitive_cli_execution_evidence(&request, local_request) {
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
        VortexLocalPrimitiveCliExecutionEvidence::has_errors,
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
        vortex_project_human_text(&result, local_execution.as_ref()),
        diagnostics,
        vortex_project_fields(&result, columns_arg, local_execution.as_ref()),
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
    source_order_limit: Option<usize>,
    local_execution_request: Option<VortexLocalPrimitiveCliExecutionRequest>,
}

struct VortexFilterArgs {
    uri: DatasetUri,
    predicate_arg: String,
    predicate: PredicateExpr,
    local_execution_request: Option<VortexLocalPrimitiveCliExecutionRequest>,
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
        source_order_limit,
        local_execution_request,
    } = parsed;
    let mut request =
        VortexQueryPrimitiveRequest::filter_and_project(uri.clone(), predicate, projection);
    if let Some(limit) = source_order_limit {
        request = request.with_source_order_limit(limit);
    }
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
        VortexLocalPrimitiveCliExecutionEvidence::has_errors,
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
        vortex_filter_project_human_text(&result, local_execution.as_ref()),
        diagnostics,
        vortex_filter_project_fields(
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
        VortexLocalPrimitiveCliExecutionEvidence::has_errors,
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
        vortex_filter_human_text(&result, local_execution.as_ref()),
        diagnostics,
        vortex_filter_fields(&result, predicate_arg, local_execution.as_ref()),
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
    let request = match parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
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
        bounded_local_execution_fields(&report, &primitive_arg, memory_gb, max_parallelism),
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
    reconcile_vortex_local_engine_why_with_execution_certificate(
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
        local_primitive_correctness_fixture_for_request(query_request, local_report)
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
    append_vortex_work_avoided_fields(&mut fields, Some(context.runtime_work_avoided));
    append_vortex_local_primitive_native_io_certificate_fields(
        &mut fields,
        context.certificates.native_io.as_ref(),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        &mut fields,
        context.certificates.execution.as_ref(),
    );
    append_vortex_local_engine_why_fields(&mut fields, context.why_report);
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
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--limit <rows>] [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(predicate_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--limit <rows>] [--execute-local-primitive <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(columns_arg) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-filter-project <dataset_uri> <predicate> <columns> [--limit <rows>] [--execute-local-primitive <memory_gb> <max_parallelism>]"
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
    let predicate = parse_tiny_predicate(&predicate_arg).map_err(|error| {
        emit_error(
            "vortex-filter-project",
            format,
            "vortex filter project failed",
            &error,
        )
    })?;
    let projection = parse_projection_columns(&columns_arg).map_err(|error| {
        emit_error(
            "vortex-filter-project",
            format,
            "vortex filter project failed",
            &error,
        )
    })?;
    let options = parse_vortex_filter_project_options(&mut args).map_err(|error| {
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
        source_order_limit: options.source_order_limit,
        local_execution_request: options.local_execution_request,
    })
}

fn parse_vortex_filter_project_options(
    args: &mut impl Iterator<Item = String>,
) -> shardloom_core::Result<VortexFilterProjectCliOptions> {
    let mut local_execution_request = None;
    let mut source_order_limit = None;
    while let Some(option) = args.next() {
        match option.as_str() {
            "--execute-local-primitive" => {
                if local_execution_request.is_some() {
                    return Err(ShardLoomError::InvalidOperation(
                        "--execute-local-primitive was provided more than once".to_string(),
                    ));
                }
                let Some(memory_gb_text) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "missing memory_gb after --execute-local-primitive".to_string(),
                    ));
                };
                let Some(max_parallelism_text) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "missing max_parallelism after --execute-local-primitive".to_string(),
                    ));
                };
                let memory_gb = memory_gb_text.parse::<u64>().map_err(|_| {
                    ShardLoomError::InvalidOperation(
                        "memory_gb must be an unsigned integer".to_string(),
                    )
                })?;
                if memory_gb == 0 {
                    return Err(ShardLoomError::InvalidOperation(
                        "memory_gb must be >= 1".to_string(),
                    ));
                }
                let max_parallelism = max_parallelism_text.parse::<usize>().map_err(|_| {
                    ShardLoomError::InvalidOperation(
                        "max_parallelism must be an unsigned integer".to_string(),
                    )
                })?;
                VortexLocalPrimitiveExecutionPolicy::new(max_parallelism)?;
                local_execution_request = Some(VortexLocalPrimitiveCliExecutionRequest {
                    memory_gb,
                    max_parallelism,
                });
            }
            "--limit" | "--source-order-limit" => {
                if source_order_limit.is_some() {
                    return Err(ShardLoomError::InvalidOperation(
                        "source-order limit was provided more than once".to_string(),
                    ));
                }
                let Some(limit_text) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "missing row limit after {option}"
                    )));
                };
                let limit = limit_text.parse::<usize>().map_err(|_| {
                    ShardLoomError::InvalidOperation(
                        "source-order limit must be an unsigned integer".to_string(),
                    )
                })?;
                if limit == 0 {
                    return Err(ShardLoomError::InvalidOperation(
                        "source-order limit must be >= 1".to_string(),
                    ));
                }
                source_order_limit = Some(limit);
            }
            other => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown option: {other}"
                )));
            }
        }
    }
    Ok(VortexFilterProjectCliOptions {
        local_execution_request,
        source_order_limit,
    })
}

fn vortex_filter_project_local_execution(
    request: &VortexQueryPrimitiveRequest,
    local_execution_request: Option<&VortexLocalPrimitiveCliExecutionRequest>,
    format: OutputFormat,
) -> std::result::Result<Option<VortexLocalPrimitiveCliExecutionEvidence>, ExitCode> {
    let Some(local_request) = local_execution_request else {
        return Ok(None);
    };
    vortex_local_primitive_cli_execution_evidence(request, local_request)
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
    let predicate = parse_tiny_predicate(&predicate_arg)
        .map_err(|error| emit_error("vortex-filter", format, "vortex filter failed", &error))?;
    let local_execution_request = parse_vortex_local_primitive_cli_execution_args(&mut args)
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
    let request = parse_vortex_primitive_request(uri.clone(), &primitive_arg).map_err(|error| {
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
    local_execution_request: Option<&VortexLocalPrimitiveCliExecutionRequest>,
    format: OutputFormat,
) -> std::result::Result<Option<VortexLocalPrimitiveCliExecutionEvidence>, ExitCode> {
    let Some(local_request) = local_execution_request else {
        return Ok(None);
    };
    vortex_local_primitive_cli_execution_evidence(request, local_request)
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
        Option<VortexCountWhereLocalExecutionRequest>,
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
            return Err(emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            ));
        }
    };
    let predicate = match parse_tiny_predicate(&predicate_arg) {
        Ok(predicate) => predicate,
        Err(error) => {
            return Err(emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            ));
        }
    };
    let local_execution_request = match parse_vortex_count_where_local_execution_args(args) {
        Ok(request) => request,
        Err(error) => {
            return Err(emit_error(
                "vortex-count-where",
                format,
                "vortex count where failed",
                &error,
            ));
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
    let (encoded_report, local_report) =
        match run_vortex_approved_local_encoded_count(uri.clone(), memory_gb, max_parallelism) {
            Ok(reports) => reports,
            Err(error) => {
                return emit_error("vortex-count", format, "vortex count failed", &error);
            }
        };
    let streaming_plan =
        match build_vortex_count_local_streaming_batch_plan(uri, memory_gb, max_parallelism) {
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
    let evidence = match vortex_count_local_encoded_evidence(&encoded_report, &local_report) {
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
    let fields = vortex_count_local_encoded_fields(
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
