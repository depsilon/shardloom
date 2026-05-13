//! Provider-side bridge from top-level `ShardLoom` plans to Vortex-native reports.

use shardloom_core::{
    ColumnRef, ComparisonOp, Diagnostic, DiagnosticCode, ExecutionProviderKind, Result, StatValue,
};
use shardloom_exec::{
    ShardLoomExecutionProvider, ShardLoomExecutionResult, ShardLoomExecutionStatus,
};
use shardloom_plan::{
    EncodedExecutionOperation, Plan, PlanKind, PreparedEncodedBatch, PreparedEncodedPlan,
    ProjectionRequest, ReaderBackedEncodedPlan, SourceBackedEncodedPlan,
    SourceBackedPreparedEncodedBatch, VortexPrimitiveOperation, VortexPrimitivePlan,
};

use crate::{
    VortexEncodedValuePredicateBatch, VortexGeneralizedEncodedFilterExecutionReport,
    VortexGeneralizedEncodedProjectionExecutionReport, VortexLocalEnginePrimitive,
    VortexLocalEngineReport, VortexLocalEngineRequest, VortexNativeProviderBoundary,
    VortexPreparedEncodedProjectionColumn, VortexReaderBackedEncodedFilterExecutionReport,
    VortexReaderBackedEncodedProjectionExecutionReport, VortexReaderBackedSplitEvidence,
    VortexSourceBackedEncodedFilterExecutionReport, VortexSourceBackedEncodedProjectionColumn,
    VortexSourceBackedEncodedProjectionExecutionReport,
    VortexSourceBackedEncodedValuePredicateBatch,
    execute_vortex_generalized_filter_from_encoded_value_batches,
    execute_vortex_generalized_projection_from_encoded_projection_batches,
    execute_vortex_reader_backed_filter_from_encoded_value_batches,
    execute_vortex_reader_backed_projection_from_encoded_projection_batches,
    execute_vortex_source_backed_filter_from_encoded_value_batches,
    execute_vortex_source_backed_projection_from_encoded_projection_batches,
    run_vortex_local_engine,
};

/// Vortex-native top-level execution provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexTopLevelExecutionProvider {
    pub memory_gb: u64,
    pub max_parallelism: usize,
}

impl Default for VortexTopLevelExecutionProvider {
    fn default() -> Self {
        Self {
            memory_gb: 1,
            max_parallelism: 1,
        }
    }
}

impl VortexTopLevelExecutionProvider {
    #[must_use]
    pub const fn new(memory_gb: u64, max_parallelism: usize) -> Self {
        Self {
            memory_gb,
            max_parallelism,
        }
    }

    fn execute_vortex_primitive(
        &self,
        plan: &Plan,
        primitive: &VortexPrimitivePlan,
    ) -> Result<ShardLoomExecutionResult> {
        let local_primitive = match plan_vortex_primitive_to_local_engine(primitive) {
            Ok(local_primitive) => local_primitive,
            Err(diagnostic) => {
                return Ok(ShardLoomExecutionResult::blocked_unsupported(
                    plan,
                    *diagnostic,
                ));
            }
        };
        let request = VortexLocalEngineRequest::new(
            primitive.source_uri.clone(),
            local_primitive,
            self.memory_gb,
            self.max_parallelism,
        )?;
        let report = run_vortex_local_engine(request)?;
        Ok(result_from_local_engine_report(plan, &report))
    }

    fn execute_prepared_encoded(
        plan: &Plan,
        prepared: &PreparedEncodedPlan,
    ) -> Result<ShardLoomExecutionResult> {
        match prepared.operation {
            EncodedExecutionOperation::Filter => {
                let Some(predicate) = prepared.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "prepared_encoded_filter_predicate",
                    ));
                };
                let batches = prepared_filter_batches(&prepared.filter_batches);
                let report = execute_vortex_generalized_filter_from_encoded_value_batches(
                    predicate, &batches,
                )?;
                Ok(result_from_prepared_filter_report(plan, &report))
            }
            EncodedExecutionOperation::Projection => {
                let batches = prepared_projection_batches(&prepared.projection_batches);
                let report = execute_vortex_generalized_projection_from_encoded_projection_batches(
                    &prepared.requested_columns,
                    &batches,
                    None,
                )?;
                Ok(result_from_prepared_projection_report(plan, &report))
            }
            EncodedExecutionOperation::FilterAndProject => {
                let Some(predicate) = prepared.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "prepared_encoded_filter_project_predicate",
                    ));
                };
                let filter_batches = prepared_filter_batches(&prepared.filter_batches);
                let filter_report = execute_vortex_generalized_filter_from_encoded_value_batches(
                    predicate,
                    &filter_batches,
                )?;
                let projection_batches = prepared_projection_batches(&prepared.projection_batches);
                let projection_report =
                    execute_vortex_generalized_projection_from_encoded_projection_batches(
                        &prepared.requested_columns,
                        &projection_batches,
                        Some(&filter_report.filter_kernel),
                    )?;
                Ok(result_from_prepared_filter_project_reports(
                    plan,
                    &filter_report,
                    &projection_report,
                ))
            }
        }
    }

    fn execute_source_backed(
        plan: &Plan,
        source_backed: &SourceBackedEncodedPlan,
    ) -> Result<ShardLoomExecutionResult> {
        match source_backed.operation {
            EncodedExecutionOperation::Filter => {
                let Some(predicate) = source_backed.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "source_backed_filter_predicate",
                    ));
                };
                let batches = source_backed_filter_batches(&source_backed.filter_batches)?;
                let report = execute_vortex_source_backed_filter_from_encoded_value_batches(
                    predicate,
                    &source_backed.source,
                    &batches,
                )?;
                Ok(result_from_source_filter_report(plan, &report))
            }
            EncodedExecutionOperation::Projection => {
                let batches = source_backed_projection_batches(&source_backed.projection_batches)?;
                let report =
                    execute_vortex_source_backed_projection_from_encoded_projection_batches(
                        &source_backed.requested_columns,
                        &source_backed.source,
                        &batches,
                        None,
                    )?;
                Ok(result_from_source_projection_report(plan, &report))
            }
            EncodedExecutionOperation::FilterAndProject => {
                let Some(predicate) = source_backed.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "source_backed_filter_project_predicate",
                    ));
                };
                let filter_batches = source_backed_filter_batches(&source_backed.filter_batches)?;
                let filter_report = execute_vortex_source_backed_filter_from_encoded_value_batches(
                    predicate,
                    &source_backed.source,
                    &filter_batches,
                )?;
                let projection_batches =
                    source_backed_projection_batches(&source_backed.projection_batches)?;
                let projection_report =
                    execute_vortex_source_backed_projection_from_encoded_projection_batches(
                        &source_backed.requested_columns,
                        &source_backed.source,
                        &projection_batches,
                        Some(&filter_report.prepared_execution.filter_kernel),
                    )?;
                Ok(result_from_source_filter_project_reports(
                    plan,
                    &filter_report,
                    &projection_report,
                ))
            }
        }
    }

    fn execute_reader_backed(
        plan: &Plan,
        reader_backed: &ReaderBackedEncodedPlan,
    ) -> Result<ShardLoomExecutionResult> {
        let reader_splits = reader_backed_splits(reader_backed)?;
        match reader_backed.operation {
            EncodedExecutionOperation::Filter => {
                let Some(predicate) = reader_backed.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "reader_backed_filter_predicate",
                    ));
                };
                let batches = source_backed_filter_batches(&reader_backed.filter_batches)?;
                let report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
                    predicate,
                    &reader_backed.source,
                    &reader_splits,
                    &batches,
                )?;
                Ok(result_from_reader_filter_report(plan, &report))
            }
            EncodedExecutionOperation::Projection => {
                let batches = source_backed_projection_batches(&reader_backed.projection_batches)?;
                let report =
                    execute_vortex_reader_backed_projection_from_encoded_projection_batches(
                        &reader_backed.requested_columns,
                        &reader_backed.source,
                        &reader_splits,
                        &batches,
                        None,
                    )?;
                Ok(result_from_reader_projection_report(plan, &report))
            }
            EncodedExecutionOperation::FilterAndProject => {
                let Some(predicate) = reader_backed.predicate.as_ref() else {
                    return Ok(blocked_missing_payload(
                        plan,
                        "reader_backed_filter_project_predicate",
                    ));
                };
                let filter_batches = source_backed_filter_batches(&reader_backed.filter_batches)?;
                let filter_report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
                    predicate,
                    &reader_backed.source,
                    &reader_splits,
                    &filter_batches,
                )?;
                let projection_batches =
                    source_backed_projection_batches(&reader_backed.projection_batches)?;
                let projection_report =
                    execute_vortex_reader_backed_projection_from_encoded_projection_batches(
                        &reader_backed.requested_columns,
                        &reader_backed.source,
                        &reader_splits,
                        &projection_batches,
                        Some(
                            &filter_report
                                .source_execution
                                .prepared_execution
                                .filter_kernel,
                        ),
                    )?;
                Ok(result_from_reader_filter_project_reports(
                    plan,
                    &filter_report,
                    &projection_report,
                ))
            }
        }
    }
}

impl ShardLoomExecutionProvider for VortexTopLevelExecutionProvider {
    fn execute_plan(&self, plan: &Plan) -> Result<ShardLoomExecutionResult> {
        match &plan.kind {
            PlanKind::VortexPrimitive(primitive) => self.execute_vortex_primitive(plan, primitive),
            PlanKind::PreparedEncoded(prepared) => Self::execute_prepared_encoded(plan, prepared),
            PlanKind::SourceBackedEncoded(source_backed) => {
                Self::execute_source_backed(plan, source_backed)
            }
            PlanKind::ReaderBackedEncoded(reader_backed) => {
                Self::execute_reader_backed(plan, reader_backed)
            }
            PlanKind::ReportOnly(_) => Ok(ShardLoomExecutionResult::report_only(plan)),
        }
    }
}

fn plan_vortex_primitive_to_local_engine(
    primitive: &VortexPrimitivePlan,
) -> std::result::Result<VortexLocalEnginePrimitive, Box<Diagnostic>> {
    match primitive.operation {
        VortexPrimitiveOperation::CountAll => Ok(VortexLocalEnginePrimitive::Count),
        VortexPrimitiveOperation::CountWhere => {
            let Some(predicate) = primitive.predicate.as_ref() else {
                return Err(unsupported_bridge_diagnostic(
                    "vortex_count_where_missing_predicate",
                    "CountWhere requires a predicate.",
                ));
            };
            tiny_predicate(predicate).map(VortexLocalEnginePrimitive::CountWhere)
        }
        VortexPrimitiveOperation::FilterPredicate => {
            let Some(predicate) = primitive.predicate.as_ref() else {
                return Err(unsupported_bridge_diagnostic(
                    "vortex_filter_missing_predicate",
                    "FilterPredicate requires a predicate.",
                ));
            };
            tiny_predicate(predicate).map(VortexLocalEnginePrimitive::Filter)
        }
        VortexPrimitiveOperation::ProjectColumns => Ok(VortexLocalEnginePrimitive::Project(
            projection_string(&primitive.projection),
        )),
        VortexPrimitiveOperation::FilterAndProject => {
            let Some(predicate) = primitive.predicate.as_ref() else {
                return Err(unsupported_bridge_diagnostic(
                    "vortex_filter_project_missing_predicate",
                    "FilterAndProject requires a predicate.",
                ));
            };
            Ok(VortexLocalEnginePrimitive::FilterAndProject {
                predicate: tiny_predicate(predicate)?,
                columns: projection_string(&primitive.projection),
            })
        }
    }
}

fn tiny_predicate(
    predicate: &shardloom_core::PredicateExpr,
) -> std::result::Result<String, Box<Diagnostic>> {
    match predicate {
        shardloom_core::PredicateExpr::IsNull { column } => {
            Ok(format!("is_null:{}", column.as_str()))
        }
        shardloom_core::PredicateExpr::IsNotNull { column } => {
            Ok(format!("is_not_null:{}", column.as_str()))
        }
        shardloom_core::PredicateExpr::Compare {
            column,
            op,
            value: StatValue::Int64(value),
        } => {
            let op = match op {
                ComparisonOp::Eq => "eq",
                ComparisonOp::Gt => "gt",
                ComparisonOp::GtEq => "gte",
                ComparisonOp::Lt => "lt",
                ComparisonOp::LtEq => "lte",
                ComparisonOp::NotEq => {
                    return Err(unsupported_bridge_diagnostic(
                        "vortex_local_primitive_predicate",
                        "local primitive bridge does not support not-equal predicates yet",
                    ));
                }
            };
            Ok(format!("{op}:{}:{value}", column.as_str()))
        }
        _ => Err(unsupported_bridge_diagnostic(
            "vortex_local_primitive_predicate",
            "local primitive bridge currently supports null checks and i64 comparisons only",
        )),
    }
}

fn projection_string(projection: &ProjectionRequest) -> String {
    match projection {
        ProjectionRequest::All => "*".to_string(),
        ProjectionRequest::Columns(columns) => columns
            .iter()
            .map(ColumnRef::as_str)
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn prepared_filter_batches(
    batches: &[PreparedEncodedBatch],
) -> Vec<VortexEncodedValuePredicateBatch> {
    batches
        .iter()
        .map(|batch| {
            VortexEncodedValuePredicateBatch::new(batch.segment.clone(), batch.values.clone())
        })
        .collect()
}

fn prepared_projection_batches(
    batches: &[PreparedEncodedBatch],
) -> Vec<VortexPreparedEncodedProjectionColumn> {
    batches
        .iter()
        .map(|batch| {
            VortexPreparedEncodedProjectionColumn::new(batch.segment.clone(), batch.values.clone())
        })
        .collect()
}

fn source_backed_filter_batches(
    batches: &[SourceBackedPreparedEncodedBatch],
) -> Result<Vec<VortexSourceBackedEncodedValuePredicateBatch>> {
    batches
        .iter()
        .map(|batch| {
            VortexSourceBackedEncodedValuePredicateBatch::new(
                batch.source_uri.clone(),
                batch.split_ref.clone(),
                VortexEncodedValuePredicateBatch::new(
                    batch.batch.segment.clone(),
                    batch.batch.values.clone(),
                ),
            )
        })
        .collect()
}

fn source_backed_projection_batches(
    batches: &[SourceBackedPreparedEncodedBatch],
) -> Result<Vec<VortexSourceBackedEncodedProjectionColumn>> {
    batches
        .iter()
        .map(|batch| {
            VortexSourceBackedEncodedProjectionColumn::new(
                batch.source_uri.clone(),
                batch.split_ref.clone(),
                VortexPreparedEncodedProjectionColumn::new(
                    batch.batch.segment.clone(),
                    batch.batch.values.clone(),
                ),
            )
        })
        .collect()
}

fn reader_backed_splits(
    plan: &ReaderBackedEncodedPlan,
) -> Result<Vec<VortexReaderBackedSplitEvidence>> {
    plan.reader_splits
        .iter()
        .map(|split| {
            let mut evidence = VortexReaderBackedSplitEvidence::new(
                split.source_uri.clone(),
                split.split_ref.clone(),
                split.row_count,
                split.dtype_summary.clone(),
                split.encoding_id.clone(),
                split.child_count,
                split.buffer_count,
            )?;
            evidence.provider_kind = split.provider_kind.as_str();
            evidence.provider_api_surface =
                static_provider_api_surface(&split.provider_api_surface);
            evidence.provider_boundary =
                reader_provider_boundary(split.provider_kind, evidence.provider_api_surface);
            Ok(evidence)
        })
        .collect()
}

fn reader_provider_boundary(
    provider_kind: ExecutionProviderKind,
    provider_api_surface: &'static str,
) -> VortexNativeProviderBoundary {
    let mut boundary = VortexNativeProviderBoundary::local_scan();
    boundary.provider_kind = provider_kind.as_str();
    boundary.provider_api_surface = provider_api_surface;
    boundary
}

fn static_provider_api_surface(value: &str) -> &'static str {
    match value {
        "vortex_reader_backed_encoded_filter" => "vortex_reader_backed_encoded_filter",
        "vortex_reader_backed_encoded_projection" => "vortex_reader_backed_encoded_projection",
        "vortex_reader_backed_encoded_filter_project" => {
            "vortex_reader_backed_encoded_filter_project"
        }
        "vortex_local_primitive" => "vortex_local_primitive",
        "vortex_prepared_encoded_filter" => "vortex_prepared_encoded_filter",
        "vortex_prepared_encoded_projection" => "vortex_prepared_encoded_projection",
        "vortex_prepared_encoded_filter_project" => "vortex_prepared_encoded_filter_project",
        "vortex_source_backed_encoded_filter" => "vortex_source_backed_encoded_filter",
        "vortex_source_backed_encoded_projection" => "vortex_source_backed_encoded_projection",
        "vortex_source_backed_encoded_filter_project" => {
            "vortex_source_backed_encoded_filter_project"
        }
        other => Box::leak(other.to_string().into_boxed_str()),
    }
}

fn result_from_local_engine_report(
    plan: &Plan,
    report: &VortexLocalEngineReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    result
        .artifact_refs
        .push("vortex_local_engine_report".to_string());
    result.result_refs.extend(report.value_summary.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    if let Some(local) = report.local_execution_report.as_ref() {
        result
            .artifact_refs
            .push("vortex_local_execution_report".to_string());
        result.diagnostics.extend(local.diagnostics.clone());
    }
    if let Some(local_primitive) = report.local_primitive_execution_report.as_ref() {
        result
            .artifact_refs
            .push("vortex_local_primitive_execution_report".to_string());
        result.split_refs.extend(
            local_primitive
                .reader_splits
                .iter()
                .map(|split| split.split_ref.clone()),
        );
        result
            .diagnostics
            .extend(local_primitive.diagnostics.clone());
    }
    result
}

fn result_from_prepared_filter_report(
    plan: &Plan,
    report: &VortexGeneralizedEncodedFilterExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    result.artifact_refs.push(report.report_id.clone());
    result
        .execution_certificate_refs
        .push(report.execution_certificate.certificate_id.clone());
    result
        .native_io_certificate_refs
        .push(report.native_io_certificate.certificate_id.clone());
    result.representation_transitions.extend(
        report
            .native_io_certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_prepared_projection_report(
    plan: &Plan,
    report: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    result.artifact_refs.push(report.report_id.clone());
    result
        .execution_certificate_refs
        .push(report.execution_certificate.certificate_id.clone());
    result
        .native_io_certificate_refs
        .push(report.native_io_certificate.certificate_id.clone());
    result.representation_transitions.extend(
        report
            .native_io_certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_prepared_filter_project_reports(
    plan: &Plan,
    filter: &VortexGeneralizedEncodedFilterExecutionReport,
    projection: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_prepared_projection_report(plan, projection);
    if filter.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(filter.report_id.clone());
    result
        .execution_certificate_refs
        .push(filter.execution_certificate.certificate_id.clone());
    result
        .native_io_certificate_refs
        .push(filter.native_io_certificate.certificate_id.clone());
    result.representation_transitions.extend(
        filter
            .native_io_certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.diagnostics.extend(filter.diagnostics.clone());
    result
}

fn result_from_source_filter_report(
    plan: &Plan,
    report: &VortexSourceBackedEncodedFilterExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_prepared_filter_report(plan, &report.prepared_execution);
    if report.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(report.report_id.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_source_projection_report(
    plan: &Plan,
    report: &VortexSourceBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_prepared_projection_report(plan, &report.prepared_execution);
    if report.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(report.report_id.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_source_filter_project_reports(
    plan: &Plan,
    filter: &VortexSourceBackedEncodedFilterExecutionReport,
    projection: &VortexSourceBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_source_projection_report(plan, projection);
    if filter.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(filter.report_id.clone());
    result.execution_certificate_refs.push(
        filter
            .prepared_execution
            .execution_certificate
            .certificate_id
            .clone(),
    );
    result.native_io_certificate_refs.push(
        filter
            .prepared_execution
            .native_io_certificate
            .certificate_id
            .clone(),
    );
    result.representation_transitions.extend(
        filter
            .prepared_execution
            .native_io_certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.diagnostics.extend(filter.diagnostics.clone());
    result
}

fn result_from_reader_filter_report(
    plan: &Plan,
    report: &VortexReaderBackedEncodedFilterExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_source_filter_report(plan, &report.source_execution);
    if report.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(report.report_id.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_reader_filter_project_reports(
    plan: &Plan,
    filter: &VortexReaderBackedEncodedFilterExecutionReport,
    projection: &VortexReaderBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_reader_projection_report(plan, projection);
    if filter.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(filter.report_id.clone());
    result.execution_certificate_refs.push(
        filter
            .source_execution
            .prepared_execution
            .execution_certificate
            .certificate_id
            .clone(),
    );
    result.native_io_certificate_refs.push(
        filter
            .source_execution
            .prepared_execution
            .native_io_certificate
            .certificate_id
            .clone(),
    );
    result.representation_transitions.extend(
        filter
            .source_execution
            .prepared_execution
            .native_io_certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.diagnostics.extend(filter.diagnostics.clone());
    result
}

fn result_from_reader_projection_report(
    plan: &Plan,
    report: &VortexReaderBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_source_projection_report(plan, &report.source_execution);
    if report.has_errors() {
        result.status = ShardLoomExecutionStatus::BlockedUnsupported;
    }
    result.artifact_refs.push(report.report_id.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_plan_and_errors(plan: &Plan, has_errors: bool) -> ShardLoomExecutionResult {
    let status = if has_errors {
        ShardLoomExecutionStatus::BlockedUnsupported
    } else {
        ShardLoomExecutionStatus::Executed
    };
    ShardLoomExecutionResult::from_plan(plan, status)
}

fn blocked_missing_payload(plan: &Plan, feature: &str) -> ShardLoomExecutionResult {
    ShardLoomExecutionResult::blocked_unsupported(
        plan,
        *unsupported_bridge_diagnostic(
            feature,
            "top-level plan did not include the payload required by the Vortex provider",
        ),
    )
}

fn unsupported_bridge_diagnostic(feature: &str, reason: &str) -> Box<Diagnostic> {
    Box::new(Diagnostic::unsupported(
        DiagnosticCode::NoFallbackExecution,
        feature,
        "Vortex top-level provider dispatch is unsupported for this plan shape.",
        Some(reason.to_string()),
    ))
}

#[cfg(test)]
mod tests {
    use shardloom_core::{
        ColumnRef, ComparisonOp, DatasetUri, EncodedSegment, EncodedValueBatch, EncodingKind,
        ExecutionProviderKind, LayoutKind, LogicalDType, Nullability, PredicateExpr, SegmentId,
        SegmentLayout, SegmentStats, StatValue, UniversalInputSource,
    };
    use shardloom_exec::{ShardLoomExecutionStatus, execute_with_provider};
    use shardloom_plan::{
        Plan, PlanId, PreparedEncodedBatch, PreparedEncodedPlan, ReaderBackedEncodedPlan,
        ReaderBackedSplitRef, SourceBackedEncodedPlan, SourceBackedPreparedEncodedBatch,
        VortexPrimitivePlan,
    };

    use super::{VortexTopLevelExecutionProvider, reader_backed_splits};

    fn column_ref(name: &str) -> ColumnRef {
        ColumnRef::new(name).expect("column")
    }

    fn segment(id: &str, row_count: u64) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            column_ref("metric"),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::Constant, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    #[test]
    fn provider_dispatches_local_count_plan_without_fallback() {
        let plan = Plan::vortex_primitive(
            PlanId::new("plan.count").expect("plan id"),
            VortexPrimitivePlan::count_all(
                DatasetUri::new("file:///definitely/missing.vortex").expect("uri"),
            ),
        );
        let provider = VortexTopLevelExecutionProvider::default();
        let result = execute_with_provider(&plan, &provider).expect("execution result");
        assert_ne!(
            result.status,
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired
        );
        assert!(
            result
                .artifact_refs
                .contains(&"vortex_local_engine_report".to_string())
        );
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
    }

    #[test]
    fn provider_blocks_unlowerable_predicate_without_fallback() {
        let plan = Plan::vortex_primitive(
            PlanId::new("plan.filter").expect("plan id"),
            VortexPrimitivePlan::filter(
                DatasetUri::new("file:///tmp/data.vortex").expect("uri"),
                PredicateExpr::AlwaysTrue,
            ),
        );
        let provider = VortexTopLevelExecutionProvider::default();
        let result = execute_with_provider(&plan, &provider).expect("execution result");
        assert_eq!(result.status, ShardLoomExecutionStatus::BlockedUnsupported);
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
    }

    #[test]
    fn provider_dispatches_prepared_encoded_filter_with_certificate_refs() {
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let batch = PreparedEncodedBatch::new(
            segment("segment-1.metric", 4),
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(9)),
                row_count: 4,
            },
        );
        let plan = Plan::prepared_encoded(
            PlanId::new("plan.prepared.filter").expect("plan id"),
            PreparedEncodedPlan::filter(predicate, vec![batch]),
        );
        let provider = VortexTopLevelExecutionProvider::default();
        let result = execute_with_provider(&plan, &provider).expect("execution result");
        assert_eq!(result.status, ShardLoomExecutionStatus::Executed);
        assert!(!result.execution_certificate_refs.is_empty());
        assert!(!result.native_io_certificate_refs.is_empty());
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
    }

    #[test]
    fn source_backed_filter_project_result_preserves_filter_native_io_transition() {
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let source = UniversalInputSource::from_dataset_uri(source_uri.clone()).expect("source");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let batch = PreparedEncodedBatch::new(
            segment("segment-1.metric", 4),
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(9)),
                row_count: 4,
            },
        );
        let filter_batch =
            SourceBackedPreparedEncodedBatch::new(source_uri.clone(), "split-1", batch.clone())
                .expect("filter batch");
        let projection_batch = SourceBackedPreparedEncodedBatch::new(source_uri, "split-1", batch)
            .expect("projection batch");
        let plan = Plan::source_backed_encoded(
            PlanId::new("plan.source.filter-project").expect("plan id"),
            SourceBackedEncodedPlan::filter_and_project(
                source,
                predicate,
                vec![column_ref("metric")],
                vec![filter_batch],
                vec![projection_batch],
            ),
        );
        let provider = VortexTopLevelExecutionProvider::default();

        let result = execute_with_provider(&plan, &provider).expect("execution result");

        assert_eq!(result.status, ShardLoomExecutionStatus::Executed);
        assert_eq!(
            result
                .representation_transitions
                .iter()
                .filter(|transition| {
                    transition.as_str() == "vortex_encoded->selection_vector_encoded"
                })
                .count(),
            2
        );
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
    }

    #[test]
    fn reader_backed_split_conversion_preserves_provider_evidence() {
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let split = ReaderBackedSplitRef::new(
            source_uri.clone(),
            "reader-split-1",
            "upstream-reader-boundary-1",
            ExecutionProviderKind::VortexSource,
            "vortex_reader_backed_encoded_projection",
            3,
            "struct(metric=int64)",
            "vortex.dictionary",
            1,
            2,
        )
        .expect("split");
        let source = UniversalInputSource::from_dataset_uri(source_uri).expect("source");
        let plan = ReaderBackedEncodedPlan::projection(
            source,
            vec![split],
            vec![column_ref("metric")],
            vec![],
        );

        let converted = reader_backed_splits(&plan).expect("reader splits");

        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].provider_kind, "vortex_source");
        assert_eq!(
            converted[0].provider_api_surface,
            "vortex_reader_backed_encoded_projection"
        );
        assert_eq!(
            converted[0].provider_boundary.provider_kind,
            "vortex_source"
        );
        assert_eq!(
            converted[0].provider_boundary.provider_api_surface,
            "vortex_reader_backed_encoded_projection"
        );
    }
}
