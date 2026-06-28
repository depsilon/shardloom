//! Provider-side bridge from top-level `ShardLoom` plans to Vortex-native reports.

use shardloom_core::{
    ColumnRef, ComparisonOp, Diagnostic, DiagnosticCode, ExecutionCertificate,
    ExecutionProviderKind, NativeIoCertificate, Result, StatValue,
};
use shardloom_exec::{
    ShardLoomExecutionInlineArtifact, ShardLoomExecutionProvider, ShardLoomExecutionResult,
    ShardLoomExecutionStatus,
};
use shardloom_plan::{
    EncodedExecutionOperation, Plan, PlanKind, PreparedEncodedBatch, PreparedEncodedPlan,
    ProjectionRequest, ReaderBackedEncodedPlan, SourceBackedEncodedPlan,
    SourceBackedPreparedEncodedBatch, VortexPrimitiveOperation, VortexPrimitivePlan,
};

use crate::{
    VortexEncodedValuePredicateBatch, VortexGeneralizedEncodedFilterExecutionReport,
    VortexGeneralizedEncodedProjectionExecutionReport, VortexLocalEnginePrimitive,
    VortexLocalEngineReport, VortexLocalEngineRequest, VortexLocalPrimitiveResourceEnvelope,
    VortexNativeProviderBoundary, VortexPreparedEncodedProjectionColumn,
    VortexReaderBackedEncodedFilterExecutionReport,
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
    local_primitive_correctness_fixture_for_request, local_primitive_execution_certificate,
    local_primitive_native_io_certificate, run_vortex_local_engine,
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
            memory_gb: VortexLocalPrimitiveResourceEnvelope::DEFAULT_MEMORY_GB,
            max_parallelism: VortexLocalPrimitiveResourceEnvelope::DEFAULT_MAX_PARALLELISM,
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
        let reader_splits = match reader_backed_splits(reader_backed) {
            Ok(reader_splits) => reader_splits,
            Err(diagnostic) => {
                return Ok(ShardLoomExecutionResult::blocked_unsupported(
                    plan,
                    *diagnostic,
                ));
            }
        };
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
) -> std::result::Result<Vec<VortexReaderBackedSplitEvidence>, Box<Diagnostic>> {
    plan.reader_splits
        .iter()
        .map(|split| {
            let provider_api_surface =
                static_provider_api_surface(&split.provider_api_surface).ok_or_else(|| {
                    unsupported_bridge_diagnostic(
                        "reader_backed_provider_api_surface",
                        &format!(
                            "reader-backed split `{}` declared unsupported provider API surface `{}`",
                            split.split_ref, split.provider_api_surface
                        ),
                    )
                })?;
            let mut evidence = VortexReaderBackedSplitEvidence::new(
                split.source_uri.clone(),
                split.split_ref.clone(),
                split.row_count,
                split.dtype_summary.clone(),
                split.encoding_id.clone(),
                split.child_count,
                split.buffer_count,
            )
            .map_err(|error| {
                unsupported_bridge_diagnostic(
                    "reader_backed_split_ref",
                    &format!(
                        "reader-backed split `{}` could not be converted to Vortex evidence: {error}",
                        split.split_ref
                    ),
                )
            })?;
            evidence.provider_kind = split.provider_kind.as_str();
            evidence.provider_api_surface = provider_api_surface;
            evidence.provider_boundary =
                reader_provider_boundary(split.provider_kind, provider_api_surface);
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

fn static_provider_api_surface(value: &str) -> Option<&'static str> {
    match value {
        "vortex_reader_backed_encoded_filter" => Some("vortex_reader_backed_encoded_filter"),
        "vortex_reader_backed_encoded_projection" => {
            Some("vortex_reader_backed_encoded_projection")
        }
        "vortex_reader_backed_encoded_filter_project" => {
            Some("vortex_reader_backed_encoded_filter_project")
        }
        "vortex_local_primitive" => Some("vortex_local_primitive"),
        "vortex_prepared_encoded_filter" => Some("vortex_prepared_encoded_filter"),
        "vortex_prepared_encoded_projection" => Some("vortex_prepared_encoded_projection"),
        "vortex_prepared_encoded_filter_project" => Some("vortex_prepared_encoded_filter_project"),
        "vortex_source_backed_encoded_filter" => Some("vortex_source_backed_encoded_filter"),
        "vortex_source_backed_encoded_projection" => {
            Some("vortex_source_backed_encoded_projection")
        }
        "vortex_source_backed_encoded_filter_project" => {
            Some("vortex_source_backed_encoded_filter_project")
        }
        _ => None,
    }
}

fn add_inline_artifact(
    result: &mut ShardLoomExecutionResult,
    artifact: ShardLoomExecutionInlineArtifact,
    artifact_ref: &str,
) {
    result.artifact_refs.push(artifact_ref.to_string());
    result.add_inline_artifact(artifact);
}

fn record_execution_certificate(
    result: &mut ShardLoomExecutionResult,
    certificate: &ExecutionCertificate,
) {
    result
        .execution_certificate_refs
        .push(certificate.certificate_id.clone());
    result.set_provider_version_if_absent(execution_certificate_provider_version(certificate));
    if certificate.external_query_engine_invoked {
        result.external_engine_invoked = true;
    }
    if certificate.fallback_attempted {
        result.fallback.attempted = true;
    }
    result.add_inline_artifact(execution_certificate_artifact(certificate));
}

fn record_native_io_certificate(
    result: &mut ShardLoomExecutionResult,
    certificate: &NativeIoCertificate,
) {
    result
        .native_io_certificate_refs
        .push(certificate.certificate_id.clone());
    result.representation_transitions.extend(
        certificate
            .representation_transitions
            .iter()
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    );
    result.materialization_boundary_refs.extend(
        certificate
            .materialization_boundaries
            .iter()
            .map(|boundary| boundary.boundary_id.clone()),
    );
    if certificate.fallback_attempted || certificate.side_effects.fallback_attempted {
        result.fallback.attempted = true;
    }
    if certificate.side_effects.fallback_execution_allowed {
        result.fallback.allowed = true;
    }
    result.add_inline_artifact(native_io_certificate_artifact(certificate));
}

fn record_provider_boundary_artifact(
    result: &mut ShardLoomExecutionResult,
    report_id: &str,
    boundary: &VortexNativeProviderBoundary,
) {
    result.set_provider_version_if_absent(Some(boundary.provider_version));
    result.add_inline_artifact(
        ShardLoomExecutionInlineArtifact::new(
            format!("{report_id}.provider_boundary"),
            "vortex_native_provider_boundary",
            if boundary.is_policy_admitted() {
                "available"
            } else {
                "evidence_incomplete"
            },
        )
        .with_field("provider_kind", boundary.provider_kind)
        .with_field("provider_crate", boundary.provider_crate)
        .with_field("provider_version", boundary.provider_version)
        .with_field("provider_api_surface", boundary.provider_api_surface)
        .with_field("feature_gate", boundary.feature_gate)
        .with_field("admission_policy", boundary.admission_policy)
        .with_field("certificate_requirement", boundary.certificate_requirement)
        .with_field(
            "external_query_engine_invoked",
            boundary.external_query_engine_invoked.to_string(),
        )
        .with_field(
            "fallback_attempted",
            boundary.fallback_attempted.to_string(),
        ),
    );
}

fn record_source_evidence_artifacts(
    result: &mut ShardLoomExecutionResult,
    report: &VortexSourceBackedEncodedFilterExecutionReport,
) {
    let evidence = report.evidence_gate_report();
    result.add_inline_artifact(
        ShardLoomExecutionInlineArtifact::new(
            evidence.report_id.clone(),
            "source_backed_expansion_evidence",
            if evidence.blocks_claims_without_benchmarks() {
                "evidence_incomplete"
            } else {
                "available"
            },
        )
        .with_field("execution_kind", evidence.execution_kind)
        .with_field("source_report_id", evidence.source_report_id)
        .with_field(
            "correctness_evidence_present",
            evidence.correctness_evidence_present.to_string(),
        )
        .with_field("correctness_refs", evidence.correctness_refs.join(","))
        .with_field(
            "benchmark_rows_present",
            evidence.benchmark_rows_present.to_string(),
        )
        .with_field(
            "benchmark_claim_allowed",
            evidence.benchmark_claim_allowed.to_string(),
        )
        .with_field(
            "execution_certificate_refs",
            evidence.execution_certificate_refs.join(","),
        )
        .with_field(
            "native_io_certificate_refs",
            evidence.native_io_certificate_refs.join(","),
        )
        .with_field(
            "native_io_certificate_path_refs",
            evidence.native_io_certificate_path_refs.join(","),
        )
        .with_field(
            "production_claim_allowed",
            evidence.production_claim_allowed.to_string(),
        )
        .with_field(
            "fallback_attempted",
            evidence.fallback_attempted.to_string(),
        ),
    );
    let pair = report.certificate_pair_report();
    result.add_inline_artifact(
        ShardLoomExecutionInlineArtifact::new(
            pair.report_id.clone(),
            "source_backed_certificate_pair",
            if pair.certificate_pair_complete {
                "available"
            } else {
                "evidence_incomplete"
            },
        )
        .with_field("execution_kind", pair.execution_kind)
        .with_field("source_report_id", pair.source_report_id)
        .with_field("execution_certificate_id", pair.execution_certificate_id)
        .with_field(
            "execution_certificate_status",
            pair.execution_certificate_status,
        )
        .with_field("native_io_certificate_id", pair.native_io_certificate_id)
        .with_field(
            "native_io_certificate_path_id",
            pair.native_io_certificate_path_id,
        )
        .with_field(
            "native_io_certificate_status",
            pair.native_io_certificate_status,
        )
        .with_field(
            "certificate_pair_complete",
            pair.certificate_pair_complete.to_string(),
        )
        .with_field("fallback_attempted", pair.fallback_attempted.to_string()),
    );
}

fn record_source_projection_evidence_artifacts(
    result: &mut ShardLoomExecutionResult,
    report: &VortexSourceBackedEncodedProjectionExecutionReport,
) {
    let evidence = report.evidence_gate_report();
    result.add_inline_artifact(
        ShardLoomExecutionInlineArtifact::new(
            evidence.report_id.clone(),
            "source_backed_expansion_evidence",
            if evidence.blocks_claims_without_benchmarks() {
                "evidence_incomplete"
            } else {
                "available"
            },
        )
        .with_field("execution_kind", evidence.execution_kind)
        .with_field("source_report_id", evidence.source_report_id)
        .with_field(
            "correctness_evidence_present",
            evidence.correctness_evidence_present.to_string(),
        )
        .with_field("correctness_refs", evidence.correctness_refs.join(","))
        .with_field(
            "benchmark_rows_present",
            evidence.benchmark_rows_present.to_string(),
        )
        .with_field(
            "benchmark_claim_allowed",
            evidence.benchmark_claim_allowed.to_string(),
        )
        .with_field(
            "execution_certificate_refs",
            evidence.execution_certificate_refs.join(","),
        )
        .with_field(
            "native_io_certificate_refs",
            evidence.native_io_certificate_refs.join(","),
        )
        .with_field(
            "native_io_certificate_path_refs",
            evidence.native_io_certificate_path_refs.join(","),
        )
        .with_field(
            "production_claim_allowed",
            evidence.production_claim_allowed.to_string(),
        )
        .with_field(
            "fallback_attempted",
            evidence.fallback_attempted.to_string(),
        ),
    );
    let pair = report.certificate_pair_report();
    result.add_inline_artifact(
        ShardLoomExecutionInlineArtifact::new(
            pair.report_id.clone(),
            "source_backed_certificate_pair",
            if pair.certificate_pair_complete {
                "available"
            } else {
                "evidence_incomplete"
            },
        )
        .with_field("execution_kind", pair.execution_kind)
        .with_field("source_report_id", pair.source_report_id)
        .with_field("execution_certificate_id", pair.execution_certificate_id)
        .with_field(
            "execution_certificate_status",
            pair.execution_certificate_status,
        )
        .with_field("native_io_certificate_id", pair.native_io_certificate_id)
        .with_field(
            "native_io_certificate_path_id",
            pair.native_io_certificate_path_id,
        )
        .with_field(
            "native_io_certificate_status",
            pair.native_io_certificate_status,
        )
        .with_field(
            "certificate_pair_complete",
            pair.certificate_pair_complete.to_string(),
        )
        .with_field("fallback_attempted", pair.fallback_attempted.to_string()),
    );
}

fn execution_certificate_provider_version(certificate: &ExecutionCertificate) -> Option<&str> {
    certificate.provider_version.as_deref().or_else(|| {
        certificate
            .provider_crate
            .as_deref()
            .is_some_and(|provider_crate| provider_crate == "shardloom-vortex")
            .then_some(env!("CARGO_PKG_VERSION"))
    })
}

fn execution_certificate_artifact(
    certificate: &ExecutionCertificate,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        certificate.certificate_id.clone(),
        "execution_certificate",
        certificate.status.as_str(),
    )
    .with_field("schema_version", certificate.schema_version)
    .with_field("execution_kind", certificate.execution_kind.clone())
    .with_field(
        "execution_provider_kind",
        certificate.execution_provider_kind.as_str(),
    )
    .with_field("provider_scope", certificate.provider_scope.clone())
    .with_field(
        "provider_crate",
        optional_string(certificate.provider_crate.as_deref()),
    )
    .with_field(
        "provider_version",
        optional_string(execution_certificate_provider_version(certificate)),
    )
    .with_field(
        "provider_api_surface",
        optional_string(certificate.provider_api_surface.as_deref()),
    )
    .with_field("certificate_status", certificate.status.as_str())
    .with_field(
        "correctness_fixture_id",
        optional_string(certificate.correctness_fixture_id.as_deref()),
    )
    .with_field(
        "correctness_passed",
        certificate.correctness_passed.to_string(),
    )
    .with_field("data_read", certificate.data_read.to_string())
    .with_field("data_decoded", certificate.data_decoded.to_string())
    .with_field(
        "data_materialized",
        certificate.data_materialized.to_string(),
    )
    .with_field(
        "external_query_engine_invoked",
        certificate.external_query_engine_invoked.to_string(),
    )
    .with_field(
        "fallback_attempted",
        certificate.fallback_attempted.to_string(),
    )
}

fn native_io_certificate_artifact(
    certificate: &NativeIoCertificate,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        certificate.certificate_id.clone(),
        "native_io_certificate",
        certificate.status(),
    )
    .with_field("schema_version", certificate.schema_version)
    .with_field("path_id", certificate.path_id.clone())
    .with_field("certificate_status", certificate.status())
    .with_field(
        "representation_transitions",
        certificate.representation_transition_order(),
    )
    .with_field(
        "materialization_boundary_refs",
        certificate.materialization_boundary_order(),
    )
    .with_field(
        "source_kind",
        certificate.source_capability_report.source_kind.clone(),
    )
    .with_field(
        "sink_target_format",
        certificate.sink_requirement_report.target_format.clone(),
    )
    .with_field("data_read", certificate.side_effects.data_read.to_string())
    .with_field(
        "data_decoded",
        certificate.side_effects.data_decoded.to_string(),
    )
    .with_field(
        "data_materialized",
        certificate.side_effects.data_materialized.to_string(),
    )
    .with_field("write_io", certificate.side_effects.write_io.to_string())
    .with_field(
        "fallback_attempted",
        (certificate.fallback_attempted || certificate.side_effects.fallback_attempted).to_string(),
    )
}

fn local_engine_artifact(report: &VortexLocalEngineReport) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        "vortex_local_engine_report",
        "vortex_local_engine_report",
        artifact_status(report.has_errors()),
    )
    .with_field("status", report.status.as_str())
    .with_field("mode", report.mode.as_str())
    .with_field("primitive", report.request.primitive.summary())
    .with_field("result_known", report.result_known.to_string())
    .with_field(
        "value_summary",
        report
            .value_summary
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    )
    .with_field("task_count", report.task_count.to_string())
    .with_field(
        "decision_trace_entries",
        report.decision_trace_entries.to_string(),
    )
    .with_field(
        "work_avoided_metrics",
        report.work_avoided_metrics.to_string(),
    )
    .with_field("tasks_executed", report.tasks_executed.to_string())
    .with_field("data_read", report.data_read.to_string())
    .with_field("data_decoded", report.data_decoded.to_string())
    .with_field("data_materialized", report.data_materialized.to_string())
    .with_field(
        "fallback_execution_allowed",
        report.fallback_execution_allowed.to_string(),
    )
}

fn local_primitive_artifact(
    report: &crate::VortexLocalPrimitiveExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        "vortex_local_primitive_execution_report",
        "vortex_local_primitive_execution_report",
        artifact_status(report.has_errors()),
    )
    .with_field("status", report.status.as_str())
    .with_field("mode", report.mode.as_str())
    .with_field("primitive_kind", report.primitive_kind.as_str())
    .with_field(
        "result_summary",
        report
            .result_summary
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    )
    .with_field("rows_scanned", report.rows_scanned.to_string())
    .with_field(
        "rows_selected",
        report
            .rows_selected
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    )
    .with_field(
        "rows_projected",
        report
            .rows_projected
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    )
    .with_field("projected_columns", report.projected_columns.join(","))
    .with_field("reader_split_count", report.reader_splits.len().to_string())
    .with_field("arrays_read_count", report.arrays_read_count.to_string())
    .with_field(
        "streaming_scan_used",
        report.streaming_scan_used.to_string(),
    )
    .with_field(
        "full_stream_collected",
        report.full_stream_collected.to_string(),
    )
    .with_field(
        "scan_concurrency_per_worker",
        report.scan_concurrency_per_worker.to_string(),
    )
    .with_field(
        "filter_pushdown_applied",
        report.filter_pushdown_applied.to_string(),
    )
    .with_field(
        "projection_pushdown_applied",
        report.projection_pushdown_applied.to_string(),
    )
    .with_field(
        "materialization_boundary_reported",
        report.materialization_boundary_reported.to_string(),
    )
    .with_field(
        "fallback_execution_allowed",
        report.fallback_execution_allowed.to_string(),
    )
}

fn prepared_filter_artifact(
    report: &VortexGeneralizedEncodedFilterExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_prepared_encoded_filter_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("predicate_summary", report.predicate_summary.clone())
    .with_field(
        "encoded_batch_count",
        report.encoded_batch_count.to_string(),
    )
    .with_field("segment_count", report.segment_count.to_string())
    .with_field(
        "selection_vector_count",
        report.selection_vector_count.to_string(),
    )
    .with_field(
        "selected_row_count",
        report
            .selected_row_count
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    )
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn prepared_projection_artifact(
    report: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_prepared_encoded_projection_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("requested_columns", report.requested_columns.join(","))
    .with_field("projected_columns", report.projected_columns.join(","))
    .with_field("input_batch_count", report.input_batch_count.to_string())
    .with_field(
        "projected_batch_count",
        report.projected_batch_count.to_string(),
    )
    .with_field("filter_project", report.filter_project.to_string())
    .with_field(
        "selection_vector_preserved",
        report.selection_vector_preserved.to_string(),
    )
    .with_field(
        "projected_row_count",
        report
            .projected_row_count
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    )
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn source_filter_artifact(
    report: &VortexSourceBackedEncodedFilterExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_source_backed_encoded_filter_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("source_summary", report.source_summary.clone())
    .with_field("split_count", report.split_count.to_string())
    .with_field("source_batch_count", report.source_batch_count.to_string())
    .with_field(
        "source_uri_matches_batches",
        report.source_uri_matches_batches.to_string(),
    )
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "selection_vector_guaranteed",
        report.selection_vector_guaranteed.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn source_projection_artifact(
    report: &VortexSourceBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_source_backed_encoded_projection_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("source_summary", report.source_summary.clone())
    .with_field("split_count", report.split_count.to_string())
    .with_field("source_batch_count", report.source_batch_count.to_string())
    .with_field(
        "source_uri_matches_batches",
        report.source_uri_matches_batches.to_string(),
    )
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "encoded_projection_guaranteed",
        report.encoded_projection_guaranteed.to_string(),
    )
    .with_field(
        "selection_vector_preserved",
        report.selection_vector_preserved.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn reader_filter_artifact(
    report: &VortexReaderBackedEncodedFilterExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_reader_backed_encoded_filter_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("provider_kind", report.provider_kind)
    .with_field("provider_api_surface", report.provider_api_surface)
    .with_field("reader_split_count", report.reader_split_count.to_string())
    .with_field("source_batch_count", report.source_batch_count.to_string())
    .with_field("reader_split_refs", report.reader_split_refs.join(","))
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "reader_generated_prepared_batches",
        report.reader_generated_prepared_batches.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("data_read", report.data_read.to_string())
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn reader_projection_artifact(
    report: &VortexReaderBackedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionInlineArtifact {
    ShardLoomExecutionInlineArtifact::new(
        report.report_id.clone(),
        "vortex_reader_backed_encoded_projection_report",
        artifact_status(report.has_errors()),
    )
    .with_field("schema_version", report.schema_version)
    .with_field("execution_kind", report.execution_kind)
    .with_field("status", report.status.as_str())
    .with_field("provider_kind", report.provider_kind)
    .with_field("provider_api_surface", report.provider_api_surface)
    .with_field("reader_split_count", report.reader_split_count.to_string())
    .with_field("source_batch_count", report.source_batch_count.to_string())
    .with_field("reader_split_refs", report.reader_split_refs.join(","))
    .with_field(
        "runtime_execution_allowed",
        report.runtime_execution_allowed.to_string(),
    )
    .with_field(
        "reader_generated_prepared_batches",
        report.reader_generated_prepared_batches.to_string(),
    )
    .with_field(
        "encoded_projection_guaranteed",
        report.encoded_projection_guaranteed.to_string(),
    )
    .with_field(
        "selection_vector_preserved",
        report.selection_vector_preserved.to_string(),
    )
    .with_field(
        "correctness_certified",
        report.correctness_certified.to_string(),
    )
    .with_field("data_read", report.data_read.to_string())
    .with_field("fallback_attempted", report.fallback_attempted.to_string())
}

fn artifact_status(has_errors: bool) -> &'static str {
    if has_errors { "blocked" } else { "available" }
}

fn optional_string(value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| "none".to_string(), str::to_string)
}

fn result_from_local_engine_report(
    plan: &Plan,
    report: &VortexLocalEngineReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    add_inline_artifact(
        &mut result,
        local_engine_artifact(report),
        "vortex_local_engine_report",
    );
    result.result_refs.extend(report.value_summary.clone());
    result.diagnostics.extend(report.diagnostics.clone());
    if let Some(local) = report.local_execution_report.as_ref() {
        add_inline_artifact(
            &mut result,
            ShardLoomExecutionInlineArtifact::new(
                "vortex_local_execution_report",
                "vortex_local_execution_report",
                artifact_status(local.has_errors()),
            )
            .with_field("status", local.status.as_str())
            .with_field("mode", local.mode.as_str())
            .with_field("tasks_executed", local.tasks_executed.to_string())
            .with_field("data_read", local.data_read.to_string())
            .with_field("data_decoded", local.data_decoded.to_string())
            .with_field("data_materialized", local.data_materialized.to_string())
            .with_field(
                "fallback_execution_allowed",
                local.fallback_execution_allowed.to_string(),
            ),
            "vortex_local_execution_report",
        );
        result.diagnostics.extend(local.diagnostics.clone());
    }
    if let Some(local_primitive) = report.local_primitive_execution_report.as_ref() {
        add_inline_artifact(
            &mut result,
            local_primitive_artifact(local_primitive),
            "vortex_local_primitive_execution_report",
        );
        result.split_refs.extend(
            local_primitive
                .reader_splits
                .iter()
                .map(|split| split.split_ref.clone()),
        );
        result
            .diagnostics
            .extend(local_primitive.diagnostics.clone());
        if let Some(query_request) = report.query_request.as_ref() {
            if let Ok(certificate) =
                local_primitive_native_io_certificate(query_request, local_primitive)
            {
                record_native_io_certificate(&mut result, &certificate);
            }
            if let Some(fixture) =
                local_primitive_correctness_fixture_for_request(query_request, local_primitive)
                && let Ok(certificate) =
                    local_primitive_execution_certificate(&fixture, query_request, local_primitive)
            {
                record_execution_certificate(&mut result, &certificate);
            }
        }
    }
    result
}

fn result_from_prepared_filter_report(
    plan: &Plan,
    report: &VortexGeneralizedEncodedFilterExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    add_inline_artifact(
        &mut result,
        prepared_filter_artifact(report),
        report.report_id.as_str(),
    );
    record_execution_certificate(&mut result, &report.execution_certificate);
    record_native_io_certificate(&mut result, &report.native_io_certificate);
    result.diagnostics.extend(report.diagnostics.clone());
    result
}

fn result_from_prepared_projection_report(
    plan: &Plan,
    report: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> ShardLoomExecutionResult {
    let mut result = result_from_plan_and_errors(plan, report.has_errors());
    add_inline_artifact(
        &mut result,
        prepared_projection_artifact(report),
        report.report_id.as_str(),
    );
    record_execution_certificate(&mut result, &report.execution_certificate);
    record_native_io_certificate(&mut result, &report.native_io_certificate);
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
    add_inline_artifact(
        &mut result,
        prepared_filter_artifact(filter),
        filter.report_id.as_str(),
    );
    record_execution_certificate(&mut result, &filter.execution_certificate);
    record_native_io_certificate(&mut result, &filter.native_io_certificate);
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
    add_inline_artifact(
        &mut result,
        source_filter_artifact(report),
        report.report_id.as_str(),
    );
    record_source_evidence_artifacts(&mut result, report);
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
    add_inline_artifact(
        &mut result,
        source_projection_artifact(report),
        report.report_id.as_str(),
    );
    record_source_projection_evidence_artifacts(&mut result, report);
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
    add_inline_artifact(
        &mut result,
        source_filter_artifact(filter),
        filter.report_id.as_str(),
    );
    record_execution_certificate(
        &mut result,
        &filter.prepared_execution.execution_certificate,
    );
    record_native_io_certificate(
        &mut result,
        &filter.prepared_execution.native_io_certificate,
    );
    record_source_evidence_artifacts(&mut result, filter);
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
    add_inline_artifact(
        &mut result,
        reader_filter_artifact(report),
        report.report_id.as_str(),
    );
    record_provider_boundary_artifact(
        &mut result,
        report.report_id.as_str(),
        &report.provider_boundary,
    );
    result.split_refs.extend(report.reader_split_refs.clone());
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
    add_inline_artifact(
        &mut result,
        reader_filter_artifact(filter),
        filter.report_id.as_str(),
    );
    record_provider_boundary_artifact(
        &mut result,
        filter.report_id.as_str(),
        &filter.provider_boundary,
    );
    record_execution_certificate(
        &mut result,
        &filter
            .source_execution
            .prepared_execution
            .execution_certificate,
    );
    record_native_io_certificate(
        &mut result,
        &filter
            .source_execution
            .prepared_execution
            .native_io_certificate,
    );
    result.split_refs.extend(filter.reader_split_refs.clone());
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
    add_inline_artifact(
        &mut result,
        reader_projection_artifact(report),
        report.report_id.as_str(),
    );
    record_provider_boundary_artifact(
        &mut result,
        report.report_id.as_str(),
        &report.provider_boundary,
    );
    result.split_refs.extend(report.reader_split_refs.clone());
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
        assert_eq!(
            result.provider_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert!(result.inline_artifacts.iter().any(|artifact| {
            artifact.artifact_kind == "vortex_prepared_encoded_filter_report"
                && artifact
                    .fields
                    .iter()
                    .any(|(key, value)| key == "runtime_execution_allowed" && value == "true")
        }));
        assert!(result.inline_artifacts.iter().any(|artifact| {
            artifact.artifact_kind == "execution_certificate"
                && artifact.fields.iter().any(|(key, value)| {
                    key == "provider_version" && value == env!("CARGO_PKG_VERSION")
                })
        }));
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
        assert!(result.inline_artifacts.iter().any(|artifact| {
            artifact.artifact_kind == "source_backed_expansion_evidence"
                && artifact.status == "evidence_incomplete"
        }));
        assert!(result.inline_artifacts.iter().any(|artifact| {
            artifact.artifact_kind == "source_backed_certificate_pair"
                && artifact
                    .fields
                    .iter()
                    .any(|(key, value)| key == "execution_certificate_id" && !value.is_empty())
        }));
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

    #[test]
    fn reader_backed_split_conversion_rejects_unknown_provider_surface_without_leak() {
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let split = ReaderBackedSplitRef::new(
            source_uri.clone(),
            "reader-split-1",
            "upstream-reader-boundary-1",
            ExecutionProviderKind::VortexSource,
            "attacker-controlled-provider-surface",
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

        let error = reader_backed_splits(&plan).expect_err("unknown provider surface rejects");

        assert_eq!(
            error.feature.as_deref(),
            Some("reader_backed_provider_api_surface")
        );
        assert!(
            error
                .suggested_next_step
                .as_deref()
                .is_some_and(|detail| detail.contains("attacker-controlled-provider-surface"))
        );
    }
}
