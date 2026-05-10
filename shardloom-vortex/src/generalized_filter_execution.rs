use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, ExecutionCertificate, NativeIoCertificate, PredicateExpr, Result,
};

use crate::{
    VortexLocalPrimitiveExecutionMode, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexLocalPrimitiveExecutionStatus,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    execute_vortex_local_primitive_with_policy, local_primitive_correctness_fixture_for_request,
    local_primitive_execution_certificate, local_primitive_native_io_certificate,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_generalized_filter_execution.v1";
const REPORT_ID: &str = "vortex.cg2.generalized-filter.local-scan-pushdown";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedFilterExecutionStatus {
    FeatureDisabled,
    ExecutedLocalScanPushdown,
    BlockedUnsupportedPrimitive,
    BlockedUnsafeEvidence,
}

impl VortexGeneralizedFilterExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::ExecutedLocalScanPushdown => "executed_local_scan_pushdown",
            Self::BlockedUnsupportedPrimitive => "blocked_unsupported_primitive",
            Self::BlockedUnsafeEvidence => "blocked_unsafe_evidence",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        matches!(
            self,
            Self::BlockedUnsupportedPrimitive | Self::BlockedUnsafeEvidence
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexGeneralizedFilterExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub predicate_summary: Option<String>,
    pub status: VortexGeneralizedFilterExecutionStatus,
    pub local_primitive_report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: Option<NativeIoCertificate>,
    pub execution_certificate: Option<ExecutionCertificate>,
    pub runtime_execution_allowed: bool,
    pub selection_vector_guaranteed: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexGeneralizedFilterExecutionReport {
    fn unsupported(request: &VortexQueryPrimitiveRequest) -> Self {
        let local_primitive_report =
            VortexLocalPrimitiveExecutionReport::feature_disabled(request.kind);
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.push(Diagnostic::not_implemented(
            "vortex_generalized_filter_execution",
            format!(
                "generalized filter execution only supports CountWhere and FilterPredicate, got {}",
                request.kind.as_str()
            ),
            "Use CountWhere or FilterPredicate while projection/filter-project generalization remains separate work.",
        ));
        let fallback_attempted = diagnostics
            .iter()
            .any(|diagnostic| diagnostic.fallback.attempted);
        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            primitive_kind: request.kind,
            predicate_summary: request.predicate.as_ref().map(PredicateExpr::summary),
            status: VortexGeneralizedFilterExecutionStatus::BlockedUnsupportedPrimitive,
            local_primitive_report,
            native_io_certificate: None,
            execution_certificate: None,
            runtime_execution_allowed: false,
            selection_vector_guaranteed: false,
            correctness_certified: false,
            production_claim_allowed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            fallback_attempted,
            diagnostics,
        }
    }

    fn from_local_report(
        request: &VortexQueryPrimitiveRequest,
        local_primitive_report: VortexLocalPrimitiveExecutionReport,
        native_io_certificate: Option<NativeIoCertificate>,
        execution_certificate: Option<ExecutionCertificate>,
    ) -> Self {
        let native_io_safe = native_io_certificate
            .as_ref()
            .is_some_and(NativeIoCertificate::is_certified);
        let safe = generalized_filter_local_scan_pushdown_safe(&local_primitive_report)
            && native_io_safe
            && request
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted);
        let correctness_certified = execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified);
        let status = if local_primitive_report.status
            == VortexLocalPrimitiveExecutionStatus::FeatureDisabled
        {
            VortexGeneralizedFilterExecutionStatus::FeatureDisabled
        } else if safe {
            VortexGeneralizedFilterExecutionStatus::ExecutedLocalScanPushdown
        } else {
            VortexGeneralizedFilterExecutionStatus::BlockedUnsafeEvidence
        };
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.extend(local_primitive_report.diagnostics.clone());
        if let Some(certificate) = &native_io_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
        if let Some(certificate) = &execution_certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            primitive_kind: request.kind,
            predicate_summary: request.predicate.as_ref().map(PredicateExpr::summary),
            status,
            runtime_execution_allowed: safe,
            selection_vector_guaranteed: safe && local_primitive_report.rows_selected.is_some(),
            correctness_certified,
            production_claim_allowed: false,
            data_read: local_primitive_report.data_read,
            data_decoded: local_primitive_report.data_decoded,
            data_materialized: local_primitive_report.data_materialized,
            row_read: local_primitive_report.row_read,
            arrow_converted: local_primitive_report.arrow_converted,
            object_store_io: local_primitive_report.object_store_io,
            write_io: local_primitive_report.write_io,
            spill_io_performed: local_primitive_report.spill_io_performed,
            external_effects_executed: local_primitive_report.external_effects_executed,
            fallback_execution_allowed: local_primitive_report.fallback_execution_allowed,
            fallback_attempted: diagnostics
                .iter()
                .any(|diagnostic| diagnostic.fallback.attempted),
            local_primitive_report,
            native_io_certificate,
            execution_certificate,
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self
                .native_io_certificate
                .as_ref()
                .is_some_and(NativeIoCertificate::has_errors)
            || self
                .execution_certificate
                .as_ref()
                .is_some_and(execution_certificate_has_errors)
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn avoids_unsafe_effects(&self) -> bool {
        !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex generalized filter execution");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "primitive: {}", self.primitive_kind.as_str());
        let _ = writeln!(
            &mut out,
            "predicate: {}",
            self.predicate_summary.as_deref().unwrap_or("none")
        );
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(
            &mut out,
            "runtime execution allowed: {}",
            self.runtime_execution_allowed
        );
        let _ = writeln!(
            &mut out,
            "selection vector guaranteed: {}",
            self.selection_vector_guaranteed
        );
        let _ = writeln!(
            &mut out,
            "correctness certified: {}",
            self.correctness_certified
        );
        let _ = writeln!(&mut out, "production claim allowed: false");
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

/// Executes the currently approved generalized local filter/count-where slice.
///
/// This is intentionally limited to local `.vortex` scan-pushdown evidence.
/// It does not add new readers, object-store access, decoded fallback,
/// SQL/DataFrame execution, writes, spill, or production certification.
///
/// # Errors
/// Returns an error only when the underlying local primitive report or Native
/// I/O certificate cannot be built.
pub fn execute_vortex_generalized_filter_from_local_scan_pushdown(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexGeneralizedFilterExecutionReport> {
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate
    ) {
        return Ok(VortexGeneralizedFilterExecutionReport::unsupported(request));
    }
    let local_primitive_report = execute_vortex_local_primitive_with_policy(request, policy)?;
    let native_io_certificate = Some(local_primitive_native_io_certificate(
        request,
        &local_primitive_report,
    )?);
    let execution_certificate =
        local_primitive_correctness_fixture_for_request(request, &local_primitive_report)
            .map(|fixture| {
                local_primitive_execution_certificate(&fixture, request, &local_primitive_report)
            })
            .transpose()?;
    Ok(VortexGeneralizedFilterExecutionReport::from_local_report(
        request,
        local_primitive_report,
        native_io_certificate,
        execution_certificate,
    ))
}

fn execution_certificate_has_errors(certificate: &ExecutionCertificate) -> bool {
    certificate.fallback_attempted
        || certificate.fallback_execution_allowed
        || certificate.unsafe_effect_detected
        || certificate.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            )
        })
}

fn generalized_filter_local_scan_pushdown_safe(
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    matches!(
        report.primitive_kind,
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate
    ) && report.status == VortexLocalPrimitiveExecutionStatus::Executed
        && report.mode == VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        && report.rows_selected.is_some()
        && report.filter_pushdown_applied
        && report.upstream_filter_expression_used
        && report.streaming_scan_used
        && !report.full_stream_collected
        && report.data_read
        && report.upstream_scan_called
        && !report.data_decoded
        && !report.data_materialized
        && !report.row_read
        && !report.arrow_converted
        && !report.object_store_io
        && !report.write_io
        && !report.spill_io_performed
        && !report.external_effects_executed
        && !report.fallback_execution_allowed
        && !report.materialization_boundary_reported
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DatasetUri;
    #[cfg(feature = "vortex-local-primitives")]
    use shardloom_core::{ColumnRef, ComparisonOp, StatValue};

    #[cfg(feature = "vortex-local-primitives")]
    fn copied_struct_fixture_path(name: &str) -> std::path::PathBuf {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let temp_path = std::env::temp_dir().join(format!(
            "shardloom-generalized-filter-{name}-{}-{nanos}.vortex",
            std::process::id()
        ));
        std::fs::copy(&fixture_path, &temp_path).expect("copy fixture");
        temp_path
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn generalized_filter_executes_copied_local_vortex_filter_scan_pushdown() {
        let path = copied_struct_fixture_path("filter");
        let request = VortexQueryPrimitiveRequest::filter(
            DatasetUri::new(path.to_string_lossy().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_generalized_filter_from_local_scan_pushdown(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            report.status,
            VortexGeneralizedFilterExecutionStatus::ExecutedLocalScanPushdown
        );
        assert_eq!(report.local_primitive_report.rows_selected, Some(3));
        assert!(report.runtime_execution_allowed);
        assert!(report.selection_vector_guaranteed);
        assert!(!report.correctness_certified);
        assert!(!report.production_claim_allowed);
        assert!(report.data_read);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.is_some());
        assert!(report.execution_certificate.is_none());
        assert!(
            report
                .native_io_certificate
                .as_ref()
                .is_some_and(NativeIoCertificate::is_certified)
        );
        assert!(!report.has_errors());
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn generalized_filter_executes_copied_local_vortex_count_where_scan_pushdown() {
        let path = copied_struct_fixture_path("count-where");
        let request = VortexQueryPrimitiveRequest::count_where(
            DatasetUri::new(path.to_string_lossy().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_generalized_filter_from_local_scan_pushdown(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            report.status,
            VortexGeneralizedFilterExecutionStatus::ExecutedLocalScanPushdown
        );
        assert_eq!(report.local_primitive_report.rows_selected, Some(3));
        assert!(report.runtime_execution_allowed);
        assert!(report.selection_vector_guaranteed);
        assert!(report.native_io_certificate.is_some());
        assert!(report.execution_certificate.is_none());
        assert!(!report.has_errors());
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn generalized_filter_certifies_checked_in_filter_fixture() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter(
            DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_generalized_filter_from_local_scan_pushdown(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedFilterExecutionStatus::ExecutedLocalScanPushdown
        );
        assert!(report.correctness_certified);
        assert!(
            report
                .execution_certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        );
        assert_eq!(
            report
                .execution_certificate
                .as_ref()
                .and_then(|certificate| certificate.correctness_fixture_id.as_deref()),
            Some("vortex-local-filter-struct-five")
        );
        assert!(!report.production_claim_allowed);
        assert!(!report.has_errors());
    }

    #[test]
    fn generalized_filter_rejects_projection_without_execution() {
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new("file:///tmp/input.vortex").expect("uri"),
            shardloom_plan::ProjectionRequest::All,
        );

        let report = execute_vortex_generalized_filter_from_local_scan_pushdown(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedFilterExecutionStatus::BlockedUnsupportedPrimitive
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.selection_vector_guaranteed);
        assert!(!report.fallback_attempted);
        assert!(report.has_errors());
    }

    #[test]
    fn generalized_filter_unsupported_report_preserves_fallback_attempt_diagnostics() {
        let mut request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new("file:///tmp/input.vortex").expect("uri"),
            shardloom_plan::ProjectionRequest::All,
        );
        request.diagnostics.push(Diagnostic::new(
            shardloom_core::DiagnosticCode::NoFallbackExecution,
            shardloom_core::DiagnosticSeverity::Error,
            shardloom_core::DiagnosticCategory::NoFallbackPolicy,
            "fallback was attempted before generalized filter admission",
            Some("vortex_generalized_filter_execution".to_string()),
            Some("review regression fixture".to_string()),
            Some("preserve attempted fallback evidence".to_string()),
            shardloom_core::FallbackStatus {
                attempted: true,
                allowed: false,
                engine: Some("external".to_string()),
                reason: "test fallback attempt".to_string(),
            },
        ));

        let report = execute_vortex_generalized_filter_from_local_scan_pushdown(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedFilterExecutionStatus::BlockedUnsupportedPrimitive
        );
        assert!(report.fallback_attempted);
        assert!(report.has_errors());
    }
}
