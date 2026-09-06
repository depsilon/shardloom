//! Caller-retained filtered counts using the ordinary native scan and report.
//!
//! Only the admitted request, lowered predicate/projection, source and runtime
//! are retained. Each call evaluates the predicate again, including validation
//! around a metadata-pruned zero result. No answer or correctness oracle is cached.

use super::{
    LocalVortexScanPlan, Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexLocalPrimitivePhysicalPolicyReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, attach_predicate_to_scan_plan,
    count_where_report, local_primitive_native_io_certificate, local_vortex_path, prepared_scan,
    project_count_where_predicate_columns,
};
use crate::resident_session::{
    PreparedVortexSource, ResidentSessionSnapshot, ResidentVortexSession,
};
use std::fmt::Write as _;

/// A complete native filtered count and the evidence produced by this execution.
pub struct ExecutedVortexCountWhere {
    pub count: u64,
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
}

/// A retained local source and one lowered count predicate. Results are never cached.
pub struct PreparedVortexCountWhere {
    request: VortexQueryPrimitiveRequest,
    source: PreparedVortexSource,
    session: ResidentVortexSession,
    path: std::path::PathBuf,
    plan: LocalVortexScanPlan,
    policy: VortexLocalPrimitiveExecutionPolicy,
    physical_policy: VortexLocalPrimitivePhysicalPolicyReport,
}

/// Prepare a filtered count in an existing native session.
///
/// # Errors
/// Rejects non-count requests, extra operation payloads, unsafe diagnostics,
/// unsupported predicates and nonlocal sources. No external engine is invoked.
pub fn prepare_count_where_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexCountWhere> {
    if request.kind != VortexQueryPrimitiveKind::CountWhere
        || request.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            ) || diagnostic.fallback.attempted
        })
    {
        return Err(failed(
            "only admitted filtered-count requests are supported",
        ));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is required"))?;
    let predicate = request
        .predicate
        .as_ref()
        .ok_or_else(|| failed("predicate is required"))?;
    let mut canonical = VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate.clone());
    canonical.diagnostics.clone_from(&request.diagnostics);
    if &canonical != request {
        return Err(failed(
            "filtered count does not admit projection, limit, or other operation payloads",
        ));
    }
    let path = local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("local Vortex source is required"))?;
    let source = session.prepare_file(&path)?;
    let mut plan = LocalVortexScanPlan::passthrough();
    let planned_predicate =
        attach_predicate_to_scan_plan(&mut plan, predicate, source.dtype(), request.kind)?;
    project_count_where_predicate_columns(&mut plan, &planned_predicate, source.dtype());
    let (policy, physical_policy) = policy.with_physical_policy_for_request(request);
    Ok(PreparedVortexCountWhere {
        request: request.clone(),
        source,
        session: session.clone(),
        path,
        plan,
        policy,
        physical_policy,
    })
}

impl PreparedVortexCountWhere {
    /// Execute the entire count and construct its native I/O certificate.
    /// Source generation validation includes metadata-pruned results. The
    /// ordinary scan's predicate projection, residual evaluation and evidence
    /// collection remain in effect; this is not a general decoded result cache.
    ///
    /// # Errors
    /// Rejects source mutation, resource-policy disagreement, provider failures
    /// and invalid report/certificate inputs. Failed calls produce no count.
    pub fn execute(&self) -> Result<ExecutedVortexCountWhere> {
        let uri = self
            .request
            .source_uri
            .as_ref()
            .ok_or_else(|| failed("prepared source URI is missing"))?;
        let predicate = self
            .request
            .predicate
            .as_ref()
            .ok_or_else(|| failed("prepared predicate is missing"))?;
        let scan = prepared_scan::read_prepared(
            uri,
            &self.source,
            self.request.kind,
            self.policy,
            |_| Ok(self.plan.clone()),
        )?;
        let report = count_where_report(&self.path, &self.request, &scan, predicate)?
            .with_physical_policy(self.physical_policy.clone());
        let count = report
            .rows_selected
            .ok_or_else(|| failed("completed count is missing"))?;
        let runtime = self.session.snapshot();
        let mut native_io_certificate =
            local_primitive_native_io_certificate(&self.request, &report)?;
        let _ = write!(
            native_io_certificate.source_pushdown_report.proof_basis,
            ";resident_source_generation_validation=before_and_after_native_scan_including_metadata_pruned_result;resident_source_opens={};resident_completed_executions={};no_query_answer_cache=true;independent_oracle_not_run",
            runtime.prepared_source_opens, runtime.completed_executions,
        );
        Ok(ExecutedVortexCountWhere {
            count,
            report,
            native_io_certificate,
            runtime,
        })
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "prepared native filtered count: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_prepared_count_tests.rs"]
mod tests;
