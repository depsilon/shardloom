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
use shardloom_exec::compute_pool::CancellationToken;
use std::fmt::Write as _;

/// A complete native filtered count and the evidence produced by this execution.
pub struct ExecutedVortexCountWhere {
    pub count: u64,
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
}

/// Complete filtered-count value, with its native scalar and execution evidence.
pub struct ExecutedOwnedVortexCountWhere {
    pub execution: ExecutedVortexCountWhere,
    pub result: crate::resident_session::OwnedVortexResultBatch,
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
    prepare_count_where_in_session_controlled(
        request,
        policy,
        session,
        &CancellationToken::default(),
    )
}

/// Prepare a filtered count in an existing native session under the caller's
/// cancellation and deadline token. No second session is created.
///
/// # Errors
/// Rejects cancellation before opening a source, invalid requests, resource
/// admission failures, unsupported predicates, and nonlocal sources.
pub fn prepare_count_where_in_session_controlled(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexCountWhere> {
    prepare_count_where(request, policy, session, None, cancellation)
}

/// Bind the filtered count to the same source used for native strategy selection.
/// # Errors
/// Rejects changed/mismatched sources, resources and unsupported predicates.
pub fn prepare_count_where_from_source(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
) -> Result<PreparedVortexCountWhere> {
    prepare_count_where_from_source_controlled(
        request,
        policy,
        source,
        &CancellationToken::default(),
    )
}

/// Bind to a previously admitted source while observing the caller's operation
/// token. The retained source and its resource owner are preserved.
///
/// # Errors
/// Rejects cancellation, changed/mismatched sources, resources and unsupported
/// predicates without opening another source.
pub fn prepare_count_where_from_source_controlled(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexCountWhere> {
    cancellation.check()?;
    let session = super::prepared_dispatch::source_session(&source, request, Some(policy))?;
    prepare_count_where(request, policy, &session, Some(source), cancellation)
}

fn prepare_count_where(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    prepared: Option<PreparedVortexSource>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexCountWhere> {
    cancellation.check()?;
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
    let source = if let Some(source) = prepared {
        source
    } else {
        session.prepare_file_with_cancellation(&path, cancellation)?
    };
    cancellation.check()?;
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
    /// Execute the existing predicate count and retain its scalar natively.
    /// Result construction shares the source's memory owner and preserves exact
    /// unsigned values; no JSON or Python row conversion participates.
    /// # Errors
    /// Rejects cancellation, changed sources, pressure or native provider errors.
    pub fn execute_native_result(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ExecutedOwnedVortexCountWhere> {
        let mut execution = self.execute_with_cancellation(cancellation)?;
        let result = self
            .session
            .own_count_result(execution.count, cancellation)?;
        self.source.validate_generation()?;
        cancellation.check()?;
        let provider_workers = execution.runtime.provider_background_workers;
        execution.runtime = self.session.snapshot();
        execution.runtime.provider_background_workers = provider_workers;
        Ok(ExecutedOwnedVortexCountWhere { execution, result })
    }

    /// Execute the same native count and serialize its exact scalar result.
    /// # Errors
    /// Preserves execution errors and rejects insufficient output capacity.
    pub fn collect_jsonl(
        &self,
    ) -> Result<(ExecutedVortexCountWhere, super::collect::CollectedCountRows)> {
        self.collect_jsonl_controlled(&CancellationToken::default())
    }

    /// Execute and serialize the exact count with one caller-owned cancellation
    /// token. Rendering remains charged to the prepared source's session.
    ///
    /// # Errors
    /// Returns execution, cancellation/deadline, source generation, report or
    /// bounded serialization errors without changing this prepared plan.
    pub fn collect_jsonl_controlled(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(ExecutedVortexCountWhere, super::collect::CollectedCountRows)> {
        cancellation.check()?;
        let started = std::time::Instant::now();
        let mut execution = self.execute_with_cancellation(cancellation)?;
        let execution_time = started.elapsed();
        cancellation.check()?;
        let mut rows = super::collect::render_count_rows(execution.count, &self.session)?;
        rows.timing.execution = execution_time;
        cancellation.check()?;
        let provider_workers = execution.runtime.provider_background_workers;
        execution.runtime = self.session.snapshot();
        execution.runtime.provider_background_workers = provider_workers;
        Ok((execution, rows))
    }

    /// Validate file admission against the generation held by the native scan.
    /// This does not evaluate the predicate or open another provider.
    ///
    /// # Errors
    /// Returns the source's metadata mismatch or generation validation error.
    pub fn validate_file_metadata(&self, expected: &std::fs::Metadata) -> Result<()> {
        self.source.validate_file_metadata(expected)
    }

    /// Logical rows in the retained source generation; no scan is performed.
    /// This supports admission before execution and does not validate a later
    /// pathname mutation. Every execution still performs generation validation.
    #[must_use]
    pub fn source_row_count(&self) -> u64 {
        self.source.file().row_count()
    }

    /// Execute the entire count and construct its native I/O certificate.
    /// Source generation validation includes metadata-pruned results. The
    /// ordinary scan's predicate projection, residual evaluation and evidence
    /// collection remain in effect; this is not a general decoded result cache.
    ///
    /// # Errors
    /// Rejects source mutation, resource-policy disagreement, provider failures
    /// and invalid report/certificate inputs. Failed calls produce no count.
    pub fn execute(&self) -> Result<ExecutedVortexCountWhere> {
        self.execute_with_cancellation(&CancellationToken::default())
    }

    /// Execute the retained native scan and exact count under a caller-owned
    /// cancellation/deadline token. The source, plan, and resource owner are reused.
    ///
    /// # Errors
    /// Rejects cancellation, source mutation, resource-policy disagreement,
    /// provider failures and invalid report/certificate inputs.
    pub fn execute_with_cancellation(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ExecutedVortexCountWhere> {
        cancellation.check()?;
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
        let (scan, provider_workers) = prepared_scan::read_prepared_controlled(
            uri,
            &self.source,
            self.request.kind,
            self.policy,
            cancellation,
            |_| Ok(self.plan.clone()),
        )?;
        cancellation.check()?;
        let report = count_where_report(&self.path, &self.request, &scan, predicate)?
            .with_physical_policy(self.physical_policy.clone());
        cancellation.check()?;
        let count = report
            .rows_selected
            .ok_or_else(|| failed("completed count is missing"))?;
        let mut runtime = self.session.snapshot();
        runtime.provider_background_workers = provider_workers;
        let mut native_io_certificate =
            local_primitive_native_io_certificate(&self.request, &report)?;
        cancellation.check()?;
        let _ = write!(
            native_io_certificate.source_pushdown_report.proof_basis,
            ";resident_source_generation_validation=before_and_after_native_scan_including_metadata_pruned_result;resident_source_opens={};resident_completed_executions={};provider_background_workers={};provider_cpu_scope=current_grant_temporary_drivers_joined_before_return_not_measured_utilization;no_query_answer_cache=true;independent_oracle_not_run",
            runtime.prepared_source_opens, runtime.completed_executions, provider_workers,
        );
        let execution = ExecutedVortexCountWhere {
            count,
            report,
            native_io_certificate,
            runtime,
        };
        cancellation.check()?;
        Ok(execution)
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
