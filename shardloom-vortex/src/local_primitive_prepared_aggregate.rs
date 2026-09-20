//! One caller-retained aggregate request/source; every execution owns fresh state.
//! The adapter retains no query-result arrays, aggregate partials or answers.

use super::{
    LocalVortexAggregateScan, Result, ShardLoomError, SimpleAggregateStates,
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionReport,
    VortexLocalPrimitivePhysicalPolicyReport, VortexQueryPrimitiveKind,
    VortexQueryPrimitiveRequest, aggregate_count_workers, aggregate_lowering::AggregateLowering,
    local_primitive_native_io_certificate, local_vortex_path,
    read_lowered_vortex_simple_aggregate_scan, required_simple_aggregate, simple_aggregate_report,
};
use crate::resident_session::{
    PreparedVortexSource, ResidentSessionSnapshot, ResidentVortexSession,
    segment_reuse::SegmentReusePolicy,
};
use std::fmt::Write as _;

// Bound width-dependent validation/template vectors before cloning the request
// or allocating aggregate state. This is a schema ceiling, not an RSS limit.
const MAX_PREPARED_AGGREGATE_MEASURES: usize = 1024;
const MAX_PREPARED_AGGREGATE_SHAPE_ENTRIES: usize = 1024;

#[cfg(feature = "vortex-write")]
#[path = "local_primitive_prepared_aggregate_spill.rs"]
mod spill;

/// A complete aggregate report and its actual native I/O certificate.
pub struct ExecutedVortexAggregate {
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
}

/// Complete typed payload and the same native execution certificate as reports.
pub struct ExecutedOwnedVortexAggregate {
    pub execution: ExecutedVortexAggregate,
    pub result: crate::resident_session::OwnedVortexResultBatch,
    #[cfg(feature = "vortex-write")]
    request: VortexQueryPrimitiveRequest,
    #[cfg(feature = "vortex-write")]
    policy: VortexLocalPrimitiveExecutionPolicy,
}

#[cfg(feature = "vortex-write")]
impl ExecutedOwnedVortexAggregate {
    /// Persist complete owned columns through the existing native/compatibility
    /// sink. This consumes the result and performs no further query execution.
    /// # Errors
    /// Rejects unsupported formats, pressure, existing targets or sink validation
    /// failures. Publication uses the sink's atomic create-if-absent contract.
    pub fn write(
        self,
        path: &std::path::Path,
        format: super::VortexLocalPrimitiveRowExportFormat,
        allow_overwrite: bool,
    ) -> Result<super::VortexLocalPrimitiveRowExportReport> {
        let plan = super::native_sink::NativeSinkPlan::completed(self.result)?;
        let mut report = match format {
            super::VortexLocalPrimitiveRowExportFormat::Vortex => {
                plan.write(&self.request, path, allow_overwrite, self.policy)?
            }
            #[cfg(feature = "universal-format-io")]
            super::VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | super::VortexLocalPrimitiveRowExportFormat::Parquet => {
                super::columnar_compat_sink::prepare_plan(
                    &self.request,
                    plan,
                    format,
                    self.policy,
                    super::columnar_compat_sink::CompatibilityLimits::default(),
                )?
                .ok_or_else(|| failed("completed result exceeds compatibility sink admission"))?
                .write(path, allow_overwrite)?
                .report
            }
            _ => {
                return Err(failed(
                    "owned aggregate sink requires enabled Vortex, Arrow IPC or Parquet output",
                ));
            }
        };
        let execution = self.execution.report;
        report.rows_scanned = execution.rows_scanned;
        report.arrays_read_count = execution.arrays_read_count;
        report.max_chunk_rows = execution.max_chunk_rows;
        report.state_budget = execution.state_budget;
        report.physical_policy = execution.physical_policy;
        report.source_order_limit_requested = execution.source_order_limit_requested;
        report.evidence.upstream_scan_called = execution.upstream_scan_called;
        report.evidence.side_effects.data_read |= execution.data_read;
        report.evidence.side_effects.data_decoded |= execution.data_decoded;
        report.evidence.side_effects.data_materialized |= execution.data_materialized;
        report.evidence.pushdown = super::VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: execution.filter_pushdown_applied,
            projection_pushdown_applied: execution.projection_pushdown_applied,
            source_order_limit_applied: execution.source_order_limit_applied,
        };
        Ok(report)
    }
}

/// Immutable lowering plus one generation-bound file and caller-owned session.
/// SUM/AVG retain the engine's existing ordered floating-point accumulation policy.
pub struct PreparedVortexAggregate {
    request: VortexQueryPrimitiveRequest,
    source: PreparedVortexSource,
    session: ResidentVortexSession,
    lowering: AggregateLowering,
    policy: VortexLocalPrimitiveExecutionPolicy,
    physical_policy: VortexLocalPrimitivePhysicalPolicyReport,
    worker_pool: bool,
    temporary_provider_drivers: bool,
    reuse: Option<SegmentReusePolicy>,
}

/// Preparation never executes a query. Native lowering and source generations
/// are reusable independently of the aggregate's key and measure types.
/// The unretained variant remains available for API compatibility.
pub enum PreparedAggregateDisposition {
    Reusable(PreparedVortexAggregate),
    Unretained(UnretainedVortexAggregate),
}

/// A schema-declined source that can execute only once and is never cached by
/// the public worker. Its existing native aggregate semantics are unchanged.
pub struct UnretainedVortexAggregate(PreparedVortexAggregate);

impl UnretainedVortexAggregate {
    /// # Errors
    /// Returns ordinary native execution, source-generation or certificate errors.
    pub fn execute(self) -> Result<ExecutedVortexAggregate> {
        let mut executed = self.0.execute_native()?;
        executed.native_io_certificate.source_pushdown_report.proof_basis.push_str(
            ";aggregate_preparation_disposition=unretained_source;aggregate_lowering_reused=false;aggregate_state_reused=false;no_query_answer_cache=true;independent_oracle_not_run",
        );
        Ok(executed)
    }
}

fn canonical(request: &VortexQueryPrimitiveRequest) -> Result<()> {
    if request.kind != VortexQueryPrimitiveKind::SimpleAggregate
        || request.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            ) || diagnostic.fallback.attempted
        })
    {
        return Err(failed("only safe simple-aggregate requests are admitted"));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is required"))?;
    let aggregate = required_simple_aggregate(request)?;
    if aggregate.measures.len() > MAX_PREPARED_AGGREGATE_MEASURES {
        return Err(failed("prepared aggregates admit at most 1024 measures"));
    }
    validate_aggregate_shape(aggregate)?;
    if aggregate.spill.is_some() {
        #[cfg(feature = "vortex-write")]
        spill::validate_request(request)?;
        #[cfg(not(feature = "vortex-write"))]
        return Err(failed("explicit spill requires the vortex-write feature"));
    }
    if aggregate.measures.is_empty() || request.source_order_limit == Some(0) {
        return Err(failed(
            "requires at least one measure and a positive optional limit",
        ));
    }
    let mut expected =
        VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), aggregate.clone());
    expected.predicate.clone_from(&request.predicate);
    expected.source_order_limit = request.source_order_limit;
    expected.diagnostics.clone_from(&request.diagnostics);
    if &expected != request {
        return Err(failed(
            "unrelated operation payload or projection is not admitted",
        ));
    }
    // Reuse the execution validator before opening a source. Preparation does
    // not need a second, narrower definition of aggregate semantics.
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    drop(SimpleAggregateStates::new(aggregate, &columns)?);
    Ok(())
}

// Bound the combined flat syntax before cloning or projected_columns()'s
// duplicate search. Check vector lengths before visiting nested argument lists.
fn validate_aggregate_shape(aggregate: &super::VortexSimpleAggregateRequest) -> Result<()> {
    let mut remaining = MAX_PREPARED_AGGREGATE_SHAPE_ENTRIES;
    let mut admit = |count| -> Result<()> {
        remaining = remaining.checked_sub(count).ok_or_else(|| {
            failed("prepared aggregates admit at most 1024 combined grouping, expression argument, ordering and HAVING entries")
        })?;
        Ok(())
    };
    admit(aggregate.group_by.len())?;
    admit(aggregate.group_expressions.len())?;
    admit(aggregate.order_by.len())?;
    admit(aggregate.having.len())?;
    for expression in &aggregate.group_expressions {
        admit(expression.extra_columns.len())?;
    }
    Ok(())
}

fn validate_policy(policy: VortexLocalPrimitiveExecutionPolicy) -> Result<()> {
    if policy.max_parallelism == 0
        || policy.max_parallelism != policy.resource_envelope.max_parallelism
        || policy.resource_envelope.memory_budget_bytes == 0
    {
        return Err(failed(
            "requires a positive memory budget and consistent positive CPU grants",
        ));
    }
    Ok(())
}

/// Prepare with the same request-based CPU ownership used by ordinary aggregates.
/// This creates exactly one session and opens the source once.
/// # Errors
/// Rejects malformed/extra payloads and unsupported spill before opening, then unsupported
/// source/schema/predicate or resource grants, including more than 1024 measures;
/// no external executor is used.
pub fn prepare_aggregate(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<PreparedVortexAggregate> {
    canonical(request)?;
    validate_policy(policy)?;
    let session = aggregate_session(request, policy)?;
    prepare_aggregate_in_session(request, policy, &session)
}

/// Prepare an optional retained execution without evaluating any source rows.
/// Unsupported request payloads return `None` before opening. Every schema and
/// predicate accepted by native lowering retains the same prepared source.
/// File, resource and lowering errors remain errors and never trigger a retry.
/// # Errors
/// Rejects invalid resource grants, unreadable/changed sources and invalid native lowering.
pub fn prepare_aggregate_for_optional_reuse(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<PreparedAggregateDisposition>> {
    if canonical(request).is_err() {
        return Ok(None);
    }
    validate_policy(policy)?;
    let session = aggregate_session(request, policy)?;
    let operation = prepare_candidate_in_session(request, policy, &session)?;
    Ok(Some(PreparedAggregateDisposition::Reusable(operation)))
}

fn aggregate_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<ResidentVortexSession> {
    let (effective, _) = policy.with_physical_policy_for_request(request);
    if external_workers(request) {
        ResidentVortexSession::for_external_cpu_pool(
            effective.resource_envelope.memory_budget_bytes,
            effective.resource_envelope.max_parallelism,
        )
    } else {
        ResidentVortexSession::new(
            effective.resource_envelope.memory_budget_bytes,
            effective.resource_envelope.max_parallelism,
        )
    }
}

fn external_workers(request: &VortexQueryPrimitiveRequest) -> bool {
    #[cfg(feature = "vortex-write")]
    if request
        .simple_aggregate
        .as_ref()
        .is_some_and(|aggregate| aggregate.spill.is_some())
    {
        return super::weighted_count_spill_query::worker_request_admitted(request);
    }
    aggregate_count_workers::request_may_be_admitted(request)
}

/// Prepare in an existing session without opening another runtime. If that
/// session already owns provider drivers, aggregation uses those lanes and does
/// not simultaneously create the dedicated chunk-worker pool.
/// # Errors
/// Rejects a wider supplied session, unsupported native shape or source change.
pub fn prepare_aggregate_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexAggregate> {
    prepare_candidate_in_session(request, policy, session)
}

fn prepare_candidate_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexAggregate> {
    canonical(request)?;
    validate_policy(policy)?;
    let (policy, physical_policy) = policy.with_physical_policy_for_request(request);
    let snapshot = session.snapshot();
    if snapshot.memory.limit_bytes > policy.resource_envelope.memory_budget_bytes {
        return Err(failed(
            "supplied session exceeds the requested memory policy",
        ));
    }
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is required"))?;
    let path = local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("requires one local Vortex file"))?;
    let source = session.prepare_file(path)?;
    let (_, parallelism) = source.resource_limits();
    if parallelism > policy.resource_envelope.max_parallelism {
        return Err(failed("supplied session exceeds the requested CPU policy"));
    }
    let (policy, physical_policy) = cap_session_cpu(policy, physical_policy, parallelism);
    #[cfg(feature = "vortex-write")]
    if required_simple_aggregate(request)?.spill.is_some() {
        spill::validate_schema(request, source.dtype())?;
    }
    let lowering = AggregateLowering::new(request, source.dtype())?;
    // Validate measure aliases/functions without retaining any aggregate state.
    drop(SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )?);
    let worker_pool = snapshot.provider_background_workers == 0
        && external_workers(request)
        && !aggregate_count_workers::restore_provider_drivers(request, source.dtype());
    let temporary_provider_drivers =
        snapshot.provider_background_workers == 0 && parallelism > 1 && !worker_pool;
    let reuse = segment_reuse_policy(&source, &lowering)?;
    source.validate_generation()?;
    Ok(PreparedVortexAggregate {
        request: request.clone(),
        source,
        session: session.clone(),
        lowering,
        policy,
        physical_policy,
        worker_pool,
        temporary_provider_drivers,
        reuse,
    })
}

fn cap_session_cpu(
    mut policy: VortexLocalPrimitiveExecutionPolicy,
    mut physical_policy: VortexLocalPrimitivePhysicalPolicyReport,
    parallelism: usize,
) -> (
    VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitivePhysicalPolicyReport,
) {
    // The retained session's CPU ceiling also limits a wider request policy.
    if parallelism < policy.resource_envelope.max_parallelism {
        policy.max_parallelism = parallelism;
        policy.resource_envelope.max_parallelism = parallelism;
        policy.resource_envelope.scan_concurrency_per_worker = policy
            .resource_envelope
            .scan_concurrency_per_worker
            .min(parallelism);
        physical_policy.selected_max_parallelism = parallelism;
        physical_policy.selected_scan_concurrency_per_worker =
            policy.resource_envelope.scan_concurrency_per_worker;
        physical_policy
            .rejected_alternatives
            .push("aggregate_workers_beyond_retained_session_cpu_grant".into());
    }
    (policy, physical_policy)
}

fn segment_reuse_policy(
    source: &PreparedVortexSource,
    lowering: &AggregateLowering,
) -> Result<Option<SegmentReusePolicy>> {
    if !source.has_segment_reuse_field_root()
        || !matches!(lowering.pushdown, Some(super::PredicateExpr::And(_)))
    {
        return Ok(None);
    }
    let mut projected = lowering.rewrite.aggregate.projected_columns();
    if let Some(predicate) = &lowering.residual {
        super::append_predicate_columns(predicate, &mut projected);
    }
    lowering
        .pushdown
        .as_ref()
        .map(|predicate| source.segment_reuse_policy(predicate, &projected))
        .transpose()
        .map(Option::flatten)
}

impl PreparedVortexAggregate {
    /// Start a fresh cancellation scope for this prepared spill handle while
    /// retaining its source and policy. Call after a cancelled execution has
    /// returned, then use the returned policy's `cancel()` to stop the next call.
    /// Old policy clones keep their old cancellation scope and cannot cancel a
    /// renewed execution. Exclusive access prevents renewing an active call.
    /// No source is reopened and no query state or answer is retained.
    ///
    /// # Errors
    /// Rejects handles without an explicit spill policy or invalid policy bounds.
    #[cfg(feature = "vortex-write")]
    pub fn renew_spill_cancellation(&mut self) -> Result<crate::VortexAggregateSpillPolicy> {
        let previous = required_simple_aggregate(&self.request)?
            .spill
            .as_ref()
            .ok_or_else(|| failed("cancellation renewal requires a prepared spill policy"))?;
        let renewed = crate::VortexAggregateSpillPolicy::new(
            previous.workspace.clone(),
            previous.quota_bytes,
            previous.memory_bytes,
        )?;
        self.request
            .simple_aggregate
            .as_mut()
            .ok_or_else(|| failed("prepared aggregate request is absent"))?
            .spill = Some(renewed.clone());
        self.lowering.rewrite.aggregate.spill = Some(renewed.clone());
        Ok(renewed)
    }

    /// Read-only cumulative session counters; unrelated handles can also advance them.
    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        self.session.snapshot()
    }

    fn read(&self) -> Result<LocalVortexAggregateScan> {
        self.read_with_output(None)
    }

    fn read_with_output(
        &self,
        mut output: Option<&mut super::aggregate_owned::OwnedAggregateFinalizer>,
    ) -> Result<LocalVortexAggregateScan> {
        #[cfg(feature = "vortex-write")]
        if required_simple_aggregate(&self.request)?.spill.is_some() {
            if output.is_some() {
                return Err(failed("owned spill output is not yet admitted"));
            }
            return self.read_spill();
        }
        let uri = self
            .request
            .source_uri
            .as_ref()
            .ok_or_else(|| failed("prepared source URI is absent"))?;
        let memory = self.worker_pool.then(|| self.session.memory());
        if let Some(policy) = self.reuse {
            let (mut scan, evidence) = self
                .source
                .with_native_execution_cached_retry_with_drivers(
                    policy,
                    self.temporary_provider_drivers,
                    |file, session, runtime, attempt| {
                        let mut retry = |error: &vortex::error::VortexError| {
                            attempt.request_uncached_retry(error)
                        };
                        read_lowered_vortex_simple_aggregate_scan(
                            uri,
                            &self.request,
                            self.policy,
                            file,
                            session,
                            runtime,
                            memory,
                            Some(&mut retry),
                            &self.lowering,
                            std::time::Instant::now(),
                            output.as_deref_mut(),
                        )
                    },
                )?;
            scan.restored_provider_background_workers = scan
                .restored_provider_background_workers
                .max(evidence.provider_background_workers);
            scan.annotate_segment_reuse(evidence)?;
            return Ok(scan);
        }
        if self.temporary_provider_drivers {
            let (mut scan, drivers) =
                self.source
                    .with_native_execution_temporary_drivers(|file, session, runtime| {
                        read_lowered_vortex_simple_aggregate_scan(
                            uri,
                            &self.request,
                            self.policy,
                            file,
                            session,
                            runtime,
                            None,
                            None,
                            &self.lowering,
                            std::time::Instant::now(),
                            output.as_deref_mut(),
                        )
                    })?;
            let mut summary: serde_json::Value = serde_json::from_str(&scan.result_summary)
                .map_err(|error| failed(&error.to_string()))?;
            summary["aggregate_provider_background_workers"] = drivers.into();
            summary["aggregate_provider_cpu_scope"] = "same_prepared_source;worker_schema_not_admitted;temporary_provider_drivers;no_concurrent_aggregate_worker_pool".into();
            scan.result_summary = summary.to_string();
            scan.restored_provider_background_workers =
                scan.restored_provider_background_workers.max(drivers);
            return Ok(scan);
        }
        self.source.with_native_execution(|file, session, runtime| {
            read_lowered_vortex_simple_aggregate_scan(
                uri,
                &self.request,
                self.policy,
                file,
                session,
                runtime,
                memory,
                None,
                &self.lowering,
                std::time::Instant::now(),
                output,
            )
        })
    }

    /// Execute fresh complete aggregate state, then construct the ordinary report
    /// and native I/O certificate. Generation validation brackets every call,
    /// including metadata-pruned results and same-source pressure replay.
    /// # Errors
    /// Rejects source changes, pressure, provider failures or uncertified results.
    pub fn execute(&self) -> Result<ExecutedVortexAggregate> {
        let mut executed = self.execute_native()?;
        let _ = write!(
            executed
                .native_io_certificate
                .source_pushdown_report
                .proof_basis,
            ";resident_source_generation_validation=before_and_after_every_execution_including_pruned_result;resident_source_opens={};resident_completed_executions={};aggregate_lowering_reused={};aggregate_state_reused=false;no_query_answer_cache=true;independent_oracle_not_run",
            executed.runtime.prepared_source_opens,
            executed.runtime.completed_executions,
            required_simple_aggregate(&self.request)?.spill.is_none()
        );
        Ok(executed)
    }

    fn execute_native(&self) -> Result<ExecutedVortexAggregate> {
        let scan = self.read()?;
        self.certify(&scan)
    }

    /// Execute fresh integer grouped COUNT(*)/COUNT DISTINCT or UTF8 COUNT(*).
    /// Preserves original integer width or exact UTF8 bytes, count-descending/key-ascending
    /// ordering and offset/limit; no result rows are rendered before a sink.
    /// # Errors
    /// Rejects unsupported or nullable shapes before execution, source changes,
    /// pressure, and offset plus limit above 65536. No query is retried to render rows.
    pub fn execute_owned(&self) -> Result<ExecutedOwnedVortexAggregate> {
        let mut output = super::aggregate_owned::OwnedAggregateFinalizer::new(
            &self.request,
            self.source.dtype(),
            &self.session,
        )?;
        let scan = self.read_with_output(Some(&mut output))?;
        let execution = self.certify(&scan)?;
        let (array, ownership) = output.into_array()?;
        let result = self.session.own_completed_array(array, ownership)?;
        Ok(ExecutedOwnedVortexAggregate {
            execution,
            result,
            #[cfg(feature = "vortex-write")]
            request: self.request.clone(),
            #[cfg(feature = "vortex-write")]
            policy: self.policy,
        })
    }

    fn certify(&self, scan: &LocalVortexAggregateScan) -> Result<ExecutedVortexAggregate> {
        let report = simple_aggregate_report(&self.request, scan)?
            .with_physical_policy(self.physical_policy.clone());
        let mut runtime = self.session.snapshot();
        runtime.provider_background_workers = runtime
            .provider_background_workers
            .max(scan.restored_provider_background_workers);
        let native_io_certificate = local_primitive_native_io_certificate(&self.request, &report)?;
        if !native_io_certificate.is_certified() || report.has_errors() {
            return Err(failed(
                "aggregate execution did not produce a certified native report",
            ));
        }
        Ok(ExecutedVortexAggregate {
            report,
            native_io_certificate,
            runtime,
        })
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "prepared native aggregate: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_prepared_aggregate_tests.rs"]
mod tests;

#[cfg(all(test, feature = "vortex-write", unix))]
#[path = "local_primitive_footer_aggregate_native_tests.rs"]
mod footer_native_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_aggregate_owned_tests.rs"]
mod owned_tests;
