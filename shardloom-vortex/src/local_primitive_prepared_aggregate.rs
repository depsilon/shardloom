//! One caller-retained aggregate request/source; every execution owns fresh state.
//! The adapter retains no query-result arrays, aggregate partials or answers.

use super::{
    LocalVortexAggregateScan, Result, ShardLoomError, SimpleAggregateFunction,
    SimpleAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexLocalPrimitivePhysicalPolicyReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, aggregate_count_workers,
    aggregate_lowering::AggregateLowering, local_primitive_native_io_certificate,
    local_vortex_path, read_lowered_vortex_simple_aggregate_scan, required_simple_aggregate,
    simple_aggregate_report,
};
use crate::resident_session::{
    PreparedVortexSource, ResidentSessionSnapshot, ResidentVortexSession,
    segment_reuse::SegmentReusePolicy,
};
use std::fmt::Write as _;
use vortex::array::dtype::{DType, PType};

/// A complete aggregate report and its actual native I/O certificate.
pub struct ExecutedVortexAggregate {
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
}

/// Immutable lowering plus one generation-bound file and caller-owned session.
/// SUM retains the engine's existing ordered floating-point accumulation policy.
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

/// Preparation never executes a query. The retained case admits exactly the
/// integer aggregate API; the other case transfers the opened source to one
/// explicit ordinary execution without probing and reopening the same file.
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
    if aggregate.spill.is_some() {
        return Err(failed(
            "explicit spill is not admitted by this retained API",
        ));
    }
    if aggregate.measures.is_empty()
        || aggregate.measures.len() > 64
        || aggregate.group_by.len() > 2
        || !aggregate.group_expressions.is_empty()
        || request.source_order_limit == Some(0)
    {
        return Err(failed(
            "requires 1..=64 identity measures, at most two identity keys and a positive optional limit",
        ));
    }
    for measure in &aggregate.measures {
        if !matches!(
            SimpleAggregateFunction::parse(&measure.function)?,
            SimpleAggregateFunction::Count
                | SimpleAggregateFunction::CountDistinct
                | SimpleAggregateFunction::Sum
        ) || measure.value_transform.is_some()
            || measure.argument_offset.is_some()
        {
            return Err(failed(
                "only existing identity COUNT, COUNT DISTINCT and SUM measures are admitted",
            ));
        }
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
    Ok(())
}

fn integer_fields(
    request: &VortexQueryPrimitiveRequest,
    source: &PreparedVortexSource,
) -> Result<()> {
    let aggregate = required_simple_aggregate(request)?;
    let fields = source
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| failed("requires a struct source"))?;
    for column in aggregate.projected_columns() {
        if !matches!(
            fields.field(column.as_str()),
            Some(DType::Primitive(
                PType::I8
                    | PType::I16
                    | PType::I32
                    | PType::I64
                    | PType::U8
                    | PType::U16
                    | PType::U32
                    | PType::U64,
                _
            ))
        ) {
            return Err(failed(
                "group and measure fields must have existing integer types",
            ));
        }
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
/// Rejects malformed/extra payloads and spill before opening, then unsupported
/// source/schema/predicate or resource grants; no external executor is used.
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
/// Unsupported request shapes return `None` before opening. A schema-only
/// rejection hands the same source to an explicit one-shot ordinary execution.
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
    let retained =
        integer_fields(request, &operation.source).is_ok() && operation.lowering.residual.is_none();
    Ok(Some(if retained {
        PreparedAggregateDisposition::Reusable(operation)
    } else {
        PreparedAggregateDisposition::Unretained(UnretainedVortexAggregate(operation))
    }))
}

fn aggregate_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<ResidentVortexSession> {
    let (effective, _) = policy.with_physical_policy_for_request(request);
    if aggregate_count_workers::request_may_be_admitted(request) {
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
    let operation = prepare_candidate_in_session(request, policy, session)?;
    integer_fields(request, &operation.source)?;
    if operation.lowering.residual.is_some() {
        return Err(failed(
            "residual predicates are not admitted by this retained API",
        ));
    }
    Ok(operation)
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
    let lowering = AggregateLowering::new(request, source.dtype())?;
    // Validate measure aliases/functions without retaining any aggregate state.
    drop(SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )?);
    let worker_pool = snapshot.provider_background_workers == 0
        && aggregate_count_workers::request_may_be_admitted(request)
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
    /// Read-only cumulative session counters; unrelated handles can also advance them.
    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        self.session.snapshot()
    }

    fn read(&self) -> Result<LocalVortexAggregateScan> {
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
            ";resident_source_generation_validation=before_and_after_every_execution_including_pruned_result;resident_source_opens={};resident_completed_executions={};aggregate_lowering_reused=true;aggregate_state_reused=false;no_query_answer_cache=true;independent_oracle_not_run",
            executed.runtime.prepared_source_opens,
            executed.runtime.completed_executions
        );
        Ok(executed)
    }

    fn execute_native(&self) -> Result<ExecutedVortexAggregate> {
        let scan = self.read()?;
        let report = simple_aggregate_report(&self.request, &scan)?
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
