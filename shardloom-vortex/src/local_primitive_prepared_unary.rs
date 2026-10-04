//! Retained unary plans with fresh, reserved state and synchronous native results.

use super::{
    LocalVortexScan, LocalVortexScanPlan, MaterializedPredicateEvaluator, Result, ShardLoomError,
    StatValue, VortexDuplicateKeepPolicy, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexLocalPrimitivePhysicalPolicyReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, vortex_error,
};
use crate::resident_session::{
    NativeExecutionContext, OwnedVortexResultBatch, PreparedVortexSource, ResidentSessionSnapshot,
    ResidentVortexSession,
};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
#[cfg(test)]
use vortex::array::VortexSessionExecute as _;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

use super::result_batch::Value;
use memory::ReservedVec;
use values::NativeBatch;

#[path = "local_primitive_unary_explode.rs"]
mod explode;
#[path = "local_primitive_unary_expression.rs"]
mod expression;
#[path = "local_primitive_unary_melt.rs"]
mod melt;
#[path = "local_primitive_unary_memory.rs"]
mod memory;
#[path = "local_primitive_unary_pivot.rs"]
mod pivot;
#[path = "local_primitive_unary_report.rs"]
mod report;
#[path = "local_primitive_unary_rolling.rs"]
mod rolling;
#[path = "local_primitive_unary_sample.rs"]
mod sample;
#[path = "local_primitive_unary_schema.rs"]
mod schema;
#[path = "local_primitive_unary_select.rs"]
mod select;
#[path = "local_primitive_unary_values.rs"]
pub(super) mod values;

const BATCH_ROWS: usize = 2048;

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "prepared native unary: {reason}; no fallback execution was attempted"
    ))
}

/// Report and certificate from the same execution that produced the payload.
pub struct ExecutedVortexUnary {
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
    _evidence: EvidenceOwners,
}

/// Complete small result; file writers have an independent streaming boundary.
pub struct ExecutedOwnedVortexUnary {
    pub execution: ExecutedVortexUnary,
    pub result: OwnedVortexResultBatch,
}

/// Complete bounded rows for a public text boundary, from the certified execution.
pub struct CollectedVortexUnary {
    pub execution: ExecutedVortexUnary,
    pub result_jsonl: shardloom_exec::live_memory::Budgeted<String>,
}

/// Immutable source identity, request and lowering. No result or operator state is cached.
pub struct PreparedVortexUnary {
    policy: VortexLocalPrimitiveExecutionPolicy,
    physical_policy: VortexLocalPrimitivePhysicalPolicyReport,
    source: PreparedVortexSource,
    session: ResidentVortexSession,
    plan: LocalVortexScanPlan,
    bound: BoundUnary,
}

/// Source-independent schema and operation metadata. Every execution owns fresh
/// state; direct file scans and composed native relations share the same kernels.
pub(super) struct BoundUnary {
    request: VortexQueryPrimitiveRequest,
    columns: Vec<String>,
    output_columns: Vec<String>,
    output_indices: Vec<usize>,
    key_indices: Vec<usize>,
    weight_index: Option<usize>,
    fields: Vec<(String, DType)>,
    predicate: Option<MaterializedPredicateEvaluator>,
    expression: Option<expression::Plan>,
    melt: Option<melt::Plan>,
    explode: Option<explode::Plan>,
    pivot: Option<pivot::Plan>,
    _metadata: MemoryLease,
}

pub(super) fn supports(kind: VortexQueryPrimitiveKind) -> bool {
    matches!(
        kind,
        VortexQueryPrimitiveKind::DistinctRows
            | VortexQueryPrimitiveKind::DropDuplicateRows
            | VortexQueryPrimitiveKind::DuplicateMaskRows
            | VortexQueryPrimitiveKind::TailRows
            | VortexQueryPrimitiveKind::SampleRows
            | VortexQueryPrimitiveKind::RollingWindowRows
            | VortexQueryPrimitiveKind::ExpressionProjectRows
            | VortexQueryPrimitiveKind::MeltRows
            | VortexQueryPrimitiveKind::ExplodeRows
            | VortexQueryPrimitiveKind::PivotRows
    )
}

fn canonical(request: &VortexQueryPrimitiveRequest, file_source: bool) -> Result<()> {
    if !supports(request.kind)
        || request.source_order_limit == Some(0)
        || request.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            ) || d.fallback.attempted
        })
    {
        return Err(failed(
            "requires an admitted unary request and a positive optional limit",
        ));
    }
    if file_source && request.source_uri.is_none() {
        return Err(failed("source URI is required"));
    }
    if !file_source && request.source_uri.is_some() {
        return Err(failed(
            "a composed operation takes its source from the preceding relation",
        ));
    }
    let has_unrelated = (request.expression_projection.is_some()
        && request.kind != VortexQueryPrimitiveKind::ExpressionProjectRows)
        || (request.melt_projection.is_some()
            && request.kind != VortexQueryPrimitiveKind::MeltRows)
        || (request.explode_projection.is_some()
            && request.kind != VortexQueryPrimitiveKind::ExplodeRows)
        || (request.pivot_projection.is_some()
            && request.kind != VortexQueryPrimitiveKind::PivotRows)
        || (request.rolling_window.is_some()
            && request.kind != VortexQueryPrimitiveKind::RollingWindowRows)
        || request.simple_aggregate.is_some()
        || request.sort_rows.is_some()
        || request.structured_projection.is_some();
    let ignores_predicate = matches!(
        request.kind,
        VortexQueryPrimitiveKind::TailRows | VortexQueryPrimitiveKind::DuplicateMaskRows
    );
    let duplicate_policy = matches!(
        request.kind,
        VortexQueryPrimitiveKind::DropDuplicateRows | VortexQueryPrimitiveKind::DuplicateMaskRows
    );
    let sample_payload = request.sample_seed.is_some()
        || request.sample_fraction.is_some()
        || request.sample_with_replacement
        || request.sample_weight_column.is_some();
    if has_unrelated
        || (ignores_predicate && request.predicate.is_some())
        || (!duplicate_policy && request.duplicate_keep != VortexDuplicateKeepPolicy::First)
        || (request.kind != VortexQueryPrimitiveKind::DropDuplicateRows
            && request.deduplicate_key_projection.is_some())
        || (request.kind != VortexQueryPrimitiveKind::SampleRows && sample_payload)
    {
        return Err(failed("unrelated operation payload is not admitted"));
    }
    if request.kind == VortexQueryPrimitiveKind::SampleRows {
        super::sample_target_count(request, 0)?;
    }
    if request.kind == VortexQueryPrimitiveKind::RollingWindowRows {
        super::required_rolling_window(request)?;
    }
    if request.kind == VortexQueryPrimitiveKind::MeltRows {
        super::required_melt_projection(request)?;
    }
    if request.kind == VortexQueryPrimitiveKind::ExplodeRows {
        super::required_explode_projection(request)?;
    }
    if request.kind == VortexQueryPrimitiveKind::PivotRows {
        super::normalized_pivot_aggregate(super::required_pivot_projection(request)?)?;
    }
    if request.kind == VortexQueryPrimitiveKind::ExpressionProjectRows
        && request
            .expression_projection
            .as_ref()
            .is_none_or(super::VortexExpressionProjectionRequest::is_empty)
    {
        return Err(failed(
            "expression projection requires at least one rewrite",
        ));
    }
    if request.kind == VortexQueryPrimitiveKind::TailRows && request.source_order_limit.is_none() {
        return Err(failed("tail requires a positive row count"));
    }
    Ok(())
}

fn validate_policy(policy: VortexLocalPrimitiveExecutionPolicy) -> Result<()> {
    if policy.max_parallelism == 0
        || policy.max_parallelism != policy.resource_envelope.max_parallelism
        || policy.resource_envelope.memory_budget_bytes == 0
    {
        return Err(failed("requires consistent positive CPU and memory grants"));
    }
    Ok(())
}

/// Bind a local source once without executing its rows.
/// # Errors
/// Rejects invalid requests, policies, source identities and unsupported schemas.
pub fn prepare_unary(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<PreparedVortexUnary> {
    canonical(request, true)?;
    validate_policy(policy)?;
    let (effective, _) = policy.with_physical_policy_for_request(request);
    let session = ResidentVortexSession::new(
        effective.resource_envelope.memory_budget_bytes,
        effective.resource_envelope.max_parallelism,
    )?;
    prepare_unary_in_session(request, policy, &session)
}

/// Reuse the supplied session and its single resource grant.
/// # Errors
/// Rejects a wider session, malformed requests and invalid native lowering.
pub fn prepare_unary_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexUnary> {
    prepare_candidate(request, policy, session, false)?
        .ok_or_else(|| failed("retained unary schema is not admitted"))
}

/// Choose retained native execution from the source schema before scanning.
/// A `None` disposition preserves a separately admitted native nested provider;
/// execution, resource and source-generation failures are never retried elsewhere.
/// # Errors
/// Rejects malformed requests, policy failures, missing sources and invalid schemas.
pub fn prepare_unary_for_optional_reuse(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<PreparedVortexUnary>> {
    // Structured projection has its own native provider and result schema.
    if request.structured_projection.is_some() {
        return Ok(None);
    }
    canonical(request, true)?;
    validate_policy(policy)?;
    let (effective, _) = policy.with_physical_policy_for_request(request);
    let session = ResidentVortexSession::new(
        effective.resource_envelope.memory_budget_bytes,
        effective.max_parallelism,
    )?;
    prepare_candidate(request, policy, &session, true)
}

fn prepare_candidate(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    optional: bool,
) -> Result<Option<PreparedVortexUnary>> {
    canonical(request, true)?;
    validate_policy(policy)?;
    let (policy, physical_policy) = policy.with_physical_policy_for_request(request);
    if session.snapshot().memory.limit_bytes > policy.resource_envelope.memory_budget_bytes
        || session.parallelism() > policy.max_parallelism
    {
        return Err(failed(
            "supplied session exceeds the requested resource policy",
        ));
    }
    let metadata = session.memory().reserve(memory::request_bytes(request)?)?;
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is required"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("requires one local Vortex file"))?;
    let source = session.prepare_file(path)?;
    if optional {
        let plan = super::row_export_scan_plan(request, source.dtype())?;
        if !schema::retained_source_admitted(request, source.dtype(), &plan)? {
            return Ok(None);
        }
    }
    bind_unary(request, policy, physical_policy, source, session, metadata).map(Some)
}

fn bind_unary(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    physical_policy: VortexLocalPrimitivePhysicalPolicyReport,
    source: PreparedVortexSource,
    session: &ResidentVortexSession,
    metadata: MemoryLease,
) -> Result<PreparedVortexUnary> {
    let plan = super::row_export_scan_plan(request, source.dtype())?;
    let bound = BoundUnary::bind(request, source.dtype(), &plan, session.memory(), metadata)?;
    Ok(PreparedVortexUnary {
        policy,
        physical_policy,
        source,
        session: session.clone(),
        plan,
        bound,
    })
}

impl BoundUnary {
    pub(super) fn for_relation(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        memory: &shardloom_exec::live_memory::LiveMemoryPool,
    ) -> Result<Self> {
        canonical(request, false)?;
        if request.kind == VortexQueryPrimitiveKind::PivotRows {
            return Err(failed(
                "composed pivot requires execution-time schema binding",
            ));
        }
        let metadata = memory.reserve(memory::request_bytes(request)?)?;
        let mut plan = super::row_export_scan_plan(request, dtype)?;
        if !schema::retained_source_admitted(request, dtype, &plan)? {
            return Err(failed(
                "composed unary state does not admit its selected input types",
            ));
        }
        // The preceding relation already executed. Any predicate attached to
        // this operation is evaluated here, including scan-pushable predicates.
        plan.residual_predicate.clone_from(&request.predicate);
        Self::bind(request, dtype, &plan, memory, metadata)
    }

    pub(super) fn fields(&self) -> &[(String, DType)] {
        &self.fields
    }

    /// A conservative metadata bound, never a row count obtained by replay.
    pub(super) fn upper_output_rows(&self, input: Option<u64>) -> Option<u64> {
        if self.request.kind == VortexQueryPrimitiveKind::ExplodeRows {
            return self.request.source_order_limit.map(|limit| limit as u64);
        }
        if self.request.kind == VortexQueryPrimitiveKind::SampleRows {
            if self.request.sample_fraction.is_none() {
                return self.request.source_order_limit.map(|n| {
                    if self.request.sample_with_replacement {
                        n as u64
                    } else {
                        input.map_or(n as u64, |rows| rows.min(n as u64))
                    }
                });
            }
            return input.and_then(|rows| {
                super::sample_target_count(&self.request, usize::try_from(rows).ok()?)
                    .ok()
                    .map(|n| n as u64)
            });
        }
        let expanded = if self.request.kind == VortexQueryPrimitiveKind::MeltRows {
            input.and_then(|rows| {
                rows.checked_mul(self.request.melt_projection.as_ref()?.value_columns.len() as u64)
            })
        } else {
            input
        };
        match self.request.source_order_limit {
            Some(limit) => Some(expanded.map_or(limit as u64, |rows| rows.min(limit as u64))),
            None => expanded,
        }
    }

    /// Drive the existing operation states inside a caller's admitted execution.
    pub(super) fn consume_relation(
        &self,
        context: &NativeExecutionContext<'_>,
        input_rows: Option<u64>,
        batch_rows: usize,
        produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<report::StateUsage> {
        let rows = super::completed_result::CompletedRows::streaming_native(
            self.fields.clone(),
            context.memory(),
            batch_rows,
            context.cancellation().clone(),
            consume,
        )?;
        let mut output = UnaryOutput {
            columns: &self.output_columns,
            rows: 0,
            payload: Some(rows),
        };
        let mut state = select::State::new(self, context, input_rows, false)?;
        let mut stopped = false;
        produce(&mut |array| {
            context.check_cancelled()?;
            if !stopped {
                let mut batch = NativeBatch::new(&array, &self.columns, context)?;
                stopped = state.consume(self, &mut batch, array.len(), context, &mut output)?;
            }
            Ok(())
        })?;
        let usage = state.usage();
        state.finish(self, context, &mut output)?;
        output.finish(context)?;
        Ok(usage)
    }

    fn bind(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        plan: &LocalVortexScanPlan,
        memory: &shardloom_exec::live_memory::LiveMemoryPool,
        mut metadata: MemoryLease,
    ) -> Result<Self> {
        let width = dtype
            .as_struct_fields_opt()
            .map_or(1, |fields| fields.names().len());
        let schema_bytes = u64::try_from(width)
            .map_err(vortex_error)?
            .checked_mul(4096)
            .and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| failed("schema size overflow"))?;
        metadata.resize(
            metadata
                .bytes()
                .checked_add(schema_bytes)
                .ok_or_else(|| failed("metadata size overflow"))?,
        )?;
        let schema::Binding {
            columns,
            output_columns,
            output_indices,
            key_indices,
            weight_index,
            fields,
            expression,
            melt,
            explode,
            pivot,
        } = schema::bind(request, dtype, plan, memory)?;
        // Validate declared types even for reports and empty sources.
        if request.kind == VortexQueryPrimitiveKind::ExplodeRows {
            drop(super::completed_result::CompletedRows::new_native(
                fields.clone(),
                memory,
            )?);
            for (_, dtype) in &fields {
                let bytes = super::native_payload::metadata_bytes(dtype)?;
                metadata.resize(
                    metadata
                        .bytes()
                        .checked_add(bytes)
                        .ok_or_else(|| failed("nested schema metadata overflow"))?,
                )?;
            }
        } else {
            drop(super::completed_result::CompletedRows::new(
                fields.clone(),
                memory,
            )?);
        }
        let predicate = plan
            .residual_predicate
            .as_ref()
            .map(|predicate| MaterializedPredicateEvaluator::compile(predicate, &columns))
            .transpose()?;
        Ok(Self {
            request: request.clone(),
            columns,
            output_columns,
            output_indices,
            key_indices,
            weight_index,
            fields,
            predicate,
            expression,
            melt,
            explode,
            pivot,
            _metadata: metadata,
        })
    }
}

impl PreparedVortexUnary {
    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        self.session.snapshot()
    }

    /// Run fresh state without retaining output rows.
    /// # Errors
    /// Rejects changed sources, resource pressure, cancellation and operator errors.
    pub fn execute(&self) -> Result<ExecutedVortexUnary> {
        if self.bound.pivot.is_some() {
            return self.execute_pivot();
        }
        let mut output = UnaryOutput::discard(&self.bound.output_columns);
        self.execute_output(&CancellationToken::default(), &mut output)
    }

    /// Deliver complete typed batches, including one typed batch for empty output.
    /// # Errors
    /// Returns consumer errors, cancellation, pressure and terminal source invalidation.
    /// Delivered batches remain provisional until the whole call succeeds.
    pub fn for_each_batch(
        &self,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexUnary> {
        let mut executed =
            self.source
                .with_native_execution_controlled(cancellation, |file, context| {
                    if self.bound.pivot.is_some() {
                        let completed = self.complete_pivot(file, context)?;
                        completed.emit(&self.bound, context, BATCH_ROWS, &mut |array| {
                            consume(array, context)
                        })?;
                        return Ok(completed.execution);
                    }
                    self.consume_in_context(file, context, BATCH_ROWS, &mut |array| {
                        consume(array, context)
                    })
                })?;
        executed.runtime = self.snapshot();
        Ok(executed)
    }

    fn consume_in_context(
        &self,
        file: &vortex::file::VortexFile,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexUnary> {
        let rows = super::completed_result::CompletedRows::streaming_native(
            self.bound.fields.clone(),
            context.memory(),
            batch_rows,
            context.cancellation().clone(),
            consume,
        )?;
        let mut output = UnaryOutput {
            columns: &self.bound.output_columns,
            rows: 0,
            payload: Some(rows),
        };
        self.run(file, context, &mut output)
    }

    fn execute_output(
        &self,
        cancellation: &CancellationToken,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<ExecutedVortexUnary> {
        let mut executed = self
            .source
            .with_native_execution_controlled(cancellation, |file, context| {
                self.run(file, context, output)
            })?;
        executed.runtime = self.snapshot();
        Ok(executed)
    }

    /// Collect at most 65,536 rows and 8 MiB of complete native buffers.
    /// # Errors
    /// Denies collection beyond its boundary; use streaming delivery or a writer.
    pub fn execute_owned(&self) -> Result<ExecutedOwnedVortexUnary> {
        let mut arrays = ReservedVec::new(self.session.memory())?;
        let mut rows = 0usize;
        let mut bytes = 0u64;
        let execution = self.for_each_batch(&CancellationToken::default(), |array, _| {
            rows = rows
                .checked_add(array.len())
                .ok_or_else(|| failed("collection row overflow"))?;
            bytes = bytes
                .checked_add(array.nbytes())
                .ok_or_else(|| failed("collection byte overflow"))?;
            if rows > 65_536 || bytes > 8 * 1024 * 1024 {
                return Err(failed(
                    "small collection exceeds 65536 rows or 8 MiB; use streaming output",
                ));
            }
            arrays.push(array)
        })?;
        let (arrays, ownership) = arrays.into_parts();
        let result = self.session.own_completed_arrays(arrays, ownership)?;
        Ok(ExecutedOwnedVortexUnary { execution, result })
    }

    /// Collect complete rows as bounded JSONL in the same execution admission.
    /// Values are serialized directly from each native batch; the source is
    /// validated again after the final value is rendered. No query is replayed.
    /// # Errors
    /// Rejects output above 65,536 rows or 8 MiB, unsupported scalar types,
    /// nonfinite numbers, memory pressure, cancellation and source invalidation.
    pub fn collect_jsonl(&self, cancellation: &CancellationToken) -> Result<CollectedVortexUnary> {
        let mut sink = super::collect::JsonRows::new(self.session.memory(), 8 * 1024 * 1024, true)?;
        let mut execution = self.for_each_batch(cancellation, |array, context| {
            sink.append_native(&array, context)
        })?;
        let result_jsonl = sink.finish_certified(&mut execution.native_io_certificate)?;
        Ok(CollectedVortexUnary {
            execution,
            result_jsonl,
        })
    }

    fn run(
        &self,
        file: &vortex::file::VortexFile,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<ExecutedVortexUnary> {
        let (state, evidence) = self.scan_state(file, context, output)?;
        let usage = state.usage();
        let pre_limit = state.finish(&self.bound, context, output)?;
        output.finish(context)?;
        self.certify_scan(
            context,
            evidence,
            output.rows,
            pre_limit,
            &self.bound.output_columns,
            usage,
        )
    }

    fn scan_state(
        &self,
        file: &vortex::file::VortexFile,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<(select::State, ScanEvidence)> {
        let filter = self.plan.filter.as_ref();
        let mut embedded = super::VortexLocalPrimitiveEmbeddedLayoutReport::from_file(
            file,
            self.bound.request.kind,
            filter.is_some(),
            self.plan.projection.is_some(),
        );
        let pruned = filter
            .map(|filter| file.can_prune(filter).map_err(vortex_error))
            .transpose()?
            .unwrap_or(false);
        if filter.is_some() {
            embedded.mark_pruning_consulted(pruned);
        }
        let mut evidence = EvidenceOwners::new(context, self.bound.columns.len())?;
        let mut splits = ReservedVec::new(context.memory())?;
        let mut state = select::State::new(
            &self.bound,
            context,
            Some(if pruned { 0 } else { file.row_count() }),
            true,
        )?;
        let max_chunk_rows = if pruned {
            0
        } else {
            self.consume_scan(
                file,
                context,
                output,
                &mut state,
                &mut splits,
                &mut evidence,
            )?
        };
        Ok((
            state,
            ScanEvidence {
                evidence,
                splits,
                embedded,
                max_chunk_rows,
                source_rows: file.row_count(),
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn certify_scan(
        &self,
        context: &NativeExecutionContext<'_>,
        scan: ScanEvidence,
        rows: usize,
        pre_limit: usize,
        columns: &[String],
        usage: report::StateUsage,
    ) -> Result<ExecutedVortexUnary> {
        let ScanEvidence {
            mut evidence,
            splits,
            embedded,
            max_chunk_rows,
            source_rows,
        } = scan;
        context.check_cancelled()?;
        let source = shardloom_core::UniversalInputSource::from_dataset_uri(
            self.bound
                .request
                .source_uri
                .clone()
                .ok_or_else(|| failed("source URI is absent"))?,
        )?;
        let prepared_report =
            super::plan_vortex_reader_generated_prepared_batch_envelopes(&source, &splits.values);
        let (reader_splits, lease) = splits.into_parts();
        evidence.payload.push(lease)?;
        let scan = LocalVortexScan {
            source_row_count: source_rows,
            result_row_count: rows,
            pre_limit_result_row_count: pre_limit,
            arrays_read_count: reader_splits.len(),
            reader_splits,
            reader_generated_prepared_batch_report: prepared_report,
            control_plane_micros: 0,
            evidence_collection_micros: 0,
            max_chunk_rows,
            resource_envelope: self.policy.resource_envelope(),
            max_parallelism_requested: self.policy.max_parallelism,
            scan_concurrency_per_worker: 1,
            projected_columns: if self.bound.request.kind
                == VortexQueryPrimitiveKind::DuplicateMaskRows
            {
                self.bound.columns.clone()
            } else {
                columns.to_vec()
            },
            filter_pushdown_applied: self.plan.filter.is_some(),
            projection_pushdown_applied: self.plan.projection.is_some(),
            residual_predicate_materialization: super::ResidualPredicateMaterialization::from_flags(
                self.bound.predicate.is_some(),
                self.bound.predicate.is_some() && max_chunk_rows > 0,
            ),
            source_order_limit: self.bound.request.source_order_limit,
            embedded_layout: embedded,
            embedded_derived_column_rewrites: self.plan.embedded_derived_column_rewrites.clone(),
        };
        let report = self.execution_report(&scan, usage)?;
        let native_io_certificate =
            super::local_primitive_native_io_certificate(&self.bound.request, &report)?;
        if report.has_errors() || !native_io_certificate.is_certified() {
            return Err(failed("native execution was not certified"));
        }
        Ok(ExecutedVortexUnary {
            report,
            native_io_certificate,
            runtime: self.snapshot(),
            _evidence: evidence,
        })
    }

    fn consume_scan(
        &self,
        file: &vortex::file::VortexFile,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
        state: &mut select::State,
        splits: &mut ReservedVec<super::VortexReaderBackedSplitEvidence>,
        evidence: &mut EvidenceOwners,
    ) -> Result<usize> {
        use vortex::layout::scan::split_by::SplitBy;
        let mut scan = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_split_by(SplitBy::RowCount(8192))
            .with_concurrency(1);
        if let Some(filter) = &self.plan.filter {
            scan = scan.with_filter(super::bind_vortex_scan_expr(file, filter)?);
        }
        if let Some(projection) = &self.plan.projection {
            scan = scan.with_projection(super::bind_vortex_scan_expr(file, projection)?);
        }
        if self.bound.request.kind == VortexQueryPrimitiveKind::TailRows {
            let limit = self
                .bound
                .request
                .source_order_limit
                .ok_or_else(|| failed("tail limit is absent"))? as u64;
            scan = scan.with_row_range(file.row_count().saturating_sub(limit)..file.row_count());
        }
        let uri = self
            .bound
            .request
            .source_uri
            .as_ref()
            .ok_or_else(|| failed("source URI is absent"))?;
        let mut max_chunk_rows = 0;
        for chunk in scan
            .into_array_iter(context.runtime())
            .map_err(vortex_error)?
        {
            context.check_cancelled()?;
            let chunk = chunk.map_err(vortex_error)?;
            evidence.reserve_chunk(context, self.bound.columns.len(), uri.as_str().len())?;
            splits.reserve_one()?;
            splits
                .values
                .push(super::VortexReaderBackedSplitEvidence::local_scan_chunk(
                    uri.clone(),
                    splits.values.len(),
                    chunk.len(),
                    chunk.dtype().to_string(),
                    chunk.encoding_id().to_string(),
                    chunk.nchildren(),
                    chunk.nbuffers(),
                )?);
            max_chunk_rows = max_chunk_rows.max(chunk.len());
            let mut batch = NativeBatch::new(&chunk, &self.bound.columns, context)?;
            if state.consume(&self.bound, &mut batch, chunk.len(), context, output)? {
                break;
            }
        }
        Ok(max_chunk_rows)
    }
}

struct ScanEvidence {
    evidence: EvidenceOwners,
    splits: ReservedVec<super::VortexReaderBackedSplitEvidence>,
    embedded: super::VortexLocalPrimitiveEmbeddedLayoutReport,
    max_chunk_rows: usize,
    source_rows: u64,
}

struct EvidenceOwners {
    payload: ReservedVec<MemoryLease>,
    _base: MemoryLease,
}
impl EvidenceOwners {
    fn new(context: &NativeExecutionContext<'_>, columns: usize) -> Result<Self> {
        Ok(Self {
            payload: ReservedVec::new(context.memory())?,
            _base: context.memory().reserve(65_536 + columns as u64 * 4096)?,
        })
    }
    fn reserve_chunk(
        &mut self,
        context: &NativeExecutionContext<'_>,
        columns: usize,
        uri: usize,
    ) -> Result<()> {
        self.payload.reserve_one()?;
        self.payload.values.push(
            context
                .memory()
                .reserve(4096 + columns as u64 * 1024 + uri as u64 * 16)?,
        );
        Ok(())
    }
}

struct UnaryOutput<'schema, 'consumer> {
    columns: &'schema [String],
    rows: usize,
    payload: Option<super::completed_result::CompletedRows<'consumer>>,
}
impl<'schema> UnaryOutput<'schema, '_> {
    fn discard(columns: &'schema [String]) -> Self {
        Self {
            columns,
            rows: 0,
            payload: None,
        }
    }
    fn emit<'a>(
        &mut self,
        rows: usize,
        value: impl FnMut(usize, usize) -> Result<Value<'a>>,
    ) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        if let Some(payload) = &mut self.payload {
            payload.push_values(self.columns, rows, value)?;
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| failed("result row count overflow"))?;
        Ok(())
    }
    fn emit_native(
        &mut self,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        build: impl FnOnce() -> Result<ArrayRef>,
    ) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        if let Some(payload) = &mut self.payload {
            let array = build()?;
            if array.len() != rows {
                return Err(failed("native emission changed its row count"));
            }
            payload.push_native(array, context)?;
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| failed("result row count overflow"))?;
        Ok(())
    }
    fn finish(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if let Some(payload) = &mut self.payload {
            if self.rows == 0 {
                payload.push_empty_native(self.columns, context)?;
            }
            payload.finish_stream()?;
        }
        Ok(())
    }
}

#[cfg(feature = "vortex-write")]
#[path = "local_primitive_unary_writer.rs"]
mod writer;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_unary_tests.rs"]
mod tests;
