//! Metadata-based native strategy admission over one retained source generation.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
};
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::dtype::DType;

/// Prepare the source with the same CPU ownership as the optimized operation.
/// No query rows are read and no alternative operation is executed here.
/// # Errors
/// Rejects nonlocal sources, invalid resource grants and source-open failures.
pub fn prepare_source(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<PreparedVortexSource> {
    let session = super::prepared_aggregate::aggregate_session(request, policy)?;
    prepare_source_in_session(request, policy, &session, &CancellationToken::default())
}

/// Prepare a source in a caller-owned resident session under its parent
/// cancellation/deadline token. This validates the same operation policy without
/// creating another session or changing native strategy selection.
///
/// # Errors
/// Rejects cancellation, invalid session grants, nonlocal sources and source-open
/// failures.
pub fn prepare_source_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexSource> {
    cancellation.check()?;
    super::prepared_relational::validate_session_policy(policy, session)?;
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("requires one local native source"))?;
    let source = session.prepare_file_with_cancellation(path, cancellation)?;
    cancellation.check()?;
    Ok(source)
}

/// Select extended native semantics from authoritative referenced field types.
/// This reads retained metadata only; unsupported metadata is rejected at bind.
/// # Errors
/// Rejects changed generations, nonstruct schemas and unknown referenced fields.
pub fn requires_relational(
    source: &PreparedVortexSource,
    columns: Option<&[String]>,
) -> Result<bool> {
    source.validate_generation()?;
    let fields = source
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| failed("requires a struct source schema"))?;
    let extended = |dtype: &DType| {
        super::native_payload::is_nested(dtype)
            || matches!(
                dtype,
                DType::Binary(_) | DType::Decimal(..) | DType::Extension(_)
            )
    };
    if let Some(columns) = columns {
        for column in columns {
            let index = fields
                .names()
                .iter()
                .position(|name| name.as_ref() == column)
                .ok_or_else(|| failed("referenced source column is absent"))?;
            let dtype = fields
                .field_by_index(index)
                .ok_or_else(|| failed("source field metadata is absent"))?;
            if extended(&dtype) {
                return Ok(true);
            }
        }
        Ok(false)
    } else {
        Ok(fields.fields().any(|dtype| extended(&dtype)))
    }
}

/// Inspect only source fields referenced by the already-lowered native request.
/// Output aliases and SQL spelling do not participate in schema selection.
/// # Errors
/// Rejects incomplete requests, changed sources and absent referenced fields.
pub fn request_requires_relational(
    source: &PreparedVortexSource,
    request: &VortexQueryPrimitiveRequest,
) -> Result<bool> {
    use super::VortexQueryPrimitiveKind as Kind;
    let mut columns = match request.kind {
        Kind::SimpleAggregate => super::required_simple_aggregate(request)?.projected_columns(),
        Kind::CountWhere => Vec::new(),
        Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject | Kind::SortRows => {
            match &request.projection {
                shardloom_plan::ProjectionRequest::All => return requires_relational(source, None),
                shardloom_plan::ProjectionRequest::Columns(columns) => columns.clone(),
            }
        }
        _ => {
            return Err(failed(
                "source strategy selection is not admitted for this operation",
            ));
        }
    };
    if let Some(predicate) = &request.predicate {
        super::append_predicate_columns(predicate, &mut columns);
    }
    if request.kind == Kind::SortRows {
        columns.extend(super::required_sort_rows(request)?.order_columns());
    }
    let columns = columns
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    requires_relational(source, Some(&columns))
}

pub(super) fn source_session(
    source: &PreparedVortexSource,
    request: &VortexQueryPrimitiveRequest,
    policy: Option<VortexLocalPrimitiveExecutionPolicy>,
) -> Result<ResidentVortexSession> {
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("requires one local native source"))?;
    source.validate_file_metadata(&std::fs::metadata(path).map_err(super::vortex_error)?)?;
    let session = source.retained_session();
    if let Some(policy) = policy {
        let snapshot = session.snapshot();
        if policy.max_parallelism == 0
            || policy.max_parallelism != policy.resource_envelope.max_parallelism
            || session.parallelism() > policy.max_parallelism
            || snapshot.memory.limit_bytes > policy.resource_envelope.memory_budget_bytes
        {
            return Err(failed(
                "retained source exceeds the requested resource grant",
            ));
        }
    }
    Ok(session)
}

/// Run the existing optimized order strategy on an already admitted source.
/// # Errors
/// Rejects mismatched generations, resource grants and unsupported sort requests.
pub fn execute_sort(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
) -> Result<super::VortexLocalPrimitiveExecutionReport> {
    execute_sort_controlled(request, policy, source, &CancellationToken::default())
}

/// Run the existing optimized order strategy under the caller's operation token.
/// Source selection, sort strategy and output semantics are unchanged.
/// # Errors
/// Rejects cancellation, mismatched generations, resource grants and unsupported
/// sort requests.
pub fn execute_sort_controlled(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<super::VortexLocalPrimitiveExecutionReport> {
    cancellation.check()?;
    execute_sort_with_sink(request, policy, source, None, cancellation)
}

/// Complete sort output through the shared bounded JSON sink. The same native
/// sort producer owns selection, spill, ordering and cleanup; no rows are read
/// from a diagnostic report and the source is not replayed.
pub struct CollectedVortexSort {
    pub report: super::VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: shardloom_core::NativeIoCertificate,
    pub result_jsonl: shardloom_exec::live_memory::Budgeted<String>,
    pub result_schema_json: shardloom_exec::live_memory::Budgeted<String>,
}

/// # Errors
/// Rejects invalid sources, resource pressure, changed generations, cancellation
/// and complete results exceeding 65,536 rows or the 8-MiB JSON bound.
pub fn collect_sort(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
) -> Result<CollectedVortexSort> {
    collect_sort_controlled(request, policy, source, &CancellationToken::default())
}

/// Collect the optimized native sort result under the caller's operation token.
/// JSON output remains bounded and owned by the retained source's session.
/// # Errors
/// Rejects cancellation, invalid sources, resource pressure, changed generations,
/// and results exceeding the existing row/JSON bounds.
pub fn collect_sort_controlled(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<CollectedVortexSort> {
    cancellation.check()?;
    let session = source_session(source, request, Some(policy))?;
    cancellation.check()?;
    let mut sink = super::collect::JsonRows::new(session.memory(), 8 * 1024 * 1024, true)?;
    cancellation.check()?;
    let report = execute_sort_with_sink(request, policy, source, Some(&mut sink), cancellation)?;
    cancellation.check()?;
    let mut native_io_certificate = super::local_primitive_native_io_certificate(request, &report)?;
    cancellation.check()?;
    let (result_jsonl, result_schema_json) = sink.finish_certified(&mut native_io_certificate)?;
    cancellation.check()?;
    source.validate_generation()?;
    cancellation.check()?;
    Ok(CollectedVortexSort {
        report,
        native_io_certificate,
        result_jsonl,
        result_schema_json,
    })
}

fn execute_sort_with_sink(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: &PreparedVortexSource,
    mut sink: Option<&mut super::collect::JsonRows>,
    parent_cancellation: &CancellationToken,
) -> Result<super::VortexLocalPrimitiveExecutionReport> {
    parent_cancellation.check()?;
    if request.kind != super::VortexQueryPrimitiveKind::SortRows {
        return Err(failed("requires a sort request"));
    }
    let session = source_session(source, request, Some(policy))?;
    let (policy, physical) = policy.with_physical_policy_for_request(request);
    let sort = super::required_sort_rows(request)?;
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("sort source is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("sort source is not local"))?;
    let cancellation = sort.spill.as_ref().map_or_else(
        || parent_cancellation.clone(),
        |spill| {
            shardloom_exec::compute_pool::CancellationToken::from_shared_flag_with_parent(
                std::sync::Arc::clone(&spill.cancellation),
                parent_cancellation,
            )
        },
    );
    cancellation.check()?;
    let report = session.with_native_execution_context(&cancellation, |context| {
        source.with_admitted_native_execution(context, |file, context| {
            let collect = sink.is_some();
            let columns =
                super::projected_column_names(source.dtype(), &request.projection, request.kind)?;
            let fields = columns
                .iter()
                .map(|name| {
                    Ok((
                        name.clone(),
                        super::completed_result::source_field(source.dtype(), name)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut consume = |array| {
                sink.as_mut()
                    .ok_or_else(|| failed("sort sink is absent"))?
                    .append_native(&array, context)
            };
            let mut output = if collect {
                Some(super::completed_result::CompletedRows::streaming(
                    fields,
                    context.memory(),
                    2048,
                    context.cancellation().clone(),
                    &mut consume,
                )?)
            } else {
                None
            };
            let generation = sort
                .spill
                .as_ref()
                .map(|_| {
                    super::sort_spill::SortSourceGeneration::capture(&path).map(std::sync::Arc::new)
                })
                .transpose()?;
            let scan = super::read_opened_local_vortex_sort_rows_scan_with_output(
                uri,
                request,
                policy,
                output.as_mut(),
                file,
                context.native_session(),
                context.runtime(),
                generation.as_ref(),
                Some(context),
            )?;
            super::sort_rows_report(request, &scan)
                .map(|report| report.with_physical_policy(physical))
        })
    })?;
    cancellation.check()?;
    Ok(report)
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native source dispatch: {message}; no fallback execution was attempted"
    ))
}

/// Write through an existing optimized provider with the admitted source.
/// `None` means that metadata binding did not admit a simple array sink; no
/// query execution or output work has occurred. Errors are terminal.
/// # Errors
/// Rejects changed sources, unsupported requests, resource pressure and failed
/// output. Aggregate and order writers retain their existing spill contracts.
#[cfg(feature = "vortex-write")]
pub fn try_write_source(
    request: &VortexQueryPrimitiveRequest,
    output: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
) -> Result<Option<super::VortexLocalPrimitiveRowExportReport>> {
    try_write_source_controlled(
        request,
        output,
        format,
        overwrite,
        policy,
        source,
        &CancellationToken::default(),
    )
}

/// Write through an existing optimized provider under the operation's parent
/// cancellation/deadline token. Native writer selection is unchanged.
/// # Errors
/// Rejects cancellation, changed sources, unsupported requests, resource pressure
/// and failed output.
#[cfg(feature = "vortex-write")]
pub fn try_write_source_controlled(
    request: &VortexQueryPrimitiveRequest,
    output: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<Option<super::VortexLocalPrimitiveRowExportReport>> {
    cancellation.check()?;
    let (policy, physical) = policy.with_writer_sink_physical_policy_for_request(request);
    source_session(&source, request, Some(policy))?;
    cancellation.check()?;
    let report = match request.kind {
        super::VortexQueryPrimitiveKind::SimpleAggregate => {
            super::prepared_aggregate::prepare_aggregate_from_source(request, policy, source)?
                .write_controlled(output, format, overwrite, cancellation)?
        }
        super::VortexQueryPrimitiveKind::SortRows => {
            super::completed_result::export_sort_from_source_controlled(
                request,
                output,
                format,
                overwrite,
                policy,
                &source,
                cancellation,
            )?
        }
        super::VortexQueryPrimitiveKind::ProjectColumns
        | super::VortexQueryPrimitiveKind::FilterPredicate
        | super::VortexQueryPrimitiveKind::FilterAndProject => {
            let Some(plan) = super::native_sink::prepare_from_source(request, source, policy)?
            else {
                return Ok(None);
            };
            super::completed_result::write_plan_controlled(
                plan,
                request,
                output,
                format,
                overwrite,
                policy,
                cancellation,
            )?
        }
        _ => {
            if super::prepared_unary::supports(request.kind)
                && request.structured_projection.is_none()
            {
                super::prepared_unary::prepare_unary_from_source(request, policy, source)?
                    .write_controlled(output, format, overwrite, cancellation)?
            } else if let Some(plan) =
                super::native_sink::prepare_from_source(request, source.clone(), policy)?
            {
                super::completed_result::write_plan_controlled(
                    plan,
                    request,
                    output,
                    format,
                    overwrite,
                    policy,
                    cancellation,
                )?
            } else {
                return Err(failed(
                    "optimized source writer is not admitted for this operation",
                ));
            }
        }
    };
    cancellation.check()?;
    Ok(Some(report.with_physical_policy(physical)))
}
