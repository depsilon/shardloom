//! Metadata-based native strategy admission over one retained source generation.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
};
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
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
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| failed("source URI is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| failed("requires one local native source"))?;
    session.prepare_file(path)
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
        matches!(
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
        shardloom_exec::compute_pool::CancellationToken::default,
        |spill| {
            shardloom_exec::compute_pool::CancellationToken::from_shared_flag(
                std::sync::Arc::clone(&spill.cancellation),
            )
        },
    );
    session.with_native_execution_context(&cancellation, |context| {
        source.with_admitted_native_execution(context, |file, context| {
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
                None,
                file,
                context.native_session(),
                context.runtime(),
                generation.as_ref(),
                Some(context),
            )?;
            super::sort_rows_report(request, &scan)
                .map(|report| report.with_physical_policy(physical))
        })
    })
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
    let (policy, physical) = policy.with_writer_sink_physical_policy_for_request(request);
    source_session(&source, request, Some(policy))?;
    let report = match request.kind {
        super::VortexQueryPrimitiveKind::SimpleAggregate => {
            super::prepared_aggregate::prepare_aggregate_from_source(request, policy, source)?
                .write(output, format, overwrite)?
        }
        super::VortexQueryPrimitiveKind::SortRows => {
            super::completed_result::export_sort_from_source(
                request, output, format, overwrite, policy, &source,
            )?
        }
        super::VortexQueryPrimitiveKind::ProjectColumns
        | super::VortexQueryPrimitiveKind::FilterPredicate
        | super::VortexQueryPrimitiveKind::FilterAndProject => {
            let Some(plan) = super::native_sink::prepare_from_source(request, source, policy)?
            else {
                return Ok(None);
            };
            super::completed_result::write_plan(plan, request, output, format, overwrite, policy)?
        }
        _ => {
            return Err(failed(
                "optimized source writer is not admitted for this operation",
            ));
        }
    };
    Ok(Some(report.with_physical_policy(physical)))
}
