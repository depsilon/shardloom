//! Complete primitive operations under the caller's native resource owner.

use super::{
    Result, VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
};
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;

#[cfg(test)]
#[path = "local_primitive_execution_tests.rs"]
mod tests;

/// Execute the same native primitive strategies in an existing session. Neither
/// source discovery nor a child operator receives an independent allocation.
/// # Errors
/// Rejects cancellation, invalid policy, changed sources and native execution errors.
pub fn execute_in_session(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    cancellation.check()?;
    super::prepared_relational::validate_session_policy(policy, session)?;
    super::execute_vortex_local_primitive_with_owner(request, policy, Some((session, cancellation)))
}

/// Execute the existing ordered native partition strategies under one session.
/// The command's retained input listing and all provider buffers share its pool.
/// # Errors
/// Rejects cancellation, invalid grants, unsupported partition semantics and input changes.
pub fn execute_partitioned_in_session(
    request: &VortexQueryPrimitiveRequest,
    sources: &[shardloom_core::DatasetUri],
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    cancellation.check()?;
    super::prepared_relational::validate_session_policy(policy, session)?;
    super::execute_vortex_local_partitioned_primitive_with_owner(
        request,
        sources,
        policy,
        Some((session, cancellation)),
    )
}

/// Write a primitive through its existing native adapter while retaining the
/// caller's allocation and cancellation through source preparation and output.
/// # Errors
/// Propagates source, schema, admission, cancellation and output errors.
#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_arguments)] // Keep the existing complete writer request and its owner together.
pub fn export_in_session(
    request: &VortexQueryPrimitiveRequest,
    output: &std::path::Path,
    format: super::VortexLocalPrimitiveRowExportFormat,
    overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<super::VortexLocalPrimitiveRowExportReport> {
    cancellation.check()?;
    let (policy, physical) = policy.with_writer_sink_physical_policy_for_request(request);
    let source = super::prepared_dispatch::prepare_source_in_session(
        request,
        policy,
        session,
        cancellation,
    )?;
    if let Some(report) = super::prepared_dispatch::try_write_source_controlled(
        request,
        output,
        format,
        overwrite,
        policy,
        source.clone(),
        cancellation,
    )? {
        return Ok(report);
    }
    // The ordinary row-export path already selects the native relational scan
    // for residual predicates that cannot bind the direct array sink. Preserve
    // that strategy with the prepared source and its existing owner.
    let uri = request.source_uri.clone().ok_or_else(|| {
        super::ShardLoomError::InvalidOperation("native row export source URI is absent".into())
    })?;
    cancellation.check()?;
    let prepared = super::prepared_relational::prepare_relational_from_source_controlled(
        uri.clone(),
        source,
        policy,
        |_| {
            use crate::relational_query::{
                VortexRelationalLimit, VortexRelationalPlan, VortexRelationalScan,
            };
            let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri,
                projection: request.projection.clone(),
                predicate: request.predicate.clone(),
            });
            Ok(match request.source_order_limit {
                None => scan,
                Some(count) => VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input: scan,
                    offset: 0,
                    count,
                })),
            })
        },
        cancellation,
    )?;
    let mut report = prepared
        .write_controlled(output, format, overwrite, cancellation)?
        .into_output_report();
    report.primitive_kind = request.kind;
    Ok(report.with_physical_policy(physical))
}

pub(super) fn execute_single(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    cancellation.check()?;
    super::prepared_relational::validate_session_policy(policy, session)?;
    let (_, physical) = policy.with_physical_policy_for_request(request);
    if super::prepared_unary::supports(request.kind)
        && let Some(prepared) = super::prepared_unary::prepare_unary_for_optional_reuse_in_session(
            request,
            policy,
            session,
            cancellation,
        )?
    {
        return prepared
            .execute_cancellable(cancellation)
            .map(|executed| executed.report.with_physical_policy(physical));
    }
    if request.kind == VortexQueryPrimitiveKind::SimpleAggregate
        && let Some(prepared) =
            super::prepared_aggregate::prepare_aggregate_for_optional_reuse_in_session(
                request,
                policy,
                session,
                cancellation,
            )?
    {
        return match prepared {
            super::prepared_aggregate::PreparedAggregateDisposition::Reusable(prepared) => {
                prepared.execute_cancellable(cancellation)
            }
            super::prepared_aggregate::PreparedAggregateDisposition::Unretained(prepared) => {
                prepared.execute_cancellable(cancellation)
            }
        }
        .map(|executed| executed.report.with_physical_policy(physical));
    }
    if request.kind == VortexQueryPrimitiveKind::SortRows {
        let source = super::prepared_dispatch::prepare_source_in_session(
            request,
            policy,
            session,
            cancellation,
        )?;
        return super::prepared_dispatch::execute_sort_controlled(
            request,
            policy,
            &source,
            cancellation,
        );
    }
    let report = session.with_native_execution_context(cancellation, |context| {
        super::execute_vortex_local_primitive_enabled(request, policy, Some(context))
    })?;
    if !report.has_errors() {
        session.record_completed_execution();
    }
    Ok(report)
}
