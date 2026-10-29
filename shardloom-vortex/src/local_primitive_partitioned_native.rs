//! Native consumers for the existing complete partitioned aggregate/sort kernels.
//! Collection, streaming and writers share selection, ordering and finalization.

use super::{
    LocalVortexPartitionSource, Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexQueryPrimitiveKind as Kind,
    VortexQueryPrimitiveRequest, completed_result,
};
use crate::resident_session::{
    NativeExecutionContext, OwnedVortexResultBatch, PreparedVortexSource, ResidentSessionSnapshot,
    ResidentVortexSession,
};
use shardloom_core::{DatasetUri, NativeIoCertificate};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use vortex::array::{ArrayRef, dtype::DType};

/// Evidence from the same native producer that delivered every result batch.
pub struct ExecutedVortexPartitioned {
    pub report: VortexLocalPrimitiveExecutionReport,
    pub native_io_certificate: NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
    #[cfg(feature = "vortex-write")]
    sort_pre_limit_rows: Option<u64>,
}

/// Complete native values with the original shared resource owner.
pub struct ExecutedOwnedVortexPartitioned {
    pub execution: ExecutedVortexPartitioned,
    pub result: OwnedVortexResultBatch,
}

/// A borrowed declaration over retained, generation-bound native sources.
/// This handle owns only temporary control metadata, never query state or rows.
pub struct PartitionedNativeOperation<'a> {
    request: &'a VortexQueryPrimitiveRequest,
    sources: &'a [PreparedVortexSource],
    paths: Vec<LocalVortexPartitionSource>,
    session: ResidentVortexSession,
    policy: VortexLocalPrimitiveExecutionPolicy,
    _metadata: MemoryLease,
}

impl<'a> PartitionedNativeOperation<'a> {
    /// Validate the same partitioned provider contract before output construction.
    /// # Errors
    /// Rejects invalid grants, changed files, incompatible schemas and requests
    /// outside the existing aggregate/sort provider, including partitioned spill.
    pub fn new(
        request: &'a VortexQueryPrimitiveRequest,
        policy: VortexLocalPrimitiveExecutionPolicy,
        uris: &[DatasetUri],
        sources: &'a [PreparedVortexSource],
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let first = sources
            .first()
            .ok_or_else(|| failed("sources are absent"))?;
        if sources.len() != uris.len() {
            return Err(failed("source declarations and retained owners differ"));
        }
        let session = super::prepared_dispatch::source_session(first, request, Some(policy))?;
        let metadata =
            session.reserve_input_scratch(metadata_bytes(request, uris, first.dtype())?)?;
        match request.kind {
            Kind::SimpleAggregate => {
                super::prepared_aggregate::canonical(request)?;
                if super::required_simple_aggregate(request)?.spill.is_some() {
                    return Err(failed(
                        "native aggregate spill is not admitted for partitioned sources",
                    ));
                }
            }
            Kind::SortRows => {
                if super::required_sort_rows(request)?.spill.is_some() {
                    return Err(failed(
                        "native sort spill is not admitted for partitioned sources",
                    ));
                }
                if request.source_order_limit.is_none_or(|limit| limit == 0) {
                    return Err(failed("partitioned sort requires a positive output limit"));
                }
            }
            _ => return Err(failed("requires an aggregate or sort declaration")),
        }
        let paths = super::partitioned_local_vortex_source_paths(uris, request.kind)?;
        for (path, source) in paths.iter().zip(sources) {
            cancellation.check()?;
            source.validate_generation()?;
            source.validate_file_metadata(
                &std::fs::metadata(&path.path).map_err(super::vortex_error)?,
            )?;
            if source.dtype() != first.dtype() {
                return Err(failed(
                    "native result partitions require matching source schemas",
                ));
            }
        }
        Ok(Self {
            request,
            sources,
            paths,
            session,
            policy,
            _metadata: metadata,
        })
    }

    /// Deliver complete batches under one shared admission and source-generation
    /// scope. Successful return certifies final validation after the consumer.
    /// # Errors
    /// Propagates cancellation, pressure, changed sources and consumer failures.
    pub fn for_each_batch(
        &self,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexPartitioned> {
        let mut execution =
            self.session
                .with_sources_execution(self.sources, cancellation, |context| {
                    self.consume(context, super::result_batch::VISITOR_ROWS, &mut |array| {
                        consume(array, context)
                    })
                })?;
        cancellation.check()?;
        execution.runtime = self.session.snapshot();
        Ok(execution)
    }

    /// Retain the complete native result without a JSON or row-count ceiling.
    /// # Errors
    /// Propagates provider, allocation, source-generation and cancellation errors.
    pub fn execute_native_result(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ExecutedOwnedVortexPartitioned> {
        cancellation.check()?;
        let mut arrays = super::native_capacity::ReservedVec::new(self.session.memory())?;
        let mut execution = self.for_each_batch(cancellation, |array, context| {
            context.check_cancelled()?;
            arrays.push(array)
        })?;
        let (arrays, ownership) = arrays.into_parts();
        let result = self.session.own_completed_arrays(arrays, ownership)?;
        cancellation.check()?;
        execution.runtime = self.session.snapshot();
        Ok(ExecutedOwnedVortexPartitioned { execution, result })
    }

    fn fields(&self) -> Result<Vec<(String, DType)>> {
        let dtype = self.sources[0].dtype();
        if self.request.kind == Kind::SimpleAggregate {
            return completed_result::aggregate_stream_fields(self.request, dtype);
        }
        super::projected_column_names(dtype, &self.request.projection, self.request.kind)?
            .into_iter()
            .map(|name| {
                let dtype = completed_result::source_field(dtype, &name)?;
                Ok((name, dtype))
            })
            .collect()
    }

    fn consume(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexPartitioned> {
        context.check_cancelled()?;
        let (policy, physical) = self.policy.with_physical_policy_for_request(self.request);
        let (report, sort_pre_limit_rows) = if self.request.kind == Kind::SimpleAggregate {
            let mut output = super::aggregate_owned::AggregateOutput::streaming(
                self.request,
                self.sources[0].dtype(),
                context.memory(),
                batch_rows,
                context.cancellation().clone(),
                consume,
            )?;
            let scan = super::read_local_vortex_simple_aggregate_partitioned_scan(
                &self.paths,
                self.request,
                policy,
                Some(context),
                Some(&mut output),
            )?;
            (super::simple_aggregate_report(self.request, &scan)?, None)
        } else {
            let mut output = completed_result::CompletedRows::streaming_native(
                self.fields()?,
                context.memory(),
                batch_rows,
                context.cancellation().clone(),
                context.float_policy(),
                consume,
            )?;
            let scan = super::read_local_vortex_sort_rows_partitioned_scan(
                &self.paths,
                self.request,
                policy,
                Some(context),
                Some(&mut output),
            )?;
            let rows = super::usize_to_u64(scan.scan.pre_limit_result_row_count)?;
            (super::sort_rows_report(self.request, &scan)?, Some(rows))
        };
        context.check_cancelled()?;
        let report =
            super::partitioned_report(report, self.sources.len())?.with_physical_policy(physical);
        let native_io_certificate =
            super::local_primitive_native_io_certificate(self.request, &report)?;
        if report.has_errors() || !native_io_certificate.is_certified() {
            return Err(failed(
                "execution did not produce a certified native report",
            ));
        }
        #[cfg(not(feature = "vortex-write"))]
        let _ = sort_pre_limit_rows;
        Ok(ExecutedVortexPartitioned {
            report,
            native_io_certificate,
            runtime: self.session.snapshot(),
            #[cfg(feature = "vortex-write")]
            sort_pre_limit_rows,
        })
    }

    /// Publish through the ordinary native writer with the same bounded producer.
    /// Source alias checks cover every participating file before staging begins.
    /// # Errors
    /// Propagates native execution, unsupported format, cancellation, resource
    /// pressure and atomic publication errors. Failed staging is cleaned up.
    #[cfg(feature = "vortex-write")]
    pub fn write_controlled(
        &self,
        path: &std::path::Path,
        format: super::VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<super::VortexLocalPrimitiveRowExportReport> {
        cancellation.check()?;
        let rows = self.sources.iter().try_fold(0_u64, |rows, source| {
            rows.checked_add(source.file().row_count())
                .ok_or_else(|| failed("source row count overflow"))
        })?;
        let upper_rows = if self
            .request
            .simple_aggregate
            .as_ref()
            .is_some_and(|aggregate| {
                aggregate.group_by.is_empty() && aggregate.group_expressions.is_empty()
            }) {
            1
        } else {
            rows
        };
        let plan = super::native_sink::NativeSinkPlan::produced_sources(
            self.session.clone(),
            DType::struct_(
                self.fields()?,
                vortex::array::dtype::Nullability::NonNullable,
            ),
            upper_rows,
            None,
            self.sources.to_vec(),
        )?;
        let mut executed = None;
        let mut producer =
            |context: &NativeExecutionContext<'_>,
             batch_rows,
             consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                executed = Some(self.session.with_admitted_sources_execution(
                    self.sources,
                    context,
                    |context| {
                        self.consume(context, batch_rows, &mut |array| {
                            if !consume(array)? {
                                return Err(failed("result consumer stopped before completion"));
                            }
                            Ok(())
                        })
                    },
                )?);
                Ok(())
            };
        let mut report = completed_result::write_stream(
            plan,
            self.request,
            path,
            format,
            overwrite,
            self.policy,
            &mut producer,
            cancellation,
        )?;
        let execution = executed.ok_or_else(|| failed("result producer did not complete"))?;
        if let Some(rows) = execution.sort_pre_limit_rows {
            report.pre_limit_result_row_count = rows;
            if let Some(evidence) = &mut report.evidence.native_array_sink {
                evidence.pre_limit_result_row_count = rows;
            }
        } else {
            super::prepared_aggregate::annotate_output_count(&mut report, self.request);
        }
        let execution = execution.report;
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
        report.evidence.side_effects.row_read |= execution.row_read;
        report.evidence.pushdown = super::VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: execution.filter_pushdown_applied,
            projection_pushdown_applied: execution.projection_pushdown_applied,
            source_order_limit_applied: execution.source_order_limit_applied,
        };
        Ok(report)
    }
}

fn metadata_bytes(
    request: &VortexQueryPrimitiveRequest,
    uris: &[DatasetUri],
    dtype: &DType,
) -> Result<u64> {
    struct Count(u64);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.0 = self
                .0
                .checked_add(value.len() as u64)
                .ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    let mut count = Count(0);
    std::fmt::write(&mut count, format_args!("{request:?}"))
        .map_err(|_| failed("request metadata size overflow"))?;
    let mut bytes = count
        .0
        .checked_mul(8)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| failed("metadata size overflow"))?;
    for uri in uris {
        bytes = bytes
            .checked_add(
                (uri.as_str().len() as u64)
                    .saturating_mul(4)
                    .saturating_add(512),
            )
            .ok_or_else(|| failed("source metadata size overflow"))?;
    }
    bytes
        .checked_add(super::native_payload::metadata_bytes(dtype)?.saturating_mul(4))
        .ok_or_else(|| failed("schema metadata size overflow"))
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native partitioned result: {message}; no fallback execution was attempted"
    ))
}
