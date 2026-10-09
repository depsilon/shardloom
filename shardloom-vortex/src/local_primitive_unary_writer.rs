//! All admitted writers consume the same complete native unary stream.

use super::super::{VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportReport};
use super::{CancellationToken, NativeExecutionContext, PreparedVortexUnary, Result, failed};

impl PreparedVortexUnary {
    /// Publish complete results atomically using the shared bounded native writer.
    /// # Errors
    /// Rejects source changes, pressure, cancellation, incomplete output and existing targets.
    pub fn write(
        &self,
        path: &std::path::Path,
        format: VortexLocalPrimitiveRowExportFormat,
        allow_overwrite: bool,
    ) -> Result<VortexLocalPrimitiveRowExportReport> {
        self.write_controlled(path, format, allow_overwrite, &CancellationToken::default())
    }

    /// Execute and write under one parent cancellation token and resource grant.
    /// # Errors
    /// Returns the same errors as [`Self::write`], including parent cancellation.
    #[allow(clippy::too_many_lines)] // Keep source validation, shared admission and publication together.
    pub fn write_controlled(
        &self,
        path: &std::path::Path,
        format: VortexLocalPrimitiveRowExportFormat,
        allow_overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<VortexLocalPrimitiveRowExportReport> {
        let uri = self
            .bound
            .request
            .source_uri
            .as_ref()
            .ok_or_else(|| failed("source URI is absent"))?;
        let source_path = super::super::local_vortex_path(uri, self.bound.request.kind)?
            .map(std::fs::canonicalize)
            .transpose()
            .map_err(super::vortex_error)?;
        if self.bound.pivot.is_some() {
            return self
                .source
                .with_native_execution_controlled(cancellation, |file, context| {
                    let completed = self.complete_pivot(file, context)?;
                    let plan = super::super::native_sink::NativeSinkPlan::produced(
                        self.session.clone(),
                        super::DType::struct_(
                            completed.result.fields().to_vec(),
                            super::Nullability::NonNullable,
                        ),
                        completed.result.rows() as u64,
                        source_path,
                        Some(self.source.clone()),
                    )?;
                    let mut delivered = false;
                    let mut result = Some(completed.result);
                    let mut producer = |context: &NativeExecutionContext<'_>,
                                        batch_rows,
                                        consume: &mut dyn FnMut(
                        vortex::array::ArrayRef,
                    )
                        -> Result<bool>| {
                        if delivered {
                            return Err(failed("pivot result producer was invoked twice"));
                        }
                        result
                            .take()
                            .ok_or_else(|| failed("pivot result producer was invoked twice"))?
                            .emit(&self.bound, context, batch_rows, None, &mut |array| {
                                if !consume(array)? {
                                    return Err(failed(
                                        "pivot result consumer stopped before completion",
                                    ));
                                }
                                Ok(())
                            })?;
                        delivered = true;
                        Ok(())
                    };
                    let report = super::super::completed_result::write_stream_admitted(
                        plan,
                        &self.bound.request,
                        path,
                        format,
                        allow_overwrite,
                        self.policy,
                        &mut producer,
                        cancellation,
                        Some(context),
                    )?;
                    if !delivered {
                        return Err(failed("pivot result producer did not complete"));
                    }
                    Ok(self.writer_report(report, completed.execution.report))
                });
        }
        let upper_rows = self.upper_output_rows()?;
        let plan = super::super::native_sink::NativeSinkPlan::produced(
            self.session.clone(),
            super::DType::struct_(self.bound.fields.clone(), super::Nullability::NonNullable),
            upper_rows,
            source_path,
            Some(self.source.clone()),
        )?;
        let mut executed = None;
        let mut producer =
            |context: &NativeExecutionContext<'_>,
             batch_rows,
             consume: &mut dyn FnMut(vortex::array::ArrayRef) -> Result<bool>| {
                executed = Some(self.source.with_admitted_native_execution(
                    context,
                    |file, context| {
                        self.consume_in_context(file, context, batch_rows, &mut |array| {
                            if !consume(array)? {
                                return Err(failed("result consumer stopped before completion"));
                            }
                            Ok(())
                        })
                    },
                )?);
                Ok(())
            };
        let report = super::super::completed_result::write_stream(
            plan,
            &self.bound.request,
            path,
            format,
            allow_overwrite,
            self.policy,
            &mut producer,
            cancellation,
        )?;
        let execution = executed
            .ok_or_else(|| failed("result producer did not complete"))?
            .report;
        Ok(self.writer_report(report, execution))
    }

    fn writer_report(
        &self,
        mut report: VortexLocalPrimitiveRowExportReport,
        execution: super::VortexLocalPrimitiveExecutionReport,
    ) -> VortexLocalPrimitiveRowExportReport {
        report.rows_scanned = execution.rows_scanned;
        report.arrays_read_count = execution.arrays_read_count;
        report.max_chunk_rows = execution.max_chunk_rows;
        report.pre_limit_result_row_count = execution
            .source_order_limit_input_rows
            .unwrap_or(report.rows_written);
        if let Some(evidence) = report.evidence.native_array_sink.as_mut() {
            evidence.pre_limit_result_row_count_exact =
                self.bound.request.source_order_limit.is_none()
                    || matches!(
                        self.bound.request.kind,
                        super::VortexQueryPrimitiveKind::TailRows
                            | super::VortexQueryPrimitiveKind::SampleRows
                            | super::VortexQueryPrimitiveKind::PivotRows
                    )
                    || (self.bound.request.kind
                        == super::VortexQueryPrimitiveKind::DropDuplicateRows
                        && self.bound.request.duplicate_keep
                            != super::super::VortexDuplicateKeepPolicy::First);
        }
        report.state_budget = execution.state_budget;
        report.physical_policy = execution.physical_policy;
        report.source_order_limit_requested = execution.source_order_limit_requested;
        report.evidence.upstream_scan_called = execution.upstream_scan_called;
        report.evidence.side_effects.data_read |= execution.data_read;
        report.evidence.side_effects.data_decoded |= execution.data_decoded;
        report.evidence.side_effects.data_materialized |= execution.data_materialized;
        report.evidence.side_effects.row_read |= execution.row_read;
        report.evidence.pushdown = super::super::VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: execution.filter_pushdown_applied,
            projection_pushdown_applied: execution.projection_pushdown_applied,
            source_order_limit_applied: execution.source_order_limit_applied,
        };
        report
    }

    fn upper_output_rows(&self) -> Result<u64> {
        let source_rows = self.source.file().row_count();
        if self.bound.request.kind == super::VortexQueryPrimitiveKind::ExplodeRows {
            // Variable list lengths are discovered during execution. The writer
            // grows reserved metadata by actual batches; no guessed expansion
            // factor or source-row count may truncate the native result.
            return Ok(self.bound.request.source_order_limit.unwrap_or(usize::MAX) as u64);
        }
        if self.bound.request.kind == super::VortexQueryPrimitiveKind::SampleRows {
            return super::super::sample_target_count(
                &self.bound.request,
                usize::try_from(source_rows).map_err(super::vortex_error)?,
            )
            .and_then(|rows| u64::try_from(rows).map_err(super::vortex_error));
        }
        let upper_rows = if self.bound.request.kind == super::VortexQueryPrimitiveKind::MeltRows {
            source_rows
                .checked_mul(
                    super::super::required_melt_projection(&self.bound.request)?
                        .value_columns
                        .len() as u64,
                )
                .ok_or_else(|| failed("melt writer row bound overflow"))?
        } else {
            source_rows
        };
        Ok(upper_rows.min(
            self.bound
                .request
                .source_order_limit
                .map_or(u64::MAX, |n| n as u64),
        ))
    }
}
