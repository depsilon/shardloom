//! Existing local writers consume one execution and validate every input source.

use super::super::{VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportReport};
use super::{
    ArrayRef, BATCH_ROWS, CancellationToken, ExecutedVortexRelational, NativeExecutionContext,
    Node, NodeKind, PreparedVortexRelational, Result, SetKind, VortexQueryPrimitiveRequest, failed,
    vortex_error,
};
use shardloom_core::{ColumnRef, DatasetUri};

/// Relational execution proof and its terminal output-adapter proof.
pub struct WrittenVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub output: VortexLocalPrimitiveRowExportReport,
}

impl PreparedVortexRelational {
    /// Write complete results through an admitted local output adapter.
    /// # Errors
    /// Rejects aliases of either input, unsupported output schemas, resource
    /// denial and changed source generations. Failure never publishes a prefix.
    pub fn write(
        &self,
        path: &std::path::Path,
        format: VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
    ) -> Result<WrittenVortexRelational> {
        self.write_controlled(path, format, overwrite, &CancellationToken::default())
    }

    /// The supplied cancellation owner covers query execution and publication.
    /// # Errors
    /// Propagates cancellation and every failure described by `write`.
    pub fn write_controlled(
        &self,
        path: &std::path::Path,
        format: VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<WrittenVortexRelational> {
        let source = self
            .source_paths
            .first()
            .ok_or_else(|| failed("relational source is absent"))?;
        // This request describes only the terminal adapter's projection of the
        // completed native columns. The relational tree has its own certificate.
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(source.display().to_string())?,
            shardloom_plan::ProjectionRequest::columns(
                self.root
                    .fields
                    .iter()
                    .map(|(name, _)| ColumnRef::new(name))
                    .collect::<Result<Vec<_>>>()?,
            ),
        );
        let plan = super::super::native_sink::NativeSinkPlan::produced_sources(
            self.session.clone(),
            self.output_dtype(),
            self.upper_rows(&self.root),
            None,
            self.sources.clone(),
        )?;
        #[cfg(feature = "universal-format-io")]
        let plan = plan.with_preparation_sources(self.preparation_sources.clone());
        let mut execution = None;
        let mut producer =
            |context: &NativeExecutionContext<'_>,
             batch_rows: usize,
             consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                if execution.is_some() {
                    return Err(failed("relational producer was invoked twice"));
                }
                execution = Some(self.session.with_admitted_sources_execution(
                    &self.sources,
                    context,
                    |context| {
                        let mut submitted = 0u64;
                        let completed = self.consume_in_context(
                            context,
                            batch_rows.min(BATCH_ROWS),
                            &mut |array| {
                                submitted = submitted
                                    .checked_add(array.len() as u64)
                                    .ok_or_else(|| failed("submitted row count overflow"))?;
                                if !consume(array)? {
                                    return Err(failed(
                                        "relational result consumer stopped before completion",
                                    ));
                                }
                                Ok(())
                            },
                        )?;
                        if completed.output_rows != submitted {
                            return Err(failed("native producer and consumer row counts disagree"));
                        }
                        let arrays_read =
                            usize::try_from(completed.scan_batches).map_err(vortex_error)?;
                        Ok((completed, arrays_read))
                    },
                )?);
                Ok(())
            };
        let mut output = super::super::completed_result::write_stream(
            plan,
            &request,
            path,
            format,
            overwrite,
            self.policy,
            &mut producer,
            cancellation,
        )?;
        let (mut execution, arrays_read) =
            execution.ok_or_else(|| failed("relational producer did not complete"))?;
        debug_assert_eq!(execution.output_rows, output.rows_written);
        execution.runtime = self.snapshot();
        output.rows_scanned = execution.scan_rows_delivered;
        output.arrays_read_count = arrays_read;
        Ok(WrittenVortexRelational { execution, output })
    }

    fn upper_rows(&self, node: &Node) -> u64 {
        match &node.kind {
            NodeKind::Outer => 1,
            NodeKind::Scan { source, .. } => self.sources[*source].file().row_count(),
            NodeKind::Aggregate { input, spec } => {
                if spec.group_names.is_empty() {
                    1
                } else {
                    self.upper_rows(input)
                }
            }
            NodeKind::Window { input, .. }
            | NodeKind::Subquery { input, .. }
            | NodeKind::Project { input, .. }
            | NodeKind::Filter { input, .. }
            | NodeKind::Sort { input, .. } => self.upper_rows(input),
            NodeKind::Limit {
                input,
                offset,
                count,
            } => self
                .upper_rows(input)
                .saturating_sub(*offset as u64)
                .min(*count as u64),
            NodeKind::Join { left, right, spec } => {
                use crate::relational_query::VortexRelationalJoinKind as Kind;
                let left = self.upper_rows(left);
                let right = self.upper_rows(right);
                match spec.kind {
                    Kind::LeftSemi | Kind::LeftAnti => left,
                    Kind::Inner | Kind::Cross => left.saturating_mul(right),
                    Kind::Left => left.saturating_mul(right).max(left),
                    Kind::Right => left.saturating_mul(right).max(right),
                    Kind::Full => left
                        .saturating_mul(right)
                        .saturating_add(left)
                        .saturating_add(right),
                }
            }
            NodeKind::Set {
                left, right, kind, ..
            } => match kind {
                SetKind::UnionAll | SetKind::UnionDistinct => {
                    self.upper_rows(left).saturating_add(self.upper_rows(right))
                }
                SetKind::Intersect | SetKind::Except => self.upper_rows(left),
            },
        }
    }
}
