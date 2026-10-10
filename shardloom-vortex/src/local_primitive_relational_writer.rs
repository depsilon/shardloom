//! Existing local writers consume one execution and validate every input source.

use super::super::{VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportReport};
use super::{
    ArrayRef, BATCH_ROWS, CancellationToken, ExecutedVortexRelational, NativeExecutionContext,
    PreparedVortexRelational, Result, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use shardloom_core::{ColumnRef, DatasetUri};

/// Relational execution proof and its terminal output-adapter proof.
pub struct WrittenVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub output: VortexLocalPrimitiveRowExportReport,
}

impl PreparedVortexRelational {
    /// Write the same admitted plan to each output adapter, staging every result
    /// before publication. Each target executes the plan with the same retained
    /// sources and budget. See `output_fanout::write` for partial-commit recovery.
    /// # Errors
    /// Propagates query, adapter, destination, generation and publication errors.
    pub fn write_many(
        &self,
        targets: &[(std::path::PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
    ) -> Result<Vec<WrittenVortexRelational>> {
        self.write_many_controlled(targets, overwrite, &CancellationToken::default())
    }

    /// Use one cancellation owner for fanout execution and pre-publication validation.
    /// # Errors
    /// Propagates cancellation and the ordinary fanout/source/adapter failures.
    pub fn write_many_controlled(
        &self,
        targets: &[(std::path::PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<Vec<WrittenVortexRelational>> {
        self.validate_batch_provider(false)?;
        let mut written = super::super::output_fanout::write(
            targets,
            overwrite,
            |path| {
                cancellation.check()?;
                for source in &self.sources {
                    source.validate_generation()?;
                    if source.aliases_file(path)? {
                        return Err(failed("source and output must be different files"));
                    }
                }
                #[cfg(feature = "universal-format-io")]
                for source in &self.preparation_sources {
                    source.validate_generation()?;
                    source.validate_destination(path)?;
                }
                Ok(())
            },
            |path, format| self.write_controlled(path, format, false, cancellation),
        )?;
        for (result, (path, _)) in written.iter_mut().zip(targets) {
            result.output.output_path = path.display().to_string();
        }
        Ok(written)
    }

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
        self.write_with_input(path, format, overwrite, cancellation, None)
    }

    pub(super) fn write_with_input(
        &self,
        path: &std::path::Path,
        format: VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<&mut super::batch_input::Provider<'_>>,
    ) -> Result<WrittenVortexRelational> {
        self.validate_batch_provider(input.is_some())?;
        let mut written =
            self.session
                .with_sources_execution(&self.sources, cancellation, |context| {
                    self.with_bound_root(context, input, |root, metrics| {
                        let source = self.writer_source()?;
                        // This request describes only the terminal adapter's projection of the
                        // completed native columns. The relational tree has its own certificate.
                        let _declaration_metadata = crate::native_payload_schema::reserve_names(
                            context.memory(),
                            root.fields.len(),
                            |index| root.fields[index].0.as_str(),
                        )?;
                        let request = VortexQueryPrimitiveRequest::project(
                            source,
                            shardloom_plan::ProjectionRequest::columns(
                                root.fields
                                    .iter()
                                    .map(|(name, _)| ColumnRef::new(name))
                                    .collect::<Result<Vec<_>>>()?,
                            ),
                        );
                        let plan = super::super::native_sink::NativeSinkPlan::produced_sources(
                            self.session.clone(),
                            super::DType::struct_(
                                root.fields.clone(),
                                super::Nullability::NonNullable,
                            ),
                            root.upper_rows(&self.sources).unwrap_or(u64::MAX),
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
                        let completed = self.consume_bound(
                            root,
                            context,
                            metrics,
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
                        let mut output = super::super::completed_result::write_stream_admitted(
                            plan,
                            &request,
                            path,
                            format,
                            overwrite,
                            self.policy,
                            &mut producer,
                            cancellation,
                            Some(context),
                        )?;
                        let (execution, arrays_read) = execution
                            .ok_or_else(|| failed("relational producer did not complete"))?;
                        debug_assert_eq!(execution.output_rows, output.rows_written);
                        output.rows_scanned = execution.scan_rows_delivered;
                        output.arrays_read_count = arrays_read;
                        Ok(WrittenVortexRelational { execution, output })
                    })
                })?;
        written.execution.runtime = self.snapshot();
        Ok(written)
    }

    fn writer_source(&self) -> Result<DatasetUri> {
        if let Some(path) = self.source_paths.first() {
            return DatasetUri::new(path.display().to_string());
        }
        self.memory_sources
            .first()
            .map(|(uri, _)| uri.clone())
            .or_else(|| self.batch_source.as_ref().map(|(uri, _)| uri.clone()))
            .ok_or_else(|| failed("relational source is absent"))
    }
}
