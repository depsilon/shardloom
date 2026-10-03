//! Generation-bound native projection/filter arrays streamed into a native sink.

use super::{
    DiagnosticSeverity, Result, ShardLoomError, VortexExpressionProjectionRequest,
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionStatus,
    VortexLocalPrimitivePhysicalPolicyReport, VortexLocalPrimitiveRowExportPushdownEvidence,
    VortexLocalPrimitiveRowExportReport, VortexLocalPrimitiveStateBudgetReport,
    VortexNativeArraySinkEvidence, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    bind_vortex_scan_expr, disabled_row_export_evidence, native_flat_layout, predicate_field_expr,
    prepare_output_target, projected_column_names, row_export_scan_plan, temporary_output_path,
    usize_to_u64, vortex_error,
};
use crate::VortexStructuredProjectionExpr;
use crate::resident_session::{
    NativeExecutionContext, PreparedVortexSource, ResidentVortexSession,
};
use sha2::{Digest, Sha256};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    fmt::Write as _,
    fs,
    io::{Read as _, Seek as _, SeekFrom},
    path::{Path, PathBuf},
};
use vortex::{
    array::{
        VortexSessionExecute as _,
        dtype::{DType, Nullability},
    },
    editions::{ComponentKind, EditionSessionExt as _},
    expr::{BoundExpression, pack},
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::scan::split_by::SplitBy,
};

const SCAN_ROWS: usize = 8192;
const METADATA_BYTES_PER_CHUNK: u64 = 8192;

pub(super) struct NativeSinkPlan {
    pub(super) session: ResidentVortexSession,
    pub(super) source: NativeSinkInput,
    source_path: Option<PathBuf>,
    pub(super) projection: Option<BoundExpression>,
    pub(super) filter: Option<BoundExpression>,
    pub(super) dtype: DType,
    pub(super) columns: Vec<String>,
    pub(super) row_count: u64,
    pub(super) limit: Option<u64>,
    pub(super) metadata_pruned: bool,
}

pub(super) enum NativeSinkInput {
    Source(PreparedVortexSource),
    Completed(crate::resident_session::OwnedVortexResultBatch),
    Produced {
        session: ResidentVortexSession,
        sources: Vec<PreparedVortexSource>,
        #[cfg(feature = "universal-format-io")]
        preparations: Vec<std::sync::Arc<crate::prepared_source_binding::LocalPreparationIdentity>>,
    },
}

pub(super) type ArrayProducer<'a> = dyn FnMut(
        &NativeExecutionContext<'_>,
        usize,
        &mut dyn FnMut(vortex::array::ArrayRef) -> Result<bool>,
    ) -> Result<()>
    + 'a;

impl NativeSinkInput {
    pub(super) fn validate_generation(&self) -> Result<()> {
        match self {
            // Complete owned output was exposed only after final source
            // validation. Its lifetime no longer depends on a source file.
            Self::Completed(_) => Ok(()),
            Self::Source(source) => source.validate_generation(),
            Self::Produced { sources, .. } => {
                sources
                    .iter()
                    .try_for_each(PreparedVortexSource::validate_generation)?;
                #[cfg(feature = "universal-format-io")]
                if let Self::Produced { preparations, .. } = self {
                    for source in preparations {
                        source.validate_generation()?;
                    }
                }
                Ok(())
            }
        }
    }

    pub(super) fn is_source(&self) -> bool {
        matches!(self, Self::Source(_))
    }

    pub(super) fn is_produced(&self) -> bool {
        matches!(self, Self::Produced { .. })
    }

    pub(super) fn with_native_execution_or_admitted<T>(
        &self,
        cancellation: &CancellationToken,
        admitted: Option<&NativeExecutionContext<'_>>,
        execute: impl FnOnce(
            Option<&vortex::file::VortexFile>,
            &NativeExecutionContext<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        cancellation.check()?;
        if let Some(context) = admitted {
            return match self {
                Self::Produced {
                    session, sources, ..
                } if !sources.is_empty() => {
                    session.with_admitted_sources_execution(sources, context, |context| {
                        let result = execute(None, context)?;
                        self.validate_generation()?;
                        Ok(result)
                    })
                }
                _ => Err(sink_error(
                    "shared admission requires a generation-bound produced result",
                )),
            };
        }
        match self {
            Self::Source(source) => source
                .with_native_execution_controlled(cancellation, |file, context| {
                    execute(Some(file), context)
                }),
            Self::Completed(result) => result
                .retained_session()
                .with_native_execution_context(cancellation, |context| execute(None, context)),
            Self::Produced {
                session, sources, ..
            } => {
                if sources.is_empty() {
                    session.with_owned_execution(cancellation, |context| {
                        let result = execute(None, context)?;
                        self.validate_generation()?;
                        Ok(result)
                    })
                } else {
                    session.with_sources_execution(sources, cancellation, |context| {
                        let result = execute(None, context)?;
                        self.validate_generation()?;
                        Ok(result)
                    })
                }
            }
        }
    }
}

pub(super) fn try_execute(
    request: &VortexQueryPrimitiveRequest,
    source_path: &Path,
    output_path: &Path,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<VortexLocalPrimitiveRowExportReport>> {
    let Some(plan) = prepare(request, source_path, policy)? else {
        return Ok(None);
    };
    plan.write(request, output_path, allow_overwrite, policy)
        .map(Some)
}

pub(super) fn prepare(
    request: &VortexQueryPrimitiveRequest,
    source_path: &Path,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<NativeSinkPlan>> {
    prepare_with_source(request, source_path, policy, None)
}

pub(super) fn prepare_from_source(
    request: &VortexQueryPrimitiveRequest,
    source: PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<NativeSinkPlan>> {
    let uri = request
        .source_uri
        .as_ref()
        .ok_or_else(|| sink_error("source URI is absent"))?;
    let path = super::local_vortex_path(uri, request.kind)?
        .ok_or_else(|| sink_error("requires one local native source"))?;
    prepare_with_source(request, &path, policy, Some(source))
}

fn is_source_projection(request: &VortexQueryPrimitiveRequest) -> bool {
    request.kind == VortexQueryPrimitiveKind::ExpressionProjectRows
        && request
            .expression_projection
            .as_ref()
            .is_none_or(VortexExpressionProjectionRequest::is_empty)
        && request
            .structured_projection
            .as_ref()
            .is_some_and(|projection| {
                !projection.is_empty()
                    && projection.columns.iter().all(|column| {
                        matches!(column.expr, VortexStructuredProjectionExpr::SourceColumn(_))
                    })
            })
}

fn prepare_with_source(
    request: &VortexQueryPrimitiveRequest,
    source_path: &Path,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source: Option<PreparedVortexSource>,
) -> Result<Option<NativeSinkPlan>> {
    let simple = matches!(
        request.kind,
        VortexQueryPrimitiveKind::ProjectColumns
            | VortexQueryPrimitiveKind::FilterPredicate
            | VortexQueryPrimitiveKind::FilterAndProject
    );
    let source_projection = is_source_projection(request);
    if !simple && !source_projection {
        if request.kind == VortexQueryPrimitiveKind::ExpressionProjectRows
            && request.predicate.is_some()
        {
            return Err(sink_error(
                "filtered expression output requires source-column projections; constructed expressions and rewrites are not admitted",
            ));
        }
        return Ok(None);
    }
    if request.diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        ) || diagnostic.fallback.attempted
    }) {
        return Err(sink_error("request diagnostics prohibit native output"));
    }
    if request.source_order_limit == Some(0) {
        return Err(sink_error("source-order limit must be positive"));
    }
    let (session, source) = if let Some(source) = source {
        let session = super::prepared_dispatch::source_session(&source, request, Some(policy))?;
        (session, source)
    } else {
        let session = ResidentVortexSession::new(
            policy.resource_envelope().memory_budget_bytes,
            policy.max_parallelism,
        )?;
        let source = session.prepare_file(source_path)?;
        (session, source)
    };
    let scan_plan = row_export_scan_plan(request, source.dtype())?;
    if scan_plan.residual_predicate.is_some() {
        if source_projection {
            return Err(sink_error(
                "source-column expression output requires a native source predicate; residual predicates are not admitted",
            ));
        }
        return Ok(None);
    }
    let (projection, columns) = if source_projection {
        let (projection, columns) = prepare_source_projection(request, &source)?;
        (Some(projection), columns)
    } else {
        (
            scan_plan
                .projection
                .as_ref()
                .map(|projection| bind_vortex_scan_expr(source.file(), projection))
                .transpose()?,
            projected_column_names(source.dtype(), &request.projection, request.kind)?,
        )
    };
    let dtype = projection.as_ref().map_or_else(
        || source.dtype().clone(),
        |projection| projection.dtype().clone(),
    );
    let filter = scan_plan
        .filter
        .as_ref()
        .map(|filter| bind_vortex_scan_expr(source.file(), filter))
        .transpose()?;
    let metadata_pruned = scan_plan
        .filter
        .as_ref()
        .map(|filter| source.file().can_prune(filter).map_err(vortex_error))
        .transpose()?
        .unwrap_or(false);
    let row_count = source.file().row_count();
    Ok(Some(NativeSinkPlan {
        session,
        source: NativeSinkInput::Source(source),
        source_path: Some(fs::canonicalize(source_path).map_err(vortex_error)?),
        projection,
        filter,
        dtype,
        columns,
        row_count,
        limit: request.source_order_limit.map(usize_to_u64).transpose()?,
        metadata_pruned,
    }))
}

fn prepare_source_projection(
    request: &VortexQueryPrimitiveRequest,
    source: &PreparedVortexSource,
) -> Result<(BoundExpression, Vec<String>)> {
    let structured = request
        .structured_projection
        .as_ref()
        .ok_or_else(|| sink_error("structured projection disappeared"))?;
    let mut fields = Vec::with_capacity(structured.columns.len());
    let mut names = std::collections::BTreeSet::new();
    for column in &structured.columns {
        if column.output_column.is_empty() || !names.insert(column.output_column.clone()) {
            return Err(sink_error("output columns must be unique and nonempty"));
        }
        let VortexStructuredProjectionExpr::SourceColumn(source_column) = &column.expr else {
            return Err(sink_error("only native source projections are admitted"));
        };
        fields.push((
            column.output_column.clone(),
            predicate_field_expr(source.dtype(), source_column.as_str(), request.kind)?.0,
        ));
    }
    Ok((
        bind_vortex_scan_expr(source.file(), &pack(fields, Nullability::NonNullable))?,
        structured.output_columns(),
    ))
}

impl NativeSinkPlan {
    #[cfg(feature = "universal-format-io")]
    pub(super) fn with_preparation_sources(
        mut self,
        sources: Vec<std::sync::Arc<crate::prepared_source_binding::LocalPreparationIdentity>>,
    ) -> Self {
        if let NativeSinkInput::Produced { preparations, .. } = &mut self.source {
            *preparations = sources;
        }
        self
    }

    pub(super) fn produced(
        session: ResidentVortexSession,
        dtype: DType,
        upper_rows: u64,
        source_path: Option<PathBuf>,
        source: Option<PreparedVortexSource>,
    ) -> Result<Self> {
        Self::produced_sources(
            session,
            dtype,
            upper_rows,
            source_path,
            source.into_iter().collect(),
        )
    }

    pub(super) fn produced_sources(
        session: ResidentVortexSession,
        dtype: DType,
        upper_rows: u64,
        source_path: Option<PathBuf>,
        sources: Vec<PreparedVortexSource>,
    ) -> Result<Self> {
        let columns = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| sink_error("produced output requires a struct dtype"))?
            .names()
            .iter()
            .map(ToString::to_string)
            .collect();
        Ok(Self {
            source: NativeSinkInput::Produced {
                session: session.clone(),
                sources,
                #[cfg(feature = "universal-format-io")]
                preparations: Vec::new(),
            },
            session,
            source_path,
            projection: None,
            filter: None,
            dtype,
            columns,
            row_count: upper_rows.max(1),
            limit: None,
            metadata_pruned: false,
        })
    }

    pub(super) fn validate_destination(&self, path: &Path) -> Result<()> {
        self.source.validate_generation()?;
        #[cfg(feature = "universal-format-io")]
        if let NativeSinkInput::Produced { preparations, .. } = &self.source {
            for source in preparations {
                source.validate_destination(path)?;
            }
        }
        let same_file = match &self.source {
            NativeSinkInput::Source(source) => source.aliases_file(path)?,
            NativeSinkInput::Completed(_) => false,
            NativeSinkInput::Produced { sources, .. } => {
                let mut same_file = false;
                for source in sources {
                    same_file |= source.aliases_file(path)?;
                }
                same_file
            }
        };
        if same_file
            || self.source_path.as_ref().is_some_and(|source_path| {
                fs::canonicalize(path).is_ok_and(|path| path == *source_path)
                    || identity(path).is_ok_and(|output| {
                        identity(source_path).is_ok_and(|source| source == output)
                    })
            })
        {
            return Err(sink_error("source and output must be different files"));
        }
        Ok(())
    }

    pub(super) fn consume(
        &self,
        file: Option<&vortex::file::VortexFile>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        producer: Option<&mut ArrayProducer<'_>>,
        mut consume: impl FnMut(vortex::array::ArrayRef) -> Result<bool>,
    ) -> Result<()> {
        if self.source.is_produced() {
            return producer.ok_or_else(|| sink_error("computed result producer is absent"))?(
                context,
                batch_rows,
                &mut |array| {
                    context.check_cancelled()?;
                    if array.dtype() != &self.dtype {
                        return Err(sink_error("computed result changed the declared dtype"));
                    }
                    let keep = consume(array)?;
                    context.check_cancelled()?;
                    if !keep {
                        return Err(sink_error(
                            "computed result consumer stopped before complete output",
                        ));
                    }
                    Ok(true)
                },
            );
        }
        let arrays = self.arrays(file, context.runtime(), batch_rows)?;
        #[cfg(test)]
        let arrays = overlap_timing::instrument(arrays);
        for array in arrays {
            context.check_cancelled()?;
            if !consume(array?)? {
                break;
            }
        }
        context.check_cancelled()
    }

    pub(super) fn completed(
        result: crate::resident_session::OwnedVortexResultBatch,
    ) -> Result<Self> {
        result.validate_schema_and_rows()?;
        let dtype = result.dtype().clone();
        let fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| sink_error("completed result requires a struct dtype"))?;
        let columns = fields.names().iter().map(ToString::to_string).collect();
        Ok(Self {
            session: result.retained_session(),
            row_count: result.row_count(),
            source: NativeSinkInput::Completed(result),
            source_path: None,
            projection: None,
            filter: None,
            dtype,
            columns,
            limit: None,
            metadata_pruned: false,
        })
    }

    pub(super) fn arrays<'a>(
        &'a self,
        file: Option<&'a vortex::file::VortexFile>,
        runtime: &'a vortex::io::runtime::current::CurrentThreadRuntime,
        batch_rows: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<vortex::array::ArrayRef>> + 'a>> {
        if let NativeSinkInput::Completed(result) = &self.source {
            return Ok(Box::new(result.arrays().iter().flat_map(move |array| {
                (0..array.len()).step_by(batch_rows).map(move |start| {
                    array
                        .slice(start..array.len().min(start + batch_rows))
                        .map_err(vortex_error)
                })
            })));
        }
        let file = file.ok_or_else(|| sink_error("native source file is absent"))?;
        let mut scan = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_split_by(SplitBy::RowCount(batch_rows))
            .with_concurrency(1);
        if let Some(projection) = self.projection.clone() {
            scan = scan.with_projection(projection);
        }
        if let Some(filter) = self.filter.clone() {
            scan = scan.with_filter(filter);
        }
        if self.filter.is_none()
            && let Some(limit) = self.limit
        {
            scan = scan.with_limit(limit);
        }
        Ok(Box::new(
            scan.into_array_iter(runtime)
                .map_err(vortex_error)?
                .map(|array| array.map_err(vortex_error)),
        ))
    }

    // Keep reservation, staged generation validation, publication, and evidence
    // in one operation so their lifetime and commit ordering remain visible.
    #[allow(clippy::too_many_lines)]
    pub(super) fn write(
        self,
        request: &VortexQueryPrimitiveRequest,
        output_path: &Path,
        allow_overwrite: bool,
        policy: VortexLocalPrimitiveExecutionPolicy,
    ) -> Result<VortexLocalPrimitiveRowExportReport> {
        self.write_produced(
            request,
            output_path,
            allow_overwrite,
            policy,
            None,
            &CancellationToken::default(),
            None,
        )
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub(super) fn write_produced(
        self,
        request: &VortexQueryPrimitiveRequest,
        output_path: &Path,
        allow_overwrite: bool,
        policy: VortexLocalPrimitiveExecutionPolicy,
        producer: Option<&mut ArrayProducer<'_>>,
        cancellation: &CancellationToken,
        admitted: Option<&NativeExecutionContext<'_>>,
    ) -> Result<VortexLocalPrimitiveRowExportReport> {
        self.validate_destination(output_path)?;
        let max_chunks = if self.source.is_produced() {
            usize::try_from(self.row_count)
                .map_err(|_| sink_error("computed chunk bound overflow"))?
        } else if let NativeSinkInput::Completed(result) = &self.source {
            result.arrays().iter().try_fold(0_usize, |chunks, array| {
                chunks
                    .checked_add(array.len().div_ceil(SCAN_ROWS))
                    .ok_or_else(|| sink_error("completed chunk count overflow"))
            })?
        } else if self.metadata_pruned {
            0
        } else {
            let chunks = if self.filter.is_none() {
                self.row_count
                    .min(self.limit.unwrap_or(u64::MAX))
                    .div_ceil(SCAN_ROWS as u64)
            } else {
                self.row_count
                    .div_ceil(SCAN_ROWS as u64)
                    .min(self.limit.unwrap_or(u64::MAX))
            };
            usize::try_from(chunks).map_err(|_| sink_error("source chunk count overflow"))?
        };
        let metadata_bytes = if self.source.is_produced() {
            128 * 1024
        } else {
            u64::try_from(max_chunks)
                .ok()
                .and_then(|chunks| chunks.checked_mul(METADATA_BYTES_PER_CHUNK))
                .and_then(|bytes| bytes.checked_add(128 * 1024))
                .ok_or_else(|| sink_error("native sink metadata reservation overflow"))?
        };
        let metadata = std::sync::Arc::new(std::sync::Mutex::new(
            self.session.memory().reserve(metadata_bytes)?,
        ));
        let mut output = OwnedOutput::new(output_path, allow_overwrite)?;
        let filter_applied = self.filter.is_some();
        let projection_applied = self.projection.is_some();
        let mut rows_written = 0_u64;
        let mut arrays_read = 0_usize;
        let mut arrays_submitted = 0_u64;
        let mut max_rows = 0_usize;
        let mut native_bytes = 0_u64;
        let mut matched_rows_observed = 0_u64;
        let mut stopped_at_limit = false;
        self.source.with_native_execution_or_admitted(
            cancellation,
            admitted,
            |file, execution| {
                let session = execution.native_session();
                let runtime = execution.runtime();
                let allowed_encodings = session
                    .enabled_component_ids(ComponentKind::Array)
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut context = session.create_execution_ctx();
                let strategy = if self.source.is_produced() {
                    native_flat_layout::SequentialNativeFlatLayout::accounted_strategy(
                        max_chunks,
                        std::sync::Arc::clone(&metadata),
                    )
                } else {
                    native_flat_layout::SequentialNativeFlatLayout::strategy(max_chunks)
                };
                let mut writer = session
                    .write_options()
                    .with_strategy(strategy)
                    .with_file_statistics(Vec::new())
                    .blocking(runtime)
                    .writer(&mut output.file, self.dtype.clone());
                let mut writer_failed = false;
                let delivery = if self.source.is_produced()
                    || (!self.metadata_pruned && self.row_count > 0)
                {
                    self.consume(file, execution, SCAN_ROWS, producer, |array| {
                        arrays_read += 1;
                        max_rows = max_rows.max(array.len());
                        if array.len() > SCAN_ROWS {
                            return Err(sink_error(
                                "native source scan exceeded admitted row batch",
                            ));
                        }
                        if array.is_empty() {
                            return Ok(true);
                        }
                        matched_rows_observed = matched_rows_observed
                            .checked_add(usize_to_u64(array.len())?)
                            .ok_or_else(|| sink_error("native sink observed row count overflow"))?;
                        let remaining = self.limit.map_or(usize::MAX, |limit| {
                            usize::try_from(limit.saturating_sub(rows_written))
                                .unwrap_or(usize::MAX)
                        });
                        let array = if array.len() > remaining {
                            array.slice(0..remaining).map_err(vortex_error)?
                        } else {
                            array
                        };
                        // File editions exclude lazy filter/slice/expression arrays.
                        // Preserve serializable encoded batches; only pending native
                        // work is completed into provider-owned canonical buffers.
                        #[cfg(test)]
                        let started = overlap_timing::clock();
                        let (array, _) = native_flat_layout::complete_for_serialization(
                            array,
                            &allowed_encodings,
                            &mut context,
                        )
                        .map_err(vortex_error)?;
                        #[cfg(test)]
                        overlap_timing::record("serialization_completion", started);
                        rows_written = rows_written
                            .checked_add(usize_to_u64(array.len())?)
                            .ok_or_else(|| sink_error("native sink row count overflow"))?;
                        native_bytes = native_bytes
                            .checked_add(array.nbytes())
                            .ok_or_else(|| sink_error("native sink logical byte overflow"))?;
                        #[cfg(test)]
                        let started = overlap_timing::clock();
                        if let Err(error) = writer.push(array) {
                            writer_failed = true;
                            return Err(vortex_error(error));
                        }
                        #[cfg(test)]
                        {
                            overlap_timing::record("writer_push", started);
                            overlap_timing::buffered(writer.buffered_bytes());
                        }
                        arrays_submitted += 1;
                        if self.limit == Some(rows_written) {
                            stopped_at_limit = true;
                            return Ok(false);
                        }
                        Ok(true)
                    })
                } else {
                    Ok(())
                };
                if let Err(error) = delivery {
                    // The upstream writer owns a spawned layout task. Dropping
                    // a healthy push writer can leave queued buffer owners in
                    // that task until the resident runtime advances again.
                    // Drain its bounded accepted prefix into the staging file;
                    // publication is still forbidden and the original error wins.
                    // A failed push already consumed the terminal writer future
                    // and must not await that fused future a second time.
                    if !writer_failed {
                        let _ = writer.finish();
                    }
                    return Err(error);
                }
                #[cfg(test)]
                let started = overlap_timing::clock();
                let summary = writer.finish().map_err(vortex_error)?;
                #[cfg(test)]
                overlap_timing::record("writer_finish", started);
                let metadata_bytes = metadata
                    .lock()
                    .map_err(|_| sink_error("metadata owner poisoned"))?
                    .bytes();
                if summary.row_count() != rows_written
                    || summary.footer().approx_byte_size().is_none_or(|bytes| {
                        u64::try_from(bytes).unwrap_or(u64::MAX) > metadata_bytes
                    })
                {
                    return Err(sink_error(
                        "native output row count or footer exceeded admission",
                    ));
                }
                #[cfg(test)]
                let started = overlap_timing::clock();
                output.file.sync_all().map_err(vortex_error)?;
                let reopened = runtime
                    .block_on(session.open_options().open_path(&output.temporary))
                    .map_err(vortex_error)?;
                if reopened.dtype() != &self.dtype || reopened.row_count() != rows_written {
                    return Err(sink_error(
                        "native output dtype or row count validation failed",
                    ));
                }
                #[cfg(test)]
                overlap_timing::record("sync_and_reopen", started);
                Ok(())
            },
        )?;
        #[cfg(test)]
        let started = overlap_timing::clock();
        let checksum = output.checksum()?;
        cancellation.check()?;
        self.source.validate_generation()?;
        output.commit()?;
        let metadata_bytes = metadata
            .lock()
            .map_err(|_| sink_error("metadata owner poisoned"))?
            .bytes();
        #[cfg(test)]
        overlap_timing::record("checksum_and_commit", started);
        // An unfiltered footer proves the pre-limit count without reading all
        // rows. Otherwise early limit termination proves only the matches
        // already observed; exhausting the stream proves the exact count.
        let pre_limit_result_row_count = if self.source.is_produced() {
            rows_written
        } else if self.metadata_pruned {
            0
        } else if self.filter.is_none() {
            self.row_count
        } else {
            matched_rows_observed
        };
        let pre_limit_result_row_count_exact = self.filter.is_none() || !stopped_at_limit;
        let mut evidence = disabled_row_export_evidence();
        evidence.pushdown = VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: filter_applied,
            projection_pushdown_applied: projection_applied,
            source_order_limit_applied: self.limit.is_some(),
        };
        evidence.upstream_scan_called =
            self.source.is_source() && !self.metadata_pruned && self.row_count > 0;
        evidence.side_effects.data_read = self.source.is_source() && arrays_read > 0;
        // Filter kernels and the native serializer may canonicalize encodings.
        // Zero adapter scalarization is not a claim that all provider work avoids
        // decoding or allocating; conservatively expose that native boundary.
        evidence.side_effects.data_decoded = arrays_read > 0;
        evidence.side_effects.data_materialized = arrays_read > 0;
        // The selected sink boundary is reported even when footer pruning or an
        // empty result means no data is actually decoded or materialized.
        evidence.materialization_boundary_reported = true;
        evidence.side_effects.write_io = true;
        evidence.native_array_sink = Some(VortexNativeArraySinkEvidence {
            native_arrays_submitted: arrays_submitted,
            native_array_logical_bytes: native_bytes,
            adapter_payload_bytes_copied: 0,
            scalar_values_materialized: 0,
            scan_row_bound: SCAN_ROWS,
            writer_input_batch_bound: 3,
            peak_reserved_bytes: self.session.snapshot().memory.peak_reserved_bytes,
            metadata_reserved_bytes: metadata_bytes,
            pre_limit_result_row_count,
            pre_limit_result_row_count_exact,
            source_generation_validated: true,
            dtype_and_row_count_validated: true,
            output_sha256: checksum,
            metadata_fidelity: "native_dtype_validity_preserved;native_serializer_selects_serializable_encodings;source_layout_user_metadata_not_copied;file_statistics_not_recomputed",
            compatibility: None,
        });
        Ok(VortexLocalPrimitiveRowExportReport {
            status: VortexLocalPrimitiveExecutionStatus::Executed,
            primitive_kind: request.kind,
            output_path: output_path.display().to_string(),
            output_format: "vortex",
            rows_scanned: self.row_count,
            rows_written,
            pre_limit_result_row_count,
            projected_columns: self.columns,
            arrays_read_count: arrays_read,
            max_chunk_rows: max_rows,
            resource_envelope: policy.resource_envelope(),
            physical_policy: VortexLocalPrimitivePhysicalPolicyReport::not_selected(),
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: 1,
            source_order_limit_requested: self.limit,
            state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
            evidence,
            diagnostics: Vec::new(),
        })
    }
}

pub(crate) struct OwnedOutput {
    target: PathBuf,
    pub(crate) temporary: PathBuf,
    pub(crate) file: fs::File,
    identity: (u64, u64),
    committed: bool,
}
impl OwnedOutput {
    pub(crate) fn new(target: &Path, allow_overwrite: bool) -> Result<Self> {
        Self::new_with_after_preflight(target, allow_overwrite, || Ok(()))
    }

    fn new_with_after_preflight(
        target: &Path,
        allow_overwrite: bool,
        after_preflight: impl FnOnce() -> Result<()>,
    ) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
        let temporary = temporary_output_path(target)?;
        // POSIX rename cannot compare an expected destination generation as
        // part of replacement. A preceding stat or an advisory lock would not
        // protect another writer's output. Admit only atomic create-if-absent,
        // even when the caller permits overwrite; never redirect to a weaker
        // publication path. Check here so the shared preflight does not suggest
        // enabling allow_overwrite for a capability this sink cannot provide.
        match fs::symlink_metadata(target) {
            Ok(_) => {
                return Err(sink_error(
                    "atomic generation-conditional replacement is unavailable for an existing destination; choose a new output path",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(vortex_error(error)),
        }
        prepare_output_target(target, &temporary, allow_overwrite)?;
        after_preflight()?;
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(vortex_error)?;
        let metadata = file.metadata().map_err(vortex_error)?;
        let owned_identity = (metadata.dev(), metadata.ino());
        Ok(Self {
            target: target.to_path_buf(),
            temporary,
            file,
            identity: owned_identity,
            committed: false,
        })
    }
    pub(crate) fn checksum(&mut self) -> Result<String> {
        self.file.rewind().map_err(vortex_error)?;
        let mut digest = Sha256::new();
        // Covered by the operation's existing 128 KiB metadata/scratch lease.
        let mut block = vec![0_u8; 64 * 1024].into_boxed_slice();
        loop {
            let count = self.file.read(&mut block).map_err(vortex_error)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
        }
        self.file.seek(SeekFrom::End(0)).map_err(vortex_error)?;
        let mut encoded = String::with_capacity(64);
        for byte in digest.finalize() {
            write!(&mut encoded, "{byte:02x}").expect("writing digest hex cannot fail");
        }
        Ok(encoded)
    }
    pub(crate) fn commit(&mut self) -> Result<()> {
        self.commit_with_unlink(|path| fs::remove_file(path))
    }

    fn commit_with_unlink(
        &mut self,
        unlink_temporary: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<()> {
        if identity(&self.temporary)? != self.identity {
            return Err(sink_error("native output temporary identity changed"));
        }
        fs::hard_link(&self.temporary, &self.target).map_err(vortex_error)?;
        if let Err(error) = unlink_temporary(&self.temporary) {
            // The complete file was published. Do not attempt a stat-then-unlink
            // rollback: that would have the same destination race as overwrite.
            // Drop still attempts cleanup of our staging file, but never removes
            // the destination. Report the partial commit for explicit recovery.
            return Err(sink_error(&format!(
                "output was published at {} but temporary unlink failed ({error}); destination preserved; inspect the output before retrying with a new path",
                self.target.display()
            )));
        }
        self.committed = true;
        Ok(())
    }
}
impl Drop for OwnedOutput {
    fn drop(&mut self) {
        if !self.committed
            && identity(&self.temporary).is_ok_and(|identity| identity == self.identity)
        {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}
fn identity(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(path).map_err(vortex_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(sink_error("native output requires regular files"));
    }
    Ok((metadata.dev(), metadata.ino()))
}

fn sink_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native array sink: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_native_sink_overlap_timing.rs"]
mod overlap_timing;

#[cfg(test)]
#[path = "local_primitive_native_sink_tests.rs"]
mod tests;
