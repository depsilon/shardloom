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
use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    fs,
    io::{Read as _, Seek as _, SeekFrom},
    path::{Path, PathBuf},
};
use vortex::{
    array::{
        IntoArray as _, RecursiveCanonical, VortexSessionExecute as _,
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

struct NativeSinkPlan {
    session: ResidentVortexSession,
    source: PreparedVortexSource,
    source_path: PathBuf,
    projection: Option<BoundExpression>,
    filter: Option<BoundExpression>,
    dtype: DType,
    columns: Vec<String>,
    row_count: u64,
    limit: Option<u64>,
    metadata_pruned: bool,
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

fn prepare(
    request: &VortexQueryPrimitiveRequest,
    source_path: &Path,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<Option<NativeSinkPlan>> {
    let simple = matches!(
        request.kind,
        VortexQueryPrimitiveKind::ProjectColumns
            | VortexQueryPrimitiveKind::FilterPredicate
            | VortexQueryPrimitiveKind::FilterAndProject
    );
    let source_projection = request.kind == VortexQueryPrimitiveKind::ExpressionProjectRows
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
            });
    if !simple && !source_projection {
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
    let session = ResidentVortexSession::new(
        policy.resource_envelope().memory_budget_bytes,
        policy.max_parallelism,
    )?;
    let source = session.prepare_file(source_path)?;
    let scan_plan = row_export_scan_plan(request, source.dtype())?;
    if scan_plan.residual_predicate.is_some() {
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
        source,
        source_path: fs::canonicalize(source_path).map_err(vortex_error)?,
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
    // Keep reservation, staged generation validation, publication, and evidence
    // in one operation so their lifetime and commit ordering remain visible.
    #[allow(clippy::too_many_lines)]
    fn write(
        self,
        request: &VortexQueryPrimitiveRequest,
        output_path: &Path,
        allow_overwrite: bool,
        policy: VortexLocalPrimitiveExecutionPolicy,
    ) -> Result<VortexLocalPrimitiveRowExportReport> {
        self.source.validate_generation()?;
        if fs::canonicalize(output_path).is_ok_and(|path| path == self.source_path)
            || identity(output_path).is_ok_and(|output| {
                identity(&self.source_path).is_ok_and(|source| source == output)
            })
        {
            return Err(sink_error("source and output must be different files"));
        }
        let max_chunks = if self.metadata_pruned {
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
        let metadata_bytes = u64::try_from(max_chunks)
            .ok()
            .and_then(|chunks| chunks.checked_mul(METADATA_BYTES_PER_CHUNK))
            .and_then(|bytes| bytes.checked_add(128 * 1024))
            .ok_or_else(|| sink_error("native sink metadata reservation overflow"))?;
        let _metadata = self.session.memory().reserve(metadata_bytes)?;
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
        self.source
            .with_native_execution(|file, session, runtime| {
                let allowed_encodings = session
                    .enabled_component_ids(ComponentKind::Array)
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut context = session.create_execution_ctx();
                let strategy = native_flat_layout::SequentialNativeFlatLayout::strategy(max_chunks);
                let mut writer = session
                    .write_options()
                    .with_strategy(strategy)
                    .with_file_statistics(Vec::new())
                    .blocking(runtime)
                    .writer(&mut output.file, self.dtype.clone());
                if !self.metadata_pruned && self.row_count > 0 {
                    let mut scan = file
                        .scan()
                        .map_err(vortex_error)?
                        .with_ordered(true)
                        .with_split_by(SplitBy::RowCount(SCAN_ROWS))
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
                    for array in scan.into_array_iter(runtime).map_err(vortex_error)? {
                        let array = array.map_err(vortex_error)?;
                        arrays_read += 1;
                        max_rows = max_rows.max(array.len());
                        if array.len() > SCAN_ROWS {
                            return Err(sink_error(
                                "native source scan exceeded admitted row batch",
                            ));
                        }
                        if array.is_empty() {
                            continue;
                        }
                        matched_rows_observed = matched_rows_observed
                            .checked_add(usize_to_u64(array.len())?)
                            .ok_or_else(|| sink_error("native sink observed row count overflow"))?;
                        let remaining = self.limit.map_or(usize::MAX, |limit| {
                            usize::try_from(limit.saturating_sub(rows_written))
                                .unwrap_or(usize::MAX)
                        });
                        let mut array = if array.len() > remaining {
                            array.slice(0..remaining).map_err(vortex_error)?
                        } else {
                            array
                        };
                        // File editions exclude lazy filter/slice/expression arrays.
                        // Preserve serializable encoded batches; only pending native
                        // work is completed into provider-owned canonical buffers.
                        if array
                            .depth_first_traversal()
                            .any(|node| !allowed_encodings.contains(&node.encoding_id()))
                        {
                            array = array
                                .execute::<RecursiveCanonical>(&mut context)
                                .map_err(vortex_error)?
                                .0
                                .into_array();
                        }
                        rows_written = rows_written
                            .checked_add(usize_to_u64(array.len())?)
                            .ok_or_else(|| sink_error("native sink row count overflow"))?;
                        native_bytes = native_bytes
                            .checked_add(array.nbytes())
                            .ok_or_else(|| sink_error("native sink logical byte overflow"))?;
                        writer.push(array).map_err(vortex_error)?;
                        arrays_submitted += 1;
                        if self.limit == Some(rows_written) {
                            stopped_at_limit = true;
                            break;
                        }
                    }
                }
                let summary = writer.finish().map_err(vortex_error)?;
                if summary.row_count() != rows_written
                    || summary.footer().approx_byte_size().is_none_or(|bytes| {
                        u64::try_from(bytes).unwrap_or(u64::MAX) > metadata_bytes
                    })
                {
                    return Err(sink_error(
                        "native output row count or footer exceeded admission",
                    ));
                }
                output.file.sync_all().map_err(vortex_error)?;
                let reopened = runtime
                    .block_on(session.open_options().open_path(&output.temporary))
                    .map_err(vortex_error)?;
                if reopened.dtype() != &self.dtype || reopened.row_count() != rows_written {
                    return Err(sink_error(
                        "native output dtype or row count validation failed",
                    ));
                }
                Ok(())
            })?;
        let checksum = output.checksum()?;
        self.source.validate_generation()?;
        output.commit()?;
        // An unfiltered footer proves the pre-limit count without reading all
        // rows. Otherwise early limit termination proves only the matches
        // already observed; exhausting the stream proves the exact count.
        let pre_limit_result_row_count = if self.metadata_pruned {
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
        evidence.upstream_scan_called = !self.metadata_pruned && self.row_count > 0;
        evidence.side_effects.data_read = arrays_read > 0;
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

struct OwnedOutput {
    target: PathBuf,
    temporary: PathBuf,
    file: fs::File,
    identity: (u64, u64),
    prior_target: Option<DestinationGeneration>,
    committed: bool,
}
impl OwnedOutput {
    fn new(target: &Path, allow_overwrite: bool) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
        let temporary = temporary_output_path(target)?;
        prepare_output_target(target, &temporary, allow_overwrite)?;
        let prior_target = if fs::symlink_metadata(target).is_ok() {
            Some(destination_generation(target)?)
        } else {
            None
        };
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
            prior_target,
            committed: false,
        })
    }
    fn checksum(&mut self) -> Result<String> {
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
    fn commit(&mut self) -> Result<()> {
        self.commit_with_unlink(|path| fs::remove_file(path))
    }

    fn commit_with_unlink(
        &mut self,
        unlink_temporary: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<()> {
        if identity(&self.temporary)? != self.identity {
            return Err(sink_error("native output temporary identity changed"));
        }
        if let Some(prior) = self.prior_target {
            if destination_generation(&self.target)? != prior {
                return Err(sink_error("native output destination changed"));
            }
            fs::rename(&self.temporary, &self.target).map_err(vortex_error)?;
        } else {
            fs::hard_link(&self.temporary, &self.target).map_err(vortex_error)?;
            if let Err(error) = unlink_temporary(&self.temporary) {
                // Publication happened, but staging cleanup failed. Roll back
                // only our own new hard link; a replaced destination belongs
                // to its new owner and must remain untouched.
                if identity(&self.target).is_ok_and(|target| target == self.identity) {
                    fs::remove_file(&self.target).map_err(|rollback| sink_error(&format!(
                        "temporary unlink failed ({error}); new output rollback also failed ({rollback}); output may remain at {}",
                        self.target.display()
                    )))?;
                    return Err(sink_error(&format!(
                        "temporary unlink failed ({error}); new output publication rolled back"
                    )));
                }
                return Err(sink_error(&format!(
                    "temporary unlink failed ({error}); output identity changed and was preserved"
                )));
            }
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

type DestinationGeneration = (u64, u64, u64, (i64, i64), (i64, i64));
fn destination_generation(path: &Path) -> Result<DestinationGeneration> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(path).map_err(vortex_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(sink_error("native output requires regular files"));
    }
    Ok((
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        (metadata.mtime(), metadata.mtime_nsec()),
        (metadata.ctime(), metadata.ctime_nsec()),
    ))
}
fn sink_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native array sink: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_native_sink_tests.rs"]
mod tests;
