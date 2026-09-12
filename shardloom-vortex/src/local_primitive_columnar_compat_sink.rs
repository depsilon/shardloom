//! Bounded native-array conversion at explicit Arrow IPC / Parquet boundaries.
//!
//! Reuses the native sink's source-generation plan;
//! no row dictionaries, new execution engine, or full-provider allocator claim.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionStatus, VortexLocalPrimitivePhysicalPolicyReport,
    VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportPushdownEvidence,
    VortexLocalPrimitiveRowExportReport, VortexLocalPrimitiveStateBudgetReport,
    VortexNativeArraySinkEvidence, VortexQueryPrimitiveRequest, disabled_row_export_evidence,
    logical_field_from_native_array, native_sink, usize_to_u64, vortex_error,
};
use arrow_array::{Array as _, RecordBatch, StructArray as ArrowStructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    fs::File,
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use vortex::{
    array::{
        ArrayRef, Columnar, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{StructArray, VarBinViewArray},
        dtype::{DType, FieldNames, Nullability, PType},
        validity::Validity,
    },
    arrow::ArrowSessionExt as _,
};

#[derive(Clone)]
pub(super) struct CompatibilityLimits {
    pub source_rows: u64,
    pub output_rows: u64,
    pub columns: usize,
    pub batch_rows: usize,
    pub batches: usize,
    pub string_bytes: usize,
    pub arrow_batch_bytes: u64,
    pub file_bytes: u64,
    pub cancellation: Arc<AtomicBool>,
}

impl Default for CompatibilityLimits {
    fn default() -> Self {
        Self {
            source_rows: 65_536,
            output_rows: 65_536,
            columns: 32,
            batch_rows: 2048,
            batches: 256,
            string_bytes: 64 * 1024,
            arrow_batch_bytes: 8 * 1024 * 1024,
            file_bytes: 128 * 1024 * 1024,
            cancellation: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl CompatibilityLimits {
    fn check(&self) -> Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(error("cancelled"));
        }
        if self.source_rows == 0
            || self.output_rows == 0
            || self.columns == 0
            || self.batch_rows == 0
            || self.batches == 0
            || self.string_bytes == 0
            || self.arrow_batch_bytes == 0
            || self.file_bytes == 0
        {
            return Err(error("all admission bounds must be positive"));
        }
        if self.source_rows > 65_536
            || self.output_rows > 65_536
            || self.columns > 32
            || self.batch_rows > 2048
            || self.batches > 256
            || self.string_bytes > 64 * 1024
            || self.arrow_batch_bytes > 8 * 1024 * 1024
            || self.file_bytes > 128 * 1024 * 1024
        {
            return Err(error(
                "requested bounds exceed the bounded flat-scalar candidate profile",
            ));
        }
        Ok(())
    }

    fn metadata_reservation(&self, columns: usize) -> Result<u64> {
        // Retained IPC/Parquet footer records are proportional to actual field
        // and batch bounds, independently of payload. The work envelope covers
        // admitted contiguous Arrow data, IPC serialization / PLAIN pages and
        // growth overlap, not every upstream allocation or process RSS.
        usize_to_u64(self.batches)?
            .checked_mul(usize_to_u64(columns)?)
            .and_then(|n| n.checked_mul(4096))
            .and_then(|n| n.checked_add(1024 * 1024))
            .ok_or_else(|| error("footer admission overflow"))
    }
}

pub(super) struct PreparedCompatibilityExport {
    request: VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    plan: native_sink::NativeSinkPlan,
    schema: SchemaRef,
    format: VortexLocalPrimitiveRowExportFormat,
    limits: CompatibilityLimits,
}

type CompatibilityWork = super::VortexColumnarCompatibilitySinkEvidence;

pub(super) struct CompletedCompatibilityExport {
    pub report: VortexLocalPrimitiveRowExportReport,
    #[cfg(test)]
    pub work: CompatibilityWork,
}

/// `None` means the request stays on its existing explicitly admitted route.
/// Errors after this admission are never redirected to a weaker writer.
pub(super) fn prepare(
    request: &VortexQueryPrimitiveRequest,
    path: &Path,
    format: VortexLocalPrimitiveRowExportFormat,
    policy: VortexLocalPrimitiveExecutionPolicy,
    limits: CompatibilityLimits,
) -> Result<Option<PreparedCompatibilityExport>> {
    limits.check()?;
    if !matches!(
        format,
        VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | VortexLocalPrimitiveRowExportFormat::Parquet
    ) {
        return Ok(None);
    }
    let Some(plan) = native_sink::prepare(request, path, policy)? else {
        return Ok(None);
    };
    prepare_plan(request, plan, format, policy, limits)
}

pub(super) fn prepare_plan(
    request: &VortexQueryPrimitiveRequest,
    plan: native_sink::NativeSinkPlan,
    format: VortexLocalPrimitiveRowExportFormat,
    policy: VortexLocalPrimitiveExecutionPolicy,
    limits: CompatibilityLimits,
) -> Result<Option<PreparedCompatibilityExport>> {
    limits.check()?;
    if !matches!(
        format,
        VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | VortexLocalPrimitiveRowExportFormat::Parquet
    ) {
        return Ok(None);
    }
    if plan.row_count > limits.source_rows
        || plan.limit.is_some_and(|limit| limit > limits.output_rows)
    {
        return Ok(None);
    }
    let Some(schema) = schema_for(&plan.dtype, limits.columns) else {
        return Ok(None);
    };
    Ok(Some(PreparedCompatibilityExport {
        request: request.clone(),
        policy,
        plan,
        schema,
        format,
        limits,
    }))
}

fn schema_for(dtype: &DType, max_columns: usize) -> Option<SchemaRef> {
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return None;
    };
    if fields.nfields() == 0 || fields.nfields() > max_columns {
        return None;
    }
    let mut arrow_fields = Vec::with_capacity(fields.nfields());
    let mut names = std::collections::BTreeSet::new();
    for (name, dtype) in fields.names().iter().zip(fields.fields()) {
        if name.as_ref().is_empty() || name.as_ref().len() > 256 || !names.insert(name.as_ref()) {
            return None;
        }
        let kind = match dtype {
            DType::Bool(_) => DataType::Boolean,
            DType::Utf8(_) => DataType::Utf8,
            DType::Primitive(ptype, _) => match ptype {
                PType::I8 => DataType::Int8,
                PType::I16 => DataType::Int16,
                PType::I32 => DataType::Int32,
                PType::I64 => DataType::Int64,
                PType::U8 => DataType::UInt8,
                PType::U16 => DataType::UInt16,
                PType::U32 => DataType::UInt32,
                PType::U64 => DataType::UInt64,
                PType::F32 => DataType::Float32,
                PType::F64 => DataType::Float64,
                PType::F16 => return None,
            },
            _ => return None,
        };
        arrow_fields.push(Field::new(name.as_ref(), kind, dtype.is_nullable()));
    }
    Some(Arc::new(Schema::new(arrow_fields)))
}

fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| error("byte or row counter overflow"))?;
    Ok(())
}

/// Canonicalization is native and charged where the provider allocator is used.
/// Inspect native string views before contiguous Arrow expansion. Null slots are
/// included conservatively in the byte bound; no scalar/string value is created.
fn admitted_canonical(
    array: &ArrayRef,
    names: &[String],
    ctx: &mut ExecutionCtx,
    limits: &CompatibilityLimits,
) -> Result<(ArrayRef, u64)> {
    if array.len() > limits.batch_rows {
        return Err(error("native scan exceeded batch row admission"));
    }
    let mut expanded = 0_u64;
    let mut children = Vec::with_capacity(names.len());
    for name in names {
        limits.check()?;
        let child = logical_field_from_native_array(array, name)?;
        let canonical = if matches!(child.dtype(), DType::Utf8(_)) {
            let text = child
                .execute::<VarBinViewArray>(ctx)
                .map_err(vortex_error)?;
            for view in text.views() {
                let len = usize::try_from(view.len()).map_err(vortex_error)?;
                if len > limits.string_bytes {
                    return Err(error("native string exceeds Arrow expansion admission"));
                }
                add(&mut expanded, usize_to_u64(len)?)?;
            }
            add(&mut expanded, usize_to_u64(array.len() + 1)? * 4)?;
            text.into_array()
        } else {
            let canonical = child
                .execute::<Columnar>(ctx)
                .map_err(vortex_error)?
                .into_array();
            let width = match canonical.dtype() {
                DType::Bool(_) => 1,
                DType::Primitive(ptype, _) => {
                    u64::try_from(ptype.byte_width()).map_err(vortex_error)?
                }
                _ => return Err(error("canonical field changed its admitted dtype")),
            };
            add(
                &mut expanded,
                usize_to_u64(array.len())?
                    .checked_mul(width)
                    .ok_or_else(|| error("Arrow expansion overflow"))?,
            )?;
            canonical
        };
        add(&mut expanded, usize_to_u64(array.len().div_ceil(8))? + 128)?;
        if expanded > limits.arrow_batch_bytes {
            return Err(error(
                "native fields exceed Arrow batch expansion admission",
            ));
        }
        children.push(canonical);
    }
    let packed = StructArray::try_new(
        FieldNames::from(names.iter().map(String::as_str).collect::<Vec<_>>()),
        children,
        array.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok((packed, expanded))
}

struct CappedWriter<'a> {
    file: &'a mut File,
    limits: &'a CompatibilityLimits,
    written: u64,
}
impl Write for CappedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.limits.check().map_err(std::io::Error::other)?;
        let size = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
        if self
            .written
            .checked_add(size)
            .is_none_or(|n| n > self.limits.file_bytes)
        {
            return Err(std::io::Error::other(
                "compatibility output byte limit exceeded",
            ));
        }
        let count = self.file.write(bytes)?;
        self.written += u64::try_from(count).map_err(std::io::Error::other)?;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.limits.check().map_err(std::io::Error::other)?;
        self.file.flush()
    }
}

enum Writer<'a> {
    Ipc(Box<arrow_ipc::writer::FileWriter<CappedWriter<'a>>>),
    Parquet(Box<ArrowWriter<CappedWriter<'a>>>),
}
impl<'a> Writer<'a> {
    fn new(
        format: VortexLocalPrimitiveRowExportFormat,
        schema: SchemaRef,
        file: &'a mut File,
        limits: &'a CompatibilityLimits,
    ) -> Result<Self> {
        let sink = CappedWriter {
            file,
            limits,
            written: 0,
        };
        match format {
            VortexLocalPrimitiveRowExportFormat::ArrowIpc => Ok(Self::Ipc(Box::new(
                arrow_ipc::writer::FileWriter::try_new(sink, &schema).map_err(vortex_error)?,
            ))),
            VortexLocalPrimitiveRowExportFormat::Parquet => {
                let properties = WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    .set_statistics_enabled(EnabledStatistics::None)
                    .set_write_batch_size(limits.batch_rows)
                    // Keep automatic flushing beyond one admitted batch so
                    // memory_size observes that batch before our explicit flush.
                    .set_max_row_group_row_count(Some(limits.batch_rows + 1))
                    .set_data_page_row_count_limit(limits.batch_rows)
                    .build();
                Ok(Self::Parquet(Box::new(
                    ArrowWriter::try_new(sink, schema, Some(properties)).map_err(vortex_error)?,
                )))
            }
            _ => Err(error("unsupported compatibility writer")),
        }
    }
    fn write(&mut self, batch: &RecordBatch, work: &mut CompatibilityWork) -> Result<()> {
        match self {
            Self::Ipc(writer) => writer.write(batch).map_err(vortex_error),
            Self::Parquet(writer) => {
                writer.write(batch).map_err(vortex_error)?;
                let retained = usize_to_u64(writer.memory_size())?;
                work.max_observed_parquet_in_progress_bytes =
                    work.max_observed_parquet_in_progress_bytes.max(retained);
                if retained > work.writer_reserved_bytes {
                    return Err(error(
                        "Parquet observed state exceeded the pre-admitted writer envelope",
                    ));
                }
                writer.flush().map_err(vortex_error)
            }
        }
    }
    fn finish(self) -> Result<()> {
        match self {
            Self::Ipc(mut writer) => writer.finish().map_err(vortex_error),
            Self::Parquet(writer) => (*writer).close().map(|_| ()).map_err(vortex_error),
        }
    }
}

impl PreparedCompatibilityExport {
    pub(super) fn write(
        &self,
        output: &Path,
        overwrite: bool,
    ) -> Result<CompletedCompatibilityExport> {
        self.write_observed(output, overwrite, |_| Ok(()))
    }

    #[allow(clippy::too_many_lines)] // Keep actual owner/scan/finish/commit ordering visible.
    fn write_observed(
        &self,
        output_path: &Path,
        overwrite: bool,
        mut after_batch: impl FnMut(u64) -> Result<()>,
    ) -> Result<CompletedCompatibilityExport> {
        self.limits.check()?;
        self.plan.source.validate_generation()?;
        let metadata = self
            .limits
            .metadata_reservation(self.schema.fields().len())?;
        let initial = metadata
            .checked_add(64 * 1024)
            .ok_or_else(|| error("writer admission overflow"))?;
        let mut work_owner = self.plan.session.memory().reserve(initial)?;
        let mut output = native_sink::OwnedOutput::new(output_path, overwrite)?;
        let mut work = CompatibilityWork {
            writer_reserved_bytes: initial,
            ..Default::default()
        };
        let mut rows = 0_u64;
        let mut observed = 0_u64;
        let mut arrays = 0_usize;
        let mut max_rows = 0_usize;
        let mut stopped = false;
        self.plan
            .source
            .with_native_execution(|file, session, runtime| {
                let mut writer = Writer::new(
                    self.format,
                    Arc::clone(&self.schema),
                    &mut output.file,
                    &self.limits,
                )?;
                let mut ctx = session.create_execution_ctx();
                let target = Field::new("", DataType::Struct(self.schema.fields().clone()), false);
                if !self.plan.metadata_pruned && self.plan.row_count > 0 {
                    for next in self.plan.arrays(file, runtime, self.limits.batch_rows)? {
                        self.limits.check()?;
                        let array = next?;
                        arrays = arrays
                            .checked_add(1)
                            .ok_or_else(|| error("scan counter overflow"))?;
                        max_rows = max_rows.max(array.len());
                        if array.len() > self.limits.batch_rows {
                            return Err(error("scan batch exceeds row admission"));
                        }
                        add(&mut observed, usize_to_u64(array.len())?)?;
                        if array.is_empty() {
                            continue;
                        }
                        let remaining = self
                            .plan
                            .limit
                            .map_or(u64::MAX, |limit| limit.saturating_sub(rows));
                        let retain = array
                            .len()
                            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                        if rows
                            .checked_add(usize_to_u64(retain)?)
                            .is_none_or(|total| total > self.limits.output_rows)
                        {
                            return Err(error(
                                "complete output exceeds row admission; no preview published",
                            ));
                        }
                        let array = if retain < array.len() {
                            array.slice(0..retain).map_err(vortex_error)?
                        } else {
                            array
                        };
                        if work.arrow_batches >= usize_to_u64(self.limits.batches)? {
                            return Err(error("writer batch count exceeds admission"));
                        }
                        add(&mut work.native_batches, 1)?;
                        add(&mut work.native_logical_bytes, array.nbytes())?;
                        let (canonical, expanded) =
                            admitted_canonical(&array, &self.plan.columns, &mut ctx, &self.limits)?;
                        let admitted = expanded
                            .checked_mul(8)
                            .and_then(|n| n.checked_add(metadata))
                            .ok_or_else(|| error("writer admission overflow"))?
                            .max(initial);
                        // Grow before Arrow allocates or the writer retains this
                        // batch. Keep the largest grant through writer destruction.
                        if admitted > work_owner.bytes() {
                            work_owner.resize(admitted)?;
                        }
                        work.writer_reserved_bytes =
                            work.writer_reserved_bytes.max(work_owner.bytes());
                        let arrow = session
                            .arrow()
                            .execute_arrow(canonical, Some(&target), &mut ctx)
                            .map_err(vortex_error)?;
                        let structure = arrow
                            .as_any()
                            .downcast_ref::<ArrowStructArray>()
                            .ok_or_else(|| {
                                error("Arrow provider did not return the admitted struct")
                            })?;
                        let batch = RecordBatch::try_new(
                            Arc::clone(&self.schema),
                            structure.columns().to_vec(),
                        )
                        .map_err(vortex_error)?;
                        if batch.num_rows() != retain {
                            return Err(error("Arrow provider changed row count"));
                        }
                        let actual = usize_to_u64(batch.get_array_memory_size())?;
                        // This is a post-conversion consistency check inside the
                        // pre-reserved envelope, not posthoc admission of a buffer.
                        if actual > self.limits.arrow_batch_bytes {
                            return Err(error("Arrow retained buffers exceed batch admission"));
                        }
                        add(&mut work.admitted_arrow_expansion_bytes, expanded)?;
                        work.max_arrow_batch_bytes = work.max_arrow_batch_bytes.max(actual);
                        writer.write(&batch, &mut work)?;
                        add(&mut work.arrow_batches, 1)?;
                        add(&mut rows, usize_to_u64(retain)?)?;
                        after_batch(rows)?;
                        if self.plan.limit == Some(rows) {
                            stopped = true;
                            break;
                        }
                    }
                }
                writer.finish()?;
                self.limits.check()?;
                output.file.sync_all().map_err(vortex_error)?;
                validate_reopen(
                    &output.temporary,
                    self.format,
                    &self.schema,
                    rows,
                    &self.limits,
                )?;
                Ok(())
            })?;
        work.output_bytes = output.file.metadata().map_err(vortex_error)?.len();
        let checksum = output.checksum()?;
        self.limits.check()?;
        self.plan.source.validate_generation()?;
        output.commit()?;
        let before_limit = if self.plan.metadata_pruned {
            0
        } else if self.plan.filter.is_none() {
            self.plan.row_count
        } else {
            observed
        };
        let exact = self.plan.filter.is_none() || !stopped;
        let mut evidence = disabled_row_export_evidence();
        evidence.pushdown = VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: self.plan.filter.is_some(),
            projection_pushdown_applied: self.plan.projection.is_some(),
            source_order_limit_applied: self.plan.limit.is_some(),
        };
        let scan_called =
            self.plan.source.is_source() && !self.plan.metadata_pruned && self.plan.row_count > 0;
        evidence.upstream_scan_called = scan_called;
        // Vortex drops all-false filter tasks before they yield an array. Zero
        // returned arrays therefore does not prove zero I/O or decoding. These
        // conservative scan-scope flags are not observed byte/work counters;
        // layout pruning may avoid some or all actual payload work.
        evidence.side_effects.data_read = scan_called;
        evidence.side_effects.data_decoded = scan_called;
        evidence.side_effects.data_materialized = scan_called;
        evidence.side_effects.arrow_converted = work.arrow_batches > 0;
        evidence.side_effects.write_io = true;
        evidence.materialization_boundary_reported = true;
        evidence.native_array_sink = Some(VortexNativeArraySinkEvidence {
            native_arrays_submitted: work.native_batches,
            native_array_logical_bytes: work.native_logical_bytes,
            adapter_payload_bytes_copied: 0,
            scalar_values_materialized: 0,
            scan_row_bound: self.limits.batch_rows,
            writer_input_batch_bound: 1,
            peak_reserved_bytes: self.plan.session.snapshot().memory.peak_reserved_bytes,
            metadata_reserved_bytes: metadata,
            pre_limit_result_row_count: before_limit,
            pre_limit_result_row_count_exact: exact,
            source_generation_validated: true,
            dtype_and_row_count_validated: true,
            output_sha256: checksum,
            metadata_fidelity: match self.format {
                VortexLocalPrimitiveRowExportFormat::Parquet => {
                    "compatibility_arrow_boundary;logical_dtype_names_order_validity_preserved;native_encodings_layout_statistics_user_metadata_not_copied;parquet_plain_uncompressed_no_dictionary;provider_copy_bytes_not_measured;read_decode_materialize_flags_conservative_scan_scope_not_observed_bytes"
                }
                _ => {
                    "compatibility_arrow_boundary;logical_dtype_names_order_validity_preserved;native_encodings_layout_statistics_user_metadata_not_copied;arrow_ipc_uncompressed;provider_copy_bytes_not_measured;read_decode_materialize_flags_conservative_scan_scope_not_observed_bytes"
                }
            },
            compatibility: Some(work.clone()),
        });
        Ok(CompletedCompatibilityExport {
            report: VortexLocalPrimitiveRowExportReport {
                status: VortexLocalPrimitiveExecutionStatus::Executed,
                primitive_kind: self.request.kind,
                output_path: output_path.display().to_string(),
                output_format: self.format.as_str(),
                rows_scanned: self.plan.row_count,
                rows_written: rows,
                pre_limit_result_row_count: before_limit,
                projected_columns: self.plan.columns.clone(),
                arrays_read_count: arrays,
                max_chunk_rows: max_rows,
                resource_envelope: self.policy.resource_envelope(),
                physical_policy: VortexLocalPrimitivePhysicalPolicyReport::not_selected(),
                max_parallelism_requested: self.policy.max_parallelism,
                scan_concurrency_per_worker: 1,
                source_order_limit_requested: self.plan.limit,
                state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
                evidence,
                diagnostics: Vec::new(),
            },
            #[cfg(test)]
            work,
        })
    }
}

fn validate_reopen(
    path: &Path,
    format: VortexLocalPrimitiveRowExportFormat,
    schema: &SchemaRef,
    rows: u64,
    limits: &CompatibilityLimits,
) -> Result<()> {
    limits.check()?;
    let file = File::open(path).map_err(vortex_error)?;
    match format {
        VortexLocalPrimitiveRowExportFormat::ArrowIpc => {
            let mut reader =
                arrow_ipc::reader::FileReader::try_new(file, None).map_err(vortex_error)?;
            if reader.schema().as_ref() != schema.as_ref() {
                return Err(error("IPC reopen schema differs"));
            }
            let mut actual = 0_u64;
            for batch in &mut reader {
                limits.check()?;
                let batch = batch.map_err(vortex_error)?;
                add(&mut actual, usize_to_u64(batch.num_rows())?)?;
                if actual > rows {
                    return Err(error("IPC reopen row count exceeds output"));
                }
            }
            if actual != rows {
                return Err(error("IPC reopen row count differs"));
            }
        }
        VortexLocalPrimitiveRowExportFormat::Parquet => {
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(vortex_error)?;
            if reader.schema().as_ref() != schema.as_ref()
                || u64::try_from(reader.metadata().file_metadata().num_rows())
                    .map_err(vortex_error)?
                    != rows
            {
                return Err(error("Parquet reopen schema or row count differs"));
            }
        }
        _ => return Err(error("unsupported reopen target")),
    }
    Ok(())
}

fn error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native columnar compatibility sink: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_columnar_compat_sink_tests.rs"]
mod tests;
