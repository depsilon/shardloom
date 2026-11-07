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
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use shardloom_exec::compute_pool::CancellationToken;
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
        ArrayRef, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::StructArray,
        dtype::{DType, Nullability},
        validity::Validity,
    },
    arrow::{ArrowExportOptions, ArrowSessionExt as _, CompactBuffers},
};

use crate::native_arrow_conversion as nested;
#[path = "local_primitive_columnar_compat_batches.rs"]
mod batches;
#[path = "local_primitive_columnar_compat_sizing.rs"]
mod sizing;
#[cfg(test)]
use vortex::array::{arrays::VarBinViewArray, dtype::FieldNames};

#[derive(Clone)]
pub(super) struct CompatibilityLimits {
    // Only an unlimited, unfiltered projection has the source cardinality as
    // its output cardinality. Filtered/limited plans enforce output_rows while
    // streaming, before conversion, and never publish a truncated result.
    pub source_rows: u64,
    pub output_rows: u64,
    pub columns: usize,
    pub batch_rows: usize,
    pub batches: usize,
    pub string_bytes: usize,
    pub arrow_batch_bytes: u64,
    pub file_bytes: u64,
    pub cancellation: Arc<AtomicBool>,
    streaming: bool,
    parent_cancellation: Option<CancellationToken>,
}

impl Default for CompatibilityLimits {
    fn default() -> Self {
        Self {
            source_rows: 65_536,
            output_rows: 65_536,
            columns: 128,
            batch_rows: 2048,
            batches: 256,
            string_bytes: 64 * 1024,
            arrow_batch_bytes: 8 * 1024 * 1024,
            file_bytes: 128 * 1024 * 1024,
            cancellation: Arc::new(AtomicBool::new(false)),
            streaming: false,
            parent_cancellation: None,
        }
    }
}

impl CompatibilityLimits {
    pub(super) fn streaming(upper_rows: u64, cancellation: &CancellationToken) -> Self {
        Self {
            source_rows: upper_rows.max(1),
            output_rows: upper_rows.max(1),
            batches: usize::MAX,
            columns: usize::MAX,
            file_bytes: u64::MAX,
            string_bytes: i32::MAX as usize,
            streaming: true,
            parent_cancellation: Some(cancellation.clone()),
            ..Self::default()
        }
    }

    fn check(&self) -> Result<()> {
        if let Some(parent) = &self.parent_cancellation {
            parent.check()?;
        }
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
        if (!self.streaming
            && (self.source_rows > 65_536
                || self.output_rows > 65_536
                || self.columns > 128
                || self.batches > 256
                || self.file_bytes > 128 * 1024 * 1024))
            || self.batch_rows > 2048
            || (!self.streaming
                && (self.string_bytes > 64 * 1024 || self.arrow_batch_bytes > 8 * 1024 * 1024))
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
        Self::metadata_for_batches(columns, if self.streaming { 0 } else { self.batches })
    }

    fn metadata_for_batches(columns: usize, batches: usize) -> Result<u64> {
        usize_to_u64(batches)?
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
    schema_fields: usize,
    schema_metadata: Option<shardloom_exec::live_memory::MemoryLease>,
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
    if !format.is_compatibility_binary() {
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
    if !format.is_compatibility_binary() {
        return Ok(None);
    }
    #[cfg(feature = "native-binary-objects")]
    if crate::native_binary_type::contains(&plan.dtype) {
        return Err(crate::binary_object::error(
            "OUTPUT",
            "this output format cannot carry owned binary objects; use Vortex or explicitly read the bytes into inline Binary",
        ));
    }
    if plan.filter.is_none() && plan.limit.is_none() && plan.row_count > limits.source_rows {
        return Err(error(
            "unlimited projection exceeds output row admission; supply an explicit limit",
        ));
    }
    if plan.limit.is_some_and(|limit| limit > limits.output_rows) {
        return Err(error("requested output limit exceeds row admission"));
    }
    if matches!(
        format,
        VortexLocalPrimitiveRowExportFormat::Avro | VortexLocalPrimitiveRowExportFormat::Orc
    ) && crate::native_embedding::contains(&plan.dtype)
    {
        return Err(error(
            "this output format cannot preserve embedding identity; use Vortex, Arrow IPC or Parquet",
        ));
    }
    if matches!(
        format,
        VortexLocalPrimitiveRowExportFormat::Avro | VortexLocalPrimitiveRowExportFormat::Orc
    ) && (crate::native_tensor::contains(&plan.dtype)
        || crate::native_variable_tensor::contains(&plan.dtype))
    {
        return Err(error(
            "this output format cannot preserve tensor metadata; use Vortex, Arrow IPC or Parquet",
        ));
    }
    // Check type eligibility without constructing Arrow metadata. Reserve the
    // growing root schema before its field vectors, names or lookup index.
    let Some(fields) = plan.dtype.as_struct_fields_opt() else {
        return Ok(None);
    };
    if fields.nfields() == 0
        || fields.nfields() > limits.columns
        || fields
            .fields()
            .any(|dtype| super::native_payload::metadata_bytes(&dtype).is_err())
    {
        return Ok(None);
    }
    let schema_metadata = Some(crate::native_payload_schema::reserve_schema(
        &plan.dtype,
        plan.session.memory(),
    )?);
    let Some(schema) = schema_for(&plan.dtype, limits.columns) else {
        return Ok(None);
    };
    if format == VortexLocalPrimitiveRowExportFormat::Orc {
        crate::universal_format_io::translate_orc_schema(&schema)?;
    }
    let schema_fields = plan
        .dtype
        .as_struct_fields_opt()
        .expect("schema is a struct")
        .fields()
        .map(|dtype| nested::field_count(&dtype))
        .sum();
    Ok(Some(PreparedCompatibilityExport {
        request: request.clone(),
        policy,
        plan,
        schema,
        format,
        limits,
        schema_fields,
        schema_metadata,
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
        if name.as_ref().is_empty()
            || name.as_ref().len() > i32::MAX as usize
            || !names.insert(name.as_ref())
        {
            return None;
        }
        arrow_fields.push(nested::field(name.as_ref(), &dtype)?);
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
    dtype: &DType,
    ctx: &mut ExecutionCtx,
    limits: &CompatibilityLimits,
) -> Result<(ArrayRef, u64)> {
    if array.len() > limits.batch_rows {
        return Err(error("native scan exceeded batch row admission"));
    }
    let mut expanded = 0_u64;
    let fields = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| error("canonical output requires a struct"))?;
    let mut children = Vec::with_capacity(fields.nfields());
    let check = || limits.check();
    let conversion = nested::ConversionLimits {
        string_bytes: limits.string_bytes,
        arrow_batch_bytes: limits.arrow_batch_bytes,
        check: &check,
    };
    for name in fields.names().iter() {
        limits.check()?;
        let child = logical_field_from_native_array(array, name.as_ref())?;
        let canonical = nested::canonical(&child, ctx, &conversion, &mut expanded)?;
        children.push(canonical);
    }
    let packed = StructArray::try_new_with_dtype(
        children,
        fields.clone(),
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

enum CodecWriter<'a> {
    Ipc(Box<arrow_ipc::writer::FileWriter<CappedWriter<'a>>>),
    Parquet(Box<ArrowWriter<CappedWriter<'a>>>),
    Avro(Box<arrow_avro::writer::AvroWriter<CappedWriter<'a>>>),
    Orc(Box<orc_rust::ArrowWriter<CappedWriter<'a>>>),
}
struct Writer<'a> {
    codec: CodecWriter<'a>,
    // Conservative input-size envelope for the complete unflushed file unit.
    // This is neither a provider allocation count nor process RSS.
    buffered_input_bytes: u64,
    target_bytes: u64,
    sizing: shardloom_exec::adaptive_work::AdaptiveWorkSizer,
}
impl<'a> Writer<'a> {
    fn new(
        format: VortexLocalPrimitiveRowExportFormat,
        schema: SchemaRef,
        file: &'a mut File,
        limits: &'a CompatibilityLimits,
        target_bytes: u64,
        parquet_page_bytes: usize,
        parquet_dictionary_enabled: bool,
    ) -> Result<Self> {
        let sink = CappedWriter {
            file,
            limits,
            written: 0,
        };
        let codec = match format {
            VortexLocalPrimitiveRowExportFormat::Avro => CodecWriter::Avro(Box::new(
                arrow_avro::writer::AvroWriter::new(sink, schema.as_ref().clone())
                    .map_err(vortex_error)?,
            )),
            VortexLocalPrimitiveRowExportFormat::Orc => CodecWriter::Orc(Box::new(
                orc_rust::ArrowWriterBuilder::new(sink, translated_schema(&schema, format)?)
                    .with_batch_size(limits.batch_rows)
                    .with_stripe_byte_size(usize::try_from(target_bytes).map_err(vortex_error)?)
                    .with_compression(orc_rust::compression::CompressionType::Snappy)
                    .try_build()
                    .map_err(vortex_error)?,
            )),
            VortexLocalPrimitiveRowExportFormat::ArrowIpc => CodecWriter::Ipc(Box::new(
                arrow_ipc::writer::FileWriter::try_new(sink, &schema).map_err(vortex_error)?,
            )),
            VortexLocalPrimitiveRowExportFormat::Parquet => {
                let properties = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .set_dictionary_enabled(parquet_dictionary_enabled)
                    .set_dictionary_page_size_limit(parquet_page_bytes)
                    .set_data_page_size_limit(parquet_page_bytes)
                    .set_statistics_enabled(EnabledStatistics::Chunk)
                    .set_write_batch_size(limits.batch_rows)
                    .set_max_row_group_row_count(Some(65_536))
                    .set_data_page_row_count_limit(8192)
                    .build();
                CodecWriter::Parquet(Box::new(
                    ArrowWriter::try_new(sink, schema, Some(properties)).map_err(vortex_error)?,
                ))
            }
            _ => return Err(error("unsupported compatibility writer")),
        };
        Ok(Self {
            codec,
            buffered_input_bytes: 0,
            target_bytes,
            sizing: shardloom_exec::adaptive_work::AdaptiveWorkSizer::default(),
        })
    }
    fn units(&self, batches: u64) -> Result<usize> {
        Ok(match &self.codec {
            CodecWriter::Ipc(_) => usize::try_from(batches).map_err(vortex_error)?,
            CodecWriter::Avro(_) => 1,
            CodecWriter::Parquet(writer) => {
                writer.flushed_row_groups().len() + usize::from(writer.in_progress_rows() != 0)
            }
            CodecWriter::Orc(writer) => {
                writer.flushed_stripes() + usize::from(writer.in_progress_rows() != 0)
            }
        })
    }
    fn pending_units(&self, batches: u64, rows: usize) -> Result<usize> {
        // A conversion batch usually extends the current row group/stripe.
        // Reserve its actual maximum resulting footer count, not another copy
        // of every column descriptor for each conversion batch.
        let (completed, pending) = match &self.codec {
            CodecWriter::Ipc(_) => (usize::try_from(batches).map_err(vortex_error)?, 1),
            CodecWriter::Avro(_) => (0, 1),
            CodecWriter::Parquet(writer) => (
                writer.flushed_row_groups().len(),
                writer
                    .in_progress_rows()
                    .checked_add(rows)
                    .ok_or_else(|| error("Parquet pending row count overflow"))?
                    .div_ceil(65_536),
            ),
            // Each admitted conversion has at most the ORC writer's batch_rows,
            // and that provider can flush only after encoding that batch.
            CodecWriter::Orc(writer) => (writer.flushed_stripes(), 1),
        };
        completed
            .checked_add(pending)
            .ok_or_else(|| error("writer pending unit count overflow"))
    }
    fn flush(&mut self, work: &mut CompatibilityWork, pressure: bool) -> Result<()> {
        if self.buffered_input_bytes == 0 {
            return Ok(());
        }
        match &mut self.codec {
            CodecWriter::Parquet(writer) => writer.flush().map_err(vortex_error)?,
            CodecWriter::Orc(writer) if writer.in_progress_rows() > 0 => {
                writer.flush_stripe().map_err(vortex_error)?;
            }
            _ => {}
        }
        self.buffered_input_bytes = 0;
        add(&mut work.writer_flushes, 1)?;
        if pressure {
            add(&mut work.pressure_flushes, 1)?;
        }
        Ok(())
    }
    fn write(
        &mut self,
        batch: &RecordBatch,
        bytes: u64,
        work: &mut CompatibilityWork,
    ) -> Result<()> {
        let retains = matches!(self.codec, CodecWriter::Parquet(_) | CodecWriter::Orc(_));
        match &mut self.codec {
            CodecWriter::Ipc(writer) => writer.write(batch).map_err(vortex_error)?,
            CodecWriter::Avro(writer) => writer.write(batch).map_err(vortex_error)?,
            CodecWriter::Orc(writer) => {
                writer
                    .write(&crate::universal_format_io::translate_orc_record_batch(
                        batch,
                    )?)
                    .map_err(vortex_error)?;
                let retained = usize_to_u64(writer.memory_size())?;
                work.max_observed_orc_in_progress_bytes =
                    work.max_observed_orc_in_progress_bytes.max(retained);
                if retained > work.writer_reserved_bytes {
                    return Err(error(
                        "ORC observed state exceeded the pre-admitted writer envelope",
                    ));
                }
            }
            CodecWriter::Parquet(writer) => {
                writer.write(batch).map_err(vortex_error)?;
                let retained = usize_to_u64(writer.memory_size())?;
                work.max_observed_parquet_in_progress_bytes =
                    work.max_observed_parquet_in_progress_bytes.max(retained);
                if retained > work.writer_reserved_bytes {
                    return Err(error(
                        "Parquet observed state exceeded the pre-admitted writer envelope",
                    ));
                }
            }
        }
        if retains {
            add(&mut self.buffered_input_bytes, bytes)?;
            let empty = match &self.codec {
                CodecWriter::Parquet(writer) => writer.in_progress_rows() == 0,
                CodecWriter::Orc(writer) => writer.in_progress_rows() == 0,
                _ => false,
            };
            if empty {
                self.buffered_input_bytes = 0;
            }
        }
        Ok(())
    }
    fn finish(self) -> Result<()> {
        match self.codec {
            CodecWriter::Ipc(mut writer) => writer.finish().map_err(vortex_error),
            CodecWriter::Parquet(writer) => (*writer).close().map(|_| ()).map_err(vortex_error),
            CodecWriter::Avro(mut writer) => writer.finish().map_err(vortex_error),
            CodecWriter::Orc(writer) => writer.close().map_err(vortex_error),
        }
    }
}

impl PreparedCompatibilityExport {
    pub(super) fn write(
        &self,
        output: &Path,
        overwrite: bool,
    ) -> Result<CompletedCompatibilityExport> {
        self.write_controlled(output, overwrite, &CancellationToken::default())
    }

    pub(super) fn write_controlled(
        &self,
        output: &Path,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<CompletedCompatibilityExport> {
        self.write_produced_observed(output, overwrite, None, cancellation, None, |_| Ok(()))
    }

    pub(super) fn write_produced(
        &self,
        output: &Path,
        overwrite: bool,
        producer: &mut native_sink::ArrayProducer<'_>,
        cancellation: &CancellationToken,
        admitted: Option<&crate::resident_session::NativeExecutionContext<'_>>,
    ) -> Result<CompletedCompatibilityExport> {
        self.write_produced_observed(
            output,
            overwrite,
            Some(producer),
            cancellation,
            admitted,
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    fn write_observed(
        &self,
        output: &Path,
        overwrite: bool,
        after_batch: impl FnMut(u64) -> Result<()>,
    ) -> Result<CompletedCompatibilityExport> {
        self.write_produced_observed(
            output,
            overwrite,
            None,
            &CancellationToken::default(),
            None,
            after_batch,
        )
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn write_produced_observed(
        &self,
        output_path: &Path,
        overwrite: bool,
        producer: Option<&mut native_sink::ArrayProducer<'_>>,
        cancellation: &CancellationToken,
        admitted: Option<&crate::resident_session::NativeExecutionContext<'_>>,
        mut after_batch: impl FnMut(u64) -> Result<()>,
    ) -> Result<CompletedCompatibilityExport> {
        cancellation.check()?;
        self.limits.check()?;
        self.plan.validate_destination(output_path)?;
        let mut metadata = self.limits.metadata_reservation(self.schema_fields)?;
        // Admit bounded page/dictionary and codec scratch before construction.
        // Actual retained state is additionally sampled inside this envelope.
        let initial = self
            .writer_overhead()?
            .checked_add(metadata)
            .ok_or_else(|| error("writer admission overflow"))?;
        let target_bytes = (self.plan.session.memory().snapshot().limit_bytes / 64)
            .clamp(64 * 1024, 8 * 1024 * 1024);
        let mut work_owner = self.plan.session.memory().reserve(initial)?;
        let mut output = native_sink::OwnedOutput::new(output_path, overwrite)?;
        let mut work = CompatibilityWork {
            writer_reserved_bytes: initial,
            file_unit_target_bytes: target_bytes,
            ..Default::default()
        };
        let mut rows = 0_u64;
        let mut observed = 0_u64;
        let mut arrays = 0_usize;
        let mut max_rows = 0_usize;
        let mut stopped = false;
        self.plan.source.with_native_execution_or_admitted(
            cancellation,
            admitted,
            |file, execution| {
                let session = execution.native_session();
                let mut writer = Writer::new(
                    self.format,
                    Arc::clone(&self.schema),
                    &mut output.file,
                    &self.limits,
                    target_bytes,
                    usize::try_from(self.parquet_page_bytes()).map_err(vortex_error)?,
                    self.parquet_dictionary_enabled(),
                )?;
                let mut ctx = session.create_execution_ctx();
                if self.plan.source.is_produced()
                    || (!self.plan.metadata_pruned && self.plan.row_count > 0)
                {
                    self.plan.consume(
                        file,
                        execution,
                        self.limits.batch_rows,
                        producer,
                        |array| {
                            self.limits.check()?;
                            arrays = arrays
                                .checked_add(1)
                                .ok_or_else(|| error("scan counter overflow"))?;
                            max_rows = max_rows.max(array.len());
                            if array.len() > self.limits.batch_rows {
                                return Err(error("scan batch exceeds row admission"));
                            }
                            add(&mut observed, usize_to_u64(array.len())?)?;
                            if array.is_empty() {
                                return Ok(true);
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
                            add(&mut work.native_batches, 1)?;
                            add(&mut work.native_logical_bytes, array.nbytes())?;
                            self.write_native_batch(
                                &array,
                                execution,
                                &mut ctx,
                                &mut writer,
                                &mut work,
                                &mut work_owner,
                                &mut metadata,
                                initial,
                                &mut rows,
                                &mut after_batch,
                            )?;
                            if self.plan.limit == Some(rows) {
                                stopped = true;
                                return Ok(false);
                            }
                            Ok(true)
                        },
                    )?;
                }
                work.output_file_units = usize_to_u64(writer.units(work.arrow_batches)?)?;
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
            },
        )?;
        work.output_bytes = output.file.metadata().map_err(vortex_error)?.len();
        let checksum = output.checksum()?;
        self.limits.check()?;
        cancellation.check()?;
        self.plan.source.validate_generation()?;
        output.commit()?;
        let before_limit = if self.plan.source.is_produced() {
            rows
        } else if self.plan.metadata_pruned {
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
                VortexLocalPrimitiveRowExportFormat::Avro => {
                    "compatibility_arrow_boundary;avro_integer_widths_widened_unsigned_to_signed_checked;uint64_above_i64_max_rejected;timestamp_instants_and_units_preserved_presentation_zone_normalized_zero_offset;fixed_size_lists_reopen_as_variable_lists;native_encodings_layout_statistics_user_metadata_not_copied;provider_copy_bytes_not_measured"
                }
                VortexLocalPrimitiveRowExportFormat::Orc => {
                    "compatibility_arrow_boundary;orc_snappy_stripes_independent_of_conversion_batches;orc_unsigned_to_signed_checked;uint64_above_i64_max_rejected;decimal128_list_struct_preserved;date64_whole_days_checked_to_date32;timestamp_instants_preserved_presentation_zone_normalized_utc_reader_precision_explicit;time_duration_unsupported;fixed_size_and_large_lists_reopen_as_variable_lists;large_offsets_narrowed_checked;schema_nullability_and_list_element_names_not_preserved;native_encodings_layout_statistics_user_metadata_not_copied;provider_copy_bytes_not_measured"
                }
                VortexLocalPrimitiveRowExportFormat::Parquet => {
                    "compatibility_arrow_boundary;logical_dtype_names_order_validity_preserved;native_encodings_layout_statistics_user_metadata_not_copied;parquet_snappy_bounded_dictionary_chunk_statistics;row_groups_independent_of_conversion_batches;provider_copy_bytes_not_measured;read_decode_materialize_flags_conservative_scan_scope_not_observed_bytes"
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
        VortexLocalPrimitiveRowExportFormat::Avro | VortexLocalPrimitiveRowExportFormat::Orc => {
            use arrow_array::RecordBatchReader as _;
            let expected = translated_schema(schema, format)?;
            let reader: Box<dyn arrow_array::RecordBatchReader> = if format
                == VortexLocalPrimitiveRowExportFormat::Avro
            {
                Box::new(
                    arrow_avro::reader::ReaderBuilder::new()
                        .with_batch_size(limits.batch_rows)
                        .build(std::io::BufReader::new(file))
                        .map_err(vortex_error)?,
                )
            } else {
                let builder = orc_rust::ArrowReaderBuilder::try_new(file).map_err(vortex_error)?;
                if !crate::universal_format_io::orc_wire_schema_matches(
                    &builder.schema(),
                    &expected,
                ) {
                    return Err(error("ORC file schema differs from declared translation"));
                }
                Box::new(
                    builder
                        .with_schema(expected.clone())
                        .with_batch_size(limits.batch_rows)
                        .build(),
                )
            };
            let actual = reader.schema();
            if actual.fields().len() != expected.fields().len()
                || actual
                    .fields()
                    .iter()
                    .zip(expected.fields())
                    .any(|(actual, expected)| {
                        if format == VortexLocalPrimitiveRowExportFormat::Avro {
                            !nested::avro_field_matches(actual, expected)
                        } else {
                            actual.name() != expected.name()
                                || actual.data_type() != expected.data_type()
                        }
                    })
            {
                return Err(error(
                    "compatibility reopen schema differs from declared translation",
                ));
            }
            let mut actual_rows = 0;
            for batch in reader {
                limits.check()?;
                add(
                    &mut actual_rows,
                    usize_to_u64(batch.map_err(vortex_error)?.num_rows())?,
                )?;
                if actual_rows > rows {
                    return Err(error("compatibility reopen row count exceeds output"));
                }
            }
            if actual_rows != rows {
                return Err(error("compatibility reopen row count differs"));
            }
        }
        _ => return Err(error("unsupported reopen target")),
    }
    Ok(())
}

fn translated_schema(
    schema: &SchemaRef,
    format: VortexLocalPrimitiveRowExportFormat,
) -> Result<SchemaRef> {
    fn translate(field: &Field, format: VortexLocalPrimitiveRowExportFormat) -> Field {
        let dtype = match (format, field.data_type()) {
            (
                VortexLocalPrimitiveRowExportFormat::Avro,
                DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16,
            )
            | (_, DataType::UInt16) => DataType::Int32,
            (_, DataType::UInt8) => DataType::Int16,
            (_, DataType::UInt32 | DataType::UInt64) => DataType::Int64,
            (VortexLocalPrimitiveRowExportFormat::Avro, DataType::Timestamp(unit, Some(_))) => {
                // Avro distinguishes instants from local timestamps, but has no
                // presentation-zone field. The pinned reader uses this exact
                // zero-offset spelling and preserves the timestamp's wire unit.
                DataType::Timestamp(*unit, Some("+00:00".into()))
            }
            (_, DataType::List(child)) => DataType::List(Arc::new(translate(child, format))),
            (VortexLocalPrimitiveRowExportFormat::Avro, DataType::FixedSizeList(child, _)) => {
                DataType::List(Arc::new(translate(child, format)))
            }
            (_, DataType::FixedSizeList(child, size)) => {
                DataType::FixedSizeList(Arc::new(translate(child, format)), *size)
            }
            (_, DataType::Struct(children)) => DataType::Struct(
                children
                    .iter()
                    .map(|child| translate(child, format))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            (_, dtype) => dtype.clone(),
        };
        Field::new(field.name(), dtype, field.is_nullable())
    }
    if format == VortexLocalPrimitiveRowExportFormat::Orc {
        return crate::universal_format_io::translate_orc_schema(schema);
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| translate(field, format))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native columnar compatibility sink: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_columnar_compat_sink_tests.rs"]
mod tests;
