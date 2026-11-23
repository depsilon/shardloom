//! Explicit columnar delivery from a retained native result. The pinned Vortex
//! provider performs representation conversion; no row/JSON bridge participates.

use super::{NativeExecutionContext, OwnedVortexResultBatch, ResidentVortexSession};
use crate::local_primitives::native_capacity::ReservedVec;
use crate::native_arrow_conversion::{self as conversion, ConversionLimits};
use crate::native_payload_schema;
#[cfg(test)]
use arrow_array::cast::AsArray as _;
use arrow_array::{Array as _, ArrayRef as ArrowArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::Arc;
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        dtype::{DType, Nullability},
    },
    arrow::ArrowSessionExt as _,
};

/// Counts at the requested representation boundary, not process-wide memory or
/// measured decoding. Buffer byte counts include each referenced region; aliases
/// can therefore appear more than once. `complete` describes representation
/// delivery; the associated execution certificate determines query completion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeArrowExportReport {
    pub output_rows: u64,
    pub output_batches: u64,
    pub referenced_arrow_buffer_bytes: u64,
    pub shared_native_buffer_bytes: u64,
    pub converted_buffer_capacity_bytes: u64,
    pub peak_conversion_work_reserved_bytes: u64,
    pub provider_canonicalization_calls: u64,
    pub complete: bool,
}

impl NativeArrowExportReport {
    pub(crate) fn append(&mut self, observed: Self) -> Result<()> {
        let mut report = *self;
        add(&mut report.output_rows, observed.output_rows)?;
        add(&mut report.output_batches, observed.output_batches)?;
        add(
            &mut report.referenced_arrow_buffer_bytes,
            observed.referenced_arrow_buffer_bytes,
        )?;
        add(
            &mut report.shared_native_buffer_bytes,
            observed.shared_native_buffer_bytes,
        )?;
        add(
            &mut report.converted_buffer_capacity_bytes,
            observed.converted_buffer_capacity_bytes,
        )?;
        add(
            &mut report.provider_canonicalization_calls,
            observed.provider_canonicalization_calls,
        )?;
        report.peak_conversion_work_reserved_bytes = report
            .peak_conversion_work_reserved_bytes
            .max(observed.peak_conversion_work_reserved_bytes);
        *self = report;
        Ok(())
    }
}

/// Per-batch interchange over an existing native producer. This adapter holds
/// no complete result and performs the same conversion as retained-result
/// export. The native producer and the consumer determine demand and lifetime.
pub struct NativeArrowBatchDelivery<'a, F> {
    session: &'a ResidentVortexSession,
    cancellation: &'a CancellationToken,
    consume: F,
    report: NativeArrowExportReport,
}

impl<'a, F: FnMut(RecordBatch) -> Result<()>> NativeArrowBatchDelivery<'a, F> {
    #[must_use]
    pub fn new(
        session: &'a ResidentVortexSession,
        cancellation: &'a CancellationToken,
        consume: F,
    ) -> Self {
        Self {
            session,
            cancellation,
            consume,
            report: NativeArrowExportReport::default(),
        }
    }

    /// Convert one produced native batch in its admitted execution context.
    /// Returned Arrow arrays keep native buffers and conversion credits alive.
    /// # Errors
    /// Rejects cancellation, foreign owners, unsupported representations,
    /// resource pressure and consumer failure without certifying completion.
    pub fn push(&mut self, array: ArrayRef, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.cancellation.check()?;
        let mut arrays = ReservedVec::new(context.memory())?;
        arrays.push(array)?;
        let (arrays, ownership) = arrays.into_parts();
        let result = Arc::new(self.session.own_completed_arrays(arrays, ownership)?);
        let mut stream = result.arrow_batches(self.cancellation.clone())?;
        let batch = stream
            .next_batch_in_context(context)?
            .ok_or_else(|| failed("native batch produced no Arrow representation"))?;
        if stream.next_batch_in_context(context)?.is_some() {
            return Err(failed(
                "one native batch produced multiple Arrow representations",
            ));
        }
        self.report.append(stream.report())?;
        (self.consume)(batch)
    }

    /// Accept completion only after the native producer validates its sources.
    /// # Errors
    /// Rejects cancellation and cardinality disagreement at the boundary.
    pub fn finish(mut self, expected_rows: u64) -> Result<NativeArrowExportReport> {
        self.cancellation.check()?;
        if self.report.output_rows != expected_rows || self.report.output_batches == 0 {
            return Err(failed(
                "Arrow delivery changed native execution cardinality",
            ));
        }
        self.report.complete = true;
        Ok(self.report)
    }
}

/// Convert one retained native batch per demand. Returned Arrow buffers retain
/// their native owners and conversion/schema credits independently of this
/// stream, its result, and the original session handle. Dropping a stream stops
/// conversion demand; it does not invalidate already returned batches.
pub struct NativeArrowBatchStream {
    source: Arc<OwnedVortexResultBatch>,
    schema: SchemaRef,
    cancellation: CancellationToken,
    next: usize,
    failed: bool,
    report: NativeArrowExportReport,
    metadata: Arc<MemoryLease>,
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W: std::io::Write> std::io::Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("Arrow IPC byte counter overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// One bounded serialized Arrow IPC batch retaining credits for both the raw
/// IPC stream payload and its accompanying native dtype JSON metadata.
pub struct SerializedArrowIpcBatch {
    pub rows: usize,
    pub ipc_bytes: shardloom_exec::live_memory::Budgeted<Vec<u8>>,
    pub result_schema_json: shardloom_exec::live_memory::Budgeted<String>,
}

struct BoundedIpcVec {
    bytes: Vec<u8>,
    lease: MemoryLease,
    max_bytes: usize,
}

impl std::io::Write for BoundedIpcVec {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(buf.len())
            .filter(|len| *len <= self.max_bytes)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Arrow IPC batch exceeds byte bound",
                )
            })?;
        self.lease
            .resize(u64::try_from(next).map_err(std::io::Error::other)?)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        self.bytes
            .try_reserve(buf.len())
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn serialize_array_arrow_ipc(
    array: &ArrayRef,
    max_bytes: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<SerializedArrowIpcBatch> {
    if max_bytes == 0 {
        return Err(failed("Arrow IPC batch byte bound must be positive"));
    }
    context.check_cancelled()?;
    let (schema, _metadata) =
        prepare_arrow_schema_from_dtype(array.dtype(), context.memory(), context.cancellation())?;
    let converted =
        convert_native_record_batch(&schema, array.dtype(), Some(array), context, false)?;
    context.check_cancelled()?;
    let mut sink = BoundedIpcVec {
        bytes: Vec::new(),
        lease: context.memory().reserve(0)?,
        max_bytes,
    };
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(&mut sink, &schema).map_err(native_error)?;
    writer.write(&converted.batch).map_err(native_error)?;
    context.check_cancelled()?;
    writer.finish().map_err(native_error)?;
    drop(writer);
    let result_schema_json =
        crate::local_primitives::collect::serialize_result_schema(array.dtype(), context)?;
    Ok(SerializedArrowIpcBatch {
        rows: converted.array_len,
        ipc_bytes: shardloom_exec::live_memory::Budgeted::new(sink.bytes, sink.lease),
        result_schema_json,
    })
}

fn prepare_arrow_schema_from_dtype(
    dtype: &DType,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<(SchemaRef, Arc<MemoryLease>)> {
    cancellation.check()?;
    let metadata = Arc::new(native_payload_schema::reserve_schema(
        dtype,
        (memory, cancellation),
    )?);
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return Err(failed(
            "completed result requires a nonnullable record schema",
        ));
    };
    let schema = fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| {
            conversion::field_admitted(name.as_ref(), &dtype, (memory, cancellation))?
                .ok_or_else(|| failed("result field has no admitted Arrow representation"))
        })
        .collect::<Result<Vec<_>>>()?;
    cancellation.check()?;
    Ok((Arc::new(Schema::new(schema)), metadata))
}

struct ConvertedNativeBatch {
    array_len: usize,
    batch: RecordBatch,
    work: MemoryLease,
    metadata_bytes: u64,
    admitted: u64,
    ranges: Option<ReservedVec<(usize, usize)>>,
}

impl OwnedVortexResultBatch {
    fn prepare_arrow_schema(
        &self,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<(SchemaRef, Arc<MemoryLease>)> {
        cancellation.check()?;
        self.validate_schema_and_rows()?;
        prepare_arrow_schema_from_dtype(self.dtype(), session.memory(), cancellation)
    }

    /// Request direct Arrow batches over this completed native result.
    /// Encoded/unsupported Arrow representations are converted once by the
    /// pinned provider. Compatible buffers are shared, never rebuilt as rows.
    /// Vortex layouts/statistics are not part of the Arrow interchange schema.
    /// # Errors
    /// Rejects cancellation, unsupported schemas, inconsistent completed results
    /// or shared-memory denial. No new resource allocation or fallback is used.
    pub fn arrow_batches(
        self: &Arc<Self>,
        cancellation: CancellationToken,
    ) -> Result<NativeArrowBatchStream> {
        let session = self.retained_session();
        let (schema, metadata) = self.prepare_arrow_schema(&session, &cancellation)?;
        Ok(NativeArrowBatchStream {
            source: Arc::clone(self),
            schema,
            cancellation,
            next: 0,
            failed: false,
            report: NativeArrowExportReport::default(),
            metadata,
        })
    }

    /// Stream this completed native result as an Arrow IPC stream directly into
    /// `writer`, returning the total encoded bytes written.
    ///
    /// # Errors
    /// Rejects cancellation, unsupported schemas, inconsistent completed results,
    /// shared-memory exhaustion, or writer I/O failures.
    pub fn write_arrow_ipc_stream(
        &self,
        writer: impl std::io::Write,
        cancellation: &CancellationToken,
    ) -> Result<u64> {
        let session = self.retained_session();
        let (schema, _metadata) = self.prepare_arrow_schema(&session, cancellation)?;
        let mut counting = CountingWriter {
            inner: writer,
            bytes_written: 0,
        };
        session.with_native_execution_context(cancellation, |context| {
            let mut ipc = arrow_ipc::writer::StreamWriter::try_new(&mut counting, &schema)
                .map_err(native_error)?;
            for index in 0..self.arrays().len().max(1) {
                context.check_cancelled()?;
                let converted = convert_native_record_batch(
                    &schema,
                    self.dtype(),
                    self.arrays().get(index),
                    context,
                    false,
                )?;
                context.check_cancelled()?;
                ipc.write(&converted.batch).map_err(native_error)?;
            }
            context.check_cancelled()?;
            ipc.finish().map_err(native_error)?;
            drop(ipc);
            counting.inner.flush().map_err(native_error)?;
            Ok(counting.bytes_written)
        })
    }
}

fn convert_native_record_batch(
    schema: &SchemaRef,
    root_dtype: &DType,
    array: Option<&ArrayRef>,
    context: &NativeExecutionContext<'_>,
    collect_ranges: bool,
) -> Result<ConvertedNativeBatch> {
    let array = if let Some(array) = array {
        array.clone()
    } else {
        crate::local_primitives::native_payload::empty_record(root_dtype, context)?
    };
    // Metadata is admitted before constructing canonical containers,
    // target fields, Arrow wrappers or the pointer-range inventory.
    let mut work = native_payload_schema::reserve_schema(array.dtype(), context)?;
    let metadata_bytes = work.bytes();
    let mut ctx = context.native_session().create_execution_ctx();
    let check = || context.check_cancelled();
    let limits = ConversionLimits {
        memory: context.memory(),
        string_bytes: usize::MAX,
        arrow_batch_bytes: context.memory().snapshot().limit_bytes,
        check: &check,
    };
    let mut expanded = 0;
    let canonical = conversion::canonical(&array, &mut ctx, &limits, &mut expanded)?;
    // This is a conservative conversion-work envelope, not a dataset
    // size ceiling or a whole-provider/process allocator guarantee.
    let admitted = expanded
        .checked_mul(8)
        .and_then(|bytes| bytes.checked_add(metadata_bytes))
        .ok_or_else(|| failed("columnar conversion admission overflow"))?;
    work.resize(admitted)?;
    let ranges = collect_ranges
        .then(|| native_ranges(&canonical, context.memory()))
        .transpose()?;
    let target = Field::new("", DataType::Struct(schema.fields().clone()), false);
    let arrow = context
        .native_session()
        .arrow()
        .execute_arrow(canonical, Some(&target), &mut ctx)
        .map_err(native_error)?;
    let structure = arrow
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(|| failed("Arrow provider did not return the declared record"))?;
    if structure.len() != array.len() || structure.null_count() != 0 {
        return Err(failed(
            "Arrow provider changed record cardinality or validity",
        ));
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(schema),
        structure.columns().to_vec(),
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(array.len())),
    )
    .map_err(native_error)?;
    Ok(ConvertedNativeBatch {
        array_len: array.len(),
        batch,
        work,
        metadata_bytes,
        admitted,
        ranges,
    })
}

impl NativeArrowBatchStream {
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    #[must_use]
    pub const fn report(&self) -> NativeArrowExportReport {
        self.report
    }

    /// Demand the next columnar batch. A failure terminates this stream; retry
    /// requires a new export over the still-owned native result.
    /// # Errors
    /// Returns cancellation, conversion, schema or resource-admission failures.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let session = self.source.retained_session();
        let cancellation = self.cancellation.clone();
        self.advance(|stream| {
            session.with_native_execution_context(&cancellation, |context| {
                stream.next_admitted(context)
            })
        })
    }

    /// Incremental execution already holds this session's admission lock. Reuse
    /// that exact grant instead of recursively acquiring a second execution.
    pub(crate) fn next_batch_in_context(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<RecordBatch>> {
        self.advance(|stream| stream.next_admitted(context))
    }

    fn advance(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<Option<RecordBatch>>,
    ) -> Result<Option<RecordBatch>> {
        if self.failed {
            return Err(failed(
                "columnar stream already failed; create a new export to retry",
            ));
        }
        match operation(self) {
            Ok(batch) => Ok(batch),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    fn next_admitted(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<RecordBatch>> {
        self.cancellation.check()?;
        self.source
            .retained_session()
            .validate_execution_context(context)?;
        context.check_cancelled()?;
        if self.next >= self.source.arrays().len().max(1) {
            if self.report.output_rows != self.source.row_count() {
                return Err(failed(
                    "completed columnar stream changed the native row count",
                ));
            }
            self.report.complete = true;
            return Ok(None);
        }
        let (batch, observed) = self.convert_next(context)?;
        let mut report = self.report;
        report.append(observed)?;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| failed("columnar batch counter overflow"))?;
        self.report = report;
        Ok(Some(batch))
    }

    fn convert_next(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<(RecordBatch, NativeArrowExportReport)> {
        let converted = convert_native_record_batch(
            &self.schema,
            self.source.dtype(),
            self.source.arrays().get(self.next),
            context,
            true,
        )?;
        let ranges = converted
            .ranges
            .ok_or_else(|| failed("native buffer range inventory is absent"))?;
        let mut work = converted.work;
        let mut observed = NativeArrowExportReport {
            output_rows: u64::try_from(converted.batch.num_rows()).map_err(native_error)?,
            output_batches: 1,
            peak_conversion_work_reserved_bytes: converted.admitted,
            provider_canonicalization_calls: 1,
            ..NativeArrowExportReport::default()
        };
        for column in converted.batch.columns() {
            observe_buffers(column, &ranges.values, &mut observed, context.memory())?;
        }
        let retained = converted
            .metadata_bytes
            .checked_add(observed.converted_buffer_capacity_bytes)
            .ok_or_else(|| failed("columnar retained admission overflow"))?;
        if retained > converted.admitted {
            return Err(failed(
                "Arrow conversion exceeded its pre-reserved work envelope",
            ));
        }
        drop(ranges);
        // Shrink an existing pre-conversion grant; never admit new provider
        // buffers after the fact. Native shared buffers keep their own grants.
        work.resize(retained)?;
        let credit = Arc::new(ExportCredit {
            #[cfg(feature = "native-binary-objects")]
            _objects: crate::native_binary_type::contains(
                self.source.dtype(),
                context.into(),
                native_error,
            )?
            .then(|| crate::binary_object::Arena::get(context.native_session()))
            .transpose()?,
            _conversion: work,
            _schema: Arc::clone(&self.metadata),
        });
        let columns = converted
            .batch
            .columns()
            .iter()
            .map(|array| retain_array(array, &credit, context.memory()))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(converted.array_len)),
        )
        .map_err(native_error)?;
        context.check_cancelled()?;
        Ok((batch, observed))
    }
}

struct ReverseReserved<T>(ReservedVec<T>);

impl<T> Drop for ReverseReserved<T> {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

fn native_ranges(array: &ArrayRef, memory: &LiveMemoryPool) -> Result<ReservedVec<(usize, usize)>> {
    let mut ranges = ReservedVec::new(memory)?;
    let mut pending = ReverseReserved(ReservedVec::new(memory)?);
    pending.0.push(array.clone())?;
    while let Some(current) = pending.0.values.pop() {
        for buffer in current.buffers() {
            let start = buffer.as_ptr() as usize;
            let end = start
                .checked_add(buffer.len())
                .ok_or_else(|| failed("native buffer address overflow"))?;
            ranges.push((start, end))?;
        }
        let slots = current.slots();
        pending.0.reserve(slots.len())?;
        for child in slots.iter().rev().flatten() {
            pending.0.push(child.clone())?;
        }
    }
    drop(pending);
    ranges.values.sort_unstable();
    let mut retained = 0;
    for index in 0..ranges.values.len() {
        let (start, end) = ranges.values[index];
        if retained > 0 && start <= ranges.values[retained - 1].1 {
            ranges.values[retained - 1].1 = ranges.values[retained - 1].1.max(end);
        } else {
            ranges.values[retained] = (start, end);
            retained += 1;
        }
    }
    ranges.values.truncate(retained);
    Ok(ranges)
}

fn observe_buffers(
    array: &ArrowArrayRef,
    ranges: &[(usize, usize)],
    report: &mut NativeArrowExportReport,
    memory: &LiveMemoryPool,
) -> Result<()> {
    let mut pending = ReverseReserved(ReservedVec::new(memory)?);
    pending.0.push(array.to_data())?;
    while let Some(data) = pending.0.values.pop() {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|nulls| nulls.inner().inner()))
        {
            let bytes = u64::try_from(buffer.len()).map_err(native_error)?;
            add(&mut report.referenced_arrow_buffer_bytes, bytes)?;
            let start = buffer.as_ptr() as usize;
            let end = start
                .checked_add(buffer.len())
                .ok_or_else(|| failed("Arrow buffer address overflow"))?;
            let position = ranges.partition_point(|(native_start, _)| *native_start <= start);
            let shared = ranges[..position]
                .last()
                .is_some_and(|(_, native_end)| *native_end >= end);
            if shared {
                add(&mut report.shared_native_buffer_bytes, bytes)?;
            } else {
                add(
                    &mut report.converted_buffer_capacity_bytes,
                    u64::try_from(buffer.capacity()).map_err(native_error)?,
                )?;
            }
        }
        let children = data.child_data();
        pending.0.reserve(children.len())?;
        for child in children.iter().rev() {
            pending.0.push(child.clone())?;
        }
    }
    Ok(())
}

// Payload drops before its grants. These wrappers change ownership only; their
// addresses and bytes are identical to the provider's compatible Arrow buffers.
struct ExportCredit {
    #[cfg(feature = "native-binary-objects")]
    _objects: Option<crate::binary_object::Arena>,
    _conversion: MemoryLease,
    _schema: Arc<MemoryLease>,
}

struct RetainedBuffer<B> {
    buffer: B,
    _credit: Arc<ExportCredit>,
}

impl<B: std::ops::Deref<Target = [u8]>> AsRef<[u8]> for RetainedBuffer<B> {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

struct RetainFrame {
    template: arrow_data::ArrayData,
    buffers: Vec<arrow_buffer::Buffer>,
    nulls: Option<arrow_buffer::NullBuffer>,
    remaining: std::vec::IntoIter<arrow_data::ArrayData>,
    children: ReservedVec<arrow_data::ArrayData>,
}

type SplitNodeParts = (
    arrow_data::ArrayData,
    Vec<arrow_buffer::Buffer>,
    Option<arrow_buffer::NullBuffer>,
    std::vec::IntoIter<arrow_data::ArrayData>,
    usize,
);

fn retain_node_parts(data: arrow_data::ArrayData, credit: &Arc<ExportCredit>) -> SplitNodeParts {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| {
            bytes::Bytes::from_owner(RetainedBuffer {
                buffer: buffer.clone(),
                _credit: Arc::clone(credit),
            })
            .into()
        })
        .collect();
    let nulls = data.nulls().map(|nulls| {
        let retained: arrow_buffer::Buffer = bytes::Bytes::from_owner(RetainedBuffer {
            buffer: nulls.inner().inner().clone(),
            _credit: Arc::clone(credit),
        })
        .into();
        arrow_buffer::NullBuffer::new(arrow_buffer::BooleanBuffer::new(
            retained,
            nulls.offset(),
            nulls.len(),
        ))
    });
    let mut children = data.child_data().to_vec();
    let child_count = children.len();
    let remaining = std::mem::take(&mut children).into_iter();
    (data, buffers, nulls, remaining, child_count)
}

fn build_retained_data(
    template: arrow_data::ArrayData,
    buffers: Vec<arrow_buffer::Buffer>,
    nulls: Option<arrow_buffer::NullBuffer>,
    children: Vec<arrow_data::ArrayData>,
) -> Result<arrow_data::ArrayData> {
    template
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()
        .map_err(native_error)
}

fn retain_array(
    array: &ArrowArrayRef,
    credit: &Arc<ExportCredit>,
    memory: &LiveMemoryPool,
) -> Result<ArrowArrayRef> {
    let mut pending = ReverseReserved(ReservedVec::<RetainFrame>::new(memory)?);
    let mut current = array.to_data();
    loop {
        let (template, buffers, nulls, mut remaining, child_count) =
            retain_node_parts(current, credit);
        let mut built = if let Some(first_child) = remaining.next() {
            let mut children = ReservedVec::new(memory)?;
            children.reserve(child_count)?;
            pending.0.push(RetainFrame {
                template,
                buffers,
                nulls,
                remaining,
                children,
            })?;
            current = first_child;
            continue;
        } else {
            build_retained_data(template, buffers, nulls, Vec::new())?
        };
        loop {
            let Some(mut frame) = pending.0.values.pop() else {
                return Ok(arrow_array::make_array(built));
            };
            frame.children.push(built)?;
            if let Some(next_child) = frame.remaining.next() {
                pending.0.push(frame)?;
                current = next_child;
                break;
            }
            let children = std::mem::take(&mut frame.children.values);
            built = build_retained_data(frame.template, frame.buffers, frame.nulls, children)?;
        }
    }
}

fn add(total: &mut u64, bytes: u64) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| failed("columnar evidence counter overflow"))?;
    Ok(())
}

fn native_error(error: impl std::fmt::Display) -> ShardLoomError {
    failed(&error.to_string())
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native Arrow export: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "resident_result_arrow_tests.rs"]
mod tests;
