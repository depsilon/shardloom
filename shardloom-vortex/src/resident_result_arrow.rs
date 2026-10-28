//! Explicit columnar delivery from a retained native result. The pinned Vortex
//! provider performs representation conversion; no row/JSON bridge participates.

use super::{NativeExecutionContext, OwnedVortexResultBatch, ResidentVortexSession};
use crate::local_primitives::native_capacity::ReservedVec;
use crate::native_arrow_conversion::{self as conversion, ConversionLimits};
use crate::native_payload_schema;
use arrow_array::{
    Array as _, ArrayRef as ArrowArrayRef, BooleanArray, RecordBatch, cast::AsArray as _,
};
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

impl OwnedVortexResultBatch {
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
        cancellation.check()?;
        self.validate_schema_and_rows()?;
        let session = self.retained_session();
        let metadata = Arc::new(native_payload_schema::reserve_schema(
            self.dtype(),
            session.memory(),
        )?);
        let DType::Struct(fields, Nullability::NonNullable) = self.dtype() else {
            return Err(failed(
                "completed result requires a nonnullable record schema",
            ));
        };
        let schema = fields
            .names()
            .iter()
            .zip(fields.fields())
            .map(|(name, dtype)| {
                let kind = conversion::data_type(&dtype)
                    .ok_or_else(|| failed("result field has no admitted Arrow representation"))?;
                crate::native_temporal::arrow::field(&Field::new(
                    name.as_ref(),
                    kind,
                    dtype.is_nullable(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        cancellation.check()?;
        Ok(NativeArrowBatchStream {
            source: Arc::clone(self),
            schema: Arc::new(Schema::new(schema)),
            cancellation,
            next: 0,
            failed: false,
            report: NativeArrowExportReport::default(),
            metadata,
        })
    }
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
        let array = if let Some(array) = self.source.arrays().get(self.next) {
            array.clone()
        } else {
            crate::local_primitives::native_payload::empty_record(self.source.dtype(), context)?
        };
        // Metadata is admitted before constructing canonical containers,
        // target fields, Arrow wrappers or the pointer-range inventory.
        let metadata_bytes = native_payload_schema::schema_bytes(array.dtype())?;
        let mut work = context.memory().reserve(metadata_bytes)?;
        let mut ctx = context.native_session().create_execution_ctx();
        let check = || context.check_cancelled();
        let limits = ConversionLimits {
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
        let ranges = native_ranges(&canonical, context.memory())?;
        let target = Field::new("", DataType::Struct(self.schema.fields().clone()), false);
        let arrow = context
            .native_session()
            .arrow()
            .execute_arrow(canonical.clone(), Some(&target), &mut ctx)
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
            Arc::clone(&self.schema),
            structure.columns().to_vec(),
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(array.len())),
        )
        .map_err(native_error)?;
        let mut observed = NativeArrowExportReport {
            output_rows: u64::try_from(batch.num_rows()).map_err(native_error)?,
            output_batches: 1,
            peak_conversion_work_reserved_bytes: admitted,
            provider_canonicalization_calls: 1,
            ..NativeArrowExportReport::default()
        };
        for column in batch.columns() {
            observe_buffers(column, &ranges.values, &mut observed)?;
        }
        let retained = metadata_bytes
            .checked_add(observed.converted_buffer_capacity_bytes)
            .ok_or_else(|| failed("columnar retained admission overflow"))?;
        if retained > admitted {
            return Err(failed(
                "Arrow conversion exceeded its pre-reserved work envelope",
            ));
        }
        // Shrink an existing pre-conversion grant; never admit new provider
        // buffers after the fact. Native shared buffers keep their own grants.
        work.resize(retained)?;
        let credit = Arc::new(ExportCredit {
            _conversion: work,
            _schema: Arc::clone(&self.metadata),
        });
        let columns = batch
            .columns()
            .iter()
            .map(|array| retain_array(array, &credit))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(array.len())),
        )
        .map_err(native_error)?;
        context.check_cancelled()?;
        Ok((batch, observed))
    }
}

fn native_ranges(array: &ArrayRef, memory: &LiveMemoryPool) -> Result<ReservedVec<(usize, usize)>> {
    fn visit(array: &ArrayRef, ranges: &mut ReservedVec<(usize, usize)>) -> Result<()> {
        for buffer in array.buffers() {
            let start = buffer.as_ptr() as usize;
            let end = start
                .checked_add(buffer.len())
                .ok_or_else(|| failed("native buffer address overflow"))?;
            ranges.push((start, end))?;
        }
        for child in array.slots().iter().flatten() {
            visit(child, ranges)?;
        }
        Ok(())
    }
    let mut ranges = ReservedVec::new(memory)?;
    visit(array, &mut ranges)?;
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
) -> Result<()> {
    let data = array.to_data();
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
    for child in data.child_data() {
        observe_buffers(&arrow_array::make_array(child.clone()), ranges, report)?;
    }
    Ok(())
}

// Payload drops before its grants. These wrappers change ownership only; their
// addresses and bytes are identical to the provider's compatible Arrow buffers.
struct ExportCredit {
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

fn retain_array(array: &ArrowArrayRef, credit: &Arc<ExportCredit>) -> Result<ArrowArrayRef> {
    let data = array.to_data();
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
    let nulls = data
        .nulls()
        .map(|nulls| -> Result<_> {
            let bits: ArrowArrayRef = Arc::new(BooleanArray::new(nulls.inner().clone(), None));
            let retained = retain_array(&bits, credit)?;
            Ok(retained.as_boolean().values().clone().into())
        })
        .transpose()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            retain_array(&arrow_array::make_array(child.clone()), credit)
                .map(|array| array.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let data = data
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()
        .map_err(native_error)?;
    Ok(arrow_array::make_array(data))
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
