//! Bounded row expansion after the existing resident/native-spill sort owner.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::{Batch, failed, index_array},
    native_relational_keys::Cell,
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{Result, temporal::operations::Kernel};
use std::{cmp::Ordering, ops::Range};
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::StructArray,
    builtins::ArrayBuiltins as _,
    dtype::{DType, FieldNames},
    memory::HostMemorySessionExt as _,
    validity::Validity,
};

pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) dtype: DType,
    pub(super) time: usize,
    pub(super) partition: Vec<usize>,
    pub(super) key_names: Vec<String>,
    pub(super) grid: Kernel,
    pub(super) start: Option<i128>,
    pub(super) end: Option<i128>,
}

pub(super) struct GapFill<'a> {
    spec: &'a Spec,
    previous: Option<Batch>,
    next: Option<i128>,
}

impl<'a> GapFill<'a> {
    pub(super) fn new(spec: &'a Spec) -> Self {
        Self {
            spec,
            previous: None,
            next: None,
        }
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        let current = Batch::for_ordering(array, &self.spec.key_names, context)?;
        let mut original_start = None;
        for row in 0..current.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let value = time(&current, row, self.spec.partition.len())?;
            let index = self.spec.grid.bucket_index(value)?;
            if self.spec.grid.grid_point(index)? != value {
                return Err(failed(
                    "gap filling requires aligned times; resample or bucket the time column first",
                ));
            }
            let prior = if row == 0 {
                self.previous
                    .as_ref()
                    .map(|batch| (batch, batch.array.len() - 1))
            } else {
                Some((&current, row - 1))
            };
            let same = prior.map_or(Ok(false), |(batch, at)| {
                for key in 0..self.spec.partition.len() {
                    if batch.compare_key(at, &current, row, key)? != Ordering::Equal {
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
            if same {
                let (batch, at) = prior.ok_or_else(|| failed("gap-fill prior row is absent"))?;
                if value <= time(batch, at, self.spec.partition.len())? {
                    return Err(failed(
                        "gap filling requires unique increasing times within each partition; aggregate duplicates first",
                    ));
                }
            } else {
                self.flush_original(&current, &mut original_start, row, context, consume)?;
                self.finish_partition(prior, context, batch_rows, consume)?;
                self.next = Some(self.spec.start.unwrap_or(index));
            }
            let next = self
                .next
                .ok_or_else(|| failed("gap-fill partition cursor is absent"))?;
            let gap_end = self.spec.end.map_or(index, |end| index.min(end + 1));
            if next < gap_end {
                self.flush_original(&current, &mut original_start, row, context, consume)?;
                self.emit_missing(
                    Some((&current, row)),
                    next..gap_end,
                    context,
                    batch_rows,
                    consume,
                )?;
            }
            if index >= next && self.spec.end.is_none_or(|end| index <= end) {
                original_start.get_or_insert(row);
            } else {
                self.flush_original(&current, &mut original_start, row, context, consume)?;
            }
            self.next = Some(next.max(index + 1));
        }
        self.flush_original(
            &current,
            &mut original_start,
            current.array.len(),
            context,
            consume,
        )?;
        self.previous = Some(current);
        Ok(())
    }

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if let Some(previous) = &self.previous {
            self.finish_partition(
                Some((previous, previous.array.len() - 1)),
                context,
                batch_rows,
                consume,
            )
        } else if self.spec.partition.is_empty() {
            if let Some((start, end)) = self.spec.start.zip(self.spec.end) {
                self.emit_missing(None, start..end + 1, context, batch_rows, consume)?;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    fn finish_partition(
        &self,
        anchor: Option<(&Batch, usize)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if let Some((next, end)) = self.next.zip(self.spec.end) {
            self.emit_missing(anchor, next..end + 1, context, batch_rows, consume)?;
        }
        Ok(())
    }

    fn flush_original(
        &self,
        source: &Batch,
        start: &mut Option<usize>,
        end: usize,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if let Some(start) = start.take() {
            context.check_cancelled()?;
            let indices = index_array(end - start, false, context, |row| Ok(Some(start + row)))?;
            // Gather with the source's exact child types first. Only these
            // already validated rows lose time nullability; payload columns
            // widen to nullable for the generated records in this operator.
            let owned = native_payload::take_record(
                &source.array,
                &indices,
                source.array.dtype(),
                context,
            )?;
            consume(owned.cast(self.spec.dtype.clone()).map_err(vortex_error)?)?;
            context.check_cancelled()?;
        }
        Ok(())
    }

    fn emit_missing(
        &self,
        anchor: Option<(&Batch, usize)>,
        mut indices: Range<i128>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        while indices.start < indices.end {
            context.check_cancelled()?;
            let rows = usize::try_from((indices.end - indices.start).min(batch_rows as i128))
                .map_err(|_| failed("gap-fill output count overflow"))?;
            if rows == 0 {
                return Err(failed("gap-fill output batches require positive capacity"));
            }
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(self.spec.fields.len())?;
            let repeated = anchor
                .map(|(_, row)| index_array(rows, false, context, |_| Ok(Some(row))))
                .transpose()?;
            for (column, (name, dtype)) in self.spec.fields.iter().enumerate() {
                context.check_cancelled()?;
                let array = if column == self.spec.time {
                    // Every generated record has a time column. Attach its
                    // whole-record schema credit to that independently owned
                    // buffer so dropping the plan cannot release output credit.
                    let metadata = crate::native_payload_schema::reserve_schema(
                        &self.spec.dtype,
                        context.memory(),
                    )?;
                    let allocator = crate::owned_buffers::with_credit(
                        context.native_session().host_allocator(),
                        metadata,
                    );
                    result_batch::build_column(dtype, rows, &allocator, |row| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        Ok(Value::Int(
                            self.spec.grid.grid_point(indices.start + row as i128)?,
                        ))
                    })?
                } else if self.spec.partition.contains(&column) {
                    let (batch, _) =
                        anchor.ok_or_else(|| failed("gap-fill partition identity is absent"))?;
                    let repeated = repeated
                        .as_ref()
                        .ok_or_else(|| failed("gap-fill partition indices are absent"))?;
                    native_payload::take(
                        &logical_field_from_native_array(&batch.array, name)?,
                        repeated,
                        dtype,
                        context,
                    )?
                } else {
                    native_payload::defaults(dtype, rows, context)?
                };
                columns.values.push(array);
            }
            let (columns, _ownership) = columns.into_parts();
            let array = StructArray::try_new(
                self.spec
                    .fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<FieldNames>(),
                columns,
                rows,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            consume(array)?;
            context.check_cancelled()?;
            indices.start += rows as i128;
        }
        Ok(())
    }
}

fn time(batch: &Batch, row: usize, key: usize) -> Result<i64> {
    match batch.key(key)?.raw_cell(row)? {
        Cell::Date(value) => Ok(i64::from(value)),
        Cell::Timestamp(value) | Cell::Temporal(value) => Ok(value),
        Cell::Null => Err(failed(
            "gap filling requires non-NULL times; filter missing times explicitly",
        )),
        _ => Err(failed("gap-fill time domain changed after binding")),
    }
}
