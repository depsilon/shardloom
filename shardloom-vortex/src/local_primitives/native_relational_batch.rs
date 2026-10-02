//! Native row owners and bounded late payload gathering shared by operators.

use super::{
    logical_field_from_native_array, native_capacity::ReservedVec,
    native_relational_keys::KeyColumn, result_batch, vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::hash::Hasher as _;
use vortex::array::{
    ArrayRef, Columnar, IntoArray as _, VortexSessionExecute as _,
    arrays::{ChunkedArray, StructArray},
    builtins::ArrayBuiltins as _,
    dtype::{DType, FieldNames, Nullability, PType},
    memory::MemorySessionExt as _,
    validity::Validity,
};

pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native relational execution: {reason}; no fallback execution was attempted"
    ))
}

pub(super) struct Batch {
    pub(super) array: ArrayRef,
    keys: ReservedVec<KeyColumn>,
}

impl Batch {
    pub(super) fn new(
        array: ArrayRef,
        key_names: &[String],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut keys = ReservedVec::new(context.memory())?;
        keys.reserve(key_names.len())?;
        let mut execution = context.native_session().create_execution_ctx();
        for name in key_names {
            context.check_cancelled()?;
            let column = logical_field_from_native_array(&array, name)?;
            keys.values.push(KeyColumn::new(
                &column,
                &mut execution,
                context.memory(),
                context.cancellation(),
            )?);
        }
        Ok(Self { array, keys })
    }

    pub(super) fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        self.hash_prefix(row, self.keys.values.len(), nulls_equal)
    }

    pub(super) fn hash_prefix(
        &self,
        row: usize,
        keys: usize,
        nulls_equal: bool,
    ) -> Result<Option<u64>> {
        let mut hash = rustc_hash::FxHasher::default();
        let columns = self
            .keys
            .values
            .get(..keys)
            .ok_or_else(|| failed("key prefix exceeds bound arity"))?;
        for column in columns {
            if !column.hash_into(row, &mut hash)? && !nulls_equal {
                return Ok(None);
            }
        }
        Ok(Some(hash.finish()))
    }

    pub(super) fn equal(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        if self.keys.values.len() != other.keys.values.len() {
            return Err(failed("key arity changed after binding"));
        }
        self.equal_prefix(row, other, other_row, self.keys.values.len(), nulls_equal)
    }

    pub(super) fn equal_prefix(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
        keys: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let left = self
            .keys
            .values
            .get(..keys)
            .ok_or_else(|| failed("key prefix exceeds bound arity"))?;
        let right = other
            .keys
            .values
            .get(..keys)
            .ok_or_else(|| failed("key prefix exceeds bound arity"))?;
        for (left, right) in left.iter().zip(right) {
            if !left.equals_at(row, right, other_row, nulls_equal)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn key_is_null(&self, row: usize, key: usize) -> Result<bool> {
        self.keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound key is absent"))?
            .is_null(row)
    }
}

struct Segment {
    start: usize,
    batch: Batch,
}

pub(super) struct Table {
    segments: ReservedVec<Segment>,
    rows: usize,
}

impl Table {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            segments: ReservedVec::new(memory)?,
            rows: 0,
        })
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    pub(super) fn push(&mut self, batch: Batch) -> Result<std::ops::Range<usize>> {
        let start = self.rows;
        let end = start
            .checked_add(batch.array.len())
            .ok_or_else(|| failed("row count overflow"))?;
        if end != start {
            self.segments.push(Segment { start, batch })?;
            self.rows = end;
        }
        Ok(start..end)
    }

    fn locate(&self, row: usize) -> Result<(usize, usize)> {
        if row >= self.rows {
            return Err(failed("row ordinal exceeds retained native input"));
        }
        let segment = self
            .segments
            .values
            .partition_point(|segment| segment.start <= row)
            - 1;
        Ok((segment, row - self.segments.values[segment].start))
    }

    pub(super) fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        let (segment, local) = self.locate(row)?;
        self.segments.values[segment].batch.hash(local, nulls_equal)
    }

    pub(super) fn hash_prefix(
        &self,
        row: usize,
        keys: usize,
        nulls_equal: bool,
    ) -> Result<Option<u64>> {
        let (segment, local) = self.locate(row)?;
        self.segments.values[segment]
            .batch
            .hash_prefix(local, keys, nulls_equal)
    }

    pub(super) fn equal_prefix(
        &self,
        row: usize,
        other: usize,
        keys: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let (segment, local) = self.locate(row)?;
        let (other_segment, other_local) = self.locate(other)?;
        self.segments.values[segment].batch.equal_prefix(
            local,
            &self.segments.values[other_segment].batch,
            other_local,
            keys,
            nulls_equal,
        )
    }

    pub(super) fn equal_prefix_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        keys: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let (segment, local) = self.locate(row)?;
        self.segments.values[segment]
            .batch
            .equal_prefix(local, batch, other, keys, nulls_equal)
    }

    /// Compare a retained right key to the corresponding left-batch key.
    pub(super) fn compare_batch_key(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        key: usize,
    ) -> Result<std::cmp::Ordering> {
        let (segment, local) = self.locate(row)?;
        let right = self.segments.values[segment]
            .batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound right key is absent"))?;
        let left = batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound left key is absent"))?;
        right.compare_at(local, left, other)
    }

    pub(super) fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool> {
        let (segment, local) = self.locate(row)?;
        let (other_segment, other_local) = self.locate(other)?;
        self.segments.values[segment].batch.equal(
            local,
            &self.segments.values[other_segment].batch,
            other_local,
            nulls_equal,
        )
    }

    pub(super) fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        batch_row: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let (segment, local) = self.locate(row)?;
        self.segments.values[segment]
            .batch
            .equal(local, batch, batch_row, nulls_equal)
    }

    pub(super) fn compare_key(
        &self,
        left: usize,
        right: usize,
        key: usize,
    ) -> Result<std::cmp::Ordering> {
        let (left_segment, left_row) = self.locate(left)?;
        let (right_segment, right_row) = self.locate(right)?;
        let left = self.segments.values[left_segment]
            .batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound order key is absent"))?;
        let right = self.segments.values[right_segment]
            .batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound order key is absent"))?;
        left.compare_at(left_row, right, right_row)
    }

    pub(super) fn key_is_null(&self, row: usize, key: usize) -> Result<bool> {
        let (segment, row) = self.locate(row)?;
        self.segments.values[segment]
            .batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound order key is absent"))?
            .is_null(row)
    }

    pub(super) fn gather<'a>(
        &'a self,
        rows: &[Option<usize>],
        nullable: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Gather<'a>> {
        let mut segments = ReservedVec::new(context.memory())?;
        segments.reserve(rows.len())?;
        for row in rows.iter().flatten() {
            segments.values.push(self.locate(*row)?.0);
        }
        segments.values.sort_unstable();
        segments.values.dedup();
        // Upstream chunked take allocates one bucket per input chunk. Supply only
        // touched chunks, bounding that metadata by the output batch size.
        let mut offsets = ReservedVec::new(context.memory())?;
        offsets.reserve(segments.values.len())?;
        let mut end = 0usize;
        for &segment in &segments.values {
            offsets.values.push(end);
            end = end
                .checked_add(self.segments.values[segment].batch.array.len())
                .ok_or_else(|| failed("gather offset overflow"))?;
        }
        let indices = index_array(rows.len(), nullable, context, |position| {
            rows[position]
                .map(|row| {
                    let (segment, local) = self.locate(row)?;
                    let position = segments
                        .values
                        .binary_search(&segment)
                        .map_err(|_| failed("gather segment is absent"))?;
                    offsets.values[position]
                        .checked_add(local)
                        .ok_or_else(|| failed("gather index overflow"))
                })
                .transpose()
        })?;
        Ok(Gather {
            table: self,
            segments,
            indices,
        })
    }
}

pub(super) struct Gather<'a> {
    table: &'a Table,
    segments: ReservedVec<usize>,
    indices: ArrayRef,
}

impl Gather<'_> {
    pub(super) fn column(
        &self,
        name: &str,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if self.segments.values.is_empty() {
            return result_batch::build_column(
                dtype,
                self.indices.len(),
                &context.native_session().allocator(),
                |_| Ok(result_batch::Value::Null),
            );
        }
        let mut chunks = ReservedVec::new(context.memory())?;
        chunks.reserve(self.segments.values.len())?;
        for &segment in &self.segments.values {
            chunks.values.push(logical_field_from_native_array(
                &self.table.segments.values[segment].batch.array,
                name,
            )?);
        }
        let source_dtype = chunks.values[0].dtype().clone();
        let (chunks, _ownership) = chunks.into_parts();
        let array = ChunkedArray::try_new(chunks, source_dtype)
            .map_err(vortex_error)?
            .into_array();
        take_column(&array, &self.indices, dtype, context)
    }
}

pub(super) fn index_array(
    rows: usize,
    nullable: bool,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<Option<usize>>,
) -> Result<ArrayRef> {
    let dtype = DType::Primitive(
        PType::U64,
        if nullable {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        },
    );
    result_batch::build_column(&dtype, rows, &context.native_session().allocator(), |row| {
        Ok(match value(row)? {
            Some(row) => result_batch::Value::UInt(u64::try_from(row).map_err(vortex_error)?),
            None => result_batch::Value::Null,
        })
    })
}

pub(super) fn take_column(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    let mut execution = context.native_session().create_execution_ctx();
    let selected = array
        .take(indices.clone())
        .map_err(vortex_error)?
        .cast(dtype.clone())
        .map_err(vortex_error)?
        .execute::<Columnar>(&mut execution)
        .map_err(vortex_error)?
        .into_array();
    // Compact final selected values into allocator-owned native buffers. This
    // explicit copy prevents small string views retaining whole source domains;
    // buffer credits then survive output clones, slices and the producer.
    let output = result_batch::build_column(
        dtype,
        indices.len(),
        &context.native_session().allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            result_batch::scalar_value(&selected, row, &mut execution)
        },
    )?;
    context.check_cancelled()?;
    Ok(output)
}

pub(super) fn take_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices = index_array(rows.len(), false, context, |row| Ok(Some(rows[row])))?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (name, dtype) in fields {
        columns.values.push(take_column(
            &logical_field_from_native_array(array, name)?,
            &indices,
            dtype,
            context,
        )?);
    }
    let (columns, _ownership) = columns.into_parts();
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)
    .map(vortex::array::IntoArray::into_array)
}
