//! Native row owners and bounded late payload gathering shared by operators.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_numeric_owner::NativeNumericOwner,
    native_payload,
    native_relational_keys::{Cell, KeyColumn, KeyLookupCache, Preparation},
    result_batch, vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::hash::Hasher as _;
use vortex::array::{
    ArrayRef, Columnar, IntoArray as _, VortexSessionExecute as _,
    arrays::{ChunkedArray, PrimitiveArray, StructArray},
    builtins::ArrayBuiltins as _,
    dtype::{DType, FieldNames, Nullability, PType},
    memory::HostMemorySessionExt as _,
    validity::Validity,
};

#[path = "native_relational_numeric_gather.rs"]
mod numeric_gather;

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
    pub(super) fn from_prepared_keys(array: ArrayRef, keys: ReservedVec<KeyColumn>) -> Self {
        Self { array, keys }
    }

    pub(super) fn new(
        array: ArrayRef,
        key_names: &[String],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::from_names(
            array,
            key_names.iter().map(String::as_str),
            context,
            Preparation::Hash,
        )
    }

    pub(super) fn for_ordering(
        array: ArrayRef,
        key_names: &[String],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::from_names(
            array,
            key_names.iter().map(String::as_str),
            context,
            Preparation::Values,
        )
    }

    pub(super) fn for_membership(
        array: ArrayRef,
        key_names: &[String],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::from_names(
            array,
            key_names.iter().map(String::as_str),
            context,
            Preparation::HashOnDemand,
        )
    }

    pub(super) fn with_names<const N: usize>(
        array: ArrayRef,
        key_names: [&str; N],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::from_names(array, key_names.into_iter(), context, Preparation::Hash)
    }

    fn from_names<'a>(
        array: ArrayRef,
        key_names: impl ExactSizeIterator<Item = &'a str>,
        context: &NativeExecutionContext<'_>,
        purpose: Preparation,
    ) -> Result<Self> {
        let mut keys = ReservedVec::new(context.memory())?;
        keys.reserve(key_names.len())?;
        let mut execution = context.native_session().create_execution_ctx();
        for name in key_names {
            context.check_cancelled()?;
            let column = logical_field_from_native_array(&array, name)?;
            let prepare = match purpose {
                Preparation::Values => KeyColumn::values_with_float_policy,
                Preparation::Hash => KeyColumn::new_with_float_policy,
                Preparation::HashOnDemand => KeyColumn::for_membership_with_float_policy,
            };
            keys.values.push(prepare(
                &column,
                &mut execution,
                context.memory(),
                context.cancellation(),
                context.float_policy(),
            )?);
        }
        Ok(Self { array, keys })
    }

    pub(super) fn validate_keys(&self, row: usize) -> Result<()> {
        for column in &self.keys.values {
            column.validate_at(row)?;
        }
        Ok(())
    }

    pub(super) fn key(&self, key: usize) -> Result<&KeyColumn> {
        self.keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound scalar key is absent"))
    }

    pub(super) fn key_cache(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<KeyLookupCache<'_>>> {
        KeyLookupCache::for_columns(&self.keys.values, context.memory(), context.cancellation())
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

    pub(super) fn raw_cell(&self, row: usize, key: usize) -> Result<Cell> {
        self.keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound scalar key is absent"))?
            .raw_cell(row)
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn hash_key(&self, row: usize, key: usize) -> Result<Option<u64>> {
        let column = self
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound scalar key is absent"))?;
        let mut hash = rustc_hash::FxHasher::default();
        Ok(column.hash_into(row, &mut hash)?.then(|| hash.finish()))
    }

    pub(super) fn compare_key(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
        key: usize,
    ) -> Result<std::cmp::Ordering> {
        self.keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound order key is absent"))?
            .compare_at(
                row,
                other
                    .keys
                    .values
                    .get(key)
                    .ok_or_else(|| failed("bound order key is absent"))?,
                other_row,
            )
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

    /// Borrow retained batches in source order without resolving row ordinals.
    pub(super) fn batches(&self) -> impl ExactSizeIterator<Item = (usize, &Batch)> {
        self.segments
            .values
            .iter()
            .map(|segment| (segment.start, &segment.batch))
    }

    /// Establish only the metadata half of an ordered-input proof. Owners must
    /// already have validated every compared value in their original order.
    /// No new key preparation, row lookup, allocation or payload visit occurs.
    pub(super) fn has_uniform_order_domain(
        &self,
        keys: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        context.check_cancelled()?;
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| counts.order_domain_checks += 1);
        let Some(first) = self.segments.values.first() else {
            return Ok(true);
        };
        let Some(reference) = first.batch.keys.values.get(..keys) else {
            return Ok(false);
        };
        for (batch_index, (_, batch)) in self.batches().enumerate() {
            if batch_index.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let Some(columns) = batch.keys.values.get(..keys) else {
                return Ok(false);
            };
            for (key_index, (column, first)) in columns.iter().zip(reference).enumerate() {
                if key_index.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if column.len() != batch.array.len() || !column.same_flat_order_domain(first) {
                    return Ok(false);
                }
            }
        }
        context.check_cancelled()?;
        Ok(true)
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
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| counts.row_location_searches += 1);
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

    /// Resolve once when a comparator visits several keys of the same row.
    pub(super) fn locate_batch(&self, row: usize) -> Result<(&Batch, usize)> {
        let (segment, local) = self.locate(row)?;
        Ok((&self.segments.values[segment].batch, local))
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

    /// Borrow one retained native key owner without reconstructing row payload.
    pub(super) fn key(&self, row: usize, key: usize) -> Result<(&KeyColumn, usize)> {
        let (segment, row) = self.locate(row)?;
        let column = self.segments.values[segment]
            .batch
            .keys
            .values
            .get(key)
            .ok_or_else(|| failed("bound scalar key is absent"))?;
        Ok((column, row))
    }

    pub(super) fn raw_cell(&self, row: usize, key: usize) -> Result<Cell> {
        let (column, row) = self.key(row, key)?;
        column.raw_cell(row)
    }

    pub(super) fn hash_key(&self, row: usize, key: usize) -> Result<Option<u64>> {
        let (column, row) = self.key(row, key)?;
        let mut hash = rustc_hash::FxHasher::default();
        Ok(column.hash_into(row, &mut hash)?.then(|| hash.finish()))
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
    pub(super) fn validate_column(
        &self,
        name: &str,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if self.segments.values.is_empty() {
            return self.column(name, dtype, context).map(drop);
        }
        native_payload::validate_selection(
            &self.source_column(name, context)?,
            &self.indices,
            dtype,
            context,
        )
    }

    pub(super) fn column(
        &self,
        name: &str,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        self.column_with_policy(
            name,
            dtype,
            native_payload::CopyPolicy::ValidateValues,
            context,
        )
    }

    pub(super) fn column_with_policy(
        &self,
        name: &str,
        dtype: &DType,
        policy: native_payload::CopyPolicy,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if self.segments.values.is_empty() {
            if native_payload::is_nested(dtype) {
                return native_payload::defaults(dtype, self.indices.len(), context);
            }
            return result_batch::build_column_with_float_policy(
                dtype,
                self.indices.len(),
                &context.native_session().host_allocator(),
                context.float_policy(),
                |_| Ok(result_batch::Value::Null),
            );
        }
        let array = self.source_column(name, context)?;
        take_column_with_policy(&array, &self.indices, dtype, policy, context)
    }

    /// Full selection still reaches the shared scalar validator. Only its
    /// retained prefix receives output buffers; no omitted value is skipped.
    pub(super) fn column_prefix(
        &self,
        name: &str,
        dtype: &DType,
        retained: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        let array = self.source_column(name, context)?;
        let mut execution = context.native_session().create_execution_ctx();
        // The retained prefix builder widens only retained scalar registers.
        // A provider Float16 cast would quiet signaling NaNs before that copy.
        let selected_dtype = if float16_promotion(array.dtype(), dtype) {
            array.dtype()
        } else {
            dtype
        };
        let selected = select_column(&array, &self.indices, selected_dtype, &mut execution)?;
        result_batch::build_column_prefix_with_float_policy(
            dtype,
            self.indices.len(),
            retained,
            &context.native_session().host_allocator(),
            context.float_policy(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                result_batch::scalar_value(&selected, row, &mut execution)
            },
        )
    }

    fn source_column(&self, name: &str, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
        if self.segments.values.is_empty() {
            return Err(failed("prefix gathering requires retained source rows"));
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
        Ok(ChunkedArray::try_new(chunks, source_dtype)
            .map_err(vortex_error)?
            .into_array())
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
    result_batch::build_column_with_float_policy(
        &dtype,
        rows,
        &context.native_session().host_allocator(),
        context.float_policy(),
        |row| {
            Ok(match value(row)? {
                Some(row) => result_batch::Value::UInt(u64::try_from(row).map_err(vortex_error)?),
                None => result_batch::Value::Null,
            })
        },
    )
}

pub(super) fn take_column(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    take_column_with_policy(
        array,
        indices,
        dtype,
        native_payload::CopyPolicy::ValidateValues,
        context,
    )
}

fn take_column_with_policy(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    policy: native_payload::CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    if float16_promotion(array.dtype(), dtype) {
        let selected = array.take(indices.clone()).map_err(vortex_error)?;
        let float_policy = if policy == native_payload::CopyPolicy::PreserveUnobserved {
            shardloom_core::FloatPolicy::Extended
        } else {
            context.float_policy()
        };
        return promote_float16(&selected, dtype, float_policy, context);
    }
    if matches!(dtype, DType::Decimal(..))
        || (policy == native_payload::CopyPolicy::PreserveUnobserved
            && matches!(
                dtype,
                DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
            ))
    {
        return native_payload::take_with_policy(array, indices, dtype, policy, context);
    }
    if native_payload::is_nested(dtype) {
        return native_payload::take_with_policy(array, indices, dtype, policy, context);
    }
    if let Some(output) = numeric_gather::try_copy(array, indices, dtype, context)? {
        return Ok(output);
    }
    let mut execution = context.native_session().create_execution_ctx();
    let selected = select_column(array, indices, dtype, &mut execution)?;
    if dtype == &DType::Null {
        // The provider has checked selection and cardinality. Untyped NULL
        // intermediates have no value buffers to compact; projection gives
        // them a typed carrier before they become result columns.
        context.check_cancelled()?;
        return Ok(selected);
    }
    // Compact final selected values into allocator-owned native buffers. This
    // explicit copy prevents small string views retaining whole source domains;
    // buffer credits then survive output clones, slices and the producer.
    // As at the native payload-copy boundary, prepare selected numeric storage
    // and validity once. Selection, casts and finite-value validation retain
    // their existing boundaries; unsupported leaves keep scalar diagnostics.
    let numeric = if matches!(dtype, DType::Primitive(..))
        || crate::native_payload_schema::temporal_storage(dtype).is_some()
    {
        let primitive = result_batch::scalar_storage(&selected, &mut execution)?
            .execute::<PrimitiveArray>(&mut execution)
            .map_err(vortex_error)?;
        Some(NativeNumericOwner::new(primitive, &mut execution)?)
    } else {
        None
    };
    let allocator = context.native_session().host_allocator();
    let output = if let Some(numeric) = &numeric {
        result_batch::copy_numeric_column(
            dtype,
            numeric.primitive(),
            numeric.validity(),
            &allocator,
            context.float_policy(),
            || context.check_cancelled(),
        )?
    } else {
        let variable = result_batch::VariableValues::try_new(&selected, &mut execution)?;
        result_batch::build_column_with_float_policy(
            dtype,
            indices.len(),
            &allocator,
            context.float_policy(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if let Some(variable) = &variable {
                    variable.value(row)
                } else {
                    result_batch::scalar_value(&selected, row, &mut execution)
                }
            },
        )?
    };
    context.check_cancelled()?;
    Ok(output)
}

pub(super) fn float16_promotion(source: &DType, target: &DType) -> bool {
    matches!(
        (source, target),
        (
            DType::Primitive(PType::F16, _),
            DType::Primitive(PType::F64, _)
        )
    )
}

/// Common-type promotion writes the final Float64 output directly from selected
/// Float16 storage. Ordinary provider casts may quiet signaling NaNs; the shared
/// numeric owner uses the exact bit bridge and observes only valid selected rows.
pub(super) fn promote_float16(
    selected: &ArrayRef,
    dtype: &DType,
    float_policy: shardloom_core::FloatPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    if !float16_promotion(selected.dtype(), dtype) {
        return Err(failed("Float16 promotion requires a Float64 result"));
    }
    let mut execution = context.native_session().create_execution_ctx();
    let primitive = selected
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let values =
        NativeNumericOwner::new_with_float_policy(primitive, &mut execution, float_policy)?;
    let output = result_batch::build_numeric_column::<f64>(
        dtype,
        selected.len(),
        &context.native_session().host_allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let value = values.numeric_value(row)?;
            if let Some(value) = value {
                result_batch::validate_numeric_output(value, float_policy)?;
            }
            Ok(value)
        },
    )?;
    context.check_cancelled()?;
    Ok(output)
}

fn select_column(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    execution: &mut vortex::array::ExecutionCtx,
) -> Result<ArrayRef> {
    Ok(array
        .take(indices.clone())
        .map_err(vortex_error)?
        .cast(dtype.clone())
        .map_err(vortex_error)?
        .execute::<Columnar>(execution)
        .map_err(vortex_error)?
        .into_array())
}

pub(super) fn take_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    take_batch_with_policy(
        array,
        fields,
        rows,
        native_payload::CopyPolicy::ValidateValues,
        context,
    )
}

pub(super) fn take_batch_with_policy(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    policy: native_payload::CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    take_batch_with_unobserved_fields(array, fields, rows, policy, &[], context)
}

pub(super) fn take_batch_with_unobserved_fields(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    policy: native_payload::CopyPolicy,
    unobserved_fields: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices = index_array(rows.len(), false, context, |row| Ok(Some(rows[row])))?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (index, (name, dtype)) in fields.iter().enumerate() {
        columns.values.push(take_column_with_policy(
            &logical_field_from_native_array(array, name)?,
            &indices,
            dtype,
            if unobserved_fields.contains(&index) {
                native_payload::CopyPolicy::PreserveUnobserved
            } else {
                policy
            },
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
