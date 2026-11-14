//! Existing integer chunk partials feed the existing grouped COUNT owner.
//! The caller proves a total sort consumer; no source replay or new state owner.

use super::{
    Aggregate, Function, Spec, failed, keys, logical_field_from_native_array, parent_validity,
};
use crate::local_primitives::{
    aggregate_chunk_jobs::ChunkWorkerContext,
    native_relational_keys::KeyColumn,
    numeric_count_partial::{self, NumericCountKey, OwnedNumericCounts},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_core::native_count_partial_work::NativeCountPartialWork;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{Constant, PrimitiveArray, StructArray},
        dtype::{DType, FieldNames, NativePType, Nullability, PType},
        memory::{HostBufferElement, HostMemorySessionExt as _},
        validity::Validity,
    },
    buffer::{Alignment, Buffer},
};

#[cfg(test)]
#[path = "native_relational_aggregate_count_partial_tests.rs"]
mod tests;

fn integer(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Primitive(
            PType::I32 | PType::I64 | PType::U64,
            Nullability::NonNullable
        )
    )
}

pub(super) fn admitted(spec: &Spec, mut field: impl FnMut(&str) -> Option<DType>) -> bool {
    let ([(name, dtype)], [group], [measure]) =
        (&spec.groups[..], &spec.group_names[..], &spec.measures[..])
    else {
        return false;
    };
    name == group
        && integer(dtype)
        && field(name).as_ref() == Some(dtype)
        && measure.function == Function::Count
        && measure.dtype == DType::Primitive(PType::U64, Nullability::NonNullable)
        && measure.secondary_column.is_none()
        && measure.filter_column.is_none()
        && measure.quantile_level.is_none()
        && measure.separator.is_none()
        && measure.selection.is_none()
        && measure.decimal_source.is_none()
        && measure.decimal_output.is_none()
        && measure.distinct_fields.is_empty()
        && measure
            .column
            .as_deref()
            .is_none_or(|column| field(column).is_some_and(|dtype| integer(&dtype)))
}

// Only this pool's capacity refusal is optional. Poisoned allocation policy,
// checked arithmetic, provider errors and cancellation remain real failures.
fn optional_reserve(
    context: &NativeExecutionContext<'_>,
    bytes: u64,
) -> Result<Option<MemoryLease>> {
    context.check_cancelled()?;
    match context.memory().try_reserve(bytes)? {
        Some(lease) => Ok(Some(lease)),
        None => capacity_declined(context),
    }
}

fn capacity_declined<T>(context: &NativeExecutionContext<'_>) -> Result<Option<T>> {
    context.check_cancelled()?;
    context.record_native_count_partial_work(NativeCountPartialWork {
        declined_capacity_batches: 1,
        ..Default::default()
    })?;
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.aggregate_count_partial_capacity_declines += 1;
    });
    Ok(None)
}

pub(super) fn try_consume(
    aggregate: &mut Aggregate<'_>,
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
) -> Result<bool> {
    context.check_cancelled()?;
    let Some(fields) = array.dtype().as_struct_fields_opt() else {
        return Ok(false);
    };
    if array.len() < 2
        || array.dtype().is_nullable()
        || !admitted(aggregate.spec, |name| fields.field(name))
    {
        return Ok(false);
    }
    context.record_native_count_partial_work(NativeCountPartialWork {
        considered_batches: 1,
        ..Default::default()
    })?;
    #[cfg(test)]
    let _cost = super::super::native_relational_grouped_cost::scope(
        super::super::native_relational_grouped_cost::Phase::AggregateRemainder,
    );
    let key = logical_field_from_native_array(array, &aggregate.spec.group_names[0])?;
    let constant = key.is::<Constant>();
    let rows = if constant { 1 } else { key.len() };
    // All three admitted pair layouts have their own checked capacity calculation.
    let bytes = match key.dtype() {
        DType::Primitive(PType::I32, _) => numeric_count_partial::partial_bytes::<i32>(rows)?,
        DType::Primitive(PType::I64, _) => numeric_count_partial::partial_bytes::<i64>(rows)?,
        DType::Primitive(PType::U64, _) => numeric_count_partial::partial_bytes::<u64>(rows)?,
        _ => return Err(failed("admitted COUNT key dtype changed")),
    };
    let selected = if constant {
        key.slice(0..1).map_err(vortex_error)?
    } else {
        key
    };
    let KeyColumn::Numeric(owner) = keys(&selected, context)? else {
        return Err(failed("admitted COUNT key is not numeric"));
    };
    if !owner.all_valid() || owner.len() != rows {
        return Err(failed("admitted COUNT key changed validity or row count"));
    }
    // Required selected-key preparation runs before optional partial storage so
    // the latter cannot consume the provider's grant and masquerade as its error.
    let Some(mut lease) = optional_reserve(context, bytes)? else {
        return Ok(false);
    };
    let worker = ChunkWorkerContext::Inline(context.cancellation().clone());
    macro_rules! reduce {
        ($type:ty) => {{
            let values = owner.primitive().as_slice::<$type>();
            let partial = if constant {
                numeric_count_partial::count_numeric_constant(
                    values[0],
                    array.len() as u64,
                    &worker,
                    &mut lease,
                )?
            } else {
                numeric_count_partial::count_numeric_values(values, &worker, &mut lease)?
            };
            consume_partial(aggregate, array, &partial, context, batch_rows, constant)
        }};
    }
    match owner.ptype() {
        PType::I32 => reduce!(i32),
        PType::I64 => reduce!(i64),
        PType::U64 => reduce!(u64),
        _ => Err(failed("admitted COUNT key physical type changed")),
    }
}

fn consume_partial<K: NumericCountKey + NativePType + HostBufferElement>(
    aggregate: &mut Aggregate<'_>,
    original: &ArrayRef,
    partial: &OwnedNumericCounts<K>,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    constant: bool,
) -> Result<bool> {
    if partial.rows() != original.len() as u64 {
        return Err(failed("COUNT partial changed input cardinality"));
    }
    if partial.pairs().len() == original.len() {
        context.record_native_count_partial_work(NativeCountPartialWork {
            declined_unique_batches: 1,
            ..Default::default()
        })?;
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.aggregate_count_partial_unique_declines += 1;
        });
        return Ok(false);
    }
    let Some(reduced) = reduced_keys(&aggregate.spec.group_names[0], partial, context)? else {
        return Ok(false);
    };
    // Every optional scratch allocation is complete. Any error below propagates;
    // the producer is never invoked again after group/state mutation.
    aggregate.prepare_compact(original, context)?;
    let ordinals = aggregate
        .groups
        .as_mut()
        .expect("admitted grouped COUNT")
        .intern_batch(reduced, context, batch_rows)?;
    aggregate.reserve_states(original, context)?;
    if ordinals.values.len() != partial.pairs().len() {
        return Err(failed("COUNT partial group mapping changed cardinality"));
    }
    let measure = &aggregate.spec.measures[0];
    if let Some(column) = &measure.column {
        let value = logical_field_from_native_array(original, column)?;
        let valid = parent_validity(&value, context)?;
        if valid.len() != original.len() || !valid.all_true() {
            return Err(failed(
                "nonnullable COUNT partial argument changed validity",
            ));
        }
    }
    let weights = partial.pairs().iter().map(|pair| pair.1);
    if let Some(table) = &mut aggregate.compact {
        table.add_count_weights(&ordinals.values, weights, context)?;
    } else {
        for (row, (&group, weight)) in ordinals.values.iter().zip(weights).enumerate() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            super::update_count(&mut aggregate.states.values[group].count, weight)?;
            #[cfg(all(test, debug_assertions))]
            super::super::native_relational_observation::record_count_update(weight);
        }
    }
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.aggregate_count_partial_batches += 1;
        counts.aggregate_count_partial_input_rows += partial.rows();
        counts.aggregate_count_partial_groups += partial.pairs().len() as u64;
        counts.aggregate_count_partial_capacity_bytes += partial.reserved_bytes();
        counts.aggregate_count_partial_constant_batches += u64::from(constant);
    });
    context.check_cancelled()?;
    context.record_native_count_partial_work(NativeCountPartialWork {
        admitted_batches: 1,
        input_rows: partial.rows(),
        reduced_groups: partial.pairs().len() as u64,
        constant_batches: u64::from(constant),
        partial_capacity_bytes: partial.reserved_bytes(),
        ..Default::default()
    })?;
    Ok(true)
}

fn reduced_keys<K: NumericCountKey + NativePType + HostBufferElement>(
    name: &str,
    partial: &OwnedNumericCounts<K>,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let rows = partial.pairs().len();
    let bytes = rows
        .checked_mul(size_of::<K>())
        .ok_or_else(|| failed("COUNT key capacity overflow"))?;
    let metadata = crate::native_payload_schema::names_bytes(1, |_| name)?;
    let Some(lease) = optional_reserve(context, metadata)? else {
        return Ok(None);
    };
    let allocator =
        crate::owned_buffers::with_credit(context.native_session().host_allocator(), lease);
    // Preserve the allocator's typed refusal before mapping its public error.
    let mut data = match allocator.allocate(bytes, Alignment::new(align_of::<K>())) {
        Ok(data) => data,
        Err(error) if crate::owned_buffers::is_owned_reservation_denial(&error) => {
            return capacity_declined(context);
        }
        Err(error) => return Err(vortex_error(error)),
    };
    for (row, (slot, &(key, _))) in data
        .as_mut_slice_typed::<K>()
        .map_err(vortex_error)?
        .iter_mut()
        .zip(partial.pairs())
        .enumerate()
    {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        *slot = key;
    }
    let column = PrimitiveArray::new(
        Buffer::<K>::from_byte_buffer(data.freeze()),
        Validity::NonNullable,
    )
    .into_array();
    let array = StructArray::try_new(
        [name].into_iter().collect::<FieldNames>(),
        vec![column],
        rows,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(Some(array))
}
