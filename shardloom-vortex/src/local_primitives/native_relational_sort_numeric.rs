//! Borrow numeric keys and K row locations after complete ordinary validation.
//! Payload ownership, gathering and the shared stable heap remain unchanged.

use super::{NativeExecutionContext, Ordering, Result, Sort, compare_key, failed};
use crate::local_primitives::{
    aggregate_dense_pages::allocate, native_capacity::bytes,
    native_numeric_owner::NativeNumericOwner, native_relational_keys::KeyColumn,
    native_relational_order,
};
use shardloom_core::float_policy::float_key_bits;
use vortex::{array::dtype::PType, mask::Mask};

#[derive(Clone, Copy)]
struct Row {
    ordinal: usize,
    key_base: usize,
    local: usize,
}

enum Values<'a> {
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
    F32(&'a [f32]),
    F64(&'a [f64]),
}

struct Key<'a> {
    values: Values<'a>,
    valid: Option<&'a Mask>,
}

impl<'a> Key<'a> {
    fn new(owner: &'a NativeNumericOwner) -> Self {
        let array = owner.primitive();
        let values = match owner.ptype() {
            PType::I8 => Values::I8(array.as_slice()),
            PType::I16 => Values::I16(array.as_slice()),
            PType::I32 => Values::I32(array.as_slice()),
            PType::I64 => Values::I64(array.as_slice()),
            PType::U8 => Values::U8(array.as_slice()),
            PType::U16 => Values::U16(array.as_slice()),
            PType::U32 => Values::U32(array.as_slice()),
            PType::U64 => Values::U64(array.as_slice()),
            PType::F32 => Values::F32(array.as_slice()),
            PType::F64 => Values::F64(array.as_slice()),
            PType::F16 => unreachable!("numeric owner excludes f16"),
        };
        Self {
            values,
            valid: (!owner.all_valid()).then(|| owner.validity()),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        self.valid.is_some_and(|valid| !valid.value(row))
    }

    fn compare(&self, row: usize, other: &Self, other_row: usize) -> Result<Ordering> {
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.sort_numeric_prefix_comparisons += 1;
        });
        Ok(match (&self.values, &other.values) {
            (Values::I8(left), Values::I8(right)) => left[row].cmp(&right[other_row]),
            (Values::I16(left), Values::I16(right)) => left[row].cmp(&right[other_row]),
            (Values::I32(left), Values::I32(right)) => left[row].cmp(&right[other_row]),
            (Values::I64(left), Values::I64(right)) => left[row].cmp(&right[other_row]),
            (Values::U8(left), Values::U8(right)) => left[row].cmp(&right[other_row]),
            (Values::U16(left), Values::U16(right)) => left[row].cmp(&right[other_row]),
            (Values::U32(left), Values::U32(right)) => left[row].cmp(&right[other_row]),
            (Values::U64(left), Values::U64(right)) => left[row].cmp(&right[other_row]),
            (Values::F32(left), Values::F32(right)) => {
                float_order(f64::from(left[row]), f64::from(right[other_row]))
            }
            (Values::F64(left), Values::F64(right)) => float_order(left[row], right[other_row]),
            _ => return Err(failed("bound ordering key type changed")),
        })
    }
}

fn float_order(left: f64, right: f64) -> Ordering {
    f64::from_bits(float_key_bits(left)).total_cmp(&f64::from_bits(float_key_bits(right)))
}

/// Return false before changing ordinals if this optional representation is not
/// admitted. All views borrow the Sort, and both leases end before payload gather.
pub(super) fn try_prefix(
    sort: &Sort<'_>,
    ordinals: &mut [usize],
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    context.check_cancelled()?;
    let width = sort.spec.keys.len();
    if ordinals.is_empty() || ordinals.len() > 16_384 || width == 0 {
        return Ok(false);
    }
    let Some((_, first)) = sort.table.batches().next() else {
        return Ok(false);
    };
    // Mixed types retain exact general comparisons and their original failures.
    // This admission reads only already prepared metadata, never input values.
    for (_, batch) in sort.table.batches() {
        context.check_cancelled()?;
        for index in 0..width {
            match (first.key(index)?, batch.key(index)?) {
                (KeyColumn::Numeric(left), KeyColumn::Numeric(right))
                    if left.ptype() == right.ptype() => {}
                _ => return Ok(false),
            }
        }
    }
    let key_count = sort
        .table
        .batches()
        .len()
        .checked_mul(width)
        .ok_or_else(|| failed("bound ordering key count overflow"))?;
    let required = bytes::<Key<'_>>(key_count)?
        .checked_add(bytes::<Row>(ordinals.len())?)
        .ok_or_else(|| failed("bound ordering metadata capacity overflow"))?;
    let snapshot = context.memory().snapshot();
    if required > snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) / 16 {
        return Ok(false);
    }
    // Tuple fields drop in declaration order: metadata storage before its credit.
    let Some(mut keys) = allocate(key_count, context.memory(), failed)? else {
        return Ok(false);
    };
    let Some(mut heap) = allocate(ordinals.len(), context.memory(), failed)? else {
        return Ok(false);
    };
    for (_, batch) in sort.table.batches() {
        context.check_cancelled()?;
        for index in 0..width {
            let KeyColumn::Numeric(owner) = batch.key(index)? else {
                return Err(failed("bound ordering numeric owner changed"));
            };
            keys.0.push(Key::new(owner));
        }
    }
    let mut candidates = sort
        .table
        .batches()
        .enumerate()
        .flat_map(|(segment, (start, batch))| {
            (0..batch.array.len()).map(move |local| Row {
                ordinal: start + local,
                key_base: segment * width,
                local,
            })
        });
    heap.0.extend(candidates.by_ref().take(ordinals.len()));
    native_relational_order::select_prefix_from(
        &mut heap.0,
        candidates,
        context.cancellation(),
        |left, right| {
            for (index, key) in sort.spec.keys.iter().enumerate() {
                let left_key = &keys.0[left.key_base + index];
                let right_key = &keys.0[right.key_base + index];
                let order = compare_key(
                    key,
                    left_key.is_null(left.local),
                    right_key.is_null(right.local),
                    || left_key.compare(left.local, right_key, right.local),
                )?;
                if order != Ordering::Equal {
                    return Ok(order);
                }
            }
            Ok(left.ordinal.cmp(&right.ordinal))
        },
    )?;
    for (ordinal, row) in ordinals.iter_mut().zip(&heap.0) {
        *ordinal = row.ordinal;
    }
    #[cfg(all(test, debug_assertions))]
    crate::local_primitives::native_relational_observation::record(|counts| {
        counts.sort_numeric_prefixes += 1;
        counts.sort_numeric_prefix_rows += sort.table.rows() as u64;
        counts.sort_numeric_prefix_key_views += key_count as u64;
    });
    Ok(true)
}
