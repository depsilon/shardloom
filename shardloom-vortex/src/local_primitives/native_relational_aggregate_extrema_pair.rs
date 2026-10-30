//! Share a flat grouped integer traversal only after proving that the adjacent
//! measures cannot fail in a different order. No grouped lists or new state are
//! constructed; the ordinary reducer owns every declined path.

use super::{
    ArrayRef, Cell, Function, GroupOrdinals, Measure, NativeExecutionContext, NativeNumericOwner,
    Result, Spec, State, add_count, extrema,
};
use shardloom_core::StatValue;
use vortex::array::{
    arrays::{
        Bool, Chunked, Primitive,
        chunked::{ChunkedArrayExt as _, ChunkedSlots},
        primitive::PrimitiveArrayExt,
    },
    dtype::{DType, Nullability},
    validity::Validity,
};

#[cfg(test)]
#[path = "native_relational_aggregate_extrema_pair_tests.rs"]
mod tests;

pub(super) struct Pair {
    pub(super) end: usize,
    pub(super) first_is_minimum: bool,
    pub(super) count: bool,
}

/// A named, unfiltered ordinary measure has no future input to execute. The
/// already-prepared primitive array and mask supply its complete value domain.
fn plain(measure: &Measure) -> bool {
    measure.column.is_some()
        && measure.filter_column.is_none()
        && measure.secondary_column.is_none()
        && measure.quantile_level.is_none()
        && measure.separator.is_none()
        && measure.selection.is_none()
        && measure.decimal_source.is_none()
        && measure.distinct_fields.is_empty()
}

fn pair(spec: &Spec, index: usize, source: &ArrayRef, count_ready: bool) -> Option<Pair> {
    let first = spec.measures.get(index)?;
    let second = spec.measures.get(index + 1)?;
    if !plain(first)
        || !plain(second)
        || first.column != second.column
        || first.dtype != source.dtype().as_nullable()
        || second.dtype != first.dtype
        || !matches!(
            (first.function, second.function),
            (Function::Min, Function::Max) | (Function::Max, Function::Min)
        )
    {
        return None;
    }
    let count = spec.measures.get(index + 2).is_some_and(|next| {
        count_ready
            && plain(next)
            && next.function == Function::Count
            && next.column == first.column
            && next.dtype
                == super::DType::Primitive(
                    super::PType::U64,
                    vortex::array::dtype::Nullability::NonNullable,
                )
    });
    Some(Pair {
        end: index + if count { 3 } else { 2 },
        first_is_minimum: first.function == Function::Min,
        count,
    })
}

/// The resident source retains ownership with one-chunk wrappers. Borrow their
/// complete domain without executing or allocating; deeper/encoded trees keep
/// the ordinary reducer. The limit bounds optional inspection, not query input.
fn transparent_leaf(mut source: &ArrayRef) -> Option<&ArrayRef> {
    for _ in 0..8 {
        let Some(chunked) = source.as_opt::<Chunked>() else {
            return Some(source);
        };
        if chunked.nchunks() != 1 {
            return None;
        }
        let child = source.slots().get(ChunkedSlots::CHUNKS_OFFSET)?.as_ref()?;
        if child.dtype() != source.dtype() || child.len() != source.len() {
            return None;
        }
        source = child;
    }
    (!source.is::<Chunked>()).then_some(source)
}

/// COUNT ordinarily executes source validity again at its own position. Skip
/// that preparation only for known physical masks, including underneath the
/// proven transparent wrappers. Encoded/pending validity keeps COUNT ordered.
fn count_ready(source: &ArrayRef) -> bool {
    let Some(primitive) = source.as_opt::<Primitive>() else {
        return false;
    };
    match PrimitiveArrayExt::validity(&primitive) {
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => true,
        Validity::Array(mask) => {
            mask.dtype() == &DType::Bool(Nullability::NonNullable)
                && mask.as_opt::<Bool>().is_some_and(|array| {
                    array
                        .data()
                        .clone()
                        .into_parts(mask.len())
                        .bits
                        .is_on_host()
                })
        }
    }
}

fn integer_extreme(state: &State) -> bool {
    matches!(
        state.extreme,
        None | Some(Cell::NegativeInteger(_) | Cell::NonnegativeInteger(_))
    )
}

/// Both extrema use this completed prepared owner. Floating comparison is
/// admissible only after a prior ordered numeric traversal established this
/// exact owner's policy domain. Extended admission does not prove finiteness.
pub(super) fn admit_inputs(
    spec: &Spec,
    index: usize,
    ordinals: &GroupOrdinals,
    source: &ArrayRef,
    owner: &NativeNumericOwner,
    completed_policy: Option<shardloom_core::FloatPolicy>,
) -> Option<Pair> {
    if !matches!(ordinals, GroupOrdinals::Grouped(_))
        || owner.len() == 0
        || (!owner.is_integer() && completed_policy != Some(owner.float_policy()))
        || owner.len() != ordinals.len()
        || source.dtype() != owner.primitive().dtype()
        || source.len() != owner.len()
    {
        return None;
    }
    let leaf = transparent_leaf(source)?;
    let primitive = leaf.as_opt::<Primitive>()?;
    if !PrimitiveArrayExt::buffer_handle(&primitive).is_on_host() {
        return None;
    }
    pair(spec, index, source, count_ready(leaf))
}

/// `source` and `owner` must come from the same existing `PreparedColumn`.
/// Its same-name/filter reuse contract also prepares the adjacent extremum;
/// upstream execution may have rebuilt the source into a different primitive.
pub(super) fn accumulate(
    spec: &Spec,
    index: usize,
    states: &mut [State],
    ordinals: &GroupOrdinals,
    source: &ArrayRef,
    owner: &NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    context.check_cancelled()?;
    let Some(pair) = admit_inputs(spec, index, ordinals, source, owner, None) else {
        return Ok(None);
    };
    let width = spec.measures.len();
    // Optional admission must not repeatedly traverse a growing group table
    // when only a small incoming batch can benefit from shared addressing.
    if !states.len().is_multiple_of(width) || states.len() / width > owner.len() / 2 {
        return Ok(None);
    }
    let Ok(rows) = u64::try_from(owner.len()) else {
        return Ok(None);
    };
    let max_start = u64::MAX - rows;
    for (group, row) in states.chunks_exact(width).enumerate() {
        if group.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.aggregate_integer_extrema_admission_groups += 1;
        });
        let (first, second) = (&row[index], &row[index + 1]);
        if first.count > max_start
            || first.count != second.count
            || !integer_extreme(first)
            || !integer_extreme(second)
            || (pair.count && first.count != row[index + 2].count)
        {
            return Ok(None);
        }
    }
    // No allocations or future preparations follow admission. Integers are
    // finite and exact comparisons cannot fail; every count has capacity for
    // even an all-present batch. Thus the original first-measure error order
    // cannot be changed by updating these states together.
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.aggregate_integer_extrema_fused_batches += 1;
        counts.aggregate_integer_extrema_fused_rows += owner.len() as u64;
        counts.aggregate_integer_extrema_saved_measure_rows +=
            owner.len() as u64 * if pair.count { 2 } else { 1 };
    });
    owner.visit_values(|row, value| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if matches!(value, StatValue::Null) {
            return Ok(());
        }
        let start = ordinals.at(row) * width + index;
        let target = &mut states[start..start + pair.end - index];
        add_count(&mut target[0], 1)?;
        extrema::update_extreme(&mut target[0], &value, pair.first_is_minimum)?;
        extrema::update_extreme(&mut target[1], &value, !pair.first_is_minimum)?;
        target[1].count = target[0].count;
        if pair.count {
            target[2].count = target[0].count;
        }
        Ok(())
    })?;
    context.check_cancelled()?;
    Ok(Some(pair.end))
}
