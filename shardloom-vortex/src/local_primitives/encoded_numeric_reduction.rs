//! Weighted native Constant/RunEnd consumers. Floating addition stays ordered;
//! only count/distinct/extrema collapse repeated logical values into one update.

use std::time::Instant;

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{
        Constant, PrimitiveArray, ScalarFn, Slice, Struct, scalar_fn::ScalarFnArrayExt as _,
        slice::SliceArraySlotsExt as _,
    },
    dtype::{DType, PType},
    matcher::Matcher,
    scalar_fn::fns::{get_item::GetItem, select::Select},
};
use vortex::encodings::runend::{RunEnd, RunEndArrayExt as _, RunEndArraySlotsExt as _};

use super::{
    AggregateDistinctValue, AggregateValueTransform, NativeNumericAccessorWork, NativeNumericOwner,
    SimpleAggregateFunction, SimpleAggregateState, SimpleAggregateStates, StatValue,
    int64_stat_to_float64, logical_field_from_native_array, simple_aggregate_max_value,
    simple_aggregate_min_value, stat_value_to_f64, uint64_stat_to_float64, usize_to_u64,
    vortex_error, vortex_scalar_to_stat_value,
};

#[derive(Clone, Default)]
pub(super) struct EncodedNumericReductionWork {
    calls: u64,
    constant_arrays: u64,
    run_end_arrays: u64,
    logical_rows: u64,
    child_primitive_executions: u64,
    child_rows: u64,
    max_child_rows: u64,
    weighted_value_visits: u64,
    elapsed_nanos: u64,
    structural_resolutions: u64,
}

impl EncodedNumericReductionWork {
    pub(super) fn add(&mut self, other: &Self) -> Result<()> {
        for (target, increment) in [
            (&mut self.calls, other.calls),
            (&mut self.constant_arrays, other.constant_arrays),
            (&mut self.run_end_arrays, other.run_end_arrays),
            (&mut self.logical_rows, other.logical_rows),
            (
                &mut self.child_primitive_executions,
                other.child_primitive_executions,
            ),
            (&mut self.child_rows, other.child_rows),
            (&mut self.weighted_value_visits, other.weighted_value_visits),
            (&mut self.elapsed_nanos, other.elapsed_nanos),
            (
                &mut self.structural_resolutions,
                other.structural_resolutions,
            ),
        ] {
            add(target, increment)?;
        }
        self.max_child_rows = self.max_child_rows.max(other.max_child_rows);
        Ok(())
    }

    pub(super) fn annotate(&self, object: &mut serde_json::Map<String, serde_json::Value>) {
        if self.calls != 0 {
            object.insert("aggregate_encoded_numeric_reduction".into(), serde_json::json!({
                "work": {
                    "calls": self.calls,
                    "constant_arrays": self.constant_arrays,
                    "run_end_arrays": self.run_end_arrays,
                    "logical_rows": self.logical_rows,
                    "child_primitive_executions": self.child_primitive_executions,
                    "child_rows": self.child_rows,
                    "max_child_rows": self.max_child_rows,
                    "weighted_value_visits": self.weighted_value_visits,
                    "elapsed_nanos": self.elapsed_nanos,
                    "structural_resolutions": self.structural_resolutions,
                },
                "scope": "native_Constant_and_RunEnd_identity_scalar_measures;weighted_count_distinct_min_max;ordered_floating_additions_and_original_per_array_fusion;selected_order_and_multiplicity_preserved;native_run_children_may_decode;no_expanded_logical_numeric_array;no_external_engine;not_zero_decode_or_RSS_bound;outer_query_cancellation_boundaries",
            }));
        }
    }
}

enum EncodedColumn {
    Constant {
        value: StatValue,
        len: usize,
    },
    Runs {
        ends: NativeNumericOwner,
        values: NativeNumericOwner,
        offset: usize,
        len: usize,
    },
}

// Resolve only projections rooted directly in a physical nonnullable Struct.
// A root matcher alone cannot prevent a provider from canonicalizing unknown
// children, so reject such trees before asking it to execute anything.
struct PhysicalLeaf;

impl Matcher for PhysicalLeaf {
    type Match<'a> = ();

    fn try_match(array: &ArrayRef) -> Option<()> {
        (!structural_wrapper(array)).then_some(())
    }
}

fn structural_wrapper(array: &ArrayRef) -> bool {
    let mut current = array.clone();
    let mut projected = false;
    for _ in 0..16 {
        if current.is::<Struct>() {
            return projected && !current.dtype().is_nullable();
        }
        let Some(expression) = current.as_opt::<ScalarFn>() else {
            return false;
        };
        if !(expression.scalar_fn().is::<GetItem>() || expression.scalar_fn().is::<Select>())
            || current.nchildren() != 1
        {
            return false;
        }
        current = current.children()[0].clone();
        projected = true;
    }
    false
}

/// Resolve only native field/selection wrappers rooted in a nonnullable Struct.
/// This preserves the actual child encoding and never asks an arbitrary scalar
/// expression to canonicalize a value domain merely to inspect its encoding.
pub(super) fn resolve_structural_projection(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<ArrayRef> {
    resolve_structural_projection_with_provider_error(array, ctx, &mut vortex_error)
}

pub(super) fn resolve_structural_projection_with_provider_error(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    provider_error: &mut impl FnMut(vortex::error::VortexError) -> ShardLoomError,
) -> Result<ArrayRef> {
    if !structural_wrapper(array) {
        return Ok(array.clone());
    }
    let resolved = array
        .clone()
        .execute_until::<PhysicalLeaf>(ctx)
        .map_err(provider_error)?;
    if resolved.dtype() != array.dtype() || resolved.len() != array.len() {
        return Err(failed("native structural resolution changed shape"));
    }
    Ok(resolved)
}

fn sliced_leaf(array: &ArrayRef) -> Option<(ArrayRef, usize)> {
    let mut leaf = array.clone();
    let mut offset = 0_usize;
    for _ in 0..16 {
        if let Some(slice) = leaf.as_opt::<Slice>() {
            offset = offset.checked_add(slice.slice_range().start)?;
            leaf = slice.child().clone();
        } else {
            return (matches!(leaf.dtype(), DType::Primitive(ptype, _) if *ptype != PType::F16)
                && (leaf.is::<Constant>() || leaf.is::<RunEnd>())
                && offset
                    .checked_add(array.len())
                    .is_some_and(|end| end <= leaf.len()))
            .then_some((leaf, offset));
        }
    }
    None
}

impl EncodedColumn {
    fn admitted(array: &ArrayRef) -> bool {
        sliced_leaf(array).is_some()
    }

    fn new(
        array: &ArrayRef,
        ctx: &mut ExecutionCtx,
        work: &mut EncodedNumericReductionWork,
    ) -> Result<Self> {
        let (leaf, slice_offset) =
            sliced_leaf(array).ok_or_else(|| failed("admitted numeric slice shape changed"))?;
        if let Some(scalar) = leaf.as_constant() {
            let value = if scalar.is_null() {
                StatValue::Null
            } else {
                vortex_scalar_to_stat_value(&scalar)
                    .ok_or_else(|| failed("constant has an unsupported numeric scalar"))?
            };
            add(&mut work.constant_arrays, 1)?;
            return Ok(Self::Constant {
                value,
                len: array.len(),
            });
        }
        let runs = leaf
            .as_opt::<RunEnd>()
            .ok_or_else(|| failed("admitted run-end representation changed"))?;
        let ends = child_owner(runs.ends(), ctx, work)?;
        let values = child_owner(runs.values(), ctx, work)?;
        let offset = runs
            .offset()
            .checked_add(slice_offset)
            .ok_or_else(|| failed("nested run slice offset overflow"))?;
        let len = array.len();
        let stop = offset
            .checked_add(len)
            .ok_or_else(|| failed("run window overflow"))?;
        if !ends.all_valid() || !ends.ptype().is_unsigned_int() || ends.len() != values.len() {
            return Err(failed(
                "run ends must be non-null unsigned integers aligned to values",
            ));
        }
        let mut previous = None;
        for row in 0..ends.len() {
            let end = run_end(&ends, row)?;
            if previous.is_some_and(|previous| end <= previous) {
                return Err(failed("run ends must be strictly increasing"));
            }
            previous = Some(end);
        }
        if len != 0 && previous.is_none_or(|end| end < stop) {
            return Err(failed("run ends do not cover the logical window"));
        }
        add(&mut work.run_end_arrays, 1)?;
        Ok(Self::Runs {
            ends,
            values,
            offset,
            len,
        })
    }

    fn visit(
        &self,
        selection: Option<&[usize]>,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Constant { value, len } => {
                let count = selection.map_or(*len, <[usize]>::len);
                if count != 0 {
                    visit(value, usize_to_u64(count)?)?;
                }
            }
            Self::Runs {
                ends,
                values,
                offset,
                len,
            } => {
                if let Some(rows) = selection {
                    // Binary search keeps arbitrary caller selection order and
                    // duplicates without a row-sized expanded run-index vector.
                    for &row in rows {
                        let position = offset
                            .checked_add(row)
                            .ok_or_else(|| failed("selected run position overflow"))?;
                        let mut low = 0;
                        let mut high = ends.len();
                        while low < high {
                            let mid = low + (high - low) / 2;
                            if run_end(ends, mid)? <= position {
                                low = mid + 1;
                            } else {
                                high = mid;
                            }
                        }
                        visit(&values.stat_value(low)?, 1)?;
                    }
                } else if *len != 0 {
                    let stop = offset
                        .checked_add(*len)
                        .ok_or_else(|| failed("run window overflow"))?;
                    let mut previous = 0;
                    for row in 0..ends.len() {
                        let end = run_end(ends, row)?;
                        let start = previous.max(*offset);
                        let limit = end.min(stop);
                        if start < limit {
                            visit(&values.stat_value(row)?, usize_to_u64(limit - start)?)?;
                        }
                        previous = end;
                        if end >= stop {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn child_owner(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    work: &mut EncodedNumericReductionWork,
) -> Result<NativeNumericOwner> {
    let primitive = array
        .clone()
        .execute::<PrimitiveArray>(ctx)
        .map_err(vortex_error)?;
    if primitive.len() != array.len() || primitive.dtype() != array.dtype() {
        return Err(failed("native child execution changed shape"));
    }
    add(&mut work.child_primitive_executions, 1)?;
    let rows = usize_to_u64(array.len())?;
    add(&mut work.child_rows, rows)?;
    work.max_child_rows = work.max_child_rows.max(rows);
    NativeNumericOwner::new(primitive, ctx)
}

fn run_end(ends: &NativeNumericOwner, row: usize) -> Result<usize> {
    usize::try_from(ends.integer_key(row)?.bits)
        .map_err(|_| failed("run end exceeds addressable rows"))
}

fn admitted_arrays(
    chunk: &ArrayRef,
    columns: &[String],
    ctx: &mut ExecutionCtx,
    structural_resolutions: &mut u64,
) -> Result<Option<Vec<ArrayRef>>> {
    // The common non-encoded miss must not allocate an accessor inventory.
    let mut arrays = Vec::new();
    for column in columns {
        let mut array = if chunk.dtype().is_struct() {
            logical_field_from_native_array(chunk, column)?
        } else {
            chunk.clone()
        };
        if structural_wrapper(&array) {
            array = resolve_structural_projection(&array, ctx)?;
            add(structural_resolutions, 1)?;
        }
        if array.len() != chunk.len() || !EncodedColumn::admitted(&array) {
            return Ok(None);
        }
        arrays.push(array);
    }
    Ok((!arrays.is_empty()).then_some(arrays))
}

/// Returns false before state mutation if any shape or expression is outside
/// this family. Admitted native errors propagate without a retry.
pub(super) fn update(
    states: &mut SimpleAggregateStates,
    chunk: &ArrayRef,
    columns: &[String],
    selection: Option<&[usize]>,
    numeric_work: &mut NativeNumericAccessorWork,
    ctx: &mut ExecutionCtx,
) -> Result<bool> {
    if states.states.is_empty()
        || states.states.iter().any(|state| {
            !matches!(state.value_transform, AggregateValueTransform::Identity)
                || state
                    .column_index
                    .is_some_and(|index| index >= columns.len())
                || (state.column_index.is_none()
                    && state.function != SimpleAggregateFunction::Count)
        })
    {
        return Ok(false);
    }
    let started = Instant::now();
    let mut structural_resolutions = 0;
    let Some(arrays) = admitted_arrays(chunk, columns, ctx, &mut structural_resolutions)? else {
        return Ok(false);
    };
    if selection.is_none() && prefer_native_fused_additive(states, &arrays) {
        // Dense additive-only fusion still performs every ordered addition.
        // The measured native typed consumer wins without a second run-table
        // validation/traversal. Keep weighted extrema/count and selected-run
        // consumers; decline the whole update before mutating any state.
        return Ok(false);
    }
    if selection.is_some_and(|rows| rows.iter().any(|&row| row >= chunk.len())) {
        return Err(failed("selected row exceeds the logical array"));
    }
    let mut work = EncodedNumericReductionWork {
        calls: 1,
        structural_resolutions,
        ..Default::default()
    };
    for (column, array) in arrays.iter().enumerate() {
        let encoded = EncodedColumn::new(array, ctx, &mut work)?;
        let fused = states
            .states
            .iter()
            .filter(|state| {
                state.column_index == Some(column)
                    && matches!(
                        state.function,
                        SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                    )
            })
            .count()
            >= 2;
        let mut base_sum = 0.0;
        let mut count = 0;
        // Select once per source column. Wide fused projections can contain
        // dozens of SUM/AVG states, none of which needs per-run dispatch.
        let mut weighted_states = states
            .states
            .iter_mut()
            .filter(|state| {
                state.column_index == Some(column)
                    && !(fused
                        && matches!(
                            state.function,
                            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                        ))
            })
            .collect::<Vec<_>>();
        encoded.visit(selection, |value, weight| {
            add(&mut work.weighted_value_visits, 1)?;
            add(&mut work.logical_rows, weight)?;
            if matches!(value, StatValue::Null) {
                return Ok(());
            }
            if fused {
                let numeric = finite_numeric(value, None)?;
                add(&mut count, weight)?;
                ordered_add(&mut base_sum, numeric, weight)?;
            }
            for state in &mut weighted_states {
                update_weighted(state, value, weight)?;
            }
            Ok(())
        })?;
        drop(weighted_states);
        if fused {
            finish_fused_column(states, column, base_sum, count)?;
        }
    }
    let row_count = usize_to_u64(selection.map_or(chunk.len(), <[usize]>::len))?;
    for state in states
        .states
        .iter_mut()
        .filter(|state| state.column_index.is_none())
    {
        add(&mut state.count, row_count)?;
    }
    states.direct_scalar_updates = true;
    states.direct_distinct_updates |= states
        .states
        .iter()
        .any(|state| state.function == SimpleAggregateFunction::CountDistinct);
    work.elapsed_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    numeric_work.encoded_reduction.add(&work)?;
    Ok(true)
}

fn prefer_native_fused_additive(states: &SimpleAggregateStates, arrays: &[ArrayRef]) -> bool {
    arrays.iter().enumerate().any(|(column, array)| {
        let mut column_states = states
            .states
            .iter()
            .filter(|state| state.column_index == Some(column));
        column_states.clone().count() >= 2
            && column_states.all(|state| {
                matches!(
                    state.function,
                    SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                )
            })
            && sliced_leaf(array).is_some_and(|(leaf, _)| leaf.is::<RunEnd>())
    })
}

fn finish_fused_column(
    states: &mut SimpleAggregateStates,
    column: usize,
    base_sum: f64,
    count: u64,
) -> Result<()> {
    for state in &mut states.states {
        if state.column_index == Some(column)
            && matches!(
                state.function,
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
            )
        {
            let offset = state.argument_offset.unwrap_or(0);
            let adjusted = base_sum + int64_stat_to_float64(offset) * uint64_stat_to_float64(count);
            if !adjusted.is_finite() {
                return Err(failed("fused argument offset became non-finite"));
            }
            add(&mut state.count, count)?;
            ordered_add(&mut state.sum, adjusted, 1)?;
        }
    }
    states.fused_numeric_additive_updates = true;
    Ok(())
}

fn update_weighted(state: &mut SimpleAggregateState, value: &StatValue, weight: u64) -> Result<()> {
    match state.function {
        SimpleAggregateFunction::Count => add(&mut state.count, weight),
        SimpleAggregateFunction::CountDistinct => {
            state
                .distinct_values
                .insert(AggregateDistinctValue::from(value));
            add(&mut state.count, weight)
        }
        SimpleAggregateFunction::Min => {
            state.min = Some(simple_aggregate_min_value(state.min.take(), value)?);
            add(&mut state.count, weight)
        }
        SimpleAggregateFunction::Max => {
            state.max = Some(simple_aggregate_max_value(state.max.take(), value)?);
            add(&mut state.count, weight)
        }
        SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
            let numeric = finite_numeric(value, state.argument_offset)?;
            add(&mut state.count, weight)?;
            ordered_add(&mut state.sum, numeric, weight)
        }
    }
}

fn finite_numeric(value: &StatValue, offset: Option<i64>) -> Result<f64> {
    let mut numeric = stat_value_to_f64(value)?;
    if let Some(offset) = offset {
        numeric += int64_stat_to_float64(offset);
    }
    if !numeric.is_finite() {
        return Err(failed("numeric value became non-finite"));
    }
    Ok(numeric)
}

fn ordered_add(sum: &mut f64, value: f64, count: u64) -> Result<()> {
    for _ in 0..count {
        *sum += value;
        if !sum.is_finite() {
            return Err(failed("numeric sum became non-finite"));
        }
    }
    Ok(())
}

fn add(total: &mut u64, increment: u64) -> Result<()> {
    *total = total
        .checked_add(increment)
        .ok_or_else(|| failed("count overflowed u64"))?;
    Ok(())
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native encoded numeric reduction {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "encoded_numeric_reduction_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "encoded_numeric_probe_tests.rs"]
mod probe_tests;
