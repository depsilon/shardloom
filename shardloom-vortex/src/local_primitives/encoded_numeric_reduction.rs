//! Weighted native Constant/RunEnd/Dict consumers. Integer SUM/AVG runs and numeric
//! dictionaries reduce without expanded logical vectors; floating addition stays ordered
//! while count/distinct/extrema and integer sums collapse by multiplicity.

use std::time::Instant;

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{
        Constant, Dict, PrimitiveArray, ScalarFn, Slice, Struct, dict::DictArraySlotsExt as _,
        scalar_fn::ScalarFnArrayExt as _, slice::SliceArraySlotsExt as _,
    },
    dtype::{DType, PType},
    matcher::Matcher,
    scalar_fn::fns::{get_item::GetItem, select::Select},
};
use vortex::encodings::runend::{RunEnd, RunEndArrayExt as _, RunEndArraySlotsExt as _};

use super::{
    AggregateDistinctValue, AggregateValueTransform, NativeNumericAccessorWork, NativeNumericOwner,
    NumericAggregateSum, SimpleAggregateFunction, SimpleAggregateState, SimpleAggregateStates,
    StatValue, int64_stat_to_float64, logical_field_from_native_array, simple_aggregate_max_value,
    simple_aggregate_min_value, stat_value_to_f64, uint64_stat_to_float64, usize_to_u64,
    vortex_error, vortex_scalar_to_stat_value,
};

#[derive(Clone, Default)]
pub(super) struct EncodedNumericReductionWork {
    calls: u64,
    constant_arrays: u64,
    run_end_arrays: u64,
    dict_arrays: u64,
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
            (&mut self.dict_arrays, other.dict_arrays),
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
                    "dict_arrays": self.dict_arrays,
                    "logical_rows": self.logical_rows,
                    "child_primitive_executions": self.child_primitive_executions,
                    "child_rows": self.child_rows,
                    "max_child_rows": self.max_child_rows,
                    "weighted_value_visits": self.weighted_value_visits,
                    "elapsed_nanos": self.elapsed_nanos,
                    "structural_resolutions": self.structural_resolutions,
                },
                "scope": "native_Constant_RunEnd_and_Dict_identity_scalar_measures;weighted_count_distinct_min_max_and_multiplicity_histogram;4lane_integer_runend;ordered_floating_additions_and_original_per_array_fusion;selected_order_and_multiplicity_preserved;native_children_may_decode;no_expanded_logical_numeric_array;no_external_engine;not_zero_decode_or_RSS_bound;outer_query_cancellation_boundaries",
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
    Dict {
        codes: NativeNumericOwner,
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
                && (leaf.is::<Constant>() || leaf.is::<RunEnd>() || leaf.is::<Dict>())
                && offset
                    .checked_add(array.len())
                    .is_some_and(|end| end <= leaf.len()))
            .then_some((leaf, offset));
        }
    }
    None
}

macro_rules! with_unsigned_slice {
    ($owner:expr, |$slice:ident| $body:expr) => {
        match $owner.ptype() {
            PType::U8 => {
                let $slice = $owner.primitive().as_slice::<u8>();
                $body
            }
            PType::U16 => {
                let $slice = $owner.primitive().as_slice::<u16>();
                $body
            }
            PType::U32 => {
                let $slice = $owner.primitive().as_slice::<u32>();
                $body
            }
            PType::U64 => {
                let $slice = $owner.primitive().as_slice::<u64>();
                $body
            }
            _ => return Err(failed("run ends must be non-null unsigned integers")),
        }
    };
}

macro_rules! with_integer_slice {
    ($owner:expr, |$slice:ident| $body:expr) => {
        match $owner.ptype() {
            PType::U8 => {
                let $slice = $owner.primitive().as_slice::<u8>();
                $body
            }
            PType::U16 => {
                let $slice = $owner.primitive().as_slice::<u16>();
                $body
            }
            PType::U32 => {
                let $slice = $owner.primitive().as_slice::<u32>();
                $body
            }
            PType::U64 => {
                let $slice = $owner.primitive().as_slice::<u64>();
                $body
            }
            PType::I8 => {
                let $slice = $owner.primitive().as_slice::<i8>();
                $body
            }
            PType::I16 => {
                let $slice = $owner.primitive().as_slice::<i16>();
                $body
            }
            PType::I32 => {
                let $slice = $owner.primitive().as_slice::<i32>();
                $body
            }
            PType::I64 => {
                let $slice = $owner.primitive().as_slice::<i64>();
                $body
            }
            _ => return Err(failed("expected integer primitive slice")),
        }
    };
}

fn checked_dict_code_index(raw: i128, values_len: usize) -> Result<usize> {
    let index =
        usize::try_from(raw).map_err(|_| failed("dictionary code exceeds dictionary values"))?;
    if index >= values_len {
        return Err(failed("dictionary code exceeds dictionary values"));
    }
    Ok(index)
}

trait RunEndUsize: Copy {
    fn into_usize(self) -> Result<usize>;
}

impl RunEndUsize for u8 {
    fn into_usize(self) -> Result<usize> {
        Ok(usize::from(self))
    }
}

impl RunEndUsize for u16 {
    fn into_usize(self) -> Result<usize> {
        Ok(usize::from(self))
    }
}

impl RunEndUsize for u32 {
    #[allow(clippy::unnecessary_fallible_conversions)]
    fn into_usize(self) -> Result<usize> {
        usize::try_from(self).map_err(|_| failed("run end exceeds addressable rows"))
    }
}

impl RunEndUsize for u64 {
    fn into_usize(self) -> Result<usize> {
        usize::try_from(self).map_err(|_| failed("run end exceeds addressable rows"))
    }
}

fn validate_strictly_increasing_run_ends(
    ends: &NativeNumericOwner,
    len: usize,
    stop: usize,
) -> Result<()> {
    let mut previous = None::<usize>;
    with_unsigned_slice!(ends, |slice| {
        for &raw_end in slice {
            let end = raw_end.into_usize()?;
            if previous.is_some_and(|prev| end <= prev) {
                return Err(failed("run ends must be strictly increasing"));
            }
            previous = Some(end);
        }
    });
    if len != 0 && previous.is_none_or(|end| end < stop) {
        return Err(failed("run ends do not cover the logical window"));
    }
    Ok(())
}

impl EncodedColumn {
    fn admitted(array: &ArrayRef) -> bool {
        sliced_leaf(array).is_some()
    }

    fn value_ptype(&self) -> Option<PType> {
        match self {
            Self::Constant { .. } => None,
            Self::Runs { values, .. } | Self::Dict { values, .. } => Some(values.ptype()),
        }
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
                vortex_scalar_to_stat_value(scalar)
                    .ok_or_else(|| failed("constant has an unsupported numeric scalar"))?
            };
            add(&mut work.constant_arrays, 1)?;
            return Ok(Self::Constant {
                value,
                len: array.len(),
            });
        }
        if let Some(dict) = leaf.as_opt::<Dict>() {
            if !matches!(dict.codes().dtype(), DType::Primitive(ptype, _) if ptype.is_int()) {
                return Err(failed("dictionary codes must be integer primitives"));
            }
            let codes = child_owner(dict.codes(), ctx, work)?;
            let values = child_owner(dict.values(), ctx, work)?;
            let len = array.len();
            let stop = slice_offset
                .checked_add(len)
                .ok_or_else(|| failed("dictionary slice window overflow"))?;
            if stop > codes.len() {
                return Err(failed("dictionary slice exceeds codes length"));
            }
            add(&mut work.dict_arrays, 1)?;
            return Ok(Self::Dict {
                codes,
                values,
                offset: slice_offset,
                len,
            });
        }
        let runs = leaf
            .as_opt::<RunEnd>()
            .ok_or_else(|| failed("admitted encoded representation changed"))?;
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
        validate_strictly_increasing_run_ends(&ends, len, stop)?;
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
        preserve_float_order: bool,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Constant { value, len } => {
                let count = selection.map_or(*len, <[usize]>::len);
                if count != 0 {
                    visit(value, usize_to_u64(count)?)?;
                }
                Ok(())
            }
            Self::Runs {
                ends,
                values,
                offset,
                len,
            } => Self::visit_runs(ends, values, *offset, *len, selection, visit),
            Self::Dict {
                codes,
                values,
                offset,
                len,
            } => Self::visit_dict(
                codes,
                values,
                *offset,
                *len,
                selection,
                preserve_float_order,
                visit,
            ),
        }
    }

    fn visit_runs(
        ends: &NativeNumericOwner,
        values: &NativeNumericOwner,
        offset: usize,
        len: usize,
        selection: Option<&[usize]>,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        if let Some(rows) = selection {
            with_unsigned_slice!(ends, |ends_slice| {
                // Binary search preserves caller selection order and duplicates
                // without allocating a row-sized run index buffer.
                for &row in rows {
                    let position = offset
                        .checked_add(row)
                        .ok_or_else(|| failed("selected run position overflow"))?;
                    let run_idx = ends_slice.partition_point(|&raw_end| {
                        raw_end.into_usize().is_ok_and(|end| end <= position)
                    });
                    visit(&values.stat_value(run_idx)?, 1)?;
                }
            });
        } else if len != 0 {
            let stop = offset
                .checked_add(len)
                .ok_or_else(|| failed("run window overflow"))?;
            with_unsigned_slice!(ends, |ends_slice| {
                let mut previous = 0_usize;
                for (row, &raw_end) in ends_slice.iter().enumerate() {
                    let end = raw_end.into_usize()?;
                    let start = previous.max(offset);
                    let limit = end.min(stop);
                    if start < limit {
                        visit(&values.stat_value(row)?, usize_to_u64(limit - start)?)?;
                    }
                    previous = end;
                    if end >= stop {
                        break;
                    }
                }
            });
        }
        Ok(())
    }

    fn visit_dict(
        codes: &NativeNumericOwner,
        values: &NativeNumericOwner,
        offset: usize,
        len: usize,
        selection: Option<&[usize]>,
        preserve_float_order: bool,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        let target_rows = selection.map_or(len, <[usize]>::len);
        if target_rows == 0 {
            return Ok(());
        }
        if values.len() == 0 {
            with_integer_slice!(codes, |code_slice| {
                if let Some(rows) = selection {
                    for &row in rows {
                        let pos = offset
                            .checked_add(row)
                            .ok_or_else(|| failed("selected dict position overflow"))?;
                        if codes.all_valid() || !codes.is_null(pos)? {
                            let _ = checked_dict_code_index(i128::from(code_slice[pos]), 0)?;
                        }
                    }
                } else {
                    let stop = offset
                        .checked_add(len)
                        .ok_or_else(|| failed("dictionary slice window overflow"))?;
                    for (rel, &raw_code) in code_slice[offset..stop].iter().enumerate() {
                        if codes.all_valid() || !codes.is_null(offset + rel)? {
                            let _ = checked_dict_code_index(i128::from(raw_code), 0)?;
                        }
                    }
                }
            });
            return visit(&StatValue::Null, usize_to_u64(target_rows)?);
        }
        let use_histogram =
            !preserve_float_order && target_rows > values.len() && values.len() <= 1_048_576;
        if use_histogram {
            Self::visit_dict_histogram(codes, values, offset, len, selection, visit)
        } else {
            Self::visit_dict_ordered(codes, values, offset, len, selection, visit)
        }
    }

    fn visit_dict_histogram(
        codes: &NativeNumericOwner,
        values: &NativeNumericOwner,
        offset: usize,
        len: usize,
        selection: Option<&[usize]>,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        let mut counts = vec![0_u64; values.len()];
        let mut null_weight = 0_u64;
        with_integer_slice!(codes, |code_slice| {
            match (selection, codes.all_valid()) {
                (None, true) => {
                    let stop = offset
                        .checked_add(len)
                        .ok_or_else(|| failed("dictionary slice window overflow"))?;
                    for &raw_code in &code_slice[offset..stop] {
                        counts[checked_dict_code_index(i128::from(raw_code), values.len())?] += 1;
                    }
                }
                (None, false) => {
                    let stop = offset
                        .checked_add(len)
                        .ok_or_else(|| failed("dictionary slice window overflow"))?;
                    for (pos, &raw_code) in (offset..stop).zip(&code_slice[offset..stop]) {
                        if codes.is_null(pos)? {
                            null_weight += 1;
                        } else {
                            counts[checked_dict_code_index(i128::from(raw_code), values.len())?] +=
                                1;
                        }
                    }
                }
                (Some(rows), true) => {
                    for &row in rows {
                        let pos = offset
                            .checked_add(row)
                            .ok_or_else(|| failed("selected dict position overflow"))?;
                        counts[checked_dict_code_index(
                            i128::from(code_slice[pos]),
                            values.len(),
                        )?] += 1;
                    }
                }
                (Some(rows), false) => {
                    for &row in rows {
                        let pos = offset
                            .checked_add(row)
                            .ok_or_else(|| failed("selected dict position overflow"))?;
                        if codes.is_null(pos)? {
                            null_weight += 1;
                        } else {
                            counts[checked_dict_code_index(
                                i128::from(code_slice[pos]),
                                values.len(),
                            )?] += 1;
                        }
                    }
                }
            }
        });
        for (code, count) in counts.into_iter().enumerate() {
            if count == 0 {
                continue;
            }
            let value = values.stat_value(code)?;
            if matches!(value, StatValue::Null) {
                add(&mut null_weight, count)?;
            } else {
                visit(&value, count)?;
            }
        }
        if null_weight != 0 {
            visit(&StatValue::Null, null_weight)?;
        }
        Ok(())
    }

    fn visit_dict_ordered(
        codes: &NativeNumericOwner,
        values: &NativeNumericOwner,
        offset: usize,
        len: usize,
        selection: Option<&[usize]>,
        mut visit: impl FnMut(&StatValue, u64) -> Result<()>,
    ) -> Result<()> {
        with_integer_slice!(codes, |code_slice| {
            let all_valid = codes.all_valid();
            if let Some(rows) = selection {
                for &row in rows {
                    let pos = offset
                        .checked_add(row)
                        .ok_or_else(|| failed("selected dict position overflow"))?;
                    if !all_valid && codes.is_null(pos)? {
                        visit(&StatValue::Null, 1)?;
                    } else {
                        let idx =
                            checked_dict_code_index(i128::from(code_slice[pos]), values.len())?;
                        visit(&values.stat_value(idx)?, 1)?;
                    }
                }
            } else {
                let stop = offset
                    .checked_add(len)
                    .ok_or_else(|| failed("dictionary slice window overflow"))?;
                for (pos, &raw_code) in (offset..stop).zip(&code_slice[offset..stop]) {
                    if !all_valid && codes.is_null(pos)? {
                        visit(&StatValue::Null, 1)?;
                    } else {
                        let idx = checked_dict_code_index(i128::from(raw_code), values.len())?;
                        visit(&values.stat_value(idx)?, 1)?;
                    }
                }
            }
        });
        Ok(())
    }
}

fn integer_stat_value(ptype: PType, value: i128) -> Result<StatValue> {
    if ptype.is_signed_int() {
        i64::try_from(value)
            .map(StatValue::Int64)
            .map_err(|_| failed("integer extremum overflowed i64"))
    } else {
        u64::try_from(value)
            .map(StatValue::UInt64)
            .map_err(|_| failed("unsigned integer extremum overflowed u64"))
    }
}

struct VectorizedRunSummary {
    count: u64,
    base_sum: NumericAggregateSum,
    min: StatValue,
    max: StatValue,
}

struct RunAccumulators {
    visited_runs: u64,
    total_rows: u64,
    sums: [NumericAggregateSum; 4],
    mins: [i128; 4],
    maxs: [i128; 4],
}

impl Default for RunAccumulators {
    fn default() -> Self {
        Self {
            visited_runs: 0,
            total_rows: 0,
            sums: [NumericAggregateSum::exact_integer(0); 4],
            mins: [i128::MAX; 4],
            maxs: [i128::MIN; 4],
        }
    }
}

fn accumulate_interior_integer_runs<E: RunEndUsize, V: Into<i128> + Copy>(
    ends_prev: &[E],
    ends_curr: &[E],
    interior_vals: &[V],
    acc: &mut RunAccumulators,
) -> Result<()> {
    let chunks = interior_vals.len() / 4;
    for chunk_idx in 0..chunks {
        let b = chunk_idx * 4;
        let w0 = usize_to_u64(ends_curr[b].into_usize()? - ends_prev[b].into_usize()?)?;
        let w1 = usize_to_u64(ends_curr[b + 1].into_usize()? - ends_prev[b + 1].into_usize()?)?;
        let w2 = usize_to_u64(ends_curr[b + 2].into_usize()? - ends_prev[b + 2].into_usize()?)?;
        let w3 = usize_to_u64(ends_curr[b + 3].into_usize()? - ends_prev[b + 3].into_usize()?)?;
        let v0: i128 = interior_vals[b].into();
        let v1: i128 = interior_vals[b + 1].into();
        let v2: i128 = interior_vals[b + 2].into();
        let v3: i128 = interior_vals[b + 3].into();
        acc.mins[0] = acc.mins[0].min(v0);
        acc.mins[1] = acc.mins[1].min(v1);
        acc.mins[2] = acc.mins[2].min(v2);
        acc.mins[3] = acc.mins[3].min(v3);
        acc.maxs[0] = acc.maxs[0].max(v0);
        acc.maxs[1] = acc.maxs[1].max(v1);
        acc.maxs[2] = acc.maxs[2].max(v2);
        acc.maxs[3] = acc.maxs[3].max(v3);
        acc.sums[0].add_i128_repeated(v0, w0);
        acc.sums[1].add_i128_repeated(v1, w1);
        acc.sums[2].add_i128_repeated(v2, w2);
        acc.sums[3].add_i128_repeated(v3, w3);
        add(&mut acc.total_rows, w0)?;
        add(&mut acc.total_rows, w1)?;
        add(&mut acc.total_rows, w2)?;
        add(&mut acc.total_rows, w3)?;
        acc.visited_runs += 4;
    }
    for r in (chunks * 4)..interior_vals.len() {
        let w = usize_to_u64(ends_curr[r].into_usize()? - ends_prev[r].into_usize()?)?;
        let v: i128 = interior_vals[r].into();
        acc.mins[0] = acc.mins[0].min(v);
        acc.maxs[0] = acc.maxs[0].max(v);
        acc.sums[0].add_i128_repeated(v, w);
        add(&mut acc.total_rows, w)?;
        acc.visited_runs += 1;
    }
    Ok(())
}

fn reduce_integer_runs_4lane(
    ends: &NativeNumericOwner,
    values: &NativeNumericOwner,
    offset: usize,
    len: usize,
    work: &mut EncodedNumericReductionWork,
) -> Result<VectorizedRunSummary> {
    let stop = offset
        .checked_add(len)
        .ok_or_else(|| failed("run window overflow"))?;
    let mut acc = RunAccumulators::default();

    with_unsigned_slice!(ends, |ends_slice| {
        with_integer_slice!(values, |vals_slice| {
            let first_run = ends_slice
                .partition_point(|&raw_end| raw_end.into_usize().is_ok_and(|end| end <= offset));
            let end_idx = ends_slice
                .partition_point(|&raw_end| raw_end.into_usize().is_ok_and(|end| end < stop))
                .min(ends_slice.len().saturating_sub(1));

            let first_prev = if first_run == 0 {
                0
            } else {
                ends_slice[first_run - 1].into_usize()?
            };
            let first_end = ends_slice[first_run].into_usize()?;
            let first_start = first_prev.max(offset);
            let first_limit = first_end.min(stop);
            if first_start < first_limit {
                let w = usize_to_u64(first_limit - first_start)?;
                let v = i128::from(vals_slice[first_run]);
                acc.mins[0] = acc.mins[0].min(v);
                acc.maxs[0] = acc.maxs[0].max(v);
                acc.sums[0].add_i128_repeated(v, w);
                add(&mut acc.total_rows, w)?;
                acc.visited_runs += 1;
            }

            if end_idx > first_run {
                let interior_start = first_run + 1;
                accumulate_interior_integer_runs(
                    &ends_slice[first_run..end_idx - 1],
                    &ends_slice[interior_start..end_idx],
                    &vals_slice[interior_start..end_idx],
                    &mut acc,
                )?;
                let last_start = ends_slice[end_idx - 1].into_usize()?.max(offset);
                let last_limit = ends_slice[end_idx].into_usize()?.min(stop);
                if last_start < last_limit {
                    let w = usize_to_u64(last_limit - last_start)?;
                    let v = i128::from(vals_slice[end_idx]);
                    acc.mins[0] = acc.mins[0].min(v);
                    acc.maxs[0] = acc.maxs[0].max(v);
                    acc.sums[0].add_i128_repeated(v, w);
                    add(&mut acc.total_rows, w)?;
                    acc.visited_runs += 1;
                }
            }
        });
    });

    add(&mut work.weighted_value_visits, acc.visited_runs)?;
    add(&mut work.logical_rows, acc.total_rows)?;
    let mut base_sum = acc.sums[0];
    base_sum.merge(acc.sums[1]);
    base_sum.merge(acc.sums[2]);
    base_sum.merge(acc.sums[3]);
    if !base_sum.is_finite() {
        return Err(failed("numeric sum became non-finite"));
    }
    let min_val = acc.mins[0]
        .min(acc.mins[1])
        .min(acc.mins[2])
        .min(acc.mins[3]);
    let max_val = acc.maxs[0]
        .max(acc.maxs[1])
        .max(acc.maxs[2])
        .max(acc.maxs[3]);
    Ok(VectorizedRunSummary {
        count: acc.total_rows,
        base_sum,
        min: integer_stat_value(values.ptype(), min_val)?,
        max: integer_stat_value(values.ptype(), max_val)?,
    })
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

fn apply_integer_run_summary(
    states: &mut SimpleAggregateStates,
    column: usize,
    fused: bool,
    summary: &VectorizedRunSummary,
) -> Result<()> {
    for state in states.states.iter_mut().filter(|state| {
        state.column_index == Some(column)
            && !(fused
                && matches!(
                    state.function,
                    SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                ))
    }) {
        match state.function {
            SimpleAggregateFunction::Count => add(&mut state.count, summary.count)?,
            SimpleAggregateFunction::Min => {
                state.min = Some(simple_aggregate_min_value(state.min.take(), &summary.min)?);
                add(&mut state.count, summary.count)?;
            }
            SimpleAggregateFunction::Max => {
                state.max = Some(simple_aggregate_max_value(state.max.take(), &summary.max)?);
                add(&mut state.count, summary.count)?;
            }
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
                let adjusted =
                    adjusted_numeric_sum(summary.base_sum, state.argument_offset, summary.count)?;
                add(&mut state.count, summary.count)?;
                state.sum.merge(adjusted);
                if !state.sum.is_finite() {
                    return Err(failed("numeric sum became non-finite"));
                }
            }
            SimpleAggregateFunction::CountDistinct => {
                return Err(failed(
                    "4-lane vectorized integer run reduction cannot handle count_distinct",
                ));
            }
        }
    }
    if fused {
        finish_fused_column(states, column, summary.base_sum, summary.count)?;
    }
    Ok(())
}

fn reduce_encoded_column(
    states: &mut SimpleAggregateStates,
    column: usize,
    array: &ArrayRef,
    selection: Option<&[usize]>,
    work: &mut EncodedNumericReductionWork,
    ctx: &mut ExecutionCtx,
) -> Result<()> {
    let encoded = EncodedColumn::new(array, ctx, work)?;
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
    let has_count_distinct = states.states.iter().any(|state| {
        state.column_index == Some(column)
            && state.function == SimpleAggregateFunction::CountDistinct
    });
    let preserve_float_order = matches!(encoded.value_ptype(), Some(PType::F32 | PType::F64))
        && states.states.iter().any(|state| {
            state.column_index == Some(column)
                && matches!(
                    state.function,
                    SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                )
        });

    if !has_count_distinct
        && selection.is_none()
        && let EncodedColumn::Runs {
            ends,
            values,
            offset,
            len,
        } = &encoded
        && *len != 0
        && values.all_valid()
        && values.ptype().is_int()
    {
        let summary = reduce_integer_runs_4lane(ends, values, *offset, *len, work)?;
        return apply_integer_run_summary(states, column, fused, &summary);
    }

    let mut base_sum = NumericAggregateSum::default();
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
    encoded.visit(selection, preserve_float_order, |value, weight| {
        add(&mut work.weighted_value_visits, 1)?;
        add(&mut work.logical_rows, weight)?;
        if matches!(value, StatValue::Null) {
            return Ok(());
        }
        if fused {
            add(&mut count, weight)?;
            add_stat_repeated(&mut base_sum, None, value, weight)?;
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
    Ok(())
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
        reduce_encoded_column(states, column, array, selection, &mut work, ctx)?;
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

fn adjusted_numeric_sum(
    base_sum: NumericAggregateSum,
    argument_offset: Option<i64>,
    count: u64,
) -> Result<NumericAggregateSum> {
    let Some(offset) = argument_offset.filter(|&offset| offset != 0) else {
        return Ok(base_sum);
    };
    let adjusted = if let Some(total_offset) = i128::from(offset).checked_mul(i128::from(count)) {
        base_sum.with_i128_offset(total_offset)
    } else {
        base_sum.with_offset(int64_stat_to_float64(offset) * uint64_stat_to_float64(count))
    };
    if !adjusted.is_finite() {
        return Err(failed("fused argument offset became non-finite"));
    }
    Ok(adjusted)
}

fn finish_fused_column(
    states: &mut SimpleAggregateStates,
    column: usize,
    base_sum: NumericAggregateSum,
    count: u64,
) -> Result<()> {
    for state in &mut states.states {
        if state.column_index == Some(column)
            && matches!(
                state.function,
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
            )
        {
            let adjusted = adjusted_numeric_sum(base_sum, state.argument_offset, count)?;
            add(&mut state.count, count)?;
            state.sum.merge(adjusted);
            if !state.sum.is_finite() {
                return Err(failed("numeric sum became non-finite"));
            }
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
            add(&mut state.count, weight)?;
            add_stat_repeated(&mut state.sum, state.argument_offset, value, weight)
        }
    }
}

fn add_stat_repeated(
    sum: &mut NumericAggregateSum,
    offset: Option<i64>,
    value: &StatValue,
    weight: u64,
) -> Result<()> {
    let integer_operand = match value {
        StatValue::Int64(v) => Some(i128::from(*v)),
        StatValue::UInt64(v) => Some(i128::from(*v)),
        _ => None,
    };
    if let Some(operand) = integer_operand {
        let adjusted = operand + i128::from(offset.unwrap_or(0));
        sum.add_i128_repeated(adjusted, weight);
        if !sum.is_finite() {
            return Err(failed("numeric sum became non-finite"));
        }
        return Ok(());
    }
    let numeric = finite_numeric(value, offset)?;
    for _ in 0..weight {
        sum.add(numeric, false);
        if !sum.is_finite() {
            return Err(failed("numeric sum became non-finite"));
        }
    }
    Ok(())
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
