//! Shared exact vector kernels over native list coordinates and selected children.

use super::{
    ArrayRef, DType, NativeExecutionContext, Nullability, ReservedVec, Result, Value, failed,
    index_array, integer, keys, parent_validity, result_batch, vortex_error,
};
use crate::{
    local_primitives::native_payload::{self, CopyPolicy},
    native_embedding, native_variable_tensor, native_vector,
};
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{ListViewArray, StructArray, struct_::StructArrayExt as _},
    memory::{HostAllocatorRef, HostMemorySessionExt as _},
    validity::Validity,
};

#[path = "native_vector_input.rs"]
mod input;
#[path = "native_vector_math.rs"]
mod math;
#[path = "native_vector_output.rs"]
mod output;

#[derive(PartialEq, Eq)]
pub(in crate::local_primitives) enum Kernel {
    Dimension(Option<native_vector::Metadata>),
    Shape(Shape),
    Values,
    Element,
    Slice,
    Reduction(Reduction),
    Normalize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::local_primitives) enum Reduction {
    Dot,
    Norm,
    L2,
    Cosine,
}

#[derive(PartialEq, Eq)]
pub(in crate::local_primitives) enum Shape {
    Fixed(Vec<u64>),
    Variable {
        metadata: native_variable_tensor::Metadata,
        rank: u32,
        order: Vec<usize>,
    },
}

impl Shape {
    pub(in crate::local_primitives) fn rank(&self) -> usize {
        match self {
            Self::Fixed(shape) => shape.len(),
            Self::Variable { rank, .. } => *rank as usize,
        }
    }
}

impl Kernel {
    pub(super) fn evaluate(
        &self,
        args: &[ArrayRef],
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        match self {
            Self::Values => input::storage(&args[0], context),
            Self::Dimension(metadata) => dimension(&args[0], metadata.as_ref(), dtype, context),
            Self::Shape(shape) => tensor_shape(&args[0], shape, dtype, context),
            Self::Element => element(args, dtype, context),
            Self::Slice => slice(args, dtype, context),
            Self::Reduction(reduction) => reduce(args, *reduction, dtype, context),
            Self::Normalize => normalize(&args[0], dtype, context),
        }
    }
}

fn check(index: usize, context: &NativeExecutionContext<'_>) -> Result<()> {
    if index.is_multiple_of(1024) {
        context.check_cancelled()?;
    }
    Ok(())
}

fn allocator(dtype: &DType, context: &NativeExecutionContext<'_>) -> Result<HostAllocatorRef> {
    Ok(crate::owned_buffers::with_credit(
        context.native_session().host_allocator(),
        context
            .memory()
            .reserve(native_payload::metadata_bytes(dtype)?)?,
    ))
}

fn scalar_result(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<Value<'static>>,
) -> Result<ArrayRef> {
    result_batch::build_column_with_float_policy(
        dtype,
        rows,
        &allocator(dtype, context)?,
        context.float_policy(),
        |row| {
            check(row, context)?;
            value(row)
        },
    )
}

fn dimension(
    array: &ArrayRef,
    metadata: Option<&native_vector::Metadata>,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    if let Some(metadata) = metadata {
        let _admission = context.memory().reserve(coordinate_bytes(array.len())?)?;
        let mut execution = context.native_session().create_execution_ctx();
        let storage = input::storage(array, context)?
            .execute::<StructArray>(&mut execution)
            .map_err(vortex_error)?;
        let valid = parent_validity(array, context)?;
        let codes = if metadata.kind() == native_vector::Kind::Quantized {
            Some(input::List::new(storage.unmasked_field(0), context)?)
        } else {
            None
        };
        return scalar_result(dtype, array.len(), context, |row| {
            if !valid.value(row) {
                return Ok(Value::Null);
            }
            let size = if let Some(codes) = &codes {
                let (_, count) = codes
                    .at(row)?
                    .ok_or_else(|| failed("present quantized vector requires codes"))?;
                u64::try_from(count).map_err(vortex_error)?
            } else {
                storage
                    .unmasked_field(0)
                    .execute_scalar(row, &mut execution)
                    .map_err(vortex_error)?
                    .as_primitive()
                    .as_::<u64>()
                    .ok_or_else(|| failed("present sparse vector requires UInt64 size"))?
            };
            metadata.validate_size(size).map_err(vortex_error)?;
            Ok(Value::UInt(size))
        });
    }
    let storage = native_embedding::storage(array.dtype()).unwrap_or(array.dtype());
    if let DType::FixedSizeList(_, size, _) = storage {
        let _admission = context.memory().reserve(coordinate_bytes(array.len())?)?;
        let valid = parent_validity(array, context)?;
        return scalar_result(dtype, array.len(), context, |row| {
            Ok(if valid.value(row) {
                Value::UInt(u64::from(*size))
            } else {
                Value::Null
            })
        });
    }
    let list = input::List::new(array, context)?;
    scalar_result(dtype, array.len(), context, |row| {
        list.at(row)?.map_or(Ok(Value::Null), |(_, count)| {
            Ok(Value::UInt(u64::try_from(count).map_err(vortex_error)?))
        })
    })
}

fn coordinate_bytes(rows: usize) -> Result<u64> {
    rows.checked_mul(64)
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("vector inspection admission overflow"))
}

fn element(
    args: &[ArrayRef],
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let list = input::List::new(&args[0], context)?;
    let controls = keys(&args[1], context)?;
    let indices = index_array(args[0].len(), true, context, |row| {
        check(row, context)?;
        let Some((from, count)) = list.at(row)? else {
            return Ok(None);
        };
        if controls.is_null(row)? {
            return Ok(None);
        }
        let count = i128::try_from(count).map_err(vortex_error)?;
        let index = integer(&controls.raw_cell(row)?)?;
        let index = if index < 0 { count + index } else { index };
        if index < 0 || index >= count {
            return Ok(None);
        }
        Ok(Some(
            from.checked_add(usize::try_from(index).map_err(vortex_error)?)
                .ok_or_else(|| failed("vector element coordinate overflow"))?,
        ))
    })?;
    native_payload::take_with_policy(
        &list.elements,
        &indices,
        dtype,
        CopyPolicy::PreserveUnobserved,
        context,
    )
}

fn slice(
    args: &[ArrayRef],
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let list = input::List::new(&args[0], context)?;
    let start = keys(&args[1], context)?;
    let end = args.get(2).map(|array| keys(array, context)).transpose()?;
    let mut spans = ReservedVec::new(context.memory())?;
    spans.reserve(args[0].len())?;
    for row in 0..args[0].len() {
        check(row, context)?;
        let span = if let Some((from, count)) = list.at(row)? {
            if start.is_null(row)?
                || end
                    .as_ref()
                    .map(|end| end.is_null(row))
                    .transpose()?
                    .unwrap_or(false)
            {
                None
            } else {
                let lo = bound(integer(&start.raw_cell(row)?)?, count)?;
                let hi = if let Some(end) = &end {
                    bound(integer(&end.raw_cell(row)?)?, count)?
                } else {
                    count
                };
                Some((
                    from.checked_add(lo)
                        .ok_or_else(|| failed("vector slice offset overflow"))?,
                    hi.saturating_sub(lo),
                ))
            }
        } else {
            None
        };
        spans.values.push(span);
    }
    let offsets = index_array(args[0].len(), false, context, |row| {
        Ok(Some(spans.values[row].map_or(0, |v| v.0)))
    })?;
    let sizes = index_array(args[0].len(), false, context, |row| {
        Ok(Some(spans.values[row].map_or(0, |v| v.1)))
    })?;
    let valid = if dtype.is_nullable() {
        Validity::Array(scalar_result(
            &DType::Bool(Nullability::NonNullable),
            args[0].len(),
            context,
            |row| Ok(Value::Bool(spans.values[row].is_some())),
        )?)
    } else {
        Validity::NonNullable
    };
    let view = ListViewArray::try_new(
        list.elements.clone(),
        offsets,
        sizes,
        valid,
        &mut context.native_session().create_execution_ctx(),
    )
    .map_err(vortex_error)?
    .into_array();
    let rows = index_array(args[0].len(), false, context, |row| Ok(Some(row)))?;
    native_payload::take_with_policy(&view, &rows, dtype, CopyPolicy::PreserveUnobserved, context)
}

fn bound(value: i128, count: usize) -> Result<usize> {
    let count = i128::try_from(count).map_err(vortex_error)?;
    let value = if value < 0 { count + value } else { value };
    usize::try_from(value.clamp(0, count)).map_err(vortex_error)
}

fn tensor_shape(
    array: &ArrayRef,
    shape: &Shape,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let _admission = context.memory().reserve(coordinate_bytes(array.len())?)?;
    let valid = parent_validity(array, context)?;
    let mut output = output::Builder::new(dtype, array.len(), context)?;
    match shape {
        Shape::Fixed(dimensions) => {
            for row in 0..array.len() {
                check(row, context)?;
                output.row(valid.value(row), dimensions.len(), context, |index| {
                    Ok(dimensions[index])
                })?;
            }
        }
        Shape::Variable {
            metadata,
            rank,
            order,
        } => {
            let rank = *rank as usize;
            let mut dimensions = ReservedVec::new(context.memory())?;
            dimensions.reserve(rank)?;
            let mut execution = context.native_session().create_execution_ctx();
            let storage = input::storage(array, context)?
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)?;
            let data = input::List::new(storage.unmasked_field(0), context)?;
            let mut shapes = input::List::new(storage.unmasked_field(1), context)?;
            for row in 0..array.len() {
                check(row, context)?;
                if !valid.value(row) {
                    output.row(false, rank, context, |_| Ok(0))?;
                    continue;
                }
                let (_, elements) = data
                    .at(row)?
                    .ok_or_else(|| failed("present variable tensor requires a data list"))?;
                let (from, count) = shapes
                    .at(row)?
                    .ok_or_else(|| failed("present variable tensor requires a shape list"))?;
                if count != rank {
                    return Err(failed("tensor shape length differs from its declared rank"));
                }
                let span = shapes.prepare(from, count, context)?;
                if span.has_null {
                    return Err(failed("tensor dimensions require nonnull int32 values"));
                }
                dimensions.values.clear();
                for index in 0..count {
                    check(index, context)?;
                    let dimension = span
                        .owner
                        .integer_ordinal(index)?
                        .ok_or_else(|| failed("tensor dimension is null"))?;
                    dimensions
                        .values
                        .push(i32::try_from(dimension).map_err(vortex_error)?);
                }
                metadata
                    .validate_dimensions(rank, dimensions.values.iter().copied().map(Ok), elements)
                    .map_err(vortex_error)?;
                output.row(true, rank, context, |index| {
                    u64::try_from(dimensions.values[order[index]]).map_err(vortex_error)
                })?;
            }
        }
    }
    output.finish(context)
}

// Three accumulators and their finished magnitudes can coexist during cosine.
// Admit this actual fixed scratch footprint before entering any component loop.
fn arithmetic_bytes() -> u64 {
    (3 * (std::mem::size_of::<math::Accumulator>() + std::mem::size_of::<math::Magnitude>())
        + std::mem::size_of::<math::Normalizer>()
        + 256) as u64
}

fn reduce(
    args: &[ArrayRef],
    reduction: Reduction,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let _scratch = context.memory().reserve(arithmetic_bytes())?;
    let mut left = input::List::new(&args[0], context)?;
    let mut right = args
        .get(1)
        .map(|array| input::List::new(array, context))
        .transpose()?;
    scalar_result(dtype, args[0].len(), context, |row| {
        // Parent masks and both present dimensions precede component masks.
        let a = left.at(row)?;
        let b = right.as_ref().map(|right| right.at(row)).transpose()?;
        let Some((from, count)) = a else {
            return Ok(Value::Null);
        };
        let other = match b {
            Some(None) => return Ok(Value::Null),
            Some(Some((from, other))) => {
                if count != other {
                    return Err(failed("vector dimensions differ"));
                }
                Some(from)
            }
            None => None,
        };
        let a = left.prepare(from, count, context)?;
        if a.has_null {
            return Ok(Value::Null);
        }
        let b = match (right.as_mut(), other) {
            (Some(right), Some(from)) => Some(right.prepare(from, count, context)?),
            _ => None,
        };
        if b.as_ref().is_some_and(|b| b.has_null) {
            return Ok(Value::Null);
        }
        reduction_value(&a.owner, b.map(|b| &b.owner), reduction, context)
            .map(|value| value.map_or(Value::Null, Value::Float))
    })
}

fn reduction_value(
    left: &crate::local_primitives::NativeNumericOwner,
    right: Option<&crate::local_primitives::NativeNumericOwner>,
    reduction: Reduction,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<f64>> {
    let mut sum = math::Accumulator::new();
    let mut left_square = math::Accumulator::new();
    let mut right_square = math::Accumulator::new();
    let (mut nan, mut positive_infinity, mut negative_infinity) = (false, false, false);
    left.visit_numeric(|index, value, _| {
        check(index, context)?;
        let a = value.ok_or_else(|| failed("vector mask changed during arithmetic"))?;
        let b = if let Some(right) = right {
            right
                .numeric_value(index)?
                .ok_or_else(|| failed("vector mask changed during arithmetic"))?
        } else {
            a
        };
        if !context.float_policy().admits(a) || !context.float_policy().admits(b) {
            return Err(failed(
                "nonfinite vector components require float_policy=extended",
            ));
        }
        if a.is_finite() && b.is_finite() {
            match reduction {
                Reduction::Dot => sum.product(a, b, false, false)?,
                Reduction::Norm => sum.product(a, a, false, false)?,
                Reduction::L2 => {
                    sum.product(a, a, false, false)?;
                    sum.product(a, b, true, true)?;
                    sum.product(b, b, false, false)?;
                }
                Reduction::Cosine => {
                    sum.product(a, b, false, false)?;
                    left_square.product(a, a, false, false)?;
                    right_square.product(b, b, false, false)?;
                }
            }
        } else {
            match reduction {
                Reduction::Dot => {
                    let product = a * b;
                    nan |= product.is_nan();
                    positive_infinity |= product == f64::INFINITY;
                    negative_infinity |= product == f64::NEG_INFINITY;
                }
                Reduction::Norm => {
                    nan |= a.is_nan();
                    positive_infinity |= a.is_infinite();
                }
                Reduction::L2 => {
                    let difference = a - b;
                    nan |= difference.is_nan();
                    positive_infinity |= difference.is_infinite();
                }
                Reduction::Cosine => nan = true,
            }
        }
        Ok(())
    })?;
    if nan || (positive_infinity && negative_infinity) {
        return Ok(Some(nan_value()));
    }
    if positive_infinity {
        return Ok(Some(f64::INFINITY));
    }
    if negative_infinity {
        return Ok(Some(f64::NEG_INFINITY));
    }
    let sum = sum.finish();
    let result = match reduction {
        Reduction::Dot => Some(sum.rounded()),
        Reduction::Norm | Reduction::L2 => Some(sum.sqrt()?),
        Reduction::Cosine => math::cosine(&sum, &left_square.finish(), &right_square.finish())?,
    };
    if result.is_some_and(|value| !context.float_policy().admits(value)) {
        return Err(failed(
            "nonfinite vector result requires float_policy=extended",
        ));
    }
    Ok(result)
}

enum Norm {
    Zero,
    Finite(math::Normalizer),
    Infinite,
    Nan,
}

fn normalization(
    owner: &crate::local_primitives::NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
) -> Result<Norm> {
    let mut square = math::Accumulator::new();
    let (mut nan, mut infinite) = (false, false);
    owner.visit_numeric(|index, value, _| {
        check(index, context)?;
        let value = value.ok_or_else(|| failed("vector mask changed during normalization"))?;
        if !context.float_policy().admits(value) {
            return Err(failed(
                "nonfinite vector components require float_policy=extended",
            ));
        }
        if value.is_finite() {
            square.product(value, value, false, false)?;
        } else {
            nan |= value.is_nan();
            infinite |= value.is_infinite();
        }
        Ok(())
    })?;
    if nan {
        Ok(Norm::Nan)
    } else if infinite {
        Ok(Norm::Infinite)
    } else {
        Ok(math::Normalizer::new(&square.finish())?.map_or(Norm::Zero, Norm::Finite))
    }
}

fn normalize(
    array: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let _scratch = context.memory().reserve(arithmetic_bytes())?;
    let mut list = input::List::new(array, context)?;
    let mut output = output::Builder::new(dtype, array.len(), context)?;
    for row in 0..array.len() {
        check(row, context)?;
        let Some((from, count)) = list.at(row)? else {
            output.row(false, 0, context, |_| Ok(0))?;
            continue;
        };
        let span = list.prepare(from, count, context)?;
        if span.has_null {
            output.row(false, 0, context, |_| Ok(0))?;
            continue;
        }
        let norm = normalization(&span.owner, context)?;
        output.row(!matches!(norm, Norm::Zero), count, context, |index| {
            let value = span
                .owner
                .numeric_value(index)?
                .ok_or_else(|| failed("vector mask changed during normalization"))?;
            let value = match &norm {
                Norm::Finite(normalizer) => normalizer.component(value),
                Norm::Infinite => {
                    if value.is_infinite() {
                        nan_value()
                    } else {
                        0.0_f64.copysign(value)
                    }
                }
                Norm::Nan => nan_value(),
                Norm::Zero => unreachable!("zero norm emits a null parent"),
            };
            if !context.float_policy().admits(value) {
                return Err(failed(
                    "nonfinite vector result requires float_policy=extended",
                ));
            }
            Ok(value.to_bits())
        })?;
    }
    output.finish(context)
}

fn nan_value() -> f64 {
    f64::from_bits(shardloom_core::float_policy::CANONICAL_F64_NAN_BITS)
}
