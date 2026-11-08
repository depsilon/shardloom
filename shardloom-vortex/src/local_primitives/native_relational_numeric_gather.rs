//! Gather already resident primitive leaves into their final credited buffers.

use crate::local_primitives::{
    native_capacity::ReservedVec, native_numeric_owner::NativeNumericOwner, result_batch,
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{Bool, Chunked, Primitive, chunked::ChunkedArrayExt},
    dtype::{DType, NativePType, PType, half::f16},
    memory::{HostBufferElement, HostMemorySessionExt as _},
    validity::Validity,
};

#[cfg(test)]
#[path = "native_relational_numeric_gather_tests.rs"]
mod tests;

struct Chunk {
    start: usize,
    values: NativeNumericOwner,
}

// Inspect only physical storage whose value and validity access cannot execute
// another provider. Encoded leaves retain selected-input provider evaluation.
fn primitive_leaf(mut array: &ArrayRef) -> Option<&ArrayRef> {
    for _ in 0..8 {
        let Some(chunked) = array.as_opt::<Chunked>() else {
            break;
        };
        if chunked.nchunks() != 1 {
            return None;
        }
        array = array
            .slots()
            .get(vortex::array::arrays::chunked::ChunkedSlots::CHUNKS_OFFSET)?
            .as_ref()?;
    }
    let primitive = array.as_opt::<Primitive>()?;
    if !array.is_host() {
        return None;
    }
    match vortex::array::arrays::primitive::PrimitiveArrayExt::validity(&primitive) {
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => Some(array),
        Validity::Array(mask) if mask.is::<Bool>() && mask.is_host() => Some(array),
        Validity::Array(_) => None,
    }
}

pub(super) fn try_copy(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let DType::Primitive(ptype, _) = dtype else {
        return Ok(None);
    };
    if array.dtype().as_nonnullable() != dtype.as_nonnullable()
        || (!dtype.is_nullable() && (array.dtype().is_nullable() || indices.dtype().is_nullable()))
        || !matches!(indices.dtype(), DType::Primitive(PType::U64, _))
        || primitive_leaf(indices).is_none()
    {
        return Ok(None);
    }
    let chunked = array.as_opt::<Chunked>();
    let chunks = chunked.as_ref().map_or(1, ChunkedArrayExt::nchunks);
    let source = |index| chunked.as_ref().map_or(array, |array| array.chunk(index));
    if (0..chunks).any(|index| primitive_leaf(source(index)).is_none()) {
        return Ok(None);
    }
    let mut execution = context.native_session().create_execution_ctx();
    let selection = NativeNumericOwner::new(
        primitive_leaf(indices)
            .expect("admitted selection")
            .as_opt::<Primitive>()
            .expect("admitted primitive")
            .into_owned(),
        &mut execution,
    )?;
    let positions = selection.primitive().as_slice::<u64>();
    // Check the complete index domain before selected-value validation. The
    // pinned unchecked scalar take kernel can panic on an invalid coordinate.
    for (row, &position) in positions.iter().enumerate() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if selection.validity().value(row)
            && usize::try_from(position).map_or(true, |position| position >= array.len())
        {
            return Err(super::failed("gather index exceeds native input"));
        }
    }
    let mut owners = ReservedVec::new(context.memory())?;
    owners.reserve(chunks)?;
    let mut start = 0;
    for index in 0..chunks {
        context.check_cancelled()?;
        let source = source(index);
        let leaf = primitive_leaf(source).expect("admitted source");
        if !leaf.is_empty() {
            owners.values.push(Chunk {
                start,
                values: NativeNumericOwner::new(
                    leaf.as_opt::<Primitive>()
                        .expect("admitted primitive")
                        .into_owned(),
                    &mut execution,
                )?,
            });
        }
        start += source.len();
    }
    macro_rules! copy {
        ($type:ty, $validate:expr) => {
            copy::<$type>(dtype, &owners.values, &selection, context, $validate)
        };
    }
    let output = match ptype {
        PType::U8 => copy!(u8, |_| Ok(())),
        PType::U16 => copy!(u16, |_| Ok(())),
        PType::U32 => copy!(u32, |_| Ok(())),
        PType::U64 => copy!(u64, |_| Ok(())),
        PType::I8 => copy!(i8, |_| Ok(())),
        PType::I16 => copy!(i16, |_| Ok(())),
        PType::I32 => copy!(i32, |_| Ok(())),
        PType::I64 => copy!(i64, |_| Ok(())),
        PType::F32 => copy!(f32, |value| result_batch::validate_numeric_output(
            f64::from(value),
            context.float_policy()
        )),
        PType::F64 => copy!(f64, |value| result_batch::validate_numeric_output(
            value,
            context.float_policy()
        )),
        PType::F16 => copy!(f16, |value: f16| result_batch::validate_numeric_output(
            shardloom_core::float_policy::widen_f16_bits(value.to_bits()),
            context.float_policy()
        )),
    }?;
    #[cfg(all(test, debug_assertions))]
    crate::local_primitives::native_relational_observation::record(|counts| {
        counts.numeric_output_copy_batches += 1;
        counts.numeric_output_copy_rows += indices.len() as u64;
        counts.numeric_gather_direct_batches += 1;
        counts.numeric_gather_direct_rows += indices.len() as u64;
    });
    context.check_cancelled()?;
    Ok(Some(output))
}

fn copy<T: NativePType + HostBufferElement + Copy>(
    dtype: &DType,
    sources: &[Chunk],
    selection: &NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
    validate: impl Fn(T) -> Result<()>,
) -> Result<ArrayRef> {
    let positions = selection.primitive().as_slice::<u64>();
    result_batch::build_numeric_column::<T>(
        dtype,
        positions.len(),
        &context.native_session().host_allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if !selection.validity().value(row) {
                return Ok(None);
            }
            let position = usize::try_from(positions[row]).map_err(vortex_error)?;
            let index = sources.partition_point(|source| source.start <= position) - 1;
            let source = &sources[index];
            let local = position - source.start;
            if !source.values.validity().value(local) {
                return Ok(None);
            }
            let value = source.values.primitive().as_slice::<T>()[local];
            validate(value)?;
            Ok(Some(value))
        },
    )
}
