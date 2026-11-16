//! Compose a demanded selection over an already validated native text dictionary.

use super::{AggregateDistinctValue, NativeNumericOwner, failed, vortex_error};
use crate::local_primitives::result_batch;
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayRef, ExecutionCtx, IntoArray as _,
    arrays::{
        Bool, Dict, DictArray, Primitive, VarBinView, dict::DictArraySlotsExt as _,
        varbinview::VarBinViewArrayExt as _,
    },
    dtype::{NativePType, PType},
    memory::HostBufferElement,
    validity::Validity,
};

#[cfg(test)]
#[path = "native_relational_selected_dictionary_tests.rs"]
mod tests;

pub(super) fn compose(
    array: &ArrayRef,
    context: &mut ExecutionCtx,
    cancellation: &CancellationToken,
) -> Result<Option<ArrayRef>> {
    let Some(selection) = array.as_opt::<Dict>() else {
        return Ok(None);
    };
    let Some(dictionary) = selection.values().as_opt::<Dict>() else {
        return Ok(None);
    };
    let Some(values) = dictionary.values().as_opt::<VarBinView>() else {
        return Ok(None);
    };
    if values.len() >= array.len()
        || !dictionary.values().is_host()
        || (!values.is_empty() && !values.has_validated_utf8())
    {
        return Ok(None);
    }
    // Ordinary preparation evaluates the complete inner dictionary before its
    // outer selection. Preserve that obligation even for discarded inner rows.
    // Only plain host code buffers are considered; no deferred provider or
    // discarded text is evaluated to decide whether this path is applicable.
    let Some(codes) = plain_codes(dictionary.codes(), values.len(), context, cancellation)? else {
        return Ok(None);
    };
    let Some(indices) = plain_codes(selection.codes(), dictionary.len(), context, cancellation)?
    else {
        return Ok(None);
    };
    // Vortex's Dict::take has the right code-composition semantics, but its
    // fixed-width gather uses the infallible BufferAllocator, not our credited
    // fallible host allocator. Reuse the existing typed native result builder
    // for the receiving codes, keeping original width and nullable selection.
    macro_rules! copy {
        ($type:ty) => {
            copy_codes::<$type>(&codes, &indices, context, cancellation)
        };
    }
    let copied = match codes.ptype() {
        PType::U8 => copy!(u8),
        PType::U16 => copy!(u16),
        PType::U32 => copy!(u32),
        PType::U64 => copy!(u64),
        PType::I8 => copy!(i8),
        PType::I16 => copy!(i16),
        PType::I32 => copy!(i32),
        PType::I64 => copy!(i64),
        _ => return Ok(None),
    }?;
    let composed = DictArray::try_new(copied, dictionary.values().clone())
        .map_err(vortex_error)?
        .into_array();
    if composed.dtype() != array.dtype() || composed.len() != array.len() {
        return Err(failed("dictionary selection changed dtype or row count"));
    }
    Ok(Some(composed))
}

fn plain_codes(
    array: &ArrayRef,
    domain: usize,
    context: &mut ExecutionCtx,
    cancellation: &CancellationToken,
) -> Result<Option<NativeNumericOwner>> {
    let Some(codes) = array.as_opt::<Primitive>().filter(|_| array.is_host()) else {
        return Ok(None);
    };
    match vortex::array::arrays::primitive::PrimitiveArrayExt::validity(&codes) {
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => {}
        Validity::Array(mask) if mask.is::<Bool>() && mask.is_host() => {}
        Validity::Array(_) => return Ok(None),
    }
    let codes = NativeNumericOwner::new(codes.into_owned(), context)?;
    if !codes.is_integer() {
        return Ok(None);
    }
    for row in 0..codes.len() {
        if row.is_multiple_of(1024) {
            cancellation.check()?;
        }
        let code = match codes.distinct_value(row)? {
            AggregateDistinctValue::Null => continue,
            AggregateDistinctValue::UInt64(value) => usize::try_from(value),
            AggregateDistinctValue::Int64(value) => usize::try_from(value),
            _ => return Ok(None),
        };
        if code.map_or(true, |code| code >= domain) {
            return Ok(None);
        }
    }
    Ok(Some(codes))
}

fn copy_codes<T: NativePType + HostBufferElement + Copy>(
    codes: &NativeNumericOwner,
    indices: &NativeNumericOwner,
    context: &ExecutionCtx,
    cancellation: &CancellationToken,
) -> Result<ArrayRef> {
    let dtype = codes
        .primitive()
        .dtype()
        .union_nullability(indices.primitive().dtype().nullability());
    let values = codes.primitive().as_slice::<T>();
    result_batch::build_numeric_column::<T>(
        &dtype,
        indices.len(),
        &context.host_allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                cancellation.check()?;
            }
            let index = match indices.distinct_value(row)? {
                AggregateDistinctValue::Null => return Ok(None),
                AggregateDistinctValue::UInt64(value) => {
                    usize::try_from(value).map_err(vortex_error)?
                }
                AggregateDistinctValue::Int64(value) => {
                    usize::try_from(value).map_err(vortex_error)?
                }
                _ => return Err(failed("dictionary selection must use integer codes")),
            };
            Ok(codes.validity().value(index).then_some(values[index]))
        },
    )
}
