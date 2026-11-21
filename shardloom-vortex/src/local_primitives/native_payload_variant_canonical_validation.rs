//! Validate the provider's canonical typed tree against its encoded residual.

use super::{NativeExecutionContext, Result, Storage, check, failed, validation, vortex_error};
use parquet_variant::{Variant as Value, VariantMetadata};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, VortexSessionExecute as _,
        arrays::{
            Chunked, Constant, Dict, Masked, Slice, StructArray, Variant,
            chunked::ChunkedArrayExt as _,
            dict::DictArraySlotsExt as _,
            masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
            slice::SliceArraySlotsExt as _,
            struct_::StructArrayExt as _,
            variant::VariantArraySlotsExt as _,
        },
        dtype::DType,
    },
    buffer::ByteBuffer,
    encodings::parquet_variant::{
        ParquetVariant, ParquetVariantArrayExt as _, ParquetVariantArraySlotsExt as _,
    },
};

pub(in crate::local_primitives::native_payload) fn validate(
    core: &ArrayRef,
    typed: &ArrayRef,
    rows: &[Option<usize>],
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut execution = context.native_session().create_execution_ctx();
    for (ordinal, row) in rows.iter().enumerate() {
        check(ordinal, context)?;
        let Some(row) = row else { continue };
        let Some((metadata, value)) = encoded_core(core.clone(), *row, &mut execution, context)?
        else {
            continue;
        };
        let bytes = metadata
            .len()
            .checked_add(value.as_ref().map_or(0, ByteBuffer::len))
            .ok_or_else(|| failed("Variant validation workspace overflow"))?;
        let _credit = context
            .memory()
            .reserve(crate::native_variant::workspace(bytes)?)?;
        VariantMetadata::try_new(metadata.as_slice()).map_err(vortex_error)?;
        let parsed = value
            .as_ref()
            .map(|value| {
                crate::native_variant::value(metadata.as_slice(), value.as_slice(), &|| {
                    context.check_cancelled()
                })
            })
            .transpose()?;
        logical(
            metadata.as_slice(),
            parsed.as_ref(),
            typed,
            *row,
            &mut execution,
            context,
            0,
        )?;
    }
    context.check_cancelled()
}

// Follow native coordinates without asking an infallible Variant scalar accessor
// to decode unvalidated bytes. Every wrapper consumes the existing depth budget.
#[allow(clippy::type_complexity)]
fn encoded_core(
    mut array: ArrayRef,
    mut row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<(ByteBuffer, Option<ByteBuffer>)>> {
    for _ in 0..=64 {
        context.check_cancelled()?;
        if row >= array.len() {
            return Err(failed("Variant core index exceeds its rows"));
        }
        array = array
            .execute_until::<Storage>(execution)
            .map_err(vortex_error)?;
        if let Some(storage) = array.as_opt::<ParquetVariant>() {
            if !storage
                .parquet_variant_validity()
                .execute_is_valid(row, execution)
                .map_err(vortex_error)?
            {
                return Ok(None);
            }
            if storage.typed_value().is_some() {
                return Err(failed(
                    "multiple levels of Variant shredding are not admitted",
                ));
            }
            let metadata = validation::binary(storage.metadata(), row, execution)?
                .ok_or_else(|| failed("present Variant requires nonnull metadata"))?;
            let value = storage
                .value()
                .map(|value| validation::binary(value, row, execution))
                .transpose()?
                .flatten();
            return Ok(Some((metadata, value)));
        }
        if let Some(chunks) = array.as_opt::<Chunked>() {
            let (chunk, offset) = chunks.find_chunk_idx(row).map_err(vortex_error)?;
            array = chunks.chunk(chunk).clone();
            row = offset;
        } else if let Some(dictionary) = array.as_opt::<Dict>() {
            let code = dictionary
                .codes()
                .execute_scalar(row, execution)
                .map_err(vortex_error)?;
            if code.is_null() {
                return Ok(None);
            }
            row = code
                .as_primitive()
                .as_::<usize>()
                .ok_or_else(|| failed("invalid Variant dictionary index"))?;
            array = dictionary.values().clone();
        } else if let Some(slice) = array.as_opt::<Slice>() {
            row = row
                .checked_add(slice.slice_range().start)
                .ok_or_else(|| failed("Variant slice index overflow"))?;
            array = slice.child().clone();
        } else if let Some(masked) = array.as_opt::<Masked>() {
            if !masked
                .masked_validity()
                .execute_is_valid(row, execution)
                .map_err(vortex_error)?
            {
                return Ok(None);
            }
            array = masked.child().clone();
        } else if let Some(variant) = array.as_opt::<Variant>() {
            if variant.shredded().is_some() {
                return Err(failed(
                    "multiple levels of Variant shredding are not admitted",
                ));
            }
            array = variant.core_storage().clone();
        } else if array
            .as_opt::<Constant>()
            .is_some_and(|constant| constant.scalar().is_null())
        {
            return Ok(None);
        } else {
            return Err(failed(
                "canonical shredded Variant requires encoded Parquet metadata; supply Parquet Variant storage",
            ));
        }
    }
    Err(failed(
        "Variant core wrappers exceed the current depth boundary",
    ))
}

#[allow(clippy::too_many_arguments)]
fn logical(
    metadata: &[u8],
    residual: Option<&Value<'_, '_>>,
    typed: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<()> {
    walk::logical(
        metadata, residual, typed, row, execution, context, depth, 24,
    )
}

#[path = "native_payload_variant_canonical_walk.rs"]
mod walk;

#[cfg(test)]
#[path = "native_payload_variant_canonical_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_payload_variant_walk_tests.rs"]
mod walk_tests;
