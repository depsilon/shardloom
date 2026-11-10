//! Admission for the pinned provider's temporary native/Arrow format work.
//!
//! Count detached physical buffers, schema/row geometry, and the expanded size
//! of encoded values. Encoded object offsets may alias large values, so buffer
//! bytes alone cannot bound the provider's rebuilt output or scalar workspace.

use super::{ArrayRef, DType, NativeExecutionContext, Result, failed, vortex_error};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    ExecutionCtx, VortexSessionExecute as _,
    arrays::{StructArray, struct_::StructArrayExt as _},
    scalar_fn::fns::variant_get::{VariantGetOptions, VariantPathElement},
};
use vortex::encodings::parquet_variant::{ParquetVariant, ParquetVariantArraySlotsExt as _};

pub(super) fn reserve(
    source: &ArrayRef,
    target: &DType,
    options: Option<&VariantGetOptions>,
    context: &NativeExecutionContext<'_>,
) -> Result<MemoryLease> {
    let mut credit = context.memory().reserve(4096)?;
    add(
        &mut credit,
        crate::native_payload_schema::metadata_bytes(target, context)?,
    )?;
    if let Some(options) = options {
        for step in options.path().elements() {
            let bytes = match step {
                VariantPathElement::Field(name) => name.as_ref().len() as u64,
                VariantPathElement::Index(_) => 0,
            };
            add(
                &mut credit,
                bytes
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(256))
                    .ok_or_else(overflow)?,
            )?;
        }
    }
    physical(source, &mut credit, context, 0)?;
    Ok(credit)
}

fn add(credit: &mut MemoryLease, bytes: u64) -> Result<()> {
    credit.resize(credit.bytes().checked_add(bytes).ok_or_else(overflow)?)
}

fn overflow() -> shardloom_core::ShardLoomError {
    failed("Variant provider workspace overflow")
}

fn physical(
    array: &ArrayRef,
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<()> {
    context.check_cancelled()?;
    // This is the already validated/copied physical tree, which can include
    // Variant shells around the existing 24-level logical payload boundary.
    if depth > 128 {
        return Err(failed(
            "Variant provider physical storage exceeds its admitted depth",
        ));
    }
    let metadata = crate::native_payload_schema::metadata_bytes(array.dtype(), context)?;
    let rows = u64::try_from(array.len()).map_err(|_| overflow())?;
    add(
        credit,
        metadata
            .checked_mul(rows.saturating_add(1))
            .and_then(|n| n.checked_add(rows.checked_mul(128)?))
            .ok_or_else(overflow)?,
    )?;

    // Admit the temporary handle vector before asking the provider to create it.
    let handles = (array.nbuffers() as u64)
        .checked_mul(64)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(overflow)?;
    let _inspection = context.memory().reserve(handles)?;
    for buffer in array.buffer_handles() {
        if !buffer.is_on_host() {
            return Err(failed("Variant expression requires admitted host storage"));
        }
        add(
            credit,
            (buffer.len() as u64)
                .checked_mul(128)
                .ok_or_else(overflow)?,
        )?;
    }
    if let Some(storage) = array.as_opt::<ParquetVariant>() {
        let mut execution = context.native_session().create_execution_ctx();
        for row in 0..array.len() {
            context.check_cancelled()?;
            if array
                .is_invalid(row, &mut execution)
                .map_err(vortex_error)?
            {
                continue;
            }
            let metadata = binary(storage.metadata(), row, &mut execution)?
                .ok_or_else(|| failed("present Variant requires metadata"))?;
            raw(
                storage.value(),
                row,
                metadata.as_slice(),
                credit,
                context,
                &mut execution,
            )?;
            if let Some(typed) = storage.typed_value() {
                typed_value(
                    typed,
                    row,
                    metadata.as_slice(),
                    credit,
                    context,
                    &mut execution,
                    0,
                )?;
            }
        }
    }
    for child in array.children_iter() {
        physical(child, credit, context, depth + 1)?;
    }
    Ok(())
}

fn binary(
    array: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
) -> Result<Option<vortex::buffer::ByteBuffer>> {
    Ok(array
        .execute_scalar(row, execution)
        .map_err(vortex_error)?
        .as_binary()
        .value()
        .cloned())
}

fn raw(
    array: Option<&ArrayRef>,
    row: usize,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
) -> Result<()> {
    let value = array
        .map(|array| binary(array, row, execution))
        .transpose()?
        .flatten();
    if let Some(value) = value {
        // The same full validator and alias-work envelope precede this borrowed
        // value walk. It allocates no decoded strings, object maps or row values.
        let _validation = context.memory().reserve(crate::native_variant::workspace(
            metadata
                .len()
                .checked_add(value.len())
                .ok_or_else(overflow)?,
        )?)?;
        let value = crate::native_variant::value(metadata, value.as_slice(), &|| {
            context.check_cancelled()
        })?;
        expanded(&value, credit, context)?;
    }
    Ok(())
}

fn expanded(
    value: &parquet_variant::Variant<'_, '_>,
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    use parquet_variant::Variant as V;
    context.check_cancelled()?;
    // Covers scalar/row-builder nodes, validity, offsets, growth and the
    // simultaneous normalization, extraction and final-detachment stages.
    add(credit, 8192)?;
    match value {
        V::Binary(bytes) => add(
            credit,
            (bytes.len() as u64).checked_mul(128).ok_or_else(overflow)?,
        )?,
        V::String(text) => add(
            credit,
            (text.len() as u64).checked_mul(128).ok_or_else(overflow)?,
        )?,
        V::ShortString(text) => add(
            credit,
            (text.as_str().len() as u64)
                .checked_mul(128)
                .ok_or_else(overflow)?,
        )?,
        V::List(list) => {
            for value in list.iter() {
                expanded(&value, credit, context)?;
            }
        }
        V::Object(object) => {
            for (name, value) in object.iter() {
                add(
                    credit,
                    (name.len() as u64).checked_mul(128).ok_or_else(overflow)?,
                )?;
                expanded(&value, credit, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn shell(
    array: &ArrayRef,
    row: usize,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
    depth: usize,
) -> Result<()> {
    context.check_cancelled()?;
    if array.is_invalid(row, execution).map_err(vortex_error)? {
        return Ok(());
    }
    let structure = array
        .clone()
        .execute::<StructArray>(execution)
        .map_err(vortex_error)?;
    let field = |name: &str| {
        structure
            .names()
            .iter()
            .position(|field| field.as_ref() == name)
            .map(|index| structure.unmasked_field(index))
    };
    raw(field("value"), row, metadata, credit, context, execution)?;
    if let Some(typed) = field("typed_value") {
        typed_value(typed, row, metadata, credit, context, execution, depth)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn typed_value(
    array: &ArrayRef,
    row: usize,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
    depth: usize,
) -> Result<()> {
    context.check_cancelled()?;
    if depth > 24 {
        return Err(failed(
            "Variant typed workspace exceeds the current depth boundary",
        ));
    }
    if array.is_invalid(row, execution).map_err(vortex_error)? {
        return Ok(());
    }
    match array.dtype() {
        DType::Struct(..) => {
            let structure = array
                .clone()
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            for index in 0..structure.names().len() {
                shell(
                    structure.unmasked_field(index),
                    row,
                    metadata,
                    credit,
                    context,
                    execution,
                    depth + 1,
                )?;
            }
        }
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(array.clone(), execution)?;
            if let Some((start, count)) = list.coordinates(row, execution)? {
                for index in start..start + count {
                    shell(
                        &list.elements,
                        index,
                        metadata,
                        credit,
                        context,
                        execution,
                        depth + 1,
                    )?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
