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
    walk::physical(array, credit, context, depth, 128)
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
    walk::expanded(value, credit, context)
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
    walk::typed_value(array, row, metadata, credit, context, execution, depth, 24)
}

#[path = "native_relational_variant_workspace_walk.rs"]
mod walk;

#[cfg(test)]
#[path = "native_relational_variant_workspace_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_relational_variant_workspace_tests.rs"]
mod tests;
