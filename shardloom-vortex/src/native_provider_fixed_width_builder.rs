//! Own the output of pinned fixed-width Chunked builders.
//!
//! Keep each native append strategy: pre-decoding leaves would discard direct
//! `BitPacked` append and other work avoidance. Child codecs, encoded validity
//! and selection scratch retain their separate resource boundaries.

use super::*;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{arrays::Decimal, builders::builder_with_capacity, dtype::DecimalType};

pub(super) fn decode(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
) -> VortexResult<ArrayRef> {
    let values = value_capacity(array.dtype(), array.len())?;
    let validity = if array.dtype().is_nullable() {
        bitmap_capacity(array.len())?
    } else {
        0
    };
    // Every fixed-width builder replaces its values with an empty BufferMut
    // on finish. Even that empty buffer requests the preferred alignment.
    let finish_bytes = capacity(0)?;
    let mut values_credit =
        crate::owned_buffers::reserve(memory, add(add(values, validity)?, finish_bytes)?)?;
    let validity_credit = values_credit
        .split(validity)
        .map_err(|error| vortex_err!("{error}"))?;
    let finish_credit = values_credit
        .split(finish_bytes)
        .map_err(|error| vortex_err!("{error}"))?;

    let mut builder = builder_with_capacity(array.dtype(), array.len());
    // Chunked appends nested chunks into this same builder. Native leaf append
    // implementations own their decode decisions; no second concatenation or
    // per-row adapter representation is introduced here.
    array.append_to_builder(builder.as_mut(), ctx)?;
    vortex_ensure!(
        builder.len() == array.len(),
        "native fixed-width builder changed row count"
    );
    let output = builder.finish();
    drop(builder);
    drop(finish_credit);
    vortex_ensure!(
        output.dtype() == array.dtype() && output.len() == array.len(),
        "native fixed-width builder changed dtype or row count"
    );

    let bitmap = retained_validity(&output, validity_credit)?;
    let retained = if output.is::<Primitive>() {
        retain(&Primitive, &output, values_credit, bitmap)?
    } else if output.is::<Bool>() {
        retain(&Bool, &output, values_credit, bitmap)?
    } else if output.is::<Decimal>() {
        retain(&Decimal, &output, values_credit, bitmap)?
    } else {
        vortex_bail!("native fixed-width builder returned a noncanonical output");
    };
    retained.statistics().inherit_from(array.statistics());
    Ok(retained)
}

fn value_capacity(dtype: &DType, rows: usize) -> VortexResult<u64> {
    let bytes = match dtype {
        DType::Primitive(ptype, _) => mul(rows, ptype.byte_width())?,
        DType::Bool(_) => return bitmap_capacity(rows),
        DType::Decimal(decimal, _) => mul(
            rows,
            DecimalType::smallest_decimal_value_type(decimal).byte_width(),
        )?,
        _ => vortex_bail!("native fixed-width builder requires a fixed-width dtype"),
    };
    allocation_capacity(bytes)
}

fn bitmap_capacity(rows: usize) -> VortexResult<u64> {
    allocation_capacity(u64::try_from(rows.div_ceil(8)).map_err(|_| size_overflow())?)
}

fn allocation_capacity(bytes: u64) -> VortexResult<u64> {
    let bytes = capacity(bytes)?;
    vortex_ensure!(
        isize::try_from(bytes).is_ok(),
        "native fixed-width builder allocation size overflow"
    );
    Ok(bytes)
}

fn retained_validity(array: &ArrayRef, credit: MemoryLease) -> VortexResult<Option<ArrayRef>> {
    match array.validity()? {
        Validity::Array(bitmap) => {
            let native = bitmap.as_::<Bool>();
            let buffer = crate::owned_buffers::retain_credit(bitmap.buffers()[0].clone(), credit);
            Ok(Some(
                BoolArray::try_from_parts(
                    Bool.with_buffers(native, &[BufferHandle::new_host(buffer)])?,
                )?
                .into_array(),
            ))
        }
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => {
            drop(credit);
            Ok(array.slots()[0].clone())
        }
    }
}

fn retain<V: ArrayVTable>(
    vtable: &V,
    output: &ArrayRef,
    credit: MemoryLease,
    validity: Option<ArrayRef>,
) -> VortexResult<ArrayRef> {
    let mut buffers = output.buffers();
    vortex_ensure!(
        buffers.len() == 1,
        "native fixed-width builder returned an unexpected buffer count"
    );
    let buffer = crate::owned_buffers::retain_credit(buffers.remove(0), credit);
    Array::<V>::try_from_parts(
        vtable
            .with_buffers(output.as_::<V>(), &[BufferHandle::new_host(buffer)])?
            .with_slots(std::iter::once(validity).collect()),
    )
    .map(IntoArray::into_array)
}
