//! Credited private ordinals and typed records shared by native stateful operators.

use super::{
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::failed,
    native_relational_sort,
    result_batch::{self, Value},
    vortex_error,
};
use crate::{
    relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
    resident_session::NativeExecutionContext,
};
use shardloom_core::{ColumnRef, Result};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{Primitive, PrimitiveArray, StructArray},
        dtype::{DType, FieldNames, Nullability, PType},
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    buffer::Buffer,
};

pub(super) const ORDINAL: &str = "ordinal";

pub(super) fn u64_type() -> DType {
    DType::Primitive(PType::U64, Nullability::NonNullable)
}

pub(super) fn order(
    fields: Vec<(String, DType)>,
    names: Vec<String>,
) -> Result<native_relational_sort::Spec> {
    let keys = names
        .iter()
        .map(|name| {
            Ok(VortexRelationalOrderKey {
                column: ColumnRef::new(name.clone())?,
                descending: false,
                nulls: Some(VortexRelationalNullOrder::First),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(native_relational_sort::Spec {
        fields,
        keys,
        names,
    })
}

pub(super) fn unsigned(
    rows: usize,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<u64>,
) -> Result<ArrayRef> {
    result_batch::build_column(
        &u64_type(),
        rows,
        &context.native_session().allocator(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            value(row).map(Value::UInt)
        },
    )
}

pub(super) fn structure(
    fields: &[(String, DType)],
    columns: ReservedVec<ArrayRef>,
    rows: usize,
) -> Result<ArrayRef> {
    let (mut columns, mut ownership) = columns.into_parts();
    reserve_structure(fields, &mut ownership)?;
    let ordinal = fields
        .iter()
        .position(|(name, _)| name == ORDINAL)
        .ok_or_else(|| failed("private native record has no ordinal owner"))?;
    let anchor = columns
        .get(ordinal)
        .ok_or_else(|| failed("native ordinal column is absent"))?;
    if anchor.dtype() != &u64_type() {
        return Err(failed("native ordinal owner has the wrong dtype"));
    }
    let anchor = anchor
        .as_opt::<Primitive>()
        .ok_or_else(|| failed("native ordinal owner is not primitive"))?;
    let buffer = crate::owned_buffers::retain_credit(
        anchor.to_buffer::<u64>().into_byte_buffer(),
        ownership,
    );
    columns[ordinal] = PrimitiveArray::new(
        Buffer::<u64>::from_byte_buffer(buffer),
        Validity::NonNullable,
    )
    .into_array();
    structure_inner(fields, columns, rows)
}

fn reserve_structure(fields: &[(String, DType)], ownership: &mut MemoryLease) -> Result<()> {
    let metadata = fields.iter().try_fold(1024_u64, |bytes, (name, dtype)| {
        native_payload::metadata_bytes(dtype)?
            .checked_add(name.len() as u64 * 2)
            .and_then(|field| bytes.checked_add(field))
            .ok_or_else(|| failed("native record metadata capacity overflow"))
    })?;
    ownership.resize(
        ownership
            .bytes()
            .checked_add(metadata)
            .ok_or_else(|| failed("native record metadata capacity overflow"))?,
    )
}

pub(super) fn delivered(
    fields: &[(String, DType)],
    columns: ReservedVec<ArrayRef>,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let (columns, mut ownership) = columns.into_parts();
    reserve_structure(fields, &mut ownership)?;
    let array = structure_inner(fields, columns, rows)?;
    // Removing a private ordinal removes its ownership anchor. Compact through
    // the common builder so every surviving public child retains metadata.
    let delivered = native_payload::detach(&array, context)?;
    drop((array, ownership));
    Ok(delivered)
}

fn structure_inner(
    fields: &[(String, DType)],
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<ArrayRef> {
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows,
        Validity::NonNullable,
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(vortex_error)
}
