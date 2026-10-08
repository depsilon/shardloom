//! Compact ownership of admitted Vortex payloads without a decoded row tree.

use super::{
    native_capacity::ReservedVec,
    native_list,
    native_relational_batch::failed,
    result_batch::{self, Value},
    vortex_error,
};
pub(super) use crate::native_payload_schema::metadata_bytes;
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, Columnar, IntoArray, VortexSessionExecute as _,
    arrays::struct_::StructArrayExt as _,
    arrays::{ChunkedArray, FixedSizeListArray, ListArray, StructArray},
    builtins::ArrayBuiltins as _,
    dtype::{DType, Nullability, PType},
    memory::{HostAllocatorRef, MemorySessionExt as _},
    validity::Validity,
};

pub(super) fn is_nested(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::List(..) | DType::FixedSizeList(..) | DType::Struct(..)
    )
}

/// Copy one bounded native payload, retaining schema credits on every child.
pub(super) fn detach(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let indices =
        super::native_relational_batch::index_array(array.len(), false, context, |row| {
            Ok(Some(row))
        })?;
    take(array, &indices, array.dtype(), context)
}

pub(super) fn take(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let metadata = metadata_bytes(dtype)?;
    if source.dtype().as_nonnullable() != dtype.as_nonnullable() {
        return Err(failed(
            "nested payload gathering requires the same declared child types",
        ));
    }
    let allocator = crate::owned_buffers::with_credit(
        context.native_session().allocator(),
        context.memory().reserve(metadata)?,
    );
    let mut execution = context.native_session().create_execution_ctx();
    let mut rows = ReservedVec::new(context.memory())?;
    rows.reserve(indices.len())?;
    for row in 0..indices.len() {
        check(row, context)?;
        let scalar = indices
            .execute_scalar(row, &mut execution)
            .map_err(vortex_error)?;
        let index = if scalar.is_null() {
            if !dtype.is_nullable() {
                return Err(failed(
                    "null gather index requires a nullable output payload",
                ));
            }
            None
        } else {
            Some(
                scalar
                    .as_primitive()
                    .as_::<usize>()
                    .filter(|index| *index < source.len())
                    .ok_or_else(|| failed("payload index exceeds native input"))?,
            )
        };
        rows.values.push(index);
    }
    copy_rows(Some(source), &rows.values, dtype, context, &allocator)
}

/// Typed empty/null output, including a missing outer-join side. Nonnullable
/// children under a null parent receive unobservable, type-correct placeholders.
pub(super) fn defaults(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let allocator = crate::owned_buffers::with_credit(
        context.native_session().allocator(),
        context.memory().reserve(metadata_bytes(dtype)?)?,
    );
    let mut indices = ReservedVec::new(context.memory())?;
    indices.reserve(rows)?;
    indices.values.resize(rows, None);
    copy_rows(None, &indices.values, dtype, context, &allocator)
}

/// Coalesce retained one-row native values into one compact, bounded column.
/// Missing values denote NULL parents; transient chunk metadata is reserved.
pub(super) fn retained_column(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<Option<ArrayRef>>,
) -> Result<ArrayRef> {
    let mut arrays = ReservedVec::new(context.memory())?;
    let mut positions = ReservedVec::new(context.memory())?;
    arrays.reserve(rows)?;
    positions.reserve(rows)?;
    for row in 0..rows {
        check(row, context)?;
        if let Some(array) = value(row)? {
            if array.len() != 1 || array.dtype().as_nonnullable() != dtype.as_nonnullable() {
                return Err(failed(
                    "retained native value changed its bound dtype or row count",
                ));
            }
            positions.values.push(Some(arrays.values.len()));
            arrays
                .values
                .push(array.cast(dtype.clone()).map_err(vortex_error)?);
        } else {
            if !dtype.is_nullable() {
                return Err(failed("missing retained value requires a nullable output"));
            }
            positions.values.push(None);
        }
    }
    if arrays.values.is_empty() {
        return defaults(dtype, rows, context);
    }
    let (arrays, _ownership) = arrays.into_parts();
    let array = ChunkedArray::try_new(arrays, dtype.clone())
        .map_err(vortex_error)?
        .into_array();
    let indices =
        super::native_relational_batch::index_array(rows, dtype.is_nullable(), context, |row| {
            Ok(positions.values[row])
        })?;
    take(&array, &indices, dtype, context)
}

fn check(row: usize, context: &NativeExecutionContext<'_>) -> Result<()> {
    if row.is_multiple_of(1024) {
        context.check_cancelled()?;
    }
    Ok(())
}

fn default_value(dtype: &DType) -> Result<Value<'static>> {
    if dtype.is_nullable() {
        return Ok(Value::Null);
    }
    Ok(match dtype {
        DType::Bool(_) => Value::Bool(false),
        DType::Utf8(_) => Value::Text("".into()),
        DType::Binary(_) => Value::Binary((&[][..]).into()),
        DType::Decimal(dtype, _) if crate::native_payload_schema::admitted_decimal(*dtype) => {
            Value::Decimal(0, *dtype)
        }
        DType::Extension(_) if crate::native_payload_schema::temporal_storage(dtype).is_some() => {
            Value::Int(0)
        }
        DType::Primitive(ptype, _) if ptype.is_signed_int() => Value::Int(0),
        DType::Primitive(ptype, _) if ptype.is_unsigned_int() => Value::UInt(0),
        DType::Primitive(PType::F32 | PType::F64, _) => Value::Float(0.0),
        _ => return Err(failed("payload default requires a scalar leaf")),
    })
}

fn validity(dtype: &DType, valid: &[bool], allocator: &HostAllocatorRef) -> Result<Validity> {
    if !dtype.is_nullable() {
        if valid.iter().any(|valid| !valid) {
            return Err(failed("null in a nonnullable nested payload"));
        }
        return Ok(Validity::NonNullable);
    }
    Ok(Validity::Array(result_batch::build_column(
        &DType::Bool(Nullability::NonNullable),
        valid.len(),
        allocator,
        |row| Ok(Value::Bool(valid[row])),
    )?))
}

fn copy_rows(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    // Reserve fixed-size expansion before invoking the provider's take kernel.
    // Child coordinates and final buffers acquire their own concurrent credits.
    let _fixed_expansion = if let DType::FixedSizeList(_, size, _) = dtype {
        let bytes = rows
            .len()
            .checked_mul(*size as usize)
            .and_then(|n| n.checked_mul(24))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("fixed-size-list expansion overflow"))?;
        Some(context.memory().reserve(bytes)?)
    } else {
        None
    };
    let mut execution = context.native_session().create_execution_ctx();
    let selected = if rows.iter().any(Option::is_some) {
        let source = source.ok_or_else(|| failed("payload source is absent"))?;
        let nullable = rows.iter().any(Option::is_none);
        let indices = result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::from(nullable)),
            rows.len(),
            allocator,
            |row| Ok(rows[row].map_or(Value::Null, |index| Value::UInt(index as u64))),
        )?;
        Some(source.take(indices).map_err(vortex_error)?)
    } else {
        None
    };

    match dtype {
        DType::Struct(fields, _) => copy_struct(selected, rows, dtype, fields, context, allocator),
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => {
            copy_list(selected, rows, dtype, element, context, allocator)
        }
        _ => {
            // Selection precedes leaf canonicalization: hidden or unselected
            // strings never become an owned part of the returned payload.
            let selected = selected
                .map(|array| {
                    array
                        .execute::<Columnar>(&mut execution)
                        .map(IntoArray::into_array)
                        .map_err(vortex_error)
                })
                .transpose()?;
            result_batch::build_column(dtype, rows.len(), allocator, |row| {
                check(row, context)?;
                if rows[row].is_none() {
                    default_value(dtype)
                } else {
                    result_batch::scalar_value(
                        selected.as_ref().expect("selected leaf"),
                        row,
                        &mut execution,
                    )
                }
            })
        }
    }
}

fn copy_struct(
    selected: Option<ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    fields: &vortex::array::dtype::StructFields,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let mut execution = context.native_session().create_execution_ctx();
    let selected = selected
        .map(|array| {
            array
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)
        })
        .transpose()?;
    let mut active = ReservedVec::new(context.memory())?;
    let mut valid = ReservedVec::new(context.memory())?;
    active.reserve(rows.len())?;
    valid.reserve(rows.len())?;
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        let present = if original.is_some() {
            selected
                .as_ref()
                .expect("selected source")
                .struct_validity()
                .execute_is_valid(row, &mut execution)
                .map_err(vortex_error)?
        } else {
            false
        };
        active.values.push(present.then_some(row));
        valid
            .values
            .push(present || (original.is_none() && !dtype.is_nullable()));
    }
    let mut children = ReservedVec::new(context.memory())?;
    children.reserve(fields.nfields())?;
    for (index, child) in fields.fields().enumerate() {
        children.values.push(copy_rows(
            selected.as_ref().map(|array| array.unmasked_field(index)),
            &active.values,
            &child,
            context,
            allocator,
        )?);
    }
    let validity = validity(dtype, &valid.values, allocator)?;
    let (children, _ownership) = children.into_parts();
    StructArray::try_new(fields.names().clone(), children, rows.len(), validity)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn copy_list(
    selected: Option<ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    element: &DType,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let mut execution = context.native_session().create_execution_ctx();
    let selected = selected
        .map(|array| native_list::Column::new(array, &mut execution))
        .transpose()?;
    let mut children = ReservedVec::new(context.memory())?;
    let mut offsets = ReservedVec::new(context.memory())?;
    let mut valid = ReservedVec::new(context.memory())?;
    offsets.reserve(
        rows.len()
            .checked_add(1)
            .ok_or_else(|| failed("list offset overflow"))?,
    )?;
    valid.reserve(rows.len())?;
    offsets.values.push(0usize);
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        let coordinates = if original.is_some() {
            selected
                .as_ref()
                .expect("selected list")
                .coordinates(row, &mut execution)?
        } else {
            None
        };
        valid
            .values
            .push(coordinates.is_some() || (original.is_none() && !dtype.is_nullable()));
        let count = match dtype {
            DType::FixedSizeList(_, size, _) => *size as usize,
            _ => coordinates.map_or(0, |(_, count)| count),
        };
        children.reserve(count)?;
        for offset in 0..count {
            check(offset, context)?;
            children
                .values
                .push(coordinates.map(|(start, _)| start + offset));
        }
        offsets.values.push(children.values.len());
    }
    let child = copy_rows(
        selected.as_ref().map(|column| &column.elements),
        &children.values,
        element,
        context,
        allocator,
    )?;
    let validity = validity(dtype, &valid.values, allocator)?;
    if let DType::FixedSizeList(_, size, _) = dtype {
        FixedSizeListArray::try_new(child, *size, validity, rows.len())
            .map(IntoArray::into_array)
            .map_err(vortex_error)
    } else {
        let offsets = result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::NonNullable),
            offsets.values.len(),
            allocator,
            |row| Ok(Value::UInt(offsets.values[row] as u64)),
        )?;
        ListArray::try_new(child, offsets, validity)
            .map(IntoArray::into_array)
            .map_err(vortex_error)
    }
}
