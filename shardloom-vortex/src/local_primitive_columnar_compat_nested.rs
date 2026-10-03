//! Recursive type and byte admission at the explicit Arrow output boundary.

use super::{CompatibilityLimits, add, error};
use crate::local_primitives::{usize_to_u64, vortex_error};
use arrow_schema::{DataType, Field};
use shardloom_core::Result;
use std::sync::Arc;
use vortex::array::{
    ArrayRef, Columnar, ExecutionCtx, IntoArray,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::struct_::StructArrayExt as _,
    arrays::{FixedSizeListArray, ListViewArray, StructArray, VarBinViewArray},
    dtype::{DType, PType},
};

pub(super) fn data_type(dtype: &DType) -> Option<DataType> {
    Some(match dtype {
        DType::Bool(_) => DataType::Boolean,
        DType::Utf8(_) => DataType::Utf8,
        DType::Primitive(ptype, _) => match ptype {
            PType::I8 => DataType::Int8,
            PType::I16 => DataType::Int16,
            PType::I32 => DataType::Int32,
            PType::I64 => DataType::Int64,
            PType::U8 => DataType::UInt8,
            PType::U16 => DataType::UInt16,
            PType::U32 => DataType::UInt32,
            PType::U64 => DataType::UInt64,
            PType::F32 => DataType::Float32,
            PType::F64 => DataType::Float64,
            PType::F16 => return None,
        },
        DType::List(element, _) => DataType::List(Arc::new(Field::new(
            "item",
            data_type(element)?,
            element.is_nullable(),
        ))),
        DType::FixedSizeList(element, size, _) => DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                data_type(element)?,
                element.is_nullable(),
            )),
            i32::try_from(*size).ok()?,
        ),
        DType::Struct(fields, _) => DataType::Struct(
            fields
                .names()
                .iter()
                .zip(fields.fields())
                .map(|(name, dtype)| {
                    let name: &str = name.as_ref();
                    if name.len() > 256 {
                        return None;
                    }
                    Some(Field::new(name, data_type(&dtype)?, dtype.is_nullable()))
                })
                .collect::<Option<Vec<_>>>()?
                .into(),
        ),
        _ => return None,
    })
}

pub(super) fn field_count(dtype: &DType) -> usize {
    1 + match dtype {
        DType::Struct(fields, _) => fields.fields().map(|child| field_count(&child)).sum(),
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => field_count(element),
        _ => 0,
    }
}

/// The pinned Avro reader attaches record names to nested Arrow fields. They
/// identify Avro records, not different logical payload types. Keep field names,
/// nullability and every translated leaf exact, and reject other metadata.
pub(super) fn avro_field_matches(actual: &Field, expected: &Field) -> bool {
    if actual.name() != expected.name()
        || actual.is_nullable() != expected.is_nullable()
        || !expected.metadata().is_empty()
        || actual
            .metadata()
            .keys()
            .any(|name| !matches!(name.as_str(), "avro.name" | "avro.namespace"))
    {
        return false;
    }
    match (actual.data_type(), expected.data_type()) {
        (DataType::List(actual), DataType::List(expected)) => avro_field_matches(actual, expected),
        (DataType::Struct(actual), DataType::Struct(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(a, e)| avro_field_matches(a, e))
        }
        (actual, expected) => actual == expected,
    }
}

fn charge(expanded: &mut u64, bytes: u64, limits: &CompatibilityLimits) -> Result<()> {
    add(expanded, bytes)?;
    if *expanded > limits.arrow_batch_bytes {
        return Err(error(
            "native fields exceed Arrow batch expansion admission",
        ));
    }
    Ok(())
}

/// Preserve native structural validity and canonicalize only at the terminal
/// adapter. A retained view's entire child domain is counted conservatively.
/// Completed relational payloads already own compact selected child domains.
pub(super) fn canonical(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &CompatibilityLimits,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    limits.check()?;
    charge(
        expanded,
        usize_to_u64(array.len().div_ceil(8))? + 128,
        limits,
    )?;
    match array.dtype() {
        DType::Struct(_, _) => {
            let structure = array
                .clone()
                .execute::<StructArray>(ctx)
                .map_err(vortex_error)?;
            let children = structure
                .iter_unmasked_fields()
                .map(|child| canonical(child, ctx, limits, expanded))
                .collect::<Result<Vec<_>>>()?;
            StructArray::try_new(
                structure.names().clone(),
                children,
                array.len(),
                structure.struct_validity(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::List(_, _) => {
            charge(
                expanded,
                usize_to_u64(array.len())?
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(4))
                    .ok_or_else(|| error("list offset overflow"))?,
                limits,
            )?;
            let list = array
                .clone()
                .execute::<ListViewArray>(ctx)
                .map_err(vortex_error)?;
            let elements = canonical(list.elements(), ctx, limits, expanded)?;
            ListViewArray::try_new(
                elements,
                list.offsets().clone(),
                list.sizes().clone(),
                list.listview_validity(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::FixedSizeList(_, _, _) => {
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(ctx)
                .map_err(vortex_error)?;
            let elements = canonical(list.elements(), ctx, limits, expanded)?;
            FixedSizeListArray::try_new(
                elements,
                list.list_size(),
                list.fixed_size_list_validity(),
                array.len(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::Utf8(_) => canonical_text(array, ctx, limits, expanded),
        DType::Bool(_) | DType::Primitive(..) => {
            let width = match array.dtype() {
                DType::Bool(_) => 1,
                DType::Primitive(ptype, _) => ptype.byte_width() as u64,
                _ => unreachable!(),
            };
            charge(
                expanded,
                usize_to_u64(array.len())?
                    .checked_mul(width)
                    .ok_or_else(|| error("Arrow expansion overflow"))?,
                limits,
            )?;
            array
                .clone()
                .execute::<Columnar>(ctx)
                .map(IntoArray::into_array)
                .map_err(vortex_error)
        }
        _ => Err(error("canonical field changed its admitted dtype")),
    }
}

fn canonical_text(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &CompatibilityLimits,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    charge(
        expanded,
        usize_to_u64(array.len())?
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| error("string offset overflow"))?,
        limits,
    )?;
    let text = array
        .clone()
        .execute::<VarBinViewArray>(ctx)
        .map_err(vortex_error)?;
    for (index, view) in text.views().iter().enumerate() {
        if index.is_multiple_of(1024) {
            limits.check()?;
        }
        let len = usize::try_from(view.len()).map_err(vortex_error)?;
        if len > limits.string_bytes {
            return Err(error("native string exceeds Arrow expansion admission"));
        }
        charge(expanded, usize_to_u64(len)?, limits)?;
    }
    Ok(text.into_array())
}
