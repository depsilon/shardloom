//! Frozen original copier bodies from 755d1cc32; test-only oracle.
use super::super::super::{
    CopyPolicy, CopyState, NativeNumericOwner, ReservedVec, Value, check, default_value, failed,
    native_list, result_batch, storage, tensor, validation, validity, vector, vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, Columnar, IntoArray, VortexSessionExecute as _,
    arrays::extension::ExtensionArrayExt,
    arrays::struct_::StructArrayExt as _,
    arrays::{ExtensionArray, FixedSizeListArray, ListArray, PrimitiveArray, StructArray},
    dtype::{DType, Nullability, PType},
    memory::HostAllocatorRef,
};
#[path = "native_payload_variant_reference.rs"]
pub(super) mod variant;
#[allow(clippy::too_many_lines)] // Copying and validation share one field-major visitation order.
pub(super) fn copy_rows<const RETAIN: bool>(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    copy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    if matches!(dtype, DType::Variant(_)) {
        return variant::copy::<RETAIN>(source, rows, dtype, copy, context, allocator, 0);
    }
    if let DType::Extension(extension) = dtype
        && crate::native_extensions::payload_storage(dtype).is_some()
    {
        let mut execution = context.native_session().create_execution_ctx();
        let storage = source
            .map(|source| {
                source
                    .clone()
                    .execute::<ExtensionArray>(&mut execution)
                    .map_err(vortex_error)
            })
            .transpose()?;
        if copy.policy == CopyPolicy::ValidateValues
            && let Some(metadata) =
                extension.metadata_opt::<crate::native_variable_tensor::VariableShapeTensor>()
        {
            tensor::validate_selection(
                storage.as_ref().map(ExtensionArrayExt::storage_array),
                rows,
                metadata,
                context,
                allocator,
            )?;
        }
        if copy.policy == CopyPolicy::ValidateValues
            && let Some(metadata) = crate::native_vector::metadata(dtype)
        {
            vector::validate_selection(
                storage.as_ref().map(ExtensionArrayExt::storage_array),
                rows,
                metadata,
                context,
                allocator,
            )?;
        }
        let copied = copy_rows::<RETAIN>(
            storage.as_ref().map(ExtensionArrayExt::storage_array),
            rows,
            extension.storage_dtype(),
            if let Some(metadata) = crate::native_spatial::metadata(dtype) {
                if copy.policy == CopyPolicy::ValidateValues {
                    super::native_spatial_input::validate_selection(
                        storage.as_ref().map(ExtensionArrayExt::storage_array),
                        rows,
                        metadata.kind(),
                        context,
                        allocator,
                    )?;
                }
                // Geometry validation admits only finite coordinates or the
                // empty Point sentinel. Preserve those bits in native storage.
                CopyState {
                    policy: CopyPolicy::PreserveUnobserved,
                    ..copy
                }
            } else {
                copy
            },
            context,
            allocator,
        )?;
        return ExtensionArray::try_new(extension.clone(), copied)
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if matches!(dtype, DType::Decimal(..)) {
        let array =
            storage::decimal::<RETAIN>(source, rows, dtype, copy.policy, context, allocator)?;
        copy.record_scalars(rows.len())?;
        return Ok(array);
    }
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
        if !RETAIN
            && rows.len() == source.len()
            && rows
                .iter()
                .enumerate()
                .all(|(row, index)| *index == Some(row))
        {
            // Complete validation already has the entire source domain. Keep
            // selection for any masked/nonidentity child so hidden values do
            // not reach a provider's canonicalization or validation boundary.
            Some(source.clone())
        } else {
            let nullable = rows.iter().any(Option::is_none);
            let indices = result_batch::build_column(
                &DType::Primitive(PType::U64, Nullability::from(nullable)),
                rows.len(),
                allocator,
                |row| Ok(rows[row].map_or(Value::Null, |index| Value::UInt(index as u64))),
            )?;
            Some(source.take(indices).map_err(vortex_error)?)
        }
    } else {
        None
    };

    match dtype {
        DType::Struct(fields, _) => {
            copy_struct::<RETAIN>(selected, rows, dtype, fields, copy, context, allocator)
        }
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => {
            copy_list::<RETAIN>(selected, rows, dtype, element, copy, context, allocator)
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
            if RETAIN
                && (copy.policy == CopyPolicy::PreserveUnobserved
                    || context.float_policy() == shardloom_core::FloatPolicy::Extended)
                && matches!(
                    dtype,
                    DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
                )
                && let Some(selected) = &selected
            {
                return storage::copy(selected, rows, dtype, context, allocator);
            }
            // This boundary already copies every selected value into credited
            // output buffers. Resolve native numeric storage and validity once
            // instead of repeatedly traversing encoded dictionaries/selections
            // for each scalar. Unselected payloads remain outside the copy.
            let numeric = selected
                .as_ref()
                .filter(|_| {
                    matches!(dtype, DType::Primitive(..))
                        || crate::native_payload_schema::temporal_storage(dtype).is_some()
                })
                .map(|array| {
                    let primitive = result_batch::scalar_storage(array, &mut execution)?
                        .execute::<PrimitiveArray>(&mut execution)
                        .map_err(vortex_error)?;
                    NativeNumericOwner::new(primitive, &mut execution)
                })
                .transpose()?;
            if !RETAIN
                && copy.policy == CopyPolicy::ValidateValues
                && rows.iter().all(Option::is_some)
                && let Some(selected) = &selected
                && validation::validate_typed_column(
                    selected,
                    dtype,
                    rows.len(),
                    numeric.as_ref(),
                    &mut execution,
                    context,
                )?
            {
                // Selection, parent masks and field order belong to this
                // recursive owner. Reuse typed validation only after those
                // obligations have produced a complete observed leaf domain.
                // Missing coordinates still use the existing placeholder path.
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.payload_recursive_typed_validation_columns += 1;
                });
                return result_batch::build_column(dtype, 0, allocator, |_| default_value(dtype));
            }
            let variable = selected
                .as_ref()
                .map(|array| result_batch::VariableValues::try_new(array, &mut execution))
                .transpose()?
                .flatten();
            let value = |row: usize| {
                check(row, context)?;
                #[cfg(all(test, debug_assertions))]
                if !RETAIN {
                    super::native_relational_observation::record(|counts| {
                        counts.payload_validation_scalar_calls += 1;
                    });
                }
                let value = if rows[row].is_none() {
                    default_value(dtype)
                } else if let Some(numeric) = &numeric {
                    numeric.stat_value(row).map(Value::from)
                } else if let Some(variable) = &variable {
                    variable.value(row)
                } else {
                    result_batch::scalar_value(
                        selected.as_ref().expect("selected leaf"),
                        row,
                        &mut execution,
                    )
                }?;
                copy.record_scalars(1)?;
                Ok(value)
            };
            if RETAIN {
                result_batch::build_column(dtype, rows.len(), allocator, value)
            } else {
                result_batch::build_column_prefix_with_float_policy(
                    dtype,
                    rows.len(),
                    0,
                    allocator,
                    context.float_policy(),
                    value,
                )
            }
        }
    }
}

fn copy_struct<const RETAIN: bool>(
    selected: Option<ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    fields: &vortex::array::dtype::StructFields,
    copy: CopyState<'_>,
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
        children.values.push(copy_rows::<RETAIN>(
            selected.as_ref().map(|array| array.unmasked_field(index)),
            &active.values,
            &child,
            copy.field(index),
            context,
            allocator,
        )?);
    }
    let validity = validity::<RETAIN>(dtype, &valid.values, allocator)?;
    let (children, _ownership) = children.into_parts();
    StructArray::try_new(
        fields.names().clone(),
        children,
        if RETAIN { rows.len() } else { 0 },
        validity,
    )
    .map(IntoArray::into_array)
    .and_then(|array| crate::owned_buffers::retain_empty_array_credit(array, allocator))
    .map_err(vortex_error)
}

fn copy_list<const RETAIN: bool>(
    selected: Option<ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    element: &DType,
    copy: CopyState<'_>,
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
    offsets.reserve(if RETAIN {
        rows.len()
            .checked_add(1)
            .ok_or_else(|| failed("list offset overflow"))?
    } else {
        1
    })?;
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
        if RETAIN {
            offsets.values.push(children.values.len());
        }
    }
    let child = copy_rows::<RETAIN>(
        selected.as_ref().map(|column| &column.elements),
        &children.values,
        element,
        copy,
        context,
        allocator,
    )?;
    let validity = validity::<RETAIN>(dtype, &valid.values, allocator)?;
    if let DType::FixedSizeList(_, size, _) = dtype {
        FixedSizeListArray::try_new(child, *size, validity, if RETAIN { rows.len() } else { 0 })
            .map(IntoArray::into_array)
            .map_err(vortex_error)
    } else {
        let offsets = result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::NonNullable),
            offsets.values.len(),
            allocator,
            |row| Ok(Value::UInt(offsets.values[row] as u64)),
        )?;
        ListArray::try_new(
            child,
            offsets,
            validity,
            &mut context.native_session().create_execution_ctx(),
        )
        .map(IntoArray::into_array)
        .map_err(vortex_error)
    }
}
