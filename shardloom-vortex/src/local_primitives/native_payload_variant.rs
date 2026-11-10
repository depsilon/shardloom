//! Detach selected Variant storage through the ordinary credited payload copier.
//! Values remain encoded native children; no Arrow or JSON conversion executes.

use super::{
    CopyPolicy, CopyState, ReservedVec, Value, check, copy_rows, failed, result_batch, vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::{
    array::{
        ArrayRef, ArrayView, IntoArray, VortexSessionExecute as _,
        arrays::{
            Chunked, ChunkedArray, Constant, Dict, Masked, Slice, StructArray, Variant,
            VariantArray,
            chunked::ChunkedArrayExt as _,
            dict::DictArraySlotsExt as _,
            masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
            slice::SliceArraySlotsExt as _,
            struct_::StructArrayExt as _,
            variant::VariantArraySlotsExt as _,
        },
        dtype::{DType, FieldNames, Nullability},
        matcher::Matcher,
        memory::HostAllocatorRef,
        validity::Validity,
    },
    encodings::parquet_variant::{
        ParquetVariant, ParquetVariantArrayExt as _, ParquetVariantArraySlotsExt as _,
    },
};

#[path = "native_payload_variant_canonical_validation.rs"]
mod canonical_validation;
#[path = "native_payload_variant_validation.rs"]
mod validation;

struct Storage;
impl Matcher for Storage {
    type Match<'a> = &'a ArrayRef;
    fn try_match(array: &ArrayRef) -> Option<Self::Match<'_>> {
        (array.is::<ParquetVariant>()
            || array.is::<Variant>()
            || array.is::<Chunked>()
            || array.is::<Dict>()
            || array.is::<Masked>()
            || array.is::<Slice>()
            || array.is::<Constant>())
        .then_some(array)
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn copy<const RETAIN: bool>(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    policy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
    depth: usize,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    if depth > 64 {
        return Err(failed(
            "Variant storage wrappers exceed the current depth boundary",
        ));
    }
    if rows.iter().all(Option::is_none) {
        return defaults(dtype, if RETAIN { rows.len() } else { 0 }, allocator);
    }
    let source = source.ok_or_else(|| failed("Variant source is absent"))?;
    let mut execution = context.native_session().create_execution_ctx();
    let source = source
        .clone()
        .execute_until::<Storage>(&mut execution)
        .map_err(vortex_error)?;
    if let Some(storage) = source.as_opt::<ParquetVariant>() {
        return copy_parquet::<RETAIN>(storage, rows, dtype, policy, context, allocator);
    }
    if let Some(dictionary) = source.as_opt::<Dict>() {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(rows.len())?;
        for (ordinal, row) in rows.iter().enumerate() {
            check(ordinal, context)?;
            selected.values.push(if let Some(row) = row {
                let code = dictionary
                    .codes()
                    .execute_scalar(*row, &mut execution)
                    .map_err(vortex_error)?;
                if code.is_null() {
                    None
                } else {
                    Some(
                        code.as_primitive()
                            .as_::<usize>()
                            .filter(|index| *index < dictionary.values().len())
                            .ok_or_else(|| failed("Variant dictionary index exceeds its values"))?,
                    )
                }
            } else {
                None
            });
        }
        return copy::<RETAIN>(
            Some(dictionary.values()),
            &selected.values,
            dtype,
            policy,
            context,
            allocator,
            depth + 1,
        );
    }
    if let Some(slice) = source.as_opt::<Slice>() {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(rows.len())?;
        for row in rows {
            selected.values.push(
                row.map(|row| {
                    row.checked_add(slice.slice_range().start)
                        .filter(|row| *row < slice.child().len())
                        .ok_or_else(|| failed("Variant slice index exceeds its child"))
                })
                .transpose()?,
            );
        }
        return copy::<RETAIN>(
            Some(slice.child()),
            &selected.values,
            dtype,
            policy,
            context,
            allocator,
            depth + 1,
        );
    }
    if let Some(masked) = source.as_opt::<Masked>() {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(rows.len())?;
        let validity = masked.masked_validity();
        for (ordinal, row) in rows.iter().enumerate() {
            check(ordinal, context)?;
            selected.values.push(match row {
                Some(row)
                    if validity
                        .execute_is_valid(*row, &mut execution)
                        .map_err(vortex_error)? =>
                {
                    Some(*row)
                }
                _ => None,
            });
        }
        return copy::<RETAIN>(
            Some(masked.child()),
            &selected.values,
            dtype,
            policy,
            context,
            allocator,
            depth + 1,
        );
    }
    if let Some(chunks) = source.as_opt::<Chunked>() {
        let credit = context.memory().reserve(
            (rows.len() as u64)
                .checked_mul(1024)
                .ok_or_else(|| failed("Variant chunk workspace overflow"))?,
        )?;
        let allocator = crate::owned_buffers::with_credit(allocator.clone(), credit);
        let mut output = ReservedVec::new(context.memory())?;
        let mut selected = ReservedVec::new(context.memory())?;
        output.reserve(rows.len())?;
        selected.reserve(rows.len())?;
        let mut start = 0;
        while start < rows.len() {
            check(start, context)?;
            let chunk = rows[start]
                .map(|row| {
                    chunks
                        .find_chunk_idx(row)
                        .map(|(chunk, _)| chunk)
                        .map_err(vortex_error)
                })
                .transpose()?;
            let mut end = start;
            selected.values.clear();
            while end < rows.len() {
                let position = rows[end]
                    .map(|row| chunks.find_chunk_idx(row).map_err(vortex_error))
                    .transpose()?;
                if position.map(|(chunk, _)| chunk) != chunk {
                    break;
                }
                selected.values.push(position.map(|(_, row)| row));
                end += 1;
            }
            let value = copy::<RETAIN>(
                chunk.map(|index| chunks.chunk(index)),
                &selected.values,
                dtype,
                policy,
                context,
                &allocator,
                depth + 1,
            )?;
            if RETAIN {
                output.values.push(value);
            }
            start = end;
        }
        if !RETAIN {
            return defaults(dtype, 0, &allocator);
        }
        let (output, _scratch) = output.into_parts();
        return ChunkedArray::try_new(output, dtype.clone())
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(variant) = source.as_opt::<Variant>() {
        let core = copy::<RETAIN>(
            Some(variant.core_storage()),
            rows,
            dtype,
            policy,
            context,
            allocator,
            depth + 1,
        )?;
        let mut visible = ReservedVec::new(context.memory())?;
        visible.reserve(rows.len())?;
        for (ordinal, row) in rows.iter().enumerate() {
            check(ordinal, context)?;
            visible.values.push(match row {
                Some(row)
                    if variant
                        .core_storage()
                        .is_valid(*row, &mut execution)
                        .map_err(vortex_error)? =>
                {
                    Some(*row)
                }
                _ => None,
            });
        }
        let shredded = variant
            .shredded()
            .map(|child| {
                let metadata =
                    crate::native_payload_schema::reserve_metadata(child.dtype(), context)?;
                let allocator = crate::owned_buffers::with_credit(allocator.clone(), metadata);
                canonical_validation::validate(variant.core_storage(), child, rows, context)?;
                copy_rows::<RETAIN>(
                    Some(child),
                    &visible.values,
                    child.dtype(),
                    CopyState {
                        policy: CopyPolicy::PreserveUnobserved,
                        ..policy
                    },
                    context,
                    &allocator,
                )
            })
            .transpose()?;
        return VariantArray::try_new(core, shredded)
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(constant) = source.as_opt::<Constant>() {
        let scalar = constant.scalar();
        if scalar.is_null() {
            if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable Variant"));
            }
            return defaults(dtype, if RETAIN { rows.len() } else { 0 }, allocator);
        }
        if scalar.as_variant().is_variant_null() == Some(true) {
            let present = DType::Variant(Nullability::NonNullable);
            let value = defaults(&present, 1, allocator)?;
            let mut selected = ReservedVec::new(context.memory())?;
            selected.reserve(rows.len())?;
            selected
                .values
                .extend(rows.iter().map(|row| row.map(|_| 0)));
            return copy::<RETAIN>(
                Some(&value),
                &selected.values,
                dtype,
                policy,
                context,
                allocator,
                depth + 1,
            );
        }
    }
    Err(failed(
        "Variant payload requires admitted Parquet Variant storage",
    ))
}

fn defaults(dtype: &DType, rows: usize, allocator: &HostAllocatorRef) -> Result<ArrayRef> {
    let binary = DType::Binary(Nullability::NonNullable);
    let metadata = result_batch::build_column(&binary, rows, allocator, |_| {
        Ok(Value::Binary((&[1_u8, 0, 0][..]).into()))
    })?;
    let value = result_batch::build_column(&binary, rows, allocator, |_| {
        Ok(Value::Binary((&[0_u8][..]).into()))
    })?;
    ParquetVariant::try_new(
        if dtype.is_nullable() {
            Validity::AllInvalid
        } else {
            Validity::NonNullable
        },
        metadata,
        Some(value),
        None,
    )
    .map(IntoArray::into_array)
    .map_err(vortex_error)
}

fn copy_parquet<const RETAIN: bool>(
    source: ArrayView<'_, ParquetVariant>,
    rows: &[Option<usize>],
    dtype: &DType,
    policy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let scratch = context.memory().reserve(4096)?;
    let mut names = vec!["metadata"];
    let mut children = vec![source.metadata().clone()];
    if let Some(value) = source.value() {
        names.push("value");
        children.push(value.clone());
    }
    if let Some(value) = source.typed_value() {
        names.push("typed_value");
        children.push(value.clone());
    }
    let storage = StructArray::try_new(
        FieldNames::from(names),
        children,
        source.len(),
        source.parquet_variant_validity(),
    )
    .map_err(vortex_error)?
    .into_array();
    let output_type = storage.dtype().with_nullability(dtype.nullability());
    let metadata = crate::native_payload_schema::reserve_metadata(&output_type, context)?;
    let allocator = crate::owned_buffers::with_credit(allocator.clone(), metadata);
    // PreserveUnobserved is a numeric observation policy, not permission to
    // publish malformed Variant bytes or conflicting shredded fields.
    validation::validate(&source, rows, context)?;
    let copied = copy_rows::<RETAIN>(
        Some(&storage),
        rows,
        &output_type,
        CopyState {
            policy: CopyPolicy::PreserveUnobserved,
            ..policy
        },
        context,
        &allocator,
    )?;
    let mut execution = context.native_session().create_execution_ctx();
    let copied = copied
        .execute::<StructArray>(&mut execution)
        .map_err(vortex_error)?;
    let find = |name: &str| {
        copied
            .names()
            .iter()
            .position(|field| field.as_ref() == name)
            .map(|index| copied.unmasked_field(index).clone())
    };
    let output = ParquetVariant::try_new(
        copied.struct_validity(),
        find("metadata").expect("copied Variant metadata"),
        find("value"),
        find("typed_value"),
    )
    .map(IntoArray::into_array)
    .map_err(vortex_error);
    drop(scratch);
    output
}
