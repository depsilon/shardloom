//! Ordered Variant storage copying through the same native payload driver.

use super::copying::{Frame as Parent, Inputs, Kind, Start, State, Visit};
use super::{CopyPolicy, CopyState, ReservedVec, Value, check, failed, result_batch, vortex_error};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, IntoArray, VortexSessionExecute as _,
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
pub(super) mod canonical_validation;
#[path = "native_payload_variant_validation.rs"]
pub(super) mod validation;

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

pub(super) enum Frame {
    Wrapper {
        _source: ArrayRef,
        _extra: Option<ArrayRef>,
        selected: usize,
    },
    Chunked {
        output: ReservedVec<ArrayRef>,
        source: ArrayRef,
        selected: usize,
        end: usize,
        depth: usize,
        allocator: HostAllocatorRef,
    },
    Core {
        source: ArrayRef,
    },
    Shredded {
        core: ArrayRef,
        _source: ArrayRef,
        visible: usize,
        _allocator: HostAllocatorRef,
    },
    Parquet {
        _storage: ArrayRef,
        _output_type: DType,
        _source: ArrayRef,
        _allocator: HostAllocatorRef,
        _scratch: MemoryLease,
    },
}

#[allow(clippy::too_many_lines)] // Preserve ordered wrapper dispatch and its validation boundaries.
pub(super) fn enter<'a, const RETAIN: bool>(
    visit: Visit<'a>,
    depth: usize,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    context.check_cancelled()?;
    if depth > 64 {
        return Err(failed(
            "Variant storage wrappers exceed the current depth boundary",
        ));
    }
    let rows = inputs.get(visit.rows);
    if rows.iter().all(Option::is_none) {
        return defaults(
            &visit.dtype,
            if RETAIN { rows.len() } else { 0 },
            &visit.allocator,
        )
        .map(Start::Complete);
    }
    let source = visit
        .source
        .as_ref()
        .ok_or_else(|| failed("Variant source is absent"))?;
    let mut execution = context.native_session().create_execution_ctx();
    let source = source
        .clone()
        .execute_until::<Storage>(&mut execution)
        .map_err(vortex_error)?;
    if let Some(storage) = source.as_opt::<ParquetVariant>() {
        let scratch = context.memory().reserve(4096)?;
        let mut names = vec!["metadata"];
        let mut children = vec![storage.metadata().clone()];
        if let Some(value) = storage.value() {
            names.push("value");
            children.push(value.clone());
        }
        if let Some(value) = storage.typed_value() {
            names.push("typed_value");
            children.push(value.clone());
        }
        let array = StructArray::try_new(
            FieldNames::from(names),
            children,
            storage.len(),
            storage.parquet_variant_validity(),
        )
        .map_err(vortex_error)?
        .into_array();
        let output_type = array.dtype().with_nullability(visit.dtype.nullability());
        let metadata = crate::native_payload_schema::reserve_metadata(&output_type, context)?;
        let allocator = crate::owned_buffers::with_credit(visit.allocator.clone(), metadata);
        validation::validate(&storage, rows, context)?;
        let mut next = visit.child(
            Some(array.clone()),
            output_type.clone(),
            visit.rows,
            CopyState {
                policy: CopyPolicy::PreserveUnobserved,
                ..visit.copy
            },
            Kind::Payload,
        );
        next.allocator = allocator.clone();
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Parquet {
                    _source: source,
                    _storage: array,
                    _output_type: output_type,
                    _allocator: allocator,
                    _scratch: scratch,
                }),
                execution,
                expansion: None,
                visit,
            },
        ));
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
        let child = dictionary.values().clone();
        let selected = inputs.push(selected)?;
        let next = visit.child(
            Some(child),
            visit.dtype.clone(),
            Some(selected),
            visit.copy,
            Kind::Variant(depth + 1),
        );
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Wrapper {
                    _source: source,
                    _extra: None,
                    selected,
                }),
                execution,
                expansion: None,
                visit,
            },
        ));
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
        let child = slice.child().clone();
        let selected = inputs.push(selected)?;
        let next = visit.child(
            Some(child),
            visit.dtype.clone(),
            Some(selected),
            visit.copy,
            Kind::Variant(depth + 1),
        );
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Wrapper {
                    _source: source,
                    _extra: None,
                    selected,
                }),
                execution,
                expansion: None,
                visit,
            },
        ));
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
        let child = masked.child().clone();
        let selected = inputs.push(selected)?;
        let next = visit.child(
            Some(child),
            visit.dtype.clone(),
            Some(selected),
            visit.copy,
            Kind::Variant(depth + 1),
        );
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Wrapper {
                    _source: source,
                    _extra: None,
                    selected,
                }),
                execution,
                expansion: None,
                visit,
            },
        ));
    }
    if source.is::<Chunked>() {
        let credit = context.memory().reserve(
            (rows.len() as u64)
                .checked_mul(1024)
                .ok_or_else(|| failed("Variant chunk workspace overflow"))?,
        )?;
        let allocator = crate::owned_buffers::with_credit(visit.allocator.clone(), credit);
        let mut output = ReservedVec::new(context.memory())?;
        let mut selected = ReservedVec::new(context.memory())?;
        output.reserve(rows.len())?;
        selected.reserve(rows.len())?;
        let selected = inputs.push(selected)?;
        let (child, end) = chunk(&source, visit.rows, selected, 0, inputs, context)?;
        let mut next = visit.child(
            child,
            visit.dtype.clone(),
            Some(selected),
            visit.copy,
            Kind::Variant(depth + 1),
        );
        next.allocator = allocator.clone();
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Chunked {
                    source,
                    selected,
                    output,
                    end,
                    depth,
                    allocator,
                }),
                execution,
                expansion: None,
                visit,
            },
        ));
    }
    if let Some(variant) = source.as_opt::<Variant>() {
        let next = visit.child(
            Some(variant.core_storage().clone()),
            visit.dtype.clone(),
            visit.rows,
            visit.copy,
            Kind::Variant(depth + 1),
        );
        return Ok(Start::Child(
            next,
            Parent {
                state: State::Variant(Frame::Core { source }),
                execution,
                expansion: None,
                visit,
            },
        ));
    }
    if let Some(constant) = source.as_opt::<Constant>() {
        let scalar = constant.scalar();
        if scalar.is_null() {
            if !visit.dtype.is_nullable() {
                return Err(failed("null in a nonnullable Variant"));
            }
            return defaults(
                &visit.dtype,
                if RETAIN { rows.len() } else { 0 },
                &visit.allocator,
            )
            .map(Start::Complete);
        }
        if scalar.as_variant().is_variant_null() == Some(true) {
            let present = DType::Variant(Nullability::NonNullable);
            let value = defaults(&present, 1, &visit.allocator)?;
            let mut selected = ReservedVec::new(context.memory())?;
            selected.reserve(rows.len())?;
            selected
                .values
                .extend(rows.iter().map(|row| row.map(|_| 0)));
            let selected = inputs.push(selected)?;
            let next = visit.child(
                Some(value.clone()),
                visit.dtype.clone(),
                Some(selected),
                visit.copy,
                Kind::Variant(depth + 1),
            );
            return Ok(Start::Child(
                next,
                Parent {
                    state: State::Variant(Frame::Wrapper {
                        _source: source,
                        _extra: Some(value),
                        selected,
                    }),
                    execution,
                    expansion: None,
                    visit,
                },
            ));
        }
    }
    Err(failed(
        "Variant payload requires admitted Parquet Variant storage",
    ))
}

fn chunk(
    source: &ArrayRef,
    parent: super::copying::RowOwner,
    selected: usize,
    start: usize,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<(Option<ArrayRef>, usize)> {
    check(start, context)?;
    let chunks = source.as_opt::<Chunked>().expect("retained chunked source");
    let (rows, selected) = inputs.parent_and_child(parent, selected);
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
    Ok((chunk.map(|index| chunks.chunk(index).clone()), end))
}

#[allow(clippy::too_many_lines)] // Each arm completes exactly one suspended native owner.
pub(super) fn resume<'a, const RETAIN: bool>(
    parent: Parent<'a>,
    complete: ArrayRef,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    let Parent {
        state,
        mut execution,
        expansion,
        visit,
    } = parent;
    let State::Variant(state) = state else {
        unreachable!()
    };
    match state {
        Frame::Wrapper {
            _source,
            _extra,
            selected,
        } => {
            inputs.release(selected);
            Ok(Start::Complete(complete))
        }
        Frame::Chunked {
            source,
            selected,
            mut output,
            end,
            depth,
            allocator,
        } => {
            if RETAIN {
                output.values.push(complete);
            } else {
                drop(complete);
            }
            if end < inputs.get(visit.rows).len() {
                let (child, end) = chunk(&source, visit.rows, selected, end, inputs, context)?;
                let mut next = visit.child(
                    child,
                    visit.dtype.clone(),
                    Some(selected),
                    visit.copy,
                    Kind::Variant(depth + 1),
                );
                next.allocator = allocator.clone();
                return Ok(Start::Child(
                    next,
                    Parent {
                        state: State::Variant(Frame::Chunked {
                            source,
                            selected,
                            output,
                            end,
                            depth,
                            allocator,
                        }),
                        execution,
                        expansion,
                        visit,
                    },
                ));
            }
            let array = if RETAIN {
                let (output, _scratch) = output.into_parts();
                ChunkedArray::try_new(output, visit.dtype.clone())
                    .map(IntoArray::into_array)
                    .map_err(vortex_error)
            } else {
                defaults(&visit.dtype, 0, &allocator)
            };
            inputs.release(selected);
            array.map(Start::Complete)
        }
        Frame::Core { source } => {
            let variant = source.as_opt::<Variant>().expect("retained Variant source");
            let rows = inputs.get(visit.rows);
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
            let Some(child) = variant.shredded() else {
                return VariantArray::try_new(complete, None)
                    .map(IntoArray::into_array)
                    .map_err(vortex_error)
                    .map(Start::Complete);
            };
            let metadata = crate::native_payload_schema::reserve_metadata(child.dtype(), context)?;
            let allocator = crate::owned_buffers::with_credit(visit.allocator.clone(), metadata);
            canonical_validation::validate(variant.core_storage(), child, rows, context)?;
            let child_dtype = child.dtype().clone();
            let child = child.clone();
            let visible = inputs.push(visible)?;
            let mut next = visit.child(
                Some(child),
                child_dtype,
                Some(visible),
                CopyState {
                    policy: CopyPolicy::PreserveUnobserved,
                    ..visit.copy
                },
                Kind::Payload,
            );
            next.allocator = allocator.clone();
            Ok(Start::Child(
                next,
                Parent {
                    state: State::Variant(Frame::Shredded {
                        _source: source,
                        core: complete,
                        visible,
                        _allocator: allocator,
                    }),
                    execution,
                    expansion,
                    visit,
                },
            ))
        }
        Frame::Shredded {
            _source,
            core,
            visible,
            _allocator,
        } => {
            let output = VariantArray::try_new(core, Some(complete))
                .map(IntoArray::into_array)
                .map_err(vortex_error);
            inputs.release(visible);
            output.map(Start::Complete)
        }
        Frame::Parquet {
            _source,
            _storage,
            _output_type,
            _allocator,
            _scratch,
        } => {
            let mut inner_execution = context.native_session().create_execution_ctx();
            let copied = complete
                .execute::<StructArray>(&mut inner_execution)
                .map_err(vortex_error)?;
            let find = |name: &str| {
                copied
                    .names()
                    .iter()
                    .position(|field| field.as_ref() == name)
                    .map(|index| copied.unmasked_field(index).clone())
            };
            ParquetVariant::try_new(
                copied.struct_validity(),
                find("metadata").expect("copied Variant metadata"),
                find("value"),
                find("typed_value"),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
            .map(Start::Complete)
        }
    }
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
