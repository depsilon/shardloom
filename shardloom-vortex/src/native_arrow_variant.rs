//! Native Variant branches of the ordered Arrow preparation walk.

use super::super::{ConversionLimits, charge, error};
use super::{Mode, Parent, ReservedVec, Step, Walk};
use crate::local_primitives::{usize_to_u64, vortex_error};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::chunked::ChunkedArrayExt as _,
        arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
        arrays::masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
        arrays::variant::VariantArraySlotsExt as _,
        arrays::{
            Chunked, ChunkedArray, Constant, ListArray, ListView, ListViewArray, Masked,
            MaskedArray, PrimitiveArray, Struct, Variant, VariantArray,
        },
        dtype::{DType, StructFields},
        matcher::Matcher,
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    buffer::{Alignment, Buffer},
    encodings::parquet_variant::{
        ParquetVariant, ParquetVariantArray, ParquetVariantArrayExt as _,
        ParquetVariantArraySlotsExt as _,
    },
};

struct Storage;
impl Matcher for Storage {
    type Match<'a> = &'a ArrayRef;
    fn try_match(array: &ArrayRef) -> Option<Self::Match<'_>> {
        (array.is::<ParquetVariant>()
            || array.is::<Variant>()
            || array.is::<Chunked>()
            || array.is::<Masked>()
            || array.is::<Constant>())
        .then_some(array)
    }
}

pub(super) enum Frame {
    Chunks {
        children: ReservedVec<ArrayRef>,
        source: ChunkedArray,
        child_depth: usize,
    },
    Core(VariantArray),
    CanonicalShredded {
        core: ArrayRef,
        source: VariantArray,
        before: u64,
    },
    LogicalShredded {
        core: ArrayRef,
        source: VariantArray,
        before: u64,
    },
    Masked(MaskedArray),
    Metadata(ParquetVariantArray),
    Raw {
        metadata: ArrayRef,
        source: ParquetVariantArray,
    },
    Typed {
        metadata: ArrayRef,
        value: Option<ArrayRef>,
        source: ParquetVariantArray,
        before: u64,
    },
    List {
        offsets: ArrayRef,
        source: ListViewArray,
    },
}

impl Frame {
    fn descend(self, mode: Mode, child: ArrayRef) -> Step {
        Step::Child(mode, child, Parent::Variant(self))
    }
}

impl Walk<'_, '_> {
    pub(super) fn variant(&mut self, array: ArrayRef, depth: usize) -> Result<Step> {
        self.limits.check()?;
        if depth > self.max_wrappers {
            return Err(error("Variant wrapper depth exceeds Arrow admission"));
        }
        let array = array
            .execute_until::<Storage>(self.ctx)
            .map_err(vortex_error)?;
        if let Some(storage) = array.as_opt::<ParquetVariant>() {
            let metadata = storage.metadata().clone();
            return Ok(Frame::Metadata(storage.into_owned()).descend(Mode::Canonical, metadata));
        }
        if let Some(chunks) = array.as_opt::<Chunked>() {
            charge(
                self.expanded,
                (chunks.nchunks() as u64)
                    .checked_mul(1024)
                    .ok_or_else(|| error("Variant chunk metadata overflow"))?,
                self.limits,
            )?;
            return Self::chunks(
                chunks.into_owned(),
                ReservedVec::new(self.limits.memory)?,
                depth + 1,
            );
        }
        if let Some(variant) = array.as_opt::<Variant>() {
            let core = variant.core_storage().clone();
            return Ok(Frame::Core(variant.into_owned()).descend(Mode::Variant(depth + 1), core));
        }
        if let Some(masked) = array.as_opt::<Masked>() {
            let child = masked.child().clone();
            return Ok(Frame::Masked(masked.into_owned()).descend(Mode::Variant(depth + 1), child));
        }
        if let Some(constant) = array.as_opt::<Constant>() {
            let scalar = constant.scalar();
            if scalar.is_null() || scalar.as_variant().is_variant_null() == Some(true) {
                charge(
                    self.expanded,
                    usize_to_u64(array.len())?
                        .checked_mul(64)
                        .ok_or_else(|| error("Variant null expansion overflow"))?,
                    self.limits,
                )?;
                return Ok(Step::Done(array));
            }
        }
        Err(error(
            "Variant export requires admitted Parquet Variant storage",
        ))
    }

    fn chunks(
        source: ChunkedArray,
        mut children: ReservedVec<ArrayRef>,
        child_depth: usize,
    ) -> Result<Step> {
        let index = children.values.len();
        if index < source.nchunks() {
            let child = source.chunk(index).clone();
            return Ok(Frame::Chunks {
                children,
                source,
                child_depth,
            }
            .descend(Mode::Variant(child_depth), child));
        }
        ChunkedArray::try_new(children.values.drain(..), source.dtype().clone())
            .map(IntoArray::into_array)
            .map(Step::Done)
            .map_err(vortex_error)
    }

    pub(super) fn resume_variant(&mut self, frame: Frame, child: ArrayRef) -> Result<Step> {
        match frame {
            Frame::Chunks {
                mut children,
                source,
                child_depth,
            } => {
                children.push(child)?;
                Self::chunks(source, children, child_depth)
            }
            Frame::Core(source) => {
                let Some(shredded) = source.shredded().cloned() else {
                    return VariantArray::try_new(child, None)
                        .map(IntoArray::into_array)
                        .map(Step::Done)
                        .map_err(vortex_error);
                };
                Ok(Frame::CanonicalShredded {
                    core: child,
                    source,
                    before: *self.expanded,
                }
                .descend(Mode::Canonical, shredded))
            }
            Frame::CanonicalShredded {
                core,
                source,
                before,
            } => Ok(Frame::LogicalShredded {
                core,
                source,
                before,
            }
            .descend(Mode::LogicalShredded, child)),
            Frame::LogicalShredded {
                core,
                source,
                before,
            } => {
                let rows = usize_to_u64(source.len())?;
                let names = names(child.dtype(), self.limits.memory)?;
                self.framing(rows, names, before)?;
                VariantArray::try_new(core, Some(child))
                    .map(IntoArray::into_array)
                    .map(Step::Done)
                    .map_err(vortex_error)
            }
            Frame::Masked(source) => {
                MaskedArray::try_new(child, source.masked_validity(), self.ctx)
                    .map(IntoArray::into_array)
                    .map(Step::Done)
                    .map_err(vortex_error)
            }
            Frame::Metadata(source) => {
                if let Some(value) = source.value().cloned() {
                    return Ok(Frame::Raw {
                        metadata: child,
                        source,
                    }
                    .descend(Mode::Canonical, value));
                }
                self.prepare_typed(source, child, None)
            }
            Frame::Raw { metadata, source } => self.prepare_typed(source, metadata, Some(child)),
            Frame::Typed {
                metadata,
                value,
                source,
                before,
            } => self.finish_parquet(&source, metadata, value, Some(child), before),
            Frame::List { offsets, source } => {
                ListArray::try_new(child, offsets, source.listview_validity(), self.ctx)
                    .map(IntoArray::into_array)
                    .map(Step::Done)
                    .map_err(vortex_error)
            }
        }
    }

    fn prepare_typed(
        &mut self,
        source: ParquetVariantArray,
        metadata: ArrayRef,
        value: Option<ArrayRef>,
    ) -> Result<Step> {
        let before = *self.expanded;
        if let Some(typed) = source.typed_value().cloned() {
            return Ok(Frame::Typed {
                metadata,
                value,
                source,
                before,
            }
            .descend(Mode::Canonical, typed));
        }
        self.finish_parquet(&source, metadata, value, None, before)
    }

    fn finish_parquet(
        &mut self,
        source: &ParquetVariantArray,
        metadata: ArrayRef,
        value: Option<ArrayRef>,
        typed: Option<ArrayRef>,
        before: u64,
    ) -> Result<Step> {
        let names = typed
            .as_ref()
            .map_or(Ok(0), |typed| names(typed.dtype(), self.limits.memory))?;
        self.framing(usize_to_u64(source.len())?, names, before)?;
        validate_rows(source, self.ctx, self.limits)?;
        ParquetVariant::try_new(source.parquet_variant_validity(), metadata, value, typed)
            .map(IntoArray::into_array)
            .map(Step::Done)
            .map_err(vortex_error)
    }

    fn framing(&mut self, rows: u64, names: u64, before: u64) -> Result<()> {
        let bytes = rows
            .checked_mul(
                names
                    .checked_add(64)
                    .ok_or_else(|| error("Variant expansion overflow"))?,
            )
            .and_then(|bytes| bytes.checked_add((*self.expanded - before).checked_mul(8)?))
            .ok_or_else(|| error("Variant expansion overflow"))?;
        charge(self.expanded, bytes, self.limits)
    }

    pub(super) fn logical_shredded(&mut self, array: ArrayRef) -> Result<Step> {
        self.limits.check()?;
        if let Some(list) = array.as_opt::<ListView>() {
            let list = list.into_owned();
            let offsets = logical_offsets(&list, self.ctx, self.limits, self.expanded)?;
            let elements = list.elements().clone();
            return Ok(Frame::List {
                offsets,
                source: list,
            }
            .descend(Mode::LogicalShredded, elements));
        }
        if let Some(structure) = array.as_opt::<Struct>() {
            return Self::structure(
                structure.into_owned(),
                array.len(),
                ReservedVec::new(self.limits.memory)?,
                Mode::LogicalShredded,
            );
        }
        Ok(Step::Done(array))
    }
}

fn validate_rows(
    storage: &ParquetVariantArray,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
) -> Result<()> {
    for row in 0..storage.len() {
        limits.check()?;
        if !storage
            .parquet_variant_validity()
            .execute_is_valid(row, ctx)
            .map_err(vortex_error)?
        {
            continue;
        }
        let metadata = storage
            .metadata()
            .execute_scalar(row, ctx)
            .map_err(vortex_error)?;
        let metadata = metadata
            .as_binary()
            .value()
            .ok_or_else(|| error("Variant metadata is null"))?;
        let value = storage
            .value()
            .map(|value| value.execute_scalar(row, ctx).map_err(vortex_error))
            .transpose()?;
        if let Some(raw) = value.as_ref().and_then(|value| value.as_binary().value()) {
            let bytes = metadata
                .len()
                .checked_add(raw.len())
                .ok_or_else(|| error("Variant validation overflow"))?;
            let workspace =
                usize::try_from(crate::native_variant::workspace(bytes)?).map_err(vortex_error)?;
            let _credit = ctx
                .session()
                .host_allocator()
                .reserve_external(workspace)
                .map_err(vortex_error)?;
            crate::native_variant::value(metadata.as_slice(), raw.as_slice(), limits.check)?;
        }
    }
    Ok(())
}

/// Reframe compact `ListView` coordinates to the layout expected by the
/// Parquet Variant provider. Element buffers stay with the native child.
fn logical_offsets(
    list: &ListViewArray,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    let rows = list
        .len()
        .checked_add(1)
        .ok_or_else(|| error("Variant list size overflow"))?;
    let bytes = rows
        .checked_mul(size_of::<u64>())
        .ok_or_else(|| error("Variant list size overflow"))?;
    charge(expanded, usize_to_u64(bytes)?, limits)?;
    let mut buffer = ctx
        .session()
        .host_allocator()
        .allocate(bytes, Alignment::of::<u64>())
        .map_err(vortex_error)?;
    let offsets = buffer.as_mut_slice_typed::<u64>().map_err(vortex_error)?;
    let mut end = None;
    for (row, slot) in offsets[..list.len()].iter_mut().enumerate() {
        limits.check()?;
        let start = list
            .offsets()
            .execute_scalar(row, ctx)
            .map_err(vortex_error)?
            .as_primitive()
            .as_::<u64>()
            .ok_or_else(|| error("invalid Variant list offset"))?;
        let size = list
            .sizes()
            .execute_scalar(row, ctx)
            .map_err(vortex_error)?
            .as_primitive()
            .as_::<u64>()
            .ok_or_else(|| error("invalid Variant list size"))?;
        if end.is_some_and(|end| end != start) {
            return Err(error(
                "canonical Variant export requires compact selected lists",
            ));
        }
        *slot = start;
        end = Some(
            start
                .checked_add(size)
                .ok_or_else(|| error("Variant list offset overflow"))?,
        );
    }
    offsets[list.len()] = end.unwrap_or(0);
    let offsets = PrimitiveArray::new(
        Buffer::<u64>::from_byte_buffer(buffer.freeze()),
        Validity::NonNullable,
    )
    .into_array();
    Ok(offsets)
}

struct NameFrame<'a> {
    fields: &'a StructFields,
    index: usize,
    total: u64,
}

pub(super) fn names(dtype: &DType, memory: &LiveMemoryPool) -> Result<u64> {
    let mut parents = ReservedVec::new(memory)?;
    let mut next = dtype;
    loop {
        match next {
            DType::Struct(fields, _) if fields.nfields() != 0 => {
                next = fields.field_dtypes().next().expect("first struct field");
                parents.push(NameFrame {
                    fields,
                    index: 0,
                    total: 0,
                })?;
                continue;
            }
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                next = child;
                continue;
            }
            _ => {}
        }
        let mut size = 0;
        loop {
            let Some(parent) = parents.values.last_mut() else {
                return Ok(size);
            };
            let name = &parent.fields.names()[parent.index];
            parent.total = parent
                .total
                .checked_add(name.as_ref().len() as u64)
                .and_then(|bytes| bytes.checked_add(16))
                .and_then(|bytes| bytes.checked_add(size))
                .ok_or_else(|| error("Variant metadata expansion overflow"))?;
            parent.index += 1;
            if let Some(child) = parent.fields.field_dtypes().nth(parent.index) {
                next = child;
                break;
            }
            size = parent.total;
            parents.values.pop();
        }
    }
}
