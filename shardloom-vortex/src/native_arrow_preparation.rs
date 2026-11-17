//! Ordered native preparation shared by result export and compatibility writers.

use super::{ConversionLimits, canonical_fixed, canonical_text, charge, error};
use crate::local_primitives::{native_capacity::ReservedVec, usize_to_u64, vortex_error};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, ExecutionCtx, IntoArray,
    arrays::extension::ExtensionArrayExt as _,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::struct_::StructArrayExt as _,
    arrays::{ExtensionArray, FixedSizeListArray, ListViewArray, StructArray},
    dtype::DType,
};

#[path = "native_arrow_variant.rs"]
mod variant;

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Canonical,
    Variant(usize),
    LogicalShredded,
}

struct Walk<'a, 'b> {
    ctx: &'a mut ExecutionCtx,
    limits: &'a ConversionLimits<'b>,
    expanded: &'a mut u64,
    max_wrappers: usize,
}

enum Parent {
    Struct {
        children: ReservedVec<ArrayRef>,
        source: StructArray,
        rows: usize,
        mode: Mode,
    },
    List(ListViewArray),
    Fixed(FixedSizeListArray, usize),
    Extension(ExtensionArray),
    Variant(variant::Frame),
}

enum Step {
    Done(ArrayRef),
    Child(Mode, ArrayRef, Parent),
}

struct Pending(ReservedVec<Parent>);

#[cfg(test)]
#[path = "native_arrow_preparation_tests.rs"]
mod tests;

impl Drop for Pending {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

pub(super) fn prepare(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
    mode: Mode,
    max_wrappers: usize,
) -> Result<ArrayRef> {
    let mut pending = Pending(ReservedVec::new(limits.memory)?);
    let mut walk = Walk {
        ctx,
        limits,
        expanded,
        max_wrappers,
    };
    let mut step = walk.visit(array.clone(), mode)?;
    loop {
        step = match step {
            Step::Child(mode, child, parent) => {
                pending.0.push(parent)?;
                walk.visit(child, mode)?
            }
            Step::Done(child) => {
                let Some(parent) = pending.0.values.pop() else {
                    return Ok(child);
                };
                walk.resume(parent, child)?
            }
        };
    }
}

impl Walk<'_, '_> {
    fn visit(&mut self, array: ArrayRef, mode: Mode) -> Result<Step> {
        match mode {
            Mode::Canonical => self.canonical(array),
            Mode::Variant(depth) => self.variant(array, depth),
            Mode::LogicalShredded => self.logical_shredded(array),
        }
    }

    fn canonical(&mut self, array: ArrayRef) -> Result<Step> {
        self.limits.check()?;
        let rows = array.len();
        charge(
            self.expanded,
            usize_to_u64(rows.div_ceil(8))? + 128,
            self.limits,
        )?;
        match array.dtype() {
            DType::Variant(_) => self.variant(array, 0),
            DType::Extension(_)
                if crate::native_extensions::payload_storage(array.dtype()).is_some() =>
            {
                let source = array
                    .execute::<ExtensionArray>(self.ctx)
                    .map_err(vortex_error)?;
                Ok(Step::Child(
                    Mode::Canonical,
                    source.storage_array().clone(),
                    Parent::Extension(source),
                ))
            }
            DType::Struct(..) => {
                let source = array
                    .execute::<StructArray>(self.ctx)
                    .map_err(vortex_error)?;
                Self::structure(
                    source,
                    rows,
                    ReservedVec::new(self.limits.memory)?,
                    Mode::Canonical,
                )
            }
            DType::List(..) => {
                let bytes = usize_to_u64(array.len())?
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(4))
                    .ok_or_else(|| error("list offset overflow"))?;
                charge(self.expanded, bytes, self.limits)?;
                let source = array
                    .execute::<ListViewArray>(self.ctx)
                    .map_err(vortex_error)?;
                Ok(Step::Child(
                    Mode::Canonical,
                    source.elements().clone(),
                    Parent::List(source),
                ))
            }
            DType::FixedSizeList(..) => {
                let source = array
                    .execute::<FixedSizeListArray>(self.ctx)
                    .map_err(vortex_error)?;
                Ok(Step::Child(
                    Mode::Canonical,
                    source.elements().clone(),
                    Parent::Fixed(source, rows),
                ))
            }
            DType::Utf8(_) | DType::Binary(_) => {
                canonical_text(&array, self.ctx, self.limits, self.expanded).map(Step::Done)
            }
            DType::Bool(_) | DType::Primitive(..) | DType::Decimal(..) | DType::Extension(_) => {
                canonical_fixed(&array, self.ctx, self.limits, self.expanded).map(Step::Done)
            }
            _ => Err(error("canonical field changed its admitted dtype")),
        }
    }

    fn structure(
        source: StructArray,
        rows: usize,
        mut children: ReservedVec<ArrayRef>,
        mode: Mode,
    ) -> Result<Step> {
        if let Some(child) = source.unmasked_field_opt(children.values.len()) {
            return Ok(Step::Child(
                mode,
                child.clone(),
                Parent::Struct {
                    children,
                    source,
                    rows,
                    mode,
                },
            ));
        }
        // The constructor creates its own slots. Draining transfers the individual
        // handles while the input vector keeps its allocation and capacity credit.
        StructArray::try_new(
            source.names().clone(),
            children.values.drain(..),
            rows,
            source.struct_validity(),
        )
        .map(IntoArray::into_array)
        .map(Step::Done)
        .map_err(vortex_error)
    }

    fn resume(&mut self, parent: Parent, child: ArrayRef) -> Result<Step> {
        let output = match parent {
            Parent::Struct {
                mut children,
                source,
                rows,
                mode,
            } => {
                children.push(child)?;
                return Self::structure(source, rows, children, mode);
            }
            Parent::List(source) => ListViewArray::try_new(
                child,
                source.offsets().clone(),
                source.sizes().clone(),
                source.listview_validity(),
                self.ctx,
            )
            .map(IntoArray::into_array),
            Parent::Fixed(source, rows) => FixedSizeListArray::try_new(
                child,
                source.list_size(),
                source.fixed_size_list_validity(),
                rows,
            )
            .map(IntoArray::into_array),
            Parent::Extension(source) => ExtensionArray::try_new(source.ext_dtype().clone(), child)
                .map(IntoArray::into_array),
            Parent::Variant(frame) => return self.resume_variant(frame, child),
        };
        output.map(Step::Done).map_err(vortex_error)
    }
}
