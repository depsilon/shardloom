//! Ordered payload copying with flat, admitted parent and coordinate owners.

use super::{
    CopyPolicy, CopyState, ReservedVec, Value, check, failed, native_list, result_batch, storage,
    tensor, validity, variant, vector, vortex_error,
};
use crate::local_primitives::native_capacity::ReservedStack;
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute as _,
    arrays::extension::ExtensionArrayExt,
    arrays::struct_::StructArrayExt as _,
    arrays::{ExtensionArray, FixedSizeListArray, ListArray, StructArray},
    dtype::{DType, Nullability, PType},
    memory::HostAllocatorRef,
};

pub(super) type RowOwner = Option<usize>;

pub(super) struct Inputs<'a> {
    root: &'a [Option<usize>],
    temporary: ReservedVec<ReservedVec<Option<usize>>>,
}

impl Inputs<'_> {
    pub(super) fn get(&self, owner: RowOwner) -> &[Option<usize>] {
        owner.map_or(self.root, |index| &self.temporary.values[index].values)
    }

    pub(super) fn parent_and_child(
        &mut self,
        parent: RowOwner,
        child: usize,
    ) -> (&[Option<usize>], &mut ReservedVec<Option<usize>>) {
        let (parents, children) = self.temporary.values.split_at_mut(child);
        let rows = parent.map_or(self.root, |index| &parents[index].values);
        (rows, &mut children[0])
    }

    pub(super) fn push(&mut self, rows: ReservedVec<Option<usize>>) -> Result<usize> {
        let index = self.temporary.values.len();
        self.temporary.push(rows)?;
        Ok(index)
    }

    pub(super) fn release(&mut self, owner: usize) {
        assert_eq!(owner + 1, self.temporary.values.len());
        drop(self.temporary.values.pop());
    }
}

impl Drop for Inputs<'_> {
    fn drop(&mut self) {
        while let Some(rows) = self.temporary.values.pop() {
            drop(rows);
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Payload,
    Variant(usize),
}

pub(super) struct Visit<'a> {
    pub(super) source: Option<ArrayRef>,
    pub(super) dtype: DType,
    pub(super) rows: RowOwner,
    pub(super) copy: CopyState<'a>,
    pub(super) kind: Kind,
    pub(super) allocator: HostAllocatorRef,
}

impl<'a> Visit<'a> {
    pub(super) fn child(
        &self,
        source: Option<ArrayRef>,
        dtype: DType,
        rows: RowOwner,
        copy: CopyState<'a>,
        kind: Kind,
    ) -> Self {
        Self {
            source,
            dtype,
            rows,
            copy,
            kind,
            allocator: self.allocator.clone(),
        }
    }
}

pub(super) enum State {
    Extension(Option<ExtensionArray>),
    Struct {
        children: ReservedVec<ArrayRef>,
        valid: ReservedVec<bool>,
        selected: Option<StructArray>,
        inner_execution: ExecutionCtx,
        active: usize,
    },
    List {
        valid: ReservedVec<bool>,
        offsets: ReservedVec<usize>,
        selected: Option<native_list::Column>,
        inner_execution: ExecutionCtx,
        children: usize,
    },
    Variant(variant::Frame),
}

pub(super) struct Frame<'a> {
    pub(super) state: State,
    pub(super) execution: ExecutionCtx,
    pub(super) expansion: Option<MemoryLease>,
    pub(super) visit: Visit<'a>,
}

#[allow(clippy::large_enum_variant)] // One admitted parent frame; no per-node box allocation.
pub(super) enum Start<'a> {
    Complete(ArrayRef),
    Child(Visit<'a>, Frame<'a>),
}

type Pending<'a> = ReservedStack<Frame<'a>, 4>;

pub(super) fn copy_rows<const RETAIN: bool>(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    copy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    let mut inputs = Inputs {
        root: rows,
        temporary: ReservedVec::new(context.memory())?,
    };
    let mut pending = Pending::new(context.memory())?;
    let mut visit = Visit {
        source: source.cloned(),
        dtype: dtype.clone(),
        rows: None,
        copy,
        kind: Kind::Payload,
        allocator: allocator.clone(),
    };
    loop {
        let mut step = match visit.kind {
            Kind::Payload => enter::<RETAIN>(visit, &mut inputs, context)?,
            Kind::Variant(depth) => variant::enter::<RETAIN>(visit, depth, &mut inputs, context)?,
        };
        loop {
            match step {
                Start::Child(next, frame) => {
                    pending.push(frame)?;
                    visit = next;
                    break;
                }
                Start::Complete(complete) => {
                    let Some(frame) = pending.pop() else {
                        return Ok(complete);
                    };
                    step = resume::<RETAIN>(frame, complete, &mut inputs, context)?;
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)] // Shared selection and extension validation keep their original order.
fn enter<'a, const RETAIN: bool>(
    visit: Visit<'a>,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    context.check_cancelled()?;
    if matches!(visit.dtype, DType::Variant(_)) {
        return variant::enter::<RETAIN>(visit, 0, inputs, context);
    }
    let rows = inputs.get(visit.rows);
    if let DType::Extension(extension) = &visit.dtype
        && crate::native_extensions::payload_storage(&visit.dtype).is_some()
    {
        let mut execution = context.native_session().create_execution_ctx();
        let storage = visit
            .source
            .as_ref()
            .map(|source| {
                source
                    .clone()
                    .execute::<ExtensionArray>(&mut execution)
                    .map_err(vortex_error)
            })
            .transpose()?;
        if visit.copy.policy == CopyPolicy::ValidateValues
            && let Some(metadata) =
                extension.metadata_opt::<crate::native_variable_tensor::VariableShapeTensor>()
        {
            tensor::validate_selection(
                storage.as_ref().map(ExtensionArrayExt::storage_array),
                rows,
                metadata,
                context,
                &visit.allocator,
            )?;
        }
        if visit.copy.policy == CopyPolicy::ValidateValues
            && let Some(metadata) = crate::native_vector::metadata(&visit.dtype)
        {
            vector::validate_selection(
                storage.as_ref().map(ExtensionArrayExt::storage_array),
                rows,
                metadata,
                context,
                &visit.allocator,
            )?;
        }
        let copy = if let Some(metadata) = crate::native_spatial::metadata(&visit.dtype) {
            if visit.copy.policy == CopyPolicy::ValidateValues {
                super::super::native_spatial_input::validate_selection(
                    storage.as_ref().map(ExtensionArrayExt::storage_array),
                    rows,
                    metadata.kind(),
                    context,
                    &visit.allocator,
                )?;
            }
            CopyState {
                policy: CopyPolicy::PreserveUnobserved,
                ..visit.copy
            }
        } else {
            visit.copy
        };
        let next = visit.child(
            storage.as_ref().map(|array| array.storage_array().clone()),
            extension.storage_dtype().clone(),
            visit.rows,
            copy,
            Kind::Payload,
        );
        return Ok(Start::Child(
            next,
            Frame {
                state: State::Extension(storage),
                execution,
                expansion: None,
                visit,
            },
        ));
    }
    if matches!(visit.dtype, DType::Decimal(..)) {
        let array = storage::decimal::<RETAIN>(
            visit.source.as_ref(),
            rows,
            &visit.dtype,
            visit.copy.policy,
            context,
            &visit.allocator,
        )?;
        visit.copy.record_scalars(rows.len())?;
        return Ok(Start::Complete(array));
    }
    let expansion = if let DType::FixedSizeList(_, size, _) = &visit.dtype {
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
        let source = visit
            .source
            .as_ref()
            .ok_or_else(|| failed("payload source is absent"))?;
        if !RETAIN
            && rows.len() == source.len()
            && rows
                .iter()
                .enumerate()
                .all(|(row, index)| *index == Some(row))
        {
            Some(source.clone())
        } else {
            let nullable = rows.iter().any(Option::is_none);
            let indices = result_batch::build_column(
                &DType::Primitive(PType::U64, Nullability::from(nullable)),
                rows.len(),
                &visit.allocator,
                |row| Ok(rows[row].map_or(Value::Null, |index| Value::UInt(index as u64))),
            )?;
            Some(source.take(indices).map_err(vortex_error)?)
        }
    } else {
        None
    };
    match &visit.dtype {
        DType::Struct(..) => {
            enter_struct::<RETAIN>(visit, selected, expansion, execution, inputs, context)
        }
        DType::List(..) | DType::FixedSizeList(..) => {
            enter_list::<RETAIN>(visit, selected, expansion, execution, inputs, context)
        }
        _ => super::copy_leaf::<RETAIN>(
            selected,
            rows,
            &visit.dtype,
            visit.copy,
            context,
            &visit.allocator,
            &mut execution,
        )
        .map(Start::Complete),
    }
}

fn enter_struct<'a, const RETAIN: bool>(
    visit: Visit<'a>,
    selected: Option<ArrayRef>,
    expansion: Option<MemoryLease>,
    execution: ExecutionCtx,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    let rows = inputs.get(visit.rows);
    let DType::Struct(fields, _) = &visit.dtype else {
        unreachable!()
    };
    let mut inner_execution = context.native_session().create_execution_ctx();
    let selected = selected
        .map(|array| {
            array
                .execute::<StructArray>(&mut inner_execution)
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
                .execute_is_valid(row, &mut inner_execution)
                .map_err(vortex_error)?
        } else {
            false
        };
        active.values.push(present.then_some(row));
        valid
            .values
            .push(present || (original.is_none() && !visit.dtype.is_nullable()));
    }
    let mut children = ReservedVec::new(context.memory())?;
    children.reserve(fields.nfields())?;
    if fields.nfields() == 0 {
        return finish_struct::<RETAIN>(&visit, children, &valid.values, rows.len())
            .map(Start::Complete);
    }
    let active = inputs.push(active)?;
    let next = field(&visit, selected.as_ref(), 0, active);
    Ok(Start::Child(
        next,
        Frame {
            state: State::Struct {
                selected,
                active,
                valid,
                children,
                inner_execution,
            },
            execution,
            expansion,
            visit,
        },
    ))
}

fn field<'a>(
    visit: &Visit<'a>,
    selected: Option<&StructArray>,
    index: usize,
    active: usize,
) -> Visit<'a> {
    let DType::Struct(fields, _) = &visit.dtype else {
        unreachable!()
    };
    let dtype = fields.field_by_index(index).expect("admitted field index");
    visit.child(
        selected.map(|array| array.unmasked_field(index).clone()),
        dtype,
        Some(active),
        visit.copy.field(index),
        Kind::Payload,
    )
}

fn finish_struct<const RETAIN: bool>(
    visit: &Visit<'_>,
    children: ReservedVec<ArrayRef>,
    valid: &[bool],
    rows: usize,
) -> Result<ArrayRef> {
    let DType::Struct(fields, _) = &visit.dtype else {
        unreachable!()
    };
    let validity = validity::<RETAIN>(&visit.dtype, valid, &visit.allocator)?;
    let (children, _ownership) = children.into_parts();
    StructArray::try_new(
        fields.names().clone(),
        children,
        if RETAIN { rows } else { 0 },
        validity,
    )
    .map(IntoArray::into_array)
    .and_then(|array| crate::owned_buffers::retain_empty_array_credit(array, &visit.allocator))
    .map_err(vortex_error)
}

fn enter_list<'a, const RETAIN: bool>(
    visit: Visit<'a>,
    selected: Option<ArrayRef>,
    expansion: Option<MemoryLease>,
    execution: ExecutionCtx,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    let rows = inputs.get(visit.rows);
    let mut inner_execution = context.native_session().create_execution_ctx();
    let selected = selected
        .map(|array| native_list::Column::new(array, &mut inner_execution))
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
                .coordinates(row, &mut inner_execution)?
        } else {
            None
        };
        valid
            .values
            .push(coordinates.is_some() || (original.is_none() && !visit.dtype.is_nullable()));
        let count = match &visit.dtype {
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
    let element = match &visit.dtype {
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => element.as_ref().clone(),
        _ => unreachable!(),
    };
    let children = inputs.push(children)?;
    let next = visit.child(
        selected.as_ref().map(|column| column.elements.clone()),
        element,
        Some(children),
        visit.copy,
        Kind::Payload,
    );
    Ok(Start::Child(
        next,
        Frame {
            state: State::List {
                selected,
                children,
                offsets,
                valid,
                inner_execution,
            },
            execution,
            expansion,
            visit,
        },
    ))
}

fn finish_list<const RETAIN: bool>(
    visit: &Visit<'_>,
    child: ArrayRef,
    offsets: &ReservedVec<usize>,
    valid: &[bool],
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let validity = validity::<RETAIN>(&visit.dtype, valid, &visit.allocator)?;
    if let DType::FixedSizeList(_, size, _) = &visit.dtype {
        FixedSizeListArray::try_new(child, *size, validity, if RETAIN { rows } else { 0 })
            .map(IntoArray::into_array)
            .map_err(vortex_error)
    } else {
        let offsets = result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::NonNullable),
            offsets.values.len(),
            &visit.allocator,
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

fn resume<'a, const RETAIN: bool>(
    frame: Frame<'a>,
    complete: ArrayRef,
    inputs: &mut Inputs<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Start<'a>> {
    let Frame {
        state,
        execution,
        expansion,
        visit,
    } = frame;
    match state {
        State::Extension(storage) => {
            let DType::Extension(extension) = &visit.dtype else {
                unreachable!()
            };
            let output = ExtensionArray::try_new(extension.clone(), complete)
                .map(IntoArray::into_array)
                .map_err(vortex_error);
            drop(storage);
            output.map(Start::Complete)
        }
        State::Struct {
            selected,
            active,
            valid,
            mut children,
            inner_execution,
        } => {
            children.values.push(complete);
            let DType::Struct(fields, _) = &visit.dtype else {
                unreachable!()
            };
            if children.values.len() < fields.nfields() {
                let next = field(&visit, selected.as_ref(), children.values.len(), active);
                return Ok(Start::Child(
                    next,
                    Frame {
                        state: State::Struct {
                            selected,
                            active,
                            valid,
                            children,
                            inner_execution,
                        },
                        execution,
                        expansion,
                        visit,
                    },
                ));
            }
            let output = finish_struct::<RETAIN>(
                &visit,
                children,
                &valid.values,
                inputs.get(visit.rows).len(),
            );
            drop(valid);
            inputs.release(active);
            drop(selected);
            drop(inner_execution);
            output.map(Start::Complete)
        }
        State::List {
            selected,
            children,
            offsets,
            valid,
            inner_execution,
        } => {
            let output = finish_list::<RETAIN>(
                &visit,
                complete,
                &offsets,
                &valid.values,
                inputs.get(visit.rows).len(),
                context,
            );
            drop(valid);
            drop(offsets);
            inputs.release(children);
            drop(selected);
            drop(inner_execution);
            output.map(Start::Complete)
        }
        State::Variant(state) => variant::resume::<RETAIN>(
            Frame {
                state: State::Variant(state),
                execution,
                expansion,
                visit,
            },
            complete,
            inputs,
            context,
        ),
    }
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_copy_tests.rs"]
mod tests;
