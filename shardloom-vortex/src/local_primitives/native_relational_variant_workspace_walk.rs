//! Existing Variant workspace arithmetic over admitted ordered continuations.

use super::*;
use crate::local_primitives::native_capacity::ReservedVec;
use parquet_variant::{Variant, VariantList, VariantObject};

struct Pending<T>(ReservedVec<T>);
impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

struct Physical<'a> {
    children: std::slice::Iter<'a, Option<ArrayRef>>,
    depth: usize,
    _inspection: MemoryLease,
}

pub(super) fn physical(
    array: &ArrayRef,
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    depth: usize,
    max_depth: usize,
) -> Result<()> {
    let mut parents = Pending(ReservedVec::new(context.memory())?);
    let mut next = Some((array, depth));
    loop {
        if let Some((array, depth)) = next.take() {
            let inspection = node(array, credit, context, depth, max_depth)?;
            let mut children = array.slots().iter();
            if let Some(child) = children.find_map(Option::as_ref) {
                parents.0.push(Physical {
                    children,
                    depth: depth + 1,
                    _inspection: inspection,
                })?;
                next = Some((child, depth + 1));
                continue;
            }
        }
        loop {
            let Some(mut parent) = parents.0.values.pop() else {
                return Ok(());
            };
            if let Some(child) = parent.children.find_map(Option::as_ref) {
                next = Some((child, parent.depth));
                parents.0.values.push(parent);
                break;
            }
        }
    }
}

fn node(
    array: &ArrayRef,
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    depth: usize,
    max_depth: usize,
) -> Result<MemoryLease> {
    context.check_cancelled()?;
    // This is the already validated/copied physical tree, which can include
    // Variant shells around the existing 24-level logical payload boundary.
    if depth > max_depth {
        return Err(failed(
            "Variant provider physical storage exceeds its admitted depth",
        ));
    }
    let metadata = crate::native_payload_schema::metadata_bytes(array.dtype(), context)?;
    let rows = u64::try_from(array.len()).map_err(|_| overflow())?;
    add(
        credit,
        metadata
            .checked_mul(rows.saturating_add(1))
            .and_then(|n| n.checked_add(rows.checked_mul(128)?))
            .ok_or_else(overflow)?,
    )?;

    // Admit the temporary handle vector before asking the provider to create it.
    let handles = (array.nbuffers() as u64)
        .checked_mul(64)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(overflow)?;
    let inspection = context.memory().reserve(handles)?;
    for buffer in array.buffer_handles() {
        if !buffer.is_on_host() {
            return Err(failed("Variant expression requires admitted host storage"));
        }
        add(
            credit,
            (buffer.len() as u64)
                .checked_mul(128)
                .ok_or_else(overflow)?,
        )?;
    }
    if let Some(storage) = array.as_opt::<ParquetVariant>() {
        let mut execution = context.native_session().create_execution_ctx();
        for row in 0..array.len() {
            context.check_cancelled()?;
            if array
                .is_invalid(row, &mut execution)
                .map_err(vortex_error)?
            {
                continue;
            }
            let metadata = binary(storage.metadata(), row, &mut execution)?
                .ok_or_else(|| failed("present Variant requires metadata"))?;
            raw(
                storage.value(),
                row,
                metadata.as_slice(),
                credit,
                context,
                &mut execution,
            )?;
            if let Some(typed) = storage.typed_value() {
                super::typed_value(
                    typed,
                    row,
                    metadata.as_slice(),
                    credit,
                    context,
                    &mut execution,
                    0,
                )?;
            }
        }
    }
    Ok(inspection)
}

enum Encoded<'m, 'v> {
    Object(VariantObject<'m, 'v>, usize),
    List(VariantList<'m, 'v>, usize),
}

impl<'m, 'v> Encoded<'m, 'v> {
    fn child(&mut self, credit: &mut MemoryLease) -> Result<Option<Variant<'m, 'v>>> {
        match self {
            Self::Object(object, next) => {
                let Some(name) = object.field_name(*next) else {
                    return Ok(None);
                };
                let value = object.field(*next).expect("validated object field");
                *next += 1;
                add(
                    credit,
                    (name.len() as u64).checked_mul(128).ok_or_else(overflow)?,
                )?;
                Ok(Some(value))
            }
            Self::List(list, next) => {
                let value = list.get(*next);
                *next += usize::from(value.is_some());
                Ok(value)
            }
        }
    }
}

pub(super) fn expanded(
    value: &Variant<'_, '_>,
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut parents = Pending(ReservedVec::new(context.memory())?);
    let mut next = Some(value.clone());
    loop {
        if let Some(value) = next.take() {
            context.check_cancelled()?;
            add(credit, 8192)?;
            let parent = match value {
                Variant::Binary(bytes) => {
                    add(
                        credit,
                        (bytes.len() as u64).checked_mul(128).ok_or_else(overflow)?,
                    )?;
                    None
                }
                Variant::String(text) => {
                    add(
                        credit,
                        (text.len() as u64).checked_mul(128).ok_or_else(overflow)?,
                    )?;
                    None
                }
                Variant::ShortString(text) => {
                    add(
                        credit,
                        (text.as_str().len() as u64)
                            .checked_mul(128)
                            .ok_or_else(overflow)?,
                    )?;
                    None
                }
                Variant::List(list) => Some(Encoded::List(list, 0)),
                Variant::Object(object) => Some(Encoded::Object(object, 0)),
                _ => None,
            };
            if let Some(parent) = parent {
                parents.0.push(parent)?;
            }
        }
        loop {
            let Some(mut parent) = parents.0.values.pop() else {
                return Ok(());
            };
            if let Some(value) = parent.child(credit)? {
                parents.0.values.push(parent);
                next = Some(value);
                break;
            }
        }
    }
}

enum Cursor {
    Struct {
        array: StructArray,
        next: usize,
        row: usize,
        depth: usize,
    },
    List {
        list: crate::local_primitives::native_list::Column,
        next: usize,
        end: usize,
        depth: usize,
    },
}

struct TypedParent {
    cursor: Cursor,
    _shell: Option<StructArray>,
}

struct Typed {
    array: ArrayRef,
    row: usize,
    depth: usize,
    shell: Option<StructArray>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn typed_value(
    array: &ArrayRef,
    row: usize,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
    depth: usize,
    max_depth: usize,
) -> Result<()> {
    let mut parents = Pending(ReservedVec::new(context.memory())?);
    let mut next = Some(Typed {
        array: array.clone(),
        row,
        depth,
        shell: None,
    });
    loop {
        if let Some(typed) = next.take()
            && let Some(parent) = enter(typed, execution, context, max_depth)?
        {
            parents.0.push(parent)?;
        }
        loop {
            let Some(mut parent) = parents.0.values.pop() else {
                return Ok(());
            };
            if let Some(typed) = resume(&mut parent, metadata, credit, context, execution)? {
                parents.0.values.push(parent);
                next = Some(typed);
                break;
            }
        }
    }
}

fn enter(
    typed: Typed,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    max_depth: usize,
) -> Result<Option<TypedParent>> {
    let Typed {
        array,
        row,
        depth,
        shell,
    } = typed;
    context.check_cancelled()?;
    if depth > max_depth {
        return Err(failed(
            "Variant typed workspace exceeds the current depth boundary",
        ));
    }
    if array.is_invalid(row, execution).map_err(vortex_error)? {
        return Ok(None);
    }
    let cursor = match array.dtype() {
        DType::Struct(..) => Cursor::Struct {
            array: array
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?,
            next: 0,
            row,
            depth: depth + 1,
        },
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(array, execution)?;
            let Some((start, count)) = list.coordinates(row, execution)? else {
                return Ok(None);
            };
            Cursor::List {
                list,
                next: start,
                end: start + count,
                depth: depth + 1,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(TypedParent {
        cursor,
        _shell: shell,
    }))
}

fn resume(
    parent: &mut TypedParent,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
) -> Result<Option<Typed>> {
    loop {
        let (array, row, depth) = match &mut parent.cursor {
            Cursor::Struct {
                array,
                next,
                row,
                depth,
            } => {
                if *next == array.names().len() {
                    return Ok(None);
                }
                let field = array.unmasked_field(*next);
                *next += 1;
                (field, *row, *depth)
            }
            Cursor::List {
                list,
                next,
                end,
                depth,
            } => {
                if *next == *end {
                    return Ok(None);
                }
                let row = *next;
                *next += 1;
                (&list.elements, row, *depth)
            }
        };
        if let Some(typed) = shell(array, row, depth, metadata, credit, context, execution)? {
            return Ok(Some(typed));
        }
    }
}

fn shell(
    array: &ArrayRef,
    row: usize,
    depth: usize,
    metadata: &[u8],
    credit: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
    execution: &mut ExecutionCtx,
) -> Result<Option<Typed>> {
    context.check_cancelled()?;
    if array.is_invalid(row, execution).map_err(vortex_error)? {
        return Ok(None);
    }
    let structure = array
        .clone()
        .execute::<StructArray>(execution)
        .map_err(vortex_error)?;
    let field = |name: &str| {
        structure
            .names()
            .iter()
            .position(|field| field.as_ref() == name)
            .map(|index| structure.unmasked_field(index))
    };
    raw(field("value"), row, metadata, credit, context, execution)?;
    let Some(array) = field("typed_value").cloned() else {
        return Ok(None);
    };
    Ok(Some(Typed {
        array,
        row,
        depth,
        shell: Some(structure),
    }))
}
