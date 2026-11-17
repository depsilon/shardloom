//! Suspended native Parquet Variant shells retain their existing owners/credits.

use super::*;
use crate::local_primitives::native_capacity::ReservedVec;
use shardloom_exec::live_memory::MemoryLease;

struct Parts {
    value: Option<ArrayRef>,
    typed: Option<ArrayRef>,
    row: usize,
    depth: usize,
    shell: Option<StructArray>,
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

struct Parent {
    cursor: Cursor,
    _credit: MemoryLease,
    raw: Option<ByteBuffer>,
    _shell: Option<StructArray>,
}

struct Parents(ReservedVec<Parent>);
impl Drop for Parents {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

enum Start {
    Complete(bool),
    Parent(Parent),
}

#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives::native_payload) fn parts(
    metadata: &[u8],
    value: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
    max_depth: usize,
) -> Result<bool> {
    let mut parents = Parents(ReservedVec::new(context.memory())?);
    let mut next = Some(Parts {
        value: value.cloned(),
        typed: typed.cloned(),
        row,
        depth,
        shell: None,
    });
    let mut complete = None;
    loop {
        if let Some(parts) = next.take() {
            match enter(parts, metadata, execution, context, max_depth)? {
                Start::Complete(present) => complete = Some(present),
                Start::Parent(parent) => parents.0.push(parent)?,
            }
        }
        loop {
            let Some(mut parent) = parents.0.values.pop() else {
                return Ok(complete.expect("completed root has a presence result"));
            };
            if let Some(present) = complete.take() {
                finish_child(&parent, present, metadata)?;
            }
            match resume(&mut parent, execution, context)? {
                Some(parts) => {
                    parents.0.values.push(parent);
                    next = Some(parts);
                    break;
                }
                None => complete = Some(true),
            }
        }
    }
}

fn enter(
    parts: Parts,
    metadata: &[u8],
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    max_depth: usize,
) -> Result<Start> {
    let Parts {
        value,
        typed,
        row,
        depth,
        shell,
    } = parts;
    context.check_cancelled()?;
    if depth > max_depth {
        return Err(failed(
            "Variant shredded value exceeds the current depth boundary",
        ));
    }
    let raw = value
        .as_ref()
        .map(|array| binary(array, row, execution))
        .transpose()?
        .flatten();
    let credit = context
        .memory()
        .reserve(workspace(raw.as_ref().map_or(0, ByteBuffer::len))?)?;
    let parsed = raw
        .as_ref()
        .map(|raw| {
            crate::native_variant::value(metadata, raw.as_slice(), &|| context.check_cancelled())
        })
        .transpose()?;
    let typed = match typed {
        Some(typed) if typed.is_valid(row, execution).map_err(vortex_error)? => typed,
        _ => return Ok(Start::Complete(parsed.is_some())),
    };
    if !matches!(typed.dtype(), DType::Struct(..)) && parsed.is_some() {
        return Err(failed(
            "Variant has conflicting raw and shredded non-object values",
        ));
    }
    let cursor = match typed.dtype() {
        DType::Struct(..) => {
            if !matches!(parsed, None | Some(Variant::Object(_))) {
                return Err(failed(
                    "Variant shredded object requires an object residual",
                ));
            }
            let array = typed
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            Cursor::Struct {
                array,
                next: 0,
                row,
                depth: depth + 1,
            }
        }
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(typed, execution)?;
            let Some((start, count)) = list.coordinates(row, execution)? else {
                return Ok(Start::Complete(true));
            };
            Cursor::List {
                list,
                next: start,
                end: start + count,
                depth: depth + 1,
            }
        }
        _ => {
            leaf(&typed, row, execution)?;
            return Ok(Start::Complete(true));
        }
    };
    Ok(Start::Parent(Parent {
        cursor,
        _credit: credit,
        raw,
        _shell: shell,
    }))
}

fn finish_child(parent: &Parent, present: bool, metadata: &[u8]) -> Result<()> {
    let Cursor::Struct { array, next, .. } = &parent.cursor else {
        return Ok(());
    };
    if !present {
        return Ok(());
    }
    let name = &array.names()[next - 1];
    if VariantMetadata::try_new(metadata)
        .map_err(vortex_error)?
        .get_entry(name.as_ref())
        .is_none()
    {
        return Err(failed(
            "Variant shredded field is absent from its metadata dictionary",
        ));
    }
    // These exact immutable bytes passed full validation before suspending this
    // parent. Rebuild only the borrowed header; do not repeat full validation.
    if parent
        .raw
        .as_ref()
        .is_some_and(|raw| match Variant::new(metadata, raw.as_slice()) {
            Variant::Object(object) => object.get(name.as_ref()).is_some(),
            _ => false,
        })
    {
        return Err(failed("Variant raw and shredded object fields overlap"));
    }
    Ok(())
}

fn resume(
    parent: &mut Parent,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Parts>> {
    loop {
        let (field, row, depth) = match &mut parent.cursor {
            Cursor::Struct {
                array,
                next,
                row,
                depth,
            } => {
                if *next == array.names().len() {
                    return Ok(None);
                }
                let index = *next;
                *next += 1;
                check(index, context)?;
                (array.unmasked_field(index), *row, *depth)
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
                let index = *next;
                *next += 1;
                check(index, context)?;
                (&list.elements, index, *depth)
            }
        };
        if let Some(parts) = shell(field, row, depth, execution)? {
            return Ok(Some(parts));
        }
        // A null shell returned false in the recursive implementation, so it
        // skips both metadata lookup and residual overlap checking.
    }
}

fn shell(
    field: &ArrayRef,
    row: usize,
    depth: usize,
    execution: &mut ExecutionCtx,
) -> Result<Option<Parts>> {
    if !matches!(field.dtype(), DType::Struct(..)) {
        return Err(failed(
            "Variant shredded field requires a value/typed_value struct",
        ));
    }
    let field = field
        .clone()
        .execute::<StructArray>(execution)
        .map_err(vortex_error)?;
    if !field
        .struct_validity()
        .execute_is_valid(row, execution)
        .map_err(vortex_error)?
    {
        return Ok(None);
    }
    if field.names().is_empty()
        || field
            .names()
            .iter()
            .any(|name| !matches!(name.as_ref(), "value" | "typed_value"))
    {
        return Err(failed(
            "Variant shredded field requires only value/typed_value children",
        ));
    }
    let find = |name: &str| {
        field
            .names()
            .iter()
            .position(|field| field.as_ref() == name)
            .map(|index| field.unmasked_field(index).clone())
    };
    Ok(Some(Parts {
        value: find("value"),
        typed: find("typed_value"),
        row,
        depth,
        shell: Some(field),
    }))
}
