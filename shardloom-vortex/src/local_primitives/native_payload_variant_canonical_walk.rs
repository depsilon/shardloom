//! Ordered, credited continuations for the existing logical Variant validator.

use super::*;
use crate::local_primitives::native_capacity::ReservedVec;

enum Parent<'m, 'v, 'a> {
    Struct {
        array: StructArray,
        next: usize,
        row: usize,
        depth: usize,
        residual: Option<&'a Value<'m, 'v>>,
    },
    List {
        list: crate::local_primitives::native_list::Column,
        next: usize,
        end: usize,
        depth: usize,
    },
}

// Match the recursive walk's inner-before-outer release order on errors/unwind.
struct Parents<'m, 'v, 'a>(ReservedVec<Parent<'m, 'v, 'a>>);

impl Drop for Parents<'_, '_, '_> {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

struct Visit<'m, 'v, 'a> {
    typed: ArrayRef,
    row: usize,
    depth: usize,
    residual: Option<&'a Value<'m, 'v>>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn logical(
    metadata: &[u8],
    residual: Option<&Value<'_, '_>>,
    typed: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
    max_depth: usize,
) -> Result<()> {
    let mut parents = Parents(ReservedVec::new(context.memory())?);
    let mut next = Some(Visit {
        typed: typed.clone(),
        row,
        depth,
        residual,
    });
    loop {
        if let Some(visit) = next.take()
            && let Some(parent) = enter(visit, execution, context, max_depth)?
        {
            parents.0.push(parent)?;
        }
        loop {
            let Some(mut parent) = parents.0.values.pop() else {
                return Ok(());
            };
            if let Some(child) = resume(&mut parent, metadata, execution, context)? {
                parents.0.values.push(parent);
                next = Some(child);
                break;
            }
        }
    }
}

fn enter<'m, 'v, 'a>(
    visit: Visit<'m, 'v, 'a>,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    max_depth: usize,
) -> Result<Option<Parent<'m, 'v, 'a>>> {
    let Visit {
        typed,
        row,
        depth,
        residual,
    } = visit;
    context.check_cancelled()?;
    if depth > max_depth {
        return Err(failed(
            "canonical shredded Variant exceeds the current depth boundary",
        ));
    }
    if !typed.is_valid(row, execution).map_err(vortex_error)? {
        return Ok(None);
    }
    match typed.dtype() {
        DType::Struct(..) => {
            if !matches!(residual, None | Some(Value::Object(_))) {
                return Err(failed("shredded object requires an object residual"));
            }
            let array = typed
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            Ok(Some(Parent::Struct {
                array,
                next: 0,
                row,
                depth: depth + 1,
                residual,
            }))
        }
        _ if residual.is_some() => Err(failed("conflicting raw and shredded non-object values")),
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(typed, execution)?;
            Ok(list
                .coordinates(row, execution)?
                .map(|(start, count)| Parent::List {
                    list,
                    next: start,
                    end: start + count,
                    depth: depth + 1,
                }))
        }
        DType::Variant(_) => Err(failed(
            "nested canonical Variant shredding is not admitted; supply Parquet Variant storage",
        )),
        _ => validation::leaf(&typed, row, execution).map(|()| None),
    }
}

fn resume<'m, 'v, 'a>(
    parent: &mut Parent<'m, 'v, 'a>,
    metadata: &[u8],
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Visit<'m, 'v, 'a>>> {
    match parent {
        Parent::Struct {
            array,
            next,
            row,
            depth,
            residual,
        } => {
            while *next < array.names().len() {
                let index = *next;
                *next += 1;
                let name = &array.names()[index];
                check(index, context)?;
                let child = array.unmasked_field(index);
                if !child.is_valid(*row, execution).map_err(vortex_error)? {
                    continue;
                }
                if VariantMetadata::try_new(metadata)
                    .map_err(vortex_error)?
                    .get_entry(name.as_ref())
                    .is_none()
                {
                    return Err(failed(
                        "shredded field is absent from its metadata dictionary",
                    ));
                }
                if residual.is_some_and(|value| match value {
                    Value::Object(object) => object.get(name.as_ref()).is_some(),
                    _ => false,
                }) {
                    return Err(failed("raw and shredded object fields overlap"));
                }
                return Ok(Some(Visit {
                    typed: child.clone(),
                    row: *row,
                    depth: *depth,
                    residual: None,
                }));
            }
            Ok(None)
        }
        Parent::List {
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
            Ok(Some(Visit {
                typed: list.elements.clone(),
                row,
                depth: *depth,
                residual: None,
            }))
        }
    }
}
