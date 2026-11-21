use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn logical(
    metadata: &[u8],
    residual: Option<&Value<'_, '_>>,
    typed: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<()> {
    context.check_cancelled()?;
    if depth > 24 {
        return Err(failed(
            "canonical shredded Variant exceeds the current depth boundary",
        ));
    }
    if !typed.is_valid(row, execution).map_err(vortex_error)? {
        return Ok(());
    }
    match typed.dtype() {
        DType::Struct(..) => {
            let residual = match residual {
                None => None,
                Some(Value::Object(object)) => Some(object),
                Some(_) => return Err(failed("shredded object requires an object residual")),
            };
            let structure = typed
                .clone()
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            for (index, name) in structure.names().iter().enumerate() {
                check(index, context)?;
                let child = structure.unmasked_field(index);
                if !child.is_valid(row, execution).map_err(vortex_error)? {
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
                if residual.is_some_and(|object| object.get(name.as_ref()).is_some()) {
                    return Err(failed("raw and shredded object fields overlap"));
                }
                logical(metadata, None, child, row, execution, context, depth + 1)?;
            }
        }
        _ if residual.is_some() => {
            return Err(failed("conflicting raw and shredded non-object values"));
        }
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(typed.clone(), execution)?;
            if let Some((start, count)) = list.coordinates(row, execution)? {
                for index in start..start + count {
                    logical(
                        metadata,
                        None,
                        &list.elements,
                        index,
                        execution,
                        context,
                        depth + 1,
                    )?;
                }
            }
        }
        DType::Variant(_) => {
            return Err(failed(
                "nested canonical Variant shredding is not admitted; supply Parquet Variant storage",
            ));
        }
        _ => validation::leaf(typed, row, execution)?,
    }
    Ok(())
}
