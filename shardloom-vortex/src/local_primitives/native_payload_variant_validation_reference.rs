use super::*;

#[allow(clippy::too_many_arguments)]
pub(in crate::local_primitives::native_payload) fn parts(
    metadata: &[u8],
    value: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<bool> {
    context.check_cancelled()?;
    if depth > 24 {
        return Err(failed(
            "Variant shredded value exceeds the current depth boundary",
        ));
    }
    let raw = value
        .map(|array| binary(array, row, execution))
        .transpose()?
        .flatten();
    let _credit = context
        .memory()
        .reserve(workspace(raw.as_ref().map_or(0, ByteBuffer::len))?)?;
    let parsed = raw
        .as_ref()
        .map(|raw| {
            crate::native_variant::value(metadata, raw.as_slice(), &|| context.check_cancelled())
        })
        .transpose()?;
    let typed = match typed {
        Some(typed) if typed.is_valid(row, execution).map_err(vortex_error)? => Some(typed),
        _ => None,
    };
    let Some(typed) = typed else {
        return Ok(parsed.is_some());
    };
    if !matches!(typed.dtype(), DType::Struct(..)) && parsed.is_some() {
        return Err(failed(
            "Variant has conflicting raw and shredded non-object values",
        ));
    }
    match typed.dtype() {
        DType::Struct(..) => {
            let raw_object = match &parsed {
                None => None,
                Some(Variant::Object(object)) => Some(object),
                Some(_) => {
                    return Err(failed(
                        "Variant shredded object requires an object residual",
                    ));
                }
            };
            let structure = typed
                .clone()
                .execute::<StructArray>(execution)
                .map_err(vortex_error)?;
            for (index, name) in structure.names().iter().enumerate() {
                check(index, context)?;
                let present = shell(
                    metadata,
                    structure.unmasked_field(index),
                    row,
                    execution,
                    context,
                    depth + 1,
                )?;
                if present
                    && VariantMetadata::try_new(metadata)
                        .map_err(vortex_error)?
                        .get_entry(name.as_ref())
                        .is_none()
                {
                    return Err(failed(
                        "Variant shredded field is absent from its metadata dictionary",
                    ));
                }
                if present && raw_object.is_some_and(|object| object.get(name.as_ref()).is_some()) {
                    return Err(failed("Variant raw and shredded object fields overlap"));
                }
            }
        }
        DType::List(..) => {
            let list = crate::local_primitives::native_list::Column::new(typed.clone(), execution)?;
            if let Some((start, count)) = list.coordinates(row, execution)? {
                for index in start..start + count {
                    check(index, context)?;
                    shell(
                        metadata,
                        &list.elements,
                        index,
                        execution,
                        context,
                        depth + 1,
                    )?;
                }
            }
        }
        _ => leaf(typed, row, execution)?,
    }
    Ok(true)
}

fn shell(
    metadata: &[u8],
    field: &ArrayRef,
    row: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    depth: usize,
) -> Result<bool> {
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
        return Ok(false);
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
            .map(|index| field.unmasked_field(index))
    };
    parts(
        metadata,
        find("value"),
        find("typed_value"),
        row,
        execution,
        context,
        depth,
    )
}
