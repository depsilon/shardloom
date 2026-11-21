//! Original recursive Variant intake used for shallow compatibility tests.

use super::{binary, dictionary_key, failed, workspace};
use arrow_array::{Array, ArrayRef, StructArray, cast::AsArray as _};
use arrow_schema::DataType;
use parquet_variant::{Variant, VariantMetadata};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;

pub(super) fn validate(
    array: &dyn Array,
    start: usize,
    end: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    if let Some(dictionary) = array.as_any_dictionary_opt() {
        for row in start..end {
            check()?;
            if dictionary.keys().is_null(row) {
                continue;
            }
            let key = dictionary_key(dictionary.keys(), row)?;
            if key >= dictionary.values().len() {
                return Err(failed("dictionary index exceeds its values"));
            }
            validate(dictionary.values().as_ref(), key, key + 1, memory, check)?;
        }
        return Ok(());
    }
    let array = array
        .as_struct_opt()
        .ok_or_else(|| failed("expected Variant struct storage"))?;
    let metadata = array
        .column_by_name("metadata")
        .ok_or_else(|| failed("missing metadata"))?;
    for row in start..end {
        check()?;
        if array.is_null(row) {
            continue;
        }
        let metadata = binary(metadata.as_ref(), row)?
            .ok_or_else(|| failed("present value requires nonnull metadata"))?;
        let _credit = memory
            .map(|memory| memory.reserve(workspace(metadata.len())?))
            .transpose()?;
        VariantMetadata::try_new(metadata).map_err(failed)?;
        parts(
            metadata,
            array.column_by_name("value"),
            array.column_by_name("typed_value"),
            row,
            memory,
            check,
            0,
        )?;
    }
    Ok(())
}

pub(super) fn parts(
    metadata: &[u8],
    value: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
    depth: usize,
) -> Result<bool> {
    check()?;
    if depth > 24 {
        return Err(failed("shredded value exceeds the current depth boundary"));
    }
    let raw = value
        .map(|value| binary(value.as_ref(), row))
        .transpose()?
        .flatten();
    let _credit = memory
        .map(|memory| memory.reserve(workspace(raw.map_or(0, <[u8]>::len))?))
        .transpose()?;
    let parsed = raw
        .map(|raw| super::super::value(metadata, raw, check))
        .transpose()?;
    let Some(typed) = typed.filter(|typed| typed.is_valid(row)) else {
        return Ok(parsed.is_some());
    };
    if !matches!(typed.data_type(), DataType::Struct(_)) && parsed.is_some() {
        return Err(failed("conflicting raw and shredded non-object values"));
    }
    match typed.data_type() {
        DataType::Struct(fields) => {
            let residual = match &parsed {
                None => None,
                Some(Variant::Object(object)) => Some(object),
                Some(_) => return Err(failed("shredded object requires an object residual")),
            };
            let structure = typed.as_struct();
            for (field, child) in fields.iter().zip(structure.columns()) {
                let present = shell(metadata, child.as_ref(), row, memory, check, depth + 1)?;
                if present
                    && VariantMetadata::try_new(metadata)
                        .map_err(failed)?
                        .get_entry(field.name())
                        .is_none()
                {
                    return Err(failed(
                        "shredded object field is absent from its metadata dictionary",
                    ));
                }
                if present && residual.is_some_and(|object| object.get(field.name()).is_some()) {
                    return Err(failed("raw and shredded object fields overlap"));
                }
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let list = if matches!(typed.data_type(), DataType::List(_)) {
                typed.as_list::<i32>().value(row)
            } else {
                typed.as_list::<i64>().value(row)
            };
            for index in 0..list.len() {
                shell(metadata, list.as_ref(), index, memory, check, depth + 1)?;
            }
        }
        DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Date32
        | DataType::Time64(arrow_schema::TimeUnit::Microsecond)
        | DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond | arrow_schema::TimeUnit::Nanosecond,
            _,
        ) => {}
        _ => {
            return Err(failed(
                "unsupported shredded physical dtype; supply unshredded Variant storage",
            ));
        }
    }
    Ok(true)
}

fn shell(
    metadata: &[u8],
    array: &dyn Array,
    row: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
    depth: usize,
) -> Result<bool> {
    let array: &StructArray = array
        .as_struct_opt()
        .ok_or_else(|| failed("shredded field requires a value/typed_value struct"))?;
    if array.is_null(row) {
        return Ok(false);
    }
    if array.fields().is_empty()
        || array
            .fields()
            .iter()
            .any(|field| !matches!(field.name().as_str(), "value" | "typed_value"))
    {
        return Err(failed(
            "shredded field requires only value/typed_value children",
        ));
    }
    parts(
        metadata,
        array.column_by_name("value"),
        array.column_by_name("typed_value"),
        row,
        memory,
        check,
        depth,
    )
}
