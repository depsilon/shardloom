// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom correction to the pinned Arrow C stream provider.

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, Schema, UnionFields, ffi::FFI_ArrowSchema,
};

type Result<T> = std::result::Result<T, ArrowError>;

const EXTENSION_NAME: &str = "ARROW:extension:name";
const EXTENSION_METADATA: &str = "ARROW:extension:metadata";

/// Rust stores an extension declaration on a Field. The C interface may put
/// that declaration on dictionary values, which have no separate Rust Field.
/// Preserve those declarations while retaining the ordinary Arrow parser for
/// physical types, nullability, names, ordering and unrelated field metadata.
pub(super) fn import(raw: &FFI_ArrowSchema) -> Result<Schema> {
    let schema = Schema::try_from(raw)?;
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| import_field(field, raw.child(index)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

fn import_field(field: &Field, raw: &FFI_ArrowSchema) -> Result<FieldRef> {
    let data_type = import_type(field.data_type(), raw)?;
    let mut metadata = field.metadata().clone();
    let mut values = raw.dictionary();
    while let Some(dictionary) = values {
        merge_extension(&mut metadata, &dictionary.metadata()?)?;
        values = dictionary.dictionary();
    }
    Ok(Arc::new(
        field
            .clone()
            .with_data_type(data_type)
            .with_metadata(metadata),
    ))
}

fn import_type(data_type: &DataType, raw: &FFI_ArrowSchema) -> Result<DataType> {
    let child = |field: &Field, index| import_field(field, raw.child(index));
    Ok(match data_type {
        DataType::Dictionary(keys, values) => DataType::Dictionary(
            keys.clone(),
            Box::new(import_type(
                values,
                raw.dictionary().ok_or_else(|| {
                    ArrowError::CDataInterface("dictionary schema has no values".into())
                })?,
            )?),
        ),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| child(field, index))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        DataType::List(field) => DataType::List(child(field, 0)?),
        DataType::LargeList(field) => DataType::LargeList(child(field, 0)?),
        DataType::ListView(field) => DataType::ListView(child(field, 0)?),
        DataType::LargeListView(field) => DataType::LargeListView(child(field, 0)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field, 0)?, *size),
        DataType::Map(field, sorted) => DataType::Map(child(field, 0)?, *sorted),
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(child(ends, 0)?, child(values, 1)?)
        }
        DataType::Union(fields, mode) => {
            let children = fields
                .iter()
                .enumerate()
                .map(|(index, (_, field))| child(field, index))
                .collect::<Result<Vec<_>>>()?;
            DataType::Union(
                UnionFields::try_new(fields.iter().map(|(id, _)| id), children)?,
                *mode,
            )
        }
        _ => data_type.clone(),
    })
}

fn merge_extension(
    field: &mut HashMap<String, String>,
    values: &HashMap<String, String>,
) -> Result<()> {
    let declared = |metadata: &HashMap<String, String>| {
        metadata.contains_key(EXTENSION_NAME) || metadata.contains_key(EXTENSION_METADATA)
    };
    if !declared(values) {
        return Ok(());
    }
    if declared(field) {
        let metadata = |map: &HashMap<String, String>| {
            map.get(EXTENSION_METADATA).cloned().unwrap_or_default()
        };
        if field.get(EXTENSION_NAME) != values.get(EXTENSION_NAME)
            || metadata(field) != metadata(values)
        {
            return Err(ArrowError::CDataInterface(
                "conflicting extension declarations on dictionary indices and values".into(),
            ));
        }
    }
    // Only extension metadata belongs to the dictionary's logical value type.
    // Incomplete and unknown declarations survive for consumer admission checks.
    for key in [EXTENSION_NAME, EXTENSION_METADATA] {
        if let Some(value) = values.get(key) {
            field.entry(key.into()).or_insert_with(|| value.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
