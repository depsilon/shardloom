// SPDX-License-Identifier: Apache-2.0
//! Pre-admitted schema/control structures, independent of output row geometry.
//! These source-derived shape bounds exclude payloads, allocator bookkeeping,
//! call stacks and RSS; buffer and codec owners admit their own real storage.
use std::mem::size_of;

use crate::{
    column::Column,
    error::{IoSnafu, Result},
    projection::ProjectionMask,
    proto::{stream::Kind, StripeFooter},
    reader::metadata::FileMetadata,
    schema::{DataType, NamedColumn, RootDataType},
    RowSelection,
};
use snafu::ResultExt;

use super::memory::invalid;

#[cfg(test)]
#[path = "structure_memory_tests.rs"]
mod tests;

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| invalid("ORC structure size overflow"))
        .context(IoSnafu)
}

fn times(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("ORC structure size overflow"))
        .context(IoSnafu)
}

fn table<T>(entries: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    // The standard hash table keeps load below 7/8, rounds to power-of-two
    // buckets and has one control byte per bucket plus a SIMD control group.
    // Four slots per entry also covers old/new table overlap during growth.
    add(times(entries, times(4, add(size_of::<T>(), 1)?)?)?, 2 * 16)
}

#[derive(Default)]
struct Shape {
    nodes: usize,
    heap: usize,
    // Sum of descendant clone heaps constructed by Column::children. Those
    // temporaries can overlap along the decoder-construction stack.
    descendants: usize,
    names: usize,
}

impl Shape {
    fn include(&mut self, child: Self, name: usize) -> Result<()> {
        self.nodes = add(self.nodes, child.nodes)?;
        self.heap = add(self.heap, add(child.heap, name)?)?;
        self.descendants = add(self.descendants, add(child.heap, child.descendants)?)?;
        self.names = add(self.names, add(child.names, name)?)?;
        Ok(())
    }
}

fn dtype(dtype: &DataType) -> Result<Shape> {
    let mut shape = Shape {
        nodes: 1,
        ..Shape::default()
    };
    match dtype {
        DataType::Struct { children, .. } => {
            shape.heap = times(children.len(), size_of::<NamedColumn>())?;
            for child in children {
                shape.include(self::dtype(child.data_type())?, child.name().len())?;
            }
        }
        DataType::List { child, .. } => {
            shape.heap = size_of::<DataType>();
            shape.include(self::dtype(child)?, 4)?;
        }
        DataType::Map { key, value, .. } => {
            shape.heap = 2 * size_of::<DataType>();
            shape.include(self::dtype(key)?, 3)?;
            shape.include(self::dtype(value)?, 5)?;
        }
        DataType::Union { variants, .. } => {
            shape.heap = times(variants.len(), size_of::<DataType>())?;
            for child in variants {
                // At most 127 variants are admitted by the schema parser.
                shape.include(self::dtype(child)?, 3)?;
            }
        }
        _ => {}
    }
    Ok(shape)
}

fn root(root: &RootDataType, mask: Option<&ProjectionMask>) -> Result<Shape> {
    let mut shape = Shape::default();
    for column in root.children() {
        if mask.map_or(true, |mask| {
            mask.is_index_projected(column.data_type().column_index())
        }) {
            shape.heap = add(shape.heap, size_of::<NamedColumn>())?;
            shape.include(dtype(column.data_type())?, column.name().len())?;
        }
    }
    Ok(shape)
}

pub(crate) fn reader(
    metadata: &FileMetadata,
    mask: &ProjectionMask,
    selection: Option<&RowSelection>,
    reader_bytes: usize,
) -> Result<usize> {
    let shape = root(metadata.root_data_type(), Some(mask))?;
    // Projected ORC type, Arrow schema and conversion temporaries. Root clones
    // share immutable storage. New names/fields and collection capacity remain
    // conservatively covered before projection/schema construction.
    let base = reader_bytes
        + size_of::<arrow::datatypes::Schema>()
        + size_of::<RootDataType>()
        + size_of::<[usize; 8]>();
    let mut bytes = add(
        base,
        times(3, add(shape.heap, table::<usize>(add(shape.nodes, 1)?)?)?)?,
    )?;
    let field_handles = 4 * size_of::<arrow::datatypes::FieldRef>() + size_of::<[usize; 8]>();
    bytes = add(
        bytes,
        times(
            shape.nodes,
            2 * size_of::<arrow::datatypes::Field>() + field_handles,
        )?,
    )?;
    bytes = add(
        bytes,
        table::<(String, String)>(metadata.user_custom_metadata().len())?,
    )?;
    for (key, value) in metadata.user_custom_metadata() {
        // Lossy UTF-8 can expand each invalid input byte to three bytes, and
        // the temporary and final metadata maps overlap during schema creation.
        bytes = add(bytes, times(2, add(key.len(), times(3, value.len())?)?)?)?;
    }
    if let Some(selection) = selection {
        bytes = add(
            bytes,
            times(
                selection.selectors().len(),
                3 * size_of::<crate::row_selection::RowSelector>(),
            )?,
        )?;
    }
    Ok(bytes)
}

pub(crate) fn stripe(root_type: &RootDataType, footer: &StripeFooter) -> Result<usize> {
    let shape = root(root_type, None)?;
    let streams = footer
        .streams
        .iter()
        .filter(|stream| root_type.contains_column_index(stream.column() as usize))
        .count();
    let mut bytes = add(
        size_of::<crate::stripe::Stripe>(),
        table::<((u32, Kind), bytes::Bytes)>(streams)?,
    )?;
    bytes = add(
        bytes,
        times(shape.nodes, crate::array_decoder::metadata_per_node())?,
    )?;
    bytes = add(
        bytes,
        times(
            shape.nodes,
            4 * size_of::<Column>() + Column::storage_bytes(),
        )?,
    )?;
    bytes = add(
        bytes,
        times(2, add(add(shape.heap, shape.descendants)?, shape.names)?)?,
    )?;
    Ok(bytes)
}
