// SPDX-License-Identifier: Apache-2.0
//! Admission for schema parsing, before serde allocates its buffered forms.
//!
//! This bounds the named parser structures for the pinned serde/serde_json
//! implementations. It is not an allocator or RSS bound, nor a bound on Avro
//! named-type expansion or the eventual record decoder. Those are later owners.

use super::decoder_memory::{DecoderMemoryReservation, Memory, invalid};
use crate::errors::AvroError;
use crate::schema::{AvroSchema, Enum, Field, Record, Schema};
use serde_json::Value;
use std::mem::size_of;

pub(super) fn add(left: usize, right: usize) -> Result<usize, AvroError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("Avro schema capacity overflow"))
}

pub(super) fn times(left: usize, right: usize) -> Result<usize, AvroError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("Avro schema capacity overflow"))
}

fn table<T>(entries: usize, maps: usize) -> Result<usize, AvroError> {
    // Hash table load, power-of-two capacity and old/new growth overlap. The
    // control groups are counted per possible map, including small maps.
    add(
        times(entries, times(4, add(size_of::<T>(), 1)?)?)?,
        times(maps, 32)?,
    )
}

#[derive(Default)]
struct JsonShape {
    tokens: usize,
    depth_sum: usize,
    max_depth: usize,
    objects: usize,
    entries: usize,
}

impl JsonShape {
    fn inspect(raw: &[u8]) -> Result<Self, AvroError> {
        let mut shape = Self {
            tokens: 1,
            depth_sum: 1,
            ..Self::default()
        };
        let (mut depth, mut quoted, mut escaped) = (0_usize, false, false);
        for &byte in raw {
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                }
                continue;
            }
            if byte.is_ascii_whitespace() {
                continue;
            }
            if matches!(byte, b'{' | b'[') {
                depth = add(depth, 1)?;
                shape.max_depth = shape.max_depth.max(depth);
            }
            shape.tokens = add(shape.tokens, 1)?;
            shape.depth_sum = add(shape.depth_sum, add(depth, 1)?)?;
            match byte {
                b'"' => quoted = true,
                b'{' => shape.objects = add(shape.objects, 1)?,
                b':' => shape.entries = add(shape.entries, 1)?,
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        // This pass only counts capacity. The original serde parser still owns
        // all syntax, type, duplicate-field and recursion-limit decisions.
        Ok(shape)
    }

    fn parser_bytes(&self, raw_bytes: usize) -> Result<usize, AvroError> {
        let ast_width = [
            size_of::<Schema<'_>>(),
            size_of::<Field<'_>>(),
            size_of::<Record<'_>>(),
            size_of::<Enum<'_>>(),
        ]
        .into_iter()
        .max()
        .unwrap();
        let ast = times(self.tokens, times(4, ast_width)?)?;
        // serde's buffered Content is a tagged scalar/String/Vec. The tuple is
        // a width bound on both 32- and 64-bit targets; a map stores two Contents.
        // Untagged enums, internal tags and flattened fields can buffer an
        // ancestor's subtree concurrently. Weighted depth counts those copies;
        // four capacity slots cover small Vec minima and replacement overlap.
        let content_pair = times(2, size_of::<(u64, Vec<u8>)>())?;
        let buffered = times(
            times(4, content_pair)?,
            add(times(3, self.depth_sum)?, self.tokens)?,
        )?;
        // serde_json Value uses Vec arrays and BTreeMap objects. For the pinned
        // standard B-tree, each non-root node has at least five entries, with
        // at most eleven entries and twelve edges in an allocated node.
        let tree_node = add(
            size_of::<[(String, Value); 11]>(),
            add(size_of::<[usize; 12]>(), size_of::<[usize; 2]>())?,
        )?;
        let tree_nodes = add(self.objects, self.entries.div_ceil(5))?;
        let json = add(
            times(self.tokens, 4 * size_of::<Value>())?,
            times(tree_nodes, tree_node)?,
        )?;
        let maps = table::<(&str, Value)>(self.entries, self.objects)?;
        // Escaped strings never decode to more bytes than their input. Each
        // buffered ancestor can own a copy; include parser scratch and growing
        // serialization output as separate overlapping forms.
        let strings = times(raw_bytes, add(times(4, self.max_depth)?, 16)?)?;
        add(add(ast, buffered)?, add(add(json, maps)?, strings)?)
    }
}

fn json_workspace(raw: &[u8]) -> Result<usize, AvroError> {
    JsonShape::inspect(raw)?.parser_bytes(raw.len())
}

pub(super) fn reserve(
    memory: &Memory,
    writer: &[u8],
    reader: Option<&AvroSchema>,
    projection: Option<&[usize]>,
) -> Result<Option<Box<dyn DecoderMemoryReservation>>, AvroError> {
    let Some(memory) = memory else {
        return Ok(None);
    };
    let writer_bytes = json_workspace(writer)?;
    let reader_bytes = reader
        .map(|schema| json_workspace(schema.json_string.as_bytes()))
        .transpose()?;
    let mut bytes = add(writer_bytes, reader_bytes.unwrap_or(0))?;
    if let Some(projection) = projection {
        // The base schema, projected Value tree/string and parsed reader schema
        // can overlap. Projection only removes fields; its output has no more
        // nodes or string content than the base. The parser's string capacity
        // includes JSON escaping and serialization growth.
        bytes = add(bytes, times(2, reader_bytes.unwrap_or(writer_bytes))?)?;
        bytes = add(bytes, table::<usize>(projection.len(), 1)?)?;
        bytes = add(bytes, times(projection.len(), size_of::<Value>())?)?;
    }
    Ok(Some(memory.reserve(bytes)?))
}

#[cfg(test)]
#[path = "schema_memory_tests.rs"]
mod tests;
