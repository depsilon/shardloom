// SPDX-License-Identifier: Apache-2.0
//! Source-derived capacity for Avro type resolution and record construction.
//!
//! Named references carry checked shapes rather than recursively expanding a
//! type tree during admission. The credited reader/schema owner covers codec,
//! decoder, projection and Arrow metadata structures. Output payloads and JSON
//! parsing have separate owners. Capacity is conservative, not minimum heap/RSS.

use super::decoder_memory::{DecoderMemoryReservation, Memory, invalid};
use super::record_buffers::Values;
use super::schema_memory::{add, times};
use crate::codec::{AvroDataType, AvroField, AvroLiteral, ResolutionInfo, ResolvedField};
use crate::errors::AvroError;
use crate::schema::{Attributes, ComplexType, Schema, TypeName};
use serde_json::Value;
use std::{mem::size_of, sync::Arc};

fn table<T>(entries: usize) -> Result<usize, AvroError> {
    if entries == 0 {
        return Ok(0);
    }
    add(times(entries, times(4, add(size_of::<T>(), 1)?)?)?, 32)
}

fn quoted(bytes: usize) -> Result<usize, AvroError> {
    // JSON control bytes can each become a six-byte Unicode escape.
    add(times(bytes, 6)?, 2)
}

fn json_bytes(value: &Value) -> Result<usize, AvroError> {
    match value {
        Value::Null | Value::Bool(_) => Ok(5),
        Value::Number(_) => Ok(32), // all supported i64/u64/f64 JSON renderings
        Value::String(text) => quoted(text.len()),
        Value::Array(values) => values
            .iter()
            .try_fold(add(2, values.len())?, |bytes, value| {
                add(bytes, json_bytes(value)?)
            }),
        Value::Object(values) => values
            .iter()
            .try_fold(add(2, times(values.len(), 2)?)?, |bytes, (key, value)| {
                add(add(bytes, quoted(key.len())?)?, json_bytes(value)?)
            }),
    }
}

fn serialized(value: &Value) -> Result<usize, AvroError> {
    // serde_json::to_vec starts with capacity 128. Allow geometric growth and
    // overlapping old/new storage, including defaults retained as JSON text.
    times(4, json_bytes(value)?.max(128))
}

fn literal(value: &Value) -> Result<usize, AvroError> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(0),
        Value::String(text) => times(4, text.len()),
        Value::Array(values) => values.iter().try_fold(
            times(values.len(), 4 * size_of::<AvroLiteral>())?,
            |bytes, value| add(bytes, literal(value)?),
        ),
        Value::Object(values) => {
            // AvroLiteral::Map is an IndexMap: ordered entries plus hash indices.
            let entries = times(values.len(), 4 * size_of::<(usize, String, AvroLiteral)>())?;
            values.iter().try_fold(
                add(entries, table::<usize>(values.len())?)?,
                |bytes, (key, value)| add(add(bytes, key.len())?, literal(value)?),
            )
        }
    }
}

fn attributes(attributes: &Attributes<'_>) -> Result<usize, AvroError> {
    let mut bytes = table::<(String, String)>(add(attributes.additional.len(), 3)?)?;
    // Named/logical metadata keys and the copied logical-type spelling.
    bytes = add(bytes, "avro.nameavro.namespacelogicalType".len())?;
    bytes = add(bytes, attributes.logical_type.map_or(0, str::len))?;
    for (key, value) in &attributes.additional {
        bytes = add(bytes, add(key.len(), serialized(value)?)?)?;
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug)]
struct Shape {
    nodes: usize,
    heap: usize,
    height: usize,
    // General unions, references and aliases can resolve to more than one
    // candidate even for a projected writer schema. Keep the full pair bound.
    complex_resolution: bool,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            nodes: 1,
            heap: 0,
            height: 1,
            complex_resolution: false,
        }
    }
}

impl Shape {
    fn child(&mut self, child: Self) -> Result<(), AvroError> {
        self.nodes = add(self.nodes, child.nodes)?;
        self.heap = add(self.heap, child.heap)?;
        self.height = self.height.max(add(child.height, 1)?);
        self.complex_resolution |= child.complex_resolution;
        Ok(())
    }
    fn names(
        &mut self,
        name: &str,
        namespace: Option<&str>,
        aliases: &[&str],
    ) -> Result<(), AvroError> {
        self.heap = add(self.heap, add(name.len(), namespace.map_or(0, str::len))?)?;
        // Name/alias matching builds owned full names in two temporary sets.
        self.heap = add(self.heap, table::<String>(add(aliases.len(), 1)?)?)?;
        for alias in aliases {
            self.heap = add(
                self.heap,
                add(add(alias.len(), namespace.map_or(0, str::len))?, 1)?,
            )?;
        }
        self.complex_resolution |= !aliases.is_empty();
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Named<'a> {
    name: &'a str,
    namespace: &'a str,
    shape: Shape,
}

struct Shapes<'a> {
    names: Values<Named<'a>>,
}

impl<'a> Shapes<'a> {
    fn named(
        &mut self,
        name: &'a str,
        namespace: Option<&'a str>,
        shape: Shape,
    ) -> Result<Shape, AvroError> {
        // Reverse lookup preserves the provider's last registration on a name.
        self.names.push(Named {
            name,
            namespace: namespace.unwrap_or(""),
            shape,
        })?;
        Ok(shape)
    }

    fn reference(&self, name: &str, namespace: Option<&str>) -> Shape {
        let (namespace, name) = name
            .rsplit_once('.')
            .unwrap_or((namespace.unwrap_or(""), name));
        let mut shape = self
            .names
            .iter()
            .rev()
            .find(|entry| entry.name == name && entry.namespace == namespace)
            .map_or_else(Shape::default, |entry| entry.shape);
        // Unknown/recursive references still fail in the original resolver.
        shape.complex_resolution = true;
        shape
    }

    fn schema(
        &mut self,
        schema: &Schema<'a>,
        namespace: Option<&'a str>,
    ) -> Result<Shape, AvroError> {
        let mut shape = Shape::default();
        match schema {
            Schema::TypeName(TypeName::Primitive(_)) => Ok(shape),
            Schema::TypeName(TypeName::Ref(name)) => Ok(self.reference(name, namespace)),
            Schema::Type(ty) => {
                if let TypeName::Ref(name) = ty.r#type {
                    shape = self.reference(name, namespace);
                }
                shape.heap = add(shape.heap, attributes(&ty.attributes)?)?;
                // Custom annotations may wrap a values type in run-end fields.
                shape.nodes = add(shape.nodes, 2)?;
                shape.height = add(shape.height, 2)?;
                Ok(shape)
            }
            Schema::Union(children) => {
                let nullable = children.len() == 2
                    && children.iter().any(|child| {
                        matches!(
                            child,
                            Schema::TypeName(TypeName::Primitive(
                                crate::schema::PrimitiveType::Null
                            ))
                        )
                    });
                shape.complex_resolution = !nullable;
                for child in children {
                    shape.child(self.schema(child, namespace)?)?;
                }
                shape.heap = add(shape.heap, table::<(usize, AvroDataType)>(children.len())?)?;
                Ok(shape)
            }
            Schema::Complex(ComplexType::Record(record)) => {
                let namespace = record.namespace.or(namespace);
                shape.names(record.name, namespace, &record.aliases)?;
                shape.heap = add(shape.heap, attributes(&record.attributes)?)?;
                let mut aliases = 0;
                for (index, field) in record.fields.iter().enumerate() {
                    shape.child(self.schema(&field.r#type, namespace)?)?;
                    shape.names(field.name, namespace, &field.aliases)?;
                    aliases = add(aliases, field.aliases.len())?;
                    shape.complex_resolution |= record.fields[..index]
                        .iter()
                        .any(|prior| prior.name == field.name);
                    if let Some(default) = &field.default {
                        shape.heap =
                            add(shape.heap, add(serialized(default)?, literal(default)?)?)?;
                    }
                }
                let lookup = add(times(record.fields.len(), 2)?, aliases)?;
                shape.heap = add(
                    shape.heap,
                    add(table::<(&str, usize)>(lookup)?, table::<&str>(aliases)?)?,
                )?;
                self.named(record.name, namespace, shape)
            }
            Schema::Complex(ComplexType::Enum(enumeration)) => {
                let namespace = enumeration.namespace.or(namespace);
                shape.names(enumeration.name, namespace, &enumeration.aliases)?;
                shape.heap = add(shape.heap, attributes(&enumeration.attributes)?)?;
                let mut rendered = add(2, enumeration.symbols.len())?;
                for symbol in &enumeration.symbols {
                    shape.heap = add(shape.heap, symbol.len())?;
                    rendered = add(rendered, quoted(symbol.len())?)?;
                }
                shape.heap = add(shape.heap, times(4, rendered.max(128))?)?;
                shape.heap = add(
                    shape.heap,
                    times(
                        enumeration.symbols.len(),
                        4 * (size_of::<String>() + size_of::<i32>()),
                    )?,
                )?;
                shape.heap = add(shape.heap, table::<(&str, i32)>(enumeration.symbols.len())?)?;
                shape.nodes = 3; // Arrow dictionary plus its key/value types
                shape.height = 2;
                self.named(enumeration.name, namespace, shape)
            }
            Schema::Complex(ComplexType::Fixed(fixed)) => {
                let namespace = fixed.namespace.or(namespace);
                shape.names(fixed.name, namespace, &fixed.aliases)?;
                shape.heap = add(shape.heap, attributes(&fixed.attributes)?)?;
                self.named(fixed.name, namespace, shape)
            }
            Schema::Complex(ComplexType::Array(array)) => {
                shape.child(self.schema(&array.items, namespace)?)?;
                shape.heap = add(
                    shape.heap,
                    add(attributes(&array.attributes)?, "item".len())?,
                )?;
                Ok(shape)
            }
            Schema::Complex(ComplexType::Map(map)) => {
                shape.nodes = 3; // map, entries struct and implicit string key
                let child = self.schema(&map.values, namespace)?;
                shape.child(child)?;
                shape.height = add(child.height, 2)?;
                shape.heap = add(
                    shape.heap,
                    add(attributes(&map.attributes)?, "entrieskeyvalue".len())?,
                )?;
                Ok(shape)
            }
        }
    }
}

fn capacity(
    writer: Shape,
    reader: Option<Shape>,
    projection_only: bool,
) -> Result<usize, AvroError> {
    // serde limits syntax nesting, but a flat list of named definitions can
    // produce a much deeper expanded codec/Arrow tree. Apply the same 128-level
    // ceiling before recursive construction, independently of available heap.
    // This guard applies only when a caller enables controlled ownership.
    if writer.height > 128 || reader.is_some_and(|shape| shape.height > 128) {
        return Err(invalid(
            "controlled Avro schema expansion exceeds depth 128",
        ));
    }
    let mut nodes = writer.nodes;
    let mut heap = writer.heap;
    let mut height = writer.height;
    if let Some(reader) = reader {
        nodes = add(nodes, reader.nodes)?;
        heap = add(heap, reader.heap)?;
        height = height.max(reader.height);
        if !projection_only || writer.complex_resolution || reader.complex_resolution {
            // General resolution may retain writer-reader pair plans, defaults,
            // skippers and named metadata. Bound all expanded pairs, not only
            // the textual field count or the final selected reader fields.
            nodes = add(nodes, times(writer.nodes, reader.nodes)?)?;
            heap = add(
                heap,
                add(
                    times(writer.heap, reader.nodes)?,
                    times(reader.heap, writer.nodes)?,
                )?,
            )?;
            height = add(writer.height, reader.height)?;
        }
    }
    let unit = super::record::structure_unit_bytes()
        + size_of::<AvroDataType>()
        + size_of::<AvroField>()
        + size_of::<ResolutionInfo>()
        + size_of::<ResolvedField>();
    let represented = add(
        add(times(nodes, unit)?, heap)?,
        table::<((&str, &str), AvroDataType)>(nodes)?,
    )?;
    // Codec/Arrow/decoder construction can recreate descendant fields along
    // each active ancestor. Four slots cover Vec growth and old/new Arc-array
    // copies. This is a source-shaped capacity, not a smallest-workspace claim.
    times(times(4, add(height, 1)?)?, represented)
}

pub(super) fn reserve(
    memory: &Memory,
    writer: &Schema<'_>,
    reader: Option<&Schema<'_>>,
    projection_only: bool,
) -> Result<Option<Arc<dyn DecoderMemoryReservation>>, AvroError> {
    let Some(factory) = memory else {
        return Ok(None);
    };
    let mut names = Values::new();
    names.set_memory(memory)?;
    let mut shapes = Shapes { names };
    let writer = shapes.schema(writer, None)?;
    let reader = reader
        .map(|reader| shapes.schema(reader, None))
        .transpose()?;
    let bytes = capacity(writer, reader, projection_only)?;
    drop(shapes);
    Ok(Some(Arc::from(factory.reserve(bytes)?)))
}

#[cfg(test)]
#[path = "structure_memory_tests.rs"]
mod tests;
