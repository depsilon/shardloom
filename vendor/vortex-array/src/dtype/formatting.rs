// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Borrowed schema formatting with the existing Display wire spelling.

use std::fmt;
use std::fmt::Write;
use std::slice;

use vortex_error::VortexExpect;

use super::extension::ExtDTypeRef;
use super::{
    DType, DecimalDType, FieldDType, FieldNames, MapDType, Nullability, PType, StructFields,
    UnionVariants,
};

struct Output<'a, 'b> {
    formatter: &'a mut fmt::Formatter<'b>,
    // The original struct/union formatting joins child strings before writing
    // them to its caller. Nested joins inside that infallible buffer can share
    // it without retaining one complete child string per ancestor.
    joined: Option<String>,
}

impl Output<'_, '_> {
    fn write(&mut self, arguments: fmt::Arguments<'_>) -> fmt::Result {
        match &mut self.joined {
            Some(joined) => joined.write_fmt(arguments),
            None => self.formatter.write_fmt(arguments),
        }
    }
}

struct Fields<'a> {
    names: &'a FieldNames,
    fields: &'a [FieldDType],
    tags: Option<&'a [u8]>,
    union: bool,
    index: usize,
    pretty: Option<usize>,
}

enum Work<'a> {
    DType(&'a DType, Option<usize>),
    Struct(&'a StructFields, Option<Nullability>, Option<usize>),
    Union(&'a UnionVariants),
    Map(&'a MapDType),
    Extension(&'a ExtDTypeRef),
    Fields(Fields<'a>),
    Text(&'static str),
    Nullability(Nullability),
    EndFixed(u32, Nullability),
    EndMap(bool),
    EndPrettyStruct {
        depth: usize,
        nonempty: bool,
        nullable: Option<Nullability>,
    },
    JoinedStruct(Option<Nullability>),
    JoinedUnion,
}

pub(super) fn display_dtype(dtype: &DType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::DType(dtype, None), f)
}

pub(super) fn display_struct(fields: &StructFields, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::Struct(fields, None, f.alternate().then_some(0)), f)
}

pub(super) fn display_union(variants: &UnionVariants, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::Union(variants), f)
}

pub(super) fn display_map(map: &MapDType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::Map(map), f)
}

pub(super) fn display_extension(
    extension: &ExtDTypeRef,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::Extension(extension), f)
}

fn display(mut work: Work<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut output = Output {
        formatter: f,
        joined: None,
    };
    let mut pending = Vec::new();
    loop {
        match work {
            Work::DType(dtype, pretty) => match dtype {
                DType::Null => output.write(format_args!("null"))?,
                DType::Bool(nullable) => output.write(format_args!("bool{nullable}"))?,
                DType::Primitive(ptype, nullable) => {
                    output.write(format_args!("{ptype}{nullable}"))?
                }
                DType::Decimal(decimal, nullable) => {
                    output.write(format_args!("{decimal}{nullable}"))?
                }
                DType::Utf8(nullable) => output.write(format_args!("utf8{nullable}"))?,
                DType::Binary(nullable) => output.write(format_args!("binary{nullable}"))?,
                DType::Variant(nullable) => output.write(format_args!("variant{nullable}"))?,
                DType::List(child, nullable) => {
                    output.write(format_args!("list("))?;
                    pending.push(Work::Nullability(*nullable));
                    pending.push(Work::Text(")"));
                    work = Work::DType(child, pretty);
                    continue;
                }
                DType::FixedSizeList(child, size, nullable) => {
                    output.write(format_args!("fixed_size_list("))?;
                    pending.push(Work::EndFixed(*size, *nullable));
                    work = Work::DType(child, pretty);
                    continue;
                }
                DType::Map(map, nullable) => {
                    pending.push(Work::Nullability(*nullable));
                    work = Work::Map(map);
                    continue;
                }
                DType::Struct(fields, nullable) => {
                    work = Work::Struct(fields, Some(*nullable), pretty);
                    continue;
                }
                DType::Union(variants, nullable) => {
                    output.write(format_args!("union("))?;
                    pending.push(Work::Nullability(*nullable));
                    pending.push(Work::Text(")"));
                    work = Work::Union(variants);
                    continue;
                }
                DType::Extension(extension) => {
                    work = Work::Extension(extension);
                    continue;
                }
            },
            Work::Struct(fields, nullable, pretty) => {
                let child_pretty = if let Some(depth) = pretty {
                    output.write(format_args!("{{\n"))?;
                    pending.push(Work::EndPrettyStruct {
                        depth,
                        nonempty: !fields.names().is_empty(),
                        nullable,
                    });
                    Some(depth + 1)
                } else {
                    if output.joined.is_none() {
                        output.joined = Some(String::new());
                        pending.push(Work::JoinedStruct(nullable));
                    } else {
                        output.write(format_args!("{{"))?;
                        if let Some(nullable) = nullable {
                            pending.push(Work::Nullability(nullable));
                        }
                        pending.push(Work::Text("}"));
                    }
                    None
                };
                work = Work::Fields(Fields {
                    names: fields.names(),
                    fields: fields.raw_dtypes(),
                    tags: None,
                    union: false,
                    index: 0,
                    pretty: child_pretty,
                });
                continue;
            }
            Work::Union(variants) => {
                if output.joined.is_none() {
                    output.joined = Some(String::new());
                    pending.push(Work::JoinedUnion);
                }
                work = Work::Fields(Fields {
                    names: variants.names(),
                    fields: variants.raw_dtypes(),
                    tags: (!variants.is_consecutive()).then(|| variants.type_ids()),
                    union: true,
                    index: 0,
                    pretty: None,
                });
                continue;
            }
            Work::Map(map) => {
                // Original write! arguments resolve both fields before any
                // output or child metadata Display callback occurs.
                let key = map
                    .try_key_dtype()
                    .vortex_expect("map key dtype must be valid");
                let value = map
                    .try_value_dtype()
                    .vortex_expect("map value dtype must be valid");
                output.write(format_args!("map("))?;
                pending.push(Work::EndMap(map.keys_sorted()));
                pending.push(Work::DType(value, None));
                pending.push(Work::Text(", "));
                work = Work::DType(key, None);
                continue;
            }
            Work::Extension(extension) => {
                let metadata = extension.display_metadata().to_string();
                if metadata.is_empty() {
                    output.write(format_args!("{}", extension.id()))?;
                } else {
                    output.write(format_args!("{}[{metadata}]", extension.id()))?;
                }
                output.write(format_args!("("))?;
                pending.push(Work::Text(")"));
                work = Work::DType(extension.storage_dtype(), None);
                continue;
            }
            Work::Fields(mut fields) => {
                if fields.index < fields.fields.len().min(fields.names.len()) {
                    let index = fields.index;
                    let dtype = fields.fields[index]
                        .value_ref()
                        .vortex_expect(if fields.union {
                            "variant DType must be valid"
                        } else {
                            "field DType must be valid"
                        });
                    let name = &fields.names[index];
                    if let Some(depth) = fields.pretty {
                        if index != 0 {
                            output.write(format_args!(",\n"))?;
                        }
                        let indent = "  ".repeat(depth);
                        output.write(format_args!("{indent}{name}="))?;
                    } else {
                        if index != 0 {
                            output.write(format_args!(", "))?;
                        }
                        if let Some(tags) = fields.tags {
                            output.write(format_args!("{name}@{}=", tags[index]))?;
                        } else {
                            output.write(format_args!("{name}="))?;
                        }
                    }
                    let pretty = fields.pretty;
                    fields.index += 1;
                    pending.push(Work::Fields(fields));
                    work = Work::DType(dtype, pretty);
                    continue;
                }
            }
            Work::Text(text) => output.write(format_args!("{text}"))?,
            Work::Nullability(nullable) => output.write(format_args!("{nullable}"))?,
            Work::EndFixed(size, nullable) => output.write(format_args!(")[{size}]{nullable}"))?,
            Work::EndMap(sorted) => output.write(format_args!(", keys_sorted={sorted})"))?,
            Work::EndPrettyStruct {
                depth,
                nonempty,
                nullable,
            } => {
                if nonempty {
                    output.write(format_args!("\n"))?;
                }
                let indent = "  ".repeat(depth);
                output.write(format_args!("{indent}}}"))?;
                if let Some(nullable) = nullable {
                    output.write(format_args!("{nullable}"))?;
                }
            }
            Work::JoinedStruct(nullable) => {
                let joined = output
                    .joined
                    .take()
                    .vortex_expect("one active joined schema buffer");
                match nullable {
                    Some(nullable) => output.write(format_args!("{{{joined}}}{nullable}"))?,
                    None => output.write(format_args!("{{{joined}}}"))?,
                }
            }
            Work::JoinedUnion => {
                let joined = output
                    .joined
                    .take()
                    .vortex_expect("one active joined schema buffer");
                output.write(format_args!("{joined}"))?;
            }
        }
        let Some(next) = pending.pop() else {
            return Ok(());
        };
        work = next;
    }
}

// Debug deliberately preserves raw lazy/cache details for existing shallow
// schemas. Do not invoke the logical child cursor here: that would parse views.
const DEBUG_CHILD_DEPTH: usize = 32;

enum RawChildren<'a> {
    Direct(std::array::IntoIter<Option<&'a DType>, 2>),
    Fields(slice::Iter<'a, FieldDType>),
}

impl<'a> RawChildren<'a> {
    fn new(dtype: &'a DType) -> Self {
        let direct = match dtype {
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                [Some(child.as_ref()), None]
            }
            DType::Extension(extension) => [Some(extension.storage_dtype()), None],
            DType::Map(map, _) => {
                let (key, value) = map.raw_fields();
                [key.initialized_ref(), value.initialized_ref()]
            }
            DType::Struct(fields, _) => return Self::Fields(fields.raw_dtypes().iter()),
            DType::Union(variants, _) => return Self::Fields(variants.raw_dtypes().iter()),
            _ => [None, None],
        };
        Self::Direct(direct.into_iter())
    }
}

impl<'a> Iterator for RawChildren<'a> {
    type Item = &'a DType;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Direct(children) => children.find_map(|child| child),
            Self::Fields(children) => children.find_map(FieldDType::initialized_ref),
        }
    }
}

fn debug_depth_exceeded(dtype: &DType) -> bool {
    let mut parents: [Option<RawChildren<'_>>; DEBUG_CHILD_DEPTH] = std::array::from_fn(|_| None);
    let mut depth = 0;
    let mut children = RawChildren::new(dtype);
    loop {
        if let Some(child) = children.next() {
            if depth == DEBUG_CHILD_DEPTH {
                return true;
            }
            parents[depth] = Some(children);
            depth += 1;
            children = RawChildren::new(child);
        } else if depth != 0 {
            depth -= 1;
            children = parents[depth]
                .take()
                .vortex_expect("pending raw schema children");
        } else {
            return false;
        }
    }
}

// Borrowed variants delegate to the same standard derived formatting as the
// original enum, including caller flags and existing raw metadata Debug impls.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "the borrowed fields are read only by the derived Debug implementation"
)]
enum ShallowDebug<'a> {
    Null,
    Bool(&'a Nullability),
    Primitive(&'a PType, &'a Nullability),
    Decimal(&'a DecimalDType, &'a Nullability),
    Utf8(&'a Nullability),
    Binary(&'a Nullability),
    List(&'a DType, &'a Nullability),
    FixedSizeList(&'a DType, &'a u32, &'a Nullability),
    Map(&'a MapDType, &'a Nullability),
    Struct(&'a StructFields, &'a Nullability),
    Union(&'a UnionVariants, &'a Nullability),
    Variant(&'a Nullability),
    Extension(&'a ExtDTypeRef),
}

impl fmt::Debug for DType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if debug_depth_exceeded(self) {
            let name = match self {
                Self::List(..) => "List",
                Self::FixedSizeList(..) => "FixedSizeList",
                Self::Map(..) => "Map",
                Self::Struct(..) => "Struct",
                Self::Union(..) => "Union",
                Self::Extension(..) => "Extension",
                _ => unreachable!("leaf dtype has no raw children"),
            };
            return f.debug_tuple(name).finish_non_exhaustive();
        }
        let shallow = match self {
            Self::Null => ShallowDebug::Null,
            Self::Bool(nullable) => ShallowDebug::Bool(nullable),
            Self::Primitive(ptype, nullable) => ShallowDebug::Primitive(ptype, nullable),
            Self::Decimal(decimal, nullable) => ShallowDebug::Decimal(decimal, nullable),
            Self::Utf8(nullable) => ShallowDebug::Utf8(nullable),
            Self::Binary(nullable) => ShallowDebug::Binary(nullable),
            Self::List(child, nullable) => ShallowDebug::List(child, nullable),
            Self::FixedSizeList(child, size, nullable) => {
                ShallowDebug::FixedSizeList(child, size, nullable)
            }
            Self::Map(map, nullable) => ShallowDebug::Map(map, nullable),
            Self::Struct(fields, nullable) => ShallowDebug::Struct(fields, nullable),
            Self::Union(variants, nullable) => ShallowDebug::Union(variants, nullable),
            Self::Variant(nullable) => ShallowDebug::Variant(nullable),
            Self::Extension(extension) => ShallowDebug::Extension(extension),
        };
        shallow.fmt(f)
    }
}

#[cfg(test)]
#[path = "formatting_tests.rs"]
mod tests;
