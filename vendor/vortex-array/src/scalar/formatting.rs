// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordinary scalar Display with ordered borrowed continuations.

use std::fmt::{self, Write};

use vortex_error::VortexExpect;
use vortex_error::vortex_panic;

use crate::dtype::extension::ForeignExtDType;
use crate::dtype::{DType, MapDType, PType, StructFields, UnionVariants};

use super::{
    BinaryScalar, BoolScalar, DecimalScalar, ExtScalar, PValue, PrimitiveScalar, Scalar,
    ScalarValue, UnionValue, Utf8Scalar,
};

struct Output<'a, 'b> {
    formatter: &'a mut fmt::Formatter<'b>,
    // A field or map entry must finish before its separator reaches the
    // caller. Nested joins can append to the same infallible String.
    joined: Option<String>,
}

impl Output<'_, '_> {
    fn write(&mut self, arguments: fmt::Arguments<'_>) -> fmt::Result {
        if let Some(joined) = &mut self.joined {
            if joined.write_fmt(arguments).is_err() {
                vortex_panic!("a Display implementation failed while formatting into a String");
            }
            Ok(())
        } else {
            self.formatter.write_fmt(arguments)
        }
    }

    fn join(&mut self, separator: bool) -> bool {
        if let Some(joined) = &mut self.joined {
            if separator {
                joined.push_str(", ");
            }
            false
        } else {
            self.joined = Some(String::new());
            true
        }
    }
}

enum Work<'a> {
    Typed(&'a DType, Option<&'a ScalarValue>),
    Raw(&'a ScalarValue),
    Primitive(PValue),
    List(&'a DType, Option<&'a [Option<ScalarValue>]>),
    ListValues {
        dtype: &'a DType,
        remaining: &'a [Option<ScalarValue>],
        separator: bool,
    },
    Struct(&'a StructFields, Option<&'a [Option<ScalarValue>]>),
    Fields {
        fields: &'a StructFields,
        values: &'a [Option<ScalarValue>],
        index: usize,
    },
    Map(&'a MapDType, Option<&'a [Option<ScalarValue>]>),
    Entries {
        map: &'a MapDType,
        remaining: &'a [Option<ScalarValue>],
        separator: bool,
    },
    Union(&'a UnionVariants, Option<&'a UnionValue>),
    Variant(Option<&'a Scalar>),
    RawValues {
        remaining: &'a [Option<ScalarValue>],
        separator: bool,
    },
    Text(&'static str),
    Joined {
        separator: bool,
    },
}

pub(super) fn display_typed(
    dtype: &DType,
    value: Option<&ScalarValue>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::Typed(dtype, value), f)
}
pub(super) fn display_raw(value: &ScalarValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::Raw(value), f)
}
pub(super) fn display_list(
    dtype: &DType,
    values: Option<&[Option<ScalarValue>]>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::List(dtype, values), f)
}
pub(super) fn display_struct(
    fields: &StructFields,
    values: Option<&[Option<ScalarValue>]>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::Struct(fields, values), f)
}
pub(super) fn display_map(
    map: &MapDType,
    values: Option<&[Option<ScalarValue>]>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::Map(map, values), f)
}
pub(super) fn display_union(
    variants: &UnionVariants,
    value: Option<&UnionValue>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display(Work::Union(variants, value), f)
}
pub(super) fn display_variant(value: Option<&Scalar>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display(Work::Variant(value), f)
}

fn constructed<'a>(
    dtype: &'a DType,
    value: Option<&'a ScalarValue>,
    message: &'static str,
) -> Work<'a> {
    if Scalar::is_compatible(dtype, value) {
        return Work::Typed(dtype, value);
    }
    if matches!(
        (dtype, value),
        (
            DType::Primitive(PType::F16, _),
            Some(ScalarValue::Primitive(PValue::U64(_)))
        )
    ) {
        // The existing constructor's only conversion is a single primitive.
        // Keep it without cloning a nested child value or schema.
        let converted = Scalar::try_new(dtype.clone(), value.cloned()).vortex_expect(message);
        return Work::Primitive(
            *converted
                .value()
                .vortex_expect("present legacy F16")
                .as_primitive(),
        );
    }
    Err::<(), _>(Scalar::incompatible_error(dtype, value)).vortex_expect(message);
    unreachable!("incompatible scalar construction always fails")
}

fn unchecked(dtype: &DType, value: Option<&ScalarValue>) {
    #[cfg(debug_assertions)]
    Scalar::validate(dtype, value)
        .vortex_expect("Scalar::new_unchecked called with incompatible dtype and value");
    #[cfg(not(debug_assertions))]
    let _ = (dtype, value);
}

struct Leaf<'a>(&'a DType, Option<&'a ScalarValue>);
impl fmt::Display for Leaf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(dtype, value) = *self;
        match dtype {
            DType::Null => write!(f, "null"),
            DType::Bool(_) => write!(
                f,
                "{}",
                BoolScalar::try_new(dtype, value)
                    .ok()
                    .vortex_expect("Failed to convert scalar to bool")
            ),
            DType::Primitive(..) => write!(
                f,
                "{}",
                PrimitiveScalar::try_new(dtype, value)
                    .ok()
                    .vortex_expect("Failed to convert scalar to primitive")
            ),
            DType::Decimal(..) => write!(
                f,
                "{}",
                DecimalScalar::try_new(dtype, value)
                    .ok()
                    .vortex_expect("Failed to convert scalar to decimal")
            ),
            DType::Utf8(_) => write!(
                f,
                "{}",
                Utf8Scalar::try_new(dtype, value)
                    .ok()
                    .vortex_expect("Failed to convert scalar to utf8")
            ),
            DType::Binary(_) => write!(
                f,
                "{}",
                BinaryScalar::try_new(dtype, value)
                    .ok()
                    .vortex_expect("Failed to convert scalar to binary")
            ),
            DType::Extension(_) => write!(f, "{}", ExtScalar::new_unchecked(dtype, value)),
            _ => unreachable!("compound scalar formatting uses continuations"),
        }
    }
}

struct RawLeaf<'a>(&'a ScalarValue);
impl fmt::Display for RawLeaf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_leaf(f)
    }
}

fn display(mut work: Work<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut output = Output {
        formatter: f,
        joined: None,
    };
    let mut pending = Vec::new();
    loop {
        match work {
            Work::Typed(dtype, value) => {
                work = match dtype {
                    DType::List(..) | DType::FixedSizeList(..) => {
                        Work::List(dtype, value.map(ScalarValue::as_list))
                    }
                    DType::Struct(fields, _) => {
                        Work::Struct(fields, value.map(ScalarValue::as_list))
                    }
                    DType::Map(map, _) => Work::Map(map, value.map(ScalarValue::as_list)),
                    DType::Union(variants, _) => {
                        Work::Union(variants, value.map(ScalarValue::as_union))
                    }
                    DType::Variant(_) => Work::Variant(value.map(ScalarValue::as_variant)),
                    DType::Extension(extension) if extension.is::<ForeignExtDType>() => {
                        if let Some(value) = value {
                            Work::Raw(value)
                        } else {
                            Work::Text("null")
                        }
                    }
                    _ => {
                        output.write(format_args!("{}", Leaf(dtype, value)))?;
                        if let Some(next) = pending.pop() {
                            work = next;
                            continue;
                        }
                        return Ok(());
                    }
                };
                continue;
            }
            Work::Primitive(value) => output.write(format_args!("{value}"))?,
            Work::List(dtype, values) => {
                if let Some(values) = values {
                    let child = dtype
                        .as_any_size_list_element_opt()
                        .vortex_expect("ListScalar always has a list dtype");
                    let prefix = if let DType::FixedSizeList(_, size, _) = dtype {
                        format!("fixed_size<{size}>")
                    } else {
                        String::new()
                    };
                    output.write(format_args!("{prefix}["))?;
                    pending.push(Work::Text("]"));
                    work = Work::ListValues {
                        dtype: child,
                        remaining: values,
                        separator: false,
                    };
                    continue;
                }
                output.write(format_args!("null"))?;
            }
            Work::ListValues {
                dtype,
                remaining,
                separator,
            } => {
                if let Some((value, remaining)) = remaining.split_first() {
                    let child = constructed(
                        dtype,
                        value.as_ref(),
                        "`ListScalar` is already a valid `Scalar`",
                    );
                    if separator {
                        output.write(format_args!(", "))?;
                    }
                    pending.push(Work::ListValues {
                        dtype,
                        remaining,
                        separator: true,
                    });
                    work = child;
                    continue;
                }
            }
            Work::Struct(fields, values) => {
                if let Some(values) = values {
                    output.write(format_args!("{{"))?;
                    pending.push(Work::Text("}"));
                    work = Work::Fields {
                        fields,
                        values,
                        index: 0,
                    };
                    continue;
                }
                output.write(format_args!("null"))?;
            }
            Work::Fields {
                fields,
                values,
                index,
            } => {
                // The original zip_eq resolves a field dtype before pairing
                // it with the next value, even when that value is missing.
                let name = fields.names().get(index);
                let dtype = fields
                    .try_field_dtypes()
                    .nth(index)
                    .map(|dtype| dtype.vortex_expect("field DType must be valid"));
                let value = values.get(index);
                match (name, dtype, value) {
                    (Some(name), Some(dtype), Some(value)) => {
                        let child = constructed(
                            dtype,
                            value.as_ref(),
                            "unable to construct a struct `Scalar`",
                        );
                        pending.push(Work::Fields {
                            fields,
                            values,
                            index: index + 1,
                        });
                        if output.join(index != 0) {
                            pending.push(Work::Joined {
                                separator: index != 0,
                            });
                        }
                        output.write(format_args!("{name}: "))?;
                        work = child;
                        continue;
                    }
                    (None, None, None) => {}
                    _ => {
                        vortex_panic!(
                            "itertools: .zip_eq() reached end of one iterator before the other"
                        )
                    }
                }
            }
            Work::Map(map, entries) => {
                if let Some(entries) = entries {
                    output.write(format_args!("{{"))?;
                    pending.push(Work::Text("}"));
                    work = Work::Entries {
                        map,
                        remaining: entries,
                        separator: false,
                    };
                    continue;
                }
                output.write(format_args!("null"))?;
            }
            Work::Entries {
                map,
                remaining,
                separator,
            } => {
                if let Some((entry, remaining)) = remaining.split_first() {
                    let values = entry
                        .as_ref()
                        .vortex_expect("map entry is non-null")
                        .as_list();
                    let key = values.first().vortex_expect("map entry has a key").as_ref();
                    let value = values
                        .get(1)
                        .vortex_expect("map entry has a value")
                        .as_ref();
                    let key_dtype = map
                        .try_key_dtype()
                        .vortex_expect("map key dtype must be valid");
                    unchecked(key_dtype, key);
                    let value_dtype = map
                        .try_value_dtype()
                        .vortex_expect("map value dtype must be valid");
                    unchecked(value_dtype, value);
                    pending.push(Work::Entries {
                        map,
                        remaining,
                        separator: true,
                    });
                    if output.join(separator) {
                        pending.push(Work::Joined { separator });
                    }
                    pending.push(Work::Typed(value_dtype, value));
                    pending.push(Work::Text(": "));
                    work = Work::Typed(key_dtype, key);
                    continue;
                }
            }
            Work::Union(variants, value) => {
                if let Some(value) = value {
                    let index = variants
                        .tag_to_child_index(value.type_id())
                        .vortex_expect("validated union type ID must select a child dtype");
                    let dtype = variants
                        .try_variant_dtypes()
                        .nth(index)
                        .vortex_expect("validated union type ID must select a child dtype")
                        .vortex_expect("variant DType must be valid");
                    unchecked(dtype, value.child_value());
                    let name = variants
                        .names()
                        .get(index)
                        .vortex_expect("non-null union scalar must select a variant");
                    output.write(format_args!("{name}("))?;
                    pending.push(Work::Text(")"));
                    work = Work::Typed(dtype, value.child_value());
                    continue;
                }
                output.write(format_args!("null"))?;
            }
            Work::Variant(value) => {
                if let Some(value) = value {
                    output.write(format_args!("variant("))?;
                    pending.push(Work::Text(")"));
                    work = Work::Typed(value.dtype(), value.value());
                    continue;
                }
                output.write(format_args!("null"))?;
            }
            Work::Raw(value) => match value {
                ScalarValue::Tuple(values) => {
                    output.write(format_args!("["))?;
                    pending.push(Work::Text("]"));
                    work = Work::RawValues {
                        remaining: values,
                        separator: false,
                    };
                    continue;
                }
                ScalarValue::Union(value) => {
                    output.write(format_args!("union@{}(", value.type_id()))?;
                    pending.push(Work::Text(")"));
                    work = value.child_value().map_or(Work::Text("null"), Work::Raw);
                    continue;
                }
                ScalarValue::Variant(value) => {
                    work = Work::Typed(value.dtype(), value.value());
                    continue;
                }
                _ => output.write(format_args!("{}", RawLeaf(value)))?,
            },
            Work::RawValues {
                remaining,
                separator,
            } => {
                if let Some((value, remaining)) = remaining.split_first() {
                    if separator {
                        output.write(format_args!(", "))?;
                    }
                    pending.push(Work::RawValues {
                        remaining,
                        separator: true,
                    });
                    work = value.as_ref().map_or(Work::Text("null"), Work::Raw);
                    continue;
                }
            }
            Work::Text(text) => output.write(format_args!("{text}"))?,
            Work::Joined { separator } => {
                let joined = output
                    .joined
                    .take()
                    .vortex_expect("one active joined scalar buffer");
                if separator {
                    output.write(format_args!(", "))?;
                }
                output.write(format_args!("{joined}"))?;
            }
        }
        let Some(next) = pending.pop() else {
            return Ok(());
        };
        work = next;
    }
}

#[cfg(test)]
#[path = "formatting_tests.rs"]
mod tests;
