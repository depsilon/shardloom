// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Borrowed zero inspection and approximate byte counts in original child order.

use vortex_error::{VortexExpect, vortex_panic};

use crate::dtype::{DType, MapDType, NativeDecimalType, StructFields, i256};

#[cfg(debug_assertions)]
use super::Scalar;
use super::ScalarValue;

type Value<'a> = (&'a DType, Option<&'a ScalarValue>);

fn unchecked(dtype: &DType, value: Option<&ScalarValue>) {
    #[cfg(debug_assertions)]
    Scalar::validate(dtype, value)
        .vortex_expect("Scalar::new_unchecked called with incompatible dtype and value");
    #[cfg(not(debug_assertions))]
    let _ = (dtype, value);
}

enum Children<'a> {
    List {
        dtype: &'a DType,
        values: &'a [Option<ScalarValue>],
        validate: bool,
    },
    Struct {
        fields: &'a StructFields,
        values: &'a [Option<ScalarValue>],
        index: usize,
    },
}

impl<'a> Children<'a> {
    fn next(&mut self) -> Option<Value<'a>> {
        match self {
            Self::List {
                dtype,
                values,
                validate,
            } => {
                let (value, remaining) = values.split_first()?;
                *values = remaining;
                if *validate {
                    unchecked(dtype, value.as_ref());
                }
                Some((dtype, value.as_ref()))
            }
            Self::Struct {
                fields,
                values,
                index,
            } => {
                // The original zip asks for the next value before its dtype.
                let (value, remaining) = values.split_first()?;
                let dtype = fields
                    .try_field_dtypes()
                    .nth(*index)?
                    .vortex_expect("field DType must be valid");
                *values = remaining;
                *index += 1;
                unchecked(dtype, value.as_ref());
                Some((dtype, value.as_ref()))
            }
        }
    }
}

pub(super) fn is_zero(mut current: Value<'_>) -> Option<bool> {
    let mut pending = Vec::<Children<'_>>::new();
    loop {
        let (dtype, value) = current;
        let Some(value) = value else {
            // A null child is false inside all(...), but propagates through
            // a root, extension or selected first union child.
            return if pending.is_empty() {
                None
            } else {
                Some(false)
            };
        };
        let zero = match dtype {
            DType::Null => vortex_panic!("non-null value somehow had `DType::Null`"),
            DType::Bool(_) => !value.as_bool(),
            DType::Primitive(..) => value.as_primitive().is_zero(),
            DType::Decimal(..) => value.as_decimal().is_zero(),
            DType::Utf8(_) => value.as_utf8().is_empty(),
            DType::Binary(_) => value.as_binary().is_empty(),
            DType::List(..) | DType::Map(..) => value.as_list().is_empty(),
            DType::FixedSizeList(child, size, _) => {
                let values = value.as_list();
                if values.len() != *size as usize {
                    false
                } else {
                    let mut children = Children::List {
                        dtype: child,
                        values,
                        validate: true,
                    };
                    if let Some(child) = children.next() {
                        pending.push(children);
                        current = child;
                        continue;
                    }
                    true
                }
            }
            DType::Struct(fields, _) => {
                let mut children = Children::Struct {
                    fields,
                    values: value.as_list(),
                    index: 0,
                };
                if let Some(child) = children.next() {
                    pending.push(children);
                    current = child;
                    continue;
                }
                true
            }
            DType::Union(variants, _) => {
                let value = value.as_union();
                if variants.tag_to_child_index(value.type_id()) != Some(0) {
                    false
                } else {
                    let dtype = variants
                        .try_variant_dtypes()
                        .next()
                        .vortex_expect("validated union type ID must select a child dtype")
                        .vortex_expect("variant DType must be valid");
                    unchecked(dtype, value.child_value());
                    current = (dtype, value.child_value());
                    continue;
                }
            }
            DType::Variant(_) => {
                let inner = value.as_variant();
                if inner.is_null() {
                    true
                } else {
                    current = (inner.dtype(), inner.value());
                    continue;
                }
            }
            DType::Extension(extension) => {
                let dtype = extension.storage_dtype();
                unchecked(dtype, Some(value));
                current = (dtype, Some(value));
                continue;
            }
        };
        if !zero {
            return Some(false);
        }
        loop {
            let Some(mut children) = pending.pop() else {
                return Some(true);
            };
            if let Some(child) = children.next() {
                pending.push(children);
                current = child;
                break;
            }
        }
    }
}

enum SizeFrame<'a> {
    Sequence(Children<'a>, usize),
    Map {
        map: &'a MapDType,
        remaining: &'a [Option<ScalarValue>],
        total: usize,
    },
    MapKey(Value<'a>),
    MapValue(usize),
    UnionTag,
}

fn map_entry<'a>(map: &'a MapDType, entry: Option<&'a ScalarValue>) -> (Value<'a>, Value<'a>) {
    let values = entry.vortex_expect("map entry is non-null").as_list();
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
    ((key_dtype, key), (value_dtype, value))
}

pub(super) fn approx_nbytes(mut current: Value<'_>) -> usize {
    let mut pending = Vec::new();
    'descend: loop {
        let (dtype, value) = current;
        let mut size = match dtype {
            DType::Null => 0,
            DType::Bool(_) => 1,
            DType::Primitive(ptype, _) => ptype.byte_width(),
            DType::Decimal(decimal, _) => {
                if decimal.precision() <= i128::MAX_PRECISION {
                    size_of::<i128>()
                } else {
                    size_of::<i256>()
                }
            }
            DType::Utf8(_) => value.map_or(0, |value| value.as_utf8().len()),
            DType::Binary(_) => value.map_or(0, |value| value.as_binary().len()),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                let values = value.map_or(&[][..], ScalarValue::as_list);
                // elements() originally constructs every child before any
                // size call. Retain its debug validation order without copies.
                #[cfg(debug_assertions)]
                for value in values {
                    unchecked(child, value.as_ref());
                }
                let mut children = Children::List {
                    dtype: child,
                    values,
                    validate: false,
                };
                if let Some(child) = children.next() {
                    pending.push(SizeFrame::Sequence(children, 0));
                    current = child;
                    continue;
                }
                0
            }
            DType::Struct(fields, _) => {
                let mut children = Children::Struct {
                    fields,
                    values: value.map_or(&[][..], ScalarValue::as_list),
                    index: 0,
                };
                if let Some(child) = children.next() {
                    pending.push(SizeFrame::Sequence(children, 0));
                    current = child;
                    continue;
                }
                0
            }
            DType::Map(map, _) => {
                let entries = value.map_or(&[][..], ScalarValue::as_list);
                if let Some((entry, remaining)) = entries.split_first() {
                    let (key, value) = map_entry(map, entry.as_ref());
                    pending.push(SizeFrame::Map {
                        map,
                        remaining,
                        total: 0,
                    });
                    pending.push(SizeFrame::MapKey(value));
                    current = key;
                    continue;
                }
                0
            }
            DType::Union(variants, _) => {
                if let Some(value) = value {
                    let value = value.as_union();
                    let index = variants
                        .tag_to_child_index(value.type_id())
                        .vortex_expect("validated union type ID must select a child dtype");
                    let dtype = variants
                        .try_variant_dtypes()
                        .nth(index)
                        .vortex_expect("validated union type ID must select a child dtype")
                        .vortex_expect("variant DType must be valid");
                    unchecked(dtype, value.child_value());
                    pending.push(SizeFrame::UnionTag);
                    current = (dtype, value.child_value());
                    continue;
                }
                0
            }
            DType::Variant(_) => {
                if let Some(value) = value {
                    let inner = value.as_variant();
                    current = (inner.dtype(), inner.value());
                    continue;
                }
                0
            }
            DType::Extension(extension) => {
                let dtype = extension.storage_dtype();
                unchecked(dtype, value);
                current = (dtype, value);
                continue;
            }
        };
        loop {
            match pending.pop() {
                None => return size,
                Some(SizeFrame::Sequence(mut children, total)) => {
                    let total = total + size;
                    if let Some(child) = children.next() {
                        pending.push(SizeFrame::Sequence(children, total));
                        current = child;
                        continue 'descend;
                    }
                    size = total;
                }
                Some(SizeFrame::Map {
                    map,
                    remaining,
                    total,
                }) => {
                    let total = total + size;
                    if let Some((entry, remaining)) = remaining.split_first() {
                        let (key, value) = map_entry(map, entry.as_ref());
                        pending.push(SizeFrame::Map {
                            map,
                            remaining,
                            total,
                        });
                        pending.push(SizeFrame::MapKey(value));
                        current = key;
                        continue 'descend;
                    }
                    size = total;
                }
                Some(SizeFrame::MapKey(value)) => {
                    pending.push(SizeFrame::MapValue(size));
                    current = value;
                    continue 'descend;
                }
                Some(SizeFrame::MapValue(key_size)) => size += key_size,
                Some(SizeFrame::UnionTag) => size += 1,
            }
        }
    }
}

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod tests;
