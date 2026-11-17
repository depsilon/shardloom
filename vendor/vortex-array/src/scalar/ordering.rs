// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Lexicographic native scalar ordering with borrowed parent continuations.

use std::cmp::Ordering;
use std::slice;

use vortex_error::VortexExpect;

use crate::dtype::{DType, DTypeChildren};

use super::ScalarValue;

#[derive(Clone, Copy)]
enum Layout<'a> {
    Native(&'a DType),
    // The existing map entry struct is interpreted without allocating a schema.
    Entry { key: &'a DType, value: &'a DType },
}

struct Node<'a> {
    layout: Layout<'a>,
    left: Option<&'a ScalarValue>,
    right: Option<&'a ScalarValue>,
}

enum Types<'a> {
    Same(Layout<'a>),
    Fields(DTypeChildren<'a>),
    Entry(std::array::IntoIter<&'a DType, 2>),
}

impl<'a> Types<'a> {
    fn next(&mut self) -> Option<Layout<'a>> {
        match self {
            Self::Same(layout) => Some(*layout),
            Self::Fields(fields) => Some(Layout::Native(
                fields.next()?.vortex_expect("field DType must be valid"),
            )),
            Self::Entry(fields) => Some(Layout::Native(fields.next()?)),
        }
    }
}

struct Pairs<'a> {
    types: Types<'a>,
    left: slice::Iter<'a, Option<ScalarValue>>,
    right: slice::Iter<'a, Option<ScalarValue>>,
    after: Ordering,
}

impl<'a> Pairs<'a> {
    fn next(&mut self) -> Option<Node<'a>> {
        let layout = self.types.next()?;
        Some(Node {
            layout,
            left: self.left.next()?.as_ref(),
            right: self.right.next()?.as_ref(),
        })
    }
}

fn tuples<'a>(
    mut layout: Layout<'a>,
    left: &'a [Option<ScalarValue>],
    right: &'a [Option<ScalarValue>],
) -> Option<Pairs<'a>> {
    let (types, after) = loop {
        match layout {
            Layout::Native(DType::Extension(extension)) => {
                layout = Layout::Native(extension.storage_dtype());
            }
            Layout::Native(DType::List(child, _) | DType::FixedSizeList(child, ..)) => {
                break (
                    Types::Same(Layout::Native(child)),
                    left.len().cmp(&right.len()),
                );
            }
            Layout::Native(dtype @ DType::Struct(fields, _)) => {
                if left.len() != fields.nfields() || right.len() != fields.nfields() {
                    return None;
                }
                break (Types::Fields(dtype.try_child_dtypes()), Ordering::Equal);
            }
            Layout::Native(DType::Map(map, _)) => {
                // The original entries_dtype resolved key then value before
                // visiting any entries, including an empty map.
                let key = map
                    .try_key_dtype()
                    .vortex_expect("map key dtype must be valid");
                let value = map
                    .try_value_dtype()
                    .vortex_expect("map value dtype must be valid");
                break (
                    Types::Same(Layout::Entry { key, value }),
                    left.len().cmp(&right.len()),
                );
            }
            Layout::Entry { key, value } => {
                if left.len() != 2 || right.len() != 2 {
                    return None;
                }
                break (Types::Entry([key, value].into_iter()), Ordering::Equal);
            }
            _ => return None,
        }
    };
    Some(Pairs {
        types,
        left: left.iter(),
        right: right.iter(),
        after,
    })
}

pub(super) fn compare(
    dtype: &DType,
    left: Option<&ScalarValue>,
    right: Option<&ScalarValue>,
) -> Option<Ordering> {
    let mut pending = Vec::new();
    let mut node = Node {
        layout: Layout::Native(dtype),
        left,
        right,
    };
    'visit: loop {
        let ordering = match (node.left, node.right) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (Some(left), Some(right)) => match (left, right) {
                (ScalarValue::Bool(left), ScalarValue::Bool(right)) => left.partial_cmp(right),
                (ScalarValue::Primitive(left), ScalarValue::Primitive(right)) => {
                    left.partial_cmp(right)
                }
                (ScalarValue::Decimal(left), ScalarValue::Decimal(right)) => {
                    left.partial_cmp(right)
                }
                (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => left.partial_cmp(right),
                (ScalarValue::Binary(left), ScalarValue::Binary(right)) => left.partial_cmp(right),
                (ScalarValue::Tuple(left), ScalarValue::Tuple(right)) => {
                    let mut pairs = tuples(node.layout, left, right)?;
                    if let Some(child) = pairs.next() {
                        pending.push(pairs);
                        node = child;
                        continue;
                    }
                    Some(pairs.after)
                }
                (ScalarValue::Union(left), ScalarValue::Union(right)) => {
                    if left.type_id() != right.type_id() {
                        return None;
                    }
                    let Layout::Native(dtype @ DType::Union(variants, _)) = node.layout else {
                        return None;
                    };
                    let index = variants.tag_to_child_index(left.type_id())?;
                    let child = dtype
                        .try_child_dtypes()
                        .nth(index)?
                        .vortex_expect("variant DType must be valid");
                    node = Node {
                        layout: Layout::Native(child),
                        left: left.child_value(),
                        right: right.child_value(),
                    };
                    continue;
                }
                // Preserve the original unordered Variant and mismatched cases.
                _ => None,
            },
        }?;
        if ordering != Ordering::Equal {
            return Some(ordering);
        }
        while let Some(mut pairs) = pending.pop() {
            if let Some(child) = pairs.next() {
                pending.push(pairs);
                node = child;
                continue 'visit;
            }
            if pairs.after != Ordering::Equal {
                return Some(pairs.after);
            }
        }
        return Some(Ordering::Equal);
    }
}

#[cfg(test)]
#[path = "ordering_tests.rs"]
mod tests;
