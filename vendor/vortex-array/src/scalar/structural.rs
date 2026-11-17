// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered native value operations without child trait recursion.

use std::hash::{Hash, Hasher};
use std::mem;

use crate::dtype::DType;

use super::{Scalar, ScalarValue, UnionValue};

struct CloneTuple<'a> {
    remaining: &'a [Option<ScalarValue>],
    copied: Vec<Option<ScalarValue>>,
}

impl<'a> CloneTuple<'a> {
    fn next(&mut self) -> Option<&'a ScalarValue> {
        while let Some((value, rest)) = self.remaining.split_first() {
            self.remaining = rest;
            if let Some(value) = value {
                return Some(value);
            }
            self.copied.push(None);
        }
        None
    }
}

enum CloneParent<'a> {
    Tuple(CloneTuple<'a>),
    Union(u8),
    Variant(&'a DType),
}

impl Clone for ScalarValue {
    fn clone(&self) -> Self {
        let mut current = self;
        let mut pending = Vec::new();
        'visit: loop {
            let mut copied = match current {
                Self::Bool(value) => Self::Bool(*value),
                Self::Primitive(value) => Self::Primitive(*value),
                Self::Decimal(value) => Self::Decimal(*value),
                Self::Utf8(value) => Self::Utf8(value.clone()),
                Self::Binary(value) => Self::Binary(value.clone()),
                Self::Tuple(values) => {
                    let mut tuple = CloneTuple {
                        remaining: values,
                        copied: Vec::with_capacity(values.len()),
                    };
                    if let Some(child) = tuple.next() {
                        pending.push(CloneParent::Tuple(tuple));
                        current = child;
                        continue;
                    }
                    Self::Tuple(tuple.copied)
                }
                Self::Union(value) => {
                    if let Some(child) = value.child_value() {
                        pending.push(CloneParent::Union(value.type_id()));
                        current = child;
                        continue;
                    }
                    Self::Union(UnionValue::new(value.type_id(), None))
                }
                Self::Variant(scalar) => {
                    if let Some(child) = &scalar.value {
                        pending.push(CloneParent::Variant(&scalar.dtype));
                        current = child;
                        continue;
                    }
                    Self::Variant(Box::new(Scalar {
                        value: None,
                        dtype: scalar.dtype.clone(),
                    }))
                }
            };
            while let Some(parent) = pending.pop() {
                copied = match parent {
                    CloneParent::Tuple(mut tuple) => {
                        tuple.copied.push(Some(copied));
                        if let Some(child) = tuple.next() {
                            pending.push(CloneParent::Tuple(tuple));
                            current = child;
                            continue 'visit;
                        }
                        Self::Tuple(tuple.copied)
                    }
                    CloneParent::Union(tag) => Self::Union(UnionValue::new(tag, Some(copied))),
                    CloneParent::Variant(dtype) => Self::Variant(Box::new(Scalar {
                        value: Some(copied),
                        dtype: dtype.clone(),
                    })),
                };
            }
            return copied;
        }
    }
}

enum Compare<'a> {
    Values(Option<&'a ScalarValue>, Option<&'a ScalarValue>),
    Tuple(&'a [Option<ScalarValue>], &'a [Option<ScalarValue>]),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        let mut current = Compare::Values(Some(self), Some(other));
        let mut pending = Vec::new();
        loop {
            match current {
                Compare::Values(None, None) => {}
                Compare::Values(Some(lhs), Some(rhs)) => match (lhs, rhs) {
                    (Self::Bool(lhs), Self::Bool(rhs)) => {
                        if lhs != rhs {
                            return false;
                        }
                    }
                    (Self::Primitive(lhs), Self::Primitive(rhs)) => {
                        if lhs != rhs {
                            return false;
                        }
                    }
                    (Self::Decimal(lhs), Self::Decimal(rhs)) => {
                        if lhs != rhs {
                            return false;
                        }
                    }
                    (Self::Utf8(lhs), Self::Utf8(rhs)) => {
                        if lhs != rhs {
                            return false;
                        }
                    }
                    (Self::Binary(lhs), Self::Binary(rhs)) => {
                        if lhs != rhs {
                            return false;
                        }
                    }
                    (Self::Tuple(lhs), Self::Tuple(rhs)) => {
                        current = Compare::Tuple(lhs, rhs);
                        continue;
                    }
                    (Self::Union(lhs), Self::Union(rhs)) => {
                        if lhs.type_id() != rhs.type_id() {
                            return false;
                        }
                        current = Compare::Values(lhs.child_value(), rhs.child_value());
                        continue;
                    }
                    (Self::Variant(lhs), Self::Variant(rhs)) => {
                        if !lhs.dtype.eq_ignore_nullability(&rhs.dtype) {
                            return false;
                        }
                        current = Compare::Values(lhs.value.as_ref(), rhs.value.as_ref());
                        continue;
                    }
                    // Every native value variant must be deliberately handled.
                    (Self::Bool(_), _)
                    | (Self::Primitive(_), _)
                    | (Self::Decimal(_), _)
                    | (Self::Utf8(_), _)
                    | (Self::Binary(_), _)
                    | (Self::Tuple(_), _)
                    | (Self::Union(_), _)
                    | (Self::Variant(_), _) => return false,
                },
                Compare::Values(_, _) => return false,
                Compare::Tuple(lhs, rhs) => {
                    if lhs.len() != rhs.len() {
                        return false;
                    }
                    if let (Some((left, rest_left)), Some((right, rest_right))) =
                        (lhs.split_first(), rhs.split_first())
                    {
                        if !rest_left.is_empty() {
                            pending.push(Compare::Tuple(rest_left, rest_right));
                        }
                        current = Compare::Values(left.as_ref(), right.as_ref());
                        continue;
                    }
                }
            }
            let Some(next) = pending.pop() else {
                return true;
            };
            current = next;
        }
    }
}

enum Feed<'a> {
    Value(&'a ScalarValue),
    Optional(Option<&'a ScalarValue>),
    Tuple(&'a [Option<ScalarValue>]),
}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut current = Feed::Value(self);
        let mut pending = Vec::new();
        loop {
            match current {
                Feed::Value(value) => {
                    mem::discriminant(value).hash(state);
                    match value {
                        Self::Bool(value) => value.hash(state),
                        Self::Primitive(value) => value.hash(state),
                        Self::Decimal(value) => value.hash(state),
                        Self::Utf8(value) => value.hash(state),
                        Self::Binary(value) => value.hash(state),
                        Self::Tuple(values) => {
                            values.len().hash(state);
                            current = Feed::Tuple(values);
                            continue;
                        }
                        Self::Union(value) => {
                            value.type_id().hash(state);
                            current = Feed::Optional(value.child_value());
                            continue;
                        }
                        Self::Variant(scalar) => {
                            scalar.dtype.hash_ignore_nullability(state);
                            current = Feed::Optional(scalar.value.as_ref());
                            continue;
                        }
                    }
                }
                Feed::Optional(value) => {
                    // Unit contributes no payload writes; keep Option's exact
                    // discriminant feed without recursively hashing its child.
                    value.map(|_| ()).hash(state);
                    if let Some(value) = value {
                        current = Feed::Value(value);
                        continue;
                    }
                }
                Feed::Tuple(values) => {
                    if let Some((value, rest)) = values.split_first() {
                        if !rest.is_empty() {
                            pending.push(Feed::Tuple(rest));
                        }
                        current = Feed::Optional(value.as_ref());
                        continue;
                    }
                }
            }
            let Some(next) = pending.pop() else {
                return;
            };
            current = next;
        }
    }
}

#[cfg(test)]
#[path = "structural_tests.rs"]
mod tests;
